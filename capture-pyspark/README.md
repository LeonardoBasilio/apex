# capture-pyspark

Replaces [`jar/`](../jar/) (Scala `SparkListener` plugin) in this portfolio port. Same job:
observe stage/task metrics, the physical plan, and AQE decisions from inside a running
Spark application — but as a native PySpark listener instead of a compiled JVM plugin, and
emitting to Kafka instead of OTLP.

## Why a native listener instead of the Scala jar

`pyspark.SparkContext._jsc.sc().addSparkListener(...)` registers a listener on the JVM's
`SparkListenerBus` through py4j, no separate build or `spark-submit --jars` flag needed —
the capture code ships with the job itself. That's the whole appeal for this stack: one
fewer moving part (no sbt/JDK pinning across Spark 3.5/4.x cells like upstream's `jar/` has to
manage).

## The one real risk this trade introduces

Listener callbacks (`onStageCompleted`, `onTaskEnd`, ...) run **synchronously on the
`SparkListenerBus` dispatch thread**, inside the driver JVM. In the Scala plugin this is
normal JVM code — fast, no cross-language hop. Here, every callback pays a py4j
round-trip into Python. If the Python-side handler blocks (a slow Kafka produce call, a
lock contended by another thread, GC pause under load), the whole listener bus backs up,
and *other* listeners on the same bus — including Spark's own UI/history-server listeners —
stall with it. This is the inverse of Apex's own thesis: a capture layer is not allowed to
be the thing that makes the job it's observing slower.

**Implemented:** `queue_worker.NonBlockingEventQueue` — `submit()` never blocks the caller;
under backpressure it drops the *oldest* queued event to make room for the newest and always
counts the drop (`QueueStats.dropped`), never silently. `listener.CaptureListener` runs on
this queue: `onTaskEnd`/`onStageCompleted` do their work synchronously on the listener-bus
thread (cheap in-memory bookkeeping only, see below), and only `onStageCompleted` ever calls
`queue.submit()` — one message per *stage*, not per task.

## Captured fields (parity target with `jar/`)

Same four categories the Scala plugin captures, redacted the same way (plan literal-stripped
in-process before it ever leaves the driver — see [README §Privacy](../README.md#privacy-diagnosable-without-your-data)):

- **stage metrics (bytes/tasks/duration per stage) — implemented**, v1 subset of
  `CONTRACT.md`'s field set: identity + shuffle/spill/gc/input/output bytes +
  `task_duration_p50_ms`/`p99_ms`/`max_ms`/`sample_count`. The percentiles are computed by
  `aggregator.StageAggregator` **in-process**, from every `onTaskEnd` for that stage —
  nothing per-task ever reaches the queue or Kafka (see
  [`stream-kafka/README.md`](../stream-kafka/README.md)'s revision note for why). Field
  extraction (`extract.py`) is best-effort: every JVM accessor is wrapped individually, so a
  version-drifted or missing method degrades that one field to `None` rather than losing the
  whole event, or — worse — silently reading it as `0`.
- plan fingerprint (operators only, columns renamed `none#N`, literals dropped) — **deferred**
- AQE decisions (skew-join coalesce, partition count changes) — **deferred**; forwarded
  generically today via `onOtherEvent`'s fallback in `__getattr__`, raw and unshaped
- resolved Spark config (the keys the detectors key off — e.g. `spark.sql.adaptive.skewJoin.enabled`) — **deferred**

None of the JVM accessors in `extract.py` have been exercised against a live Spark cluster
yet — only against duck-typed fakes in `tests/fakes.py` that mirror the method names Spark's
Scala API exposes. That gap is tracked, not hidden; closing it is the first thing to do once
a real cluster is available (same item upstream's own README lists first in its Roadmap).

## Status

Done: non-blocking queue, listener bridge, per-stage percentile aggregation, Kafka wiring via
[`stream-kafka/`](../stream-kafka/)'s producer. Not done: plan fingerprint + AQE extraction,
live-cluster validation.
