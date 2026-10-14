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

## A second real risk this trade introduces (found running it, not designing it)

Registering the listener needs more than `gateway.start_callback_server()`. PySpark's JVM
gateway is launched with the callback port already fixed (usually disabled) at startup;
starting the server afterward opens a socket on a new ephemeral port the JVM was never told
about. The result wasn't an error — every callback silently failed with
`Connection refused`, retried forever, and `attach()`'s queue sat at `received=0` for an
entire real 2M-row job. `listener.attach()` now uses PySpark's own
`ensure_callback_server_started` (the exact function `pyspark.sql.streaming.listener`'s
`StreamingQueryListener` uses internally for the same kind of JVM→Python callback), which
also calls `GatewayServer.resetCallbackClient(...)` to tell the JVM the real port. See
[`dev-pyspark/README.md`](../dev-pyspark/README.md) for the live run that surfaced this.

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
- plan fingerprint (operators only, columns renamed `none#N`, literals dropped) — **deferred**;
  no listener-bus event carries the full logical plan the way `SparkListenerSQLAdaptiveExecutionUpdate`
  carries the physical one, so this needs `QueryExecutionListener`
  (`spark.listenerManager.register`) — a separate registration point from `addSparkListener`,
  not yet wired.
- **AQE decisions (`aqe.py`) — implemented**: tracks the physical-plan snapshot
  `SparkListenerSQLAdaptiveExecutionUpdate` carries per `execution_id`, diffs consecutive
  snapshots, and classifies `skew_split`/`join_switch`/`coalesce`/`other` from structural
  markers only (operator names, never literal data — see `aqe.py`'s module docstring on
  redaction). `skew_split` detection is grounded in a real captured transition (see
  `dev-pyspark/README.md`); `join_switch` and `coalesce` are implemented from upstream's
  documented conventions but **not yet exercised by a real run**.
- resolved Spark config (the keys the detectors key off — e.g. `spark.sql.adaptive.skewJoin.enabled`) — **deferred**

Stage-metrics extraction (`extract.py`) and AQE extraction (`aqe.py`) have both now been
validated against a real Spark 4.0.1 cluster (`dev-pyspark/`'s skew-join job), not only against
duck-typed fakes — the fakes still carry the tests day to day, but the shapes they mirror are
no longer guesses.

## Status

Done: non-blocking queue, listener bridge, per-stage percentile aggregation, AQE
`skew_split`/`join_switch`/`coalesce` transition capture, Kafka wiring via
[`stream-kafka/`](../stream-kafka/)'s producer — all verified against a real Spark job in
[`dev-pyspark/`](../dev-pyspark/). Not done: plan fingerprint extraction, `join_switch`/`coalesce`
validation against a real triggering run, resolved-config capture.
