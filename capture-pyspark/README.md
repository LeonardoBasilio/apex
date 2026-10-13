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

**Mitigation, not yet implemented:** the listener callback only enqueues the raw event
(non-blocking, bounded queue, drop-oldest under backpressure) onto a background thread that
owns the actual Kafka producer. A dropped event must be *counted*, never silent — same rule
upstream calls "no silent green," applied to data loss instead of test coverage.

## Captured fields (parity target with `jar/`)

Same four categories the Scala plugin captures, redacted the same way (plan literal-stripped
in-process before it ever leaves the driver — see [README §Privacy](../README.md#privacy-diagnosable-without-your-data)):

- stage metrics (bytes/tasks/duration per stage)
- plan fingerprint (operators only, columns renamed `none#N`, literals dropped)
- AQE decisions (skew-join coalesce, partition count changes)
- resolved Spark config (the keys the detectors key off — e.g. `spark.sql.adaptive.skewJoin.enabled`)

## Status

Not yet implemented — scaffold only. First task: a minimal listener that proves the
non-blocking queue under synthetic backpressure before any real Kafka wiring lands.
