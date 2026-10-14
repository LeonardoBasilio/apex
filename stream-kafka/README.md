# stream-kafka

Replaces [`collect/`](../collect/) (OTel Collector config) in this portfolio port. Sits
between [`capture-pyspark/`](../capture-pyspark/) (producer) and
[`store-postgres/`](../store-postgres/) (consumer).

## Why Kafka instead of an OTel Collector

Upstream's `collect/` is config-only: an `otelcol` YAML pipeline (OTLP receiver → PII-scrub
processor → ClickHouse exporter), no custom code. That's a fine choice when the store on the
other end is built for high-cardinality telemetry. On this stack the store is Postgres, which
is not — so the transport layer's job changes from "move events fast" to "absorb the
capture/store rate mismatch," which an OTel Collector pipeline doesn't do and a log-compacted
Kafka topic does.

**What this buys, concretely:** decouples capture-pyspark's event rate from Postgres's write
rate — the `store-postgres/` consumer reads at whatever pace it can sustain regardless of
job size.

## Revision — there is no per-task topic

The original plan here (see git history on this branch) had a third topic,
`apex.spark.task_events`, carrying one raw message per `onTaskEnd`, with percentile
aggregation deferred to the `store-postgres/` consumer. That was dropped before any code
landed: a 10k-task stage would mean 10k Kafka messages for a number
(`task_duration_p50_ms`/`p99_ms`) that [`capture-pyspark/`](../capture-pyspark/)'s
`aggregator.py` can compute in-process, synchronously, for free. **Percentiles are now
computed at capture time**, per stage, and only the aggregated result ever reaches Kafka.
This is a real capability loss, stated plainly rather than hidden: nothing in this port can
later ask "show me every individual task duration for this stage" — only the aggregate
survives. If a future lane genuinely needs that, it's a contract change with its own
write-volume cost, same as upstream's own rule for widening what gets persisted.

## Topics

| Topic | Producer | Consumer | Payload |
|---|---|---|---|
| `apex.spark.stage_events` | capture-pyspark | store-postgres | one message per completed stage, with task percentiles already computed — see `apex_capture.extract.build_stage_contract_event` |
| `apex.spark.plan_events` | capture-pyspark | store-postgres | everything else capture-pyspark's listener forwards generically (job/application lifecycle, AQE plan-transition events) — field extraction for these is deferred, see capture-pyspark/README.md |

Message schema (the `stage_events` topic) mirrors the relevant subset of upstream's
`CONTRACT.md` field names, so `engine/`'s detectors don't need to know which transport
produced the row — only `store-postgres/`'s consumer and upstream's `collect/` differ.

## Implemented

`apex_stream.producer.KafkaEventProducer` is the `sink` callable injected into
`apex_capture.listener.attach()`. It runs on capture-pyspark's background worker thread
(never the Spark listener-bus thread) and:

- routes by `event["type"]` via `topics.topic_for()` — `"stage_completed"` → `stage_events`,
  everything else → `plan_events`
- JSON-serializes the payload; an event whose extraction hasn't been built yet (so `event`
  is still a raw, non-serializable JVM object) degrades to a minimal envelope
  (`type`/`received_at`/the payload's type name) instead of raising — counted separately
  (`ProducerStats.degraded`) from an actual transport failure, so the two don't get
  conflated when reading the numbers later
- `from_bootstrap_servers()` wraps kafka-python's `KafkaProducer`; the class itself takes
  any object shaped like it (`.send(topic, value)`, `.flush(timeout=None)`), which is how
  its own test suite runs without a broker

9 tests, no Kafka broker needed to run them.

## Status

Producer side done. Depends next on `store-postgres/`'s consumer existing to read these
topics and apply the DDL that defines `stage_events`' shape in Postgres.
