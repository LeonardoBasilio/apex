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

**What this buys, concretely:** `capture-pyspark/` can produce one event per task even on a
job with 10k+ tasks without that rate ever reaching Postgres directly — the
`store-postgres/` consumer reads at whatever pace it can sustain, and Kafka's retention
window (not Postgres) is where raw per-task granularity lives. Postgres only gets what the
consumer chooses to aggregate before insert.

## Topics

| Topic | Producer | Consumer | Payload |
|---|---|---|---|
| `apex.spark.stage_events` | capture-pyspark | store-postgres | one message per `onStageCompleted` |
| `apex.spark.task_events` | capture-pyspark | store-postgres | one message per `onTaskEnd` (raw; not all of this reaches Postgres — see store-postgres's README) |
| `apex.spark.plan_events` | capture-pyspark | store-postgres | one message per captured physical plan + AQE decision |

Message schema mirrors the OTLP payload shape from upstream's `CONTRACT.md` (same field
names) so the detectors in `engine/` don't need to know which transport produced the row —
only `store-postgres/`'s consumer and `collect/`'s exporter differ.

## Status

Not yet implemented — scaffold only. Depends on `capture-pyspark/`'s producer existing first.
