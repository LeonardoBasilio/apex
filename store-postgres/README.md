# store-postgres

Replaces [`infra/`](../infra/) (ClickHouse + HyperDX) in this portfolio port. Owns the Kafka
consumer that aggregates [`stream-kafka/`](../stream-kafka/) events and the Postgres DDL that
[`engine/`](../engine/), [`serve/`](../serve/), [`memory/`](../memory/), and
[`verify/`](../verify/) read from — same role upstream's bronze-equivalent plays: **all DDL
application lives here**, no other lane creates a table.

## The trade-off this lane exists to be honest about

ClickHouse is a columnar store built for exactly this workload — aggregate over millions of
task-level rows in milliseconds. Postgres is a row store; querying raw per-task metrics at
the volume a real Spark job produces will not perform the same way, full stop. This isn't a
"tune it later" caveat — it is a design decision made up front, same spirit as the
README's "no silent green": pretending this lane has ClickHouse's query profile would be the
exact failure mode Apex's whole thesis argues against.

**The actual boundary** (revised — see [`stream-kafka/README.md`](../stream-kafka/README.md)'s
revision note): [`capture-pyspark/`](../capture-pyspark/)'s `aggregator.py` computes
per-stage task-duration percentiles **in-process, before anything reaches Kafka**. There is
no raw per-task topic to consume, so this is not "ClickHouse aggregates on read, Postgres
aggregates on write by convention" — raw per-task numbers never exist anywhere past the
Spark driver's memory for the few seconds a stage runs. That is a real capability loss
(no later drill-down to an individual task), traded for a transport and store that never see
task-level volume at all, which is a cheaper trade-off than "store it all in Kafka and hope
Postgres never needs it":

| Granularity | Where it lives | Why |
|---|---|---|
| Per-stage aggregates (bytes/task, p50/p99/max duration, task count) | **Postgres**, one row per stage per run | This is everything the skew/tail/spill detectors in `engine/` actually read — upstream's own detectors key off stage-level stats, not raw tasks. |
| Per-task raw metrics | **Nowhere** — discarded by `capture-pyspark/`'s aggregator immediately after it folds a task's duration into that stage's running percentile state | No consumer in this port has ever needed a single task's duration on its own; upstream's own detectors are stage-level too. |
| Plan fingerprints + AQE decisions | **Postgres**, one row per stage transition | Low cardinality; this is what `verify/`'s mechanism checks and `memory/`'s plan-shape recall both need. Extraction for these from capture-pyspark is still deferred (see its README). |

If a future lane genuinely needs raw per-task data (e.g. a finer tail-latency model), that's
a contract change at the capture layer first — not a Postgres schema change — since the data
doesn't exist past `capture-pyspark/`'s in-memory aggregator today.

## DDL ownership

Mirrors upstream's rule: `create_schema: false` everywhere else. Tables are versioned
migrations in [`ddl/`](ddl/) (`001_stage_events.sql`, applied in filename order by
`apex_store.migrate.apply_ddl`); `engine/`, `serve/`, `memory/`, `verify/` only
`SELECT`/`INSERT` into tables this lane defines.

## Implemented

- **`ddl/001_stage_events.sql`** — the v1 subset table: identity + the stage metrics
  `capture-pyspark/` actually emits. `PRIMARY KEY (job_id, stage_id, stage_attempt)`.
- **`apex_store.migrate.apply_ddl(connection, ddl_dir)`** — executes each `*.sql` file in
  filename order, committing after each one (a failure partway through leaves prior
  migrations applied, not rolled back together).
- **`apex_store.consumer.StageEventConsumer`** — reads only `apex.spark.stage_events`
  messages (a message on `apex.spark.plan_events` is counted and ignored, not treated as an
  error — there's nothing shaped to insert from that topic yet). `ON CONFLICT (job_id,
  stage_id, stage_attempt) DO NOTHING` makes a redelivered message a no-op rather than a
  duplicate row, covering Kafka's at-least-once delivery after a consumer restart before an
  offset commit. A malformed or missing-field payload — including stream-kafka's own
  "degraded envelope" for an event it couldn't serialize — is counted
  (`ConsumeStats.skipped_malformed`) and skipped, never inserted with nulls standing in for
  real data.
- **`apex_store.connection.from_dsn()`** — thin `psycopg` wrapper exposing
  `execute`/`commit`; every other module depends on that shape as a Protocol, not this
  concrete class, so `migrate.py`'s and `consumer.py`'s own tests run without a Postgres
  driver installed.
- **`docker-compose.yml`** — closes the infra loop: `kafka` (single-node KRaft) +
  `postgres` + a containerized `consumer` running `scripts/run_consumer.py` continuously
  (`apply_ddl` on start, then consumes `apex.spark.stage_events` forever).

14 unit tests (no Postgres/Kafka needed), + 1 live integration test
(`tests/test_integration_live.py`, skipped unless `POSTGRES_DSN` and
`KAFKA_BOOTSTRAP_SERVERS` are set — see below).

## Run against Docker Compose

```sh
cp .env.example .env
make up      # kafka + postgres + consumer, DDL applied, topics created
make smoke   # produces one fake stage_completed event, confirms it lands in postgres
make logs    # tail the consumer
make down    # stop (keeps volumes)
```

**Verified live**, not just assumed to work: `make smoke` passes end to end — a message
produced from a host Python process reaches the containerized consumer's own long-running
Kafka consumer group with **zero lag**, and the row is queryable in Postgres immediately
after.

### Two real pitfalls this surfaced, fixed in `docker-compose.yml`

Both looked like "the consumer is just hanging" with no error pointing at the cause — the
kind of thing that's easy to paper over with a longer timeout instead of finding. Neither is
specific to this port; both apply to any single-broker Kafka setup:

1. **Topic-creation race.** `consumer` subscribes at container start, before any topic
   necessarily exists yet. `kafka-python`'s client only refreshes topic metadata every
   `metadata_max_age_ms` (default 5 minutes) — a consumer that started before the topic
   existed stays blind to it for up to 5 minutes after the topic is finally auto-created by
   the first producer. Fixed by a `kafka-init` one-shot service that creates both topics
   explicitly before `consumer` is allowed to start (`depends_on: condition:
   service_completed_successfully`).
2. **Default replication factor.** Kafka's internal `__consumer_offsets` topic — every
   consumer group's coordinator depends on it existing — defaults to replication factor 3,
   unsatisfiable on a single broker. The failure mode isn't an error: the broker logs "Sent
   auto-creation request for Set(__consumer_offsets)..." once per poll, forever, and every
   consumer group hangs indefinitely with no coordinator. Disabling topic auto-create
   entirely was tried first (looked like the right fix for pitfall #1) and made this one
   worse, since that flag also gates `__consumer_offsets` itself. Fixed by setting
   `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1` (and the transaction-log equivalents) —
   see the comment block at the top of `docker-compose.yml`.

## Status

Done: DDL, migration runner, consumer with idempotent insert, a verified-live Docker Compose
stack. Not done: the `engine/`/`serve/` read-side queries against this schema, and wiring a
real Spark job (via `capture-pyspark/` + `stream-kafka/`) instead of this lane's own smoke
producer — that needs `dev/`'s pathology lab ported next.
