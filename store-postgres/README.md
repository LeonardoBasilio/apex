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

**The actual boundary:**

| Granularity | Where it lives | Why |
|---|---|---|
| Per-stage aggregates (bytes/task, p50/p99 duration, task count) | **Postgres**, one row per stage per run | This is everything the skew/tail/spill detectors in `engine/` actually read — upstream's own detectors key off stage-level stats, not raw tasks. |
| Per-task raw metrics | **Kafka topic retention only**, not inserted into Postgres by default | Keeps Postgres write volume bounded regardless of job size. A task-level drill-down tool would read Kafka directly (or a time-boxed replay), not Postgres. |
| Plan fingerprints + AQE decisions | **Postgres**, one row per stage transition | Low cardinality; this is what `verify/`'s mechanism checks and `memory/`'s plan-shape recall both need. |

If a future lane genuinely needs raw per-task Postgres rows (e.g. a finer tail-latency
model), that's a contract change — add a table, document the write-volume cost in this
README, never silently widen what the consumer inserts.

## DDL ownership

Mirrors upstream's rule: `create_schema: false` everywhere else. Tables are versioned
migrations in this directory; `engine/`, `serve/`, `memory/`, `verify/` only `SELECT`/`INSERT`
into tables this lane defines.

## Status

Not yet implemented — scaffold only. Depends on `stream-kafka/`'s topic schema being frozen
first (same "contract before code" discipline as upstream's `CONTRACT.md`).
