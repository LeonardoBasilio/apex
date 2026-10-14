# Adaptation Note — portfolio stack

This branch (`portfolio/aws-databricks-stack`) forks [luanmorenommaciel/apex](https://github.com/luanmorenommaciel/apex)
to rebuild it on a different stack, as a portfolio case. The architecture discipline is kept —
decoupled lanes, a frozen cross-lane contract, deterministic detectors before any LLM —
only the implementation of each lane changes.

## Why this exists

Apex's own case for itself is the point: a tool that refuses to report a number it can't
measure, built across lanes that never import each other's code. That discipline is worth
demonstrating on a stack closer to day-to-day data engineering work (PySpark, Kafka, Postgres,
Databricks, AWS/Azure) rather than rebuilding it from scratch.

## What changes, and why

| Lane | Original | This branch | Reason |
|---|---|---|---|
| **capture** (`jar/`) | Scala plugin on `SparkListener` | **PySpark native listener** — `sparkContext._jsc.sc().addSparkListener(...)` via py4j, no JVM build step | Avoids a Scala/sbt build entirely; PySpark 3.x exposes the same listener interface Java/Scala does. Trade-off: listener callbacks still run on the JVM thread, so the Python-side handler must stay non-blocking — this is the main engineering risk of the swap and gets its own ADR. |
| **transport** (`collect/`) | OTel Collector (config-only) → ClickHouse | **Kafka** — the listener produces one event per stage/task/plan decision to a topic | Matches the ingestion pattern actually used in AWS/Azure data platforms (MSK / Event Hubs) instead of an OTel sidecar. Decouples capture rate from store write rate — a slow Postgres write no longer backs up the Spark job itself. |
| **store** (`infra/` ClickHouse+HyperDX) | ClickHouse (columnar, built for this) | **Postgres**, fed by a Kafka consumer that aggregates before insert | Matches the requested stack. Known trade-off, accepted deliberately: Postgres is not a columnar store, so the consumer pre-aggregates (rolling stats per stage, not per task) before the insert, and any raw-task-level drill-down stays in Kafka's retention window, not in Postgres. This boundary is the single most important documented trade-off in this port — see [CONTRACT.md](../CONTRACT.md) ground rules once this lane's contract delta is drafted. |
| **reason** (`engine/`) | Python + CrewAI, deterministic detectors first | **`engine-postgres/`** — physics.py (the closed form) ported verbatim, reading `store-postgres/`'s schema | Already the target language; only the read path and the scope changed. Deliberately v1-scoped: no plan-evidence join detection, no noise-floor history, no job_conf-sourced width — each is a missing *input* (deferred upstream in `capture-pyspark/`), not a weaker reimplementation. See `engine-postgres/README.md`'s "What's deferred" table. |
| **answer** (`serve/`) | Python + FastMCP | unchanged | Already the target stack. |
| **memory** / **verify** | Python, ClickHouse reads | Python, Postgres reads | Query layer only; detector logic is store-agnostic by design in the original (lanes only talk through the contract). |
| **generate** (`dev/`) | PySpark + Delta lab | **`dev-pyspark/`** — a real 2M-row skewed sort-merge join, no S3/Delta dependency | Same purpose, minimal implementation: just enough to produce genuine skew through `capture-pyspark/`, run in the same `apache/spark:4.0.1` image upstream's own `dev/` already pins. |
| local infra | Docker Compose | Docker Compose (unchanged) | Already the target tooling. |
| cloud target | — | **`deploy-cloud/`** — Terraform (AWS: MSK + RDS) + a Databricks Job running `dev-pyspark/`'s script unmodified | Written and locally validated (`terraform validate`, valid JSON against the documented Jobs API schema) — **not applied**: no real AWS/Databricks resources created from this session. See `deploy-cloud/README.md`'s "What this is, precisely." |

## What this is a case *for*

Not "I copied an open-source repo." The portfolio claim is specific:
1. **Ported a measured, contract-driven architecture to a different ingestion/storage stack** without touching the detection logic — proof the lane boundaries actually hold.
2. **Replaced a JVM build step with a native PySpark listener**, and documented the one real risk that trade introduces (blocking the listener thread).
3. **Replaced a columnar store with Postgres fed by Kafka**, and was explicit about what capability that costs (raw per-task drill-down) rather than hiding it.
4. **Deploys to two different places** (local Docker lab, Databricks Job) off the same capture code.

## Status

Implemented and verified live, end to end, from a **real Spark execution** — not just a
crafted event: `dev-pyspark/`'s skewed join → `capture-pyspark/` (native listener,
AQE `skew_split` capture) → `stream-kafka/` → `store-postgres/` (running consumer) →
`engine-postgres/`'s ported closed form. A real run produced AQE actually splitting a
skewed partition (task_count 8→9), and the detector called it CRITICAL at 8 slots and
WORK_BOUND (no finding at all) at 2 slots against that same stored row — see
`dev-pyspark/README.md`'s "Verified live" section for the exact numbers. Three real
engineering problems surfaced running this for real rather than designing around them in
advance (py4j's callback server needing more than `start_callback_server()`, AQE's own
coalescing erasing the skew before capture, Catalyst pruning an unused column before any
shuffle) — each documented where it was found, not smoothed over.

`deploy-cloud/`'s Terraform (AWS) and Databricks Job definition are written and locally
validated but **not applied** — no real cloud resources exist from this session; see its
README for exactly what that boundary means.

Each lane kept its own tests green before the next one started, same house rule as upstream:
**no silent green**. 86 unit tests across the five Python lanes. Still not done: plan
fingerprint extraction, `join_switch`/`coalesce` AQE detection validated against a real
triggering run (only `skew_split` has been), and an actual cloud deployment.
