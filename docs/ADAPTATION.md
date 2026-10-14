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
| **generate** (`dev/`) | PySpark + Delta lab | unchanged, add a Databricks Job variant | Shows the same lab running both locally (Docker) and as a Databricks job — the two deployment targets this case is meant to prove. |

## What this is a case *for*

Not "I copied an open-source repo." The portfolio claim is specific:
1. **Ported a measured, contract-driven architecture to a different ingestion/storage stack** without touching the detection logic — proof the lane boundaries actually hold.
2. **Replaced a JVM build step with a native PySpark listener**, and documented the one real risk that trade introduces (blocking the listener thread).
3. **Replaced a columnar store with Postgres fed by Kafka**, and was explicit about what capability that costs (raw per-task drill-down) rather than hiding it.
4. **Deploys to two different places** (local Docker lab, Databricks Job) off the same capture code.

## Status

Implemented and verified live, end to end: `capture-pyspark/` → `stream-kafka/` →
`store-postgres/` → `engine-postgres/`. A crafted skewed stage event produced through the real
chain (Kafka → the running `store-postgres` consumer → Postgres) reproduces the exact verdict
CONTRACT.md's rule 1 predicts — critical on 8 slots, no finding at all on 2 slots, same data —
see `engine-postgres/README.md`'s "Verified live" section. Each lane kept its own tests green
before the next one started, same house rule as upstream: **no silent green**. Still not done:
`dev/`'s pathology lab port (a real Spark job instead of a crafted event) and plan/AQE
extraction in `capture-pyspark/`.
