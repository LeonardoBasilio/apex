# Portfolio case: porting Apex to a Spark/Kafka/Postgres/AWS-Databricks stack

> **Fork of** [luanmorenommaciel/apex](https://github.com/luanmorenommaciel/apex) ·
> **branch** `portfolio/aws-databricks-stack` · built with Claude Code.
> Original project's own README is unchanged below this file — this document is the
> case for *this branch's* work, not a claim about the upstream project.

## The pitch

Apex is an open-source Spark performance tool whose entire thesis is refusing to report a
number it can't measure — a fixed-threshold skew rule gets rebuilt into a closed-form physics
bound, derived and tested against real measured runs, because a 21.6× ratio on a 2-slot
cluster and the same ratio on a 50-slot cluster mean opposite things. That discipline —
decoupled lanes, a frozen contract between them, deterministic logic before any LLM — is the
part worth demonstrating independently of the original stack.

This branch ports that architecture to **PySpark, Kafka, Postgres, Docker, and Databricks/AWS**
instead of Scala, OTel Collector, and ClickHouse. Four lanes, each a real engineering decision
with a stated trade-off, not a global search-and-replace:

```mermaid
flowchart LR
    L["capture-pyspark<br/>native SparkListener"] -->|stage_completed event| K["stream-kafka<br/>producer"]
    K -->|Kafka topic| C["store-postgres<br/>consumer"]
    C -->|INSERT| PG[("Postgres<br/>stage_events")]
    PG -->|SELECT| E["engine-postgres<br/>tail-bound detector"]
    E -->|Finding| OUT(["critical / warning / nothing,<br/>depending on cluster width"])
```

## What was actually proven, not just built

The strongest claim a portfolio case can make is a command and its real output. This is one,
run against the live stack (not mocked):

```sh
$ POSTGRES_DSN=postgresql://apex:apex_local_dev@localhost:5432/apex \
    python scripts/analyze_job.py demo-skew-job-001 --slots 8
```
```json
{"findings": [{"severity": "critical",
  "evidence": "p99/p50 = 20.70x ... > (n-1)/(slots-1) = (100-1)/(8-1) = 14.14 [operator]; an ideal rebalance could remove at most 39.6% of this stage's wall time",
  ...}]}
```
```sh
$ python scripts/analyze_job.py demo-skew-job-001 --slots 2   # same stored row
```
```json
{"findings": []}
```

Same stage, same ratio, same row in Postgres — produced through a real Kafka topic and
consumed by a real running container, not a fixture. One cluster width makes it critical; a
narrower one makes it nothing, because a 20.7× ratio needs more than 49× to be tail-bound on
2 slots. That asymmetry is the entire reason Apex's closed form exists instead of a fixed
`5×`/`10×` rule, and reproducing it end-to-end through a different stack is the proof the
architecture's lane boundaries actually hold — not just that the code compiles.

## The four lanes

| Lane | Replaces | What it does | Tests |
|---|---|---|---|
| [`capture-pyspark/`](capture-pyspark/) | `jar/` (Scala plugin) | Native `SparkListener` bridged through py4j — no JVM build step. Aggregates per-stage task-duration percentiles **in-process**, so a 10k-task stage produces one Kafka message, not 10k. | 28 |
| [`stream-kafka/`](stream-kafka/) | `collect/` (OTel Collector) | Kafka producer, routes by event type, degrades an unshaped payload to a minimal envelope instead of crashing. | 9 |
| [`store-postgres/`](store-postgres/) | `infra/` (ClickHouse) | DDL + idempotent Kafka consumer + a **verified-live Docker Compose stack** (Kafka KRaft + Postgres + a containerized consumer). | 13 unit + 1 live integration |
| [`engine-postgres/`](engine-postgres/) | `engine/` (detectors) | The tail-bound skew physics, ported near-verbatim (it's pure math — nothing stack-specific to change), reading from Postgres. | 25 |

**75 unit tests, 0 requiring live infrastructure to pass, + 1 integration test that does.**
Every lane's own README documents what it does, what it costs, and what's still missing —
written the way the rest of this is: as a decision record, not a changelog.

## Real engineering judgment calls, each with a documented trade-off

A portfolio case built only from things that worked on the first try doesn't show much. These
didn't:

1. **Percentile aggregation moved into the capture layer, mid-build.** The original plan had a
   per-task Kafka topic, with percentiles computed downstream. Before writing that consumer,
   the math made the problem obvious: a 10k-task stage would mean 10k Kafka messages for a
   number the Spark driver can compute for free. Reworked to aggregate in-process
   (`capture-pyspark/aggregator.py`), synchronously, on the same thread that receives the
   events — which only works because `onTaskEnd` is O(1) bookkeeping with no I/O. The topic was
   deleted before it ever shipped. ([stream-kafka/README.md](stream-kafka/README.md)'s revision note)

2. **Two silent Kafka failures, found running the stack for real, not in a design doc.**
   - A consumer that subscribes before its topic exists can stay blind to it for up to 5
     minutes (`kafka-python`'s metadata refresh interval) after the topic is finally
     auto-created — fixed by creating topics explicitly, before any consumer starts.
   - Kafka's internal `__consumer_offsets` topic defaults to replication factor 3, which no
     single-broker cluster can satisfy — every consumer group hangs forever with **no error
     message**, just a broker silently retrying forever. The first fix attempt (disabling
     topic auto-create) made this worse, since that flag also gates the internal topic. Root
     cause confirmed by reading broker logs, not guessed. ([store-postgres/docker-compose.yml](store-postgres/docker-compose.yml)'s comments)

3. **A scope cut stated as a cut, not hidden as a feature.** `engine-postgres/`'s detector
   cannot detect join-vs-task skew (no captured query plan yet) and has no cross-run noise-floor
   history (no history table yet). Rather than approximate either, the fix text explicitly says
   it will not recommend `spark.sql.adaptive.skewJoin.*` without plan evidence, and every
   finding takes upstream's own "no measured floor yet" confidence penalty. Documented as a
   table of missing *inputs*, not weaker *logic*. ([engine-postgres/README.md](engine-postgres/README.md))

## Stack mapping

| Role | Upstream | This branch |
|---|---|---|
| Capture | Scala, compiled JVM plugin | Python, PySpark, py4j |
| Transport | OpenTelemetry Collector | Kafka (KRaft, single-broker) |
| Store | ClickHouse + HyperDX | Postgres |
| Detection | Python (unchanged) | Python, same physics, Postgres reads |
| Local infra | Docker Compose | Docker Compose |
| Target deploy (next) | — | Databricks Job / AWS (MSK, RDS) |

See [`docs/ADAPTATION.md`](docs/ADAPTATION.md) for the lane-by-lane rationale this table
summarizes.

## What's not done yet

- `dev/`'s pathology lab (a real Spark job generating genuine skew) is not ported — every
  verification so far uses a crafted event with the right shape, not a real Spark execution.
  This is the next milestone.
- Plan-fingerprint and AQE-decision capture in `capture-pyspark/` — blocks join-vs-task skew
  detection and plan-shape memory.
- A Databricks Job / AWS deployment target for the capture side.

## Run it yourself

```sh
cd store-postgres && cp .env.example .env && make up && make smoke
```

Brings up Kafka + Postgres + the consumer, produces one event, confirms it lands. See each
lane's own README for its unit tests (`python -m pytest`, no infra needed).
