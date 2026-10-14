# dev-pyspark

Replaces upstream [`dev/`](../dev/) (a Delta/MinIO pathology lab) in this portfolio port with
the minimum needed to run a **real** Spark job through
[`capture-pyspark/`](../capture-pyspark/)'s listener into the live
[`stream-kafka/`](../stream-kafka/) → [`store-postgres/`](../store-postgres/) chain — no S3,
no Delta, just a hot key.

## What it generates

[`jobs/skew_join_job.py`](jobs/skew_join_job.py): a 2,000,000-row fact table where 90% of rows
share one join key, sort-merge joined (broadcast disabled) against a 10-row dimension table.
Attaches `capture-pyspark`'s listener and `stream-kafka`'s producer, runs the join for real,
then shuts both down cleanly.

## Three things found running this for real, not designed around in advance

1. **py4j's callback server needs more than `start_callback_server()`.** Documented in
   [`capture-pyspark/listener.py`](../capture-pyspark/listener.py) now — the JVM gateway is
   created already pointed at a fixed (usually disabled) callback port; starting the server
   afterward opens a socket the JVM was never told about. Every callback failed with
   `Connection refused`, silently, forever, until switched to PySpark's own
   `ensure_callback_server_started` (the same helper `StreamingQueryListener` uses
   internally), which actually resets the JVM-side `CallbackClient`'s target port.

2. **AQE's own partition coalescing can erase the skew before this port ever measures it** —
   CONTRACT.md's rule 7, reproduced live rather than as a thought experiment. The job's first
   version (`groupBy("key").count()`, AQE coalescing left on) merged all 8 shuffle partitions
   down to a single task, because so little data survived partial aggregation that AQE judged
   one partition sufficient. `spark.sql.adaptive.coalescePartitions.enabled=false` is set
   explicitly so the distribution captured in `stage_events` is the one that actually ran, not
   the one AQE already "fixed" by the time a listener could see it.

3. **`.count()` needs no column values, so Catalyst prunes them before any shuffle.** Adding a
   high-entropy `payload` column didn't change shuffle bytes at all on the first attempt — the
   final action never referenced it, so it was eliminated before the exchange. Aggregating
   `F.sum(F.length("payload"))` instead forces it to survive pruning, which is what makes the
   skew show up in `shuffle_read_bytes`/`shuffle_write_bytes` and not just task duration.

## Verified live

```sh
cd ../store-postgres && cp .env.example .env && docker compose up -d --wait   # kafka+postgres+consumer
cd ../dev-pyspark && docker compose build skew-join-job
docker compose run --rm skew-join-job
```

A real run (`job_id=local-1791941143879`) landed this row in `stage_events` — AQE genuinely
split the skewed partition, raising stage 4's task count from 8 to 9:

| stage_id | task_count | p50 | p99 | shuffle_read_bytes |
|---|---|---|---|---|
| 4 | 9 | 398ms | 2033.6ms | 128,470,756 |

Running [`engine-postgres/`](../engine-postgres/)'s detector against that exact stored row:

```sh
POSTGRES_DSN=... python ../engine-postgres/scripts/analyze_job.py local-1791941143879 --slots 8
# -> CRITICAL, tail_bound, 78.0% headroom, margin 4.47x
POSTGRES_DSN=... python ../engine-postgres/scripts/analyze_job.py local-1791941143879 --slots 2
# -> no findings (WORK_BOUND: 5.11x ratio needs >8x to be tail-bound on 2 slots)
```

Same real job, same real row, opposite verdict depending on cluster width — the same physics
[`PORTFOLIO.md`](../PORTFOLIO.md) demonstrates with a crafted event, now from an actual Spark
execution.

The real run also produced a genuine AQE `skew_split` transition, captured by
`capture-pyspark/aqe.py` and delivered through `apex.spark.plan_events`:

```json
{
  "execution_id": 0, "update_seq": 2, "transition_type": "skew_split",
  "detail": "SortMergeJoin(skew=true) -- AQEShuffleRead split a skewed partition",
  "before": "SortMergeJoin", "after": "SortMergeJoin(skew=true)", "confidence": "HIGH"
}
```

## Status

Done: a real, reproducible skewed job, captured end to end. Not done: `join_switch` and
`coalesce` AQE transition detection are implemented from upstream's documented conventions but
**not exercised by a real run in this session** (this job's conf disables broadcast joins and
never shrinks partition counts) — see `capture-pyspark/aqe.py`'s module docstring. `plan_events`
land in Kafka but `store-postgres/` doesn't consume that topic yet (no DDL for it), so AQE
transitions aren't queryable from Postgres, only observable on the wire.
