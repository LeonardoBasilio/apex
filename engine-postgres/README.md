# engine-postgres

A scoped v1 port of upstream [`engine/`](../engine/)'s tail-bound skew detector
(`watchers/skew.py` + `physics.py`), reading from
[`store-postgres/`](../store-postgres/)'s `stage_events` table instead of
ClickHouse. This is the lane that proves the whole chain holds: a stage shaped by
[`capture-pyspark/`](../capture-pyspark/), transported by
[`stream-kafka/`](../stream-kafka/), stored by `store-postgres/`, is enough to
re-run CONTRACT.md's cross-lane rule 1 and get the same verdict upstream's own
tests anchor to real measured runs.

## What's ported close to verbatim

**`physics.py`** — the closed form itself:

> tail-bound ⟺ p99/p50 > (n_tasks − 1) / (slots − 1)

This is pure math with zero I/O and zero ClickHouse dependency, so there was
nothing stack-specific to adapt — it's reproduced here (not imported; lanes in
this port don't import each other's packages) field-for-field identical to
upstream's. `tests/test_physics.py` carries over upstream's own anchors: the
21.62x/50-task/2-slot false positive (needs > 49x, is WORK_BOUND), the balanced
control stage's 7–12x jitter that must never pass a 28.43x bar, a genuinely
skewed 17–20x stage that always does.

## What's deferred — and why none of it is silently approximated

Upstream's watcher does four things this v1 port does not. Each is a missing
*input*, not a weaker implementation of the same idea — the detector simply
reports what it can prove from what it has:

| Upstream has | This port has | Consequence |
|---|---|---|
| `plan_json`/`plan_fingerprint` → join-evidence check | Nothing (not in `stage_events` yet — `capture-pyspark/`'s plan extraction is deferred, see its README) | Every finding is `TASK_SKEW`, **never** `SKEW_ON_JOIN`. The fix text says so explicitly and will not recommend `spark.sql.adaptive.skewJoin.*` on no evidence — that's the exact false-positive class upstream's watcher was rebuilt to stop, and this port has even less basis to make that claim than upstream's pre-rebuild version did. |
| Cross-run noise-floor history (rule 2) | Nothing (no history table) | This detector permanently runs upstream's own "first-ever run of this shape" code path — the one case upstream's `skew.py` already handles when no floor exists yet. The only claim that needs no reference measurement survives: that the tail *dominates* the stage (`DOMINANT_GAIN`, removing it would more than halve the wall time). Every finding also takes upstream's floor-unknown confidence penalty, never the floor-known score. |
| `job_conf` table → cluster width + skewJoin no-op check | Width is always supplied by the caller (`physics.ClusterWidth(source="operator")`) | Not actually a weaker path: upstream's own tests found **0 of 51 real `job_conf` rows** carried `spark.executor.instances` either, so "an operator supplies the width" was already the common case upstream, not one this port introduced. |
| History-based attribution/credit-worthiness (rule 3) | Nothing | Out of scope entirely for v1 — there is no second run to compare against yet. |

## Verified live, end to end

Not just unit-tested — run against a real row, through the real chain:

```sh
# 1. produce a genuinely skewed stage event (p99/p50 = 20.7x, 100 tasks,
#    ~1.08 MiB/task) into the real Kafka topic, same as stream-kafka/ would
# 2. store-postgres/'s running consumer inserts it
# 3.
POSTGRES_DSN=postgresql://apex:apex_local_dev@localhost:5432/apex \
  python scripts/analyze_job.py demo-skew-job-001 --slots 8
```

```json
{
  "findings": [{
    "severity": "critical",
    "evidence": "p99/p50 = 20.70x ... > (n-1)/(slots-1) = (100-1)/(8-1) = 14.14 [operator]; ...",
    ...
  }]
}
```

Re-run the *same stored row* with `--slots 2` instead, and the finding
disappears — `20.7x` needs `> 49x` to be tail-bound on 2 slots, so the stage is
`WORK_BOUND` and a perfect skew fix would return nothing. Same data, same
detector, different cluster width, opposite verdict — which is the entire
point of rule 1 existing: a fixed `5x`/`10x` threshold would have called both
cases "critical."

## Implemented

- **`physics.py`** — the closed form, ported verbatim (see above).
- **`schema.py`** — `StageRow` (mirrors `store-postgres/ddl/001_stage_events.sql`
  field-for-field) and `Finding`.
- **`query.py`** — `fetch_stages(fetcher, job_id)`; a plain `SELECT` is enough
  here (no ClickHouse `argMax` equivalent needed — `stage_events`' primary key
  is already one settled row per stage attempt).
- **`detector.py`** — `evaluate(stage, width)`, the scoped skew watcher
  described above.
- **`connection.py`** — thin read-only `psycopg` wrapper; this lane only
  `SELECT`s (`store-postgres/` owns all writes and DDL).
- **`scripts/analyze_job.py`** — the CLI used in the verification above.

25 tests, no Postgres server needed to run them.

## Status

Done: the closed form, the scoped detector, the read path, live verification
against a real stage event through the full chain. Not done: plan-evidence
join detection, noise-floor history, job_conf-sourced width, attribution — all
require their own upstream `jar/`-equivalent captures first (tracked in
`capture-pyspark/README.md`'s "deferred" list), not an engine-side fix.
