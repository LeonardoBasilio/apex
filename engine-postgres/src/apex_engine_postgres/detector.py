"""Tail-bound skew detection — the v1 scoped port of upstream engine/'s
`watchers/skew.py`, restricted to CONTRACT.md's cross-lane rule 1 (the
closed form in physics.py) alone. See README.md's "What's deferred" section
for exactly what upstream's watcher does that this one does not, and why
none of it is silently approximated instead of simply absent:

  - no join-evidence check (no plan_json/plan_fingerprint in this port's
    schema yet) -- every finding here is reported as task-level skew, never
    "SKEW_ON_JOIN", because this port cannot tell the two apart
  - no measured noise floor (no cross-run history table) -- this detector
    permanently runs upstream's own "first-ever run of this shape" code
    path, which already exists in skew.py for exactly this situation: the
    only claim that needs no reference measurement is that the tail
    DOMINATES the stage (removing it would more than halve the stage's
    wall time), gated by DOMINANT_GAIN below
  - no job_conf-driven already-active check (no job_conf table) -- the fix
    text is generic, never claims to know whether skewJoin is already on
  - no history-based attribution/credit-worthiness rule

Cluster width is always supplied by the caller
(`physics.ClusterWidth(source="operator")`) rather than read from stored
telemetry, matching upstream's own fallback path: its own tests found 0 of
51 real `job_conf` rows carried `spark.executor.instances` either, so
"an operator supplies the width" was already the common case upstream, not
a weaker one introduced by this port.
"""

from __future__ import annotations

from typing import Optional

from .physics import (
    NOT_A_DISTRIBUTION,
    SERIAL,
    SLOTS_UNKNOWN,
    WORK_BOUND,
    ClusterWidth,
    TailBound,
    UNKNOWN_WIDTH,
    evaluate_tail_bound,
)
from .schema import Finding, Severity, StageRow

NAME = "skew_watcher_v1_postgres"

MIB = 1024 * 1024

# Same measurability bound upstream uses (`MIN_BYTES_PER_TASK` in
# watchers/skew.py) -- a mechanism bound, not a noise floor: below 1 MiB/task
# the spread is JVM warm-up and scheduler jitter whatever the ratio says.
MIN_BYTES_PER_TASK = 1 * MIB

# Once a stage IS tail-bound, severity follows the SIZE OF THE WIN.
STRONG_HEADROOM = 0.25

# With no measured floor to compare a predicted win against (see module
# docstring), the only claim that needs no reference measurement is that
# the tail DOMINATES the stage: removing it could more than halve the
# stage's wall time.
DOMINANT_GAIN = 0.5

CONF_OPERATOR_STRONG = 0.80
CONF_OPERATOR_WEAK = 0.65
CONF_SLOTS_UNKNOWN = 0.50
# Always applied: this v1 port has no noise-floor history at all, so every
# finding takes upstream's own "no measured floor for this shape" penalty,
# never the floor-known score.
NO_FLOOR_PENALTY = 0.03


def evaluate(stage: StageRow, width: ClusterWidth = UNKNOWN_WIDTH) -> Optional[Finding]:
    if (
        stage.task_count is None
        or stage.task_duration_p50_ms is None
        or stage.task_duration_p99_ms is None
    ):
        return None

    tail = evaluate_tail_bound(
        n_tasks=stage.task_count,
        p50_ms=stage.task_duration_p50_ms,
        p99_ms=stage.task_duration_p99_ms,
        width=width,
    )

    if tail.verdict == NOT_A_DISTRIBUTION:
        return None
    if stage.bytes_per_task < MIN_BYTES_PER_TASK:
        return None
    if tail.verdict in (WORK_BOUND, SERIAL):
        return None

    gain_frac = tail.claimed_gain_frac
    if gain_frac is None:
        return None
    gain_pct = gain_frac * 100

    # Degenerate-wide-cluster regime (threshold <= 1, rule 6) and the
    # permanent "no floor measured yet" regime collapse to the same gate
    # here: without a reference measurement, only a dominant win survives.
    if tail.threshold is None or tail.threshold <= 1.0:
        if gain_frac <= DOMINANT_GAIN:
            return None

    slots_unknown = tail.verdict == SLOTS_UNKNOWN
    strong = not slots_unknown and (tail.headroom_frac or 0) >= STRONG_HEADROOM
    severity = Severity.CRITICAL if strong else Severity.WARNING
    confidence_score = _confidence(strong, slots_unknown)

    return Finding(
        job_id=stage.job_id,
        stage_id=stage.stage_id,
        stage_attempt=stage.stage_attempt,
        finding_type="TASK_SKEW",
        severity=severity,
        confidence_score=confidence_score,
        evidence=_evidence(stage, tail, gain_pct),
        impact=_impact(tail, gain_pct),
        fix=_fix(),
        details={
            "skew_ratio": tail.ratio,
            "task_count": stage.task_count,
            "task_duration_p50_ms": stage.task_duration_p50_ms,
            "task_duration_p99_ms": stage.task_duration_p99_ms,
            "bytes_per_task": stage.bytes_per_task,
            "tail_bound_verdict": tail.verdict,
            "tail_bound_threshold": tail.threshold,
            "tail_bound_margin": tail.margin,
            "slots": width.slots,
            "slots_source": width.source,
            "min_slots_required": tail.min_slots_required,
            "headroom_frac_upper_bound": tail.headroom_frac,
            "headroom_frac_width_free": tail.headroom_upper_frac,
        },
    )


def _confidence(strong: bool, slots_unknown: bool) -> float:
    if slots_unknown:
        return CONF_SLOTS_UNKNOWN
    score = CONF_OPERATOR_STRONG if strong else CONF_OPERATOR_WEAK
    return round(score - NO_FLOOR_PENALTY, 2)


def _evidence(stage: StageRow, tail: TailBound, gain_pct: float) -> str:
    basis = "at this width" if tail.headroom_frac is not None else "at ANY width"
    return "; ".join(
        [
            f"p99/p50 = {tail.ratio:.2f}x on stage {stage.stage_id} "
            f"(p99={stage.task_duration_p99_ms:.0f}ms, p50={stage.task_duration_p50_ms:.0f}ms, "
            f"{stage.task_count} tasks, {int(stage.bytes_per_task):,} bytes/task)",
            tail.explain(),
            f"an ideal rebalance could remove at most {gain_pct:.1f}% of this stage's "
            f"wall time (upper bound {basis})",
        ]
    )


def _impact(tail: TailBound, gain_pct: float) -> str:
    if tail.verdict == SLOTS_UNKNOWN:
        return (
            "The stage's slowest task is far slower than its median. Whether that tail "
            "actually bounds the stage depends on cluster width, which this run did not "
            f"supply, so the cost of the tail cannot be quantified precisely here -- at "
            f"most {gain_pct:.1f}% could ever be reclaimed, on any cluster width."
        )
    return (
        f"The stage cannot finish before its slowest partition: removing the tail could "
        f"return at most {gain_pct:.1f}% of the stage's wall time. No run-to-run history "
        "exists in this v1 port to say how much of that is resolvable by replay (see "
        "README's noise-floor gap) -- this is reported as a mechanism finding, not a "
        "certified runtime win."
    )


def _fix() -> str:
    return (
        "This v1 port has no captured query plan, so it cannot distinguish task-level "
        "skew from join skew (see README) and will not recommend spark.sql.adaptive."
        "skewJoin.* on that basis. Rebalance the input generically: repartition on a "
        "higher-cardinality key, split the oversized input partition, or salt the "
        "grouping/join key if a join is suspected."
    )
