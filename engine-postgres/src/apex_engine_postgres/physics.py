"""The closed form. CONTRACT.md cross-lane rule 1, and nothing else.

Ported close to verbatim from upstream's `engine/src/apex_engine/physics.py`
(this is pure math with zero I/O and zero dependency on ClickHouse, so there
was nothing stack-specific to adapt). Reproduced here, not imported, because
this port's lanes don't import each other's packages — only the field shapes
`store-postgres/`'s schema defines are shared.

    tail-bound  <=>  p99/p50 > (n_tasks - 1) / (slots - 1)

This is a list-scheduling makespan bound, not a heuristic. Data volume
CANCELS OUT of it (volume scales p50 and p99 together), so the threshold
depends only on how many tasks the stage has and how wide the cluster is.
Two consequences a fixed `5x`/`10x` threshold gets wrong in both directions:

  * a 21.6x ratio on 50 tasks / 2 slots needs > 49x to be tail-bound -- a
    fixed threshold calls it CRITICAL, the physics says the stage is
    WORK-bound and a perfect skew fix returns nothing;
  * a 1.5x ratio on 50 tasks / 50 slots needs only > 1x -- a fixed
    threshold never looks at it.

`slots` is an OBSERVATION. It is never guessed and never inferred from
`spark.sql.shuffle.partitions`, which is a partition count, not a cluster
width. This v1 port has no `job_conf`-equivalent table (see
detector.py's README note), so `slots` always arrives from the caller
(`ClusterWidth(source="operator")`), never read off stored telemetry --
upstream's own tests show 0 of 51 real `job_conf` rows carried
`spark.executor.instances` either, so "an operator supplies it" is not a
weaker path, it's the common one. When unknown, the closed form is not
evaluable, and this module says so rather than substituting a number:
`verdict = slots_unknown` carries `min_slots_required`, the width at which
the stage WOULD become tail-bound, derived from the observation itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# A p99 over a handful of tasks is not a distribution.
MIN_TASKS_FOR_RATIO = 4

# Verdicts. Exhaustive; `TailBound.verdict` is always one of these.
TAIL_BOUND = "tail_bound"
WORK_BOUND = "work_bound"
SLOTS_UNKNOWN = "slots_unknown"
SERIAL = "serial"
NOT_A_DISTRIBUTION = "not_a_distribution"


@dataclass(frozen=True)
class ClusterWidth:
    """Observed cluster width, with provenance. `slots is None` means UNKNOWN."""

    slots: Optional[int] = None
    source: str = "unknown"
    detail: str = "cluster width not determinable from available telemetry"

    @property
    def known(self) -> bool:
        return self.slots is not None and self.slots > 0

    def __str__(self) -> str:
        return f"{self.slots} slots ({self.source})" if self.known else f"unknown ({self.detail})"


UNKNOWN_WIDTH = ClusterWidth()


def operator_width(slots: int) -> ClusterWidth:
    return ClusterWidth(slots=slots, source="operator", detail="supplied by the caller")


@dataclass(frozen=True)
class TailBound:
    """The verdict of rule 1 for one stage, with every input kept visible."""

    n_tasks: int
    ratio: float
    width: ClusterWidth
    threshold: Optional[float]          # (n-1)/(slots-1); None when not evaluable
    verdict: str
    margin: Optional[float]             # ratio / threshold; > 1 means tail-bound
    min_slots_required: Optional[float]  # break-even width for THIS observation
    headroom_frac: Optional[float]      # upper bound on the fraction of stage wall
    #                                     time a perfect rebalance could remove
    headroom_upper_frac: Optional[float]  # the same bound in the slots >= n_tasks
    #                                       limit: width-free, so it survives an
    #                                       unknown cluster width

    @property
    def is_tail_bound(self) -> bool:
        return self.verdict == TAIL_BOUND

    @property
    def claimed_gain_frac(self) -> Optional[float]:
        """The largest win this finding could honestly claim: width-aware when
        the width is known, the width-FREE bound otherwise."""
        return self.headroom_frac if self.headroom_frac is not None else self.headroom_upper_frac

    def explain(self) -> str:
        """One clause, quoting the physics -- safe to paste into evidence."""
        if self.verdict == NOT_A_DISTRIBUTION:
            return (
                f"{self.n_tasks} tasks is below the {MIN_TASKS_FOR_RATIO} needed for a p99 "
                f"to describe a distribution"
            )
        if self.verdict == SERIAL:
            return (
                f"cluster width is {self.width.slots} slot: tasks run serially, so the stage "
                f"is work-bound by construction and no tail can dominate it"
            )
        if self.verdict == SLOTS_UNKNOWN:
            need = f"{self.min_slots_required:.1f}" if self.min_slots_required else "?"
            return (
                f"cluster width is {self.width.detail}, so (n-1)/(slots-1) is not evaluable; "
                f"this {self.ratio:.2f}x over {self.n_tasks} tasks is tail-bound only on a "
                f"cluster of more than {need} slots"
            )
        cmp = ">" if self.is_tail_bound else "<="
        return (
            f"p99/p50 = {self.ratio:.2f}x {cmp} (n-1)/(slots-1) = "
            f"({self.n_tasks}-1)/({self.width.slots}-1) = {self.threshold:.2f} "
            f"[{self.width.source}]"
        )


def tail_bound_threshold(n_tasks: int, slots: Optional[int]) -> Optional[float]:
    """(n_tasks - 1) / (slots - 1), or None when that is not a number."""
    if slots is None or slots <= 1 or n_tasks < 2:
        return None
    return (n_tasks - 1) / (slots - 1)


def min_slots_for_tail_bound(n_tasks: int, ratio: float) -> Optional[float]:
    """Break-even cluster width: solve ratio > (n-1)/(s-1) for s."""
    if ratio <= 0 or n_tasks < 2:
        return None
    return 1.0 + (n_tasks - 1) / ratio


def headroom_fraction(
    n_tasks: int, p50_ms: float, p99_ms: float, slots: Optional[int]
) -> Optional[float]:
    """UPPER bound on what removing the tail could return, as a fraction."""
    if slots is None or slots < 1 or n_tasks < 1 or p99_ms <= 0 or p50_ms <= 0:
        return None
    balanced_ms = max(n_tasks * p50_ms / slots, p50_ms)
    if balanced_ms >= p99_ms:
        return 0.0
    return 1.0 - balanced_ms / p99_ms


def evaluate_tail_bound(
    *,
    n_tasks: int,
    p50_ms: float,
    p99_ms: float,
    width: ClusterWidth = UNKNOWN_WIDTH,
) -> TailBound:
    """Apply rule 1 to one stage. Pure; the only place the closed form is used."""
    ratio = p99_ms / p50_ms if p50_ms > 0 else 0.0
    min_slots = min_slots_for_tail_bound(n_tasks, ratio)
    threshold = tail_bound_threshold(n_tasks, width.slots)
    headroom = headroom_fraction(n_tasks, p50_ms, p99_ms, width.slots)
    headroom_upper = headroom_fraction(n_tasks, p50_ms, p99_ms, n_tasks)

    if n_tasks < MIN_TASKS_FOR_RATIO or ratio <= 0:
        verdict = NOT_A_DISTRIBUTION
    elif not width.known:
        verdict = SLOTS_UNKNOWN
    elif width.slots == 1:
        verdict = SERIAL
    elif threshold is not None and ratio > threshold:
        verdict = TAIL_BOUND
    else:
        verdict = WORK_BOUND

    return TailBound(
        n_tasks=n_tasks,
        ratio=ratio,
        width=width,
        threshold=threshold,
        verdict=verdict,
        margin=(ratio / threshold) if threshold else None,
        min_slots_required=min_slots,
        headroom_frac=headroom,
        headroom_upper_frac=headroom_upper,
    )
