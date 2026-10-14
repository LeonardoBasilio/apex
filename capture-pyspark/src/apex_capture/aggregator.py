"""Per-stage task-duration aggregation, computed in-process at capture time
instead of shipping one Kafka message per task (see stream-kafka/README.md's
revision note). A stage can have tens of thousands of tasks; the only thing
any consumer in this port needs from them is p50/p99/max/count, so this
collapses N task events into one number set, synchronously, on the same
listener-bus thread that delivers onTaskEnd/onStageCompleted. This must stay
O(1)-ish work per event, never a sort-on-every-task, or it reintroduces the
exact blocking risk capture-pyspark/listener.py exists to avoid.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from typing import Dict, List, Tuple

StageKey = Tuple[int, int]  # (stage_id, stage_attempt)


@dataclass(frozen=True)
class StageStats:
    p50_ms: float
    p99_ms: float
    max_ms: float
    count: int


ZERO_STATS = StageStats(p50_ms=0.0, p99_ms=0.0, max_ms=0.0, count=0)


class StageAggregator:
    """Not thread-safe by design: Spark's SparkListenerBus dispatches to a
    given listener sequentially from one thread, so onTaskEnd/onStageCompleted
    calls for the SAME listener instance never race each other. A lock here
    would be dead weight on the hot path, so it is deliberately absent."""

    def __init__(self) -> None:
        self._durations: Dict[StageKey, List[float]] = {}

    def on_task_end(self, key: StageKey, duration_ms: float) -> None:
        bucket = self._durations.setdefault(key, [])
        bisect.insort(bucket, duration_ms)

    def pop_stage(self, key: StageKey) -> StageStats:
        """Returns stats for `key` and discards its state — a given stage
        attempt completes exactly once, so there is nothing to retain after.
        Returns ZERO_STATS if no onTaskEnd was ever observed for this key."""
        durations = self._durations.pop(key, None)
        if not durations:
            return ZERO_STATS
        return StageStats(
            p50_ms=_percentile(durations, 0.50),
            p99_ms=_percentile(durations, 0.99),
            max_ms=durations[-1],
            count=len(durations),
        )

    def pending_stage_count(self) -> int:
        """Stage attempts with buffered durations awaiting completion —
        exposed so a long-running job can be checked for a leak (a stage key
        observed via onTaskEnd that never reaches onStageCompleted)."""
        return len(self._durations)


def _percentile(sorted_values: List[float], p: float) -> float:
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = p * (len(sorted_values) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(sorted_values) - 1)
    frac = rank - lower
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * frac
