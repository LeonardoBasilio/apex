"""Field extraction from Spark's JVM listener events into the stage event
this port emits, scoped to a v1 subset of CONTRACT.md's "stage metrics" field
set (identity + core metrics; shuffle/spill/gc/input/output + percentiles
computed by aggregator.py — the v0.5/v0.6 retry-safe fields, plan fingerprint,
and AQE plan-transition fields are explicitly deferred, see
stream-kafka/README.md's Status note).

Every accessor here crosses into JVM objects py4j wraps, which have not been
exercised against a live Spark cluster yet (same gap capture-pyspark/README.md
already calls out for the listener itself). `_get` walks a chain of zero-arg
method calls and returns None at the first missing/raising one, so a single
renamed method (version drift) or an event shape with no SQL metrics (a pure
RDD stage) degrades that one field to None instead of losing the whole event —
this repo's "no silent green" rule, applied to a missing field staying
visibly None in the output rather than being silently coerced to 0.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple

from .aggregator import StageStats

StageKey = Tuple[int, int]


def _get(obj: Any, *calls: str) -> Optional[Any]:
    current = obj
    for call in calls:
        if current is None:
            return None
        try:
            current = getattr(current, call)()
        except Exception:
            return None
    return current


def extract_task_duration_ms(task_end_event: Any) -> Optional[float]:
    duration = _get(task_end_event, "taskInfo", "duration")
    return float(duration) if duration is not None else None


def extract_stage_key(event: Any) -> Optional[StageKey]:
    """Works for both SparkListenerTaskEnd (stageId/stageAttemptId are direct
    zero-arg fields on the event) and SparkListenerStageCompleted (nested
    one level down, under .stageInfo())."""
    stage_id = _get(event, "stageId")
    attempt = _get(event, "stageAttemptId")
    if stage_id is not None and attempt is not None:
        return (int(stage_id), int(attempt))

    stage_info = _get(event, "stageInfo")
    stage_id = _get(stage_info, "stageId")
    attempt = _get(stage_info, "attemptNumber")
    if stage_id is not None and attempt is not None:
        return (int(stage_id), int(attempt))
    return None


def build_stage_contract_event(
    *,
    job_id: str,
    app_id: str,
    stage_completed_event: Any,
    stats: StageStats,
    ts_millis: int,
) -> dict:
    stage_info = _get(stage_completed_event, "stageInfo")
    task_metrics = _get(stage_info, "taskMetrics")

    return {
        "job_id": job_id,
        "app_id": app_id,
        "stage_id": _get(stage_info, "stageId"),
        "stage_attempt": _get(stage_info, "attemptNumber"),
        "ts": ts_millis,
        "task_count": _get(stage_info, "numTasks"),
        "task_duration_p50_ms": stats.p50_ms,
        "task_duration_p99_ms": stats.p99_ms,
        "task_duration_max_ms": stats.max_ms,
        "task_duration_sample_count": stats.count,
        "shuffle_read_bytes": _get(task_metrics, "shuffleReadMetrics", "totalBytesRead"),
        "shuffle_write_bytes": _get(task_metrics, "shuffleWriteMetrics", "bytesWritten"),
        "spill_disk_bytes": _get(task_metrics, "diskBytesSpilled"),
        "spill_mem_bytes": _get(task_metrics, "memoryBytesSpilled"),
        "gc_time_ms": _get(task_metrics, "jvmGCTime"),
        "input_bytes": _get(task_metrics, "inputMetrics", "bytesRead"),
        "output_bytes": _get(task_metrics, "outputMetrics", "bytesWritten"),
    }
