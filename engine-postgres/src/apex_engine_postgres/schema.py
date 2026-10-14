"""Types this lane's detector reads and produces. `StageRow` mirrors
store-postgres/ddl/001_stage_events.sql field-for-field -- the shared shape
between the two lanes, not a shared import.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass(frozen=True)
class StageRow:
    job_id: str
    app_id: str
    stage_id: int
    stage_attempt: int
    task_count: Optional[int]
    task_duration_p50_ms: Optional[float]
    task_duration_p99_ms: Optional[float]
    task_duration_max_ms: Optional[float]
    task_duration_sample_count: Optional[int]
    shuffle_read_bytes: Optional[int]
    shuffle_write_bytes: Optional[int]
    spill_disk_bytes: Optional[int]
    spill_mem_bytes: Optional[int]
    gc_time_ms: Optional[int]
    input_bytes: Optional[int]
    output_bytes: Optional[int]

    @property
    def bytes_per_task(self) -> float:
        """Shuffle + input bytes touched, per task. 0 when task_count is
        unknown/zero -- deliberately not None, since detector.py's
        MIN_BYTES_PER_TASK gate needs a comparable number and "0 bytes
        moved" is the correct read of a stage with no task count."""
        if not self.task_count:
            return 0.0
        touched = (self.shuffle_read_bytes or 0) + (self.shuffle_write_bytes or 0) + (
            self.input_bytes or 0
        )
        return touched / self.task_count


@dataclass(frozen=True)
class Finding:
    job_id: str
    stage_id: int
    stage_attempt: int
    finding_type: str
    severity: Severity
    confidence_score: float
    evidence: str
    impact: str
    fix: str
    details: dict[str, Any]
