"""Reads store-postgres/'s `stage_events` table for one job. The only query
this v1 port needs: upstream's ClickHouse version uses `argMax(..., ts)` per
stage because its MergeTree table can hold more than one row describing the
"current" state of a stage; store-postgres's PRIMARY KEY is already
(job_id, stage_id, stage_attempt), one settled row per attempt, so a plain
SELECT is enough -- no argMax equivalent needed.
"""

from __future__ import annotations

from typing import List, Optional, Protocol

from .schema import StageRow

SELECT_STAGES_FOR_JOB = """
SELECT job_id, app_id, stage_id, stage_attempt, task_count,
       task_duration_p50_ms, task_duration_p99_ms, task_duration_max_ms,
       task_duration_sample_count, shuffle_read_bytes, shuffle_write_bytes,
       spill_disk_bytes, spill_mem_bytes, gc_time_ms, input_bytes, output_bytes
FROM stage_events
WHERE job_id = %(job_id)s
ORDER BY stage_id, stage_attempt DESC
"""


class RowFetcher(Protocol):
    def fetch_all(self, sql: str, params: Optional[dict] = None) -> List[tuple]: ...


def fetch_stages(fetcher: RowFetcher, job_id: str) -> List[StageRow]:
    rows = fetcher.fetch_all(SELECT_STAGES_FOR_JOB, {"job_id": job_id})
    return [
        StageRow(
            job_id=row[0],
            app_id=row[1],
            stage_id=row[2],
            stage_attempt=row[3],
            task_count=row[4],
            task_duration_p50_ms=row[5],
            task_duration_p99_ms=row[6],
            task_duration_max_ms=row[7],
            task_duration_sample_count=row[8],
            shuffle_read_bytes=row[9],
            shuffle_write_bytes=row[10],
            spill_disk_bytes=row[11],
            spill_mem_bytes=row[12],
            gc_time_ms=row[13],
            input_bytes=row[14],
            output_bytes=row[15],
        )
        for row in rows
    ]
