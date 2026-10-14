"""Kafka -> Postgres consumer. Reads only `apex.spark.stage_events` (see
stream-kafka/README.md) -- `apex.spark.plan_events` exists on the wire but
has no extraction built yet (capture-pyspark/README.md's Status note), so
there is nothing shaped to insert from it; this consumer ignores that topic
rather than inserting a schema it would have to guess at.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Protocol

STAGE_EVENTS_TOPIC = "apex.spark.stage_events"

REQUIRED_FIELDS = {"job_id", "app_id", "stage_id", "stage_attempt"}

INSERT_SQL = """
INSERT INTO stage_events (
    job_id, app_id, stage_id, stage_attempt, ts,
    task_count, task_duration_p50_ms, task_duration_p99_ms,
    task_duration_max_ms, task_duration_sample_count,
    shuffle_read_bytes, shuffle_write_bytes,
    spill_disk_bytes, spill_mem_bytes, gc_time_ms,
    input_bytes, output_bytes
) VALUES (
    %(job_id)s, %(app_id)s, %(stage_id)s, %(stage_attempt)s, %(ts)s,
    %(task_count)s, %(task_duration_p50_ms)s, %(task_duration_p99_ms)s,
    %(task_duration_max_ms)s, %(task_duration_sample_count)s,
    %(shuffle_read_bytes)s, %(shuffle_write_bytes)s,
    %(spill_disk_bytes)s, %(spill_mem_bytes)s, %(gc_time_ms)s,
    %(input_bytes)s, %(output_bytes)s
)
ON CONFLICT (job_id, stage_id, stage_attempt) DO NOTHING
"""


class DBConnection(Protocol):
    def execute(self, sql: str, params: Optional[dict] = None) -> Any: ...

    def commit(self) -> None: ...


class KafkaMessage(Protocol):
    topic: str
    value: bytes


@dataclass
class ConsumeStats:
    inserted: int = 0
    skipped_malformed: int = 0
    ignored_other_topic: int = 0


class StageEventConsumer:
    """(job_id, stage_id, stage_attempt) is the idempotency key: ON CONFLICT
    DO NOTHING makes a redelivered message (consumer restart before an
    offset commit -- Kafka's at-least-once default) a no-op instead of a
    duplicate row, since a given stage attempt of a given run completes
    exactly once."""

    def __init__(self, connection: DBConnection) -> None:
        self._connection = connection
        self._stats = ConsumeStats()

    def consume_one(self, message: KafkaMessage) -> None:
        if message.topic != STAGE_EVENTS_TOPIC:
            self._stats.ignored_other_topic += 1
            return

        try:
            payload = json.loads(message.value)
        except (json.JSONDecodeError, TypeError):
            self._stats.skipped_malformed += 1
            return

        if not isinstance(payload, dict) or not REQUIRED_FIELDS.issubset(payload):
            self._stats.skipped_malformed += 1
            return

        params = {
            "job_id": payload.get("job_id"),
            "app_id": payload.get("app_id"),
            "stage_id": payload.get("stage_id"),
            "stage_attempt": payload.get("stage_attempt"),
            "ts": payload.get("ts"),
            "task_count": payload.get("task_count"),
            "task_duration_p50_ms": payload.get("task_duration_p50_ms"),
            "task_duration_p99_ms": payload.get("task_duration_p99_ms"),
            "task_duration_max_ms": payload.get("task_duration_max_ms"),
            "task_duration_sample_count": payload.get("task_duration_sample_count"),
            "shuffle_read_bytes": payload.get("shuffle_read_bytes"),
            "shuffle_write_bytes": payload.get("shuffle_write_bytes"),
            "spill_disk_bytes": payload.get("spill_disk_bytes"),
            "spill_mem_bytes": payload.get("spill_mem_bytes"),
            "gc_time_ms": payload.get("gc_time_ms"),
            "input_bytes": payload.get("input_bytes"),
            "output_bytes": payload.get("output_bytes"),
        }
        self._connection.execute(INSERT_SQL, params)
        self._connection.commit()
        self._stats.inserted += 1

    def consume_many(self, messages: Iterable[KafkaMessage]) -> None:
        for message in messages:
            self.consume_one(message)

    def stats(self) -> ConsumeStats:
        return ConsumeStats(
            inserted=self._stats.inserted,
            skipped_malformed=self._stats.skipped_malformed,
            ignored_other_topic=self._stats.ignored_other_topic,
        )
