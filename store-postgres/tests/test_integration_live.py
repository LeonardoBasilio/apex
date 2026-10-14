"""Live integration smoke test against `docker compose up` (see
docker-compose.yml). Skips rather than fails when the stack isn't running --
same rule upstream states for its own ClickHouse-dependent tests: this must
stay green with no infrastructure up, so `make test` / CI never needs Docker
to pass. Set POSTGRES_DSN and KAFKA_BOOTSTRAP_SERVERS to actually run it
(see README.md's "Run against Docker Compose" section).
"""

from __future__ import annotations

import json
import os
import time
import uuid

import pytest

pytest.importorskip("kafka")
pytest.importorskip("psycopg")

POSTGRES_DSN = os.environ.get("POSTGRES_DSN")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")

pytestmark = pytest.mark.skipif(
    not (POSTGRES_DSN and KAFKA_BOOTSTRAP_SERVERS),
    reason=(
        "set POSTGRES_DSN and KAFKA_BOOTSTRAP_SERVERS against a running "
        "`docker compose up` stack to run this test (see README.md)"
    ),
)


def test_a_produced_stage_event_is_consumed_and_lands_in_postgres():
    from kafka import KafkaConsumer, KafkaProducer

    from apex_store.connection import from_dsn
    from apex_store.consumer import STAGE_EVENTS_TOPIC, StageEventConsumer
    from apex_store.migrate import apply_ddl

    connection = from_dsn(POSTGRES_DSN)
    apply_ddl(connection)

    job_id = f"smoke-{uuid.uuid4()}"
    payload = {
        "job_id": job_id,
        "app_id": job_id,
        "stage_id": 1,
        "stage_attempt": 0,
        "ts": int(time.time() * 1000),
        "task_count": 10,
        "task_duration_p50_ms": 12.5,
        "task_duration_p99_ms": 99.9,
        "task_duration_max_ms": 120.0,
        "task_duration_sample_count": 10,
        "shuffle_read_bytes": 1000,
        "shuffle_write_bytes": 2000,
        "spill_disk_bytes": 0,
        "spill_mem_bytes": 0,
        "gc_time_ms": 5,
        "input_bytes": 500,
        "output_bytes": 600,
    }

    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS.split(",")
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer.send(STAGE_EVENTS_TOPIC, json.dumps(payload).encode("utf-8"))
    producer.flush()
    producer.close()

    kafka_consumer = KafkaConsumer(
        STAGE_EVENTS_TOPIC,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=20_000,
        group_id=f"smoke-{job_id}",
    )
    stage_consumer = StageEventConsumer(connection)
    for message in kafka_consumer:
        stage_consumer.consume_one(message)
        if stage_consumer.stats().inserted >= 1:
            break
    kafka_consumer.close()

    assert stage_consumer.stats().inserted == 1

    row = connection.fetchone(
        "SELECT stage_id, task_duration_p99_ms FROM stage_events WHERE job_id = %(job_id)s",
        {"job_id": job_id},
    )
    assert row == (1, 99.9)
