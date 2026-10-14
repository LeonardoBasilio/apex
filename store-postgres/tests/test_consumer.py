import json

from apex_store.consumer import STAGE_EVENTS_TOPIC, StageEventConsumer
from fakes import FakeConnection, FakeMessage


def _stage_event_message(**overrides) -> FakeMessage:
    payload = {
        "job_id": "app-20260729182801-0045",
        "app_id": "app-20260729182801-0045",
        "stage_id": 29,
        "stage_attempt": 0,
        "ts": 1_753_815_000_000,
        "task_count": 114,
        "task_duration_p50_ms": 120.0,
        "task_duration_p99_ms": 980.0,
        "task_duration_max_ms": 1200.0,
        "task_duration_sample_count": 114,
        "shuffle_read_bytes": 113_632_037,
        "shuffle_write_bytes": 0,
        "spill_disk_bytes": 0,
        "spill_mem_bytes": 0,
        "gc_time_ms": 40,
        "input_bytes": 0,
        "output_bytes": 0,
    }
    payload.update(overrides)
    return FakeMessage(topic=STAGE_EVENTS_TOPIC, value=json.dumps(payload).encode("utf-8"))


def test_valid_stage_event_is_inserted_and_committed():
    connection = FakeConnection()
    consumer = StageEventConsumer(connection)

    consumer.consume_one(_stage_event_message())

    assert consumer.stats().inserted == 1
    assert connection.commit_count == 1
    sql, params = connection.executed[0]
    assert "ON CONFLICT (job_id, stage_id, stage_attempt) DO NOTHING" in sql
    assert params["job_id"] == "app-20260729182801-0045"
    assert params["stage_id"] == 29
    assert params["task_duration_p99_ms"] == 980.0


def test_message_on_a_different_topic_is_ignored_not_counted_as_malformed():
    connection = FakeConnection()
    consumer = StageEventConsumer(connection)

    consumer.consume_one(FakeMessage(topic="apex.spark.plan_events", value=b"{}"))

    assert consumer.stats().ignored_other_topic == 1
    assert consumer.stats().inserted == 0
    assert consumer.stats().skipped_malformed == 0
    assert connection.executed == []


def test_invalid_json_is_counted_as_malformed_not_raised():
    connection = FakeConnection()
    consumer = StageEventConsumer(connection)

    consumer.consume_one(FakeMessage(topic=STAGE_EVENTS_TOPIC, value=b"not json"))

    assert consumer.stats().skipped_malformed == 1
    assert connection.executed == []


def test_missing_required_field_is_counted_as_malformed():
    connection = FakeConnection()
    consumer = StageEventConsumer(connection)
    payload = json.dumps({"job_id": "j1", "app_id": "a1"}).encode("utf-8")  # no stage_id/stage_attempt

    consumer.consume_one(FakeMessage(topic=STAGE_EVENTS_TOPIC, value=payload))

    assert consumer.stats().skipped_malformed == 1
    assert connection.executed == []


def test_degraded_envelope_from_stream_kafka_producer_is_malformed_here():
    """stream-kafka's producer degrades an unserializable payload to
    {"type", "received_at", "unserializable_payload_type"} rather than
    raising (see stream-kafka/README.md) -- that envelope correctly lacks
    the required stage_events fields and must be skipped, not inserted with
    nulls standing in for real data."""
    connection = FakeConnection()
    consumer = StageEventConsumer(connection)
    degraded_payload = json.dumps(
        {"type": "onOtherEvent", "received_at": 1.0, "unserializable_payload_type": "JavaObject"}
    ).encode("utf-8")

    consumer.consume_one(FakeMessage(topic=STAGE_EVENTS_TOPIC, value=degraded_payload))

    assert consumer.stats().skipped_malformed == 1


def test_optional_fields_default_to_none_when_absent():
    connection = FakeConnection()
    consumer = StageEventConsumer(connection)
    minimal_payload = json.dumps(
        {"job_id": "j1", "app_id": "a1", "stage_id": 1, "stage_attempt": 0}
    ).encode("utf-8")

    consumer.consume_one(FakeMessage(topic=STAGE_EVENTS_TOPIC, value=minimal_payload))

    assert consumer.stats().inserted == 1
    _sql, params = connection.executed[0]
    assert params["task_count"] is None
    assert params["shuffle_read_bytes"] is None


def test_consume_many_processes_every_message_and_accumulates_stats():
    connection = FakeConnection()
    consumer = StageEventConsumer(connection)
    messages = [
        _stage_event_message(stage_id=1),
        _stage_event_message(stage_id=2),
        FakeMessage(topic="apex.spark.plan_events", value=b"{}"),
    ]

    consumer.consume_many(messages)

    assert consumer.stats().inserted == 2
    assert consumer.stats().ignored_other_topic == 1
