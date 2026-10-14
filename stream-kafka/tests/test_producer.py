import json

import pytest

from apex_stream.producer import KafkaEventProducer, from_bootstrap_servers
from apex_stream.topics import TOPIC_OTHER_EVENTS, TOPIC_STAGE_EVENTS
from fake_client import FakeKafkaClient


def test_stage_completed_event_is_sent_as_json_to_the_stage_topic():
    client = FakeKafkaClient()
    producer = KafkaEventProducer(client)

    producer.send(
        {
            "type": "stage_completed",
            "received_at": 1.0,
            "event": {"job_id": "j1", "stage_id": 5, "task_duration_p99_ms": 42.0},
        }
    )

    assert len(client.sent) == 1
    topic, body = client.sent[0]
    assert topic == TOPIC_STAGE_EVENTS
    assert json.loads(body) == {"job_id": "j1", "stage_id": 5, "task_duration_p99_ms": 42.0}


def test_unserializable_payload_degrades_instead_of_raising():
    client = FakeKafkaClient()
    producer = KafkaEventProducer(client)

    class NotJsonSerializable:
        pass

    producer.send({"type": "onOtherEvent", "received_at": 2.0, "event": NotJsonSerializable()})

    assert len(client.sent) == 1
    topic, body = client.sent[0]
    assert topic == TOPIC_OTHER_EVENTS
    decoded = json.loads(body)
    assert decoded["type"] == "onOtherEvent"
    assert decoded["unserializable_payload_type"] == "NotJsonSerializable"
    assert producer.stats().degraded == 1
    assert producer.stats().sent == 1


def test_plain_dict_payload_on_other_topic_is_not_degraded():
    client = FakeKafkaClient()
    producer = KafkaEventProducer(client)

    producer.send({"type": "onJobStart", "received_at": 1.0, "event": {"jobId": 7}})

    assert producer.stats().degraded == 0
    assert producer.stats().sent == 1


def test_stats_accumulate_across_multiple_sends():
    client = FakeKafkaClient()
    producer = KafkaEventProducer(client)

    for i in range(5):
        producer.send({"type": "stage_completed", "event": {"n": i}})

    assert producer.stats().sent == 5
    assert producer.stats().degraded == 0
    assert len(client.sent) == 5


def test_flush_delegates_to_the_underlying_client():
    client = FakeKafkaClient()
    producer = KafkaEventProducer(client)

    producer.flush(timeout=3.0)

    assert client.flush_calls == [3.0]


def test_from_bootstrap_servers_raises_a_clear_error_without_kafka_python(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def blocking_import(name, *args, **kwargs):
        if name == "kafka":
            raise ImportError("no module named kafka")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocking_import)

    with pytest.raises(ImportError, match="kafka-python is required"):
        from_bootstrap_servers(["localhost:9092"])
