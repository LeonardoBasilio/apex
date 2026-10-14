"""Kafka transport for capture-pyspark's events, replacing the upstream OTel
Collector config (collect/). KafkaEventProducer is injected as the `sink`
callable into apex_capture.listener.attach(); it runs on
NonBlockingEventQueue's background worker thread, never on Spark's listener
bus thread (see capture-pyspark/README.md for why that boundary exists).

Injectable with anything shaped like kafka-python's KafkaProducer
(`.send(topic, value) -> future-like`, `.flush(timeout=None)`), so these
tests run without a broker — the kafka-python dependency is optional and
only required by `from_bootstrap_servers()`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional, Protocol

from .topics import topic_for


class KafkaLikeClient(Protocol):
    def send(self, topic: str, value: bytes) -> Any: ...

    def flush(self, timeout: Optional[float] = None) -> None: ...


@dataclass
class ProducerStats:
    sent: int = 0
    degraded: int = 0


class KafkaEventProducer:
    def __init__(self, client: KafkaLikeClient) -> None:
        self._client = client
        self._stats = ProducerStats()

    def send(self, event: dict) -> None:
        """Capture-pyspark hasn't shaped every event type yet — AQE/plan
        extraction is deferred (see README.md's Status note), so a raw,
        unserializable JVM object can still reach here for those types.
        That degrades to a minimal envelope rather than raising: a transport
        failure and "haven't built the extractor for this event type yet"
        are different problems, and only the first should count as a sink
        error against NonBlockingEventQueue's `sink_errors` stat."""
        topic = topic_for(event)
        body, degraded = self._serialize(event)
        if degraded:
            self._stats.degraded += 1
        self._client.send(topic, body)
        self._stats.sent += 1

    def flush(self, timeout: Optional[float] = None) -> None:
        self._client.flush(timeout)

    def stats(self) -> ProducerStats:
        return ProducerStats(sent=self._stats.sent, degraded=self._stats.degraded)

    def _serialize(self, event: dict) -> tuple[bytes, bool]:
        payload = event.get("event")
        try:
            return json.dumps(payload).encode("utf-8"), False
        except TypeError:
            degraded_payload = {
                "type": event.get("type"),
                "received_at": event.get("received_at"),
                "unserializable_payload_type": type(payload).__name__,
            }
            return json.dumps(degraded_payload).encode("utf-8"), True


def from_bootstrap_servers(bootstrap_servers: list) -> KafkaEventProducer:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise ImportError(
            "kafka-python is required for from_bootstrap_servers() — install "
            "apex-stream-kafka[kafka], or inject a client directly into "
            "KafkaEventProducer (e.g. in tests)."
        ) from exc

    client = KafkaProducer(bootstrap_servers=bootstrap_servers)
    return KafkaEventProducer(client)
