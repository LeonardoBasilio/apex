class FakeKafkaClient:
    """Duck-types kafka-python's KafkaProducer surface that KafkaEventProducer
    actually uses, so the lane's own tests run without a broker."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []
        self.flush_calls: list = []

    def send(self, topic: str, value: bytes):
        self.sent.append((topic, value))
        return None

    def flush(self, timeout=None) -> None:
        self.flush_calls.append(timeout)
