from dataclasses import dataclass


@dataclass
class FakeMessage:
    topic: str
    value: bytes


class FakeConnection:
    """Duck-types the DBConnection Protocol both migrate.py and consumer.py
    depend on, so their tests run without a Postgres driver or server."""

    def __init__(self) -> None:
        self.executed: list = []
        self.commit_count = 0

    def execute(self, sql: str, params=None) -> None:
        self.executed.append((sql, params))

    def commit(self) -> None:
        self.commit_count += 1
