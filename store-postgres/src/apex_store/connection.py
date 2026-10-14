"""Thin psycopg wrapper matching the `execute(sql, params)` / `commit()`
shape that migrate.py and consumer.py depend on (as a Protocol, not this
concrete class) -- same injectable-client pattern as
stream-kafka.producer.from_bootstrap_servers: this factory is the only
place psycopg is imported, so every other module's tests run against a
plain fake with no Postgres driver installed.
"""

from __future__ import annotations

from typing import Any, Optional


class PsycopgConnection:
    def __init__(self, raw_connection: Any) -> None:
        self._raw = raw_connection

    def execute(self, sql: str, params: Optional[dict] = None) -> None:
        with self._raw.cursor() as cursor:
            cursor.execute(sql, params)

    def fetchone(self, sql: str, params: Optional[dict] = None) -> Optional[tuple]:
        """Not part of the DBConnection Protocol migrate.py/consumer.py
        depend on -- this is a verification convenience for callers (tests,
        the smoke script) that already hold a concrete PsycopgConnection."""
        with self._raw.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def commit(self) -> None:
        self._raw.commit()

    def close(self) -> None:
        self._raw.close()


def from_dsn(dsn: str) -> PsycopgConnection:
    try:
        import psycopg
    except ImportError as exc:
        raise ImportError(
            "psycopg is required for from_dsn() -- install "
            "apex-store-postgres[postgres], or inject a connection "
            "directly into migrate.apply_ddl()/consumer.StageEventConsumer "
            "(e.g. in tests)."
        ) from exc

    raw_connection = psycopg.connect(dsn)
    return PsycopgConnection(raw_connection)
