"""Thin, read-only psycopg wrapper. This lane only SELECTs (store-postgres/
owns all writes and DDL) -- deliberately its own small implementation
rather than importing store-postgres/'s PsycopgConnection: lanes don't
import each other's packages, only the table shape they agree on.
"""

from __future__ import annotations

from typing import Any, List, Optional


class PsycopgFetcher:
    def __init__(self, raw_connection: Any) -> None:
        self._raw = raw_connection

    def fetch_all(self, sql: str, params: Optional[dict] = None) -> List[tuple]:
        with self._raw.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def close(self) -> None:
        self._raw.close()


def from_dsn(dsn: str) -> PsycopgFetcher:
    try:
        import psycopg
    except ImportError as exc:
        raise ImportError(
            "psycopg is required for from_dsn() -- install "
            "apex-engine-postgres[postgres], or inject a fetcher directly "
            "into query.fetch_stages() (e.g. in tests)."
        ) from exc

    return PsycopgFetcher(psycopg.connect(dsn))
