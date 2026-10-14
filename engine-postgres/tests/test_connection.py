import builtins

import pytest

from apex_engine_postgres.connection import PsycopgFetcher, from_dsn


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self._rows


class FakeRawConnection:
    def __init__(self, rows=None):
        self._cursor = FakeCursor(rows or [])
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


def test_fetch_all_returns_rows_from_the_cursor():
    raw = FakeRawConnection(rows=[(1, "a"), (2, "b")])
    fetcher = PsycopgFetcher(raw)

    rows = fetcher.fetch_all("SELECT 1", {"x": 1})

    assert rows == [(1, "a"), (2, "b")]
    assert raw._cursor.executed == [("SELECT 1", {"x": 1})]


def test_close_delegates_to_the_raw_connection():
    raw = FakeRawConnection()
    PsycopgFetcher(raw).close()
    assert raw.closed is True


def test_from_dsn_raises_a_clear_error_without_psycopg(monkeypatch):
    real_import = builtins.__import__

    def blocking_import(name, *args, **kwargs):
        if name == "psycopg":
            raise ImportError("no module named psycopg")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocking_import)

    with pytest.raises(ImportError, match="psycopg is required"):
        from_dsn("postgresql://localhost/apex")
