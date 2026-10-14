import builtins

import pytest

from apex_store.connection import PsycopgConnection, from_dsn


class FakeCursor:
    def __init__(self, parent) -> None:
        self._parent = parent
        self._last_result = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, params=None):
        self._parent.executed.append((sql, params))
        self._last_result = self._parent.fetch_result

    def fetchone(self):
        return self._last_result


class FakeRawConnection:
    def __init__(self) -> None:
        self.executed: list = []
        self.committed = False
        self.closed = False
        self.fetch_result = None

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def test_psycopg_connection_executes_through_a_cursor_and_commits():
    raw = FakeRawConnection()
    connection = PsycopgConnection(raw)

    connection.execute("SELECT 1", {"a": 1})
    connection.commit()
    connection.close()

    assert raw.executed == [("SELECT 1", {"a": 1})]
    assert raw.committed is True
    assert raw.closed is True


def test_psycopg_connection_fetchone_returns_the_cursor_result():
    raw = FakeRawConnection()
    raw.fetch_result = (1, "ok")
    connection = PsycopgConnection(raw)

    result = connection.fetchone("SELECT 1, 'ok'")

    assert result == (1, "ok")
    assert raw.executed == [("SELECT 1, 'ok'", None)]


def test_from_dsn_raises_a_clear_error_without_psycopg(monkeypatch):
    real_import = builtins.__import__

    def blocking_import(name, *args, **kwargs):
        if name == "psycopg":
            raise ImportError("no module named psycopg")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocking_import)

    with pytest.raises(ImportError, match="psycopg is required"):
        from_dsn("postgresql://localhost/apex")
