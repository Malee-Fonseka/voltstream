"""Unit tests for storage.postgres: connect (R38), transaction and healthcheck (R31).

psycopg.connect is replaced, so nothing here needs a database. What is pinned: a transient
failure to connect is retried with backoff, the last failure still surfaces, nothing but a
connection failure is retried, and no code opens a connection any other way.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg
import psycopg_pool
import pytest

from voltstream.storage import postgres

_SRC = Path(__file__).resolve().parents[2] / "src" / "voltstream"


def _flaky_connect(failures: int, error: BaseException) -> tuple[list[str], Any]:
    """A psycopg.connect stand-in failing `failures` times, then returning a sentinel."""
    calls: list[str] = []
    connection = object()

    def fake(conninfo: str) -> object:
        calls.append(conninfo)
        if len(calls) <= failures:
            raise error
        return connection

    return calls, (fake, connection)


def test_a_transient_failure_is_retried_with_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    dns = psycopg.OperationalError("failed to resolve host 'postgres': Temporary failure")
    calls, (fake, connection) = _flaky_connect(2, dns)
    monkeypatch.setattr(postgres.psycopg, "connect", fake)
    slept: list[float] = []

    assert postgres.connect("host=postgres", sleep=slept.append) is connection
    assert calls == ["host=postgres"] * 3
    assert slept == [1.0, 2.0]


def test_the_last_failure_surfaces_once_the_attempts_are_spent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    down = psycopg.OperationalError("connection refused")
    calls, (fake, _) = _flaky_connect(99, down)
    monkeypatch.setattr(postgres.psycopg, "connect", fake)
    slept: list[float] = []

    with pytest.raises(psycopg.OperationalError, match="connection refused"):
        postgres.connect("host=postgres", sleep=slept.append)
    assert len(calls) == 5
    assert slept == [1.0, 2.0, 4.0, 8.0]


def test_anything_but_a_connection_failure_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls, (fake, _) = _flaky_connect(99, psycopg.ProgrammingError("bad conninfo"))
    monkeypatch.setattr(postgres.psycopg, "connect", fake)
    slept: list[float] = []

    with pytest.raises(psycopg.ProgrammingError):
        postgres.connect("host=postgres", sleep=slept.append)
    assert len(calls) == 1
    assert slept == []


def test_the_default_conninfo_comes_from_config(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, (fake, _) = _flaky_connect(0, psycopg.OperationalError())
    monkeypatch.setattr(postgres.psycopg, "connect", fake)

    postgres.connect(sleep=lambda _: None)
    assert calls == [postgres.connection_string()]


def test_no_module_opens_a_connection_without_the_retry() -> None:
    """Every short-lived connection goes through connect(); the pool is the API's own."""
    bare = []
    for path in _SRC.rglob("*.py"):
        if path.name == "postgres.py" and path.parent.name == "storage":
            continue
        text = path.read_text(encoding="utf-8")
        bare += [
            f"{path.relative_to(_SRC)}:{m.start()}"
            for m in re.finditer(r"psycopg\.connect\(", text)
        ]
    assert bare == []


# ---------------------------------------------------------------------------------------
# R31: only an outage is DatabaseUnavailable, and the readiness probe fails fast.
# ---------------------------------------------------------------------------------------


class _FakeCursor:
    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class _FakeConnection:
    def cursor(self) -> _FakeCursor:
        return _FakeCursor()

    def execute(self, sql: str) -> Any:
        return self

    def fetchone(self) -> tuple[int]:
        return (1,)


class _FakePool:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.timeouts: list[float | None] = []

    @contextmanager
    def connection(self, timeout: float | None = None) -> Iterator[_FakeConnection]:
        self.timeouts.append(timeout)
        if self.error is not None:
            raise self.error
        yield _FakeConnection()


def test_an_outage_is_database_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(postgres, "_pool", _FakePool(psycopg_pool.PoolTimeout("no connection")))
    with pytest.raises(postgres.DatabaseUnavailable), postgres.transaction():
        pass


def test_a_wrong_query_keeps_its_own_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """A SQL typo used to surface as DatabaseUnavailable, as if Postgres were down."""
    monkeypatch.setattr(postgres, "_pool", _FakePool())
    with pytest.raises(psycopg.errors.UndefinedColumn), postgres.transaction():
        raise psycopg.errors.UndefinedColumn('column "totl" does not exist')


def test_the_readiness_probe_waits_seconds_not_the_pools_thirty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _FakePool(psycopg_pool.PoolTimeout("no connection"))
    monkeypatch.setattr(postgres, "_pool", pool)
    assert postgres.healthcheck() is False
    assert pool.timeouts == [postgres._HEALTHCHECK_TIMEOUT_SECONDS]
    assert postgres._HEALTHCHECK_TIMEOUT_SECONDS < 5, "inside the compose healthcheck timeout"


def test_the_readiness_probe_passes_when_postgres_answers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(postgres, "_pool", _FakePool())
    assert postgres.healthcheck() is True
