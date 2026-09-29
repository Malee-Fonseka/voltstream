"""PostgreSQL connections for the serving layer (T098, §5.5).

**Two access paths, deliberately separate.**

The API holds a long-lived pool sized for concurrent requests. The Spark driver opens a
short-lived connection per micro-batch inside `foreachBatch` and closes it again. They do
not share a pool, and that is the point: a streaming job that stalls mid-transaction would
otherwise hold a pooled connection open indefinitely, and after a few stalls the API would
have no connections left to serve requests with. A dashboard going dark because a batch
job is wedged is exactly the coupling the serving layer exists to avoid.

The streaming path lives in `streaming/sinks.py` and opens its short-lived connections with
`connect()` below. The pool is the API's side.

**`connect()` is the one way to open a connection outside the pool** (R38). It retries
*establishing* a connection, never a query, so nothing half-executed is ever repeated.
One failed DNS lookup of `postgres` on a busy Docker host used to end whatever was
connecting: a speed-layer query (and with it the container, 15 restarts in one session) or
a day's rollup (and with it that day's reconciliation and report).
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import psycopg

from voltstream.config import get_config
from voltstream.logging_setup import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from psycopg_pool import ConnectionPool

log = get_logger("postgres")

# Sized for the API container, not for Spark. The read pattern is point lookups and small
# aggregates (§5.5), so connections are held briefly; more of them would buy queueing
# rather than throughput.
_MIN_POOL = 1
_MAX_POOL = 8

_pool: ConnectionPool | None = None


class DatabaseUnavailable(RuntimeError):
    """Raised when Postgres cannot be reached.

    A typed error rather than letting psycopg's exception escape: `/health/ready` has to
    distinguish "the database is down" from "the query was wrong", and the two look alike
    if both surface as a bare OperationalError. Only an outage becomes this; a query that
    is wrong keeps its own exception (R31).
    """


# How long /health/ready waits for a connection. Well inside the compose healthcheck's 5 s,
# so an outage reads as "not ready" rather than as a probe that timed out (R31).
_HEALTHCHECK_TIMEOUT_SECONDS = 2.0


def connection_string() -> str:
    """libpq connection string from config. The password is env-only (T018)."""
    pg = get_config().postgres
    return f"host={pg.host} port={pg.port} dbname={pg.db} user={pg.user} password={pg.password}"


# Seconds to wait before each retry: five attempts over about 15 s. Long enough to ride out
# a DNS timeout or a Postgres restart, short enough that a real outage still fails the
# micro-batch or the task promptly, where Airflow and the container restart policy take
# over.
_CONNECT_BACKOFF_SECONDS = (1.0, 2.0, 4.0, 8.0)


def connect(
    conninfo: str | None = None, *, sleep: Callable[[float], None] = time.sleep
) -> psycopg.Connection[Any]:
    """Open a connection, retrying transient failures to establish it (R38).

    Only `psycopg.OperationalError` raised while connecting is retried: DNS failures,
    refused connections, timeouts. Every other exception propagates at once, and so does
    the last failure once the attempts are spent, with its own message. A wrong password
    is an OperationalError too; it simply fails about 15 s later, which is harmless.

    Use it exactly as `psycopg.connect`: `with connect() as conn, conn.cursor() as cur:`.
    """
    info = conninfo or connection_string()
    attempts = len(_CONNECT_BACKOFF_SECONDS) + 1
    for attempt in range(1, attempts + 1):
        try:
            return psycopg.connect(info)
        except psycopg.OperationalError as exc:
            if attempt == attempts:
                raise
            delay = _CONNECT_BACKOFF_SECONDS[attempt - 1]
            log.warning(
                "postgres connection failed, retrying",
                extra={
                    "stage": "postgres",
                    "attempt": attempt,
                    "attempts": attempts,
                    "retry_in_s": delay,
                    "detail": str(exc)[:200],
                },
            )
            sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover - the loop returns or raises


def open_pool() -> ConnectionPool:
    """Create the process-wide pool. Called once from the API's lifespan (T103)."""
    global _pool
    if _pool is not None:
        return _pool

    from psycopg_pool import ConnectionPool

    # open=False then open(wait=...) so a Postgres that is slow to accept connections
    # delays startup rather than failing it — tier 3 can outrace tier 1 on a cold boot.
    _pool = ConnectionPool(connection_string(), min_size=_MIN_POOL, max_size=_MAX_POOL, open=False)
    _pool.open(wait=True, timeout=30.0)
    log.info("postgres pool opened", extra={"stage": "storage", "max_size": _MAX_POOL})
    return _pool


def close_pool() -> None:
    """Close the pool on shutdown, so connections are released rather than reaped."""
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
        log.info("postgres pool closed", extra={"stage": "storage"})


def get_pool() -> ConnectionPool:
    if _pool is None:
        raise DatabaseUnavailable("connection pool is not open; call open_pool() first")
    return _pool


@contextmanager
def transaction() -> Iterator[Any]:
    """A cursor inside a transaction, committed on success and rolled back on error.

    Uses the pool when one is open and a single short-lived connection when it is not.
    That is what lets `storage/repositories.py` be the one place SQL lives: the API runs
    pooled, while a batch script in the Spark image has no pool at all — `psycopg_pool`
    is deliberately only in the `api` extra, because a Spark driver must not hold pooled
    connections across a stalled micro-batch. Without this fallback every batch-side
    caller would have to re-implement the same queries against a raw connection, and the
    two copies would drift.

    Reads use it too. A read-only transaction costs nothing and means a caller that later
    grows a write cannot accidentally leave it uncommitted.

    Only `psycopg.OperationalError` becomes `DatabaseUnavailable`: a refused or lost
    connection, or the pool timing out (`PoolTimeout` is one). A SQL typo or a mapping bug
    propagates as itself, so it is reported as the bug it is rather than as an outage (R31).
    """
    try:
        if _pool is not None:
            with _pool.connection() as conn, conn.cursor() as cur:
                yield cur
        else:
            with connect() as conn, conn.cursor() as cur:
                yield cur
    except psycopg.OperationalError as exc:
        raise DatabaseUnavailable(f"postgres unavailable: {exc}") from exc


def healthcheck() -> bool:
    """True when Postgres answers within `_HEALTHCHECK_TIMEOUT_SECONDS`. For `/health/ready`.

    Not through `transaction()`: that waits up to the pool's 30 s for a connection, or
    retries a failed connect for about 15 s, and the compose healthcheck gives up after 5
    (R31). A probe must fail fast.
    """
    try:
        if _pool is not None:
            with _pool.connection(timeout=_HEALTHCHECK_TIMEOUT_SECONDS) as conn:
                return conn.execute("SELECT 1").fetchone() is not None
        with psycopg.connect(
            connection_string(), connect_timeout=int(_HEALTHCHECK_TIMEOUT_SECONDS)
        ) as conn:
            return conn.execute("SELECT 1").fetchone() is not None
    except psycopg.OperationalError:
        return False
