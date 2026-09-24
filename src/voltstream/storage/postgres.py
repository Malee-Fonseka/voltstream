"""PostgreSQL connections for the serving layer (T098, §5.5).

**Two access paths, deliberately separate.**

The API holds a long-lived pool sized for concurrent requests. The Spark driver opens a
short-lived connection per micro-batch inside `foreachBatch` and closes it again. They do
not share a pool, and that is the point: a streaming job that stalls mid-transaction would
otherwise hold a pooled connection open indefinitely, and after a few stalls the API would
have no connections left to serve requests with. A dashboard going dark because a batch
job is wedged is exactly the coupling the serving layer exists to avoid.

The streaming path lives in `streaming/sinks.py` and uses `psycopg.connect` directly. This
module is the API's side.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

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
    if both surface as a bare OperationalError.
    """


def connection_string() -> str:
    """libpq connection string from config. The password is env-only (T018)."""
    pg = get_config().postgres
    return f"host={pg.host} port={pg.port} dbname={pg.db} user={pg.user} password={pg.password}"


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
    """
    try:
        if _pool is not None:
            with _pool.connection() as conn, conn.cursor() as cur:
                yield cur
        else:
            import psycopg

            with psycopg.connect(connection_string()) as conn, conn.cursor() as cur:
                yield cur
    except DatabaseUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001 - re-raised as a typed error below
        raise DatabaseUnavailable(f"postgres query failed: {exc}") from exc


def healthcheck() -> bool:
    """True when Postgres answers. Used by `/health/ready` (T105)."""
    try:
        with transaction() as cur:
            cur.execute("SELECT 1")
            return cur.fetchone() is not None
    except DatabaseUnavailable:
        return False
