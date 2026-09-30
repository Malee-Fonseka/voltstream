"""The run ledger, `pipeline_runs`, as both batch jobs keep it (T115, T122, R24, R38).

A run is bracketed: a `running` row when it starts, and at the end either `success` —
written in the same transaction as the run's output, superseding the day's previous
`success` — or `failed`. `layer` is `batch_billing` or `batch_rollup` (§6.4); the partial
unique index allows one `success` row per day and layer.
"""

from __future__ import annotations

import os
import uuid
from datetime import date
from typing import Any

from voltstream.logging_setup import get_logger
from voltstream.storage.postgres import connect
from voltstream.streaming.sinks import pg_connection_string

log = get_logger("ledger")


def orchestrator_run_id() -> str | None:
    """Airflow's dag_run_id, when this job was launched by a DAG.

    None when it was not — a bare `spark-submit` or `make backfill` is a legitimate way to
    run a job, not a degraded one. Recording it is what makes a restatement legible later:
    two ledger rows for one simulated day, each naming the execution that produced it.
    """
    return os.environ.get("VOLTSTREAM_ORCHESTRATOR_RUN_ID") or None


def start_run(run_id: uuid.UUID, sim_date: date, layer: str) -> None:
    """Open this run's ledger row, closing any the day's earlier attempts left open.

    An attempt killed before it can record its own failure (it lost Postgres, or its
    container was killed) leaves its row at `running` for good (R38). The DAG runs one
    run at a time (`max_active_runs=1`), so when an attempt starts, any `running` row for
    the same day and layer belongs to one that died, and it is marked `failed` in the same
    transaction. Should a run outside Airflow overlap after all, nothing is lost:
    `complete_run` sets its row to `success` by run_id, whatever the row says by then.
    """
    with connect(pg_connection_string()) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_runs SET status = 'failed', finished_at = now() "
            "WHERE sim_date = %s AND layer = %s AND status = 'running'",
            (sim_date, layer),
        )
        abandoned = cur.rowcount
        cur.execute(
            "INSERT INTO pipeline_runs "
            "(run_id, sim_date, layer, status, started_at, orchestrator_run_id) "
            "VALUES (%s, %s, %s, 'running', now(), %s)",
            (run_id, sim_date, layer, orchestrator_run_id()),
        )
        conn.commit()
    if abandoned:
        log.warning(
            "closed abandoned runs",
            extra={
                "stage": layer,
                "sim_date": sim_date.isoformat(),
                "abandoned": abandoned,
            },
        )


def fail_run(run_id: uuid.UUID, rows_in: int) -> None:
    with connect(pg_connection_string()) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_runs SET status = 'failed', finished_at = now(), rows_in = %s "
            "WHERE run_id = %s",
            (rows_in, run_id),
        )
        conn.commit()


def supersede_success(cur: Any, sim_date: date, layer: str) -> None:
    """Demote the day's current `success` row. Call inside the transaction that completes.

    Inside, because the partial unique index permits only one `success` row at a time:
    completing before demoting would violate it, and demoting in a separate transaction
    would leave a window where the day looks unfinished.
    """
    cur.execute(
        "UPDATE pipeline_runs SET status = 'superseded' "
        "WHERE sim_date = %s AND layer = %s AND status = 'success'",
        (sim_date, layer),
    )


def complete_run(cur: Any, run_id: uuid.UUID, rows_in: int, rows_out: int) -> None:
    """Mark this run `success`, inside the transaction that writes its output."""
    cur.execute(
        "UPDATE pipeline_runs SET status = 'success', finished_at = now(), "
        "rows_in = %s, rows_out = %s WHERE run_id = %s",
        (rows_in, rows_out, run_id),
    )
