"""Watch the landing zone and trigger a billing run per simulated day (T121, D6 Design B).

Airflow schedules on *real* time. This project's days are simulated and pass every five
real minutes, so no cron expression can express "once per simulated day" — and Airflow's
own backfill machinery, which addresses runs by data interval, cannot address a simulated
date at all. Hence a watcher: poll the landing zone on a real-time schedule, and trigger
the billing DAG once per tariff file that appears.

Idempotent by construction rather than by bookkeeping. The run id is derived from the
date in the filename, so re-listing the same file produces the same run id, and
`skip_when_already_exists` makes the repeat a no-op. There is no "last seen" state to get
out of step with reality.

This file imports no project code — the Airflow image deliberately has none (§5.6).
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET = "voltstream-landing"
PREFIX = "tariff/"
AWS_CONN_ID = "minio_s3"

# tariff_2026-01-02.csv -> 2026-01-02
_TARIFF_NAME = re.compile(r"tariff_(\d{4}-\d{2}-\d{2})\.csv$")


@task
def list_pending_days() -> list[dict]:
    """Every tariff file in the landing zone, as trigger arguments.

    Returns all of them, not only new ones. Deciding what is "new" would need state that
    can drift from the bucket; letting the trigger operator skip runs that already exist
    keeps the bucket itself as the only source of truth. It also means a day whose run was
    deleted gets picked up again, which is the behaviour you want.
    """
    keys = S3Hook(aws_conn_id=AWS_CONN_ID).list_keys(bucket_name=BUCKET, prefix=PREFIX) or []

    days = sorted({m.group(1) for key in keys if (m := _TARIFF_NAME.search(key))})
    return [
        {
            "trigger_run_id": f"billing__{day}",
            "conf": {"sim_date": day},
        }
        for day in days
    ]


with DAG(
    dag_id="tariff_watcher",
    description="Trigger a billing run for each tariff file in the landing zone.",
    start_date=datetime(2026, 1, 1),
    # Real minutes: this is infrastructure polling, not simulated-time logic.
    schedule=timedelta(minutes=1),
    catchup=False,
    # Two watchers listing the same bucket would race to create the same run ids. Harmless
    # given skip_when_already_exists, but pointless.
    max_active_runs=1,
    tags=["voltstream", "orchestration"],
) as dag:
    TriggerDagRunOperator.partial(
        task_id="trigger_billing",
        trigger_dag_id="daily_billing",
        # The idempotency guarantee: a repeat listing of the same file is a no-op.
        skip_when_already_exists=True,
        reset_dag_run=False,
    ).expand_kwargs(list_pending_days())
