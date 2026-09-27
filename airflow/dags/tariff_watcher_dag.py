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

# The master dataset, laid out by the raw archiver as meter_readings/sim_date=YYYY-MM-DD/.
RAW_BUCKET = "voltstream-raw"
RAW_PREFIX = "meter_readings"

# tariff_2026-01-02.csv -> 2026-01-02
_TARIFF_NAME = re.compile(r"tariff_(\d{4}-\d{2}-\d{2})\.csv$")


@task
def list_pending_days() -> list[dict]:
    """Every tariff file in the landing zone, as trigger arguments.

    Returns every *complete* day, not only new ones. Deciding what is "new" would need
    state that can drift from the bucket; letting the trigger operator skip runs that
    already exist keeps the bucket itself as the only source of truth. It also means a day
    whose run was deleted gets picked up again, which is the behaviour you want.
    """
    keys = S3Hook(aws_conn_id=AWS_CONN_ID).list_keys(bucket_name=BUCKET, prefix=PREFIX) or []
    days = sorted({m.group(1) for key in keys if (m := _TARIFF_NAME.search(key))})

    # **The newest day is deliberately excluded.** The reference dropper writes
    # `tariff_D.csv` at the *start* of simulated day D, not at its end, so the presence of
    # that file says the day has begun — not that it is over. Billing on arrival meant
    # rescanning a partition the archiver was still writing into: the billing job and the
    # zone rollup read it a minute apart and saw 216 kWh and 337 kWh of the same day. The
    # cross-check gate caught it, which is what that gate is for.
    #
    # A day is complete once the *next* day's tariff exists. That is a fact about the
    # simulated clock rather than a timer, so it stays correct if the stack is paused,
    # slow, or replayed.
    complete = days[:-1]

    # **A day with no readings is not billed (R04).** The dropper seeds a tariff for the
    # day before the first one (T075), so the speed layer can price day one against
    # "yesterday's" tariff. No meter ever reported on that seed day, so billing it could
    # only fail — every cold start used to leave a red run in the UI, and a watchdog on
    # "tariff files without a successful billing run" would have alarmed forever. The same
    # rule covers any day the producer never ran. A watchdog must apply it too.
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    billable = [day for day in complete if _has_readings(hook, day)]
    skipped = [day for day in complete if day not in billable]
    if skipped:
        print(f"not billing {skipped}: no readings for those days in {RAW_BUCKET}")

    return [
        {
            "trigger_run_id": f"billing__{day}",
            "conf": {"sim_date": day},
        }
        for day in billable
    ]


def _has_readings(hook: S3Hook, day: str) -> bool:
    """Whether the master dataset holds anything for `day`. One key is enough to know."""
    prefix = f"{RAW_PREFIX}/sim_date={day}/"
    return bool(hook.list_keys(bucket_name=RAW_BUCKET, prefix=prefix, max_items=1))


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
