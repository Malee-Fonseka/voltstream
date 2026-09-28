"""Bill one simulated day (T121–T123, T139, D6).

Triggered by `tariff_watcher` with `conf={"sim_date": "YYYY-MM-DD"}`. In order: wait for
the tariff file, wait out the late-data grace, run the Spark billing job in its own
container, check what it wrote, roll up the zones, reconcile the speed layer's estimates
against the new bills, and write the day's report.

**Every task is keyed on `params.sim_date`, never on the logical date.** That is what makes
a restatement work (T123): re-running this DAG for an already-billed day with a fresh
`run_id` recomputes it from immutable inputs and supersedes the previous result. Airflow's
own backfill cannot do this — its data intervals are real time, and a simulated day is not
addressable that way. The mechanism §5.6 describes is unchanged; only the CLI verb differs.

**No business logic here.** The DAG submits a container and verifies SQL. Any tariff or
netting arithmetic appearing under `airflow/dags/` would mean two implementations of the
billing rules, which is precisely what the shared `core/` module exists to prevent — and
this image cannot even import that module.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLValueCheckOperator
from airflow.providers.docker.operators.docker import DockerOperator

BUCKET = "voltstream-landing"
AWS_CONN_ID = "minio_s3"
PG_CONN_ID = "voltstream_pg"

# Mounted read-only by compose. The DAG reads three tunables from it rather than importing
# the project package, which this image deliberately does not contain (§5.6).
_CONFIG_PATH = Path("/opt/airflow/voltstream-config/base.yaml")


def _config() -> dict:
    with _CONFIG_PATH.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


_CFG = _config()
_GRACE_SECONDS = int(_CFG["batch"]["late_data_grace_real_seconds"])
_SLA_MINUTES = int(_CFG["alerts"]["batch_sla_minutes"])

# Where the batch containers push their metrics as they exit (T116, T142): billing and the
# rollup push voltstream_batch_duration_seconds, reconciliation pushes
# voltstream_lambda_divergence. Set here rather than in base.yaml so a local run or a test
# never tries to reach a Pushgateway that only exists on the Compose network.
_PUSHGATEWAY_URL = "http://pushgateway:9091"
_HOUSEHOLDS = int(_CFG["simulation"]["households"])


@task
def wait_late_data_grace(sim_date: str) -> float:
    """Sleep until the tariff file has been settled for `late_data_grace_real_seconds`.

    Not arbitrary padding. Per D3, a meter dropout straddling simulated midnight flushes
    the previous day's readings up to 30 real seconds after midnight, and the archiver
    commits them up to one trigger later. Starting the rescan before that lands would bill
    an incomplete day — and completeness is the batch layer's entire justification for
    existing. Waiting 90 real seconds is cheap; a bill computed from a partial day is not.

    Keyed on the file's own `LastModified`, so a restatement of an old file waits no time
    at all: the grace has long since elapsed.
    """
    key = f"tariff/tariff_{sim_date}.csv"
    obj = S3Hook(aws_conn_id=AWS_CONN_ID).get_key(key=key, bucket_name=BUCKET)
    ready_at = obj.last_modified.timestamp() + _GRACE_SECONDS
    remaining = ready_at - time.time()

    print(f"tariff {key} last modified {obj.last_modified.isoformat()}")
    print(f"grace {_GRACE_SECONDS}s -> ready at {datetime.fromtimestamp(ready_at).isoformat()}")
    print(f"sleeping {max(0.0, remaining):.1f}s")

    if remaining > 0:
        time.sleep(remaining)
    return max(0.0, remaining)


with DAG(
    dag_id="daily_billing",
    description="Recompute authoritative bills for one simulated day.",
    start_date=datetime(2026, 1, 1),
    # Triggered by tariff_watcher, never scheduled: a simulated day has no real-time cron.
    schedule=None,
    catchup=False,
    # Two runs for different days would contend on the same Spark resources and, worse,
    # interleave their pipeline_runs bookkeeping.
    max_active_runs=1,
    params={"sim_date": Param(type="string", format="date")},
    tags=["voltstream", "batch"],
) as dag:
    # S3KeySensor, not FileSensor: the tariff drop is an object-store key, and FileSensor
    # can only see a filesystem path. Poke rather than reschedule — at a 15 s interval and
    # a few minutes' timeout, a held worker slot is cheaper than the scheduler round trip.
    wait_for_tariff = S3KeySensor(
        task_id="wait_for_tariff",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=BUCKET,
        bucket_key="tariff/tariff_{{ params.sim_date }}.csv",
        # A key that exists but is empty means the writer is mid-flight. The dropper writes
        # to a temporary key and copies, so this should not happen — checking anyway costs
        # nothing and turns a silent zero-row bill into a wait.
        check_fn=lambda files: all(f["Size"] > 0 for f in files),
        mode="poke",
        poke_interval=15,
        timeout=_SLA_MINUTES * 60,
    )

    grace = wait_late_data_grace(sim_date="{{ params.sim_date }}")

    run_daily_billing = DockerOperator(
        task_id="run_daily_billing",
        image=os.environ.get("VOLTSTREAM_SPARK_IMAGE", "voltstream-spark:local"),
        # Through the proxy; this container has no access to the Docker socket itself.
        docker_url="tcp://docker-socket-proxy:2375",
        network_mode=os.environ.get("VOLTSTREAM_NETWORK", "voltstream"),
        mount_tmp_dir=False,
        force_pull=False,
        auto_remove="force",
        # Streams the Spark job's structured JSON log lines into this task's log, so one
        # place shows both the orchestration and what the job actually did.
        tty=False,
        command=[
            "spark-submit",
            "--master",
            "local[*]",
            "/opt/venv/lib/python3.11/site-packages/voltstream/batch/daily_billing.py",
            "--sim-date",
            "{{ params.sim_date }}",
        ],
        environment={
            "VOLTSTREAM_ENV": "docker",
            "VOLTSTREAM_CONFIG_DIR": "/app/config",
            "VOLTSTREAM__KAFKA__BOOTSTRAP_SERVERS": "kafka:9092",
            "VOLTSTREAM__POSTGRES__HOST": "postgres",
            "VOLTSTREAM__SIMULATION__ANCHOR_REAL": os.environ.get("VOLTSTREAM_ANCHOR_REAL", ""),
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_DEFAULT_REGION": "us-east-1",
            "VOLTSTREAM__OBSERVABILITY__PUSHGATEWAY_URL": _PUSHGATEWAY_URL,
            # Lineage: the job records this against pipeline_runs.orchestrator_run_id, so
            # a restated day shows which DAG run produced each of its two ledger rows.
            "VOLTSTREAM_ORCHESTRATOR_RUN_ID": "{{ run_id }}",
        },
        private_environment={
            "VOLTSTREAM__POSTGRES__PASSWORD": os.environ.get("POSTGRES_PASSWORD", "voltstream"),
            "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", "voltstream"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", "voltstream-dev"),
        },
        # Safe to retry because T115 writes the day in one transaction: a failed attempt
        # leaves no partial bills, so attempt two starts from the same clean state.
        retries=2,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
    )

    # Two checks, deliberately different in kind. The first asserts the expected number of
    # bills — a job that silently billed 43 of 50 households has failed, even though it
    # exited zero. The second asserts the rows are sane.
    verify_row_count = SQLValueCheckOperator(
        task_id="verify_row_count",
        conn_id=PG_CONN_ID,
        sql=(
            "SELECT count(*) FROM household_bill_daily "
            "WHERE sim_date = DATE '{{ params.sim_date }}'"
        ),
        pass_value=_HOUSEHOLDS,
    )

    verify_bill_sanity = SQLCheckOperator(
        task_id="verify_bill_sanity",
        conn_id=PG_CONN_ID,
        # SQLCheckOperator passes when every value in the first row is **truthy**, so the
        # columns are predicates that are TRUE when healthy — not counts of problems,
        # which would evaluate 0 as falsy and fail the check on a perfectly good run.
        #
        # `final_bill` is allowed to be negative (D5: a net exporter is owed money), so
        # the bound is on magnitude rather than sign.
        sql="""
            SELECT
              -- First and load-bearing: every predicate below is a count-is-zero test,
              -- and all of them are trivially true of an empty table. Without this the
              -- sanity check passes on a day that produced no bills at all, which is
              -- exactly the failure it should catch.
              count(*) > 0                                           AS day_has_bills,
              count(*) FILTER (WHERE final_bill IS NULL)        = 0 AS no_null_bills,
              count(*) FILTER (WHERE tier_breakdown IS NULL)    = 0 AS no_null_breakdown,
              count(*) FILTER (WHERE readings_count <= 0)       = 0 AS every_bill_has_readings,
              count(*) FILTER (WHERE abs(final_bill) > 1000000) = 0 AS no_absurd_bills,
              count(*) FILTER (WHERE tariff_effective_date > DATE '{{ params.sim_date }}')
                                                                = 0 AS no_future_tariff
            FROM household_bill_daily
            WHERE sim_date = DATE '{{ params.sim_date }}'
        """,
    )

    # The rollup runs after the bills, not beside them: its cross-check compares zone
    # totals against what household_bill_daily actually holds, so the bills have to be
    # committed first or it would check against an empty table and fail.
    run_zone_rollup = DockerOperator(
        task_id="run_zone_rollup",
        image=os.environ.get("VOLTSTREAM_SPARK_IMAGE", "voltstream-spark:local"),
        docker_url="tcp://docker-socket-proxy:2375",
        network_mode=os.environ.get("VOLTSTREAM_NETWORK", "voltstream"),
        mount_tmp_dir=False,
        force_pull=False,
        auto_remove="force",
        tty=False,
        command=[
            "spark-submit",
            "--master",
            "local[*]",
            "/opt/venv/lib/python3.11/site-packages/voltstream/batch/daily_zone_rollup.py",
            "--sim-date",
            "{{ params.sim_date }}",
        ],
        environment={
            "VOLTSTREAM_ENV": "docker",
            "VOLTSTREAM_CONFIG_DIR": "/app/config",
            "VOLTSTREAM__POSTGRES__HOST": "postgres",
            "VOLTSTREAM__SIMULATION__ANCHOR_REAL": os.environ.get("VOLTSTREAM_ANCHOR_REAL", ""),
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_DEFAULT_REGION": "us-east-1",
            "VOLTSTREAM__OBSERVABILITY__PUSHGATEWAY_URL": _PUSHGATEWAY_URL,
            "VOLTSTREAM_ORCHESTRATOR_RUN_ID": "{{ run_id }}",
        },
        private_environment={
            "VOLTSTREAM__POSTGRES__PASSWORD": os.environ.get("POSTGRES_PASSWORD", "voltstream"),
            "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", "voltstream"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", "voltstream-dev"),
        },
        # No retries. The cross-check failing is a data-consistency verdict, not a
        # transient fault, and retrying it would just fail again more slowly.
        retries=0,
    )

    # T139 — after the bills and the rollup, because it compares the speed layer's
    # estimates against the bills this run just committed. On the app image, not the Spark
    # one (D4, D6): fifty rows of Decimal arithmetic with `core/tariff.py` need no cluster.
    run_reconciliation = DockerOperator(
        task_id="run_reconciliation",
        image=os.environ.get("VOLTSTREAM_APP_IMAGE", "voltstream-app:local"),
        docker_url="tcp://docker-socket-proxy:2375",
        network_mode=os.environ.get("VOLTSTREAM_NETWORK", "voltstream"),
        mount_tmp_dir=False,
        force_pull=False,
        auto_remove="force",
        tty=False,
        command=["voltstream-reconcile", "--sim-date", "{{ params.sim_date }}"],
        environment={
            "VOLTSTREAM_ENV": "docker",
            "VOLTSTREAM_CONFIG_DIR": "/app/config",
            "VOLTSTREAM__POSTGRES__HOST": "postgres",
            "VOLTSTREAM__SIMULATION__ANCHOR_REAL": os.environ.get("VOLTSTREAM_ANCHOR_REAL", ""),
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_DEFAULT_REGION": "us-east-1",
            "VOLTSTREAM__OBSERVABILITY__PUSHGATEWAY_URL": _PUSHGATEWAY_URL,
        },
        private_environment={
            "VOLTSTREAM__POSTGRES__PASSWORD": os.environ.get("POSTGRES_PASSWORD", "voltstream"),
            "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", "voltstream"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", "voltstream-dev"),
        },
        # Safe to retry: the job replaces the day's rows in one transaction.
        retries=2,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
    )

    # Last, because it reports on everything above it — bills, rollup, reconciliation and
    # the run ledger.
    # Generated rather than served on request: the brief's deliverable is a report that
    # exists, not an endpoint someone has to know to call.
    #
    # Published to voltstream-archive/reports/ (R08). This container is removed as soon as
    # it exits, so a file on its own disk would vanish with it. On the app image, per D6:
    # it needs Postgres and the S3 client, not Spark.
    generate_report = DockerOperator(
        task_id="generate_report",
        image=os.environ.get("VOLTSTREAM_APP_IMAGE", "voltstream-app:local"),
        docker_url="tcp://docker-socket-proxy:2375",
        network_mode=os.environ.get("VOLTSTREAM_NETWORK", "voltstream"),
        mount_tmp_dir=False,
        force_pull=False,
        auto_remove="force",
        tty=False,
        command=[
            "python",
            "/app/scripts/generate_report.py",
            "--sim-date",
            "{{ params.sim_date }}",
        ],
        environment={
            "VOLTSTREAM_ENV": "docker",
            "VOLTSTREAM_CONFIG_DIR": "/app/config",
            "VOLTSTREAM__POSTGRES__HOST": "postgres",
            "VOLTSTREAM__SIMULATION__ANCHOR_REAL": os.environ.get("VOLTSTREAM_ANCHOR_REAL", ""),
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_DEFAULT_REGION": "us-east-1",
        },
        private_environment={
            "VOLTSTREAM__POSTGRES__PASSWORD": os.environ.get("POSTGRES_PASSWORD", "voltstream"),
            "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", "voltstream"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", "voltstream-dev"),
        },
        retries=1,
    )

    (
        wait_for_tariff
        >> grace
        >> run_daily_billing
        >> [verify_row_count, verify_bill_sanity]
        >> run_zone_rollup
        >> run_reconciliation
        >> generate_report
    )
