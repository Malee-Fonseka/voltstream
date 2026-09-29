"""The batch layer end to end, on committed fixtures (T172, T173).

A known day of readings goes into the master dataset exactly as the raw archiver would
write it, a known tariff file goes into the landing zone, and the real billing job runs in
the Spark image, as the DAG runs it. The bills it commits to Postgres must equal the
**hand-computed** ones in `tests/fixtures/expected_bills_2025-06-15.csv`; the derivations
are in `tests/fixtures/README.md`. The first three households are D5's worked examples
(720.17, 454.41, −150.00).

Then two properties the design claims:

- **Determinism (D5, §5.4).** Billing the same day again from the same inputs produces
  identical rows, apart from the run that produced them (`pipeline_run_id`) and when
  (`computed_at`). This is what Decimal buys over float64.
- **It checks arithmetic, not plumbing (T172's "Done when").** Raising one household's
  `block_1_rate` changes that household's bill to the hand-computed 780.17 and nobody
  else's.

Needs the Compose stack up. While it runs, the tariff watcher is paused so Airflow does not
bill the fixture day on its own, and on the way out every trace of the fixture day is
removed: the raw partition, the landing and archived tariff, the bills and the ledger rows.
"""

from __future__ import annotations

import csv
import subprocess
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from dotenv import dotenv_values

pytestmark = pytest.mark.integration

_REPO = Path(__file__).resolve().parents[2]
_FIXTURES = _REPO / "tests" / "fixtures"
_DAY = "2025-06-15"
_ENV = dotenv_values(_REPO / ".env")
_SPARK_IMAGE = "voltstream-spark:local"

# Columns compared against the hand-computed bills. tier_breakdown is JSON and derived
# from the same block charges; pipeline_run_id and computed_at identify the run.
_COMPARED = (
    "consumption_kwh",
    "solar_kwh",
    "self_consumed_kwh",
    "billable_import_kwh",
    "export_kwh",
    "energy_charge",
    "fixed_charge",
    "subsidy_discount",
    "export_credit",
    "final_bill",
    "readings_count",
    "duplicates_removed",
)


def _env(key: str, default: str) -> str:
    return _ENV.get(key) or default


def _run(
    *args: str, stdin: bytes | None = None, check: bool = True
) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(args, input=stdin, capture_output=True, check=False)
    if check and result.returncode != 0:
        tail = (result.stdout + result.stderr).decode("utf-8", "replace")[-3000:]
        raise AssertionError(f"{' '.join(args[:4])} ... exited {result.returncode}:\n{tail}")
    return result


def _mc(
    *args: str, stdin: bytes | None = None, check: bool = True
) -> subprocess.CompletedProcess[bytes]:
    user, password = (
        _env("MINIO_ROOT_USER", "voltstream"),
        _env("MINIO_ROOT_PASSWORD", "voltstream-dev"),
    )
    return _run(
        "docker",
        "exec",
        "-i",
        "-e",
        f"MC_HOST_local=http://{user}:{password}@localhost:9000",
        "voltstream-minio",
        "mc",
        *args,
        stdin=stdin,
        check=check,
    )


def _spark_env() -> list[str]:
    """The environment the DAG gives a billing container, minus the Pushgateway: a test run
    must not overwrite the real jobs' pushed metrics."""
    pairs = {
        "VOLTSTREAM_ENV": "docker",
        "VOLTSTREAM_CONFIG_DIR": "/app/config",
        "VOLTSTREAM__KAFKA__BOOTSTRAP_SERVERS": "kafka:9092",
        "VOLTSTREAM__POSTGRES__HOST": "postgres",
        "VOLTSTREAM__POSTGRES__PASSWORD": _env("POSTGRES_PASSWORD", "voltstream"),
        "AWS_ENDPOINT_URL": "http://minio:9000",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": _env("MINIO_ROOT_USER", "voltstream"),
        "AWS_SECRET_ACCESS_KEY": _env("MINIO_ROOT_PASSWORD", "voltstream-dev"),
    }
    flags: list[str] = []
    for key, value in pairs.items():
        flags += ["-e", f"{key}={value}"]
    return flags


# Runs inside the Spark image: the fixture readings, parsed against the same contract the
# stream uses and written with the archiver's own partitioning, in its exact column layout.
_WRITE_RAW_PARTITION = f"""
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BinaryType, StringType, StructField, StructType
from voltstream.storage.objectstore import raw_root
from voltstream.streaming.raw_archiver import with_partition_columns
from voltstream.streaming.session import build_session
from voltstream.streaming.sources import METER_READING_SCHEMA

spark = build_session("t172-fixture-writer")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
headers = ArrayType(
    StructType([StructField("key", StringType()), StructField("value", BinaryType())])
)
readings = spark.read.schema(METER_READING_SCHEMA).json("/fixtures/readings_{_DAY}.jsonl")
readings = (
    readings.withColumn("kafka_key", F.col("household_id"))
    .withColumn("kafka_timestamp", F.current_timestamp())
    .withColumn("kafka_partition", F.lit(0))
    .withColumn("kafka_offset", F.monotonically_increasing_id())
    .withColumn(
        "kafka_headers",
        F.array(
            F.struct(
                F.lit("trace_id").alias("key"), F.col("trace_id").cast("binary").alias("value")
            )
        ).cast(headers),
    )
)
columns = [f.name for f in METER_READING_SCHEMA.fields] + [
    "kafka_key", "kafka_timestamp", "kafka_partition", "kafka_offset", "kafka_headers",
    "ingest_ts", "sim_date", "hour",
]
(
    with_partition_columns(readings).select(*columns)
    .write.mode("overwrite").partitionBy("sim_date", "hour")
    .option("compression", "snappy").parquet(raw_root())
)
print("fixture rows written:", readings.count())
spark.stop()
"""


def _write_raw_partition() -> None:
    _run(
        "docker",
        "run",
        "--rm",
        "--network",
        "voltstream",
        "-v",
        f"{_FIXTURES.as_posix()}:/fixtures:ro",
        *_spark_env(),
        _SPARK_IMAGE,
        "python",
        "-c",
        _WRITE_RAW_PARTITION,
    )


def _put_tariff(text: str) -> None:
    _mc("pipe", f"local/voltstream-landing/tariff/tariff_{_DAY}.csv", stdin=text.encode("utf-8"))


def _bill_the_day(run_label: str) -> None:
    """The DAG's run_daily_billing command, in the same image, on the same network."""
    _run(
        "docker",
        "run",
        "--rm",
        "--network",
        "voltstream",
        *_spark_env(),
        "-e",
        f"VOLTSTREAM_ORCHESTRATOR_RUN_ID={run_label}",
        _SPARK_IMAGE,
        "spark-submit",
        "--master",
        "local[*]",
        "/opt/venv/lib/python3.11/site-packages/voltstream/batch/daily_billing.py",
        "--sim-date",
        _DAY,
    )


def _connect() -> psycopg.Connection:
    return psycopg.connect(
        host="localhost",
        port=int(_env("POSTGRES_HOST_PORT", "5432")),
        dbname=_env("POSTGRES_DB", "voltstream"),
        user=_env("POSTGRES_USER", "voltstream"),
        password=_env("POSTGRES_PASSWORD", "voltstream"),
    )


def _bills() -> dict[str, dict[str, object]]:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT household_id, {', '.join(_COMPARED)}, tariff_effective_date, tier_breakdown "
            "FROM household_bill_daily WHERE sim_date = %s ORDER BY household_id",
            (_DAY,),
        )
        names = [d.name for d in cur.description or []]
        return {row[0]: dict(zip(names, row, strict=True)) for row in cur.fetchall()}


def _expected() -> dict[str, dict[str, object]]:
    with (_FIXTURES / f"expected_bills_{_DAY}.csv").open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return {
        row["household_id"]: {
            column: int(row[column])
            if column.endswith("_count") or column == "duplicates_removed"
            else Decimal(row[column])
            for column in _COMPARED
        }
        for row in rows
    }


def _purge() -> None:
    """Every trace of the fixture day, in Postgres and in the object store."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM household_bill_daily WHERE sim_date = %s", (_DAY,))
        cur.execute("DELETE FROM household_bill_history WHERE sim_date = %s", (_DAY,))
        cur.execute("DELETE FROM pipeline_runs WHERE sim_date = %s", (_DAY,))
        conn.commit()
    for path in (
        f"local/voltstream-raw/meter_readings/sim_date={_DAY}",
        f"local/voltstream-landing/tariff/tariff_{_DAY}.csv",
        f"local/voltstream-archive/tariff/sim_date={_DAY}",
    ):
        _mc("rm", "--recursive", "--force", path, check=False)


@pytest.fixture(scope="module")
def billed_day() -> Iterator[dict[str, dict[str, object]]]:
    """The fixture day in place and billed once; the first run's bills."""
    if _run("docker", "inspect", "voltstream-postgres", check=False).returncode != 0:
        pytest.fail(
            "the Compose stack is not running: start it first (voltstream.ps1 start / make up)"
        )

    _run("docker", "exec", "voltstream-airflow", "airflow", "dags", "pause", "tariff_watcher")
    try:
        _purge()
        _write_raw_partition()
        _put_tariff((_FIXTURES / f"tariff_{_DAY}.csv").read_text(encoding="utf-8"))
        _bill_the_day("t172-run-1")
        yield _bills()
    finally:
        _purge()
        _run(
            "docker",
            "exec",
            "voltstream-airflow",
            "airflow",
            "dags",
            "unpause",
            "tariff_watcher",
            check=False,
        )


def test_the_bills_are_the_hand_computed_ones(billed_day: dict[str, dict[str, object]]) -> None:
    expected = _expected()
    assert set(billed_day) == set(expected)
    for household, want in expected.items():
        got = {column: billed_day[household][column] for column in _COMPARED}
        assert got == want, household
    # The three D5 worked examples, spelled out.
    assert billed_day["HH-0001"]["final_bill"] == Decimal("720.17")
    assert billed_day["HH-0002"]["final_bill"] == Decimal("454.41")
    assert billed_day["HH-0003"]["final_bill"] == Decimal("-150.00")


def test_rebilling_the_same_inputs_is_identical(billed_day: dict[str, dict[str, object]]) -> None:
    _bill_the_day("t172-run-2")
    again = _bills()
    assert again == billed_day


def test_a_changed_block_rate_changes_that_bill_and_no_other(
    billed_day: dict[str, dict[str, object]],
) -> None:
    tariff = (_FIXTURES / f"tariff_{_DAY}.csv").read_text(encoding="utf-8")
    raised = tariff.replace(
        f"HH-0001,{_DAY},TIER_2,false,0.00,240.00,8.00,",
        f"HH-0001,{_DAY},TIER_2,false,0.00,240.00,9.00,",
    )
    assert raised != tariff, "the fixture tariff's HH-0001 row has changed; update this test"
    _put_tariff(raised)

    _bill_the_day("t172-run-3")
    bills = _bills()

    # By hand: 60 x 9.00 = 540.00, plus 0.0100 x 16.50 = 0.17, plus the 240.00 fixed charge.
    assert bills["HH-0001"]["energy_charge"] == Decimal("540.17")
    assert bills["HH-0001"]["final_bill"] == Decimal("780.17")
    for household in ("HH-0002", "HH-0003", "HH-0004"):
        assert bills[household] == billed_day[household], household

    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, orchestrator_run_id FROM pipeline_runs "
            "WHERE sim_date = %s AND layer = 'batch_billing' ORDER BY started_at",
            (_DAY,),
        )
        ledger = cur.fetchall()
    assert ledger == [
        ("superseded", "t172-run-1"),
        ("superseded", "t172-run-2"),
        ("success", "t172-run-3"),
    ]


def test_every_runs_bill_and_tariff_stay_on_record(
    billed_day: dict[str, dict[str, object]],
) -> None:
    """R25, D6: the audit trail shows each run's bill and which run produced it, and each
    run's archived tariff, though only the last run's bills are current."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT r.orchestrator_run_id, r.status, h.final_bill, h.pipeline_run_id "
            "FROM household_bill_history h JOIN pipeline_runs r ON r.run_id = h.pipeline_run_id "
            "WHERE h.sim_date = %s AND h.household_id = 'HH-0001' ORDER BY r.started_at",
            (_DAY,),
        )
        history = cur.fetchall()
    assert [row[:3] for row in history] == [
        ("t172-run-1", "superseded", Decimal("720.17")),
        ("t172-run-2", "superseded", Decimal("720.17")),
        ("t172-run-3", "success", Decimal("780.17")),
    ]

    archived = _mc("ls", f"local/voltstream-archive/tariff/sim_date={_DAY}/").stdout.decode()
    for _, _, _, run_id in history:
        assert f"run_id={run_id}" in archived, f"no archived tariff for run {run_id}"
