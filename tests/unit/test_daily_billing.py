"""Unit tests for the billing job's pure parts (T118).

Static DataFrames and the local Spark fixture — no Kafka, no MinIO, no Postgres. What is
under test is the logic that decides *which* rows a bill is computed from: deduplication,
the effective-dated tariff rule, and the refusal to bill a household with no tariff. The
arithmetic itself is `core/`'s and is covered by the consistency and property tests.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from voltstream.batch.daily_billing import (
    MissingTariffError,
    aggregate_to_daily,
    compute_bills,
    deduplicate,
    join_tariff,
    with_idle_households,
)
from voltstream.streaming.reference import read_tariff

_KWH = DecimalType(12, 4)
_TS = datetime(2026, 1, 2, 10, 0, tzinfo=UTC)

_RAW_SCHEMA = StructType(
    [
        StructField("meter_id", StringType()),
        StructField("household_id", StringType()),
        StructField("grid_zone", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("consumption_kwh", _KWH),
        StructField("solar_generation_kwh", _KWH),
        StructField("ingest_ts", TimestampType()),
    ]
)

_TOTALS_SCHEMA = StructType(
    [
        StructField("household_id", StringType()),
        StructField("consumption_kwh", _KWH),
        StructField("solar_kwh", _KWH),
        StructField("readings_count", IntegerType()),
    ]
)

_TARIFF_SCHEMA = StructType(
    [
        StructField("household_id", StringType()),
        StructField("effective_date", StringType()),
        StructField("fixed_charge", DecimalType(12, 2)),
    ]
)


def _raw(meter: str, household: str, kwh: str, event_ts=_TS, ingest_ts=_TS):
    return (meter, household, "ZONE-A", event_ts, Decimal(kwh), Decimal("0.0000"), ingest_ts)


def test_duplicates_collapse_on_meter_and_event_ts(spark) -> None:  # type: ignore[no-untyped-def]
    """The dedup key is (meter_id, event_ts) — not event_id, which is fresh per emission."""
    rows = [
        _raw("MTR-1", "HH-1", "5.0000", ingest_ts=_TS),
        _raw("MTR-1", "HH-1", "5.0000", ingest_ts=_TS.replace(minute=5)),
        _raw("MTR-1", "HH-1", "7.0000", event_ts=_TS.replace(hour=11)),
    ]
    out = deduplicate(spark.createDataFrame(rows, schema=_RAW_SCHEMA)).collect()
    assert len(out) == 2, "two distinct (meter_id, event_ts) pairs"


def test_dedup_keeps_the_earliest_ingest(spark) -> None:  # type: ignore[no-untyped-def]
    """Deterministic across reruns: the first arrival wins, not an arbitrary row.

    Both copies carry the same event_ts, so only the ingest time distinguishes them. If
    the choice were arbitrary a restatement could produce a different bill from identical
    input, which would defeat the point of restatement.
    """
    early, late = _TS, _TS.replace(minute=30)
    rows = [
        ("MTR-1", "HH-1", "ZONE-A", _TS, Decimal("1.0000"), Decimal("0.0000"), late),
        ("MTR-1", "HH-1", "ZONE-A", _TS, Decimal("9.0000"), Decimal("0.0000"), early),
    ]
    out = deduplicate(spark.createDataFrame(rows, schema=_RAW_SCHEMA)).collect()
    assert len(out) == 1
    assert out[0]["consumption_kwh"] == Decimal("9.0000"), "row with the earlier ingest_ts"


def test_readings_from_different_meters_are_not_duplicates(spark) -> None:  # type: ignore[no-untyped-def]
    rows = [_raw("MTR-1", "HH-1", "5.0000"), _raw("MTR-2", "HH-2", "5.0000")]
    assert len(deduplicate(spark.createDataFrame(rows, schema=_RAW_SCHEMA)).collect()) == 2


def test_daily_aggregate_sums_per_household(spark) -> None:  # type: ignore[no-untyped-def]
    rows = [
        _raw("MTR-1", "HH-1", "3.0000"),
        _raw("MTR-1", "HH-1", "2.0000", event_ts=_TS.replace(hour=11)),
        _raw("MTR-2", "HH-2", "4.0000"),
    ]
    out = {
        r["household_id"]: r
        for r in aggregate_to_daily(spark.createDataFrame(rows, schema=_RAW_SCHEMA)).collect()
    }
    assert out["HH-1"]["consumption_kwh"] == Decimal("5.0000")
    assert out["HH-1"]["readings_count"] == 2
    assert out["HH-2"]["readings_count"] == 1


def test_join_fails_loudly_when_a_household_has_no_tariff(spark) -> None:  # type: ignore[no-untyped-def]
    """A null bill looks like a household that owes nothing. That must never ship.

    T113 requires the job to fail naming the household rather than emit the row.
    """
    totals = spark.createDataFrame(
        [
            ("HH-1", Decimal("10.0000"), Decimal("0.0000"), 1),
            ("HH-MISSING", Decimal("10.0000"), Decimal("0.0000"), 1),
        ],
        schema=_TOTALS_SCHEMA,
    )
    tariff = spark.createDataFrame(
        [("HH-1", "2026-01-02", Decimal("120.00"))], schema=_TARIFF_SCHEMA
    )

    with pytest.raises(MissingTariffError, match="HH-MISSING"):
        join_tariff(totals, tariff)


def test_join_succeeds_when_every_household_has_a_tariff(spark) -> None:  # type: ignore[no-untyped-def]
    totals = spark.createDataFrame(
        [("HH-1", Decimal("10.0000"), Decimal("0.0000"), 1)], schema=_TOTALS_SCHEMA
    )
    tariff = spark.createDataFrame(
        [("HH-1", "2026-01-02", Decimal("120.00"))], schema=_TARIFF_SCHEMA
    )
    out = join_tariff(totals, tariff).collect()
    assert len(out) == 1
    assert out[0]["fixed_charge"] == Decimal("120.00")


# ---------------------------------------------------------------------------------------
# R22: the tariff file, read the way both layers now read it.
# ---------------------------------------------------------------------------------------

_TARIFF_HEADER = (
    "household_id,effective_date,billing_tier,subsidy_flag,subsidy_pct,fixed_charge,"
    "block_1_rate,block_2_rate,block_3_rate,export_rate"
)
_DAY = date(2026, 1, 2)


def _tariff_file(tmp_path: Path, *rows: str, header: str = _TARIFF_HEADER) -> str:
    path = tmp_path / "tariff.csv"
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    return path.as_uri()


def _tariff_row(household: str, effective: str = "2026-01-02", **overrides: str) -> str:
    values = {
        "billing_tier": "TIER_2",
        "subsidy_flag": "false",
        "subsidy_pct": "25.00",
        "fixed_charge": "240.00",
        "block_1_rate": "8.00",
        "block_2_rate": "16.50",
        "block_3_rate": "24.50",
        "export_rate": "18.00",
    } | overrides
    return ",".join([household, effective, *values.values()])


def test_effective_dated_rule_picks_the_latest_applicable_row(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """§10.2: latest row with effective_date <= sim_date, not full SCD Type 2."""
    path = _tariff_file(
        tmp_path,
        _tariff_row("HH-1", "2025-12-01", fixed_charge="100.00"),
        _tariff_row("HH-1", "2026-01-02", fixed_charge="120.00"),
        # After the day being billed: must not win.
        _tariff_row("HH-1", "2026-02-01", fixed_charge="999.00"),
    )
    out = read_tariff(spark, _DAY, path).collect()

    assert len(out) == 1
    assert out[0]["fixed_charge"] == Decimal("120.00"), "the 2026-01-02 row, not the future one"
    assert out[0]["effective_date"] == _DAY


def test_the_tariff_is_one_partition(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """Fifty rows; archiving them must write one file, not one per core."""
    path = _tariff_file(tmp_path, _tariff_row("HH-1"), _tariff_row("HH-2"))
    assert read_tariff(spark, _DAY, path).rdd.getNumPartitions() == 1


def test_every_spelling_the_contract_accepts_means_subsidised(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """R22: only the literal `true` used to count, so a restated file with `1` lost it."""
    path = _tariff_file(
        tmp_path,
        _tariff_row("HH-1", subsidy_flag="true"),
        _tariff_row("HH-2", subsidy_flag="1"),
        _tariff_row("HH-3", subsidy_flag="True"),
        _tariff_row("HH-4", subsidy_flag="0"),
    )
    flags = {r["household_id"]: r["subsidy_flag"] for r in read_tariff(spark, _DAY, path).collect()}
    assert flags == {"HH-1": True, "HH-2": True, "HH-3": True, "HH-4": False}


def test_a_row_that_breaks_the_contract_fails_naming_its_line(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    path = _tariff_file(tmp_path, _tariff_row("HH-1"), _tariff_row("HH-2", block_1_rate="-8.00"))
    with pytest.raises(ValueError, match="line 3"):
        read_tariff(spark, _DAY, path)


def test_swapped_columns_fail_rather_than_bill_on_the_wrong_rates(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    swapped = _TARIFF_HEADER.replace("block_1_rate,block_2_rate", "block_2_rate,block_1_rate")
    path = _tariff_file(tmp_path, _tariff_row("HH-1"), header=swapped)
    with pytest.raises(Exception, match="(?i)header"):
        read_tariff(spark, _DAY, path)


# ---------------------------------------------------------------------------------------
# R35: a household with no valid reading all day is still billed.
# ---------------------------------------------------------------------------------------


def test_a_household_with_no_readings_is_billed_its_fixed_charge(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    import uuid

    from pyspark.sql import functions as F

    tariff = read_tariff(
        spark,
        _DAY,
        _tariff_file(tmp_path, _tariff_row("HH-1"), _tariff_row("HH-2", fixed_charge="120.00")),
    )
    totals = spark.createDataFrame(
        [("HH-1", Decimal("10.0000"), Decimal("0.0000"), 150)], schema=_TOTALS_SCHEMA
    )

    totals = with_idle_households(totals, tariff).withColumn("duplicates_removed", F.lit(0))
    bills = {
        r["household_id"]: r
        for r in compute_bills(join_tariff(totals, tariff), _DAY, uuid.uuid4()).collect()
    }

    assert set(bills) == {"HH-1", "HH-2"}
    assert bills["HH-2"]["readings_count"] == 0
    assert bills["HH-2"]["consumption_kwh"] == Decimal("0.0000")
    assert bills["HH-2"]["final_bill"] == Decimal("120.00"), "the fixed charge and nothing else"
    assert bills["HH-1"]["readings_count"] == 150, "a household with readings is unchanged"


# ---------------------------------------------------------------------------------------
# Recording doubles for the Postgres connection.
# ---------------------------------------------------------------------------------------


class _RecordingCursor:
    def __init__(self, rowcounts: list[int] | None = None) -> None:
        self.statements: list[str] = []
        self.params: list[object] = []
        self._rowcounts = rowcounts or []
        self.rowcount = 0

    def execute(self, sql: str, params: object = None) -> None:
        self.statements.append(" ".join(sql.split()))
        self.params.append(params)
        self.rowcount = self._rowcounts.pop(0) if self._rowcounts else 1

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        self.statements.append(" ".join(sql.split()))
        self.params.append(rows)

    def __enter__(self) -> _RecordingCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class _RecordingConnection:
    def __init__(self, cursor: _RecordingCursor) -> None:
        self._cursor = cursor
        self.committed = False

    def cursor(self) -> _RecordingCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def __enter__(self) -> _RecordingConnection:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def test_starting_a_run_closes_the_days_abandoned_running_rows(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """R38: a new attempt closes the ledger rows earlier attempts left at `running`."""
    import uuid

    from voltstream.batch import ledger

    cursor = _RecordingCursor(rowcounts=[1, 1])  # one abandoned row, then the insert
    conn = _RecordingConnection(cursor)
    monkeypatch.setattr(ledger, "connect", lambda *a, **k: conn)

    ledger.start_run(uuid.uuid4(), date(2026, 2, 24), "batch_rollup")

    close, open_ = cursor.statements
    assert close.startswith("UPDATE pipeline_runs SET status = 'failed'")
    assert "status = 'running'" in close
    assert cursor.params[0] == (date(2026, 2, 24), "batch_rollup"), "only this day and layer"
    assert open_.startswith("INSERT INTO pipeline_runs")
    assert "batch_rollup" in cursor.params[1]  # type: ignore[operator]
    assert conn.committed, "both statements must land in one transaction"
    assert "closed abandoned runs" in capsys.readouterr().out


def test_a_run_replaces_the_day_and_keeps_its_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R23: every retry and restatement used to append another full set of rejects.
    R25: and to overwrite the previous run's bills, leaving no record of them."""
    import uuid

    from voltstream.batch import daily_billing

    cursor = _RecordingCursor()
    conn = _RecordingConnection(cursor)
    monkeypatch.setattr(daily_billing, "connect", lambda *a, **k: conn)
    day = date(2026, 2, 24)
    bills = [("HH-1", day)]
    rejects = [("batch", "negative_kwh", "trace-1", '{"event_ts": "2026-02-24T10:00:00Z"}')]

    daily_billing.finalise(bills, ["household_id", "sim_date"], uuid.uuid4(), day, 150, rejects)

    (
        supersede,
        delete_rejects,
        insert_rejects,
        delete_bills,
        insert_bills,
        insert_history,
        complete,
    ) = cursor.statements
    assert supersede.startswith("UPDATE pipeline_runs SET status = 'superseded'")
    assert delete_rejects.startswith("DELETE FROM rejected_records WHERE stage = 'batch'")
    assert cursor.params[1] == (day,)
    assert insert_rejects.startswith("INSERT INTO rejected_records")
    assert cursor.params[2] == rejects
    assert delete_bills.startswith("DELETE FROM household_bill_daily WHERE sim_date")
    assert insert_bills.startswith("INSERT INTO household_bill_daily")
    assert "ON CONFLICT" not in insert_bills, "replaced, so no stale row survives"
    assert insert_history.startswith("INSERT INTO household_bill_history")
    assert cursor.params[5] == bills, "the history gets exactly the bills this run wrote"
    assert complete.startswith("UPDATE pipeline_runs SET status = 'success'")
    assert conn.committed, "rejects, bills, history and the ledger land in one transaction"
