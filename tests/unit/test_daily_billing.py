"""Unit tests for the billing job's pure parts (T118).

Static DataFrames and the local Spark fixture — no Kafka, no MinIO, no Postgres. What is
under test is the logic that decides *which* rows a bill is computed from: deduplication,
the effective-dated tariff rule, and the refusal to bill a household with no tariff. The
arithmetic itself is `core/`'s and is covered by the consistency and property tests.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

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
    deduplicate,
    join_tariff,
)

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


def test_effective_dated_rule_picks_the_latest_applicable_row(spark) -> None:  # type: ignore[no-untyped-def]
    """§10.2: latest row with effective_date <= sim_date, not full SCD Type 2.

    Exercised through the same window logic `read_tariff` applies, over a static frame —
    reading an actual CSV would make this an integration test for no extra confidence in
    the rule itself.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    sim_date = date(2026, 1, 2)
    rows = spark.createDataFrame(
        [
            ("HH-1", "2025-12-01", Decimal("100.00")),
            ("HH-1", "2026-01-02", Decimal("120.00")),
            # After the day being billed: must not win.
            ("HH-1", "2026-02-01", Decimal("999.00")),
        ],
        schema=_TARIFF_SCHEMA,
    ).withColumn("effective_date", F.to_date(F.col("effective_date")))

    applicable = rows.filter(F.col("effective_date") <= F.lit(sim_date))
    latest = Window.partitionBy("household_id").orderBy(F.col("effective_date").desc())
    out = (
        applicable.withColumn("_rank", F.row_number().over(latest))
        .filter(F.col("_rank") == 1)
        .collect()
    )

    assert len(out) == 1
    assert out[0]["fixed_charge"] == Decimal("120.00"), "the 2026-01-02 row, not the future one"


# ---------------------------------------------------------------------------------------
# R38: a new attempt closes the ledger rows earlier attempts left at `running`.
# ---------------------------------------------------------------------------------------


class _RecordingCursor:
    def __init__(self, rowcounts: list[int]) -> None:
        self.statements: list[str] = []
        self._rowcounts = rowcounts
        self.rowcount = 0

    def execute(self, sql: str, params: object = None) -> None:
        self.statements.append(" ".join(sql.split()))
        self.rowcount = self._rowcounts.pop(0) if self._rowcounts else 1

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
    import uuid

    from voltstream.batch import daily_billing

    cursor = _RecordingCursor(rowcounts=[1, 1])  # one abandoned row, then the insert
    conn = _RecordingConnection(cursor)
    monkeypatch.setattr(daily_billing, "connect", lambda *a, **k: conn)

    daily_billing._start_run(uuid.uuid4(), date(2026, 2, 24))

    close, open_ = cursor.statements
    assert close.startswith("UPDATE pipeline_runs SET status = 'failed'")
    assert "status = 'running'" in close and "layer = 'batch_billing'" in close
    assert open_.startswith("INSERT INTO pipeline_runs")
    assert conn.committed, "both statements must land in one transaction"
    assert "closed abandoned billing runs" in capsys.readouterr().out
