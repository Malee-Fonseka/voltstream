"""Unit tests for the speed layer's aggregations and provisional bill.

These run over static DataFrames with the local Spark fixture — no Kafka, no Postgres, no
object store. What they pin down is the arithmetic and the column shapes, which is where a
silent wrong answer would come from. Whether the streaming plumbing works is a question
for the integration tests.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from voltstream.streaming.speed_layer import (
    household_aggregation,
    with_provisional_bill,
    zone_aggregation,
)

_KWH = DecimalType(12, 4)
_TS = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

_READING_SCHEMA = StructType(
    [
        StructField("event_ts", TimestampType()),
        StructField("grid_zone", StringType()),
        StructField("household_id", StringType()),
        StructField("meter_id", StringType()),
        StructField("consumption_kwh", _KWH),
        StructField("solar_generation_kwh", _KWH),
    ]
)

_TARIFF_SCHEMA = StructType(
    [
        StructField("household_id", StringType()),
        StructField("subsidy_flag", BooleanType()),
        StructField("subsidy_pct", DecimalType(5, 2)),
        StructField("fixed_charge", DecimalType(12, 2)),
        StructField("block_1_rate", DecimalType(12, 2)),
        StructField("block_2_rate", DecimalType(12, 2)),
        StructField("block_3_rate", DecimalType(12, 2)),
        StructField("export_rate", DecimalType(12, 2)),
    ]
)


def _reading(zone: str, household: str, meter: str, consumption: str, solar: str, ts=_TS):
    return (ts, zone, household, meter, Decimal(consumption), Decimal(solar))


@pytest.fixture(scope="module")
def zone_rows(spark):  # type: ignore[no-untyped-def]
    df = spark.createDataFrame(
        [
            _reading("ZONE-A", "HH-0001", "MTR-0001", "2.0000", "0.5000"),
            _reading("ZONE-A", "HH-0002", "MTR-0002", "2.0000", "1.5000"),
            # Second reading from a meter already counted - active_meters must not
            # double-count it.
            _reading("ZONE-A", "HH-0001", "MTR-0001", "1.0000", "0.0000"),
            _reading("ZONE-B", "HH-0003", "MTR-0003", "4.0000", "0.0000"),
        ],
        schema=_READING_SCHEMA,
    )
    return {r["grid_zone"]: r for r in zone_aggregation(df).collect()}


def test_zone_totals_sum_every_reading(zone_rows) -> None:  # type: ignore[no-untyped-def]
    a = zone_rows["ZONE-A"]
    assert a["total_consumption_kwh"] == Decimal("5.0000")
    assert a["total_solar_kwh"] == Decimal("2.0000")


def test_active_meters_counts_distinct_meters(zone_rows) -> None:  # type: ignore[no-untyped-def]
    """Three readings from two meters is two active meters, not three."""
    assert zone_rows["ZONE-A"]["active_meters"] == 2


def test_renewable_ratio_is_solar_over_consumption(zone_rows) -> None:  # type: ignore[no-untyped-def]
    assert zone_rows["ZONE-A"]["renewable_ratio"] == Decimal("0.4000")
    assert zone_rows["ZONE-B"]["renewable_ratio"] == Decimal("0.0000")


def test_window_bounds_are_present_and_ordered(zone_rows) -> None:  # type: ignore[no-untyped-def]
    a = zone_rows["ZONE-A"]
    assert a["window_start"] < a["window_end"]


def test_zero_consumption_zone_does_not_divide_by_zero(spark) -> None:  # type: ignore[no-untyped-def]
    """A zone whose meters are all dropped out is expected, not exceptional.

    Without the guard this writes a null into a NOT NULL column and kills the query.
    """
    df = spark.createDataFrame(
        [_reading("ZONE-C", "HH-0009", "MTR-0009", "0.0000", "0.0000")],
        schema=_READING_SCHEMA,
    )
    row = zone_aggregation(df).collect()[0]
    assert row["renewable_ratio"] == Decimal("0.0000")


def test_household_totals_are_per_household_per_day(spark) -> None:  # type: ignore[no-untyped-def]
    df = spark.createDataFrame(
        [
            _reading("ZONE-A", "HH-0001", "MTR-0001", "3.0000", "1.0000"),
            _reading("ZONE-A", "HH-0001", "MTR-0001", "2.0000", "0.5000"),
            _reading("ZONE-A", "HH-0002", "MTR-0002", "1.0000", "0.0000"),
        ],
        schema=_READING_SCHEMA,
    )
    rows = {r["household_id"]: r for r in household_aggregation(df).collect()}
    assert rows["HH-0001"]["consumption_kwh"] == Decimal("5.0000")
    assert rows["HH-0001"]["solar_kwh"] == Decimal("1.5000")
    assert rows["HH-0001"]["sim_date"] == date(2026, 1, 1)
    assert rows["HH-0002"]["consumption_kwh"] == Decimal("1.0000")


@pytest.fixture(scope="module")
def bill_row(spark):  # type: ignore[no-untyped-def]
    totals = spark.createDataFrame(
        [("HH-0001", date(2026, 1, 2), Decimal("100.0000"), Decimal("20.0000"))],
        schema="household_id string, sim_date date, consumption_kwh decimal(12,4), "
        "solar_kwh decimal(12,4)",
    )
    tariff = spark.createDataFrame(
        [
            (
                "HH-0001",
                True,
                Decimal("25.00"),
                Decimal("240.00"),
                Decimal("8.00"),
                Decimal("16.50"),
                Decimal("24.50"),
                Decimal("18.00"),
            )
        ],
        schema=_TARIFF_SCHEMA,
    )
    return with_provisional_bill(totals, tariff, date(2026, 1, 1)).collect()[0]


def test_every_bill_component_is_persisted(bill_row) -> None:  # type: ignore[no-untyped-def]
    """D4: the breakdown is stored, not just the total.

    Reconciliation has to explain *why* the speed estimate and the batch final differ,
    and that is impossible to recover from the total after the fact.
    """
    for column in (
        "self_consumed_kwh",
        "billable_import_kwh",
        "export_kwh",
        "energy_charge",
        "fixed_charge",
        "subsidy_discount",
        "export_credit",
        "tier_breakdown",
        "estimated_bill",
    ):
        assert bill_row[column] is not None, f"{column} is null"


def test_estimated_bill_equals_its_components(bill_row) -> None:  # type: ignore[no-untyped-def]
    """The identity T091 states: energy + fixed - subsidy - export."""
    expected = (
        bill_row["energy_charge"]
        + bill_row["fixed_charge"]
        - bill_row["subsidy_discount"]
        - bill_row["export_credit"]
    )
    assert bill_row["estimated_bill"] == expected


def test_netting_applies_before_the_tariff(bill_row) -> None:  # type: ignore[no-untyped-def]
    """100 kWh consumed against 20 kWh solar bills 80, not 100."""
    assert bill_row["self_consumed_kwh"] == Decimal("20.0000")
    assert bill_row["billable_import_kwh"] == Decimal("80.0000")
    assert bill_row["export_kwh"] == Decimal("0.0000")


def test_tariff_source_date_is_yesterday(bill_row) -> None:  # type: ignore[no-untyped-def]
    """Deliberately stale, deliberately labelled (§3.1)."""
    assert bill_row["tariff_source_date"] == date(2026, 1, 1)
    assert bill_row["sim_date"] == date(2026, 1, 2)
    assert bill_row["tariff_source_date"] < bill_row["sim_date"]


def test_surplus_solar_saturates_the_ratio(spark) -> None:  # type: ignore[no-untyped-def]
    """A zone generating more than it consumes must not overflow renewable_ratio.

    Regression test. The column is NUMERIC(5,4), so a ratio of 14.3 — a real value from a
    low-demand zone at midday — cast to null and violated the NOT NULL constraint, which
    killed the streaming query outright. The original tests only ever had solar below
    consumption, so none of them touched this.
    """
    df = spark.createDataFrame(
        [_reading("ZONE-C", "HH-0009", "MTR-0009", "0.4917", "7.0519")],
        schema=_READING_SCHEMA,
    )
    row = zone_aggregation(df).collect()[0]
    assert row["renewable_ratio"] == Decimal("1.0000")
    # The surplus is still recoverable: both raw totals are kept on the same row.
    assert row["total_solar_kwh"] > row["total_consumption_kwh"]
