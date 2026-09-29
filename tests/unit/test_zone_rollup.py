"""Unit tests for the daily zone rollup, its cross-check and retention (T124, T125, R24, R32)."""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

from voltstream.batch.daily_zone_rollup import (
    CrossCheckFailed,
    cross_check,
    speed_window_cutoff,
    zone_rollup,
)
from voltstream.core.netting import net

_KWH = DecimalType(12, 4)
_DAY = date(2026, 1, 2)

_VALID_SCHEMA = StructType(
    [
        StructField("grid_zone", StringType()),
        StructField("household_id", StringType()),
        StructField("meter_id", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("consumption_kwh", _KWH),
        StructField("solar_generation_kwh", _KWH),
    ]
)


def _reading(household: str, hour: int, consumption: str, solar: str) -> tuple:
    return (
        "ZONE-A",
        household,
        f"MTR-{household}",
        datetime(2026, 1, 2, hour, tzinfo=UTC),
        Decimal(consumption),
        Decimal(solar),
    )


def test_zone_netting_is_the_sum_of_the_households_bills(spark) -> None:  # type: ignore[no-untyped-def]
    """R24: HH-1 exports at noon and imports in the evening. Its bill nets the day, so
    it exported nothing; netting each reading used to report 0.8 kWh of export."""
    readings = [
        _reading("HH-1", 12, "0.2000", "1.0000"),
        _reading("HH-1", 19, "1.5000", "0.0000"),
        _reading("HH-2", 12, "0.1000", "0.9000"),
    ]
    rows = zone_rollup(
        spark.createDataFrame(readings, schema=_VALID_SCHEMA), _DAY, uuid.uuid4()
    ).collect()
    zone = rows[0]

    hh1 = net(Decimal("1.7000"), Decimal("1.0000"))
    hh2 = net(Decimal("0.1000"), Decimal("0.9000"))
    assert zone["export_kwh"] == hh1.export_kwh + hh2.export_kwh == Decimal("0.8000")
    assert zone["self_consumed_kwh"] == hh1.self_consumed_kwh + hh2.self_consumed_kwh
    assert zone["total_consumption_kwh"] == Decimal("1.8000")
    assert zone["readings_count"] == 3
    assert zone["active_meters"] == 2


def test_cross_check_passes_within_tolerance() -> None:
    cross_check(Decimal("100.0000"), Decimal("100.0090"), _DAY)


def test_cross_check_fails_a_day_that_does_not_add_up() -> None:
    """T125: a corrupted zone total withholds the day rather than publishing it."""
    with pytest.raises(CrossCheckFailed, match="disagree"):
        cross_check(Decimal("337.0000"), Decimal("216.0000"), _DAY)


def test_cross_check_fails_an_unbilled_day() -> None:
    with pytest.raises(CrossCheckFailed):
        cross_check(Decimal("12.5000"), Decimal(0), _DAY)


def test_the_speed_view_keeps_two_simulated_weeks_of_windows() -> None:
    """R32: the table grew by ~480 rows a simulated day, forever."""
    cutoff = speed_window_cutoff(_DAY)
    assert cutoff == datetime(2025, 12, 19, tzinfo=UTC)
    assert _DAY - cutoff.date() >= timedelta(days=12), "the longest dashboard looks back 1 h"
