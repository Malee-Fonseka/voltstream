"""Unit tests for voltstream.core.keys (T052)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from voltstream.config import get_config
from voltstream.contracts.events import MeterReading
from voltstream.core.keys import DEDUP_COLUMNS, dedup_key, parquet_partition, partition_key


def _reading(**overrides: object) -> MeterReading:
    defaults: dict[str, object] = {
        "schema_version": "1.0",
        "event_id": uuid4(),
        "trace_id": uuid4(),
        "meter_id": "MTR-0042",
        "household_id": "HH-0042",
        "grid_zone": "ZONE-C",
        "event_ts": datetime(2026, 8, 10, 14, 23, 0, tzinfo=UTC),
        "consumption_kwh": Decimal("0.412"),
        "solar_generation_kwh": Decimal("0.180"),
        "voltage": 232.4,
        "producer_id": "sim-01",
    }
    defaults.update(overrides)
    return MeterReading(**defaults)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def _clear_config_cache() -> Iterator[None]:
    get_config.cache_clear()
    yield
    get_config.cache_clear()


def test_dedup_key_ignores_event_id() -> None:
    a = _reading(event_id=uuid4())
    b = _reading(event_id=uuid4())
    assert dedup_key(a) == dedup_key(b)


def test_dedup_key_differs_on_event_ts() -> None:
    a = _reading(event_ts=datetime(2026, 8, 10, 14, 23, 0, tzinfo=UTC))
    b = _reading(event_ts=datetime(2026, 8, 10, 14, 23, 1, tzinfo=UTC))
    assert dedup_key(a) != dedup_key(b)


def test_dedup_key_differs_on_meter_id() -> None:
    a = _reading(meter_id="MTR-0001")
    b = _reading(meter_id="MTR-0002")
    assert dedup_key(a) != dedup_key(b)


def test_dedup_key_shape() -> None:
    r = _reading()
    assert dedup_key(r) == (r.meter_id, r.event_ts)


def test_dedup_columns_name_the_dedup_key() -> None:
    """The Spark jobs dedup on DEDUP_COLUMNS (R02); it must be the same key, field for field."""
    r = _reading()
    assert tuple(getattr(r, column) for column in DEDUP_COLUMNS) == dedup_key(r)


def test_partition_key_equals_household_id() -> None:
    r = _reading(household_id="HH-0099")
    assert partition_key(r) == "HH-0099"


def test_parquet_partition_matches_simclock() -> None:
    r = _reading(event_ts=datetime(2026, 8, 10, 14, 23, 0, tzinfo=UTC))
    sim_date, hour = parquet_partition(r)
    assert sim_date.isoformat() == "2026-08-10"
    assert hour == 14
