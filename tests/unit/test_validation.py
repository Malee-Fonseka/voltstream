"""Unit tests for voltstream.core.validation (T054)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from voltstream.contracts.events import MeterReading
from voltstream.core.validation import REJECTION_REASONS, validate

_KNOWN_HOUSEHOLDS = frozenset({"HH-0042"})
_ZONES = frozenset({"ZONE-A", "ZONE-B", "ZONE-C"})
_WINDOW_LOW = datetime(2026, 1, 1, tzinfo=UTC)
_WINDOW_HIGH = datetime(2027, 1, 1, tzinfo=UTC)


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


def _validate(reading: MeterReading, **overrides: object) -> object:
    kwargs: dict[str, object] = {
        "known_household_ids": _KNOWN_HOUSEHOLDS,
        "configured_zones": _ZONES,
        "event_ts_bounds": (_WINDOW_LOW, _WINDOW_HIGH),
    }
    kwargs.update(overrides)
    return validate(reading, **kwargs)  # type: ignore[arg-type]


def test_clean_reading_passes_every_rule() -> None:
    result = _validate(_reading())
    assert result.valid is True
    assert result.reason is None
    assert result.warnings == ()


def test_null_field_rejects_and_reason_is_exact() -> None:
    result = _validate(_reading(household_id=""))
    assert result.valid is False
    assert result.reason == "null_field"


def test_negative_consumption_rejects() -> None:
    result = _validate(_reading(consumption_kwh=Decimal("-0.01")))
    assert result.valid is False
    assert result.reason == "negative_kwh"


def test_negative_solar_rejects() -> None:
    result = _validate(_reading(solar_generation_kwh=Decimal("-0.01")))
    assert result.valid is False
    assert result.reason == "negative_kwh"


def test_unknown_household_rejects() -> None:
    result = _validate(_reading(household_id="HH-9999"))
    assert result.valid is False
    assert result.reason == "unknown_household"


def test_unknown_zone_rejects() -> None:
    result = _validate(_reading(grid_zone="ZONE-Z"))
    assert result.valid is False
    assert result.reason == "unknown_zone"


def test_event_ts_out_of_range_rejects() -> None:
    too_early = _WINDOW_LOW - timedelta(days=1)
    result = _validate(_reading(event_ts=too_early))
    assert result.valid is False
    assert result.reason == "event_ts_out_of_range"


def test_voltage_out_of_bounds_warns_but_does_not_reject() -> None:
    result = _validate(_reading(voltage=999.0))
    assert result.valid is True
    assert result.reason is None
    assert "voltage_out_of_range" in result.warnings


def test_rejection_reasons_vocabulary_is_exactly_five() -> None:
    assert REJECTION_REASONS == {
        "null_field",
        "negative_kwh",
        "unknown_household",
        "unknown_zone",
        "event_ts_out_of_range",
    }
