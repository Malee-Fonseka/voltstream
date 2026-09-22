"""Unit tests for voltstream.simulators.faults (T071)."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from voltstream.config import FaultsConfig
from voltstream.contracts.events import MeterReading
from voltstream.core.validation import validate
from voltstream.simulators.faults import UNKNOWN_HOUSEHOLD_SENTINEL, FaultInjector

_TS = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)


def _reading(**overrides: object) -> MeterReading:
    defaults: dict[str, object] = {
        "schema_version": "1.0",
        "event_id": uuid4(),
        "trace_id": uuid4(),
        "meter_id": "MTR-0001",
        "household_id": "HH-0001",
        "grid_zone": "ZONE-A",
        "event_ts": _TS,
        "consumption_kwh": Decimal("0.500"),
        "solar_generation_kwh": Decimal("0.100"),
        "voltage": 230.0,
        "producer_id": "sim-01",
    }
    defaults.update(overrides)
    return MeterReading(**defaults)  # type: ignore[arg-type]


def _all_zero_faults(**overrides: object) -> FaultsConfig:
    defaults: dict[str, object] = {
        "duplicate_rate": 0.0,
        "out_of_order_rate": 0.0,
        "out_of_order_lateness_sim_minutes": (1, 30),
        "null_field_rate": 0.0,
        "negative_value_rate": 0.0,
        "unknown_household_rate": 0.0,
        "dropout_probability_per_meter_tick": 0.0,
        "dropout_duration_real_seconds": 30,
        "dropout_backfill": True,
    }
    defaults.update(overrides)
    return FaultsConfig(**defaults)  # type: ignore[arg-type]


def test_all_rates_zero_is_the_identity() -> None:
    injector = FaultInjector(_all_zero_faults(), random.Random(1))
    reading = _reading()
    assert injector.apply(reading) == [reading]


# ---------------------------------------------------------------------------
# Each rejectable fault type maps to exactly one core/validation.py reason.
# ---------------------------------------------------------------------------

_KNOWN = frozenset({"HH-0001"})
_ZONES = frozenset({"ZONE-A"})
_BOUNDS = (_TS - timedelta(days=1), _TS + timedelta(days=1))


def _reason_for(reading: MeterReading) -> str | None:
    result = validate(
        reading, known_household_ids=_KNOWN, configured_zones=_ZONES, event_ts_bounds=_BOUNDS
    )
    return result.reason


def test_null_field_fault_maps_to_null_field_reason() -> None:
    injector = FaultInjector(_all_zero_faults(null_field_rate=1.0), random.Random(1))
    [result] = injector.apply(_reading())
    assert _reason_for(result) == "null_field"


def test_negative_value_fault_maps_to_negative_kwh_reason() -> None:
    injector = FaultInjector(_all_zero_faults(negative_value_rate=1.0), random.Random(1))
    [result] = injector.apply(_reading())
    assert _reason_for(result) == "negative_kwh"


def test_unknown_household_fault_maps_to_unknown_household_reason() -> None:
    injector = FaultInjector(_all_zero_faults(unknown_household_rate=1.0), random.Random(1))
    [result] = injector.apply(_reading())
    assert result.household_id == UNKNOWN_HOUSEHOLD_SENTINEL
    assert _reason_for(result) == "unknown_household"


def test_out_of_order_and_backfilled_readings_are_valid_not_rejected() -> None:
    injector = FaultInjector(_all_zero_faults(out_of_order_rate=1.0), random.Random(1))
    [result] = injector.apply(_reading())
    assert result.event_ts < _reading().event_ts  # shifted backwards
    outcome = validate(
        result, known_household_ids=_KNOWN, configured_zones=_ZONES, event_ts_bounds=_BOUNDS
    )
    assert outcome.valid is True


def test_out_of_order_shift_within_configured_spread() -> None:
    cfg = _all_zero_faults(
        out_of_order_rate=1.0, out_of_order_lateness_sim_minutes=(5, 10)
    )
    injector = FaultInjector(cfg, random.Random(7))
    original = _reading()
    for _ in range(50):
        [result] = injector.apply(_reading())
        lateness = (original.event_ts - result.event_ts).total_seconds() / 60
        assert 5 <= lateness <= 10


# ---------------------------------------------------------------------------
# Duplicate: additive, on top of whatever the tick otherwise produced.
# ---------------------------------------------------------------------------


def test_duplicate_rate_one_always_emits_two() -> None:
    injector = FaultInjector(_all_zero_faults(duplicate_rate=1.0), random.Random(1))
    result = injector.apply(_reading())
    assert len(result) == 2
    assert result[0].meter_id == result[1].meter_id
    assert result[0].event_ts == result[1].event_ts


# ---------------------------------------------------------------------------
# Dropout: per-meter state machine, real time, deterministic via an injected clock.
# ---------------------------------------------------------------------------


def test_dropout_buffers_and_backfills_exactly_the_configured_duration() -> None:
    clock = {"now": datetime(2026, 1, 1, tzinfo=UTC)}
    cfg = _all_zero_faults(
        dropout_probability_per_meter_tick=1.0,
        dropout_duration_real_seconds=30,
        dropout_backfill=True,
    )
    injector = FaultInjector(cfg, random.Random(1), now_fn=lambda: clock["now"])

    emitted: list[MeterReading] = []
    for i in range(16):
        emitted.extend(injector.apply(_reading(event_ts=_TS + timedelta(minutes=i))))
        clock["now"] += timedelta(seconds=2)

    # 15 buffered (t=0..14 ticks, 2s apart, spanning 28s < 30s) + 1 live reconnection tick.
    assert len(emitted) == 16
    backfilled, live = emitted[:-1], emitted[-1]
    assert len(backfilled) == 15
    assert [r.event_ts for r in backfilled] == [_TS + timedelta(minutes=i) for i in range(15)]
    assert live.event_ts == _TS + timedelta(minutes=15)


def test_dropout_backfill_false_discards_the_buffer() -> None:
    clock = {"now": datetime(2026, 1, 1, tzinfo=UTC)}
    cfg = _all_zero_faults(
        dropout_probability_per_meter_tick=1.0,
        dropout_duration_real_seconds=10,
        dropout_backfill=False,
    )
    injector = FaultInjector(cfg, random.Random(1), now_fn=lambda: clock["now"])

    emitted: list[MeterReading] = []
    for i in range(6):
        emitted.extend(injector.apply(_reading(event_ts=_TS + timedelta(minutes=i))))
        clock["now"] += timedelta(seconds=2)

    # Buffer discarded: only the live reconnection-tick reading survives.
    assert len(emitted) == 1
    assert emitted[0].event_ts == _TS + timedelta(minutes=5)


def test_is_dropped_out_reflects_state() -> None:
    clock = {"now": datetime(2026, 1, 1, tzinfo=UTC)}
    cfg = _all_zero_faults(
        dropout_probability_per_meter_tick=1.0, dropout_duration_real_seconds=10
    )
    injector = FaultInjector(cfg, random.Random(1), now_fn=lambda: clock["now"])

    assert injector.is_dropped_out("MTR-0001") is False
    injector.apply(_reading())
    assert injector.is_dropped_out("MTR-0001") is True
    clock["now"] += timedelta(seconds=10)
    injector.apply(_reading(event_ts=_TS + timedelta(minutes=1)))
    assert injector.is_dropped_out("MTR-0001") is False


# ---------------------------------------------------------------------------
# Injection rates are honoured over many samples.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field_name,rate",
    [
        ("null_field_rate", 0.1),
        ("negative_value_rate", 0.05),
        ("unknown_household_rate", 0.02),
        ("duplicate_rate", 0.15),
    ],
)
def test_observed_rate_matches_configured_rate_over_many_samples(
    field_name: str, rate: float
) -> None:
    cfg = _all_zero_faults(**{field_name: rate})
    injector = FaultInjector(cfg, random.Random(42))
    n = 10_000

    if field_name == "duplicate_rate":
        count = sum(len(injector.apply(_reading())) - 1 for _ in range(n))
    else:
        count = sum(
            1
            for _ in range(n)
            if _reason_for(injector.apply(_reading())[0])
            in {"null_field", "negative_kwh", "unknown_household"}
        )
    observed = count / n
    assert abs(observed - rate) < 0.02
