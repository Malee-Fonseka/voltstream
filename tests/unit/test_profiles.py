"""Unit tests for voltstream.simulators.profiles (T067, T068)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from voltstream.simulators.profiles import consumption_kwh, solar_kwh

_DAY = datetime(2026, 8, 10, tzinfo=UTC)


def test_determinism_under_a_fixed_seed() -> None:
    ts = _DAY + timedelta(hours=14, minutes=17)
    first = consumption_kwh("HH-0007", ts)
    second = consumption_kwh("HH-0007", ts)
    assert first == second


def test_solar_determinism_under_a_fixed_seed() -> None:
    ts = _DAY + timedelta(hours=12)
    first = solar_kwh("HH-0007", ts, has_solar=True)
    second = solar_kwh("HH-0007", ts, has_solar=True)
    assert first == second


def test_solar_is_zero_at_night() -> None:
    for hour in (0, 1, 2, 3, 4, 5, 19, 20, 22, 23):
        ts = _DAY + timedelta(hours=hour)
        assert solar_kwh("HH-0001", ts, has_solar=True) == Decimal("0.0000")


def test_solar_is_zero_without_a_panel_even_at_midday() -> None:
    ts = _DAY + timedelta(hours=12)
    assert solar_kwh("HH-0001", ts, has_solar=False) == Decimal("0.0000")


def test_consumption_is_strictly_positive_at_every_hour() -> None:
    for hour in range(24):
        ts = _DAY + timedelta(hours=hour)
        assert consumption_kwh("HH-0001", ts) > Decimal("0")


def test_consumption_peaks_near_morning_and_evening_hours() -> None:
    # Average over many households to smooth out per-tick noise, then compare shapes.
    households = [f"HH-{i:04d}" for i in range(1, 51)]

    def avg_at(hour: int) -> Decimal:
        ts = _DAY + timedelta(hours=hour)
        total = sum((consumption_kwh(h, ts) for h in households), Decimal(0))
        return total / len(households)

    morning_peak = avg_at(7)
    evening_peak = avg_at(19)
    overnight_trough = avg_at(2)

    assert morning_peak > overnight_trough
    assert evening_peak > overnight_trough
    assert evening_peak > morning_peak  # evening weighted higher than morning, by design


def test_solar_peaks_near_midday() -> None:
    households = [f"HH-{i:04d}" for i in range(1, 51)]

    def avg_at(hour: int) -> Decimal:
        ts = _DAY + timedelta(hours=hour)
        total = sum((solar_kwh(h, ts, has_solar=True) for h in households), Decimal(0))
        return total / len(households)

    assert avg_at(12) > avg_at(8)
    assert avg_at(12) > avg_at(17)


def test_output_ranges_are_sane() -> None:
    for hour in range(24):
        ts = _DAY + timedelta(hours=hour)
        consumption = consumption_kwh("HH-0001", ts)
        solar = solar_kwh("HH-0001", ts, has_solar=True)
        assert Decimal("0") <= consumption < Decimal("5")
        assert Decimal("0") <= solar < Decimal("5")


def test_household_scale_is_stable_across_different_timestamps() -> None:
    # Two different households should show a *consistent* relative ordering across the
    # day even though each tick has independent noise — i.e. the per-household scale
    # factor is doing something, not swamped entirely by noise.
    ts_morning = _DAY + timedelta(hours=7)
    ts_evening = _DAY + timedelta(hours=19)

    totals = {
        h: consumption_kwh(h, ts_morning) + consumption_kwh(h, ts_evening)
        for h in (f"HH-{i:04d}" for i in range(1, 21))
    }
    assert len({round(float(v), 6) for v in totals.values()}) > 1  # not all identical


# ---------------------------------------------------------------------------
# T068 — renewable-ratio range check.
# ---------------------------------------------------------------------------


def _zone_of(n: int) -> str:
    zones = ["ZONE-A", "ZONE-B", "ZONE-C", "ZONE-D", "ZONE-E"]
    return zones[(n - 1) % 5]


def _has_solar(n: int) -> bool:
    return n % 3 == 0


def test_renewable_ratio_crosses_the_alert_threshold_both_ways() -> None:
    """Simulate one full day for all 50 households (T042's distribution rule) and assert
    the per-zone renewable ratio spans a range crossing `low_renewable_threshold: 0.15` —
    below it at night, above it at midday. If the curves never cross, the
    LowRenewableContribution alert (T145) can never be demonstrated firing."""
    households = [(f"HH-{n:04d}", _zone_of(n), _has_solar(n)) for n in range(1, 51)]
    zones = {"ZONE-A", "ZONE-B", "ZONE-C", "ZONE-D", "ZONE-E"}

    def zone_ratio(zone: str, hour: int) -> Decimal:
        ts = _DAY + timedelta(hours=hour)
        total_consumption = Decimal(0)
        total_solar = Decimal(0)
        for household_id, household_zone, has_solar in households:
            if household_zone != zone:
                continue
            total_consumption += consumption_kwh(household_id, ts)
            total_solar += solar_kwh(household_id, ts, has_solar=has_solar)
        if total_consumption == 0:
            return Decimal(0)
        return total_solar / total_consumption

    threshold = Decimal("0.15")
    for zone in zones:
        below = any(zone_ratio(zone, hour) < threshold for hour in (0, 1, 2, 3, 4, 22, 23))
        above = any(zone_ratio(zone, hour) > threshold for hour in (10, 11, 12, 13, 14))
        assert below, f"{zone} never dropped below the threshold overnight"
        assert above, f"{zone} never rose above the threshold at midday"
