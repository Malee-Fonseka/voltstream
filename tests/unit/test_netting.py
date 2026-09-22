"""Unit tests for voltstream.core.netting (T056)."""

from __future__ import annotations

from decimal import Decimal

from voltstream.core.netting import NettingResult, net


def _assert_invariants(consumption: Decimal, solar: Decimal, result: NettingResult) -> None:
    self_consumed, billable_import, export = result
    # §9 Phase 2 invariants, exact Decimal equality — no rounding in netting.
    assert self_consumed + export == solar
    assert self_consumed + billable_import == consumption


def test_solar_greater_than_consumption() -> None:
    consumption, solar = Decimal("4.0"), Decimal("10.5")
    result = net(consumption, solar)
    assert result.self_consumed_kwh == Decimal("4.0")
    assert result.billable_import_kwh == Decimal("0.0")
    assert result.export_kwh == Decimal("6.5")
    _assert_invariants(consumption, solar, result)


def test_solar_less_than_consumption() -> None:
    consumption, solar = Decimal("10.5"), Decimal("4.0")
    result = net(consumption, solar)
    assert result.self_consumed_kwh == Decimal("4.0")
    assert result.billable_import_kwh == Decimal("6.5")
    assert result.export_kwh == Decimal("0.0")
    _assert_invariants(consumption, solar, result)


def test_solar_equals_consumption() -> None:
    consumption, solar = Decimal("7.25"), Decimal("7.25")
    result = net(consumption, solar)
    assert result.self_consumed_kwh == Decimal("7.25")
    assert result.billable_import_kwh == Decimal("0.00")
    assert result.export_kwh == Decimal("0.00")
    _assert_invariants(consumption, solar, result)


def test_both_zero() -> None:
    consumption, solar = Decimal("0"), Decimal("0")
    result = net(consumption, solar)
    assert result == (Decimal("0"), Decimal("0"), Decimal("0"))
    _assert_invariants(consumption, solar, result)


def test_pure_export_zero_consumption() -> None:
    consumption, solar = Decimal("0"), Decimal("5.5")
    result = net(consumption, solar)
    assert result.self_consumed_kwh == Decimal("0")
    assert result.billable_import_kwh == Decimal("0")
    assert result.export_kwh == Decimal("5.5")
    _assert_invariants(consumption, solar, result)


def test_no_rounding_kwh_stays_exact_at_four_decimal_places() -> None:
    consumption, solar = Decimal("10.1234"), Decimal("3.5678")
    result = net(consumption, solar)
    assert result.self_consumed_kwh == Decimal("3.5678")
    assert result.billable_import_kwh == Decimal("6.5556")
    _assert_invariants(consumption, solar, result)
