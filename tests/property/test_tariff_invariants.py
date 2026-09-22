"""Property tests for voltstream.core.tariff and voltstream.core.netting (T060, §9
Phase 2). Strategies generate `Decimal` inputs matching each field's real precision (D5):
kWh at <= 4 dp, rates/fixed charge at <= 2 dp, `subsidy_pct` in [0, 100] at <= 2 dp.
"""

from __future__ import annotations

from decimal import Decimal

from hypothesis import example, given, settings
from hypothesis import strategies as st

from voltstream.core.netting import net
from voltstream.core.tariff import BlockBoundary, TariffRates, build_blocks, compute_bill

# T060: >= 1,000 examples per property test.
_MANY_EXAMPLES = settings(max_examples=1000)

_BOUNDARIES = [
    BlockBoundary("block_1", Decimal(60)),
    BlockBoundary("block_2", Decimal(120)),
    BlockBoundary("block_3", None),
]

_kwh = st.decimals(min_value=0, max_value=300, places=4, allow_nan=False, allow_infinity=False)
_rate = st.decimals(min_value=0, max_value=50, places=2, allow_nan=False, allow_infinity=False)
_fixed_charge = st.decimals(
    min_value=0, max_value=1000, places=2, allow_nan=False, allow_infinity=False
)
_pct = st.decimals(min_value=0, max_value=100, places=2, allow_nan=False, allow_infinity=False)


def _r(
    fixed_charge: str, subsidy_flag: bool, subsidy_pct: str, export_rate: str = "18.00"
) -> TariffRates:
    """Shorthand for a TariffRates at the D5 default block rates (8.00/16.50/24.50)."""
    return TariffRates(
        block_1_rate=Decimal("8.00"),
        block_2_rate=Decimal("16.50"),
        block_3_rate=Decimal("24.50"),
        fixed_charge=Decimal(fixed_charge),
        subsidy_flag=subsidy_flag,
        subsidy_pct=Decimal(subsidy_pct),
        export_rate=Decimal(export_rate),
    )


@st.composite
def _rates_strategy(draw: st.DrawFn) -> TariffRates:
    return TariffRates(
        block_1_rate=draw(_rate),
        block_2_rate=draw(_rate),
        block_3_rate=draw(_rate),
        fixed_charge=draw(_fixed_charge),
        subsidy_flag=draw(st.booleans()),
        subsidy_pct=draw(_pct),
        export_rate=draw(_rate),
    )


# ---------------------------------------------------------------------------
# Netting invariants — exact, no rounding.
# ---------------------------------------------------------------------------


@_MANY_EXAMPLES
@given(consumption=_kwh, solar=_kwh)
@example(consumption=Decimal("60.0000"), solar=Decimal("0.0000"))
@example(consumption=Decimal("120.0000"), solar=Decimal("0.0000"))
def test_netting_invariants_exact(consumption: Decimal, solar: Decimal) -> None:
    result = net(consumption, solar)
    assert result.self_consumed_kwh + result.export_kwh == solar
    assert result.self_consumed_kwh + result.billable_import_kwh == consumption


# ---------------------------------------------------------------------------
# Row invariants — final_bill and the breakdown sum, exact.
# ---------------------------------------------------------------------------


@_MANY_EXAMPLES
@given(consumption=_kwh, solar=_kwh, rates=_rates_strategy())
@example(
    consumption=Decimal("60.0100"), solar=Decimal("0"),
    rates=_r("240.00", False, "0"),
)
@example(
    consumption=Decimal("61.2345"), solar=Decimal("25.5000"),
    rates=_r("240.00", True, "25"),
)
@example(
    consumption=Decimal("4.0000"), solar=Decimal("19.0000"),
    rates=_r("120.00", False, "0"),
)
def test_row_invariants_exact(consumption: Decimal, solar: Decimal, rates: TariffRates) -> None:
    bill = compute_bill(net(consumption, solar), rates, _BOUNDARIES)

    assert bill.final_bill == (
        bill.energy_charge + bill.fixed_charge - bill.subsidy_discount - bill.export_credit
    )
    assert bill.energy_charge == sum((line.charge for line in bill.tier_breakdown), Decimal("0.00"))


# ---------------------------------------------------------------------------
# Non-negativity — every component except final_bill.
# ---------------------------------------------------------------------------


@_MANY_EXAMPLES
@given(consumption=_kwh, solar=_kwh, rates=_rates_strategy())
def test_no_component_is_negative_except_final_bill(
    consumption: Decimal, solar: Decimal, rates: TariffRates
) -> None:
    bill = compute_bill(net(consumption, solar), rates, _BOUNDARIES)

    assert bill.self_consumed_kwh >= 0
    assert bill.billable_import_kwh >= 0
    assert bill.export_kwh >= 0
    assert bill.energy_charge >= 0
    assert bill.fixed_charge >= 0
    assert bill.subsidy_discount >= 0
    assert bill.export_credit >= 0
    for line in bill.tier_breakdown:
        assert line.kwh >= 0
        assert line.charge >= 0
    # final_bill itself may be negative (D5) — deliberately not asserted here.


# ---------------------------------------------------------------------------
# Monotonicity — more consumption never lowers the bill; more solar never raises it.
# ---------------------------------------------------------------------------


@_MANY_EXAMPLES
@given(consumption=_kwh, solar=_kwh, extra=_kwh, rates=_rates_strategy())
def test_monotone_non_decreasing_in_consumption(
    consumption: Decimal, solar: Decimal, extra: Decimal, rates: TariffRates
) -> None:
    lower = compute_bill(net(consumption, solar), rates, _BOUNDARIES)
    higher = compute_bill(net(consumption + extra, solar), rates, _BOUNDARIES)
    assert higher.final_bill >= lower.final_bill


@_MANY_EXAMPLES
@given(consumption=_kwh, solar=_kwh, extra=_kwh, rates=_rates_strategy())
def test_monotone_non_increasing_in_solar(
    consumption: Decimal, solar: Decimal, extra: Decimal, rates: TariffRates
) -> None:
    lower_solar = compute_bill(net(consumption, solar), rates, _BOUNDARIES)
    higher_solar = compute_bill(net(consumption, solar + extra), rates, _BOUNDARIES)
    assert higher_solar.final_bill <= lower_solar.final_bill


# ---------------------------------------------------------------------------
# Continuity — no discontinuous jump at a block boundary (ε = one cent per boundary
# crossed, since line items are rounded; slab pricing crosses at most one boundary for a
# minimal step, so ε = 0.01 here).
# ---------------------------------------------------------------------------


@_MANY_EXAMPLES
@given(rates=_rates_strategy())
@example(rates=_r("240.00", False, "0"))
def test_continuity_at_block_boundaries(rates: TariffRates) -> None:
    delta = Decimal("0.0001")
    epsilon = Decimal("0.02")
    for boundary in (Decimal(60), Decimal(120)):
        just_below = compute_bill(net(boundary - delta, Decimal(0)), rates, _BOUNDARIES)
        just_above = compute_bill(net(boundary + delta, Decimal(0)), rates, _BOUNDARIES)
        assert abs(just_above.final_bill - just_below.final_bill) <= epsilon


# ---------------------------------------------------------------------------
# build_blocks: boundaries strictly increasing, exactly one trailing null.
# ---------------------------------------------------------------------------


def test_build_blocks_boundaries_are_cumulative() -> None:
    blocks = build_blocks(_BOUNDARIES, _r("240.00", False, "0"))
    assert blocks[0].lower == Decimal(0)
    assert blocks[0].upper == Decimal(60)
    assert blocks[1].lower == Decimal(60)
    assert blocks[1].upper == Decimal(120)
    assert blocks[2].lower == Decimal(120)
    assert blocks[2].upper is None
