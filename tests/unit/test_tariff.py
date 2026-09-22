"""Unit tests for voltstream.core.tariff (T059)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from voltstream.core.netting import net
from voltstream.core.tariff import (
    BlockBoundary,
    TariffRates,
    build_blocks,
    compute_bill,
    energy_charge,
)

_BOUNDARIES = [
    BlockBoundary("block_1", Decimal(60)),
    BlockBoundary("block_2", Decimal(120)),
    BlockBoundary("block_3", None),
]
_DEFAULT_RATES = Decimal("8.00"), Decimal("16.50"), Decimal("24.50")


def _rates(
    *,
    fixed_charge: Decimal = Decimal("240.00"),
    subsidy_flag: bool = False,
    subsidy_pct: Decimal = Decimal("0"),
    export_rate: Decimal = Decimal("18.00"),
    block_rates: tuple[Decimal, Decimal, Decimal] = _DEFAULT_RATES,
) -> TariffRates:
    return TariffRates(
        block_1_rate=block_rates[0],
        block_2_rate=block_rates[1],
        block_3_rate=block_rates[2],
        fixed_charge=fixed_charge,
        subsidy_flag=subsidy_flag,
        subsidy_pct=subsidy_pct,
        export_rate=export_rate,
    )


def _bill(consumption: str, solar: str, rates: TariffRates) -> object:
    return compute_bill(net(Decimal(consumption), Decimal(solar)), rates, _BOUNDARIES)


# ---------------------------------------------------------------------------
# §4.4's worked failure example: block boundary from both sides, at exactly 60 and 120.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "billable_import,expected_total",
    [
        (Decimal("0"), Decimal("0.00")),
        (Decimal("59.99"), Decimal("479.92")),  # 59.99 * 8.00
        (Decimal("60"), Decimal("480.00")),  # exactly at the boundary -> still block 1
        (Decimal("60.01"), Decimal("480.17")),  # 60*8.00 + 0.01*16.50, rounded
        (Decimal("120"), Decimal("1470.00")),  # 60*8.00 + 60*16.50
        (Decimal("200"), Decimal("3430.00")),  # 60*8.00 + 60*16.50 + 80*24.50
    ],
)
def test_block_boundary_charges_at_default_rates(
    billable_import: Decimal, expected_total: Decimal
) -> None:
    blocks = build_blocks(_BOUNDARIES, _rates())
    total, _ = energy_charge(billable_import, blocks)
    assert total == expected_total


def test_flipping_boundary_comparison_breaks_a_case() -> None:
    """A `>` vs `>=` slip at a block boundary must be observable — the 60.00 exactly-at-
    boundary case is the one that catches it (kwh_in_block clamped to the block width)."""
    blocks = build_blocks(_BOUNDARIES, _rates())
    total, breakdown = energy_charge(Decimal("60"), blocks)
    assert breakdown[0].kwh == Decimal("60")
    assert breakdown[1].kwh == Decimal("0")
    assert total == Decimal("480.00")


# ---------------------------------------------------------------------------
# Subsidised vs unsubsidised; each fixed-charge tier; zero consumption; export-only.
# ---------------------------------------------------------------------------


def test_subsidised_reduces_final_bill_versus_unsubsidised() -> None:
    unsubsidised = _bill("61.2345", "0", _rates(subsidy_flag=False))
    subsidised = _bill("61.2345", "0", _rates(subsidy_flag=True, subsidy_pct=Decimal("25")))
    assert subsidised.final_bill < unsubsidised.final_bill  # type: ignore[attr-defined]
    assert subsidised.subsidy_discount == round(unsubsidised.energy_charge * Decimal("0.25"), 2)  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "fixed_charge", [Decimal("120.00"), Decimal("240.00"), Decimal("480.00")]
)
def test_each_fixed_charge_tier_passes_through_unchanged(fixed_charge: Decimal) -> None:
    bill = _bill("10", "0", _rates(fixed_charge=fixed_charge))
    assert bill.fixed_charge == fixed_charge  # type: ignore[attr-defined]


def test_zero_consumption_zero_solar() -> None:
    bill = _bill("0", "0", _rates())
    assert bill.energy_charge == Decimal("0.00")  # type: ignore[attr-defined]
    assert bill.final_bill == bill.fixed_charge  # type: ignore[attr-defined]


def test_export_only_household() -> None:
    bill = _bill("0", "10", _rates())
    assert bill.export_kwh == Decimal("10")  # type: ignore[attr-defined]
    assert bill.export_credit == Decimal("180.00")  # type: ignore[attr-defined]
    assert bill.final_bill == bill.fixed_charge - bill.export_credit  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# The three D5 worked examples, as exact-equality assertions.
# ---------------------------------------------------------------------------


def test_d5_example_boundary_tie() -> None:
    bill = _bill("60.0100", "0", _rates())
    assert bill.energy_charge == Decimal("480.17")  # type: ignore[attr-defined]
    assert bill.final_bill == Decimal("720.17")  # type: ignore[attr-defined]


def test_d5_example_typical_subsidised_solar() -> None:
    rates = _rates(subsidy_flag=True, subsidy_pct=Decimal("25"))
    bill = _bill("61.2345", "25.5000", rates)
    assert bill.subsidy_discount == Decimal("71.47")  # type: ignore[attr-defined]
    assert bill.export_credit == Decimal("0.00")  # type: ignore[attr-defined]
    assert bill.final_bill == Decimal("454.41")  # type: ignore[attr-defined]


def test_d5_example_net_exporter() -> None:
    bill = _bill("4.0000", "19.0000", _rates(fixed_charge=Decimal("120.00")))
    assert bill.final_bill == Decimal("-150.00")  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Line-item rounding: two half-cent lines round to 0.34, not 0.33.
# ---------------------------------------------------------------------------


def test_line_item_rounding_not_sum_then_round() -> None:
    # Three tiny blocks (still the real three-block shape); the first two each produce a
    # charge that rounds up from a half-cent: 0.165 -> 0.17. Summing the ROUNDED lines
    # gives 0.34; rounding the exact sum (0.33) would not.
    boundaries = [
        BlockBoundary("block_1", Decimal("0.01")),
        BlockBoundary("block_2", Decimal("0.02")),
        BlockBoundary("block_3", None),
    ]
    rates = _rates(block_rates=(Decimal("16.50"), Decimal("16.50"), Decimal("0")))
    blocks = build_blocks(boundaries, rates)
    total, breakdown = energy_charge(Decimal("0.02"), blocks)
    assert breakdown[0].charge == Decimal("0.17")
    assert breakdown[1].charge == Decimal("0.17")
    assert breakdown[2].charge == Decimal("0.00")
    assert total == Decimal("0.34")
