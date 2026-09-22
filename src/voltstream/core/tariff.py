"""Block-tariff bill assembly (decisions T057/T058, D2, D5, D6) — the executable
specification: nine lines of Decimal arithmetic that `core/spark_expr.py` (T061/T062)
implements identically as Spark Column expressions, pinned together by
`tests/consistency/test_pure_vs_spark.py` (T063). `batch/reconciliation.py` is this
module's one production caller (D4) — everywhere else uses `spark_expr.py`.

Per D2, `build_blocks` is the *only* place the config-supplied block **boundaries** are
combined with the tariff-file-supplied block **rates** — `energy_charge` and
`compute_bill` never read either directly, only the `Block`s this function returns.
"""

from __future__ import annotations

from decimal import Decimal
from typing import NamedTuple

from voltstream.core.money import round_money
from voltstream.core.netting import NettingResult


class BlockBoundary(NamedTuple):
    """One block's kWh boundary, decoupled from `config.TariffConfig` on purpose — this
    module must not import `voltstream.config` (no config lookups inside `core/`, D2/D5's
    architectural rule). Callers translate `get_config().tariff.blocks` into these."""

    name: str
    up_to_kwh: Decimal | None  # None = unbounded top block


class Block(NamedTuple):
    """A boundary paired with the day's rate for it — `build_blocks`'s output."""

    name: str
    lower: Decimal
    upper: Decimal | None
    rate: Decimal


class BlockCharge(NamedTuple):
    """One line item of the tier breakdown — becomes one entry of
    `household_bill_daily.tier_breakdown` / `household_running_rt.tier_breakdown`."""

    name: str
    up_to_kwh: Decimal | None
    rate: Decimal
    kwh: Decimal
    charge: Decimal


class TariffRates(NamedTuple):
    """The subset of `contracts.reference.TariffRecord` that bill arithmetic needs —
    accepted as plain values (not the Pydantic model) so `core/` stays free of a
    `contracts` import too; callers pass `TariffRecord` fields through directly."""

    block_1_rate: Decimal
    block_2_rate: Decimal
    block_3_rate: Decimal
    fixed_charge: Decimal
    subsidy_flag: bool
    subsidy_pct: Decimal
    export_rate: Decimal


class BillBreakdown(NamedTuple):
    self_consumed_kwh: Decimal
    billable_import_kwh: Decimal
    export_kwh: Decimal
    energy_charge: Decimal
    fixed_charge: Decimal
    subsidy_discount: Decimal
    export_credit: Decimal
    final_bill: Decimal  # may be negative (D5: never clamped)
    tier_breakdown: tuple[BlockCharge, ...]


def build_blocks(boundaries: list[BlockBoundary], rates: TariffRates) -> list[Block]:
    """Zip the config `boundaries` (structure) with the day's `rates` (money, D2). The
    boundaries are cumulative `up_to_kwh` values; each block's lower edge is the previous
    block's upper edge."""
    block_rates = (rates.block_1_rate, rates.block_2_rate, rates.block_3_rate)
    blocks: list[Block] = []
    lower = Decimal(0)
    for boundary, rate in zip(boundaries, block_rates, strict=True):
        blocks.append(Block(name=boundary.name, lower=lower, upper=boundary.up_to_kwh, rate=rate))
        if boundary.up_to_kwh is not None:
            lower = boundary.up_to_kwh
    return blocks


def energy_charge(
    billable_import_kwh: Decimal, blocks: list[Block]
) -> tuple[Decimal, tuple[BlockCharge, ...]]:
    """Marginal/slab energy charge: the first block's kWh at its rate, the next block's
    kWh at its rate, and so on. Each line is rounded to money precision (D5); the total is
    the exact sum of the rounded lines, not a rounding of the exact sum."""
    breakdown: list[BlockCharge] = []
    for block in blocks:
        width = None if block.upper is None else block.upper - block.lower
        kwh_in_block = max(Decimal(0), billable_import_kwh - block.lower)
        if width is not None:
            kwh_in_block = min(kwh_in_block, width)
        charge = round_money(kwh_in_block * block.rate)
        breakdown.append(
            BlockCharge(
                name=block.name, up_to_kwh=block.upper, rate=block.rate,
                kwh=kwh_in_block, charge=charge,
            )
        )
    total = sum((line.charge for line in breakdown), Decimal("0.00"))
    return total, tuple(breakdown)


def compute_bill(
    netting: NettingResult, rates: TariffRates, boundaries: list[BlockBoundary]
) -> BillBreakdown:
    """The D5 formula, in order:

        blocks           = build_blocks(boundaries, rates)
        energy_charge    = sum of rounded per-block lines               (marginal/slab)
        fixed_charge     = rates.fixed_charge
        subsidy_discount = round(energy_charge * subsidy_pct / 100)  if subsidy_flag else 0
        export_credit    = round(export_kwh * export_rate)
        final_bill       = energy_charge + fixed_charge - subsidy_discount - export_credit

    `final_bill` is an exact `Decimal` sum/difference of already-rounded components and is
    never clamped at zero — a net exporter's bill may be negative (D5).

    >>> from voltstream.core.netting import net
    >>> boundaries = [
    ...     BlockBoundary("block_1", Decimal(60)),
    ...     BlockBoundary("block_2", Decimal(120)),
    ...     BlockBoundary("block_3", None),
    ... ]
    >>> rates = TariffRates(
    ...     block_1_rate=Decimal("8.00"), block_2_rate=Decimal("16.50"),
    ...     block_3_rate=Decimal("24.50"), fixed_charge=Decimal("240.00"),
    ...     subsidy_flag=False, subsidy_pct=Decimal("0"), export_rate=Decimal("18.00"),
    ... )
    >>> bill = compute_bill(net(Decimal("60.0100"), Decimal("0")), rates, boundaries)
    >>> bill.energy_charge
    Decimal('480.17')
    >>> bill.final_bill
    Decimal('720.17')
    """
    blocks = build_blocks(boundaries, rates)
    energy, breakdown = energy_charge(netting.billable_import_kwh, blocks)

    subsidy_discount = (
        round_money(energy * rates.subsidy_pct / Decimal(100))
        if rates.subsidy_flag
        else Decimal("0.00")
    )
    export_credit = round_money(netting.export_kwh * rates.export_rate)
    final_bill = energy + rates.fixed_charge - subsidy_discount - export_credit

    return BillBreakdown(
        self_consumed_kwh=netting.self_consumed_kwh,
        billable_import_kwh=netting.billable_import_kwh,
        export_kwh=netting.export_kwh,
        energy_charge=energy,
        fixed_charge=rates.fixed_charge,
        subsidy_discount=subsidy_discount,
        export_credit=export_credit,
        final_bill=final_bill,
        tier_breakdown=breakdown,
    )
