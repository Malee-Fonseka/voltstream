"""Netting and tariff as Spark Column expressions (decisions T061/T062, D5). The
production implementation — imported unchanged by the speed layer and the batch layer, so
those two cannot drift from each other by construction — and the counterpart
`core/tariff.py` (`core/netting.py`) is pinned to via
`tests/consistency/test_pure_vs_spark.py` (T063).

**No Python row functions anywhere in this module** — every expression below is built
from `pyspark.sql.functions` so Catalyst can optimise it and no row is ever serialised out
to a Python worker (§4.4).

**Every literal is a `Decimal` cast to the matching `DecimalType`, never a bare float**
(D5) — `DecimalType`s are built from `core/money.py`'s `(precision, scale)` ints, the same
ones `core/tariff.py` and the Postgres schema use, so there is one source for precision
across Python, Spark and the database. Rounding is `F.round` (HALF_UP), never `F.bround`
(HALF_EVEN, D5's pinned rule) — SQL's `round()` on this engine reproduces the exact
Decimal-based `ROUND_HALF_UP` `core/money.round_money` uses.
"""

from __future__ import annotations

from decimal import Decimal
from typing import NamedTuple

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from voltstream.core.money import KWH, MONEY, PCT
from voltstream.core.tariff import BlockBoundary

_KWH_TYPE = DecimalType(*KWH)
_MONEY_TYPE = DecimalType(*MONEY)
_PCT_TYPE = DecimalType(*PCT)


def _decimal_lit(value: Decimal, dtype: DecimalType) -> Column:
    """`F.lit` needs an active SparkContext, so every literal is built lazily inside a
    function — never as a module-level constant, which would break at import time in a
    process that has not yet created a `SparkSession` (e.g. anything merely importing this
    module to build a query plan later)."""
    return F.lit(value).cast(dtype)


class NettingColumns(NamedTuple):
    self_consumed_kwh: Column
    billable_import_kwh: Column
    export_kwh: Column


def netting_expr(consumption_col: Column, solar_col: Column) -> NettingColumns:
    """§3.3c as Column expressions — the exact counterpart of `core/netting.net`."""
    self_consumed = F.least(consumption_col, solar_col).cast(_KWH_TYPE)
    return NettingColumns(
        self_consumed_kwh=self_consumed,
        billable_import_kwh=(consumption_col - self_consumed).cast(_KWH_TYPE),
        export_kwh=(solar_col - self_consumed).cast(_KWH_TYPE),
    )


class SparkBlock(NamedTuple):
    """`core/tariff.Block`'s Column counterpart: boundaries are static (config,
    expression-build time, D2); `rate_col` is a Column reference into the joined tariff
    DataFrame — a literal here would silently pin every household to one rate."""

    name: str
    lower: Decimal
    upper: Decimal | None
    rate_col: Column


def spark_build_blocks(
    boundaries: list[BlockBoundary], rate_cols: tuple[Column, Column, Column]
) -> list[SparkBlock]:
    """The Column-expression counterpart of `core/tariff.build_blocks`, built from the
    *same* `boundaries` argument, so the two cannot drift structurally."""
    blocks: list[SparkBlock] = []
    lower = Decimal(0)
    for boundary, rate_col in zip(boundaries, rate_cols, strict=True):
        blocks.append(SparkBlock(boundary.name, lower, boundary.up_to_kwh, rate_col))
        if boundary.up_to_kwh is not None:
            lower = boundary.up_to_kwh
    return blocks


def energy_charge_expr(
    billable_import_col: Column, blocks: list[SparkBlock]
) -> tuple[Column, list[Column]]:
    """The Column-expression counterpart of `core/tariff.energy_charge`: marginal/slab,
    each block's line item rounded (HALF_UP) before summing — never sum-then-round."""
    block_structs: list[Column] = []
    charges: list[Column] = []
    for block in blocks:
        lower_lit = F.lit(block.lower).cast(_KWH_TYPE)
        kwh_in_block = F.greatest(
            billable_import_col - lower_lit, _decimal_lit(Decimal(0), _KWH_TYPE)
        )
        if block.upper is not None:
            width_lit = F.lit(block.upper - block.lower).cast(_KWH_TYPE)
            kwh_in_block = F.least(kwh_in_block, width_lit)
        kwh_in_block = kwh_in_block.cast(_KWH_TYPE)

        charge = F.round(kwh_in_block * block.rate_col, MONEY[1]).cast(_MONEY_TYPE)
        charges.append(charge)

        up_to_lit = (
            _decimal_lit(block.upper, _KWH_TYPE)
            if block.upper is not None
            else F.lit(None).cast(_KWH_TYPE)
        )
        block_structs.append(
            F.struct(
                F.lit(block.name).alias("name"),
                up_to_lit.alias("up_to_kwh"),
                block.rate_col.cast(_MONEY_TYPE).alias("rate"),
                kwh_in_block.alias("kwh"),
                charge.alias("charge"),
            )
        )

    total = charges[0]
    for charge in charges[1:]:
        total = total + charge
    total = total.cast(_MONEY_TYPE)

    return total, block_structs


class BillColumns(NamedTuple):
    self_consumed_kwh: Column
    billable_import_kwh: Column
    export_kwh: Column
    energy_charge: Column
    fixed_charge: Column
    subsidy_discount: Column
    export_credit: Column
    final_bill: Column
    tier_breakdown: Column


def compute_bill_expr(
    consumption_col: Column,
    solar_col: Column,
    *,
    rate_cols: tuple[Column, Column, Column],
    fixed_charge_col: Column,
    subsidy_flag_col: Column,
    subsidy_pct_col: Column,
    export_rate_col: Column,
    boundaries: list[BlockBoundary],
) -> BillColumns:
    """The Column-expression counterpart of `core/tariff.compute_bill` — the same D5
    formula, in the same order, over the same `boundaries`."""
    netting = netting_expr(consumption_col, solar_col)
    blocks = spark_build_blocks(boundaries, rate_cols)
    energy, block_structs = energy_charge_expr(netting.billable_import_kwh, blocks)

    fixed_charge = fixed_charge_col.cast(_MONEY_TYPE)
    hundred_pct = _decimal_lit(Decimal(100), _PCT_TYPE)
    subsidy_discount = F.when(
        subsidy_flag_col,
        F.round(energy * subsidy_pct_col.cast(_PCT_TYPE) / hundred_pct, MONEY[1]).cast(_MONEY_TYPE),
    ).otherwise(_decimal_lit(Decimal("0.00"), _MONEY_TYPE))
    export_credit = F.round(netting.export_kwh * export_rate_col, MONEY[1]).cast(_MONEY_TYPE)
    final_bill = (energy + fixed_charge - subsidy_discount - export_credit).cast(_MONEY_TYPE)
    # ignoreNullFields=false: the top block's up_to_kwh is null (unbounded) by design, and
    # to_json silently drops null fields by default, giving the last line item a different
    # JSON shape from the other two. Keep every line item's shape identical.
    tier_breakdown = F.to_json(F.array(*block_structs), {"ignoreNullFields": "false"})

    return BillColumns(
        self_consumed_kwh=netting.self_consumed_kwh,
        billable_import_kwh=netting.billable_import_kwh,
        export_kwh=netting.export_kwh,
        energy_charge=energy,
        fixed_charge=fixed_charge,
        subsidy_discount=subsidy_discount,
        export_credit=export_credit,
        final_bill=final_bill,
        tier_breakdown=tier_breakdown,
    )
