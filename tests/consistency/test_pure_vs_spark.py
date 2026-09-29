"""The consistency test (decision T063) — the centrepiece of §4.4's architectural
argument: `core/tariff.py` (scalar Python, the executable specification) and
`core/spark_expr.py` (Spark Column expressions, the production implementation) must agree
**exactly** over thousands of generated inputs, including every block boundary and
deliberate half-cent rounding ties. If they ever disagree, this test — not a production
incident — is where that surfaces.

≥ 5,000 rows: a bulk of uniformly random inputs, plus rows constructed to straddle every
`up_to_kwh` boundary (60, 120) from both sides, plus rows constructed to produce an exact
half-cent charge (a product ending in `...5` at the third decimal place — the case that
separates float rounding from `Decimal` `ROUND_HALF_UP`). One fixed seed, so a failure is
reproducible.
"""

from __future__ import annotations

import json
import random
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DecimalType, LongType, StructField, StructType

from voltstream.core.money import KWH, MONEY, PCT
from voltstream.core.netting import net
from voltstream.core.spark_expr import compute_bill_expr
from voltstream.core.tariff import BlockBoundary, TariffRates, compute_bill

_SEED = 20260922
_N_RANDOM = 5000
_BOUNDARIES = [
    BlockBoundary("block_1", Decimal(60)),
    BlockBoundary("block_2", Decimal(120)),
    BlockBoundary("block_3", None),
]

_KWH_TYPE = DecimalType(*KWH)
_MONEY_TYPE = DecimalType(*MONEY)
_PCT_TYPE = DecimalType(*PCT)

_SCHEMA = StructType(
    [
        StructField("row_id", LongType()),
        StructField("consumption_kwh", _KWH_TYPE),
        StructField("solar_kwh", _KWH_TYPE),
        StructField("block_1_rate", _MONEY_TYPE),
        StructField("block_2_rate", _MONEY_TYPE),
        StructField("block_3_rate", _MONEY_TYPE),
        StructField("fixed_charge", _MONEY_TYPE),
        StructField("subsidy_flag", BooleanType()),
        StructField("subsidy_pct", _PCT_TYPE),
        StructField("export_rate", _MONEY_TYPE),
    ]
)


def _random_decimal(rng: random.Random, low: str, high: str, places: int) -> Decimal:
    quant = Decimal(1).scaleb(-places)
    lo, hi = Decimal(low), Decimal(high)
    span = hi - lo
    fraction = Decimal(rng.random())
    return (lo + fraction * span).quantize(quant)


def _random_row(rng: random.Random) -> tuple[Decimal, Decimal, TariffRates]:
    consumption = _random_decimal(rng, "0", "300", 4)
    solar = _random_decimal(rng, "0", "300", 4)
    rates = TariffRates(
        block_1_rate=_random_decimal(rng, "0", "50", 2),
        block_2_rate=_random_decimal(rng, "0", "50", 2),
        block_3_rate=_random_decimal(rng, "0", "50", 2),
        fixed_charge=_random_decimal(rng, "0", "1000", 2),
        subsidy_flag=rng.random() < 0.5,
        subsidy_pct=_random_decimal(rng, "0", "100", 2),
        export_rate=_random_decimal(rng, "0", "50", 2),
    )
    return consumption, solar, rates


def _boundary_straddling_rows() -> list[tuple[Decimal, Decimal, TariffRates]]:
    rates = TariffRates(
        Decimal("8.00"),
        Decimal("16.50"),
        Decimal("24.50"),
        Decimal("240.00"),
        False,
        Decimal("0"),
        Decimal("18.00"),
    )
    rows = []
    for boundary in (Decimal(60), Decimal(120)):
        for offset_hundredths in range(-10, 11):
            consumption = boundary + Decimal(offset_hundredths).scaleb(-2)
            if consumption < 0:
                continue
            rows.append((consumption, Decimal(0), rates))
    return rows


def _half_cent_tie_rows() -> list[tuple[Decimal, Decimal, TariffRates]]:
    # kwh * rate landing on exactly x.xx5 at the third decimal — the case that separates
    # float rounding from Decimal ROUND_HALF_UP (T002's evidence: 60.01 * ... -> .165).
    rows = []
    rates = TariffRates(
        Decimal("8.00"),
        Decimal("16.50"),
        Decimal("24.50"),
        Decimal("240.00"),
        True,
        Decimal("25"),
        Decimal("18.00"),
    )
    for kwh_hundredths in range(1, 51):  # 0.01 .. 0.50 kWh into block_2
        consumption = Decimal(60) + Decimal(kwh_hundredths).scaleb(-2)
        rows.append((consumption, Decimal(0), rates))
    return rows


def _build_dataset() -> list[tuple[Decimal, Decimal, TariffRates]]:
    rng = random.Random(_SEED)
    rows = [_random_row(rng) for _ in range(_N_RANDOM)]
    rows += _boundary_straddling_rows()
    rows += _half_cent_tie_rows()
    return rows


@pytest.mark.slow
def test_pure_and_spark_agree_exactly(spark: SparkSession) -> None:
    dataset = _build_dataset()
    assert len(dataset) >= 5000

    pure_bills = [
        compute_bill(net(consumption, solar), rates, _BOUNDARIES)
        for consumption, solar, rates in dataset
    ]

    spark_rows = [
        (
            i,
            consumption,
            solar,
            rates.block_1_rate,
            rates.block_2_rate,
            rates.block_3_rate,
            rates.fixed_charge,
            rates.subsidy_flag,
            rates.subsidy_pct,
            rates.export_rate,
        )
        for i, (consumption, solar, rates) in enumerate(dataset)
    ]
    df = spark.createDataFrame(spark_rows, schema=_SCHEMA)

    bill_cols = compute_bill_expr(
        F.col("consumption_kwh"),
        F.col("solar_kwh"),
        rate_cols=(F.col("block_1_rate"), F.col("block_2_rate"), F.col("block_3_rate")),
        fixed_charge_col=F.col("fixed_charge"),
        subsidy_flag_col=F.col("subsidy_flag"),
        subsidy_pct_col=F.col("subsidy_pct"),
        export_rate_col=F.col("export_rate"),
        boundaries=_BOUNDARIES,
    )
    out = df.select(
        F.col("row_id"),
        bill_cols.self_consumed_kwh.alias("self_consumed_kwh"),
        bill_cols.billable_import_kwh.alias("billable_import_kwh"),
        bill_cols.export_kwh.alias("export_kwh"),
        bill_cols.energy_charge.alias("energy_charge"),
        bill_cols.fixed_charge.alias("fixed_charge"),
        bill_cols.subsidy_discount.alias("subsidy_discount"),
        bill_cols.export_credit.alias("export_credit"),
        bill_cols.final_bill.alias("final_bill"),
        bill_cols.tier_breakdown.alias("tier_breakdown"),
    )

    # Assert schema: every kWh/money column must be DecimalType — a stray float literal
    # in spark_expr.py would silently promote a column to DoubleType even when the
    # *values* happen to agree, which is exactly what this schema check catches.
    schema_by_name = {f.name: f.dataType for f in out.schema.fields}
    for kwh_col in ("self_consumed_kwh", "billable_import_kwh", "export_kwh"):
        assert isinstance(schema_by_name[kwh_col], DecimalType), kwh_col
    money_cols = (
        "energy_charge",
        "fixed_charge",
        "subsidy_discount",
        "export_credit",
        "final_bill",
    )
    for money_col in money_cols:
        assert isinstance(schema_by_name[money_col], DecimalType), money_col

    spark_results = {row["row_id"]: row for row in out.collect()}
    assert len(spark_results) == len(dataset)

    for i, pure in enumerate(pure_bills):
        row = spark_results[i]

        assert row["self_consumed_kwh"] == pure.self_consumed_kwh, i
        assert row["billable_import_kwh"] == pure.billable_import_kwh, i
        assert row["export_kwh"] == pure.export_kwh, i
        assert row["energy_charge"] == pure.energy_charge, i
        assert row["fixed_charge"] == pure.fixed_charge, i
        assert row["subsidy_discount"] == pure.subsidy_discount, i
        assert row["export_credit"] == pure.export_credit, i
        assert row["final_bill"] == pure.final_bill, i

        # tier_breakdown: parse both sides' numbers as Decimal and compare per field —
        # Spark's to_json writes "480.00", a naive Python json.dumps would write "480.0";
        # textual comparison would spuriously fail, so this compares numerically instead.
        spark_breakdown = json.loads(row["tier_breakdown"], parse_float=Decimal)
        assert len(spark_breakdown) == len(pure.tier_breakdown)
        for spark_line, pure_line in zip(spark_breakdown, pure.tier_breakdown, strict=True):
            assert spark_line["name"] == pure_line.name, i
            assert spark_line["up_to_kwh"] == pure_line.up_to_kwh, i
            assert spark_line["rate"] == pure_line.rate, i
            assert spark_line["kwh"] == pure_line.kwh, i
            assert spark_line["charge"] == pure_line.charge, i
