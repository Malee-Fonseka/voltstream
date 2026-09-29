"""A day's tariff file as a Spark DataFrame, read one way by both layers (R22).

The speed layer and the billing job used to parse the same file differently: one cast
strings column by column, the other declared a CSV schema, and both counted only the
literal `true` as subsidised, so a restated file writing `1` or `True` silently lost the
subsidy. Neither checked the file against its contract. Now both read every column as
text, check the header names, and put every row through `TariffRecord`
(`contracts.reference.applicable_tariffs`), the same validation reconciliation uses. A
negative rate, a subsidy above 100 % or a malformed number fails the read, naming the line.

Collecting the file to the driver is deliberate: it holds one row per household, and
validation needs the rows in Python.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from voltstream.contracts.reference import TARIFF_COLUMNS, applicable_tariffs
from voltstream.storage.objectstore import landing_tariff_path

_MONEY = DecimalType(12, 2)

# Text first: the contract, not Spark's CSV parser, decides what a valid value is. A
# declared DecimalType would turn "8.5O" into a null that surfaces as a missing tariff.
_TEXT_SCHEMA = StructType([StructField(name, StringType()) for name in TARIFF_COLUMNS])

TARIFF_SCHEMA = StructType(
    [
        StructField("household_id", StringType(), nullable=False),
        StructField("effective_date", DateType(), nullable=False),
        StructField("billing_tier", StringType(), nullable=False),
        StructField("subsidy_flag", BooleanType(), nullable=False),
        StructField("subsidy_pct", DecimalType(5, 2), nullable=False),
        StructField("fixed_charge", _MONEY, nullable=False),
        StructField("block_1_rate", _MONEY, nullable=False),
        StructField("block_2_rate", _MONEY, nullable=False),
        StructField("block_3_rate", _MONEY, nullable=False),
        StructField("export_rate", _MONEY, nullable=False),
    ]
)


def read_tariff(spark: SparkSession, sim_date: date, path: str | None = None) -> DataFrame:
    """The tariff that applies on `sim_date`: one validated row per household.

    Effective-dated (T113, §10.2): per household, the latest row with
    `effective_date <= sim_date`. `path` overrides the landing-zone location, for tests.
    Raises `ValueError` on a row that breaks the contract.
    """
    rows = (
        spark.read.option("header", "true")
        # Check the header against the schema instead of trusting column order: a
        # hand-edited file with two columns swapped fails rather than billing on them.
        .option("enforceSchema", "false")
        .option("mode", "FAILFAST")
        .schema(_TEXT_SCHEMA)
        .csv(path or landing_tariff_path(sim_date))
        .collect()
    )
    records = applicable_tariffs((row.asDict() for row in rows), sim_date)
    # One partition: createDataFrame would spread fifty rows over every core, and archiving
    # them then opened one Parquet writer per core, each reserving its own buffers.
    return spark.createDataFrame(
        [tuple(getattr(record, name) for name in TARIFF_COLUMNS) for record in records.values()],
        schema=TARIFF_SCHEMA,
    ).coalesce(1)
