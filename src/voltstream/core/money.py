"""Precision and rounding constants (decision T055, D5). The single source for the
`Decimal`/`DecimalType`/`NUMERIC` precision every kWh and money value uses throughout the
codebase — `core/spark_expr.py` builds its Spark `DecimalType`s from these same ints, so
Python and Spark cannot drift on precision even if they drift on nothing else.

No `pyspark` import here — this module is imported by the `app` image too (via
`core/tariff.py`, `batch/reconciliation.py`), which never ships PySpark (§7.3).
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

# (precision, scale) pairs, as plain ints — matches the Postgres NUMERIC columns in
# docker/init/postgres/01_schema.sql exactly.
MONEY = (12, 2)
KWH = (12, 4)
PCT = (5, 2)

_MONEY_QUANT = Decimal(1).scaleb(-MONEY[1])  # Decimal("0.01")
_KWH_QUANT = Decimal(1).scaleb(-KWH[1])  # Decimal("0.0001")


def round_money(value: Decimal) -> Decimal:
    """Round to money precision (2 dp), half-up (away from zero) — D5's pinned rule.
    Every rounded value here is non-negative, so half-up and round-towards-+infinity
    coincide; this is never used on a value that could legitimately need round-to-even."""
    return value.quantize(_MONEY_QUANT, rounding=ROUND_HALF_UP)


def quantize_kwh(value: Decimal) -> Decimal:
    """Quantize to kWh precision (4 dp) for storage/display uniformity. Readings already
    arrive with <= 4 dp (D5); this never rounds away meaningful precision in practice."""
    return value.quantize(_KWH_QUANT, rounding=ROUND_HALF_UP)
