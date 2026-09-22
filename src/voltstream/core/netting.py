"""Solar self-consumption netting (decision T055, §3.3c) — pure `Decimal` arithmetic, no
rounding: kWh in at <= 4 dp comes out at <= 4 dp exactly. `core/spark_expr.py` implements
the identical formula as Column expressions (T061); `tests/consistency/test_pure_vs_spark.py`
pins the two together.
"""

from __future__ import annotations

from decimal import Decimal
from typing import NamedTuple


class NettingResult(NamedTuple):
    self_consumed_kwh: Decimal
    billable_import_kwh: Decimal
    export_kwh: Decimal


def net(consumption_kwh: Decimal, solar_kwh: Decimal) -> NettingResult:
    """§3.3c, exactly:

        self_consumed   = min(solar_kwh, consumption_kwh)
        billable_import  = consumption_kwh - self_consumed
        export_kwh       = solar_kwh - self_consumed
    """
    self_consumed = min(solar_kwh, consumption_kwh)
    return NettingResult(
        self_consumed_kwh=self_consumed,
        billable_import_kwh=consumption_kwh - self_consumed,
        export_kwh=solar_kwh - self_consumed,
    )
