"""The daily reference-data contracts (decision T031, §6.2, D2) — the two files
`reference_dropper.py` drops once per simulated day: the tariff CSV and the weather CSV.

Per D2, **every number that enters bill arithmetic comes from the day's tariff file, per
household** — `TariffRecord` below is the frozen, final ten-column contract, not the
seven-column shape in the original §6.2 draft. `config/base.yaml`'s `tariff.blocks` holds
only the block *boundaries*; every rate is a per-household, per-day value here instead.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Mapping
from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class TariffRecord(BaseModel):
    """One row of `tariff_YYYY-MM-DD.csv` (D2's final ten-column contract)."""

    model_config = ConfigDict(extra="forbid")

    household_id: str
    effective_date: date
    # The tariff structure is fixed at three tiers for the life of the deployment — the
    # same three keys config/base.yaml's generator_defaults.fixed_charge_by_tier assumes.
    # Kept as a Literal here (not read from config) so contracts/ has no config dependency.
    billing_tier: Literal["TIER_1", "TIER_2", "TIER_3"]
    subsidy_flag: bool
    # Written on every row regardless of subsidy_flag (D2), so a restatement that flips
    # eligibility is a one-column edit.
    subsidy_pct: Decimal = Field(max_digits=5, decimal_places=2, ge=0, le=100)
    fixed_charge: Decimal = Field(max_digits=12, decimal_places=2, ge=0)
    block_1_rate: Decimal = Field(max_digits=12, decimal_places=2, ge=0)
    block_2_rate: Decimal = Field(max_digits=12, decimal_places=2, ge=0)
    block_3_rate: Decimal = Field(max_digits=12, decimal_places=2, ge=0)
    export_rate: Decimal = Field(max_digits=12, decimal_places=2, ge=0)

    # Deliberately NOT validated: block_1_rate <= block_2_rate <= block_3_rate (D2) —
    # monotonicity and continuity of the bill hold for any non-negative rate schedule,
    # and constraining the ordering would only make the restatement demo less flexible.


TARIFF_COLUMNS = tuple(TariffRecord.model_fields)


def applicable_tariffs(
    rows: Iterable[Mapping[str, object]], sim_date: date
) -> dict[str, TariffRecord]:
    """A tariff file's rows as validated records, keyed by household.

    Every row goes through `TariffRecord`, so a hand-edited restatement file with a
    negative rate or a subsidy above 100 % fails here, naming the line, rather than
    producing a bill nobody can explain. `subsidy_flag` is read the contract's way:
    `true`, `1` and `True` all mean subsidised (R22).

    The effective-dated rule (T113, §10.2): per household, the latest row with
    `effective_date <= sim_date`. Two rows tied on that date are an error, since picking
    one of them would be arbitrary.
    """
    applicable: dict[str, TariffRecord] = {}
    for line_no, raw in enumerate(rows, start=2):  # line 1 is the header
        try:
            record = TariffRecord.model_validate(dict(raw))
        except ValidationError as exc:
            raise ValueError(f"tariff file for {sim_date}, line {line_no}: {exc}") from exc

        if record.effective_date > sim_date:
            continue
        current = applicable.get(record.household_id)
        if current is None or record.effective_date > current.effective_date:
            applicable[record.household_id] = record
        elif record.effective_date == current.effective_date:
            raise ValueError(
                f"tariff file for {sim_date}, line {line_no}: a second row for "
                f"{record.household_id} effective {record.effective_date}"
            )
    return applicable


def parse_tariff_csv(data: bytes, sim_date: date) -> dict[str, TariffRecord]:
    """`applicable_tariffs` over the bytes of a tariff file."""
    return applicable_tariffs(csv.DictReader(io.StringIO(data.decode("utf-8"))), sim_date)


class WeatherForecast(BaseModel):
    """One row of `weather_YYYY-MM-DD.csv` (§6.2, unchanged by D2)."""

    model_config = ConfigDict(extra="forbid")

    grid_zone: str
    forecast_date: date
    cloud_cover_pct: Decimal = Field(max_digits=5, decimal_places=2, ge=0, le=100)
    temperature_c: Decimal = Field(max_digits=5, decimal_places=2, ge=-90, le=60)
    solar_irradiance_index: Decimal = Field(max_digits=4, decimal_places=3, ge=0, le=1)
