"""The daily reference-data contracts (decision T031, §6.2, D2) — the two files
`reference_dropper.py` drops once per simulated day: the tariff CSV and the weather CSV.

Per D2, **every number that enters bill arithmetic comes from the day's tariff file, per
household** — `TariffRecord` below is the frozen, final ten-column contract, not the
seven-column shape in the original §6.2 draft. `config/base.yaml`'s `tariff.blocks` holds
only the block *boundaries*; every rate is a per-household, per-day value here instead.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


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


class WeatherForecast(BaseModel):
    """One row of `weather_YYYY-MM-DD.csv` (§6.2, unchanged by D2)."""

    model_config = ConfigDict(extra="forbid")

    grid_zone: str
    forecast_date: date
    cloud_cover_pct: Decimal = Field(max_digits=5, decimal_places=2, ge=0, le=100)
    temperature_c: Decimal = Field(max_digits=5, decimal_places=2, ge=-90, le=60)
    solar_irradiance_index: Decimal = Field(max_digits=4, decimal_places=3, ge=0, le=1)
