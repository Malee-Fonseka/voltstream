"""Response schemas for the API (T106, §5.8).

The important one is `BillResponse`. **One model serves both branches of the merge** (D4),
and that is a design decision rather than convenience: the §3.2 contract is that a client
asking for a household's bill gets the same shape whether the day is finalised or not, and
learns which it got from `source` and `provisional` rather than from the keys present.

Two models would let the two branches drift — a field added to the batch response and
forgotten on the speed one — and a client would have to branch on shape before it could
read a value. Here a provisional and a final response for the same household differ in
their values and in four batch-only fields that are explicitly `null` when provisional,
never in which keys exist.

That labelling is what the demo screenshots: the same URL, before and after the batch run,
showing `provisional` flip from true to false and the total change.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field


class ZoneLoad(BaseModel):
    """One zone's most recent window — the operational view."""

    grid_zone: str
    window_start: datetime
    window_end: datetime
    total_consumption_kwh: Decimal
    total_solar_kwh: Decimal
    renewable_ratio: Decimal = Field(
        description=(
            "Solar as a fraction of consumption, saturated at 1.0. A zone generating more "
            "than it consumes reports 1.0; the surplus is visible in the two raw totals."
        )
    )
    active_meters: int


class ZoneHistory(BaseModel):
    grid_zone: str
    windows: list[ZoneLoad]


class BillResponse(BaseModel):
    """A household's bill for a simulated day, from whichever layer can answer.

    `source` and `provisional` are the merge contract. They always carry a value, and a
    client should read them before the total rather than inferring freshness from
    anything else.
    """

    household_id: str
    sim_date: date

    source: Literal["batch", "speed"] = Field(
        description=(
            "Which layer served this. 'batch' is authoritative; 'speed' is an estimate "
            "computed against the previous day's tariff."
        )
    )
    provisional: bool = Field(
        description="True when served from the speed layer, i.e. the day is not finalised."
    )
    tariff_date: date = Field(
        description=(
            "The tariff actually applied. For a provisional bill this is the previous "
            "day's, deliberately stale; for a final bill it is the day's own."
        )
    )

    consumption_kwh: Decimal
    solar_kwh: Decimal
    self_consumed_kwh: Decimal
    billable_import_kwh: Decimal
    export_kwh: Decimal

    energy_charge: Decimal
    fixed_charge: Decimal
    subsidy_discount: Decimal
    export_credit: Decimal
    tier_breakdown: Any = Field(description="Per-block kWh and charge, as written by the layer.")

    total: Decimal = Field(
        description="energy_charge + fixed_charge - subsidy_discount - export_credit."
    )

    # Batch-only. Present as keys on every response, null while provisional, so the shape
    # never changes between the two branches.
    readings_count: int | None = None
    duplicates_removed: int | None = None
    pipeline_run_id: str | None = None
    computed_at: datetime | None = None


class RejectedReason(BaseModel):
    reason: str
    count: int


class AlertStatus(BaseModel):
    """Firing alerts, surfaced to the dashboard (§10.4)."""

    name: str
    severity: str
    summary: str
    since: datetime | None = None


class DependencyHealth(BaseModel):
    name: str
    healthy: bool
    detail: str | None = None


class ReadinessResponse(BaseModel):
    """`/health/ready` — 503 when any dependency is down, with which one named.

    Per-dependency detail rather than a bare boolean: "not ready" without saying what is
    unreachable makes an operator check all of them by hand.
    """

    ready: bool
    dependencies: list[DependencyHealth]


class LivenessResponse(BaseModel):
    """`/health/live` — the process is up. Touches no dependency, by design."""

    alive: bool = True
