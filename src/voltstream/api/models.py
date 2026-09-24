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


class BillDelta(BaseModel):
    """Speed estimate against batch final for one household and day, and why they differ.

    Every figure is optional because the interesting states are the incomplete ones: a day
    still open has an estimate and no final, a day finalised before the speed layer saw it
    has the reverse, and the decomposition only exists once reconciliation has run.
    Returning nulls with `reconciled` saying so is more honest than inventing zeros.
    """

    household_id: str
    sim_date: date

    speed_estimate: Decimal | None = Field(
        default=None, description="Provisional total, computed against yesterday's tariff."
    )
    batch_final: Decimal | None = Field(
        default=None, description="Authoritative total, computed against the day's own tariff."
    )
    delta: Decimal | None = Field(
        default=None,
        description="batch_final - speed_estimate. Positive means the estimate was low.",
    )
    delta_pct: Decimal | None = Field(
        default=None,
        description=(
            "Divergence as a percentage of the final figure, not of the estimate — "
            "measuring the error against the wrong number would flatter a bad estimate."
        ),
    )

    tariff_effect: Decimal | None = Field(
        default=None,
        description="Share of the delta explained by the speed layer using yesterday's tariff.",
    )
    data_effect: Decimal | None = Field(
        default=None,
        description="Share explained by readings the speed layer never saw.",
    )
    reconciled: bool = Field(
        description="False until the reconciliation job has decomposed this day's divergence."
    )


class ReportZone(BaseModel):
    """One zone's authoritative daily totals, from the batch rollup."""

    grid_zone: str
    total_consumption_kwh: Decimal
    total_solar_kwh: Decimal
    self_consumed_kwh: Decimal
    export_kwh: Decimal
    renewable_ratio: Decimal
    peak_window_start: datetime
    peak_consumption_kwh: Decimal
    active_meters: int
    readings_count: int


class ReportBilling(BaseModel):
    households: int
    total_consumption_kwh: Decimal
    total_billed: Decimal
    readings_count: int
    duplicates_removed: int


class ReportRun(BaseModel):
    """One billing run. Superseded runs are included on purpose — a restated day has a
    history, and the report is where it should be visible."""

    status: str
    rows_in: int | None = None
    rows_out: int | None = None
    started_at: datetime
    finished_at: datetime | None = None
    orchestrator_run_id: str | None = None


class DailyReport(BaseModel):
    """The consolidated daily document.

    Returned for an open day as well as a finalised one. `finalised` and `completeness`
    tell the reader which sections are authoritative and which are simply not written yet,
    so an absent figure is never mistaken for a zero.
    """

    sim_date: date
    finalised: bool = Field(
        description="True once a billing run has succeeded for this simulated day."
    )
    completeness: dict[str, bool] = Field(
        description="Which sections have been produced: bills, zone_rollup, reconciliation."
    )

    zones: list[ReportZone]
    billing: ReportBilling | None = None
    runs: list[ReportRun]
    rejected: list[dict]
    reconciliation: dict | None = None


class RejectedReason(BaseModel):
    reason: str
    count: int


class AlertStatus(BaseModel):
    """Firing alerts, surfaced to the dashboard (§10.4)."""

    name: str
    severity: str
    summary: str
    since: datetime | None = None


class AlertsResponse(BaseModel):
    """Firing alerts, plus whether we could actually ask.

    `available` distinguishes "no alerts are firing" from "we could not find out", which
    look identical if the response is just a list. A dashboard showing an empty list for
    the second case is quietly claiming everything is fine.
    """

    alerts: list[AlertStatus]
    available: bool
    warning: str | None = None


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
