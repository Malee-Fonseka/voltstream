"""Request and response schemas for the API (T106, §5.8).

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

from pydantic import BaseModel, ConfigDict, Field


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


class HouseholdInfo(BaseModel):
    """One seeded household: who it is, not what it used."""

    household_id: str
    meter_id: str
    grid_zone: str
    billing_tier: str
    subsidy_flag: bool
    has_solar: bool


class ClockResponse(BaseModel):
    """The simulated clock as the API process reads it (§3.4).

    A dashboard shows this rather than the newest window's end, which trails the
    simulated present by up to a trigger interval. `time_scale` is included so a client
    can advance the clock smoothly between polls and re-sync on the next one.
    """

    sim_now: datetime = Field(description="The current simulated instant, UTC.")
    sim_date: date = Field(description="The simulated day that instant falls on.")
    time_scale: int = Field(description="Simulated seconds per real second.")
    day_progress: float = Field(
        description="Fraction of the simulated day elapsed, 0 at midnight, towards 1."
    )
    real_seconds_to_day_close: float = Field(
        description="REAL seconds until simulated midnight, when the day becomes billable."
    )


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
        description=(
            "speed_estimate - batch_final (D4). Positive means the estimate was high. Once "
            "the day is reconciled, delta == tariff_effect + data_effect."
        ),
    )
    delta_pct: Decimal | None = Field(
        default=None,
        description=(
            "|delta| as a percentage of the final bill's gross charges (energy + fixed), "
            "0 when those are 0 (D5): the figure reconciliation_daily.pct_divergence "
            "holds. Not a percentage of batch_final, which is negative or near zero for "
            "a net exporter."
        ),
    )

    tariff_effect: Decimal | None = Field(
        default=None,
        description=(
            "Part of delta caused by the speed layer using yesterday's tariff (S - C, D4)."
        ),
    )
    data_effect: Decimal | None = Field(
        default=None,
        description=(
            "Part of delta caused by readings the two layers saw differently (C - B, D4)."
        ),
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


class AlertmanagerAlert(BaseModel):
    """One alert in an Alertmanager webhook notification.

    Only the fields the receiver logs. Unknown fields are ignored rather than rejected:
    the payload belongs to Alertmanager, and a new field in a later version must not turn
    every notification into a 422.
    """

    model_config = ConfigDict(extra="ignore")

    status: str
    labels: dict[str, str] = Field(default_factory=dict)
    annotations: dict[str, str] = Field(default_factory=dict)
    startsAt: datetime | None = None  # noqa: N815 - Alertmanager's field name
    endsAt: datetime | None = None  # noqa: N815 - Alertmanager's field name
    fingerprint: str | None = None


class AlertmanagerNotification(BaseModel):
    """The body Alertmanager POSTs to a webhook receiver (payload version 4)."""

    model_config = ConfigDict(extra="ignore")

    receiver: str
    status: str
    alerts: list[AlertmanagerAlert]


class WebhookAck(BaseModel):
    received: int


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
