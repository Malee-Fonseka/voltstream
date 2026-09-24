"""Every query the API makes (T100, T101, §7.2).

**All SQL lives here.** Routers call functions on this module and never write SQL — the
rule is enforced by grepping the router package, and it exists so a schema change has one
place to land rather than a dozen. Every query is parameterised; no value is ever
interpolated into a statement.

Each function returns a typed row or `None`, never a raw tuple. A tuple's meaning is its
position, which is exactly the thing that silently changes when a column is added to a
`SELECT *` or reordered in a schema.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, NamedTuple

from voltstream.storage.postgres import transaction

# Batch layer marks a day finished by inserting a row here; the merge function reads it.
_FINALISED_LAYER = "batch_billing"


class ZoneMetric(NamedTuple):
    grid_zone: str
    window_start: datetime
    window_end: datetime
    total_consumption_kwh: Decimal
    total_solar_kwh: Decimal
    renewable_ratio: Decimal
    active_meters: int


class RunningEstimate(NamedTuple):
    """A row of `household_running_rt` — the speed layer's provisional bill."""

    household_id: str
    sim_date: date
    consumption_kwh: Decimal
    solar_kwh: Decimal
    self_consumed_kwh: Decimal
    billable_import_kwh: Decimal
    export_kwh: Decimal
    energy_charge: Decimal
    fixed_charge: Decimal
    subsidy_discount: Decimal
    export_credit: Decimal
    tier_breakdown: Any
    estimated_bill: Decimal
    tariff_source_date: date


class RejectedSummary(NamedTuple):
    reason: str
    # Not `count`: a NamedTuple field of that name shadows tuple.count, so the method is
    # gone and anything calling it gets an int back instead.
    total: int


def _zone_metric(row: tuple) -> ZoneMetric:
    return ZoneMetric(*row)


# --------------------------------------------------------------------------------------
# Zone queries (T100)
# --------------------------------------------------------------------------------------

_ZONE_COLUMNS = (
    "grid_zone, window_start, window_end, total_consumption_kwh, "
    "total_solar_kwh, renewable_ratio, active_meters"
)


def get_latest_zone_metrics() -> list[ZoneMetric]:
    """The newest window for each zone — what the operational dashboard shows.

    `DISTINCT ON` rather than a window function or a correlated subquery: it is the
    cheapest way to take one row per zone in Postgres, and the index on
    `(grid_zone, window_start DESC)` serves it directly.
    """
    with transaction() as cur:
        cur.execute(
            f"SELECT DISTINCT ON (grid_zone) {_ZONE_COLUMNS} "
            "FROM zone_metrics_rt ORDER BY grid_zone, window_start DESC"
        )
        return [_zone_metric(r) for r in cur.fetchall()]


def get_zone_metrics_range(
    grid_zone: str, window_from: datetime, window_to: datetime
) -> list[ZoneMetric]:
    """One zone's windows over an interval, oldest first, for a trend line."""
    with transaction() as cur:
        cur.execute(
            f"SELECT {_ZONE_COLUMNS} FROM zone_metrics_rt "
            "WHERE grid_zone = %s AND window_start >= %s AND window_start < %s "
            "ORDER BY window_start",
            (grid_zone, window_from, window_to),
        )
        return [_zone_metric(r) for r in cur.fetchall()]


# --------------------------------------------------------------------------------------
# Bill and run queries (T101)
# --------------------------------------------------------------------------------------


def is_day_finalised(sim_date: date) -> bool:
    """Whether the batch layer has successfully closed this simulated day.

    This is the merge function's whole decision (§3.2), so the query has to be exact.

    The filter on `status = 'success'` is load-bearing and is T040's trap. A restated day
    keeps its earlier run rows and marks them `superseded`, so a day that has been
    rebuilt once holds several rows for the same `(sim_date, layer)`. Matching on
    "a row exists" would report a failed or superseded run as finalised and serve a bill
    that was withdrawn. The partial unique index guarantees at most one `success` row per
    day and layer, which is what makes this a safe existence check.
    """
    with transaction() as cur:
        cur.execute(
            "SELECT 1 FROM pipeline_runs "
            "WHERE sim_date = %s AND layer = %s AND status = 'success' LIMIT 1",
            (sim_date, _FINALISED_LAYER),
        )
        return cur.fetchone() is not None


def get_running_estimate(household_id: str, sim_date: date) -> RunningEstimate | None:
    """The speed layer's provisional bill for one household and day."""
    with transaction() as cur:
        cur.execute(
            "SELECT household_id, sim_date, consumption_kwh, solar_kwh, self_consumed_kwh, "
            "billable_import_kwh, export_kwh, energy_charge, fixed_charge, subsidy_discount, "
            "export_credit, tier_breakdown, estimated_bill, tariff_source_date "
            "FROM household_running_rt WHERE household_id = %s AND sim_date = %s",
            (household_id, sim_date),
        )
        row = cur.fetchone()
        return RunningEstimate(*row) if row else None


def get_finalised_bill(household_id: str, sim_date: date) -> tuple | None:
    """The batch layer's authoritative bill.

    Returns the raw row for now: `household_bill_daily` is written by the batch job,
    which does not exist until Phase 9, so the column set is not yet settled. Typing it
    against a guess would be worse than typing it once the writer is real.

    ponytail: give this a NamedTuple like the others when the batch job lands.
    """
    with transaction() as cur:
        cur.execute(
            "SELECT * FROM household_bill_daily WHERE household_id = %s AND sim_date = %s",
            (household_id, sim_date),
        )
        return cur.fetchone()


def get_reconciliation(sim_date: date) -> list[tuple]:
    """Speed-versus-batch divergence per household for a day (Phase 11 writes it)."""
    with transaction() as cur:
        cur.execute(
            "SELECT * FROM reconciliation_daily WHERE sim_date = %s ORDER BY household_id",
            (sim_date,),
        )
        return cur.fetchall()


def get_rejected_summary(minutes: int = 60) -> list[RejectedSummary]:
    """Rejected records by reason over a recent real-time window.

    Real minutes, not simulated: this answers "is the pipeline healthy right now", which
    is a wall-clock question, and `rejected_at` is a wall-clock column.
    """
    with transaction() as cur:
        cur.execute(
            "SELECT reason, count(*) FROM rejected_records "
            "WHERE rejected_at >= now() - make_interval(mins => %s) "
            "GROUP BY reason ORDER BY count(*) DESC",
            (minutes,),
        )
        return [RejectedSummary(reason, total) for reason, total in cur.fetchall()]
