"""Every query the API makes (T100, T101, §7.2), plus the reconciliation job's (D4).

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


class ReconciliationEffects(NamedTuple):
    """D4's decomposition of one household's divergence for one day."""

    tariff_effect: Decimal
    data_effect: Decimal
    abs_divergence: Decimal
    pct_divergence: Decimal


class BatchBill(NamedTuple):
    """The part of a `household_bill_daily` row reconciliation needs: `B`, the kWh it was
    computed from, and the gross charges that are `pct_divergence`'s base (D5)."""

    household_id: str
    sim_date: date
    consumption_kwh: Decimal
    solar_kwh: Decimal
    energy_charge: Decimal
    fixed_charge: Decimal
    final_bill: Decimal


class ReconciliationRow(NamedTuple):
    """One row of `reconciliation_daily`, in the table's column order."""

    household_id: str
    sim_date: date
    speed_estimate: Decimal
    batch_final: Decimal
    abs_divergence: Decimal
    pct_divergence: Decimal
    tariff_effect: Decimal
    data_effect: Decimal


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


# In RunningEstimate's field order.
_RUNNING_COLUMNS = (
    "household_id, sim_date, consumption_kwh, solar_kwh, self_consumed_kwh, "
    "billable_import_kwh, export_kwh, energy_charge, fixed_charge, subsidy_discount, "
    "export_credit, tier_breakdown, estimated_bill, tariff_source_date"
)


def get_running_estimate(household_id: str, sim_date: date) -> RunningEstimate | None:
    """The speed layer's provisional bill for one household and day."""
    with transaction() as cur:
        cur.execute(
            f"SELECT {_RUNNING_COLUMNS} FROM household_running_rt "
            "WHERE household_id = %s AND sim_date = %s",
            (household_id, sim_date),
        )
        row = cur.fetchone()
        return RunningEstimate(*row) if row else None


def get_running_estimates_for_day(sim_date: date) -> list[RunningEstimate]:
    """Every household's provisional bill for one day — reconciliation's `S` (D4)."""
    with transaction() as cur:
        cur.execute(
            f"SELECT {_RUNNING_COLUMNS} FROM household_running_rt "
            "WHERE sim_date = %s ORDER BY household_id",
            (sim_date,),
        )
        return [RunningEstimate(*r) for r in cur.fetchall()]


_BILL_COLUMNS = (
    "household_id, sim_date, consumption_kwh, solar_kwh, self_consumed_kwh, "
    "billable_import_kwh, export_kwh, energy_charge, fixed_charge, subsidy_discount, "
    "export_credit, final_bill, tier_breakdown, tariff_effective_date, readings_count, "
    "duplicates_removed, pipeline_run_id, computed_at"
)


def get_finalised_bill_row(household_id: str, sim_date: date) -> dict | None:
    """The batch layer's authoritative bill, as a column-keyed mapping.

    A dict rather than a positional tuple, and an explicit column list rather than
    `SELECT *`: this row has eighteen columns and is consumed by the merge function, so
    a schema change that reorders or inserts one must not silently shift every value one
    position to the left.
    """
    with transaction() as cur:
        cur.execute(
            f"SELECT {_BILL_COLUMNS} FROM household_bill_daily "
            "WHERE household_id = %s AND sim_date = %s",
            (household_id, sim_date),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return dict(zip([c.name for c in cur.description], row, strict=True))


def get_reconciliation_effects(household_id: str, sim_date: date) -> ReconciliationEffects | None:
    """One household's divergence decomposition, or None before the day is reconciled."""
    with transaction() as cur:
        cur.execute(
            "SELECT tariff_effect, data_effect, abs_divergence, pct_divergence "
            "FROM reconciliation_daily WHERE household_id = %s AND sim_date = %s",
            (household_id, sim_date),
        )
        row = cur.fetchone()
        return ReconciliationEffects(*row) if row else None


def get_batch_bills_for_day(sim_date: date) -> list[BatchBill]:
    """Every household's final bill for one day — reconciliation's `B` (D4)."""
    with transaction() as cur:
        cur.execute(
            "SELECT household_id, sim_date, consumption_kwh, solar_kwh, energy_charge, "
            "fixed_charge, final_bill FROM household_bill_daily "
            "WHERE sim_date = %s ORDER BY household_id",
            (sim_date,),
        )
        return [BatchBill(*r) for r in cur.fetchall()]


def replace_reconciliation(sim_date: date, rows: list[ReconciliationRow]) -> int:
    """Replace one day's reconciliation, in one transaction. Returns rows written.

    Delete-then-insert rather than upsert, so a rerun after a restatement cannot leave
    behind a row for a household the new run no longer reconciles. One transaction, so a
    reader never sees the day half-replaced — which also makes a DAG retry safe.
    """
    stray = {row.sim_date for row in rows} - {sim_date}
    if stray:
        raise ValueError(f"rows for {sorted(stray)} passed to replace the day {sim_date}")

    with transaction() as cur:
        cur.execute("DELETE FROM reconciliation_daily WHERE sim_date = %s", (sim_date,))
        if rows:
            cur.executemany(
                f"INSERT INTO reconciliation_daily ({', '.join(ReconciliationRow._fields)}) "
                f"VALUES ({', '.join(['%s'] * len(ReconciliationRow._fields))})",
                rows,
            )
    return len(rows)


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


# --------------------------------------------------------------------------------------
# Daily report (T130)
# --------------------------------------------------------------------------------------


class ZoneDaily(NamedTuple):
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


class BillingSummary(NamedTuple):
    households: int
    total_kwh: Decimal
    total_billed: Decimal
    readings_count: int
    duplicates_removed: int


class RunSummary(NamedTuple):
    status: str
    rows_in: int | None
    rows_out: int | None
    started_at: datetime
    finished_at: datetime | None
    orchestrator_run_id: str | None


def get_zone_daily(sim_date: date) -> list[ZoneDaily]:
    """Authoritative per-zone totals for a day, from the batch rollup."""
    with transaction() as cur:
        cur.execute(
            "SELECT grid_zone, total_consumption_kwh, total_solar_kwh, self_consumed_kwh, "
            "export_kwh, renewable_ratio, peak_window_start, peak_consumption_kwh, "
            "active_meters, readings_count FROM zone_metrics_daily "
            "WHERE sim_date = %s ORDER BY grid_zone",
            (sim_date,),
        )
        return [ZoneDaily(*r) for r in cur.fetchall()]


def get_billing_summary(sim_date: date) -> BillingSummary | None:
    """Totals across every household's finalised bill, or None if the day is not billed."""
    with transaction() as cur:
        cur.execute(
            "SELECT count(*), COALESCE(SUM(consumption_kwh), 0), COALESCE(SUM(final_bill), 0), "
            "COALESCE(SUM(readings_count), 0), COALESCE(SUM(duplicates_removed), 0) "
            "FROM household_bill_daily WHERE sim_date = %s",
            (sim_date,),
        )
        row = cur.fetchone()
        if row is None or row[0] == 0:
            return None
        return BillingSummary(*row)


def get_run_summary(sim_date: date) -> list[RunSummary]:
    """Every billing run for a day, newest first.

    All of them, not only the successful one: a day that was restated has a history, and
    the report is where that history should be visible rather than hidden behind the
    final number.
    """
    with transaction() as cur:
        cur.execute(
            "SELECT status, rows_in, rows_out, started_at, finished_at, orchestrator_run_id "
            "FROM pipeline_runs WHERE sim_date = %s AND layer = 'batch_billing' "
            "ORDER BY started_at DESC",
            (sim_date,),
        )
        return [RunSummary(*r) for r in cur.fetchall()]


def get_rejected_for_day(sim_date: date) -> list[RejectedSummary]:
    """Rejections attributable to a simulated day.

    Matched on the payload's `event_ts` rather than `rejected_at`: a record rejected by
    the batch layer is rejected when the job runs, which can be a different real day from
    the simulated one it belongs to. Counting by wall clock would attribute a restatement
    of last week's data to today.
    """
    with transaction() as cur:
        cur.execute(
            "SELECT reason, count(*) FROM rejected_records "
            "WHERE (raw_payload ->> 'event_ts')::timestamptz::date = %s "
            "GROUP BY reason ORDER BY count(*) DESC",
            (sim_date,),
        )
        return [RejectedSummary(reason, total) for reason, total in cur.fetchall()]


def get_reconciliation_summary(sim_date: date) -> tuple[int, Decimal, Decimal] | None:
    """Count, mean absolute divergence and mean percent divergence for a day."""
    with transaction() as cur:
        cur.execute(
            "SELECT count(*), COALESCE(AVG(abs_divergence), 0), COALESCE(AVG(pct_divergence), 0) "
            "FROM reconciliation_daily WHERE sim_date = %s",
            (sim_date,),
        )
        row = cur.fetchone()
        if row is None or row[0] == 0:
            return None
        return (row[0], row[1], row[2])
