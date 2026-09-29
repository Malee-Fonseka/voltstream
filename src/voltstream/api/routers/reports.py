"""The consolidated daily report (T130, §5.8).

This is the deliverable the brief asks for: one document answering the use case's business
question for a whole simulated day — grid load and renewable contribution by zone, and
what each household's bill came to once the day's tariff was applied.

**A day that is not finalised still returns a report, clearly labelled partial.** The
alternative is a 404 until the batch job has run, which would make the endpoint useless
for the part of the day that is actually happening. `finalised` and `completeness` say
which kind of document this is, so a reader is never guessing whether a missing figure
means "zero" or "not yet".
"""

from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Query

from voltstream.api.dependencies import TraceIdDep
from voltstream.api.models import DailyReport, ReportBilling, ReportRun, ReportZone
from voltstream.logging_setup import get_logger
from voltstream.storage import repositories

router = APIRouter(prefix="/api/v1/reports", tags=["reports"])

log = get_logger("api")


@router.get(
    "/daily",
    response_model=DailyReport,
    summary="Consolidated report for one simulated day",
)
def daily_report(
    trace_id: TraceIdDep,
    report_date: Annotated[date, Query(alias="date", description="Simulated date, YYYY-MM-DD.")],
) -> DailyReport:
    """Everything known about one simulated day, from both layers and the run ledger."""
    finalised = repositories.is_day_finalised(report_date)

    zones = [ReportZone(**z._asdict()) for z in repositories.get_zone_daily(report_date)]
    billing = repositories.get_billing_summary(report_date)
    runs = [ReportRun(**r._asdict()) for r in repositories.get_run_summary(report_date)]
    rejected = repositories.get_rejected_for_day(
        report_date, repositories.reject_stage_for(finalised)
    )
    reconciliation = repositories.get_reconciliation_summary(report_date)

    # Spelled out rather than inferred from `finalised` alone, because the three can come
    # apart: the rollup runs after the bills, and reconciliation is a later phase again.
    # A reader seeing an empty zones list deserves to know whether that means no data or
    # no rollup yet.
    completeness = {
        "bills": billing is not None,
        "zone_rollup": bool(zones),
        "reconciliation": reconciliation is not None,
    }

    report = DailyReport(
        sim_date=report_date,
        finalised=finalised,
        completeness=completeness,
        zones=zones,
        billing=(
            ReportBilling(
                households=billing.households,
                total_consumption_kwh=billing.total_kwh,
                total_billed=billing.total_billed,
                readings_count=billing.readings_count,
                duplicates_removed=billing.duplicates_removed,
            )
            if billing
            else None
        ),
        runs=runs,
        rejected=[{"reason": r.reason, "count": r.total} for r in rejected],
        reconciliation=(
            {
                "households": reconciliation[0],
                "mean_abs_divergence": reconciliation[1],
                "mean_pct_divergence": reconciliation[2],
            }
            if reconciliation
            else None
        ),
    )

    log.info(
        "daily report served",
        extra={
            "stage": "api",
            "sim_date": report_date.isoformat(),
            "finalised": finalised,
            "zones": len(zones),
        },
    )
    return report
