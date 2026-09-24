"""Household endpoints — where the two Lambda layers meet (T127, T129, §3.2).

This module contains the merge function, which is the architecture rendered as product
behaviour:

    query = merge(batch_view, realtime_view)

The whole of Lambda's promise reduces to a choice between two rows. The batch view wins
whenever it exists, unconditionally, because it is strictly better informed: it saw the
late data the speed layer dropped, applied the day's own tariff rather than yesterday's,
and ran deterministically over a closed input set. There is no case where the speed
estimate is the better answer — only cases where it is the *only* answer.

**No arithmetic here.** The merge picks a row and maps it to a response. Every figure was
computed by the layer that wrote it, using `core/`. An API that recomputed anything would
be a third implementation of the billing rules and would eventually disagree with both.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status

from voltstream.api.dependencies import TraceIdDep
from voltstream.api.models import BillDelta, BillResponse
from voltstream.logging_setup import get_logger
from voltstream.storage import repositories

router = APIRouter(prefix="/api/v1/households", tags=["households"])

log = get_logger("api")


def _from_batch(row: dict) -> BillResponse:
    """Map a `household_bill_daily` row. Authoritative, so `provisional=false`."""
    return BillResponse(
        household_id=row["household_id"],
        sim_date=row["sim_date"],
        source="batch",
        provisional=False,
        tariff_date=row["tariff_effective_date"],
        consumption_kwh=row["consumption_kwh"],
        solar_kwh=row["solar_kwh"],
        self_consumed_kwh=row["self_consumed_kwh"],
        billable_import_kwh=row["billable_import_kwh"],
        export_kwh=row["export_kwh"],
        energy_charge=row["energy_charge"],
        fixed_charge=row["fixed_charge"],
        subsidy_discount=row["subsidy_discount"],
        export_credit=row["export_credit"],
        tier_breakdown=row["tier_breakdown"],
        total=row["final_bill"],
        readings_count=row["readings_count"],
        duplicates_removed=row["duplicates_removed"],
        pipeline_run_id=str(row["pipeline_run_id"]),
        computed_at=row["computed_at"],
    )


def _from_speed(row) -> BillResponse:  # type: ignore[no-untyped-def]
    """Map a `household_running_rt` row.

    The four batch-only fields stay `None` — present as keys, absent as values. A client
    gets the same shape either way and learns which layer answered from `source` and
    `provisional`, never from which keys exist.
    """
    return BillResponse(
        household_id=row.household_id,
        sim_date=row.sim_date,
        source="speed",
        provisional=True,
        # Deliberately stale and deliberately labelled: yesterday's tariff, because
        # today's does not exist until the day closes (§3.1).
        tariff_date=row.tariff_source_date,
        consumption_kwh=row.consumption_kwh,
        solar_kwh=row.solar_kwh,
        self_consumed_kwh=row.self_consumed_kwh,
        billable_import_kwh=row.billable_import_kwh,
        export_kwh=row.export_kwh,
        energy_charge=row.energy_charge,
        fixed_charge=row.fixed_charge,
        subsidy_discount=row.subsidy_discount,
        export_credit=row.export_credit,
        tier_breakdown=row.tier_breakdown,
        total=row.estimated_bill,
        readings_count=None,
        duplicates_removed=None,
        pipeline_run_id=None,
        computed_at=None,
    )


@router.get(
    "/{household_id}/bill",
    response_model=BillResponse,
    summary="A household's bill, from whichever layer can answer",
)
def get_bill(
    household_id: str,
    trace_id: TraceIdDep,
    bill_date: Annotated[date, Query(alias="date", description="Simulated date, YYYY-MM-DD.")],
) -> BillResponse:
    """The merge function (§3.2).

    Finalised day -> the batch row. Otherwise -> the speed estimate, labelled
    provisional. Neither -> 404.
    """
    if repositories.is_day_finalised(bill_date):
        batch = repositories.get_finalised_bill_row(household_id, bill_date)
        if batch is not None:
            log.info(
                "bill served",
                extra={"stage": "api", "source": "batch", "household_id": household_id},
            )
            return _from_batch(batch)

    speed = repositories.get_running_estimate(household_id, bill_date)
    if speed is not None:
        log.info(
            "bill served",
            extra={"stage": "api", "source": "speed", "household_id": household_id},
        )
        return _from_speed(speed)

    # A day can be finalised without a row for this household only if the household does
    # not exist, so 404 is right for both the unknown household and the unbilled day.
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"no bill for household {household_id!r} on {bill_date.isoformat()}",
    )


@router.get(
    "/{household_id}/bill/delta",
    response_model=BillDelta,
    summary="Speed estimate against batch final, and why they differ",
)
def get_bill_delta(
    household_id: str,
    trace_id: TraceIdDep,
    bill_date: Annotated[date, Query(alias="date", description="Simulated date, YYYY-MM-DD.")],
) -> BillDelta:
    """Both figures for one household and day, with the divergence decomposed.

    §3.2 argues that the same household before and after finalisation proves
    comprehension of Lambda better than pages of prose. This makes that a single request,
    and — once the day has been reconciled — says *why* the two differ rather than only
    that they do.
    """
    speed = repositories.get_running_estimate(household_id, bill_date)
    batch = repositories.get_finalised_bill_row(household_id, bill_date)

    if speed is None and batch is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"no figures for household {household_id!r} on {bill_date.isoformat()}",
        )

    estimate = speed.estimated_bill if speed else None
    final = batch["final_bill"] if batch else None

    delta = None
    delta_pct = None
    if estimate is not None and final is not None:
        delta = final - estimate
        # Percentage against the final figure, which is the correct one — expressing the
        # error as a fraction of the estimate would flatter a bad estimate.
        #
        # Quantised to three places to match reconciliation_daily.pct_divergence. Decimal
        # division otherwise returns 28 significant digits, which is noise in a figure
        # this endpoint exists to put in a screenshot.
        if final != 0:
            delta_pct = ((delta / final) * 100).quantize(Decimal("0.001"))

    effects = repositories.get_reconciliation_effects(household_id, bill_date)

    return BillDelta(
        household_id=household_id,
        sim_date=bill_date,
        speed_estimate=estimate,
        batch_final=final,
        delta=delta,
        delta_pct=delta_pct,
        tariff_effect=effects.tariff_effect if effects else None,
        data_effect=effects.data_effect if effects else None,
        reconciled=effects is not None,
    )
