"""Zone endpoints — the operational view (T107, §5.8).

This is the business question the brief poses in its live half: *what is the current grid
load and renewable contribution by zone?* It reads the speed layer's output and nothing
else, so it stays available whether or not the batch layer has run.

No SQL here (§7.2). Every query is a call into `storage/repositories.py`.
"""

from __future__ import annotations

from datetime import timedelta

from fastapi import APIRouter, HTTPException, Query, status

from voltstream.api.dependencies import TraceIdDep
from voltstream.api.models import ZoneHistory, ZoneLoad
from voltstream.logging_setup import get_logger
from voltstream.simclock import sim_now
from voltstream.storage import repositories

router = APIRouter(prefix="/api/v1/zones", tags=["zones"])

log = get_logger("api")

# History is served from the speed view, which holds recent windows rather than the full
# history — the batch view is where a long horizon lives. Capping the request makes that
# boundary explicit instead of quietly returning a short answer to a long question.
_MAX_HISTORY_MINUTES = 24 * 60


@router.get("", response_model=list[ZoneLoad], summary="Latest window for every zone")
@router.get("/load", response_model=list[ZoneLoad], summary="Latest window for every zone")
def zone_load(trace_id: TraceIdDep) -> list[ZoneLoad]:
    """The newest 15-simulated-minute window per zone.

    Values change every trigger while the speed layer runs, which is the point: this is
    the endpoint a dashboard polls.
    """
    rows = repositories.get_latest_zone_metrics()
    log.info("zone load served", extra={"stage": "api", "zones": len(rows)})
    return [ZoneLoad(**row._asdict()) for row in rows]


@router.get(
    "/{grid_zone}/history",
    response_model=ZoneHistory,
    summary="One zone's recent windows",
)
def zone_history(
    grid_zone: str,
    trace_id: TraceIdDep,
    minutes: int = Query(
        60,
        ge=1,
        le=_MAX_HISTORY_MINUTES,
        description="How far back to look, in SIMULATED minutes.",
    ),
) -> ZoneHistory:
    """Windows for one zone over a simulated-time interval.

    `minutes` is **simulated**, because `window_start` is simulated time. The range is
    therefore anchored to `sim_now()`, not to the wall clock: `datetime.now()` here would
    compare a real instant against simulated timestamps and return nothing at all, since
    simulated time runs 288x ahead of it.
    """
    now = sim_now()
    rows = repositories.get_zone_metrics_range(grid_zone, now - timedelta(minutes=minutes), now)
    if not rows:
        # A zone with no windows is either an unknown zone or one whose meters are all
        # silent. Both are worth a 404 rather than an empty list, which reads as "this
        # zone is fine and using nothing".
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"no windows for zone {grid_zone!r} in the last {minutes} simulated minutes",
        )
    return ZoneHistory(grid_zone=grid_zone, windows=[ZoneLoad(**row._asdict()) for row in rows])
