"""The simulated clock, served (§3.4).

Every other endpoint answers in simulated time without saying what simulated time it is.
A dashboard that wants to show "now" otherwise has to guess it from the newest window,
which trails the simulated present by up to a trigger interval (48 simulated minutes) and
stops moving entirely when the speed layer does.

All arithmetic goes through `simclock`, which is the only module allowed to do
`time_scale` conversions. No SQL and no dependency: the clock is derived from config and
the wall clock, so this endpoint answers even when Postgres is down.
"""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta

from fastapi import APIRouter

from voltstream.api.dependencies import TraceIdDep
from voltstream.api.models import ClockResponse
from voltstream.config import get_config
from voltstream.simclock import sim_date_of, sim_duration_to_real, sim_now

router = APIRouter(prefix="/api/v1/clock", tags=["clock"])

_DAY = timedelta(days=1)


@router.get("", response_model=ClockResponse, summary="The current simulated instant")
def clock(trace_id: TraceIdDep) -> ClockResponse:
    """Simulated now, how far through its day it is, and how long until the day closes."""
    now = sim_now()
    day = sim_date_of(now)
    midnight = datetime.combine(day, time.min, tzinfo=UTC)
    elapsed = now - midnight
    return ClockResponse(
        sim_now=now,
        sim_date=day,
        time_scale=get_config().simulation.time_scale,
        day_progress=elapsed / _DAY,
        real_seconds_to_day_close=sim_duration_to_real(_DAY - elapsed).total_seconds(),
    )
