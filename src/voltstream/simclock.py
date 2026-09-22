"""The simulated clock (decision T023, §3.4) — the single source of truth for simulated
time. No other module may do `time_scale` arithmetic; every conversion between real and
simulated time goes through the functions here.

Simulated time is anchored to a fixed real-world instant, `simulation.epoch_real`
(`get_config().simulation.epoch_real`) — the "sim epoch". From that instant on:

    sim_now() = epoch_real + (real_now() - epoch_real) * time_scale

Every process in a run reads the same `epoch_real` from config, so independent processes
(the producer, the speed layer, the API) agree on simulated "now" without talking to each
other — that is the entire point of anchoring to a config value instead of "process start".

All timestamps in and out of this module are timezone-aware UTC `datetime` objects.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from voltstream.config import get_config


def _time_scale() -> int:
    return get_config().simulation.time_scale


def _epoch_real() -> datetime:
    return get_config().simulation.epoch_real


def _real_now() -> datetime:
    return datetime.now(UTC)


def sim_now() -> datetime:
    """The current simulated instant, derived from real wall-clock time."""
    return real_to_sim(_real_now())


def real_to_sim(real_dt: datetime) -> datetime:
    """Convert a real (wall-clock) instant to its simulated instant."""
    epoch = _epoch_real()
    real_elapsed = real_dt - epoch
    return epoch + real_elapsed * _time_scale()


def sim_to_real(sim_dt: datetime) -> datetime:
    """Convert a simulated instant back to the real instant it occurs at."""
    epoch = _epoch_real()
    sim_elapsed = sim_dt - epoch
    return epoch + sim_elapsed / _time_scale()


def real_duration_to_sim(real_delta: timedelta) -> timedelta:
    """Scale a real-time duration up into the equivalent simulated-time duration."""
    return real_delta * _time_scale()


def sim_duration_to_real(sim_delta: timedelta) -> timedelta:
    """Scale a simulated-time duration down into the equivalent real-time duration."""
    return sim_delta / _time_scale()


def sim_date_of(event_ts: datetime) -> date:
    """The simulated calendar date (UTC) a simulated timestamp falls on."""
    return event_ts.astimezone(UTC).date()


def sim_hour_of(event_ts: datetime) -> int:
    """The simulated hour-of-day (0-23, UTC) a simulated timestamp falls on."""
    return event_ts.astimezone(UTC).hour
