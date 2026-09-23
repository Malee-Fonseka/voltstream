"""The simulated clock (decision T023, §3.4) — the single source of truth for simulated
time. No other module may do `time_scale` arithmetic; every conversion between real and
simulated time goes through the functions here.

Two anchors, deliberately separate:

- **`simulation.epoch_sim`** — the simulated instant a run begins at. Fixed in config, so
  every run's data starts on the same simulated date and partition names are stable.
- **`simulation.anchor_real`** — the *real* instant that maps to `epoch_sim`. When unset
  it defaults to process start.

      sim_now() = epoch_sim + (real_now() - anchor_real) * time_scale

Collapsing these into one value is a trap, and this module used to fall into it. If a
single config field is both the simulated start *and* the real anchor, then the moment
real time drifts past it the gap is multiplied by `time_scale`: an anchor left at
2026-01-01 and read 265 real days later yields a simulated date in the year 2235. Nothing
is inconsistent — every process agrees — but the dates are nonsense, and they end up in
partition paths, serving-table keys and report screenshots.

**Multi-process runs must set `anchor_real` explicitly.** The process-start default keeps
a single process sane, but two processes started seconds apart would disagree by
`skew * time_scale` — five real seconds of startup skew is 24 simulated minutes, enough
to place the same reading in a different window. Compose passes one value to every
container through `VOLTSTREAM__SIMULATION__ANCHOR_REAL` so they share an anchor.

All timestamps in and out of this module are timezone-aware UTC `datetime` objects.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from voltstream.config import get_config

# Captured once, at import, so every call in this process shares one anchor. Reading the
# clock per call would make simulated time advance at 1x between calls and jump at
# time_scale within them.
_PROCESS_START_REAL = datetime.now(UTC)


def _time_scale() -> int:
    return get_config().simulation.time_scale


def _epoch_sim() -> datetime:
    return get_config().simulation.epoch_sim


def _anchor_real() -> datetime:
    """The real instant mapping to `epoch_sim`; process start when unconfigured."""
    return get_config().simulation.anchor_real or _PROCESS_START_REAL


def _real_now() -> datetime:
    return datetime.now(UTC)


def sim_now() -> datetime:
    """The current simulated instant, derived from real wall-clock time."""
    return real_to_sim(_real_now())


def real_to_sim(real_dt: datetime) -> datetime:
    """Convert a real (wall-clock) instant to its simulated instant."""
    return _epoch_sim() + (real_dt - _anchor_real()) * _time_scale()


def sim_to_real(sim_dt: datetime) -> datetime:
    """Convert a simulated instant back to the real instant it occurs at."""
    return _anchor_real() + (sim_dt - _epoch_sim()) / _time_scale()


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
