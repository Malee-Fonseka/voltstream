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
    return get_config().simulation.anchor_real or _PROCESS_START_REAL


def _real_now() -> datetime:
    return datetime.now(UTC)


def sim_now() -> datetime:
    return real_to_sim(_real_now())


def real_to_sim(real_dt: datetime) -> datetime:
    return _epoch_sim() + (real_dt - _anchor_real()) * _time_scale()


def sim_to_real(sim_dt: datetime) -> datetime:
    return _anchor_real() + (sim_dt - _epoch_sim()) / _time_scale()


def real_duration_to_sim(real_delta: timedelta) -> timedelta:
    return real_delta * _time_scale()


def sim_duration_to_real(sim_delta: timedelta) -> timedelta:
    return sim_delta / _time_scale()


def sim_date_of(event_ts: datetime) -> date:
    return event_ts.astimezone(UTC).date()


def sim_hour_of(event_ts: datetime) -> int:
    return event_ts.astimezone(UTC).hour
