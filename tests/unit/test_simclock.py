"""Unit tests for voltstream.simclock (T024)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from voltstream import simclock
from voltstream.config import get_config

_EPOCH = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _fixed_epoch(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin epoch_real so every test reasons about the same anchor, per T024."""
    monkeypatch.setenv("VOLTSTREAM__SIMULATION__EPOCH_REAL", _EPOCH.isoformat())
    get_config.cache_clear()
    yield
    get_config.cache_clear()


def test_time_scale_is_288() -> None:
    assert get_config().simulation.time_scale == 288


def test_round_trip_real_to_sim_to_real() -> None:
    """sim_to_real(real_to_sim(t)) == t for any real t (T024).

    real_to_sim multiplies the elapsed duration by the (integer) time_scale, which is
    exact; sim_to_real then divides by the same integer, exactly undoing it. The reverse
    composition (sim -> real -> sim) is not guaranteed exact in general — dividing by 288
    can require sub-microsecond precision that a datetime cannot represent — so it is not
    asserted here.
    """
    real_dt = _EPOCH + timedelta(hours=3, minutes=17, seconds=4)
    assert simclock.sim_to_real(simclock.real_to_sim(real_dt)) == real_dt


def test_five_real_minutes_is_one_simulated_day() -> None:
    real_dt = _EPOCH + timedelta(minutes=5)
    sim_dt = simclock.real_to_sim(real_dt)
    assert sim_dt - _EPOCH == timedelta(days=1)


def test_two_real_seconds_is_9_6_simulated_minutes() -> None:
    real_dt = _EPOCH + timedelta(seconds=2)
    sim_dt = simclock.real_to_sim(real_dt)
    assert sim_dt - _EPOCH == timedelta(minutes=9.6)


def test_real_duration_to_sim_matches_the_288_scale_factor() -> None:
    assert simclock.real_duration_to_sim(timedelta(seconds=1)) == timedelta(seconds=288)


def test_sim_duration_to_real_matches_the_288_scale_factor() -> None:
    assert simclock.sim_duration_to_real(timedelta(seconds=288)) == timedelta(seconds=1)


def test_sim_date_of_before_simulated_midnight() -> None:
    just_before_midnight = _EPOCH + timedelta(hours=23, minutes=59, seconds=59)
    assert simclock.sim_date_of(just_before_midnight) == _EPOCH.date()


def test_sim_date_of_after_simulated_midnight() -> None:
    just_after_midnight = _EPOCH + timedelta(days=1, seconds=1)
    assert simclock.sim_date_of(just_after_midnight) == (_EPOCH + timedelta(days=1)).date()


def test_sim_hour_of() -> None:
    fourteen_thirty = _EPOCH + timedelta(hours=14, minutes=30)
    assert simclock.sim_hour_of(fourteen_thirty) == 14


def test_sim_now_is_close_to_epoch_immediately_after_it() -> None:
    now = simclock.sim_now()
    assert now.tzinfo is not None
    assert now >= _EPOCH


def test_all_returned_timestamps_are_tz_aware() -> None:
    real_dt = _EPOCH + timedelta(minutes=1)
    assert simclock.real_to_sim(real_dt).tzinfo is not None
    assert simclock.sim_to_real(real_dt).tzinfo is not None
    assert simclock.sim_now().tzinfo is not None
