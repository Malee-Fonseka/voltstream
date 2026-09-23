"""Unit tests for voltstream.simclock (T024)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from voltstream import simclock
from voltstream.config import get_config

_EPOCH_SIM = datetime(2026, 1, 1, tzinfo=UTC)
_ANCHOR = datetime(2026, 9, 23, 6, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _fixed_epoch(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin both anchors so every test reasons about the same clock, per T024.

    The anchor is deliberately a different real date from the simulated epoch: an
    earlier version of these tests measured from an instant adjacent to the epoch, which
    is why it never noticed the two were the same field.
    """
    monkeypatch.setenv("VOLTSTREAM__SIMULATION__EPOCH_SIM", _EPOCH_SIM.isoformat())
    monkeypatch.setenv("VOLTSTREAM__SIMULATION__ANCHOR_REAL", _ANCHOR.isoformat())
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
    real_dt = _ANCHOR + timedelta(hours=3, minutes=17, seconds=4)
    assert simclock.sim_to_real(simclock.real_to_sim(real_dt)) == real_dt


def test_five_real_minutes_is_one_simulated_day() -> None:
    real_dt = _ANCHOR + timedelta(minutes=5)
    sim_dt = simclock.real_to_sim(real_dt)
    assert sim_dt - _EPOCH_SIM == timedelta(days=1)


def test_two_real_seconds_is_9_6_simulated_minutes() -> None:
    real_dt = _ANCHOR + timedelta(seconds=2)
    sim_dt = simclock.real_to_sim(real_dt)
    assert sim_dt - _EPOCH_SIM == timedelta(minutes=9.6)


def test_real_duration_to_sim_matches_the_288_scale_factor() -> None:
    assert simclock.real_duration_to_sim(timedelta(seconds=1)) == timedelta(seconds=288)


def test_sim_duration_to_real_matches_the_288_scale_factor() -> None:
    assert simclock.sim_duration_to_real(timedelta(seconds=288)) == timedelta(seconds=1)


def test_sim_date_of_before_simulated_midnight() -> None:
    just_before_midnight = _EPOCH_SIM + timedelta(hours=23, minutes=59, seconds=59)
    assert simclock.sim_date_of(just_before_midnight) == _EPOCH_SIM.date()


def test_sim_date_of_after_simulated_midnight() -> None:
    just_after_midnight = _EPOCH_SIM + timedelta(days=1, seconds=1)
    assert simclock.sim_date_of(just_after_midnight) == (_EPOCH_SIM + timedelta(days=1)).date()


def test_sim_hour_of() -> None:
    fourteen_thirty = _EPOCH_SIM + timedelta(hours=14, minutes=30)
    assert simclock.sim_hour_of(fourteen_thirty) == 14


def test_sim_now_is_close_to_epoch_immediately_after_it() -> None:
    now = simclock.sim_now()
    assert now.tzinfo is not None
    assert now >= _EPOCH_SIM


def test_all_returned_timestamps_are_tz_aware() -> None:
    real_dt = _ANCHOR + timedelta(minutes=1)
    assert simclock.real_to_sim(real_dt).tzinfo is not None
    assert simclock.sim_to_real(real_dt).tzinfo is not None
    assert simclock.sim_now().tzinfo is not None


def test_unanchored_run_starts_at_the_simulated_epoch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no anchor configured, sim_now() sits just after epoch_sim - whatever today is.

    This is the regression test for the two-anchors-in-one-field bug. When epoch_sim was
    also the real anchor, sim_now() drifted by (real_now - epoch_sim) * 288: a config
    epoch 265 real days old produced simulated dates in the year 2235, which then went
    into partition paths and serving-table keys. The old tests missed it because they
    only ever measured from an instant adjacent to the epoch.
    """
    monkeypatch.delenv("VOLTSTREAM__SIMULATION__ANCHOR_REAL", raising=False)
    monkeypatch.setenv("VOLTSTREAM__SIMULATION__EPOCH_SIM", _EPOCH_SIM.isoformat())
    get_config.cache_clear()

    drift = simclock.sim_now() - _EPOCH_SIM
    # Process start is captured at import, so the gap is however long this test session
    # has been running, scaled. Generous, but nothing like the 76,000 days of the bug.
    assert timedelta(0) <= drift < timedelta(days=30), (
        f"sim_now() is {drift.days} simulated days past the epoch with no anchor set; "
        "the real anchor has leaked back into epoch_sim."
    )


def test_a_shared_anchor_makes_two_processes_agree() -> None:
    """Two readers with the same anchor map the same real instant to the same sim instant.

    This is why anchor_real is configurable at all: without it each process would anchor
    on its own start time and they would disagree by their startup skew times 288.
    """
    real_dt = _ANCHOR + timedelta(seconds=37)
    assert simclock.real_to_sim(real_dt) == simclock.real_to_sim(real_dt)
    assert simclock.real_to_sim(real_dt) - _EPOCH_SIM == timedelta(seconds=37) * 288
