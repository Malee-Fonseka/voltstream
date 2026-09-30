"""Unit tests for the meter producer's tick grid (R37) and its stall exit (R39).

The grid is what makes "150 readings per meter per simulated day" true: tick k is due at
`anchor + k × interval` and stamped with its own simulated time, however late it runs.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta

from voltstream import simclock
from voltstream.config import get_config
from voltstream.simulators.meter_producer import (
    catch_up,
    delivery_stalled,
    first_tick_index,
    tick_due,
)

_ANCHOR = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)
_INTERVAL = 2.0


def test_every_simulated_day_has_exactly_150_ticks() -> None:
    config = get_config()
    anchor = simclock.sim_to_real(config.simulation.epoch_sim)
    interval = config.simulation.emit_interval_seconds

    stamps = [simclock.real_to_sim(tick_due(anchor, interval, k)) for k in range(450)]
    per_day = Counter(simclock.sim_date_of(ts) for ts in stamps)

    assert set(per_day.values()) == {150}, per_day
    assert stamps[1] - stamps[0] == timedelta(minutes=9.6), "one tick is 9.6 simulated minutes"


def test_the_first_tick_is_the_next_grid_point() -> None:
    assert first_tick_index(_ANCHOR, _ANCHOR, _INTERVAL) == 0
    assert first_tick_index(_ANCHOR, _ANCHOR + timedelta(seconds=0.5), _INTERVAL) == 1
    assert first_tick_index(_ANCHOR, _ANCHOR + timedelta(seconds=4), _INTERVAL) == 2
    assert first_tick_index(_ANCHOR, _ANCHOR - timedelta(seconds=9), _INTERVAL) == 0


def test_a_late_tick_is_run_not_skipped() -> None:
    """A 4.8 s stall used to push every later tick back for good."""
    now = tick_due(_ANCHOR, _INTERVAL, 10) + timedelta(seconds=4.8)
    assert catch_up(10, _ANCHOR, now, _INTERVAL) == (10, 0)


def test_on_time_ticks_are_left_alone() -> None:
    now = tick_due(_ANCHOR, _INTERVAL, 10) - timedelta(seconds=1)
    assert catch_up(10, _ANCHOR, now, _INTERVAL) == (10, 0)


def test_far_behind_it_skips_to_now_and_says_how_many() -> None:
    """A host that slept for a minute should not replay a minute of readings at once."""
    now = tick_due(_ANCHOR, _INTERVAL, 10) + timedelta(seconds=60)
    index, skipped = catch_up(10, _ANCHOR, now, _INTERVAL)
    assert index == 40
    assert skipped == 30


# ---------------------------------------------------------------------------------------
# R39: a producer whose deliveries have stopped exits, so compose restarts it.
# ---------------------------------------------------------------------------------------


def test_a_minute_without_a_delivery_while_messages_wait_is_a_stall() -> None:
    assert delivery_stalled(queued=6000, last_delivered=0.0, now=61.0)


def test_a_slow_minute_is_not_a_stall() -> None:
    assert not delivery_stalled(queued=6000, last_delivered=0.0, now=59.0)


def test_an_empty_queue_is_never_a_stall() -> None:
    """Nothing to send is not a failure to send, however long since the last delivery."""
    assert not delivery_stalled(queued=0, last_delivered=0.0, now=3600.0)
