"""Unit tests for GET /api/v1/clock.

A minimal app with the real router. Both clock anchors are pinned, as in test_simclock,
and the wall clock is fixed, so every figure below is exact: 3 min 45 s of real time
after the anchor is 18 simulated hours into the first simulated day.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from voltstream import simclock
from voltstream.api.routers import clock
from voltstream.config import get_config

app = FastAPI()
app.include_router(clock.router)
client = TestClient(app)

_EPOCH_SIM = datetime(2026, 1, 1, tzinfo=UTC)
_ANCHOR = datetime(2026, 9, 23, 6, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _pinned_clock(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("VOLTSTREAM__SIMULATION__EPOCH_SIM", _EPOCH_SIM.isoformat())
    monkeypatch.setenv("VOLTSTREAM__SIMULATION__ANCHOR_REAL", _ANCHOR.isoformat())
    monkeypatch.setattr(simclock, "_real_now", lambda: _ANCHOR + timedelta(minutes=3, seconds=45))
    get_config.cache_clear()
    yield
    get_config.cache_clear()


def test_the_clock_reports_simulated_now_and_its_day() -> None:
    body = client.get("/api/v1/clock").json()

    assert datetime.fromisoformat(body["sim_now"]) == datetime(2026, 1, 1, 18, 0, tzinfo=UTC)
    assert body["sim_date"] == "2026-01-01"
    assert body["time_scale"] == 288


def test_progress_and_time_to_close_describe_the_same_day() -> None:
    """18 of 24 simulated hours gone; the remaining 6 are 75 real seconds at 288x."""
    body = client.get("/api/v1/clock").json()

    assert body["day_progress"] == pytest.approx(0.75)
    assert body["real_seconds_to_day_close"] == pytest.approx(75.0)
