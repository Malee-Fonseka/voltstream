"""Unit tests for the two listings the dashboard polls: households, and every zone's history.

A minimal app with the real routers; the repository reads are replaced, so no Postgres is
needed. What is pinned is the shape the dashboard builds on — the household dimension as
the database holds it, and the history grouped by zone, anchored to simulated now.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from voltstream.api.routers import households, zones
from voltstream.storage import repositories
from voltstream.storage.repositories import Household, ZoneMetric

app = FastAPI()
app.include_router(zones.router)
app.include_router(households.router)
client = TestClient(app)

_NOW = datetime(2026, 1, 3, 12, 0, tzinfo=UTC)


def _window(zone: str, minutes_ago: int, consumption: str) -> ZoneMetric:
    start = _NOW - timedelta(minutes=minutes_ago)
    return ZoneMetric(
        grid_zone=zone,
        window_start=start,
        window_end=start + timedelta(minutes=15),
        total_consumption_kwh=Decimal(consumption),
        total_solar_kwh=Decimal("1.0000"),
        renewable_ratio=Decimal("0.2500"),
        active_meters=10,
    )


def test_households_are_listed_as_the_database_holds_them(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        Household("HH-0001", "MTR-0001", "ZONE-A", "TIER_1", False, False),
        Household("HH-0003", "MTR-0003", "ZONE-C", "TIER_3", False, True),
    ]
    monkeypatch.setattr(repositories, "list_households", lambda: rows)

    body = client.get("/api/v1/households").json()

    assert [h["household_id"] for h in body] == ["HH-0001", "HH-0003"]
    assert body[1] == {
        "household_id": "HH-0003",
        "meter_id": "MTR-0003",
        "grid_zone": "ZONE-C",
        "billing_tier": "TIER_3",
        "subsidy_flag": False,
        "has_solar": True,
    }


def test_history_is_grouped_by_zone_and_anchored_to_simulated_now(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asked: list[tuple[datetime, datetime]] = []

    def fake_range(window_from: datetime, window_to: datetime) -> list[ZoneMetric]:
        asked.append((window_from, window_to))
        return [
            _window("ZONE-A", 30, "10.0000"),
            _window("ZONE-A", 15, "11.0000"),
            _window("ZONE-B", 15, "20.0000"),
        ]

    monkeypatch.setattr(zones, "sim_now", lambda: _NOW)
    monkeypatch.setattr(repositories, "get_all_zone_metrics_range", fake_range)

    body = client.get("/api/v1/zones/history?minutes=120").json()

    assert asked == [(_NOW - timedelta(minutes=120), _NOW)]
    assert [z["grid_zone"] for z in body] == ["ZONE-A", "ZONE-B"]
    assert [Decimal(w["total_consumption_kwh"]) for w in body[0]["windows"]] == [
        Decimal("10.0000"),
        Decimal("11.0000"),
    ]


def test_no_windows_in_range_is_an_empty_list_not_a_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unlike the per-zone endpoint: "nothing yet" is an answer for every zone at once."""
    monkeypatch.setattr(zones, "sim_now", lambda: _NOW)
    monkeypatch.setattr(repositories, "get_all_zone_metrics_range", lambda a, b: [])

    response = client.get("/api/v1/zones/history")

    assert response.status_code == 200
    assert response.json() == []


def test_history_range_is_capped_at_a_simulated_day() -> None:
    assert client.get("/api/v1/zones/history?minutes=1441").status_code == 422
