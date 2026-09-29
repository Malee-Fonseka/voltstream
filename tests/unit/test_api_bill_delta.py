"""Unit tests for GET /api/v1/households/{id}/bill/delta (T129, R09).

A minimal app with the real router; the three repository reads are replaced, so no
Postgres is needed. What is pinned is D4's direction (speed minus batch, so the effects
add up to the delta) and D5's percentage base (gross charges, never the final bill).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from voltstream.api.routers import households
from voltstream.storage import repositories
from voltstream.storage.repositories import ReconciliationEffects

app = FastAPI()
app.include_router(households.router)
client = TestClient(app)

_DAY = date(2026, 1, 2)
_URL = f"/api/v1/households/HH-0001/bill/delta?date={_DAY.isoformat()}"


def _serve(
    monkeypatch: pytest.MonkeyPatch,
    *,
    estimate: str | None,
    final: str | None,
    energy: str = "114.21",
    fixed: str = "120.00",
    effects: ReconciliationEffects | None = None,
) -> None:
    speed = SimpleNamespace(estimated_bill=Decimal(estimate)) if estimate else None
    batch: dict[str, Any] | None = (
        {
            "final_bill": Decimal(final),
            "energy_charge": Decimal(energy),
            "fixed_charge": Decimal(fixed),
        }
        if final
        else None
    )
    monkeypatch.setattr(repositories, "get_running_estimate", lambda hh, d: speed)
    monkeypatch.setattr(repositories, "get_finalised_bill_row", lambda hh, d: batch)
    monkeypatch.setattr(repositories, "get_reconciliation_effects", lambda hh, d: effects)


def test_delta_is_speed_minus_batch_and_equals_the_two_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HH-0001 on 2026-01-02 in the Gate 4 run: estimate 227.49, final 234.21, the whole
    difference being the tariff step (-6.72) and none of it data."""
    effects = ReconciliationEffects(
        tariff_effect=Decimal("-6.72"),
        data_effect=Decimal("0.00"),
        abs_divergence=Decimal("6.72"),
        pct_divergence=Decimal("2.869"),
    )
    _serve(monkeypatch, estimate="227.49", final="234.21", effects=effects)

    body = client.get(_URL).json()

    assert Decimal(body["delta"]) == Decimal("-6.72")
    assert Decimal(body["delta"]) == Decimal(body["tariff_effect"]) + Decimal(body["data_effect"])
    assert body["reconciled"] is True


def test_delta_pct_is_reconciliations_figure(monkeypatch: pytest.MonkeyPatch) -> None:
    """|delta| over energy + fixed charges, three places: 6.72 / 234.21 = 2.869 %."""
    _serve(monkeypatch, estimate="227.49", final="234.21")

    assert Decimal(client.get(_URL).json()["delta_pct"]) == Decimal("2.869")


def test_a_net_exporter_does_not_explode_the_percentage(monkeypatch: pytest.MonkeyPatch) -> None:
    """A final bill of -0.40 would give a percentage in the thousands if it were the base."""
    _serve(monkeypatch, estimate="1.60", final="-0.40", energy="0.00", fixed="120.00")

    body = client.get(_URL).json()

    assert Decimal(body["delta"]) == Decimal("2.00")
    assert Decimal(body["delta_pct"]) == Decimal("1.667")


def test_an_open_day_has_an_estimate_and_no_delta(monkeypatch: pytest.MonkeyPatch) -> None:
    _serve(monkeypatch, estimate="200.53", final=None)

    body = client.get(_URL).json()

    assert Decimal(body["speed_estimate"]) == Decimal("200.53")
    assert body["batch_final"] is None
    assert body["delta"] is None
    assert body["delta_pct"] is None
    assert body["reconciled"] is False


def test_neither_figure_is_a_404(monkeypatch: pytest.MonkeyPatch) -> None:
    _serve(monkeypatch, estimate=None, final=None)
    assert client.get(_URL).status_code == 404
