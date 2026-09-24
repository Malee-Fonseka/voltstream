"""Repository queries against a real Postgres (T102).

Marked `integration` because these need the schema, and the schema is the thing under
test as much as the SQL is — a query that works against a mock proves nothing about a
partial unique index or a `DISTINCT ON`.

Each test inserts its own fixtures and cleans up after itself, so the suite can run
against a database that already has pipeline data in it without either disturbing that
data or being confused by it. Fixture rows use household and zone ids outside the
simulated ranges for the same reason.

    pytest -m integration tests/integration/test_repositories.py
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from voltstream.storage import postgres, repositories

pytestmark = pytest.mark.integration

# Deliberately outside the simulated ranges (HH-0001..HH-0050, ZONE-A..E) so these rows
# cannot be mistaken for pipeline output, and cannot collide with it.
_HOUSEHOLD = "HH-TEST-01"
_ZONE = "ZONE-TEST"
_SIM_DATE = date(2020, 1, 1)


@pytest.fixture(scope="module", autouse=True)
def _pool() -> Iterator[None]:
    postgres.open_pool()
    yield
    postgres.close_pool()


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    """Remove this module's rows before and after each test."""

    def purge() -> None:
        with postgres.transaction() as cur:
            cur.execute("DELETE FROM zone_metrics_rt WHERE grid_zone = %s", (_ZONE,))
            cur.execute("DELETE FROM household_running_rt WHERE household_id = %s", (_HOUSEHOLD,))
            cur.execute("DELETE FROM pipeline_runs WHERE sim_date = %s", (_SIM_DATE,))

    purge()
    yield
    purge()


def _insert_zone_window(window_start: datetime, consumption: str, solar: str) -> None:
    with postgres.transaction() as cur:
        cur.execute(
            "INSERT INTO zone_metrics_rt (grid_zone, window_start, window_end, "
            "total_consumption_kwh, total_solar_kwh, renewable_ratio, active_meters) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                _ZONE,
                window_start,
                window_start + timedelta(minutes=15),
                Decimal(consumption),
                Decimal(solar),
                Decimal("0.5000"),
                10,
            ),
        )


def _insert_run(status: str, run_id: uuid.UUID | None = None) -> None:
    with postgres.transaction() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (run_id, sim_date, layer, status, started_at) "
            "VALUES (%s, %s, 'batch_billing', %s, now())",
            (run_id or uuid.uuid4(), _SIM_DATE, status),
        )


def test_latest_zone_metrics_returns_one_row_per_zone() -> None:
    """`DISTINCT ON` must take the newest window, not an arbitrary one."""
    base = datetime(2020, 1, 1, 12, 0, tzinfo=UTC)
    _insert_zone_window(base, "10.0000", "5.0000")
    _insert_zone_window(base + timedelta(minutes=15), "20.0000", "6.0000")

    rows = {r.grid_zone: r for r in repositories.get_latest_zone_metrics()}
    assert _ZONE in rows
    assert rows[_ZONE].total_consumption_kwh == Decimal("20.0000")
    assert rows[_ZONE].window_start == base + timedelta(minutes=15)


def test_zone_metrics_range_is_ordered_and_bounded() -> None:
    base = datetime(2020, 1, 1, 12, 0, tzinfo=UTC)
    for i in range(4):
        _insert_zone_window(base + timedelta(minutes=15 * i), f"{10 + i}.0000", "1.0000")

    rows = repositories.get_zone_metrics_range(
        _ZONE, base + timedelta(minutes=15), base + timedelta(minutes=45)
    )
    # Half-open: includes 12:15 and 12:30, excludes 12:45.
    assert [r.window_start for r in rows] == [
        base + timedelta(minutes=15),
        base + timedelta(minutes=30),
    ]


def test_running_estimate_round_trips_every_component() -> None:
    with postgres.transaction() as cur:
        cur.execute(
            "INSERT INTO household_running_rt (household_id, sim_date, consumption_kwh, "
            "solar_kwh, self_consumed_kwh, billable_import_kwh, export_kwh, energy_charge, "
            "fixed_charge, subsidy_discount, export_credit, tier_breakdown, estimated_bill, "
            "tariff_source_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                _HOUSEHOLD,
                _SIM_DATE,
                Decimal("100.0000"),
                Decimal("20.0000"),
                Decimal("20.0000"),
                Decimal("80.0000"),
                Decimal("0.0000"),
                Decimal("640.00"),
                Decimal("240.00"),
                Decimal("0.00"),
                Decimal("0.00"),
                json.dumps([{"name": "block_1", "kwh": "60.0000"}]),
                Decimal("880.00"),
                _SIM_DATE - timedelta(days=1),
            ),
        )

    row = repositories.get_running_estimate(_HOUSEHOLD, _SIM_DATE)
    assert row is not None
    assert row.estimated_bill == Decimal("880.00")
    assert row.tariff_source_date == _SIM_DATE - timedelta(days=1)
    assert row.tier_breakdown is not None


def test_running_estimate_returns_none_when_absent() -> None:
    """None, never an exception and never a bare empty tuple."""
    assert repositories.get_running_estimate(_HOUSEHOLD, _SIM_DATE) is None


def test_day_is_not_finalised_without_a_run() -> None:
    assert repositories.is_day_finalised(_SIM_DATE) is False


def test_day_is_finalised_on_a_success_row() -> None:
    _insert_run("success")
    assert repositories.is_day_finalised(_SIM_DATE) is True


def test_a_failed_run_does_not_finalise_the_day() -> None:
    _insert_run("failed")
    assert repositories.is_day_finalised(_SIM_DATE) is False


def test_superseded_alongside_success_still_finalises_once() -> None:
    """T040's trap: a restated day holds several rows for the same (sim_date, layer).

    The first run is marked `superseded` when the day is rebuilt, and a fresh `success`
    row is inserted. A query matching on "a row exists" would see two rows and, depending
    how it is written, either double-count or return the withdrawn one. The partial
    unique index permits at most one `success`, which is what makes the existence check
    on `status = 'success'` safe.
    """
    _insert_run("superseded")
    _insert_run("superseded")
    _insert_run("success")

    assert repositories.is_day_finalised(_SIM_DATE) is True

    with postgres.transaction() as cur:
        cur.execute(
            "SELECT count(*) FROM pipeline_runs WHERE sim_date = %s AND layer = 'batch_billing'",
            (_SIM_DATE,),
        )
        assert cur.fetchone()[0] == 3, "fixture should leave three rows for one day"


def test_the_partial_index_forbids_two_success_rows() -> None:
    """The guarantee `is_day_finalised` relies on, asserted rather than assumed."""
    _insert_run("success")
    with pytest.raises(Exception, match="(?i)unique|duplicate"):
        _insert_run("success")


def test_rejected_summary_groups_by_reason() -> None:
    """Returns typed rows; `total` rather than `count`, which would shadow tuple.count."""
    rows = repositories.get_rejected_summary(minutes=60)
    assert all(isinstance(r.reason, str) and isinstance(r.total, int) for r in rows)
