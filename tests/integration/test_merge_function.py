"""The merge function's five cases (T128).

`query = merge(batch_view, realtime_view)` is one `if`, and every interesting failure of
Lambda hides inside it. These tests exist because the cheap version of this logic — "is
there a row in `household_bill_daily`?" — is wrong in two ways that only show up on a day
that failed or was restated, which is exactly when a wrong bill is most expensive.

Integration rather than unit: what is under test is the interaction between the merge and
the run ledger, including a partial unique index. A mocked repository would return
whatever the test told it to and prove nothing about either.

    pytest -m integration tests/integration/test_merge_function.py
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from datetime import date
from decimal import Decimal

import pytest

from voltstream.storage import postgres, repositories

pytestmark = pytest.mark.integration

# Outside the simulated ranges so these rows cannot collide with pipeline output.
_HOUSEHOLD = "HH-MERGE-01"
_SIM_DATE = date(2020, 6, 1)
_TARIFF_DATE = date(2020, 5, 31)


@pytest.fixture(scope="module", autouse=True)
def _pool() -> Iterator[None]:
    postgres.open_pool()
    yield
    postgres.close_pool()


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    def purge() -> None:
        with postgres.transaction() as cur:
            cur.execute("DELETE FROM household_running_rt WHERE household_id = %s", (_HOUSEHOLD,))
            cur.execute("DELETE FROM household_bill_daily WHERE household_id = %s", (_HOUSEHOLD,))
            cur.execute("DELETE FROM pipeline_runs WHERE sim_date = %s", (_SIM_DATE,))

    purge()
    yield
    purge()


def _insert_speed_row(estimate: str = "100.00") -> None:
    with postgres.transaction() as cur:
        cur.execute(
            "INSERT INTO household_running_rt (household_id, sim_date, consumption_kwh, "
            "solar_kwh, self_consumed_kwh, billable_import_kwh, export_kwh, energy_charge, "
            "fixed_charge, subsidy_discount, export_credit, tier_breakdown, estimated_bill, "
            "tariff_source_date) VALUES (%s, %s, 50, 5, 5, 45, 0, %s, 0, 0, 0, %s, %s, %s)",
            (
                _HOUSEHOLD,
                _SIM_DATE,
                Decimal(estimate),
                json.dumps([{"name": "block_1"}]),
                Decimal(estimate),
                _TARIFF_DATE,
            ),
        )


def _insert_batch_row(final: str = "111.11") -> uuid.UUID:
    run_id = uuid.uuid4()
    with postgres.transaction() as cur:
        cur.execute(
            "INSERT INTO household_bill_daily (household_id, sim_date, consumption_kwh, "
            "solar_kwh, self_consumed_kwh, billable_import_kwh, export_kwh, energy_charge, "
            "fixed_charge, subsidy_discount, export_credit, final_bill, tier_breakdown, "
            "tariff_effective_date, readings_count, duplicates_removed, pipeline_run_id) "
            "VALUES (%s, %s, 50, 5, 5, 45, 0, %s, 0, 0, 0, %s, %s, %s, 144, 2, %s)",
            (
                _HOUSEHOLD,
                _SIM_DATE,
                Decimal(final),
                Decimal(final),
                json.dumps([{"name": "block_1"}]),
                _SIM_DATE,
                run_id,
            ),
        )
    return run_id


def _insert_run(status: str) -> None:
    with postgres.transaction() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (run_id, sim_date, layer, status, started_at) "
            "VALUES (%s, %s, 'batch_billing', %s, now())",
            (uuid.uuid4(), _SIM_DATE, status),
        )


def _merge_source() -> str | None:
    """The merge decision, exercised through the same calls the router makes."""
    if repositories.is_day_finalised(_SIM_DATE):
        if repositories.get_finalised_bill_row(_HOUSEHOLD, _SIM_DATE) is not None:
            return "batch"
    if repositories.get_running_estimate(_HOUSEHOLD, _SIM_DATE) is not None:
        return "speed"
    return None


def test_speed_row_only_serves_the_estimate() -> None:
    """The ordinary case during a simulated day: no batch run yet."""
    _insert_speed_row()
    assert _merge_source() == "speed"


def test_a_finalised_day_serves_the_batch_row() -> None:
    """Once the day closes the authoritative figure wins, unconditionally."""
    _insert_speed_row()
    _insert_batch_row()
    _insert_run("success")
    assert _merge_source() == "batch"


def test_a_failed_run_does_not_flip_to_batch() -> None:
    """The case the cheap implementation gets wrong.

    A failed billing run can still leave `household_bill_daily` rows behind — from an
    earlier attempt, or from a restatement that failed partway. Merging on "a bill row
    exists" would serve those as authoritative. The day is only finalised when the ledger
    says a run *succeeded*.
    """
    _insert_speed_row()
    _insert_batch_row()
    _insert_run("failed")
    assert _merge_source() == "speed", "a failed run must not finalise the day"


def test_superseded_plus_success_still_flips_to_batch() -> None:
    """The other case it gets wrong — T040's restatement trap.

    A restated day holds several ledger rows for one `(sim_date, layer)`: the old runs
    marked `superseded` and the new one `success`. A query that counted rows, or took the
    first, would either double-count or read the withdrawn one. Exactly one `success` is
    permitted by the partial unique index, which is what makes the existence check safe.
    """
    _insert_speed_row()
    _insert_batch_row()
    _insert_run("superseded")
    _insert_run("superseded")
    _insert_run("success")
    assert _merge_source() == "batch"


def test_neither_view_has_the_day() -> None:
    """Nothing to serve, so the router raises 404 rather than inventing zeros."""
    assert _merge_source() is None
