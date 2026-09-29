"""Unit tests for the reference dropper's daily tariff (T074, R01, R03).

No object store: `_tariff_rows` and `closed_days_owed` are the pure parts of the dropper.
The first is the rule the speed layer's stale-tariff divergence depends on; the second
decides which closed days get a tariff when the dropper starts.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from voltstream.config import get_config
from voltstream.contracts.reference import TariffRecord
from voltstream.core.netting import net
from voltstream.core.tariff import TariffRates, compute_bill
from voltstream.simulators.households import household_roster
from voltstream.simulators.profiles import consumption_kwh, solar_kwh
from voltstream.simulators.reference_dropper import _tariff_rows, closed_days_owed

_DAY = date(2026, 1, 2)
_YESTERDAY = _DAY - timedelta(days=1)


def _rows(day: date) -> dict[str, TariffRecord]:
    config = get_config()
    roster = household_roster(config.simulation.households, config.simulation.zones)
    return {r.household_id: r for r in _tariff_rows(day, roster, config.tariff.generator_defaults)}


def _rates(record: TariffRecord) -> TariffRates:
    return TariffRates(
        block_1_rate=record.block_1_rate,
        block_2_rate=record.block_2_rate,
        block_3_rate=record.block_3_rate,
        fixed_charge=record.fixed_charge,
        subsidy_flag=record.subsidy_flag,
        subsidy_pct=record.subsidy_pct,
        export_rate=record.export_rate,
    )


def test_consecutive_days_differ_in_block_1_rate_and_nothing_else() -> None:
    """T074's Done-when: two tariff files a day apart differ in the documented column only
    (and in effective_date, which is the day itself)."""
    today, yesterday = _rows(_DAY), _rows(_YESTERDAY)
    assert today.keys() == yesterday.keys()

    for household_id, t in today.items():
        y = yesterday[household_id]
        assert abs(t.block_1_rate - y.block_1_rate) == Decimal("0.50"), household_id
        changed = {
            field for field in TariffRecord.model_fields if getattr(t, field) != getattr(y, field)
        }
        assert changed == {"block_1_rate", "effective_date"}, household_id


def test_the_step_is_on_even_day_ordinals() -> None:
    """The documented rule, exactly: 0.50 above the default on even ordinals."""
    base = Decimal(str(get_config().tariff.generator_defaults.block_rates["block_1"]))
    for day in (_YESTERDAY, _DAY):
        expected = base + (Decimal("0.50") if day.toordinal() % 2 == 0 else Decimal(0))
        assert {r.block_1_rate for r in _rows(day).values()} == {expected}, day


def test_yesterdays_tariff_prices_every_simulated_household_differently() -> None:
    """R01: the stale tariff must move every provisional bill.

    Uses the producer's real consumption and solar curves for one full simulated day.
    Before R01 the day-over-day change was in block 2, which starts at 60 kWh; no
    household reaches it, so S == C and `tariff_effect` was 0.00 for all 50.
    """
    config = get_config()
    boundaries = config.tariff.boundaries()
    roster = household_roster(config.simulation.households, config.simulation.zones)
    today, yesterday = _rows(_DAY), _rows(_YESTERDAY)

    start = datetime(_DAY.year, _DAY.month, _DAY.day, tzinfo=UTC)
    ticks = 150  # 300 real s per simulated day / 2 s per tick
    step = timedelta(days=1) / ticks

    unchanged = []
    for household in roster:
        hid = household.household_id
        consumption = sum(
            (consumption_kwh(hid, start + i * step) for i in range(ticks)), Decimal(0)
        )
        solar = sum(
            (solar_kwh(hid, start + i * step, has_solar=household.has_solar) for i in range(ticks)),
            Decimal(0),
        )
        netting = net(consumption, solar)
        stale = compute_bill(netting, _rates(yesterday[hid]), boundaries).final_bill
        current = compute_bill(netting, _rates(today[hid]), boundaries).final_bill
        if stale == current:
            unchanged.append(hid)

    assert unchanged == [], f"yesterday's tariff prices these exactly like today's: {unchanged}"


# ---------------------------------------------------------------------------------------
# R03: tariff_D is written when D closes, so startup owes every day closed since the last.
# ---------------------------------------------------------------------------------------


def test_a_fresh_stack_is_owed_only_yesterdays_tariff() -> None:
    """The T075 seed: the speed layer costs day one against it."""
    assert closed_days_owed(set(), _DAY) == [_YESTERDAY]


def test_a_restart_the_same_day_owes_nothing() -> None:
    assert closed_days_owed({_YESTERDAY - timedelta(days=1), _YESTERDAY}, _DAY) == []


def test_a_restart_after_a_stop_owes_every_day_that_closed_meanwhile() -> None:
    """Stopped during day 5 and started on day 8: days 5, 6 and 7 all closed unbilled."""
    day5 = date(2026, 1, 5)
    owed = closed_days_owed({date(2026, 1, 3), date(2026, 1, 4)}, date(2026, 1, 8))
    assert owed == [day5, day5 + timedelta(days=1), day5 + timedelta(days=2)]


def test_todays_tariff_is_never_owed() -> None:
    """Today has not closed; a file for it would get it billed while it is still running."""
    assert _DAY not in closed_days_owed(set(), _DAY)
    assert closed_days_owed({_DAY}, _DAY) == [_YESTERDAY], "a file dated today is not a closed day"


def test_catching_up_stops_at_a_week() -> None:
    owed = closed_days_owed({date(2025, 1, 1)}, _DAY)
    assert owed[0] == _DAY - timedelta(days=7)
    assert owed[-1] == _YESTERDAY
    assert len(owed) == 7
