"""Unit tests for the reconciliation arithmetic (T141, D4, D5).

No Postgres and no MinIO: the functions under test take the rows the job reads and return
the rows it writes. Every expected figure is worked by hand in the comment beside it —
computing expectations with `core/tariff.py` would test the module against itself. The
one exception is the property test, which uses `core/tariff.py` only to *generate*
realistic inputs; the property it checks, the D4 identity, does not depend on those values
being right.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from voltstream.batch.reconciliation import (
    DayReconciliation,
    MissingTariffError,
    parse_tariff_csv,
    pct_divergence,
    reconcile_day,
    reconcile_household,
    summarise,
)
from voltstream.contracts.reference import TariffRecord
from voltstream.core.netting import net
from voltstream.core.tariff import BlockBoundary, TariffRates, compute_bill
from voltstream.storage.repositories import BatchBill, RunningEstimate

_SIM_DATE = date(2026, 1, 2)
_YESTERDAY = date(2026, 1, 1)
_BOUNDARIES = [
    BlockBoundary("block_1", Decimal(60)),
    BlockBoundary("block_2", Decimal(120)),
    BlockBoundary("block_3", None),
]


def _speed(
    estimate: str, consumption: str, solar: str = "0.0000", household_id: str = "HH-0001"
) -> RunningEstimate:
    """A speed-layer row. Reconciliation reads only the kWh and the total; the other
    components are filler of the right type."""
    zero = Decimal("0.00")
    return RunningEstimate(
        household_id=household_id,
        sim_date=_SIM_DATE,
        consumption_kwh=Decimal(consumption),
        solar_kwh=Decimal(solar),
        self_consumed_kwh=Decimal(0),
        billable_import_kwh=Decimal(0),
        export_kwh=Decimal(0),
        energy_charge=zero,
        fixed_charge=zero,
        subsidy_discount=zero,
        export_credit=zero,
        tier_breakdown=[],
        estimated_bill=Decimal(estimate),
        tariff_source_date=_YESTERDAY,
    )


def _batch(
    final: str,
    energy: str,
    fixed: str,
    consumption: str,
    solar: str = "0.0000",
    household_id: str = "HH-0001",
) -> BatchBill:
    return BatchBill(
        household_id=household_id,
        sim_date=_SIM_DATE,
        consumption_kwh=Decimal(consumption),
        solar_kwh=Decimal(solar),
        energy_charge=Decimal(energy),
        fixed_charge=Decimal(fixed),
        final_bill=Decimal(final),
    )


def _tariff(
    household_id: str = "HH-0001",
    *,
    block_1: str = "8.00",
    block_2: str = "16.50",
    block_3: str = "24.50",
    fixed: str = "240.00",
    subsidy_flag: bool = False,
    subsidy_pct: str = "25.00",
    export: str = "18.00",
) -> TariffRecord:
    return TariffRecord(
        household_id=household_id,
        effective_date=_SIM_DATE,
        billing_tier="TIER_2",
        subsidy_flag=subsidy_flag,
        subsidy_pct=Decimal(subsidy_pct),
        fixed_charge=Decimal(fixed),
        block_1_rate=Decimal(block_1),
        block_2_rate=Decimal(block_2),
        block_3_rate=Decimal(block_3),
        export_rate=Decimal(export),
    )


# ---------------------------------------------------------------------------
# pct_divergence (D5): base is gross charges, 0 when the base is 0.
# ---------------------------------------------------------------------------


def test_pct_divergence_uses_gross_charges_as_the_base() -> None:
    # 100 * 12.00 / (80.00 + 240.00) = 3.75
    assert pct_divergence(Decimal("12.00"), Decimal("80.00"), Decimal("240.00")) == Decimal("3.750")


def test_pct_divergence_is_zero_when_the_base_is_zero() -> None:
    result = pct_divergence(Decimal("5.00"), Decimal("0.00"), Decimal("0.00"))
    assert result == Decimal("0.000")
    assert isinstance(result, Decimal)


def test_pct_divergence_rounds_half_up_to_three_places() -> None:
    # 100 * 0.01 / 16.00 = 0.0625 exactly: half-up gives 0.063, half-even would give 0.062.
    assert pct_divergence(Decimal("0.01"), Decimal("16.00"), Decimal("0.00")) == Decimal("0.063")


# ---------------------------------------------------------------------------
# reconcile_household: hand-computed decompositions.
# ---------------------------------------------------------------------------


def test_hand_computed_decomposition() -> None:
    # S: 10 kWh at yesterday's block_1 of 7.60 = 76.00, + 240.00 fixed      = 316.00
    # C: the same 10 kWh at today's 8.00       = 80.00, + 240.00 fixed      = 320.00
    # B: the batch layer's 9 kWh at 8.00       = 72.00, + 240.00 fixed      = 312.00
    row = reconcile_household(
        _speed(estimate="316.00", consumption="10.0000"),
        _batch(final="312.00", energy="72.00", fixed="240.00", consumption="9.0000"),
        _tariff(),
        _BOUNDARIES,
    )

    assert row.speed_estimate == Decimal("316.00")
    assert row.batch_final == Decimal("312.00")
    assert row.tariff_effect == Decimal("-4.00")  # S - C
    assert row.data_effect == Decimal("8.00")  # C - B
    assert row.abs_divergence == Decimal("4.00")
    # 100 * 4.00 / (72.00 + 240.00) = 1.28205...
    assert row.pct_divergence == Decimal("1.282")


def test_net_exporter_is_measured_against_gross_charges_not_the_negative_bill() -> None:
    # D5's net-exporter example as the batch bill: 4 kWh used, 19 kWh solar, TIER_1.
    #   B: export 15 * 18.00 = 270.00  ->  0.00 + 120.00 - 270.00           = -150.00
    # The speed layer saw 0.5 kWh more solar and costed it at yesterday's export 17.50.
    #   S: export 15.5 * 17.50 = 271.25  ->  120.00 - 271.25                = -151.25
    #   C: export 15.5 * 18.00 = 279.00  ->  120.00 - 279.00                = -159.00
    row = reconcile_household(
        _speed(estimate="-151.25", consumption="4.0000", solar="19.5000"),
        _batch(
            final="-150.00", energy="0.00", fixed="120.00", consumption="4.0000", solar="19.0000"
        ),
        _tariff(fixed="120.00"),
        _BOUNDARIES,
    )

    assert row.tariff_effect == Decimal("7.75")  # -151.25 - (-159.00)
    assert row.data_effect == Decimal("-9.00")  # -159.00 - (-150.00)
    assert row.abs_divergence == Decimal("1.25")
    # 100 * 1.25 / (0.00 + 120.00) = 1.0416... Against final_bill it would be negative.
    assert row.pct_divergence == Decimal("1.042")


def test_a_rate_change_in_an_unused_block_has_no_tariff_effect() -> None:
    """`tariff_effect` isolates the tariff: a change in a block the household never
    reaches costs it nothing, so the whole divergence is data.

    Yesterday and today differ only in block_2_rate (16.50 -> 17.00); 10 kWh never
    leaves block 1. S = C = 10 * 8.00 + 240.00 = 320.00; B = 9 * 8.00 + 240.00 = 312.00.
    """
    row = reconcile_household(
        _speed(estimate="320.00", consumption="10.0000"),
        _batch(final="312.00", energy="72.00", fixed="240.00", consumption="9.0000"),
        _tariff(block_2="17.00"),
        _BOUNDARIES,
    )

    assert row.tariff_effect == Decimal("0.00")
    assert row.data_effect == Decimal("8.00")


def test_mismatched_rows_are_refused() -> None:
    with pytest.raises(ValueError, match="paired with"):
        reconcile_household(
            _speed(estimate="1.00", consumption="1.0000", household_id="HH-0001"),
            _batch(
                final="1.00",
                energy="1.00",
                fixed="0.00",
                consumption="1.0000",
                household_id="HH-0002",
            ),
            _tariff(),
            _BOUNDARIES,
        )


# ---------------------------------------------------------------------------
# The D4 identity, over generated inputs.
# ---------------------------------------------------------------------------

_kwh = st.decimals(min_value=0, max_value=300, places=4, allow_nan=False, allow_infinity=False)
_rate = st.decimals(min_value=0, max_value=50, places=2, allow_nan=False, allow_infinity=False)
_fixed = st.decimals(min_value=0, max_value=1000, places=2, allow_nan=False, allow_infinity=False)
_pct = st.decimals(min_value=0, max_value=100, places=2, allow_nan=False, allow_infinity=False)


@st.composite
def _tariffs(draw: st.DrawFn) -> TariffRecord:
    return TariffRecord(
        household_id="HH-0001",
        effective_date=_SIM_DATE,
        billing_tier="TIER_1",
        subsidy_flag=draw(st.booleans()),
        subsidy_pct=draw(_pct),
        fixed_charge=draw(_fixed),
        block_1_rate=draw(_rate),
        block_2_rate=draw(_rate),
        block_3_rate=draw(_rate),
        export_rate=draw(_rate),
    )


def _as_rates(record: TariffRecord) -> TariffRates:
    return TariffRates(
        block_1_rate=record.block_1_rate,
        block_2_rate=record.block_2_rate,
        block_3_rate=record.block_3_rate,
        fixed_charge=record.fixed_charge,
        subsidy_flag=record.subsidy_flag,
        subsidy_pct=record.subsidy_pct,
        export_rate=record.export_rate,
    )


@settings(max_examples=500)
@given(
    speed_kwh=_kwh,
    speed_solar=_kwh,
    batch_kwh=_kwh,
    batch_solar=_kwh,
    yesterday=_tariffs(),
    today=_tariffs(),
)
def test_effects_sum_to_the_divergence_on_every_row(
    speed_kwh: Decimal,
    speed_solar: Decimal,
    batch_kwh: Decimal,
    batch_solar: Decimal,
    yesterday: TariffRecord,
    today: TariffRecord,
) -> None:
    s = compute_bill(net(speed_kwh, speed_solar), _as_rates(yesterday), _BOUNDARIES)
    b = compute_bill(net(batch_kwh, batch_solar), _as_rates(today), _BOUNDARIES)

    speed = _speed(str(s.final_bill), str(speed_kwh), str(speed_solar))
    batch = BatchBill(
        household_id="HH-0001",
        sim_date=_SIM_DATE,
        consumption_kwh=batch_kwh,
        solar_kwh=batch_solar,
        energy_charge=b.energy_charge,
        fixed_charge=b.fixed_charge,
        final_bill=b.final_bill,
    )
    row = reconcile_household(speed, batch, today, _BOUNDARIES)

    # D4: exact Decimal equality, not "within a cent".
    assert row.tariff_effect + row.data_effect == row.speed_estimate - row.batch_final
    assert row.abs_divergence == abs(row.speed_estimate - row.batch_final)
    assert row.pct_divergence >= 0
    assert row.pct_divergence.as_tuple().exponent == -3
    for value in row[2:]:
        assert isinstance(value, Decimal)


# ---------------------------------------------------------------------------
# reconcile_day: pairing, loud failures, saturation, kWh totals.
# ---------------------------------------------------------------------------


def test_households_missing_from_one_view_are_reported_not_reconciled() -> None:
    day = reconcile_day(
        [
            _speed("320.00", "10.0000", household_id="HH-0001"),
            _speed("320.00", "10.0000", household_id="HH-0002"),
        ],
        [
            _batch("312.00", "72.00", "240.00", "9.0000", household_id="HH-0001"),
            _batch("312.00", "72.00", "240.00", "9.0000", household_id="HH-0003"),
        ],
        {h: _tariff(h) for h in ("HH-0001", "HH-0002", "HH-0003")},
        _BOUNDARIES,
    )

    assert [r.household_id for r in day.rows] == ["HH-0001"]
    assert day.speed_only == ["HH-0002"]
    assert day.batch_only == ["HH-0003"]
    # Totals cover the reconciled household only.
    assert day.speed_kwh == Decimal("10.0000")
    assert day.batch_kwh == Decimal("9.0000")


def test_a_reconciled_household_without_a_tariff_fails_loudly() -> None:
    with pytest.raises(MissingTariffError, match="HH-0001"):
        reconcile_day(
            [_speed("320.00", "10.0000")],
            [_batch("312.00", "72.00", "240.00", "9.0000")],
            {},
            _BOUNDARIES,
        )


def test_an_unrepresentable_percentage_is_saturated_and_reported() -> None:
    # Zero fixed charge and a cent of energy: base 0.01. The stale estimate is 50.00 off,
    # so the exact figure is 100 * 50.00 / 0.01 = 500000 %, which NUMERIC(6,3) cannot hold.
    # C = 0.01 kWh * 1.00 + 0.00 = 0.01, so tariff 50.00 and data 0.00.
    day = reconcile_day(
        [_speed("50.01", "0.0100")],
        [_batch("0.01", "0.01", "0.00", "0.0100")],
        {"HH-0001": _tariff(block_1="1.00", fixed="0.00")},
        _BOUNDARIES,
    )

    [row] = day.rows
    assert row.pct_divergence == Decimal("999.999")
    assert day.capped == ["HH-0001"]
    # The effects are money, not a percentage, and are never saturated.
    assert row.tariff_effect == Decimal("50.00")
    assert row.data_effect == Decimal("0.00")


# ---------------------------------------------------------------------------
# summarise: the gauge value, the max, and the attribution.
# ---------------------------------------------------------------------------


def test_summary_figures() -> None:
    day = reconcile_day(
        [
            # The two hand-computed cases above, as one day.
            _speed("316.00", "10.0000", household_id="HH-0001"),
            _speed("-151.25", "4.0000", "19.5000", household_id="HH-0002"),
        ],
        [
            _batch("312.00", "72.00", "240.00", "9.0000", household_id="HH-0001"),
            _batch("-150.00", "0.00", "120.00", "4.0000", "19.0000", household_id="HH-0002"),
        ],
        {"HH-0001": _tariff("HH-0001"), "HH-0002": _tariff("HH-0002", fixed="120.00")},
        _BOUNDARIES,
    )
    summary = summarise(day)

    assert summary.households == 2
    assert summary.mean_pct_divergence == Decimal("1.162")  # (1.282 + 1.042) / 2
    assert summary.max_pct_divergence == Decimal("1.282")
    assert summary.mean_tariff_effect == Decimal("1.88")  # (-4.00 + 7.75) / 2 = 1.875
    assert summary.mean_data_effect == Decimal("-0.50")  # (8.00 - 9.00) / 2
    # |tariff| / (|tariff| + |data|) = (4.00 + 7.75) / (4.00 + 7.75 + 8.00 + 9.00)
    assert summary.tariff_share_pct == Decimal("40.870")
    # Speed saw 14 kWh, batch 13: the speed layer saw *more*, so the shortfall is negative.
    assert summary.speed_kwh == Decimal("14.0000")
    assert summary.batch_kwh == Decimal("13.0000")
    assert summary.speed_kwh_shortfall_pct == Decimal("-7.692")  # 100 * (13 - 14) / 13


def test_summarising_an_empty_day_is_an_error() -> None:
    empty = DayReconciliation([], [], [], [], Decimal(0), Decimal(0))
    with pytest.raises(ValueError, match="nothing was reconciled"):
        summarise(empty)


# ---------------------------------------------------------------------------
# parse_tariff_csv: validated, effective-dated.
# ---------------------------------------------------------------------------

_HEADER = (
    "household_id,effective_date,billing_tier,subsidy_flag,subsidy_pct,fixed_charge,"
    "block_1_rate,block_2_rate,block_3_rate,export_rate"
)


def _csv(*rows: str) -> bytes:
    return "\n".join([_HEADER, *rows]).encode("utf-8")


def test_the_contract_sample_row_parses() -> None:
    # The D2 sample row, verbatim.
    tariffs = parse_tariff_csv(
        _csv("HH-0042,2026-08-10,TIER_2,false,25.0,240.00,8.00,16.50,24.50,18.00"),
        date(2026, 8, 10),
    )
    record = tariffs["HH-0042"]
    assert record.block_2_rate == Decimal("16.50")
    assert record.subsidy_flag is False


def test_the_latest_applicable_row_wins() -> None:
    tariffs = parse_tariff_csv(
        _csv(
            "HH-0001,2026-01-01,TIER_1,false,25.0,100.00,8.00,16.50,24.50,18.00",
            "HH-0001,2026-01-02,TIER_1,false,25.0,120.00,8.00,16.50,24.50,18.00",
            # After the day being reconciled: must not win.
            "HH-0001,2026-01-03,TIER_1,false,25.0,999.00,8.00,16.50,24.50,18.00",
        ),
        _SIM_DATE,
    )
    assert tariffs["HH-0001"].fixed_charge == Decimal("120.00")


def test_an_invalid_row_fails_naming_its_line() -> None:
    with pytest.raises(ValueError, match="line 3"):
        parse_tariff_csv(
            _csv(
                "HH-0001,2026-01-02,TIER_1,false,25.0,120.00,8.00,16.50,24.50,18.00",
                "HH-0002,2026-01-02,TIER_1,false,25.0,120.00,8.00,-1.00,24.50,18.00",
            ),
            _SIM_DATE,
        )


def test_two_rows_tied_on_the_applicable_date_are_an_error() -> None:
    with pytest.raises(ValueError, match="second row for HH-0001"):
        parse_tariff_csv(
            _csv(
                "HH-0001,2026-01-02,TIER_1,false,25.0,120.00,8.00,16.50,24.50,18.00",
                "HH-0001,2026-01-02,TIER_1,false,25.0,130.00,8.00,16.50,24.50,18.00",
            ),
            _SIM_DATE,
        )
