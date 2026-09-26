"""Speed-versus-batch reconciliation (T137, T138, D4, D5).

Runs after each billing run. For every household, how far was the speed layer's
provisional bill from the batch layer's final one, and *why*:

    S = household_running_rt.estimated_bill    speed kWh  x yesterday's tariff  (spark_expr)
    B = household_bill_daily.final_bill        batch kWh  x today's tariff      (spark_expr)
    C = compute_bill(speed kWh, today's tariff)                                 (core/tariff)

    tariff_effect = S - C       what costing against yesterday's tariff did
    data_effect   = C - B       what the two layers seeing different readings did
    S - B         = tariff_effect + data_effect         exactly, in Decimal

`C` differs from `S` in the tariff alone and from `B` in the kWh alone. That is what makes
the split an attribution rather than two numbers that happen to add up, and it holds only
because `core/tariff.py` and `core/spark_expr.py` agree exactly
(`tests/consistency/test_pure_vs_spark.py`). This module is `core/tariff.py`'s one
production caller (D4).

`data_effect` is whatever made the speed layer's kWh differ from the batch layer's: late
readings the speed layer's watermark dropped, and duplicates it counted that the batch
layer removed.

**Plain Python, no SparkSession.** Fifty rows do not need a cluster, and this runs on the
app image, which has no PySpark (D4, D6).

**`pct_divergence` is measured against the batch bill's gross charges**
(`energy_charge + fixed_charge`), not `final_bill` (D5). A net exporter's final bill is
negative or near zero; dividing by it would make the percentage explode and fire
LambdaDivergenceHigh on a household whose estimate was fine.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import time
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import NamedTuple

from pydantic import ValidationError

from voltstream.config import get_config
from voltstream.contracts.reference import TariffRecord
from voltstream.core.money import DIVERGENCE_PCT, round_money
from voltstream.core.netting import net
from voltstream.core.tariff import BlockBoundary, TariffRates, compute_bill
from voltstream.logging_setup import get_logger
from voltstream.metrics import lambda_divergence, push_metrics
from voltstream.storage import objectstore, repositories
from voltstream.storage.repositories import BatchBill, ReconciliationRow, RunningEstimate

_JOB = "reconciliation"

log = get_logger("reconciliation")

_PCT_QUANT = Decimal(1).scaleb(-DIVERGENCE_PCT[1])  # 0.001
# The largest value NUMERIC(6,3) holds: 999.999.
_PCT_MAX = Decimal(10) ** (DIVERGENCE_PCT[0] - DIVERGENCE_PCT[1]) - _PCT_QUANT
_HUNDRED = Decimal(100)


class MissingTariffError(RuntimeError):
    """A household with both a provisional and a final bill has no tariff row today.

    Loud for the same reason as the billing job's error of the same name (T113): without
    today's tariff there is no counterfactual, and a guessed one would put an invented
    number into `tariff_effect`.
    """


class DayReconciliation(NamedTuple):
    """One day reconciled, plus what could not be."""

    rows: list[ReconciliationRow]
    # Households in one view but not the other. Reported, not reconciled: a divergence
    # needs both figures, and inventing the missing one would be worse than a gap.
    speed_only: list[str]
    batch_only: list[str]
    # Households whose pct_divergence exceeded what NUMERIC(6,3) holds and was saturated.
    capped: list[str]
    # Consumption over the reconciled households, as each layer saw it.
    speed_kwh: Decimal
    batch_kwh: Decimal


class DaySummary(NamedTuple):
    households: int
    mean_pct_divergence: Decimal
    max_pct_divergence: Decimal
    mean_tariff_effect: Decimal
    mean_data_effect: Decimal
    # Share of the absolute divergence attributed to the tariff, 0-100. Absolute values,
    # because the two effects can have opposite signs and would otherwise cancel.
    tariff_share_pct: Decimal
    speed_kwh: Decimal
    batch_kwh: Decimal
    # How much of the batch layer's energy the speed layer did not see, as a percentage.
    # Same sign convention as T094's gap: positive means the speed layer saw less.
    speed_kwh_shortfall_pct: Decimal


# --------------------------------------------------------------------------------------
# Today's tariff
# --------------------------------------------------------------------------------------


def parse_tariff_csv(data: bytes, sim_date: date) -> dict[str, TariffRecord]:
    """The day's tariff file as validated records, keyed by household.

    Every row goes through `TariffRecord`, so a hand-edited restatement file with a
    negative rate or a subsidy above 100 % fails here, naming the line, rather than
    producing a counterfactual nobody can explain.

    The effective-dated rule is the billing job's (T113): per household, the latest row
    with `effective_date <= sim_date`. Two rows tied on that date are an error — the
    billing job would have picked one of them arbitrarily, and there is no way to know
    which.
    """
    applicable: dict[str, TariffRecord] = {}
    reader = csv.DictReader(io.StringIO(data.decode("utf-8")))
    for line_no, raw in enumerate(reader, start=2):  # line 1 is the header
        try:
            record = TariffRecord.model_validate(raw)
        except ValidationError as exc:
            raise ValueError(f"tariff file for {sim_date}, line {line_no}: {exc}") from exc

        if record.effective_date > sim_date:
            continue
        current = applicable.get(record.household_id)
        if current is None or record.effective_date > current.effective_date:
            applicable[record.household_id] = record
        elif record.effective_date == current.effective_date:
            raise ValueError(
                f"tariff file for {sim_date}, line {line_no}: a second row for "
                f"{record.household_id} effective {record.effective_date}"
            )
    return applicable


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


# --------------------------------------------------------------------------------------
# The arithmetic (D4, D5)
# --------------------------------------------------------------------------------------


def pct_divergence(
    abs_divergence: Decimal, batch_energy_charge: Decimal, batch_fixed_charge: Decimal
) -> Decimal:
    """`100 * abs_divergence / (energy_charge + fixed_charge)`, 0 when that base is 0 (D5).

    Rounded half-up to the column's three places. The base is reachable at zero only with
    a zero fixed charge and zero consumption.
    """
    base = batch_energy_charge + batch_fixed_charge
    if base == 0:
        return Decimal(0).quantize(_PCT_QUANT)
    return (_HUNDRED * abs_divergence / base).quantize(_PCT_QUANT, rounding=ROUND_HALF_UP)


def reconcile_household(
    speed: RunningEstimate,
    batch: BatchBill,
    todays_tariff: TariffRecord,
    boundaries: list[BlockBoundary],
) -> ReconciliationRow:
    """Decompose one household's divergence into its tariff and data effects.

    `pct_divergence` is returned exact; fitting it to the column is the caller's job
    (`reconcile_day`), so that saturating it is visible rather than silent.
    """
    if (speed.household_id, speed.sim_date) != (batch.household_id, batch.sim_date):
        raise ValueError(
            f"speed row {speed.household_id}/{speed.sim_date} paired with batch row "
            f"{batch.household_id}/{batch.sim_date}"
        )

    s = speed.estimated_bill
    b = batch.final_bill
    c = compute_bill(
        net(speed.consumption_kwh, speed.solar_kwh), _rates(todays_tariff), boundaries
    ).final_bill

    abs_divergence = abs(s - b)
    return ReconciliationRow(
        household_id=speed.household_id,
        sim_date=speed.sim_date,
        speed_estimate=s,
        batch_final=b,
        abs_divergence=abs_divergence,
        pct_divergence=pct_divergence(abs_divergence, batch.energy_charge, batch.fixed_charge),
        tariff_effect=s - c,
        data_effect=c - b,
    )


def reconcile_day(
    speed_rows: list[RunningEstimate],
    batch_rows: list[BatchBill],
    tariffs: dict[str, TariffRecord],
    boundaries: list[BlockBoundary],
) -> DayReconciliation:
    """Reconcile every household present in both views."""
    speed_by_id = {r.household_id: r for r in speed_rows}
    batch_by_id = {r.household_id: r for r in batch_rows}
    matched = sorted(speed_by_id.keys() & batch_by_id.keys())

    missing = [h for h in matched if h not in tariffs]
    if missing:
        raise MissingTariffError(
            f"{len(missing)} household(s) have no tariff row for this day: "
            f"{', '.join(missing[:10])}" + (" ..." if len(missing) > 10 else "")
        )

    rows: list[ReconciliationRow] = []
    capped: list[str] = []
    speed_kwh = Decimal(0)
    batch_kwh = Decimal(0)
    for household_id in matched:
        speed, batch = speed_by_id[household_id], batch_by_id[household_id]
        row = reconcile_household(speed, batch, tariffs[household_id], boundaries)
        if row.pct_divergence > _PCT_MAX:
            # Only reachable with a near-zero gross charge — a zero fixed charge and almost
            # no consumption. Saturate so the day still lands, and report which household.
            capped.append(household_id)
            row = row._replace(pct_divergence=_PCT_MAX)
        rows.append(row)
        speed_kwh += speed.consumption_kwh
        batch_kwh += batch.consumption_kwh

    return DayReconciliation(
        rows=rows,
        speed_only=sorted(speed_by_id.keys() - batch_by_id.keys()),
        batch_only=sorted(batch_by_id.keys() - speed_by_id.keys()),
        capped=capped,
        speed_kwh=speed_kwh,
        batch_kwh=batch_kwh,
    )


def summarise(day: DayReconciliation) -> DaySummary:
    """The day's figures for the gauge (T138), the log line and T140's write-up."""
    rows = day.rows
    if not rows:
        raise ValueError("nothing was reconciled; there is no divergence to summarise")
    n = Decimal(len(rows))

    abs_tariff = sum((abs(r.tariff_effect) for r in rows), Decimal(0))
    abs_data = sum((abs(r.data_effect) for r in rows), Decimal(0))
    attributed = abs_tariff + abs_data
    tariff_share = _HUNDRED * abs_tariff / attributed if attributed else Decimal(0)

    shortfall = (
        _HUNDRED * (day.batch_kwh - day.speed_kwh) / day.batch_kwh if day.batch_kwh else Decimal(0)
    )

    return DaySummary(
        households=len(rows),
        mean_pct_divergence=(sum((r.pct_divergence for r in rows), Decimal(0)) / n).quantize(
            _PCT_QUANT, rounding=ROUND_HALF_UP
        ),
        max_pct_divergence=max(r.pct_divergence for r in rows),
        mean_tariff_effect=round_money(sum((r.tariff_effect for r in rows), Decimal(0)) / n),
        mean_data_effect=round_money(sum((r.data_effect for r in rows), Decimal(0)) / n),
        tariff_share_pct=tariff_share.quantize(_PCT_QUANT, rounding=ROUND_HALF_UP),
        speed_kwh=day.speed_kwh,
        batch_kwh=day.batch_kwh,
        speed_kwh_shortfall_pct=shortfall.quantize(_PCT_QUANT, rounding=ROUND_HALF_UP),
    )


# --------------------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------------------


def _push_gauge(sim_date: date) -> bool:
    """Push the gauge, or say why not. Never fails the run.

    `reconciliation_daily` is the record; the gauge is a notification about it. A missed
    push is worth a warning, not a failed DAG task that would also skip the day's report.
    """
    try:
        return push_metrics(_JOB)
    except Exception as exc:  # noqa: BLE001 - any push failure is non-fatal, see above
        log.warning(
            "could not push voltstream_lambda_divergence",
            extra={"stage": "reconcile", "sim_date": sim_date.isoformat(), "detail": str(exc)},
        )
        return False


def run(sim_date: date) -> DaySummary:
    """Reconcile one simulated day. Returns the day's summary."""
    started = time.monotonic()
    config = get_config()

    # Reconciling against a day the batch layer has not closed would compare the speed
    # estimate with nothing authoritative — or, after a failed run, with bills the merge
    # function does not even serve.
    if not repositories.is_day_finalised(sim_date):
        raise RuntimeError(f"{sim_date} has no successful billing run to reconcile against")

    # Today's tariff from the landing file, not the archive: after a restatement this is
    # the corrected file, which is the one the batch bills were just recomputed from (D4).
    tariffs = parse_tariff_csv(
        objectstore.get_object_bytes(
            config.minio.bucket_landing, objectstore.landing_tariff_key(sim_date)
        ),
        sim_date,
    )

    day = reconcile_day(
        repositories.get_running_estimates_for_day(sim_date),
        repositories.get_batch_bills_for_day(sim_date),
        tariffs,
        config.tariff.boundaries(),
    )
    if not day.rows:
        raise RuntimeError(
            f"no household has both a provisional and a final bill for {sim_date}; "
            f"{len(day.batch_only)} billed, {len(day.speed_only)} estimated"
        )
    if day.speed_only or day.batch_only:
        log.warning(
            "households missing from one view were not reconciled",
            extra={
                "stage": "reconcile",
                "sim_date": sim_date.isoformat(),
                "speed_only": day.speed_only,
                "batch_only": day.batch_only,
            },
        )
    if day.capped:
        log.warning(
            "pct_divergence saturated at the column maximum",
            extra={
                "stage": "reconcile",
                "sim_date": sim_date.isoformat(),
                "households": day.capped,
                "ceiling": str(_PCT_MAX),
            },
        )

    written = repositories.replace_reconciliation(sim_date, day.rows)
    summary = summarise(day)

    # T138. One label-less gauge (§10.1), on the mean percentage, which is what
    # alerts.lambda_divergence_threshold_pct is compared against (D5). The max is logged.
    lambda_divergence.set(float(summary.mean_pct_divergence))
    pushed = _push_gauge(sim_date)

    log.info(
        "reconciliation complete",
        extra={
            "stage": "reconcile",
            "sim_date": sim_date.isoformat(),
            "rows_out": written,
            "mean_pct_divergence": str(summary.mean_pct_divergence),
            "max_pct_divergence": str(summary.max_pct_divergence),
            "mean_tariff_effect": str(summary.mean_tariff_effect),
            "mean_data_effect": str(summary.mean_data_effect),
            "tariff_share_pct": str(summary.tariff_share_pct),
            "speed_kwh": str(summary.speed_kwh),
            "batch_kwh": str(summary.batch_kwh),
            "speed_kwh_shortfall_pct": str(summary.speed_kwh_shortfall_pct),
            "gauge_pushed": pushed,
            "duration_seconds": round(time.monotonic() - started, 2),
        },
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile the speed layer's estimates against one day's final bills."
    )
    parser.add_argument("--sim-date", required=True, help="Simulated date, YYYY-MM-DD.")
    args = parser.parse_args()

    try:
        run(date.fromisoformat(args.sim_date))
    except Exception as exc:  # noqa: BLE001 - the exit code is the DAG's signal
        print(f"reconciliation failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
