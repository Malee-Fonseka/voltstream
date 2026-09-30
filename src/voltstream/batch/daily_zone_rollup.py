"""Authoritative daily per-zone aggregates, plus the cross-check gate (T124, T125).

The speed layer already publishes per-zone numbers, but they are 15-minute windows
computed under a watermark and measurably incomplete — about 1 % of a day's energy never
reaches them (`docs/assumptions.md` §2). This job recomputes the day's zone totals from
the master dataset with no watermark, so `zone_metrics_daily` is to `zone_metrics_rt` what
`household_bill_daily` is to `household_running_rt`.

**The cross-check is the interesting part.** Zone totals and household totals are two
aggregations of the same readings along different keys, so they must sum to the same
energy. If they do not, something is wrong in a way no individual row would reveal —
a dropped join, a double-counted partition, a zone assignment that changed mid-day. The
job fails rather than publishing, because a report that is internally inconsistent is
worse than a missing one: someone will act on it.
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from voltstream.batch import ledger
from voltstream.batch.daily_billing import deduplicate, read_day
from voltstream.core.spark_expr import netting_expr
from voltstream.logging_setup import get_logger
from voltstream.metrics import batch_duration_seconds, push_metrics
from voltstream.storage.postgres import connect
from voltstream.streaming.session import build_session
from voltstream.streaming.sinks import pg_connection_string
from voltstream.streaming.sources import event_ts_bounds, known_household_ids, split_valid_invalid

_JOB = "daily_zone_rollup"
_LEDGER_LAYER = "batch_rollup"

log = get_logger("zone-rollup")

_KWH = DecimalType(12, 4)
_RATIO = DecimalType(5, 4)

# Energy is stored to 4 decimal places, so the two aggregations can differ in the last
# place from summing in a different order. A tolerance of one hundredth of a kWh is far
# below anything that would matter and far above float-ordering noise.
_CROSS_CHECK_TOLERANCE_KWH = Decimal("0.01")

# How far back the speed view keeps its 15-minute windows (R32). Fourteen simulated days
# is 70 real minutes, more than the one hour the longest Grafana dashboard looks back.
_SPEED_WINDOW_RETENTION_DAYS = 14


class CrossCheckFailed(RuntimeError):
    """Zone totals and household totals disagree for the same day.

    Fails the run rather than publishing (T125, §10.4). Two aggregations of one set of
    readings along different keys have to agree; when they do not, the day's report is
    internally inconsistent and nobody downstream can tell which half to believe.
    """


def zone_rollup(valid: DataFrame, sim_date: date, run_id: uuid.UUID) -> DataFrame:
    """Per-zone daily totals, including the peak 15-simulated-minute window.

    Self-consumed and exported energy are the sums of the zone's households' figures, as
    their bills compute them: each household's day is netted on its daily totals, with
    `core/spark_expr.py`'s function, and only then summed by zone (R24). Netting each
    reading instead counted midday export that the household's bill nets away against its
    evening import, so the zone figures did not add up to the bills they sit beside.
    """
    households = valid.groupBy("grid_zone", "household_id").agg(
        F.sum("consumption_kwh").cast(_KWH).alias("consumption_kwh"),
        F.sum("solar_generation_kwh").cast(_KWH).alias("solar_kwh"),
    )
    netting = netting_expr(F.col("consumption_kwh"), F.col("solar_kwh"))
    energy = households.groupBy("grid_zone").agg(
        F.sum("consumption_kwh").cast(_KWH).alias("total_consumption_kwh"),
        F.sum("solar_kwh").cast(_KWH).alias("total_solar_kwh"),
        F.sum(netting.self_consumed_kwh).cast(_KWH).alias("self_consumed_kwh"),
        F.sum(netting.export_kwh).cast(_KWH).alias("export_kwh"),
    )
    counts = valid.groupBy("grid_zone").agg(
        F.size(F.collect_set("meter_id")).alias("active_meters"),
        F.count("*").cast("int").alias("readings_count"),
    )
    daily = energy.join(counts, on="grid_zone", how="inner")

    # Peak window: the busiest 15 simulated minutes of the day, per zone. Computed here
    # rather than read from zone_metrics_rt, which is the incomplete view.
    windows = valid.groupBy("grid_zone", F.window(F.col("event_ts"), "15 minutes").alias("w")).agg(
        F.sum("consumption_kwh").cast(_KWH).alias("window_kwh")
    )
    peaks = windows.groupBy("grid_zone").agg(
        F.max_by(F.col("w.start"), F.col("window_kwh")).alias("peak_window_start"),
        F.max("window_kwh").alias("peak_consumption_kwh"),
    )

    return (
        daily.join(peaks, on="grid_zone", how="inner")
        .withColumn("sim_date", F.lit(sim_date).cast("date"))
        .withColumn(
            # Same saturation rule as the speed layer: the column is NUMERIC(5,4) and the
            # metric answers "what fraction of demand renewables met", which cannot exceed
            # all of it. The surplus stays visible in the two raw totals.
            "renewable_ratio",
            F.when(
                F.col("total_consumption_kwh") > 0,
                F.least(
                    F.col("total_solar_kwh") / F.col("total_consumption_kwh"),
                    F.lit(Decimal(1)),
                ).cast(_RATIO),
            ).otherwise(F.lit(0).cast(_RATIO)),
        )
        .withColumn("pipeline_run_id", F.lit(str(run_id)))
        .select(
            "grid_zone",
            "sim_date",
            "total_consumption_kwh",
            "total_solar_kwh",
            "self_consumed_kwh",
            "export_kwh",
            "renewable_ratio",
            "peak_window_start",
            "peak_consumption_kwh",
            "active_meters",
            "readings_count",
            "pipeline_run_id",
        )
    )


def cross_check(zone_total_kwh: Decimal, household_total_kwh: Decimal, sim_date: date) -> None:
    """Assert the two aggregations of the day agree (T125).

    Raises rather than warns. §10.4 puts it plainly: fail the DAG rather than silently
    publish a wrong report.
    """
    difference = abs(zone_total_kwh - household_total_kwh)
    if difference > _CROSS_CHECK_TOLERANCE_KWH:
        raise CrossCheckFailed(
            f"zone and household totals disagree for {sim_date}: "
            f"zones {zone_total_kwh} kWh vs households {household_total_kwh} kWh "
            f"(difference {difference} kWh, tolerance {_CROSS_CHECK_TOLERANCE_KWH}). "
            "Both are aggregations of the same readings, so this is a pipeline defect, "
            "not a rounding artefact — the day has not been published."
        )


def _household_total_kwh(sim_date: date) -> Decimal:
    """The day's energy as the billing job recorded it.

    Zero when the day has no bills yet. That is not a passing cross-check: the caller
    compares it against a non-zero zone total, so an unbilled day fails loudly rather than
    matching a zone total of zero against nothing.
    """
    with connect(pg_connection_string()) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(SUM(consumption_kwh), 0) FROM household_bill_daily "
            "WHERE sim_date = %s",
            (sim_date,),
        )
        row = cur.fetchone()
        return Decimal(row[0]) if row else Decimal(0)


def speed_window_cutoff(sim_date: date) -> datetime:
    """Speed-view windows starting before this are deleted once `sim_date` is rolled up."""
    start = sim_date - timedelta(days=_SPEED_WINDOW_RETENTION_DAYS)
    return datetime.combine(start, datetime.min.time(), UTC)


def _write(
    rows: list[tuple], columns: list[str], run_id: uuid.UUID, sim_date: date, rows_in: int
) -> int:
    """The day's zone rows and this run's `success`, in one transaction (R24).

    Also retires the speed view's old windows (R32). `zone_metrics_rt` gained about 480
    rows per simulated day and was never pruned; once a day has its authoritative row
    here, the provisional windows from well before it have nothing left to answer.
    """
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c not in ("grid_zone", "sim_date")
    )
    with connect(pg_connection_string()) as conn, conn.cursor() as cur:
        ledger.supersede_success(cur, sim_date, _LEDGER_LAYER)
        cur.executemany(
            f"INSERT INTO zone_metrics_daily ({', '.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT (grid_zone, sim_date) DO UPDATE SET {updates}, computed_at = now()",
            rows,
        )
        cur.execute(
            "DELETE FROM zone_metrics_rt WHERE window_start < %s",
            (speed_window_cutoff(sim_date),),
        )
        retired = cur.rowcount
        ledger.complete_run(cur, run_id, rows_in, len(rows))
        conn.commit()
    if retired:
        log.info(
            "retired old speed-view windows",
            extra={"stage": "batch", "sim_date": sim_date.isoformat(), "rows_deleted": retired},
        )
    return len(rows)


def run(sim_date: date) -> int:
    started = time.monotonic()
    run_id = uuid.uuid4()
    spark = build_session(f"voltstream-{_JOB}")
    spark.sparkContext.setLogLevel("WARN")

    # Its own ledger row (R24): zone_metrics_daily.pipeline_run_id used to point at a run
    # nobody recorded, and a failed rollup left no trace outside Airflow's log.
    rows_in = 0
    ledger.start_run(run_id, sim_date, _LEDGER_LAYER)
    try:
        raw = read_day(spark, sim_date)
        valid, _ = split_valid_invalid(
            deduplicate(raw),
            known_household_ids=known_household_ids(),
            event_ts_bounds=event_ts_bounds(),
        )

        rollup = zone_rollup(valid, sim_date, run_id).persist()
        rows = [tuple(r) for r in rollup.collect()]
        if not rows:
            raise RuntimeError(f"no readings to roll up for {sim_date}")

        zone_total = sum((r[2] for r in rows), Decimal(0))
        rows_in = sum(r[rollup.columns.index("readings_count")] for r in rows)

        # Cross-check before the write, not after: the point is to withhold a report that
        # does not add up, and a check that runs afterwards has already published it.
        cross_check(zone_total, _household_total_kwh(sim_date), sim_date)

        written = _write(rows, rollup.columns, run_id, sim_date, rows_in)

        duration = time.monotonic() - started
        batch_duration_seconds.labels(job=_JOB).observe(duration)
        # T116 / R07: a one-shot container cannot be scraped, so the duration is pushed.
        pushed = push_metrics(_JOB)
        log.info(
            "zone rollup complete",
            extra={
                "stage": "batch",
                "sim_date": sim_date.isoformat(),
                "run_id": str(run_id),
                "rows_out": written,
                "zone_total_kwh": str(zone_total),
                "duration_seconds": round(duration, 2),
                "metrics_pushed": pushed,
            },
        )
        return written
    except Exception:
        ledger.fail_run(run_id, rows_in)
        log.exception(
            "zone rollup failed",
            extra={"stage": "batch", "sim_date": sim_date.isoformat(), "run_id": str(run_id)},
        )
        raise
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily per-zone rollup for one simulated day.")
    parser.add_argument("--sim-date", required=True, help="Simulated date to roll up, YYYY-MM-DD.")
    args = parser.parse_args()

    try:
        run(date.fromisoformat(args.sim_date))
    except Exception as exc:  # noqa: BLE001 - the exit code is the DAG's signal
        print(f"daily_zone_rollup failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
