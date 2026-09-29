"""The daily billing job — the batch layer's authoritative pass (§8.5).

Runs once per simulated day, after the tariff file lands. It rescans the whole closed day
from the master dataset and recomputes every bill from raw readings, which is the property
that makes restatement possible months later: nothing here depends on state carried
forward from a previous run.

**No watermark anywhere in this file.** A watermark is a streaming device for deciding
when to stop waiting; this job reads a day that is already over, so every reading it will
ever see is already in Parquet. That is the whole reason the batch layer is more correct
than the speed layer — it sees the late data the speed layer dropped.

**No arithmetic here either.** Netting and the block tariff come from `core/spark_expr.py`,
the same expressions the speed layer uses. The moment a rate or a boundary appears in this
file the shared-core argument (§4.4) is dead, and the consistency test stops proving
anything about the code that actually bills people.

The run is bracketed by `pipeline_runs`: a `running` row at the start, and in one
transaction at the end, the bills plus a `success` row — or a `failed` row and a re-raise.
A partially written billing day is worse than no billing day.
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from datetime import date
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from voltstream.batch import ledger
from voltstream.config import get_config
from voltstream.core.keys import DEDUP_COLUMNS
from voltstream.core.spark_expr import compute_bill_expr
from voltstream.logging_setup import get_logger
from voltstream.metrics import batch_duration_seconds, push_metrics
from voltstream.storage.objectstore import archive_tariff_path, raw_root
from voltstream.storage.postgres import connect
from voltstream.streaming.reference import read_tariff
from voltstream.streaming.session import build_session
from voltstream.streaming.sinks import (
    INSERT_REJECTED,
    RejectedRow,
    collect_rejected,
    pg_connection_string,
)
from voltstream.streaming.sources import (
    event_ts_bounds,
    known_household_ids,
    split_valid_invalid,
)

_JOB = "daily_billing"
_LAYER = "batch"
_LEDGER_LAYER = "batch_billing"

log = get_logger("daily-billing")

_KWH = DecimalType(12, 4)

# Billing reads six of the eleven contract columns. `voltage` is excluded on purpose: the
# contract carries it precisely so the column-pruning claim (§5.4) can be demonstrated
# rather than asserted, and `explain()` shows it absent from ReadSchema.
_BILLING_COLUMNS = (
    "meter_id",
    "household_id",
    "grid_zone",
    "event_ts",
    "consumption_kwh",
    "solar_generation_kwh",
    # Not used by the arithmetic, but carried: a record this job rejects is written to
    # rejected_records, and without trace_id that row cannot be joined back to the
    # producer's log lines or to the speed layer's view of the same reading (§10.3). One
    # short string per row is a cheap price for keeping the correlation story true of the
    # batch path as well as the streaming one.
    "trace_id",
)


class MissingTariffError(RuntimeError):
    """Raised when a household has no tariff row for the day.

    Loud by design (T113). The alternative is a null bill, which looks like a household
    that owes nothing — a silently wrong money figure is the one failure mode this job
    must never have.
    """


# --------------------------------------------------------------------------------------
# T111 — read exactly one partition, exactly the columns needed
# --------------------------------------------------------------------------------------


def read_day(spark: SparkSession, sim_date: date) -> DataFrame:
    """Read one simulated day from the master dataset.

    The filter is on `sim_date`, which is a partition column, so Spark resolves it as a
    PartitionFilter and never lists the other days' directories. Selecting the six billing
    columns keeps `voltage` out of ReadSchema entirely — Parquet is columnar, so those
    bytes are never read from the object store.
    """
    return (
        spark.read.parquet(raw_root())
        .filter(F.col("sim_date") == F.lit(sim_date))
        .select(*_BILLING_COLUMNS, "ingest_ts")
    )


# --------------------------------------------------------------------------------------
# T112 — dedup and validate
# --------------------------------------------------------------------------------------


def deduplicate(df: DataFrame) -> DataFrame:
    """Collapse repeated emissions of the same physical reading.

    The key is `(meter_id, event_ts)` — `core.keys.dedup_key` — not `event_id`, which is a
    fresh UUID on every emission and would treat a retransmission as a new reading. The
    earliest `ingest_ts` wins, so the surviving row is the one that arrived first rather
    than an arbitrary one, which keeps the choice deterministic across reruns.
    """
    ordering = Window.partitionBy(*DEDUP_COLUMNS).orderBy(F.col("ingest_ts").asc())
    return (
        df.withColumn("_rank", F.row_number().over(ordering))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


# --------------------------------------------------------------------------------------
# T113 — reference joins
# --------------------------------------------------------------------------------------
#
# The tariff is read by `streaming.reference.read_tariff`, shared with the speed layer:
# validated against the contract, effective-dated (§10.2: the latest row per household
# with `effective_date <= sim_date`, not full SCD Type 2).
#
# There is no weather join. T113 lists one, but nothing in a bill depends on the weather,
# and the producer does not use cloud cover either (R16), so joining it would only carry
# columns no one reads. Recorded as a cut in docs/assumptions.md §5.


def with_idle_households(totals: DataFrame, tariff: DataFrame) -> DataFrame:
    """Add a zero-usage row for every tariffed household with no valid reading (R35).

    A meter that was offline, or rejected, all day still has a customer behind it, and
    that customer owes the fixed charge. Without this the household got no bill at all
    and the day's row-count check failed the whole run. The tariff file is the day's
    customer list; a household it omits fails `join_tariff` if it has readings.
    """
    idle = tariff.select("household_id").join(
        totals.select("household_id"), on="household_id", how="left_anti"
    )
    return totals.unionByName(
        idle.select(
            F.col("household_id"),
            F.lit(Decimal(0)).cast(_KWH).alias("consumption_kwh"),
            F.lit(Decimal(0)).cast(_KWH).alias("solar_kwh"),
            F.lit(0).cast("int").alias("readings_count"),
        )
    )


def join_tariff(totals: DataFrame, tariff: DataFrame) -> DataFrame:
    """Inner-join the tariff, then assert nothing was lost.

    An inner join would quietly drop a household with no tariff row, and the day's report
    would simply be missing them — the kind of error nobody notices until a customer does.
    The count check turns that into a failed run naming the household.
    """
    joined = totals.join(F.broadcast(tariff), on="household_id", how="left")

    missing = [r["household_id"] for r in joined.filter(F.col("fixed_charge").isNull()).collect()]
    if missing:
        raise MissingTariffError(
            f"{len(missing)} household(s) have no tariff row for this day: "
            f"{', '.join(sorted(missing)[:10])}" + (" ..." if len(missing) > 10 else "")
        )
    return joined


# --------------------------------------------------------------------------------------
# T114 — netting and tariff, orchestration only
# --------------------------------------------------------------------------------------


def aggregate_to_daily(df: DataFrame) -> DataFrame:
    """Per-household totals for the day, plus the counts the bill row carries."""
    return df.groupBy("household_id").agg(
        F.sum("consumption_kwh").cast(_KWH).alias("consumption_kwh"),
        F.sum("solar_generation_kwh").cast(_KWH).alias("solar_kwh"),
        F.count("*").cast("int").alias("readings_count"),
    )


def compute_bills(joined: DataFrame, sim_date: date, run_id: uuid.UUID) -> DataFrame:
    """Apply `core/spark_expr.py` and shape the row for `household_bill_daily`.

    Every number below comes from `compute_bill_expr` — the same call the speed layer
    makes. This function chooses columns; it does not do arithmetic.
    """
    bill = compute_bill_expr(
        F.col("consumption_kwh"),
        F.col("solar_kwh"),
        rate_cols=(F.col("block_1_rate"), F.col("block_2_rate"), F.col("block_3_rate")),
        fixed_charge_col=F.col("fixed_charge"),
        subsidy_flag_col=F.col("subsidy_flag"),
        subsidy_pct_col=F.col("subsidy_pct"),
        export_rate_col=F.col("export_rate"),
        # Structure only, never rates (D2); the speed layer reads the same (R26).
        boundaries=get_config().tariff.boundaries(),
    )

    return joined.select(
        F.col("household_id"),
        F.lit(sim_date).cast("date").alias("sim_date"),
        F.col("consumption_kwh"),
        F.col("solar_kwh"),
        bill.self_consumed_kwh.alias("self_consumed_kwh"),
        bill.billable_import_kwh.alias("billable_import_kwh"),
        bill.export_kwh.alias("export_kwh"),
        bill.energy_charge.alias("energy_charge"),
        bill.fixed_charge.alias("fixed_charge"),
        bill.subsidy_discount.alias("subsidy_discount"),
        bill.export_credit.alias("export_credit"),
        # D5: never clamped at zero. A household that exported more than it imported is
        # owed money, and a bill that cannot go negative would quietly keep it.
        bill.final_bill.alias("final_bill"),
        bill.tier_breakdown.alias("tier_breakdown"),
        F.col("effective_date").alias("tariff_effective_date"),
        F.col("readings_count"),
        F.col("duplicates_removed"),
        F.lit(str(run_id)).alias("pipeline_run_id"),
    )


# --------------------------------------------------------------------------------------
# T117 — archive the reference file the run consumed
# --------------------------------------------------------------------------------------


def archive_tariff(tariff: DataFrame, sim_date: date, run_id: uuid.UUID) -> str:
    """Write the tariff this run billed with to the archive bucket as Parquet, per run.

    This is what makes a restatement possible after the landing zone has been cleaned up.
    Recomputing a bill from the master dataset is only reproducible if the *rates* that
    applied that day are still recoverable too — raw readings alone would let you
    recompute a different bill with today's tariff and call it a restatement.

    Written as Parquet rather than copied as CSV so the Decimal types survive: a
    re-inferred CSV gives DoubleType and the restated bill differs from the original in
    the last cent, which is precisely the kind of discrepancy a restatement must not have.
    """
    path = archive_tariff_path(sim_date, str(run_id))
    tariff.write.mode("overwrite").parquet(path)
    return path


# --------------------------------------------------------------------------------------
# T115 — run ledger and the one-transaction write
# --------------------------------------------------------------------------------------


def finalise(
    bills: list[tuple],
    columns: list[str],
    run_id: uuid.UUID,
    sim_date: date,
    rows_in: int,
    rejected: list[RejectedRow],
) -> int:
    """Write the day's bills and rejects and close the run, in **one** transaction.

    Either the whole day lands or none of it does (§5.5). A half-written billing day is
    not a smaller problem than an unwritten one — it is a worse one, because it looks
    finished.

    Superseding the previous `success` row happens inside the same transaction, because
    the partial unique index permits only one at a time: inserting the new one before
    demoting the old would violate it, and demoting first in a separate transaction would
    leave a window where the day looks unfinalised and the API serves a provisional bill.

    The day's batch rejects are replaced, not appended (R23): every retry and every
    restatement used to insert another full set, so the report's reject count grew with
    each run. A reject's day is its reading's `event_ts`, as
    `repositories.get_rejected_for_day` reads it.
    """
    column_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    with connect(pg_connection_string()) as conn, conn.cursor() as cur:
        ledger.supersede_success(cur, sim_date, _LEDGER_LAYER)
        cur.execute(
            "DELETE FROM rejected_records WHERE stage = 'batch' "
            "AND (raw_payload ->> 'event_ts')::timestamptz::date = %s",
            (sim_date,),
        )
        if rejected:
            cur.executemany(INSERT_REJECTED, rejected)
        # Replaced, not upserted (R25): a restatement that bills fewer households must not
        # leave the previous run's rows behind, attributed to a run that was superseded.
        cur.execute("DELETE FROM household_bill_daily WHERE sim_date = %s", (sim_date,))
        cur.executemany(
            f"INSERT INTO household_bill_daily ({column_list}) VALUES ({placeholders})", bills
        )
        # Every run's bills, kept (D6, R25): what the current table no longer shows.
        cur.executemany(
            f"INSERT INTO household_bill_history ({column_list}) VALUES ({placeholders})", bills
        )
        ledger.complete_run(cur, run_id, rows_in, len(bills))
        conn.commit()
    return len(bills)


# --------------------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------------------


def run(sim_date: date) -> int:
    """Bill one simulated day. Returns the number of bills written."""
    started = time.monotonic()
    run_id = uuid.uuid4()
    spark = build_session(f"voltstream-{_JOB}")
    spark.sparkContext.setLogLevel("WARN")

    rows_in = 0
    ledger.start_run(run_id, sim_date, _LEDGER_LAYER)
    try:
        raw = read_day(spark, sim_date).persist()
        rows_in = raw.count()
        if rows_in == 0:
            raise RuntimeError(f"no raw readings for {sim_date}; nothing to bill")

        deduped = deduplicate(raw).persist()
        valid, invalid = split_valid_invalid(
            deduped,
            known_household_ids=known_household_ids(),
            event_ts_bounds=event_ts_bounds(),
        )
        rejected = collect_rejected(invalid, _LAYER)

        tariff = read_tariff(spark, sim_date)
        totals = with_idle_households(aggregate_to_daily(valid), tariff)
        # Duplicates are counted per household from the pre-dedup frame, so the column
        # reports what this day actually contained rather than a global figure.
        dupes = raw.groupBy("household_id").agg(
            (F.count("*") - F.countDistinct(*DEDUP_COLUMNS)).cast("int").alias("duplicates_removed")
        )
        totals = totals.join(dupes, on="household_id", how="left").fillna({"duplicates_removed": 0})

        joined = join_tariff(totals, tariff)
        bills_df = compute_bills(joined, sim_date, run_id)

        # After the join, so a day that fails on a missing tariff does not leave an
        # archive implying it was processed.
        archive_tariff(tariff, sim_date, run_id)

        columns = bills_df.columns
        bills = [tuple(r) for r in bills_df.collect()]
        written = finalise(bills, columns, run_id, sim_date, rows_in, rejected)

        duration = time.monotonic() - started
        batch_duration_seconds.labels(job=_JOB).observe(duration)
        # T116 / R07: this container exits in seconds, before any scrape, so the duration
        # (and this run's batch-stage reject counts) are pushed. After finalise(): the bills
        # are committed whether or not the push lands.
        pushed = push_metrics(_JOB)
        log.info(
            "billing run complete",
            extra={
                "stage": "batch",
                "sim_date": sim_date.isoformat(),
                "run_id": str(run_id),
                "rows_in": rows_in,
                "rows_out": written,
                "rows_rejected": len(rejected),
                "duration_seconds": round(duration, 2),
                "metrics_pushed": pushed,
            },
        )
        return written
    except Exception:
        ledger.fail_run(run_id, rows_in)
        log.exception(
            "billing run failed",
            extra={"stage": "batch", "sim_date": sim_date.isoformat(), "run_id": str(run_id)},
        )
        raise
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute authoritative bills for one simulated day."
    )
    parser.add_argument("--sim-date", required=True, help="Simulated date to bill, YYYY-MM-DD.")
    args = parser.parse_args()

    try:
        run(date.fromisoformat(args.sim_date))
    except Exception as exc:  # noqa: BLE001 - the exit code is the DAG's signal
        print(f"daily_billing failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
