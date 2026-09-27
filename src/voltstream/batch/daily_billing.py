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
import os
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal

import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType
from pyspark.sql.window import Window

from voltstream.config import get_config
from voltstream.core.keys import DEDUP_COLUMNS
from voltstream.core.spark_expr import compute_bill_expr
from voltstream.core.tariff import BlockBoundary
from voltstream.logging_setup import get_logger
from voltstream.metrics import batch_duration_seconds, push_metrics
from voltstream.storage.objectstore import archive_tariff_path, landing_tariff_path, raw_root
from voltstream.streaming.session import build_session
from voltstream.streaming.sinks import pg_connection_string, write_rejected
from voltstream.streaming.sources import split_valid_invalid

_JOB = "daily_billing"
_LAYER = "batch"

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

# The tariff contract (T031), declared rather than inferred. inferSchema would read the
# money columns as DoubleType and silently defeat D5's Decimal-everywhere rule — a bill
# would then be computed in binary floating point and be wrong in the last cent.
_TARIFF_SCHEMA = StructType(
    [
        StructField("household_id", StringType()),
        StructField("effective_date", StringType()),
        StructField("billing_tier", StringType()),
        StructField("subsidy_flag", StringType()),
        StructField("subsidy_pct", DecimalType(5, 2)),
        StructField("fixed_charge", DecimalType(12, 2)),
        StructField("block_1_rate", DecimalType(12, 2)),
        StructField("block_2_rate", DecimalType(12, 2)),
        StructField("block_3_rate", DecimalType(12, 2)),
        StructField("export_rate", DecimalType(12, 2)),
    ]
)


class MissingTariffError(RuntimeError):
    """Raised when a household has no tariff row for the day.

    Loud by design (T113). The alternative is a null bill, which looks like a household
    that owes nothing — a silently wrong money figure is the one failure mode this job
    must never have.
    """


def _boundaries() -> list[BlockBoundary]:
    """Block boundaries from config — structure only, never rates (D2).

    Same conversion as the speed layer's. Both layers must read the same boundaries from
    the same place or the consistency test proves nothing about production behaviour.
    """
    return [
        BlockBoundary(b.name, None if b.up_to_kwh is None else Decimal(b.up_to_kwh))
        for b in get_config().tariff.blocks
    ]


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


def _known_household_ids() -> frozenset[str]:
    config = get_config()
    return frozenset(f"HH-{i:04d}" for i in range(1, config.simulation.households + 1))


def _event_ts_bounds() -> tuple[datetime, datetime]:
    epoch = get_config().simulation.epoch_sim
    return (epoch - timedelta(days=365), epoch + timedelta(days=365 * 50))


# --------------------------------------------------------------------------------------
# T113 — reference joins
# --------------------------------------------------------------------------------------


def read_tariff(spark: SparkSession, sim_date: date) -> DataFrame:
    """The day's tariff file, declared schema, effective-dated.

    §10.2 is explicit that this is a simple effective-dated join and not full SCD Type 2:
    take the latest row per household with `effective_date <= sim_date`. That is enough
    for a tariff that changes daily and is stated as a limitation rather than hidden.
    """
    raw = (
        spark.read.option("header", "true")
        .schema(_TARIFF_SCHEMA)
        .csv(landing_tariff_path(sim_date))
        .withColumn("effective_date", F.to_date(F.col("effective_date")))
        .withColumn("subsidy_flag", F.col("subsidy_flag") == F.lit("true"))
    )

    applicable = raw.filter(F.col("effective_date") <= F.lit(sim_date))
    latest = Window.partitionBy("household_id").orderBy(F.col("effective_date").desc())
    return (
        applicable.withColumn("_rank", F.row_number().over(latest))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
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
        boundaries=_boundaries(),
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


def archive_tariff(tariff: DataFrame, sim_date: date) -> str:
    """Write the day's tariff to the archive bucket as Parquet.

    This is what makes a restatement possible after the landing zone has been cleaned up.
    Recomputing a bill from the master dataset is only reproducible if the *rates* that
    applied that day are still recoverable too — raw readings alone would let you
    recompute a different bill with today's tariff and call it a restatement.

    Written as Parquet rather than copied as CSV so the Decimal types survive: a
    re-inferred CSV gives DoubleType and the restated bill differs from the original in
    the last cent, which is precisely the kind of discrepancy a restatement must not have.
    """
    path = archive_tariff_path(sim_date)
    tariff.write.mode("overwrite").parquet(path)
    return path


# --------------------------------------------------------------------------------------
# T115 — run ledger and the one-transaction write
# --------------------------------------------------------------------------------------


def _orchestrator_run_id() -> str | None:
    """Airflow's dag_run_id, when this job was launched by a DAG.

    None when it was not — a bare `spark-submit` or `make backfill` is a legitimate way to
    run this, not a degraded one. Recording it is what makes a restatement legible later:
    two ledger rows for one simulated day, each naming the execution that produced it.
    """
    return os.environ.get("VOLTSTREAM_ORCHESTRATOR_RUN_ID") or None


def _start_run(run_id: uuid.UUID, sim_date: date) -> None:
    with psycopg.connect(pg_connection_string()) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs "
            "(run_id, sim_date, layer, status, started_at, orchestrator_run_id) "
            "VALUES (%s, %s, 'batch_billing', 'running', now(), %s)",
            (run_id, sim_date, _orchestrator_run_id()),
        )
        conn.commit()


def _fail_run(run_id: uuid.UUID, rows_in: int) -> None:
    with psycopg.connect(pg_connection_string()) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_runs SET status = 'failed', finished_at = now(), rows_in = %s "
            "WHERE run_id = %s",
            (rows_in, run_id),
        )
        conn.commit()


def finalise(
    bills: list[tuple], columns: list[str], run_id: uuid.UUID, sim_date: date, rows_in: int
) -> int:
    """Write the day's bills and close the run, in **one** transaction.

    Either the whole day lands or none of it does (§5.5). A half-written billing day is
    not a smaller problem than an unwritten one — it is a worse one, because it looks
    finished.

    Superseding the previous `success` row happens inside the same transaction, because
    the partial unique index permits only one at a time: inserting the new one before
    demoting the old would violate it, and demoting first in a separate transaction would
    leave a window where the day looks unfinalised and the API serves a provisional bill.
    """
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c not in ("household_id", "sim_date")
    )

    with psycopg.connect(pg_connection_string()) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_runs SET status = 'superseded' "
            "WHERE sim_date = %s AND layer = 'batch_billing' AND status = 'success'",
            (sim_date,),
        )
        cur.executemany(
            f"INSERT INTO household_bill_daily ({', '.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT (household_id, sim_date) DO UPDATE SET {updates}, computed_at = now()",
            bills,
        )
        cur.execute(
            "UPDATE pipeline_runs SET status = 'success', finished_at = now(), "
            "rows_in = %s, rows_out = %s WHERE run_id = %s",
            (rows_in, len(bills), run_id),
        )
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
    _start_run(run_id, sim_date)
    try:
        raw = read_day(spark, sim_date).persist()
        rows_in = raw.count()
        if rows_in == 0:
            raise RuntimeError(f"no raw readings for {sim_date}; nothing to bill")

        deduped = deduplicate(raw).persist()
        valid, invalid = split_valid_invalid(
            deduped,
            known_household_ids=_known_household_ids(),
            event_ts_bounds=_event_ts_bounds(),
        )
        rejected = write_rejected(invalid, _LAYER)

        totals = aggregate_to_daily(valid)
        # Duplicates are counted per household from the pre-dedup frame, so the column
        # reports what this day actually contained rather than a global figure.
        dupes = raw.groupBy("household_id").agg(
            (F.count("*") - F.countDistinct(*DEDUP_COLUMNS)).cast("int").alias("duplicates_removed")
        )
        totals = totals.join(dupes, on="household_id", how="left").fillna({"duplicates_removed": 0})

        tariff = read_tariff(spark, sim_date)
        joined = join_tariff(totals, tariff)
        bills_df = compute_bills(joined, sim_date, run_id)

        # After the join, so a day that fails on a missing tariff does not leave an
        # archive implying it was processed.
        archive_tariff(tariff, sim_date)

        columns = bills_df.columns
        bills = [tuple(r) for r in bills_df.collect()]
        written = finalise(bills, columns, run_id, sim_date, rows_in)

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
                "rows_rejected": rejected,
                "duration_seconds": round(duration, 2),
                "metrics_pushed": pushed,
            },
        )
        return written
    except Exception:
        _fail_run(run_id, rows_in)
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
