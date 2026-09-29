"""The speed layer: approximate, low-latency views over the live stream (§8.4).

Two streaming queries over one source, because they aggregate at different grains:

- **zone** — 15-simulated-minute tumbling windows per `grid_zone`, into `zone_metrics_rt`.
  This is the operational view: grid load and renewable contribution *right now*.
- **household** — a 1-simulated-day window per household, into `household_running_rt`,
  carrying a provisional bill costed against **yesterday's** tariff.

Both run `outputMode("update")`. Per D3 that matters more than it looks: each micro-batch
upserts the windows it touched, so a window appears roughly a trigger after its first
event and is revised as late data is absorbed. The watermark governs only when a window
becomes immutable and its state is evicted — not when it first becomes visible. Under
`append` the operational view would lag the watermark, which is the opposite of what the
speed layer is for.

Every duration below is in the units its name states. A watermark of 30 **simulated**
minutes is 6.25 real seconds at TIME_SCALE 288; the 10-**real**-second trigger is 48
simulated minutes, which is the larger of the two. At this time compression the trigger,
not the watermark, dominates what gets dropped — see D3 and `docs/assumptions.md`.

Both queries read the stream through `deduplicated()`: one watermark, and the batch
layer's dedup key, applied before any aggregation (R02). The provisional figures therefore
count each reading once, and drop the stragglers the watermark rules out.

The provisional bill is deliberately stale and deliberately labelled. It applies
yesterday's tariff because today's does not exist until the day closes (§3.1), and every
row records `tariff_source_date` so the staleness is visible rather than implied. The
bill is computed with `core/spark_expr.py` — the same call the batch job makes, which is
what makes the two layers comparable rather than merely similar.
"""

from __future__ import annotations

import signal
import sys
from datetime import date, timedelta
from decimal import Decimal
from types import FrameType

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DecimalType

from voltstream.config import get_config
from voltstream.core.keys import DEDUP_COLUMNS
from voltstream.core.spark_expr import compute_bill_expr
from voltstream.logging_setup import get_logger
from voltstream.metrics import events_consumed_total, start_metrics_server, zone_renewable_ratio
from voltstream.storage.objectstore import landing_tariff_path
from voltstream.streaming.reference import read_tariff
from voltstream.streaming.session import build_session, checkpoint_path
from voltstream.streaming.sinks import observe_e2e_latency, upsert_batch, write_rejected
from voltstream.streaming.sources import (
    KafkaLagListener,
    event_ts_bounds,
    known_household_ids,
    read_meter_stream,
    split_valid_invalid,
)

_LAYER = "speed"
_ZONE_JOB = "speed_layer_zone"
_HOUSEHOLD_JOB = "speed_layer_household"
_VALIDATION_JOB = "speed_layer_validation"

# Carried through the zone aggregation for the latency metric only; not a table column.
_NEWEST_KAFKA_TS = "newest_kafka_ts"

log = get_logger("speed-layer")

_KWH = DecimalType(12, 4)
_RATIO = DecimalType(5, 4)

# Tariff frames, cached per effective date. The speed layer re-reads only when the
# simulated day rolls over, which at this time scale is once every 5 real minutes.
_tariff_cache: dict[date, DataFrame] = {}


def _watermark_sim_minutes() -> str:
    return f"{get_config().speed_layer.watermark_sim_minutes} minutes"


def _window_sim_minutes() -> str:
    return f"{get_config().speed_layer.window_sim_minutes} minutes"


# --------------------------------------------------------------------------------------
# Zone aggregation (T088, T089)
# --------------------------------------------------------------------------------------


def deduplicated(valid_df: DataFrame) -> DataFrame:
    """Watermark on `event_ts`, then drop repeated readings (R02). Both aggregations read
    this, never the raw valid stream.

    The watermark string is SIMULATED minutes: it is applied to `event_ts`, which advances
    at TIME_SCALE relative to the wall clock.

    The key is `core.keys.DEDUP_COLUMNS`, the batch layer's key, so the two layers agree on
    what a duplicate is. Without this the speed layer summed the 2 % injected duplicates
    that the batch layer removes, and every provisional figure ran about 2 % high.

    Streaming dedup also drops records older than the watermark ("to avoid any possibility
    of duplicates", in Spark's words). That is the speed layer's documented trade (§5.3,
    "drop stragglers") applied before aggregation:

    - Network reordering (under 30 simulated minutes, D3) is not dropped. The producer
      sends a reordered record with its own tick, and a micro-batch's watermark is the
      newest tick of the batch before it minus 30 simulated minutes, so it trails the
      record's tick by at least the watermark (by 39.6 unless the tick straddles two
      batches). That holds while delivery keeps up with the 2-real-second tick; a
      producer stalled for longer can still lose some.
    - Store-and-forward backfill is dropped once it is older than the watermark, so the
      daily provisional totals now miss part of each meter's backlog. The batch layer
      rescans a closed day and sees all of it. That gap is the `data_effect` D4 attributes
      and T094 measures.
    """
    return valid_df.withWatermark("event_ts", _watermark_sim_minutes()).dropDuplicates(
        list(DEDUP_COLUMNS)
    )


def zone_aggregation(valid_df: DataFrame) -> DataFrame:
    """Windowed per-zone load and renewable contribution.

    Expects `deduplicated()` output: that is where the watermark is set. The window string
    is SIMULATED minutes, like the watermark.
    """
    return (
        valid_df.groupBy(F.window(F.col("event_ts"), _window_sim_minutes()), F.col("grid_zone"))
        .agg(
            F.sum("consumption_kwh").cast(_KWH).alias("total_consumption_kwh"),
            F.sum("solar_generation_kwh").cast(_KWH).alias("total_solar_kwh"),
            # Not countDistinct: distinct aggregations are rejected outright on a
            # streaming DataFrame. Spark suggests approx_count_distinct, but this count
            # is reported per zone and ~10 meters is far too small a cardinality to want
            # a HyperLogLog estimate. collect_set is exact, and its state is bounded by
            # the meters in one zone in one window.
            F.size(F.collect_set("meter_id")).alias("active_meters"),
            # For the latency metric (R21). In update mode a window is emitted only when a
            # record in this micro-batch touched it, so its newest Kafka timestamp always
            # belongs to this micro-batch.
            #
            # Adding or removing an aggregate here changes the query's state schema, and
            # Spark then refuses to resume from the old checkpoint: reset the
            # speed_checkpoints volume (`make clean`) after any such change.
            F.max("kafka_timestamp").alias(_NEWEST_KAFKA_TS),
        )
        .select(
            F.col("grid_zone"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("total_consumption_kwh"),
            F.col("total_solar_kwh"),
            # Two guards, both for situations that happen routinely rather than rarely.
            #
            # Zero consumption: a window where every meter in a zone is dropped out
            # divides by zero.
            #
            # Solar above consumption: a low-demand zone at midday generates more than it
            # uses, giving a ratio well above 1. The column is NUMERIC(5,4), so anything
            # from 10.0 up does not fit, casts to null, and violates the NOT NULL
            # constraint — which killed the query in testing on a ratio of 14.3.
            # Saturating at 1.0 is the right reading of the metric as well as the safe
            # one: it answers "what fraction of demand did renewables meet", which cannot
            # exceed all of it. Surplus is not lost, because the raw numerator and
            # denominator are both stored in this same row.
            F.when(
                F.col("total_consumption_kwh") > 0,
                F.least(
                    F.col("total_solar_kwh") / F.col("total_consumption_kwh"),
                    F.lit(Decimal(1)),
                ).cast(_RATIO),
            )
            .otherwise(F.lit(0).cast(_RATIO))
            .alias("renewable_ratio"),
            F.col("active_meters"),
            F.col(_NEWEST_KAFKA_TS),
        )
    )


def _write_zone_batch(batch_df: DataFrame, batch_id: int) -> None:
    batch_df.persist()
    try:
        written = upsert_batch(
            batch_df.drop(_NEWEST_KAFKA_TS),
            "zone_metrics_rt",
            conflict_cols=("grid_zone", "window_start"),
        )

        # R21: e2e latency for the speed layer is measured here, once the zone view has
        # committed — it is the view the dashboard reads, so "visible in Postgres" is the
        # end of the path (§2.5's < 60 s). One observation per window written: now minus
        # the newest record in that window. At 288x a trigger spans about three windows,
        # so a batch's observations spread across the interval its records waited for it.
        observe_e2e_latency(batch_df, _NEWEST_KAFKA_TS, _LAYER)

        # T092: the gauge is set from the same value written to Postgres. Computing it
        # separately would let the dashboard and the LowRenewableContribution alert
        # disagree about the same zone at the same instant.
        latest = (
            batch_df.groupBy("grid_zone")
            .agg(F.max_by("renewable_ratio", "window_start").alias("ratio"))
            .collect()
        )
        for row in latest:
            zone_renewable_ratio.labels(grid_zone=row["grid_zone"]).set(float(row["ratio"]))

        log.info(
            "zone windows upserted",
            extra={"stage": "speed-zone", "batch_id": batch_id, "rows_out": written},
        )
    finally:
        batch_df.unpersist()


# --------------------------------------------------------------------------------------
# Household running total and provisional bill (T090, T091)
# --------------------------------------------------------------------------------------


def household_aggregation(valid_df: DataFrame) -> DataFrame:
    """Per-household running totals over a 1-simulated-day event-time window.

    Grouped by a day *window* rather than by a derived `sim_date` column, per D3. The
    window carries an end, so the watermark can evict the previous day's keys shortly
    after simulated midnight. A derived date column has no end and its state would grow
    without bound for the life of the query.

    Expects `deduplicated()` output: that is where the watermark is set.
    """
    return (
        valid_df.groupBy(F.window(F.col("event_ts"), "1 day"), F.col("household_id"))
        .agg(
            F.sum("consumption_kwh").cast(_KWH).alias("consumption_kwh"),
            F.sum("solar_generation_kwh").cast(_KWH).alias("solar_kwh"),
        )
        .select(
            F.col("household_id"),
            F.to_date(F.col("window.start")).alias("sim_date"),
            F.col("consumption_kwh"),
            F.col("solar_kwh"),
        )
    )


def _tariff_for(spark: SparkSession, effective: date) -> DataFrame | None:
    """Yesterday's tariff file, cached. Returns None when it is not there yet.

    Read by `streaming.reference.read_tariff`, the billing job's reader too (R22), so both
    layers price a household with the same validated rates and the same subsidy flag.
    """
    if effective in _tariff_cache:
        return _tariff_cache[effective]

    path = landing_tariff_path(effective)
    try:
        frame = read_tariff(spark, effective, path)
        frame.cache()
        frame.count()
    except Exception as exc:  # noqa: BLE001 - missing (not dropped yet) or invalid: no bill
        log.warning(
            "tariff file not available",
            extra={"stage": "speed-household", "path": path, "detail": str(exc)[:200]},
        )
        return None

    _tariff_cache[effective] = frame
    # One day of tariffs is all that is ever needed; holding more would leak a cached
    # DataFrame per simulated day for the life of the query.
    for stale in [d for d in _tariff_cache if d < effective - timedelta(days=1)]:
        _tariff_cache.pop(stale).unpersist()
    return frame


def _rate_cols() -> tuple[Column, Column, Column]:
    return (F.col("block_1_rate"), F.col("block_2_rate"), F.col("block_3_rate"))


def with_provisional_bill(totals_df: DataFrame, tariff_df: DataFrame, effective: date) -> DataFrame:
    """Join yesterday's tariff and compute every bill component (D4).

    All of them are persisted, not just the total: the merge function and the
    reconciliation metric both need the breakdown to explain *why* the speed estimate and
    the batch final differ, and recomputing it later from the total is impossible.
    """
    joined = totals_df.join(F.broadcast(tariff_df), on="household_id", how="inner")

    bill = compute_bill_expr(
        F.col("consumption_kwh"),
        F.col("solar_kwh"),
        rate_cols=_rate_cols(),
        fixed_charge_col=F.col("fixed_charge"),
        subsidy_flag_col=F.col("subsidy_flag"),
        subsidy_pct_col=F.col("subsidy_pct"),
        export_rate_col=F.col("export_rate"),
        # Structure only, never rates (D2); the billing job reads the same (R26).
        boundaries=get_config().tariff.boundaries(),
    )

    return joined.select(
        F.col("household_id"),
        F.col("sim_date"),
        F.col("consumption_kwh"),
        F.col("solar_kwh"),
        bill.self_consumed_kwh.alias("self_consumed_kwh"),
        bill.billable_import_kwh.alias("billable_import_kwh"),
        bill.export_kwh.alias("export_kwh"),
        bill.energy_charge.alias("energy_charge"),
        bill.fixed_charge.alias("fixed_charge"),
        bill.subsidy_discount.alias("subsidy_discount"),
        bill.export_credit.alias("export_credit"),
        bill.tier_breakdown.alias("tier_breakdown"),
        bill.final_bill.alias("estimated_bill"),
        F.lit(effective).cast("date").alias("tariff_source_date"),
    )


def _write_household_batch(batch_df: DataFrame, batch_id: int, *, spark: SparkSession) -> None:
    batch_df.persist()
    try:
        written = 0
        # A batch can straddle simulated midnight, so group by the day it belongs to and
        # cost each with that day's predecessor rather than assuming one tariff per batch.
        for row in batch_df.select("sim_date").distinct().collect():
            sim_date: date = row["sim_date"]
            effective = sim_date - timedelta(days=1)
            tariff = _tariff_for(spark, effective)
            if tariff is None:
                # Day zero, before any tariff has been dropped. Skipping is correct:
                # writing a bill with invented rates would be worse than writing none.
                log.info(
                    "no tariff yet, skipping provisional bill",
                    extra={
                        "stage": "speed-household",
                        "batch_id": batch_id,
                        "sim_date": sim_date.isoformat(),
                        "wanted": effective.isoformat(),
                    },
                )
                continue

            day_df = batch_df.filter(F.col("sim_date") == F.lit(sim_date))
            written += upsert_batch(
                with_provisional_bill(day_df, tariff, effective),
                "household_running_rt",
                conflict_cols=("household_id", "sim_date"),
            )

        log.info(
            "household totals upserted",
            extra={"stage": "speed-household", "batch_id": batch_id, "rows_out": written},
        )
    finally:
        batch_df.unpersist()


# --------------------------------------------------------------------------------------
# Validation split, metrics and DLQ (T092, T093)
# --------------------------------------------------------------------------------------


def _write_validation_batch(
    batch_df: DataFrame,
    batch_id: int,
    *,
    known_households: frozenset[str],
    bounds: tuple,
) -> None:
    """Split the raw batch, dead-letter the rejects, and reconcile the counts (T093)."""
    batch_df.persist()
    try:
        rows_in = batch_df.count()
        if not rows_in:
            return

        valid_df, invalid_df = split_valid_invalid(
            batch_df, known_household_ids=known_households, event_ts_bounds=bounds
        )
        rows_rejected = write_rejected(invalid_df, _LAYER)
        rows_out = rows_in - rows_rejected

        # Latency is observed at the zone sink, and consumer lag by KafkaLagListener —
        # this query writes neither view, so neither belongs here.
        events_consumed_total.labels(layer=_LAYER).inc(rows_in)

        # rows_in == rows_out + rows_rejected, by construction. Logged so the identity is
        # checkable from the logs rather than taken on trust.
        log.info(
            "validation batch",
            extra={
                "stage": "speed-validate",
                "batch_id": batch_id,
                "rows_in": rows_in,
                "rows_out": rows_out,
                "rows_rejected": rows_rejected,
            },
        )
    finally:
        batch_df.unpersist()


# --------------------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------------------


def start(await_termination: bool = True) -> list[StreamingQuery]:
    config = get_config()
    spark = build_session("voltstream-speed-layer")
    spark.sparkContext.setLogLevel("WARN")
    # All three queries read the topic; the gauge reports the slowest per partition.
    spark.streams.addListener(
        KafkaLagListener({q: _LAYER for q in (_ZONE_JOB, _HOUSEHOLD_JOB, _VALIDATION_JOB)})
    )

    known_households = known_household_ids()
    bounds = event_ts_bounds()

    # One source per query. Each gets its own Spark-generated consumer group and, more to
    # the point, its own checkpoint — which is where offsets actually live and what makes
    # the branches independent (§5.2).
    #
    # The cost is that each query reads the topic separately. At this volume (~7,500
    # events per simulated day) that is cheap, and it buys genuinely independent failure:
    # the zone view keeps serving if the household query dies.
    def valid_stream() -> DataFrame:
        valid, _ = split_valid_invalid(
            read_meter_stream(spark),
            known_household_ids=known_households,
            event_ts_bounds=bounds,
        )
        # Watermarked and deduplicated once here, so both aggregations see the same
        # records the batch layer would bill (R02).
        return deduplicated(valid)

    trigger = f"{config.speed_layer.trigger_interval_real_seconds} seconds"
    output_mode = config.speed_layer.output_mode

    zone_query = (
        zone_aggregation(valid_stream())
        .writeStream.queryName(_ZONE_JOB)
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_path(_ZONE_JOB))
        .trigger(processingTime=trigger)
        .foreachBatch(_write_zone_batch)
        .start()
    )

    household_query = (
        household_aggregation(valid_stream())
        .writeStream.queryName(_HOUSEHOLD_JOB)
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_path(_HOUSEHOLD_JOB))
        .trigger(processingTime=trigger)
        .foreachBatch(lambda df, bid: _write_household_batch(df, bid, spark=spark))
        .start()
    )

    # A third query over the raw stream handles validation, metrics and the DLQ. It is
    # separate because the two aggregations consume only valid rows by construction, so
    # neither of them ever sees a rejected record to dead-letter.
    validation_query = (
        read_meter_stream(spark)
        .writeStream.queryName(_VALIDATION_JOB)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path(_VALIDATION_JOB))
        .trigger(processingTime=trigger)
        .foreachBatch(
            lambda df, bid: _write_validation_batch(
                df, bid, known_households=known_households, bounds=bounds
            )
        )
        .start()
    )

    queries = [zone_query, household_query, validation_query]
    log.info(
        "speed layer started",
        extra={
            "stage": "speed",
            "window_sim_minutes": config.speed_layer.window_sim_minutes,
            "watermark_sim_minutes": config.speed_layer.watermark_sim_minutes,
            "trigger_real_seconds": config.speed_layer.trigger_interval_real_seconds,
            "output_mode": output_mode,
        },
    )

    if await_termination:
        for query in queries:
            query.awaitTermination()
    return queries


def main() -> None:
    start_metrics_server()

    def _stop(signum: int, _frame: FrameType | None) -> None:
        log.info("shutdown requested", extra={"stage": "speed", "signal": signum})
        sys.exit(0)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    start()


if __name__ == "__main__":
    main()
