"""Kafka source and validation split for both streaming jobs (T078, T079, §8.3).

The schema is **declared, never inferred**. `inferSchema` is not available on a streaming
source at all, and even where Spark would guess, a guess is the wrong mechanism for a
frozen contract (`contracts/events.py`): a day where every reading happens to have a whole
number of kWh would infer `LongType` and silently truncate every bill thereafter.

`includeHeaders=true` is set explicitly because it is **off by default** and two things
depend on it — `trace_id` propagation (§10.3) and nothing else in the pipeline carries it,
since the contract is frozen and T030 chose the Kafka record timestamp over a new field.

**The validation rules here duplicate `core/validation.py`** — Python predicates over a
`MeterReading` there, Spark `Column` expressions here — for the same reason
`core/spark_expr.py` duplicates `core/tariff.py`: a per-row Python UDF would serialise
every record and defeat the query optimiser. The duplication is bounded by importing
`REJECTION_REASONS` from `core.validation` so the reason *vocabulary* cannot drift, and by
`_assert_reason_coverage()` below, which fails at import time if a reason is added there
and not handled here.
"""

from __future__ import annotations

import json
from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.listener import (
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
)
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from voltstream.config import get_config
from voltstream.core.validation import REJECTION_REASONS
from voltstream.metrics import consumer_lag

# D5: kWh precision is (12, 4) everywhere. `voltage` stays a double — §6.2 marks it
# deliberately unused by billing, so it never enters money arithmetic.
KWH_TYPE = DecimalType(12, 4)

# Every field is declared nullable, deliberately. The contract in `contracts/events.py`
# forbids nulls, but `from_json` does **not** enforce nullability: a payload with a null
# field, or one that fails to parse, yields nulls whatever this schema claims. Declaring
# `nullable=False` here would be a promise Spark does not keep, and an optimiser entitled
# to trust it could prune the very null check that `null_field` rejection depends on.
# Nulls are rejected by `split_valid_invalid`, which is where the contract is enforced.
METER_READING_SCHEMA = StructType(
    [
        StructField("schema_version", StringType()),
        StructField("event_id", StringType()),
        StructField("trace_id", StringType()),
        StructField("meter_id", StringType()),
        StructField("household_id", StringType()),
        StructField("grid_zone", StringType()),
        # SIMULATED time. Windowing and sim_date only — never latency (T030).
        StructField("event_ts", TimestampType()),
        StructField("consumption_kwh", KWH_TYPE),
        StructField("solar_generation_kwh", KWH_TYPE),
        StructField("voltage", DoubleType()),
        StructField("producer_id", StringType()),
    ]
)

# Kafka metadata carried alongside every contract field. `kafka_timestamp` is the wall
# clock T030 selected for latency; `partition`/`offset` support the consumer-lag gauge.
_KAFKA_METADATA_COLUMNS = ("kafka_key", "kafka_timestamp", "kafka_partition", "kafka_offset")


def read_meter_stream(spark: SparkSession, *, starting_offsets: str = "earliest") -> DataFrame:
    """Read `meter.readings` as a stream, parsed against the frozen contract.

    `starting_offsets` applies only on the very first run of a query; afterwards the
    checkpoint's committed offsets win, which is exactly the behaviour the restart test
    exercises.

    The Kafka consumer group is deliberately left to Spark, which generates a unique one
    per query. Setting `kafka.group.id` looked attractive for making the two branches
    visible to `kafka-consumer-groups`, but it does nothing of the sort: this source
    *assigns* partitions directly rather than subscribing, so no group is ever registered
    with the coordinator and `--describe` reports it does not exist. Spark also warns that
    a shared group id makes concurrent queries interfere. Branch independence is real, and
    it is visible where it actually lives — each query's own checkpoint (§5.2).
    """
    config = get_config()

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka.bootstrap_servers)
        .option("subscribe", config.kafka.topic)
        .option("startingOffsets", starting_offsets)
        # Off by default; trace_id lives here and nowhere else. See module docstring.
        .option("includeHeaders", "true")
        # A lost offset must be loud, not silently skipped to the newest record.
        .option("failOnDataLoss", "true")
    )

    return (
        reader.load()
        .select(
            F.from_json(F.col("value").cast("string"), METER_READING_SCHEMA).alias("r"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("headers").alias("kafka_headers"),
        )
        .select("r.*", *_KAFKA_METADATA_COLUMNS, "kafka_headers")
    )


def kafka_lag_by_partition(end_offset: str | None, latest_offset: str | None) -> dict[str, int]:
    """Per-partition lag of one Kafka source, from a query's progress report.

    Both arguments are offset maps as Spark reports them — `{"meter.readings": {"0": 1234}}`
    — where `end_offset` is the next offset the query will read and `latest_offset` the
    next one Kafka will write. The difference is the number of records published but not
    yet processed. Empty when either side is missing: before a query has fetched anything
    Spark reports the offsets as the string "None".
    """
    try:
        end = json.loads(end_offset) if end_offset else None
        latest = json.loads(latest_offset) if latest_offset else None
    except ValueError:
        return {}
    if not isinstance(end, dict) or not isinstance(latest, dict):
        return {}

    lag: dict[str, int] = {}
    for topic, newest_by_partition in latest.items():
        processed = end.get(topic)
        if not isinstance(newest_by_partition, dict) or not isinstance(processed, dict):
            continue
        for partition, newest in newest_by_partition.items():
            if partition in processed:
                lag[partition] = max(0, int(newest) - int(processed[partition]))
    return lag


class KafkaLagListener(StreamingQueryListener):
    """Keeps `voltstream_consumer_lag{layer, partition}` current from query progress.

    Spark's Kafka source assigns partitions itself and commits no offsets to Kafka (see
    `read_meter_stream`), so `kafka-consumer-groups` has nothing to report for these
    queries. Their own progress events are where the lag lives: each carries, per source,
    the offsets the micro-batch ended at and the newest offsets Kafka held.

    A layer can run several queries over the topic — the speed layer runs three — and the
    gauge reports the worst of them per partition, because "is this branch falling behind"
    is answered by its slowest query. Progress events arrive one at a time on Spark's
    listener thread, so the bookkeeping needs no lock.
    """

    def __init__(self, layer_by_query: dict[str, str]) -> None:
        super().__init__()
        self._layer_by_query = dict(layer_by_query)
        self._lag_by_query: dict[str, dict[str, int]] = {}

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        """Nothing to report before the first micro-batch."""

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        progress = event.progress
        query = progress.name  # None for an unnamed query, which is never one of ours
        layer = self._layer_by_query.get(query) if query is not None else None
        if query is None or layer is None:
            return

        lag: dict[str, int] = {}
        for source in progress.sources:
            lag.update(kafka_lag_by_partition(source.endOffset, source.latestOffset))
        if not lag:
            return
        self._lag_by_query[query] = lag

        for partition in lag:
            worst = max(
                query_lag.get(partition, 0)
                for query, query_lag in self._lag_by_query.items()
                if self._layer_by_query[query] == layer
            )
            consumer_lag.labels(layer=layer, partition=partition).set(worst)

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        """Nothing to do: the job exits with its query, and the scrape target with it."""


def trace_id_from_headers(headers_col: str = "kafka_headers") -> Column:
    """Extract `trace_id` from the Kafka record headers.

    Headers arrive as an array of `{key, value}` structs; the value is binary. Falls back
    to the in-payload `trace_id` when the header is absent, so a record produced before
    header propagation existed still correlates.
    """
    header_value = F.filter(F.col(headers_col), lambda h: h.getField("key") == F.lit("trace_id"))
    return F.coalesce(
        F.element_at(header_value, 1).getField("value").cast("string"),
        F.col("trace_id"),
    )


def _assert_reason_coverage(handled: set[str]) -> None:
    """Fail at import time if `core.validation` grows a reason this module ignores.

    Without this the new reason would simply never be emitted here, and the rows it
    describes would be silently classified valid — a data-quality regression that no test
    over existing reasons would catch.
    """
    missing = REJECTION_REASONS - handled
    if missing:
        raise RuntimeError(
            f"streaming/sources.py does not handle rejection reason(s): {sorted(missing)}. "
            "Add a Column expression for each, in the same order as core/validation.py."
        )


def split_valid_invalid(
    df: DataFrame,
    *,
    known_household_ids: frozenset[str],
    event_ts_bounds: tuple[datetime, datetime],
    configured_zones: frozenset[str] | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Partition a meter-reading DataFrame into `(valid_df, invalid_df)`.

    `invalid_df` carries a `reason` column from the `core.validation` vocabulary and the
    original record as JSON in `payload`, which is what `rejected_records` stores (§6.4)
    and what makes a rejection reproducible after the fact.

    Rules are evaluated in the same order as `core.validation.validate()`, so a row
    failing two rules is labelled with the same reason in both paths — otherwise the
    batch and speed layers would disagree about *why* a record was rejected even when
    they agree that it was.
    """
    zones = configured_zones or frozenset(get_config().simulation.zones)
    ts_low, ts_high = event_ts_bounds

    is_null_field = (
        F.col("household_id").isNull()
        | (F.col("household_id") == "")
        | F.col("meter_id").isNull()
        | (F.col("meter_id") == "")
        | F.col("grid_zone").isNull()
        | (F.col("grid_zone") == "")
        | F.col("event_ts").isNull()
        | F.col("consumption_kwh").isNull()
        | F.col("solar_generation_kwh").isNull()
    )
    is_negative = (F.col("consumption_kwh") < 0) | (F.col("solar_generation_kwh") < 0)
    is_unknown_household = ~F.col("household_id").isin(list(known_household_ids))
    is_unknown_zone = ~F.col("grid_zone").isin(list(zones))
    is_ts_out_of_range = (F.col("event_ts") < F.lit(ts_low)) | (F.col("event_ts") > F.lit(ts_high))

    _assert_reason_coverage(
        {
            "null_field",
            "negative_kwh",
            "unknown_household",
            "unknown_zone",
            "event_ts_out_of_range",
        }
    )

    # First matching rule wins, mirroring validate()'s early returns.
    reason = (
        F.when(is_null_field, F.lit("null_field"))
        .when(is_negative, F.lit("negative_kwh"))
        .when(is_unknown_household, F.lit("unknown_household"))
        .when(is_unknown_zone, F.lit("unknown_zone"))
        .when(is_ts_out_of_range, F.lit("event_ts_out_of_range"))
        .otherwise(F.lit(None).cast(StringType()))
    )

    labelled = df.withColumn("reason", reason)

    valid_df = labelled.filter(F.col("reason").isNull()).drop("reason")

    # The payload captures the contract columns **this caller actually read**, not every
    # column the contract defines. The speed layer reads the whole event; the batch job
    # prunes to the six columns billing needs (T111), so naming all eleven here fails to
    # resolve against its DataFrame. Intersecting keeps one validation path usable from
    # both layers, which is the point of sharing it.
    #
    # The consequence is honest and worth knowing: a batch-stage rejection stores a
    # narrower payload than a speed-stage one. The raw event is still in the master
    # dataset either way, so nothing is unrecoverable — `rejected_records` is the index,
    # not the archive.
    present = [f.name for f in METER_READING_SCHEMA.fields if f.name in df.columns]
    invalid_df = labelled.filter(F.col("reason").isNotNull()).withColumn(
        "payload", F.to_json(F.struct(*present))
    )

    return valid_df, invalid_df
