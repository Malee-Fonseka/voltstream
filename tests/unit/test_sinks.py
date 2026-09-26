"""Unit tests for the e2e-latency helper in streaming/sinks.py (R21).

The Postgres and Kafka sinks themselves need the stack and are covered by the integration
tests; this is the part that is pure Spark.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pyspark.sql.types import StructField, StructType, TimestampType

from voltstream.streaming import sinks

_SCHEMA = StructType([StructField("kafka_timestamp", TimestampType())])


def _latency(layer: str) -> tuple[float, float]:
    """(count, sum) of voltstream_e2e_latency_seconds{layer=...}."""
    count = total = 0.0
    for metric in sinks.e2e_latency_seconds.collect():
        for sample in metric.samples:
            if sample.labels.get("layer") != layer:
                continue
            if sample.name.endswith("_count"):
                count = sample.value
            elif sample.name.endswith("_sum"):
                total = sample.value
    return count, total


def test_latency_is_now_minus_the_kafka_timestamp_to_the_microsecond(spark) -> None:  # type: ignore[no-untyped-def]
    produced = datetime.now(UTC) - timedelta(seconds=1.5)
    df = spark.createDataFrame([(produced,)], schema=_SCHEMA)

    assert sinks.observe_e2e_latency(df, "kafka_timestamp", "test-subsecond") == 1

    count, seconds = _latency("test-subsecond")
    assert count == 1
    # At least the 1.5 s already elapsed; a generous ceiling for a cold Spark job.
    assert 1.5 <= seconds < 60
    # unix_timestamp() truncated to whole seconds; the double cast keeps the fraction.
    assert seconds != int(seconds)


def test_missing_and_future_timestamps_are_not_observed(spark) -> None:  # type: ignore[no-untyped-def]
    """A null timestamp has no latency, and one ahead of the driver's clock is skew, not a
    negative latency — neither may reach the histogram."""
    future = datetime.now(UTC) + timedelta(hours=1)
    df = spark.createDataFrame([(None,), (future,)], schema=_SCHEMA)

    assert sinks.observe_e2e_latency(df, "kafka_timestamp", "test-skipped") == 0
    assert _latency("test-skipped") == (0.0, 0.0)
