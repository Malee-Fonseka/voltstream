"""Unit tests for the streaming validation split (T079).

One row per rejection reason, plus a valid row, through `split_valid_invalid` over a
static DataFrame. The point is not that rejection works at all, but that each row is
labelled with the *same* reason `core.validation.validate()` would give it — the two
implementations are separate code and are only kept honest by tests like this one.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from voltstream.core.validation import REJECTION_REASONS
from voltstream.streaming import sources
from voltstream.streaming.sources import (
    METER_READING_SCHEMA,
    KafkaLagListener,
    kafka_lag_by_partition,
    split_valid_invalid,
)

_KNOWN_HOUSEHOLDS = frozenset({"HH-0001", "HH-0002"})
_ZONES = frozenset({"ZONE-A", "ZONE-B"})
_TS_LOW = datetime(2026, 1, 1, tzinfo=UTC)
_TS_HIGH = datetime(2026, 12, 31, tzinfo=UTC)
_IN_RANGE = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


def _row(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "schema_version": "1.0",
        "event_id": "11111111-1111-4111-8111-111111111111",
        "trace_id": "22222222-2222-4222-8222-222222222222",
        "meter_id": "MTR-0001",
        "household_id": "HH-0001",
        "grid_zone": "ZONE-A",
        "event_ts": _IN_RANGE,
        "consumption_kwh": Decimal("0.4120"),
        "solar_generation_kwh": Decimal("0.1800"),
        "voltage": 232.4,
        "producer_id": "sim-01",
    }
    base.update(overrides)
    return base


# (case name, overrides, expected reason or None for a valid row)
_CASES = [
    ("valid", {}, None),
    ("null_field", {"household_id": None}, "null_field"),
    ("negative_kwh", {"consumption_kwh": Decimal("-1.0000")}, "negative_kwh"),
    ("unknown_household", {"household_id": "HH-9999"}, "unknown_household"),
    ("unknown_zone", {"grid_zone": "ZONE-Z"}, "unknown_zone"),
    (
        "event_ts_out_of_range",
        {"event_ts": datetime(2020, 1, 1, tzinfo=UTC)},
        "event_ts_out_of_range",
    ),
]


@pytest.fixture(scope="module")
def split(spark):  # type: ignore[no-untyped-def]
    rows = [_row(**overrides) for _, overrides, _ in _CASES]
    df = spark.createDataFrame(rows, schema=METER_READING_SCHEMA)
    valid_df, invalid_df = split_valid_invalid(
        df,
        known_household_ids=_KNOWN_HOUSEHOLDS,
        event_ts_bounds=(_TS_LOW, _TS_HIGH),
        configured_zones=_ZONES,
    )
    return valid_df.collect(), invalid_df.collect()


def test_every_reason_is_covered_by_a_case() -> None:
    """If core/validation.py grows a reason, this test list must grow with it."""
    assert {r for _, _, r in _CASES if r} == set(REJECTION_REASONS)


def test_valid_row_passes(split) -> None:  # type: ignore[no-untyped-def]
    valid, _ = split
    assert len(valid) == 1
    assert valid[0]["household_id"] == "HH-0001"


def test_each_invalid_row_gets_the_right_reason(split) -> None:  # type: ignore[no-untyped-def]
    _, invalid = split
    expected = {reason for _, _, reason in _CASES if reason}
    assert {row["reason"] for row in invalid} == expected


def test_invalid_rows_carry_the_original_payload(split) -> None:  # type: ignore[no-untyped-def]
    """`rejected_records.payload` must be enough to reproduce the rejection later."""
    _, invalid = split
    assert all(row["payload"] and "meter_id" in row["payload"] for row in invalid)


def test_rules_are_evaluated_in_validate_order(spark) -> None:  # type: ignore[no-untyped-def]
    """A row breaking two rules is labelled by the first, as `validate()` returns early.

    Without this, the speed and batch layers could agree a record is bad but disagree why,
    which shows up as an unexplainable split in `records_rejected_total{reason=...}`.
    """
    both = _row(household_id=None, consumption_kwh=Decimal("-1.0000"))
    df = spark.createDataFrame([both], schema=METER_READING_SCHEMA)
    _, invalid_df = split_valid_invalid(
        df,
        known_household_ids=_KNOWN_HOUSEHOLDS,
        event_ts_bounds=(_TS_LOW, _TS_HIGH),
        configured_zones=_ZONES,
    )
    assert invalid_df.collect()[0]["reason"] == "null_field"


def test_split_works_on_a_pruned_frame(spark) -> None:  # type: ignore[no-untyped-def]
    """The batch job reads six columns, not eleven, and must still be able to split.

    Regression test. The payload was built by naming every field in the contract, which
    fails to resolve against a frame that pruned columns for performance (T111) — the
    billing job died with UNRESOLVED_COLUMN on `schema_version`. Sharing one validation
    path between the layers only works if it adapts to what each of them read.
    """
    pruned_fields = [
        f
        for f in METER_READING_SCHEMA.fields
        if f.name
        in {
            "meter_id",
            "household_id",
            "grid_zone",
            "event_ts",
            "consumption_kwh",
            "solar_generation_kwh",
        }
    ]
    from pyspark.sql.types import StructType

    schema = StructType(pruned_fields)
    df = spark.createDataFrame(
        [
            ("MTR-1", "HH-0001", "ZONE-A", _IN_RANGE, Decimal("1.0000"), Decimal("0.0000")),
            ("MTR-2", "HH-9999", "ZONE-A", _IN_RANGE, Decimal("1.0000"), Decimal("0.0000")),
        ],
        schema=schema,
    )

    valid_df, invalid_df = split_valid_invalid(
        df,
        known_household_ids=_KNOWN_HOUSEHOLDS,
        event_ts_bounds=(_TS_LOW, _TS_HIGH),
        configured_zones=_ZONES,
    )

    assert valid_df.count() == 1
    rejected = invalid_df.collect()
    assert len(rejected) == 1
    assert rejected[0]["reason"] == "unknown_household"
    # The payload holds what was read, and says so rather than failing.
    assert "meter_id" in rejected[0]["payload"]
    assert "schema_version" not in rejected[0]["payload"]


# ---------------------------------------------------------------------------
# Consumer lag (R06): from query progress, not from the micro-batch.
# ---------------------------------------------------------------------------


def test_kafka_lag_is_newest_minus_processed_per_partition() -> None:
    end = '{"meter.readings": {"0": 100, "1": 250, "2": 7}}'
    latest = '{"meter.readings": {"0": 130, "1": 250, "2": 9}}'
    assert kafka_lag_by_partition(end, latest) == {"0": 30, "1": 0, "2": 2}


@pytest.mark.parametrize(
    "end,latest",
    [
        # Spark reports a missing offset map as the string "None" (str() of a Java null).
        ("None", '{"t": {"0": 1}}'),
        ('{"t": {"0": 1}}', "None"),
        (None, None),
        ("", ""),
    ],
)
def test_kafka_lag_is_empty_until_both_offsets_exist(end: str | None, latest: str | None) -> None:
    assert kafka_lag_by_partition(end, latest) == {}


def _progress(query: str, end: str, latest: str) -> SimpleNamespace:
    """The shape of a QueryProgressEvent, as far as the listener reads it."""
    source = SimpleNamespace(endOffset=end, latestOffset=latest)
    return SimpleNamespace(progress=SimpleNamespace(name=query, sources=[source]))


def _lag_gauge(layer: str, partition: str) -> float | None:
    for metric in sources.consumer_lag.collect():
        for sample in metric.samples:
            if sample.labels == {"layer": layer, "partition": partition}:
                return float(sample.value)
    return None


def test_listener_reports_the_slowest_query_of_a_layer() -> None:
    """The speed layer runs three queries over the topic; the gauge is the worst of them."""
    listener = KafkaLagListener({"fast": "lagtest", "slow": "lagtest"})

    listener.onQueryProgress(_progress("fast", '{"t": {"0": 90}}', '{"t": {"0": 100}}'))
    listener.onQueryProgress(_progress("slow", '{"t": {"0": 40}}', '{"t": {"0": 100}}'))
    assert _lag_gauge("lagtest", "0") == 60

    # The slow query catches up; the gauge now follows the other one.
    listener.onQueryProgress(_progress("slow", '{"t": {"0": 100}}', '{"t": {"0": 100}}'))
    assert _lag_gauge("lagtest", "0") == 10


def test_listener_ignores_queries_it_was_not_given() -> None:
    listener = KafkaLagListener({"mine": "lagtest-ignored"})
    listener.onQueryProgress(_progress("not-mine", '{"t": {"0": 0}}', '{"t": {"0": 5}}'))
    assert _lag_gauge("lagtest-ignored", "0") is None
