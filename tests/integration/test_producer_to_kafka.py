"""The meter producer, observed from Kafka (T171).

Consumes a sample of `meter.readings` as the running producer writes it and checks what
every downstream consumer relies on:

- **Schema.** Every message parses against the frozen contract (`MeterReading`, which
  forbids extra fields). Fault-injected records are still schema-valid: their faults are
  in the values, which is what validation downstream exists to catch.
- **Keying (§3.3d).** The Kafka key is the household id, so one household's readings stay
  on one partition and in order. The `trace_id` header matches the payload's (§10.3).
- **Fault rates.** Each injected fault appears at the rate `config/base.yaml` sets, within
  four binomial standard deviations. Null field, negative kWh and unknown household are
  read off the record itself; a duplicate is a repeated `event_id`; an out-of-order
  reading carries an `event_ts` no tick has.

Needs the Compose stack up, producer running. Reads about 3,000 messages: two minutes.
"""

from __future__ import annotations

import math
import uuid
from collections import Counter, defaultdict
from datetime import timedelta
from pathlib import Path

import pytest
from confluent_kafka import Consumer
from dotenv import dotenv_values

from voltstream.config import get_config
from voltstream.contracts.events import MeterReading

pytestmark = pytest.mark.integration

_ENV = dotenv_values(Path(__file__).resolve().parents[2] / ".env")
_SAMPLE = 3000
_TIMEOUT_S = 300
_UNKNOWN_HOUSEHOLD = "HH-9999"  # simulators/faults.py UNKNOWN_HOUSEHOLD_SENTINEL

# (Kafka key, partition, headers, parsed reading), one per message.
Sample = list[tuple[bytes | None, int, dict[str, bytes], MeterReading]]


def _consume(count: int) -> Sample:
    """`count` messages from the head of the topic: (key, partition, headers, reading)."""
    consumer = Consumer(
        {
            "bootstrap.servers": f"localhost:{_ENV.get('KAFKA_HOST_PORT') or '29092'}",
            "group.id": f"t171-{uuid.uuid4()}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([get_config().kafka.topic])
    out: Sample = []
    try:
        polls = 0
        while len(out) < count and polls < _TIMEOUT_S:
            polls += 1
            for message in consumer.consume(num_messages=500, timeout=1.0):
                if message.error():
                    raise AssertionError(f"Kafka error: {message.error()}")
                headers = {k: v for k, v in (message.headers() or [])}
                out.append(
                    (
                        message.key(),
                        message.partition(),
                        headers,
                        MeterReading.from_kafka_value(message.value()),
                    )
                )
    finally:
        consumer.close()
    assert len(out) >= count, (
        f"only {len(out)} messages in {_TIMEOUT_S} s: is meter-producer running?"
    )
    return out


@pytest.fixture(scope="module")
def sample() -> Sample:
    return _consume(_SAMPLE)


def _assert_rate(label: str, observed: int, trials: int, rate: float) -> None:
    """Within four binomial standard deviations of the configured rate, plus 0.3 percentage
    points for the sample's edges; a sample this size resolves a 1 % rate to about ±0.7 pp."""
    expected = trials * rate
    tolerance = 4 * math.sqrt(trials * rate * (1 - rate)) + 0.003 * trials
    assert abs(observed - expected) <= tolerance, (
        f"{label}: {observed} of {trials} ({observed / trials:.2%}), configured {rate:.2%} "
        f"(expected {expected:.0f} ± {tolerance:.0f})"
    )


def test_every_message_is_keyed_by_household_and_carries_its_trace_id(sample: Sample) -> None:
    partitions_of: dict[str, set[int]] = defaultdict(set)
    for key, partition, headers, reading in sample:
        assert (key or b"").decode("utf-8") == reading.household_id
        assert headers["trace_id"].decode("utf-8") == str(reading.trace_id)
        assert "produced_at" in headers
        partitions_of[reading.household_id].add(partition)

    # One household, one partition: the ordering guarantee a per-household key buys. Not
    # for the null-field fault's records: their household id, and so their key, is empty,
    # and librdkafka's default partitioner (consistent_random) deliberately spreads records
    # with an empty key at random. They are invalid and carry no household to keep in order.
    keyed = {h: p for h, p in partitions_of.items() if h != ""}
    assert all(len(p) == 1 for p in keyed.values()), {h: p for h, p in keyed.items() if len(p) > 1}
    # And the load is actually spread.
    used = {partition for _, partition, _, _ in sample}
    assert used == set(range(get_config().kafka.partitions))


def test_the_value_based_faults_occur_at_their_configured_rates(sample: Sample) -> None:
    faults = get_config().faults
    unique = {reading.event_id: reading for _, _, _, reading in sample}
    readings = list(unique.values())

    _assert_rate(
        "null field",
        sum(r.household_id == "" for r in readings),
        len(readings),
        faults.null_field_rate,
    )
    _assert_rate(
        "negative kWh",
        sum(r.consumption_kwh < 0 or r.solar_generation_kwh < 0 for r in readings),
        len(readings),
        faults.negative_value_rate,
    )
    _assert_rate(
        "unknown household",
        sum(r.household_id == _UNKNOWN_HOUSEHOLD for r in readings),
        len(readings),
        faults.unknown_household_rate,
    )


def test_duplicates_occur_at_the_configured_rate(sample: Sample) -> None:
    """A duplicate is the same record sent twice, event_id included (faults.py)."""
    sends = Counter(reading.event_id for _, _, _, reading in sample)
    duplicates = sum(n - 1 for n in sends.values())
    _assert_rate("duplicate", duplicates, len(sends), get_config().faults.duplicate_rate)


def test_out_of_order_readings_occur_at_the_configured_rate(sample: Sample) -> None:
    """Every reading of one producer tick shares that tick's `event_ts`; an out-of-order
    reading had its `event_ts` shifted back by a random 1-30 simulated minutes, so it
    matches no tick. A dropout's backfill does match a tick, just an earlier one, so it is
    not miscounted. The first 30 simulated minutes of the sample are left out: an
    out-of-order reading from before the sample began would look tick-less there too."""
    config = get_config()
    unique = {reading.event_id: reading for _, _, _, reading in sample}
    per_ts = Counter(r.event_ts for r in unique.values())
    ticks = {ts for ts, n in per_ts.items() if n >= 10}
    assert len(ticks) >= 30, f"only {len(ticks)} ticks in the sample"

    _, high = config.faults.out_of_order_lateness_sim_minutes
    window_start = min(ticks) + timedelta(minutes=high)
    in_window = [r for r in unique.values() if r.event_ts >= window_start]
    # Matching no tick exactly is the signature: tick timestamps carry microseconds, so a
    # random shift never lands on one. (Its distance to the *nearest* tick proves nothing:
    # ticks are 9.6 simulated minutes apart, so a 1-30 minute shift can land next to an
    # earlier one.)
    shifted = [r for r in in_window if r.event_ts not in ticks]
    _assert_rate("out of order", len(shifted), len(in_window), config.faults.out_of_order_rate)
