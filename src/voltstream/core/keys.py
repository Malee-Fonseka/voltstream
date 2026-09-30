"""Key derivation (decision T051, §3.3d) — the ONE place that defines what a duplicate
reading is, what a Kafka/storage partition key is, and how a reading maps onto a Parquet
partition. The streaming and batch jobs both import these functions so they cannot
disagree about any of the three.
"""

from __future__ import annotations

from datetime import date, datetime

from voltstream import simclock
from voltstream.contracts.events import MeterReading

DedupKey = tuple[str, datetime]

# The same key as column names, for the Spark jobs: the batch layer's window dedup and the
# speed layer's streaming dedup both use this, so they cannot disagree about a duplicate.
DEDUP_COLUMNS: tuple[str, str] = ("meter_id", "event_ts")


def dedup_key(reading: MeterReading) -> DedupKey:
    """Two readings are the same event iff they share `(meter_id, event_ts)` — not
    `event_id`, which is a randomly generated UUID per emission and would treat every
    retransmission of the same physical reading as a distinct one (§3.3d)."""
    return (reading.meter_id, reading.event_ts)


def partition_key(reading: MeterReading) -> str:
    """The Kafka message key and the natural per-entity partition key: `household_id`."""
    return reading.household_id


def parquet_partition(reading: MeterReading) -> tuple[date, int]:
    """The `(sim_date, hour)` pair `raw_archiver.py` partitions `voltstream-raw` by,
    derived from the reading's simulated `event_ts` via the one clock module (simclock)."""
    return (simclock.sim_date_of(reading.event_ts), simclock.sim_hour_of(reading.event_ts))
