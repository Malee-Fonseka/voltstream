"""The streaming event contract (decision T029, §6.2) — `meter.readings`.

Frozen at the end of Phase 1 and changed only by explicit agreement, because the
producer, the speed layer and the batch layer all build against this shape independently
(`T033`'s drift guard is what makes "frozen" mean something mechanically, not just by
convention).

**`event_ts` is SIMULATED time (§3.4), not wall-clock time, and MUST NOT be used to
measure latency.** `voltstream_e2e_latency_seconds` is measured from the Kafka record
timestamp instead (decision T030) — that field is wall-clock by construction and needs no
new field on this frozen contract. Use `event_ts` only for windowing and business dates
(`simclock.sim_date_of(event_ts)`); never subtract it from `datetime.now()`.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_serializer


class MeterReading(BaseModel):
    """One smart-meter reading, as published to Kafka (key = `household_id`)."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str
    event_id: UUID
    trace_id: UUID
    meter_id: str
    household_id: str
    grid_zone: str
    # Simulated time — see the module docstring. Rejects naive datetimes.
    event_ts: AwareDatetime
    # D5: kWh precision is (12, 4) everywhere — Python Decimal, Spark DecimalType,
    # Postgres NUMERIC, Parquet DECIMAL. A value with more than 4 decimal places raises.
    consumption_kwh: Decimal = Field(max_digits=12, decimal_places=4)
    solar_generation_kwh: Decimal = Field(max_digits=12, decimal_places=4)
    # Deliberately unused by billing (§6.2) — demonstrates Parquet column pruning (T111).
    voltage: float
    producer_id: str

    @field_serializer("consumption_kwh", "solar_generation_kwh", when_used="json")
    def _serialize_kwh_as_number(self, value: Decimal) -> float:
        """Emit kWh fields as JSON numbers, not strings, so the §6.2 sample stays valid
        and Spark's `from_json` parses the exact decimal text against an explicit
        `DecimalType(12, 4)` schema (D5) rather than tripping over a quoted value."""
        return float(value)

    @classmethod
    def from_kafka_value(cls, value: bytes) -> MeterReading:
        return cls.model_validate_json(value)

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json().encode("utf-8")
