"""Pure validation rules for a `MeterReading` (decision T053).

Returns a structured result rather than raising, because a rejected reading is a normal,
expected outcome that has to be routed (to `rejected_records` and the DLQ topic, §8.3),
not an exceptional one.

`reason` values come from the single fixed vocabulary in `REJECTION_REASONS` below — the
same strings `rejected_records.reason` stores and the same strings the
`voltstream_records_rejected_total{reason=...}` metric label uses. This module is the
only place those strings are allowed to appear (enforced by grepping for them elsewhere).

Correspondence with the injected faults in `simulators/faults.py` (§9 Phase 1) — not every
fault is a validation rejection:

| `faults.*` rate | Outcome |
|---|---|
| `null_field_rate` | rejected here, reason `null_field` |
| `negative_value_rate` | rejected here, reason `negative_kwh` |
| `unknown_household_rate` | rejected here, reason `unknown_household` |
| `duplicate_rate` | **not** rejected — deduplicated later by `core/keys.dedup_key` |
| `out_of_order_rate` | **not** rejected — valid but late; the watermark absorbs or drops it (D3) |
| `dropout_probability_per_meter_tick` | **not** rejected — valid, store-and-forward (D3) |

Two more rules are structural, not fault-injection-driven, and get their own reasons:
`grid_zone` outside the configured set (`unknown_zone`), and `event_ts` outside a sane
window (`event_ts_out_of_range`). `voltage` is checked but never rejects — §6.2 marks it
deliberately unused by billing — a value outside its bounds is surfaced as a *warning*
only, in `ValidationResult.warnings`.

No config lookups happen inside `validate()` — every configured set/bound is passed in as
an argument, so tests can vary it and so batch/streaming call sites (which read config
once, not per row) are unambiguous about which snapshot of config applied.
"""

from __future__ import annotations

from datetime import datetime
from typing import NamedTuple

from voltstream.contracts.events import MeterReading

REJECTION_REASONS = frozenset(
    {
        "null_field",
        "negative_kwh",
        "unknown_household",
        "unknown_zone",
        "event_ts_out_of_range",
    }
)

_WARNING_KINDS = frozenset({"voltage_out_of_range"})


class ValidationResult(NamedTuple):
    valid: bool
    reason: str | None
    warnings: tuple[str, ...] = ()


def validate(
    reading: MeterReading,
    *,
    known_household_ids: frozenset[str],
    configured_zones: frozenset[str],
    event_ts_bounds: tuple[datetime, datetime],
    voltage_bounds: tuple[float, float] = (180.0, 260.0),
) -> ValidationResult:
    """Apply every rule in order, stopping at the first rejection. `event_ts_bounds` and
    `voltage_bounds` are `(low, high)` pairs, inclusive."""
    if (
        not reading.household_id
        or not reading.meter_id
        or not reading.grid_zone
        or reading.event_ts is None
    ):
        return ValidationResult(False, "null_field")

    if reading.consumption_kwh < 0 or reading.solar_generation_kwh < 0:
        return ValidationResult(False, "negative_kwh")

    if reading.household_id not in known_household_ids:
        return ValidationResult(False, "unknown_household")

    if reading.grid_zone not in configured_zones:
        return ValidationResult(False, "unknown_zone")

    low, high = event_ts_bounds
    if not (low <= reading.event_ts <= high):
        return ValidationResult(False, "event_ts_out_of_range")

    warnings: tuple[str, ...] = ()
    v_low, v_high = voltage_bounds
    if not (v_low <= reading.voltage <= v_high):
        warnings = ("voltage_out_of_range",)

    return ValidationResult(True, None, warnings)
