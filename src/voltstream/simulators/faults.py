"""Fault injection (decisions T069/T070, D3, §9 Phase 1) — deliberately corrupts or
delays a fraction of readings so the pipeline has something real to detect, reject and
reconcile. "Fault injection is not optional... it earns marks in three rubric rows at
once" (§9).

`FaultInjector.apply(reading)` returns a **list** of readings (never a bare reading)
because a duplicate emits two, a suppressed/buffered reading emits zero, and a flushed
dropout backfill emits many at once. With every rate in `faults_config` at 0, `apply`
is the identity: one record in, one record out.

Every fault type is mutually exclusive per tick *except* `duplicate_rate`, which is
additive (rolled independently, after whichever other fault — if any — was applied) — a
duplicated reading is a duplicate of whatever the tick actually produced, corrupted or
not, because that is what a real re-transmission would duplicate.

Dropout (`dropout_probability_per_meter_tick`) is different in kind from the other five:
it is a **per-meter state machine** keyed on **real** wall-clock time
(`dropout_duration_real_seconds`), not a per-reading coin flip, because a comms outage
spans many ticks. See `D3` for why this is the mechanism that produces the modelled
~1.7% speed-vs-batch kWh gap.
"""

from __future__ import annotations

import random
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from voltstream.config import FaultsConfig
from voltstream.contracts.events import MeterReading

UNKNOWN_HOUSEHOLD_SENTINEL = "HH-9999"


@dataclass
class _DropoutState:
    expiry: datetime
    buffer: list[MeterReading] = field(default_factory=list)


class FaultInjector:
    """One injector per producer process, holding the per-meter dropout state machine.
    Not thread-safe — one instance per producer, called from one loop (§8.3)."""

    def __init__(
        self,
        faults_config: FaultsConfig,
        rng: random.Random,
        *,
        now_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._config = faults_config
        self._rng = rng
        self._now_fn = now_fn
        self._dropout_state: dict[str, _DropoutState] = {}
        # Observability only (T073): how many times each fault kind has fired since this
        # injector was created. Never read by apply() itself — purely for the producer to
        # report via metrics/logs once per tick.
        self.injected_counts: Counter[str] = Counter()

    def is_dropped_out(self, meter_id: str) -> bool:
        """Whether `meter_id` is currently buffering (mid comms-outage)."""
        return meter_id in self._dropout_state

    def apply(self, reading: MeterReading) -> list[MeterReading]:
        flushed, continue_processing = self._advance_dropout(reading)
        if not continue_processing:
            return flushed  # [] while still buffering, or [] this tick has no live reading

        primary = self._apply_mutually_exclusive_fault(reading)
        readings = list(flushed)
        readings.append(primary)
        if self._rng.random() < self._config.duplicate_rate:
            self.injected_counts["duplicate"] += 1
            readings.append(primary.model_copy())
        return readings

    # -- dropout: per-meter state machine, real time --------------------------------

    def _advance_dropout(self, reading: MeterReading) -> tuple[list[MeterReading], bool]:
        """Returns `(extra_readings, continue_processing)`.

        `continue_processing=False` means this tick's reading was consumed by the
        dropout mechanism (buffered) and `apply` must return `extra_readings` (always
        `[]`) as-is, with no other fault processing applied to it.

        `continue_processing=True` means this meter is live this tick — either it was
        never down, or it has *just* reconnected — so `apply` should still run the
        reading through the normal per-reading fault logic. `extra_readings` is the
        flushed backfill batch to prepend ahead of that live reading (D3: unchanged
        `event_ts`), or `[]` if there is nothing to flush.

        The tick that first observes `now >= expiry` is the reconnection tick: it flushes
        everything buffered *before* that instant and treats its own reading as live,
        never as the last buffered item — this is what makes a `dropout_duration_real_seconds
        == N * emit_interval_seconds` outage flush exactly `N` backfilled readings rather
        than `N + 1`.
        """
        meter_id = reading.meter_id
        now = self._now_fn()
        state = self._dropout_state.get(meter_id)

        if state is None:
            if self._rng.random() < self._config.dropout_probability_per_meter_tick:
                self.injected_counts["dropout_triggered"] += 1
                expiry = now + timedelta(seconds=self._config.dropout_duration_real_seconds)
                self._dropout_state[meter_id] = _DropoutState(expiry=expiry, buffer=[reading])
                return [], False
            return [], True

        if now >= state.expiry:
            del self._dropout_state[meter_id]
            flushed = list(state.buffer) if self._config.dropout_backfill else []
            self.injected_counts["dropout_flushed"] += len(flushed)
            return flushed, True

        state.buffer.append(reading)
        return [], False

    # -- the five per-reading faults, mutually exclusive per tick --------------------

    def _apply_mutually_exclusive_fault(self, reading: MeterReading) -> MeterReading:
        roll = self._rng.random()

        roll -= self._config.null_field_rate
        if roll < 0:
            self.injected_counts["null_field"] += 1
            return self._null_a_field(reading)

        roll -= self._config.negative_value_rate
        if roll < 0:
            self.injected_counts["negative_kwh"] += 1
            return self._negate_a_value(reading)

        roll -= self._config.unknown_household_rate
        if roll < 0:
            self.injected_counts["unknown_household"] += 1
            return self._unknown_household(reading)

        roll -= self._config.out_of_order_rate
        if roll < 0:
            self.injected_counts["out_of_order"] += 1
            return self._shift_out_of_order(reading)

        return reading

    def _null_a_field(self, reading: MeterReading) -> MeterReading:
        # An empty string, not Python None: the frozen contract's required fields are
        # non-Optional, so a genuinely null value fails Pydantic parsing before it ever
        # reaches here (that failure is itself the "null_field" rejection, one layer up,
        # at the Kafka deserialisation boundary). An empty string is the null-*like* value
        # that reaches core/validation.validate as a legitimately-parsed reading.
        return reading.model_copy(update={"household_id": ""})

    def _negate_a_value(self, reading: MeterReading) -> MeterReading:
        field_name = self._rng.choice(["consumption_kwh", "solar_generation_kwh"])
        current = getattr(reading, field_name)
        return reading.model_copy(update={field_name: -(abs(current) + 1)})

    def _unknown_household(self, reading: MeterReading) -> MeterReading:
        return reading.model_copy(update={"household_id": UNKNOWN_HOUSEHOLD_SENTINEL})

    def _shift_out_of_order(self, reading: MeterReading) -> MeterReading:
        low, high = self._config.out_of_order_lateness_sim_minutes
        lateness_minutes = self._rng.uniform(low, high)
        shifted = reading.event_ts - timedelta(minutes=lateness_minutes)
        return reading.model_copy(update={"event_ts": shifted})
