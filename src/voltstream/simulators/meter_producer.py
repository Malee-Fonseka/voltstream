"""The meter producer (decisions T072/T073, §8.3) — the streaming data source.

Once per `simulation.emit_interval_seconds` (REAL seconds): read `sim_now()`; for every
household in the roster, compute a reading from `simulators/profiles.py`, run it through
`FaultInjector`, and publish each resulting reading to Kafka — key = `household_id`
(§3.3d, so all of one household's readings land on one partition), headers `trace_id` and
`produced_at` (T030, wall clock). One structured log line per tick with counts. SIGTERM/
SIGINT trigger a graceful shutdown that flushes the producer before exiting.

**Ticks run on a fixed grid (R37).** Tick k is due at `anchor + k × interval` real time
and stamped `epoch + k × interval × time_scale` simulated time, so every simulated day has
exactly `86400 / (interval × time_scale)` ticks (150 at the defaults) however the host
behaves. A tick that runs late is made up at once rather than pushing every later tick
back; before this, a day had about 138 ticks and each stall left a 14–23 simulated-minute
gap in every meter's series.
"""

from __future__ import annotations

import math
import random
import signal
import time
from datetime import UTC, datetime, timedelta
from types import FrameType
from uuid import uuid4

from confluent_kafka import KafkaError, Message, Producer

from voltstream import metrics, simclock
from voltstream.config import VoltstreamConfig, get_config
from voltstream.contracts.events import MeterReading
from voltstream.logging_setup import bind_trace_id, get_logger
from voltstream.simulators.faults import FaultInjector
from voltstream.simulators.households import Household, household_roster
from voltstream.simulators.profiles import consumption_kwh, solar_kwh

PRODUCER_ID = "sim-01"

# Deliberately unused by billing (§6.2) — a plausible mains voltage with small noise,
# nothing more sophisticated than that is warranted for a column nothing downstream reads.
_NOMINAL_VOLTAGE = 230.0
_VOLTAGE_NOISE = 3.0

# Further behind than this (30 real seconds, a meter's store-and-forward buffer) and the
# producer skips ahead instead of replaying: a host that slept for an hour should not
# flood Kafka with an hour of readings. The skipped ticks are logged as lost.
_MAX_CATCH_UP_TICKS = 15

# Nothing delivered for this long while messages wait, and the producer exits so that
# compose restarts it with a fresh Kafka client (R39). After a few seconds of Docker DNS
# failure, librdkafka once lost the partition leader from its cache and never found it
# again: nine minutes of "tick complete" while every message timed out, until a restart
# fixed it at once. Short of the 5-minute message timeout, so less is lost.
_DELIVERY_STALL_SECONDS = 60.0

_shutdown_requested = False
_last_delivered = time.monotonic()


def _request_shutdown(signum: int, frame: FrameType | None) -> None:
    global _shutdown_requested
    _shutdown_requested = True


def _build_reading(household: Household, event_ts: datetime, rng: random.Random) -> MeterReading:
    consumption = consumption_kwh(household.household_id, event_ts)
    solar = solar_kwh(household.household_id, event_ts, has_solar=household.has_solar)
    voltage = round(rng.gauss(_NOMINAL_VOLTAGE, _VOLTAGE_NOISE), 1)
    return MeterReading(
        schema_version="1.0",
        event_id=uuid4(),
        trace_id=uuid4(),
        meter_id=household.meter_id,
        household_id=household.household_id,
        grid_zone=household.grid_zone,
        event_ts=event_ts,
        consumption_kwh=consumption,
        solar_generation_kwh=solar,
        voltage=voltage,
        producer_id=PRODUCER_ID,
    )


def _delivery_report(err: KafkaError | None, msg: Message) -> None:
    global _last_delivered
    if err is not None:
        get_logger("meter-producer").error(
            "Kafka delivery failed", extra={"stage": "produce", "kafka_error": str(err)}
        )
    else:
        _last_delivered = time.monotonic()


def delivery_stalled(queued: int, last_delivered: float, now: float) -> bool:
    """Messages are waiting and none has been delivered for `_DELIVERY_STALL_SECONDS`.

    An empty queue is never a stall: a producer with nothing to send has nothing to lose.
    """
    return queued > 0 and now - last_delivered > _DELIVERY_STALL_SECONDS


def _produce_reading(producer: Producer, topic: str, reading: MeterReading) -> None:
    now_wall_clock = datetime.now(UTC).isoformat()
    producer.produce(
        topic=topic,
        key=reading.household_id.encode("utf-8"),
        value=reading.to_kafka_value(),
        headers=[
            ("trace_id", str(reading.trace_id).encode("utf-8")),
            ("produced_at", now_wall_clock.encode("utf-8")),
        ],
        on_delivery=_delivery_report,
    )
    metrics.events_produced_total.labels(producer_id=PRODUCER_ID).inc()


def tick_due(anchor_real: datetime, interval_seconds: float, index: int) -> datetime:
    """The real instant tick `index` is due."""
    return anchor_real + timedelta(seconds=interval_seconds * index)


def first_tick_index(anchor_real: datetime, now_real: datetime, interval_seconds: float) -> int:
    """The first grid point at or after `now_real`."""
    elapsed = (now_real - anchor_real).total_seconds()
    return max(0, math.ceil(elapsed / interval_seconds))


def catch_up(
    index: int, anchor_real: datetime, now_real: datetime, interval_seconds: float
) -> tuple[int, int]:
    """The tick to run next and how many were skipped to reach it.

    A tick or two behind is made up by running the late ones immediately, each stamped
    with its own grid time. Further behind than `_MAX_CATCH_UP_TICKS` jumps to the
    current grid point.
    """
    behind = (now_real - tick_due(anchor_real, interval_seconds, index)).total_seconds()
    if behind / interval_seconds <= _MAX_CATCH_UP_TICKS:
        return index, 0
    current = first_tick_index(anchor_real, now_real, interval_seconds)
    return current, current - index


def _run_tick(
    config: VoltstreamConfig,
    roster: list[Household],
    injector: FaultInjector,
    producer: Producer,
    rng: random.Random,
    event_ts: datetime,
    index: int,
) -> None:
    logger = get_logger("meter-producer")

    produced = 0
    for household in roster:
        reading = _build_reading(household, event_ts, rng)
        with bind_trace_id(str(reading.trace_id)):
            for outgoing in injector.apply(reading):
                _produce_reading(producer, config.kafka.topic, outgoing)
                produced += 1

    producer.poll(0)  # serve delivery-report callbacks without blocking the tick

    logger.info(
        "tick complete",
        extra={
            "stage": "produce",
            "tick": index,
            "sim_date": simclock.sim_date_of(event_ts).isoformat(),
            "households": len(roster),
            "readings_produced": produced,
            **{f"fault_{k}": v for k, v in injector.injected_counts.items()},
        },
    )


def main() -> None:
    config = get_config()
    logger = get_logger("meter-producer")
    metrics.start_metrics_server()

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    roster = household_roster(config.simulation.households, config.simulation.zones)
    injector = FaultInjector(config.faults, random.Random())
    producer = Producer({"bootstrap.servers": config.kafka.bootstrap_servers})

    logger.info(
        "meter-producer starting",
        extra={
            "stage": "startup",
            "households": len(roster),
            "emit_interval_seconds": config.simulation.emit_interval_seconds,
        },
    )

    rng = random.Random()
    interval = config.simulation.emit_interval_seconds
    anchor = simclock.sim_to_real(config.simulation.epoch_sim)
    index = first_tick_index(anchor, datetime.now(UTC), interval)
    global _last_delivered
    _last_delivered = time.monotonic()
    try:
        while not _shutdown_requested:
            if delivery_stalled(len(producer), _last_delivered, time.monotonic()):
                logger.error(
                    "nothing delivered to Kafka, exiting so the container restarts",
                    extra={
                        "stage": "produce",
                        "queued": len(producer),
                        "stalled_seconds": round(time.monotonic() - _last_delivered, 1),
                    },
                )
                raise SystemExit(1)
            index, skipped = catch_up(index, anchor, datetime.now(UTC), interval)
            if skipped:
                logger.warning(
                    "producer fell behind, skipping ticks",
                    extra={"stage": "produce", "ticks_skipped": skipped, "resume_tick": index},
                )
            due = tick_due(anchor, interval, index)
            wait = (due - datetime.now(UTC)).total_seconds()
            if wait > 0:
                time.sleep(wait)
            _run_tick(config, roster, injector, producer, rng, simclock.real_to_sim(due), index)
            index += 1
    finally:
        logger.info("meter-producer shutting down, flushing", extra={"stage": "shutdown"})
        producer.flush(timeout=10)


if __name__ == "__main__":
    main()
