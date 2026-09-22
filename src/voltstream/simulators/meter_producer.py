"""The meter producer (decisions T072/T073, §8.3) — the streaming data source.

Once per `simulation.emit_interval_seconds` (REAL seconds): read `sim_now()`; for every
household in the roster, compute a reading from `simulators/profiles.py`, run it through
`FaultInjector`, and publish each resulting reading to Kafka — key = `household_id`
(§3.3d, so all of one household's readings land on one partition), headers `trace_id` and
`produced_at` (T030, wall clock). One structured log line per tick with counts. SIGTERM/
SIGINT trigger a graceful shutdown that flushes the producer before exiting.
"""

from __future__ import annotations

import random
import signal
import time
from datetime import UTC, datetime
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

_shutdown_requested = False


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
    if err is not None:
        get_logger("meter-producer").error(
            "Kafka delivery failed", extra={"stage": "produce", "kafka_error": str(err)}
        )


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


def _run_tick(
    config: VoltstreamConfig,
    roster: list[Household],
    injector: FaultInjector,
    producer: Producer,
    rng: random.Random,
) -> None:
    logger = get_logger("meter-producer")
    event_ts = simclock.sim_now()

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
    try:
        while not _shutdown_requested:
            tick_started = time.monotonic()
            _run_tick(config, roster, injector, producer, rng)
            elapsed = time.monotonic() - tick_started
            time.sleep(max(0.0, config.simulation.emit_interval_seconds - elapsed))
    finally:
        logger.info("meter-producer shutting down, flushing", extra={"stage": "shutdown"})
        producer.flush(timeout=10)


if __name__ == "__main__":
    main()
