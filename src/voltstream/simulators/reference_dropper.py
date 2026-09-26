"""The reference dropper (decisions T074/T075, §6.2/§6.3, D2) — the daily-batch data
source: once per simulated day, writes `tariff_<date>.csv` and `weather_<date>.csv` into
`voltstream-landing/`.

This is the **only** module allowed to read `config.tariff.generator_defaults` (D2) — the
money values there exist purely to let this module write a plausible daily file; nothing
else in the codebase may read them (`core/`, `streaming/`, `batch/`, `api/` all get their
money from the tariff file this module writes, per household, per day).

**Deterministic day-over-day change (D2/T074):** `block_2_rate` steps up by a fixed amount
on alternate simulated days. Without a real day-to-day change somewhere in the tariff, the
speed layer's stale-tariff divergence (§3.1) would be zero every day, which is a weak
story for the Lambda-divergence demo (`T140`). The rule lives in this docstring and
nowhere else — it is the whole rule.

Each file is written to a temporary key and then copied to its final key (`copy_object` +
delete), so the Airflow tariff-watcher sensor (T121) can never observe a partially-written
object — a poke that lands mid-`put_object` would otherwise see a truncated CSV.

**On this module's `objectstore.py` dependency:** Phase 5 (simulators) is built and must
run before Phase 8 (`storage/objectstore.py`, T099) exists, so this module talks to MinIO
directly via `boto3` rather than through that not-yet-built abstraction. `T099`'s own
Done-when (no `s3a://`/S3 path literal outside `objectstore.py`) applies from the point
that module exists onward; until then, the two S3 key-building functions below are this
module's only place path strings are assembled, which is the same discipline in spirit.
"""

from __future__ import annotations

import csv
import io
import time
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import boto3
from botocore.config import Config

from voltstream import simclock
from voltstream.config import GeneratorDefaults, VoltstreamConfig, get_config
from voltstream.contracts.reference import TariffRecord, WeatherForecast
from voltstream.logging_setup import get_logger
from voltstream.simulators.households import Household, household_roster

PRODUCER_ID = "reference-dropper"

_BLOCK_2_STEP = Decimal("0.50")  # D2/T074: alternate-day step, documented here in full.
_POLL_INTERVAL_REAL_SECONDS = 5

_TARIFF_FIELDS = [
    "household_id",
    "effective_date",
    "billing_tier",
    "subsidy_flag",
    "subsidy_pct",
    "fixed_charge",
    "block_1_rate",
    "block_2_rate",
    "block_3_rate",
    "export_rate",
]
_WEATHER_FIELDS = [
    "grid_zone",
    "forecast_date",
    "cloud_cover_pct",
    "temperature_c",
    "solar_irradiance_index",
]


def _s3_client() -> Any:
    # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION / AWS_ENDPOINT_URL are
    # read by boto3 itself from the environment (.env, T018) — nothing to pass explicitly.
    # path addressing is required for MinIO (virtual-host style will not resolve).
    return boto3.client("s3", config=Config(s3={"addressing_style": "path"}))


def _tariff_key(sim_date: date) -> str:
    return f"tariff/tariff_{sim_date.isoformat()}.csv"


def _weather_key(sim_date: date) -> str:
    return f"weather/weather_{sim_date.isoformat()}.csv"


def _block_2_rate_for(sim_date: date, generator_defaults: GeneratorDefaults) -> Decimal:
    base = Decimal(str(generator_defaults.block_rates["block_2"]))
    return base + (_BLOCK_2_STEP if sim_date.toordinal() % 2 == 0 else Decimal(0))


def _tariff_rows(
    sim_date: date, households: list[Household], generator_defaults: GeneratorDefaults
) -> list[TariffRecord]:
    block_1 = Decimal(str(generator_defaults.block_rates["block_1"]))
    block_2 = _block_2_rate_for(sim_date, generator_defaults)
    block_3 = Decimal(str(generator_defaults.block_rates["block_3"]))
    subsidy_pct = Decimal(str(generator_defaults.subsidy_pct))
    export_rate = Decimal(str(generator_defaults.export_rate))

    rows = []
    for household in households:
        fixed_charge = Decimal(str(generator_defaults.fixed_charge_by_tier[household.billing_tier]))
        rows.append(
            TariffRecord(
                household_id=household.household_id,
                effective_date=sim_date,
                billing_tier=household.billing_tier,
                subsidy_flag=household.subsidy_flag,
                subsidy_pct=subsidy_pct,
                fixed_charge=fixed_charge,
                block_1_rate=block_1,
                block_2_rate=block_2,
                block_3_rate=block_3,
                export_rate=export_rate,
            )
        )
    return rows


def _weather_rows(sim_date: date, zones: list[str]) -> list[WeatherForecast]:
    # A simple deterministic-per-(zone, date) forecast — plausible variety across zones
    # and days without needing real weather data, using the date itself as the seed.
    rows = []
    for i, zone in enumerate(zones):
        day_offset = sim_date.toordinal()
        cloud_cover = Decimal(((day_offset * 7 + i * 13) % 80) + 10)  # 10-89%
        temperature = Decimal(20 + (day_offset * 3 + i * 5) % 15)  # 20-34 C
        irradiance = (Decimal(100) - cloud_cover) / Decimal(100)
        rows.append(
            WeatherForecast(
                grid_zone=zone,
                forecast_date=sim_date,
                cloud_cover_pct=cloud_cover,
                temperature_c=temperature,
                solar_irradiance_index=irradiance.quantize(Decimal("0.001")),
            )
        )
    return rows


def _write_csv(fields: list[str], rows: list[TariffRecord] | list[WeatherForecast]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                field: (str(value).lower() if isinstance(value, bool) else str(value))
                for field, value in row.model_dump().items()
            }
        )
    return buffer.getvalue().encode("utf-8")


def _put_atomically(s3: Any, bucket: str, key: str, data: bytes) -> None:
    tmp_key = f"{key}.tmp"
    s3.put_object(Bucket=bucket, Key=tmp_key, Body=data)
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": tmp_key}, Key=key)
    s3.delete_object(Bucket=bucket, Key=tmp_key)


def drop_reference_files(
    s3: Any, config: VoltstreamConfig, households: list[Household], sim_date: date
) -> None:
    """Write `tariff_<sim_date>.csv` and `weather_<sim_date>.csv`, atomically."""
    bucket = config.minio.bucket_landing

    tariff_rows = _tariff_rows(sim_date, households, config.tariff.generator_defaults)
    _put_atomically(s3, bucket, _tariff_key(sim_date), _write_csv(_TARIFF_FIELDS, tariff_rows))

    weather_rows = _weather_rows(sim_date, config.simulation.zones)
    _put_atomically(s3, bucket, _weather_key(sim_date), _write_csv(_WEATHER_FIELDS, weather_rows))


def main() -> None:
    config = get_config()
    logger = get_logger(PRODUCER_ID)
    s3 = _s3_client()
    households = household_roster(config.simulation.households, config.simulation.zones)

    today = simclock.sim_date_of(simclock.sim_now())

    # T075: the speed layer's provisional estimate costs against *yesterday's* tariff
    # (§3.1). On the first simulated day there is no yesterday on disk yet unless we put
    # one there now, or household_running_rt.estimated_bill has nothing to join against.
    drop_reference_files(s3, config, households, today - timedelta(days=1))
    logger.info(
        "seeded day-zero (yesterday's) reference files",
        extra={"stage": "startup", "sim_date": (today - timedelta(days=1)).isoformat()},
    )

    last_dropped: date | None = None
    while True:
        current = simclock.sim_date_of(simclock.sim_now())
        if current != last_dropped:
            drop_reference_files(s3, config, households, current)
            last_dropped = current
            logger.info(
                "dropped reference files",
                extra={
                    "stage": "produce",
                    "sim_date": current.isoformat(),
                    "households": len(households),
                },
            )
        time.sleep(_POLL_INTERVAL_REAL_SECONDS)


if __name__ == "__main__":
    main()
