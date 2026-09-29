"""The reference dropper (decisions T074/T075, §6.2/§6.3, D2) — the daily-batch data
source: once per simulated day, writes `tariff_<date>.csv` and `weather_<date>.csv` into
`voltstream-landing/`.

**When each file lands (§8.3, D3, R03).** `tariff_D` is written when day D *closes* —
at the simulated midnight into D+1 — because its arrival is what tells the orchestrator
the day is over and can be billed, and the late-data grace is timed from it. A tariff for
D written while D was still running undercut §2.4 ("depends on data that does not exist
until the day ends") and turned the grace into a no-op. `weather_D` is a forecast, so it
lands when D *starts*.

The speed layer costs day D against `tariff_{D-1}`, which therefore appears within one
poll of D beginning. On start, the dropper writes the tariff of every closed day since
the newest one already in the bucket: on a fresh stack that is only yesterday (the T075
day-zero seed), and after a restart it is whatever closed while the dropper was down.

This is the **only** module allowed to read `config.tariff.generator_defaults` (D2) — the
money values there exist purely to let this module write a plausible daily file; nothing
else in the codebase may read them (`core/`, `streaming/`, `batch/`, `api/` all get their
money from the tariff file this module writes, per household, per day).

**Deterministic day-over-day change (D2/T074):** `block_1_rate` is 0.50 higher on even
simulated days (by date ordinal) than on odd ones; every other column is the same every
day. Without a real day-to-day change the speed layer's stale-tariff divergence (§3.1)
would be zero, and there would be nothing for `tariff_effect` (D4, T140) to attribute.
The rule lives in this docstring and nowhere else — it is the whole rule.

It is block 1 because every household pays block 1. The rule used to step `block_2_rate`,
which starts at 60 kWh, and no simulated household uses 60 kWh in a day (the measured
maximum is about 22), so yesterday's and today's tariffs priced every bill identically
and `tariff_effect` was 0.00 everywhere (R01).

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
import re
import time
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from voltstream import simclock
from voltstream.config import GeneratorDefaults, VoltstreamConfig, get_config
from voltstream.contracts.reference import TariffRecord, WeatherForecast
from voltstream.logging_setup import get_logger
from voltstream.simulators.households import Household, household_roster

PRODUCER_ID = "reference-dropper"

_BLOCK_1_STEP = Decimal("0.50")  # D2/T074: alternate-day step, documented here in full.
# One real second is 4.8 simulated minutes: how late after midnight a day's tariff can land.
_POLL_INTERVAL_REAL_SECONDS = 1
# A restart after a long stop owes a tariff per closed day; beyond a week, nobody is
# waiting for those bills, and the days have no readings to bill anyway (R04).
_MAX_CATCH_UP_DAYS = 7

# tariff/tariff_2026-01-02.csv -> 2026-01-02
_TARIFF_NAME = re.compile(r"tariff_(\d{4}-\d{2}-\d{2})\.csv$")

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


def _block_1_rate_for(sim_date: date, generator_defaults: GeneratorDefaults) -> Decimal:
    base = Decimal(str(generator_defaults.block_rates["block_1"]))
    return base + (_BLOCK_1_STEP if sim_date.toordinal() % 2 == 0 else Decimal(0))


def _tariff_rows(
    sim_date: date, households: list[Household], generator_defaults: GeneratorDefaults
) -> list[TariffRecord]:
    block_1 = _block_1_rate_for(sim_date, generator_defaults)
    block_2 = Decimal(str(generator_defaults.block_rates["block_2"]))
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


def drop_tariff(
    s3: Any, config: VoltstreamConfig, households: list[Household], sim_date: date
) -> None:
    """Write `tariff_<sim_date>.csv`, atomically. Called once `sim_date` has closed."""
    rows = _tariff_rows(sim_date, households, config.tariff.generator_defaults)
    _put_atomically(
        s3, config.minio.bucket_landing, _tariff_key(sim_date), _write_csv(_TARIFF_FIELDS, rows)
    )


def drop_weather(s3: Any, config: VoltstreamConfig, sim_date: date) -> None:
    """Write `weather_<sim_date>.csv`, atomically. Called as `sim_date` starts."""
    rows = _weather_rows(sim_date, config.simulation.zones)
    _put_atomically(
        s3, config.minio.bucket_landing, _weather_key(sim_date), _write_csv(_WEATHER_FIELDS, rows)
    )


def closed_days_owed(dropped: set[date], today: date) -> list[date]:
    """The closed days whose tariff the landing zone is missing, oldest first.

    Every day after the newest tariff already dropped, up to yesterday. With nothing
    dropped yet that is yesterday alone: the T075 seed, so the speed layer has a tariff to
    cost day one against. Capped at `_MAX_CATCH_UP_DAYS`.
    """
    yesterday = today - timedelta(days=1)
    closed = [d for d in dropped if d <= yesterday]
    first = max(closed) + timedelta(days=1) if closed else yesterday
    first = max(first, yesterday - timedelta(days=_MAX_CATCH_UP_DAYS - 1))
    return [first + timedelta(days=i) for i in range((yesterday - first).days + 1)]


def _dropped_tariff_dates(s3: Any, bucket: str) -> set[date]:
    dates: set[date] = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="tariff/"):
        for obj in page.get("Contents", []):
            if match := _TARIFF_NAME.search(obj["Key"]):
                dates.add(date.fromisoformat(match.group(1)))
    return dates


def _exists(s3: Any, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return True


def main() -> None:
    config = get_config()
    logger = get_logger(PRODUCER_ID)
    s3 = _s3_client()
    bucket = config.minio.bucket_landing
    households = household_roster(config.simulation.households, config.simulation.zones)

    today = simclock.sim_date_of(simclock.sim_now())

    for closed in closed_days_owed(_dropped_tariff_dates(s3, bucket), today):
        drop_tariff(s3, config, households, closed)
        logger.info(
            "dropped tariff for a day that closed before startup",
            extra={"stage": "startup", "sim_date": closed.isoformat()},
        )
    if not _exists(s3, bucket, _weather_key(today)):
        drop_weather(s3, config, today)

    last_seen = today
    while True:
        current = simclock.sim_date_of(simclock.sim_now())
        if current > last_seen:
            # Normally one day; more only if this process stalled across a midnight.
            for offset in range((current - last_seen).days):
                closed = last_seen + timedelta(days=offset)
                drop_tariff(s3, config, households, closed)
                logger.info(
                    "day closed, tariff dropped",
                    extra={
                        "stage": "produce",
                        "sim_date": closed.isoformat(),
                        "households": len(households),
                    },
                )
            drop_weather(s3, config, current)
            last_seen = current
        time.sleep(_POLL_INTERVAL_REAL_SECONDS)


if __name__ == "__main__":
    main()
