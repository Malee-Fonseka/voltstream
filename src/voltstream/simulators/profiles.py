"""Deterministic consumption and solar curves (decisions T065/T066, §2.5).

Both `consumption_kwh` and `solar_kwh` are **pure functions** of `(household_id,
event_ts, ...)` — no internal RNG state carried between calls. Every source of
randomness is seeded from a hash of the call's own arguments, so the same
`(household_id, event_ts)` pair always produces the same reading, whether this is the
first tick of a fresh run or a replay: reproducibility is explicitly graded (§2.5), and a
producer that carries RNG state across ticks would make a restart change the readings
from that point on.
"""

from __future__ import annotations

import hashlib
import math
from datetime import datetime
from decimal import Decimal

import numpy as np

from voltstream.core.money import quantize_kwh

# Tunable curve shape constants — not configuration (they describe the *shape* of a
# household's day, not a business rule), so they are not read from config/base.yaml.
_BASE_LOAD_KWH = Decimal("0.30")
_MORNING_PEAK_HOUR = 7.5
_MORNING_PEAK_WIDTH = 1.5
_EVENING_PEAK_HOUR = 19.5
_EVENING_PEAK_WIDTH = 2.0
_LOAD_BASELINE = 0.15
_LOAD_MORNING_WEIGHT = 0.5
_LOAD_EVENING_WEIGHT = 0.7
_LOAD_NOISE_STDDEV = 0.08

# Peak generation for one tick, i.e. one 9.6-simulated-minute interval — so this is an
# energy figure, not a power rating. 0.30 kWh per tick is roughly a 1.9 kW array, a
# plausible domestic rooftop.
#
# Calibrated rather than guessed. At the original 2.00 a solar household generated about
# 450 % of its own daily consumption: every zone's daily renewable ratio pinned at 100 %,
# a third of households billed a negative total from export credits alone, and the
# headline metric the use case asks about — renewable contribution by zone — carried no
# information at all. 0.30 puts a solar household at roughly two-thirds of its own use,
# which leaves it exporting at midday and importing at dusk, and puts the zone daily
# ratio near 20 % with 32 % of households on solar.
_SOLAR_PEAK_CAPACITY_KWH = Decimal("0.30")
_SOLAR_SUNRISE_HOUR = 6.0
_SOLAR_SUNSET_HOUR = 18.0
_SOLAR_NOISE_STDDEV = 0.05
_SOLAR_CLOUD_ATTENUATION = 0.8  # 100% cloud cover cuts output by this fraction


def _fractional_hour(event_ts: datetime) -> float:
    return event_ts.hour + event_ts.minute / 60 + event_ts.second / 3600


def _seeded_rng(*parts: str) -> np.random.Generator:
    """A fresh, deterministic RNG seeded from a hash of `parts` — never a shared or
    module-level generator, so calls are independent and reproducible regardless of order.
    """
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    seed = int(digest, 16) % (2**32)
    return np.random.default_rng(seed)


def _household_scale_factor(household_id: str) -> Decimal:
    """A stable per-household multiplier in [0.7, 1.3] — some households simply use more
    or less power than others, consistently, day to day."""
    digest = hashlib.sha256(household_id.encode("utf-8")).hexdigest()
    fraction = (int(digest, 16) % 10_000) / 9_999
    return Decimal(str(round(0.7 + fraction * 0.6, 4)))


def _load_shape(hour: float) -> float:
    """A double-humped curve over the 24-hour day: a morning peak and a larger evening
    peak, on a low overnight baseline."""
    morning = math.exp(-((hour - _MORNING_PEAK_HOUR) ** 2) / (2 * _MORNING_PEAK_WIDTH**2))
    evening = math.exp(-((hour - _EVENING_PEAK_HOUR) ** 2) / (2 * _EVENING_PEAK_WIDTH**2))
    return _LOAD_BASELINE + _LOAD_MORNING_WEIGHT * morning + _LOAD_EVENING_WEIGHT * evening


def consumption_kwh(household_id: str, event_ts: datetime) -> Decimal:
    """A household's consumption for one tick at `event_ts` (simulated time, §3.4)."""
    shape = _load_shape(_fractional_hour(event_ts))
    scale = _household_scale_factor(household_id)
    rng = _seeded_rng(household_id, event_ts.isoformat(), "consumption")
    noise = max(0.0, float(rng.normal(1.0, _LOAD_NOISE_STDDEV)))

    value = _BASE_LOAD_KWH * Decimal(str(shape)) * scale * Decimal(str(round(noise, 4)))
    return quantize_kwh(max(value, Decimal(0)))


def _solar_shape(hour: float) -> float:
    """Zero outside daylight, a bell centred on simulated midday in between."""
    if hour <= _SOLAR_SUNRISE_HOUR or hour >= _SOLAR_SUNSET_HOUR:
        return 0.0
    half_day = (_SOLAR_SUNSET_HOUR - _SOLAR_SUNRISE_HOUR) / 2
    midday = (_SOLAR_SUNRISE_HOUR + _SOLAR_SUNSET_HOUR) / 2
    return max(0.0, math.cos((hour - midday) / half_day * (math.pi / 2)) ** 2)


def solar_kwh(
    household_id: str,
    event_ts: datetime,
    *,
    has_solar: bool,
    cloud_cover_pct: Decimal = Decimal(0),
) -> Decimal:
    """A household's solar generation for one tick. Households without a panel
    (`has_solar=False`, from the `households` dimension, T038) always read zero — driving
    this from the database keeps the simulator, the seed data and the tariff generator in
    agreement about who has solar (T066)."""
    if not has_solar:
        return Decimal("0.0000")

    shape = _solar_shape(_fractional_hour(event_ts))
    if shape == 0.0:
        return Decimal("0.0000")

    cloud_factor = max(0.0, 1 - float(cloud_cover_pct) / 100 * _SOLAR_CLOUD_ATTENUATION)
    rng = _seeded_rng(household_id, event_ts.isoformat(), "solar")
    noise = max(0.0, float(rng.normal(1.0, _SOLAR_NOISE_STDDEV)))

    value = (
        _SOLAR_PEAK_CAPACITY_KWH
        * Decimal(str(shape))
        * Decimal(str(round(cloud_factor, 4)))
        * Decimal(str(round(noise, 4)))
    )
    return quantize_kwh(max(value, Decimal(0)))
