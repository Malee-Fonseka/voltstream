"""Typed application configuration (decision T021).

`get_config()` is the *only* sanctioned way to read configuration in this codebase — no
other module may read YAML or `os.environ` directly. Load order:

1. `config/base.yaml` — every tunable, with safe defaults.
2. `config/<VOLTSTREAM_ENV>.yaml`, deep-merged over (1). Missing overlay is not an error.
3. `.env` (if present) populates any environment variable not already exported by the
   shell — via `python-dotenv` with `override=False`, so a real shell export always wins.
4. `VOLTSTREAM__SECTION__KEY` environment variables (from the shell or from step 3)
   override the merged mapping from (1)+(2), nested-key by nested-key.

The result is validated against `VoltstreamConfig`, whose submodels all forbid unknown
fields — a typo or a leftover key in `base.yaml` or an env var is a load-time error, not a
silently ignored one.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ENV_VAR_PREFIX = "VOLTSTREAM__"


class _StrictModel(BaseModel):
    """Base for every config section: unknown keys are a load-time error."""

    model_config = ConfigDict(extra="forbid")


class TariffBlock(_StrictModel):
    name: str
    up_to_kwh: int | None


class GeneratorDefaults(_StrictModel):
    """Read ONLY by simulators/reference_dropper.py — see D2. Nothing else may use this."""

    block_rates: dict[str, float]
    fixed_charge_by_tier: dict[str, float]
    subsidy_pct: float
    export_rate: float


class TariffConfig(_StrictModel):
    blocks: list[TariffBlock]
    generator_defaults: GeneratorDefaults


class SimulationConfig(_StrictModel):
    time_scale: int
    emit_interval_seconds: float
    households: int
    zones: list[str]
    # The simulated instant a run starts at. Fixed, so partition dates are stable.
    epoch_sim: datetime
    # The real instant that maps to epoch_sim. None = this process's start time; set it
    # explicitly whenever more than one process takes part in a run, or they will
    # disagree by their startup skew times time_scale. See simclock's module docstring.
    anchor_real: datetime | None = None


class FaultsConfig(_StrictModel):
    duplicate_rate: float
    out_of_order_rate: float
    out_of_order_lateness_sim_minutes: tuple[int, int]
    null_field_rate: float
    negative_value_rate: float
    unknown_household_rate: float
    dropout_probability_per_meter_tick: float
    dropout_duration_real_seconds: float
    dropout_backfill: bool


class KafkaConfig(_StrictModel):
    bootstrap_servers: str
    topic: str
    dlq_topic: str
    partitions: int
    retention_ms: int


class SpeedLayerConfig(_StrictModel):
    window_sim_minutes: int
    watermark_sim_minutes: int
    trigger_interval_real_seconds: int
    output_mode: Literal["update", "append", "complete"]


class BatchConfig(_StrictModel):
    late_data_grace_real_seconds: int


class AlertsConfig(_StrictModel):
    stale_data_minutes: int
    low_renewable_threshold: float
    reject_rate_threshold: float
    batch_sla_minutes: int
    lambda_divergence_threshold_pct: float


class PostgresConfig(_StrictModel):
    host: str
    port: int
    db: str
    user: str
    # Never set in base.yaml/local.yaml. Supplied via VOLTSTREAM__POSTGRES__PASSWORD (or
    # left as the empty-string default for a config-only smoke test that never connects).
    password: str = ""


class MinioConfig(_StrictModel):
    bucket_raw: str
    bucket_landing: str
    bucket_archive: str


class ApiConfig(_StrictModel):
    host: str
    port: int


class ObservabilityConfig(_StrictModel):
    metrics_port: int
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]


class VoltstreamConfig(_StrictModel):
    """The root config object. Build only via `get_config()`."""

    simulation: SimulationConfig
    faults: FaultsConfig
    kafka: KafkaConfig
    speed_layer: SpeedLayerConfig
    batch: BatchConfig
    tariff: TariffConfig
    alerts: AlertsConfig
    postgres: PostgresConfig
    minio: MinioConfig
    api: ApiConfig
    observability: ObservabilityConfig


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge `overlay` over `base`. Neither argument is mutated."""
    merged = dict(base)
    for key, overlay_value in overlay.items():
        base_value = merged.get(key)
        if isinstance(base_value, dict) and isinstance(overlay_value, dict):
            merged[key] = _deep_merge(base_value, overlay_value)
        else:
            merged[key] = overlay_value
    return merged


def _coerce_env_value(raw: str) -> Any:
    """Best-effort parse of an environment-variable string into a native YAML-ish value.

    JSON covers everything base.yaml can express for a leaf value (numbers, booleans,
    null, and `[1, 30]`-style lists for fields like `out_of_order_lateness_sim_minutes`).
    A value that is not valid JSON (e.g. a bare hostname) is kept as the raw string.
    """
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _apply_env_overrides(config: dict[str, Any]) -> dict[str, Any]:
    """Apply every `VOLTSTREAM__SECTION__KEY[__SUBKEY...]` env var onto `config`."""
    result = dict(config)
    for env_key, env_value in os.environ.items():
        if not env_key.startswith(_ENV_VAR_PREFIX):
            continue
        # Compose interpolates an unset `${VAR:-}` to an empty string rather than leaving
        # the variable out, so an empty value means "not configured", not "the empty
        # string". Without this an optional field like simulation.anchor_real would
        # receive "" and fail validation whenever the stack runs without an anchor.
        if env_value == "":
            continue
        path = [segment.lower() for segment in env_key[len(_ENV_VAR_PREFIX) :].split("__")]
        if not path or not path[0]:
            continue
        cursor = result
        for segment in path[:-1]:
            existing = cursor.get(segment)
            if not isinstance(existing, dict):
                existing = {}
            cursor[segment] = existing
            cursor = existing
        cursor[path[-1]] = _coerce_env_value(env_value)
    return result


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        loaded = yaml.safe_load(f)
    return loaded or {}


def _load_config() -> VoltstreamConfig:
    dotenv_path = _REPO_ROOT / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path, override=False)

    config_dir = Path(os.environ.get("VOLTSTREAM_CONFIG_DIR", _REPO_ROOT / "config"))
    merged = _load_yaml(config_dir / "base.yaml")

    env_name = os.environ.get("VOLTSTREAM_ENV")
    if env_name:
        overlay_path = config_dir / f"{env_name}.yaml"
        if overlay_path.exists():
            merged = _deep_merge(merged, _load_yaml(overlay_path))

    merged = _apply_env_overrides(merged)
    return VoltstreamConfig(**merged)


@lru_cache(maxsize=1)
def get_config() -> VoltstreamConfig:
    """Return the process-wide config, loaded and validated once then cached.

    Call `get_config.cache_clear()` (tests only) to force a reload after mutating
    `os.environ` or the config files — application code should never need to.
    """
    return _load_config()
