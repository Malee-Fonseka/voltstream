from __future__ import annotations

import json
import os
from datetime import datetime
from decimal import Decimal
from functools import lru_cache
from itertools import pairwise
from pathlib import Path
from typing import Any, Literal

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, model_validator

from voltstream.core.tariff import BlockBoundary

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ENV_VAR_PREFIX = "VOLTSTREAM__"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TariffBlock(_StrictModel):
    name: str
    up_to_kwh: int | None


class GeneratorDefaults(_StrictModel):
    block_rates: dict[str, float]
    fixed_charge_by_tier: dict[str, float]
    subsidy_pct: float
    export_rate: float


class TariffConfig(_StrictModel):
    blocks: list[TariffBlock]
    generator_defaults: GeneratorDefaults

    @model_validator(mode="after")
    def _blocks_are_a_valid_structure(self) -> TariffConfig:
        """D2's load-time rules (R14): three blocks, strictly increasing upper bounds, and
        exactly one unbounded block, last. A bad file fails here, naming the rule, instead
        of mis-billing later: unordered bounds priced silently wrong, and a wrong count
        only surfaced deep inside `build_blocks`."""
        bounds = [b.up_to_kwh for b in self.blocks]
        if len(bounds) != 3:
            raise ValueError(f"tariff.blocks: expected 3 blocks, got {len(bounds)}")
        if bounds[-1] is not None or None in bounds[:-1]:
            raise ValueError("tariff.blocks: exactly one block may be unbounded, and it is last")
        finite = [b for b in bounds[:-1] if b is not None]
        if any(b <= 0 for b in finite) or any(a >= b for a, b in pairwise(finite)):
            raise ValueError(f"tariff.blocks: bounds must be positive and increasing: {finite}")
        return self

    def boundaries(self) -> list[BlockBoundary]:
        return [
            BlockBoundary(b.name, None if b.up_to_kwh is None else Decimal(b.up_to_kwh))
            for b in self.blocks
        ]


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
    # Where short-lived batch processes push their metrics. None = do not push.
    pushgateway_url: str | None = None


class VoltstreamConfig(_StrictModel):
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
    merged = dict(base)
    for key, overlay_value in overlay.items():
        base_value = merged.get(key)
        if isinstance(base_value, dict) and isinstance(overlay_value, dict):
            merged[key] = _deep_merge(base_value, overlay_value)
        else:
            merged[key] = overlay_value
    return merged


def _coerce_env_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _apply_env_overrides(config: dict[str, Any]) -> dict[str, Any]:
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

    # `.env` names the database password POSTGRES_PASSWORD, as the Postgres image wants it.
    # Compose hands containers VOLTSTREAM__POSTGRES__PASSWORD, but a process on the host
    # (`make test-all`, a script) reads `.env` and gets only the former, and without this
    # could not connect at all. The explicit variable still wins.
    postgres = merged.setdefault("postgres", {})
    if not postgres.get("password") and os.environ.get("POSTGRES_PASSWORD"):
        postgres["password"] = os.environ["POSTGRES_PASSWORD"]

    return VoltstreamConfig(**merged)


@lru_cache(maxsize=1)
def get_config() -> VoltstreamConfig:
    return _load_config()
