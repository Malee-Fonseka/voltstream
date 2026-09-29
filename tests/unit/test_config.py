"""Unit tests for voltstream.config (T022)."""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

import voltstream.config as config_module
from voltstream.config import TariffConfig, get_config
from voltstream.core.tariff import BlockBoundary

_REPO_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


@pytest.fixture(autouse=True)
def _isolated_config_cache(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Every test gets a clean cache and starts with no VOLTSTREAM_* env vars set.

    monkeypatch.delenv/setenv are auto-reverted after the test; explicitly clearing the
    lru_cache before and after keeps get_config() honest about which env was in effect.
    """
    for name in list(config_module.os.environ):
        if name.startswith("VOLTSTREAM"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("VOLTSTREAM_CONFIG_DIR", str(_REPO_CONFIG_DIR))
    get_config.cache_clear()
    yield
    get_config.cache_clear()


def test_base_yaml_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = get_config()
    assert cfg.tariff.blocks[0].up_to_kwh == 60
    assert cfg.simulation.households == 50


def test_tariff_boundaries_convert_to_core_block_boundaries() -> None:
    boundaries = get_config().tariff.boundaries()

    assert boundaries == [
        BlockBoundary("block_1", Decimal(60)),
        BlockBoundary("block_2", Decimal(120)),
        BlockBoundary("block_3", None),
    ]
    # Decimal, not int: tariff arithmetic is Decimal throughout (D5).
    assert all(isinstance(b.up_to_kwh, Decimal) for b in boundaries[:2])


def test_local_overlay_deep_merge_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VOLTSTREAM_ENV", "local")

    cfg = get_config()

    # Overridden by config/local.yaml.
    assert cfg.simulation.households == 10
    assert cfg.observability.log_level == "DEBUG"
    # Not touched by local.yaml — inherited from base.yaml unchanged.
    assert cfg.simulation.zones == ["ZONE-A", "ZONE-B", "ZONE-C", "ZONE-D", "ZONE-E"]
    assert cfg.tariff.blocks[0].up_to_kwh == 60


def test_missing_overlay_is_not_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VOLTSTREAM_ENV", "does-not-exist")

    cfg = get_config()

    assert cfg.simulation.households == 50


def test_env_var_override_beats_yaml(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VOLTSTREAM_ENV", "local")  # would otherwise set households=10
    monkeypatch.setenv("VOLTSTREAM__SIMULATION__HOUSEHOLDS", "7")

    cfg = get_config()

    assert cfg.simulation.households == 7


def test_env_var_override_nested_and_typed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VOLTSTREAM__TARIFF__GENERATOR_DEFAULTS__SUBSIDY_PCT", "42.5")
    monkeypatch.setenv("VOLTSTREAM__FAULTS__OUT_OF_ORDER_LATENESS_SIM_MINUTES", "[2, 15]")

    cfg = get_config()

    assert cfg.tariff.generator_defaults.subsidy_pct == 42.5
    assert cfg.faults.out_of_order_lateness_sim_minutes == (2, 15)


def test_unknown_key_in_yaml_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base = yaml.safe_load((_REPO_CONFIG_DIR / "base.yaml").read_text())
    base["simulation"]["bogus_key"] = "surprise"
    (tmp_path / "base.yaml").write_text(yaml.dump(base))
    monkeypatch.setenv("VOLTSTREAM_CONFIG_DIR", str(tmp_path))

    with pytest.raises(ValidationError, match="bogus_key|extra"):
        get_config()


def test_unknown_top_level_section_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base = yaml.safe_load((_REPO_CONFIG_DIR / "base.yaml").read_text())
    base["not_a_real_section"] = {"x": 1}
    (tmp_path / "base.yaml").write_text(yaml.dump(base))
    monkeypatch.setenv("VOLTSTREAM_CONFIG_DIR", str(tmp_path))

    with pytest.raises(ValidationError):
        get_config()


def test_get_config_is_cached_by_identity() -> None:
    first = get_config()
    second = get_config()

    assert first is second


def test_get_config_cache_clear_forces_reload(monkeypatch: pytest.MonkeyPatch) -> None:
    first = get_config()

    get_config.cache_clear()
    monkeypatch.setenv("VOLTSTREAM__SIMULATION__HOUSEHOLDS", "3")
    second = get_config()

    assert first is not second
    assert second.simulation.households == 3


# ---------------------------------------------------------------------------------------
# R14: D2's load-time validation of tariff.blocks.
# ---------------------------------------------------------------------------------------


def _tariff_with(blocks: list[tuple[str, int | None]]) -> dict:
    tariff = get_config().tariff.model_dump()
    tariff["blocks"] = [{"name": n, "up_to_kwh": up} for n, up in blocks]
    return tariff


@pytest.mark.parametrize(
    ("blocks", "rule"),
    [
        ([("block_1", 120), ("block_2", 60), ("block_3", None)], "increasing"),
        ([("block_1", 60), ("block_2", 60), ("block_3", None)], "increasing"),
        ([("block_1", 0), ("block_2", 60), ("block_3", None)], "positive"),
        ([("block_1", 60), ("block_2", 120), ("block_3", 200)], "unbounded"),
        ([("block_1", 60), ("block_2", None), ("block_3", None)], "unbounded"),
        ([("block_1", None), ("block_2", 60), ("block_3", 120)], "unbounded"),
        ([("block_1", 60), ("block_2", None)], "3 blocks"),
    ],
)
def test_a_malformed_block_structure_fails_at_load(
    blocks: list[tuple[str, int | None]], rule: str
) -> None:
    with pytest.raises(ValidationError, match=rule):
        TariffConfig.model_validate(_tariff_with(blocks))


def test_the_shipped_block_structure_is_valid() -> None:
    shipped = [("block_1", 60), ("block_2", 120), ("block_3", None)]
    TariffConfig.model_validate(_tariff_with(shipped))


def test_host_side_processes_take_the_password_env_already_holds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`.env` sets POSTGRES_PASSWORD; `make test-all` on the host had no password at all."""
    monkeypatch.setenv("POSTGRES_PASSWORD", "from-dotenv")
    assert get_config().postgres.password == "from-dotenv"


def test_the_explicit_password_variable_still_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "from-dotenv")
    monkeypatch.setenv("VOLTSTREAM__POSTGRES__PASSWORD", "explicit")
    assert get_config().postgres.password == "explicit"


def test_env_example_and_base_yaml_name_the_same_topics_and_buckets() -> None:
    """The init jobs create topics and buckets from .env; Python reads base.yaml (R30)."""
    from dotenv import dotenv_values

    env = dotenv_values(_REPO_CONFIG_DIR.parent / ".env.example")
    cfg = get_config()
    assert (env["KAFKA_TOPIC"], env["KAFKA_DLQ_TOPIC"]) == (cfg.kafka.topic, cfg.kafka.dlq_topic)
    assert (env["MINIO_BUCKET_RAW"], env["MINIO_BUCKET_LANDING"], env["MINIO_BUCKET_ARCHIVE"]) == (
        cfg.minio.bucket_raw,
        cfg.minio.bucket_landing,
        cfg.minio.bucket_archive,
    )
