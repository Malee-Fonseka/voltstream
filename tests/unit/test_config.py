"""Unit tests for voltstream.config (T022)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

import voltstream.config as config_module
from voltstream.config import get_config

_REPO_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


@pytest.fixture(autouse=True)
def _isolated_config_cache(monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setenv(
        "VOLTSTREAM__FAULTS__OUT_OF_ORDER_LATENESS_SIM_MINUTES", "[2, 15]"
    )

    cfg = get_config()

    assert cfg.tariff.generator_defaults.subsidy_pct == 42.5
    assert cfg.faults.out_of_order_lateness_sim_minutes == (2, 15)


def test_unknown_key_in_yaml_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = yaml.safe_load((_REPO_CONFIG_DIR / "base.yaml").read_text())
    base["simulation"]["bogus_key"] = "surprise"
    (tmp_path / "base.yaml").write_text(yaml.dump(base))
    monkeypatch.setenv("VOLTSTREAM_CONFIG_DIR", str(tmp_path))

    with pytest.raises(ValidationError, match="bogus_key|extra"):
        get_config()


def test_unknown_top_level_section_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
