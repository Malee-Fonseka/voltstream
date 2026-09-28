"""Static checks on docker/docker-compose*.yml (T177, T178, §8.2).

Startup order is a property of the file, so it is tested on the file: every service that
writes to Kafka, Postgres or the object store waits for the init job that creates what it
writes to. Without it, a service starts, fails its first write against a topic, table or
bucket that does not exist yet, and crash-loops until it happens to win the race.

The dev override is checked the same way: it may only change development settings, and
nothing on the demo path may load it.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]
_BASE = yaml.safe_load((_REPO / "docker" / "docker-compose.yml").read_text(encoding="utf-8"))
_OVERRIDE = yaml.safe_load(
    (_REPO / "docker" / "docker-compose.override.yml").read_text(encoding="utf-8")
)

# What each service needs to exist before its first write, per §8.2's tiers.
_NEEDS_INIT = {
    "meter-producer": {"kafka-init"},
    "reference-dropper": {"minio-init"},
    "raw-archiver": {"kafka-init", "minio-init"},
    "speed-layer": {"kafka-init", "postgres-init", "minio-init"},
    "api": {"postgres-init"},
    "airflow": {"postgres-init"},
    "sql-exporter": {"postgres-init"},
    "grafana": {"postgres-init"},
}


def _depends_on(service: str) -> dict[str, Any]:
    deps = _BASE["services"][service].get("depends_on") or {}
    return deps if isinstance(deps, dict) else {name: {} for name in deps}


@pytest.mark.parametrize("service", sorted(_NEEDS_INIT))
def test_a_service_waits_for_the_init_jobs_it_writes_through(service: str) -> None:
    deps = _depends_on(service)
    for init in _NEEDS_INIT[service]:
        assert deps.get(init, {}).get("condition") == "service_completed_successfully", (
            f"{service} must wait for {init} to complete successfully"
        )


def test_every_init_job_waits_for_its_server_to_be_healthy() -> None:
    for init, server in (
        ("kafka-init", "kafka"),
        ("postgres-init", "postgres"),
        ("minio-init", "minio"),
    ):
        assert _depends_on(init)[server]["condition"] == "service_healthy", init


def test_every_infrastructure_service_has_a_healthcheck() -> None:
    """`service_healthy` means nothing without one, and `up --wait` waits on them."""
    for service in ("kafka", "postgres", "minio", "api", "prometheus", "alertmanager", "grafana"):
        assert "healthcheck" in _BASE["services"][service], service


# --------------------------------------------------------------------------------------
# T178 — the dev override
# --------------------------------------------------------------------------------------


def test_the_override_only_changes_development_settings() -> None:
    allowed = {"command", "environment", "volumes", "ports"}
    for service, settings in _OVERRIDE["services"].items():
        assert service in _BASE["services"], service
        assert set(settings) <= allowed, f"{service}: {set(settings) - allowed}"


def test_the_demo_path_never_loads_the_override() -> None:
    """Compose auto-loads docker-compose.override.yml only when no -f is given. Every compose
    command the scripts and the Makefile run must name the base file explicitly."""
    sources = {
        "Makefile": (_REPO / "Makefile").read_text(encoding="utf-8"),
        "voltstream.ps1": (_REPO / "scripts" / "voltstream.ps1").read_text(encoding="utf-8"),
        "lib/common.sh": (_REPO / "scripts" / "lib" / "common.sh").read_text(encoding="utf-8"),
        "smoke_test.sh": (_REPO / "scripts" / "smoke_test.sh").read_text(encoding="utf-8"),
    }
    for name, text in sources.items():
        for line in text.splitlines():
            if re.search(r"\bdocker compose\b", line) and not line.lstrip().startswith("#"):
                assert re.search(r"-f\s", line), f"{name}: {line.strip()}"
                assert "override" not in line, f"{name}: {line.strip()}"
