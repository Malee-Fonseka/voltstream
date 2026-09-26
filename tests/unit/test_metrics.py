"""Unit tests for voltstream.metrics (T028)."""

from __future__ import annotations

import importlib
from collections.abc import Iterator

import pytest
from prometheus_client import generate_latest

from voltstream import metrics

_EXPECTED_NAMES = {
    "voltstream_events_produced_total",
    "voltstream_events_consumed_total",
    "voltstream_records_rejected_total",
    "voltstream_e2e_latency_seconds",
    "voltstream_consumer_lag",
    "voltstream_zone_renewable_ratio",
    "voltstream_batch_duration_seconds",
    "voltstream_lambda_divergence",
}


def _exposition_text() -> str:
    return generate_latest(metrics.REGISTRY).decode("utf-8")


def test_all_eight_metric_names_present() -> None:
    text = _exposition_text()
    for name in _EXPECTED_NAMES:
        assert f"# TYPE {name}" in text, f"missing metric: {name}"
        assert f"# HELP {name}" in text, f"missing HELP line: {name}"


def test_no_unexpected_metrics_on_the_registry() -> None:
    text = _exposition_text()
    declared_names = {line.split()[2] for line in text.splitlines() if line.startswith("# TYPE ")}
    assert declared_names == _EXPECTED_NAMES


def test_label_sets_match_the_spec() -> None:
    # Exercise every metric once so its label-bearing lines appear in the exposition.
    metrics.events_produced_total.labels(producer_id="p1").inc()
    metrics.events_consumed_total.labels(layer="speed").inc()
    metrics.records_rejected_total.labels(layer="speed", reason="negative_kwh").inc()
    metrics.e2e_latency_seconds.labels(layer="speed").observe(1.2)
    metrics.consumer_lag.labels(layer="speed", partition="0").set(3)
    metrics.zone_renewable_ratio.labels(grid_zone="ZONE-A").set(0.4)
    metrics.batch_duration_seconds.labels(job="daily_billing").observe(120)
    metrics.lambda_divergence.set(1.7)

    text = _exposition_text()
    assert 'producer_id="p1"' in text
    assert 'layer="speed"' in text
    assert 'reason="negative_kwh"' in text
    assert 'partition="0"' in text
    assert 'grid_zone="ZONE-A"' in text
    assert 'job="daily_billing"' in text
    # voltstream_lambda_divergence is a label-less gauge per §10.1.
    assert "voltstream_lambda_divergence 1.7" in text


def test_histogram_buckets_are_explicit_not_library_defaults() -> None:
    text = _exposition_text()
    # prometheus_client's own default bucket ceiling is 10.0; both histograms here go
    # well past it, which is only possible if the defaults were overridden.
    assert 'voltstream_e2e_latency_seconds_bucket{layer="speed",le="300.0"}' in text
    assert 'voltstream_batch_duration_seconds_bucket{job="daily_billing",le="3600.0"}' in text


def test_reimporting_the_module_does_not_raise_duplicate_timeseries() -> None:
    # A real hazard once Spark re-imports this module per executor process (T028). Using
    # a dedicated CollectorRegistry per module execution (not prometheus_client's global
    # default REGISTRY) is what makes a reload safe.
    importlib.reload(metrics)
    importlib.reload(metrics)


def test_start_metrics_server_uses_configured_port_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_start_http_server(port: int, registry: object) -> None:
        calls["port"] = port
        calls["registry"] = registry

    monkeypatch.setattr(metrics, "start_http_server", fake_start_http_server)
    metrics.start_metrics_server()

    from voltstream.config import get_config

    assert calls["port"] == get_config().observability.metrics_port
    assert calls["registry"] is metrics.REGISTRY


@pytest.fixture
def _fresh_config() -> Iterator[None]:
    """Reload config around a test that changes a VOLTSTREAM__* variable."""
    from voltstream.config import get_config

    get_config.cache_clear()
    yield
    get_config.cache_clear()


def test_push_metrics_does_nothing_without_a_pushgateway(
    monkeypatch: pytest.MonkeyPatch, _fresh_config: None
) -> None:
    monkeypatch.delenv("VOLTSTREAM__OBSERVABILITY__PUSHGATEWAY_URL", raising=False)
    calls: list[object] = []
    monkeypatch.setattr(metrics, "push_to_gateway", lambda *a, **k: calls.append((a, k)))

    assert metrics.push_metrics("reconciliation") is False
    assert calls == []


def test_push_metrics_pushes_the_shared_registry(
    monkeypatch: pytest.MonkeyPatch, _fresh_config: None
) -> None:
    monkeypatch.setenv("VOLTSTREAM__OBSERVABILITY__PUSHGATEWAY_URL", "http://pushgateway:9091")
    calls: dict[str, object] = {}

    def fake_push(gateway: str, job: str, registry: object, timeout: float) -> None:
        calls.update(gateway=gateway, job=job, registry=registry, timeout=timeout)

    monkeypatch.setattr(metrics, "push_to_gateway", fake_push)

    assert metrics.push_metrics("reconciliation") is True
    assert calls == {
        "gateway": "http://pushgateway:9091",
        "job": "reconciliation",
        "registry": metrics.REGISTRY,
        "timeout": 5.0,
    }
