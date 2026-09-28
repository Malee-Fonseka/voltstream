"""Consistency tests for the observability configuration (T142-T156).

Prometheus, Alertmanager, Grafana and sql_exporter each read their own files and none of
them can read config/base.yaml or import voltstream.metrics. So every threshold, metric
name, port and datasource reference below is written down twice, once in Python-land and
once in their configuration. These tests fail when the two copies disagree: a threshold
changed in base.yaml but not in the rule, a metric renamed in metrics.py but not on the
dashboard, a panel pointing at a datasource that does not exist.

The rules' behaviour is tested separately, by promtool against synthetic series
(config/prometheus/alert_rules.test.yml, `make check-alerts`).
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

from voltstream import metrics
from voltstream.api.routers import alerts as alerts_router
from voltstream.config import get_config

_REPO = Path(__file__).resolve().parents[2]
_CONFIG = _REPO / "config"
_DASHBOARDS = sorted((_CONFIG / "grafana" / "dashboards").glob("*.json"))


@pytest.fixture(autouse=True)
def _clear_config_cache() -> Iterator[None]:
    get_config.cache_clear()
    yield
    get_config.cache_clear()


def _yaml(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _rules() -> dict[str, dict[str, Any]]:
    groups = _yaml(_CONFIG / "prometheus" / "alert_rules.yml")["groups"]
    return {rule["alert"]: rule for group in groups for rule in group["rules"]}


def _threshold(expr: str, op: str) -> float:
    """The number compared against in the rule's first `op` comparison."""
    match = re.search(rf"\)?\s*{re.escape(op)}\s*([0-9.]+)", expr)
    assert match, f"no '{op} <number>' in {expr!r}"
    return float(match.group(1))


def _duration_seconds(value: str) -> float:
    units = {"s": 1, "m": 60, "h": 3600}
    return sum(float(n) * units[u] for n, u in re.findall(r"(\d+)([smh])", value))


# --------------------------------------------------------------------------------------
# Alert rules against config/base.yaml
# --------------------------------------------------------------------------------------


def test_the_five_rules_from_the_design_exist() -> None:
    assert set(_rules()) == {
        "MeterDataStale",
        "LowRenewableContribution",
        "HighRejectRate",
        "BatchSLAMiss",
        "LambdaDivergenceHigh",
    }


def test_rule_thresholds_match_base_yaml() -> None:
    config = get_config()
    alerts = config.alerts
    rules = _rules()
    real_minutes_per_sim_day = 24 * 60 / config.simulation.time_scale

    assert _threshold(rules["MeterDataStale"]["expr"], ">") == alerts.stale_data_minutes * 60
    assert _threshold(rules["LowRenewableContribution"]["expr"], "<") == pytest.approx(
        alerts.low_renewable_threshold
    )
    assert _threshold(rules["HighRejectRate"]["expr"], ">") == pytest.approx(
        alerts.reject_rate_threshold
    )
    assert _threshold(rules["BatchSLAMiss"]["expr"], ">") == pytest.approx(
        (real_minutes_per_sim_day + alerts.batch_sla_minutes) * 60
    )
    assert _threshold(rules["LambdaDivergenceHigh"]["expr"], ">") == pytest.approx(
        alerts.lambda_divergence_threshold_pct
    )


def test_low_renewable_waits_at_least_three_windows() -> None:
    """T145: three consecutive 15-simulated-minute windows, in real seconds."""
    config = get_config()
    three_windows_real_seconds = (
        3 * config.speed_layer.window_sim_minutes * 60 / config.simulation.time_scale
    )
    held_for = _duration_seconds(_rules()["LowRenewableContribution"]["for"])
    assert held_for >= three_windows_real_seconds


def test_meter_data_stale_has_a_for_clause() -> None:
    """T144 asks for it explicitly."""
    assert _duration_seconds(_rules()["MeterDataStale"]["for"]) > 0


def test_divergence_is_read_from_the_job_that_sets_it() -> None:
    """voltstream_lambda_divergence is unlabelled, so every process exports it at 0; only
    the reconciliation job's series is real. The rule and every panel must select it."""
    from voltstream.batch import reconciliation

    selector = f'voltstream_lambda_divergence{{job="{reconciliation._JOB}"}}'
    assert selector in _rules()["LambdaDivergenceHigh"]["expr"]
    for expr in _prometheus_exprs():
        if "voltstream_lambda_divergence" in expr:
            assert selector in expr, expr


def test_every_rule_is_routable_and_explained() -> None:
    for name, rule in _rules().items():
        assert rule["labels"]["severity"] in {"critical", "warning"}, name
        assert rule["annotations"]["summary"], name
        assert rule["annotations"]["description"], name


# --------------------------------------------------------------------------------------
# Metric names: every one a rule or a panel uses must exist somewhere
# --------------------------------------------------------------------------------------


def _application_series_names() -> set[str]:
    """What metrics.py's registry exposes, with each type's sample-name suffixes."""
    suffixes = {
        "counter": ("_total", "_created"),
        "histogram": ("_bucket", "_sum", "_count", "_created"),
    }
    names: set[str] = set()
    for family in metrics.REGISTRY.collect():
        names.add(family.name)
        names.update(family.name + s for s in suffixes.get(family.type, ()))
    return names


def _exporter_series_names() -> set[str]:
    collectors = (_CONFIG / "sql_exporter").glob("*.collector.yml")
    return {m["metric_name"] for path in collectors for m in _yaml(path)["metrics"]}


def _voltstream_names(expr: str) -> set[str]:
    return set(re.findall(r"\bvoltstream_[a-z0-9_]+\b", expr))


def _prometheus_exprs() -> list[str]:
    exprs = [rule["expr"] for rule in _rules().values()]
    for path in _DASHBOARDS:
        for panel in json.loads(path.read_text(encoding="utf-8"))["panels"]:
            exprs += [t["expr"] for t in panel.get("targets", []) if "expr" in t]
    return exprs


def test_every_metric_a_rule_or_panel_reads_exists() -> None:
    known = _application_series_names() | _exporter_series_names()
    for expr in _prometheus_exprs():
        unknown = _voltstream_names(expr) - known
        assert not unknown, f"{sorted(unknown)} in {expr!r} is not exported by anything"


def test_the_exporter_does_not_shadow_an_application_metric() -> None:
    """sql_exporter's gauges are voltstream_pg_*; metrics.py owns the rest (T027)."""
    for name in _exporter_series_names():
        assert name.startswith("voltstream_pg_"), name
    assert not _exporter_series_names() & _application_series_names()


# --------------------------------------------------------------------------------------
# Scrape targets and wiring against docker-compose.yml
# --------------------------------------------------------------------------------------


def _compose() -> dict[str, Any]:
    return _yaml(_REPO / "docker" / "docker-compose.yml")


def test_every_scrape_target_is_a_compose_service() -> None:
    services = set(_compose()["services"])
    config = _yaml(_CONFIG / "prometheus" / "prometheus.yml")
    for job in config["scrape_configs"]:
        for static in job["static_configs"]:
            for target in static["targets"]:
                host, _port = target.split(":")
                assert host == "localhost" or host in services, f"{job['job_name']}: {target}"


def test_the_voltstream_processes_are_scraped_on_their_metrics_ports() -> None:
    config = get_config()
    jobs = {
        job["job_name"]: job["static_configs"][0]["targets"][0]
        for job in _yaml(_CONFIG / "prometheus" / "prometheus.yml")["scrape_configs"]
    }
    port = config.observability.metrics_port
    assert jobs["api"] == f"api:{config.api.port}"
    assert jobs["meter-producer"] == f"meter-producer:{port}"
    assert jobs["speed-layer"] == f"speed-layer:{port}"
    assert jobs["raw-archiver"] == f"raw-archiver:{port}"


def test_the_pushgateway_is_scraped_with_the_jobs_own_labels() -> None:
    jobs = {
        j["job_name"]: j for j in _yaml(_CONFIG / "prometheus" / "prometheus.yml")["scrape_configs"]
    }
    assert jobs["pushgateway"]["honor_labels"] is True


def test_the_dag_pushes_to_the_compose_pushgateway() -> None:
    dag = (_REPO / "airflow" / "dags" / "daily_billing_dag.py").read_text(encoding="utf-8")
    assert '_PUSHGATEWAY_URL = "http://pushgateway:9091"' in dag
    assert "pushgateway" in _compose()["services"]
    # Billing, the rollup and reconciliation push; the report does not.
    assert dag.count('"VOLTSTREAM__OBSERVABILITY__PUSHGATEWAY_URL": _PUSHGATEWAY_URL') == 3


# --------------------------------------------------------------------------------------
# Alertmanager
# --------------------------------------------------------------------------------------


def _alertmanager() -> dict[str, Any]:
    return _yaml(_CONFIG / "alertmanager" / "alertmanager.yml")


def test_inhibition_names_real_rules_and_a_label_they_carry() -> None:
    rules = _rules()
    [inhibit] = _alertmanager()["inhibit_rules"]
    assert inhibit["source_matchers"] == ['alertname="MeterDataStale"']
    assert inhibit["target_matchers"] == ['alertname="LowRenewableContribution"']
    # Both alerts are per zone: their expressions keep grid_zone, so `equal` can match.
    assert inhibit["equal"] == ["grid_zone"]
    assert "grid_zone" in rules["MeterDataStale"]["annotations"]["summary"]
    assert "grid_zone" in rules["LowRenewableContribution"]["annotations"]["summary"]


def test_every_severity_has_a_route() -> None:
    routed = {
        route["matchers"][0].split("=")[1].strip('"')
        for route in _alertmanager()["route"]["routes"]
    }
    assert {rule["labels"]["severity"] for rule in _rules().values()} <= routed


def test_receivers_post_to_the_api_webhook() -> None:
    webhook_paths = {route.path for route in alerts_router.router.routes if "POST" in route.methods}  # type: ignore[attr-defined]
    for receiver in _alertmanager()["receivers"]:
        for hook in receiver["webhook_configs"]:
            url = hook["url"]
            assert url.startswith(f"http://api:{get_config().api.port}/"), url
            assert url.split(":8000", 1)[1] in webhook_paths, url


# --------------------------------------------------------------------------------------
# Grafana
# --------------------------------------------------------------------------------------


def _datasource_uids() -> dict[str, str]:
    provisioned = _yaml(_CONFIG / "grafana" / "provisioning" / "datasources.yml")["datasources"]
    return {ds["uid"]: ds["type"] for ds in provisioned}


def test_the_three_dashboards_exist() -> None:
    assert [p.name for p in _DASHBOARDS] == [
        "grid-operations.json",
        "lambda-divergence.json",
        "pipeline-health.json",
    ]


@pytest.mark.parametrize("path", _DASHBOARDS, ids=lambda p: p.stem)
def test_every_panel_uses_a_provisioned_datasource(path: Path) -> None:
    uids = _datasource_uids()
    board = json.loads(path.read_text(encoding="utf-8"))
    refs = [v["datasource"] for v in board["templating"]["list"] if "datasource" in v]
    for panel in board["panels"]:
        refs += [panel["datasource"]] if "datasource" in panel else []
        refs += [t["datasource"] for t in panel.get("targets", [])]
    assert refs, path.name
    for ref in refs:
        assert uids.get(ref["uid"]) == ref["type"], f"{path.name}: {ref}"


@pytest.mark.parametrize("path", _DASHBOARDS, ids=lambda p: p.stem)
def test_panels_fit_the_grid_and_have_unique_ids(path: Path) -> None:
    board = json.loads(path.read_text(encoding="utf-8"))
    ids = [panel["id"] for panel in board["panels"]]
    assert len(ids) == len(set(ids)), path.name
    for panel in board["panels"]:
        grid = panel["gridPos"]
        assert grid["x"] + grid["w"] <= 24, f"{path.name}: {panel['title']}"


def test_dashboard_uids_are_unique() -> None:
    uids = [json.loads(p.read_text(encoding="utf-8"))["uid"] for p in _DASHBOARDS]
    assert len(uids) == len(set(uids))


def test_the_home_dashboard_is_one_that_is_provisioned() -> None:
    env = _compose()["services"]["grafana"]["environment"]
    home = Path(env["GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH"]).name
    assert home in {p.name for p in _DASHBOARDS}


# --------------------------------------------------------------------------------------
# sim_clock, the view Grafana maps simulated time with
# --------------------------------------------------------------------------------------


def test_sim_clock_uses_the_configured_epoch_and_time_scale() -> None:
    sql = (_REPO / "docker" / "init" / "postgres" / "04_observability.sql").read_text(
        encoding="utf-8"
    )
    view = sql[sql.index("CREATE OR REPLACE VIEW sim_clock") :]
    simulation = get_config().simulation
    epoch = simulation.epoch_sim.strftime("%Y-%m-%d %H:%M:%S+00")

    assert view.count(f"TIMESTAMPTZ '{epoch}'") == 2
    assert view.count(f"{simulation.time_scale}") == 2


# --------------------------------------------------------------------------------------
# T156: the metric table in 04-observability.md is metrics.py, exactly
# --------------------------------------------------------------------------------------


def test_the_documented_metric_table_matches_metrics_py() -> None:
    from prometheus_client.metrics import MetricWrapperBase

    doc = (_REPO / "docs" / "architecture" / "04-observability.md").read_text(encoding="utf-8")
    section = doc[doc.index("### 3.1 Application metrics") : doc.index("### 3.2")]
    documented = set()
    for row in re.findall(r"^\| `(voltstream_[a-z0-9_]+)` \| (\w+) \| ([^|]+) \|", section, re.M):
        name, kind, labels = row
        label_set = tuple(sorted(re.findall(r"`([a-z_]+)`", labels)))
        documented.add((name, kind.lower(), label_set))

    defined = set()
    for value in vars(metrics).values():
        if isinstance(value, MetricWrapperBase):
            # prometheus_client strips a counter's `_total`; the exposed name keeps it.
            name = value._name + ("_total" if value._type == "counter" else "")
            defined.add((name, value._type, tuple(sorted(value._labelnames))))

    assert documented == defined
