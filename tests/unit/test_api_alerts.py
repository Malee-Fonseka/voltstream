"""Unit tests for the alerts router (T132, T149).

A minimal app with the real router, so no Postgres or Alertmanager is needed. The webhook
payloads follow Alertmanager's version-4 format, including the fields the receiver does
not use: those must be ignored, not rejected.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from voltstream.api.routers import alerts

app = FastAPI()
app.include_router(alerts.router)
client = TestClient(app)


def _notification(status: str, **labels: str) -> dict[str, Any]:
    alert_labels = {"alertname": "MeterDataStale", "severity": "critical", **labels}
    return {
        "version": "4",
        "groupKey": '{}/{severity="critical"}:{alertname="MeterDataStale"}',
        "truncatedAlerts": 0,
        "status": status,
        "receiver": "critical-log",
        "groupLabels": {"alertname": "MeterDataStale"},
        "commonLabels": alert_labels,
        "commonAnnotations": {},
        "externalURL": "http://alertmanager:9093",
        "alerts": [
            {
                "status": status,
                "labels": alert_labels,
                "annotations": {"summary": "No readings from ZONE-A for 2m 40s"},
                "startsAt": "2026-09-27T13:00:00Z",
                # Alertmanager's "not ended" value for a firing alert.
                "endsAt": "0001-01-01T00:00:00Z",
                "generatorURL": "http://prometheus:9090/graph",
                "fingerprint": "2f1b9c",
            }
        ],
    }


def _alert_lines(output: str) -> list[dict[str, Any]]:
    lines = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
    return [line for line in lines if line.get("stage") == "alert"]


def test_a_firing_alert_is_logged_with_its_route_and_zone(
    capsys: pytest.CaptureFixture[str],
) -> None:
    response = client.post(
        "/api/v1/alerts/webhook", json=_notification("firing", grid_zone="ZONE-A")
    )

    assert response.status_code == 200
    assert response.json() == {"received": 1}
    [line] = _alert_lines(capsys.readouterr().out)
    assert line["level"] == "WARNING"
    assert line["msg"] == "alert firing"
    assert line["receiver"] == "critical-log"
    assert line["alertname"] == "MeterDataStale"
    assert line["severity"] == "critical"
    assert line["grid_zone"] == "ZONE-A"
    assert line["summary"] == "No readings from ZONE-A for 2m 40s"
    assert line["starts_at"].startswith("2026-09-27T13:00:00")


def test_a_resolution_is_logged_at_info(capsys: pytest.CaptureFixture[str]) -> None:
    client.post("/api/v1/alerts/webhook", json=_notification("resolved", grid_zone="ZONE-A"))

    [line] = _alert_lines(capsys.readouterr().out)
    assert line["level"] == "INFO"
    assert line["msg"] == "alert resolved"


def test_an_alert_without_a_zone_logs_no_zone(capsys: pytest.CaptureFixture[str]) -> None:
    client.post("/api/v1/alerts/webhook", json=_notification("firing"))

    [line] = _alert_lines(capsys.readouterr().out)
    assert "grid_zone" not in line


def test_a_payload_that_is_not_a_notification_is_rejected() -> None:
    response = client.post("/api/v1/alerts/webhook", json={"receiver": "critical-log"})
    assert response.status_code == 422


def test_status_lists_only_active_alerts() -> None:
    """Alertmanager's v2 API also returns suppressed (inhibited or silenced) alerts;
    showing those on the dashboard would report a problem someone has already handled."""
    payload = [
        {
            "status": {"state": "active"},
            "labels": {"alertname": "MeterDataStale", "severity": "critical"},
            "annotations": {"summary": "No readings from ZONE-A for 2m 40s"},
            "startsAt": "2026-09-27T13:00:00Z",
        },
        {
            "status": {"state": "suppressed"},
            "labels": {"alertname": "LowRenewableContribution", "severity": "warning"},
            "annotations": {},
            "startsAt": "2026-09-27T12:58:00Z",
        },
    ]

    [alert] = alerts._parse(payload)
    assert (alert.name, alert.severity) == ("MeterDataStale", "critical")
