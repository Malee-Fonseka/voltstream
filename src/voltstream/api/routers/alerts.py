"""Firing alerts, proxied from Alertmanager (T132, §10.1).

**Alertmanager being unreachable is not an API failure.** This endpoint returns an empty
list and a warning rather than a 5xx, and `/health/ready` does not consider it at all.
The reasoning matters: monitoring exists to observe the system, and a system that reports
itself unhealthy because its monitoring is down has inverted the relationship. Worse, in a
real deployment that is how a monitoring outage becomes a service outage — readiness flips,
the orchestrator pulls the container out of rotation, and a working API stops serving
because a sidecar died.

So the failure is reported *in the payload*, where a dashboard can show "alerts
unavailable" while continuing to display everything else.

The same router is Alertmanager's receiver (T149): `POST /webhook` writes each notification
as a structured log line, `stage: alert`, so an alert lands in the one JSON log stream the
rest of the pipeline already writes to and can be found with the same grep. It is the only
write-shaped endpoint in the API and it writes nothing but that log line; the database
stays read-only from here.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.request

from fastapi import APIRouter

from voltstream.api.dependencies import TraceIdDep
from voltstream.api.models import (
    AlertmanagerNotification,
    AlertsResponse,
    AlertStatus,
    WebhookAck,
)
from voltstream.logging_setup import get_logger

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])

log = get_logger("api")

# Short: this endpoint is polled by a dashboard, and a slow monitoring stack must not turn
# into a slow dashboard. Failing fast and saying so beats blocking the page.
_TIMEOUT_SECONDS = 2.0


def _alertmanager_url() -> str:
    base = os.environ.get("ALERTMANAGER_URL", "http://alertmanager:9093")
    return f"{base.rstrip('/')}/api/v2/alerts"


def _parse(payload: list[dict]) -> list[AlertStatus]:
    """Map Alertmanager's v2 payload onto our shape.

    Only firing alerts: a resolved alert lingers in the API for a while after it clears,
    and showing it on a dashboard would report a problem that no longer exists.
    """
    alerts: list[AlertStatus] = []
    for item in payload:
        state = (item.get("status") or {}).get("state")
        if state != "active":
            continue
        labels = item.get("labels") or {}
        annotations = item.get("annotations") or {}
        alerts.append(
            AlertStatus(
                name=labels.get("alertname", "unknown"),
                severity=labels.get("severity", "unknown"),
                summary=annotations.get("summary", ""),
                since=item.get("startsAt"),
            )
        )
    return alerts


@router.get("/status", response_model=AlertsResponse, summary="Currently firing alerts")
def alert_status(trace_id: TraceIdDep) -> AlertsResponse:
    """Firing alerts, or an empty list with a warning when Alertmanager is unreachable."""
    import json

    url = _alertmanager_url()
    try:
        with urllib.request.urlopen(url, timeout=_TIMEOUT_SECONDS) as response:  # noqa: S310
            payload = json.loads(response.read())
        return AlertsResponse(alerts=_parse(payload), available=True, warning=None)
    except (urllib.error.URLError, TimeoutError, ValueError, OSError) as exc:
        # Warning, not error: this is an expected state before Phase 12 exists, and a
        # degraded one afterwards — neither is an API fault.
        log.warning(
            "alertmanager unreachable",
            extra={"stage": "api", "url": url, "detail": str(exc)[:200]},
        )
        return AlertsResponse(
            alerts=[],
            available=False,
            warning=f"Alertmanager unreachable at {url}; alert state is unknown, not empty.",
        )


@router.post(
    "/webhook",
    response_model=WebhookAck,
    summary="Alertmanager webhook receiver",
)
def alertmanager_webhook(
    notification: AlertmanagerNotification, trace_id: TraceIdDep
) -> WebhookAck:
    """Log each alert in an Alertmanager notification as one structured line.

    Firing alerts at WARNING, resolutions at INFO, so a level filter shows what is wrong
    now. The line carries the receiver, which is the route the alert took
    (config/alertmanager/alertmanager.yml), and the zone when the alert has one.
    """
    for alert in notification.alerts:
        extra = {
            "stage": "alert",
            "receiver": notification.receiver,
            "alert_status": alert.status,
            "alertname": alert.labels.get("alertname", "unknown"),
            "severity": alert.labels.get("severity", "unknown"),
            "summary": alert.annotations.get("summary", ""),
            "starts_at": alert.startsAt.isoformat() if alert.startsAt else None,
        }
        if "grid_zone" in alert.labels:
            extra["grid_zone"] = alert.labels["grid_zone"]
        if alert.status == "firing":
            log.warning("alert firing", extra=extra)
        else:
            log.info("alert resolved", extra=extra)
    return WebhookAck(received=len(notification.alerts))
