from __future__ import annotations

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    push_to_gateway,
    start_http_server,
)

from voltstream.config import get_config
from voltstream.logging_setup import get_logger

# One registry, shared by every metric below and by whichever server exposes them —
# either start_metrics_server() (non-HTTP services) or the API's own /metrics route.
REGISTRY = CollectorRegistry()

# Seconds. Covers "well under a second" up to a few minutes, log-ish spacing, so both the
# speed-layer's sub-60s target and an occasional slow outlier land in a meaningful bucket.
_E2E_LATENCY_BUCKETS_SECONDS = (
    0.5,
    1,
    2,
    5,
    10,
    15,
    30,
    60,
    120,
    300,
    600,
)

# Seconds. Covers "well under a minute" up to "worryingly slow", for a nightly batch job
# whose SLA (alerts.batch_sla_minutes) is expressed in minutes, not seconds.
_BATCH_DURATION_BUCKETS_SECONDS = (
    10,
    30,
    60,
    120,
    300,
    600,
    900,
    1800,
    3600,
)

events_produced_total = Counter(
    "voltstream_events_produced_total",
    "Meter readings published by a producer.",
    labelnames=("producer_id",),
    registry=REGISTRY,
)

events_consumed_total = Counter(
    "voltstream_events_consumed_total",
    "Meter readings consumed by a processing layer.",
    labelnames=("layer",),
    registry=REGISTRY,
)

records_rejected_total = Counter(
    "voltstream_records_rejected_total",
    "Records failing validation, by layer and rejection reason.",
    labelnames=("layer", "reason"),
    registry=REGISTRY,
)

e2e_latency_seconds = Histogram(
    "voltstream_e2e_latency_seconds",
    "End-to-end latency from Kafka record timestamp to sink commit, by layer.",
    labelnames=("layer",),
    buckets=_E2E_LATENCY_BUCKETS_SECONDS,
    registry=REGISTRY,
)

consumer_lag = Gauge(
    "voltstream_consumer_lag",
    "Kafka consumer lag in records, by layer and partition.",
    labelnames=("layer", "partition"),
    registry=REGISTRY,
)

zone_renewable_ratio = Gauge(
    "voltstream_zone_renewable_ratio",
    "Solar generation as a fraction of total consumption, by grid zone.",
    labelnames=("grid_zone",),
    registry=REGISTRY,
)

batch_duration_seconds = Histogram(
    "voltstream_batch_duration_seconds",
    "Wall-clock duration of a batch job run, by job name.",
    labelnames=("job",),
    buckets=_BATCH_DURATION_BUCKETS_SECONDS,
    registry=REGISTRY,
)

lambda_divergence = Gauge(
    "voltstream_lambda_divergence",
    "Mean percent divergence between the speed and batch layers for the current day.",
    registry=REGISTRY,
)


def start_metrics_server(port: int | None = None) -> None:
    start_http_server(port or get_config().observability.metrics_port, registry=REGISTRY)


def push_metrics(job: str, *, timeout_seconds: float = 5.0) -> bool:
    url = get_config().observability.pushgateway_url
    if not url:
        return False
    try:
        push_to_gateway(url, job=job, registry=REGISTRY, timeout=timeout_seconds)
    except OSError as exc:  # URLError, HTTP error statuses and timeouts are all OSError
        get_logger("metrics").warning(
            "could not push metrics to the Pushgateway",
            extra={"stage": "metrics", "job": job, "url": url, "detail": str(exc)[:200]},
        )
        return False
    return True
