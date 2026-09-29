"""The serving API (T103, §5.8).

The generated OpenAPI page is a graded demo artefact, so the title, description and
version below are real rather than FastAPI's defaults — `/docs` is something a marker
opens, not just a debugging aid.

The Postgres pool is opened and closed by the lifespan rather than on first use. Opening
lazily would make the first request after startup pay the connection cost and, worse,
would let the container report healthy before it could serve anything.

`/metrics` is a plain route on this same port, serving the shared registry from
`voltstream.metrics` (see `create_app` for why not the instrumentator). The API does not
call `start_metrics_server()` — that is for processes with no HTTP server of their own.
"""

from __future__ import annotations

import mimetypes
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from voltstream import __version__
from voltstream.api.routers import alerts, clock, health, households, reports, zones
from voltstream.config import get_config
from voltstream.logging_setup import get_logger
from voltstream.metrics import REGISTRY
from voltstream.storage.postgres import close_pool, open_pool

log = get_logger("api")

_DESCRIPTION = """
Serving layer for **voltstream**, a Lambda-architecture platform for smart-grid
monitoring and billing.

Two views over the same meter stream:

* **Zone endpoints** read the speed layer — grid load and renewable contribution over
  15-simulated-minute windows, updated every few seconds.
* **Household endpoints** read whichever layer can answer. Once the batch job has closed
  a simulated day the authoritative bill is served; until then a provisional estimate
  computed against the *previous* day's tariff. Every response says which it is, in
  `source` and `provisional`.

All timestamps are UTC. `event_ts`, `sim_date` and window bounds are **simulated** time,
which runs 288x wall-clock; only latency metrics use the real clock.
"""


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    open_pool()
    log.info("api started", extra={"stage": "api"})
    try:
        yield
    finally:
        close_pool()
        log.info("api stopped", extra={"stage": "api"})


def _dashboard_dir() -> Path | None:
    """Locate `dashboard/`, in the image or in a source checkout.

    Two candidates because the API runs both ways: `/app/dashboard` inside the container,
    and the repository directory when someone runs `voltstream-api` locally.
    """
    candidates = [
        Path(os.environ.get("VOLTSTREAM_DASHBOARD_DIR", "/app/dashboard")),
        Path(__file__).resolve().parents[3] / "dashboard",
    ]
    return next((c for c in candidates if (c / "index.html").is_file()), None)


def create_app() -> FastAPI:
    app = FastAPI(
        title="voltstream serving API",
        description=_DESCRIPTION,
        version=__version__,
        lifespan=lifespan,
        openapi_tags=[
            {"name": "zones", "description": "Real-time grid load and renewable mix."},
            {
                "name": "households",
                "description": "Per-household bills. The merge function lives here.",
            },
            {"name": "reports", "description": "Consolidated daily reports."},
            {"name": "alerts", "description": "Firing alerts, proxied from Alertmanager."},
            {"name": "clock", "description": "The simulated clock, as this process reads it."},
            {"name": "health", "description": "Liveness and dependency readiness."},
        ],
    )

    app.include_router(health.router)
    app.include_router(clock.router)
    app.include_router(zones.router)
    app.include_router(households.router)
    app.include_router(reports.router)
    app.include_router(alerts.router)

    # /metrics serves the shared registry directly rather than through
    # prometheus-fastapi-instrumentator. Two reasons:
    #
    # The instrumentator (7.1) does not work with FastAPI 0.141 — it walks `app.routes`
    # expecting every entry to have `.path`, and newer FastAPI puts `_IncludedRouter`
    # objects there, so every request 500s inside the middleware. Pinning FastAPI back a
    # dozen minor versions to keep one optional dependency is the wrong trade.
    #
    # And what it adds is per-endpoint HTTP counters, which are not among the eight
    # metrics §10.1 names. `voltstream.metrics` is deliberately the only place a metric is
    # defined; exposing its registry is all this endpoint owes the design.
    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

    # The dashboard is served from this same app (T135) so it shares an origin with the
    # API. A separate static server would mean cross-origin requests, and CORS is exactly
    # the sort of thing that goes wrong in front of an audience.
    #
    # Mounted last, at "/", because a mount at the root matches everything — registering
    # it before the routers would shadow /api, /docs and /metrics.
    dashboard = _dashboard_dir()
    if dashboard is not None:
        # The dashboard's scripts are ES modules, which a browser refuses to run unless
        # they arrive as JavaScript. StaticFiles takes the type from `mimetypes`, and on
        # Windows that reads the registry, where `.js` is often `text/plain` — so a local
        # `voltstream-api` would serve a blank page. Registering it here makes the type
        # the same on every platform.
        mimetypes.add_type("text/javascript", ".js")
        app.mount("/", StaticFiles(directory=str(dashboard), html=True), name="dashboard")
    else:
        # Not fatal. The API is useful without the dashboard, and the image that serves
        # the API in a headless test has no reason to carry a static page.
        log.warning("dashboard directory not found; API served without it", extra={"stage": "api"})

    return app


app = create_app()


def run() -> None:
    """Console entry point (`voltstream-api`)."""
    import uvicorn

    config = get_config()
    uvicorn.run(
        "voltstream.api.main:app",
        host=config.api.host,
        port=config.api.port,
        # Logging is the structured JSON envelope from logging_setup; uvicorn's own
        # access log would emit a second, differently shaped line for every request.
        access_log=False,
    )
