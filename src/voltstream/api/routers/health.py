"""Liveness and readiness (T105, §10.1).

The split matters to the orchestrator, not just to us.

`/health/live` answers "is this process running" and touches **nothing**. If it checked
Postgres, then a database blip would make Compose or Kubernetes restart a perfectly
healthy API — which cannot fix the database and throws away the API's warm state on the
way. Liveness failing should mean "restart me"; a dependency being down never means that.

`/health/ready` answers "can this process serve requests", checks Postgres and the object
store, and returns 503 naming whichever is unreachable. Readiness failing should mean
"stop sending me traffic", which is the correct response to a dependency outage.
"""

from __future__ import annotations

from fastapi import APIRouter, Response, status

from voltstream.api.models import DependencyHealth, LivenessResponse, ReadinessResponse
from voltstream.storage import objectstore, postgres

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live", response_model=LivenessResponse, summary="Process liveness")
def live() -> LivenessResponse:
    """Up. Deliberately touches no dependency — see the module docstring."""
    return LivenessResponse(alive=True)


@router.get(
    "/ready",
    response_model=ReadinessResponse,
    summary="Dependency readiness",
    responses={503: {"model": ReadinessResponse, "description": "A dependency is unreachable"}},
)
def ready(response: Response) -> ReadinessResponse:
    """Postgres and the object store, each reported separately.

    Named per dependency so an operator reading a 503 knows which one to look at rather
    than checking both by hand.
    """
    dependencies = [
        DependencyHealth(
            name="postgres",
            healthy=postgres.healthcheck(),
            detail=None,
        ),
        DependencyHealth(
            name="objectstore",
            healthy=objectstore.healthcheck(),
            detail=objectstore.endpoint_url() or None,
        ),
    ]
    ready_now = all(d.healthy for d in dependencies)
    if not ready_now:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessResponse(ready=ready_now, dependencies=dependencies)
