"""Unit tests for the API's trace-id dependency (T104, R05).

A minimal app with the real `TraceIdDep`, so no Postgres is needed. `TestClient` runs in its
default mode, which re-raises server-side exceptions: before R05 every request raised
`ValueError` in the dependency's teardown, and every endpoint log line carried
`trace_id: null`.
"""

from __future__ import annotations

import json
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from voltstream.api.dependencies import TRACE_HEADER, TraceIdDep
from voltstream.logging_setup import get_logger, get_trace_id

log = get_logger("api-dependency-test")

app = FastAPI()


@app.get("/sync")
def sync_endpoint(trace_id: TraceIdDep) -> dict[str, str]:
    """Every router in the API is a sync `def`, run by FastAPI in the thread pool."""
    log.info("sync endpoint", extra={"stage": "api"})
    return {"trace_id": trace_id}


@app.get("/async")
async def async_endpoint(trace_id: TraceIdDep) -> dict[str, str]:
    log.info("async endpoint", extra={"stage": "api"})
    return {"trace_id": trace_id}


def _logged_trace_id(output: str, msg: str) -> str | None:
    for line in output.splitlines():
        if f'"msg": "{msg}"' in line:
            return json.loads(line)["trace_id"]
    raise AssertionError(f"no log line with msg {msg!r} in:\n{output}")


@pytest.mark.parametrize("path,msg", [("/sync", "sync endpoint"), ("/async", "async endpoint")])
def test_the_callers_trace_id_reaches_the_log_and_the_response(
    path: str, msg: str, capsys: pytest.CaptureFixture[str]
) -> None:
    response = TestClient(app).get(path, headers={TRACE_HEADER: "trace-from-caller"})

    assert response.status_code == 200
    assert response.headers[TRACE_HEADER] == "trace-from-caller"
    assert _logged_trace_id(capsys.readouterr().out, msg) == "trace-from-caller"


def test_a_request_without_one_gets_a_fresh_uuid_everywhere(
    capsys: pytest.CaptureFixture[str],
) -> None:
    response = TestClient(app).get("/sync")

    minted = response.headers[TRACE_HEADER]
    uuid.UUID(minted)  # raises if it is not a UUID
    assert response.json() == {"trace_id": minted}
    assert _logged_trace_id(capsys.readouterr().out, "sync endpoint") == minted


def test_the_binding_does_not_outlive_the_request() -> None:
    TestClient(app).get("/sync", headers={TRACE_HEADER: "request-scoped"})
    assert get_trace_id() is None
