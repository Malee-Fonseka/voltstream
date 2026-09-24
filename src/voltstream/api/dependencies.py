"""FastAPI dependency providers (T104).

The one with substance is `trace_id`. A `trace_id` is generated at the meter producer and
carried through Kafka headers, the master dataset and every log line (§10.3). A request
that arrives without one still needs an identifier, or its log lines are the only part of
the pipeline that cannot be correlated — which is precisely when you want to correlate
them, because someone is investigating a bad response.

So: take the caller's `X-Trace-Id` when there is one, mint a UUID when there is not, bind
it for the duration of the request so every log line carries it, and echo it in the
response header so the caller can quote it back.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Annotated

from fastapi import Depends, Header, Request, Response

from voltstream.config import VoltstreamConfig, get_config
from voltstream.logging_setup import bind_trace_id

TRACE_HEADER = "X-Trace-Id"


def config_provider() -> VoltstreamConfig:
    return get_config()


def trace_id_provider(
    request: Request,
    response: Response,
    x_trace_id: Annotated[str | None, Header(alias=TRACE_HEADER)] = None,
) -> Iterator[str]:
    """Bind a trace id for the request and echo it back.

    Generated rather than optional: an unidentified request is one that cannot be
    followed through the logs later.
    """
    trace_id = x_trace_id or str(uuid.uuid4())
    response.headers[TRACE_HEADER] = trace_id
    request.state.trace_id = trace_id
    with bind_trace_id(trace_id):
        yield trace_id


ConfigDep = Annotated[VoltstreamConfig, Depends(config_provider)]
TraceIdDep = Annotated[str, Depends(trace_id_provider)]
