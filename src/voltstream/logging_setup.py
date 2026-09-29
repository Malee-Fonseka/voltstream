"""Structured JSON logging (decision T025, §10.1).

`get_logger(service)` returns a stdlib `logging.Logger` that emits one JSON object per
line to stdout, with the fixed envelope every service agrees on:

    {"ts": ..., "level": ..., "service": ..., "stage": ..., "trace_id": ...,
     "sim_date": ..., "msg": ..., <any extra fields, merged at the top level>}

`stage` and any extra business fields (e.g. `rows_in`, `rows_out`) are passed per call
site via the stdlib `extra=` kwarg:

    logger.info("micro-batch committed", extra={"stage": "aggregate", "rows_in": 412})

`trace_id` is different: per §10.1 it must propagate end-to-end (producer -> Kafka header
-> every log line at every stage) without being threaded through every function
signature, so it is backed by a `contextvars.ContextVar` instead. Bind it once per unit
of work (one Kafka message, one API request, one micro-batch) with `bind_trace_id`, and
every log line emitted from that context picks it up automatically. An explicit
`extra={"trace_id": ...}` on a single call overrides the bound value for that line only.

`sim_date` is likewise automatic — computed from `simclock.sim_now()` at log time unless
overridden via `extra={"sim_date": ...}` — because nearly every log line wants "today" in
simulated terms and almost none want to compute it themselves.
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from voltstream.config import get_config

# The envelope fields that are never treated as ad-hoc "extras", even though some of them
# arrive via the same `extra=` mechanism as extras do.
_ENVELOPE_KEYS = frozenset({"stage", "trace_id", "sim_date"})

# Attributes stdlib `logging.LogRecord` always carries. Anything else found on a record
# is a caller-supplied extra and gets merged into the JSON line at the top level.
_STANDARD_RECORD_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "message",
    "asctime",
    "taskName",
}

_trace_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "voltstream_trace_id", default=None
)

_HANDLER_MARKER = "_voltstream_json_handler"


class _StdoutProxy:
    """Resolves `sys.stdout` at write time rather than once at handler creation.

    `get_logger` is idempotent — it reuses one handler per service name for the life of
    the process — but a plain `logging.StreamHandler(sys.stdout)` would bind to whatever
    object `sys.stdout` happened to be on the *first* call. That breaks test frameworks
    (e.g. pytest's `capsys`) that swap `sys.stdout` out per test.
    """

    def write(self, message: str) -> int:
        return sys.stdout.write(message)

    def flush(self) -> None:
        sys.stdout.flush()


class _ServiceFilter(logging.Filter):
    """Stamps every record from one logger with its fixed `service` name."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def filter(self, record: logging.LogRecord) -> bool:
        record.service = self._service
        return True


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds")
        envelope: dict[str, Any] = {
            "ts": ts.replace("+00:00", "Z"),
            "level": record.levelname,
            "service": getattr(record, "service", "unknown"),
            "stage": getattr(record, "stage", "unspecified"),
            "trace_id": getattr(record, "trace_id", None) or _trace_id_var.get(),
            "sim_date": getattr(record, "sim_date", None) or _sim_date_today(),
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _STANDARD_RECORD_ATTRS or key in _ENVELOPE_KEYS or key == "service":
                continue
            envelope[key] = value
        return json.dumps(envelope, default=str)


def _sim_date_today() -> str:
    # Imported lazily: simclock -> config, and this module already imports config, so
    # there is no cycle, but keeping the import local avoids paying for it at module
    # import time in processes that configure logging before a config file exists (tests).
    from voltstream import simclock

    return simclock.sim_date_of(simclock.sim_now()).isoformat()


def get_logger(service: str) -> logging.Logger:
    """Return the JSON-logging `Logger` for `service`. Safe to call repeatedly — the
    underlying stdlib logger and its handler are configured once per service name."""
    logger = logging.getLogger(f"voltstream.{service}")
    if not any(getattr(h, _HANDLER_MARKER, False) for h in logger.handlers):
        handler = logging.StreamHandler(stream=_StdoutProxy())
        handler.setFormatter(_JsonFormatter())
        setattr(handler, _HANDLER_MARKER, True)
        logger.addHandler(handler)
        logger.addFilter(_ServiceFilter(service))
        logger.propagate = False
    logger.setLevel(get_config().observability.log_level)
    return logger


@contextmanager
def bind_trace_id(trace_id: str) -> Iterator[None]:
    """Bind `trace_id` for every log line emitted within this context (§10.1)."""
    token = _trace_id_var.set(trace_id)
    try:
        yield
    finally:
        _trace_id_var.reset(token)


def get_trace_id() -> str | None:
    """The `trace_id` currently bound in this context, if any."""
    return _trace_id_var.get()
