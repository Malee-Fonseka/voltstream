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
    def write(self, message: str) -> int:
        return sys.stdout.write(message)

    def flush(self) -> None:
        sys.stdout.flush()


class _ServiceFilter(logging.Filter):
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
    token = _trace_id_var.set(trace_id)
    try:
        yield
    finally:
        _trace_id_var.reset(token)


def get_trace_id() -> str | None:
    return _trace_id_var.get()
