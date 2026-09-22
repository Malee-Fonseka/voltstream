"""Unit tests for voltstream.logging_setup (T026)."""

from __future__ import annotations

import json

import pytest

from voltstream.logging_setup import bind_trace_id, get_logger, get_trace_id

_ENVELOPE_KEYS = {"ts", "level", "service", "stage", "trace_id", "sim_date", "msg"}


def _emit_and_capture(capsys: pytest.CaptureFixture[str], **extra: object) -> dict:
    logger = get_logger("test-service")
    logger.info("hello", extra=extra)
    out = capsys.readouterr().out.strip()
    return json.loads(out)


def test_emitted_line_is_valid_json(capsys: pytest.CaptureFixture[str]) -> None:
    line = _emit_and_capture(capsys, stage="ingest")
    assert isinstance(line, dict)


def test_envelope_has_all_required_fields(capsys: pytest.CaptureFixture[str]) -> None:
    line = _emit_and_capture(capsys, stage="ingest")
    assert _ENVELOPE_KEYS <= line.keys()
    assert line["service"] == "test-service"
    assert line["level"] == "INFO"
    assert line["stage"] == "ingest"
    assert line["msg"] == "hello"


def test_trace_id_from_context_var_appears(capsys: pytest.CaptureFixture[str]) -> None:
    with bind_trace_id("trace-abc-123"):
        assert get_trace_id() == "trace-abc-123"
        line = _emit_and_capture(capsys, stage="ingest")
    assert line["trace_id"] == "trace-abc-123"


def test_trace_id_unset_outside_context_is_null(capsys: pytest.CaptureFixture[str]) -> None:
    assert get_trace_id() is None
    line = _emit_and_capture(capsys, stage="ingest")
    assert line["trace_id"] is None


def test_per_call_trace_id_overrides_bound_one(capsys: pytest.CaptureFixture[str]) -> None:
    with bind_trace_id("bound-trace"):
        line = _emit_and_capture(capsys, stage="ingest", trace_id="override-trace")
    assert line["trace_id"] == "override-trace"


def test_extras_are_merged_flat_not_nested(capsys: pytest.CaptureFixture[str]) -> None:
    line = _emit_and_capture(capsys, stage="aggregate", rows_in=412, rows_out=5)
    assert line["rows_in"] == 412
    assert line["rows_out"] == 5
    assert "extra" not in line
    assert "extras" not in line


def test_sim_date_present_and_overridable(capsys: pytest.CaptureFixture[str]) -> None:
    line = _emit_and_capture(capsys, stage="ingest")
    assert line["sim_date"]  # auto-computed, non-empty

    overridden = _emit_and_capture(capsys, stage="ingest", sim_date="2026-08-10")
    assert overridden["sim_date"] == "2026-08-10"


def test_get_logger_is_idempotent_no_duplicate_lines(
    capsys: pytest.CaptureFixture[str],
) -> None:
    get_logger("dup-service")
    get_logger("dup-service")
    logger = get_logger("dup-service")
    logger.info("once", extra={"stage": "ingest"})
    lines = [ln for ln in capsys.readouterr().out.strip().splitlines() if ln]
    assert len(lines) == 1
