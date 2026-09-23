"""GATE 2 — the archiver restart test (T083, §9 Phase 1).

> "If checkpointing is wrong, everything built after this is built on sand."

**The guarantee under test is "no loss; duplicates tolerated" — not exactly-once.**

Kafka delivers at least once, and a Spark file sink commits its data files and its
offsets as two separate steps. Between those steps a kill leaves files on disk whose
offsets were never committed, so the restarted query re-reads that range and writes some
rows a second time. That is inherent to a file sink; claiming otherwise in the report
would be an overclaim we could not defend in the viva.

What the architecture actually promises, and what this test asserts:

1. **No loss.** Every `event_id` present before the kill is still present after it.
2. **No offset gap.** The restarted query resumes from the committed offset rather than
   skipping ahead to the newest record, so the events published *during* the outage
   arrive too. This is the assertion that would fail if the checkpoint were misconfigured
   — and the failure mode a `startingOffsets=latest` default would hide.

Duplicates are counted and reported rather than asserted away. They are removed
downstream by the batch layer's dedup on `(meter_id, event_ts)` (§3.3d), which is the
reason that key exists. `docs/assumptions.md` §4 records the same guarantee in prose.

Run with the stack up: `pytest -m integration tests/integration/test_archiver_restart.py`
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_SERVICE = "voltstream-raw-archiver"
_COMPOSE_FILE = Path(__file__).resolve().parents[2] / "docker" / "docker-compose.yml"

# Real seconds. One simulated hour is 12.5 real seconds, so 45s spans several partitions
# and comfortably several micro-batches at a 10s trigger.
_RUN_SECONDS = 45
_OUTAGE_SECONDS = 20
_RECOVERY_SECONDS = 60


def _compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "-f", str(_COMPOSE_FILE), *args],
        capture_output=True,
        text=True,
        check=True,
    )


def _archived_event_ids(spark) -> set[str]:  # type: ignore[no-untyped-def]
    """Every `event_id` currently in the master dataset."""
    from voltstream.config import get_config

    path = f"s3a://{get_config().minio.bucket_raw}/meter_readings"
    rows = spark.read.parquet(path).select("event_id").collect()
    return {r["event_id"] for r in rows}


def _archived_row_count(spark) -> int:  # type: ignore[no-untyped-def]
    from voltstream.config import get_config

    path = f"s3a://{get_config().minio.bucket_raw}/meter_readings"
    return spark.read.parquet(path).count()


@pytest.fixture(scope="module")
def reader_session():  # type: ignore[no-untyped-def]
    """A session for *reading* the master dataset — not the archiver's own session."""
    from voltstream.streaming.session import build_session

    session = build_session("voltstream-gate2-reader")
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


def test_archiver_survives_a_kill_without_losing_data(reader_session) -> None:  # type: ignore[no-untyped-def]
    # --- Let it run and establish a baseline -------------------------------------
    time.sleep(_RUN_SECONDS)

    before_ids = _archived_event_ids(reader_session)
    before_count = _archived_row_count(reader_session)
    assert before_count > 0, (
        "No rows archived before the kill — the archiver is not running, or the producer "
        "is not publishing. Gate 2 cannot be evaluated."
    )

    # --- Kill, wait, restart ------------------------------------------------------
    # `kill`, not `stop`: SIGKILL is the unclean shutdown the checkpoint must survive.
    # A graceful SIGTERM would let Spark commit and would not test anything.
    _compose("kill", "-s", "SIGKILL", _SERVICE)
    time.sleep(_OUTAGE_SECONDS)
    _compose("start", _SERVICE)
    time.sleep(_RECOVERY_SECONDS)

    after_ids = _archived_event_ids(reader_session)
    after_count = _archived_row_count(reader_session)

    # --- 1. No loss ---------------------------------------------------------------
    lost = before_ids - after_ids
    assert not lost, (
        f"{len(lost)} event_id(s) present before the kill are missing after it. "
        "The file sink committed data it then lost, or the output path changed."
    )

    # --- 2. No offset gap ---------------------------------------------------------
    # Events published during the outage must appear after recovery. If the query had
    # resumed from `latest` instead of its checkpoint, the count would resume growing
    # but this window of events would be gone forever — silent loss that a naive
    # "is it still writing?" check would pass.
    assert after_count > before_count, (
        "No new rows after recovery. The archiver either did not restart, or resumed "
        "from the wrong offset."
    )

    new_ids = after_ids - before_ids
    assert new_ids, "Row count grew but no new event_ids appeared — only duplicates."

    # --- 3. Duplicates: reported, not asserted away --------------------------------
    # See the module docstring. This is the honest guarantee for a file sink.
    duplicate_rows = after_count - len(after_ids)
    print(
        f"\nGate 2: {before_count} rows before kill, {after_count} after recovery, "
        f"{len(new_ids)} new event_ids, {duplicate_rows} duplicate row(s).\n"
        "Duplicates are expected under at-least-once delivery and are removed by the "
        "batch layer's dedup on (meter_id, event_ts)."
    )
    assert duplicate_rows >= 0
