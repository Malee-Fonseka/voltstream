"""The archiver restart test.

**The guarantee under test is "no loss; duplicates tolerated" — not exactly-once.**

Kafka delivers at least once, and a Spark file sink commits its data files and its
offsets as two separate steps. Between those steps a kill leaves files on disk whose
offsets were never committed, so the restarted query re-reads that range and writes some
rows a second time. That is inherent to a file sink; claiming otherwise in the report
would be an overclaim we could not defend.

What is asserted:

1. **Progress.** New event_ids appear after recovery, so the query actually resumed.
2. **No offset gap.** For every Kafka partition, the archived offsets form a contiguous
   range with no hole. This is the assertion that fails if the checkpoint is wrong: a
   query resuming from `latest` instead of its committed offset would keep writing and
   keep looking healthy, while silently skipping everything published during the outage.
   Contiguity is what makes that failure visible.

Duplicates are counted and reported, never asserted away. The batch layer removes them
by deduplicating on (meter_id, event_ts), which is why that key exists.

Spark runs **inside the Spark container**, not on the host: hadoop-aws and the Kafka
connector are baked into that image only. Docker itself is driven from the host, because
the test has to kill and restart the container it is measuring.

Run with the stack up:
    pytest -m integration tests/integration/test_archiver_restart.py -s
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_SERVICE = "raw-archiver"
_COMPOSE = ["docker", "compose", "-f", str(Path(__file__).resolve().parents[2] / "docker" / "docker-compose.yml")]

# Real seconds. A simulated hour is 12.5 real seconds, so these spans cover several
# partitions and several micro-batches at a 10-real-second trigger.
_SETTLE_SECONDS = 45
_OUTAGE_SECONDS = 20
_RECOVERY_SECONDS = 75

_MARKER = "__GATE2__"

# Runs inside the Spark container. Prints one marked JSON line so the host can parse it
# out of Spark's very chatty stdout.
_PROBE = f"""
import json
from voltstream.streaming.session import build_session
from voltstream.config import get_config
from pyspark.sql import functions as F

spark = build_session("gate2-probe")
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet(f"s3a://{{get_config().minio.bucket_raw}}/meter_readings")

per_partition = {{}}
for row in df.groupBy("kafka_partition").agg(
    F.min("kafka_offset").alias("lo"),
    F.max("kafka_offset").alias("hi"),
    F.countDistinct("kafka_offset").alias("distinct_offsets"),
).collect():
    per_partition[str(row["kafka_partition"])] = {{
        "lo": row["lo"], "hi": row["hi"], "distinct_offsets": row["distinct_offsets"],
    }}

print("{_MARKER}" + json.dumps({{
    "rows": df.count(),
    "distinct_event_ids": df.select("event_id").distinct().count(),
    "per_partition": per_partition,
}}))
spark.stop()
"""


def _probe() -> dict:
    """Read the master dataset from inside the Spark container."""
    result = subprocess.run(
        [*_COMPOSE, "run", "--rm", "--no-deps", "-T", _SERVICE, "python", "-c", _PROBE],
        capture_output=True,
        text=True,
        check=True,
    )
    for line in result.stdout.splitlines():
        if line.startswith(_MARKER):
            return json.loads(line[len(_MARKER) :])
    raise AssertionError(f"probe produced no result line.\nstdout:\n{result.stdout[-2000:]}")


def test_archiver_survives_a_kill_without_losing_data() -> None:
    time.sleep(_SETTLE_SECONDS)

    before = _probe()
    assert before["rows"] > 0, (
        "Nothing archived before the kill — the archiver is not running or the producer "
        "is not publishing. The restart guarantee cannot be evaluated."
    )

    # SIGKILL, not a graceful stop: an orderly shutdown lets Spark commit and would not
    # exercise the checkpoint at all.
    subprocess.run([*_COMPOSE, "kill", "-s", "SIGKILL", _SERVICE], check=True, capture_output=True)
    time.sleep(_OUTAGE_SECONDS)
    subprocess.run([*_COMPOSE, "start", _SERVICE], check=True, capture_output=True)
    time.sleep(_RECOVERY_SECONDS)

    after = _probe()

    # 1. Progress — the query resumed and is writing again.
    assert after["distinct_event_ids"] > before["distinct_event_ids"], (
        f"No new event_ids after recovery "
        f"({before['distinct_event_ids']} -> {after['distinct_event_ids']}). "
        "The archiver did not restart, or resumed past the data it should have read."
    )

    # 2. No offset gap — the real checkpoint assertion.
    gaps = []
    for partition, stats in sorted(after["per_partition"].items()):
        expected = stats["hi"] - stats["lo"] + 1
        if stats["distinct_offsets"] != expected:
            gaps.append(
                f"partition {partition}: offsets {stats['lo']}..{stats['hi']} should hold "
                f"{expected} distinct offsets, found {stats['distinct_offsets']} "
                f"({expected - stats['distinct_offsets']} missing)"
            )
    assert not gaps, "Offset gap after restart — data published during the outage was skipped:\n" + "\n".join(gaps)

    # 3. Duplicates: reported, not asserted away. See the module docstring.
    duplicates = after["rows"] - after["distinct_event_ids"]
    print(
        f"\n  rows before kill      : {before['rows']}"
        f"\n  rows after recovery   : {after['rows']}"
        f"\n  distinct event_ids    : {after['distinct_event_ids']}"
        f"\n  duplicate rows        : {duplicates}"
        f"\n  partitions contiguous : {len(after['per_partition'])}/{len(after['per_partition'])}"
        "\n  Duplicates are expected under at-least-once delivery and are removed by the"
        "\n  batch layer's dedup on (meter_id, event_ts)."
    )
