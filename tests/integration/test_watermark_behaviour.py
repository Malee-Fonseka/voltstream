"""Does the watermark actually drop stragglers? (T094)

The watermark decision is only worth anything if its effect is observable, so this
measures the kWh the speed layer misses against what the archiver actually received.

Two scenarios, from D3's two deliberately separated lateness mechanisms:

1. **Default faults.** Meters drop out, buffer, and flush a backlog far beyond the
   watermark, so the speed layer misses some of the day's energy. The gap should sit
   between 0.25 % and 5 % (the sizing model puts it near 1.7 %). That gap *is* the
   speed-versus-batch divergence the whole Lambda demonstration rests on — if it were
   zero there would be nothing for the merge function to correct.

2. **Dropouts disabled, reordering still on.** Out-of-order events are shifted back by at
   most the watermark. At the daily grain none of them is dropped: a reordered record is
   sent with its own tick, and the watermark trails that tick by at least 30 simulated
   minutes (R02), so the daily gap must be zero, give or take a straggler. This is what
   makes the attribution honest: "the daily kWh gap is dropped backfill, nothing else" becomes
   something shown rather than hoped. The 15-minute windows are a different matter — D3's
   correction measured reordering being dropped there — so their figure is printed, not
   asserted.

**What is compared, and why it is not simply "Parquet total vs Postgres total".** The
archiver keeps everything: invalid records, duplicates, late arrivals. The speed layer
sums only records that pass validation, counts each reading once on the batch layer's key
(`core.keys.DEDUP_COLUMNS`, R02), and misses whatever the watermark dropped. So the
Parquet side gets the same validation and the same dedup before the comparison, leaving
exactly one difference between the two totals: lateness. Comparing raw totals instead
would fold in rejected records and duplicates and measure the wrong thing.

Spark runs inside the Spark container; the host only drives Docker and reads the result.

    pytest -m integration tests/integration/test_watermark_behaviour.py -s
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_REPO = Path(__file__).resolve().parents[2]
_COMPOSE = [
    "docker",
    "compose",
    "--env-file",
    str(_REPO / ".env"),
    "-f",
    str(_REPO / "docker" / "docker-compose.yml"),
]

_MARKER = "__T094__"

# One simulated day is 5 real minutes. Let two elapse so there is a complete, closed day
# to measure, well clear of the day currently being written.
_RUN_SECONDS = 700

_PROBE = f"""
import json
from datetime import timedelta
from voltstream.streaming.session import build_session
from voltstream.streaming.sinks import pg_connection_string
from voltstream.streaming.sources import split_valid_invalid
from voltstream.config import get_config
from voltstream.core.keys import DEDUP_COLUMNS
from pyspark.sql import functions as F
import psycopg

config = get_config()
spark = build_session("t094-probe")
spark.sparkContext.setLogLevel("ERROR")

raw = spark.read.parquet(f"s3a://{{config.minio.bucket_raw}}/meter_readings")

# The same validation the speed layer applies, so the only remaining difference between
# the two totals is lateness.
known = frozenset(f"HH-{{i:04d}}" for i in range(1, config.simulation.households + 1))
bounds = (
    config.simulation.epoch_sim - timedelta(days=365),
    config.simulation.epoch_sim + timedelta(days=365 * 50),
)
valid, _ = split_valid_invalid(raw, known_household_ids=known, event_ts_bounds=bounds)
# ... and the same dedup (R02): each physical reading counted once, as both layers do.
valid = valid.dropDuplicates(list(DEDUP_COLUMNS))

# Measure the newest day that is definitely closed, not the one still being written.
days = sorted(r["sim_date"] for r in raw.select("sim_date").distinct().collect())
target = days[-2] if len(days) >= 2 else days[-1]

archived = valid.filter(F.col("sim_date") == F.lit(target)).agg(
    F.sum("consumption_kwh").alias("kwh")
).collect()[0]["kwh"]

with psycopg.connect(pg_connection_string()) as conn, conn.cursor() as cur:
    cur.execute(
        "SELECT COALESCE(SUM(consumption_kwh), 0), COUNT(*) "
        "FROM household_running_rt WHERE sim_date = %s",
        (target,),
    )
    daily_kwh, households = cur.fetchone()
    # The 15-minute windows, summed over the same simulated day. This is the view the
    # watermark can actually evict, so it is where dropped backfill shows up.
    cur.execute(
        "SELECT COALESCE(SUM(total_consumption_kwh), 0), COUNT(*) "
        "FROM zone_metrics_rt WHERE window_start >= %s AND window_start < %s::date + 1",
        (target, target),
    )
    window_kwh, windows = cur.fetchone()

archived_f = float(archived or 0)
daily_f = float(daily_kwh or 0)
window_f = float(window_kwh or 0)
pct = lambda v: 0.0 if archived_f == 0 else (archived_f - v) / archived_f * 100.0

print("{_MARKER}" + json.dumps({{
    "sim_date": str(target),
    "archived_kwh": round(archived_f, 4),
    "daily_window_kwh": round(daily_f, 4),
    "daily_gap_pct": round(pct(daily_f), 4),
    "fifteen_min_window_kwh": round(window_f, 4),
    "fifteen_min_gap_pct": round(pct(window_f), 4),
    "households": households,
    "windows": windows,
    "days_seen": len(days),
}}))
spark.stop()
"""


def _compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([*_COMPOSE, *args], capture_output=True, text=True, check=True)


def _probe() -> dict:
    result = subprocess.run(
        [*_COMPOSE, "run", "--rm", "--no-deps", "-T", "speed-layer", "python", "-c", _PROBE],
        capture_output=True,
        text=True,
        check=True,
    )
    for line in result.stdout.splitlines():
        if line.startswith(_MARKER):
            return json.loads(line[len(_MARKER) :])
    raise AssertionError(f"probe produced no result line.\nstdout:\n{result.stdout[-2000:]}")


def _clean_run(dropout_probability: str) -> dict:
    """Wipe, restart with the given dropout rate, let a day elapse, and measure.

    A wipe per scenario is not optional: the gap is a property of one run's data, and
    leaving the previous scenario's dropped readings in Parquet would carry its answer
    into this one.
    """
    _compose("down")
    subprocess.run(
        [
            "docker",
            "volume",
            "rm",
            "voltstream_minio_data",
            "voltstream_kafka_data",
            "voltstream_archiver_checkpoints",
            "voltstream_speed_checkpoints",
            "voltstream_postgres_data",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    env_file = _REPO / ".env"
    original = env_file.read_text(encoding="utf-8")
    try:
        anchor = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        lines = [
            line
            for line in original.splitlines()
            if not line.startswith("VOLTSTREAM_ANCHOR_REAL=")
            and not line.startswith("VOLTSTREAM__FAULTS__DROPOUT_PROBABILITY_PER_METER_TICK=")
        ]
        lines.append(f"VOLTSTREAM_ANCHOR_REAL={anchor}")
        lines.append(
            f"VOLTSTREAM__FAULTS__DROPOUT_PROBABILITY_PER_METER_TICK={dropout_probability}"
        )
        env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

        _compose("up", "-d")
        time.sleep(_RUN_SECONDS)

        # Stop everything that writes before measuring. Two reasons, and the second is
        # the one that bit us: the data is at rest by now, so nothing is lost by
        # stopping; and leaving three Spark drivers running while a fourth probes them
        # saturates the host. On the first run a probe container waited fifteen minutes
        # for CPU, and a starved driver processes a micro-batch spanning far more event
        # time than one trigger, which advances the watermark in leaps and drops late
        # data that a healthy run would have absorbed. That turns the measurement into a
        # reading of machine load rather than of the watermark.
        _compose("stop", "meter-producer", "raw-archiver", "speed-layer")
        return _probe()
    finally:
        env_file.write_text(original, encoding="utf-8")


@pytest.fixture(scope="module")
def with_dropouts() -> dict:
    return _clean_run("0.002")


@pytest.fixture(scope="module")
def without_dropouts() -> dict:
    return _clean_run("0.0")


def test_watermark_drops_some_of_the_dropout_backlog(with_dropouts: dict) -> None:
    """The 15-minute windows should miss a small but non-zero slice of the day's energy.

    Measured on the zone windows, not the daily household totals, for the reason set out
    in the module docstring. The band is D3's: a dropout flush is 10-144 simulated
    minutes late, and everything past ~103 is certainly gone, so a meaningful fraction of
    each backlog never reaches the 15-minute view.
    """
    result = with_dropouts
    print(f"\nwith dropouts: {json.dumps(result, indent=2)}")

    assert result["windows"] > 0, "no zone windows for the measured day"
    assert 0.25 <= result["fifteen_min_gap_pct"] <= 5.0, (
        f"15-minute windows are {result['fifteen_min_gap_pct']:.2f}% below the archived "
        f"total for {result['sim_date']}; D3's model expects 0.25-5%. Below that the "
        "watermark is dropping nothing and the merge function has nothing to correct; "
        "above it, more is going missing than the dropout model accounts for."
    )


def test_daily_totals_miss_backfill_past_the_watermark(with_dropouts: dict) -> None:
    """T094 as written: the provisional daily totals fall 0.25-5 % short of the archive.

    Since R02 both speed-layer queries read a deduplicated stream, and Spark's streaming
    dedup drops records older than the watermark. A dropout flushes its backlog 10-144
    simulated minutes late, so the part of it older than the watermark never reaches the
    daily totals, while the batch layer, rescanning the closed day, bills all of it. That
    gap is the `data_effect` D4 attributes. D3's model puts it near 1.7 %.

    Before R02 this asserted the opposite (no daily gap). That held only because the 1-day
    window absorbed lateness nothing else in the speed layer was dropping.
    """
    result = with_dropouts
    print(f"\nwith dropouts (daily): {json.dumps(result, indent=2)}")
    assert result["households"] > 0, "no speed-layer rows for the measured day"
    assert 0.25 <= result["daily_gap_pct"] <= 5.0, (
        f"daily household totals are {result['daily_gap_pct']:.4f}% short of the "
        "deduplicated archive; T094 expects 0.25-5 %. Below that the watermark is dropping "
        "no backfill; above it, more is missing than the dropout model accounts for."
    )


def test_reordering_alone_never_reaches_the_daily_gap(without_dropouts: dict) -> None:
    """With dropouts off, the daily totals must match the archive to within a few readings.

    The control that makes the attribution honest: "the daily kWh gap is dropped backfill,
    nothing else" is shown here rather than assumed. Reordering shifts a record by less
    than 30 simulated minutes, and the producer sends it with its own tick. A micro-batch's
    watermark is the newest tick of the batch before it minus 30 minutes, so it trails the
    record's tick by at least the watermark, and neither the dedup nor the daily window can
    drop it. That assumes delivery keeps pace with the 2-real-second tick: on a host too
    loaded for that, a failure here is a reading of machine load (see `_clean_run`).

    The 15-minute windows are printed but not asserted. D3's correction measured about 1 %
    missing there even without dropouts: a window can close within a trigger of its end,
    before a reordered record for it arrives. That is the operational view's documented
    trade, not an attribution error.
    """
    result = without_dropouts
    print(f"\nwithout dropouts: {json.dumps(result, indent=2)}")

    assert result["households"] > 0, "no speed-layer rows for the measured day"
    # Not exactly 0.0. One reading is about 0.013 % of a day's energy (~508 kWh over
    # 7,500 readings), and the last idle run before R02 measured 0.0162 % here: a single
    # straggler, cause not pinned down. 0.05 % allows a few such readings and is still a
    # fifth of the smallest gap the dropouts-on test accepts.
    assert abs(result["daily_gap_pct"]) < 0.05, (
        f"daily gap is {result['daily_gap_pct']:.4f}% with dropouts disabled; it should be "
        "zero. Reordering is bounded by the watermark, so if energy is still going missing "
        "from the daily totals, something other than backfill is being dropped."
    )
