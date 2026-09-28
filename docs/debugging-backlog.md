# Debugging backlog — review of Phases 0–10

**Reviewed:** 2026-09-26, commit `0e58e3a` on `dev`, against Phases 0–10 of
[`Implementation_Tasks.md`](Implementation_Tasks.md).
**Purpose:** the work list for a debugging session once the remaining phases are
implemented. Fixed items are ticked, with the date and what changed.

Line numbers are as of `0e58e3a` and will drift. Every item also names the function or
section, so search for that if a line reference no longer lands.

Severity:

- **Blocker**: produces wrong numbers or breaks a demo/gate.
- **High**: a task's *Done when* is not met.
- **Medium**: a correctness or robustness gap.
- **Low**: hygiene.

---

## How the review was done

| Check | Result |
|---|---|
| `python -m pytest -m "not integration"` | **135 passed**, 19 deselected (incl. the 5,000-row consistency test and 1,000-example property tests) |
| `ruff check src tests` | clean |
| `ruff format --check src tests` | **fails**: 12 files (R10) |
| `mypy src` | **fails**: 1 error (R11) |
| bare `pytest --collect-only tests/unit/test_contracts.py` | **fails**: import error (R12) |
| `pre-commit run no-volstream-misspelling --all-files` | **fails** (R13) |
| Integration tests / live stack | **not run**: Docker was not running (see "Verify with the stack up") |

---

## Heads-up while implementing Phases 11–14

The remaining phases can be built before this session, but these issues will make their
outputs look wrong in predictable ways. Don't debug the symptom in the new code:

| You will see | Caused by |
|---|---|
| `reconciliation_daily.tariff_effect = 0.00` on every row | R01 |
| `data_effect` ≈ 2 % of energy charge on every row, the reverse of D3's prediction | R02 |
| `billing__<first day − 1>` failed on every cold start; a watchdog built per D6 fires forever | R04 |
| No report file after a DAG run | R08 |
| API log lines with `trace_id: null`; a `ValueError` traceback per request in `api` logs | R05 |
| `/bill/delta` effects summing to `−delta` | R09 |
| `LowRenewableContribution` cannot be forced via cloud cover (T160) | R16 |

R06 (lag), R07 (batch duration) and R21 (latency) were fixed on 2026-09-26, before Phase 12,
because Phase 12's dashboards and alerts read those metrics directly. Batch pushes took
effect in Phase 12 (2026-09-27): the DAG now points the three batch jobs at the Pushgateway.

R01, R02, R04, R05 and R08 were fixed on 2026-09-27, so the rows above for them no longer
apply. After R02 the expected picture is D4's revised one: `tariff_effect` non-zero on every
row with a sign that alternates by day, and `data_effect` carrying the dropped backfill.

---

## Summary

| ID | Sev | Area | Issue | Tasks |
|---|---|---|---|---|
| R01 | Blocker | simulators | Day-over-day tariff change is in a block nobody reaches → stale-tariff divergence is always 0 | T074, D2, T137/T140 |
| R02 | Blocker | speed layer | No dedup in the speed layer; batch dedups → speed ~2 % high; docs claim the opposite | T088–T091, T094, D3 |
| R03 | Blocker | simulators / DAG | Tariff for day D written at the *start* of D; watcher workaround makes the D3 grace a no-op | T074, T121, D3 |
| R04 | Blocker | DAG / batch | Day-zero seed file triggers a billing run that always fails | T075, T121, D6 |
| R05 | Blocker | API | Trace-id dependency: `trace_id` null in logs and `ValueError` on every request | T104 |
| R06 | Blocker | observability | `voltstream_consumer_lag` hard-coded to 0 | T081, T092, T096 |
| R07 | High | observability | Batch duration recorded in a container that exits; never collected | T116 |
| R08 | High | DAG | Daily report file written into an auto-removed container; lost | T131 |
| R09 | High | API | `/bill/delta` sign and percentage base contradict D4/D5 | T129 |
| R10 | High | tooling | `ruff format --check` fails (12 files) | T016, T110 |
| R11 | High | tooling | `mypy src` fails: `psycopg_pool` missing from `.venv` | T016 |
| R12 | High | tooling | Bare `pytest` cannot import `scripts` (CI command in T169 will fail) | T033, T169 |
| R13 | High | tooling | Pre-commit D7 hook fails on a compose-file comment | T017, D7 |
| R14 | Medium | config | D2's load-time validation of `tariff.blocks` missing | T021, D2 |
| R15 | Medium | config | Five modules read `os.environ` directly; session not built from config | T021, T077 |
| R16 | Medium | simulators | Producer ignores cloud cover → weather file has no effect | T066, T160 |
| R17 | Medium | observability | Injected-fault counts only in logs, not metrics | T073 |
| R18 | Medium | archiver | Master dataset stores parsed columns only, not the raw Kafka value | T080 |
| R19 | Medium | docs / evidence | Gate 2 "334 duplicates" mostly injected duplicates, not restart duplicates | T083 |
| R20 | Medium | speed layer | T096 not implemented; T095 deviation not in decision log | T095, T096 |
| R21 | Medium | observability | E2E latency measured at the validation query, whole-second resolution | T081, T092, T168 |
| R22 | Medium | batch | Tariff CSV not validated against `TariffRecord`; no weather join; `subsidy_flag` parse too narrow | T113 |
| R23 | Medium | batch / sinks | Batch rejects go to DLQ and are re-inserted on every retry/restatement | T087, T112, T130 |
| R24 | Medium | batch | Zone rollup: no `pipeline_runs` row; per-reading netting ≠ bill netting | T124 |
| R25 | Medium | batch / design | Restatement overwrites bills and archived tariff; audit claim only half true | T115, T117, T123, D6 |
| R26 | Medium | design | Speed and batch duplicate helpers outside `core/` (and parse tariffs differently) | §4.4 |
| R27 | Medium | tests | Merge-function and effective-dated tests re-implement the code they test | T118, T128 |
| R28 | Low | simulators | Dropper still builds its own S3 keys and client | T099 |
| R29 | Low | tests | Missing tests for T029/T031/T125 acceptance; doctest not collected | T029, T031, T058, T125 |
| R30 | Low | docs | Design-doc and comment drift | T037, T038, T040, T126, T182 |
| R31 | Low | storage | `DatabaseUnavailable` swallows query bugs; readiness can hang 30 s | T098, T105 |
| R32 | Low | storage | `DISTINCT ON` query has no supporting index; `zone_metrics_rt` grows unbounded | T041, T100 |
| R33 | Low | storage | Some repository functions return raw tuples | T101 |
| R34 | Low | tests | Small inconsistencies in property test / T062 grep | T060, T062 |
| R35 | Low | batch | A household with no valid readings gets no bill → row-count check fails | T113, T122 |
| R36 | Medium | DAG / compose | DAG tasks always get the default Postgres and MinIO passwords | T121, D6 |
| R37 | Low | simulators | A stalled producer tick is never made up: 138 readings per meter-day, not 150 | T072 |
| R38 | High | batch / DAG | A transient DNS or connection failure fails a batch job outright; the rollup never retries, and a killed job leaves a `running` row forever | T113, T124, T122 |

---

## Blockers

### R01 — Stale-tariff divergence is always zero
- [x] Fixed 2026-09-27 (minimum fix): the dropper now steps `block_1_rate` by +0.50 on even
  date ordinals, and `block_2_rate` is constant. `tests/unit/test_reference_dropper.py` runs
  one simulated day through the real profiles and checks that yesterday's and today's
  tariffs price all 50 households differently; it fails on the old rule. D2 records the change.
  - **Still open, a design decision:** the block boundaries are unchanged, so no bill
    reaches block 2 and the tier breakdown is still never exercised (the "Better" fix below).
  - **Caveat for T162:** the backfill demo's example corruption (`block_2_rate`
    `16.50 → 61.50`, in D2 and `Implementation_Tasks.md`) changes no bill. Corrupt
    `block_1_rate` or `fixed_charge`. D2 now says so; the task text does not.
  - **Verified live** (Gate 4 run, 2026-09-27): `tariff_effect` non-zero for all 50
    households on both days, positive on 2026-01-01 and negative on 2026-01-02.

**Where:** [`simulators/reference_dropper.py:75-77`](../src/voltstream/simulators/reference_dropper.py#L75-L77) `_block_2_rate_for`

**What's wrong:** the only day-over-day tariff change is `block_2_rate` (+0.50 on
alternate days). Block 2 starts at 60 kWh, and no simulated household ever uses that
much in a day. So:

- `S` (yesterday's tariff) equals `C` (today's tariff) for every household.
- `tariff_effect = S − C = 0.00` everywhere.
- The whole §3.1 stale-tariff story shows nothing.
- Live bills also never cross a block boundary, so the tier breakdown is never exercised
  in the demo.

**Evidence:** one simulated day through the real profile functions, all 50 households:

```
daily consumption kWh  min/mean/max: 11.6368 16.2943 21.8371
billable import kWh    max: 21.8371
households with billable import > 60 kWh (enter block_2): 0
```

Reproduce:

```bash
.venv/Scripts/python.exe - <<'EOF'
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from voltstream.simulators.profiles import consumption_kwh, solar_kwh
from voltstream.simulators.households import household_roster
from voltstream.core.netting import net
roster = household_roster(50, ["ZONE-A","ZONE-B","ZONE-C","ZONE-D","ZONE-E"])
start, tick = datetime(2026,1,5,tzinfo=UTC), timedelta(minutes=9.6)
imp = {}
for h in roster:
    c = sum((consumption_kwh(h.household_id, start+i*tick) for i in range(150)), Decimal(0))
    s = sum((solar_kwh(h.household_id, start+i*tick, has_solar=h.has_solar) for i in range(150)), Decimal(0))
    imp[h.household_id] = net(c, s).billable_import_kwh
print("max billable import:", max(imp.values()), "| above 60:", sum(v > 60 for v in imp.values()))
EOF
```

**Fix:**

- **Minimum:** step a rate every household pays, i.e. `block_1_rate`, and update the rule
  in the module docstring.
- **Better:** also rescale `tariff.blocks` in `config/base.yaml` to daily magnitudes
  (e.g. 10 / 20 kWh), so households fall into all three blocks and the breakdown means
  something.
- If the boundaries change, update the T172 fixture expectations. The unit, property and
  consistency tests pass their own boundaries, so they are unaffected.

**Done when:** over one simulated day, `tariff_effect ≠ 0` for every household. If the
boundaries are rescaled, at least some bills should have non-zero kWh in block 2.

---

### R02 — Speed layer counts duplicates; batch removes them
- [x] Fixed 2026-09-27 (option 1): `speed_layer.deduplicated()` sets the watermark and
  drops duplicates on `core.keys.DEDUP_COLUMNS`, the key the batch layer's window dedup now
  uses too. Both aggregations read its output.
  - `dropDuplicates` rather than `dropDuplicatesWithinWatermark`: the key contains the
    watermarked `event_ts`, which is the documented pattern for plain `dropDuplicates`, and
    each key's state is evicted once the watermark passes it. Both variants drop records
    older than the watermark.
  - Consequence, recorded in D3, D4 and `assumptions.md` §2: backfill older than the
    watermark is now missing from the **daily** totals too, so `data_effect` carries D3's
    ~1.7 % as originally designed. Reordering is still not dropped at the daily grain.
  - T094's probe dedups the archive side. Its daily test asserts the 0.25–5 % band; the
    control (dropouts off) asserts under 0.05 %.
  - **Measured live** (Gate 4 run, 2026-09-27): daily gap 0.523 % on 2026-01-02, inside the
    band and below the modelled 1.7 % (D4 "Measured attribution" explains why). Duplicates:
    the batch layer removed 127 and the speed layer counted none.
  - **The probe has not been re-run.** Run `pytest -m integration tests/integration/test_watermark_behaviour.py`
    for the control (reordering alone) and record the figures.
  - **Run `clean` before the next start.** The zone query gained a dedup operator, so its
    old checkpoint cannot be restored.

**Where:**

- [`streaming/speed_layer.py:413-419`](../src/voltstream/streaming/speed_layer.py#L413-L419) `start()` → `valid_stream()`
- Baseline in [`tests/integration/test_watermark_behaviour.py:21-27`](../tests/integration/test_watermark_behaviour.py#L21-L27)

**What's wrong:**

- `faults.duplicate_rate: 0.02` re-sends readings with the same `event_id`.
- The speed layer sums them. Both `zone_metrics_rt` and `household_running_rt` run about
  2 % high.
- The batch layer deduplicates on `(meter_id, event_ts)`.
- T094's probe compares the speed layer against an archived total that is validated but
  **not deduplicated**. That is why the daily gap measured 0.00 %.

The measurement is internally consistent, but it doesn't describe speed vs batch, and two
documented claims are wrong as a result:

- [`docs/assumptions.md`](assumptions.md) §2, "Two consequences worth stating plainly":
  "the speed-versus-batch divergence on bills comes from the stale tariff, not from lost
  readings".
- [`05-open-decisions.md`](architecture/05-open-decisions.md) D3 correction: the
  prediction that `data_effect` is at or near zero.

With R01 and R02 as they are, reconciliation shows ~100 % `data_effect` (duplicates) and
0 `tariff_effect`. That is the reverse of the documented story.

**Fix (decide one):**

1. **Recommended:** deduplicate in the speed layer. In `valid_stream()`, call
   `.dropDuplicatesWithinWatermark(["meter_id", "event_ts"])` (Spark 3.5) after
   `withWatermark`. Both aggregations then see deduplicated input. The watermark must
   be applied before the dedup, so restructure `zone_aggregation` /
   `household_aggregation` to take an already-watermarked frame.
2. Keep the duplicates and document them explicitly as the data effect ("the speed layer
   does not dedup; at-least-once duplicates are a real data effect").

Either way, correct the D3 correction text and `assumptions.md` §2. Add dedup to the
T094 probe's baseline if option 1 is chosen.

**Done when:** speed-layer daily household totals match the **deduplicated** archived
total for a closed day, or the docs state the duplicate effect with a measured number.

---

### R03 — Tariff written at the start of the day; D3 grace never waits
- [ ] Fixed

**Where:**

- [`simulators/reference_dropper.py:181-186`](../src/voltstream/simulators/reference_dropper.py#L181-L186) `main()`: drops `tariff_<current sim date>` as soon as that day begins.
- [`airflow/dags/tariff_watcher_dag.py:47-57`](../airflow/dags/tariff_watcher_dag.py#L47-L57) `list_pending_days()`: compensates by excluding the newest file.
- [`airflow/dags/daily_billing_dag.py:55-79`](../airflow/dags/daily_billing_dag.py#L55-L79) `wait_late_data_grace()`: waits on `tariff_<sim_date>`'s `LastModified`.

**What's wrong:** §8.3 and D3 have `tariff_D` land when day D **closes**, and its arrival
triggers billing for D. The dropper instead writes `tariff_D` when D **starts**.

The watcher patched the trigger timing: it bills D only once `tariff_{D+1}` exists. But
the grace task still reads `tariff_D`'s `LastModified`, which by then is ~5 real minutes
old. So `remaining < 0` and the grace always sleeps 0 s.

Consequences:

- Readings from a dropout that straddles midnight (flushed up to 30 real s after
  midnight, plus up to 10 s archiver trigger) can miss the rescan, depending on where the
  watcher's 1-minute cycle falls.
- Today's tariff exists all day. That undercuts §2.4 ("depends on data that does not
  exist until the day ends") and makes "yesterday's tariff" look like a choice rather
  than a necessity.
- Two files now contradict each other: the watcher comment says files land at day
  start; the grace docstring assumes day end.

**Fix:**

- Change the dropper to write `tariff_D` when the simulated date rolls from D to D+1,
  i.e. write `last_seen_date` on rollover.
- Keep the T075 day-zero seed of `tariff_{D0−1}`.
- Remove the `days[:-1]` exclusion in the watcher so billing triggers on arrival again.
- The speed layer still finds `tariff_{D−1}` during day D.
- Update the watcher and grace docstrings.

**Done when:** a billing task log shows `sleeping ~90s` for a freshly closed day, and
`tariff_D`'s `LastModified` is within a few real seconds of the D→D+1 simulated midnight.

---

### R04 — Every cold start leaves a permanently failed billing run
- [x] Fixed 2026-09-27 (recommended fix): `list_pending_days()` only offers a day with at
  least one object under `voltstream-raw/meter_readings/sim_date=<day>/`
  (`S3Hook.list_keys(max_items=1)`), and logs the days it skips. Checked in the Airflow
  image against the object store: the seed day was skipped and only the first real day
  triggered. `voltstream.ps1 check` now reports a failed seed-day run as a regression.
  **Verified live** (Gate 4 run): no run for 2025-12-31; both billing runs `success`.

**Where:**

- Seed file: [`reference_dropper.py:175`](../src/voltstream/simulators/reference_dropper.py#L175)
- Trigger: `tariff_watcher_dag.list_pending_days`
- Failure: [`batch/daily_billing.py:379-381`](../src/voltstream/batch/daily_billing.py#L379-L381) raises `no raw readings`

**What's wrong:**

- The day-zero tariff (`D0 − 1`) exists only so the speed layer can cost day D0.
- The watcher treats it as a billable day. The partition is empty, so the job raises,
  retries twice, and ends `failed`.
- D6 said this run should succeed with zero rows. Instead, every demo starts with a red
  run in the Airflow UI.
- D6's planned watchdog ("age of the oldest tariff file without a `success` row") would
  alarm forever.

**Fix (recommended):** in `list_pending_days()`, only offer a day whose raw partition
exists, e.g.
`S3Hook(...).check_for_prefix(bucket_name="voltstream-raw", prefix=f"meter_readings/sim_date={day}/", delimiter="/")`.

The alternative is to make an empty day a successful zero-row run and relax
`verify_row_count` for it. That weakens the check, so it isn't recommended.

**Done when:** after `make clean && make up`, no `billing__*` run is ever `failed`.

---

### R05 — Trace-id dependency: null trace ids and a `ValueError` per request
- [x] Fixed 2026-09-27: `trace_id_provider` is `async`, with the same body.
  `tests/unit/test_api_dependencies.py` checks the logged `trace_id` against the response
  header for a sync and an async endpoint, with `TestClient` defaults. It fails on the old
  code with the `ValueError`. `/health` and `/metrics` still don't declare the dependency
  (the middleware option), which T104 doesn't require.

**Where:** [`api/dependencies.py:32-46`](../src/voltstream/api/dependencies.py#L32-L46) `trace_id_provider`

**What's wrong:**

- It is a **sync generator** dependency. FastAPI enters and exits it in separate
  thread-pool calls, each running in its own copy of the context.
- So the `contextvars` binding never reaches the (sync) endpoint.
- `ContextVar.reset(token)` then fails on exit because the token belongs to the other
  context.
- T104's *Done when* ("every request log line carries a `trace_id` matching the response
  header") fails.

**Evidence** (fastapi 0.141.1, starlette 0.52.1, anyio 4.15.1):

```
HTTP status      : 200
response header  : abc-123
log line trace_id: None
# and with TestClient's default raise_server_exceptions=True:
ValueError: <Token var=<ContextVar name='voltstream_trace_id' ...>> was created in a different Context
```

**Fix (verified):** make it `async def trace_id_provider(...) -> AsyncIterator[str]`,
with the same body. With that change: status 200, header `abc-123`, log line
`trace_id: abc-123`, no teardown error.

A pure-ASGI middleware would also cover `/health` and `/metrics`, which do not declare
the dependency.

**Done when:** an API test asserts the logged `trace_id` equals the `X-Trace-Id` response
header, using `TestClient` with default settings (it raises on server errors).

---

### R06 — `voltstream_consumer_lag` is always 0
- [x] Fixed 2026-09-26 — `KafkaLagListener` (`streaming/sources.py`) sets the gauge per partition from each query's progress (`latestOffset − endOffset`); the speed layer reports its slowest of three queries. The hard-coded `set(0)` calls are gone.

**Where:**

- [`streaming/raw_archiver.py:93-98`](../src/voltstream/streaming/raw_archiver.py#L93-L98) `_write_batch`
- [`streaming/speed_layer.py:368-369`](../src/voltstream/streaming/speed_layer.py#L368-L369) `_write_validation_batch`

**What's wrong:** both call `consumer_lag.labels(...).set(0)`. The archiver computes
`max_offset` and discards it. The metric can never show lag, so:

- T096 cannot be demonstrated.
- Grafana's lag panel (T152) will be flat.

**Fix:** after each micro-batch, set lag per partition to
`latestOffset − endOffset` from `query.lastProgress["sources"][0]`. Spark's Kafka source
reports both as per-partition offset maps. `foreachBatch` doesn't have the query handle,
so either:

- poll `query.lastProgress` from the driver thread, or
- use a `StreamingQueryListener.onQueryProgress`.

**Done when:** stopping the archiver for 60 s makes its lag rise while the speed layer's
stays flat, and the lag falls back after restart.

---

## High

### R07 — Batch duration never reaches a backend (T116)
- [x] Fixed 2026-09-26 — Pushgateway chosen. `daily_billing` and `daily_zone_rollup` call `metrics.push_metrics()` after committing; a push failure is a logged warning, never a failed run. Pushes are no-ops until `observability.pushgateway_url` is set in Phase 12.
  **Live since Phase 12 (2026-09-27):** the DAG sets `VOLTSTREAM__OBSERVABILITY__PUSHGATEWAY_URL` for billing, the rollup and reconciliation. Verified: the first run pushed 16.5 s (billing) and 12.8 s (rollup), shown on the pipeline health dashboard.

**Where:**

- [`batch/daily_billing.py:413-414`](../src/voltstream/batch/daily_billing.py#L413-L414)
- [`batch/daily_zone_rollup.py:213-214`](../src/voltstream/batch/daily_zone_rollup.py#L213-L214)

**What's wrong:** `batch_duration_seconds.observe()` writes to the in-process registry of
a container that exits seconds later with `auto_remove`. Nothing scrapes or pushes it.
T116 required choosing Pushgateway or Postgres and recording the choice. `rows_in` and
`rows_out` do reach `pipeline_runs`; duration does not.

**Fix:** pick one.

- **Pushgateway:** `.env.example` already reserves `PUSHGATEWAY_HOST_PORT`. Phase 11 added
  `metrics.push_metrics(job)` and `observability.pushgateway_url` (the reconciliation job
  uses them), so this is one call at the end of each job.
- **Postgres:** derive duration from `pipeline_runs.finished_at − started_at` (already
  written). Remove the dead `observe()` so it doesn't imply otherwise.

Record the choice for the report.

---

### R08 — Daily report file is deleted with its container (T131)
- [x] Fixed 2026-09-27 (object-store option): `generate_report.py` publishes to
  `voltstream-archive/reports/report_<date>.md` (`objectstore.report_key`) and still writes
  a local file with `--out DIR`. The DAG task runs on the **app** image with object-store
  credentials; the Spark image no longer copies `scripts/`. `voltstream.ps1 check` lists the
  report in stage 9. Tests: `tests/unit/test_generate_report.py`. Needs `build` for both
  images. **Verified live** (Gate 4 run): `report_2026-01-01.md` and `report_2026-01-02.md`
  listed in the archive bucket, both FINAL.

**Where:**

- [`airflow/dags/daily_billing_dag.py:240-265`](../airflow/dags/daily_billing_dag.py#L240-L265) `generate_report`
- [`scripts/generate_report.py:30`](../scripts/generate_report.py#L30)

**What's wrong:** the report is written to `/var/lib/voltstream/reports` inside a
`DockerOperator` container that has no mounts and uses `auto_remove="force"`, so it is
gone immediately. The task also runs on the Spark image; D6 assigns the report (and
reconciliation) to the app image.

**Fix:** either

- pass `mounts=[Mount(source="voltstream_reports", target="/var/lib/voltstream/reports", type="volume")]`
  and declare the volume in compose, or
- write the Markdown to MinIO (e.g. `voltstream-archive/reports/`) from the **app**
  image, which has boto3.

**Done when:** after a DAG run the file can be listed outside the task container.

---

### R09 — `/bill/delta` contradicts D4/D5 (T129)
- [x] Fixed 2026-09-28, for T163's demo, which shows the delta. `delta = speed_estimate −
  batch_final` (D4), so `delta == tariff_effect + data_effect` once reconciled.
  `delta_pct` goes through `pct_divergence`, moved from `batch/reconciliation.py` to
  `core/money.py` so the API and the reconciliation job share one definition (D5's gross
  charges base). `BillDelta`'s field descriptions are updated.
  `tests/unit/test_api_bill_delta.py` covers the sign, the identity, the net exporter and the
  open day; two of its tests fail on the old code.

**Where:** [`api/routers/households.py:165-176`](../src/voltstream/api/routers/households.py#L165-L176) `get_bill_delta`

**What's wrong:**

- `delta = final − estimate`, but D4 defines divergence as `S − B = tariff_effect + data_effect`.
  The effects therefore sum to `−delta`, and T129's *Done when* fails.
- `delta_pct` divides by `final_bill`, which D5 explicitly rejects: net exporters have
  negative or near-zero bills.
- The comment says it matches `reconciliation_daily.pct_divergence`; it doesn't.

**Fix:**

- `delta = estimate − final`.
- `delta_pct = 100 × |delta| / (batch.energy_charge + batch.fixed_charge)`, and 0 when
  that base is 0. Share the formula with `reconciliation.py` (Phase 11) rather than
  writing it twice.
- Update the field descriptions in `api/models.py` (`BillDelta`).

---

### R10 — `ruff format --check` fails
- [x] Fixed 2026-09-26 — `ruff format` applied repo-wide; `net()`'s docstring gained a prose line so the formula block keeps its indent.

12 files: `core/netting.py`, `core/tariff.py`, `logging_setup.py`, `metrics.py`,
`simulators/reference_dropper.py`, `tests/consistency/test_pure_vs_spark.py`,
`tests/property/test_tariff_invariants.py`, `tests/unit/test_config.py`,
`tests/unit/test_contracts.py`, `tests/unit/test_faults.py`,
`tests/unit/test_metrics.py`, `tests/unit/test_tariff.py`.

**Fix:** run `ruff format src tests` and commit.

Check the `netting.py` / `tariff.py` docstrings after formatting: ruff re-indents the
formula block inside them.

---

### R11 — `mypy src` fails
- [x] Fixed 2026-09-26 — `.venv` now has the full CI set (`.[dev,api,sim,spark]`); unused `hypothesis.*` override removed. `mypy src` is clean.

`src/voltstream/storage/postgres.py:26: Cannot find implementation or library stub for module named "psycopg_pool"`

The `.venv` predates the `pool` extra.

**Fix:** `pip install -e ".[dev,api,sim,spark]"` (the T169 CI set). Also drop the unused
`hypothesis.*` override, which mypy reports as an unused section.

---

### R12 — Bare `pytest` cannot import `scripts`
- [x] Fixed 2026-09-26 — `pythonpath = ["."]` in `[tool.pytest.ini_options]`; bare `pytest` collects everything.

[`tests/unit/test_contracts.py:16`](../tests/unit/test_contracts.py#L16) imports
`scripts.export_schemas`. `python -m pytest` works because it puts the working directory
on `sys.path`; bare `pytest` (the T169 CI command) gets `ModuleNotFoundError`.

**Fix:** add `pythonpath = ["."]` under `[tool.pytest.ini_options]`, or move the model
list into the package (e.g. `voltstream.contracts.MODELS`).

---

### R13 — Pre-commit D7 hook fails
- [x] Fixed 2026-09-26 — The hook now skips comment lines, as `make lint` does, and the compose comment is restored without the misspelling. `pre-commit run --all-files` passes except the end-of-file fix on `docs/report/main.tex`, left for its owner to avoid a merge conflict.

The `no-volstream-misspelling` pygrep hook matches the explanatory comment at
[`docker/docker-compose.yml:5`](../docker/docker-compose.yml#L5). `make lint` excludes
comment lines; the hook doesn't. So T017's `pre-commit run --all-files` fails.

**Fix:** reword the comment, or add an `exclude` / a pattern that skips `#` comment
lines, to match `make lint`.

---

### R38 — Transient infrastructure failures are fatal to a day's batch run
- [x] Fixed 2026-09-28:
  - `storage.postgres.connect()` is now the one way to open a short-lived connection. It
    retries *establishing* one on `psycopg.OperationalError`, five attempts over about
    15 s, and never retries a query. The speed layer's sinks, the billing and rollup jobs
    and the repositories' no-pool fallback all use it. A unit test fails if any module
    calls `psycopg.connect` directly again.
  - `daily_billing._start_run` marks the day's leftover `running` rows `failed` in the
    same transaction that opens its own (safe with `max_active_runs=1`).
  - The rollup keeps `retries=0`: its cross-check verdict is still never retried, and its
    connection is.
  - Tests: `tests/unit/test_postgres_connect.py`, and the abandoned-run test in
    `test_daily_billing.py`.
  - The sinks are the speed layer's path to Postgres, and the one that killed a streaming
    query on a single failed lookup and restarted the container 15 times. Spark's own S3A
    and Kafka clients already retry on their own, so they were left as they are.

*Found 2026-09-28, by the Phase 13 fault drills, on a saturated laptop (the SLA-pause backlog
catching up, the divergence fault running, and an image build at the same time).*

**What happened, from the Airflow logs:**

- `billing__2026-02-22`: `run_zone_rollup` failed with
  `failed to resolve host 'postgres': [Errno -3] Temporary failure in name resolution`, a
  transient Docker DNS timeout. It has `retries=0`, so `run_reconciliation` and
  `generate_report` never ran for that day: no reconciliation row, no report.
- `billing__2026-02-24`: `run_daily_billing` failed the same way; its retry succeeded. The
  failed attempt left its `pipeline_runs` row at `running` for good, since it could not
  reach Postgres to mark itself `failed`.
- `billing__2026-02-23`: a first billing attempt died in Spark (`An error occurred while
  calling o65.count`, a block write failing under memory pressure); the retry succeeded.

**What's wrong:**

- The batch jobs open connections with bare `psycopg.connect` (`daily_billing.py`,
  `daily_zone_rollup.py`, `storage/postgres.py`), so one failed DNS lookup ends the job.
- The rollup's `retries=0` is deliberate: a failed cross-check is a data verdict, and
  retrying it only fails again more slowly. But it makes every transient failure before
  the cross-check fatal too, and everything downstream of the rollup is lost for the day.
- An attempt killed before it can write `failed` leaves an orphan `running` row. Nothing
  reads `running` rows for decisions, so bills are unaffected, but the ledger shows a run
  that never ended.

**Fix:**

- Retry connection *establishment* only, a few times with backoff, on
  `psycopg.OperationalError`, in one shared helper used by the batch jobs. A cross-check
  verdict is still never retried.
- Or give the rollup `retries=1` and accept one slow repeat of a genuine cross-check
  failure.
- On start, mark the day's stale `running` rows from earlier attempts `failed`.
- Meanwhile, recover a lost day by clearing the failed task in Airflow (Graph →
  `run_zone_rollup` → Clear), which reruns it and everything downstream.

---

## Medium

### R14 — D2's config validation is missing
- [ ] Fixed

[`config.py:55-57`](../src/voltstream/config.py#L55-L57) `TariffConfig` has no
validators. D2 requires failing at load when:

- `up_to_kwh` is not strictly increasing,
- there isn't exactly one trailing `null`, or
- `len(blocks) != 3`.

Today a bad config fails later, in `build_blocks`'s `zip(strict=True)`, or silently
mis-bills if the boundaries are unordered.

---

### R15 — `os.environ` read outside `config.py`
- [ ] Fixed

T021: "No other module may read YAML or `os.environ` directly". T077: the session is
built "reading everything from config". Violations:

- `streaming/session.py`: checkpoint dir, S3 endpoint, credentials, region.
- `storage/objectstore.py:128`
- `api/main.py:72`
- `api/routers/alerts.py:40`
- `batch/daily_billing.py:300`

**Fix:**

- Add the fields to config: `minio.endpoint`, `speed_layer.checkpoint_dir`,
  `api.dashboard_dir`, `observability.alertmanager_url`, and an orchestrator run id.
- Credentials can stay env-only, but read them via `config.py` (like
  `postgres.password`).

---

### R16 — Weather has no effect on solar (T066)
- [x] Decided 2026-09-28: **not fixed.** Data-generation accuracy is out of scope; the focus
  is the pipeline and its orchestration. T160 demonstrates LowRenewableContribution through
  the simulated night instead (`inject_faults.sh renewable`), which needs no data change.
  Even fixed, 100 % cloud leaves 20 % of solar (`_SOLAR_CLOUD_ATTENUATION`), not reliably
  under 15 % at midday.

[`simulators/meter_producer.py:47`](../src/voltstream/simulators/meter_producer.py#L47)
calls `solar_kwh(...)` without `cloud_cover_pct`, so it is always 0. T066 requires
"scaled by a per-zone cloud-cover factor so the weather file has a real effect". T160
("force cloud cover to ~100 % via config override") has nothing to hook into.

**Fix:**

- Move the dropper's deterministic per-(zone, date) cloud-cover formula
  (`_weather_rows`) into a shared function.
- Call it from the producer.
- Add a config override for T160.

---

### R17 — Injected-fault counts are logs only (T073)
- [ ] Decided / fixed

`FaultInjector.injected_counts` appears only as `fault_*` fields in the producer's tick
log. "Show injection rate and rejection rate side by side" needs a metric. `metrics.py`
and `test_metrics.py` enforce exactly eight metrics, so either:

- add `voltstream_faults_injected_total{kind}` and update §10.1 and the test, or
- record that the comparison is done from logs.

---

### R18 — Master dataset is not fully raw (T080)
- [ ] Fixed

**Where:** [`streaming/sources.py:99-110`](../src/voltstream/streaming/sources.py#L99-L110) `read_meter_stream`

The archiver writes the `from_json`-parsed columns only. A payload that fails to parse
becomes all-nulls, and a new field in a future schema version is silently dropped: both
unrecoverable, which contradicts §5.4 ("raw data is a superset of every aggregate").

**Fix:** also select `F.col("value").cast("string").alias("raw_value")` and archive it.
Only the archiver needs it, so pass a flag, or add it in `with_partition_columns`.

---

### R19 — Gate 2 duplicate count is misattributed (T083)
- [ ] Fixed

**Where:**

- [`docs/assumptions.md`](assumptions.md) §7 "Gate 2"
- [`tests/integration/test_archiver_restart.py:144`](../tests/integration/test_archiver_restart.py#L144)

"Duplicate rows 334" is `rows − distinct event_id`. Injected duplicates share
`event_id` by design (`primary.model_copy()`), so about 2 % of 14,012 ≈ **275** of them
are injected, not restart duplicates.

**Fix:** count restart duplicates as rows with a repeated `(kafka_partition, kafka_offset)`,
i.e. `rows − Σ distinct offsets`. The probe already computes distinct offsets per
partition. Re-record the evidence.

---

### R20 — T096 missing; T095 deviation undocumented
- [ ] Fixed

- **T096** ("kill the archiver 60 s; speed-layer lag does not grow; archiver catches up")
  is not in `scripts/smoke_test.sh`. It needs R06 first.
- **T095:** the `sources.py` docstring explains, correctly, that Spark's Kafka source
  assigns partitions, so `kafka-consumer-groups --describe` can never show the two
  branches. That changes T095's *Done when*. Record it in `05-open-decisions.md` and for
  T182, and say what evidence replaces it (checkpoint offsets or the lag metric).

---

### R21 — E2E latency measured at the wrong point (T081/T092/T168)
- [x] Fixed 2026-09-26 — `sinks.observe_e2e_latency()` casts to double (microseconds). The speed layer observes at the zone sink after the upsert, per window written (the aggregation carries `newest_kafka_ts`); the validation query no longer observes. **Reset `speed_checkpoints` (`make clean`) before the next run: the zone query's state schema changed.**

**Where:**

- [`speed_layer.py:362-367`](../src/voltstream/streaming/speed_layer.py#L362-L367)
- [`raw_archiver.py:83-89`](../src/voltstream/streaming/raw_archiver.py#L83-L89)

**What's wrong:**

- The speed layer's latency comes from the *validation* query, which doesn't write to
  the zone table. It measures Kafka → validation, not Kafka → `zone_metrics_rt` commit.
  T168's table needs the latter.
- `F.unix_timestamp(...)` truncates to whole seconds, while the histogram's first bucket
  is 0.5 s.

**Fix:**

- Carry `F.max("kafka_timestamp")` through `zone_aggregation`, and observe
  `now − max_kafka_ts` in `_write_zone_batch` after the upsert.
- Use `F.col(ts).cast("double")` for sub-second resolution.

---

### R22 — Tariff reading gaps (T113)
- [ ] Fixed

**Where:**

- [`batch/daily_billing.py:170-191`](../src/voltstream/batch/daily_billing.py#L170-L191) `read_tariff`
- [`streaming/speed_layer.py:209-246`](../src/voltstream/streaming/speed_layer.py#L209-L246) `_tariff_for`

**What's wrong:**

- The CSV is **not validated against `TariffRecord`**, as T113 requires. A hand-edited
  restatement file with a negative rate, or `subsidy_pct > 100`, bills silently.
  Malformed decimals become nulls and surface as a misleading "missing tariff" error.
- **No weather join** on `grid_zone` (T113). It is either unimplemented or cut; if cut,
  record the cut (§9.2 item 4).
- `subsidy_flag` is parsed as `== "true"` in both layers (billing line 182, speed layer
  line 223). `TariffRecord` accepts `true/false/1/0/True`, so a restated CSV using `1`
  or `True` loses the subsidy silently.
- Speed and batch parse the same file two different ways: `.cast()` on strings vs a
  declared CSV schema. See R26.

**Fix:** one shared `read_tariff(spark, date)`:

- declared schema, `mode=FAILFAST`,
- validate the ~50 collected rows against `TariffRecord`,
- normalise `subsidy_flag` exactly as the contract does.

---

### R23 — Batch rejects: DLQ duplicates and non-idempotent inserts
- [ ] Fixed

**Where:**

- [`streaming/sinks.py:98-149`](../src/voltstream/streaming/sinks.py#L98-L149) `write_rejected`
- Called at [`daily_billing.py:389`](../src/voltstream/batch/daily_billing.py#L389)
- Read in [`repositories.py:299-314`](../src/voltstream/storage/repositories.py#L299-L314) `get_rejected_for_day`

**What's wrong:**

- The batch job publishes its rejects to `meter.readings.dlq` too, so the speed layer
  already put every bad record there. DLQ replay (§10.4) would re-inject each one twice.
- It inserts into `rejected_records` **before** the bill transaction, with no idempotency
  key. Each DAG retry (`retries=2`) and each restatement adds another full set.
- `get_rejected_for_day` counts speed and batch rows together, so report reject counts
  are at least 2× reality. **Confirmed on the Gate 4 run:** `report_2026-01-01.md` lists
  212 rejects. `rejected_records` holds 105 distinct rejected readings with
  `stage='batch'` and the same 105 again with `stage='speed'`, plus 2 more speed rows for
  injected duplicates of bad readings: the speed layer validates before it dedups.

**Fix:**

- Batch writes to Postgres only (`stage='batch'`), not the DLQ.
- Delete the day's `stage='batch'` rows and insert in the same transaction as
  `finalise()`.
- Have the report filter or group by `stage`.

---

### R24 — Zone rollup lineage and netting (T124)
- [ ] Fixed

**Where:** [`batch/daily_zone_rollup.py`](../src/voltstream/batch/daily_zone_rollup.py)

**What's wrong:**

- There is no `pipeline_runs` row (layer `batch_rollup` per §6.4). The
  `zone_metrics_daily.pipeline_run_id` it writes points at no ledger row, and the
  `VOLTSTREAM_ORCHESTRATOR_RUN_ID` the DAG passes is ignored.
- Lines 73-82 net **per reading** (per 9.6-minute tick), while bills net the **daily
  totals** per household. Zone `export_kwh` / `self_consumed_kwh` therefore ≠ Σ household
  values: midday per-tick export shows up in the zone figure but is netted away in the
  bill. The docstring (lines 69-71) claims they are sums of each other.
- `cross_check` (T125) has no test.

**Fix:**

- Add running/success/failed ledger rows.
- Either net per household-day, then sum by zone (matching bills), or keep interval
  netting and correct the docstring and report label ("physical, per-interval").
- Add a unit test for `cross_check`.

---

### R25 — Restatement overwrites history
- [ ] Decided / fixed

**Where:**

- `daily_billing.finalise` (`ON CONFLICT DO UPDATE`)
- `archive_tariff` (`mode("overwrite")`)

**What's wrong:** D6 says each restatement is preserved and "the audit trail shows the
wrong bill, the corrected bill, and which run produced each". In fact only
`pipeline_runs` keeps both runs:

- the superseded bills are overwritten, and
- the archived tariff of the superseded run is overwritten too.

That breaks D2's lineage sentence for the old run. In addition, a restatement that bills
fewer households leaves the old rows behind with the old `pipeline_run_id`.

**Fix (pick):**

- a `household_bill_history` table (append-only, keyed by `pipeline_run_id`), or keep
  every run's rows and select the current one via `pipeline_runs.status='success'`;
- archive the tariff per run (`tariff/sim_date=…/run_id=…/`);
- delete-then-insert the day's rows in the finalise transaction.

Otherwise soften the D6 and report wording.

---

### R26 — Speed and batch are forking outside `core/`
- [ ] Fixed

These are duplicated, some with different mechanisms:

| Helper | Where it's duplicated |
|---|---|
| `_boundaries()` | `speed_layer.py:72-86`, `daily_billing.py:102-111` |
| `_known_household_ids()` | `speed_layer.py:335-338`, `daily_billing.py:155-157` |
| Event-ts bounds | `speed_layer.py:401-404`, `daily_billing.py:160-162` |
| Tariff CSV parsing | see R22 |

`daily_zone_rollup.py` imports the underscore-private helpers from `daily_billing.py`.
`speed_layer._boundaries`' own "ponytail" comment says to lift this into a shared helper
before the batch job copies it; it was copied anyway.

**Fix:** one module (e.g. `voltstream/streaming/reference.py`, or `batch/common.py`)
imported by both layers.

Phase 11 already added `get_config().tariff.boundaries()`, and reconciliation uses it.
Replace the two `_boundaries()` copies with it. Reconciliation parses the tariff CSV
through `TariffRecord` (`reconciliation.parse_tariff_csv`), which is the validation R22
wants for the Spark readers too.

---

### R27 — Tests that copy the code under test
- [ ] Fixed

- **T128:** [`tests/integration/test_merge_function.py:104-111`](../tests/integration/test_merge_function.py#L104-L111)
  `_merge_source()` re-implements the merge. If `households.get_bill` dropped the
  `is_day_finalised` check, all five tests would still pass, and the "neither → 404"
  case never checks for a 404. Call `households.get_bill(...)` directly (it's a plain
  function), or use `TestClient` with the pool fixture.
- **T118:** [`tests/unit/test_daily_billing.py:148-178`](../tests/unit/test_daily_billing.py#L148-L178)
  rebuilds the effective-dated window inside the test instead of calling `read_tariff`.
  Split `read_tariff` into I/O plus a pure `select_effective(df, sim_date)` and test that.

---

### R36 — DAG tasks always get the default Postgres and MinIO passwords
- [ ] Fixed

*Found 2026-09-27, while fixing R08.*

**Where:**

- [`airflow/dags/daily_billing_dag.py`](../airflow/dags/daily_billing_dag.py): every
  `DockerOperator`'s `private_environment`
- [`docker/docker-compose.yml`](../docker/docker-compose.yml): the `airflow` service's
  `environment`

**What's wrong:** each task passes `os.environ.get("POSTGRES_PASSWORD", "voltstream")`,
`os.environ.get("MINIO_ROOT_USER", "voltstream")` and
`os.environ.get("MINIO_ROOT_PASSWORD", "voltstream-dev")`. The airflow container has none of
these variables: compose uses them only inside the two `AIRFLOW_CONN_*` URLs and the
metadata-database URL. So the tasks always get the defaults. With the default `.env`
nothing breaks. Change either password in `.env` and every billing, rollup,
reconciliation and report task fails to authenticate, while the watcher's S3 hook, which
reads the connection, keeps working.

**Fix:** pass `POSTGRES_PASSWORD`, `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` to the
`airflow` service's `environment` in compose, with the same defaults.

**Done when:** with a non-default `POSTGRES_PASSWORD` in `.env`, a `daily_billing` run
succeeds.

---

## Low

### R28 — Dropper bypasses `objectstore.py` (T099)
- [ ] Fixed

`reference_dropper.py` still has its own `_tariff_key` / `_weather_key` and boto3 client.
Its docstring says it would switch once `objectstore.py` existed. Use
`objectstore.landing_tariff_key` / `landing_weather_key` / `get_client`. Phase 11 already
gave `get_client` the path-style addressing the dropper sets.

### R29 — Acceptance checks without committed tests
- [ ] Fixed

- **T029 / T031:** round-trip `0.412`, 5 dp raises, unknown/missing field raises;
  negative rate raises, unknown tier raises. All verified manually and all correct, but
  no test exists.
- **T058:** the `compute_bill` doctest passes (`python -m doctest -v src/voltstream/core/tariff.py`)
  but isn't collected. Add `--doctest-modules` scoped to `src/voltstream/core`.
- **T125:** no test that a corrupted zone total fails `cross_check`.
- **T071:** the rate test omits `out_of_order_rate` and `dropout_probability_per_meter_tick`.

### R30 — Documentation drift
- [ ] Fixed

- **T037 / T038:** their *Done when* requires Master Design §6.4 to gain
  `zone_metrics_daily` and `households`. It hasn't.
- **T040:** "record the rule in the decision log" isn't done. It is only mentioned inside
  D6.
- **T126:** no Gate 4 prerequisite evidence in `assumptions.md` §7 (Gates 2 and 3 have
  theirs).
- **`config/base.yaml`:** comments still say out-of-order events are "always absorbed"
  (refuted by the D3 correction). The alert names in comments (`RejectRateHigh`,
  `BatchSlaMissed`) differ from the task names (`HighRejectRate`, `BatchSLAMiss`). It
  still mentions `prometheus-fastapi-instrumentator`, as does the `metrics.py` docstring.
- **`.env.example`:**
  - claims `smoke_test.sh` asserts that `.env` and `base.yaml` names agree; it doesn't;
  - `MINIO_BUCKET_CHECKPOINTS` is still present (T047 recommended removal);
  - `AIRFLOW_DB_*` are unused (Airflow connects as the `voltstream` superuser, and
    `00_airflow_db.sql` creates no `airflow` role).
- **README:** "builds two images" (it's three); "Lambda vs Kappa … §3" (it's §4); the
  services table omits Airflow and the socket proxy.
- **`repositories.get_latest_zone_metrics` docstring:** cites an index
  `(grid_zone, window_start DESC)` that doesn't exist (see R32).
- **`scripts/generate_report.py`:** says provisional figures "come from the speed layer";
  it reads no speed-layer data.

### R31 — `DatabaseUnavailable` too broad; readiness can hang
- [ ] Fixed

- [`storage/postgres.py:85-112`](../src/voltstream/storage/postgres.py#L85-L112)
  `transaction()` turns *any* exception inside the `with` block (a SQL typo, a mapping
  bug) into `DatabaseUnavailable`. That contradicts the class docstring and makes a bug
  look like an outage (503 instead of 500). Catch `psycopg.OperationalError` /
  `psycopg_pool.PoolTimeout` only.
- Once the pool's idle connection is gone, `/health/ready` waits for the pool's default
  30 s timeout; the compose healthcheck timeout is 5 s. Pass a short `timeout` to
  `pool.connection()` in `healthcheck()`.

### R32 — `/zones/load` query and table growth (T041/T100)
- [ ] Fixed

- `DISTINCT ON (grid_zone) … ORDER BY grid_zone, window_start DESC` has no matching
  index (the PK is ascending; `idx_zone_metrics_rt_window` is `window_start DESC` only).
  T041's `EXPLAIN` check is unlikely to pass.
- `zone_metrics_rt` gains ~480 rows per simulated day (~138k per real day) and is never
  pruned.
- Fix: add `(grid_zone, window_start DESC)`, and optionally a retention delete.

### R33 — Raw tuples from repositories (T101)
- [ ] Fixed

`get_reconciliation` returns `list[tuple]` via `SELECT *`, and `get_reconciliation_summary`
returns a tuple. `get_finalised_bill_row` returns an untyped `dict`. Phase 11 will use
these, so make them NamedTuples first.

### R34 — Small test inconsistencies
- [ ] Fixed

- [`tests/property/test_tariff_invariants.py:154-170`](../tests/property/test_tariff_invariants.py#L154-L170):
  the comment says ε = 0.01; the code uses `Decimal("0.02")`. 0.02 is justified (two
  rounded lines plus the subsidy line); fix the comment.
- T062's `grep -nE "\b[0-9]+\.[0-9]+\b" core/spark_expr.py` matches `Decimal("0.00")` at
  line 162 (a string, not a float). Use `Decimal(0)` so the check passes literally.

### R35 — A household with no valid readings gets no bill
- [ ] Decided / fixed

`aggregate_to_daily(valid)` only produces rows for households with valid readings, and
`join_tariff` left-joins totals → tariff. A meter that is offline or rejected all day
therefore has no bill, and `verify_row_count` (50) fails the DAG. It is rare, but decide
whether such a household gets a fixed-charge-only bill (tariff left-join readings) or
the check should expect it.

---

### R37 — A stalled producer tick is never made up
- [ ] Fixed

*Found 2026-09-27, on the Gate 4 run.*

**Where:** [`simulators/meter_producer.py`](../src/voltstream/simulators/meter_producer.py) `main()`: `time.sleep(max(0.0, emit_interval_seconds - elapsed))`, and `_run_tick()`: `event_ts = simclock.sim_now()`.

**What's wrong:** each tick sleeps for whatever is left of 2 s after its own work, and
stamps its readings with the simulated time at which it happens to run. A tick that
stalls (9 of 137 on 2026-01-02 took 3–4.8 s, the rest 2.0 s) pushes every later tick back
and is never made up. The day then has 138 ticks rather than 150, readings are on average
about 10.4 simulated minutes apart rather than 9.6, and each stall stretches one gap in
every meter's series to 14–23 simulated minutes.

It doesn't affect correctness: both layers see the same readings, and the reconciliation
and the kWh gap are ratios. It does make figures quoted as "150 readings per meter per
day" wrong, and daily energy about 8 % lower than the profiles imply.

**Fix:** schedule ticks on a fixed grid (`next_tick += interval`; sleep until `next_tick`,
and run at once when behind). Optionally derive `event_ts` from the tick index
(`anchor + k × 9.6 sim min`) so every meter-day has exactly 150 readings however the host
behaves.

**Done when:** a complete simulated day has 150 ticks in the producer log, and
`household_bill_daily.readings_count` is 150 minus that household's rejects.

---

## Verify with the stack up

These could not be checked without Docker:

- [ ] `pytest -m integration` (19 tests) passes against a fresh `make clean && make up`.
- [ ] R02: `test_watermark_behaviour.py` passes, and its figures are recorded in D3 and
  `assumptions.md` §2.
- [x] R08: after a billing run, `voltstream.ps1 check` shows `Daily report` PASS
  (2026-09-27, Gate 4 run).
- [ ] **Kafka persistence:** compose mounts `kafka_data` at `/var/lib/kafka/data` but
  doesn't set `KAFKA_LOG_DIRS`. Confirm the broker writes there:
  `docker exec voltstream-kafka grep log.dirs /opt/kafka/config/server.properties`.
  If it writes elsewhere, topics are lost on `make down && make up` while the Spark
  checkpoints survive, and `failOnDataLoss=true` will stop both streaming jobs.
- [ ] T111: `df.explain()` in `daily_billing.read_day` shows `voltage` absent from
  `ReadSchema` and the date under `PartitionFilters`.
- [ ] T041: `EXPLAIN` on the merge-function lookup and on `/zones/load` (see R32).
- [ ] T121: after the R03 fix, a grace of ~90 s appears in the task log.
- [ ] T123: a `billing__D__r2` run produces a `superseded` + `success` pair with distinct
  `orchestrator_run_id`s.
- [ ] T105: `/health/ready` returns 503 promptly with Postgres stopped (see R31).

---

## Suggested order for the session

1. **R10–R13**: tooling green first, so every later fix is checked by `make lint` / `make test`.
2. **R05**: one-line change, fixes every API request.
3. **R03 → R04**: day-boundary semantics; they touch the same dropper/watcher code.
4. **R01, R02**: re-run T094 after these and re-record `assumptions.md` §2.
5. **R06, R07, R08, R21**: observability, before screenshots are taken.
6. **R09, R22–R27**: correctness and tests.
7. The rest, plus a T182 pass over the docs (R30).
