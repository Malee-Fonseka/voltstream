# voltstream — Implementation Task List

**Companion to [`Master_Design.md`](Master_Design.md).** That document says *what* and
*why*. This one says *in what order*, in units small enough to review one at a time.

---

## How to use this list

- Tasks are **strictly ordered**. `T042` assumes `T041` is merged. Where order genuinely
  does not matter, tasks are adjacent.
- Every task is sized to **one commit / one reviewable diff** — typically 1–3 files.
- Each task states **Files**, **Do**, and **Done when**. "Done when" is the acceptance
  check; if you cannot demonstrate it, the task is not done.
- **Gates** (`G1`–`G5`) are hard stops carried over from Master Design §9. Do not proceed
  past a gate with it red.
- `§x.y` references point into `Master_Design.md`.

### Conventions

| | |
|---|---|
| Branch | `feat/<task-id>-<slug>`, e.g. `feat/t042-postgres-schema` |
| Commit | `T042: postgres schema — core tables` |
| Rule | `main` stays green. One review per PR. |
| Never | Business logic outside `core/`. Magic numbers outside `config/base.yaml`. |

### Current repository state (verified before writing this list)

- 54 files exist under `src/`, `tests/`, `docker/`, `airflow/` — **all empty stubs**
  (only the four Dockerfiles / compose file contain a single comment line).
- Layout is flat `src/…`, **not** the `src/voltstream/…` package layout §7.2 requires.
- Several stub filenames are misspelled relative to §7.2 (see `T012`).
- `docs/` contains only `Master_Design.md`.
- `config/`, `dashboard/`, `.github/`, `docs/architecture/`, `tests/property/`,
  `tests/consistency/`, `tests/integration/`, `tests/fixtures/` do not exist.
- `.venv` is **Python 3.13** — see `T002`. This is a blocking compatibility risk.

---

## Phase 0 — Decisions, hygiene, scaffold

> The design document leaves seven things genuinely ambiguous or self-contradictory. Each
> is load-bearing for code written later, and each is cheap to settle now and expensive to
> settle in Phase 7. Resolve them first, in writing.

### T001 — Create the decision log
**Files:** `docs/architecture/05-open-decisions.md` (new), `docs/architecture/` (new dir)
**Do:** Create an ADR-style file with one section per decision `D1`–`D7` below, each with
*Context / Options / Decision / Consequence*. Leave Decision blank for now.
**Done when:** File exists with seven headed, empty-decision sections.

### T002 — D1: Pin the Python version (BLOCKING)
**Files:** `docs/architecture/05-open-decisions.md`, `.python-version`
**Do:** The venv is Python 3.13. PySpark 3.5.x is built and tested against Python 3.8–3.11;
3.13 is outside that range and `pip install pyspark==3.5.*` will either fail or break at
runtime in Py4J/cloudpickle. This matters beyond Docker, because
`tests/consistency/test_pure_vs_spark.py` needs **local** PySpark — on the laptop and in CI.
Decide one of:
- **(recommended)** Pin **Python 3.11** + `pyspark==3.5.*`. Recreate `.venv`.
- Pin Python 3.12 + `pyspark==4.0.*` — newer, less battle-tested, some API changes.
- Keep 3.13 and run consistency tests only inside the Spark container — loses fast local
  feedback and complicates CI.
Record the choice and the Spark version it implies.
**Done when:** D1 filled; `.python-version` committed; `python --version` in the venv matches
it; `python -c "import pyspark; print(pyspark.__version__)"` prints the pinned version.

### T003 — D2: Tariff source of truth (config vs CSV)
**Files:** `docs/architecture/05-open-decisions.md`
**Do:** §6.2's tariff CSV carries `tariff_rate, billing_tier, subsidy_flag, fixed_charge,
export_rate` while Appendix A's `base.yaml` carries `blocks`, `fixed_charge_by_tier`,
`subsidy_discount_pct`, `export_credit_rate`. **These overlap and can disagree.** Pin it:
- **(recommended)** `base.yaml` defines the *band structure* (`up_to_kwh` boundaries) and
  default rates. The CSV supplies everything per-household and per-day: `billing_tier`,
  `subsidy_flag`, `fixed_charge`, `export_rate`, `effective_date`. Either drop `tariff_rate`
  from the CSV or redefine it explicitly (e.g. as the top-block rate) — do not leave it
  undefined.
- Alternative: the CSV carries only `billing_tier` / `subsidy_flag` / `effective_date` and
  all money comes from config. Simpler — but then a "retroactive tariff correction" becomes a
  code change, which destroys the backfill demo (§5.6). **Do not choose this.**
**Done when:** D2 filled; the exact final CSV column list is written down and is what `T031`
will implement.
**Outcome (2026-09-19):** Decided as **Option 3**, not the Option 1 lean above — see D2 in
`05-open-decisions.md` for the reasoning (lineage hole; §2.3 calls rates data; stale-tariff
divergence needs rates to move). Final contract is ten columns:
`household_id, effective_date, billing_tier, subsidy_flag, subsidy_pct, fixed_charge,
block_1_rate, block_2_rate, block_3_rate, export_rate`. Config keeps only block boundaries plus
`generator_defaults` that only the dropper may read.

### T004 — D3: Watermark units (BLOCKING, subtle)
**Files:** `docs/architecture/05-open-decisions.md`
**Do:** `speed_layer.watermark_seconds: 30` is applied to `event_ts`, which is **simulated**
time. With `TIME_SCALE = 288`, 30 simulated seconds is **0.104 real seconds** — effectively
no watermark at all. Conversely, reading it as real seconds and converting
(30 × 288 = 8,640 simulated seconds = 2.4 simulated hours) would hold roughly ten 15-minute
windows open at once.

Note also that the *natural* event-time skew here is ~0: the producer stamps every household
with the same simulated instant each tick, so **all lateness in this system is injected
deliberately** by `faults.out_of_order_rate`. The watermark must therefore be sized against
the injected lateness spread, not against network jitter. Decide:
- **(recommended)** Keep the watermark in **event-time (simulated) units** and rename the key
  to `watermark_sim_minutes` so the unit is unambiguous at the call site. Set it *smaller*
  than the out-of-order injection spread, so the speed layer genuinely drops some
  stragglers — that dropped data is what makes the speed-vs-batch delta (§3.2) real and
  demonstrable. Suggested starting point: injection spread uniform over 1–20 simulated
  minutes, watermark 5 simulated minutes.
- Alternative: express it in real seconds and convert inside `simclock.py`.
**Done when:** D3 filled with the chosen unit, the chosen value, the chosen injection spread,
and one sentence on why some events are *expected* to be dropped.
**Outcome (2026-09-19):** Simulated units, as recommended — but **not** the suggested numbers.
A sizing model (see D3) showed W = 5 / spread 1–20 drops only 2.9 % of late events, because
one trigger interval is 48 sim min and the watermark advances once per trigger. Pinned:
`watermark_sim_minutes: 30`; out-of-order U[1, 30] sim min (always absorbed); dropouts become
**store-and-forward** (30 real s buffer, flushed on reconnect) at `0.002` per meter-tick, of
which ~57 % is dropped by the speed layer → expected kWh gap ≈ 1.7 %. Output mode `update`.
Batch gets a 90 real-s late-data grace after the tariff file lands.

### T005 — D4: Who computes the provisional bill
**Files:** `docs/architecture/05-open-decisions.md`
**Do:** §4.4 justifies duplicating the tariff logic on the grounds that "the FastAPI
container has no Spark but still computes provisional estimates" — yet §6.4 stores
`household_running_rt.estimated_bill NOT NULL`, implying the speed layer computes it. If the
API never calls `core/tariff.py`, the pure module has no production caller and the §4.4
argument is hollow under viva questioning. Decide:
- **(recommended)** Both, deliberately. The speed layer persists `estimated_bill` via
  `core/spark_expr.py` (reconciliation and Grafana read the stored value), and the merge
  endpoint recomputes the provisional figure from the stored kWh columns via
  `core/tariff.py`. `tests/consistency/test_pure_vs_spark.py` is what guarantees the two
  agree — which is exactly the story §4.4 wants to tell.
- Alternative: speed layer only, with the API's use of `core/` confined to `/reports`.
**Done when:** D4 filled. If the recommended option is chosen, note that the merge endpoint
returns the recomputed value and logs a warning when it differs from the stored one.
**Outcome (2026-09-19):** Decided as **Option 2, refined** — not the Option 1 lean above.
The API is `SELECT`-only (as §6.1/§8.3 already say); the speed layer persists the **full**
provisional breakdown via `spark_expr.py` (free — it computes it for batch anyway); and the
pure module's production caller is **`reconciliation.py`**, which uses it to compute the
counterfactual `bill(speed_kwh, today's tariff)` and split each divergence into a
`tariff_effect` and a `data_effect`. That gives the consistency test a production consequence.
See D4 for why recomputing in the API would *add* a drift surface rather than remove one.

### T006 — D5: Subsidy and final-bill arithmetic
**Files:** `docs/architecture/05-open-decisions.md`
**Do:** `subsidy_discount_pct: 25.0` has no stated base. Pin the exact formula, because
`core/tariff.py` and `core/spark_expr.py` must implement it identically:
```
energy_charge    = Σ over blocks of (kwh_in_block × block_rate)      # marginal / slab
fixed_charge     = tariff.fixed_charge                               # per household, from CSV
subsidy_discount = energy_charge × subsidy_discount_pct / 100  if subsidy_flag else 0
export_credit    = export_kwh × tariff.export_rate
final_bill       = energy_charge + fixed_charge − subsidy_discount − export_credit
```
Also pin two things the document never states:
1. **Is `final_bill` floored at 0?** A large exporter can go negative. Recommended: allow
   negative and treat it as a credit carried forward — state it rather than silently clamping.
2. **Confirm the blocks are marginal/slab**, not "whole consumption at the top rate". Marginal
   is what makes the continuity property test (§9 Phase 2) satisfiable at all; a
   whole-consumption-at-top-rate reading is discontinuous at every boundary by construction.

Note the fixed charge is keyed off the household's `billing_tier` from the CSV, **not** off
consumption — so it is a per-household constant and does not break continuity in consumption.
Keep it that way.
**Done when:** D5 filled with the formula above (or the agreed variant) and an explicit
yes/no on the negative-bill question.
**Outcome (2026-09-19):** Formula as above with `tariff.subsidy_pct` (D2). Pinned in D5:
**`Decimal` everywhere** (Python `Decimal`, Spark `DecimalType`, kWh `(12,4)`, money `(12,2)`)
— decided by batch **determinism** (float sums are order-dependent; Spark's sum order is not)
as much as by the half-cent case; **half-up rounding per line item**, totals are exact sums of
rounded parts; subsidy on the energy charge only; **negative bills allowed**, never clamped
(carry-forward is cross-day state and would break lineage); `pct_divergence` base is gross
charges, not `final_bill`. New file `core/money.py` holds the precision constants. Three
verified worked examples in D5 become the first T172/T173 fixtures.

### T007 — D6: How Airflow submits Spark jobs, and what the sensor is
**Files:** `docs/architecture/05-open-decisions.md`
**Do:** Two coupled sub-decisions.

*Submission.* §5.6 requires a **thin** Airflow image with no PySpark. That rules out
`SparkSubmitOperator`, which needs `spark-submit` on the Airflow container. Choose:
- **(recommended)** `DockerOperator` running the `voltstream-spark` image, with the Docker
  socket mounted into the Airflow container. Keeps Airflow thin; keeps the boundary honest.
- `BashOperator` + `docker exec` into a long-lived Spark container — simpler, uglier.
- A small HTTP job-runner inside the Spark image that Airflow calls — most work.

*Sensor.* §8.3 says `FileSensor`, but the tariff file lands in **MinIO**, not on a local
filesystem — `FileSensor` will never see it. Use `S3KeySensor` from the Amazon provider with
an `aws_conn_id` pointing at the MinIO endpoint, or a `PythonSensor` wrapping boto3.
**Done when:** D6 filled with both the submission mechanism and the sensor type, and
`00-master-design.md` §8.3 corrected so it no longer says `FileSensor`.
**Outcome (2026-09-19):** `DockerOperator` **through `tecnativa/docker-socket-proxy`** (not
the raw socket — avoids Docker Desktop permission failures); `S3KeySensor` via an
`AIRFLOW_CONN_…` env var. A third sub-decision surfaced: Airflow's clock is real, the
pipeline's days are simulated, so `{{ ds }}` can never be a sim date and §5.6's
`airflow dags backfill` cannot address one. Resolved by **Design B**: a `tariff_watcher` DAG
(new file) triggers one `daily_billing` run per new tariff file with `sim_date` parsed from
the filename and `run_id = billing__<date>`; restatement is a new run `billing__<date>__r<n>`,
preserved alongside the original. No `simclock`/`voltstream` in Airflow. **Airflow 3.x**,
one `airflow standalone` container, LocalExecutor, separate metadata DB; tripwire to 2.11.
Verification via `SQLCheckOperator`s. §8.3 corrected in the Master Design.

### T008 — D7: Package and repository name spelling
**Files:** `docs/architecture/05-open-decisions.md`
**Do:** The working directory is `volstream` (no `t`); the design document, the Python
package, the Compose project name and every `import` say `voltstream`. §7.1's entire point is
one name everywhere. Decide to rename the directory and git remote to `voltstream`
(recommended), or to accept the mismatch and note it explicitly. Either way, **the Python
package is `voltstream`.**
**Done when:** D7 filled; if renaming, the local directory and git remote are renamed.
**Outcome (2026-09-19):** **Option 2** — repository and folder stay `volstream`; package,
Compose project and every runtime name are `voltstream`. `name: voltstream` in the compose
file is what makes this safe (Compose would otherwise derive the project name from the
`docker/` folder). README carries a one-line note; `grep -rni volstream` over
`src/ docker/ config/ airflow/ scripts/ tests/` must stay empty and is added to `make lint`.

### T009 — Line-ending policy
**Files:** `.gitattributes` (new)
**Do:** You are developing on Windows and shipping `.sh` scripts into Linux containers. CRLF
line endings make `scripts/demo.sh` and `docker/init/*/*.sh` fail inside a container with a
cryptic `bad interpreter` error. Add:
```
* text=auto eol=lf
*.sh    text eol=lf
*.ps1   text eol=crlf
*.drawio text eol=lf
*.png   binary
```
**Done when:** `git add --renormalize . && git status` is clean, and once the init scripts
exist, `file docker/init/kafka/create_topics.sh` reports no CRLF terminators.

### T010 — Real `.gitignore`
**Files:** `.gitignore`
**Do:** Current content is two lines. Add `__pycache__/`, `*.py[cod]`, `.pytest_cache/`,
`.ruff_cache/`, `.mypy_cache/`, `*.egg-info/`, `build/`, `dist/`, `.coverage`, `htmlcov/`,
`spark-warehouse/`, `metastore_db/`, `derby.log`, `logs/`, `.idea/`, `.vscode/`, `.DS_Store`.
Do **not** ignore `config/` — it is a reviewable, graded artefact (§5.7).
**Done when:** `git status` is clean after a full local test run and a local Spark job.

### T011 — LICENSE
**Files:** `LICENSE`
**Do:** Fill the empty file. MIT is the conventional choice for coursework.
**Done when:** File contains a complete licence text with the correct year and holder.

### T012 — Restructure `src/` into the package layout
**Files:** everything under `src/`
**Do:** `git mv` the flat tree into `src/voltstream/`, fixing the misspelled stubs on the way:

| From | To |
|---|---|
| `src/simlock.py` | `src/voltstream/simclock.py` |
| `src/contracts/references.py` | `src/voltstream/contracts/reference.py` |
| `src/core/validations.py` | `src/voltstream/core/validation.py` |
| `src/simulators/profile.py` | `src/voltstream/simulators/profiles.py` |
| `src/storage/objectsotre.py` | `src/voltstream/storage/objectstore.py` |
| `src/streaming/source.py` | `src/voltstream/streaming/sources.py` |
| `src/streaming/raw_achiever.py` | `src/voltstream/streaming/raw_archiver.py` |
| `src/batch/daily_zone_rolleup.py` | `src/voltstream/batch/daily_zone_rollup.py` |
| `src/batch/reconcilliation.py` | `src/voltstream/batch/reconciliation.py` |
| `src/api/routers/zone.py` | `src/voltstream/api/routers/zones.py` |
| everything else | same name, under `src/voltstream/` |

Add `__init__.py` to every subpackage (`contracts`, `core`, `simulators`, `streaming`,
`batch`, `storage`, `api`, `api/routers`). Create `src/voltstream/contracts/schemas/`.
**Done when:** `find src -name '*.py' | sort` matches §7.2 exactly, and every directory under
`src/voltstream/` has an `__init__.py`.

### T013 — Create the missing directories
**Files:** `config/`, `dashboard/`, `.github/workflows/`, `docs/architecture/diagrams/`,
`docs/report/`, `tests/property/`, `tests/consistency/`, `tests/integration/`,
`tests/fixtures/`, `docker/init/kafka/`, `docker/init/minio/`
**Do:** Create each, with a `.gitkeep` where it would otherwise be empty. Also
`git mv docker/init/kafka_create_topics.sh docker/init/kafka/create_topics.sh` to match §7.2.
**Done when:** The directory tree matches §7.2.

### T014 — Move the master design into `docs/architecture/`
**Files:** `docs/Master_Design.md` → `docs/architecture/00-master-design.md`
**Do:** `git mv` it, then fix the relative link at the top of **this** file and anywhere else
it is referenced.
**Done when:** `grep -rn "Master_Design.md" . --exclude-dir=.git` returns only intentional
historical notes.

### T015 — `pyproject.toml`
**Files:** `pyproject.toml`
**Do:** Fill the empty file. Build backend (setuptools or hatchling); `[project]` with
`name = "voltstream"` and `requires-python` matching `T002`; package discovery pointed at
`src`. Optional-dependency groups exactly as §7.3: `spark`, `api`, `sim`, `dev`. Pin majors,
not exact patches — except `pyspark`, which is pinned exactly.
**Done when:** `pip install -e ".[dev]"` succeeds and `python -c "import voltstream"` works
from a directory other than the repository root.

### T016 — Tooling configuration
**Files:** `pyproject.toml`
**Do:** Add `[tool.ruff]` (line length 100; select `E,F,I,UP,B`), `[tool.mypy]`
(`strict = true` for `voltstream.core.*` at minimum, `ignore_missing_imports` for pyspark),
`[tool.pytest.ini_options]` (`testpaths`, markers `integration` and `slow`,
`--strict-markers`), and `[tool.coverage]`. Per D7, the `make lint` target (T110) also runs
`grep -rni "volstream" src/ docker/ config/ airflow/ scripts/ tests/` and fails if it matches
— the misspelling is permitted only in the README's note and in repository URLs.
**Done when:** `ruff check .`, `mypy src` and `pytest --collect-only` all run without
*configuration* errors. Findings are fine; config errors are not.

### T017 — Pre-commit hooks
**Files:** `.pre-commit-config.yaml`
**Do:** Fill the empty file: `ruff` (lint + format), `mypy`, `end-of-file-fixer`,
`trailing-whitespace`, `check-yaml`, `check-merge-conflict`, `mixed-line-ending --fix=lf`.
**Done when:** `pre-commit run --all-files` passes, after one auto-fix commit if needed.

### T018 — `.env.example`
**Files:** `.env.example`
**Do:** Fill the empty file with every variable the stack needs, with safe local defaults and
no real secrets: `POSTGRES_*`, `KAFKA_BOOTSTRAP_SERVERS`, `MINIO_ROOT_USER`,
`MINIO_ROOT_PASSWORD`, `MINIO_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AIRFLOW_UID`, `AIRFLOW__CORE__FERNET_KEY`, `VOLTSTREAM_ENV`. One comment line per variable.
**Done when:** `cp .env.example .env` produces a file the stack can boot from unmodified.

---

## Phase 1 — Cross-cutting modules

> §9 Phase 0: "`config.py`, `simclock.py`, `logging_setup.py`, `metrics.py` — the four
> cross-cutting modules every other module imports." Nothing else can be written well until
> these exist.

### T019 — `config/base.yaml`
**Files:** `config/base.yaml`
**Do:** Transcribe Appendix A, **with the corrections from `T003`–`T006` applied** (watermark
key renamed per D3, tariff keys per D2, subsidy base documented per D5). Add the sections
Appendix A omits but the code needs: `postgres`, `minio` (bucket names from §6.3), `api`,
`batch`, `observability`, `sim_epoch`. Comment every value with its unit and whether it is
simulated or real time.
**Done when:** The file parses as YAML, and every key in Appendix A is either present or
explicitly superseded by a comment saying so.

### T020 — `config/local.yaml`
**Files:** `config/local.yaml`
**Do:** Laptop overrides only — a deep-merge layer on top of `base.yaml`. Keep it short
(fewer households for a fast test run, chattier logging).
**Done when:** Loading with `VOLTSTREAM_ENV=local` yields a merged config where overridden
keys change and all others are inherited.

### T021 — `config.py`: the typed settings object
**Files:** `src/voltstream/config.py`
**Do:** Pydantic-settings models for every section of `base.yaml`. Load order: `base.yaml` →
`<env>.yaml` deep-merge → environment variables (`VOLTSTREAM__SECTION__KEY`) → `.env`. Expose
one cached `get_config()`. **No other module may read YAML or `os.environ` directly.**
**Done when:** `get_config().tariff.blocks[0].up_to_kwh == 60`, and an unknown key in a YAML
file raises a validation error rather than being silently ignored.

### T022 — Unit tests for `config.py`
**Files:** `tests/unit/test_config.py` (new)
**Do:** Deep-merge precedence, env-var override, unknown-key rejection, and that
`get_config()` is cached (same object identity on repeat call).
**Done when:** `pytest tests/unit/test_config.py` passes.

### T023 — `simclock.py`: the simulated clock
**Files:** `src/voltstream/simclock.py`
**Do:** The **single** source of truth for simulated time (§3.4). Provide `sim_now()`,
`real_to_sim(dt)`, `sim_to_real(dt)`, `sim_duration_to_real(td)`, `real_duration_to_sim(td)`,
`sim_date_of(event_ts)`, `sim_hour_of(event_ts)`, and `SIM_EPOCH` — the fixed real instant
simulated time is anchored to, read from config so runs are reproducible. All timestamps
timezone-aware UTC.
**Done when:** No other module does `time_scale` arithmetic:
`grep -rn "time_scale" src/ | grep -vE "simclock.py|config.py"` returns nothing.

### T024 — Unit tests for `simclock.py`
**Files:** `tests/unit/test_simclock.py`
**Do:** Round-trip (`sim_to_real(real_to_sim(t)) == t`); the documented anchors (5 real
minutes == 1 simulated day; 2 real seconds == 9.6 simulated minutes); `sim_date_of` behaviour
either side of simulated midnight; everything tz-aware.
**Done when:** Passes, including an exact assertion on the 288 scale factor.

### T025 — `logging_setup.py`: structured JSON logging
**Files:** `src/voltstream/logging_setup.py`
**Do:** `get_logger(service: str)` returning a logger that emits one JSON object per line with
the §10.1 envelope — `ts, level, service, stage, trace_id, sim_date, msg` — plus arbitrary
extras merged at the top level, not nested. Back `trace_id` with a `contextvars` variable so
it propagates without threading it through every signature. Level from config.
**Done when:** A single emitted line is valid JSON containing every envelope field, verifiable
with `| python -m json.tool`.

### T026 — Unit tests for `logging_setup.py`
**Files:** `tests/unit/test_logging_setup.py` (new)
**Do:** Capture stdout; assert the line parses as JSON; assert all envelope keys present;
assert a `trace_id` set via the context var appears; assert extras are merged, not nested.
**Done when:** Test passes.

### T027 — `metrics.py`: the Prometheus registry
**Files:** `src/voltstream/metrics.py`
**Do:** Define **exactly** the eight metrics in §10.1, with their stated types and labels:
`voltstream_events_produced_total`, `voltstream_events_consumed_total`,
`voltstream_records_rejected_total`, `voltstream_e2e_latency_seconds`,
`voltstream_consumer_lag`, `voltstream_zone_renewable_ratio`,
`voltstream_batch_duration_seconds`, `voltstream_lambda_divergence`. Provide
`start_metrics_server(port)` for non-HTTP services (producer, Spark drivers) and expose the
registry for FastAPI. Set histogram buckets explicitly — the library defaults are wrong for a
sub-60-second latency target.
**Done when:** `curl localhost:<port>/metrics` on a scratch process lists all eight names with
`# HELP` and `# TYPE` lines.

### T028 — Unit tests for `metrics.py`
**Files:** `tests/unit/test_metrics.py` (new)
**Do:** Assert all eight names exist; assert label sets match §10.1; assert importing the
module twice does not raise `Duplicated timeseries in CollectorRegistry` (a real hazard once
Spark re-imports it per executor process).
**Done when:** Test passes.

---

## Phase 2 — Data contracts (freeze before anything reads them)

> §6.2: "Contracts are **frozen at the end of Phase 1** and changed only by explicit
> agreement, because three people build against them in parallel."

### T029 — `contracts/events.py`
**Files:** `src/voltstream/contracts/events.py`
**Do:** Pydantic v2 `MeterReading` with every field in §6.2 and correct types — `event_ts` as
a tz-aware `datetime`; kWh fields as **`Decimal`** with `max_digits=12, decimal_places=4`
(D5); `voltage` as `float` (unused by billing). Serialise `Decimal` fields to JSON as
**numbers**, not strings, via a `field_serializer`, so the §6.2 sample stays valid and Spark's
`from_json` parses the exact decimal text. `model_config = ConfigDict(extra="forbid")`. Add
`from_kafka_value(bytes)` and `to_kafka_value() -> bytes`.
**Done when:** The §6.2 sample JSON round-trips with `consumption_kwh == Decimal("0.412")`; a
kWh value with 5 decimal places raises; an unknown field raises; a missing required field
raises.

### T030 — Decide and document the latency timestamp
**Files:** `src/voltstream/contracts/events.py`, `docs/architecture/05-open-decisions.md`
**Do:** `voltstream_e2e_latency_seconds` needs a **wall-clock** origin, but every timestamp in
the contract is *simulated* — measuring latency from `event_ts` would produce numbers scaled
by 288 and be meaningless. Do not add a field to the frozen contract. Instead use the Kafka
record timestamp: the producer sets it at send time and Spark's Kafka source exposes it as the
`timestamp` column. Additionally set a `produced_at` Kafka **header** (RFC3339 wall clock)
alongside `trace_id`, so the archiver can persist it into Parquet.
**Done when:** The decision is written down, and `events.py` carries a docstring stating that
`event_ts` is simulated and MUST NOT be used for latency.

### T031 — `contracts/reference.py`
**Files:** `src/voltstream/contracts/reference.py`
**Do:** `TariffRecord` and `WeatherForecast` matching the **final** CSV columns agreed in
`T003`. Validators: `effective_date` parses as a date; `subsidy_flag` accepts `true/false/1/0`;
rates non-negative; `billing_tier` one of the configured tiers.
**Done when:** The §6.2 sample rows parse; a negative rate raises; an unknown tier raises.

### T032 — Export JSON Schema
**Files:** `src/voltstream/contracts/schemas/*.json`, `scripts/export_schemas.py` (new)
**Do:** A script writing `model_json_schema()` for each contract model into
`contracts/schemas/`. §10.2 notes there is no schema registry; these exported files are the
honest substitute and are worth pointing at in the viva.
**Done when:** `python scripts/export_schemas.py` writes the schema files, and re-running
produces no diff.

### T033 — Contract drift guard
**Files:** `tests/unit/test_contracts.py` (new)
**Do:** A test that regenerates the JSON Schema in memory and asserts equality with the
committed file. Changing a contract then becomes a deliberate act (regenerate + commit) rather
than an accident — which is what "frozen" has to mean mechanically.
**Done when:** Test passes; mutating any model field makes it fail.

### T034 — `docs/architecture/03-data-contracts.md`
**Files:** `docs/architecture/03-data-contracts.md` (new)
**Do:** Extract §6.2. Include the final field tables, the Kafka key and header decisions
(§3.3d and `T030`), the CSV column lists as resolved by `T003`, and an explicit **FROZEN**
banner with the date.
**Done when:** Committed; the CSV columns in it match `reference.py` exactly.

---

## Phase 3 — Infrastructure: tiers 1 and 2

> §8.2. Get the stack booting before writing anything that depends on it. §9 Gate 1: this is
> the single biggest time sink in the project and is worth **zero marks** — which is exactly
> why it must not be left late.

### T035 — Postgres schema: speed-view tables
**Files:** `docker/init/postgres/01_schema.sql`
**Do:** `zone_metrics_rt` exactly as §6.4. `household_running_rt` as §6.4 **plus the D4
columns** so a provisional response has the same shape as a final one:
`self_consumed_kwh NUMERIC(12,4)`, `energy_charge`, `fixed_charge`, `subsidy_discount`,
`export_credit` (all `NUMERIC(12,2)`), `tier_breakdown JSONB` — all `NOT NULL`.
`estimated_bill` remains the total.
**Done when:** `psql -f` against an empty database succeeds, and re-running it is idempotent
(`CREATE TABLE IF NOT EXISTS`).

### T036 — Postgres schema: batch-view table
**Files:** `docker/init/postgres/01_schema.sql`
**Do:** Append `household_bill_daily` as §6.4.
**Done when:** Applies cleanly; `tier_breakdown` accepts a JSONB object.

### T037 — Postgres schema: the missing `zone_metrics_daily`
**Files:** `docker/init/postgres/01_schema.sql`, `docs/architecture/00-master-design.md`
**Do:** §6.1's diagram lists `zone_metrics_daily` as a batch-view table, but **§6.4 never
defines it.** Define it now, because `daily_zone_rollup.py` (`T124`) writes to it. Suggested
columns: `grid_zone, sim_date, total_consumption_kwh, total_solar_kwh, self_consumed_kwh,
export_kwh, renewable_ratio, peak_window_start, peak_consumption_kwh, active_meters,
readings_count, pipeline_run_id, computed_at`; PK `(grid_zone, sim_date)`.
**Done when:** Table created, and §6.4 in the design document updated to match — the document
states "if the code and this document disagree, one of them is a bug."

### T038 — Postgres schema: the missing `households` dimension
**Files:** `docker/init/postgres/01_schema.sql`, `docs/architecture/00-master-design.md`
**Do:** `03_seed_households.sql` implies a `households` table that §6.4 never defines. Define
it: `household_id PK, meter_id, grid_zone, billing_tier, subsidy_flag, has_solar, created_at`.
This is also the known-household set that `core/validation.py` checks
`faults.unknown_household_rate` events against.
**Done when:** Table created; §6.4 updated to match.

### T039 — Postgres schema: operational tables
**Files:** `docker/init/postgres/01_schema.sql`
**Do:** `rejected_records` and `pipeline_runs` as §6.4. `reconciliation_daily` as §6.4 **plus
the D4 decomposition columns** `tariff_effect NUMERIC(12,2) NOT NULL` and
`data_effect NUMERIC(12,2) NOT NULL` (invariant: `speed_estimate − batch_final ==
tariff_effect + data_effect`).
**Done when:** All three created.

### T040 — Handle the `pipeline_runs` unique-index trap
**Files:** `docker/init/postgres/01_schema.sql`, `docs/architecture/05-open-decisions.md`
**Do:** `idx_runs_date_layer_success` is `UNIQUE (sim_date, layer) WHERE status = 'success'`.
**A backfill re-run of an already-successful day will violate it and the DAG will fail** —
which breaks the restatement demo that the entire architecture chapter rests on (§5.6). Fix it
now: add a `superseded` status, and have the batch job — in the same transaction as its own
success insert — run
`UPDATE pipeline_runs SET status='superseded' WHERE sim_date=? AND layer=? AND status='success'`.
Record the rule in the decision log.
**Done when:** A scripted double-run of the same `(sim_date, layer)` produces two rows — one
`superseded`, one `success` — with no index violation.

### T041 — Postgres indexes
**Files:** `docker/init/postgres/02_indexes.sql`
**Do:** The indexes named in §6.4, plus those the read paths actually need:
`household_bill_daily (sim_date)`, `reconciliation_daily (sim_date)`,
`pipeline_runs (sim_date, layer, status)`, `household_running_rt (sim_date)`.
**Done when:** `EXPLAIN` on the merge-function lookup and on `/zones/load` both show index
scans, not sequential scans.

### T042 — Seed households
**Files:** `docker/init/postgres/03_seed_households.sql`
**Do:** Generate 50 rows, `HH-0001`–`HH-0050` with meters `MTR-0001`–`MTR-0050`, distributed
across the five configured zones, with a realistic spread of `billing_tier`, `subsidy_flag` and
`has_solar`. **This distribution must match what `profiles.py` and `reference_dropper.py`
assume** — derive all three from one stated rule and put that rule in a comment.
**Done when:** `SELECT grid_zone, count(*) FROM households GROUP BY 1` returns 5 zones summing
to 50.

### T043 — Compose: tier 1 infrastructure
**Files:** `docker/docker-compose.yml`
**Do:** `kafka` (KRaft mode, single broker — no ZooKeeper), `postgres`, `minio`. Real
**healthchecks**, not sleeps: `kafka-broker-api-versions` for Kafka, `pg_isready` for Postgres,
the `/minio/health/live` endpoint for MinIO. Named volumes for all three. A project network.
`name: voltstream` at the top of the file — per D7 this is **mandatory**: Compose otherwise
derives the project name from the folder holding the compose file (`docker/`), and the
repository folder is `volstream`; this key is what makes every network, volume and container
come out `voltstream-*`.
**Done when:** `docker compose up -d kafka postgres minio` reaches `healthy` on all three,
confirmed by `docker compose ps`; `docker compose ls` shows project `voltstream`; nothing in
`docker network ls` / `docker volume ls` is named `docker_*` or `volstream_*`.

### T044 — Compose: named volumes for Spark checkpoints
**Files:** `docker/docker-compose.yml`
**Do:** §8.5: "Put checkpoints on named Docker volumes, never on a bind mount into `/tmp`."
Declare `speed_checkpoints` and `archiver_checkpoints` now, before the jobs exist, so nobody
bind-mounts them later out of convenience.
**Done when:** `docker volume ls` shows both after `compose up`.

### T045 — Bootstrap: `postgres-init`
**Files:** `docker/docker-compose.yml`
**Do:** A tier-2 one-shot service applying `01`, `02`, `03` in order, then exiting 0.
`depends_on: postgres: condition: service_healthy`. Must be **idempotent** — it runs again on
every `compose up` (§8.2: "Tier 2 is the one people forget").
**Done when:** `docker compose up postgres-init` exits 0; running it twice also exits 0 and
does not duplicate seed rows.

### T046 — Bootstrap: `kafka-init`
**Files:** `docker/init/kafka/create_topics.sh`, `docker/docker-compose.yml`
**Do:** Create `meter.readings` (3 partitions, `retention.ms` per config) and
`meter.readings.dlq`. Use `--if-not-exists`. Exit 0. LF line endings (`T009`).
**Done when:** `kafka-topics --list` shows both with the correct partition count.

### T047 — Bootstrap: `minio-init`
**Files:** `docker/init/minio/create_buckets.sh`, `docker/docker-compose.yml`
**Do:** Create the four buckets from §6.3: `voltstream-raw`, `voltstream-landing`,
`voltstream-archive`, `voltstream-checkpoints`. Idempotent (`mc mb --ignore-existing`).
**Done when:** `mc ls local/` lists all four; re-running exits 0.

### T048 — `app.Dockerfile`
**Files:** `docker/images/app.Dockerfile`
**Do:** Slim Python base at the pinned version; `pip install -e ".[api,sim]"`; non-root user;
multi-stage so the final image carries no build toolchain. **Must not** contain PySpark (§7.3:
"the API image does not ship a 300 MB Spark distribution").
**Done when:** `docker build` succeeds;
`docker run --rm <img> python -c "import voltstream"` works; and
`docker run --rm <img> python -c "import pyspark"` **fails** — that failure is the proof the
image split is real.

### T049 — `spark.Dockerfile`, with the right JARs
**Files:** `docker/images/spark.Dockerfile`
**Do:** Spark base image at the version pinned in `T002`, plus `pip install -e ".[spark]"`.
Bake the connector JARs into the image rather than resolving them at runtime — `--packages` on
every job start is slow and fails without network:
- `spark-sql-kafka-0-10`, matching the Spark version exactly.
- `hadoop-aws` + `aws-java-sdk-bundle` — **these must match the Hadoop version bundled with
  your Spark build.** A mismatch produces `NoSuchMethodError` at runtime and is a classic
  multi-hour sink. **Measured in T002:** PySpark 3.5.9 bundles Hadoop **3.3.4**, Scala
  **2.12** → `hadoop-aws:3.3.4`, `spark-sql-kafka-0-10_2.12:3.5.9`, and the
  `aws-java-sdk-bundle` version declared in `hadoop-aws:3.3.4`'s POM (expected 1.12.262 —
  confirm against the POM). Re-verify with
  `sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()` inside the built image, since the
  Docker base image may not bundle the same Hadoop as the pip wheel.
- The PostgreSQL JDBC driver.
**Done when:** `spark-submit --version` works inside the image, and a scratch job that reads
Kafka, reads `s3a://`, and opens a JDBC connection starts with no `ClassNotFoundException`.

### T050 — GATE 1 · infrastructure smoke test
**Files:** `scripts/smoke_test.sh`
**Do:** A script that brings up tiers 1–2, waits on **health conditions** (not `sleep`), then
asserts: three containers healthy, three init jobs exited 0, both topics present, four buckets
present, all Postgres tables present, 50 seeded households. Print a pass/fail summary; exit
non-zero on failure.
**Done when:** From a destroyed state, `bash scripts/smoke_test.sh` passes in roughly the
60–90 s §8.2 predicts.

> **GATE 1.** Compose is green from cold, the schema applies, topics and buckets exist. Per
> §9, if this is not achievable, cut Grafana now and keep Prometheus alone.

---

## Phase 4 — `core/`: the shared transformation module

> §4.4 and §7.3: the single most important structural decision in the repository. Pure
> functions only — no I/O, no clock reads, no database calls, and no config lookups *inside*
> the functions (pass config in as arguments, so tests can vary it).

### T051 — `core/keys.py`
**Files:** `src/voltstream/core/keys.py`
**Do:** `dedup_key(reading)` returning `(meter_id, event_ts)` (§3.3d), `partition_key(reading)`
returning `household_id`, and the Parquet partition helper for `(sim_date, hour)`. One place,
so the streaming and batch jobs cannot disagree about what a duplicate is.
**Done when:** `tests/unit/test_keys.py` passes.

### T052 — Unit tests for `core/keys.py`
**Files:** `tests/unit/test_keys.py` (new)
**Do:** Two readings differing only in `event_id` share a dedup key; two differing in
`event_ts` do not; the partition key equals `household_id`.
**Done when:** Test passes.

### T053 — `core/validation.py`
**Files:** `src/voltstream/core/validation.py`
**Do:** Pure rules returning a structured result (`valid: bool`, `reason: str | None`), where
`reason` comes from a fixed vocabulary held in **one** constant — the same vocabulary used by
`rejected_records.reason` and the `voltstream_records_rejected_total{reason=…}` label. Rules:
required fields non-null; `consumption_kwh >= 0`; `solar_generation_kwh >= 0`; `household_id`
in the known set; `grid_zone` in the configured zones; `event_ts` inside a sane window;
`voltage` within bounds (warn, do not reject — §6.2 marks it deliberately unused by billing).
**Done when:** Every `faults.*` rate in `base.yaml` maps to exactly one rejection reason, and
the constant is the only place those strings appear.

### T054 — Unit tests for `core/validation.py`
**Files:** `tests/unit/test_validation.py`
**Do:** One test per rule, each asserting the exact `reason` string, plus one test that a clean
reading passes every rule.
**Done when:** Passes, with one case per reason in the vocabulary.

### T055 — `core/money.py` and `core/netting.py`
**Files:** `src/voltstream/core/money.py` (new, per D5), `src/voltstream/core/netting.py`
**Do:** First `money.py`: the precision/scale constants as plain ints — `MONEY = (12, 2)`,
`KWH = (12, 4)`, `PCT = (5, 2)` — plus `round_money(Decimal) -> Decimal` (`ROUND_HALF_UP`,
2 dp) and `quantize_kwh(Decimal) -> Decimal` (4 dp). **No pyspark import**; `spark_expr.py`
builds its `DecimalType`s from these same ints. Then `netting.py`, exactly §3.3c, over
`Decimal`:
```
self_consumed   = min(solar_kwh, consumption_kwh)
billable_import = consumption_kwh - self_consumed
export_kwh      = solar_kwh - self_consumed
```
Return a frozen dataclass or `NamedTuple`, not a dict — it feeds `tariff.py` and should be
type-checked. No rounding in netting: ≤ 4 dp in, ≤ 4 dp out, exactly.
**Done when:** `mypy --strict` clean on both modules; `round_money(Decimal("0.165")) ==
Decimal("0.17")`.

### T056 — Unit tests for `core/netting.py`
**Files:** `tests/unit/test_netting.py`
**Do:** Solar > consumption; solar < consumption; solar == consumption; both zero; consumption
zero with solar positive (pure export). Assert both §9 Phase 2 invariants in every case.
**Done when:** Test passes.

### T057 — `core/tariff.py`: block-tariff energy charge
**Files:** `src/voltstream/core/tariff.py`
**Do:** `energy_charge(billable_import_kwh, blocks) -> (total, breakdown)` implementing the
**marginal/slab** reading fixed in `T006`: the first 60 kWh at the household's `block_1_rate`,
the next 60 at `block_2_rate`, the remainder at `block_3_rate`. Per D2, add
`build_blocks(boundaries, tariff_record) -> list[Block]`, which zips the config boundaries with
the record's three rates; that is the only place the two are combined. `blocks` is passed in,
never read from config inside `energy_charge`. The breakdown is the per-block
`{up_to, rate, kwh, charge}` list that becomes `household_bill_daily.tier_breakdown`.
**Done when:** Hand-computed values for 0, 59.99, 60, 60.01, 120 and 200 kWh all match at the
default rates (8.00 / 16.50 / 24.50), **and**
`grep -rn "generator_defaults" src/voltstream/core src/voltstream/streaming src/voltstream/batch src/voltstream/api`
returns nothing.

### T058 — `core/tariff.py`: full bill assembly
**Files:** `src/voltstream/core/tariff.py`
**Do:** `compute_bill(netting_result, tariff_record, boundaries) -> BillBreakdown`, applying
the D5 formula in order: per-block line items (each `round_money`'d) → `energy_charge` as the
exact sum of the rounded lines → `fixed_charge` → `subsidy_discount` on the energy charge only
→ `export_credit` → `final_bill` as the exact sum/difference of the four rounded components,
**never clamped**. Every intermediate is a `Decimal` field — `household_bill_daily` and
`household_running_rt` store them all. All rounding goes through `core/money.py`.
**Done when:** The D5 boundary-tie example (60.0100 kWh → energy `480.17`, final `720.17`)
is the docstring doctest and passes; `final_bill == energy_charge + fixed_charge −
subsidy_discount − export_credit` is an exact `Decimal` equality.

### T059 — Unit tests for `core/tariff.py`
**Files:** `tests/unit/test_tariff.py`
**Do:** Boundaries at exactly 60 and 120, from both sides — this is §4.4's worked failure
example ("the speed layer believes the first block ends at 60 kWh and the batch layer believes
61"), so it gets an explicit test. Plus: subsidised vs unsubsidised; each of the three
fixed-charge tiers; zero consumption; export-only household. Then the **three D5 worked
examples as exact-equality assertions** (boundary tie → `720.17`; typical subsidised solar →
`454.41`; net exporter → `−150.00`), and the line-item rounding case (two half-cent lines →
`0.34`, not `0.33`).
**Done when:** Passes, and deliberately flipping a `>` to `>=` in the block logic makes at
least one test fail; deliberately rounding the sum instead of the lines makes the `0.34` test
fail.

### T060 — Property tests: tariff invariants
**Files:** `tests/property/test_tariff_invariants.py`
**Do:** `hypothesis` strategies generating **`Decimal`** inputs — kWh with ≤ 4 dp, rates and
fixed charge with ≤ 2 dp, `subsidy_pct` in `[0, 100]` (D2/D5) — asserting the four properties
§9 Phase 2 names, plus two D5 additions:
- **Monotonicity** — more consumption never lowers the bill; **and** more solar never raises
  it (holds because every rate is `≥ 0` and `subsidy_pct ≤ 100`).
- **Continuity** — no discontinuous jump at a block boundary; assert the bill is ε-continuous
  across `up_to_kwh ± δ` (ε = one cent, since line items are rounded).
- **Netting invariants** — `self_consumed + export == solar` and
  `self_consumed + billable_import == consumption`, exactly.
- **Non-negativity** — no computed *component* is ever negative. (`final_bill` itself may be,
  per D5.)
- **Row invariants** — `final_bill == energy + fixed − subsidy − export` exactly, and the
  breakdown's block charges sum to `energy_charge` exactly.
**Done when:** ≥ 1,000 examples pass, with `@example()` cases pinned at every block boundary
and at the three D5 worked examples.

### T061 — `core/spark_expr.py`: netting as Column expressions
**Files:** `src/voltstream/core/spark_expr.py`
**Do:** The same netting logic as `Column` expressions (`F.least`, arithmetic). **No UDFs** —
§4.4 requires Catalyst-optimisable expressions, "to preserve Catalyst optimisation and avoid
per-row serialisation cost."
**Done when:** `grep -n "udf" src/voltstream/core/spark_expr.py` returns nothing.

### T062 — `core/spark_expr.py`: tariff as Column expressions
**Files:** `src/voltstream/core/spark_expr.py`
**Do:** The block tariff as nested `F.when` / `F.greatest` / `F.least` arithmetic, **generated
programmatically from the same `blocks` structure `tariff.py` consumes**, so the two cannot
drift structurally even before the consistency test catches them numerically. Per D5,
**everything is `DecimalType`**: build the types from `core/money.py`'s ints; every literal is
`F.lit(Decimal("...")).cast(DecimalType(...))` — a single bare float (`* 0.01`, `F.lit(60.0)`)
silently promotes the whole expression to `DoubleType`; round each block charge with
`F.round(c, 2)` (half-up — never `F.bround`, which is half-even) **before** summing, and cast
each stored component to its target `DecimalType`. Build `tier_breakdown` with
`F.to_json(F.struct(...))`.
**Done when:** Still no UDFs; `df.explain()` shows the arithmetic inlined in the physical plan
rather than a `BatchEvalPython` node; `grep -nE "\b[0-9]+\.[0-9]+\b"
src/voltstream/core/spark_expr.py` finds no float literals; and the output schema for every
kWh/money column is `DecimalType`.

### T063 — The consistency test (the centrepiece)
**Files:** `tests/consistency/test_pure_vs_spark.py`
**Do:** Generate ≥ 5,000 random rows of `Decimal` inputs — consumption, solar, and a full
D2 `TariffRecord` (three block rates, fixed charge, `subsidy_flag`, `subsidy_pct`,
`export_rate`) — with a fixed seed, **including values that straddle every `up_to_kwh`
boundary and deliberate half-cent tie cases** (products ending in exactly `…5` at the third
decimal — the case that separates float from decimal). Run `core/tariff.py` row-wise and
`core/spark_expr.py` over a DataFrame. Assert **exact `==`** on every output component — D5's
rounding rules make both sides deterministic, so no tolerance. Assert the Spark output
**schema** is `DecimalType` for every kWh/money column (this catches a stray float literal
even when the values happen to agree). Compare `tier_breakdown` **numerically per field**
after parsing both JSON strings — Spark's `to_json` writes `480.00`, Python writes `480.0`;
textual comparison would spuriously fail.
**Done when:** Passes over ≥ 5,000 inputs; changing one block boundary in `spark_expr.py`
alone makes it fail; replacing one `Decimal` literal in `spark_expr.py` with a float makes it
fail. **This is the §4.4 viva answer — "we made drift a test failure" — so it must be
demonstrably fragile in exactly those ways.**

### T064 — Session-scoped Spark fixture
**Files:** `tests/conftest.py`
**Do:** A session-scoped local `SparkSession` (`local[2]`, `spark.ui.enabled=false`, shuffle
partitions 1) shared by the consistency and Spark tests, so the run creates one JVM rather than
one per test. **Before building the session**, set
`os.environ.setdefault("PYSPARK_PYTHON", sys.executable)` and likewise
`PYSPARK_DRIVER_PYTHON` — T002 found that on Windows, an unset `PYSPARK_PYTHON` makes Spark
launch workers via the Microsoft Store `python` alias stub, and every task fails with
`Python worker failed to connect back`. Harmless on Linux. Add a comment noting that local
Spark may need `HADOOP_HOME`/`winutils.exe` for anything touching the local filesystem — keep
these tests purely in-memory to sidestep it.
**Done when:** A full `pytest` run creates exactly one `SparkSession` and finishes in a
tolerable time (target < ~2 min).

---

## Phase 5 — Simulators

### T065 — `simulators/profiles.py`: load curve
**Files:** `src/voltstream/simulators/profiles.py`
**Do:** Deterministic consumption as a function of simulated hour-of-day, with a morning and an
evening peak, per-household scaling, and Gaussian noise. Seeded RNG per household so runs are
reproducible (§2.5 lists reproducibility as explicitly graded).
**Done when:** 144 points for one household show the expected double-peak shape, and the same
seed produces identical output twice.

### T066 — `simulators/profiles.py`: solar curve
**Files:** `src/voltstream/simulators/profiles.py`
**Do:** Bell curve centred on simulated midday, zero at night, scaled by a per-zone cloud-cover
factor so the weather file has a real effect. Not every household has solar — drive that from
`households.has_solar` (`T038`) so the database, the simulator and the tariff file agree.
**Done when:** Solar is exactly 0 outside daylight hours, and renewable ratio differs between
zones.

### T067 — Unit tests for `profiles.py`
**Files:** `tests/unit/test_profiles.py` (new)
**Do:** Determinism under a fixed seed; solar zero at night; consumption strictly positive;
peaks at the expected hours; output ranges sane.
**Done when:** Test passes.

### T068 — Renewable-ratio range check
**Files:** `tests/unit/test_profiles.py`
**Do:** Simulate one full day for all 50 households and assert the resulting per-zone renewable
ratio spans a range that will actually cross `alerts.low_renewable_threshold: 0.15` — below it
at night, above it at midday. **If the curves never cross the threshold, the
`LowRenewableContribution` alert can never be demonstrated firing** — and §9 Phase 4 is explicit
that demonstrated firing is what the ten observability marks turn on.
**Done when:** The test asserts at least one window below and one above the threshold, per zone.

### T069 — `simulators/faults.py`: the injection framework
**Files:** `src/voltstream/simulators/faults.py`
**Do:** A `FaultInjector` taking the `faults` config block and a seeded RNG, exposing
`apply(reading) -> list[MeterReading]` (a list, because duplication emits more than one) and
`should_drop_out()`. Every rate configurable, and every rate settable to 0 for clean runs.
**Done when:** With all rates at 0, output equals input exactly — one record in, one record out.

### T070 — `simulators/faults.py`: the six fault types
**Files:** `src/voltstream/simulators/faults.py`
**Do:** Implement each fault §9 Phase 1 requires — commit them one at a time if you prefer
smaller diffs: duplicates (same `event_id` re-sent), out-of-order (`event_ts` shifted backwards
by U[1, 30] sim min per D3), nulls in required fields, negative kWh, unknown `household_id`,
and meter dropouts. Per D3, a dropout is a **per-meter state machine**: on trigger
(`dropout_probability_per_meter_tick`), the meter buffers its readings for
`dropout_duration_real_seconds`; on expiry it flushes the whole buffer in one tick with the
**original** `event_ts` values (store-and-forward). `dropout_backfill: false` discards the
buffer instead — keep the switch for contrasting the two behaviours in the demo. §9: "Fault
injection is not optional… it earns marks in three rubric rows at once."
**Done when:** Each *rejectable* fault type maps to exactly one `T053` rejection reason,
verified by a test that injects each fault and runs validation over the result. Out-of-order
and backfilled readings are **valid** records (they are late, not wrong) and must pass
validation — assert that too. A backfill after a 30 real-s dropout emits exactly 15 readings
whose `event_ts` are unchanged.

### T071 — Unit tests for `faults.py`
**Files:** `tests/unit/test_faults.py` (new)
**Do:** Over 10,000 seeded samples, assert each observed injection rate is within tolerance of
its configured rate; assert out-of-order shifts fall inside the `T004` spread; assert dropout
duration is honoured.
**Done when:** Passes deterministically — fixed seed, no flakes.

### T072 — `simulators/meter_producer.py`
**Files:** `src/voltstream/simulators/meter_producer.py`
**Do:** The §8.3 loop: read `sim_now()`; compute each household's reading from `profiles`; apply
`faults`; publish to Kafka with **key = `household_id`** (§3.3d) and headers `trace_id` and
`produced_at` (`T030`); sleep `emit_interval_seconds`; repeat. Graceful SIGTERM shutdown that
flushes the producer. One structured log line per tick with counts.
**Done when:** `kafka-console-consumer --property print.key=true` shows correctly keyed JSON
matching the §6.2 contract, arriving every 2 s.

### T073 — Producer metrics
**Files:** `src/voltstream/simulators/meter_producer.py`
**Do:** Increment `voltstream_events_produced_total{producer_id}` and expose `/metrics` via
`start_metrics_server`. Also count injected faults by type, so the demo can show injection rate
and rejection rate side by side — that pairing is what makes the observability story concrete.
**Done when:** `curl producer:<port>/metrics` shows a monotonically rising counter.

### T074 — `simulators/reference_dropper.py`
**Files:** `src/voltstream/simulators/reference_dropper.py`
**Do:** Wake once per simulated day at simulated midnight; generate `tariff_<date>.csv` (the ten
D2 columns, sourcing `billing_tier`/`subsidy_flag` from the `households` dimension and money
from `config.tariff.generator_defaults` — this is the **only** module allowed to read those
defaults) and `weather_<date>.csv`, for all 50 households and 5 zones; write to
`s3a://voltstream-landing/tariff/` and `/weather/`; sleep. **Write to a temporary key and copy
to the final key**, so the Airflow sensor can never observe a partial file. Apply a
**deterministic, documented day-over-day change** to at least one monetary column (e.g.
`block_2_rate` steps by a fixed amount on alternate simulated days), so the speed layer's
stale-tariff divergence (§3.1) is non-zero and attributable in `T140`. State the rule in the
module docstring.
**Done when:** After one simulated day, both files exist in MinIO and parse cleanly with
`TariffRecord` / `WeatherForecast`; after two, the two tariff files differ in the documented
column and nothing else.

### T075 — Seed a day-zero tariff
**Files:** `src/voltstream/simulators/reference_dropper.py` or `docker/init/minio/`
**Do:** The speed layer's provisional estimate costs against **yesterday's** tariff (§3.1). On
the first simulated day there is no yesterday, so `household_running_rt.estimated_bill` cannot
be computed and the merge demo has nothing to show on day one. Drop a `day −1` tariff file at
bootstrap.
**Done when:** The speed layer produces a non-null `estimated_bill` on the first simulated day
without the job special-casing it.

### T076 — Compose: `meter-producer` and `reference-dropper`
**Files:** `docker/docker-compose.yml`
**Do:** Both as tier-3 services on the `app` image; `depends_on` Kafka/MinIO healthy and the
init jobs completed successfully; restart policy `unless-stopped`.
**Done when:** `docker compose up -d` runs both; producer logs show ticks; after ~5 real
minutes the first tariff file appears in MinIO.

---

## Phase 6 — Raw archiver and the master dataset

### T077 — `streaming/session.py`
**Files:** `src/voltstream/streaming/session.py`
**Do:** A `SparkSession` builder reading everything from config: S3A endpoint, **path-style
access** (required for MinIO — virtual-host addressing will not resolve), credentials,
`spark.sql.session.timeZone = UTC`, shuffle partitions sized for a single node, checkpoint base
path. One builder used by all four Spark entrypoints.
**Done when:** A scratch job using only this builder reads and writes `s3a://` against MinIO.

### T078 — `streaming/sources.py`: Kafka read and deserialise
**Files:** `src/voltstream/streaming/sources.py`
**Do:** `read_meter_stream(spark)` returning a DataFrame with the §6.2 schema **declared
explicitly** — never `inferSchema` on a stream — with kWh fields as `DecimalType(12,4)` per
D5 (`voltage` stays `DoubleType`), plus the Kafka metadata columns `key`, `timestamp`,
`partition`, `offset`, and headers. Set `includeHeaders=true`: it is **off by default**, and
`T030`'s latency measurement and `trace_id` propagation both depend on it.
**Done when:** `printSchema()` shows every contract field with the right type
(`decimal(12,4)` for kWh) plus the metadata columns, and `trace_id` is extractable from
headers.

### T079 — `streaming/sources.py`: validation split
**Files:** `src/voltstream/streaming/sources.py`
**Do:** `split_valid_invalid(df)` applying the `core/` validation rules as Column expressions
and returning `(valid_df, invalid_df)`, where `invalid_df` carries a `reason` column from the
`T053` vocabulary and the original payload as JSON.
**Done when:** A unit test over a static DataFrame routes one row per reason into `invalid_df`
with the correct label.

### T080 — `streaming/raw_archiver.py`
**Files:** `src/voltstream/streaming/raw_archiver.py`
**Do:** §8.3: read the stream, add an ingest timestamp and the derived `sim_date` / `hour`
partition columns, write Parquet to `s3a://voltstream-raw/meter_readings/sim_date=…/hour=…/`.
**No transformation, no validation, no filtering** — the master dataset is raw by definition
(§5.4), and filtering here would destroy the bug-recovery property that justifies its existence.
Checkpoint to the named volume from `T044`. Snappy compression.
**Done when:** Files appear under correctly named partition directories, and
`spark.read.parquet(...)` round-trips the §6.2 schema with real types, not strings.

### T081 — Archiver metrics and logging
**Files:** `src/voltstream/streaming/raw_archiver.py`
**Do:** Per micro-batch: emit the §10.1 log envelope with `rows_in`/`rows_out`; increment
`voltstream_events_consumed_total{layer="archiver"}`; observe
`voltstream_e2e_latency_seconds{layer="archiver"}` as `now − kafka_timestamp` (**wall clock**,
per `T030`); set `voltstream_consumer_lag`. §5.7's honest limitation applies and should be
noted in the code: these come from the driver inside `foreachBatch`, not scraped from executors.
**Done when:** Metrics appear on the driver's metrics port, and latency values are plausible
seconds — values in the thousands mean simulated time leaked into the calculation.

### T082 — Compose: `raw-archiver`
**Files:** `docker/docker-compose.yml`
**Do:** Tier-3 service on the Spark image, checkpoint volume mounted, metrics port exposed.
**Done when:** `docker compose up -d raw-archiver` runs and Parquet grows continuously.

### T083 — GATE 2 · archiver restart test
**Files:** `tests/integration/test_archiver_restart.py` (new) or `scripts/smoke_test.sh`
**Do:** §9 Phase 1's exit criterion. Let the archiver run; record the exact row count and the
set of `event_id`s in Parquet; `docker compose kill raw-archiver`; wait; restart; wait; then
assert no data loss and no offset gap.

One honesty point to settle while writing it: at-least-once delivery means a small number of
duplicate *rows* can legitimately reappear after an unclean kill, because a Spark file sink
commits files and offsets in two steps. Decide whether your assertion is "no duplicates"
(which requires genuinely idempotent writes) or "no loss; duplicates tolerated and removed by
the batch dedup on `(meter_id, event_ts)`" (§3.3d). The latter is the truthful answer for a
file sink — assert that, and say so in the report rather than overclaiming.
**Done when:** The test passes and the documented guarantee matches what the code actually
provides.

> **GATE 2.** Checkpointing is correct. §9: "If checkpointing is wrong, everything built after
> this is built on sand."

### T084 — Measure the small-file problem
**Files:** `docs/assumptions.md`
**Do:** Count the Parquet files produced by one simulated day and their average size. §10.2
lists the small-file problem as a limitation; having the real number makes the report concrete
instead of generic.
**Done when:** The measured file count and average size are written down.

### T085 — `docs/assumptions.md`
**Files:** `docs/assumptions.md` (new)
**Do:** Collect every simplification from §3.3b, §3.4 and §10.2 in one place: interval rather
than cumulative meter readings, the simulated clock, watermark unrealism, 50 households, no
schema registry, single-broker everything, `.env` instead of a secret manager.
**Done when:** Every `> **Limitation:**` block in `00-master-design.md` has a matching entry.

---

## Phase 7 — Speed layer

### T086 — `streaming/sinks.py`: Postgres upsert helper
**Files:** `src/voltstream/streaming/sinks.py`
**Do:** JDBC has no upsert. Implement `upsert_batch(df, table, conflict_cols)` that collects the
micro-batch on the driver and uses `psycopg.execute_values` with
`INSERT … ON CONFLICT (…) DO UPDATE`. At ~500 rows per micro-batch this is correct and simple,
and it is what §10.3 already claims in the report: "one write of ~500 rows per micro-batch, not
500 round-trips." Guard it with a configurable row-count ceiling that logs loudly when exceeded,
so the approach fails visibly rather than silently degrading if volume grows.
**Done when:** A test against a real Postgres upserts 500 rows twice and the table still holds
500 rows, with updated values.

### T087 — `streaming/sinks.py`: rejected-records sink
**Files:** `src/voltstream/streaming/sinks.py`
**Do:** `write_rejected(df, stage)` inserting into `rejected_records` with `raw_payload` as
JSONB **and** producing the same record to `meter.readings.dlq`. Increment
`voltstream_records_rejected_total{layer, reason}`.
**Done when:** An injected bad record appears in both the table and the DLQ topic, carrying the
same `trace_id`.

### T088 — `streaming/speed_layer.py`: windowed zone aggregation
**Files:** `src/voltstream/streaming/speed_layer.py`
**Do:** Read the stream; split valid/invalid; `withWatermark("event_ts", "30 minutes")` built
from `watermark_sim_minutes` — **simulated** minutes, and say so in a comment right there;
`window(event_ts, "15 minutes")` likewise; `groupBy(window, grid_zone)`; aggregate total
consumption, total solar, renewable ratio, and active meter count. **`outputMode("update")`**
— per D3, each micro-batch upserts the windows it touched, so a window is visible ~10–12 real s
after its first event and is revised as absorbed late data arrives; the watermark governs only
when it becomes immutable (~16 real s after it ends), not when it is first seen.
**Done when:** A console sink shows one row per `(zone, window)` with plausible values, windows
advance at the simulated rate, and a window's totals visibly *increase* across two consecutive
micro-batches while it is still open.

### T089 — Speed layer: zone sink
**Files:** `src/voltstream/streaming/speed_layer.py`
**Do:** `foreachBatch` upserting `zone_metrics_rt` on `(grid_zone, window_start)`, at the
configured trigger interval.
**Done when:** `SELECT * FROM zone_metrics_rt ORDER BY window_start DESC LIMIT 5` returns rows
that change every ~10 s.

### T090 — Speed layer: per-household running total
**Files:** `src/voltstream/streaming/speed_layer.py`
**Do:** A second aggregation grouped by `(household_id, window(event_ts, "1 day"))`,
accumulating consumption, solar and the netting outputs via `core/spark_expr.py`. Per D3,
group by a **1-day event-time window**, not by a derived `sim_date` column: the window key is
what lets the same 30-sim-min watermark evict the day's state ~16 real s after simulated
midnight. A derived date column would never be evicted. Derive `sim_date` from `window.start`
for the sink.
**Done when:** `household_running_rt` totals rise monotonically through the simulated day and
reset at simulated midnight; the streaming query's state-store metrics show the previous day's
keys evicted shortly after midnight.

### T091 — Speed layer: provisional bill against yesterday's tariff
**Files:** `src/voltstream/streaming/speed_layer.py`
**Do:** Broadcast-join yesterday's tariff (from `voltstream-archive`, or the landing file for
`sim_date − 1`); compute **every** bill component with `core/spark_expr.py` — the same call the
batch job makes — and persist all of them per D4 (`self_consumed_kwh`, `energy_charge`,
`fixed_charge`, `subsidy_discount`, `export_credit`, `tier_breakdown`, `estimated_bill`); set
`tariff_source_date` to that file's date — **deliberately stale, deliberately labelled** (§3.1).
Reload the broadcast when the simulated day rolls over.
**Done when:** `tariff_source_date == sim_date − 1`, every D4 column is non-null for all 50
households, and `estimated_bill == energy_charge + fixed_charge − subsidy_discount −
export_credit` row by row.

### T092 — Speed layer: metrics
**Files:** `src/voltstream/streaming/speed_layer.py`
**Do:** `voltstream_events_consumed_total{layer="speed"}`,
`voltstream_e2e_latency_seconds{layer="speed"}`, `voltstream_consumer_lag`, and
`voltstream_zone_renewable_ratio{grid_zone}`. The last drives the `LowRenewableContribution`
alert, so set it from the **same** value written to Postgres — a separate calculation is a
future discrepancy between the dashboard and the alert.
**Done when:** `/metrics` on the speed-layer driver shows all four, with one renewable-ratio
series per zone.

### T093 — Speed layer: DLQ wiring
**Files:** `src/voltstream/streaming/speed_layer.py`
**Do:** Route `invalid_df` through `write_rejected`, and confirm the counts reconcile:
`valid + rejected == total consumed`.
**Done when:** A per-micro-batch log line reports `rows_in`, `rows_out`, `rows_rejected`, and
the three numbers are consistent.

### T094 — Verify the watermark actually drops stragglers
**Files:** `tests/integration/test_watermark_behaviour.py` (new)
**Do:** The `T004` decision only pays off if it is observable. Two assertions, per D3:
1. With the default fault config, the speed layer's daily kWh total across all households is
   below the raw Parquet total by **0.25 % – 5 %** (modelled expectation ≈ 1.7 %). That gap is
   the speed-vs-batch delta the whole Lambda demonstration depends on (§3.2).
2. With `dropout_probability_per_meter_tick: 0` (network reordering still on), the gap is
   **exactly zero**. This proves the watermark absorbs all reordering ≤ 30 sim min, and is what
   makes T140's attribution — "the kWh gap is dropped backfill, nothing else" — honest.
**Done when:** Both assertions pass, and the observed percentage from (1) is recorded for the
report.

### T095 — Compose: `speed-layer`
**Files:** `docker/docker-compose.yml`
**Do:** Tier-3 Spark service with its own checkpoint volume, its own metrics port, and — 
critically — **its own consumer group**, distinct from the archiver's. §5.2: independent
consumer groups are "what makes the two-branch Lambda shape possible from a single source."
**Done when:** `kafka-consumer-groups --describe` shows **two** groups on `meter.readings`, both
advancing independently.

### T096 — Prove the consumer groups are independent
**Files:** `scripts/smoke_test.sh`
**Do:** Kill the archiver for 60 s; assert the speed layer's lag does not grow; restart it;
assert it catches up. This is the §8.1 claim — "if the archiver crashes, the speed layer neither
notices nor cares" — turned into evidence.
**Done when:** The assertion passes and the output is capture-worthy for the report.

### T097 — GATE 3 · speed path complete
**Do:** Confirm §9 Phase 2's exit criteria: the consistency test green (`T063` already exceeds
the required 1,000 inputs); `GET /api/v1/zones/load` returning live updating data — deferred to
`T107` if the API is not up yet, in which case verify directly against Postgres; invalid records
in both the DLQ and `rejected_records`, and counted.
**Done when:** All three demonstrated in one run.

> **GATE 3.** Half the pipeline provably works, and the drift mitigation is proven rather than
> asserted.

---

## Phase 8 — Storage layer and API

### T098 — `storage/postgres.py`
**Files:** `src/voltstream/storage/postgres.py`
**Do:** Engine / connection-pool factory from config, a transaction context manager, and
`healthcheck()`. Pool sized for the API; a separate short-lived path for Spark driver writes, so
a stalled streaming job cannot exhaust the API's pool.
**Done when:** `healthcheck()` returns True against the running container, and raises a typed
error when Postgres is down.

### T099 — `storage/objectstore.py`
**Files:** `src/voltstream/storage/objectstore.py`
**Do:** boto3/MinIO client from config, plus the §6.3 path conventions as functions:
`raw_partition_path(sim_date, hour)`, `landing_tariff_key(sim_date)`,
`archive_tariff_path(sim_date)`, `checkpoint_path(job)`. **No string-formatted S3 paths anywhere
else in the codebase.**
**Done when:** `grep -rn "s3a://" src/ | grep -v objectstore.py` returns nothing.

### T100 — `storage/repositories.py`: zone queries
**Files:** `src/voltstream/storage/repositories.py`
**Do:** `get_latest_zone_metrics()` and `get_zone_metrics_range(from, to)`. Parameterised SQL
only. §7.2: no SQL in routers.
**Done when:** `grep -rn "SELECT" src/voltstream/api/` returns nothing.

### T101 — `storage/repositories.py`: bill and run queries
**Files:** `src/voltstream/storage/repositories.py`
**Do:** `get_finalised_bill(household_id, sim_date)`, `get_running_estimate(household_id,
sim_date)`, `is_day_finalised(sim_date)` — which queries `pipeline_runs` for a `success` row,
§6.4's stated purpose for that table — plus `get_reconciliation(sim_date)` and
`get_rejected_summary(window)`.
**Done when:** Each returns a typed object or `None`, never a raw tuple.

### T102 — Integration tests for repositories
**Files:** `tests/integration/test_repositories.py` (new, marked `integration`)
**Do:** Against a real Postgres, insert fixtures and assert each query. Include the
`is_day_finalised` case where a `superseded` row sits alongside a `success` row (`T040`) — the
naive query returns two rows and breaks.
**Done when:** `pytest -m integration tests/integration/test_repositories.py` passes.

### T103 — `api/main.py` skeleton
**Files:** `src/voltstream/api/main.py`
**Do:** FastAPI app; lifespan managing the Postgres pool; `prometheus-fastapi-instrumentator`
mounted at `/metrics`; router registration; OpenAPI title, description and version filled in
properly — §5.8 counts the generated docs page as a graded demo artefact, so it should not say
"FastAPI 0.1.0".
**Done when:** `GET /docs` renders with a real title and description.

### T104 — `api/dependencies.py`
**Files:** `src/voltstream/api/dependencies.py`
**Do:** DI providers for the connection pool, config, and a request-scoped `trace_id` —
generated when absent, echoed in a response header, and bound to the `T025` log context var.
**Done when:** Every request log line carries a `trace_id` matching the response header.

### T105 — `api/routers/health.py`
**Files:** `src/voltstream/api/routers/health.py`
**Do:** §10.1: `GET /health/live` (process up, touching no dependency) and `GET /health/ready`
(Postgres reachable, MinIO reachable; 503 with per-dependency detail when not).
**Done when:** Stopping Postgres turns `/health/ready` to 503 while `/health/live` stays 200.

### T106 — `api/models.py`: response schemas
**Files:** `src/voltstream/api/models.py`
**Do:** Pydantic response models for every endpoint. Per D4, **one** `BillResponse` model
serves both branches of the merge: `household_id`, `sim_date`, `source:
Literal["batch","speed"]`, `provisional: bool`, `tariff_date` (speed → `tariff_source_date`,
batch → `tariff_effective_date`), the five kWh figures, the four charge components,
`tier_breakdown`, `total` (speed → `estimated_bill`, batch → `final_bill`), and batch-only
optionals `readings_count`, `duplicates_removed`, `pipeline_run_id`, `computed_at` (null when
provisional). The `source`/`provisional` labelling is the §3.2 merge contract and the thing
the demo screenshots.
**Done when:** The OpenAPI schema shows the enumerated `source` values, and a provisional and
a final response for the same household differ only in values, never in keys.

### T107 — `api/routers/zones.py`
**Files:** `src/voltstream/api/routers/zones.py`
**Do:** `GET /api/v1/zones/load` returning the latest window per zone, and
`GET /api/v1/zones/{zone}/history?minutes=N`.
**Done when:** Polling every 5 s returns changing data while the speed layer runs — closing the
§9 Phase 2 exit criterion deferred in `T097`.

### T108 — Compose: `api`
**Files:** `docker/docker-compose.yml`
**Do:** Tier-3 service on the `app` image, with a container healthcheck hitting `/health/ready`.
**Done when:** `docker compose ps` shows `api` healthy, and `/docs` is reachable from the host.

### T109 — README skeleton
**Files:** `README.md`
**Do:** Fill the empty file with the §7/§8/§9-derived structure: what this is, an architecture
diagram placeholder, prerequisites, quickstart (`make demo`), a service/port table, and a
"where to look" map from rubric criterion to file. Per D7, one line near the top: *"The
repository is `volstream`; the Python package, Compose project and all runtime names are
`voltstream`."* Clone instructions use the real slug.
**Done when:** Someone who has never seen the repository can start it from the README alone.

### T110 — Makefile
**Files:** `Makefile`
**Do:** Every target in Appendix B: `up`, `demo`, `down`, `clean`, `test`, `test-all`, `lint`,
`logs s=`, `faults`, `backfill d=`. `up` **waits on health conditions**, never `sleep` (§8.2).
Because the graded entry point is `make demo` and you are on Windows, add a README note on
getting `make` (Git Bash plus `choco install make`, or WSL) — and verify it on the machine that
will run the demo.
**Done when:** `make up && make test && make down` works from a clean checkout on that machine.

---

## Phase 9 — Batch layer and orchestration

### T111 — `batch/daily_billing.py`: read the partition
**Files:** `src/voltstream/batch/daily_billing.py`
**Do:** Entrypoint taking `--sim-date`. Read **only** `sim_date=<date>/` from `voltstream-raw`,
and select **only** the columns billing needs. §5.4's column-pruning claim should be true of the
actual code, and `voltage` exists in the contract specifically so you can demonstrate not
reading it.
**Done when:** `df.explain()` shows a pruned `ReadSchema` that excludes `voltage`, and the date
filter appears under `PartitionFilters` rather than as a post-scan `Filter`.

### T112 — `batch/daily_billing.py`: dedup and validate
**Files:** `src/voltstream/batch/daily_billing.py`
**Do:** Deduplicate on `core/keys.dedup_key` = `(meter_id, event_ts)` (§3.3d), keeping the first
by ingest time. Apply the same `core/` validation rules as the speed layer, routing rejects to
`rejected_records` with `stage='batch'`. Record `duplicates_removed` per household for the
`household_bill_daily` column. **No watermark** — §5.3: the batch layer rescans a complete,
closed day.
**Done when:** With `duplicate_rate: 0.02` injected, `duplicates_removed` is ~2 % of
`readings_count`, and the deduped count matches a `SELECT count(DISTINCT …)` over the raw
partition.

### T113 — `batch/daily_billing.py`: reference joins
**Files:** `src/voltstream/batch/daily_billing.py`
**Do:** Read the tariff CSV for the run date from `voltstream-landing` with an **explicit
schema** — `DecimalType(12,2)` for money and rates, `DecimalType(5,2)` for `subsidy_pct`,
never `inferSchema`, which yields `DoubleType` and silently defeats D5; validate against
`TariffRecord`; join on `household_id` with the **effective-dated** rule — §10.2 is explicit
that this is a simple effective-dated join, not full SCD Type 2, so pick the latest row with
`effective_date <= sim_date` and say so. Join weather on `grid_zone`. **Fail the job loudly if
any household has no tariff row**, rather than emitting a null bill.
**Done when:** All 50 households join, and deleting one CSV row fails the job with a message
naming the household.

### T114 — `batch/daily_billing.py`: netting and tariff
**Files:** `src/voltstream/batch/daily_billing.py`
**Do:** Aggregate to per-household daily kWh, then apply `core/spark_expr.py` netting and block
tariff. Build `tier_breakdown` as JSON. **No arithmetic written inline in this file** — every
formula comes from `core/`, because the moment a rate appears here the §4.4 argument is dead.
**Done when:** The file orchestrates only; no tariff arithmetic appears in it.

### T115 — `batch/daily_billing.py`: write and finalise
**Files:** `src/voltstream/batch/daily_billing.py`
**Do:** Insert a `running` row into `pipeline_runs` at start. Write `household_bill_daily` in
**one transaction** — §5.5: "a batch billing run either fully lands or does not; for money, that
matters" — carrying `pipeline_run_id` for lineage. In the same transaction, supersede any prior
success row (`T040`) and insert the `success` row. On exception, write `failed` and re-raise.
**Done when:** A deliberate exception mid-write leaves zero new `household_bill_daily` rows and
exactly one `failed` run row.

### T116 — Batch metrics
**Files:** `src/voltstream/batch/daily_billing.py`
**Do:** `voltstream_batch_duration_seconds{job="daily_billing"}`, plus `rows_in`/`rows_out` on
`pipeline_runs`. **A short-lived job cannot be scraped** — it has exited before Prometheus comes
round. Either push to a Pushgateway or write the values to Postgres and read them from there.
Pick one, and note the constraint in the report; it is a genuine observability point, not an
oversight.
**Done when:** The duration is visible in the chosen backend after a run.

### T117 — Archive the processed reference file
**Files:** `src/voltstream/batch/daily_billing.py`
**Do:** §6.3 defines `voltstream-archive/tariff/sim_date=…/tariff.parquet`. Convert and write
the consumed CSV there. This is what makes a months-later restatement possible even after the
landing zone is cleaned up.
**Done when:** After a run the archive path exists and reads back identical to the CSV.

### T118 — Unit tests for the billing job's pure parts
**Files:** `tests/unit/test_daily_billing.py` (new)
**Do:** Dedup selection, the effective-dated join rule, and the missing-tariff failure — over
small static DataFrames. No Kafka, no MinIO.
**Done when:** Runs in seconds with no containers.

### T119 — `airflow.Dockerfile`
**Files:** `docker/images/airflow.Dockerfile`
**Do:** `apache/airflow:<3.x latest stable>-python3.11` plus **only** the three providers D6
names — `apache-airflow-providers-docker`, `-amazon`, `-postgres` — installed with that
version's constraints file. **No PySpark, no FastAPI, no `voltstream`** — §5.6: "Airflow's
notoriously constrained dependency set never has to coexist with PySpark and FastAPI in one
image." The DAGs read their three tunables from `config/base.yaml` mounted `:ro` with
`yaml.safe_load`. D6 tripwire: if Airflow 3 + `DockerOperator` + proxy is not green within one
working session, switch the base image to 2.11 — nothing in the DAGs is version-specific.
**Done when:** `docker run --rm <img> python -c "import pyspark"` **and** `… "import
voltstream"` both fail; `python -c "import airflow.providers.docker, airflow.providers.amazon,
airflow.providers.postgres"` succeeds; the image is materially smaller than the Spark image.

### T120 — Compose: Airflow and the socket proxy
**Files:** `docker/docker-compose.yml`, `docker/init/postgres/00_airflow_db.sql` (new)
**Do:** Per D6, two tier-4 services. (1) `docker-socket-proxy` (`tecnativa/docker-socket-proxy`)
with `/var/run/docker.sock` mounted **into the proxy only**, `CONTAINERS=1 POST=1 IMAGES=1
NETWORKS=1`, everything else off. (2) `airflow` as **one** container running `airflow
standalone` with `AIRFLOW__CORE__EXECUTOR=LocalExecutor`,
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` → a **separate** `airflow` database on the existing
Postgres instance (created by `00_airflow_db.sql` in `docker-entrypoint-initdb.d`, which runs
on first init only — never the application database), `AIRFLOW_CONN_MINIO_S3` and
`AIRFLOW_CONN_VOLTSTREAM_PG` as env vars (no UI clicking), `AIRFLOW_UID`, `dags/` mounted,
`config/base.yaml` mounted `:ro`. The Docker socket is **not** mounted into Airflow.
**Done when:** The Airflow UI loads and the scheduler heartbeat is current; a scratch DAG with
a `DockerOperator(image="alpine", command="echo ok", docker_url="tcp://docker-socket-proxy:2375",
mount_tmp_dir=False, network_mode=<compose network>)` succeeds; `\l` in Postgres shows
`airflow` and the app database as separate databases.

### T121 — `tariff_watcher_dag.py` and the head of `daily_billing_dag.py`
**Files:** `airflow/dags/tariff_watcher_dag.py` (new, per D6), `airflow/dags/daily_billing_dag.py`
**Do:** Per D6 Design B. **Watcher:** schedule every real minute, `catchup=False`,
`max_active_runs=1`; one `@task` lists `voltstream-landing/tariff/` via the S3 hook, parses
`tariff_(\d{4}-\d{2}-\d{2})\.csv`, and feeds
`TriggerDagRunOperator.partial(trigger_dag_id="daily_billing", skip_when_already_exists=True)
.expand_kwargs([...])` with `trigger_run_id=f"billing__{d}"` and `conf={"sim_date": d}`.
Idempotent by construction. **Billing DAG head:** `schedule=None`,
`params={"sim_date": Param(type="string", format="date")}`, `max_active_runs=1`,
`catchup=False`. First task: `S3KeySensor` on `tariff_{{ params.sim_date }}.csv` against MinIO
(`aws_conn_id` from the env-var connection) — **not** `FileSensor`, which cannot see an
object-store key — `check_fn` size > 0, poke mode, 15 s, `timeout = alerts.batch_sla_minutes
× 60`. Second task: `@task wait_late_data_grace` reads the object's `LastModified` and sleeps
until `LastModified + batch.late_data_grace_real_seconds` (90). Per D3: a meter dropout that
straddles simulated midnight flushes D−1 readings up to 30 real s after midnight, and the
archiver commits them up to a trigger later; without the wait, the rescan can start before
D−1's partition is complete — and the batch layer's entire justification is completeness. For a
restatement of an old file the wait is a no-op.
**Done when:** Within a minute of `reference_dropper` writing `tariff_D.csv`, a run
`billing__D` exists; re-running the watcher creates no duplicate; the sensor succeeds within
one poke; the grace task's log shows the computed wake time and the billing job starts ≥ 90
real s after the file's `LastModified`.

### T122 — `daily_billing_dag.py`: submit and verify
**Files:** `airflow/dags/daily_billing_dag.py`
**Do:** Per D6: `run_daily_billing` is a `DockerOperator` on `voltstream-spark:local` with
`docker_url="tcp://docker-socket-proxy:2375"`, `mount_tmp_dir=False`, `network_mode=<compose
network>`, `force_pull=False`, `auto_remove="force"`, credentials in `private_environment`,
`AIRFLOW_CTX_DAG_RUN_ID` passed through for lineage (`pipeline_runs.orchestrator_run_id`),
and the full command `spark-submit … daily_billing.py --sim-date {{ params.sim_date }}`.
Then `verify_bills`: an `SQLValueCheckOperator` (50 rows) and an `SQLCheckOperator` (no nulls,
`final_bill` within sane bounds) on the `VOLTSTREAM_PG` connection. `retries=2` with
exponential backoff on compute tasks — T115's transaction makes a retry safe. **No task
`sla=`** (removed in Airflow 3; `BatchSLAMiss` in T147 is the SLA). **Zero business logic in
the DAG** (§5.6: "they submit Spark jobs and verify results").
**Done when:** No tariff or netting arithmetic appears anywhere under `airflow/dags/`; the
task log shows the Spark job's structured JSON log lines streamed from the child container.

### T123 — `daily_billing_dag.py`: make restatement work
**Files:** `airflow/dags/daily_billing_dag.py`
**Do:** Per D6, restatement is **a new run of the same DAG for the same `sim_date`** with
`run_id = billing__<date>__r<n>` — not `airflow dags backfill`, whose data intervals are real
time and cannot address a simulated day. Make the DAG genuinely idempotent under that: every
task keyed on `params.sim_date`, nothing keyed on the logical date, and T040's supersede logic
exercised. §5.6's *mechanism* — re-execute deterministic code over immutable inputs for a
past day — is exactly this; only the CLI verb changes. It is the single highest-value demo in
the project — it must actually work, not merely be configured.
**Done when:** `airflow dags trigger daily_billing --conf '{"sim_date":"D"}' --run-id
billing__D__r2` on an already-finalised day D produces updated `household_bill_daily` rows, a
`superseded` + `success` pair in `pipeline_runs` (with two distinct `orchestrator_run_id`s),
and both runs remain visible in the UI.

### T124 — `batch/daily_zone_rollup.py`
**Files:** `src/voltstream/batch/daily_zone_rollup.py`, `airflow/dags/daily_billing_dag.py`
**Do:** Authoritative daily per-zone aggregates into the `zone_metrics_daily` table from `T037`.
Add it as a downstream task in the DAG.
**Done when:** `zone_metrics_daily` holds 5 rows per simulated day.

### T125 — Cross-check data-quality gate
**Files:** `src/voltstream/batch/daily_zone_rollup.py`, or a dedicated DAG task
**Do:** §10.4's cheap high-impact idea, promoted here because it is genuinely load-bearing:
assert `sum(zone totals) == sum(household totals)` for the day and **fail the DAG** on violation,
"rather than silently publishing a wrong report."
**Done when:** Deliberately corrupting one zone's aggregate fails the task with a clear message.

### T126 — GATE 4 prerequisite check
**Do:** Confirm one simulated day produces a complete finalised `household_bill_daily` (50 rows),
a `success` row in `pipeline_runs`, and an archived tariff parquet.
**Done when:** All three verified by SQL in a single run.

---

## Phase 10 — The merge function, reports, dashboard

### T127 — `api/routers/households.py` — the merge function
**Files:** `src/voltstream/api/routers/households.py`
**Do:** `GET /api/v1/households/{id}/bill?date=YYYY-MM-DD`, implementing §3.2 **exactly**: a
finalised row in `household_bill_daily` → return it with `source="batch", provisional=false`;
otherwise the speed estimate with `source="speed", provisional=true`. §3.2 calls it nine lines of
logic — keep it nine lines, and comment it as the physical embodiment of
`query = merge(batch_view, realtime_view)`. Per D4, it does **no arithmetic**: pick a row, map
it to `BillResponse`. The API is `SELECT`-only.
**Done when:** The same household and date returns `speed` before the DAG runs and `batch` after,
with no restart in between; and `grep -rn "core.tariff\|core.netting" src/voltstream/api/`
returns nothing.

### T128 — Merge-function tests
**Files:** `tests/integration/test_merge_function.py` (new)
**Do:** Speed row only → `source="speed"`. Add a finalised row → `source="batch"`. A `failed` run
row → does **not** flip to batch. A `superseded` + `success` pair → does flip. Neither present →
404.
**Done when:** All five pass. After `T063`, this is the highest-value test in the repository.

### T129 — Bill-delta endpoint
**Files:** `src/voltstream/api/routers/households.py`
**Do:** `GET /api/v1/households/{id}/bill/delta?date=…` returning speed estimate, batch final,
absolute and percentage divergence, and — once the day is reconciled — the D4 decomposition
`tariff_effect` and `data_effect` from `reconciliation_daily`. §3.2: a screenshot of the same
household before and after finalisation "proves comprehension of Lambda more convincingly than
several pages of prose" — so make that screenshot a single request, and let it say *why* the
figures differ.
**Done when:** One request returns both figures, the delta, and the two effects summing to it.

### T130 — `api/routers/reports.py`
**Files:** `src/voltstream/api/routers/reports.py`
**Do:** `GET /api/v1/reports/daily?date=…` returning the day's summary: per-zone totals, household
bill totals, reject counts, run status, reconciliation summary.
**Done when:** Returns a complete document for a finalised day, and a clearly-labelled partial one
for a day still open.

### T131 — Daily report file generator
**Files:** `scripts/generate_report.py` (new), wired into the DAG
**Do:** §9 Phase 3's exit criterion "Daily report file generated" — write the `T130` payload to a
Markdown or CSV file in an output volume.
**Done when:** A file is produced automatically at the end of the billing DAG.

### T132 — `api/routers/alerts.py`
**Files:** `src/voltstream/api/routers/alerts.py`
**Do:** `GET /api/v1/alerts/status` (§10.1), proxying Alertmanager's API for currently firing
alerts so the dashboard can surface them. Degrade gracefully to an empty list plus a warning if
Alertmanager is unreachable — **the API must not report unhealthy because monitoring is down.**
**Done when:** Firing an alert makes it appear in the response within one scrape interval.

### T133 — `dashboard/index.html`: layout
**Files:** `dashboard/index.html`
**Do:** Single file, no build step (§7.2). Zone load table, renewable ratio per zone, household
bill panel. Poll every 5 s (§8.3).
**Done when:** Opens in a browser and renders live data.

### T134 — Dashboard: the provisional/final indicator
**Files:** `dashboard/index.html`
**Do:** Show `source` and `provisional` **visually and unmissably** — a badge that changes when
the day finalises. §1.2's claim is that the architecture becomes a user-facing feature; this badge
is that claim made visible in one screenshot.
**Done when:** The badge visibly flips from "Provisional (speed)" to "Final (batch)" during a live
run.

### T135 — Serve the dashboard
**Files:** `src/voltstream/api/main.py`, `docker/docker-compose.yml`
**Do:** Mount `dashboard/` as FastAPI static files, so there is one origin and no CORS
configuration to debug during a demo.
**Done when:** `http://localhost:<api-port>/` serves the dashboard.

### T136 — GATE 4 · both Lambda paths live
**Do:** §9 Phase 3's exit criteria: finalised bills exist; the merge function flips `speed` →
`batch` for the same household and date; `reconciliation_daily` is populated (after Phase 11); the
daily report file is generated.
**Done when:** Demonstrated end to end in one uninterrupted run, with screenshots captured.

> **GATE 4.** Both Lambda paths exist. Everything after this is hardening and evidence.

---

## Phase 11 — Reconciliation

### T137 — `batch/reconciliation.py`
**Files:** `src/voltstream/batch/reconciliation.py`
**Do:** After each billing run, compare `household_running_rt.estimated_bill` (`S`) against
`household_bill_daily.final_bill` (`B`) per household. Per D4 this is **plain Python, no
`SparkSession`** — 50 rows — and it is the pure module's production caller: read today's
tariff CSV from the landing bucket via `objectstore.py`, compute the counterfactual
`C = compute_bill(netting(speed_kwh), today's tariff)` with `core/tariff.py`, and write
`reconciliation_daily` with `abs_divergence`, `pct_divergence`, `tariff_effect = S − C` and
`data_effect = C − B`. Runs on the **app** image.
**Done when:** 50 rows per simulated day with non-trivial divergence, and
`tariff_effect + data_effect == speed_estimate − batch_final` on every row. If divergence is
exactly zero everywhere, either the watermark is dropping nothing (see `T094`) or the
stale-tariff logic in `T091` is not actually stale — both are bugs, not successes. If
`data_effect` is zero but `tariff_effect` is not, D3's dropouts are off.

### T138 — Reconciliation metric
**Files:** `src/voltstream/batch/reconciliation.py`
**Do:** Emit mean and max divergence as `voltstream_lambda_divergence`. §9 Phase 3: "roughly forty
lines of code, and the thing an examiner will remember."
**Done when:** The gauge is visible in Prometheus after a run.

### T139 — Wire reconciliation into the DAG
**Files:** `airflow/dags/daily_billing_dag.py`
**Do:** Add as a downstream task of billing.
**Done when:** One DAG run produces bills, rollups and reconciliation in order.

### T140 — Attribute the divergence in writing
**Files:** `docs/architecture/01-lambda-vs-kappa.md` (or `05-open-decisions.md` until it exists)
**Do:** Record the two causes of divergence and their measured relative contribution — per D4
these are now read straight from `reconciliation_daily`, not estimated: (a) `data_effect` —
backfill dropped past the speed layer's watermark (D3), (b) `tariff_effect` — yesterday's
tariff versus today's (D2). Check the mean `data_effect` in kWh terms against D3's modelled
~1.7 % gap. Being able to *attribute* the delta — not merely display it — is the difference
between showing a number and understanding the architecture, and it is a very likely viva
question.
**Done when:** The measured split is written down, with numbers from a real run, and the
`data_effect` share is inside T094's band.

### T141 — Reconciliation tests
**Files:** `tests/unit/test_reconciliation.py` (new)
**Do:** The divergence arithmetic per D4/D5: `pct_divergence = 100 × abs_divergence /
(batch.energy_charge + batch.fixed_charge)`, and `0` when that base is `0` — the base is
**gross charges, not `final_bill`**, because a net exporter's `final_bill` can be negative or
near zero. Cases: base `= 0`; negative `final_bill`; the identity
`tariff_effect + data_effect == speed_estimate − batch_final` on every row; all `Decimal`.
**Done when:** Test passes.

---

## Phase 12 — Observability

### T142 — `config/prometheus/prometheus.yml`
**Files:** `config/prometheus/prometheus.yml`
**Do:** Scrape configs for every service exposing `/metrics`: api, meter-producer, the speed-layer
driver, the raw-archiver driver, and the Pushgateway if `T116` chose one. Scrape interval sized
against `alerts.stale_data_minutes` — a 2-minute alert cannot be evaluated reliably on a 1-minute
scrape.
**Done when:** Prometheus `/targets` shows every target `UP`.

### T143 — Compose: Prometheus
**Files:** `docker/docker-compose.yml`
**Do:** Tier-4 service, config mounted read-only, named volume for TSDB data.
**Done when:** The UI loads and returns data for all eight metric names.

### T144 — Alert rule: `MeterDataStale`
**Files:** `config/prometheus/alert_rules.yml`
**Do:** No reading from a zone for > `stale_data_minutes` **real** minutes. Include `for:`, a
severity label, and an annotation naming the zone.
**Done when:** `promtool check rules` passes and the rule appears in the Prometheus UI.

### T145 — Alert rule: `LowRenewableContribution`
**Files:** `config/prometheus/alert_rules.yml`
**Do:** `voltstream_zone_renewable_ratio` below `low_renewable_threshold` for 3 consecutive
windows. **Explicitly required by the brief** (§9 Phase 4's table), so get the window arithmetic
right and sanity-check it against `T068`'s measured range.
**Done when:** Fires during simulated night and clears during simulated midday.

### T146 — Alert rule: `HighRejectRate`
**Files:** `config/prometheus/alert_rules.yml`
**Do:** `rate(rejected) / rate(consumed) > reject_rate_threshold` over 5 minutes. Guard the
denominator against zero, or the rule fires spuriously whenever the pipeline is idle.
**Done when:** Raising the injection rates makes it fire; an idle pipeline does not.

### T147 — Alert rule: `BatchSLAMiss`
**Files:** `config/prometheus/alert_rules.yml`
**Do:** Billing DAG not complete within `batch_sla_minutes` of simulated day close. This needs a
"last successful run" gauge, which does not exist yet — source it from the Pushgateway or scrape
it from Postgres via `postgres_exporter`. Pick one and add it as part of this task.
**Done when:** Pausing the DAG makes it fire.

### T148 — Alert rule: `LambdaDivergenceHigh`
**Files:** `config/prometheus/alert_rules.yml`
**Do:** Mean `voltstream_lambda_divergence` above `lambda_divergence_threshold_pct`. The bonus
rule, and the one showing the system monitoring its own architecture.
**Done when:** Fires when the threshold is lowered below the observed value.

### T149 — `config/alertmanager/alertmanager.yml`
**Files:** `config/alertmanager/alertmanager.yml`
**Do:** Routing by severity, grouping, and inhibition — `MeterDataStale` should inhibit
`LowRenewableContribution` for the same zone, because killing the producer otherwise fires both
and the demo looks like noise rather than signal. A webhook or file receiver is enough; no real
paging.
**Done when:** `amtool check-config` passes and a test alert reaches the receiver.

### T150 — Compose: Alertmanager
**Files:** `docker/docker-compose.yml`
**Do:** Tier-4 service, config mounted, wired to Prometheus.
**Done when:** Prometheus `/alertmanagers` shows it discovered.

### T151 — Grafana provisioning
**Files:** `config/grafana/provisioning/datasources.yml`, `config/grafana/provisioning/dashboards.yml`
**Do:** Prometheus **and** Postgres datasources provisioned as code, plus a dashboard provider
pointed at `config/grafana/dashboards/`. Zero manual clicking on a fresh start — §2.5 makes
reproducibility explicitly graded.
**Done when:** A cold `make clean && make up` yields a Grafana with both datasources and all
dashboards already present.

### T152 — Grafana dashboard: pipeline health
**Files:** `config/grafana/dashboards/pipeline-health.json`
**Do:** Events produced vs consumed per layer, consumer lag, reject rate by reason, e2e latency
quantiles, batch duration.
**Done when:** Exported JSON committed and loading from provisioning.

### T153 — Grafana dashboard: grid operations
**Files:** `config/grafana/dashboards/grid-operations.json`
**Do:** Per-zone load and renewable ratio over time from the Postgres datasource, with the alert
threshold drawn as a line.
**Done when:** Committed and loading.

### T154 — Grafana dashboard: Lambda divergence
**Files:** `config/grafana/dashboards/lambda-divergence.json`
**Do:** Speed vs batch per household, divergence distribution, divergence over time. This is what
makes §9 Phase 3's "standout addition" visible to an examiner.
**Done when:** Committed and loading.

### T155 — Compose: Grafana
**Files:** `docker/docker-compose.yml`
**Do:** Tier-4 service, provisioning mounted, anonymous viewer access enabled so the demo needs no
login.
**Done when:** Dashboards render with data on a cold start.

### T156 — `docs/architecture/04-observability.md`
**Files:** `docs/architecture/04-observability.md` (new)
**Do:** Extract §10.1 — the logging envelope, the metric table, the honest correlation-ID tracing
limitation, the health endpoints. Add the alert-rule table, with measured firing conditions filled
in after Phase 13.
**Done when:** Committed; the metric table matches `metrics.py` exactly.

---

## Phase 13 — Demonstrating failure

> §9 Phase 4: "'We instrumented the pipeline' is an assertion; a screenshot of `MeterDataStale`
> firing after you killed the producer is **evidence**. Ten marks turn on that difference." This
> phase is explicitly not optional.

### T157 — `scripts/inject_faults.sh`: stale data
**Files:** `scripts/inject_faults.sh`
**Do:** Stop the producer; wait past `stale_data_minutes`; confirm `MeterDataStale` is firing via
the Alertmanager API; restart; confirm it resolves. Print each step.
**Done when:** The script **asserts** the alert state rather than pausing for a human to look.

### T158 — `scripts/inject_faults.sh`: high reject rate
**Files:** `scripts/inject_faults.sh`
**Do:** Restart the producer with elevated fault rates via environment override; confirm
`HighRejectRate` fires; restore.
**Done when:** Asserted programmatically.

### T159 — `scripts/inject_faults.sh`: batch SLA miss
**Files:** `scripts/inject_faults.sh`
**Do:** Pause the DAG; wait past the SLA; confirm `BatchSLAMiss` fires; unpause.
**Done when:** Asserted programmatically.

### T160 — `scripts/inject_faults.sh`: low renewable contribution
**Files:** `scripts/inject_faults.sh`
**Do:** Force cloud cover to ~100 % via config override so solar collapses; confirm
`LowRenewableContribution` fires.
**Done when:** Asserted programmatically.

### T161 — `scripts/inject_faults.sh`: lambda divergence
**Files:** `scripts/inject_faults.sh`
**Do:** Temporarily shrink the watermark (or raise the out-of-order rate) so the speed layer drops
more; confirm `LambdaDivergenceHigh` fires after the next batch run.
**Done when:** Asserted programmatically.

### T162 — `scripts/backfill.sh` — the restatement demo
**Files:** `scripts/backfill.sh`
**Do:** §5.6's thirty-second demo, scripted: corrupt a monetary column in the day's tariff CSV
(D2: e.g. `block_2_rate` `16.50 → 61.50`) → trigger a restatement run → print the wrong bills
→ restore the file → trigger another → print the corrected bills → print the diff. Per D6 the
restatement command is `docker compose exec airflow airflow dags trigger daily_billing --conf
'{"sim_date":"<date>"}' --run-id billing__<date>__r<n>`, with `<n>` computed from the existing
runs; the script polls the run to completion. Confirms `T040`'s superseded-run handling under
real conditions and leaves **both** runs visible in the Airflow UI — the audit trail shows the
wrong bill, the corrected bill, and which run produced each. §5.6: "That single demo defends
the entire architecture chapter." Keep the `make backfill d=<date>` name.
**Done when:** Running it end to end produces a visible before/after bill table with no manual
intervention, and `pipeline_runs` shows `superseded → superseded → success` for the day with
three distinct `orchestrator_run_id`s.

### T163 — `scripts/demo.sh`
**Files:** `scripts/demo.sh`
**Do:** The full sequence: cold start → **wait on health checks, never `sleep 30` and hope**
(§8.2) → run one full simulated day → show the provisional bill → wait for finalisation → show
the final bill and the delta → open the dashboard. Idempotent and re-runnable.
**Done when:** `make demo` on a warm stack completes in roughly one simulated day plus batch time,
with no human input.

### T164 — Capture the evidence
**Files:** `docs/report/screenshots/`
**Do:** Screenshot every one of: each of the five alerts firing; the dashboard badge before and
after finalisation; the Grafana divergence panel; the backfill before/after; the Airflow DAG graph;
the OpenAPI docs page; CI green.
**Done when:** Every §9 Phase 4 and Appendix C item has a corresponding image, named after what it
evidences.

### T165 — `docs/runbook.md`
**Files:** `docs/runbook.md` (new)
**Do:** §7.2 asks for "how to demo, how to break it on purpose". Write the demo as prose with
expected outputs and timings, plus a troubleshooting table for the failure modes you actually hit
while building. This is the document you read from during the live demo.
**Done when:** Someone who did not build the system can run the demo from this file alone.

### T166 — `make faults` and `make backfill`
**Files:** `Makefile`
**Do:** Wire `T157`–`T162` into the Appendix B targets.
**Done when:** `make faults` runs all five in sequence; `make backfill d=<date>` runs the
restatement.

### T167 — Kill-test the speed layer
**Files:** `docs/runbook.md`, `scripts/smoke_test.sh`
**Do:** §10.3's anticipated viva question, "What happens if the speed layer dies mid-day?" — kill
it, show the dashboard gap, restart, show it resume from checkpoint, and show the day's bill is
**unaffected** because billing reads Parquet, not the speed view. Script it and capture it.
**Done when:** Demonstrated and screenshotted, with the bill total identical before and after the
kill.

### T168 — Replace the latency table with measurements
**Files:** `docs/architecture/04-observability.md`
**Do:** §10.3 presents a latency breakdown table as fact. Replace its numbers with **measured**
ones from `voltstream_e2e_latency_seconds`. §10.3's own words: "We measure this with
`voltstream_e2e_latency_seconds` rather than asserting it" — so it must actually be measured.
**Done when:** The table carries measured values with a measurement date, and the end-to-end figure
is confirmed under the 60 s NFR (§2.5).

---

## Phase 14 — Tests, CI, reproducibility

### T169 — CI workflow
**Files:** `.github/workflows/ci.yml`
**Do:** On push and PR: checkout; `setup-python` at the pinned version; **`setup-java`** — PySpark
needs a JVM and the consistency test runs in CI, so omitting this is the usual reason CI goes red
here; cache pip; `pip install -e ".[dev,api,sim,spark]"`; `ruff check`; `ruff format --check`;
`mypy src`; `pytest -m "not integration"`.
**Done when:** Green badge on `main`, with the consistency test running in CI and not only locally.

### T170 — CI: coverage gate on `core/`
**Files:** `.github/workflows/ci.yml`, `pyproject.toml`
**Do:** `pytest --cov=voltstream.core --cov-fail-under=<N>`. Gate `core/` specifically — that is
where the correctness marks live; gating the whole repository would mostly reward testing glue.
**Done when:** CI fails if `core/` coverage drops below the threshold.

### T171 — Integration test: producer to Kafka
**Files:** `tests/integration/test_producer_to_kafka.py`
**Do:** Run the producer against a test broker; consume N messages; assert keying, schema validity,
and that observed fault rates match configuration.
**Done when:** `pytest -m integration` passes against the Compose stack.

### T172 — Integration test: batch end to end
**Files:** `tests/integration/test_batch_end_to_end.py`
**Do:** Write a known synthetic day to Parquet and a known tariff CSV; run `daily_billing.py`;
assert the exact expected bills — **hand-computed**, not regenerated from the code under test. A
test that computes its own expectation from the implementation asserts nothing. The first three
households are the D5 worked examples (`720.17`, `454.41`, `−150.00`). Then, per D5, **run the
job a second time on the same fixtures and assert every `household_bill_daily` row is identical**
(excluding `pipeline_run_id` and `computed_at`) — this is §5.4's "byte-identical output" claim
as a test, and it is what `DecimalType` buys over float64.
**Done when:** Passes, and changing a block rate in the fixture CSV changes the expected value —
proving the test checks arithmetic rather than plumbing.

### T173 — Test fixtures
**Files:** `tests/fixtures/`
**Do:** Committed sample files — one day of readings, one tariff CSV, one weather CSV, and the
expected bill output — so integration tests do not depend on the simulator's RNG.
**Done when:** `T172` runs from fixtures alone.

### T174 — `make test` / `make test-all`
**Files:** `Makefile`
**Do:** `test` = unit + property + consistency, no containers. `test-all` = everything, including
integration.
**Done when:** `make test` passes on a machine with Docker stopped.

### T175 — Cold-start reproducibility test
**Files:** `scripts/smoke_test.sh`
**Do:** §9 Gate 5: clone into a **new directory**, `cp .env.example .env`, `make demo`, and assert
a working system with zero manual steps. "Reproducibility means a *cold* start, not 'it works on
my machine with the containers already warm.'"
**Done when:** Verified on a genuinely fresh clone — ideally on a second machine — with
`docker system prune` run first.

### T176 — Resource sizing check
**Files:** `docs/runbook.md`, `README.md`
**Do:** Eleven-plus containers including Spark and Airflow is heavy. Measure peak RAM and CPU and
state the minimum Docker Desktop allocation required. Discovering on demo day that the presentation
machine cannot run the stack is an avoidable disaster, and it is avoided only by measuring in
advance.
**Done when:** Minimum requirements are stated in the README and verified on the machine that will
run the demo.

### T177 — Startup-order hardening
**Files:** `docker/docker-compose.yml`, `scripts/demo.sh`
**Do:** Re-verify §8.2's tier discipline now that all services exist: every tier-3 service
`depends_on` its tier-2 init job with `condition: service_completed_successfully`. Then run
`make clean && make up` five times and confirm no crash-loops.
**Done when:** Five consecutive cold starts succeed with no container restarting more than once.

### T178 — `docker-compose.override.yml`
**Files:** `docker/docker-compose.override.yml`
**Do:** §7.2's dev-only file: source bind-mounts for hot reload, debug logging, debugger ports. It
must not be required for the demo path.
**Done when:** `docker compose -f docker-compose.yml up` with the override excluded works
identically.

> **GATE 5.** Cold start, zero manual steps, all alerts demonstrated, restatement rehearsed.

---

## Phase 15 — Documentation, report, viva

### T179 — `docs/architecture/01-lambda-vs-kappa.md`
**Files:** `docs/architecture/01-lambda-vs-kappa.md`
**Do:** Extract §4 in full: both architectures; the four-criterion table; the honest Kappa case
(§4.3 warns that "dismissing it weakly loses marks"); the drift cost and the structural mitigation;
and §4.5's falsifiable contingency. ≥ 1,500 words — it is the 20-mark deliverable.
**Done when:** Every claim in it points at a file or a test that actually exists.

### T180 — `docs/architecture/02-tech-stack.md`
**Files:** `docs/architecture/02-tech-stack.md`
**Do:** Extract §5. Every choice keeps its named rejected alternative and its use-case constraint —
§1.3 is explicit that this pairing is what the 10 marks are for.
**Done when:** Seven layers, each with at least one explicitly rejected alternative.

### T181 — Architecture diagrams
**Files:** `docs/architecture/diagrams/*.drawio`, `*.png`
**Do:** Render §6.1's layer view, a data-flow diagram, and a sequence diagram of one simulated day
(§8.3). Commit both source and export.
**Done when:** PNGs are legible at report print size and depict the system **as built**, not as
originally designed.

### T182 — Reconcile the document against the code
**Files:** `docs/architecture/00-master-design.md`
**Do:** Apply the document's own rule — "If the code and this document disagree, one of them is a
bug." Walk §6.2, §6.3, §6.4, §7.2 and Appendix A against the built system and fix every divergence,
including: the `T037`/`T038` missing tables, the `T003` tariff columns, the `T004` watermark units,
the `T007` sensor type, and the corrected filenames from `T012`.
**Done when:** A reviewer can diff the document against the tree and find no contradictions.

### T183 — Complete the README
**Files:** `README.md`
**Do:** §7/§8/§9-derived: architecture summary and diagram, quickstart, service/port table, the
Appendix B command reference, a rubric-criterion-to-file map, and a limitations summary. §9.2 lists
the README among the five things that must never be cut.
**Done when:** It stands alone as the repository's front door.

### T184 — Report chapter: use case and problem
**Files:** `docs/report/`
**Do:** From §1 and §2, including §2.2's opposing-requirements table — that table *is* the
architecture decision, so it belongs in the report verbatim.
**Done when:** Drafted.

### T185 — Report chapter: architecture decision (20 marks)
**Files:** `docs/report/`
**Do:** From `01-lambda-vs-kappa.md`, with the measured divergence numbers from `T140` as supporting
evidence.
**Done when:** Drafted and reviewed by everyone who will be in the viva.

### T186 — Report chapter: technology stack (10 marks)
**Files:** `docs/report/`
**Do:** From `02-tech-stack.md`.
**Done when:** Drafted.

### T187 — Report chapter: observability (10 marks)
**Files:** `docs/report/`
**Do:** From `04-observability.md`, with the `T164` alert screenshots inline and the `T168` measured
latency table.
**Done when:** Drafted; every alert has a screenshot of it actually firing.

### T188 — Report chapter: results
**Files:** `docs/report/`
**Do:** Sample outputs, the provisional→final screenshot pair, the backfill before/after, the
reconciliation distribution.
**Done when:** Drafted.

### T189 — Report chapter: limitations
**Files:** `docs/report/`
**Do:** From §10.2 and `docs/assumptions.md`, including §10.2's Postgres scale-boundary sentence
verbatim.
**Done when:** Every limitation named in the design document appears.

### T190 — Assemble the PDF
**Files:** `docs/report/`
**Do:** 8–15 pages per §9 Phase 5, with the individual-contributions statement.
**Done when:** PDF produced, page count in range, every rubric criterion addressed.

### T191 — Demo video
**Do:** 5–10 minutes following `docs/runbook.md`: cold start, both paths live simultaneously, the
merge flip, an alert firing, the backfill restatement.
**Done when:** Recorded, within the time limit, audio intelligible.

### T192 — Viva preparation
**Files:** `docs/architecture/06-viva-prep.md` (new)
**Do:** Write out §10.3's questions with answers grounded in **this** implementation's measured
numbers, not the design document's estimates. Add the questions you were actually asked during
internal cross-examination. The brief requires every member to defend every component, including
ones they did not write (§9.1).
**Done when:** Every member can answer every §10.3 question unaided.

### T193 — Definition of done
**Files:** `docs/runbook.md`
**Do:** Walk Appendix C's nine-item checklist and tick each with evidence — a file path, a
screenshot, or a test name.
**Done when:** All nine ticked, with evidence attached.

---

## Dependency map — the parts that are easy to get wrong

```
T002 (Python pin) ──────── blocks everything Spark, including CI

T003 (tariff source) ──┬── T019 base.yaml ──┬── T031 reference.py
                       │                    └── T057/T058 core/tariff.py ──┬── T062 spark_expr
                       └── T074 reference_dropper                          └── T063 consistency

T004 (watermark units) ─── T070 faults ─── T088 speed_layer ─── T094 verify ─── T137 reconciliation
T005 (who computes)    ─── T091 speed bill ─── T127 merge function
T006 (bill formula)    ─── T057/T058 ─── T063 ─── T172 integration expectations
T007 (Airflow submit)  ─── T119 airflow image ─── T121/T122 DAG
T037/T038 (missing tables) ─── T124 rollup, T053 known-household validation
T040 (unique index)    ─── T115 finalise ─── T123 backfill ─── T162 restatement demo
T044 (checkpoint volumes) ─── T083 GATE 2 ─── T167 kill test
```

**The five things that carry disproportionate marks and must never be cut** (§9.2): the master
dataset, the merge function (`T127`), structured logging (`T025`), the alert rules
(`T144`–`T148`), and the README (`T183`).

**Cut order if time runs short** — §9.2 says decide this now, while calm, not on day 11:
Grafana (`T151`–`T155`) → the custom dashboard (`T133`–`T135`) → integration tests
(`T171`–`T173`) → the weather file → MinIO replaced by a local volume mount.
