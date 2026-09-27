# Voltstream — Codebase guide

What voltstream is, how each part of it is implemented, and the order in which to read the
code. This describes the code as it exists at commit `96b49c1` on `dev` (2026-09-27), not
as it was planned.

| Document | Use it for |
|---|---|
| [README](../README.md) | Quickstart and service list |
| [00-master-design.md](architecture/00-master-design.md) | The original plan and the argument for it (why Lambda, why each tool). Written before the code; some details have since changed. |
| [05-open-decisions.md](architecture/05-open-decisions.md) | Decisions D1–D8: what the design left open, what was chosen, and the measurements behind it. When the code and the master design disagree, the reason is usually here. |
| [Implementation_Tasks.md](Implementation_Tasks.md) | The build plan, T001 onwards, phase by phase. Code comments cite these T-numbers. |
| [assumptions.md](assumptions.md) | Simplifications and measured limitations |
| [debugging-backlog.md](debugging-backlog.md) | Known defects R01–R35 found in review |
| **this file** | How the code works, and where to start reading it |

## Contents

- [Voltstream — Codebase guide](#voltstream--codebase-guide)
  - [Contents](#contents)
- [Part 1 — The high-level view](#part-1--the-high-level-view)
  - [1.1 What the application does](#11-what-the-application-does)
  - [1.2 Why a Lambda architecture](#12-why-a-lambda-architecture)
  - [1.3 The big picture](#13-the-big-picture)
  - [1.4 The running pieces](#14-the-running-pieces)
  - [1.5 Where data lives](#15-where-data-lives)
  - [1.6 Simulated time](#16-simulated-time)
  - [1.7 One simulated day, end to end](#17-one-simulated-day-end-to-end)
  - [1.8 The merge function](#18-the-merge-function)
  - [1.9 The rules the code follows](#19-the-rules-the-code-follows)
- [Part 2 — How it is implemented](#part-2--how-it-is-implemented)
  - [2.1 Repository layout](#21-repository-layout)
  - [2.2 One package, three images](#22-one-package-three-images)
  - [2.3 Configuration](#23-configuration)
  - [2.4 The simulated clock](#24-the-simulated-clock)
  - [2.5 Data contracts](#25-data-contracts)
  - [2.6 The shared core](#26-the-shared-core)
    - [`netting.py` — solar self-consumption](#nettingpy--solar-self-consumption)
    - [`money.py` — precision](#moneypy--precision)
    - [`tariff.py` — the executable specification](#tariffpy--the-executable-specification)
    - [`spark_expr.py` — the production implementation](#spark_exprpy--the-production-implementation)
    - [`validation.py` and `keys.py`](#validationpy-and-keyspy)
  - [2.7 Simulators](#27-simulators)
  - [2.8 Streaming: raw archiver and speed layer](#28-streaming-raw-archiver-and-speed-layer)
    - [`session.py`](#sessionpy)
    - [`sources.py`](#sourcespy)
    - [`sinks.py`](#sinkspy)
    - [`raw_archiver.py` — the master dataset](#raw_archiverpy--the-master-dataset)
    - [`speed_layer.py` — three independent streaming queries](#speed_layerpy--three-independent-streaming-queries)
  - [2.9 Storage and the serving schema](#29-storage-and-the-serving-schema)
    - [The schema](#the-schema)
    - [`objectstore.py`](#objectstorepy)
    - [`postgres.py`](#postgrespy)
    - [`repositories.py`](#repositoriespy)
  - [2.10 Batch layer](#210-batch-layer)
    - [`daily_billing.py` — the authoritative bill](#daily_billingpy--the-authoritative-bill)
    - [`daily_zone_rollup.py`](#daily_zone_rolluppy)
    - [`reconciliation.py` — the system checking its own divergence](#reconciliationpy--the-system-checking-its-own-divergence)
  - [2.11 Orchestration with Airflow](#211-orchestration-with-airflow)
    - [`tariff_watcher_dag.py`](#tariff_watcher_dagpy)
    - [`daily_billing_dag.py`](#daily_billing_dagpy)
  - [2.12 API and dashboard](#212-api-and-dashboard)
  - [2.13 Observability](#213-observability)
  - [2.14 Infrastructure and tooling](#214-infrastructure-and-tooling)
    - [Compose (`docker/docker-compose.yml`)](#compose-dockerdocker-composeyml)
    - [Makefile](#makefile)
    - [Scripts](#scripts)
  - [2.15 Tests](#215-tests)
- [Part 3 — Where to start reading](#part-3--where-to-start-reading)
  - [3.1 First, about 30 minutes of documents](#31-first-about-30-minutes-of-documents)
  - [3.2 Recommended reading order](#32-recommended-reading-order)
  - [3.3 Or follow one reading through the system](#33-or-follow-one-reading-through-the-system)
  - [3.4 Reading tips](#34-reading-tips)
- [Part 4 — What is built, what is not, known issues](#part-4--what-is-built-what-is-not-known-issues)
- [Glossary](#glossary)

---

# Part 1 — The high-level view

## 1.1 What the application does

voltstream is a data platform for a made-up electricity utility. It serves fifty
households, each with a smart meter, spread across five grid zones. A third of the
households have rooftop solar panels. Two kinds of data come in:

- **A continuous stream of meter readings.** Each one records the energy a household
  consumed, and the solar energy it generated, over a short interval.
- **Two reference files per day:** a tariff file giving each household's rates for that
  day, and a weather forecast per zone.

The same readings have to answer two questions for two different users:

| | Grid operations | Billing |
|---|---|---|
| Question | What are the load and the renewable share in each zone right now? | What does each household owe for a day, with that day's tariff applied? |
| Needs the answer | Within seconds | After the day has ended |
| Effect of a 2 % error | None: it does not change an operational decision | A wrong bill, which is a legal problem |
| Late-arriving readings | Can be ignored | Must be included |
| Recompute months later? | Never | Yes, for disputes and tariff corrections |

Nothing is real. A producer process generates the meter readings, and a second process
writes the daily files. The whole system runs on one laptop under Docker Compose.

## 1.2 Why a Lambda architecture

A single processing path cannot be both fast and exact. Tuning it for completeness makes
the operations view late, and tuning it for speed makes the bills wrong. So voltstream
runs two paths over the same input:

- The **speed layer** reads the live stream and produces approximate figures within
  seconds. It drops readings that arrive too late.
- The **batch layer** waits until a day has ended, then rescans every raw reading for
  that day from an immutable archive and computes exact bills.
- The **serving layer** (the API) answers from the batch layer when the batch layer has
  finished the day, and from the speed layer otherwise. Every response says which layer
  it came from.

The deciding argument against the single-path alternative, Kappa, is retention. Bills
must be recomputable months later, and Kafka keeps data for seven days. So every raw
reading is also written to object storage as Parquet. This copy is the **master
dataset**, and the batch layer recomputes from it rather than from Kafka. The full
argument is in [master design §4](architecture/00-master-design.md#4-architecture-decision-lambda-vs-kappa).

Lambda has one well-known weakness: the same business logic lives in two code paths,
and the two copies can drift apart. voltstream deals with this structurally. The billing
logic lives once, in [`core/`](../src/voltstream/core/). Both layers import the same
Spark expressions from it, and a test pins those expressions to a plain-Python
specification over thousands of random inputs.

## 1.3 The big picture

```
                 SOURCES (simulated)
   ┌──────────────────────────┐        ┌──────────────────────────────┐
   │ meter-producer           │        │ reference-dropper            │
   │ 50 readings / 2 real s   │        │ tariff + weather, 1 per day  │
   │ + injected faults        │        │                              │
   └────────────┬─────────────┘        └───────────────┬──────────────┘
                │ JSON, key = household_id             │ CSV
                ▼                                      ▼
   ┌──────────────────────────┐        ┌──────────────────────────────┐
   │ Kafka  meter.readings    │        │ MinIO  voltstream-landing    │
   │ 3 partitions, 7 days     │        │ tariff/tariff_<date>.csv     │
   │ (+ meter.readings.dlq)   │        │ weather/weather_<date>.csv   │
   └──────┬────────────┬──────┘        └──────┬──────────────┬────────┘
          │            │                      │              │
          │            │   yesterday's tariff │              │ today's tariff
          ▼            ▼                      │              │
 ┌─────────────────┐ ┌──────────────────┐     │              │
 │ speed-layer     │ │ raw-archiver     │     │              │
 │ Spark, 3 queries│◄┼──────────────────┼─────┘              │
 │ validate,       │ │ NO transformation│                    │
 │ window, bill    │ └────────┬─────────┘                    │
 └────────┬────────┘          │ Parquet                      │
          │                   ▼                              │
          │        ┌──────────────────────────┐              │
          │        │ MinIO  voltstream-raw    │              │
          │        │ sim_date=…/hour=…        │              │
          │        │ MASTER DATASET           │              │
          │        └────────────┬─────────────┘              │
          │                     │ full-day rescan            │
          │                     ▼                            │
          │        ┌──────────────────────────┐              │
          │        │ Airflow → Spark job      │◄─────────────┘
          │        │ dedup, validate, bill,   │
          │        │ rollup, reconcile        │
          │        └────────────┬─────────────┘
          ▼                     ▼
 ┌──────────────────────────────────────────────────────────┐
 │ PostgreSQL (serving layer)                               │
 │ speed view: zone_metrics_rt, household_running_rt        │
 │ batch view: household_bill_daily, zone_metrics_daily,    │
 │             pipeline_runs, reconciliation_daily          │
 │ both:       rejected_records                             │
 └──────────────────────────┬───────────────────────────────┘
                            │ SELECT only
                            ▼
 ┌──────────────────────────────────────────────────────────┐
 │ FastAPI: zones, bills (MERGE), reports, health, alerts   │
 │ + the single-file dashboard, polling every 5 s           │
 └──────────────────────────────────────────────────────────┘
```

**There is no orchestrating program.** There is no `run_pipeline.py`. The system is five
long-running processes plus a batch job that Airflow launches once per simulated day.
They are connected only through the stores: Kafka, MinIO and Postgres. The producer does
not know whether anything consumes its readings, and the API does not know whether Spark
is running. Each piece can therefore be restarted on its own. To follow the system, trace
data through the stores, not calls through a stack.

## 1.4 The running pieces

| Container | Image | Code it runs | Reads | Writes |
|---|---|---|---|---|
| `meter-producer` | app | [`simulators/meter_producer.py`](../src/voltstream/simulators/meter_producer.py) | the simulated clock | Kafka `meter.readings` |
| `reference-dropper` | app | [`simulators/reference_dropper.py`](../src/voltstream/simulators/reference_dropper.py) | the simulated clock | MinIO landing bucket |
| `raw-archiver` | spark | [`streaming/raw_archiver.py`](../src/voltstream/streaming/raw_archiver.py) | Kafka | MinIO raw bucket (Parquet) |
| `speed-layer` | spark | [`streaming/speed_layer.py`](../src/voltstream/streaming/speed_layer.py) | Kafka, yesterday's tariff CSV | `zone_metrics_rt`, `household_running_rt`, `rejected_records`, the DLQ topic |
| `api` | app | [`api/main.py`](../src/voltstream/api/main.py) | Postgres (SELECT only), MinIO health, Alertmanager | nothing |
| `airflow` | airflow | [`airflow/dags/`](../airflow/dags/) | the landing bucket listing, Postgres checks | launches the four containers below |
| *per day:* billing | spark | [`batch/daily_billing.py`](../src/voltstream/batch/daily_billing.py) | one day of Parquet, that day's tariff | `household_bill_daily`, `pipeline_runs`, `rejected_records`, archived tariff |
| *per day:* zone rollup | spark | [`batch/daily_zone_rollup.py`](../src/voltstream/batch/daily_zone_rollup.py) | one day of Parquet, `household_bill_daily` | `zone_metrics_daily` |
| *per day:* reconciliation | app | [`batch/reconciliation.py`](../src/voltstream/batch/reconciliation.py) | both bill tables, that day's tariff | `reconciliation_daily`, divergence gauge |
| *per day:* report | spark | [`scripts/generate_report.py`](../scripts/generate_report.py) | Postgres | a Markdown report file |

Infrastructure containers: `kafka`, `postgres`, `minio` (actually `pgsty/silo`, a MinIO
fork; see D8), three one-shot `*-init` containers that create topics, tables and buckets,
and `docker-socket-proxy`, which lets Airflow start containers without holding the Docker
socket.

## 1.5 Where data lives

**Kafka topics**

| Topic | Content | Key |
|---|---|---|
| `meter.readings` | every meter reading, JSON | `household_id`, so one household's readings share a partition and stay in order |
| `meter.readings.dlq` | rejected records, with the reason | `trace_id` |

**MinIO buckets**

| Bucket / path | Content | Written by |
|---|---|---|
| `voltstream-raw/meter_readings/sim_date=D/hour=H/*.parquet` | the master dataset: every reading as received | raw archiver |
| `voltstream-landing/tariff/tariff_D.csv`, `weather/weather_D.csv` | the daily reference drops | reference dropper |
| `voltstream-archive/tariff/sim_date=D/` | the tariff a billing run actually used, as Parquet | billing job |

**Postgres tables** (database `voltstream`; Airflow keeps its metadata in a separate
`airflow` database on the same server)

| Table | Layer | Key | Written by | Read by |
|---|---|---|---|---|
| `zone_metrics_rt` | speed | zone, window start | speed layer | API zone endpoints, dashboard |
| `household_running_rt` | speed | household, day | speed layer | merge function, reconciliation |
| `household_bill_daily` | batch | household, day | billing job | merge function, rollup cross-check, reconciliation, report |
| `zone_metrics_daily` | batch | zone, day | zone rollup | daily report |
| `pipeline_runs` | batch | run id | billing job | merge function: "is this day finalised?" |
| `reconciliation_daily` | batch | household, day | reconciliation | `/bill/delta`, report |
| `rejected_records` | both | serial id | speed layer, billing job | report |
| `households` | dimension | household | seed SQL | nothing in code yet; the roster is derived from the same formula instead |

**Docker volumes:** data for Kafka, Postgres and MinIO, plus `speed_checkpoints` and
`archiver_checkpoints` for Spark. `make clean` deletes all of them.

## 1.6 Simulated time

This is the concept most of the code depends on. One simulated day passes in five real
minutes, so a full day's cycle, including billing, can be shown in a single demo.

```
sim_now() = epoch_sim + (real_now − anchor_real) × time_scale
```

- `time_scale = 288`: 86,400 simulated seconds in 300 real seconds.
- `epoch_sim = 2026-01-01T00:00:00Z`: the first simulated day of every fresh run.
- `anchor_real`: the real instant that corresponds to `epoch_sim`. `make up` (or
  `voltstream.ps1 start`) writes it to `.env` as `VOLTSTREAM_ANCHOR_REAL`, and Compose
  passes it to every container. Without a shared anchor, each process would count from
  its own start time. Five seconds of startup skew is 24 simulated minutes, enough to put
  the same reading in a different window.

| Real | Simulated | What it is |
|---|---|---|
| 2 s | 9.6 min | one producer tick |
| 3.125 s | 15 min | one zone window |
| 6.25 s | 30 min | the speed layer's watermark |
| 10 s | 48 min | one speed-layer micro-batch trigger |
| 90 s | 7.2 h | the batch layer's late-data grace |
| 5 min | 1 day | one simulated day, 150 readings per meter |

**Rule:** `event_ts`, `sim_date` and window bounds are simulated time. Latency and
`rejected_at` are real (wall-clock) time. Every duration key in the config names its
unit (`watermark_sim_minutes`, `trigger_interval_real_seconds`), because mixing the two
units up breaks the watermark (decision D3).

## 1.7 One simulated day, end to end

Take simulated day D = 2026-01-02.

1. **D begins.** Within five real seconds the reference dropper notices the date has
   changed and writes `tariff_2026-01-02.csv` and `weather_2026-01-02.csv`. It writes
   *today's* file at the *start* of the day, which matters in step 5.
2. **During D (five real minutes).** Every two real seconds the producer stamps 50
   readings with the current simulated time. It corrupts, duplicates or delays some of
   them on purpose, then publishes them to Kafka.
   - The **raw archiver** appends each micro-batch to Parquet under `sim_date=2026-01-02/hour=HH/`.
   - The **speed layer** validates readings and sends bad ones to `rejected_records` and
     the DLQ. Every ten real seconds it upserts 15-minute zone windows into
     `zone_metrics_rt`. It also upserts each household's running total for the day into
     `household_running_rt`, costed against **yesterday's** tariff
     (`tariff_2026-01-01.csv`), because today's is not considered final yet.
   - The **dashboard** polls `/api/v1/zones/load` every five seconds. A request for
     `/api/v1/households/HH-0012/bill?date=2026-01-02` returns `source: "speed"` and
     `provisional: true`.
3. **D ends.** The dropper writes `tariff_2026-01-03.csv`. The speed layer does nothing
   special, because it has no idea what a day is.
4. **The watcher notices.** Airflow's `tariff_watcher` DAG lists the landing bucket once
   a real minute. A day counts as complete once the *next* day's file exists, so it now
   triggers `daily_billing` with `sim_date=2026-01-02` and run id `billing__2026-01-02`.
5. **`daily_billing` runs.** It waits for the tariff file and a late-data grace period.
   Then it starts a Spark container that rescans the whole day of Parquet, removes
   duplicates, validates, joins the day's own tariff and computes the bills. In **one
   transaction** it writes all 50 bills and marks the run `success` in `pipeline_runs`.
   After that, SQL checks run, then the zone rollup with its cross-check, then
   reconciliation, then the report.
6. **The merge flips.** The same bill request now returns `source: "batch"` and
   `provisional: false`. The total may have changed, and that difference is the point of
   the demonstration. `/bill/delta` shows both figures and why they differ.

Meanwhile the producer, archiver and speed layer never paused. By the time the bill
flips, they are already well into 2026-01-03.

## 1.8 The merge function

The architecture comes down to this function in
[`api/routers/households.py`](../src/voltstream/api/routers/households.py):

```python
if repositories.is_day_finalised(bill_date):           # a 'success' row in pipeline_runs
    batch = repositories.get_finalised_bill_row(household_id, bill_date)
    if batch is not None:
        return _from_batch(batch)                        # source="batch", provisional=False

speed = repositories.get_running_estimate(household_id, bill_date)
if speed is not None:
    return _from_speed(speed)                            # source="speed", provisional=True

raise HTTPException(404)
```

The batch layer always wins. It saw the late readings the speed layer dropped, it
applied the correct day's tariff, and it ran over a closed, deduplicated input. The
speed estimate is never the better answer. It is only the answer that exists before the
day is billed.

## 1.9 The rules the code follows

Knowing these makes the code much easier to read, because most of the unusual choices
follow from one of them.

1. **Billing arithmetic exists only in `core/`.** The speed layer, the batch layer and
   the API contain no rates and no formulas. The API computes nothing at all.
2. **Rates are data, not config** (D2). Every money value comes from the day's tariff
   file, per household. `config/base.yaml` holds only the kWh block boundaries (60, 120).
   A set of `generator_defaults` exists, and only the reference dropper may read it.
3. **`Decimal` everywhere, rounded half-up per line item** (D5). kWh values are
   `(12,4)` and money values `(12,2)`, in Python, Spark and Postgres alike. A bill may be
   negative; it is never clamped to zero.
4. **The raw archiver never transforms.** No validation, filtering or deduplication, so
   a bug in any later layer can be fixed and the data reprocessed.
5. **Units live in names:** `_sim_minutes`, `_real_seconds` (D3).
6. **One home for each kind of thing.** All SQL is in `storage/repositories.py` (plus the
   streaming and batch writers). All S3 paths are in `storage/objectstore.py`. All config
   reads go through `config.py`. All simulated-time arithmetic goes through `simclock.py`.
   All metrics are defined in `metrics.py`.
7. **Everything is idempotent.** Init scripts, upserts, watcher run ids, the batch
   transaction and reconciliation's delete-then-insert can all be rerun safely.
8. **Money failures are loud.** A missing tariff raises an error. A zone/household total
   mismatch fails the run. SQL checks fail the DAG. A wrong bill is worse than no bill.
9. **Images are isolated.** The app image has no Spark. The Airflow image has neither
   Spark nor the `voltstream` package, and its build fails if either appears.

---

# Part 2 — How it is implemented

## 2.1 Repository layout

```
voltstream/
├── src/voltstream/              the one installable package
│   ├── config.py                typed config loader (the only place config is read)
│   ├── simclock.py              simulated clock (the only place time_scale is applied)
│   ├── logging_setup.py         JSON-lines logger with trace_id propagation
│   ├── metrics.py               the eight Prometheus metrics
│   ├── contracts/               Pydantic models for the Kafka event and the daily CSVs
│   │   └── schemas/             exported JSON Schema, guarded by a drift test
│   ├── core/                    shared billing logic: pure Python spec + Spark expressions
│   ├── simulators/              data sources: roster, curves, faults, producer, dropper
│   ├── streaming/               Spark Structured Streaming: session, source, sinks, jobs
│   ├── batch/                   daily billing, zone rollup, reconciliation
│   ├── storage/                 Postgres pool, object-store paths, every API query
│   └── api/                     FastAPI app, routers, response models
├── airflow/dags/                tariff_watcher, daily_billing (launch containers, check SQL)
├── dashboard/index.html         single-file polling dashboard, served by the API
├── config/                      base.yaml (every tunable), local.yaml (laptop overlay)
├── docker/
│   ├── docker-compose.yml       the whole stack, four tiers
│   ├── images/                  app, spark and airflow Dockerfiles
│   └── init/                    SQL schema + seed, Kafka topics, MinIO buckets
├── scripts/                     report generator, schema exporter, smoke test, voltstream.ps1
├── tests/                       unit, property, consistency, integration
└── docs/                        design, decisions, tasks, assumptions, backlog, LaTeX report
```

The subpackages match the boxes in the architecture diagram one to one. `core/` sits
beside the layer packages rather than inside one, so the sharing of logic between layers
is visible in the directory tree itself.

## 2.2 One package, three images

[`pyproject.toml`](../pyproject.toml) defines one package, `voltstream`, with optional
dependency groups. Each Docker image installs only the groups it needs:

| Image | Installs | Runs |
|---|---|---|
| `voltstream-app` ([Dockerfile](../docker/images/app.Dockerfile)) | `.[api,sim]`: FastAPI, psycopg with pool, boto3, confluent-kafka, numpy. **No PySpark.** | producer, dropper, API, reconciliation |
| `voltstream-spark` ([Dockerfile](../docker/images/spark.Dockerfile)) | `.[spark]`: PySpark 3.5 and psycopg, plus JRE 17, the Kafka, S3A and JDBC JARs, and copies of `config/` and `scripts/` | archiver, speed layer, billing, zone rollup, report |
| `voltstream-airflow` ([Dockerfile](../docker/images/airflow.Dockerfile)) | Airflow 3.0.3 with the docker, amazon and postgres providers. **No `voltstream`, no PySpark**; the build fails if either is present. | Airflow standalone |

Console entry points, from `[project.scripts]`: `voltstream-producer`,
`voltstream-dropper`, `voltstream-archiver`, `voltstream-speed-layer`,
`voltstream-billing`, `voltstream-zone-rollup`, `voltstream-reconcile`, `voltstream-api`.

Two consequences you will notice while reading:

- Modules the app image imports must not import PySpark. That is why `core/money.py` and
  `core/tariff.py` have no Spark import, why `storage/objectstore.py` imports boto3
  lazily (the Spark image has no boto3), and why `storage/postgres.py` imports
  `psycopg_pool` lazily (only the API has it).
- Python is pinned to 3.11 (D1), because PySpark 3.5 is not tested above 3.11.

## 2.3 Configuration

**Files:** [`config/base.yaml`](../config/base.yaml), [`config/local.yaml`](../config/local.yaml),
[`src/voltstream/config.py`](../src/voltstream/config.py)

`get_config()` returns a `VoltstreamConfig`, a tree of Pydantic models. Every section
forbids unknown keys, so a typo in the YAML or in an environment variable fails at load
time. The result is cached with `lru_cache`; tests call `get_config.cache_clear()`.

Load order, later wins:

1. `config/base.yaml`, which holds every tunable. Each key is commented with its unit.
2. `config/<VOLTSTREAM_ENV>.yaml`, deep-merged. Inside Docker `VOLTSTREAM_ENV=docker`,
   and no `docker.yaml` exists, so containers use `base.yaml` as it stands. `local.yaml`
   lowers the load for laptop runs: 10 households, 1-second ticks, DEBUG logging.
3. `.env` is loaded into the environment, without overriding anything already exported.
4. `VOLTSTREAM__SECTION__KEY` environment variables override single leaves. Values are
   parsed as JSON where possible, and an empty value is ignored. Compose uses these to
   set things like `VOLTSTREAM__KAFKA__BOOTSTRAP_SERVERS=kafka:9092`.

| Section | Controls |
|---|---|
| `simulation` | `time_scale`, `emit_interval_seconds`, `households`, `zones`, `epoch_sim`, `anchor_real` |
| `faults` | injection rates and the dropout model |
| `kafka` | bootstrap servers, topic names, partitions, retention |
| `speed_layer` | window, watermark, trigger, output mode |
| `batch` | `late_data_grace_real_seconds` |
| `tariff` | block boundaries and the dropper-only `generator_defaults` |
| `alerts` | thresholds for the future Prometheus rules |
| `postgres`, `minio`, `api`, `observability` | connection defaults, bucket names, ports, log level, Pushgateway URL |

**Secrets never go in YAML.** The Postgres password arrives as
`VOLTSTREAM__POSTGRES__PASSWORD`. S3 credentials arrive as `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` and `AWS_ENDPOINT_URL`, which boto3 and `streaming/session.py`
read directly from the environment.

Exceptions to "only `config.py` reads config": the Airflow DAGs read three values from a
read-only mounted `base.yaml` with `yaml.safe_load`, because the Airflow image does not
contain the package. A few modules also read `os.environ` directly for S3 settings and
the checkpoint directory (backlog R15).

## 2.4 The simulated clock

**File:** [`src/voltstream/simclock.py`](../src/voltstream/simclock.py)

| Function | Does |
|---|---|
| `sim_now()` | current simulated instant |
| `real_to_sim(dt)` / `sim_to_real(dt)` | convert instants |
| `real_duration_to_sim(td)` / `sim_duration_to_real(td)` | scale durations |
| `sim_date_of(ts)` / `sim_hour_of(ts)` | the UTC date and hour of a simulated timestamp |

The process start time is captured once, at import, and used as the fallback anchor.
The module docstring tells the story of an earlier bug: one config field doubled as both
the simulated start and the real anchor, and 265 real days later every partition landed
in the year 2235.

Callers: the producer (to stamp `event_ts`), the dropper (to decide which day's file to
write), the logger (for the `sim_date` log field), the zone-history endpoint (to anchor
its query) and `core/keys.py`. Spark jobs do not call it. They derive `sim_date` from the
`event_ts` column, which is why the Spark session is forced to UTC.

## 2.5 Data contracts

**Files:** [`contracts/events.py`](../src/voltstream/contracts/events.py),
[`contracts/reference.py`](../src/voltstream/contracts/reference.py),
[`contracts/schemas/`](../src/voltstream/contracts/schemas/),
[`docs/architecture/03-data-contracts.md`](architecture/03-data-contracts.md)

**`MeterReading`**, one Kafka message:

| Field | Type | Notes |
|---|---|---|
| `schema_version` | str | `"1.0"` |
| `event_id` | UUID | new on every emission, so it is **not** the deduplication key |
| `trace_id` | UUID | correlation id, also sent as a Kafka header |
| `meter_id`, `household_id`, `grid_zone` | str | `MTR-0012`, `HH-0012`, `ZONE-B` |
| `event_ts` | aware datetime | **simulated** time. Used for windows and dates, never for latency. |
| `consumption_kwh`, `solar_generation_kwh` | Decimal(12,4) | serialised as JSON numbers |
| `voltage` | float | deliberately unused, so the batch job can show column pruning |
| `producer_id` | str | `sim-01` |

The Kafka key is `household_id`. Headers carry `trace_id` and `produced_at`. Latency is
measured from the Kafka record timestamp, never from `event_ts` (task T030).

**`TariffRecord`**, one row of `tariff_D.csv`, ten columns (D2): `household_id`,
`effective_date`, `billing_tier`, `subsidy_flag`, `subsidy_pct`, `fixed_charge`,
`block_1_rate`, `block_2_rate`, `block_3_rate`, `export_rate`. `billing_tier` is a
label only. The fixed charge is billed from its own column.

**`WeatherForecast`**, one row of `weather_D.csv`: `grid_zone`, `forecast_date`,
`cloud_cover_pct`, `temperature_c`, `solar_irradiance_index`. It is written every day,
but nothing uses it yet (backlog R16, R22).

The contracts are frozen. `scripts/export_schemas.py` writes JSON Schema files, and
`tests/unit/test_contracts.py` fails if a model no longer matches its committed schema.
Spark does not use these Pydantic models. It declares an equivalent `StructType` in
`streaming/sources.py`.

## 2.6 The shared core

**Directory:** [`src/voltstream/core/`](../src/voltstream/core/)

`core/` modules are pure: no I/O, no config reads, no clock. mypy checks this package
with strict settings. The package holds the business rules twice, on purpose:

| Rule | Python specification | Spark implementation used in production | What keeps them equal |
|---|---|---|---|
| Solar netting and block tariff | `netting.py`, `tariff.py` | `spark_expr.py`, imported by **both** the speed and batch layers | [`tests/consistency/test_pure_vs_spark.py`](../tests/consistency/test_pure_vs_spark.py) |
| Record validation | `validation.validate()` | `streaming/sources.split_valid_invalid()` | `tests/unit/test_sources.py`, plus `_assert_reason_coverage()`, which raises on every call if a reason is unhandled (its docstring says "import time", which is inaccurate) |
| Deduplication key | `keys.dedup_key()` | `batch/daily_billing.deduplicate()` | `tests/unit/test_daily_billing.py` |
| Parquet partition | `keys.parquet_partition()` | `streaming/raw_archiver.with_partition_columns()` | none |

Why two copies of the tariff logic? The Spark path must use Column expressions, not
Python UDFs, so Catalyst can optimise it and no row is ever serialised out to a Python
worker. The scalar version is the readable specification: small enough to print, and
fast enough to property-test over thousands of inputs. It has exactly one production
caller, `batch/reconciliation.py`.

### `netting.py` — solar self-consumption

```
self_consumed   = min(solar, consumption)
billable_import = consumption − self_consumed
export          = solar − self_consumed
```

No rounding. kWh values in with at most 4 decimal places come out exact.

### `money.py` — precision

The `(precision, scale)` constants `MONEY (12,2)`, `KWH (12,4)`, `PCT (5,2)` and
`DIVERGENCE_PCT (6,3)` match the Postgres columns exactly. `round_money()` rounds half-up
to cents. `spark_expr.py` builds its `DecimalType`s from the same constants.

### `tariff.py` — the executable specification

- `build_blocks(boundaries, rates)` combines the config's kWh edges (structure) with the
  day's per-household rates (money). It is the only place the two meet.
- `energy_charge(import_kwh, blocks)` is a marginal ("slab") tariff. The kWh falling in
  each block are charged at that block's rate. Each line item is rounded to cents, then
  the rounded lines are summed. That order is part of the specification.
- `compute_bill(netting, rates, boundaries)`:

```
energy_charge    = Σ round(kWh_in_block × block_rate)
fixed_charge     = rates.fixed_charge
subsidy_discount = round(energy_charge × subsidy_pct / 100)  if subsidy_flag else 0
export_credit    = round(export_kWh × export_rate)
final_bill       = energy_charge + fixed_charge − subsidy_discount − export_credit
```

**Worked example 1** (the module's doctest). A household uses 60.01 kWh with no solar,
at rates 8.00 / 16.50 / 24.50, a fixed charge of 240.00 and no subsidy:

| Block | kWh | × rate | Charge |
|---|---|---|---|
| block_1 (0–60) | 60.0000 | 8.00 | 480.00 |
| block_2 (60–120) | 0.0100 | 16.50 | 0.165 → **0.17** (half-up) |
| block_3 (120+) | 0 | 24.50 | 0.00 |

Energy charge 480.17, plus the fixed charge 240.00, gives a final bill of **720.17**.
Binary floating point gives 480.16 at this boundary (noted under D1), which is why the
code uses `Decimal` throughout.

**Worked example 2.** A net exporter uses 3.0 kWh and generates 5.0 kWh, with a fixed
charge of 120.00 and an export rate of 18.00. Self-consumed 3.0, import 0, export 2.0.
Energy charge 0.00, export credit 36.00, final bill 120.00 − 36.00 = **84.00**. If the
export credit were larger than the charges, the bill would go negative, and that is
allowed.

### `spark_expr.py` — the production implementation

The same functions as Spark `Column` expressions: `netting_expr`,
`spark_build_blocks`, `energy_charge_expr` and `compute_bill_expr`. Things to notice:

- `F.least` and `F.greatest` stand in for `min` and `max`. Every literal is cast to a
  `DecimalType`. `F.round` rounds half-up; `F.bround` (half-even) is deliberately not
  used.
- Rates are **column references** into the joined tariff DataFrame, never literals,
  because every household has its own rates.
- `tier_breakdown` is built as `to_json(array(struct(...)))` with
  `ignoreNullFields=false`, so the unbounded top block keeps the same JSON shape as the
  others.
- `compute_bill_expr` returns a `BillColumns` named tuple, and the callers `select` the
  columns they need.

### `validation.py` and `keys.py`

`validate()` applies these rules in order and stops at the first failure, returning a
`ValidationResult` rather than raising:

| Reason | Rejects when |
|---|---|
| `null_field` | household, meter, zone or `event_ts` is empty |
| `negative_kwh` | consumption or solar is below zero |
| `unknown_household` | the id is not in the known set |
| `unknown_zone` | the zone is not configured |
| `event_ts_out_of_range` | the timestamp falls outside a generous sanity window |

A voltage outside 180–260 V produces a warning, never a rejection. `REJECTION_REASONS`
is the single vocabulary used by the `rejected_records.reason` column, the metric label
and the Spark implementation.

`keys.py` states that a duplicate is the same `(meter_id, event_ts)`, not the same
`event_id`, because a retransmission gets a new UUID. It also states that the Kafka key
is `household_id` and that Parquet is partitioned by simulated `(sim_date, hour)`.

## 2.7 Simulators

**Directory:** [`src/voltstream/simulators/`](../src/voltstream/simulators/)

**`households.py`**: `household_roster(count, zones)` builds `HH-0001` to `HH-0050`
deterministically. Zones and tiers cycle through their lists, every 4th household is
subsidised, and every 3rd has solar. With 50 households that gives 10 per zone, 16 with
solar and 12 subsidised. `docker/init/postgres/03_seed_households.sql` seeds the database
from the same rule. The simulators call this function rather than querying the database,
because they were built before the storage layer existed.

**`profiles.py`**: consumption and solar as **pure functions** of
`(household_id, event_ts)`.

- Consumption follows a double-humped daily curve, with peaks at 07:30 and 19:30 over a
  low baseline. It is multiplied by a stable per-household factor between 0.7 and 1.3,
  and by noise.
- Solar is zero outside 06:00–18:00, with a cos² bell centred on midday, peaking at
  0.30 kWh per tick. The docstring explains how that figure was calibrated.
- The noise comes from a fresh random generator seeded from a SHA-256 hash of the
  arguments, so replaying a timestamp gives exactly the same reading.

**`faults.py`**: `FaultInjector.apply(reading)` returns a **list**, because a duplicate
produces two readings, a buffered one produces none, and a reconnecting meter flushes
many at once.

| Fault | Rate | Effect | Handled by |
|---|---|---|---|
| null field | 1 % | `household_id = ""` | validation → `null_field` |
| negative value | 0.5 % | a kWh field forced negative | validation → `negative_kwh` |
| unknown household | 0.5 % | `household_id = "HH-9999"` | validation → `unknown_household` |
| out of order | 3 % | `event_ts` moved back 1–30 simulated minutes | watermark (speed); full rescan (batch) |
| duplicate | 2 %, rolled independently | the same reading sent twice | deduplication (batch only, see Part 4) |
| dropout | 0.2 % per meter per tick | the meter goes silent for 30 real s, buffers, then flushes the backlog with the original timestamps | watermark (speed); full rescan (batch) |

The first four faults are mutually exclusive: one random roll picks at most one per
reading. The duplicate roll is applied after them. Dropout is a per-meter state machine
driven by **real** time, because an outage spans many ticks.

**`meter_producer.py`**: every `emit_interval_seconds`, it reads `sim_now()` and builds
a reading for each household. Each reading goes through the injector, and each result is
produced to Kafka with the household as key and the `trace_id` and `produced_at`
headers. The producer writes one log line per tick and increments
`events_produced_total`. SIGTERM flushes the producer before exit.

**`reference_dropper.py`**: at startup it writes **yesterday's** files. This seed exists
so that the speed layer has a "yesterday's tariff" to cost against on the very first
day. After that it polls every five real seconds, and when the simulated date changes it
writes that day's tariff and weather. It is the only reader of `generator_defaults`. On
alternate days it raises `block_2_rate` by 0.50, the one deterministic day-to-day tariff
change. Files are written to a `.tmp` key and then copied into place, so the Airflow
sensor never sees a half-written CSV.

## 2.8 Streaming: raw archiver and speed layer

**Directory:** [`src/voltstream/streaming/`](../src/voltstream/streaming/)

### `session.py`

`build_session(app_name)` is the one `SparkSession` builder for all four Spark jobs. It
sets:

- the session time zone to **UTC**; otherwise one simulated day could be split across
  two partition folders
- **4** shuffle partitions instead of 200, to avoid 200 tiny tasks and files per batch
- the S3A settings for MinIO: path-style access and credentials from the environment

`checkpoint_path(job)` returns a folder on a named Docker volume. A checkpoint is a
query's identity: it holds the committed Kafka offsets. Changing or deleting it makes
the job start over from the earliest offset in the topic.

### `sources.py`

- `METER_READING_SCHEMA` is declared, never inferred, and every field is nullable on
  purpose. `from_json` does not enforce non-null fields anyway, and declaring them
  non-null could let the optimiser remove the very null checks validation depends on.
- `read_meter_stream(spark)` subscribes to `meter.readings` from the earliest offset
  (this applies on the first run only; after that the checkpoint wins). It enables
  `includeHeaders` and `failOnDataLoss`, parses the JSON, and keeps the Kafka metadata
  columns: key, timestamp, partition, offset and headers.
- No `kafka.group.id` is set. Spark assigns partitions itself, so what makes the two
  branches independent is their separate checkpoints, not consumer groups.
- `split_valid_invalid(df, ...)` is the Column-expression version of `validate()`: a
  `when` chain in which the first matching rule wins. It returns `(valid_df,
  invalid_df)`. The invalid rows carry a `reason` and the original record as JSON in
  `payload`.
- `KafkaLagListener` is a `StreamingQueryListener` that works out per-partition lag from
  each query's progress events and sets `voltstream_consumer_lag`.

### `sinks.py`

- `upsert_batch(df, table, conflict_cols)`: Spark's JDBC sink cannot upsert, so the
  micro-batch (a few hundred rows) is collected to the driver and written with
  `INSERT … ON CONFLICT … DO UPDATE` through psycopg `executemany`. It logs a warning
  above 5,000 rows.
- `write_rejected(df, stage)`: writes rejected records to `rejected_records` **and** to
  the DLQ topic through Spark's Kafka sink, and increments
  `records_rejected_total{layer, reason}`.
- `observe_e2e_latency(df, col, layer)`: records wall-clock now minus the Kafka
  timestamp, once per row, into the latency histogram.

### `raw_archiver.py` — the master dataset

```
read_meter_stream → with_partition_columns (ingest_ts, sim_date = to_date(event_ts), hour)
                  → foreachBatch: append Parquet, partitionBy(sim_date, hour), snappy
```

It does nothing else: no validation, no filtering, no deduplication. Partitions come
from `event_ts`, so a late reading still lands in the day it describes. The query runs
on Spark's default trigger, starting the next micro-batch as soon as the last one
finishes. The guarantee is **no loss, duplicates tolerated**: Kafka delivers at least
once, and a file sink commits data and offsets in two separate steps. The Gate 2 restart
test in `docs/assumptions.md` §7 is the evidence.

### `speed_layer.py` — three independent streaming queries

Each query has its own Kafka read and its own checkpoint, so one can fail without taking
the others down. The cost is reading the topic three times, which is cheap at about
7,500 events per simulated day.

| Query | Pipeline | Writes |
|---|---|---|
| `speed_layer_zone` | valid rows → watermark 30 simulated min → group by (15-minute window, zone) → sum consumption and solar, count distinct meters, newest Kafka timestamp → `renewable_ratio = min(solar / consumption, 1)` | `zone_metrics_rt`, the latency histogram, the per-zone renewable gauge |
| `speed_layer_household` | valid rows → watermark → group by (**1-day window**, household) → sum kWh → for each `sim_date` in the batch, load `tariff_(sim_date − 1)` (cached), broadcast join, `compute_bill_expr` | `household_running_rt`, including every bill component and `tariff_source_date` |
| `speed_layer_validation` | raw rows → `split_valid_invalid` → `write_rejected(stage="speed")` | `rejected_records`, DLQ, `events_consumed_total` |

All three use a 10-real-second trigger and `outputMode("update")`. Design points to look
for:

- **Update mode** means each micro-batch upserts the windows it touched. A window becomes
  visible after its first trigger and is revised as late data arrives. The watermark
  only decides when a window becomes final and its state is dropped (D3).
- **A 1-day window rather than a `sim_date` column** gives the per-household state an
  end, so the watermark can evict it after midnight. A plain date column would keep
  state forever.
- **The renewable ratio is capped at 1.0.** A zone at midday can generate more than it
  uses; an uncapped ratio overflowed `NUMERIC(5,4)` and killed the query in testing.
- **Missing tariff:** on day zero, if yesterday's file is absent, the provisional bill
  is skipped rather than invented.

Measured behaviour (`docs/assumptions.md` §2): the 15-minute zone view misses about
**1.16 %** of a day's energy, and the daily household totals miss **0.00 %**. At this
time compression one trigger (48 simulated minutes) is longer than the watermark
(30 simulated minutes), so the trigger, not the watermark, decides what gets dropped.

## 2.9 Storage and the serving schema

**Files:** [`docker/init/postgres/01_schema.sql`](../docker/init/postgres/01_schema.sql),
[`02_indexes.sql`](../docker/init/postgres/02_indexes.sql),
[`storage/`](../src/voltstream/storage/)

### The schema

- `household_running_rt` (speed) and `household_bill_daily` (batch) are **separate
  tables** with the same bill columns. The layers never overwrite each other, and the API
  maps either row onto one response shape.
- `pipeline_runs` is the run ledger. `status` is one of `running`, `success`, `failed`
  or `superseded`. A **partial unique index** on `(sim_date, layer) WHERE status =
  'success'` allows at most one successful run per day, which is what makes "is this day
  finalised?" a safe existence check. A restatement demotes the old `success` row to
  `superseded` in the same transaction that writes the new one.
  `orchestrator_run_id` records which Airflow DAG run produced each row.
- `reconciliation_daily` stores `speed_estimate`, `batch_final`, `abs_divergence`,
  `pct_divergence`, `tariff_effect` and `data_effect` for each household and day.
- Every init file is idempotent (`IF NOT EXISTS`, `ON CONFLICT DO NOTHING`, and a
  `\gexec` trick for `CREATE DATABASE airflow`), because `postgres-init` runs on every
  `compose up`.

### `objectstore.py`

The one place S3 paths are built: `raw_root()`, `raw_partition_path()`,
`landing_tariff_key()`/`landing_tariff_path()`, `landing_weather_key()` and
`archive_tariff_path()`. It returns both `s3a://` paths for Spark and bucket/key pairs
for boto3. It also provides a lazily created boto3 client (path-style), `healthcheck()`,
`get_object_bytes()` and `object_exists()`.

### `postgres.py`

There are two Postgres access paths, kept apart on purpose:

- **The API** uses a `psycopg_pool` of 1 to 8 connections, opened and closed in the
  FastAPI lifespan.
- **Spark drivers** (`streaming/sinks.py`, the batch jobs) open a short-lived connection
  per write. A stalled micro-batch therefore cannot exhaust the API's pool.

`transaction()` yields a cursor. It uses the pool if one is open and a direct connection
otherwise, which lets batch scripts reuse `repositories.py`. Errors are wrapped in
`DatabaseUnavailable`.

### `repositories.py`

Every query the API, reconciliation and the report generator make lives here. The
routers contain no SQL. Queries are parameterised and return typed `NamedTuple`s or
dicts, never positional tuples. The key functions:

| Function | Used for |
|---|---|
| `is_day_finalised(date)` | the merge decision; the `status = 'success'` filter is essential |
| `get_finalised_bill_row` / `get_running_estimate` | the two sides of the merge |
| `get_latest_zone_metrics` | `DISTINCT ON (grid_zone)`, newest window per zone |
| `get_running_estimates_for_day` / `get_batch_bills_for_day` / `replace_reconciliation` | reconciliation's inputs and output (delete then insert, one transaction) |
| `get_zone_daily`, `get_billing_summary`, `get_run_summary`, `get_rejected_for_day`, `get_reconciliation_summary` | the daily report |

## 2.10 Batch layer

**Directory:** [`src/voltstream/batch/`](../src/voltstream/batch/)

### `daily_billing.py` — the authoritative bill

`run(sim_date)`, step by step:

1. Generate a `run_id`, then `_start_run` inserts a `running` row into `pipeline_runs`.
2. `read_day`: `spark.read.parquet(raw_root()).filter(sim_date == D)`. `sim_date` is a
   partition column, so only one day's folders are listed. Only seven columns are
   selected, and `voltage` is left out, so it is never read from storage (column
   pruning).
3. If the day has zero rows, the run fails.
4. `deduplicate`: `row_number()` over `(meter_id, event_ts)` ordered by `ingest_ts`,
   keeping the first arrival, so reruns are deterministic.
5. `split_valid_invalid` → `write_rejected(stage="batch")`.
6. `aggregate_to_daily`: per household, sum consumption and solar and count readings.
   A per-household `duplicates_removed` count is joined on.
7. `read_tariff`: day D's CSV, read with a **declared** Decimal schema (inference would
   produce doubles), keeping for each household the latest row with
   `effective_date ≤ D`.
8. `join_tariff`: a left join, then **raise `MissingTariffError`** if any household has
   no tariff. A plain inner join would silently drop that household's bill.
9. `compute_bills`: `compute_bill_expr` from `core/spark_expr.py`. This file contains no
   arithmetic.
10. `archive_tariff`: write the tariff that was used to `voltstream-archive` as Parquet,
    so the rates stay recoverable for a later restatement.
11. `finalise`: in **one transaction**, demote any earlier `success` row to
    `superseded`, upsert all bills, and mark this run `success`. The merge function
    flips at the moment this transaction commits, never partway through.
12. Record `batch_duration_seconds` and push metrics. The push does nothing until a
    Pushgateway is configured.

On any error the run is marked `failed`, the exception is re-raised, and the process
exits with code 1, which is the DAG's signal to retry. The file has no watermark
anywhere, because the day is already closed.

### `daily_zone_rollup.py`

It reuses `read_day` and `deduplicate`, validates, then computes per-zone daily totals:
netting per reading, the peak 15-minute window via `max_by`, and the capped renewable
ratio. **Before writing**, `cross_check` compares Σ zone consumption with Σ
`household_bill_daily.consumption_kwh` for the day. If they differ by more than
0.01 kWh it raises `CrossCheckFailed`. Both are aggregations of the same readings, so a
mismatch means a pipeline defect, and publishing an inconsistent report would be worse
than publishing none. On success it upserts `zone_metrics_daily`.

### `reconciliation.py` — the system checking its own divergence

This is plain Python on the app image, with no Spark, because 50 rows do not need a
cluster. For each household present in both views:

```
S = household_running_rt.estimated_bill        speed kWh × yesterday's tariff
B = household_bill_daily.final_bill            batch kWh × today's tariff
C = compute_bill(net(speed kWh), today's tariff)          ← core/tariff.py

tariff_effect = S − C      what costing against yesterday's tariff did
data_effect   = C − B      what seeing different readings did
S − B = tariff_effect + data_effect                       (exact, in Decimal)
```

`C` differs from `S` only in the tariff and from `B` only in the kWh. That is what makes
the split an attribution rather than two numbers that happen to add up, and it only
holds because the consistency test proves the Python and Spark implementations agree.

`pct_divergence = 100 × |S − B| / (B.energy_charge + B.fixed_charge)`. The base is the
gross charges, not `final_bill`, which can be near zero or negative for exporters (D5).
Values that overflow the column are capped and logged. The job refuses to run for a day
that is not finalised. It reads today's tariff from the landing CSV and validates every
row through `TariffRecord`, so after a correction it uses the corrected file. It replaces
the day's rows, sets the `voltstream_lambda_divergence` gauge to the mean percentage, and
pushes metrics.

## 2.11 Orchestration with Airflow

**Directory:** [`airflow/dags/`](../airflow/dags/) · decision D6

**The problem:** Airflow schedules on real time, but a simulated day lasts five real
minutes, so no cron expression can mean "once per simulated day". The solution is to
watch for files instead of a clock.

### `tariff_watcher_dag.py`

This DAG runs every real minute, with at most one run active. It:

1. Lists `voltstream-landing/tariff/` and parses the dates out of the file names.
2. **Drops the newest date.** The dropper writes a day's file at the *start* of that
   day, so a day is only known to be complete once the next day's file exists.
3. Calls `TriggerDagRunOperator.partial(...).expand_kwargs(...)` with run id
   `billing__<date>`, `conf={"sim_date": date}` and `skip_when_already_exists=True`.

Offering every date every minute is safe, because Airflow skips run ids that already
exist. There is no "last seen" state to get out of step with the bucket.

### `daily_billing_dag.py`

It has no schedule (only the watcher triggers it), takes one parameter `sim_date`, and
allows one active run at a time. Its tasks:

```
wait_for_tariff        S3KeySensor, object size > 0, poke every 15 s
  → wait_late_data_grace   sleep until the tariff file's LastModified + 90 real s
  → run_daily_billing      DockerOperator, spark image, spark-submit daily_billing.py (retries 2)
  → verify_row_count       SQLValueCheck: exactly 50 bills for the day
    verify_bill_sanity     SQLCheck: bills exist, no nulls, readings > 0, no absurd totals, no future tariff
  → run_zone_rollup        DockerOperator, spark image (no retries: a failed cross-check is a verdict)
  → run_reconciliation     DockerOperator, app image, voltstream-reconcile (retries 2)
  → generate_report        DockerOperator, spark image, scripts/generate_report.py
```

The containers start through `docker-socket-proxy` (`tcp://docker-socket-proxy:2375`) on
the `voltstream` network, with `mount_tmp_dir=False` and `auto_remove="force"`.
Credentials are passed as `private_environment` so they do not appear in task logs.
Airflow reaches MinIO and Postgres through connections defined as environment variables
(`AIRFLOW_CONN_MINIO_S3`, `AIRFLOW_CONN_VOLTSTREAM_PG`).

**Restating a day:** trigger `daily_billing` again for the same `sim_date` with a new run
id, for example `billing__2026-01-02__r2`. The billing job supersedes the previous
`success` row, and both runs stay visible in the Airflow UI and in `pipeline_runs`.
Airflow's built-in `backfill` cannot address simulated dates, so it is not used.
`scripts/backfill.sh` is meant to wrap this but is still empty.

## 2.12 API and dashboard

**Directory:** [`src/voltstream/api/`](../src/voltstream/api/), [`dashboard/index.html`](../dashboard/index.html)

`main.create_app()` builds the app:

- The lifespan opens and closes the Postgres pool, so the container is not reported
  healthy before it can actually serve requests.
- It includes five routers.
- `/metrics` returns the shared Prometheus registry directly. The
  `prometheus-fastapi-instrumentator` package was removed because it breaks on newer
  FastAPI versions.
- `dashboard/` is mounted as static files at `/`, last, so it does not shadow the API
  routes. The dashboard is served from the same origin as the API, so no CORS
  configuration is needed.

`dependencies.trace_id_provider` takes the caller's `X-Trace-Id` header or generates one,
echoes it in the response, and binds it for every log line written during the request.

| Endpoint | Reads | Notes |
|---|---|---|
| `GET /api/v1/zones`, `/api/v1/zones/load` | newest window per zone | what the dashboard polls |
| `GET /api/v1/zones/{zone}/history?minutes=N` | `zone_metrics_rt` range | `minutes` is simulated, anchored at `sim_now()`; 404 if empty |
| `GET /api/v1/households/{id}/bill?date=D` | `pipeline_runs`, then batch, else speed | **the merge function** |
| `GET /api/v1/households/{id}/bill/delta?date=D` | both bill tables + `reconciliation_daily` | estimate vs final, delta, tariff and data effects |
| `GET /api/v1/reports/daily?date=D` | report queries | returns a report for an open day too, with `finalised` and `completeness` flags |
| `GET /api/v1/alerts/status` | Alertmanager `/api/v2/alerts` | returns `available: false` instead of a 5xx when Alertmanager is down |
| `GET /health/live` | nothing | liveness never checks dependencies |
| `GET /health/ready` | Postgres `SELECT 1`, MinIO `head_bucket` | 503 naming the failing dependency |
| `GET /metrics`, `/docs`, `/` | | Prometheus, OpenAPI, dashboard |

`models.py` defines **one** `BillResponse` for both sides of the merge. A provisional
and a final bill have the same keys. The client learns which one it received from
`source` and `provisional`; the four batch-only fields are `null` while provisional.

The dashboard is plain HTML and JavaScript with no build step. Every five seconds it
redraws the zone table, with a bar for each renewable ratio, and a bill panel for a
chosen household and date. The panel's badge flips from **Provisional (speed)** to
**Final (batch)** when the day is finalised. The date field defaults to the newest
simulated day.

## 2.13 Observability

**Logging** ([`logging_setup.py`](../src/voltstream/logging_setup.py)):
`get_logger(service)` writes one JSON object per line to stdout, with the fields `ts`,
`level`, `service`, `stage`, `trace_id`, `sim_date` and `msg`, plus any `extra=` fields.
`trace_id` lives in a `contextvars` variable set by `bind_trace_id(...)`, so it does not
have to be passed through every function. `sim_date` is filled in from the simulated
clock.

**Tracing** uses correlation ids rather than distributed spans, a stated limitation. The
producer generates a `trace_id`, which travels in the Kafka header and payload, into the
Parquet column, into `rejected_records` and the DLQ, and into every log line. Filtering
on one id reconstructs a record's path.

**Metrics** ([`metrics.py`](../src/voltstream/metrics.py)) are all defined in this one
module, on one registry:

| Metric | Type | Labels | Set by |
|---|---|---|---|
| `voltstream_events_produced_total` | Counter | `producer_id` | producer |
| `voltstream_events_consumed_total` | Counter | `layer` | archiver, speed validation query |
| `voltstream_records_rejected_total` | Counter | `layer`, `reason` | `sinks.write_rejected` |
| `voltstream_e2e_latency_seconds` | Histogram | `layer` | archiver after its write; speed layer at the zone sink |
| `voltstream_consumer_lag` | Gauge | `layer`, `partition` | `KafkaLagListener` |
| `voltstream_zone_renewable_ratio` | Gauge | `grid_zone` | speed zone sink |
| `voltstream_batch_duration_seconds` | Histogram | `job` | billing, rollup (pushed) |
| `voltstream_lambda_divergence` | Gauge | none | reconciliation (pushed) |

Processes without a web server call `start_metrics_server()` on port 8001. The host maps
the archiver to 8011 and the speed layer to 8012. The API serves `/metrics` itself.
One-shot batch containers call `push_metrics()`, which does nothing while
`observability.pushgateway_url` is null.

**Not built yet:** Prometheus, Grafana, Alertmanager and the Pushgateway (Phase 12).
`config/prometheus/`, `config/alertmanager/` and `config/grafana/` contain only
`.gitkeep` files, and Compose does not include these services. Until they exist,
`/api/v1/alerts/status` reports `available: false`.

## 2.14 Infrastructure and tooling

### Compose ([`docker/docker-compose.yml`](../docker/docker-compose.yml))

`name: voltstream` fixes the project name (D7). Services start in four tiers, each
waiting for the previous one through `depends_on` health conditions. Nothing uses
`sleep`.

| Tier | Services | Ready when |
|---|---|---|
| 1 Infrastructure | `kafka` (apache/kafka 3.8.0, single-node KRaft), `postgres` (16-alpine), `minio` (`pgsty/silo`, pinned) | healthchecks pass |
| 2 Bootstrap, one-shot | `postgres-init` (runs every `docker/init/postgres/*.sql` in order), `kafka-init` (`--if-not-exists`), `minio-init` (`mc mb --ignore-existing`) | exit 0 |
| 3 Applications | `meter-producer`, `reference-dropper`, `raw-archiver`, `speed-layer`, `api` | API: `/health/ready` |
| 4 Orchestration | `docker-socket-proxy`, `airflow` (`standalone`, LocalExecutor) | started |

| Port | Service |
|---|---|
| 8000 | API, `/docs` and the dashboard |
| 8080 | Airflow UI (the generated password is in the container; `voltstream.ps1 status` prints it) |
| 9000 / 9001 | MinIO API / console (`voltstream` / `voltstream-dev`) |
| 29092 | Kafka, from the host |
| 5432 | Postgres |
| 8011 / 8012 | archiver / speed-layer metrics |

### Makefile

| Target | Does |
|---|---|
| `make up` | runs `anchor` (stamps `VOLTSTREAM_ANCHOR_REAL` into `.env`), then `docker compose up -d --wait` |
| `make demo` | `up`, waits 420 s (one simulated day plus margin), then prints zone load |
| `make down` / `make clean` | stop / stop and **delete volumes**, including checkpoints |
| `make test` / `make test-all` | tests without / with integration tests |
| `make lint` | ruff check and format check, mypy, and a grep for the misspelling `volstream` (D7) |
| `make logs s=<service>` | follow one service's logs |
| `make faults`, `make backfill d=…` | call scripts that are still empty |

### Scripts

- [`scripts/voltstream.ps1`](../scripts/voltstream.ps1) is the Windows counterpart of the
  Makefile: `run`, `build`, `start`, `stop`, `clean`, `status`, `check`, `bill` and
  `logs`. `check` verifies every stage end to end: containers, the shared clock, Kafka,
  both Spark jobs, Parquet, Postgres, Airflow and the merge function. `start` keeps the
  old clock anchor when the stack already holds data, so simulated time does not restart
  at 2026-01-01 over days that are already billed.
- [`scripts/smoke_test.sh`](../scripts/smoke_test.sh) is the Gate 1 infrastructure test
  for tiers 1–2.
- [`scripts/generate_report.py`](../scripts/generate_report.py) renders the daily
  Markdown report from `repositories.py`.
- [`scripts/export_schemas.py`](../scripts/export_schemas.py) regenerates the contract
  JSON Schemas.

## 2.15 Tests

**Directory:** [`tests/`](../tests/). `conftest.py` provides one session-wide local
`SparkSession` and sets `PYSPARK_PYTHON`, which fixes Spark workers on Windows (D1).

| Folder | What it proves | Needs |
|---|---|---|
| `unit/` (17 files) | config loading, the simulated clock, logging, metrics, the contract drift guard, netting, tariff, validation, keys, profiles, faults, that `split_valid_invalid` agrees with `validate()`, the speed-layer aggregations and bill, billing dedup and the effective-dated tariff rule, reconciliation arithmetic with hand-worked expectations | local Spark for some |
| `property/` | Hypothesis invariants: the netting identities, no negative components except `final_bill`, the bill rising with consumption and not rising with solar, continuity at block boundaries | nothing |
| `consistency/` | `test_pure_vs_spark.py`: 5,000 random rows plus sweeps across each boundary and half-cent rounding ties, with **exact** equality between `tariff.py` and `spark_expr.py` | local Spark |
| `integration/` | archiver restart (no loss), the merge function's five cases (speed only, finalised, failed run, superseded plus success, neither), the repository SQL, watermark behaviour | the running stack; marked `integration` |

Run `make test`, or `.venv/Scripts/python.exe -m pytest -m "not integration"`.

---

# Part 3 — Where to start reading

## 3.1 First, about 30 minutes of documents

1. [README](../README.md): the service list and quickstart.
2. [Master design](architecture/00-master-design.md) §2 (the problem), §3 (the solution),
   §4 (Lambda vs Kappa) and §8 (how it runs). Skip §9, which is the project schedule.
3. [Decision log](architecture/05-open-decisions.md): read the status table, then **D2**
   (rates are data), **D3** (watermark units, and the measurement that overturned its
   prediction) and **D4** (who computes the provisional bill). These three shape more
   code than the others.

## 3.2 Recommended reading order

The order goes vocabulary → rules → data sources → the two layers → serving. Each step
only needs what the earlier steps covered.

| # | Read | What to look for |
|---|---|---|
| 1 | [`config/base.yaml`](../config/base.yaml) → [`config.py`](../src/voltstream/config.py) | every tunable; which units are simulated and which real; the load order |
| 2 | [`simclock.py`](../src/voltstream/simclock.py) | the formula and why there are two anchors |
| 3 | [`contracts/events.py`](../src/voltstream/contracts/events.py), [`contracts/reference.py`](../src/voltstream/contracts/reference.py), [`01_schema.sql`](../docker/init/postgres/01_schema.sql) | the shape of a reading, a tariff row and each table, and which layer owns which table |
| 4 | [`core/netting.py`](../src/voltstream/core/netting.py) → [`core/money.py`](../src/voltstream/core/money.py) → [`core/tariff.py`](../src/voltstream/core/tariff.py) | the whole billing rule, in about 60 lines of arithmetic |
| 5 | [`core/spark_expr.py`](../src/voltstream/core/spark_expr.py) next to `tariff.py`, then [`test_pure_vs_spark.py`](../tests/consistency/test_pure_vs_spark.py) | the same rule line for line as Column expressions, and how the test ties them together |
| 6 | [`core/validation.py`](../src/voltstream/core/validation.py), [`core/keys.py`](../src/voltstream/core/keys.py) | the rejection vocabulary; why `event_id` is not the dedup key |
| 7 | [`simulators/households.py`](../src/voltstream/simulators/households.py) → [`profiles.py`](../src/voltstream/simulators/profiles.py) → [`faults.py`](../src/voltstream/simulators/faults.py) → [`meter_producer.py`](../src/voltstream/simulators/meter_producer.py) → [`reference_dropper.py`](../src/voltstream/simulators/reference_dropper.py) | where the data comes from; each fault type and what should catch it |
| 8 | [`streaming/session.py`](../src/voltstream/streaming/session.py) → [`sources.py`](../src/voltstream/streaming/sources.py) → [`sinks.py`](../src/voltstream/streaming/sinks.py) | Spark setup, the declared schema, the validation split, upserts and the DLQ |
| 9 | [`streaming/raw_archiver.py`](../src/voltstream/streaming/raw_archiver.py) | the whole master dataset, and why it is so short |
| 10 | [`streaming/speed_layer.py`](../src/voltstream/streaming/speed_layer.py) | three queries, watermark, windows, update mode, yesterday's tariff |
| 11 | [`storage/objectstore.py`](../src/voltstream/storage/objectstore.py) → [`postgres.py`](../src/voltstream/storage/postgres.py) → [`repositories.py`](../src/voltstream/storage/repositories.py) | path conventions, the two Postgres access paths, every query |
| 12 | [`batch/daily_billing.py`](../src/voltstream/batch/daily_billing.py) | partition and column pruning, dedup, the loud tariff join, the one-transaction finalise |
| 13 | [`batch/daily_zone_rollup.py`](../src/voltstream/batch/daily_zone_rollup.py) → [`batch/reconciliation.py`](../src/voltstream/batch/reconciliation.py) | the cross-check gate; the S / B / C decomposition |
| 14 | [`airflow/dags/tariff_watcher_dag.py`](../airflow/dags/tariff_watcher_dag.py) → [`daily_billing_dag.py`](../airflow/dags/daily_billing_dag.py) | how simulated days become DAG runs; the task chain |
| 15 | [`api/main.py`](../src/voltstream/api/main.py) → [`dependencies.py`](../src/voltstream/api/dependencies.py) → [`routers/households.py`](../src/voltstream/api/routers/households.py) → the other routers → [`models.py`](../src/voltstream/api/models.py) → [`dashboard/index.html`](../dashboard/index.html) | the merge function, and how the architecture shows up in the product |
| 16 | [`logging_setup.py`](../src/voltstream/logging_setup.py), [`metrics.py`](../src/voltstream/metrics.py) | the log envelope, trace-id binding, the eight metrics |
| 17 | [`docker-compose.yml`](../docker/docker-compose.yml), the Dockerfiles, [`Makefile`](../Makefile) | how it all starts and in what order |
| 18 | [`assumptions.md`](assumptions.md), [`debugging-backlog.md`](debugging-backlog.md) | what is simplified, what was measured, what is known to be wrong |

**With only an hour:** read `base.yaml`, `simclock.py`, `core/tariff.py`,
`core/spark_expr.py`, `streaming/speed_layer.py`, `batch/daily_billing.py`,
`api/routers/households.py` and `airflow/dags/daily_billing_dag.py`. Those eight files
cover the architecture.

## 3.3 Or follow one reading through the system

Following one reading with a debugger mindset is a good second pass. Take household
`HH-0012`: zone B, tier 3, subsidised and with solar, on simulated day 2026-01-02.

1. **Created.** `meter_producer._run_tick` → `_build_reading` (profiles give the kWh) →
   `FaultInjector.apply` → `_produce_reading`. The message goes to Kafka with key
   `HH-0012` and a `trace_id` header.
2. **Archived.** `raw_archiver`: `read_meter_stream` parses it, `with_partition_columns`
   derives `sim_date=2026-01-02` and the hour, and `_write_batch` appends it to
   `voltstream-raw/meter_readings/sim_date=2026-01-02/hour=HH/part-*.snappy.parquet`.
3. **Validated.** In the `speed_layer_validation` query, `split_valid_invalid` either
   passes the reading or writes it to `rejected_records` and the DLQ.
4. **Aggregated live.** In the `speed_layer_zone` query it joins the ZONE-B window that
   contains its `event_ts` → `zone_metrics_rt`. In the `speed_layer_household` query it
   joins HH-0012's running total → costed against `tariff_2026-01-01.csv` →
   `household_running_rt`.
5. **Served provisionally.** `GET /api/v1/households/HH-0012/bill?date=2026-01-02` →
   `is_day_finalised` is false → `get_running_estimate` → `source: "speed"`.
6. **Billed.** The dropper writes the 2026-01-03 file → `tariff_watcher` triggers
   `billing__2026-01-02` → `daily_billing.read_day` finds the reading in Parquet →
   `deduplicate` → `split_valid_invalid` → `aggregate_to_daily` → `join_tariff` with
   `tariff_2026-01-02.csv` → `compute_bill_expr` → `finalise`.
7. **Served finally.** The same request now returns `source: "batch"`. `reconciliation`
   has split any difference into `tariff_effect` and `data_effect`, visible at
   `/bill/delta`.
8. **Traced.** Search the container logs for the reading's `trace_id` to see its path.

## 3.4 Reading tips

- **Read the docstrings.** Module and function docstrings are long on purpose. They
  explain *why* a choice was made and what failure it prevents, and they are the design
  rationale kept next to the code.
- **Reference codes:** `T042` is a task in [Implementation_Tasks.md](Implementation_Tasks.md),
  `D3` a decision in [05-open-decisions.md](architecture/05-open-decisions.md), `§8.3` a
  section of the [master design](architecture/00-master-design.md), and `R02` an item in
  the [debugging backlog](debugging-backlog.md).
- **`ponytail:` comments** mark a known shortcut together with its upgrade path.
- **Invariants you can check with grep:** no SQL under `api/routers/`; no
  `generator_defaults` outside `simulators/`; no `core.tariff` or `core.netting` import
  under `api/`; no `volstream` anywhere in identifiers.
- **Two implementations of one rule are always deliberate** and always named in the
  docstring, together with the test that keeps them in step.

---

# Part 4 — What is built, what is not, known issues

**Built (task phases 0–11):** everything in Part 2. That covers configuration, the
clock, contracts, `core/` with its tests, the simulators, the infrastructure tiers, the
raw archiver, the speed layer, storage and the API, the batch layer and Airflow
orchestration, the merge function with reports and dashboard, and reconciliation.

**Not built yet (phases 12–15):**

| Missing | Where it would go |
|---|---|
| Prometheus, Grafana, Alertmanager, Pushgateway, and the five alert rules | `config/prometheus/`, `config/alertmanager/`, `config/grafana/` (only `.gitkeep` so far); Compose services |
| Pipeline watchdog DAG | [`airflow/dags/pipeline_watchdog_dag.py`](../airflow/dags/pipeline_watchdog_dag.py) is empty |
| Demo, backfill and fault-injection scripts | [`scripts/demo.sh`](../scripts/demo.sh), [`backfill.sh`](../scripts/backfill.sh) and [`inject_faults.sh`](../scripts/inject_faults.sh) are empty, so `make faults` and `make backfill` do nothing yet |
| CI workflow | `.github/workflows/` holds only `.gitkeep` |
| Architecture docs 01, 02 and 04 | named in master design §7.2; not written |

**Known issues to keep in mind while reading.** These come from
[debugging-backlog.md](debugging-backlog.md), which tracks the status of each item. At
the time of writing R06, R07 and R21 are marked fixed.

| ID | Issue | Effect |
|---|---|---|
| R01 | The only day-to-day tariff change is in block 2, which starts at 60 kWh, and simulated households use at most about 22 kWh a day | the stale-tariff effect is always 0.00 |
| R02 | The speed layer does not deduplicate; the batch layer does | provisional totals run about 2 % high; `data_effect` is positive |
| R03 | Day D's tariff is written at the *start* of D; the watcher's "skip the newest date" rule compensates | the 90 s late-data grace never actually waits |
| R04 | The day-zero seed file (the day before the epoch) triggers a billing run over a day with no readings | one failed run on every cold start |
| R08 | `generate_report` writes into an auto-removed container with no volume | the report file is lost |
| R24 | The zone rollup nets solar per reading, while bills net per household-day | zone self-consumption ≠ Σ bill self-consumption |
| R26 | `_boundaries()` is copied in `speed_layer.py` and `daily_billing.py` instead of using `TariffConfig.boundaries()` | a small drift risk outside `core/` |
| D3 correction | The 15-minute view loses about 1.16 % of energy, mostly from reordering rather than dropouts | the opposite of D3's original prediction; see `assumptions.md` §2 |

---

# Glossary

| Term | Meaning here |
|---|---|
| **Lambda architecture** | Two processing paths (speed and batch) over one immutable input, merged at serving time |
| **Speed layer** | `streaming/speed_layer.py`: live, approximate, drops late data |
| **Batch layer** | `batch/`: recomputes a closed day from the master dataset; authoritative |
| **Serving layer** | Postgres plus the API |
| **Master dataset** | `voltstream-raw` Parquet: every raw reading, append-only, never modified |
| **Merge function** | `get_bill`: batch if the day is finalised, speed otherwise, always labelled |
| **Finalised** | the day has a `success` row in `pipeline_runs` for `batch_billing` |
| **Provisional** | a speed-layer figure, costed against yesterday's tariff |
| **Restatement** | re-running billing for a past day; the old run becomes `superseded` |
| **Simulated time** | the clock that runs 288 times faster than real time; see §1.6 |
| **Anchor** | the real instant that maps to `epoch_sim`, shared by every container |
| **Watermark** | the speed layer's limit on lateness: windows older than (newest `event_ts` − 30 simulated min) are closed |
| **Tumbling window** | a fixed, non-overlapping time bucket (15 simulated min for zones, 1 day for households) |
| **Trigger / micro-batch** | Spark processes the stream in batches, one every 10 real seconds |
| **`foreachBatch`** | Spark hook that hands each micro-batch to Python code; used for upserts |
| **Checkpoint** | a streaming query's saved offsets and state; it defines where the query resumes |
| **Upsert** | `INSERT … ON CONFLICT DO UPDATE` |
| **DLQ** | dead-letter queue: `meter.readings.dlq`, where rejected records go |
| **Netting** | offsetting solar generation against consumption: self-consumed, import, export |
| **Block (slab) tariff** | kWh in each band charged at that band's rate |
| **Effective-dated join** | pick the latest tariff row with `effective_date ≤ day` |
| **`tariff_effect` / `data_effect`** | the two parts of the speed-vs-batch divergence, from reconciliation |
