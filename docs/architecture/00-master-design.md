# voltstream — Master Design Document

**Smart Grid Energy Monitoring & Billing Platform**
Applied Big Data Engineering — Mini Project (EC8203, 2026)
Architecture: **Lambda** | Use Case: **3 — Smart Grid Energy Monitoring & Billing**

---

## Document purpose

This is the single source of truth for the project. Every other written artefact is
derived from it:

| Derived artefact | Sections used |
|---|---|
| `README.md` | 7, 8, 9 |
| Report chapter: architecture decision | 2, 4 |
| Report chapter: tech stack | 5 |
| Report chapter: observability | 10.1 |
| Report chapter: limitations | 10.2 |
| Viva preparation | 10.3 |

Written before implementation begins, and updated as decisions change. If the code
and this document disagree, one of them is a bug.

---

## Table of contents

1. [Project brief](#1-project-brief)
2. [Problem statement](#2-problem-statement)
3. [Solution overview](#3-solution-overview)
4. [Architecture decision: Lambda vs Kappa](#4-architecture-decision-lambda-vs-kappa)
5. [Technology stack and justification](#5-technology-stack-and-justification)
6. [Solution architecture](#6-solution-architecture)
7. [Repository structure](#7-repository-structure)
8. [How the pipeline works at runtime](#8-how-the-pipeline-works-at-runtime)
9. [Build phases](#9-build-phases)
10. [Observability, limitations and viva preparation](#10-observability-limitations-and-viva-preparation)

---

## 1. Project brief

### 1.1 What this system is

`voltstream` is an end-to-end data platform for a utility company operating a smart
electricity grid with distributed rooftop solar. It ingests two sources — a
continuous stream of smart-meter readings and a once-daily tariff/weather reference
file — and serves two consumers whose requirements are in direct opposition:

- **Grid operations** need approximate load and renewable-mix figures per zone
  within seconds, to make ramping and load-shedding decisions.
- **Billing** needs exact, auditable, restatable per-household bills, and can wait
  until the day closes.

The system is built on a **Lambda architecture**: a speed layer serving provisional
real-time estimates, a batch layer producing authoritative finalised results from an
immutable master dataset, and a serving layer that merges the two.

### 1.2 Why this use case was chosen

Three use cases were offered. Use Case 3 was selected because it produces the
sharpest architecture argument, which is where the marks concentrate.

| Criterion | UC1 Ride-hailing | UC2 Hospital vitals | UC3 Smart grid |
|---|---|---|---|
| Latency requirement split | Muddy — both paths want real-time | Hard real-time + soft daily | **Crisp: seconds for ops, daily for money** |
| Replay requirement | Weak | Moderate (clinical audit) | **Strong (tariff corrections, meter backfill, disputes)** |
| Correctness requirement | Moderate | Fuzzy — risk scores are heuristic | **Absolute — billing is money, must be auditable** |
| Simulator complexity | High (geospatial, routing, trip state) | Low | **Low (deterministic curves + noise)** |
| Transformation depth | Sum of costs | Clinical scoring | **Tiered tariff + solar netting** |

**Rejected — UC1 (ride-hailing).** Realistic GPS simulation (route generation, zone
polygons, trip state machines) consumes days of a two-week budget and earns marks in
none of the rubric rows. The architecture argument is also weak: nothing in the use
case genuinely *demands* a batch layer.

**Rejected — UC2 (hospital vitals).** The strongest runner-up, and notably it has the
better *Kappa* story: vitals are natively an event log, labs are just a slower
stream, and in a clinical-safety context a dual codebase is an actual hazard — a
patient flagged by the speed layer but not the batch layer is a real risk. It was
rejected because "patient risk score" has no ground truth, making the
15-mark *correctness of transformation logic* criterion much harder to evidence.

**The decisive advantage of UC3:** the architecture becomes a **user-facing feature**.
A household's bill shows a provisional speed-layer estimate during the day and a
finalised batch-layer figure after day close, with the delta between them visible.
That is literally `query = merge(batch_view, realtime_view)` rendered as product
behaviour, not just described in prose.

### 1.3 The mark allocation shapes the design

| Criterion | Marks | Design consequence |
|---|---|---|
| Architecture decision & justification | 20 | Written on day 1, *before* code. Section 4 is the deliverable. |
| Data ingestion implementation | 15 | Graded on *robustness* — hence deliberate fault injection. |
| Processing layer implementation | 15 | Shared pure transformation module + consistency test. |
| Report | 15 | Assembled from this document; written continuously, not at the end. |
| Technology stack justification | 10 | Every choice names a rejected alternative and a use-case constraint. |
| Storage & serving layer | 10 | The merge function is the centrepiece. |
| Observability | 10 | Alerts must be *demonstrated firing*, not merely configured. |
| Code quality & documentation | 5 | `src/` layout, typed config, CI, `make demo`. |

**30 of 100 marks are for reasoning, not code.** A technically flawless pipeline with
a weak architecture chapter scores worse than a modest pipeline with a rigorous one.

The brief also states that every member must "explain and defend every architectural
decision and every line of core pipeline logic in a viva/demo." The deliverable is
therefore **a defended argument, evidenced by working code** — not code with an
argument attached afterwards.

---

## 2. Problem statement

### 2.1 The domain problem

Electricity has a property that makes it unlike almost any other commodity: it cannot
be meaningfully stored at grid scale. **Supply must match demand continuously.** If
demand exceeds supply, grid frequency falls, and the operator must either ramp
generation or shed load.

Distributed rooftop solar makes this harder rather than easier. Solar is generation
the utility does not control and cannot dispatch, and its output swings fast — a
cloud bank crossing a zone can cut solar contribution substantially within a minute.
The grid must instantly absorb that shortfall from thermal or hydro plant that takes
minutes to ramp. So *"renewable contribution by zone, right now"* is not a
decorative dashboard metric. It is an operational alarm that triggers a physical
control action, and it is worthless if it arrives ten minutes late.

Simultaneously, a completely different department must bill those same households
from those same meter readings. Billing is not `consumption × rate`. It is a
**piecewise function**:

- **Block tariffs** charge different rates for different consumption bands.
- **Subsidies** apply to qualifying households.
- **Net metering** credits exported solar against imported grid energy.
- **Fixed charges** vary by tier.

Get it wrong and the utility has a regulatory problem, not a data problem.

### 2.2 The engineering problem

One input stream. Two consumers with **opposite requirements**:

| Dimension | Grid operations | Billing |
|---|---|---|
| Answer needed within | Seconds | End of day |
| Effect of a 2% error | None — doesn't change a ramping decision | Legally actionable |
| Yesterday's answer | Disposable | Reproducible for years |
| Late-arriving data | Ignore it | Must be included |
| Restatement | Never | Months later, on demand |
| Auditability | None | Full lineage to source readings |

**This table is the architecture decision.** One workload wants low latency and
tolerates approximation; the other wants exactness, reproducibility and unbounded
replay. No single processing path optimises both. Everything downstream — Lambda over
Kappa, the master dataset, two views, the merge function — is a consequence of these
two columns not fitting in one system.

### 2.3 The generalisable pattern

The brief's requirement of "one streaming source plus one daily file" is not an
artificial constraint. It is the most common shape in production data engineering:

> **Fast facts joined to slow dimensions.**

Telemetry, clicks, transactions, sensor readings — continuous. Reference data —
customer records, price lists, regulatory rates, partner extracts — arrives on a
*business* cadence, typically as a file dropped overnight by someone else's ETL.
Almost every pipeline you will build is a version of this join. The assignment
teaches that pattern wearing a smart-grid costume.

### 2.4 What the business questions actually demand

| Business question | Serving requirement |
|---|---|
| "What is current grid load and renewable contribution by zone?" | Sub-minute, approximate, per-zone aggregate, continuously refreshed |
| "What will each household's bill be once daily tariff data is applied?" | Exact, per-household, available after day close, restatable |

Note the second question's phrasing: *"once daily tariff data is applied."* The
question itself encodes a dependency on data that does not exist until the day ends.
No amount of streaming cleverness removes that dependency — it is a property of the
business process, not of the technology.

### 2.5 Non-functional requirements

| Requirement | Target | Rationale |
|---|---|---|
| Speed-layer end-to-end latency | < 60 s (event time → API) | Grid ramping decision window |
| Batch completion after day close | < 10 real minutes | Demo tractability |
| No data loss on any single component failure | Zero | Kafka retention + immutable master dataset |
| Reproducibility | `git clone` → `make demo` → running | Explicitly graded |
| Restatement horizon | Unbounded (limited only by object storage) | Billing dispute window exceeds Kafka retention |

---

## 3. Solution overview

### 3.1 The three views

Lambda's structure is three views over one immutable input.

**Master dataset (immutable, append-only).**
Every raw meter event, unmodified, written to Parquet on object storage, partitioned
by `sim_date` and `hour`. Daily reference files archived alongside. Nothing is ever
updated in place. This is the ground truth from which the batch layer recomputes, and
it is what makes replay possible after Kafka's retention window expires.

**Speed view (approximate, low latency).**
15-simulated-minute event-time tumbling windows per `grid_zone`: total consumption,
total solar generation, renewable ratio, active meter count. Plus a running
per-household kWh total costed against *yesterday's* tariff — deliberately stale,
deliberately labelled provisional. Written to Postgres via `foreachBatch` upsert.

**Batch view (exact, authoritative).**
Triggered by Airflow when the day's tariff file lands. Reads the complete day's
Parquet partition, deduplicates, validates, joins the effective-dated tariff and
weather dimensions, applies netting and block-tariff logic, writes finalised rows and
marks the day closed.

### 3.2 The merge function — the centrepiece

```
GET /api/v1/households/{id}/bill?date=YYYY-MM-DD

    if a finalised row exists in household_bill_daily for (id, date):
        return that row, source="batch", provisional=false
    else:
        return the speed-layer estimate, source="speed", provisional=true
```

Nine lines of logic, and it is the physical embodiment of
`query = merge(batch_view, realtime_view)`.

**Why batch wins unconditionally.** The batch layer is strictly better-informed in
every respect: it saw the late-arriving events the speed layer dropped past its
watermark, it applied the correct day's tariff rather than yesterday's stale one, and
it ran deterministically over a closed input set. There is no case in which the speed
estimate is more correct. The rule is therefore simply *batch if finalised, speed
otherwise, and always label which one was served.*

**Report evidence.** A screenshot of the same household before and after
finalisation, showing the delta between provisional and final, proves comprehension
of Lambda more convincingly than several pages of prose.

### 3.3 Domain modelling decisions

Four decisions do most of the work. Each is graded under *correctness of
transformation logic* and each must be defensible in the viva.

**(a) Event time, not processing time.**
Meter readings carry their own `event_ts`. Network buffering means they arrive out of
order. All windowing uses event time with a watermark.

*Why it matters:* if you window on arrival time, a network hiccup silently moves a
reading into the wrong window and daily totals stop reconciling. Since the meter
stamps the reading, use that stamp. Anything else makes your numbers a function of
your infrastructure's mood.

**(b) Interval readings, not cumulative registers.**

> **Limitation (state in report §Limitations):** Real meters report a *cumulative*
> register value, not an interval delta. We simulate interval kWh directly.
> Production would require per-meter stateful diffing plus meter-reset and rollover
> detection. Naming this simplification scores better than hiding it.

**(c) Solar netting — the non-trivial transformation.**

```
self_consumed   = min(solar_kwh, consumption_kwh)
billable_import = consumption_kwh - self_consumed
export_kwh      = solar_kwh - self_consumed
```

`billable_import` is then charged against a **block tariff** (bands with different
rates, a tier-dependent fixed charge, and a subsidy discount for flagged households).
`export_kwh` is credited at a net-metering rate.

This is genuinely tiered arithmetic with real edge cases at band boundaries — ideal
for unit and property testing, and it mirrors how block tariffs actually work in
practice.

**(d) Idempotency.**
Kafka provides at-least-once delivery. The deduplication key is
`(meter_id, event_ts)`. The Kafka **message key is `household_id`**, which achieves
two things at once: per-household ordering (needed for dedup and any stateful
per-meter logic) and co-partitioning with the tariff join key, so the join is local
rather than a shuffle.

### 3.4 The simulated clock

```
1 simulated day = 5 real minutes
TIME_SCALE = 288
```

| Quantity | Value |
|---|---|
| Producer emit interval | 2 real seconds |
| Equivalent simulated interval | ~9.6 simulated minutes |
| Readings per meter per simulated day | 144 |
| Households simulated | 50 |
| Grid zones | 5 |
| Raw events per simulated day | ~7,200 |
| Zone metric rows per simulated day (96 windows × 5 zones) | ~480 |
| Household bill rows per simulated day | 50 |

All event timestamps and all windowing use **simulated** time. Only ingestion-latency
metrics use wall-clock time. `src/voltstream/simclock.py` is the single source of
truth for this conversion — no module computes simulated time independently.

> **Limitation:** time compression scales *event* time but not *processing* time.
> Network latency, GC pauses and micro-batch intervals run at wall-clock speed.
> Watermark tuning is therefore not realistic — in production, watermarks are set
> against observed lateness distributions measured in real time.

---

## 4. Architecture decision: Lambda vs Kappa

> This section is the 20-mark deliverable. It is written first because it constrains
> every subsequent decision.

### 4.1 The two architectures

**Lambda** maintains two processing paths over the same immutable input. A batch
layer recomputes authoritative results from all historical data; a speed layer
computes approximate results from recent data; a serving layer merges them.
`query = merge(batch_view, realtime_view)`.

**Kappa** maintains one processing path. Everything is a stream. Correction is
achieved by replaying the log from an earlier offset through the same code.

### 4.2 The decision

**Lambda is selected.**

The decision follows directly from the requirements table in §2.2, evaluated against
the four criteria the rubric names:

| Criterion | Analysis | Verdict |
|---|---|---|
| **Latency** | Two consumers with requirements two orders of magnitude apart: seconds for grid ops, hours for billing. A single path must be tuned for one of them, penalising the other. Tuning for exactness (long watermarks, complete-day state) makes the ops view unusably late; tuning for latency makes billing wrong. | **Lambda** |
| **Replay** | Billing disputes and retroactive tariff corrections have a restatement horizon measured in **months**. Kafka's practical retention is measured in **days**. Kappa's correction mechanism — replay from offset zero — is unavailable beyond retention. Lambda's batch layer replays from object storage, which is unbounded. | **Lambda** |
| **Cost** | Lambda pays for two codepaths and a nightly compute burst. Kappa pays for either extended Kafka retention (expensive at scale, and still bounded) or very large long-lived streaming state to hold per-household tiered billing accumulators across the restatement horizon. At our data volumes both are affordable; at production volumes Kappa's state cost grows with `households × horizon`, which is the worse curve. | **Lambda, narrowly** |
| **Consistency** | Billing requires an immutable, auditable ledger independent of the message bus, with lineage from final amount back to source readings. Lambda's master dataset provides exactly this. Kappa's ground truth is the log itself, which is a transport system with a retention policy — an uncomfortable foundation for a financial audit trail. | **Lambda, decisively** |

### 4.3 The rejected alternative: Kappa

Kappa is a genuinely viable design here and must be presented as such — dismissing it
weakly loses marks.

**How Kappa would work.** Model tariffs as a **compacted Kafka topic** keyed by
`household_id`, so the latest tariff per household is always retained. Run a single
Spark Structured Streaming job performing a stream-stream join between meter readings
and tariff updates. Corrections are made by deploying fixed code and replaying from
an earlier offset into a new output table, then atomically switching readers to it.

**Kappa's real advantages.**
- One codebase. No possibility of speed/batch drift — Lambda's canonical weakness.
- Simpler operationally: one job to deploy, monitor and reason about.
- Conceptually cleaner: batch is just a bounded stream.

**Why it is rejected here.**

1. **Retention horizon mismatch.** This is the decisive reason. Billing restatement
   must be possible months after the fact. Kafka retention is days. Kappa's
   correction mechanism is therefore unavailable exactly when billing most needs it.
2. **Audit independence.** Finance requires a durable ledger that does not depend on
   the availability or configuration of the message bus. A retention policy change
   should not be able to destroy the billing audit trail.
3. **State cost and fragility.** Holding per-household tiered billing accumulators as
   streaming state across a months-long horizon is expensive and operationally
   fragile compared with a bounded nightly job over a fixed input partition.

### 4.4 Lambda's cost, and how we mitigate it structurally

**The cost is real and must be stated honestly.** Lambda requires the same business
logic in two codepaths, which can silently drift.

**The concrete failure this causes:** if the speed layer believes the first tariff
block ends at 60 kWh and the batch layer believes 61 kWh, a household sitting near
that boundary sees one figure in the app during the day and a different figure on the
final bill — and calls the helpline. The bug is invisible in testing unless you
specifically test the boundary.

**Our mitigation is structural, not procedural.**

1. **A shared pure module.** `src/voltstream/core/` contains the netting, tariff and
   validation logic as pure functions — no I/O, no clock reads, no database calls.
   Both the streaming job and the batch job import it. It is a **peer** of the layer
   packages, not nested inside either, so the sharing is visible in the directory
   listing.
2. **A consistency test that pins the two implementations together.**
   `core/tariff.py` holds scalar Python functions; `core/spark_expr.py` holds the
   same logic as Spark `Column` expressions. This duplication is deliberate and
   necessary — the FastAPI container has no Spark but still computes provisional
   estimates, and the Spark path must use Column expressions rather than UDFs to
   preserve Catalyst optimisation and avoid per-row serialisation cost.
   `tests/consistency/test_pure_vs_spark.py` generates several thousand random inputs,
   runs both implementations, and asserts identical output. **That test is the proof
   that the duplication cannot silently diverge.**

*Viva answer:* "We didn't mitigate drift by being careful. We made drift a test
failure."

### 4.5 The contingency — what would change our decision

The honest closing position, and the strongest form of the argument:

> Our choice of Lambda is contingent on one specific constraint: **the master dataset
> must outlive Kafka's retention window**. Adopt Kafka tiered storage (offloading old
> segments to object storage) or a table format such as Apache Iceberg or Delta Lake
> (giving the log ACID semantics, time travel and unbounded queryable history), and
> that constraint disappears. At that point Kappa's single codebase becomes the
> better trade, and we would migrate.

Showing that the decision is *contingent and falsifiable* — and naming precisely the
condition that would flip it — demonstrates architectural judgement rather than
allegiance.

---

## 5. Technology stack and justification

Each layer states: what problem the layer solves, what the tool mechanically does,
the use-case constraint it satisfies, and what was rejected and why.

### 5.1 Summary table

| Layer | Chosen | Constraint satisfied | Rejected |
|---|---|---|---|
| Ingestion | **Apache Kafka** | Replayable log is a *precondition* for Lambda; independent multi-consumer reads enable the two-branch fan-out | RabbitMQ, direct-to-DB writes |
| Stream processing | **Spark Structured Streaming** | Unified batch/stream API enables the shared `core/` module — structurally mitigating Lambda's drift weakness; native event-time watermarking | Apache Storm, Apache Flink |
| Master dataset | **Parquet on MinIO** | Columnar, compressed, immutable, partition-pruned full-day rescans; unbounded retention decoupled from compute | HDFS, raw JSON/CSV, database storage |
| Serving store | **PostgreSQL** | Relational joins, ACID for financial writes, indexed point lookups; write volume is trivial post-aggregation | Cassandra, MongoDB |
| Orchestration | **Apache Airflow** | Sensors, retries, SLA alerts, and `backfill` — which *is* the Lambda restatement mechanism, demonstrable live | cron, manual scripts |
| Observability | **Prometheus + Grafana + Alertmanager** | Pull-based scraping fits Compose; alert rules are declarative, version-controlled, reviewable | Bespoke log-grepping, ELK |
| API | **FastAPI** | Async, auto-generated OpenAPI docs (free demo artefact), Pydantic schema enforcement at the boundary | Flask, Django REST |

### 5.2 Ingestion — Apache Kafka

**What it mechanically is.** Not a queue. Kafka is a **distributed append-only log**.
Producers append messages to a *partition*; each message receives a monotonically
increasing *offset*. Consumers track their own offset independently. **Reading does
not delete** — the message remains until the retention period expires.

That single mechanical difference drives three properties this project depends on:

- **Replay.** If the speed layer had a bug for six hours, reset the consumer offset
  and reprocess. With a queue, an acknowledged message is gone forever.
- **Decoupling and backpressure.** The producer neither knows nor cares whether the
  consumer is slow, restarting or dead. Kafka absorbs the burst. If the simulator
  wrote directly to Postgres and Postgres restarted, those events would be lost.
- **Independent multi-consumer reads.** The speed layer and the raw archiver read the
  same topic in separate **consumer groups**, at their own pace, with separate
  offsets. Neither affects the other. *This is what makes the two-branch Lambda shape
  possible from a single source.*

**Partitions and keys.** The message key (`household_id`) is hashed to select a
partition. All events for one household therefore land in the same partition, and
Kafka guarantees ordering *within* a partition. This gives per-household ordering —
needed for deduplication — without requiring (or providing) global ordering, which we
do not need. Partition count also sets the parallelism ceiling: consumers within a
group can never exceed partition count. We use **3 partitions**.

**Retention** defaults to approximately 7 days. **This single number is the mechanical
reason Lambda beats Kappa here** (§4.2).

**Rejected — RabbitMQ.** Queue semantics: messages are removed on acknowledgement, so
there is no replay and no independent multi-consumer reading of the same data. Both
are load-bearing requirements.

**Rejected — writing directly to the database.** No buffering, no backpressure
absorption, no replay, and the producer's availability becomes coupled to the
database's.

### 5.3 Stream processing — Spark Structured Streaming

**What it mechanically is.** A micro-batch engine that models a stream as an
*unbounded table*. New events are new rows. You write the same DataFrame code you
would write against a static table, and Spark incrementally maintains the result.

**Why this specific engine, for this specific architecture.** Because Spark's batch
and streaming APIs are the *same* DataFrame API, the billing logic can live in one
shared module imported by both jobs (§4.4). We are not mitigating Lambda's canonical
weakness rhetorically — we are structurally eliminating that class of bug. **No other
candidate engine offers this.**

**Event-time watermarking.** A watermark is the system's stated belief about how late
data can possibly arrive. With a 30-second watermark, Spark holds a window open 30
seconds past its end before finalising and discarding its state.

The trade-off, which must be stated:

> **Shorter watermark → lower latency, more late data dropped.
> Longer watermark → more complete results, higher latency, more state held in memory.**

The speed layer chooses *"low latency, drop stragglers."* The batch layer needs **no
watermark at all**, because it rescans a complete, closed day. That divergence is not
an inconsistency — it is Lambda working exactly as intended, with each layer tuned
for its own consumer.

**Checkpointing** durably records committed offsets and in-flight state, so a restart
resumes precisely where it stopped rather than reprocessing or skipping.

**Rejected — Apache Storm.** Storm processes tuple-at-a-time and achieves genuinely
lower latency (single-digit milliseconds versus Spark's hundreds). But it has no
native event-time windowing or watermarks — you hand-roll them — and no batch API to
share code with. Our latency requirement is *seconds*, so Storm's single advantage is
irrelevant to us while both its costs are real.

**Rejected — Apache Flink (state this honestly).** Flink is the better pure streaming
engine: true event-at-a-time processing, a superior state backend, stronger
end-to-end exactly-once semantics. It is rejected on grounds specific to *this
project*: no unified batch story in the same idiom for code sharing, a steeper
learning curve within a two-week budget, and a latency requirement Spark already
satisfies comfortably.

> Stating "Flink is technically superior but wrong for our constraints" scores
> considerably higher than pretending Spark wins on merit.

### 5.4 Master dataset storage — Parquet on MinIO

#### Why a master dataset is needed at all

Lambda's founding premise is `query = function(all data)`. You cannot compute a
function of all data if you discarded the data. Five concrete reasons, ordered by how
often they bite in practice:

1. **Bug recovery.** You discover in week three that a tier-boundary condition used
   `>` instead of `>=`. Every bill for three weeks is wrong at the boundary. With raw
   inputs retained, fix the function and backfill. Without them, there is no way to
   compute the right answer — the wrong answer is all that exists.
2. **Late and corrected data.** A meter was offline for six hours and backfills. A
   regulator retroactively adjusts a tariff. The speed layer already emitted figures
   based on incomplete inputs. Reprocessing from raw is the only clean remedy.
3. **Questions not yet asked.** Someone requests peak 15-minute demand per household
   for last month — a metric never computed. **Raw data is a superset of every
   aggregate you might ever want; an aggregate is a superset of nothing.** This is
   the reason that generalises beyond this project.
4. **Audit.** Billing must be traceable from the final amount back to the specific
   readings that produced it.
5. **Kafka expires.** The mechanical reason the master dataset cannot simply be "the
   topic."

The defining property is **immutability**: append-only, never updated in place. That
is what makes reprocessing deterministic — re-running yesterday's job over
yesterday's partition produces byte-identical output. That determinism *is* the audit
story.

#### Why Parquet specifically

CSV and JSON are **row-oriented** — all of record 1, then all of record 2. Parquet is
**columnar** — all values of `consumption_kwh`, then all values of `solar_generation_kwh`.
That layout change yields four benefits:

- **Column pruning.** The billing job needs four columns; the raw schema has a dozen
  (`meter_id`, `voltage`, `trace_id`, `producer_id`, `schema_version`, …). A columnar
  reader physically reads only the required columns' bytes. On a wide schema this is
  routinely a 5–10× I/O reduction, for free.
- **Compression that works.** Within a column, values are homogeneous — same type,
  similar magnitude. `grid_zone` has 5 distinct values across millions of rows, so
  dictionary encoding stores it as tiny integers plus a 5-entry lookup. Timestamps
  compress via delta encoding. Files land roughly 5–10× smaller than equivalent CSV.
- **Embedded schema and real types.** CSV has no types — everything is a string and
  every reader re-guesses. Is `01` a number or a string? Is that timestamp UTC?
  Parquet stores the schema in the file footer, so `event_ts` returns as a timestamp,
  not a hopeful `strptime`. This is a **correctness** benefit, not merely convenience.
- **Predicate pushdown.** Parquet stores min/max statistics per column per row group,
  so a filter on `event_ts` skips entire row groups without decompressing them.

**Partitioning multiplies this.** Writing to `sim_date=2026-08-10/hour=14/` means the
daily job's directory listing alone eliminates every other day — those files are
never opened.

> **Limitation:** Parquet is immutable-friendly and therefore poor at row-level
> updates and deletes — changing one row means rewriting the file. This aligns
> perfectly with an append-only master dataset, so it is not a problem here.
> Separately, **streaming writes produce many small files**, which degrades read
> performance; production requires a compaction job. Both belong in §Limitations.

#### Why MinIO rather than AWS S3

Object storage is unambiguously the correct *category* for a master dataset: cheap
per GB, extremely durable, effectively unlimited, and — most importantly —
**decoupled from compute**. The Spark cluster can be destroyed and recreated without
touching the data. That separation is the defining property of modern data platforms.

For this project, **MinIO runs in Docker Compose** rather than using real AWS S3.
MinIO implements the S3 API, so the code uses `s3a://` paths and the same S3A
connector or boto3 client it would use against AWS. Migrating to real S3 later is a
change of endpoint and credentials — nothing structural.

Why that is the better call here:

- **Reproducibility is explicitly graded.** A grader clones the repo and runs
  `docker compose up`. No AWS account, no credit card, no IAM policy, no region
  configuration.
- **No cost and no surprises.** Nobody leaves a bucket running.
- **No network dependency during the demo.** Campus wifi cannot ruin the presentation.

**Rejected — a plain local folder.** It works, and it is a legitimate fallback under
time pressure. But it forfeits the object-store semantics claimed in the report and
reads as less considered.

**Rejected — HDFS.** NameNode operational overhead is unjustified at this scale, and
it reintroduces the compute/storage coupling that object storage removes.

> **Update (2026-09-26, decision D8):** MinIO Inc. withdrew its community distribution
> between October 2025 and September 2026. It stopped publishing images, archived the
> repository, and deleted its Docker Hub repositories, so `quay.io/minio/*` and `minio/*`
> no longer pull. The stack now runs `pgsty/silo`, a community-maintained fork of MinIO with
> the same S3 API, pinned to a release tag. SeaweedFS is the planned long-term replacement.
> The swap changed two image lines and no code: this section's argument that moving store is
> "a change of endpoint and credentials", tested for real.

Two caveats worth a paragraph in the report, because they demonstrate genuine
understanding:

- **The commit problem.** Object stores have no atomic rename. Spark's default
  file-commit protocol writes to a temporary path and renames on success — which on
  S3 is a slow copy, not an atomic operation. Production uses the S3A magic committer
  or a table format that handles commits properly.
- **Consistency history.** S3 has offered strong read-after-write consistency since
  late 2020; before that, eventual consistency caused genuine data-loss bugs in Spark
  pipelines. Knowing this history is exactly the detail that lands well in a viva.

### 5.5 Serving store — PostgreSQL

**What "serving layer" means.** The store the *reader* hits. It is optimised for the
query pattern, not for ingestion.

Our read patterns are: point lookup by `(household_id, sim_date)`; aggregate over
recent windows grouped by zone; join billing rows to tariff rows. Small result sets,
relational shape, tens to hundreds of households.

Postgres provides SQL joins, **ACID transactions** (a batch billing run either fully
lands or does not — for money, that matters), B-tree indexes, `JSONB` for the
per-tier breakdown, and `ON CONFLICT DO UPDATE` for idempotent upserts.

**Rejected — Cassandra.** Superb at massive write throughput and time-series at
scale — but you query by partition key only, there are no joins, no ad-hoc
aggregation, and it is eventually consistent. You would denormalise a separate table
per query pattern. Our write volume is a few hundred rows per simulated day.
Choosing Cassandra here optimises for a problem we do not have — precisely the
"generic popularity" reasoning the rubric penalises.

**Rejected — MongoDB.** Document model adds nothing over `JSONB`, and we lose join
capability that the serving queries genuinely use.

### 5.6 Orchestration — Apache Airflow

**What orchestration means.** Something must decide *when* the batch job runs, what it
depends on, and what happens when it fails at 02:00.

Airflow provides a DAG of tasks with explicit dependencies, **sensors** (wait until
the tariff file appears), **retries with exponential backoff**, **SLA alerts** (this
should have completed by now), and — most importantly here — **backfill**.

```bash
airflow dags backfill --start-date 2026-08-01 --end-date 2026-08-10 daily_billing
```

**That command *is* the Lambda restatement mechanism**, and it can be demonstrated
live in thirty seconds: deliberately corrupt a tariff file, run the DAG, show
incorrect bills, fix the file, backfill, show corrected bills. That single demo
defends the entire architecture chapter.

**Rejected — cron.** No dependency graph, no retry semantics, no record of which runs
succeeded, no backfill, no SLA alerting. Cron is a timer; Airflow is a control plane.

**Design note:** our Airflow DAGs are deliberately **thin**. They contain no billing
logic — they submit Spark jobs and verify results. Two payoffs: Airflow's notoriously
constrained dependency set never has to coexist with PySpark and FastAPI in one
image, and the architecture stays honest — Airflow orchestrates, Spark computes.

### 5.7 Observability — Prometheus, Grafana, Alertmanager

Prometheus uses a **pull** model: it scrapes a `/metrics` HTTP endpoint on each
service on a schedule. Pull fits containerised environments — services merely expose
an endpoint, and Prometheus discovers them; nothing needs to know where the
monitoring system lives.

Metrics are time series with **labels**, so a single `consumption_kwh_total` metric
labelled by `grid_zone` supports per-zone slicing without defining five metrics.

Alertmanager consumes rules from a file in the repository — **alerts as
version-controlled, reviewable code**, which is directly what the rubric asks for.

> **Limitation to state honestly:** Spark *executors* are awkward to scrape because
> they are transient. Our pragmatic approach emits metrics from inside `foreachBatch`
> on the driver. Naming this is worth more than pretending it is solved.

### 5.8 API — FastAPI

Async by default; auto-generated OpenAPI documentation (a free and genuinely
impressive demo artefact); Pydantic response models that enforce the output schema at
the boundary (directly relevant to code-quality marks); one-line Prometheus
instrumentation via `prometheus-fastapi-instrumentator`.

**Rejected — Flask.** Synchronous by default, no built-in schema validation, no
generated docs.

---

## 6. Solution architecture

### 6.1 Layer view

```
┌─────────────────────────────────────────────────────────────────────┐
│ SOURCES (simulated)                                                 │
│                                                                     │
│  meter-producer                       reference-dropper             │
│  50 households, every 2s              1 file per simulated day      │
│  + deliberate fault injection         tariff + weather              │
└───────────┬─────────────────────────────────────┬───────────────────┘
            │ JSON, key=household_id              │ CSV
            ▼                                     ▼
┌───────────────────────────┐         ┌───────────────────────────────┐
│ KAFKA                     │         │ MinIO — landing zone          │
│ topic: meter.readings     │         │ voltstream-landing/           │
│ 3 partitions, ~7d retain  │         │   tariff_YYYY-MM-DD.csv       │
│ topic: meter.readings.dlq │         │   weather_YYYY-MM-DD.csv      │
└─────┬─────────────────┬───┘         └───────────────┬───────────────┘
      │                 │                             │
      │ group:          │ group:                      │
      │ speed-consumer  │ archive-consumer            │
      ▼                 ▼                             │
┌─────────────────┐  ┌──────────────────────────┐     │
│ SPEED LAYER     │  │ RAW ARCHIVER             │     │
│ validate        │  │ NO transformation        │     │
│ 15-min windows  │  │ append-only              │     │
│ watermark 30s   │  │                          │     │
│ zone aggregates │  │                          │     │
│ provisional est.│  │                          │     │
└────────┬────────┘  └────────────┬─────────────┘     │
         │                        │ Parquet           │
         │                        ▼                   │
         │            ┌──────────────────────────┐    │
         │            │ MinIO — MASTER DATASET   │    │
         │            │ voltstream-raw/          │    │
         │            │   sim_date=/hour=/       │◄───┘
         │            │ IMMUTABLE, APPEND-ONLY   │
         │            └────────────┬─────────────┘
         │                         │ full-day rescan
         │                         ▼
         │            ┌──────────────────────────┐
         │            │ BATCH LAYER   (Airflow)  │
         │            │ dedup → validate → join  │
         │            │ netting → block tariff   │
         │            │ + reconciliation metric  │
         │            └────────────┬─────────────┘
         │                         │
         ▼                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│ POSTGRESQL — serving layer                                          │
│  zone_metrics_rt      (speed)   │  household_bill_daily   (batch)   │
│  household_running_rt (speed)   │  zone_metrics_daily     (batch)   │
│  rejected_records     (both)    │  pipeline_runs          (batch)   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ SELECT only
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│ FASTAPI — merge layer                                               │
│  /zones/load  /households/{id}/bill  /reports/daily  /health        │
│  MERGE: batch if finalised, else speed (labelled provisional)       │
└──────────┬──────────────────────────────────────┬───────────────────┘
           │ polled every 5s                      │
           ▼                                      ▼
   Live dashboard                        Daily report file
```

**Observability spans every stage:** structured JSON logs with a propagated
`trace_id`, Prometheus metrics scraped from each service, five alert rules in
Alertmanager, Grafana dashboards over both Prometheus and Postgres.

### 6.2 Data contracts

Contracts are **frozen at the end of Phase 1** and changed only by explicit agreement,
because three people build against them in parallel.

**Streaming event — `meter.readings`**

```json
{
  "schema_version": "1.0",
  "event_id":       "uuid4",
  "trace_id":       "uuid4",
  "meter_id":       "MTR-0042",
  "household_id":   "HH-0042",
  "grid_zone":      "ZONE-C",
  "event_ts":       "2026-08-10T14:23:00Z",
  "consumption_kwh": 0.412,
  "solar_generation_kwh": 0.180,
  "voltage": 232.4,
  "producer_id": "sim-01"
}
```

| Field | Purpose |
|---|---|
| `schema_version` | Enables forward-compatible evolution |
| `event_id` | Idempotency; secondary dedup safety net |
| `trace_id` | Propagated end-to-end — our pragmatic tracing (§10.1) |
| `event_ts` | **Simulated** time — drives all windowing |
| `voltage` | Deliberately unused by billing — demonstrates Parquet column pruning |

Kafka message key: `household_id`. Value: JSON, UTF-8.

**Daily reference — `tariff_YYYY-MM-DD.csv`**

```csv
household_id,tariff_rate,billing_tier,subsidy_flag,fixed_charge,export_rate,effective_date
HH-0042,24.50,TIER_2,false,240.00,18.00,2026-08-10
```

**Daily reference — `weather_YYYY-MM-DD.csv`**

```csv
grid_zone,forecast_date,cloud_cover_pct,temperature_c,solar_irradiance_index
ZONE-C,2026-08-10,35,31.2,0.78
```

### 6.3 Object storage layout

```
voltstream-raw/                       ← MASTER DATASET (immutable)
  meter_readings/
    sim_date=2026-08-10/
      hour=00/  part-*.snappy.parquet
      hour=01/  ...

voltstream-landing/                   ← daily drops (as received)
  tariff/   tariff_2026-08-10.csv
  weather/  weather_2026-08-10.csv

voltstream-archive/                   ← processed reference, immutable
  tariff/  sim_date=2026-08-10/tariff.parquet
  reports/ report_2026-08-10.md       ← daily report (T131); a restatement replaces it

voltstream-checkpoints/               ← Spark checkpoints (see §8.5)
  speed_layer/
  raw_archiver/
```

### 6.4 Postgres schema (essentials)

```sql
-- SPEED VIEW: zone metrics, upserted every micro-batch
CREATE TABLE zone_metrics_rt (
    grid_zone            TEXT        NOT NULL,
    window_start         TIMESTAMPTZ NOT NULL,
    window_end           TIMESTAMPTZ NOT NULL,
    total_consumption_kwh NUMERIC(12,4) NOT NULL,
    total_solar_kwh      NUMERIC(12,4) NOT NULL,
    renewable_ratio      NUMERIC(5,4)  NOT NULL,
    active_meters        INTEGER       NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (grid_zone, window_start)
);
CREATE INDEX idx_zone_metrics_rt_window ON zone_metrics_rt (window_start DESC);

-- SPEED VIEW: per-household running total, provisional
CREATE TABLE household_running_rt (
    household_id        TEXT        NOT NULL,
    sim_date            DATE        NOT NULL,
    consumption_kwh     NUMERIC(12,4) NOT NULL,
    solar_kwh           NUMERIC(12,4) NOT NULL,
    billable_import_kwh NUMERIC(12,4) NOT NULL,
    export_kwh          NUMERIC(12,4) NOT NULL,
    estimated_bill      NUMERIC(12,2) NOT NULL,
    tariff_source_date  DATE        NOT NULL,  -- yesterday's: deliberately stale
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (household_id, sim_date)
);

-- BATCH VIEW: authoritative bill
CREATE TABLE household_bill_daily (
    household_id        TEXT        NOT NULL,
    sim_date            DATE        NOT NULL,
    consumption_kwh     NUMERIC(12,4) NOT NULL,
    solar_kwh           NUMERIC(12,4) NOT NULL,
    self_consumed_kwh   NUMERIC(12,4) NOT NULL,
    billable_import_kwh NUMERIC(12,4) NOT NULL,
    export_kwh          NUMERIC(12,4) NOT NULL,
    energy_charge       NUMERIC(12,2) NOT NULL,
    fixed_charge        NUMERIC(12,2) NOT NULL,
    subsidy_discount    NUMERIC(12,2) NOT NULL,
    export_credit       NUMERIC(12,2) NOT NULL,
    final_bill          NUMERIC(12,2) NOT NULL,
    tier_breakdown      JSONB       NOT NULL,  -- per-block kWh and charge
    tariff_effective_date DATE      NOT NULL,
    readings_count      INTEGER     NOT NULL,
    duplicates_removed  INTEGER     NOT NULL,
    is_finalised        BOOLEAN     NOT NULL DEFAULT true,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    pipeline_run_id     UUID        NOT NULL,  -- lineage
    PRIMARY KEY (household_id, sim_date)
);

-- Dead-letter records: evidence for observability marks
CREATE TABLE rejected_records (
    id            BIGSERIAL PRIMARY KEY,
    stage         TEXT        NOT NULL,   -- 'speed' | 'batch'
    reason        TEXT        NOT NULL,   -- 'schema' | 'null_key' | 'negative_kwh' | ...
    trace_id      TEXT,
    raw_payload   JSONB       NOT NULL,
    rejected_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_rejected_at ON rejected_records (rejected_at DESC);

-- Run ledger: lineage and the finalisation flag the merge function reads
CREATE TABLE pipeline_runs (
    run_id          UUID PRIMARY KEY,
    sim_date        DATE NOT NULL,
    layer           TEXT NOT NULL,        -- 'batch_billing' | 'batch_rollup'
    status          TEXT NOT NULL,        -- 'running' | 'success' | 'failed'
    rows_in         BIGINT,
    rows_out        BIGINT,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ
);
CREATE UNIQUE INDEX idx_runs_date_layer_success
    ON pipeline_runs (sim_date, layer) WHERE status = 'success';

-- Reconciliation: the system monitoring its own Lambda divergence
CREATE TABLE reconciliation_daily (
    household_id    TEXT NOT NULL,
    sim_date        DATE NOT NULL,
    speed_estimate  NUMERIC(12,2) NOT NULL,
    batch_final     NUMERIC(12,2) NOT NULL,
    abs_divergence  NUMERIC(12,2) NOT NULL,
    pct_divergence  NUMERIC(6,3)  NOT NULL,
    PRIMARY KEY (household_id, sim_date)
);
```

**Note the design intent:** `household_bill_daily` and `household_running_rt` are
**separate tables**. The two layers never overwrite one another, and every row
carries unambiguous provenance. `pipeline_runs` is what the merge function queries to
decide whether a day is finalised.

---

## 7. Repository structure

### 7.1 Repository name

**`voltstream`** — short, memorable, valid as a Python package name
(`import voltstream`), and signals both domain (electricity) and architecture
(streaming) without being a description.

*Alternatives considered:* `gridpulse` (leans on real-time monitoring),
`meterflow` (leans on ingestion). Rejected `smartgrid-lambda-pipeline` — reads like a
folder name, not a project.

**Use the same name for the repository, the Python package and the Compose project.**
One name everywhere removes an entire class of confusion.

### 7.2 The tree

```
voltstream/
├── README.md
├── LICENSE
├── Makefile
├── pyproject.toml
├── .env.example
├── .gitignore
├── .pre-commit-config.yaml
├── .github/workflows/ci.yml
│
├── docs/
│   ├── architecture/
│   │   ├── 00-master-design.md          # THIS DOCUMENT
│   │   ├── 01-lambda-vs-kappa.md        # extracted from §4
│   │   ├── 02-tech-stack.md             # extracted from §5
│   │   ├── 03-data-contracts.md         # extracted from §6.2
│   │   ├── 04-observability.md          # extracted from §10.1
│   │   └── diagrams/                    # .drawio sources + .png exports
│   ├── runbook.md                       # how to demo, how to break it on purpose
│   ├── assumptions.md                   # simulated clock, simplifications
│   └── report/                          # working files for the PDF
│
├── docker/
│   ├── docker-compose.yml
│   ├── docker-compose.override.yml      # dev-only mounts
│   ├── images/
│   │   ├── app.Dockerfile               # simulators + API
│   │   ├── spark.Dockerfile             # Spark + package installed
│   │   └── airflow.Dockerfile           # thin: orchestration deps only
│   └── init/
│       ├── postgres/01_schema.sql
│       ├── postgres/02_indexes.sql
│       ├── postgres/03_seed_households.sql
│       ├── kafka/create_topics.sh
│       └── minio/create_buckets.sh
│
├── config/
│   ├── base.yaml                        # defaults — committed, reviewable
│   ├── local.yaml                       # laptop overrides
│   ├── prometheus/prometheus.yml
│   ├── prometheus/alert_rules.yml       # ← GRADED ARTEFACT
│   ├── alertmanager/alertmanager.yml
│   └── grafana/
│       ├── provisioning/datasources.yml
│       ├── provisioning/dashboards.yml
│       └── dashboards/*.json            # exported, version-controlled
│
├── src/voltstream/
│   ├── __init__.py
│   ├── config.py                        # single typed settings object
│   ├── simclock.py                      # ← ONE source of truth for simulated time
│   ├── logging_setup.py                 # structured JSON logger factory
│   ├── metrics.py                       # Prometheus registry + metric definitions
│   │
│   ├── contracts/
│   │   ├── events.py                    # MeterReading (Pydantic)
│   │   ├── reference.py                 # TariffRecord, WeatherForecast
│   │   └── schemas/                     # exported JSON Schema
│   │
│   ├── core/                            # ← THE SHARED TRANSFORMATION MODULE
│   │   ├── netting.py                   # pure: solar self-consumption / export
│   │   ├── tariff.py                    # pure: block tariff, subsidy, fixed charge
│   │   ├── validation.py                # pure: record validity rules
│   │   ├── spark_expr.py                # same logic as Column expressions
│   │   └── keys.py                      # dedup keys, partition keys
│   │
│   ├── simulators/
│   │   ├── profiles.py                  # load curve + solar curve maths
│   │   ├── faults.py                    # duplicate/null/negative/dropout injection
│   │   ├── meter_producer.py            # ENTRYPOINT
│   │   └── reference_dropper.py         # ENTRYPOINT
│   │
│   ├── streaming/
│   │   ├── session.py                   # SparkSession builder
│   │   ├── sources.py                   # Kafka read + deserialise + validate
│   │   ├── sinks.py                     # foreachBatch upsert, Parquet writer
│   │   ├── speed_layer.py               # ENTRYPOINT
│   │   └── raw_archiver.py              # ENTRYPOINT
│   │
│   ├── batch/
│   │   ├── daily_billing.py             # ENTRYPOINT
│   │   ├── daily_zone_rollup.py         # ENTRYPOINT
│   │   └── reconciliation.py            # ← speed vs batch divergence
│   │
│   ├── storage/
│   │   ├── postgres.py                  # engine, connection pool
│   │   ├── objectstore.py               # MinIO/S3 client + path conventions
│   │   └── repositories.py              # query functions — no SQL in routers
│   │
│   └── api/
│       ├── main.py
│       ├── dependencies.py
│       ├── models.py                    # response schemas
│       └── routers/
│           ├── health.py
│           ├── zones.py
│           ├── households.py            # ← THE MERGE FUNCTION
│           ├── reports.py
│           └── alerts.py
│
├── airflow/
│   ├── dags/
│   │   ├── daily_billing_dag.py
│   │   └── pipeline_watchdog_dag.py
│   └── plugins/
│
├── dashboard/
│   └── index.html                       # single-file polling dashboard
│
├── scripts/
│   ├── demo.sh                          # full end-to-end demo sequence
│   ├── inject_faults.sh                 # trigger each alert on demand
│   ├── backfill.sh                      # the restatement demo
│   └── smoke_test.sh
│
└── tests/
    ├── conftest.py
    ├── unit/
    │   ├── test_tariff.py
    │   ├── test_netting.py
    │   ├── test_validation.py
    │   └── test_simclock.py
    ├── property/
    │   └── test_tariff_invariants.py    # hypothesis: monotonicity, continuity
    ├── consistency/
    │   └── test_pure_vs_spark.py        # ← PROVES the layers cannot drift
    ├── integration/
    │   ├── test_producer_to_kafka.py
    │   └── test_batch_end_to_end.py
    └── fixtures/
```

### 7.3 Why it is shaped this way

**`src/` layout with one installable package.** `pip install -e .` means every
service imports `voltstream.core.tariff` by the same path — no `sys.path` hacks, no
relative-import fragility, no copy-pasted helper files. Tests import the package
exactly as production does, so a test cannot pass against code that would not run in
a container.

**Subpackages named after architecture layers.** `simulators / streaming / batch /
storage / api` maps one-to-one onto the boxes in the architecture diagram. A grader
who has read the report finds any component in seconds. That is most of what
"readability and modularity" means in the rubric.

**`core/` is a peer of the layers, not nested inside one.** The single most important
structural decision in the repository. Placing billing logic inside `batch/` and
re-implementing it in `streaming/` is precisely the Lambda drift failure. As a shared
sibling, the mitigation is **visible in the directory listing** and can be pointed at
during the viva.

**`core/` holds the logic twice, with a test pinning them together.** `tariff.py`
holds pure scalar functions; `spark_expr.py` holds the same logic as Column
expressions. This duplication is deliberate and necessary (§4.4), and
`tests/consistency/test_pure_vs_spark.py` makes divergence a test failure.

**Airflow is deliberately thin.** DAGs submit Spark jobs and check results; they
contain no business logic (§5.6).

**`pyproject.toml` with optional dependency groups**, not one `requirements.txt`:

```toml
[project.optional-dependencies]
spark = ["pyspark==3.5.*"]
api   = ["fastapi", "uvicorn", "psycopg[binary]", "prometheus-fastapi-instrumentator"]
sim   = ["confluent-kafka", "numpy"]
dev   = ["pytest", "hypothesis", "ruff", "mypy", "pytest-cov"]
```

Each Dockerfile installs only what it needs, so the API image does not ship a 300 MB
Spark distribution.

**Configuration in three tiers.** `config/base.yaml` for defaults (committed,
reviewable); `.env` for secrets and host-specific values (gitignored, with
`.env.example` committed); `src/voltstream/config.py` as the single typed loader.
**No magic numbers anywhere in source.** Tier boundaries, watermark duration and
`TIME_SCALE` all live in `base.yaml` — which also means an examiner asking "what if
the watermark were 60 s?" gets a config change, not a code change.

**`docs/architecture/` written as you build.** These files become the report. Writing
the architecture argument on day one forces it to exist *before* the code that
depends on it, rather than being retrofitted — and retrofitted arguments are obvious
to read.

**`.github/workflows/ci.yml`** running ruff, mypy and unit tests on every push. About
fifteen lines, visible as a green badge on the repository homepage, and direct
evidence for the code-quality marks.

---

## 8. How the pipeline works at runtime

### 8.1 The most important thing to understand

The system is **not** a sequential chain. The two branches do not take turns — they
run **simultaneously**, as independent consumer groups reading the same Kafka topic.

| | Speed branch | Batch branch |
|---|---|---|
| Order of operations | **process, then store** | **store, then process** |
| Reads from | Kafka (live) | Parquet (yesterday) |
| Triggered by | Continuous micro-batches | Airflow sensor on file arrival |
| Latency | ~10–40 s | Minutes after day close |
| Sees late data | No — dropped past watermark | Yes — rescans everything |
| Output | Approximate, provisional | Exact, authoritative |
| If it dies | Gap in live dashboard; data safe in Kafka and Parquet | Report is late; rerun the DAG |

Because they belong to **different consumer groups**, Kafka tracks their offsets
separately and delivers every message to both. If the archiver crashes, the speed
layer neither notices nor cares.

**Neither branch failing loses data**, because raw events exist in Kafka (for ~7 days)
and in Parquet (indefinitely). Failure means *delay*, not loss. That is the entire
purpose of the master dataset, expressed operationally.

### 8.2 What `docker compose up` starts

Eighteen containers in four dependency tiers, three of them one-shot. Compose starts
services roughly in parallel, so `depends_on` with **health conditions** is mandatory —
otherwise application containers crash-loop against a Kafka that is not yet listening.

| Tier | Containers | Ready when |
|---|---|---|
| **1 — Infrastructure** | `kafka`, `postgres`, `minio` | Kafka accepts connections; Postgres accepts queries; MinIO API responds |
| **2 — Bootstrap** *(run once, then exit 0)* | `kafka-init`, `postgres-init`, `minio-init` | Exit code 0 |
| **3 — Long-running apps** | `meter-producer`, `reference-dropper`, `speed-layer`, `raw-archiver`, `api` | Own `/health` endpoints pass |
| **4 — Orchestration & observability** | `airflow`, `docker-socket-proxy`, `prometheus`, `alertmanager`, `pushgateway`, `sql-exporter`, `grafana` | Scheduler heartbeat; scrape targets up (see `04-observability.md`) |

**Tier 2 is the one people forget.** These are short-lived jobs, not services: they
create topics, apply `01_schema.sql`, create buckets, then exit. Without them, tier-3
containers start correctly and fail on their first write because the target does not
exist.

**Warm-up is not instant.** Expect roughly **60–90 seconds** from `docker compose up`
to a genuinely working system — Kafka broker startup, Airflow scheduler
initialisation and Spark session creation all take real time. `scripts/demo.sh` must
**wait on health checks**, not `sleep 30` and hope.

### 8.3 One simulated day, traced end to end

With `TIME_SCALE = 288`, five real minutes is one simulated day.

**T+0 — `meter-producer` starts.**
Reads current simulated time from `simclock.py`; computes each household's expected
consumption and solar output from the load/solar curves for that simulated hour; adds
noise; applies the configured fault-injection rates; publishes to Kafka keyed by
`household_id`. Sleeps 2 seconds. Repeats. **It never stops and coordinates with
nothing.**

**T+0 — `raw-archiver` starts.**
Reads from Kafka, deserialises, adds an ingest timestamp, writes to
`s3a://voltstream-raw/meter_readings/sim_date=…/hour=…/`. **No transformation
whatsoever** — that is the point of a master dataset. Flushes a Parquet file per
micro-batch and records its Kafka offset in its checkpoint directory.

**T+0 — `speed-layer` starts.**
Reads the same messages; validates them (invalid records → dead-letter topic and
`rejected_records`); assigns each to a 15-simulated-minute event-time window;
aggregates by `grid_zone`; every ~10 seconds upserts results into `zone_metrics_rt`
and `household_running_rt`. Because of the 30-second watermark, a window is not
finalised the instant it ends — Spark holds it open in case a straggler arrives.

**T+0 — `api` starts.**
It merely listens. **It has no connection to Kafka or Spark whatsoever.** It queries
Postgres when asked.

**T+0 → T+5:00 — the dashboard.**
The browser polls `GET /api/v1/zones/load` every ~5 seconds. The API runs a `SELECT`
against `zone_metrics_rt` and returns whatever the speed layer most recently wrote.
**Nothing is pushed to the dashboard.** The data path ends at Postgres; the dashboard
reaches backwards to pull from it. This is why a closed browser tab changes nothing
upstream.

**T+5:00 — simulated midnight.** Two independent things happen:

- `reference-dropper` writes `tariff_2026-08-10.csv` (and the weather file) to the
  MinIO landing zone. That is its entire job — wake once per simulated day, write,
  sleep.
- The speed layer notices **nothing**. It has no concept of a day boundary; it keeps
  windowing.

**T+5:00 onward — Airflow.**
A small `tariff_watcher` DAG lists the MinIO landing bucket every real minute. When it
sees `tariff_2026-08-10.csv` it triggers one run of `daily_billing` with
`sim_date=2026-08-10` (parsed from the filename) and `run_id=billing__2026-08-10`;
re-offering the same date later is skipped, so the trigger is idempotent. The
`daily_billing` run then:

0. **Waits on an `S3KeySensor`** for the tariff object (the file is in object storage,
   so a local `FileSensor` cannot see it), then **waits a 90-second late-data grace**
   after the object's `LastModified`, so store-and-forward backfills for the closed day
   have landed in the Parquet partition before it is rescanned (decision D3).
1. **Submit the Spark billing job** as a sibling container via `DockerOperator`. Reads
   the *entire* `sim_date=2026-08-10/` Parquet partition; deduplicates on
   `(meter_id, event_ts)`; joins the tariff and weather dimensions; applies netting and
   block-tariff logic from `core/`; writes `household_bill_daily`; and **marks the day
   finalised** by inserting a `success` row into `pipeline_runs` in the same
   transaction.
2. **Verifies the result in SQL** (50 rows, no nulls), runs the zone roll-up, and fails
   the run if `sum(zone totals) ≠ sum(household totals)`.
3. **Run `reconciliation.py`.** Compares each household's speed estimate against the
   batch final; writes `reconciliation_daily`, splitting the divergence into a tariff
   effect and a data effect; emits the divergence as a Prometheus gauge.

Airflow itself contains no billing logic and no PySpark — it launches containers and
checks tables (decision D6).

**T+5:30 (approx.) — the merge function flips.**
`GET /api/v1/households/HH-0042/bill?date=2026-08-10` now finds a finalised row and
returns `source: "batch"` instead of `source: "speed", provisional: true`. The figure
may have changed slightly — **that delta is the Lambda demonstration.**

Meanwhile the producer, archiver and speed layer never paused. They are already three
simulated hours into the next day.

### 8.4 There is no orchestrating process

There is no `run_pipeline.py`. The system is **five independent long-running
processes plus one scheduled job**, connected only by shared storage:

```
meter-producer     → writes to Kafka
reference-dropper  → writes to MinIO
speed-layer        → reads Kafka,    writes Postgres
raw-archiver       → reads Kafka,    writes MinIO
api                → reads Postgres
airflow            → wakes, submits Spark, sleeps
```

None of them knows the others exist. The producer does not know whether anyone is
consuming. The API does not know whether Spark is running. Coupling is **entirely
through data at rest in a shared store** — which is exactly what makes each piece
independently restartable, scalable and debuggable.

> This is the mental shift from application programming to data engineering. In an
> application you trace a call stack. Here you trace **data through stores**, and the
> question "what is running right now?" has the answer "everything, always,
> independently."

### 8.5 What survives a restart

| Component | State lives in | Restart behaviour |
|---|---|---|
| `meter-producer` | Nothing (stateless) | Resumes at current simulated time — leaves a gap for the downtime |
| `speed-layer` | Spark checkpoint directory | Resumes from last committed offset; restores in-flight window state |
| `raw-archiver` | Spark checkpoint directory | Resumes from last committed offset; no duplicate files |
| `airflow` | Its metadata database | Remembers which DAG runs succeeded; will not re-run a completed day unless backfilled |
| `api` | Nothing (stateless) | Instant |

**The two checkpoint directories are the only fragile thing here.** Deleting a
checkpoint causes the job to restart from the topic's earliest offset and reprocess
everything — occasionally what you want, usually not. **Put checkpoints on named
Docker volumes, never on a bind mount into `/tmp`.**

---

## 9. Build phases

### 9.1 Ownership by directory

Assigning **directories** rather than tasks is what prevents three people editing
`docker-compose.yml` simultaneously. Merge conflicts in a two-week project are pure
lost time.

| Member | Owns | Also writes |
|---|---|---|
| **A** | `simulators/`, `contracts/`, `core/` (pure), Kafka setup, `docker/init/kafka/` | `docs/architecture/03-data-contracts.md` |
| **B** | `streaming/`, `batch/`, `core/spark_expr.py`, `airflow/`, `docker/init/postgres/` | `docs/architecture/01-lambda-vs-kappa.md` |
| **C** | `api/`, `storage/`, `dashboard/`, `config/prometheus\|grafana/`, `docker/`, `scripts/`, CI | `docs/architecture/02` and `04` |

**Conventions.** `main` is always green. Feature branches: `feat/speed-layer`,
`fix/watermark-drop`. Every PR requires one review — not for gatekeeping, but because
the brief requires *every* member to defend *every* decision. Reviewing is how you
learn the parts you did not write.

---

### Phase 0 — Foundations (Days 1–2)

**Goal:** the argument exists, and the infrastructure runs.

| Member | Tasks |
|---|---|
| **A** | Repository scaffold; `pyproject.toml` with dependency groups; draft `contracts/events.py` and `reference.py`; `.gitignore`, `.pre-commit-config.yaml` |
| **B** | **Write `docs/architecture/01-lambda-vs-kappa.md` first** (extracted from §4); design and write `docker/init/postgres/01_schema.sql` |
| **C** | `docker-compose.yml` with all 11 containers, health checks and dependency tiers; `.env.example`; `Makefile`; CI workflow |

**Also in this phase:** `config.py`, `simclock.py`, `logging_setup.py`, `metrics.py` —
the four cross-cutting modules every other module imports.

**Exit criteria:**
- [ ] `docker compose up` reaches healthy on **all 11 containers**
- [ ] CI green on `main`
- [ ] **Data contracts frozen and signed off by all three members**
- [ ] Architecture argument drafted (≥ 1,500 words)

> **GATE 1 (end of Day 2).** Kafka + Spark + Airflow networking is the single biggest
> time sink in this project and is worth **zero marks**. Solve it while you still have
> slack. **If it is not green by end of Day 2, cut Grafana immediately** and keep
> Prometheus alone.

---

### Phase 1 — Ingestion and the master dataset (Days 3–4)

**Goal:** events flow, and raw data lands durably.

| Member | Tasks |
|---|---|
| **A** | `profiles.py` (load and solar curves); `meter_producer.py`; `faults.py` with configurable injection rates; `reference_dropper.py`; producer-side Prometheus metrics |
| **B** | `session.py`; `sources.py` (Kafka read, deserialise, validate); `raw_archiver.py` writing partitioned Parquet with checkpointing |
| **C** | MinIO wiring, bucket creation; `objectstore.py`; API skeleton with `/health` and `/metrics`; README skeleton |

**Fault injection is not optional.** The simulator must emit, at configurable rates:
duplicates, out-of-order events, nulls in required fields, negative kWh, unknown
`household_id`, and periodic meter dropouts. This earns marks in **three** rubric rows
at once — ingestion robustness (15), processing correctness (15), and observability
(10) — because it gives the alerts something real to fire on during the demo.

**Exit criteria:**
- [ ] Events visible via console consumer, correctly keyed
- [ ] Parquet growing under `sim_date=/hour=` partitions
- [ ] Fault injection rates configurable via `base.yaml`
- [ ] **Restart test passes:** kill the archiver container, restart it, verify **no
      duplicate rows and no missing rows** in Parquet

> **GATE 2 (end of Day 4).** If checkpointing is wrong, everything built after this is
> built on sand. Do not proceed until the restart test passes.

---

### Phase 2 — Core logic and the speed layer (Days 5–7)

**Goal:** the shared transformation module exists, is proven, and the live path works.

| Member | Tasks |
|---|---|
| **A** | `core/netting.py`, `core/tariff.py`, `core/validation.py`, `core/keys.py` — all pure; full unit test suite; property tests with `hypothesis` |
| **B** | `core/spark_expr.py`; `speed_layer.py` with event-time windows and watermark; `sinks.py` with `foreachBatch` upsert |
| **C** | `storage/postgres.py`, `storage/repositories.py`; `routers/zones.py`; first Grafana dashboard |

**Property tests to write** (these are where tier boundaries actually get exercised):
- **Monotonicity:** more consumption never produces a lower bill
- **Continuity:** no discontinuous jump at a tier boundary
- **Netting invariant:** `self_consumed + export == solar`, and
  `self_consumed + billable_import == consumption`
- **Non-negativity:** no computed component is ever negative

**Exit criteria:**
- [ ] `tests/consistency/test_pure_vs_spark.py` passing over ≥ 1,000 random inputs
- [ ] `GET /api/v1/zones/load` returns live, updating data
- [ ] Invalid records land in the DLQ and `rejected_records`, and are counted

> **GATE 3 (end of Day 7).** Half the pipeline provably works, and the drift
> mitigation is proven rather than asserted.

---

### Phase 3 — Batch layer and the merge (Days 8–10)

**Goal:** a simulated day closes, finalises, and the merge function flips.

| Member | Tasks |
|---|---|
| **A** | Simulator hardening and edge cases; DLQ wiring; begin `docs/architecture/03` |
| **B** | `daily_billing.py` with tariff SCD join; `daily_billing_dag.py` (FileSensor, retries, SLA); `daily_zone_rollup.py`; `reconciliation.py` |
| **C** | **`routers/households.py` — the merge function**; `routers/reports.py`; report generator; `dashboard/index.html`; remaining Grafana dashboards |

**The reconciliation metric is the standout addition.** After each batch run, emit
`abs(speed_estimate − batch_final)` per household as a Prometheus gauge and alert
above a threshold. This is a system that **monitors its own Lambda divergence** —
roughly forty lines of code, and the thing an examiner will remember.

**Exit criteria:**
- [ ] One simulated day produces a complete, finalised `household_bill_daily`
- [ ] Merge function verifiably returns `source="speed"` before finalisation and
      `source="batch"` after, for the same household and date
- [ ] `reconciliation_daily` populated; divergence visible in Grafana
- [ ] Daily report file generated

> **GATE 4 (end of Day 9).** Both Lambda paths now exist. Everything after this is
> hardening and evidence.

---

### Phase 4 — Observability, testing, reproducibility (Days 11–12)

**Goal:** prove it works, and prove it fails visibly.

| Member | Tasks |
|---|---|
| **A** | `scripts/inject_faults.sh`; integration tests |
| **B** | `pipeline_watchdog_dag.py`; `scripts/backfill.sh`; rehearse the restatement demo |
| **C** | Alert rules; Alertmanager routing; complete README; `scripts/demo.sh`; `scripts/smoke_test.sh` |

**The alert rules — four required, one bonus:**

| Rule | Condition | Rubric relevance |
|---|---|---|
| `MeterDataStale` | No reading from a zone for > 2 real minutes | Health-check requirement |
| `LowRenewableContribution` | Zone renewable ratio below threshold for 3 consecutive windows | **Explicitly required by the brief** |
| `HighRejectRate` | rejected / total > 5% over 5 minutes | Error-rate requirement |
| `BatchSLAMiss` | Billing DAG incomplete within N minutes of simulated day close | Pipeline-failure detection |
| `LambdaDivergenceHigh` *(bonus)* | Mean speed-vs-batch divergence > threshold | Self-monitoring architecture |

**Then break things on purpose.** This day is **not optional**:
- Kill the producer → `MeterDataStale` fires
- Spike the corrupt-record rate → `HighRejectRate` fires
- Pause the Airflow DAG → `BatchSLAMiss` fires
- Corrupt a tariff file → run DAG → show wrong bills → fix → backfill → show corrected

**Screenshot every one of these.** "We instrumented the pipeline" is an assertion; a
screenshot of `MeterDataStale` firing after you killed the producer is **evidence**.
Ten marks turn on that difference.

**Exit criteria:**
- [ ] All alerts demonstrated firing, with screenshots captured
- [ ] Backfill/restatement demo rehearsed end to end
- [ ] **Fresh clone into a new directory → `make demo` → working system, zero manual
      steps**

> **GATE 5 (end of Day 12).** Reproducibility means a *cold* start, not "it works on
> my machine with the containers already warm."

---

### Phase 5 — Report, demo, viva (Days 13–14)

| Member | Tasks |
|---|---|
| **All** | Extract report chapters from `docs/architecture/`; produce final diagrams; insert screenshots and sample outputs |
| **C** | Assemble the PDF; write the individual-contributions statement |
| **All** | Record the 5–10 minute demo video; **cross-examine each other** on every component |

**Report structure (8–15 pages), mapped to this document:**

| Chapter | Source | Target marks |
|---|---|---|
| Use case and business requirements | §1, §2 | Context |
| **Architecture decision: Lambda vs Kappa** | §4 | **20** |
| Architecture diagrams | §6.1 | Report clarity |
| Technology stack justification | §5 | **10** |
| Observability design | §10.1 | **10** |
| Results with screenshots | Phase 4 captures | Report clarity |
| Limitations and production-scale changes | §10.2 | Honesty |

---

### 9.2 Pre-decided scope cuts

**Decide these now, while calm**, so that on Day 11 you are executing a plan rather
than panicking. Cut in this order:

1. **Grafana** → keep Prometheus and the alert rules file. *Alerting is graded; pretty
   graphs are not.*
2. **Custom dashboard UI** → the FastAPI OpenAPI docs page is a perfectly respectable
   demo surface.
3. **Integration tests** → keep unit, property and consistency tests, which is where
   the interesting assertions live.
4. **Weather file** → keep the tariff file only. The join still works; you lose only
   the forecasting hook.
5. **MinIO** → local volume mount, stated honestly in Limitations.

**Never cut:** the master dataset, the merge function, structured logging, the alert
rules, or the README. Those five carry disproportionate marks.

### 9.3 Schedule at a glance

| Day | A — sources | B — processing | C — platform | Gate |
|---|---|---|---|---|
| 1 | Scaffold, contracts draft | **Architecture argument** | Compose skeleton | |
| 2 | `simclock`, `config` | Postgres schema | CI, logging, metrics | **G1** |
| 3 | `meter_producer`, topics | `session`, `sources` | `.env`, Makefile, README | |
| 4 | `reference_dropper`, `faults` | `raw_archiver` → Parquet | MinIO, buckets | **G2** |
| 5 | `core/` pure + unit tests | `core/spark_expr.py` | API skeleton, `/health` | |
| 6 | Fault config, metrics | `speed_layer` windows | `postgres.py`, repositories | |
| 7 | Property tests | `sinks.py` upsert | `routers/zones.py` | **G3** |
| 8 | Simulator hardening | `daily_billing` + SCD join | Grafana datasource | |
| 9 | Support B; docs 03 | Billing DAG | **Merge function** | **G4** |
| 10 | DLQ wiring | `reconciliation.py` | Reports, dashboard | |
| 11 | `inject_faults.sh` | Watchdog DAG, backfill | Alert rules | |
| 12 | Integration tests | Restatement rehearsal | README, `demo.sh` | **G5** |
| 13 | Report sections | Report sections | Diagrams, PDF assembly | |
| 14 | Demo video, viva rehearsal — all three, cross-examining | | | |

---

## 10. Observability, limitations and viva preparation

### 10.1 Observability design

The rubric asks for "logging, metrics and tracing across pipeline stages to detect and
diagnose pipeline failures." Each of the three is addressed deliberately.

#### Structured logging

Every service emits **JSON lines** to stdout via `logging_setup.py`, with a fixed
envelope:

```json
{
  "ts": "2026-08-10T14:23:01.412Z",
  "level": "INFO",
  "service": "speed-layer",
  "stage": "aggregate",
  "trace_id": "…",
  "sim_date": "2026-08-10",
  "msg": "micro-batch committed",
  "rows_in": 412, "rows_out": 5, "rows_rejected": 3
}
```

Structured rather than free text because it is greppable, machine-parseable, and
survives being shipped to a log aggregator later. The fixed `service` and `stage`
fields let you follow one record's journey across container boundaries.

#### Metrics

| Metric | Type | Labels | Answers |
|---|---|---|---|
| `voltstream_events_produced_total` | Counter | `producer_id` | Is the source alive? |
| `voltstream_events_consumed_total` | Counter | `layer` | Is the consumer keeping up? |
| `voltstream_records_rejected_total` | Counter | `layer`, `reason` | What kind of bad data, and how much? |
| `voltstream_e2e_latency_seconds` | Histogram | `layer` | **Where does latency actually go?** |
| `voltstream_consumer_lag` | Gauge | `layer`, `partition` | Is a branch falling behind? |
| `voltstream_zone_renewable_ratio` | Gauge | `grid_zone` | Business metric, drives an alert |
| `voltstream_batch_duration_seconds` | Histogram | `job` | Is the nightly job degrading? |
| `voltstream_lambda_divergence` | Gauge | — | **Are the two layers agreeing?** |

`voltstream_e2e_latency_seconds` is the one that wins arguments. It lets you answer
the "isn't Postgres the bottleneck?" question with measurement rather than assertion
(§10.3).

#### Tracing — the pragmatic version, stated honestly

> **Limitation:** true distributed tracing through Spark executors (OpenTelemetry
> spans crossing the JVM/Python boundary inside a micro-batch) is impractical within
> this project's scope. We implement **correlation-ID tracing** instead: a `trace_id`
> generated at the producer, carried in Kafka message headers, propagated into
> Parquet and into every log line at every stage, plus an end-to-end latency
> histogram. This permits reconstructing any single record's path and timing across
> the whole pipeline by grepping on one identifier.
>
> We chose this over a half-working Jaeger deployment deliberately. Naming the
> limitation scores better than pretending it is solved.

#### Health checks

| Endpoint | Checks |
|---|---|
| `GET /health/live` | Process is up |
| `GET /health/ready` | Postgres reachable, MinIO reachable |
| `GET /api/v1/alerts/status` | Current firing alerts, surfaced in the dashboard |

### 10.2 Limitations — state all of these in the report

Grouped so they map directly onto report sections.

**Simulation fidelity**
- Time compression scales event time but **not** processing time; watermark tuning is
  therefore not representative of production, where watermarks are derived from
  observed lateness distributions.
- Meters emit interval kWh rather than cumulative register readings; no meter-reset
  or rollover handling.
- Load and solar curves are deterministic functions plus noise, not real
  measurements; no weather-driven correlation across zones beyond the forecast index.
- 50 households and 5 zones — three to four orders of magnitude below a real utility.

**Architecture and storage**
- **Small-file problem:** streaming Parquet writes produce many small files, which
  degrades read performance. Production requires a compaction job.
- **Object-store commit protocol:** Spark's default file commit relies on rename,
  which is a copy on S3. Production requires the S3A magic committer or a table
  format that handles commits properly.
- No schema registry — producer/consumer schema agreement is by convention, enforced
  only by Pydantic at the boundary.
- Tariff dimension is handled as a simple effective-dated join, not a full SCD Type 2
  implementation with validity intervals.

**Operations**
- Single-broker Kafka, single-node Spark, single Postgres instance — no replication,
  no failover; every component is a single point of failure.
- Spark executor metrics are emitted from the driver inside `foreachBatch` rather
  than scraped from executors directly.
- Secrets are managed via `.env`, not a secret manager.

**Scale boundaries — the sentence to include**
> PostgreSQL is correct at our scale and would remain correct up to roughly the low
> millions of aggregate rows per day. Beyond that, the serving layer should move to
> TimescaleDB, ClickHouse or Cassandra. Choosing Postgres here reflects our actual
> data volume rather than aspirational scale.

### 10.3 Anticipated viva questions and answers

The brief requires every member to defend every decision. These are the questions
most likely to be asked.

**"Why Lambda and not Kappa?"**
Two consumers with opposite latency and correctness profiles from one input stream,
and — decisively — a billing restatement horizon of months against a Kafka retention
of days. Kappa's correction mechanism is replay from the log; the log does not live
long enough. See §4.2.

**"Lambda has two codebases that drift. How did you handle that?"**
Structurally, not procedurally. `core/` is a shared pure module imported by both
layers, and `tests/consistency/test_pure_vs_spark.py` asserts both implementations
produce identical output across thousands of random inputs. **We made drift a test
failure.** See §4.4.

**"Isn't Postgres a bottleneck if everything ends up there?"**
Everything does not end up there. Raw events go to Parquet; only aggregates reach
Postgres — roughly a 13× reduction at our scale, and the ratio improves as meter
count grows, because aggregate row count is driven by `zones × windows`, not by meter
count. Furthermore, end-to-end latency is dominated by two numbers we configured:

| Stage | Contribution |
|---|---|
| Meter → Kafka | ~10 ms |
| Micro-batch trigger interval | **~10 s (our choice)** |
| Watermark wait | **~30 s (our choice)** |
| Postgres bulk write | ~50–100 ms |
| API read | ~20–50 ms |

Postgres contributes on the order of 1%. We measure this with
`voltstream_e2e_latency_seconds` rather than asserting it. *(Measured 2026-09-28: speed path
p50 5.1 s, p95 14.3 s, none over 60 s; the watermark adds nothing in update mode, so the
"~30 s" row above is wrong and the trigger alone dominates. Table and method:
`04-observability.md` §11, T168.)* Writes go through
`foreachBatch` as bulk upserts — one write of ~500 rows per micro-batch, not 500
round-trips — and a Postgres outage stalls offset advancement rather than losing
data, because Kafka absorbs it.

**"Why not just derive the master dataset from the speed view and save a write?"**
Aggregation is irreversible. The speed view holds 15-minute zone sums; the raw events
are per-meter, per-instant. The second cannot be reconstructed from the first. A
master dataset must be raw and complete or it is not a master dataset.

**"Why does the batch layer read Parquet instead of replaying Kafka?"**
Kafka retention is ~7 days; the restatement horizon is months. Kafka is a transport
buffer, not an archive. Additionally, a partition-pruned columnar rescan is
dramatically cheaper than replaying a topic from offset zero.

**"Why event time rather than processing time?"**
If you window on arrival time, a network hiccup silently moves a reading into the
wrong window and daily totals stop reconciling. The meter stamps the reading; use
that stamp.

**"What happens if the speed layer dies mid-day?"**
The live dashboard develops a gap. No data is lost — events remain in Kafka and in
Parquet. On restart, the checkpoint resumes from the last committed offset. The
day's bill is unaffected, because billing reads from Parquet, not from the speed view.

**"Why is `household_id` the Kafka message key?"**
Two reasons at once: per-household ordering within a partition (needed for
deduplication) and co-partitioning with the tariff join key, making the join local
rather than a shuffle.

**"What would you do differently at production scale?"**
Adopt Iceberg or Delta Lake over raw Parquet for ACID, time travel and automatic
compaction; add a schema registry; replace the daily file drop with CDC via Debezium;
move the serving layer off single-node Postgres; run Spark on Kubernetes with
autoscaling. And note that Iceberg or Kafka tiered storage removes the retention
constraint that drove us to Lambda — **at which point Kappa becomes the better
choice**. See §4.5.

### 10.4 Beyond the current scope

Ideas already designed for, should time permit, ordered by effort-to-impact.

**Cheap, high-impact**
- **Reconciliation alerting** (already in Phase 3) — the system monitoring its own
  Lambda divergence.
- **DLQ replay tooling** — a script that repairs and re-injects dead-lettered
  messages, turning a counter into an operational capability.
- **Data quality gates** between stages — row-count bounds, null checks, and a
  cross-check that `sum(zone totals) == sum(household totals)`. Fail the DAG on
  violation rather than silently publishing a wrong report.
- **Schema Registry** with Avro or Protobuf, enforcing backward-compatible evolution.

**Ambitious extensions**
- **Short-term load forecasting.** The weather file is already in the pipeline. Join
  tomorrow's cloud-cover forecast to each zone's historical solar profile and predict
  the shortfall an hour ahead. This converts a *monitoring* system into a
  *decision-support* system — the utility can pre-ramp generation instead of reacting
  to a frequency drop. It is the most natural high-value extension available, and it
  uses a data source already built.
- **Theft and tamper detection** — flag households whose consumption pattern deviates
  sharply from their own historical baseline.
- **Demand-response simulation** — identify households able to shift load out of peak
  windows and quantify the effect on zone peak demand.

---

## Appendix A — Configuration reference

`config/base.yaml` holds every tunable. No magic numbers appear in source code.

```yaml
simulation:
  time_scale: 288                  # 1 sim day = 5 real minutes
  emit_interval_seconds: 2
  households: 50
  zones: ["ZONE-A", "ZONE-B", "ZONE-C", "ZONE-D", "ZONE-E"]

faults:                            # deliberate fault injection rates
  duplicate_rate: 0.02
  out_of_order_rate: 0.03
  null_field_rate: 0.01
  negative_value_rate: 0.005
  unknown_household_rate: 0.005
  dropout_probability: 0.01
  dropout_duration_seconds: 30

kafka:
  topic: "meter.readings"
  dlq_topic: "meter.readings.dlq"
  partitions: 3
  retention_ms: 604800000          # 7 days

speed_layer:
  window_minutes: 15               # simulated minutes
  watermark_seconds: 30
  trigger_interval_seconds: 10

tariff:
  blocks:
    - { up_to_kwh: 60,   rate: 8.00 }
    - { up_to_kwh: 120,  rate: 16.50 }
    - { up_to_kwh: null, rate: 24.50 }   # null = unbounded top block
  fixed_charge_by_tier: { TIER_1: 120.00, TIER_2: 240.00, TIER_3: 480.00 }
  subsidy_discount_pct: 25.0
  export_credit_rate: 18.00

alerts:
  stale_data_minutes: 2
  low_renewable_threshold: 0.15
  reject_rate_threshold: 0.05
  batch_sla_minutes: 10
  lambda_divergence_threshold_pct: 5.0
```

## Appendix B — Command reference

```bash
make up            # docker compose up -d, wait for health
make demo          # up + wait + run one full simulated day + open dashboard
make down          # tear down, keep volumes
make clean         # tear down, DESTROY volumes (resets checkpoints)
make test          # unit + property + consistency
make test-all      # + integration
make lint          # ruff + mypy
make logs s=api    # follow one service's logs
make faults        # trigger each alert in sequence (demo aid)
make backfill d=2026-08-10   # restatement demo
```

## Appendix C — Definition of done

The project is complete when all of the following are true:

- [ ] Fresh clone → `make demo` → working system with zero manual steps
- [ ] Both Lambda paths demonstrably active simultaneously
- [ ] Merge function shown returning `speed` then `batch` for the same household/date
- [ ] All five alerts demonstrated firing, screenshots captured
- [ ] Backfill/restatement demonstrated end to end
- [ ] `test_pure_vs_spark.py` passing — drift mitigation proven
- [ ] Report PDF, 8–15 pages, every rubric criterion addressed
- [ ] Demo video 5–10 minutes
- [ ] Individual contributions statement included
- [ ] **Every member can defend every component**
