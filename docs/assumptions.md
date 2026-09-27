# Assumptions, simplifications and measured limitations

Every deliberate simplification in voltstream, in one place (T085). Each is a conscious
decision rather than an oversight, and each states what would change at production scale.

This file is the single source for §10 of the report (`docs/report/main.tex`, Appendix A
and the Limitations chapter). When a limitation is measured rather than estimated, the
measurement and the date it was taken are recorded here.

Cross-references: `00-master-design.md` §3.3b, §3.4, §5.4, §5.7 and §10.2;
`05-open-decisions.md` D1–D7.

---

## 1. Simulated time

| Assumption | Value | Where |
|---|---|---|
| Time compression | 1 simulated day = 5 real minutes (`TIME_SCALE = 288`) | `config/base.yaml` `simulation.time_scale` |
| Emit interval | one reading per household per 2 real seconds | `simulation.emit_interval_seconds` |
| Readings per meter per simulated day | 150 | derived: 300 real s ÷ 2 s |
| Raw events per simulated day | ~7,500 across 50 households | derived |

**Limitation — time compression scales event time but not processing time** (§3.4).
Network latency, garbage-collection pauses and micro-batch intervals all proceed at
wall-clock speed. Watermark tuning is therefore not representative of production, where
watermarks are set against lateness distributions measured in real time.

This has a consequence specific to our time scale, recorded by D3: the 10-real-second
trigger interval equals **48 simulated minutes**, which is longer than the
30-simulated-minute watermark. At `TIME_SCALE = 288` the trigger interval, not the
watermark, is the dominant term in what gets dropped, and a watermark shorter than one
trigger is decorative. This is an artefact of the simulation, not of the architecture.

**All timestamps are UTC.** `event_ts` is simulated time and is used only for windowing
and for deriving `sim_date`. Latency is measured from the Kafka record timestamp, which is
wall-clock by construction (D/T030) — `event_ts` must never be subtracted from `now()`.

---

## 2. The simulated population and its data

- **50 households across 5 grid zones**, with fixed zone membership. Three to four orders
  of magnitude below a real utility deployment.
- **Meters emit interval energy in kWh, not cumulative register readings.** A real meter
  reports a monotonically increasing register; production would require per-meter stateful
  differencing with meter-reset and rollover detection.
- **Load and solar curves are deterministic functions with additive noise**, not measured
  data. Cross-zone weather correlation is limited to the daily forecast index.
- **Tariff values are synthetic.** Block boundaries (60 / 120 kWh, unbounded above) are
  configuration; every rate, fixed charge and subsidy percentage reaches the billing logic
  through the day's tariff file (D2). The generator defaults — 8.00 / 16.50 / 24.50 per
  kWh, tier fixed charges 120 / 240 / 480, 25 % subsidy, 18.00 export rate — are not drawn
  from a published tariff schedule.

### Fault injection

Rates are chosen to exercise validation and alerting paths, not to model realistic meter
failure. With every rate set to zero, output equals input exactly — asserted as a test.

| Fault | Rate | Exercises |
|---|---|---|
| Duplicate event | 2.0 % | dedup on `(meter_id, event_ts)` |
| Out-of-order event | 3.0 % | watermark; shift uniform over 1–30 simulated minutes. Measured: *not* fully absorbed — see below |
| Null required field | 1.0 % | validation → `null_field` |
| Negative kWh | 0.5 % | validation → `negative_kwh` |
| Unknown household | 0.5 % | validation → `unknown_household` |
| Meter dropout | 0.2 % per meter per tick | store-and-forward, 30 real s buffer |

**The two lateness mechanisms are deliberately separated** (D3). Out-of-order events model
network reordering and are bounded by the watermark, so D3 expects them to be *always*
absorbed — **a claim the measurement below refutes.**
Dropouts model a communications outage and flush a backlog far beyond the watermark, so
the speed layer misses some of them while the batch layer, rescanning a closed day, does
not. It is injected on purpose; without it there would be nothing for the merge function
to correct.

### Where the dropped energy actually goes — measured

Whether a late reading is dropped depends on the size of the window it belongs to. A
window stops accepting data at roughly `watermark + trigger + tick + window`:

| Aggregation | Window | Lateness certainly dropped beyond |
|---|---|---|
| Zone metrics | 15 sim min | **102.6 sim min** |
| Household running total | 1 sim day | **1527.6 sim min** |

A dropout buffers for 30 real seconds and flushes readings **10–144 simulated minutes**
late. That straddles the 15-minute threshold and is nowhere near the daily one, so the
consequence is asymmetric — measured over one complete simulated day:

| View | kWh | Gap vs archived |
|---|---|---|
| Archived, after the same validation | 508.5150 | — |
| Household daily totals | 508.5150 | **0.0000 %** |
| 15-minute zone windows | 506.6273 | **0.3712 %** |

### The control, and what it overturned

Disabling dropouts should take the 15-minute gap to zero: reordering is bounded by the
watermark (`L <= W`), so D3 states it is "always absorbed, by construction". It is not.

| Host | Dropouts | 15-min gap | Daily gap |
|---|---|---|---|
| Loaded | on | 0.3712 % | 0.0000 % |
| Loaded | **off** | 0.5635 % | — |
| Idle | on | 1.1624 % | 0.0000 % |
| Idle | **off** | **0.9996 %** | 0.0162 % |

The first control failure was provisionally blamed on host saturation — a probe waited
fifteen minutes for CPU during that run, and a starved driver spans more event time per
micro-batch, which advances the watermark in leaps. **Re-running on an idle host refuted
that**: the gap went *up*, to 0.9996 %.

So the finding is real and reproducible. **Reordering within the watermark is dropped.**
Of the 1.16 % missing with dropouts enabled, roughly 1.0 point is reordering and only
about 0.16 is dropout backfill — the reverse of the assumed attribution.

The mechanism is the one D3 identifies and then contradicts itself about: the trigger
interval is 48 simulated minutes and the watermark is 30, and *"a watermark smaller than
one trigger is decorative"*. Both statements are in D3; only one survives measurement.

**Consequences.** D3's regime table needs correcting — network reordering is not fully
absorbed. Any attribution of the speed-versus-batch divergence to "dropped backfill,
nothing else" is wrong as written. Raising the watermark above one trigger (60 simulated
minutes rather than 30) is the change D3's own analysis implies if the stated intent is
to be met; that has not been done, because it revises a pinned decision.

What is *not* affected: the daily totals, which stay complete in every run.

**Two consequences worth stating plainly.** The real-time *operational* view is
measurably incomplete — about 1 % of the day's energy on an idle host — which is the
speed layer doing its job, trading completeness for latency. The *provisional bill* is
not incomplete at all: nothing the fault model injects arrives late enough to miss the
day it belongs to. So the speed-versus-batch divergence on bills comes from the **stale
tariff**, not from lost readings, and the reconciliation metric should be read
accordingly.

**Figures to quote.** The 15-minute gap is **1.16 %** with the default fault
configuration, measured on an idle host over one complete simulated day; the daily gap is
**0.00 %**. The loaded-host figures (0.37 % / 0.56 %) are recorded above only to document
how the control was chased down, and should not be quoted as results — they were taken
while four Spark drivers competed for one machine.

D3's sizing model put the drop near 1.7 %, attributed entirely to dropout backfill. The
measured total is 1.16 %, and the attribution is wrong: most of it is reordering.

---

## 3. Storage and the master dataset

**Limitation — Parquet is poor at row-level updates and deletes** (§5.4). Changing one row
means rewriting the file. This aligns with an append-only master dataset, so it is not a
problem here; it would become one if the design ever needed in-place correction.

**Limitation — streaming writes produce many small files** (§10.2), degrading subsequent
read performance. Production requires a periodic compaction job.

**Measured**, over one complete simulated day, archiver at a 10-real-second trigger with
4 shuffle partitions:

| Metric | Value |
|---|---|
| Parquet files | 245 |
| Total size | 2.49 MB |
| Average file size | **10.2 kB** |
| Smallest / largest file | 6.0 kB / 24.6 kB |
| Hour partition directories | 24 |
| Rows archived | 6,946 |

Measured 2026-09-23 against the full Compose stack.

The average file is **10.2 kB against a Parquet row-group target of roughly 128 MB** —
about four orders of magnitude too small. Every one carries a footer, schema and
row-group metadata, so the fixed overhead dominates the payload, and a full-day read
pays 245 object-store round trips where a compacted day would pay a handful.

The cause is structural, not a misconfiguration: a streaming sink commits at least one
file per partition per micro-batch. At 10 real seconds per trigger a simulated day is
about 30 triggers, and each writes into whichever hour partitions its rows fall in.
Production fixes this with a compaction job, or with a table format (Iceberg, Delta Lake)
that compacts as part of its commit protocol.

**Limitation — the object-store commit protocol.** The default file-output committer
relies on rename, which object stores implement as copy-then-delete rather than as an
atomic operation. Production requires the S3A magic committer or a table format that
handles commits natively (Iceberg, Delta Lake).

**Checkpoints live on named Docker volumes, not on S3A** (T044). A checkpoint directory
needs stable inode identity across restarts, which an object store does not provide.
`.env.example` still defines `MINIO_BUCKET_CHECKPOINTS`; it is unused and is scheduled for
removal at T182.

---

## 4. Delivery guarantees

Kafka provides **at-least-once** delivery, and a Spark file sink commits its data files
and its offsets in two separate steps. After an unclean kill, a small number of duplicate
*rows* can therefore legitimately reappear in the master dataset.

**The guarantee this system claims is "no loss; duplicates tolerated".** It does not claim
exactly-once. Duplicates are removed downstream by the batch layer's deduplication on
`(meter_id, event_ts)` (§3.3d), which is why that key exists. Gate 2's restart test
(T083) asserts no loss and no offset gap — not the absence of duplicates — because
asserting the latter would be asserting something the file sink does not provide.

---

## 5. Architecture and operations

- **No schema registry.** Producer–consumer schema agreement is by convention, enforced
  only by explicit schema declaration at the ingestion boundary and by T033's drift guard.
- **Simplified dimension handling.** The tariff dimension uses an effective-dated join
  rather than a full slowly-changing-dimension implementation with validity intervals.
- **Single points of failure throughout** — one Kafka broker, one Spark node, one
  PostgreSQL instance. No replication, no failover.
- **Secrets live in `.env`**, not in a secret manager. Every credential in the repository
  is a non-secret local default.
- **The object store is a community fork of MinIO** (`pgsty/silo`, pinned to a release tag),
  because MinIO Inc. withdrew its community images in 2025–26 (D8). The fork is maintained
  by one person; SeaweedFS is the planned replacement. The pipeline speaks only the S3 API,
  so the swap is configuration, not code.
- **The raw archiver performs no validation or filtering.** This is deliberate: filtering
  at the archiver would destroy the bug-recovery property that justifies the master
  dataset's existence. Validation happens in the layers that read it.

**Limitation — tracing is by correlation ID, not distributed spans** (§5.7). True
distributed tracing through Spark executors, with spans crossing the JVM/Python boundary
inside a micro-batch, is impractical within this project's scope. A `trace_id` is
generated at the producer, carried in Kafka headers, propagated into the master dataset
and emitted in every log line, alongside an end-to-end latency histogram. Any record's
path and timing can be reconstructed by filtering on one identifier.

**Limitation — executor metrics are driver-side.** Metrics are emitted from the driver
inside `foreachBatch` rather than scraped from executors, which are transient and would
require a push gateway. The counts are exact; what is approximated is attribution to a
specific executor, which nothing in this project needs.

---

## 6. Serving-layer scale boundary

PostgreSQL is appropriate at our volume and would remain so to roughly the low millions of
aggregate rows per day. Beyond that the serving layer should migrate to a time-series or
columnar analytical store (TimescaleDB, ClickHouse, Cassandra). Selecting PostgreSQL
reflects actual data volume rather than aspirational scale.

---

## 7. Gate evidence

Recorded as each gate passed, so the report's results chapter quotes measurements rather
than reconstructing them later.

### Gate 2 — archiver restart (Phase 6)

SIGKILL, 20 s outage, restart:

| Metric | Value |
|---|---|
| Rows before kill | 10,661 |
| Rows after recovery | 14,012 |
| Distinct `event_id` | 13,678 |
| Duplicate rows | 334 |
| Kafka partitions with contiguous offsets | 3 / 3 |

No loss and no offset gap. The 334 duplicates are expected under at-least-once delivery
and are removed downstream by the batch dedup on `(meter_id, event_ts)` — see §4.

### Gate 3 — speed path complete (Phase 7)

| Criterion | Evidence |
|---|---|
| Pure-vs-Spark consistency test green | `tests/consistency/test_pure_vs_spark.py` passes: 5,000 random inputs plus per-block boundary sweeps |
| Live zone data | 40 real seconds advanced the newest window from 07:30 to 10:45 simulated (3 h 15 min, i.e. 288x); `zone_metrics_rt` grew 119 → 180 rows |
| Rejects in both sinks, counted | 53 rows in `rejected_records` and 53 messages in `meter.readings.dlq` — exact match |
| Reject reasons | `null_field` 27, `negative_kwh` 13, `unknown_household` 13 |
| One identifier across both sinks | `trace_id` 389048a5… present in the Postgres row and in the DLQ message |
| Provisional bills | 50 / 50 households, every D4 component non-null, `estimated_bill = energy + fixed − subsidy − export` on every row, `tariff_source_date = sim_date − 1` on every row |

The API endpoint named in the gate does not exist until Phase 8, so the zone view was
verified directly against PostgreSQL, which that task permits.
