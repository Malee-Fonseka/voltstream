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
| Out-of-order event | 3.0 % | watermark; shift uniform over 1–30 simulated minutes |
| Null required field | 1.0 % | validation → `null_field` |
| Negative kWh | 0.5 % | validation → `negative_kwh` |
| Unknown household | 0.5 % | validation → `unknown_household` |
| Meter dropout | 0.2 % per meter per tick | store-and-forward, 30 real s buffer |

**The two lateness mechanisms are deliberately separated** (D3). Out-of-order events model
network reordering and are bounded by the watermark, so they are *always* absorbed.
Dropouts model a communications outage and flush a backlog far beyond the watermark, so
the speed layer misses them while the batch layer, rescanning a closed day, does not. That
gap is the source of the speed-versus-batch divergence the reconciliation metric measures.
It is injected on purpose; without it the divergence would be zero and the merge function
would demonstrate nothing.

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
