# voltstream — Decision Log

Architecture Decision Records for the questions the
[Master Design](00-master-design.md) leaves ambiguous or self-contradictory.
Each is load-bearing for code written later. Nothing in `src/` that depends on a
decision below may be written until that decision's **Decision** section is filled.

Task references (`Txxx`) point into [`Implementation_Tasks.md`](../Implementation_Tasks.md).

## Status

| ID | Decision | Resolved in | Status | Blocks |
|---|---|---|---|---|
| D1 | Python version and PySpark version | T002 | ✅ decided 2026-09-19 | Everything Spark, CI |
| D2 | Tariff source of truth (config vs CSV) | T003 | ✅ decided 2026-09-19 | T019, T031, T057, T074 |
| D3 | Watermark units and value | T004 | ✅ decided 2026-09-19 | T070, T088, T094, T122, T137 |
| D4 | Who computes the provisional bill | T005 | ✅ decided 2026-09-19 | T035, T039, T091, T106, T127, T137 |
| D5 | Subsidy and final-bill arithmetic | T006 | ✅ decided 2026-09-19 | T029, T055, T057, T058, T062, T063, T078, T113, T141, T172 |
| D6 | Airflow → Spark submission, and the sensor | T007 | ✅ decided 2026-09-19 | T039, T119, T120, T121, T122, T123, T125, T139, T162 |
| D7 | Package and repository name spelling | T008 | ✅ decided 2026-09-19 | T015, T043, T109, T183 |
| D8 | Object store image after MinIO's withdrawal | — | ✅ decided 2026-09-26 | T043, T047, T050, T182 |
| D9 | Where two alerts get facts no application metric carries | T147 | ✅ decided 2026-09-27 | T144, T147, T156 |

Legend: ⬜ open · ✅ decided · 🔁 superseded

---

## D1 — Python version and PySpark version

**Status:** ✅ decided 2026-09-19 · **Resolved in:** T002

### Context

The Master Design pins `pyspark==3.5.*` (§7.3) but never states a Python version.
PySpark 3.5.x is built and tested against Python 3.8–3.11. The consistency test
`tests/consistency/test_pure_vs_spark.py` — the centrepiece of the §4.4 drift argument —
requires **local** PySpark on the laptop and in CI, so this is not a Docker-only concern.
The Spark version chosen here also fixes the Kafka connector and Hadoop/S3A JAR versions
baked into `spark.Dockerfile` (T049).

### Options

| # | Option | For | Against |
|---|---|---|---|
| 1 | Python 3.11 + `pyspark==3.5.*` | Inside PySpark's tested range; most battle-tested combination; widest documentation | Not the newest Python |
| 2 | Python 3.12 + `pyspark==4.0.*` | Newer | Spark 4.0 changes some APIs and defaults; less community troubleshooting material within a two-week budget |
| 3 | Python 3.13, consistency tests only inside the Spark container | No venv change | Loses fast local feedback; complicates CI; CI must build the Spark image to run one test |

### Decision

**Option 1.** Python **3.11.9** (pinned in `.python-version`) with **`pyspark==3.5.*`**
(resolves to 3.5.9 as of today). The venv was recreated at 3.11.9 and PySpark installed.

**Verified on this machine (2026-09-19)**, using a local `SparkSession` (`local[2]`) running
an in-memory DataFrame through Catalyst Column expressions — the same workload
`tests/consistency/test_pure_vs_spark.py` will run:

| Check | Result |
|---|---|
| `import pyspark; pyspark.__version__` | `3.5.9` |
| Local JVM | Java **21.0.11** — outside Spark 3.5's officially supported 8/11/17, but the session starts, executes, and returns correct results. No change required locally; if anything JVM-related misbehaves later, install Temurin 17 and set `JAVA_HOME` rather than debugging. |
| Scala (bundled) | 2.12.18 |
| Hadoop (bundled) | **3.3.4** — this fixes the S3A JAR versions for T049 |
| Session start time | 7–12 s cold |
| Column-expression arithmetic | Executes and returns rows |

**One Windows-specific failure was hit and resolved.** With `PYSPARK_PYTHON` unset, Spark
executors launch Python workers via `python` on `PATH`, which on Windows resolves to the
Microsoft Store alias stub. The worker never connects back and every task fails with
`SparkException: Python worker failed to connect back` / `SocketTimeoutException: Accept timed
out`. Setting `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to the venv interpreter fixes it.
The test harness must do this itself (see Consequence) so no developer has to know.

### Consequence

- `.python-version` → `3.11.9` — **done**.
- **T015 `pyproject.toml`:** `requires-python = ">=3.11,<3.12"`; optional group
  `spark = ["pyspark==3.5.*"]`. Do not widen the Python range — nothing above 3.11 is inside
  PySpark 3.5's tested matrix.
- **T048 `app.Dockerfile`:** base image `python:3.11-slim`.
- **T049 `spark.Dockerfile`:** base image from the official `apache/spark` 3.5.x Python
  variant (ships Java 17, within official support). JARs pinned to the measured versions:
  `spark-sql-kafka-0-10_2.12:3.5.9`, `hadoop-aws:3.3.4`, and the `aws-java-sdk-bundle`
  version that `hadoop-aws:3.3.4`'s POM declares (1.12.262 — confirm against the POM when
  writing the Dockerfile), plus the PostgreSQL JDBC driver. Scala suffix is `_2.12`.
- **T064 `tests/conftest.py`:** before building the session, set
  `os.environ.setdefault("PYSPARK_PYTHON", sys.executable)` and the same for
  `PYSPARK_DRIVER_PYTHON`. This is the fix for the Windows Store-alias failure above, and it is
  harmless on Linux/CI. Also `local[2]`, `spark.ui.enabled=false`, shuffle partitions 1,
  session-scoped — startup is 7–12 s, so one session per test run, not one per test.
- **Local Spark tests stay in-memory.** `winutils.exe` / `HADOOP_HOME` are not installed and
  are not needed unless a test writes to the local filesystem. If one ever does, that test is
  marked `integration` and runs in the container, not the laptop.
- **T169 CI:** `actions/setup-python` at `3.11`, `actions/setup-java` at **17** (Temurin) —
  CI stays inside official Spark support even though the laptop runs 21.
- **T182 Master Design reconciliation:** §7.3 names `pyspark==3.5.*` but no Python version;
  add "Python 3.11" there.
- **Side finding for D5:** while verifying, the float64 result at the 60 kWh block boundary
  (`60.01 kWh → 480.16`) differed from the decimal-correct answer (`480.17`). Both the pure
  Python and Spark paths agree with each other on float64, so the consistency test would pass —
  but both would be a cent off the correct bill. Recorded under D5's context; the numeric type
  for money is decided there, not here.

---

## D2 — Tariff source of truth (config vs CSV)

**Status:** ✅ decided 2026-09-19 · **Resolved in:** T003

### Context

Two parts of the Master Design each define tariff money, and they overlap:

- §6.2, the daily `tariff_YYYY-MM-DD.csv`:
  `household_id, tariff_rate, billing_tier, subsidy_flag, fixed_charge, export_rate, effective_date`
- Appendix A, `config/base.yaml`:
  `tariff.blocks[]` (three bands with rates), `fixed_charge_by_tier`, `subsidy_discount_pct`,
  `export_credit_rate`

`fixed_charge` and `export_rate` therefore exist in **both** places, and `tariff_rate` (a single
scalar) has no defined relationship to the three block rates. Both `core/tariff.py` and
`core/spark_expr.py` need one unambiguous input shape, and `reference_dropper.py` needs to
know what to generate.

A constraint that rules options out: §5.6's restatement demo requires a *retroactive tariff
correction* to be a **data** change (edit a CSV, backfill), not a code or config change.
Whatever carries the money that can change must therefore be in the daily file.

### Options

| # | Option | For | Against |
|---|---|---|---|
| 1 | `base.yaml` defines band **structure** (`up_to_kwh` boundaries) and default block rates. CSV supplies per-household, per-day values: `billing_tier`, `subsidy_flag`, `fixed_charge`, `export_rate`, `effective_date`. `tariff_rate` is either dropped or explicitly redefined (e.g. the household's top-block rate). | Retroactive correction is a CSV edit → backfill demo works. Band boundaries stay stable, so property tests are stable. | Two sources must be documented clearly; `tariff_rate` needs an explicit meaning or removal |
| 2 | CSV carries only `billing_tier`, `subsidy_flag`, `effective_date`; all money from `base.yaml` | Simplest contract | A tariff correction becomes a config/code change — **destroys the backfill demo (§5.6)**. Rejected on that ground. |
| 3 | CSV carries everything including per-block rates (`block1_rate, block2_rate, block3_rate`); `base.yaml` carries only boundaries | Maximum restatement flexibility; single source for money | Wider CSV; more columns to validate; the daily file must be complete for every household every day |

### Decision

**Option 3.** The rule is one sentence: **every number that enters the bill arithmetic comes
from the day's tariff file, per household.** `config/base.yaml` holds only the *structure* of
the block tariff (the kWh boundaries) plus a clearly-labelled set of generator defaults that
`reference_dropper.py` uses to write files — and that nothing else is permitted to read.

**Final tariff contract — `tariff_YYYY-MM-DD.csv`:**

| # | Column | Type / domain | Meaning | In arithmetic? |
|---|---|---|---|---|
| 1 | `household_id` | `HH-nnnn` | Join key | key |
| 2 | `effective_date` | ISO date | Row applies for `sim_date >= effective_date` (T113 join rule) | join filter |
| 3 | `billing_tier` | `TIER_1 \| TIER_2 \| TIER_3` | Customer-class label. **Informational in `core/`** — the fixed charge is billed from column 6, not derived from this | no |
| 4 | `subsidy_flag` | `true \| false` | Household eligibility for the subsidy | yes — gate |
| 5 | `subsidy_pct` | decimal, `0 ≤ x ≤ 100` | Discount applied to the energy charge when the flag is true | yes |
| 6 | `fixed_charge` | decimal `≥ 0` | Fixed charge for this household for this day | yes |
| 7 | `block_1_rate` | decimal `≥ 0` | Rate per kWh, 0 → first boundary | yes |
| 8 | `block_2_rate` | decimal `≥ 0` | Rate per kWh, first → second boundary | yes |
| 9 | `block_3_rate` | decimal `≥ 0` | Rate per kWh above the second boundary | yes |
| 10 | `export_rate` | decimal `≥ 0` | Net-metering credit per exported kWh | yes |

Sample row: `HH-0042,2026-08-10,TIER_2,false,25.0,240.00,8.00,16.50,24.50,18.00`

Changes versus §6.2: **removed** `tariff_rate` (it had no defined meaning; in the §6.2 sample it
happened to equal block 3's rate, which is now explicit as `block_3_rate`); **added**
`subsidy_pct`, `block_1_rate`, `block_2_rate`, `block_3_rate`. The dropper writes the policy
`subsidy_pct` on every row regardless of the flag, so a restatement that flips a household's
eligibility needs to edit one column, not two.

**Weather contract — `weather_YYYY-MM-DD.csv`:** unchanged from §6.2 —
`grid_zone, forecast_date, cloud_cover_pct, temperature_c, solar_irradiance_index`.

**`config/base.yaml` `tariff` section becomes:**

```yaml
tariff:
  # STRUCTURE — the only tariff data core/ reads. Fixed for the life of the deployment.
  blocks:
    - { name: block_1, up_to_kwh: 60 }
    - { name: block_2, up_to_kwh: 120 }
    - { name: block_3, up_to_kwh: null }     # null = unbounded top block
  # GENERATOR DEFAULTS — read ONLY by simulators/reference_dropper.py to write the daily file.
  # core/, streaming/, batch/ and api/ MUST NOT read these (enforced by grep in T057).
  generator_defaults:
    block_rates: { block_1: 8.00, block_2: 16.50, block_3: 24.50 }
    fixed_charge_by_tier: { TIER_1: 120.00, TIER_2: 240.00, TIER_3: 480.00 }
    subsidy_pct: 25.0
    export_rate: 18.00
```

**Why not Option 1** (rates in config, per-household extras in CSV), which the task list had
initially leaned towards:

1. **It opens a lineage hole.** `config/base.yaml` is not archived per simulated day. If block
   rates live there and are changed in week two, a week-one backfill silently bills week-one
   consumption at week-two rates. That contradicts §2.2 ("yesterday's answer: reproducible for
   years"), §5.4 ("re-running yesterday's job over yesterday's partition produces byte-identical
   output"), and the §4.2 consistency argument that is the strongest reason Lambda was chosen.
   Under Option 3, a finalised bill is a pure function of three things: the raw Parquet
   partition, the archived tariff file for that day, and the block boundaries — the last of which
   are structure, not money, and are versioned in git.
2. **The design's own framing says rates are data.** §2.3: the slow dimension is "customer
   records, price lists, regulatory rates". A price list that lives in a config file is not a
   slow dimension; it is code.
3. **The stale-tariff divergence needs something monetary to move.** §3.1's provisional estimate
   is costed against *yesterday's* tariff on purpose. That produces a visible speed-vs-batch
   delta only if the tariff actually changes between days, and rates are the natural thing to
   change. With rates frozen in config, the only movers would be per-household fixed charges and
   flags, which is a weak story to tell in the viva.

**Why not Option 2:** it makes a tariff correction a config or code change, which removes the
§5.6 backfill demo. Rejected outright.

The cost of Option 3 over Option 1 is three CSV columns and three Pydantic fields. The daily file
must be complete for every household — T113 already fails the job loudly if it is not, which is
the right behaviour regardless.

### Consequence

**The lineage sentence for the viva:** *a finalised bill is a pure function of the raw Parquet
partition for `sim_date`, the archived tariff file for `sim_date`, and the block boundaries in
git. Nothing that can change day to day lives outside the archived file.*

Per task:

- **T019 `base.yaml`:** `tariff` section exactly as above. The Appendix A keys
  `fixed_charge_by_tier`, `subsidy_discount_pct`, `export_credit_rate` move under
  `generator_defaults` and are renamed as shown; `blocks[].rate` is removed.
- **T021 `config.py`:** `TariffStructure` model with `blocks` and `generator_defaults`. Validate:
  `up_to_kwh` strictly increasing, exactly one trailing `null`, and `len(blocks) == 3` matches
  the number of `block_n_rate` columns the contract defines — fail at load, not at first bill.
- **T031 `reference.py`:** `TariffRecord` with the ten columns. Validators: rates `≥ 0`;
  `0 ≤ subsidy_pct ≤ 100`; `billing_tier` in the configured set; `effective_date` parses.
  Do **not** require `block_1_rate ≤ block_2_rate ≤ block_3_rate` — monotonicity and continuity
  of the bill hold for any non-negative rates, and constraining the ordering would only make the
  restatement demo less flexible.
- **T057 / T058 `core/tariff.py`:** add `build_blocks(boundaries, tariff_record) -> list[Block]`,
  which zips the config boundaries with the record's three rates. `energy_charge(kwh, blocks)`
  keeps its signature. `compute_bill(netting, tariff_record, boundaries)` takes **every**
  monetary parameter from the record. Enforcement, added to the task's *Done when*:
  `grep -rn "generator_defaults" src/voltstream/core src/voltstream/streaming src/voltstream/batch src/voltstream/api`
  returns nothing.
- **T062 `spark_expr.py`:** block rates are **Column references** (`F.col("block_1_rate")` …)
  from the joined tariff DataFrame, never literals. Only the boundaries come from config, at
  expression-build time.
- **T060 property tests / T063 consistency test:** strategies draw random non-negative rate
  schedules and random `subsidy_pct` too. Monotonicity and continuity must hold for *any* such
  schedule — the tests become stronger, not weaker.
- **T074 `reference_dropper.py`:** writes all ten columns, sourcing `billing_tier`,
  `subsidy_flag` from the `households` dimension (T038/T042) and money from
  `generator_defaults`. It must apply a **deterministic, documented day-over-day change** — e.g.
  `block_2_rate` steps by a fixed amount on alternate simulated days — so the stale-tariff
  divergence is non-zero and attributable in T140. Put the rule in the module docstring.
  **As built (R01, 2026-09-27): the step is on `block_1_rate`, +0.50 on even date
  ordinals.** Block 2 starts at 60 kWh and no simulated household uses more than about
  22 kWh a day, so a block 2 step changed no bill and `tariff_effect` was 0.00 everywhere.
- **T113 `daily_billing.py`:** joins all ten columns. Missing household → fail loudly (already
  specified).
- **T162 `backfill.sh`:** the corruption step edits a monetary column in the day's CSV (e.g.
  `block_2_rate` `16.50 → 61.50`, large enough to be obvious in a screenshot), runs the DAG,
  restores the file, backfills. **Corrupt a column every bill uses** — `block_1_rate` or
  `fixed_charge` — not `block_2_rate`: for the reason in the T074 note above, a block 2 edit
  leaves every bill unchanged and the demo shows nothing.
- **T034 `03-data-contracts.md`:** the table above is the frozen tariff contract.
  **T182:** update Master Design §6.2's CSV sample and Appendix A's `tariff` block.
- **Coupling to D4:** if D4 chooses option 1 (API recomputes the provisional bill), the API must
  be able to fetch the full tariff row for `(household_id, tariff_source_date)` — from the
  archived parquet via `objectstore.py`, or from a small `tariff_daily` Postgres table populated
  when the file is consumed. Decided in D4; noted in its context.
- **Coupling to D5:** the subsidy term becomes
  `energy_charge × tariff.subsidy_pct / 100 if tariff.subsidy_flag else 0`. D5's proposed
  formula is updated below.
- **Optional lineage hardening (recommended, cheap):** record the git SHA in `pipeline_runs`
  (T039 column, T115 write) so the "block boundaries in git" leg of the lineage sentence is
  pinned per run rather than assumed.

---

## D3 — Watermark units and value

**Status:** ✅ decided 2026-09-19 · **Resolved in:** T004

### Context

Appendix A gives `speed_layer.watermark_seconds: 30`. The watermark is applied to `event_ts`,
which is **simulated** time (§3.4). With `TIME_SCALE = 288`:

| Reading of "30 seconds" | Effective watermark |
|---|---|
| 30 simulated seconds | **0.104 real seconds** — effectively no watermark |
| 30 real seconds, converted | 30 × 288 = 8,640 simulated seconds = **2.4 simulated hours** — holds ~10 of the 15-minute windows open at once |

Neither is what the design intends. A second fact changes how the value should be chosen:
the producer stamps all 50 households with the **same** simulated instant every tick, so the
natural event-time skew between concurrently arriving events is ~0. **All lateness in this
system is injected deliberately** by `faults.out_of_order_rate`. The watermark must therefore
be sized against the injected lateness spread, not against network jitter — and the spread
itself is undefined in the Master Design.

Finally, the speed layer is *supposed* to drop some stragglers (§5.3: "low latency, drop
stragglers"). That dropped data is one of the two causes of the speed-vs-batch delta that the
merge demo (§3.2) and the reconciliation metric (§9 Phase 3) depend on. A watermark that
drops nothing makes the Lambda demonstration invisible.

### Options

| # | Option | For | Against |
|---|---|---|---|
| 1 | Keep the watermark in **event-time (simulated) units**; rename the key to `watermark_sim_minutes`; define `faults.out_of_order_spread_sim_minutes` alongside it; set the watermark *smaller* than the spread. Suggested start: spread uniform over 1–20 simulated minutes, watermark 5 simulated minutes. | Unit is unambiguous at the call site; the dropped fraction is tunable and predictable; delta is guaranteed non-zero | Must be explained in the report (§3.4 already flags watermark unrealism as a limitation) |
| 2 | Express in real seconds; convert inside `simclock.py` | Reads naturally to an operator | Realistic-sounding values produce absurd simulated watermarks; every reader must do the ×288 in their head |

### Decision

**Option 1 — event-time (simulated) units — but with different numbers from the ones
suggested in the options table, because modelling the actual Spark semantics showed those
numbers would demonstrate nothing.**

#### What the model showed

A sizing model was run (2026-09-19) with the project's own constants — tick 9.6 sim min,
window 15 sim min, trigger 10 real s = **48 sim min** — against the two Spark rules that
govern late data in a windowed aggregation:

1. The watermark used during micro-batch *k* is `max(event_ts in batch k−1) − W`. **It advances
   once per trigger.** At this project's time scale, one trigger is three windows wide.
2. A late row is dropped iff its **window end** ≤ watermark — the predicate is on the window,
   not on the row's own `event_ts`.

Consequences, for a late event arriving `L` sim min behind the current tick:

| Bound | Value | Meaning |
|---|---|---|
| Guaranteed **included** | `L ≤ W` | Regardless of where in the micro-batch it lands |
| Guaranteed **dropped** | `L ≥ W + 48 (trigger) + 9.6 (tick) + 15 (window) ≈ W + 73` | Regardless of timing |
| In between | probabilistic | Depends on the phase of the micro-batch it happens to arrive in |

Measured drop fractions:

| Scenario | Drop rate of late events | Share of all events |
|---|---|---|
| Doc literal: 30 *simulated* seconds, lateness U[1,20] | 5.4 % | 0.16 % |
| Task-list suggestion: W = 5, lateness U[1,20] | **2.9 %** | **0.09 %** — invisible |
| W = 30, network reordering U[1,30] | 0 % (by construction, `L ≤ W`) | 0 % |
| W = 30, store-and-forward backfill after a 30 real-s dropout (readings 10–144 sim min late) | **57.5 %** of each backfill | see below |

The lesson: **at TIME_SCALE 288 the trigger interval, not the watermark, is the dominant term
in what gets dropped.** A watermark smaller than one trigger is decorative. This is §3.4's
"time compression scales event time but not processing time" limitation with a number on it,
and belongs in the report verbatim.

#### The decision

Two regimes of lateness, deliberately separated so the divergence is attributable:

| Regime | Mechanism | Lateness | Speed-layer behaviour |
|---|---|---|---|
| **Network reordering** | `faults.out_of_order_rate: 0.03` — individual readings with `event_ts` shifted back by U[1, 30] sim min | ≤ W | ~~**Always absorbed.**~~ **Superseded — see the measurement below.** Predicted absorbed by construction; measured otherwise. |
| **Comms outage** | `faults.dropout_probability_per_meter_tick: 0.002` — the meter goes silent for 30 real s (144 sim min), **buffers** its readings, and flushes all of them on reconnect with their original `event_ts` (store-and-forward) | 10–144 sim min | **~57 % dropped**, the rest absorbed. The batch layer sees all of them from Parquet. This is §5.4 reason 2 ("a meter was offline and backfills") made concrete. |

#### Correction (measured 2026-09-24, T094)

**The "always absorbed" prediction above is wrong, and the measurement supersedes it.**

Disabling dropouts and leaving reordering on should take the speed layer's 15-minute gap
to zero. Measured over a complete simulated day on an idle host, it was **0.9996 %** —
against **1.1624 %** with dropouts also enabled. So of the energy the 15-minute view
misses, roughly 1.0 percentage point is *reordering* and only about 0.16 is dropout
backfill. That is the reverse of the attribution this decision assumed.

An earlier run on a loaded host gave 0.5635 % and 0.3712 %, and the discrepancy was
provisionally blamed on CPU starvation advancing the watermark in leaps. Re-running idle
refuted that: the gap went *up*, not to zero.

The cause is the one this document already identifies two sections above and then
contradicts: **the trigger interval is 48 simulated minutes and the watermark is 30**, and
*"a watermark smaller than one trigger is decorative"*. Both claims are in D3. Only the
second survives measurement.

**What this changes.**

- The `L ≤ W` guarantee does not hold at this time compression. Reordering within the
  watermark is dropped.
- `T140`'s attribution — "the kWh gap is dropped backfill, nothing else" — is false as
  written and must be stated as measured instead. **A prediction for Phase 11, so it is
  not rediscovered there** (*superseded by the R02 update below*): `T140` splits
  divergence into `data_effect` (backfill dropped past the watermark) and `tariff_effect`
  (yesterday's rates vs today's), and expects `data_effect` to carry D3's ~1.7 %. It will
  not. `reconciliation_daily` compares a household's *daily* speed bill against its batch
  bill, and the daily window loses nothing — so `data_effect` should come out at or near
  **zero**, with essentially all divergence in `tariff_effect`. `T140`'s stated acceptance
  ("the `data_effect` share is inside T094's band") therefore cannot pass as written, for
  the same structural reason T094's original framing could not: the band was measured on
  15-minute windows, and bills are daily.
- *Superseded by the R02 update below.* The daily household totals are **unaffected**:
  they use a 1-day window, which tolerates lateness far beyond anything the fault model
  injects, and measured 0.00 % missing in every run. The speed-versus-batch divergence
  *on bills* therefore remains the stale tariff alone, exactly as designed.

**What would fix it, and why we did not.** Raising the watermark above one trigger — 60
simulated minutes rather than 30 — is what this decision's own analysis implies if the
stated intent is to be met. It is recorded as a production-scale recommendation rather
than applied: the measured finding is more useful than the tuned number, because it
demonstrates the time-compression limitation (§3.4) with data rather than asserting it.

Full figures and method: `docs/assumptions.md` §2.

#### Update (2026-09-27, R02): the daily totals now miss backfill

The two statements above that the daily totals lose nothing, and that `data_effect` will
sit near zero, described a speed layer that did not deduplicate. It summed the 2 %
injected duplicates the batch layer removes, and T094's probe compared it against an
archive that was validated but not deduplicated, so the 0.00 % was two omissions agreeing
(backlog R02).

The speed layer now deduplicates on the batch layer's key (`core.keys.DEDUP_COLUMNS`)
before either aggregation, and Spark's streaming dedup drops every record older than the
watermark, whichever window it would have fallen in. So:

- **Backfill older than the watermark no longer reaches the daily totals.** The daily gap
  is D3's original prediction again, ≈ 1.7 % of consumption and inside T094's 0.25–5 %
  band, and `data_effect` carries it: the speed layer bills less energy than the batch
  layer.
- **Reordering is still not dropped at the daily grain.** A reordered record is sent with
  its own tick, and a micro-batch's watermark is the newest tick of the batch before it
  minus W, so it trails the record's tick by at least W. The probe's control run
  (dropouts off) asserts a daily gap under 0.05 %, a few readings: the last idle run
  before R02 measured 0.0162 % there, about one reading, and the cause is not pinned
  down.
- **The 15-minute view loses at least what it did.** The dedup tests the record's own
  `event_ts` against the watermark, which is stricter than the window-end test, so backfill
  that used to land in a still-open window is now dropped before it gets there.

**Measured on the Gate 4 run (2026-09-27):** the daily gap was **0.523 %** on the first
complete day (0.861 % on the partial first day), inside T094's band and below the modelled
1.7 %. D4's "Measured attribution" gives the breakdown and the likely reasons. The probe
(`tests/integration/test_watermark_behaviour.py`) has not been re-run, so the control,
reordering alone, is still unmeasured since R02.

Pinned values:

```yaml
speed_layer:
  window_sim_minutes: 15               # tumbling, event time
  watermark_sim_minutes: 30            # event time; = 6.25 real s; ≥ max network reordering
  trigger_interval_real_seconds: 10    # = 48 sim min; sets watermark advancement granularity
  output_mode: update                  # see Consequence — the watermark is NOT on the latency path

faults:
  out_of_order_rate: 0.03
  out_of_order_lateness_sim_minutes: [1, 30]     # uniform; upper bound == watermark, on purpose
  dropout_probability_per_meter_tick: 0.002      # ≈ 0.3 dropouts per meter per sim day
  dropout_duration_real_seconds: 30              # = 144 sim min = 15 buffered readings
  dropout_backfill: true                         # store-and-forward on reconnect

batch:
  late_data_grace_real_seconds: 90     # see Consequence — wait this long after day close before the rescan
```

Why `dropout_probability` drops from the Appendix A value of 0.01 to 0.002: at 0.01, 15 % of all
readings arrive as backfill and the modelled speed-vs-batch kWh gap is **8.6 %** — above the
5 % `LambdaDivergenceHigh` threshold in steady state, so the alert would be permanently firing.
At 0.002, 3 % of readings are backfill and the gap is **≈ 1.7 %** (Poisson noise on ~15
dropouts/day gives a typical range of ~1.2–2.2 %, tails ~0.6–3 %). That sits comfortably under
the alert and is still unmistakable in a screenshot.

**Expected divergence, for T094 and T140:** kWh gap ≈ 1.7 % of daily consumption, caused
*entirely* by dropped backfill (network reordering contributes zero by construction). Money
divergence = that kWh gap **plus** the stale-tariff effect from D2. T094 asserts the kWh gap in
the band **0.25 % – 5 %**; T140 attributes the two components.

**Why not Option 2** (real seconds, converted): every plausible-sounding real value maps to an
implausible simulated one, and the model above shows the value must be reasoned about in the
same units as the trigger interval and the window, both of which are already expressed per
event time.

### Consequence

- **T019 `base.yaml`:** keys exactly as pinned above. Every key name carries its unit
  (`_sim_minutes`, `_real_seconds`). Remove `watermark_seconds`, `window_minutes`,
  `trigger_interval_seconds`, `dropout_probability`, `dropout_duration_seconds`.
- **T070 `faults.py`:** two distinct mechanisms. Out-of-order shifts `event_ts` back by
  U[1, 30] sim min. Dropout is a **per-meter state machine**: on trigger, buffer readings for
  `dropout_duration_real_seconds`; on expiry, emit the whole buffer in one tick with original
  timestamps. With `dropout_backfill: false` the buffer is discarded instead (a switch for the
  demo, so the two behaviours can be contrasted). Tests assert the backfill preserves
  `event_ts` and emits exactly 15 readings.
- **T088 `speed_layer.py`:** `withWatermark("event_ts", "30 minutes")` — the string is built
  from `watermark_sim_minutes`, with a comment that these are simulated minutes.
  `outputMode("update")` with `foreachBatch` upsert. In update mode the watermark does **not**
  add to dashboard latency: every micro-batch upserts the windows it touched, so a window is
  visible ~10–12 real s after its first event and is *revised* as absorbed late data arrives.
  The watermark governs only when a window becomes immutable (state evicted, further late data
  dropped) — ~16 real s after the window ends at W = 30.
- **T090 per-household daily total:** group by `window(event_ts, "1 day")` (not by a derived
  `sim_date` column) so the same watermark evicts its state ~16 real s after simulated midnight.
  Grouping by a derived date column would never evict.
- **T094 `test_watermark_behaviour.py`:** assert the speed-layer daily kWh total is below the
  Parquet total by **0.25 % – 5 %**; also assert that with `dropout_probability_per_meter_tick:
  0` the gap is exactly zero — that second assertion is what proves network reordering is fully
  absorbed and the attribution in T140 is honest.
- **T121 / T122 `daily_billing_dag.py` — a correctness bug D3 exposes:** a dropout that
  straddles simulated midnight flushes readings for day D−1 up to 30 real s *after* midnight,
  and the archiver commits them up to one trigger later. The tariff file also lands at
  midnight, and the sensor pokes every 30 s. Without a wait, the rescan can start **before**
  the last of D−1's readings reach the partition, and the batch layer — whose entire
  justification is completeness — misses them. Add a wait of
  `batch.late_data_grace_real_seconds: 90` between the sensor succeeding and the job
  submitting. Still well inside the §2.5 "batch < 10 real min after day close" NFR.
- **Limitation to state (§10.2):** an outage longer than the grace period that straddles
  midnight is not caught by that day's run. Production reprocesses D−1 on a delayed schedule
  or runs a second late-data pass; this project does not.
- **T144 `MeterDataStale`:** unaffected — it fires on a *zone* going silent for > 2 real min.
  A single-meter dropout of 30 real s leaves the zone's other meters reporting.
- **T182 Master Design corrections:** §3.4's table says 144 readings per meter per day and
  ~7,200 events per day; at a 2 real-s tick over a 5 real-min day it is **150** and
  **~7,500**. §5.3 "with a 30-second watermark" → 30 *simulated-minute* watermark. §10.3's
  latency table lists "Watermark wait ~30 s" as a contributor to end-to-end latency; under
  update mode it is not — reword to say it governs finality, not first visibility.
- **Report (§10.1 / observability chapter):** the modelled bounds table above, and the sentence
  *"at our time scale one trigger interval is three windows wide, so the trigger interval, not
  the watermark, dominates late-data handling."* It is the most concrete evidence in the report
  that the watermark was reasoned about rather than copied.

---

## D4 — Who computes the provisional bill

**Status:** ✅ decided 2026-09-19 · **Resolved in:** T005

### Context

§4.4 justifies keeping the tariff logic twice (`core/tariff.py` scalar Python and
`core/spark_expr.py` Column expressions) on the grounds that "the FastAPI container has no
Spark but still computes provisional estimates." But §6.4 declares
`household_running_rt.estimated_bill NUMERIC(12,2) NOT NULL`, which implies the **speed layer**
computes and stores it. If the API never calls `core/tariff.py`, the pure module has no
production caller, and the §4.4 argument — the single most important architectural claim in
the viva — is hollow.

**Coupling from D2.** Every monetary input now lives in the per-household tariff row, not in
config. So if the API is to recompute anything, it needs the full ten-column `TariffRecord` for
`(household_id, tariff_source_date)` — the `tariff_source_date` is already stored on
`household_running_rt`. Two ways to get it: read the archived parquet for that date via
`objectstore.py`, or add a small `tariff_daily` Postgres table that the batch job populates when
it consumes the file. Whichever option is chosen below must say which.

### Options

| # | Option | For | Against |
|---|---|---|---|
| 1 | **Both, deliberately.** Speed layer persists `estimated_bill` via `spark_expr.py` (reconciliation and Grafana read the stored value). The merge endpoint recomputes the provisional figure from the stored kWh columns via `tariff.py`, returns the recomputed value, and logs a warning if it differs from the stored one. | Both implementations have a real production caller; the consistency test (T063) is what guarantees they agree — exactly the §4.4 story | Two computations of the same number; must be explained as intentional |
| 2 | Speed layer only. API reads `estimated_bill` as-is; `tariff.py`'s only callers are tests and `/reports`. | Simpler | Weakens §4.4 — "the API computes provisional estimates" becomes untrue |
| 3 | API only. Speed layer stores kWh; `estimated_bill` column dropped or made nullable; reconciliation computes the speed estimate on the fly. | Single computation | Reconciliation and Grafana lose a stored speed figure; schema change; `spark_expr.py` tariff logic has no production caller instead |

### Decision

**Option 2, refined: the speed layer computes and persists the full provisional breakdown;
the API is a reader; the pure module's production caller is `reconciliation.py`, not the
API.** This is not the option the task list leaned towards. The reasoning follows.

#### Where each piece of billing logic runs

| Component | Module used | Input tariff | Output |
|---|---|---|---|
| Speed layer (Spark) | `core/spark_expr.py` | yesterday's file (`tariff_source_date = sim_date − 1`) | `household_running_rt` — kWh **and** every bill component |
| Batch layer (Spark) | `core/spark_expr.py` | today's file | `household_bill_daily` |
| Reconciliation (plain Python, 50 rows) | **`core/tariff.py`** | today's file | the **counterfactual** `bill(speed_kwh, today's tariff)`, used to split divergence |
| API | none — `SELECT` only | — | serves whichever row the merge rule picks |
| Tests | `core/tariff.py` as oracle | generated | `test_pure_vs_spark.py` pins `spark_expr` to it |

#### Why the API does not recompute (against Option 1)

1. **Speed and batch import the same `spark_expr.py` functions.** Between the two Lambda
   layers there is no drift *by construction*. The consistency test exists to pin that shared
   implementation to a specification, not to referee two production callers.
2. **Option 1 would make the API a third implementation site serving user-visible numbers** —
   it *adds* a drift surface (API vs speed) and then patches it with a runtime warning, for
   something the consistency test already guarantees before deployment.
3. **It contradicts the design's own boundary for the API.** §6.1 marks the API's connection to
   Postgres "SELECT only"; §8.3 says "it merely listens … it queries Postgres when asked". A
   tariff-access path (a new `tariff_daily` table, or MinIO reads with day-rollover caching)
   is a real component with real failure modes, sitting inside the graded centrepiece (§3.2:
   "nine lines of logic") for no functional gain.
4. **The only thing recomputation would add is the breakdown shape** for provisional responses
   — and `spark_expr.py` already builds `tier_breakdown` and every component for the batch
   path, so the speed layer stores them at no cost. The provisional and final API responses
   then have identical shape without the API computing anything.

#### Why the pure module still earns its place (the replacement for §4.4's sentence)

- **It is the executable specification.** Nine lines of scalar arithmetic that can be printed
  in the report; `spark_expr.py` is a tree of `F.when`/`F.least` that cannot.
- **It is the test oracle.** `hypothesis` can run thousands of examples against it in
  milliseconds; a Spark round-trip per example is seconds. Property tests (T060) are only
  practical against the pure module.
- **It has one production caller with a non-redundant job.** `reconciliation.py` computes, per
  household, `C = bill(speed_kwh, today's tariff)` and decomposes the day's divergence:

  ```
  S = estimated_bill   (speed:  speed_kwh  × yesterday's tariff)   — stored, spark_expr
  B = final_bill       (batch:  batch_kwh  × today's tariff)       — stored, spark_expr
  C = bill(speed_kwh, today's tariff)                              — computed, core/tariff.py

  tariff_effect = S − C      # what the stale tariff cost
  data_effect   = C − B      # what the dropped backfill cost (D3)
  S − B         = tariff_effect + data_effect                      # identity
  ```

  The split is only *meaningful* if `C` comes from the same function as `S` with only the
  tariff changed — which is exactly what the consistency test asserts. **The consistency test
  therefore has a production consequence, not just a green tick.** That is a stronger viva
  answer than "the API recomputes what the speed layer already stored".

**Why not Option 3:** moving all billing out of the speed layer makes it a pure kWh aggregator,
so the two Lambda layers no longer share billing logic and the §4.4 argument shrinks to netting.
Reconciliation and Grafana also lose a stored speed figure.

### Consequence

- **T035 `01_schema.sql` — `household_running_rt` gains the bill components** so provisional
  and final responses have identical shape: `self_consumed_kwh NUMERIC(12,4)`,
  `energy_charge`, `fixed_charge`, `subsidy_discount`, `export_credit` (all `NUMERIC(12,2)`),
  and `tier_breakdown JSONB`, all `NOT NULL`. `estimated_bill` stays as the total.
  **T182:** update §6.4.
- **T039 — `reconciliation_daily` gains** `tariff_effect NUMERIC(12,2) NOT NULL` and
  `data_effect NUMERIC(12,2) NOT NULL`. `abs_divergence` and `pct_divergence` stay.
  **T182:** update §6.4.
- **T091 speed layer:** computes every component with `spark_expr.py` against the broadcast of
  yesterday's tariff and persists all of them. `tariff_source_date` is what tells a reader the
  figure is stale, so it is always populated (D3/T075 guarantees a day-zero file).
- **T101 repositories:** `get_running_estimate` returns the full row, not the scalar.
- **T106 `models.py`:** one `BillResponse` model for both branches. Fields: `household_id`,
  `sim_date`, `source`, `provisional`, `tariff_date` (speed → `tariff_source_date`; batch →
  `tariff_effective_date`), the five kWh figures, the four charge components,
  `tier_breakdown`, `total` (speed → `estimated_bill`; batch → `final_bill`), and
  batch-only optionals `readings_count`, `duplicates_removed`, `pipeline_run_id`,
  `computed_at` (null when provisional).
- **T127 merge function:** stays nine lines and does **no arithmetic** — pick a row, map it to
  `BillResponse`. Its *Done when* gains: `grep -n "core.tariff\|core.netting"
  src/voltstream/api/` returns nothing.
- **T129 `/bill/delta`:** returns `tariff_effect` and `data_effect` from `reconciliation_daily`
  alongside the totals when the day is reconciled — the decomposition becomes visible in the
  app, not only in a table.
- **T137 `reconciliation.py`:** plain Python — no `SparkSession` for 50 rows. Reads
  `household_running_rt` and `household_bill_daily` via `repositories.py`, reads today's tariff
  from the landing CSV via `objectstore.py` (so a post-backfill reconciliation uses the
  corrected file), computes `C` with `core/tariff.py`, writes the two effects. It runs on the
  **app** image (`[api,sim]` deps — psycopg and boto3 already present), which is a D6 input:
  the DAG launches three containers, two Spark and one app.
- **T138 metric:** `voltstream_lambda_divergence` stays a label-less gauge, as §10.1
  specifies, on the day's mean `pct_divergence` (base defined in D5 — matches the `_pct` alert
  threshold). The two effects are charted from the Postgres datasource (T154), not as extra
  metric series.
- **T140 attribution:** no longer estimated — read from `reconciliation_daily`. The report
  states the day's mean `tariff_effect` and `data_effect` and checks the latter against D3's
  modelled ~1.7 % kWh gap.
- **T179 / T182 — replace §4.4 item 2's justification.** Current text: "the FastAPI container
  has no Spark but still computes provisional estimates". Replacement:

  > `core/tariff.py` holds the billing rules as scalar Python — the executable specification,
  > small enough to print in this report and fast enough to property-test over thousands of
  > generated inputs. `core/spark_expr.py` holds the same rules as Spark Column expressions —
  > the production implementation, imported unchanged by both the speed and batch jobs, so those
  > two cannot drift from each other by construction, and written as Column arithmetic rather
  > than UDFs to preserve Catalyst optimisation. `tests/consistency/test_pure_vs_spark.py` pins
  > the implementation to the specification over ≥ 5,000 inputs. The specification also has one
  > production caller: `reconciliation.py` uses it to compute the counterfactual bill that splits
  > each day's speed-vs-batch divergence into a tariff effect and a data effect — a decomposition
  > that is only meaningful because the consistency test holds.

- **Coupling to D2, closed:** the API needs no tariff row, so neither a `tariff_daily` table
  nor MinIO reads from the API are required. The only new tariff reader is `reconciliation.py`,
  which already has `objectstore.py`.

### Measured attribution (T140)

**Status: measured 2026-09-27, on the Gate 4 run** (the first run with R01 and R02
fixed; `assumptions.md` §7). Both days in the table below come from that one run. Add rows
from later runs; never fill the table from the expectations further down.

**Where the numbers come from.** `batch/reconciliation.py` logs every figure below in its
`reconciliation complete` line: `mean_tariff_effect`, `mean_data_effect`,
`tariff_share_pct`, `mean_pct_divergence`, `max_pct_divergence` and
`speed_kwh_shortfall_pct`. The same numbers straight from Postgres:

```sql
-- Split of the divergence, per day.
SELECT count(*)                                AS households,
       round(avg(tariff_effect), 2)            AS mean_tariff_effect,
       round(avg(data_effect), 2)              AS mean_data_effect,
       round(100 * sum(abs(tariff_effect))
             / nullif(sum(abs(tariff_effect)) + sum(abs(data_effect)), 0), 3)
                                               AS tariff_share_pct,
       round(avg(pct_divergence), 3)           AS mean_pct_divergence,
       max(pct_divergence)                     AS max_pct_divergence
FROM reconciliation_daily
WHERE sim_date = DATE '<sim_date>';

-- data_effect in kWh terms: the energy the speed layer did not see, as a percentage of
-- what the batch layer billed. Same sign as T094's gap: positive = speed saw less.
SELECT round(100 * (sum(b.consumption_kwh) - sum(s.consumption_kwh))
             / sum(b.consumption_kwh), 3)      AS speed_kwh_shortfall_pct
FROM household_bill_daily b
JOIN household_running_rt s ON s.household_id = b.household_id AND s.sim_date = b.sim_date
WHERE b.sim_date = DATE '<sim_date>';
```

`tariff_share_pct` uses absolute values because the two effects can have opposite signs for
the same household and would otherwise cancel.

| sim_date | households | mean `tariff_effect` | mean `data_effect` | tariff share | mean pct | max pct | speed kWh shortfall |
|---|---|---|---|---|---|---|---|
| 2026-01-01 ¹ | 50 | +5.14 | −0.89 | 85.203 % | 1.570 % | 3.537 % | 0.861 % |
| 2026-01-02 | 50 | −5.35 | −0.61 | 89.742 % | 1.878 % | 4.450 % | 0.523 % |

¹ Partial: the stack starts at the clock anchor, so the first simulated hours of day one
have no readings (6,296 readings processed against 6,787 on day two). Quote day two.

Per row, on both days: `tariff_effect` non-zero for all 50 households; `data_effect`
non-zero for 12 (day one) and 10 (day two) households, all of them negative; the D4
identity holds on every row. Day two's `tariff_effect` ranged from −9.84 to −0.21, and
its `data_effect` from −5.63 to 0.00.

**Expected result with the code as of 2026-09-27**, after R01 and R02. Written down before
the run, so the run confirms or refutes it. It replaces the 2026-09-26 prediction
(`tariff_effect` 0.00 everywhere, `data_effect` a ~2 % surplus from duplicates), which
described those two defects rather than the design. Details and evidence are in
`docs/debugging-backlog.md`.

- **`tariff_effect` non-zero for every household, with a sign that alternates by day**
  (R01). The dropper's day-over-day change is now `block_1_rate` ± 0.50, and every
  simulated household's billable import falls in block 1. So `tariff_effect` ≈ 0.50 × the
  household's billable kWh, less its subsidy share. It is negative when the billed day
  has an even date ordinal (2026-01-02, 2026-01-04, …), because that day's tariff is dearer
  than the one the speed layer used, and positive on odd days.
- **`data_effect` zero for most households, and mostly negative for the rest** (R02). The
  speed layer now deduplicates on the batch layer's key, so duplicates no longer appear
  here. What remains is store-and-forward backfill older than the watermark, which the
  speed layer drops and the batch layer bills. At 0.002 dropouts per meter tick, roughly
  a quarter of meters drop out on a given day. A household with solar can come out
  positive when the readings it lost were mostly generation.
- **`speed_kwh_shortfall_pct` positive, near D3's modelled 1.7 %**, inside T094's
  0.25 %–5 % band: the speed layer sees *less* energy than the batch layer.

**Consequence for T140's acceptance check.** It can now pass as written. `data_effect` is
the backfill the watermark dropped, which is what T094's band measures, and T094's probe
asserts that band at the daily grain (`test_daily_totals_miss_backfill_past_the_watermark`).
Check the band against `speed_kwh_shortfall_pct`, the kWh measure T094 uses, rather than
`tariff_share_pct`: that is a split in money, and it depends on the size of the tariff
step as much as on the data.

**Result: the expectation held, and T140's acceptance check passes.**

- **`tariff_effect`**: non-zero for every household, positive on the odd day (+5.14 mean)
  and negative on the even day (−5.35), as predicted. HH-0001, which has no solar and no
  subsidy, shows the rule exactly: 13.4363 kWh × −0.50 = −6.72 on day two.
- **`data_effect`**: zero for most households and negative for the rest. On day two the
  10 non-zero households match the 10 dropouts the producer injected that day.
- **`speed_kwh_shortfall_pct`**: 0.523 % on day two (0.861 % on the partial day one).
  That is inside T094's 0.25 %–5 % band, so the acceptance check passes.
- **Share of divergence**: the tariff effect carries about 90 % of it in money terms. That
  is the size of the 0.50 step, not a statement about the data.

**Below D3's modelled 1.7 %, for two reasons, one of them measured.**

- **Fewer dropouts than modelled** (measured). Day two had 10 dropouts, 126 backfilled
  readings or 1.9 % of the day, against the ~14 the model's rate implies. That is Poisson
  noise.
- **A smaller share of each backlog dropped** (inferred). The speed layer missed about
  3.8 kWh, roughly 35 readings or a quarter of the backlog, where D3's model has 57.5 %.
  The micro-batches ran at the 10 s trigger throughout (median 10.0 s), so a starved
  driver is not the cause. A likely one, not yet verified: since Spark 3.4, stateful
  operators filter late rows against the *previous* batch's watermark, which gives a
  backfilled row one more trigger (48 simulated minutes) of slack than D3's model allows.
- **Also on day two**: the producer emitted 138 ticks, not 150, because stalled ticks are
  never made up (backlog R37). That shrinks the day, not the ratio.

The report should quote the measured figures and name the model as a model.

---

## D5 — Subsidy and final-bill arithmetic

**Status:** ✅ decided 2026-09-19 · **Resolved in:** T006

### Context

Appendix A gives `subsidy_discount_pct: 25.0` with no stated base, and §3.3c stops at netting.
`core/tariff.py` and `core/spark_expr.py` must implement one formula identically, and the
property tests (§9 Phase 2) constrain what that formula may be:

- **Continuity** at block boundaries is only satisfiable if the blocks are **marginal/slab**
  (first 60 kWh at 8.00, next 60 at 16.50, remainder at 24.50). A "whole consumption charged at
  the top reached rate" reading is discontinuous at every boundary by construction.
- The fixed charge is keyed off the household's `billing_tier` from the CSV, **not** off
  consumption, so it is a per-household constant and does not break continuity.

Three things are undefined and must be pinned:

1. What the subsidy percentage applies to (energy charge only? energy + fixed?).
2. Whether `final_bill` is floored at 0 — a large solar exporter can go negative.
3. The rounding rule and precision, so both implementations round identically and the
   consistency test can assert exact equality rather than a loose tolerance.

**Evidence from T002 (2026-09-19).** A local Spark run of the marginal block formula at the
first boundary produced a concrete example of why sub-question 3 is not cosmetic:

| `billable_import_kwh = 60.01` | Result |
|---|---|
| float64 — Python `round(60*8.00 + (60.01-60.0)*16.50, 2)` | `480.16` (raw `480.16499999999996`) |
| float64 — Spark `F.round(..., 2)` over `DoubleType` | `480.16` |
| `decimal.Decimal`, `ROUND_HALF_UP` to 0.01 | **`480.17`** (raw `480.1650`) |

The two float64 paths agree with each other, so a consistency test built on `DoubleType` would
**pass while both layers are a cent wrong** at every block boundary. That is a billing system
producing legally-actionable figures (§2.2) from binary fractions. Sub-question 3 therefore also
has to decide the **numeric type**, not only the rounding mode: `Decimal` on the Python side with
`DecimalType(12,4)` / `DecimalType(12,2)` on the Spark side (matching the Postgres `NUMERIC`
columns in §6.4 exactly), versus float64 everywhere with the discrepancy accepted and stated.

### Options

Proposed formula (option A for sub-question 1). Per D2, every monetary parameter is a field of
the day's `TariffRecord` (`tariff.*` below); only the block *boundaries* come from config:

```
blocks           = build_blocks(config.tariff.blocks, tariff)       # boundaries ⨯ tariff.block_n_rate
energy_charge    = Σ over blocks of (kwh_in_block × block.rate)      # marginal / slab
fixed_charge     = tariff.fixed_charge                               # per household, per day
subsidy_discount = energy_charge × tariff.subsidy_pct / 100  if tariff.subsidy_flag else 0
export_credit    = export_kwh × tariff.export_rate
final_bill       = energy_charge + fixed_charge − subsidy_discount − export_credit
```

| Sub-question | Option A | Option B |
|---|---|---|
| Subsidy base | Energy charge only (proposed) | Energy + fixed charge |
| Negative bill | Allow negative; report as credit carried forward (proposed) | Clamp at 0 |
| Rounding | Round each stored component to its column precision (`NUMERIC(12,2)` money, `NUMERIC(12,4)` kWh) using one shared helper in `core/`, half-up (proposed) | Round only `final_bill` |
| Numeric type | `Decimal` in Python, `DecimalType` in Spark, matching the Postgres columns — exact at boundaries (see evidence above) | float64 / `DoubleType` everywhere — simpler, but a cent off at block boundaries; must be stated in Limitations |

### Decision

**Option A on all four sub-questions**, with one addition the options table did not list
(line-item rounding). In order of how much they matter:

#### 1. Numeric type: `Decimal` everywhere — decided by determinism, not just by the half-cent

| Where | Type |
|---|---|
| Python (`core/`, contracts, reconciliation, API) | `decimal.Decimal` |
| Spark (`spark_expr.py`, sources, batch) | `DecimalType` — kWh `(12,4)`, money `(12,2)`, percentages `(5,2)` |
| Postgres | `NUMERIC(12,4)` / `NUMERIC(12,2)` as §6.4 already specifies |
| Parquet (master dataset) | `DECIMAL(12,4)` logical type for kWh |
| Kafka JSON | JSON **number** with at most 4 decimal places; the producer rounds before serialising |

Two independent reasons, either sufficient:

- **Exactness at boundaries** — the T002 evidence above: `60.01 kWh → 480.16` on float64 versus
  the correct `480.17`. Both float paths agree with each other, so a consistency test on
  `DoubleType` would pass while the bills are wrong.
- **Determinism of the batch layer.** Float64 addition is order-dependent, and Spark's sum
  order across partitions is not deterministic. Two runs of `daily_billing.py` over the *same*
  partition can therefore differ in the last ulp of a household's daily kWh — almost always
  invisible after quantisation, but not guaranteed. That silently falsifies §5.4's claim that
  re-running yesterday's job "produces byte-identical output", which is the determinism the
  entire audit story rests on. Decimal sums are exact and order-independent. T172 now tests this
  directly (see Consequence).

The known cost is discipline in `spark_expr.py`: **Spark silently promotes `Decimal op Double`
to `Double`**, so one bare Python float literal (`F.lit(60.0)`, `* 0.01`) reverts the whole
expression to float64 without an error. Every literal is built from a `Decimal` and cast to
the target `DecimalType`; the consistency test asserts the output *schema* is `DecimalType`,
not only the values.

#### 2. Rounding: half-up, at each line item, with the total derived from rounded parts

- Rounding mode is **half-up (away from zero)** — Python `ROUND_HALF_UP`, Spark `round()` (not
  `bround()`, which is half-even), Java `RoundingMode.HALF_UP`. All three agree; all rounded
  quantities are non-negative so "away from zero" and "towards +∞" coincide.
- **Money is rounded per line item, and every total is the exact sum of its rounded parts.**
  Verified: two half-cent lines round to `0.17 + 0.17 = 0.34` under this rule but `0.33` if the
  sum were rounded instead. The rule chosen is the invoice rule — a customer adding up the
  breakdown gets the printed total — and it makes the row-level invariants in T091 and T115
  exact equalities rather than "within a cent".
- **kWh is never rounded.** Readings arrive with ≤ 4 dp; sums, `min`, and subtraction keep
  ≤ 4 dp exactly. Stored quantised to 4 dp for uniform display only.

#### 3. Subsidy base: the energy charge only

`subsidy_discount = round(energy_charge × subsidy_pct × 0.01)` when `subsidy_flag`, else
`0.00`. Not applied to the fixed charge, not applied to the export credit. Because
`0 ≤ subsidy_pct ≤ 100` (D2 validator), the effective energy rate `rate × (1 − pct/100)` is
never negative, which is what keeps the bill monotone in consumption — that validator is
load-bearing, not cosmetic.

#### 4. Negative bills: allowed, never clamped

A net exporter's `final_bill` may be negative and is stored as such. Verified example: 4 kWh
consumed, 19 kWh solar, TIER_1 → `0.00 + 120.00 − 0.00 − 270.00 = −150.00`. Reasons:

- Clamping destroys information the utility owes the customer.
- Genuine carry-forward (apply day *D*'s credit to day *D+1*) is **stateful across days**, which
  would break the D2 lineage sentence — a bill would no longer be a function of one day's
  partition and one tariff file. Carry-forward belongs to a billing-*period* roll-up that
  aggregates daily rows; that roll-up is out of scope and is stated in §10.2.
- `pct_divergence` in `reconciliation_daily` therefore cannot use `final_bill` as its base
  (near-zero or negative denominators explode and would fire `LambdaDivergenceHigh`
  spuriously). Its base is the **gross charges**:
  `pct_divergence = 100 × abs_divergence / (batch.energy_charge + batch.fixed_charge)`, and
  `0` when that base is `0` (reachable only with a zero fixed charge and zero consumption).

#### The formula, as pinned

```
# inputs: consumption_kwh, solar_kwh  (Decimal, ≤ 4 dp);  tariff = the day's TariffRecord (D2)
self_consumed_kwh   = min(solar_kwh, consumption_kwh)
billable_import_kwh = consumption_kwh − self_consumed_kwh
export_kwh          = solar_kwh − self_consumed_kwh

blocks = build_blocks(config.tariff.blocks, tariff)        # [(name, lower, upper|None, rate)]
for each block:  kwh_i    = clamp(billable_import_kwh − lower_i, 0, upper_i − lower_i)   # top block unbounded
                 charge_i = round_money(kwh_i × rate_i)                                 # line item
energy_charge    = Σ charge_i                                                            # sum of ROUNDED lines
fixed_charge     = tariff.fixed_charge
subsidy_discount = round_money(energy_charge × tariff.subsidy_pct × 0.01)  if tariff.subsidy_flag  else 0.00
export_credit    = round_money(export_kwh × tariff.export_rate)
final_bill       = energy_charge + fixed_charge − subsidy_discount − export_credit      # exact; may be < 0
```

Worked examples (verified by execution, 2026-09-19), at the D2 default rates:

| Case | Inputs | Lines | Result |
|---|---|---|---|
| Boundary tie (T002) | 60.0100 kWh, no solar, no subsidy, TIER_2 | 60 × 8.00 = 480.00; 0.0100 × 16.50 = **0.17** | energy 480.17, final **720.17** |
| Typical subsidised solar | 61.2345 kWh, 25.5000 solar, subsidy 25 %, TIER_2 | billable 35.7345 × 8.00 = 285.88 | subsidy 71.47, export 0.00, final **454.41** |
| Net exporter | 4.0000 kWh, 19.0000 solar, TIER_1 | export 15 × 18.00 = 270.00 | final **−150.00** |

These three rows are the first three fixtures in `tests/fixtures/` (T173) and the first three
hand-computed expectations in T172.

### Consequence

- **T055 — new file `core/money.py`** (one-file addition to §7.2's `core/` listing; T182 notes
  it). Holds the precision/scale constants as plain ints (`MONEY = (12, 2)`, `KWH = (12, 4)`,
  `PCT = (5, 2)`), `round_money()`, `quantize_kwh()`. No pyspark import. `spark_expr.py`
  builds its `DecimalType`s from the same ints — one source for precision.
- **T029 `events.py`:** kWh fields are `Decimal` with `decimal_places=4, max_digits=12`;
  serialised to JSON as **numbers** (a `field_serializer`), never strings, so the §6.2 sample
  stays valid and Spark's `from_json` parses the exact decimal text. Producer rounds to 4 dp.
- **T031 `reference.py`:** all money and rate fields `Decimal(12,2)`; `subsidy_pct`
  `Decimal(5,2)`.
- **T057 / T058 `core/tariff.py`:** implements the pinned formula with line-item rounding;
  `BillBreakdown` carries every intermediate as `Decimal`. The docstring doctest is the
  boundary-tie example.
- **T059 unit tests:** the three worked examples above as exact-equality assertions; plus the
  `0.34 vs 0.33` line-item case.
- **T060 property tests:** strategies generate `Decimal` inputs with ≤ 4 dp kWh and ≤ 2 dp
  rates; add **monotone non-increasing in solar** alongside monotone non-decreasing in
  consumption; assert `final_bill == energy + fixed − subsidy − export` exactly; assert the
  breakdown's charges sum to `energy_charge` exactly.
- **T062 `spark_expr.py`:** every literal is `F.lit(Decimal(...)).cast(DecimalType(...))`; use
  `F.round(c, 2)` (half-up) never `F.bround`; round each block charge before summing. *Done
  when* gains: `grep -nE "\b[0-9]+\.[0-9]+\b" src/voltstream/core/spark_expr.py` finds no
  float literals, and the output schema for every kWh/money column is `DecimalType`.
- **T063 consistency test:** inputs are `Decimal`; include deliberate **half-cent tie cases**
  (products ending in exactly `…5` at the third decimal — the case that separates float from
  decimal); assert **exact** `==` on every component; assert the Spark output schema is
  `DecimalType` for all money/kWh columns; compare `tier_breakdown` **numerically per field**
  after parsing both JSON strings (Spark's `to_json` writes `480.00`, Python writes `480.0` —
  textual comparison would spuriously fail).
- **T078 `sources.py`:** the declared `from_json` schema uses `DecimalType(12,4)` for kWh;
  `voltage` stays `DoubleType` (unused by billing). **T080:** Parquet inherits the decimal
  types — check with `spark.read.parquet(...).schema`.
- **T113 `daily_billing.py`:** read the tariff CSV with an **explicit schema** (`DecimalType`
  for money, `(5,2)` for `subsidy_pct`), never `inferSchema`, which yields `DoubleType`.
- **T086 sinks:** psycopg maps `Decimal ↔ NUMERIC` natively — never pass through `float`.
  Write-then-read equality is an assertion, not an assumption.
- **T101 / T106:** repositories return `Decimal`; the API serialises money and kWh as JSON
  **strings** with fixed scale (`"480.17"`, `"61.2345"`) — Pydantic's default for `Decimal`,
  and the correct convention for money over JSON. The dashboard parses. Documented in OpenAPI.
- **T137 / T141 reconciliation:** `pct_divergence` base is gross charges as defined above;
  tests cover base `= 0`, negative `final_bill`, and the identity
  `tariff_effect + data_effect == speed − batch`.
- **T138:** `voltstream_lambda_divergence` = mean `pct_divergence` for the day (matches the
  `_pct` alert threshold); D4's "mean absolute total" wording is corrected to this.
- **T172 integration test — the determinism claim, tested:** run `daily_billing.py` **twice**
  on identical fixtures and assert `household_bill_daily` rows are identical excluding
  `pipeline_run_id` and `computed_at`. This is §5.4's "byte-identical output" as a test.
- **T182 Master Design:** §3.3c gains the tariff formula and the line-item rounding rule;
  §7.2 gains `core/money.py`; §10.2 gains "no billing-period roll-up; negative daily bills are
  stored as credits, not carried forward"; §6.4's `pct_divergence` comment states its base.

---

## D6 — Airflow → Spark submission, and the tariff-arrival sensor

**Status:** ✅ decided 2026-09-19 · **Resolved in:** T007

### Context

Two coupled sub-decisions.

**Submission.** §5.6 requires Airflow to be **thin** — orchestration dependencies only, no
PySpark, no FastAPI. `SparkSubmitOperator` requires the `spark-submit` binary inside the
Airflow container, which contradicts that.

**Sensor.** §8.3 says the DAG "runs a `FileSensor` in poke mode, checking every 30 seconds for
the tariff file." But `reference_dropper.py` writes the file to **MinIO** (`voltstream-landing`,
§6.3), not to a local filesystem. `FileSensor` polls a local path and will never see it.

### Options

*Submission*

| # | Option | For | Against |
|---|---|---|---|
| 1 | `DockerOperator` running the `voltstream-spark` image; Docker socket mounted into Airflow | Airflow stays thin; boundary honest; each job is an isolated container with a clean exit code | Socket mount is a privilege escalation (acceptable in a local demo, state it); `apache-airflow-providers-docker` dependency |
| 2 | `BashOperator` + `docker exec` into a long-lived Spark container | Simplest to write | A Spark container must be kept idle just to receive execs; brittle |
| 3 | Small HTTP job-runner inside the Spark image that Airflow calls | Cleanest separation | Most code to write and test; out of scope for two weeks |

*Sensor*

| # | Option | For | Against |
|---|---|---|---|
| 1 | `S3KeySensor` (`apache-airflow-providers-amazon`) with an `aws_conn_id` pointed at the MinIO endpoint | Purpose-built; poke mode and timeout for free | Amazon provider is a large dependency; needs connection provisioning at bootstrap |
| 2 | `PythonSensor` wrapping a boto3 `head_object` | Tiny dependency footprint | Hand-rolled; must implement timeout/retry semantics yourself |

### Decision

**Submission: Option 1 (`DockerOperator`), through a socket proxy rather than the raw socket.
Sensor: Option 1 (`S3KeySensor`). Plus a third sub-decision the options table did not list,
which reshapes the DAG: how a DAG run learns which simulated day it is processing.**

#### 3. The clock mismatch, and the DAG shape it forces

Airflow schedules in **real** time. This pipeline's days are **simulated**, one per five real
minutes (§3.4). Consequently Airflow's data intervals, logical dates and `{{ ds }}` can never
be simulated dates, and the command §5.6 quotes —
`airflow dags backfill --start-date 2026-08-01 --end-date 2026-08-10 daily_billing` — cannot
address a simulated day without a translation layer. Two designs were weighed:

| | A — real 5-minute schedule + `simclock` translation | B — event-triggered run per simulated day |
|---|---|---|
| Shape | `daily_billing` on a `*/5` schedule; a first task maps the real interval to a `sim_date` via `voltstream.simclock` | A small **`tariff_watcher`** DAG lists the landing bucket every real minute and triggers `daily_billing` once per new file, with `conf={"sim_date"}` parsed **from the filename** and `run_id = billing__<sim_date>` |
| Restatement | Literal `airflow dags backfill` over the translated real interval (`--reset-dagruns`) | Trigger a new run for the same `sim_date` with `run_id = billing__<sim_date>__r<n>` |
| Needs `simclock` in Airflow | **Yes** — and the schedule must stay aligned to `SIM_EPOCH`, or a custom Timetable plugin | **No** — the date is in the filename |
| UI shows | Real timestamps; the sim date is buried in logs | `billing__2026-08-10`, `billing__2026-08-10__r2` |
| Airflow-version sensitivity | High — `backfill` CLI and semantics changed materially between 2.x and 3.x | None — `trigger` and `TriggerDagRunOperator` are stable across both |
| Audit trail of a restatement | The original run is reset in place | Each restatement is its own run with its own `pipeline_run_id`; both are retained |

**Design B is chosen.** It removes `simclock` (and therefore the `voltstream` package) from
the Airflow image, keeps §8.4's "none of them knows the others exist" intact (the watcher
observes the landing bucket; the dropper does not call Airflow), makes simulated dates visible
in the UI, and — decisively — turns each restatement into a preserved run rather than an
overwritten one. The `backfill` **verb** is given up; the **mechanism** (re-execute the same
deterministic code over the same immutable inputs for a past day) is exactly what §5.6
describes, and the report says so in those words.

#### 1. Submission: `DockerOperator` via a socket proxy

`DockerOperator` from `apache-airflow-providers-docker`, launching sibling containers on the
Compose network. The Docker API is reached **through `tecnativa/docker-socket-proxy`**
(`docker_url="tcp://docker-socket-proxy:2375"`), not by mounting `/var/run/docker.sock` into
Airflow. Two reasons: it sidesteps the socket-permission failure that a non-root Airflow user
hits on Docker Desktop (a classic multi-hour sink on this very host), and it lets the report
say "Airflow was given an allow-listed subset of the Docker API, not the socket" — a security
posture rather than an apology. Fallback if the proxy misbehaves: raw socket mount plus
`group_add`.

Non-negotiable operator settings, each one a known failure mode:

| Setting | Value | Why |
|---|---|---|
| `mount_tmp_dir` | `False` | Default `True` bind-mounts the Airflow container's tmp dir into the child — fails through a proxied daemon |
| `network_mode` | the Compose network's real name (e.g. `voltstream-net`) | Otherwise the child cannot resolve `kafka`, `postgres`, `minio` |
| `image` | fixed tags `voltstream-spark:local`, `voltstream-app:local` | `force_pull=False`; `make up` builds before Airflow starts |
| `private_environment` | credentials | Not rendered into task logs; `environment` is |
| `auto_remove` | `"force"` | Airflow already captured stdout; leaked containers accumulate otherwise |
| `command` | the full entrypoint (`spark-submit … daily_billing.py --sim-date {{ params.sim_date }}`) | Image `ENTRYPOINT` stays neutral |

The DAG launches **three kinds of container** (D4): Spark for billing and the zone roll-up,
the app image for reconciliation and the report. Result **verification stays in Airflow** via
`SQLValueCheckOperator` / `SQLCheckOperator` (`apache-airflow-providers-postgres`) — §5.6's
"they submit Spark jobs and verify results", literally.

**Rejected:** `BashOperator` + `docker exec` needs an idle Spark container kept alive to be
exec'd into; an HTTP job-runner is the most code for no marks; `SparkSubmitOperator` puts
Spark in the Airflow image; Spark Connect needs the `pyspark` client in Airflow and moves job
logic into the DAG.

#### 2. Sensor: `S3KeySensor` against MinIO

`S3KeySensor` from `apache-airflow-providers-amazon`, `aws_conn_id` pointing at MinIO via an
`AIRFLOW_CONN_…` environment variable (no UI clicking — §2.5), `check_fn` asserting size > 0,
poke mode, 15 s interval, `timeout = alerts.batch_sla_minutes × 60`. In Design B the file
normally exists the moment the run starts, so the sensor's job is (a) the honest wait when the
watcher raced the dropper's temp-then-copy write (T074), and (b) making a restatement over a
deleted or renamed file fail *visibly* at the first task rather than inside Spark.

The **D3 late-data grace** is a `@task` after the sensor that reads the object's
`LastModified` and sleeps until `LastModified + batch.late_data_grace_real_seconds`. For a
restatement of an old file this is a no-op — a nice property `TimeDeltaSensor` would not have.

#### Airflow version and process shape

**Airflow 3.x**, latest stable at the time T119 is written. Design B neutralises every 2→3
delta that would otherwise have mattered here: the reworked `backfill` (unused), task-level
`sla=` (removed in 3.0 — unused; `BatchSLAMiss` is Prometheus-based per T147), and the
separate providers (`docker`, `amazon`, `postgres`, all installed with the matching
constraints file). 2.x is past end-of-life at the time of writing. **Tripwire:** if Airflow 3
+ `DockerOperator` + proxy is not green within one working session on this host, drop to
**2.11** (final 2.x) — nothing in the DAGs is version-specific, so it is a base-image change.

**One container running `airflow standalone`** (API server, scheduler, DAG processor and
triggerer in-process), `LocalExecutor`, metadata in a **separate database** on the existing
Postgres instance (created by a first-init script; never the application database). This
matches §8.2's single `airflow` container instead of the four-to-five that a split Airflow 3
deployment needs — a real saving on a laptop demo (T176). Stated in the report as a
demo-appropriate topology, not a production one.

### Consequence

- **T119 `airflow.Dockerfile`:** `apache/airflow:<3.x>-python3.11` plus **only**
  `apache-airflow-providers-docker`, `-amazon`, `-postgres` (which brings `common-sql`),
  installed with the version's constraints file. **No `voltstream`, no `pyspark`, no
  `fastapi`.** The DAGs read the three orchestration tunables they need (landing bucket,
  `late_data_grace_real_seconds`, `batch_sla_minutes`) from `config/base.yaml` mounted `:ro`
  with `yaml.safe_load` — the one sanctioned direct YAML read outside `config.py`, and it
  is outside the package.
- **T120 Compose:** add `docker-socket-proxy` (tier 4, alongside Airflow) with `CONTAINERS=1`,
  `POST=1`, `IMAGES=1`, `NETWORKS=1` and everything else off; `airflow` as one `standalone`
  container with `LocalExecutor`, `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` → the separate
  database, `AIRFLOW_CONN_MINIO_S3` and `AIRFLOW_CONN_VOLTSTREAM_PG` as env vars, `dags/`
  mounted, `config/base.yaml` mounted `:ro`. A `00_airflow_db.sql` in Postgres's
  `docker-entrypoint-initdb.d` creates the role and database on first init. **§8.2's "eleven
  containers" becomes thirteen** (proxy, and the Pushgateway if T116 chooses it) — T182.
- **T121 `daily_billing_dag.py`:** `schedule=None`, `params={"sim_date": Param(format="date")}`,
  `max_active_runs=1` (two runs of the same `sim_date` would race on T040's supersede),
  `catchup=False`. Task chain:
  `wait_for_tariff (S3KeySensor) → wait_late_data_grace (@task) → run_daily_billing (Docker, spark) → verify_bills (SQLValueCheck: 50 rows, no nulls) → run_zone_rollup (Docker, spark) → dq_gate (SQLCheck: Σzone == Σhousehold, T125) → run_reconciliation (Docker, app) → generate_report (Docker, app)`.
  Every compute task `retries=2` with exponential backoff; T115's transaction makes a retry
  safe.
- **New DAG file `airflow/dags/tariff_watcher_dag.py`** (one-file addition to §7.2 — T182):
  schedule every real minute, `catchup=False`, `max_active_runs=1`. One `@task` lists
  `voltstream-landing/tariff/`, parses `tariff_(\d{4}-\d{2}-\d{2})\.csv`, and feeds
  `TriggerDagRunOperator.partial(trigger_dag_id="daily_billing",
  skip_when_already_exists=True).expand_kwargs(...)` with `trigger_run_id=f"billing__{d}"` and
  `conf={"sim_date": d}`. Idempotent by construction — the watcher may offer every date every
  minute; Airflow skips the ones that exist. Also seeds `billing__<day −1>` from T075's
  day-zero file, which is harmless (a full run over an empty partition should succeed with
  zero rows — a useful edge-case test in itself).
- **T123 (backfill-safe) is redefined:** *Done when* becomes "triggering
  `daily_billing` a second time for an already-finalised `sim_date` with a new `run_id`
  produces updated `household_bill_daily` rows, a `superseded` + `success` pair in
  `pipeline_runs`, and both DAG runs remain visible in the UI."
- **T162 `scripts/backfill.sh` / `make backfill d=<date>`:** computes the next `__r<n>`
  suffix from existing runs (REST API or CLI list), then
  `airflow dags trigger daily_billing --conf '{"sim_date":"<date>"}' --run-id billing__<date>__r<n>`
  (via `docker compose exec airflow …`), then polls the run to completion and prints the
  before/after bill table. Keep the `make backfill` name.
- **T039 / T115 lineage:** add `orchestrator_run_id TEXT` to `pipeline_runs`, populated from
  `AIRFLOW_CTX_DAG_RUN_ID` passed into the container's environment — ties DAG run ↔
  `pipeline_run_id` ↔ bills in one query. Pairs with D2's optional git-SHA column.
- **T122's "SLA per §5.6"** is dropped from the DAG (no such field in Airflow 3);
  `BatchSLAMiss` (T147) is the SLA. The watchdog DAG's role (T159) is to compute "age of the
  oldest tariff file without a `success` row" from the bucket listing and `pipeline_runs`, and
  push it to the gauge T147 needs — no `simclock` required there either.
- **Master Design corrections.** §8.3 is corrected **now** (T007's acceptance criterion):
  `FileSensor` → the watcher + `S3KeySensor` + grace shape. For **T182**: §5.6's quoted
  `airflow dags backfill …` command and the sentence "That command *is* the Lambda
  restatement mechanism" are replaced with the `trigger … --run-id billing__<date>__r<n>`
  form and: *"Restatement is a new DAG run of the same code over the same immutable inputs for
  the same simulated day. Airflow's native `backfill` addresses real-time data intervals; our
  days are simulated, so restatement is keyed by simulated date and every restatement is
  retained as its own run — the audit trail shows the wrong bill, the corrected bill, and which
  run produced each."* Also §7.2 (`tariff_watcher_dag.py`), §8.2 (container count, the
  proxy), §9 Phase 3 table (`FileSensor, retries, SLA` → `S3KeySensor, retries`), and
  Appendix B's `make backfill` comment.

---

## D7 — Package and repository name spelling

**Status:** ✅ decided 2026-09-19 · **Resolved in:** T008

### Context

The working directory is `volstream` (no `t`). The Master Design, the Python package name,
the Compose project name, every bucket prefix and every `import` say **`voltstream`**. §7.1's
whole point is: "Use the same name for the repository, the Python package and the Compose
project. One name everywhere removes an entire class of confusion."

### Options

| # | Option | For | Against |
|---|---|---|---|
| 1 | Rename the local directory and git remote to `voltstream` | One name everywhere, as §7.1 intends | One-time rename; IDE workspace and any clones must be updated |
| 2 | Keep `volstream` for the directory/remote; package and Compose project remain `voltstream`; note the mismatch in the README | No rename | Permanent inconsistency; the exact confusion §7.1 warns against |

In either case the Python package is `voltstream`.

### Decision

**Option 2.** The repository and working directory stay **`volstream`**; everything the code
and the running system are named by is **`voltstream`**: the Python package, the Compose
project, every service, network, volume and image, every bucket prefix, every metric prefix.
The mismatch is confined to the git slug and the folder on disk, and is documented once in the
README. Renaming was judged not worth the disruption to clones, the remote and the IDE workspace.

The one place the directory name could have leaked into behaviour is **Docker Compose's default
project name**, which is derived from the directory containing the compose file — here that
would be `docker`, not even `volstream`. The `name: voltstream` key in `docker-compose.yml`
(T043) therefore carries the whole decision: it is what makes the project, network, volumes and
containers come out as `voltstream-*` regardless of where the repository is checked out or what
it is called.

### Consequence

- **T043 `docker-compose.yml`:** `name: voltstream` is **mandatory**, not cosmetic. *Done when*
  gains: `docker compose ls` shows project `voltstream`; `docker network ls` and `docker volume
  ls` show `voltstream_*` / `voltstream-*` names; nothing is named `docker_*` or `volstream_*`.
- **T015 `pyproject.toml`:** `name = "voltstream"`; nothing derives from the folder.
- **T048 / T049 / T119 images:** tagged explicitly `voltstream-app:local`,
  `voltstream-spark:local`, `voltstream-airflow:local` in the compose `image:` keys, so
  `DockerOperator` (D6) references fixed names.
- **T109 / T183 README:** one line near the top — *"The repository is `volstream`; the Python
  package, Compose project and all runtime names are `voltstream`. Clone with
  `git clone …/volstream && cd volstream`; everything after that says `voltstream`."* Any
  GitHub URLs, badges and the CI workflow reference the real slug `volstream`.
- **T182 Master Design §7.1:** append a sentence noting the repository slug differs and why.
- **Guard for the future:** `grep -rni "volstream" src/ docker/ config/ airflow/ scripts/
  tests/` must return nothing — the misspelling is allowed only in the README's note and in
  URLs. Add it to T016's lint target so it runs with `make lint`.

---

## D8 — Object store image after MinIO's withdrawal

**Status:** ✅ decided 2026-09-26 · **Resolved in:** infrastructure change outside the task list

### Context

§5.4 chose MinIO as the object store, because it serves the S3 API from a container. Between
October 2025 and September 2026, MinIO Inc. withdrew its community distribution:

- **October 2025:** it stopped publishing images.
- **December 2025:** the repository went into maintenance mode.
- **February 2026:** the repository was archived.
- **11 September 2026:** its Docker Hub repositories were deleted.

On 2026-09-26 both images the stack used failed on a clean machine.
`quay.io/minio/minio` and `quay.io/minio/mc` returned *401 Unauthorized*; `minio/minio` and
`minio/mc` on Docker Hub reported that the repository does not exist. A fresh clone could no
longer start the stack, which breaks the graded reproducibility requirement (§2.5, Gate 5).

The architecture is unaffected. Every reader and writer speaks the S3 API against a
configurable endpoint: `s3a://` in Spark, boto3 in Python, and the S3 hook and sensor in
Airflow. §5.4 anticipated this: moving store is "a change of endpoint and credentials —
nothing structural".

Sources: [lobehub#9845](https://github.com/lobehub/lobehub/issues/9845),
[StableBuild](https://www.stablebuild.com/blog/minio-images-disappeared-from-docker-hub),
[VONNG: MinIO Is Dead, Long Live MinIO](https://blog.vonng.com/en/db/minio-resurrect/),
[VONNG: Silo](https://vonng.com/en/db/silo-is-coming/).

### Options

| # | Option | For | Against |
|---|---|---|---|
| 1 | **`pgsty/silo`**, a community fork of MinIO (named `pgsty/minio` until August 2026) | Drop-in: same S3 API, same `MINIO_*` variables, same `/minio/health/live`, `mc` in the same image. No code change. Multi-arch. | One maintainer; already renamed once, over the trademark |
| 2 | SeaweedFS (Apache-2.0) | Established, multi-maintainer, S3 gateway | New healthcheck, bucket creation and credential config; no `mc`, no MinIO console; Spark S3A to be verified |
| 3 | Garage (AGPL-3.0) | Lightweight, maintained | Cluster layout and key setup through its CLI: more bootstrap |
| 4 | Build MinIO from the archived source | The original code | Unmaintained; security fixes missing; adds a build step |
| 5 | Local volume instead of object storage (§9.2, cut 5) | No external dependency | Gives up the object-store semantics the report claims |

### Decision

**Option 1 now, pinned: `pgsty/silo:RELEASE.2026-09-16T00-00-00Z`. Option 2 (SeaweedFS) is
the planned long-term replacement.**

The tag is pinned rather than `latest`, so a single-maintainer project cannot change
underneath a graded demo. The `-distroless` variant is excluded: it has no `curl` for the
healthcheck and no shell for `minio-init`.

Verified on 2026-09-26 before adopting, against this repository's own configuration:

- The image pulls, for both amd64 and arm64.
- It starts with the compose file's `server /data --console-address ":9001"` and `MINIO_*`
  variables.
- The existing healthcheck (`curl -f …/minio/health/live`) returns 200.
- `minio-init`'s unmodified `create_buckets.sh` creates all three buckets, and exits 0 when
  run a second time.
- boto3 1.43, with its default request checksums, completes every call the pipeline makes:
  `head_bucket`, the reference dropper's put, copy and delete, `list_objects_v2`,
  `head_object`, `get_object`, and a multipart upload (the path Spark's S3A takes for larger
  files).

Spark S3A itself has not yet run against it. It is the same server code as MinIO, but T082
and T083 should confirm it on the next full run. **Confirmed on the Gate 4 run
(2026-09-27):** the raw archiver wrote the master dataset, and the batch jobs read it and
archived the tariff Parquet, all through S3A against this image.

### Consequence

- **`docker-compose.yml`:** the image is defined once (`x-object-store-image`) and used by
  both `minio` and `minio-init`; the separate `mc` image is gone. The service keeps the name
  `minio`, because `minio:9000` is the endpoint every client is configured with.
- **`scripts/smoke_test.sh` and `scripts/voltstream.ps1`:** bucket listings run `mc` inside
  the `voltstream-minio` container with `docker exec`, so neither script names an image.
- **T182 / report:** §5.4 and the tech-stack chapter name MinIO. Keep the S3-compatible
  store as the choice, note the fork, and add to the limitations that the store is a
  single-maintainer fork. The episode is also evidence for the viva: the vendor withdrew its
  images, and because the pipeline only speaks S3, the swap was two image lines and no code.
- **The SeaweedFS migration** should need no Python or Spark code changes, because the
  endpoint is configuration. What would change, to be verified at the time:
  - The image, and an S3 gateway command.
  - Credentials, set through SeaweedFS's S3 identity configuration instead of
    `MINIO_ROOT_*`.
  - The healthcheck.
  - Bucket creation through the S3 API (boto3 from the app image, or the AWS CLI) instead
    of `mc`.
  - The scripts' listings, for the same reason.
  - The MinIO console link, which would go.

  Also re-check Spark S3A (multipart upload, rename as copy plus delete, `ListObjectsV2`)
  and boto3's default checksums. If the store rejects those checksums, set
  `AWS_REQUEST_CHECKSUM_CALCULATION=when_required`.

---

## D9 — Where two alerts get facts no application metric carries

**Status:** ✅ decided 2026-09-27 · **Resolved in:** T147 (also serves T144)

### Context

Two of the five alert rules need a fact that none of the eight metrics in `metrics.py`
carries:

- **MeterDataStale (T144)** needs to know when each zone last received a reading. The
  obvious candidate, `voltstream_zone_renewable_ratio{grid_zone}`, cannot answer it: the
  ratio is a constant 0 all night, so an unchanged value is indistinguishable from a dead
  feed. Prometheus cannot see that a gauge was *set*, only its value.
- **BatchSLAMiss (T147)** needs to know when billing last succeeded. T147 names the gap
  itself ("a 'last successful run' gauge, which does not exist yet") and asks for either
  the Pushgateway or a Postgres exporter.

Both facts are already in Postgres, written by the pipeline: `zone_metrics_rt.updated_at`
is set to `now()` on every speed-layer write, and `pipeline_runs.finished_at` records every
billing run.

### Options

| # | Option | For | Against |
|---|---|---|---|
| 1 | **`sql_exporter`**: gauges from our own SQL | Reads facts the pipeline already writes; nothing new to keep in sync; metric names and types chosen freely; maintained for exactly this purpose | One more container; if it or Postgres is down, both alerts go quiet |
| 2 | `postgres_exporter` with custom queries | The tool T147 names | Its custom-query flag (`--extend.query-path`) is marked **deprecated** in the current release (v0.20.1) |
| 3 | BatchSLAMiss from the Pushgateway's `push_time_seconds{job="daily_billing"}` | No new service for that alert | Lost on a Pushgateway restart; absent until the first run, so a DAG that never runs never alerts; and it does nothing for MeterDataStale |
| 4 | New or relabelled application metrics (e.g. `grid_zone` on `events_consumed_total`) | No exporter | Changes the eight-metric contract (§10.1, T027); a dead speed layer removes the series instead of firing |

### Decision

**Option 1: `burningalchemist/sql_exporter:0.24.8`, pinned, with two gauges and nothing else.**

- `voltstream_pg_zone_data_age_seconds{grid_zone}`: `now() − updated_at` of the zone's
  newest window. It is the newest window rather than the latest write anywhere, because a
  backfill after an outage rewrites old windows while the zone is still silent. It also
  grows when the speed layer is down, which is correct: no reading is reaching the live view.
- `voltstream_pg_billing_last_success_timestamp_seconds`: `max(finished_at)` of successful
  billing runs; the stack's creation time before the first, so a DAG that never runs still
  ages past the SLA.

The `voltstream_pg_` prefix keeps them apart from the application metrics; a unit test
fails if the exporter ever defines a name `metrics.py` owns.

### Consequence

- **A read-only database role**, `voltstream_reader` (`docker/init/postgres/04_observability.sql`):
  `SELECT` only, read-only transactions and a 5-second statement timeout. Grafana uses it
  too, since anonymous viewers can open its dashboards.
- **BatchSLAMiss measures the age of the last success, not each day's deadline.** It fires
  when the age exceeds one simulated day plus the SLA (900 s), about a minute after the exact
  deadline (04-observability.md §6).
- **One more silent-failure mode**, stated in 04-observability.md §9: if the exporter stops,
  both alerts lose their input. Its scrape target shows as down on the pipeline health
  dashboard.
- **T182 / report:** §10.1's metric table is unchanged. The two gauges are a separate table
  in 04-observability.md §3.3.

---

## How to record a decision

1. Fill **Decision** with one or two sentences naming the option chosen and the values pinned
   (e.g. "Option 1. `watermark_sim_minutes: 5`, `out_of_order_spread_sim_minutes: [1, 20]`").
2. Fill **Consequence** with what changes as a result: config keys added or renamed, sections of
   the Master Design to correct, tests that must exist.
3. Update the **Status** table: `✅ decided`, and the date.
4. If a decision is later reversed, do not edit it in place — mark it `🔁 superseded`, add a new
   entry `Dn-bis` below it, and link the two.
