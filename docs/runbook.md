# voltstream runbook — demo it, break it, fix it

**For:** whoever runs the live demo or the viva walkthrough, including someone who did not
build the system. Everything below was run on the real stack. The timings are what we
measured on a Windows laptop with Docker Desktop; a faster machine is quicker, never slower.

Commands are shown for PowerShell (`.\scripts\voltstream.ps1 …`). The same steps exist as
`make` targets and as bash scripts under `scripts/`, if you prefer those.

---

## 0. Before you start

| You need | Check |
|---|---|
| Docker Desktop, running, 8 GB of memory allotted | `docker info` answers |
| Git for Windows (for Git Bash; the fault, backfill and demo scripts are bash) | `C:\Program Files\Git\bin\bash.exe` exists |
| Free ports 3000, 5432, 8000, 8011, 8012, 8080, 9000, 9001, 9090, 9091, 9093, 29092 | nothing else listening on them |
| PowerShell allowed to run the script | if refused: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |

**Time.** One simulated day is 5 real minutes (288× compression). Everything below is in
real time unless it says *simulated*.

---

## 1. Start from nothing

```powershell
.\scripts\voltstream.ps1 run
```

This wipes any old data, builds the three images and starts all 18 containers, waiting on
their health checks. The first build takes several minutes; later runs take about a minute.
It ends by printing where to look:

| What | Where |
|---|---|
| Dashboard (provisional/final bill) | http://localhost:8000/ |
| API docs (OpenAPI) | http://localhost:8000/docs |
| Grafana (three dashboards, no login) | http://localhost:3000/ |
| Airflow (`admin` / `admin`) | http://localhost:8080/ |
| Prometheus | http://localhost:9090/ |
| Alertmanager | http://localhost:9093/ |
| MinIO console (`voltstream` / `voltstream-dev`) | http://localhost:9001/ |

**What healthy looks like, and when:**

| After | Where | Expect |
|---|---|---|
| ~2 min | Grafana → pipeline health | Scrape targets down 0. Readings produced and consumed overlap (~24/s, both layers). Consumer lag ~0. Rejects ~2 %, under the dashed 5 % line. Latency under the dashed 1-minute line |
| ~2 min | Grafana → grid operations | Load, solar and renewable ratio by zone moving; the ratio crosses the dashed 15 % line |
| every simulated night | Grafana → "Alerts firing now" | LowRenewableContribution for all 5 zones, gone by mid-morning. **Expected, not a fault** |
| ~6–7 min | Airflow → daily_billing | First run: all 8 tasks green. No run for 2025-12-31, the seed day |
| ~10–13 min | Grafana → Lambda divergence | The first complete day reconciled: divergence around 1.5–2 % |

Then `.\scripts\voltstream.ps1 check` should report **0 failed**. Run it outside a billing
run: while Spark's batch containers run, the laptop can be slow enough that a health call
times out and shows a false FAIL. Re-run it a minute later.

---

## 2. The demo (T163)

```powershell
.\scripts\voltstream.ps1 demo
```

No input needed. It starts the stack if it is not running, keeping the clock of a stack that
already has data, then:

1. Shows HH-0001's bill for the day open now. **Served by: speed layer (provisional:
   true)**, costed on yesterday's tariff.
2. Waits for that day to close and be billed: up to one simulated day, then about two
   minutes of batch time.
3. Shows the same request again. **Served by: batch layer (provisional: false)**, on the
   day's own tariff.
4. Shows the delta: speed minus batch, as a percentage of the gross charges, split into the
   **tariff effect** (yesterday's rates vs today's) and the **data effect** (readings the
   speed layer dropped). The two add up to the delta exactly.
5. Opens the dashboard. Its badge makes the same flip, Provisional → Final.

Measured 2026-09-28 on a warm stack: 6¼ minutes. HH-0001 on 2026-01-02 went from speed
(provisional, 01-01 tariff) to batch (final, 237.38 on the 01-02 tariff); delta −6.90, all
of it tariff effect, 2.907 %.

*Say while it waits:* the same URL answers from whichever layer can. While the day is open,
the only answer is the speed layer's estimate, labelled as such. Once the batch layer has
billed the closed day from the master dataset, its answer replaces the estimate. That is the
Lambda merge function as a product feature.

---

## 3. Break it on purpose (T157–T161)

```powershell
.\scripts\voltstream.ps1 faults                       # all five, about 30 minutes
.\scripts\voltstream.ps1 faults -Scenario stale       # or one at a time
.\scripts\voltstream.ps1 faults -Capture              # hold each alert and screenshot it
```

Each scenario injects one fault, **asserts** the alert through Alertmanager's API, and undoes
the fault. The undo is registered before the fault and runs on any exit, Ctrl+C included, so
an interrupted run never leaves billing paused or the producer broken.

Watch it in Grafana's pipeline health dashboard and in Alertmanager. Notifications land in
the API's log:

```powershell
docker logs -f voltstream-api 2>&1 | Select-String '"stage": "alert"'
```

| Scenario | The fault | The alert | Measured |
|---|---|---|---|
| `stale` (T157) | Stop the meter producer | **MeterDataStale**, critical, all 5 zones in one notification; LowRenewableContribution silenced for those zones if it is night | Fires 173–195 s after the stop; resolves 41–46 s after the restart |
| `rejects` (T158) | Restart the producer with 15 % null fields (normally 1 %) | **HighRejectRate**, warning | Fires after about 2 minutes, at a ratio of about 11 % |
| `sla` (T159) | Pause the `daily_billing` DAG | **BatchSLAMiss**, critical | Fires 15 minutes after the last successful billing run; clears once the unpaused DAG catches up |
| `renewable` (T160) | None needed: the simulated night | **LowRenewableContribution**, warning | Clears after dawn (~10:00 simulated), fires after dusk (~19:00–20:00 simulated) |
| `divergence` (T161) | Restart the producer sending 60 % of readings 3–5 simulated hours late | **LambdaDivergenceHigh**, warning | *filled in below after the run* |

Two notes for the viva:

- **T160 deviates from the task list on purpose.** It asked for cloud cover forced to 100 %.
  The producer ignores cloud cover (backlog R16), and even 100 % cloud leaves 20 % of solar.
  The night is a real, repeatable low-renewable condition that needs no change to data
  generation.
- **Grafana and Alertmanager disagree during a producer outage, correctly.** Grafana's
  "Alerts firing now" is Prometheus' view, where LowRenewableContribution is still true.
  Alertmanager silences it because MeterDataStale explains it. Alertmanager's page hides
  silenced alerts unless you tick "Inhibited".

---

## 4. Restate a day (T162)

```powershell
.\scripts\voltstream.ps1 backfill -Date 2026-01-03
```

Pick a day billed at least two simulated days ago. The script corrupts that day's tariff
file (`block_1_rate` + 45.00), restates the day, restores the file and restates again. It
prints the original, wrong and corrected bills side by side, then the run ledger:
`superseded → superseded → success`, each row naming its Airflow run
(`billing__<date>`, `…__r1`, `…__r2`). It takes about 6–8 minutes, because each run waits
out the 90-second late-data grace behind a freshly written tariff file.

Measured 2026-09-28 on 2026-01-02: the corrupted run took 139 s and made all 50 bills wrong
(the day's total went from 18,737.41 to 44,876.57); the corrected run put all 50 back to the
cent. The ledger ended `superseded → superseded → success` from three distinct runs. The
corrected run's first attempt failed on a memory-starved host and Airflow's retry succeeded
(5.7 minutes in all); the script reports such retried attempts rather than counting them.

*Say while it runs:* the batch layer never patches a bill. It recomputes the day from the
immutable master dataset and the corrected reference data, and the earlier result is marked
superseded, not erased from the ledger. (The bills table keeps only the current version;
backlog R25.)

---

## 5. Kill the speed layer (T167)

```powershell
.\scripts\voltstream.ps1 killtest
```

It kills the speed layer mid-day with SIGKILL. While it is down, the live zone view goes
stale (the dashboard gap), and the raw archiver keeps writing the master dataset. The script
then restarts the speed layer and shows it resume from its checkpoint and catch up. Last, it
shows the bills are unaffected: a finalised day's bill is served identically before, during
and after the kill, and the day of the kill is billed in full once it closes. About 7–10
minutes.

Measured 2026-09-28: during a 60-second SIGKILL outage the live view went 56 s stale while
the raw archiver took in 1,526 readings. The speed layer caught up 22 s after restarting,
the finalised bill for the previous day stayed 237.38 throughout, and the day of the kill
was billed in full: 50 bills, HH-0001 with 148 readings against 144 the day before.

*Say:* billing reads the master dataset, not the speed view. The speed layer is a fast,
disposable approximation, and losing it costs freshness, never correctness.

---

## 6. Capture the evidence (T164)

```powershell
.\scripts\voltstream.ps1 capture -Label <moment>
```

This screenshots Grafana's three dashboards, the dashboard, the API docs, Alertmanager and
Prometheus' alerts page into `docs/report/screenshots/`, each named `<moment>-<page>.png`.
`faults -Capture` does it automatically while each alert fires.

| Evidence (§9 Phase 4, T164) | How |
|---|---|
| Each of the five alerts firing | `faults -Capture` |
| Dashboard badge before and after finalisation | `capture -Label before` during step 2 of the demo, `capture -Label after` at the end |
| Grafana divergence panel | any `capture`, `grafana-lambda-divergence.png` |
| Backfill before/after | the `backfill` output (a text table: save it, or screenshot the terminal) |
| Airflow DAG graph | by hand: Airflow → daily_billing → Graph (it needs a login) |
| OpenAPI docs page | any `capture`, `openapi-docs.png` |
| CI green | by hand, once Phase 14 adds CI |

---

## 7. Stop, resume, clean up

| Command | Effect |
|---|---|
| `.\scripts\voltstream.ps1 stop` | Stop the containers, keep all data |
| `.\scripts\voltstream.ps1 start` | Resume on the same clock. Simulated time kept running while stopped, so there is a gap |
| `.\scripts\voltstream.ps1 clean` | Delete containers and all data, monitoring history included |

---

## 8. Troubleshooting — failures we actually hit

| Symptom | Cause | Fix |
|---|---|---|
| `minio` image fails to pull: *401 Unauthorized* or *repository does not exist* | MinIO withdrew its images in 2025–26 (D8) | Already handled: the stack uses `pgsty/silo`. If a pull of it fails, check the network, not the tag |
| `check` shows a FAIL like "no answer" during a billing run | The Spark batch containers starve the laptop for a minute | Re-run `check` a minute later |
| The speed layer restarts in a loop after a code change | Its checkpoint no longer matches the query (state schema or operators changed) | `clean`, then `run`. Checkpoints only survive restarts of the same query |
| Right after the laptop wakes from sleep, BatchSLAMiss fires and simulated time has jumped | The clock is real time × 288, so hours asleep are weeks simulated, with no billing in between | Expected. It clears after the next successful billing run. For a tidy demo, `clean` and `run` |
| After a sleep, or under heavy load, logs show `failed to resolve host 'postgres'` | Docker Desktop's internal DNS stalls for a moment | Connections are retried for about 15 s (R38), so it passes. If the speed layer still restarts repeatedly, restart Docker Desktop, then `start` |
| An image build fails on `files.pythonhosted.org`: *Temporary failure in name resolution* | The same DNS stall, during `pip install` | Restart Docker Desktop and build again. Don't build while fault drills are running |
| The dashboard's bill panel stays empty | An old dashboard page whose date box never filled | Reload the page. Fixed since Phase 12: the box now fills itself |
| Alertmanager's page lacks an alert Grafana shows | The alert is silenced by an inhibition; the page hides those by default | Tick "Inhibited" in Alertmanager |
| LowRenewableContribution fires every few minutes | Every simulated night is low-renewable | Expected (T145) |
| A script fails with *no such file or directory* for `/opt/...` or `/bin/...` | Git Bash rewrote a container path into a Windows path | Use `voltstream.ps1` or the scripts as given: they set `MSYS_NO_PATHCONV` where needed |
| `bash scripts/...` runs but cannot find `docker` | `bash` resolved to WSL's `C:\Windows\System32\bash.exe` | Run it through `voltstream.ps1`, which finds Git Bash, or run it from a Git Bash window |
| `make up` on a stack with data restarts simulated time at 2026-01-01 | `make up` always stamps a new clock anchor | Use `voltstream.ps1 start` (keeps the anchor) or `make demo`, or `clean` first |
| A test run wiped the stack | `pytest -m integration` for the watermark probe deliberately starts from empty volumes | Run it only when you do not need the current data |
