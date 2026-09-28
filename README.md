# voltstream

A Lambda-architecture data platform for smart-grid monitoring and billing, built for
EC8203 Applied Big Data Engineering (Use Case 3).

> The repository is `volstream`; the Python package, Compose project and all runtime names
> are `voltstream`. The missing "t" in the repository slug is a typo we chose to live with
> rather than break existing clone URLs — everything inside the repository is spelled
> correctly, and a lint rule enforces it (decision D7).

Smart meters emit readings continuously; tariff data arrives once a day. Those two feeds
serve two consumers with opposite requirements — grid operators need load and renewable
mix *within seconds*, billing needs an exact, auditable, restatable figure *after the day
closes*. That tension is the reason for the architecture, and it is argued out in
[docs/architecture/00-master-design.md](docs/architecture/00-master-design.md).

## Architecture

```
 meter producer ──┐
                  ├─→ Kafka ──┬─→ raw archiver ──→ Parquet on MinIO   (master dataset)
 reference dropper┘           │                          │
        │                     │                          └─→ batch layer ──┐
        └─→ landing bucket    └─→ speed layer ──→ Postgres (speed view) ──┤
            (daily tariff)                                                 ├─→ merge ──→ API
                                                          Postgres (batch view)
```

Both branches read the same topic with entirely independent offsets. The speed layer
answers in seconds and is deliberately approximate; the batch layer rescans a closed day
from the master dataset and is authoritative. The API serves whichever can answer and
says which one it used.

_(Diagram placeholder — `docs/architecture/diagrams/` holds the rendered version for the
report.)_

## Prerequisites

- Docker Desktop, with roughly 6 GB available to the engine
- Python 3.11 (`.python-version` pins 3.11.9 — PySpark 3.5 does not support 3.12+)
- `make`. On Windows use Git Bash with `choco install make`, or WSL.

## Quickstart

```bash
cp .env.example .env     # safe local defaults; nothing needs editing
make demo
```

`make demo` brings the stack up, waits on health checks rather than sleeping, runs one
simulated day, and prints where to look. First run builds two images and takes a while;
later runs start in under a minute.

Then:

- **API and docs** — <http://localhost:8000/docs>
- **MinIO console** — <http://localhost:9001> (`voltstream` / `voltstream-dev`)
- **Live zone load** — `curl http://localhost:8000/api/v1/zones/load`
- **Grafana** — <http://localhost:3000> (no login): pipeline health, grid operations and
  Lambda divergence dashboards
- **Prometheus** — <http://localhost:9090> (targets, alert rules) and **Alertmanager** —
  <http://localhost:9093>

Other targets: `make up`, `make down`, `make clean` (destroys volumes), `make test`,
`make test-all`, `make lint`, `make check-alerts`, `make logs s=speed-layer`.
Observability is described in [`docs/architecture/04-observability.md`](docs/architecture/04-observability.md).

To break it on purpose and prove the alerts notice: `make faults` (or one scenario:
`make faults s=stale`), `make backfill d=<date>` for the restatement demo, and
`make kill-test` for a speed-layer crash. On Windows without `make`, the same commands are
`.\scripts\voltstream.ps1 faults | backfill -Date <date> | killtest | demo`. The demo, the
fault drills and what to say during them are in [`docs/runbook.md`](docs/runbook.md).

## Simulated time

One simulated day takes **5 real minutes** (`TIME_SCALE = 288`). Every `event_ts`,
`sim_date` and window boundary is simulated time; only latency metrics use the wall clock.

`VOLTSTREAM_ANCHOR_REAL` in `.env` is the real instant simulated time is measured from.
`make up` restamps it on every start, and it matters: the gap between the anchor and now
is multiplied by 288, so an anchor left over from yesterday puts the simulation months
into the future. Bringing the stack up with raw `docker compose` skips that — pass
`--env-file .env` and set the anchor yourself if you do.

## Services

| Service | Port | What it does |
|---|---|---|
| `kafka` | 29092 | Event log; 3 partitions, ~7 day retention |
| `postgres` | 5432 | Serving layer — speed and batch views |
| `minio` | 9000 / 9001 | Master dataset and the daily landing zone. Runs `pgsty/silo`, a MinIO-compatible fork, since MinIO stopped publishing images (decision D8) |
| `meter-producer` | — | 50 meters, one reading each per 2 real seconds, with injected faults |
| `reference-dropper` | — | One tariff and weather file per simulated day |
| `raw-archiver` | 8011 | Kafka → Parquet, no transformation whatsoever |
| `speed-layer` | 8012 | Windowed zone metrics and the provisional bill |
| `api` | 8000 | Serving API and OpenAPI docs |

## Where to look

| Looking for | Start here |
|---|---|
| Lambda vs Kappa, and why | [00-master-design.md](docs/architecture/00-master-design.md) §3 |
| Decisions the design left open | [05-open-decisions.md](docs/architecture/05-open-decisions.md) D1–D7 |
| Billing logic, shared by both layers | [src/voltstream/core/](src/voltstream/core/) |
| That the two layers agree | [tests/consistency/test_pure_vs_spark.py](tests/consistency/test_pure_vs_spark.py) |
| Ingestion | [simulators/](src/voltstream/simulators/), [streaming/sources.py](src/voltstream/streaming/sources.py) |
| The master dataset | [streaming/raw_archiver.py](src/voltstream/streaming/raw_archiver.py) |
| Stream processing | [streaming/speed_layer.py](src/voltstream/streaming/speed_layer.py) |
| Serving layer | [storage/repositories.py](src/voltstream/storage/repositories.py), [api/](src/voltstream/api/) |
| Observability | [metrics.py](src/voltstream/metrics.py), [logging_setup.py](src/voltstream/logging_setup.py) |
| Measured limitations, honestly | [docs/assumptions.md](docs/assumptions.md) |

## What is deliberately simplified

Fifty households, single-broker Kafka, single-node Spark, no schema registry, secrets in
`.env`. The simulated clock compresses event time but not processing time, which has a
measured consequence: about 1 % of a day's energy never reaches the 15-minute operational
view, while the daily totals stay complete.

Every simplification, and what it would cost to remove, is in
[docs/assumptions.md](docs/assumptions.md). Nothing there is an oversight we are hoping
goes unnoticed.
