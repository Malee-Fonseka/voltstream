# voltstream — Data contracts

> **FROZEN as of 2026-09-22.** Changed only by explicit agreement — the producer, the
> speed layer and the batch layer all build against these shapes independently, and
> `tests/unit/test_contracts.py` (T033) fails the build the moment the committed JSON
> Schema under [`src/voltstream/contracts/schemas/`](../../src/voltstream/contracts/schemas/)
> stops matching the Pydantic models in [`contracts/events.py`](../../src/voltstream/contracts/events.py)
> and [`contracts/reference.py`](../../src/voltstream/contracts/reference.py). A contract
> change is therefore a deliberate act — edit the model, run
> `scripts/export_schemas.py`, commit the regenerated schema and this document together.

Extracted from Master Design §6.2, with the T003/D2 tariff-column correction applied.

---

## Streaming event — `meter.readings`

Published by `simulators/meter_producer.py`, consumed independently by the raw archiver
and the speed layer (§5.2 — separate Kafka consumer groups on one topic is what makes the
Lambda shape possible from a single source).

**Kafka message:** key = `household_id` (UTF-8 string). Value = JSON, UTF-8.
**Kafka headers:** `trace_id` and `produced_at` (RFC 3339, wall clock) — see
[Latency timestamp](#latency-timestamp-decision-t030) below. `includeHeaders=true` must be
set on the Spark Kafka source (`T078`) — it is off by default.

```json
{
  "schema_version": "1.0",
  "event_id": "3fae2b1e-9a4e-4e9a-8b7f-1a2b3c4d5e6f",
  "trace_id": "8c1d2e3f-4a5b-4c6d-9e0f-1a2b3c4d5e6f",
  "meter_id": "MTR-0042",
  "household_id": "HH-0042",
  "grid_zone": "ZONE-C",
  "event_ts": "2026-08-10T14:23:00Z",
  "consumption_kwh": 0.412,
  "solar_generation_kwh": 0.180,
  "voltage": 232.4,
  "producer_id": "sim-01"
}
```

| Field | Type | Purpose |
|---|---|---|
| `schema_version` | `str` | Enables forward-compatible evolution |
| `event_id` | `UUID` | Idempotency; secondary dedup safety net |
| `trace_id` | `UUID` | Propagated end-to-end — the pragmatic tracing story (§10.1) |
| `meter_id` | `str` | Physical meter identifier |
| `household_id` | `str` | Kafka partition/message key; joins to `households` |
| `grid_zone` | `str` | One of the five configured zones |
| `event_ts` | `datetime`, tz-aware | **Simulated** time — drives all windowing. **MUST NOT** be used for latency (see below) |
| `consumption_kwh` | `Decimal(12,4)` | D5 precision; billing input |
| `solar_generation_kwh` | `Decimal(12,4)` | D5 precision; billing input |
| `voltage` | `float` | Deliberately unused by billing — demonstrates Parquet column pruning (`T111`) |
| `producer_id` | `str` | Which simulator instance emitted this reading |

Model: `voltstream.contracts.events.MeterReading`. `extra="forbid"` — an unknown field is
a validation error, not a silently accepted one. kWh fields serialise to JSON as
**numbers**, not strings (`ser_json_decimal`-equivalent behaviour via a `field_serializer`
on `to_kafka_value()`), so the sample above stays byte-for-byte representative of what is
actually on the wire, and Spark's `from_json` parses the exact decimal text against an
explicit `DecimalType(12, 4)` schema.

### Latency timestamp (decision T030)

`voltstream_e2e_latency_seconds` needs a wall-clock origin, but `event_ts` is simulated —
measuring latency from it would produce numbers scaled by `TIME_SCALE` (288×) and be
meaningless. The frozen contract gains **no new field** for this. Instead:

- The **Kafka record timestamp** (set by the producer at send time, exposed by Spark's
  Kafka source as the `timestamp` column) is the wall-clock origin for latency
  measurement.
- A `produced_at` Kafka **header** (RFC 3339, wall clock) is set alongside `trace_id`, so
  the raw archiver can persist it into Parquet for later inspection without it living on
  the JSON contract itself.

---

## Daily reference — `tariff_YYYY-MM-DD.csv`

Dropped once per simulated day by `simulators/reference_dropper.py` into
`voltstream-landing/tariff/`, consumed by the speed layer (yesterday's file) and the batch
layer (today's file). **This is the final ten-column contract from D2** — it replaces the
seven-column draft in the original design document (`tariff_rate` removed; `subsidy_pct`,
`block_1_rate`, `block_2_rate`, `block_3_rate` added).

```csv
household_id,effective_date,billing_tier,subsidy_flag,subsidy_pct,fixed_charge,block_1_rate,block_2_rate,block_3_rate,export_rate
HH-0042,2026-08-10,TIER_2,false,25.0,240.00,8.00,16.50,24.50,18.00
```

| # | Column | Type / domain | In arithmetic? |
|---|---|---|---|
| 1 | `household_id` | `str`, `HH-nnnn` | join key |
| 2 | `effective_date` | ISO date | join filter — row applies for `sim_date >= effective_date` |
| 3 | `billing_tier` | `TIER_1 \| TIER_2 \| TIER_3` | no — informational only |
| 4 | `subsidy_flag` | `bool` | yes — gate |
| 5 | `subsidy_pct` | `Decimal(5,2)`, `0-100` | yes |
| 6 | `fixed_charge` | `Decimal(12,2)`, `>= 0` | yes |
| 7 | `block_1_rate` | `Decimal(12,2)`, `>= 0` | yes |
| 8 | `block_2_rate` | `Decimal(12,2)`, `>= 0` | yes |
| 9 | `block_3_rate` | `Decimal(12,2)`, `>= 0` | yes |
| 10 | `export_rate` | `Decimal(12,2)`, `>= 0` | yes |

Model: `voltstream.contracts.reference.TariffRecord`. `extra="forbid"`. Rates are **not**
required to be monotonically increasing across blocks — see D2/D5 for why constraining
that would only make the restatement demo less flexible.

**The lineage sentence:** a finalised bill is a pure function of the raw Parquet
partition for `sim_date`, the archived tariff file for `sim_date`, and the block
boundaries in git. Nothing that can change day to day lives outside this file.

---

## Daily reference — `weather_YYYY-MM-DD.csv`

Dropped alongside the tariff file, unchanged from the original §6.2 draft.

```csv
grid_zone,forecast_date,cloud_cover_pct,temperature_c,solar_irradiance_index
ZONE-C,2026-08-10,35,31.2,0.78
```

| Column | Type / domain |
|---|---|
| `grid_zone` | `str`, one of the five configured zones |
| `forecast_date` | ISO date |
| `cloud_cover_pct` | `Decimal(5,2)`, `0-100` |
| `temperature_c` | `Decimal(5,2)`, `-90` to `60` |
| `solar_irradiance_index` | `Decimal(4,3)`, `0-1` |

Model: `voltstream.contracts.reference.WeatherForecast`. `extra="forbid"`.

---

## Regenerating the schemas

```bash
python scripts/export_schemas.py
```

Writes `src/voltstream/contracts/schemas/{meter_reading,tariff_record,weather_forecast}.json`.
Re-running with no model changes produces no diff — that is what `test_contracts.py`
checks on every test run.
