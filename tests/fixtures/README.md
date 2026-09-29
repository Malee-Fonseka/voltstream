# Test fixtures (T173)

Committed inputs and hand-computed outputs for one simulated day, **2025-06-15**, so the
batch end-to-end test (`tests/integration/test_batch_end_to_end.py`, T172) depends on
neither the simulator's RNG nor the running simulation. The date is half a year before
`simulation.epoch_sim`, so it never collides with a day the simulation writes, while still
inside the batch job's accepted `event_ts` range (epoch − 365 days).

| File | What |
|---|---|
| `readings_2025-06-15.jsonl` | Nine meter readings, one JSON object per line, exactly as the producer sends them (`MeterReading.to_kafka_value()`): two per household, plus one retransmission |
| `tariff_2025-06-15.csv` | The day's tariff, the frozen ten-column contract (D2) |
| `weather_2025-06-15.csv` | The day's weather forecast, the dropper's format. The batch job does not read weather yet (backlog R22); it is here so the fixture day is complete |
| `expected_bills_2025-06-15.csv` | The bills the batch job must produce, **computed by hand** below, never from the code under test |

The tariff file, not the `households` table, sets each household's tier, subsidy and rates
for the day (D2: the file is authoritative). The first three households are D5's worked
examples, in D5's order.

## The expected bills, by hand

The formula is D5's: net the day's totals (`self_consumed = min(solar, consumption)`), price
the billable import through the blocks (boundaries 60 and 120 kWh, `config/base.yaml`),
round each block's charge to the cent half-up, then
`final = energy + fixed − subsidy − export credit`.

**HH-0001, D5 "boundary tie".** Two readings of 30.0050 kWh, no solar, plus a retransmission
of the first (same meter and `event_ts`, new `event_id`), which the dedup removes. TIER_2,
no subsidy.

- Consumption 60.0100; billable 60.0100.
- Block 1: 60 × 8.00 = 480.00. Block 2: 0.0100 × 16.50 = 0.165 → **0.17**.
- Energy 480.17; fixed 240.00; final **720.17**. 2 readings, 1 duplicate removed.

**HH-0002, D5 "typical subsidised solar".** Readings (30.0000 kWh, 12.5000 solar) and
(31.2345, 13.0000). TIER_2, subsidy 25 %.

- Consumption 61.2345, solar 25.5000: self-consumed 25.5000, billable 35.7345, export 0.
- Block 1: 35.7345 × 8.00 = 285.876 → **285.88**.
- Subsidy 285.88 × 25 % = 71.47; fixed 240.00; final 285.88 + 240.00 − 71.47 = **454.41**.

**HH-0003, D5 "net exporter".** Two readings of (2.0000 kWh, 9.5000 solar). TIER_1.

- Consumption 4.0000, solar 19.0000: self-consumed 4.0000, billable 0, export 15.0000.
- Energy 0.00; export credit 15 × 18.00 = 270.00; fixed 120.00; final **−150.00**.

**HH-0004, all three blocks.** Two readings of 62.5000 kWh, no solar. TIER_3, no subsidy.

- Billable 125.0000.
- Block 1: 60 × 8.00 = 480.00. Block 2: 60 × 16.50 = 990.00. Block 3: 5 × 24.50 = 122.50.
- Energy 1,592.50; fixed 480.00; final **2,072.50**.

**The rate check (T172's "Done when").** The test then raises HH-0001's `block_1_rate` from
8.00 to 9.00 and bills the day again: block 1 becomes 60 × 9.00 = 540.00, energy 540.17 and
the final bill **780.17**, while the other three stay as above. A test that only checked
plumbing would not notice the rate at all.
