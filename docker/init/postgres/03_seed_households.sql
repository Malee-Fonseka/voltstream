-- voltstream — seed the 50 households (decision T042). Applied after 02_indexes.sql.
--
-- Distribution rule (single source of truth — simulators/profiles.py and
-- simulators/reference_dropper.py MUST derive zone/tier/solar assignment from this same
-- rule, not from their own independent logic, or the database, the load-curve simulator
-- and the tariff generator will disagree about a given household):
--
--   For household number n = 1..50 (HH-0001..HH-0050, MTR-0001..MTR-0050):
--     grid_zone    = the 5 configured zones (simulation.zones in base.yaml), cycling
--                    every 5 -> exactly 10 households per zone.
--     billing_tier = TIER_1 / TIER_2 / TIER_3, cycling every 3.
--     subsidy_flag = true for every 4th household (n % 4 = 0) -> ~25% subsidised.
--     has_solar    = true for every 3rd household (n % 3 = 0) -> ~33% have solar.
--
-- ON CONFLICT DO NOTHING makes this idempotent: postgres-init (T045) runs again on
-- every `compose up`, and must not duplicate seed rows on a second run.

INSERT INTO households (household_id, meter_id, grid_zone, billing_tier, subsidy_flag, has_solar)
SELECT
    'HH-' || to_char(n, 'FM0000'),
    'MTR-' || to_char(n, 'FM0000'),
    (ARRAY['ZONE-A', 'ZONE-B', 'ZONE-C', 'ZONE-D', 'ZONE-E'])[((n - 1) % 5) + 1],
    (ARRAY['TIER_1', 'TIER_2', 'TIER_3'])[((n - 1) % 3) + 1],
    (n % 4 = 0),
    (n % 3 = 0)
FROM generate_series(1, 50) AS n
ON CONFLICT (household_id) DO NOTHING;
