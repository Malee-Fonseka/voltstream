-- voltstream — read-only access for the observability stack (T147, T151). Applied after
-- 03_seed_households.sql, and like every file here it runs again on every `compose up`,
-- so each statement is idempotent.
--
-- Grafana (anonymous viewers, T155) and sql_exporter (T147) read the application
-- database. They get their own role, not the owner's: a dashboard query should not be
-- able to write to a bill, and a slow panel should not be able to hold a connection for
-- minutes. The password comes from the environment through a psql variable
-- (`-v reader_password=...` in the postgres-init command), never from this file.

SELECT 'CREATE ROLE voltstream_reader LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'voltstream_reader')\gexec

-- Set on every run, so changing the password in .env takes effect on the next `up`.
ALTER ROLE voltstream_reader WITH LOGIN PASSWORD :'reader_password';
ALTER ROLE voltstream_reader SET default_transaction_read_only = on;
ALTER ROLE voltstream_reader SET statement_timeout = '5s';

GRANT USAGE ON SCHEMA public TO voltstream_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO voltstream_reader;
-- Tables added by a later schema file are readable too, without another grant.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO voltstream_reader;

-- ============================================================================
-- sim_clock — the real instant simulated time was anchored at, for Grafana (T153/T154).
--
-- Grafana's time axis is the real clock, and every timestamp in this database is
-- simulated: a run started today writes windows dated 2026-01-01. Without a mapping, a
-- "last 30 minutes" panel over zone_metrics_rt finds nothing. The mapping is simclock's
-- own formula turned around: real = anchor_real + (sim - epoch_sim) / time_scale.
--
-- The anchor itself lives in .env, which Postgres cannot see, so it is recovered from the
-- speed layer's writes. A window is last written after its newest reading was produced,
-- so for every row `updated_at - (window_start - epoch_sim) / time_scale` is at least the
-- anchor, and the minimum over the table is the anchor plus the fastest write latency,
-- a few real seconds. That is a shift of a few seconds on a chart measured in minutes.
--
-- epoch_sim and time_scale repeat simulation.epoch_sim and simulation.time_scale from
-- config/base.yaml; tests/unit/test_observability_config.py fails if they drift apart.
-- ============================================================================
CREATE OR REPLACE VIEW sim_clock AS
SELECT
    TIMESTAMPTZ '2026-01-01 00:00:00+00' AS epoch_sim,
    288 AS time_scale,
    min(updated_at - (window_start - TIMESTAMPTZ '2026-01-01 00:00:00+00') / 288) AS anchor_real
FROM zone_metrics_rt;

GRANT SELECT ON sim_clock TO voltstream_reader;
