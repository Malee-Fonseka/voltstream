-- voltstream — application database schema (decisions T035-T040, §6.4).
--
-- Applied by the postgres-init bootstrap service (T045), in numeric filename order:
-- 01_schema.sql (this file) -> 02_indexes.sql -> 03_seed_households.sql. Every statement
-- is written to be idempotent (`IF NOT EXISTS`), because postgres-init runs again on
-- every `compose up`, not only on first boot (§8.2: "Tier 2 is the one people forget").
--
-- household_bill_daily (batch view) and household_running_rt (speed view) are
-- deliberately SEPARATE tables — the two Lambda layers never overwrite one another, and
-- every row carries unambiguous provenance. pipeline_runs.status is what the merge
-- function (T127) queries to decide whether a day is finalised.

-- ============================================================================
-- households — the known-household dimension (T038). §6.4 never defined this table
-- even though 03_seed_households.sql and core/validation.py's unknown_household_rate
-- check both assume it exists.
-- ============================================================================
CREATE TABLE IF NOT EXISTS households (
    household_id TEXT PRIMARY KEY,
    meter_id TEXT NOT NULL UNIQUE,
    grid_zone TEXT NOT NULL,
    billing_tier TEXT NOT NULL,
    subsidy_flag BOOLEAN NOT NULL,
    has_solar BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================================
-- zone_metrics_rt — SPEED VIEW: per-(zone, window) load, upserted every micro-batch.
-- ============================================================================
CREATE TABLE IF NOT EXISTS zone_metrics_rt (
    grid_zone TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    total_consumption_kwh NUMERIC(12,4) NOT NULL,
    total_solar_kwh NUMERIC(12,4) NOT NULL,
    renewable_ratio NUMERIC(5,4) NOT NULL,
    active_meters INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (grid_zone, window_start)
);

-- ============================================================================
-- household_running_rt — SPEED VIEW: per-household provisional daily total and bill.
-- §6.4 plus the D4 bill-component columns, so a provisional response has the same shape
-- as a finalised one (household_bill_daily) and the API's merge function (T127) can map
-- either row onto one response model without special-casing.
-- ============================================================================
CREATE TABLE IF NOT EXISTS household_running_rt (
    household_id TEXT NOT NULL,
    sim_date DATE NOT NULL,
    consumption_kwh NUMERIC(12,4) NOT NULL,
    solar_kwh NUMERIC(12,4) NOT NULL,
    self_consumed_kwh NUMERIC(12,4) NOT NULL,  -- D4
    billable_import_kwh NUMERIC(12,4) NOT NULL,
    export_kwh NUMERIC(12,4) NOT NULL,
    energy_charge NUMERIC(12,2) NOT NULL,  -- D4
    fixed_charge NUMERIC(12,2) NOT NULL,  -- D4
    subsidy_discount NUMERIC(12,2) NOT NULL,  -- D4
    export_credit NUMERIC(12,2) NOT NULL,  -- D4
    tier_breakdown JSONB NOT NULL,  -- D4: per-block kWh and charge
    estimated_bill NUMERIC(12,2) NOT NULL,  -- total; == sum of the four components above
    tariff_source_date DATE NOT NULL,  -- yesterday's: deliberately stale (§3.1)
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (household_id, sim_date)
);

-- ============================================================================
-- household_bill_daily — BATCH VIEW: authoritative, finalised daily bill.
-- ============================================================================
CREATE TABLE IF NOT EXISTS household_bill_daily (
    household_id TEXT NOT NULL,
    sim_date DATE NOT NULL,
    consumption_kwh NUMERIC(12,4) NOT NULL,
    solar_kwh NUMERIC(12,4) NOT NULL,
    self_consumed_kwh NUMERIC(12,4) NOT NULL,
    billable_import_kwh NUMERIC(12,4) NOT NULL,
    export_kwh NUMERIC(12,4) NOT NULL,
    energy_charge NUMERIC(12,2) NOT NULL,
    fixed_charge NUMERIC(12,2) NOT NULL,
    subsidy_discount NUMERIC(12,2) NOT NULL,
    export_credit NUMERIC(12,2) NOT NULL,
    final_bill NUMERIC(12,2) NOT NULL,  -- may be negative (D5: never clamped)
    tier_breakdown JSONB NOT NULL,  -- per-block kWh and charge
    tariff_effective_date DATE NOT NULL,
    readings_count INTEGER NOT NULL,
    duplicates_removed INTEGER NOT NULL,
    is_finalised BOOLEAN NOT NULL DEFAULT true,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    pipeline_run_id UUID NOT NULL,  -- lineage -> pipeline_runs.run_id
    PRIMARY KEY (household_id, sim_date)
);

-- ============================================================================
-- zone_metrics_daily — BATCH VIEW: authoritative per-zone daily aggregate (T037). §6.1's
-- layer diagram lists this table but §6.4 never defined it; daily_zone_rollup.py (T124)
-- writes to it and the reconciliation cross-check (T125) reads it.
-- ============================================================================
CREATE TABLE IF NOT EXISTS zone_metrics_daily (
    grid_zone TEXT NOT NULL,
    sim_date DATE NOT NULL,
    total_consumption_kwh NUMERIC(12,4) NOT NULL,
    total_solar_kwh NUMERIC(12,4) NOT NULL,
    self_consumed_kwh NUMERIC(12,4) NOT NULL,
    export_kwh NUMERIC(12,4) NOT NULL,
    renewable_ratio NUMERIC(5,4) NOT NULL,
    peak_window_start TIMESTAMPTZ NOT NULL,
    peak_consumption_kwh NUMERIC(12,4) NOT NULL,
    active_meters INTEGER NOT NULL,
    readings_count INTEGER NOT NULL,
    pipeline_run_id UUID NOT NULL,  -- lineage -> pipeline_runs.run_id
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (grid_zone, sim_date)
);

-- ============================================================================
-- rejected_records — dead-letter evidence for the observability rubric row.
-- ============================================================================
CREATE TABLE IF NOT EXISTS rejected_records (
    id BIGSERIAL PRIMARY KEY,
    stage TEXT NOT NULL,   -- 'speed' | 'batch'
    reason TEXT NOT NULL,  -- from core/validation.py's fixed vocabulary
    trace_id TEXT,
    raw_payload JSONB NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================================
-- pipeline_runs — run ledger: lineage, and the finalisation flag the merge function
-- (T127) and is_day_finalised() (T101) read. 'superseded' added per T040: a restatement
-- (backfill) re-run of an already-successful day must not collide with the partial
-- unique index in 02_indexes.sql, so the batch job marks the prior success row
-- 'superseded' in the same transaction as inserting its own new 'success' row.
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id UUID PRIMARY KEY,
    sim_date DATE NOT NULL,
    layer TEXT NOT NULL,   -- 'batch_billing' | 'batch_rollup'
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed', 'superseded')),
    rows_in BIGINT,
    rows_out BIGINT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    -- Airflow's dag_run_id for this execution (T122). Nullable because the job runs
    -- standalone too, from `make backfill` or a bare spark-submit, and a run without an
    -- orchestrator is a normal run rather than an incomplete one.
    --
    -- This is what makes a restatement legible after the fact: two rows for the same
    -- sim_date, one superseded and one success, each naming the DAG run that produced it.
    -- Without it the ledger says a day was rebuilt but not by which execution.
    orchestrator_run_id TEXT
);

-- ============================================================================
-- reconciliation_daily — the system monitoring its own Lambda divergence (D4). §6.4 plus
-- the D4 decomposition: tariff_effect (stale-tariff cost) and data_effect (dropped-
-- backfill cost), with the invariant speed_estimate - batch_final == tariff_effect +
-- data_effect, enforced in tests (T141), not in the database.
-- ============================================================================
CREATE TABLE IF NOT EXISTS reconciliation_daily (
    household_id TEXT NOT NULL,
    sim_date DATE NOT NULL,
    speed_estimate NUMERIC(12,2) NOT NULL,
    batch_final NUMERIC(12,2) NOT NULL,
    abs_divergence NUMERIC(12,2) NOT NULL,
    pct_divergence NUMERIC(6,3) NOT NULL,
    tariff_effect NUMERIC(12,2) NOT NULL,  -- D4: S - C
    data_effect NUMERIC(12,2) NOT NULL,    -- D4: C - B
    PRIMARY KEY (household_id, sim_date)
);
