-- voltstream — secondary indexes (decision T041). Applied after 01_schema.sql. Every
-- statement uses IF NOT EXISTS so re-running this file on a later `compose up` is a
-- no-op, not an error.

-- zone_metrics_rt: the speed layer's zone-load read path always wants "latest window
-- first" (GET /api/v1/zones/load, T107).
CREATE INDEX IF NOT EXISTS idx_zone_metrics_rt_window
    ON zone_metrics_rt (window_start DESC);

-- ...and the newest window per zone: `SELECT DISTINCT ON (grid_zone) ... ORDER BY
-- grid_zone, window_start DESC` (repositories.get_latest_zone_metrics). Neither the
-- ascending primary key nor the index above matches that order (R32).
CREATE INDEX IF NOT EXISTS idx_zone_metrics_rt_zone_window
    ON zone_metrics_rt (grid_zone, window_start DESC);

-- rejected_records: the alerts/observability read path scans recent rejections.
CREATE INDEX IF NOT EXISTS idx_rejected_at
    ON rejected_records (rejected_at DESC);

-- pipeline_runs: T040's fix for the restatement trap — at most one 'success' row per
-- (sim_date, layer); a restatement supersedes the prior row rather than colliding with it.
CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_date_layer_success
    ON pipeline_runs (sim_date, layer) WHERE status = 'success';

-- The general (sim_date, layer, status) lookup the merge function (is_day_finalised,
-- T101) and the DAG's verification tasks use — broader than the partial index above,
-- which only covers 'success' rows.
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_date_layer_status
    ON pipeline_runs (sim_date, layer, status);

-- household_bill_daily: /reports/daily and the backfill demo both filter by sim_date.
CREATE INDEX IF NOT EXISTS idx_household_bill_daily_sim_date
    ON household_bill_daily (sim_date);

-- household_bill_history: a household's bill across every run for a day (R25).
CREATE INDEX IF NOT EXISTS idx_household_bill_history_day
    ON household_bill_history (sim_date, household_id);

-- household_running_rt: the provisional-bill read path and the daily rollup both filter
-- by sim_date within the (household_id, sim_date) primary key.
CREATE INDEX IF NOT EXISTS idx_household_running_rt_sim_date
    ON household_running_rt (sim_date);

-- reconciliation_daily: the Grafana Lambda-divergence dashboard (T154) and
-- voltstream_lambda_divergence (T138) both aggregate one sim_date at a time.
CREATE INDEX IF NOT EXISTS idx_reconciliation_daily_sim_date
    ON reconciliation_daily (sim_date);
