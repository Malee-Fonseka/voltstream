#!/bin/bash
# voltstream — break the pipeline on purpose and prove the alerts notice (T157-T161).
#
# §9 Phase 4: "a screenshot of MeterDataStale firing after you killed the producer is
# evidence". Each scenario injects one fault, then ASSERTS the alert through Alertmanager's
# API (the view that reaches a receiver: not silenced, not inhibited) rather than pausing
# for a human to look, then undoes the fault. Undo is registered before each change and
# runs on any exit, Ctrl+C included, so the stack is never left broken.
#
# Usage: bash scripts/inject_faults.sh [stale|rejects|sla|renewable|divergence|all]
#   (default: all)   make faults   |   .\scripts\voltstream.ps1 faults [-Scenario <name>]
#   FAULT_HOLD_SECONDS=60 keeps each alert firing a minute for screenshots, and
#   FAULT_CAPTURE=1 takes them (Windows): .\scripts\voltstream.ps1 faults -Capture
#
# Needs the stack running with at least one billed day. Times are real time: one simulated
# day is 5 real minutes. Rough durations: stale 4 min, rejects 3 min, renewable up to
# 7 min, sla 15-17 min, divergence 8-13 min. `all` overlaps the SLA wait with the first
# three, about 30 minutes in total.
#
# Watch it happen: Grafana (pipeline health), Alertmanager, and the notifications:
#   docker logs -f voltstream-api 2>&1 | grep '"stage": "alert"'
set -uo pipefail

# shellcheck source=lib/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

REJECT_RATIO='sum(rate(voltstream_records_rejected_total{layer="speed"}[5m])) / sum(rate(voltstream_events_consumed_total{layer="speed"}[5m]))'

# --------------------------------------------------------------------------------------
# Evidence: keep a firing alert up long enough to screenshot it (T164)
# --------------------------------------------------------------------------------------
#
# FAULT_HOLD_SECONDS (default 0) keeps each fault in place that long after its alert is
# asserted. FAULT_CAPTURE=1 also screenshots the evidence pages at that moment, through
# `voltstream.ps1 capture` (Windows, headless Edge), into docs/report/screenshots/ with the
# scenario as the prefix. `voltstream.ps1 faults -Capture` sets both.

hold_for_evidence() {
    local label="fault-$1" hold="${FAULT_HOLD_SECONDS:-0}" started
    [ "${hold}" -gt 0 ] 2>/dev/null || return 0
    started="$(date +%s)"
    if [ "${FAULT_CAPTURE:-0}" = "1" ] && command -v powershell.exe >/dev/null 2>&1; then
        say "  capturing the evidence pages as '${label}-*'"
        powershell.exe -NoProfile -ExecutionPolicy Bypass \
            -File "$(cygpath -w "${REPO_ROOT}/scripts/voltstream.ps1")" capture -Label "${label}" \
            | grep -E 'PASS|FAIL' | tr -d '\r' | sed 's/^/    /'
    fi
    local left=$((hold - ($(date +%s) - started)))
    if [ "${left}" -gt 0 ]; then
        say "  holding the fault ${left} s more for screenshots"
        sleep "${left}"
    fi
}

# --------------------------------------------------------------------------------------
# The producer, restarted with different fault rates (T158, T161)
# --------------------------------------------------------------------------------------

OVERRIDE_FILE=""

# producer_with_env KEY=VALUE... — recreate meter-producer with extra environment, through
# a throwaway Compose override. VOLTSTREAM__FAULTS__* variables override config/base.yaml
# (config.py step 4), so no file in the repository changes.
producer_with_env() {
    local kv
    OVERRIDE_FILE="$(mktemp "${TMPDIR:-/tmp}/voltstream-faults.XXXXXX")"
    {
        echo "services:"
        echo "  meter-producer:"
        echo "    environment:"
        for kv in "$@"; do echo "      ${kv%%=*}: '${kv#*=}'"; done
    } > "${OVERRIDE_FILE}"
    on_exit_undo restore_producer
    docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" -f "${OVERRIDE_FILE}" \
        up -d --no-deps meter-producer >/dev/null 2>&1
    say "meter-producer restarted with: $*"
}

restore_producer() {
    compose up -d --no-deps --force-recreate meter-producer >/dev/null 2>&1
    rm -f "${OVERRIDE_FILE}"
    forget_undo restore_producer
    say "meter-producer restored to the configured fault rates"
}

# --------------------------------------------------------------------------------------
# T157 — stale data
# --------------------------------------------------------------------------------------

scenario_stale() {
    section "T157 — stale data: stop the meter producer"
    say "MeterDataStale fires when no reading reaches a zone for alerts.stale_data_minutes (2 real"
    say "minutes), held for 30 s. It should also silence LowRenewableContribution for those zones."
    if alert_firing MeterDataStale; then
        fail "MeterDataStale was already firing before the fault; is the producer down?"
        return
    fi
    local t0 low_renewable

    docker stop voltstream-meter-producer >/dev/null
    on_exit_undo "docker start voltstream-meter-producer >/dev/null"
    t0="$(date +%s)"
    say "meter-producer stopped"

    if wait_until 330 "MeterDataStale for all 5 zones" alert_firing MeterDataStale 5; then
        pass "MeterDataStale firing for $(am_zones MeterDataStale)after $(($(date +%s) - t0)) s"
    else
        fail "MeterDataStale not firing for all 5 zones after 330 s ($(am_count MeterDataStale) firing)"
    fi

    # The inhibition. Prometheus still evaluates LowRenewableContribution true for any zone
    # whose renewable gauge froze at a night-time value; Alertmanager must then hold every
    # one of those back, because MeterDataStale explains them. A stop in daylight freezes
    # the gauge high, and there is nothing to silence.
    low_renewable="$(prom_int 'count(ALERTS{alertname="LowRenewableContribution",alertstate="firing"})')"
    if [ -n "${low_renewable}" ] && [ "${low_renewable}" -gt 0 ]; then
        if alert_clear LowRenewableContribution \
            && [ "$(am_inhibited_count LowRenewableContribution)" -ge "${low_renewable}" ]; then
            pass "LowRenewableContribution true for ${low_renewable} zone(s) in Prometheus, all silenced by Alertmanager (inhibition)"
        else
            fail "LowRenewableContribution not silenced: $(am_count LowRenewableContribution) of ${low_renewable} still notifying"
        fi
    else
        say "  (no LowRenewableContribution at simulated $(sim_now_text): nothing for the inhibition to silence)"
    fi
    hold_for_evidence stale

    docker start voltstream-meter-producer >/dev/null
    forget_undo "docker start voltstream-meter-producer >/dev/null"
    t0="$(date +%s)"
    say "meter-producer started again"
    if wait_until 240 "MeterDataStale to resolve" alert_clear MeterDataStale; then
        pass "MeterDataStale resolved $(($(date +%s) - t0)) s after the restart"
    else
        fail "MeterDataStale still firing 240 s after the restart"
    fi
}

# --------------------------------------------------------------------------------------
# T158 — high reject rate
# --------------------------------------------------------------------------------------

scenario_rejects() {
    section "T158 — high reject rate: restart the producer with 15 % null fields"
    say "HighRejectRate fires when the speed layer rejects more than 5 % of what it consumes over"
    say "5 minutes, held for 1 minute. Normally about 2 % is rejected."
    if alert_firing HighRejectRate; then
        fail "HighRejectRate was already firing before the fault"
        return
    fi
    local t0
    producer_with_env VOLTSTREAM__FAULTS__NULL_FIELD_RATE=0.15
    t0="$(date +%s)"

    WAIT_NOTE='printf "reject ratio %s" "$(prom_value "${REJECT_RATIO}" | cut -c1-5)"'
    if wait_until 420 "HighRejectRate" alert_firing HighRejectRate; then
        pass "HighRejectRate firing after $(($(date +%s) - t0)) s, ratio $(prom_value "${REJECT_RATIO}" | cut -c1-5)"
        hold_for_evidence rejects
    else
        fail "HighRejectRate not firing after 420 s (ratio $(prom_value "${REJECT_RATIO}" | cut -c1-5))"
    fi
    WAIT_NOTE=""

    restore_producer
    say "  it clears once the 5-minute ratio falls back under 5 %, a few minutes from now"
}

# --------------------------------------------------------------------------------------
# T159 — batch SLA miss (split so `all` can run other scenarios during the wait)
# --------------------------------------------------------------------------------------

SLA_DUE=0

sla_begin() {
    section "T159 — batch SLA miss: pause the billing DAG"
    local last
    last="$(prom_value voltstream_pg_billing_last_success_timestamp_seconds)"
    if [ -z "${last}" ]; then
        fail "no billing-success gauge in Prometheus; is sql-exporter up?"
        return 1
    fi
    airflow_cli dags pause daily_billing >/dev/null 2>&1
    on_exit_undo "airflow_cli dags unpause daily_billing >/dev/null 2>&1"
    SLA_DUE=$((${last%.*} + 900))
    say "daily_billing paused. BatchSLAMiss is due at $(date -u -d "@${SLA_DUE}" +%H:%M:%S) UTC:"
    say "15 real minutes after the last successful run (one simulated day plus the 10-minute SLA)."
}

sla_note() {
    local last
    last="$(prom_int voltstream_pg_billing_last_success_timestamp_seconds)"
    [ -n "${last}" ] && printf 'no billing for %s s' "$(($(date +%s) - last))"
}

sla_finish() {
    [ "${SLA_DUE}" -gt 0 ] || return
    local wait_s t0
    wait_s=$((SLA_DUE - $(date +%s) + 120))
    [ "${wait_s}" -ge 60 ] || wait_s=60
    WAIT_NOTE='sla_note'
    if wait_until "${wait_s}" "BatchSLAMiss" alert_firing BatchSLAMiss; then
        pass "BatchSLAMiss firing at $(date -u +%H:%M:%S) with billing paused"
        hold_for_evidence sla
    else
        fail "BatchSLAMiss not firing $(( $(date +%s) - SLA_DUE )) s past its due time"
    fi
    WAIT_NOTE=""

    airflow_cli dags unpause daily_billing >/dev/null 2>&1
    forget_undo "airflow_cli dags unpause daily_billing >/dev/null 2>&1"
    t0="$(date +%s)"
    say "daily_billing unpaused; the queued runs catch up"
    if wait_until 600 "BatchSLAMiss to resolve" alert_clear BatchSLAMiss; then
        pass "BatchSLAMiss resolved $(($(date +%s) - t0)) s after unpausing, once a run succeeded"
    else
        fail "BatchSLAMiss still firing 600 s after unpausing"
    fi
}

# --------------------------------------------------------------------------------------
# T160 — low renewable contribution
# --------------------------------------------------------------------------------------

scenario_renewable() {
    section "T160 — low renewable contribution: the simulated night"
    say "T160 asks for cloud cover forced to 100 %. The producer ignores cloud cover (backlog R16),"
    say "and even 100 % cloud leaves 20 % of solar. The night does the job instead: solar is zero"
    say "from 18:00 to 06:00 simulated, so the alert must fire at dusk and clear after dawn."
    say "simulated time now: $(sim_now_text)"
    if alert_firing LowRenewableContribution; then
        if wait_until 330 "dawn" alert_clear LowRenewableContribution; then
            pass "LowRenewableContribution cleared at simulated $(sim_now_text) (daylight)"
        else
            fail "LowRenewableContribution did not clear within 330 s"
        fi
    fi
    if wait_until 330 "dusk" alert_firing LowRenewableContribution 5; then
        pass "LowRenewableContribution firing for $(am_zones LowRenewableContribution)at simulated $(sim_now_text)"
        hold_for_evidence renewable
    else
        fail "LowRenewableContribution not firing for all 5 zones within 330 s"
    fi
}

# --------------------------------------------------------------------------------------
# T161 — Lambda divergence
# --------------------------------------------------------------------------------------

scenario_divergence() {
    section "T161 — Lambda divergence: make the speed layer drop most readings"
    say "60 % of readings are sent 3-5 simulated hours late: far past the speed layer's watermark,"
    say "so it drops them, while the batch layer bills them from the archive. The next day"
    say "reconciled under the fault diverges well past 5 %."
    if alert_firing LambdaDivergenceHigh; then
        fail "LambdaDivergenceHigh was already firing before the fault"
        return
    fi
    local t0 wait_s
    producer_with_env VOLTSTREAM__FAULTS__OUT_OF_ORDER_RATE=0.6 \
        'VOLTSTREAM__FAULTS__OUT_OF_ORDER_LATENESS_SIM_MINUTES=[180, 300]'
    t0="$(date +%s)"
    # The rest of today, one full day, then billing, rollup and reconciliation.
    wait_s=$(($(real_seconds_to_sim_midnight) + 300 + 240))
    say "expect it within ${wait_s} s: after the first day reconciled under the fault"

    WAIT_NOTE='printf "latest reconciled divergence %s %%" "$(prom_value "voltstream_lambda_divergence{job=\"reconciliation\"}" | cut -c1-5)"'
    if wait_until "${wait_s}" "LambdaDivergenceHigh" alert_firing LambdaDivergenceHigh; then
        pass "LambdaDivergenceHigh firing after $(($(date +%s) - t0)) s: mean divergence $(prom_value 'voltstream_lambda_divergence{job="reconciliation"}' | cut -c1-5) %"
        hold_for_evidence divergence
    else
        fail "LambdaDivergenceHigh not firing after ${wait_s} s"
    fi
    WAIT_NOTE=""

    restore_producer
    say "  it clears when a day reconciled without the fault replaces the gauge: two simulated days"
}

# --------------------------------------------------------------------------------------

usage() {
    sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 2
}

SCENARIO="${1:-all}"
case "${SCENARIO}" in
    stale | rejects | sla | renewable | divergence | all) ;;
    -h | --help | help) usage ;;
    *) echo "unknown scenario: ${SCENARIO}" >&2; usage ;;
esac

require_stack
say "voltstream fault injection: ${SCENARIO}. Simulated time $(sim_now_text)."
say "watch: Grafana ${GRAFANA}  Alertmanager ${ALERTMANAGER}"

case "${SCENARIO}" in
    stale) scenario_stale ;;
    rejects) scenario_rejects ;;
    sla) sla_begin && sla_finish ;;
    renewable) scenario_renewable ;;
    divergence) scenario_divergence ;;
    all)
        # Billing is paused first, so its 15-minute wait runs alongside the three scenarios
        # that do not need billing. Divergence runs last: it needs billing to catch up.
        sla_begin
        scenario_stale
        scenario_rejects
        scenario_renewable
        sla_finish
        scenario_divergence
        ;;
esac

summary || exit 1
