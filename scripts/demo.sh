#!/bin/bash
# voltstream — the demo, end to end, with no human input (T163, §8.2).
#
#   1. Start the stack and wait on its health checks. Never `sleep 30` and hope.
#   2. Show one household's bill for the day that is open now: served by the speed layer,
#      provisional, costed on yesterday's tariff.
#   3. Wait for that day to close and be billed.
#   4. Show the same URL again: now served by the batch layer, final. Then the delta
#      between the two, and why they differ (tariff effect vs data effect, D4).
#   5. Point at the dashboards.
#
# Idempotent and re-runnable. On a fresh stack it anchors the simulated clock and starts
# everything. On a running stack it changes nothing and demonstrates the day open now.
# On a stopped stack with data it resumes on the existing clock, as voltstream.ps1 does,
# because a new anchor would restart simulated time on days that are already billed.
#
# Usage: bash scripts/demo.sh [HOUSEHOLD]    (default HH-0001)
#        make demo   |   .\scripts\voltstream.ps1 demo
# Takes up to one simulated day (5 real minutes) plus about two minutes of batch time.
set -uo pipefail

# shellcheck source=lib/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

HOUSEHOLD="${1:-HH-0001}"

json_field() {
    sed -n "s/.*\"$1\":\(\"[^\"]*\"\|[^,}]*\).*/\1/p" | head -n 1 | tr -d '"'
}

bill_json() { curl -s --max-time 10 "${API}/api/v1/households/${HOUSEHOLD}/bill?date=$1"; }

bill_source() { bill_json "$1" | json_field source; }

show_bill() {
    local body
    body="$(bill_json "$1")"
    printf '  %-22s %s\n' \
        "household" "$(echo "${body}" | json_field household_id)" \
        "simulated day" "$(echo "${body}" | json_field sim_date)" \
        "served by" "$(echo "${body}" | json_field source) layer (provisional: $(echo "${body}" | json_field provisional))" \
        "tariff applied" "$(echo "${body}" | json_field tariff_date)" \
        "consumption kWh" "$(echo "${body}" | json_field consumption_kwh)" \
        "total" "$(echo "${body}" | json_field total)"
}

# --------------------------------------------------------------------------------------
# 1. The stack
# --------------------------------------------------------------------------------------

section "1. Starting the stack"
[ -f "${ENV_FILE}" ] || cp "${REPO_ROOT}/.env.example" "${ENV_FILE}"
if docker volume inspect voltstream_postgres_data >/dev/null 2>&1; then
    say "existing data: keeping the clock anchored at $(env_value VOLTSTREAM_ANCHOR_REAL)"
else
    ANCHOR="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if grep -q '^VOLTSTREAM_ANCHOR_REAL=' "${ENV_FILE}"; then
        sed -i "s|^VOLTSTREAM_ANCHOR_REAL=.*|VOLTSTREAM_ANCHOR_REAL=${ANCHOR}|" "${ENV_FILE}"
    else
        echo "VOLTSTREAM_ANCHOR_REAL=${ANCHOR}" >> "${ENV_FILE}"
    fi
    say "fresh stack: clock anchored at ${ANCHOR} = simulated ${EPOCH_SIM}"
fi
say "starting, and waiting on every health check (the first build takes a while)"
if ! compose up -d --wait --wait-timeout 600 >/dev/null 2>&1; then
    compose ps -a
    echo "Not every service became healthy; see above." >&2
    exit 1
fi
pass "stack up and healthy"
say "simulated time now: $(sim_now_text)"

# --------------------------------------------------------------------------------------
# 2. The provisional bill
# --------------------------------------------------------------------------------------

section "2. ${HOUSEHOLD}'s bill while the day is open"
DAY="$(sim_today)"
speed_bill_ready() { [ "$(bill_source "${DAY}")" = "speed" ]; }
if ! wait_until 300 "a provisional bill for ${DAY}" speed_bill_ready; then
    # The day may have closed while the stack was starting; demonstrate the next one.
    DAY="$(sim_today)"
    wait_until 300 "a provisional bill for ${DAY}" speed_bill_ready \
        || { fail "no provisional bill from the speed layer"; summary; exit 1; }
fi
say "GET ${API}/api/v1/households/${HOUSEHOLD}/bill?date=${DAY}"
show_bill "${DAY}"
pass "served by the speed layer while ${DAY} is open"

# --------------------------------------------------------------------------------------
# 3-4. The day closes; the same request is now answered by the batch layer
# --------------------------------------------------------------------------------------

section "3. Waiting for ${DAY} to close and be billed"
TO_MIDNIGHT="$(real_seconds_to_sim_midnight)"
say "${DAY} closes in about ${TO_MIDNIGHT} s; billing follows within about two minutes"
batch_bill_ready() { [ "$(bill_source "${DAY}")" = "batch" ]; }
WAIT_NOTE='printf "simulated %s" "$(sim_now_text)"'
if ! wait_until $((TO_MIDNIGHT + 420)) "${DAY} to be billed" batch_bill_ready; then
    fail "${DAY} was not billed; check the daily_billing DAG in Airflow"
    summary
    exit 1
fi
WAIT_NOTE=""

section "4. The same request, after finalisation"
say "GET ${API}/api/v1/households/${HOUSEHOLD}/bill?date=${DAY}"
show_bill "${DAY}"
pass "the merge function now serves the batch layer's final bill"

reconciled() {
    [ "$(curl -s --max-time 10 "${API}/api/v1/households/${HOUSEHOLD}/bill/delta?date=${DAY}" | json_field reconciled)" = "true" ]
}
wait_until 180 "reconciliation of ${DAY}" reconciled || true
DELTA="$(curl -s --max-time 10 "${API}/api/v1/households/${HOUSEHOLD}/bill/delta?date=${DAY}")"
say "GET ${API}/api/v1/households/${HOUSEHOLD}/bill/delta?date=${DAY}"
printf '  %-22s %s\n' \
    "provisional estimate" "$(echo "${DELTA}" | json_field speed_estimate)" \
    "final bill" "$(echo "${DELTA}" | json_field batch_final)" \
    "delta (speed - batch)" "$(echo "${DELTA}" | json_field delta)" \
    "delta %" "$(echo "${DELTA}" | json_field delta_pct)" \
    "  from the tariff" "$(echo "${DELTA}" | json_field tariff_effect)   (yesterday's rates vs today's)" \
    "  from the data" "$(echo "${DELTA}" | json_field data_effect)   (readings the speed layer dropped)"

# --------------------------------------------------------------------------------------
# 5. Where to look
# --------------------------------------------------------------------------------------

section "5. Where to look"
echo "  Dashboard      ${API}/        (the badge flips from Provisional to Final)"
echo "  API docs       ${API}/docs"
echo "  Grafana        ${GRAFANA}/     (pipeline health, grid operations, Lambda divergence)"
echo "  Airflow        http://localhost:$(env_value AIRFLOW_HOST_PORT 8080)/"
echo "  Alertmanager   ${ALERTMANAGER}/"
# Best effort, and never fatal: a headless machine has nothing to open.
case "$(uname -s)" in
    MINGW* | MSYS* | CYGWIN*) cmd.exe //c start "" "${API}/" >/dev/null 2>&1 || true ;;
    Darwin) open "${API}/" >/dev/null 2>&1 || true ;;
    *) command -v xdg-open >/dev/null && xdg-open "${API}/" >/dev/null 2>&1 || true ;;
esac

summary || exit 1
