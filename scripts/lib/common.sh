# shellcheck shell=bash
# voltstream — shared helpers for the Phase 13 scripts (inject_faults.sh, backfill.sh,
# demo.sh, smoke_test.sh kill-speed-layer). Sourced, never run.
#
# Deliberately plain: bash, curl, sed, awk and docker only. No jq and no host Python, so
# the scripts run in a stock Git Bash on Windows (the project's dev shell, T009) as well as
# on Linux and macOS. JSON is read with grep/sed, which is enough because every value we
# need is a flat string or number.
#
# Git Bash rewrites absolute-looking arguments such as /opt/... into Windows paths before
# they reach docker. MSYS_NO_PATHCONV=1 is set only on the commands that pass
# container-internal paths, never globally, so host paths still resolve (see smoke_test.sh).

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"

compose() { docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" "$@"; }

# Value of KEY in .env, or DEFAULT when absent or empty.
env_value() {
    local line value
    line="$(grep -E "^$1=" "${ENV_FILE}" 2>/dev/null | tail -n 1 | tr -d '\r')"
    value="${line#*=}"
    if [ -n "${line}" ] && [ -n "${value}" ]; then echo "${value}"; else echo "${2:-}"; fi
}

PROM="http://localhost:$(env_value PROMETHEUS_HOST_PORT 9090)"
ALERTMANAGER="http://localhost:$(env_value ALERTMANAGER_HOST_PORT 9093)"
API="http://localhost:$(env_value API_HOST_PORT 8000)"
GRAFANA="http://localhost:$(env_value GRAFANA_HOST_PORT 3000)"
PG_USER="$(env_value POSTGRES_USER voltstream)"
PG_DB="$(env_value POSTGRES_DB voltstream)"
MINIO_USER="$(env_value MINIO_ROOT_USER voltstream)"
MINIO_PASS="$(env_value MINIO_ROOT_PASSWORD voltstream-dev)"

# --------------------------------------------------------------------------------------
# Output and assertions
# --------------------------------------------------------------------------------------

PASSED=0
FAILED=0

say() { echo "[$(date -u +%H:%M:%S)] $*"; }
section() { echo; echo "== $* =="; }
pass() { echo "  PASS  $*"; PASSED=$((PASSED + 1)); }
fail() { echo "  FAIL  $*"; FAILED=$((FAILED + 1)); }

summary() {
    echo
    echo "== summary: ${PASSED} passed, ${FAILED} failed =="
    [ "${FAILED}" -eq 0 ]
}

# wait_until TIMEOUT_S DESCRIPTION COMMAND... — poll COMMAND every 5 s until it succeeds.
# Prints a progress line every 30 s so a long wait is visibly alive, followed by the
# output of WAIT_NOTE (a command string) when the caller sets one. Returns 1 on timeout.
#
# Timed on the wall clock, not by counting polls: on a host busy with Spark each poll can
# take longer than its 5-second sleep, and counting polls would stretch every timeout.
wait_until() {
    local timeout_s="$1" description="$2" start waited next_note=30 note
    shift 2
    start="$(date +%s)"
    while ! "$@"; do
        waited=$(($(date +%s) - start))
        if [ "${waited}" -ge "${timeout_s}" ]; then
            say "gave up waiting for ${description} after ${waited}s"
            return 1
        fi
        if [ "${waited}" -ge "${next_note}" ]; then
            note=""
            [ -n "${WAIT_NOTE:-}" ] && note=" — $(eval "${WAIT_NOTE}")"
            say "  still waiting for ${description} (${waited}s)${note}"
            next_note=$((next_note + 30))
        fi
        sleep 5
    done
    return 0
}

# --------------------------------------------------------------------------------------
# The stack
# --------------------------------------------------------------------------------------

container_running() {
    [ "$(docker inspect --format '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]
}

require_stack() {
    local c missing=""
    for c in voltstream-api voltstream-prometheus voltstream-alertmanager voltstream-postgres \
             voltstream-airflow voltstream-meter-producer voltstream-speed-layer; do
        container_running "${c}" || missing="${missing} ${c}"
    done
    if [ -n "${missing}" ]; then
        echo "The stack is not running (missing:${missing})." >&2
        echo "Start it first: .\\scripts\\voltstream.ps1 start   or   make up" >&2
        exit 2
    fi
}

psql_value() {
    docker exec voltstream-postgres psql -U "${PG_USER}" -d "${2:-${PG_DB}}" -tA -F '|' -c "$1" \
        2>/dev/null | tr -d '\r'
}

airflow_cli() { docker exec voltstream-airflow airflow "$@"; }

mc() {
    MSYS_NO_PATHCONV=1 docker exec -i -e "MC_HOST_local=http://${MINIO_USER}:${MINIO_PASS}@localhost:9000" \
        voltstream-minio mc "$@"
}

# --------------------------------------------------------------------------------------
# Prometheus and Alertmanager
# --------------------------------------------------------------------------------------

# First sample value of an instant PromQL query; empty when there is none or Prometheus did
# not answer. Callers must treat empty as unknown.
prom_value() {
    curl -sf --max-time 20 --get --data-urlencode "query=$1" "${PROM}/api/v1/query" \
        | sed -n 's/.*"value":\[[^,]*,"\([^"]*\)"\].*/\1/p' | head -n 1
}

# Integer part of prom_value, or empty.
prom_int() { prom_value "$1" | cut -d. -f1; }

# Alertmanager's alerts named $2, filtered by the query string $1. Fails, printing nothing,
# unless Alertmanager actually answered with a JSON list: an unanswered request must never
# read as "no alerts". On a laptop starved by Spark's batch containers a request can time
# out, and counting that as zero once turned "still firing" into a false "resolved".
_am_alerts() {
    local body
    body="$(curl -sf --max-time 20 "${ALERTMANAGER}/api/v2/alerts?$1&filter=alertname%3D%22$2%22")" \
        || return 1
    case "${body}" in \[*) echo "${body}" ;; *) return 1 ;; esac
}
# Alerts Alertmanager is actively notifying for: not silenced, not inhibited. This is the
# view T157-T161 assert on, because it is what reaches a receiver.
am_active() { _am_alerts "active=true&silenced=false&inhibited=false" "$1"; }
am_inhibited() { _am_alerts "active=false&silenced=false&inhibited=true" "$1"; }

# How many; "?" when Alertmanager did not answer (and a non-zero status).
_count_fingerprints() {
    local body
    body="$("$@")" || { echo "?"; return 1; }
    echo "${body}" | grep -o '"fingerprint"' | wc -l | tr -d ' '
}
am_count() { _count_fingerprints am_active "$1"; }
am_inhibited_count() { _count_fingerprints am_inhibited "$1"; }
am_zones() {
    am_active "$1" | grep -o '"grid_zone":"[^"]*"' | sed 's/.*:"\(.*\)"/\1/' | sort -u | tr '\n' ' '
}
# Both are false when Alertmanager did not answer, so a wait simply polls again.
alert_firing() { local n; n="$(am_count "$1")" && [ "${n}" -ge "${2:-1}" ]; }
alert_clear() { local n; n="$(am_count "$1")" && [ "${n}" -eq 0 ]; }

# --------------------------------------------------------------------------------------
# The simulated clock (simclock.py's formula: sim = epoch + (real - anchor) * time_scale)
# --------------------------------------------------------------------------------------

_config_value() {
    grep -E "^[[:space:]]+$1:" "${REPO_ROOT}/config/base.yaml" | head -n 1 \
        | sed -E "s/^[[:space:]]+$1:[[:space:]]*\"?([^\"#]*)\"?.*/\1/" | tr -d ' \r'
}
TIME_SCALE="$(_config_value time_scale)"
EPOCH_SIM="$(_config_value epoch_sim)"

# The anchor the running producer uses; .env's when no container exists yet.
current_anchor() {
    local anchor
    anchor="$(docker inspect voltstream-meter-producer \
        --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null \
        | sed -n 's/^VOLTSTREAM__SIMULATION__ANCHOR_REAL=//p' | tr -d '\r')"
    [ -n "${anchor}" ] || anchor="$(env_value VOLTSTREAM_ANCHOR_REAL)"
    echo "${anchor}"
}

# Simulated now as Unix seconds.
sim_now_epoch() {
    local anchor_s epoch_s now_s
    anchor_s="$(date -u -d "$(current_anchor)" +%s)"
    epoch_s="$(date -u -d "${EPOCH_SIM}" +%s)"
    now_s="$(date -u +%s)"
    echo $((epoch_s + (now_s - anchor_s) * TIME_SCALE))
}
sim_today() { date -u -d "@$(sim_now_epoch)" +%Y-%m-%d; }
sim_now_text() { date -u -d "@$(sim_now_epoch)" '+%Y-%m-%d %H:%M'; }

# Real seconds until simulated midnight.
real_seconds_to_sim_midnight() {
    local sim_s
    sim_s="$(sim_now_epoch)"
    echo $(((86400 - sim_s % 86400) / TIME_SCALE + 1))
}

# --------------------------------------------------------------------------------------
# Restoring what a script changed, however it exits
# --------------------------------------------------------------------------------------
#
# Each change registers how to undo it. The EXIT trap runs the undos in reverse order, so
# a script stopped with Ctrl+C, or failing halfway, never leaves billing paused, the
# producer stopped or a corrupted tariff file behind.

_UNDO=()
on_exit_undo() { _UNDO+=("$*"); }
forget_undo() {
    local keep=() item
    for item in "${_UNDO[@]}"; do [ "${item}" = "$*" ] || keep+=("${item}"); done
    _UNDO=("${keep[@]}")
}
_run_undos() {
    local i
    for ((i = ${#_UNDO[@]} - 1; i >= 0; i--)); do
        say "restoring: ${_UNDO[i]}"
        eval "${_UNDO[i]}" || say "  could not restore: ${_UNDO[i]}"
    done
}
trap _run_undos EXIT
trap 'exit 130' INT TERM
