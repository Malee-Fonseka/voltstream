#!/bin/bash
# voltstream — GATE 1 infrastructure smoke test (decision T050).
#
# Brings up tiers 1-2, waits on health CONDITIONS (never `sleep N`, §8.2), then asserts:
# three containers healthy, three init jobs exited 0, both Kafka topics present, three
# MinIO buckets present (per T047: the fourth, voltstream-checkpoints, is deliberately
# not created — checkpoints live on named Docker volumes instead), all eight Postgres
# tables present, 50 seeded households across exactly 5 zones.
#
# Usage: bash scripts/smoke_test.sh   (run from anywhere inside the repository)
#
# Second mode, T167: `bash scripts/smoke_test.sh kill-speed-layer` answers §10.3's viva
# question "what happens if the speed layer dies mid-day?" on the running stack. It kills
# the speed layer (SIGKILL, no clean shutdown), shows the live view going stale while the
# raw archiver keeps consuming, restarts it, shows it resume from its checkpoint, and shows
# that bills are unaffected: a finalised day's bill is served identically before, during
# and after the kill, and the day of the kill is billed in full, because billing reads the
# master dataset, not the speed view.
set -uo pipefail

if [ "${1:-}" = "kill-speed-layer" ]; then
    # shellcheck source=lib/common.sh
    source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
    HOUSEHOLD="${2:-HH-0001}"
    OUTAGE_SECONDS=60
    require_stack

    bill_total() {
        curl -s --max-time 10 "${API}/api/v1/households/${HOUSEHOLD}/bill?date=$1" \
            | sed -n 's/.*"total":"\{0,1\}\([^",}]*\).*/\1/p'
    }
    readings_of() {
        psql_value "SELECT readings_count FROM household_bill_daily WHERE household_id = '${HOUSEHOLD}' AND sim_date = '$1'"
    }

    section "T167 — kill the speed layer mid-day"
    KILL_DAY="$(sim_today)"
    FINAL_DAY="$(psql_value "SELECT max(sim_date) FROM pipeline_runs WHERE layer = 'batch_billing' AND status = 'success'")"
    [ -n "${FINAL_DAY}" ] || { echo "no billed day yet; run this once the first day is billed" >&2; exit 2; }
    BEFORE="$(bill_total "${FINAL_DAY}")"
    ARCHIVED_BEFORE="$(prom_value 'sum(voltstream_events_consumed_total{layer="archiver"})' | cut -d. -f1)"
    say "simulated $(sim_now_text); killing the speed layer during ${KILL_DAY}"
    say "${HOUSEHOLD}'s final bill for ${FINAL_DAY} before the kill: ${BEFORE}"

    docker kill voltstream-speed-layer >/dev/null
    on_exit_undo "docker start voltstream-speed-layer >/dev/null"
    say "speed layer killed (SIGKILL); down for ${OUTAGE_SECONDS} s"
    sleep "${OUTAGE_SECONDS}"

    DURING="$(bill_total "${FINAL_DAY}")"
    AGE="$(prom_value 'max(voltstream_pg_zone_data_age_seconds)' | cut -d. -f1)"
    ARCHIVED_DURING="$(prom_value 'sum(voltstream_events_consumed_total{layer="archiver"})' | cut -d. -f1)"
    say "during the outage: the live zone view is ${AGE} s old (the dashboard gap), while the"
    say "raw archiver consumed $((ARCHIVED_DURING - ARCHIVED_BEFORE)) more readings into the master dataset"
    if [ "${ARCHIVED_DURING}" -gt "${ARCHIVED_BEFORE}" ]; then
        pass "the batch layer's input kept flowing while the speed layer was down"
    else
        fail "the raw archiver stopped consuming too"
    fi
    if [ -n "${DURING}" ] && [ "${DURING}" = "${BEFORE}" ]; then
        pass "the final bill for ${FINAL_DAY} is still served, unchanged (${DURING}), with the speed layer down"
    else
        fail "the final bill for ${FINAL_DAY} changed or vanished during the outage ('${DURING}')"
    fi

    docker start voltstream-speed-layer >/dev/null
    forget_undo "docker start voltstream-speed-layer >/dev/null"
    T0="$(date +%s)"
    say "speed layer started; it resumes from its checkpoint, not from the head of the topic"
    caught_up() {
        local age lag
        age="$(prom_value 'max(voltstream_pg_zone_data_age_seconds)' | cut -d. -f1)"
        lag="$(prom_value 'max(voltstream_consumer_lag{layer="speed"})' | cut -d. -f1)"
        [ -n "${age}" ] && [ "${age}" -lt 30 ] && [ "${lag:-1}" = "0" ]
    }
    if wait_until 300 "the speed layer to catch up" caught_up; then
        pass "caught up $(($(date +%s) - T0)) s after the restart: live view fresh, consumer lag 0"
    else
        fail "the speed layer had not caught up 300 s after the restart"
    fi
    PEAK_LAG="$(prom_value 'max(max_over_time(voltstream_consumer_lag{layer="speed"}[5m]))' | cut -d. -f1)"
    say "  the backlog it worked through from the checkpoint: up to ${PEAK_LAG} records of lag"

    AFTER="$(bill_total "${FINAL_DAY}")"
    if [ "${AFTER}" = "${BEFORE}" ]; then
        pass "the final bill for ${FINAL_DAY} is identical before, during and after the kill (${AFTER})"
    else
        fail "the final bill for ${FINAL_DAY} changed: ${BEFORE} before, ${AFTER} after"
    fi

    section "The day of the kill, once billed"
    billed() { [ "$(psql_value "SELECT count(*) FROM pipeline_runs WHERE sim_date = '${KILL_DAY}' AND layer = 'batch_billing' AND status = 'success'")" = "1" ]; }
    say "${KILL_DAY} closes in about $(real_seconds_to_sim_midnight) s; billing follows within two minutes"
    if wait_until $(($(real_seconds_to_sim_midnight) + 420)) "${KILL_DAY} to be billed" billed; then
        BILLS="$(psql_value "SELECT count(*) FROM household_bill_daily WHERE sim_date = '${KILL_DAY}'")"
        R_KILL="$(readings_of "${KILL_DAY}")"
        R_PREV="$(readings_of "$(date -u -d "${KILL_DAY} - 1 day" +%Y-%m-%d)")"
        say "${HOUSEHOLD}: ${R_KILL} readings billed for ${KILL_DAY}, ${R_PREV} for the day before"
        if [ "${BILLS}" = "50" ] && [ -n "${R_KILL}" ] && [ -n "${R_PREV}" ] \
            && [ $((R_KILL * 100)) -ge $((R_PREV * 95)) ]; then
            pass "${KILL_DAY} billed in full: 50 bills, and no readings lost to the outage"
        else
            fail "${KILL_DAY}: ${BILLS} bills, ${R_KILL} readings against ${R_PREV} the day before"
        fi
        say "${HOUSEHOLD}'s final bill for ${KILL_DAY}: $(bill_total "${KILL_DAY}")"
    else
        fail "${KILL_DAY} was not billed"
    fi
    summary || exit 1
    exit 0
fi

# Git Bash on Windows (this project's primary dev shell, §T009) rewrites bare
# absolute-looking arguments like `/opt/kafka/bin/...` or `/bin/sh` into Windows paths
# before they ever reach `docker exec`/`docker run`, which then fail with "no such file
# or directory" inside the (Linux) container. `MSYS_NO_PATHCONV=1` disables that — but
# only scoped to the specific commands below that pass container-internal paths, never
# exported globally, because the *host* paths this script also passes to `docker compose`
# (--env-file, -f) need the normal conversion to resolve on Windows. Harmless either way
# on native Linux/macOS shells.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="docker compose --env-file ${REPO_ROOT}/.env -f ${REPO_ROOT}/docker/docker-compose.yml"

PASS=0
FAIL=0

pass() { echo "  PASS  $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL  $1"; FAIL=$((FAIL + 1)); }

wait_for_healthy() {
    local container="$1" timeout_s="${2:-90}" waited=0
    while true; do
        local status
        status="$(docker inspect --format='{{.State.Health.Status}}' "${container}" 2>/dev/null || echo "missing")"
        if [ "${status}" = "healthy" ]; then
            return 0
        fi
        if [ "${waited}" -ge "${timeout_s}" ]; then
            return 1
        fi
        sleep 2
        waited=$((waited + 2))
    done
}

wait_for_exit() {
    local container="$1" timeout_s="${2:-60}" waited=0
    while true; do
        local state
        state="$(docker inspect --format='{{.State.Status}}' "${container}" 2>/dev/null || echo "missing")"
        if [ "${state}" = "exited" ]; then
            return 0
        fi
        if [ "${waited}" -ge "${timeout_s}" ]; then
            return 1
        fi
        sleep 2
        waited=$((waited + 2))
    done
}

echo "== GATE 1 smoke test =="
echo "-- bringing up tier 1 (kafka, postgres, minio) --"
${COMPOSE} up -d kafka postgres minio >/dev/null

for c in voltstream-kafka voltstream-postgres voltstream-minio; do
    if wait_for_healthy "${c}" 90; then
        pass "${c} reached healthy"
    else
        fail "${c} did not reach healthy within 90s"
    fi
done

echo "-- running tier 2 bootstrap (postgres-init, kafka-init, minio-init) --"
${COMPOSE} up postgres-init kafka-init minio-init >/dev/null 2>&1

for c in voltstream-postgres-init voltstream-kafka-init voltstream-minio-init; do
    if wait_for_exit "${c}" 60; then
        code="$(docker inspect --format='{{.State.ExitCode}}' "${c}")"
        if [ "${code}" = "0" ]; then
            pass "${c} exited 0"
        else
            fail "${c} exited ${code}"
        fi
    else
        fail "${c} did not exit within 60s"
    fi
done

echo "-- checking Kafka topics --"
TOPICS="$(MSYS_NO_PATHCONV=1 docker exec voltstream-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null)"
for t in "meter.readings" "meter.readings.dlq"; do
    if echo "${TOPICS}" | grep -qx "${t}"; then
        pass "topic ${t} present"
    else
        fail "topic ${t} missing"
    fi
done

echo "-- checking MinIO buckets --"
# `mc` ships inside the object-store image (D8), so it runs in the server container itself
# rather than in a separate client image. MC_HOST_<alias> supplies endpoint and credentials.
BUCKETS="$(docker exec -e MC_HOST_local=http://voltstream:voltstream-dev@localhost:9000 \
    voltstream-minio mc ls local/ 2>/dev/null)"
for b in "voltstream-raw" "voltstream-landing" "voltstream-archive"; do
    if echo "${BUCKETS}" | grep -q "${b}/"; then
        pass "bucket ${b} present"
    else
        fail "bucket ${b} missing"
    fi
done

echo "-- checking Postgres schema --"
TABLES="$(docker exec voltstream-postgres psql -U voltstream -d voltstream -tAc \
    "SELECT string_agg(table_name, ',') FROM information_schema.tables WHERE table_schema='public';" 2>/dev/null)"
for tbl in households zone_metrics_rt household_running_rt household_bill_daily \
           zone_metrics_daily rejected_records pipeline_runs reconciliation_daily; do
    if echo ",${TABLES}," | grep -q ",${tbl},"; then
        pass "table ${tbl} present"
    else
        fail "table ${tbl} missing"
    fi
done

echo "-- checking seeded households --"
HH_COUNT="$(docker exec voltstream-postgres psql -U voltstream -d voltstream -tAc \
    "SELECT count(*) FROM households;" 2>/dev/null | tr -d '[:space:]')"
if [ "${HH_COUNT}" = "50" ]; then
    pass "50 households seeded"
else
    fail "expected 50 households, found '${HH_COUNT}'"
fi

ZONE_COUNT="$(docker exec voltstream-postgres psql -U voltstream -d voltstream -tAc \
    "SELECT count(DISTINCT grid_zone) FROM households;" 2>/dev/null | tr -d '[:space:]')"
if [ "${ZONE_COUNT}" = "5" ]; then
    pass "households span 5 zones"
else
    fail "expected 5 zones, found '${ZONE_COUNT}'"
fi

echo
echo "== summary: ${PASS} passed, ${FAIL} failed =="
if [ "${FAIL}" -eq 0 ]; then
    echo "GATE 1: PASS"
    exit 0
else
    echo "GATE 1: FAIL"
    exit 1
fi
