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
set -uo pipefail

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
BUCKETS="$(MSYS_NO_PATHCONV=1 docker run --rm --network voltstream --entrypoint /bin/sh quay.io/minio/mc:latest -c \
    "mc alias set local http://minio:9000 voltstream voltstream-dev >/dev/null 2>&1 && mc ls local/" 2>/dev/null)"
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
