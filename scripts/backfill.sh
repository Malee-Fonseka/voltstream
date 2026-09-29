#!/bin/bash
# voltstream — the restatement demo (T162, §5.6, D6).
#
# A tariff file is found to be wrong after a day was billed. The batch layer's answer is
# not a patch but a re-run: recompute the day from the immutable master dataset and the
# corrected reference data, and supersede the earlier result. This script shows that in
# one pass:
#
#   1. corrupt a rate every bill uses in the day's tariff file (block_1_rate + 45.00);
#   2. restate the day -> the wrong bills;
#   3. restore the file;
#   4. restate again -> the corrected bills;
#   5. print the three versions side by side, the run ledger and every run's bills.
#
# block_1_rate, not the block_2_rate D2 first suggested: no simulated household reaches
# block 2 in a day, so a block-2 edit would change no bill at all (backlog R01, D2).
#
# Every restatement is a new, named Airflow run (billing__<date>__r<n>, D6), so the Airflow
# UI and pipeline_runs both keep the history: the original run, the wrong one and the
# corrected one, each with its own orchestrator_run_id. The original tariff file is put
# back on any exit, Ctrl+C included.
#
# Usage: bash scripts/backfill.sh YYYY-MM-DD    make backfill d=YYYY-MM-DD
#        .\scripts\voltstream.ps1 backfill -Date YYYY-MM-DD
# Pick a day that was billed at least two simulated days ago: day D's tariff is also the
# speed layer's input for day D+1. Takes about 6-8 minutes: each run waits out the
# late-data grace (90 s) behind a freshly written tariff file, then bills.
set -uo pipefail

# shellcheck source=lib/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

DAY="${1:-}"
if ! [[ "${DAY}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "usage: bash scripts/backfill.sh YYYY-MM-DD" >&2
    exit 2
fi
require_stack

TARIFF_KEY="local/voltstream-landing/tariff/tariff_${DAY}.csv"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/voltstream-backfill.XXXXXX")"
on_exit_undo "rm -rf '${WORK}'"

# --------------------------------------------------------------------------------------

billed_successfully() {
    [ "$(psql_value "SELECT count(*) FROM pipeline_runs WHERE sim_date = '${DAY}' AND layer = 'batch_billing' AND status = 'success'")" = "1" ]
}

# Every household's final bill for the day, as "household|final_bill" lines, sorted.
snapshot_bills() {
    psql_value "SELECT household_id, final_bill FROM household_bill_daily WHERE sim_date = '${DAY}' ORDER BY household_id" \
        > "$1"
}

run_state() {
    psql_value "SELECT state FROM dag_run WHERE dag_id = 'daily_billing' AND run_id = '$1'" airflow
}

run_finished() { case "$(run_state "$1")" in success | failed) return 0 ;; *) return 1 ;; esac }

# restate RUN_ID — trigger a named billing run for the day and wait for it to finish.
restate() {
    local run_id="$1" t0 state
    say "triggering ${run_id}"
    if ! airflow_cli dags trigger daily_billing --conf "{\"sim_date\": \"${DAY}\"}" \
        --run-id "${run_id}" >/dev/null 2>&1; then
        fail "could not trigger ${run_id}"
        return 1
    fi
    t0="$(date +%s)"
    WAIT_NOTE="printf 'run state: %s' \"\$(run_state ${run_id})\""
    if ! wait_until 900 "${run_id} to finish" run_finished "${run_id}"; then
        WAIT_NOTE=""
        fail "${run_id} did not finish within 900 s"
        return 1
    fi
    WAIT_NOTE=""
    state="$(run_state "${run_id}")"
    if [ "${state}" = "success" ]; then
        pass "${run_id} succeeded in $(($(date +%s) - t0)) s"
    else
        fail "${run_id} ended ${state}: open it in Airflow to see which task"
        return 1
    fi
}

put_tariff() { mc pipe "${TARIFF_KEY}" < "$1" >/dev/null; }

# --------------------------------------------------------------------------------------

section "Restating ${DAY}"
say "simulated time now: $(sim_now_text)"
if ! billed_successfully; then
    fail "${DAY} has no successful billing run to restate (pipeline_runs)"
    summary
    exit 1
fi
if [ "$(date -u -d "${DAY} + 2 days" +%s)" -gt "$(date -u -d "$(sim_today)" +%s)" ]; then
    say "note: ${DAY} is within two simulated days of today; its tariff also feeds the speed"
    say "      layer's provisional bills for ${DAY}+1, so pick an older day for a clean demo."
fi

if ! mc cat "${TARIFF_KEY}" > "${WORK}/original.csv" 2>/dev/null || [ ! -s "${WORK}/original.csv" ]; then
    fail "cannot read ${TARIFF_KEY}"
    summary
    exit 1
fi

PREVIOUS="$(psql_value "SELECT count(*) FROM dag_run WHERE dag_id = 'daily_billing' AND run_id LIKE 'billing__${DAY}__r%'" airflow)"
RUN_WRONG="billing__${DAY}__r$((PREVIOUS + 1))"
RUN_FIXED="billing__${DAY}__r$((PREVIOUS + 2))"

snapshot_bills "${WORK}/before.txt"
say "$(wc -l < "${WORK}/before.txt" | tr -d ' ') bills on record for ${DAY}"

# 1. Corrupt block_1_rate in every row, found by header name rather than position.
awk -F',' -v OFS=',' '
    NR == 1 { for (i = 1; i <= NF; i++) if ($i == "block_1_rate") col = i; print; next }
    col { $col = sprintf("%.2f", $col + 45.00) } { print }
' "${WORK}/original.csv" > "${WORK}/corrupted.csv"
if cmp -s "${WORK}/original.csv" "${WORK}/corrupted.csv"; then
    fail "the tariff file has no block_1_rate column to corrupt"
    summary
    exit 1
fi
on_exit_undo "put_tariff '${WORK}/original.csv' && say 'original tariff file restored'"
put_tariff "${WORK}/corrupted.csv"
say "tariff_${DAY}.csv corrupted: block_1_rate $(sed -n '2p' "${WORK}/original.csv" | awk -F',' -v h="$(head -n 1 "${WORK}/original.csv")" 'BEGIN { n = split(h, cols, ","); for (i = 1; i <= n; i++) if (cols[i] == "block_1_rate") c = i } { print $c }') -> $(sed -n '2p' "${WORK}/corrupted.csv" | awk -F',' -v h="$(head -n 1 "${WORK}/original.csv")" 'BEGIN { n = split(h, cols, ","); for (i = 1; i <= n; i++) if (cols[i] == "block_1_rate") c = i } { print $c }') (first row)"

# 2. Restate with the wrong file.
restate "${RUN_WRONG}" || { summary; exit 1; }
snapshot_bills "${WORK}/wrong.txt"

# 3. Put the real file back.
put_tariff "${WORK}/original.csv"
forget_undo "put_tariff '${WORK}/original.csv' && say 'original tariff file restored'"
say "tariff_${DAY}.csv restored"

# 4. Restate with the corrected file.
restate "${RUN_FIXED}" || { summary; exit 1; }
snapshot_bills "${WORK}/fixed.txt"

# 5. Side by side.
section "Bills for ${DAY}: original, wrong (${RUN_WRONG}), corrected (${RUN_FIXED})"
join -t '|' "${WORK}/before.txt" "${WORK}/wrong.txt" | join -t '|' - "${WORK}/fixed.txt" \
    > "${WORK}/all.txt"
{
    printf '%-10s %12s %12s %12s\n' "household" "original" "wrong" "corrected"
    head -n 10 "${WORK}/all.txt" | awk -F'|' '{ printf "%-10s %12s %12s %12s\n", $1, $2, $3, $4 }'
    echo "   ..."
    awk -F'|' '{ o += $2; w += $3; f += $4 } END { printf "%-10s %12.2f %12.2f %12.2f\n", "TOTAL", o, w, f }' "${WORK}/all.txt"
}

HOUSEHOLDS="$(wc -l < "${WORK}/all.txt" | tr -d ' ')"
CHANGED="$(awk -F'|' '$3 != $2' "${WORK}/all.txt" | wc -l | tr -d ' ')"
CORRECTED="$(awk -F'|' '$4 != $3' "${WORK}/all.txt" | wc -l | tr -d ' ')"
SAME_AS_BEFORE="$(awk -F'|' '$4 == $2' "${WORK}/all.txt" | wc -l | tr -d ' ')"
echo
if [ "${HOUSEHOLDS}" -gt 0 ] && [ "${CHANGED}" = "${HOUSEHOLDS}" ]; then
    pass "the corrupted tariff changed all ${HOUSEHOLDS} bills"
else
    fail "the corrupted tariff changed ${CHANGED} of ${HOUSEHOLDS} bills"
fi
if [ "${CORRECTED}" = "${HOUSEHOLDS}" ]; then
    pass "the corrected run replaced all ${HOUSEHOLDS} wrong bills"
else
    fail "the corrected run replaced ${CORRECTED} of ${HOUSEHOLDS} wrong bills"
fi
# Not asserted equal: readings that reached the master dataset after the original run
# (a late backfill) are billed by the restatement too, which is the point of restating.
say "${SAME_AS_BEFORE} of ${HOUSEHOLDS} corrected bills equal the original to the cent; any"
say "difference is late data archived after the original run, now included."

section "Run ledger for ${DAY} (pipeline_runs)"
LEDGER="$(psql_value "SELECT status, orchestrator_run_id, to_char(finished_at, 'HH24:MI:SS') FROM pipeline_runs WHERE sim_date = '${DAY}' AND layer = 'batch_billing' ORDER BY started_at")"
echo "${LEDGER}" | awk -F'|' '{ printf "  %-11s %-32s finished %s\n", $1, $2, $3 }'
# A `failed` row is an attempt Airflow retried within the same run (each attempt records
# its own row), not a restatement, so the check reads the completed ones.
COMPLETED="$(echo "${LEDGER}" | grep -v '^failed|')"
LAST3_STATUS="$(echo "${COMPLETED}" | tail -n 3 | cut -d'|' -f1 | tr '\n' ' ')"
LAST3_RUNS="$(echo "${COMPLETED}" | tail -n 3 | cut -d'|' -f2 | sort -u | wc -l | tr -d ' ')"
RETRIED="$(echo "${LEDGER}" | grep -c '^failed|')"
if [ "${LAST3_STATUS}" = "superseded superseded success " ] && [ "${LAST3_RUNS}" = "3" ]; then
    pass "superseded -> superseded -> success, from three distinct orchestrator runs"
else
    fail "ledger ends '${LAST3_STATUS}' with ${LAST3_RUNS} distinct runs"
fi
if [ "${RETRIED}" -gt 0 ]; then
    say "  ${RETRIED} failed attempt(s) above were retried by Airflow within their run; see its log"
fi

section "Bills of every run for ${DAY} (household_bill_history)"
# household_bill_daily holds the current bills only; every run's are kept here (D6, R25),
# so the wrong bills stay on record next to the run that produced them.
HISTORY="$(psql_value "SELECT r.orchestrator_run_id, r.status, count(*), sum(h.final_bill) FROM household_bill_history h JOIN pipeline_runs r ON r.run_id = h.pipeline_run_id WHERE h.sim_date = '${DAY}' GROUP BY r.run_id, r.orchestrator_run_id, r.status, r.started_at ORDER BY r.started_at")"
echo "${HISTORY}" | awk -F'|' '{ printf "  %-32s %-11s %3s bills, total %s\n", $1, $2, $3, $4 }'
KEPT="$(echo "${HISTORY}" | grep -c .)"
if [ "${KEPT}" -ge 3 ]; then
    pass "all ${KEPT} runs' bills are on record, not only the current run's"
else
    fail "household_bill_history holds ${KEPT} run(s) for ${DAY}; expected at least 3"
fi

say "the Airflow UI shows all three runs for ${DAY}: http://localhost:$(env_value AIRFLOW_HOST_PORT 8080)/"
summary || exit 1
