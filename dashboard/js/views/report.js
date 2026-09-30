// Daily report: everything known about one simulated day (T130).
//
// An open day still has a report, clearly labelled partial, and `completeness` says which
// sections exist yet — so an empty section reads as "not written yet", never as zero.

import { barList, legend, table } from "../charts.js";
import { addDays, h, hhmm, icon, num, pct, realTime } from "../util.js";

const LOAD = { id: "load", label: "Consumption", color: "var(--c-grid)" };
const SOLAR = { id: "solar", label: "Solar", color: "var(--c-solar)" };
const REJECTED = { id: "n", label: "Rejected", color: "var(--c-step)" };

let ctx;
let day = null;
let seq = 0;
let picked = false;
const $ = (id) => document.getElementById(id);

const kwh = (v) => num(v, Number(v) >= 1000 ? 0 : 1);
const isEmpty = (r) => !r.finalised && !r.zones.length && !r.billing && !r.runs.length;

function kpi(label, value, unit, sub) {
  return h(
    "div",
    { class: "kpi" },
    h("div", { class: "kpi-label" }, label),
    h("div", { class: "kpi-value" }, value, unit ? h("span", { class: "kpi-unit" }, unit) : null),
    h("div", { class: "kpi-sub" }, sub ?? ""),
  );
}

function duration(start, end) {
  if (!start || !end) return "—";
  const seconds = (Date.parse(end) - Date.parse(start)) / 1000;
  return seconds < 90 ? `${num(seconds, 0)} s` : `${num(seconds / 60, 1)} min`;
}

function render(r) {
  if ($("reportDate").value !== day) $("reportDate").value = day;
  $("reportJson").href = ctx.api.reportUrl(day);

  const done = (ok, label) => h("span", { class: `pill ${ok ? "done" : "todo"}` }, icon(ok ? "check" : "clock"), label);
  $("reportStatus").replaceChildren(
    r.finalised
      ? h("span", { class: "pill final" }, icon("layers"), "Finalised")
      : h("span", { class: "pill partial" }, icon("bolt"), day === ctx.simDay() ? "Partial · day still open" : "Partial · not billed yet"),
    done(r.completeness.bills, "Bills"),
    done(r.completeness.zone_rollup, "Zone rollup"),
    done(r.completeness.reconciliation, "Reconciliation"),
  );

  const b = r.billing;
  const rec = r.reconciliation;
  const readings = b?.readings_count;
  $("reportKpis").replaceChildren(
    kpi("Households billed", b ? num(b.households, 0) : "—", null, b ? "final bills written" : "no bills yet"),
    kpi("Energy consumed", b ? kwh(b.total_consumption_kwh) : "—", b ? "kWh" : null, "across every household"),
    kpi("Total billed", b ? num(b.total_billed, 2) : "—", null, "sum of final bills"),
    kpi("Readings", readings !== undefined ? num(readings, 0) : "—", null, b ? `${num(b.duplicates_removed, 0)} duplicates removed` : "counted by the batch layer"),
    kpi(
      "Speed vs batch",
      rec ? num(rec.mean_pct_divergence, 2) : "—",
      rec ? "%" : null,
      rec ? `mean gap over ${rec.households} households · |Δ| ${num(rec.mean_abs_divergence, 2)}` : "reconciliation not run yet",
    ),
  );

  legend($("rZoneLegend"), [LOAD, SOLAR], { box: true });
  barList($("rZoneBars"), {
    rows: r.zones.map((z) => ({ key: z.grid_zone, label: z.grid_zone, values: { load: Number(z.total_consumption_kwh), solar: Number(z.total_solar_kwh) } })),
    series: [LOAD, SOLAR],
    format: (v) => `${kwh(v)} kWh`,
    empty: r.finalised ? "The zone rollup has not been written yet." : "Written by the batch layer once the day is billed.",
  });

  $("rRejectSub").textContent = r.finalised
    ? "by reason · batch layer, deduplicated"
    : "by reason · speed layer so far";
  barList($("rRejects"), {
    rows: r.rejected.map((x) => ({ key: x.reason, label: x.reason.replaceAll("_", " "), values: { n: x.count } })),
    series: [REJECTED],
    format: (v) => num(v, 0),
    empty: "No rejected records attributed to this day.",
  });
  // Reasons are longer than zone names; give the label column the room.
  for (const row of $("rRejects").querySelectorAll(".hbar-row")) row.style.gridTemplateColumns = "150px minmax(0, 1fr)";

  if (r.zones.length) {
    table(
      $("rZoneTable"),
      [
        { label: "Zone" },
        { label: "Consumption kWh", num: true },
        { label: "Solar kWh", num: true },
        { label: "Self-consumed", num: true },
        { label: "Exported", num: true },
        { label: "Renewable", num: true },
        { label: "Peak window" },
        { label: "Peak kWh", num: true },
        { label: "Meters", num: true },
        { label: "Readings", num: true },
      ],
      r.zones.map((z) => [
        h("b", {}, z.grid_zone),
        num(z.total_consumption_kwh, 2),
        num(z.total_solar_kwh, 2),
        num(z.self_consumed_kwh, 2),
        num(z.export_kwh, 2),
        pct(z.renewable_ratio),
        hhmm(z.peak_window_start),
        num(z.peak_consumption_kwh, 3),
        String(z.active_meters),
        num(z.readings_count, 0),
      ]),
    );
  } else {
    $("rZoneTable").replaceChildren(h("p", { class: "muted" }, "No zone rollup for this day yet."));
  }

  if (r.runs.length) {
    table(
      $("rRuns"),
      [
        { label: "Status" },
        { label: "Started" },
        { label: "Duration", num: true },
        { label: "Rows in", num: true },
        { label: "Rows out", num: true },
        { label: "Airflow run" },
      ],
      r.runs.map((run) => [
        h("span", { class: `status-chip ${run.status}` }, run.status),
        realTime(run.started_at),
        duration(run.started_at, run.finished_at),
        run.rows_in === null ? "—" : num(run.rows_in, 0),
        run.rows_out === null ? "—" : num(run.rows_out, 0),
        run.orchestrator_run_id ? h("span", { class: "mono" }, run.orchestrator_run_id) : h("span", { class: "muted" }, "standalone"),
      ]),
    );
  } else {
    $("rRuns").replaceChildren(h("p", { class: "muted" }, "No billing run for this day yet."));
  }
}

async function load() {
  const token = ++seq;
  try {
    const r = await ctx.api.report(day);
    if (token !== seq) return;
    document.querySelectorAll("#view-report .card, #view-report .kpi").forEach((c) => c.classList.remove("is-stale"));
    return r;
  } catch {
    if (token === seq) document.querySelectorAll("#view-report .card, #view-report .kpi").forEach((c) => c.classList.add("is-stale"));
    return null;
  }
}

async function refresh() {
  if (!day) {
    const base = ctx.dataDay() ?? ctx.simDay();
    if (!base) return;
    // Yesterday first: it is the day most likely to be finalised, which is the report
    // worth reading. If the run is too young to have one, fall back to today.
    day = addDays(base, -1);
    picked = true;
  }
  let r = await load();
  if (r && picked && isEmpty(r)) {
    picked = false;
    day = addDays(day, 1);
    r = await load();
  }
  picked = false;
  if (r) {
    ctx.setParams("report", { date: day });
    render(r);
  }
}

function go(next) {
  day = next;
  picked = false;
  ctx.setParams("report", { date: day });
  refresh();
}

export default {
  title: "Daily report",
  subtitle: "Everything known about one simulated day, from both layers and the run ledger",

  mount(c) {
    ctx = c;
    $("reportDate").addEventListener("change", (e) => e.target.value && go(e.target.value));
    $("rDayPrev").addEventListener("click", () => day && go(addDays(day, -1)));
    $("rDayNext").addEventListener("click", () => day && go(addDays(day, 1)));
  },

  enter(params) {
    if (params.date) day = params.date;
    if (day) $("reportDate").value = day;
    refresh();
  },

  refresh,
};
