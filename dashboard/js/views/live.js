// Live grid: the speed layer's view of the grid, right now.
//
// Reads /zones/load (fetched by the shell every poll) and /zones/history. The zone and
// range filters sit in one row above everything and scope every figure on the page, so
// the tiles, the chart and the bars always agree.

import { barList, legend, ring, sparkline, table, timeSeries } from "../charts.js";
import { groupBy, h, hhmm, num, pct, simSpan, smart, store, tween } from "../util.js";

const RANGES = [180, 360, 720, 1440];
const LOAD = { id: "load", label: "Consumption", color: "var(--c-grid)" };
const SOLAR = { id: "solar", label: "Solar", color: "var(--c-solar)" };

let ctx;
let range = Number(store.get("vs-range")) || 720;
let scope = "all";
let history = [];
let historyError = null;
let householdsByZone = null;
let lagTimer = null;
const $ = (id) => document.getElementById(id);

const inScope = (zone) => scope === "all" || zone === scope;
const kwh = (v) => num(v, v >= 1000 ? 0 : v >= 100 ? 1 : 2);

function zonesInScope() {
  return ctx.state.zones.filter((z) => inScope(z.grid_zone));
}

/** Per-window totals over the zones in scope, oldest first. */
function totals() {
  const byTime = new Map();
  for (const zone of history) {
    if (!inScope(zone.grid_zone)) continue;
    for (const w of zone.windows) {
      const t = Date.parse(w.window_start);
      const row = byTime.get(t) ?? { t, load: 0, solar: 0 };
      row.load += Number(w.total_consumption_kwh);
      row.solar += Number(w.total_solar_kwh);
      byTime.set(t, row);
    }
  }
  return [...byTime.values()].sort((a, b) => a.t - b.t);
}

function zoneSeries(zoneId) {
  const zone = history.find((z) => z.grid_zone === zoneId);
  const windows = zone?.windows ?? [];
  return [
    { ...LOAD, area: true, points: windows.map((w) => ({ t: Date.parse(w.window_start), v: Number(w.total_consumption_kwh) })) },
    { ...SOLAR, points: windows.map((w) => ({ t: Date.parse(w.window_start), v: Number(w.total_solar_kwh) })) },
  ];
}

// ---------------------------------------------------------------- controls

function paintRange() {
  for (const b of $("liveRange").querySelectorAll("button")) {
    b.setAttribute("aria-pressed", String(Number(b.dataset.min) === range));
  }
}

function paintScope() {
  const seg = $("liveScope");
  const zones = ctx.state.zones.map((z) => z.grid_zone).sort();
  const have = [...seg.querySelectorAll("button")].map((b) => b.dataset.zone);
  for (const zone of zones) {
    if (!have.includes(zone)) seg.append(h("button", { type: "button", "data-zone": zone }, zone.replace("ZONE-", "Zone ")));
  }
  for (const b of seg.querySelectorAll("button")) b.setAttribute("aria-pressed", String(b.dataset.zone === scope));
}

function setScope(next) {
  scope = next === scope && next !== "all" ? "all" : next;
  ctx.setParams("live", { zone: scope === "all" ? null : scope });
  render();
}

// ------------------------------------------------------------------ render

function render() {
  paintRange();
  paintScope();
  renderKpis();
  renderLoadChart();
  renderZoneBars();
  renderZoneCards();
  renderLag();
  const stale = Boolean(ctx.state.zonesError || historyError);
  document.querySelectorAll("#view-live .card, #view-live .kpi").forEach((c) => c.classList.toggle("is-stale", stale));
  const note = $("liveNote");
  note.classList.toggle("stale", stale);
  note.lastChild.textContent = stale
    ? "Zone data unavailable — showing the last values received"
    : "Speed layer · 15-simulated-minute windows";
}

function tile(id) {
  const node = $(id);
  return {
    value: node.querySelector("[data-v]"),
    sub: node.querySelector("[data-sub]"),
    spark: node.querySelector("[data-spark]"),
    meter: node.querySelector("[data-meter]"),
    of: node.querySelector("[data-of]"),
    unit: node.querySelector("[data-unit]"),
  };
}

function renderKpis() {
  const zones = zonesInScope();
  if (!ctx.state.zonesLoaded) return; // keep the skeletons until the first answer
  const load = zones.reduce((a, z) => a + Number(z.total_consumption_kwh), 0);
  const solar = zones.reduce((a, z) => a + Number(z.total_solar_kwh), 0);
  const meters = zones.reduce((a, z) => a + Number(z.active_meters), 0);
  const series = totals();
  const newest = zones.map((z) => z.window_start).sort().pop();
  const newestEnd = zones.map((z) => z.window_end).sort().pop();
  const windowText = newest ? `window ${hhmm(newest)}–${hhmm(newestEnd)}` : "no window yet";

  const t1 = tile("kpiLoad");
  tween(t1.value, zones.length ? load : null, kwh);
  t1.sub.textContent = windowText;
  sparkline(t1.spark, { series: [{ ...LOAD, area: true, points: series.map((r) => ({ t: r.t, v: r.load })) }] });

  const t2 = tile("kpiSolar");
  tween(t2.value, zones.length ? solar : null, kwh);
  t2.sub.textContent = windowText;
  sparkline(t2.spark, { series: [{ ...SOLAR, area: true, points: series.map((r) => ({ t: r.t, v: r.solar })) }] });

  const share = load > 0 ? Math.min(1, solar / load) : null;
  const t3 = tile("kpiRenew");
  tween(t3.value, share, (v) => pct(v, 1));
  t3.meter.style.setProperty("--v", share ?? 0);
  t3.sub.textContent = solar > load && load > 0 ? "solar exceeds consumption" : "of consumption met by solar";

  const t4 = tile("kpiMeters");
  const expected = householdsByZone
    ? [...householdsByZone.entries()].filter(([z]) => inScope(z)).reduce((a, [, list]) => a + list.length, 0)
    : null;
  tween(t4.value, zones.length ? meters : null, (v) => num(v, 0));
  t4.of.textContent = expected ? `/ ${expected}` : "";
  t4.meter.style.setProperty("--v", expected ? Math.min(1, meters / expected) : 0);
}

/** Lag is re-read every second: simulated now moves 4.8 minutes per real second. */
function renderLag() {
  if (!ctx.state.zonesLoaded) return;
  const t = tile("kpiLag");
  const now = ctx.simNow();
  const newestEnd = zonesInScope().map((z) => z.window_end).sort().pop();
  t.value.classList.remove("skeleton");
  if (now === null || !newestEnd) {
    t.value.textContent = "—";
    t.unit.textContent = "";
    t.sub.textContent = "waiting for the first window";
    return;
  }
  const minutes = (now - Date.parse(newestEnd)) / 60e3;
  if (minutes <= 0) {
    t.value.textContent = "Live";
    t.unit.textContent = "";
    t.sub.textContent = "the newest window is still open";
    return;
  }
  const [value, unit] = simSpan(minutes).split(/ (?=sim)/);
  t.value.textContent = value;
  t.unit.textContent = unit ?? "";
  const realSeconds = (minutes * 60) / (ctx.timeScale() || 1);
  t.sub.textContent = `≈ ${num(realSeconds, realSeconds < 10 ? 1 : 0)} s of real time behind now`;
}

function renderLoadChart() {
  const series = totals();
  const label = scope === "all" ? "All zones" : scope;
  $("loadSub").textContent = `${label} · last ${range / 60} simulated hours · kWh per 15-minute window`;
  legend($("loadLegend"), [LOAD, SOLAR]);
  timeSeries($("loadChart"), {
    label: `Consumption and solar generation, ${label}`,
    unit: "kWh",
    format: smart,
    series: [
      { ...LOAD, area: true, points: series.map((r) => ({ t: r.t, v: r.load })) },
      { ...SOLAR, area: true, points: series.map((r) => ({ t: r.t, v: r.solar })) },
    ],
    empty: ctx.state.zonesLoaded
      ? `No windows in the last ${range / 60} simulated hours. Is the speed layer running?`
      : "Loading…",
  });
  table(
    $("loadTable"),
    [{ label: "Window" }, { label: "Consumption kWh", num: true }, { label: "Solar kWh", num: true }, { label: "Renewable", num: true }],
    [...series].reverse().map((r) => [
      `${hhmm(r.t)}–${hhmm(r.t + 15 * 60e3)}`,
      num(r.load, 3),
      num(r.solar, 3),
      pct(r.load > 0 ? Math.min(1, r.solar / r.load) : 0),
    ]),
  );
}

function renderZoneBars() {
  const zones = [...ctx.state.zones].sort((a, b) => a.grid_zone.localeCompare(b.grid_zone));
  legend($("zoneLegend"), [LOAD, SOLAR], { box: true });
  barList($("zoneBars"), {
    rows: zones.map((z) => ({
      key: z.grid_zone,
      label: z.grid_zone,
      values: { load: Number(z.total_consumption_kwh), solar: Number(z.total_solar_kwh) },
    })),
    series: [LOAD, SOLAR],
    format: (v) => `${kwh(v)} kWh`,
    highlight: scope === "all" ? null : scope,
    onSelect: setScope,
    empty: ctx.state.zonesLoaded ? "Waiting for the speed layer's first window…" : "Loading…",
  });
  table(
    $("zoneTable"),
    [
      { label: "Zone" },
      { label: "Window" },
      { label: "Consumption kWh", num: true },
      { label: "Solar kWh", num: true },
      { label: "Renewable", num: true },
      { label: "Meters", num: true },
    ],
    zones.map((z) => [
      z.grid_zone,
      `${hhmm(z.window_start)}–${hhmm(z.window_end)}`,
      num(z.total_consumption_kwh, 4),
      num(z.total_solar_kwh, 4),
      pct(z.renewable_ratio),
      String(z.active_meters),
    ]),
  );
}

function renderZoneCards() {
  const grid = $("zoneCards");
  const zones = [...ctx.state.zones].sort((a, b) => a.grid_zone.localeCompare(b.grid_zone));
  if (!zones.length) {
    grid.replaceChildren(
      h(
        "div",
        { class: "zone-card placeholder" },
        h("span", { class: "live-dot" }),
        ctx.state.zonesLoaded ? "Waiting for the speed layer's first window…" : "Loading zones…",
      ),
    );
    return;
  }
  grid.querySelector(".placeholder")?.remove();
  for (const z of zones) {
    let card = grid.querySelector(`[data-zone="${z.grid_zone}"]`);
    if (!card) {
      card = h(
        "button",
        { type: "button", class: "zone-card", "data-zone": z.grid_zone },
        h("div", { class: "zc-head" }, h("span", { class: "zc-name" }, z.grid_zone), h("span", { class: "zc-meters" })),
        h(
          "div",
          { class: "zc-body" },
          h("div", {}, h("div", { class: "zc-value" }, h("span", { "data-v": "" }), h("span", { class: "kpi-unit" }, "kWh")), h("div", { class: "zc-sub" })),
          h("div", { class: "zc-ring" }),
        ),
        h("div", { class: "zc-spark" }),
        h("div", { class: "zc-foot" }),
      );
      card.addEventListener("click", () => setScope(z.grid_zone));
      grid.append(card);
    }
    const expected = householdsByZone?.get(z.grid_zone)?.length;
    card.setAttribute("aria-pressed", String(scope === z.grid_zone));
    card.setAttribute("aria-label", `${z.grid_zone}: focus the page on this zone`);
    card.querySelector(".zc-meters").textContent = `${z.active_meters}${expected ? ` / ${expected}` : ""} meters`;
    tween(card.querySelector("[data-v]"), Number(z.total_consumption_kwh), kwh);
    card.querySelector(".zc-sub").textContent = `${kwh(Number(z.total_solar_kwh))} kWh solar`;
    ring(card.querySelector(".zc-ring"), Number(z.renewable_ratio), { label: pct(z.renewable_ratio, 0) });
    sparkline(card.querySelector(".zc-spark"), { series: zoneSeries(z.grid_zone) });
    card.querySelector(".zc-foot").textContent = `window ${hhmm(z.window_start)}–${hhmm(z.window_end)} · renewable share`;
  }
}

// --------------------------------------------------------------- lifecycle

async function loadHistory() {
  try {
    history = await ctx.api.zonesHistory(range);
    historyError = null;
  } catch (err) {
    historyError = err;
  }
}

export default {
  title: "Live grid",
  subtitle: "Speed layer · grid load and renewable contribution by zone, right now",

  mount(c) {
    ctx = c;
    if (!RANGES.includes(range)) range = 720;
    $("liveRange").addEventListener("click", async (e) => {
      const b = e.target.closest("button[data-min]");
      if (!b) return;
      range = Number(b.dataset.min);
      store.set("vs-range", String(range));
      paintRange();
      await loadHistory();
      render();
    });
    $("liveScope").addEventListener("click", (e) => {
      const b = e.target.closest("button[data-zone]");
      if (b) setScope(b.dataset.zone);
    });
    ctx.households()
      .then((list) => {
        householdsByZone = groupBy(list, "grid_zone");
        if (ctx.state.zonesLoaded) renderKpis();
      })
      .catch(() => {});
  },

  enter(params) {
    scope = params.zone || "all";
    render();
    this.refresh();
    clearInterval(lagTimer);
    lagTimer = setInterval(renderLag, 1000);
  },

  leave() {
    clearInterval(lagTimer);
  },

  async refresh() {
    await loadHistory();
    render();
  },
};
