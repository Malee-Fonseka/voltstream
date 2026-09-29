// Household bills: the merge function, one household and one simulated day at a time.
//
// The same request answers from the speed layer while the day is open and from the batch
// layer once it has closed. The badge says which; this page is built to make that flip
// visible, and to show what changed when it happens (the delta card).

import { stackBar, table, waterfall } from "../charts.js";
import { HttpError } from "../api.js";
import {
  addDays, countdown, dayMs, groupBy, h, icon, isNum, longDay, num, realTime, signed, store, toNum, tween,
} from "../util.js";

const DAY_MS = 86400e3;
const COLOR = { speed: "var(--c-speed)", batch: "var(--c-batch)" };

let ctx;
let households = [];
let hh = null;
let day = null;
let bill = null;
let delta = null;
let seq = 0;
let lastShown = { key: null, source: null };
let countdownTimer = null;
const $ = (id) => document.getElementById(id);

const tierLabel = (t) => (t ? t.replace(/^TIER_/, "Tier ") : "");
const kwh = (v) => `${num(v, 2)} kWh`;
const money = (v) => num(v, 2);

function defaultDay() {
  return ctx.dataDay() ?? ctx.simDay();
}

// ---------------------------------------------------------------- controls

function optionText(x) {
  const parts = [x.household_id, tierLabel(x.billing_tier)];
  if (x.has_solar) parts.push("solar");
  if (x.subsidy_flag) parts.push("subsidised");
  return parts.join(" · ");
}

function fillSelect() {
  const select = $("hhSelect");
  if (!households.length) {
    select.replaceChildren(h("option", { value: hh ?? "" }, hh ?? "Household list unavailable"));
    return;
  }
  const byZone = groupBy(households, "grid_zone");
  select.replaceChildren(
    ...[...byZone.entries()].map(([zone, list]) =>
      h("optgroup", { label: `${zone} · ${list.length} households` }, ...list.map((x) => h("option", { value: x.household_id }, optionText(x)))),
    ),
  );
}

function paintControls() {
  if ($("hhSelect").value !== hh) {
    if (![...$("hhSelect").options].some((o) => o.value === hh)) $("hhSelect").append(h("option", { value: hh }, hh));
    $("hhSelect").value = hh;
  }
  // Only when it differs: rewriting it on every poll would fight a viewer mid-edit.
  if ($("billDate").value !== (day ?? "")) $("billDate").value = day ?? "";
  const x = households.find((it) => it.household_id === hh);
  const chips = x
    ? [
        h("span", { class: "chip" }, icon("map"), x.grid_zone),
        h("span", { class: "chip" }, icon("tag"), tierLabel(x.billing_tier)),
        x.has_solar ? h("span", { class: "chip solar" }, icon("solar"), "Rooftop solar") : h("span", { class: "chip" }, "No solar"),
        x.subsidy_flag ? h("span", { class: "chip" }, "Subsidised") : null,
      ]
    : [];
  $("hhChips").replaceChildren(...chips.filter(Boolean));
}

function select(nextHh, nextDay) {
  if (nextHh) hh = nextHh;
  if (nextDay) day = nextDay;
  store.set("vs-household", hh);
  ctx.setParams("bills", { hh, date: day });
  paintControls();
  refresh();
}

function stepHousehold(dir) {
  if (!households.length) return;
  const i = households.findIndex((x) => x.household_id === hh);
  const next = households[(i + dir + households.length) % households.length];
  select(next.household_id);
}

// ------------------------------------------------------------------ render

function stage() {
  if (!bill) return 0;
  if (bill.source === "batch") return delta?.reconciled ? 4 : 3;
  return day === ctx.simDay() ? 1 : 2;
}

function paintBadge(source) {
  const badge = $("badge");
  badge.className = `badge ${source ?? "unknown"}`;
  const text =
    source === "batch" ? "Final (batch)" : source === "speed" ? "Provisional (speed)" : "No bill";
  badge.replaceChildren(icon(source === "batch" ? "layers" : source === "speed" ? "bolt" : "clock"), h("span", { id: "badgeText" }, text));
  badge.title =
    source === "speed" ? "The day is still open or not yet billed; this is an estimate."
      : source === "batch" ? "The day has been closed by a successful billing run." : "";
}

function renderHero() {
  const card = $("billHero");
  const source = bill?.source ?? null;
  card.dataset.layer = source ?? "";
  paintBadge(source);

  const key = `${hh}|${day}`;
  if (lastShown.key === key && lastShown.source === "speed" && source === "batch") {
    // The moment §1.2 is about: the same URL, now answered by the batch layer.
    const badge = $("badge");
    badge.classList.add("flip");
    card.classList.remove("celebrate");
    void card.offsetWidth;
    card.classList.add("celebrate");
    ctx.toast({
      title: `${hh}'s bill is now final`,
      body: `The batch layer closed ${longDay(`${day}T00:00:00Z`)}. Estimate ${money(delta?.speed_estimate)} → final ${money(bill.total)}.`,
      color: "var(--c-batch)",
    });
  }
  lastShown = { key, source };

  tween($("heroTotal"), bill ? bill.total : null, (v) => (v === null ? "—" : money(v)));
  $("heroCaption").textContent = source === "batch" ? "Final bill" : source === "speed" ? "Estimated bill so far" : "";
  $("tariffNote").textContent = !bill
    ? ""
    : source === "speed"
      ? `Priced with the tariff for ${bill.tariff_date} — yesterday's, deliberately stale`
      : `Priced with the day's own tariff (${bill.tariff_date})`;

  const current = stage();
  for (const li of $("lifecycle").children) {
    const n = Number(li.dataset.step);
    li.classList.toggle("done", n < current);
    li.classList.toggle("current", n === current);
  }
  renderStatus();
}

/** The line under the lifecycle; the open-day countdown updates every second. */
function renderStatus() {
  const node = $("heroStatus");
  const current = stage();
  if (current === 1) {
    const now = ctx.simNow();
    const left = now === null ? null : (dayMs(day) + DAY_MS - now) / (ctx.timeScale() || 1) / 1000;
    node.replaceChildren(icon("clock"), `Day open · closes in ${left === null ? "—" : countdown(left)} real time`);
  } else if (current === 2) {
    node.replaceChildren(icon("clock"), "Day closed · waiting for the batch layer to bill it");
  } else if (current >= 3) {
    const run = bill.pipeline_run_id ? `run ${bill.pipeline_run_id.slice(0, 8)}` : "the batch layer";
    const at = bill.computed_at ? ` · ${realTime(bill.computed_at)}` : "";
    node.replaceChildren(icon("layers"), `Billed by ${run}${at}${current === 4 ? " · reconciled" : ""}`);
  } else {
    node.replaceChildren();
  }
}

function renderWaterfall() {
  const steps = [
    { label: "Energy charge", short: "Energy", value: toNum(bill.energy_charge) ?? 0 },
    { label: "Fixed charge", short: "Fixed", value: toNum(bill.fixed_charge) ?? 0 },
    { label: "Subsidy", short: "Subsidy", value: -(toNum(bill.subsidy_discount) ?? 0) },
    { label: "Export credit", short: "Export", value: -(toNum(bill.export_credit) ?? 0) },
    { label: "Total", short: "Total", total: true },
  ];
  waterfall($("waterfall"), {
    steps,
    format: money,
    totalColor: COLOR[bill.source],
    label: `Bill for ${hh}: energy and fixed charges, less subsidy and export credit`,
  });
  table(
    $("waterfallTable"),
    [{ label: "Component" }, { label: "Amount", num: true }],
    [
      ...steps.filter((st) => !st.total).map((st) => [st.label, signed(st.value)]),
      [h("b", {}, "Total"), h("b", {}, money(bill.total))],
    ],
  );
}

function renderEnergy() {
  const self = toNum(bill.self_consumed_kwh) ?? 0;
  const imported = toNum(bill.billable_import_kwh) ?? 0;
  const exported = toNum(bill.export_kwh) ?? 0;
  const solar = toNum(bill.solar_kwh) ?? 0;
  const parts = {
    self: { label: "Own solar, used at home", value: self, color: "var(--c-solar)" },
    grid: { label: "Imported from the grid", value: imported, color: "var(--c-grid)" },
    export: { label: "Exported to the grid", value: exported, color: "var(--c-export)" },
  };

  const consumption = h("div", { class: "flow-bar" });
  stackBar(consumption, [parts.self, parts.grid], { format: kwh, title: "Consumption" });
  const blocks = [
    h(
      "div",
      {},
      h("div", { class: "flow-head" }, h("span", {}, "Consumption"), h("b", {}, kwh(bill.consumption_kwh))),
      consumption,
      h(
        "div",
        { class: "flow-legend" },
        ...[parts.self, parts.grid].map((p) => h("span", { class: "legend-item" }, h("span", { class: "legend-key box", style: `--k:${p.color}` }), p.label, h("b", {}, kwh(p.value)))),
      ),
    ),
  ];
  if (solar > 0) {
    const gen = h("div", { class: "flow-bar" });
    stackBar(gen, [parts.self, parts.export], { format: kwh, title: "Solar generation" });
    blocks.push(
      h(
        "div",
        {},
        h("div", { class: "flow-head" }, h("span", {}, "Solar generation"), h("b", {}, kwh(solar))),
        gen,
        h(
          "div",
          { class: "flow-legend" },
          ...[parts.self, parts.export].map((p) => h("span", { class: "legend-item" }, h("span", { class: "legend-key box", style: `--k:${p.color}` }), p.label, h("b", {}, kwh(p.value)))),
        ),
      ),
    );
  } else {
    blocks.push(h("p", { class: "flow-note" }, "No solar generation at this household, so every kWh it used came from the grid."));
  }
  $("energyFlow").replaceChildren(h("div", { class: "flow" }, ...blocks));
}

function renderDelta() {
  const body = $("deltaBody");
  const est = toNum(delta?.speed_estimate ?? (bill.source === "speed" ? bill.total : null));
  const fin = toNum(delta?.batch_final ?? (bill.source === "batch" ? bill.total : null));
  const top = Math.max(Math.abs(est ?? 0), Math.abs(fin ?? 0)) || 1;
  const row = (label, value, color, glyph) =>
    h(
      "div",
      { class: "versus-row" },
      h("span", { class: "versus-label" }, h("span", { class: "key-dot", style: `--k:${color}` }), label),
      h(
        "div",
        { class: "versus-track" },
        value === null
          ? h("span", { class: "muted" }, glyph)
          : [h("span", { class: "versus-bar", style: `--c:${color}; --w:${(Math.abs(value) / top).toFixed(4)}` }), h("span", { class: "versus-value" }, money(value))],
      ),
    );

  const children = [
    h(
      "div",
      { class: "versus" },
      row("Speed estimate", est, COLOR.speed, "no estimate — the speed layer never saw this day"),
      row("Batch final", fin, COLOR.batch, "not billed yet"),
    ),
  ];

  if (est !== null && fin !== null && isNum(delta?.delta)) {
    const d = Number(delta.delta);
    const direction = d > 0 ? "high" : d < 0 ? "low" : "exact";
    children.push(
      h(
        "div",
        { class: "delta-line" },
        h("span", { class: "delta-big" }, signed(d)),
        h(
          "span",
          { class: "delta-text" },
          direction === "exact"
            ? "The estimate matched the final bill exactly."
            : `The estimate was ${money(Math.abs(d))} ${direction} — ${num(delta.delta_pct, 2)}% of the final bill's gross charges.`,
        ),
      ),
    );
    if (delta.reconciled) {
      children.push(
        h(
          "div",
          { class: "effects" },
          h("div", { class: "effect" }, h("b", {}, "Tariff effect"), h("span", { class: "v" }, signed(delta.tariff_effect)), h("span", { class: "w" }, "priced with yesterday's tariff")),
          h("div", { class: "effect" }, h("b", {}, "Data effect"), h("span", { class: "v" }, signed(delta.data_effect)), h("span", { class: "w" }, "readings the speed layer dropped or never saw")),
        ),
        h("div", { class: "check-line" }, icon("check"), "estimate − final = tariff effect + data effect"),
      );
    } else {
      children.push(h("div", { class: "pending-note" }, icon("clock"), h("span", {}, "Reconciliation has not run for this day yet. When it has, the difference is split into its two causes.")));
    }
  } else if (fin === null) {
    const open = day === ctx.simDay();
    children.push(
      h(
        "div",
        { class: "pending-note" },
        icon("clock"),
        h(
          "span",
          {},
          open
            ? "The day is still open. After simulated midnight the batch layer rescans it in full, with the day's own tariff, and its figure appears here beside the estimate."
            : "The day has closed. The batch layer bills it once the tariff file lands and the late-data grace period passes.",
        ),
      ),
    );
  }
  body.replaceChildren(...children);
}

function blockRows(raw) {
  let lines = raw;
  if (typeof lines === "string") {
    try { lines = JSON.parse(lines); } catch { lines = []; }
  }
  if (!Array.isArray(lines)) return [];
  let lower = 0;
  return lines.map((line) => {
    const upper = toNum(line.up_to_kwh);
    const width = upper === null ? null : upper - lower;
    const used = toNum(line.kwh) ?? 0;
    const row = { name: String(line.name ?? "").replace(/^block_/, "Block "), lower, upper, used, rate: line.rate, charge: line.charge, fill: width ? used / width : used > 0 ? 1 : 0 };
    if (upper !== null) lower = upper;
    return row;
  });
}

function renderTiers() {
  const rows = blockRows(bill.tier_breakdown);
  if (!rows.length) {
    $("tierBody").replaceChildren(h("p", { class: "muted" }, "No tier breakdown recorded for this bill."));
    return;
  }
  table(
    $("tierBody"),
    [{ label: "Block" }, { label: "Range" }, { label: "Used" }, { label: "kWh", num: true }, { label: "Rate", num: true }, { label: "Charge", num: true }],
    rows.map((r) => [
      h("b", {}, r.name),
      r.upper === null ? `over ${num(r.lower, 0)} kWh` : `${num(r.lower, 0)}–${num(r.upper, 0)} kWh`,
      h("div", { class: "tier-bar", title: r.upper === null ? "unbounded block" : `${num(r.fill * 100, 0)}% of the block used` }, h("i", { style: `--v:${Math.min(1, r.fill).toFixed(4)}` })),
      num(r.used, 2),
      num(r.rate, 2),
      money(r.charge),
    ]),
  );
}

function renderDetails() {
  const x = households.find((it) => it.household_id === hh);
  // Batch-only fields: present on every response, null while provisional (BillResponse).
  const pending = () => h("span", { class: "muted" }, "once final");
  const items = [
    ["Household", `${hh}${x ? ` · meter ${x.meter_id}` : ""}`],
    ["Simulated day", longDay(`${day}T00:00:00Z`)],
    ["Served by", bill.source === "batch" ? "household_bill_daily (batch)" : "household_running_rt (speed)"],
    ["Readings", bill.readings_count ?? pending()],
    ["Duplicates removed", bill.duplicates_removed ?? pending()],
    ["Billing run", bill.pipeline_run_id ? h("span", { class: "mono" }, bill.pipeline_run_id) : pending()],
    ["Computed at", bill.computed_at ? realTime(bill.computed_at) : pending()],
  ];
  $("detailBody").replaceChildren(
    h("dl", { class: "kv" }, ...items.map(([k, v]) => h("div", {}, h("dt", {}, k), h("dd", {}, typeof v === "number" ? num(v, 0) : v)))),
  );
}

function renderEmpty(reason) {
  $("billGrid").hidden = true;
  const empty = $("billEmpty");
  empty.hidden = false;
  const latest = defaultDay();
  empty.replaceChildren(
    icon("bill"),
    h("h3", {}, reason === "wait" ? "Waiting for the first simulated day" : `No bill for ${hh} on ${longDay(`${day}T00:00:00Z`)}`),
    h(
      "p",
      {},
      reason === "wait"
        ? "The speed layer has not written a window yet. This page fills in as soon as it does."
        : "Neither layer has a figure for this household on this day: the speed layer never saw it, and the batch layer has not billed it.",
    ),
    latest && latest !== day && reason !== "wait"
      ? h("button", { class: "btn primary", type: "button", onclick: () => select(null, latest) }, "Go to the latest day")
      : null,
  );
  lastShown = { key: null, source: null };
}

function render() {
  $("billEmpty").hidden = true;
  $("billGrid").hidden = false;
  renderHero();
  renderWaterfall();
  renderEnergy();
  renderDelta();
  renderTiers();
  renderDetails();
}

// --------------------------------------------------------------- lifecycle

async function refresh() {
  if (!day) day = defaultDay();
  if (!hh || !day) {
    renderEmpty("wait");
    return;
  }
  paintControls();
  const token = ++seq;
  const [billResult, deltaResult] = await Promise.allSettled([ctx.api.bill(hh, day), ctx.api.billDelta(hh, day)]);
  if (token !== seq) return; // a newer selection has been made since
  if (billResult.status === "rejected") {
    if (billResult.reason instanceof HttpError && billResult.reason.status === 404) {
      bill = null;
      renderEmpty("missing");
    } else {
      $("billGrid").classList.add("is-stale");
    }
    return;
  }
  $("billGrid").classList.remove("is-stale");
  bill = billResult.value;
  delta = deltaResult.status === "fulfilled" ? deltaResult.value : null;
  render();
}

export default {
  title: "Household bills",
  subtitle: "The merge function · an estimate while the day is open, the final bill once the batch layer closes it",

  mount(c) {
    ctx = c;
    $("hhSelect").addEventListener("change", (e) => select(e.target.value));
    $("hhPrev").addEventListener("click", () => stepHousehold(-1));
    $("hhNext").addEventListener("click", () => stepHousehold(1));
    $("billDate").addEventListener("change", (e) => e.target.value && select(null, e.target.value));
    $("dayPrev").addEventListener("click", () => day && select(null, addDays(day, -1)));
    $("dayNext").addEventListener("click", () => day && select(null, addDays(day, 1)));
    $("billLatest").addEventListener("click", () => {
      const latest = defaultDay();
      if (latest) select(null, latest);
    });
    ctx.bus.addEventListener("dayclose", () => {
      if (bill) renderHero();
    });
  },

  async enter(params) {
    try {
      households = await ctx.households();
    } catch {
      households = [];
    }
    hh = params.hh || hh || store.get("vs-household") || households[0]?.household_id || "HH-0001";
    day = params.date || day || defaultDay();
    fillSelect();
    paintControls();
    ctx.setParams("bills", { hh, date: day });
    clearInterval(countdownTimer);
    countdownTimer = setInterval(() => stage() === 1 && renderStatus(), 1000);
    await refresh();
  },

  leave() {
    clearInterval(countdownTimer);
  },

  refresh,
};
