// The shell: navigation, the simulated clock, system health and alerts, and the polling
// loop every view hangs off.
//
// Nothing is pushed to this page (§6): it polls every 5 s (§8.3) and only asks for what
// the visible view shows. A hidden tab stops polling altogether.

import { api } from "./api.js";
import { hideTip, showTip } from "./charts.js";
import { countdown, h, hhmm, icon, isoDay, longDay, realTime, store } from "./util.js";
import bills from "./views/bills.js";
import docs from "./views/docs.js";
import live from "./views/live.js";
import report from "./views/report.js";

const POLL_MS = 5000;
const STATUS_MS = 10000;
const DAY_MS = 86400e3;

const views = { live, bills, report, docs };
const $ = (id) => document.getElementById(id);
const root = document.documentElement;

// -------------------------------------------------------------- shared ctx

const bus = new EventTarget();

// The API's clock, re-synced every poll and advanced locally in between at time_scale,
// so the header ticks smoothly instead of jumping 24 simulated minutes every 5 s.
const clock = { simMs: null, perf: 0, scale: null };

let householdsPromise = null;

const ctx = {
  api,
  bus,
  state: { zones: [], zonesError: null, zonesLoaded: false, newestWindowStart: null },
  simNow: () => (clock.simMs === null ? null : clock.simMs + (performance.now() - clock.perf) * clock.scale),
  timeScale: () => clock.scale,
  /** Today in simulated time, or null before the first clock sync. */
  simDay: () => (clock.simMs === null ? null : isoDay(ctx.simNow())),
  /** The newest simulated day the speed layer has written a window for. */
  dataDay: () => ctx.state.newestWindowStart?.slice(0, 10) ?? null,
  households() {
    householdsPromise ??= api.households().catch((err) => {
      householdsPromise = null; // try again on the next call
      throw err;
    });
    return householdsPromise;
  },
  setParams(view, params) {
    history.replaceState(null, "", hashFor(view, params));
  },
  toast,
};

// ------------------------------------------------------------------ router

let current = null;

function hashFor(name, params = {}) {
  const query = new URLSearchParams(Object.entries(params).filter(([, v]) => v)).toString();
  return `#/${name}${query ? `?${query}` : ""}`;
}

function parseHash() {
  const [name, query = ""] = location.hash.replace(/^#\/?/, "").split("?");
  return { name: views[name] ? name : "live", params: Object.fromEntries(new URLSearchParams(query)) };
}

function route() {
  const { name, params } = parseHash();
  if (name !== current) {
    if (current) {
      views[current].leave?.();
      $(`view-${current}`).hidden = true;
    }
    current = name;
    const section = $(`view-${name}`);
    section.hidden = false;
    section.classList.remove("view-enter");
    void section.offsetWidth; // restart the entrance animation
    section.classList.add("view-enter");
    for (const link of document.querySelectorAll(".nav a[data-view]")) {
      if (link.dataset.view === name) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    }
    $("pageTitle").textContent = views[name].title;
    $("pageSub").textContent = views[name].subtitle;
    document.title = `${views[name].title} · voltstream`;
    window.scrollTo({ top: 0 });
    hideTip();
    closeDrawer();
  }
  views[name].enter(params);
}

// ----------------------------------------------------------------- sidebar

function closeDrawer() {
  root.classList.remove("nav-open");
  $("menuBtn").setAttribute("aria-expanded", "false");
}

function wireSidebar() {
  $("menuBtn").addEventListener("click", () => {
    const open = root.classList.toggle("nav-open");
    $("menuBtn").setAttribute("aria-expanded", String(open));
  });
  $("scrim").addEventListener("click", closeDrawer);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeDrawer();
  });
  $("collapseBtn").addEventListener("click", () => {
    const collapsed = root.classList.toggle("sidebar-collapsed");
    store.set("vs-sidebar", collapsed ? "collapsed" : "open");
    $("collapseBtn").setAttribute("aria-label", collapsed ? "Expand sidebar" : "Collapse sidebar");
    $("collapseBtn").title = collapsed ? "Expand sidebar" : "Collapse sidebar";
  });

  // The other tools run on their own ports on the same host (docker-compose.yml).
  const ports = { grafana: 3000, airflow: 8080, prometheus: 9090 };
  for (const link of document.querySelectorAll("a[data-ext]")) {
    const port = ports[link.dataset.ext];
    if (port) link.href = `${location.protocol}//${location.hostname}:${port}/`;
  }
}

// Every chart card can show its data as a table instead: the accessible twin of the
// chart, and the way to read exact values without hovering.
function wireTableToggles() {
  document.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-table-toggle]");
    if (!btn) return;
    const card = btn.closest(".card");
    const asTable = btn.getAttribute("aria-pressed") !== "true";
    btn.setAttribute("aria-pressed", String(asTable));
    btn.title = asTable ? "Show as chart" : "Show as table";
    btn.setAttribute("aria-label", btn.title);
    card.querySelectorAll(".chart-box, .legend-top").forEach((n) => { n.hidden = asTable; });
    card.querySelectorAll(".table-box").forEach((n) => { n.hidden = !asTable; });
  });
}

// ------------------------------------------------------------------- theme

function effectiveTheme() {
  return root.dataset.theme ?? (matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark");
}

function paintThemeButton() {
  const next = effectiveTheme() === "dark" ? "light" : "dark";
  $("themeBtn").replaceChildren(icon(next === "light" ? "sun" : "moon"));
  $("themeBtn").setAttribute("aria-label", `Switch to ${next} theme`);
  $("themeBtn").title = `Switch to ${next} theme`;
}

function wireTheme() {
  paintThemeButton();
  $("themeBtn").addEventListener("click", () => {
    const next = effectiveTheme() === "dark" ? "light" : "dark";
    root.dataset.theme = next;
    store.set("vs-theme", next);
    paintThemeButton();
  });
  matchMedia("(prefers-color-scheme: light)").addEventListener("change", paintThemeButton);
}

// ------------------------------------------------------------------- clock

let lastDay = null;

async function syncClock() {
  try {
    const c = await api.clock();
    clock.simMs = Date.parse(c.sim_now);
    clock.perf = performance.now();
    clock.scale = c.time_scale;
    $("simScale").textContent = `${c.time_scale}×`;
    $("simclock").classList.remove("is-stale");
  } catch {
    $("simclock").classList.add("is-stale");
  }
}

function renderClock() {
  const now = ctx.simNow();
  if (now === null) return;
  $("simTime").textContent = hhmm(now);
  $("simDate").textContent = longDay(now);
  const into = ((now % DAY_MS) + DAY_MS) % DAY_MS;
  $("dayFill").style.setProperty("--p", (into / DAY_MS).toFixed(4));
  const realLeft = (DAY_MS - into) / clock.scale / 1000;
  $("dayClose").textContent = `day closes in ${countdown(realLeft)}`;
  $("simclock").classList.toggle("closing", realLeft <= 30);

  const day = isoDay(now);
  if (lastDay && day !== lastDay) {
    const sc = $("simclock");
    sc.classList.remove("rollover");
    void sc.offsetWidth;
    sc.classList.add("rollover");
    bus.dispatchEvent(new CustomEvent("dayclose", { detail: { closed: lastDay, opened: day } }));
    toast({
      title: `Simulated ${longDay(`${lastDay}T00:00:00Z`)} closed`,
      body: "The batch layer bills it once the day's tariff file lands — watch the badge flip to Final.",
      color: "var(--c-batch)",
    });
  }
  lastDay = day;
}

// ------------------------------------------------------------ health, alerts

let health = { reachable: null, dependencies: [] };

async function refreshHealth() {
  try {
    const r = await api.health();
    health = { reachable: true, dependencies: r.dependencies };
  } catch {
    health = { reachable: false, dependencies: [] };
  }
  const state = (ok) => (ok === null || ok === undefined ? "" : ok ? "good" : "bad");
  const dep = (name) => health.dependencies.find((d) => d.name === name)?.healthy;
  const dots = {
    api: health.reachable,
    postgres: health.reachable ? dep("postgres") : null,
    objectstore: health.reachable ? dep("objectstore") : null,
  };
  for (const [name, ok] of Object.entries(dots)) {
    document.querySelector(`.dot[data-dep="${name}"]`).dataset.state = state(ok);
  }
  const allGood = Object.values(dots).every(Boolean);
  $("healthText").textContent = health.reachable === false ? "API down" : allGood ? "Healthy" : "Degraded";
}

function healthTip() {
  const rect = $("healthBtn").getBoundingClientRect();
  const line = (label, ok) => ({
    color: ok === undefined || ok === null ? "var(--muted)" : ok ? "var(--good)" : "var(--critical)",
    box: true,
    label,
    value: ok === undefined || ok === null ? "unknown" : ok ? "up" : "down",
  });
  const dep = (name) => health.dependencies.find((d) => d.name === name)?.healthy;
  showTip(rect.left, rect.bottom - 6, "System health", [
    line("API", health.reachable),
    line("PostgreSQL", health.reachable ? dep("postgres") : null),
    line("Object store", health.reachable ? dep("objectstore") : null),
  ]);
}

let alerts = { alerts: [], available: null };
let dismissedKey = null;

async function refreshAlerts() {
  try {
    alerts = await api.alerts();
  } catch {
    alerts = { alerts: [], available: false, warning: "The alerts endpoint did not answer." };
  }
  const chip = $("alertChip");
  const n = alerts.alerts.length;
  const critical = alerts.alerts.some((a) => a.severity === "critical");
  let tone = "good";
  let text = "No alerts";
  let glyph = "check";
  if (!alerts.available) {
    tone = "muted";
    text = "Alerts unknown";
    glyph = "alert";
  } else if (n) {
    tone = critical ? "critical" : "warning";
    text = `${n} alert${n === 1 ? "" : "s"} firing`;
    glyph = "alert";
  }
  chip.dataset.tone = tone;
  chip.title = alerts.available ? text : alerts.warning ?? "Alert state unknown";
  chip.replaceChildren(icon(glyph), h("span", {}, text));
  renderBanner();
}

function renderBanner() {
  const key = alerts.alerts.map((a) => `${a.name}:${a.since}`).sort().join("|");
  const banner = $("alertBanner");
  if (!alerts.alerts.length || key === dismissedKey) {
    banner.hidden = true;
    return;
  }
  $("alertList").replaceChildren(
    ...alerts.alerts.map((a) =>
      h(
        "div",
        { class: "alert-item" },
        h("span", { class: `sev ${a.severity === "critical" ? "critical" : "warning"}` }, a.severity),
        h("b", {}, a.name),
        a.summary ? h("span", {}, a.summary) : null,
        a.since ? h("small", {}, `since ${realTime(a.since)}`) : null,
      ),
    ),
  );
  banner.hidden = false;
}

function wireStatus() {
  const btn = $("healthBtn");
  btn.addEventListener("pointerenter", healthTip);
  btn.addEventListener("focus", healthTip);
  btn.addEventListener("pointerleave", hideTip);
  btn.addEventListener("blur", hideTip);
  btn.addEventListener("click", refreshHealth);
  $("alertChip").addEventListener("click", () => {
    if (alerts.alerts.length) {
      dismissedKey = $("alertBanner").hidden ? null : dismissedKey;
      renderBanner();
      $("alertBanner").scrollIntoView({ block: "nearest", behavior: "smooth" });
    } else {
      refreshAlerts();
    }
  });
  $("alertDismiss").addEventListener("click", () => {
    dismissedKey = alerts.alerts.map((a) => `${a.name}:${a.since}`).sort().join("|");
    $("alertBanner").hidden = true;
  });
}

// ------------------------------------------------------------------ toasts

function toast({ title, body, color }) {
  const node = h(
    "div",
    { class: "toast", role: "status", style: color ? `--k:${color}` : null },
    h("div", { class: "toast-title" }, title),
    body ? h("div", { class: "toast-body" }, body) : null,
  );
  $("toasts").append(node);
  setTimeout(() => {
    node.classList.add("out");
    setTimeout(() => node.remove(), 400);
  }, 7000);
}

// --------------------------------------------------------------------- poll

async function refreshZones() {
  try {
    const zones = await api.zonesLoad();
    ctx.state.zones = zones;
    ctx.state.zonesError = null;
    ctx.state.zonesLoaded = true;
    const newest = zones.map((z) => z.window_start).sort().pop();
    if (newest) ctx.state.newestWindowStart = newest;
  } catch (err) {
    // Keep the last good values on screen, marked stale: a dashboard that blanks itself
    // on one failed poll is worse than one that says it is out of date.
    ctx.state.zonesError = err;
  }
}

let ticking = false;

async function tick() {
  if (ticking) return;
  ticking = true;
  try {
    // Clock and zones first: the views read simulated now and the newest window's day.
    await Promise.all([syncClock(), refreshZones()]);
    renderClock();
    await views[current]?.refresh?.();
  } finally {
    ticking = false;
  }
}

// --------------------------------------------------------------------- init

wireSidebar();
wireTheme();
wireStatus();
wireTableToggles();
for (const view of Object.values(views)) view.mount?.(ctx);
window.addEventListener("hashchange", route);
route();
tick();
refreshHealth();
refreshAlerts();
setInterval(() => !document.hidden && tick(), POLL_MS);
setInterval(() => {
  if (document.hidden) return;
  refreshHealth();
  refreshAlerts();
}, STATUS_MS);
setInterval(renderClock, 250);
document.addEventListener("visibilitychange", () => !document.hidden && tick());
