// Formatting and DOM helpers shared by every view.
//
// The API serialises every Decimal as a JSON string (Pydantic's convention, which keeps
// money exact over the wire), so everything here accepts strings and numbers alike.
//
// Simulated timestamps are UTC by contract, so every simulated time is formatted in UTC.
// Only `realTime` uses the viewer's zone, for the few wall-clock instants (run times,
// alert start times) the API returns.

export const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");

// ------------------------------------------------------------------ numbers

const fixedFormats = new Map();
function fixed(dp) {
  if (!fixedFormats.has(dp)) {
    fixedFormats.set(
      dp,
      new Intl.NumberFormat(undefined, { minimumFractionDigits: dp, maximumFractionDigits: dp }),
    );
  }
  return fixedFormats.get(dp);
}

export const isNum = (v) =>
  v !== null && v !== undefined && v !== "" && Number.isFinite(Number(v));
export const toNum = (v) => (isNum(v) ? Number(v) : null);

export function num(v, dp = 2) {
  return isNum(v) ? fixed(dp).format(Number(v)) : "—";
}

/** A sign always shown, and a real minus sign rather than a hyphen. */
export function signed(v, dp = 2) {
  if (!isNum(v)) return "—";
  const n = Number(v);
  const sign = n > 0 ? "+" : n < 0 ? "−" : "±";
  return sign + fixed(dp).format(Math.abs(n));
}

/** A fraction (0.23) as a percentage ("23.0%"). */
export function pct(fraction, dp = 1) {
  return isNum(fraction) ? `${fixed(dp).format(Number(fraction) * 100)}%` : "—";
}

/** Fewer digits the larger the number, for axis ticks and tiles. */
export function smart(v) {
  if (!isNum(v)) return "—";
  const a = Math.abs(Number(v));
  if (a === 0) return "0";
  if (a >= 1e6) return `${fixed(1).format(Number(v) / 1e6)}M`;
  if (a >= 1e4) return `${fixed(1).format(Number(v) / 1e3)}K`;
  if (a >= 100) return fixed(0).format(Number(v));
  if (a >= 10) return fixed(1).format(Number(v));
  return fixed(2).format(Number(v));
}

// -------------------------------------------------------------------- time

const hourMinute = new Intl.DateTimeFormat("en-GB", {
  hour: "2-digit", minute: "2-digit", hourCycle: "h23", timeZone: "UTC",
});
const dayLong = new Intl.DateTimeFormat("en-GB", {
  weekday: "short", day: "numeric", month: "short", year: "numeric", timeZone: "UTC",
});
const dayShort = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });
const wallClock = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" });

export const toDate = (v) => (v instanceof Date ? v : new Date(v));
export const hhmm = (v) => hourMinute.format(toDate(v));
export const longDay = (v) => dayLong.format(toDate(v));
export const shortDay = (v) => dayShort.format(toDate(v));
export const realTime = (v) => wallClock.format(toDate(v));
export const isoDay = (v) => toDate(v).toISOString().slice(0, 10);
export const dayMs = (iso) => Date.parse(`${iso}T00:00:00Z`);

export function addDays(iso, n) {
  const d = new Date(dayMs(iso));
  d.setUTCDate(d.getUTCDate() + n);
  return isoDay(d);
}

/** Real seconds as m:ss — the countdown to simulated midnight. */
export function countdown(seconds) {
  const s = Math.max(0, Math.round(seconds));
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
}

/** Simulated minutes, the unit lag and windows are naturally in. */
export function simSpan(minutes) {
  const m = Math.round(minutes);
  if (m < 60) return `${m} sim-min`;
  const h = Math.floor(m / 60);
  return m % 60 ? `${h} h ${m % 60} sim-min` : `${h} sim-h`;
}

// --------------------------------------------------------------------- DOM

function applyProps(node, props) {
  for (const [key, value] of Object.entries(props ?? {})) {
    if (value === undefined || value === null || value === false) continue;
    if (key === "class") node.setAttribute("class", value);
    else if (key === "text") node.textContent = value;
    else if (key === "dataset") Object.assign(node.dataset, value);
    else if (key.startsWith("on") && typeof value === "function") {
      node.addEventListener(key.slice(2), value);
    } else node.setAttribute(key, value === true ? "" : value);
  }
}

function appendChildren(node, children) {
  for (const child of children.flat(Infinity)) {
    if (child === null || child === undefined || child === false) continue;
    // Strings become text nodes, never markup: API values are data, not HTML.
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
}

/** Build an HTML element. `h("div", {class: "x"}, "text", child)` */
export function h(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  applyProps(node, props);
  appendChildren(node, children);
  return node;
}

const SVG_NS = "http://www.w3.org/2000/svg";

/** Build an SVG element, the same way. */
export function s(tag, props = {}, ...children) {
  const node = document.createElementNS(SVG_NS, tag);
  applyProps(node, props);
  appendChildren(node, children);
  return node;
}

/** An icon from the sprite in index.html. */
export function icon(name, cls = "icon") {
  return s("svg", { class: cls, "aria-hidden": "true" }, s("use", { href: `#i-${name}` }));
}

/**
 * Animate a number in place. The first value is written directly; later ones count
 * from the previous value, so a poll that changes a figure is visible as a change.
 */
export function tween(node, value, format, ms = 650) {
  const to = toNum(value);
  const from = node._tweenValue;
  node._tweenValue = to;
  node.classList.remove("skeleton");
  cancelAnimationFrame(node._tweenFrame);
  if (to === null || from === null || from === undefined || from === to || reducedMotion.matches) {
    node.textContent = format(to);
    return;
  }
  const start = performance.now();
  const step = (now) => {
    const t = Math.min(1, (now - start) / ms);
    const eased = 1 - (1 - t) ** 3;
    node.textContent = format(from + (to - from) * eased);
    if (t < 1) node._tweenFrame = requestAnimationFrame(step);
  };
  node._tweenFrame = requestAnimationFrame(step);
}

export function groupBy(items, key) {
  const out = new Map();
  for (const item of items) {
    const k = typeof key === "function" ? key(item) : item[key];
    if (!out.has(k)) out.set(k, []);
    out.get(k).push(item);
  }
  return out;
}

export const store = {
  get(key) {
    try { return localStorage.getItem(key); } catch { return null; }
  },
  set(key, value) {
    try { localStorage.setItem(key, value); } catch { /* storage blocked: not remembered */ }
  },
};
