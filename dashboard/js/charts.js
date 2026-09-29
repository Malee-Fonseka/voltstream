// Hand-drawn charts: SVG for plots, HTML for bar lists. No library, so nothing is fetched
// from a CDN and every mark follows the theme's CSS variables directly — a theme switch
// repaints without a redraw.
//
// The rules they follow: 2px lines, a 10% area wash, bars at most 24px thick with a 4px
// rounded data end and a square baseline, a 2px surface gap between touching marks,
// hairline grid, a legend whenever there are two series, values on hover *and* in a table
// view (the card's table toggle), and text in ink colours, never the series colour.

import { h, s, num, hhmm, shortDay, reducedMotion } from "./util.js";

// ----------------------------------------------------------------- tooltip

let tipNode = null;

function tip() {
  if (!tipNode) {
    tipNode = h("div", { class: "tooltip", role: "tooltip" });
    document.body.append(tipNode);
  }
  return tipNode;
}

/** rows: [{ color, label, value, box }] — the value leads, the label follows. */
export function showTip(x, y, title, rows) {
  const node = tip();
  node.replaceChildren(
    title ? h("div", { class: "tt-title" }, title) : null,
    ...rows.map((r) =>
      h(
        "div",
        { class: "tt-row" },
        r.color ? h("span", { class: `tt-key${r.box ? " box" : ""}`, style: `--k:${r.color}` }) : null,
        h("span", { class: "tt-value" }, r.value),
        r.label ? h("span", { class: "tt-label" }, r.label) : null,
      ),
    ),
  );
  node.classList.add("show");
  const pad = 14;
  const { width, height } = node.getBoundingClientRect();
  let left = x + pad;
  let top = y + pad;
  if (left + width > window.innerWidth - 8) left = x - width - pad;
  if (top + height > window.innerHeight - 8) top = y - height - pad;
  node.style.left = `${Math.max(8, left)}px`;
  node.style.top = `${Math.max(8, top)}px`;
}

export function hideTip() {
  tipNode?.classList.remove("show");
}

/** Hover and keyboard focus show the same readout. */
export function attachTip(node, content) {
  const show = (e) => {
    const c = content();
    const rect = node.getBoundingClientRect();
    showTip(e?.clientX ?? rect.left + rect.width / 2, e?.clientY ?? rect.top, c.title, c.rows);
  };
  node.addEventListener("pointermove", show);
  node.addEventListener("pointerleave", hideTip);
  node.addEventListener("focus", () => show());
  node.addEventListener("blur", hideTip);
}

// ----------------------------------------------------------------- helpers

export function legend(node, items, { box = false } = {}) {
  node.replaceChildren(
    ...items.map((it) =>
      h(
        "span",
        { class: "legend-item" },
        h("span", { class: `legend-key${box ? " box" : ""}`, style: `--k:${it.color}` }),
        it.label,
      ),
    ),
  );
}

/** columns: [{ label, num }]; rows: arrays of strings or nodes. */
export function table(node, columns, rows) {
  node.replaceChildren(
    h(
      "div",
      { class: "table-scroll" },
      h(
        "table",
        { class: "table" },
        h("thead", {}, h("tr", {}, ...columns.map((c) => h("th", { class: c.num ? "num" : null }, c.label)))),
        h(
          "tbody",
          {},
          ...rows.map((r) => h("tr", {}, ...r.map((cell, i) => h("td", { class: columns[i]?.num ? "num" : null }, cell)))),
        ),
      ),
    ),
  );
}

function niceStep(raw) {
  const power = 10 ** Math.floor(Math.log10(raw));
  const f = raw / power;
  return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10) * power;
}

/** One precision for a whole axis, from its step: 0, 10, 20 — never 0.00, 10.0, 20.0. */
function decimalsFor(ticks) {
  const step = ticks.length > 1 ? Math.abs(ticks[1] - ticks[0]) : 1;
  return step >= 1 ? 0 : step >= 0.1 ? 1 : 2;
}

/** Clean round ticks covering [lo, hi]. */
export function niceTicks(lo, hi, count = 4) {
  if (!(hi > lo)) hi = lo + 1;
  const step = niceStep((hi - lo) / count);
  const start = Math.floor(lo / step + 1e-9) * step;
  const end = Math.ceil(hi / step - 1e-9) * step;
  const ticks = [];
  for (let v = start; v <= end + step / 2; v += step) ticks.push(Math.round(v / step) * step);
  return { lo: start, hi: end, ticks };
}

// Charts redraw when their box changes size (a sidebar collapsing, a view becoming
// visible). One observer for all of them.
const resizer = new ResizeObserver((entries) => {
  for (const entry of entries) {
    const node = entry.target;
    const width = Math.round(entry.contentRect.width);
    if (width && width !== node._width) {
      node._width = width;
      node._draw?.(false);
    }
  }
});

function mount(node, draw) {
  const first = !node._draw;
  node._draw = draw;
  if (first) resizer.observe(node);
  node._width = Math.round(node.clientWidth);
  draw(first);
}

const widthOf = (node, fallback) => Math.max(200, node.clientWidth || fallback);

// ------------------------------------------------------------- time series

/**
 * A line/area chart over simulated time, with a crosshair that snaps to the nearest
 * window and lists every series there.
 *
 * series: [{ label, color, area, points: [{ t (ms), v }] }]
 */
export function timeSeries(node, config) {
  mount(node, (first) => drawTimeSeries(node, config, first));
}

function drawTimeSeries(node, cfg, animate) {
  const { series, height = 270, unit = "", windowMinutes = 15, empty = "No data yet" } = cfg;
  const format = cfg.format ?? ((v) => num(v, 1));
  const width = widthOf(node, 640);
  const all = series.flatMap((sr) => sr.points);
  node.replaceChildren();
  if (!all.length) {
    node.append(h("div", { class: "chart-empty", style: `height:${height}px` }, empty));
    return;
  }

  const times = [...new Set(all.map((p) => p.t))].sort((a, b) => a - b);
  const t0 = times[0];
  const t1 = times.length > 1 ? times.at(-1) : t0 + windowMinutes * 60e3;
  const { hi, ticks } = niceTicks(0, Math.max(...all.map((p) => p.v)) || 1, 4);
  const tickText = ticks.map((v) => num(v, decimalsFor(ticks)));
  const M = {
    top: 12,
    right: 62,
    bottom: 28,
    left: Math.max(...tickText.map((t) => t.length)) * 6.8 + 16,
  };
  const iw = width - M.left - M.right;
  const ih = height - M.top - M.bottom;
  const X = (t) => M.left + ((t - t0) / (t1 - t0)) * iw;
  const Y = (v) => M.top + ih - (v / hi) * ih;

  const root = s("svg", {
    class: "chart",
    width,
    height,
    viewBox: `0 0 ${width} ${height}`,
    role: "img",
    tabindex: 0,
    "aria-label": cfg.label ?? "Chart",
  });

  ticks.forEach((v, i) => {
    root.append(s("line", { class: i === 0 ? "baseline" : "grid", x1: M.left, x2: width - M.right, y1: Y(v), y2: Y(v) }));
    root.append(s("text", { class: "tick", x: M.left - 8, y: Y(v) + 4, "text-anchor": "end" }, tickText[i]));
  });

  // Time ticks on round simulated hours; midnight is labelled with its date.
  const HOUR = 3.6e6;
  const maxTicks = Math.max(2, Math.floor(iw / 78));
  const stepHours = [0.25, 0.5, 1, 2, 3, 4, 6, 12, 24].find((h) => (t1 - t0) / (h * HOUR) <= maxTicks) ?? 24;
  const step = stepHours * HOUR;
  for (let t = Math.ceil(t0 / step) * step; t <= t1; t += step) {
    const label = t % (24 * HOUR) === 0 ? shortDay(t) : hhmm(t);
    root.append(s("text", { class: "tick", x: X(t), y: height - 8, "text-anchor": "middle" }, label));
  }

  const drawn = [];
  for (const sr of series) {
    const pts = [...sr.points].sort((a, b) => a.t - b.t);
    if (!pts.length) continue;
    const d = pts.map((p, i) => `${i ? "L" : "M"}${X(p.t).toFixed(1)},${Y(p.v).toFixed(1)}`).join("");
    if (sr.area && pts.length > 1) {
      const base = Y(0).toFixed(1);
      root.append(
        s("path", {
          class: "area",
          d: `${d}L${X(pts.at(-1).t).toFixed(1)},${base}L${X(pts[0].t).toFixed(1)},${base}Z`,
          style: `fill:${sr.color}`,
        }),
      );
    }
    const line = s("path", { class: "line", d, style: `stroke:${sr.color}` });
    root.append(line);
    drawn.push({ sr, pts, line, byTime: new Map(pts.map((p) => [p.t, p.v])) });
  }

  // End dots, and a value at each line's end unless two would collide (the legend and
  // the tooltip still identify both).
  const labelled = [];
  for (const { sr, pts } of drawn) {
    const last = pts.at(-1);
    const y = Y(last.v);
    root.append(s("circle", { class: "end-dot", cx: X(last.t), cy: y, r: 4, style: `fill:${sr.color}` }));
    if (labelled.every((ly) => Math.abs(ly - y) >= 15)) {
      labelled.push(y);
      root.append(s("text", { class: "end-label", x: X(last.t) + 10, y: y + 4 }, format(last.v)));
    }
  }

  // Crosshair.
  const hover = s("g", { style: "display:none" });
  const cross = s("line", { class: "crosshair", y1: M.top, y2: M.top + ih });
  hover.append(cross);
  const dots = drawn.map(({ sr }) => {
    const dot = s("circle", { class: "hover-dot", r: 4.5, style: `fill:${sr.color}` });
    hover.append(dot);
    return dot;
  });
  root.append(hover);
  const hit = s("rect", { class: "hit", x: M.left, y: M.top, width: Math.max(0, iw), height: Math.max(0, ih) });
  root.append(hit);

  let index = -1;
  const suffix = unit ? ` ${unit}` : "";
  const showAt = (i, clientX, clientY) => {
    index = Math.max(0, Math.min(times.length - 1, i));
    const t = times[index];
    const x = X(t);
    hover.style.display = "";
    cross.setAttribute("x1", x);
    cross.setAttribute("x2", x);
    const rows = drawn.map(({ sr, byTime }, k) => {
      const v = byTime.get(t);
      dots[k].style.display = v === undefined ? "none" : "";
      if (v !== undefined) {
        dots[k].setAttribute("cx", x);
        dots[k].setAttribute("cy", Y(v));
      }
      return { color: sr.color, label: sr.label, value: v === undefined ? "—" : `${format(v)}${suffix}` };
    });
    const rect = root.getBoundingClientRect();
    const scale = rect.width / width || 1;
    showTip(
      clientX ?? rect.left + x * scale,
      clientY ?? rect.top + M.top * scale,
      `${hhmm(t)}–${hhmm(t + windowMinutes * 60e3)} · ${shortDay(t)}`,
      rows,
    );
  };
  const nearest = (clientX) => {
    const rect = root.getBoundingClientRect();
    const px = (clientX - rect.left) * (width / rect.width);
    const t = t0 + ((px - M.left) / iw) * (t1 - t0);
    let best = 0;
    for (let i = 1; i < times.length; i++) if (Math.abs(times[i] - t) < Math.abs(times[best] - t)) best = i;
    return best;
  };
  const hide = () => {
    hover.style.display = "none";
    index = -1;
    hideTip();
  };

  hit.addEventListener("pointermove", (e) => {
    node._pointer = { x: e.clientX, y: e.clientY };
    showAt(nearest(e.clientX), e.clientX, e.clientY);
  });
  hit.addEventListener("pointerleave", () => {
    node._pointer = null;
    hide();
  });
  root.addEventListener("keydown", (e) => {
    if (e.key === "ArrowLeft" || e.key === "ArrowRight") {
      e.preventDefault();
      const from = index < 0 ? times.length : index;
      showAt(from + (e.key === "ArrowRight" ? 1 : -1));
    } else if (e.key === "Escape") hide();
  });
  root.addEventListener("focus", () => {
    if (root.matches(":focus-visible")) showAt(times.length - 1);
  });
  root.addEventListener("blur", hide);

  node.append(root);

  // A poll redraws the chart; keep the readout under a pointer that has not moved.
  if (node._pointer) showAt(nearest(node._pointer.x), node._pointer.x, node._pointer.y);

  if (animate && !reducedMotion.matches) {
    for (const { line } of drawn) {
      const length = line.getTotalLength();
      line.style.strokeDasharray = `${length}`;
      line.style.strokeDashoffset = `${length}`;
      line.getBoundingClientRect();
      line.style.transition = "stroke-dashoffset 1.1s cubic-bezier(.2,.7,.2,1)";
      line.style.strokeDashoffset = "0";
      line.addEventListener("transitionend", () => {
        line.style.strokeDasharray = "";
        line.style.transition = "";
      }, { once: true });
    }
  }
}

// --------------------------------------------------------------- sparkline

/** A small trend with no axes. The tile beside it carries the numbers. */
export function sparkline(node, { series, height = 34 }) {
  mount(node, () => drawSparkline(node, series, height));
}

function drawSparkline(node, series, height) {
  const width = Math.max(60, node.clientWidth || 180);
  const all = series.flatMap((sr) => sr.points);
  node.replaceChildren();
  if (all.length < 2) return;
  const t0 = Math.min(...all.map((p) => p.t));
  const t1 = Math.max(...all.map((p) => p.t));
  const vmax = Math.max(...all.map((p) => p.v)) || 1;
  const pad = 3;
  const X = (t) => pad + ((t - t0) / (t1 - t0 || 1)) * (width - pad * 2);
  const Y = (v) => pad + (height - pad * 2) * (1 - v / vmax);
  const root = s("svg", { class: "spark", width, height, viewBox: `0 0 ${width} ${height}`, "aria-hidden": "true" });
  for (const sr of series) {
    const pts = [...sr.points].sort((a, b) => a.t - b.t);
    if (pts.length < 2) continue;
    const d = pts.map((p, i) => `${i ? "L" : "M"}${X(p.t).toFixed(1)},${Y(p.v).toFixed(1)}`).join("");
    if (sr.area) {
      root.append(s("path", { class: "area", d: `${d}L${X(pts.at(-1).t)},${height}L${X(pts[0].t)},${height}Z`, style: `fill:${sr.color}` }));
    }
    root.append(s("path", { class: "line", d, style: `stroke:${sr.color}` }));
    root.append(s("circle", { cx: X(pts.at(-1).t), cy: Y(pts.at(-1).v), r: 2.5, style: `fill:${sr.color}` }));
  }
  node.append(root);
}

// ---------------------------------------------------------------- bar list

/**
 * Horizontal bars, one row per category, one thin bar per series in each row.
 * rows: [{ key, label, values: { [seriesId]: number } }]
 */
export function barList(node, { rows, series, format = (v) => num(v, 1), max, highlight, onSelect, empty = "No data yet" }) {
  if (!rows.length) {
    node.replaceChildren(h("div", { class: "chart-empty", style: "height:160px" }, empty));
    return;
  }
  const top = max ?? (Math.max(0, ...rows.flatMap((r) => series.map((sr) => Math.abs(r.values[sr.id] ?? 0)))) || 1);
  // Reuse rows by key, so a poll animates bar widths instead of rebuilding them.
  const existing = new Map([...node.querySelectorAll(":scope > .hbars > .hbar-row")].map((el) => [el.dataset.key, el]));
  const list = node.querySelector(":scope > .hbars") ?? h("div", { class: "hbars" });
  const ordered = rows.map((r) => {
    let row = existing.get(r.key);
    if (!row) {
      row = h("div", { class: "hbar-row", "data-key": r.key });
      row.append(h("div", { class: "hbar-label" }), h("div", { class: "hbar-bars" }));
      for (const sr of series) {
        const bar = h("div", { class: "hbar", tabindex: 0, style: `--c:${sr.color}` });
        const value = h("span", { class: "hbar-value" });
        row.lastChild.append(h("div", { class: "hbar-line" }, bar, value));
        attachTip(bar, () => ({
          title: row._label,
          rows: [{ color: sr.color, box: true, label: sr.label, value: format(row._values[sr.id]) }],
        }));
      }
      if (onSelect) {
        row.classList.add("clickable");
        row.addEventListener("click", () => onSelect(r.key));
      }
    }
    row._label = r.label;
    row._values = r.values;
    row.firstChild.textContent = r.label;
    row.classList.toggle("dim", Boolean(highlight) && highlight !== r.key);
    series.forEach((sr, i) => {
      const line = row.lastChild.children[i];
      const v = r.values[sr.id];
      line.firstChild.style.setProperty("--w", Math.max(0, Math.abs(v ?? 0) / top).toFixed(4));
      line.lastChild.textContent = format(v);
    });
    return row;
  });
  list.replaceChildren(...ordered);
  if (!list.isConnected || list.parentNode !== node) node.replaceChildren(list);
}

// --------------------------------------------------------------- waterfall

/**
 * How a total is built: each step floats from the running total, the total stands on the
 * baseline. steps: [{ label, short, value } | { label, short, total: true }]
 */
export function waterfall(node, config) {
  mount(node, (first) => drawWaterfall(node, config, first));
}

function columnPath(x, yFrom, yTo, w, r) {
  // A column from yFrom to yTo, rounded only at yTo — the data end.
  const top = Math.min(yFrom, yTo);
  const bottom = Math.max(yFrom, yTo);
  const rr = Math.min(r, (bottom - top) / 2, w / 2);
  if (yTo <= yFrom) {
    return `M${x},${bottom}V${top + rr}Q${x},${top} ${x + rr},${top}H${x + w - rr}Q${x + w},${top} ${x + w},${top + rr}V${bottom}Z`;
  }
  return `M${x},${top}V${bottom - rr}Q${x},${bottom} ${x + rr},${bottom}H${x + w - rr}Q${x + w},${bottom} ${x + w},${bottom - rr}V${top}Z`;
}

function drawWaterfall(node, cfg, animate) {
  const { steps, height = 260, stepColor = "var(--c-step)", totalColor = "var(--accent)" } = cfg;
  const format = cfg.format ?? ((v) => num(v, 2));
  const width = widthOf(node, 520);
  node.replaceChildren();

  let running = 0;
  const bars = steps.map((st) => {
    if (st.total) return { ...st, from: 0, to: running };
    const from = running;
    running += st.value;
    return { ...st, from, to: running };
  });
  const lo = Math.min(0, ...bars.map((b) => Math.min(b.from, b.to)));
  const hi = Math.max(0, ...bars.map((b) => Math.max(b.from, b.to)));
  const domain = niceTicks(lo, hi || 1, 4);
  const tickText = domain.ticks.map((v) => num(v, decimalsFor(domain.ticks)));
  const M = { top: 22, right: 8, bottom: 34, left: Math.max(...tickText.map((t) => t.length)) * 6.8 + 16 };
  const iw = width - M.left - M.right;
  const ih = height - M.top - M.bottom;
  const Y = (v) => M.top + ih - ((v - domain.lo) / (domain.hi - domain.lo)) * ih;
  const band = iw / bars.length;
  const bw = Math.min(24, band * 0.5);

  const root = s("svg", {
    class: `chart${animate && !reducedMotion.matches ? " anim" : ""}`,
    width,
    height,
    viewBox: `0 0 ${width} ${height}`,
    role: "img",
    "aria-label": cfg.label ?? "Waterfall chart",
  });
  domain.ticks.forEach((v, i) => {
    root.append(s("line", { class: v === 0 ? "baseline" : "grid", x1: M.left, x2: width - M.right, y1: Y(v), y2: Y(v) }));
    root.append(s("text", { class: "tick", x: M.left - 8, y: Y(v) + 4, "text-anchor": "end" }, tickText[i]));
  });

  bars.forEach((b, i) => {
    const x = M.left + band * i + (band - bw) / 2;
    let yFrom = Y(b.from);
    let yTo = Y(b.to);
    // A zero step still gets a hairline, so "no subsidy" reads as zero rather than missing.
    if (Math.abs(yTo - yFrom) < 1.5) yTo = yFrom - 1.5 * (b.value < 0 ? -1 : 1);
    const color = b.total ? totalColor : stepColor;
    const bar = s("path", {
      class: "wf-bar",
      d: columnPath(x, yFrom, yTo, bw, 4),
      style: `fill:${color}; --i:${i}; transform-origin: 50% ${yTo < yFrom ? "100%" : "0%"}`,
      tabindex: 0,
    });
    // Signed steps, except a zero step, which is neither added nor taken away.
    const text = b.total || b.value === 0 ? format(b.total ? b.to : 0) : `${b.value < 0 ? "−" : "+"}${format(Math.abs(b.value))}`;
    attachTip(bar, () => ({ title: b.label, rows: [{ color, box: true, value: text }] }));
    root.append(bar);

    if (i < bars.length - 1) {
      const nx = M.left + band * (i + 1) + (band - bw) / 2;
      root.append(s("line", { class: "wf-link", x1: x + bw, x2: nx, y1: Y(b.to), y2: Y(b.to) }));
    }

    const up = b.total ? b.to >= 0 : b.value >= 0;
    const labelY = up ? Math.min(yFrom, yTo) - 7 : Math.max(yFrom, yTo) + 15;
    root.append(s("text", { class: `wf-value${b.total ? " total" : ""}`, x: x + bw / 2, y: labelY, "text-anchor": "middle" }, text));
    const name = band < 84 && b.short ? b.short : b.label;
    root.append(s("text", { class: "wf-label", x: x + bw / 2, y: height - 10, "text-anchor": "middle" }, name));
  });

  node.append(root);
}

// -------------------------------------------------------------------- ring

/** A progress ring: the fill carries the value, the track is the same hue, lighter. */
export function ring(node, ratio, { color = "var(--c-solar)", size = 48, stroke = 5, label = "" } = {}) {
  const r = (size - stroke) / 2;
  const circumference = 2 * Math.PI * r;
  if (!node._ring) {
    const svgEl = s("svg", { class: "ring", width: size, height: size, viewBox: `0 0 ${size} ${size}`, "aria-hidden": "true" });
    const track = s("circle", { class: "track", cx: size / 2, cy: size / 2, r, "stroke-width": stroke, style: `stroke: color-mix(in srgb, ${color} 20%, transparent)` });
    const arc = s("circle", {
      class: "arc",
      cx: size / 2,
      cy: size / 2,
      r,
      "stroke-width": stroke,
      transform: `rotate(-90 ${size / 2} ${size / 2})`,
      style: `stroke:${color}; stroke-dasharray:${circumference}; stroke-dashoffset:${circumference}`,
    });
    const text = s("text", { x: size / 2, y: size / 2 + 4, "text-anchor": "middle" });
    svgEl.append(track, arc, text);
    node.replaceChildren(svgEl);
    node._ring = { arc, text };
    arc.getBoundingClientRect();
  }
  const v = Math.max(0, Math.min(1, Number(ratio) || 0));
  node._ring.arc.style.strokeDashoffset = `${circumference * (1 - v)}`;
  node._ring.text.textContent = label;
}

// --------------------------------------------------------------- stack bar

/** One bar split into parts, with a 2px gap between them. parts: [{ label, value, color }] */
export function stackBar(node, parts, { format, title }) {
  const total = parts.reduce((sum, p) => sum + Math.max(0, p.value), 0);
  if (!(total > 0)) {
    node.replaceChildren(h("div", { class: "stack empty" }));
    return;
  }
  const bar = h("div", { class: "stack", role: "img", "aria-label": title });
  for (const p of parts) {
    if (!(p.value > 0)) continue;
    const seg = h("span", { tabindex: 0, style: `--c:${p.color}; flex:${p.value} 1 0` });
    attachTip(seg, () => ({
      title,
      rows: [{ color: p.color, box: true, label: p.label, value: `${format(p.value)} · ${num((p.value / total) * 100, 0)}%` }],
    }));
    bar.append(seg);
  }
  node.replaceChildren(bar);
}
