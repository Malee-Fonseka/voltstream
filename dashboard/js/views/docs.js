// How it works: the pipeline drawn as an animated diagram, with a detail panel per stage
// and a guided tour that follows one reading from the meter to the bill.
//
// The diagram is data (NODES, EDGES) rendered to SVG once. On first view the edges draw
// themselves and the stages rise in, in the order data flows through them; afterwards,
// packets travel the two paths — amber and frequent on the speed path, teal and once a
// "day" on the batch path — so the difference in latency is visible without reading.
// All of it is skipped for viewers who ask for reduced motion.

import { h, icon, reducedMotion, s } from "../util.js";

const W = 1240;
const H = 510;
const NODE_W = 160;
const NODE_H = 64;

const LANE_COLOR = {
  source: "var(--text-2)",
  shared: "var(--accent)",
  speed: "var(--c-speed)",
  batch: "var(--c-batch)",
  serve: "var(--accent)",
};
const LANE_NAME = {
  source: "Source",
  shared: "Shared log",
  speed: "Speed layer",
  batch: "Batch layer",
  serve: "Serving",
};

const NODES = [
  {
    id: "sim", x: 20, y: 208, lane: "source", glyph: "home", delay: 0,
    title: "Simulators", sub: "50 meters · 2 s tick",
    lead: "A producer emits one reading per household every 2 real seconds: 9.6 simulated minutes of consumption and rooftop solar, shaped by the time of day.",
    facts: [
      "Published to Kafka keyed by `household_id`, so each household's readings stay in order",
      "Every event carries an `event_id` and a `trace_id` that follow it through the pipeline",
      "Faults injected on purpose: duplicates, late readings, bad values, meter dropouts",
    ],
    why: "Without the faults the two layers would always agree, and there would be nothing to reconcile.",
  },
  {
    id: "kafka", x: 220, y: 208, lane: "shared", glyph: "flow", delay: 0.3,
    title: "Kafka", sub: "meter.readings",
    lead: "The durable buffer between producers and consumers. Two consumer groups read the same topic independently: the speed layer and the raw archiver.",
    facts: [
      "Topic `meter.readings`: 3 partitions, 7 days' retention",
      "A slow consumer never slows the other one, or the producer",
      "Recent history can be replayed from Kafka alone",
    ],
    why: "Decoupling is what lets one path be fast and the other slow, from the same input.",
  },
  {
    id: "speed", x: 430, y: 70, lane: "speed", glyph: "bolt", delay: 0.7,
    title: "Speed layer", sub: "Spark Streaming",
    lead: "Spark Structured Streaming validates, deduplicates and aggregates the live stream, one micro-batch every 10 real seconds.",
    facts: [
      "15-simulated-minute windows per zone → `zone_metrics_rt`",
      "A running bill per household → `household_running_rt`, priced with yesterday's tariff",
      "30-simulated-minute watermark: readings later than that are dropped",
      "Invalid readings go to `rejected_records`, with their reason",
    ],
    why: "It trades completeness for latency — exactly what the batch layer corrects later.",
  },
  {
    id: "archiver", x: 430, y: 346, lane: "batch", glyph: "report", delay: 0.7,
    title: "Raw archiver", sub: "append-only",
    lead: "A second consumer copies every reading, untouched, into the master dataset.",
    facts: [
      "No transformation and no validation: raw means raw",
      "Parquet, partitioned by `sim_date` and `hour`",
      "Restarting it never duplicates or loses a file",
    ],
    why: "The batch layer can only be authoritative if its input is complete and immutable.",
  },
  {
    id: "dropper", x: 640, y: 208, lane: "source", glyph: "tag", delay: 1.05,
    title: "Tariff dropper", sub: "one file per day",
    lead: "At each simulated midnight it drops the tariff and weather files for the day that just closed into the landing bucket.",
    facts: [
      "`tariff_YYYY-MM-DD.csv`: block rates, fixed charge, subsidy and export rate per household",
      "Its arrival is what tells Airflow a day can be billed",
      "Until it lands, the speed layer only has yesterday's tariff",
    ],
    why: "The day's own price exists only once the day is over. That is the tariff effect.",
  },
  {
    id: "minio", x: 640, y: 346, lane: "batch", glyph: "layers", delay: 1.05,
    title: "MinIO", sub: "master dataset",
    lead: "Every raw reading, immutable, plus the landing zone for the daily reference files.",
    facts: [
      "`voltstream-raw` holds readings; `voltstream-landing` receives tariff and weather drops",
      "Processed reference files are archived, never edited",
      "Any day can be recomputed from here — that is how a restatement works",
    ],
    why: "Immutability is what makes the batch answer reproducible.",
  },
  {
    id: "batch", x: 850, y: 346, lane: "batch", glyph: "clock", delay: 1.4,
    title: "Batch layer", sub: "Airflow · Spark",
    lead: "Airflow waits for the day's tariff file, allows 90 real seconds for late data, then runs the Spark billing job over the whole day.",
    facts: [
      "Full-day rescan → dedup → validate → join tariff → net solar → block tariff",
      "Writes `household_bill_daily` and `zone_metrics_daily`, then marks the day `success` in `pipeline_runs`",
      "Reconciliation then explains the speed-vs-batch gap, household by household",
    ],
    why: "It saw every late reading and used the right tariff, so its answer always wins.",
  },
  {
    id: "pg", x: 850, y: 208, lane: "serve", glyph: "table", delay: 1.55,
    title: "PostgreSQL", sub: "serving tables",
    lead: "The serving layer. The two layers write to separate tables side by side, and never overwrite each other.",
    facts: [
      "Speed: `zone_metrics_rt`, `household_running_rt`",
      "Batch: `household_bill_daily`, `zone_metrics_daily`, `pipeline_runs`",
      "Every row carries its provenance: layer, tariff date, run id",
    ],
    why: "Separate tables keep provenance unambiguous: you can always tell which layer said what.",
  },
  {
    id: "api", x: 1060, y: 208, lane: "serve", glyph: "code", delay: 1.85,
    title: "FastAPI", sub: "merge function",
    lead: "The API reads Postgres and nothing else, and runs the merge function on every bill request.",
    facts: [
      "The batch row if the day is finalised, otherwise the speed estimate",
      "Every response says which, in `source` and `provisional`",
      "No arithmetic: each figure comes from the layer that computed it",
    ],
    why: "The architecture becomes a product feature: the same URL gives a better answer over time.",
  },
  {
    id: "dash", x: 1060, y: 70, lane: "serve", glyph: "live", delay: 2.15,
    title: "Dashboard", sub: "polls every 5 s",
    lead: "This page. It asks the API every 5 seconds; nothing is pushed to it.",
    facts: [
      "The badge shows which layer answered",
      "Closing the tab changes nothing upstream",
      "Served by the API itself, so there is no CORS",
    ],
    why: "One household's badge flipping from Provisional to Final is Lambda in a single screenshot.",
  },
  {
    id: "report", x: 1060, y: 346, lane: "serve", glyph: "bill", delay: 2.15,
    title: "Daily report", sub: "/reports/daily",
    lead: "The consolidated report for one day: zone totals, the billing summary, rejected records and every billing run.",
    facts: [
      "`GET /api/v1/reports/daily?date=…`",
      "Returned for open days too, labelled partial",
      "The billing DAG also archives a report file after each run",
    ],
    why: "An absent figure is never mistaken for zero: the report says which sections exist yet.",
  },
];

const OBS = {
  id: "obs", lane: "shared", delay: 2.35,
  title: "Observability",
  lead: "Every stage is observable. Prometheus scrapes metrics from each service, Alertmanager routes five alert rules, and Grafana shows both.",
  facts: [
    "Structured JSON logs carry each reading's `trace_id` end to end",
    "Grafana: grid operations, pipeline health, Lambda divergence",
    "Alerts: stale data, low renewables, reject rate, batch SLA, divergence",
  ],
  why: "Monitoring observes the system and never takes it down: the API stays up when Alertmanager is not.",
};

const EDGES = [
  { id: "sim-kafka", from: "sim", to: "kafka", lane: "shared", d: "M180,240 H220", delay: 0.15 },
  { id: "kafka-speed", from: "kafka", to: "speed", lane: "speed", d: "M300,208 V122 Q300,102 320,102 H430", delay: 0.45, label: ["speed consumer", 375, 93] },
  { id: "kafka-arch", from: "kafka", to: "archiver", lane: "batch", d: "M300,272 V358 Q300,378 320,378 H430", delay: 0.45, label: ["archive consumer", 375, 369] },
  { id: "speed-pg", from: "speed", to: "pg", lane: "speed", d: "M590,102 H910 Q930,102 930,122 V208", delay: 0.9, label: ["upsert every trigger", 760, 93] },
  { id: "arch-minio", from: "archiver", to: "minio", lane: "batch", d: "M590,378 H640", delay: 0.9 },
  { id: "drop-minio", from: "dropper", to: "minio", lane: "source", d: "M720,272 V346", delay: 1.2, label: ["tariff CSV", 728, 313, "start"] },
  { id: "minio-batch", from: "minio", to: "batch", lane: "batch", d: "M800,378 H850", delay: 1.25 },
  { id: "batch-pg", from: "batch", to: "pg", lane: "batch", d: "M930,346 V272", delay: 1.55, label: ["final bills", 938, 313, "start"] },
  { id: "pg-api", from: "pg", to: "api", lane: "serve", d: "M1010,240 H1060", delay: 1.7 },
  { id: "api-dash", from: "api", to: "dash", lane: "serve", d: "M1140,208 V134", delay: 2.0, label: ["poll · 5 s", 1148, 175, "start"] },
  { id: "api-report", from: "api", to: "report", lane: "serve", d: "M1140,272 V346", delay: 2.0 },
];

// Packet routes run through node centres; the nodes are drawn above them, so a packet
// appears to enter a stage and leave it again.
const ROUTE = {
  shared: "M100,240 H300",
  speed: "M300,240 V122 Q300,102 320,102 H910 Q930,102 930,122 V240 H1140 V102",
  archive: "M300,240 V358 Q300,378 320,378 H720",
  tariff: "M720,240 V378",
  close: "M720,378 H930 V240 H1140 V102",
};

// {route, duration s, period s, delay s, cls, r}. Readings leave every 1.2 s; a "day"
// closes every 9 s here — in the running system it is every 5 real minutes.
const STREAMS = [
  { route: "shared", duration: 0.9, period: 1.2, delay: 0, cls: "ref", r: 3.5 },
  { route: "speed", duration: 3.3, period: 1.2, delay: 0.9, cls: "speed", r: 4 },
  { route: "archive", duration: 2.1, period: 1.2, delay: 0.9, cls: "batch", r: 3.2 },
  { route: "tariff", duration: 1.1, period: 9, delay: 2, cls: "ref", r: 4.5 },
  { route: "close", duration: 3, period: 9, delay: 3.2, cls: "batch", r: 6, glow: true },
  { route: "close", duration: 3, period: 9, delay: 3.38, cls: "batch", r: 4.5 },
  { route: "close", duration: 3, period: 9, delay: 3.56, cls: "batch", r: 3.5 },
];

const TOUR = [
  {
    nodes: ["sim"], edges: [], route: null,
    title: "A meter takes a reading",
    text: "HH-0042 reports 0.41 kWh used and 0.18 kWh of solar for the last 9.6 simulated minutes, stamped with its simulated time and a trace id.",
  },
  {
    nodes: ["sim", "kafka"], edges: ["sim-kafka"], route: "M180,240 H300",
    title: "Kafka buffers it",
    text: "The reading lands on `meter.readings`, on its household's partition. From here two consumers read it, independently.",
  },
  {
    nodes: ["kafka", "speed"], edges: ["kafka-speed"], route: "M300,240 V122 Q300,102 320,102 H510",
    title: "The speed layer counts it, fast",
    text: "Within one trigger — 10 real seconds — Spark adds it to its zone's 15-minute window and to the household's running bill, priced with yesterday's tariff.",
  },
  {
    nodes: ["speed", "pg"], edges: ["speed-pg"], route: "M510,102 H910 Q930,102 930,122 V240",
    title: "…and publishes an estimate",
    text: "The window and the running bill are upserted into Postgres. The Live grid page shows them seconds after the reading was taken.",
  },
  {
    nodes: ["kafka", "archiver", "minio"], edges: ["kafka-arch", "arch-minio"], route: "M300,240 V358 Q300,378 320,378 H720",
    title: "Meanwhile, the raw copy is kept",
    text: "The archiver writes the reading, untouched, to Parquet in MinIO: the immutable master dataset the batch layer will trust.",
  },
  {
    nodes: ["dropper", "minio"], edges: ["drop-minio"], route: "M720,240 V378",
    title: "Midnight: the day closes",
    text: "At simulated midnight the tariff file for the day that just ended lands in MinIO. Only now does the day's real price exist.",
  },
  {
    nodes: ["minio", "batch"], edges: ["minio-batch"], route: "M720,378 H930",
    title: "The batch layer recomputes the day",
    text: "Airflow sees the tariff, waits for stragglers, and Spark rescans the whole day: dedup, validate, join the tariff, net the solar, price the blocks.",
  },
  {
    nodes: ["batch", "pg"], edges: ["batch-pg"], route: "M930,378 V240",
    title: "The final bill is written",
    text: "`household_bill_daily` gets the authoritative bill and `pipeline_runs` marks the day `success`. The speed estimate stays beside it, untouched.",
  },
  {
    nodes: ["pg", "api", "dash"], edges: ["pg-api", "api-dash"], route: "M930,240 H1140 V102",
    title: "The merge flips the badge",
    text: "Next time the dashboard asks, the API finds the day finalised and serves the batch row. The badge flips from Provisional to Final, and reconciliation explains the difference.",
  },
];

let svgRoot = null;
let routes = {};
let packetLayer = null;
let pool = [];
let frame = 0;
let t0 = 0;
let introDone = false;
let active = false;
let selected = null;
let tour = -1;
const $ = (id) => document.getElementById(id);

/** Backticks mark code in the prose above; build it as text nodes, never markup. */
function rich(text) {
  return text.split("`").map((part, i) => (i % 2 ? h("code", {}, part) : part));
}

// ------------------------------------------------------------------- build

function buildNode(n) {
  const g = s("g", {
    class: "pl-node",
    "data-id": n.id,
    transform: `translate(${n.x} ${n.y})`,
    tabindex: 0,
    role: "button",
    "aria-label": `${n.title}: ${n.sub}`,
    style: `--k:${LANE_COLOR[n.lane]}; --d:${n.delay}s`,
  });
  const inner = s("g", { class: "pl-node-in" });
  inner.append(
    s("rect", { class: "box", width: NODE_W, height: NODE_H, rx: 12 }),
    s("rect", { class: "accent", x: 0, y: 14, width: 3, height: NODE_H - 28, rx: 1.5 }),
    s("use", { class: "glyph", href: `#i-${n.glyph}`, x: 14, y: 14, width: 18, height: 18 }),
    s("text", { class: "t", x: 40, y: 28 }, n.title),
    s("text", { class: "s", x: 40, y: 46 }, n.sub),
  );
  g.append(inner);
  return g;
}

function build() {
  // A group, not an image: the stages inside are buttons and must stay reachable.
  const root = s("svg", {
    class: "pipeline",
    viewBox: `0 0 ${W} ${H}`,
    role: "group",
    "aria-label": "The voltstream pipeline: simulators to Kafka, then a speed path and a batch path into PostgreSQL, merged by the API for the dashboard.",
  });

  root.append(
    s("rect", { class: "pl-lane speed", x: 410, y: 48, width: 580, height: 108, rx: 16, style: "--d:.6s" }),
    s("text", { class: "pl-lane-label speed", x: 426, y: 40, style: "--d:.6s" }, "SPEED LAYER · SECONDS, APPROXIMATE"),
    s("rect", { class: "pl-lane batch", x: 410, y: 326, width: 620, height: 104, rx: 16, style: "--d:.6s" }),
    s("text", { class: "pl-lane-label batch", x: 426, y: 450, style: "--d:.6s" }, "BATCH LAYER · ONCE PER DAY, AUTHORITATIVE"),
  );

  const edgeLayer = s("g");
  for (const e of EDGES) {
    const path = s("path", { class: `pl-edge ${e.lane}`, d: e.d, pathLength: 1, "data-id": e.id, style: `--d:${e.delay}s` });
    edgeLayer.append(path);
    if (e.label) {
      const [text, x, y, anchor = "middle"] = e.label;
      edgeLayer.append(s("text", { class: "pl-edge-label", x, y, "text-anchor": anchor, "data-edge": e.id, style: `--d:${e.delay}s` }, text));
    }
  }
  root.append(edgeLayer);

  const routeLayer = s("g", { "aria-hidden": "true" });
  for (const [name, d] of Object.entries(ROUTE)) {
    const p = s("path", { d, fill: "none", stroke: "none" });
    routeLayer.append(p);
    routes[name] = p;
  }
  root.append(routeLayer);

  packetLayer = s("g", { "aria-hidden": "true" });
  root.append(packetLayer);

  const nodeLayer = s("g");
  for (const n of NODES) nodeLayer.append(buildNode(n));
  const obs = s("g", { class: "pl-node pl-obs", "data-id": "obs", tabindex: 0, role: "button", "aria-label": "Observability", style: `--d:${OBS.delay}s` });
  const obsIn = s("g", { class: "pl-node-in" });
  obsIn.append(
    s("rect", { class: "box", x: 20, y: 466, width: W - 40, height: 36, rx: 10 }),
    s("text", { x: W / 2, y: 489, "text-anchor": "middle" },
      s("tspan", { class: "t2" }, "Observability"),
      " — Prometheus metrics from every stage · 5 alert rules · Grafana dashboards · JSON logs with trace_id",
    ),
  );
  obs.append(obsIn);
  nodeLayer.append(obs);
  root.append(nodeLayer);

  $("pipeline").replaceChildren(root);
  svgRoot = root;
  svgRoot._edgeLayer = edgeLayer;

  root.addEventListener("click", (ev) => {
    const node = ev.target.closest(".pl-node");
    if (node) selectNode(node.dataset.id);
  });
  root.addEventListener("keydown", (ev) => {
    const node = ev.target.closest?.(".pl-node");
    if (node && (ev.key === "Enter" || ev.key === " ")) {
      ev.preventDefault();
      selectNode(node.dataset.id);
    }
  });
}

/**
 * Arrowheads, pointed along each edge's last segment. Deferred to the first time the
 * view is shown: path geometry is not reliably measurable inside a hidden section.
 */
function addArrows() {
  if (svgRoot._arrows) return;
  svgRoot._arrows = true;
  const edgeLayer = svgRoot._edgeLayer;
  for (const e of EDGES) {
    const path = svgRoot.querySelector(`.pl-edge[data-id="${e.id}"]`);
    const len = path.getTotalLength();
    const tip = path.getPointAtLength(len);
    const back = path.getPointAtLength(Math.max(0, len - 0.02));
    const angle = Math.atan2(tip.y - back.y, tip.x - back.x);
    const at = (dist, off) => {
      const x = tip.x - Math.cos(angle) * dist - Math.sin(angle) * off;
      const y = tip.y - Math.sin(angle) * dist + Math.cos(angle) * off;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    };
    edgeLayer.append(s("polygon", { class: `pl-arrow ${e.lane}`, points: `${at(0, 0)} ${at(8, -4.5)} ${at(8, 4.5)}`, "data-edge": e.id, style: `--d:${e.delay}s` }));
  }
}

// --------------------------------------------------------------- animation

function playIntro() {
  if (reducedMotion.matches) {
    introDone = true;
    return;
  }
  introDone = false;
  svgRoot.classList.remove("pl-go");
  svgRoot.classList.add("pl-intro");
  void svgRoot.getBoundingClientRect();
  svgRoot.classList.add("pl-go");
  clearTimeout(playIntro.timer);
  playIntro.timer = setTimeout(() => {
    svgRoot.classList.remove("pl-intro", "pl-go");
    introDone = true;
    t0 = performance.now();
  }, 2900);
}

function packet(i) {
  if (!pool[i]) {
    const glow = s("circle", { class: "pl-pkt pl-glow" });
    const dot = s("circle", { class: "pl-pkt" });
    packetLayer.append(glow, dot);
    pool[i] = { glow, dot };
  }
  return pool[i];
}

function draw(now) {
  frame = requestAnimationFrame(draw);
  if (!introDone) return;
  const t = (now - t0) / 1000;
  const streams = [...STREAMS];
  if (tour >= 0 && TOUR[tour].route) streams.push({ path: focusPath, duration: 1.6, period: 2.2, delay: 0, cls: "focus", r: 6, glow: true });
  let used = 0;
  for (const st of streams) {
    const path = st.path ?? routes[st.route];
    const len = path.getTotalLength();
    const first = Math.max(0, Math.floor((t - st.delay - st.duration) / st.period) + 1);
    const last = Math.floor((t - st.delay) / st.period);
    for (let j = first; j <= last; j++) {
      const p = (t - st.delay - j * st.period) / st.duration;
      if (p < 0 || p > 1) continue;
      const pt = path.getPointAtLength(p * len);
      const { glow, dot } = packet(used++);
      const alpha = Math.min(1, p * 6, (1 - p) * 6);
      dot.setAttribute("class", `pl-pkt ${st.cls}`);
      dot.setAttribute("cx", pt.x);
      dot.setAttribute("cy", pt.y);
      dot.setAttribute("r", st.r);
      // A variable rather than opacity itself, so the tour's dimming can scale it.
      dot.style.setProperty("--a", alpha.toFixed(3));
      glow.style.setProperty("--a", alpha.toFixed(3));
      dot.style.display = "";
      glow.setAttribute("class", `pl-pkt pl-glow ${st.cls}`);
      glow.setAttribute("cx", pt.x);
      glow.setAttribute("cy", pt.y);
      glow.setAttribute("r", st.r * 2.2);
      glow.style.display = st.glow ? "" : "none";
    }
  }
  for (let k = used; k < pool.length; k++) {
    pool[k].dot.style.display = "none";
    pool[k].glow.style.display = "none";
  }
}

let focusPath = null;

function startPackets() {
  cancelAnimationFrame(frame);
  if (reducedMotion.matches) return;
  if (!t0) t0 = performance.now();
  frame = requestAnimationFrame(draw);
}

// ------------------------------------------------------------ panel, focus

function setFocus(nodes, edges) {
  const on = nodes.length > 0;
  svgRoot.classList.toggle("focus-mode", on);
  for (const el of svgRoot.querySelectorAll(".pl-node")) el.classList.toggle("hl", nodes.includes(el.dataset.id));
  for (const el of svgRoot.querySelectorAll(".pl-edge")) el.classList.toggle("hl", edges.includes(el.dataset.id));
  for (const el of svgRoot.querySelectorAll("[data-edge]")) el.classList.toggle("hl", edges.includes(el.dataset.edge));
  for (const el of svgRoot.querySelectorAll(".pl-node")) el.classList.toggle("selected", el.dataset.id === selected);
}

function panelIntro() {
  $("plPanel").replaceChildren(
    h("span", { class: "eyebrow" }, "Explore"),
    h("h3", {}, "Two paths from one stream"),
    h("p", {}, "Every reading is read twice. The speed path answers within seconds, approximately. The batch path waits for the day to close and answers exactly. The API chooses between them on every request."),
    h("div", { class: "hint" }, icon("right"), h("span", {}, "Select a stage to see what it does, or press ", h("b", {}, "Follow a reading"), " to walk through the pipeline step by step.")),
  );
}

function selectNode(id) {
  endTour(false);
  if (selected === id) {
    clearSelection();
    return;
  }
  selected = id;
  const n = id === "obs" ? OBS : NODES.find((x) => x.id === id);
  const edges = EDGES.filter((e) => e.from === id || e.to === id);
  const neighbours = edges.map((e) => (e.from === id ? e.to : e.from));
  setFocus(id === "obs" ? NODES.map((x) => x.id).concat("obs") : [id, ...neighbours], edges.map((e) => e.id));
  $("plPanel").replaceChildren(
    h(
      "div",
      { class: "tour-head" },
      h("span", { class: "lane-tag", style: `--k:${LANE_COLOR[n.lane]}; color:var(--text-2)` }, LANE_NAME[n.lane]),
      h("button", { class: "icon-btn small", type: "button", "aria-label": "Close", onclick: clearSelection }, icon("x")),
    ),
    h("h3", {}, n.title),
    h("p", {}, n.lead),
    h("ul", {}, ...n.facts.map((f) => h("li", {}, ...rich(f)))),
    h("div", { class: "why" }, h("b", {}, "Why it matters. "), n.why),
  );
}

function clearSelection() {
  selected = null;
  setFocus([], []);
  panelIntro();
}

// -------------------------------------------------------------------- tour

function renderTour() {
  const step = TOUR[tour];
  selected = null;
  focusPath?.remove();
  focusPath = step.route ? s("path", { d: step.route, fill: "none", stroke: "none" }) : null;
  if (focusPath) svgRoot.append(focusPath);
  setFocus(step.nodes, step.edges);
  const last = tour === TOUR.length - 1;
  $("plPanel").replaceChildren(
    h(
      "div",
      { class: "tour-head" },
      h("span", { class: "tour-count" }, `Step ${tour + 1} of ${TOUR.length}`),
      h("button", { class: "icon-btn small", type: "button", "aria-label": "End the tour", onclick: () => endTour(true) }, icon("x")),
    ),
    h("div", { class: "tour-dots" }, ...TOUR.map((_, i) => h("i", { class: i <= tour ? "on" : null }))),
    h("h3", {}, step.title),
    h("p", {}, ...rich(step.text)),
    h(
      "div",
      { class: "tour-nav" },
      h("button", { class: "btn", type: "button", disabled: tour === 0, onclick: () => goTour(tour - 1) }, icon("left"), "Back"),
      h("button", { class: "btn primary", type: "button", onclick: () => (last ? endTour(true) : goTour(tour + 1)) }, last ? "Finish" : "Next", last ? icon("check") : icon("right")),
    ),
  );
}

function goTour(i) {
  tour = Math.max(0, Math.min(TOUR.length - 1, i));
  renderTour();
}

function endTour(resetPanel) {
  if (tour < 0) return;
  tour = -1;
  focusPath?.remove();
  focusPath = null;
  setFocus([], []);
  if (resetPanel) panelIntro();
}

// --------------------------------------------------------------- lifecycle

export default {
  title: "How it works",
  subtitle: "The pipeline, the two layers, and how this dashboard reads them",

  mount() {
    build();
    panelIntro();
    $("tourBtn").addEventListener("click", () => {
      goTour(0);
      $("pipeline").closest(".card").scrollIntoView({ block: "nearest", behavior: reducedMotion.matches ? "auto" : "smooth" });
    });
    $("replayBtn").addEventListener("click", () => {
      endTour(true);
      clearSelection();
      t0 = 0;
      playIntro();
      startPackets();
    });
    document.addEventListener("keydown", (e) => {
      if (!active || tour < 0 || e.target.closest?.("input, select, textarea")) return;
      if (e.key === "ArrowRight") goTour(tour + 1);
      if (e.key === "ArrowLeft") goTour(tour - 1);
      if (e.key === "Escape") endTour(true);
    });
  },

  enter() {
    active = true;
    addArrows();
    if (!this.played) {
      this.played = true;
      playIntro();
    }
    startPackets();
  },

  leave() {
    active = false;
    cancelAnimationFrame(frame);
  },
};
