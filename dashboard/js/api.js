// The serving API, as the dashboard uses it. Same origin (T135), so no CORS and no base
// URL: every path here is absolute on the host that served the page.

export class HttpError extends Error {
  constructor(status, url) {
    super(`${status} ${url}`);
    this.status = status;
  }
}

async function getJSON(url) {
  const res = await fetch(url, { cache: "no-store", headers: { Accept: "application/json" } });
  if (!res.ok) throw new HttpError(res.status, url);
  return res.json();
}

const enc = encodeURIComponent;

export const api = {
  clock: () => getJSON("/api/v1/clock"),
  zonesLoad: () => getJSON("/api/v1/zones/load"),
  zonesHistory: (minutes) => getJSON(`/api/v1/zones/history?minutes=${minutes}`),
  households: () => getJSON("/api/v1/households"),
  bill: (hh, day) => getJSON(`/api/v1/households/${enc(hh)}/bill?date=${enc(day)}`),
  billDelta: (hh, day) => getJSON(`/api/v1/households/${enc(hh)}/bill/delta?date=${enc(day)}`),
  report: (day) => getJSON(`/api/v1/reports/daily?date=${enc(day)}`),
  reportUrl: (day) => `/api/v1/reports/daily?date=${enc(day)}`,
  alerts: () => getJSON("/api/v1/alerts/status"),

  /** Readiness answers 503 *with a body* naming what is down, so read it either way. */
  async health() {
    const res = await fetch("/health/ready", { cache: "no-store" });
    const body = await res.json().catch(() => null);
    return { ready: res.ok, dependencies: body?.dependencies ?? [] };
  },
};
