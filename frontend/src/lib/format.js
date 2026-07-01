// Shared formatters and HTTP utilities (ported from static/format.js).

export function fmt(n, digits = 2) {
  if (n === null || n === undefined) return "";
  return Number(n).toLocaleString(undefined, { maximumFractionDigits: digits });
}

export function fmtCompact(n, digits = 2) {
  if (n === null || n === undefined) return "";
  const v = Number(n);
  if (v >= 1e9) return (v / 1e9).toFixed(digits) + "B";
  if (v >= 1e6) return (v / 1e6).toFixed(digits) + "M";
  return v.toLocaleString(undefined, { maximumFractionDigits: 0 });
}

export function fmtTime(ts) {
  return new Date(ts * 1000).toLocaleTimeString(undefined, {
    hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

export function fmtHour(ts) {
  return new Date(ts * 1000).toLocaleTimeString(undefined, {
    hour12: false, hour: "2-digit", minute: "2-digit",
  });
}

export function fmtAgo(ts) {
  if (!ts) return "never";
  const s = Math.floor(Date.now() / 1000 - ts);
  if (s < 60) return s + "s ago";
  if (s < 3600) return Math.floor(s / 60) + "m ago";
  if (s < 86400) return Math.floor(s / 3600) + "h ago";
  return Math.floor(s / 86400) + "d ago";
}

export function fmtTTFT(ttft) {
  if (!ttft || ttft <= 0) return "";
  if (ttft < 1) return Math.round(ttft * 1000) + "ms";
  return fmt(ttft) + "s";
}

export function fmtTPS(tps) {
  if (!tps || tps <= 0) return "";
  return fmt(tps, 1) + "t/s";
}

export function statusColor(status) {
  if (status >= 500) return "var(--danger)";
  if (status >= 400) return "var(--orange)";
  if (status === 0) return "var(--text-muted)";
  return "var(--success)";
}

export function adminKey() {
  return localStorage.getItem("proxen-admin-key") || "";
}

export function adminHeaders(extra = {}) {
  const h = { "Content-Type": "application/json", ...extra };
  const k = adminKey();
  if (k) h["Authorization"] = `Bearer ${k}`;
  return h;
}

export async function api(method, path, body) {
  const opts = { method, headers: adminHeaders() };
  if (body !== undefined) opts.body = JSON.stringify(body);
  const resp = await fetch(path, opts);
  if (resp.status === 403) throw new Error("management disabled");
  if (!resp.ok) {
    let detail = resp.statusText;
    try { detail = (await resp.json()).error?.message || detail; } catch (_) {}
    throw new Error(detail);
  }
  return resp.json();
}


export function randomKey() {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return "px-" + Array.from(bytes).map((b) => b.toString(16).padStart(2, "0")).join("");
}

export function orEmpty(v) {
  return (v === null || v === undefined || v === "") ? "" : v;
}

export function resolveUser(keyId, keyMap, fallback = "") {
  return keyId ? keyMap[keyId] || keyId.slice(0, 8) : fallback;
}

export function parseLimits(l) {
  return {
    inflight: l?.max_inflight ?? "",
    req: l?.max_requests ?? "",
    reqWin: l?.max_requests_window_s ? l.max_requests_window_s / 3600 : "",
    tok: l?.max_tokens ?? "",
    tokWin: l?.max_tokens_window_s ? l.max_tokens_window_s / 3600 : "",
  };
}

export function buildLimits(f) {
  return {
    max_inflight: f.inflight ? parseInt(f.inflight) : null,
    max_requests: f.req ? parseInt(f.req) : null,
    max_requests_window_s: f.reqWin ? parseFloat(f.reqWin) * 3600 : null,
    max_tokens: f.tok ? parseInt(f.tok) : null,
    max_tokens_window_s: f.tokWin ? parseFloat(f.tokWin) * 3600 : null,
  };
}

export function buildModelBody(f) {
  return {
    id: f.id || undefined,
    enabled: f.enabled,
    input_per_1m: parseFloat(f.inputPer) || 0,
    cached_input_per_1m: parseFloat(f.cachedPer) || 0,
    output_per_1m: parseFloat(f.outputPer) || 0,
    max_input_tokens: f.maxIn ? parseInt(f.maxIn) : null,
    max_output_tokens: f.maxOut ? parseInt(f.maxOut) : null,
    fallback_strategy: f.fallbackStrategy || "manual",
    extra_body: f.extraBody || null,
    routes: (f.routes || []).map((r, i) => ({
      upstream_name: r.upstream_name,
      upstream_model_id: r.upstream_model_id,
      sort_order: i,
      enabled: r.enabled !== false,
    })),
  };
}

