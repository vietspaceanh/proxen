import { useEffect, useState } from "react";
import { Zap } from "lucide-react";
import { Card, CardContent } from "./ui/card";
import { Badge } from "./ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./ui/table";
import { ChartCanvas } from "./ChartCanvas.jsx";
import { fmt, fmtTime, fmtHour, fmtTTFT, fmtTPS, statusColor, resolveUser, fmtCompact } from "../lib/format.js";
import { THEMES } from "../lib/theme.js";
import { chartOpts, barLabels } from "../lib/chart-setup.js";

// ─── small cell components ───────────────────────────────────────────

function Latency({ ttft, tps }) {
  const t = fmtTTFT(ttft);
  const burst = tps == null; // short-stream burst: rate not measurable (gen < 1s)
  const p = burst ? null : fmtTPS(tps);
  if (!t && !burst && !p) return <TableCell />;
  return (
    <TableCell className="tabular-nums">
      <span className="inline-flex border rounded-full border-border overflow-hidden text-xs">
        {burst
          ? <span style={{ color: "var(--text-muted)" }} title="generation too short to measure rate (< 1s)" className="px-1 py-0.5 inline-flex items-center"><Zap size={11} /></span>
          : p && <span style={{ color: "var(--accent-2)" }} className="px-1 py-0.5">{p}</span>}
        {t && <span style={{ color: "var(--accent)" }} className="px-1 py-0.5">{t}</span>}
      </span>
    </TableCell>
  );
}

function Tokens({ inp, out, cached }) {
  inp = inp || 0; out = out || 0; cached = cached || 0;
  if (!inp && !out) return <TableCell />;
  const parts = [
    <span style={{ color: "var(--accent)" }}>↑{fmt(inp, 0)}</span>,
    <span style={{ color: "var(--success)" }}>↓{fmt(out, 0)}</span>,
  ];
  if (inp > 0 && cached > 0) {
    const pct = Math.round((cached / inp) * 100);
    parts.push(<span style={{ color: pct > 50 ? "var(--success)" : "var(--warning)" }}>{pct}%</span>);
  }
  return (
    <TableCell className="tabular-nums">
      <span className="inline-flex border rounded-full border-border text-xs">{parts.map((p, i) => <span key={i} className="px-1 py-0.5">{p}</span>)}</span>
    </TableCell>
  );
}

function StatusBadge({ status, dropped, review }) {
  if (!status) return <Badge variant="outline">ERR</Badge>;
  if (dropped)
    return (
      <Badge
        variant="outline"
        style={{ color: "var(--orange)", borderColor: "var(--orange)" }}
        title={`upstream returned ${status} but closed the stream before completion`}
      >
        dropped
      </Badge>
    );
  const color = status >= 500 ? "var(--danger)" : status >= 400 ? "var(--orange)" : "var(--success)";
  return (
    <Badge variant="outline" style={{ color, borderColor: color }}>
      {status}{review ? <span style={{ color: "var(--warning)" }}> ⚠</span> : ""}
    </Badge>
  );
}

function ElapsedBadge({ now, startedAt, noSignal, ttft, phase }) {
  const [start] = useState(() => startedAt || now);
  const elapsed = Math.floor(Math.max(0, now - start) / 1000);
  const t = fmtTTFT(ttft);
  const receiving = phase === "receiving";
  const statusBadge = noSignal
    ? <Badge variant="outline" style={{ color: "var(--danger)", borderColor: "var(--danger)" }}>no signal {elapsed}s</Badge>
    : <Badge variant="outline" style={{ color: "var(--warning)", borderColor: "var(--warning)" }}>{receiving ? "receiving" : "requesting"} {elapsed}s</Badge>;
  if (!t) return statusBadge;
  return (
    <span className="inline-flex items-center gap-1.5">
      {statusBadge}
      <Badge variant="outline" style={{ color: "var(--accent)", borderColor: "var(--accent)" }}>ttft: {t}</Badge>
    </span>
  );
}

function InflightRow({ req, keyMap, now }) {
  const noSignal = !!req.no_signal;
  return (
    <TableRow className="bg-[--warning]/5 hover:bg-[--warning]/10">
      <TableCell className="w-[3px] p-0" style={{ background: noSignal ? "var(--danger)" : "var(--warning)" }} />
      <TableCell className="text-muted-foreground tabular-nums text-[0.78rem] whitespace-nowrap">{fmtTime(req.started_at / 1000)}</TableCell>
      <TableCell className="mono"><span className="inline-flex items-center gap-1.5 max-w-[240px]">{req.upstream && <Badge variant="outline" className="h-auto px-1 py-px text-[0.62rem] uppercase tracking-wide text-muted-foreground">{req.upstream}</Badge>}<span className="truncate" title={req.model || ""}>{req.model || ""}</span></span></TableCell>
      <TableCell colSpan={4}>
        <ElapsedBadge now={now} startedAt={req.started_at} noSignal={noSignal} ttft={req.ttft} phase={req.phase} />
      </TableCell>
      <TableCell className="mono text-muted-foreground">{resolveUser(req.key_id, keyMap)}</TableCell>
    </TableRow>
  );
}

// ─── chart config builders ───────────────────────────────────────────

function buildTpsChart(stats, theme) {
  const v = THEMES[theme]?.vars ?? THEMES.kanagawa.vars;
  const ttft = stats.tps_ttft_24h || [];
  const accent = v["--accent"];
  const accent2 = v["--accent-2"];
  const grid = v["--chart-grid"];
  const text = v["--text-muted"];
  return {
    data: {
      labels: ttft.map((r) => fmtHour(r.timestamp)),
      datasets: [
        { label: "TPS", data: ttft.map((r) => r.tps), borderColor: accent, backgroundColor: accent + "22", yAxisID: "y", tension: 0.3, pointRadius: 0, borderWidth: 2, spanGaps: true },
        { label: "TTFT (s)", data: ttft.map((r) => r.ttft), borderColor: accent2, backgroundColor: accent2 + "22", yAxisID: "y1", tension: 0.3, pointRadius: 0, borderWidth: 2 },
      ],
    },
    options: chartOpts({
      scales: {
        x: { ticks: { color: text, maxTicksLimit: 8, font: { size: 11 } }, grid: { color: grid } },
        y: { position: "left", ticks: { color: accent, font: { size: 11 } }, grid: { color: grid }, title: { display: true, text: "TPS", color: accent, font: { size: 11 } } },
        y1: { position: "right", ticks: { color: accent2, font: { size: 11 } }, grid: { drawOnChartArea: false }, title: { display: true, text: "TTFT (s)", color: accent2, font: { size: 11 } } },
      },
    }, theme),
  };
}

function buildDailyTokensChart(stats, theme) {
  const v = THEMES[theme]?.vars ?? THEMES.kanagawa.vars;
  const dt = stats.daily_tokens || [];
  const accent = v["--accent"];
  const accent2 = v["--accent-2"];
  const success = v["--success"];
  const grid = v["--chart-grid"];
  const text = v["--text-muted"];
  return {
    data: {
      labels: dt.map((r) => r.day),
      datasets: [
        { label: "Input", data: dt.map((r) => r.input_tokens), backgroundColor: accent, borderRadius: 3 },
        { label: "Cached", data: dt.map((r) => r.cached_input_tokens), backgroundColor: accent2, borderRadius: 3 },
        { label: "Output", data: dt.map((r) => r.output_tokens), backgroundColor: success, borderRadius: 3 },
      ],
    },
    options: chartOpts({ scales: { x: { ticks: { color: text, maxTicksLimit: 12, font: { size: 11 } }, grid: { color: grid } }, y: { ticks: { color: text, font: { size: 11 } }, grid: { color: grid } } } }, theme),
  };
}

function buildDailyRequestsChart(stats, theme) {
  const v = THEMES[theme]?.vars ?? THEMES.kanagawa.vars;
  const dr = stats.daily_requests || [];
  const warning = v["--warning"];
  const grid = v["--chart-grid"];
  const text = v["--text-muted"];
  return {
    plugins: [barLabels],
    data: {
      labels: dr.map((r) => r.day),
      datasets: [{ label: "Requests", data: dr.map((r) => r.requests), backgroundColor: warning, borderRadius: 3 }],
    },
    options: chartOpts({ scales: { x: { ticks: { color: text, maxTicksLimit: 12, font: { size: 11 } }, grid: { color: grid } }, y: { ticks: { color: text, font: { size: 11 } }, grid: { color: grid } } } }, theme),
  };
}

// ─── Monitor ─────────────────────────────────────────────────────────

function StatCard({ label, children, active }) {
  return (
    <Card className={active ? "card-active" : ""}>
      <CardContent className="p-[18px_20px]">
        <div className="text-muted-foreground text-[0.72rem] font-medium uppercase tracking-wide">{label}</div>
        {children}
      </CardContent>
    </Card>
  );
}

function MonitorImpl({ stats, theme }) {
  const [now, setNow] = useState(() => Date.now());
  const inflight = (stats.gate || {}).inflight || [];
  const hasInflight = inflight.length > 0;
  useEffect(() => {
    if (!hasInflight) return;
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, [hasInflight]);
  const keyMap = stats.key_map || {};
  const gate = stats.gate || {};
  const totals = stats.totals || {};
  const recent = stats.recent || [];

  const inp = totals.total_input || 0;
  const cached = totals.total_cached || 0;
  const cachedPct = inp > 0 ? Math.round((cached / inp) * 100) + "%" : "";

  const tps = buildTpsChart(stats, theme);
  const dailyTokens = buildDailyTokensChart(stats, theme);
  const dailyRequests = buildDailyRequestsChart(stats, theme);

  return (
    <div className="flex flex-col gap-4 px-6 pt-5 pb-5 h-full max-w-[1400px] mx-auto overflow-y-auto">
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4 shrink-0">
        <StatCard label={<>Active{gate.active > 0 && <span className="inline-block w-[7px] h-[7px] rounded-full ml-1.5 align-middle heartbeat" style={{ background: "var(--warning)" }} />}</>} active={gate.active > 0}>
          <div className={"text-[1.55rem] font-semibold mt-1.5 tracking-tight" + (gate.active > 0 ? " text-[--warning]" : "")}>
            {gate.active || 0}<span className="text-muted-foreground text-[0.95rem] font-normal"> / {gate.max_inflight || 5}</span>
          </div>
        </StatCard>
        <StatCard label="Waiting">
          <div className="text-[1.55rem] font-semibold mt-1.5 tracking-tight">{gate.waiting || 0}<span className="text-muted-foreground text-[0.95rem] font-normal"> / {gate.max_waiting || 50}</span></div>
        </StatCard>
        <StatCard label="Total Requests">
          <div className="text-[1.55rem] font-semibold mt-1.5 tracking-tight">{fmt(totals.total_requests || 0, 0)}</div>
        </StatCard>
        <StatCard label="Tokens (in / out)">
          <div className="text-[1.55rem] font-semibold mt-1.5 tracking-tight">{fmtCompact(totals.total_input || 0)} <span className="text-muted-foreground text-[0.95rem] font-normal">/ {fmtCompact(totals.total_output || 0)}</span></div>
          <div className="text-muted-foreground text-[0.72rem] uppercase tracking-wide mt-1">{cachedPct} cached</div>
        </StatCard>
        <StatCard label="Total Cost">
          <div className="text-[1.55rem] font-semibold mt-1.5 tracking-tight text-[--accent]">${fmt(totals.total_cost || 0, 2)}</div>
        </StatCard>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.7fr)] lg:grid-rows-[1fr_1.25fr] gap-4 flex-none lg:flex-1 lg:min-h-0">
        <Card className="flex flex-col h-[240px] lg:h-auto">
          <CardContent className="p-4 flex flex-col flex-1 min-h-0">
            <div className="text-muted-foreground text-[0.72rem] font-semibold uppercase tracking-wide mb-2">24h TPS &amp; TTFT</div>
            <div className="flex-1 min-h-0"><ChartCanvas type="line" {...tps} /></div>
          </CardContent>
        </Card>
        <Card className="flex flex-col h-[240px] lg:h-auto">
          <CardContent className="p-4 flex flex-col flex-1 min-h-0">
            <div className="text-muted-foreground text-[0.72rem] font-semibold uppercase tracking-wide mb-2">Daily Tokens (30d)</div>
            <div className="flex-1 min-h-0"><ChartCanvas type="bar" {...dailyTokens} /></div>
          </CardContent>
        </Card>
        <Card className="flex flex-col h-[240px] lg:h-auto">
          <CardContent className="p-4 flex flex-col flex-1 min-h-0">
            <div className="text-muted-foreground text-[0.72rem] font-semibold uppercase tracking-wide mb-2">Daily Requests (30d)</div>
            <div className="flex-1 min-h-0"><ChartCanvas type="bar" {...dailyRequests} /></div>
          </CardContent>
        </Card>
        <Card className="flex flex-col h-[480px] lg:h-auto">
          <CardContent className="p-4 flex flex-col flex-1 min-h-0">
            <div className="text-muted-foreground text-[0.72rem] font-semibold uppercase tracking-wide mb-2">Live Request Stream</div>
            <Table containerClassName="flex-1 min-h-0 rounded max-h-none">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[3px] p-0" />
                  <TableHead>Time</TableHead>
                  <TableHead>Model</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>TPS · TTFT</TableHead>
                  <TableHead>Tokens</TableHead>
                  <TableHead>Cost</TableHead>
                  <TableHead>User</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {[...inflight].reverse().map((req) => (
                   <InflightRow key={req.id} req={req} keyMap={keyMap} now={now} />
                 ))}
                {recent.map((rec, i) => {
                  const status = rec.status || 0;
                  const indColor = rec.client_disconnect
                    ? "var(--text-muted)"
                    : rec.upstream_dropped
                      ? "var(--orange)"
                      : statusColor(status);
                  return (
                    <TableRow key={rec.id || `r-${rec.timestamp}-${i}`}>
                      <TableCell className="w-[3px] p-0" style={{ background: indColor }} />
                      <TableCell className="text-muted-foreground tabular-nums text-[0.78rem] whitespace-nowrap">{fmtTime(rec.timestamp)}</TableCell>
                      <TableCell className="mono"><span className="inline-flex items-center gap-1.5 max-w-[240px]">{rec.upstream && <Badge variant="outline" className="h-auto px-1 py-px text-[0.62rem] uppercase tracking-wide text-muted-foreground">{rec.upstream}</Badge>}<span className="truncate" title={rec.model || ""}>{rec.model || ""}</span></span></TableCell>
                      <TableCell>{rec.client_disconnect ? <Badge variant="outline">cancelled</Badge> : <StatusBadge status={status} dropped={rec.upstream_dropped} review={rec.needs_review} />}</TableCell>
                      <Latency ttft={rec.ttft} tps={rec.tps} />
                      <Tokens inp={rec.input_tokens} out={rec.output_tokens} cached={rec.cached_input_tokens} />
                      <TableCell className="mono tabular-nums">${fmt(rec.cost || 0, 4)}</TableCell>
                      <TableCell className="mono text-muted-foreground">{resolveUser(rec.key_id, keyMap)}</TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

export const Monitor = MonitorImpl;
