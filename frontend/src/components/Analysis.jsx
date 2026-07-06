import { useEffect, useState } from "react";
import { Card, CardContent } from "./ui/card";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./ui/table";
import { ChartCanvas } from "./ChartCanvas.jsx";
import { fmt, fmtCompact, fmtTTFT, fmtTPS, api, resolveUser } from "../lib/format.js";
import { THEMES } from "../lib/theme.js";
import { chartOpts } from "../lib/chart-setup.js";

function doughnutOpts(theme) {
  const v = THEMES[theme]?.vars ?? THEMES.kanagawa.vars;
  const text = v["--text-muted"];
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { position: "right", labels: { color: text, boxWidth: 12, font: { size: 11 }, padding: 8 } },
      tooltip: { callbacks: { label: (ctx) => ` ${ctx.label}: $${fmt(ctx.parsed, 2)}` } },
    },
  };
}

function SectionLabel({ children }) {
  return <div className="text-muted-foreground text-[0.72rem] font-semibold uppercase tracking-wide mb-2">{children}</div>;
}

function buildCostDoughnut(models, theme) {
  const v = THEMES[theme]?.vars ?? THEMES.kanagawa.vars;
  const top = models.slice(0, 8);
  const otherCost = models.slice(8).reduce((s, m) => s + (m.total_cost || 0), 0);
  const labels = top.map((m) => m.model);
  const data = top.map((m) => m.total_cost || 0);
  if (otherCost > 0) { labels.push("Other"); data.push(otherCost); }
  const palette = [v["--accent"], v["--accent-2"], v["--success"], v["--warning"], v["--orange"], v["--danger"], "#7dd3fc", "#c084fc", "#86efac"];
  return {
    data: { labels, datasets: [{ data, backgroundColor: palette.slice(0, labels.length), borderWidth: 0 }] },
    options: doughnutOpts(theme),
  };
}

function buildErrorChart(dailyErrors, theme) {
  const v = THEMES[theme]?.vars ?? THEMES.kanagawa.vars;
  const de = dailyErrors || [];
  const accent = v["--accent"];
  const orange = v["--orange"];
  const danger = v["--danger"];
  const warning = v["--warning"];
  const grid = v["--chart-grid"];
  const text = v["--text-muted"];
  return {
    data: {
      labels: de.map((r) => r.day),
      datasets: [
        { label: "4xx", data: de.map((r) => r.errors_4xx || 0), backgroundColor: orange, borderRadius: 3 },
        { label: "5xx", data: de.map((r) => r.errors_5xx || 0), backgroundColor: danger, borderRadius: 3 },
        { label: "Cancelled", data: de.map((r) => r.cancelled || 0), backgroundColor: accent, borderRadius: 3 },
        { label: "Dropped", data: de.map((r) => r.dropped || 0), backgroundColor: warning, borderRadius: 3 },
      ],
    },
    options: chartOpts({
      scales: {
        x: { stacked: true, ticks: { color: text, maxTicksLimit: 12, font: { size: 11 } }, grid: { color: grid } },
        y: { stacked: true, ticks: { color: text, font: { size: 11 } }, grid: { color: grid } },
      },
    }, theme),
  };
}

function buildDailyCostChart(dailyCost, theme) {
  const v = THEMES[theme]?.vars ?? THEMES.kanagawa.vars;
  const dc = dailyCost || [];
  const accent = v["--accent"];
  const warning = v["--warning"];
  const grid = v["--chart-grid"];
  const text = v["--text-muted"];
  const trendData = buildTrend(dc);
  return {
    data: {
      labels: dc.map((r) => r.day),
      datasets: [
        { label: "Daily Cost", data: dc.map((r) => r.cost || 0), borderColor: accent, backgroundColor: accent + "22", tension: 0.3, pointRadius: 3, pointHoverRadius: 6, borderWidth: 2, fill: true, pointStyle: "circle" },
        ...(trendData.length ? [{ label: "Trend", data: trendData, borderColor: warning, borderDash: [6, 3], tension: 0, pointRadius: 0, borderWidth: 1.5, fill: false, pointStyle: "line" }] : []),
      ],
    },
    options: chartOpts({
      scales: {
        x: { ticks: { color: text, maxTicksLimit: 12, font: { size: 11 } }, grid: { color: grid } },
        y: { ticks: { color: text, font: { size: 11 }, callback: (v2) => "$" + fmt(v2, 2) }, grid: { color: grid } },
      },
      plugins: {
        legend: { labels: { color: text, boxWidth: 20, boxHeight: 1, font: { size: 11 } } },
        tooltip: { callbacks: { label: (ctx) => ` ${ctx.dataset.label}: $${fmt(ctx.parsed.y, 2)}` } },
      },
    }, theme),
  };
}

function buildCacheChart(models, theme) {
  const v = THEMES[theme]?.vars ?? THEMES.kanagawa.vars;
  const sorted = [...(models || [])].sort((a, b) => (b.total_cached || 0) - (a.total_cached || 0));
  const cs = sorted.slice(0, 15);
  const accent = v["--accent"];
  const accent2 = v["--accent-2"];
  const grid = v["--chart-grid"];
  const text = v["--text-muted"];
  return {
    data: {
      labels: cs.map((r) => r.model),
      datasets: [
        { label: "Cached", data: cs.map((r) => r.total_cached || 0), backgroundColor: accent2, borderRadius: 3 },
        { label: "Input", data: cs.map((r) => (r.total_input || 0) - (r.total_cached || 0)), backgroundColor: accent, borderRadius: 3 },
      ],
    },
    options: chartOpts({
      indexAxis: "y",
      scales: {
        x: { stacked: true, ticks: { color: text, font: { size: 11 } }, grid: { color: grid } },
        y: { stacked: true, ticks: { color: text, font: { size: 11 } }, grid: { color: grid } },
      },
    }, theme),
  };
}

function projectedMonthly(dailyCost, windowDays = 7) {
  const todayUTC = new Date().toISOString().slice(0, 10);
  const recent = (dailyCost || []).filter((d) => d.day !== todayUTC).slice(-windowDays);
  if (!recent.length) return 0;
  return (recent.reduce((s, d) => s + (d.cost || 0), 0) / recent.length) * 30;
}

function buildTrend(dailyCost) {
  const dc = dailyCost || [];
  const n = dc.length;
  if (n < 2) return [];
  const todayUTC = new Date().toISOString().slice(0, 10);
  let sumX = 0, sumX2 = 0, sumY = 0, sumXY = 0, m = 0;
  for (let i = 0; i < n; i++) {
    if (dc[i].day === todayUTC) continue;
    const y = dc[i].cost || 0;
    sumX += i; sumX2 += i * i; sumY += y; sumXY += i * y; m++;
  }
  if (m < 2) return [];
  const slope = (m * sumXY - sumX * sumY) / (m * sumX2 - sumX * sumX);
  const intercept = (sumY - slope * sumX) / m;
  return dc.map((_, i) => Math.max(0, intercept + slope * i));
}

function AnalysisImpl({ stats, theme, active }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchAnalysis = async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await api("GET", "/api/analysis");
      setData(result);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { if (active) fetchAnalysis(); }, [active]);

  const keyMap = data?.key_map || stats?.key_map || {};

  const costDoughnut = data ? buildCostDoughnut(data.model_breakdown || [], theme) : null;
  const costChart = data ? buildDailyCostChart(data.daily_cost || [], theme) : null;
  const errorChart = data ? buildErrorChart(data.daily_errors || [], theme) : null;
  const cacheChart = data ? buildCacheChart(data.model_breakdown || [], theme) : null;

  const days = data?.daily_cost || [];
  const costSummary = days.length ? (() => {
    const totalCost = days.reduce((s, d) => s + (d.cost || 0), 0);
    const last7 = days.slice(-7);
    const dailyAvg7 = last7.length ? last7.reduce((s, d) => s + (d.cost || 0), 0) / last7.length : 0;
    const monthlyEst = projectedMonthly(days);
    return { totalCost, dailyAvg7, monthlyEst };
  })() : null;

  const breakdown = data?.model_breakdown || [];
  const cacheSummary = breakdown.length ? (() => {
    const totalIn = breakdown.reduce((s, x) => s + (x.total_input || 0), 0);
    const totalCached = breakdown.reduce((s, x) => s + (x.total_cached || 0), 0);
    const pct = totalIn > 0 ? Math.round((totalCached / totalIn) * 100) : 0;
    return { totalIn, totalCached, pct };
  })() : null;

  if (loading && !data) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground">
        Loading analysis...
      </div>
    );
  }

  if (error && !data) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 text-muted-foreground">
        <span>Failed to load analysis: {error}</span>
        <button onClick={fetchAnalysis} className="text-[--accent] underline cursor-pointer">Retry</button>
      </div>
    );
  }

  const models = data?.model_breakdown || [];
  const keys = data?.key_breakdown || [];
  const errors = data?.error_stats || [];

  return (
    <div className="flex flex-col gap-4 px-6 pt-5 pb-5 max-w-[1400px] mx-auto">

      {/* ── Model Breakdown ───────────────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-[1fr_minmax(260px,0.8fr)] gap-4">
        <Card>
          <CardContent className="p-4">
            <SectionLabel>Model Breakdown (30d)</SectionLabel>
            <Table containerClassName="max-h-[30vh]">
              <TableHeader>
                <TableRow>
                  <TableHead>Model</TableHead>
                  <TableHead className="text-right">Requests</TableHead>
                  <TableHead className="text-right">Cost</TableHead>
                  <TableHead className="text-right">Tokens In</TableHead>
                  <TableHead className="text-right">Tokens Out</TableHead>
                  <TableHead className="text-right">Cached</TableHead>
                  <TableHead className="text-right">Avg TPS</TableHead>
                  <TableHead className="text-right">Avg TTFT</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {models.map((m) => {
                  const inp = m.total_input || 0;
                  const cached = m.total_cached || 0;
                  const cachePct = inp > 0 ? Math.round((cached / inp) * 100) : 0;
                  return (
                    <TableRow key={m.model}>
                      <TableCell className="mono max-w-[220px] truncate font-medium">{m.model}</TableCell>
                      <TableCell className="text-right tabular-nums">{fmt(m.requests, 0)}</TableCell>
                      <TableCell className="text-right tabular-nums text-[--accent]">${fmt(m.total_cost, 2)}</TableCell>
                      <TableCell className="text-right tabular-nums">{fmtCompact(m.total_input)}</TableCell>
                      <TableCell className="text-right tabular-nums">{fmtCompact(m.total_output)}</TableCell>
                      <TableCell className="text-right tabular-nums">
                        <span style={{ color: cachePct > 50 ? "var(--success)" : cachePct > 0 ? "var(--warning)" : "var(--text-muted)" }}>
                          {cachePct}%
                        </span>
                      </TableCell>
                      <TableCell className="text-right tabular-nums">{fmtTPS(m.avg_tps)}</TableCell>
                      <TableCell className="text-right tabular-nums">{fmtTTFT(m.avg_ttft)}</TableCell>
                    </TableRow>
                  );
                })}
                {models.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={8} className="text-center text-muted-foreground py-6">No data yet</TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
        <Card className="flex flex-col h-[300px] lg:h-auto">
          <CardContent className="p-4 flex flex-col flex-1 min-h-0">
            <SectionLabel>Cost Distribution</SectionLabel>
            <div className="flex-1 min-h-0">
              {costDoughnut ? <ChartCanvas type="doughnut" {...costDoughnut} /> : <span className="text-muted-foreground text-sm">No data</span>}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* ── Daily Cost Trend ──────────────────────────────────────── */}
      <Card>
        <CardContent className="p-4 flex flex-col gap-3">
          <SectionLabel>Daily Cost Trend (30d)</SectionLabel>
          <div className="h-[240px]">
            {costChart ? <ChartCanvas type="line" {...costChart} /> : <span className="text-muted-foreground text-sm">No data</span>}
          </div>
          {costSummary && (
            <div className="flex gap-6 text-sm text-muted-foreground">
              <span>30d total: <span className="text-[--accent]">${fmt(costSummary.totalCost, 2)}</span></span>
              <span>7d avg/day: <span className="text-[--accent]">${fmt(costSummary.dailyAvg7, 2)}</span></span>
              {costSummary.monthlyEst > 0 && <span>Projected: <span className="text-[--warning]">${fmt(costSummary.monthlyEst, 2)}/mo</span></span>}
            </div>
          )}
        </CardContent>
      </Card>

      {/* ── User / Key Analytics ──────────────────────────────────── */}
      <Card>
        <CardContent className="p-4">
          <SectionLabel>User / Key Analytics (30d)</SectionLabel>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>User</TableHead>
                <TableHead className="text-right">Requests</TableHead>
                <TableHead className="text-right">Cost</TableHead>
                <TableHead className="text-right">Tokens In</TableHead>
                <TableHead className="text-right">Tokens Out</TableHead>
                <TableHead className="text-right">Cached</TableHead>
                <TableHead className="text-right">Avg TPS</TableHead>
                <TableHead className="text-right">Avg TTFT</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {keys.map((k, i) => {
                const inp = k.total_input || 0;
                const cached = k.total_cached || 0;
                const cachePct = inp > 0 ? Math.round((cached / inp) * 100) : 0;
                return (
                  <TableRow key={k.key_id || "anon-" + i}>
                    <TableCell className="mono font-medium">{resolveUser(k.key_id, keyMap, "—")}</TableCell>
                    <TableCell className="text-right tabular-nums">{fmt(k.requests, 0)}</TableCell>
                    <TableCell className="text-right tabular-nums text-[--accent]">${fmt(k.total_cost, 2)}</TableCell>
                    <TableCell className="text-right tabular-nums">{fmtCompact(k.total_input)}</TableCell>
                    <TableCell className="text-right tabular-nums">{fmtCompact(k.total_output)}</TableCell>
                    <TableCell className="text-right tabular-nums">
                      <span style={{ color: cachePct > 50 ? "var(--success)" : cachePct > 0 ? "var(--warning)" : "var(--text-muted)" }}>
                        {cachePct}%
                      </span>
                    </TableCell>
                    <TableCell className="text-right tabular-nums">{fmtTPS(k.avg_tps)}</TableCell>
                    <TableCell className="text-right tabular-nums">{fmtTTFT(k.avg_ttft)}</TableCell>
                  </TableRow>
                );
              })}
              {keys.length === 0 && (
                <TableRow>
                  <TableCell colSpan={8} className="text-center text-muted-foreground py-6">No data yet</TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* ── Error Analysis ────────────────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-[1fr_minmax(300px,0.8fr)] gap-4">
        <Card>
          <CardContent className="p-4">
            <SectionLabel>Error Analysis (30d)</SectionLabel>
            <Table containerClassName="max-h-[40vh]">
              <TableHeader>
                <TableRow>
                  <TableHead>Model</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Count</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {errors.map((e, i) => {
                  const color = e.status >= 500 ? "var(--danger)" : "var(--orange)";
                  return (
                    <TableRow key={i}>
                      <TableCell className="mono max-w-[220px] truncate">{e.model}</TableCell>
                      <TableCell>
                        <Badge variant="outline" style={{ color, borderColor: color }}>{e.status}</Badge>
                      </TableCell>
                      <TableCell className="text-right tabular-nums">{fmt(e.count, 0)}</TableCell>
                    </TableRow>
                  );
                })}
                {errors.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={3} className="text-center text-muted-foreground py-6">
                      <span style={{ color: "var(--success)" }}>No errors recorded</span>
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
        <Card className="flex flex-col h-[280px] lg:h-auto">
          <CardContent className="p-4 flex flex-col flex-1 min-h-0">
            <SectionLabel>Daily Errors</SectionLabel>
            <div className="flex-1 min-h-0">
              {errorChart ? <ChartCanvas type="bar" {...errorChart} /> : <span className="text-muted-foreground text-sm">No data</span>}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* ── Cache Efficiency ──────────────────────────────────────── */}
      <Card>
        <CardContent className="p-4 flex flex-col gap-3">
          <SectionLabel>Cache Efficiency by Model (30d)</SectionLabel>
          <div className="h-[280px]">
            {cacheChart ? <ChartCanvas type="bar" {...cacheChart} /> : <span className="text-muted-foreground text-sm">No data</span>}
          </div>
          {cacheSummary && (
            <div className="flex gap-6 text-sm text-muted-foreground">
              <span>Overall cache rate: <span style={{ color: cacheSummary.pct > 50 ? "var(--success)" : "var(--warning)" }}>{cacheSummary.pct}%</span></span>
              <span>Cached tokens: <span style={{ color: "var(--accent-2)" }}>{fmtCompact(cacheSummary.totalCached)}</span></span>
               <span>Total input tokens: {fmtCompact(cacheSummary.totalIn)}</span>
            </div>
          )}
        </CardContent>
      </Card>

      {/* ── Refresh ──────────────────────────────────────────────── */}
      <div className="flex justify-center">
        <Button variant="outline" size="sm" onClick={fetchAnalysis} disabled={loading}>
          {loading ? "Refreshing..." : "Refresh data"}
        </Button>
      </div>
    </div>
  );
}

export const Analysis = AnalysisImpl;
