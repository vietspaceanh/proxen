import {
  Chart,
  LineController,
  BarController,
  DoughnutController,
  LineElement,
  BarElement,
  ArcElement,
  PointElement,
  LinearScale,
  CategoryScale,
  Tooltip,
  Legend,
} from "chart.js";
import { THEMES } from "./theme.js";

Chart.register(
  LineController,
  BarController,
  DoughnutController,
  LineElement,
  BarElement,
  ArcElement,
  PointElement,
  LinearScale,
  CategoryScale,
  Tooltip,
  Legend,
);

export { Chart };

export function chartOpts(extra, theme) {
  const v = THEMES[theme]?.vars ?? THEMES.kanagawa.vars;
  const grid = v["--chart-grid"];
  const text = v["--text-muted"];
  const base = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { labels: { color: text, boxWidth: 12, font: { size: 11 } } } },
    scales: {
      x: { ticks: { color: text, font: { size: 11 } }, grid: { color: grid } },
      y: { ticks: { color: text, font: { size: 11 } }, grid: { color: grid } },
    },
  };
  if (!extra) return base;
  const merged = { ...base, ...extra };
  if (extra.plugins) merged.plugins = { ...base.plugins, ...extra.plugins };
  if (extra.scales) merged.scales = { ...base.scales, ...extra.scales };
  return merged;
}
