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
import { cssVar, THEMES } from "./theme.js";

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

export const barLabels = {
  id: "barLabels",
  afterDraw(chart) {
    const ctx = chart.ctx;
    chart.data.datasets.forEach((ds, i) => {
      chart.getDatasetMeta(i).data.forEach((bar, j) => {
        const v = ds.data[j];
        if (!v) return;
        ctx.save();
        ctx.fillStyle = cssVar("--text-muted");
        ctx.font = "400 12px sans-serif";
        ctx.textAlign = "center";
        ctx.fillText(v, bar.x, bar.y - 7);
        ctx.restore();
      });
    });
  },
};

export { Chart };

export function chartOpts(extra, theme) {
  const v = THEMES[theme]?.vars ?? THEMES.kanagawa.vars;
  const grid = v["--chart-grid"];
  const text = v["--text-muted"];
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { labels: { color: text, boxWidth: 12, font: { size: 11 } } } },
    scales: {
      x: { ticks: { color: text, font: { size: 11 } }, grid: { color: grid } },
      y: { ticks: { color: text, font: { size: 11 } }, grid: { color: grid } },
    },
    ...extra,
  };
}
