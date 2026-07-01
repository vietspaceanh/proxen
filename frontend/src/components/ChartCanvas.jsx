import { useEffect, useRef } from "react";
import { Chart } from "../lib/chart-setup.js";

// Thin Chart.js lifecycle wrapper. Recreates on `type` change; otherwise
// mutates data/options and calls update("none") for cheap re-renders.
export function ChartCanvas({ type, data, options, plugins, className }) {
  const canvasRef = useRef(null);
  const chartRef = useRef(null);
  useEffect(() => {
    if (!canvasRef.current) return;
    chartRef.current = new Chart(canvasRef.current, { type, data, options, plugins: plugins || [] });
    return () => {
      chartRef.current?.destroy();
      chartRef.current = null;
    };
  }, [type]);

  useEffect(() => {
    const c = chartRef.current;
    if (!c) return;
    const vis = c.data.datasets.map((_, i) => c.isDatasetVisible(i));
    c.data = data;
    c.options = options;
    data.datasets.forEach((_, i) => {
      if (i < vis.length) c.setDatasetVisibility(i, vis[i]);
    });
    c.update("none");
  }, [data, options]);

  return (
    <div className={className} style={{ position: "relative", height: "100%", width: "100%" }}>
      <canvas ref={canvasRef} />
    </div>
  );
}
