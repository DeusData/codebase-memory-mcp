import type { LoadProgress } from "../hooks/useGraphData";

/* Small constellation whose nodes pulse and whose edges draw themselves in a
 * staggered loop — a miniature of the graph being loaded. Pure SVG/CSS
 * (keyframes in globals.css) so it costs nothing while the real payload
 * streams in, and it degrades to a static constellation under
 * prefers-reduced-motion. */

const LOADER_NODES: Array<{ cx: number; cy: number; r: number; delay: string }> = [
  { cx: 60, cy: 18, r: 5, delay: "0s" },
  { cx: 22, cy: 42, r: 4, delay: "0.35s" },
  { cx: 98, cy: 40, r: 4, delay: "0.7s" },
  { cx: 42, cy: 78, r: 4.5, delay: "1.05s" },
  { cx: 84, cy: 82, r: 3.5, delay: "1.4s" },
  { cx: 60, cy: 52, r: 6, delay: "0.15s" },
  { cx: 14, cy: 88, r: 3, delay: "1.75s" },
];

const LOADER_EDGES: Array<{ x1: number; y1: number; x2: number; y2: number; delay: string }> = [
  { x1: 60, y1: 18, x2: 60, y2: 52, delay: "0s" },
  { x1: 22, y1: 42, x2: 60, y2: 52, delay: "0.3s" },
  { x1: 98, y1: 40, x2: 60, y2: 52, delay: "0.6s" },
  { x1: 42, y1: 78, x2: 60, y2: 52, delay: "0.9s" },
  { x1: 84, y1: 82, x2: 60, y2: 52, delay: "1.2s" },
  { x1: 42, y1: 78, x2: 14, y2: 88, delay: "1.5s" },
  { x1: 22, y1: 42, x2: 60, y2: 18, delay: "1.8s" },
];

function formatMegabytes(bytes: number): string {
  return (bytes / (1024 * 1024)).toFixed(1);
}

interface GraphLoaderProps {
  nodeBudget: number;
  progress: LoadProgress;
}

export function GraphLoader({ nodeBudget, progress }: GraphLoaderProps) {
  const receiving = progress.receivedBytes > 0;
  return (
    <div className="text-center" role="status" aria-live="polite">
      <svg
        width="120"
        height="104"
        viewBox="0 0 120 104"
        className="mx-auto"
        aria-hidden="true"
      >
        {LOADER_EDGES.map((e, i) => (
          <line
            key={`e${i}`}
            x1={e.x1}
            y1={e.y1}
            x2={e.x2}
            y2={e.y2}
            className="graph-loader-edge"
            style={{ animationDelay: e.delay }}
          />
        ))}
        {LOADER_NODES.map((n, i) => (
          <circle
            key={`n${i}`}
            cx={n.cx}
            cy={n.cy}
            r={n.r}
            className="graph-loader-node"
            style={{ animationDelay: n.delay }}
          />
        ))}
      </svg>
      <p className="text-white/50 text-sm mt-4">
        {receiving ? "Receiving graph" : "Computing layout"} — up to{" "}
        {nodeBudget.toLocaleString("en-US")} nodes
      </p>
      <p className="text-cyan-300/60 text-xs font-mono mt-1 h-4">
        {receiving
          ? progress.totalBytes
            ? `${formatMegabytes(progress.receivedBytes)} of ${formatMegabytes(progress.totalBytes)} MB`
            : `${formatMegabytes(progress.receivedBytes)} MB received`
          : " "}
      </p>
    </div>
  );
}
