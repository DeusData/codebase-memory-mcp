import { Html } from "@react-three/drei";
import type { GraphNode, Region } from "../lib/types";
import { cohesionWord } from "../lib/regions";

/* Hover card for a region blob in the coarsest galaxy view: what a human
 * needs before deciding to drill — the region's arithmetic as scannable
 * rows, cohesion in words, the hub. Only facts the regions payload already
 * carries are shown — no fetch on hover, ever (jitter); internal/external
 * share and the top external tie are not in the payload today and stay in
 * the RegionPanel. "cohesion" is a wiki term, but this tooltip follows the
 * cursor with pointer-events off, so a MetricChip here would be an
 * unclickable target — labels stay plain text; the wiki remains reachable
 * from the panels. */
export function RegionTooltip({ region, node }: { region: Region; node: GraphNode }) {
  const rows: [string, string][] = [
    ["symbols", region.members.toLocaleString("en-US")],
    ["files", region.files.toLocaleString("en-US")],
    ["cohesion", region.cohesion.toFixed(2)],
  ];
  return (
    <Html
      position={[node.x, node.y + node.size * 0.7, node.z]}
      center
      style={{ pointerEvents: "none" }}
    >
      <div className="bg-popover/95 backdrop-blur border border-border/60 rounded-md px-3 py-2 text-xs whitespace-nowrap shadow-xl max-w-[360px]">
        <div className="flex items-center gap-1.5 mb-1">
          <span
            className="w-2 h-2 rounded-full shrink-0"
            style={{ backgroundColor: region.color }}
          />
          <span className="text-foreground font-medium truncate">{region.name}</span>
        </div>
        <table>
          <tbody>
            {rows.map(([label, value]) => (
              <tr key={label}>
                <td className="pr-4 text-foreground/45">{label}</td>
                <td className="text-right tabular-nums text-foreground/70">
                  {value}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className="text-foreground/45 mt-0.5">{cohesionWord(region.cohesion)}</p>
        {region.hub && (
          <p className="text-foreground/45 font-mono truncate mt-0.5">hub: {region.hub}</p>
        )}
        <p className="text-foreground/35 mt-1 text-[12px]">double-click to open →</p>
      </div>
    </Html>
  );
}
