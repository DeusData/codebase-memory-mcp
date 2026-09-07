import { Html } from "@react-three/drei";
import type { GraphNode } from "../lib/types";
import { colorForLabel, colorForStatus } from "../lib/colors";

interface NodeTooltipProps {
  node: GraphNode;
}

function lineRange(node: GraphNode): string | null {
  if (!node.start_line) return null;
  if (node.end_line && node.end_line !== node.start_line)
    return `L${node.start_line}-${node.end_line}`;
  return `L${node.start_line}`;
}

/* The arithmetic behind the node, from the facts the scene payload already
 * carries — no fetch on hover, ever (jitter). "fan" is a wiki term, but this
 * tooltip follows the cursor with pointer-events off, so a MetricChip here
 * would be an unclickable target — labels stay plain text; the wiki remains
 * reachable from the panels. */
function breakdownRows(node: GraphNode): [string, string][] {
  const rows: [string, string][] = [];
  if (node.in_calls !== undefined)
    rows.push(["fan-in", node.in_calls.toLocaleString("en-US")]);
  if (node.out_calls !== undefined)
    rows.push(["fan-out", node.out_calls.toLocaleString("en-US")]);
  if (node.start_line && node.end_line && node.end_line >= node.start_line)
    rows.push([
      "lines",
      (node.end_line - node.start_line + 1).toLocaleString("en-US"),
    ]);
  return rows;
}

export function NodeTooltip({ node }: NodeTooltipProps) {
  const rows = breakdownRows(node);
  return (
    <Html
      position={[node.x, node.y + node.size * 0.7, node.z]}
      center
      style={{ pointerEvents: "none" }}
    >
      <div className="bg-popover/95 backdrop-blur border border-border/60 rounded-md px-3 py-2 text-xs whitespace-nowrap shadow-xl max-w-[350px]">
        <div className="flex items-center gap-1.5 mb-1">
          <span
            className="w-2 h-2 rounded-full shrink-0"
            style={{ backgroundColor: colorForLabel(node.label) }}
          />
          <span className="text-white font-medium truncate">{node.name}</span>
          <span className="text-foreground/45 ml-1 shrink-0">{node.label}</span>
        </div>
        {node.file_path && (
          <p className="text-foreground/45 font-mono truncate">
            {node.file_path}
            {lineRange(node) && <span className="text-foreground/40"> · {lineRange(node)}</span>}
          </p>
        )}
        {rows.length > 0 && (
          <table className="mt-1">
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
        )}
        {node.status && node.status !== "structural" && (
          <div className="flex items-center gap-1.5 mt-1">
            <span
              className="w-1.5 h-1.5 rounded-full shrink-0"
              style={{ backgroundColor: colorForStatus(node.status) }}
            />
            <span className="text-foreground/45">{node.status}</span>
          </div>
        )}
        <p className="text-foreground/35 mt-1 text-[12px]">click for code →</p>
      </div>
    </Html>
  );
}
