import { useMemo } from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { colorForLabel } from "../lib/colors";
import type { GraphData } from "../lib/types";

interface FilterPanelProps {
  data: GraphData;
  enabledLabels: Set<string>;
  enabledEdgeTypes: Set<string>;
  showLabels: boolean;
  onToggleLabel: (label: string) => void;
  onToggleEdgeType: (type: string) => void;
  onToggleShowLabels: () => void;
  onEnableAll: () => void;
  onDisableAll: () => void;
}

export function FilterPanel({
  data,
  enabledLabels,
  enabledEdgeTypes,
  showLabels,
  onToggleLabel,
  onToggleEdgeType,
  onToggleShowLabels,
  onEnableAll,
  onDisableAll,
}: FilterPanelProps) {
  const { labelCounts, edgeTypeCounts } = useMemo(() => {
    const lc = new Map<string, number>();
    for (const n of data.nodes) lc.set(n.label, (lc.get(n.label) ?? 0) + 1);
    const ec = new Map<string, number>();
    for (const e of data.edges) ec.set(e.type, (ec.get(e.type) ?? 0) + 1);
    return {
      labelCounts: [...lc.entries()].sort((a, b) => b[1] - a[1]),
      edgeTypeCounts: [...ec.entries()].sort((a, b) => b[1] - a[1]),
    };
  }, [data]);

  return (
    <div className="flex flex-col shrink-0 max-h-[45%] border-b border-border/40">
      {/* Header row — always visible */}
      <div className="flex items-center justify-between px-4 pt-3 pb-2 shrink-0">
        <span className="text-[11px] font-medium text-foreground/50 uppercase tracking-widest">
          Filters
        </span>
        <div className="flex items-center gap-2">
          <button onClick={onEnableAll} className="text-[10px] text-primary/70 hover:text-primary transition-colors">All</button>
          <span className="text-foreground/15">|</span>
          <button onClick={onDisableAll} className="text-[10px] text-primary/70 hover:text-primary transition-colors">None</button>
        </div>
      </div>

      {/* Scrollable filter groups */}
      <ScrollArea className="flex-1 min-h-0">
        <div className="px-4 pb-3 space-y-3">
          {/* Node types */}
          {labelCounts.length > 0 && (
            <div>
              <p className="text-[10px] font-medium text-foreground/40 mb-1.5 uppercase tracking-wider">Node types</p>
              <div className="flex flex-wrap gap-1">
                {labelCounts.map(([label, count]) => {
                  const on = enabledLabels.has(label);
                  const c = colorForLabel(label);
                  return (
                    <button
                      key={label}
                      onClick={() => onToggleLabel(label)}
                      className={`inline-flex items-center gap-1 px-1.5 py-[3px] rounded-md text-[10px] font-medium transition-all border ${
                        on ? "border-white/[0.08] bg-white/[0.04]" : "border-transparent opacity-25"
                      }`}
                    >
                      <span className="w-[5px] h-[5px] rounded-full" style={{ backgroundColor: on ? c : "#444" }} />
                      <span style={{ color: on ? c : "#555" }}>{label}</span>
                      <span className="text-foreground/20 tabular-nums">{count.toLocaleString()}</span>
                    </button>
                  );
                })}
              </div>
            </div>
          )}

          {/* Relationships */}
          {edgeTypeCounts.length > 0 && (
            <div>
              <p className="text-[10px] font-medium text-foreground/40 mb-1.5 uppercase tracking-wider">Relationships</p>
              <div className="flex flex-wrap gap-1">
                {edgeTypeCounts.map(([type, count]) => {
                  const on = enabledEdgeTypes.has(type);
                  return (
                    <button
                      key={type}
                      onClick={() => onToggleEdgeType(type)}
                      className={`inline-flex items-center gap-1 px-1.5 py-[3px] rounded-md text-[10px] font-medium transition-all border ${
                        on ? "border-white/[0.06] bg-white/[0.03] text-foreground/60" : "border-transparent opacity-20 text-foreground/30"
                      }`}
                    >
                      {type.replace(/_/g, " ").toLowerCase()}
                      <span className="text-foreground/15 tabular-nums">{count.toLocaleString()}</span>
                    </button>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      </ScrollArea>

      {/* Display options — pinned footer */}
      <div className="px-4 py-2.5 border-t border-border/20 shrink-0">
        <button
          onClick={onToggleShowLabels}
          className={`inline-flex items-center gap-1.5 text-[11px] font-medium transition-all ${
            showLabels ? "text-primary" : "text-foreground/30"
          }`}
        >
          <span className={`w-3.5 h-3.5 rounded border flex items-center justify-center transition-all ${
            showLabels ? "border-primary bg-primary/20" : "border-foreground/15"
          }`}>
            {showLabels && <span className="text-primary text-[9px]">✓</span>}
          </span>
          Show labels
        </button>
      </div>
    </div>
  );
}
