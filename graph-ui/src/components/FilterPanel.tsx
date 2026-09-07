import { useMemo } from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { colorForLabel, STATUS_LEGEND } from "../lib/colors";
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
  /* Dead-code view */
  deadCodeView: boolean;
  showOnlyDead: boolean;
  hideEntryPoints: boolean;
  hideTests: boolean;
  onToggleDeadCodeView: () => void;
  onToggleShowOnlyDead: () => void;
  onToggleHideEntryPoints: () => void;
  onToggleHideTests: () => void;
  /* Missed skeleton (#963): white satellite of not-fully-indexed files */
  missedView: boolean;
  missedCount: number;
  onToggleMissedView: () => void;
}

/* Checkbox row matching the existing "Show labels" toggle style */
function CheckRow({
  checked,
  onToggle,
  label,
  count,
}: {
  checked: boolean;
  onToggle: () => void;
  label: string;
  count?: number;
}) {
  return (
    <button
      onClick={onToggle}
      className={`flex items-center gap-1.5 text-[13px] font-medium transition-all ${
        checked ? "text-primary" : "text-foreground/40"
      }`}
    >
      <span
        className={`w-3.5 h-3.5 rounded border flex items-center justify-center transition-all ${
          checked ? "border-primary bg-primary/20" : "border-foreground/15"
        }`}
      >
        {checked && <span className="text-primary text-[12px]">✓</span>}
      </span>
      {label}
      {count !== undefined && (
        <span className="text-foreground/40 tabular-nums">{count.toLocaleString("en-US")}</span>
      )}
    </button>
  );
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
  deadCodeView,
  showOnlyDead,
  hideEntryPoints,
  hideTests,
  onToggleDeadCodeView,
  onToggleShowOnlyDead,
  onToggleHideEntryPoints,
  onToggleHideTests,
  missedView,
  missedCount,
  onToggleMissedView,
}: FilterPanelProps) {
  const { labelCounts, edgeTypeCounts, statusCounts } = useMemo(() => {
    const lc = new Map<string, number>();
    for (const n of data.nodes) lc.set(n.label, (lc.get(n.label) ?? 0) + 1);
    const ec = new Map<string, number>();
    for (const e of data.edges) ec.set(e.type, (ec.get(e.type) ?? 0) + 1);
    const sc = new Map<string, number>();
    for (const n of data.nodes)
      if (n.status) sc.set(n.status, (sc.get(n.status) ?? 0) + 1);
    return {
      labelCounts: [...lc.entries()].sort((a, b) => b[1] - a[1]),
      edgeTypeCounts: [...ec.entries()].sort((a, b) => b[1] - a[1]),
      statusCounts: sc,
    };
  }, [data]);

  const deadCount = statusCounts.get("dead") ?? 0;

  /* Tri-state master over everything the panel lists (node types + edge
   * types — the same global scope enableAll/disableAll always had). */
  const filterCount = labelCounts.length + edgeTypeCounts.length;
  let enabledCount = 0;
  for (const [label] of labelCounts) if (enabledLabels.has(label)) enabledCount++;
  for (const [type] of edgeTypeCounts) if (enabledEdgeTypes.has(type)) enabledCount++;
  const master: "all" | "none" | "some" =
    filterCount > 0 && enabledCount === filterCount
      ? "all"
      : enabledCount === 0
        ? "none"
        : "some";

  return (
    <div className="flex flex-col shrink-0 max-h-[45%] border-b border-border/40">
      {/* Header row — always visible */}
      <div className="flex items-center justify-between px-4 pt-3 pb-2 shrink-0">
        <span className="text-[13px] font-medium text-foreground/50 uppercase tracking-widest">
          Filters
        </span>
        <button
          role="checkbox"
          aria-checked={master === "all" ? "true" : master === "none" ? "false" : "mixed"}
          aria-label="All filters"
          onClick={master === "all" ? onDisableAll : onEnableAll}
          className={`flex items-center gap-1.5 text-[12px] font-medium transition-all ${
            master === "none" ? "text-foreground/40" : "text-primary"
          }`}
        >
          <span
            className={`w-3.5 h-3.5 rounded border flex items-center justify-center transition-all ${
              master === "none" ? "border-foreground/15" : "border-primary bg-primary/20"
            }`}
          >
            {master === "all" && <span className="text-primary text-[12px]">✓</span>}
            {master === "some" && <span className="w-2 h-[2px] rounded-full bg-primary" />}
          </span>
          All
        </button>
      </div>

      {/* Scrollable filter groups */}
      <ScrollArea className="flex-1 min-h-0">
        <div className="px-4 pb-3 space-y-3">
          {/* Node types */}
          {labelCounts.length > 0 && (
            <div>
              <p className="text-[12px] font-medium text-foreground/40 mb-1.5 uppercase tracking-wider">Node types</p>
              <div className="flex flex-wrap gap-1">
                {labelCounts.map(([label, count]) => {
                  const on = enabledLabels.has(label);
                  const c = colorForLabel(label);
                  return (
                    <button
                      key={label}
                      onClick={() => onToggleLabel(label)}
                      className={`inline-flex items-center gap-1 px-1.5 py-[3px] rounded-md text-[12px] font-medium transition-all border ${
                        on ? "border-white/[0.08] bg-popover" : "border-transparent opacity-25 line-through"
                      }`}
                    >
                      <span className="w-[5px] h-[5px] rounded-full" style={{ backgroundColor: on ? c : "#444" }} />
                      <span style={{ color: on ? c : "#555" }}>{label}</span>
                      <span className="text-foreground/35 tabular-nums">{count.toLocaleString("en-US")}</span>
                    </button>
                  );
                })}
              </div>
            </div>
          )}

          {/* Relationships */}
          {edgeTypeCounts.length > 0 && (
            <div>
              <p className="text-[12px] font-medium text-foreground/40 mb-1.5 uppercase tracking-wider">Relationships</p>
              <div className="flex flex-wrap gap-1">
                {edgeTypeCounts.map(([type, count]) => {
                  const on = enabledEdgeTypes.has(type);
                  return (
                    <button
                      key={type}
                      onClick={() => onToggleEdgeType(type)}
                      className={`inline-flex items-center gap-1 px-1.5 py-[3px] rounded-md text-[12px] font-medium transition-all border ${
                        on ? "border-border bg-popover text-foreground/60" : "border-transparent opacity-20 text-foreground/45 line-through"
                      }`}
                    >
                      {type.replace(/_/g, " ").toLowerCase()}
                      <span className="text-foreground/30 tabular-nums">{count.toLocaleString("en-US")}</span>
                    </button>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      </ScrollArea>

      {/* Missed skeleton (#963): white satellite cluster of files the indexer
          could not fully cover, shown beside the code galaxy. Click it to
          focus; click the code galaxy to come back. */}
      <div className="px-4 pt-2 border-t border-border/30 space-y-2 shrink-0">
        <div className="flex items-center justify-between">
          <span className="text-[12px] text-foreground/45 uppercase tracking-widest">
            Missed files
          </span>
          {missedCount > 0 && (
            <span className="text-[12px] text-foreground/50 tabular-nums">
              {missedCount.toLocaleString("en-US")} files
            </span>
          )}
        </div>
        <CheckRow
          checked={missedView}
          onToggle={onToggleMissedView}
          label="Show missed skeleton"
        />
        <p className="text-[12px] leading-snug text-foreground/45">
          {missedCount > 0
            ? "White satellite = files not fully indexed (best-effort). Click it to focus, click the galaxy to return."
            : "No known misses (best-effort — not a completeness guarantee)."}
        </p>
      </div>

      {/* Dead-code view */}
      <div className="px-4 pt-2 border-t border-border/30 space-y-2 shrink-0">
        <div className="flex items-center justify-between">
          <span className="text-[12px] text-foreground/45 uppercase tracking-widest">
            Dead code
          </span>
          <span className="text-[12px] text-red-400/80 tabular-nums">
            {deadCount.toLocaleString("en-US")} dead
          </span>
        </div>

        <CheckRow
          checked={deadCodeView}
          onToggle={onToggleDeadCodeView}
          label="Color by status"
        />
        <CheckRow
          checked={showOnlyDead}
          onToggle={onToggleShowOnlyDead}
          label="Show only dead code"
        />
        <CheckRow
          checked={hideEntryPoints}
          onToggle={onToggleHideEntryPoints}
          label="Hide entry points"
        />
        <CheckRow checked={hideTests} onToggle={onToggleHideTests} label="Hide tests" />

        {/* Legend (only meaningful while colored by status) */}
        {deadCodeView && (
          <div className="flex flex-wrap gap-x-2 gap-y-1 pt-1">
            {STATUS_LEGEND.map((s) => (
              <span
                key={s.status}
                className="inline-flex items-center gap-1 text-[12px] text-foreground/40"
              >
                <span
                  className="w-[6px] h-[6px] rounded-full"
                  style={{ backgroundColor: s.color }}
                />
                {s.label}
              </span>
            ))}
          </div>
        )}
      </div>

      {/* Display options — pinned footer */}
      <div className="px-4 py-2.5 border-t border-border/20 shrink-0">
        <button
          onClick={onToggleShowLabels}
          className={`inline-flex items-center gap-1.5 text-[13px] font-medium transition-all ${
            showLabels ? "text-primary" : "text-foreground/45"
          }`}
        >
          <span className={`w-3.5 h-3.5 rounded border flex items-center justify-center transition-all ${
            showLabels ? "border-primary bg-primary/20" : "border-foreground/15"
          }`}>
            {showLabels && <span className="text-primary text-[12px]">✓</span>}
          </span>
          Show labels
        </button>
      </div>
    </div>
  );
}
