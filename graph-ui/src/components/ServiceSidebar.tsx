import { useMemo, useState } from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { SERVICE_NODE_COLORS } from "../lib/colors";
import type { GraphNode } from "../lib/types";

interface ServiceSidebarProps {
  nodes: GraphNode[];
  onSelectNode: (path: string, nodeIds: Set<number>) => void;
  selectedPath: string | null;
}

const KIND_ORDER = ["Service", "Topic", "GraphQL", "Table"];

function KindSection({
  kind,
  nodes,
  onSelectNode,
  selectedPath,
}: {
  kind: string;
  nodes: GraphNode[];
  onSelectNode: (path: string, nodeIds: Set<number>) => void;
  selectedPath: string | null;
}) {
  const [expanded, setExpanded] = useState(true);
  const color = SERVICE_NODE_COLORS[kind] ?? "#94a3b8";

  return (
    <div>
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full text-left px-4 py-2 text-[12px] font-semibold hover:bg-white/[0.03] transition-colors"
        style={{ color: color + "cc" }}
      >
        <span className="text-foreground/20 w-3 text-center text-[10px]">
          {expanded ? "▾" : "▸"}
        </span>
        <span
          className="w-[6px] h-[6px] rounded-full"
          style={{ backgroundColor: color }}
        />
        {kind}s ({nodes.length})
      </button>
      {expanded && (
        <div>
          {nodes.map((n) => {
            const isSelected = selectedPath === n.name;
            return (
              <button
                key={n.id}
                onClick={() => onSelectNode(n.name, new Set([n.id]))}
                className={`flex items-center gap-2 w-full text-left px-4 py-[5px] text-[11px] transition-colors ${
                  isSelected
                    ? "bg-primary/10 text-primary"
                    : "text-foreground/50 hover:text-foreground/70 hover:bg-white/[0.02]"
                }`}
                style={{ paddingLeft: "40px" }}
              >
                <span
                  className="w-[4px] h-[4px] rounded-full shrink-0"
                  style={{ backgroundColor: color }}
                />
                <span className="truncate font-mono">{n.name}</span>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

export function ServiceSidebar({
  nodes,
  onSelectNode,
  selectedPath,
}: ServiceSidebarProps) {
  const [search, setSearch] = useState("");

  const grouped = useMemo(() => {
    const map = new Map<string, GraphNode[]>();
    for (const kind of KIND_ORDER) map.set(kind, []);
    for (const n of nodes) {
      const list = map.get(n.label);
      if (list) list.push(n);
      else {
        map.set(n.label, [n]);
      }
    }
    // Remove empty groups
    for (const [k, v] of map) {
      if (v.length === 0) map.delete(k);
    }
    return map;
  }, [nodes]);

  const filtered = useMemo(() => {
    if (!search) return null;
    const q = search.toLowerCase();
    return nodes
      .filter((n) => n.name.toLowerCase().includes(q))
      .slice(0, 50);
  }, [nodes, search]);

  return (
    <div className="flex flex-col flex-1 min-h-0">
      <div className="px-3 py-2.5 border-b border-border/30">
        <div className="relative">
          <input
            type="text"
            placeholder="Search services, topics..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full bg-white/[0.04] border border-white/[0.06] rounded-lg px-3 py-1.5 text-[12px] text-foreground placeholder-foreground/25 outline-none focus:border-primary/40 focus:bg-white/[0.06] transition-all"
          />
        </div>
      </div>

      <ScrollArea className="flex-1 min-h-0">
        <div className="py-1">
          {filtered ? (
            filtered.length === 0 ? (
              <p className="text-foreground/20 text-[12px] px-4 py-6 text-center">
                No matches
              </p>
            ) : (
              filtered.map((n) => (
                <button
                  key={n.id}
                  onClick={() => onSelectNode(n.name, new Set([n.id]))}
                  className="flex items-center gap-2 w-full text-left px-4 py-1.5 text-[11px] hover:bg-white/[0.03] transition-colors"
                >
                  <span
                    className="w-[5px] h-[5px] rounded-full shrink-0"
                    style={{ backgroundColor: n.color }}
                  />
                  <span className="text-foreground/60 truncate">{n.name}</span>
                  <span className="text-foreground/15 ml-auto text-[10px]">
                    {n.label}
                  </span>
                </button>
              ))
            )
          ) : (
            Array.from(grouped.entries()).map(([kind, kindNodes]) => (
              <KindSection
                key={kind}
                kind={kind}
                nodes={kindNodes.sort((a, b) => a.name.localeCompare(b.name))}
                onSelectNode={onSelectNode}
                selectedPath={selectedPath}
              />
            ))
          )}
        </div>
      </ScrollArea>

      {selectedPath && (
        <div className="px-3 py-2 border-t border-border/30">
          <button
            onClick={() => onSelectNode("", new Set())}
            className="w-full px-3 py-1.5 rounded-lg bg-white/[0.04] hover:bg-white/[0.07] text-[11px] text-foreground/40 font-medium transition-all"
          >
            Clear selection
          </button>
        </div>
      )}
    </div>
  );
}
