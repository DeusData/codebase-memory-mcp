import { useMemo, useState } from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import type { GraphNode } from "../lib/types";
import { useUiMessages } from "../lib/i18n";

interface SidebarProps {
  nodes: GraphNode[];
  onSelectPath: (path: string, nodeIds: Set<number>) => void;
  selectedPath: string | null;
}

interface DirNode {
  name: string;
  fullPath: string;
  children: Map<string, DirNode>;
  nodeIds: Set<number>;
  directNodes: GraphNode[];
}

function buildFileTree(nodes: GraphNode[]): DirNode {
  const root: DirNode = { name: "/", fullPath: "", children: new Map(), nodeIds: new Set(), directNodes: [] };
  for (const node of nodes) {
    if (!node.file_path) continue;
    const parts = node.file_path.split("/");
    let cur = root;
    for (let i = 0; i < parts.length - 1; i++) {
      if (!parts[i]) continue;
      let child = cur.children.get(parts[i]);
      if (!child) {
        const prefix = parts.slice(0, i + 1).join("/");
        child = { name: parts[i], fullPath: prefix, children: new Map(), nodeIds: new Set(), directNodes: [] };
        cur.children.set(parts[i], child);
      }
      cur = child;
    }
    cur.directNodes.push(node);
  }
  function collect(d: DirNode): Set<number> {
    const ids = new Set<number>();
    for (const n of d.directNodes) ids.add(n.id);
    for (const c of d.children.values()) for (const id of collect(c)) ids.add(id);
    d.nodeIds = ids;
    return ids;
  }
  collect(root);
  return root;
}

function flattenSingleChild(dir: DirNode): DirNode {
  const children = new Map<string, DirNode>();
  for (const [key, child] of dir.children) {
    let flat = flattenSingleChild(child);
    while (flat.children.size === 1 && flat.directNodes.length === 0) {
      const [sk, sc] = [...flat.children.entries()][0];
      flat = { ...sc, name: `${flat.name}/${sk}`, children: flattenSingleChild(sc).children };
    }
    children.set(key, flat);
  }
  return { ...dir, children };
}

function TreeItem({ dir, depth, onSelect, selectedPath }: {
  dir: DirNode; depth: number;
  onSelect: (path: string, ids: Set<number>) => void;
  selectedPath: string | null;
}) {
  const [expanded, setExpanded] = useState(false);
  const isSelected = selectedPath === dir.fullPath;
  const sorted = useMemo(() => [...dir.children.values()].sort((a, b) => a.name.localeCompare(b.name)), [dir.children]);
  const sortedNodes = useMemo(() => [...dir.directNodes].sort((a, b) => a.name.localeCompare(b.name)), [dir.directNodes]);

  return (
    <div>
      <button
        onClick={() => { setExpanded(!expanded); onSelect(dir.fullPath, dir.nodeIds); }}
        className={`flex items-center gap-1.5 w-full text-left px-3 py-[5px] text-[12px] transition-colors ${
          isSelected ? "bg-primary/10 text-primary" : "text-foreground/60 hover:text-foreground/80 hover:bg-white/[0.03]"
        }`}
        style={{ paddingLeft: `${depth * 16 + 12}px` }}
      >
        <span className="text-foreground/20 w-3 text-center text-[10px] shrink-0">
          {(dir.children.size > 0 || dir.directNodes.length > 0) ? (expanded ? "▾" : "▸") : ""}
        </span>
        <span className="truncate font-medium">{dir.name}</span>
        <span className="text-foreground/15 ml-auto text-[10px] tabular-nums shrink-0">{dir.nodeIds.size}</span>
      </button>
      {expanded && (
        <>
          {sorted.map((c) => <TreeItem key={c.fullPath} dir={c} depth={depth+1} onSelect={onSelect} selectedPath={selectedPath} />)}
          {sortedNodes.map((gn) => (
            <button
              key={gn.id}
              onClick={() => onSelect(dir.fullPath + "/" + gn.name, new Set([gn.id]))}
              className="flex items-center gap-1.5 w-full text-left px-3 py-[3px] text-[11px] text-foreground/40 hover:text-foreground/60 hover:bg-white/[0.02] transition-colors"
              style={{ paddingLeft: `${(depth+1) * 16 + 12}px` }}
            >
              <span className="w-[5px] h-[5px] rounded-full shrink-0" style={{ backgroundColor: gn.color }} />
              <span className="truncate font-mono">{gn.name}</span>
              <span className="text-foreground/10 ml-auto text-[10px] shrink-0">{gn.label}</span>
            </button>
          ))}
        </>
      )}
    </div>
  );
}

export function Sidebar({ nodes, onSelectPath, selectedPath }: SidebarProps) {
  const t = useUiMessages();
  const [search, setSearch] = useState("");
  const tree = useMemo(() => flattenSingleChild(buildFileTree(nodes)), [nodes]);

  const filtered = useMemo(() => {
    if (!search) return null;
    const q = search.toLowerCase();
    return nodes.filter((n) => n.name.toLowerCase().includes(q) || (n.file_path ?? "").toLowerCase().includes(q)).slice(0, 50);
  }, [nodes, search]);

  const topLevel = useMemo(() => [...tree.children.values()].sort((a, b) => a.name.localeCompare(b.name)), [tree.children]);

  return (
    <div className="flex flex-col flex-1 min-h-0">
      <div className="px-4 pt-3 pb-2 shrink-0">
        <span className="text-[11px] font-medium text-foreground/50 uppercase tracking-widest">
          {t.graph.folders}
        </span>
      </div>
      <div className="px-3 pb-2.5 border-b border-border/30 shrink-0">
        <div className="relative">
          <input
            type="text"
            placeholder={t.graph.search}
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
                {t.common.noMatches}
              </p>
            ) : (
              filtered.map((n) => (
                <button
                  key={n.id}
                  onClick={() => onSelectPath(n.file_path ?? "", new Set([n.id]))}
                  className="flex items-center gap-2 w-full text-left px-4 py-1.5 text-[11px] hover:bg-white/[0.03] transition-colors"
                >
                  <span className="w-[5px] h-[5px] rounded-full shrink-0" style={{ backgroundColor: n.color }} />
                  <span className="text-foreground/60 truncate">{n.name}</span>
                  <span className="text-foreground/15 ml-auto text-[10px] font-mono truncate max-w-[100px]">{n.file_path}</span>
                </button>
              ))
            )
          ) : (
            topLevel.map((c) => <TreeItem key={c.fullPath} dir={c} depth={0} onSelect={onSelectPath} selectedPath={selectedPath} />)
          )}
        </div>
      </ScrollArea>

      {selectedPath && (
        <div className="px-3 py-2 border-t border-border/30">
          <button
            onClick={() => onSelectPath("", new Set())}
            className="w-full px-3 py-1.5 rounded-lg bg-white/[0.04] hover:bg-white/[0.07] text-[11px] text-foreground/40 font-medium transition-all"
          >
            {t.graph.clearSelection}
          </button>
        </div>
      )}
    </div>
  );
}
