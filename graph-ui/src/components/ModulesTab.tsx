/* Modules — the territory: a squarified treemap of one folder's children,
 * sized by symbols, colored by dominant region, hatched when the indexer
 * missed files inside. Click a folder to drill, a file to list and open its
 * symbols. */
import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchTree, type TreeChild, type TreePayload } from "../lib/atlas";
import { fetchRegions } from "../hooks/useGraphData";
import { squarify } from "../lib/treemap";
import { disambiguateRegionNames } from "../lib/regions";
import { searchGraph, type SearchRow } from "../lib/atlas";
import type { RegionsPayload } from "../lib/types";

interface ModulesTabProps {
  project: string;
  path: string;
  onNavigatePath: (path: string) => void;
  onOpenSymbol: (ref: { id?: number; qn?: string }) => void;
  onOpenRegion: (regionId: number) => void;
}

const NEUTRAL = "#39505f";

/* search_graph rows for one file, via the read-only /rpc surface. */
async function fetchFileSymbols(project: string, file: string): Promise<SearchRow[]> {
  const { rows } = await searchGraph(project, { file_pattern: file, limit: 200 });
  return rows;
}

export function ModulesTab({
  project,
  path,
  onNavigatePath,
  onOpenSymbol,
  onOpenRegion,
}: ModulesTabProps) {
  const [tree, setTree] = useState<TreePayload | null>(null);
  const [regions, setRegions] = useState<RegionsPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedFile, setSelectedFile] = useState<TreeChild | null>(null);
  const [fileSymbols, setFileSymbols] = useState<SearchRow[] | null>(null);
  const [size, setSize] = useState({ w: 900, h: 560 });

  useEffect(() => {
    let cancelled = false;
    setTree(null);
    setError(null);
    setSelectedFile(null);
    setFileSymbols(null);
    fetchTree(project, path)
      .then((payload) => {
        if (!cancelled) setTree(payload);
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "failed");
      });
    return () => {
      cancelled = true;
    };
  }, [project, path]);

  useEffect(() => {
    let cancelled = false;
    fetchRegions(project)
      .then((payload) => {
        if (!cancelled) setRegions(payload);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [project]);

  const regionMeta = useMemo(() => {
    const map = new Map<number, { name: string; color: string }>();
    if (regions)
      for (const region of disambiguateRegionNames(regions.regions))
        map.set(region.id, { name: region.name, color: region.color });
    return map;
  }, [regions]);

  const rects = useMemo(() => {
    if (!tree) return [];
    const items = tree.children.map((child) => ({
      id: `${child.kind}:${child.path}`,
      value: Math.max(1, child.symbols),
    }));
    return squarify(items, 0, 0, size.w, size.h);
  }, [tree, size]);

  const childByKey = useMemo(() => {
    const map = new Map<string, TreeChild>();
    if (tree)
      for (const child of tree.children) map.set(`${child.kind}:${child.path}`, child);
    return map;
  }, [tree]);

  const openFile = useCallback(
    (child: TreeChild) => {
      setSelectedFile(child);
      setFileSymbols(null);
      fetchFileSymbols(project, child.path)
        .then(setFileSymbols)
        .catch(() => setFileSymbols([]));
    },
    [project],
  );

  const [viewMode, setViewMode] = useState<"map" | "bars">(() => {
    try {
      return localStorage.getItem("cbm-modules-view") === "bars" ? "bars" : "map";
    } catch {
      return "map";
    }
  });
  const switchView = (mode: "map" | "bars") => {
    setViewMode(mode);
    try {
      localStorage.setItem("cbm-modules-view", mode);
    } catch {
      /* ignore */
    }
  };

  const crumbs = useMemo(() => {
    const parts = path ? path.split("/") : [];
    return [{ label: "root", path: "" }].concat(
      parts.map((part, index) => ({
        label: part,
        path: parts.slice(0, index + 1).join("/"),
      })),
    );
  }, [path]);

  const measureRef = useCallback((element: HTMLDivElement | null) => {
    if (!element) return;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const rect = entry.contentRect;
        if (rect.width > 100 && rect.height > 100)
          setSize({ w: rect.width, h: rect.height });
      }
    });
    observer.observe(element);
  }, []);

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-red-400/80 text-sm">{error}</p>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      {/* Breadcrumbs */}
      <div className="flex items-center gap-1 px-5 py-2.5 border-b border-border/30 shrink-0 flex-wrap">
        {crumbs.map((crumb, index) => (
          <span key={crumb.path} className="flex items-center gap-1">
            {index > 0 && <span className="text-foreground/35 text-[13px]">/</span>}
            <button
              onClick={() => onNavigatePath(crumb.path)}
              className={`text-[12px] transition-colors ${
                index === crumbs.length - 1
                  ? "text-foreground/80 font-medium"
                  : "text-foreground/40 hover:text-primary"
              }`}
            >
              {crumb.label}
            </button>
          </span>
        ))}
        <div className="ml-auto flex items-center gap-0.5 bg-popover rounded-md p-0.5 border border-border/40">
          {(["map", "bars"] as const).map((mode) => (
            <button
              key={mode}
              onClick={() => switchView(mode)}
              className={`px-2 py-0.5 rounded text-[12px] transition-colors ${
                viewMode === mode
                  ? "bg-primary/20 text-primary"
                  : "text-foreground/45 hover:text-foreground/70"
              }`}
            >
              {mode}
            </button>
          ))}
        </div>
        {tree && (
          <span className="text-[13px] text-foreground/45 font-mono">
            {tree.files.toLocaleString("en-US")} files ·{" "}
            {tree.symbols.toLocaleString("en-US")} symbols
            {tree.children_dropped ? ` · ${tree.children_dropped} children not shown` : ""}
          </span>
        )}
      </div>

      <div className="flex-1 min-h-0 flex">
        {/* Treemap */}
        <div ref={measureRef} className="flex-1 min-w-0 relative m-3">
          {!tree ? (
            <div className="flex items-center justify-center h-full">
              <p className="text-foreground/45 text-sm">Measuring the territory…</p>
            </div>
          ) : viewMode === "bars" ? (
            <div className="h-full overflow-y-auto pr-2">
              {/* Bars are scaled to the largest sibling, not the whole —
               * without saying so, they read as share-of-total. */}
              <p className="text-[12px] text-foreground/35 px-2 pb-1">
                bar length: relative to largest module
              </p>
              {[...tree.children]
                .sort((a, b) => b.symbols - a.symbols)
                .map((child) => {
                  const meta =
                    child.region !== undefined ? regionMeta.get(child.region) : undefined;
                  const max = Math.max(...tree.children.map((c) => c.symbols), 1);
                  return (
                    <button
                      key={child.path}
                      onClick={() =>
                        child.kind === "dir" ? onNavigatePath(child.path) : openFile(child)
                      }
                      className="flex items-center gap-2 w-full text-left px-2 py-[5px] rounded-md hover:bg-surface-3 transition-colors group"
                    >
                      <span
                        className="w-[7px] h-[7px] rounded-full shrink-0"
                        style={{ backgroundColor: meta?.color ?? NEUTRAL }}
                      />
                      <span className="text-[13px] font-mono text-foreground/70 group-hover:text-foreground/90 truncate w-[220px] shrink-0 transition-colors">
                        {child.name}
                        {child.kind === "dir" ? "/" : ""}
                      </span>
                      <span className="flex-1 h-[10px] bg-surface-3/60 rounded-sm overflow-hidden">
                        <span
                          className="block h-full rounded-sm"
                          style={{
                            width: `${Math.max(1, (child.symbols / max) * 100)}%`,
                            backgroundColor: meta?.color ?? NEUTRAL,
                            opacity: 0.65,
                          }}
                        />
                      </span>
                      <span className="text-[12px] tabular-nums text-foreground/40 shrink-0 w-[70px] text-right">
                        {child.symbols.toLocaleString("en-US")}
                      </span>
                    </button>
                  );
                })}
            </div>
          ) : (
            <svg
              width="100%"
              height="100%"
              viewBox={`0 0 ${size.w} ${size.h}`}
              className="rounded-md"
              role="img"
              aria-label={`Treemap of ${path || "the repository root"}`}
            >
              <defs>
                <pattern id="missed-hatch" width="7" height="7" patternTransform="rotate(45)" patternUnits="userSpaceOnUse">
                  <rect width="7" height="7" fill="transparent" />
                  <line x1="0" y1="0" x2="0" y2="7" stroke="#e9eef5" strokeOpacity="0.25" strokeWidth="2" />
                </pattern>
              </defs>
              {rects.map((rect) => {
                const child = childByKey.get(rect.id);
                if (!child) return null;
                const meta = child.region !== undefined ? regionMeta.get(child.region) : undefined;
                const fill = meta?.color ?? NEUTRAL;
                const showLabel = rect.w > 60 && rect.h > 24;
                return (
                  <g
                    key={rect.id}
                    onClick={() =>
                      child.kind === "dir" ? onNavigatePath(child.path) : openFile(child)
                    }
                    className="cursor-pointer"
                  >
                    <rect
                      x={rect.x + 1}
                      y={rect.y + 1}
                      width={Math.max(0, rect.w - 2)}
                      height={Math.max(0, rect.h - 2)}
                      rx={3}
                      fill={fill}
                      fillOpacity={child.kind === "dir" ? 0.32 : 0.18}
                      stroke={fill}
                      strokeOpacity={0.55}
                    >
                      <title>
                        {`${child.path} — ${child.symbols.toLocaleString("en-US")} symbols` +
                          (child.kind === "dir" ? `, ${child.files} files` : "") +
                          (meta ? `\nregion: ${meta.name}` : "") +
                          (child.missed ? `\n${child.missed} file(s) not fully indexed` : "")}
                      </title>
                    </rect>
                    {child.missed ? (
                      <rect
                        x={rect.x + 1}
                        y={rect.y + 1}
                        width={Math.max(0, rect.w - 2)}
                        height={Math.max(0, rect.h - 2)}
                        rx={3}
                        fill="url(#missed-hatch)"
                        pointerEvents="none"
                      />
                    ) : null}
                    {showLabel && (
                      <text
                        x={rect.x + 8}
                        y={rect.y + 17}
                        fill="#e4e4ed"
                        fillOpacity={0.85}
                        fontSize={11.5}
                        fontFamily="ui-monospace, monospace"
                        pointerEvents="none"
                      >
                        {child.kind === "dir" ? `${child.name}/` : child.name}
                      </text>
                    )}
                    {showLabel && rect.h > 40 && (
                      <text
                        x={rect.x + 8}
                        y={rect.y + 31}
                        fill="#e4e4ed"
                        fillOpacity={0.35}
                        fontSize={9.5}
                        fontFamily="ui-monospace, monospace"
                        pointerEvents="none"
                      >
                        {child.symbols.toLocaleString("en-US")}
                      </text>
                    )}
                  </g>
                );
              })}
            </svg>
          )}
        </div>

        {/* File panel */}
        {selectedFile && (
          <div className="w-[320px] shrink-0 border-l border-border/40 bg-card/90 flex flex-col">
            <div className="px-4 py-3 border-b border-border/30">
              <div className="flex items-start justify-between gap-2">
                <p className="text-[12px] font-mono text-foreground/80 break-all">
                  {selectedFile.path}
                </p>
                <button
                  onClick={() => setSelectedFile(null)}
                  className="text-foreground/40 hover:text-foreground/60 text-[15px] leading-none"
                >
                  ×
                </button>
              </div>
              <p className="text-[12px] text-foreground/45 mt-1">
                {selectedFile.symbols.toLocaleString("en-US")} symbols
                {selectedFile.missed ? " · not fully indexed" : ""}
              </p>
            </div>
            <div className="flex-1 min-h-0 overflow-y-auto py-1">
              {fileSymbols === null ? (
                <p className="text-[13px] text-foreground/40 px-4 py-3">Loading…</p>
              ) : fileSymbols.length === 0 ? (
                <p className="text-[13px] text-foreground/40 px-4 py-3">No symbols found.</p>
              ) : (
                fileSymbols.map((symbol) => (
                  <button
                    key={symbol.qualified_name}
                    onClick={() => onOpenSymbol({ qn: symbol.qualified_name })}
                    className="flex items-center gap-2 w-full text-left px-4 py-[4px] text-[13px] hover:bg-surface-3 transition-colors group"
                  >
                    <span className="font-mono text-foreground/60 group-hover:text-primary truncate transition-colors">
                      {symbol.name}
                    </span>
                    <span className="text-foreground/35 text-[12px] ml-auto shrink-0">
                      {symbol.label}
                    </span>
                  </button>
                ))
              )}
            </div>
            {selectedFile.region !== undefined && regionMeta.get(selectedFile.region) && (
              <button
                onClick={() => onOpenRegion(selectedFile.region!)}
                className="px-4 py-2.5 border-t border-border/30 text-left text-[13px] text-primary/70 hover:text-primary transition-colors"
              >
                region: {regionMeta.get(selectedFile.region)!.name} — view in galaxy →
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
