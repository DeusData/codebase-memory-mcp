import { useEffect, useState, useCallback, useMemo, useRef } from "react";
import { Button } from "@/components/ui/button";
import {
  useGraphData,
  fetchRegions,
  clampNodeBudget,
  GRAPH_RENDER_NODE_LIMIT,
  GRAPH_NODE_BUDGET_STEP,
  GRAPH_NODE_BUDGET_MAX,
} from "../hooks/useGraphData";
import { regionsToGraphData, regionsViewWorthwhile, disambiguateRegionNames, isTestRegion } from "../lib/regions";
import { fetchScent } from "../lib/atlas";
import { RegionTooltip } from "./RegionTooltip";
import { NodeTooltip } from "./NodeTooltip";
import { Minimap } from "./Minimap";
import { RegionPanel } from "./RegionPanel";
import { GraphLoader } from "./GraphLoader";
import { DisplaySettingsMenu } from "./DisplaySettingsMenu";
import {
  loadDisplaySettings,
  saveDisplaySettings,
  type DisplaySettings,
} from "../lib/density";
import {
  GraphScene,
  computeCameraTarget,
  type CameraTarget,
} from "./GraphScene";
import { Sidebar } from "./Sidebar";
import { FilterPanel } from "./FilterPanel";
import { NodeDetailPanel } from "./NodeDetailPanel";
import { MissedCallout } from "./MissedCallout";
import { ResizeHandle } from "./ResizeHandle";
import { ErrorBoundary } from "./ErrorBoundary";
import type { GraphNode, GraphData, Region, RegionsPayload, RepoInfo } from "../lib/types";
import { colorForStatus } from "../lib/colors";

/* One-time in-place orientation hint for the region scene. Dismissing
 * persists; navigating away without dismissing does not (skip != dismiss). */
function GalaxyHint() {
  const [dismissed, setDismissed] = useState(() => {
    try {
      return localStorage.getItem("cbm-hint-galaxy") === "1";
    } catch {
      return true;
    }
  });
  if (dismissed) return null;
  return (
    <div className="absolute bottom-4 left-4 bg-card/90 backdrop-blur border border-border/50 rounded-md px-3 py-2.5 max-w-[300px] shadow-lg">
      <p className="text-[13px] text-foreground/70 leading-relaxed">
        Each sphere is a region of related code. Double-click one to enter it,
        type above to light up matches, and use ⌂ on the map to fit everything.
      </p>
      <button
        onClick={() => {
          try {
            localStorage.setItem("cbm-hint-galaxy", "1");
          } catch {
            /* ignore */
          }
          setDismissed(true);
        }}
        className="mt-1.5 text-[12px] text-primary hover:text-primary/80 transition-colors"
      >
        Got it
      </button>
    </div>
  );
}

/* Persist panel widths */
function loadWidth(key: string, fallback: number): number {
  try {
    const v = localStorage.getItem(key);
    if (v) return Math.max(150, Math.min(600, parseInt(v, 10)));
  } catch { /* ignore */ }
  return fallback;
}
function saveWidth(key: string, value: number) {
  try { localStorage.setItem(key, String(Math.round(value))); } catch { /* ignore */ }
}

/* Persist the node budget per project */
function budgetKey(project: string): string {
  return `cbm-node-budget:${project}`;
}
function loadNodeBudget(project: string): number {
  try {
    const v = localStorage.getItem(budgetKey(project));
    if (v) return clampNodeBudget(parseInt(v, 10));
  } catch { /* ignore */ }
  return GRAPH_RENDER_NODE_LIMIT;
}
function saveNodeBudget(project: string, value: number) {
  try { localStorage.setItem(budgetKey(project), String(value)); } catch { /* ignore */ }
}

interface GraphTabProps {
  /* False while the tab is hidden-but-mounted: the render loop pauses. */
  active?: boolean;
  project: string | null;
  /* Deep-link state from the URL (?node=&region=) and the reporter back. */
  routeNode?: string | null;
  routeRegion?: string | null;
  onRouteChange?: (node: string | null, region: string | null) => void;
}

/* The galaxy's level-of-detail state: the region scene (coarsest), one
 * opened region (full detail, scoped), or the classic full galaxy. */
type AtlasView =
  | { kind: "deciding" }
  | { kind: "regions" }
  | { kind: "region"; region: Region }
  | { kind: "full" };

export function formatGraphLimitNotice(data: GraphData | null): string | null {
  if (!data || data.total_nodes <= data.nodes.length) return null;
  return `Showing ${data.nodes.length.toLocaleString("en-US")} of ${data.total_nodes.toLocaleString("en-US")} nodes (${data.edges.length.toLocaleString("en-US")} edges). Raise the node budget or use filters.`;
}

export function GraphTab({
  active = true,
  project,
  routeNode = null,
  routeRegion = null,
  onRouteChange,
}: GraphTabProps) {
  const { data, loading, error, progress, fetchOverview } = useGraphData();
  const [view, setView] = useState<AtlasView>({ kind: "deciding" });
  const viewRef = useRef<AtlasView>(view);
  viewRef.current = view;
  const [regionsPayload, setRegionsPayload] = useState<RegionsPayload | null>(null);
  const [regionsError, setRegionsError] = useState<string | null>(null);
  const [selectedRegion, setSelectedRegion] = useState<Region | null>(null);
  const [highlightedIds, setHighlightedIds] = useState<Set<number> | null>(null);
  const [selectedPath, setSelectedPath] = useState<string | null>(null);
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null);
  const [cameraTarget, setCameraTarget] = useState<CameraTarget | null>(null);
  const [viewTarget, setViewTarget] = useState<{ x: number; y: number } | null>(null);
  const [repoInfo, setRepoInfo] = useState<RepoInfo | null>(null);
  const [showLabels, setShowLabels] = useState(true);
  const [display, setDisplay] = useState<DisplaySettings>(() =>
    loadDisplaySettings(),
  );
  const updateDisplay = useCallback((next: DisplaySettings) => {
    setDisplay(next);
    saveDisplaySettings(next);
  }, []);
  const [leftWidth, setLeftWidth] = useState(() => loadWidth("cbm-left-w", 260));
  const [rightWidth, setRightWidth] = useState(() => loadWidth("cbm-right-w", 280));
  const limitNotice = formatGraphLimitNotice(data);

  /* Node budget — keyed to its project so switching projects re-reads the
   * persisted value and triggers exactly one fetch. */
  const [budget, setBudget] = useState<{ project: string | null; value: number }>(
    { project: null, value: GRAPH_RENDER_NODE_LIMIT },
  );
  const [budgetDraft, setBudgetDraft] = useState(String(GRAPH_RENDER_NODE_LIMIT));

  const commitBudget = useCallback(() => {
    const parsed = clampNodeBudget(parseInt(budgetDraft, 10));
    setBudgetDraft(String(parsed));
    if (project && parsed !== budget.value) {
      saveNodeBudget(project, parsed);
      setBudget({ project, value: parsed });
    }
  }, [budgetDraft, project, budget.value]);

  /* Filter state — all enabled by default */
  const [enabledLabels, setEnabledLabels] = useState<Set<string>>(new Set());
  const [enabledEdgeTypes, setEnabledEdgeTypes] = useState<Set<string>>(new Set());

  /* Missed skeleton (#963): the file structure of files the indexer could
   * not fully cover, shown as a white satellite cluster beside the code
   * galaxy. Toggle only hides/shows it — the data rides along with every
   * code-graph layout. */
  const [showMissedSkeleton, setShowMissedSkeleton] = useState(true);

  /* Dead-code view: recolor by status + status-based filters */
  const [deadCodeView, setDeadCodeView] = useState(false);
  const [showOnlyDead, setShowOnlyDead] = useState(false);
  const [hideEntryPoints, setHideEntryPoints] = useState(false);
  const [hideTests, setHideTests] = useState(false);

  /* Initialize filters when data loads */
  useEffect(() => {
    if (!data) return;
    const labels = new Set(data.nodes.map((n) => n.label));
    const types = new Set(data.edges.map((e) => e.type));
    for (const lp of data.linked_projects ?? []) {
      for (const n of lp.nodes) labels.add(n.label);
      for (const e of lp.edges) types.add(e.type);
      for (const e of lp.cross_edges) types.add(e.type);
    }
    setEnabledLabels(labels);
    setEnabledEdgeTypes(types);
  }, [data]);

  /* Compute filtered data */
  const filteredData: GraphData | null = useMemo(() => {
    if (!data) return null;

    /* Status-based filters (dead-code view) */
    const statusOk = (n: GraphNode) => {
      if (showOnlyDead && n.status !== "dead") return false;
      if (hideEntryPoints && n.status === "entry") return false;
      if (hideTests && n.status === "test") return false;
      return true;
    };
    /* Recolor by status when the dead-code view is on */
    const paint = (n: GraphNode): GraphNode =>
      deadCodeView ? { ...n, color: colorForStatus(n.status) } : n;
    const keep = (n: GraphNode) => enabledLabels.has(n.label) && statusOk(n);

    const nodes = data.nodes.filter(keep).map(paint);
    const nodeIds = new Set(nodes.map((n) => n.id));
    const edges = data.edges.filter(
      (e) =>
        enabledEdgeTypes.has(e.type) &&
        nodeIds.has(e.source) &&
        nodeIds.has(e.target),
    );

    const linked_projects = data.linked_projects?.map((lp) => {
      const lpNodes = lp.nodes.filter(keep).map(paint);
      const lpIds = new Set(lpNodes.map((n) => n.id));
      const lpEdges = lp.edges.filter(
        (e) =>
          enabledEdgeTypes.has(e.type) && lpIds.has(e.source) && lpIds.has(e.target),
      );
      const crossEdges = lp.cross_edges.filter(
        (e) =>
          enabledEdgeTypes.has(e.type) && nodeIds.has(e.source) && lpIds.has(e.target),
      );
      return { ...lp, nodes: lpNodes, edges: lpEdges, cross_edges: crossEdges };
    });

    return { nodes, edges, total_nodes: data.total_nodes, linked_projects };
  }, [
    data,
    enabledLabels,
    enabledEdgeTypes,
    deadCodeView,
    showOnlyDead,
    hideEntryPoints,
    hideTests,
  ]);

  /* Re-read the persisted budget when the project changes… */
  useEffect(() => {
    if (project) {
      const value = loadNodeBudget(project);
      setBudget({ project, value });
      setBudgetDraft(String(value));
    }
  }, [project]);

  /* …and decide the level of detail once budget and project agree. The
   * region scene is the default above REGIONS_MIN_TOTAL_NODES; small
   * projects load the full galaxy directly. An open region survives budget
   * changes (the ref carries the current view across this effect). */
  useEffect(() => {
    if (!project || budget.project !== project) return;
    let cancelled = false;
    setHighlightedIds(null);
    setSelectedPath(null);
    const current = viewRef.current;
    if (current.kind === "region") {
      fetchOverview(project, budget.value, "code", `region:${current.region.id}`);
      return;
    }
    if (current.kind === "full") {
      fetchOverview(project, budget.value);
      return;
    }
    setView({ kind: "deciding" });
    setRegionsError(null);
    (async () => {
      try {
        const payload = await fetchRegions(project);
        if (cancelled) return;
        setRegionsPayload(payload);
        const restored = routeRegion
          ? payload.regions.find((r) => String(r.id) === routeRegion)
          : null;
        if (restored) {
          setView({ kind: "region", region: restored });
          fetchOverview(project, budget.value, "code", `region:${restored.id}`);
        } else if (regionsViewWorthwhile(payload)) {
          setView({ kind: "regions" });
        } else {
          setView({ kind: "full" });
          fetchOverview(project, budget.value);
        }
      } catch (e) {
        if (cancelled) return;
        /* Older servers have no region level — fall back to the galaxy. */
        setRegionsError(e instanceof Error ? e.message : "regions unavailable");
        setView({ kind: "full" });
        fetchOverview(project, budget.value);
      }
    })();
    return () => {
      cancelled = true;
    };
    /* routeRegion is a first-load restore input, not a live dependency. */
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [project, budget, fetchOverview]);

  /* Deep link: once data is loaded, restore the node the URL names. */
  useEffect(() => {
    if (!routeNode || !data || selectedNode) return;
    const target = data.nodes.find((n) => String(n.id) === routeNode);
    if (target) handleNodeClickRef.current?.(target);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [routeNode, data]);

  /* Missed skeleton: offset into place and paint white — a ghost of the
   * files the graph could not fully cover, sitting beside the galaxy. */
  const missedSkeleton = useMemo(() => {
    const mg = data?.missed_graph;
    if (!mg || mg.nodes.length === 0) return null;
    const nodes = mg.nodes.map((n) => ({
      ...n,
      x: n.x + mg.offset.x,
      y: n.y + mg.offset.y,
      z: n.z + mg.offset.z,
      color: "#e9eef5",
    }));
    return { nodes, edges: mg.edges, ids: new Set(nodes.map((n) => n.id)) };
  }, [data]);

  /* Overview framing: both clusters (galaxy + skeleton) in one shot. */
  const overviewTarget = useMemo(() => {
    if (!data) return null;
    const all = missedSkeleton ? [...data.nodes, ...missedSkeleton.nodes] : data.nodes;
    return computeCameraTarget(all, new Set(all.map((n) => n.id)));
  }, [data, missedSkeleton]);

  /* Auto-frame the scene on load (skeleton or not): the fixed default
   * camera lands inside larger clouds. Deep-linked nodes keep their own
   * focus fly-to instead. */
  useEffect(() => {
    if (overviewTarget && !routeNode) {
      setCameraTarget(overviewTarget);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [overviewTarget]);

  /* Clicking empty space while the skeleton has focus flies back to the
   * overview (the galaxy may be entirely off-screen at that point, so there
   * is no code node to click). No-op during normal galaxy exploration. */
  const handleBackgroundClick = useCallback(() => {
    if (selectedNode && missedSkeleton?.ids.has(selectedNode.id) && overviewTarget) {
      setSelectedNode(null);
      setHighlightedIds(null);
      setSelectedPath(null);
      setCameraTarget(overviewTarget);
    }
  }, [selectedNode, missedSkeleton, overviewTarget]);

  /* Fetch git remote metadata for GitHub deep-links */
  useEffect(() => {
    if (!project) {
      setRepoInfo(null);
      return;
    }
    let cancelled = false;
    fetch(`/api/repo-info?project=${encodeURIComponent(project)}`)
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => {
        if (!cancelled && d && !d.error) setRepoInfo(d as RepoInfo);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [project]);

  /* Search scent: a query is answerable from the coarsest view — matched
   * regions brighten and carry hit counts before any drill. */
  const [scentQuery, setScentQuery] = useState("");
  const [scent, setScent] = useState<{
    total: number;
    unmapped: number;
    counts: Map<number, number>;
  } | null>(null);
  useEffect(() => {
    setScentQuery("");
    setScent(null);
  }, [project]);
  useEffect(() => {
    const q = scentQuery.trim();
    if (!project || q.length < 2) {
      setScent(null);
      return;
    }
    let cancelled = false;
    const timer = setTimeout(async () => {
      try {
        const payload = await fetchScent(project, q);
        if (cancelled) return;
        setScent({
          total: payload.total,
          unmapped: payload.unmapped,
          counts: new Map(payload.regions.map((row) => [row.region, row.count])),
        });
      } catch {
        if (!cancelled) setScent(null);
      }
    }, 300);
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [project, scentQuery]);

  const handleNodeClick = useCallback(
    (node: GraphNode) => {
      if (!filteredData) return;

      /* Clicking the missed skeleton re-centers the camera on that whole
       * cluster (it's small — the natural focus unit is the skeleton, not a
       * single node); clicking any code node flies back to the code galaxy
       * via the normal per-node focus below. */
      if (missedSkeleton?.ids.has(node.id)) {
        setSelectedNode(node);
        setHighlightedIds(null);
        setSelectedPath(node.file_path ?? null);
        setCameraTarget(computeCameraTarget(missedSkeleton.nodes, missedSkeleton.ids));
        return;
      }

      setSelectedNode(node);

      /* Highlight the node and its direct connections */
      const connectedIds = new Set([node.id]);
      for (const edge of filteredData.edges) {
        if (edge.source === node.id) connectedIds.add(edge.target);
        if (edge.target === node.id) connectedIds.add(edge.source);
      }
      setHighlightedIds(connectedIds);
      setSelectedPath(node.file_path ?? null);
      setCameraTarget(computeCameraTarget(filteredData.nodes, connectedIds));
      onRouteChange?.(
        String(node.id),
        viewRef.current.kind === "region" ? String(viewRef.current.region.id) : null,
      );
    },
    [filteredData, missedSkeleton, onRouteChange],
  );
  const handleNodeClickRef = useRef<typeof handleNodeClick | null>(null);
  handleNodeClickRef.current = handleNodeClick;

  /* Region scene interactions. */
  const regionGraph = useMemo(
    () => (regionsPayload ? regionsToGraphData(regionsPayload) : null),
    [regionsPayload],
  );
  const displayRegions = useMemo(
    () => (regionsPayload ? disambiguateRegionNames(regionsPayload.regions) : []),
    [regionsPayload],
  );
  const openRegion = useCallback(
    (region: Region) => {
      if (!project) return;
      setSelectedRegion(null);
      setSelectedNode(null);
      setHighlightedIds(null);
      setSelectedPath(null);
      setCameraTarget(null);
      setView({ kind: "region", region });
      fetchOverview(project, budget.value, "code", `region:${region.id}`);
      onRouteChange?.(null, String(region.id));
    },
    [project, budget.value, fetchOverview, onRouteChange],
  );
  const backToRegions = useCallback(() => {
    setSelectedRegion(null);
    setSelectedNode(null);
    setHighlightedIds(null);
    setSelectedPath(null);
    setCameraTarget(null);
    setView({ kind: "regions" });
    onRouteChange?.(null, null);
  }, [onRouteChange]);
  const loadFullGalaxy = useCallback(() => {
    if (!project) return;
    setSelectedRegion(null);
    setView({ kind: "full" });
    fetchOverview(project, budget.value);
    onRouteChange?.(null, null);
  }, [project, budget.value, fetchOverview, onRouteChange]);

  const handleNavigateToNode = useCallback(
    (node: GraphNode) => {
      handleNodeClick(node);
    },
    [handleNodeClick],
  );

  const handleSelectPath = useCallback(
    (path: string, nodeIds: Set<number>, node?: GraphNode) => {
      if (node) {
        handleNodeClick(node);
        return;
      }

      if (!filteredData || !path || nodeIds.size === 0) {
        setHighlightedIds(null);
        setSelectedPath(null);
        setSelectedNode(null);
        setCameraTarget(null);
        return;
      }

      setSelectedNode(null);
      setSelectedPath(path);
      setHighlightedIds(nodeIds);
      setCameraTarget(computeCameraTarget(filteredData.nodes, nodeIds));
    },
    [filteredData, handleNodeClick],
  );

  const toggleLabel = useCallback((label: string) => {
    setEnabledLabels((prev) => {
      const next = new Set(prev);
      if (next.has(label)) next.delete(label);
      else next.add(label);
      return next;
    });
  }, []);

  const toggleEdgeType = useCallback((type: string) => {
    setEnabledEdgeTypes((prev) => {
      const next = new Set(prev);
      if (next.has(type)) next.delete(type);
      else next.add(type);
      return next;
    });
  }, []);

  const enableAll = useCallback(() => {
    if (!data) return;
    const labels = new Set(data.nodes.map((n) => n.label));
    const types = new Set(data.edges.map((e) => e.type));
    for (const lp of data.linked_projects ?? []) {
      for (const n of lp.nodes) labels.add(n.label);
      for (const e of lp.edges) types.add(e.type);
      for (const e of lp.cross_edges) types.add(e.type);
    }
    setEnabledLabels(labels);
    setEnabledEdgeTypes(types);
  }, [data]);

  const disableAll = useCallback(() => {
    setEnabledLabels(new Set());
    setEnabledEdgeTypes(new Set());
  }, []);

  if (!project) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-foreground/45 text-sm">
          Select a project from the Projects tab
        </p>
      </div>
    );
  }

  if (view.kind === "deciding") {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-foreground/45 text-sm">Mapping regions…</p>
      </div>
    );
  }

  /* ── Region scene: the coarsest level of detail ──────────────── */
  if (view.kind === "regions" && regionGraph) {
    const selected = selectedRegion;
    return (
      <div className="h-full flex">
        <div
          className="border-r border-border/30 flex flex-col h-full bg-card/90 backdrop-blur-md shrink-0"
          style={{ width: leftWidth }}
        >
          <div className="px-4 pt-3 pb-2 shrink-0">
            <span className="text-[13px] font-medium text-foreground/50 uppercase tracking-widest">
              Regions
            </span>
            <p className="text-[12px] text-foreground/40 mt-1">
              {regionsPayload?.method === "leiden+folders"
                ? "call communities + folder groups"
                : "folder groups"}
            </p>
            <input
              type="text"
              value={scentQuery}
              onChange={(e) => setScentQuery(e.target.value)}
              placeholder="find a symbol…"
              className="mt-2 w-full bg-popover border border-border/50 rounded-md px-2 py-1 text-[13px] text-foreground placeholder-foreground/30 outline-none focus:border-primary/50 transition-all"
            />
            {scent && (
              <p className="text-[12px] text-primary/70 mt-1 tabular-nums">
                {scent.total.toLocaleString("en-US")} match
                {scent.total === 1 ? "" : "es"}
                {scent.unmapped > 0 && ` · ${scent.unmapped} outside regions`}
              </p>
            )}
          </div>
          <div className="flex-1 min-h-0 overflow-y-auto py-1">
            {[...displayRegions]
              .sort((a, b) => Number(isTestRegion(a.name)) - Number(isTestRegion(b.name)))
              .map((region) => (
              <button
                key={region.id}
                onClick={() => setSelectedRegion(region)}
                onDoubleClick={() => openRegion(region)}
                className={`flex items-center gap-2 w-full text-left px-4 py-[5px] text-[12px] transition-colors ${
                  selected?.id === region.id
                    ? "bg-primary/10 text-primary"
                    : "text-foreground/60 hover:text-foreground/80 hover:bg-surface-3"
                }`}
              >
                <span
                  className="w-[7px] h-[7px] rounded-full shrink-0"
                  style={{ backgroundColor: region.color }}
                />
                <span className="truncate">{region.name}</span>
                {(scent?.counts.get(region.id) ?? 0) > 0 && (
                  <span className="text-primary text-[11px] tabular-nums shrink-0 bg-primary/15 rounded-full px-1.5">
                    {scent?.counts.get(region.id)}
                  </span>
                )}
                <span className="text-foreground/30 ml-auto text-[12px] tabular-nums shrink-0">
                  {region.members.toLocaleString("en-US")}
                </span>
              </button>
            ))}
          </div>
        </div>
        <ResizeHandle
          side="left"
          onResize={(d) => {
            setLeftWidth((w) => {
              const nw = Math.max(150, Math.min(500, w + d));
              saveWidth("cbm-left-w", nw);
              return nw;
            });
          }}
        />

        <div className="flex-1 relative overflow-hidden">
          <ErrorBoundary>
            <GraphScene
              active={active}
              data={regionGraph}
              missed={null}
              highlightedIds={
                scent && scent.counts.size > 0
                  ? new Set(scent.counts.keys())
                  : selected
                    ? new Set([selected.id])
                    : null
              }
              cameraTarget={cameraTarget}
              showLabels={true}
              display={display}
              onNodeClick={(node) => {
                const region = displayRegions.find((r) => r.id === node.id);
                if (region) {
                  setSelectedRegion(region);
                  setCameraTarget(
                    computeCameraTarget(regionGraph.nodes, new Set([node.id])),
                  );
                }
              }}
              onBackgroundClick={() => setSelectedRegion(null)}
              renderTooltip={(node) => {
                const region = displayRegions.find((r) => r.id === node.id);
                return region ? (
                  <RegionTooltip region={region} node={node} />
                ) : (
                  <NodeTooltip node={node} />
                );
              }}
              onViewTarget={(x, y) => setViewTarget({ x, y })}
              onApproachNode={(node) => {
                const region = displayRegions.find((r) => r.id === node.id);
                if (region) openRegion(region);
              }}
            />
          </ErrorBoundary>

          <Minimap
            regions={displayRegions}
            openRegionId={null}
            scentCounts={scent?.counts ?? null}
            viewTarget={viewTarget}
            onOpen={openRegion}
            onHome={() =>
              setCameraTarget(
                computeCameraTarget(
                  regionGraph.nodes,
                  new Set(regionGraph.nodes.map((n) => n.id)),
                ),
              )
            }
          />

          <div className="absolute top-4 left-4 text-[13px] text-foreground/55 pointer-events-none tabular-nums">
            <p>
              {displayRegions.length} regions /{" "}
              {regionsPayload?.total_nodes.toLocaleString("en-US")} nodes
            </p>
            <p className="text-foreground/35 mt-0.5">
              double-click a region — or just zoom into it — to open it
            </p>
          </div>

          <GalaxyHint />

          <div className="absolute top-4 right-4 flex gap-2 items-center">
            <Button variant="outline" size="sm" onClick={loadFullGalaxy}>
              Load full galaxy
            </Button>
            <DisplaySettingsMenu settings={display} onChange={updateDisplay} />
          </div>
        </div>

        {selected && (
          <>
            <ResizeHandle
              side="right"
              onResize={(d) => {
                setRightWidth((w) => {
                  const nw = Math.max(200, Math.min(500, w + d));
                  saveWidth("cbm-right-w", nw);
                  return nw;
                });
              }}
            />
            <div
              className="border-l border-border shrink-0 h-full overflow-hidden"
              style={{ width: rightWidth, maxHeight: "100%" }}
            >
              <RegionPanel
                region={selected}
                onOpen={openRegion}
                onClose={() => setSelectedRegion(null)}
              />
            </div>
          </>
        )}
      </div>
    );
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <GraphLoader nodeBudget={budget.value} progress={progress} />
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center p-8">
          <p className="text-red-400 text-sm mb-2">{error}</p>
          <Button variant="outline" size="sm" onClick={() => fetchOverview(project)}>
            Retry
          </Button>
        </div>
      </div>
    );
  }

  /* No data, or the project genuinely has no nodes — there are no filters to
     interact with, so show a plain full-screen message. The "all filtered out"
     case is handled inside the layout below so the filter sidebar stays put. */
  if (!data || !filteredData || data.nodes.length === 0) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-foreground/45 text-sm">No nodes in this project</p>
      </div>
    );
  }

  return (
    <div className="h-full flex">
      {/* Left sidebar — resizable */}
      <div
        className="border-r border-border/30 flex flex-col h-full bg-card/90 backdrop-blur-md shrink-0"
        style={{ width: leftWidth }}
      >
        <FilterPanel
          data={data}
          enabledLabels={enabledLabels}
          enabledEdgeTypes={enabledEdgeTypes}
          showLabels={showLabels}
          onToggleLabel={toggleLabel}
          onToggleEdgeType={toggleEdgeType}
          onToggleShowLabels={() => setShowLabels((v) => !v)}
          onEnableAll={enableAll}
          onDisableAll={disableAll}
          deadCodeView={deadCodeView}
          showOnlyDead={showOnlyDead}
          hideEntryPoints={hideEntryPoints}
          hideTests={hideTests}
          onToggleDeadCodeView={() => setDeadCodeView((v) => !v)}
          onToggleShowOnlyDead={() => setShowOnlyDead((v) => !v)}
          onToggleHideEntryPoints={() => setHideEntryPoints((v) => !v)}
          onToggleHideTests={() => setHideTests((v) => !v)}
          missedView={showMissedSkeleton}
          missedCount={data?.missed_graph?.nodes.filter((n) => n.label === "File").length ?? 0}
          onToggleMissedView={() => setShowMissedSkeleton((v) => !v)}
        />
        <Sidebar
          nodes={filteredData.nodes}
          onSelectPath={handleSelectPath}
          onSelectNode={handleNodeClick}
          selectedPath={selectedPath}
        />
      </div>
      <ResizeHandle
        side="left"
        onResize={(d) => {
          setLeftWidth((w) => {
            const nw = Math.max(150, Math.min(500, w + d));
            saveWidth("cbm-left-w", nw);
            return nw;
          });
        }}
      />

      {/* Graph area */}
      <div className="flex-1 relative overflow-hidden">
        {filteredData.nodes.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center">
              <p className="text-foreground/45 text-sm mb-3">All nodes filtered out</p>
              <Button size="sm" onClick={enableAll}>
                Reset Filters
              </Button>
            </div>
          </div>
        ) : (
          <>
            <ErrorBoundary>
              <GraphScene
                active={active}
                data={filteredData}
                missed={showMissedSkeleton ? missedSkeleton : null}
                highlightedIds={highlightedIds}
                cameraTarget={cameraTarget}
                showLabels={showLabels}
                display={display}
                onNodeClick={handleNodeClick}
                onBackgroundClick={handleBackgroundClick}
                landmarks
                onViewTarget={(x, y) => setViewTarget({ x, y })}
              />
            </ErrorBoundary>

            <Minimap
              regions={displayRegions}
              openRegionId={view.kind === "region" ? view.region.id : null}
              scentCounts={null}
              viewTarget={viewTarget}
              onOpen={openRegion}
              onHome={() =>
                setCameraTarget(
                  computeCameraTarget(
                    filteredData.nodes,
                    new Set(filteredData.nodes.map((n) => n.id)),
                  ),
                )
              }
            />

            {/* HUD */}
            <div className="absolute top-4 left-4 text-[13px] text-foreground/55 pointer-events-none tabular-nums">
              {regionsError && (
                <p className="text-amber-300/60">regions unavailable: {regionsError}</p>
              )}
              {view.kind === "region" && (
                <p className="text-primary/60">region: {view.region.name}</p>
              )}
              <p>
                {filteredData.nodes.length.toLocaleString("en-US")} nodes /{" "}
                {filteredData.edges.length.toLocaleString("en-US")} edges
              </p>
              {data.nodes.length > filteredData.nodes.length && (
                <p className="text-foreground/40 mt-0.5">
                  filtered from {data.nodes.length.toLocaleString("en-US")}
                </p>
              )}
              {limitNotice && (
                <p className="text-amber-300/80 mt-0.5">{limitNotice}</p>
              )}
              {highlightedIds && highlightedIds.size > 0 && (
                <p className="text-cyan-400/50 mt-0.5">
                  {highlightedIds.size} selected
                </p>
              )}
            </div>

            <div className="absolute top-4 right-4 flex gap-2 items-center">
              {view.kind === "region" && (
                <Button variant="outline" size="sm" onClick={backToRegions}>
                  ‹ Regions
                </Button>
              )}
              {highlightedIds && (
                <Button
                  size="sm"
                  onClick={() => {
                    setHighlightedIds(null);
                    setSelectedPath(null);
                    setSelectedNode(null);
                    setCameraTarget(null);
                  }}
                >
                  Clear selection
                </Button>
              )}
              <div className="flex items-center gap-1.5 h-8 px-2 rounded-md border border-border/50 bg-card/80 backdrop-blur-sm">
                <label
                  htmlFor="node-budget"
                  className="text-[12px] uppercase tracking-wider text-foreground/40"
                >
                  Nodes
                </label>
                <input
                  id="node-budget"
                  type="number"
                  min={GRAPH_NODE_BUDGET_STEP}
                  max={GRAPH_NODE_BUDGET_MAX}
                  step={GRAPH_NODE_BUDGET_STEP}
                  value={budgetDraft}
                  onChange={(e) => setBudgetDraft(e.target.value)}
                  onBlur={commitBudget}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      e.currentTarget.blur();
                    }
                  }}
                  className="w-24 bg-transparent text-right text-xs font-mono text-cyan-200/90 outline-none [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none"
                  aria-label="Node budget: how many nodes to load"
                  title="How many nodes to load (5,000 steps, edges between loaded nodes follow automatically)"
                />
              </div>
              <DisplaySettingsMenu settings={display} onChange={updateDisplay} />
              <Button
                variant="outline"
                size="sm"
                onClick={() => {
                  setHighlightedIds(null);
                  setSelectedPath(null);
                  setSelectedNode(null);
                  setCameraTarget(null);
                  fetchOverview(
                    project,
                    budget.value,
                    "code",
                    view.kind === "region" ? `region:${view.region.id}` : undefined,
                  );
                }}
              >
                Refresh
              </Button>
            </div>
          </>
        )}
      </div>

      {/* Right detail panel — resizable */}
      {selectedNode && filteredData && (
        <>
          <ResizeHandle
            side="right"
            onResize={(d) => {
              setRightWidth((w) => {
                const nw = Math.max(200, Math.min(500, w + d));
                saveWidth("cbm-right-w", nw);
                return nw;
              });
            }}
          />
          <div
            className="border-l border-border shrink-0 h-full overflow-hidden"
            style={{ width: rightWidth, maxHeight: "100%" }}
          >
            {missedSkeleton?.ids.has(selectedNode.id) ? (
              /* Skeleton node: the standard panel (code snippet, callers) is
               * meaningless for a not-fully-indexed file — show the coverage
               * callout with its report-the-edge-case actions instead. */
              <MissedCallout
                node={selectedNode}
                project={project}
                onClose={() => {
                  setSelectedNode(null);
                  setHighlightedIds(null);
                  setSelectedPath(null);
                }}
              />
            ) : (
              <NodeDetailPanel
                node={selectedNode}
                allNodes={filteredData.nodes}
                allEdges={filteredData.edges}
                project={project}
                repoInfo={repoInfo}
                scopeNote={(() => {
                  if (view.kind !== "region" || selectedNode.in_calls === undefined)
                    return null;
                  const visible = filteredData.edges.filter(
                    (e) => e.target === selectedNode.id && e.type === "CALLS",
                  ).length;
                  const outside = selectedNode.in_calls - visible;
                  return outside > 0
                    ? `+${outside.toLocaleString("en-US")} caller${outside === 1 ? "" : "s"} outside this region`
                    : null;
                })()}
                onClose={() => {
                  setSelectedNode(null);
                  setHighlightedIds(null);
                  setSelectedPath(null);
                  onRouteChange?.(
                    null,
                    view.kind === "region" ? String(view.region.id) : null,
                  );
                }}
                onNavigate={handleNavigateToNode}
              />
            )}
          </div>
        </>
      )}
    </div>
  );
}
