import { useCallback, useEffect, useState } from "react";
import { GraphTab } from "./components/GraphTab";
import { StatsTab } from "./components/StatsTab";
import { ControlTab } from "./components/ControlTab";
import { OverviewTab } from "./components/OverviewTab";
import { ModulesTab } from "./components/ModulesTab";
import { SymbolTab } from "./components/SymbolTab";
import { FlowsTab } from "./components/FlowsTab";
import { ChangesTab } from "./components/ChangesTab";
import { DashboardTab } from "./components/DashboardTab";
import { CommandK } from "./components/CommandK";
import { ProjectSwitcher } from "./components/ProjectSwitcher";
import { WikiPanel } from "./components/WikiPanel";
import { wikiEntry } from "./wiki/entries";
import {
  PromptBasketProvider,
  PromptBasketDrawer,
} from "./components/PromptBasket";
import type { TabId } from "./lib/types";
import { useUiMessages } from "./lib/i18n";

const TAB_IDS: TabId[] = [
  "overview",
  "modules",
  "graph",
  "flows",
  "changes",
  "dashboard",
  "symbol",
  "stats",
  "control",
];

/* Tabs that need a selected project. "symbol" is reachable only via links. */

interface RouteState {
  tab: TabId;
  project: string | null;
  /* Deep links (#564): node/region on the galaxy, sym on the symbol page,
   * path in Modules, flow in Flows — all survive reloads and sharing. */
  node: string | null;
  region: string | null;
  sym: string | null;
  path: string;
  flow: string | null;
}

function readRoute(): RouteState {
  const params = new URLSearchParams(window.location.search);
  const rawTab = params.get("tab");
  const tab = TAB_IDS.includes(rawTab as TabId) ? (rawTab as TabId) : "stats";
  return {
    tab,
    project: params.get("project") || null,
    node: params.get("node") || null,
    region: params.get("region") || null,
    sym: params.get("sym") || null,
    path: params.get("path") || "",
    flow: params.get("flow") || null,
  };
}

function routeUrl(route: RouteState): string {
  const params = new URLSearchParams();
  params.set("tab", route.tab);
  if (route.project) params.set("project", route.project);
  if (route.region) params.set("region", route.region);
  if (route.node) params.set("node", route.node);
  if (route.sym) params.set("sym", route.sym);
  if (route.path) params.set("path", route.path);
  if (route.flow) params.set("flow", route.flow);
  return `${window.location.pathname}?${params.toString()}${window.location.hash}`;
}

export function App() {
  const t = useUiMessages();
  const [route, setRoute] = useState<RouteState>(readRoute);
  const [commandOpen, setCommandOpen] = useState(false);
  /* The metric wiki: any dotted-underline term opens its entry here. */
  const [wikiSlug, setWikiSlug] = useState<string | null>(null);
  /* The galaxy keeps its WebGL context and layout across tab switches:
   * mounted once visited, hidden with `invisible` (never display:none, which
   * would zero the canvas), frameloop paused while hidden. */
  const [graphVisited, setGraphVisited] = useState(false);
  useEffect(() => {
    if (route.tab === "graph") setGraphVisited(true);
  }, [route.tab]);
  const { tab: activeTab, project: selectedProject } = route;

  const [version, setVersion] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    void fetch("/api/ui-config")
      .then((response) => (response.ok ? response.json() : null))
      .then((config) => {
        if (!cancelled && typeof config?.version === "string" && config.version) {
          setVersion(config.version);
        }
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, []);

  /* Normalize the URL on first load so it always carries the current route. */
  useEffect(() => {
    const initial = readRoute();
    window.history.replaceState(null, "", routeUrl(initial));
  }, []);

  useEffect(() => {
    const onPopState = () => setRoute(readRoute());
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  /* #wiki=<slug> deep link: wiki entries are addressable by URL (the doc_url
   * target), opening the panel over whatever view the rest of the URL names. */
  useEffect(() => {
    const openFromHash = () => {
      const match = window.location.hash.match(/wiki=([a-z][a-z-]*)/);
      if (match && wikiEntry(match[1])) setWikiSlug(match[1]);
    };
    openFromHash();
    window.addEventListener("hashchange", openFromHash);
    return () => window.removeEventListener("hashchange", openFromHash);
  }, []);

  /* ⌘K / ctrl-K anywhere. */
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setCommandOpen((value) => !value);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const navigate = useCallback((next: Partial<RouteState> & { tab: TabId }) => {
    setRoute((previous) => {
      const merged: RouteState = {
        project: previous.project,
        node: null,
        region: null,
        sym: null,
        path: "",
        flow: null,
        ...next,
      } as RouteState;
      const url = routeUrl(merged);
      const current = `${window.location.pathname}${window.location.search}${window.location.hash}`;
      if (url !== current) window.history.pushState(null, "", url);
      return merged;
    });
  }, []);

  const openSymbol = useCallback(
    (ref: { id?: number; qn?: string }) => {
      navigate({
        tab: "symbol",
        sym: ref.qn ?? (ref.id !== undefined ? `#${ref.id}` : null),
      });
    },
    [navigate],
  );
  const openRegion = useCallback(
    (regionId: number) => {
      navigate({ tab: "graph", region: String(regionId) });
    },
    [navigate],
  );

  /* Project-scoped views only — they render once a project is chosen.
   * App-level destinations (Projects, Control) live on the right instead of
   * being mixed into this row as dead buttons. */
  const projectTabs: { id: TabId; label: string }[] = [
    { id: "overview", label: t.tabs.overview },
    { id: "modules", label: t.tabs.modules },
    { id: "graph", label: t.tabs.graph },
    { id: "flows", label: t.tabs.flows },
    { id: "changes", label: t.tabs.changes },
    { id: "dashboard", label: t.tabs.dashboard },
  ];

  const symbolRef = route.sym
    ? route.sym.startsWith("#")
      ? { id: Number(route.sym.slice(1)) }
      : { qn: route.sym }
    : null;

  return (
    <PromptBasketProvider>
      <div className="h-screen flex flex-col bg-background text-foreground">
        {/* Header */}
        <header className="flex items-center justify-between px-5 h-12 border-b border-border bg-card/80 backdrop-blur-md shrink-0">
          <div className="flex items-center gap-4 min-w-0">
            <div className="flex items-center gap-2.5 shrink-0">
              <div className="w-[7px] h-[7px] rounded-full bg-primary" />
              <span className="text-[13px] font-semibold text-foreground/90 tracking-tight">
                CBM Atlas
              </span>
              {version && (
                <span
                  className="translate-y-px text-[10px] font-mono text-foreground/30"
                  title="Server version"
                >
                  {version.startsWith("v") ? version : `v${version}`}
                </span>
              )}
            </div>

            {/* Context first: which project is everything below about? */}
            <ProjectSwitcher
              selected={selectedProject}
              labels={t.switcher}
              allProjectsLabel={t.tabs.projects}
              onSelect={(project) => navigate({ tab: "overview", project })}
              onAllProjects={() => navigate({ tab: "stats", project: null })}
            />

            {selectedProject && (
              <>
                <div className="w-px h-5 bg-border/50 shrink-0" />
                <nav className="flex items-center gap-0.5 overflow-x-auto">
                  {projectTabs.map((tab) => (
                    <button
                      key={tab.id}
                      onClick={() => navigate({ tab: tab.id, project: selectedProject })}
                      className={`px-3 py-1 rounded-md text-[12px] font-medium transition-all whitespace-nowrap ${
                        activeTab === tab.id
                          ? "bg-primary/15 text-primary"
                          : "text-muted-foreground hover:text-foreground hover:bg-surface-3"
                      }`}
                    >
                      {tab.label}
                    </button>
                  ))}
                </nav>
              </>
            )}
          </div>

          <div className="flex items-center gap-2 shrink-0">
            {selectedProject && (
              <button
                onClick={() => setCommandOpen(true)}
                className="px-2.5 py-1 rounded-md text-[13px] text-muted-foreground hover:text-foreground bg-popover hover:bg-surface-3 border border-border/40 transition-all"
                title="Search symbols and code"
              >
                ⌘K
              </button>
            )}
            <button
              onClick={() => navigate({ tab: "control", project: selectedProject })}
              className={`px-3 py-1 rounded-md text-[12px] font-medium transition-all ${
                activeTab === "control"
                  ? "bg-primary/15 text-primary"
                  : "text-muted-foreground hover:text-foreground hover:bg-surface-3"
              }`}
            >
              {t.tabs.control}
            </button>
          </div>
        </header>

        {/* Content */}
        <main className="flex-1 min-h-0 relative">
          {graphVisited && (
            <div
              className={
                activeTab === "graph"
                  ? "h-full"
                  : "invisible pointer-events-none absolute inset-0"
              }
            >
              <GraphTab
                active={activeTab === "graph"}
                project={selectedProject}
                routeNode={route.node}
                routeRegion={route.region}
                onRouteChange={(node, region) =>
                  navigate({ tab: "graph", node, region })
                }
              />
            </div>
          )}
          {activeTab === "overview" && selectedProject ? (
            <OverviewTab
              project={selectedProject}
              onOpenRegion={openRegion}
              onOpenSymbol={(qn) => openSymbol({ qn })}
              onOpenModules={() => navigate({ tab: "modules" })}
              onOpenFlows={() => navigate({ tab: "flows" })}
              onOpenDashboard={() => navigate({ tab: "dashboard" })}
              onOpenTab={(tab) => navigate({ tab })}
            />
          ) : activeTab === "modules" && selectedProject ? (
            <ModulesTab
              project={selectedProject}
              path={route.path}
              onNavigatePath={(path) => navigate({ tab: "modules", path })}
              onOpenSymbol={openSymbol}
              onOpenRegion={openRegion}
            />
          ) : activeTab === "graph" ? null : activeTab === "flows" &&
            selectedProject ? (
            <FlowsTab
              project={selectedProject}
              flowId={route.flow !== null ? Number(route.flow) : null}
              onOpenFlow={(id) =>
                navigate({ tab: "flows", flow: id !== null ? String(id) : null })
              }
              onOpenSymbol={openSymbol}
              onOpenWiki={setWikiSlug}
            />
          ) : activeTab === "changes" && selectedProject ? (
            <ChangesTab project={selectedProject} onOpenSymbol={openSymbol} />
          ) : activeTab === "dashboard" && selectedProject ? (
            <DashboardTab
              project={selectedProject}
              onOpenSymbol={(ref) => openSymbol(ref)}
              onOpenModulesPath={(path) => navigate({ tab: "modules", path })}
              onOpenWiki={setWikiSlug}
            />
          ) : activeTab === "symbol" && selectedProject && symbolRef ? (
            <SymbolTab
              project={selectedProject}
              symbolRef={symbolRef}
              onOpenSymbol={openSymbol}
              onOpenRegion={openRegion}
              onOpenWiki={setWikiSlug}
            />
          ) : activeTab === "control" ? (
            <ControlTab />
          ) : (
            <StatsTab
              onSelectProject={(project) => navigate({ tab: "overview", project })}
            />
          )}
        </main>

        <CommandK
          project={selectedProject}
          open={commandOpen}
          onClose={() => setCommandOpen(false)}
          onOpenSymbol={openSymbol}
        />
        {wikiSlug && (
          <WikiPanel
            slug={wikiSlug}
            onClose={() => setWikiSlug(null)}
            onNavigate={setWikiSlug}
          />
        )}
        <PromptBasketDrawer project={selectedProject} />
      </div>
    </PromptBasketProvider>
  );
}
