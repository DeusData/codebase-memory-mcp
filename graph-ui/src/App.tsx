import { useState } from "react";
import { GraphTab } from "./components/GraphTab";
import { StatsTab } from "./components/StatsTab";
import { ControlTab } from "./components/ControlTab";
import { SERVICE_GRAPH_SENTINEL } from "./hooks/useGraphData";
import type { TabId } from "./lib/types";

const TABS: { id: TabId; label: string }[] = [
  { id: "graph", label: "Graph" },
  { id: "stats", label: "Projects" },
  { id: "control", label: "Control" },
];

export function App() {
  const [activeTab, setActiveTab] = useState<TabId>("stats");
  const [selectedProject, setSelectedProject] = useState<string | null>(null);

  return (
    <div className="h-screen flex flex-col bg-background text-foreground">
      {/* Header */}
      <header className="flex items-center justify-between px-5 h-12 border-b border-border bg-[#0b1920]/80 backdrop-blur-md shrink-0">
        <div className="flex items-center gap-6">
          <div className="flex items-center gap-2.5">
            <div className="w-[7px] h-[7px] rounded-full bg-primary" />
            <span className="text-[13px] font-semibold text-foreground/90 tracking-tight">
              Codebase Memory
            </span>
          </div>

          {/* Tabs inline in header */}
          <nav className="flex items-center gap-0.5">
            {TABS.map((t) => (
              <button
                key={t.id}
                onClick={() => setActiveTab(t.id)}
                className={`px-3 py-1 rounded-md text-[12px] font-medium transition-all ${
                  activeTab === t.id
                    ? "bg-primary/15 text-primary"
                    : "text-muted-foreground hover:text-foreground hover:bg-white/[0.04]"
                }`}
              >
                {t.label}
              </button>
            ))}
          </nav>
        </div>

        {selectedProject && (
          <div className="flex items-center gap-2 px-3 py-1 rounded-lg bg-white/[0.04] border border-border/30">
            <span className="text-[10px] text-foreground/30 uppercase tracking-wider">Graph</span>
            <span className="text-[11px] text-primary font-mono truncate max-w-[300px]">
              {selectedProject === SERVICE_GRAPH_SENTINEL ? "Service Graph" : selectedProject}
            </span>
            <button
              onClick={() => { setSelectedProject(null); setActiveTab("stats"); }}
              className="text-foreground/20 hover:text-foreground/50 text-[12px] ml-1 transition-colors"
            >
              ×
            </button>
          </div>
        )}
      </header>

      {/* Content */}
      <main className="flex-1 min-h-0">
        {activeTab === "graph" ? (
          <GraphTab project={selectedProject} />
        ) : activeTab === "control" ? (
          <ControlTab />
        ) : (
          <StatsTab
            onSelectProject={(p) => {
              setSelectedProject(p);
              setActiveTab("graph");
            }}
          />
        )}
      </main>
    </div>
  );
}
