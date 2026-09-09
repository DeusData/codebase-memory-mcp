import { useState } from "react";
import { StatsTab } from "./components/StatsTab";
import { ControlTab } from "./components/ControlTab";

type TabId = "diagnostics" | "logs";

const TABS: { id: TabId; label: string }[] = [
  { id: "diagnostics", label: "Diagnostics" },
  { id: "logs", label: "Logs" },
];

export function App() {
  const [activeTab, setActiveTab] = useState<TabId>("diagnostics");

  return (
    <div className="h-screen flex flex-col bg-background text-foreground">
      <header className="flex items-center justify-between px-5 h-12 border-b border-border bg-[#0b1920]/80 backdrop-blur-md shrink-0">
        <div className="flex items-center gap-6">
          <div className="flex items-center gap-2.5">
            <div className="w-[7px] h-[7px] rounded-full bg-primary" />
            <span className="text-[13px] font-semibold text-foreground/90 tracking-tight">
              Codebase Memory Diagnostics
            </span>
          </div>

          <nav className="flex items-center gap-0.5" aria-label="Dashboard">
            {TABS.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`px-3 py-1 rounded-md text-[12px] font-medium transition-all ${
                  activeTab === tab.id
                    ? "bg-primary/15 text-primary"
                    : "text-muted-foreground hover:text-foreground hover:bg-white/[0.04]"
                }`}
              >
                {tab.label}
              </button>
            ))}
          </nav>
        </div>

        <span className="rounded-md border border-emerald-400/20 bg-emerald-400/5 px-2 py-1 text-[10px] font-medium text-emerald-300/70">
          READ ONLY
        </span>
      </header>

      <main className="flex-1 min-h-0">
        {activeTab === "logs" ? <ControlTab /> : <StatsTab />}
      </main>
    </div>
  );
}
