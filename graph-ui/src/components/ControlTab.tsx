import { useEffect, useState } from "react";
import { dashboardFetch } from "../api/dashboardAuth";

export function ControlTab() {
  const [lines, setLines] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let stopped = false;
    const load = async () => {
      try {
        const response = await dashboardFetch("/api/logs?lines=200");
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        if (!stopped) {
          setLines(data.lines ?? []);
          setError(null);
        }
      } catch (cause) {
        if (!stopped) {
          setError(cause instanceof Error ? cause.message : "Unable to load logs");
        }
      }
    };
    void load();
    const interval = window.setInterval(load, 2000);
    return () => {
      stopped = true;
      window.clearInterval(interval);
    };
  }, []);

  return (
    <div className="h-full overflow-auto">
      <div className="p-8 max-w-4xl mx-auto">
        <h2 className="text-[15px] font-semibold text-foreground/80 mb-1">Diagnostic log</h2>
        <p className="text-[12px] text-foreground/30 mb-6">
          Read-only server events. Process control and terminal access are intentionally unavailable.
        </p>

        {error && (
          <div className="rounded-xl border border-destructive/20 bg-destructive/5 p-4 mb-4">
            <p className="text-destructive text-[12px]">{error}</p>
          </div>
        )}

        <div className="rounded-xl border border-border/30 bg-black/30 overflow-hidden">
          <div className="px-4 py-2 border-b border-border/20">
            <span className="text-[11px] font-medium text-foreground/40">Recent events</span>
            <span className="text-[10px] text-foreground/15 ml-2">{lines.length} lines</span>
          </div>
          <div className="h-[520px] overflow-auto">
            <div className="p-3 font-mono text-[10px] leading-relaxed">
              {lines.length === 0 ? (
                <p className="text-foreground/15 text-center py-8">No logs yet</p>
              ) : (
                lines.map((line, index) => {
                  const isError = line.includes("level=error");
                  const isWarning = line.includes("level=warn");
                  return (
                    <div
                      key={`${index}-${line}`}
                      className={`py-[1px] ${
                        isError
                          ? "text-red-400/70"
                          : isWarning
                            ? "text-yellow-400/60"
                            : "text-foreground/30"
                      }`}
                    >
                      {line}
                    </div>
                  );
                })
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
