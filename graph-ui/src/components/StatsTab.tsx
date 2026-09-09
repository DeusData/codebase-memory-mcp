import { useCallback, useEffect, useState } from "react";
import { callTool } from "../api/rpc";
import { dashboardFetch } from "../api/dashboardAuth";

type DiagnosticValue = string | number | boolean | null | DiagnosticValue[] | {
  [key: string]: DiagnosticValue;
};

interface IndexDiagnostic {
  project?: string;
  status?: string;
  freshness?: string;
  generation?: number;
  commit?: string;
  dirty_fingerprint?: string;
  nodes?: number;
  edges?: number;
  languages?: DiagnosticValue;
  unresolved_calls?: number;
  ambiguous_calls?: number;
  digest?: string;
  [key: string]: DiagnosticValue | undefined;
}

interface HealthDiagnostic {
  status?: string;
  nodes?: number;
  edges?: number;
  size_bytes?: number;
  reason?: string;
}

function Metric({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-xl border border-border/30 bg-white/[0.02] p-4">
      <p className="text-[10px] text-foreground/25 uppercase tracking-widest mb-1">{label}</p>
      <p className="text-[18px] font-semibold tabular-nums text-foreground/80 break-all">
        {typeof value === "number" ? value.toLocaleString() : value}
      </p>
    </div>
  );
}

export function StatsTab() {
  const [project, setProject] = useState("");
  const [status, setStatus] = useState<IndexDiagnostic | null>(null);
  const [health, setHealth] = useState<HealthDiagnostic | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const args = project.trim() ? { project: project.trim() } : {};
      const next = await callTool<IndexDiagnostic>("index_status", args);
      setStatus(next);

      const resolvedProject = project.trim() || next.project;
      if (resolvedProject) {
        const response = await dashboardFetch(
          `/api/project-health?name=${encodeURIComponent(resolvedProject)}`,
        );
        setHealth(response.ok ? await response.json() : null);
      } else {
        setHealth(null);
      }
    } catch (cause) {
      setStatus(null);
      setHealth(null);
      setError(cause instanceof Error ? cause.message : "Unable to load index diagnostics");
    } finally {
      setLoading(false);
    }
  }, [project]);

  useEffect(() => {
    void refresh();
  }, []); // Load the session project once; manual refresh handles later input.

  const fields: Array<[string, string | number | undefined]> = [
    ["Freshness", status?.freshness],
    ["Generation", status?.generation],
    ["Nodes", status?.nodes ?? health?.nodes],
    ["Edges", status?.edges ?? health?.edges],
    ["DB size", health?.size_bytes !== undefined
      ? `${(health.size_bytes / 1024 / 1024).toFixed(1)} MB`
      : undefined],
    ["Unresolved calls", status?.unresolved_calls],
    ["Ambiguous calls", status?.ambiguous_calls],
  ];

  return (
    <div className="h-full overflow-auto">
      <div className="p-8 max-w-4xl mx-auto">
        <div className="mb-8">
          <h2 className="text-[16px] font-semibold text-foreground/85 mb-1">Index diagnostics</h2>
          <p className="text-[12px] text-foreground/30">
            Read-only freshness, integrity, coverage, and reproducibility information.
          </p>
        </div>

        <form
          className="flex gap-2 mb-6"
          onSubmit={(event) => {
            event.preventDefault();
            void refresh();
          }}
        >
          <input
            value={project}
            onChange={(event) => setProject(event.target.value)}
            placeholder="Project name (leave empty for session project)"
            className="flex-1 rounded-lg border border-border/30 bg-white/[0.03] px-3 py-2 text-[12px] font-mono outline-none focus:border-primary/40"
          />
          <button
            type="submit"
            disabled={loading}
            className="rounded-lg bg-primary/15 px-4 py-2 text-[12px] font-medium text-primary hover:bg-primary/25 disabled:opacity-40"
          >
            {loading ? "Checking…" : "Refresh"}
          </button>
        </form>

        {error && (
          <div className="rounded-xl border border-destructive/20 bg-destructive/5 p-4 mb-6">
            <p className="text-destructive text-[12px]">{error}</p>
          </div>
        )}

        {status && (
          <>
            <div className="flex items-center gap-3 rounded-xl border border-border/30 bg-white/[0.02] p-4 mb-4">
              <span
                className={`h-2.5 w-2.5 rounded-full ${
                  status.status === "ready" && health?.status !== "corrupt"
                    ? "bg-emerald-400"
                    : "bg-amber-400"
                }`}
              />
              <div>
                <p className="text-[13px] font-medium text-foreground/80">
                  {status.project ?? (project || "Session project")}
                </p>
                <p className="text-[11px] text-foreground/30">
                  Index: {status.status ?? "unknown"} · Integrity: {health?.status ?? "unknown"}
                </p>
              </div>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
              {fields
                .filter((field): field is [string, string | number] => field[1] !== undefined)
                .map(([label, value]) => <Metric key={label} label={label} value={value} />)}
            </div>

            {(status.commit || status.dirty_fingerprint || status.digest) && (
              <div className="rounded-xl border border-border/30 bg-black/20 p-4 mb-4 font-mono text-[11px]">
                {status.commit && <p className="text-foreground/45">commit: {status.commit}</p>}
                {status.dirty_fingerprint && (
                  <p className="text-foreground/45">dirty: {status.dirty_fingerprint}</p>
                )}
                {status.digest && <p className="text-foreground/45">digest: {status.digest}</p>}
              </div>
            )}

            {status.languages !== undefined && (
              <div className="rounded-xl border border-border/30 bg-white/[0.02] p-4">
                <p className="text-[10px] text-foreground/25 uppercase tracking-widest mb-2">
                  Language coverage
                </p>
                <pre className="overflow-auto text-[11px] text-foreground/50">
                  {JSON.stringify(status.languages, null, 2)}
                </pre>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
