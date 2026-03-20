import { useMemo, useState, useCallback, useEffect } from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { useProjects } from "../hooks/useProjects";
import { colorForLabel, SERVICE_NODE_COLORS } from "../lib/colors";
import { callTool } from "../api/rpc";
import { SERVICE_GRAPH_SENTINEL } from "../hooks/useGraphData";
import type { ServiceGraph } from "../lib/types";

interface StatsTabProps {
  onSelectProject: (project: string) => void;
}

/* ── Glowy health dot ───────────────────────────────────── */

function HealthDot({ name }: { name: string }) {
  const [status, setStatus] = useState<"loading" | "healthy" | "corrupt" | "missing">("loading");
  const [info, setInfo] = useState("");

  useEffect(() => {
    fetch(`/api/project-health?name=${encodeURIComponent(name)}`)
      .then((r) => r.json())
      .then((d) => {
        setStatus(d.status ?? "corrupt");
        if (d.nodes !== undefined) {
          const sizeMB = ((d.size_bytes ?? 0) / 1024 / 1024).toFixed(1);
          setInfo(`${d.nodes.toLocaleString()} nodes, ${d.edges.toLocaleString()} edges, ${sizeMB} MB`);
        } else if (d.reason) {
          setInfo(d.reason);
        }
      })
      .catch(() => setStatus("corrupt"));
  }, [name]);

  const dotColor =
    status === "healthy" ? "#34d399" :
    status === "missing" ? "#fbbf24" :
    status === "corrupt" ? "#f87171" : "#555";

  const label =
    status === "healthy" ? "Database healthy" :
    status === "missing" ? "Database missing" :
    status === "corrupt" ? "Database unhealthy" : "Checking...";

  return (
    <div className="group relative inline-flex items-center">
      {/* Glow layer */}
      <span
        className="absolute w-3 h-3 rounded-full animate-pulse opacity-40 blur-[3px]"
        style={{ backgroundColor: dotColor }}
      />
      {/* Dot */}
      <span
        className="relative w-[8px] h-[8px] rounded-full"
        style={{ backgroundColor: dotColor, boxShadow: `0 0 6px ${dotColor}80` }}
      />
      {/* Tooltip */}
      <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-3 hidden group-hover:block z-20 pointer-events-none">
        <div className="bg-[#0b1920] border border-border/50 rounded-lg px-3 py-2 text-[11px] whitespace-nowrap shadow-xl">
          <p className="font-medium" style={{ color: dotColor }}>{label}</p>
          {info && <p className="text-foreground/35 text-[10px] mt-0.5">{info}</p>}
        </div>
      </div>
    </div>
  );
}

/* ── ADR button + modal ─────────────────────────────────── */

function AdrButton({ project }: { project: string }) {
  const [hasAdr, setHasAdr] = useState<boolean | null>(null);
  const [open, setOpen] = useState(false);
  const [content, setContent] = useState("");
  const [saving, setSaving] = useState(false);
  const [updatedAt, setUpdatedAt] = useState("");

  const fetchAdr = useCallback(async () => {
    try {
      const res = await fetch(`/api/adr?project=${encodeURIComponent(project)}`);
      const data = await res.json();
      setHasAdr(data.has_adr ?? false);
      if (data.content) setContent(data.content);
      if (data.updated_at) setUpdatedAt(data.updated_at);
    } catch { setHasAdr(false); }
  }, [project]);

  useEffect(() => { fetchAdr(); }, [fetchAdr]);

  const save = async () => {
    setSaving(true);
    try {
      await fetch("/api/adr", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ project, content }),
      });
      await fetchAdr();
      setOpen(false);
    } catch { /* ignore */ }
    finally { setSaving(false); }
  };

  if (hasAdr === null) return null;

  return (
    <>
      <button
        onClick={() => { setOpen(true); fetchAdr(); }}
        className={`px-2.5 py-1 rounded-lg text-[10px] font-medium transition-all ${
          hasAdr
            ? "bg-accent/15 text-accent hover:bg-accent/25"
            : "bg-white/[0.03] text-foreground/25 hover:text-foreground/40 hover:bg-white/[0.06]"
        }`}
      >
        {hasAdr ? "ADR" : "+ ADR"}
      </button>

      {open && (
        <div className="fixed inset-0 z-50 flex items-center justify-center" onClick={() => setOpen(false)}>
          <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" />
          <div className="relative bg-[#0e2028] border border-border/40 rounded-2xl p-6 w-full max-w-2xl shadow-2xl max-h-[80vh] flex flex-col" onClick={(e) => e.stopPropagation()}>
            <div className="flex items-center justify-between mb-4">
              <div>
                <h3 className="text-[15px] font-semibold text-foreground/90">Architecture Decision Record</h3>
                <p className="text-[11px] text-foreground/30 font-mono mt-0.5">{project}</p>
              </div>
              <button onClick={() => setOpen(false)} className="text-foreground/20 hover:text-foreground/50 text-[16px] p-1">×</button>
            </div>
            {updatedAt && (
              <p className="text-[10px] text-foreground/20 mb-3">Last updated: {updatedAt}</p>
            )}
            <textarea
              value={content}
              onChange={(e) => setContent(e.target.value)}
              placeholder={"# Architecture Decision Record\n\n## Context\n...\n\n## Decision\n...\n\n## Consequences\n..."}
              className="flex-1 min-h-[300px] bg-white/[0.03] border border-white/[0.06] rounded-xl px-4 py-3 text-[12px] text-foreground font-mono placeholder-foreground/15 outline-none focus:border-primary/30 resize-none leading-relaxed"
            />
            <div className="flex justify-end gap-2 mt-4">
              {hasAdr && (
                <button
                  onClick={async () => {
                    setContent(""); await save();
                  }}
                  className="px-3 py-2 rounded-lg text-[12px] text-destructive/60 hover:text-destructive hover:bg-destructive/10 font-medium transition-all"
                >
                  Delete
                </button>
              )}
              <button onClick={() => setOpen(false)} className="px-4 py-2 rounded-lg text-[12px] text-foreground/40 hover:bg-white/[0.04] font-medium transition-all">Cancel</button>
              <button onClick={save} disabled={saving} className="px-4 py-2 rounded-lg bg-primary/20 hover:bg-primary/30 text-primary text-[12px] font-medium transition-all disabled:opacity-30">
                {saving ? "Saving..." : "Save"}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

/* ── Create Index Modal ─────────────────────────────────── */

function CreateIndexModal({ onClose, onCreated }: { onClose: () => void; onCreated: () => void }) {
  const [currentPath, setCurrentPath] = useState("");
  const [dirs, setDirs] = useState<string[]>([]);
  const [parentPath, setParentPath] = useState("");
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const browse = useCallback(async (path?: string) => {
    setLoading(true);
    try {
      const q = path ? `?path=${encodeURIComponent(path)}` : "";
      const res = await fetch(`/api/browse${q}`);
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setCurrentPath(data.path ?? "");
      setDirs((data.dirs ?? []).sort());
      setParentPath(data.parent ?? "/");
    } catch (e) { setError(e instanceof Error ? e.message : "Browse failed"); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { browse(); }, [browse]);

  const submit = async () => {
    if (!currentPath) return;
    setSubmitting(true); setError(null);
    try {
      const res = await fetch("/api/index", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ root_path: currentPath }) });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? "Failed");
      onCreated(); onClose();
    } catch (e) { setError(e instanceof Error ? e.message : "Failed"); }
    finally { setSubmitting(false); }
  };

  /* Breadcrumb segments */
  const segments = currentPath.split("/").filter(Boolean);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center" onClick={onClose}>
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" />
      <div className="relative bg-[#0e2028] border border-border/40 rounded-2xl w-full max-w-lg shadow-2xl flex flex-col overflow-hidden" style={{ height: "min(70vh, 550px)" }} onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div className="px-5 pt-5 pb-3 shrink-0">
          <h3 className="text-[15px] font-semibold text-foreground/90 mb-1">Select Repository Folder</h3>
          <p className="text-[12px] text-foreground/30">Navigate to the project root and click "Index This Folder".</p>
        </div>

        {/* Breadcrumb */}
        <div className="px-5 py-2 border-y border-border/20 flex items-center gap-0.5 overflow-x-auto text-[11px] shrink-0">
          <button onClick={() => browse("/")} className="text-primary/60 hover:text-primary shrink-0 transition-colors">/</button>
          {segments.map((seg, i) => (
            <span key={i} className="flex items-center gap-0.5 shrink-0">
              <span className="text-foreground/15">/</span>
              <button
                onClick={() => browse("/" + segments.slice(0, i + 1).join("/"))}
                className={`transition-colors ${i === segments.length - 1 ? "text-foreground/70 font-medium" : "text-primary/50 hover:text-primary"}`}
              >
                {seg}
              </button>
            </span>
          ))}
        </div>

        {/* Directory list */}
        <ScrollArea className="flex-1 min-h-0">
          <div className="px-2 py-1">
            {/* Go up */}
            {currentPath !== "/" && (
              <button
                onClick={() => browse(parentPath)}
                className="flex items-center gap-2 w-full text-left px-3 py-2 rounded-lg hover:bg-white/[0.04] text-[12px] text-foreground/40 transition-colors"
              >
                <span className="text-foreground/20">↑</span>
                <span>..</span>
              </button>
            )}
            {loading ? (
              <p className="text-foreground/20 text-[12px] text-center py-8">Loading...</p>
            ) : dirs.length === 0 ? (
              <p className="text-foreground/15 text-[12px] text-center py-8">No subdirectories</p>
            ) : (
              dirs.map((d) => (
                <button
                  key={d}
                  onClick={() => browse(`${currentPath}/${d}`)}
                  className="flex items-center gap-2 w-full text-left px-3 py-1.5 rounded-lg hover:bg-white/[0.04] text-[12px] text-foreground/60 transition-colors group"
                >
                  <span className="text-foreground/20 group-hover:text-foreground/40">📁</span>
                  <span className="truncate">{d}</span>
                </button>
              ))
            )}
          </div>
        </ScrollArea>

        {/* Footer */}
        <div className="px-5 py-4 border-t border-border/20 shrink-0">
          {error && <div className="rounded-lg bg-destructive/10 border border-destructive/20 px-3 py-2 mb-3"><p className="text-destructive text-[11px]">{error}</p></div>}
          <div className="flex items-center justify-between">
            <p className="text-[11px] text-foreground/25 font-mono truncate max-w-[250px]">{currentPath}</p>
            <div className="flex gap-2 shrink-0">
              <button onClick={onClose} className="px-3 py-2 rounded-lg text-[12px] text-foreground/40 hover:bg-white/[0.04] font-medium transition-all">Cancel</button>
              <button onClick={submit} disabled={submitting || !currentPath} className="px-4 py-2 rounded-lg bg-primary/20 hover:bg-primary/30 text-primary text-[12px] font-medium transition-all disabled:opacity-30">
                {submitting ? "Starting..." : "Index This Folder"}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Index Progress ─────────────────────────────────────── */

function IndexProgress({ onDone }: { onDone: () => void }) {
  const [jobs, setJobs] = useState<{ slot: number; status: string; path: string }[]>([]);
  useEffect(() => {
    const poll = setInterval(async () => {
      try {
        const data = await (await fetch("/api/index-status")).json();
        setJobs(data);
        if (data.length > 0 && data.every((j: { status: string }) => j.status !== "indexing")) onDone();
      } catch { /* */ }
    }, 2000);
    return () => clearInterval(poll);
  }, [onDone]);
  const active = jobs.filter((j) => j.status === "indexing");
  if (active.length === 0) return null;
  return (
    <div className="rounded-xl border border-primary/20 bg-primary/5 p-4 mb-6">
      {active.map((j) => (
        <div key={j.slot} className="flex items-center gap-3">
          <div className="w-4 h-4 border-2 border-primary/30 border-t-primary rounded-full animate-spin shrink-0" />
          <div>
            <p className="text-[12px] text-primary font-medium">Indexing in progress</p>
            <p className="text-[11px] text-foreground/30 font-mono">{j.path}</p>
          </div>
        </div>
      ))}
    </div>
  );
}

/* ── Service Graph Card ─────────────────────────────────── */

function ServiceGraphCard({ onSelectProject, repoPaths }: { onSelectProject: (p: string) => void; repoPaths: string[] }) {
  const [stats, setStats] = useState<{ services: number; topics: number; graphql: number; tables: number; edges: number } | null>(null);
  const [scanning, setScanning] = useState(false);
  const [scanError, setScanError] = useState<string | null>(null);

  const fetchStats = useCallback(async () => {
    try {
      const sg = await callTool<ServiceGraph>("get_graph");
      setStats({
        services: sg.services?.length ?? 0,
        topics: sg.topics?.length ?? 0,
        graphql: sg.graphqlEndpoints?.length ?? 0,
        tables: sg.tables?.length ?? 0,
        edges: sg.edges?.length ?? 0,
      });
    } catch {
      // Service graph not available or empty
      setStats(null);
    }
  }, []);

  useEffect(() => { fetchStats(); }, [fetchStats]);

  const handleScan = async () => {
    setScanning(true);
    setScanError(null);
    try {
      // Pass indexed project root paths so scan_repos knows what to scan
      const args: Record<string, unknown> = {};
      if (repoPaths.length > 0) {
        args.repos = repoPaths;
      }
      await callTool("scan_repos", args);
      await fetchStats();
    } catch (e) {
      setScanError(e instanceof Error ? e.message : "Scan failed");
    } finally {
      setScanning(false);
    }
  };

  const hasData = stats && stats.services > 0;

  return (
    <div className="rounded-xl border border-border/30 bg-white/[0.02] hover:bg-white/[0.035] transition-all p-5 mb-6">
      <div className="flex items-start justify-between gap-3 mb-3">
        <div className="min-w-0 flex items-start gap-2.5">
          <div className="mt-1.5">
            <span
              className="relative inline-block w-[8px] h-[8px] rounded-full"
              style={{ backgroundColor: SERVICE_NODE_COLORS.Service, boxShadow: `0 0 6px ${SERVICE_NODE_COLORS.Service}80` }}
            />
          </div>
          <div className="min-w-0">
            <h3 className="text-[14px] font-semibold text-foreground/90 mb-0.5">Service Graph</h3>
            <p className="text-[11px] text-foreground/20">Cross-service topology: Pub/Sub, GraphQL, Database</p>
          </div>
        </div>
        <div className="flex items-center gap-1.5 shrink-0">
          <button
            onClick={handleScan}
            disabled={scanning}
            className="px-3 py-1.5 rounded-lg bg-white/[0.04] hover:bg-white/[0.07] text-[12px] text-foreground/40 font-medium transition-all disabled:opacity-30"
          >
            {scanning ? (
              <span className="flex items-center gap-2">
                <span className="w-3 h-3 border-2 border-foreground/20 border-t-foreground/50 rounded-full animate-spin" />
                Scanning...
              </span>
            ) : "Scan Repos"}
          </button>
          {hasData && (
            <button
              onClick={() => onSelectProject(SERVICE_GRAPH_SENTINEL)}
              className="px-3 py-1.5 rounded-lg bg-primary/15 hover:bg-primary/25 text-primary text-[12px] font-medium transition-all"
            >
              View Graph
            </button>
          )}
        </div>
      </div>
      {scanError && (
        <div className="rounded-lg bg-destructive/10 border border-destructive/20 px-3 py-2 mb-3">
          <p className="text-destructive text-[11px]">{scanError}</p>
        </div>
      )}
      {hasData && (
        <div className="flex gap-6 text-[12px] text-foreground/30">
          {[
            { label: "services", value: stats!.services, color: SERVICE_NODE_COLORS.Service },
            { label: "topics", value: stats!.topics, color: SERVICE_NODE_COLORS.Topic },
            { label: "graphql", value: stats!.graphql, color: SERVICE_NODE_COLORS.GraphQL },
            { label: "tables", value: stats!.tables, color: SERVICE_NODE_COLORS.Table },
            { label: "edges", value: stats!.edges, color: undefined },
          ].filter(s => s.value > 0).map((s) => (
            <span key={s.label}>
              <strong className="tabular-nums" style={s.color ? { color: s.color + "bb" } : { color: "var(--foreground-55)" }}>
                {s.value.toLocaleString()}
              </strong>{" "}{s.label}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

/* ── Main Stats Tab ─────────────────────────────────────── */

export function StatsTab({ onSelectProject }: StatsTabProps) {
  const { projects, loading, error, refresh } = useProjects();
  const [showModal, setShowModal] = useState(false);
  const [indexing, setIndexing] = useState(false);

  const aggregate = useMemo(() => {
    let totalNodes = 0, totalEdges = 0;
    for (const p of projects) {
      totalNodes += p.schema?.node_labels?.reduce((s, l) => s + l.count, 0) ?? 0;
      totalEdges += p.schema?.edge_types?.reduce((s, t) => s + t.count, 0) ?? 0;
    }
    return { projects: projects.length, nodes: totalNodes, edges: totalEdges };
  }, [projects]);

  const deleteProject = useCallback(async (name: string) => {
    if (!confirm(`Delete index for "${name}"?`)) return;
    try { await fetch(`/api/project?name=${encodeURIComponent(name)}`, { method: "DELETE" }); refresh(); } catch { /* */ }
  }, [refresh]);

  return (
    <ScrollArea className="h-full">
      <div className="p-8 max-w-3xl mx-auto">
        {projects.length > 0 && (
          <div className="flex gap-4 mb-8">
            {[
              { label: "Projects", value: aggregate.projects, color: "text-primary" },
              { label: "Nodes", value: aggregate.nodes, color: "text-foreground/80" },
              { label: "Edges", value: aggregate.edges, color: "text-foreground/80" },
            ].map((s) => (
              <div key={s.label} className="flex-1 rounded-xl border border-border/30 bg-white/[0.02] p-4">
                <p className="text-[10px] text-foreground/25 uppercase tracking-widest mb-1">{s.label}</p>
                <p className={`text-[22px] font-semibold tabular-nums ${s.color}`}>{s.value.toLocaleString()}</p>
              </div>
            ))}
          </div>
        )}

        <ServiceGraphCard onSelectProject={onSelectProject} repoPaths={projects.map(p => p.project.root_path)} />

        {indexing && <IndexProgress onDone={() => { setIndexing(false); refresh(); }} />}

        <div className="flex items-center justify-between mb-6">
          <h2 className="text-[15px] font-semibold text-foreground/80">Indexed Projects</h2>
          <div className="flex items-center gap-2">
            <button onClick={() => setShowModal(true)} className="px-3 py-1.5 rounded-lg bg-primary/15 hover:bg-primary/25 text-primary text-[12px] font-medium transition-all">+ New Index</button>
            <button onClick={refresh} disabled={loading} className="px-3 py-1.5 rounded-lg bg-white/[0.04] hover:bg-white/[0.07] text-[12px] text-foreground/40 font-medium transition-all disabled:opacity-30">{loading ? "..." : "Refresh"}</button>
          </div>
        </div>

        {error && <div className="rounded-xl border border-destructive/20 bg-destructive/5 p-4 mb-6"><p className="text-destructive text-[13px]">{error}</p></div>}

        {!loading && projects.length === 0 && !error && (
          <div className="text-center py-20">
            <p className="text-foreground/25 text-[13px] mb-2">No indexed projects</p>
            <button onClick={() => setShowModal(true)} className="px-4 py-2 rounded-lg bg-primary/15 hover:bg-primary/25 text-primary text-[12px] font-medium transition-all">Index your first repository</button>
          </div>
        )}

        <div className="space-y-3">
          {projects.map((p) => {
            const totalNodes = p.schema?.node_labels?.reduce((s, l) => s + l.count, 0) ?? 0;
            const totalEdges = p.schema?.edge_types?.reduce((s, t) => s + t.count, 0) ?? 0;
            return (
              <div key={p.project.name} className="rounded-xl border border-border/30 bg-white/[0.02] hover:bg-white/[0.035] transition-all p-5">
                <div className="flex items-start justify-between gap-3 mb-3">
                  <div className="min-w-0 flex items-start gap-2.5">
                    <div className="mt-1.5"><HealthDot name={p.project.name} /></div>
                    <div className="min-w-0">
                      <h3 className="text-[14px] font-semibold text-foreground/90 mb-0.5">{p.project.name}</h3>
                      <p className="text-[11px] text-foreground/20 font-mono truncate">{p.project.root_path}</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-1.5 shrink-0">
                    <AdrButton project={p.project.name} />
                    <button onClick={() => onSelectProject(p.project.name)} className="px-3 py-1.5 rounded-lg bg-primary/15 hover:bg-primary/25 text-primary text-[12px] font-medium transition-all">View Graph</button>
                    <button onClick={() => deleteProject(p.project.name)} className="px-2 py-1.5 rounded-lg hover:bg-destructive/10 text-foreground/20 hover:text-destructive text-[12px] transition-all" title="Delete index">✕</button>
                  </div>
                </div>
                {p.schema && (
                  <>
                    <div className="flex gap-6 text-[12px] text-foreground/30 mb-3">
                      <span><strong className="text-foreground/55 tabular-nums">{totalNodes.toLocaleString()}</strong> nodes</span>
                      <span><strong className="text-foreground/55 tabular-nums">{totalEdges.toLocaleString()}</strong> edges</span>
                    </div>
                    <div className="flex flex-wrap gap-1">
                      {p.schema.node_labels?.map((l) => (
                        <span key={l.label} className="inline-flex items-center gap-1 px-1.5 py-[2px] rounded-md text-[10px] font-medium" style={{ backgroundColor: colorForLabel(l.label) + "10", color: colorForLabel(l.label) + "bb" }}>
                          <span className="w-[4px] h-[4px] rounded-full" style={{ backgroundColor: colorForLabel(l.label) }} />
                          {l.label} {l.count.toLocaleString()}
                        </span>
                      ))}
                    </div>
                  </>
                )}
              </div>
            );
          })}
        </div>
      </div>
      {showModal && <CreateIndexModal onClose={() => setShowModal(false)} onCreated={() => { setIndexing(true); refresh(); }} />}
    </ScrollArea>
  );
}
