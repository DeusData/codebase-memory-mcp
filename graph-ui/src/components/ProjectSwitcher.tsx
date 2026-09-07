import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { callTool } from "../api/rpc";
import type { Project } from "../lib/types";

/* The project is the context every other view depends on, so it lives
 * first in the header as a switcher — not as a trailing chip gating a row
 * of dead tabs. */
export function ProjectSwitcher({
  selected,
  labels,
  allProjectsLabel,
  onSelect,
  onAllProjects,
}: {
  selected: string | null;
  labels: { select: string; search: string; manage: string };
  allProjectsLabel: string;
  onSelect: (project: string) => void;
  onAllProjects: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [projects, setProjects] = useState<Project[] | null>(null);
  const [query, setQuery] = useState("");
  const [anchor, setAnchor] = useState<{ top: number; left: number } | null>(null);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  const load = useCallback(() => {
    if (projects !== null) return;
    callTool<{ projects: Project[] }>("list_projects", {})
      .then((result) => setProjects(result.projects ?? []))
      .catch(() => setProjects([]));
  }, [projects]);

  useEffect(() => {
    if (!open) return;
    setQuery("");
    /* The panel is portaled to <body>, so anchor it to the button. */
    const rect = rootRef.current?.getBoundingClientRect();
    if (rect) setAnchor({ top: rect.bottom + 6, left: rect.left });
    /* Focus lands in the filter so typing narrows immediately. */
    const focusTimer = setTimeout(() => inputRef.current?.focus(), 0);
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
    };
    const onResize = () => setOpen(false);
    document.addEventListener("keydown", onKey);
    window.addEventListener("resize", onResize);
    return () => {
      clearTimeout(focusTimer);
      document.removeEventListener("keydown", onKey);
      window.removeEventListener("resize", onResize);
    };
  }, [open]);

  const filtered = useMemo(() => {
    if (projects === null) return null;
    const needle = query.trim().toLowerCase();
    if (!needle) return projects;
    return projects.filter((project) => project.name.toLowerCase().includes(needle));
  }, [projects, query]);

  return (
    <div ref={rootRef} className="relative shrink-0">
      <button
        onClick={() => {
          setOpen((v) => !v);
          load();
        }}
        className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md border text-[13px] transition-all max-w-[280px] ${
          selected
            ? "bg-popover border-border/40 text-foreground/85 hover:border-border"
            : "bg-primary/15 border-primary/40 text-primary hover:bg-primary/25"
        }`}
        title={selected ?? "Choose which indexed project to explore"}
      >
        <span className="font-mono truncate">{selected ?? labels.select}</span>
        <span className="text-foreground/40 text-[11px] shrink-0">▾</span>
      </button>
      {open &&
        anchor &&
        /* BOTH the backdrop and the panel are portaled to <body>. The
         * header's backdrop-filter makes it a stacking context, so anything
         * left inside paints as one atomic layer BELOW a body-level
         * overlay — the first version dimmed its own panel. Out here the
         * order is explicit: backdrop z-20, panel z-30. */
        createPortal(
          <>
            <div
              className="fixed inset-0 bg-black/50 z-20"
              onMouseDown={() => setOpen(false)}
            />
            <div
              className="fixed w-[340px] bg-surface-3 border border-border rounded-md shadow-2xl shadow-black/60 z-30 overflow-hidden"
              style={{ top: anchor.top, left: anchor.left }}
            >
            <div className="p-2 border-b border-border/60">
              <input
                ref={inputRef}
                type="text"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" && filtered && filtered.length > 0) {
                    setOpen(false);
                    onSelect(filtered[0].name);
                  }
                }}
                placeholder={labels.search}
                className="w-full bg-background border border-border/60 rounded-md px-2.5 py-1.5 text-[13px] text-foreground placeholder-foreground/35 outline-none focus:border-primary/60 transition-all"
              />
            </div>
            <div className="py-1 max-h-[50vh] overflow-y-auto">
              {filtered === null && (
                <p className="px-3 py-2 text-[13px] text-foreground/40">Loading…</p>
              )}
              {filtered !== null && filtered.length === 0 && (
                <p className="px-3 py-2 text-[13px] text-foreground/40">
                  {query ? `Nothing matches "${query}".` : "No indexed projects yet."}
                </p>
              )}
              {(filtered ?? []).map((project) => (
                <button
                  key={project.name}
                  onClick={() => {
                    setOpen(false);
                    onSelect(project.name);
                  }}
                  className={`flex items-center gap-2 w-full text-left px-3 py-1.5 transition-colors hover:bg-surface-4 ${
                    project.name === selected ? "text-primary" : "text-foreground/75"
                  }`}
                >
                  <span className="text-[13px] font-mono truncate">{project.name}</span>
                  {project.name === selected && (
                    <span className="ml-auto text-[11px] shrink-0">●</span>
                  )}
                </button>
              ))}
            </div>
              <div className="border-t border-border/60 py-1 bg-surface-3">
                <button
                  onClick={() => {
                    setOpen(false);
                    onAllProjects();
                  }}
                  className="w-full text-left px-3 py-1.5 text-[13px] text-foreground/55 hover:text-foreground/85 hover:bg-surface-4 transition-colors"
                >
                  {allProjectsLabel} — {labels.manage} →
                </button>
              </div>
            </div>
          </>,
          document.body,
        )}
    </div>
  );
}
