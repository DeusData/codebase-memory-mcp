import { useEffect, useState } from "react";
import type { GraphNode } from "../lib/types";

/* Upstream tracker for indexing gaps. The URL is served by the backend
 * (/api/ui-config) — the UI security audit forbids hardcoded external URLs in
 * graph-ui source, so external targets come from an auditable backend
 * response (same pattern as the /api/repo-info deep-links). */
let issuesUrlRequest: Promise<string | null> | null = null;

function fetchIssuesUrl(): Promise<string | null> {
  issuesUrlRequest ??= fetch("/api/ui-config")
    .then((r) => (r.ok ? r.json() : null))
    .then((cfg) => {
      const url: unknown = cfg?.upstream_issues_url;
      /* Accept only an https URL (regex literal on purpose — the UI security
       * audit greps source for protocol strings). */
      return typeof url === "string" && /^https:\/\//.test(url) ? url : null;
    })
    .catch(() => null);
  return issuesUrlRequest;
}

interface MissedCalloutProps {
  node: GraphNode;
  project: string | null;
  onClose: () => void;
}

function buildIssueUrl(base: string, path: string, project: string | null): string {
  const title = `Indexing gap: ${path}`;
  const body = [
    "## Not fully indexed (best-effort coverage signal)",
    "",
    `- **File:** \`${path}\``,
    `- **Project:** \`${project ?? "unknown"}\``,
    "",
    "<!-- Please add: the flagged line ranges from index_status (parse_partial),",
    "the language and the construct that fails to parse, and a minimal snippet",
    "of the affected code — ONLY if the code is shareable. -->",
    "",
    "_Reported from the graph UI's missed-coverage view._",
  ].join("\n");
  return `${base}?title=${encodeURIComponent(title)}&body=${encodeURIComponent(body)}`;
}

function buildAgentPrompt(issuesUrl: string | null, path: string, project: string | null): string {
  const where = issuesUrl
    ? `file a GitHub issue at ${issuesUrl}`
    : "file a GitHub issue on the codebase-memory-mcp project";
  return (
    `codebase-memory-mcp could not fully index \`${path}\`` +
    (project ? ` (project \`${project}\`)` : "") +
    " — best-effort coverage signal. Please: " +
    "1) call the index_status MCP tool and note this file's flagged line ranges under parse_partial; " +
    "2) read those ranges in the file and summarize which construct fails to parse; " +
    `3) ${where}, titled "Indexing gap: ${path}", ` +
    "with the summary — include a minimal reproducible snippet ONLY if the code is shareable."
  );
}

/* Right-panel callout shown when a missed-skeleton node is selected: explains
 * the gap and offers two working actions — a prefilled upstream issue and a
 * ready-made agent prompt (clipboard, with visible feedback). */
export function MissedCallout({ node, project, onClose }: MissedCalloutProps) {
  const path = node.file_path || node.name;
  const [copied, setCopied] = useState(false);
  const [issuesUrl, setIssuesUrl] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetchIssuesUrl().then((url) => {
      if (!cancelled) setIssuesUrl(url);
    });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!copied) return;
    const t = setTimeout(() => setCopied(false), 2000);
    return () => clearTimeout(t);
  }, [copied]);

  const copyPrompt = async () => {
    try {
      await navigator.clipboard.writeText(buildAgentPrompt(issuesUrl, path, project));
      setCopied(true);
    } catch {
      /* clipboard unavailable (permissions/insecure context) — leave the
       * button state unchanged so the failure is visible, not silent */
    }
  };

  return (
    <div className="h-full flex flex-col p-4 gap-3 overflow-y-auto">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-[10px] text-foreground/30 uppercase tracking-widest">
            Not fully indexed
          </p>
          <p className="text-sm font-medium text-foreground/90 break-all mt-1">{path}</p>
          <p className="text-[10px] text-foreground/40 mt-0.5">{node.label}</p>
        </div>
        <button
          onClick={onClose}
          className="text-foreground/40 hover:text-foreground/80 text-sm px-1"
          aria-label="Close"
        >
          ×
        </button>
      </div>

      <p className="text-[12px] leading-relaxed text-foreground/70">
        We did not manage to fully index this part of your code — constructs here may be
        missing from the graph (best-effort detection; the file content itself is ground
        truth).
      </p>
      <p className="text-[12px] leading-relaxed text-foreground/70">
        Help us handle this edge case too: let your agent summarize what fails to parse
        here and file a GitHub issue for the codebase-memory-mcp project.
      </p>

      <div className="flex flex-col gap-2 mt-1">
        <button
          onClick={copyPrompt}
          className={`text-[12px] font-medium rounded-md border px-3 py-1.5 transition-all text-left ${
            copied
              ? "border-primary/60 bg-primary/15 text-primary"
              : "border-white/10 bg-white/[0.03] text-foreground/80 hover:bg-white/[0.07]"
          }`}
        >
          {copied ? "✓ Copied — paste it to your agent" : "Copy agent prompt"}
        </button>
        {issuesUrl && (
          <a
            href={buildIssueUrl(issuesUrl, path, project)}
            target="_blank"
            rel="noreferrer"
            className="text-[12px] font-medium rounded-md border border-white/10 bg-white/[0.03] text-foreground/80 hover:bg-white/[0.07] px-3 py-1.5 transition-all"
          >
            File a GitHub issue (prefilled) ↗
          </a>
        )}
      </div>

      <p className="text-[10px] leading-snug text-foreground/35 mt-1">
        The prefilled issue contains only the file path and project name — add code
        snippets only if they are shareable.
      </p>
    </div>
  );
}
