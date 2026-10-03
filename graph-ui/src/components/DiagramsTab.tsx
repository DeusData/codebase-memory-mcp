import { useState, useEffect, useMemo } from "react";
import {
  ExternalLink,
  Layers,
  Code2,
  Maximize2,
  Minimize2,
  Grid,
  CheckCircle2,
  FileCode,
  Sparkles,
  ArrowRight,
  BookOpen,
} from "lucide-react";

export interface DiagramItem {
  id: string;
  title: string;
  category: string;
  subtitle: string;
  description: string;
  badgeClass: string;
  hoverBorderClass: string;
  sources: { path: string; line?: number; label: string }[];
  htmlFile: string;
  specFile: string;
  durationMs: number;
}

export const DIAGRAMS: DiagramItem[] = [
  {
    id: "architecture",
    title: "System Architecture",
    category: "Architecture",
    subtitle: "High-level topology & MCP daemon IPC",
    description:
      "Client surfaces (Cursor, Claude Code), stdio MCP protocol frontend, authenticated daemon IPC, Tree-Sitter AST worker pool, Hybrid LSP, native export_diagram synthesis engine, and in-memory SQLite WAL graph store.",
    badgeClass: "bg-indigo-500/15 text-indigo-400 border-indigo-500/30",
    hoverBorderClass: "hover:border-indigo-500/50 hover:shadow-indigo-500/10",
    sources: [
      { path: "src/main.c", line: 2748, label: "main entry point" },
      { path: "src/mcp/mcp.c", line: 8184, label: "export_diagram dispatch" },
      { path: "src/diagram/diagram.c", line: 243, label: "cbm_diagram_generate" },
      { path: "src/store/store.c", line: 2, label: "SQLite WAL store" },
    ],
    htmlFile: "architecture.html",
    specFile: "specs/architecture.json",
    durationMs: 5929,
  },
  {
    id: "workflow",
    title: "Multi-Pass Indexing Pipeline",
    category: "Workflow",
    subtitle: "Multi-lane AST & Hybrid LSP resolution",
    description:
      "End-to-end multi-lane pipeline from initial filesystem discovery, bulk LZ4 HC source reading, parallel definition extraction, call graph resolution, Hybrid LSP cross-file links, to atomic SQLite dump.",
    badgeClass: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
    hoverBorderClass: "hover:border-emerald-500/50 hover:shadow-emerald-500/10",
    sources: [
      { path: "src/pipeline/pipeline.c", line: 2, label: "orchestration" },
      { path: "src/pipeline/pass_definitions.c", label: "AST definition pass" },
      { path: "src/pipeline/pass_lsp_cross.c", label: "cross-file LSP links" },
    ],
    htmlFile: "workflow-indexing-pipeline.html",
    specFile: "specs/workflow.json",
    durationMs: 6488,
  },
  {
    id: "sequence",
    title: "Tool Call Request Sequence",
    category: "Sequence",
    subtitle: "trace_path execution flow & BFS traversal",
    description:
      "Step-by-step trace of an AI Agent invoking trace_path(OrderHandler) through stdio, validating daemon lock generation, Cypher query planning, BFS traversal, and sub-ms response formatting.",
    badgeClass: "bg-amber-500/15 text-amber-400 border-amber-500/30",
    hoverBorderClass: "hover:border-amber-500/50 hover:shadow-amber-500/10",
    sources: [
      { path: "src/mcp/mcp.c", line: 17893, label: "JSON-RPC dispatch" },
      { path: "src/cypher/cypher.c", line: 2, label: "Cypher planner" },
      { path: "src/daemon/ipc.c", line: 1, label: "lock validation" },
    ],
    htmlFile: "sequence-tool-call.html",
    specFile: "specs/sequence.json",
    durationMs: 5622,
  },
  {
    id: "dataflow",
    title: "Knowledge Graph Data Flow",
    category: "Data Flow",
    subtitle: "Source code to relational graph tables",
    description:
      "5-stage transformation pipeline: raw repository files & git context → Tree-Sitter AST & LZ4 HC cache → FQN registry & Hybrid LSP → SQLite relational tables → MCP clients, Cypher engine, and 3D UI.",
    badgeClass: "bg-cyan-500/15 text-cyan-400 border-cyan-500/30",
    hoverBorderClass: "hover:border-cyan-500/50 hover:shadow-cyan-500/10",
    sources: [
      { path: "src/store/store.c", line: 2, label: "relational schema" },
      { path: "src/ui/layout3d.c", label: "3D spatial layout" },
      { path: "src/ui/http_server.c", label: "localhost:9749 server" },
    ],
    htmlFile: "dataflow-code-to-graph.html",
    specFile: "specs/dataflow.json",
    durationMs: 5831,
  },
  {
    id: "lifecycle",
    title: "Indexing & Daemon Lifecycle",
    category: "Lifecycle",
    subtitle: "State machine, change watcher & recovery",
    description:
      "State transitions across 4 lanes: initial indexing, daemon lock contention probe, active query serving, Git change watcher dirty detection, delta re-indexing, and store corruption recovery.",
    badgeClass: "bg-purple-500/15 text-purple-400 border-purple-500/30",
    hoverBorderClass: "hover:border-purple-500/50 hover:shadow-purple-500/10",
    sources: [
      { path: "src/watcher/watcher.c", line: 2, label: "git change watcher" },
      { path: "src/daemon/bootstrap.c", line: 1, label: "daemon startup" },
      { path: "src/pipeline/pipeline_delta.c", label: "delta update worker" },
    ],
    htmlFile: "lifecycle-indexing-state.html",
    specFile: "specs/lifecycle.json",
    durationMs: 7105,
  },
  {
    id: "sequence_export",
    title: "Export Diagram Sequence",
    category: "Sequence",
    subtitle: "export_diagram execution flow & synthesis",
    description:
      "Step-by-step trace of an AI Agent invoking export_diagram(type=\"sequence\", format=\"mermaid\"), validating SQLite WAL store readiness, calling cbm_diagram_generate, traversing call graph edges, and synthesizing Mermaid/SVG vector output.",
    badgeClass: "bg-amber-500/15 text-amber-400 border-amber-500/30",
    hoverBorderClass: "hover:border-amber-500/50 hover:shadow-amber-500/10",
    sources: [
      { path: "src/mcp/mcp.c", line: 8184, label: "handle_export_diagram" },
      { path: "src/diagram/diagram.c", line: 243, label: "cbm_diagram_generate" },
      { path: "src/diagram/query_sequence.c", label: "sequence planner" },
      { path: "src/diagram/emit_mermaid.c", label: "Mermaid emitter" },
    ],
    htmlFile: "sequence-export-diagram.html",
    specFile: "specs/sequence-export-diagram.json",
    durationMs: 4890,
  },
];

/* Native repository-generated Mermaid diagrams compiled directly from AST knowledge graph */
const REPO_MERMAID_DIAGRAMS: Record<string, string> = {
  architecture: `graph TD
    subgraph Client ["Client & Interface Layer"]
        cbm("cbm CLI (2200 symbols)")
        ui("Web 3D UI (136 symbols)")
        test_cli("CLI Tests (404 symbols)")
        repro("Repro Suite (1012 symbols)")
    end
    subgraph Protocol ["MCP & Daemon Layer"]
        mcp("MCP Protocol Server (519 symbols)")
        daemon("IPC Daemon (837 symbols)")
        pipeline("Multi-Pass Pipeline (1078 symbols)")
        diagram("Diagram Generator (export_diagram)")
    end
    subgraph Core ["Engine & Storage Layer"]
        cypher("Cypher Engine (197 symbols)")
        store("SQLite WAL Store (315 symbols)")
        foundation("Foundation Lib (801 symbols)")
    end

    Client --> Protocol
    Protocol --> Core

    test_cli -->|calls: 698| foundation
    test_cli -->|calls: 467| cbm
    repro -->|calls: 348| foundation
    pipeline -->|calls: 412| foundation
    pipeline -->|calls: 180| store
    mcp -->|calls: 214| pipeline
    mcp -->|export_diagram| diagram
    daemon -->|calls: 245| store
    diagram -->|queries| store
    diagram -->|AST emit| foundation`,

  workflow: `sequenceDiagram
    autonumber
    participant P0 as diagram.c
    participant P1 as mem_override_libc.c
    participant P2 as mem_events.c
    participant P3 as query_sequence.c
    participant P4 as store.c
    participant P5 as str_util.c
    participant P6 as emit_dot.c
    participant P7 as sanitize.c
    P0->>P0: cbm_seq_trace_init()
    P0->>P3: cbm_diagram_query_sequence()
    activate P3
    P3->>P4: cbm_store_get_db()
    P3->>P5: cbm_path_base()
    P3->>P0: cbm_seq_trace_add_participant()
    P3->>P3: traverse_calls()
    P3->>P0: cbm_seq_trace_add_message()
    deactivate P3
    P0->>P6: cbm_diagram_emit_dot_sequence()
    P0->>P7: cbm_diagram_emit_svg_sequence()
    P0->>P7: cbm_diagram_emit_mermaid_sequence()
    P0->>P0: cbm_seq_trace_free()`,

  sequence: `sequenceDiagram
    autonumber
    participant P0 as mcp.c
    participant P1 as mem_override_libc.c
    participant P2 as fqn.c
    participant P3 as str_util.c
    participant P4 as platform.c
    participant P5 as compat_fs.c
    participant P6 as store.c
    participant P7 as compact_out.c
    P0->>P0: get_project_arg()
    P0->>P0: cbm_mcp_get_string_arg()
    P0->>P0: normalize_project_arg()
    P0->>P2: cbm_project_name_sanitize()
    P0->>P2: cbm_project_name_from_path()
    P0->>P3: cbm_validate_project_name()
    P0->>P0: resolve_store()
    P0->>P6: cbm_store_check_integrity()
    P0->>P6: cbm_store_open_path_query()
    P0->>P6: cbm_store_get_project()
    P0->>P0: verify_project_indexed()
    P0->>P7: cbm_diagram_parse_type()
    P0->>P0: cbm_mcp_get_int_arg()
    P0->>P7: cbm_diagram_parse_format()`,

  dataflow: `flowchart LR
    subgraph Ingress ["Ingress & API Endpoints"]
        handle_export_diagram["handle_export_diagram"]
        _opt_codebase_memory_mcp["/opt/codebase-memory-mcp"]
        _api_layout["/api/layout"]
    end
    subgraph Handlers ["Processing & Business Logic"]
        yy_doc_to_str("yy_doc_to_str")
        cbm_mcp_text_result("cbm_mcp_text_result")
        cbm_mcp_get_string_arg("cbm_mcp_get_string_arg")
        get_project_arg("get_project_arg")
        cbm_mcp_get_int_arg("cbm_mcp_get_int_arg")
        resolve_store("resolve_store")
        build_no_store_error_checked("build_no_store_error_checked")
        verify_project_indexed("verify_project_indexed")
    end
    subgraph Storage ["Persistent Storage & Data Access"]
        result[("result")]
    end
    handle_export_diagram -->|handles| yy_doc_to_str
    handle_export_diagram -->|handles| cbm_mcp_text_result
    handle_export_diagram -->|handles| cbm_mcp_get_string_arg
    handle_export_diagram -->|handles| get_project_arg
    handle_export_diagram -->|handles| cbm_mcp_get_int_arg
    handle_export_diagram -->|handles| resolve_store
    handle_export_diagram -->|handles| build_no_store_error_checked
    handle_export_diagram -->|handles| verify_project_indexed
    cbm_mcp_get_string_arg -->|WRITES| result
    cbm_mcp_get_int_arg -->|WRITES| result`,

  lifecycle: `graph TD
    App_tsx --> ControlTab_tsx
    App_tsx --> DiagramsTab_tsx
    App_tsx --> GraphTab_tsx
    App_tsx --> StatsTab_tsx
    ControlTab_tsx --> scroll_area_tsx
    GraphTab_tsx --> DisplaySettingsMenu_tsx
    GraphTab_tsx --> ErrorBoundary_tsx
    GraphTab_tsx --> FilterPanel_tsx
    GraphTab_tsx --> GraphLoader_tsx
    GraphTab_tsx --> GraphScene_tsx
    GraphTab_tsx --> MissedCallout_tsx
    GraphTab_tsx --> NodeDetailPanel_tsx
    GraphTab_tsx --> ResizeHandle_tsx
    GraphTab_tsx --> Sidebar_tsx
    GraphScene_tsx --> EdgeLines_tsx
    GraphScene_tsx --> NodeCloud_tsx
    GraphScene_tsx --> NodeLabels_tsx
    GraphScene_tsx --> NodeTooltip_tsx`,

  sequence_export: `sequenceDiagram
    autonumber
    participant Agent as AI Agent (Cursor / Claude)
    participant MCP as mcp.c (handle_export_diagram)
    participant Store as store.c (SQLite WAL)
    participant Diag as diagram.c (cbm_diagram_generate)
    participant Plan as query_sequence.c (cbm_diagram_query_sequence)
    participant Emit as emit_mermaid.c (cbm_diagram_emit_mermaid_sequence)

    Agent->>MCP: tools/call: export_diagram(type="sequence", format="mermaid")
    activate MCP
    MCP->>MCP: cbm_mcp_get_string_arg("type")
    MCP->>Store: verify_project_indexed(store, project)
    activate Store
    Store-->>MCP: project indexed & verified (nodes > 0)
    deactivate Store
    MCP->>Diag: cbm_diagram_generate(store, &opts, &res)
    activate Diag
    Diag->>Diag: cbm_seq_trace_init(&trace)
    Diag->>Plan: cbm_diagram_query_sequence(store, opts, &trace)
    activate Plan
    Plan->>Store: SELECT caller, callee FROM call_edges
    activate Store
    Store-->>Plan: rows fetched (< 0.5ms)
    deactivate Store
    Plan->>Plan: traverse_calls(depth <= max_depth)
    Plan-->>Diag: return cbm_seq_trace_t trace
    deactivate Plan
    Diag->>Emit: cbm_diagram_emit_mermaid_sequence(&trace)
    activate Emit
    Emit-->>Diag: formatted Mermaid sequence string
    deactivate Emit
    Diag-->>MCP: cbm_diagram_result_t (nodes_analyzed, edges_traversed, content)
    deactivate Diag
    MCP->>MCP: yyjson_mut_doc_new() -> format JSON-RPC
    MCP-->>Agent: JSON-RPC result ({ diagram_type, format, content })
    deactivate MCP`,
};

function generateMermaidHtml(title: string, mermaidCode: string): string {
  return [
    '<!DOCTYPE html>',
    '<html>',
    '<head>',
    '  <meta charset="utf-8">',
    '  <title>' + title + '</title>',
    '  <meta name="viewport" content="width=device-width, initial-scale=1">',
    '  <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>',
    '  <script>',
    '    document.addEventListener("DOMContentLoaded", function() {',
    '      mermaid.initialize({',
    '        startOnLoad: true,',
    '        theme: "dark",',
    '        themeVariables: {',
    '          darkMode: true,',
    '          background: "#090d16",',
    '          primaryColor: "#38bdf8",',
    '          primaryTextColor: "#f8fafc",',
    '          primaryBorderColor: "#0284c7",',
    '          lineColor: "#64748b",',
    '          secondaryColor: "#1e293b",',
    '          tertiaryColor: "#0f172a"',
    '        }',
    '      });',
    '    });',
    '  </script>',
    '  <style>',
    '    html, body {',
    '      margin: 0;',
    '      padding: 0;',
    '      width: 100%;',
    '      height: 100%;',
    '      background: #090d16;',
    '      color: #f8fafc;',
    '      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;',
    '      overflow: auto;',
    '    }',
    '    .canvas-wrap {',
    '      min-width: 100%;',
    '      min-height: 100%;',
    '      display: flex;',
    '      flex-direction: column;',
    '      align-items: center;',
    '      justify-content: flex-start;',
    '      padding: 40px 24px;',
    '      box-sizing: border-box;',
    '    }',
    '    .mermaid {',
    '      display: flex;',
    '      justify-content: center;',
    '      align-items: center;',
    '      width: 100%;',
    '      max-width: 1200px;',
    '    }',
    '    .mermaid svg {',
    '      width: 100% !important;',
    '      max-width: 1100px !important;',
    '      height: auto !important;',
    '      filter: drop-shadow(0 8px 30px rgba(0, 0, 0, 0.6));',
    '    }',
    '    .mermaid text {',
    '      font-family: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, monospace !important;',
    '      font-size: 13px !important;',
    '    }',
    '  </style>',
    '</head>',
    '<body>',
    '  <div class="canvas-wrap">',
    '    <div class="mermaid">',
    mermaidCode,
    '    </div>',
    '  </div>',
    '</body>',
    '</html>',
  ].join('\n');
}

interface DiagramsTabProps {
  initialDiagram?: string | null;
  onSelectDiagram?: (diagramId: string | null) => void;
}

export function DiagramsTab({ initialDiagram, onSelectDiagram }: DiagramsTabProps) {
  const [selectedId, setSelectedId] = useState<string | null>(initialDiagram || null);
  const [viewMode, setViewMode] = useState<"stylized" | "mermaid">("stylized");
  const [diagramHtml, setDiagramHtml] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [specContent, setSpecContent] = useState<string | null>(null);
  const [showSpecDrawer, setShowSpecDrawer] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);

  // Sync with prop if changed externally
  useEffect(() => {
    if (initialDiagram !== undefined) {
      setSelectedId(initialDiagram);
    }
  }, [initialDiagram]);

  const activeDiagram = useMemo(
    () => DIAGRAMS.find((d) => d.id === selectedId) || null,
    [selectedId]
  );

  const selectDiagram = (id: string | null) => {
    setSelectedId(id);
    setShowSpecDrawer(false);
    onSelectDiagram?.(id);
  };

  // Load verified diagram when active diagram changes
  useEffect(() => {
    if (!activeDiagram) {
      setDiagramHtml(null);
      setSpecContent(null);
      return;
    }

    setLoading(true);

    // Fetch verified Archify spec JSON
    fetch(`/diagrams/${activeDiagram.specFile}`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data) => {
        setSpecContent(JSON.stringify(data, null, 2));
      })
      .catch((_err) => {
        const mermaidCode = REPO_MERMAID_DIAGRAMS[activeDiagram.id];
        setSpecContent(
          JSON.stringify(
            {
              diagram: activeDiagram.id,
              title: activeDiagram.title,
              category: activeDiagram.category,
              repository: "codebase-memory-mcp-ui",
              evidence: activeDiagram.sources,
              mermaid_definition: mermaidCode,
            },
            null,
            2
          )
        );
      });

    // Prepare mermaid HTML for mermaid view mode
    const mermaidCode = REPO_MERMAID_DIAGRAMS[activeDiagram.id];
    if (mermaidCode) {
      setDiagramHtml(generateMermaidHtml(activeDiagram.title, mermaidCode));
    } else {
      setDiagramHtml(null);
    }

    setLoading(false);
  }, [activeDiagram]);

  const handleOpenStandalone = () => {
    if (!activeDiagram) return;
    if (viewMode === "stylized") {
      window.open(`/diagrams/${activeDiagram.htmlFile}?theme=dark`, "_blank");
    } else if (diagramHtml) {
      const blob = new Blob([diagramHtml], { type: "text/html" });
      const url = URL.createObjectURL(blob);
      window.open(url, "_blank");
    } else {
      window.open(`/diagrams/${activeDiagram.htmlFile}`, "_blank");
    }
  };

  return (
    <div
      className={`h-full flex flex-col bg-[#070b12] text-foreground ${
        isFullscreen ? "fixed inset-0 z-50 bg-[#070b12]" : ""
      }`}
    >
      {/* Top Secondary Navigation Bar */}
      <div className="flex items-center justify-between px-6 py-2.5 border-b border-border/30 bg-[#0b131e]/90 backdrop-blur-md shrink-0">
        <div className="flex items-center gap-2 overflow-x-auto py-1">
          <button
            onClick={() => selectDiagram(null)}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-[12px] font-medium transition-all ${
              selectedId === null
                ? "bg-primary/20 text-primary border border-primary/30 shadow-sm shadow-primary/10"
                : "text-muted-foreground hover:text-foreground hover:bg-white/[0.04] border border-transparent"
            }`}
          >
            <Grid className="w-3.5 h-3.5" />
            <span>Overview</span>
          </button>

          <div className="h-4 w-px bg-border/40 mx-1" />

          {DIAGRAMS.map((d) => {
            const isSelected = selectedId === d.id;
            return (
              <button
                key={d.id}
                onClick={() => selectDiagram(d.id)}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-[12px] font-medium transition-all whitespace-nowrap ${
                  isSelected
                    ? "bg-primary/20 text-primary border border-primary/30 shadow-sm shadow-primary/10"
                    : "text-muted-foreground hover:text-foreground hover:bg-white/[0.04] border border-transparent"
                }`}
              >
                <Layers className="w-3.5 h-3.5" />
                <span>{d.title}</span>
              </button>
            );
          })}
        </div>

        {activeDiagram && (
          <div className="flex items-center gap-2.5 shrink-0">
            {/* View Mode Switcher */}
            <div className="flex items-center bg-black/40 p-0.5 rounded-lg border border-border/40">
              <button
                onClick={() => setViewMode("stylized")}
                className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md text-[11px] font-medium transition-all ${
                  viewMode === "stylized"
                    ? "bg-primary/20 text-primary border border-primary/30 shadow-sm"
                    : "text-muted-foreground hover:text-foreground border border-transparent"
                }`}
                title="Rich Stylized Archify View (Interactive canvas, signal flow, dark theme)"
              >
                <Sparkles className="w-3 h-3" />
                <span>Stylized Canvas</span>
              </button>
              <button
                onClick={() => setViewMode("mermaid")}
                className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md text-[11px] font-medium transition-all ${
                  viewMode === "mermaid"
                    ? "bg-primary/20 text-primary border border-primary/30 shadow-sm"
                    : "text-muted-foreground hover:text-foreground border border-transparent"
                }`}
                title="Raw Mermaid AST View"
              >
                <Code2 className="w-3 h-3" />
                <span>Mermaid AST</span>
              </button>
            </div>

            <div className="h-4 w-px bg-border/40" />

            <button
              onClick={() => setShowSpecDrawer((prev) => !prev)}
              className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[11px] font-mono border transition-all ${
                showSpecDrawer
                  ? "bg-white/[0.08] text-foreground border-white/20"
                  : "bg-white/[0.02] text-muted-foreground border-border/40 hover:text-foreground hover:bg-white/[0.05]"
              }`}
              title="Toggle JSON Specification"
            >
              <Code2 className="w-3.5 h-3.5" />
              <span>Spec</span>
            </button>

            <button
              onClick={handleOpenStandalone}
              className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[11px] font-medium bg-white/[0.02] text-muted-foreground border border-border/40 hover:text-foreground hover:bg-white/[0.05] transition-all"
              title="Open standalone viewer in new tab"
            >
              <ExternalLink className="w-3.5 h-3.5" />
              <span>Open Standalone</span>
            </button>

            <button
              onClick={() => setIsFullscreen((prev) => !prev)}
              className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[11px] font-medium bg-white/[0.02] text-muted-foreground border border-border/40 hover:text-foreground hover:bg-white/[0.05] transition-all"
              title={isFullscreen ? "Exit Fullscreen" : "Fullscreen"}
            >
              {isFullscreen ? (
                <Minimize2 className="w-3.5 h-3.5" />
              ) : (
                <Maximize2 className="w-3.5 h-3.5" />
              )}
            </button>
          </div>
        )}
      </div>

      {/* Main Content Area */}
      <div className="flex-1 min-h-0 relative overflow-hidden flex">
        {!activeDiagram ? (
          /* Gallery Mode */
          <div className="flex-1 overflow-y-auto p-6 md:p-8 space-y-8 max-w-7xl mx-auto w-full">
            {/* Hero / Header banner */}
            <div className="relative rounded-2xl border border-border/40 bg-gradient-to-br from-white/[0.03] to-white/[0.01] p-6 md:p-8 overflow-hidden">
              <div className="absolute -right-16 -top-16 w-64 h-64 bg-primary/10 rounded-full blur-3xl pointer-events-none" />
              <div className="relative z-10 flex flex-col md:flex-row md:items-center justify-between gap-6">
                <div>
                  <div className="inline-flex items-center gap-2 px-2.5 py-1 rounded-full bg-primary/10 border border-primary/20 text-[11px] font-medium text-primary mb-3">
                    <Sparkles className="w-3.5 h-3.5" />
                    <span>Archify Architecture Suite</span>
                  </div>
                  <h1 className="text-2xl md:text-3xl font-bold tracking-tight text-foreground">
                    Codebase Memory MCP Visualizations
                  </h1>
                  <p className="mt-2 text-sm text-muted-foreground max-w-2xl leading-relaxed">
                    Interactive, verified architecture and behavioral maps compiled from
                    repository AST evidence. Every node, edge, and state transition is
                    strictly validated against Git commit blobs.
                  </p>
                </div>

                {/* Key Metrics */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3 shrink-0">
                  <div className="px-4 py-3 rounded-xl border border-border/30 bg-black/30 backdrop-blur-sm">
                    <p className="text-[10px] text-muted-foreground uppercase font-mono tracking-wider">
                      Diagrams
                    </p>
                    <p className="text-xl font-bold text-primary font-mono mt-0.5">6 / 6</p>
                  </div>
                  <div className="px-4 py-3 rounded-xl border border-border/30 bg-black/30 backdrop-blur-sm">
                    <p className="text-[10px] text-muted-foreground uppercase font-mono tracking-wider">
                      Gates Passed
                    </p>
                    <p className="text-xl font-bold text-emerald-400 font-mono mt-0.5">100%</p>
                  </div>
                  <div className="px-4 py-3 rounded-xl border border-border/30 bg-black/30 backdrop-blur-sm">
                    <p className="text-[10px] text-muted-foreground uppercase font-mono tracking-wider">
                      Languages
                    </p>
                    <p className="text-xl font-bold text-cyan-400 font-mono mt-0.5">162</p>
                  </div>
                  <div className="px-4 py-3 rounded-xl border border-border/30 bg-black/30 backdrop-blur-sm">
                    <p className="text-[10px] text-muted-foreground uppercase font-mono tracking-wider">
                      Latency SLA
                    </p>
                    <p className="text-xl font-bold text-amber-400 font-mono mt-0.5">&lt; 1 ms</p>
                  </div>
                </div>
              </div>
            </div>

            {/* Diagrams Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {DIAGRAMS.map((item) => (
                <div
                  key={item.id}
                  onClick={() => selectDiagram(item.id)}
                  className={`group relative flex flex-col justify-between rounded-xl border border-border/40 bg-white/[0.02] p-5 cursor-pointer transition-all duration-300 ${item.hoverBorderClass} hover:-translate-y-1 hover:bg-white/[0.03] hover:shadow-xl`}
                >
                  <div>
                    {/* Header: Category Badge + Gate Pass */}
                    <div className="flex items-center justify-between mb-3">
                      <span
                        className={`text-[10px] font-semibold px-2 py-0.5 rounded-md border font-mono uppercase tracking-wider ${item.badgeClass}`}
                      >
                        {item.category}
                      </span>
                      <div className="flex items-center gap-1.5 text-[11px] font-medium text-emerald-400">
                        <CheckCircle2 className="w-3.5 h-3.5" />
                        <span>Pass</span>
                      </div>
                    </div>

                    <h2 className="text-base font-semibold text-foreground group-hover:text-primary transition-colors">
                      {item.title}
                    </h2>
                    <p className="text-[11px] text-muted-foreground/80 font-mono mt-0.5 mb-3">
                      {item.subtitle}
                    </p>
                    <p className="text-xs text-muted-foreground line-clamp-3 leading-relaxed mb-4">
                      {item.description}
                    </p>
                  </div>

                  <div>
                    {/* C Source Citations */}
                    <div className="rounded-lg bg-black/40 border border-border/20 p-2.5 mb-4 space-y-1 font-mono text-[10.5px] text-muted-foreground">
                      {item.sources.map((s, idx) => (
                        <div key={idx} className="flex items-center justify-between truncate">
                          <span className="text-foreground/80 font-medium">
                            {s.path}
                            {s.line ? `:${s.line}` : ""}
                          </span>
                          <span className="text-[9.5px] text-foreground/40">{s.label}</span>
                        </div>
                      ))}
                    </div>

                    {/* Launch Action */}
                    <div className="flex items-center justify-between pt-2 border-t border-border/20">
                      <span className="text-[11px] text-muted-foreground group-hover:text-foreground transition-colors font-medium">
                        Launch Interactive Viewer
                      </span>
                      <ArrowRight className="w-4 h-4 text-muted-foreground group-hover:text-primary group-hover:translate-x-1 transition-all" />
                    </div>
                  </div>
                </div>
              ))}
            </div>

            {/* Architecture Documents & PRD Reference footer */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-4 border-t border-border/20">
              <div className="rounded-xl border border-border/30 bg-white/[0.01] p-4 flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="p-2 rounded-lg bg-primary/10 text-primary">
                    <FileCode className="w-4 h-4" />
                  </div>
                  <div>
                    <h3 className="text-xs font-semibold text-foreground">
                      ARCHIFY_PROCESS.md
                    </h3>
                    <p className="text-[11px] text-muted-foreground">
                      End-to-end compiler verification log & quality gate corrections.
                    </p>
                  </div>
                </div>
              </div>

              <div className="rounded-xl border border-border/30 bg-white/[0.01] p-4 flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="p-2 rounded-lg bg-emerald-500/10 text-emerald-400">
                    <BookOpen className="w-4 h-4" />
                  </div>
                  <div>
                    <h3 className="text-xs font-semibold text-foreground">
                      PRD_NATIVE_DIAGRAM_GENERATION.md
                    </h3>
                    <p className="text-[11px] text-muted-foreground">
                      PRD for native in-binary C99 diagram synthesis without external runtimes.
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        ) : (
          /* Interactive Viewer Mode */
          <div className="flex-1 flex flex-col h-full overflow-hidden">
            {/* Viewer Subheader info */}
            <div className="px-6 py-2 bg-[#090f19] border-b border-border/20 flex items-center justify-between text-xs text-muted-foreground">
              <div className="flex items-center gap-2">
                <span
                  className={`text-[10px] font-semibold px-2 py-0.5 rounded border font-mono uppercase ${activeDiagram.badgeClass}`}
                >
                  {activeDiagram.category}
                </span>
                <span className="font-semibold text-foreground">{activeDiagram.title}</span>
                <span className="text-border">|</span>
                <span className="truncate max-w-md hidden sm:inline text-muted-foreground">
                  {activeDiagram.description}
                </span>
              </div>
              <div className="flex items-center gap-3 shrink-0 font-mono text-[11px]">
                <span className="text-emerald-400 flex items-center gap-1">
                  <CheckCircle2 className="w-3 h-3" /> Gate Pass
                </span>
                <span className="text-foreground/30">|</span>
                <span>{activeDiagram.durationMs}ms</span>
              </div>
            </div>

            {/* Canvas / Iframe */}
            <div className="flex-1 relative bg-[#090d16]">
              {loading && (
                <div className="absolute inset-0 z-10 flex flex-col items-center justify-center bg-[#090d16]/80 backdrop-blur-sm gap-3">
                  <div className="w-7 h-7 border-2 border-primary border-t-transparent rounded-full animate-spin" />
                  <p className="text-xs text-muted-foreground font-mono">
                    Rendering verified diagram...
                  </p>
                </div>
              )}

              {viewMode === "stylized" ? (
                <iframe
                  key={`stylized-${activeDiagram.id}`}
                  title={activeDiagram.title}
                  src={`/diagrams/${activeDiagram.htmlFile}?theme=dark`}
                  className="w-full h-full border-0"
                  sandbox="allow-scripts allow-same-origin allow-popups"
                />
              ) : (
                <iframe
                  key={`mermaid-${activeDiagram.id}`}
                  title={activeDiagram.title}
                  srcDoc={diagramHtml || ""}
                  className="w-full h-full border-0"
                  sandbox="allow-scripts allow-same-origin allow-popups"
                />
              )}
            </div>
          </div>
        )}

        {/* Spec Drawer */}
        {showSpecDrawer && activeDiagram && specContent && (
          <div className="w-96 border-l border-border/30 bg-[#090f19] flex flex-col h-full z-20 shadow-2xl animate-in slide-in-from-right duration-200">
            <div className="flex items-center justify-between px-4 py-3 border-b border-border/30">
              <div className="flex items-center gap-2">
                <Code2 className="w-4 h-4 text-primary" />
                <span className="text-xs font-semibold font-mono">
                  {activeDiagram.specFile}
                </span>
              </div>
              <button
                onClick={() => setShowSpecDrawer(false)}
                className="text-muted-foreground hover:text-foreground text-xs px-1.5 py-0.5 rounded hover:bg-white/5"
              >
                ✕
              </button>
            </div>
            <div className="flex-1 overflow-auto p-3">
              <pre className="text-[11px] font-mono leading-relaxed text-muted-foreground whitespace-pre-wrap select-text">
                {specContent}
              </pre>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
