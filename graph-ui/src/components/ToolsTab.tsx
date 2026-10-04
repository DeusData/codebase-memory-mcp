import { useState, useMemo } from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import type { McpToolDefinition } from "../lib/types";
import {
  Search,
  Copy,
  Check,
  Terminal,
  ShieldCheck,
  Layers,
  Wrench,
  Code2,
  ChevronDown,
  ChevronRight,
  Sparkles,
  GitBranch,
  Database,
  Compass,
} from "lucide-react";

export const MCP_TOOLS: McpToolDefinition[] = [
  {
    name: "search_graph",
    category: "Code Discovery",
    summary: "Search symbols by BM25 query, regex patterns, or semantic embeddings",
    description:
      "Find symbols via BM25 query, regex name/qn filters, or semantic_query. Rows keep qualified names, file paths, line ranges, and in/out degrees over CALLS, USAGE, CALL_REFERENCE, INHERITS, and IMPLEMENTS edges.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "project", type: "string", required: true, description: "Target project identifier" },
      { name: "query", type: "string", required: false, description: "BM25 full-text symbol search string" },
      { name: "name_pattern", type: "string", required: false, description: "Regex pattern matched against symbol name" },
      { name: "qn_pattern", type: "string", required: false, description: "Regex pattern matched against fully qualified name" },
      { name: "file_pattern", type: "string", required: false, description: "Glob or regex pattern to filter source file paths" },
      { name: "label", type: "string", required: false, description: "Filter by node label (e.g. Function, Class, Route, Interface)" },
      { name: "relationship", type: "string", required: false, description: "Filter connected edge type (e.g. CALLS, INHERITS, READS)" },
      { name: "min_degree", type: "integer", required: false, description: "Minimum connected edge degree filter" },
      { name: "max_degree", type: "integer", required: false, description: "Maximum connected edge degree filter" },
      { name: "exclude_entry_points", type: "boolean", required: false, defaultVal: false, description: "Exclude top-level entry point symbols" },
      { name: "include_connected", type: "boolean", required: false, defaultVal: false, description: "Include 1-hop connected neighbor symbols" },
      { name: "semantic_query", type: "string[]", required: false, description: "Array of natural language search queries for vector search" },
      { name: "limit", type: "integer", required: false, defaultVal: 50, description: "Maximum symbols to return (1-500)" },
      { name: "offset", type: "integer", required: false, defaultVal: 0, description: "Paging offset" },
      { name: "format", type: "string", required: false, defaultVal: "tree", enumVals: ["tree", "json"], description: "Output format" },
    ],
    cliExample: "cbm cli search_graph --project={PROJECT} --name_pattern=\".*Handler.*\" --limit=20",
    jsonExample: JSON.stringify(
      {
        name: "search_graph",
        arguments: {
          project: "{PROJECT}",
          name_pattern: ".*Handler.*",
          limit: 20,
        },
      },
      null,
      2
    ),
  },
  {
    name: "query_graph",
    category: "Code Discovery",
    summary: "Execute read-only Cypher queries for multi-hop graph analysis",
    description:
      "Read-only open Cypher query engine for multi-hop path traversal, aggregations, complexity metrics, or cross-service flows. Defaults to 200 visible rows with exact or lower-bound totals.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "project", type: "string", required: true, description: "Target project identifier" },
      { name: "query", type: "string", required: true, description: "Open Cypher query string" },
      { name: "graph", type: "string", required: false, defaultVal: "code", enumVals: ["code", "missed"], description: "Target code graph or missed coverage gap file tree" },
      { name: "max_rows", type: "integer", required: false, defaultVal: 200, description: "Visible rows ceiling (default 200, max 99998)" },
      { name: "offset", type: "integer", required: false, defaultVal: 0, description: "Live compatibility paging offset" },
      { name: "cursor", type: "string", required: false, description: "Snapshot continuation token" },
      { name: "format", type: "string", required: false, defaultVal: "tree", enumVals: ["tree", "json"], description: "Output format" },
    ],
    cliExample: "cbm cli query_graph --project={PROJECT} --query=\"MATCH (f:Function)-[:CALLS]->(c) RETURN f.name, count(c) LIMIT 10\"",
    jsonExample: JSON.stringify(
      {
        name: "query_graph",
        arguments: {
          project: "{PROJECT}",
          query: "MATCH (f:Function)-[:CALLS]->(c) RETURN f.name, count(c) ORDER BY count(c) DESC LIMIT 10",
        },
      },
      null,
      2
    ),
  },
  {
    name: "trace_path",
    category: "Tracing & Architecture",
    summary: "Trace inbound callers, outbound callees, data flows, and cross-service edges",
    description:
      "Trace callers/callees, data flow, or cross-service paths. Defaults exclude tests and framework boilerplates. Supports recursive BFS traversal with configurable depth limits.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "project", type: "string", required: true, description: "Target project identifier" },
      { name: "function_name", type: "string", required: true, description: "Root function or symbol name to trace" },
      { name: "direction", type: "string", required: false, defaultVal: "outbound", enumVals: ["inbound", "outbound", "bidirectional"], description: "Trace direction: inbound callers or outbound callees" },
      { name: "mode", type: "string", required: false, defaultVal: "calls", enumVals: ["calls", "data_flow", "cross_service"], description: "Edge filter mode" },
      { name: "max_depth", type: "integer", required: false, defaultVal: 3, description: "Maximum recursive traversal depth (1-10)" },
      { name: "format", type: "string", required: false, defaultVal: "tree", enumVals: ["tree", "json"], description: "Output format" },
    ],
    cliExample: "cbm cli trace_path --project={PROJECT} --function_name=\"process_payment\" --direction=outbound --max_depth=3",
    jsonExample: JSON.stringify(
      {
        name: "trace_path",
        arguments: {
          project: "{PROJECT}",
          function_name: "process_payment",
          direction: "outbound",
          max_depth: 3,
        },
      },
      null,
      2
    ),
  },
  {
    name: "export_diagram",
    category: "Tracing & Architecture",
    summary: "Native C99 diagram synthesis emitting Mermaid, DOT, SVG, and Archify JSON specs",
    description:
      "High-speed native C99 diagram synthesis engine. Directly queries the SQLite knowledge graph and outputs Mermaid diagrams, Graphviz DOT graphs, SVG vector renders, or Archify JSON specifications in sub-10ms.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "project", type: "string", required: true, description: "Target project identifier" },
      { name: "type", type: "string", required: true, enumVals: ["architecture", "sequence", "dataflow", "dependencies"], description: "Diagram type to generate" },
      { name: "format", type: "string", required: false, defaultVal: "mermaid", enumVals: ["mermaid", "dot", "svg", "json"], description: "Output syntax format" },
      { name: "entry_point", type: "string", required: false, description: "Root symbol or function for sequence or dataflow tracing" },
      { name: "scope_path", type: "string", required: false, description: "Optional folder/package path to scope diagram boundaries" },
      { name: "max_depth", type: "integer", required: false, defaultVal: 3, description: "Nesting depth for package hierarchy or call trees" },
    ],
    cliExample: "cbm diagram architecture --project={PROJECT} --format=mermaid",
    jsonExample: JSON.stringify(
      {
        name: "export_diagram",
        arguments: {
          project: "{PROJECT}",
          type: "architecture",
          format: "mermaid",
        },
      },
      null,
      2
    ),
  },
  {
    name: "get_code_snippet",
    category: "Code Discovery",
    summary: "Fetch precise source code for a symbol, function, or line range",
    description:
      "Retrieve exact source code for a symbol by qualified name, file path and line numbers, or symbol hash without reading whole files.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "project", type: "string", required: true, description: "Target project identifier" },
      { name: "qualified_name", type: "string", required: false, description: "Fully qualified symbol name (e.g. pkg/orders.OrderHandler.Process)" },
      { name: "file_path", type: "string", required: false, description: "Source file path relative to repo root" },
      { name: "start_line", type: "integer", required: false, description: "Optional 1-indexed starting line number" },
      { name: "end_line", type: "integer", required: false, description: "Optional 1-indexed ending line number" },
    ],
    cliExample: "cbm cli get_code_snippet --project={PROJECT} --qualified_name=\"OrderHandler.Process\"",
    jsonExample: JSON.stringify(
      {
        name: "get_code_snippet",
        arguments: {
          project: "{PROJECT}",
          qualified_name: "OrderHandler.Process",
        },
      },
      null,
      2
    ),
  },
  {
    name: "get_file_outline",
    category: "Code Discovery",
    summary: "Extract top-level symbol outline, functions, classes, and imports of a file",
    description:
      "Extract the top-level symbol outline, functions, classes, interfaces, and imports of a source file without loading full implementations.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "project", type: "string", required: true, description: "Target project identifier" },
      { name: "file_path", type: "string", required: true, description: "Source file path relative to repo root" },
      { name: "depth", type: "integer", required: false, defaultVal: 2, description: "Nesting depth of AST symbols to return" },
    ],
    cliExample: "cbm cli get_file_outline --project={PROJECT} --file_path=\"src/main.c\"",
    jsonExample: JSON.stringify(
      {
        name: "get_file_outline",
        arguments: {
          project: "{PROJECT}",
          file_path: "src/main.c",
          depth: 2,
        },
      },
      null,
      2
    ),
  },
  {
    name: "get_architecture",
    category: "Tracing & Architecture",
    summary: "Condense repository into architectural layers, packages, and dependency cycles",
    description:
      "Condense repository into architectural layers, subsystems, and package groups. Runs Tarjan's Strongly-Connected Components (SCC) algorithm to flag circular dependencies.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "project", type: "string", required: true, description: "Target project identifier" },
      { name: "scope_path", type: "string", required: false, description: "Optional folder/package path to isolate architecture" },
      { name: "detect_cycles", type: "boolean", required: false, defaultVal: true, description: "Run Tarjan's SCC to identify circular package dependencies" },
    ],
    cliExample: "cbm cli get_architecture --project={PROJECT} --detect_cycles=true",
    jsonExample: JSON.stringify(
      {
        name: "get_architecture",
        arguments: {
          project: "{PROJECT}",
          detect_cycles: true,
        },
      },
      null,
      2
    ),
  },
  {
    name: "compare_graphs",
    category: "Tracing & Architecture",
    summary: "Compare two graph states or branches to detect symbol regressions or additions",
    description:
      "Compare two graph generations, projects, or Git branches to detect added symbols, deleted symbols, breaking signature modifications, and altered dependency edges.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "base_project", type: "string", required: true, description: "Baseline project identifier or branch" },
      { name: "target_project", type: "string", required: true, description: "Target project identifier or branch to compare against" },
    ],
    cliExample: "cbm cli compare_graphs --base_project=\"repo-main\" --target_project=\"repo-feature\"",
    jsonExample: JSON.stringify(
      {
        name: "compare_graphs",
        arguments: {
          base_project: "repo-main",
          target_project: "repo-feature",
        },
      },
      null,
      2
    ),
  },
  {
    name: "get_graph_schema",
    category: "Governance & Schema",
    summary: "Inspect node labels, edge types, property schemas, and index metadata",
    description:
      "Inspect knowledge graph metadata including available node labels, edge types, property keys, total symbol counts, and SQLite storage properties.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "project", type: "string", required: true, description: "Target project identifier" },
      { name: "diagnostics", type: "string", required: false, defaultVal: "summary", enumVals: ["summary", "full"], description: "Level of schema and diagnostic details" },
    ],
    cliExample: "cbm cli get_graph_schema --project={PROJECT} --diagnostics=full",
    jsonExample: JSON.stringify(
      {
        name: "get_graph_schema",
        arguments: {
          project: "{PROJECT}",
          diagnostics: "full",
        },
      },
      null,
      2
    ),
  },
  {
    name: "search_code",
    category: "Code Discovery",
    summary: "Fast ripgrep-accelerated text and regex search across indexed files",
    description:
      "Perform fast lexical text and regular expression searches across indexed repository files with surrounding code lines and hit context.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "project", type: "string", required: true, description: "Target project identifier" },
      { name: "query", type: "string", required: true, description: "Text or regex search query string" },
      { name: "is_regex", type: "boolean", required: false, defaultVal: false, description: "Treat search query as regular expression" },
      { name: "file_pattern", type: "string", required: false, description: "Glob pattern to restrict search file scope" },
      { name: "limit", type: "integer", required: false, defaultVal: 50, description: "Maximum matching lines to return" },
    ],
    cliExample: "cbm cli search_code --project={PROJECT} --query=\"TODO: deprecate\"",
    jsonExample: JSON.stringify(
      {
        name: "search_code",
        arguments: {
          project: "{PROJECT}",
          query: "TODO: deprecate",
          limit: 20,
        },
      },
      null,
      2
    ),
  },
  {
    name: "list_projects",
    category: "Indexing & Projects",
    summary: "List all indexed projects with status, branches, symbol counts, and DB sizes",
    description:
      "List all indexed repositories currently resident in the local SQLite store with their branch, root path, node and edge counts, and database sizes.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "detail", type: "string", required: false, defaultVal: "identity", enumVals: ["identity", "stats"], description: "Include node/edge counts and database size (stats mode)" },
      { name: "limit", type: "integer", required: false, defaultVal: 50, description: "Number of projects to return per page" },
      { name: "offset", type: "integer", required: false, defaultVal: 0, description: "Pagination offset" },
    ],
    cliExample: "cbm cli list_projects --detail=stats",
    jsonExample: JSON.stringify(
      {
        name: "list_projects",
        arguments: {
          detail: "stats",
        },
      },
      null,
      2
    ),
  },
  {
    name: "index_repository",
    category: "Indexing & Projects",
    summary: "Index a repository from source using 162 Tree-Sitter grammars and Hybrid LSP",
    description:
      "Parse and index a repository into the in-memory SQLite knowledge graph using multi-pass parallel AST workers. Supports full, moderate, fast, and cross-repo modes.",
    readOnly: false,
    destructive: false,
    idempotent: true,
    parameters: [
      { name: "repo_path", type: "string", required: true, description: "Filesystem path to repository root folder" },
      { name: "mode", type: "string", required: false, defaultVal: "full", enumVals: ["full", "moderate", "fast", "cross-repo-intelligence"], description: "Indexing pipeline mode" },
      { name: "name", type: "string", required: false, description: "Optional project identifier override" },
      { name: "persistence", type: "boolean", required: false, defaultVal: false, description: "Persist graph database file to disk" },
      { name: "async", type: "boolean", required: false, defaultVal: false, description: "Start indexing in background daemon and return immediately" },
      { name: "status", type: "boolean", required: false, defaultVal: false, description: "Query status of running or previous indexing job" },
    ],
    cliExample: "cbm index . --mode=full",
    jsonExample: JSON.stringify(
      {
        name: "index_repository",
        arguments: {
          repo_path: ".",
          mode: "full",
        },
      },
      null,
      2
    ),
  },
  {
    name: "index_status",
    category: "Indexing & Projects",
    summary: "Check indexing progress, completed file count, and background worker state",
    description:
      "Inspect the live indexing state of a repository, displaying completed file percentage, AST error counts, and background worker thread status.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "repo_path", type: "string", required: false, description: "Filesystem path to repository root" },
      { name: "project", type: "string", required: false, description: "Project identifier to query" },
    ],
    cliExample: "cbm cli index_status --project={PROJECT}",
    jsonExample: JSON.stringify(
      {
        name: "index_status",
        arguments: {
          project: "{PROJECT}",
        },
      },
      null,
      2
    ),
  },
  {
    name: "check_index_coverage",
    category: "Governance & Schema",
    summary: "Validate candidate paths and report unindexed files or parse gaps",
    description:
      "Audit index coverage across files, verifying if files or ranges were skipped, excluded, or encountered AST parse failures during indexing.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "project", type: "string", required: true, description: "Target project identifier" },
      { name: "paths", type: "string[]", required: false, description: "Array of specific file paths to check for recorded coverage gaps" },
    ],
    cliExample: "cbm cli check_index_coverage --project={PROJECT}",
    jsonExample: JSON.stringify(
      {
        name: "check_index_coverage",
        arguments: {
          project: "{PROJECT}",
        },
      },
      null,
      2
    ),
  },
  {
    name: "detect_changes",
    category: "Indexing & Projects",
    summary: "Compare working copy against Git commit tree to identify dirty symbols",
    description:
      "Compare local working tree modifications against Git commit tree hashes to identify modified files, altered symbol signatures, and stale graph nodes.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "project", type: "string", required: true, description: "Target project identifier" },
      { name: "repo_path", type: "string", required: false, description: "Working copy filesystem root" },
    ],
    cliExample: "cbm cli detect_changes --project={PROJECT}",
    jsonExample: JSON.stringify(
      {
        name: "detect_changes",
        arguments: {
          project: "{PROJECT}",
        },
      },
      null,
      2
    ),
  },
  {
    name: "delete_project",
    category: "Indexing & Projects",
    summary: "Delete an indexed project database from the local store",
    description:
      "Remove an indexed project and its SQLite WAL database files permanently from the local cache and reclaim disk space.",
    readOnly: false,
    destructive: true,
    idempotent: true,
    parameters: [
      { name: "project", type: "string", required: true, description: "Project identifier to delete" },
    ],
    cliExample: "cbm cli delete_project --project={PROJECT}",
    jsonExample: JSON.stringify(
      {
        name: "delete_project",
        arguments: {
          project: "{PROJECT}",
        },
      },
      null,
      2
    ),
  },
  {
    name: "manage_adr",
    category: "Governance & Schema",
    summary: "Create, list, query, or supersede Architectural Decision Records (ADRs)",
    description:
      "Manage Architectural Decision Records (ADRs) tied directly to graph symbols and packages. Create proposals, record decisions, query history, or mark decisions superseded.",
    readOnly: false,
    destructive: false,
    idempotent: false,
    parameters: [
      { name: "project", type: "string", required: true, description: "Target project identifier" },
      { name: "action", type: "string", required: true, enumVals: ["list", "get", "create", "supersede"], description: "ADR operation to perform" },
      { name: "title", type: "string", required: false, description: "Title of the ADR" },
      { name: "status", type: "string", required: false, enumVals: ["Proposed", "Accepted", "Deprecated", "Superseded"], description: "Lifecycle state of the ADR" },
      { name: "context", type: "string", required: false, description: "Context and problem statement" },
      { name: "decision", type: "string", required: false, description: "Architectural decision description" },
      { name: "consequences", type: "string", required: false, description: "Consequences and trade-offs" },
    ],
    cliExample: "cbm cli manage_adr --project={PROJECT} --action=list",
    jsonExample: JSON.stringify(
      {
        name: "manage_adr",
        arguments: {
          project: "{PROJECT}",
          action: "list",
        },
      },
      null,
      2
    ),
  },
  {
    name: "ingest_traces",
    category: "Governance & Schema",
    summary: "Ingest runtime execution spans and telemetry to link with static AST nodes",
    description:
      "Ingest runtime OpenTelemetry traces or span logs to correlate dynamic execution paths and latency profiles with static knowledge graph functions.",
    readOnly: true,
    idempotent: true,
    parameters: [
      { name: "project", type: "string", required: true, description: "Target project identifier" },
      { name: "traces", type: "string", required: true, description: "OpenTelemetry JSON or formatted trace payload" },
    ],
    cliExample: "cbm cli ingest_traces --project={PROJECT} --traces=\"@trace.json\"",
    jsonExample: JSON.stringify(
      {
        name: "ingest_traces",
        arguments: {
          project: "{PROJECT}",
          traces: "...",
        },
      },
      null,
      2
    ),
  },
];

type CategoryFilter = "All" | "Code Discovery" | "Tracing & Architecture" | "Indexing & Projects" | "Governance & Schema";

interface ToolsTabProps {
  selectedProject: string | null;
  onSelectProject?: (project: string) => void;
}

export function ToolsTab({ selectedProject }: ToolsTabProps) {
  const [search, setSearch] = useState("");
  const [selectedCategory, setSelectedCategory] = useState<CategoryFilter>("All");
  const [readOnlyOnly, setReadOnlyOnly] = useState(false);
  const [expandedTool, setExpandedTool] = useState<string | null>("export_diagram");
  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  const categories: CategoryFilter[] = [
    "All",
    "Code Discovery",
    "Tracing & Architecture",
    "Indexing & Projects",
    "Governance & Schema",
  ];

  const filteredTools = useMemo(() => {
    return MCP_TOOLS.filter((tool) => {
      if (selectedCategory !== "All" && tool.category !== selectedCategory) {
        return false;
      }
      if (readOnlyOnly && !tool.readOnly) {
        return false;
      }
      if (search.trim()) {
        const q = search.toLowerCase();
        const matchesName = tool.name.toLowerCase().includes(q);
        const matchesSummary = tool.summary.toLowerCase().includes(q);
        const matchesDesc = tool.description.toLowerCase().includes(q);
        const matchesParam = tool.parameters.some((p) => p.name.toLowerCase().includes(q));
        if (!matchesName && !matchesSummary && !matchesDesc && !matchesParam) {
          return false;
        }
      }
      return true;
    });
  }, [search, selectedCategory, readOnlyOnly]);

  const copyToClipboard = (key: string, text: string) => {
    const formatted = text.replace(/\{PROJECT\}/g, selectedProject || "my-project");
    navigator.clipboard.writeText(formatted).then(() => {
      setCopiedKey(key);
      setTimeout(() => setCopiedKey(null), 2000);
    });
  };

  const activeProjectLabel = selectedProject || "my-project";

  return (
    <div className="h-full flex flex-col bg-background text-foreground overflow-hidden">
      {/* Top Banner & Header */}
      <div className="px-8 pt-8 pb-6 border-b border-border/30 bg-[#081014]/60 backdrop-blur-sm shrink-0">
        <div className="max-w-6xl mx-auto">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-6">
            <div>
              <div className="flex items-center gap-2 mb-1.5">
                <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[10px] font-semibold tracking-wider uppercase bg-primary/15 text-primary border border-primary/20">
                  <Sparkles className="w-3 h-3" />
                  Protocol Server · stdio / JSON-RPC 2.0
                </span>
                <span className="text-[11px] font-mono text-foreground/40">18 Tools Registered</span>
              </div>
              <h1 className="text-2xl font-bold tracking-tight text-foreground/90">MCP Tools Directory</h1>
              <p className="text-[13px] text-foreground/50 max-w-2xl mt-1">
                Authoritative reference for all 18 tools exposed by <code className="text-primary font-mono text-[12px]">codebase-memory-mcp</code> to AI assistants (Cursor, Claude Code, Antigravity) and CLI scripts.
              </p>
            </div>

            {/* Quick KPI Counters */}
            <div className="flex items-center gap-3">
              <div className="px-3.5 py-2 rounded-xl bg-white/[0.03] border border-border/30 flex flex-col items-center">
                <span className="text-[18px] font-bold text-primary font-mono tabular-nums">18</span>
                <span className="text-[9px] uppercase tracking-wider text-foreground/40">Total Tools</span>
              </div>
              <div className="px-3.5 py-2 rounded-xl bg-white/[0.03] border border-border/30 flex flex-col items-center">
                <span className="text-[18px] font-bold text-emerald-400 font-mono tabular-nums">14</span>
                <span className="text-[9px] uppercase tracking-wider text-foreground/40">Read-Only</span>
              </div>
              <div className="px-3.5 py-2 rounded-xl bg-white/[0.03] border border-border/30 flex flex-col items-center">
                <span className="text-[18px] font-bold text-amber-400 font-mono tabular-nums">4</span>
                <span className="text-[9px] uppercase tracking-wider text-foreground/40">Mutating</span>
              </div>
            </div>
          </div>

          {/* Search Bar & Filters */}
          <div className="flex flex-col sm:flex-row items-stretch sm:items-center justify-between gap-3">
            {/* Search Input */}
            <div className="relative flex-1 max-w-md">
              <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-foreground/30 pointer-events-none" />
              <input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search tools by name, parameter, or keyword..."
                className="w-full pl-9 pr-4 py-1.5 rounded-lg bg-white/[0.03] border border-border/40 text-[12px] text-foreground placeholder:text-foreground/30 focus:outline-none focus:border-primary/60 transition-colors"
              />
              {search && (
                <button
                  onClick={() => setSearch("")}
                  className="absolute right-2.5 top-1/2 -translate-y-1/2 text-foreground/30 hover:text-foreground text-[11px]"
                >
                  ✕
                </button>
              )}
            </div>

            {/* Category Filter Pills */}
            <div className="flex items-center gap-1.5 overflow-x-auto pb-1 sm:pb-0 scrollbar-none">
              {categories.map((cat) => {
                const count =
                  cat === "All"
                    ? MCP_TOOLS.length
                    : MCP_TOOLS.filter((t) => t.category === cat).length;
                const active = selectedCategory === cat;
                return (
                  <button
                    key={cat}
                    onClick={() => setSelectedCategory(cat)}
                    className={`px-2.5 py-1 rounded-md text-[11px] font-medium whitespace-nowrap transition-all ${
                      active
                        ? "bg-primary/20 text-primary border border-primary/30"
                        : "bg-white/[0.02] text-foreground/50 border border-border/20 hover:text-foreground hover:bg-white/[0.05]"
                    }`}
                  >
                    {cat}
                    <span className="ml-1.5 opacity-40 font-mono text-[10px]">({count})</span>
                  </button>
                );
              })}

              {/* Safe Only Toggle */}
              <button
                onClick={() => setReadOnlyOnly((prev) => !prev)}
                className={`ml-1 px-2.5 py-1 rounded-md text-[11px] font-medium flex items-center gap-1.5 transition-all ${
                  readOnlyOnly
                    ? "bg-emerald-500/20 text-emerald-400 border border-emerald-500/30"
                    : "bg-white/[0.02] text-foreground/40 border border-border/20 hover:text-foreground"
                }`}
              >
                <ShieldCheck className="w-3.5 h-3.5" />
                <span>Read-only only</span>
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content: Tool Catalog */}
      <ScrollArea className="flex-1 min-h-0">
        <div className="max-w-6xl mx-auto px-8 py-8 space-y-4">
          {filteredTools.length === 0 ? (
            <div className="text-center py-16 border border-border/20 rounded-2xl bg-white/[0.01]">
              <Wrench className="w-8 h-8 text-foreground/20 mx-auto mb-3" />
              <p className="text-[14px] font-medium text-foreground/60">No matching MCP tools found</p>
              <p className="text-[12px] text-foreground/30 mt-1">Try clearing your search query or filters</p>
            </div>
          ) : (
            filteredTools.map((tool) => {
              const isExpanded = expandedTool === tool.name;
              const requiredCount = tool.parameters.filter((p) => p.required).length;

              return (
                <div
                  key={tool.name}
                  className={`rounded-2xl border transition-all overflow-hidden ${
                    isExpanded
                      ? "border-primary/40 bg-[#0b1920]/70 shadow-lg shadow-black/20"
                      : "border-border/30 bg-white/[0.02] hover:bg-white/[0.035] hover:border-border/50"
                  }`}
                >
                  {/* Tool Header Card */}
                  <div
                    onClick={() => setExpandedTool(isExpanded ? null : tool.name)}
                    className="p-5 cursor-pointer select-none flex items-start justify-between gap-4"
                  >
                    <div className="flex-1 min-w-0">
                      <div className="flex flex-wrap items-center gap-2 mb-1.5">
                        <span className="font-mono text-[15px] font-bold text-foreground tracking-tight hover:text-primary transition-colors">
                          {tool.name}
                        </span>

                        {/* Category Badge */}
                        <span
                          className={`text-[10px] px-2 py-0.5 rounded-full font-medium ${
                            tool.category === "Code Discovery"
                              ? "bg-blue-500/15 text-blue-400 border border-blue-500/20"
                              : tool.category === "Tracing & Architecture"
                                ? "bg-purple-500/15 text-purple-400 border border-purple-500/20"
                                : tool.category === "Indexing & Projects"
                                  ? "bg-emerald-500/15 text-emerald-400 border border-emerald-500/20"
                                  : "bg-amber-500/15 text-amber-400 border border-amber-500/20"
                          }`}
                        >
                          {tool.category}
                        </span>

                        {/* Safety Badges */}
                        {tool.readOnly ? (
                          <span className="text-[10px] px-2 py-0.5 rounded-full font-medium bg-emerald-500/15 text-emerald-400 border border-emerald-500/20 flex items-center gap-1">
                            <ShieldCheck className="w-3 h-3" />
                            Read-Only
                          </span>
                        ) : tool.destructive ? (
                          <span className="text-[10px] px-2 py-0.5 rounded-full font-medium bg-rose-500/15 text-rose-400 border border-rose-500/20">
                            Destructive
                          </span>
                        ) : (
                          <span className="text-[10px] px-2 py-0.5 rounded-full font-medium bg-amber-500/15 text-amber-400 border border-amber-500/20">
                            State-Mutating
                          </span>
                        )}

                        {tool.idempotent && (
                          <span className="text-[10px] px-1.5 py-0.5 rounded font-mono text-foreground/30 bg-white/[0.03]">
                            idempotent
                          </span>
                        )}
                      </div>

                      <p className="text-[13px] text-foreground/70 leading-relaxed font-normal">
                        {tool.summary}
                      </p>

                      {/* Quick parameter preview */}
                      <div className="flex flex-wrap items-center gap-1.5 mt-2.5">
                        <span className="text-[10px] uppercase font-mono text-foreground/30 mr-1">
                          Params ({tool.parameters.length}):
                        </span>
                        {tool.parameters.map((p) => (
                          <span
                            key={p.name}
                            className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${
                              p.required
                                ? "bg-primary/10 text-primary border border-primary/20 font-semibold"
                                : "bg-white/[0.03] text-foreground/40"
                            }`}
                          >
                            {p.required ? `*${p.name}` : p.name}
                          </span>
                        ))}
                      </div>
                    </div>

                    <div className="flex items-center gap-2 shrink-0 pt-1">
                      <button
                        type="button"
                        className="text-foreground/30 hover:text-foreground/80 transition-colors p-1"
                      >
                        {isExpanded ? <ChevronDown className="w-5 h-5" /> : <ChevronRight className="w-5 h-5" />}
                      </button>
                    </div>
                  </div>

                  {/* Expanded Detail Panel */}
                  {isExpanded && (
                    <div className="border-t border-border/20 p-5 bg-black/20 space-y-6">
                      {/* Full Description */}
                      <div>
                        <h4 className="text-[11px] font-semibold text-foreground/40 uppercase tracking-wider mb-1.5">
                          Description
                        </h4>
                        <p className="text-[13px] text-foreground/80 leading-relaxed">
                          {tool.description}
                        </p>
                      </div>

                      {/* Parameters Table */}
                      <div>
                        <div className="flex items-center justify-between mb-2">
                          <h4 className="text-[11px] font-semibold text-foreground/40 uppercase tracking-wider">
                            Parameters ({tool.parameters.length} total, {requiredCount} required)
                          </h4>
                          {selectedProject && (
                            <span className="text-[10px] text-primary font-mono">
                              Active Project: {selectedProject}
                            </span>
                          )}
                        </div>

                        <div className="border border-border/20 rounded-xl overflow-hidden bg-black/30">
                          <table className="w-full text-left border-collapse">
                            <thead>
                              <tr className="border-b border-border/20 bg-white/[0.02] text-[11px] text-foreground/40 font-medium">
                                <th className="py-2 px-3">Name</th>
                                <th className="py-2 px-3">Type</th>
                                <th className="py-2 px-3">Default / Enums</th>
                                <th className="py-2 px-3">Description</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-border/10 font-mono text-[11px]">
                              {tool.parameters.map((param) => (
                                <tr key={param.name} className="hover:bg-white/[0.015]">
                                  <td className="py-2.5 px-3">
                                    <div className="flex items-center gap-1.5">
                                      <span className="text-foreground/90 font-semibold">{param.name}</span>
                                      {param.required && (
                                        <span className="text-[9px] px-1 py-0.2 rounded bg-rose-500/15 text-rose-400 font-sans font-medium">
                                          req
                                        </span>
                                      )}
                                    </div>
                                  </td>
                                  <td className="py-2.5 px-3 text-primary/80">{param.type}</td>
                                  <td className="py-2.5 px-3 text-foreground/40">
                                    {param.enumVals ? (
                                      <div className="flex flex-wrap gap-1">
                                        {param.enumVals.map((v) => (
                                          <span
                                            key={v}
                                            className={`px-1 py-0.5 rounded text-[9px] ${
                                              v === param.defaultVal
                                                ? "bg-primary/20 text-primary border border-primary/30"
                                                : "bg-white/[0.04] text-foreground/60"
                                            }`}
                                          >
                                            {v}
                                          </span>
                                        ))}
                                      </div>
                                    ) : param.defaultVal !== undefined ? (
                                      <span className="text-foreground/50">{String(param.defaultVal)}</span>
                                    ) : (
                                      <span className="text-foreground/20">—</span>
                                    )}
                                  </td>
                                  <td className="py-2.5 px-3 font-sans text-foreground/70 text-[12px]">
                                    {param.description}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>

                      {/* Code Examples: JSON-RPC & CLI */}
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        {/* MCP JSON-RPC Payload */}
                        <div className="rounded-xl border border-border/20 bg-black/40 p-3.5 flex flex-col justify-between">
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <span className="text-[11px] font-semibold text-foreground/50 flex items-center gap-1.5 uppercase tracking-wider font-mono">
                                <Code2 className="w-3.5 h-3.5 text-primary" />
                                MCP Tool Call (JSON-RPC)
                              </span>
                              <button
                                onClick={() => copyToClipboard(`json-${tool.name}`, tool.jsonExample)}
                                className="flex items-center gap-1 text-[10px] text-primary/80 hover:text-primary transition-colors px-2 py-0.5 rounded bg-primary/10 border border-primary/20"
                              >
                                {copiedKey === `json-${tool.name}` ? (
                                  <>
                                    <Check className="w-3 h-3 text-emerald-400" />
                                    <span>Copied!</span>
                                  </>
                                ) : (
                                  <>
                                    <Copy className="w-3 h-3" />
                                    <span>Copy JSON</span>
                                  </>
                                )}
                              </button>
                            </div>
                            <pre className="font-mono text-[11px] text-foreground/80 leading-relaxed overflow-x-auto p-2.5 rounded-lg bg-black/50 border border-white/[0.03]">
                              {tool.jsonExample.replace(/\{PROJECT\}/g, activeProjectLabel)}
                            </pre>
                          </div>
                        </div>

                        {/* CLI Equivalent */}
                        <div className="rounded-xl border border-border/20 bg-black/40 p-3.5 flex flex-col justify-between">
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <span className="text-[11px] font-semibold text-foreground/50 flex items-center gap-1.5 uppercase tracking-wider font-mono">
                                <Terminal className="w-3.5 h-3.5 text-amber-400" />
                                CLI Equivalent (cbm)
                              </span>
                              <button
                                onClick={() => copyToClipboard(`cli-${tool.name}`, tool.cliExample)}
                                className="flex items-center gap-1 text-[10px] text-amber-400/80 hover:text-amber-400 transition-colors px-2 py-0.5 rounded bg-amber-400/10 border border-amber-400/20"
                              >
                                {copiedKey === `cli-${tool.name}` ? (
                                  <>
                                    <Check className="w-3 h-3 text-emerald-400" />
                                    <span>Copied!</span>
                                  </>
                                ) : (
                                  <>
                                    <Copy className="w-3 h-3" />
                                    <span>Copy CLI</span>
                                  </>
                                )}
                              </button>
                            </div>
                            <pre className="font-mono text-[11px] text-foreground/80 leading-relaxed overflow-x-auto p-2.5 rounded-lg bg-black/50 border border-white/[0.03]">
                              {tool.cliExample.replace(/\{PROJECT\}/g, activeProjectLabel)}
                            </pre>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              );
            })
          )}
        </div>
      </ScrollArea>
    </div>
  );
}
