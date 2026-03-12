package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"runtime/pprof"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/cbm"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/DeusData/codebase-memory-mcp/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var version = "dev"

func main() {
	tools.SetVersion(version)

	// Parse global flags before subcommand dispatch.
	var pprofOnExit string
	var remaining []string
	for _, a := range os.Args[1:] {
		switch {
		case strings.HasPrefix(a, "--pprof-on-exit="):
			pprofOnExit = strings.TrimPrefix(a, "--pprof-on-exit=")
		default:
			remaining = append(remaining, a)
		}
	}

	if len(remaining) > 0 {
		switch remaining[0] {
		case "--version":
			fmt.Println("codebase-memory-mcp", version)
			os.Exit(0)
		case "--help", "-h", "help":
			printTopLevelHelp()
			os.Exit(0)
		case "install":
			os.Exit(runInstall(remaining[1:]))
		case "uninstall":
			os.Exit(runUninstall(remaining[1:]))
		case "update":
			os.Exit(runUpdate(remaining[1:]))
		case "config":
			os.Exit(runConfig(remaining[1:]))
		case "cli":
			if len(remaining) >= 2 {
				os.Exit(runCLI(remaining[1:]))
			}
		}
	}

	router, err := store.NewRouter()
	if err != nil {
		log.Fatalf("store router err=%v", err)
	}

	cfg, err := store.OpenConfig()
	if err != nil {
		log.Fatalf("config err=%v", err)
	}

	// Apply GOMEMLIMIT from config (e.g. "2G", "512M").
	if memLimit := cfg.Get(store.ConfigMemLimit, ""); memLimit != "" {
		if limit, parseErr := parseByteSize(memLimit); parseErr != nil {
			log.Printf("config: invalid mem_limit %q: %v", memLimit, parseErr)
		} else {
			debug.SetMemoryLimit(limit)
		}
	}

	srv := tools.NewServer(router, tools.WithConfig(cfg))

	ctx, cancel := context.WithCancel(context.Background())
	router.StartEvictor(ctx)
	srv.StartWatcher(ctx)

	runErr := srv.MCPServer().Run(ctx, &mcp.StdioTransport{})
	cancel()

	// Shutdown frees the calling thread's TLS tree-sitter parser pool.
	// Main goroutine doesn't run extraction, so this frees 0 bytes at runtime,
	// but signals clean shutdown for Valgrind/ASan runs.
	cbm.Shutdown()

	if pprofOnExit != "" {
		writeHeapProfile(pprofOnExit)
	}

	cfg.Close()
	router.CloseAll()
	if runErr != nil {
		log.Fatalf("server err=%v", runErr)
	}
}

// writeHeapProfile writes a heap profile to the given path.
func writeHeapProfile(path string) {
	f, err := os.Create(path) //nolint:gosec // path from trusted internal pprof flag
	if err != nil {
		log.Printf("pprof: create %s err=%v", path, err)
		return
	}
	defer f.Close()
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Printf("pprof: write err=%v", err)
		return
	}
	log.Printf("pprof: heap profile written to %s", path)
}

// parseByteSize parses human-readable byte sizes like "1G", "512M", "2048K".
func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	multiplier := int64(1)
	switch {
	case strings.HasSuffix(s, "G") || strings.HasSuffix(s, "g"):
		multiplier = 1 << 30
		s = s[:len(s)-1]
	case strings.HasSuffix(s, "M") || strings.HasSuffix(s, "m"):
		multiplier = 1 << 20
		s = s[:len(s)-1]
	case strings.HasSuffix(s, "K") || strings.HasSuffix(s, "k"):
		multiplier = 1 << 10
		s = s[:len(s)-1]
	}
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, fmt.Errorf("parse %q: %w", s, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("size must be positive")
	}
	return n * multiplier, nil
}

func printTopLevelHelp() {
	fmt.Fprintf(os.Stderr, "codebase-memory-mcp %s — Code knowledge graph MCP server\n\n", version)
	printHelpUsage()
	printHelpTools()
	printHelpIgnoring()
	printHelpCLI()
}

func printHelpUsage() {
	fmt.Fprint(os.Stderr, `Usage:
  codebase-memory-mcp                         Start the MCP server (stdio transport)
  codebase-memory-mcp cli <tool> [json_args]  Invoke a tool directly from the shell
  codebase-memory-mcp install [--dry-run]     Auto-detect and configure MCP clients
  codebase-memory-mcp uninstall               Remove MCP registrations and skills
  codebase-memory-mcp update                  Self-update to the latest release
  codebase-memory-mcp config <command>        Manage server configuration
  codebase-memory-mcp --version               Print version and exit
  codebase-memory-mcp --help                  Show this help

Supported MCP clients:
  Claude Code, Codex CLI, Cursor, Windsurf, Gemini CLI, VS Code, Zed

Language support (64 languages):
  Tree-sitter AST parsing for: Python, Go, JavaScript, TypeScript, TSX, Rust,
  Java, C++, C#, C, PHP, Lua, Scala, Kotlin, Ruby, Bash, Zig, Elixir, Haskell,
  OCaml, Objective-C, Swift, Dart, Perl, Groovy, Erlang, R, Clojure, F#, Julia,
  Vim Script, Nix, Common Lisp, Elm, Fortran, CUDA, COBOL, Verilog, Emacs Lisp,
  MATLAB, Lean 4, FORM, Magma, Wolfram, HTML, CSS, SCSS, YAML, TOML, HCL, SQL,
  Dockerfile, JSON, XML, Markdown, Makefile, CMake, Protobuf, GraphQL, Vue,
  Svelte, Meson, GLSL, INI

  Go uses a tree-sitter + LSP hybrid methodology for enhanced cross-file type
  resolution and call graph accuracy. This hybrid approach is coming soon for
  additional languages.

`)
}

func printHelpTools() {
	fmt.Fprint(os.Stderr, `MCP tools (14):

  index_repository — Index a repo into the knowledge graph
    {"repo_path": "/path/to/repo", "mode": "full|fast"}
    repo_path: absolute path (optional — defaults to session project root)
    mode: "full" (default) or "fast" (skips generated code, test fixtures, large files)

  index_status — Check indexing status of a project
    {"project": "my-project"}
    Returns: status (not_indexed/indexing/ready), node/edge counts, timestamps

  list_projects — List all indexed projects
    {}    (no parameters)
    Returns: project names, node/edge counts, indexed_at timestamps

  delete_project — Remove a project and all its graph data (irreversible)
    {"project_name": "my-project"}

  search_graph — Structural search with filters
    {"label": "Function", "name_pattern": ".*Handler.*", "project": "my-api"}
    label: Function, Class, Module, Method, Interface, Enum, Type, File, Route
    name_pattern: regex on short name (case-insensitive by default)
    qn_pattern: regex on qualified name (e.g. ".*services\\.order\\..*")
    file_pattern: glob for file path (e.g. "*.py", "**/services/**")
    relationship: CALLS, HTTP_CALLS, ASYNC_CALLS, IMPORTS, IMPLEMENTS, etc.
    direction: inbound, outbound, any (for degree filtering)
    min_degree/max_degree: edge count filters (e.g. max_degree=0 for dead code)
    exclude_entry_points: true to skip route handlers/main() in dead code search
    limit (default 10), offset: pagination (response includes has_more, total)
    case_sensitive: false (default) — set true for exact case matching

  search_code — Grep-like text search within indexed files
    {"pattern": "TODO|FIXME", "regex": true, "file_pattern": "*.go"}
    pattern: search text (case-insensitive by default)
    regex: true to enable regex (default false)
    file_pattern: glob to filter files
    max_results (default 10), offset: pagination

  trace_call_path — BFS call chain traversal
    {"function_name": "ProcessOrder", "direction": "both", "depth": 3}
    function_name: exact name (use search_graph first to find it)
    direction: outbound (what it calls), inbound (what calls it), both
    depth: 1-5 (default 3)
    risk_labels: true to add CRITICAL/HIGH/MEDIUM/LOW classification
    min_confidence: 0.0-1.0 threshold for CALLS edges

  detect_changes — Git diff → affected symbols + blast radius
    {"scope": "unstaged", "depth": 3}
    scope: unstaged (default), staged, all, branch
    base_branch: for scope=branch (default "main")
    depth: 1-5 (default 3) for blast radius tracing

  query_graph — Execute Cypher-like graph queries (read-only)
    {"query": "MATCH (f:Function)-[:CALLS]->(g) WHERE f.name = 'main' RETURN g.name LIMIT 20"}
    Supports: MATCH, WHERE (=, <>, >, <, =~, CONTAINS, STARTS WITH), RETURN,
              ORDER BY, LIMIT, COUNT, DISTINCT, variable-length paths (*1..3)
    max_rows: default 200, max 10000

  get_graph_schema — Node/edge counts and relationship patterns
    {"project": "my-project"}    (project optional)
    Returns: node label counts, edge type counts, sample names

  get_code_snippet — Read source code by qualified name
    {"qualified_name": "myproject.cmd.server.main.HandleRequest"}
    Accepts: full QN, partial suffix ("main.HandleRequest"), or short name
    auto_resolve: true to auto-pick from <=2 ambiguous candidates
    include_neighbors: true to get caller/callee name lists

  get_architecture — Codebase overview
    {"aspects": ["all"], "project": "my-project"}
    aspects: languages, packages, entry_points, routes, hotspots, boundaries,
             services, layers, clusters, file_tree, adr (or "all" for everything)

  manage_adr — CRUD for Architecture Decision Records
    {"mode": "store", "content": "## PURPOSE\n..."}
    mode: get, store, update, delete
    content: full ADR text (for store)
    sections: {"PATTERNS": "..."} (for update — unmentioned sections preserved)
    Fixed sections: PURPOSE, STACK, ARCHITECTURE, PATTERNS, TRADEOFFS, PHILOSOPHY

  ingest_traces — Ingest OpenTelemetry JSON traces
    {"traces": [...]}    (OTLP format spans)
    Validates HTTP_CALLS edges with runtime data, boosts confidence

`)
}

func printHelpIgnoring() {
	fmt.Fprint(os.Stderr, `File ignoring (layered):
  1. Hardcoded patterns   Always-on: .git, node_modules, dist, __pycache__, etc.
  2. .gitignore           Full gitignore hierarchy (nested files + .git/info/exclude)
  3. .cbmignore           Repo-root file with gitignore-style patterns, stacks on top
  4. .cgrignore           Legacy glob patterns (backward-compatible)

  Symlinked files and directories are always skipped.

`)
}

func printHelpCLI() {
	fmt.Fprint(os.Stderr, `CLI invocation:
  codebase-memory-mcp cli <tool_name> '<json_args>'
  codebase-memory-mcp cli --raw <tool_name> '<json_args>'   (full JSON output)

  Tools are called with a single JSON object as the argument. All parameters
  are passed as JSON keys. String values must be quoted. Arrays use JSON syntax.

CLI examples:
  codebase-memory-mcp cli index_repository '{"repo_path": "/path/to/repo"}'
  codebase-memory-mcp cli search_graph '{"name_pattern": ".*Handler.*", "label": "Function"}'
  codebase-memory-mcp cli trace_call_path '{"function_name": "main", "direction": "outbound"}'
  codebase-memory-mcp cli search_code '{"pattern": "TODO", "file_pattern": "*.go"}'
  codebase-memory-mcp cli get_architecture '{"aspects": ["languages", "packages"]}'
  codebase-memory-mcp cli --raw list_projects | jq .
  codebase-memory-mcp cli get_code_snippet '{"qualified_name": "HandleRequest"}'

Configuration:
  codebase-memory-mcp config list              Show all settings
  codebase-memory-mcp config set <key> <val>   Set a config value
  codebase-memory-mcp config get <key>         Get a config value

Data storage:
  ~/.cache/codebase-memory-mcp/   SQLite databases (WAL mode, persists across restarts)

For more information: https://github.com/DeusData/codebase-memory-mcp
`)
}

func runCLI(args []string) int {
	// Parse flags
	raw := false
	var positional []string
	for _, a := range args {
		switch a {
		case "--raw":
			raw = true
		default:
			positional = append(positional, a)
		}
	}

	router, err := store.NewRouter()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	defer router.CloseAll()

	if len(positional) == 0 || positional[0] == "--help" || positional[0] == "-h" {
		srv := tools.NewServer(router)
		fmt.Fprintf(os.Stderr, "Usage: codebase-memory-mcp cli [--raw] <tool_name> [json_args]\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n  --raw    Print full JSON output (default: human-friendly summary)\n\n")
		fmt.Fprintf(os.Stderr, "Available tools:\n  %s\n", strings.Join(srv.ToolNames(), "\n  "))
		return 0
	}

	toolName := positional[0]

	srv := tools.NewServer(router)

	// In CLI mode, try to set session root from cwd
	if cwd, cwdErr := os.Getwd(); cwdErr == nil {
		srv.SetSessionRoot(cwd)
	}

	var argsJSON json.RawMessage
	if len(positional) > 1 {
		argsJSON = json.RawMessage(positional[1])
	}

	result, err := srv.CallTool(context.Background(), toolName, argsJSON)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	if result.IsError {
		for _, c := range result.Content {
			if tc, ok := c.(*mcp.TextContent); ok {
				fmt.Fprintf(os.Stderr, "error: %s\n", tc.Text)
			}
		}
		return 1
	}

	// Extract the text content
	var text string
	for _, c := range result.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			text = tc.Text
			break
		}
	}

	if raw {
		printRawJSON(text)
		return 0
	}

	// Summary mode (default): print a human-friendly summary
	dbPath := filepath.Join(router.Dir(), srv.SessionProject()+".db")
	printSummary(toolName, text, dbPath)
	return 0
}

// printRawJSON pretty-prints JSON text to stdout.
func printRawJSON(text string) {
	var buf json.RawMessage
	if json.Unmarshal([]byte(text), &buf) == nil {
		if pretty, err := json.MarshalIndent(buf, "", "  "); err == nil {
			fmt.Println(string(pretty))
			return
		}
	}
	fmt.Println(text)
}

// printSummary prints a human-friendly summary of the tool result.
func printSummary(toolName, text, dbPath string) {
	var data map[string]any
	if err := json.Unmarshal([]byte(text), &data); err != nil {
		// Not a JSON object — might be an array (e.g. list_projects)
		var arr []any
		if err2 := json.Unmarshal([]byte(text), &arr); err2 == nil {
			printArraySummary(toolName, arr, dbPath)
			return
		}
		// Plain text — print as-is
		fmt.Println(text)
		return
	}

	switch toolName {
	case "index_repository":
		printIndexSummary(data, dbPath)
	case "search_graph":
		printSearchGraphSummary(data)
	case "search_code":
		printSearchCodeSummary(data)
	case "trace_call_path":
		printTraceSummary(data)
	case "query_graph":
		printQuerySummary(data)
	case "get_graph_schema":
		printSchemaSummary(data)
	case "get_code_snippet":
		printSnippetSummary(data)
	case "delete_project":
		printDeleteSummary(data)
	case "read_file":
		printReadFileSummary(data)
	case "list_directory":
		printListDirSummary(data)
	case "ingest_traces":
		printIngestSummary(data, dbPath)
	case "index_status":
		printIndexStatusSummary(data)
	case "detect_changes":
		printDetectChangesSummary(data)
	default:
		// Fallback: pretty-print the JSON
		printRawJSON(text)
	}
}

func printArraySummary(toolName string, arr []any, dbPath string) {
	switch toolName {
	case "list_projects":
		if len(arr) == 0 {
			fmt.Println("No projects indexed.")
			fmt.Printf("  db_dir: %s\n", filepath.Dir(dbPath))
			return
		}
		fmt.Printf("%d project(s) indexed:\n", len(arr))
		for _, item := range arr {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			name, _ := m["name"].(string)
			nodes := jsonInt(m["nodes"])
			edges := jsonInt(m["edges"])
			indexedAt, _ := m["indexed_at"].(string)
			rootPath, _ := m["root_path"].(string)
			isSession, _ := m["is_session_project"].(bool)
			sessionMarker := ""
			if isSession {
				sessionMarker = " *"
			}
			fmt.Printf("  %-30s %d nodes, %d edges  (indexed %s)%s\n", name, nodes, edges, indexedAt, sessionMarker)
			if rootPath != "" {
				fmt.Printf("  %-30s %s\n", "", rootPath)
			}
			if dbp, ok := m["db_path"].(string); ok {
				fmt.Printf("  %-30s %s\n", "", dbp)
			}
		}
	default:
		fmt.Printf("%d result(s)\n", len(arr))
		printRawJSON(mustJSON(arr))
	}
}

func printIndexSummary(data map[string]any, dbPath string) {
	project, _ := data["project"].(string)
	nodes := jsonInt(data["nodes"])
	edges := jsonInt(data["edges"])
	indexedAt, _ := data["indexed_at"].(string)
	fmt.Printf("Indexed %q: %d nodes, %d edges\n", project, nodes, edges)
	fmt.Printf("  indexed_at: %s\n", indexedAt)
	fmt.Printf("  db: %s\n", dbPath)
}

func printSearchGraphSummary(data map[string]any) {
	total := jsonInt(data["total"])
	hasMore, _ := data["has_more"].(bool)
	results, _ := data["results"].([]any)
	shown := len(results)

	fmt.Printf("%d result(s) found", total)
	if hasMore {
		fmt.Printf(" (showing %d, has_more=true)", shown)
	}
	fmt.Println()

	for _, r := range results {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		label, _ := m["label"].(string)
		filePath, _ := m["file_path"].(string)
		startLine := jsonInt(m["start_line"])
		fmt.Printf("  [%s] %s", label, name)
		if filePath != "" {
			fmt.Printf("  %s:%d", filePath, startLine)
		}
		fmt.Println()
	}
}

func printSearchCodeSummary(data map[string]any) {
	total := jsonInt(data["total"])
	hasMore, _ := data["has_more"].(bool)
	matches, _ := data["matches"].([]any)
	shown := len(matches)

	fmt.Printf("%d match(es) found", total)
	if hasMore {
		fmt.Printf(" (showing %d, has_more=true)", shown)
	}
	fmt.Println()

	for _, m := range matches {
		if entry, ok := m.(map[string]any); ok {
			file, _ := entry["file"].(string)
			line := jsonInt(entry["line"])
			content, _ := entry["content"].(string)
			fmt.Printf("  %s:%d  %s\n", file, line, content)
		}
	}
}

func printTraceSummary(data map[string]any) {
	root, _ := data["root"].(map[string]any)
	rootName, _ := root["name"].(string)
	totalResults := jsonInt(data["total_results"])
	edges, _ := data["edges"].([]any)
	hops, _ := data["hops"].([]any)

	fmt.Printf("Trace from %q: %d node(s), %d edge(s), %d hop(s)\n", rootName, totalResults, len(edges), len(hops))

	for _, h := range hops {
		if hop, ok := h.(map[string]any); ok {
			hopNum := jsonInt(hop["hop"])
			nodes, _ := hop["nodes"].([]any)
			fmt.Printf("  hop %d: %d node(s)\n", hopNum, len(nodes))
			for _, n := range nodes {
				if nm, ok := n.(map[string]any); ok {
					name, _ := nm["name"].(string)
					label, _ := nm["label"].(string)
					fmt.Printf("    [%s] %s\n", label, name)
				}
			}
		}
	}
}

func printQuerySummary(data map[string]any) {
	total := jsonInt(data["total"])
	columns, _ := data["columns"].([]any)
	rows, _ := data["rows"].([]any)

	colNames := make([]string, len(columns))
	for i, c := range columns {
		colNames[i], _ = c.(string)
	}

	fmt.Printf("%d row(s) returned", total)
	if len(colNames) > 0 {
		fmt.Printf("  [%s]", strings.Join(colNames, ", "))
	}
	fmt.Println()

	for _, row := range rows {
		switch r := row.(type) {
		case map[string]any:
			// Rows are maps keyed by column name
			parts := make([]string, len(colNames))
			for i, col := range colNames {
				parts[i] = fmt.Sprintf("%v", r[col])
			}
			fmt.Printf("  %s\n", strings.Join(parts, " | "))
		case []any:
			parts := make([]string, len(r))
			for i, v := range r {
				parts[i] = fmt.Sprintf("%v", v)
			}
			fmt.Printf("  %s\n", strings.Join(parts, " | "))
		}
	}
}

func printSchemaSummary(data map[string]any) {
	projects, _ := data["projects"].([]any)
	if len(projects) == 0 {
		fmt.Println("No projects indexed.")
		return
	}

	for _, p := range projects {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		projName, _ := pm["project"].(string)
		schema, _ := pm["schema"].(map[string]any)
		if schema == nil {
			continue
		}

		fmt.Printf("Project: %s\n", projName)
		if labels, ok := schema["node_labels"].([]any); ok {
			fmt.Printf("  Node labels (%d):\n", len(labels))
			for _, l := range labels {
				if lm, ok := l.(map[string]any); ok {
					label, _ := lm["label"].(string)
					count := jsonInt(lm["count"])
					fmt.Printf("    %-15s %d\n", label, count)
				}
			}
		}
		if rels, ok := schema["relationship_types"].([]any); ok {
			fmt.Printf("  Edge types (%d):\n", len(rels))
			for _, r := range rels {
				if rm, ok := r.(map[string]any); ok {
					relType, _ := rm["type"].(string)
					count := jsonInt(rm["count"])
					fmt.Printf("    %-25s %d\n", relType, count)
				}
			}
		}
	}
}

func printSnippetSummary(data map[string]any) {
	name, _ := data["name"].(string)
	label, _ := data["label"].(string)
	filePath, _ := data["file_path"].(string)
	startLine := jsonInt(data["start_line"])
	endLine := jsonInt(data["end_line"])
	source, _ := data["source"].(string)

	fmt.Printf("[%s] %s  (%s:%d-%d)\n\n", label, name, filePath, startLine, endLine)
	fmt.Println(source)
}

func printDeleteSummary(data map[string]any) {
	deleted, _ := data["deleted"].(string)
	fmt.Printf("Deleted project %q\n", deleted)
}

func printReadFileSummary(data map[string]any) {
	path, _ := data["path"].(string)
	totalLines := jsonInt(data["total_lines"])
	content, _ := data["content"].(string)

	fmt.Printf("%s (%d lines)\n\n", path, totalLines)
	fmt.Println(content)
}

func printListDirSummary(data map[string]any) {
	dir, _ := data["directory"].(string)
	count := jsonInt(data["count"])
	entries, _ := data["entries"].([]any)

	fmt.Printf("%s (%d entries)\n", dir, count)
	for _, e := range entries {
		if em, ok := e.(map[string]any); ok {
			name, _ := em["name"].(string)
			isDir, _ := em["is_dir"].(bool)
			if isDir {
				fmt.Printf("  %s/\n", name)
			} else {
				size := jsonInt(em["size"])
				fmt.Printf("  %-40s %d bytes\n", name, size)
			}
		}
	}
}

func printIngestSummary(data map[string]any, dbPath string) {
	matched := jsonInt(data["matched"])
	boosted := jsonInt(data["boosted"])
	total := jsonInt(data["total_spans"])
	fmt.Printf("Ingested %d span(s): %d matched, %d boosted\n", total, matched, boosted)
	fmt.Printf("  db: %s\n", dbPath)
}

func printIndexStatusSummary(data map[string]any) {
	project, _ := data["project"].(string)
	status, _ := data["status"].(string)

	switch status {
	case "no_session":
		msg, _ := data["message"].(string)
		fmt.Println(msg)
	case "not_indexed":
		fmt.Printf("Project %q: not indexed\n", project)
		if dbPath, ok := data["db_path"].(string); ok {
			fmt.Printf("  expected db: %s\n", dbPath)
		}
	case "partial":
		fmt.Printf("Project %q: partially indexed (metadata missing)\n", project)
	case "indexing":
		fmt.Printf("Project %q: indexing in progress\n", project)
		if elapsed, ok := data["index_elapsed_seconds"]; ok {
			fmt.Printf("  elapsed: %ds\n", jsonInt(elapsed))
		}
		if indexType, ok := data["index_type"].(string); ok {
			fmt.Printf("  type: %s\n", indexType)
		}
	case "ready":
		nodes := jsonInt(data["nodes"])
		edges := jsonInt(data["edges"])
		indexedAt, _ := data["indexed_at"].(string)
		indexType, _ := data["index_type"].(string)
		isSession, _ := data["is_session_project"].(bool)
		fmt.Printf("Project %q: ready (%d nodes, %d edges)\n", project, nodes, edges)
		fmt.Printf("  indexed_at: %s\n", indexedAt)
		fmt.Printf("  index_type: %s\n", indexType)
		if isSession {
			fmt.Printf("  session_project: true\n")
		}
		if dbPath, ok := data["db_path"].(string); ok {
			fmt.Printf("  db: %s\n", dbPath)
		}
	default:
		printRawJSON(mustJSON(data))
	}
}

func printDetectChangesSummary(data map[string]any) {
	summary, _ := data["summary"].(map[string]any)
	changedFiles := jsonInt(summary["changed_files"])
	changedSymbols := jsonInt(summary["changed_symbols"])
	total := jsonInt(summary["total"])
	critical := jsonInt(summary["critical"])
	high := jsonInt(summary["high"])
	medium := jsonInt(summary["medium"])
	low := jsonInt(summary["low"])

	fmt.Printf("Changes: %d file(s), %d symbol(s) modified\n", changedFiles, changedSymbols)
	fmt.Printf("Impact: %d affected symbol(s)\n", total)
	if total > 0 {
		fmt.Printf("  CRITICAL: %d  HIGH: %d  MEDIUM: %d  LOW: %d\n", critical, high, medium, low)
	}

	impacted, _ := data["impacted_symbols"].([]any)
	for _, is := range impacted {
		m, ok := is.(map[string]any)
		if !ok {
			continue
		}
		risk, _ := m["risk"].(string)
		name, _ := m["name"].(string)
		label, _ := m["label"].(string)
		changedBy, _ := m["changed_by"].(string)
		fmt.Printf("  [%s] [%s] %s  (via %s)\n", risk, label, name, changedBy)
	}
}

// jsonInt extracts an integer from a JSON-decoded value (float64 or int).
func jsonInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	default:
		return 0
	}
}

func mustJSON(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}
