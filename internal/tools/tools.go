package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeusData/codebase-memory-mcp/internal/pipeline"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/DeusData/codebase-memory-mcp/internal/watcher"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Version is the current release version, referenced by MCP handshake and update checker.
const Version = "0.2.0"

// releaseURL is the GitHub API endpoint for latest release. Package-level var for test injection.
var releaseURL = "https://api.github.com/repos/DeusData/codebase-memory-mcp/releases/latest"

// Server wraps the MCP server with tool handlers.
type Server struct {
	mcp      *mcp.Server
	router   *store.StoreRouter
	watcher  *watcher.Watcher
	indexMu  sync.Mutex
	handlers map[string]mcp.ToolHandler

	// Session-aware fields (set once via sync.Once, then immutable)
	sessionOnce    sync.Once
	sessionRoot    string // absolute path from client
	sessionProject string // filepath.Base(sessionRoot)
	indexStatus    atomic.Value
	indexStartedAt atomic.Value // time.Time — when current/last index started
	updateNotice   atomic.Value // string — set once by checkForUpdate, cleared after first injection
}

// NewServer creates a new MCP server with all tools registered.
func NewServer(r *store.StoreRouter) *Server {
	srv := &Server{
		router:   r,
		handlers: make(map[string]mcp.ToolHandler),
	}

	srv.mcp = mcp.NewServer(
		&mcp.Implementation{
			Name:    "codebase-memory-mcp",
			Version: Version,
		},
		&mcp.ServerOptions{
			InitializedHandler:      srv.onInitialized,
			RootsListChangedHandler: srv.onRootsChanged,
		},
	)

	srv.registerTools()
	srv.watcher = watcher.New(r, srv.syncProject)
	return srv
}

// StartWatcher launches the background file-change polling goroutine.
// It stops when ctx is cancelled.
func (s *Server) StartWatcher(ctx context.Context) {
	go s.watcher.Run(ctx)
}

// syncProject is called by the watcher when file changes are detected.
// Uses TryLock to skip if an index operation is already in progress.
func (s *Server) syncProject(projectName, rootPath string) error {
	if !s.indexMu.TryLock() {
		slog.Debug("watcher.skip", "path", rootPath, "reason", "index_in_progress")
		return nil
	}
	defer s.indexMu.Unlock()
	st, err := s.router.ForProject(projectName)
	if err != nil {
		return fmt.Errorf("store for %s: %w", projectName, err)
	}
	p := pipeline.New(st, rootPath)
	return p.Run()
}

// MCPServer returns the underlying MCP server.
func (s *Server) MCPServer() *mcp.Server {
	return s.mcp
}

// Router returns the underlying StoreRouter for direct access (e.g. CLI mode).
func (s *Server) Router() *store.StoreRouter {
	return s.router
}

// SessionProject returns the auto-detected session project name (may be empty).
func (s *Server) SessionProject() string {
	return s.sessionProject
}

// SetSessionRoot sets the session root path directly (for CLI mode).
func (s *Server) SetSessionRoot(rootPath string) {
	s.sessionOnce.Do(func() {
		s.sessionRoot = rootPath
		if rootPath != "" {
			s.sessionProject = filepath.Base(rootPath)
		}
	})
	go s.checkForUpdate()
}

// --- Session detection ---

// onInitialized is called when the client sends notifications/initialized.
func (s *Server) onInitialized(ctx context.Context, req *mcp.InitializedRequest) {
	s.sessionOnce.Do(func() {
		s.sessionRoot = s.detectSessionRoot(ctx, req.Session)
		if s.sessionRoot != "" {
			s.sessionProject = filepath.Base(s.sessionRoot)
			s.startAutoIndex()
		}
	})
	go s.checkForUpdate()
}

// onRootsChanged re-detects session root if not yet set.
func (s *Server) onRootsChanged(ctx context.Context, req *mcp.RootsListChangedRequest) {
	s.sessionOnce.Do(func() {
		s.sessionRoot = s.detectSessionRoot(ctx, req.Session)
		if s.sessionRoot != "" {
			s.sessionProject = filepath.Base(s.sessionRoot)
			s.startAutoIndex()
		}
	})
	go s.checkForUpdate()
}

// detectSessionRoot tries multiple fallback strategies to find the project root.
func (s *Server) detectSessionRoot(ctx context.Context, session *mcp.ServerSession) string {
	// 1. Try MCP roots protocol
	if session != nil {
		result, err := session.ListRoots(ctx, nil)
		if err == nil && len(result.Roots) > 0 {
			uri := result.Roots[0].URI
			if path, ok := parseFileURI(uri); ok {
				slog.Info("session.root.from_roots", "path", path)
				return path
			}
		}
	}

	// 2. Fall back to process working directory
	if cwd, err := os.Getwd(); err == nil && cwd != "/" && cwd != os.Getenv("HOME") {
		slog.Info("session.root.from_cwd", "path", cwd)
		return cwd
	}

	// 3. Fall back to single indexed project from DB
	projects, err := s.router.ListProjects()
	if err == nil && len(projects) == 1 && projects[0].RootPath != "" {
		slog.Info("session.root.from_db", "path", projects[0].RootPath)
		return projects[0].RootPath
	}

	slog.Info("session.root.none", "reason", "no_roots_no_cwd_no_single_project")
	return ""
}

// parseFileURI extracts a filesystem path from a file:// URI.
func parseFileURI(uri string) (string, bool) {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "file" {
		return "", false
	}
	return u.Path, true
}

// startAutoIndex triggers background indexing for the session project.
func (s *Server) startAutoIndex() {
	hasDB := s.router.HasProject(s.sessionProject)

	if !hasDB {
		s.indexStatus.Store("indexing")
	} else {
		s.indexStatus.Store("ready")
	}

	go func() {
		if !s.indexMu.TryLock() {
			slog.Debug("autoindex.skip", "reason", "index_in_progress")
			return
		}
		defer s.indexMu.Unlock()

		s.indexStartedAt.Store(time.Now())
		if !hasDB {
			s.indexStatus.Store("indexing")
		}

		st, err := s.router.ForProject(s.sessionProject)
		if err != nil {
			slog.Warn("autoindex.store.err", "err", err)
			return
		}
		p := pipeline.New(st, s.sessionRoot)
		if err := p.Run(); err != nil {
			slog.Warn("autoindex.err", "err", err)
			return
		}
		s.indexStatus.Store("ready")
		slog.Info("autoindex.done", "project", s.sessionProject)
	}()
}

// --- Store routing ---

// resolveStore returns the Store for the given project, or the session project if empty.
func (s *Server) resolveStore(project string) (*store.Store, error) {
	if project == "*" || project == "all" {
		return nil, fmt.Errorf("cross-project queries are not supported; use list_projects to find a specific project name, or omit the project parameter to use the current session project")
	}
	if project == "" {
		project = s.sessionProject
	}
	if project == "" {
		return nil, fmt.Errorf("no project specified and no session project detected; pass project parameter")
	}
	if !s.router.HasProject(project) {
		return nil, fmt.Errorf("project %q not found; use list_projects to see available projects", project)
	}
	return s.router.ForProject(project)
}

// resolveProjectName returns the effective project name for routing.
func (s *Server) resolveProjectName(project string) string {
	if project == "" {
		return s.sessionProject
	}
	return project
}

// addIndexStatus adds the index_status field to response data if indexing is in progress.
func (s *Server) addIndexStatus(data map[string]any) {
	status, _ := s.indexStatus.Load().(string)
	if status == "indexing" {
		data["index_status"] = "indexing"
	}
}

// addUpdateNotice injects update_available into the first tool response, then clears itself.
func (s *Server) addUpdateNotice(data map[string]any) {
	if notice, ok := s.updateNotice.Load().(string); ok && notice != "" {
		data["update_available"] = notice
		s.updateNotice.Store("")
	}
}

// checkForUpdate fetches the latest GitHub release and stores a notice if newer.
func (s *Server) checkForUpdate() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", releaseURL, nil)
	if err != nil {
		return
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return
	}

	var release struct {
		TagName string `json:"tag_name"`
	}
	if err := json.Unmarshal(body, &release); err != nil {
		return
	}

	latest := strings.TrimPrefix(release.TagName, "v")
	if latest == "" || latest == Version {
		return
	}
	if compareVersions(latest, Version) > 0 {
		s.updateNotice.Store(fmt.Sprintf(
			"v%s → v%s available. Update: go install github.com/DeusData/codebase-memory-mcp/cmd/codebase-memory-mcp@v%s",
			Version, latest, latest))
	}
}

// compareVersions compares two semver strings (e.g. "0.2.1" vs "0.2.0").
// Returns >0 if a > b, <0 if a < b, 0 if equal.
func compareVersions(a, b string) int {
	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")
	for i := 0; i < len(aParts) && i < len(bParts); i++ {
		ai, _ := strconv.Atoi(aParts[i])
		bi, _ := strconv.Atoi(bParts[i])
		if ai != bi {
			return ai - bi
		}
	}
	return len(aParts) - len(bParts)
}

// --- Tool registration ---

func (s *Server) addTool(tool *mcp.Tool, handler mcp.ToolHandler) {
	s.mcp.AddTool(tool, handler)
	s.handlers[tool.Name] = handler
}

// CallTool invokes a tool handler directly by name, bypassing MCP transport.
func (s *Server) CallTool(ctx context.Context, name string, argsJSON json.RawMessage) (*mcp.CallToolResult, error) {
	handler, ok := s.handlers[name]
	if !ok {
		return nil, fmt.Errorf("unknown tool: %s", name)
	}
	if len(argsJSON) == 0 {
		argsJSON = json.RawMessage(`{}`)
	}
	req := &mcp.CallToolRequest{
		Params: &mcp.CallToolParamsRaw{
			Name:      name,
			Arguments: argsJSON,
		},
	}
	return handler(ctx, req)
}

// ToolNames returns all registered tool names in sorted order.
func (s *Server) ToolNames() []string {
	names := make([]string, 0, len(s.handlers))
	for name := range s.handlers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *Server) registerTools() {
	s.registerGraphTools()
	s.registerFileTools()
	s.registerProjectTools()
	s.registerTraceTools()
}

// registerGraphTools registers tools for graph querying, searching, and tracing.
func (s *Server) registerGraphTools() {
	s.registerIndexAndTraceTool()
	s.registerSchemaAndSnippetTools()
	s.registerSearchTools()
	s.registerQueryTool()
}

func (s *Server) registerIndexAndTraceTool() {
	s.addTool(&mcp.Tool{
		Name:        "index_repository",
		Description: "Index a repository into the code graph. Parses source files, extracts functions/classes/modules, resolves call relationships (CALLS), read references (USAGE), interface implementations (IMPLEMENTS + OVERRIDE), HTTP/async cross-service links, and git history change coupling (FILE_CHANGES_WITH). Supports incremental reindex via content hashing. Auto-sync keeps the graph fresh after initial indexing. If repo_path is omitted, uses the auto-detected session project root.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"repo_path": {
					"type": "string",
					"description": "Absolute path to the repository to index. If omitted, uses the auto-detected session project root."
				}
			}
		}`),
	}, s.handleIndexRepository)

	s.addTool(&mcp.Tool{
		Name:        "trace_call_path",
		Description: "Trace the call path of a function (who calls it, what it calls). Requires exact function name — use search_graph first to find the exact name. Follow up with get_code_snippet to read the actual source code. Returns hop-by-hop callees/callers with edge types (CALLS, HTTP_CALLS, ASYNC_CALLS, USAGE, OVERRIDE). If the function is not found, returns suggestions of similar names — use the qualified_name from suggestions in a retry. Use depth=1 first, increase only if needed. Use direction='both' for full cross-service context — HTTP_CALLS edges from other services appear as inbound edges, so direction='outbound' alone misses cross-service callers. Best practice: search_graph(name_pattern='.*Order.*') → trace_call_path(function_name='processOrder') → get_code_snippet(qualified_name='...')",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"function_name": {
					"type": "string",
					"description": "Name of the function to trace (e.g. 'ProcessOrder')"
				},
				"depth": {
					"type": "integer",
					"description": "Maximum BFS depth (1-5, default 3)"
				},
				"direction": {
					"type": "string",
					"description": "Traversal direction: 'outbound' (what it calls), 'inbound' (what calls it), or 'both'",
					"enum": ["outbound", "inbound", "both"]
				},
				"project": {
					"type": "string",
					"description": "Project to trace in. Defaults to session project."
				}
			},
			"required": ["function_name"]
		}`),
	}, s.handleTraceCallPath)
}

func (s *Server) registerSchemaAndSnippetTools() {
	s.addTool(&mcp.Tool{
		Name:        "get_graph_schema",
		Description: "Return the schema of the indexed code graph: node label counts, edge type counts, relationship patterns (e.g. Function-CALLS->Function), and sample function/class names. Use to understand what's in the graph before querying.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"project": {
					"type": "string",
					"description": "Project to get schema for. Defaults to session project."
				}
			}
		}`),
	}, s.handleGetGraphSchema)

	s.addTool(&mcp.Tool{
		Name:        "get_code_snippet",
		Description: "Retrieve source code for a function/class by qualified name. Reads directly from disk using the stored file path and line range. Returns the source code with line numbers.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"qualified_name": {
					"type": "string",
					"description": "Fully qualified name of the node (e.g. 'myproject.cmd.server.main.HandleRequest')"
				},
				"project": {
					"type": "string",
					"description": "Project to search in. Defaults to session project."
				}
			},
			"required": ["qualified_name"]
		}`),
	}, s.handleGetCodeSnippet)
}

func (s *Server) registerSearchTools() {
	s.addTool(&mcp.Tool{
		Name:        "search_graph",
		Description: "Search the code knowledge graph for functions, classes, modules, routes, and other code elements. Returns nodes matching the criteria with their connectivity (in/out degree). Results are sorted by relevance by default (exact match first, prefix match second, then by connectivity). Community nodes are excluded by default. Pass exclude_labels: [] to include them. Best practice: Chain with trace_call_path and get_code_snippet for complete answers. Example workflow: search_graph(name_pattern='.*Order.*') → trace_call_path(function_name='processOrder') → get_code_snippet(qualified_name='...'). Returns 10 results per page (use offset to paginate, has_more indicates more pages). For dead code: use relationship='CALLS', direction='inbound', max_degree=0, exclude_entry_points=true. For fan-out: use relationship='CALLS', direction='outbound', min_degree=N. Route nodes: properties.handler contains the actual handler function name. Prefer this over query_graph for counting — no row cap. IMPORTANT: The 'relationship' filter counts how many edges of that type each node has (degree filtering) — it does NOT return the actual edges. To list cross-service HTTP_CALLS or ASYNC_CALLS edges with their properties, use query_graph with Cypher instead. Relationship types: CALLS, HTTP_CALLS, ASYNC_CALLS, IMPORTS, DEFINES, DEFINES_METHOD, HANDLES, CONTAINS_FILE, CONTAINS_FOLDER, CONTAINS_PACKAGE, IMPLEMENTS, OVERRIDE, USAGE, FILE_CHANGES_WITH.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"project": {
					"type": "string",
					"description": "Project to search in. Defaults to session project."
				},
				"label": {
					"type": "string",
					"description": "Node label filter: Function, Class, Module, Method, Interface, Enum, Type, File, Package, Folder, Route"
				},
				"name_pattern": {
					"type": "string",
					"description": "Regex pattern for node name (e.g. '.*Handler', 'Send.*')"
				},
				"file_pattern": {
					"type": "string",
					"description": "Glob pattern for file path within the project (e.g. '**/services/**', '*.py')"
				},
				"relationship": {
					"type": "string",
					"description": "Filter by relationship type: CALLS, HTTP_CALLS, ASYNC_CALLS, IMPORTS, DEFINES, DEFINES_METHOD, HANDLES, CONTAINS_FILE, CONTAINS_FOLDER, CONTAINS_PACKAGE, IMPLEMENTS"
				},
				"direction": {
					"type": "string",
					"description": "Edge direction for degree filters: 'inbound', 'outbound', or 'any'",
					"enum": ["inbound", "outbound", "any"]
				},
				"min_degree": {
					"type": "integer",
					"description": "Minimum edge count (e.g. 10 for high fan-out functions)"
				},
				"max_degree": {
					"type": "integer",
					"description": "Maximum edge count (e.g. 0 for dead code detection)"
				},
				"exclude_entry_points": {
					"type": "boolean",
					"description": "Exclude entry points (route handlers, main(), framework-registered functions) from results. Use with max_degree=0 for accurate dead code detection."
				},
				"limit": {
					"type": "integer",
					"description": "Max results per page (default: 10). Use small limits and paginate with offset — response includes has_more flag."
				},
				"offset": {
					"type": "integer",
					"description": "Skip N results for pagination (default: 0). Check has_more in response to know if more pages exist."
				},
				"include_connected": {
					"type": "boolean",
					"description": "Include connected node names in results (default: false). Expensive — only enable when you need to see neighbor names."
				},
				"exclude_labels": {
					"type": "array",
					"items": {"type": "string"},
					"description": "Labels to exclude from results. Community nodes are excluded by default — pass [] to include them."
				},
				"sort_by": {
					"type": "string",
					"enum": ["relevance", "name", "degree"],
					"description": "Sort order. Default: relevance (exact match first, prefix match second, then by connectivity)"
				}
			}
		}`),
	}, s.handleSearchGraph)

	s.addTool(&mcp.Tool{
		Name:        "search_code",
		Description: "Search for text in source code files (like grep, scoped to indexed project). Returns 10 matches per page — use offset to paginate, has_more indicates more pages. Use for string literals, error messages, TODO comments.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"pattern": {
					"type": "string",
					"description": "Text to search for (literal string, or regex if regex=true)"
				},
				"file_pattern": {
					"type": "string",
					"description": "Glob pattern to filter files (e.g. '*.go', '*.py')"
				},
				"regex": {
					"type": "boolean",
					"description": "Treat pattern as a regular expression (default: false)"
				},
				"max_results": {
					"type": "integer",
					"description": "Max matches per page (default: 10). Response includes has_more flag for pagination."
				},
				"offset": {
					"type": "integer",
					"description": "Skip N matches for pagination (default: 0). Check has_more in response."
				},
				"project": {
					"type": "string",
					"description": "Project to search in. Defaults to session project."
				}
			},
			"required": ["pattern"]
		}`),
	}, s.handleSearchCode)
}

func (s *Server) registerQueryTool() {
	s.addTool(&mcp.Tool{
		Name:        "query_graph",
		Description: "Execute a Cypher-like graph query. WARNING: 200-row cap applies BEFORE aggregation — COUNT queries on large codebases silently undercount. For fan-out/fan-in counting, use search_graph with min_degree/max_degree instead. Best for: relationship patterns, filtered joins, path queries, and edge property filtering. Supports WHERE on edge properties: r.url_path CONTAINS 'orders', r.confidence >= 0.6, r.method = 'POST', r.confidence_band = 'high', r.validated_by_trace = true, r.coupling_score >= 0.5. This is the correct tool for listing cross-service edges — use MATCH (a)-[r:HTTP_CALLS]->(b) RETURN a.name, b.name, r.url_path, r.confidence, r.confidence_band to see HTTP links with URLs and confidence scores (bands: high>=0.7, medium>=0.45, speculative>=0.25), or MATCH (a)-[r:ASYNC_CALLS]->(b) for async dispatch edges. For change coupling: MATCH (a)-[r:FILE_CHANGES_WITH]->(b) RETURN a.name, b.name, r.coupling_score, r.co_change_count. For interface method overrides: MATCH (s)-[r:OVERRIDE]->(i) to find struct methods implementing interface methods. For read references (callbacks, variable assignments): MATCH (a)-[r:USAGE]->(b). Always use LIMIT. Edge types: CALLS, HTTP_CALLS, ASYNC_CALLS, IMPORTS, DEFINES, DEFINES_METHOD, HANDLES, IMPLEMENTS, OVERRIDE, USAGE, FILE_CHANGES_WITH.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"query": {
					"type": "string",
					"description": "Cypher query, e.g. MATCH (f:Function)-[:CALLS]->(g:Function) WHERE f.name = 'main' RETURN g.name, g.qualified_name LIMIT 20"
				},
				"project": {
					"type": "string",
					"description": "Project to query. Defaults to session project."
				}
			},
			"required": ["query"]
		}`),
	}, s.handleQueryGraph)
}

// registerFileTools registers tools for file and directory operations.
func (s *Server) registerFileTools() {
	s.addTool(&mcp.Tool{
		Name:        "read_file",
		Description: "Read any file from the indexed project. Supports line range selection for large files. Use for reading config files (Dockerfile, go.mod, requirements.txt), source code, or any text file.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"path": {
					"type": "string",
					"description": "File path (absolute, or relative to project root)"
				},
				"start_line": {
					"type": "integer",
					"description": "Start reading from this line (1-based, optional)"
				},
				"end_line": {
					"type": "integer",
					"description": "Stop reading at this line (inclusive, optional)"
				}
			},
			"required": ["path"]
		}`),
	}, s.handleReadFile)

	s.addTool(&mcp.Tool{
		Name:        "list_directory",
		Description: "List files and subdirectories in a directory. Supports glob patterns for filtering. Use for exploring project structure.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"path": {
					"type": "string",
					"description": "Directory path (absolute, or relative to project root). Empty for project root."
				},
				"pattern": {
					"type": "string",
					"description": "Glob pattern to filter entries (e.g. '*.go', '*.py')"
				},
				"project": {
					"type": "string",
					"description": "Project name to resolve root from. Defaults to session project."
				}
			}
		}`),
	}, s.handleListDirectory)
}

// registerProjectTools registers tools for project management.
func (s *Server) registerProjectTools() {
	s.addTool(&mcp.Tool{
		Name:        "list_projects",
		Description: "List all indexed projects with their indexed_at timestamp, root path, and node/edge counts.",
		InputSchema: json.RawMessage(`{"type": "object"}`),
	}, s.handleListProjects)

	s.addTool(&mcp.Tool{
		Name:        "delete_project",
		Description: "Delete an indexed project and all its graph data (nodes, edges, file hashes). Removes the project's .db file. This action is irreversible.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"project_name": {
					"type": "string",
					"description": "Name of the project to delete"
				}
			},
			"required": ["project_name"]
		}`),
	}, s.handleDeleteProject)

	s.addTool(&mcp.Tool{
		Name:        "index_status",
		Description: "Check the indexing status of a project. Returns whether the project is indexed, currently indexing, or not found. Shows last indexed timestamp, node/edge counts, and whether the index is initial or incremental. Use this to check if the graph is ready for queries.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"project": {
					"type": "string",
					"description": "Project name to check. Defaults to the auto-detected session project."
				}
			}
		}`),
	}, s.handleIndexStatus)
}

// --- Helpers ---

// jsonResult marshals data to JSON and returns as tool result.
func jsonResult(data any) *mcp.CallToolResult {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return errResult("json marshal err=" + err.Error())
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(b)},
		},
	}
}

// errResult returns a tool result indicating an error.
func errResult(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: msg},
		},
		IsError: true,
	}
}

// parseArgs unmarshals the raw JSON arguments into a map.
func parseArgs(req *mcp.CallToolRequest) (map[string]any, error) {
	if len(req.Params.Arguments) == 0 {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal(req.Params.Arguments, &m); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	return m, nil
}

// getStringArg extracts a string argument from parsed args.
func getStringArg(args map[string]any, key string) string {
	v, ok := args[key]
	if !ok {
		return ""
	}
	str, ok := v.(string)
	if !ok {
		return ""
	}
	return str
}

// getIntArg extracts an integer argument with a default value.
func getIntArg(args map[string]any, key string, defaultVal int) int {
	v, ok := args[key]
	if !ok {
		return defaultVal
	}
	f, ok := v.(float64) // JSON numbers decode as float64
	if !ok {
		return defaultVal
	}
	return int(f)
}

// getBoolArg extracts a boolean argument from parsed args.
func getBoolArg(args map[string]any, key string) bool {
	v, ok := args[key]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	if !ok {
		return false
	}
	return b
}

// findNodeAcrossProjects searches for a node by simple name in the specified project.
// Falls back to the session project if no filter is given.
func (s *Server) findNodeAcrossProjects(name string, projectFilter ...string) (*store.Node, string, error) {
	filter := s.sessionProject
	if len(projectFilter) > 0 && projectFilter[0] != "" {
		if projectFilter[0] == "*" || projectFilter[0] == "all" {
			return nil, "", fmt.Errorf("cross-project queries are not supported; use list_projects to find a specific project name, or omit the project parameter to use the current session project")
		}
		filter = projectFilter[0]
	}
	if filter == "" {
		return nil, "", fmt.Errorf("no project specified and no session project detected")
	}
	if !s.router.HasProject(filter) {
		return nil, "", fmt.Errorf("project %q not found; use list_projects to see available projects", filter)
	}

	st, err := s.router.ForProject(filter)
	if err != nil {
		return nil, "", err
	}
	projects, _ := st.ListProjects()
	for _, p := range projects {
		nodes, findErr := st.FindNodesByName(p.Name, name)
		if findErr != nil {
			continue
		}
		if len(nodes) > 0 {
			return nodes[0], p.Name, nil
		}
	}
	return nil, "", fmt.Errorf("node not found: %s", name)
}

// findNodeByQNAcrossProjects searches for a node by qualified name in the specified project.
// Falls back to the session project if no filter is given.
func (s *Server) findNodeByQNAcrossProjects(qn string, projectFilter ...string) (*store.Node, string, error) {
	filter := s.sessionProject
	if len(projectFilter) > 0 && projectFilter[0] != "" {
		if projectFilter[0] == "*" || projectFilter[0] == "all" {
			return nil, "", fmt.Errorf("cross-project queries are not supported; use list_projects to find a specific project name, or omit the project parameter to use the current session project")
		}
		filter = projectFilter[0]
	}
	if filter == "" {
		return nil, "", fmt.Errorf("no project specified and no session project detected")
	}
	if !s.router.HasProject(filter) {
		return nil, "", fmt.Errorf("project %q not found; use list_projects to see available projects", filter)
	}

	st, err := s.router.ForProject(filter)
	if err != nil {
		return nil, "", err
	}
	projects, _ := st.ListProjects()
	for _, p := range projects {
		node, findErr := st.FindNodeByQN(p.Name, qn)
		if findErr != nil {
			continue
		}
		if node != nil {
			return node, p.Name, nil
		}
	}
	return nil, "", fmt.Errorf("node not found: %s", qn)
}
