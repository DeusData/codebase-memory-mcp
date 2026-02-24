package tools

import (
	"encoding/json"
	"fmt"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Server wraps the MCP server with tool handlers.
type Server struct {
	mcp   *mcp.Server
	store *store.Store
}

// NewServer creates a new MCP server with all tools registered.
func NewServer(s *store.Store) *Server {
	srv := &Server{
		store: s,
		mcp: mcp.NewServer(
			&mcp.Implementation{
				Name:    "codebase-memory-mcp",
				Version: "0.1.0",
			},
			nil,
		),
	}
	srv.registerTools()
	return srv
}

// MCPServer returns the underlying MCP server.
func (s *Server) MCPServer() *mcp.Server {
	return s.mcp
}

func (s *Server) registerTools() {
	// 1. index_repository
	s.mcp.AddTool(&mcp.Tool{
		Name:        "index_repository",
		Description: "Index a repository into the code graph. Parses source files, extracts functions/classes/modules, resolves call relationships, and stores the graph for querying. Supports incremental reindex via content hashing.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"repo_path": {
					"type": "string",
					"description": "Absolute path to the repository to index. If omitted, uses the configured project root."
				}
			}
		}`),
	}, s.handleIndexRepository)

	// 2. trace_call_path
	s.mcp.AddTool(&mcp.Tool{
		Name:        "trace_call_path",
		Description: "Trace call paths from/to a function using BFS traversal. Returns the root function with signature and module constants, hop-by-hop callees/callers, and call edges with type (CALLS or HTTP_CALLS). Use for understanding call chains and data flow.",
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
				}
			},
			"required": ["function_name"]
		}`),
	}, s.handleTraceCallPath)

	// 3. get_graph_schema
	s.mcp.AddTool(&mcp.Tool{
		Name:        "get_graph_schema",
		Description: "Return the schema of the indexed code graph: node label counts, edge type counts, relationship patterns (e.g. Function-CALLS->Function), and sample function/class names. Use to understand what's in the graph before querying.",
		InputSchema: json.RawMessage(`{"type": "object"}`),
	}, s.handleGetGraphSchema)

	// 4. get_code_snippet
	s.mcp.AddTool(&mcp.Tool{
		Name:        "get_code_snippet",
		Description: "Retrieve source code for a function/class by qualified name. Reads directly from disk using the stored file path and line range. Returns the source code with line numbers.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"qualified_name": {
					"type": "string",
					"description": "Fully qualified name of the node (e.g. 'myproject.cmd.server.main.HandleRequest')"
				}
			},
			"required": ["qualified_name"]
		}`),
	}, s.handleGetCodeSnippet)

	// 5. search_graph
	s.mcp.AddTool(&mcp.Tool{
		Name:        "search_graph",
		Description: "Search the code graph with structured filters. Replaces raw Cypher queries with safe, parameterized search. Supports filtering by node label, name pattern (regex), file pattern (glob), relationship type, direction, and degree (fan-in/fan-out). Returns matching nodes with properties and edge counts.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"label": {
					"type": "string",
					"description": "Node label filter: Function, Class, Module, Method, Interface, Enum, Type, File, Package, Folder"
				},
				"name_pattern": {
					"type": "string",
					"description": "Regex pattern for node name (e.g. '.*Handler', 'Send.*')"
				},
				"file_pattern": {
					"type": "string",
					"description": "Glob pattern for file path (e.g. '**/order-service/**')"
				},
				"relationship": {
					"type": "string",
					"description": "Filter by relationship type: CALLS, HTTP_CALLS, IMPORTS, DEFINES, etc."
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
					"description": "Max results (default 50, max 200)"
				}
			}
		}`),
	}, s.handleSearchGraph)

	// 6. list_projects
	s.mcp.AddTool(&mcp.Tool{
		Name:        "list_projects",
		Description: "List all indexed projects with their indexed_at timestamp, root path, and node/edge counts.",
		InputSchema: json.RawMessage(`{"type": "object"}`),
	}, s.handleListProjects)

	// 7. delete_project
	s.mcp.AddTool(&mcp.Tool{
		Name:        "delete_project",
		Description: "Delete an indexed project and all its graph data (nodes, edges, file hashes). This action is irreversible.",
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

	// 8. read_file
	s.mcp.AddTool(&mcp.Tool{
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

	// 9. list_directory
	s.mcp.AddTool(&mcp.Tool{
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
				}
			}
		}`),
	}, s.handleListDirectory)

	// 10. search_code
	s.mcp.AddTool(&mcp.Tool{
		Name:        "search_code",
		Description: "Search for text patterns within source code files. Like grep/ripgrep but scoped to indexed project files. Returns matching lines with file paths and line numbers. Use for finding string literals, error messages, TODO comments, or any text within function bodies.",
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
					"description": "Maximum number of matches to return (default 50, max 200)"
				}
			},
			"required": ["pattern"]
		}`),
	}, s.handleSearchCode)

	// 11. query_graph
	s.mcp.AddTool(&mcp.Tool{
		Name:        "query_graph",
		Description: "Execute a Cypher-like graph query. Supports MATCH patterns with node labels, relationship types, variable-length paths, WHERE filters (=, =~, CONTAINS, STARTS WITH, >, <), and RETURN with COUNT/ORDER BY/LIMIT/DISTINCT. Read-only (no CREATE/DELETE).",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"query": {
					"type": "string",
					"description": "Cypher query, e.g. MATCH (f:Function)-[:CALLS]->(g:Function) WHERE f.name = 'main' RETURN g.name, g.qualified_name LIMIT 20"
				}
			},
			"required": ["query"]
		}`),
	}, s.handleQueryGraph)
}

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
	if req.Params.Arguments == nil || len(req.Params.Arguments) == 0 {
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
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
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

// findNodeAcrossProjects searches all projects for a node by simple name.
func (s *Server) findNodeAcrossProjects(name string) (*store.Node, string, error) {
	projects, err := s.store.ListProjects()
	if err != nil {
		return nil, "", fmt.Errorf("list projects: %w", err)
	}
	for _, p := range projects {
		nodes, findErr := s.store.FindNodesByName(p.Name, name)
		if findErr != nil {
			continue
		}
		if len(nodes) > 0 {
			return nodes[0], p.Name, nil
		}
	}
	return nil, "", fmt.Errorf("node not found: %s", name)
}

// findNodeByQNAcrossProjects searches all projects for a node by qualified name.
func (s *Server) findNodeByQNAcrossProjects(qn string) (*store.Node, string, error) {
	projects, err := s.store.ListProjects()
	if err != nil {
		return nil, "", fmt.Errorf("list projects: %w", err)
	}
	for _, p := range projects {
		node, findErr := s.store.FindNodeByQN(p.Name, qn)
		if findErr != nil {
			continue
		}
		if node != nil {
			return node, p.Name, nil
		}
	}
	return nil, "", fmt.Errorf("node not found: %s", qn)
}
