package mcp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/Shidfar/codebase-memory-mcp/service-graph/cbm"
	"github.com/Shidfar/codebase-memory-mcp/service-graph/graph"
	"github.com/Shidfar/codebase-memory-mcp/service-graph/scanner"
)

// Server is the unified MCP server that routes 14 C tools and 7 Go tools.
type Server struct {
	bridge   *cbm.Bridge
	store    *graph.Store
	scanners graph.Scanners
}

// NewServer initializes the CGo bridge and the service-graph store.
func NewServer() (*Server, error) {
	bridge, err := cbm.Init("")
	if err != nil {
		return nil, fmt.Errorf("init cbm bridge: %w", err)
	}
	store, err := graph.NewStore()
	if err != nil {
		bridge.Close()
		return nil, fmt.Errorf("init graph store: %w", err)
	}
	sc := graph.Scanners{
		DiscoverFiles:           scanner.DiscoverFiles,
		InferServiceID:          scanner.InferServiceID,
		CollectTopicDeclarations: scanner.CollectTopicDeclarations,
		ResolveEdges:            scanner.ResolveEdges,
		ScanGraphQL:             scanner.ScanGraphQL,
		ScanDatabase:            scanner.ScanDatabase,
	}
	return &Server{bridge: bridge, store: store, scanners: sc}, nil
}

// Close releases resources.
func (s *Server) Close() {
	if s.store != nil {
		s.store.Close()
	}
	if s.bridge != nil {
		s.bridge.Close()
	}
}

// JSON-RPC types

type jsonrpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonrpcResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   *rpcError   `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

type textContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type toolResult struct {
	Content []textContent `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

// 7 service-graph tool names
var goTools = map[string]bool{
	"scan_repos":       true,
	"list_services":    true,
	"list_topics":      true,
	"trace_message":    true,
	"find_dependencies": true,
	"get_graph":        true,
	"shared_resources": true,
}

// Run starts the stdio JSON-RPC event loop.
func (s *Server) Run() error {
	reader := bufio.NewReaderSize(os.Stdin, 4*1024*1024)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil // EOF = clean shutdown
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		resp := s.handleLine(line)
		if resp == "" {
			continue // notification — no response
		}
		fmt.Fprintln(os.Stdout, resp)
	}
}

func (s *Server) handleLine(line string) string {
	var req jsonrpcRequest
	if err := json.Unmarshal([]byte(line), &req); err != nil {
		return marshalResponse(jsonrpcResponse{
			JSONRPC: "2.0",
			ID:      nil,
			Error:   &rpcError{Code: -32700, Message: "Parse error"},
		})
	}

	var id interface{}
	if req.ID != nil {
		json.Unmarshal(req.ID, &id)
	}

	switch req.Method {
	case "initialize":
		return s.handleInitialize(id)
	case "notifications/initialized":
		return "" // notification
	case "tools/list":
		return s.handleToolsList(id)
	case "tools/call":
		return s.handleToolsCall(id, req.Params)
	default:
		// Try routing through C for any unknown method
		result, err := s.bridge.HandleRequest(line)
		if err != nil || result == "" {
			return marshalResponse(jsonrpcResponse{
				JSONRPC: "2.0",
				ID:      id,
				Error:   &rpcError{Code: -32601, Message: "Method not found"},
			})
		}
		return result
	}
}

func (s *Server) handleInitialize(id interface{}) string {
	result := map[string]interface{}{
		"protocolVersion": "2024-11-05",
		"capabilities": map[string]interface{}{
			"tools": map[string]interface{}{},
		},
		"serverInfo": map[string]interface{}{
			"name":    "codebase-memory-mcp",
			"version": "1.0.0",
		},
	}
	return marshalResponse(jsonrpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func (s *Server) handleToolsList(id interface{}) string {
	// Get C tools from the bridge — returns {"tools": [...]}
	cToolsJSON := cbm.ToolsList()
	var cWrapper struct {
		Tools []interface{} `json:"tools"`
	}
	json.Unmarshal([]byte(cToolsJSON), &cWrapper)

	// Append Go service-graph tool definitions
	goToolDefs := serviceGraphToolDefs()
	allTools := append(cWrapper.Tools, goToolDefs...)

	result := map[string]interface{}{
		"tools": allTools,
	}
	return marshalResponse(jsonrpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func (s *Server) handleToolsCall(id interface{}, params json.RawMessage) string {
	var p toolCallParams
	if err := json.Unmarshal(params, &p); err != nil {
		return marshalResponse(jsonrpcResponse{
			JSONRPC: "2.0",
			ID:      id,
			Error:   &rpcError{Code: -32602, Message: "Invalid params"},
		})
	}

	var result toolResult

	if goTools[p.Name] {
		// Route to Go handler
		result = s.handleGoTool(p.Name, p.Arguments)
	} else {
		// Route to C handler via bridge
		argsStr := "{}"
		if p.Arguments != nil {
			argsStr = string(p.Arguments)
		}
		resp, err := s.bridge.HandleTool(p.Name, argsStr)
		if err != nil {
			result = toolResult{
				Content: []textContent{{Type: "text", Text: fmt.Sprintf("Error: %v", err)}},
				IsError: true,
			}
		} else {
			// C handler returns MCP tool result JSON — parse and re-wrap
			var cResult toolResult
			if err := json.Unmarshal([]byte(resp), &cResult); err != nil {
				// Treat raw response as text
				result = toolResult{
					Content: []textContent{{Type: "text", Text: resp}},
				}
			} else {
				result = cResult
			}
		}
	}

	return marshalResponse(jsonrpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

// handleGoTool dispatches to the 7 service-graph tool handlers.
func (s *Server) handleGoTool(name string, args json.RawMessage) toolResult {
	switch name {
	case "scan_repos":
		return s.toolScanRepos(args)
	case "list_services":
		return s.toolListServices()
	case "list_topics":
		return s.toolListTopics()
	case "trace_message":
		return s.toolTraceMessage(args)
	case "find_dependencies":
		return s.toolFindDependencies(args)
	case "get_graph":
		return s.toolGetGraph(args)
	case "shared_resources":
		return s.toolSharedResources(args)
	default:
		return textResult(fmt.Sprintf("Unknown tool: %s", name), true)
	}
}

// ── Tool 1: scan_repos ──────────────────────────────────────

func (s *Server) toolScanRepos(args json.RawMessage) toolResult {
	var params struct {
		Repos []string `json:"repos"`
	}
	if args != nil {
		json.Unmarshal(args, &params)
	}

	var repoPaths []string
	if len(params.Repos) > 0 {
		repoPaths = params.Repos
	} else {
		reposDir := os.Getenv("REPOS_DIR")
		if reposDir == "" {
			return textResult("No repos specified and REPOS_DIR not set. Either pass repo paths or set REPOS_DIR.", true)
		}
		var err error
		repoPaths, err = scanner.DiscoverRepos(reposDir)
		if err != nil {
			return textResult(fmt.Sprintf("Error discovering repos: %v", err), true)
		}
		if len(repoPaths) == 0 {
			return textResult(fmt.Sprintf("No git repos found in %s", reposDir), true)
		}
	}

	g, err := graph.ScanAllRepos(repoPaths, s.scanners, s.store)
	if err != nil {
		return textResult(fmt.Sprintf("Scan error: %v", err), true)
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Scanned %d services:\n", len(g.Services))
	for _, svc := range g.Services {
		fmt.Fprintf(&sb, "  - %s (%s)\n", svc.ID, svc.RepoPath)
	}
	fmt.Fprintf(&sb, "\nFound:\n")
	fmt.Fprintf(&sb, "  %d Pub/Sub topics\n", len(g.Topics))
	fmt.Fprintf(&sb, "  %d GraphQL endpoints\n", len(g.GraphQLEndpoints))
	fmt.Fprintf(&sb, "  %d database tables\n", len(g.Tables))
	fmt.Fprintf(&sb, "  %d dependency edges\n", len(g.Edges))
	fmt.Fprintf(&sb, "\nGraph saved at %s", g.ScannedAt)

	dangling := graph.FindDanglingLeaves(g)
	sb.WriteString(formatDangling(dangling))

	return textResult(sb.String(), false)
}

// ── Tool 2: list_services ───────────────────────────────────

func (s *Server) toolListServices() toolResult {
	g, err := s.ensureGraph()
	if err != nil {
		return textResult(err.Error(), true)
	}

	var sb strings.Builder
	for _, svc := range g.Services {
		edges := edgesBySource(g.Edges, svc.ID)
		publishes := countByType(edges, graph.EdgePublishes)
		subscribes := countByType(edges, graph.EdgeSubscribes)
		queries := countByType(edges, graph.EdgeGraphQLQuery) + countByType(edges, graph.EdgeGraphQLMutation)
		tables := countByType(edges, graph.EdgeDBOwns) + countByType(edges, graph.EdgeDBReads)

		fmt.Fprintf(&sb, "## %s\n", svc.ID)
		fmt.Fprintf(&sb, "  Path: %s\n", svc.RepoPath)
		fmt.Fprintf(&sb, "  Publishes: %d topics\n", publishes)
		fmt.Fprintf(&sb, "  Subscribes: %d topics\n", subscribes)
		fmt.Fprintf(&sb, "  GraphQL calls: %d\n", queries)
		fmt.Fprintf(&sb, "  DB tables: %d\n\n", tables)
	}

	return textResult(sb.String(), false)
}

// ── Tool 3: list_topics ─────────────────────────────────────

func (s *Server) toolListTopics() toolResult {
	g, err := s.ensureGraph()
	if err != nil {
		return textResult(err.Error(), true)
	}

	var sb strings.Builder
	for _, topic := range g.Topics {
		publishers := edgesForTarget(g.Edges, topic.ID, graph.EdgePublishes)
		subscribers := edgesForTarget(g.Edges, topic.ID, graph.EdgeSubscribes)

		fmt.Fprintf(&sb, "## %s\n", topic.ID)
		if len(publishers) > 0 {
			sb.WriteString("  Publishers:\n")
			for _, p := range publishers {
				fmt.Fprintf(&sb, "    - %s @ %s:%d\n", p.Source, p.File, p.Line)
			}
		}
		if len(subscribers) > 0 {
			sb.WriteString("  Subscribers:\n")
			for _, s := range subscribers {
				fmt.Fprintf(&sb, "    - %s @ %s:%d\n", s.Source, s.File, s.Line)
			}
		}
		if len(publishers) == 0 && len(subscribers) == 0 {
			sb.WriteString("  (declared but no publish/subscribe calls found)\n")
		}
		sb.WriteByte('\n')
	}

	return textResult(sb.String(), false)
}

// ── Tool 4: trace_message ───────────────────────────────────

func (s *Server) toolTraceMessage(args json.RawMessage) toolResult {
	var params struct {
		Topic string `json:"topic"`
	}
	if args != nil {
		json.Unmarshal(args, &params)
	}
	if params.Topic == "" {
		return textResult("Missing required parameter: topic", true)
	}

	g, err := s.ensureGraph()
	if err != nil {
		return textResult(err.Error(), true)
	}

	var matching []graph.TopicNode
	for _, t := range g.Topics {
		if t.ID == params.Topic || strings.Contains(t.ID, params.Topic) {
			matching = append(matching, t)
		}
	}

	if len(matching) == 0 {
		var ids []string
		for _, t := range g.Topics {
			ids = append(ids, t.ID)
		}
		return textResult(fmt.Sprintf("No topics matching %q. Available: %s", params.Topic, strings.Join(ids, ", ")), false)
	}

	var sb strings.Builder
	for _, t := range matching {
		publishers := edgesForTarget(g.Edges, t.ID, graph.EdgePublishes)
		subscribers := edgesForTarget(g.Edges, t.ID, graph.EdgeSubscribes)

		fmt.Fprintf(&sb, "=== Message Flow: %s ===\n\n", t.ID)
		if len(publishers) > 0 {
			sb.WriteString("PUBLISH (source):\n")
			for _, p := range publishers {
				fmt.Fprintf(&sb, "  %s", formatEdge(p))
			}
		} else {
			sb.WriteString("PUBLISH: (!) no publisher found in scanned repos\n")
		}
		sb.WriteString("  |\n")
		fmt.Fprintf(&sb, "  v topic: %s\n", t.ID)
		sb.WriteString("  |\n")
		if len(subscribers) > 0 {
			sb.WriteString("SUBSCRIBE (consumers):\n")
			for _, sub := range subscribers {
				fmt.Fprintf(&sb, "  %s", formatEdge(sub))
			}
		} else {
			sb.WriteString("SUBSCRIBE: (!) no subscriber found in scanned repos\n")
		}
		sb.WriteByte('\n')
	}

	return textResult(sb.String(), false)
}

// ── Tool 5: find_dependencies ───────────────────────────────

func (s *Server) toolFindDependencies(args json.RawMessage) toolResult {
	var params struct {
		Service string `json:"service"`
	}
	if args != nil {
		json.Unmarshal(args, &params)
	}
	if params.Service == "" {
		return textResult("Missing required parameter: service", true)
	}

	g, err := s.ensureGraph()
	if err != nil {
		return textResult(err.Error(), true)
	}

	var svc *graph.ServiceNode
	for i := range g.Services {
		if g.Services[i].ID == params.Service || strings.Contains(g.Services[i].ID, params.Service) {
			svc = &g.Services[i]
			break
		}
	}
	if svc == nil {
		var ids []string
		for _, s := range g.Services {
			ids = append(ids, s.ID)
		}
		return textResult(fmt.Sprintf("Service %q not found. Available: %s", params.Service, strings.Join(ids, ", ")), false)
	}

	edges := edgesBySource(g.Edges, svc.ID)
	sections := make(map[string][]graph.Edge)
	for _, e := range edges {
		sections[e.Type] = append(sections[e.Type], e)
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "=== Dependencies: %s ===\n\n", svc.ID)

	typeLabels := []struct {
		t     string
		label string
	}{
		{graph.EdgePublishes, "PUBLISHES to topics"},
		{graph.EdgeSubscribes, "SUBSCRIBES to topics"},
		{graph.EdgeGraphQLQuery, "GRAPHQL QUERIES (outgoing)"},
		{graph.EdgeGraphQLMutation, "GRAPHQL MUTATIONS (outgoing)"},
		{graph.EdgeGraphQLExposes, "GRAPHQL SCHEMA (exposed)"},
		{graph.EdgeDBOwns, "DATABASE TABLES (owned)"},
		{graph.EdgeDBReads, "DATABASE TABLES (read)"},
	}

	for _, tl := range typeLabels {
		group := sections[tl.t]
		if len(group) > 0 {
			fmt.Fprintf(&sb, "%s:\n", tl.label)
			for _, e := range group {
				fmt.Fprintf(&sb, "  %s", formatEdge(e))
			}
			sb.WriteByte('\n')
		}
	}

	// Incoming edges
	var incoming []graph.Edge
	for _, e := range g.Edges {
		if strings.HasPrefix(e.Target, svc.ID) && e.Source != svc.ID {
			incoming = append(incoming, e)
		}
	}
	if len(incoming) > 0 {
		sb.WriteString("DEPENDED ON BY:\n")
		for _, e := range incoming {
			fmt.Fprintf(&sb, "  %s", formatEdge(e))
		}
		sb.WriteByte('\n')
	}

	return textResult(sb.String(), false)
}

// ── Tool 6: get_graph ───────────────────────────────────────

func (s *Server) toolGetGraph(args json.RawMessage) toolResult {
	var params struct {
		Filter string `json:"filter"`
	}
	if args != nil {
		json.Unmarshal(args, &params)
	}
	if params.Filter == "" {
		params.Filter = "all"
	}

	g, err := s.ensureGraph()
	if err != nil {
		return textResult(err.Error(), true)
	}

	filtered := filterEdges(g.Edges, params.Filter)
	output := graph.ServiceGraph{
		Services:         g.Services,
		Topics:           g.Topics,
		GraphQLEndpoints: g.GraphQLEndpoints,
		Tables:           g.Tables,
		Edges:            filtered,
		ScannedAt:        g.ScannedAt,
	}

	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return textResult(fmt.Sprintf("Error marshaling graph: %v", err), true)
	}
	return textResult(string(data), false)
}

// ── Tool 7: shared_resources ────────────────────────────────

func (s *Server) toolSharedResources(args json.RawMessage) toolResult {
	var params struct {
		Filter string `json:"filter"`
	}
	if args != nil {
		json.Unmarshal(args, &params)
	}
	if params.Filter == "" {
		params.Filter = "all"
	}

	g, err := s.ensureGraph()
	if err != nil {
		return textResult(err.Error(), true)
	}

	var sb strings.Builder

	if params.Filter == "all" || params.Filter == "pubsub" || params.Filter == "database" {
		type targetInfo struct {
			sources map[string]map[string]bool // source → set of types
			edges   []graph.Edge
		}
		targetToSources := make(map[string]*targetInfo)

		for _, edge := range g.Edges {
			if params.Filter == "pubsub" && edge.Type != graph.EdgePublishes && edge.Type != graph.EdgeSubscribes {
				continue
			}
			if params.Filter == "database" && edge.Type != graph.EdgeDBOwns && edge.Type != graph.EdgeDBReads {
				continue
			}

			info, ok := targetToSources[edge.Target]
			if !ok {
				info = &targetInfo{sources: make(map[string]map[string]bool)}
				targetToSources[edge.Target] = info
			}
			if _, ok := info.sources[edge.Source]; !ok {
				info.sources[edge.Source] = make(map[string]bool)
			}
			info.sources[edge.Source][edge.Type] = true
			info.edges = append(info.edges, edge)
		}

		// Filter to shared (>1 source service)
		var shared []struct {
			target string
			info   *targetInfo
		}
		for target, info := range targetToSources {
			if len(info.sources) > 1 {
				shared = append(shared, struct {
					target string
					info   *targetInfo
				}{target, info})
			}
		}

		if len(shared) > 0 {
			sb.WriteString("=== Shared Resources ===\n\n")
			for _, s := range shared {
				var serviceList []string
				for svc, types := range s.info.sources {
					var typeList []string
					for t := range types {
						typeList = append(typeList, t)
					}
					serviceList = append(serviceList, fmt.Sprintf("%s (%s)", svc, strings.Join(typeList, ", ")))
				}
				fmt.Fprintf(&sb, "## %s\n", s.target)
				fmt.Fprintf(&sb, "  Services: %s\n", strings.Join(serviceList, ", "))
				for _, e := range s.info.edges {
					fmt.Fprintf(&sb, "    %s --[%s]--> %s @ %s:%d\n", e.Source, e.Type, e.Target, e.File, e.Line)
				}
				sb.WriteByte('\n')
			}
		} else {
			sb.WriteString("No shared resources found across scanned services.\n\n")
		}
	}

	if params.Filter == "all" || params.Filter == "dangling" {
		dangling := graph.FindDanglingLeaves(g)
		hasAny := len(dangling.SubscribedNotPublished) > 0 ||
			len(dangling.PublishedNotSubscribed) > 0 ||
			len(dangling.ReadNotOwned) > 0

		if hasAny {
			sb.WriteString("=== Dangling Leaves ===\n\n")
			for _, d := range dangling.SubscribedNotPublished {
				fmt.Fprintf(&sb, "## %s\n", d.Resource)
				fmt.Fprintf(&sb, "  Subscribed by: %s\n", strings.Join(d.Services, ", "))
				sb.WriteString("  Publisher: NOT FOUND in scanned repos\n\n")
			}
			for _, d := range dangling.PublishedNotSubscribed {
				fmt.Fprintf(&sb, "## %s\n", d.Resource)
				fmt.Fprintf(&sb, "  Published by: %s\n", strings.Join(d.Services, ", "))
				sb.WriteString("  Subscriber: NOT FOUND in scanned repos\n\n")
			}
			for _, d := range dangling.ReadNotOwned {
				fmt.Fprintf(&sb, "## %s\n", d.Resource)
				fmt.Fprintf(&sb, "  Read by: %s\n", strings.Join(d.Services, ", "))
				sb.WriteString("  Owner: NOT FOUND in scanned repos\n\n")
			}
		} else if params.Filter == "dangling" {
			sb.WriteString("No dangling leaves — all dependencies resolved within scanned repos.\n")
		}
	}

	return textResult(sb.String(), false)
}

// ── Helpers ─────────────────────────────────────────────────

func (s *Server) ensureGraph() (*graph.ServiceGraph, error) {
	g, err := s.store.GetGraph()
	if err != nil {
		return nil, fmt.Errorf("load graph: %w", err)
	}
	if len(g.Services) == 0 {
		return nil, fmt.Errorf("no graph available. Run scan_repos first")
	}
	return g, nil
}

func textResult(text string, isError bool) toolResult {
	return toolResult{
		Content: []textContent{{Type: "text", Text: text}},
		IsError: isError,
	}
}

func formatEdge(e graph.Edge) string {
	meta := ""
	if len(e.Metadata) > 0 {
		var parts []string
		for k, v := range e.Metadata {
			parts = append(parts, fmt.Sprintf("%s: %s", k, v))
		}
		meta = fmt.Sprintf(" (%s)", strings.Join(parts, ", "))
	}
	return fmt.Sprintf("%s --[%s]--> %s  @ %s:%d%s\n", e.Source, e.Type, e.Target, e.File, e.Line, meta)
}

func formatDangling(d graph.DanglingResult) string {
	hasAny := len(d.SubscribedNotPublished) > 0 ||
		len(d.PublishedNotSubscribed) > 0 ||
		len(d.ReadNotOwned) > 0
	if !hasAny {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("\n\nDangling leaves (unresolved dependencies):")
	for _, e := range d.SubscribedNotPublished {
		fmt.Fprintf(&sb, "\n  (!) topic %q — subscribed by [%s] but no publisher found", e.Resource, strings.Join(e.Services, ", "))
	}
	for _, e := range d.PublishedNotSubscribed {
		fmt.Fprintf(&sb, "\n  (!) topic %q — published by [%s] but no subscriber found", e.Resource, strings.Join(e.Services, ", "))
	}
	for _, e := range d.ReadNotOwned {
		fmt.Fprintf(&sb, "\n  (!) table %q — read by [%s] but no owning service found", e.Resource, strings.Join(e.Services, ", "))
	}
	return sb.String()
}

func edgesBySource(edges []graph.Edge, source string) []graph.Edge {
	var out []graph.Edge
	for _, e := range edges {
		if e.Source == source {
			out = append(out, e)
		}
	}
	return out
}

func edgesForTarget(edges []graph.Edge, target, edgeType string) []graph.Edge {
	var out []graph.Edge
	for _, e := range edges {
		if e.Target == target && e.Type == edgeType {
			out = append(out, e)
		}
	}
	return out
}

func countByType(edges []graph.Edge, edgeType string) int {
	n := 0
	for _, e := range edges {
		if e.Type == edgeType {
			n++
		}
	}
	return n
}

func filterEdges(edges []graph.Edge, filter string) []graph.Edge {
	if filter == "all" {
		return edges
	}
	var out []graph.Edge
	for _, e := range edges {
		switch filter {
		case "pubsub":
			if e.Type == graph.EdgePublishes || e.Type == graph.EdgeSubscribes {
				out = append(out, e)
			}
		case "graphql":
			if e.Type == graph.EdgeGraphQLQuery || e.Type == graph.EdgeGraphQLMutation || e.Type == graph.EdgeGraphQLExposes {
				out = append(out, e)
			}
		case "database":
			if e.Type == graph.EdgeDBOwns || e.Type == graph.EdgeDBReads {
				out = append(out, e)
			}
		}
	}
	return out
}

func marshalResponse(r jsonrpcResponse) string {
	data, _ := json.Marshal(r)
	return string(data)
}

// serviceGraphToolDefs returns the 7 Go tool definitions in MCP format.
func serviceGraphToolDefs() []interface{} {
	return []interface{}{
		map[string]interface{}{
			"name":        "scan_repos",
			"description": "Scan repos and build the service dependency graph. Uses REPOS_DIR env var by default, or pass explicit paths.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"repos": map[string]interface{}{
						"type":        "array",
						"items":       map[string]interface{}{"type": "string"},
						"description": "Absolute paths to repos. If omitted, discovers all git repos in REPOS_DIR.",
					},
				},
			},
		},
		map[string]interface{}{
			"name":        "list_services",
			"description": "List all discovered services with summary of their dependencies.",
			"inputSchema": map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			},
		},
		map[string]interface{}{
			"name":        "list_topics",
			"description": "List all Pub/Sub topics with their publishers and subscribers.",
			"inputSchema": map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			},
		},
		map[string]interface{}{
			"name":        "trace_message",
			"description": "Trace a Pub/Sub message flow: who publishes a topic and who subscribes to it, with file locations.",
			"inputSchema": map[string]interface{}{
				"type":     "object",
				"required": []string{"topic"},
				"properties": map[string]interface{}{
					"topic": map[string]interface{}{
						"type":        "string",
						"description": "Topic name or substring to search for.",
					},
				},
			},
		},
		map[string]interface{}{
			"name":        "find_dependencies",
			"description": "Show all dependencies for a service: what it publishes, subscribes to, queries, and which tables it accesses.",
			"inputSchema": map[string]interface{}{
				"type":     "object",
				"required": []string{"service"},
				"properties": map[string]interface{}{
					"service": map[string]interface{}{
						"type":        "string",
						"description": "Service name (e.g. 'user-service').",
					},
				},
			},
		},
		map[string]interface{}{
			"name":        "get_graph",
			"description": "Return the full dependency graph as structured JSON, or a filtered subset.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"filter": map[string]interface{}{
						"type":        "string",
						"enum":        []string{"all", "pubsub", "graphql", "database"},
						"description": "Filter to specific dependency type. Default: all.",
					},
				},
			},
		},
		map[string]interface{}{
			"name":        "shared_resources",
			"description": "Find resources (Pub/Sub topics, DB tables) accessed by multiple services. Shows coupling points and dangling leaves.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"filter": map[string]interface{}{
						"type":        "string",
						"enum":        []string{"all", "pubsub", "database", "dangling"},
						"description": "Filter output. 'dangling' shows only unresolved dependencies. Default: all.",
					},
				},
			},
		},
	}
}
