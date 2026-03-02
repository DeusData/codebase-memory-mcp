package tools

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *Server) handleTraceCallPath(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := parseArgs(req)
	if err != nil {
		return errResult(err.Error()), nil
	}

	funcName := getStringArg(args, "function_name")
	if funcName == "" {
		return errResult("function_name is required"), nil
	}

	depth := getIntArg(args, "depth", 3)
	if depth < 1 {
		depth = 1
	}
	if depth > 5 {
		depth = 5
	}

	direction := getStringArg(args, "direction")
	if direction == "" {
		direction = "outbound"
	}

	riskLabels := getBoolArg(args, "risk_labels")

	project := getStringArg(args, "project")
	effectiveProject := s.resolveProjectName(project)

	// Find the function node
	rootNode, foundProject, findErr := s.findNodeAcrossProjects(funcName, effectiveProject)
	if findErr != nil && !strings.HasPrefix(findErr.Error(), "node not found") {
		return errResult(findErr.Error()), nil
	}
	if rootNode == nil {
		// Fuzzy fallback: search for similar names and return structured suggestions
		suggestions := s.findSimilarNodes(funcName, effectiveProject, 5)
		if len(suggestions) > 0 {
			suggList := make([]map[string]string, len(suggestions))
			for i, n := range suggestions {
				suggList[i] = map[string]string{
					"name":           n.Name,
					"qualified_name": n.QualifiedName,
					"label":          n.Label,
				}
			}
			return jsonResult(map[string]any{
				"error":       fmt.Sprintf("function not found: %s", funcName),
				"suggestions": suggList,
			}), nil
		}
		return errResult(fmt.Sprintf("function not found: %s", funcName)), nil
	}

	// Get the store for the found project
	st, err := s.router.ForProject(foundProject)
	if err != nil {
		return errResult(fmt.Sprintf("store: %v", err)), nil
	}

	edgeTypes := []string{"CALLS", "HTTP_CALLS", "ASYNC_CALLS"}

	allVisited, allEdges, bfsErr := runTraceBFS(st, rootNode.ID, direction, edgeTypes, depth)
	if bfsErr != nil {
		return errResult(fmt.Sprintf("bfs err: %v", bfsErr)), nil
	}

	if riskLabels {
		allVisited = store.DeduplicateHops(allVisited)
	}

	var hops []hopEntry
	if riskLabels {
		hops = buildHopsWithRisk(allVisited)
	} else {
		hops = buildHops(allVisited)
	}

	responseData := buildTraceResponse(st, rootNode, foundProject, hops, allVisited, allEdges)
	if riskLabels {
		responseData["impact_summary"] = store.BuildImpactSummary(allVisited, allEdges)
	}
	responseData["module"] = s.getModuleInfo(st, rootNode, foundProject)
	s.addIndexStatus(responseData)

	result := jsonResult(responseData)
	s.addUpdateNotice(result)
	return result, nil
}

func runTraceBFS(st *store.Store, rootID int64, direction string, edgeTypes []string, depth int) ([]*store.NodeHop, []store.EdgeInfo, error) {
	if direction == "both" {
		var allVisited []*store.NodeHop
		var allEdges []store.EdgeInfo
		outResult, outErr := st.BFS(rootID, "outbound", edgeTypes, depth, 200)
		if outErr == nil {
			allVisited = append(allVisited, outResult.Visited...)
			allEdges = append(allEdges, outResult.Edges...)
		}
		inResult, inErr := st.BFS(rootID, "inbound", edgeTypes, depth, 200)
		if inErr == nil {
			allVisited = append(allVisited, inResult.Visited...)
			allEdges = append(allEdges, inResult.Edges...)
		}
		return allVisited, allEdges, nil
	}
	result, err := st.BFS(rootID, direction, edgeTypes, depth, 200)
	if err != nil {
		return nil, nil, err
	}
	return result.Visited, result.Edges, nil
}

func buildTraceResponse(st *store.Store, rootNode *store.Node, project string, hops []hopEntry, visited []*store.NodeHop, edges []store.EdgeInfo) map[string]any {
	proj, _ := st.GetProject(project)
	indexedAt := ""
	if proj != nil {
		indexedAt = proj.IndexedAt
	}
	return map[string]any{
		"root":          buildNodeInfo(rootNode),
		"hops":          hops,
		"edges":         buildEdgeList(edges),
		"indexed_at":    indexedAt,
		"total_results": len(visited),
	}
}

func buildNodeInfo(n *store.Node) map[string]any {
	info := map[string]any{
		"name":           n.Name,
		"qualified_name": n.QualifiedName,
		"label":          n.Label,
		"file_path":      n.FilePath,
		"start_line":     n.StartLine,
		"end_line":       n.EndLine,
	}
	if sig, ok := n.Properties["signature"]; ok {
		info["signature"] = sig
	}
	if rt, ok := n.Properties["return_type"]; ok {
		info["return_type"] = rt
	}
	return info
}

func (s *Server) getModuleInfo(st *store.Store, funcNode *store.Node, project string) map[string]any {
	if funcNode.FilePath == "" {
		return map[string]any{}
	}

	modules, err := st.FindNodesByLabel(project, "Module")
	if err != nil {
		return map[string]any{}
	}

	for _, m := range modules {
		if m.FilePath == funcNode.FilePath {
			info := map[string]any{"name": m.Name}
			if constants, ok := m.Properties["constants"]; ok {
				info["constants"] = constants
			}
			return info
		}
	}
	return map[string]any{}
}

type hopEntry struct {
	Hop   int              `json:"hop"`
	Nodes []map[string]any `json:"nodes"`
}

func buildHops(visited []*store.NodeHop) []hopEntry {
	hopMap := map[int][]map[string]any{}
	for _, nh := range visited {
		info := map[string]any{
			"name":           nh.Node.Name,
			"qualified_name": nh.Node.QualifiedName,
			"label":          nh.Node.Label,
		}
		if sig, ok := nh.Node.Properties["signature"]; ok {
			info["signature"] = sig
		}
		hopMap[nh.Hop] = append(hopMap[nh.Hop], info)
	}

	var hops []hopEntry
	for h := 1; h <= len(hopMap); h++ {
		if nodes, ok := hopMap[h]; ok {
			hops = append(hops, hopEntry{Hop: h, Nodes: nodes})
		}
	}
	return hops
}

func buildHopsWithRisk(visited []*store.NodeHop) []hopEntry {
	hopMap := map[int][]map[string]any{}
	for _, nh := range visited {
		info := map[string]any{
			"name":           nh.Node.Name,
			"qualified_name": nh.Node.QualifiedName,
			"label":          nh.Node.Label,
			"risk":           string(store.HopToRisk(nh.Hop)),
			"hop":            nh.Hop,
		}
		if sig, ok := nh.Node.Properties["signature"]; ok {
			info["signature"] = sig
		}
		hopMap[nh.Hop] = append(hopMap[nh.Hop], info)
	}

	var hops []hopEntry
	for h := 1; h <= len(hopMap); h++ {
		if nodes, ok := hopMap[h]; ok {
			hops = append(hops, hopEntry{Hop: h, Nodes: nodes})
		}
	}
	return hops
}

// findSimilarNodes searches for nodes whose name contains the input string (case-insensitive).
func (s *Server) findSimilarNodes(name, project string, limit int) []*store.Node {
	effectiveProject := s.resolveProjectName(project)
	if effectiveProject == "" {
		return nil
	}
	if !s.router.HasProject(effectiveProject) {
		return nil
	}
	st, err := s.router.ForProject(effectiveProject)
	if err != nil {
		return nil
	}
	// Get actual project name from DB
	projName := effectiveProject
	projects, _ := st.ListProjects()
	if len(projects) > 0 {
		projName = projects[0].Name
	}
	params := &store.SearchParams{
		Project:       projName,
		NamePattern:   "(?i)" + regexp.QuoteMeta(name),
		Limit:         limit,
		MinDegree:     -1,
		MaxDegree:     -1,
		ExcludeLabels: []string{"Community"},
	}
	out, searchErr := st.Search(params)
	if searchErr != nil {
		return nil
	}
	nodes := make([]*store.Node, len(out.Results))
	for i, r := range out.Results {
		nodes[i] = r.Node
	}
	return nodes
}

func buildEdgeList(edges []store.EdgeInfo) []map[string]any {
	result := make([]map[string]any, 0, len(edges))
	for _, e := range edges {
		result = append(result, map[string]any{
			"from": e.FromName,
			"to":   e.ToName,
			"type": e.Type,
		})
	}
	return result
}
