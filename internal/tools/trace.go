package tools

import (
	"context"
	"fmt"

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

	// Find the function node across all projects
	rootNode, project, _ := s.findNodeAcrossProjects(funcName)
	if rootNode == nil {
		return errResult(fmt.Sprintf("function not found: %s", funcName)), nil
	}

	edgeTypes := []string{"CALLS", "HTTP_CALLS", "ASYNC_CALLS"}

	// Build root info
	root := buildNodeInfo(rootNode)

	// Get module info (constants) by finding the module that defines this function
	moduleInfo := s.getModuleInfo(rootNode, project)

	// Run BFS
	var allVisited []*store.NodeHop
	var allEdges []store.EdgeInfo

	if direction == "both" {
		// Run outbound + inbound separately, merge
		outResult, outErr := s.store.BFS(rootNode.ID, "outbound", edgeTypes, depth, 200)
		if outErr == nil {
			allVisited = append(allVisited, outResult.Visited...)
			allEdges = append(allEdges, outResult.Edges...)
		}
		inResult, inErr := s.store.BFS(rootNode.ID, "inbound", edgeTypes, depth, 200)
		if inErr == nil {
			allVisited = append(allVisited, inResult.Visited...)
			allEdges = append(allEdges, inResult.Edges...)
		}
	} else {
		result, bfsErr := s.store.BFS(rootNode.ID, direction, edgeTypes, depth, 200)
		if bfsErr != nil {
			return errResult(fmt.Sprintf("bfs err: %v", bfsErr)), nil
		}
		allVisited = result.Visited
		allEdges = result.Edges
	}

	// Group visited nodes by hop
	hops := buildHops(allVisited)

	// Build edge list
	edges := buildEdgeList(allEdges)

	// Get indexed_at from project
	proj, _ := s.store.GetProject(project)
	indexedAt := ""
	if proj != nil {
		indexedAt = proj.IndexedAt
	}

	return jsonResult(map[string]any{
		"root":          root,
		"module":        moduleInfo,
		"hops":          hops,
		"edges":         edges,
		"indexed_at":    indexedAt,
		"total_results": len(allVisited),
	}), nil
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

func (s *Server) getModuleInfo(funcNode *store.Node, project string) map[string]any {
	if funcNode.FilePath == "" {
		return map[string]any{}
	}

	// Find module nodes in the same file
	modules, err := s.store.FindNodesByLabel(project, "Module")
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
