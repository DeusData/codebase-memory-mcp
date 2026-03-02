package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"

	"github.com/DeusData/codebase-memory-mcp/internal/pipeline"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *Server) registerDetectChanges() {
	s.addTool(&mcp.Tool{
		Name:        "detect_changes",
		Description: "Detect uncommitted or branch changes and map them to affected graph symbols + blast radius. Runs git diff, maps changed hunks to functions/classes in the graph, then traces inbound callers with risk classification (CRITICAL/HIGH/MEDIUM/LOW). Requires git in PATH. Risk is topology-based: hop 1=CRITICAL (direct callers), 2=HIGH, 3=MEDIUM, 4+=LOW.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"scope": {
					"type": "string",
					"description": "Which changes to analyze: 'unstaged' (working tree), 'staged' (git add), 'all' (HEAD, default), 'branch' (compare with base_branch)",
					"enum": ["unstaged", "staged", "all", "branch"]
				},
				"base_branch": {
					"type": "string",
					"description": "Base branch for scope=branch comparison (default: main)"
				},
				"depth": {
					"type": "integer",
					"description": "Maximum BFS depth for impact tracing (1-5, default 3)"
				},
				"project": {
					"type": "string",
					"description": "Project to analyze. Defaults to session project."
				}
			}
		}`),
	}, s.handleDetectChanges)
}

func (s *Server) handleDetectChanges(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := parseArgs(req)
	if err != nil {
		return errResult(err.Error()), nil
	}

	scopeStr := getStringArg(args, "scope")
	if scopeStr == "" {
		scopeStr = "all"
	}
	scope := pipeline.DiffScope(scopeStr)

	baseBranch := getStringArg(args, "base_branch")
	depth := getIntArg(args, "depth", 3)
	if depth < 1 {
		depth = 1
	}
	if depth > 5 {
		depth = 5
	}

	project := getStringArg(args, "project")
	effectiveProject := s.resolveProjectName(project)

	st, repoPath, projName, resolveErr := s.resolveDetectRepo(effectiveProject)
	if resolveErr != nil {
		return resolveErr, nil
	}

	// Parse changed files
	changedFiles, err := pipeline.ParseGitDiffFiles(repoPath, scope, baseBranch)
	if err != nil {
		return errResult(fmt.Sprintf("git diff: %v", err)), nil
	}

	if len(changedFiles) == 0 {
		return s.emptyDetectResponse(), nil
	}

	// Parse hunks for line-level mapping
	hunks, err := pipeline.ParseGitDiffHunks(repoPath, scope, baseBranch)
	if err != nil {
		slog.Warn("detect_changes.hunks.err", "err", err)
	}

	changedSymbols := mapChangesToSymbols(st, projName, changedFiles, hunks)
	impactedSymbols, allEdges := traceImpact(st, changedSymbols, depth)
	summary := buildDetectSummary(changedFiles, changedSymbols, impactedSymbols, allEdges)

	responseData := map[string]any{
		"changed_files":    buildFileList(changedFiles),
		"changed_symbols":  buildSymbolList(changedSymbols),
		"impacted_symbols": buildImpactList(impactedSymbols),
		"summary":          summary,
	}
	s.addIndexStatus(responseData)

	result := jsonResult(responseData)
	s.addUpdateNotice(result)
	return result, nil
}

// resolveDetectRepo resolves the store, repo path, and project name for detect_changes.
func (s *Server) resolveDetectRepo(effectiveProject string) (st *store.Store, repoPath, projName string, toolErr *mcp.CallToolResult) {
	var err error
	st, err = s.resolveStore(effectiveProject)
	if err != nil {
		return nil, "", "", errResult(err.Error())
	}

	projects, _ := st.ListProjects()
	if len(projects) == 0 {
		return nil, "", "", errResult("no projects indexed")
	}
	projName = projects[0].Name
	proj, err := st.GetProject(projName)
	if err != nil {
		return nil, "", "", errResult(fmt.Sprintf("project %q: %v", effectiveProject, err))
	}
	if proj == nil {
		return nil, "", "", errResult(fmt.Sprintf("project %q not found", effectiveProject))
	}
	if proj.RootPath == "" {
		return nil, "", "", errResult("project has no root_path — reindex with repo_path")
	}
	return st, proj.RootPath, projName, nil
}

func (s *Server) emptyDetectResponse() *mcp.CallToolResult {
	responseData := map[string]any{
		"changed_files":    []any{},
		"changed_symbols":  []any{},
		"impacted_symbols": []any{},
		"summary": map[string]any{
			"changed_files":     0,
			"changed_symbols":   0,
			"critical":          0,
			"high":              0,
			"medium":            0,
			"low":               0,
			"total":             0,
			"has_cross_service": false,
		},
	}
	s.addIndexStatus(responseData)
	result := jsonResult(responseData)
	s.addUpdateNotice(result)
	return result
}

func buildFileList(files []pipeline.ChangedFile) []map[string]any {
	result := make([]map[string]any, len(files))
	for i, f := range files {
		entry := map[string]any{"status": f.Status, "path": f.Path}
		if f.OldPath != "" {
			entry["old_path"] = f.OldPath
		}
		result[i] = entry
	}
	return result
}

func buildSymbolList(symbols []*store.Node) []map[string]any {
	result := make([]map[string]any, len(symbols))
	for i, n := range symbols {
		result[i] = map[string]any{
			"name":           n.Name,
			"qualified_name": n.QualifiedName,
			"label":          n.Label,
			"file_path":      n.FilePath,
			"start_line":     n.StartLine,
			"end_line":       n.EndLine,
		}
	}
	return result
}

func buildImpactList(impacted []impactedSymbol) []map[string]any {
	result := make([]map[string]any, len(impacted))
	for i, is := range impacted {
		result[i] = map[string]any{
			"name":           is.Node.Name,
			"qualified_name": is.Node.QualifiedName,
			"label":          is.Node.Label,
			"file_path":      is.Node.FilePath,
			"risk":           string(store.HopToRisk(is.Hop)),
			"hop":            is.Hop,
			"changed_by":     is.ChangedBy,
		}
	}
	return result
}

// impactedSymbol extends NodeHop with the symbol that caused the impact.
type impactedSymbol struct {
	Node      *store.Node
	Hop       int
	ChangedBy string // name of the changed symbol that led to this impact
}

// mapChangesToSymbols finds graph nodes affected by the changed hunks.
func mapChangesToSymbols(st *store.Store, project string, files []pipeline.ChangedFile, hunks []pipeline.ChangedHunk) []*store.Node {
	seen := make(map[int64]bool)
	var result []*store.Node

	hunksByFile := make(map[string][]pipeline.ChangedHunk)
	for _, h := range hunks {
		hunksByFile[h.Path] = append(hunksByFile[h.Path], h)
	}

	for _, f := range files {
		if f.Status == "D" {
			continue
		}
		nodes := resolveFileSymbols(st, project, f.Path, hunksByFile[f.Path])
		for _, n := range nodes {
			if !seen[n.ID] {
				seen[n.ID] = true
				result = append(result, n)
			}
		}
	}

	if len(result) == 0 && len(files) > 0 {
		logZeroSymbolsDiag(files)
	}

	return result
}

// resolveFileSymbols finds symbols for a single file, using hunk-level overlap when available.
func resolveFileSymbols(st *store.Store, project, path string, hunks []pipeline.ChangedHunk) []*store.Node {
	if len(hunks) > 0 {
		return resolveByHunks(st, project, path, hunks)
	}
	return resolveByFile(st, project, path)
}

func resolveByHunks(st *store.Store, project, path string, hunks []pipeline.ChangedHunk) []*store.Node {
	var result []*store.Node
	for _, h := range hunks {
		nodes, err := st.FindNodesByFileOverlap(project, path, h.StartLine, h.EndLine)
		if err != nil {
			slog.Debug("detect_changes.overlap.err", "path", path, "err", err)
			continue
		}
		result = append(result, nodes...)
	}
	return result
}

func resolveByFile(st *store.Store, project, path string) []*store.Node {
	nodes, err := st.FindNodesByFile(project, path)
	if err != nil {
		slog.Debug("detect_changes.file.err", "path", path, "err", err)
		return nil
	}
	if len(nodes) == 0 {
		nodes, err = st.FindNodesByFileOverlap(project, path, 0, 999999)
		if err != nil {
			return nil
		}
	}
	return nodes
}

func logZeroSymbolsDiag(files []pipeline.ChangedFile) {
	paths := make([]string, 0, 3)
	for i, f := range files {
		if i >= 3 {
			break
		}
		paths = append(paths, f.Path)
	}
	slog.Warn("detect_changes.zero_symbols", "diff_paths", paths)
}

// traceImpact runs inbound BFS from each changed symbol and deduplicates results.
func traceImpact(st *store.Store, changedSymbols []*store.Node, depth int) ([]impactedSymbol, []store.EdgeInfo) {
	edgeTypes := []string{"CALLS", "HTTP_CALLS", "ASYNC_CALLS"}
	bestHop := make(map[int64]*impactedSymbol)
	changedIDs := make(map[int64]bool)
	var allEdges []store.EdgeInfo

	for _, sym := range changedSymbols {
		changedIDs[sym.ID] = true
	}

	for _, sym := range changedSymbols {
		result, err := st.BFS(sym.ID, "inbound", edgeTypes, depth, 200)
		if err != nil {
			slog.Debug("detect_changes.bfs.err", "sym", sym.Name, "err", err)
			continue
		}

		for _, nh := range result.Visited {
			if changedIDs[nh.Node.ID] {
				continue // skip the changed symbols themselves
			}
			existing, ok := bestHop[nh.Node.ID]
			if !ok || nh.Hop < existing.Hop {
				bestHop[nh.Node.ID] = &impactedSymbol{
					Node:      nh.Node,
					Hop:       nh.Hop,
					ChangedBy: sym.Name,
				}
			}
		}
		allEdges = append(allEdges, result.Edges...)
	}

	// Sort by risk (hop ascending)
	sorted := make([]impactedSymbol, 0, len(bestHop))
	for _, is := range bestHop {
		sorted = append(sorted, *is)
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Hop != sorted[j].Hop {
			return sorted[i].Hop < sorted[j].Hop
		}
		return sorted[i].Node.Name < sorted[j].Node.Name
	})

	return sorted, allEdges
}

func buildDetectSummary(files []pipeline.ChangedFile, symbols []*store.Node, impacted []impactedSymbol, edges []store.EdgeInfo) map[string]any {
	var critical, high, medium, low int
	for _, is := range impacted {
		switch store.HopToRisk(is.Hop) {
		case store.RiskCritical:
			critical++
		case store.RiskHigh:
			high++
		case store.RiskMedium:
			medium++
		case store.RiskLow:
			low++
		}
	}

	hasCrossService := false
	for _, e := range edges {
		if e.Type == "HTTP_CALLS" || e.Type == "ASYNC_CALLS" {
			hasCrossService = true
			break
		}
	}

	return map[string]any{
		"changed_files":     len(files),
		"changed_symbols":   len(symbols),
		"critical":          critical,
		"high":              high,
		"medium":            medium,
		"low":               low,
		"total":             len(impacted),
		"has_cross_service": hasCrossService,
	}
}
