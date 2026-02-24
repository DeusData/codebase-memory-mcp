package tools

import (
	"context"
	"fmt"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *Server) handleSearchGraph(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := parseArgs(req)
	if err != nil {
		return errResult(err.Error()), nil
	}

	params := store.SearchParams{
		Label:              getStringArg(args, "label"),
		NamePattern:        getStringArg(args, "name_pattern"),
		FilePattern:        getStringArg(args, "file_pattern"),
		Relationship:       getStringArg(args, "relationship"),
		Direction:          getStringArg(args, "direction"),
		MinDegree:          getIntArg(args, "min_degree", -1),
		MaxDegree:          getIntArg(args, "max_degree", -1),
		Limit:              getIntArg(args, "limit", 50),
		ExcludeEntryPoints: getBoolArg(args, "exclude_entry_points"),
	}

	projects, err := s.store.ListProjects()
	if err != nil {
		return errResult(fmt.Sprintf("list projects: %v", err)), nil
	}

	if len(projects) == 0 {
		return jsonResult(map[string]any{
			"message": "no projects indexed",
			"results": []any{},
		}), nil
	}

	// Search across all projects, collect results
	type resultEntry struct {
		Project        string   `json:"project"`
		Name           string   `json:"name"`
		QualifiedName  string   `json:"qualified_name"`
		Label          string   `json:"label"`
		FilePath       string   `json:"file_path"`
		StartLine      int      `json:"start_line"`
		EndLine        int      `json:"end_line"`
		InDegree       int      `json:"in_degree"`
		OutDegree      int      `json:"out_degree"`
		ConnectedNames []string `json:"connected_names,omitempty"`
	}

	var allResults []resultEntry
	for _, p := range projects {
		params.Project = p.Name
		results, searchErr := s.store.Search(params)
		if searchErr != nil {
			continue
		}
		for _, r := range results {
			entry := resultEntry{
				Project:        p.Name,
				Name:           r.Node.Name,
				QualifiedName:  r.Node.QualifiedName,
				Label:          r.Node.Label,
				FilePath:       r.Node.FilePath,
				StartLine:      r.Node.StartLine,
				EndLine:        r.Node.EndLine,
				InDegree:       r.InDegree,
				OutDegree:      r.OutDegree,
				ConnectedNames: r.ConnectedNames,
			}
			allResults = append(allResults, entry)
		}
	}

	return jsonResult(map[string]any{
		"total":   len(allResults),
		"results": allResults,
	}), nil
}
