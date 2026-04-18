// Package pipeline — PopulateFromProjectDB builds org.db from list_projects MCP call.
// Only populates repo metadata + team ownership. Route/dependency data comes from
// the indexing pipeline (OnRepoDone) when repos are cloned and enriched.
//
// IMPORTANT: Do NOT open project .db files from Go — this conflicts with the C binary
// subprocesses and crashes the bridge pool. Use MCP tools only.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/orgdb"
)

// MCPCaller is the interface for calling MCP tools on the C binary.
type MCPCaller interface {
	CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error)
}

// PopulateOrgFromProjectDBs builds org.db repo metadata from list_projects.
// Makes ONE MCP call to get all indexed repos with node/edge counts.
// Route/dependency/contract data comes separately from indexing pipeline (OnRepoDone).
func PopulateOrgFromProjectDBs(ctx context.Context, db *orgdb.DB, caller MCPCaller, repos []manifest.Repo, cbmCacheDir string) error {
	result, err := caller.CallTool(ctx, "list_projects", nil)
	if err != nil {
		return fmt.Errorf("pipeline: list_projects: %w", err)
	}
	text := extractText(result)
	if text == "" || text == "null" {
		return fmt.Errorf("pipeline: list_projects returned empty")
	}

	var projects []projectInfo
	if err := json.Unmarshal([]byte(text), &projects); err != nil {
		var wrapped struct{ Projects []projectInfo }
		if err2 := json.Unmarshal([]byte(text), &wrapped); err2 != nil {
			return fmt.Errorf("pipeline: parse list_projects: %w", err)
		}
		projects = wrapped.Projects
	}

	slog.Info("populating org.db from project list", "projects", len(projects))

	repoByName := make(map[string]manifest.Repo, len(repos))
	for _, r := range repos {
		repoByName[r.Name] = r
	}

	populated := 0
	for _, proj := range projects {
		repoName := stripProjectPrefix(proj.Name)

		repo, ok := repoByName[repoName]
		if !ok {
			repo = manifest.Repo{Name: repoName}
		}

		db.UpsertRepo(orgdb.RepoRecord{
			Name:      repoName,
			GitHubURL: repo.GitHubURL,
			Team:      repo.Team,
			Type:      repo.Type,
			NodeCount: proj.Nodes,
			EdgeCount: proj.Edges,
		})
		db.UpsertTeamOwnership(repoName, repo.Team, "")
		populated++
	}

	slog.Info("org.db populated from project list", "repos", populated)
	return nil
}

type projectInfo struct {
	Name  string `json:"name"`
	Nodes int    `json:"nodes"`
	Edges int    `json:"edges"`
}

func stripProjectPrefix(name string) string {
	for _, prefix := range []string{
		"data-fleet-cache-repos-",
		"tmp-fleet-cache-repos-",
		"tmp-fleet-cache-",
		"app-fleet-cache-",
	} {
		if strings.HasPrefix(name, prefix) {
			return strings.TrimPrefix(name, prefix)
		}
	}
	return name
}

func extractText(result *mcp.ToolResult) string {
	if result == nil || len(result.Content) == 0 {
		return ""
	}
	return result.Content[0].Text
}
