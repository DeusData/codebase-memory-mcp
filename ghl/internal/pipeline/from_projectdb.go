// Package pipeline — PopulateFromProjectDB builds org.db from hydrated project .db files.
// Uses list_projects (single MCP call) to get all indexed repos, then populates org.db
// with repo metadata + team mapping. No per-project MCP calls needed.
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

// PopulateOrgFromProjectDBs builds org.db from all hydrated project .db files.
// It makes ONE MCP call (list_projects) to get all indexed repos with node/edge counts,
// then writes repo metadata + team ownership to org.db.
// This works on fresh containers because project .db files are hydrated from GCS.
func PopulateOrgFromProjectDBs(ctx context.Context, db *orgdb.DB, caller MCPCaller, repos []manifest.Repo) error {
	// Single MCP call: list all indexed projects
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
		// Try wrapped format
		var wrapped struct{ Projects []projectInfo }
		if err2 := json.Unmarshal([]byte(text), &wrapped); err2 != nil {
			return fmt.Errorf("pipeline: parse list_projects: %w", err)
		}
		projects = wrapped.Projects
	}

	slog.Info("populating org.db from project list", "projects", len(projects))

	// Build repo lookup for team/type metadata
	repoByName := make(map[string]manifest.Repo, len(repos))
	for _, r := range repos {
		repoByName[r.Name] = r
	}

	populated := 0
	for _, proj := range projects {
		repoName := proj.Name

		// Strip path-based prefixes the C binary adds to project names
		for _, prefix := range []string{
			"data-fleet-cache-repos-",
			"tmp-fleet-cache-repos-",
			"tmp-fleet-cache-",
			"app-fleet-cache-",
		} {
			if strings.HasPrefix(repoName, prefix) {
				repoName = strings.TrimPrefix(repoName, prefix)
				break
			}
		}

		// Match to manifest repo for team/type metadata
		repo, ok := repoByName[repoName]
		if !ok {
			repo = manifest.Repo{Name: repoName}
		}

		// Write to org.db
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

func extractText(result *mcp.ToolResult) string {
	if result == nil || len(result.Content) == 0 {
		return ""
	}
	return result.Content[0].Text
}
