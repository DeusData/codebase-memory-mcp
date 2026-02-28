package tools

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *Server) handleListProjects(_ context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	projectInfos, err := s.router.ListProjects()
	if err != nil {
		return errResult(fmt.Sprintf("list projects: %v", err)), nil
	}

	type projectEntry struct {
		Name             string `json:"name"`
		RootPath         string `json:"root_path"`
		IndexedAt        string `json:"indexed_at"`
		Nodes            int    `json:"nodes"`
		Edges            int    `json:"edges"`
		DBPath           string `json:"db_path"`
		IsSessionProject bool   `json:"is_session_project,omitempty"`
	}

	result := make([]projectEntry, 0, len(projectInfos))
	for _, info := range projectInfos {
		st, err := s.router.ForProject(info.Name)
		if err != nil {
			continue
		}

		nc, _ := st.CountNodes(info.Name)
		ec, _ := st.CountEdges(info.Name)

		indexedAt := ""
		rootPath := info.RootPath
		proj, _ := st.GetProject(info.Name)
		if proj != nil {
			indexedAt = proj.IndexedAt
			rootPath = proj.RootPath
		}

		entry := projectEntry{
			Name:      info.Name,
			RootPath:  rootPath,
			IndexedAt: indexedAt,
			Nodes:     nc,
			Edges:     ec,
			DBPath:    info.DBPath,
		}
		if info.Name == s.sessionProject {
			entry.IsSessionProject = true
		}
		result = append(result, entry)
	}

	return jsonResult(result), nil
}

func (s *Server) handleDeleteProject(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := parseArgs(req)
	if err != nil {
		return errResult(err.Error()), nil
	}

	name := getStringArg(args, "project_name")
	if name == "" {
		return errResult("project_name is required"), nil
	}

	// Verify project exists
	if !s.router.HasProject(name) {
		return errResult(fmt.Sprintf("project not found: %s", name)), nil
	}

	if err := s.router.DeleteProject(name); err != nil {
		return errResult(fmt.Sprintf("delete failed: %v", err)), nil
	}

	return jsonResult(map[string]any{
		"deleted": name,
		"status":  "ok",
	}), nil
}

func (s *Server) handleIndexStatus(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := parseArgs(req)
	if err != nil {
		return errResult(err.Error()), nil
	}

	projectName := getStringArg(args, "project")
	if projectName == "" {
		projectName = s.sessionProject
	}
	if projectName == "" {
		return jsonResult(map[string]any{
			"status":  "no_session",
			"message": "No session project detected. Pass 'project' parameter or ensure the MCP client provides roots.",
		}), nil
	}

	// Check if DB file exists
	if !s.router.HasProject(projectName) {
		return jsonResult(map[string]any{
			"project": projectName,
			"status":  "not_indexed",
			"message": fmt.Sprintf("No index found for project %q. Call index_repository to create one.", projectName),
			"db_path": filepath.Join(s.router.Dir(), projectName+".db"),
		}), nil
	}

	// Get store and project metadata
	st, err := s.router.ForProject(projectName)
	if err != nil {
		return errResult(fmt.Sprintf("open store: %v", err)), nil
	}

	proj, _ := st.GetProject(projectName)
	if proj == nil {
		// DB file exists but no project row — partially indexed or corrupted
		return jsonResult(map[string]any{
			"project": projectName,
			"status":  "partial",
			"message": "Database file exists but project metadata is missing. Re-run index_repository.",
			"db_path": filepath.Join(s.router.Dir(), projectName+".db"),
		}), nil
	}

	nodeCount, _ := st.CountNodes(projectName)
	edgeCount, _ := st.CountEdges(projectName)

	// Determine current indexing state
	currentStatus, _ := s.indexStatus.Load().(string)
	isSessionProject := projectName == s.sessionProject

	result := map[string]any{
		"project":            projectName,
		"status":             "ready",
		"indexed_at":         proj.IndexedAt,
		"root_path":          proj.RootPath,
		"nodes":              nodeCount,
		"edges":              edgeCount,
		"is_session_project": isSessionProject,
		"db_path":            filepath.Join(s.router.Dir(), projectName+".db"),
	}

	// Determine index type (initial vs incremental)
	if nodeCount == 0 && edgeCount == 0 {
		result["index_type"] = "none"
	} else {
		result["index_type"] = "incremental"
	}

	// If this is the session project and indexing is in progress
	if isSessionProject && currentStatus == "indexing" {
		result["status"] = "indexing"
		if startedAt, ok := s.indexStartedAt.Load().(time.Time); ok {
			result["index_started_at"] = startedAt.UTC().Format(time.RFC3339)
			result["index_elapsed_seconds"] = int(time.Since(startedAt).Seconds())
		}
		if nodeCount == 0 {
			result["index_type"] = "initial"
		}
	}

	return jsonResult(result), nil
}
