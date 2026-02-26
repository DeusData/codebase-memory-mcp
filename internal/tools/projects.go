package tools

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *Server) handleListProjects(_ context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	projects, err := s.store.ListProjects()
	if err != nil {
		return errResult(fmt.Sprintf("list projects: %v", err)), nil
	}

	type projectInfo struct {
		Name      string `json:"name"`
		RootPath  string `json:"root_path"`
		IndexedAt string `json:"indexed_at"`
		Nodes     int    `json:"nodes"`
		Edges     int    `json:"edges"`
	}

	result := make([]projectInfo, 0, len(projects))
	for _, p := range projects {
		nc, _ := s.store.CountNodes(p.Name)
		ec, _ := s.store.CountEdges(p.Name)
		result = append(result, projectInfo{
			Name:      p.Name,
			RootPath:  p.RootPath,
			IndexedAt: p.IndexedAt,
			Nodes:     nc,
			Edges:     ec,
		})
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
	proj, _ := s.store.GetProject(name)
	if proj == nil {
		return errResult(fmt.Sprintf("project not found: %s", name)), nil
	}

	if err := s.store.DeleteProject(name); err != nil {
		return errResult(fmt.Sprintf("delete failed: %v", err)), nil
	}

	// file_hashes table is not cascaded from projects — delete separately
	if err := s.store.DeleteFileHashes(name); err != nil {
		return errResult(fmt.Sprintf("delete file hashes failed: %v", err)), nil
	}

	return jsonResult(map[string]any{
		"deleted": name,
		"status":  "ok",
	}), nil
}
