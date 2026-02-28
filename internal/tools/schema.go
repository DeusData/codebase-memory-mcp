package tools

import (
	"context"
	"fmt"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *Server) handleGetGraphSchema(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := parseArgs(req)
	if err != nil {
		return errResult(err.Error()), nil
	}

	project := getStringArg(args, "project")
	st, err := s.resolveStore(project)
	if err != nil {
		return errResult(fmt.Sprintf("resolve store: %v", err)), nil
	}

	projName := s.resolveProjectName(project)
	projects, _ := st.ListProjects()
	if len(projects) > 0 {
		projName = projects[0].Name
	}

	schema, schemaErr := st.GetSchema(projName)
	if schemaErr != nil {
		return errResult(fmt.Sprintf("schema: %v", schemaErr)), nil
	}

	type projectSchema struct {
		Project string            `json:"project"`
		Schema  *store.SchemaInfo `json:"schema"`
	}

	return jsonResult(map[string]any{
		"projects": []projectSchema{{Project: projName, Schema: schema}},
	}), nil
}
