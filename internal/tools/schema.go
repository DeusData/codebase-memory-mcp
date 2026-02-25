package tools

import (
	"context"
	"fmt"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *Server) handleGetGraphSchema(_ context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	projects, err := s.store.ListProjects()
	if err != nil {
		return errResult(fmt.Sprintf("list projects: %v", err)), nil
	}

	if len(projects) == 0 {
		return jsonResult(map[string]any{
			"message":  "no projects indexed",
			"projects": []any{},
		}), nil
	}

	type projectSchema struct {
		Project string            `json:"project"`
		Schema  *store.SchemaInfo `json:"schema"`
	}

	schemas := make([]projectSchema, 0, len(projects))
	for _, p := range projects {
		schema, schemaErr := s.store.GetSchema(p.Name)
		if schemaErr != nil {
			continue
		}
		schemas = append(schemas, projectSchema{
			Project: p.Name,
			Schema:  schema,
		})
	}

	return jsonResult(map[string]any{
		"projects": schemas,
	}), nil
}
