package tools

import (
	"context"
	"fmt"

	"github.com/DeusData/codebase-memory-mcp/internal/cypher"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *Server) handleQueryGraph(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := parseArgs(req)
	if err != nil {
		return errResult(err.Error()), nil
	}

	query := getStringArg(args, "query")
	if query == "" {
		return errResult("missing required 'query' parameter"), nil
	}

	exec := &cypher.Executor{Store: s.store}
	result, err := exec.Execute(query)
	if err != nil {
		return errResult(fmt.Sprintf("query error: %v", err)), nil
	}

	return jsonResult(map[string]any{
		"columns": result.Columns,
		"rows":    result.Rows,
		"total":   len(result.Rows),
	}), nil
}
