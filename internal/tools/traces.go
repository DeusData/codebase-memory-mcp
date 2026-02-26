package tools

import (
	"context"
	"encoding/json"

	"github.com/DeusData/codebase-memory-mcp/internal/traces"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *Server) registerTraceTools() {
	s.addTool(&mcp.Tool{
		Name:        "ingest_traces",
		Description: "Ingest OpenTelemetry JSON traces (OTLP format) to validate and enrich HTTP_CALLS edges. Matches HTTP spans to existing edges by URL path, boosts confidence by +0.15 (capped at 1.0), and sets validated_by_trace=true, trace_call_count, and p99_latency_ns on matched edges. Use after index_repository to confirm static analysis predictions with runtime data. Export traces via: otel-cli or collector with OTLP JSON exporter.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"project": {
					"type": "string",
					"description": "Name of the indexed project"
				},
				"file_path": {
					"type": "string",
					"description": "Path to the OTLP JSON export file"
				}
			},
			"required": ["project", "file_path"]
		}`),
	}, s.handleIngestTraces)
}

func (s *Server) handleIngestTraces(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := parseArgs(req)
	if err != nil {
		return errResult(err.Error()), nil
	}

	project := getStringArg(args, "project")
	filePath := getStringArg(args, "file_path")

	if project == "" || filePath == "" {
		return errResult("project and file_path are required"), nil
	}

	result, err := traces.Ingest(s.store, project, filePath)
	if err != nil {
		return errResult(err.Error()), nil
	}

	return jsonResult(result), nil
}
