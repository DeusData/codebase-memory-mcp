package discovery

import (
	"context"
)

// ToolDefinition describes the wrapper-owned discover_projects MCP tool.
type ToolDefinition struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	InputSchema map[string]interface{} `json:"inputSchema"`
}

// Candidate is a single repo candidate returned by discovery.
type Candidate struct {
	Project    string   `json:"project"`
	RepoSlug   string   `json:"repo_slug"`
	Score      float64  `json:"score,omitempty"`
	Confidence string   `json:"confidence,omitempty"`
	Reasons    []string `json:"reasons,omitempty"`
}

// Request is the discover_projects tool input.
type Request struct {
	Query                  string `json:"query"`
	Limit                  int    `json:"limit,omitempty"`
	IncludeGraphConfidence bool   `json:"include_graph_confidence,omitempty"`
	IncludeSemantic        bool   `json:"include_semantic,omitempty"`
}

// Response is the discover_projects tool output.
type Response struct {
	Query        string      `json:"query"`
	CrossRepo    bool        `json:"cross_repo,omitempty"`
	PrimaryRepos []Candidate `json:"primary_repos,omitempty"`
	RelatedRepos []Candidate `json:"related_repos,omitempty"`
}

// Service executes wrapper-owned repo discovery.
type Service interface {
	Definition() ToolDefinition
	DiscoverProjects(ctx context.Context, req Request) (Response, error)
}

// NewDefinition returns the canonical wrapper tool definition.
func NewDefinition() ToolDefinition {
	return ToolDefinition{
		Name:        "discover_projects",
		Description: "Discover the most likely indexed repos for a task using metadata, code search, and graph evidence.",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"query": map[string]interface{}{
					"type":        "string",
					"description": "Task or feature description to map to indexed repositories.",
				},
				"limit": map[string]interface{}{
					"type":        "integer",
					"default":     5,
					"description": "Maximum number of candidate repositories to return.",
				},
				"include_graph_confidence": map[string]interface{}{
					"type":        "boolean",
					"default":     true,
					"description": "When true, use graph-level architecture checks to refine confidence for top candidates.",
				},
				"include_semantic": map[string]interface{}{
					"type":        "boolean",
					"default":     false,
					"description": "When true, optionally use semantic vector hits where available as positive evidence.",
				},
			},
			"required": []string{"query"},
		},
	}
}
