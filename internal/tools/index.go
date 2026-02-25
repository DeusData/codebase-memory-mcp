package tools

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/DeusData/codebase-memory-mcp/internal/pipeline"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *Server) handleIndexRepository(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := parseArgs(req)
	if err != nil {
		return errResult(err.Error()), nil
	}

	repoPath := getStringArg(args, "repo_path")
	if repoPath == "" {
		return errResult("repo_path is required"), nil
	}

	// Resolve to absolute path
	absPath, err := filepath.Abs(repoPath)
	if err != nil {
		return errResult(fmt.Sprintf("invalid path: %v", err)), nil
	}

	projectName := filepath.Base(absPath)

	// Lock to prevent concurrent indexing with auto-sync watcher
	s.indexMu.Lock()
	defer s.indexMu.Unlock()

	// Run the indexing pipeline
	p := pipeline.New(s.store, absPath)
	if err := p.Run(); err != nil {
		return errResult(fmt.Sprintf("indexing failed: %v", err)), nil
	}

	// Gather stats
	nodeCount, _ := s.store.CountNodes(projectName)
	edgeCount, _ := s.store.CountEdges(projectName)

	proj, _ := s.store.GetProject(projectName)
	indexedAt := store.Now()
	if proj != nil {
		indexedAt = proj.IndexedAt
	}

	return jsonResult(map[string]any{
		"project":    projectName,
		"nodes":      nodeCount,
		"edges":      edgeCount,
		"indexed_at": indexedAt,
	}), nil
}
