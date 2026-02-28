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
		repoPath = s.sessionRoot // auto-detected from session
	}
	if repoPath == "" {
		return errResult("repo_path is required (no session root detected)"), nil
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

	// Get per-project store
	st, err := s.router.ForProject(projectName)
	if err != nil {
		return errResult(fmt.Sprintf("store: %v", err)), nil
	}

	// Run the indexing pipeline
	p := pipeline.New(st, absPath)
	if err := p.Run(); err != nil {
		return errResult(fmt.Sprintf("indexing failed: %v", err)), nil
	}

	// Update session state if this is the session project
	if projectName == s.sessionProject {
		s.indexStatus.Store("ready")
	}

	// Gather stats
	nodeCount, _ := st.CountNodes(projectName)
	edgeCount, _ := st.CountEdges(projectName)

	proj, _ := st.GetProject(projectName)
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
