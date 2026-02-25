package tools

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *Server) handleGetCodeSnippet(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := parseArgs(req)
	if err != nil {
		return errResult(err.Error()), nil
	}

	qn := getStringArg(args, "qualified_name")
	if qn == "" {
		return errResult("qualified_name is required"), nil
	}

	// Find the node across all projects
	node, project, _ := s.findNodeByQNAcrossProjects(qn)
	if node == nil {
		return errResult(fmt.Sprintf("node not found: %s", qn)), nil
	}

	if node.FilePath == "" {
		return errResult("node has no file path"), nil
	}

	if node.StartLine == 0 || node.EndLine == 0 {
		return errResult("node has no line range"), nil
	}

	// Resolve file path against the project's root path
	proj, _ := s.store.GetProject(project)
	if proj == nil {
		return errResult(fmt.Sprintf("project not found: %s", project)), nil
	}

	absPath := filepath.Join(proj.RootPath, node.FilePath)

	// Read the source file and extract lines
	source, readErr := readLines(absPath, node.StartLine, node.EndLine)
	if readErr != nil {
		return errResult(fmt.Sprintf("read file: %v", readErr)), nil
	}

	return jsonResult(map[string]any{
		"qualified_name": node.QualifiedName,
		"name":           node.Name,
		"label":          node.Label,
		"file_path":      absPath,
		"start_line":     node.StartLine,
		"end_line":       node.EndLine,
		"source":         source,
	}), nil
}

// readLines reads specific lines from a file, returning them with line numbers.
func readLines(path string, startLine, endLine int) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	var sb strings.Builder
	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		if lineNum > endLine {
			break
		}
		if lineNum >= startLine {
			fmt.Fprintf(&sb, "%4d | %s\n", lineNum, scanner.Text())
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scan: %w", err)
	}

	if sb.Len() == 0 {
		return "", fmt.Errorf("no lines found in range %d-%d (file has %d lines)", startLine, endLine, lineNum)
	}

	return sb.String(), nil
}
