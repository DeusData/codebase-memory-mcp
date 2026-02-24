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

func (s *Server) handleReadFile(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := parseArgs(req)
	if err != nil {
		return errResult(err.Error()), nil
	}

	filePath := getStringArg(args, "path")
	if filePath == "" {
		return errResult("path is required"), nil
	}

	startLine := getIntArg(args, "start_line", 0)
	endLine := getIntArg(args, "end_line", 0)

	// Resolve relative path against project root
	absPath := filePath
	if !filepath.IsAbs(filePath) {
		root, rootErr := s.resolveProjectRoot()
		if rootErr != nil {
			return errResult(fmt.Sprintf("resolve root: %v", rootErr)), nil
		}
		absPath = filepath.Join(root, filePath)
	}

	// Check file exists and is not a directory
	info, err := os.Stat(absPath)
	if err != nil {
		return errResult(fmt.Sprintf("file not found: %s", absPath)), nil
	}
	if info.IsDir() {
		return errResult("path is a directory, use list_directory instead"), nil
	}

	// Cap file size at 500KB
	if info.Size() > 500*1024 {
		return errResult(fmt.Sprintf("file too large (%d bytes, max 500KB). Use start_line/end_line to read a portion", info.Size())), nil
	}

	f, err := os.Open(absPath)
	if err != nil {
		return errResult(fmt.Sprintf("open: %v", err)), nil
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 1MB line buffer
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		if startLine > 0 && lineNum < startLine {
			continue
		}
		if endLine > 0 && lineNum > endLine {
			break
		}
		line := scanner.Text()
		if len(line) > 500 {
			line = line[:500] + "..."
		}
		lines = append(lines, fmt.Sprintf("%4d | %s", lineNum, line))
	}

	if err := scanner.Err(); err != nil {
		return errResult(fmt.Sprintf("read: %v", err)), nil
	}

	result := map[string]any{
		"path":        absPath,
		"total_lines": lineNum,
		"content":     strings.Join(lines, "\n"),
	}
	if startLine > 0 || endLine > 0 {
		result["range"] = fmt.Sprintf("%d-%d", startLine, endLine)
	}

	return jsonResult(result), nil
}

func (s *Server) handleListDirectory(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := parseArgs(req)
	if err != nil {
		return errResult(err.Error()), nil
	}

	dirPath := getStringArg(args, "path")
	globPattern := getStringArg(args, "pattern")

	// Resolve relative path against project root
	absPath := dirPath
	if dirPath == "" || !filepath.IsAbs(dirPath) {
		root, rootErr := s.resolveProjectRoot()
		if rootErr != nil {
			return errResult(fmt.Sprintf("resolve root: %v", rootErr)), nil
		}
		if dirPath == "" {
			absPath = root
		} else {
			absPath = filepath.Join(root, dirPath)
		}
	}

	info, err := os.Stat(absPath)
	if err != nil {
		return errResult(fmt.Sprintf("path not found: %s", absPath)), nil
	}
	if !info.IsDir() {
		return errResult("path is a file, use read_file instead"), nil
	}

	type entry struct {
		Name  string `json:"name"`
		Path  string `json:"path"`
		IsDir bool   `json:"is_dir"`
		Size  int64  `json:"size,omitempty"`
	}

	var entries []entry

	if globPattern != "" {
		// Glob matching within the directory
		matches, globErr := filepath.Glob(filepath.Join(absPath, globPattern))
		if globErr != nil {
			return errResult(fmt.Sprintf("glob: %v", globErr)), nil
		}
		for _, m := range matches {
			fi, statErr := os.Stat(m)
			if statErr != nil {
				continue
			}
			relPath, _ := filepath.Rel(absPath, m)
			e := entry{
				Name:  relPath,
				Path:  m,
				IsDir: fi.IsDir(),
			}
			if !fi.IsDir() {
				e.Size = fi.Size()
			}
			entries = append(entries, e)
		}
	} else {
		// List immediate children
		dirEntries, readErr := os.ReadDir(absPath)
		if readErr != nil {
			return errResult(fmt.Sprintf("read dir: %v", readErr)), nil
		}
		for _, de := range dirEntries {
			// Skip hidden files/dirs
			if strings.HasPrefix(de.Name(), ".") {
				continue
			}
			fi, statErr := de.Info()
			if statErr != nil {
				continue
			}
			e := entry{
				Name:  de.Name(),
				Path:  filepath.Join(absPath, de.Name()),
				IsDir: de.IsDir(),
			}
			if !de.IsDir() {
				e.Size = fi.Size()
			}
			entries = append(entries, e)
		}
	}

	return jsonResult(map[string]any{
		"directory": absPath,
		"count":     len(entries),
		"entries":   entries,
	}), nil
}

// resolveProjectRoot finds the first indexed project root path.
func (s *Server) resolveProjectRoot() (string, error) {
	projects, err := s.store.ListProjects()
	if err != nil {
		return "", err
	}
	if len(projects) == 0 {
		return "", fmt.Errorf("no projects indexed")
	}
	return projects[0].RootPath, nil
}
