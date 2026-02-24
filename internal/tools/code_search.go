package tools

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type codeMatch struct {
	File    string `json:"file"`
	Line    int    `json:"line"`
	Content string `json:"content"`
}

func (s *Server) handleSearchCode(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := parseArgs(req)
	if err != nil {
		return errResult(err.Error()), nil
	}

	pattern := getStringArg(args, "pattern")
	if pattern == "" {
		return errResult("pattern is required"), nil
	}

	fileGlob := getStringArg(args, "file_pattern")
	maxResults := getIntArg(args, "max_results", 50)
	if maxResults > 200 {
		maxResults = 200
	}

	isRegex := false
	if v, ok := args["regex"]; ok {
		if b, ok := v.(bool); ok {
			isRegex = b
		}
	}

	// Resolve project root
	root, err := s.resolveProjectRoot()
	if err != nil {
		return errResult(fmt.Sprintf("resolve root: %v", err)), nil
	}

	// Compile regex or prepare literal search
	var re *regexp.Regexp
	if isRegex {
		re, err = regexp.Compile(pattern)
		if err != nil {
			return errResult(fmt.Sprintf("invalid regex: %v", err)), nil
		}
	}

	// Get indexed file paths from the store
	projects, _ := s.store.ListProjects()
	var filePaths []string
	for _, p := range projects {
		files, _ := s.store.FindNodesByLabel(p.Name, "File")
		for _, f := range files {
			if f.FilePath == "" {
				continue
			}
			if fileGlob != "" {
				matched, _ := filepath.Match(fileGlob, filepath.Base(f.FilePath))
				// Also try against the full relative path using double-star simulation
				if !matched {
					matched = globMatch(fileGlob, f.FilePath)
				}
				if !matched {
					continue
				}
			}
			filePaths = append(filePaths, f.FilePath)
		}
	}

	var matches []codeMatch
	for _, relPath := range filePaths {
		if len(matches) >= maxResults {
			break
		}

		absPath := filepath.Join(root, relPath)
		fileMatches := searchFile(absPath, relPath, pattern, re, isRegex, maxResults-len(matches))
		matches = append(matches, fileMatches...)
	}

	return jsonResult(map[string]any{
		"pattern":     pattern,
		"total":       len(matches),
		"truncated":   len(matches) >= maxResults,
		"matches":     matches,
		"files_count": len(filePaths),
	}), nil
}

func searchFile(absPath, relPath, pattern string, re *regexp.Regexp, isRegex bool, limit int) []codeMatch {
	f, err := os.Open(absPath)
	if err != nil {
		return nil
	}
	defer f.Close()

	var matches []codeMatch
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		var found bool
		if isRegex {
			found = re.MatchString(line)
		} else {
			found = strings.Contains(line, pattern)
		}

		if found {
			content := strings.TrimSpace(line)
			if len(content) > 200 {
				content = content[:200] + "..."
			}
			matches = append(matches, codeMatch{
				File:    relPath,
				Line:    lineNum,
				Content: content,
			})
			if len(matches) >= limit {
				break
			}
		}
	}

	return matches
}

// globMatch does a simple glob match supporting ** patterns.
func globMatch(pattern, path string) bool {
	if strings.Contains(pattern, "**") {
		// Split pattern on **
		parts := strings.SplitN(pattern, "**", 2)
		prefix := strings.TrimRight(parts[0], "/")
		suffix := strings.TrimLeft(parts[1], "/")

		if prefix != "" && !strings.HasPrefix(path, prefix) {
			return false
		}
		if suffix != "" {
			matched, _ := filepath.Match(suffix, filepath.Base(path))
			return matched
		}
		return true
	}
	matched, _ := filepath.Match(pattern, path)
	return matched
}
