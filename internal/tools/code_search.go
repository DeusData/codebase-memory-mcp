package tools

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type codeMatch struct {
	File    string `json:"file"`
	Line    int    `json:"line"`
	Content string `json:"content"`
}

// searchCodeParams holds parsed parameters for a code search request.
type searchCodeParams struct {
	pattern    string
	fileGlob   string
	maxResults int
	offset     int
	isRegex    bool
	re         *regexp.Regexp
	project    string
}

// parseSearchCodeParams extracts and validates search_code parameters from the request.
func parseSearchCodeParams(req *mcp.CallToolRequest) (*searchCodeParams, *mcp.CallToolResult) {
	args, err := parseArgs(req)
	if err != nil {
		return nil, errResult(err.Error())
	}

	p := &searchCodeParams{
		pattern:    getStringArg(args, "pattern"),
		fileGlob:   getStringArg(args, "file_pattern"),
		maxResults: getIntArg(args, "max_results", 10),
		offset:     getIntArg(args, "offset", 0),
		isRegex:    getBoolArg(args, "regex"),
		project:    getStringArg(args, "project"),
	}

	if p.pattern == "" {
		return nil, errResult("pattern is required")
	}

	if p.isRegex {
		p.re, err = regexp.Compile(p.pattern)
		if err != nil {
			return nil, errResult(fmt.Sprintf("invalid regex: %v", err))
		}
	}

	return p, nil
}

func (s *Server) handleSearchCode(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	params, errRes := parseSearchCodeParams(req)
	if errRes != nil {
		return errRes, nil
	}

	// Resolve project root
	root, err := s.resolveProjectRoot(params.project)
	if err != nil {
		return errResult(fmt.Sprintf("resolve root: %v", err)), nil
	}

	filePaths := s.collectSearchFilePaths(params.fileGlob, params.project)

	// Collect all matches up to offset+maxResults for accurate total count
	fetchLimit := params.offset + params.maxResults
	var allMatches []codeMatch
	for _, relPath := range filePaths {
		if len(allMatches) >= fetchLimit {
			break
		}

		absPath := filepath.Join(root, relPath)
		fileMatches := searchFile(absPath, relPath, params.pattern, params.re, params.isRegex, fetchLimit-len(allMatches))
		allMatches = append(allMatches, fileMatches...)
	}

	total := len(allMatches)
	hasMore := total >= fetchLimit

	// Apply offset and limit
	start := params.offset
	if start > total {
		start = total
	}
	end := start + params.maxResults
	if end > total {
		end = total
	}
	pageMatches := allMatches[start:end]

	responseData := map[string]any{
		"pattern":     params.pattern,
		"total":       total,
		"limit":       params.maxResults,
		"offset":      params.offset,
		"has_more":    hasMore,
		"matches":     pageMatches,
		"files_count": len(filePaths),
	}
	s.addIndexStatus(responseData)

	result := jsonResult(responseData)
	s.addUpdateNotice(result)
	return result, nil
}

// collectSearchFilePaths gathers indexed file paths, optionally filtered by a glob pattern.
func (s *Server) collectSearchFilePaths(fileGlob, project string) []string {
	var filePaths []string

	collectFromStore := func(st *store.Store, projName string) {
		files, _ := st.FindNodesByLabel(projName, "File")
		for _, f := range files {
			if f.FilePath == "" {
				continue
			}
			if fileGlob != "" {
				matched, _ := filepath.Match(fileGlob, filepath.Base(f.FilePath))
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

	st, err := s.resolveStore(project)
	if err != nil {
		return filePaths
	}

	projName := s.resolveProjectName(project)
	projects, _ := st.ListProjects()
	if len(projects) > 0 {
		projName = projects[0].Name
	}
	collectFromStore(st, projName)

	return filePaths
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
