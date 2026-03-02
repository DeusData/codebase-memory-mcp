package pipeline

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// DiffScope controls which changes to include.
type DiffScope string

const (
	DiffUnstaged DiffScope = "unstaged"
	DiffStaged   DiffScope = "staged"
	DiffAll      DiffScope = "all"
	DiffBranch   DiffScope = "branch"
)

// ChangedFile represents a file with a status from git diff --name-status.
type ChangedFile struct {
	Status  string // M, A, D, R (modified, added, deleted, renamed)
	Path    string
	OldPath string // non-empty only for renames
}

// ChangedHunk represents a changed region within a file.
type ChangedHunk struct {
	Path      string
	StartLine int
	EndLine   int
}

// ParseGitDiffFiles runs git diff --name-status and returns changed files.
func ParseGitDiffFiles(repoPath string, scope DiffScope, baseBranch string) ([]ChangedFile, error) {
	args := buildDiffArgs(scope, baseBranch)
	args = append(args, "--name-status")
	return parseDiffNameStatus(repoPath, args)
}

// ParseGitDiffHunks runs git diff --unified=0 and extracts changed line ranges.
func ParseGitDiffHunks(repoPath string, scope DiffScope, baseBranch string) ([]ChangedHunk, error) {
	args := buildDiffArgs(scope, baseBranch)
	args = append(args, "--unified=0")
	return parseDiffHunks(repoPath, args)
}

func buildDiffArgs(scope DiffScope, baseBranch string) []string {
	base := []string{"diff"}
	switch scope {
	case DiffStaged:
		return append(base, "--cached")
	case DiffAll:
		return append(base, "HEAD")
	case DiffBranch:
		if baseBranch == "" {
			baseBranch = "main"
		}
		return append(base, baseBranch+"...HEAD")
	default: // unstaged
		return base
	}
}

func parseDiffNameStatus(repoPath string, args []string) ([]ChangedFile, error) {
	output, err := runGit(repoPath, args)
	if err != nil {
		return nil, err
	}
	return ParseNameStatusOutput(output), nil
}

// ParseNameStatusOutput parses the raw output of git diff --name-status.
func ParseNameStatusOutput(output string) []ChangedFile {
	var files []ChangedFile
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			continue
		}
		status := parts[0]
		path := parts[1]

		cf := ChangedFile{Path: path}

		// Handle renames: R100\told\tnew
		if strings.HasPrefix(status, "R") {
			cf.Status = "R"
			cf.OldPath = path
			if len(parts) >= 3 {
				cf.Path = parts[2]
			}
		} else {
			cf.Status = status[:1] // first char: M, A, D, etc.
		}

		if !isTrackableFile(cf.Path) {
			continue
		}
		files = append(files, cf)
	}
	return files
}

func parseDiffHunks(repoPath string, args []string) ([]ChangedHunk, error) {
	output, err := runGit(repoPath, args)
	if err != nil {
		return nil, err
	}
	return ParseHunksOutput(output), nil
}

// ParseHunksOutput parses the raw output of git diff --unified=0.
func ParseHunksOutput(output string) []ChangedHunk {
	var hunks []ChangedHunk
	var currentFile string

	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()

		// Track current file from +++ header
		if strings.HasPrefix(line, "+++ b/") {
			currentFile = strings.TrimPrefix(line, "+++ b/")
			continue
		}

		// Skip binary files
		if strings.HasPrefix(line, "Binary files") {
			continue
		}

		// Parse @@ hunk headers: @@ -old,count +new,count @@
		if strings.HasPrefix(line, "@@") && currentFile != "" {
			hunk := parseHunkHeader(line, currentFile)
			if hunk != nil && isTrackableFile(currentFile) {
				hunks = append(hunks, *hunk)
			}
		}
	}
	return hunks
}

// parseHunkHeader extracts the new-file line range from a @@ header.
// Format: @@ -old_start[,old_count] +new_start[,new_count] @@
func parseHunkHeader(line, file string) *ChangedHunk {
	// Find the +start,count part
	plusIdx := strings.Index(line, "+")
	if plusIdx < 0 {
		return nil
	}
	// Find the end @@
	endIdx := strings.Index(line[plusIdx:], " @@")
	if endIdx < 0 {
		endIdx = len(line) - plusIdx
	}
	rangeStr := line[plusIdx+1 : plusIdx+endIdx]

	start, count := parseRange(rangeStr)
	if start == 0 {
		return nil
	}
	end := start + count - 1
	if end < start {
		end = start
	}

	return &ChangedHunk{
		Path:      file,
		StartLine: start,
		EndLine:   end,
	}
}

// parseRange parses "start,count" or "start" into (start, count).
func parseRange(s string) (start, count int) {
	parts := strings.SplitN(s, ",", 2)
	start, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0
	}
	count = 1
	if len(parts) == 2 {
		count, err = strconv.Atoi(parts[1])
		if err != nil {
			count = 1
		}
	}
	return start, count
}

// runGit executes a git command and returns stdout. Returns a clear error if git is not found.
func runGit(repoPath string, args []string) (string, error) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		return "", fmt.Errorf("git not found in PATH: install git to use detect_changes")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, gitPath, args...)
	cmd.Dir = repoPath

	output, err := cmd.Output()
	if err != nil {
		// git diff returns exit code 1 when there are differences in some modes
		// but we still want the output
		if exitErr, ok := err.(*exec.ExitError); ok {
			slog.Debug("git.exit", "code", exitErr.ExitCode(), "args", args)
			return string(output), nil
		}
		return "", fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return string(output), nil
}
