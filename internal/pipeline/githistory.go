package pipeline

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// CommitFiles holds the files changed in a single commit.
type CommitFiles struct {
	Hash  string
	Files []string
}

// ChangeCoupling represents a pair of files that change together.
type ChangeCoupling struct {
	FileA         string
	FileB         string
	CoChangeCount int
	TotalChangesA int
	TotalChangesB int
	CouplingScore float64
}

// passGitHistory analyzes git log to find change coupling and creates FILE_CHANGES_WITH edges.
func (p *Pipeline) passGitHistory() {
	slog.Info("pass6.githistory")

	commits, err := parseGitLog(p.RepoPath)
	if err != nil {
		slog.Warn("pass6.githistory.err", "err", err)
		return
	}

	if len(commits) == 0 {
		slog.Info("pass6.githistory.skip", "reason", "no_commits")
		return
	}

	couplings := computeChangeCoupling(commits)
	edgeCount := p.createCouplingEdges(couplings)

	slog.Info("pass6.githistory.done", "commits", len(commits), "couplings", len(couplings), "edges", edgeCount)
}

// parseGitLog runs git log and extracts commit → files mapping.
func parseGitLog(repoPath string) ([]CommitFiles, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "log", "--name-only", "--pretty=format:COMMIT:%H", "--since=6 months ago")
	cmd.Dir = repoPath

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git log: %w", err)
	}

	var commits []CommitFiles
	var current *CommitFiles

	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "COMMIT:") {
			if current != nil && len(current.Files) > 0 {
				commits = append(commits, *current)
			}
			current = &CommitFiles{Hash: strings.TrimPrefix(line, "COMMIT:")}
			continue
		}
		if current != nil && isTrackableFile(line) {
			current.Files = append(current.Files, line)
		}
	}
	if current != nil && len(current.Files) > 0 {
		commits = append(commits, *current)
	}

	return commits, nil
}

// isTrackableFile returns true if the file is a source file worth tracking.
func isTrackableFile(path string) bool {
	skipPrefixes := []string{".git/", "node_modules/", "vendor/", "__pycache__/", ".cache/"}
	for _, prefix := range skipPrefixes {
		if strings.HasPrefix(path, prefix) {
			return false
		}
	}
	// Exact lock file names
	skipNames := []string{"package-lock.json", "yarn.lock", "pnpm-lock.yaml", "Cargo.lock", "poetry.lock", "composer.lock", "Gemfile.lock", "Pipfile.lock"}
	base := path[strings.LastIndex(path, "/")+1:]
	for _, name := range skipNames {
		if base == name {
			return false
		}
	}
	skipSuffixes := []string{".lock", ".sum", ".min.js", ".min.css", ".map", ".wasm", ".png", ".jpg", ".gif", ".ico", ".svg"}
	for _, suffix := range skipSuffixes {
		if strings.HasSuffix(path, suffix) {
			return false
		}
	}
	return true
}

// computeChangeCoupling computes which file pairs change together frequently.
func computeChangeCoupling(commits []CommitFiles) []ChangeCoupling {
	fileChangeCount := make(map[string]int)
	pairCount := make(map[[2]string]int)

	for _, commit := range commits {
		files := commit.Files
		if len(files) > 20 {
			continue // skip large merge/refactor commits
		}

		for _, f := range files {
			fileChangeCount[f]++
		}

		for i := 0; i < len(files); i++ {
			for j := i + 1; j < len(files); j++ {
				a, b := files[i], files[j]
				if a > b {
					a, b = b, a
				}
				pairCount[[2]string{a, b}]++
			}
		}
	}

	couplings := make([]ChangeCoupling, 0, len(pairCount)/2)
	for pair, count := range pairCount {
		if count < 3 {
			continue
		}

		totalA := fileChangeCount[pair[0]]
		totalB := fileChangeCount[pair[1]]
		minTotal := totalA
		if totalB < minTotal {
			minTotal = totalB
		}
		if minTotal == 0 {
			continue
		}

		score := float64(count) / float64(minTotal)
		if score < 0.3 {
			continue
		}

		couplings = append(couplings, ChangeCoupling{
			FileA:         pair[0],
			FileB:         pair[1],
			CoChangeCount: count,
			TotalChangesA: totalA,
			TotalChangesB: totalB,
			CouplingScore: score,
		})
	}

	sort.Slice(couplings, func(i, j int) bool {
		return couplings[i].CouplingScore > couplings[j].CouplingScore
	})

	if len(couplings) > 100 {
		couplings = couplings[:100]
	}

	return couplings
}

// createCouplingEdges creates FILE_CHANGES_WITH edges for detected change couplings.
func (p *Pipeline) createCouplingEdges(couplings []ChangeCoupling) int {
	count := 0
	for _, c := range couplings {
		nodeA := p.findFileNode(c.FileA)
		nodeB := p.findFileNode(c.FileB)
		if nodeA == nil || nodeB == nil {
			continue
		}

		_, err := p.Store.InsertEdge(&store.Edge{
			Project:  p.ProjectName,
			SourceID: nodeA.ID,
			TargetID: nodeB.ID,
			Type:     "FILE_CHANGES_WITH",
			Properties: map[string]any{
				"co_change_count": c.CoChangeCount,
				"total_changes_a": c.TotalChangesA,
				"total_changes_b": c.TotalChangesB,
				"coupling_score":  c.CouplingScore,
			},
		})
		if err == nil {
			count++
		}
	}
	return count
}

// findFileNode looks up the File node for a given relative path.
func (p *Pipeline) findFileNode(relPath string) *store.Node {
	nodes, err := p.Store.FindNodesByFile(p.ProjectName, relPath)
	if err != nil || len(nodes) == 0 {
		return nil
	}
	for _, n := range nodes {
		if n.Label == "File" {
			return n
		}
	}
	return nodes[0]
}
