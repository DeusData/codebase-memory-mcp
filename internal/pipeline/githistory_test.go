package pipeline

import (
	"fmt"
	"testing"
)

func TestIsTrackableFile(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"main.go", true},
		{"src/app.py", true},
		{"node_modules/foo/bar.js", false},
		{"vendor/lib/dep.go", false},
		{"package-lock.json", false},
		{"go.sum", false},
		{"image.png", false},
		{".git/config", false},
		{"__pycache__/mod.pyc", false},
		{"src/style.min.css", false},
		{"README.md", true},
	}
	for _, tt := range tests {
		if got := isTrackableFile(tt.path); got != tt.want {
			t.Errorf("isTrackableFile(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestComputeChangeCoupling(t *testing.T) {
	commits := []CommitFiles{
		{Hash: "aaa", Files: []string{"a.go", "b.go", "c.go"}},
		{Hash: "bbb", Files: []string{"a.go", "b.go"}},
		{Hash: "ccc", Files: []string{"a.go", "b.go"}},
		{Hash: "ddd", Files: []string{"a.go", "c.go"}},
		{Hash: "eee", Files: []string{"d.go", "e.go"}},
	}

	couplings := computeChangeCoupling(commits)

	found := false
	for _, c := range couplings {
		if (c.FileA == "a.go" && c.FileB == "b.go") || (c.FileA == "b.go" && c.FileB == "a.go") {
			found = true
			if c.CoChangeCount != 3 {
				t.Errorf("expected 3 co-changes for a.go/b.go, got %d", c.CoChangeCount)
			}
			if c.CouplingScore < 0.9 {
				t.Errorf("expected high coupling score, got %f", c.CouplingScore)
			}
		}
	}
	if !found {
		t.Error("expected coupling between a.go and b.go")
	}

	for _, c := range couplings {
		if c.FileA == "d.go" || c.FileB == "d.go" {
			t.Error("d.go should not appear (below threshold)")
		}
	}
}

func TestComputeChangeCouplingSkipsLargeCommits(t *testing.T) {
	files := make([]string, 25)
	for i := range files {
		files[i] = fmt.Sprintf("file%d.go", i)
	}
	commits := []CommitFiles{{Hash: "large", Files: files}}

	couplings := computeChangeCoupling(commits)
	if len(couplings) != 0 {
		t.Errorf("expected 0 couplings from large commit, got %d", len(couplings))
	}
}

func TestComputeChangeCouplingLimitsTo100(t *testing.T) {
	// Create many commits with overlapping files to generate >100 couplings
	var commits []CommitFiles
	for i := 0; i < 50; i++ {
		for j := i + 1; j < 50; j++ {
			// Create 3 commits per pair to exceed threshold
			for k := 0; k < 3; k++ {
				commits = append(commits, CommitFiles{
					Hash:  fmt.Sprintf("c%d_%d_%d", i, j, k),
					Files: []string{fmt.Sprintf("f%d.go", i), fmt.Sprintf("f%d.go", j)},
				})
			}
		}
	}

	couplings := computeChangeCoupling(commits)
	if len(couplings) > 100 {
		t.Errorf("expected max 100 couplings, got %d", len(couplings))
	}
}
