package pipeline

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

func TestPassUsagesCreatesEdges(t *testing.T) {
	// Create a Go file that defines two functions, where one references
	// the other as a variable (callback) rather than calling it.
	goSource := `package mypkg

func Process(data string) string {
	return data
}

func Register() {
	handler := Process
	_ = handler
}
`
	tmpDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmpDir, "mypkg"), 0o755); err != nil {
		t.Fatal(err)
	}
	goPath := filepath.Join(tmpDir, "mypkg", "main.go")
	if err := os.WriteFile(goPath, []byte(goSource), 0o600); err != nil {
		t.Fatal(err)
	}
	// Need go.mod for discover
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module testmod\ngo 1.21\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(s, tmpDir)
	if err := p.Run(); err != nil {
		t.Fatal(err)
	}

	// Look for USAGE edges
	edges, err := s.FindEdgesByType(p.ProjectName, "USAGE")
	if err != nil {
		t.Fatal(err)
	}

	// Register should have a USAGE edge to Process (identifier reference, not a call)
	found := false
	for _, e := range edges {
		src, _ := s.FindNodeByID(e.SourceID)
		tgt, _ := s.FindNodeByID(e.TargetID)
		if src != nil && tgt != nil {
			if src.Name == "Register" && tgt.Name == "Process" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected USAGE edge from Register to Process (callback reference)")
		for _, e := range edges {
			src, _ := s.FindNodeByID(e.SourceID)
			tgt, _ := s.FindNodeByID(e.TargetID)
			if src != nil && tgt != nil {
				t.Logf("  USAGE: %s -> %s", src.Name, tgt.Name)
			}
		}
	}
}

func TestPassUsagesDoesNotDuplicateCalls(t *testing.T) {
	// When a function is called (not just referenced), only a CALLS edge
	// should exist, not a USAGE edge for the call expression.
	goSource := `package mypkg

func Helper() string {
	return "ok"
}

func Main() {
	Helper()
}
`
	tmpDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmpDir, "mypkg"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "mypkg", "main.go"), []byte(goSource), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module testmod\ngo 1.21\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(s, tmpDir)
	if err := p.Run(); err != nil {
		t.Fatal(err)
	}

	// Should have CALLS edge from Main to Helper
	callEdges, _ := s.FindEdgesByType(p.ProjectName, "CALLS")
	foundCall := false
	for _, e := range callEdges {
		src, _ := s.FindNodeByID(e.SourceID)
		tgt, _ := s.FindNodeByID(e.TargetID)
		if src != nil && tgt != nil && src.Name == "Main" && tgt.Name == "Helper" {
			foundCall = true
		}
	}
	if !foundCall {
		t.Error("expected CALLS edge from Main to Helper")
	}

	// Should NOT have USAGE edge from Main to Helper (it's a call, not a reference)
	usageEdges, _ := s.FindEdgesByType(p.ProjectName, "USAGE")
	for _, e := range usageEdges {
		src, _ := s.FindNodeByID(e.SourceID)
		tgt, _ := s.FindNodeByID(e.TargetID)
		if src != nil && tgt != nil && src.Name == "Main" && tgt.Name == "Helper" {
			t.Error("should NOT have USAGE edge from Main to Helper — it's a call, not a reference")
		}
	}
}

func TestIsKeywordOrBuiltin(t *testing.T) {
	tests := []struct {
		name     string
		language string
		want     bool
	}{
		{"if", "go", true},
		{"self", "python", true},
		{"nil", "go", true},
		{"make", "go", true},
		{"print", "python", true},
		{"MyFunction", "go", false},
		{"processOrder", "python", false},
		{"x", "go", true}, // single char
	}
	for _, tt := range tests {
		var l lang.Language
		switch tt.language {
		case "go":
			l = lang.Go
		case "python":
			l = lang.Python
		}
		got := isKeywordOrBuiltin(tt.name, l)
		if got != tt.want {
			t.Errorf("isKeywordOrBuiltin(%q, %s) = %v, want %v", tt.name, tt.language, got, tt.want)
		}
	}
}
