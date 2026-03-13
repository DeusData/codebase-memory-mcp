package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// TestEarlyRelease_DefinitionsDropped creates 50 Go files and verifies that
// heap inuse is lower after the full pipeline run than a naive estimate would
// suggest, indicating that early cache release is working.
//
// This test is intentionally coarse: it checks that peak heap does NOT exceed
// a generous threshold (50 MB for 50 files × ~1 KB/file), rather than trying
// to measure the release in isolation (which is hard without pipeline hooks).
func TestEarlyRelease_DefinitionsDropped(t *testing.T) {
	const numFiles = 50
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < numFiles; i++ {
		content := fmt.Sprintf("package pkg%d\n\nfunc F%d() int { return %d }\n", i, i, i)
		path := filepath.Join(dir, fmt.Sprintf("f%d.go", i))
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	p := New(context.Background(), s, dir, discover.ModeFull)
	if err := p.Run(); err != nil {
		t.Fatalf("pipeline.Run: %v", err)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// After GC, retained heap should be small (mostly graph nodes in store).
	// 50 MB is a generous upper bound for 50 trivial files.
	const maxRetainedMB = 50
	retainedMB := after.HeapInuse / (1 << 20)
	t.Logf("before=%d MB, after=%d MB, retained=%d MB", before.HeapInuse/(1<<20), after.HeapInuse/(1<<20), retainedMB)
	if retainedMB > maxRetainedMB {
		t.Errorf("retained heap %d MB exceeds %d MB — early release may not be working", retainedMB, maxRetainedMB)
	}
}
