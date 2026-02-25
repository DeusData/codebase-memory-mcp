package watcher

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

func TestSnapshotsEqual(t *testing.T) {
	now := time.Now()

	a := map[string]fileSnapshot{
		"main.go": {modTime: now, size: 100},
		"util.go": {modTime: now, size: 200},
	}
	b := map[string]fileSnapshot{
		"main.go": {modTime: now, size: 100},
		"util.go": {modTime: now, size: 200},
	}
	if !snapshotsEqual(a, b) {
		t.Error("identical snapshots should be equal")
	}

	// Different size
	c := map[string]fileSnapshot{
		"main.go": {modTime: now, size: 101},
		"util.go": {modTime: now, size: 200},
	}
	if snapshotsEqual(a, c) {
		t.Error("different size should not be equal")
	}

	// Different mtime
	d := map[string]fileSnapshot{
		"main.go": {modTime: now.Add(time.Second), size: 100},
		"util.go": {modTime: now, size: 200},
	}
	if snapshotsEqual(a, d) {
		t.Error("different mtime should not be equal")
	}

	// Missing file
	e := map[string]fileSnapshot{
		"main.go": {modTime: now, size: 100},
	}
	if snapshotsEqual(a, e) {
		t.Error("different file count should not be equal")
	}

	// Extra file
	f := map[string]fileSnapshot{
		"main.go": {modTime: now, size: 100},
		"util.go": {modTime: now, size: 200},
		"new.go":  {modTime: now, size: 50},
	}
	if snapshotsEqual(a, f) {
		t.Error("extra file should not be equal")
	}

	// Both empty
	if !snapshotsEqual(map[string]fileSnapshot{}, map[string]fileSnapshot{}) {
		t.Error("both empty should be equal")
	}
}

func TestPollInterval(t *testing.T) {
	tests := []struct {
		files    int
		expected time.Duration
	}{
		{0, 1 * time.Second},
		{70, 1 * time.Second},
		{499, 1 * time.Second},
		{500, 2 * time.Second},
		{2000, 5 * time.Second},
		{5000, 11 * time.Second},
		{10000, 21 * time.Second},
		{50000, 60 * time.Second},
		{100000, 60 * time.Second},
	}
	for _, tt := range tests {
		got := pollInterval(tt.files)
		if got != tt.expected {
			t.Errorf("pollInterval(%d) = %v, want %v", tt.files, got, tt.expected)
		}
	}
}

func TestCaptureSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	// Create a Go file that discover.Discover will pick up
	if err := os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	snap, err := captureSnapshot(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if len(snap) != 1 {
		t.Fatalf("expected 1 file, got %d", len(snap))
	}

	s, ok := snap["main.go"]
	if !ok {
		t.Fatal("expected main.go in snapshot")
	}
	if s.size == 0 {
		t.Error("expected non-zero size")
	}
	if s.modTime.IsZero() {
		t.Error("expected non-zero modtime")
	}
}

func TestCaptureSnapshotDetectsChanges(t *testing.T) {
	tmpDir := t.TempDir()
	goFile := filepath.Join(tmpDir, "main.go")
	if err := os.WriteFile(goFile, []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	snap1, err := captureSnapshot(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure mtime advances (some filesystems have 1s granularity)
	time.Sleep(10 * time.Millisecond)
	now := time.Now().Add(time.Second)
	if err := os.Chtimes(goFile, now, now); err != nil {
		t.Fatal(err)
	}

	snap2, err := captureSnapshot(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if snapshotsEqual(snap1, snap2) {
		t.Error("snapshots should differ after mtime change")
	}
}

func TestWatcherTriggersOnChange(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	tmpDir := t.TempDir()
	goFile := filepath.Join(tmpDir, "main.go")
	if err := os.WriteFile(goFile, []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Register the project so ListProjects returns it
	if err := s.UpsertProject(filepath.Base(tmpDir), tmpDir); err != nil {
		t.Fatal(err)
	}

	var indexCount atomic.Int32
	indexFn := func(_, _ string) error {
		indexCount.Add(1)
		return nil
	}

	w := New(s, indexFn)

	// First poll — baseline capture, no index
	w.pollAll()
	if indexCount.Load() != 0 {
		t.Errorf("first poll should not trigger index, got %d", indexCount.Load())
	}

	// Poll again without changes — no index
	// Reset nextPoll to allow immediate re-poll
	for _, state := range w.projects {
		state.nextPoll = time.Time{}
	}
	w.pollAll()
	if indexCount.Load() != 0 {
		t.Errorf("no-change poll should not trigger index, got %d", indexCount.Load())
	}

	// Modify the file
	now := time.Now().Add(time.Second)
	if err := os.Chtimes(goFile, now, now); err != nil {
		t.Fatal(err)
	}

	// Reset nextPoll and poll again — should trigger
	for _, state := range w.projects {
		state.nextPoll = time.Time{}
	}
	w.pollAll()
	if indexCount.Load() != 1 {
		t.Errorf("changed file should trigger index, got %d", indexCount.Load())
	}
}

func TestWatcherCancellation(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	w := New(s, func(_, _ string) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// OK — goroutine exited cleanly
	case <-time.After(3 * time.Second):
		t.Fatal("watcher did not stop after context cancellation")
	}
}

func TestWatcherSkipsMissingRoot(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Register a project with a non-existent path
	if err := s.UpsertProject("ghost", "/nonexistent/path"); err != nil {
		t.Fatal(err)
	}

	var indexCount atomic.Int32
	w := New(s, func(_, _ string) error {
		indexCount.Add(1)
		return nil
	})

	w.pollAll()
	if indexCount.Load() != 0 {
		t.Errorf("should not index missing root, got %d", indexCount.Load())
	}
}

func TestWatcherNewFileTriggersIndex(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	tmpDir := t.TempDir()
	goFile := filepath.Join(tmpDir, "main.go")
	if err := os.WriteFile(goFile, []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := s.UpsertProject(filepath.Base(tmpDir), tmpDir); err != nil {
		t.Fatal(err)
	}

	var indexCount atomic.Int32
	w := New(s, func(_, _ string) error {
		indexCount.Add(1)
		return nil
	})

	// Baseline
	w.pollAll()

	// Add a new file
	if err := os.WriteFile(filepath.Join(tmpDir, "util.go"), []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, state := range w.projects {
		state.nextPoll = time.Time{}
	}
	w.pollAll()
	if indexCount.Load() != 1 {
		t.Errorf("new file should trigger index, got %d", indexCount.Load())
	}
}
