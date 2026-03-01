package discover

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverBasic(t *testing.T) {
	dir := t.TempDir()

	// Create a Go file and a Python file
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "app.py"), []byte("def main(): pass\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	files, err := Discover(ctx, dir, nil)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}

	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}

	// Verify file info is populated
	for _, f := range files {
		if f.Path == "" {
			t.Error("expected non-empty Path")
		}
		if f.RelPath == "" {
			t.Error("expected non-empty RelPath")
		}
		if f.Language == "" {
			t.Error("expected non-empty Language")
		}
	}
}

func TestDiscoverCancellation(t *testing.T) {
	dir := t.TempDir()

	// Create a file so the directory isn't empty
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	_, err := Discover(ctx, dir, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
