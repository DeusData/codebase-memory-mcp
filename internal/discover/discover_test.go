package discover

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
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

func TestDisambiguateM(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name    string
		content string
		want    lang.Language
	}{
		{"objc", "@interface Foo\n@end\n", lang.ObjectiveC},
		{"matlab_function", "function y = foo(x)\n  y = x^2;\nend\n", lang.MATLAB},
		{"matlab_comment", "% This is MATLAB\nx = 1;\n", lang.MATLAB},
		{"magma_end_function", "function Fact(n)\n  return n;\nend function;\n", lang.Magma},
		{"magma_procedure", "procedure DoStuff(~x)\n  x := 1;\nend procedure;\n", lang.Magma},
		{"magma_intrinsic", "intrinsic IsSmall(x :: RngIntElt) -> BoolElt\n", lang.Magma},
		{"magma_end_if", "if n le 1 then\n  return 1;\nend if;\n", lang.Magma},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(dir, tt.name+".m")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatal(err)
			}
			got, ok := disambiguateM(path)
			if !ok {
				t.Fatal("disambiguateM returned ok=false")
			}
			if got != tt.want {
				t.Errorf("disambiguateM(%s) = %s, want %s", tt.name, got, tt.want)
			}
		})
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
