package pipeline

import (
	"strings"
	"testing"
)

func TestParseNameStatusOutput(t *testing.T) {
	input := "M\tinternal/store/nodes.go\nA\tnew_file.go\nD\told_file.go\nR100\tsrc/old.go\tsrc/new.go\n"

	files := ParseNameStatusOutput(input)

	if len(files) != 4 {
		t.Fatalf("expected 4 files, got %d", len(files))
	}

	tests := []struct {
		idx     int
		status  string
		path    string
		oldPath string
	}{
		{0, "M", "internal/store/nodes.go", ""},
		{1, "A", "new_file.go", ""},
		{2, "D", "old_file.go", ""},
		{3, "R", "src/new.go", "src/old.go"},
	}

	for _, tt := range tests {
		f := files[tt.idx]
		if f.Status != tt.status {
			t.Errorf("[%d] status = %q, want %q", tt.idx, f.Status, tt.status)
		}
		if f.Path != tt.path {
			t.Errorf("[%d] path = %q, want %q", tt.idx, f.Path, tt.path)
		}
		if f.OldPath != tt.oldPath {
			t.Errorf("[%d] oldPath = %q, want %q", tt.idx, f.OldPath, tt.oldPath)
		}
	}
}

func TestParseNameStatusOutput_FiltersUntrackable(t *testing.T) {
	input := "M\tpackage-lock.json\nM\tsrc/main.go\nM\tvendor/lib.go\n"
	files := ParseNameStatusOutput(input)

	if len(files) != 1 {
		t.Fatalf("expected 1 trackable file, got %d", len(files))
	}
	if files[0].Path != "src/main.go" {
		t.Errorf("expected src/main.go, got %s", files[0].Path)
	}
}

func TestParseHunksOutput(t *testing.T) {
	input := `diff --git a/main.go b/main.go
index abc1234..def5678 100644
--- a/main.go
+++ b/main.go
@@ -10,3 +10,5 @@ func main() {
+	newLine1()
+	newLine2()
@@ -50,0 +52,2 @@ func helper() {
+	another()
+	line()
diff --git a/binary.png b/binary.png
Binary files a/binary.png and b/binary.png differ
diff --git a/utils.go b/utils.go
--- a/utils.go
+++ b/utils.go
@@ -1 +1 @@ package utils
-old
+new
`

	hunks := ParseHunksOutput(input)

	if len(hunks) != 3 {
		t.Fatalf("expected 3 hunks, got %d", len(hunks))
	}

	// First hunk: main.go @@ -10,3 +10,5 @@
	if hunks[0].Path != "main.go" {
		t.Errorf("hunk 0 path = %q", hunks[0].Path)
	}
	if hunks[0].StartLine != 10 || hunks[0].EndLine != 14 {
		t.Errorf("hunk 0 range = %d-%d, want 10-14", hunks[0].StartLine, hunks[0].EndLine)
	}

	// Second hunk: main.go @@ -50,0 +52,2 @@
	if hunks[1].Path != "main.go" {
		t.Errorf("hunk 1 path = %q", hunks[1].Path)
	}
	if hunks[1].StartLine != 52 || hunks[1].EndLine != 53 {
		t.Errorf("hunk 1 range = %d-%d, want 52-53", hunks[1].StartLine, hunks[1].EndLine)
	}

	// Third hunk: utils.go @@ -1 +1 @@
	if hunks[2].Path != "utils.go" {
		t.Errorf("hunk 2 path = %q", hunks[2].Path)
	}
	if hunks[2].StartLine != 1 || hunks[2].EndLine != 1 {
		t.Errorf("hunk 2 range = %d-%d, want 1-1", hunks[2].StartLine, hunks[2].EndLine)
	}
}

func TestParseHunksOutput_NoNewlineMarker(t *testing.T) {
	input := `diff --git a/file.go b/file.go
--- a/file.go
+++ b/file.go
@@ -5,2 +5,3 @@ func foo() {
+	bar()
\ No newline at end of file
`
	hunks := ParseHunksOutput(input)
	if len(hunks) != 1 {
		t.Fatalf("expected 1 hunk, got %d", len(hunks))
	}
	if hunks[0].StartLine != 5 || hunks[0].EndLine != 7 {
		t.Errorf("range = %d-%d, want 5-7", hunks[0].StartLine, hunks[0].EndLine)
	}
}

func TestParseRange(t *testing.T) {
	tests := []struct {
		input     string
		wantStart int
		wantCount int
	}{
		{"10,5", 10, 5},
		{"10", 10, 1},
		{"52,2", 52, 2},
		{"1,0", 1, 0},
	}
	for _, tt := range tests {
		start, count := parseRange(tt.input)
		if start != tt.wantStart || count != tt.wantCount {
			t.Errorf("parseRange(%q) = (%d, %d), want (%d, %d)", tt.input, start, count, tt.wantStart, tt.wantCount)
		}
	}
}

func TestParseHunksOutput_ModeChange(t *testing.T) {
	input := `diff --git a/script.sh b/script.sh
old mode 100644
new mode 100755
`
	hunks := ParseHunksOutput(input)
	if len(hunks) != 0 {
		t.Fatalf("expected 0 hunks for mode-only change, got %d", len(hunks))
	}
}

func TestGitNotFound(t *testing.T) {
	// Override PATH to ensure git can't be found
	t.Setenv("PATH", t.TempDir())

	_, err := runGit(t.TempDir(), []string{"status"})
	if err == nil {
		t.Fatal("expected error when git is not found")
	}
	if !strings.Contains(err.Error(), "git not found in PATH") {
		t.Errorf("expected 'git not found in PATH' error, got: %v", err)
	}
}

func TestParseHunksOutput_Deletion(t *testing.T) {
	// Deletion hunks have +start,0 — should still produce a valid hunk at start line
	input := `diff --git a/file.go b/file.go
--- a/file.go
+++ b/file.go
@@ -10,3 +10,0 @@ func foo() {
`
	hunks := ParseHunksOutput(input)
	if len(hunks) != 1 {
		t.Fatalf("expected 1 hunk, got %d", len(hunks))
	}
	// count=0, so endLine = start + 0 - 1 = 9, but we clamp to start
	if hunks[0].StartLine != 10 {
		t.Errorf("start = %d, want 10", hunks[0].StartLine)
	}
}
