package cachepersist

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHydrateCopiesDBArtifactsOnly(t *testing.T) {
	artifactDir := t.TempDir()
	runtimeDir := t.TempDir()

	writeFile(t, filepath.Join(artifactDir, "platform-backend.db"), "db")
	writeFile(t, filepath.Join(artifactDir, "platform-backend.db-wal"), "wal")
	writeFile(t, filepath.Join(artifactDir, "platform-backend.db-shm"), "shm")
	writeFile(t, filepath.Join(artifactDir, "README.txt"), "ignore")

	syncer, err := New(runtimeDir, artifactDir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	copied, err := syncer.Hydrate()
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}
	if copied != 1 {
		t.Fatalf("copied: want 1, got %d", copied)
	}
	if _, err := os.Stat(filepath.Join(runtimeDir, "platform-backend.db")); err != nil {
		t.Fatalf("runtime db missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runtimeDir, "platform-backend.db-wal")); !os.IsNotExist(err) {
		t.Fatalf("unexpected wal copied: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runtimeDir, "platform-backend.db-shm")); !os.IsNotExist(err) {
		t.Fatalf("unexpected shm copied: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runtimeDir, "README.txt")); !os.IsNotExist(err) {
		t.Fatalf("unexpected non-db file copied: %v", err)
	}
}

func TestPersistProjectCopiesMatchingArtifacts(t *testing.T) {
	artifactDir := t.TempDir()
	runtimeDir := t.TempDir()

	writeFile(t, filepath.Join(runtimeDir, "platform-backend.db"), "db")
	writeFile(t, filepath.Join(runtimeDir, "platform-backend.db-wal"), "wal")
	writeFile(t, filepath.Join(runtimeDir, "platform-backend.db-shm"), "shm")
	writeFile(t, filepath.Join(runtimeDir, "other.db"), "other")

	syncer, err := New(runtimeDir, artifactDir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	copied, err := syncer.PersistProject("platform-backend")
	if err != nil {
		t.Fatalf("PersistProject: %v", err)
	}
	if copied != 1 {
		t.Fatalf("copied: want 1, got %d", copied)
	}
	if _, err := os.Stat(filepath.Join(artifactDir, "platform-backend.db")); err != nil {
		t.Fatalf("artifact db missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(artifactDir, "platform-backend.db-wal")); !os.IsNotExist(err) {
		t.Fatalf("unexpected wal artifact copied: %v", err)
	}
	if _, err := os.Stat(filepath.Join(artifactDir, "platform-backend.db-shm")); !os.IsNotExist(err) {
		t.Fatalf("unexpected shm artifact copied: %v", err)
	}
	if _, err := os.Stat(filepath.Join(artifactDir, "other.db")); !os.IsNotExist(err) {
		t.Fatalf("unexpected unrelated artifact copied: %v", err)
	}
}

func TestCountArtifacts(t *testing.T) {
	artifactDir := t.TempDir()
	runtimeDir := t.TempDir()

	writeFile(t, filepath.Join(artifactDir, "a.db"), "a")
	writeFile(t, filepath.Join(artifactDir, "a.db-wal"), "wal")
	writeFile(t, filepath.Join(artifactDir, "a.db-shm"), "shm")
	writeFile(t, filepath.Join(artifactDir, "notes.md"), "ignore")

	syncer, err := New(runtimeDir, artifactDir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	count, err := syncer.CountArtifacts()
	if err != nil {
		t.Fatalf("CountArtifacts: %v", err)
	}
	if count != 1 {
		t.Fatalf("count: want 1, got %d", count)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
		t.Fatalf("write file: %v", err)
	}
}
