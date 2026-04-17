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

func TestSyncer_PersistOrgGraph(t *testing.T) {
	runtimeDir := t.TempDir()
	artifactDir := t.TempDir()

	s, err := New(runtimeDir, artifactDir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Create org.db in runtime dir under org/ subdir
	orgDir := filepath.Join(runtimeDir, "org")
	if err := os.MkdirAll(orgDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeFile(t, filepath.Join(orgDir, "org.db"), "org data")

	n, err := s.PersistOrgGraph()
	if err != nil {
		t.Fatalf("PersistOrgGraph: %v", err)
	}
	if n != 1 {
		t.Errorf("persisted: got %d, want 1", n)
	}

	// Verify file exists in artifact dir under org/ subdir
	dst := filepath.Join(artifactDir, "org", "org.db")
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		t.Errorf("expected %s to exist", dst)
	}
}

func TestSyncer_HydrateOrgGraph(t *testing.T) {
	runtimeDir := t.TempDir()
	artifactDir := t.TempDir()

	// Create org.db in artifact dir under org/ subdir
	orgDir := filepath.Join(artifactDir, "org")
	if err := os.MkdirAll(orgDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeFile(t, filepath.Join(orgDir, "org.db"), "org data")

	s, err := New(runtimeDir, artifactDir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	n, err := s.HydrateOrgGraph()
	if err != nil {
		t.Fatalf("HydrateOrgGraph: %v", err)
	}
	if n != 1 {
		t.Errorf("hydrated: got %d, want 1", n)
	}

	dst := filepath.Join(runtimeDir, "org", "org.db")
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		t.Errorf("expected %s to exist", dst)
	}
}

func TestSyncer_PersistOrgGraph_NoOrgDir(t *testing.T) {
	runtimeDir := t.TempDir()
	artifactDir := t.TempDir()
	s, err := New(runtimeDir, artifactDir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// No org/ dir exists — should return 0, nil
	n, err := s.PersistOrgGraph()
	if err != nil {
		t.Fatalf("PersistOrgGraph: %v", err)
	}
	if n != 0 {
		t.Errorf("persisted: got %d, want 0", n)
	}
}

func TestSyncer_HydrateOrgGraph_NoArtifact(t *testing.T) {
	runtimeDir := t.TempDir()
	artifactDir := t.TempDir()
	s, err := New(runtimeDir, artifactDir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// No org/ dir in artifact — should return 0, nil
	n, err := s.HydrateOrgGraph()
	if err != nil {
		t.Fatalf("HydrateOrgGraph: %v", err)
	}
	if n != 0 {
		t.Errorf("hydrated: got %d, want 0", n)
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
