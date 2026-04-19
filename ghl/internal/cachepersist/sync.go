package cachepersist

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type backend interface {
	Hydrate(runtimeDir string) (int, error)
	PersistProject(runtimeDir, project string) (int, error)
	PersistOrgDB(runtimeDir string) (int, error)
	HydrateOrgDB(runtimeDir string) (int, error)
	CountArtifacts() (int, error)
	Close() error
}

// Syncer keeps runtime SQLite indexes on local disk while persisting copies in
// a durable artifact directory.
type Syncer struct {
	RuntimeDir  string
	ArtifactDir string
	backend     backend
}

// New validates and prepares a cache syncer.
func New(runtimeDir, artifactDir string) (*Syncer, error) {
	runtimeDir = strings.TrimSpace(runtimeDir)
	artifactDir = strings.TrimSpace(artifactDir)
	if runtimeDir == "" {
		return nil, fmt.Errorf("cachepersist: runtime dir is required")
	}
	if err := os.MkdirAll(runtimeDir, 0o750); err != nil {
		return nil, fmt.Errorf("cachepersist: create runtime dir: %w", err)
	}
	artifactDir = strings.TrimSpace(artifactDir)
	if artifactDir == "" {
		return nil, fmt.Errorf("cachepersist: artifact dir is required")
	}
	if err := os.MkdirAll(artifactDir, 0o750); err != nil {
		return nil, fmt.Errorf("cachepersist: create artifact dir: %w", err)
	}
	return &Syncer{
		RuntimeDir:  runtimeDir,
		ArtifactDir: artifactDir,
		backend:     &fsBackend{artifactDir: artifactDir},
	}, nil
}

// Hydrate restores persisted index artifacts into the local runtime cache.
func (s *Syncer) Hydrate() (int, error) {
	if s == nil || s.backend == nil {
		return 0, nil
	}
	return s.backend.Hydrate(s.RuntimeDir)
}

// PersistProject persists one project's SQLite files into the artifact dir.
func (s *Syncer) PersistProject(project string) (int, error) {
	if s == nil || s.backend == nil {
		return 0, nil
	}
	return s.backend.PersistProject(s.RuntimeDir, project)
}

// PersistOrgGraph persists org.db from runtime org/ subdir to durable storage.
func (s *Syncer) PersistOrgGraph() (int, error) {
	if s == nil || s.backend == nil {
		return 0, nil
	}
	return s.backend.PersistOrgDB(s.RuntimeDir)
}

// HydrateOrgGraph restores org.db from durable storage to runtime org/ subdir.
func (s *Syncer) HydrateOrgGraph() (int, error) {
	if s == nil || s.backend == nil {
		return 0, nil
	}
	return s.backend.HydrateOrgDB(s.RuntimeDir)
}

// CountArtifacts returns the number of persisted DB artifact files.
func (s *Syncer) CountArtifacts() (int, error) {
	if s == nil || s.backend == nil {
		return 0, nil
	}
	return s.backend.CountArtifacts()
}

// Close releases any resources held by the syncer backend.
func (s *Syncer) Close() error {
	if s == nil || s.backend == nil {
		return nil
	}
	return s.backend.Close()
}

func listDBArtifacts(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("cachepersist: read dir %s: %w", dir, err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !isDBArtifact(entry.Name()) {
			continue
		}
		files = append(files, entry.Name())
	}
	sort.Strings(files)
	return files, nil
}

func isDBArtifact(name string) bool {
	return strings.HasSuffix(name, ".db")
}

type fsBackend struct {
	artifactDir string
}

func (b *fsBackend) Hydrate(runtimeDir string) (int, error) {
	files, err := listDBArtifacts(b.artifactDir)
	if err != nil {
		return 0, err
	}
	copied := 0
	for _, name := range files {
		src := filepath.Join(b.artifactDir, name)
		dst := filepath.Join(runtimeDir, name)
		if err := copyFileAtomic(src, dst); err != nil {
			return copied, fmt.Errorf("cachepersist: hydrate %s: %w", name, err)
		}
		copied++
	}
	return copied, nil
}

func (b *fsBackend) PersistProject(runtimeDir, project string) (int, error) {
	project = strings.TrimSpace(project)
	if project == "" {
		return 0, fmt.Errorf("cachepersist: project is required")
	}
	pattern := filepath.Join(runtimeDir, project+".db*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return 0, fmt.Errorf("cachepersist: glob project artifacts: %w", err)
	}
	sort.Strings(matches)
	copied := 0
	for _, src := range matches {
		info, err := os.Stat(src)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return copied, fmt.Errorf("cachepersist: stat %s: %w", src, err)
		}
		if info.IsDir() || !isDBArtifact(info.Name()) {
			continue
		}
		dst := filepath.Join(b.artifactDir, info.Name())
		if err := copyFileAtomic(src, dst); err != nil {
			return copied, fmt.Errorf("cachepersist: persist %s: %w", info.Name(), err)
		}
		copied++
	}
	return copied, nil
}

func (b *fsBackend) PersistOrgDB(runtimeDir string) (int, error) {
	srcDir := filepath.Join(runtimeDir, "org")
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("cachepersist: read org dir: %w", err)
	}
	copied := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Copy .db files AND WAL journal files (.db-wal, .db-shm)
		// Without the WAL, the persisted .db file is empty when using WAL mode.
		if !strings.HasSuffix(name, ".db") &&
			!strings.HasSuffix(name, ".db-wal") &&
			!strings.HasSuffix(name, ".db-shm") {
			continue
		}
		src := filepath.Join(srcDir, name)
		dst := filepath.Join(b.artifactDir, "org", name)
		if err := copyFileAtomic(src, dst); err != nil {
			return copied, fmt.Errorf("cachepersist: persist org %s: %w", name, err)
		}
		copied++
	}
	return copied, nil
}

func (b *fsBackend) HydrateOrgDB(runtimeDir string) (int, error) {
	srcDir := filepath.Join(b.artifactDir, "org")
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("cachepersist: read org artifact dir: %w", err)
	}
	copied := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Restore .db files AND WAL journal files (.db-wal, .db-shm)
		if !strings.HasSuffix(name, ".db") &&
			!strings.HasSuffix(name, ".db-wal") &&
			!strings.HasSuffix(name, ".db-shm") {
			continue
		}
		src := filepath.Join(srcDir, name)
		dst := filepath.Join(runtimeDir, "org", name)
		if err := copyFileAtomic(src, dst); err != nil {
			return copied, fmt.Errorf("cachepersist: hydrate org %s: %w", name, err)
		}
		copied++
	}
	return copied, nil
}

func (b *fsBackend) CountArtifacts() (int, error) {
	files, err := listDBArtifacts(b.artifactDir)
	if err != nil {
		return 0, err
	}
	return len(files), nil
}

func (b *fsBackend) Close() error {
	return nil
}

func copyFileAtomic(src, dst string) error {
	input, err := os.Open(src)
	if err != nil {
		return err
	}
	defer input.Close()

	info, err := input.Stat()
	if err != nil {
		return err
	}

	return copyReaderAtomic(input, dst, info.Mode())
}

func copyReaderAtomic(input io.Reader, dst string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".cachepersist-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()

	if _, err := io.Copy(tmp, input); err != nil {
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return err
	}
	return nil
}
