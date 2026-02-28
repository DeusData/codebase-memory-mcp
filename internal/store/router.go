package store

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ProjectInfo holds metadata about a discovered project database.
type ProjectInfo struct {
	Name     string
	DBPath   string
	RootPath string
}

// StoreRouter manages per-project SQLite databases.
// Each project gets its own .db file in the cache directory.
type StoreRouter struct {
	dir    string            // ~/.cache/codebase-memory-mcp/
	stores map[string]*Store // project name → open Store (lazy)
	mu     sync.Mutex
}

// NewRouter creates a StoreRouter, ensuring the cache directory exists.
// Runs migration from single-DB layout if needed.
func NewRouter() (*StoreRouter, error) {
	dir, err := cacheDir()
	if err != nil {
		return nil, err
	}

	r := &StoreRouter{
		dir:    dir,
		stores: make(map[string]*Store),
	}

	// Run one-time migration from single DB to per-project DBs
	if err := r.migrate(); err != nil {
		slog.Warn("router.migrate.err", "err", err)
	}

	return r, nil
}

// NewRouterWithDir creates a StoreRouter using a custom directory (for testing).
// No migration is run.
func NewRouterWithDir(dir string) (*StoreRouter, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	return &StoreRouter{
		dir:    dir,
		stores: make(map[string]*Store),
	}, nil
}

// ForProject returns the Store for the given project, opening it lazily.
func (r *StoreRouter) ForProject(name string) (*Store, error) {
	if name == "*" || name == "all" {
		return nil, fmt.Errorf("invalid project name: %q", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if s, ok := r.stores[name]; ok {
		return s, nil
	}

	s, err := OpenInDir(r.dir, name)
	if err != nil {
		return nil, fmt.Errorf("open store %q: %w", name, err)
	}
	r.stores[name] = s
	return s, nil
}

// AllStores opens all .db files in the cache dir and returns a name→Store map.
func (r *StoreRouter) AllStores() map[string]*Store {
	r.mu.Lock()
	defer r.mu.Unlock()

	entries, err := os.ReadDir(r.dir)
	if err != nil {
		slog.Warn("router.all_stores.readdir", "err", err)
		return r.stores
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".db")
		if name == "codebase-memory" {
			continue // skip legacy single DB
		}
		if _, ok := r.stores[name]; ok {
			continue
		}
		s, err := OpenInDir(r.dir, name)
		if err != nil {
			slog.Warn("router.all_stores.open", "project", name, "err", err)
			continue
		}
		r.stores[name] = s
	}

	// Return a copy to avoid external mutation
	result := make(map[string]*Store, len(r.stores))
	for k, v := range r.stores {
		result[k] = v
	}
	return result
}

// ListProjects scans .db files and queries each for metadata.
func (r *StoreRouter) ListProjects() ([]*ProjectInfo, error) {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return nil, fmt.Errorf("readdir: %w", err)
	}

	result := make([]*ProjectInfo, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".db")
		if name == "codebase-memory" {
			continue // skip legacy single DB
		}
		info := &ProjectInfo{
			Name:   name,
			DBPath: filepath.Join(r.dir, e.Name()),
		}

		// Try to get root_path from the projects table
		s, err := r.ForProject(name)
		if err == nil {
			projects, listErr := s.ListProjects()
			if listErr == nil && len(projects) > 0 {
				info.RootPath = projects[0].RootPath
			}
		}

		result = append(result, info)
	}
	return result, nil
}

// DeleteProject closes the Store connection and removes the .db + WAL/SHM files.
func (r *StoreRouter) DeleteProject(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if s, ok := r.stores[name]; ok {
		s.Close()
		delete(r.stores, name)
	}

	dbPath := filepath.Join(r.dir, name+".db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		p := dbPath + suffix
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove %s: %w", p, err)
		}
	}
	slog.Info("router.delete", "project", name)
	return nil
}

// HasProject checks if a .db file exists for the given project (without opening it).
func (r *StoreRouter) HasProject(name string) bool {
	dbPath := filepath.Join(r.dir, name+".db")
	_, err := os.Stat(dbPath)
	return err == nil
}

// Dir returns the cache directory path.
func (r *StoreRouter) Dir() string {
	return r.dir
}

// CloseAll closes all open Store connections.
func (r *StoreRouter) CloseAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for name, s := range r.stores {
		if err := s.Close(); err != nil {
			slog.Warn("router.close", "project", name, "err", err)
		}
	}
	r.stores = make(map[string]*Store)
}
