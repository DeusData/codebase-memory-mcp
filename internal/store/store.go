package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps a SQLite connection for graph storage.
type Store struct {
	db     *sql.DB
	dbPath string
}

// Node represents a graph node stored in SQLite.
type Node struct {
	ID            int64
	Project       string
	Label         string
	Name          string
	QualifiedName string
	FilePath      string
	StartLine     int
	EndLine       int
	Properties    map[string]any
}

// Edge represents a graph edge stored in SQLite.
type Edge struct {
	ID         int64
	Project    string
	SourceID   int64
	TargetID   int64
	Type       string
	Properties map[string]any
}

// cacheDir returns the default cache directory for databases.
func cacheDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("home dir: %w", err)
	}
	dir := filepath.Join(home, ".cache", "codebase-memory-mcp")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir cache: %w", err)
	}
	return dir, nil
}

// Open opens or creates a SQLite database for the given project.
func Open(project string) (*Store, error) {
	dir, err := cacheDir()
	if err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dir, project+".db")
	return OpenPath(dbPath)
}

// OpenPath opens a SQLite database at the given path.
func OpenPath(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	s := &Store{db: db, dbPath: dbPath}
	if err := s.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return s, nil
}

// OpenMemory opens an in-memory SQLite database (for testing).
func OpenMemory() (*Store, error) {
	db, err := sql.Open("sqlite", ":memory:?_pragma=foreign_keys(ON)")
	if err != nil {
		return nil, fmt.Errorf("open memory db: %w", err)
	}
	s := &Store{db: db, dbPath: ":memory:"}
	if err := s.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return s, nil
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// DB returns the underlying sql.DB (for advanced queries).
func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS projects (
		name TEXT PRIMARY KEY,
		indexed_at TEXT NOT NULL,
		root_path TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS file_hashes (
		project TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
		rel_path TEXT NOT NULL,
		sha256 TEXT NOT NULL,
		PRIMARY KEY (project, rel_path)
	);

	CREATE TABLE IF NOT EXISTS nodes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		project TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
		label TEXT NOT NULL,
		name TEXT NOT NULL,
		qualified_name TEXT NOT NULL,
		file_path TEXT DEFAULT '',
		start_line INTEGER DEFAULT 0,
		end_line INTEGER DEFAULT 0,
		properties TEXT DEFAULT '{}',
		UNIQUE(project, qualified_name)
	);

	CREATE INDEX IF NOT EXISTS idx_nodes_label ON nodes(project, label);
	CREATE INDEX IF NOT EXISTS idx_nodes_name ON nodes(project, name);
	CREATE INDEX IF NOT EXISTS idx_nodes_file ON nodes(project, file_path);

	CREATE TABLE IF NOT EXISTS edges (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		project TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
		source_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
		target_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
		type TEXT NOT NULL,
		properties TEXT DEFAULT '{}',
		UNIQUE(source_id, target_id, type)
	);

	CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id, type);
	CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id, type);
	CREATE INDEX IF NOT EXISTS idx_edges_type ON edges(project, type);
	`
	_, err := s.db.Exec(schema)
	return err
}

// marshalProps serializes properties to JSON.
func marshalProps(props map[string]any) string {
	if props == nil {
		return "{}"
	}
	b, err := json.Marshal(props)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// unmarshalProps deserializes JSON properties.
func unmarshalProps(data string) map[string]any {
	if data == "" {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		return map[string]any{}
	}
	return m
}

// Now returns the current time in ISO 8601 format.
func Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}
