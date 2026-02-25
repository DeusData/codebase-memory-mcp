package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// Querier abstracts *sql.DB and *sql.Tx so store methods work in both contexts.
type Querier interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// Store wraps a SQLite connection for graph storage.
type Store struct {
	db     *sql.DB
	q      Querier // active querier: db or tx
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
	s.q = s.db
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
	s.q = s.db
	if err := s.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return s, nil
}

// WithTransaction executes fn within a single SQLite transaction.
// The callback receives a transaction-scoped Store — all store methods called on
// txStore use the transaction. The receiver's q field is never mutated, so
// concurrent read-only handlers (using s.q == s.db) are unaffected.
func (s *Store) WithTransaction(fn func(txStore *Store) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	txStore := &Store{db: s.db, q: tx, dbPath: s.dbPath}
	if err := fn(txStore); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
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

	CREATE INDEX IF NOT EXISTS idx_edges_target_type ON edges(project, target_id, type);
	CREATE INDEX IF NOT EXISTS idx_edges_source_type ON edges(project, source_id, type);
	`
	_, err := s.db.Exec(schema)
	if err != nil {
		return err
	}

	// Migration: add url_path generated column to edges table.
	// Generated columns require SQLite 3.31.0+ (modernc.org/sqlite supports this).
	// We check if the column already exists to make this idempotent.
	var colCount int
	_ = s.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_xinfo('edges') WHERE name='url_path_gen'`).Scan(&colCount)
	if colCount == 0 {
		_, err = s.db.Exec(`ALTER TABLE edges ADD COLUMN url_path_gen TEXT GENERATED ALWAYS AS (json_extract(properties, '$.url_path'))`)
		if err != nil {
			// If generated columns aren't supported, skip gracefully
			slog.Warn("schema.url_path_gen.skip", "err", err)
		}
	}

	// Index on generated column (safe to CREATE IF NOT EXISTS)
	_, _ = s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_edges_url_path ON edges(project, url_path_gen)`)

	return nil
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

// UnmarshalProps deserializes JSON properties. Exported for use by cypher executor.
func UnmarshalProps(data string) map[string]any {
	return unmarshalProps(data)
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
