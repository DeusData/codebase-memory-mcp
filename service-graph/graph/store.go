package graph

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// Store provides SQLite-backed persistence for the service graph.
type Store struct {
	db *sql.DB
}

// NewStore opens (or creates) the graph database and runs migrations.
func NewStore() (*Store, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home dir: %w", err)
	}
	dir := filepath.Join(home, ".cache", "codebase-memory-mcp", "service-graph")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	dbPath := filepath.Join(dir, "graph.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// SQLite pragmas for performance.
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA cache_size=-65536", // 64 MB
		"PRAGMA mmap_size=67108864", // 64 MB
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("exec pragma %q: %w", p, err)
		}
	}

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

// migrate creates the schema tables and indices if they do not exist.
func (s *Store) migrate() error {
	ddl := `
CREATE TABLE IF NOT EXISTS services (
    id TEXT PRIMARY KEY,
    repo_path TEXT NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS topics (
    id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS graphql_endpoints (
    id TEXT PRIMARY KEY,
    schema_files TEXT,
    gateway_url TEXT
);

CREATE TABLE IF NOT EXISTS tables_ (
    id TEXT PRIMARY KEY,
    orm TEXT
);

CREATE TABLE IF NOT EXISTS edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    target TEXT NOT NULL,
    type TEXT NOT NULL,
    file TEXT NOT NULL,
    line INTEGER NOT NULL,
    metadata TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source);
CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target);
CREATE INDEX IF NOT EXISTS idx_edges_type ON edges(type);
`
	_, err := s.db.Exec(ddl)
	return err
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// Clear deletes all data from every table.
func (s *Store) Clear() error {
	tables := []string{"services", "topics", "graphql_endpoints", "tables_", "edges", "meta"}
	for _, t := range tables {
		if _, err := s.db.Exec("DELETE FROM " + t); err != nil {
			return fmt.Errorf("clear %s: %w", t, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Insert helpers
// ---------------------------------------------------------------------------

// InsertService upserts a service node.
func (s *Store) InsertService(svc ServiceNode) error {
	_, err := s.db.Exec(
		"INSERT OR REPLACE INTO services (id, repo_path, description) VALUES (?, ?, ?)",
		svc.ID, svc.RepoPath, svc.Description,
	)
	return err
}

// InsertTopic upserts a topic node.
func (s *Store) InsertTopic(t TopicNode) error {
	_, err := s.db.Exec(
		"INSERT OR REPLACE INTO topics (id) VALUES (?)",
		t.ID,
	)
	return err
}

// InsertGraphQLEndpoint upserts a GraphQL endpoint.
func (s *Store) InsertGraphQLEndpoint(e GraphQLEndpoint) error {
	schemaJSON, err := json.Marshal(e.SchemaFiles)
	if err != nil {
		return fmt.Errorf("marshal schema_files: %w", err)
	}
	_, err = s.db.Exec(
		"INSERT OR REPLACE INTO graphql_endpoints (id, schema_files, gateway_url) VALUES (?, ?, ?)",
		e.ID, string(schemaJSON), e.GatewayURL,
	)
	return err
}

// InsertTable upserts a database table node.
func (s *Store) InsertTable(t DatabaseTable) error {
	_, err := s.db.Exec(
		"INSERT OR REPLACE INTO tables_ (id, orm) VALUES (?, ?)",
		t.ID, t.ORM,
	)
	return err
}

// InsertEdges batch-inserts edges inside a single transaction.
func (s *Store) InsertEdges(edges []Edge) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.Prepare(
		"INSERT INTO edges (source, target, type, file, line, metadata) VALUES (?, ?, ?, ?, ?, ?)",
	)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for _, e := range edges {
		var metaJSON []byte
		if e.Metadata != nil {
			metaJSON, err = json.Marshal(e.Metadata)
			if err != nil {
				return fmt.Errorf("marshal metadata: %w", err)
			}
		}
		if _, err := stmt.Exec(e.Source, e.Target, e.Type, e.File, e.Line, string(metaJSON)); err != nil {
			return fmt.Errorf("insert edge %s->%s: %w", e.Source, e.Target, err)
		}
	}

	return tx.Commit()
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

// AllServices returns every service node.
func (s *Store) AllServices() ([]ServiceNode, error) {
	rows, err := s.db.Query("SELECT id, repo_path, description FROM services")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ServiceNode
	for rows.Next() {
		var svc ServiceNode
		var desc sql.NullString
		if err := rows.Scan(&svc.ID, &svc.RepoPath, &desc); err != nil {
			return nil, err
		}
		if desc.Valid {
			svc.Description = desc.String
		}
		out = append(out, svc)
	}
	return out, rows.Err()
}

// AllTopics returns every topic node.
func (s *Store) AllTopics() ([]TopicNode, error) {
	rows, err := s.db.Query("SELECT id FROM topics")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TopicNode
	for rows.Next() {
		var t TopicNode
		if err := rows.Scan(&t.ID); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// AllGraphQLEndpoints returns every GraphQL endpoint.
func (s *Store) AllGraphQLEndpoints() ([]GraphQLEndpoint, error) {
	rows, err := s.db.Query("SELECT id, schema_files, gateway_url FROM graphql_endpoints")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []GraphQLEndpoint
	for rows.Next() {
		var e GraphQLEndpoint
		var schemaStr sql.NullString
		var gwURL sql.NullString
		if err := rows.Scan(&e.ID, &schemaStr, &gwURL); err != nil {
			return nil, err
		}
		if schemaStr.Valid {
			if err := json.Unmarshal([]byte(schemaStr.String), &e.SchemaFiles); err != nil {
				return nil, fmt.Errorf("unmarshal schema_files for %s: %w", e.ID, err)
			}
		}
		if gwURL.Valid {
			e.GatewayURL = gwURL.String
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// AllTables returns every database table node.
func (s *Store) AllTables() ([]DatabaseTable, error) {
	rows, err := s.db.Query("SELECT id, orm FROM tables_")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DatabaseTable
	for rows.Next() {
		var t DatabaseTable
		var orm sql.NullString
		if err := rows.Scan(&t.ID, &orm); err != nil {
			return nil, err
		}
		if orm.Valid {
			t.ORM = orm.String
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// AllEdges returns every edge.
func (s *Store) AllEdges() ([]Edge, error) {
	return s.queryEdges("SELECT source, target, type, file, line, metadata FROM edges")
}

// EdgesBySource returns edges originating from source.
func (s *Store) EdgesBySource(source string) ([]Edge, error) {
	return s.queryEdges(
		"SELECT source, target, type, file, line, metadata FROM edges WHERE source = ?",
		source,
	)
}

// EdgesByTarget returns edges pointing to target.
func (s *Store) EdgesByTarget(target string) ([]Edge, error) {
	return s.queryEdges(
		"SELECT source, target, type, file, line, metadata FROM edges WHERE target = ?",
		target,
	)
}

// EdgesByType returns edges of a given type.
func (s *Store) EdgesByType(edgeType string) ([]Edge, error) {
	return s.queryEdges(
		"SELECT source, target, type, file, line, metadata FROM edges WHERE type = ?",
		edgeType,
	)
}

// queryEdges is a shared helper that scans edge rows.
func (s *Store) queryEdges(query string, args ...interface{}) ([]Edge, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Edge
	for rows.Next() {
		var e Edge
		var metaStr sql.NullString
		if err := rows.Scan(&e.Source, &e.Target, &e.Type, &e.File, &e.Line, &metaStr); err != nil {
			return nil, err
		}
		if metaStr.Valid && metaStr.String != "" {
			if err := json.Unmarshal([]byte(metaStr.String), &e.Metadata); err != nil {
				return nil, fmt.Errorf("unmarshal edge metadata: %w", err)
			}
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Composite operations
// ---------------------------------------------------------------------------

// GetGraph loads the full service graph from the database.
func (s *Store) GetGraph() (*ServiceGraph, error) {
	services, err := s.AllServices()
	if err != nil {
		return nil, fmt.Errorf("load services: %w", err)
	}
	topics, err := s.AllTopics()
	if err != nil {
		return nil, fmt.Errorf("load topics: %w", err)
	}
	gqlEndpoints, err := s.AllGraphQLEndpoints()
	if err != nil {
		return nil, fmt.Errorf("load graphql endpoints: %w", err)
	}
	tables, err := s.AllTables()
	if err != nil {
		return nil, fmt.Errorf("load tables: %w", err)
	}
	edges, err := s.AllEdges()
	if err != nil {
		return nil, fmt.Errorf("load edges: %w", err)
	}

	scannedAt, _ := s.GetMeta("scannedAt")

	return &ServiceGraph{
		Services:         services,
		Topics:           topics,
		GraphQLEndpoints: gqlEndpoints,
		Tables:           tables,
		Edges:            edges,
		ScannedAt:        scannedAt,
	}, nil
}

// SaveGraph clears the database and writes the entire graph.
func (s *Store) SaveGraph(g *ServiceGraph) error {
	if err := s.Clear(); err != nil {
		return fmt.Errorf("clear before save: %w", err)
	}

	for _, svc := range g.Services {
		if err := s.InsertService(svc); err != nil {
			return fmt.Errorf("insert service %s: %w", svc.ID, err)
		}
	}
	for _, t := range g.Topics {
		if err := s.InsertTopic(t); err != nil {
			return fmt.Errorf("insert topic %s: %w", t.ID, err)
		}
	}
	for _, e := range g.GraphQLEndpoints {
		if err := s.InsertGraphQLEndpoint(e); err != nil {
			return fmt.Errorf("insert graphql endpoint %s: %w", e.ID, err)
		}
	}
	for _, t := range g.Tables {
		if err := s.InsertTable(t); err != nil {
			return fmt.Errorf("insert table %s: %w", t.ID, err)
		}
	}
	if err := s.InsertEdges(g.Edges); err != nil {
		return fmt.Errorf("insert edges: %w", err)
	}

	if g.ScannedAt != "" {
		if err := s.SetMeta("scannedAt", g.ScannedAt); err != nil {
			return fmt.Errorf("set scannedAt: %w", err)
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Meta key-value store
// ---------------------------------------------------------------------------

// SetMeta upserts a key-value pair in the meta table.
func (s *Store) SetMeta(key, value string) error {
	_, err := s.db.Exec(
		"INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
		key, value,
	)
	return err
}

// GetMeta retrieves a value by key from the meta table.
// Returns an empty string (and no error) when the key is not found.
func (s *Store) GetMeta(key string) (string, error) {
	var value string
	err := s.db.QueryRow("SELECT value FROM meta WHERE key = ?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}
