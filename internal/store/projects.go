package store

import (
	"fmt"
	"strings"
)

// Project represents an indexed project.
type Project struct {
	Name      string
	IndexedAt string
	RootPath  string
}

// UpsertProject creates or updates a project record.
func (s *Store) UpsertProject(name, rootPath string) error {
	_, err := s.q.Exec(`
		INSERT INTO projects (name, indexed_at, root_path) VALUES (?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET indexed_at=excluded.indexed_at, root_path=excluded.root_path`,
		name, Now(), rootPath)
	return err
}

// GetProject returns a project by name.
func (s *Store) GetProject(name string) (*Project, error) {
	var p Project
	err := s.q.QueryRow("SELECT name, indexed_at, root_path FROM projects WHERE name=?", name).
		Scan(&p.Name, &p.IndexedAt, &p.RootPath)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// ListProjects returns all indexed projects.
func (s *Store) ListProjects() ([]*Project, error) {
	rows, err := s.q.Query("SELECT name, indexed_at, root_path FROM projects ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*Project
	for rows.Next() {
		var p Project
		if err := rows.Scan(&p.Name, &p.IndexedAt, &p.RootPath); err != nil {
			return nil, err
		}
		result = append(result, &p)
	}
	return result, rows.Err()
}

// DeleteProject deletes a project and all associated data (CASCADE).
func (s *Store) DeleteProject(name string) error {
	_, err := s.q.Exec("DELETE FROM projects WHERE name=?", name)
	return err
}

// FileHash represents a stored file content hash for incremental reindex.
type FileHash struct {
	Project string
	RelPath string
	SHA256  string
}

// UpsertFileHash stores a file's content hash.
func (s *Store) UpsertFileHash(project, relPath, sha256 string) error {
	_, err := s.q.Exec(`
		INSERT INTO file_hashes (project, rel_path, sha256) VALUES (?, ?, ?)
		ON CONFLICT(project, rel_path) DO UPDATE SET sha256=excluded.sha256`,
		project, relPath, sha256)
	return err
}

// GetFileHashes returns all file hashes for a project.
func (s *Store) GetFileHashes(project string) (map[string]string, error) {
	rows, err := s.q.Query("SELECT rel_path, sha256 FROM file_hashes WHERE project=?", project)
	if err != nil {
		return nil, fmt.Errorf("get file hashes: %w", err)
	}
	defer rows.Close()
	result := make(map[string]string)
	for rows.Next() {
		var path, hash string
		if err := rows.Scan(&path, &hash); err != nil {
			return nil, err
		}
		result[path] = hash
	}
	return result, rows.Err()
}

// ListFilesForProject returns all distinct file paths indexed for a project.
func (s *Store) ListFilesForProject(project string) ([]string, error) {
	rows, err := s.q.Query("SELECT DISTINCT file_path FROM nodes WHERE project=? AND file_path != ''", project)
	if err != nil {
		return nil, fmt.Errorf("list files: %w", err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		result = append(result, path)
	}
	return result, rows.Err()
}

// fileHashBatchSize is the max rows per batch INSERT for file hashes (3 cols × 200 = 600 vars < 999).
const fileHashBatchSize = 200

// UpsertFileHashBatch inserts or updates multiple file hashes in batched multi-row INSERTs.
func (s *Store) UpsertFileHashBatch(hashes []FileHash) error {
	if len(hashes) == 0 {
		return nil
	}

	for i := 0; i < len(hashes); i += fileHashBatchSize {
		end := i + fileHashBatchSize
		if end > len(hashes) {
			end = len(hashes)
		}
		if err := s.upsertFileHashChunk(hashes[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) upsertFileHashChunk(batch []FileHash) error {
	var sb strings.Builder
	sb.WriteString(`INSERT INTO file_hashes (project, rel_path, sha256) VALUES `)

	args := make([]any, 0, len(batch)*3)
	for i, h := range batch {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?)")
		args = append(args, h.Project, h.RelPath, h.SHA256)
	}
	sb.WriteString(` ON CONFLICT(project, rel_path) DO UPDATE SET sha256=excluded.sha256`)

	if _, err := s.q.Exec(sb.String(), args...); err != nil {
		return fmt.Errorf("upsert file hash batch: %w", err)
	}
	return nil
}

// DeleteFileHash deletes a single file hash entry.
func (s *Store) DeleteFileHash(project, relPath string) error {
	_, err := s.q.Exec("DELETE FROM file_hashes WHERE project=? AND rel_path=?", project, relPath)
	return err
}

// DeleteFileHashes deletes all file hashes for a project.
func (s *Store) DeleteFileHashes(project string) error {
	_, err := s.q.Exec("DELETE FROM file_hashes WHERE project=?", project)
	return err
}
