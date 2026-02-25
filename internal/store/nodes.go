package store

import (
	"database/sql"
	"fmt"
)

// UpsertNode inserts or replaces a node (dedup by qualified_name).
func (s *Store) UpsertNode(n *Node) (int64, error) {
	res, err := s.q.Exec(`
		INSERT INTO nodes (project, label, name, qualified_name, file_path, start_line, end_line, properties)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(project, qualified_name) DO UPDATE SET
			label=excluded.label, name=excluded.name, file_path=excluded.file_path,
			start_line=excluded.start_line, end_line=excluded.end_line, properties=excluded.properties`,
		n.Project, n.Label, n.Name, n.QualifiedName, n.FilePath, n.StartLine, n.EndLine, marshalProps(n.Properties))
	if err != nil {
		return 0, fmt.Errorf("upsert node: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	// On conflict, LastInsertId may return 0; query the actual id
	if id == 0 {
		err = s.q.QueryRow("SELECT id FROM nodes WHERE project=? AND qualified_name=?", n.Project, n.QualifiedName).Scan(&id)
		if err != nil {
			return 0, fmt.Errorf("get node id: %w", err)
		}
	}
	return id, nil
}

// FindNodeByID finds a node by its primary key ID.
func (s *Store) FindNodeByID(id int64) (*Node, error) {
	row := s.q.QueryRow(`SELECT id, project, label, name, qualified_name, file_path, start_line, end_line, properties
		FROM nodes WHERE id=?`, id)
	return scanNode(row)
}

// FindNodeByQN finds a node by project and qualified name.
func (s *Store) FindNodeByQN(project, qualifiedName string) (*Node, error) {
	row := s.q.QueryRow(`SELECT id, project, label, name, qualified_name, file_path, start_line, end_line, properties
		FROM nodes WHERE project=? AND qualified_name=?`, project, qualifiedName)
	return scanNode(row)
}

// FindNodesByName finds nodes by project and name.
func (s *Store) FindNodesByName(project, name string) ([]*Node, error) {
	rows, err := s.q.Query(`SELECT id, project, label, name, qualified_name, file_path, start_line, end_line, properties
		FROM nodes WHERE project=? AND name=?`, project, name)
	if err != nil {
		return nil, fmt.Errorf("find by name: %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}

// FindNodesByLabel finds all nodes with a given label in a project.
func (s *Store) FindNodesByLabel(project, label string) ([]*Node, error) {
	rows, err := s.q.Query(`SELECT id, project, label, name, qualified_name, file_path, start_line, end_line, properties
		FROM nodes WHERE project=? AND label=?`, project, label)
	if err != nil {
		return nil, fmt.Errorf("find by label: %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}

// FindNodesByFile finds all nodes in a given file.
func (s *Store) FindNodesByFile(project, filePath string) ([]*Node, error) {
	rows, err := s.q.Query(`SELECT id, project, label, name, qualified_name, file_path, start_line, end_line, properties
		FROM nodes WHERE project=? AND file_path=?`, project, filePath)
	if err != nil {
		return nil, fmt.Errorf("find by file: %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}

// CountNodes returns the number of nodes in a project.
func (s *Store) CountNodes(project string) (int, error) {
	var count int
	err := s.q.QueryRow("SELECT COUNT(*) FROM nodes WHERE project=?", project).Scan(&count)
	return count, err
}

// DeleteNodesByProject deletes all nodes for a project.
func (s *Store) DeleteNodesByProject(project string) error {
	_, err := s.q.Exec("DELETE FROM nodes WHERE project=?", project)
	return err
}

// DeleteNodesByFile deletes all nodes for a specific file in a project.
func (s *Store) DeleteNodesByFile(project, filePath string) error {
	_, err := s.q.Exec("DELETE FROM nodes WHERE project=? AND file_path=?", project, filePath)
	return err
}

// DeleteNodesByLabel deletes all nodes with a given label in a project.
func (s *Store) DeleteNodesByLabel(project, label string) error {
	_, err := s.q.Exec("DELETE FROM nodes WHERE project=? AND label=?", project, label)
	return err
}

// AllNodes returns all nodes for a project.
func (s *Store) AllNodes(project string) ([]*Node, error) {
	rows, err := s.q.Query(`SELECT id, project, label, name, qualified_name, file_path, start_line, end_line, properties
		FROM nodes WHERE project=?`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNodes(rows)
}

type scanner interface {
	Scan(dest ...any) error
}

func scanNode(row scanner) (*Node, error) {
	var n Node
	var props string
	err := row.Scan(&n.ID, &n.Project, &n.Label, &n.Name, &n.QualifiedName, &n.FilePath, &n.StartLine, &n.EndLine, &props)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	n.Properties = unmarshalProps(props)
	return &n, nil
}

func scanNodes(rows *sql.Rows) ([]*Node, error) {
	var result []*Node
	for rows.Next() {
		var n Node
		var props string
		if err := rows.Scan(&n.ID, &n.Project, &n.Label, &n.Name, &n.QualifiedName, &n.FilePath, &n.StartLine, &n.EndLine, &props); err != nil {
			return nil, err
		}
		n.Properties = unmarshalProps(props)
		result = append(result, &n)
	}
	return result, rows.Err()
}
