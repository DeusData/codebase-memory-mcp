package store

import (
	"database/sql"
	"fmt"
)

// InsertEdge inserts an edge (dedup by source_id, target_id, type).
func (s *Store) InsertEdge(e *Edge) (int64, error) {
	res, err := s.q.Exec(`
		INSERT INTO edges (project, source_id, target_id, type, properties)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(source_id, target_id, type) DO UPDATE SET properties=excluded.properties`,
		e.Project, e.SourceID, e.TargetID, e.Type, marshalProps(e.Properties))
	if err != nil {
		return 0, fmt.Errorf("insert edge: %w", err)
	}
	return res.LastInsertId()
}

// FindEdgesBySource finds all edges from a given source node.
func (s *Store) FindEdgesBySource(sourceID int64) ([]*Edge, error) {
	rows, err := s.q.Query(`SELECT id, project, source_id, target_id, type, properties
		FROM edges WHERE source_id=?`, sourceID)
	if err != nil {
		return nil, fmt.Errorf("find edges by source: %w", err)
	}
	defer rows.Close()
	return scanEdges(rows)
}

// FindEdgesByTarget finds all edges to a given target node.
func (s *Store) FindEdgesByTarget(targetID int64) ([]*Edge, error) {
	rows, err := s.q.Query(`SELECT id, project, source_id, target_id, type, properties
		FROM edges WHERE target_id=?`, targetID)
	if err != nil {
		return nil, fmt.Errorf("find edges by target: %w", err)
	}
	defer rows.Close()
	return scanEdges(rows)
}

// FindEdgesBySourceAndType finds edges from a source with a specific type.
func (s *Store) FindEdgesBySourceAndType(sourceID int64, edgeType string) ([]*Edge, error) {
	rows, err := s.q.Query(`SELECT id, project, source_id, target_id, type, properties
		FROM edges WHERE source_id=? AND type=?`, sourceID, edgeType)
	if err != nil {
		return nil, fmt.Errorf("find edges by source+type: %w", err)
	}
	defer rows.Close()
	return scanEdges(rows)
}

// FindEdgesByTargetAndType finds edges to a target with a specific type.
func (s *Store) FindEdgesByTargetAndType(targetID int64, edgeType string) ([]*Edge, error) {
	rows, err := s.q.Query(`SELECT id, project, source_id, target_id, type, properties
		FROM edges WHERE target_id=? AND type=?`, targetID, edgeType)
	if err != nil {
		return nil, fmt.Errorf("find edges by target+type: %w", err)
	}
	defer rows.Close()
	return scanEdges(rows)
}

// CountEdges returns the number of edges in a project.
func (s *Store) CountEdges(project string) (int, error) {
	var count int
	err := s.q.QueryRow("SELECT COUNT(*) FROM edges WHERE project=?", project).Scan(&count)
	return count, err
}

// DeleteEdgesByProject deletes all edges for a project.
func (s *Store) DeleteEdgesByProject(project string) error {
	_, err := s.q.Exec("DELETE FROM edges WHERE project=?", project)
	return err
}

// DeleteEdgesByType deletes all edges of a given type for a project.
func (s *Store) DeleteEdgesByType(project, edgeType string) error {
	_, err := s.q.Exec("DELETE FROM edges WHERE project=? AND type=?", project, edgeType)
	return err
}

// DeleteEdgesBySourceFile deletes edges of a given type where the source node
// belongs to a specific file. Used for incremental re-indexing of CALLS edges.
func (s *Store) DeleteEdgesBySourceFile(project, filePath, edgeType string) error {
	_, err := s.q.Exec(`
		DELETE FROM edges WHERE id IN (
			SELECT e.id FROM edges e
			JOIN nodes n ON e.source_id = n.id
			WHERE e.project=? AND n.file_path=? AND e.type=?
		)`, project, filePath, edgeType)
	return err
}

func scanEdges(rows *sql.Rows) ([]*Edge, error) {
	var result []*Edge
	for rows.Next() {
		var e Edge
		var props string
		if err := rows.Scan(&e.ID, &e.Project, &e.SourceID, &e.TargetID, &e.Type, &props); err != nil {
			return nil, err
		}
		e.Properties = unmarshalProps(props)
		result = append(result, &e)
	}
	return result, rows.Err()
}
