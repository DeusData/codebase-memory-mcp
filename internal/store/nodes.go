package store

import (
	"database/sql"
	"fmt"
	"strings"
)

// UpsertNode inserts or replaces a node (dedup by qualified_name).
// Note: LastInsertId() can return stale IDs for ON CONFLICT DO UPDATE,
// causing occasional FK failures in downstream edge inserts. This is
// accepted for performance — the fallback SELECT only runs when id==0.
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

// FindNodesByIDs returns a map of nodeID → *Node for the given IDs.
func (s *Store) FindNodesByIDs(ids []int64) (map[int64]*Node, error) {
	if len(ids) == 0 {
		return map[int64]*Node{}, nil
	}
	result := make(map[int64]*Node, len(ids))
	const batchSize = 998 // leave room under 999 limit

	for i := 0; i < len(ids); i += batchSize {
		end := i + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[i:end]

		placeholders := make([]string, len(chunk))
		args := make([]any, len(chunk))
		for j, id := range chunk {
			placeholders[j] = "?"
			args[j] = id
		}

		query := fmt.Sprintf(
			"SELECT id, project, label, name, qualified_name, file_path, start_line, end_line, properties FROM nodes WHERE id IN (%s)",
			strings.Join(placeholders, ","))

		if err := func() error {
			rows, err := s.q.Query(query, args...)
			if err != nil {
				return fmt.Errorf("find nodes by ids: %w", err)
			}
			defer rows.Close()
			nodes, err := scanNodes(rows)
			if err != nil {
				return err
			}
			for _, n := range nodes {
				result[n.ID] = n
			}
			return nil
		}(); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// FindNodesByFileOverlap returns nodes whose line range overlaps [startLine, endLine].
// The fileSuffix is matched with LIKE '%' || ? against the file_path column to handle
// relative/absolute path differences.
func (s *Store) FindNodesByFileOverlap(project, fileSuffix string, startLine, endLine int) ([]*Node, error) {
	rows, err := s.q.Query(`SELECT id, project, label, name, qualified_name, file_path, start_line, end_line, properties
		FROM nodes WHERE project=? AND file_path LIKE '%' || ? AND start_line <= ? AND end_line >= ?
		AND label NOT IN ('Project', 'Package', 'Folder', 'File', 'Module')`,
		project, fileSuffix, endLine, startLine)
	if err != nil {
		return nil, fmt.Errorf("find by file overlap: %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
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

// Formula-derived batch size: SQLite has a 999 bind variable limit.
const numNodeCols = 8
const nodesBatchSize = 999 / numNodeCols // = 124

// UpsertNodeBatch inserts or updates multiple nodes in batched multi-row INSERTs.
// Returns a map of qualifiedName → ID for all upserted nodes.
func (s *Store) UpsertNodeBatch(nodes []*Node) (map[string]int64, error) {
	if len(nodes) == 0 {
		return map[string]int64{}, nil
	}

	result := make(map[string]int64, len(nodes))

	for i := 0; i < len(nodes); i += nodesBatchSize {
		end := i + nodesBatchSize
		if end > len(nodes) {
			end = len(nodes)
		}
		batch := nodes[i:end]

		if err := s.upsertNodeChunk(batch, result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *Store) upsertNodeChunk(batch []*Node, idMap map[string]int64) error {
	// Build multi-row INSERT
	var sb strings.Builder
	sb.WriteString(`INSERT INTO nodes (project, label, name, qualified_name, file_path, start_line, end_line, properties) VALUES `)

	args := make([]any, 0, len(batch)*8)
	for i, n := range batch {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?,?,?,?)")
		args = append(args, n.Project, n.Label, n.Name, n.QualifiedName, n.FilePath, n.StartLine, n.EndLine, marshalProps(n.Properties))
	}
	sb.WriteString(` ON CONFLICT(project, qualified_name) DO UPDATE SET
		label=excluded.label, name=excluded.name, file_path=excluded.file_path,
		start_line=excluded.start_line, end_line=excluded.end_line, properties=excluded.properties`)

	if _, err := s.q.Exec(sb.String(), args...); err != nil {
		return fmt.Errorf("upsert node batch: %w", err)
	}

	// Recover IDs via SELECT ... IN (...)
	// Group by project since the UNIQUE constraint is (project, qualified_name)
	byProject := make(map[string][]string)
	for _, n := range batch {
		byProject[n.Project] = append(byProject[n.Project], n.QualifiedName)
	}

	for project, qns := range byProject {
		if err := s.resolveNodeIDs(project, qns, idMap); err != nil {
			return err
		}
	}
	return nil
}

// resolveNodeIDs fetches IDs for a set of qualified names in a single project.
// Respects the 999-var limit by batching the IN clause.
func (s *Store) resolveNodeIDs(project string, qns []string, idMap map[string]int64) error {
	// 1 var for project + N vars for QNs; batch to stay under 999
	const maxQNsPerQuery = 998

	for i := 0; i < len(qns); i += maxQNsPerQuery {
		end := i + maxQNsPerQuery
		if end > len(qns) {
			end = len(qns)
		}
		chunk := qns[i:end]

		placeholders := make([]string, len(chunk))
		args := make([]any, 0, len(chunk)+1)
		args = append(args, project)
		for j, qn := range chunk {
			placeholders[j] = "?"
			args = append(args, qn)
		}

		query := fmt.Sprintf("SELECT id, qualified_name FROM nodes WHERE project = ? AND qualified_name IN (%s)",
			strings.Join(placeholders, ","))

		if err := func() error {
			rows, err := s.q.Query(query, args...)
			if err != nil {
				return fmt.Errorf("resolve node IDs: %w", err)
			}
			defer rows.Close()
			for rows.Next() {
				var id int64
				var qn string
				if err := rows.Scan(&id, &qn); err != nil {
					return err
				}
				idMap[qn] = id
			}
			return rows.Err()
		}(); err != nil {
			return err
		}
	}
	return nil
}

// FindNodeIDsByQNs returns a map of qualifiedName → ID for the given QNs in a project.
func (s *Store) FindNodeIDsByQNs(project string, qns []string) (map[string]int64, error) {
	if len(qns) == 0 {
		return map[string]int64{}, nil
	}
	idMap := make(map[string]int64, len(qns))
	if err := s.resolveNodeIDs(project, qns, idMap); err != nil {
		return nil, err
	}
	return idMap, nil
}
