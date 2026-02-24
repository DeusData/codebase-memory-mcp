package store

import "fmt"

// SchemaInfo contains graph schema statistics.
type SchemaInfo struct {
	NodeLabels           []LabelCount `json:"node_labels"`
	RelationshipTypes    []TypeCount  `json:"relationship_types"`
	RelationshipPatterns []string     `json:"relationship_patterns"`
	SampleFunctionNames  []string     `json:"sample_function_names"`
	SampleClassNames     []string     `json:"sample_class_names"`
	SampleQualifiedNames []string     `json:"sample_qualified_names"`
}

// LabelCount is a label with its count.
type LabelCount struct {
	Label string `json:"label"`
	Count int    `json:"count"`
}

// TypeCount is a relationship type with its count.
type TypeCount struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

// GetSchema returns graph schema statistics for a project.
func (s *Store) GetSchema(project string) (*SchemaInfo, error) {
	info := &SchemaInfo{}

	// Node label counts
	rows, err := s.db.Query("SELECT label, COUNT(*) as cnt FROM nodes WHERE project=? GROUP BY label ORDER BY cnt DESC", project)
	if err != nil {
		return nil, fmt.Errorf("schema labels: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var lc LabelCount
		if err := rows.Scan(&lc.Label, &lc.Count); err != nil {
			return nil, err
		}
		info.NodeLabels = append(info.NodeLabels, lc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Edge type counts
	rows2, err := s.db.Query("SELECT type, COUNT(*) as cnt FROM edges WHERE project=? GROUP BY type ORDER BY cnt DESC", project)
	if err != nil {
		return nil, fmt.Errorf("schema edge types: %w", err)
	}
	defer rows2.Close()
	for rows2.Next() {
		var tc TypeCount
		if err := rows2.Scan(&tc.Type, &tc.Count); err != nil {
			return nil, err
		}
		info.RelationshipTypes = append(info.RelationshipTypes, tc)
	}
	if err := rows2.Err(); err != nil {
		return nil, err
	}

	// Relationship patterns: (src_label)-[type]->(tgt_label) with counts
	rows3, err := s.db.Query(`
		SELECT sn.label, e.type, tn.label, COUNT(*) as cnt
		FROM edges e
		JOIN nodes sn ON e.source_id = sn.id
		JOIN nodes tn ON e.target_id = tn.id
		WHERE e.project=?
		GROUP BY sn.label, e.type, tn.label
		ORDER BY cnt DESC
		LIMIT 25`, project)
	if err != nil {
		return nil, fmt.Errorf("schema patterns: %w", err)
	}
	defer rows3.Close()
	for rows3.Next() {
		var src, rel, tgt string
		var cnt int
		if err := rows3.Scan(&src, &rel, &tgt, &cnt); err != nil {
			return nil, err
		}
		info.RelationshipPatterns = append(info.RelationshipPatterns, fmt.Sprintf("(:%s)-[:%s]->(:%s)  [%dx]", src, rel, tgt, cnt))
	}
	if err := rows3.Err(); err != nil {
		return nil, err
	}

	// Sample function names
	rows4, err := s.db.Query("SELECT name FROM nodes WHERE project=? AND label='Function' ORDER BY name LIMIT 30", project)
	if err != nil {
		return nil, fmt.Errorf("schema sample funcs: %w", err)
	}
	defer rows4.Close()
	for rows4.Next() {
		var name string
		if err := rows4.Scan(&name); err != nil {
			return nil, err
		}
		info.SampleFunctionNames = append(info.SampleFunctionNames, name)
	}

	// Sample class names
	rows5, err := s.db.Query("SELECT name FROM nodes WHERE project=? AND label='Class' ORDER BY name LIMIT 20", project)
	if err != nil {
		return nil, fmt.Errorf("schema sample classes: %w", err)
	}
	defer rows5.Close()
	for rows5.Next() {
		var name string
		if err := rows5.Scan(&name); err != nil {
			return nil, err
		}
		info.SampleClassNames = append(info.SampleClassNames, name)
	}

	// Sample qualified names
	rows6, err := s.db.Query("SELECT qualified_name FROM nodes WHERE project=? LIMIT 5", project)
	if err != nil {
		return nil, fmt.Errorf("schema sample qns: %w", err)
	}
	defer rows6.Close()
	for rows6.Next() {
		var qn string
		if err := rows6.Scan(&qn); err != nil {
			return nil, err
		}
		info.SampleQualifiedNames = append(info.SampleQualifiedNames, qn)
	}

	return info, nil
}
