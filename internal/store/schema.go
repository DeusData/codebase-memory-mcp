package store

import (
	"fmt"
	"sort"
)

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
	rows, err := s.q.Query("SELECT label, COUNT(*) as cnt FROM nodes WHERE project=? GROUP BY label ORDER BY cnt DESC", project)
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
	rows2, err := s.q.Query("SELECT type, COUNT(*) as cnt FROM edges WHERE project=? GROUP BY type ORDER BY cnt DESC", project)
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

	// Relationship patterns: build id→label map, then scan edges and aggregate in Go.
	// This replaces a 3-way JOIN (O(edges × 2 lookups)) with two sequential scans.
	idLabel := make(map[int64]string, 4096)
	rows3, err := s.q.Query("SELECT id, label FROM nodes WHERE project=?", project)
	if err != nil {
		return nil, fmt.Errorf("schema id-label: %w", err)
	}
	for rows3.Next() {
		var id int64
		var label string
		if err := rows3.Scan(&id, &label); err != nil {
			rows3.Close()
			return nil, err
		}
		idLabel[id] = label
	}
	rows3.Close()
	if err := rows3.Err(); err != nil {
		return nil, err
	}

	type patternKey struct{ src, rel, tgt string }
	patternCounts := make(map[patternKey]int)
	rows3b, err := s.q.Query("SELECT source_id, target_id, type FROM edges WHERE project=?", project)
	if err != nil {
		return nil, fmt.Errorf("schema edge scan: %w", err)
	}
	for rows3b.Next() {
		var srcID, tgtID int64
		var edgeType string
		if err := rows3b.Scan(&srcID, &tgtID, &edgeType); err != nil {
			rows3b.Close()
			return nil, err
		}
		pk := patternKey{src: idLabel[srcID], rel: edgeType, tgt: idLabel[tgtID]}
		patternCounts[pk]++
	}
	rows3b.Close()
	if err := rows3b.Err(); err != nil {
		return nil, err
	}

	// Sort by count descending, take top 25
	type patternEntry struct {
		key patternKey
		cnt int
	}
	entries := make([]patternEntry, 0, len(patternCounts))
	for k, v := range patternCounts {
		entries = append(entries, patternEntry{k, v})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].cnt > entries[j].cnt })
	if len(entries) > 25 {
		entries = entries[:25]
	}
	for _, e := range entries {
		info.RelationshipPatterns = append(info.RelationshipPatterns,
			fmt.Sprintf("(:%s)-[:%s]->(:%s)  [%dx]", e.key.src, e.key.rel, e.key.tgt, e.cnt))
	}

	// Sample function names
	rows4, err := s.q.Query("SELECT name FROM nodes WHERE project=? AND label='Function' ORDER BY name LIMIT 30", project)
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
	if err := rows4.Err(); err != nil {
		return nil, err
	}

	// Sample class names
	rows5, err := s.q.Query("SELECT name FROM nodes WHERE project=? AND label='Class' ORDER BY name LIMIT 20", project)
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
	if err := rows5.Err(); err != nil {
		return nil, err
	}

	// Sample qualified names
	rows6, err := s.q.Query("SELECT qualified_name FROM nodes WHERE project=? LIMIT 5", project)
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
	if err := rows6.Err(); err != nil {
		return nil, err
	}

	return info, nil
}
