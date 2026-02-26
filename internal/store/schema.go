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

	var err error
	if info.NodeLabels, err = s.schemaNodeLabels(project); err != nil {
		return nil, err
	}
	if info.RelationshipTypes, err = s.schemaEdgeTypes(project); err != nil {
		return nil, err
	}
	if info.RelationshipPatterns, err = s.schemaRelPatterns(project); err != nil {
		return nil, err
	}
	if info.SampleFunctionNames, err = s.schemaSampleNames(project, "Function", 30); err != nil {
		return nil, err
	}
	if info.SampleClassNames, err = s.schemaSampleNames(project, "Class", 20); err != nil {
		return nil, err
	}
	if info.SampleQualifiedNames, err = s.schemaSampleQNs(project); err != nil {
		return nil, err
	}
	return info, nil
}

func (s *Store) schemaNodeLabels(project string) ([]LabelCount, error) {
	rows, err := s.q.Query("SELECT label, COUNT(*) as cnt FROM nodes WHERE project=? GROUP BY label ORDER BY cnt DESC", project)
	if err != nil {
		return nil, fmt.Errorf("schema labels: %w", err)
	}
	defer rows.Close()
	var labels []LabelCount
	for rows.Next() {
		var lc LabelCount
		if err := rows.Scan(&lc.Label, &lc.Count); err != nil {
			return nil, err
		}
		labels = append(labels, lc)
	}
	return labels, rows.Err()
}

func (s *Store) schemaEdgeTypes(project string) ([]TypeCount, error) {
	rows, err := s.q.Query("SELECT type, COUNT(*) as cnt FROM edges WHERE project=? GROUP BY type ORDER BY cnt DESC", project)
	if err != nil {
		return nil, fmt.Errorf("schema edge types: %w", err)
	}
	defer rows.Close()
	var types []TypeCount
	for rows.Next() {
		var tc TypeCount
		if err := rows.Scan(&tc.Type, &tc.Count); err != nil {
			return nil, err
		}
		types = append(types, tc)
	}
	return types, rows.Err()
}

// schemaRelPatterns builds relationship patterns via id→label map + edge scan.
// This replaces a 3-way JOIN (O(edges × 2 lookups)) with two sequential scans.
func (s *Store) schemaRelPatterns(project string) ([]string, error) {
	idLabel := make(map[int64]string, 4096)
	rows, err := s.q.Query("SELECT id, label FROM nodes WHERE project=?", project)
	if err != nil {
		return nil, fmt.Errorf("schema id-label: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var label string
		if err := rows.Scan(&id, &label); err != nil {
			return nil, err
		}
		idLabel[id] = label
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	type patternKey struct{ src, rel, tgt string }
	patternCounts := make(map[patternKey]int)
	rows2, err := s.q.Query("SELECT source_id, target_id, type FROM edges WHERE project=?", project)
	if err != nil {
		return nil, fmt.Errorf("schema edge scan: %w", err)
	}
	defer rows2.Close()
	for rows2.Next() {
		var srcID, tgtID int64
		var edgeType string
		if err := rows2.Scan(&srcID, &tgtID, &edgeType); err != nil {
			return nil, err
		}
		pk := patternKey{src: idLabel[srcID], rel: edgeType, tgt: idLabel[tgtID]}
		patternCounts[pk]++
	}
	if err := rows2.Err(); err != nil {
		return nil, err
	}

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
	patterns := make([]string, 0, len(entries))
	for _, e := range entries {
		patterns = append(patterns, fmt.Sprintf("(:%s)-[:%s]->(:%s)  [%dx]", e.key.src, e.key.rel, e.key.tgt, e.cnt))
	}
	return patterns, nil
}

func (s *Store) schemaSampleNames(project, label string, limit int) ([]string, error) {
	rows, err := s.q.Query("SELECT name FROM nodes WHERE project=? AND label=? ORDER BY name LIMIT ?", project, label, limit)
	if err != nil {
		return nil, fmt.Errorf("schema sample %s: %w", label, err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

func (s *Store) schemaSampleQNs(project string) ([]string, error) {
	rows, err := s.q.Query("SELECT qualified_name FROM nodes WHERE project=? LIMIT 5", project)
	if err != nil {
		return nil, fmt.Errorf("schema sample qns: %w", err)
	}
	defer rows.Close()
	var qns []string
	for rows.Next() {
		var qn string
		if err := rows.Scan(&qn); err != nil {
			return nil, err
		}
		qns = append(qns, qn)
	}
	return qns, rows.Err()
}
