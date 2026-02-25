package store

import (
	"fmt"
	"regexp"
	"strings"
)

// SearchParams defines structured search parameters.
type SearchParams struct {
	Project            string
	Label              string
	NamePattern        string
	FilePattern        string
	Relationship       string
	Direction          string // "inbound", "outbound", "any"
	MinDegree          int
	MaxDegree          int
	Limit              int
	Offset             int
	ExcludeEntryPoints bool     // when true, exclude nodes with is_entry_point=true
	IncludeConnected   bool     // when true, load connected node names (expensive, off by default)
	ExcludeLabels      []string // labels to exclude from results
}

// SearchResult is a node with edge degree info.
type SearchResult struct {
	Node           *Node
	InDegree       int
	OutDegree      int
	ConnectedNames []string
}

// SearchOutput wraps search results with total count for pagination.
type SearchOutput struct {
	Results []*SearchResult
	Total   int
}

// countDegrees populates in/out degree counts on a SearchResult.
func (s *Store) countDegrees(sr *SearchResult, nodeID int64, relationship string) error {
	if relationship != "" {
		if err := s.q.QueryRow("SELECT COUNT(*) FROM edges WHERE target_id=? AND type=?", nodeID, relationship).Scan(&sr.InDegree); err != nil {
			return fmt.Errorf("count in-degree: %w", err)
		}
		if err := s.q.QueryRow("SELECT COUNT(*) FROM edges WHERE source_id=? AND type=?", nodeID, relationship).Scan(&sr.OutDegree); err != nil {
			return fmt.Errorf("count out-degree: %w", err)
		}
	} else {
		if err := s.q.QueryRow("SELECT COUNT(*) FROM edges WHERE target_id=?", nodeID).Scan(&sr.InDegree); err != nil {
			return fmt.Errorf("count in-degree: %w", err)
		}
		if err := s.q.QueryRow("SELECT COUNT(*) FROM edges WHERE source_id=?", nodeID).Scan(&sr.OutDegree); err != nil {
			return fmt.Errorf("count out-degree: %w", err)
		}
	}
	return nil
}

// loadConnectedNames fetches up to 10 connected node names for display.
func (s *Store) loadConnectedNames(sr *SearchResult, nodeID int64) {
	connRows, connErr := s.q.Query(`
		SELECT DISTINCT n2.name FROM edges e
		JOIN nodes n2 ON (e.target_id = n2.id OR e.source_id = n2.id)
		WHERE (e.source_id = ? OR e.target_id = ?) AND n2.id != ?
		LIMIT 10`, nodeID, nodeID, nodeID)
	if connErr != nil {
		return
	}
	defer connRows.Close()
	for connRows.Next() {
		var name string
		if err := connRows.Scan(&name); err != nil {
			break
		}
		sr.ConnectedNames = append(sr.ConnectedNames, name)
	}
	_ = connRows.Err()
}

// Search executes a parameterized search query with pagination support.
func (s *Store) Search(params *SearchParams) (*SearchOutput, error) {
	// Limit=0 means use default; use a high ceiling for SQL
	if params.Limit <= 0 {
		params.Limit = 100000
	}

	// Build the query dynamically with parameterized values
	var conditions []string
	var args []any

	conditions = append(conditions, "n.project = ?")
	args = append(args, params.Project)

	if params.Label != "" {
		conditions = append(conditions, "n.label = ?")
		args = append(args, params.Label)
	}

	if params.FilePattern != "" {
		// Convert glob to SQL LIKE pattern
		likePattern := globToLike(params.FilePattern)
		conditions = append(conditions, "n.file_path LIKE ?")
		args = append(args, likePattern)
	}

	if len(params.ExcludeLabels) > 0 {
		placeholders := make([]string, len(params.ExcludeLabels))
		for i, label := range params.ExcludeLabels {
			placeholders[i] = "?"
			args = append(args, label)
		}
		conditions = append(conditions, "n.label NOT IN ("+strings.Join(placeholders, ",")+")")
	}

	where := strings.Join(conditions, " AND ")

	// When Go-side filtering is needed (regex, degree), fetch more rows from SQL
	// and apply the user limit after filtering.
	hasDegreeFilter := params.MinDegree >= 0 || params.MaxDegree >= 0
	var sqlLimit int
	if params.NamePattern != "" || hasDegreeFilter {
		sqlLimit = 10000 // fetch enough rows for Go-side filtering
	} else {
		// Fetch enough for offset + limit
		sqlLimit = params.Offset + params.Limit
		if sqlLimit > 100000 {
			sqlLimit = 100000
		}
	}

	query := fmt.Sprintf(`
		SELECT n.id, n.project, n.label, n.name, n.qualified_name, n.file_path, n.start_line, n.end_line, n.properties
		FROM nodes n
		WHERE %s
		LIMIT ?`, where)
	args = append(args, sqlLimit)

	rows, err := s.q.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	defer rows.Close()

	var nodes []*Node
	for rows.Next() {
		var n Node
		var props string
		if err := rows.Scan(&n.ID, &n.Project, &n.Label, &n.Name, &n.QualifiedName, &n.FilePath, &n.StartLine, &n.EndLine, &props); err != nil {
			return nil, err
		}
		n.Properties = unmarshalProps(props)
		nodes = append(nodes, &n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Apply name pattern filter in Go (regex)
	if params.NamePattern != "" {
		nodes, err = filterByNamePattern(nodes, params.NamePattern)
		if err != nil {
			return nil, err
		}
	}

	// Build all qualifying results (before offset/limit) for accurate total count
	allResults, err := s.buildFilteredResults(nodes, params)
	if err != nil {
		return nil, err
	}

	total := len(allResults)

	// Apply offset and limit
	start := params.Offset
	if start > total {
		start = total
	}
	end := start + params.Limit
	if end > total {
		end = total
	}

	return &SearchOutput{
		Results: allResults[start:end],
		Total:   total,
	}, nil
}

// globToLike converts a glob pattern to SQL LIKE pattern.
func globToLike(pattern string) string {
	// Handle **/ (zero-or-more directory prefix) and /** (zero-or-more directory suffix)
	// before single * to avoid double-replacement
	result := strings.ReplaceAll(pattern, "**/", "%")
	result = strings.ReplaceAll(result, "/**", "%")
	result = strings.ReplaceAll(result, "*", "%")
	result = strings.ReplaceAll(result, "?", "_")
	return result
}

// isEntryPoint returns true if a node has is_entry_point=true in its properties.
func isEntryPoint(n *Node) bool {
	if n.Properties == nil {
		return false
	}
	ep, ok := n.Properties["is_entry_point"]
	if !ok {
		return false
	}
	b, ok := ep.(bool)
	return ok && b
}

// buildFilteredResults applies degree, direction, and entry-point filters to nodes,
// counts degrees, and loads connected names for each qualifying result.
func (s *Store) buildFilteredResults(nodes []*Node, params *SearchParams) ([]*SearchResult, error) {
	results := make([]*SearchResult, 0, len(nodes))
	for _, n := range nodes {
		sr := &SearchResult{Node: n}

		if err := s.countDegrees(sr, n.ID, params.Relationship); err != nil {
			return nil, err
		}

		degree := sr.InDegree
		if params.Direction == "outbound" {
			degree = sr.OutDegree
		}
		if params.MinDegree >= 0 && degree < params.MinDegree {
			continue
		}
		if params.MaxDegree >= 0 && degree > params.MaxDegree {
			continue
		}

		if params.ExcludeEntryPoints && isEntryPoint(n) {
			continue
		}

		if params.IncludeConnected {
			s.loadConnectedNames(sr, n.ID)
		}
		results = append(results, sr)
	}
	return results, nil
}

// filterByNamePattern filters nodes by a regex name pattern.
func filterByNamePattern(nodes []*Node, pattern string) ([]*Node, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid name pattern: %w", err)
	}
	var filtered []*Node
	for _, n := range nodes {
		if re.MatchString(n.Name) || re.MatchString(n.QualifiedName) {
			filtered = append(filtered, n)
		}
	}
	return filtered, nil
}
