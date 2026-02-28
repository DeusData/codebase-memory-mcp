package store

import (
	"fmt"
	"regexp"
	"slices"
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
	SortBy             string   // "relevance" (default), "name", "degree"
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

	// When a name pattern is provided, try to extract literal substrings for SQL LIKE
	// pre-filtering. This drastically reduces the rows Go needs to regex-match.
	// The full regex is still applied in Go for correctness.
	nameHasLikeHints := false
	if params.NamePattern != "" {
		hints := extractLikeHints(params.NamePattern)
		if len(hints) > 0 {
			nameHasLikeHints = true
			var likeParts []string
			for _, hint := range hints {
				likeVal := "%" + hint + "%"
				likeParts = append(likeParts, "(n.name LIKE ? OR n.qualified_name LIKE ?)")
				args = append(args, likeVal, likeVal)
			}
			conditions = append(conditions, "("+strings.Join(likeParts, " AND ")+")")
		}
	}

	where := strings.Join(conditions, " AND ")

	// When Go-side filtering is needed (regex, degree), we must scan all
	// qualifying rows since matching nodes may appear at any position.
	// However, if we pushed LIKE hints into SQL, the result set is already
	// narrowed and we don't need the full 200K scan for name patterns.
	hasDegreeFilter := params.MinDegree >= 0 || params.MaxDegree >= 0
	needsNameScan := params.NamePattern != "" && !nameHasLikeHints
	var sqlLimit int
	if needsNameScan || hasDegreeFilter {
		sqlLimit = 200000 // must cover full dataset for accurate Go-side filtering
	} else {
		// Scan beyond offset+limit so total/has_more are accurate.
		// The +1000 buffer ensures has_more is correct for result sets
		// up to ~1000 beyond the requested page, at negligible cost
		// (~2 extra batch degree queries ≈ 20ms worst case).
		sqlLimit = params.Offset + params.Limit + 1000
		if sqlLimit > 200000 {
			sqlLimit = 200000
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

	// Sort results before pagination
	sortBy := params.SortBy
	if sortBy == "" {
		sortBy = "relevance"
	}
	sortSearchResults(allResults, sortBy, params.NamePattern)

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

// degreePair stores in-degree and out-degree for a node.
type degreePair struct {
	InDegree  int
	OutDegree int
}

// batchCountDegrees counts in/out degrees for a batch of node IDs.
// Returns map[nodeID] → degreePair. Respects the 999-var limit.
func (s *Store) batchCountDegrees(nodeIDs []int64, relationship string) (map[int64]degreePair, error) {
	if len(nodeIDs) == 0 {
		return map[int64]degreePair{}, nil
	}

	result := make(map[int64]degreePair, len(nodeIDs))
	const maxIDsPerQuery = 998

	for i := 0; i < len(nodeIDs); i += maxIDsPerQuery {
		end := i + maxIDsPerQuery
		if end > len(nodeIDs) {
			end = len(nodeIDs)
		}
		chunk := nodeIDs[i:end]

		placeholders := make([]string, len(chunk))
		inArgs := make([]any, len(chunk))
		for j, id := range chunk {
			placeholders[j] = "?"
			inArgs[j] = id
		}
		inClause := strings.Join(placeholders, ",")

		// Count inbound (target_id)
		var inQuery string
		var inQueryArgs []any
		if relationship != "" {
			inQuery = fmt.Sprintf("SELECT target_id, COUNT(*) FROM edges WHERE target_id IN (%s) AND type=? GROUP BY target_id", inClause)
			inQueryArgs = append(append([]any{}, inArgs...), relationship)
		} else {
			inQuery = fmt.Sprintf("SELECT target_id, COUNT(*) FROM edges WHERE target_id IN (%s) GROUP BY target_id", inClause)
			inQueryArgs = inArgs
		}

		if err := s.scanDegrees(inQuery, inQueryArgs, result, true); err != nil {
			return nil, fmt.Errorf("batch count in-degree: %w", err)
		}

		// Count outbound (source_id)
		var outQuery string
		var outQueryArgs []any
		if relationship != "" {
			outQuery = fmt.Sprintf("SELECT source_id, COUNT(*) FROM edges WHERE source_id IN (%s) AND type=? GROUP BY source_id", inClause)
			outQueryArgs = append(append([]any{}, inArgs...), relationship)
		} else {
			outQuery = fmt.Sprintf("SELECT source_id, COUNT(*) FROM edges WHERE source_id IN (%s) GROUP BY source_id", inClause)
			outQueryArgs = inArgs
		}

		if err := s.scanDegrees(outQuery, outQueryArgs, result, false); err != nil {
			return nil, fmt.Errorf("batch count out-degree: %w", err)
		}
	}

	return result, nil
}

// scanDegrees runs a degree-count query and populates the result map.
// If inbound is true, sets InDegree; otherwise sets OutDegree.
func (s *Store) scanDegrees(query string, args []any, result map[int64]degreePair, inbound bool) error {
	rows, err := s.q.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var count int
		if err := rows.Scan(&id, &count); err != nil {
			return err
		}
		dp := result[id]
		if inbound {
			dp.InDegree = count
		} else {
			dp.OutDegree = count
		}
		result[id] = dp
	}
	return rows.Err()
}

// buildFilteredResults applies degree, direction, and entry-point filters to nodes,
// counts degrees, and loads connected names for each qualifying result.
func (s *Store) buildFilteredResults(nodes []*Node, params *SearchParams) ([]*SearchResult, error) {
	// Batch count degrees for all nodes at once
	nodeIDs := make([]int64, len(nodes))
	for i, n := range nodes {
		nodeIDs[i] = n.ID
	}

	degrees, err := s.batchCountDegrees(nodeIDs, params.Relationship)
	if err != nil {
		return nil, err
	}

	results := make([]*SearchResult, 0, len(nodes))
	for _, n := range nodes {
		dp := degrees[n.ID]
		sr := &SearchResult{
			Node:      n,
			InDegree:  dp.InDegree,
			OutDegree: dp.OutDegree,
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

// extractLikeHints extracts literal substrings from a regex pattern for SQL LIKE pre-filtering.
// Returns substrings that MUST appear in matching text (concatenated via .* or similar).
// Conservative: returns nil for patterns with alternation (|) since AND LIKE is incorrect for OR semantics.
func extractLikeHints(pattern string) []string {
	// Bail out on alternation — can't safely convert OR regex to AND LIKE
	if strings.Contains(pattern, "|") {
		return nil
	}

	var hints []string
	var current strings.Builder
	i := 0
	for i < len(pattern) {
		ch := pattern[i]
		switch ch {
		case '\\':
			// Escaped character — the next char is literal
			if i+1 < len(pattern) {
				current.WriteByte(pattern[i+1])
				i += 2
			} else {
				i++
			}
		case '.', '*', '+', '?', '^', '$', '(', ')', '[', ']', '{', '}':
			// Meta character — flush current literal segment
			if current.Len() >= 3 { // only use hints >= 3 chars to be selective
				hints = append(hints, current.String())
			}
			current.Reset()
			i++
		default:
			current.WriteByte(ch)
			i++
		}
	}
	if current.Len() >= 3 {
		hints = append(hints, current.String())
	}
	return hints
}

// sortSearchResults sorts results based on the requested sort mode.
func sortSearchResults(results []*SearchResult, sortBy, namePattern string) {
	switch sortBy {
	case "name":
		slices.SortStableFunc(results, func(a, b *SearchResult) int {
			return strings.Compare(a.Node.Name, b.Node.Name)
		})
	case "degree":
		slices.SortStableFunc(results, func(a, b *SearchResult) int {
			return (b.InDegree + b.OutDegree) - (a.InDegree + a.OutDegree) // descending
		})
	default: // "relevance"
		literalQuery := extractLiteralQuery(namePattern)
		lowerQuery := strings.ToLower(literalQuery)
		slices.SortStableFunc(results, func(a, b *SearchResult) int {
			return compareRelevance(a, b, literalQuery, lowerQuery)
		})
	}
}

// compareRelevance implements tiered relevance: exact match > prefix match > degree.
func compareRelevance(a, b *SearchResult, literalQuery, lowerQuery string) int {
	if literalQuery == "" {
		return compareDegree(a, b)
	}
	// Tier 1: Exact name match (case-insensitive)
	if cmp := compareBool(strings.EqualFold(a.Node.Name, literalQuery), strings.EqualFold(b.Node.Name, literalQuery)); cmp != 0 {
		return cmp
	}
	// Tier 2: Prefix match
	if cmp := compareBool(strings.HasPrefix(strings.ToLower(a.Node.Name), lowerQuery), strings.HasPrefix(strings.ToLower(b.Node.Name), lowerQuery)); cmp != 0 {
		return cmp
	}
	return compareDegree(a, b)
}

// compareBool returns -1 if a is true and b is false, 1 if opposite, 0 if equal.
func compareBool(a, b bool) int {
	if a == b {
		return 0
	}
	if a {
		return -1
	}
	return 1
}

// compareDegree compares total degree descending (higher degree first).
func compareDegree(a, b *SearchResult) int {
	return (b.InDegree + b.OutDegree) - (a.InDegree + a.OutDegree)
}

// extractLiteralQuery returns the literal string from a regex pattern if it contains
// no special regex syntax beyond leading/trailing .* anchors and (?i) flag.
func extractLiteralQuery(pattern string) string {
	if pattern == "" {
		return ""
	}
	p := strings.TrimPrefix(pattern, "(?i)")
	p = strings.TrimPrefix(p, ".*")
	p = strings.TrimSuffix(p, ".*")
	if strings.ContainsAny(p, ".*+?^${}()|[]\\") {
		return ""
	}
	return p
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
