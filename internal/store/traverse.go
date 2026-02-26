package store

import (
	"fmt"
	"strings"
)

// TraverseResult holds BFS traversal results.
type TraverseResult struct {
	Root    *Node
	Visited []*NodeHop
	Edges   []EdgeInfo
}

// NodeHop is a node with its BFS hop distance.
type NodeHop struct {
	Node *Node
	Hop  int
}

// EdgeInfo is a simplified edge for output.
type EdgeInfo struct {
	FromName string
	ToName   string
	Type     string
}

// BFS performs breadth-first traversal following edges of given types using a
// recursive CTE, replacing the previous per-node Go-side loop with a single
// SQL round-trip.
// direction: "outbound" follows source->target, "inbound" follows target->source.
// maxDepth caps the BFS depth, maxResults caps total visited nodes.
func (s *Store) BFS(startNodeID int64, direction string, edgeTypes []string, maxDepth, maxResults int) (*TraverseResult, error) {
	if maxDepth <= 0 {
		maxDepth = 3
	}
	if maxResults <= 0 {
		maxResults = 200
	}
	if len(edgeTypes) == 0 {
		edgeTypes = []string{"CALLS"}
	}

	// Build type filter placeholders
	typePlaceholders := make([]string, len(edgeTypes))
	typeArgs := make([]any, len(edgeTypes))
	for i, et := range edgeTypes {
		typePlaceholders[i] = "?"
		typeArgs[i] = et
	}
	typeFilter := strings.Join(typePlaceholders, ",")

	// Determine join columns based on direction
	var joinCol, nextCol string
	if direction == "inbound" {
		joinCol, nextCol = "target_id", "source_id"
	} else {
		joinCol, nextCol = "source_id", "target_id"
	}

	// Recursive CTE: traverse edges up to maxDepth hops, collect node IDs + edges
	query := fmt.Sprintf(`
		WITH RECURSIVE bfs(node_id, hop) AS (
			SELECT ?, 0
			UNION ALL
			SELECT e.%s, b.hop + 1
			FROM bfs b
			JOIN edges e ON e.%s = b.node_id AND e.type IN (%s)
			WHERE b.hop < ?
		)
		SELECT DISTINCT n.id, n.project, n.label, n.name, n.qualified_name,
			n.file_path, n.start_line, n.end_line, n.properties, bfs.hop
		FROM bfs
		JOIN nodes n ON n.id = bfs.node_id
		WHERE bfs.hop > 0
		ORDER BY bfs.hop, n.name
		LIMIT ?`,
		nextCol, joinCol, typeFilter)

	// Build args: startNodeID, ...typeArgs, maxDepth, maxResults
	args := make([]any, 0, 3+len(typeArgs))
	args = append(args, startNodeID)
	args = append(args, typeArgs...)
	args = append(args, maxDepth, maxResults)

	rows, err := s.q.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("bfs cte: %w", err)
	}
	defer rows.Close()

	result := &TraverseResult{}
	for rows.Next() {
		var n Node
		var props string
		var hop int
		if err := rows.Scan(&n.ID, &n.Project, &n.Label, &n.Name, &n.QualifiedName,
			&n.FilePath, &n.StartLine, &n.EndLine, &props, &hop); err != nil {
			return nil, err
		}
		n.Properties = unmarshalProps(props)
		result.Visited = append(result.Visited, &NodeHop{Node: &n, Hop: hop})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Collect edge info with a second CTE query that returns actual edges
	edgeQuery := fmt.Sprintf(`
		WITH RECURSIVE bfs(node_id, hop) AS (
			SELECT ?, 0
			UNION ALL
			SELECT e.%s, b.hop + 1
			FROM bfs b
			JOIN edges e ON e.%s = b.node_id AND e.type IN (%s)
			WHERE b.hop < ?
		)
		SELECT DISTINCT src.name, tgt.name, e.type
		FROM bfs b
		JOIN edges e ON e.%s = b.node_id AND e.type IN (%s)
		JOIN nodes src ON src.id = e.source_id
		JOIN nodes tgt ON tgt.id = e.target_id
		WHERE b.hop < ?`,
		nextCol, joinCol, typeFilter,
		joinCol, typeFilter)

	// Build edge args: startNodeID, ...typeArgs, maxDepth, ...typeArgs, maxDepth
	edgeArgs := make([]any, 0, 4+2*len(typeArgs))
	edgeArgs = append(edgeArgs, startNodeID)
	edgeArgs = append(edgeArgs, typeArgs...)
	edgeArgs = append(edgeArgs, maxDepth)
	edgeArgs = append(edgeArgs, typeArgs...)
	edgeArgs = append(edgeArgs, maxDepth)

	edgeRows, err := s.q.Query(edgeQuery, edgeArgs...)
	if err != nil {
		return nil, fmt.Errorf("bfs edges: %w", err)
	}
	defer edgeRows.Close()

	for edgeRows.Next() {
		var ei EdgeInfo
		if err := edgeRows.Scan(&ei.FromName, &ei.ToName, &ei.Type); err != nil {
			return nil, err
		}
		result.Edges = append(result.Edges, ei)
	}

	return result, edgeRows.Err()
}
