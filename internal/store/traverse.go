package store

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

// BFS performs breadth-first traversal following edges of given types.
// direction: "outbound" follows source->target, "inbound" follows target->source.
// maxDepth caps the BFS depth, maxResults caps total visited nodes.
func (s *Store) BFS(startNodeID int64, direction string, edgeTypes []string, maxDepth, maxResults int) (*TraverseResult, error) {
	if maxDepth <= 0 {
		maxDepth = 3
	}
	if maxResults <= 0 {
		maxResults = 200
	}

	result := &TraverseResult{}
	visited := make(map[int64]int) // nodeID -> hop
	visited[startNodeID] = 0

	type queueItem struct {
		nodeID int64
		hop    int
	}
	queue := []queueItem{{startNodeID, 0}}

	for len(queue) > 0 && len(result.Visited) < maxResults {
		item := queue[0]
		queue = queue[1:]

		if item.hop >= maxDepth {
			continue
		}

		// Get edges from this node
		var edges []*Edge
		for _, et := range edgeTypes {
			var found []*Edge
			var err error
			if direction == "outbound" {
				found, err = s.FindEdgesBySourceAndType(item.nodeID, et)
			} else {
				found, err = s.FindEdgesByTargetAndType(item.nodeID, et)
			}
			if err != nil {
				return nil, err
			}
			edges = append(edges, found...)
		}

		for _, e := range edges {
			var nextID int64
			if direction == "outbound" {
				nextID = e.TargetID
			} else {
				nextID = e.SourceID
			}

			if _, seen := visited[nextID]; !seen {
				visited[nextID] = item.hop + 1

				row := s.db.QueryRow(`SELECT id, project, label, name, qualified_name, file_path, start_line, end_line, properties
					FROM nodes WHERE id=?`, nextID)
				nextNode, err := scanNode(row)
				if err != nil || nextNode == nil {
					continue
				}

				result.Visited = append(result.Visited, &NodeHop{Node: nextNode, Hop: item.hop + 1})
				queue = append(queue, queueItem{nextID, item.hop + 1})

				if len(result.Visited) >= maxResults {
					break
				}
			}

			// Record edge info
			var fromName, toName string
			s.db.QueryRow("SELECT name FROM nodes WHERE id=?", e.SourceID).Scan(&fromName)
			s.db.QueryRow("SELECT name FROM nodes WHERE id=?", e.TargetID).Scan(&toName)
			result.Edges = append(result.Edges, EdgeInfo{FromName: fromName, ToName: toName, Type: e.Type})
		}
	}

	return result, nil
}
