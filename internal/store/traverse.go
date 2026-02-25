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

type bfsQueue struct {
	nodeID int64
	hop    int
}

// fetchEdgesForNode retrieves edges from a node in the given direction and edge types.
func (s *Store) fetchEdgesForNode(nodeID int64, direction string, edgeTypes []string) ([]*Edge, error) {
	var edges []*Edge
	for _, et := range edgeTypes {
		var found []*Edge
		var err error
		if direction == "outbound" {
			found, err = s.FindEdgesBySourceAndType(nodeID, et)
		} else {
			found, err = s.FindEdgesByTargetAndType(nodeID, et)
		}
		if err != nil {
			return nil, err
		}
		edges = append(edges, found...)
	}
	return edges, nil
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
	visited := make(map[int64]int)     // nodeID -> hop
	nodeCache := make(map[int64]*Node) // nodeID -> resolved node
	visited[startNodeID] = 0

	// Cache the start node for edge name resolution
	startNode, err := s.FindNodeByID(startNodeID)
	if err == nil && startNode != nil {
		nodeCache[startNodeID] = startNode
	}

	queue := []bfsQueue{{startNodeID, 0}}

	for len(queue) > 0 && len(result.Visited) < maxResults {
		item := queue[0]
		queue = queue[1:]

		if item.hop >= maxDepth {
			continue
		}

		edges, err := s.fetchEdgesForNode(item.nodeID, direction, edgeTypes)
		if err != nil {
			return nil, err
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

				nextNode, lookupErr := s.FindNodeByID(nextID)
				if lookupErr != nil || nextNode == nil {
					continue
				}
				nodeCache[nextID] = nextNode

				result.Visited = append(result.Visited, &NodeHop{Node: nextNode, Hop: item.hop + 1})
				queue = append(queue, bfsQueue{nextID, item.hop + 1})

				if len(result.Visited) >= maxResults {
					break
				}
			}

			// Record edge info using cached nodes (avoid extra queries)
			fromName := resolveNodeName(nodeCache, s, e.SourceID)
			toName := resolveNodeName(nodeCache, s, e.TargetID)
			result.Edges = append(result.Edges, EdgeInfo{FromName: fromName, ToName: toName, Type: e.Type})
		}
	}

	return result, nil
}

// resolveNodeName returns the name for a node ID, using the cache first.
func resolveNodeName(cache map[int64]*Node, s *Store, id int64) string {
	if n, ok := cache[id]; ok {
		return n.Name
	}
	n, err := s.FindNodeByID(id)
	if err != nil || n == nil {
		return ""
	}
	cache[id] = n
	return n.Name
}
