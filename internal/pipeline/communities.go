package pipeline

import (
	"fmt"
	"log/slog"
	"math"
	"sort"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// passCommunities runs Leiden-style community detection on the CALLS graph
// and creates Community nodes + MEMBER_OF edges.
func (p *Pipeline) passCommunities() {
	slog.Info("pass.communities")

	// Load CALLS edges
	callEdges, err := p.Store.FindEdgesByType(p.ProjectName, "CALLS")
	if err != nil || len(callEdges) == 0 {
		slog.Info("pass.communities.skip", "reason", "no_calls")
		return
	}

	// Build adjacency list (undirected for community detection)
	adj := make(map[int64]map[int64]bool)
	allNodes := make(map[int64]bool)
	for _, e := range callEdges {
		allNodes[e.SourceID] = true
		allNodes[e.TargetID] = true
		if adj[e.SourceID] == nil {
			adj[e.SourceID] = make(map[int64]bool)
		}
		if adj[e.TargetID] == nil {
			adj[e.TargetID] = make(map[int64]bool)
		}
		adj[e.SourceID][e.TargetID] = true
		adj[e.TargetID][e.SourceID] = true
	}

	// Run Louvain/Leiden community detection
	communities := louvainCommunities(adj, allNodes)

	// Create Community nodes + MEMBER_OF edges
	communityCount, memberOfCount := p.storeCommunities(communities)
	slog.Info("pass.communities.done", "communities", communityCount, "member_of", memberOfCount)
}

// louvainCommunities implements a simplified Louvain algorithm for community detection.
// Returns a map of community_id → []node_id.
func louvainCommunities(adj map[int64]map[int64]bool, allNodes map[int64]bool) map[int][]int64 {
	nodeCommunity := make(map[int64]int, len(allNodes))
	communityID := 0
	for nodeID := range allNodes {
		nodeCommunity[nodeID] = communityID
		communityID++
	}

	totalEdges := 0
	for _, neighbors := range adj {
		totalEdges += len(neighbors)
	}
	m := float64(totalEdges) / 2.0
	if m == 0 {
		m = 1
	}

	improved := true
	for iteration := 0; improved && iteration < 50; iteration++ {
		improved = louvainIteration(adj, allNodes, nodeCommunity, m)
	}

	return groupAndFilter(nodeCommunity)
}

// louvainIteration runs one pass of greedy modularity optimization.
// Returns true if any node changed community.
func louvainIteration(adj map[int64]map[int64]bool, allNodes map[int64]bool, nodeCommunity map[int64]int, m float64) bool {
	improved := false
	for nodeID := range allNodes {
		currentComm := nodeCommunity[nodeID]

		neighborComms := make(map[int]bool)
		for neighborID := range adj[nodeID] {
			neighborComms[nodeCommunity[neighborID]] = true
		}

		bestComm, bestGain := currentComm, 0.0
		ki := float64(len(adj[nodeID]))

		for comm := range neighborComms {
			if comm == currentComm {
				continue
			}
			gain := modularityGain(nodeID, comm, adj, nodeCommunity, ki, m)
			if gain > bestGain {
				bestGain = gain
				bestComm = comm
			}
		}

		if bestComm != currentComm && bestGain > 1e-10 {
			nodeCommunity[nodeID] = bestComm
			improved = true
		}
	}
	return improved
}

// modularityGain calculates the gain from moving nodeID to targetComm.
func modularityGain(nodeID int64, targetComm int, adj map[int64]map[int64]bool, nodeCommunity map[int64]int, ki, m float64) float64 {
	kiIn := 0.0
	sumTot := 0.0
	for otherID, otherComm := range nodeCommunity {
		if otherComm != targetComm {
			continue
		}
		if adj[nodeID][otherID] {
			kiIn++
		}
		sumTot += float64(len(adj[otherID]))
	}
	return kiIn/m - ki*sumTot/(2*m*m)
}

// groupAndFilter groups nodes by community and filters out singletons.
func groupAndFilter(nodeCommunity map[int64]int) map[int][]int64 {
	communities := make(map[int][]int64)
	for nodeID, comm := range nodeCommunity {
		communities[comm] = append(communities[comm], nodeID)
	}

	filtered := make(map[int][]int64)
	idx := 0
	for _, members := range communities {
		if len(members) >= 2 {
			filtered[idx] = members
			idx++
		}
	}
	return filtered
}

// storeCommunities creates Community nodes and MEMBER_OF edges in the database.
func (p *Pipeline) storeCommunities(communities map[int][]int64) (communityCount, memberOfCount int) {
	if len(communities) == 0 {
		return 0, 0
	}

	// Collect all member node IDs for batch lookup
	var allMemberIDs []int64
	for _, members := range communities {
		allMemberIDs = append(allMemberIDs, members...)
	}
	nodeMap, _ := p.Store.FindNodesByIDs(allMemberIDs)

	communityNodes := make([]*store.Node, 0, len(communities))
	memberEdges := make([]pendingEdge, 0, len(allMemberIDs))

	for commIdx, memberIDs := range communities {
		// Find top symbols by name for labeling
		topNames := topMemberNames(memberIDs, nodeMap, 5)

		commName := fmt.Sprintf("community_%d", commIdx)
		if len(topNames) > 0 {
			commName = topNames[0] + "_cluster"
		}

		commQN := fmt.Sprintf("%s.__community__.%d", p.ProjectName, commIdx)

		// Calculate cohesion: ratio of internal edges to possible edges
		cohesion := communityCohesion(memberIDs, nodeMap)

		communityNodes = append(communityNodes, &store.Node{
			Project:       p.ProjectName,
			Label:         "Community",
			Name:          commName,
			QualifiedName: commQN,
			Properties: map[string]any{
				"cohesion":     math.Round(cohesion*100) / 100,
				"symbol_count": len(memberIDs),
				"top_symbols":  topNames,
			},
		})

		for _, memberID := range memberIDs {
			memberNode := nodeMap[memberID]
			if memberNode == nil {
				continue
			}
			memberEdges = append(memberEdges, pendingEdge{
				SourceQN: memberNode.QualifiedName,
				TargetQN: commQN,
				Type:     "MEMBER_OF",
			})

			// Also store community_id on the member node (via properties update)
			if memberNode.Properties == nil {
				memberNode.Properties = make(map[string]any)
			}
			memberNode.Properties["community_id"] = commIdx
		}
	}

	// Batch insert community nodes
	idMap, err := p.Store.UpsertNodeBatch(communityNodes)
	if err != nil {
		slog.Warn("pass.communities.upsert.err", "err", err)
		return 0, 0
	}

	// Resolve and insert MEMBER_OF edges
	var edges []*store.Edge
	for _, pe := range memberEdges {
		srcQN := pe.SourceQN
		tgtQN := pe.TargetQN

		srcNode, _ := p.Store.FindNodeByQN(p.ProjectName, srcQN)
		tgtID, tgtOK := idMap[tgtQN]

		if srcNode != nil && tgtOK {
			edges = append(edges, &store.Edge{
				Project:  p.ProjectName,
				SourceID: srcNode.ID,
				TargetID: tgtID,
				Type:     "MEMBER_OF",
			})
		}
	}

	if len(edges) > 0 {
		if err := p.Store.InsertEdgeBatch(edges); err != nil {
			slog.Warn("pass.communities.edges.err", "err", err)
		}
	}

	return len(communityNodes), len(edges)
}

func topMemberNames(memberIDs []int64, nodeMap map[int64]*store.Node, limit int) []string {
	type entry struct {
		name  string
		label string
	}
	var entries []entry
	for _, id := range memberIDs {
		n := nodeMap[id]
		if n != nil {
			entries = append(entries, entry{n.Name, n.Label})
		}
	}

	// Sort: Classes first, then Functions, alphabetical
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].label != entries[j].label {
			// Prefer Class/Interface over Function/Method
			return labelPriority(entries[i].label) < labelPriority(entries[j].label)
		}
		return entries[i].name < entries[j].name
	})

	names := make([]string, 0, limit)
	for i, e := range entries {
		if i >= limit {
			break
		}
		names = append(names, e.name)
	}
	return names
}

func labelPriority(label string) int {
	switch label {
	case "Class":
		return 0
	case "Interface":
		return 1
	case "Type":
		return 2
	case "Function":
		return 3
	case "Method":
		return 4
	default:
		return 5
	}
}

func communityCohesion(memberIDs []int64, nodeMap map[int64]*store.Node) float64 {
	n := len(memberIDs)
	if n < 2 {
		return 1.0
	}
	// Simplified cohesion: proportion of members with known types
	knownCount := 0
	for _, id := range memberIDs {
		if nodeMap[id] != nil {
			knownCount++
		}
	}
	return float64(knownCount) / float64(n)
}
