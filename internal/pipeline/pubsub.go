package pipeline

import (
	"log/slog"
	"strings"
	"time"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// publishMethodNames are method names that indicate event publishing (case-insensitive).
var publishMethodNames = map[string]bool{
	"publish":   true,
	"emit":      true,
	"dispatch":  true,
	"fire":      true,
	"send":      true,
	"notify":    true,
	"trigger":   true,
	"broadcast": true,
}

// subscribeMethodNames are method names that indicate event subscribing (case-insensitive).
var subscribeMethodNames = map[string]bool{
	"subscribe":   true,
	"on":          true,
	"addlistener": true,
	"listen":      true,
	"handle":      true,
	"register":    true,
}

// passPubSubLinks detects in-process event bus patterns (Publish/Subscribe with shared
// event constants) and creates ASYNC_CALLS edges between publisher functions and
// subscriber registration functions.
//
// Algorithm:
//  1. Find all CALLS edges whose target is a known publish/subscribe method name.
//  2. For each such caller, find USAGE edges to identify referenced event constants.
//  3. Match publishers and subscribers that share the same event constant.
//  4. Create ASYNC_CALLS edges from publisher functions to subscriber functions.
func (p *Pipeline) passPubSubLinks() {
	t := time.Now()

	callEdges, err := p.Store.FindEdgesByType(p.ProjectName, "CALLS")
	if err != nil {
		slog.Warn("pubsub.calls_err", "err", err)
		return
	}
	if len(callEdges) == 0 {
		return
	}

	// Collect all node IDs referenced by CALLS edges for batch lookup.
	nodeIDs := collectEdgeNodeIDs(callEdges)
	nodeLookup, err := p.Store.FindNodesByIDs(nodeIDs)
	if err != nil {
		slog.Warn("pubsub.node_lookup_err", "err", err)
		return
	}

	// Partition callers into publishers and subscribers based on target method name.
	// Key: caller node ID, Value: true.
	publisherIDs := make(map[int64]bool)
	subscriberIDs := make(map[int64]bool)

	for _, e := range callEdges {
		target := nodeLookup[e.TargetID]
		if target == nil {
			continue
		}
		nameLower := strings.ToLower(target.Name)
		if publishMethodNames[nameLower] {
			publisherIDs[e.SourceID] = true
		} else if subscribeMethodNames[nameLower] {
			subscriberIDs[e.SourceID] = true
		}
	}

	if len(publisherIDs) == 0 || len(subscriberIDs) == 0 {
		slog.Info("pubsub.skip", "publishers", len(publisherIDs), "subscribers", len(subscriberIDs))
		return
	}

	// Find USAGE edges from publisher and subscriber functions to identify
	// which event constants each references.
	allCandidateIDs := make([]int64, 0, len(publisherIDs)+len(subscriberIDs))
	for id := range publisherIDs {
		allCandidateIDs = append(allCandidateIDs, id)
	}
	for id := range subscriberIDs {
		allCandidateIDs = append(allCandidateIDs, id)
	}

	usageBySource, err := p.Store.FindEdgesBySourceIDs(allCandidateIDs, []string{"USAGE"})
	if err != nil {
		slog.Warn("pubsub.usage_err", "err", err)
		return
	}

	// Collect all USAGE target node IDs so we can resolve their names.
	usageTargetIDs := make([]int64, 0)
	for _, edges := range usageBySource {
		for _, e := range edges {
			usageTargetIDs = append(usageTargetIDs, e.TargetID)
		}
	}
	usageTargetNodes, err := p.Store.FindNodesByIDs(usageTargetIDs)
	if err != nil {
		slog.Warn("pubsub.usage_target_err", "err", err)
		return
	}

	// Build event constant → publisher functions and event constant → subscriber functions.
	// An "event constant" is identified by its qualified name (the USAGE target QN).
	eventToPublishers := make(map[string][]int64)  // eventQN → []publisherNodeID
	eventToSubscribers := make(map[string][]int64) // eventQN → []subscriberNodeID
	eventNames := make(map[string]string)          // eventQN → short name

	for sourceID, edges := range usageBySource {
		for _, e := range edges {
			targetNode := usageTargetNodes[e.TargetID]
			if targetNode == nil {
				continue
			}
			// Only consider Variable nodes (constants) as event identifiers.
			if targetNode.Label != "Variable" {
				continue
			}
			eventQN := targetNode.QualifiedName
			eventNames[eventQN] = targetNode.Name

			if publisherIDs[sourceID] {
				eventToPublishers[eventQN] = append(eventToPublishers[eventQN], sourceID)
			}
			if subscriberIDs[sourceID] {
				eventToSubscribers[eventQN] = append(eventToSubscribers[eventQN], sourceID)
			}
		}
	}

	// Create ASYNC_CALLS edges: for each event constant that has both publishers and
	// subscribers, link every publisher to every subscriber.
	var edges []*store.Edge
	seen := make(map[[2]int64]bool) // dedup (sourceID, targetID) pairs

	for eventQN, pubs := range eventToPublishers {
		subs, ok := eventToSubscribers[eventQN]
		if !ok {
			continue
		}
		eventName := eventNames[eventQN]
		for _, pubID := range pubs {
			for _, subID := range subs {
				if pubID == subID {
					continue // don't self-link
				}
				key := [2]int64{pubID, subID}
				if seen[key] {
					continue
				}
				seen[key] = true

				edges = append(edges, &store.Edge{
					Project:  p.ProjectName,
					SourceID: pubID,
					TargetID: subID,
					Type:     "ASYNC_CALLS",
					Properties: map[string]any{
						"event_name":      eventName,
						"event_qn":        eventQN,
						"confidence":      0.8,
						"confidence_band": "high",
						"async_type":      "event_bus",
					},
				})
			}
		}
	}

	if len(edges) > 0 {
		if err := p.Store.InsertEdgeBatch(edges); err != nil {
			slog.Warn("pubsub.write_err", "err", err)
		}
	}

	slog.Info("pubsub.done",
		"publishers", len(publisherIDs),
		"subscribers", len(subscriberIDs),
		"matched_events", countMatchedEvents(eventToPublishers, eventToSubscribers),
		"edges_created", len(edges),
		"elapsed", time.Since(t),
	)
}

// collectEdgeNodeIDs returns a deduplicated slice of all source and target node IDs
// referenced by the given edges.
func collectEdgeNodeIDs(edges []*store.Edge) []int64 {
	seen := make(map[int64]bool, len(edges)*2)
	for _, e := range edges {
		seen[e.SourceID] = true
		seen[e.TargetID] = true
	}
	ids := make([]int64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids
}

// countMatchedEvents returns the number of event constants that have both
// publishers and subscribers.
func countMatchedEvents(pubs, subs map[string][]int64) int {
	count := 0
	for eventQN := range pubs {
		if _, ok := subs[eventQN]; ok {
			count++
		}
	}
	return count
}
