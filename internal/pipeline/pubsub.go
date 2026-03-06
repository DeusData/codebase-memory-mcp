package pipeline

import (
	"log/slog"
	"strings"
	"time"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// publishMethodNames are method names that indicate event publishing (case-insensitive).
// Only high-signal names are included to avoid false positives with generic methods.
var publishMethodNames = map[string]bool{
	"publish":   true,
	"emit":      true,
	"dispatch":  true,
	"fire":      true,
	"broadcast": true,
}

// subscribeMethodNames are method names that indicate event subscribing (case-insensitive).
var subscribeMethodNames = map[string]bool{
	"subscribe":   true,
	"addlistener": true,
	"listen":      true,
}

// passPubSubLinks detects in-process event bus patterns and creates ASYNC_CALLS
// edges from publisher functions to the handler functions called by subscribers.
//
// Algorithm:
//  1. Find all CALLS edges whose target is a known publish/subscribe method name.
//  2. For subscribers: collect the other functions they CALL (the actual handlers).
//  3. Create ASYNC_CALLS edges from each publisher to each subscriber handler.
//
// This approach avoids relying on USAGE edges for event constant matching, which
// fails because the C extractor skips identifiers inside call expressions.
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
	publisherIDs := make(map[int64]bool)
	subscriberIDs := make(map[int64]bool)
	// Track the Publish/Subscribe target QNs to exclude them from handler list.
	pubsubTargetQNs := make(map[string]bool)

	for _, e := range callEdges {
		target := nodeLookup[e.TargetID]
		if target == nil {
			continue
		}
		nameLower := strings.ToLower(target.Name)
		if publishMethodNames[nameLower] {
			publisherIDs[e.SourceID] = true
			pubsubTargetQNs[target.QualifiedName] = true
		} else if subscribeMethodNames[nameLower] {
			subscriberIDs[e.SourceID] = true
			pubsubTargetQNs[target.QualifiedName] = true
		}
	}

	if len(publisherIDs) == 0 || len(subscriberIDs) == 0 {
		slog.Info("pubsub.skip", "publishers", len(publisherIDs), "subscribers", len(subscriberIDs))
		return
	}

	// For each subscriber function, collect the OTHER functions it calls
	// (excluding Publish/Subscribe themselves and error handlers).
	// These are the actual event handler functions.
	subscriberHandlers := make(map[int64][]int64) // subscriberID → []handlerNodeID
	excludeNames := map[string]bool{
		"error": true, "warn": true, "info": true, "debug": true,
		"printf": true, "println": true, "sprintf": true, "errorf": true,
	}

	for _, e := range callEdges {
		if !subscriberIDs[e.SourceID] {
			continue
		}
		target := nodeLookup[e.TargetID]
		if target == nil {
			continue
		}
		// Skip the pub/sub methods themselves
		if pubsubTargetQNs[target.QualifiedName] {
			continue
		}
		// Skip logging/error utility functions
		if excludeNames[strings.ToLower(target.Name)] {
			continue
		}
		subscriberHandlers[e.SourceID] = append(subscriberHandlers[e.SourceID], e.TargetID)
	}

	// Create ASYNC_CALLS edges: from each publisher to each handler function
	// called by any subscriber.
	var edges []*store.Edge
	seen := make(map[[2]int64]bool)

	for pubID := range publisherIDs {
		for _, handlers := range subscriberHandlers {
			for _, handlerID := range handlers {
				if pubID == handlerID {
					continue
				}
				key := [2]int64{pubID, handlerID}
				if seen[key] {
					continue
				}
				seen[key] = true

				handlerNode := nodeLookup[handlerID]
				handlerName := ""
				if handlerNode != nil {
					handlerName = handlerNode.Name
				}

				edges = append(edges, &store.Edge{
					Project:  p.ProjectName,
					SourceID: pubID,
					TargetID: handlerID,
					Type:     "ASYNC_CALLS",
					Properties: map[string]any{
						"handler_name":    handlerName,
						"confidence":      0.7,
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
		"handlers", len(subscriberHandlers),
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

