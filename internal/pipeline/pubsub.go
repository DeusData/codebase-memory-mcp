package pipeline

import (
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
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

// Per-language regex patterns for extracting event names from Publish/Subscribe calls.
// Each pattern must have exactly one capture group for the event name.
var publishEventPatterns = []*regexp.Regexp{
	// Go: bus.Publish(events.EventCheckinCompleted, ...) or bus.Publish(EventCheckinCompleted, ...)
	regexp.MustCompile(`\.(?i:Publish|Emit|Dispatch|Fire|Broadcast)\(\s*(?:\w+\.)?(\w+)`),
	// JS/TS: emitter.emit('event-name', ...) or emitter.emit("event-name", ...)
	regexp.MustCompile(`\.(?i:emit|dispatch|fire|broadcast)\(\s*['"]([^'"]+)['"]`),
}

var subscribeEventPatterns = []*regexp.Regexp{
	// Go: bus.Subscribe(events.EventCheckinCompleted, func(...) { ... })
	regexp.MustCompile(`\.(?i:Subscribe|AddListener|Listen)\(\s*(?:\w+\.)?(\w+)`),
	// JS/TS: emitter.addListener('event-name', handler) or emitter.subscribe('event-name', handler)
	regexp.MustCompile(`\.(?i:addListener|listen|subscribe)\(\s*['"]([^'"]+)['"]`),
}

// subscribeCallLine holds a Subscribe call's event name and its source line number.
type subscribeCallLine struct {
	eventName string
	line      int
}

// passPubSubLinks detects in-process event bus patterns and creates ASYNC_CALLS
// edges from publisher functions to the handler functions called by subscribers.
//
// Algorithm (event-routed):
//  1. Find all CALLS edges whose target is a known publish/subscribe method name.
//  2. For each publisher function, read its source and extract event names from Publish calls.
//  3. For each subscriber function, read its source and extract (eventName, line) pairs from
//     Subscribe calls. Attribute handler CALLS edges to the nearest preceding Subscribe call.
//  4. Create ASYNC_CALLS edges routed by event name: publisher → event → handlers.
//
// This replaces the previous cartesian-product approach which connected every publisher
// to every handler regardless of event type.
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
	// (excluding Publish/Subscribe themselves and logging functions).
	// These are the actual event handler functions.
	excludeNames := map[string]bool{
		"error": true, "warn": true, "info": true, "debug": true,
		"printf": true, "println": true, "sprintf": true, "errorf": true,
	}

	// subscriberHandlerCalls: subscriberID -> []handlerNodeID (all handler calls from that function)
	subscriberHandlerCalls := make(map[int64][]int64)
	for _, e := range callEdges {
		if !subscriberIDs[e.SourceID] {
			continue
		}
		target := nodeLookup[e.TargetID]
		if target == nil {
			continue
		}
		if pubsubTargetQNs[target.QualifiedName] {
			continue
		}
		if excludeNames[strings.ToLower(target.Name)] {
			continue
		}
		subscriberHandlerCalls[e.SourceID] = append(subscriberHandlerCalls[e.SourceID], e.TargetID)
	}

	// --- Event-routed matching via source scanning ---

	fileCache := &sourceFileCache{files: make(map[string]string)}

	// Step 1: Build publisher event map: publisherNodeID -> []eventName
	publisherEvents := make(map[int64][]string)
	for pubID := range publisherIDs {
		node := nodeLookup[pubID]
		if node == nil || node.FilePath == "" {
			continue
		}
		src, err := fileCache.readLines(p.RepoPath, node.FilePath, node.StartLine, node.EndLine)
		if err != nil {
			slog.Debug("pubsub.read_publisher", "file", node.FilePath, "err", err)
			continue
		}
		events := extractEventNames(src, publishEventPatterns)
		if len(events) > 0 {
			publisherEvents[pubID] = events
			slog.Debug("pubsub.publisher_events", "func", node.Name, "events", events)
		}
	}

	// Step 2: Build subscriber event-to-handler map: eventName -> []handlerNodeID
	eventHandlers := make(map[string][]int64)
	for subID := range subscriberIDs {
		node := nodeLookup[subID]
		if node == nil || node.FilePath == "" {
			continue
		}
		handlers := subscriberHandlerCalls[subID]
		if len(handlers) == 0 {
			continue
		}
		src, err := fileCache.readLines(p.RepoPath, node.FilePath, node.StartLine, node.EndLine)
		if err != nil {
			slog.Debug("pubsub.read_subscriber", "file", node.FilePath, "err", err)
			continue
		}

		// Build handler name -> node ID map for this subscriber's handlers
		handlerNameToIDs := make(map[string][]int64)
		for _, hID := range handlers {
			hNode := nodeLookup[hID]
			if hNode != nil {
				handlerNameToIDs[hNode.Name] = append(handlerNameToIDs[hNode.Name], hID)
			}
		}

		attribution := attributeHandlersToEvents(src, subscribeEventPatterns, handlerNameToIDs)
		for eventName, hIDs := range attribution {
			eventHandlers[eventName] = append(eventHandlers[eventName], hIDs...)
			slog.Debug("pubsub.subscriber_event", "func", node.Name, "event", eventName, "handlers", len(hIDs))
		}
	}

	// Deduplicate handlers per event
	for evt, hIDs := range eventHandlers {
		eventHandlers[evt] = deduplicateInt64(hIDs)
	}

	// Step 3: Create event-routed ASYNC_CALLS edges
	var edges []*store.Edge
	seen := make(map[[2]int64]bool)
	var fallbackCount int

	for pubID := range publisherIDs {
		pubEvents := publisherEvents[pubID]
		if len(pubEvents) == 0 {
			// Fallback: publisher has no extracted events -> connect to all handlers with low confidence
			for evt, hIDs := range eventHandlers {
				for _, hID := range hIDs {
					if pubID == hID {
						continue
					}
					key := [2]int64{pubID, hID}
					if seen[key] {
						continue
					}
					seen[key] = true
					fallbackCount++

					handlerNode := nodeLookup[hID]
					handlerName := ""
					if handlerNode != nil {
						handlerName = handlerNode.Name
					}
					edges = append(edges, &store.Edge{
						Project:  p.ProjectName,
						SourceID: pubID,
						TargetID: hID,
						Type:     "ASYNC_CALLS",
						Properties: map[string]any{
							"handler_name":    handlerName,
							"event_name":      evt,
							"confidence":      0.5,
							"confidence_band": "medium",
							"async_type":      "event_bus",
							"fallback":        true,
						},
					})
				}
			}
			continue
		}

		for _, evt := range pubEvents {
			hIDs := eventHandlers[evt]
			for _, hID := range hIDs {
				if pubID == hID {
					continue
				}
				key := [2]int64{pubID, hID}
				if seen[key] {
					continue
				}
				seen[key] = true

				handlerNode := nodeLookup[hID]
				handlerName := ""
				if handlerNode != nil {
					handlerName = handlerNode.Name
				}
				edges = append(edges, &store.Edge{
					Project:  p.ProjectName,
					SourceID: pubID,
					TargetID: hID,
					Type:     "ASYNC_CALLS",
					Properties: map[string]any{
						"handler_name":    handlerName,
						"event_name":      evt,
						"confidence":      0.9,
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
		"event_types", len(eventHandlers),
		"edges_created", len(edges),
		"fallback_edges", fallbackCount,
		"elapsed", time.Since(t),
	)
}

// extractEventNames extracts event constant names from source code using the given patterns.
// Each pattern must have exactly one capture group for the event name.
// Returns a deduplicated slice of event names found.
func extractEventNames(source string, patterns []*regexp.Regexp) []string {
	seen := make(map[string]bool)
	var result []string
	for _, pat := range patterns {
		matches := pat.FindAllStringSubmatch(source, -1)
		for _, m := range matches {
			if len(m) >= 2 && m[1] != "" {
				name := m[1]
				if !seen[name] {
					seen[name] = true
					result = append(result, name)
				}
			}
		}
	}
	return result
}

// attributeHandlersToEvents scans subscriber function source to attribute handler calls
// to specific events. It extracts (eventName, line) pairs from Subscribe calls, then
// for each handler function name, finds its call site(s) in the source and attributes
// each to the nearest preceding Subscribe call.
//
// This correctly handles the pattern where one function (e.g. RegisterListeners) calls
// Subscribe multiple times with different events and different inline handlers.
func attributeHandlersToEvents(source string, patterns []*regexp.Regexp, handlerNameToIDs map[string][]int64) map[string][]int64 {
	result := make(map[string][]int64)

	// Extract Subscribe call sites: (eventName, lineNumber)
	subCalls := extractSubscribeCallLines(source, patterns)
	if len(subCalls) == 0 {
		return result
	}

	lines := strings.Split(source, "\n")

	// For each handler name, find its call sites in the source and attribute
	// to the nearest preceding Subscribe call.
	for handlerName, hIDs := range handlerNameToIDs {
		// Find the handler name followed by '(' to confirm it's a call site,
		// not a substring of another identifier or a string/comment.
		for lineIdx, line := range lines {
			lineNum := lineIdx + 1 // 1-based
			idx := strings.Index(line, handlerName)
			if idx < 0 {
				continue
			}
			endIdx := idx + len(handlerName)
			if endIdx >= len(line) || line[endIdx] != '(' {
				continue
			}
			// Verify it looks like a call, not just a comment or string
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "/*") {
				continue
			}

			// Attribute to nearest preceding Subscribe call
			bestEvent := findNearestPrecedingEvent(subCalls, lineNum)
			if bestEvent != "" {
				result[bestEvent] = append(result[bestEvent], hIDs...)
			}
		}
	}

	// Deduplicate handler IDs per event
	for evt, hIDs := range result {
		result[evt] = deduplicateInt64(hIDs)
	}

	return result
}

// extractSubscribeCallLines extracts (eventName, lineNumber) pairs from Subscribe calls
// in the given source string.
func extractSubscribeCallLines(source string, patterns []*regexp.Regexp) []subscribeCallLine {
	var calls []subscribeCallLine
	lines := strings.Split(source, "\n")
	for lineIdx, line := range lines {
		for _, pat := range patterns {
			m := pat.FindStringSubmatch(line)
			if len(m) >= 2 && m[1] != "" {
				calls = append(calls, subscribeCallLine{
					eventName: m[1],
					line:      lineIdx + 1, // 1-based
				})
			}
		}
	}
	// Sort by line number (should already be ordered, but be safe)
	sort.Slice(calls, func(i, j int) bool {
		return calls[i].line < calls[j].line
	})
	return calls
}

// findNearestPrecedingEvent finds the event name of the Subscribe call that is
// closest to (and before or on) the given line number.
func findNearestPrecedingEvent(subCalls []subscribeCallLine, lineNum int) string {
	best := ""
	bestLine := 0
	for _, sc := range subCalls {
		if sc.line <= lineNum && sc.line > bestLine {
			best = sc.eventName
			bestLine = sc.line
		}
	}
	return best
}

// sourceFileCache caches file contents to avoid re-reading the same file multiple times.
type sourceFileCache struct {
	mu    sync.Mutex
	files map[string]string // absPath -> file contents
}

// readLines reads the given file and returns the lines in the range [startLine, endLine] (1-based, inclusive).
// File contents are cached.
func (c *sourceFileCache) readLines(repoPath, relPath string, startLine, endLine int) (string, error) {
	absPath := filepath.Join(repoPath, relPath)

	c.mu.Lock()
	content, ok := c.files[absPath]
	c.mu.Unlock()

	if !ok {
		data, err := os.ReadFile(absPath)
		if err != nil {
			return "", err
		}
		content = string(data)
		c.mu.Lock()
		c.files[absPath] = content
		c.mu.Unlock()
	}

	if startLine <= 0 || endLine <= 0 {
		return content, nil
	}

	lines := strings.Split(content, "\n")
	if startLine > len(lines) {
		return "", nil
	}
	if endLine > len(lines) {
		endLine = len(lines)
	}
	// Convert to 0-based indexing
	return strings.Join(lines[startLine-1:endLine], "\n"), nil
}

// deduplicateInt64 returns a new slice with duplicate values removed, preserving order.
func deduplicateInt64(ids []int64) []int64 {
	seen := make(map[int64]bool, len(ids))
	result := make([]int64, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}
	return result
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
