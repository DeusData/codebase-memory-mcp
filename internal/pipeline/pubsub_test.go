package pipeline

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"
)

// --- extractEventNames tests ---

func TestExtractEventNames_GoPublish(t *testing.T) {
	source := `
	go func() {
		s.EventBus.Publish(events.EventCheckinCompleted, events.CheckinCompletedPayload{
			UserID:  userID,
			EventID: eventID,
		})
	}()
`
	names := extractEventNames(source, publishEventPatterns)
	if len(names) != 1 {
		t.Fatalf("expected 1 event, got %d: %v", len(names), names)
	}
	if names[0] != "EventCheckinCompleted" {
		t.Errorf("expected EventCheckinCompleted, got %s", names[0])
	}
}

func TestExtractEventNames_GoPublishMultiple(t *testing.T) {
	source := `
	s.eventBus.Publish(events.EventUserMentioned, events.UserMentionedPayload{})
	s.eventBus.Publish(events.EventMessageSent, events.MessageSentPayload{})
`
	names := extractEventNames(source, publishEventPatterns)
	if len(names) != 2 {
		t.Fatalf("expected 2 events, got %d: %v", len(names), names)
	}
	sort.Strings(names)
	if names[0] != "EventMessageSent" || names[1] != "EventUserMentioned" {
		t.Errorf("unexpected events: %v", names)
	}
}

func TestExtractEventNames_GoPublishWithoutPackagePrefix(t *testing.T) {
	source := `bus.Publish(EventCheckinCompleted, payload)`
	names := extractEventNames(source, publishEventPatterns)
	if len(names) != 1 || names[0] != "EventCheckinCompleted" {
		t.Errorf("expected [EventCheckinCompleted], got %v", names)
	}
}

func TestExtractEventNames_GoSubscribe(t *testing.T) {
	source := `
	bus.Subscribe(events.EventCheckinCompleted, func(p any) {
		payload, ok := p.(events.CheckinCompletedPayload)
		if !ok { return }
		xpSvc.AwardXP(ctx, payload.UserID, XPActionCheckin)
	})
`
	names := extractEventNames(source, subscribeEventPatterns)
	if len(names) != 1 || names[0] != "EventCheckinCompleted" {
		t.Errorf("expected [EventCheckinCompleted], got %v", names)
	}
}

func TestExtractEventNames_JSEmit(t *testing.T) {
	source := `emitter.emit('user.created', { userId: 123 });`
	names := extractEventNames(source, publishEventPatterns)
	if len(names) != 1 || names[0] != "user.created" {
		t.Errorf("expected [user.created], got %v", names)
	}
}

func TestExtractEventNames_NoMatches(t *testing.T) {
	source := `fmt.Println("hello world")`
	names := extractEventNames(source, publishEventPatterns)
	if len(names) != 0 {
		t.Errorf("expected 0 events, got %v", names)
	}
}

func TestExtractEventNames_Deduplicates(t *testing.T) {
	source := `
	s.EventBus.Publish(events.EventCheckinCompleted, payload1)
	s.EventBus.Publish(events.EventCheckinCompleted, payload2)
`
	names := extractEventNames(source, publishEventPatterns)
	if len(names) != 1 {
		t.Errorf("expected 1 deduplicated event, got %d: %v", len(names), names)
	}
}

// --- attributeHandlersToEvents tests ---

func TestAttributeHandlersToEvents_SingleSubscribe(t *testing.T) {
	source := `bus.Subscribe(events.EventCheckinCompleted, func(p any) {
	payload, ok := p.(events.CheckinCompletedPayload)
	if !ok { return }
	xpSvc.AwardXP(ctx, payload.UserID, XPActionCheckin)
	badgeSvc.CheckAndAwardBadges(ctx, payload.UserID, "checkin")
})`
	handlerNameToIDs := map[string][]int64{
		"AwardXP":            {100},
		"CheckAndAwardBadges": {101},
	}

	result := attributeHandlersToEvents(source, subscribeEventPatterns, handlerNameToIDs)

	handlers, ok := result["EventCheckinCompleted"]
	if !ok {
		t.Fatal("expected EventCheckinCompleted in result")
	}
	sort.Slice(handlers, func(i, j int) bool { return handlers[i] < handlers[j] })
	if len(handlers) != 2 || handlers[0] != 100 || handlers[1] != 101 {
		t.Errorf("expected [100, 101], got %v", handlers)
	}
}

func TestAttributeHandlersToEvents_MultipleSubscribes(t *testing.T) {
	// Simulates RegisterListeners with multiple Subscribe calls
	source := `bus.Subscribe(events.EventCheckinCompleted, func(p any) {
	payload, ok := p.(events.CheckinCompletedPayload)
	if !ok { return }
	xpSvc.AwardXP(ctx, payload.UserID, XPActionCheckin)
	badgeSvc.CheckAndAwardBadges(ctx, payload.UserID, "checkin")
	missionSvc.IncrementMissionProgress(ctx, payload.UserID, "checkin")
})

bus.Subscribe(events.EventVoteCast, func(p any) {
	payload, ok := p.(events.VoteCastPayload)
	if !ok { return }
	xpSvc.AwardXP(ctx, payload.UserID, XPActionVote)
	badgeSvc.CheckAndAwardBadges(ctx, payload.UserID, "vote")
	missionSvc.IncrementMissionProgress(ctx, payload.UserID, "vote")
})

bus.Subscribe(events.EventPlaceSuggested, func(p any) {
	payload, ok := p.(events.PlaceSuggestedPayload)
	if !ok { return }
	xpSvc.AwardXP(ctx, payload.UserID, XPActionPlaceSuggest)
})

bus.Subscribe(events.EventReactionAdded, func(p any) {
	payload, ok := p.(events.ReactionAddedPayload)
	if !ok { return }
	xpSvc.AwardXP(ctx, payload.UserID, XPActionReaction)
})`

	handlerNameToIDs := map[string][]int64{
		"AwardXP":                 {100},
		"CheckAndAwardBadges":     {101},
		"IncrementMissionProgress": {102},
	}

	result := attributeHandlersToEvents(source, subscribeEventPatterns, handlerNameToIDs)

	// EventCheckinCompleted -> AwardXP, CheckAndAwardBadges, IncrementMissionProgress
	checkin := result["EventCheckinCompleted"]
	sort.Slice(checkin, func(i, j int) bool { return checkin[i] < checkin[j] })
	if len(checkin) != 3 || checkin[0] != 100 || checkin[1] != 101 || checkin[2] != 102 {
		t.Errorf("EventCheckinCompleted: expected [100,101,102], got %v", checkin)
	}

	// EventVoteCast -> AwardXP, CheckAndAwardBadges, IncrementMissionProgress
	vote := result["EventVoteCast"]
	sort.Slice(vote, func(i, j int) bool { return vote[i] < vote[j] })
	if len(vote) != 3 || vote[0] != 100 || vote[1] != 101 || vote[2] != 102 {
		t.Errorf("EventVoteCast: expected [100,101,102], got %v", vote)
	}

	// EventPlaceSuggested -> AwardXP only
	suggested := result["EventPlaceSuggested"]
	if len(suggested) != 1 || suggested[0] != 100 {
		t.Errorf("EventPlaceSuggested: expected [100], got %v", suggested)
	}

	// EventReactionAdded -> AwardXP only
	reaction := result["EventReactionAdded"]
	if len(reaction) != 1 || reaction[0] != 100 {
		t.Errorf("EventReactionAdded: expected [100], got %v", reaction)
	}

	// Verify no extra events
	if len(result) != 4 {
		t.Errorf("expected 4 events, got %d: %v", len(result), keysOf(result))
	}
}

func TestAttributeHandlersToEvents_NoSubscribeCalls(t *testing.T) {
	source := `fmt.Println("no subscribe here")`
	handlerNameToIDs := map[string][]int64{"AwardXP": {100}}
	result := attributeHandlersToEvents(source, subscribeEventPatterns, handlerNameToIDs)
	if len(result) != 0 {
		t.Errorf("expected empty result, got %v", result)
	}
}

func TestAttributeHandlersToEvents_HandlerInComment(t *testing.T) {
	source := `bus.Subscribe(events.EventCheckinCompleted, func(p any) {
	// AwardXP is called here for XP
	xpSvc.AwardXP(ctx, payload.UserID, XPActionCheckin)
})`
	handlerNameToIDs := map[string][]int64{"AwardXP": {100}}
	result := attributeHandlersToEvents(source, subscribeEventPatterns, handlerNameToIDs)

	// The comment line is skipped, but the real call on the next line should still match
	handlers := result["EventCheckinCompleted"]
	if len(handlers) != 1 || handlers[0] != 100 {
		t.Errorf("expected [100], got %v", handlers)
	}
}

// --- extractSubscribeCallLines tests ---

func TestExtractSubscribeCallLines(t *testing.T) {
	source := `bus.Subscribe(events.EventCheckinCompleted, func(p any) {
	xpSvc.AwardXP(ctx, p)
})
bus.Subscribe(events.EventVoteCast, func(p any) {
	xpSvc.AwardXP(ctx, p)
})`
	calls := extractSubscribeCallLines(source, subscribeEventPatterns)
	if len(calls) != 2 {
		t.Fatalf("expected 2 subscribe calls, got %d", len(calls))
	}
	if calls[0].eventName != "EventCheckinCompleted" || calls[0].line != 1 {
		t.Errorf("call[0]: expected EventCheckinCompleted@1, got %s@%d", calls[0].eventName, calls[0].line)
	}
	if calls[1].eventName != "EventVoteCast" || calls[1].line != 4 {
		t.Errorf("call[1]: expected EventVoteCast@4, got %s@%d", calls[1].eventName, calls[1].line)
	}
}

// --- sourceFileCache tests ---

func TestSourceFileCache_ReadLines(t *testing.T) {
	dir := t.TempDir()
	content := "line1\nline2\nline3\nline4\nline5\n"
	path := filepath.Join(dir, "test.go")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cache := &sourceFileCache{files: make(map[string]string)}

	// Read lines 2-4
	got, err := cache.readLines(dir, "test.go", 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	expected := "line2\nline3\nline4"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	// Verify caching: second call should use cache
	got2, err := cache.readLines(dir, "test.go", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got2 != "line1" {
		t.Errorf("expected 'line1', got %q", got2)
	}
}

func TestSourceFileCache_ReadWholeFile(t *testing.T) {
	dir := t.TempDir()
	content := "line1\nline2\n"
	path := filepath.Join(dir, "test.go")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cache := &sourceFileCache{files: make(map[string]string)}
	got, err := cache.readLines(dir, "test.go", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != content {
		t.Errorf("expected full file, got %q", got)
	}
}

// --- findNearestPrecedingEvent tests ---

func TestFindNearestPrecedingEvent(t *testing.T) {
	calls := []subscribeCallLine{
		{"EventA", 5},
		{"EventB", 15},
		{"EventC", 25},
	}

	tests := []struct {
		line     int
		expected string
	}{
		{3, ""},       // before any subscribe
		{5, "EventA"}, // on the subscribe line itself
		{10, "EventA"},
		{15, "EventB"},
		{20, "EventB"},
		{30, "EventC"},
	}

	for _, tt := range tests {
		got := findNearestPrecedingEvent(calls, tt.line)
		if got != tt.expected {
			t.Errorf("line %d: expected %q, got %q", tt.line, tt.expected, got)
		}
	}
}

// --- deduplicateInt64 tests ---

func TestDeduplicateInt64(t *testing.T) {
	input := []int64{1, 2, 3, 2, 1, 4}
	got := deduplicateInt64(input)
	if len(got) != 4 {
		t.Errorf("expected 4 unique values, got %d: %v", len(got), got)
	}
}

// --- Pattern compilation tests ---

func TestPublishEventPatterns_Compile(t *testing.T) {
	// Verify the patterns compile and match expected strings
	tests := []struct {
		input    string
		patterns []*regexp.Regexp
		want     string
	}{
		{`.Publish(events.EventCheckinCompleted,`, publishEventPatterns, "EventCheckinCompleted"},
		{`.Emit(events.EventCheckinCompleted,`, publishEventPatterns, "EventCheckinCompleted"},
		{`.Subscribe(events.EventVoteCast,`, subscribeEventPatterns, "EventVoteCast"},
		{`.emit('user.created',`, publishEventPatterns, "user.created"},
		{`.on('user.created',`, subscribeEventPatterns, "user.created"},
	}

	for _, tt := range tests {
		found := false
		for _, pat := range tt.patterns {
			m := pat.FindStringSubmatch(tt.input)
			if len(m) >= 2 && m[1] == tt.want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no pattern matched %q for expected %q", tt.input, tt.want)
		}
	}
}

// keysOf returns the keys of a map for debugging.
func keysOf(m map[string][]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
