package store

import (
	"sort"
	"testing"
)

func TestHopToRisk(t *testing.T) {
	tests := []struct {
		hop  int
		want RiskLevel
	}{
		{1, RiskCritical},
		{2, RiskHigh},
		{3, RiskMedium},
		{4, RiskLow},
		{5, RiskLow},
		{10, RiskLow},
	}
	for _, tt := range tests {
		got := HopToRisk(tt.hop)
		if got != tt.want {
			t.Errorf("HopToRisk(%d) = %s, want %s", tt.hop, got, tt.want)
		}
	}
}

func TestBuildImpactSummary(t *testing.T) {
	hops := []*NodeHop{
		{Node: &Node{ID: 1}, Hop: 1},
		{Node: &Node{ID: 2}, Hop: 1},
		{Node: &Node{ID: 3}, Hop: 2},
		{Node: &Node{ID: 4}, Hop: 3},
		{Node: &Node{ID: 5}, Hop: 4},
	}
	edges := []EdgeInfo{
		{FromName: "A", ToName: "B", Type: "CALLS"},
	}

	s := BuildImpactSummary(hops, edges)

	if s.Critical != 2 {
		t.Errorf("critical = %d, want 2", s.Critical)
	}
	if s.High != 1 {
		t.Errorf("high = %d, want 1", s.High)
	}
	if s.Medium != 1 {
		t.Errorf("medium = %d, want 1", s.Medium)
	}
	if s.Low != 1 {
		t.Errorf("low = %d, want 1", s.Low)
	}
	if s.Total != 5 {
		t.Errorf("total = %d, want 5", s.Total)
	}
	if s.HasCrossService {
		t.Error("expected has_cross_service=false")
	}
}

func TestCrossServiceDetection(t *testing.T) {
	hops := []*NodeHop{{Node: &Node{ID: 1}, Hop: 1}}
	edges := []EdgeInfo{
		{FromName: "A", ToName: "B", Type: "HTTP_CALLS"},
	}
	s := BuildImpactSummary(hops, edges)
	if !s.HasCrossService {
		t.Error("expected has_cross_service=true for HTTP_CALLS")
	}

	edges2 := []EdgeInfo{
		{FromName: "A", ToName: "B", Type: "ASYNC_CALLS"},
	}
	s2 := BuildImpactSummary(hops, edges2)
	if !s2.HasCrossService {
		t.Error("expected has_cross_service=true for ASYNC_CALLS")
	}
}

func TestDeduplicateHops(t *testing.T) {
	hops := []*NodeHop{
		{Node: &Node{ID: 1, Name: "A"}, Hop: 2},
		{Node: &Node{ID: 1, Name: "A"}, Hop: 3}, // duplicate at higher hop
		{Node: &Node{ID: 2, Name: "B"}, Hop: 1},
		{Node: &Node{ID: 3, Name: "C"}, Hop: 3},
	}

	result := DeduplicateHops(hops)

	// Sort for deterministic comparison
	sort.Slice(result, func(i, j int) bool {
		return result[i].Node.ID < result[j].Node.ID
	})

	if len(result) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(result))
	}
	if result[0].Node.ID != 1 || result[0].Hop != 2 {
		t.Errorf("node 1: expected hop=2, got hop=%d", result[0].Hop)
	}
	if result[1].Node.ID != 2 || result[1].Hop != 1 {
		t.Errorf("node 2: expected hop=1, got hop=%d", result[1].Hop)
	}
	if result[2].Node.ID != 3 || result[2].Hop != 3 {
		t.Errorf("node 3: expected hop=3, got hop=%d", result[2].Hop)
	}
}
