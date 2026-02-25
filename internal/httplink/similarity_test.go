package httplink

import "testing"

func TestLevenshteinDistance(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"abc", "", 3},
		{"", "abc", 3},
		{"abc", "abc", 0},
		{"kitten", "sitting", 3},
		{"api/orders", "api/order", 1},
		{"/api/v1/orders", "/api/v2/orders", 1},
	}
	for _, tt := range tests {
		got := levenshteinDistance(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("levenshteinDistance(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestNormalizedLevenshtein(t *testing.T) {
	tests := []struct {
		a, b string
		min  float64
		max  float64
	}{
		{"abc", "abc", 1.0, 1.0},
		{"", "", 1.0, 1.0},
		{"api/orders", "api/order", 0.88, 0.92},
		{"/api/v1/items", "/api/v2/items", 0.90, 0.94},
		{"completely", "different", 0.0, 0.4},
	}
	for _, tt := range tests {
		got := normalizedLevenshtein(tt.a, tt.b)
		if got < tt.min || got > tt.max {
			t.Errorf("normalizedLevenshtein(%q, %q) = %.3f, want [%.2f, %.2f]", tt.a, tt.b, got, tt.min, tt.max)
		}
	}
}

func TestNgramOverlap(t *testing.T) {
	tests := []struct {
		a, b string
		n    int
		min  float64
		max  float64
	}{
		{"api/orders", "api/orders", 3, 1.0, 1.0},
		{"api/orders", "api/order", 3, 0.8, 1.0},
		{"abcdef", "ghijkl", 3, 0.0, 0.0},
		{"ab", "cd", 3, 0.0, 0.0}, // too short for trigrams
	}
	for _, tt := range tests {
		got := ngramOverlap(tt.a, tt.b, tt.n)
		if got < tt.min || got > tt.max {
			t.Errorf("ngramOverlap(%q, %q, %d) = %.3f, want [%.2f, %.2f]", tt.a, tt.b, tt.n, got, tt.min, tt.max)
		}
	}
}

func TestConfidenceBand(t *testing.T) {
	tests := []struct {
		score float64
		want  string
	}{
		{0.95, "high"},
		{0.70, "high"},
		{0.69, "medium"},
		{0.45, "medium"},
		{0.44, "speculative"},
		{0.25, "speculative"},
		{0.24, ""},
		{0.0, ""},
	}
	for _, tt := range tests {
		got := confidenceBand(tt.score)
		if got != tt.want {
			t.Errorf("confidenceBand(%.2f) = %q, want %q", tt.score, got, tt.want)
		}
	}
}
