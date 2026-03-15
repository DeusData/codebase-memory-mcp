package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestEstimateTokens(t *testing.T) {
	if got := EstimateTokens("hello"); got != 1 {
		t.Fatalf("EstimateTokens(\"hello\") = %d, want 1", got)
	}
	if got := EstimateTokens(""); got != 0 {
		t.Fatalf("EstimateTokens(\"\") = %d, want 0", got)
	}
}

func TestCalculateSavings(t *testing.T) {
	tests := []struct {
		name                 string
		baselineBytes        int
		responseBytes        int
		pricePerToken        float64
		wantTokensSaved      int
		wantReductionRatio float64
	}{
		{
			name:                 "NormalCase",
			baselineBytes:        4000,
			responseBytes:        400,
			pricePerToken:        0.000015,
			wantTokensSaved:      900,
			wantReductionRatio: 0.1,
		},
		{
			name:                 "NoSavings",
			baselineBytes:        100,
			responseBytes:        200,
			pricePerToken:        0.000015,
			wantTokensSaved:      0,
			wantReductionRatio: 2.0,
		},
		{
			name:                 "ZeroBaseline",
			baselineBytes:        0,
			responseBytes:        400,
			pricePerToken:        0.000015,
			wantTokensSaved:      0,
			wantReductionRatio: 0.0,
		},
		{
			name:                 "ZeroBoth",
			baselineBytes:        0,
			responseBytes:        0,
			pricePerToken:        0.000015,
			wantTokensSaved:      0,
			wantReductionRatio: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateSavings(tt.baselineBytes, tt.responseBytes, tt.pricePerToken)
			if got.TokensSaved != tt.wantTokensSaved {
				t.Errorf("TokensSaved = %d, want %d", got.TokensSaved, tt.wantTokensSaved)
			}
			if got.ReductionRatio != tt.wantReductionRatio {
				t.Errorf("ReductionRatio = %v, want %v", got.ReductionRatio, tt.wantReductionRatio)
			}
		})
	}
}

func TestCalculateSavings_NormalCaseFields(t *testing.T) {
	got := CalculateSavings(4000, 400, 0.000015)
	if got.BaselineTokens != 1000 {
		t.Errorf("BaselineTokens = %d, want 1000", got.BaselineTokens)
	}
	if got.ResponseTokens != 100 {
		t.Errorf("ResponseTokens = %d, want 100", got.ResponseTokens)
	}
	if got.CostAvoided != 0.0135 {
		t.Errorf("CostAvoided = %v, want 0.0135", got.CostAvoided)
	}
}

func TestTracker_RecordAndPersist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "savings.json")

	tr := NewTracker(path)
	meta := TokenMetadata{TokensSaved: 500, CostAvoided: 0.0075}
	tr.Record(meta)
	tr.Record(meta)

	totalTokens, totalCost := tr.Snapshot()
	if totalTokens != 1000 {
		t.Errorf("TotalTokensSaved = %d, want 1000", totalTokens)
	}
	if totalCost != 0.015 {
		t.Errorf("TotalCostAvoided = %v, want 0.015", totalCost)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read savings file: %v", err)
	}
	var rec savingsRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("savings file is not valid JSON: %v", err)
	}
	if rec.TotalTokensSaved != 1000 {
		t.Errorf("file TotalTokensSaved = %d, want 1000", rec.TotalTokensSaved)
	}
	if rec.InstallID == "" {
		t.Error("file InstallID should not be empty")
	}
	if rec.LastUpdated == "" {
		t.Error("file LastUpdated should not be empty")
	}
}

func TestTracker_LoadExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "savings.json")

	existing := savingsRecord{
		InstallID:        "abc123",
		TotalTokensSaved: 5000,
		TotalCostAvoided: 0.075,
		LastUpdated:      "2026-01-01T00:00:00Z",
	}
	data, _ := json.Marshal(existing)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	tr := NewTracker(path)
	if tr.installID != "abc123" {
		t.Errorf("InstallID = %q, want %q", tr.installID, "abc123")
	}

	tr.Record(TokenMetadata{TokensSaved: 200})
	totalTokens, _ := tr.Snapshot()
	if totalTokens != 5200 {
		t.Errorf("TotalTokensSaved = %d, want 5200", totalTokens)
	}
}

func TestTracker_MissingDir(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nonexistent-subdir", "savings.json")

	tr := NewTracker(path)
	totalTokens, totalCost := tr.Snapshot()
	if totalTokens != 0 || totalCost != 0 {
		t.Errorf("expected zero snapshot for missing dir tracker, got (%d, %v)", totalTokens, totalCost)
	}

	// Record should not panic even if dir doesn't exist
	tr.Record(TokenMetadata{TokensSaved: 100})
	totalTokens, _ = tr.Snapshot()
	if totalTokens != 100 {
		t.Errorf("TotalTokensSaved after Record = %d, want 100", totalTokens)
	}
}
