package metrics

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TokenMetadata holds per-call token savings estimation.
// Attached as _meta in tool responses when metrics are enabled.
type TokenMetadata struct {
	TokensSaved      int     `json:"tokens_saved"`
	BaselineTokens   int     `json:"baseline_tokens"`
	ResponseTokens   int     `json:"response_tokens"`
	CostAvoided      float64 `json:"cost_avoided"`
	CompressionRatio float64 `json:"compression_ratio"`
}

// EstimateTokens approximates token count from byte length using
// the standard English heuristic of 1 token ≈ 4 characters.
func EstimateTokens(s string) int {
	return len(s) / 4
}

// CalculateSavings computes token savings for a single tool call.
// baselineBytes: byte count of all files the user would have read manually.
// responseBytes: byte count of the actual tool response.
// pricePerToken: USD cost per output token (e.g. 0.000015 for Claude Sonnet).
func CalculateSavings(baselineBytes, responseBytes int, pricePerToken float64) TokenMetadata {
	baseline := baselineBytes / 4
	response := responseBytes / 4
	saved := baseline - response
	if saved < 0 {
		saved = 0
	}
	ratio := 0.0
	if baseline > 0 {
		ratio = math.Round(float64(response)/float64(baseline)*1000) / 1000
	}
	return TokenMetadata{
		TokensSaved:      saved,
		BaselineTokens:   baseline,
		ResponseTokens:   response,
		CostAvoided:      math.Round(float64(saved)*pricePerToken*1e6) / 1e6,
		CompressionRatio: ratio,
	}
}

// savingsRecord is the on-disk format for cumulative savings.
type savingsRecord struct {
	InstallID        string  `json:"install_id"`
	TotalTokensSaved int64   `json:"total_tokens_saved"`
	TotalCostAvoided float64 `json:"total_cost_avoided"`
	LastUpdated      string  `json:"last_updated"`
}

// Tracker accumulates token savings across calls and persists totals.
type Tracker struct {
	mu               sync.Mutex
	path             string
	InstallID        string
	TotalTokensSaved int64
	TotalCostAvoided float64
}

// NewTracker loads or creates savings.json at path. Generates a random
// InstallID on first run. Never returns an error (fail-open).
func NewTracker(path string) *Tracker {
	t := &Tracker{path: path}

	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("metrics: failed to read savings file", "path", path, "err", err)
		}
		t.InstallID = newInstallID()
		return t
	}

	var rec savingsRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		slog.Warn("metrics: malformed savings file, starting fresh", "path", path, "err", err)
		t.InstallID = newInstallID()
		return t
	}

	t.InstallID = rec.InstallID
	t.TotalTokensSaved = rec.TotalTokensSaved
	t.TotalCostAvoided = rec.TotalCostAvoided
	return t
}

// Record atomically increments TotalTokensSaved and TotalCostAvoided
// by the values in meta, then persists to file.
func (t *Tracker) Record(meta TokenMetadata) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.TotalTokensSaved += int64(meta.TokensSaved)
	t.TotalCostAvoided += meta.CostAvoided

	rec := savingsRecord{
		InstallID:        t.InstallID,
		TotalTokensSaved: t.TotalTokensSaved,
		TotalCostAvoided: t.TotalCostAvoided,
		LastUpdated:      time.Now().UTC().Format(time.RFC3339),
	}

	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		slog.Warn("metrics: failed to marshal savings", "err", err)
		return
	}

	dir := filepath.Dir(t.path)
	tmp, err := os.CreateTemp(dir, "savings-*.json.tmp")
	if err != nil {
		slog.Warn("metrics: failed to create temp file", "dir", dir, "err", err)
		return
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		slog.Warn("metrics: failed to write temp file", "err", err)
		return
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		slog.Warn("metrics: failed to close temp file", "err", err)
		return
	}

	if err := os.Rename(tmpName, t.path); err != nil {
		os.Remove(tmpName)
		slog.Warn("metrics: failed to rename temp file", "src", tmpName, "dst", t.path, "err", err)
		return
	}

	slog.Debug("metrics: savings persisted", "path", t.path, "total_tokens_saved", t.TotalTokensSaved)
}

// Snapshot returns current cumulative totals under lock.
func (t *Tracker) Snapshot() (totalTokensSaved int64, totalCostAvoided float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.TotalTokensSaved, t.TotalCostAvoided
}

func newInstallID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b)
}
