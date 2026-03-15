package store

import (
	"testing"
)

func TestGetFloat64(t *testing.T) {
	cfg, err := OpenConfigInDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cfg.Close()

	// Default when key is missing.
	if got := cfg.GetFloat64("missing_key", 3.14); got != 3.14 {
		t.Errorf("GetFloat64 missing key = %v, want 3.14", got)
	}

	// Valid float value.
	if err := cfg.Set("price", "0.000015"); err != nil {
		t.Fatal(err)
	}
	if got := cfg.GetFloat64("price", 0); got != 0.000015 {
		t.Errorf("GetFloat64 valid = %v, want 0.000015", got)
	}

	// Invalid value falls back to default.
	if err := cfg.Set("bad", "not-a-number"); err != nil {
		t.Fatal(err)
	}
	if got := cfg.GetFloat64("bad", 99.9); got != 99.9 {
		t.Errorf("GetFloat64 invalid = %v, want 99.9", got)
	}
}
