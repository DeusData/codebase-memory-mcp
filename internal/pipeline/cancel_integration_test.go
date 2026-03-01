package pipeline

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// TestGracefulShutdownLargeRepo indexes a real 300MB repo with a 2s timeout.
// Skipped if /tmp/bench/erlang does not exist.
func TestGracefulShutdownLargeRepo(t *testing.T) {
	repoPath := "/tmp/bench/erlang"
	if _, err := os.Stat(repoPath); os.IsNotExist(err) {
		t.Skip("skipping: /tmp/bench/erlang not found")
	}

	st, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	p := New(ctx, st, repoPath)
	err = p.Run()
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected cancellation error, but index completed in %v", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context error, got: %v (after %v)", err, elapsed)
	}

	t.Logf("PASS: cancelled after %v with %v", elapsed, err)

	// Ensure it stopped reasonably fast (within 5s of the 2s deadline)
	if elapsed > 7*time.Second {
		t.Errorf("cancellation took too long: %v (expected <7s)", elapsed)
	}
}
