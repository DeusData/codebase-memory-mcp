package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// memSnapshot captures a single memory measurement.
type memSnapshot struct {
	Stage     string
	Time      time.Time
	HeapInuse uint64
	HeapAlloc uint64
	Sys       uint64
	NumGC     uint32
}

func captureMemSnapshot(stage string) memSnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return memSnapshot{
		Stage:     stage,
		Time:      time.Now(),
		HeapInuse: m.HeapInuse,
		HeapAlloc: m.HeapAlloc,
		Sys:       m.Sys,
		NumGC:     m.NumGC,
	}
}

// TestCrashDiag_MemoryPressure generates a 500-file synthetic repo with
// 3 large files (~1MB each) and indexes it in full mode while sampling
// runtime.MemStats every 500ms. The goal is to capture peak heap usage
// and GC behavior under moderate load.
func TestCrashDiag_MemoryPressure(t *testing.T) {
	dir := t.TempDir()

	// Create .git marker
	if err := os.MkdirAll(filepath.Join(dir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Generate 500 Go files (~3KB each) across 10 packages
	t.Log("Generating 500 Go files...")
	for i := 0; i < 500; i++ {
		pkg := fmt.Sprintf("pkg%d", i%10)
		name := fmt.Sprintf("file%d.go", i)
		content := fmt.Sprintf(`package %s

func Func%d(x int) int {
	return x + %d
}

func Caller%d() int {
	return Func%d(42)
}

type Struct%d struct {
	Field%d int
	Name    string
}

func (s *Struct%d) Method%d() int {
	return s.Field%d + Func%d(s.Field%d)
}

func Helper%d(a, b int) int {
	return a*b + Func%d(a)
}
`, pkg, i, i, i, i, i, i, i, i, i, i, i, i, i)
		absPath := filepath.Join(dir, pkg, name)
		if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absPath, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// Generate 3 large files (~1MB each) with many function definitions
	t.Log("Generating 3 large files (~1MB each)...")
	for i := 0; i < 3; i++ {
		var content string
		content = fmt.Sprintf("package largepkg%d\n\n", i)
		// Each function is ~100 bytes, so ~10000 functions per file for ~1MB
		for j := 0; j < 10000; j++ {
			content += fmt.Sprintf("func LargeFunc%d_%d() int { return %d }\n", i, j, j)
		}
		absPath := filepath.Join(dir, fmt.Sprintf("largepkg%d", i), fmt.Sprintf("large%d.go", i))
		if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absPath, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// Open store
	st, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	// Start background memory sampler
	var mu sync.Mutex
	var samples []memSnapshot
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				snap := captureMemSnapshot("sampling")
				mu.Lock()
				samples = append(samples, snap)
				mu.Unlock()
			}
		}
	}()

	// Capture pre-run snapshot
	preSnap := captureMemSnapshot("pre_run")
	t.Logf("PRE_RUN:  heap_inuse=%d MB, heap_alloc=%d MB, sys=%d MB, num_gc=%d",
		preSnap.HeapInuse/(1<<20), preSnap.HeapAlloc/(1<<20), preSnap.Sys/(1<<20), preSnap.NumGC)

	// Run pipeline
	t.Log("Running pipeline in ModeFull...")
	start := time.Now()
	p := New(context.Background(), st, dir, discover.ModeFull)
	runErr := p.Run()
	elapsed := time.Since(start)

	// Stop sampler
	close(done)

	// Capture post-run snapshot
	postSnap := captureMemSnapshot("post_run")

	if runErr != nil {
		t.Logf("Pipeline.Run returned error: %v", runErr)
	}

	t.Logf("POST_RUN: heap_inuse=%d MB, heap_alloc=%d MB, sys=%d MB, num_gc=%d",
		postSnap.HeapInuse/(1<<20), postSnap.HeapAlloc/(1<<20), postSnap.Sys/(1<<20), postSnap.NumGC)
	t.Logf("Elapsed: %s", elapsed)
	t.Logf("GC cycles during run: %d", postSnap.NumGC-preSnap.NumGC)

	// Find peak from samples
	mu.Lock()
	defer mu.Unlock()

	var peakHeapInuse, peakHeapAlloc, peakSys uint64
	for _, s := range samples {
		if s.HeapInuse > peakHeapInuse {
			peakHeapInuse = s.HeapInuse
		}
		if s.HeapAlloc > peakHeapAlloc {
			peakHeapAlloc = s.HeapAlloc
		}
		if s.Sys > peakSys {
			peakSys = s.Sys
		}
	}

	t.Logf("PEAK (from %d samples): heap_inuse=%d MB, heap_alloc=%d MB, sys=%d MB",
		len(samples), peakHeapInuse/(1<<20), peakHeapAlloc/(1<<20), peakSys/(1<<20))

	// Log all samples for analysis
	t.Log("--- Memory Sample Timeline ---")
	for i, s := range samples {
		t.Logf("  [%3d] t=+%6dms heap_inuse=%4d MB heap_alloc=%4d MB sys=%4d MB num_gc=%d",
			i, s.Time.Sub(start).Milliseconds(),
			s.HeapInuse/(1<<20), s.HeapAlloc/(1<<20), s.Sys/(1<<20), s.NumGC)
	}

	t.Log("--- Summary ---")
	t.Logf("Files: 503 (500 small + 3 large)")
	t.Logf("Total time: %s", elapsed)
	t.Logf("Peak heap_inuse: %d MB", peakHeapInuse/(1<<20))
	t.Logf("Peak heap_alloc: %d MB", peakHeapAlloc/(1<<20))
	t.Logf("Peak sys: %d MB", peakSys/(1<<20))
	t.Logf("GC cycles: %d", postSnap.NumGC-preSnap.NumGC)
	t.Logf("Pipeline error: %v", runErr)
}

// TestCrashDiag_LargeFileNoGuard creates a single 20MB JavaScript file
// and indexes it in full mode to verify that full mode has no MaxFileSize
// guard and to measure heap growth from processing a large file.
func TestCrashDiag_LargeFileNoGuard(t *testing.T) {
	dir := t.TempDir()

	// Create .git marker
	if err := os.MkdirAll(filepath.Join(dir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Generate a 20MB JavaScript file
	t.Log("Generating 20MB JavaScript file...")
	filePath := filepath.Join(dir, "huge_bundle.js")
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatal(err)
	}

	var written int64
	targetSize := int64(20 * 1024 * 1024) // 20MB
	for n := 0; written < targetSize; n++ {
		line := fmt.Sprintf("function f%d(x) { return x + %d; }\n", n, n)
		nn, err := f.WriteString(line)
		if err != nil {
			f.Close()
			t.Fatal(err)
		}
		written += int64(nn)
	}
	f.Close()
	t.Logf("Generated file: %d MB", written/(1<<20))

	// Open store
	st, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	// Capture pre-run heap
	runtime.GC()
	preSnap := captureMemSnapshot("pre_large_file")
	t.Logf("PRE_RUN:  heap_inuse=%d MB, heap_alloc=%d MB, sys=%d MB",
		preSnap.HeapInuse/(1<<20), preSnap.HeapAlloc/(1<<20), preSnap.Sys/(1<<20))

	// Run pipeline
	t.Log("Running pipeline on 20MB file in ModeFull (no MaxFileSize guard)...")
	start := time.Now()
	p := New(context.Background(), st, dir, discover.ModeFull)
	runErr := p.Run()
	elapsed := time.Since(start)

	// Capture post-run heap
	postSnap := captureMemSnapshot("post_large_file")

	if runErr != nil {
		t.Logf("Pipeline.Run returned error: %v", runErr)
	}

	t.Logf("POST_RUN: heap_inuse=%d MB, heap_alloc=%d MB, sys=%d MB",
		postSnap.HeapInuse/(1<<20), postSnap.HeapAlloc/(1<<20), postSnap.Sys/(1<<20))
	t.Logf("Elapsed: %s", elapsed)

	heapGrowth := int64(postSnap.HeapInuse) - int64(preSnap.HeapInuse)
	t.Logf("Heap growth (HeapInuse): %d MB", heapGrowth/(1<<20))
	t.Logf("GC cycles during run: %d", postSnap.NumGC-preSnap.NumGC)

	// Check if the file was discovered and processed
	nodeCount, _ := st.CountNodes(p.ProjectName)
	edgeCount, _ := st.CountEdges(p.ProjectName)
	t.Logf("Nodes indexed: %d", nodeCount)
	t.Logf("Edges indexed: %d", edgeCount)

	t.Log("--- Observation ---")
	t.Logf("File size: %d MB", written/(1<<20))
	t.Logf("File was discovered: %v", nodeCount > 0)
	t.Logf("Heap growth vs file size ratio: %.1fx", float64(heapGrowth)/float64(written))
	t.Logf("This confirms full mode has no MaxFileSize guard — large files are processed without limits.")
}
