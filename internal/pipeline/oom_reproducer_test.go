package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// heapSampler tracks peak HeapInuse by polling runtime.ReadMemStats at a fixed interval.
type heapSampler struct {
	peakHeapInuse uint64
	peakHeapAlloc uint64
	peakSys       uint64
	numSamples    int
	stop          chan struct{}
	done          chan struct{}
}

func newHeapSampler(interval time.Duration) *heapSampler {
	s := &heapSampler{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				s.numSamples++
				if m.HeapInuse > s.peakHeapInuse {
					s.peakHeapInuse = m.HeapInuse
				}
				if m.HeapAlloc > s.peakHeapAlloc {
					s.peakHeapAlloc = m.HeapAlloc
				}
				if m.Sys > s.peakSys {
					s.peakSys = m.Sys
				}
			}
		}
	}()
	return s
}

func (s *heapSampler) Stop() {
	close(s.stop)
	<-s.done
}

// TestOOMReproducer_LargeFile creates a single 20MB JavaScript file and indexes it
// in full mode (no MaxFileSize guard). This is diagnostic: it logs memory behavior
// but always passes. The goal is to show the memory amplification from a large file.
func TestOOMReproducer_LargeFile(t *testing.T) {
	dir := t.TempDir()

	// Write a 20MB JavaScript file with valid-ish JS content
	const targetSize = 20 * 1024 * 1024 // 20MB
	jsFile := filepath.Join(dir, "large_bundle.js")
	f, err := os.Create(jsFile)
	if err != nil {
		t.Fatal(err)
	}
	written := 0
	lineNum := 0
	for written < targetSize {
		line := fmt.Sprintf("var x%d = %d;\n", lineNum, lineNum)
		n, err := f.WriteString(line)
		if err != nil {
			f.Close()
			t.Fatal(err)
		}
		written += n
		lineNum++
	}
	f.Close()
	t.Logf("Created %s: %d bytes (%d lines)", jsFile, written, lineNum)

	// Create .git marker so discover treats it as a repo root
	if err := os.MkdirAll(filepath.Join(dir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Open store
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Baseline memory
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	t.Logf("Baseline: heap_inuse=%d MB, heap_alloc=%d MB, sys=%d MB",
		baseline.HeapInuse/(1<<20), baseline.HeapAlloc/(1<<20), baseline.Sys/(1<<20))

	// Start heap sampler
	sampler := newHeapSampler(200 * time.Millisecond)

	// Run pipeline in full mode (bypasses MaxFileSize guard)
	p := New(context.Background(), s, dir, discover.ModeFull)
	start := time.Now()
	runErr := p.Run()
	elapsed := time.Since(start)

	sampler.Stop()

	// Final memory snapshot
	runtime.GC()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)

	// Log findings
	fileSizeMB := float64(written) / float64(1<<20)
	peakHeapMB := float64(sampler.peakHeapInuse) / float64(1<<20)
	ratio := peakHeapMB / fileSizeMB

	t.Logf("=== TestOOMReproducer_LargeFile Results ===")
	t.Logf("File size:        %.1f MB", fileSizeMB)
	t.Logf("Peak heap inuse:  %.1f MB", peakHeapMB)
	t.Logf("Peak heap alloc:  %.1f MB", float64(sampler.peakHeapAlloc)/float64(1<<20))
	t.Logf("Peak sys:         %.1f MB", float64(sampler.peakSys)/float64(1<<20))
	t.Logf("Heap/file ratio:  %.1fx", ratio)
	t.Logf("Wall time:        %v", elapsed)
	t.Logf("NumGC (final):    %d", final.NumGC)
	t.Logf("Samples taken:    %d", sampler.numSamples)
	if runErr != nil {
		t.Logf("Pipeline error:   %v", runErr)
	} else {
		t.Logf("Pipeline:         completed successfully")
	}
	t.Logf("Post-GC heap:     %d MB", final.HeapInuse/(1<<20))
}

// TestOOMReproducer_ManyFiles creates 300 small Go files and indexes them in full mode.
// This exercises GraphBuffer growth and extractionCache scaling. Diagnostic only.
func TestOOMReproducer_ManyFiles(t *testing.T) {
	dir := t.TempDir()

	const numFiles = 300
	const funcsPerFile = 5

	// Create 300 Go files across 10 packages
	for i := 0; i < numFiles; i++ {
		pkg := fmt.Sprintf("pkg%d", i/30)
		pkgDir := filepath.Join(dir, pkg)
		if err := os.MkdirAll(pkgDir, 0o755); err != nil {
			t.Fatal(err)
		}
		filename := filepath.Join(pkgDir, fmt.Sprintf("file%d.go", i))
		var content string
		content += fmt.Sprintf("package %s\n\n", pkg)
		for j := 0; j < funcsPerFile; j++ {
			content += fmt.Sprintf("func Func%d_%s() int { return %d }\n\n", i, fmt.Sprintf("f%d", j), i*funcsPerFile+j)
		}
		if err := os.WriteFile(filename, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// Create .git marker
	if err := os.MkdirAll(filepath.Join(dir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}

	t.Logf("Created %d Go files (%d functions total) across 10 packages",
		numFiles, numFiles*funcsPerFile)

	// Open store
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Baseline
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	t.Logf("Baseline: heap_inuse=%d MB", baseline.HeapInuse/(1<<20))

	// Sample heap during run
	sampler := newHeapSampler(200 * time.Millisecond)

	p := New(context.Background(), s, dir, discover.ModeFull)
	start := time.Now()
	runErr := p.Run()
	elapsed := time.Since(start)

	sampler.Stop()

	// Final stats
	runtime.GC()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)

	// Count nodes/edges
	nodeCount, _ := s.CountNodes(p.ProjectName)
	edgeCount, _ := s.CountEdges(p.ProjectName)

	expectedNodes := numFiles * funcsPerFile * 3 // rough: ~3 nodes per function (module, function, etc.)

	t.Logf("=== TestOOMReproducer_ManyFiles Results ===")
	t.Logf("Files:            %d", numFiles)
	t.Logf("Nodes:            %d (estimated ~%d)", nodeCount, expectedNodes)
	t.Logf("Edges:            %d", edgeCount)
	t.Logf("Peak heap inuse:  %.1f MB", float64(sampler.peakHeapInuse)/float64(1<<20))
	t.Logf("Peak heap alloc:  %.1f MB", float64(sampler.peakHeapAlloc)/float64(1<<20))
	t.Logf("Peak sys:         %.1f MB", float64(sampler.peakSys)/float64(1<<20))
	t.Logf("Wall time:        %v", elapsed)
	t.Logf("NumGC (final):    %d", final.NumGC)
	t.Logf("Samples taken:    %d", sampler.numSamples)
	if runErr != nil {
		t.Logf("Pipeline error:   %v", runErr)
	} else {
		t.Logf("Pipeline:         completed successfully")
	}
	t.Logf("Post-GC heap:     %d MB", final.HeapInuse/(1<<20))

	// Estimate GraphBuffer memory: ~512 bytes/node + ~256 bytes/edge
	estimatedGraphBufMB := float64(nodeCount*512+edgeCount*256) / float64(1<<20)
	t.Logf("Est. GraphBuffer: %.1f MB (nodes×512 + edges×256)", estimatedGraphBufMB)
}

// TestOOMReproducer_AdaptivePool creates 100 medium-size Go files (~200KB each) and
// indexes them in full mode, monitoring goroutine count to observe adaptive pool behavior.
func TestOOMReproducer_AdaptivePool(t *testing.T) {
	dir := t.TempDir()

	const numFiles = 100
	const targetFileSize = 200 * 1024 // 200KB per file

	// Create 100 Go files with many functions each (~200KB)
	for i := 0; i < numFiles; i++ {
		filename := filepath.Join(dir, fmt.Sprintf("file%d.go", i))
		var content string
		content += "package main\n\n"
		funcNum := 0
		for len(content) < targetFileSize {
			content += fmt.Sprintf("func File%d_Func%d(a, b, c int) int {\n\tx := a + b\n\ty := b + c\n\tz := x * y\n\treturn z + %d\n}\n\n", i, funcNum, funcNum)
			funcNum++
		}
		if err := os.WriteFile(filename, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// Create .git marker
	if err := os.MkdirAll(filepath.Join(dir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}

	t.Logf("Created %d Go files (~%d KB each, total ~%d MB)",
		numFiles, targetFileSize/1024, numFiles*targetFileSize/(1024*1024))

	// Open store
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Baseline
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	// Track goroutines and heap simultaneously
	var peakGoroutines int64
	var peakHeapInuse uint64
	var peakHeapAlloc uint64
	var goroutineSamples int
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})

	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				goroutineSamples++
				numG := int64(runtime.NumGoroutine())
				if numG > atomic.LoadInt64(&peakGoroutines) {
					atomic.StoreInt64(&peakGoroutines, numG)
				}
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapInuse > peakHeapInuse {
					peakHeapInuse = m.HeapInuse
				}
				if m.HeapAlloc > peakHeapAlloc {
					peakHeapAlloc = m.HeapAlloc
				}
			}
		}
	}()

	p := New(context.Background(), s, dir, discover.ModeFull)
	start := time.Now()
	runErr := p.Run()
	elapsed := time.Since(start)

	close(stopCh)
	<-doneCh

	// Final stats
	runtime.GC()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)

	t.Logf("=== TestOOMReproducer_AdaptivePool Results ===")
	t.Logf("Files:              %d × ~%d KB = ~%d MB total",
		numFiles, targetFileSize/1024, numFiles*targetFileSize/(1024*1024))
	t.Logf("Peak goroutines:    %d", atomic.LoadInt64(&peakGoroutines))
	t.Logf("Peak heap inuse:    %.1f MB", float64(peakHeapInuse)/float64(1<<20))
	t.Logf("Peak heap alloc:    %.1f MB", float64(peakHeapAlloc)/float64(1<<20))
	t.Logf("Wall time:          %v", elapsed)
	t.Logf("NumGC (final):      %d", final.NumGC)
	t.Logf("Goroutine samples:  %d", goroutineSamples)
	t.Logf("NumCPU:             %d", runtime.NumCPU())
	t.Logf("Adaptive pool max:  %d (NumCPU × 8)", runtime.NumCPU()*8)
	if runErr != nil {
		t.Logf("Pipeline error:     %v", runErr)
	} else {
		t.Logf("Pipeline:           completed successfully")
	}
	t.Logf("Post-GC heap:       %d MB", final.HeapInuse/(1<<20))
}
