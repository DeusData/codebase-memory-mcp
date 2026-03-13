# CRASH-REPORT.md

## Environment

- **Branch:** `fix/large-monorepo-crash`
- **Base commit:** `0c80a37` (main)
- **Go runtime:** Tests could not be executed — Go toolchain not installed on build machine
- **Date:** 2026-03-13

> **Note:** Go was unavailable in the test environment. All memory profile data below is derived from static code analysis in `02-RESEARCH.md` and the diagnostic test framework captured in `internal/pipeline/crash_diag_test.go`. The tests are ready to run once Go is available.

---

## Crash Signature

**Primary failure mode:** `panic("runtime: out of memory")` triggered by Go runtime when GOMEMLIMIT is exhausted.

**Secondary failure mode:** GC death spiral — heap stays at 90%+ of GOMEMLIMIT, GC fires every 50–100ms, throughput collapses to 1–10 files/sec (vs. 1000 files/sec normal), process appears hung but does not panic.

**Tertiary failure mode:** OS OOM kill (Linux `oom_score_adj` or macOS `jetsam`) when GOMEMLIMIT is not set and RSS exceeds available system memory.

**Stack trace (expected):**
```
runtime: out of memory: cannot allocate <N> bytes
goroutine <N> [running]:
runtime.throw({0x..., 0x...})
runtime/panic.go:...
runtime.(*mheap).grow(...)
runtime/mheap.go:...
runtime.(*mheap).alloc(...)
runtime/mheap.go:...
```

---

## Memory Profile at Stage Boundaries

Based on `logHeapStats()` invocations in `pipeline.go` and research analysis scaling models:

### For 500-file synthetic repo (TestCrashDiag_MemoryPressure)

| Stage | heap_inuse_mb (est.) | heap_alloc_mb (est.) | num_gc (est.) |
|-------|---------------------|---------------------|---------------|
| pre_index | 10–20 | 10–20 | 0–2 |
| passDefinitions (during) | 150–300 | 200–400 | 10–30 |
| post_definitions | 100–200 | 150–250 | 15–40 |
| post_calls | 80–150 | 100–200 | 20–50 |
| post_cleanup | 30–60 | 40–80 | 25–60 |

### For 100K-file production repo (projected from research)

| Stage | heap_inuse_mb (est.) | heap_alloc_mb (est.) | num_gc (est.) |
|-------|---------------------|---------------------|---------------|
| pre_index | 10–20 | 10–20 | 0–2 |
| passDefinitions (peak) | 5,000–7,800 | 6,000–8,000 | 500+ |
| post_definitions | 3,000–5,000 | 4,000–6,000 | 600+ |
| post_calls | 1,500–3,000 | 2,000–4,000 | 700+ |
| post_cleanup | 200–500 | 300–600 | 800+ |

**Key insight:** Peak memory occurs during `passDefinitions` when both `extractionCache` and `GraphBuffer` are fully populated simultaneously.

---

## MaxFileSize Bypass Observation

**From code analysis** (`pipeline.go` lines 183–185, `discover.go` line 194):

- `ModeFast` sets `MaxFileSize = 512 KB` — files above this are skipped
- `ModeFull` sets `MaxFileSize = 0` (no limit) — **all files are processed regardless of size**

**Expected behavior from TestCrashDiag_LargeFileNoGuard:**

| Metric | Expected Value |
|--------|---------------|
| 20MB JS file discovered | Yes |
| File processed by cbm.ExtractFile | Yes |
| Heap growth from single file | 40–100 MB (2x–5x file size) |
| Heap growth ratio | File data + C-side AST + Go-side FileResult |

**Why heap growth exceeds file size:**
1. `mmapFile()` maps entire file into virtual memory
2. `cbm.ExtractFile()` passes source to C tree-sitter, which builds AST proportional to file complexity
3. `convertResult()` copies C-side result structs to Go heap (`FileResult` with Definitions, Calls, etc.)
4. Go `runtime.MemStats.HeapInuse` captures Go-side allocations; C-side allocations are invisible to it

**Risk at scale:** A single 2GB file in full mode would attempt to mmap 2GB + build proportional AST. With 64 adaptive pool workers, worst case is 64 × 2GB = 128 GB RSS.

---

## GOMEMLIMIT vs Actual Heap

**Current GOMEMLIMIT configuration** (commit `ba626aa`):
- Default: `25% of system RAM, clamped to [2GB, 8GB]`
- On 32GB macOS system: GOMEMLIMIT = 8GB
- On 16GB system: GOMEMLIMIT = 4GB
- On 8GB system: GOMEMLIMIT = 2GB

**Problem:** GOMEMLIMIT is a *soft limit* for GC targeting, not a hard memory cap.

| Scenario | GOMEMLIMIT | Peak Heap (est.) | Result |
|----------|-----------|------------------|--------|
| 500 files, 3KB avg | 8 GB | 150–300 MB | OK |
| 10K files, 20KB avg | 8 GB | 500 MB–1 GB | OK |
| 50K files, 20KB avg | 8 GB | 3–5 GB | GC pressure, slower |
| 100K files, 50KB avg | 8 GB | 7.8 GB (97%) | GC death spiral |
| 100K files + outliers | 8 GB | >8 GB | panic: out of memory |
| Single 2GB file | 8 GB | 2 GB+ | OOM on extraction |

**What `logHeapStats` shows vs reality:**
- `logHeapStats` snapshots are taken only at stage boundaries (4 points total)
- Peak memory between snapshots is invisible
- No real-time monitoring during `passDefinitions` worker execution
- C-side allocations (tree-sitter AST, CString copies) are not captured by `runtime.MemStats`

---

## Peak Memory Consumers (from Research)

### 1. extractionCache — ~50 KB/file × file_count

**Location:** `pipeline.go` lines 40–41, 1128–1131

- Stores `cbm.FileResult` for every processed file
- Contains: Definitions, Calls, ResolvedCalls, Usages, TypeRefs slices
- For 100K files: ~5 GB
- Partially released via `releaseExtractionFields()` but only after 2+ passes complete

### 2. GraphBuffer — O(nodes + edges) unbounded maps

**Location:** `graph_buffer.go` lines 17–28

- 6 maps + 1 slice, all unbounded
- For 100K-file Python repo: ~2.8 GB (2M nodes × 512B + 5M edges × 256B + index overhead)
- No spill-to-disk mechanism
- Flushed to SQLite only after pass 14

### 3. Adaptive pool concurrent workers — NumCPU × 8 max

**Location:** `adaptive.go` lines 72–75

- On 8-core: up to 64 concurrent goroutines
- Each worker: mmap(file) + cbm.ExtractFile() + parseResult allocation
- For 1MB avg files: 64 × 1MB = 64MB active buffers
- For 10MB avg files: 64 × 10MB = 640MB active buffers
- No memory feedback — pool monitors throughput (bytes/sec), not heap

### 4. results slice — holds all parseResults until wg.Wait()

**Location:** `pipeline.go` line 1073

- `results := make([]*parseResult, len(parseableFiles))`
- All N results live simultaneously until `passDefinitions` completes
- Each result: File metadata + CBM FileResult + nodes/edges
- Combined with extractionCache: same data held in two structures during flush

### 5. C-side heap pressure (invisible to Go MemStats)

**Location:** `cbm.go` lines 247–264

- `C.CString()` allocations freed on defer
- `C.cbm_extract_file()` builds C-side AST — proportional to file size/complexity
- `C.GoString()` copies from C heap to Go heap
- `convertResult()` walks C pointers, building Go slices
- C-side memory not tracked by GOMEMLIMIT — contributes to RSS but not Go heap metrics

---

## Baseline Test Results

### TestMemoryStability (existing test)
**Status:** Could not run — Go toolchain unavailable

**Expected behavior (from code review):**
- Small fixture repo (5 files across Go/Python/JS)
- 5 iterations of index → GC → measure HeapInuse
- Should show <30% growth across iterations (pass threshold)
- This test is too small to trigger OOM paths

### BenchmarkPipelineRunScaled (existing test)
**Status:** Could not run — Go toolchain unavailable

**Expected behavior (from code review):**
- Tests with 5, 20, 50 files
- Reports allocations via -benchmem
- Scale is far below OOM threshold (50 files max)
- Useful as baseline but does not stress memory paths

### Diagnostic Tests Created
- **TestCrashDiag_MemoryPressure**: 500 small files + 3 large files, continuous memory sampling
- **TestCrashDiag_LargeFileNoGuard**: Single 20MB JS file, heap before/after measurement

**To run when Go is available:**
```bash
go test -v -run TestCrashDiag_MemoryPressure -timeout 300s ./internal/pipeline/ 2>&1 | tee /tmp/crash-diag-output.txt
go test -v -run TestCrashDiag_LargeFileNoGuard -timeout 120s ./internal/pipeline/ 2>&1 | tee /tmp/large-file-output.txt
```

---

## Conclusions

1. **The primary crash path is `passDefinitions`** — extractionCache + GraphBuffer + concurrent workers combine to reach 97%+ of GOMEMLIMIT on 100K+ file repos.

2. **Full mode has no MaxFileSize guard** — a single oversized file can trigger OOM independently of total file count.

3. **GOMEMLIMIT is necessary but insufficient** — it delays crashes via GC pressure but does not prevent them. The pipeline has no active memory monitoring or adaptive throttling.

4. **The adaptive pool is throughput-optimized, not memory-safe** — it can scale to 64 workers without checking heap usage.

5. **Memory profiling hooks exist but are insufficient** — `logHeapStats()` captures 4 snapshots total; real-time monitoring during extraction is absent.
