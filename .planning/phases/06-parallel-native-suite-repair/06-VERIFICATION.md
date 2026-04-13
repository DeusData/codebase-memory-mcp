---
phase: 06-parallel-native-suite-repair
verified: 2026-04-13T04:32:58Z
status: passed
score: 3/3 must-haves verified
---

# Phase 6: Parallel Native Suite Repair Verification Report

**Phase Goal:** Maintainers can run the full native suite with `CBM_FORCE_PIPELINE_MODE=parallel` without the deferred `pipeline_fastapi_depends_edges` failure that remained after Phase 03.
**Verified:** 2026-04-13T04:32:58Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | `tests/test_pipeline.c:4361` `pipeline_fastapi_depends_edges` passes when the full suite is run with `CBM_FORCE_PIPELINE_MODE=parallel`. | ✓ VERIFIED | `tests/test_pipeline.c:4418-4429` adds `pipeline_fastapi_depends_edges_forced_parallel`; `tests/test_pipeline.c:6418-6419` registers both FastAPI tests; `CBM_FORCE_PIPELINE_MODE=parallel make -f Makefile.cbm test` exited `0`. |
| 2 | FastAPI `Depends(func)` CALLS edges are emitted on the full parallel pipeline path instead of only on the sequential path. | ✓ VERIFIED | `src/pipeline/pipeline.c:538-545` wires live parallel cache into `ctx->result_cache`; `src/pipeline/pipeline.c:583-589` runs `cbm_pipeline_pass_fastapi_depends()` after `cbm_parallel_resolve()`; `src/pipeline/pass_calls.c:1210-1242` reads cached results and `src/pipeline/pass_calls.c:1169-1170` inserts `CALLS` edges with `"strategy":"fastapi_depends"`. |
| 3 | The repair preserves existing sequential behavior and the already-verified forced-parallel incremental path. | ✓ VERIFIED | `make -f Makefile.cbm test` exited `0`; `src/pipeline/pipeline_incremental.c:212-217` runs the same FastAPI pass before cache teardown on incremental parallel flow; `tests/test_pipeline.c:5530-5580` and `6424-6425` keep forced-parallel incremental coverage in-suite. |

**Score:** 3/3 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `src/pipeline/pipeline.c` | Full parallel orchestration keeps cache-backed FastAPI edge extraction alive before cache teardown | ✓ VERIFIED | `run_fastapi_depends_from_cache()` temporarily exposes the parallel cache, and `run_parallel_pipeline()` invokes it before freeing `cache`. |
| `src/pipeline/pipeline_incremental.c` | Incremental parallel orchestration matches the full-path FastAPI edge contract | ✓ VERIFIED | Incremental parallel branch saves `ctx->result_cache`, runs `cbm_pipeline_pass_fastapi_depends()`, then restores the original pointer before freeing cache entries. |
| `tests/test_pipeline.c` | Regression coverage for FastAPI Depends edges under forced parallel | ✓ VERIFIED | Shared helper `run_fastapi_depends_edge_case()` supports both sequential and forced-parallel tests, and the dedicated forced-parallel regression is registered in the suite. |

### Key Link Verification

| From | To | Via | Status | Details |
| --- | --- | --- | --- | --- |
| `src/pipeline/pipeline.c` | `src/pipeline/pass_calls.c` | cache-backed FastAPI Depends post-pass on the full parallel path | ✓ WIRED | `gsd-tools verify key-links` confirmed the required pattern in source. |
| `src/pipeline/pipeline_incremental.c` | `src/pipeline/pass_calls.c` | cache-backed FastAPI Depends post-pass on the incremental parallel path | ✓ WIRED | `gsd-tools verify key-links` confirmed the required pattern in source. |
| `tests/test_pipeline.c` | `src/pipeline/pipeline.c` | forced parallel full-suite regression for FastAPI Depends edges | ✓ WIRED | `gsd-tools verify key-links` confirmed the forced-parallel pattern in source. |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
| --- | --- | --- | --- | --- |
| `src/pipeline/pipeline.c` | `cache` exposed via `ctx->result_cache` | `cbm_parallel_extract()` populates `CBMFileResult **cache` before `cbm_pipeline_pass_fastapi_depends()` runs | Yes — `pass_calls.c` consumes `result->calls`/`result->defs` from cached extraction output | ✓ FLOWING |
| `src/pipeline/pipeline_incremental.c` | `cache` exposed via `ctx->result_cache` | Incremental `cbm_parallel_extract()` populates changed-file cache before the same pass runs | Yes — identical pass consumes live cached extraction output before teardown | ✓ FLOWING |
| `tests/test_pipeline.c` | `found_depends_edge` | `cbm_pipeline_run()` writes DB edges, then `cbm_store_find_edges_by_type()` reads `CALLS` edges and scans for `fastapi_depends` | Yes — assertion depends on real persisted edge data, not hardcoded fixtures | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Default native suite still passes | `make -f Makefile.cbm test` | Exit `0` | ✓ PASS |
| Forced-parallel full native suite passes | `CBM_FORCE_PIPELINE_MODE=parallel make -f Makefile.cbm test` | Exit `0` | ✓ PASS |
| Native C runner passes in forced parallel mode | `CBM_FORCE_PIPELINE_MODE=parallel ./build/c/test-runner` | Exit `0`, summary included `2798 passed` | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| None declared | `06-01-PLAN.md` | `requirements: []`; this phase is audit tech-debt closure only | ✓ SATISFIED | `.planning/REQUIREMENTS.md:79` states Phase 6 does not remap any v1 requirement coverage; no requirement IDs were declared, and no orphaned Phase 6 requirement IDs were found. |

### Anti-Patterns Found

No goal-blocking anti-patterns found in the modified Phase 6 artifacts.

### Gaps Summary

No implementation gaps found. The FastAPI Depends repair is present on both full and incremental parallel branches, the forced-parallel regression exists in-suite, and both default and forced-parallel native suite commands pass when run directly through `make`.

---

_Verified: 2026-04-13T04:32:58Z_
_Verifier: OpenCode (gsd-verifier)_
