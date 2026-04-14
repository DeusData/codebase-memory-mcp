# Phase 06: Parallel Native Suite Repair - Research

**Researched:** 2026-04-12
**Domain:** Forced-parallel native-suite failure in the pipeline FastAPI dependency edge path
**Confidence:** HIGH

<user_constraints>
## User Constraints (from roadmap and prior phase state)

### Locked Decisions
- **D-01:** Phase 06 closes only the deferred Phase 03 native-suite debt recorded in `.planning/phases/03-real-repo-semantic-verification/deferred-items.md`.
- **D-02:** The target failure is `tests/test_pipeline.c:4361` `pipeline_fastapi_depends_edges` when the suite is run with `CBM_FORCE_PIPELINE_MODE=parallel`.
- **D-03:** The fix must preserve the sequential and parallel proof behaviors already verified in Phase 03 instead of widening scope into unrelated semantic work.

### OpenCode's Discretion
- Whether the regression is best expressed by strengthening `pipeline_fastapi_depends_edges` itself or by adding a dedicated forced-parallel variant beside it.
- Whether the cache-backed FastAPI Depends pass should be invoked through a tiny shared helper or by temporarily wiring `ctx->result_cache` around the existing pass.

### Deferred Ideas (OUT OF SCOPE)
- Any new FastAPI semantic modeling beyond `Depends(func_ref)` edge preservation.
- Broad cleanup of other cache-backed parallel post-passes unless directly required to land the fix safely.
- Non-pipeline changes to proof harnesses, docs, or milestone summaries beyond what is needed to close this gap.
</user_constraints>

<phase_requirements>
## Phase Requirements

This phase has no mapped product requirement IDs. It is explicit audit-tech-debt closure for the roadmap gap in Phase 06.
</phase_requirements>

## Summary

The failing suite command reproduces exactly as the roadmap describes: `CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test` fails at `pipeline_fastapi_depends_edges` with `ASSERT(found_depends_edge)`. The captured test log shows the failure is stable and isolated to that assertion in the forced-parallel full-suite run. [VERIFIED: `~/Library/Application Support/rtk/tee/1776050433_make_-f_Makefile_cbm_test.log`]

The root cause is structural rather than data-dependent. Sequential indexing reaches `cbm_pipeline_pass_calls()`, which always invokes `cbm_pipeline_pass_fastapi_depends()` afterward. The full parallel path in `src/pipeline/pipeline.c` bypasses `cbm_pipeline_pass_calls()` entirely, going `cbm_parallel_extract()` -> `cbm_build_registry_from_cache()` -> `cbm_parallel_resolve()` -> free cache -> `cbm_pipeline_pass_k8s()`. That means the FastAPI pattern pass never runs in full parallel mode even though the result cache needed by the pass exists briefly. [VERIFIED: `src/pipeline/pass_calls.c`, `src/pipeline/pipeline.c`]

The same omission also exists in the incremental parallel branch: `src/pipeline/pipeline_incremental.c` runs extract, registry, and parallel resolve, then frees cache without invoking `cbm_pipeline_pass_fastapi_depends()`. The current suite failure is in the full pipeline test, but the safe repair surface is both parallel paths because they share the same skipped cache-backed pass pattern. [VERIFIED: `src/pipeline/pipeline_incremental.c`]

**Primary recommendation:** keep `cbm_pipeline_pass_fastapi_depends()` as the source of truth, but explicitly run it while `CBMFileResult **cache` is still alive in both full and incremental parallel flows. Avoid duplicating the Depends scanning logic inside `pass_parallel.c`; Phase 06 should restore parity by reusing the existing pass rather than creating a second implementation. [VERIFIED: `src/pipeline/pass_calls.c`][ASSUMED]

## Project Constraints (from AGENTS.md)

- Read `codemap.md` before working; it is the required repository architecture reference. [CITED: `AGENTS.md`]
- Prefer codebase-memory graph tools over manual code search for code discovery. [CITED: `/Users/shaunmcmanus/.config/opencode/AGENTS.md`]

## Relevant Code Paths

| Path | Why it matters |
|---|---|
| `src/pipeline/pass_calls.c` | Defines `cbm_pipeline_pass_fastapi_depends()` and the existing `fastapi_depends` edge insertion contract. |
| `src/pipeline/pipeline.c` | Owns the full sequential vs parallel pipeline split and currently frees the parallel cache before any FastAPI pattern pass runs. |
| `src/pipeline/pipeline_incremental.c` | Owns the incremental sequential vs parallel split and mirrors the same omission on large changed-file sets. |
| `tests/test_pipeline.c` | Contains the failing `pipeline_fastapi_depends_edges` regression and is the right place for forced-parallel coverage. |

## Architecture Patterns

### Pattern 1: Cache-backed post-processing
Use the extracted `CBMFileResult **result_cache` as the shared data source for post-extract semantic passes that need parsed calls, imports, and definitions without re-extracting files. `cbm_pipeline_pass_fastapi_depends()` already expects that cache through `ctx->result_cache`. [VERIFIED: `src/pipeline/pass_calls.c`]

### Pattern 2: One semantic implementation per behavior
The existing repo pattern is to keep semantic edge creation in one pass and invoke it from the right pipeline stage, not to duplicate behavior separately for sequential and parallel paths. Phase 06 should preserve that pattern. [VERIFIED: `src/pipeline/pass_calls.c`, `src/pipeline/pipeline.c`]

### Anti-Patterns to Avoid
- Duplicating Depends scanning logic into `pass_parallel.c`.
- Freeing the parallel cache before all cache-dependent semantic passes have finished.
- Fixing only the targeted full-path test while leaving incremental parallel mode structurally divergent.

## Common Pitfalls

### Pitfall 1: Treating the failure as a test-only issue
**What goes wrong:** A plan rewrites the test to match current parallel behavior instead of restoring the missing edge generation.
**How to avoid:** Keep the acceptance bar on the `fastapi_depends` CALLS edge existing under forced parallel. [VERIFIED: `tests/test_pipeline.c:4321-4367`]

### Pitfall 2: Re-running the pass after cache free
**What goes wrong:** The code tries to reuse `cbm_pipeline_pass_fastapi_depends()` after `cbm_free_result()` has already reclaimed parsed results.
**How to avoid:** Run the pass before the `cache[i]` free loop and restore `ctx->result_cache` bookkeeping cleanly afterward. [VERIFIED: `src/pipeline/pipeline.c:574-585`, `src/pipeline/pipeline_incremental.c:206-216`]

### Pitfall 3: Closing the suite gap but regressing Phase 03 parity
**What goes wrong:** A narrow fix lands without proving the existing forced-parallel incremental path and normal suite still pass.
**How to avoid:** Verify both `CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test` and the normal `rtk make -f Makefile.cbm test` path after the change. [CITED: `.planning/phases/03-real-repo-semantic-verification/03-VERIFICATION.md`]

## Code Examples

### Existing FastAPI Depends hook point
```c
cbm_log_info("pass.done", "pass", "calls", ...);
cbm_pipeline_pass_fastapi_depends(ctx, files, file_count);
```
Source: `src/pipeline/pass_calls.c`

### Current full parallel path omission
```c
rc = cbm_parallel_resolve(ctx, files, file_count, cache, &shared_ids, worker_count);
cbm_pipeline_extract_infra_routes(p->gbuf, files, cache, file_count);
cbm_pipeline_process_infra_bindings(p->gbuf, files, cache, file_count);
for (int i = 0; i < file_count; i++) {
    if (cache[i]) {
        cbm_free_result(cache[i]);
    }
}
```
Source: `src/pipeline/pipeline.c`

### Current incremental parallel path omission
```c
cbm_parallel_extract(ctx, changed_files, ci, cache, &shared_ids, worker_count);
cbm_build_registry_from_cache(ctx, changed_files, ci, cache);
cbm_parallel_resolve(ctx, changed_files, ci, cache, &shared_ids, worker_count);
for (int j = 0; j < ci; j++) {
    if (cache[j]) {
        cbm_free_result(cache[j]);
    }
}
```
Source: `src/pipeline/pipeline_incremental.c`

## Recommendation

Plan one focused execution wave:
1. Add an explicit forced-parallel FastAPI Depends regression in `tests/test_pipeline.c` so the deferred debt is directly exercised.
2. Restore `cbm_pipeline_pass_fastapi_depends()` on both parallel code paths while the extracted cache is still available.
3. Verify the forced-parallel full suite and the normal suite to prove the gap is closed without reopening Phase 03 behavior.

---

*Phase: 06-parallel-native-suite-repair*
*Research completed: 2026-04-12*
