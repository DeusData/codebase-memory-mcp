---
status: investigating
trigger: "Investigate the Phase 03 UAT blocker in /Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp-gdscript-support. This is diagnosis only, not code changes. The user reported that forced/parallel mode crashes with AddressSanitizer heap-use-after-free during `make test`. Relevant stack frames:
- ts_lexer_goto lexer.c:150
- ts_parser_parse parser.c:2087
- ts_parser_parse_with_options parser.c:2209
- cbm_extract_file internal/cbm/cbm.c:304
- cbm_pipeline_pass_definitions src/pipeline/pass_definitions.c:411
- cbm_pipeline_run_incremental src/pipeline/pipeline_incremental.c:396
- cbm_pipeline_run / src/pipeline/pipeline.c lines around 770 and 834
- extract_worker src/pipeline/pass_parallel.c:1261
- cbm_slab_destroy_thread src/foundation/slab_alloc.c:219
- suite_pipeline tests/test_pipeline.c around line 6306

Please inspect the code path and determine the most likely root cause, with concrete evidence. Return:
1. A concise root cause statement.
2. The specific files/functions involved.
3. Why this violates the Phase 03 forced-mode determinism expectation.
4. A minimal fix direction suitable for a gap-closure plan.
5. Any reproduction/verification command(s) that should prove the fix.
Do not edit files."
created: 2026-04-12T00:00:00Z
updated: 2026-04-12T00:22:00Z
---

## Current Focus
<!-- OVERWRITE on each update - reflects NOW -->

hypothesis: forced parallel extraction bulk-frees tree-sitter slab pages from extract_worker even though parser lexer state (notably included_ranges) can still be live/reused, so the next parse dereferences freed parser-owned memory in ts_lexer_goto
test: correlate ASan stack with tree-sitter lexer ownership and compare documented parser/slab contract against pass_parallel cleanup path
expecting: ts_lexer_goto should read parser->lexer.included_ranges, and pass_parallel should be the only path that frees all slab pages underneath parser-managed allocations
next_action: finalize diagnosis and root-cause summary

## Symptoms
<!-- Written during gathering, then IMMUTABLE -->

expected: forced/parallel mode should complete deterministically during make test without crashes
actual: AddressSanitizer reports heap-use-after-free in tree-sitter parse path during forced/parallel incremental pipeline test
errors: "AddressSanitizer heap-use-after-free", "ts_lexer_goto lexer.c:150", "ts_parser_parse parser.c:2087", "ts_parser_parse_with_options parser.c:2209"
reproduction: run make test in Phase 03 forced/parallel mode path; stack includes suite_pipeline around tests/test_pipeline.c:6306
started: Phase 03 UAT blocker

## Eliminated
<!-- APPEND only - prevents re-investigating -->

## Evidence
<!-- APPEND only - facts discovered -->

- timestamp: 2026-04-12T00:05:00Z
  checked: codemap.md, src/codemap.md, internal/codemap.md
  found: pipeline orchestrates full/incremental indexing through src/pipeline while internal/cbm/cbm.c owns tree-sitter parsing and extraction
  implication: the reported stack frames span the exact boundary between pipeline worker lifecycle and tree-sitter extraction ownership

- timestamp: 2026-04-12T00:06:00Z
  checked: internal/cbm/cbm.c:190-234,245-427,441-446
  found: cbm_extract_file reuses a thread-local parser via get_thread_parser(), parses with ts_parser_parse_with_options(), retains the resulting TSTree on result->cached_tree, and only cbm_free_tree/cbm_free_result delete that tree; cbm_destroy_thread_parser deletes the TLS parser separately
  implication: parser lifetime and tree lifetime are intentionally decoupled, so cleanup ordering matters if both allocate from the slab allocator

- timestamp: 2026-04-12T00:07:00Z
  checked: src/pipeline/pass_parallel.c:1205-1261
  found: extract_worker frees the tree, then immediately calls cbm_destroy_thread_parser(), cbm_slab_reclaim(), and later cbm_slab_destroy_thread() on the worker thread after every file / at worker exit
  implication: forced parallel mode uniquely performs aggressive per-file and per-thread slab teardown that sequential definitions path does not perform

- timestamp: 2026-04-12T00:08:00Z
  checked: src/foundation/slab_alloc.c:202-245
  found: cbm_slab_install globally redirects tree-sitter allocators to slab_*; cbm_slab_reclaim and cbm_slab_destroy_thread free all slab pages outright
  implication: any remaining tree-sitter allocation not fully released before reclaim/destroy becomes a dangling pointer into freed heap pages

- timestamp: 2026-04-12T00:09:00Z
  checked: src/pipeline/pipeline_incremental.c:178-225,278-409 and tests/test_pipeline.c:5428-5464,6303-6307
  found: incremental path uses cbm_parallel_extract when enough files/workers are available, and the failing suite location includes incremental_new_file_added among incremental tests
  implication: the crash is consistent with incremental forced/parallel extraction, not unrelated postpasses

- timestamp: 2026-04-12T00:18:00Z
  checked: forced parallel ASan run via `CBM_FORCE_PIPELINE_MODE=parallel make -f Makefile.cbm test`
  found: local run did not reproduce before output truncation, but it exercised many forced-parallel pipeline invocations successfully
  implication: the bug is likely timing/data dependent rather than a universal parse failure; code-path evidence remains primary

- timestamp: 2026-04-12T00:20:00Z
  checked: internal/cbm/vendored/ts_runtime/src/lexer.c:148-150,384-391,450-475 and parser.c:2047-2088
  found: the crashing frame ts_lexer_goto reads parser lexer field `self->included_ranges`; that buffer is parser-owned, allocated via ts_realloc in ts_lexer_set_included_ranges, freed by ts_lexer_delete during parser teardown, and read again on every parse from ts_lexer_set_input -> ts_lexer_goto
  implication: the ASan read is coming from stale parser/lexer state, not from result-cache arena strings or graph-buffer data

- timestamp: 2026-04-12T00:21:00Z
  checked: internal/cbm/cbm.h:384-390, internal/cbm/cbm.c:218-234, src/pipeline/pass_parallel.c:1243-1261
  found: cbm.h documents `cbm_reset_thread_parser()` must run before slab reset and `cbm_destroy_thread_parser()` should be called on worker thread exit, but pass_parallel instead destroys parser and reclaims/destroys the entire slab from inside the per-file worker cleanup path based on the unverified assumption that zero live tree-sitter allocations remain
  implication: forced parallel mode violates the documented allocator contract and is the unique code path capable of producing the reported heap-use-after-free

## Resolution
<!-- OVERWRITE as understanding evolves -->

root_cause: pass_parallel's forced-parallel cleanup path aggressively reclaims/destroys the thread-local slab allocator underneath tree-sitter parser state. ASan's read in ts_lexer_goto maps to parser->lexer.included_ranges, proving the dangling object is parser-owned memory. This cleanup only exists in extract_worker and violates the documented parser/slab lifetime contract.
fix: diagnose only; likely fix is to stop per-file slab page destruction in extract_worker and honor the reset-before-reset / destroy-on-worker-exit contract for the thread-local parser.
verification: prove by rerunning forced parallel incremental/pipeline tests under ASan after removing unsafe per-file slab reclaim; crash should disappear while sequential and forced-parallel outputs remain identical.
files_changed: []
