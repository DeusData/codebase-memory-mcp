---
status: diagnosed
phase: 03-real-repo-semantic-verification
source:
  - 03-real-repo-semantic-verification-01-SUMMARY.md
  - 03-real-repo-semantic-verification-02-SUMMARY.md
  - 03-real-repo-semantic-verification-03-SUMMARY.md
started: 2026-04-12T19:25:01Z
updated: 2026-04-12T20:03:57Z
---

## Current Test

[testing complete]

## Tests

### 1. Default Test Path Covers Phase 03
expected: Running `rtk make -f Makefile.cbm test` should complete successfully and include the Phase 03 proof regressions in the standard verification path, so maintainers do not need a separate semantic-parity-only command.
result: pass

### 2. Manifest Proof Run Emits Dual-Mode Artifacts
expected: Running the canonical manifest proof workflow should create both sequential and parallel artifact runs for each approved repo, plus machine-addressable pairing metadata rather than a single combined artifact per repo.
result: pass

### 3. Semantic Parity Reports Are Reviewable
expected: The generated semantic parity artifacts should expose reviewable counts and representative samples for SEM-01 through SEM-06, so a maintainer can inspect parity without reverse-engineering wrapper JSON by hand.
result: pass

### 4. Forced Mode Selection Is Deterministic
expected: Forcing `CBM_FORCE_PIPELINE_MODE=sequential` or `parallel` should produce deterministic requested-versus-actual mode behavior, including an explicit failure instead of a silent fallback when forced parallel cannot run.
result: issue
reported: "When using 'parallel' I got this error: level=info msg=incremental.registry_seed symbols=7 elapsed_ms=0\nlevel=info msg=incremental.mode mode=sequential changed=1\nlevel=info msg=pass.start pass=definitions files=1\n=================================================================\n==93906==ERROR: AddressSanitizer: heap-use-after-free on address 0x6310016187dc at pc 0x000105f4f37c bp 0x00016b56ba30 sp 0x00016b56ba28\nREAD of size 4 at 0x6310016187dc thread T0\n    #0 0x000105f4f378 in ts_lexer_goto lexer.c:150\n    #1 0x000105f64684 in ts_parser_parse parser.c:2087\n    #2 0x000105f73e3c in ts_parser_parse_with_options parser.c:2209\n    #3 0x000104c4f4c4 in cbm_extract_file cbm.c:304\n    #4 0x000104a4dd30 in cbm_pipeline_pass_definitions pass_definitions.c:411\n    #5 0x000104a336f4 in cbm_pipeline_run_incremental pipeline_incremental.c:396\n    #6 0x000104a2baec in cbm_pipeline_run pipeline.c:770\n    #7 0x000104786cf4 in suite_pipeline test_pipeline.c:6306\n    #8 0x000104624fe8 in main test_main.c:98\n    #9 0x000184cbfda0 in start+0x1b4c (dyld:arm64e+0x1fda0)\n\n0x6310016187dc is located 65500 bytes inside of 65544-byte region [0x631001608800,0x631001618808)\nfreed by thread T0 here:\n    #0 0x000110b09258 in free+0x7c (libclang_rt.asan_osx_dynamic.dylib:arm64e+0x41258)\n    #1 0x000104906828 in cbm_slab_destroy_thread slab_alloc.c:219\n    #2 0x000104a38f5c in extract_worker pass_parallel.c:1261\n    #3 0x000104a350d4 in cbm_parallel_for worker_pool.c:116\n    #4 0x000104a35c3c in cbm_parallel_extract pass_parallel.c:1325\n    #5 0x000104a2dd50 in run_parallel_pipeline pipeline.c:552\n    #6 0x000104a2d1b8 in cbm_pipeline_run pipeline.c:834\n    #7 0x000104786b54 in suite_pipeline test_pipeline.c:6306\n    #8 0x000104624fe8 in main test_main.c:98\n    #9 0x000184cbfda0 in start+0x1b4c (dyld:arm64e+0x1fda0)\n\npreviously allocated by thread T0 here:\n    #0 0x000110b09164 in malloc+0x78 (libclang_rt.asan_osx_dynamic.dylib:arm64e+0x41164)\n    #1 0x000104905fac in slab_malloc slab_alloc.c:115\n    #2 0x000105f4ce50 in ts_lexer_set_included_ranges lexer.c:471\n    #3 0x000105f5c1b0 in ts_parser_new parser.c:1937\n    #4 0x000104c4f33c in cbm_extract_file cbm.c:275\n    #5 0x00010476d2ec in suite_pipeline test_pipeline.c:6256\n    #6 0x000104624fe8 in main test_main.c:98\n    #7 0x000184cbfda0 in start+0x1b4c (dyld:arm64e+0x1fda0)\n\nSUMMARY: AddressSanitizer: heap-use-after-free lexer.c:150 in ts_lexer_goto\nShadow bytes around the buggy address:\n  0x631001618500: fd fd fd fd fd fd fd fd fd fd fd fd fd fd fd fd\n  0x631001618580: fd fd fd fd fd fd fd fd fd fd fd fd fd fd fd fd\n  0x631001618600: fd fd fd fd fd fd fd fd fd fd fd fd fd fd fd fd\n  0x631001618680: fd fd fd fd fd fd fd fd fd fd fd fd fd fd fd fd\n  0x631001618700: fd fd fd fd fd fd fd fd fd fd fd fd fd fd fd fd\n=>0x631001618780: fd fd fd fd fd fd fd fd fd fd fd[fd]fd fd fd fd\n  0x631001618800: fd fa fa fa fa fa fa fa fa fa fa fa fa fa fa fa\n  0x631001618880: fa fa fa fa fa fa fa fa fa fa fa fa fa fa fa fa\n  0x631001618900: fa fa fa fa fa fa fa fa fa fa fa fa fa fa fa fa\n  0x631001618980: fa fa fa fa fa fa fa fa fa fa fa fa fa fa fa fa\n  0x631001618a00: fa fa fa fa fa fa fa fa fa fa fa fa fa fa fa fa\nShadow byte legend (one shadow byte represents 8 application bytes):\n  Addressable:           00\n  Partially addressable: 01 02 03 04 05 06 07 \n  Heap left redzone:       fa\n  Freed heap region:       fd\n  Stack left redzone:      f1\n  Stack mid redzone:       f2\n  Stack right redzone:     f3\n  Stack after return:      f5\n  Stack use after scope:   f8\n  Global redzone:          f9\n  Global init order:       f6\n  Poisoned by user:        f7\n  Container overflow:      fc\n  Array cookie:            ac\n  Intra object redzone:    bb\n  ASan internal:           fe\n  Left alloca redzone:     ca\n  Right alloca redzone:    cb\n==93906==ABORTING\n/bin/sh: line 1: 93906 Abort trap: 6           build/c/test-runner\nmake: *** [test] Error 134\ncd /Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp-gdscript-support && build/c/test-runner\n  codebase-memory-mcp  C test suite\n=== arena ===\n  arena_init_default                                     PASS\n  arena_init_sized                                       PASS"
severity: blocker

## Summary

total: 4
passed: 3
issues: 1
pending: 0
skipped: 0
blocked: 0

## Gaps

- truth: "Forcing `CBM_FORCE_PIPELINE_MODE=sequential` or `parallel` should produce deterministic requested-versus-actual mode behavior, including an explicit failure instead of a silent fallback when forced parallel cannot run."
  status: failed
  reason: "User reported: When using 'parallel' I got an AddressSanitizer heap-use-after-free in Tree-sitter parsing after the parallel extraction worker freed thread slab memory, causing `make test` to abort instead of giving deterministic forced-mode behavior."
  severity: blocker
  test: 4
  root_cause: "Forced parallel extraction in `src/pipeline/pass_parallel.c:extract_worker` reclaims and destroys thread-local Tree-sitter slab/parser state (`cbm_destroy_thread_parser`, `cbm_slab_reclaim`, then `cbm_slab_destroy_thread`) too aggressively relative to parser-owned lexer allocations used by later parses, so `ts_lexer_goto` reads freed `included_ranges` memory during forced parallel runs."
  artifacts:
    - path: "src/pipeline/pass_parallel.c"
      issue: "Per-file worker cleanup bulk-frees parser/slab state inside the extraction loop."
    - path: "internal/cbm/cbm.c"
      issue: "Thread-local parser lifecycle is reused across files, so freeing slab pages mid-worker can invalidate parser-owned allocations."
    - path: "src/foundation/slab_alloc.c"
      issue: "`cbm_slab_reclaim` and `cbm_slab_destroy_thread` free all slab pages, matching the ASan free site."
    - path: "internal/cbm/vendored/ts_runtime/src/lexer.c"
      issue: "`ts_lexer_goto` dereferences parser-owned `included_ranges`, matching the ASan read site."
  missing:
    - "Keep parser/slab state alive for the full worker lifetime instead of reclaiming it after each file."
    - "If memory reset remains necessary, use the documented parser-reset-before-slab-reset contract instead of bulk slab free while parser-owned allocations may still be live."
    - "Add a regression that forces parallel mode through the incremental/new-file-added path without crashing under ASan."
  debug_session: ".planning/debug/phase-03-uat-forced-mode-asan.md"
