---
phase: 03-real-repo-semantic-verification
verified: 2026-04-12T22:15:00Z
status: passed
score: 17/17 must-haves verified
---

# Phase 3: Real-Repo Semantic Verification Verification Report

**Phase Goal:** Maintainers can verify that real Godot demo projects expose the required GDScript definitions and relationships through the parser-to-MCP path in both sequential and parallel indexing modes.
**Verified:** 2026-04-12T22:15:00Z
**Status:** passed
**Re-verification:** No — prior report existed, but it had no open `gaps` section

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Maintainer can confirm non-zero GDScript class and method extraction on real proof targets and review representative samples. | ✓ VERIFIED | `scripts/gdscript-proof.sh:2466-2480,2583-2589` builds SEM-01 from wrapper counts/samples; `scripts/test_gdscript_proof_semantic_parity.py:168-188` requires non-zero class/method counts and representative samples in both modes. |
| 2 | Maintainer can confirm same-script method calls resolve on real proof targets through captured MCP query results. | ✓ VERIFIED | `scripts/gdscript-proof.sh:2482,2363-2381` compares same-script counts and representative edges from wrapper data; `scripts/test_gdscript_proof_semantic_parity.py:190-197` requires SEM-02 count and edge evidence. |
| 3 | Maintainer can confirm queryable `extends` inheritance and `.gd` `preload`/`load` dependency relationships on real proof targets. | ✓ VERIFIED | `scripts/gdscript-proof.sh:2483-2484,2315-2319,2363-2381` compares `gd-inherits`/`gd-imports` counts plus `gd-inherits-edges`/`gd-import-edges`; parity regression asserts SEM-03 and SEM-04 structure. |
| 4 | Maintainer can confirm signal declarations and conservative signal-call behavior on real proof targets. | ✓ VERIFIED | `scripts/gdscript-proof.sh:2485,2315-2319,2363-2381` compares `signal-calls` counts plus `signal-call-edges`; parity regression requires SEM-05 evidence. |
| 5 | Maintainer can compare sequential and parallel indexing runs and see the core GDScript behaviors remain consistent across both paths. | ✓ VERIFIED | `scripts/gdscript-proof.sh:2390-2505` computes per-label semantic parity and SEM-06 from sequential/parallel wrapper artifacts; `docs/superpowers/proofs/gdscript-real-project-validation.md:219-230` documents the review flow. |
| 6 | The pipeline exposes a deterministic sequential/parallel override that the canonical proof workflow can consume. | ✓ VERIFIED | `src/pipeline/pipeline.c:118-157,802-835` implements `cbm_pipeline_select_mode` with `CBM_FORCE_PIPELINE_MODE`; `scripts/gdscript-proof.sh:930-937` passes the env var into `index_repository`. |
| 7 | The pipeline records which execution mode was requested and which mode actually ran. | ✓ VERIFIED | `src/pipeline/pipeline.c:807-827` logs `pipeline.mode_selection`; `scripts/gdscript-proof.sh:642-644,1078-1080,1227-1232` persists `requested_mode`, `actual_mode`, and `comparison_label`. |
| 8 | Forced-mode behavior is covered by native regressions so proof parity work can rely on it. | ✓ VERIFIED | `tests/test_parallel.c:800-850,1204-1237` adds forced auto/sequential/parallel tests and registers them in `SUITE(parallel)`. |
| 9 | Maintainer can run the canonical proof harness once and get sequential plus parallel evidence for all four approved manifest repos. | ✓ VERIFIED | `docs/superpowers/proofs/gdscript-good-tier-manifest.json:27-280` defines four approved repos; `scripts/gdscript-proof.sh:328-334` duplicates each manifest repo into sequential+parallel runs; `scripts/gdscript-proof.sh:2143-2167` flags missing manifest labels. |
| 10 | Maintainer can review non-zero counts and representative samples for classes and methods from wrapper-backed proof output. | ✓ VERIFIED | `scripts/gdscript-proof.sh:2466-2480,2583-2589` emits count/sample review data into `semantic-parity.json` and `semantic-parity.md`; `docs/superpowers/proofs/gdscript-real-project-validation.md:223-229,289-298` tells operators to inspect it. |
| 11 | Maintainer can compare same-script calls, inherits, imports, and signal behavior across sequential and parallel runs without relying on aggregate pass/fail only. | ✓ VERIFIED | `scripts/gdscript-proof.sh:2426-2505,2591-2601` compares counts plus representative edges for SEM-02..SEM-05 and derives SEM-06 separately from aggregate verdicts. |
| 12 | Maintainer can rerun automated proof regressions and catch broken semantic-parity artifacts before approval. | ✓ VERIFIED | `scripts/test_gdscript_proof_manifest_contract.py:153-275` and `scripts/test_gdscript_proof_semantic_parity.py:150-215` run the harness, read disk artifacts, and fail on contract drift. |
| 13 | The standard repo test path exercises the new proof-layer regressions. | ✓ VERIFIED | `Makefile.cbm:423-427` runs manifest-contract, same-script, and semantic-parity regressions under `test`; both `rtk make -f Makefile.cbm test` and `CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test` completed successfully in this verification. |
| 14 | The operator docs and checklist say exactly how to review counts, samples, and sequential-versus-parallel parity. | ✓ VERIFIED | `docs/superpowers/proofs/gdscript-real-project-validation.md:219-230,284-304` and `docs/superpowers/proofs/gdscript-good-tier-checklist.md:17-23,32-33` define the review bar, artifact order, and incomplete-run fallback. |
| 15 | Forcing `CBM_FORCE_PIPELINE_MODE=parallel` through the incremental new-file path completes without an AddressSanitizer heap-use-after-free. | ✓ VERIFIED | `src/pipeline/pass_parallel.c:1226-1251` keeps parser/slab state alive until worker exit; `tests/test_pipeline.c:5467-5518` exercises the exact new-file-added forced-parallel rerun path and asserts successful indexing plus queryable output. |
| 16 | Forced parallel runs keep requested-versus-actual mode behavior deterministic instead of crashing mid-run. | ✓ VERIFIED | `src/pipeline/pipeline.c:143-151,807-835` keeps requested/selected mode explicit; `CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test` succeeded, showing the forced path runs rather than aborting. |
| 17 | The native test suite catches the worker-lifetime regression before Phase 03 proof runs are used again. | ✓ VERIFIED | `tests/test_pipeline.c:6357-6362` wires `incremental_new_file_added_forced_parallel` into the default suite, and both full test commands in this verification exercised it successfully. |

**Score:** 17/17 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `src/pipeline/pipeline.c` | Deterministic execution-mode selection | ✓ VERIFIED | Exists, substantive helper logic, runtime env handling, selection logging, and sequential/parallel dispatch. |
| `src/pipeline/pipeline_internal.h` | Mode-selection contract shared with tests | ✓ VERIFIED | Exports `cbm_pipeline_mode`, `cbm_pipeline_mode_selection_t`, and `cbm_pipeline_select_mode(...)`. |
| `tests/test_parallel.c` | Native forced-mode regressions | ✓ VERIFIED | Contains forced-mode tests and suite registration. |
| `scripts/gdscript-proof.sh` | Dual-mode manifest proof execution and semantic comparison summaries | ✓ VERIFIED | Iterates manifest repos in both modes, writes mode-aware metadata, `semantic_pairs`, and additive parity artifacts from wrapper JSON. |
| `docs/superpowers/proofs/gdscript-real-project-validation.md` | Updated artifact layout and review flow | ✓ VERIFIED | Documents approval-bearing workflow, parity artifacts, mode pairing, and exact review bar. |
| `scripts/test_gdscript_proof_semantic_parity.py` | Proof-harness parity regression | ✓ VERIFIED | Runs the harness and asserts `semantic-parity.json`/`.md` plus SEM-01..SEM-06 evidence. |
| `Makefile.cbm` | Standard test-path wiring | ✓ VERIFIED | Default `test` target runs proof regressions. |
| `docs/superpowers/proofs/gdscript-good-tier-checklist.md` | Phase 03 review checklist | ✓ VERIFIED | Requires all four repos, both modes, counts/samples, and incomplete-run inspection. |
| `src/pipeline/pass_parallel.c` | Worker-lifetime parser/slab cleanup contract | ✓ VERIFIED | Resets parser between files and destroys parser before thread slab teardown at worker exit. |
| `tests/test_pipeline.c` | Forced-parallel incremental regression | ✓ VERIFIED | Adds the exact new-file-added forced-parallel regression and registers it in the main suite. |

### Key Link Verification

| From | To | Via | Status | Details |
| --- | --- | --- | --- | --- |
| `src/pipeline/pipeline.c` | `tests/test_parallel.c` | shared mode-selection helper | ✓ WIRED | `cbm_pipeline_select_mode` is declared in the shared header and exercised directly in native tests. |
| `scripts/gdscript-proof.sh` | `src/pipeline/pipeline.c` | `CBM_FORCE_PIPELINE_MODE` | ✓ WIRED | Harness injects the env var before invoking `index_repository`; pipeline reads it before selecting mode. |
| `scripts/gdscript-proof.sh` | `docs/superpowers/proofs/gdscript-good-tier-manifest.json` | full four-repo manifest iteration | ✓ WIRED | Script duplicates manifest entries into sequential/parallel repo runs and checks for missing manifest labels. |
| `Makefile.cbm` | `scripts/test_gdscript_proof_semantic_parity.py` | test target | ✓ WIRED | Proof parity regression is part of the default `test` target. |
| `docs/superpowers/proofs/gdscript-good-tier-checklist.md` | `docs/superpowers/proofs/gdscript-real-project-validation.md` | operator review instructions | ✓ WIRED | Checklist and runbook use the same sequential/parallel semantic review contract. |
| `tests/test_pipeline.c` | `src/pipeline/pipeline.c` | forced-parallel incremental pipeline run | ✓ WIRED | Regression sets `CBM_FORCE_PIPELINE_MODE=parallel` and calls `cbm_pipeline_run()` twice on the incremental path. |
| `src/pipeline/pass_parallel.c` | `internal/cbm/cbm.h` | parser reset/destroy ordering | ✓ WIRED | Worker logic calls `cbm_reset_thread_parser()` per file and `cbm_destroy_thread_parser()` before slab teardown at exit. |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
| --- | --- | --- | --- | --- |
| `scripts/gdscript-proof.sh` | `reports[label].requirements` | Real `queries/*.json` wrapper artifacts plus `repo-meta.json` mode metadata | Yes — `load_wrapper`, `count_value`, `sample_values`, and `edge_values` read actual wrapper rows before writing `semantic-parity.json`/`.md` (`2307-2505`). | ✓ FLOWING |
| `scripts/test_gdscript_proof_manifest_contract.py` | `run_index`, `semantic_pairs`, `manifest_metas` | Real subprocess run of `scripts/gdscript-proof.sh` on a temp repo fixture | Yes — test reads generated `repo-meta.json`, `run-index.json`, and `semantic-parity.json` from disk and asserts mode-aware metadata. | ✓ FLOWING |
| `scripts/test_gdscript_proof_semantic_parity.py` | `payload`, `requirements` | Real subprocess run of `scripts/gdscript-proof.sh` on a temp repo fixture | Yes — test reads emitted `semantic-parity.json` and checks SEM-01..SEM-06 structure and non-zero/sample evidence. | ✓ FLOWING |
| `tests/test_pipeline.c` | `extra_count` | Real forced-parallel pipeline runs persisted into the test SQLite store | Yes — regression reruns indexing after adding `extra.go` and queries the store for `Extra`, proving the incremental parallel path produced usable persisted data. | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Manifest run emits dual-mode metadata and parity artifacts | `python3 -B scripts/test_gdscript_proof_manifest_contract.py` | `PASS` | ✓ PASS |
| Semantic parity artifact exposes SEM-01..SEM-06 evidence | `python3 -B scripts/test_gdscript_proof_semantic_parity.py` | `PASS` | ✓ PASS |
| Standard repo test path runs the full regression stack | `rtk make -f Makefile.cbm test` | Command succeeded (output noisy/truncated, exit 0) | ✓ PASS |
| Forced-parallel test path completes without reintroducing the lifetime crash | `CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test` | Command succeeded (output noisy/truncated, exit 0) | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| `SEM-01` | `03-02`, `03-03` | Non-zero GDScript class and method extraction with reviewable samples | ✓ SATISFIED | `scripts/gdscript-proof.sh:2466-2480,2583-2589`; `scripts/test_gdscript_proof_semantic_parity.py:168-188`; docs/checklist require counts and samples. |
| `SEM-02` | `03-02`, `03-03` | Same-script method calls resolve on a real proof target | ✓ SATISFIED | `scripts/gdscript-proof.sh:2482,2363-2381`; parity regression requires SEM-02 count and representative edges. |
| `SEM-03` | `03-02`, `03-03` | Queryable `extends` inheritance relationships | ✓ SATISFIED | `scripts/gdscript-proof.sh:2483,2315-2319`; parity regression requires SEM-03 count and representative edges. |
| `SEM-04` | `03-02`, `03-03` | Queryable `.gd` dependency relationships from `preload`/`load` | ✓ SATISFIED | `scripts/gdscript-proof.sh:2484,2315-2319`; parity regression requires SEM-04 count and representative edges. |
| `SEM-05` | `03-02`, `03-03` | Signal declarations and conservative signal-call behavior | ✓ SATISFIED | `scripts/gdscript-proof.sh:2485,2315-2319`; parity regression requires SEM-05 count and representative edges. |
| `SEM-06` | `03-01`, `03-02`, `03-03`, `03-04` | Core GDScript behaviors stay consistent across sequential and parallel indexing | ✓ SATISFIED | Native deterministic mode forcing, dual-mode parity artifacts, regression-backed proof checks, and the forced-parallel worker-lifetime fix all support SEM-06. |

All requirement IDs declared in Phase 03 plans are present in `REQUIREMENTS.md`, and `REQUIREMENTS.md` lists no extra Phase 3 requirement IDs outside `SEM-01`..`SEM-06`.

### Anti-Patterns Found

No blocker anti-patterns found in phase-modified files. Targeted scans found no TODO/FIXME/placeholder implementations tied to the Phase 03 artifacts.

### Gaps Summary

No implementation gaps found. The codebase contains deterministic sequential/parallel mode forcing in the native pipeline, dual-mode proof execution and wrapper-backed semantic parity reporting, regression coverage in the standard test path, operator documentation/checklist guidance, and the forced-parallel worker-lifetime fix needed to keep sequential and parallel verification trustworthy.

---

_Verified: 2026-04-12T22:15:00Z_
_Verifier: OpenCode (gsd-verifier)_
