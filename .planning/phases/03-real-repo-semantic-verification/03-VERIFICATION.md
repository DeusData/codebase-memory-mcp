---
phase: 03-real-repo-semantic-verification
verified: 2026-04-12T18:04:23Z
status: passed
score: 11/11 must-haves verified
---

# Phase 3: Real-Repo Semantic Verification Verification Report

**Phase Goal:** Maintainers can verify that real Godot demo projects expose the required GDScript definitions and relationships through the parser-to-MCP path in both sequential and parallel indexing modes.
**Verified:** 2026-04-12T18:04:23Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Maintainer can confirm non-zero GDScript class and method extraction on real proof targets and review representative samples. | ✓ VERIFIED | `scripts/gdscript-proof.sh:2466-2480,2583-2589` builds SEM-01 from `gd-classes`, `gd-methods`, `gd-class-sample`, `gd-method-sample`; `scripts/test_gdscript_proof_semantic_parity.py:168-188` asserts non-zero counts and samples. |
| 2 | Maintainer can confirm same-script method calls resolve on real proof targets through captured MCP query results. | ✓ VERIFIED | `scripts/gdscript-proof.sh:2482,2591-2601` compares `gd-same-script-calls`; `scripts/test_gdscript_proof_semantic_parity.py:190-197` requires SEM-02 count and representative edges. |
| 3 | Maintainer can confirm queryable `extends` inheritance and `.gd` `preload`/`load` dependency relationships on real proof targets. | ✓ VERIFIED | `scripts/gdscript-proof.sh:2483-2484,2591-2601` compares `gd-inherits` and `gd-imports`; parity test requires SEM-03 and SEM-04 entries with counts and representative edges. |
| 4 | Maintainer can confirm signal declarations and conservative signal-call behavior on real proof targets. | ✓ VERIFIED | `scripts/gdscript-proof.sh:2485,2591-2601` compares `signal-calls`; parity test requires SEM-05 count and representative edges. |
| 5 | Maintainer can compare sequential and parallel indexing runs and see core GDScript behaviors remain consistent across both paths. | ✓ VERIFIED | `src/pipeline/pipeline.c:802-835` selects forced/auto modes; `scripts/gdscript-proof.sh:333-334,1264-1303,2390-2505` emits paired sequential/parallel artifacts, `semantic_pairs`, and SEM-06 outcome. |
| 6 | The pipeline exposes a deterministic sequential/parallel override that the canonical proof workflow can consume. | ✓ VERIFIED | `src/pipeline/pipeline.c:118-157,802-835` implements `CBM_FORCE_PIPELINE_MODE`; `scripts/gdscript-proof.sh:934-937` passes the env var to `index_repository`. |
| 7 | The pipeline records which execution mode was requested and which mode actually ran. | ✓ VERIFIED | `src/pipeline/pipeline.c:809-827` logs `pipeline.mode_selection`; `scripts/gdscript-proof.sh:1077-1080,1227-1233` persists `requested_mode`/`actual_mode`. |
| 8 | Forced-mode behavior is covered by native regressions. | ✓ VERIFIED | `tests/test_parallel.c:800-849,1235-1237` adds forced-mode tests and suite registration. |
| 9 | Maintainer can rerun automated proof regressions and catch broken semantic-parity artifacts before approval. | ✓ VERIFIED | `scripts/test_gdscript_proof_manifest_contract.py:138-275` and `scripts/test_gdscript_proof_semantic_parity.py:131-216` execute the harness and validate emitted artifacts. |
| 10 | The standard repo test path exercises the new proof-layer regressions. | ✓ VERIFIED | `Makefile.cbm:423-427` runs manifest-contract, same-script, and semantic-parity proof tests under `test`; `rtk make -f Makefile.cbm test` returned `STATUS=0`. |
| 11 | Operator docs and checklist say exactly how to review counts, samples, and sequential-vs-parallel parity. | ✓ VERIFIED | `docs/superpowers/proofs/gdscript-real-project-validation.md:219-230,284-304` documents the review flow; `docs/superpowers/proofs/gdscript-good-tier-checklist.md:17-23` encodes the gates. |

**Score:** 11/11 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `src/pipeline/pipeline.c` | Deterministic execution-mode selection | ✓ VERIFIED | Exists, substantive helper + runtime wiring, logs `pipeline.mode_selection`, consumes `CBM_FORCE_PIPELINE_MODE`. |
| `src/pipeline/pipeline_internal.h` | Mode-selection contract shared with tests | ✓ VERIFIED | Exports `cbm_pipeline_mode`, selection struct, and helper declaration used by tests/runtime. |
| `tests/test_parallel.c` | Native forced-mode regressions | ✓ VERIFIED | Contains forced sequential/parallel tests and suite registration. |
| `scripts/gdscript-proof.sh` | Dual-mode proof execution and semantic comparison summaries | ✓ VERIFIED | Expands manifest runs to sequential+parallel, writes run index, semantic pairs, and semantic parity artifacts from wrappers. |
| `docs/superpowers/proofs/gdscript-real-project-validation.md` | Updated artifact layout and review flow | ✓ VERIFIED | Documents approval workflow, artifact layout, semantic parity review order, and four-repo/two-mode bar. |
| `scripts/test_gdscript_proof_semantic_parity.py` | Proof-harness parity regression | ✓ VERIFIED | Runs harness, parses run root, asserts `semantic-parity.json`/`.md` and SEM-01..SEM-06 structure. |
| `Makefile.cbm` | Standard test-path wiring | ✓ VERIFIED | `test` target includes proof regressions. |
| `docs/superpowers/proofs/gdscript-good-tier-checklist.md` | Phase 03 review checklist | ✓ VERIFIED | Checklist requires all four repos, both modes, counts, samples, and incomplete-run fallback. |

### Key Link Verification

| From | To | Via | Status | Details |
| --- | --- | --- | --- | --- |
| `src/pipeline/pipeline.c` | `tests/test_parallel.c` | shared mode-selection helper | ✓ WIRED | `cbm_pipeline_select_mode` is declared in header and exercised by forced-mode tests. |
| `scripts/gdscript-proof.sh` | `src/pipeline/pipeline.c` | `CBM_FORCE_PIPELINE_MODE` | ✓ WIRED | Script injects env var before `index_repository`; pipeline reads it before selecting mode. |
| `scripts/gdscript-proof.sh` | `docs/superpowers/proofs/gdscript-good-tier-manifest.json` | full four-repo manifest iteration | ✓ WIRED | Script validates `minimum_repo_count`, manifest labels, and generates semantic reports for manifest labels. |
| `Makefile.cbm` | `scripts/test_gdscript_proof_semantic_parity.py` | test target | ✓ WIRED | Proof parity regression is part of default `test` target. |
| `docs/superpowers/proofs/gdscript-good-tier-checklist.md` | `docs/superpowers/proofs/gdscript-real-project-validation.md` | operator review instructions | ✓ WIRED | Checklist and runbook use the same sequential/parallel semantic parity review contract. |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
| --- | --- | --- | --- | --- |
| `scripts/gdscript-proof.sh` | `reports` / `pair["requirements"]` | Wrapper files under `<repo>/queries/*.json` plus `repo-meta.json` pair metadata | Yes — `load_wrapper`, `count_value`, `sample_values`, and `edge_values` read emitted query artifacts before writing `semantic-parity.json`/`.md` (`2297-2603`). | ✓ FLOWING |
| `scripts/test_gdscript_proof_semantic_parity.py` | `payload` / `requirements` | Real subprocess run of `scripts/gdscript-proof.sh` on a temp fixture repo | Yes — test reads generated `semantic-parity.json` from disk and asserts SEM-01..SEM-06 contents. | ✓ FLOWING |
| `scripts/test_gdscript_proof_manifest_contract.py` | `run_index` / `semantic_pairs` | Real subprocess run of `scripts/gdscript-proof.sh` on a temp fixture repo | Yes — test reads generated `run-index.json`, repo metadata, and parity files from disk. | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Manifest run emits dual-mode metadata and parity artifacts | `python3 -B scripts/test_gdscript_proof_manifest_contract.py` | `PASS` | ✓ PASS |
| Semantic parity artifact exposes SEM-01..SEM-06 evidence | `python3 -B scripts/test_gdscript_proof_semantic_parity.py` | `PASS` | ✓ PASS |
| Standard repo test path runs proof regressions | `rtk make -f Makefile.cbm test` | `STATUS=0` | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| `SEM-01` | `03-02`, `03-03` | Non-zero GDScript class and method extraction with reviewable samples | ✓ SATISFIED | `scripts/gdscript-proof.sh:2466-2480`; `scripts/test_gdscript_proof_semantic_parity.py:168-188`; runbook/checklist require counts + samples. |
| `SEM-02` | `03-02`, `03-03` | Same-script method calls resolve on a real proof target | ✓ SATISFIED | `scripts/gdscript-proof.sh:2482`; parity test requires SEM-02 count and representative edges. |
| `SEM-03` | `03-02`, `03-03` | Queryable `extends` inheritance relationships | ✓ SATISFIED | `scripts/gdscript-proof.sh:2483`; parity test requires SEM-03 count and representative edges. |
| `SEM-04` | `03-02`, `03-03` | Queryable `.gd` dependency relationships from `preload`/`load` | ✓ SATISFIED | `scripts/gdscript-proof.sh:2484`; parity test requires SEM-04 count and representative edges. |
| `SEM-05` | `03-02`, `03-03` | Signal declarations and conservative signal-call behavior | ✓ SATISFIED | `scripts/gdscript-proof.sh:2485`; parity test requires SEM-05 count and representative edges. |
| `SEM-06` | `03-01`, `03-02`, `03-03` | Core GDScript behaviors stay consistent across sequential and parallel indexing | ✓ SATISFIED | Native forced-mode helper + tests (`pipeline.c`, `test_parallel.c`), dual-mode harness pairing, `SEM-06` parity outcome, and default test-path regressions. |

All requirement IDs declared in Phase 03 plans are present in `REQUIREMENTS.md`, and `REQUIREMENTS.md` lists no extra Phase 3 requirement IDs outside `SEM-01`..`SEM-06`.

### Anti-Patterns Found

No blocker anti-patterns found in phase-modified files. Targeted scans found no TODO/FIXME/placeholder implementations tied to the Phase 03 artifacts.

### Gaps Summary

No implementation gaps found. The codebase contains deterministic mode forcing in the native pipeline, dual-mode manifest proof execution with additive semantic parity reporting, automated regressions that exercise the proof artifacts, and operator documentation/checklists aligned to the Phase 03 approval bar.

---

_Verified: 2026-04-12T18:04:23Z_
_Verifier: OpenCode (gsd-verifier)_
