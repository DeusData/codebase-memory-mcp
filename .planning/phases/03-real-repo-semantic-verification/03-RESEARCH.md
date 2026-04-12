# Phase 03: Real-Repo Semantic Verification - Research

**Researched:** 2026-04-12
**Domain:** Real-repo GDScript semantic verification through the existing proof harness
**Confidence:** MEDIUM

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

### MCP usefulness bar
- **D-01:** Phase 03 will treat the existing fixed proof query suite as the approval bar for parser-to-MCP usefulness.
- **D-02:** Phase 03 does not add separate direct MCP graph discovery, tracing, or ad hoc query-walkthrough checks as required approval evidence.
- **D-03:** Usefulness for this phase means the captured proof queries expose the required GDScript semantic behaviors through the existing proof harness output, not that every interactive MCP workflow is re-validated separately.

### Repo coverage
- **D-04:** Phase 03 evidence must run across all four pinned manifest repos, not just category-owning repos or a narrower anchor set.
- **D-05:** The fixed manifest corpus remains the full approval surface for this phase, so sequential and parallel evidence should be interpretable across the same four approved targets.

### Evidence granularity
- **D-06:** The acceptance bar for Phase 03 semantic behaviors is non-zero counts plus representative samples.
- **D-07:** Class and method extraction must remain reviewable through representative sample outputs, not counts alone.
- **D-08:** Explicit edge-assertion artifacts may still be used when already available, but Phase 03 does not require edge-by-edge asserted checks as the universal acceptance bar for every semantic behavior.

### Sequential versus parallel parity
- **D-09:** Sequential and parallel indexing paths must produce the same core semantic outcomes for the Phase 03 behaviors, even if non-semantic artifact details differ.
- **D-10:** Phase 03 parity does not require byte-for-byte artifact equality between sequential and parallel runs.
- **D-11:** Matching top-level pass/fail alone is not sufficient; planning must verify that the required semantic behaviors remain meaningfully consistent across both indexing modes.

### OpenCode's Discretion
- Exact shape of any comparison summaries or reporting that make sequential-versus-parallel semantic consistency easy to review.
- Exact sample-selection thresholds and presentation details, as long as they preserve the counts-plus-samples acceptance bar.
- Exact implementation split between harness logic, regression tests, and documentation updates needed to express these decisions.

### Deferred Ideas (OUT OF SCOPE)
- Broader direct MCP workflow spot-checks for graph discovery, tracing, or exploratory query ergonomics remain out of scope for Phase 03 approval unless later phases explicitly add them.
- Exact artifact-level equality or byte-for-byte diff requirements between sequential and parallel runs are out of scope for this phase.
- Any expansion or replacement of the approved four-repo manifest corpus remains outside Phase 03 and stays governed by earlier locked decisions.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|---|---|---|
| SEM-01 | Maintainer can confirm non-zero GDScript class and method extraction from a real proof target with reviewable samples | Use existing `gd-classes`, `gd-methods`, `gd-class-sample`, and `gd-method-sample` wrappers as the approval surface across all four manifest repos. [VERIFIED: scripts/gdscript-proof.sh] |
| SEM-02 | Maintainer can confirm same-script method calls resolve on a real proof target | Reuse `gd-same-script-calls` plus representative edge samples; preserve current same-script filtering that keeps only same-file non-signal targets. [VERIFIED: scripts/gdscript-proof.sh] |
| SEM-03 | Maintainer can confirm queryable `extends` inheritance relationships on a real proof target | Reuse `gd-inherits` count plus `gd-inherits-edges` representative detail artifacts. [VERIFIED: scripts/gdscript-proof.sh] |
| SEM-04 | Maintainer can confirm queryable `.gd` dependency relationships from `preload` or `load` targets on a real proof target | Reuse `gd-imports` count plus `gd-import-edges` representative detail artifacts. [VERIFIED: scripts/gdscript-proof.sh] |
| SEM-05 | Maintainer can confirm signal declarations and conservative signal-call behavior on a real proof target | Reuse `signal-calls` count plus `signal-call-edges`; keep conservative behavior rather than broadening schema/workflow scope. [VERIFIED: scripts/gdscript-proof.sh] |
| SEM-06 | Maintainer can confirm the core GDScript behaviors above remain consistent across sequential and parallel indexing paths | Add an explicit two-mode proof path and compare semantic outputs, not just aggregate pass/fail. [VERIFIED: src/pipeline/pipeline.c][ASSUMED] |
</phase_requirements>

## Summary

Phase 03 should extend the existing manifest-mode proof lane instead of introducing a second acceptance workflow. The current harness already captures the exact fixed query suite needed for class, method, signal, inherits, imports, and same-script evidence, and the manifest already pins the four approved repos plus expected representative assertions. [VERIFIED: scripts/gdscript-proof.sh][CITED: docs/superpowers/proofs/gdscript-real-project-validation.md][CITED: docs/superpowers/proofs/gdscript-good-tier-manifest.json]

The main planning problem is not semantic query design; it is deterministic two-mode execution and reviewable parity reporting. Today, the indexing pipeline auto-selects parallel mode only when `cbm_default_worker_count(true) > 1` and discovered file count exceeds 50, while the proof harness exposes no sequential/parallel option or comparison layer. [VERIFIED: src/pipeline/pipeline.c][VERIFIED: src/foundation/system_info.c][VERIFIED: scripts/gdscript-proof.sh]

**Primary recommendation:** Keep `scripts/gdscript-proof.sh` as the canonical entrypoint, run each of the four manifest repos through both indexing modes, preserve the existing wrapper-first artifact contract, and add a semantic comparison summary that checks counts plus representative samples for the six Phase 03 behaviors. [VERIFIED: scripts/gdscript-proof.sh][CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md][ASSUMED]

## Project Constraints (from AGENTS.md)

- Read `codemap.md` before working; it is the required repository architecture reference. [CITED: AGENTS.md]
- For deep work on a specific folder, also read that folder's `codemap.md`. [CITED: AGENTS.md]
- Prefer codebase-memory graph tools over manual code search for code discovery. [CITED: /Users/shaunmcmanus/.config/opencode/AGENTS.md]

## Standard Stack

### Core
| Component | Version | Purpose | Why Standard |
|---|---|---|---|
| `scripts/gdscript-proof.sh` | repo-local current | Canonical proof entrypoint, artifact writer, manifest evaluator, and fixed query runner. [VERIFIED: scripts/gdscript-proof.sh] | Phase 2 already locked this script as the approval-bearing workflow, so Phase 03 should extend it rather than wrap or replace it. [CITED: .planning/phases/02-isolated-proof-harness/02-CONTEXT.md] |
| `build/c/codebase-memory-mcp` | `dev` | Local binary under test for indexing and MCP queries. [VERIFIED: build/c/codebase-memory-mcp --version] | The runbook requires the locally built binary, keeping proof evidence tied to the worktree under test. [CITED: docs/superpowers/proofs/gdscript-real-project-validation.md] |
| `docs/superpowers/proofs/gdscript-good-tier-manifest.json` | version `1` | Canonical four-repo approval corpus and expected-result manifest. [CITED: docs/superpowers/proofs/gdscript-good-tier-manifest.json] | Locked Phase 03 coverage must stay on all four pinned targets. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md] |

### Supporting
| Component | Version | Purpose | When to Use |
|---|---|---|---|
| `run-index.json` + `queries/*.json` | repo-local current | Machine-readable run inventory plus raw wrapper evidence. [CITED: docs/superpowers/proofs/gdscript-real-project-validation.md] | Use for mode-aware review and downstream comparison without replacing raw query wrappers. [CITED: .planning/phases/02-isolated-proof-harness/02-CONTEXT.md] |
| `scripts/test_gdscript_proof_manifest_contract.py` | repo-local current | Existing subprocess-based proof-harness contract regression style. [VERIFIED: scripts/test_gdscript_proof_manifest_contract.py] | Use for new proof-harness behavior such as mode metadata, parity summaries, and artifact presence. [VERIFIED: scripts/test_gdscript_proof_manifest_contract.py][ASSUMED] |
| `tests/test_parallel.c` | repo-local current | Existing sequential/parallel semantic parity layer inside native tests. [VERIFIED: tests/test_parallel.c] | Use when Phase 03 uncovers parity gaps that require product-code fixes or new low-level parity assertions. [VERIFIED: tests/test_parallel.c] |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|---|---|---|
| Extending `scripts/gdscript-proof.sh` | New standalone phase-specific verifier | Conflicts with the locked “existing harness is canonical” decision and would duplicate artifact contracts. [CITED: .planning/phases/02-isolated-proof-harness/02-CONTEXT.md] |
| Comparing aggregate pass/fail only | Byte-for-byte artifact diff | Too weak for SEM-06 on one side, too strict for Phase 03 scope on the other. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md] |
| Category-owner repo subset | Full four-repo manifest run | The subset violates the locked approval surface. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md] |

**Installation:** No new third-party package is required by the phase if the implementation stays inside the existing shell/Python/C stack. [VERIFIED: scripts/gdscript-proof.sh][VERIFIED: Makefile.cbm]

## Architecture Patterns

### Recommended Project Structure
```text
scripts/
├── gdscript-proof.sh                  # canonical proof runner and artifact writer
├── test_gdscript_proof_*.py          # harness-level proof contract regressions

docs/superpowers/proofs/
├── gdscript-real-project-validation.md # operator runbook and artifact contract
├── gdscript-good-tier-manifest.json    # pinned corpus and expected assertions
└── gdscript-good-tier-checklist.md     # human acceptance checklist

tests/
└── test_parallel.c                    # native seq/par semantic parity layer
```

### Pattern 1: Wrapper-first proof evidence
**What:** Keep `queries/*.json` as the raw evidence source and add any parity/reporting data as additive summaries or indexes, not replacements. [CITED: docs/superpowers/proofs/gdscript-real-project-validation.md]

**When to use:** For all SEM-01 through SEM-06 evidence, especially if planners want new comparison summaries. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]

**Example:**
```text
<run>/<repo>/queries/gd-methods.json
<run>/<repo>/queries/gd-method-sample.json
<run>/<repo>/queries/gd-import-edges.json
<run>/<repo>/summary.md
<run>/run-index.json
```
Source: `docs/superpowers/proofs/gdscript-real-project-validation.md` [CITED: docs/superpowers/proofs/gdscript-real-project-validation.md]

### Pattern 2: Manifest-driven semantic approval
**What:** Evaluate proof outcomes from the committed manifest’s fixed repo set and assertion list rather than from ad hoc operator choices. [CITED: docs/superpowers/proofs/gdscript-good-tier-manifest.json][CITED: .planning/phases/01-proof-contract-and-corpus/01-CONTEXT.md]

**When to use:** Always for approval-bearing Phase 03 runs. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]

### Pattern 3: Two-layer parity strategy
**What:** Put real-repo semantic parity in the proof harness and keep low-level edge parity in `tests/test_parallel.c`. [VERIFIED: tests/test_parallel.c][CITED: docs/superpowers/specs/2026-04-06-gdscript-python-bar-design.md]

**When to use:** Use the harness for release-gate evidence and native tests for root-cause regression protection. [CITED: docs/superpowers/specs/2026-04-06-gdscript-python-bar-design.md]

### Anti-Patterns to Avoid
- **A second approval workflow:** Do not add separate interactive MCP walkthrough checks or a new verifier command for approval. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]
- **Artifact identity as parity bar:** Do not require byte-for-byte equality between sequential and parallel outputs. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]
- **Repo narrowing:** Do not shrink the approval lane to only category-owning repos. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---|---|---|---|
| Proof execution | New wrapper CLI or one-off scripts | `scripts/gdscript-proof.sh` | The harness already owns build, indexing, query capture, metadata, and artifact layout. [VERIFIED: scripts/gdscript-proof.sh] |
| Raw evidence schema | Custom parity-only storage format | Existing wrapper JSON files plus additive run/repo summaries | Phase 2 explicitly locked wrapper JSON as canonical raw evidence. [CITED: .planning/phases/02-isolated-proof-harness/02-CONTEXT.md] |
| Real-repo selection | Ad hoc repo lists | Committed four-repo manifest | The approved coverage surface is manifest-defined. [CITED: docs/superpowers/proofs/gdscript-good-tier-manifest.json] |
| Seq/par semantic debugging | Only top-level proof summaries | `tests/test_parallel.c` for low-level parity checks | Native parity tests already cover meaningful edge parity and are the right place for product-code regressions. [VERIFIED: tests/test_parallel.c] |

**Key insight:** Phase 03 is mostly a proof-orchestration and evidence-review problem, not a new semantic-modeling problem. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]

## Common Pitfalls

### Pitfall 1: No deterministic way to hit both indexing modes
**What goes wrong:** The proof may silently exercise only whichever mode the pipeline auto-selects. [VERIFIED: src/pipeline/pipeline.c]
**Why it happens:** The pipeline chooses parallel only when worker count is greater than one and discovered file count is above 50, and the proof harness currently exposes no explicit mode switch. [VERIFIED: src/pipeline/pipeline.c][VERIFIED: src/foundation/system_info.c][VERIFIED: scripts/gdscript-proof.sh]
**How to avoid:** Plan an explicit mode-control mechanism or an equally explicit observable mode contract before designing parity tasks. [ASSUMED]
**Warning signs:** Proof artifacts have no mode label and no second run per repo. [VERIFIED: scripts/gdscript-proof.sh]

### Pitfall 2: Proving only pass/fail parity
**What goes wrong:** Sequential and parallel runs can both end `pass` while still drifting on semantic counts or samples. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]
**Why it happens:** Current aggregate summaries focus on outcomes and assertion totals, not cross-mode semantic comparison. [VERIFIED: scripts/gdscript-proof.sh]
**How to avoid:** Compare per-behavior counts and representative samples for classes, methods, signals, inherits, imports, and same-script calls. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md][ASSUMED]
**Warning signs:** A plan talks about “same final verdict” without naming semantic fields to compare. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]

### Pitfall 3: Breaking the Phase 2 artifact contract
**What goes wrong:** New parity reporting replaces or hides the raw `queries/*.json` evidence. [CITED: .planning/phases/02-isolated-proof-harness/02-CONTEXT.md]
**Why it happens:** Comparison summaries are tempting to treat as the new source of truth. [ASSUMED]
**How to avoid:** Keep wrappers canonical and make all new parity output additive. [CITED: docs/superpowers/proofs/gdscript-real-project-validation.md]
**Warning signs:** A proposed artifact tree omits existing wrapper files. [CITED: docs/superpowers/proofs/gdscript-real-project-validation.md]

### Pitfall 4: Using only counts for reviewability
**What goes wrong:** SEM-01 passes numerically but reviewers cannot tell whether extracted classes/methods are representative. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]
**Why it happens:** Count queries already exist and are easy to summarize. [VERIFIED: scripts/gdscript-proof.sh]
**How to avoid:** Preserve `gd-class-sample` and `gd-method-sample` as mandatory review evidence. [VERIFIED: scripts/gdscript-proof.sh][CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]
**Warning signs:** Plans mention counts for class/method extraction but omit sample queries. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]

### Pitfall 5: Adding proof regressions without wiring them into the standard test path
**What goes wrong:** New Phase 03 proof tests exist but are not exercised by the routine `make -f Makefile.cbm test` path. [VERIFIED: Makefile.cbm]
**Why it happens:** The current `test` target runs the native test runner plus `scripts/test_gdscript_proof_same_script_calls.py`, but not the other proof-harness Python scripts. [VERIFIED: Makefile.cbm]
**How to avoid:** Decide during planning whether new proof-harness regressions should be folded into `make test`, a narrower dedicated target, or both. [ASSUMED]
**Warning signs:** A new proof script lands under `scripts/` with no corresponding Makefile change or documented invocation. [VERIFIED: Makefile.cbm][ASSUMED]

## Code Examples

Verified patterns from repository sources:

### Fixed proof query suite
```bash
GDSCRIPT_QUERY_NAMES=(
  "gd-files"
  "gd-classes"
  "gd-methods"
  "gd-class-sample"
  "gd-method-sample"
  "signal-calls"
  "gd-inherits"
  "gd-imports"
  "signal-call-edges"
  "gd-inherits-edges"
  "gd-import-edges"
  "gd-same-script-calls"
)
```
Source: `scripts/gdscript-proof.sh` [VERIFIED: scripts/gdscript-proof.sh]

### Sequential vs parallel pipeline split
```c
int worker_count = cbm_default_worker_count(true);
#define MIN_FILES_FOR_PARALLEL 50
rc = (worker_count > 1 && file_count > MIN_FILES_FOR_PARALLEL)
         ? run_parallel_pipeline(p, &ctx, files, file_count, worker_count, &t)
         : run_sequential_pipeline(p, &ctx, files, file_count, &t);
```
Source: `src/pipeline/pipeline.c` [VERIFIED: src/pipeline/pipeline.c]

### Same-script edge normalization in proof evaluation
```python
elif query_name == "gd-same-script-calls":
    if src_file != dst_file:
        continue
    if _dst_qn and ".signal." in str(_dst_qn):
        continue
    values.append(f"{src_file}:{src_name}->{dst_name}")
```
Source: `scripts/gdscript-proof.sh` [VERIFIED: scripts/gdscript-proof.sh]

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|---|---|---|---|
| Coarse non-zero coverage across categories | Manifest-based expected assertions with gating vs informational outcomes | Present in current harness/runbook as of 2026-04-12 research. [CITED: docs/superpowers/proofs/gdscript-real-project-validation.md] | Phase 03 should build on expected-result proofing, not revert to coarse-only checks. [CITED: docs/superpowers/specs/2026-04-06-gdscript-python-bar-design.md] |
| One-mode real proof lane | Native `tests/test_parallel.c` parity plus harness real-proof lane | Present in current repo state. [VERIFIED: tests/test_parallel.c][VERIFIED: scripts/gdscript-proof.sh] | Phase 03 should connect these layers instead of inventing a third parity mechanism. [ASSUMED] |

**Deprecated/outdated:** “Any three qualifying repos are enough” is outdated for Phase 03 planning because the context locks coverage to all four pinned manifest repos. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|---|---|---|
| A1 | The cleanest implementation is to add an explicit mode-control mechanism rather than infer modes indirectly. | Summary / Pitfalls | Medium — planning may target the wrong integration surface. |
| A2 | Harness-level parity summaries should compare counts plus representative samples for each semantic behavior. | Summary / Pitfalls | Low — presentation could change without changing core phase scope. |
| A3 | `scripts/test_gdscript_proof_manifest_contract.py` is the best regression file to extend for new proof-harness mode metadata and parity artifacts. | Standard Stack | Low — another existing proof test could absorb the coverage instead. |

## Open Questions (RESOLVED)

1. **How should the repo force sequential and parallel indexing for the same proof target?**
   - Resolution: Add an explicit environment-driven override in the native pipeline using `CBM_FORCE_PIPELINE_MODE=auto|sequential|parallel`, then have `scripts/gdscript-proof.sh` drive that override for approval-bearing proof runs. This keeps `scripts/gdscript-proof.sh` as the only approval workflow while avoiding a second CLI surface. [VERIFIED: src/pipeline/pipeline.c][CITED: .planning/phases/02-isolated-proof-harness/02-CONTEXT.md][CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]
   - Why this is the planning answer: it resolves SEM-06 deterministically at the product layer the harness already owns, preserves the existing auto-selection rule for normal runs, and avoids brittle repo-shaping tricks. [VERIFIED: src/pipeline/pipeline.c][ASSUMED]

2. **Where should per-mode artifacts live?**
   - Resolution: Keep the existing run root and wrapper-first layout, but write each repo/mode result to its own mode-aware repo artifact directory under the same run, with run-level pairing metadata in `run-index.json`. The additive keys should include `requested_mode`, `actual_mode`, `comparison_label`, and `semantic_pairs` so sequential and parallel outputs for one manifest target are machine-addressable without replacing `queries/*.json`. [CITED: docs/superpowers/proofs/gdscript-real-project-validation.md][CITED: .planning/phases/02-isolated-proof-harness/02-CONTEXT.md]
   - Why this is the planning answer: it preserves the Phase 2 artifact contract, keeps wrapper paths readable for operators, and gives Phase 03 a clean additive place to hang `semantic-parity.json` and `semantic-parity.md`. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md][ASSUMED]

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|---|---|---|---|---|
| git | Manifest pin checks and fixture repo setup | ✓ | 2.50.1 | — |
| python3 | Proof harness helpers and proof regression scripts | ✓ | 3.14.4 | — |
| make | Building `cbm` and full native test suite | ✓ | GNU Make 3.81 | — |
| local `build/c/codebase-memory-mcp` | Real proof indexing/query execution | ✓ | `dev` | Rebuild with `make -f Makefile.cbm cbm` |
| local proof repo checkouts | Four approved manifest targets | ✓ | pinned commits present | — |

**Missing dependencies with no fallback:** None. [VERIFIED: git --version][VERIFIED: python3 --version][VERIFIED: make --version][VERIFIED: build/c/codebase-memory-mcp --version][VERIFIED: local repo path checks]

**Missing dependencies with fallback:** None. [VERIFIED: local environment audit]

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---|---|---|
| V2 Authentication | no | Local proof workflow; no auth surface added by this phase. [VERIFIED: scripts/gdscript-proof.sh] |
| V3 Session Management | no | Local CLI/harness workflow; no session state. [VERIFIED: scripts/gdscript-proof.sh] |
| V4 Access Control | no | Repo-local proof artifacts and local checkouts only. [CITED: docs/superpowers/proofs/gdscript-real-project-validation.md] |
| V5 Input Validation | yes | Preserve path canonicalization, manifest schema validation, and explicit query-name validation in the harness. [VERIFIED: scripts/gdscript-proof.sh] |
| V6 Cryptography | no | Phase 03 does not introduce cryptographic behavior. [VERIFIED: repository scope documents] |

### Known Threat Patterns for this phase

| Pattern | STRIDE | Standard Mitigation |
|---|---|---|
| Malformed manifest data causing misleading proof results | Tampering | Keep strict manifest validation for labels, queries, classifications, and expected keys. [VERIFIED: scripts/gdscript-proof.sh] |
| Path confusion or accidental writes outside the artifact tree | Tampering | Continue canonicalizing repo/manifest paths and writing under `.artifacts/gdscript-proof/`. [VERIFIED: scripts/gdscript-proof.sh][CITED: docs/superpowers/proofs/gdscript-real-project-validation.md] |
| Misstating parity from incomplete evidence | Repudiation | Keep `incomplete` distinct from `pass`, and surface missing repos/non-comparable runs explicitly. [VERIFIED: scripts/gdscript-proof.sh][CITED: docs/superpowers/proofs/gdscript-real-project-validation.md] |

## Sources

### Primary (HIGH confidence)
- `scripts/gdscript-proof.sh` - fixed query suite, artifact contract, manifest validation, summary generation, and same-script edge normalization. [VERIFIED: scripts/gdscript-proof.sh]
- `src/pipeline/pipeline.c` - auto-selection between sequential and parallel indexing. [VERIFIED: src/pipeline/pipeline.c]
- `src/foundation/system_info.c` - worker-count source used by mode selection. [VERIFIED: src/foundation/system_info.c]
- `tests/test_parallel.c` - existing meaningful sequential/parallel semantic parity tests. [VERIFIED: tests/test_parallel.c]
- `docs/superpowers/proofs/gdscript-real-project-validation.md` - official runbook and artifact layout. [CITED: docs/superpowers/proofs/gdscript-real-project-validation.md]
- `docs/superpowers/proofs/gdscript-good-tier-manifest.json` - canonical four-repo corpus and assertion surface. [CITED: docs/superpowers/proofs/gdscript-good-tier-manifest.json]

### Secondary (MEDIUM confidence)
- `.planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md` - locked phase decisions and scope constraints. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md]
- `docs/superpowers/specs/2026-04-06-gdscript-python-bar-design.md` - parity and real-proof layering guidance. [CITED: docs/superpowers/specs/2026-04-06-gdscript-python-bar-design.md]
- `docs/superpowers/specs/2026-03-30-gdscript-support-design.md` - original GDScript parity and semantic mapping design. [CITED: docs/superpowers/specs/2026-03-30-gdscript-support-design.md]

### Tertiary (LOW confidence)
- None.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - the phase is centered on already-committed harness, manifest, runbook, and tests. [VERIFIED: repository sources]
- Architecture: MEDIUM - the extension points are clear, but deterministic mode forcing is not yet specified in the current code or docs. [VERIFIED: src/pipeline/pipeline.c][ASSUMED]
- Pitfalls: HIGH - the main failure modes are directly visible in locked context plus current harness/pipeline behavior. [CITED: .planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md][VERIFIED: scripts/gdscript-proof.sh]

**Research date:** 2026-04-12
**Valid until:** 2026-05-12
