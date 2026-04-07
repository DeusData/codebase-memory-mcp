# Design Spec: Raise GDScript to a Python-Bar "Good" Support Level

Date: 2026-04-06  
Related issue: https://github.com/DeusData/codebase-memory-mcp/issues/186

## 1) Objective

Raise GDScript support from "credible and useful" to an **honest Python-like "Good tier usefulness" bar** for Godot workflows in this repository.

This does **not** mean feature-for-feature Python parity, and it does **not** target C++-style deep type resolution. The goal is a support level strong enough that GDScript can be evaluated for a scored support tier using measured evidence rather than optimistic claims.

## 2) Current Baseline

GDScript already has meaningful support:

- extraction coverage for definitions, signals, imports, inheritance-related metadata, and core Godot patterns
- persisted graph behavior validated through integration tests
- sequential/parallel parity coverage
- a real-project proof workflow covering multiple Godot repositories

But GDScript is still below the repo's stronger language tiers:

- it is listed in the README's unscored "Plus" bucket rather than the benchmarked support tiers
- its real-project proof currently proves practical usefulness, but the acceptance bar is still closer to **non-zero evidence** than to **correctness against expected results**
- some semantics remain intentionally conservative or heuristic-driven, especially around dynamic receiver resolution and built-in Godot inheritance

## 3) Target Bar

The comparison bar is **Python-level usefulness**, not Python's exact feature set.

That means GDScript should eventually demonstrate:

- strong practical extraction and graph usefulness on real projects
- real-project validation based on **expected outputs**, not just presence/absence of evidence
- tight regression protection across extraction, persisted graph behavior, and seq/parallel parity
- documentation and support-tier claims that match measured evidence

This effort should **not** chase:

- C++-style hybrid/LSP resolution depth
- complete resolution of every dynamic Godot runtime case
- Python-specific features that do not matter for Godot indexing quality

## 4) Selected Approach

Chosen approach: **evidence-first parity climb**.

Why this approach:

1. It creates a measurable bar before adding more heuristics.
2. It reveals which misses matter in real Godot repositories.
3. It prevents product claims from outrunning implementation quality.
4. It keeps the work focused on Godot usefulness rather than generic language completeness.

## 5) Scope and Constraints

### In scope

- strengthening the real-project proof from coarse evidence to expected-result validation
- identifying the most important real-world GDScript misses from proof data
- adding targeted resolver improvements for those misses
- adding regression tests at fixture, integration, and parity levels
- defining an explicit GDScript "Good tier" acceptance checklist
- updating README / benchmark / support-tier positioning only if evidence supports it

### Out of scope

- full Python feature parity
- full Godot runtime simulation
- broad new graph schema work unless clearly required by validated misses
- deep type-system work comparable to C++ support
- solving every possible dynamic dispatch case

### Hard constraints

- prefer conservative under-resolution over false positives
- do not strengthen README claims until measurement supports them
- use real Godot repos as the primary signal for prioritization
- keep seq/parallel behavior aligned for every new resolver improvement

## 6) Architecture of the Effort

The work is organized into three phases.

### Phase 1: Accuracy Bar

Turn the current proof workflow into a **correctness-oriented release gate**.

Instead of only checking for non-zero evidence, the proof should validate expected results for a small curated set of real Godot repositories.

Required outputs:

- expected `.gd` file/class/method counts per repo
- expected representative `CALLS` edges for signals and same-script calls
- expected representative `IMPORTS` edges for `preload`/`load` dependencies
- expected representative `INHERITS` edges for repo-local script inheritance
- proof output that separates:
  - exact matches
  - explicitly declared non-gating observations
  - regressions / failures

For planning purposes, **Phase 1 should not introduce an ambiguous "partial pass" state**. Each expected assertion must be classified as one of:

- **gating** — must match exactly, otherwise the proof run fails
- **informational** — recorded for visibility, but does not affect pass/fail yet

This keeps the proof manifest and release-gate behavior unambiguous.

Run-level outcome model:

- **pass** — all gating assertions matched on all required repos
- **fail** — one or more gating assertions failed
- **incomplete** — one or more required repos could not be compared against their pinned revision or required proof inputs were missing

For release-gate purposes, **incomplete is treated the same as fail**. It may be surfaced with different messaging, but it is not a passing result.

The result of Phase 1 is a trustworthy signal about whether GDScript support is improving or regressing on real code.

### Phase 2: Semantic Gap Closure

Use Phase 1 findings to decide what to improve.

Resolver work should be driven by observed misses, not by abstract completeness. Candidate gap areas include:

- under-resolved signal targets in real projects
- cross-script/cross-file resolution misses that matter to graph usefulness
- inheritance cases that should persist as graph edges but currently do not
- built-in Godot base class representation if it materially affects support-tier credibility

Every resolver improvement must add or extend three layers of protection:

1. extraction/fixture regression coverage
2. integration coverage for persisted graph behavior
3. sequential/parallel parity coverage

### Phase 3: Support-Tier Promotion

Only after Phases 1-2 are complete should the repo consider moving GDScript into a scored support tier.

This phase defines and enforces the promotion gate:

- GDScript has an explicit acceptance checklist for a Python-like "Good" level
- proof results are correctness-oriented and stable
- important known limitations are documented
- README / benchmark positioning can be defended honestly

## 7) Deliverables by Phase

### Phase 1 deliverables

- curated real Godot repo set with stable paths, metadata, and pinned revisions
- expected-result manifest(s) for representative counts and edges
- proof runner support for asserting expected outcomes
- proof summaries that clearly identify exact match vs regression

The real-repo set must be reproducible. The design requirement is:

- each repo in the proof set is pinned by commit SHA
- the expected manifest records repo identity, commit SHA, and Godot version metadata
- the proof runner verifies that the local repo checkout matches the pinned commit before treating results as comparable
- if the local checkout does not match the pinned revision, the run is reported as non-comparable/incomplete rather than silently accepted

Minimum Phase 1 proof-set shape:

- at least **3** real Godot repositories
- all required repos target **Godot 4.x** for the main support claim
- the set must collectively exercise:
  - signal-call behavior
  - `.gd` dependency/import behavior
  - `.gd` inheritance behavior
- at least one repo must be small/simple enough to keep expected-result maintenance practical
- at least one repo must contain enough cross-file behavior to exercise real resolver value beyond toy fixtures

### Phase 2 deliverables

- prioritized miss list derived from Phase 1 proof output
- narrow resolver improvements for the highest-value misses
- new targeted tests for each fix at fixture/integration/parity levels

### Phase 3 deliverables

- written "Good tier" acceptance checklist for GDScript
- README / benchmark / support-table updates, but only if the checklist passes
- explicit documentation of remaining limitations and non-goals

## 8) File and Module Boundaries

This design expects future implementation work to concentrate in a few clear areas.

### A. Real-project validation workflow

- `scripts/gdscript-proof.sh`
- `docs/superpowers/proofs/gdscript-real-project-validation.md`
- generated proof artifacts under `.artifacts/gdscript-proof/` (local only)

Responsibility:

- define the repo set, run indexing and query checks, compare against expected outputs, and surface coverage/regression status clearly

### B. GDScript extraction / resolver behavior

- `internal/cbm/helpers.c`
- `internal/cbm/extract_defs.c`
- `internal/cbm/extract_imports.c`
- `internal/cbm/extract_unified.c`
- `src/pipeline/registry.c`
- `src/pipeline/pass_calls.c`
- `src/pipeline/pass_semantic.c`
- `src/pipeline/pass_parallel.c`

Responsibility:

- implement only those semantic improvements justified by Phase 1 misses

### C. Regression test layers

- `tests/test_extraction.c`
- `tests/test_pipeline.c`
- `tests/test_integration.c`
- `tests/test_parallel.c`

Responsibility:

- cover newly fixed behaviors at the smallest reliable scope while ensuring persisted graph and seq/par parity stay correct

### D. Product positioning

- `README.md`
- `docs/BENCHMARK.md`

Responsibility:

- describe GDScript honestly once the evidence supports stronger claims

## 9) Testing Strategy

The testing model for this effort has three layers.

### Layer 1: Fixture tests

Use small fixtures to verify parser/extractor behavior for newly discovered miss categories.

Guidelines:

- add the smallest fixture that reproduces the real-project miss
- avoid speculative test growth unrelated to validated misses

### Layer 2: Integration + persisted graph tests

Verify that extracted semantics actually survive into stored graph behavior.

Guidelines:

- assert the persisted node/edge behavior that users query
- cover positive and negative cases where false positives are a risk

### Layer 3: Sequential / parallel parity

Every new GDScript resolver improvement must prove equivalent behavior in both execution modes.

Guidelines:

- parity checks should compare meaningful graph behavior, not only broad counts
- parity fixtures should stay aligned with integration fixtures where possible

### Layer 4: Real-project proof

Use a small stable repo set as the release gate.

Guidelines:

- assert expected results for representative counts and edges
- fail clearly when a previously validated behavior regresses
- distinguish correctness failures from incomplete evidence collection

Manifest and artifact contract:

- expected manifests are committed repository files
- generated proof outputs remain local-only under `.artifacts/gdscript-proof/`
- manifests identify which assertions are gating vs informational
- gating assertions require exact match on the pinned repo revision
- if any required repo is missing, on the wrong pinned revision, or otherwise non-comparable, the run result is **incomplete**, which fails the release gate

## 10) Success Criteria

This effort is successful when all of the following are true:

1. Real-project validation checks **correctness**, not just non-zero evidence.
2. The most important observed under-resolution cases in core Godot workflows are addressed or explicitly documented.
3. GDScript regression coverage is stronger at fixture, integration, and parity layers.
4. Product positioning can truthfully describe GDScript as approaching or reaching a Python-like "Good" usefulness bar.

## 11) Promotion Gate for a Scored Support Tier

GDScript should not move into a scored support tier until:

- proof manifests exist for a stable multi-repo Godot set
- proof runs are consistently green against expected results
- key signal/imports/inherits behaviors are validated by tests and real-project checks
- major known limitations are documented
- maintainers can explain why the support level is comparable in usefulness to Python, even if the implementation strategy differs
- the release-gate proof status is **pass**, not fail or incomplete

The eventual "Good tier" checklist should minimally cover:

- reproducible pinned real-repo proof set
- gating expected-result checks for counts and representative edges
- regression coverage at extraction, integration, and seq/parallel parity levels
- documented limitations for intentionally conservative/unresolved cases

## 12) Risks and Mitigations

### Risk: heuristic sprawl

Adding many ad hoc rules can increase false positives and maintenance burden.

Mitigation:

- only add heuristics that close validated real-project misses
- require regression tests and parity tests for each one

### Risk: product claims outrun reality

README or benchmark language may become stronger than the evidence supports.

Mitigation:

- keep support-tier promotion as a gated final phase

### Risk: plan grows into "solve all Godot semantics"

The effort could become unbounded.

Mitigation:

- separate correctness blockers from nice-to-have coverage
- keep non-goals explicit

## 13) Non-Goals

This design explicitly does **not** require:

- feature-for-feature Python parity
- complete dynamic runtime resolution
- C++-style semantic depth
- automatic support-tier promotion just because tests exist

## 14) Planning Readiness

This spec is intentionally shaped for one implementation-planning cycle with phased execution:

1. strengthen proof quality first
2. use proof findings to drive resolver work
3. promote support tier only if evidence supports it

That sequence is the core design decision.
