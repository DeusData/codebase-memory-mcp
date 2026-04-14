# Roadmap: GDScript Support Verification

## Overview

This roadmap turns the existing brownfield GDScript implementation and proof assets into a trustworthy real-project verification path. It follows the research-recommended sequence: first lock the proof targets and acceptance contract, then harden isolated proof execution, then verify the core GDScript semantics on real Godot demo projects, and finally promote results through explicit verdicts and summary guardrails.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Proof Contract & Corpus** - Lock the pinned Godot demo targets and the target-identity rules the proof must use. (completed 2026-04-12)
- [x] **Phase 2: Isolated Proof Harness** - Make proof runs reproducible, isolated, and backed by a fixed raw MCP query evidence set. (completed 2026-04-12)
- [x] **Phase 3: Real-Repo Semantic Verification** - Prove the required GDScript extraction and resolution behaviors on real projects across both indexing paths. (completed 2026-04-12)
- [x] **Phase 4: Verdicts & Acceptance Summaries** - Turn raw proof evidence into explicit per-target outcomes and maintainable promotion summaries. (completed 2026-04-12)
- [x] **Phase 5: Web UI Launch Repair** - Restore `--ui=true` so the embedded web UI actually starts again through the supported persisted launch path. (completed 2026-04-13)
- [ ] **Phase 6: Parallel Native Suite Repair** - Resolve the deferred forced-parallel native-suite failure so the remaining Phase 03 parallel-mode debt is closed.
- [ ] **Phase 7: Nyquist Validation Backfill** - Add the missing per-phase Nyquist validation docs for Phases 01-04 so milestone validation coverage is complete.

## Phase Details

### Phase 1: Proof Contract & Corpus
**Goal**: Maintainers can run verification against a pinned, comparable set of real Godot 4.x proof targets with unambiguous repo identity and project subpath handling.
**Depends on**: Nothing (first phase)
**Requirements**: PROOF-01
**Success Criteria** (what must be TRUE):
  1. Maintainer can select approved real proof targets from `/Users/shaunmcmanus/Downloads/ShitsAndGiggles/godot-demo-projects/` without relying on ad hoc repo choices.
  2. Each proof target records enough identity metadata for reruns to target the same repo and project subpath again.
  3. Proof targets that count toward v1 acceptance are explicitly qualified as Godot 4.x instead of being assumed valid by default.
**Plans**: 2 plans

Plans:
- [x] 01-01-PLAN.md — Lock the written approved corpus contract in the manifest, runbook, and checklist.
- [x] 01-02-PLAN.md — Enforce canonical approval metadata in the proof harness and add contract regression coverage.

### Phase 2: Isolated Proof Harness
**Goal**: Maintainers can run a reproducible proof command that indexes real targets in isolated local state and captures a fixed MCP query suite as machine-readable evidence.
**Depends on**: Phase 1
**Requirements**: PROOF-02, EVID-01, EVID-02
**Success Criteria** (what must be TRUE):
  1. Maintainer can execute a single repo-owned proof workflow that keeps HOME, config, cache, and proof outputs under repo-owned local artifacts.
  2. After indexing a proof target, the workflow automatically runs the fixed MCP query suite needed for v1 validation.
  3. Maintainer can inspect machine-readable raw query artifacts for every proof target without needing to query the database manually.
  4. Re-running the same proof target on the same machine produces comparable artifact structure and evidence locations.
**Plans**: 2 plans

Plans:
- [x] 02-01-PLAN.md — Add a run-level machine-readable evidence index and preserve incomplete-path artifact status in the existing proof harness.
- [x] 02-02-PLAN.md — Document the additive evidence contract and add regressions for successful and incomplete proof runs.

### Phase 3: Real-Repo Semantic Verification
**Goal**: Maintainers can verify that real Godot demo projects expose the required GDScript definitions and relationships through the parser-to-MCP path in both sequential and parallel indexing modes.
**Depends on**: Phase 2
**Requirements**: SEM-01, SEM-02, SEM-03, SEM-04, SEM-05, SEM-06
**Success Criteria** (what must be TRUE):
  1. Maintainer can confirm non-zero GDScript class and method extraction on real proof targets and review representative samples.
  2. Maintainer can confirm same-script method calls resolve on real proof targets through captured MCP query results.
  3. Maintainer can confirm queryable `extends` inheritance and `.gd` `preload`/`load` dependency relationships on real proof targets.
  4. Maintainer can confirm signal declarations and conservative signal-call behavior on real proof targets.
  5. Maintainer can compare sequential and parallel indexing runs and see the core GDScript behaviors remain consistent across both paths.
**Plans**: 4 plans

Plans:
- [x] 03-01-PLAN.md — Add deterministic sequential/parallel mode selection in the native pipeline and cover it with native parity regressions.
- [x] 03-02-PLAN.md — Extend the canonical proof harness to capture dual-mode evidence and emit additive semantic parity summaries across all four manifest repos.
- [x] 03-03-PLAN.md — Add proof-harness regressions plus runbook/checklist updates for the Phase 03 semantic review bar. (completed 2026-04-12)
- [x] 03-04-PLAN.md — Close the forced-parallel UAT gap by fixing worker-lifetime cleanup and adding a regression for the incremental new-file path. (completed 2026-04-12)

### Phase 4: Verdicts & Acceptance Summaries
**Goal**: Maintainers can review each proof target through explicit pass/fail/incomplete outcomes and concise summaries that support honest promotion decisions.
**Depends on**: Phase 3
**Requirements**: PROOF-03, EVID-03
**Success Criteria** (what must be TRUE):
  1. Each proof target ends with an explicit `pass`, `fail`, or `incomplete` verdict instead of informal interpretation.
  2. Maintainer can inspect a per-repo summary explaining which checks passed, failed, or remained incomplete.
  3. Aggregate proof output distinguishes verified v1 support from incomplete or out-of-scope behavior so promotion language stays honest.
**Plans**: 2 plans

Plans:
- [x] 04-01-PLAN.md — Add regression-backed repo verdict sections and promotion-first aggregate summary wording in the canonical proof harness.
- [x] 04-02-PLAN.md — Update the runbook, checklist, and misses tracker for the new verdict and promotion contract.

### Phase 5: Web UI Launch Repair
**Goal**: Maintainers can launch the web UI with `--ui=true` and get the intended embedded UI startup path instead of a broken or ignored enable flag.
**Depends on**: Phase 4
**Requirements**: UI-01
**Success Criteria** (what must be TRUE):
  1. Running the repo's normal web UI entrypoint with `--ui=true` starts the UI again instead of silently leaving it down.
  2. The `--ui=true` and `--ui=false` persisted contract remains explicit in code and help text.
  3. Regression coverage or equivalent verification protects the `--ui=true` startup path from breaking again.
**Plans**: 1 plan

Plans:
- [x] 05-01-PLAN.md — Restore the persisted `--ui=true` launch path and add end-to-end regression coverage for enable, relaunch, and disable behavior. (completed 2026-04-13)

### Phase 6: Parallel Native Suite Repair
**Goal**: Maintainers can run the full native suite with `CBM_FORCE_PIPELINE_MODE=parallel` without the deferred `pipeline_fastapi_depends_edges` failure that remained after Phase 03.
**Depends on**: Phase 5
**Requirements**: None (audit tech debt closure)
**Gap Closure**: Closes the deferred Phase 03 native-suite debt recorded by the milestone audit.
**Success Criteria** (what must be TRUE):
  1. `tests/test_pipeline.c:4361` `pipeline_fastapi_depends_edges` passes when the full native suite is run with `CBM_FORCE_PIPELINE_MODE=parallel`.
  2. The fix preserves the targeted sequential and parallel proof behaviors already verified in Phase 03.
  3. Regression coverage or equivalent verification protects the forced-parallel full-suite path from silently regressing again.
**Plans**: 0 plans

### Phase 7: Nyquist Validation Backfill
**Goal**: Maintainers can audit the milestone with complete Nyquist validation coverage because Phases 01-04 each have an explicit `*-VALIDATION.md` contract.
**Depends on**: Phase 5
**Requirements**: None (audit tech debt closure)
**Gap Closure**: Closes the milestone audit's missing Nyquist validation-doc debt for Phases 01-04.
**Success Criteria** (what must be TRUE):
  1. Phases 01-04 each have a `*-VALIDATION.md` file in the expected location.
  2. Each new validation doc records an explicit per-phase sampling and verification contract compatible with Nyquist discovery.
  3. A follow-up milestone audit no longer reports partial Nyquist coverage for Phases 01-04.
**Plans**: 1 plan

Plans:
- [x] 07-01-PLAN.md — Backfill approved Nyquist validation docs for Phases 01-04 and refresh the milestone audit coverage.

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4 → 5 → 6 → 7

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Proof Contract & Corpus | 2/2 | Complete | 2026-04-12 |
| 2. Isolated Proof Harness | 2/2 | Complete | 2026-04-12 |
| 3. Real-Repo Semantic Verification | 4/4 | Complete | 2026-04-12 |
| 4. Verdicts & Acceptance Summaries | 2/2 | Complete | 2026-04-12 |
| 5. Web UI Launch Repair | 1/1 | Complete | 2026-04-13 |
| 6. Parallel Native Suite Repair | 0/0 | Planned | — |
| 7. Nyquist Validation Backfill | 0/0 | Planned | — |

### Phase 07.1: Add support for parsing, indexing, and linking '@export' in GDScript files (INSERTED)

**Goal:** Maintainers can index real GDScript files where `@export` variables are parsed correctly, persisted with accurate exported metadata, and linked consistently with existing GDScript anchor/class naming.
**Requirements**: TBD
**Depends on:** Phase 7
**Plans:** 1/1 plans complete

Plans:
- [x] 07.1-01-PLAN.md — Add regression-backed `@export` parsing/indexing/linking support in the GDScript extraction and pipeline paths.
