# GDScript Support Verification

## What This Is

This project validates that `codebase-memory-mcp` can index real Godot 4.x GDScript repositories and expose useful results through the MCP tool surface. It is aimed at maintainers of this brownfield codebase who need proof that the existing GDScript extraction and resolution work survives end-to-end against real projects, not just fixtures.

## Core Value

A real Godot demo project indexes cleanly and yields useful GDScript answers through the parser-to-MCP path.

## Requirements

### Validated

- ✓ The native C pipeline can discover source files, extract language-aware symbols, persist graph data, and serve it through MCP and the local UI — existing brownfield capability
- ✓ The codebase already contains GDScript-specific language registration, grammar integration, resolution logic, and fixture/proof infrastructure — existing brownfield capability
- ✓ Brownfield documentation already describes architecture, stack, and testing patterns for GDScript-oriented work — existing brownfield capability
- ✓ Maintainer can run validation against pinned real Godot 4.x demo targets with recorded repo identity and project subpath when needed — Validated in Phase 1: Proof Contract & Corpus
- ✓ Maintainer has a reproducible, isolated proof path with machine-readable raw MCP evidence and incomplete-run diagnostics — Validated in Phase 2: Isolated Proof Harness
- ✓ Maintainer can verify real-repo GDScript definitions, relationships, and sequential-versus-parallel parity through approval-bearing proof artifacts, regressions, and a forced-parallel lifetime fix — Validated in Phase 3: Real-Repo Semantic Verification
- ✓ Each proof target now emits an explicit `pass`, `fail`, or `incomplete` verdict with a per-repo summary and promotion-safe aggregate guidance — Validated in Phase 4: Verdicts & Acceptance Summaries
- ✓ Maintainers can launch the embedded web UI through the persisted `--ui=true` / `--ui=false` contract and keep that path protected by regression coverage — Validated in Phase 5: Web UI Launch Repair
- ✓ Forced-parallel full and incremental native-suite behavior now preserves the FastAPI Depends CALLS edge extraction contract — Validated in Phase 6: Parallel Native Suite Repair
- ✓ Nyquist validation documentation coverage is complete across milestone phases, so audit discovery no longer reports missing phase validation contracts — Validated in Phase 7: Nyquist Validation Backfill
- ✓ GDScript `@export` variables persist accurate exported metadata and anchor-linked qualified names through extraction and pipeline indexing — Validated in Phase 07.1: Add support for parsing, indexing, and linking `@export` in GDScript files

### Active

- [ ] Define v1.1 milestone requirements and roadmap scope with fresh milestone-scoped planning docs
- [ ] Close the Phase 07.1 traceability debt by mapping exported-variable support to an explicit requirement ID in the next requirement set
- [ ] Triage and resolve dependency hygiene findings surfaced during milestone audit (`npm audit` warning path)
- [ ] Expand proof-management capabilities (`PMGT-01`, `PMGT-02`, `PMGT-03`) after v1.1 scope is set

### Out of Scope

- Broad Godot engine feature support beyond GDScript indexing/query behavior — the current scope is parser-through-MCP verification, not a full Godot product surface
- Non-GDScript language expansion — this effort is validating GDScript support already under development
- Large-scale platform or UI redesigns unrelated to GDScript verification — avoid scope creep away from the proof goal

## Context

This repository is a layered native application with a Tree-sitter-backed extraction core, SQLite graph store, MCP server, and optional React UI. The current worktree is already focused on production-ready Godot 4.x GDScript indexing, with concrete implementation hooks in files such as `internal/cbm/grammar_gdscript.c`, `src/discover/userconfig.c`, and `src/pipeline/pass_parallel.c`.

Brownfield planning docs already exist under `.planning/codebase/`, and the repo contains GDScript-oriented proof assets such as `scripts/gdscript-proof.sh`, `scripts/test_gdscript_proof_same_script_calls.py`, and `docs/superpowers/proofs/gdscript-real-project-validation.md`. The user also provided a larger external Godot demo corpus at `/Users/shaunmcmanus/Downloads/ShitsAndGiggles/godot-demo-projects/` to use as the real-world validation target.

## Current State

- **Shipped version:** v1.0 (2026-04-14)
- **Milestone delivery:** 8 phases, 14 plans, 29 tasks completed
- **Acceptance status:** All 13 v1 requirements are complete in traceability, and all in-scope verification, integration, and end-to-end flows pass
- **Known post-ship debt:** Phase 07.1 requirement traceability mapping gap and dependency hygiene follow-up

## Next Milestone Goals

- Define v1.1 scope and success criteria with `/gsd-new-milestone`
- Create a fresh `.planning/REQUIREMENTS.md` for next-milestone requirements and traceability
- Resolve shipped-tech-debt follow-ups (07.1 traceability mapping and dependency hygiene)
- Prioritize proof management enhancements for multi-repo lanes and CI smoke validation

## Constraints

- **Tech stack**: Stay within the existing native C, Tree-sitter, SQLite, and MCP architecture — the goal is to verify and tighten current behavior, not re-platform it
- **Verification target**: Use real Godot demo projects from `/Users/shaunmcmanus/Downloads/ShitsAndGiggles/godot-demo-projects/` — success must be proven on external repositories, not only synthetic fixtures
- **Scope**: Prioritize parser-through-MCP behavior — fixes should serve end-to-end indexing and query usefulness rather than broad speculative feature work
- **Brownfield compatibility**: Preserve established repository patterns and existing multi-language indexing behavior while improving GDScript support

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Treat this as brownfield verification work | The repository already has GDScript-specific implementation and proof artifacts | Confirmed |
| Use real Godot demo repositories as the success benchmark | The user wants end-to-end proof on realistic projects, not a fixture-only pass | Confirmed |
| Define done as useful parser-through-MCP behavior on a real project | The first milestone is validation that real indexing and MCP querying work in practice | Confirmed |
| Manifest mode is the only approval-bearing v1 workflow | Approval evidence must come from the committed four-target corpus instead of operator choice | Confirmed in Phase 1 |
| Keep promotion language verdict-gated (`pass`/`fail`/`incomplete`) | Support claims must be bounded to reproducible evidence and explicit repo outcomes | Confirmed in Phase 4 |
| Preserve persisted UI launch toggles as first-class behavior (`--ui=true` / `--ui=false`) | Web UI startup path is part of maintainers' supported runtime contract and must be regression protected | Confirmed in Phase 5 |
| Treat GDScript `@export` metadata and anchor-linked variable naming as required indexing behavior | Real-world Godot projects rely on exported properties and stable variable identity through parser-to-pipeline flow | Confirmed in Phase 07.1 |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition**:
1. Requirements invalidated? -> Move to Out of Scope with reason
2. Requirements validated? -> Move to Validated with phase reference
3. New requirements emerged? -> Add to Active
4. Decisions to log? -> Add to Key Decisions
5. "What This Is" still accurate? -> Update if drifted

**After each milestone**:
1. Full review of all sections
2. Core Value check - still the right priority?
3. Audit Out of Scope - reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-14 after v1.0 milestone completion*
