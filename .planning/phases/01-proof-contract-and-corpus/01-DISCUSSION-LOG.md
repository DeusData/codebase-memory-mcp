# Phase 1 Discussion Log

**Phase:** `01-proof-contract-and-corpus`
**Date:** 2026-04-11
**Mode:** `discuss`
**Outcome:** Context captured and ready for planning

## Scope Framing

- Phase boundary: lock the approved real Godot proof corpus and the target-identity rules that make reruns comparable.
- Out of scope for this discussion: proof harness isolation details, machine-readable evidence capture, and semantic acceptance checks in later phases.

## Repository Context Reviewed

- `.planning/PROJECT.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/ROADMAP.md`
- `.planning/codebase/ARCHITECTURE.md`
- `.planning/codebase/TESTING.md`
- `.planning/codebase/CONVENTIONS.md`
- `scripts/gdscript-proof.sh`
- `scripts/test_gdscript_proof_same_script_calls.py`
- `docs/superpowers/proofs/gdscript-real-project-validation.md`
- `docs/superpowers/proofs/gdscript-good-tier-manifest.json`
- `docs/superpowers/proofs/gdscript-good-tier-checklist.md`
- `docs/superpowers/specs/2026-04-04-gdscript-real-project-proof-design.md`
- `docs/superpowers/plans/2026-04-04-gdscript-real-project-proof.md`

## Gray Areas Offered

- Corpus membership
- Target identity
- Godot qualification
- Approved-run policy

## Areas Selected By User

- Corpus membership
- Target identity
- Godot qualification

## Decisions Made

### Corpus membership

- Freeze the current four manifest targets as the approved v1 proof corpus.
- Do not allow approval-bearing runs to swap in arbitrary alternate qualifying targets.
- Treat manifest edits as the only valid way to change the approved corpus.

### Target identity

- Canonical identity is `remote` + `pinned_commit` + `project_subpath` when needed + recorded `godot_version`.
- `label` stays human-facing metadata and is not part of canonical identity.
- Local checkout path is evidence about a run environment, not part of the identity contract.

### Godot qualification

- Only explicitly confirmed Godot `4.x` targets count toward v1 acceptance.
- Unknown-version or non-4.x targets may still be run for investigation, but they are non-qualifying.

### Approved-run policy

- Follow-up clarification captured after the main three decisions: only manifest-defined runs are approval-bearing.
- Ad hoc `--repo` runs remain allowed for debugging and investigation but do not count toward acceptance.

## Notes For Planning

- The current manifest already appears to satisfy the user's preferred corpus shape, so planning can focus on locking and enforcing that contract instead of inventing a new corpus.
- Because the current proof script already supports both manifest and ad hoc modes, planning should distinguish approval-bearing behavior from debugging behavior explicitly.
- The main implementation risk is mismatch between documented approval rules and what `scripts/gdscript-proof.sh` currently accepts without validation.

## Deferred To Later Phases

- Proof harness isolation and deterministic artifact capture.
- Fixed evidence schema and raw query outputs.
- Sequential versus parallel proof parity.
- Semantic usefulness checks for proof queries.
