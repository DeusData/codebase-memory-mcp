# Phase 2: Isolated Proof Harness - Context

**Gathered:** 2026-04-12
**Status:** Ready for planning

<domain>
## Phase Boundary

Make the real-project GDScript proof workflow reproducible and machine-readable. This phase standardizes the repo-owned proof command, keeps runtime state and outputs isolated under repo-owned artifacts, and defines the raw MCP evidence contract for each proof target. It does not expand the proof corpus, add new semantic categories, or define final promotion verdict policy.

</domain>

<decisions>
## Implementation Decisions

### Canonical proof workflow
- **D-01:** The canonical Phase 2 proof entrypoint is the existing `scripts/gdscript-proof.sh` harness.
- **D-02:** Manifest mode remains the only approval-bearing workflow; Phase 2 should harden and standardize that path instead of introducing a new wrapper command or direct binary-driven operator flow.
- **D-03:** Ad hoc `--repo` runs remain available for debugging, but they stay outside the canonical approval lane.

### Isolation contract
- **D-04:** Phase 2 isolates proof runtime state and outputs under repo-owned artifacts only: `HOME`, `XDG_CONFIG_HOME`, `XDG_CACHE_HOME`, local store/cache, logs, and generated proof outputs.
- **D-05:** Phase 2 does not take ownership of cloning, fetching, or materializing proof target repositories under `.artifacts`; maintainers may continue to point the harness at existing local checkouts.
- **D-06:** Reproducibility for this phase means consistent local state layout and evidence locations for the same target on the same machine, not a fully self-bootstrapping checkout pipeline.

### Raw evidence contract
- **D-07:** The existing per-query wrapper JSON files under each repo's `queries/` directory remain the canonical raw MCP evidence set.
- **D-08:** Phase 2 should add a small machine-readable rollup or index artifact alongside the per-query wrappers so later phases can consume a run without rediscovering file layout manually.
- **D-09:** The rollup/index is additive; it must not replace or collapse away the underlying per-query wrapper artifacts.

### Incomplete-run behavior
- **D-10:** When indexing or part of the fixed query suite fails, the harness should preserve partial artifacts and record what succeeded.
- **D-11:** Such repos or runs should be classified as `incomplete` rather than triggering a global fail-fast abort.
- **D-12:** Incomplete handling should keep machine-readable failure context inspectable so maintainers can diagnose problems without rerunning queries by hand.

### OpenCode's Discretion
- Exact filename and schema for the additive run-level rollup/index artifact.
- Exact metadata fields needed in the rollup to point cleanly at per-query wrappers, repo status, and failure notes.
- Exact wording and summary formatting for incomplete evidence so it stays machine-readable and human-reviewable.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase contract and carried-forward decisions
- `.planning/ROADMAP.md` — Phase 2 goal, dependency on Phase 1, and success criteria for isolated execution and machine-readable raw evidence.
- `.planning/REQUIREMENTS.md` — `PROOF-02`, `EVID-01`, and `EVID-02` requirements mapped to this phase.
- `.planning/STATE.md` — Current project state and carried-forward notes about isolated proof execution and fixed query capture.
- `.planning/phases/01-proof-contract-and-corpus/01-CONTEXT.md` — Locked Phase 1 decisions for manifest-only approval, canonical identity, and debug-only ad hoc runs.

### Existing proof workflow and artifact contract
- `docs/superpowers/proofs/gdscript-real-project-validation.md` — Current proof runbook, isolated state layout, fixed query suite, wrapper artifact layout, and operator checklist.
- `docs/superpowers/proofs/gdscript-good-tier-manifest.json` — Approved corpus, pinned targets, assertion/query names, and manifest-mode approval contract that Phase 2 must preserve.
- `scripts/gdscript-proof.sh` — Existing harness implementation for repo state isolation, indexing, project resolution, query capture, and summary generation.
- `scripts/test_gdscript_proof_manifest_contract.py` — Regression pattern for proof-harness contract testing and artifact inspection.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `scripts/gdscript-proof.sh`: Existing proof harness already builds the local binary, isolates per-repo runtime state, indexes repositories, resolves project IDs, captures a fixed query suite, and writes summaries plus `repo-meta.json`.
- `scripts/test_gdscript_proof_manifest_contract.py`: Existing subprocess-based regression test pattern for building temporary repos/manifests and asserting generated proof artifacts.
- `docs/superpowers/proofs/gdscript-real-project-validation.md`: Existing runbook already documents the artifact tree and wrapper-file evidence model that Phase 2 can formalize.

### Established Patterns
- Proof artifacts live under `.artifacts/gdscript-proof/<timestamp-pid-suffix>/` with per-repo subdirectories and isolated state roots.
- Per-repo isolation already uses repo-owned `HOME`, `XDG_CONFIG_HOME`, and `XDG_CACHE_HOME` directories rather than ambient user state.
- Raw query capture already uses named wrapper JSON files keyed to a fixed query suite, with repo metadata and summaries written beside them.
- Manifest mode is already the approval-bearing lane; ad hoc runs already exist as a separate debug-oriented path.

### Integration Points
- Phase 2 work should extend `scripts/gdscript-proof.sh` rather than inventing a second proof harness.
- Any new rollup/index artifact should live alongside existing `queries/*.json`, `repo-meta.json`, and summary files in the current proof artifact tree.
- Regression coverage should follow the current Python proof-test approach and assert both machine-readable evidence presence and incomplete-path behavior.

</code_context>

<specifics>
## Specific Ideas

- Keep the current script-centric operator flow instead of introducing a new wrapper layer for this phase.
- Treat raw per-query wrappers as the source evidence and add a lightweight index on top, not a replacement summary-only artifact.

</specifics>

<deferred>
## Deferred Ideas

- Fully repo-owned clone/bootstrap/materialization of proof target repositories is out of scope for Phase 2.
- Final pass/fail/incomplete promotion summaries remain Phase 4 work.
- Sequential versus parallel semantic parity stays in Phase 3.

</deferred>

---

*Phase: 02-isolated-proof-harness*
*Context gathered: 2026-04-12*
