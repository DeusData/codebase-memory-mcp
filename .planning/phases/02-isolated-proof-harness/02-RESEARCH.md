# Phase 2 Research: Isolated Proof Harness

**Researched:** 2026-04-12
**Status:** Complete
**Requirement IDs:** PROOF-02, EVID-01, EVID-02

## Research Question

What do we need to know to plan Phase 2 well so the existing proof harness becomes reproducible on one machine, keeps runtime state under repo-owned artifacts, and emits a fixed machine-readable MCP evidence set without replacing the current wrapper files?

## Current State

- `scripts/gdscript-proof.sh` is already the canonical proof entrypoint and already keeps per-repo `HOME`, `XDG_CONFIG_HOME`, and `XDG_CACHE_HOME` inside `.artifacts/gdscript-proof/<run>/state/<slug>/`.
- The harness already builds the local binary, indexes each repo, resolves `project_id`, runs a fixed 12-query MCP suite, writes wrapper JSON files under `<slug>/queries/`, and marks partial failures as `incomplete` via `task5` status updates in `repo-meta.json`.
- `write_repo_and_aggregate_summaries()` already derives manifest outcomes from wrapper files instead of querying the database directly, which is the right seam for Phase 2's additive rollup/index artifact.
- Existing proof regressions (`scripts/test_gdscript_proof_manifest_contract.py`, `scripts/test_gdscript_proof_same_script_calls.py`) validate generated artifacts directly, matching the established testing pattern for this workflow.

## Relevant Findings

### 1. The implementation seam is already in `scripts/gdscript-proof.sh`

Evidence:
- `process_repo_indexing()` writes `index.json`, `list-projects.json`, and `queries/*.json` and already preserves partial state on failures.
- `prepare_repo_metadata()` writes `repo-meta.json` before indexing starts.
- `write_repo_and_aggregate_summaries()` reads wrappers from disk and computes outcomes from artifact state.

Why it matters:
- D-01 and D-02 require hardening the existing harness, not introducing a new command layer.
- The best Phase 2 work is narrow script evolution around existing artifact emission points.

Planning implication:
- Modify `scripts/gdscript-proof.sh` directly.
- Add the new run-level rollup/index beside the existing files under the current run root.

### 2. Isolation is already mostly implemented, but not yet formalized as a contract artifact

Evidence:
- The runbook documents `.artifacts/gdscript-proof/<run>/state/<slug>/{home,config,cache}`.
- `record_workspace_env()` writes `env.txt` with run root, binary path, state root, and manifest path.
- `process_repo_indexing()` logs repo-specific runtime roots into `index.log`.

Why it matters:
- EVID-01 and D-04 through D-06 require reproducible local layout, not repo cloning/bootstrap.
- Current isolation exists operationally, but later phases still need a machine-readable way to discover evidence locations without parsing markdown/log text.

Planning implication:
- Add a machine-readable run index (for example `run-index.json`) that records run root metadata, per-repo state roots, artifact relative paths, and fixed query artifact paths.
- Keep cloning/materialization out of scope per D-05.

### 3. The raw evidence contract should stay wrapper-first, with an additive index

Evidence:
- `write_query_wrapper_json()` already produces canonical wrapper files with `query_name`, `project_id`, `artifact_slug`, literal `query`, and raw `result`.
- `write_repo_and_aggregate_summaries()` validates wrappers by filename/query-name mapping and consumes them directly.

Why it matters:
- D-07 through D-09 explicitly forbid replacing the per-query wrapper JSON files.
- The rollup/index should point to wrappers and summarize availability/status, not collapse results into a lossy aggregate.

Planning implication:
- Define the run-level index as an additive manifest of artifact paths, statuses, and failure notes.
- Preserve the existing `queries/*.json` files as the source evidence.

### 4. Incomplete behavior is already close to the desired model

Evidence:
- `set_repo_task5_status()` records `complete` vs `incomplete` plus message, project-resolution status, and CLI capture notes into `repo-meta.json`.
- `process_repo_indexing()` continues through all repos and sets `incomplete` on build, indexing, project-resolution, and query failures.
- Manifest summaries already convert comparability issues into repo/aggregate `incomplete` outcomes.

Why it matters:
- D-10 through D-12 require preserving partial artifacts and machine-readable failure context rather than fail-fast behavior.
- Phase 2 mainly needs to expose that state cleanly in the additive rollup/index and regression coverage.

Planning implication:
- Ensure the new index records per-repo stage status, available files, missing query wrappers, and failure messages.
- Add regression coverage for an intentionally broken query so partial artifacts survive and remain inspectable.

## Architecture Patterns To Reuse

- **Artifact-first proof verification:** tests should inspect `repo-meta.json`, wrapper JSON files, and generated summaries instead of depending only on exit codes.
- **Repo-owned local state:** continue using `.artifacts/gdscript-proof/<run>/...` as the only runtime/evidence root.
- **Script-level Python regressions:** add focused `scripts/test_gdscript_proof_*.py` coverage that builds a temp repo, runs the harness, then inspects emitted artifacts.

## Recommended Implementation Shape

1. **Run index / evidence inventory**
   - Add one machine-readable run-level artifact at the run root.
   - Include run metadata, aggregate summary path, fixed query suite names, and per-repo paths to `repo-meta.json`, `index.json`, `list-projects.json`, `queries/*.json`, and `summary.md`.

2. **Incomplete-path preservation contract**
   - Emit index entries even when a repo is incomplete.
   - Record stage status, failure messages, query availability, and any partial evidence paths already written.

3. **Docs and regression hardening**
   - Update the runbook to document the new additive artifact and incomplete semantics.
   - Add at least one regression that forces a query failure and proves partial artifacts plus the index survive.

## Dont-Hand-Roll

- Do **not** add a second wrapper/runner command outside `scripts/gdscript-proof.sh`.
- Do **not** replace `queries/*.json` with a summary-only artifact.
- Do **not** expand scope into repo cloning/materialization or parity checks; those belong to later phases.
- Do **not** convert incomplete repo/query failures into global fail-fast behavior.

## Common Pitfalls

- Writing the new rollup as human-only markdown instead of machine-readable JSON.
- Recording absolute paths only, which makes artifacts harder to compare across reruns.
- Treating missing wrappers as success because aggregate summaries still render.
- Forgetting to include incomplete-path metadata in the rollup, forcing later phases to rediscover failure state from logs.
- Accidentally changing ad hoc vs manifest approval semantics while touching harness output.

## Phase 2 Planning Guidance

- This is **Level 0/1 discovery** work: existing patterns already exist in the codebase; no external library research is needed.
- Keep plan scope on three outcomes only:
  1. isolated repo-owned runtime layout remains authoritative,
  2. the fixed query suite is discoverable through machine-readable artifacts,
  3. incomplete runs preserve inspectable evidence.
- A good split is:
  1. harness artifact/index contract,
  2. docs + regression coverage for success and incomplete paths.

## Recommendation

Proceed with planning.

Phase 2 is ready for execution planning without additional discovery.

## RESEARCH COMPLETE
