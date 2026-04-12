# Phase 1 Research: Proof Contract & Corpus

**Researched:** 2026-04-11
**Status:** Complete
**Requirement IDs:** PROOF-01

## Research Question

What do we need to know to plan Phase 1 well so the proof corpus is pinned, reproducible, and explicit about canonical identity and Godot 4.x qualification?

## Current State

- The approved corpus already exists in `docs/superpowers/proofs/gdscript-good-tier-manifest.json` with 4 pinned targets.
- Three approved targets share the same upstream repo and are currently disambiguated with `project_subpath`.
- `scripts/gdscript-proof.sh` already supports manifest mode, ad hoc `--repo` runs, manifest defaulting for `godot_version`, per-target metadata capture, and pass/fail/incomplete output.
- The runbook and checklist already describe manifest mode, but the Phase 1 contract needs to be made more explicit so approval-bearing runs cannot drift into ad hoc repo selection.

## Relevant Findings

### 1. Manifest is already the strongest source of truth

Evidence:
- `docs/superpowers/proofs/gdscript-good-tier-manifest.json`
- `docs/superpowers/proofs/gdscript-real-project-validation.md`

Why it matters:
- The manifest already records `remote`, `pinned_commit`, `project_subpath` when needed, `godot_version`, `required_for`, and assertions.
- Phase 1 should refine this committed contract rather than introduce a second approval source.

Planning implication:
- Treat the manifest as the canonical approval-bearing corpus per D-01, D-02, D-03, D-10.

### 2. Canonical identity must be documented separately from convenience metadata

Evidence:
- `01-CONTEXT.md` locked decisions D-04 through D-07
- Runbook currently describes labels and local repo paths, but canonical identity wording is still implicit.

Why it matters:
- Multiple proof targets come from the same remote, so `project_subpath` cannot remain informal.
- Local checkout path changes between machines and runs, so it cannot be identity.

Planning implication:
- Add explicit language in manifest-adjacent docs and script output that canonical identity is:
  `remote + pinned_commit + project_subpath (when needed) + recorded godot_version`.
- Keep `label` readable but non-canonical.
- Keep local checkout path as run evidence only.

### 3. Godot 4.x qualification needs an explicit qualifying vs non-qualifying distinction

Evidence:
- Manifest already records `godot_version` values (`4.2`, `4.3`).
- Design spec says unknown-version repos may run but must be marked non-qualifying.

Why it matters:
- Phase 1 success criteria require approved targets to be explicitly qualified as Godot 4.x, not assumed.
- Future ad hoc runs should remain possible without silently counting toward acceptance.

Planning implication:
- Keep runnable-but-non-qualifying behavior for unknown/non-4.x targets.
- Make the approval contract explicit in docs and executable output, not only in prose.

### 4. Existing harness already contains the best implementation seam for enforcement

Evidence:
- `scripts/gdscript-proof.sh` contains manifest parsing, metadata preparation, repo-meta writing, and summary generation helpers.
- Indexed symbols include `parse_args`, `prepare_repo_metadata`, `write_repo_meta_json`, `write_repo_and_aggregate_summaries`, and manifest helpers.

Why it matters:
- Phase 1 can enforce the corpus/identity contract by tightening current proof-harness validation instead of inventing a parallel validator.

Planning implication:
- Prefer narrow changes in `scripts/gdscript-proof.sh` plus a focused regression test over large architectural work.

## Architecture Patterns To Reuse

- **Committed source of truth in docs + script parity:** update manifest/runbook/checklist together so written policy and executable behavior match.
- **Local-only proof artifacts:** continue using `.artifacts/gdscript-proof/<run>/` outputs for evidence instead of committing generated data.
- **Focused proof regressions in Python:** add a targeted script-level regression similar to `scripts/test_gdscript_proof_same_script_calls.py` when enforcing new manifest contract behavior.

## Recommended Implementation Shape

1. **Contract hardening plan**
   - Update the manifest and operator docs so the approved v1 corpus, canonical identity tuple, and qualifying/non-qualifying Godot status are explicit.
   - Document that manifest mode is the only approval-bearing lane.
   - Document that ad hoc `--repo` runs are debugging lanes only.

2. **Executable enforcement plan**
   - Tighten `scripts/gdscript-proof.sh` so manifest-driven runs emit/validate canonical identity fields and approval status consistently in repo metadata and summaries.
   - Preserve ad hoc runs, but mark them non-canonical and non-qualifying unless they satisfy explicit manifest-driven rules.

3. **Regression coverage plan**
   - Add a focused regression that proves shared-remote targets stay distinguishable via `project_subpath` and that non-manifest/ad hoc runs do not present themselves as approval-bearing.

## Dont-Hand-Roll

- Do **not** introduce a second corpus source outside `docs/superpowers/proofs/gdscript-good-tier-manifest.json`.
- Do **not** replace the existing proof harness with a new tool.
- Do **not** infer qualification from repo labels or local paths.
- Do **not** promote ad hoc `--repo` runs into acceptance evidence.

## Common Pitfalls

- Treating `label` as identity instead of readability metadata.
- Forgetting `project_subpath` for multi-project upstream repos.
- Allowing unknown-version or non-4.x targets to look equivalent to approved corpus entries.
- Updating docs without tightening machine-readable output, which leaves the contract unenforced.
- Tightening approval behavior so aggressively that debugging/investigation runs stop working at all.

## Phase 1 Planning Guidance

- Keep scope to **contract + corpus only**.
- Defer fixed query suite, evidence schema, and parity checks to later phases exactly as listed in `01-CONTEXT.md` deferred ideas.
- A good plan split is:
  1. corpus contract + docs/manifest alignment
  2. proof-harness enforcement + regression coverage

## Recommendation

Proceed with planning. No extra discovery phase is needed.

Phase 1 is **Level 0/1 discovery** work: existing patterns and tooling already exist, and the remaining work is contract hardening plus narrow harness enforcement.

## RESEARCH COMPLETE
