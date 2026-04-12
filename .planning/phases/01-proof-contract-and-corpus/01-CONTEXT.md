# Phase 1: Proof Contract & Corpus - Context

**Gathered:** 2026-04-11
**Status:** Ready for planning

<domain>
## Phase Boundary

Maintainers can run verification against a pinned, comparable set of real Godot 4.x proof targets with unambiguous repo identity and project subpath handling. This phase locks the approved corpus and target-identity contract only. Proof execution details, evidence capture, and semantic acceptance stay in later phases.

</domain>

<decisions>
## Implementation Decisions

### Corpus membership
- **D-01:** Phase 1 freezes the current four manifest targets in `docs/superpowers/proofs/gdscript-good-tier-manifest.json` as the approved v1 proof corpus.
- **D-02:** Approved corpus membership is manifest-defined and does not vary run-to-run based on any three qualifying targets.
- **D-03:** Corpus changes require an explicit committed manifest update rather than ad hoc operator choice at runtime.

### Target identity
- **D-04:** The canonical identity of an approved proof target is `remote` + `pinned_commit` + `project_subpath` when needed + recorded `godot_version`.
- **D-05:** `project_subpath` is required whenever one upstream repo contains multiple proof targets so reruns select the same project again.
- **D-06:** Human-friendly labels remain metadata for readability and reporting, not part of canonical identity.
- **D-07:** Local checkout path is run evidence only and must not be treated as part of the target identity contract.

### Godot qualification
- **D-08:** Only proof targets with an explicitly recorded confirmed Godot `4.x` version count toward v1 acceptance.
- **D-09:** Unknown-version and non-4.x targets may still be runnable for debugging or investigation, but they are non-qualifying and cannot satisfy the approved corpus requirement.

### Approved-run policy
- **D-10:** Approval-bearing verification is manifest-driven only.
- **D-11:** Ad hoc `--repo` runs remain allowed as debugging or investigation lanes, but they are explicitly non-canonical and do not count toward acceptance.

### OpenCode's Discretion
- Exact validation wording and guardrails that enforce the locked corpus and identity contract.
- Whether to add manifest/schema validation helpers in Phase 1 or defer stricter enforcement to planning if existing proof tooling already expresses the contract clearly enough.

</decisions>

<specifics>
## Specific Ideas

- Keep the approved v1 proof corpus aligned to the existing committed good-tier manifest instead of introducing a new target set for Phase 1.
- Preserve the current ability to run ad hoc proof commands for debugging, but document them as non-canonical.

</specifics>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase contract
- `.planning/ROADMAP.md` — Phase 1 goal, dependency, and success criteria.
- `.planning/REQUIREMENTS.md` — `PROOF-01` requirement mapping for pinned real Godot 4.x targets with recorded identity and project subpath support.
- `.planning/STATE.md` — Current project focus and existing concerns about proof target coverage.

### Proof corpus and operator contract
- `docs/superpowers/proofs/gdscript-good-tier-manifest.json` — Current approved four-target manifest, including pinned commits, remotes, project subpaths, and recorded Godot versions.
- `docs/superpowers/proofs/gdscript-real-project-validation.md` — Manifest-mode runbook, approved artifact interpretation, aggregate verdict rules, and current proof-target handling.
- `docs/superpowers/proofs/gdscript-good-tier-checklist.md` — Current checklist for corpus composition and qualifying coverage expectations.

### Proof design constraints
- `docs/superpowers/specs/2026-04-04-gdscript-real-project-proof-design.md` — Reproducibility and comparability requirements, recorded metadata expectations, and Godot version qualification rules.
- `docs/superpowers/plans/2026-04-04-gdscript-real-project-proof.md` — Historical proof workflow plan and implementation context that shaped the current harness.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `scripts/gdscript-proof.sh`: Existing proof harness already supports manifest mode, ad hoc `--repo` mode, per-target metadata capture, and project-subpath-aware target handling.
- `scripts/test_gdscript_proof_same_script_calls.py`: Existing regression test already exercises manifest-driven proof execution with pinned target metadata and assertion handling.

### Established Patterns
- Proof artifacts are isolated under repo-owned `.artifacts/gdscript-proof/<timestamp>/` directories.
- The current proof workflow already records per-target metadata such as git refs, commit SHA, labels, project IDs, and capture status.
- The committed manifest is already the strongest existing source of truth for approved target selection.

### Integration Points
- Phase 1 planning will likely refine `docs/superpowers/proofs/gdscript-good-tier-manifest.json`, `docs/superpowers/proofs/gdscript-real-project-validation.md`, and `scripts/gdscript-proof.sh` so the written contract and executable behavior match.
- Any enforcement or validation added in this phase must fit the existing proof harness rather than invent a separate approval path.

</code_context>

<deferred>
## Deferred Ideas

- Exact proof query set, artifact schema, and machine-readable evidence requirements belong to Phase 2.
- Sequential versus parallel parity evidence belongs to Phase 3.
- Final semantic usefulness checks for queries and answers belong to later proof-validation phases.

</deferred>

---

*Phase: 01-proof-contract-and-corpus*
*Context gathered: 2026-04-11*
