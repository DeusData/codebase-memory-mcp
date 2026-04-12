# Phase 3: Real-Repo Semantic Verification - Context

**Gathered:** 2026-04-12
**Status:** Ready for planning

<domain>
## Phase Boundary

Maintain the existing real-project proof workflow and verify that approved Godot demo targets expose the required GDScript definitions and relationships through the parser-to-MCP path in both sequential and parallel indexing modes. This phase locks the approval bar for semantic usefulness, evidence coverage, and parity expectations. It does not redesign the proof corpus, replace the existing harness, or broaden acceptance beyond the fixed Phase 03 roadmap requirements.

</domain>

<decisions>
## Implementation Decisions

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

</decisions>

<specifics>
## Specific Ideas

- Use the existing fixed proof query suite as the canonical usefulness surface for this phase rather than inventing a second direct-MCP acceptance layer.
- Keep review anchored in the full four-repo manifest corpus so downstream planning does not weaken approved coverage to only category-owner repos.
- Treat representative samples as mandatory evidence alongside counts, especially for class and method extraction, while avoiding an unnecessarily strict exact-edge-everywhere requirement.
- Compare sequential and parallel runs at the semantic-behavior level rather than enforcing exact artifact identity.

</specifics>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase contract and carried-forward decisions
- `.planning/ROADMAP.md` — Phase 3 goal, dependencies, and success criteria for real-repo semantic verification in sequential and parallel modes.
- `.planning/REQUIREMENTS.md` — `SEM-01` through `SEM-06` requirement definitions mapped to this phase.
- `.planning/STATE.md` — Current project state, Phase 03 focus, and parity concerns carried into planning.
- `.planning/phases/01-proof-contract-and-corpus/01-CONTEXT.md` — Locked corpus, identity, and manifest-only approval decisions that remain in force.
- `.planning/phases/02-isolated-proof-harness/02-CONTEXT.md` — Locked proof-entrypoint, artifact, and incomplete-run decisions that Phase 03 must preserve.

### Existing proof workflow and approval surface
- `scripts/gdscript-proof.sh` — Existing manifest-mode proof harness, fixed query suite, and current evidence-capture behavior.
- `docs/superpowers/proofs/gdscript-real-project-validation.md` — Real-project proof runbook, fixed query names, artifact layout, and run-review flow.
- `docs/superpowers/proofs/gdscript-good-tier-manifest.json` — Approved four-repo manifest corpus, `required_for` tags, and existing per-repo assertions.
- `docs/superpowers/proofs/gdscript-good-tier-checklist.md` — Current coverage checklist for indexing, signals, imports, inherits, and same-script-call behaviors.

### Existing proof design and regression patterns
- `docs/superpowers/specs/2026-04-04-gdscript-real-project-proof-design.md` — Original proof-design constraints, evidence expectations, and out-of-scope boundaries.
- `scripts/test_gdscript_proof_same_script_calls.py` — Existing regression-test pattern for asserting real proof artifacts and same-script call evidence.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `scripts/gdscript-proof.sh`: Already defines the fixed GDScript proof query suite, writes per-query wrapper artifacts, and summarizes per-repo and aggregate outcomes across the approved corpus.
- `scripts/test_gdscript_proof_same_script_calls.py`: Existing proof-oriented regression pattern that can be extended for additional semantic expectations without inventing a new test style.
- `src/pipeline/pipeline.c`: Existing sequential indexing orchestrator whose semantic outputs are part of the Phase 03 comparison surface.
- `src/pipeline/pass_parallel.c`: Existing parallel extraction and resolution pipeline whose outputs must remain semantically consistent with the sequential path.

### Established Patterns
- Manifest mode is already the only approval-bearing lane, so Phase 03 should harden evidence expectations inside that lane rather than introduce a new operator workflow.
- Raw evidence remains the per-query wrapper JSON files plus additive rollup artifacts under `.artifacts/gdscript-proof/`.
- Real proof validation in this repo prefers real pipeline runs and proof-artifact inspection over mocked semantic tests.

### Integration Points
- Phase 03 planning will likely touch `scripts/gdscript-proof.sh`, proof-focused regression tests under `scripts/`, and proof documentation under `docs/superpowers/proofs/`.
- Any parity implementation must connect the existing proof harness to both sequential and parallel indexing paths without changing the locked Phase 02 artifact contract.
- Any new comparison/reporting logic must remain compatible with the fixed four-repo manifest corpus and the existing summary/artifact tree.

</code_context>

<deferred>
## Deferred Ideas

- Broader direct MCP workflow spot-checks for graph discovery, tracing, or exploratory query ergonomics remain out of scope for Phase 03 approval unless later phases explicitly add them.
- Exact artifact-level equality or byte-for-byte diff requirements between sequential and parallel runs are out of scope for this phase.
- Any expansion or replacement of the approved four-repo manifest corpus remains outside Phase 03 and stays governed by earlier locked decisions.

</deferred>

---

*Phase: 03-real-repo-semantic-verification*
*Context gathered: 2026-04-12*
