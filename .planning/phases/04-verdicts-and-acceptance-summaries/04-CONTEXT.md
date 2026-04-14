# Phase 4: Verdicts & Acceptance Summaries - Context

**Gathered:** 2026-04-12
**Status:** Ready for planning

<domain>
## Phase Boundary

Turn the existing manifest-driven proof evidence into explicit per-target `pass`, `fail`, or `incomplete` outcomes and maintainable summaries that help maintainers decide whether current GDScript support claims are honestly justified. This phase refines verdict framing and promotion-facing summary behavior; it does not replace the proof harness, broaden the approved corpus, or redefine the raw evidence contract established in earlier phases.

</domain>

<decisions>
## Implementation Decisions

### Aggregate framing
- **D-01:** `aggregate-summary.md` should act as a maintainer decision memo, not just an operator run log or artifact index.
- **D-02:** The aggregate summary should answer the promotion question first: whether the current manifest run is sufficient to support current or stronger GDScript support wording.
- **D-03:** Aggregate `incomplete` remains distinct from `fail`, but it still blocks promotion because missing or non-comparable evidence is not enough to justify support claims.
- **D-04:** Aggregate `pass` only justifies a qualified support claim bounded by the approved manifest corpus and the current commit under test; it must not be framed as proof of broad GDScript support beyond that scope.
- **D-05:** Aggregate `fail` and aggregate `incomplete` summaries should explicitly say that support wording must not be promoted or broadened until the listed failures or evidence gaps are resolved.

### OpenCode's Discretion
- Exact presentation details for per-repo verdict summaries, as long as they remain consistent with prior locked evidence contracts and the aggregate framing decisions above.
- Exact requirement-traceability wording or placement, provided planning keeps Phase 4 aligned with `PROOF-03` and `EVID-03`.
- Exact depth of follow-up guidance for `fail` or `incomplete`, as long as planning preserves honest promotion blocking and points maintainers toward the existing artifact tree when needed.

</decisions>

<specifics>
## Specific Ideas

- Lead the aggregate summary with a promotion-facing verdict rather than making maintainers infer it from tables or assertion counts.
- Keep any public-facing support language qualified to what the approved manifest corpus actually proves for the tested commit.
- Preserve the distinction between `fail` and `incomplete`, but treat both as insufficient for promotion.

</specifics>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase contract and carried-forward decisions
- `.planning/ROADMAP.md` — Phase 4 goal, dependencies, and success criteria for explicit verdicts and honest promotion summaries.
- `.planning/REQUIREMENTS.md` — `PROOF-03` and `EVID-03` requirement definitions mapped to this phase.
- `.planning/STATE.md` — Current project state and accumulated decisions that Phase 4 must preserve.
- `.planning/phases/01-proof-contract-and-corpus/01-CONTEXT.md` — Locked manifest-only approval, corpus identity, and qualification rules that bound Phase 4 summary claims.
- `.planning/phases/02-isolated-proof-harness/02-CONTEXT.md` — Locked harness-entrypoint, raw evidence, additive rollup, and `incomplete` handling decisions that Phase 4 must preserve.
- `.planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md` — Locked semantic approval surface, four-repo coverage, and sequential/parallel parity decisions that feed Phase 4 verdicts.

### Existing proof workflow and summary surfaces
- `scripts/gdscript-proof.sh` — Current source of per-repo and aggregate summary generation, verdict computation, and additive artifact output.
- `docs/superpowers/proofs/gdscript-real-project-validation.md` — Runbook describing manifest-mode outcomes, artifact review flow, and honest acceptance behavior.
- `docs/superpowers/proofs/gdscript-good-tier-manifest.json` — Approved proof corpus and assertion surface that Phase 4 summaries must interpret rather than redefine.
- `docs/superpowers/proofs/gdscript-good-tier-checklist.md` — Maintainer review checklist and current promotion-gate expectations that Phase 4 summaries should support.
- `docs/superpowers/proofs/gdscript-good-tier-misses.md` — Existing proof-gap tracking document for unresolved gating misses and advisory limitations.

### Existing proof design and regression coverage
- `docs/superpowers/specs/2026-04-04-gdscript-real-project-proof-design.md` — Original proof-design expectations for concise summaries, honest acceptance, and non-promotable incomplete evidence.
- `scripts/test_gdscript_proof_manifest_contract.py` — Regression coverage for manifest-mode summary shape and additive artifact expectations.
- `scripts/test_gdscript_proof_incomplete_artifacts.py` — Regression coverage for incomplete-path artifact retention and reporting expectations.
- `scripts/test_gdscript_proof_same_script_calls.py` — Existing proof-summary regression pattern that already inspects aggregate and repo summary content.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `scripts/gdscript-proof.sh`: Already computes repo-level and aggregate `pass` / `fail` / `incomplete` outcomes, writes `summary.md` and `aggregate-summary.md`, and records additive machine-readable artifacts.
- `docs/superpowers/proofs/gdscript-real-project-validation.md`: Already explains how maintainers review `summary.md`, `aggregate-summary.md`, `run-index.json`, and `semantic-parity.*` after a manifest run.
- `docs/superpowers/proofs/gdscript-good-tier-checklist.md`: Already captures promotion-oriented checklist language that Phase 4 can align with rather than replace.

### Established Patterns
- Manifest mode remains the only approval-bearing lane, so Phase 4 should improve interpretation of that lane instead of introducing a separate approval workflow.
- Raw per-query wrapper JSON files remain the canonical evidence; summaries stay additive and must not become the only source of truth.
- `incomplete` already has concrete operational meaning in the harness and docs, so Phase 4 should preserve that distinction while making its promotion impact clearer.

### Integration Points
- Phase 4 planning will likely touch `scripts/gdscript-proof.sh`, proof regressions under `scripts/`, and proof documentation under `docs/superpowers/proofs/`.
- Any aggregate-framing changes must fit the existing artifact tree rooted under `.artifacts/gdscript-proof/` and remain compatible with repo summaries, `run-index.json`, and `semantic-parity.*`.
- Any promotion wording must stay bounded by the approved manifest corpus and the current commit under test, not broader repo-wide or ecosystem-wide claims.

</code_context>

<deferred>
## Deferred Ideas

- Exact per-repo summary emphasis beyond the locked aggregate framing remains open for planning unless a later discussion explicitly narrows it.
- Exact requirement-to-summary mapping format remains open for planning.
- Exact amount of follow-up guidance and artifact routing for `fail` / `incomplete` remains open for planning.

</deferred>

---

*Phase: 04-verdicts-and-acceptance-summaries*
*Context gathered: 2026-04-12*
