# Phase 07: nyquist-validation-backfill - Context

**Gathered:** 2026-04-12
**Status:** Ready for planning

<domain>
## Phase Boundary

Add the missing `*-VALIDATION.md` files for Phases 01-04 so the milestone has complete Nyquist validation coverage. This phase backfills explicit per-phase validation contracts in the expected locations; it does not re-run the earlier phases as a new validation campaign or expand milestone scope beyond closing the audit gap.

</domain>

<decisions>
## Implementation Decisions

### Approval posture
- **D-01:** The new `01-VALIDATION.md` through `04-VALIDATION.md` files should be created as approved documents immediately, not left in draft or pending state.
- **D-02:** Immediate approval is justified from existing committed evidence only: completed phase artifacts, plans, summaries, tests, proof docs, and the current repo state that already reflects those completed phases.
- **D-03:** Phase 07 is a documentation backfill, not a fresh rerun effort. Planning should not turn approval of the backfilled validation docs into a requirement for new spot-checks or full reruns unless a concrete gap is discovered while reconstructing the contract.

### OpenCode's Discretion
- Exact per-phase verification-map granularity for Phases 01-04, as long as each file remains Nyquist compliant and grounded in the evidence already produced by that phase.
- Exact wording for manual-only versus automated sections, provided the docs stay honest about what is reconstructed from existing evidence versus what is directly automated today.
- Exact task-to-command mapping details and feedback-latency estimates, using the established `05-VALIDATION.md` and `06-VALIDATION.md` shape as the consistency model.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 07 contract and audit gap
- `.planning/ROADMAP.md` — Phase 07 goal, gap-closure statement, and success criteria for adding Nyquist validation docs for Phases 01-04.
- `.planning/REQUIREMENTS.md` — Audit-closure note that Phase 07 is tech-debt closure and does not remap v1 requirement coverage.
- `.planning/STATE.md` — Current project state and accumulated prior-phase decisions that still define the contracts being backfilled.
- `.planning/v1.0-v1.0-MILESTONE-AUDIT.md` — Nyquist discovery findings identifying Phases 01-04 as missing `*-VALIDATION.md` coverage.

### Validation doc contract and approved examples
- `.opencode/get-shit-done/templates/VALIDATION.md` — Required validation-doc structure, frontmatter keys, and Nyquist sign-off checklist.
- `.planning/phases/05-web-ui-launch-repair/05-VALIDATION.md` — Existing approved validation-doc example showing the expected section layout and approval posture.
- `.planning/phases/06-parallel-native-suite-repair/06-VALIDATION.md` — Existing approved validation-doc example for a completed audit-closure phase.

### Phase contracts being backfilled
- `.planning/phases/01-proof-contract-and-corpus/01-CONTEXT.md` — Locked Phase 01 corpus, identity, and approval-lane decisions that its validation doc must reflect.
- `.planning/phases/02-isolated-proof-harness/02-CONTEXT.md` — Locked Phase 02 harness, isolation, evidence, and incomplete-run decisions that its validation doc must reflect.
- `.planning/phases/03-real-repo-semantic-verification/03-CONTEXT.md` — Locked Phase 03 usefulness, coverage, and sequential/parallel parity decisions that its validation doc must reflect.
- `.planning/phases/04-verdicts-and-acceptance-summaries/04-CONTEXT.md` — Locked Phase 04 verdict and promotion-summary decisions that its validation doc must reflect.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `.opencode/get-shit-done/templates/VALIDATION.md`: Shared Nyquist validation template that already defines the required file shape and sign-off fields.
- `.planning/phases/05-web-ui-launch-repair/05-VALIDATION.md`: Approved example for a product phase with explicit quick/full commands and per-task verification mapping.
- `.planning/phases/06-parallel-native-suite-repair/06-VALIDATION.md`: Approved example for an audit-closure phase with the same Nyquist structure Phase 07 should mirror.
- `.planning/v1.0-v1.0-MILESTONE-AUDIT.md`: Concrete source naming the missing validation files and the exact audit gap this phase must close.

### Established Patterns
- Validation files live directly in the phase directory as `{phase}-VALIDATION.md` with frontmatter including `status`, `nyquist_compliant`, and `wave_0_complete`.
- Approved validation docs use the same section order: Test Infrastructure, Sampling Rate, Per-task Verification Map, Wave 0 Requirements, Manual-Only Verifications, and Validation Sign-Off.
- This repo treats validation docs as explicit execution contracts tied to existing plan/task structure rather than vague testing notes.

### Integration Points
- Phase 07 work should add `01-VALIDATION.md`, `02-VALIDATION.md`, `03-VALIDATION.md`, and `04-VALIDATION.md` under the existing phase directories.
- The backfilled docs should align with milestone-audit Nyquist discovery, the shared validation template, and the earlier phase contracts without changing those phase scopes.
- A follow-up milestone audit should be able to discover the new files automatically and report full Nyquist coverage with no workflow changes.

</code_context>

<specifics>
## Specific Ideas

- Use the existing approved `05-VALIDATION.md` and `06-VALIDATION.md` files as the style and approval model for the backfilled docs.
- Treat already-completed phase artifacts as sufficient evidence to approve the new docs immediately.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 07-nyquist-validation-backfill*
*Context gathered: 2026-04-12*
