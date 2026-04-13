# Phase 07: Nyquist Validation Backfill - Research

**Researched:** 2026-04-12
**Domain:** Backfilling Nyquist validation contracts for already-completed phases
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** `01-VALIDATION.md` through `04-VALIDATION.md` must be created as approved documents immediately, not left draft or pending.
- **D-02:** Approval must be grounded in existing committed evidence only: completed plans, summaries, verification reports, proof docs, tests, and the current repo state.
- **D-03:** This phase is a documentation backfill, not a fresh rerun campaign. Do not turn the work into new spot-checks or full reruns unless reconstruction exposes a concrete evidence gap.

### OpenCode's Discretion
- Exact per-phase verification-map granularity, so long as each doc stays Nyquist compliant and traces to existing phase evidence.
- Exact wording for reconstructed-vs-directly-automated verification, so long as the docs stay honest.
- Exact quick/full command pairings and latency estimates, using `05-VALIDATION.md` and `06-VALIDATION.md` as the repo-standard examples.

### Deferred Ideas (OUT OF SCOPE)
- Re-running Phases 01-04 as a new campaign just to justify the backfill.
- Changing prior phase scope, requirements mapping, or execution history.
- Expanding Nyquist coverage beyond the missing Phase 01-04 docs.
</user_constraints>

<phase_requirements>
## Phase Requirements

This phase has no mapped product requirement IDs. It is explicit milestone-audit tech-debt closure for missing Nyquist validation docs.
</phase_requirements>

## Summary

Phase 07 should be planned as a documentation reconstruction pass rooted in existing approved evidence, not as a new implementation or verification campaign. The milestone audit explicitly names the gap: Nyquist discovery is enabled, `05-VALIDATION.md` exists and is compliant, and Phases 01-04 are missing the required validation docs. [CITED: `.planning/v1.0-v1.0-MILESTONE-AUDIT.md`]

The strongest consistency model is already in-repo. `05-VALIDATION.md` and `06-VALIDATION.md` show the exact structure, approval posture, frontmatter, and verification-map style expected by Nyquist. They both mark `status: approved`, `nyquist_compliant: true`, and `wave_0_complete: true`, and they express phase validation as a contract tied to concrete plan tasks and automated commands. [VERIFIED: `.planning/phases/05-web-ui-launch-repair/05-VALIDATION.md`][VERIFIED: `.planning/phases/06-parallel-native-suite-repair/06-VALIDATION.md`]

The evidence needed to backfill Phases 01-04 already exists in the completed artifacts. Each phase has plans with task-level verify commands, summaries with actual files and decisions, and verification reports with must-have checks and behavioral spot-checks. The missing work is synthesis: translate those completed artifacts into per-phase Nyquist contracts in the standard validation-doc format without inventing new requirements or pretending the docs were used during the original execution. [VERIFIED: `.planning/phases/01-proof-contract-and-corpus/01-VERIFICATION.md`][VERIFIED: `.planning/phases/02-isolated-proof-harness/02-VERIFICATION.md`][VERIFIED: `.planning/phases/03-real-repo-semantic-verification/03-VERIFICATION.md`][VERIFIED: `.planning/phases/04-verdicts-and-acceptance-summaries/04-VERIFICATION.md`]

**Primary recommendation:** create one focused execution plan that backfills `01-VALIDATION.md` through `04-VALIDATION.md` from existing phase artifacts, keeps all four docs immediately approved per D-01 and D-02, and refreshes the milestone audit so Nyquist discovery can report full coverage without requiring fresh reruns. [CITED: `.planning/phases/07-nyquist-validation-backfill/07-CONTEXT.md`]

## Project Constraints (from AGENTS.md)

- Read `codemap.md` before working; it is the required repository architecture reference. [CITED: `AGENTS.md`]
- Prefer codebase-memory graph tools over manual code search for code discovery, though this phase is primarily planning/docs-oriented. [CITED: `/Users/shaunmcmanus/.config/opencode/AGENTS.md`]

## Relevant Paths

| Path | Why it matters |
|---|---|
| `.opencode/get-shit-done/templates/VALIDATION.md` | Canonical structure, frontmatter keys, and sign-off checklist for validation docs. |
| `.planning/phases/05-web-ui-launch-repair/05-VALIDATION.md` | Approved example for a completed product phase. |
| `.planning/phases/06-parallel-native-suite-repair/06-VALIDATION.md` | Approved example for an audit-closure phase. |
| `.planning/phases/01-proof-contract-and-corpus/01-VERIFICATION.md` | Existing evidence source for Phase 01 truths, commands, and requirements. |
| `.planning/phases/02-isolated-proof-harness/02-VERIFICATION.md` | Existing evidence source for Phase 02 commands and machine-readable artifact checks. |
| `.planning/phases/03-real-repo-semantic-verification/03-VERIFICATION.md` | Existing evidence source for Phase 03 parity and regression commands. |
| `.planning/phases/04-verdicts-and-acceptance-summaries/04-VERIFICATION.md` | Existing evidence source for Phase 04 verdict/promotion checks. |
| `.planning/v1.0-v1.0-MILESTONE-AUDIT.md` | Names the exact Nyquist gap and the success condition this phase should clear. |

## Architecture Patterns

### Pattern 1: Validation docs are execution contracts, not retrospectives
Use the template/example structure to express what should be sampled during execution, but ground every entry in commands and artifacts that already existed for the completed phase. The docs should read like approved contracts reconstructed from evidence, not like narrative postmortems.

### Pattern 2: Wrapper-first evidence stays authoritative
For Phases 02-04 especially, validation docs should point back to the canonical harness artifacts, regressions, and summaries already established by those phases rather than introducing alternate evidence paths.

### Pattern 3: Immediate approval requires explicit evidence lineage
Because D-01 and D-02 require immediate approval, each backfilled doc should make its per-task verification map traceable to concrete prior plans, summaries, verification reports, and known automated commands. Approval should come from evidence provenance, not from new reruns.

### Anti-Patterns to Avoid
- Marking the new docs `draft` or `pending`.
- Inventing new requirement IDs or pretending these docs changed historical phase scope.
- Requiring fresh reruns when existing plans, verification reports, and approved examples already provide enough evidence.
- Copying `05-VALIDATION.md` mechanically without aligning commands and requirement coverage to the earlier phases.

## Common Pitfalls

### Pitfall 1: Treating the backfill like a new verification campaign
**What goes wrong:** The plan asks for rerunning old tests or proof flows just to approve the docs.
**How to avoid:** Build each validation doc from existing PLAN/SUMMARY/VERIFICATION evidence first; only escalate if a concrete gap prevents an honest command or task map. [CITED: `.planning/phases/07-nyquist-validation-backfill/07-CONTEXT.md`]

### Pitfall 2: Losing phase-specific commands while copying the example format
**What goes wrong:** All four docs end up with generic commands or a Phase 05-style command set that does not match the earlier phases.
**How to avoid:** Pull quick/full command pairs from each phase's actual plan and verification artifacts. Phase 01 is manifest-contract oriented, Phase 02 centers on isolated harness artifact checks, Phase 03 includes parity/native suite commands, and Phase 04 centers on verdict summary regressions. [VERIFIED: phase 01-04 verification reports]

### Pitfall 3: Claiming automation that was never part of the original phase
**What goes wrong:** The backfilled docs list commands or file checks that were never actually part of the phase evidence.
**How to avoid:** Only use commands already present in plans, verification reports, Makefile targets, or directly adjacent completed regressions. If a check is reconstructed from doc evidence only, say so honestly in manual-only or sign-off wording.

## Validation Architecture

Nyquist for Phase 07 should enforce the same contract shape already used in Phases 05 and 06:

- Frontmatter must be `status: approved`, `nyquist_compliant: true`, `wave_0_complete: true` per D-01.
- Each backfilled phase doc must define one quick command, one full command, a per-task verification map, Wave 0 requirements, manual-only verifications, and a completed sign-off block.
- Each row in the per-task map should derive from the original phase plans and verification artifacts rather than from new execution.
- Phase 07's own execution can be validated by checking that `01-VALIDATION.md` through `04-VALIDATION.md` exist, use approved Nyquist frontmatter, and align with the milestone audit gap.

## Recommendation

Plan one autonomous documentation-focused wave:
1. Backfill `01-VALIDATION.md` and `02-VALIDATION.md` from the completed proof-contract and proof-harness artifacts.
2. Backfill `03-VALIDATION.md` and `04-VALIDATION.md` from the completed semantic parity and verdict-summary artifacts.
3. Refresh the milestone audit so Nyquist discovery records full validation-doc coverage for Phases 01-04.

---

*Phase: 07-nyquist-validation-backfill*
*Research completed: 2026-04-12*
