# Phase 4: Verdicts & Acceptance Summaries - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md - this log preserves the alternatives considered.

**Date:** 2026-04-12
**Phase:** 04-verdicts-and-acceptance-summaries
**Areas discussed:** Aggregate framing

---

## Aggregate framing

### Primary answer

| Option | Description | Selected |
|--------|-------------|----------|
| Promotion verdict first | Lead with whether the current manifest run is sufficient to support promotion or wording changes, then support that conclusion with evidence totals and repo outcomes. | ✓ |
| Evidence inventory first | Lead with counts and tables, leaving maintainers to infer whether promotion is justified. | |
| Failure analysis first | Lead with failure and incomplete details before stating whether the run is promotable. | |

**User's choice:** Promotion verdict first

### Aggregate `incomplete` meaning

| Option | Description | Selected |
|--------|-------------|----------|
| Blocks promotion | Keep `incomplete` distinct from `fail`, but still treat it as insufficient evidence for support claims. | ✓ |
| Soft warning only | Treat `incomplete` as advisory and potentially still promotable. | |
| Equivalent to fail | Collapse `incomplete` into the same practical meaning as `fail`. | |

**User's choice:** Blocks promotion

### Primary audience

| Option | Description | Selected |
|--------|-------------|----------|
| Maintainer decision memo | Optimize the aggregate summary for the maintainer deciding whether a run supports a release, README, or support claim. | ✓ |
| Operator run report | Optimize the summary for the person who ran the workflow and already knows the details. | |
| Raw artifact index | Treat the summary mostly as navigation to deeper files. | |

**User's choice:** Maintainer decision memo

### Promotion strength on `pass`

| Option | Description | Selected |
|--------|-------------|----------|
| Qualified support claim | A pass supports only a bounded claim tied to the approved manifest corpus and current commit. | ✓ |
| Broad support claim | A pass supports general GDScript support claims beyond the approved corpus. | |
| Internal confidence only | A pass is useful internally but should not directly support public wording. | |

**User's choice:** Qualified support claim

### Non-pass wording

| Option | Description | Selected |
|--------|-------------|----------|
| Explicitly block promotion | Say directly that support wording should not be promoted or broadened until failures or evidence gaps are resolved. | ✓ |
| Imply but don't state | Present evidence status and let maintainers infer the promotion consequence. | |
| Stay neutral | Avoid any recommendation about promotion. | |

**User's choice:** Explicitly block promotion

---

## OpenCode's Discretion

- Exact per-repo summary formatting and information density.
- Exact placement of requirement traceability in summaries or docs.
- Exact amount of artifact-routing guidance for `fail` and `incomplete` outcomes.

## Deferred Ideas

- Per-target summary emphasis.
- Requirement mapping in summary surfaces.
- Failure follow-up detail level.

---

*Phase: 04-verdicts-and-acceptance-summaries*
*Discussion log generated: 2026-04-12*
