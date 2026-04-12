# GDScript Good-Tier Proof Misses

## How to use this file

- One row per failing or incomplete gating assertion.
- `Current Outcome` must stay `fail` or `incomplete`; both block promotion, but they mean different things.
- `fail` means comparable evidence disproved the gate.
- `incomplete` means promotion is blocked because the evidence is missing, unreadable, or not comparable enough to justify the claim.
- Informational mismatches go in the advisory section unless they expose a real correctness bug.
- Update this file immediately after every manifest-driven proof run.
- Record the aggregate promotion answer from `aggregate-summary.md` on every open gating miss so future reviewers can see whether the run ended in `qualified-support-only` or `do-not-promote`.
- Capture the next artifact path to inspect (`repo-meta.json`, `run-index.json`, or the relevant wrapper `queries/*.json`) instead of rewriting summary text by hand.

## Open gating misses

| Priority | Assertion ID | Repo | Current Outcome | Aggregate Promotion Answer | Suspected Layer | Next Artifact Path(s) To Inspect | Test File To Add First | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

### Open gating miss rules

- Set `Current Outcome` to `fail` when wrapper evidence is present and comparable, but it disproves the gating assertion.
- Set `Current Outcome` to `incomplete` when the gate cannot be judged honestly because indexing, metadata, query capture, or wrapper comparability is missing or broken.
- `Aggregate Promotion Answer` should usually be `do-not-promote` for any run that still has an open gating miss. If a miss is copied forward from an older run, note that explicitly in `Notes`.
- `Next Artifact Path(s) To Inspect` should point maintainers to the concrete evidence source for follow-up, for example:
  - `.../repo-meta.json` for requested/actual mode, pin, or comparability metadata
  - `.../run-index.json` for failure context and wrapper status
  - `.../queries/gd-same-script-calls.json` (or another wrapper) for the canonical raw MCP output

## Advisory / informational mismatches

| Assertion ID | Repo | Observation | Action |
| --- | --- | --- | --- |

## Closed misses

| Assertion ID | Repo | Fix Commit | Regression Coverage Added |
| --- | --- | --- | --- |
| calls.same_script_edges | topdown-starter | uncommitted | `scripts/test_gdscript_proof_same_script_calls.py` (wired into `make -f Makefile.cbm test`). Root cause was a false-negative proof query in `scripts/gdscript-proof.sh` (`gd-same-script-calls` matched `t:Function` instead of GDScript method targets), corrected and re-baselined from fresh proof evidence. |

When closing a miss, preserve the original `fail` versus `incomplete` story in the closure note so later promotion reviews can reconstruct whether the blocker was disproven behavior or missing evidence.
