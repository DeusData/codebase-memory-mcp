# Phase 2: Isolated Proof Harness - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-12
**Phase:** 02-isolated-proof-harness
**Areas discussed:** Canonical proof workflow, Isolation contract, Raw evidence contract, Incomplete-run behavior

---

## Canonical proof workflow

| Option | Description | Selected |
|--------|-------------|----------|
| Manifest script | Keep `scripts/gdscript-proof.sh` as the single repo-owned canonical command, with manifest mode as the approval-bearing path and ad hoc runs still available for debug. | ✓ |
| Wrapper command | Add a new higher-level wrapper around the script and make that the canonical operator entrypoint. | |
| Binary direct | Drive proof steps through direct `codebase-memory-mcp` or MCP commands instead of standardizing the existing harness script. | |

**User's choice:** Manifest script
**Notes:** The existing script should remain the canonical operator flow for Phase 2.

---

## Isolation contract

| Option | Description | Selected |
|--------|-------------|----------|
| State only | Isolate HOME/config/cache/output under `.artifacts`, but let maintainers point the harness at existing local repo checkouts. | ✓ |
| State + checkout | Have the workflow also manage local repo clones/checkouts under repo-owned artifacts. | |
| Full bootstrap | Own state, repo materialization, and all setup steps end-to-end inside the proof workflow. | |

**User's choice:** State only
**Notes:** Self-contained runtime state matters here; proof target checkout/bootstrap does not need to move into Phase 2.

---

## Raw evidence contract

| Option | Description | Selected |
|--------|-------------|----------|
| Wrappers only | Treat the existing per-query wrapper JSON files in `queries/` as the canonical machine-readable evidence set. | |
| Wrappers + index | Keep per-query wrapper files as raw evidence and add a small machine-readable rollup/index for easier downstream consumption. | ✓ |
| Rollup only | Prefer a single summarized machine-readable artifact over the individual query wrapper files. | |

**User's choice:** Wrappers + index
**Notes:** The rollup should be additive. The underlying per-query wrapper artifacts remain the raw proof evidence.

---

## Incomplete-run behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Keep partials | Preserve partial artifacts, record what succeeded, and mark the repo/run `incomplete` so evidence is still inspectable. | ✓ |
| Repo fail-fast | Stop processing that repo on first failure and emit only limited failure metadata for it. | |
| Global fail-fast | Abort the whole run on first repo/query failure instead of finishing the remaining repos. | |

**User's choice:** Keep partials
**Notes:** Partial evidence should remain available for diagnosis; incompleteness should be explicit rather than hidden behind fail-fast behavior.

---

## OpenCode's Discretion

- Exact rollup/index filename and schema.
- Exact machine-readable fields linking repo status to per-query wrapper files.
- Exact incomplete-status wording and summary formatting.

## Deferred Ideas

- Fully repo-owned proof-target checkout/bootstrap workflow.
- Final promotion verdict and summary policy beyond raw evidence capture.
