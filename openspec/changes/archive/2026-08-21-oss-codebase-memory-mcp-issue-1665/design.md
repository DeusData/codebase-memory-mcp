## Context

See proposal.md. Verified state:

- `export_after_publish` (src/pipeline/pipeline.c:2492) runs AFTER the DB publish/rename (pipeline.c:2602-2613): the graph DB is already live at `final_path` when the artifact export runs. On failure it logs `pipeline.err` (phase `artifact_export`, err = `cbm_artifact_export_last_error()`) and returns the non-zero rc, which `cbm_pipeline_run` propagates (pipeline.c:2615-2618).
- `handle_index_repository` (src/mcp/mcp.c:8335-8351): `rc != 0` ⇒ `status: "error"` + the fixed hint "Pipeline failed. Check repo_path exists and contains source files. Try mode='fast'..." — written regardless of where in the pipeline the failure occurred.
- The fail-hard policy is an in-tree, cited decision ("A failed persistence export intentionally fails the run; this used to be ignored.", pipeline.c; commit 44d51745 "fix: address review feedback on artifact export error surfacing"). This change does NOT touch that policy.
- `cbm_artifact_export_last_error()` (src/pipeline/artifact.c:67) is global state — it can carry a PREVIOUS run's error, so the response must not consult it directly; the pipeline must snapshot the error of the run it just executed.

## Goals / Non-Goals

**Goals:**
- When a run fails post-publish specifically at the artifact export, the `index_repository` error hint names the real cause (artifact_export, the directory, the remedy: writable checkout or `--persistence false`), and notes the index was published.
- All other failures keep the existing hint byte-for-byte.
- The error surfaced is from the current run (snapshot on the pipeline), never stale global state.

**Non-Goals:**
- Changing the fail-hard semantics (non-zero rc on export failure stays).
- Changing the success path, `artifact_present`, or any non-error surface.
- Moving the artifact target elsewhere (a cache-dir fallback would be a maintainer-level design decision; not taken).

## Decisions

**D1 — Snapshot on the pipeline struct.**
`cbm_pipeline_t` (defined in pipeline_internal.h) gains `char export_error[CBM_SZ_1K]`, zeroed at `cbm_pipeline_run` start; `export_after_publish` writes `cbm_artifact_export_last_error()` (or "unknown") into it on failure. Accessor `cbm_pipeline_export_error()` declared in pipeline.h returns NULL-safe `export_error`. Rationale: the MCP handler already holds `p`; no global-state races (pipeline runs happen in workers or in-process).

**D2 — MCP hint selection by rc + snapshot only.**
In `handle_index_repository`'s error branch: if `cbm_pipeline_export_error(p)` is non-empty AND non-zero rc, emit the truthful artifact hint via the pipeline's `pipeline.err` phase knowledge (the snapshot is only ever set by `export_after_publish`, so its presence identifies the phase exactly). Otherwise the existing generic hint is unchanged, preserving the spec's "genuine pipeline failure" scenario.

**D3 — Test at the pipeline+store level, not full MCP.**
A full read-only-repo `index_repository` run in tests is heavy and platform-dependent (root can write anywhere; EROFS needs a real read-only mount). Instead the regression test drives `cbm_pipeline_run` with `persistence` enabled against a fixture repo whose artifact directory is replaced by a read-only directory (`chmod 0555` on the `.codebase-memory` parent — works for non-root; skipped politely when running as root, where it is not meaningful). Asserts: `rc != 0`, `cbm_pipeline_export_error(p)` non-empty and mentioning the artifact directory, and the pipeline error log contains `pipeline.err`.

## Risks / Trade-offs

- [chmod-based read-only fixture flaky under root/CI] → Test skips when `geteuid() == 0` (root bypasses permission checks); CI runs the suite as non-root in this repo's matrix.
- [Hint wording churn] → The truthful-hint string is new; existing tests asserting the exact generic hint on non-export failures keep passing because the snapshot is empty there (spec scenario 2 pins this).
- [Adding a field to `cbm_pipeline_t`] → Internal struct, single definition; accessor keeps the public header minimal.

## Migration Plan

No data/config migration. Behavior: only the error message of an already-failing run becomes accurate. Rollback: revert.

## Open Questions

None.
