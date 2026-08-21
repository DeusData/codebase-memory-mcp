## Why

`index_repository` on a read-only repository with `--persistence true` fails after a successful publish. The graph database was published and is usable; only the team-sharing artifact export (`.codebase-memory/graph.db.zst` written inside the repo) fails with EROFS. The run then reports `status: "error"` with a generic hint — "Pipeline failed. Check repo_path exists and contains source files" — which blames `repo_path` for a cause (read-only repo) that the hint never names, and hides the fact that the index itself succeeded.

## What Changes

- The artifact export failure is captured on the pipeline and surfaced in the `index_repository` error hint, naming `artifact_export`, the artifact path, and the remedy (writable checkout or `--persistence false`), instead of the generic repo_path blame.
- Fail-hard semantics are **preserved**: a failed persistence export still returns a non-zero run result, exactly per the in-tree decision ("A failed persistence export intentionally fails the run; this used to be ignored", pipeline.c). Only the error message becomes truthful and actionable.
- The pipeline records its own artifact-export error snapshot so a hint is never built from a stale global error left over from a previous run.

## Capabilities

### New Capabilities
- `index-artifact-export-error-surfacing`: the `index_repository` error response truthfully attributes a post-publish persistence failure to the artifact export.

### Modified Capabilities
- None (no existing specs).

## Impact

- `src/pipeline/pipeline.c` — capture the artifact export error in `export_after_publish`; expose a query accessor.
- `src/pipeline/pipeline.h` — declare the accessor.
- `src/mcp/mcp.c` — error branch of `handle_index_repository` builds a truthful hint when the failure is the artifact export.
- `tests/test_pipeline.c` or `tests/test_artifact.c` — regression test: persistence export failure into an unwritable artifact directory returns non-zero, names the artifact export, and does not blame repo_path.
