## Why

`get_code_snippet` and `search_code` read the live file from disk but slice it using line coordinates captured at index time. After the file is edited on disk without re-indexing, those coordinates are stale: the tools return shifted/corrupted source text, or a grep hit is misattributed to an adjacent function. There is no signal that the answer is drifting — the tools present stale slices as though they were the current source.

## What Changes

- `get_code_snippet` detects index-to-disk drift before slicing the live file and, instead of returning corrupted source text, reports the drift explicitly (no `source` payload; a `source_drift` marker and a human-readable reason with the freshness state).
- `search_code` applies the same guard in `MODE_FULL` (and `MODE_COMPACT` where result rows carry source lines) so matches are not attached to unrelated source text.
- Drift detection reuses the existing freshness machinery (`coverage_path_freshness`, the same signal `check_index_coverage` already exposes as `metadata_match` / `metadata_changed` / `missing`) — no new metadata is written by the indexer.
- Behavior on `metadata_match` (clean) is unchanged, so a current graph keeps returning full snippets.
- No index format or API surface changes on the write side: this only changes what the two read tools return when the file drifted.

## Capabilities

### New Capabilities
- `mcp-tools-code-snippet-drift`: behavior of `get_code_snippet` and `search_code` when disk content no longer matches the indexed metadata (drift detection and honest reporting).

### Modified Capabilities
- None (no existing specs; this repo has no committed specs yet — `openspec/specs/` is empty).

## Impact

- `src/mcp/mcp.c` — `build_snippet_response`, `resolve_snippet_source`, `attach_result_source`, and the two tool handlers (drift guard + response shape).
- `tests/test_mcp.c` — new regression tests: snippet after drift reports drift instead of stale text; search results after drift do not attach drifted source; clean-metadata path keeps returning full source.
- Response contract of the two MCP tools gains an optional `source_drift` field on the drifted path only. No tool is added/removed/renamed; the healthy path is byte-compatible.
