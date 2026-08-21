## Context

See proposal.md for motivation. The relevant current state (verified against source):

- `get_code_snippet` → `build_snippet_response` (src/mcp/mcp.c:8639) → `resolve_snippet_source` (mcp.c:8506) → `read_file_lines(abs_path, start, end)` slices the LIVE file with `node->start_line/end_line` taken from the graph index. No freshness check anywhere on that path.
- `search_code` → `classify_all_grep_hits` (mcp.c:9576) attributes grep hits to symbols by `find_tightest_node` over indexed node ranges; `attach_result_source` (mcp.c:9071) then reads the live file with `r->start_line/end_line` (mode full, mcp.c:9107) or around `r->match_lines` (context mode, mcp.c:9126).
- The drift signal already exists: `coverage_path_freshness` (mcp.c:4218) compares the stored `mtime_ns`/`size` (from `cbm_store_get_file_hash`) with the file on disk. It is only consumed by `handle_check_index_coverage`; the read tools never consult it.
- Both tools already guard containment via `cbm_path_within_root`; the missing guard is metadata drift, not traversal.

## Goals / Non-Goals

**Goals:**
- `get_code_snippet` reports drift explicitly instead of slicing stale coordinates.
- `search_code` never attaches `source`/`context` sliced from a drifted file using stale ranges, and marks such rows.
- Drift reporting reuses `coverage_path_freshness` so the signal is identical to `check_index_coverage`.
- Healthy path (`metadata_match`) is byte-identical to today.

**Non-Goals:**
- Re-attributing grep hits for drifted files to different symbols (impossible without re-index; the drift flag makes the row explicitly untrustworthy). Re-indexing remains the user's action.
- Changing grep-hit classification rules, result ordering, or the TOON (text) output. TOON already carries only file:line:ranges and never reads source.
- Any index-format or DB change; no new metadata is recorded.

## Decisions

**D1 — Reuse `coverage_path_freshness` as the single drift oracle.**
`freshness == "metadata_changed" || "missing"` ⇒ drifted → skip the live read, emit `source_drift: true` + `freshness: <state>`.
`"metadata_match"` ⇒ current behavior unchanged.
Other states (`unavailable`, `outside_project`, `not_tracked`) ⇒ keep current behavior (no new refusal paths; `outside_project` is already handled by the containment guards).

**D2 — get_code_snippet: gate the read at `resolve_snippet_source`.**
`resolve_snippet_source` gains a `bool read_allowed` parameter (same path-building/containment logic; only the `read_file_lines` call is skipped). `build_snippet_response` computes freshness first and passes the flag; on drift it adds `source_drift`, `freshness`, and a `source` string stating no source is available because the file changed after indexing. `file_path`/`start_line`/`end_line`/`source_clipped` remain reported (they describe the indexed node the user asked about).

**D3 — search_code: gate each item's read in `attach_result_source`.**
`attach_result_source` gains `cbm_store_t *store, const char *project` (threaded from `handle_search_code` via `assemble_search_output`). When the item's file is drifted, neither `source` (full mode) nor `context` (context mode) is attached, and `source_drift: true` + `freshness: <state>` are added to the item. Raw (un-attributed) hits keep their grep-verified content — those lines come from the live grep output, not from stale index coordinates.

**D4 — Drift check per item, not per response.**
A search result spans multiple files; per-item checks keep a single drifted file from suppressing healthy results. Cost: one `stat`+hash lookup per distinct result file, which is proportional to the result set the tool already touches.

## Risks / Trade-offs

- [Extra `stat`+DB lookup per snippet/search item] → Bound by result set size; `coverage_path_freshness` already runs once per path in `check_index_coverage`; the lookup is a single indexed row read.
- [Agents relying on `source` in drifted worktrees get placeholder text] → This is the intended behavior change (issue #1750); the explicit `source_drift` marker is the machine-readable signal, and the message states the remedy (re-index).
- [`not_tracked`/`unavailable` files keep today's behavior] → A file without a hash record cannot be judged; refusing would regress read-only tool behavior for foreign files. Flagged in specs as out of contract scope.
- [Implementer must not widen classification semantics] → Guard is additive-only; healthy paths unchanged (tested).

## Migration Plan

No data migration. No configuration change. Release notes: read tools now flag source-drift on stale coordinates instead of serving them. Rollback = revert PR; no persistent state.

## Open Questions

None.
