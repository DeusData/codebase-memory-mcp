# TypeScript Reference Results Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Explain incomplete graph results immediately, then improve TypeScript references without changing the meaning of function calls.

**Architecture:** Phase 1 adds conditional interpretation metadata to the existing MCP search and trace renderers. Phase 2 extends exact TypeScript symbol resolution and the shared usage materialization path, preserving lexical identity, bounded queries, and incremental indexing.

**Tech Stack:** C backend, tree-sitter extraction, existing hybrid TypeScript resolver, SQLite graph store, existing C test framework.

**Spec:** [TypeScript reference results design](../specs/2026-10-20-typescript-reference-results-design.md), based on [issue #2709](https://github.com/DeusData/codebase-memory-mcp/issues/2709).

## Global constraints

- Keep `CALLS` for invocation, `CALL_REFERENCE` for proven callable values, and `USAGE` for ordinary references.
- Preserve tool defaults, existing table columns, search-degree semantics, filtering, and cursor meaning.
- Add no new hosted service, model, runtime dependency, or standalone MCP tool.
- Keep tree/JSON behavior equivalent and respect existing byte ceilings and budget-floor behavior.
- Preserve lexical shadowing, type/value namespace identity, and exact source occurrence matching.
- Update sequential and fused-parallel usage paths together; preserve atomic publication and incremental repair.
- Defer Prisma-generated references, arbitrary event strings, unresolved re-export barrels, and owner-registration inference.
- Work on feature branches; use separate PRs for the two phases. Do not commit or push to main.

## Review focus

- An empty page caused by offset, continuation, filtering, or output budget must not be presented as proof of a globally empty graph.
- A custom reference trace must describe its selected edge types rather than claiming no function calls.
- Same-name variables, properties, and type/value declarations must not acquire edges to unrelated definitions.
- Warm/incremental/spilled indexing must produce the same reference identities as a clean build.
- Existing readers, compact outputs, and Windows paths must remain usable after additive metadata and resolver changes.

## Delivery and file map

| Delivery | Files | Responsibility |
| --- | --- | --- |
| Phase 1, task 1 | `src/mcp/mcp.c`, `tests/test_mcp.c` | Search/trace interpretation metadata and renderer tests |
| Phase 1, task 2 | `src/mcp/mcp.c`, `README.md`, `tests/test_mcp.c` | Tool guidance and public response contracts |
| Phase 2, task 3 | `tests/fixtures/typescript_references/`, `tests/repro/repro_ts_symbol_references.c`, `tests/test_main.c`, `Makefile.cbm` | Public semantic fixtures and end-to-end regression suite |
| Phase 2, task 4 | `internal/cbm/extract_usages.c`, `internal/cbm/extract_imports.c`, `internal/cbm/lsp/ts_lsp.c`, `src/pipeline/pass_lsp_cross.c`, `tests/test_extraction.c`, `tests/test_ts_lsp.c` | Constant/type identities, alias propagation, and lexical tests |
| Phase 2, task 5 | `internal/cbm/cbm.h`, `internal/cbm/lsp/ts_lsp.c`, `src/pipeline/lsp_resolve.h`, `src/pipeline/pass_usages.c`, `src/pipeline/pass_parallel.c`, relevant tests | Exact typed property references and matching/materialization parity |
| Phase 2, task 6 | `src/pipeline/lsp_surface.c`, `src/pipeline/lsp_surface.h`, `src/store/store.h`, `tests/test_pipeline.c`, regression suite, `README.md` | Refresh compatibility, benchmark evidence, documented remaining gaps |

Source locations were inspected at `a22271570eddba4ebed2c43577a5a4d36cb82086`; locate by symbol again when implementing. Do not edit a listed file merely because it appears in the map: existing behavior that passes the required fixture needs no redundant rewrite.

## Phase 1: clearer results, independent of reindexing

### Task 1: conditional result interpretation

**Interfaces:** Extend the responses of `handle_trace_call_path(cbm_mcp_server_t *srv, const char *args)` and `sg_render_payload(...)`. Add top-level `usage_status` and `usage_note`; do not rename existing response keys. Status strings and trigger rules are specified in the design table.

- [ ] Add C regression cases in `tests/test_mcp.c`, using `setup_snippet_server` and `cbm_mcp_handle_tool`. Create a constant with inbound `USAGE` but no `CALLS`: default inbound tracing must keep `callers_total == 0`, add `usage_status == "no_matching_inbound_edges"`, and identify `edge_types == ["CALLS"]`, `include_tests == false`. An explicit one-hop `USAGE` trace must recover the reference while leaving call counts unchanged.
- [ ] Add cases for an unused function, a framework entry method, a zero-degree variable, and a genuinely unmatched name. Assert the specified status and a note that zero does not prove unused code. Existing positive traces have no empty-result status.
- [ ] Add negative trigger tests: offset beyond a nonempty search, a continuation with no remaining displayed inbound rows but nonzero traversal totals, semantic-only results, and a combined search with structural misses but positive semantic results. A read failure stays an error, not an empty-result status. Preserve existing filter hints alongside usage notes.
- [ ] Run `scripts/test.sh --suites mcp`; verify failures are the new response assertions before implementation. Baseline failures unrelated to these cases are recorded separately.
- [ ] Compute interpretation from actual search totals and final displayed rows, or complete filtered trace totals. Emit metadata through tree and JSON render paths. Do not perform new reference traversals or degree queries. Reuse the existing `cbm_search_result_t.in_degree`; its count already includes references.
- [ ] Add renderer/budget cases: `direction: "both"`, custom edge types, all callers excluded by `include_tests: false`, `include_tests: true`, IDs mode, JSON structured content, and `max_output_tokens: 128`. Applicable empty-result statuses survive the budget floor; degree-based notes about omitted symbol rows do not. Verbose note omission is explicit, and primary rows/cursor progress retain precedence.
- [ ] Run `scripts/test.sh --suites mcp,store_search`. Require the new cases, existing pagination tests, and `tool_output_byte_budgets` to pass; do not loosen byte limits to hide output bloat.
- [ ] Commit the response change on the phase-1 feature branch.

### Task 2: document and verify the public workflow

**Interfaces:** Existing `trace_path` arguments only: `direction: "inbound"`, `depth: 1`, `edge_types: ["USAGE", "CALL_REFERENCE"]`. Keep search `in`/`out` as relationship degrees.

- [ ] Extend `mcp_tools_list` and metadata tests to require concise descriptions distinguishing invocation tracing from reference tracing and instructing source verification for zero results.
- [ ] Run `scripts/test.sh --suites mcp` to observe the description-test failure; update the canonical tool registry descriptions in `src/mcp/mcp.c`. Reuse generated client metadata rather than editing every emitted integration.
- [ ] Add README examples using an actual indexed constant/interface fixture: locate the symbol with `search_graph`, trace explicit reference edges by exact QN, inspect `check_index_coverage`, and use source search when evidence is incomplete. State that grep matches require semantic interpretation.
- [ ] Run focused suites and the full `scripts/test.sh` merge gate in the normal development/CI environment. Exercise Linux and Windows through existing CI; do not claim an unrun platform passed.
- [ ] Commit and prepare a phase-1 PR with the before/after constant example and validation evidence. State that extraction coverage is unchanged. Publishing the PR is a separate authorized action.

**Phase-1 exit:** Existing indexes immediately return clearer empty results; no extractor, DB format, call counts, or reference coverage changes are required.

## Phase 2: TypeScript reference coverage

### Task 3: establish a semantic baseline

**Interfaces:** New `suite_repro_ts_symbol_references(void)` follows existing repro suite registration. A fixture manifest `tests/fixtures/typescript_references/expected.json` names symbol QNs, expected inbound edge types/source files, and unsupported cases.

- [ ] Create a small repository fixture with `symbols.ts`, an exported constant object read by nine distinct consumer files, an interface referenced by eight files, an aliased named import, a namespace import, `import type`, and TSX use. Add comments/import-only files and same-name decoys that must not count as semantic uses.
- [ ] In `tests/repro/repro_ts_symbol_references.c`, index the fixture into a temporary store using existing pipeline test helpers, then query exact target QNs. Compare distinct referring-file sets against the manifest. Avoid external CLI daemons and npm installs.
- [ ] Add positive invocation/callback controls and unsupported cases: a Prisma-generated field with no proven schema mapping, a raw event string, a dynamic property, and an unresolved re-export. Unsupported cases record the expected limitation rather than fabricating reference edges.
- [ ] Register the suite in `tests/test_main.c` and `Makefile.cbm`. Run `scripts/test.sh --suites repro_ts_symbol_references` and record, per case, which failures reproduce on this checkout. Existing passing cases become regression coverage and need no extractor change.
- [ ] Commit the fixture, manifest, and reproducible baseline on a phase-2 feature branch based on the accepted phase-1 work. Any known failing baseline is intermediate local work; do not publish a failing final PR.

### Task 4: resolve constants and type references by binding identity

**Interfaces:** Preserve `cbm_extract_usages(CBMExtractCtx *ctx)` and existing usage arrays. Reuse `CBMImport.local_name`, `.member`, `.module_path`, and `.is_default`; the import's exported identity must survive cross-file projection. Semantic references to these targets produce `USAGE`.

- [ ] Add extraction tests for ordinary constant reads, annotations, type arguments, type-only aliases, and module-scope initializers. Assert reference-token spans and enclosing symbol/module identity. Assert that declaration/import syntax and comments do not create uses.
- [ ] Add resolver tests for direct named imports, aliases, namespace imports, value shadowing, type/value bindings sharing a spelling, missing modules, and duplicate short names in unrelated files. Run `scripts/test.sh --suites extraction,ts_lsp,repro_ts_symbol_references` and isolate the failing identity cases.
- [ ] Extend `internal/cbm/extract_usages.c` only for demonstrated missing syntax. Preserve annotation/value context needed to distinguish TypeScript namespaces, using extraction metadata rather than a later name-only guess.
- [ ] Extend exact TS symbol lookup and cross-file binding propagation in `internal/cbm/lsp/ts_lsp.c` / `src/pipeline/pass_lsp_cross.c`. Reuse exported-name metadata already retained by `process_named_imports`; fix the outdated alias comment. Keep definitions indexed by module/QN instead of scanning the whole project for each reference. Do not rewrite unrelated call dispatch.
- [ ] Preserve conservative behavior for unsupported barrels and known unresolved imports; suppress unrelated project-wide fallback. Keep JavaScript/JSX resolution regressions in the `ts_lsp` suite passing.
- [ ] Run the focused suites again. Require the constant/interface referring-file sets to match the manifest, with zero same-name-decoy edges. Commit the narrow resolver changes.

### Task 5: exact typed properties and usage materialization

**Interfaces:** If existing semantic records cannot represent non-callable property identity, add `CBM_RESOLVED_SYMBOL_REFERENCE` to `CBMResolvedKind` in `internal/cbm/cbm.h`. Add `static void ts_emit_resolved_symbol_reference(TSLSPContext *ctx, const char *target_qn, TSNode site)` in `ts_lsp.c`; it fills the existing source-origin/span/enclosing-QN fields and never emits invocation evidence.

- [ ] Add tests for an interface field, class field, and exported object property, including reads outside call arguments. Include two unrelated owners with the same member name, local receiver shadowing, optional access, a literal bracket key, an unknown/dynamic key, and ambiguous union receivers.
- [ ] Require a reference to its exact owner/member QN for a single proven receiver type; require no precise edge for unresolved or ambiguous receivers. A property reference must never become `CALLS` or `CALL_REFERENCE` merely because a same-named callable exists.
- [ ] Run `scripts/test.sh --suites ts_lsp,pipeline,repro_ts_symbol_references` to establish failures. Extend the TS AST/type resolver to emit the exact property identity, reusing its receiver typing and graph definition identities. Populate project field metadata only when fixtures demonstrate it is missing.
- [ ] Update shared reference-candidate admission, pointer-index matching, and linear allocation-fallback matching in `src/pipeline/lsp_resolve.h` for the new resolved kind. Match enclosing QN, source origin, and exact token span; keep callable-reference promotion restricted to `CBM_RESOLVED_CALL_REFERENCE`.
- [ ] Apply the shared rule in `resolve_usage_edges` (`pass_usages.c`) and `resolve_file_usages` (`pass_parallel.c`). Add parity tests through the existing sequential/fused test seams and allocation-fallback tests. A known exact-but-unmaterialized target blocks short-name fallback.
- [ ] Run `scripts/test.sh --suites extraction,ts_lsp,pipeline,repro_call_scope_usages,repro_call_argument_usages,repro_ts_symbol_references`. Verify correct identities, unchanged call controls, and no new false edges; commit.

### Task 6: refresh compatibility and publishable evidence

**Interfaces:** Retain existing node QNs and store edge schema. Use the existing `CBM_INDEX_FORMAT_VERSION` full-rebuild routing for the changed indexing contract. Update the LSP surface codec only if reference metadata added in tasks 4–5 must persist there.

- [ ] Add pipeline tests for an old-format fixture index, no-edit refresh, consumer edit, exported definition rename, import alias change, delete/recreate, dependency-only edit, and spill/rehydration. Assert that reference target/source sets equal a clean build and that stale edges disappear. A failed rebuild preserves the previous published database.
- [ ] Run `scripts/test.sh --suites pipeline,pipeline_frozen,repro_ts_symbol_references` to establish the missing refresh behavior. Update rebuild-version handling and any new metadata serialization/deserialization together. Verify that the first new-version refresh repairs unchanged files, and the next no-op refresh avoids unnecessary rebuilding.
- [ ] Extend the repro suite to produce per-category recall and false-edge counts from `expected.json`. Require exact file-set recovery for supported fixture cases and zero edges to labeled decoys. Keep raw grep counts as a separate lexical baseline; do not present them as semantic truth or invent field-session benchmark results.
- [ ] Record cold-index time, no-op refresh time, peak memory, and bounded query output sizes before/after on the same fixture and an unchanged representative public TypeScript repository. Investigate added per-symbol SQL or whole-project scans before accepting a regression. Use existing measurement facilities; no universal timing guarantee is inferred from one machine.
- [ ] Update README limitations for Prisma-generated fields, event strings, unresolved barrels, and dynamic cases. State precisely which #2709 cases are covered; leave remaining work linked instead of claiming the full report resolved.
- [ ] Run the full `scripts/test.sh` merge gate and the existing Linux/Windows CI legs. Inspect the final diff, then commit and prepare a phase-2 PR containing fixture results, compatibility behavior, limitations, and actual validation evidence.

## Completion checklist

- [ ] Phase 1 is independently usable with existing indexes and does not alter call/reference semantics.
- [ ] Phase 2 retrieves all expected referring files for the supported constant, interface, and property fixtures without false same-name edges.
- [ ] Existing invocation/callback controls, pagination, output budgets, refresh behavior, and cross-platform checks pass.
- [ ] Deferred categories are visible, and no claim treats an empty graph as proof that deletion is safe.

## Planning validation

This document is a proposed implementation plan, not a test result. Only repository reads, MCP queries, GitHub issue inspection, and `scripts/test.sh --help` were performed during planning. No product code, fixture, build, or implementation test was run as part of creating the plan.
