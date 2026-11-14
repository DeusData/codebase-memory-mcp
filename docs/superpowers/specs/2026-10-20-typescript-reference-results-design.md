# TypeScript reference results design

## Intent and scope

Address [issue #2709](https://github.com/DeusData/codebase-memory-mcp/issues/2709): an agent must not mistake an empty indexed result for proof that a symbol is unused. The proposal has two phases: clearer results first, TypeScript references second. Implementation choices below are proposed for maintainer review.

The report concerns v0.11.0. Planning inspected checkout `a22271570eddba4ebed2c43577a5a4d36cb82086`, using the installed v0.12.1 MCP server. The report's failures have not been reproduced on this checkout. Establish the current behavior with fixtures before changing extraction.

## Existing behavior verified in source

- `src/mcp/mcp.c:8760`, `resolve_trace_edge_types`: normal tracing defaults to `CALLS`; callers can supply `edge_types` explicitly.
- `src/store/store.c:6662`, `st_degree_types`: search degrees already include `CALLS`, `USAGE`, `CALL_REFERENCE`, `INHERITS`, and `IMPLEMENTS`. Search degrees are not caller counts.
- `src/mcp/mcp.c:5229`, `sg_render_payload`, and `:9774`, `handle_trace_call_path`: both tools render bounded tree/JSON output with continuation behavior.
- `internal/cbm/lsp/ts_lsp.c:2777`, `resolve_value_references_at`: exact callable-value reference resolution currently handles narrow direct call-argument shapes.
- `internal/cbm/cbm.h:395` and `internal/cbm/extract_imports.c:1656`: `CBMImport.member` already retains the exported name of named import aliases. Reuse it; a nearby TypeScript resolver comment saying the metadata is unavailable is outdated.
- `src/pipeline/pass_usages.c` and `src/pipeline/pass_parallel.c` each materialize usage edges; `src/pipeline/lsp_resolve.h` contains their shared reference matching rules. Both paths must agree.

The graph reports partial parsing in several of these files. The source reads above, rather than graph absence, establish these findings.

## Phase 1: explain incomplete results

Preserve existing tool parameters, defaults, tables, counts, and edge classifications. Add two top-level fields, `usage_status` and `usage_note`, only in these situations:

| Situation | `usage_status` | Required meaning of `usage_note` |
| --- | --- | --- |
| An inbound or both-direction trace has zero visible inbound results over the complete requested traversal | `no_matching_inbound_edges` | No recorded inbound edges match the effective edge types and test filtering. This does not establish that the symbol is unused. Verify references and source. |
| A search has zero matches in its actual search domain | `no_search_match` | No indexed matches under these search settings; source search and coverage inspection remain necessary. |
| A returned structural/BM25 search row has inbound degree zero | `zero_indexed_inbound` | At least one displayed symbol has no recorded inbound relationships. This is not proof of unused code. |

Do not trigger on an empty continuation page, an offset beyond a nonempty result set, a semantic-only page lacking structural rows, or a failed read. For combined structural/semantic searches, `no_search_match` requires both domains to have no matches. Decide degree-based search notes from rows actually emitted after budget trimming; omit them if the budget floor displays no symbol rows. `detail: ids` omits degree-based notes because it does not display degrees. Existing filter and semantic diagnostics remain intact.

For an empty inbound trace, also report `edge_types` and `include_tests` with their effective values. Do not describe custom `USAGE` traces as an absence of function calls. Apply empty-result notes to callable and non-callable symbols alike; framework dispatch can also lack static calls.

Emit equivalent information in tree and JSON output and MCP structured content where applicable. An applicable empty-result status remains on budget-floor responses; verbose notes yield before primary graph rows, and omission is explicit using the existing budget conventions. Never bypass the byte ceiling to append a warning.

Tool descriptions and README examples explain the two workflows: `CALLS` for invocation; explicit `edge_types: ["USAGE", "CALL_REFERENCE"]`, `direction: "inbound"`, `depth: 1` for recorded references. A zero result requires source verification. Text searches may find comments, imports, or unrelated same-named symbols; those are candidates, not semantic references.

Phase 1 adds no automatic secondary traversal, owner-class heuristic, or per-result SQL query. [Issue #2152](https://github.com/DeusData/codebase-memory-mcp/issues/2152) proposes owner usages separately and still requires an upstream behavior decision.

## Phase 2: improve supported TypeScript references

Support these explicit cases in TypeScript and TSX, preserving existing JavaScript behavior:

1. Reads of exported constant objects within their defining module and through direct named imports, including aliases and namespace imports from a repository-local module.
2. References to interfaces and type aliases in annotations and type arguments, including `import type` and aliased named imports. Respect TypeScript's separate type and value bindings.
3. Property reads on a receiver with one proven project type, including interface/class fields and exported object properties whose defining graph node exists. Literal bracket access may resolve when it names the same proven property; dynamic keys and ambiguous receiver unions remain unresolved.

Use `USAGE` for constant/type/property references, `CALL_REFERENCE` only for an exact callable used as a value, and `CALLS` only for invocation. Imports alone, declarations, comments, and unrelated same-named symbols must not become usage edges. References at module scope originate from the existing module node. Keep existing edge deduplication: acceptance counts distinct referring files, not text occurrences.

Reuse existing extraction, import metadata, registry, cross-file definition filtering, and source-span matching. If exact non-callable semantic records are needed, add `CBM_RESOLVED_SYMBOL_REFERENCE` to `CBMResolvedKind`, carrying target QN, enclosing QN, source origin, and token span in `CBMResolvedCall`. Such records always materialize as `USAGE`, even when their target has a callable label. Admit them through both indexed and allocation-fallback reference matchers. Resolve exact symbol identity; never promote a project-wide short-name guess to precise evidence.

Preserve source-span and lexical-shadow blockers. A known but unresolved import/receiver blocks unrelated name fallback. Use existing `CBMImport.member`; carry it through the cross-file import binding path when the current projection loses alias identity. Extend shared metadata only where a failing fixture proves information is lost. Keep memory overhead bounded and reuse project/module indexes rather than scanning every definition for every usage.

Changes to extraction or semantic metadata must survive spill/rehydration, cold indexing, unchanged-file refresh, dependency edits, and incremental repair. Use the existing format-version rebuild mechanism to make an older index recompute the new edges once; never leave unchanged old files permanently missing the new reference kinds. Retain atomic index publication and failure preservation.

## Explicit follow-ups

Prisma schema-to-generated-client field identity, arbitrary event string values, unresolved re-export barrels, dynamic property keys, owner-registration inference, and adoption/performance hook changes are separate work. Keep these gaps visible in documentation and examples. This plan does not justify closing #2709 as fully resolved while those requests remain outstanding.

## Acceptance and validation

- Existing positive function-call results, pagination, test filtering, and default edge sets retain their meanings.
- Every empty-result status is tested in tree and JSON, including minimum-budget and excluded-tests cases.
- A checked-in public fixture provides an exported constant referenced from nine source files and an interface referenced from eight. Reference queries recover every expected file without counting imports/comments or same-name decoys as semantic evidence.
- Typed property tests distinguish two unrelated owners with the same property name, plus shadowed variables, unresolved imports, computed keys, and ambiguous receivers.
- Full and incremental indexing produce equal fixture reference edge sets after a definition rename and consumer edit. Both usage materialization paths agree.
- A fixture manifest is the semantic oracle. Grep is a lexical comparison, never the ground truth for semantic precision. Report recall and false edges by symbol category; mark deferred categories explicitly.
- Use the canonical `scripts/test.sh` entry point, focused suites during development, and its full default merge gate before shipping. Run the existing Linux and Windows CI legs; the report originated on Windows.

No new hosted service, model, runtime dependency, or standalone MCP tool is required. Implementation goes through feature branches and separate PRs for the two phases. This proposal contains planning documentation; implementation remains future work.
