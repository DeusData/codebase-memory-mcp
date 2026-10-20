# Semantic IR fixture provenance

Copied byte-for-byte on 2026-08-28 from Bernhard Jackiewicz's CodeAtlasIDE project,
`theia-extensions/codeatlas-views/test/fixtures/`, by the same author.
`tools/record-ir-fixtures.mjs` in that project recorded these files with engine 0.9.0.
They are snapshots, not hand-maintained alternative implementations.

| Files | Purpose |
| --- | --- |
| `create-user-ir.json` | Populates every fact family for render-model behavior tests. |
| Ten `ir-*.json` files | Cover leaf nodes, missing callers, absent error paths and symbols shared by several services. |

The recordings refer to the source project's `atlas-sample`, whose source matches
this repository's sample. Qualified names retain their recorded
`codeatlas-atlas-sample-<hash>` prefix and file URIs use `file:///workspace/...`.
They do not expose the recording machine's paths.
