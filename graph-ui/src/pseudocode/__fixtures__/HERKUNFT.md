# Closure fixture provenance

`closure-userService-create.json` was copied byte-for-byte on 2026-08-29 from
Bernhard Jackiewicz's CodeAtlasIDE project, at
`theia-extensions/codeatlas-views/test/fixtures/closure-userService-create.json`.
It is an original contribution from the same author.

The walk was assembled by hand from the IR recordings retained in
`src/twin/__fixtures__/`; it is not a recorded backend closure response. Symbols,
edges and line numbers come from those IRs. `depth: 3`, `cap: 8` and `truncated`
exercise clipping, traversal order, cycle handling and flow folding.

The source format uses `ClosureDto` fields `root`, `symbols`, `edges`, `truncated`,
`visited`, `depth` and `cap`. The provider uses `nodes` instead of `symbols`;
`closureDocumentOf` adapts that difference. Tests read the original fixture format.
The IR files are shared with the twin tests instead of duplicated here.
