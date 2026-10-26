# Large-source fixture provenance

Generated for this frontend on 2026-08-28; no external code was copied.
`src/big.ts` deterministically contains 100 small exported functions and `runAll`.
`src/support.ts` and `src/index.ts` were written for the fixture.

The 717-line source exceeds the 500-line `get_code_snippet` limit and exercises
`source_clipped`. The reader must disclose missing source rather than presenting
an incomplete file as complete. This corpus is separate from `atlas-sample` to
preserve that fixture's graph counts. It is indexing input, not a runnable package.
