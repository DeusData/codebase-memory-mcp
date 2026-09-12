# Upstream sync playbook & log

This fork (`turtacn/codebase-memory-mcp`) carries a large language-LSP-uplift
campaign on top of upstream `DeusData/codebase-memory-mcp`. This doc is the
reusable procedure for pulling upstream forward without losing either side's
work, plus the log of each sync (newest first) with the conflicts hit and how
they were resolved.

## Procedure (merge, never rebase)

Rebasing our campaign commits would rewrite already-pushed history and force a
force-push. **Always merge** — it preserves both histories and both sides'
commits verbatim.

1. **Safety anchor first.** `git branch -f backup/pre-upstream-sync-<sha> main`
   and push it. Our work is then recoverable no matter what the merge does.
2. **Add + fetch upstream.** `git remote add upstream
   https://github.com/DeusData/codebase-memory-mcp.git`; `git fetch upstream`.
3. **Measure divergence.** `git merge-base main upstream/main`;
   `git log --oneline upstream/main..main` (ours) and `main..upstream/main`
   (theirs); `git rev-list --left-right --count main...upstream/main`.
4. **Anticipate conflicts.** Intersect the files each side changed since the
   merge-base:
   `comm -12 <(git diff --name-only <base> main|sort) <(git diff --name-only <base> upstream/main|sort)`.
5. **Merge without auto-commit** so you can verify before finalizing:
   `git merge --no-ff --no-commit upstream/main`. Resolve every `UU` file
   preserving **both** sides' intent (never blind-pick a side).
6. **Build BEFORE committing.** `make -f Makefile.cbm cbm`. A textually clean
   auto-merge can still be **semantically** broken (a call site on one side, a
   signature change on the other — see 2026-09 log). The compiler is the only
   reliable detector; fix each error and rebuild until green.
7. **Focused gate at the intersection.** `make -f Makefile.cbm test-focused
   TEST_SUITES="<suites covering the overlap + our campaign>"`. Must be all-pass.
8. **Commit the merge + push.** Only after build + gate are green.

## Log

### 2026-09-11 — sync onto upstream `daaf538c`-era → merged at build-green + gate 887/0

- **Divergence:** merge-base `b3d898e1`. **Ours +49** (Perl/Java/Python/Rust/Go
  Hybrid-LSP uplift campaign; see `lsp-uplift/`). **Upstream +139** (daemon
  memory-budget + userns fixes, CI clang-21 repin, yaml/gitignore/watcher
  correctness, C# multi-attribute, coverage-range, graph-ui, Makefile/vendored).
- **Safety:** `backup/pre-upstream-sync-4be307f6` pushed to origin.
- **Auto-merge:** 14 files changed on both sides; 13 auto-merged textually. Our
  Perl LSP core (`perl_lsp.c`, `pass_lsp_cross.c`, `registry.c`,
  `extract_calls.c`) had **zero** conflicts — upstream never touched them.

**Conflict 1 (textual, trivial): `src/pipeline/pass_semantic_edges.c`.**
Both sides independently fixed the *same* bug — `qsort` with a NULL base when a
graph has zero Function/Method nodes (ours + upstream `77a0c7d9`
"skip the function sort when the graph has no functions"). The guard
(`if (func_count > 0) qsort(...)`) was identical on both sides; only the comment
differed. Resolved by keeping upstream's fuller single-block comment. No code
lost.

**Conflict 2 (SEMANTIC, build-caught — the one that mattered):
`find_jvm_modifiers` signature drift.**
Auto-merge produced a clean tree that **did not compile**:
```
extract_defs.c:5485: error: too few arguments to function 'find_jvm_modifiers'
```
- **Upstream** (#1692, C#/PHP multi-`attribute_list`) refactored
  `find_jvm_modifiers` from `TSNode find_jvm_modifiers(node, lang)` (returns the
  single modifiers wrapper) to `int find_jvm_modifiers(node, lang, TSNode *out,
  int max)` (fills `out[]`, returns the count).
- **Our side** (Java wave-2/3, `8b27aad4`) added a *new* call site
  `jvm_class_testng_test` still using the old 2-arg form.
- They are in different regions, so git merged them with no marker — but the new
  call site calls the new signature wrongly. **Lesson: an auto-merge that leaves
  no conflict markers is not proven correct; only the build proves it.**
- **Fix:** rewrote `jvm_class_testng_test` to the new fill-array/count API
  (matching the existing call sites at `extract_defs.c:1733,2105`), iterating the
  returned modifier wrappers to find a class-level `@Test`. Semantics preserved.
  Verified by `jlsp_testng_class_level_test PASS`.

- **Verification:** `make -f Makefile.cbm cbm` green; focused gate
  `perl_lsp java_lsp extraction pipeline registry` = **887 passed / 10 skipped
  (pre-existing tracked skips) / 0 failed**. Merge committed and pushed to
  `origin/main`.
