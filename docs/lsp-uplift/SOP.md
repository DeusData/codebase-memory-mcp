# LSP Uplift Harness SOP（可持续迭代规程）

Standing operating procedure for raising per-language Hybrid LSP / extraction capability. Designed to be re-entered by any future session; state lives in `PLAN.md` (adjudicated scope) + git history.

## Roles & artifacts

- **PLAN.md** — adjudicated proposal set.每个条目含：文件锚点、scope、两位对拍 reviewer 的 binding 修正、test plan、wave 分配。Implementation MUST fold in the binding corrections.
- **对拍 (adversarial duel)** — any new scope or hard blocker gets 2+ independent reviewer agents (feasibility-skeptic vs depth-completeness) before code is written. One analyst proposal + dual review + adjudication; disagreements resolved by evidence (file:line), not seniority.
- **Waves** — small consensus items first (S), then M with corrections, then cross-file/L. Every wave ends pushed to `origin feat/lang-lsp-uplift`.

## Per-batch loop (one language, 1-3 items)

1. **Re-read scope**: PLAN.md entry + binding corrections; open every anchor file:line and verify the claim still holds (code moves).
2. **TDD**: add failing tests to `tests/test_<lang>_lsp.c` (or extraction/grammar tests) using the entry's test plan; fixtures inline via `cbm_extract_file` like existing tests.
3. **Implement** in `internal/cbm/lsp/<lang>_lsp.c` / `internal/cbm/grammar_<lang>.c` / pipeline files per anchors. Follow constraints:
   - Pure C11, `-Wall -Wextra -Werror`, ASan/UBSan-clean, no external processes/runtimes.
   - Perf is sacred: O(n) passes, interned strings, arena allocators, neg-memo where repeated misses possible; walk-depth caps; zero-edge guarantee on unresolved.
   - Cross-platform (macOS/Linux/Windows) — no platform-only APIs without guards.
4. **Focused verify**: `make -f Makefile.cbm test-focused TEST_SUITES="<suite> <related-suites>"` (suite names registered in `tests/test_main.c`). Fix until green.
5. **Adjacent-blast check**: grep for shared files touched (`helpers.c`, `service_patterns.c`, `lang_specs.c`, `registry.c`, pipeline passes) → run the suites of every language that consumes them.
6. **Full gate**: `make -f Makefile.cbm test-par -j$(nproc)` before push (batch several commits if runtime is long, but never push a red tree).
7. **Commit** (one item or one coherent batch per commit; message notes proposal id) → **push**.
8. **Update PLAN.md** status inline (`✅ done <commit>` on the entry heading) so the next session resumes precisely.

## Blocker protocol

- Any surprise (architecture mismatch, grammar missing nodes, perf regression, flaky test): STOP coding, spawn 2 independent reviewer agents on the specific question (对拍), adjudicate with evidence, record the ruling in PLAN.md, then continue.
- Never weaken an existing test to pass; repro first (`tests/repro/` pattern exists for known bugs).

## Environment facts (verified 2026-09-04)

- Toolchains: perl 5.38.0 ✓, rustc/cargo 1.97 ✓, python3 3.10 ✓, javac 11 ✓; go ✗, python2 ✗ (install if ground-truth needed; extraction itself never shells out).
- Build: `Makefile.cbm`; BUILD_DIR shared — do not run two makes concurrently.
- Suites of record: `perl_lsp`, `go_lsp`, `py_lsp` (+bench/scale/stress), `java_lsp` (+coverage), `rust_lsp`, plus `extraction`, `grammar_*`, `lang_contract`, `parse_coverage`, `matrix_*`.

## Wave exit criteria

- All wave items ✅ with tests, full `test-par` green, pushed.
- Retro line appended to PLAN.md top: what shipped, edge-count/coverage deltas if measurable (`test_parse_coverage`, matrix tests), lessons.
- Next wave re-scoped if retro invalidates assumptions (sustainable-iteration clause).
