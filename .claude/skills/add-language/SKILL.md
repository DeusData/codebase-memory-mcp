---
name: add-language
description: Guide through adding or fixing language support (tree-sitter extraction + pipeline passes)
disable-model-invocation: true
---

# Adding Language Support

Language support has two layers. Determine which type of language you're adding:

## Standard Languages (need tree-sitter grammar)

1. **Add grammar** — Vendor the tree-sitter grammar into `internal/cbm/grammar_<lang>.c` using `scripts/vendor-grammar.sh`
2. **Configure node types** — Add language entry in `internal/cbm/lang_specs.c` with AST node types for functions, classes, calls, imports
3. **Write extractor** — Create `internal/cbm/extract_<lang>.c` for language-specific extraction logic
4. **Add enum** — Add `CBM_LANG_<LANG>` to `internal/cbm/cbm.h`
5. **Hook into pipeline** — Update `src/pipeline/pipeline.c` for call resolution, usage tracking
6. **Add tests**:
   - `tests/test_extraction.c` — AST extraction regression tests
   - `tests/test_pipeline.c` — Integration-level pipeline tests

## Infrastructure Languages (Dockerfile, K8s, etc. — no new grammar needed)

Follow the **infra-pass pattern**:

1. **Detection helper** — Add `cbm_is_<lang>_file()` in `src/pipeline/pass_infrascan.c`
2. **Enum value** — Add `CBM_LANG_<LANG>` in `internal/cbm/cbm.h` and row in `lang_specs.c`
3. **Custom extractor** — Write extractor returning `CBMFileResult*` (reuse YAML grammar if applicable)
4. **Pipeline pass** — Register in `pipeline.c`
5. **Tests** — Follow `TEST(infra_is_dockerfile)` and `TEST(k8s_extract_manifest)` patterns in `tests/test_pipeline.c`

## Verification

```bash
scripts/test.sh          # Full test suite
scripts/lint.sh          # Must pass all linters
```

Test against a real open-source repo that uses the language.
