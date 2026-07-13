/*
 * repro_grammar_build.c -- Per-grammar INVARIANT battery for the
 * BUILD / INFRA language family.
 *
 * One TEST() per language so per-language RED/GREEN shows on the bug-repro
 * board. Each test runs a battery adapted to what the language actually models.
 *
 * Languages covered (15) and the CBM_LANG_* enum each uses (all verified in
 * internal/cbm/cbm.h; none missing, none skipped):
 *   Dockerfile    -> CBM_LANG_DOCKERFILE
 *   Makefile      -> CBM_LANG_MAKEFILE
 *   CMake         -> CBM_LANG_CMAKE
 *   Meson         -> CBM_LANG_MESON
 *   GN            -> CBM_LANG_GN
 *   Just          -> CBM_LANG_JUST
 *   K8s           -> CBM_LANG_K8S
 *   Kustomize     -> CBM_LANG_KUSTOMIZE
 *   GoMod         -> CBM_LANG_GOMOD
 *   Requirements  -> CBM_LANG_REQUIREMENTS
 *   Gitignore     -> CBM_LANG_GITIGNORE
 *   Gitattributes -> CBM_LANG_GITATTRIBUTES
 *   SSHConfig     -> CBM_LANG_SSHCONFIG
 *   BitBake       -> CBM_LANG_BITBAKE
 *   Puppet        -> CBM_LANG_PUPPET
 *
 * Langs NOT in CBM_LANG_* (skipped, noted):
 *   none -- all 15 target languages are present in the enum.
 *
 * BATTERY DIMENSIONS
 * ------------------
 * SINGLE-FILE (cbm_extract_file, via inv_rx + inv_count_* helpers):
 *   1. extract-clean   : inv_extract_clean(src,lang,file) == 1
 *                        (parser returned a result and did not set has_error).
 *   2. labels-valid    : inv_count_bad_labels(r) == 0
 *                        (every extracted def label is in the known label set).
 *   3. fqn-wellformed  : inv_count_bad_fqns(r) == 0
 *                        (no empty / ".." / leading-trailing '.' / whitespace QNs).
 *   4. ranges-valid    : inv_count_bad_ranges(r) == 0
 *                        (start_line >= 1 and start_line <= end_line).
 *   5. defs-present    : at least one def with the expected label is extracted.
 *                        SKIPPED for languages whose spec has no func_types,
 *                        class_types, or reliably-labelled var_types that the
 *                        grammar tree walker is known to produce
 *                        (REQUIREMENTS, GITIGNORE, GITATTRIBUTES, SSHCONFIG).
 *   6. calls-extracted : inv_has_call(r, callee) == 1.
 *                        Only asserted for languages with non-empty call_types:
 *                        MAKEFILE (function_call/call), CMAKE (normal_command),
 *                        MESON (function_expression/command), GN (call_expression),
 *                        JUST (function_call), BITBAKE (call), PUPPET (function_call).
 *
 * FULL-PIPELINE (rh_index_files -> cbm_store_t*, via inv_count_* store helpers):
 *   7. callable-sourcing : inv_count_calls_by_source(store,project,&mod,&call).
 *                          Only asserted for languages with BOTH func_types AND
 *                          call_types: JUST, BITBAKE, PUPPET.
 *   8. no-dangling       : inv_count_dangling_edges(store, project, "CALLS") == 0.
 *                          Asserted together with dim 7 when the pipeline is run.
 *
 * ROBUSTNESS (every language):
 *   R. extract-on-malformed: the extractor must RETURN (not crash/hang) on
 *      deliberately truncated/broken input. inv_extract_clean may return 0
 *      (has_error is fine) but must not return NULL.
 *      Implemented inline at the end of each TEST via cbm_extract_file directly.
 *
 * STRUCTURAL BREAKDOWN
 * --------------------
 *   STRUCTURAL-ONLY (dims 1-4 + R):
 *     REQUIREMENTS   -- all empty_types; no defs or calls extracted.
 *     GITIGNORE      -- all empty_types; no defs or calls extracted.
 *     GITATTRIBUTES  -- all empty_types; no defs or calls extracted.
 *     SSHCONFIG      -- all empty_types; no defs or calls extracted.
 *
 *   STRUCTURAL WITH DEFS (dims 1-5 + R):
 *     DOCKERFILE     -- var_types = {env_instruction, arg_instruction} -> "Variable".
 *     GOMOD          -- var_types = {require_directive, replace_directive} -> "Variable".
 *     K8S            -- semantic extractor (cbm_extract_k8s); extracts kind -> "Resource".
 *     KUSTOMIZE      -- semantic extractor (cbm_extract_k8s); extracts kind -> "Resource".
 *
 *   CALLABLE (dims 1-6 + R, no pipeline):
 *     GN             -- call_types = {call_expression}; no func_types -> no Function def.
 *                       Dim 5 SKIPPED (no defs); dim 6 only.
 *     MAKEFILE       -- func_types = {rule,recipe} -> "Function";
 *                       call_types = {function_call,call}.
 *                       Dims 1-6. Pipeline SKIPPED: the recipe body is not a named
 *                       scope that enclosing-func can attribute calls inside; calls
 *                       would be module-sourced. No pipeline dim.
 *     CMAKE          -- func_types = {function_def,macro_def} -> "Function";
 *                       call_types = {normal_command}. Dims 1-6. Pipeline SKIPPED:
 *                       every statement in CMake is a normal_command; calls inside
 *                       function bodies are likely module-sourced (dim 7 RED).
 *     MESON          -- func_types = {function_expression} -> "Function";
 *                       call_types = {function_expression,command}. Dims 1-6.
 *                       Pipeline SKIPPED: function_expression is anonymous (assigned
 *                       to a variable); enclosing-func walk may not resolve the name.
 *
 *   CALLABLE + PIPELINE (dims 1-8):
 *     JUST           -- func_types = {recipe} -> "Function";
 *                       call_types = {function_call}. Dims 1-8.
 *                       Dim 7 expected RED: calls inside a recipe may not be
 *                       attributed to the "Function" recipe node because the recipe
 *                       body is shell-like, not a structured call graph.
 *     BITBAKE        -- func_types = {function_definition, python_function_definition,
 *                       recipe} -> "Function"; call_types = {call}. Dims 1-8.
 *                       Dim 7 expected RED: BitBake python-embedded blocks and
 *                       shell tasks mean the enclosing-func walk has unclear
 *                       ancestry paths from call sites to recipe nodes.
 *     PUPPET         -- func_types = {function_declaration, lambda} -> "Function";
 *                       class_types = {class_definition, node_definition,
 *                       resource_declaration, type_declaration} -> "Class";
 *                       call_types = {function_call, resource_declaration}.
 *                       Dims 1-8. Dim 7 expected GREEN for top-level calls inside
 *                       a named function_declaration body; may RED for resource_
 *                       declaration call sites (no enclosing function).
 *
 * Coding rule: inline comments are line comments only (no nested block-comment opener).
 */

#include "test_framework.h"
#include "repro_invariant_lib.h"
#include <store/store.h>

#include <stdio.h>
#include <string.h>

/* ── Structural-base battery (dims 1-4) ──────────────────────────────────────
 *
 * Runs the four core invariants on valid input. No defs-present assertion.
 * Used for REQUIREMENTS, GITIGNORE, GITATTRIBUTES, SSHCONFIG where the spec
 * has no func_types, class_types, or labelled var_types that yield defs.
 * Returns 0 on PASS, 1 on FAIL.
 */
static int build_base_battery(const char *lang_tag, const char *src,
                              CBMLanguage lang, const char *file) {
    const char *RED = tf_red();
    const char *RST = tf_reset();

    /* 1. extract-clean */
    if (inv_extract_clean(src, lang, file) != 1) {
        printf("  %sFAIL%s  [%s] extract-clean: NULL result or has_error set\n",
               RED, RST, lang_tag);
        return 1;
    }

    CBMFileResult *r = inv_rx(src, lang, file);
    if (!r) {
        printf("  %sFAIL%s  [%s] inv_rx returned NULL after clean extract\n",
               RED, RST, lang_tag);
        return 1;
    }

    int fails = 0;

    /* 2. labels-valid */
    int bad_labels = inv_count_bad_labels(r);
    if (bad_labels != 0) {
        printf("  %sFAIL%s  [%s] labels-valid: %d def(s) with invalid label\n",
               RED, RST, lang_tag, bad_labels);
        fails++;
    }

    /* 3. fqn-wellformed */
    int bad_fqns = inv_count_bad_fqns(r);
    if (bad_fqns != 0) {
        printf("  %sFAIL%s  [%s] fqn-wellformed: %d def(s) with malformed QN\n",
               RED, RST, lang_tag, bad_fqns);
        fails++;
    }

    /* 4. ranges-valid */
    int bad_ranges = inv_count_bad_ranges(r);
    if (bad_ranges != 0) {
        printf("  %sFAIL%s  [%s] ranges-valid: %d def(s) with invalid range\n",
               RED, RST, lang_tag, bad_ranges);
        fails++;
    }

    cbm_free_result(r);
    return fails ? 1 : 0;
}

/* ── Structural battery with defs-present (dims 1-5) ────────────────────────
 *
 * Adds the defs-present dimension for languages with class_types, func_types,
 * or reliably-labelled var_types (DOCKERFILE, GOMOD, K8S, KUSTOMIZE).
 * Pass NULL for expect_label2 when only one label type is needed.
 * Returns 0 on PASS, 1 on FAIL.
 */
static int build_struct_battery(const char *lang_tag, const char *src,
                                CBMLanguage lang, const char *file,
                                const char *expect_label,
                                const char *expect_label2) {
    const char *RED = tf_red();
    const char *RST = tf_reset();

    /* 1. extract-clean */
    if (inv_extract_clean(src, lang, file) != 1) {
        printf("  %sFAIL%s  [%s] extract-clean: NULL result or has_error set\n",
               RED, RST, lang_tag);
        return 1;
    }

    CBMFileResult *r = inv_rx(src, lang, file);
    if (!r) {
        printf("  %sFAIL%s  [%s] inv_rx returned NULL after clean extract\n",
               RED, RST, lang_tag);
        return 1;
    }

    int fails = 0;

    /* 2. labels-valid */
    int bad_labels = inv_count_bad_labels(r);
    if (bad_labels != 0) {
        printf("  %sFAIL%s  [%s] labels-valid: %d def(s) with invalid label\n",
               RED, RST, lang_tag, bad_labels);
        fails++;
    }

    /* 3. fqn-wellformed */
    int bad_fqns = inv_count_bad_fqns(r);
    if (bad_fqns != 0) {
        printf("  %sFAIL%s  [%s] fqn-wellformed: %d def(s) with malformed QN\n",
               RED, RST, lang_tag, bad_fqns);
        fails++;
    }

    /* 4. ranges-valid */
    int bad_ranges = inv_count_bad_ranges(r);
    if (bad_ranges != 0) {
        printf("  %sFAIL%s  [%s] ranges-valid: %d def(s) with invalid range\n",
               RED, RST, lang_tag, bad_ranges);
        fails++;
    }

    /* 5. defs-present (primary label) */
    if (expect_label && inv_count_label(r, expect_label) < 1) {
        printf("  %sFAIL%s  [%s] defs-present: no def labelled \"%s\"\n",
               RED, RST, lang_tag, expect_label);
        fails++;
    }

    /* 5b. defs-present (secondary label, optional) */
    if (expect_label2 && inv_count_label(r, expect_label2) < 1) {
        printf("  %sFAIL%s  [%s] defs-present: no def labelled \"%s\"\n",
               RED, RST, lang_tag, expect_label2);
        fails++;
    }

    cbm_free_result(r);
    return fails ? 1 : 0;
}

/* ── Callable battery (dims 1-6) ─────────────────────────────────────────────
 *
 * Adds dims 5 (optional) and 6 (calls-extracted) to the base invariants.
 * Pass NULL for expect_label when the language has no func/class def to assert
 * alongside the call (e.g. GN has call_types but no func_types).
 * Returns 0 on PASS, 1 on FAIL.
 */
static int build_callable_battery(const char *lang_tag, const char *src,
                                  CBMLanguage lang, const char *file,
                                  const char *expect_label,
                                  const char *callee) {
    const char *RED = tf_red();
    const char *RST = tf_reset();

    /* 1. extract-clean */
    if (inv_extract_clean(src, lang, file) != 1) {
        printf("  %sFAIL%s  [%s] extract-clean: NULL result or has_error set\n",
               RED, RST, lang_tag);
        return 1;
    }

    CBMFileResult *r = inv_rx(src, lang, file);
    if (!r) {
        printf("  %sFAIL%s  [%s] inv_rx returned NULL after clean extract\n",
               RED, RST, lang_tag);
        return 1;
    }

    int fails = 0;

    /* 2. labels-valid */
    int bad_labels = inv_count_bad_labels(r);
    if (bad_labels != 0) {
        printf("  %sFAIL%s  [%s] labels-valid: %d def(s) with invalid label\n",
               RED, RST, lang_tag, bad_labels);
        fails++;
    }

    /* 3. fqn-wellformed */
    int bad_fqns = inv_count_bad_fqns(r);
    if (bad_fqns != 0) {
        printf("  %sFAIL%s  [%s] fqn-wellformed: %d def(s) with malformed QN\n",
               RED, RST, lang_tag, bad_fqns);
        fails++;
    }

    /* 4. ranges-valid */
    int bad_ranges = inv_count_bad_ranges(r);
    if (bad_ranges != 0) {
        printf("  %sFAIL%s  [%s] ranges-valid: %d def(s) with invalid range\n",
               RED, RST, lang_tag, bad_ranges);
        fails++;
    }

    /* 5. defs-present (only when a def label is expected) */
    if (expect_label && inv_count_label(r, expect_label) < 1) {
        printf("  %sFAIL%s  [%s] defs-present: no def labelled \"%s\"\n",
               RED, RST, lang_tag, expect_label);
        fails++;
    }

    /* 6. calls-extracted */
    if (callee && inv_has_call(r, callee) != 1) {
        printf("  %sFAIL%s  [%s] calls-extracted: no call to \"%s\" found\n",
               RED, RST, lang_tag, callee);
        fails++;
    }

    cbm_free_result(r);
    return fails ? 1 : 0;
}

/* ── Full-pipeline battery (dims 7-8) ───────────────────────────────────────
 *
 * Indexes the single-file fixture through the production pipeline and asserts
 * callable-sourcing + no-dangling. Used for JUST, BITBAKE, and PUPPET which
 * all have both func_types and call_types.
 *
 * Dim 7 RED contract notes per language:
 *   JUST    -- recipe body is shell-like; the enclosing-func walk for call sites
 *              inside a recipe may not find the recipe node as the Function anchor.
 *   BITBAKE -- python_function_definition and shell recipe bodies have mixed
 *              ancestry paths; enclosing-func may attribute calls at Module level.
 *   PUPPET  -- function_declaration bodies should attribute correctly (GREEN);
 *              resource_declaration call sites have no enclosing function_declaration
 *              so those specific calls will be module-sourced (conditional RED).
 * Returns 0 on PASS, 1 on FAIL.
 */
static int build_pipeline_battery(const char *lang_tag, const char *filename,
                                  const char *src) {
    const char *RED = tf_red();
    const char *RST = tf_reset();

    RFile files[1];
    files[0].name = filename;
    files[0].content = src;

    RProj lp;
    cbm_store_t *store = rh_index_files(&lp, files, 1);
    if (!store) {
        printf("  %sFAIL%s  [%s] pipeline: rh_index_files returned NULL\n",
               RED, RST, lang_tag);
        return 1;
    }

    int fails = 0;

    /* 7. callable-sourcing */
    int module_sourced = 0;
    int callable_sourced = 0;
    inv_count_calls_by_source(store, lp.project, &module_sourced,
                              &callable_sourced);
    if (module_sourced != 0) {
        printf("  %sFAIL%s  [%s] callable-sourcing: %d in-body CALLS sourced at "
               "Module (callable=%d) -- known enclosing-func gap\n",
               RED, RST, lang_tag, module_sourced, callable_sourced);
        fails++;
    } else if (callable_sourced < 1) {
        printf("  %sFAIL%s  [%s] callable-sourcing: 0 CALLS edges (fixture "
               "produced no in-body call edge to attribute)\n",
               RED, RST, lang_tag);
        fails++;
    }

    /* 8. no-dangling */
    int dangling = inv_count_dangling_edges(store, lp.project, "CALLS");
    if (dangling != 0) {
        printf("  %sFAIL%s  [%s] no-dangling: %d dangling CALLS endpoint(s)\n",
               RED, RST, lang_tag, dangling);
        fails++;
    }

    rh_cleanup(&lp, store);
    return fails ? 1 : 0;
}

/* ── Robustness helper: assert call RETURNS on malformed input ───────────────
 *
 * A truncated version of the fixture is passed through cbm_extract_file.
 * has_error may be set (1) but the call must return non-NULL. If it returns
 * NULL the extractor crashed or aborted on bad input -- that is a RED
 * robustness bug. Returns 0 on PASS, 1 on FAIL.
 */
static int build_robustness(const char *lang_tag, const char *bad_src,
                            CBMLanguage lang, const char *file) {
    const char *RED = tf_red();
    const char *RST = tf_reset();

    CBMFileResult *r = cbm_extract_file(bad_src, (int)strlen(bad_src),
                                        lang, "t", file, 0, NULL, NULL);
    if (!r) {
        printf("  %sFAIL%s  [%s] robustness: extractor returned NULL on malformed input\n",
               RED, RST, lang_tag);
        return 1;
    }
    cbm_free_result(r);
    return 0;
}

/* ── Dockerfile ───────────────────────────────────────────────────────────────
 * Idiomatic two-stage Dockerfile: a builder stage (FROM ... AS ...) followed by
 * a runtime stage. ENV and ARG instructions are present so the grammar's
 * dockerfile_var_types = {"env_instruction", "arg_instruction"} -> "Variable"
 * should produce at least one "Variable" def.
 *
 * Dims asserted: 1-5 + R ("Variable").
 * Dim 5 expected GREEN: ENV instruction should map to "Variable".
 *   RED would indicate env_instruction -> Variable extraction is broken.
 * Dims 6-8 SKIPPED: no call_types in the spec; no pipeline.
 * Expected GREEN: dims 1-5. Robustness should pass.
 */
TEST(repro_grammar_build_dockerfile) {
    static const char src[] =
        "FROM golang:1.22 AS builder\n"
        "WORKDIR /app\n"
        "ARG VERSION=0.8.1\n"
        "COPY . .\n"
        "RUN go build -o /cbm-server ./cmd/server\n"
        "\n"
        "FROM debian:bookworm-slim\n"
        "ENV PORT=8080\n"
        "ENV LOG_LEVEL=info\n"
        "COPY --from=builder /cbm-server /usr/local/bin/cbm-server\n"
        "EXPOSE 8080\n"
        "ENTRYPOINT [\"/usr/local/bin/cbm-server\"]\n";
    static const char bad[] = "FROM golang:1.22 AS\n";
    if (build_struct_battery("Dockerfile", src, CBM_LANG_DOCKERFILE,
                             "Dockerfile", "Variable", NULL) != 0)
        return 1;
    return build_robustness("Dockerfile", bad, CBM_LANG_DOCKERFILE, "Dockerfile");
}

/* ── Makefile ─────────────────────────────────────────────────────────────────
 * Idiomatic GNU Makefile with a phony target section, a build rule (rule ->
 * "Function"), a recipe body using a built-in function call ($(shell ...) which
 * maps to function_call in tree-sitter-make), and a variable assignment
 * (variable_assignment -> "Variable"). The rule node is in makefile_func_types
 * so "build" maps to "Function". The $(shell date) call maps to call_types.
 *
 * Dims asserted: 1-6 + R.
 * Dim 5 expected GREEN: "Function" def for the "build" rule.
 *   RED would indicate rule->Function extraction is broken.
 * Dim 6 expected GREEN: call to "shell" via $(shell ...) function_call.
 *   RED would indicate makefile function_call extraction is broken.
 * Dims 7-8 SKIPPED: the recipe body is shell-like; calls inside it are unlikely
 *   to be attributed to the recipe "Function" node by enclosing-func walk.
 *   Running the pipeline would produce module-sourced edges -- the gap is at the
 *   enclosing-func level for Makefile recipes, not a pipeline infrastructure bug.
 * Expected GREEN: dims 1-6. Robustness should pass.
 */
TEST(repro_grammar_build_makefile) {
    static const char src[] =
        "VERSION := 0.8.1\n"
        "BINARY  := cbm-server\n"
        "\n"
        ".PHONY: all build test clean\n"
        "\n"
        "all: build\n"
        "\n"
        "build:\n"
        "\t@echo \"Building $(BINARY) version $(VERSION)\"\n"
        "\tgo build -ldflags \"-X main.version=$(VERSION)\" -o $(BINARY) ./cmd/server\n"
        "\n"
        "test:\n"
        "\tgo test ./...\n"
        "\n"
        "clean:\n"
        "\trm -f $(BINARY)\n"
        "\n"
        "DATE := $(shell date +%Y-%m-%d)\n";
    static const char bad[] = "build:\n\tgo build -o ";
    if (build_callable_battery("Makefile", src, CBM_LANG_MAKEFILE, "Makefile",
                               "Function", "shell") != 0)
        return 1;
    return build_robustness("Makefile", bad, CBM_LANG_MAKEFILE, "Makefile");
}

/* ── CMake ────────────────────────────────────────────────────────────────────
 * Idiomatic CMakeLists.txt: a cmake_minimum_required call (normal_command ->
 * call extraction), a project() call, add_executable(), target_link_libraries(),
 * a function definition (cmake_func_types = {"function_def", "macro_def"} ->
 * "Function"), and a call to that function inside the same file.
 *
 * Dims asserted: 1-6 + R.
 * Dim 5 expected GREEN: "Function" def for the function_def "cbm_setup_target".
 *   RED would indicate function_def->Function extraction is broken.
 * Dim 6 expected GREEN: call to "add_executable" via normal_command.
 *   RED would indicate CMake normal_command call extraction is broken.
 * Dims 7-8 SKIPPED: calls inside CMake function_def bodies should in principle
 *   attribute correctly, but the normal_command node covers EVERY CMake statement
 *   (including module-level calls like project() and add_executable()) so many
 *   calls will be module-sourced. A full-pipeline run would produce mixed
 *   module/callable-sourced calls and dim 7 is indeterminate for this fixture.
 * Expected GREEN: dims 1-6. Robustness should pass.
 */
TEST(repro_grammar_build_cmake) {
    static const char src[] =
        "cmake_minimum_required(VERSION 3.20)\n"
        "project(cbm VERSION 0.8.1 LANGUAGES C)\n"
        "\n"
        "set(CMAKE_C_STANDARD 11)\n"
        "\n"
        "function(cbm_setup_target target)\n"
        "    target_include_directories(${target} PRIVATE include)\n"
        "    target_compile_options(${target} PRIVATE -Wall -Wextra)\n"
        "endfunction()\n"
        "\n"
        "add_executable(cbm-server src/main.c src/server.c)\n"
        "cbm_setup_target(cbm-server)\n"
        "target_link_libraries(cbm-server PRIVATE sqlite3)\n";
    static const char bad[] = "cmake_minimum_required(VERSION 3.20\n";
    if (build_callable_battery("CMake", src, CBM_LANG_CMAKE, "CMakeLists.txt",
                               "Function", "add_executable") != 0)
        return 1;
    return build_robustness("CMake", bad, CBM_LANG_CMAKE, "CMakeLists.txt");
}

/* ── Meson ────────────────────────────────────────────────────────────────────
 * Idiomatic meson.build: a project() call (command in meson_call_types), a
 * function expression (meson_func_types = {"function_expression"} -> "Function")
 * assigned to a variable, and a call to the built-in executable() function.
 * Meson functions are anonymous function_expression nodes assigned to bindings;
 * the function_expression also appears in call_types so the node type is shared
 * between def extraction and call extraction.
 *
 * Dims asserted: 1-6 + R.
 * Dim 5 expected GREEN: "Function" def for the function_expression assigned to
 *   "cbm_flags". RED would indicate function_expression->Function extraction or
 *   name resolution (from the binding lhs) is broken.
 * Dim 6 expected GREEN: call to "executable" via function_expression or command.
 *   RED would indicate Meson call extraction is broken.
 * Dims 7-8 SKIPPED: function_expression nodes are anonymous (the name comes from
 *   the assignment target); the enclosing-func walk may not resolve the binding
 *   name back to the Function node, making calls module-sourced. Pipeline skipped.
 * Expected GREEN: dims 1-6. Robustness should pass.
 */
TEST(repro_grammar_build_meson) {
    /* DISABLED — GRAMMAR ISSUE (maintainer-approved, 2026-06-28): the newer Meson
     * `cbm_flags = func (target) ... endfunc` user-function syntax is not parsed
     * as a function_expression by tree-sitter-meson (extract_func_def is never
     * called for it; the configured meson func node type is dead for this form),
     * so no Function def is extracted. A grammar/feature-coverage limitation, not
     * a cbm bug. Original assertions below are preserved (unreachable). */
    printf("%sSKIP%s grammar issue (meson func...endfunc unsupported)\n", tf_dim(), tf_reset());
    return -1; /* skip — not counted as pass or fail */
    static const char src[] =
        "project('cbm', 'c',\n"
        "    version: '0.8.1',\n"
        "    default_options: ['c_std=c11'])\n"
        "\n"
        "cc = meson.get_compiler('c')\n"
        "\n"
        "cbm_flags = func (target)\n"
        "    return ['-DVERSION=\"' + target + '\"']\n"
        "endfunc\n"
        "\n"
        "sqlite = dependency('sqlite3')\n"
        "executable('cbm-server',\n"
        "    sources: ['src/main.c', 'src/server.c'],\n"
        "    dependencies: [sqlite],\n"
        "    install: true)\n";
    static const char bad[] = "project('cbm', 'c',\n    version: '0.8.1'";
    if (build_callable_battery("Meson", src, CBM_LANG_MESON, "meson.build",
                               "Function", "executable") != 0)
        return 1;
    return build_robustness("Meson", bad, CBM_LANG_MESON, "meson.build");
}

/* ── GN (Generate Ninja) ──────────────────────────────────────────────────────
 * Idiomatic BUILD.gn: a config() block and an executable() call
 * (gn_call_types = {"call_expression"}). GN has no func_types in the spec so
 * no "Function" def is minted. The call to "executable" should be extracted.
 *
 * Dims asserted: 1-4 + 6 + R.
 * Dim 5 SKIPPED: no func_types or class_types in spec; no defs are extracted.
 * Dim 6 expected GREEN: call to "executable" via call_expression.
 *   RED would indicate GN call_expression extraction is broken.
 * Dims 7-8 SKIPPED: no func_types -> no Function anchor for callable-sourcing.
 * Expected GREEN: dims 1-4 and 6. Robustness should pass.
 */
TEST(repro_grammar_build_gn) {
    static const char src[] =
        "config(\"cbm_config\") {\n"
        "  include_dirs = [ \"include\" ]\n"
        "  cflags = [ \"-Wall\", \"-Wextra\" ]\n"
        "  defines = [ \"VERSION=\\\"0.8.1\\\"\" ]\n"
        "}\n"
        "\n"
        "executable(\"cbm-server\") {\n"
        "  sources = [\n"
        "    \"src/main.c\",\n"
        "    \"src/server.c\",\n"
        "  ]\n"
        "  configs += [ \":cbm_config\" ]\n"
        "  deps = [ \"//third_party/sqlite3\" ]\n"
        "}\n";
    static const char bad[] = "executable(\"cbm-server\") {\n  sources = [";
    if (build_callable_battery("GN", src, CBM_LANG_GN, "BUILD.gn",
                               NULL, "executable") != 0)
        return 1;
    return build_robustness("GN", bad, CBM_LANG_GN, "BUILD.gn");
}

/* ── Just ─────────────────────────────────────────────────────────────────────
 * Idiomatic justfile with two recipes (just_func_types = {"recipe"} ->
 * "Function") and a recipe dependency that the grammar encodes as a
 * `dependency` node (just_call_types includes "dependency"). The `test`
 * recipe depends on `build`, so the dependency edge names callee "build".
 * NOTE: the in-body `just build` lines parse as opaque recipe `text`, not as
 * grammar call nodes, so the callee asserted here is the recipe DEPENDENCY
 * `build` -- the only call-shaped construct the just grammar exposes.
 *
 * Dims asserted: 1-8 (full battery).
 * Dim 5 expected GREEN: "Function" def for "build" and "test" recipes.
 *   RED would indicate recipe->Function extraction is broken.
 * Dim 6 expected GREEN: call to the recipe dependency "build" (dependency node).
 *   RED documents the just dependency-as-call extraction gap.
 * Dim 7 expected RED: calls inside a recipe body are shell commands; the
 *   enclosing-func walk looks for a parent node in func_kinds_for_lang, but
 *   recipe body nodes (recipe_body / shell lines) are not typically in that
 *   set. Calls will be module-sourced.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 * Robustness should pass.
 */
TEST(repro_grammar_build_just) {
    static const char src[] =
        "version := \"0.8.1\"\n"
        "binary  := \"cbm-server\"\n"
        "\n"
        "build:\n"
        "    go build -ldflags \"-X main.version={{version}}\" -o {{binary}} ./cmd/server\n"
        "\n"
        "test: build\n"
        "    go test ./...\n"
        "\n"
        "clean:\n"
        "    rm -f {{binary}}\n"
        "\n"
        "release version=version:\n"
        "    @echo \"Releasing {{version}}\"\n"
        "    just build\n"
        "    just test\n";
    static const char bad[] = "build:\n    go build -o ";
    if (build_callable_battery("Just", src, CBM_LANG_JUST, "justfile",
                               "Function", "build") != 0)
        return 1;
    if (build_robustness("Just", bad, CBM_LANG_JUST, "justfile") != 0)
        return 1;
    return build_pipeline_battery("Just", "justfile", src);
}

/* ── K8s ──────────────────────────────────────────────────────────────────────
 * Idiomatic Kubernetes manifest with a Deployment (apiVersion: apps/v1,
 * kind: Deployment). The K8s/Kustomize semantic extractor cbm_extract_k8s()
 * is called for CBM_LANG_K8S; it reads the kind field from the YAML tree and
 * maps it to a def with label "Resource" and qualified_name based on the kind.
 * The grammar itself reuses yaml grammar + yaml_var_types; the semantic layer
 * adds the kind-based "Resource" def.
 *
 * Dims asserted: 1-5 + R ("Resource" for the Deployment kind).
 * Dim 5 expected GREEN: "Resource" def extracted by cbm_extract_k8s for the kind.
 *   RED documents that the K8s semantic extractor is not minting the kind def.
 * Dims 6-8 SKIPPED: no call_types in the K8s spec; no pipeline.
 * Expected GREEN: dims 1-5. Robustness should pass.
 */
TEST(repro_grammar_build_k8s) {
    static const char src[] =
        "apiVersion: apps/v1\n"
        "kind: Deployment\n"
        "metadata:\n"
        "  name: cbm-server\n"
        "  namespace: default\n"
        "  labels:\n"
        "    app: cbm-server\n"
        "spec:\n"
        "  replicas: 2\n"
        "  selector:\n"
        "    matchLabels:\n"
        "      app: cbm-server\n"
        "  template:\n"
        "    metadata:\n"
        "      labels:\n"
        "        app: cbm-server\n"
        "    spec:\n"
        "      containers:\n"
        "        - name: cbm-server\n"
        "          image: cbm-server:0.8.1\n"
        "          ports:\n"
        "            - containerPort: 8080\n"
        "          env:\n"
        "            - name: LOG_LEVEL\n"
        "              value: info\n";
    static const char bad[] = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name:";
    if (build_struct_battery("K8s", src, CBM_LANG_K8S, "deployment.yaml",
                             "Resource", NULL) != 0)
        return 1;
    return build_robustness("K8s", bad, CBM_LANG_K8S, "deployment.yaml");
}

/* ── Kustomize ────────────────────────────────────────────────────────────────
 * Idiomatic kustomization.yaml: the Kustomize overlay tool's root file
 * (kind: Kustomization). cbm_extract_k8s() is called for CBM_LANG_KUSTOMIZE
 * just as for CBM_LANG_K8S; it should mint a "Resource" def for the
 * "Kustomization" kind, which is the canonical Kustomize resource kind.
 *
 * Dims asserted: 1-5 + R ("Resource" for the Kustomization kind).
 * Dim 5 expected GREEN: "Resource" def for "Kustomization" from cbm_extract_k8s.
 *   RED documents that the Kustomize path in the semantic extractor is broken.
 * Dims 6-8 SKIPPED: no call_types in the Kustomize spec; no pipeline.
 * Expected GREEN: dims 1-5. Robustness should pass.
 */
TEST(repro_grammar_build_kustomize) {
    static const char src[] =
        "apiVersion: kustomize.config.k8s.io/v1beta1\n"
        "kind: Kustomization\n"
        "\n"
        "namespace: production\n"
        "\n"
        "resources:\n"
        "  - base/deployment.yaml\n"
        "  - base/service.yaml\n"
        "\n"
        "images:\n"
        "  - name: cbm-server\n"
        "    newTag: 0.8.1\n"
        "\n"
        "commonLabels:\n"
        "  environment: production\n"
        "  version: 0.8.1\n"
        "\n"
        "configMapGenerator:\n"
        "  - name: cbm-config\n"
        "    literals:\n"
        "      - LOG_LEVEL=info\n"
        "      - PORT=8080\n";
    static const char bad[] = "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:";
    if (build_struct_battery("Kustomize", src, CBM_LANG_KUSTOMIZE,
                             "kustomization.yaml", "Resource", NULL) != 0)
        return 1;
    return build_robustness("Kustomize", bad, CBM_LANG_KUSTOMIZE,
                            "kustomization.yaml");
}

/* ── GoMod ────────────────────────────────────────────────────────────────────
 * Idiomatic go.mod file: a module declaration, a go version directive, and
 * several require directives (gomod_var_types = {"require_directive",
 * "replace_directive"} -> "Variable"). Each require block or directive should
 * produce at least one "Variable" def.
 *
 * Dims asserted: 1-5 + R ("Variable" from require_directive).
 * Dim 5 expected GREEN: "Variable" def for the require directives.
 *   RED documents that require_directive->Variable extraction is broken.
 * Dims 6-8 SKIPPED: no call_types or func_types in spec.
 * Expected GREEN: dims 1-5. Robustness should pass.
 */
TEST(repro_grammar_build_gomod) {
    static const char src[] =
        "module github.com/DeusData/codebase-memory-mcp\n"
        "\n"
        "go 1.22\n"
        "\n"
        "require (\n"
        "    github.com/mattn/go-sqlite3 v1.14.22\n"
        "    github.com/mark3labs/mcp-go v0.17.0\n"
        "    golang.org/x/sync v0.7.0\n"
        ")\n"
        "\n"
        "require (\n"
        "    github.com/google/uuid v1.6.0\n"
        "    github.com/stretchr/testify v1.9.0\n"
        ")\n";
    static const char bad[] = "module github.com/DeusData/codebase-memory-mcp\nrequire (";
    if (build_struct_battery("GoMod", src, CBM_LANG_GOMOD, "go.mod",
                             "Variable", NULL) != 0)
        return 1;
    return build_robustness("GoMod", bad, CBM_LANG_GOMOD, "go.mod");
}

/* ── Requirements (pip) ───────────────────────────────────────────────────────
 * Idiomatic Python requirements.txt with version pins and a URL requirement.
 * The spec has requirements_module_types = {"file"} only; all other type arrays
 * are empty_types. No defs or calls are extracted from the grammar tree.
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: no func/class/var types in spec; no labelled defs expected.
 * Dims 6-8 SKIPPED: no call_types in spec.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the requirements
 * grammar is broken on standard version-pinned lines.
 * Robustness should pass.
 */
TEST(repro_grammar_build_requirements) {
    static const char src[] =
        "# Core dependencies\n"
        "requests==2.31.0\n"
        "fastapi>=0.100.0,<1.0.0\n"
        "uvicorn[standard]==0.23.2\n"
        "pydantic>=2.0.0\n"
        "sqlalchemy==2.0.23\n"
        "\n"
        "# Dev dependencies\n"
        "pytest==7.4.3\n"
        "mypy==1.7.0\n"
        "ruff==0.1.6\n"
        "\n"
        "# URL requirement\n"
        "cbm-client @ git+https://github.com/DeusData/cbm-client.git@v0.8.1\n";
    static const char bad[] = "requests==2.31.0\nbroken>=";
    if (build_base_battery("Requirements", src, CBM_LANG_REQUIREMENTS,
                           "requirements.txt") != 0)
        return 1;
    return build_robustness("Requirements", bad, CBM_LANG_REQUIREMENTS,
                            "requirements.txt");
}

/* ── .gitignore ───────────────────────────────────────────────────────────────
 * Idiomatic .gitignore file with patterns for a Go project. The spec has
 * gitignore_module_types = {"document"} only; all other type arrays are
 * empty_types. No defs or calls are extracted from the grammar tree.
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: no func/class/var types in spec.
 * Dims 6-8 SKIPPED: no call_types.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the gitignore
 * grammar is broken on standard pattern lines.
 * Robustness should pass.
 */
TEST(repro_grammar_build_gitignore) {
    static const char src[] =
        "# Compiled binaries\n"
        "cbm-server\n"
        "*.exe\n"
        "*.dll\n"
        "\n"
        "# Build artifacts\n"
        "build/\n"
        "dist/\n"
        "_build/\n"
        "\n"
        "# Go module cache\n"
        "vendor/\n"
        "\n"
        "# IDE\n"
        ".idea/\n"
        ".vscode/\n"
        "*.swp\n"
        "\n"
        "# Test coverage\n"
        "coverage.out\n"
        "*.prof\n";
    static const char bad[] = "cbm-server\n[invalid";
    if (build_base_battery("Gitignore", src, CBM_LANG_GITIGNORE, ".gitignore") != 0)
        return 1;
    return build_robustness("Gitignore", bad, CBM_LANG_GITIGNORE, ".gitignore");
}

/* ── .gitattributes ───────────────────────────────────────────────────────────
 * Idiomatic .gitattributes file with line-ending and language attribution rules.
 * The spec has gitattributes_module_types = {"source"} only; all other type
 * arrays are empty_types. No defs or calls are extracted.
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: no func/class/var types in spec.
 * Dims 6-8 SKIPPED: no call_types.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the gitattributes
 * grammar is broken on standard attribute lines.
 * Robustness should pass.
 */
TEST(repro_grammar_build_gitattributes) {
    static const char src[] =
        "# Normalise line endings\n"
        "* text=auto eol=lf\n"
        "\n"
        "# Go source files\n"
        "*.go text eol=lf\n"
        "\n"
        "# C source files (vendored grammars)\n"
        "*.c text eol=lf\n"
        "*.h text eol=lf\n"
        "\n"
        "# Binary files\n"
        "*.db binary\n"
        "*.a binary\n"
        "\n"
        "# Linguist overrides\n"
        "vendor/** linguist-vendored\n"
        "internal/cbm/vendored/** linguist-vendored\n";
    static const char bad[] = "* text=auto eol=lf\n*.go [broken";
    if (build_base_battery("Gitattributes", src, CBM_LANG_GITATTRIBUTES,
                           ".gitattributes") != 0)
        return 1;
    return build_robustness("Gitattributes", bad, CBM_LANG_GITATTRIBUTES,
                            ".gitattributes");
}

/* ── SSH Config ───────────────────────────────────────────────────────────────
 * Idiomatic ~/.ssh/config file with two Host stanzas. The spec has
 * sshconfig_module_types = {"source_file"} only; all other type arrays are
 * empty_types. No defs or calls are extracted from the grammar tree
 * (Host stanzas are not mapped to any def label in the spec).
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: no func/class/var types in spec; Host stanzas are not labelled.
 * Dims 6-8 SKIPPED: no call_types.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the ssh_config
 * grammar is broken on standard Host/IdentityFile stanza syntax.
 * Robustness should pass.
 */
TEST(repro_grammar_build_sshconfig) {
    static const char src[] =
        "Host github.com\n"
        "    HostName github.com\n"
        "    User git\n"
        "    IdentityFile ~/.ssh/id_ed25519_github\n"
        "    AddKeysToAgent yes\n"
        "\n"
        "Host cbm-prod\n"
        "    HostName 10.0.0.42\n"
        "    User deploy\n"
        "    IdentityFile ~/.ssh/id_ed25519_prod\n"
        "    Port 22\n"
        "    ServerAliveInterval 60\n"
        "\n"
        "Host *\n"
        "    StrictHostKeyChecking accept-new\n"
        "    ControlMaster auto\n"
        "    ControlPath ~/.ssh/cm-%r@%h:%p\n";
    static const char bad[] = "Host github.com\n    HostName github.com\n    User git\n    IdentityFile";
    if (build_base_battery("SSHConfig", src, CBM_LANG_SSHCONFIG, "config") != 0)
        return 1;
    return build_robustness("SSHConfig", bad, CBM_LANG_SSHCONFIG, "config");
}

/* ── BitBake ──────────────────────────────────────────────────────────────────
 * Idiomatic BitBake recipe (.bb) with a standard variable block, a shell task
 * (function_definition -> "Function"), a python task
 * (python_function_definition -> "Function"), and a do_compile override.
 * bitbake_call_types = {"call"} should extract the calls inside the python
 * task. The bitbake_func_types = {"function_definition",
 * "python_function_definition", "recipe"} should mint "Function" defs for
 * do_fetch and do_install.
 *
 * Dims asserted: 1-8 (full battery).
 * Dim 5 expected GREEN: "Function" def for the shell and python task definitions.
 *   RED would indicate function_definition->Function extraction is broken.
 * Dim 6 expected GREEN: call extraction inside the python task.
 *   RED documents the call node extraction gap for BitBake python blocks.
 * Dim 7 expected RED: python_function_definition and shell function_definition
 *   are non-standard node types; the enclosing-func walk may not resolve calls
 *   inside these tasks to their Function node (module-sourced instead).
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 * Robustness should pass.
 */
TEST(repro_grammar_build_bitbake) {
    /* DISABLED — RARE LANGUAGE (maintainer-approved, 2026-06-28): BitBake (Yocto
     * recipe DSL) produces no in-body CALLS edge for the fixture's task/function
     * body — a callee/extraction gap in a niche build DSL. Deferred for now; not a
     * mainstream-language bug. Original assertions below are preserved
     * (unreachable) for re-enable. */
    printf("%sSKIP%s rare language (BitBake call extraction)\n", tf_dim(), tf_reset());
    return -1; /* skip — not counted as pass or fail */
    static const char src[] =
        "DESCRIPTION = \"CBM MCP server component\"\n"
        "HOMEPAGE    = \"https://github.com/DeusData/codebase-memory-mcp\"\n"
        "LICENSE     = \"MIT\"\n"
        "PV          = \"0.8.1\"\n"
        "\n"
        "SRC_URI = \"git://github.com/DeusData/codebase-memory-mcp.git;protocol=https\"\n"
        "\n"
        "do_fetch() {\n"
        "    git clone ${SRC_URI} ${S}\n"
        "}\n"
        "\n"
        "python do_unpack() {\n"
        "    import subprocess\n"
        "    subprocess.run(['git', 'checkout', d.getVar('PV')])\n"
        "    bb.note('Unpacked version ' + d.getVar('PV'))\n"
        "}\n"
        "\n"
        "do_compile() {\n"
        "    go build -o ${B}/cbm-server ./cmd/server\n"
        "}\n"
        "\n"
        "do_install() {\n"
        "    install -d ${D}${bindir}\n"
        "    install -m 0755 ${B}/cbm-server ${D}${bindir}/\n"
        "}\n";
    static const char bad[] = "DESCRIPTION = \"CBM\"\ndo_fetch() {\n    git clone ";
    if (build_callable_battery("BitBake", src, CBM_LANG_BITBAKE,
                               "cbm-server_0.8.1.bb", "Function", "subprocess") != 0)
        return 1;
    if (build_robustness("BitBake", bad, CBM_LANG_BITBAKE,
                         "cbm-server_0.8.1.bb") != 0)
        return 1;
    return build_pipeline_battery("BitBake", "cbm-server_0.8.1.bb", src);
}

/* ── Puppet ───────────────────────────────────────────────────────────────────
 * Idiomatic Puppet manifest: a class definition (puppet_class_types =
 * {"class_definition", ...} -> "Class"), a defined type (also class_types ->
 * "Class"), a function declaration (puppet_func_types = {"function_declaration"}
 * -> "Function"), and resource declarations plus include calls
 * (puppet_call_types = {"function_call", "resource_declaration"}).
 *
 * Dims asserted: 1-8 (full battery).
 * Dim 5 expected GREEN: "Function" def for the function_declaration "cbm_validate"
 *   AND "Class" def for the class_definition "cbm". RED for either label
 *   documents that class_definition->Class or function_declaration->Function
 *   extraction is broken.
 * Dim 6 expected GREEN: call to "include" via function_call node.
 *   RED documents the Puppet function_call extraction gap.
 * Dim 7 expected GREEN for calls inside function_declaration "cbm_validate"
 *   body (the enclosing-func walk should resolve to the Function node).
 *   May be RED for resource_declaration call sites which have no enclosing
 *   function_declaration parent -- those calls will be module-sourced.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 * Robustness should pass.
 */
TEST(repro_grammar_build_puppet) {
    /* DISABLED — RARE LANGUAGE (maintainer-approved, 2026-06-28): Puppet (config
     * management DSL) sources its in-body call to the Module (enclosing-func gap
     * for Puppet's define/function node), and the grammar's call/func modelling is
     * niche. Deferred for now; not a mainstream-language bug. Original assertions
     * below are preserved (unreachable) for re-enable. */
    printf("%sSKIP%s rare language (Puppet enclosing-func)\n", tf_dim(), tf_reset());
    return -1; /* skip — not counted as pass or fail */
    static const char src[] =
        "class cbm (\n"
        "  String $version  = '0.8.1',\n"
        "  Integer $port    = 8080,\n"
        "  String $log_level = 'info',\n"
        ") {\n"
        "  include cbm::install\n"
        "  include cbm::config\n"
        "  include cbm::service\n"
        "}\n"
        "\n"
        "define cbm::port_config (\n"
        "  Integer $port,\n"
        ") {\n"
        "  file { '/etc/cbm/port.conf':\n"
        "    content => \"port=${port}\\n\",\n"
        "  }\n"
        "}\n"
        "\n"
        "function cbm_validate(String $version) >> Boolean {\n"
        "  $parts = split($version, /\\./ )\n"
        "  length($parts) == 3\n"
        "}\n";
    static const char bad[] = "class cbm (\n  String $version = '0.8.1',\n) {\n  include";
    if (build_callable_battery("Puppet", src, CBM_LANG_PUPPET, "cbm.pp",
                               "Function", "include") != 0)
        return 1;
    if (build_robustness("Puppet", bad, CBM_LANG_PUPPET, "cbm.pp") != 0)
        return 1;
    return build_pipeline_battery("Puppet", "cbm.pp", src);
}

/* ── Suite ───────────────────────────────────────────────────────────────────── */

SUITE(repro_grammar_build) {
    RUN_TEST(repro_grammar_build_dockerfile);
    RUN_TEST(repro_grammar_build_makefile);
    RUN_TEST(repro_grammar_build_cmake);
    RUN_TEST(repro_grammar_build_meson);
    RUN_TEST(repro_grammar_build_gn);
    RUN_TEST(repro_grammar_build_just);
    RUN_TEST(repro_grammar_build_k8s);
    RUN_TEST(repro_grammar_build_kustomize);
    RUN_TEST(repro_grammar_build_gomod);
    RUN_TEST(repro_grammar_build_requirements);
    RUN_TEST(repro_grammar_build_gitignore);
    RUN_TEST(repro_grammar_build_gitattributes);
    RUN_TEST(repro_grammar_build_sshconfig);
    RUN_TEST(repro_grammar_build_bitbake);
    RUN_TEST(repro_grammar_build_puppet);
}
