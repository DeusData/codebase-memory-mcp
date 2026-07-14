/*
 * repro_new_py_tuple_unpack.c -- Reproduce-first case for a NEW, un-filed
 * bug discovered during QA sweep (2026-06-26).
 *
 * BUG: Python module-level tuple-unpacking assignments silently produce no
 * Variable definitions.  `x, y = some_func()` is in py_var_types
 * (as "assignment") but the Python branch of extract_vars_mainstream()
 * only emits a def when the `left` child is a plain `identifier`.  When
 * `left` is a `pattern_list` (the tree-sitter node type for comma-separated
 * LHS in an assignment), the guard fails silently and zero Variable defs
 * are emitted for x or y.
 *
 * PATTERN AFFECTED:
 *   x, y = some_func()          # left is pattern_list
 *   a, b, c = 1, 2, 3           # left is pattern_list
 *   result, err = parse(data)   # common Go-style unpack in Python
 *
 * ROOT CAUSE -- extract_defs.c, extract_vars_mainstream(), Python case
 * (~line 4068):
 *
 *   case CBM_LANG_PYTHON: {
 *       TSNode left = ts_node_child_by_field_name(node, TS_FIELD("left"));
 *       if (!ts_node_is_null(left) && strcmp(ts_node_type(left), "identifier") == 0) {
 *           push_var_def(ctx, cbm_node_text(a, left, ctx->source), node);
 *       }
 *       break;
 *   }
 *
 *   The guard `strcmp(ts_node_type(left), "identifier") == 0` passes only
 *   for single-variable assignments (`x = 1`).  For `x, y = func()` the
 *   tree-sitter-python grammar produces `left` as a `pattern_list` node
 *   containing two `identifier` children.  The strcmp fails -> no
 *   push_var_def is called -> both `x` and `y` are silently dropped.
 *
 *   py_var_types (lang_specs.c) includes both "assignment" AND
 *   "augmented_assignment", so the walk_variables path DOES reach
 *   extract_vars_mainstream for these nodes -- the gap is purely inside
 *   the Python case guard.
 *
 * EXPECTED (correct) behavior:
 *   `x, y = some_func()` at module level must produce AT LEAST one
 *   Variable def; ideally one for `x` and one for `y`.
 *   `result, err = parse(data)` must produce Variable defs for `result`
 *   and `err`.
 *
 * ACTUAL (buggy) behavior:
 *   r->defs contains zero Variable defs for these assignments.
 *   ASSERT_GT(count, 0) fires -> RED.
 *
 * HOW TO CONFIRM WITHOUT COMPILING:
 *   1. lang_specs.c: py_var_types = {"assignment", "augmented_assignment", NULL}
 *      -> walk_variables correctly calls extract_var_names for "assignment" nodes.
 *   2. extract_defs.c extract_vars_mainstream() Python case (~4068):
 *      left node for `x, y = ...` is of type "pattern_list" (confirmed by
 *      tree-sitter-python grammar symbol sym_pattern_list = 200).
 *   3. The strcmp("pattern_list", "identifier") == 0 check FAILS -> no def.
 *
 * FIX LOCATION (not implemented here):
 *   extract_defs.c extract_vars_mainstream() Python case: when left is
 *   "pattern_list", iterate its named children and call push_var_def for
 *   each child that is an "identifier".
 */

#include "test_framework.h"
#include "cbm.h"

#include <string.h>

static CBMFileResult *rx_py(const char *src) {
    return cbm_extract_file(src, (int)strlen(src), CBM_LANG_PYTHON, "proj", "mod.py",
                            0, NULL, NULL);
}

static int count_var_defs(CBMFileResult *r) {
    int n = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (r->defs.items[i].label && strcmp(r->defs.items[i].label, "Variable") == 0)
            n++;
    }
    return n;
}

static int has_var_def(CBMFileResult *r, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (d->label && strcmp(d->label, "Variable") == 0 &&
            d->name && strcmp(d->name, name) == 0)
            return 1;
    }
    return 0;
}

/*
 * repro_new_py_tuple_unpack_two_vars
 *
 * `x, y = some_func()` must produce at least one Variable def.
 *
 * Precondition: single-var assignment `z = 1` must work (tests the
 * happy path so we know Variable extraction is wired up at all).
 *
 * WHY RED on current code:
 *   extract_vars_mainstream() Python case checks
 *   strcmp(ts_node_type(left), "identifier") == 0.
 *   For `x, y = some_func()` the left node is "pattern_list" -> check
 *   fails -> push_var_def is never called -> count_var_defs returns 0
 *   for the tuple assignment -> ASSERT_GT(count, 0) fires -> RED.
 */
TEST(repro_new_py_tuple_unpack_two_vars) {
    static const char *src =
        "def some_func():\n"
        "    return 1, 2\n"
        "\n"
        "z = 1\n"
        "x, y = some_func()\n";

    CBMFileResult *r = rx_py(src);
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    /* Precondition: single-var `z = 1` must yield a Variable def for z.
     * If RED here, the Variable extraction path itself is broken, not the
     * tuple-unpack case specifically. */
    ASSERT_TRUE(has_var_def(r, "z")); /* should already pass */

    /* PRIMARY ASSERTION: at least one Variable def must come from `x, y = ...`.
     * Because we already confirmed `z` works, any Variable count > 1 means
     * the tuple-unpack path is working.
     * WHY RED: the pattern_list branch is missing; push_var_def is never called
     * for x or y -> total count stays at 1 (only z) -> ASSERT_GT(count, 1)
     * fails -> RED. */
    int total = count_var_defs(r);
    ASSERT_GT(total, 1); /* RED on buggy code: count == 1 (only z) */

    cbm_free_result(r);
    PASS();
}

/*
 * repro_new_py_tuple_unpack_named_vars
 *
 * Stronger assertion: x and y must each appear as named Variable defs.
 *
 * WHY RED on current code:
 *   has_var_def(r, "x") and has_var_def(r, "y") both return 0 since
 *   push_var_def is never called for pattern_list assignments.
 */
TEST(repro_new_py_tuple_unpack_named_vars) {
    static const char *src =
        "def parse(data):\n"
        "    return data, None\n"
        "\n"
        "result, err = parse('hello')\n";

    CBMFileResult *r = rx_py(src);
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    /* PRIMARY ASSERTION: both unpacked names must appear as Variable defs.
     * WHY RED: pattern_list is not handled; neither "result" nor "err" is
     * emitted -> has_var_def returns 0 for both -> at least one ASSERT_TRUE
     * fires -> RED. */
    ASSERT_TRUE(has_var_def(r, "result")); /* RED on buggy code */
    ASSERT_TRUE(has_var_def(r, "err"));    /* RED on buggy code */

    cbm_free_result(r);
    PASS();
}

/* ---- Suite --------------------------------------------------------------- */
SUITE(repro_new_py_tuple_unpack) {
    RUN_TEST(repro_new_py_tuple_unpack_two_vars);
    RUN_TEST(repro_new_py_tuple_unpack_named_vars);
}
