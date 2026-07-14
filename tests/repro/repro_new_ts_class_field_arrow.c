/*
 * repro_new_ts_class_field_arrow.c -- Reproduce-first case for a NEW, un-filed
 * bug discovered during QA sweep (2026-06-26).
 *
 * BUG: TypeScript class field arrow functions are silently dropped from
 * the Method definition list AND calls inside them receive the wrong
 * enclosing_func_qn (the class QN instead of the method QN).
 *
 * PATTERN AFFECTED:
 *   class Foo {
 *       handleClick = () => {
 *           helper();
 *       };
 *   }
 *
 * This is an extremely common React/TypeScript pattern for event handlers.
 *
 * ROOT CAUSE -- TWO co-located defects:
 *
 * DEFECT A -- extract_defs.c, extract_class_methods() (~line 3578):
 *   The function iterates the class body's direct children.  For each child it
 *   checks:
 *     cbm_kind_in_set(method_node, spec->function_node_types)
 *   "public_field_definition" is NOT in ts_func_types -- only
 *   "function_declaration", "arrow_function", "method_definition", etc. are.
 *   So the body-scan loop hits `continue` and the method is never emitted.
 *
 *   The parallel path (extract_func_def, called from walk_defs when the DFS
 *   visits the inner "arrow_function" node) also fails: it calls
 *   resolve_toplevel_arrow_name() which only handles the `variable_declarator`
 *   and `pair` parent cases -- NOT `public_field_definition`.  So it returns
 *   NULL and extract_func_def() returns early with no def emitted.
 *
 * DEFECT B -- extract_unified.c, push_boundary_scopes() / compute_func_qn():
 *   When the DFS cursor visits the `arrow_function` node inside
 *   `public_field_definition`, it IS in ts_func_types so push_boundary_scopes
 *   calls compute_func_qn().  compute_func_qn() calls resolve_func_name_node()
 *   which only handles the `variable_declarator` parent -- NOT
 *   `public_field_definition`.  So name_node is NULL -> compute_func_qn
 *   returns NULL -> no SCOPE_FUNC is pushed for this arrow function.
 *
 *   Consequence: any call inside the arrow function body runs handle_calls()
 *   with state->enclosing_func_qn still set to state->enclosing_class_qn
 *   (the class "proj.ts.Foo"), NOT the method "proj.ts.Foo.handleClick".
 *
 * EXPECTED (correct) behavior:
 *   A. cbm_extract_file must emit a Method def with name="handleClick"
 *      and qualified_name containing both "Foo" and "handleClick".
 *   B. The call to helper() inside handleClick must have
 *      enclosing_func_qn pointing to the handleClick method, NOT just
 *      the class "Foo".  Specifically enclosing_func_qn must contain
 *      "handleClick" and must NOT equal the module QN.
 *
 * ACTUAL (buggy) behavior:
 *   A. r->defs contains no Method entry for "handleClick" -- the def is
 *      silently dropped.  ASSERT_NOT_NULL(method_def) fires -> RED.
 *   B. The helper() call has enclosing_func_qn == class QN ("proj.ts.Foo"),
 *      not the method QN.  ASSERT_NOT_NULL(strstr(enc, "handleClick")) fires
 *      -> RED.
 *
 * HOW TO CONFIRM THE BUG WITHOUT COMPILING:
 *   1. extract_class_methods (extract_defs.c ~3578): iterates body children;
 *      line ~3620 guards on cbm_kind_in_set(method_node, spec->function_node_types);
 *      "public_field_definition" is absent from ts_func_types (lang_specs.c ~237)
 *      -> guard fails -> no Method emitted.
 *   2. resolve_toplevel_arrow_name (extract_defs.c ~598): only handles
 *      variable_declarator and pair parents -- not public_field_definition.
 *   3. resolve_func_name_node (extract_unified.c ~91): same gap for
 *      push_boundary_scopes scope tracking.
 *
 * FIX LOCATION (not implemented here):
 *   extract_defs.c extract_class_methods: add a peek-through for
 *   "public_field_definition" (similar to the decorated_definition peek),
 *   extract the inner arrow_function's name from the field's "name" child,
 *   and call push_method_def.
 *   extract_unified.c resolve_func_name_node: add a "public_field_definition"
 *   / "field_definition" parent case (similar to the variable_declarator case)
 *   so compute_func_qn can push a SCOPE_FUNC for the arrow function.
 */

#include "test_framework.h"
#include "cbm.h"

#include <string.h>

static CBMFileResult *rx_ts(const char *src) {
    return cbm_extract_file(src, (int)strlen(src), CBM_LANG_TYPESCRIPT, "proj", "ts.ts",
                            0, NULL, NULL);
}

static CBMDefinition *find_def_by_name(CBMFileResult *r, const char *label, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (label && (!d->label || strcmp(d->label, label) != 0))
            continue;
        if (name && (!d->name || strcmp(d->name, name) != 0))
            continue;
        return d;
    }
    return NULL;
}

/*
 * repro_new_ts_class_field_arrow_method_def_dropped
 *
 * DEFECT A: the "handleClick" Method def is not emitted at all.
 *
 * WHY RED on current code:
 *   extract_class_methods skips public_field_definition (not in ts_func_types);
 *   resolve_toplevel_arrow_name only handles variable_declarator/pair parents.
 *   find_def_by_name returns NULL -> ASSERT_NOT_NULL fires.
 */
TEST(repro_new_ts_class_field_arrow_method_def_dropped) {
    static const char *src =
        "function helper(): void {}\n"
        "\n"
        "class Foo {\n"
        "    handleClick = () => {\n"
        "        helper();\n"
        "    };\n"
        "}\n";

    CBMFileResult *r = rx_ts(src);
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    /* Precondition: the class Foo itself must be extracted. */
    CBMDefinition *cls = find_def_by_name(r, "Class", "Foo");
    ASSERT_NOT_NULL(cls);

    /* Precondition: the free helper() function must be extracted. */
    CBMDefinition *helper = find_def_by_name(r, "Function", "helper");
    ASSERT_NOT_NULL(helper);

    /* DEFECT A PRIMARY ASSERTION: the arrow-function class field must
     * be emitted as a Method def under the class.
     * WHY RED: extract_class_methods bails out at the cbm_kind_in_set check
     * (public_field_definition is not in ts_func_types) without ever calling
     * push_method_def; and the walk_defs path fails in resolve_toplevel_arrow_name
     * (parent is public_field_definition, not variable_declarator). */
    CBMDefinition *method = find_def_by_name(r, "Method", "handleClick");
    ASSERT_NOT_NULL(method); /* RED on buggy code */

    /* Sanity: the emitted Method must be scoped to its class. */
    ASSERT_NOT_NULL(method->qualified_name);
    ASSERT_TRUE(strstr(method->qualified_name, "Foo") != NULL);
    ASSERT_TRUE(strstr(method->qualified_name, "handleClick") != NULL);

    cbm_free_result(r);
    PASS();
}

/*
 * repro_new_ts_class_field_arrow_call_enclosing_qn
 *
 * DEFECT B: calls inside the arrow-function body receive enclosing_func_qn
 * equal to the CLASS qn, not the METHOD qn.
 *
 * WHY RED on current code:
 *   resolve_func_name_node (extract_unified.c) only handles variable_declarator
 *   arrow parents.  For public_field_definition it returns NULL, so compute_func_qn
 *   returns NULL and no SCOPE_FUNC is pushed.  The enclosing scope remains the
 *   class scope ("proj.ts.Foo"), so state->enclosing_func_qn == class_qn.
 *   The assertion that enclosing_func_qn contains "handleClick" then FAILS -> RED.
 */
TEST(repro_new_ts_class_field_arrow_call_enclosing_qn) {
    static const char *src =
        "function helper(): void {}\n"
        "\n"
        "class Foo {\n"
        "    handleClick = () => {\n"
        "        helper();\n"
        "    };\n"
        "}\n";

    CBMFileResult *r = rx_ts(src);
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    /* Find the call to helper() inside handleClick. */
    const char *enc = NULL;
    for (int i = 0; i < r->calls.count; i++) {
        if (strcmp(r->calls.items[i].callee_name, "helper") == 0) {
            enc = r->calls.items[i].enclosing_func_qn;
            break;
        }
    }

    /* The helper() call must be found at all. */
    ASSERT_NOT_NULL(enc);

    /* DEFECT B PRIMARY ASSERTION: enclosing_func_qn must point to the
     * handleClick arrow function, NOT just to the class.
     * WHY RED: push_boundary_scopes never pushes a SCOPE_FUNC for the
     * arrow function (compute_func_qn returns NULL for public_field_definition
     * parents), so the scope stays at the class level -> enc is "proj.ts.Foo"
     * which does not contain "handleClick" -> ASSERT_TRUE fires -> RED. */
    ASSERT_TRUE(strstr(enc, "handleClick") != NULL); /* RED on buggy code */

    cbm_free_result(r);
    PASS();
}

/* ---- Suite --------------------------------------------------------------- */
SUITE(repro_new_ts_class_field_arrow) {
    RUN_TEST(repro_new_ts_class_field_arrow_method_def_dropped);
    RUN_TEST(repro_new_ts_class_field_arrow_call_enclosing_qn);
}
