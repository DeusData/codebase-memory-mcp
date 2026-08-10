/*
 * repro_lexical_binding_precision.c — exact lexical-shadowing contracts.
 *
 * These tests inspect raw CBMUsage rows rather than graph edges.  A local
 * binding must block semantic promotion only for the identifier occurrence
 * whose language scope actually contains that binding.  The exact byte span
 * keeps a declaration, label, or same-spelled occurrence from satisfying the
 * assertion accidentally.
 */
#include "test_framework.h"
#include "cbm.h"

#include <stdio.h>
#include <stdint.h>
#include <string.h>

typedef struct {
    int count;
    int value_count;
    int candidate_count;
    int blocked_count;
    int local_shadow_count;
} LBBindingSite;

static int lb_qn_ends_with(const char *qn, const char *suffix) {
    if (!qn || !suffix) {
        return 0;
    }
    size_t qn_len = strlen(qn);
    size_t suffix_len = strlen(suffix);
    return qn_len >= suffix_len && strcmp(qn + qn_len - suffix_len, suffix) == 0;
}

static uint32_t lb_identifier_offset(const char *source, const char *marker,
                                     const char *identifier) {
    const char *occurrence = strstr(source, marker);
    if (!occurrence) {
        return UINT32_MAX;
    }
    const char *site = strstr(occurrence, identifier);
    if (!site || site >= occurrence + strlen(marker)) {
        return UINT32_MAX;
    }
    return (uint32_t)(site - source);
}

static LBBindingSite lb_binding_site(const CBMFileResult *result, const char *caller_suffix,
                                     const char *identifier, uint32_t start) {
    LBBindingSite match = {0};
    uint32_t end = start + (uint32_t)strlen(identifier);
    for (int i = 0; result && i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        if (!usage->ref_name || strcmp(usage->ref_name, identifier) != 0 ||
            (caller_suffix && !lb_qn_ends_with(usage->enclosing_func_qn, caller_suffix)) ||
            usage->site_start_byte != start || usage->site_end_byte != end) {
            continue;
        }
        match.count++;
        match.value_count += usage->kind == CBM_USAGE_VALUE;
        match.candidate_count += usage->may_be_call_reference;
        match.blocked_count += usage->semantic_reference_blocked;
        match.local_shadow_count += usage->semantic_reference_local_shadow;
    }
    return match;
}

static CBMFileResult *lb_extract(const char *source, CBMLanguage language, const char *filename) {
    return cbm_extract_file(source, (int)strlen(source), language, "repro", filename, 0, NULL,
                            NULL);
}

/* A Python keyword label names the callee's parameter.  It does not introduce
 * a local named `handler` in caller, so the later global function value remains
 * eligible for exact semantic resolution. */
TEST(repro_python_keyword_label_does_not_bind_local) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def sink(**kwargs):\n"
                                 "    pass\n"
                                 "def caller():\n"
                                 "    sink(handler=1)\n"
                                 "    accept(handler)\n"
                                 "def parameter_control(handler):\n"
                                 "    accept_control(handler)\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    uint32_t control_site = lb_identifier_offset(source, "accept_control(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    ASSERT_NEQ(control_site, UINT32_MAX);

    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "keyword_label.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "handler", site);
    LBBindingSite control = lb_binding_site(result, "parameter_control", "handler", control_site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.value_count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(control.count, 1);
    ASSERT_EQ(control.blocked_count, 1);
    ASSERT_EQ(control.local_shadow_count, 1);
    ASSERT_EQ(usage.blocked_count, 0);
    ASSERT_EQ(usage.local_shadow_count, 0);
    PASS();
}

/* The declaration identifier belongs to the enclosing scope.  Once the
 * function body is active, visiting that identifier must not make the function
 * shadow itself. */
TEST(repro_python_function_name_does_not_shadow_itself) {
    static const char source[] = "def accept(callback):\n"
                                 "    pass\n"
                                 "def pass_self():\n"
                                 "    accept(pass_self)\n"
                                 "def parameter_control(pass_self):\n"
                                 "    accept_control(pass_self)\n";

    uint32_t site = lb_identifier_offset(source, "accept(pass_self)", "pass_self");
    uint32_t control_site = lb_identifier_offset(source, "accept_control(pass_self)", "pass_self");
    ASSERT_NEQ(site, UINT32_MAX);
    ASSERT_NEQ(control_site, UINT32_MAX);

    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "self_reference.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "pass_self", "pass_self", site);
    LBBindingSite control = lb_binding_site(result, "parameter_control", "pass_self", control_site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.value_count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(control.count, 1);
    ASSERT_EQ(control.blocked_count, 1);
    ASSERT_EQ(control.local_shadow_count, 1);
    ASSERT_EQ(usage.blocked_count, 0);
    ASSERT_EQ(usage.local_shadow_count, 0);
    PASS();
}

/* A nested Python def creates a local in its enclosing function.  Recording
 * the declaration name only under the nested function's own QN lets a later
 * use in outer fall through to an unrelated global callable. */
TEST(repro_python_nested_function_binds_in_enclosing_function) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def outer():\n"
                                 "    def handler():\n"
                                 "        pass\n"
                                 "    accept(handler)\n"
                                 "def parameter_control(handler):\n"
                                 "    accept_control(handler)\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    uint32_t control_site = lb_identifier_offset(source, "accept_control(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    ASSERT_NEQ(control_site, UINT32_MAX);

    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "nested_definition.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "outer", "handler", site);
    LBBindingSite control = lb_binding_site(result, "parameter_control", "handler", control_site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.value_count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(control.count, 1);
    ASSERT_EQ(control.blocked_count, 1);
    ASSERT_EQ(control.local_shadow_count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 1);
    PASS();
}

/* Python determines locals for the whole function at compile time.  A read
 * before the assignment is still a local (and would raise UnboundLocalError at
 * runtime); it must not resolve to the same-named module function. */
TEST(repro_python_assignment_blocks_earlier_use_for_whole_function) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept_before(callback):\n"
                                 "    pass\n"
                                 "def accept_after(callback):\n"
                                 "    pass\n"
                                 "def caller():\n"
                                 "    accept_before(handler)\n"
                                 "    handler = 1\n"
                                 "    accept_after(handler)\n";

    uint32_t before_site = lb_identifier_offset(source, "accept_before(handler)", "handler");
    uint32_t after_site = lb_identifier_offset(source, "accept_after(handler)", "handler");
    ASSERT_NEQ(before_site, UINT32_MAX);
    ASSERT_NEQ(after_site, UINT32_MAX);

    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "whole_function_local.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite before = lb_binding_site(result, "caller", "handler", before_site);
    LBBindingSite after = lb_binding_site(result, "caller", "handler", after_site);
    cbm_free_result(result);

    ASSERT_EQ(before.count, 1);
    ASSERT_EQ(before.value_count, 1);
    ASSERT_EQ(before.candidate_count, 1);
    ASSERT_EQ(after.count, 1);
    ASSERT_EQ(after.blocked_count, 1);
    ASSERT_EQ(after.local_shadow_count, 1);
    ASSERT_EQ(before.blocked_count, 1);
    ASSERT_EQ(before.local_shadow_count, 1);
    PASS();
}

/* JavaScript `let` is block-scoped.  The in-block argument is a local, while
 * the later argument after the block again denotes the module function. */
TEST(repro_javascript_block_local_stops_at_block_exit) {
    static const char source[] = "function handler() {}\n"
                                 "function acceptInside(callback) {}\n"
                                 "function acceptOutside(callback) {}\n"
                                 "function caller(flag) {\n"
                                 "  if (flag) {\n"
                                 "    let handler = 1;\n"
                                 "    acceptInside(handler);\n"
                                 "  }\n"
                                 "  acceptOutside(handler);\n"
                                 "}\n";

    uint32_t inside_site = lb_identifier_offset(source, "acceptInside(handler)", "handler");
    uint32_t outside_site = lb_identifier_offset(source, "acceptOutside(handler)", "handler");
    ASSERT_NEQ(inside_site, UINT32_MAX);
    ASSERT_NEQ(outside_site, UINT32_MAX);

    CBMFileResult *result = lb_extract(source, CBM_LANG_JAVASCRIPT, "block_scope.js");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite inside = lb_binding_site(result, "caller", "handler", inside_site);
    LBBindingSite outside = lb_binding_site(result, "caller", "handler", outside_site);
    cbm_free_result(result);

    ASSERT_EQ(inside.count, 1);
    ASSERT_EQ(inside.value_count, 1);
    ASSERT_EQ(inside.blocked_count, 1);
    ASSERT_EQ(inside.local_shadow_count, 1);
    ASSERT_EQ(outside.count, 1);
    ASSERT_EQ(outside.value_count, 1);
    ASSERT_EQ(outside.candidate_count, 1);
    ASSERT_EQ(outside.blocked_count, 0);
    ASSERT_EQ(outside.local_shadow_count, 0);
    PASS();
}

/* Overloads currently share a textual function QN.  Parameter bindings are
 * body instances, not QN-wide facts: the first overload's `handler` parameter
 * must not shadow the method group used by the second overload. */
TEST(repro_csharp_overload_bodies_do_not_share_parameter_bindings) {
    static const char source[] =
        "using System;\n"
        "class Sample {\n"
        "  static void handler() {}\n"
        "  static void acceptFirst(Action callback) {}\n"
        "  static void acceptSecond(Action callback) {}\n"
        "  static void overloaded(Action handler) { acceptFirst(handler); }\n"
        "  static void overloaded(int count) { acceptSecond(handler); }\n"
        "}\n";

    uint32_t first_site = lb_identifier_offset(source, "acceptFirst(handler)", "handler");
    uint32_t second_site = lb_identifier_offset(source, "acceptSecond(handler)", "handler");
    ASSERT_NEQ(first_site, UINT32_MAX);
    ASSERT_NEQ(second_site, UINT32_MAX);

    CBMFileResult *result = lb_extract(source, CBM_LANG_CSHARP, "Overloads.cs");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite first = lb_binding_site(result, "overloaded", "handler", first_site);
    LBBindingSite second = lb_binding_site(result, "overloaded", "handler", second_site);
    cbm_free_result(result);

    ASSERT_EQ(first.count, 1);
    ASSERT_EQ(first.value_count, 1);
    ASSERT_EQ(first.blocked_count, 1);
    ASSERT_EQ(first.local_shadow_count, 1);
    ASSERT_EQ(second.count, 1);
    ASSERT_EQ(second.value_count, 1);
    ASSERT_EQ(second.candidate_count, 1);
    ASSERT_EQ(second.blocked_count, 0);
    ASSERT_EQ(second.local_shadow_count, 0);
    PASS();
}

/* PowerShell variable names are case-insensitive.  Normalizing the sigil but
 * retaining case lets `$handler` evade the `$Handler` parameter binding. */
TEST(repro_powershell_parameter_shadow_is_case_insensitive) {
    static const char source[] = "function Handler { }\n"
                                 "function Caller {\n"
                                 "  param($Handler)\n"
                                 "  Write-Output $Handler\n"
                                 "  Write-Output $handler\n"
                                 "}\n";

    uint32_t exact_site = lb_identifier_offset(source, "Write-Output $Handler", "$Handler");
    uint32_t folded_site = lb_identifier_offset(source, "Write-Output $handler", "$handler");
    ASSERT_NEQ(exact_site, UINT32_MAX);
    ASSERT_NEQ(folded_site, UINT32_MAX);

    CBMFileResult *result = lb_extract(source, CBM_LANG_POWERSHELL, "case_scope.ps1");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite exact = lb_binding_site(result, "Caller", "$Handler", exact_site);
    LBBindingSite folded = lb_binding_site(result, "Caller", "$handler", folded_site);
    cbm_free_result(result);

    ASSERT_EQ(exact.count, 1);
    ASSERT_EQ(exact.value_count, 1);
    ASSERT_EQ(exact.blocked_count, 1);
    ASSERT_EQ(exact.local_shadow_count, 1);
    ASSERT_EQ(folded.count, 1);
    ASSERT_EQ(folded.value_count, 1);
    ASSERT_EQ(folded.blocked_count, 1);
    ASSERT_EQ(folded.local_shadow_count, 1);
    PASS();
}

/* A class statement executes inside outer(), but its namespace is not outer's
 * local namespace. Class attributes must not poison a later outer-scope value
 * reference with the same spelling. */
TEST(repro_python_local_class_binding_stops_at_class_namespace) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def outer():\n"
                                 "    class Local:\n"
                                 "        handler = 1\n"
                                 "    accept(handler)\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "local_class.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "outer", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(usage.blocked_count, 0);
    ASSERT_EQ(usage.local_shadow_count, 0);
    PASS();
}

/* A method's unqualified lookup skips its containing class namespace.  The
 * class attribute named `handler` therefore must not block the module-level
 * function value used inside method(). */
TEST(repro_python_method_lookup_bypasses_class_namespace) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def accept_class(callback):\n"
                                 "    pass\n"
                                 "class Sample:\n"
                                 "    handler = 1\n"
                                 "    accept_class(handler)\n"
                                 "    def method(self):\n"
                                 "        accept(handler)\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    uint32_t class_site = lb_identifier_offset(source, "accept_class(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    ASSERT_NEQ(class_site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "method_class_scope.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "method", "handler", site);
    LBBindingSite class_usage = lb_binding_site(result, NULL, "handler", class_site);
    cbm_free_result(result);

    ASSERT_EQ(class_usage.count, 1);
    ASSERT_EQ(class_usage.blocked_count, 1);
    ASSERT_EQ(class_usage.local_shadow_count, 1);
    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.value_count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(usage.blocked_count, 0);
    ASSERT_EQ(usage.local_shadow_count, 0);
    PASS();
}

/* A method's default expressions are evaluated while the class statement is
 * executing. Unlike the method body, that expression must therefore see and
 * be blocked by the class attribute with the same spelling. */
TEST(repro_python_method_default_uses_class_namespace) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "class Sample:\n"
                                 "    handler = 1\n"
                                 "    def method(self, callback=handler):\n"
                                 "        return callback\n";

    uint32_t site = lb_identifier_offset(source, "callback=handler", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "method_default_class_scope.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "method", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.value_count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 1);
    PASS();
}

/* Imports are bindings even though import subtrees are excluded from ordinary
 * usage emission. Python local imports obey whole-function local analysis, so
 * both the read before the statement and the read after it are blocked. */
TEST(repro_python_local_import_alias_binds_whole_function) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept_before(callback):\n"
                                 "    pass\n"
                                 "def accept_after(callback):\n"
                                 "    pass\n"
                                 "def caller():\n"
                                 "    accept_before(handler)\n"
                                 "    import external_package as handler\n"
                                 "    accept_after(handler)\n";

    uint32_t before_site = lb_identifier_offset(source, "accept_before(handler)", "handler");
    uint32_t after_site = lb_identifier_offset(source, "accept_after(handler)", "handler");
    ASSERT_NEQ(before_site, UINT32_MAX);
    ASSERT_NEQ(after_site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "local_import_alias.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite before = lb_binding_site(result, "caller", "handler", before_site);
    LBBindingSite after = lb_binding_site(result, "caller", "handler", after_site);
    cbm_free_result(result);

    ASSERT_EQ(before.count, 1);
    ASSERT_EQ(before.blocked_count, 1);
    ASSERT_EQ(before.local_shadow_count, 1);
    ASSERT_EQ(after.count, 1);
    ASSERT_EQ(after.blocked_count, 1);
    ASSERT_EQ(after.local_shadow_count, 1);
    PASS();
}

TEST(repro_python_local_from_import_alias_binds_whole_function) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def caller():\n"
                                 "    from external_package import remote_value as handler\n"
                                 "    accept(handler)\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "local_from_import_alias.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 1);
    PASS();
}

/* Import source paths are not local declarations. In particular, dotted
 * imports bind their first component and from-imports bind the imported name,
 * never the source module spelling. */
TEST(repro_python_import_source_roles_do_not_bind) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def caller():\n"
                                 "    import package.handler\n"
                                 "    from handler import imported_value\n"
                                 "    accept(handler)\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "import_source_roles.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.value_count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(usage.blocked_count, 0);
    ASSERT_EQ(usage.local_shadow_count, 0);
    PASS();
}

/* A future import is a compiler directive. Its feature name is neither an
 * ordinary value use nor a local binding. */
TEST(repro_python_future_import_is_not_binding_or_usage) {
    static const char source[] = "from __future__ import annotations\n"
                                 "def annotations():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def caller():\n"
                                 "    accept(annotations)\n";

    uint32_t directive_site =
        lb_identifier_offset(source, "from __future__ import annotations", "annotations");
    uint32_t usage_site = lb_identifier_offset(source, "accept(annotations)", "annotations");
    ASSERT_NEQ(directive_site, UINT32_MAX);
    ASSERT_NEQ(usage_site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "future_import.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite directive = lb_binding_site(result, NULL, "annotations", directive_site);
    LBBindingSite usage = lb_binding_site(result, "caller", "annotations", usage_site);
    cbm_free_result(result);

    ASSERT_EQ(directive.count, 0);
    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(usage.blocked_count, 0);
    PASS();
}

/* CBMImport metadata is consumed independently of lexical blockers. Preserve
 * the complete dependency path while recording Python's actual local binding. */
TEST(repro_python_dotted_import_records_root_local_name) {
    static const char source[] = "import package.child\n";
    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "dotted_import.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    ASSERT_EQ(result->imports.count, 1);
    ASSERT_NOT_NULL(result->imports.items[0].local_name);
    ASSERT_NOT_NULL(result->imports.items[0].module_path);
    ASSERT_STR_EQ(result->imports.items[0].local_name, "package");
    ASSERT_STR_EQ(result->imports.items[0].module_path, "package.child");
    cbm_free_result(result);
    PASS();
}

/* A TypeScript import alias owns the module binding. Without exact semantic
 * proof, the argument must not raw-name-fallback to an unrelated callable
 * named `handler`. */
TEST(repro_typescript_import_alias_blocks_raw_callable_fallback) {
    static const char source[] = "import { remoteValue as handler } from 'external-package';\n"
                                 "function accept(callback: unknown): void {}\n"
                                 "export function caller(): void { accept(handler); }\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_TYPESCRIPT, "import_alias.ts");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.value_count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 0);
    PASS();
}

/* The source side of an aliased ES import does not enter local scope. A local
 * function with that spelling remains eligible for exact semantic proof. */
TEST(repro_typescript_import_source_name_does_not_bind) {
    static const char source[] = "import { handler as importedHandler } from './external';\n"
                                 "function handler(): void {}\n"
                                 "function accept(callback: unknown): void {}\n"
                                 "export function caller(): void { accept(handler); }\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_TYPESCRIPT, "import_source_role.ts");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(usage.blocked_count, 0);
    ASSERT_EQ(usage.local_shadow_count, 0);
    PASS();
}

/* TypeScript import-equals uses import_require_clause rather than the ES
 * import_clause family. Its leading identifier still owns the module binding. */
TEST(repro_typescript_import_require_clause_binds_local) {
    static const char source[] = "import handler = require('./external');\n"
                                 "function accept(callback: unknown): void {}\n"
                                 "export function caller(): void { accept(handler); }\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_TYPESCRIPT, "import_require.ts");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.value_count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 0);
    PASS();
}

/* Dynamic lexical-scope storage must preserve blockers beyond the historical
 * 128-entry inline capacity. */
TEST(repro_javascript_deep_block_nesting_keeps_lexical_binding) {
    enum { NESTING = 140, SOURCE_CAP = 8192 };
    char source[SOURCE_CAP];
    size_t used = (size_t)snprintf(
        source, sizeof(source),
        "function handler() {}\nfunction accept(callback) {}\nfunction caller() {\n");
    for (int i = 0; i < NESTING; i++) {
        used += (size_t)snprintf(source + used, sizeof(source) - used, "{\n");
    }
    used += (size_t)snprintf(source + used, sizeof(source) - used,
                             "let handler = 1;\naccept(handler);\n");
    for (int i = 0; i < NESTING; i++) {
        used += (size_t)snprintf(source + used, sizeof(source) - used, "}\n");
    }
    used += (size_t)snprintf(source + used, sizeof(source) - used, "}\n");
    ASSERT_LT(used, sizeof(source));

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_JAVASCRIPT, "deep_blocks.js");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 1);
    PASS();
}

TEST(repro_typescript_deep_block_nesting_keeps_lexical_binding) {
    enum { NESTING = 140, SOURCE_CAP = 8192 };
    char source[SOURCE_CAP];
    size_t used = (size_t)snprintf(source, sizeof(source),
                                   "function handler(): void {}\n"
                                   "function accept(callback: unknown): void {}\n"
                                   "function caller(): void {\n");
    for (int i = 0; i < NESTING; i++) {
        used += (size_t)snprintf(source + used, sizeof(source) - used, "{\n");
    }
    used += (size_t)snprintf(source + used, sizeof(source) - used,
                             "let handler = 1;\naccept(handler);\n");
    for (int i = 0; i < NESTING; i++) {
        used += (size_t)snprintf(source + used, sizeof(source) - used, "}\n");
    }
    used += (size_t)snprintf(source + used, sizeof(source) - used, "}\n");
    ASSERT_LT(used, sizeof(source));

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_TYPESCRIPT, "deep_blocks.ts");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 1);
    PASS();
}

/* Rust block-local use aliases are lexical bindings as well. */
TEST(repro_rust_block_use_alias_binds_local_scope) {
    static const char source[] = "fn handler() {}\n"
                                 "mod values { pub const remote_value: i32 = 1; }\n"
                                 "fn accept<T>(_callback: T) {}\n"
                                 "fn caller() {\n"
                                 "    use crate::values::remote_value as handler;\n"
                                 "    accept(handler);\n"
                                 "}\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_RUST, "block_use_alias.rs");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.value_count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 1);
    PASS();
}

/* The terminal source segment of a use-as item is not itself introduced into
 * scope; only the alias is. */
TEST(repro_rust_use_source_name_does_not_bind) {
    static const char source[] = "fn handler() {}\n"
                                 "use crate::values::handler as imported_handler;\n"
                                 "fn accept<T>(_callback: T) {}\n"
                                 "fn caller() { accept(handler); }\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_RUST, "use_source_role.rs");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(usage.blocked_count, 0);
    ASSERT_EQ(usage.local_shadow_count, 0);
    PASS();
}

/* extern-crate aliases put name/alias directly on the import boundary. The
 * alias binds, while the source crate spelling remains available to a distinct
 * same-named item. */
TEST(repro_rust_extern_crate_alias_binds_only_alias) {
    static const char source[] = "extern crate dependency as handler;\n"
                                 "fn dependency() {}\n"
                                 "fn accept<T>(_callback: T) {}\n"
                                 "fn accept_dependency<T>(_callback: T) {}\n"
                                 "fn caller() {\n"
                                 "    accept(handler);\n"
                                 "    accept_dependency(dependency);\n"
                                 "}\n";

    uint32_t alias_site = lb_identifier_offset(source, "accept(handler)", "handler");
    uint32_t source_site = lb_identifier_offset(source, "dependency);", "dependency");
    ASSERT_NEQ(alias_site, UINT32_MAX);
    ASSERT_NEQ(source_site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_RUST, "extern_alias.rs");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite alias = lb_binding_site(result, "caller", "handler", alias_site);
    LBBindingSite original = lb_binding_site(result, "caller", "dependency", source_site);
    cbm_free_result(result);

    ASSERT_EQ(alias.count, 1);
    ASSERT_EQ(alias.blocked_count, 1);
    ASSERT_EQ(alias.local_shadow_count, 0);
    ASSERT_EQ(original.count, 1);
    ASSERT_EQ(original.candidate_count, 1);
    ASSERT_EQ(original.blocked_count, 0);
    PASS();
}

/* A direct self entry imports the prefix module under its terminal segment. */
TEST(repro_rust_brace_self_binds_prefix) {
    static const char source[] = "mod values {}\n"
                                 "use crate::values::{self};\n"
                                 "fn accept<T>(_callback: T) {}\n"
                                 "fn caller() { accept(values); }\n";

    uint32_t site = lb_identifier_offset(source, "accept(values)", "values");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_RUST, "brace_self.rs");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "values", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 0);
    PASS();
}

/* Rust use declarations are items, so their bindings cover the complete
 * containing block even when the textual declaration appears later. */
TEST(repro_rust_use_item_binds_before_declaration) {
    static const char source[] = "fn accept<T>(_callback: T) {}\n"
                                 "fn caller() {\n"
                                 "    accept(handler);\n"
                                 "    use crate::values::remote as handler;\n"
                                 "}\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_RUST, "use_before_declaration.rs");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 1);
    PASS();
}

/* An inline module owns a distinct import namespace. Its alias blocks inside
 * that module but must not leak to a sibling function in the file root. */
TEST(repro_rust_inline_module_import_does_not_leak) {
    static const char source[] = "fn handler() {}\n"
                                 "fn accept<T>(_callback: T) {}\n"
                                 "mod inner {\n"
                                 "    use crate::values::remote as handler;\n"
                                 "    fn inside() { accept(handler); }\n"
                                 "}\n"
                                 "fn outside() { accept(handler); }\n";

    uint32_t inside_site = lb_identifier_offset(source, "accept(handler)", "handler");
    const char *outside_marker = strstr(source, "fn outside()");
    uint32_t outside_site = outside_marker
                                ? lb_identifier_offset(outside_marker, "accept(handler)", "handler")
                                : UINT32_MAX;
    if (outside_site != UINT32_MAX) {
        outside_site += (uint32_t)(outside_marker - source);
    }
    ASSERT_NEQ(inside_site, UINT32_MAX);
    ASSERT_NEQ(outside_site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_RUST, "inline_module.rs");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite inside = lb_binding_site(result, "inside", "handler", inside_site);
    LBBindingSite outside = lb_binding_site(result, "outside", "handler", outside_site);
    cbm_free_result(result);

    ASSERT_EQ(inside.count, 1);
    ASSERT_EQ(inside.blocked_count, 1);
    ASSERT_EQ(inside.local_shadow_count, 0);
    ASSERT_EQ(outside.count, 1);
    ASSERT_EQ(outside.candidate_count, 1);
    ASSERT_EQ(outside.blocked_count, 0);
    ASSERT_EQ(outside.local_shadow_count, 0);
    PASS();
}

/* ObjectScript SET targets create routine-local values. The later argument is
 * not a reference to a same-named routine label. */
TEST(repro_objectscript_set_target_binds_routine_local) {
    static const char source[] = "Watched()\n"
                                 "    Quit\n"
                                 "Accept(callback)\n"
                                 "    Quit\n"
                                 "Caller()\n"
                                 "    Set watched=1\n"
                                 "    Set result=$$Accept(watched)\n"
                                 "    Quit result\n";

    uint32_t site = lb_identifier_offset(source, "$$Accept(watched)", "watched");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_OBJECTSCRIPT_ROUTINE, "Binding.mac");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "Caller", "watched", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.value_count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 1);
    PASS();
}

TEST(repro_objectscript_udl_set_target_binds_method_local) {
    static const char source[] = "Class Binding.Sample Extends %RegisteredObject\n"
                                 "{\n"
                                 "ClassMethod Watched() { Quit }\n"
                                 "ClassMethod Accept(callback) { Quit }\n"
                                 "ClassMethod Caller()\n"
                                 "{\n"
                                 "    Set watched=1\n"
                                 "    Set result=..Accept(watched)\n"
                                 "    Quit result\n"
                                 "}\n"
                                 "}\n";

    uint32_t site = lb_identifier_offset(source, "..Accept(watched)", "watched");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_OBJECTSCRIPT_UDL, "Binding.cls");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "Caller", "watched", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.value_count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 1);
    PASS();
}

/* Python evaluates a default expression in the enclosing namespace before the
 * new function's parameter scope exists. `handler=handler` therefore reads the
 * module function on the right, not the parameter being declared on the left. */
TEST(repro_python_default_expression_uses_enclosing_namespace) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def outer():\n"
                                 "    def nested(handler=handler):\n"
                                 "        return handler\n"
                                 "    return nested\n";

    uint32_t site = lb_identifier_offset(source, "=handler", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "default_scope.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "nested", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.value_count, 1);
    ASSERT_EQ(usage.blocked_count, 0);
    ASSERT_EQ(usage.local_shadow_count, 0);
    PASS();
}

/* A global directive changes binding ownership. The reassignment blocks an
 * unproven callable fallback, but its owner is the module rather than caller's
 * local function scope. */
TEST(repro_python_global_assignment_is_module_binding) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def replacement():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def caller():\n"
                                 "    global handler\n"
                                 "    handler = replacement\n"
                                 "    accept(handler)\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "global_scope.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 0);
    PASS();
}

/* Module assignments participate in name lookup too. A later function must
 * not raw-name-fallback through a module variable to an older same-named
 * callable declaration. */
TEST(repro_python_module_assignment_blocks_callable_fallback) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "handler = 1\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def caller():\n"
                                 "    accept(handler)\n";

    uint32_t site = lb_identifier_offset(source, "accept(handler)", "handler");
    ASSERT_NEQ(site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_PYTHON, "module_binding.py");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite usage = lb_binding_site(result, "caller", "handler", site);
    cbm_free_result(result);

    ASSERT_EQ(usage.count, 1);
    ASSERT_EQ(usage.candidate_count, 1);
    ASSERT_EQ(usage.blocked_count, 1);
    ASSERT_EQ(usage.local_shadow_count, 0);
    PASS();
}

/* A function literal owns its parameters independently. The literal's
 * `handler` must block inside the closure but disappear after the closure. */
TEST(repro_go_function_literal_parameter_does_not_leak) {
    static const char source[] = "package sample\n"
                                 "func handler() {}\n"
                                 "func acceptInside(value interface{}) {}\n"
                                 "func acceptOutside(value interface{}) {}\n"
                                 "func caller() {\n"
                                 "    callback := func(handler int) { acceptInside(handler) }\n"
                                 "    _ = callback\n"
                                 "    acceptOutside(handler)\n"
                                 "}\n";

    uint32_t inside_site = lb_identifier_offset(source, "acceptInside(handler)", "handler");
    uint32_t outside_site = lb_identifier_offset(source, "acceptOutside(handler)", "handler");
    ASSERT_NEQ(inside_site, UINT32_MAX);
    ASSERT_NEQ(outside_site, UINT32_MAX);
    CBMFileResult *result = lb_extract(source, CBM_LANG_GO, "literal.go");
    ASSERT_NOT_NULL(result);
    ASSERT_FALSE(result->has_error || result->parse_incomplete);
    LBBindingSite inside = lb_binding_site(result, "caller", "handler", inside_site);
    LBBindingSite outside = lb_binding_site(result, "caller", "handler", outside_site);
    cbm_free_result(result);

    ASSERT_EQ(inside.count, 1);
    ASSERT_EQ(inside.blocked_count, 1);
    ASSERT_EQ(inside.local_shadow_count, 1);
    ASSERT_EQ(outside.count, 1);
    ASSERT_EQ(outside.candidate_count, 1);
    ASSERT_EQ(outside.blocked_count, 0);
    ASSERT_EQ(outside.local_shadow_count, 0);
    PASS();
}

SUITE(repro_lexical_binding_precision) {
    RUN_TEST(repro_python_keyword_label_does_not_bind_local);
    RUN_TEST(repro_python_function_name_does_not_shadow_itself);
    RUN_TEST(repro_python_nested_function_binds_in_enclosing_function);
    RUN_TEST(repro_python_assignment_blocks_earlier_use_for_whole_function);
    RUN_TEST(repro_javascript_block_local_stops_at_block_exit);
    RUN_TEST(repro_csharp_overload_bodies_do_not_share_parameter_bindings);
    RUN_TEST(repro_powershell_parameter_shadow_is_case_insensitive);
    RUN_TEST(repro_python_local_class_binding_stops_at_class_namespace);
    RUN_TEST(repro_python_method_lookup_bypasses_class_namespace);
    RUN_TEST(repro_python_method_default_uses_class_namespace);
    RUN_TEST(repro_python_local_import_alias_binds_whole_function);
    RUN_TEST(repro_python_local_from_import_alias_binds_whole_function);
    RUN_TEST(repro_python_import_source_roles_do_not_bind);
    RUN_TEST(repro_python_future_import_is_not_binding_or_usage);
    RUN_TEST(repro_python_dotted_import_records_root_local_name);
    RUN_TEST(repro_typescript_import_alias_blocks_raw_callable_fallback);
    RUN_TEST(repro_typescript_import_source_name_does_not_bind);
    RUN_TEST(repro_typescript_import_require_clause_binds_local);
    RUN_TEST(repro_javascript_deep_block_nesting_keeps_lexical_binding);
    RUN_TEST(repro_typescript_deep_block_nesting_keeps_lexical_binding);
    RUN_TEST(repro_rust_block_use_alias_binds_local_scope);
    RUN_TEST(repro_rust_use_source_name_does_not_bind);
    RUN_TEST(repro_rust_extern_crate_alias_binds_only_alias);
    RUN_TEST(repro_rust_brace_self_binds_prefix);
    RUN_TEST(repro_rust_use_item_binds_before_declaration);
    RUN_TEST(repro_rust_inline_module_import_does_not_leak);
    RUN_TEST(repro_objectscript_set_target_binds_routine_local);
    RUN_TEST(repro_objectscript_udl_set_target_binds_method_local);
    RUN_TEST(repro_python_default_expression_uses_enclosing_namespace);
    RUN_TEST(repro_python_global_assignment_is_module_binding);
    RUN_TEST(repro_python_module_assignment_blocks_callable_fallback);
    RUN_TEST(repro_go_function_literal_parameter_does_not_leak);
}
