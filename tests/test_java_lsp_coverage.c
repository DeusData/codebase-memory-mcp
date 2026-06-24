/*
 * test_java_lsp_coverage.c — Comprehensive coverage tests for the Java LSP.
 *
 * This is the deep-coverage suite that exercises every code path in the
 * resolver and every stdlib type in the registry. ~200 tests organized by:
 *
 *   1.  Literals (10)               — all numeric / string / char / bool variants
 *   2.  Type-name resolution (12)   — every path in java_resolve_type_name
 *   3.  Primitive expressions (10)  — arithmetic, comparison, logical, bitwise
 *   4.  Type-AST parsing (12)       — every kind handled by java_parse_type_node
 *   5.  Identifier resolution (10)  — scope chain, fields, imports, types
 *   6.  Field access (10)           — instance, static (System.out), arrays, scope
 *   7.  Method invocation (15)      — bare, this, super, static, instance, chain
 *   8.  Object creation (8)         — generic, raw, anonymous, nested
 *   9.  Cast / instanceof (8)
 *   10. Generic substitution (15)   — every entry in is_value_typed_container/is_map_like
 *   11. Lambda SAM inference (12)   — every functional-interface category
 *   12. Method references (10)      — Class::method, instance::method, ::new, super::
 *   13. Inheritance walking (10)    — multi-level, interfaces, sole-impl
 *   14. java.lang stdlib (15)       — Object/String/StringBuilder/Math/System/Thread/Class/Throwable
 *   15. java.util stdlib (15)       — collections, Optional, Iterator, regex
 *   16. java.io stdlib (10)
 *   17. java.nio.file (5)
 *   18. java.util.stream (10)
 *   19. java.util.function (10)
 *   20. java.util.concurrent (10)
 *   21. java.time (10)
 *   22. Imports (8)                 — single, on-demand, static, static on-demand
 *   23. Inner / nested types (10)
 *   24. Variable / scope binding (10)
 *   25. Control flow (10)           — if, for, while, switch, try
 *   26. Diagnostics (8)             — unresolved markers
 *   27. Cross-file (5)
 *   28. Stress / edge (10)          — pathological inputs, recursion, deep nesting
 *
 * Total: ~278 tests.
 */

#include "test_framework.h"
#include "cbm.h"
#include "lsp/java_lsp.h"

#include <string.h>
#include <stdio.h>

/* ── Helpers (shared form, kept local to avoid linker collisions) ─── */

static CBMFileResult *cov_extract(const char *source) {
    return cbm_extract_file(source, (int)strlen(source), CBM_LANG_JAVA, "test", "Main.java", 0,
                            NULL, NULL);
}

static int cov_find(const CBMFileResult *r, const char *callerSub, const char *calleeSub) {
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->confidence < 0.5f) continue;
        if (rc->caller_qn && strstr(rc->caller_qn, callerSub) && rc->callee_qn &&
            strstr(rc->callee_qn, calleeSub)) {
            return i;
        }
    }
    return -1;
}

static int cov_require(const CBMFileResult *r, const char *callerSub, const char *calleeSub) {
    int idx = cov_find(r, callerSub, calleeSub);
    if (idx < 0) {
        printf("\n  MISSING: %s -> %s (have %d entries)\n", callerSub, calleeSub,
               r->resolved_calls.count);
        for (int i = 0; i < r->resolved_calls.count; i++) {
            const CBMResolvedCall *rc = &r->resolved_calls.items[i];
            printf("    %s -> %s [%s %.2f]\n", rc->caller_qn ? rc->caller_qn : "(null)",
                   rc->callee_qn ? rc->callee_qn : "(null)",
                   rc->strategy ? rc->strategy : "(null)", rc->confidence);
        }
    }
    return idx;
}

static int cov_count_strict(const CBMFileResult *r) {
    int n = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        if (r->resolved_calls.items[i].confidence >= 0.85f) n++;
    }
    return n;
}

static int cov_no_crash(CBMFileResult *r) { return r != NULL; }

/* Macro for quick "wrap source in a Main class with run() that returns
 * void and contains the body" pattern. */
#define WRAP_RUN(body) \
    "public class Main {\n  public void run(){\n    " body "\n  }\n}\n"

/* ── 1. Literals ─────────────────────────────────────────────────── */

TEST(cov_lit_int) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int run() { int x = 42; return x; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lit_long) {
    CBMFileResult *r = cov_extract("public class Main {\n"
                                    "  public long run() { long x = 99L; return x; }\n"
                                    "}");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lit_hex) {
    CBMFileResult *r =
        cov_extract("public class Main { public int f() { int x = 0xFF; return x; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lit_binary) {
    CBMFileResult *r =
        cov_extract("public class Main { public int f() { int x = 0b1010; return x; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lit_octal) {
    CBMFileResult *r =
        cov_extract("public class Main { public int f() { int x = 0777; return x; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lit_float_suffix) {
    CBMFileResult *r = cov_extract(
        "public class Main { public float f() { float x = 1.5f; return x; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lit_double) {
    CBMFileResult *r = cov_extract(
        "public class Main { public double f() { double x = 3.14; return x; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lit_char) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { char c = 'a'; return Character.getNumericValue(c); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Character.getNumericValue"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lit_string_method) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { return \"hello\".length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lit_bool_method) {
    CBMFileResult *r = cov_extract(
        "public class Main { public String f() { return Boolean.toString(true); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Boolean.toString"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lit_null_no_crash) {
    CBMFileResult *r = cov_extract(
        "public class Main { public Object f() { return null; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── 2. Type-name resolution paths ───────────────────────────────── */

TEST(cov_typename_java_lang_implicit) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) { return s.length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typename_via_single_import) {
    CBMFileResult *r =
        cov_extract("import java.util.HashMap;\n"
                    "public class Main { public int f(HashMap<String,String> m) { return m.size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "HashMap.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typename_via_on_demand_import) {
    CBMFileResult *r =
        cov_extract("import java.util.*;\n"
                    "public class Main { public int f(HashMap<String,String> m) { return m.size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "HashMap.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typename_via_static_import) {
    CBMFileResult *r = cov_extract("import static java.lang.Math.PI;\n"
                                    "public class Main { public double f() { return PI; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typename_qualified_path) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(java.util.HashMap<String,String> m) { return m.size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "HashMap.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typename_inner_via_outer) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class Inner { public String tag() { return \"x\"; } }\n"
        "  public String f() { Inner i = new Inner(); return i.tag(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "tag"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typename_nested_two_levels) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class A {\n"
        "    static class B { public String tag() { return \"b\"; } }\n"
        "  }\n"
        "  public String f() { Main.A.B b = new Main.A.B(); return b.tag(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typename_array_type) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String[] xs) { return xs.length; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typename_2d_array) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(int[][] m) { return m[0].length; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typename_generic_type) {
    CBMFileResult *r = cov_extract("import java.util.List;\n"
                                    "public class Main { public int f(List<String> xs) { return xs.size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "List.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typename_wildcard) {
    CBMFileResult *r = cov_extract("import java.util.List;\n"
                                    "public class Main { public int f(List<?> xs) { return xs.size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "List.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typename_bounded_wildcard) {
    CBMFileResult *r = cov_extract("import java.util.List;\n"
                                    "public class Main { public int f(List<? extends Number> xs) { return xs.size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "List.size"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 3. Primitive operations ─────────────────────────────────────── */

TEST(cov_op_arith) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(int a, int b) { return a + b * 2 - 1 / 3 % 4; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_op_compare) {
    CBMFileResult *r = cov_extract(
        "public class Main { public boolean f(int a, int b) { return a < b && a >= 0 && a != b; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_op_bitwise) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(int a) { return (a & 0xFF) | (a << 8) ^ (a >> 4); } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_op_unary_negate) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(int a) { return -a; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_op_logical_not) {
    CBMFileResult *r = cov_extract(
        "public class Main { public boolean f(boolean b) { return !b; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_op_increment) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { int a = 0; a++; ++a; return a; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_op_string_concat) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) { String t = \"a\" + s + 1; return t.length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_op_ternary_simple) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(boolean b) { return b ? 1 : 2; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_op_assignment) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { int a = 0; a += 1; a -= 1; return a; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_op_paren_expr) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) { return ((s)).length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 4. Type-AST parsing kinds ───────────────────────────────────── */

TEST(cov_typeast_void_method) {
    CBMFileResult *r = cov_extract("public class Main { public void f() {} }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typeast_byte_param) {
    CBMFileResult *r =
        cov_extract("public class Main { public int f(byte b) { return b + 1; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typeast_short_param) {
    CBMFileResult *r =
        cov_extract("public class Main { public int f(short s) { return s + 1; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typeast_int_param) {
    CBMFileResult *r =
        cov_extract("public class Main { public int f(int i) { return i + 1; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typeast_long_param) {
    CBMFileResult *r =
        cov_extract("public class Main { public long f(long l) { return l + 1; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typeast_float_param) {
    CBMFileResult *r =
        cov_extract("public class Main { public float f(float f) { return f + 1; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typeast_double_param) {
    CBMFileResult *r =
        cov_extract("public class Main { public double f(double d) { return d + 1; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typeast_char_param) {
    CBMFileResult *r =
        cov_extract("public class Main { public char f(char c) { return c; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typeast_boolean_param) {
    CBMFileResult *r =
        cov_extract("public class Main { public boolean f(boolean b) { return b; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typeast_string_param) {
    CBMFileResult *r =
        cov_extract("public class Main { public int f(String s) { return s.length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typeast_array_of_string) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String[] xs) { return xs[0].length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_typeast_generic_list) {
    CBMFileResult *r = cov_extract(
        "import java.util.List;\n"
        "public class Main { public int f(List<String> xs) { return xs.get(0).length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 5. Identifier resolution ────────────────────────────────────── */

TEST(cov_id_local_var) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { String s = \"x\"; return s.length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_id_param) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) { return s.length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_id_field) {
    CBMFileResult *r = cov_extract(
        "public class Main { private String s = \"x\"; public int f() { return s.length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_id_inherited_field) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class A { protected String s = \"x\"; }\n"
        "  static class B extends A { public int f() { return s.length(); } }\n"
        "}");
    ASSERT_NOT_NULL(r);
    /* Fields of inner classes — the LSP propagates parent fields via
     * the class registry. The exact resolution rate isn't asserted but
     * the test must not crash. */
    cbm_free_result(r);
    PASS();
}
TEST(cov_id_shadow_param) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  private String s = \"x\";\n"
        "  public int f(String s) { return s.length(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_id_block_shadow) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  public int f() {\n"
        "    String s = \"a\";\n"
        "    if (true) { String s2 = s.trim(); return s2.length(); }\n"
        "    return s.length();\n"
        "  }\n"
        "}");
    ASSERT_NOT_NULL(r);
    /* Both String.length and String.trim should appear. */
    ASSERT_GTE(cov_require(r, "f", "String.trim"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_id_outer_class_field) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  private String name = \"x\";\n"
        "  class Inner { public int f() { return name.length(); } }\n"
        "}");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_id_class_as_static) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { return Integer.MAX_VALUE; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_id_unresolved_emits_diagnostic) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { return undefined.size(); } }");
    ASSERT_NOT_NULL(r);
    int diag = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        if (r->resolved_calls.items[i].confidence == 0.0f) diag++;
    }
    ASSERT_GTE(diag, 1);
    cbm_free_result(r);
    PASS();
}
TEST(cov_id_for_loop_var) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public int total(ArrayList<String> xs) {\n"
        "    int n = 0;\n"
        "    for (int i = 0; i < xs.size(); i++) n += xs.get(i).length();\n"
        "    return n;\n"
        "  }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "total", "ArrayList.size"), 0);
    ASSERT_GTE(cov_require(r, "total", "ArrayList.get"), 0);
    ASSERT_GTE(cov_require(r, "total", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 6. Field access ─────────────────────────────────────────────── */

TEST(cov_fa_system_out) {
    CBMFileResult *r = cov_extract(
        "public class Main { public void f() { System.out.println(\"x\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "PrintStream.println"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fa_system_err) {
    CBMFileResult *r = cov_extract(
        "public class Main { public void f() { System.err.println(\"x\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "PrintStream.println"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fa_array_length) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(int[] xs) { return xs.length; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fa_chained_field_method) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class Box { public String s = \"x\"; }\n"
        "  public int f(Box b) { return b.s.length(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fa_this_field) {
    CBMFileResult *r = cov_extract(
        "public class Main { private String s = \"x\"; public int f() { return this.s.length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fa_static_class_field) {
    CBMFileResult *r =
        cov_extract("public class Main { public int f() { return Integer.MAX_VALUE; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fa_method_then_field) {
    CBMFileResult *r =
        cov_extract("import java.util.ArrayList;\n"
                    "public class Main { public int f(ArrayList<String> xs) { return xs.toArray().length; } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "ArrayList.toArray"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fa_field_through_param) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class Bag { public String tag = \"x\"; }\n"
        "  public int f(Bag b) { return b.tag.length(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fa_field_through_chained_call) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class A { public String tag = \"x\"; public A self() { return this; } }\n"
        "  public int f(A a) { return a.self().tag.length(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "self"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fa_unresolved_field_no_crash) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { return some.weird.thing.length; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── 7. Method invocation paths ──────────────────────────────────── */

TEST(cov_mi_bare_local) {
    CBMFileResult *r = cov_extract(
        "public class Main { String greet() { return \"hi\"; } public void f() { greet(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "greet"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_this) {
    CBMFileResult *r = cov_extract(
        "public class Main { String greet() { return \"\"; } public void f() { this.greet(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "greet"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_super) {
    CBMFileResult *r = cov_extract(
        "public class Main extends Object {\n"
        "  public String f() { return super.toString(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Object.toString"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_static) {
    CBMFileResult *r = cov_extract(
        "public class Main { public double f() { return Math.sqrt(4); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Math.sqrt"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_chained_static) {
    CBMFileResult *r = cov_extract(
        "public class Main { public String f() { return Integer.toBinaryString(Integer.parseInt(\"5\")); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Integer.toBinaryString"), 0);
    ASSERT_GTE(cov_require(r, "f", "Integer.parseInt"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_instance) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) { return s.length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_chain_3hop) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) { return s.trim().toUpperCase().length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.trim"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.toUpperCase"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_recursive_self) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int fact(int n) { return n <= 1 ? 1 : n * fact(n-1); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "fact", "fact"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_overload_by_arity) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  String fmt(String s) { return s; }\n"
        "  String fmt(String s, int i) { return s; }\n"
        "  public String f() { return fmt(\"a\") + fmt(\"b\", 1); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "fmt"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_param_chain) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) { return s.toUpperCase().length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.toUpperCase"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_return_chain) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  String tag() { return \"x\"; }\n"
        "  public int f() { return tag().length(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "tag"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_args_walked) {
    CBMFileResult *r = cov_extract(
        "import java.util.Objects;\n"
        "public class Main { public int f(String a, String b) { return Objects.hash(a.length(), b.trim()); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Objects.hash"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.trim"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_nested_args) {
    CBMFileResult *r = cov_extract(
        "public class Main { public double f() { return Math.max(Math.abs(-1), Math.sqrt(4)); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Math.max"), 0);
    ASSERT_GTE(cov_require(r, "f", "Math.abs"), 0);
    ASSERT_GTE(cov_require(r, "f", "Math.sqrt"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_through_field_getter) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class Box { public String getTag() { return \"x\"; } }\n"
        "  private Box b = new Box();\n"
        "  public int f() { return b.getTag().length(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "getTag"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mi_call_in_condition) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) { if (s.isEmpty()) return 0; return s.length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.isEmpty"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 8. Object creation paths ────────────────────────────────────── */

TEST(cov_oc_simple) {
    CBMFileResult *r = cov_extract(
        "public class Main { public StringBuilder f() { return new StringBuilder(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "StringBuilder"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_oc_with_args) {
    CBMFileResult *r =
        cov_extract("import java.util.ArrayList;\n"
                    "public class Main { public ArrayList<String> f() { return new ArrayList<>(10); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "ArrayList"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_oc_diamond) {
    CBMFileResult *r =
        cov_extract("import java.util.HashMap;\n"
                    "public class Main { public HashMap<String,Integer> f() { return new HashMap<>(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "HashMap"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_oc_generic_explicit) {
    CBMFileResult *r =
        cov_extract("import java.util.ArrayList;\n"
                    "public class Main { public ArrayList<String> f() { return new ArrayList<String>(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "ArrayList"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_oc_user_class) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class Box { public Box() {} }\n"
        "  public Box f() { return new Box(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Box"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_oc_with_chained_method) {
    CBMFileResult *r = cov_extract(
        "public class Main { public String f() { return new StringBuilder().append(\"x\").toString(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "StringBuilder.append"), 0);
    ASSERT_GTE(cov_require(r, "f", "StringBuilder.toString"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_oc_array) {
    CBMFileResult *r =
        cov_extract("public class Main { public int[] f() { return new int[10]; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_oc_array_init) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int[] f() { int[] xs = {1,2,3}; return xs; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── 9. Cast / instanceof ────────────────────────────────────────── */

TEST(cov_cast_to_string) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(Object o) { return ((String)o).length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_cast_chained) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(Object o) { return ((String)((Object)o)).length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_cast_primitive) {
    CBMFileResult *r =
        cov_extract("public class Main { public int f(double d) { return (int) d; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_instanceof_basic) {
    CBMFileResult *r =
        cov_extract("public class Main { public boolean f(Object o) { return o instanceof String; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_cast_from_iterator) {
    CBMFileResult *r =
        cov_extract("import java.util.Iterator;\n"
                    "public class Main { public int f(Iterator<Object> it) {\n"
                    "  return ((String) it.next()).length();\n"
                    "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_cast_arr_elem) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(Object[] xs) { return ((String)xs[0]).length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_cast_in_ternary) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(Object o, boolean b) { return b ? ((String)o).length() : 0; } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_cast_then_chain) {
    CBMFileResult *r = cov_extract(
        "public class Main { public String f(Object o) { return ((String)o).toUpperCase().trim(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.toUpperCase"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.trim"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 10. Generic substitution table coverage ─────────────────────── */

TEST(cov_gen_list_get) {
    CBMFileResult *r = cov_extract(
        "import java.util.List;\n"
        "public class Main { public int f(List<String> xs) { return xs.get(0).length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_arraylist_get) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public int f(ArrayList<String> xs) { return xs.get(0).length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_linkedlist_first) {
    CBMFileResult *r = cov_extract(
        "import java.util.LinkedList;\n"
        "public class Main { public int f(LinkedList<String> xs) { return xs.getFirst().length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_set_iterator_next) {
    CBMFileResult *r = cov_extract(
        "import java.util.Set;\n"
        "import java.util.Iterator;\n"
        "public class Main { public int f(Set<String> s) {\n"
        "  Iterator<String> it = s.iterator();\n"
        "  return it.next().length();\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Iterator.next"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_map_get) {
    CBMFileResult *r = cov_extract(
        "import java.util.Map;\n"
        "public class Main { public int f(Map<String,String> m) { return m.get(\"k\").length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_hashmap_get) {
    CBMFileResult *r = cov_extract(
        "import java.util.HashMap;\n"
        "public class Main { public int f(HashMap<String,String> m) { return m.get(\"k\").length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_optional_orElse) {
    CBMFileResult *r = cov_extract(
        "import java.util.Optional;\n"
        "public class Main { public int f(Optional<String> o) { return o.orElse(\"\").length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_optional_get) {
    CBMFileResult *r = cov_extract(
        "import java.util.Optional;\n"
        "public class Main { public int f(Optional<String> o) { return o.get().length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_iterator_next_chain) {
    CBMFileResult *r = cov_extract(
        "import java.util.Iterator;\n"
        "public class Main { public int f(Iterator<String> it) { return it.next().length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_function_apply) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Function;\n"
        "public class Main { public int f(Function<String,String> fn, String s) { return fn.apply(s).length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_supplier_get) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Supplier;\n"
        "public class Main { public int f(Supplier<String> sup) { return sup.get().length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_bifunction_apply) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.BiFunction;\n"
        "public class Main { public int f(BiFunction<String,Integer,String> fn, String s) {\n"
        "  return fn.apply(s, 1).length();\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_map_keyset) {
    CBMFileResult *r = cov_extract(
        "import java.util.HashMap;\n"
        "import java.util.Set;\n"
        "public class Main { public int f(HashMap<String,Integer> m) { return m.keySet().size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Set.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_map_values) {
    CBMFileResult *r = cov_extract(
        "import java.util.HashMap;\n"
        "public class Main { public int f(HashMap<String,Integer> m) { return m.values().size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Collection.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_gen_collection_iterator) {
    CBMFileResult *r = cov_extract(
        "import java.util.Collection;\n"
        "public class Main { public int f(Collection<String> c) { return c.iterator().next().length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 11. Lambda SAM inference table coverage ─────────────────────── */

TEST(cov_lambda_function) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Function;\n"
        "public class Main { public Function<String,Integer> f() { return s -> s.length(); } }");
    ASSERT_NOT_NULL(r);
    /* Standalone lambdas without a method-call context don't bind via SAM,
     * but the body should still be walked. */
    cbm_free_result(r);
    PASS();
}
TEST(cov_lambda_predicate_filter) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public long f(ArrayList<String> xs) { return xs.stream().filter(s -> s.length() > 0).count(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lambda_consumer_forEach) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public void f(ArrayList<String> xs) { xs.forEach(s -> { s.toUpperCase(); }); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.toUpperCase"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lambda_function_map) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public Object f(ArrayList<String> xs) { return xs.stream().map(s -> s.trim()).count(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.trim"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lambda_user_consumer) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Consumer;\n"
        "public class Main {\n"
        "  void run(Consumer<String> c) { c.accept(\"x\"); }\n"
        "  public void f() { run(s -> s.toUpperCase()); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.toUpperCase"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lambda_user_function) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Function;\n"
        "public class Main {\n"
        "  Object run(Function<String,Integer> fn) { return fn.apply(\"x\"); }\n"
        "  public Object f() { return run(s -> s.length()); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lambda_user_predicate) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Predicate;\n"
        "public class Main {\n"
        "  boolean run(Predicate<String> p) { return p.test(\"x\"); }\n"
        "  public boolean f() { return run(s -> s.isEmpty()); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.isEmpty"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lambda_block_body) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public void f(ArrayList<String> xs) {\n"
        "  xs.forEach(s -> { String t = s.trim(); int n = t.length(); });\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.trim"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lambda_bipredicate) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.BiPredicate;\n"
        "public class Main {\n"
        "  boolean run(BiPredicate<String,String> bp) { return bp.test(\"a\",\"b\"); }\n"
        "  public boolean f() { return run((a, b) -> a.equals(b)); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.equals"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lambda_biconsumer_map) {
    CBMFileResult *r = cov_extract(
        "import java.util.HashMap;\n"
        "public class Main { public void f(HashMap<String,Integer> m) {\n"
        "  m.forEach((k, v) -> { k.length(); });\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lambda_anyMatch) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public boolean f(ArrayList<String> xs) {\n"
        "  return xs.stream().anyMatch(s -> s.startsWith(\"x\"));\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.startsWith"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lambda_removeIf) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public void f(ArrayList<String> xs) {\n"
        "  xs.removeIf(s -> s.isEmpty());\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.isEmpty"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 12. Method references ───────────────────────────────────────── */

TEST(cov_mref_static) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public void f(ArrayList<String> xs) { xs.forEach(System.out::println); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "PrintStream.println"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mref_instance_method) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public Object f(ArrayList<String> xs) {\n"
        "  return xs.stream().map(String::toUpperCase).count();\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.toUpperCase"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mref_static_class_method) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public Object f(ArrayList<String> xs) {\n"
        "  return xs.stream().map(Integer::parseInt).count();\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Integer.parseInt"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mref_constructor_simple) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Function;\n"
        "public class Main { public Function<String, StringBuilder> f() { return StringBuilder::new; } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "StringBuilder"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mref_in_local_var) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Function;\n"
        "public class Main { public void f() { Function<String,Integer> g = String::length; } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mref_with_chain) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "import java.util.stream.Collectors;\n"
        "public class Main { public Object f(ArrayList<String> xs) {\n"
        "  return xs.stream().map(String::trim).collect(Collectors.toList());\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.trim"), 0);
    ASSERT_GTE(cov_require(r, "f", "Collectors.toList"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mref_user_class) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Function;\n"
        "public class Main {\n"
        "  static class U { public String tag() { return \"x\"; } }\n"
        "  public Function<U,String> f() { return U::tag; }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "tag"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_mref_user_constructor) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Supplier;\n"
        "public class Main {\n"
        "  static class Box { public Box() {} }\n"
        "  public Supplier<Box> f() { return Box::new; }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Box"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 13. Inheritance walking ─────────────────────────────────────── */

TEST(cov_inh_basic) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class A { public String tag() { return \"a\"; } }\n"
        "  static class B extends A { public String f(B b) { return b.tag(); } }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "tag"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_inh_3level) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class A { public String tag() { return \"a\"; } }\n"
        "  static class B extends A {}\n"
        "  static class C extends B { public String f(C c) { return c.tag(); } }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "tag"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_inh_4level) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class A { public String tag() { return \"a\"; } }\n"
        "  static class B extends A {}\n"
        "  static class C extends B {}\n"
        "  static class D extends C { public String f(D d) { return d.tag(); } }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "tag"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_inh_override) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class A { public String tag() { return \"a\"; } }\n"
        "  static class B extends A {\n"
        "    public String tag() { return \"b\"; }\n"
        "    public String f(B b) { return b.tag(); }\n"
        "  }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "tag"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_inh_super_after_override) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class A { public String tag() { return \"a\"; } }\n"
        "  static class B extends A {\n"
        "    public String tag() { return super.tag(); }\n"
        "  }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "tag", "tag"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_inh_via_object) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class A {}\n"
        "  public String f(A a) { return a.toString(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Object.toString"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_inh_interface_default) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  interface Greet { default String hello() { return \"hi\"; } }\n"
        "  static class Impl implements Greet { public String f() { return hello(); } }\n"
        "}");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_inh_interface_via_param) {
    CBMFileResult *r = cov_extract(
        "import java.util.List;\n"
        "public class Main { public int f(List<String> xs) { return xs.size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "List.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_inh_collection_to_list) {
    CBMFileResult *r = cov_extract(
        "import java.util.Collection;\n"
        "public class Main { public int f(Collection<String> c) { return c.size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Collection.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_inh_list_iterable_iterator) {
    CBMFileResult *r = cov_extract(
        "import java.util.List;\n"
        "public class Main { public int f(List<String> xs) { return xs.iterator().next().length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Iterator.next"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 14. java.lang stdlib coverage ───────────────────────────────── */

TEST(cov_lang_object_hashCode) {
    CBMFileResult *r =
        cov_extract("public class Main { public int f(Object o) { return o.hashCode(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Object.hashCode"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_object_equals) {
    CBMFileResult *r = cov_extract(
        "public class Main { public boolean f(Object a, Object b) { return a.equals(b); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Object.equals"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_object_getClass) {
    CBMFileResult *r =
        cov_extract("public class Main { public Class<?> f(Object o) { return o.getClass(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Object.getClass"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_string_repeat) {
    CBMFileResult *r = cov_extract(
        "public class Main { public String f() { return \"x\".repeat(3); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.repeat"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_string_split) {
    CBMFileResult *r = cov_extract(
        "public class Main { public String[] f(String s) { return s.split(\",\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.split"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_string_indexOf) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) { return s.indexOf('a'); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.indexOf"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_string_substring) {
    CBMFileResult *r = cov_extract(
        "public class Main { public String f(String s) { return s.substring(1, 3); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.substring"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_math_pow) {
    CBMFileResult *r = cov_extract(
        "public class Main { public double f() { return Math.pow(2, 10); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Math.pow"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_math_log) {
    CBMFileResult *r =
        cov_extract("public class Main { public double f() { return Math.log(2.0); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Math.log"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_math_random) {
    CBMFileResult *r =
        cov_extract("public class Main { public double f() { return Math.random(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Math.random"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_system_nano) {
    CBMFileResult *r = cov_extract(
        "public class Main { public long f() { return System.nanoTime(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "System.nanoTime"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_system_getenv) {
    CBMFileResult *r = cov_extract(
        "public class Main { public String f() { return System.getenv(\"X\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "System.getenv"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_thread_sleep) {
    CBMFileResult *r = cov_extract(
        "public class Main { public void f() throws InterruptedException { Thread.sleep(1); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Thread.sleep"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_class_forName) {
    CBMFileResult *r = cov_extract(
        "public class Main { public Class<?> f() throws ClassNotFoundException { return Class.forName(\"x\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Class.forName"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_lang_throwable_getCause) {
    CBMFileResult *r = cov_extract(
        "public class Main { public Throwable f(Throwable t) { return t.getCause(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Throwable.getCause"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 15. java.util stdlib coverage ───────────────────────────────── */

TEST(cov_util_arraylist_add) {
    CBMFileResult *r =
        cov_extract("import java.util.ArrayList;\n"
                    "public class Main { public void f(ArrayList<String> xs) { xs.add(\"x\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "ArrayList.add"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_arraylist_remove) {
    CBMFileResult *r =
        cov_extract("import java.util.ArrayList;\n"
                    "public class Main { public void f(ArrayList<String> xs) { xs.remove(0); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "ArrayList.remove"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_linkedlist_addFirst) {
    CBMFileResult *r =
        cov_extract("import java.util.LinkedList;\n"
                    "public class Main { public void f(LinkedList<String> xs) { xs.addFirst(\"x\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "LinkedList.addFirst"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_hashset_add) {
    CBMFileResult *r =
        cov_extract("import java.util.HashSet;\n"
                    "public class Main { public boolean f(HashSet<String> s) { return s.add(\"x\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "HashSet.add"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_treeset_first) {
    CBMFileResult *r =
        cov_extract("import java.util.TreeSet;\n"
                    "public class Main { public Object f(TreeSet<String> s) { return s.first(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "TreeSet.first"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_hashmap_put) {
    CBMFileResult *r =
        cov_extract("import java.util.HashMap;\n"
                    "public class Main { public void f(HashMap<String,Integer> m) { m.put(\"k\", 1); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "HashMap.put"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_treemap_firstKey) {
    CBMFileResult *r =
        cov_extract("import java.util.TreeMap;\n"
                    "public class Main { public Object f(TreeMap<String,Integer> m) { return m.firstKey(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "TreeMap.firstKey"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_arrays_sort) {
    CBMFileResult *r =
        cov_extract("import java.util.Arrays;\n"
                    "public class Main { public void f(int[] xs) { Arrays.sort(xs); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Arrays.sort"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_collections_unmodifiable) {
    CBMFileResult *r =
        cov_extract("import java.util.Collections;\n"
                    "import java.util.List;\n"
                    "public class Main { public List<String> f(List<String> xs) { return Collections.unmodifiableList(xs); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Collections.unmodifiableList"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_objects_isNull) {
    CBMFileResult *r = cov_extract(
        "import java.util.Objects;\n"
        "public class Main { public boolean f(Object o) { return Objects.isNull(o); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Objects.isNull"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_optional_isPresent) {
    CBMFileResult *r = cov_extract(
        "import java.util.Optional;\n"
        "public class Main { public boolean f(Optional<String> o) { return o.isPresent(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Optional.isPresent"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_optional_map) {
    CBMFileResult *r = cov_extract(
        "import java.util.Optional;\n"
        "public class Main { public Object f(Optional<String> o) { return o.map(s -> s.length()); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Optional.map"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_uuid_random) {
    CBMFileResult *r = cov_extract(
        "import java.util.UUID;\n"
        "public class Main { public String f() { return UUID.randomUUID().toString(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "UUID.randomUUID"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_random_nextInt) {
    CBMFileResult *r = cov_extract(
        "import java.util.Random;\n"
        "public class Main { public int f(Random r) { return r.nextInt(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Random.nextInt"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_util_scanner_nextLine) {
    CBMFileResult *r = cov_extract(
        "import java.util.Scanner;\n"
        "public class Main { public String f(Scanner s) { return s.nextLine(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Scanner.nextLine"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 16. java.io stdlib ──────────────────────────────────────────── */

TEST(cov_io_print_writer) {
    CBMFileResult *r = cov_extract(
        "import java.io.PrintWriter;\n"
        "public class Main { public void f(PrintWriter w) { w.println(\"x\"); w.flush(); w.close(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "PrintWriter.println"), 0);
    ASSERT_GTE(cov_require(r, "f", "PrintWriter.flush"), 0);
    ASSERT_GTE(cov_require(r, "f", "PrintWriter.close"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_io_input_stream_read) {
    CBMFileResult *r = cov_extract(
        "import java.io.InputStream;\n"
        "public class Main { public int f(InputStream in) throws java.io.IOException { return in.read(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "InputStream.read"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_io_output_stream_write) {
    CBMFileResult *r = cov_extract(
        "import java.io.OutputStream;\n"
        "public class Main { public void f(OutputStream o) throws java.io.IOException { o.write(1); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "OutputStream.write"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_io_buffered_writer) {
    CBMFileResult *r = cov_extract(
        "import java.io.BufferedWriter;\n"
        "public class Main { public void f(BufferedWriter w) throws java.io.IOException {\n"
        "  w.write(\"x\"); w.newLine(); w.flush();\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "BufferedWriter.newLine"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_io_file_construction) {
    CBMFileResult *r = cov_extract(
        "import java.io.File;\n"
        "public class Main { public File f() { return new File(\"x\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "File"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_io_file_listFiles) {
    CBMFileResult *r = cov_extract(
        "import java.io.File;\n"
        "public class Main { public File[] f(File d) { return d.listFiles(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "File.listFiles"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_io_file_canRead) {
    CBMFileResult *r = cov_extract(
        "import java.io.File;\n"
        "public class Main { public boolean f(File d) { return d.canRead(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "File.canRead"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_io_file_to_path) {
    CBMFileResult *r = cov_extract(
        "import java.io.File;\n"
        "public class Main { public java.nio.file.Path f(File d) { return d.toPath(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "File.toPath"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_io_reader_ready) {
    CBMFileResult *r = cov_extract(
        "import java.io.Reader;\n"
        "public class Main { public boolean f(Reader r) throws java.io.IOException { return r.ready(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Reader.ready"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_io_writer_close) {
    CBMFileResult *r = cov_extract(
        "import java.io.Writer;\n"
        "public class Main { public void f(Writer w) throws java.io.IOException { w.close(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Writer.close"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 17. java.nio.file ──────────────────────────────────────────── */

TEST(cov_nio_path_resolve) {
    CBMFileResult *r = cov_extract(
        "import java.nio.file.Path;\n"
        "public class Main { public Path f(Path p) { return p.resolve(\"x\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Path.resolve"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_nio_path_getFileName) {
    CBMFileResult *r = cov_extract(
        "import java.nio.file.Path;\n"
        "public class Main { public Path f(Path p) { return p.getFileName(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Path.getFileName"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_nio_files_exists) {
    CBMFileResult *r = cov_extract(
        "import java.nio.file.Files;\n"
        "import java.nio.file.Path;\n"
        "public class Main { public boolean f(Path p) { return Files.exists(p); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Files.exists"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_nio_files_walk) {
    CBMFileResult *r = cov_extract(
        "import java.nio.file.Files;\n"
        "import java.nio.file.Path;\n"
        "public class Main { public Object f(Path p) throws java.io.IOException { return Files.walk(p); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Files.walk"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_nio_files_lines) {
    CBMFileResult *r = cov_extract(
        "import java.nio.file.Files;\n"
        "import java.nio.file.Path;\n"
        "public class Main { public Object f(Path p) throws java.io.IOException { return Files.lines(p); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Files.lines"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 18. java.util.stream ────────────────────────────────────────── */

TEST(cov_stream_sorted) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public Object f(ArrayList<String> xs) { return xs.stream().sorted().count(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Stream.sorted"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_stream_distinct) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public Object f(ArrayList<String> xs) { return xs.stream().distinct().count(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Stream.distinct"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_stream_limit) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public Object f(ArrayList<String> xs) { return xs.stream().limit(5).count(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Stream.limit"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_stream_skip) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public Object f(ArrayList<String> xs) { return xs.stream().skip(2).count(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Stream.skip"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_stream_min) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public Object f(ArrayList<String> xs) { return xs.stream().min((a,b) -> a.compareTo(b)); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Stream.min"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_stream_findFirst) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public Object f(ArrayList<String> xs) { return xs.stream().findFirst(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Stream.findFirst"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_stream_allMatch) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public boolean f(ArrayList<String> xs) { return xs.stream().allMatch(s -> s.length() > 0); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Stream.allMatch"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_stream_collect_toSet) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "import java.util.stream.Collectors;\n"
        "public class Main { public Object f(ArrayList<String> xs) { return xs.stream().collect(Collectors.toSet()); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Collectors.toSet"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_stream_collect_groupingBy) {
    /* The lambda inside Collectors.groupingBy is typed via Function<? super
     * T, ? extends K>, where T comes from the OUTER stream's element type
     * — that bidirectional flow isn't currently modeled (the LSP looks at
     * the static method's static signature only). Resolving Collectors.
     * groupingBy itself is the test we keep; lambda body resolution is a
     * known gap. */
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "import java.util.stream.Collectors;\n"
        "public class Main { public Object f(ArrayList<String> xs) {\n"
        "  return xs.stream().collect(Collectors.groupingBy(s -> s.length()));\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Collectors.groupingBy"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_intstream_range) {
    CBMFileResult *r = cov_extract(
        "import java.util.stream.IntStream;\n"
        "public class Main { public int f() { return IntStream.range(0, 10).sum(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "IntStream.range"), 0);
    ASSERT_GTE(cov_require(r, "f", "IntStream.sum"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 19. java.util.function ──────────────────────────────────────── */

TEST(cov_fn_unaryoperator) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.UnaryOperator;\n"
        "public class Main { public Object f(UnaryOperator<String> op) { return op.apply(\"x\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "UnaryOperator.apply"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fn_binaryoperator) {
    /* BinaryOperator<T> extends BiFunction<T,T,T>, so apply() resolves to
     * BiFunction.apply (correct per JLS §8.4.8). Either callee is OK. */
    CBMFileResult *r = cov_extract(
        "import java.util.function.BinaryOperator;\n"
        "public class Main { public Object f(BinaryOperator<String> op) { return op.apply(\"x\", \"y\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "apply"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fn_consumer_andThen) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Consumer;\n"
        "public class Main { public Object f(Consumer<String> c1, Consumer<String> c2) { return c1.andThen(c2); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Consumer.andThen"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fn_predicate_and) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Predicate;\n"
        "public class Main { public Object f(Predicate<String> p1, Predicate<String> p2) { return p1.and(p2); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Predicate.and"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fn_predicate_negate) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Predicate;\n"
        "public class Main { public Object f(Predicate<String> p) { return p.negate(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Predicate.negate"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fn_function_andThen) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.Function;\n"
        "public class Main { public Object f(Function<String,Integer> f1, Function<Integer,String> f2) {\n"
        "  return f1.andThen(f2);\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Function.andThen"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fn_intpredicate) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.IntPredicate;\n"
        "public class Main { public boolean f(IntPredicate p) { return p.test(1); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "IntPredicate.test"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fn_toint_function) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.ToIntFunction;\n"
        "public class Main { public int f(ToIntFunction<String> fn) { return fn.applyAsInt(\"x\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "ToIntFunction.applyAsInt"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fn_int_supplier) {
    CBMFileResult *r = cov_extract(
        "import java.util.function.IntSupplier;\n"
        "public class Main { public int f(IntSupplier s) { return s.getAsInt(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "IntSupplier.getAsInt"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_fn_runnable) {
    CBMFileResult *r = cov_extract(
        "public class Main { public void f(Runnable r) { r.run(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Runnable.run"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 20. java.util.concurrent ───────────────────────────────────── */

TEST(cov_conc_atomic_get) {
    CBMFileResult *r = cov_extract(
        "import java.util.concurrent.atomic.AtomicInteger;\n"
        "public class Main { public int f(AtomicInteger a) { return a.get(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "AtomicInteger.get"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_conc_atomic_compareAndSet) {
    CBMFileResult *r = cov_extract(
        "import java.util.concurrent.atomic.AtomicInteger;\n"
        "public class Main { public boolean f(AtomicInteger a) { return a.compareAndSet(0, 1); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "AtomicInteger.compareAndSet"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_conc_atomic_long) {
    CBMFileResult *r = cov_extract(
        "import java.util.concurrent.atomic.AtomicLong;\n"
        "public class Main { public long f(AtomicLong a) { return a.incrementAndGet(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "AtomicLong.incrementAndGet"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_conc_atomic_reference) {
    CBMFileResult *r = cov_extract(
        "import java.util.concurrent.atomic.AtomicReference;\n"
        "public class Main { public Object f(AtomicReference<String> a) { return a.get(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "AtomicReference.get"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_conc_executor_singleThread) {
    CBMFileResult *r = cov_extract(
        "import java.util.concurrent.ExecutorService;\n"
        "import java.util.concurrent.Executors;\n"
        "public class Main { public ExecutorService f() { return Executors.newSingleThreadExecutor(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Executors.newSingleThreadExecutor"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_conc_executor_cached) {
    CBMFileResult *r = cov_extract(
        "import java.util.concurrent.ExecutorService;\n"
        "import java.util.concurrent.Executors;\n"
        "public class Main { public ExecutorService f() { return Executors.newCachedThreadPool(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Executors.newCachedThreadPool"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_conc_future_get) {
    CBMFileResult *r = cov_extract(
        "import java.util.concurrent.Future;\n"
        "public class Main { public Object f(Future<String> fut) throws Exception { return fut.get(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Future.get"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_conc_completable_supplyAsync) {
    CBMFileResult *r = cov_extract(
        "import java.util.concurrent.CompletableFuture;\n"
        "public class Main { public CompletableFuture<String> f() {\n"
        "  return CompletableFuture.supplyAsync(() -> \"x\");\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "CompletableFuture.supplyAsync"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_conc_concurrent_hashmap) {
    CBMFileResult *r = cov_extract(
        "import java.util.concurrent.ConcurrentHashMap;\n"
        "public class Main { public Object f(ConcurrentHashMap<String,Integer> m) { return m.get(\"k\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "ConcurrentHashMap.get"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_conc_reentrant_lock) {
    CBMFileResult *r = cov_extract(
        "import java.util.concurrent.locks.ReentrantLock;\n"
        "public class Main { public void f(ReentrantLock l) { l.lock(); l.unlock(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "ReentrantLock.lock"), 0);
    ASSERT_GTE(cov_require(r, "f", "ReentrantLock.unlock"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 21. java.time ───────────────────────────────────────────────── */

TEST(cov_time_localdate_plus) {
    CBMFileResult *r = cov_extract(
        "import java.time.LocalDate;\n"
        "public class Main { public LocalDate f(LocalDate d) { return d.plusDays(1); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "LocalDate.plusDays"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_time_localdate_isAfter) {
    CBMFileResult *r = cov_extract(
        "import java.time.LocalDate;\n"
        "public class Main { public boolean f(LocalDate a, LocalDate b) { return a.isAfter(b); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "LocalDate.isAfter"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_time_localdatetime_now) {
    CBMFileResult *r = cov_extract(
        "import java.time.LocalDateTime;\n"
        "public class Main { public LocalDateTime f() { return LocalDateTime.now(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "LocalDateTime.now"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_time_instant_now) {
    CBMFileResult *r = cov_extract(
        "import java.time.Instant;\n"
        "public class Main { public Instant f() { return Instant.now(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Instant.now"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_time_duration_between) {
    CBMFileResult *r = cov_extract(
        "import java.time.Duration;\n"
        "import java.time.Instant;\n"
        "public class Main { public Duration f(Instant a, Instant b) { return Duration.between(a, b); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Duration.between"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_time_duration_minutes) {
    CBMFileResult *r = cov_extract(
        "import java.time.Duration;\n"
        "public class Main { public long f(Duration d) { return d.toMinutes(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Duration.toMinutes"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_time_zoneid_systemDefault) {
    CBMFileResult *r = cov_extract(
        "import java.time.ZoneId;\n"
        "public class Main { public ZoneId f() { return ZoneId.systemDefault(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "ZoneId.systemDefault"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_time_localdate_format) {
    CBMFileResult *r = cov_extract(
        "import java.time.LocalDate;\n"
        "import java.time.format.DateTimeFormatter;\n"
        "public class Main { public String f(LocalDate d, DateTimeFormatter fmt) { return d.format(fmt); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "LocalDate.format"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_time_formatter_ofPattern) {
    CBMFileResult *r = cov_extract(
        "import java.time.format.DateTimeFormatter;\n"
        "public class Main { public DateTimeFormatter f() { return DateTimeFormatter.ofPattern(\"yyyy\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "DateTimeFormatter.ofPattern"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_time_localdate_parse) {
    CBMFileResult *r = cov_extract(
        "import java.time.LocalDate;\n"
        "public class Main { public LocalDate f(String s) { return LocalDate.parse(s); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "LocalDate.parse"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 22. Imports ─────────────────────────────────────────────────── */

TEST(cov_imp_single_type) {
    CBMFileResult *r = cov_extract(
        "import java.util.HashMap;\n"
        "public class Main { public int f(HashMap<String,Integer> m) { return m.size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "HashMap.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_imp_on_demand) {
    CBMFileResult *r = cov_extract(
        "import java.util.*;\n"
        "public class Main { public int f(HashMap<String,Integer> m) { return m.size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "HashMap.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_imp_static_method) {
    CBMFileResult *r = cov_extract(
        "import static java.lang.Math.sqrt;\n"
        "public class Main { public double f() { return sqrt(2); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Math.sqrt"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_imp_static_on_demand) {
    CBMFileResult *r = cov_extract(
        "import static java.lang.Math.*;\n"
        "public class Main { public double f() { return PI; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_imp_multiple) {
    CBMFileResult *r = cov_extract(
        "import java.util.HashMap;\n"
        "import java.util.ArrayList;\n"
        "import java.util.List;\n"
        "public class Main {\n"
        "  public int f(HashMap<String,List<String>> m) { return m.size(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "HashMap.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_imp_qualified_no_import) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(java.util.HashMap<String,Integer> m) { return m.size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "HashMap.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_imp_with_package) {
    CBMFileResult *r = cov_extract(
        "package com.example;\n"
        "import java.util.ArrayList;\n"
        "public class Main { public int f(ArrayList<String> xs) { return xs.size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "ArrayList.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_imp_chained_packages) {
    CBMFileResult *r = cov_extract(
        "package a.b.c;\n"
        "import java.util.concurrent.atomic.AtomicInteger;\n"
        "public class Main { public int f(AtomicInteger a) { return a.get(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "AtomicInteger.get"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 23. Inner / nested types ────────────────────────────────────── */

TEST(cov_nest_static_inner) {
    CBMFileResult *r = cov_extract(
        "public class Main { static class A { String tag() { return \"a\"; } }\n"
        "  public String f() { return new A().tag(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "tag"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_nest_inner_3level) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class A { static class B { static class C { String tag() { return \"c\"; } } } }\n"
        "  public String f() { return new Main.A.B.C().tag(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_nest_interface) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  interface I { String tag(); }\n"
        "  static class A implements I { public String tag() { return \"a\"; } }\n"
        "  public String f() { I x = new A(); return x.tag(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "tag"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_nest_enum) {
    CBMFileResult *r = cov_extract(
        "public class Main { enum Color { RED, GREEN, BLUE } public Color f() { return Color.RED; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_nest_static_field) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class K { static String NAME = \"x\"; }\n"
        "  public int f() { return K.NAME.length(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_nest_outer_method_from_inner) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  String greet() { return \"hi\"; }\n"
        "  class Inner { public String f() { return greet(); } }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "greet"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_nest_outer_field_from_inner) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  private String tag = \"x\";\n"
        "  class Inner { public int f() { return tag.length(); } }\n"
        "}");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_nest_record) {
    CBMFileResult *r = cov_extract(
        "public class Main { public record Point(int x, int y) {} public int f() { return new Point(1,2).x(); } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_nest_static_member_access) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class Util { static String tag() { return \"u\"; } }\n"
        "  public String f() { return Util.tag(); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "tag"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_nest_constructor_call) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  static class Item { public Item(String s) {} }\n"
        "  public Item f() { return new Item(\"x\"); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Item"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 24. Variable / scope binding ────────────────────────────────── */

TEST(cov_bind_local_int) {
    CBMFileResult *r = cov_extract(
        "public class Main { public void f() { int a = 1; a++; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_bind_var_string) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { var s = \"x\"; return s.length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_bind_var_arraylist) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public int f() { var xs = new ArrayList<String>(); xs.add(\"x\"); return xs.size(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "ArrayList.add"), 0);
    ASSERT_GTE(cov_require(r, "f", "ArrayList.size"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_bind_multi_decl) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { int a = 1, b = 2, c = 3; return a + b + c; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_bind_final_local) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { final String s = \"x\"; return s.length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_bind_for_loop_init) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { int n = 0; for (int i = 0; i < 10; i++) n++; return n; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_bind_enhanced_for_array) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String[] xs) { int n = 0; for (String s : xs) n += s.length(); return n; } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_bind_enhanced_for_iterable) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public int f(ArrayList<String> xs) { int n = 0; for (String s : xs) n += s.length(); return n; } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_bind_catch_param) {
    CBMFileResult *r = cov_extract(
        "public class Main { public String f() {\n"
        "  try { return Integer.toString(Integer.parseInt(\"x\")); }\n"
        "  catch (NumberFormatException e) { return e.getMessage(); }\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Throwable.getMessage"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_bind_try_resources) {
    CBMFileResult *r = cov_extract(
        "import java.io.BufferedReader;\n"
        "import java.io.FileReader;\n"
        "public class Main { public String f(String path) throws java.io.IOException {\n"
        "  try (BufferedReader br = new BufferedReader(new FileReader(path))) { return br.readLine(); }\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "BufferedReader.readLine"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 25. Control flow ─────────────────────────────────────────────── */

TEST(cov_ctrl_if_call) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) { if (s.isEmpty()) return 0; else return s.length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.isEmpty"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_ctrl_for_call) {
    CBMFileResult *r = cov_extract(
        "import java.util.ArrayList;\n"
        "public class Main { public int f(ArrayList<String> xs) {\n"
        "  int n = 0;\n"
        "  for (int i = 0; i < xs.size(); i++) n += xs.get(i).length();\n"
        "  return n;\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "ArrayList.size"), 0);
    ASSERT_GTE(cov_require(r, "f", "ArrayList.get"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_ctrl_while_call) {
    CBMFileResult *r = cov_extract(
        "import java.util.Iterator;\n"
        "public class Main { public int f(Iterator<String> it) {\n"
        "  int n = 0;\n"
        "  while (it.hasNext()) n += it.next().length();\n"
        "  return n;\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Iterator.hasNext"), 0);
    ASSERT_GTE(cov_require(r, "f", "Iterator.next"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_ctrl_do_while) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) { int n = 0; do { n++; } while (n < s.length()); return n; } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_ctrl_switch_basic) {
    CBMFileResult *r = cov_extract(
        "public class Main { public String f(int x) {\n"
        "  switch (x) {\n"
        "    case 1: return \"one\".toUpperCase();\n"
        "    case 2: return \"two\".trim();\n"
        "    default: return \"\";\n"
        "  }\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.toUpperCase"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.trim"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_ctrl_try_finally) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) {\n"
        "  try { return s.length(); }\n"
        "  finally { s.toString(); }\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_ctrl_try_multi_catch) {
    CBMFileResult *r = cov_extract(
        "public class Main { public String f() {\n"
        "  try { return Integer.toString(Integer.parseInt(\"x\")); }\n"
        "  catch (NumberFormatException | NullPointerException e) { return e.getMessage(); }\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "Throwable.getMessage"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_ctrl_throw_in_method) {
    CBMFileResult *r = cov_extract(
        "public class Main { public void f() throws IllegalStateException { throw new IllegalStateException(\"x\"); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "IllegalStateException"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_ctrl_break_continue_no_crash) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(int[] xs) {\n"
        "  int n = 0;\n"
        "  for (int x : xs) { if (x < 0) continue; if (x > 100) break; n++; }\n"
        "  return n;\n"
        "} }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_ctrl_synchronized) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(Object lock, String s) { synchronized(lock) { return s.length(); } } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── 26. Diagnostics ─────────────────────────────────────────────── */

TEST(cov_diag_unknown_method) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) { return s.nonexistentMethod(); } }");
    ASSERT_NOT_NULL(r);
    int diag = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        if (r->resolved_calls.items[i].confidence == 0.0f) diag++;
    }
    ASSERT_GTE(diag, 1);
    cbm_free_result(r);
    PASS();
}
TEST(cov_diag_unknown_var) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { return undef.size(); } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_diag_unresolved_count) {
    CBMFileResult *r = cov_extract(
        "public class Main { public void f() {\n"
        "  unknownObj1.unknownMethod1();\n"
        "  unknownObj2.unknownMethod2();\n"
        "} }");
    ASSERT_NOT_NULL(r);
    int diag = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        if (r->resolved_calls.items[i].confidence == 0.0f) diag++;
    }
    ASSERT_GTE(diag, 1);
    cbm_free_result(r);
    PASS();
}
TEST(cov_diag_no_resolved_no_calls) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { return 1 + 2; } }");
    ASSERT_NOT_NULL(r);
    /* No calls, no resolved entries. */
    int strict = cov_count_strict(r);
    ASSERT_EQ(strict, 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_diag_unresolved_super_no_match) {
    CBMFileResult *r = cov_extract(
        "public class Main { public String f() { return super.nonexistent(); } }");
    ASSERT_NOT_NULL(r);
    int diag = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        if (r->resolved_calls.items[i].confidence == 0.0f) diag++;
    }
    ASSERT_GTE(diag, 1);
    cbm_free_result(r);
    PASS();
}
TEST(cov_diag_method_ref_unknown_lhs) {
    CBMFileResult *r = cov_extract(
        "public class Main { public Object f() { return undefined::method; } }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── 27. Cross-file API ──────────────────────────────────────────── */

TEST(cov_cross_basic) {
    const char *src =
        "package com.example;\n"
        "public class Caller { public String f(com.example.Greeter g) { return g.greet(\"x\"); } }";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    CBMLSPDef defs[2];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "com.example.Greeter";
    defs[0].short_name = "Greeter";
    defs[0].label = "Class";
    defs[0].def_module_qn = "com.example";
    defs[1].qualified_name = "com.example.Greeter.greet";
    defs[1].short_name = "greet";
    defs[1].label = "Method";
    defs[1].receiver_type = "com.example.Greeter";
    defs[1].return_types = "java.lang.String";
    const char *imp_names[] = {"Greeter"};
    const char *imp_qns[] = {"com.example.Greeter"};
    cbm_run_java_lsp_cross(&arena, src, (int)strlen(src), "test.com.example", defs, 2, imp_names,
                           imp_qns, 1, NULL, &out);
    int found = 0;
    for (int i = 0; i < out.count; i++) {
        if (out.items[i].confidence < 0.5f) continue;
        if (out.items[i].callee_qn && strstr(out.items[i].callee_qn, "Greeter.greet")) found = 1;
    }
    ASSERT_EQ(found, 1);
    cbm_arena_destroy(&arena);
    PASS();
}
TEST(cov_cross_no_imports) {
    const char *src = "public class A { public int f() { return 1; } }";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    cbm_run_java_lsp_cross(&arena, src, (int)strlen(src), "test.A", NULL, 0, NULL, NULL, 0, NULL, &out);
    /* Just a smoke test — no calls, no edges. */
    cbm_arena_destroy(&arena);
    PASS();
}
TEST(cov_cross_batch) {
    const char *src1 = "package p; public class A { public int f() { return 1; } }";
    const char *src2 = "package p; public class B { public int f() { return 2; } }";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMBatchJavaLSPFile files[2];
    memset(files, 0, sizeof(files));
    files[0].source = src1;
    files[0].source_len = (int)strlen(src1);
    files[0].module_qn = "test.A";
    files[1].source = src2;
    files[1].source_len = (int)strlen(src2);
    files[1].module_qn = "test.B";
    CBMResolvedCallArray outs[2];
    memset(outs, 0, sizeof(outs));
    cbm_batch_java_lsp_cross(&arena, files, 2, outs);
    cbm_arena_destroy(&arena);
    PASS();
}
TEST(cov_cross_inheritance) {
    const char *src =
        "package x;\n"
        "public class B { public String f(x.A a) { return a.tag(); } }";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    CBMLSPDef defs[2];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "x.A";
    defs[0].short_name = "A";
    defs[0].label = "Class";
    defs[1].qualified_name = "x.A.tag";
    defs[1].short_name = "tag";
    defs[1].label = "Method";
    defs[1].receiver_type = "x.A";
    defs[1].return_types = "java.lang.String";
    cbm_run_java_lsp_cross(&arena, src, (int)strlen(src), "test.B", defs, 2, NULL, NULL, 0, NULL, &out);
    int found = 0;
    for (int i = 0; i < out.count; i++) {
        if (out.items[i].confidence < 0.5f) continue;
        if (out.items[i].callee_qn && strstr(out.items[i].callee_qn, "A.tag")) found = 1;
    }
    ASSERT_EQ(found, 1);
    cbm_arena_destroy(&arena);
    PASS();
}

/* ── 28. Stress / edge cases ─────────────────────────────────────── */

TEST(cov_edge_empty_method) {
    CBMFileResult *r = cov_extract("public class Main { public void f() {} }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_edge_empty_class) {
    CBMFileResult *r = cov_extract("public class Main {}");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_edge_long_chain) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) {\n"
        "  return s.trim().toLowerCase().toUpperCase().substring(0).length();\n"
        "} }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.trim"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.toLowerCase"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.toUpperCase"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.substring"), 0);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_edge_recursive_user_method) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int fib(int n) { return n < 2 ? n : fib(n-1) + fib(n-2); } }");
    ASSERT_NOT_NULL(r);
    int fib_count = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->confidence >= 0.5f && rc->callee_qn && strstr(rc->callee_qn, "fib")) fib_count++;
    }
    ASSERT_GTE(fib_count, 2);
    cbm_free_result(r);
    PASS();
}
TEST(cov_edge_mutual_recursion) {
    CBMFileResult *r = cov_extract(
        "public class Main {\n"
        "  public boolean isEven(int n) { return n == 0 || isOdd(n-1); }\n"
        "  public boolean isOdd(int n) { return n != 0 && isEven(n-1); }\n"
        "}");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "isEven", "isOdd"), 0);
    ASSERT_GTE(cov_require(r, "isOdd", "isEven"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_edge_deeply_nested_blocks) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f(String s) {\n"
        "  if (s != null) {\n"
        "    if (!s.isEmpty()) {\n"
        "      if (s.length() > 5) {\n"
        "        return s.length();\n"
        "      }\n"
        "    }\n"
        "  }\n"
        "  return 0;\n"
        "} }");
    ASSERT_NOT_NULL(r);
    int len_calls = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->confidence >= 0.5f && rc->callee_qn && strstr(rc->callee_qn, "String.length")) {
            len_calls++;
        }
    }
    ASSERT_GTE(len_calls, 2);
    cbm_free_result(r);
    PASS();
}
TEST(cov_edge_anonymous_class_no_crash) {
    CBMFileResult *r = cov_extract(
        "public class Main { public Runnable f() {\n"
        "  return new Runnable() { public void run() { System.out.println(\"x\"); } };\n"
        "} }");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_edge_unicode_string) {
    CBMFileResult *r = cov_extract(
        "public class Main { public int f() { return \"\\u00e9\".length(); } }");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}
TEST(cov_edge_huge_class_no_crash) {
    char buf[8192];
    int len = snprintf(buf, sizeof(buf), "public class Main {\n");
    for (int i = 0; i < 50 && len < (int)sizeof(buf) - 200; i++) {
        len += snprintf(buf + len, sizeof(buf) - len,
                         "  public int m%d(int x) { return x + %d; }\n", i, i);
    }
    snprintf(buf + len, sizeof(buf) - len, "}\n");
    CBMFileResult *r = cov_extract(buf);
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}
TEST(cov_edge_unusual_whitespace) {
    CBMFileResult *r = cov_extract(
        "public  class   Main {\npublic\tint    f(String\ts) { return s.length(); }\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(cov_require(r, "f", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Suite registration ──────────────────────────────────────────── */

void suite_java_lsp_coverage(void) {
    /* 1. Literals */
    RUN_TEST(cov_lit_int);
    RUN_TEST(cov_lit_long);
    RUN_TEST(cov_lit_hex);
    RUN_TEST(cov_lit_binary);
    RUN_TEST(cov_lit_octal);
    RUN_TEST(cov_lit_float_suffix);
    RUN_TEST(cov_lit_double);
    RUN_TEST(cov_lit_char);
    RUN_TEST(cov_lit_string_method);
    RUN_TEST(cov_lit_bool_method);
    RUN_TEST(cov_lit_null_no_crash);

    /* 2. Type-name resolution */
    RUN_TEST(cov_typename_java_lang_implicit);
    RUN_TEST(cov_typename_via_single_import);
    RUN_TEST(cov_typename_via_on_demand_import);
    RUN_TEST(cov_typename_via_static_import);
    RUN_TEST(cov_typename_qualified_path);
    RUN_TEST(cov_typename_inner_via_outer);
    RUN_TEST(cov_typename_nested_two_levels);
    RUN_TEST(cov_typename_array_type);
    RUN_TEST(cov_typename_2d_array);
    RUN_TEST(cov_typename_generic_type);
    RUN_TEST(cov_typename_wildcard);
    RUN_TEST(cov_typename_bounded_wildcard);

    /* 3. Primitive operations */
    RUN_TEST(cov_op_arith);
    RUN_TEST(cov_op_compare);
    RUN_TEST(cov_op_bitwise);
    RUN_TEST(cov_op_unary_negate);
    RUN_TEST(cov_op_logical_not);
    RUN_TEST(cov_op_increment);
    RUN_TEST(cov_op_string_concat);
    RUN_TEST(cov_op_ternary_simple);
    RUN_TEST(cov_op_assignment);
    RUN_TEST(cov_op_paren_expr);

    /* 4. Type-AST kinds */
    RUN_TEST(cov_typeast_void_method);
    RUN_TEST(cov_typeast_byte_param);
    RUN_TEST(cov_typeast_short_param);
    RUN_TEST(cov_typeast_int_param);
    RUN_TEST(cov_typeast_long_param);
    RUN_TEST(cov_typeast_float_param);
    RUN_TEST(cov_typeast_double_param);
    RUN_TEST(cov_typeast_char_param);
    RUN_TEST(cov_typeast_boolean_param);
    RUN_TEST(cov_typeast_string_param);
    RUN_TEST(cov_typeast_array_of_string);
    RUN_TEST(cov_typeast_generic_list);

    /* 5. Identifier resolution */
    RUN_TEST(cov_id_local_var);
    RUN_TEST(cov_id_param);
    RUN_TEST(cov_id_field);
    RUN_TEST(cov_id_inherited_field);
    RUN_TEST(cov_id_shadow_param);
    RUN_TEST(cov_id_block_shadow);
    RUN_TEST(cov_id_outer_class_field);
    RUN_TEST(cov_id_class_as_static);
    RUN_TEST(cov_id_unresolved_emits_diagnostic);
    RUN_TEST(cov_id_for_loop_var);

    /* 6. Field access */
    RUN_TEST(cov_fa_system_out);
    RUN_TEST(cov_fa_system_err);
    RUN_TEST(cov_fa_array_length);
    RUN_TEST(cov_fa_chained_field_method);
    RUN_TEST(cov_fa_this_field);
    RUN_TEST(cov_fa_static_class_field);
    RUN_TEST(cov_fa_method_then_field);
    RUN_TEST(cov_fa_field_through_param);
    RUN_TEST(cov_fa_field_through_chained_call);
    RUN_TEST(cov_fa_unresolved_field_no_crash);

    /* 7. Method invocation */
    RUN_TEST(cov_mi_bare_local);
    RUN_TEST(cov_mi_this);
    RUN_TEST(cov_mi_super);
    RUN_TEST(cov_mi_static);
    RUN_TEST(cov_mi_chained_static);
    RUN_TEST(cov_mi_instance);
    RUN_TEST(cov_mi_chain_3hop);
    RUN_TEST(cov_mi_recursive_self);
    RUN_TEST(cov_mi_overload_by_arity);
    RUN_TEST(cov_mi_param_chain);
    RUN_TEST(cov_mi_return_chain);
    RUN_TEST(cov_mi_args_walked);
    RUN_TEST(cov_mi_nested_args);
    RUN_TEST(cov_mi_through_field_getter);
    RUN_TEST(cov_mi_call_in_condition);

    /* 8. Object creation */
    RUN_TEST(cov_oc_simple);
    RUN_TEST(cov_oc_with_args);
    RUN_TEST(cov_oc_diamond);
    RUN_TEST(cov_oc_generic_explicit);
    RUN_TEST(cov_oc_user_class);
    RUN_TEST(cov_oc_with_chained_method);
    RUN_TEST(cov_oc_array);
    RUN_TEST(cov_oc_array_init);

    /* 9. Cast / instanceof */
    RUN_TEST(cov_cast_to_string);
    RUN_TEST(cov_cast_chained);
    RUN_TEST(cov_cast_primitive);
    RUN_TEST(cov_instanceof_basic);
    RUN_TEST(cov_cast_from_iterator);
    RUN_TEST(cov_cast_arr_elem);
    RUN_TEST(cov_cast_in_ternary);
    RUN_TEST(cov_cast_then_chain);

    /* 10. Generic substitution */
    RUN_TEST(cov_gen_list_get);
    RUN_TEST(cov_gen_arraylist_get);
    RUN_TEST(cov_gen_linkedlist_first);
    RUN_TEST(cov_gen_set_iterator_next);
    RUN_TEST(cov_gen_map_get);
    RUN_TEST(cov_gen_hashmap_get);
    RUN_TEST(cov_gen_optional_orElse);
    RUN_TEST(cov_gen_optional_get);
    RUN_TEST(cov_gen_iterator_next_chain);
    RUN_TEST(cov_gen_function_apply);
    RUN_TEST(cov_gen_supplier_get);
    RUN_TEST(cov_gen_bifunction_apply);
    RUN_TEST(cov_gen_map_keyset);
    RUN_TEST(cov_gen_map_values);
    RUN_TEST(cov_gen_collection_iterator);

    /* 11. Lambda SAM inference */
    RUN_TEST(cov_lambda_function);
    RUN_TEST(cov_lambda_predicate_filter);
    RUN_TEST(cov_lambda_consumer_forEach);
    RUN_TEST(cov_lambda_function_map);
    RUN_TEST(cov_lambda_user_consumer);
    RUN_TEST(cov_lambda_user_function);
    RUN_TEST(cov_lambda_user_predicate);
    RUN_TEST(cov_lambda_block_body);
    RUN_TEST(cov_lambda_bipredicate);
    RUN_TEST(cov_lambda_biconsumer_map);
    RUN_TEST(cov_lambda_anyMatch);
    RUN_TEST(cov_lambda_removeIf);

    /* 12. Method references */
    RUN_TEST(cov_mref_static);
    RUN_TEST(cov_mref_instance_method);
    RUN_TEST(cov_mref_static_class_method);
    RUN_TEST(cov_mref_constructor_simple);
    RUN_TEST(cov_mref_in_local_var);
    RUN_TEST(cov_mref_with_chain);
    RUN_TEST(cov_mref_user_class);
    RUN_TEST(cov_mref_user_constructor);

    /* 13. Inheritance */
    RUN_TEST(cov_inh_basic);
    RUN_TEST(cov_inh_3level);
    RUN_TEST(cov_inh_4level);
    RUN_TEST(cov_inh_override);
    RUN_TEST(cov_inh_super_after_override);
    RUN_TEST(cov_inh_via_object);
    RUN_TEST(cov_inh_interface_default);
    RUN_TEST(cov_inh_interface_via_param);
    RUN_TEST(cov_inh_collection_to_list);
    RUN_TEST(cov_inh_list_iterable_iterator);

    /* 14. java.lang */
    RUN_TEST(cov_lang_object_hashCode);
    RUN_TEST(cov_lang_object_equals);
    RUN_TEST(cov_lang_object_getClass);
    RUN_TEST(cov_lang_string_repeat);
    RUN_TEST(cov_lang_string_split);
    RUN_TEST(cov_lang_string_indexOf);
    RUN_TEST(cov_lang_string_substring);
    RUN_TEST(cov_lang_math_pow);
    RUN_TEST(cov_lang_math_log);
    RUN_TEST(cov_lang_math_random);
    RUN_TEST(cov_lang_system_nano);
    RUN_TEST(cov_lang_system_getenv);
    RUN_TEST(cov_lang_thread_sleep);
    RUN_TEST(cov_lang_class_forName);
    RUN_TEST(cov_lang_throwable_getCause);

    /* 15. java.util */
    RUN_TEST(cov_util_arraylist_add);
    RUN_TEST(cov_util_arraylist_remove);
    RUN_TEST(cov_util_linkedlist_addFirst);
    RUN_TEST(cov_util_hashset_add);
    RUN_TEST(cov_util_treeset_first);
    RUN_TEST(cov_util_hashmap_put);
    RUN_TEST(cov_util_treemap_firstKey);
    RUN_TEST(cov_util_arrays_sort);
    RUN_TEST(cov_util_collections_unmodifiable);
    RUN_TEST(cov_util_objects_isNull);
    RUN_TEST(cov_util_optional_isPresent);
    RUN_TEST(cov_util_optional_map);
    RUN_TEST(cov_util_uuid_random);
    RUN_TEST(cov_util_random_nextInt);
    RUN_TEST(cov_util_scanner_nextLine);

    /* 16. java.io */
    RUN_TEST(cov_io_print_writer);
    RUN_TEST(cov_io_input_stream_read);
    RUN_TEST(cov_io_output_stream_write);
    RUN_TEST(cov_io_buffered_writer);
    RUN_TEST(cov_io_file_construction);
    RUN_TEST(cov_io_file_listFiles);
    RUN_TEST(cov_io_file_canRead);
    RUN_TEST(cov_io_file_to_path);
    RUN_TEST(cov_io_reader_ready);
    RUN_TEST(cov_io_writer_close);

    /* 17. java.nio.file */
    RUN_TEST(cov_nio_path_resolve);
    RUN_TEST(cov_nio_path_getFileName);
    RUN_TEST(cov_nio_files_exists);
    RUN_TEST(cov_nio_files_walk);
    RUN_TEST(cov_nio_files_lines);

    /* 18. java.util.stream */
    RUN_TEST(cov_stream_sorted);
    RUN_TEST(cov_stream_distinct);
    RUN_TEST(cov_stream_limit);
    RUN_TEST(cov_stream_skip);
    RUN_TEST(cov_stream_min);
    RUN_TEST(cov_stream_findFirst);
    RUN_TEST(cov_stream_allMatch);
    RUN_TEST(cov_stream_collect_toSet);
    RUN_TEST(cov_stream_collect_groupingBy);
    RUN_TEST(cov_intstream_range);

    /* 19. java.util.function */
    RUN_TEST(cov_fn_unaryoperator);
    RUN_TEST(cov_fn_binaryoperator);
    RUN_TEST(cov_fn_consumer_andThen);
    RUN_TEST(cov_fn_predicate_and);
    RUN_TEST(cov_fn_predicate_negate);
    RUN_TEST(cov_fn_function_andThen);
    RUN_TEST(cov_fn_intpredicate);
    RUN_TEST(cov_fn_toint_function);
    RUN_TEST(cov_fn_int_supplier);
    RUN_TEST(cov_fn_runnable);

    /* 20. java.util.concurrent */
    RUN_TEST(cov_conc_atomic_get);
    RUN_TEST(cov_conc_atomic_compareAndSet);
    RUN_TEST(cov_conc_atomic_long);
    RUN_TEST(cov_conc_atomic_reference);
    RUN_TEST(cov_conc_executor_singleThread);
    RUN_TEST(cov_conc_executor_cached);
    RUN_TEST(cov_conc_future_get);
    RUN_TEST(cov_conc_completable_supplyAsync);
    RUN_TEST(cov_conc_concurrent_hashmap);
    RUN_TEST(cov_conc_reentrant_lock);

    /* 21. java.time */
    RUN_TEST(cov_time_localdate_plus);
    RUN_TEST(cov_time_localdate_isAfter);
    RUN_TEST(cov_time_localdatetime_now);
    RUN_TEST(cov_time_instant_now);
    RUN_TEST(cov_time_duration_between);
    RUN_TEST(cov_time_duration_minutes);
    RUN_TEST(cov_time_zoneid_systemDefault);
    RUN_TEST(cov_time_localdate_format);
    RUN_TEST(cov_time_formatter_ofPattern);
    RUN_TEST(cov_time_localdate_parse);

    /* 22. Imports */
    RUN_TEST(cov_imp_single_type);
    RUN_TEST(cov_imp_on_demand);
    RUN_TEST(cov_imp_static_method);
    RUN_TEST(cov_imp_static_on_demand);
    RUN_TEST(cov_imp_multiple);
    RUN_TEST(cov_imp_qualified_no_import);
    RUN_TEST(cov_imp_with_package);
    RUN_TEST(cov_imp_chained_packages);

    /* 23. Inner/nested */
    RUN_TEST(cov_nest_static_inner);
    RUN_TEST(cov_nest_inner_3level);
    RUN_TEST(cov_nest_interface);
    RUN_TEST(cov_nest_enum);
    RUN_TEST(cov_nest_static_field);
    RUN_TEST(cov_nest_outer_method_from_inner);
    RUN_TEST(cov_nest_outer_field_from_inner);
    RUN_TEST(cov_nest_record);
    RUN_TEST(cov_nest_static_member_access);
    RUN_TEST(cov_nest_constructor_call);

    /* 24. Variable / scope binding */
    RUN_TEST(cov_bind_local_int);
    RUN_TEST(cov_bind_var_string);
    RUN_TEST(cov_bind_var_arraylist);
    RUN_TEST(cov_bind_multi_decl);
    RUN_TEST(cov_bind_final_local);
    RUN_TEST(cov_bind_for_loop_init);
    RUN_TEST(cov_bind_enhanced_for_array);
    RUN_TEST(cov_bind_enhanced_for_iterable);
    RUN_TEST(cov_bind_catch_param);
    RUN_TEST(cov_bind_try_resources);

    /* 25. Control flow */
    RUN_TEST(cov_ctrl_if_call);
    RUN_TEST(cov_ctrl_for_call);
    RUN_TEST(cov_ctrl_while_call);
    RUN_TEST(cov_ctrl_do_while);
    RUN_TEST(cov_ctrl_switch_basic);
    RUN_TEST(cov_ctrl_try_finally);
    RUN_TEST(cov_ctrl_try_multi_catch);
    RUN_TEST(cov_ctrl_throw_in_method);
    RUN_TEST(cov_ctrl_break_continue_no_crash);
    RUN_TEST(cov_ctrl_synchronized);

    /* 26. Diagnostics */
    RUN_TEST(cov_diag_unknown_method);
    RUN_TEST(cov_diag_unknown_var);
    RUN_TEST(cov_diag_unresolved_count);
    RUN_TEST(cov_diag_no_resolved_no_calls);
    RUN_TEST(cov_diag_unresolved_super_no_match);
    RUN_TEST(cov_diag_method_ref_unknown_lhs);

    /* 27. Cross-file */
    RUN_TEST(cov_cross_basic);
    RUN_TEST(cov_cross_no_imports);
    RUN_TEST(cov_cross_batch);
    RUN_TEST(cov_cross_inheritance);

    /* 28. Stress / edge */
    RUN_TEST(cov_edge_empty_method);
    RUN_TEST(cov_edge_empty_class);
    RUN_TEST(cov_edge_long_chain);
    RUN_TEST(cov_edge_recursive_user_method);
    RUN_TEST(cov_edge_mutual_recursion);
    RUN_TEST(cov_edge_deeply_nested_blocks);
    RUN_TEST(cov_edge_anonymous_class_no_crash);
    RUN_TEST(cov_edge_unicode_string);
    RUN_TEST(cov_edge_huge_class_no_crash);
    RUN_TEST(cov_edge_unusual_whitespace);

    (void)cov_no_crash;  /* silence unused-static warning if any */
}
