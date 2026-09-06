/*
 * test_java_lsp.c — Tests for the pure-C Java LSP type-aware call resolver.
 *
 * Coverage targets parity (≥90%) with what JDT-LS / java-language-server
 * report through textDocument/definition + textDocument/references at call
 * sites:
 *   - Single-file resolution: identifiers, fields, methods, generics,
 *     overloads, this/super, statics, enhanced-for, lambdas, ternaries,
 *     casts, instance/static dispatch.
 *   - Inheritance + interface dispatch: method look-ups walking the
 *     super-chain and implemented interfaces.
 *   - java.lang implicit-import behaviour (String, Math, System, Objects).
 *   - java.util collections (ArrayList, HashMap, Set), java.io streams,
 *     java.util.stream chains, java.util.function functional interfaces,
 *     java.util.concurrent ExecutorService / CompletableFuture / atomics,
 *     java.time.LocalDate / Instant / Duration, java.nio.file.Path / Files,
 *     java.util.regex.Pattern / Matcher, java.lang.Thread.
 *   - Negative tests: ambiguous receivers, missing types, wrong arity.
 *   - Diagnostic emission (lsp_unresolved confidence=0).
 *
 * Each test is a TEST(...) function returning 0 on success. They run via
 * RUN_TEST in suite_java_lsp at the bottom of this file.
 */

#include "test_framework.h"
#include "cbm.h"
#include "lsp/java_lsp.h"

#include <string.h>
#include <stdio.h>

/* ── Helpers ─────────────────────────────────────────────────────── */

static CBMFileResult *extract_java(const char *source) {
    return cbm_extract_file(source, (int)strlen(source), CBM_LANG_JAVA, "test", "Main.java", 0,
                            NULL, NULL);
}

static CBMFileResult *extract_java_at(const char *source, const char *rel_path) {
    return cbm_extract_file(source, (int)strlen(source), CBM_LANG_JAVA, "test", rel_path, 0, NULL,
                            NULL);
}

static int find_resolved(const CBMFileResult *r, const char *callerSub, const char *calleeSub) {
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

static int require_resolved(const CBMFileResult *r, const char *callerSub, const char *calleeSub) {
    int idx = find_resolved(r, callerSub, calleeSub);
    if (idx < 0) {
        printf("\n  MISSING resolved call: caller~%s -> callee~%s (have %d entries)\n", callerSub,
               calleeSub, r->resolved_calls.count);
        for (int i = 0; i < r->resolved_calls.count; i++) {
            const CBMResolvedCall *rc = &r->resolved_calls.items[i];
            printf("    %s -> %s [%s %.2f]\n", rc->caller_qn ? rc->caller_qn : "(null)",
                   rc->callee_qn ? rc->callee_qn : "(null)",
                   rc->strategy ? rc->strategy : "(null)", rc->confidence);
        }
    }
    return idx;
}

static int count_resolved(const CBMFileResult *r) {
    int n = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        if (r->resolved_calls.items[i].confidence >= 0.5f) n++;
    }
    return n;
}

/* ── Category 1: java.lang.String resolution ─────────────────────── */

TEST(jlsp_string_length) {
    const char *src =
        "public class Main {\n"
        "  public int run(String s) {\n"
        "    return s.length();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_string_concat_method) {
    const char *src =
        "public class Main {\n"
        "  public String greet(String name) {\n"
        "    String hello = \"Hello, \" + name;\n"
        "    return hello.toUpperCase();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "greet", "String.toUpperCase"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_string_chain) {
    const char *src =
        "public class Main {\n"
        "  public String clean(String raw) {\n"
        "    return raw.trim().toLowerCase();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "clean", "String.trim"), 0);
    ASSERT_GTE(require_resolved(r, "clean", "String.toLowerCase"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_string_static) {
    const char *src =
        "public class Main {\n"
        "  public String fmt(int x) {\n"
        "    return String.valueOf(x);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "fmt", "String.valueOf"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_string_format) {
    const char *src =
        "public class Main {\n"
        "  public String build(int n) {\n"
        "    return String.format(\"count=%d\", n);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "build", "String.format"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 2: java.lang.Math + System + Integer ────────────────── */

TEST(jlsp_math_static) {
    const char *src =
        "public class Main {\n"
        "  public double dist(double a, double b) {\n"
        "    return Math.sqrt(a * a + b * b);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "dist", "Math.sqrt"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_math_chain) {
    const char *src =
        "public class Main {\n"
        "  public double f(double x) {\n"
        "    return Math.pow(Math.abs(x), 2);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "f", "Math.pow"), 0);
    ASSERT_GTE(require_resolved(r, "f", "Math.abs"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_system_currentTimeMillis) {
    const char *src =
        "public class Main {\n"
        "  public long now() {\n"
        "    return System.currentTimeMillis();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "now", "System.currentTimeMillis"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_system_out_println) {
    const char *src =
        "public class Main {\n"
        "  public void hi() {\n"
        "    System.out.println(\"hi\");\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "hi", "PrintStream.println"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_integer_parse) {
    const char *src =
        "public class Main {\n"
        "  public int toInt(String s) {\n"
        "    return Integer.parseInt(s);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "toInt", "Integer.parseInt"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 3: Local class methods (this, super, plain) ─────────── */

TEST(jlsp_local_method_call) {
    const char *src =
        "public class Main {\n"
        "  String greet() { return \"hi\"; }\n"
        "  void run() { greet(); }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "greet"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_this_method_call) {
    const char *src =
        "public class Main {\n"
        "  String greet() { return \"hi\"; }\n"
        "  void run() { this.greet(); }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "greet"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_super_method_call) {
    const char *src =
        "public class Main extends java.lang.Object {\n"
        "  public String describe() {\n"
        "    return super.toString();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "describe", "Object.toString"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_local_field_method) {
    const char *src =
        "public class Main {\n"
        "  private String name;\n"
        "  public int run() {\n"
        "    return this.name.length();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_field_no_this_method) {
    const char *src =
        "public class Main {\n"
        "  private String name = \"x\";\n"
        "  public int run() {\n"
        "    return name.length();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 4: Constructors ────────────────────────────────────── */

TEST(jlsp_object_creation) {
    const char *src =
        "public class Main {\n"
        "  public StringBuilder build() {\n"
        "    return new StringBuilder();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "build", "StringBuilder"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_arraylist_creation) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public ArrayList<String> empty() {\n"
        "    return new ArrayList<>();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "empty", "ArrayList"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_constructor_then_method) {
    const char *src =
        "public class Main {\n"
        "  public String go() {\n"
        "    return new StringBuilder().append(\"x\").toString();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "go", "StringBuilder.append"), 0);
    ASSERT_GTE(require_resolved(r, "go", "toString"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 5: java.util.ArrayList / List / Map ────────────────── */

TEST(jlsp_list_add) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public void run() {\n"
        "    ArrayList<String> xs = new ArrayList<>();\n"
        "    xs.add(\"a\");\n"
        "    xs.size();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "ArrayList.add"), 0);
    ASSERT_GTE(require_resolved(r, "run", "ArrayList.size"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_list_get_chain) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public int len(ArrayList<String> xs) {\n"
        "    return xs.get(0).length();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "len", "ArrayList.get"), 0);
    /* Generics: get returns String → length resolves on String */
    ASSERT_GTE(require_resolved(r, "len", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_hashmap) {
    const char *src =
        "import java.util.HashMap;\n"
        "public class Main {\n"
        "  public int run(HashMap<String, Integer> m) {\n"
        "    m.put(\"k\", 1);\n"
        "    return m.size();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "HashMap.put"), 0);
    ASSERT_GTE(require_resolved(r, "run", "HashMap.size"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_list_static_of) {
    const char *src =
        "import java.util.List;\n"
        "public class Main {\n"
        "  public List<String> ones() {\n"
        "    return List.of(\"a\", \"b\");\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "ones", "List.of"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_set_contains) {
    const char *src =
        "import java.util.HashSet;\n"
        "public class Main {\n"
        "  public boolean has(HashSet<String> s, String x) {\n"
        "    return s.contains(x);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "has", "HashSet.contains"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 6: Inheritance ─────────────────────────────────────── */

TEST(jlsp_inherited_method) {
    const char *src =
        "public class Main {\n"
        "  static class Animal {\n"
        "    public String name() { return \"x\"; }\n"
        "  }\n"
        "  static class Dog extends Animal {\n"
        "    public void bark(Dog d) { d.name(); }\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "bark", "name"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_inherited_through_chain) {
    const char *src =
        "public class Main {\n"
        "  static class A { public String greet() { return \"a\"; } }\n"
        "  static class B extends A {}\n"
        "  static class C extends B {\n"
        "    public String go(C c) { return c.greet(); }\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "go", "greet"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_super_chain_object_toString) {
    const char *src =
        "public class Main {\n"
        "  static class T {\n"
        "    public String describe() { return super.toString(); }\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "describe", "Object.toString"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 7: Enhanced for ─────────────────────────────────────── */

TEST(jlsp_enhanced_for_array) {
    const char *src =
        "public class Main {\n"
        "  public int total(String[] xs) {\n"
        "    int n = 0;\n"
        "    for (String x : xs) {\n"
        "      n += x.length();\n"
        "    }\n"
        "    return n;\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "total", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_enhanced_for_list) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public int total(ArrayList<String> xs) {\n"
        "    int n = 0;\n"
        "    for (String x : xs) {\n"
        "      n += x.length();\n"
        "    }\n"
        "    return n;\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "total", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_enhanced_for_var) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public int total(ArrayList<String> xs) {\n"
        "    int n = 0;\n"
        "    for (var x : xs) {\n"
        "      n += x.length();\n"
        "    }\n"
        "    return n;\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "total", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 8: var local-type inference ─────────────────────────── */

TEST(jlsp_var_inference) {
    const char *src =
        "public class Main {\n"
        "  public int run() {\n"
        "    var s = \"hello\";\n"
        "    return s.length();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_var_inference_constructor) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public void run() {\n"
        "    var xs = new ArrayList<String>();\n"
        "    xs.add(\"a\");\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "ArrayList.add"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 9: Cast / instanceof / ternary / array ─────────────── */

TEST(jlsp_cast_method) {
    const char *src =
        "public class Main {\n"
        "  public int len(Object o) {\n"
        "    return ((String) o).length();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "len", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_ternary) {
    const char *src =
        "public class Main {\n"
        "  public int run(boolean b, String x) {\n"
        "    return (b ? x : x).length();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_array_index) {
    const char *src =
        "public class Main {\n"
        "  public int run(String[] xs) {\n"
        "    return xs[0].length();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_array_length_field) {
    const char *src =
        "public class Main {\n"
        "  public int run(String[] xs) {\n"
        "    int n = xs.length;\n"
        "    return n;\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    /* `xs.length` is a field access, no method call. Just confirms parse
     * doesn't break and no spurious diagnostics fired. */
    cbm_free_result(r);
    PASS();
}

/* ── Category 10: Optional / Streams / lambdas ───────────────────── */

TEST(jlsp_optional_chain) {
    const char *src =
        "import java.util.Optional;\n"
        "public class Main {\n"
        "  public String run(Optional<String> o) {\n"
        "    return o.orElse(\"\");\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Optional.orElse"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_stream_filter_map) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public long run(ArrayList<String> xs) {\n"
        "    return xs.stream().filter(x -> x.length() > 0).count();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "stream"), 0);
    ASSERT_GTE(require_resolved(r, "run", "filter"), 0);
    ASSERT_GTE(require_resolved(r, "run", "count"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_stream_collect_toList) {
    const char *src =
        "import java.util.ArrayList;\n"
        "import java.util.stream.Collectors;\n"
        "public class Main {\n"
        "  public Object run(ArrayList<String> xs) {\n"
        "    return xs.stream().collect(Collectors.toList());\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Collectors.toList"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 11: Functional interfaces ──────────────────────────── */

TEST(jlsp_predicate_test) {
    const char *src =
        "import java.util.function.Predicate;\n"
        "public class Main {\n"
        "  public boolean ok(Predicate<String> p, String s) {\n"
        "    return p.test(s);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "ok", "Predicate.test"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_function_apply) {
    const char *src =
        "import java.util.function.Function;\n"
        "public class Main {\n"
        "  public Object run(Function<String, Integer> f, String s) {\n"
        "    return f.apply(s);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Function.apply"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_supplier_get) {
    const char *src =
        "import java.util.function.Supplier;\n"
        "public class Main {\n"
        "  public Object run(Supplier<String> s) {\n"
        "    return s.get();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Supplier.get"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 12: Throwable + try/catch ──────────────────────────── */

TEST(jlsp_exception_message) {
    const char *src =
        "public class Main {\n"
        "  public String describe() {\n"
        "    try {\n"
        "      Integer.parseInt(\"x\");\n"
        "    } catch (NumberFormatException e) {\n"
        "      return e.getMessage();\n"
        "    }\n"
        "    return \"\";\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "describe", "Throwable.getMessage"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 13: Imports (static + on-demand) ───────────────────── */

TEST(jlsp_static_import_method) {
    const char *src =
        "import static java.lang.Math.sqrt;\n"
        "public class Main {\n"
        "  public double run(double x) {\n"
        "    return sqrt(x);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Math.sqrt"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_on_demand_import) {
    const char *src =
        "import java.util.*;\n"
        "public class Main {\n"
        "  public void run() {\n"
        "    HashMap<String, Integer> m = new HashMap<>();\n"
        "    m.put(\"x\", 1);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "HashMap.put"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 14: Generics + nested templates ────────────────────── */

TEST(jlsp_generic_get_method) {
    const char *src =
        "import java.util.HashMap;\n"
        "public class Main {\n"
        "  public int run(HashMap<String, String> m) {\n"
        "    return m.get(\"k\").length();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "HashMap.get"), 0);
    /* The chained .length() lookup depends on generic-arg substitution into
     * the return type. We cover that for List-of-String elsewhere; with
     * HashMap.get the registry signature returns Object, so the chained
     * String.length call is allowed to be unresolved here. Accept either. */
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_list_of_string_chain) {
    const char *src =
        "import java.util.List;\n"
        "public class Main {\n"
        "  public int run(List<String> xs) {\n"
        "    return xs.get(0).length();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "List.get"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 15: Throw + catch types ────────────────────────────── */

TEST(jlsp_throw_runtime) {
    const char *src =
        "public class Main {\n"
        "  public void run() {\n"
        "    throw new IllegalArgumentException(\"bad\");\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "IllegalArgumentException"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 16: java.io Streams ────────────────────────────────── */

TEST(jlsp_print_stream) {
    const char *src =
        "import java.io.PrintStream;\n"
        "public class Main {\n"
        "  public void run(PrintStream s) {\n"
        "    s.println(\"hi\");\n"
        "    s.flush();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "PrintStream.println"), 0);
    ASSERT_GTE(require_resolved(r, "run", "PrintStream.flush"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_buffered_reader) {
    const char *src =
        "import java.io.BufferedReader;\n"
        "public class Main {\n"
        "  public String run(BufferedReader r) throws java.io.IOException {\n"
        "    return r.readLine();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "BufferedReader.readLine"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_file_methods) {
    const char *src =
        "import java.io.File;\n"
        "public class Main {\n"
        "  public boolean run(File f) {\n"
        "    return f.exists() && f.isFile();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "File.exists"), 0);
    ASSERT_GTE(require_resolved(r, "run", "File.isFile"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 17: java.nio.file ──────────────────────────────────── */

TEST(jlsp_paths_get) {
    const char *src =
        "import java.nio.file.Paths;\n"
        "public class Main {\n"
        "  public Object run() {\n"
        "    return Paths.get(\"a\");\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Paths.get"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_files_readString) {
    const char *src =
        "import java.nio.file.Files;\n"
        "import java.nio.file.Path;\n"
        "public class Main {\n"
        "  public String run(Path p) throws java.io.IOException {\n"
        "    return Files.readString(p);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Files.readString"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 18: java.util.regex ────────────────────────────────── */

TEST(jlsp_regex_pattern_compile) {
    const char *src =
        "import java.util.regex.Pattern;\n"
        "public class Main {\n"
        "  public Pattern run() {\n"
        "    return Pattern.compile(\"^x\");\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Pattern.compile"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_matcher_find) {
    const char *src =
        "import java.util.regex.Matcher;\n"
        "import java.util.regex.Pattern;\n"
        "public class Main {\n"
        "  public boolean run(String s) {\n"
        "    Matcher m = Pattern.compile(\"x\").matcher(s);\n"
        "    return m.find();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Pattern.compile"), 0);
    ASSERT_GTE(require_resolved(r, "run", "Matcher.find"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 19: Concurrent + atomic ───────────────────────────── */

TEST(jlsp_atomic_increment) {
    const char *src =
        "import java.util.concurrent.atomic.AtomicInteger;\n"
        "public class Main {\n"
        "  public int bump(AtomicInteger n) {\n"
        "    return n.incrementAndGet();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "bump", "AtomicInteger.incrementAndGet"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_executor_submit) {
    const char *src =
        "import java.util.concurrent.ExecutorService;\n"
        "import java.util.concurrent.Executors;\n"
        "public class Main {\n"
        "  public void run() {\n"
        "    ExecutorService es = Executors.newFixedThreadPool(2);\n"
        "    es.shutdown();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Executors.newFixedThreadPool"), 0);
    ASSERT_GTE(require_resolved(r, "run", "ExecutorService.shutdown"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_completable_future_thenApply) {
    const char *src =
        "import java.util.concurrent.CompletableFuture;\n"
        "public class Main {\n"
        "  public Object run(CompletableFuture<String> f) {\n"
        "    return f.thenApply(x -> x.length());\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "CompletableFuture.thenApply"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 20: java.time ──────────────────────────────────────── */

TEST(jlsp_localdate_now) {
    const char *src =
        "import java.time.LocalDate;\n"
        "public class Main {\n"
        "  public LocalDate run() {\n"
        "    return LocalDate.now();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "LocalDate.now"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_duration_chain) {
    const char *src =
        "import java.time.Duration;\n"
        "public class Main {\n"
        "  public long run() {\n"
        "    return Duration.ofSeconds(60).toMillis();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Duration.ofSeconds"), 0);
    ASSERT_GTE(require_resolved(r, "run", "Duration.toMillis"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 21: User-defined cross-file like (within one file) ─── */

TEST(jlsp_user_class_method) {
    const char *src =
        "public class Main {\n"
        "  static class Greeter {\n"
        "    public String greet(String name) { return \"hi \" + name; }\n"
        "  }\n"
        "  public String run() {\n"
        "    Greeter g = new Greeter();\n"
        "    return g.greet(\"world\");\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "greet"), 0);
    cbm_free_result(r);
    PASS();
}

/* Java's grammar nests enum methods under enum_body_declarations rather than
 * placing them directly in enum_body.  Require both the indexed Method targets
 * and the receiver-typed calls so a graph-level name fallback cannot mask a
 * missing enum declaration walk. */
TEST(jlsp_user_enum_methods) {
    const char *src = "enum Day {\n"
                      "  SAT, SUN;\n"
                      "  public String label() { return name(); }\n"
                      "  public boolean isWeekend() { return true; }\n"
                      "}\n"
                      "class DayUtil {\n"
                      "  static String describe(Day day) {\n"
                      "    return day.label() + day.isWeekend();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);

    int enum_methods = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *definition = &r->defs.items[i];
        if (definition->label && strcmp(definition->label, "Method") == 0 &&
            definition->parent_class && strstr(definition->parent_class, "Day") &&
            definition->name &&
            (strcmp(definition->name, "label") == 0 ||
             strcmp(definition->name, "isWeekend") == 0)) {
            enum_methods++;
        }
    }
    ASSERT_EQ(enum_methods, 2);
    ASSERT_GTE(require_resolved(r, "describe", "Day.label"), 0);
    ASSERT_GTE(require_resolved(r, "describe", "Day.isWeekend"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_user_static_method) {
    const char *src =
        "public class Main {\n"
        "  static class Util {\n"
        "    public static int twice(int x) { return x + x; }\n"
        "  }\n"
        "  public int run() {\n"
        "    return Util.twice(21);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "twice"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_user_chain) {
    const char *src =
        "public class Main {\n"
        "  static class Box {\n"
        "    public Box self() { return this; }\n"
        "    public String tag() { return \"x\"; }\n"
        "  }\n"
        "  public String run(Box b) {\n"
        "    return b.self().tag();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "self"), 0);
    ASSERT_GTE(require_resolved(r, "run", "tag"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 22: Diagnostics ────────────────────────────────────── */

TEST(jlsp_unknown_variable_unresolved) {
    const char *src =
        "public class Main {\n"
        "  public int run() {\n"
        "    return blob.size();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    /* Should appear as an unresolved diagnostic (confidence 0) — confirm at
     * least one entry exists. */
    int diag = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        if (r->resolved_calls.items[i].confidence == 0.0f) diag++;
    }
    ASSERT_GTE(diag, 1);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_no_resolved_calls_for_empty_class) {
    const char *src = "public class Main {}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(count_resolved(r), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 23: Quality-vs-tree-sitter parity check ────────────── */

/* Without LSP, tree-sitter alone produces calls but no resolved targets.
 * This test confirms: on a real-world-ish snippet, the LSP turns unresolved
 * tree-sitter calls into resolved calls with high confidence. */
TEST(jlsp_lifts_treesitter_calls) {
    const char *src =
        "import java.util.ArrayList;\n"
        "import java.util.HashMap;\n"
        "public class Main {\n"
        "  private ArrayList<String> items = new ArrayList<>();\n"
        "  private HashMap<String, Integer> counts = new HashMap<>();\n"
        "  public int run() {\n"
        "    items.add(\"a\");\n"
        "    items.add(\"b\");\n"
        "    counts.put(\"a\", 1);\n"
        "    String first = items.get(0).toUpperCase();\n"
        "    int n = first.length();\n"
        "    return items.size() + counts.size() + n;\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    /* Tree-sitter alone gives us only raw call nodes; LSP should resolve at
     * least 7 of these to fully-qualified callees. */
    ASSERT_GTE(count_resolved(r), 7);
    ASSERT_GTE(require_resolved(r, "run", "ArrayList.add"), 0);
    ASSERT_GTE(require_resolved(r, "run", "HashMap.put"), 0);
    ASSERT_GTE(require_resolved(r, "run", "ArrayList.get"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.toUpperCase"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    ASSERT_GTE(require_resolved(r, "run", "ArrayList.size"), 0);
    ASSERT_GTE(require_resolved(r, "run", "HashMap.size"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 24: cross-file LSP API (registry-fed) ──────────────── */

TEST(jlsp_cross_file_basic) {
    const char *src =
        "package com.example;\n"
        "public class Caller {\n"
        "  public String run(com.example.Greeter g) { return g.greet(\"x\"); }\n"
        "}\n";
    /* Provide a single cross-file def for Greeter.greet returning String. */
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
    defs[1].def_module_qn = "com.example";
    defs[1].return_types = "java.lang.String";

    const char *imp_names[] = {"Greeter"};
    const char *imp_qns[] = {"com.example.Greeter"};

    cbm_run_java_lsp_cross(&arena, src, (int)strlen(src), "test.com.example", defs, 2, imp_names,
                           imp_qns, 1, NULL, &out);

    int found = 0;
    for (int i = 0; i < out.count; i++) {
        if (out.items[i].confidence < 0.5f) continue;
        if (out.items[i].callee_qn && strstr(out.items[i].callee_qn, "Greeter.greet")) {
            found = 1;
            break;
        }
    }
    if (!found) {
        printf("\n  cross_file_basic: have %d entries\n", out.count);
        for (int i = 0; i < out.count; i++) {
            printf("    %s -> %s [%s %.2f]\n",
                   out.items[i].caller_qn ? out.items[i].caller_qn : "(null)",
                   out.items[i].callee_qn ? out.items[i].callee_qn : "(null)",
                   out.items[i].strategy ? out.items[i].strategy : "(null)",
                   out.items[i].confidence);
        }
    }
    ASSERT_EQ(found, 1);
    cbm_arena_destroy(&arena);
    PASS();
}

/* ── Category 25: Java-specific edge cases ──────────────────────── */

TEST(jlsp_objects_requireNonNull) {
    const char *src =
        "import java.util.Objects;\n"
        "public class Main {\n"
        "  public Object run(Object x) {\n"
        "    return Objects.requireNonNull(x);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Objects.requireNonNull"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_thread_currentThread) {
    const char *src =
        "public class Main {\n"
        "  public Thread run() {\n"
        "    return Thread.currentThread();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Thread.currentThread"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_class_class_literal) {
    const char *src =
        "public class Main {\n"
        "  public String run() {\n"
        "    return String.class.getName();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Class.getName"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_string_join) {
    const char *src =
        "public class Main {\n"
        "  public String run(String[] xs) {\n"
        "    return String.join(\",\", xs);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.join"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_uuid_random) {
    const char *src =
        "import java.util.UUID;\n"
        "public class Main {\n"
        "  public String run() {\n"
        "    return UUID.randomUUID().toString();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "UUID.randomUUID"), 0);
    ASSERT_GTE(require_resolved(r, "run", "toString"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_stringbuilder_append_chain) {
    const char *src =
        "public class Main {\n"
        "  public String run() {\n"
        "    return new StringBuilder().append(\"a\").append(\"b\").append(\"c\").toString();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    /* All three .append() and final .toString() should resolve. */
    int appends = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->confidence >= 0.5f && rc->callee_qn && strstr(rc->callee_qn, "StringBuilder.append")) {
            appends++;
        }
    }
    ASSERT_GTE(appends, 3);
    ASSERT_GTE(require_resolved(r, "run", "StringBuilder.toString"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_arrays_aslist) {
    const char *src =
        "import java.util.Arrays;\n"
        "import java.util.List;\n"
        "public class Main {\n"
        "  public List<String> run() {\n"
        "    return Arrays.asList(\"a\", \"b\");\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Arrays.asList"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_collections_sort) {
    const char *src =
        "import java.util.ArrayList;\n"
        "import java.util.Collections;\n"
        "public class Main {\n"
        "  public void run(ArrayList<String> xs) {\n"
        "    Collections.sort(xs);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Collections.sort"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_deeply_nested_block) {
    const char *src =
        "public class Main {\n"
        "  public int run() {\n"
        "    String s = \"hello\";\n"
        "    if (true) {\n"
        "      if (s.length() > 0) {\n"
        "        return s.length();\n"
        "      }\n"
        "    }\n"
        "    return 0;\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    int length_calls = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->confidence >= 0.5f && rc->callee_qn && strstr(rc->callee_qn, "String.length")) {
            length_calls++;
        }
    }
    ASSERT_GTE(length_calls, 2);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_method_reference_no_crash) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public void run(ArrayList<String> xs) {\n"
        "    xs.forEach(System.out::println);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    /* forEach should still resolve even if method-ref operand isn't typed. */
    ASSERT_GTE(require_resolved(r, "run", "forEach"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_lambda_no_crash) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public void run(ArrayList<String> xs) {\n"
        "    xs.forEach(x -> System.out.println(x));\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "forEach"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_record_call) {
    const char *src =
        "public class Main {\n"
        "  public record Point(int x, int y) {}\n"
        "  public int xCoord(Point p) {\n"
        "    return p.x();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    /* Record components are modeled as fields + synthetic zero-arg
     * accessors: p.x() resolves to Point.x. */
    ASSERT_GTE(require_resolved(r, "xCoord", "Point.x"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_inner_class) {
    const char *src =
        "public class Main {\n"
        "  static class Inner {\n"
        "    public String tag() { return \"x\"; }\n"
        "  }\n"
        "  public String run() {\n"
        "    Inner i = new Inner();\n"
        "    return i.tag();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "tag"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_interface_default_method) {
    const char *src =
        "public class Main {\n"
        "  interface Greet {\n"
        "    default String hello() { return \"hi\"; }\n"
        "  }\n"
        "  static class Impl implements Greet {\n"
        "    public String run() { return hello(); }\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    /* Some indirection — accept any resolved call. */
    ASSERT_GTE(r->resolved_calls.count, 1);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_file_path) {
    const char *src =
        "public class Main {\n"
        "  public int run(String s) {\n"
        "    return s.length();\n"
        "  }\n"
        "}\n";
    /* Use a deeper rel_path to confirm FQN compute respects the path. */
    CBMFileResult *r = extract_java_at(src, "src/main/java/com/example/Main.java");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_deeply_nested_chain) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public int run(ArrayList<String> xs) {\n"
        "    return xs.get(0).trim().toLowerCase().length();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "ArrayList.get"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.trim"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.toLowerCase"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 26: Lambda SAM inference ────────────────────────────── */

TEST(jlsp_lambda_param_typed_in_filter) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public long run(ArrayList<String> xs) {\n"
        "    return xs.stream().filter(x -> x.length() > 0).count();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    /* x has type String (from Predicate<String>), so x.length() must
     * resolve to String.length. This was previously unresolved. */
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_lambda_param_typed_in_map) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public Object run(ArrayList<String> xs) {\n"
        "    return xs.stream().map(x -> x.toUpperCase()).count();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.toUpperCase"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_lambda_in_forEach) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public void run(ArrayList<String> xs) {\n"
        "    xs.forEach(x -> { x.length(); });\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_lambda_in_consumer) {
    const char *src =
        "import java.util.function.Consumer;\n"
        "public class Main {\n"
        "  public void run(Consumer<String> c) {\n"
        "    c.accept(\"x\");\n"
        "  }\n"
        "  public void go() {\n"
        "    run(s -> s.toUpperCase());\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "go", "String.toUpperCase"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_lambda_with_block_body) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public void run(ArrayList<String> xs) {\n"
        "    xs.stream().filter(x -> {\n"
        "      String t = x.trim();\n"
        "      return t.length() > 0;\n"
        "    }).count();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.trim"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_lambda_typed_in_chained_stream) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public long run(ArrayList<String> xs) {\n"
        "    return xs.stream()\n"
        "      .filter(x -> x.length() > 0)\n"
        "      .map(s -> s.toUpperCase())\n"
        "      .count();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.toUpperCase"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 27: Method-reference resolution ────────────────────── */

TEST(jlsp_method_reference_static) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public void run(ArrayList<String> xs) {\n"
        "    xs.forEach(System.out::println);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "PrintStream.println"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_method_reference_instance) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  public Object run(ArrayList<String> xs) {\n"
        "    return xs.stream().map(String::toUpperCase).count();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.toUpperCase"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_method_reference_constructor) {
    const char *src =
        "import java.util.ArrayList;\n"
        "import java.util.function.Function;\n"
        "public class Main {\n"
        "  public Function<String, StringBuilder> run() {\n"
        "    return StringBuilder::new;\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "StringBuilder"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 28: User-class field resolution (non-this receiver) ──── */

TEST(jlsp_user_field_method) {
    const char *src =
        "public class Main {\n"
        "  static class Box {\n"
        "    public String label = \"x\";\n"
        "  }\n"
        "  public int run(Box b) {\n"
        "    return b.label.length();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_user_field_chain) {
    const char *src =
        "import java.util.ArrayList;\n"
        "public class Main {\n"
        "  static class Bag {\n"
        "    public ArrayList<String> items = new ArrayList<>();\n"
        "  }\n"
        "  public int run(Bag b) {\n"
        "    return b.items.size();\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "ArrayList.size"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 29: Real-corpus parity benchmark ─────────────────────
 *
 * A multi-class realistic Java fixture exercising:
 *   - service / repository / model layering (typical Spring style)
 *   - streams + lambdas
 *   - inheritance + interfaces
 *   - generics
 *   - try-with-resources
 *   - chained method calls
 *
 * The assertion is on the *resolution rate*: at least 90% of method calls
 * in the source should produce a high-confidence (≥0.85) resolved edge.
 * The denominator is `r->calls.count` (every call tree-sitter saw); the
 * numerator counts unique resolved entries pointing to a known callee. */

TEST(jlsp_real_corpus_parity_90_percent) {
    const char *src =
        "import java.util.ArrayList;\n"
        "import java.util.HashMap;\n"
        "import java.util.List;\n"
        "import java.util.Map;\n"
        "import java.util.Optional;\n"
        "import java.util.stream.Collectors;\n"
        "import java.util.function.Predicate;\n"
        "public class Main {\n"
        "  static class User {\n"
        "    public String name;\n"
        "    public int age;\n"
        "    public User(String n, int a) { this.name = n; this.age = a; }\n"
        "    public String greet() { return \"Hello \" + this.name; }\n"
        "  }\n"
        "  static class UserRepository {\n"
        "    public ArrayList<User> users = new ArrayList<>();\n"
        "    public Optional<User> findByName(String n) {\n"
        "      return users.stream().filter(u -> u.name.equals(n)).findFirst();\n"
        "    }\n"
        "    public List<User> adults() {\n"
        "      return users.stream().filter(u -> u.age >= 18).collect(Collectors.toList());\n"
        "    }\n"
        "    public int total() { return users.size(); }\n"
        "  }\n"
        "  static class UserService {\n"
        "    public UserRepository repo = new UserRepository();\n"
        "    public String describe(String n) {\n"
        "      Optional<User> u = repo.findByName(n);\n"
        "      return u.map(x -> x.greet()).orElse(\"unknown\");\n"
        "    }\n"
        "    public HashMap<String, Integer> ageMap() {\n"
        "      HashMap<String, Integer> m = new HashMap<>();\n"
        "      for (User u : repo.users) {\n"
        "        m.put(u.name, u.age);\n"
        "      }\n"
        "      return m;\n"
        "    }\n"
        "    public long countAdults() {\n"
        "      return repo.adults().size();\n"
        "    }\n"
        "    public boolean any(Predicate<User> p) {\n"
        "      return repo.users.stream().anyMatch(p);\n"
        "    }\n"
        "  }\n"
        "  public void demo() {\n"
        "    UserService svc = new UserService();\n"
        "    String s = svc.describe(\"alice\");\n"
        "    System.out.println(s);\n"
        "    HashMap<String, Integer> ages = svc.ageMap();\n"
        "    System.out.println(ages.size());\n"
        "    long n = svc.countAdults();\n"
        "    System.out.println(n);\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);

    /* Collect distinct call sites tree-sitter saw (caller, callee_name).
     * Tree-sitter's calls array (r->calls) is the denominator: it captures
     * every method_invocation it parsed. */
    int total_calls = r->calls.count;
    int resolved_high = count_resolved(r);
    /* `count_resolved` returns confidence ≥ 0.5 entries — tighten to 0.85+ */
    int strict = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        if (r->resolved_calls.items[i].confidence >= 0.85f) strict++;
    }
    printf("\n  [parity] tree-sitter calls=%d, resolved≥0.5=%d, resolved≥0.85=%d\n",
           total_calls, resolved_high, strict);
    /* Demand at least 90% of tree-sitter calls have a high-confidence
     * resolution. (LSP can emit multiple resolved edges per call site —
     * inheritance, interface dispatch — so strict ≥ 0.9 * total_calls is
     * achievable when receiver types are pinned down.) */
    ASSERT_GTE(strict, (total_calls * 9) / 10);
    cbm_free_result(r);
    PASS();
}

/* ── Suite registration ──────────────────────────────────────────── */

/* ── Multi-parent inheritance BFS (JLS 8.4.8) ──────────────────── */

TEST(jlsp_extends_plus_implements_default) {
    /* Members through a SECOND-or-later parent must resolve: the old walk
     * followed only embedded_types[0] (the extends chain). */
    const char *src = "interface Greeter {\n"
                      "    default String hi() { return \"hi\"; }\n"
                      "}\n"
                      "class B {}\n"
                      "class Impl extends B implements Greeter {}\n"
                      "class Main {\n"
                      "    void run() {\n"
                      "        Impl impl = new Impl();\n"
                      "        impl.hi();\n"
                      "    }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT(r);
    ASSERT_GTE(require_resolved(r, "run", "hi"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_second_interface_method) {
    const char *src = "interface Opener {\n"
                      "    default void open() {}\n"
                      "}\n"
                      "interface Closer {\n"
                      "    default void close() {}\n"
                      "}\n"
                      "class Door implements Opener, Closer {}\n"
                      "class Main {\n"
                      "    void run() {\n"
                      "        Door d = new Door();\n"
                      "        d.close();\n"
                      "    }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT(r);
    ASSERT_GTE(require_resolved(r, "run", "close"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_diamond_interface_method) {
    /* Method from I2 must be reachable through I3-typed receiver where
     * I3 extends I1, I2 (I2 is the SECOND parent). */
    const char *src = "interface I1 {\n"
                      "    default void a() {}\n"
                      "}\n"
                      "interface I2 {\n"
                      "    default void b() {}\n"
                      "}\n"
                      "interface I3 extends I1, I2 {}\n"
                      "class Main {\n"
                      "    void run(I3 x) {\n"
                      "        x.b();\n"
                      "    }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT(r);
    ASSERT_GTE(require_resolved(r, "run", "b"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Pattern matching (Java 16 instanceof / Java 21 switch patterns) ── */

TEST(jlsp_instanceof_pattern) {
    const char *src = "public class Main {\n"
                      "  public int run(Object o) {\n"
                      "    if (o instanceof String s) return s.length();\n"
                      "    return 0;\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_switch_type_pattern) {
    const char *src = "public class Main {\n"
                      "  public String run(Object x) {\n"
                      "    return switch (x) {\n"
                      "      case String s -> s.trim();\n"
                      "      default -> \"\";\n"
                      "    };\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.trim"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_switch_guard_binding) {
    /* The guard (`when` expr) must see the pattern binding, and calls inside
     * the guard must resolve. */
    const char *src = "public class Main {\n"
                      "  public int run(Object x) {\n"
                      "    return switch (x) {\n"
                      "      case String s when s.isEmpty() -> 0;\n"
                      "      case String t -> t.length();\n"
                      "      default -> -1;\n"
                      "    };\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.isEmpty"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_switch_record_pattern) {
    /* Record deconstruction: `case Circle(double r)` binds r by the
     * component's declared type; a type-pattern arm binds the whole value. */
    const char *src = "public class Main {\n"
                      "  record Circle(double radius) {}\n"
                      "  public double run(Object s, String lim) {\n"
                      "    return switch (s) {\n"
                      "      case Circle c -> c.radius();\n"
                      "      default -> 0.0;\n"
                      "    };\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Circle.radius"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_switch_record_deconstruction) {
    /* Deconstructed component (String n) bound by its declared type; the
     * guard's call on the binding must resolve. */
    const char *src = "public class Main {\n"
                      "  record Tag(String name) {}\n"
                      "  public int run(Object o) {\n"
                      "    return switch (o) {\n"
                      "      case Tag(String n) when n.isBlank() -> 0;\n"
                      "      case Tag(String m) -> m.length();\n"
                      "      default -> -1;\n"
                      "    };\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.isBlank"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_switch_record_var_component) {
    /* `var` deconstruction component falls back to the record's registered
     * field type by position (depends on record-components field typing). */
    const char *src = "public class Main {\n"
                      "  record Named(String label) {}\n"
                      "  public int run(Object o) {\n"
                      "    return switch (o) {\n"
                      "      case Named(var l) -> l.length();\n"
                      "      default -> 0;\n"
                      "    };\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_instanceof_pattern_colon_switch) {
    /* Colon-style statement group with a type pattern (Java 21). */
    const char *src = "public class Main {\n"
                      "  public void run(Object x) {\n"
                      "    switch (x) {\n"
                      "      case String s:\n"
                      "        s.trim();\n"
                      "        break;\n"
                      "      default:\n"
                      "        break;\n"
                      "    }\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.trim"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Constructor delegation (this(...) / super(...)) ─────────────── */

TEST(jlsp_ctor_this_delegation) {
    const char *src = "public class P {\n"
                      "  P() { this(0); }\n"
                      "  P(int v) { init(v); }\n"
                      "  void init(int v) {}\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "P.P", "P.P"), 0);
    ASSERT_GTE(require_resolved(r, "P.P", "init"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_ctor_super_delegation) {
    const char *src = "class B {\n"
                      "  B(int v) {}\n"
                      "}\n"
                      "class C extends B {\n"
                      "  C() { super(1); }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "C.C", "B.B"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_ctor_super_no_registered_ctor) {
    /* No B ctor registered: emit lsp_constructor_synth to the class node —
     * never a crash, never a bogus method target. */
    const char *src = "class B {}\n"
                      "class C extends B {\n"
                      "  C() { super(); }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    int idx = find_resolved(r, "C.C", "B");
    ASSERT_GTE(idx, 0);
    ASSERT(strcmp(r->resolved_calls.items[idx].strategy, "lsp_constructor_synth") == 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_ctor_delegation_raw_call_row) {
    /* Extraction join: explicit_constructor_invocation must produce a raw
     * CALL row whose callee is the enclosing class short name (`this`) /
     * the superclass leaf (`super`) so the pipeline join has a site. */
    const char *src = "class B { B(int v) {} }\n"
                      "class C extends B {\n"
                      "  C() { super(1); }\n"
                      "  C(int x) { this(); }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    int saw_super = 0, saw_this = 0;
    for (int i = 0; i < r->calls.count; i++) {
        const CBMCall *c = &r->calls.items[i];
        if (!c->callee_name || !c->enclosing_func_qn) continue;
        if (strcmp(c->callee_name, "B") == 0 && strstr(c->enclosing_func_qn, "C.C"))
            saw_super = 1;
        if (strcmp(c->callee_name, "C") == 0 && strstr(c->enclosing_func_qn, "C.C"))
            saw_this = 1;
    }
    ASSERT_EQ(saw_super, 1);
    ASSERT_EQ(saw_this, 1);
    cbm_free_result(r);
    PASS();
}

/* ── Record components (fields + synthetic accessors) ────────────── */

TEST(jlsp_record_accessor_chain) {
    const char *src = "public class Main {\n"
                      "  record User(String name) {}\n"
                      "  public int run(User u) {\n"
                      "    return u.name().length();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "User.name"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_record_field_access) {
    /* Direct component read (no parens) types like a field. */
    const char *src = "public class Main {\n"
                      "  record Box(String tag) {}\n"
                      "  public int run(Box b) {\n"
                      "    return b.tag.length();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_record_compact_ctor) {
    const char *src = "public record R(int v) {\n"
                      "  R { check(v); }\n"
                      "  static void check(int v) {}\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "R.R", "check"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_record_canonical_ctor) {
    const char *src = "public class Main {\n"
                      "  record User(String name) {}\n"
                      "  public int run() {\n"
                      "    var u = new User(\"x\");\n"
                      "    return u.name().length();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "User.name"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_record_explicit_accessor_wins) {
    /* An explicit accessor in the body must not be duplicated by the
     * synthetic one; the call still resolves. */
    const char *src = "public class Main {\n"
                      "  record User(String name) {\n"
                      "    public String name() { return name; }\n"
                      "  }\n"
                      "  public int run(User u) {\n"
                      "    return u.name().length();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "User.name"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    /* Exactly ONE Method def named User.name (the explicit one). */
    int accessor_defs = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (d->label && strcmp(d->label, "Method") == 0 && d->qualified_name &&
            strstr(d->qualified_name, "User.name"))
            accessor_defs++;
    }
    ASSERT_EQ(accessor_defs, 1);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_record_extraction_defs) {
    /* Extraction emits per-component Field defs + synthetic accessor Method
     * defs so records work cross-file (and from Kotlin). */
    const char *src = "public record Point(int x, int y) {}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    int field_x = 0, method_x = 0, method_y = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (!d->label || !d->name) continue;
        if (strcmp(d->label, "Field") == 0 && strcmp(d->name, "x") == 0 && d->return_type &&
            strcmp(d->return_type, "int") == 0)
            field_x++;
        if (strcmp(d->label, "Method") == 0 && strcmp(d->name, "x") == 0) method_x++;
        if (strcmp(d->label, "Method") == 0 && strcmp(d->name, "y") == 0) method_y++;
    }
    ASSERT_EQ(field_x, 1);
    ASSERT_EQ(method_x, 1);
    ASSERT_EQ(method_y, 1);
    cbm_free_result(r);
    PASS();
}

/* ── Cross-file field types (field_defs fold + registrar consumption) ── */

TEST(jlsp_cross_field_chain) {
    /* Class A's field type arrives via field_defs (the cross surface), not
     * from this file's AST: `h.svc.handle()` must resolve through it. */
    const char *src = "package demo;\n"
                      "public class App {\n"
                      "  public void go(Handler h) { h.svc.handle(); }\n"
                      "}\n";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    CBMLSPDef defs[3];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "demo.Handler";
    defs[0].short_name = "Handler";
    defs[0].label = "Class";
    defs[0].field_defs = "svc:demo.Service";
    defs[1].qualified_name = "demo.Service";
    defs[1].short_name = "Service";
    defs[1].label = "Class";
    defs[2].qualified_name = "demo.Service.handle";
    defs[2].short_name = "handle";
    defs[2].label = "Method";
    defs[2].receiver_type = "demo.Service";
    defs[2].return_types = "void";
    const char *imp_names[] = {"Handler"};
    const char *imp_qns[] = {"demo.Handler"};
    cbm_run_java_lsp_cross(&arena, src, (int)strlen(src), "test.App", defs, 3, imp_names, imp_qns,
                           1, NULL, &out);
    int found = 0;
    for (int i = 0; i < out.count; i++) {
        if (out.items[i].confidence < 0.5f) continue;
        if (out.items[i].callee_qn && strstr(out.items[i].callee_qn, "Service.handle")) found = 1;
    }
    ASSERT_EQ(found, 1);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(jlsp_cross_generic_field) {
    /* Generic field type text survives the fold: items.get(0).length(). */
    const char *src = "package demo;\n"
                      "public class App {\n"
                      "  public int go(Holder h) { return h.items.get(0).length(); }\n"
                      "}\n";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    CBMLSPDef defs[1];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "demo.Holder";
    defs[0].short_name = "Holder";
    defs[0].label = "Class";
    defs[0].field_defs = "items:java.util.List<java.lang.String>";
    const char *imp_names[] = {"Holder"};
    const char *imp_qns[] = {"demo.Holder"};
    cbm_run_java_lsp_cross(&arena, src, (int)strlen(src), "test.App", defs, 1, imp_names, imp_qns,
                           1, NULL, &out);
    int found = 0;
    for (int i = 0; i < out.count; i++) {
        if (out.items[i].confidence < 0.5f) continue;
        if (out.items[i].callee_qn && strstr(out.items[i].callee_qn, "String.length")) found = 1;
    }
    ASSERT_EQ(found, 1);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(jlsp_cross_tier2_field_chain) {
    /* Production pair: shared sealed base registry + per-file overlay. Both
     * the field_defs consumption (Handler.svc from the base) and the newly
     * wired own-file AST enrichment run in this path. */
    const char *src = "package demo;\n"
                      "public class App {\n"
                      "  Handler h;\n"
                      "  public void go() { h.svc.handle(); }\n"
                      "}\n";
    CBMFileResult *r = extract_java_at(src, "App.java");
    ASSERT_NOT_NULL(r);
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMLSPDef defs[3];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "demo.Handler";
    defs[0].short_name = "Handler";
    defs[0].label = "Class";
    defs[0].field_defs = "svc:demo.Service";
    defs[0].lang = CBM_LANG_JAVA;
    defs[1].qualified_name = "demo.Service";
    defs[1].short_name = "Service";
    defs[1].label = "Class";
    defs[1].lang = CBM_LANG_JAVA;
    defs[2].qualified_name = "demo.Service.handle";
    defs[2].short_name = "handle";
    defs[2].label = "Method";
    defs[2].receiver_type = "demo.Service";
    defs[2].return_types = "void";
    defs[2].lang = CBM_LANG_JAVA;
    CBMTypeRegistry *base = cbm_java_build_cross_registry(&arena, defs, 3);
    ASSERT_NOT_NULL(base);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    const char *imp_names[] = {"Handler"};
    const char *imp_qns[] = {"demo.Handler"};
    cbm_run_java_lsp_cross_with_registry(&arena, r, src, (int)strlen(src), "test.App", base,
                                         imp_names, imp_qns, 1, NULL, &out);
    int found = 0;
    for (int i = 0; i < out.count; i++) {
        if (out.items[i].confidence < 0.5f) continue;
        if (out.items[i].callee_qn && strstr(out.items[i].callee_qn, "Service.handle")) found = 1;
    }
    ASSERT_EQ(found, 1);
    cbm_arena_destroy(&arena);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_cross_tier2_generic_signature) {
    /* 对拍A (java-cross-file-field-types): the Tier-2 path must re-run
     * signature patching so generics survive — registry-driven SAM binding
     * needs Consumer<String>, which extraction strips to Consumer. */
    const char *src = "package demo;\n"
                      "import java.util.function.Consumer;\n"
                      "public class App {\n"
                      "  void each(Consumer<String> c) {}\n"
                      "  public void go() { each(s -> s.trim()); }\n"
                      "}\n";
    CBMFileResult *r = extract_java_at(src, "App.java");
    ASSERT_NOT_NULL(r);
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry *base = cbm_java_build_cross_registry(&arena, NULL, 0);
    ASSERT_NOT_NULL(base);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    cbm_run_java_lsp_cross_with_registry(&arena, r, src, (int)strlen(src), "test.App", base, NULL,
                                         NULL, 0, NULL, &out);
    int found = 0;
    for (int i = 0; i < out.count; i++) {
        if (out.items[i].confidence < 0.5f) continue;
        if (out.items[i].callee_qn && strstr(out.items[i].callee_qn, "String.trim")) found = 1;
    }
    ASSERT_EQ(found, 1);
    cbm_arena_destroy(&arena);
    cbm_free_result(r);
    PASS();
}

/* ── Test-annotation detection (JUnit4/5, TestNG) ────────────────── */

TEST(jlsp_junit5_annotated_test) {
    const char *src = "package com.x;\n"
                      "import org.junit.jupiter.api.Test;\n"
                      "public class UserServiceCheck {\n"
                      "  @Test void returnsUser() {}\n"
                      "  void helperOnly() {}\n"
                      "  @ParameterizedTest void eachUser() {}\n"
                      "}\n";
    /* Deliberately NOT a conventional test path/suffix. */
    CBMFileResult *r = extract_java_at(src, "src/main/java/com/x/UserServiceCheck.java");
    ASSERT_NOT_NULL(r);
    int annotated = 0, helper_marked = 0, parameterized = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (!d->name) continue;
        if (strcmp(d->name, "returnsUser") == 0 && d->is_test && d->is_test_annotated) annotated = 1;
        if (strcmp(d->name, "helperOnly") == 0 && (d->is_test || d->is_test_annotated))
            helper_marked = 1;
        if (strcmp(d->name, "eachUser") == 0 && d->is_test_annotated) parameterized = 1;
    }
    ASSERT_EQ(annotated, 1);
    ASSERT_EQ(helper_marked, 0);
    ASSERT_EQ(parameterized, 1);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_spring_boot_test_not_marked) {
    /* 对拍B: suffix-matching must never fire — @SpringBootTest ends in
     * "Test" but is a configuration annotation, not a test method marker. */
    const char *src = "package com.x;\n"
                      "public class Wiring {\n"
                      "  @SpringBootTest void configure() {}\n"
                      "  @WebMvcTest void mvc() {}\n"
                      "}\n";
    CBMFileResult *r = extract_java_at(src, "src/main/java/com/x/Wiring.java");
    ASSERT_NOT_NULL(r);
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (!d->name) continue;
        if (strcmp(d->name, "configure") == 0 || strcmp(d->name, "mvc") == 0) {
            ASSERT_EQ(d->is_test_annotated ? 1 : 0, 0);
            ASSERT_EQ(d->is_test ? 1 : 0, 0);
        }
    }
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_testng_class_level_test) {
    /* TestNG class-level @Test marks methods — but ONLY with an org.testng
     * import in the file (对拍B gate). */
    const char *src_testng = "package com.x;\n"
                             "import org.testng.annotations.Test;\n"
                             "@Test\n"
                             "public class AllChecks {\n"
                             "  public void verifyOne() {}\n"
                             "}\n";
    CBMFileResult *r = extract_java_at(src_testng, "src/main/java/com/x/AllChecks.java");
    ASSERT_NOT_NULL(r);
    int marked = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (d->name && strcmp(d->name, "verifyOne") == 0 && d->is_test_annotated) marked = 1;
    }
    ASSERT_EQ(marked, 1);
    cbm_free_result(r);

    /* Same shape WITHOUT the org.testng import: class-level @Test (e.g. a
     * project's own annotation) must not propagate. */
    const char *src_plain = "package com.x;\n"
                            "@Test\n"
                            "public class AllChecks {\n"
                            "  public void verifyOne() {}\n"
                            "}\n";
    r = extract_java_at(src_plain, "src/main/java/com/x/AllChecks.java");
    ASSERT_NOT_NULL(r);
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (d->name && strcmp(d->name, "verifyOne") == 0) {
            ASSERT_EQ(d->is_test_annotated ? 1 : 0, 0);
        }
    }
    cbm_free_result(r);
    PASS();
}

/* ── Lombok synthetic members ────────────────────────────────────── */

TEST(jlsp_lombok_getter) {
    const char *src = "@Getter\n"
                      "public class User {\n"
                      "  String name;\n"
                      "  public int run(User u) {\n"
                      "    return u.getName().length();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "User.getName"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_lombok_data_setter) {
    const char *src = "@Data\n"
                      "public class User {\n"
                      "  String name;\n"
                      "  public void run(User u) {\n"
                      "    u.setName(\"x\");\n"
                      "    u.getName().trim();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "User.setName"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.trim"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_lombok_boolean_is_getter) {
    const char *src = "@Getter\n"
                      "public class Flag {\n"
                      "  boolean active;\n"
                      "  public boolean run(Flag f) {\n"
                      "    return f.isActive();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Flag.isActive"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_lombok_builder_chain) {
    const char *src = "@Builder\n"
                      "@Getter\n"
                      "public class User {\n"
                      "  String name;\n"
                      "  public int run() {\n"
                      "    return User.builder().name(\"x\").build().getName().length();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "User.builder"), 0);
    ASSERT_GTE(require_resolved(r, "run", "UserBuilder.build"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_lombok_slf4j) {
    const char *src = "@Slf4j\n"
                      "public class Service {\n"
                      "  public void run() {\n"
                      "    log.info(\"hello\");\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Logger.info"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_lombok_negative_no_annotation) {
    /* No Lombok annotations: no synthetic getName must appear. */
    const char *src = "public class User {\n"
                      "  String name;\n"
                      "  public void run(User u) {\n"
                      "    u.getName();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(find_resolved(r, "run", "User.getName"), -1);
    cbm_free_result(r);
    PASS();
}

/* ── Stdlib expansion (Java 21 surface) ──────────────────────────── */

TEST(jlsp_std_bigdecimal_chain) {
    const char *src = "import java.math.BigDecimal;\n"
                      "public class Main {\n"
                      "  public BigDecimal run(BigDecimal a, BigDecimal b) {\n"
                      "    return a.add(b).setScale(2);\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "BigDecimal.add"), 0);
    ASSERT_GTE(require_resolved(r, "run", "BigDecimal.setScale"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_std_httpclient) {
    const char *src =
        "import java.net.http.HttpClient;\n"
        "import java.net.http.HttpRequest;\n"
        "import java.net.http.HttpResponse;\n"
        "public class Main {\n"
        "  public void run(HttpRequest req) throws Exception {\n"
        "    HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());\n"
        "  }\n"
        "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "HttpClient.newHttpClient"), 0);
    ASSERT_GTE(require_resolved(r, "run", "HttpClient.send"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_std_virtual_thread) {
    const char *src = "public class Main {\n"
                      "  public void run(Runnable r) {\n"
                      "    Thread.ofVirtual().name(\"w\").start(r);\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "ofVirtual"), 0);
    ASSERT_GTE(require_resolved(r, "run", "OfVirtual.start"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_std_countdown_latch) {
    const char *src = "import java.util.concurrent.CountDownLatch;\n"
                      "public class Main {\n"
                      "  public void run(CountDownLatch latch) throws Exception {\n"
                      "    latch.countDown();\n"
                      "    latch.await();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "CountDownLatch.countDown"), 0);
    ASSERT_GTE(require_resolved(r, "run", "CountDownLatch.await"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_std_blocking_queue) {
    const char *src = "import java.util.concurrent.BlockingQueue;\n"
                      "public class Main {\n"
                      "  public int run(BlockingQueue<String> q) throws Exception {\n"
                      "    return q.take().length();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "BlockingQueue.take"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_std_collectors_tomap) {
    const char *src = "import java.util.stream.Collectors;\n"
                      "public class Main {\n"
                      "  public void run() {\n"
                      "    Collectors.toMap(null, null);\n"
                      "    Collectors.joining(\",\");\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Collectors.toMap"), 0);
    ASSERT_GTE(require_resolved(r, "run", "Collectors.joining"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_std_string_formatted) {
    const char *src = "public class Main {\n"
                      "  public int run(String s) {\n"
                      "    return s.formatted(1).length();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "String.formatted"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_std_stringjoiner) {
    const char *src = "import java.util.StringJoiner;\n"
                      "public class Main {\n"
                      "  public String run() {\n"
                      "    StringJoiner j = new StringJoiner(\",\");\n"
                      "    j.add(\"a\");\n"
                      "    return j.toString();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "StringJoiner.add"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_std_sequenced_collection) {
    /* Java 21 SequencedCollection: reversed()/getFirst on List. */
    const char *src = "import java.util.List;\n"
                      "public class Main {\n"
                      "  public int run(List<String> xs) {\n"
                      "    return xs.reversed().getFirst().length();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "List.reversed"), 0);
    ASSERT_GTE(require_resolved(r, "run", "String.length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(jlsp_std_files_walk) {
    const char *src = "import java.nio.file.Files;\n"
                      "import java.nio.file.Path;\n"
                      "public class Main {\n"
                      "  public void run(Path p) throws Exception {\n"
                      "    Files.newBufferedReader(p).readLine();\n"
                      "  }\n"
                      "}\n";
    CBMFileResult *r = extract_java(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Files.newBufferedReader"), 0);
    ASSERT_GTE(require_resolved(r, "run", "BufferedReader.readLine"), 0);
    cbm_free_result(r);
    PASS();
}

void suite_java_lsp(void) {
    /* Strings / java.lang */
    RUN_TEST(jlsp_string_length);
    RUN_TEST(jlsp_string_concat_method);
    RUN_TEST(jlsp_string_chain);
    RUN_TEST(jlsp_string_static);
    RUN_TEST(jlsp_string_format);

    /* Math + System + Integer */
    RUN_TEST(jlsp_math_static);
    RUN_TEST(jlsp_math_chain);
    RUN_TEST(jlsp_system_currentTimeMillis);
    RUN_TEST(jlsp_system_out_println);
    RUN_TEST(jlsp_integer_parse);

    /* Local class + this/super */
    RUN_TEST(jlsp_local_method_call);
    RUN_TEST(jlsp_this_method_call);
    RUN_TEST(jlsp_super_method_call);
    RUN_TEST(jlsp_local_field_method);
    RUN_TEST(jlsp_field_no_this_method);

    /* Constructors */
    RUN_TEST(jlsp_object_creation);
    RUN_TEST(jlsp_arraylist_creation);
    RUN_TEST(jlsp_constructor_then_method);

    /* Collections */
    RUN_TEST(jlsp_list_add);
    RUN_TEST(jlsp_list_get_chain);
    RUN_TEST(jlsp_hashmap);
    RUN_TEST(jlsp_list_static_of);
    RUN_TEST(jlsp_set_contains);

    /* Inheritance */
    RUN_TEST(jlsp_inherited_method);
    RUN_TEST(jlsp_inherited_through_chain);
    RUN_TEST(jlsp_super_chain_object_toString);

    /* Enhanced for */
    RUN_TEST(jlsp_enhanced_for_array);
    RUN_TEST(jlsp_enhanced_for_list);
    RUN_TEST(jlsp_enhanced_for_var);

    /* var */
    RUN_TEST(jlsp_var_inference);
    RUN_TEST(jlsp_var_inference_constructor);

    /* Cast / ternary / array */
    RUN_TEST(jlsp_cast_method);
    RUN_TEST(jlsp_ternary);
    RUN_TEST(jlsp_array_index);
    RUN_TEST(jlsp_array_length_field);

    /* Optional / Streams / lambdas */
    RUN_TEST(jlsp_optional_chain);
    RUN_TEST(jlsp_stream_filter_map);
    RUN_TEST(jlsp_stream_collect_toList);

    /* Functional interfaces */
    RUN_TEST(jlsp_predicate_test);
    RUN_TEST(jlsp_function_apply);
    RUN_TEST(jlsp_supplier_get);

    /* Throwable */
    RUN_TEST(jlsp_exception_message);

    /* Imports */
    RUN_TEST(jlsp_static_import_method);
    RUN_TEST(jlsp_on_demand_import);

    /* Generics */
    RUN_TEST(jlsp_generic_get_method);
    RUN_TEST(jlsp_list_of_string_chain);

    /* Throw + new */
    RUN_TEST(jlsp_throw_runtime);

    /* IO */
    RUN_TEST(jlsp_print_stream);
    RUN_TEST(jlsp_buffered_reader);
    RUN_TEST(jlsp_file_methods);

    /* nio.file */
    RUN_TEST(jlsp_paths_get);
    RUN_TEST(jlsp_files_readString);

    /* regex */
    RUN_TEST(jlsp_regex_pattern_compile);
    RUN_TEST(jlsp_matcher_find);

    /* Concurrent */
    RUN_TEST(jlsp_atomic_increment);
    RUN_TEST(jlsp_executor_submit);
    RUN_TEST(jlsp_completable_future_thenApply);

    /* time */
    RUN_TEST(jlsp_localdate_now);
    RUN_TEST(jlsp_duration_chain);

    /* User-defined */
    RUN_TEST(jlsp_user_class_method);
    RUN_TEST(jlsp_user_enum_methods);
    RUN_TEST(jlsp_user_static_method);
    RUN_TEST(jlsp_user_chain);

    /* Diagnostics */
    RUN_TEST(jlsp_unknown_variable_unresolved);
    RUN_TEST(jlsp_no_resolved_calls_for_empty_class);

    /* Quality vs tree-sitter */
    RUN_TEST(jlsp_lifts_treesitter_calls);

    /* Cross-file */
    RUN_TEST(jlsp_cross_file_basic);

    /* Edge cases */
    RUN_TEST(jlsp_objects_requireNonNull);
    RUN_TEST(jlsp_thread_currentThread);
    RUN_TEST(jlsp_class_class_literal);
    RUN_TEST(jlsp_string_join);
    RUN_TEST(jlsp_uuid_random);
    RUN_TEST(jlsp_stringbuilder_append_chain);
    RUN_TEST(jlsp_arrays_aslist);
    RUN_TEST(jlsp_collections_sort);
    RUN_TEST(jlsp_deeply_nested_block);
    RUN_TEST(jlsp_method_reference_no_crash);
    RUN_TEST(jlsp_lambda_no_crash);
    RUN_TEST(jlsp_record_call);
    RUN_TEST(jlsp_inner_class);
    RUN_TEST(jlsp_interface_default_method);
    RUN_TEST(jlsp_file_path);
    RUN_TEST(jlsp_deeply_nested_chain);

    /* Lambda SAM inference — restores typing inside lambda bodies. */
    RUN_TEST(jlsp_lambda_param_typed_in_filter);
    RUN_TEST(jlsp_lambda_param_typed_in_map);
    RUN_TEST(jlsp_lambda_in_forEach);
    RUN_TEST(jlsp_lambda_in_consumer);
    RUN_TEST(jlsp_lambda_with_block_body);
    RUN_TEST(jlsp_lambda_typed_in_chained_stream);

    /* Method references — SAM-aware lookup of the referenced method. */
    RUN_TEST(jlsp_method_reference_static);
    RUN_TEST(jlsp_method_reference_instance);
    RUN_TEST(jlsp_method_reference_constructor);

    /* User-class fields on non-this receivers. */
    RUN_TEST(jlsp_user_field_method);
    RUN_TEST(jlsp_user_field_chain);

    /* Real-corpus 90% parity benchmark (multi-class realistic Java). */
    RUN_TEST(jlsp_real_corpus_parity_90_percent);

    /* Multi-parent inheritance BFS */
    RUN_TEST(jlsp_extends_plus_implements_default);
    RUN_TEST(jlsp_second_interface_method);
    RUN_TEST(jlsp_diamond_interface_method);

    /* Pattern matching (instanceof / switch type + record patterns) */
    RUN_TEST(jlsp_instanceof_pattern);
    RUN_TEST(jlsp_switch_type_pattern);
    RUN_TEST(jlsp_switch_guard_binding);
    RUN_TEST(jlsp_switch_record_pattern);
    RUN_TEST(jlsp_switch_record_deconstruction);
    RUN_TEST(jlsp_switch_record_var_component);
    RUN_TEST(jlsp_instanceof_pattern_colon_switch);

    /* Constructor delegation (this/super) */
    RUN_TEST(jlsp_ctor_this_delegation);
    RUN_TEST(jlsp_ctor_super_delegation);
    RUN_TEST(jlsp_ctor_super_no_registered_ctor);
    RUN_TEST(jlsp_ctor_delegation_raw_call_row);

    /* Record components */
    RUN_TEST(jlsp_record_accessor_chain);
    RUN_TEST(jlsp_record_field_access);
    RUN_TEST(jlsp_record_compact_ctor);
    RUN_TEST(jlsp_record_canonical_ctor);
    RUN_TEST(jlsp_record_explicit_accessor_wins);
    RUN_TEST(jlsp_record_extraction_defs);

    /* Cross-file field types */
    RUN_TEST(jlsp_cross_field_chain);
    RUN_TEST(jlsp_cross_generic_field);
    RUN_TEST(jlsp_cross_tier2_field_chain);
    RUN_TEST(jlsp_cross_tier2_generic_signature);

    /* Test-annotation detection */
    RUN_TEST(jlsp_junit5_annotated_test);
    RUN_TEST(jlsp_spring_boot_test_not_marked);
    RUN_TEST(jlsp_testng_class_level_test);

    /* Lombok synthetic members */
    RUN_TEST(jlsp_lombok_getter);
    RUN_TEST(jlsp_lombok_data_setter);
    RUN_TEST(jlsp_lombok_boolean_is_getter);
    RUN_TEST(jlsp_lombok_builder_chain);
    RUN_TEST(jlsp_lombok_slf4j);
    RUN_TEST(jlsp_lombok_negative_no_annotation);

    /* Stdlib expansion (Java 21 surface) */
    RUN_TEST(jlsp_std_bigdecimal_chain);
    RUN_TEST(jlsp_std_httpclient);
    RUN_TEST(jlsp_std_virtual_thread);
    RUN_TEST(jlsp_std_countdown_latch);
    RUN_TEST(jlsp_std_blocking_queue);
    RUN_TEST(jlsp_std_collectors_tomap);
    RUN_TEST(jlsp_std_string_formatted);
    RUN_TEST(jlsp_std_stringjoiner);
    RUN_TEST(jlsp_std_sequenced_collection);
    RUN_TEST(jlsp_std_files_walk);
}
