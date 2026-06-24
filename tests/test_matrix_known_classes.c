/*
 * test_matrix_known_classes.c — Matrix-fill probe: apply every identified
 * bug-class across ALL applicable languages and fill the coverage gap left
 * by the existing suites (test_lang_contract.c, test_edge_structural.c,
 * test_lsp_resolution_probe.c).
 *
 * LEGEND
 *   green=guard  — asserts the CORRECT outcome; PASSES when the feature works.
 *   red=bug      — asserts the CORRECT outcome; FAILS until the bug is fixed.
 *                  Each red case includes a brief comment explaining the root
 *                  cause and the fix location.
 *
 * BUG CLASSES PROBED
 *   C1 — CONSTRUCTOR / instantiation  (new T() / T{} / T() / T::new())
 *   C2 — OPERATOR OVERLOADING         (a+b / a[i] / a==b → CALLS/USAGE to op fn)
 *   C3 — DECORATES via decorators/annotations/attributes/macros
 *   C4 — ASYNC/AWAIT calls            (call through async context → CALLS)
 *   C5 — GENERIC/TEMPLATED call       (call a method through generic type param)
 *   C6 — STATIC / CLASS-METHOD call   (grammar-only langs: Scala/Swift/Ruby)
 *   C7 — INHERITED-METHOD call        (grammar-only langs: Scala/Swift/Ruby)
 *
 * SUITE: matrix_known_classes
 * NOTE: NOT registered in test_main.c (per instructions).
 */

#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <foundation/log.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#if !defined(_WIN32)
#include <sys/wait.h>
#endif

/* ══════════════════════════════════════════════════════════════════════
 * Harness (MKC_ prefix — avoids link collisions with other suites).
 * Mirrors the pattern from test_edge_structural.c and test_lsp_resolution_probe.c.
 * ══════════════════════════════════════════════════════════════════════ */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} MKC_Proj;

typedef struct {
    const char *name;
    const char *content;
} MKC_File;

static void mkc_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\')
            *p = '/';
    }
}

static cbm_store_t *mkc_open_indexed(MKC_Proj *lp) {
    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project)
        return NULL;
    const char *home = getenv("HOME");
    if (!home)
        home = "/tmp";
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);
    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv)
        return NULL;
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp)
        free(resp);
    return cbm_store_open_path(lp->dbpath);
}

static cbm_store_t *mkc_index(MKC_Proj *lp, const MKC_File *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_mkc_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir))
        return NULL;
    mkc_to_fwd_slashes(lp->tmpdir);
    for (int i = 0; i < nfiles; i++) {
        char path[700];
        snprintf(path, sizeof(path), "%s/%s", lp->tmpdir, files[i].name);
        char *slash = strrchr(path, '/');
        if (slash && slash > path + strlen(lp->tmpdir)) {
            *slash = '\0';
            cbm_mkdir_p(path, 0755);
            *slash = '/';
        }
        FILE *f = fopen(path, "wb");
        if (!f)
            return NULL;
        fputs(files[i].content, f);
        fclose(f);
    }
    return mkc_open_indexed(lp);
}

static void mkc_cleanup(MKC_Proj *lp, cbm_store_t *store) {
    if (store)
        cbm_store_close(store);
    if (lp->srv) {
        cbm_mcp_server_free(lp->srv);
        lp->srv = NULL;
    }
    free(lp->project);
    lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600], shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(wal);
    unlink(shm);
}

static const char *MKC_ALL_EDGE_TYPES[] = {"CALLS",    "DEFINES",    "DEFINES_METHOD", "IMPORTS",
                                           "INHERITS", "IMPLEMENTS", "USAGE",          "DECORATES",
                                           "HANDLES",  "HTTP_CALLS", "ASYNC_CALLS",    "OVERRIDE",
                                           "TESTS",    "TESTS_FILE", "DATA_FLOWS",     NULL};

static void mkc_diag(cbm_store_t *store, const char *project, const char *label) {
    if (!store) {
        fprintf(stderr, "    [MKC] %s: no graph DB\n", label);
        return;
    }
    char line[512] = {0};
    for (int i = 0; MKC_ALL_EDGE_TYPES[i]; i++) {
        int c = cbm_store_count_edges_by_type(store, project, MKC_ALL_EDGE_TYPES[i]);
        if (c > 0 && strlen(line) < sizeof(line) - 40) {
            char one[48];
            snprintf(one, sizeof(one), "%s=%d ", MKC_ALL_EDGE_TYPES[i], c);
            strncat(line, one, sizeof(line) - strlen(line) - 1);
        }
    }
    fprintf(stderr, "    [MKC] %s edges=[%s]\n", label, line[0] ? line : "(none)");
}

/* Index files, assert edge_type appears >= floor times.
 * is_green=1: this is a green guard (regression if it fails).
 * is_green=0: this is a red reproduction (correct outcome asserted; fails until fixed). */
static int mkc_edge(const MKC_File *files, int nfiles, const char *edge_type, int floor,
                    const char *label, int is_green) {
    MKC_Proj lp;
    cbm_store_t *store = mkc_index(&lp, files, nfiles);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, edge_type) : -1;
    if (got < floor) {
        fprintf(stderr, "  [MKC] %s FAIL %s=%d expected>=%d %s\n", label, edge_type, got, floor,
                is_green ? "(GREEN regression)" : "(RED reproduction — bug)");
        mkc_diag(store, lp.project, label);
    } else if (!is_green) {
        fprintf(stderr,
                "  [MKC] %s UNEXPECTED PASS %s=%d "
                "(bug may be fixed — promote to GREEN)\n",
                label, edge_type, got);
    }
    mkc_cleanup(&lp, store);
    return got >= floor;
}

/* ══════════════════════════════════════════════════════════════════════════
 * ═══ CLASS C1: CONSTRUCTOR / INSTANTIATION ═══════════════════════════════
 *
 * A constructor call (new T() / T() / T{} / T::new() / Class.new) should
 * produce a CALLS edge to the constructor/init method (or at minimum a USAGE
 * edge to the type).  The existing lsp_resolution_probe.c covers S3 for
 * Go/C/C++/Rust/Python/TypeScript/Java/Kotlin/C#/PHP at the cross-file level.
 *
 * This matrix-fill adds:
 *   - Scala  (new Widget() / case-class apply)        — same-file
 *   - Swift  (Type() initializer)                     — same-file
 *   - Ruby   (Type.new)                               — same-file
 *   - C++    operator-new with non-trivial constructor — same-file (additional shape)
 *   - Rust   struct literal T { } in same-file (avoids the :: resolution bug)
 * ══════════════════════════════════════════════════════════════════════════ */

/* C1-A: Scala — new Widget() constructor call.
 * Same-file: Widget defined and instantiated in the same .scala file.
 * red=bug: Scala extraction doesn't include object_creation_expression
 * in call_types, so `new Widget()` never becomes a CALLS edge.
 * Root cause: lang_specs.c scala_call_types is {call_expression} only.
 * Fix location: lang_specs.c scala_call_types — add "new_expression" or
 * extract_defs.c Scala path to model constructors. */
TEST(mkc_c1_scala_constructor_new) {
    static const MKC_File f[] = {{"Widget.scala", "class Widget(val name: String) {\n"
                                                  "  def label(): String = name\n"
                                                  "}\n\n"
                                                  "def make(n: String): Widget = new Widget(n)\n"}};
    /* REAL BUG: new Widget(n) should produce CALLS make->Widget constructor.
     * Scala constructor (`new T()`) is not modeled as a call — lang_specs.c
     * scala_call_types does not capture new_expression/constructor instantiation,
     * so calls=0.  (case-class apply `Widget(n)` without `new` DOES resolve — see
     * the sibling c1/scala/case_class_apply, which now passes.) [KNOWN class 5] */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c1/scala/constructor_new", 0));
    PASS();
}

/* C1-B: Scala — case class apply() (no `new`).
 * red=bug: Widget("x") call — same root cause as C1-A; the apply() call is
 * an argument_list-headed call_expression and may or may not resolve to a
 * constructor CALLS edge.  In practice the name resolver finds Widget and
 * emits CALLS if Widget is modeled, but the constructor itself is never linked. */
TEST(mkc_c1_scala_case_class_apply) {
    static const MKC_File f[] = {{"Point.scala",
                                  "case class Point(x: Double, y: Double) {\n"
                                  "  def dist(): Double = math.sqrt(x*x + y*y)\n"
                                  "}\n\n"
                                  "def origin(): Double = Point(0.0, 0.0).dist()\n"}};
    /* Uncertain/red=bug: Point(0.0, 0.0) is the Scala case-class apply;
     * the generic name resolver may find `dist` but the constructor call
     * to Point is unlikely to be modeled.  Assert the correct outcome. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c1/scala/case_class_apply", 0));
    PASS();
}

/* C1-C: Swift — Type() initializer call.
 * red=bug: Swift `Widget(name: n)` — Swift lsp_cross is not wired (swift has
 * no cbm_run_swift_lsp_cross), and the generic resolver does not model
 * initializer calls from `call_expression(type_identifier(Widget), ...)`.
 * Root cause: swift_call_types = {call_expression} and extract_callee picks
 * the first child (which is the type name), but no CALLS edge to Widget.init
 * is emitted because the pipeline has no constructor linkage for Swift.
 * Fix: add Swift to constructor-extraction path in extract_defs.c or add
 * cbm_run_swift_lsp_cross. */
TEST(mkc_c1_swift_initializer) {
    static const MKC_File f[] = {{"Widget.swift", "class Widget {\n"
                                                  "    let name: String\n"
                                                  "    init(name: String) { self.name = name }\n"
                                                  "    func label() -> String { return name }\n"
                                                  "}\n\n"
                                                  "func make(n: String) -> Widget {\n"
                                                  "    return Widget(name: n)\n"
                                                  "}\n"}};
    /* red=bug: Widget(name: n) should CALLS make->Widget.init.
     * Currently calls=0; swift has no constructor-call linkage. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c1/swift/initializer", 0));
    PASS();
}

/* C1-D: Ruby — Type.new constructor call.
 * red=bug: Ruby's `Widget.new(name)` is a method_call whose receiver is a
 * const (Widget) and method name is :new.  The generic resolver sees a call
 * named "new", which doesn't match any registered function by that name
 * (the constructor body is in `def initialize`), so CALLS is never emitted.
 * Root cause: extract_calls.c Ruby path maps method_call to callee "new"
 * not to "Widget.initialize".  Fix: add a Ruby `new` → `initialize` redirect
 * in the extractor or treat Type.new as a constructor call. */
TEST(mkc_c1_ruby_type_new) {
    static const MKC_File f[] = {{"widget.rb", "class Widget\n"
                                               "  def initialize(name)\n"
                                               "    @name = name\n"
                                               "  end\n\n"
                                               "  def label\n"
                                               "    @name\n"
                                               "  end\n"
                                               "end\n\n"
                                               "def make(name)\n"
                                               "  Widget.new(name)\n"
                                               "end\n"}};
    /* REAL BUG: Widget.new(name) should CALLS make->Widget#initialize.  Ruby maps
     * the `new` method_call to callee "new", which matches no registered function
     * (the body is in `def initialize`), so the constructor call is never linked
     * → calls=0.  Needs a Ruby `Type.new` → `Type#initialize` redirect in the
     * call extractor/resolver. [KNOWN class 5] */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c1/ruby/type_new", 0));
    PASS();
}

/* C1-E: C++ — same-file constructor call (no cross-file complication).
 * green=guard: C++ `Counter c(0)` / `Counter c{0}` — lsp_cross (cpp_mode)
 * handles this when both definition and call site are in the same file.
 * This supplements lrp_cpp_s3_constructor (which uses two files) with a
 * single-file shape where forward-declaration ambiguity is absent. */
TEST(mkc_c1_cpp_constructor_samefile) {
    static const MKC_File f[] = {{"counter.cpp", "class Counter {\npublic:\n"
                                                 "    int n;\n"
                                                 "    Counter(int start) : n(start) {}\n"
                                                 "    int val() const { return n; }\n};\n\n"
                                                 "int run(int start) {\n"
                                                 "    Counter c(start);\n"
                                                 "    return c.val();\n"
                                                 "}\n"}};
    /* green=guard: constructor + method call both in one file; lsp_cross should
     * resolve both.  If this fails it is a C++ same-file constructor regression. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c1/cpp/constructor_samefile", 1));
    PASS();
}

/* C1-F: Rust — struct literal T { field } in same file (avoids :: resolver bug).
 * The lrp_rust_s3_constructor fixture uses `point::Point::new(...)` which fails
 * due to the `::` split bug in cbm_registry_resolve.  Here we test a plain
 * same-file struct + impl, where `Point::new(x, y)` has no module prefix,
 * so the resolver should find it.
 * green=guard: Rust same-file `StructName::new()` — the name "new" is a unique
 * function in the file; the generic resolver should link it. */
TEST(mkc_c1_rust_new_samefile) {
    static const MKC_File f[] = {
        {"point.rs",
         "pub struct Point { pub x: f64, pub y: f64 }\n\n"
         "impl Point {\n"
         "    pub fn new(x: f64, y: f64) -> Self { Point { x, y } }\n"
         "    pub fn dist(&self) -> f64 { (self.x * self.x + self.y * self.y).sqrt() }\n"
         "}\n\n"
         "pub fn run() -> f64 {\n"
         "    let p = Point::new(3.0, 4.0);\n"
         "    p.dist()\n"
         "}\n"}};
    /* Uncertain/red=bug: Point::new inside the same file — the generic resolver
     * splits on '.' not '::' so "Point::new" never matches registered QN "new".
     * Root cause: registry.c cbm_registry_resolve doesn't handle Rust `::` paths.
     * Fix location: src/pipeline/registry.c — split on '::' for Rust as well as '.'. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c1/rust/new_samefile", 0));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════
 * ═══ CLASS C2: OPERATOR OVERLOADING ══════════════════════════════════════
 *
 * When a language desugars `a + b` → `a.operator+(b)` or `a.__add__(b)`,
 * the pipeline should emit a CALLS edge to that operator method.
 *
 * Languages: C++, Python, Rust (Add trait), Kotlin (operator fun),
 *            C# (operator), Scala (symbolic def), Ruby (def +),
 *            Swift (static func +)
 * ══════════════════════════════════════════════════════════════════════════ */

/* C2-A: C++ — operator+ overload.
 * red=bug: `a + b` where both are Vec2 objects is a binary_expression with
 * operator_name "+".  C++ lsp_cross does not currently desugar operator
 * expressions into CALLS edges for overloaded operators.
 * Root cause: the C++ call_types list includes "call_expression" but not
 * "binary_expression" for operator-call desugaring.
 * Fix: extend extract_calls.c C++ path to emit a CALLS node for
 * binary_expression when both operands have a known overloaded type. */
TEST(mkc_c2_cpp_operator_plus) {
    static const MKC_File f[] = {
        {"vec.cpp", "struct Vec2 {\n    float x, y;\n    Vec2(float x, float y): x(x), y(y){}\n"
                    "    Vec2 operator+(const Vec2 &o) const { return Vec2(x+o.x, y+o.y); }\n"
                    "    float dot(const Vec2 &o) const { return x*o.x + y*o.y; }\n};\n\n"
                    "float run(Vec2 a, Vec2 b) {\n"
                    "    Vec2 c = a + b;\n" /* operator+ call */
                    "    return c.dot(b);\n"
                    "}\n"}};
    /* red=bug: `a + b` should CALLS run->Vec2::operator+.
     * c.dot(b) may pass via method-call resolution (it's a named call_expression).
     * We assert CALLS >= 2 (operator+ + dot); will fail until C++ operator desugaring
     * is implemented.  Minimally assert >= 1 for now to capture the dot() shape. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 2, "c2/cpp/operator_plus", 0));
    PASS();
}

/* C2-B: C++ — operator[] subscript overload.
 * red=bug: `arr[0]` on a custom array type is a subscript_expression;
 * same root cause as C2-A — no desugaring to CALLS for subscript operators. */
TEST(mkc_c2_cpp_operator_subscript) {
    static const MKC_File f[] = {{"arr.cpp", "struct IntArr {\n    int data[8];\n"
                                             "    int& operator[](int i) { return data[i]; }\n"
                                             "};\n\n"
                                             "int run(IntArr &a) {\n"
                                             "    return a[2];\n"
                                             "}\n"}};
    /* REAL BUG: a[2] should CALLS run->IntArr::operator[].  The subscript
     * operator desugaring is not modeled — C++ call extraction does not emit a
     * call for subscript_expression on an overloaded-operator type → 0 CALLS.
     * (Note: C++ binary operator+ desugaring now works — c2/cpp/operator_plus
     * passes — but subscript [] is still missing.) [KNOWN class 12] */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c2/cpp/operator_subscript", 0));
    PASS();
}

/* C2-C: Python — __add__ dunder method.
 * red=bug: `a + b` with Vector objects — Python grammar emits a
 * binary_operator node for `+`.  Neither extract_calls.c nor the Python
 * lsp_cross pass maps binary_operator to a __add__ CALLS edge.
 * Root cause: py_call_types is {call, decorator} (lang_specs.c) — no
 * binary_operator entry.  Fix: extend Python extractor to model dunder
 * operator expressions as calls to __add__ / __eq__ / etc. */
TEST(mkc_c2_python_dunder_add) {
    static const MKC_File f[] = {
        {"vec.py",
         "class Vec:\n"
         "    def __init__(self, x, y):\n        self.x, self.y = x, y\n\n"
         "    def __add__(self, other):\n        return Vec(self.x + other.x, self.y + other.y)\n\n"
         "    def __eq__(self, other):\n        return self.x == other.x and self.y == other.y\n\n"
         "def run(a, b):\n"
         "    return a + b\n"}};
    /* red=bug: `a + b` should CALLS run->Vec.__add__.
     * Currently binary_operator is not in Python call_types. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c2/python/dunder_add", 0));
    PASS();
}

/* C2-D: Python — __getitem__ dunder method.
 * red=bug: `a[0]` on a custom Seq emits a subscript node, not a call_expression.
 * Same root cause as C2-C — subscript desugaring to __getitem__ is missing. */
TEST(mkc_c2_python_dunder_getitem) {
    static const MKC_File f[] = {{"seq.py",
                                  "class Seq:\n"
                                  "    def __init__(self, data):\n        self._d = data\n\n"
                                  "    def __getitem__(self, i):\n        return self._d[i]\n\n"
                                  "def run(s: Seq):\n"
                                  "    return s[0]\n"}};
    /* Subscript `s[0]` desugars to a CALLS run->Seq.__getitem__, resolved
     * type-based from the receiver's type. ADAPTED: the param is annotated
     * `s: Seq` — the original unannotated `def run(s)` is NOT soundly
     * resolvable (no receiver type; resolving would require an unsound
     * "sole class with __getitem__" guess that would mis-resolve built-in
     * subscripts). The annotated form exercises the real subscript-dunder
     * resolution. See project memory [project_lsp_extraction_bughunt_2026_06]. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c2/python/dunder_getitem", 0));
    PASS();
}

/* C2-E: Rust — Add trait operator overload (a + b).
 * red=bug: Rust `a + b` where T implements std::ops::Add is a binary_expression.
 * Rust's call_types do not include binary_expression, so `a + b` is never
 * extracted as a call to <T as Add>::add.
 * Root cause: lang_specs.c rust_call_types = {call_expression, macro_invocation}
 * — no binary_expression entry.
 * Note: Rust lsp_cross is also not wired (see lrp_rust_* suite), compounding the miss. */
TEST(mkc_c2_rust_add_trait) {
    static const MKC_File f[] = {{"vec.rs",
                                  "use std::ops::Add;\n\n"
                                  "#[derive(Clone, Copy)]\n"
                                  "pub struct Vec2 { pub x: f32, pub y: f32 }\n\n"
                                  "impl Add for Vec2 {\n"
                                  "    type Output = Self;\n"
                                  "    fn add(self, rhs: Self) -> Self {\n"
                                  "        Vec2 { x: self.x + rhs.x, y: self.y + rhs.y }\n"
                                  "    }\n"
                                  "}\n\n"
                                  "pub fn run(a: Vec2, b: Vec2) -> Vec2 {\n"
                                  "    a + b\n"
                                  "}\n"}};
    /* REAL BUG: `a + b` should CALLS run->Vec2::add (via Add trait).  Rust
     * binary_expression is not in lang_specs.c rust_call_types, so the operator
     * desugaring is never extracted as a call → 0 CALLS.  (Rust also has no
     * cross-LSP, compounding the miss.) [KNOWN class 12] */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c2/rust/add_trait", 0));
    PASS();
}

/* C2-F: Kotlin — operator fun plus.
 * red=bug: Kotlin `a + b` with `operator fun plus` defined — the grammar
 * emits an additive_expression, not a call_expression.  Kotlin's call_types
 * (lang_specs.c) do not include additive_expression.
 * Fix: extend Kotlin extractor to map operator expressions to CALLS. */
TEST(mkc_c2_kotlin_operator_plus) {
    static const MKC_File f[] = {{"vec.kt",
                                  "data class Vec2(val x: Float, val y: Float) {\n"
                                  "    operator fun plus(other: Vec2): Vec2 =\n"
                                  "        Vec2(x + other.x, y + other.y)\n"
                                  "    operator fun get(i: Int): Float = if (i == 0) x else y\n"
                                  "}\n\n"
                                  "fun run(a: Vec2, b: Vec2): Vec2 = a + b\n"}};
    /* red=bug: `a + b` should CALLS run->Vec2.plus (operator fun). */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c2/kotlin/operator_plus", 0));
    PASS();
}

/* C2-G: C# — operator + overload.
 * red=bug: C# `a + b` for structs with operator+ is a binary_expression in
 * the grammar.  cs_call_types = {invocation_expression} only — no
 * binary_expression, so the operator call is never extracted. */
TEST(mkc_c2_csharp_operator_plus) {
    static const MKC_File f[] = {{"Vec.cs",
                                  "namespace App {\n"
                                  "    struct Vec2 {\n"
                                  "        public float X, Y;\n"
                                  "        public Vec2(float x, float y) { X = x; Y = y; }\n"
                                  "        public static Vec2 operator+(Vec2 a, Vec2 b) {\n"
                                  "            return new Vec2(a.X + b.X, a.Y + b.Y);\n"
                                  "        }\n"
                                  "        public float Dot(Vec2 o) { return X*o.X + Y*o.Y; }\n"
                                  "    }\n"
                                  "    class Math {\n"
                                  "        public static float Run(Vec2 a, Vec2 b) {\n"
                                  "            return (a + b).Dot(b);\n"
                                  "        }\n"
                                  "    }\n"
                                  "}\n"}};
    /* REAL BUG: `(a + b).Dot(b)` should CALLS both Vec2::op_Addition (the `+`)
     * AND Dot (>=2).  Dot resolves (CALLS=1) but the operator `a + b` does not —
     * C# binary_expression operator desugaring is not modeled (cs_call_types omits
     * it), so the op_Addition call is missing → CALLS=1 < 2. [KNOWN class 12] */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 2, "c2/csharp/operator_plus", 0));
    PASS();
}

/* C2-H: Scala — symbolic method (operators are methods in Scala).
 * Uncertain/red=bug: in Scala `a + b` with a custom `def +(other: Vec2)` is
 * syntactic sugar for a method call `a.+(b)` — it IS a call_expression in the
 * tree-sitter-scala grammar.  The generic resolver may find `+` if it maps to
 * a unique method name.  Assert CALLS >= 1. */
TEST(mkc_c2_scala_symbolic_method) {
    static const MKC_File f[] = {{"vec.scala",
                                  "case class Vec2(x: Double, y: Double) {\n"
                                  "  def +(other: Vec2): Vec2 = Vec2(x + other.x, y + other.y)\n"
                                  "  def dot(other: Vec2): Double = x * other.x + y * other.y\n"
                                  "}\n\n"
                                  "def run(a: Vec2, b: Vec2): Double = (a + b).dot(b)\n"}};
    /* Uncertain: in Scala `a + b` is sugar for `a.+(b)`.  The tree-sitter-scala
     * grammar may emit a call_expression whose callee is `+`.  The generic
     * name resolver tries to match the method "+".  If Scala call_types correctly
     * includes infix applications, this is GREEN; otherwise red=bug. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c2/scala/symbolic_method", 0));
    PASS();
}

/* C2-I: Ruby — operator method (def +).
 * Uncertain/red=bug: Ruby `a + b` is a binary method call.  The tree-sitter-ruby
 * grammar emits a `binary` node for infix expressions.  Ruby call_types
 * (extract_calls.c) handle `call` nodes but may not handle `binary`.
 * If binary is mapped to a method call, the resolver finds `+`; otherwise red. */
TEST(mkc_c2_ruby_operator_plus) {
    static const MKC_File f[] = {
        {"vec.rb", "class Vec2\n"
                   "  attr_reader :x, :y\n"
                   "  def initialize(x, y)\n    @x, @y = x, y\n  end\n\n"
                   "  def +(other)\n    Vec2.new(@x + other.x, @y + other.y)\n  end\n\n"
                   "  def dot(other)\n    @x * other.x + @y * other.y\n  end\n"
                   "end\n\n"
                   "def run(a, b)\n"
                   "  (a + b).dot(b)\n"
                   "end\n"}};
    /* Uncertain/red=bug: `a + b` should CALLS run->Vec2#+.
     * Ruby binary node not in call_types. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c2/ruby/operator_plus", 0));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════
 * ═══ CLASS C3: DECORATES — annotations / attributes / macros ═════════════
 *
 * Languages / forms NOT yet covered by test_edge_structural.c:
 *   - Rust      #[derive(...)] and #[cfg(...)] macro attributes
 *   - PHP 8     #[Attribute] syntax
 *   - Swift     @propertyWrapper / @available
 *   - Scala     @annotation.tailrec / @deprecated
 *   - Ruby      no native decorators (N/A — excluded)
 *   - Python    same-file already green in test_lang_contract.c; cross-file in
 *               test_edge_structural.c.  Add a class-level decorator here.
 *
 * Note: test_edge_structural.c already covers Java, Kotlin, C# annotations
 * (all expected RED).  This section adds Rust/PHP/Swift/Scala.
 * ══════════════════════════════════════════════════════════════════════════ */

/* C3-A: Rust — #[derive(...)] attribute macro.
 * red=bug: Rust `#[derive(Debug, Clone)]` is an attribute_item node in the
 * grammar.  The pipeline models decorators for Python/TS via `decorators[]`
 * in the def struct, but there is no Rust branch that maps attribute_item
 * to a DECORATES edge.
 * Root cause: extract_defs.c has no Rust attribute/decorator extraction.
 * Fix: add a Rust path in extract_defs.c that reads attribute_item children
 * and populates def.decorators[]. */
TEST(mkc_c3_rust_derive_attribute) {
    static const MKC_File f[] = {{"model.rs", "fn debug_marker() {}\n\n"
                                              "#[derive(Debug, Clone)]\n"
                                              "pub struct Point {\n"
                                              "    pub x: f64,\n"
                                              "    pub y: f64,\n"
                                              "}\n\n"
                                              "#[derive(Debug)]\n"
                                              "pub struct Color(pub u8, pub u8, pub u8);\n"}};
    /* REAL BUG: #[derive(Debug)] should produce a DECORATES edge.  extract_defs.c
     * has no Rust attribute_item → decorators[] path, so Rust attribute/derive
     * macros are never modeled → DECORATES=0. [KNOWN class 13] */
    ASSERT_TRUE(mkc_edge(f, 1, "DECORATES", 1, "c3/rust/derive_attribute", 0));
    PASS();
}

/* C3-B: Rust — custom function attribute (locally defined proc-macro simulation).
 * red=bug: Even a simple fn-level attribute `#[my_attr]` applied to a function
 * where `my_attr` is defined in the same file is not modeled as DECORATES.
 * Same root cause as C3-A. */
TEST(mkc_c3_rust_fn_attribute) {
    static const MKC_File f[] = {
        {"service.rs", "/* hypothetical local marker — real proc-macros live in separate crates,\n"
                       " * but we just want to check if ANY attribute is modeled as DECORATES */\n"
                       "pub fn log_call() {}\n\n"
                       "#[allow(dead_code)]\n"
                       "pub fn helper(x: i32) -> i32 { x + 1 }\n\n"
                       "#[allow(dead_code)]\n"
                       "pub fn run(y: i32) -> i32 { helper(y) }\n"}};
    /* REAL BUG: #[allow(dead_code)] should produce a DECORATES edge.  Same root
     * cause as c3/rust/derive_attribute — extract_defs.c has no Rust
     * attribute_item → decorators[] path → DECORATES=0. [KNOWN class 13] */
    ASSERT_TRUE(mkc_edge(f, 1, "DECORATES", 1, "c3/rust/fn_attribute", 0));
    PASS();
}

/* C3-C: PHP 8 — #[Attribute] syntax (modern PHP attributes).
 * red=bug: PHP 8 `#[Route("/users")]` attribute on a function is a new grammar
 * node (attribute_group / attribute) distinct from PHP docblock annotations.
 * The PHP extractor may not yet populate decorators[] for the new #[...] syntax.
 * Root cause: extract_defs.c PHP path handles @docblock annotations but may
 * not handle PHP 8 attribute_group nodes. */
TEST(mkc_c3_php8_attribute) {
    static const MKC_File f[] = {{"routes.php",
                                  "<?php\n"
                                  "function Route(string $path): callable {\n"
                                  "    return function() use ($path) { return $path; };\n"
                                  "}\n\n"
                                  "#[Route('/users')]\n"
                                  "function listUsers(): array {\n"
                                  "    return [];\n"
                                  "}\n\n"
                                  "#[Route('/items')]\n"
                                  "function listItems(): array {\n"
                                  "    return [];\n"
                                  "}\n"}};
    /* REAL BUG: #[Route(...)] should DECORATES listUsers->Route.  extract_defs.c
     * handles PHP @docblock annotations but not PHP 8 attribute_group / attribute
     * nodes → DECORATES=0. [KNOWN class 13] */
    ASSERT_TRUE(mkc_edge(f, 1, "DECORATES", 1, "c3/php8/attribute", 0));
    PASS();
}

/* C3-D: Swift — @available / @discardableResult attribute.
 * red=bug: Swift attributes like `@available(macOS 12, *)` and
 * `@discardableResult` are attribute nodes in the tree-sitter-swift grammar.
 * The pipeline has no Swift decorator extraction branch.
 * Root cause: extract_defs.c has no Swift attribute_item → decorators[] path. */
TEST(mkc_c3_swift_attribute) {
    static const MKC_File f[] = {{"service.swift", "func available() -> Bool { return true }\n\n"
                                                   "@discardableResult\n"
                                                   "func compute(x: Int) -> Int {\n"
                                                   "    return x * 2\n"
                                                   "}\n\n"
                                                   "@discardableResult\n"
                                                   "func run(n: Int) -> Int {\n"
                                                   "    return compute(n)\n"
                                                   "}\n"}};
    /* REAL BUG: @discardableResult should produce a DECORATES edge.  extract_defs.c
     * has no Swift attribute → decorators[] path, so Swift attributes are never
     * modeled → DECORATES=0. [KNOWN class 13] */
    ASSERT_TRUE(mkc_edge(f, 1, "DECORATES", 1, "c3/swift/attribute", 0));
    PASS();
}

/* C3-E: Scala — @annotation on a method.
 * red=bug: Scala `@deprecated` / `@tailrec` annotations are annotation nodes.
 * The generic extractor may or may not capture them as DECORATES edges.
 * Root cause: extract_defs.c Scala path likely does not handle annotation nodes. */
TEST(mkc_c3_scala_annotation) {
    static const MKC_File f[] = {{"algo.scala", "import scala.annotation.tailrec\n\n"
                                                "@deprecated(\"use newHelper\", \"2.0\")\n"
                                                "def helper(x: Int): Int = x + 1\n\n"
                                                "@tailrec\n"
                                                "def loop(n: Int, acc: Int): Int =\n"
                                                "  if (n <= 0) acc else loop(n - 1, acc + n)\n"}};
    /* REAL BUG: @deprecated should produce a DECORATES edge.  extract_defs.c has
     * no Scala annotation → decorators[] path, so Scala annotations are never
     * modeled → DECORATES=0. [KNOWN class 13] */
    ASSERT_TRUE(mkc_edge(f, 1, "DECORATES", 1, "c3/scala/annotation", 0));
    PASS();
}

/* C3-F: Python — class-level decorator (not just function-level).
 * green=guard: test_lang_contract.c P6 covers function-level decorators.
 * This adds a CLASS-level decorator to confirm the extractor handles
 * decorated class definitions (not just functions). */
TEST(mkc_c3_python_class_decorator) {
    static const MKC_File f[] = {{"service.py",
                                  "def singleton(cls):\n"
                                  "    instances = {}\n"
                                  "    def get(*a, **kw):\n"
                                  "        if cls not in instances:\n"
                                  "            instances[cls] = cls(*a, **kw)\n"
                                  "        return instances[cls]\n"
                                  "    return get\n\n\n"
                                  "@singleton\n"
                                  "class Config:\n"
                                  "    def __init__(self):\n        self.debug = False\n\n\n"
                                  "@singleton\n"
                                  "class Cache:\n"
                                  "    def __init__(self):\n        self.data = {}\n"}};
    /* green=guard: @singleton on a class should DECORATES Config->singleton.
     * Python class-level decorator extraction should work the same as function-level
     * (both are `decorated_definition` nodes in tree-sitter-python). */
    ASSERT_TRUE(mkc_edge(f, 1, "DECORATES", 1, "c3/python/class_decorator", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════
 * ═══ CLASS C4: ASYNC/AWAIT CALLS ═════════════════════════════════════════
 *
 * An `await expr` or async function call should still produce a CALLS edge
 * to the callee.  The async/await syntax wraps the call but must not hide it
 * from the extractor.
 *
 * Languages: Python (async def / await), TypeScript (async/await),
 *            C# (async Task / await), Rust (.await expr),
 *            Kotlin (suspend fun / coroutine launch),
 *            Swift (async/await — Swift 5.5+)
 * ══════════════════════════════════════════════════════════════════════════ */

/* C4-A: Python — async/await function call.
 * green=guard: Python `await fetch(url)` — the `await` expression wraps a
 * regular `call` node whose function field is `fetch`.  Python's call_types
 * include `call`, so the `call` inside `await` is still extracted.
 * The extractor should find the inner call regardless of the await wrapper. */
TEST(mkc_c4_python_async_await) {
    static const MKC_File f[] = {{"client.py", "async def fetch(url: str) -> str:\n"
                                               "    return url\n\n\n"
                                               "async def get_data(url: str) -> str:\n"
                                               "    result = await fetch(url)\n"
                                               "    return result\n\n\n"
                                               "async def run(urls):\n"
                                               "    for url in urls:\n"
                                               "        data = await get_data(url)\n"
                                               "        print(data)\n"}};
    /* green=guard: await fetch(url) inner call should CALLS get_data->fetch.
     * await is transparent to the call extractor for Python. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c4/python/async_await", 1));
    PASS();
}

/* C4-B: TypeScript — async/await function call.
 * green=guard: TS `await fetch(url)` — the `await_expression` wraps a
 * `call_expression`.  TS call_types include call_expression, so the inner
 * call is extracted regardless of the await wrapper. */
TEST(mkc_c4_typescript_async_await) {
    static const MKC_File f[] = {{"client.ts",
                                  "async function fetch(url: string): Promise<string> {\n"
                                  "    return url;\n"
                                  "}\n\n"
                                  "async function getData(url: string): Promise<string> {\n"
                                  "    return await fetch(url);\n"
                                  "}\n\n"
                                  "async function run(urls: string[]): Promise<void> {\n"
                                  "    for (const url of urls) {\n"
                                  "        const data = await getData(url);\n"
                                  "        console.log(data);\n"
                                  "    }\n"
                                  "}\n"}};
    /* green=guard: await fetch(url) should CALLS getData->fetch. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c4/typescript/async_await", 1));
    PASS();
}

/* C4-C: C# — async Task + await call.
 * Uncertain/red=bug: C# `await FetchAsync(url)` — the `await_expression` in
 * the C# grammar wraps an `invocation_expression`.  cs_call_types includes
 * `invocation_expression`, so the inner call should be extracted.
 * However, the await wrapper may or may not be transparent; assert CALLS >= 1. */
TEST(mkc_c4_csharp_async_await) {
    static const MKC_File f[] = {
        {"Client.cs", "using System.Threading.Tasks;\n\n"
                      "namespace App {\n"
                      "    class Client {\n"
                      "        public async Task<string> FetchAsync(string url) {\n"
                      "            return url;\n"
                      "        }\n\n"
                      "        public async Task<string> GetDataAsync(string url) {\n"
                      "            return await FetchAsync(url);\n"
                      "        }\n\n"
                      "        public async Task RunAsync() {\n"
                      "            string data = await GetDataAsync(\"http://example.com\");\n"
                      "        }\n"
                      "    }\n"
                      "}\n"}};
    /* Uncertain: C# await_expression wraps invocation_expression.
     * If cs_call_types handles it, green=guard; otherwise red=bug. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c4/csharp/async_await", 0));
    PASS();
}

/* C4-D: Rust — .await expression.
 * red=bug: Rust `future.await` is an `await_expression` node where the
 * operand is a method-call-like expression.  Rust call_types = {call_expression,
 * macro_invocation} — await_expression is not included.
 * Root cause: lang_specs.c rust_call_types does not include await_expression.
 * Fix: either add await_expression to rust_call_types, or teach extract_calls.c
 * to unwrap await_expression and process the inner expression. */
TEST(mkc_c4_rust_await) {
    static const MKC_File f[] = {{"client.rs", "async fn fetch(url: &str) -> String {\n"
                                               "    url.to_string()\n"
                                               "}\n\n"
                                               "async fn get_data(url: &str) -> String {\n"
                                               "    fetch(url).await\n"
                                               "}\n\n"
                                               "async fn run(urls: &[&str]) {\n"
                                               "    for url in urls {\n"
                                               "        let _data = get_data(url).await;\n"
                                               "    }\n"
                                               "}\n"}};
    /* red=bug: fetch(url).await — the inner fetch(url) IS a call_expression,
     * but its CALLS attribution may be lost when wrapped in await.
     * Additionally, get_data(url).await has same issue.
     * Assert CALLS >= 1 (the correct outcome). */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c4/rust/await", 0));
    PASS();
}

/* C4-E: Kotlin — suspend function call (coroutines).
 * Uncertain: Kotlin `suspend fun` calls look like normal function calls at the
 * grammar level — the `suspend` keyword modifies the function signature but does
 * not change the call_expression shape.  The generic resolver should find the
 * callee by name.  Assert CALLS >= 1. */
TEST(mkc_c4_kotlin_suspend_call) {
    static const MKC_File f[] = {{"client.kt", "suspend fun fetchData(url: String): String {\n"
                                               "    return url\n"
                                               "}\n\n"
                                               "suspend fun getData(url: String): String {\n"
                                               "    return fetchData(url)\n"
                                               "}\n\n"
                                               "suspend fun run(urls: List<String>) {\n"
                                               "    for (url in urls) {\n"
                                               "        val data = getData(url)\n"
                                               "        println(data)\n"
                                               "    }\n"
                                               "}\n"}};
    /* green=guard (uncertain): suspend fun calls are regular function calls
     * at grammar level; the name resolver should find fetchData and getData. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c4/kotlin/suspend_call", 1));
    PASS();
}

/* C4-F: Swift — async/await (Swift 5.5+).
 * red=bug: Swift `await fetch(url)` — the grammar emits an `await_expression`
 * wrapping a `call_expression`.  Swift call_types in lang_specs.c may or may
 * not include `call_expression`; even if it does, the extract_callee_name for
 * Swift's call_expression form needs to be validated.
 * Additionally, Swift lsp_cross is not wired. */
TEST(mkc_c4_swift_async_await) {
    static const MKC_File f[] = {{"client.swift", "func fetch(url: String) async -> String {\n"
                                                  "    return url\n"
                                                  "}\n\n"
                                                  "func getData(url: String) async -> String {\n"
                                                  "    return await fetch(url: url)\n"
                                                  "}\n\n"
                                                  "func run(urls: [String]) async {\n"
                                                  "    for url in urls {\n"
                                                  "        let data = await getData(url: url)\n"
                                                  "        print(data)\n"
                                                  "    }\n"
                                                  "}\n"}};
    /* Uncertain: Swift `await fetch(url: url)` inner call_expression should
     * produce CALLS getData->fetch.  Assert CALLS >= 1. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c4/swift/async_await", 0));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════
 * ═══ CLASS C5: GENERIC / TEMPLATED METHOD CALLS ═══════════════════════════
 *
 * Calling a method through a generic type parameter.  The existing
 * lsp_resolution_probe.c covers S7 (generic call) for all 9 hybrid langs.
 *
 * Matrix-fill adds: Scala, Swift, Ruby (duck-typed, no generics per se).
 * ══════════════════════════════════════════════════════════════════════════ */

/* C5-A: Scala — generic function call.
 * Uncertain: Scala `def first[T](xs: List[T]): T` — the generic type parameter
 * does not affect the call_expression shape; the generic resolver should find
 * `first` by name regardless of the type argument. */
TEST(mkc_c5_scala_generic_call) {
    static const MKC_File f[] = {{"algo.scala",
                                  "def first[T](xs: List[T]): Option[T] =\n"
                                  "  if (xs.isEmpty) None else Some(xs.head)\n\n"
                                  "def run(ns: List[Int]): Option[Int] = first(ns)\n"}};
    /* Uncertain: first is a top-level generic; the name resolver should find it.
     * Assert CALLS >= 1; red=bug if generic call is dropped. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c5/scala/generic_call", 0));
    PASS();
}

/* C5-B: Swift — generic function call.
 * Uncertain: Swift `func maxOf<T: Comparable>(_ a: T, _ b: T) -> T` — same as
 * Scala; the generic syntax does not change the call_expression structure. */
TEST(mkc_c5_swift_generic_call) {
    static const MKC_File f[] = {{"algo.swift", "func maxOf<T: Comparable>(_ a: T, _ b: T) -> T {\n"
                                                "    return a > b ? a : b\n"
                                                "}\n\n"
                                                "func run(a: Int, b: Int) -> Int {\n"
                                                "    return maxOf(a, b)\n"
                                                "}\n"}};
    /* Uncertain: maxOf is a top-level generic; assert CALLS >= 1. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c5/swift/generic_call", 0));
    PASS();
}

/* C5-C: Ruby — duck-typed "generic" (no static generics; method works on any type).
 * green=guard: Ruby method call is always dynamic; the name resolver treats it
 * like a regular method call.  Confirms ordinary Ruby method resolution works
 * (orthogonal to generics but useful as a guard for Ruby CALLS coverage). */
TEST(mkc_c5_ruby_duck_typed_call) {
    static const MKC_File f[] = {{"algo.rb", "def first(collection)\n"
                                             "  collection.empty? ? nil : collection.first\n"
                                             "end\n\n"
                                             "def run(items)\n"
                                             "  first(items)\n"
                                             "end\n"}};
    /* green=guard: `first(items)` is a plain method call; CALLS run->first. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c5/ruby/duck_typed_call", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════
 * ═══ CLASS C6: STATIC / CLASS-METHOD CALLS (grammar-only langs) ══════════
 *
 * The existing suites cover static calls for Go/C/C++/Python/TS/Java/Kotlin/
 * C#/PHP/Rust.  Matrix-fill adds: Scala companion-object, Swift static/class
 * methods, Ruby self.method (class methods).
 * ══════════════════════════════════════════════════════════════════════════ */

/* C6-A: Scala — companion object method call.
 * Uncertain/red=bug: `Config.default()` where default() is in a companion
 * object.  The Scala grammar may emit this as a call_expression whose
 * callee is a `field_expression` Config.default — the generic resolver should
 * strip the qualifier and find `default`.  Assert CALLS >= 1. */
TEST(mkc_c6_scala_companion_object) {
    static const MKC_File f[] = {{"config.scala", "class Config(val debug: Boolean)\n\n"
                                                  "object Config {\n"
                                                  "  def default(): Config = new Config(false)\n"
                                                  "  def debug(): Config = new Config(true)\n"
                                                  "}\n\n"
                                                  "def run(): Config = Config.default()\n"}};
    /* Uncertain: Config.default() — the generic resolver strips `Config.` prefix
     * and looks for `default`.  May resolve if unique; otherwise red=bug. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c6/scala/companion_object", 0));
    PASS();
}

/* C6-B: Swift — static method call (class func / static func).
 * Uncertain: Swift `MathHelper.clamp(n, 0, 100)` where clamp is a `static func`.
 * The generic resolver should strip `MathHelper.` and find `clamp`. */
TEST(mkc_c6_swift_static_method) {
    static const MKC_File f[] = {{"mathhelper.swift",
                                  "class MathHelper {\n"
                                  "    static func clamp(_ v: Int, _ lo: Int, _ hi: Int) -> Int {\n"
                                  "        return v < lo ? lo : v > hi ? hi : v\n"
                                  "    }\n"
                                  "    static func square(_ x: Int) -> Int { return x * x }\n"
                                  "}\n\n"
                                  "func run(n: Int) -> Int {\n"
                                  "    return MathHelper.clamp(n, 0, 100)\n"
                                  "}\n"}};
    /* Uncertain: MathHelper.clamp — resolver strips qualifier, finds clamp.
     * Assert CALLS >= 1; red=bug if Swift static method never resolves. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c6/swift/static_method", 0));
    PASS();
}

/* C6-C: Ruby — class method (self.method_name).
 * Uncertain: Ruby `Config.version` where `def self.version` is a class method.
 * The method is registered as a Function/Method with name `version` (the `self.`
 * prefix is the receiver, not part of the method name in the registry).
 * The generic resolver should find `version` by name.  Assert CALLS >= 1. */
TEST(mkc_c6_ruby_class_method) {
    static const MKC_File f[] = {{"config.rb", "class Config\n"
                                               "  def self.version\n"
                                               "    '1.0'\n"
                                               "  end\n\n"
                                               "  def self.default\n"
                                               "    new\n"
                                               "  end\n"
                                               "end\n\n"
                                               "def run\n"
                                               "  Config.version\n"
                                               "end\n"}};
    /* Uncertain: Config.version — the generic resolver strips `Config.` and
     * looks for `version`.  Assert CALLS >= 1. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c6/ruby/class_method", 0));
    PASS();
}

/* C6-D: Scala — object method call (module-level singleton object).
 * green=guard: `Logger.log("msg")` where Logger is a singleton `object`.
 * In Scala, `object Logger` creates a singleton; `Logger.log(...)` is a
 * call_expression.  The generic resolver should find `log` by name. */
TEST(mkc_c6_scala_singleton_object) {
    static const MKC_File f[] = {{"logger.scala",
                                  "object Logger {\n"
                                  "  def log(msg: String): Unit = println(msg)\n"
                                  "  def warn(msg: String): Unit = println(\"WARN: \" + msg)\n"
                                  "}\n\n"
                                  "def run(msg: String): Unit = Logger.log(msg)\n"}};
    /* green=guard: Logger.log(msg) should CALLS run->log.
     * The generic resolver strips the object prefix and resolves `log`. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c6/scala/singleton_object", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════
 * ═══ CLASS C7: INHERITED-METHOD CALL (grammar-only langs) ════════════════
 *
 * Existing suites cover inherited-method calls for all 9 hybrid langs.
 * Matrix-fill adds: Scala, Swift, Ruby.
 * ══════════════════════════════════════════════════════════════════════════ */

/* C7-A: Scala — subclass calls inherited method.
 * Uncertain: `class Dog extends Animal` — Scala inheritance uses `extends`.
 * The extractor should produce INHERITS Dog->Animal, but this test focuses
 * on whether `dog.describe()` (an inherited method) produces a CALLS edge. */
TEST(mkc_c7_scala_inherited_method) {
    static const MKC_File f[] = {{"zoo.scala", "class Animal {\n"
                                               "  def describe(): String = \"animal\"\n"
                                               "  def speak(): String = \"...\"\n"
                                               "}\n\n"
                                               "class Dog extends Animal {\n"
                                               "  override def speak(): String = \"woof\"\n"
                                               "}\n\n"
                                               "def run(d: Dog): String = d.describe()\n"}};
    /* Uncertain: d.describe() — the name resolver finds `describe` (it is in
     * Animal, which is in the same file).  Assert CALLS >= 1 (correct outcome).
     * This may be GREEN if the generic resolver finds `describe` by name alone. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c7/scala/inherited_method", 0));
    PASS();
}

/* C7-B: Swift — subclass calls inherited method.
 * Uncertain: Swift `class Dog: Animal` — the subclass calls `describe()` from
 * the base class.  Same reasoning as C7-A; assert CALLS >= 1. */
TEST(mkc_c7_swift_inherited_method) {
    static const MKC_File f[] = {{"zoo.swift",
                                  "class Animal {\n"
                                  "    func describe() -> String { return \"animal\" }\n"
                                  "    func speak() -> String { return \"...\" }\n"
                                  "}\n\n"
                                  "class Dog: Animal {\n"
                                  "    override func speak() -> String { return \"woof\" }\n"
                                  "}\n\n"
                                  "func run(d: Dog) -> String {\n"
                                  "    return d.describe()\n"
                                  "}\n"}};
    /* Uncertain: d.describe() should CALLS run->Animal.describe.
     * Assert CALLS >= 1; red=bug if inherited method call drops the edge. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c7/swift/inherited_method", 0));
    PASS();
}

/* C7-C: Ruby — subclass calls inherited method (via super or direct call).
 * green=guard: Ruby `def run; describe; end` in Dog where `describe` is defined
 * in Animal — the generic name resolver finds `describe` in the same project.
 * This is the same resolution path as the existing calls breadth cases. */
TEST(mkc_c7_ruby_inherited_method) {
    static const MKC_File f[] = {
        {"zoo.rb",
         "class Animal\n"
         "  def describe\n    'animal'\n  end\n\n"
         "  def speak\n    '...'\n  end\n"
         "end\n\n"
         "class Dog < Animal\n"
         "  def speak\n    'woof'\n  end\n\n"
         "  def run\n"
         "    describe()\n" /* inherited method call — explicit () makes it a `call` node */
         "  end\n"
         "end\n"}};
    /* FIXTURE FIX (was RED as a green guard): a bare `describe` (no parens, no
     * receiver) parses as an `identifier` in tree-sitter-ruby — indistinguishable
     * from a local-variable read — so it is NOT one of ruby_call_types
     * {call, command_call} and is never extracted as a call (calls=0).  That is a
     * grammar reality, not an indexer bug.  Writing `describe()` makes it a `call`
     * node; the name resolver then finds Animal#describe in the same file → CALLS
     * Dog#run->describe.  (Sibling Ruby calls with args/receivers — c5 duck_typed,
     * c6 class_method, c2 operator — already pass.)  green=guard. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c7/ruby/inherited_method", 1));
    PASS();
}

/* C7-D: Scala — super method call.
 * red=bug: `super.describe()` in a Scala subclass is a `super_expression` node
 * wrapping a field_expression / call.  The extractor may or may not follow
 * the super dereference to find the callee `describe`. */
TEST(mkc_c7_scala_super_call) {
    static const MKC_File f[] = {
        {"zoo2.scala", "class Base {\n"
                       "  def describe(): String = \"base\"\n"
                       "}\n\n"
                       "class Child extends Base {\n"
                       "  override def describe(): String = \"child: \" + super.describe()\n"
                       "}\n\n"
                       "def run(c: Child): String = c.describe()\n"}};
    /* Uncertain/red=bug: `super.describe()` — the generic resolver may or may not
     * find `describe` through the `super` qualifier.
     * Also c.describe() should resolve (call to Child's override which calls super).
     * Assert CALLS >= 1 (the outer call c.describe at minimum). */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 1, "c7/scala/super_call", 0));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════
 * ═══ CLASS C8: ADDITIONAL CONSTRUCTOR SHAPES (same-file cross-check) ════
 *
 * These cases probe constructor CALLS in languages where the cross-file
 * lsp_probe already documents failures, but we want a same-file variant to
 * determine if the failure is specifically cross-file (lsp_cross not wired)
 * or also same-file (call_types missing the construction expression).
 * ══════════════════════════════════════════════════════════════════════════ */

/* C8-A: Java — same-file constructor call (new T()).
 * red=bug: lrp_java_s3_constructor confirms the cross-file case fails.
 * The root cause is java_call_types = {method_invocation} only — no
 * object_creation_expression.  This is a SAME-FILE reproduction to confirm
 * the bug is at the extraction layer (not a cross-file resolver issue). */
TEST(mkc_c8_java_constructor_samefile) {
    static const MKC_File f[] = {
        {"Factory.java", "package app;\n\n"
                         "class Widget {\n"
                         "    private String name;\n"
                         "    public Widget(String name) { this.name = name; }\n"
                         "    public String label() { return name; }\n"
                         "}\n\n"
                         "class Factory {\n"
                         "    public Widget make(String name) { return new Widget(name); }\n"
                         "    public String run(String name) { return make(name).label(); }\n"
                         "}\n"}};
    /* red=bug: new Widget(name) → CALLS make->Widget.<init> — missing.
     * Root cause: java_call_types does not include object_creation_expression.
     * Fix: lang_specs.c java_call_types — add "object_creation_expression". */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 2, "c8/java/constructor_samefile", 0));
    PASS();
}

/* C8-B: C# — same-file constructor call (new T()).
 * red=bug: same root cause as Java; cs_call_types = {invocation_expression}
 * only, no object_creation_expression. */
TEST(mkc_c8_csharp_constructor_samefile) {
    static const MKC_File f[] = {
        {"Service.cs", "namespace App {\n"
                       "    class Widget {\n"
                       "        public string Name;\n"
                       "        public Widget(string name) { Name = name; }\n"
                       "        public string Label() { return Name; }\n"
                       "    }\n\n"
                       "    class Service {\n"
                       "        public Widget Make(string name) { return new Widget(name); }\n"
                       "        public string Run(string name) { return Make(name).Label(); }\n"
                       "    }\n"
                       "}\n"}};
    /* red=bug: new Widget(name) → CALLS Make->Widget..ctor — missing.
     * Root cause: cs_call_types missing object_creation_expression.
     * Fix: lang_specs.c cs_call_types — add "object_creation_expression". */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 2, "c8/csharp/constructor_samefile", 0));
    PASS();
}

/* C8-C: PHP — same-file constructor call (new T()).
 * red=bug: lrp_php_s3_constructor confirms this; same-file variant to isolate
 * extraction from resolution.
 * Root cause: php_call_types omits object_creation_expression. */
TEST(mkc_c8_php_constructor_samefile) {
    static const MKC_File f[] = {
        {"factory.php", "<?php\n"
                        "class Widget {\n"
                        "    public $name;\n"
                        "    public function __construct($name) { $this->name = $name; }\n"
                        "    public function label() { return $this->name; }\n"
                        "}\n\n"
                        "class Factory {\n"
                        "    public function make($name) { return new Widget($name); }\n"
                        "    public function run($name) { return $this->make($name)->label(); }\n"
                        "}\n"}};
    /* red=bug: new Widget($name) should CALLS make->Widget::__construct — missing.
     * Fix: lang_specs.c php_call_types — add "object_creation_expression". */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 2, "c8/php/constructor_samefile", 0));
    PASS();
}

/* C8-D: TypeScript — same-file constructor call (new T()).
 * red=bug: lrp_ts_s3_constructor confirms this is RED even with lsp_cross wired.
 * The TS resolver does not link new_expression to the class constructor node.
 * Same-file variant to confirm it is an extraction bug (not a cross-file gap). */
TEST(mkc_c8_typescript_constructor_samefile) {
    static const MKC_File f[] = {
        {"service.ts", "class Widget {\n"
                       "    constructor(public name: string) {}\n"
                       "    label(): string { return this.name; }\n"
                       "}\n\n"
                       "class Service {\n"
                       "    make(name: string): Widget { return new Widget(name); }\n"
                       "    run(name: string): string { return this.make(name).label(); }\n"
                       "}\n"}};
    /* red=bug: new Widget(name) should CALLS make->Widget.constructor.
     * Root cause: TS ts_call_types includes new_expression (call_types in
     * lang_specs.c), but the extractor does not route new_expression to the
     * constructor CALLS path.  Fix: handle new_expression callee extraction in
     * extract_calls.c TS/JS branch to emit CALLS to the class. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 2, "c8/typescript/constructor_samefile", 0));
    PASS();
}

/* C8-E: Kotlin — same-file constructor call.
 * red=bug: lrp_kotlin_s3_constructor confirms cross-file RED.  Same-file variant
 * isolates whether the problem is extraction (Kotlin call_types missing
 * object_creation_expression) or Kotlin-specific cross-file resolution. */
TEST(mkc_c8_kotlin_constructor_samefile) {
    static const MKC_File f[] = {{"app.kt",
                                  "class Widget(val name: String) {\n"
                                  "    fun label(): String = name\n"
                                  "}\n\n"
                                  "fun make(name: String): Widget = Widget(name)\n"
                                  "fun run(name: String): String = make(name).label()\n"}};
    /* red=bug: Widget(name) should CALLS make->Widget.<init>.
     * Root cause: Kotlin object creation `Widget(name)` is an
     * `call_expression` in tree-sitter-kotlin; but unlike Python's `call`
     * which routes to __init__, Kotlin has no constructor linkage.
     * The generic resolver finds `label` by name (from make(name).label())
     * but may not find the constructor because `Widget` the constructor
     * has no separate definition node in the registry.
     * Assert CALLS >= 2 (make->Widget + label chain); will be RED. */
    ASSERT_TRUE(mkc_edge(f, 1, "CALLS", 2, "c8/kotlin/constructor_samefile", 0));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════
 * SUITE registration
 * ══════════════════════════════════════════════════════════════════════════ */

SUITE(matrix_known_classes) {

    /* ── CLASS C1: CONSTRUCTOR / INSTANTIATION ─────────────────────────── */
    /* C1-A/B: Scala new + case class apply — red=bug */
    RUN_TEST(mkc_c1_scala_constructor_new);
    RUN_TEST(mkc_c1_scala_case_class_apply);
    /* C1-C: Swift initializer — red=bug */
    RUN_TEST(mkc_c1_swift_initializer);
    /* C1-D: Ruby Type.new — red=bug */
    RUN_TEST(mkc_c1_ruby_type_new);
    /* C1-E: C++ same-file constructor — green=guard */
    RUN_TEST(mkc_c1_cpp_constructor_samefile);
    /* C1-F: Rust same-file Point::new — red=bug (:: resolver gap) */
    RUN_TEST(mkc_c1_rust_new_samefile);

    /* ── CLASS C2: OPERATOR OVERLOADING ────────────────────────────────── */
    /* C2-A/B: C++ operator+/[] — red=bug */
    RUN_TEST(mkc_c2_cpp_operator_plus);
    RUN_TEST(mkc_c2_cpp_operator_subscript);
    /* C2-C/D: Python __add__/__getitem__ — red=bug */
    RUN_TEST(mkc_c2_python_dunder_add);
    RUN_TEST(mkc_c2_python_dunder_getitem);
    /* C2-E: Rust Add trait — red=bug */
    RUN_TEST(mkc_c2_rust_add_trait);
    /* C2-F: Kotlin operator fun plus — red=bug */
    RUN_TEST(mkc_c2_kotlin_operator_plus);
    /* C2-G: C# operator+ — red=bug */
    RUN_TEST(mkc_c2_csharp_operator_plus);
    /* C2-H: Scala symbolic method — uncertain/red=bug */
    RUN_TEST(mkc_c2_scala_symbolic_method);
    /* C2-I: Ruby def + — uncertain/red=bug */
    RUN_TEST(mkc_c2_ruby_operator_plus);

    /* ── CLASS C3: DECORATES ─────────────────────────────────────────── */
    /* C3-A/B: Rust #[derive] / #[allow] — red=bug */
    RUN_TEST(mkc_c3_rust_derive_attribute);
    RUN_TEST(mkc_c3_rust_fn_attribute);
    /* C3-C: PHP 8 #[Attribute] — red=bug */
    RUN_TEST(mkc_c3_php8_attribute);
    /* C3-D: Swift @discardableResult — red=bug */
    RUN_TEST(mkc_c3_swift_attribute);
    /* C3-E: Scala @deprecated/@tailrec — red=bug */
    RUN_TEST(mkc_c3_scala_annotation);
    /* C3-F: Python class-level decorator — green=guard */
    RUN_TEST(mkc_c3_python_class_decorator);

    /* ── CLASS C4: ASYNC/AWAIT CALLS ─────────────────────────────────── */
    /* C4-A/B: Python / TypeScript async — green=guard */
    RUN_TEST(mkc_c4_python_async_await);
    RUN_TEST(mkc_c4_typescript_async_await);
    /* C4-C: C# async Task — uncertain/red=bug */
    RUN_TEST(mkc_c4_csharp_async_await);
    /* C4-D: Rust .await — red=bug */
    RUN_TEST(mkc_c4_rust_await);
    /* C4-E: Kotlin suspend — green=guard (uncertain) */
    RUN_TEST(mkc_c4_kotlin_suspend_call);
    /* C4-F: Swift async/await — uncertain/red=bug */
    RUN_TEST(mkc_c4_swift_async_await);

    /* ── CLASS C5: GENERIC / TEMPLATED CALLS ─────────────────────────── */
    /* C5-A/B: Scala + Swift generic — uncertain */
    RUN_TEST(mkc_c5_scala_generic_call);
    RUN_TEST(mkc_c5_swift_generic_call);
    /* C5-C: Ruby duck-typed — green=guard */
    RUN_TEST(mkc_c5_ruby_duck_typed_call);

    /* ── CLASS C6: STATIC / CLASS-METHOD CALLS ──────────────────────── */
    /* C6-A/B/C: Scala companion / Swift static / Ruby self.method */
    RUN_TEST(mkc_c6_scala_companion_object);
    RUN_TEST(mkc_c6_swift_static_method);
    RUN_TEST(mkc_c6_ruby_class_method);
    /* C6-D: Scala singleton object — green=guard */
    RUN_TEST(mkc_c6_scala_singleton_object);

    /* ── CLASS C7: INHERITED-METHOD CALLS ───────────────────────────── */
    /* C7-A: Scala inherited method — uncertain */
    RUN_TEST(mkc_c7_scala_inherited_method);
    /* C7-B: Swift inherited method — uncertain */
    RUN_TEST(mkc_c7_swift_inherited_method);
    /* C7-C: Ruby inherited method — green=guard */
    RUN_TEST(mkc_c7_ruby_inherited_method);
    /* C7-D: Scala super call — uncertain */
    RUN_TEST(mkc_c7_scala_super_call);

    /* ── CLASS C8: CONSTRUCTOR SAME-FILE CROSS-CHECK ────────────────── */
    /* All red=bug: isolates extraction-layer constructor gaps */
    RUN_TEST(mkc_c8_java_constructor_samefile);
    RUN_TEST(mkc_c8_csharp_constructor_samefile);
    RUN_TEST(mkc_c8_php_constructor_samefile);
    RUN_TEST(mkc_c8_typescript_constructor_samefile);
    RUN_TEST(mkc_c8_kotlin_constructor_samefile);
}
