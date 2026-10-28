/*
 * test_matrix_known_classes.c — Matrix-fill probe: apply every identified
 * bug-class across ALL applicable languages and fill the coverage gap left
 * by the existing suites (test_lang_contract.c, test_edge_structural.c,
 * test_lsp_resolution_probe.c).
 *
 * LEGEND
 *   green=guard  — asserts the CORRECT outcome; PASSES when the feature works.
 *   red=bug      — legacy aggregate assertion in unmodified cases.
 *   RQ cases     — exact source obligations: required calls, named missing-edge
 *                  quality gaps, and forbidden unsupported target bindings.
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
 * Registered in the canonical test runner; named quality gaps are gating data.
 */

#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "resolver_quality.h"
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
        FILE *f = cbm_fopen(path, "wb");
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

/* Each obligation below is authored from its fixture, independently of emitted
 * rows. Missing edges remain quality debt; invented targets fail the oracle.
 * Unsupported implicit constructors retain a real source declaration anchor
 * with no fabricated callable identity. The explicit C# operator also retains
 * its unmapped callable identity as debt. Unknown receiver sites are forbidden. */
typedef struct {
    const char *test, *caller, *needle, *target, *declaration, *label;
    int line;
    unsigned occurrence;
    bool required;
} MKC_Golden;
static const MKC_Golden mkc_goldens[] = {
    {"mkc_c1_scala_constructor_new", "make", "new Widget(n)", NULL,
     "class Widget(val name: String) {", "Method", 1, 0, false},
    {"mkc_c1_scala_case_class_apply", "origin", "Point(0.0, 0.0)", NULL,
     "case class Point(x: Double, y: Double) {", "Method", 1, 0, false},
    {"mkc_c1_scala_case_class_apply", "origin", "Point(0.0, 0.0).dist()", "dist",
     "  def dist(): Double = math.sqrt(x*x + y*y)", "Method", 2, 0, false},
    {"mkc_c1_swift_initializer", "make", "Widget(name: n)", "init",
     "    init(name: String) { self.name = name }", "Method", 3, 0, false},
    {"mkc_c1_ruby_type_new", "make", "Widget.new(name)", "initialize", "  def initialize(name)",
     "Method", 2, 0, false},
    {"mkc_c1_rust_new_samefile", "run", "Point::new(3.0, 4.0)", "new",
     "    pub fn new(x: f64, y: f64) -> Self { Point { x, y } }", "Method", 4, 0, false},
    {"mkc_c1_rust_new_samefile", "run", "p.dist()", "dist",
     "    pub fn dist(&self) -> f64 { (self.x * self.x + self.y * self.y).sqrt() }", "Method", 5, 0,
     false},
    {"mkc_c2_cpp_operator_plus", "run", "a + b", "operator+",
     "    Vec2 operator+(const Vec2 &o) const { return Vec2(x+o.x, y+o.y); }", "Method", 4, 0,
     false},
    {"mkc_c2_cpp_operator_plus", "run", "c.dot(b)", "dot",
     "    float dot(const Vec2 &o) const { return x*o.x + y*o.y; }", "Method", 5, 0, false},
    {"mkc_c2_cpp_operator_plus", "operator+", "Vec2(x+o.x, y+o.y)", "Vec2",
     "    Vec2(float x, float y): x(x), y(y){}", "Method", 3, 0, false},
    {"mkc_c2_cpp_operator_subscript", "run", "a[2]", "operator[]",
     "    int& operator[](int i) { return data[i]; }", "Method", 3, 0, false},
    {"mkc_c2_python_dunder_add", "__add__", "Vec(self.x + other.x, self.y + other.y)", "__init__",
     "    def __init__(self, x, y):", "Method", 2, 0, false},
    {"mkc_c2_python_dunder_getitem", "run", "s[0]", "__getitem__",
     "    def __getitem__(self, i):", "Method", 5, 0, false},
    {"mkc_c2_rust_add_trait", "run", "a + b", "add", "    fn add(self, rhs: Self) -> Self {",
     "Method", 8, 0, false},
    {"mkc_c2_kotlin_operator_plus", "run", "a + b", "plus",
     "    operator fun plus(other: Vec2): Vec2 =", "Method", 2, 0, false},
    {"mkc_c2_csharp_operator_plus", "Run", "a + b", NULL,
     "        public static Vec2 operator+(Vec2 a, Vec2 b) {", "Method", 5, 0, false},
    {"mkc_c2_csharp_operator_plus", "Run", "(a + b).Dot(b)", "Dot",
     "        public float Dot(Vec2 o) { return X*o.X + Y*o.Y; }", "Method", 8, 0, false},
    {"mkc_c2_csharp_operator_plus", "operator+", "new Vec2(a.X + b.X, a.Y + b.Y)", "Vec2",
     "        public Vec2(float x, float y) { X = x; Y = y; }", "Method", 4, 0, false},
    {"mkc_c2_scala_symbolic_method", "run", "a + b", "+",
     "  def +(other: Vec2): Vec2 = Vec2(x + other.x, y + other.y)", "Method", 2, 0, false},
    {"mkc_c2_scala_symbolic_method", "run", "(a + b).dot(b)", "dot",
     "  def dot(other: Vec2): Double = x * other.x + y * other.y", "Method", 3, 0, false},
    {"mkc_c2_ruby_operator_plus", "+", "Vec2.new(@x + other.x, @y + other.y)", "initialize",
     "  def initialize(x, y)", "Method", 3, 0, false},
    {"mkc_c4_typescript_async_await", "getData", "fetch(url)", "fetch",
     "async function fetch(url: string): Promise<string> {", "Function", 1, 0, false},
    {"mkc_c4_typescript_async_await", "run", "getData(url)", "getData",
     "async function getData(url: string): Promise<string> {", "Function", 5, 0, false},
    {"mkc_c4_csharp_async_await", "GetDataAsync", "FetchAsync(url)", "FetchAsync",
     "        public async Task<string> FetchAsync(string url) {", "Method", 5, 0, false},
    {"mkc_c4_csharp_async_await", "RunAsync", "GetDataAsync(\"http://example.com\")",
     "GetDataAsync", "        public async Task<string> GetDataAsync(string url) {", "Method", 9, 0,
     false},
    {"mkc_c4_rust_await", "get_data", "fetch(url)", "fetch",
     "async fn fetch(url: &str) -> String {", "Function", 1, 0, false},
    {"mkc_c4_rust_await", "run", "get_data(url)", "get_data",
     "async fn get_data(url: &str) -> String {", "Function", 5, 0, false},
    {"mkc_c4_kotlin_suspend_call", "getData", "fetchData(url)", "fetchData",
     "suspend fun fetchData(url: String): String {", "Function", 1, 0, false},
    {"mkc_c4_kotlin_suspend_call", "run", "getData(url)", "getData",
     "suspend fun getData(url: String): String {", "Function", 5, 0, false},
    {"mkc_c4_swift_async_await", "getData", "fetch(url: url)", "fetch",
     "func fetch(url: String) async -> String {", "Function", 1, 0, false},
    {"mkc_c4_swift_async_await", "run", "getData(url: url)", "getData",
     "func getData(url: String) async -> String {", "Function", 5, 0, false},
    {"mkc_c5_scala_generic_call", "run", "first(ns)", "first",
     "def first[T](xs: List[T]): Option[T] =", "Function", 1, 0, false},
    {"mkc_c5_swift_generic_call", "run", "maxOf(a, b)", "maxOf",
     "func maxOf<T: Comparable>(_ a: T, _ b: T) -> T {", "Function", 1, 0, false},
    {"mkc_c5_ruby_duck_typed_call", "run", "first(items)", "first", "def first(collection)",
     "Function", 1, 0, false},
    {"mkc_c6_scala_companion_object", "run", "Config.default()", "default",
     "  def default(): Config = new Config(false)", "Method", 4, 0, false},
    {"mkc_c6_swift_static_method", "run", "MathHelper.clamp(n, 0, 100)", "clamp",
     "    static func clamp(_ v: Int, _ lo: Int, _ hi: Int) -> Int {", "Method", 2, 0, false},
    {"mkc_c6_ruby_class_method", "run", "Config.version", "version", "  def self.version", "Method",
     2, 0, false},
    {"mkc_c6_scala_singleton_object", "run", "Logger.log(msg)", "log",
     "  def log(msg: String): Unit = println(msg)", "Method", 2, 0, false},
    {"mkc_c7_scala_inherited_method", "run", "d.describe()", "describe",
     "  def describe(): String = \"animal\"", "Method", 2, 0, false},
    {"mkc_c7_swift_inherited_method", "run", "d.describe()", "describe",
     "    func describe() -> String { return \"animal\" }", "Method", 2, 0, false},
    {"mkc_c7_ruby_inherited_method", "run", "describe()", "describe", "  def describe", "Method", 2,
     0, false},
    {"mkc_c7_scala_super_call", "describe", "super.describe()", "Base.describe",
     "  def describe(): String = \"base\"", "Method", 2, 0, false},
    {"mkc_c7_scala_super_call", "run", "c.describe()", "Child.describe",
     "  override def describe(): String = \"child: \" + super.describe()", "Method", 6, 0, false},
    {"mkc_c8_java_constructor_samefile", "make", "new Widget(name)", "Widget",
     "    public Widget(String name) { this.name = name; }", "Method", 5, 0, false},
    {"mkc_c8_java_constructor_samefile", "run", "make(name)", "make",
     "    public Widget make(String name) { return new Widget(name); }", "Method", 10, 0, false},
    {"mkc_c8_java_constructor_samefile", "run", "make(name).label()", "label",
     "    public String label() { return name; }", "Method", 6, 0, false},
    {"mkc_c8_csharp_constructor_samefile", "Make", "new Widget(name)", "Widget",
     "        public Widget(string name) { Name = name; }", "Method", 4, 0, false},
    {"mkc_c8_csharp_constructor_samefile", "Run", "Make(name)", "Make",
     "        public Widget Make(string name) { return new Widget(name); }", "Method", 9, 0, false},
    {"mkc_c8_csharp_constructor_samefile", "Run", "Make(name).Label()", "Label",
     "        public string Label() { return Name; }", "Method", 5, 0, false},
    {"mkc_c8_php_constructor_samefile", "make", "new Widget($name)", "__construct",
     "    public function __construct($name) { $this->name = $name; }", "Method", 4, 0, false},
    {"mkc_c8_php_constructor_samefile", "run", "$this->make($name)", "make",
     "    public function make($name) { return new Widget($name); }", "Method", 9, 0, true},
    {"mkc_c8_php_constructor_samefile", "run", "$this->make($name)->label()", "label",
     "    public function label() { return $this->name; }", "Method", 5, 0, false},
    {"mkc_c8_typescript_constructor_samefile", "make", "new Widget(name)", "constructor",
     "    constructor(public name: string) {}", "Method", 2, 0, false},
    {"mkc_c8_typescript_constructor_samefile", "run", "this.make(name)", "make",
     "    make(name: string): Widget { return new Widget(name); }", "Method", 7, 0, true},
    {"mkc_c8_typescript_constructor_samefile", "run", "this.make(name).label()", "label",
     "    label(): string { return this.name; }", "Method", 3, 0, false},
    {"mkc_c8_kotlin_constructor_samefile", "make", "Widget(name)", NULL,
     "class Widget(val name: String) {", "Method", 1, 0, false},
    {"mkc_c8_kotlin_constructor_samefile", "run", "make(name)", "make",
     "fun make(name: String): Widget = Widget(name)", "Function", 5, 0, true},
    {"mkc_c8_kotlin_constructor_samefile", "run", "make(name).label()", "label",
     "    fun label(): String = name", "Method", 2, 0, false},
};
typedef struct {
    const char *test, *caller, *needle;
    unsigned occurrence;
} MKC_Forbidden;
static const MKC_Forbidden mkc_forbidden[] = {
    {"mkc_c2_python_dunder_add", "run", "a + b", 0},
    {"mkc_c2_ruby_operator_plus", "run", "a + b", 0},
    {"mkc_c2_ruby_operator_plus", "run", "(a + b).dot(b)", 0},
};

static bool mkc_quality(const MKC_File *original, int count, const char *test) {
    if (!strncmp(test, "test_", 5))
        test += 5;
    if (count != 1)
        return false;
    /* The same production harness must retain a nonzero exact-call control. */
    const MKC_File files[] = {original[0],
                              {"quality_control.py",
                               "def exact_target():\n    return 1\n"
                               "def exact_caller():\n    return exact_target()\n"}};
    const RQ_Source sources[] = {{.path = files[0].name, .text = files[0].content},
                                 {.path = files[1].name, .text = files[1].content}};
    RQ_Expected expected[8] = {{.case_id = "required-local-control",
                                .site = {.file = "quality_control.py",
                                         .caller = "exact_caller",
                                         .needle = "exact_target()",
                                         .occurrence_index = 1},
                                .relation = "CALLS",
                                .target = {.file = "quality_control.py",
                                           .suffix = "quality_control.exact_target",
                                           .label = "Function",
                                           .declaration_line = 1,
                                           .declaration_text = "def exact_target():"},
                                .mode = RQ_REQUIRED}};
    RQ_Forbidden invalid[3] = {0};
    char ids[8][100];
    size_t n = 1, nf = 0, required = 1;
    for (size_t i = 0; i < sizeof(mkc_goldens) / sizeof(mkc_goldens[0]); i++) {
        const MKC_Golden *g = mkc_goldens + i;
        if (strcmp(test, g->test))
            continue;
        if (n == sizeof(expected) / sizeof(expected[0]))
            return false;
        snprintf(ids[n], sizeof(ids[n]), "%s-%zu", test, n);
        expected[n] = (RQ_Expected){
            .case_id = ids[n],
            .site = {.file = original[0].name,
                     .caller = g->caller,
                     .needle = g->needle,
                     .occurrence_index = g->occurrence},
            .relation = "CALLS",
            .target = {.file = original[0].name,
                       .suffix = g->target,
                       .label = g->label,
                       .declaration_line = g->line,
                       .declaration_text = g->declaration},
            .mode = g->required ? RQ_REQUIRED : RQ_KNOWN_GAP,
            .gap_id = g->target ? "source-call-binding-proof-missing"
                                : "callable-identity-not-materialized",
            .why = g->target
                       ? "This source-authored call requires a complete language binding and "
                         "receiver/overload proof to its exact callable declaration."
                       : "The genuine call has no faithful callable graph identity. A class "
                         "anchor for an implicit constructor, or an explicit operator anchor, "
                         "records debt without accepting a Class as a callable target.",
            .tried = "The original v69 production harness failed its aggregate call floor. "
                     "Preserved fixture bytes and replaced that floor with exact source "
                     "occurrences/declarations. Direct make calls remain required source "
                     "obligations; an old aggregate count does not establish exact admission."};
        required += g->required;
        n++;
    }
    for (size_t i = 0; i < sizeof(mkc_forbidden) / sizeof(mkc_forbidden[0]); i++) {
        const MKC_Forbidden *g = mkc_forbidden + i;
        if (strcmp(test, g->test))
            continue;
        if (nf == sizeof(invalid) / sizeof(invalid[0]))
            return false;
        invalid[nf++] = (RQ_Forbidden){.case_id = g->needle,
                                       .site = {.file = original[0].name,
                                                .caller = g->caller,
                                                .needle = g->needle,
                                                .occurrence_index = g->occurrence},
                                       .relation = "CALLS",
                                       .any_target = true};
    }
    if (n == 1 && nf == 0)
        return false;
    MKC_Proj project;
    cbm_store_t *store = mkc_index(&project, files, 2);
    RQ_Report report = {0};
    bool ok = store && rq_check_project_declarations(store, project.project, test, sources, 2,
                                                     expected, n, invalid, nf, &report, stderr);
    mkc_cleanup(&project, store);
    return ok && report.genuine == n && report.found == required && report.missing == n - required;
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

/* c1 scala constructor new: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c1_scala_constructor_new) {
    static const MKC_File f[] = {{"Widget.scala", "class Widget(val name: String) {\n"
                                                  "  def label(): String = name\n"
                                                  "}\n\n"
                                                  "def make(n: String): Widget = new Widget(n)\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c1 scala case class apply: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c1_scala_case_class_apply) {
    static const MKC_File f[] = {{"Point.scala",
                                  "case class Point(x: Double, y: Double) {\n"
                                  "  def dist(): Double = math.sqrt(x*x + y*y)\n"
                                  "}\n\n"
                                  "def origin(): Double = Point(0.0, 0.0).dist()\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c1 swift initializer: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c1_swift_initializer) {
    static const MKC_File f[] = {{"Widget.swift", "class Widget {\n"
                                                  "    let name: String\n"
                                                  "    init(name: String) { self.name = name }\n"
                                                  "    func label() -> String { return name }\n"
                                                  "}\n\n"
                                                  "func make(n: String) -> Widget {\n"
                                                  "    return Widget(name: n)\n"
                                                  "}\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c1 ruby type new: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
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

/* c1 rust new samefile: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
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

/* c2 cpp operator plus: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c2_cpp_operator_plus) {
    static const MKC_File f[] = {
        {"vec.cpp", "struct Vec2 {\n    float x, y;\n    Vec2(float x, float y): x(x), y(y){}\n"
                    "    Vec2 operator+(const Vec2 &o) const { return Vec2(x+o.x, y+o.y); }\n"
                    "    float dot(const Vec2 &o) const { return x*o.x + y*o.y; }\n};\n\n"
                    "float run(Vec2 a, Vec2 b) {\n"
                    "    Vec2 c = a + b;\n" /* operator+ call */
                    "    return c.dot(b);\n"
                    "}\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c2 cpp operator subscript: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c2_cpp_operator_subscript) {
    static const MKC_File f[] = {{"arr.cpp", "struct IntArr {\n    int data[8];\n"
                                             "    int& operator[](int i) { return data[i]; }\n"
                                             "};\n\n"
                                             "int run(IntArr &a) {\n"
                                             "    return a[2];\n"
                                             "}\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c2 python dunder add: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c2_python_dunder_add) {
    static const MKC_File f[] = {
        {"vec.py",
         "class Vec:\n"
         "    def __init__(self, x, y):\n        self.x, self.y = x, y\n\n"
         "    def __add__(self, other):\n        return Vec(self.x + other.x, self.y + other.y)\n\n"
         "    def __eq__(self, other):\n        return self.x == other.x and self.y == other.y\n\n"
         "def run(a, b):\n"
         "    return a + b\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c2 python dunder getitem: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c2_python_dunder_getitem) {
    static const MKC_File f[] = {{"seq.py",
                                  "class Seq:\n"
                                  "    def __init__(self, data):\n        self._d = data\n\n"
                                  "    def __getitem__(self, i):\n        return self._d[i]\n\n"
                                  "def run(s: Seq):\n"
                                  "    return s[0]\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c2 rust add trait: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c2 kotlin operator plus: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c2_kotlin_operator_plus) {
    static const MKC_File f[] = {{"vec.kt",
                                  "data class Vec2(val x: Float, val y: Float) {\n"
                                  "    operator fun plus(other: Vec2): Vec2 =\n"
                                  "        Vec2(x + other.x, y + other.y)\n"
                                  "    operator fun get(i: Int): Float = if (i == 0) x else y\n"
                                  "}\n\n"
                                  "fun run(a: Vec2, b: Vec2): Vec2 = a + b\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c2 csharp operator plus: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c2 scala symbolic method: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c2_scala_symbolic_method) {
    static const MKC_File f[] = {{"vec.scala",
                                  "case class Vec2(x: Double, y: Double) {\n"
                                  "  def +(other: Vec2): Vec2 = Vec2(x + other.x, y + other.y)\n"
                                  "  def dot(other: Vec2): Double = x * other.x + y * other.y\n"
                                  "}\n\n"
                                  "def run(a: Vec2, b: Vec2): Double = (a + b).dot(b)\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c2 ruby operator plus: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
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

/* c4 typescript async await: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c4 csharp async await: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c4 rust await: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c4 kotlin suspend call: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c4 swift async await: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
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

/* c5 scala generic call: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c5_scala_generic_call) {
    static const MKC_File f[] = {{"algo.scala",
                                  "def first[T](xs: List[T]): Option[T] =\n"
                                  "  if (xs.isEmpty) None else Some(xs.head)\n\n"
                                  "def run(ns: List[Int]): Option[Int] = first(ns)\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c5 swift generic call: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c5_swift_generic_call) {
    static const MKC_File f[] = {{"algo.swift", "func maxOf<T: Comparable>(_ a: T, _ b: T) -> T {\n"
                                                "    return a > b ? a : b\n"
                                                "}\n\n"
                                                "func run(a: Int, b: Int) -> Int {\n"
                                                "    return maxOf(a, b)\n"
                                                "}\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c5 ruby duck typed call: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c5_ruby_duck_typed_call) {
    static const MKC_File f[] = {{"algo.rb", "def first(collection)\n"
                                             "  collection.empty? ? nil : collection.first\n"
                                             "end\n\n"
                                             "def run(items)\n"
                                             "  first(items)\n"
                                             "end\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════
 * ═══ CLASS C6: STATIC / CLASS-METHOD CALLS (grammar-only langs) ══════════
 *
 * The existing suites cover static calls for Go/C/C++/Python/TS/Java/Kotlin/
 * C#/PHP/Rust.  Matrix-fill adds: Scala companion-object, Swift static/class
 * methods, Ruby self.method (class methods).
 * ══════════════════════════════════════════════════════════════════════════ */

/* c6 scala companion object: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c6_scala_companion_object) {
    static const MKC_File f[] = {{"config.scala", "class Config(val debug: Boolean)\n\n"
                                                  "object Config {\n"
                                                  "  def default(): Config = new Config(false)\n"
                                                  "  def debug(): Config = new Config(true)\n"
                                                  "}\n\n"
                                                  "def run(): Config = Config.default()\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c6 swift static method: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c6 ruby class method: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c6 scala singleton object: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c6_scala_singleton_object) {
    static const MKC_File f[] = {{"logger.scala",
                                  "object Logger {\n"
                                  "  def log(msg: String): Unit = println(msg)\n"
                                  "  def warn(msg: String): Unit = println(\"WARN: \" + msg)\n"
                                  "}\n\n"
                                  "def run(msg: String): Unit = Logger.log(msg)\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════
 * ═══ CLASS C7: INHERITED-METHOD CALL (grammar-only langs) ════════════════
 *
 * Existing suites cover inherited-method calls for all 9 hybrid langs.
 * Matrix-fill adds: Scala, Swift, Ruby.
 * ══════════════════════════════════════════════════════════════════════════ */

/* c7 scala inherited method: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c7_scala_inherited_method) {
    static const MKC_File f[] = {{"zoo.scala", "class Animal {\n"
                                               "  def describe(): String = \"animal\"\n"
                                               "  def speak(): String = \"...\"\n"
                                               "}\n\n"
                                               "class Dog extends Animal {\n"
                                               "  override def speak(): String = \"woof\"\n"
                                               "}\n\n"
                                               "def run(d: Dog): String = d.describe()\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c7 swift inherited method: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c7 ruby inherited method: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c7 scala super call: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c7_scala_super_call) {
    static const MKC_File f[] = {
        {"zoo2.scala", "class Base {\n"
                       "  def describe(): String = \"base\"\n"
                       "}\n\n"
                       "class Child extends Base {\n"
                       "  override def describe(): String = \"child: \" + super.describe()\n"
                       "}\n\n"
                       "def run(c: Child): String = c.describe()\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
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

/* c8 java constructor samefile: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c8 csharp constructor samefile: exact source calls are required obligations or named quality
 * gaps; unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c8 php constructor samefile: exact source calls are required obligations or named quality gaps;
 * unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c8 typescript constructor samefile: exact source calls are required obligations or named quality
 * gaps; unknown receiver sites cannot bind to a same-named project method. */
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
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
    PASS();
}

/* c8 kotlin constructor samefile: exact source calls are required obligations or named quality
 * gaps; unknown receiver sites cannot bind to a same-named project method. */
TEST(mkc_c8_kotlin_constructor_samefile) {
    static const MKC_File f[] = {{"app.kt",
                                  "class Widget(val name: String) {\n"
                                  "    fun label(): String = name\n"
                                  "}\n\n"
                                  "fun make(name: String): Widget = Widget(name)\n"
                                  "fun run(name: String): String = make(name).label()\n"}};
    /* Exact source obligations and unsupported-binding gaps are tracked above. */
    ASSERT_TRUE(mkc_quality(f, 1, __func__));
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
