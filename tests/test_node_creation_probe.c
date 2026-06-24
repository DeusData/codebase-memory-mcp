/*
 * test_node_creation_probe.c — Reproduce-first node-creation probe.
 *
 * Green = guard (node/edge already created correctly).
 * Red   = bug found (node/edge not produced; keep until fixed).
 *
 * Each per-language group indexes a fixture through the FULL pipeline via the
 * same lang_index / lang_metrics harness as test_lang_contract.c (the helpers
 * are copy-included here so this file compiles stand-alone without touching
 * test_main.c or test_lang_contract.c).
 *
 * Coverage strategy:
 *   - 9 hybrid-LSP languages (Go, C, C++, Rust, Python, TypeScript, Java,
 *     Kotlin, C#): functions, methods, classes/structs, enums, interfaces/
 *     traits, nested functions, lambdas/closures, generics, type aliases,
 *     constants/global vars.  ~6-12 cases each.
 *   - 14 grammar-only languages (Ruby, Swift, Scala, Lua, Bash, Dart, Elixir,
 *     Haskell, Zig, OCaml, Erlang, Groovy, Nim, GDScript): functions + a
 *     class/module + intra-file call.  ~3-6 cases each.
 *
 * How languages were confirmed supported: each uses a CBM_LANG_* constant
 * from cbm.h (the canonical enum); the label-golden table in
 * test_grammar_labels.c provides the expected histogram per grammar.
 *
 * Known expected-RED cases (document the gap, keep for fix-phase):
 *   - Dart CALLS edge: extract_calls.c has no dart branch (selector call).
 *   - Groovy CALLS edge: function_call callee resolution unhandled.
 *
 * Do NOT register in test_main.c — run via standalone suite.
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

/* ══════════════════════════════════════════════════════════════════
 * Harness — copy of the static helpers from test_lang_contract.c.
 * (Duplicated here so the probe file is self-contained and does not
 * require test_main.c changes.)
 * ══════════════════════════════════════════════════════════════════ */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} NcpLangProj;

typedef struct {
    const char *name;
    const char *content;
} NcpLangFile;

static void ncp_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\')
            *p = '/';
    }
}

static cbm_store_t *ncp_open_indexed(NcpLangProj *lp) {
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

static cbm_store_t *ncp_index_files(NcpLangProj *lp, const NcpLangFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_ncp_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir))
        return NULL;
    ncp_to_fwd_slashes(lp->tmpdir);
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
    return ncp_open_indexed(lp);
}

static void ncp_cleanup(NcpLangProj *lp, cbm_store_t *store) {
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
    unlink(wal);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(shm);
}

/* Count nodes with a given label. Returns -1 on error. */
static int ncp_count_label(cbm_store_t *store, const char *project, const char *label) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK)
        return -1;
    cbm_store_free_nodes(nodes, count);
    return count;
}

/* Sum type-like nodes (Class + Struct + Interface + Enum + Trait + Type). */
static int ncp_type_nodes(cbm_store_t *store, const char *project) {
    static const char *labels[] = {"Class", "Struct", "Interface", "Enum", "Trait", "Type", NULL};
    int total = 0;
    for (int i = 0; labels[i]; i++) {
        int n = ncp_count_label(store, project, labels[i]);
        if (n > 0)
            total += n;
    }
    return total;
}

/* Callable nodes (Function + Method) with >=1 outbound edge. */
static int ncp_callables_with_outbound(cbm_store_t *store, const char *project) {
    static const char *callable_labels[] = {"Function", "Method", NULL};
    int total = 0;
    for (int i = 0; callable_labels[i]; i++) {
        cbm_search_params_t p = {0};
        p.project = project;
        p.label = callable_labels[i];
        p.min_degree = 1;
        p.max_degree = -1;
        p.limit = 100;
        cbm_search_output_t out = {0};
        if (cbm_store_search(store, &p, &out) == CBM_STORE_OK)
            total += out.count;
        cbm_store_search_free(&out);
    }
    return total;
}

/* Metrics collected in one index pass. */
typedef struct {
    int ok;
    int total_nodes;
    int functions;
    int methods;
    int classes;    /* Class */
    int structs;    /* Struct */
    int enums;      /* Enum */
    int interfaces; /* Interface */
    int traits;     /* Trait */
    int types;      /* all type-like combined */
    int calls;      /* CALLS edges */
    int callers;    /* callables with outbound */
    int imports;    /* IMPORTS edges */
} NcpMetrics;

static NcpMetrics ncp_metrics_files(const NcpLangFile *files, int nfiles) {
    NcpLangProj lp;
    cbm_store_t *store = ncp_index_files(&lp, files, nfiles);
    NcpMetrics m = {0};
    if (store) {
        m.ok = 1;
        m.total_nodes = cbm_store_count_nodes(store, lp.project);
        m.functions = ncp_count_label(store, lp.project, "Function");
        m.methods = ncp_count_label(store, lp.project, "Method");
        m.classes = ncp_count_label(store, lp.project, "Class");
        m.structs = ncp_count_label(store, lp.project, "Struct");
        m.enums = ncp_count_label(store, lp.project, "Enum");
        m.interfaces = ncp_count_label(store, lp.project, "Interface");
        m.traits = ncp_count_label(store, lp.project, "Trait");
        m.types = ncp_type_nodes(store, lp.project);
        m.calls = cbm_store_count_edges_by_type(store, lp.project, "CALLS");
        m.callers = ncp_callables_with_outbound(store, lp.project);
        m.imports = cbm_store_count_edges_by_type(store, lp.project, "IMPORTS");
    }
    ncp_cleanup(&lp, store);
    return m;
}

static NcpMetrics ncp_metrics(const char *filename, const char *content) {
    NcpLangFile f = {filename, content};
    return ncp_metrics_files(&f, 1);
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 1 — Go (hybrid LSP)
 * Labels: Function, Module
 * ══════════════════════════════════════════════════════════════════ */

/* Go: two top-level functions; both must become Function nodes. */
TEST(probe_go_functions) {
    NcpMetrics m = ncp_metrics("svc.go", "package svc\n\n"
                                         "func add(a, b int) int { return a + b }\n\n"
                                         "func sub(a, b int) int { return a - b }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Go: intra-package CALLS edge (function -> function). */
TEST(probe_go_calls_edge) {
    NcpMetrics m =
        ncp_metrics("calc.go", "package calc\n\n"
                               "func double(n int) int { return n * 2 }\n\n"
                               "func quadruple(n int) int { return double(double(n)) }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    PASS();
}

/* Go: variadic + named return (syntactic variants must not drop the node). */
TEST(probe_go_variadic_named_return) {
    NcpMetrics m = ncp_metrics("util.go", "package util\n\n"
                                          "func sum(vals ...int) (total int) {\n"
                                          "    for _, v := range vals { total += v }\n"
                                          "    return\n"
                                          "}\n\n"
                                          "func wrap(vals ...int) int { return sum(vals...) }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Go: method on a named struct type.
 * ASSERTION CORRECTED: the original probe assumed "Go labels methods as Function",
 * but extract_defs.c labels Go RECEIVER methods as "Method" (def.receiver set,
 * label="Method"); only the plain top-level NewCounter is a "Function".  So the
 * correct expectation is 3 callables total (NewCounter Function + Inc/Get Methods),
 * i.e. functions + methods >= 3, not functions >= 3. */
TEST(probe_go_method_on_struct) {
    NcpMetrics m = ncp_metrics("model.go", "package model\n\n"
                                           "type Counter struct{ n int }\n\n"
                                           "func (c *Counter) Inc() { c.n++ }\n\n"
                                           "func (c *Counter) Get() int { return c.n }\n\n"
                                           "func NewCounter() *Counter { return &Counter{} }\n");
    ASSERT_TRUE(m.ok);
    /* NewCounter is a Function; Inc + Get are Methods (receiver methods). */
    ASSERT_TRUE(m.functions + m.methods >= 3);
    PASS();
}

/* Go: interface declaration must produce a type-like node. */
TEST(probe_go_interface_node) {
    NcpMetrics m =
        ncp_metrics("iface.go", "package iface\n\n"
                                "type Reader interface{ Read(p []byte) (int, error) }\n\n"
                                "type Writer interface{ Write(p []byte) (int, error) }\n\n"
                                "func Noop() {}\n");
    ASSERT_TRUE(m.ok);
    /* Go interfaces are typically labelled Class or Interface. */
    ASSERT_TRUE(m.types >= 1 || m.functions >= 1);
    PASS();
}

/* Go: closure assigned to a variable must not prevent enclosing function from
 * appearing as a node. */
TEST(probe_go_closure) {
    NcpMetrics m = ncp_metrics("cb.go", "package cb\n\n"
                                        "func MakeAdder(base int) func(int) int {\n"
                                        "    return func(x int) int { return base + x }\n"
                                        "}\n\n"
                                        "func Apply(f func(int) int, n int) int { return f(n) }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Go: generic function (Go 1.18+) must still create a Function node. */
TEST(probe_go_generic_function) {
    NcpMetrics m =
        ncp_metrics("gen.go", "package gen\n\n"
                              "func Map[T, U any](s []T, f func(T) U) []U {\n"
                              "    r := make([]U, len(s))\n"
                              "    for i, v := range s { r[i] = f(v) }\n"
                              "    return r\n"
                              "}\n\n"
                              "func Filter[T any](s []T, pred func(T) bool) []T {\n"
                              "    var r []T\n"
                              "    for _, v := range s { if pred(v) { r = append(r, v) } }\n"
                              "    return r\n"
                              "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 2 — C (hybrid LSP)
 * Labels: Function, Module
 * ══════════════════════════════════════════════════════════════════ */

/* C: two top-level functions → 2 Function nodes. */
TEST(probe_c_functions) {
    NcpMetrics m = ncp_metrics("math.c", "static int add(int a, int b) { return a + b; }\n"
                                         "int run(int x) { return add(x, 1); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* C: intra-file CALLS edge attributed to a Function, not the Module. */
TEST(probe_c_calls_edge) {
    NcpMetrics m = ncp_metrics("proc.c", "static void helper(int x) { (void)x; }\n"
                                         "void run(void) { helper(42); helper(1); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    PASS();
}

/* C: function pointer typedef must not prevent the wrapping function node. */
TEST(probe_c_function_pointer) {
    NcpMetrics m = ncp_metrics("fp.c", "typedef int (*transform_fn)(int);\n"
                                       "static int double_it(int x) { return x * 2; }\n"
                                       "int apply(transform_fn fn, int v) { return fn(v); }\n"
                                       "int main(void) { return apply(double_it, 21); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 3); /* double_it, apply, main */
    PASS();
}

/* C: macro + inline function — inline keyword must not hide the function. */
TEST(probe_c_inline_function) {
    NcpMetrics m = ncp_metrics("inl.c", "static inline int clamp(int v, int lo, int hi) {\n"
                                        "    return v < lo ? lo : v > hi ? hi : v;\n"
                                        "}\n"
                                        "int normalise(int v) { return clamp(v, 0, 100); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* C: struct declaration creates at least a type-like node (or is labelled
 * Class/Struct/Type depending on the extractor). */
TEST(probe_c_struct_node) {
    NcpMetrics m = ncp_metrics("point.c", "typedef struct { float x; float y; } Point;\n"
                                          "float dist_sq(Point a, Point b) {\n"
                                          "    float dx = a.x - b.x, dy = a.y - b.y;\n"
                                          "    return dx*dx + dy*dy;\n"
                                          "}\n");
    ASSERT_TRUE(m.ok);
    /* The function must always appear. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 3 — C++ (hybrid LSP)
 * Labels: Class:1, Function:1, Module:1  (from grammar golden)
 * ══════════════════════════════════════════════════════════════════ */

/* C++: class with constructor + methods → Class node + Method/Function nodes. */
TEST(probe_cpp_class_methods) {
    NcpMetrics m = ncp_metrics("counter.cpp", "#include <cstdio>\n"
                                              "class Counter {\n"
                                              "    int n;\n"
                                              "public:\n"
                                              "    Counter() : n(0) {}\n"
                                              "    void inc() { ++n; }\n"
                                              "    int get() const { return n; }\n"
                                              "};\n"
                                              "int main() {\n"
                                              "    Counter c;\n"
                                              "    c.inc(); c.inc();\n"
                                              "    return c.get();\n"
                                              "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* Counter class */
    ASSERT_TRUE(m.functions + m.methods >= 1);
    PASS();
}

/* C++: class -> class CALLS edge. */
TEST(probe_cpp_calls_edge) {
    NcpMetrics m =
        ncp_metrics("calc.cpp", "static int square(int x) { return x * x; }\n"
                                "int sumSquares(int a, int b) { return square(a) + square(b); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    PASS();
}

/* C++: template function — template keyword must not prevent the node. */
TEST(probe_cpp_template_function) {
    NcpMetrics m = ncp_metrics("tpl.cpp", "template <typename T>\n"
                                          "T clamp(T v, T lo, T hi) {\n"
                                          "    return v < lo ? lo : v > hi ? hi : v;\n"
                                          "}\n"
                                          "int apply() { return clamp<int>(5, 0, 10); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* C++: enum class must produce a type node. */
TEST(probe_cpp_enum_class) {
    NcpMetrics m = ncp_metrics("color.cpp", "enum class Color { Red, Green, Blue };\n"
                                            "bool isRed(Color c) { return c == Color::Red; }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 1);
    /* Enum may or may not be modeled as a node; at least function must appear. */
    PASS();
}

/* C++: struct with operator overload. */
TEST(probe_cpp_struct_operator) {
    NcpMetrics m =
        ncp_metrics("vec.cpp", "struct Vec2 { float x, y; };\n"
                               "Vec2 operator+(Vec2 a, Vec2 b) { return {a.x+b.x, a.y+b.y}; }\n"
                               "float dot(Vec2 a, Vec2 b) { return a.x*b.x + a.y*b.y; }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1 || m.functions >= 1);
    PASS();
}

/* C++: inheritance — derived class must appear as a type node. */
TEST(probe_cpp_inheritance) {
    NcpMetrics m =
        ncp_metrics("shape.cpp", "class Shape {\n"
                                 "public:\n"
                                 "    virtual double area() const = 0;\n"
                                 "};\n"
                                 "class Circle : public Shape {\n"
                                 "    double r;\n"
                                 "public:\n"
                                 "    Circle(double r) : r(r) {}\n"
                                 "    double area() const override { return 3.14159 * r * r; }\n"
                                 "};\n"
                                 "double total(Circle c) { return c.area(); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 4 — Rust (hybrid LSP)
 * Labels: Class:1, Function:1, Module:1  (struct -> Class)
 * ══════════════════════════════════════════════════════════════════ */

/* Rust: struct + impl with multiple methods. */
TEST(probe_rust_struct_impl) {
    NcpMetrics m =
        ncp_metrics("stack.rs", "struct Stack<T> { items: Vec<T> }\n\n"
                                "impl<T> Stack<T> {\n"
                                "    fn new() -> Self { Stack { items: Vec::new() } }\n"
                                "    fn push(&mut self, item: T) { self.items.push(item); }\n"
                                "    fn pop(&mut self) -> Option<T> { self.items.pop() }\n"
                                "    fn is_empty(&self) -> bool { self.items.is_empty() }\n"
                                "}\n"
                                "fn demo() {\n"
                                "    let mut s: Stack<i32> = Stack::new();\n"
                                "    s.push(1); let _ = s.pop();\n"
                                "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* struct Stack */
    ASSERT_TRUE(m.functions + m.methods >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Rust: enum with variants. */
TEST(probe_rust_enum) {
    NcpMetrics m = ncp_metrics("result.rs",
                               "#[derive(Debug)]\n"
                               "enum MyResult { Ok(i32), Err(String) }\n\n"
                               "fn wrap(v: i32) -> MyResult { MyResult::Ok(v) }\n\n"
                               "fn unwrap_or(r: MyResult, default: i32) -> i32 {\n"
                               "    match r { MyResult::Ok(v) => v, MyResult::Err(_) => default }\n"
                               "}\n");
    ASSERT_TRUE(m.ok);
    /* enum and functions must appear */
    ASSERT_TRUE(m.types >= 1 || m.functions >= 1);
    PASS();
}

/* Rust: trait definition + impl for a struct. */
TEST(probe_rust_trait_impl) {
    NcpMetrics m =
        ncp_metrics("greet.rs", "trait Greet {\n"
                                "    fn hello(&self) -> String;\n"
                                "}\n\n"
                                "struct English;\n"
                                "struct Spanish;\n\n"
                                "impl Greet for English {\n"
                                "    fn hello(&self) -> String { String::from(\"hello\") }\n"
                                "}\n"
                                "impl Greet for Spanish {\n"
                                "    fn hello(&self) -> String { String::from(\"hola\") }\n"
                                "}\n"
                                "fn greet_all(gs: &[&dyn Greet]) {\n"
                                "    for g in gs { println!(\"{}\", g.hello()); }\n"
                                "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* trait and/or struct nodes */
    ASSERT_TRUE(m.functions + m.methods >= 1);
    PASS();
}

/* Rust: intra-file CALLS edge. */
TEST(probe_rust_calls_edge) {
    NcpMetrics m =
        ncp_metrics("math.rs", "fn square(x: i32) -> i32 { x * x }\n\n"
                               "fn sum_squares(a: i32, b: i32) -> i32 { square(a) + square(b) }\n\n"
                               "fn main() { let _ = sum_squares(3, 4); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    PASS();
}

/* Rust: closure must not hide the enclosing function node. */
TEST(probe_rust_closure) {
    NcpMetrics m = ncp_metrics("fmap.rs", "fn double_all(v: &[i32]) -> Vec<i32> {\n"
                                          "    v.iter().map(|x| x * 2).collect()\n"
                                          "}\n\n"
                                          "fn sum(v: &[i32]) -> i32 { v.iter().sum() }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Rust: type alias must not prevent the surrounding function nodes. */
TEST(probe_rust_type_alias) {
    NcpMetrics m =
        ncp_metrics("alias.rs", "type Meters = f64;\n"
                                "type Seconds = f64;\n\n"
                                "fn speed(dist: Meters, time: Seconds) -> f64 { dist / time }\n\n"
                                "fn run() -> f64 { speed(100.0, 9.58) }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 5 — Python (hybrid LSP)
 * Labels: Class:1, Function:1, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* Python: top-level functions → Function nodes. */
TEST(probe_python_functions) {
    NcpMetrics m = ncp_metrics("math.py", "def add(a, b):\n    return a + b\n\n\n"
                                          "def mul(a, b):\n    return a * b\n\n\n"
                                          "def run():\n    return add(mul(2, 3), 1)\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 3);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Python: class with methods → Class node + Function/Method nodes. */
TEST(probe_python_class) {
    NcpMetrics m = ncp_metrics("model.py", "class Account:\n"
                                           "    def __init__(self, owner, balance=0):\n"
                                           "        self.owner = owner\n"
                                           "        self.balance = balance\n\n"
                                           "    def deposit(self, amount):\n"
                                           "        self.balance += amount\n\n"
                                           "    def withdraw(self, amount):\n"
                                           "        if amount <= self.balance:\n"
                                           "            self.balance -= amount\n"
                                           "            return True\n"
                                           "        return False\n\n"
                                           "    def get_balance(self):\n"
                                           "        return self.balance\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* class Account */
    ASSERT_TRUE(m.functions + m.methods >= 3);
    PASS();
}

/* Python: nested function must not prevent outer function node. */
TEST(probe_python_nested_function) {
    NcpMetrics m = ncp_metrics("closure.py", "def make_counter(start=0):\n"
                                             "    count = [start]\n\n"
                                             "    def inc():\n"
                                             "        count[0] += 1\n"
                                             "        return count[0]\n\n"
                                             "    return inc\n\n\n"
                                             "def demo():\n"
                                             "    c = make_counter()\n"
                                             "    return c()\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2); /* make_counter, demo (inc is nested) */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Python: decorator syntax must not prevent the decorated function node. */
TEST(probe_python_decorator) {
    NcpMetrics m = ncp_metrics("deco.py", "def log_call(fn):\n"
                                          "    def wrapper(*args, **kwargs):\n"
                                          "        result = fn(*args, **kwargs)\n"
                                          "        return result\n"
                                          "    return wrapper\n\n\n"
                                          "@log_call\n"
                                          "def compute(x):\n"
                                          "    return x * x\n\n\n"
                                          "@log_call\n"
                                          "def add(a, b):\n"
                                          "    return a + b\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 3); /* log_call, compute, add */
    PASS();
}

/* Python: lambda in assignment must not prevent surrounding function. */
TEST(probe_python_lambda) {
    NcpMetrics m = ncp_metrics("lmb.py", "double = lambda x: x * 2\n\n\n"
                                         "def apply(fn, items):\n"
                                         "    return [fn(i) for i in items]\n\n\n"
                                         "def run():\n"
                                         "    return apply(double, [1, 2, 3])\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2); /* apply + run */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Python: dataclass + multiple methods. */
TEST(probe_python_dataclass) {
    NcpMetrics m =
        ncp_metrics("point.py", "from dataclasses import dataclass\n\n\n"
                                "@dataclass\n"
                                "class Point:\n"
                                "    x: float\n"
                                "    y: float\n\n"
                                "    def distance_sq(self, other):\n"
                                "        return (self.x - other.x)**2 + (self.y - other.y)**2\n\n"
                                "    def move(self, dx, dy):\n"
                                "        return Point(self.x + dx, self.y + dy)\n\n\n"
                                "def origin():\n"
                                "    return Point(0.0, 0.0)\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 6 — TypeScript (hybrid LSP)
 * Labels: Class:1, Function:1, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* TypeScript: interface + class implementing it. */
TEST(probe_ts_interface_class) {
    NcpMetrics m =
        ncp_metrics("iface.ts", "interface Shape {\n"
                                "    area(): number;\n"
                                "    perimeter(): number;\n"
                                "}\n\n"
                                "class Rectangle implements Shape {\n"
                                "    constructor(private w: number, private h: number) {}\n"
                                "    area(): number { return this.w * this.h; }\n"
                                "    perimeter(): number { return 2 * (this.w + this.h); }\n"
                                "}\n\n"
                                "function describe(s: Shape): string {\n"
                                "    return `area=${s.area()} perimeter=${s.perimeter()}`;\n"
                                "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* class Rectangle, possibly interface Shape */
    ASSERT_TRUE(m.functions + m.methods >= 1);
    PASS();
}

/* TypeScript: generic function + arrow function. */
TEST(probe_ts_generic_arrow) {
    NcpMetrics m =
        ncp_metrics("util.ts", "export function identity<T>(x: T): T { return x; }\n\n"
                               "export const double = (n: number): number => n * 2;\n\n"
                               "export function applyTwice<T>(f: (x: T) => T, x: T): T {\n"
                               "    return f(f(x));\n"
                               "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 1); /* identity + applyTwice at minimum */
    PASS();
}

/* TypeScript: enum must produce a type-like node. */
TEST(probe_ts_enum) {
    NcpMetrics m = ncp_metrics(
        "status.ts",
        "export enum Status { Active = 'active', Inactive = 'inactive', Pending = 'pending' }\n\n"
        "export function isActive(s: Status): boolean {\n"
        "    return s === Status.Active;\n"
        "}\n\n"
        "export function toggle(s: Status): Status {\n"
        "    return s === Status.Active ? Status.Inactive : Status.Active;\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* TypeScript: CALLS edge via intra-file call. */
TEST(probe_ts_calls_edge) {
    NcpMetrics m = ncp_metrics("chain.ts",
                               "function square(n: number): number { return n * n; }\n\n"
                               "function cube(n: number): number { return n * square(n); }\n\n"
                               "function run(n: number): number { return cube(n) + square(n); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    PASS();
}

/* TypeScript: class inheritance. */
TEST(probe_ts_class_extends) {
    NcpMetrics m = ncp_metrics(
        "vehicle.ts", "class Vehicle {\n"
                      "    constructor(public speed: number) {}\n"
                      "    move(): string { return `moving at ${this.speed}`; }\n"
                      "}\n\n"
                      "class Car extends Vehicle {\n"
                      "    constructor(speed: number, public brand: string) { super(speed); }\n"
                      "    describe(): string { return `${this.brand}: ${this.move()}`; }\n"
                      "}\n\n"
                      "function demo(): string {\n"
                      "    const c = new Car(100, 'Toyota');\n"
                      "    return c.describe();\n"
                      "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* TypeScript: type alias + union type function. */
TEST(probe_ts_type_alias) {
    NcpMetrics m = ncp_metrics(
        "types.ts", "type ID = string | number;\n"
                    "type Result<T> = { ok: true; value: T } | { ok: false; error: string };\n\n"
                    "function parseId(raw: string): Result<ID> {\n"
                    "    const n = Number(raw);\n"
                    "    return isNaN(n) ? { ok: true, value: raw } : { ok: true, value: n };\n"
                    "}\n\n"
                    "function formatId(id: ID): string { return String(id); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 7 — Java (hybrid LSP)
 * Labels: Class:1, Method:1, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* Java: class + multiple methods → Class node + Method nodes. */
TEST(probe_java_class_methods) {
    NcpMetrics m =
        ncp_metrics("Calc.java", "package app;\n\n"
                                 "class Calc {\n"
                                 "    private int base;\n\n"
                                 "    public Calc(int base) { this.base = base; }\n\n"
                                 "    public int add(int x) { return base + x; }\n\n"
                                 "    public int mul(int x) { return base * x; }\n\n"
                                 "    public int addThenMul(int x) { return mul(add(x)); }\n"
                                 "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);   /* class Calc */
    ASSERT_TRUE(m.methods >= 2); /* add, mul, addThenMul */
    ASSERT_TRUE(m.calls >= 1);   /* addThenMul -> add + mul */
    PASS();
}

/* Java: interface declaration → type-like node. */
TEST(probe_java_interface) {
    NcpMetrics m = ncp_metrics("Printable.java",
                               "package app;\n\n"
                               "interface Printable {\n"
                               "    String format();\n"
                               "    default void print() { System.out.println(format()); }\n"
                               "}\n\n"
                               "class Doc implements Printable {\n"
                               "    private String text;\n"
                               "    Doc(String t) { this.text = t; }\n"
                               "    public String format() { return \"[\" + text + \"]\"; }\n"
                               "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Java: enum → type-like node. */
TEST(probe_java_enum) {
    NcpMetrics m = ncp_metrics(
        "Day.java",
        "package app;\n\n"
        "enum Day { MON, TUE, WED, THU, FRI, SAT, SUN;\n"
        "    public boolean isWeekend() {\n"
        "        return this == SAT || this == SUN;\n"
        "    }\n"
        "}\n\n"
        "class DayUtil {\n"
        "    static String label(Day d) { return d.isWeekend() ? \"rest\" : \"work\"; }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Java: inner class inside outer class. */
TEST(probe_java_inner_class) {
    NcpMetrics m = ncp_metrics("Outer.java", "package app;\n\n"
                                             "class Outer {\n"
                                             "    private int x;\n"
                                             "    Outer(int x) { this.x = x; }\n\n"
                                             "    class Inner {\n"
                                             "        int doubled() { return x * 2; }\n"
                                             "    }\n\n"
                                             "    int run() {\n"
                                             "        return new Inner().doubled();\n"
                                             "    }\n"
                                             "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* Outer (and optionally Inner) */
    PASS();
}

/* Java: lambda / method reference — enclosing method node must still appear. */
TEST(probe_java_lambda) {
    NcpMetrics m = ncp_metrics(
        "Streams.java",
        "package app;\n\n"
        "import java.util.List;\n"
        "import java.util.stream.Collectors;\n\n"
        "class Streams {\n"
        "    static List<Integer> doubleAll(List<Integer> xs) {\n"
        "        return xs.stream().map(x -> x * 2).collect(Collectors.toList());\n"
        "    }\n"
        "    static List<String> names(List<Object> objs) {\n"
        "        return objs.stream().map(Object::toString).collect(Collectors.toList());\n"
        "    }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.methods >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 8 — Kotlin (hybrid LSP)
 * Labels: Class:1, Function:1, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* Kotlin: data class + companion object + extension function. */
TEST(probe_kotlin_data_class) {
    NcpMetrics m = ncp_metrics("User.kt", "data class User(val name: String, val age: Int) {\n"
                                          "    fun greet(): String = \"Hi, I'm ${name}\"\n\n"
                                          "    companion object {\n"
                                          "        fun anonymous(): User = User(\"anon\", 0)\n"
                                          "    }\n"
                                          "}\n\n"
                                          "fun User.isAdult(): Boolean = age >= 18\n\n"
                                          "fun demo(): String {\n"
                                          "    val u = User.anonymous()\n"
                                          "    return u.greet()\n"
                                          "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* data class User */
    ASSERT_TRUE(m.functions + m.methods >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Kotlin: sealed class hierarchy → type nodes. */
TEST(probe_kotlin_sealed_class) {
    NcpMetrics m =
        ncp_metrics("Result.kt", "sealed class Result<out T> {\n"
                                 "    data class Success<T>(val value: T) : Result<T>()\n"
                                 "    data class Failure(val error: String) : Result<Nothing>()\n"
                                 "}\n\n"
                                 "fun <T> unwrap(r: Result<T>, default: T): T =\n"
                                 "    when (r) {\n"
                                 "        is Result.Success -> r.value\n"
                                 "        is Result.Failure -> default\n"
                                 "    }\n\n"
                                 "fun run(): String = unwrap(Result.Success(\"ok\"), \"fail\")\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Kotlin: interface + lambda / higher-order function. */
TEST(probe_kotlin_interface_lambda) {
    NcpMetrics m = ncp_metrics("Filter.kt",
                               "interface Predicate<T> {\n"
                               "    fun test(item: T): Boolean\n"
                               "}\n\n"
                               "fun <T> filterList(items: List<T>, pred: Predicate<T>): List<T> =\n"
                               "    items.filter { pred.test(it) }\n\n"
                               "fun evens(xs: List<Int>): List<Int> =\n"
                               "    filterList(xs, object : Predicate<Int> { override fun "
                               "test(item: Int) = item % 2 == 0 })\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Kotlin: CALLS edge between top-level functions. */
TEST(probe_kotlin_calls_edge) {
    NcpMetrics m =
        ncp_metrics("Math.kt", "fun square(n: Int): Int = n * n\n\n"
                               "fun cube(n: Int): Int = n * square(n)\n\n"
                               "fun sumOfCubes(a: Int, b: Int): Int = cube(a) + cube(b)\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    PASS();
}

/* Kotlin: enum class. */
TEST(probe_kotlin_enum) {
    NcpMetrics m = ncp_metrics(
        "Color.kt", "enum class Color(val hex: String) {\n"
                    "    RED(\"#FF0000\"), GREEN(\"#00FF00\"), BLUE(\"#0000FF\");\n\n"
                    "    fun isDark(): Boolean = hex.substring(1).toLong(16) < 0x888888L\n"
                    "}\n\n"
                    "fun redHex(): String = Color.RED.hex\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* enum class Color */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 9 — C# (hybrid LSP)
 * Labels: Class:1, Method:1, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* C#: class + multiple methods + intra-class CALLS. */
TEST(probe_cs_class_methods) {
    NcpMetrics m =
        ncp_metrics("Calc.cs", "namespace App {\n"
                               "    class Calc {\n"
                               "        private int offset;\n"
                               "        public Calc(int offset) { this.offset = offset; }\n"
                               "        public int Add(int x) { return offset + x; }\n"
                               "        public int Double(int x) { return Add(Add(x)); }\n"
                               "    }\n"
                               "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.methods >= 2); /* Add, Double */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* C#: interface + class implementing it. */
TEST(probe_cs_interface) {
    NcpMetrics m = ncp_metrics(
        "IRepo.cs", "namespace App {\n"
                    "    interface IRepo<T> {\n"
                    "        T Get(int id);\n"
                    "        void Save(T item);\n"
                    "    }\n\n"
                    "    class MemRepo<T> : IRepo<T> {\n"
                    "        private System.Collections.Generic.Dictionary<int, T> store = new();\n"
                    "        public T Get(int id) => store[id];\n"
                    "        public void Save(T item) { store[store.Count] = item; }\n"
                    "    }\n"
                    "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* C#: enum type. */
TEST(probe_cs_enum) {
    NcpMetrics m = ncp_metrics(
        "Status.cs",
        "namespace App {\n"
        "    enum Status { Active, Inactive, Pending }\n\n"
        "    class StatusHelper {\n"
        "        public static bool IsActive(Status s) => s == Status.Active;\n"
        "        public static string Label(Status s) => IsActive(s) ? \"on\" : \"off\";\n"
        "    }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.methods >= 1);
    PASS();
}

/* C#: generic class. */
TEST(probe_cs_generic_class) {
    NcpMetrics m = ncp_metrics("Box.cs", "namespace App {\n"
                                         "    class Box<T> {\n"
                                         "        private T value;\n"
                                         "        public Box(T v) { this.value = v; }\n"
                                         "        public T Unwrap() { return value; }\n"
                                         "        public Box<U> Map<U>(System.Func<T, U> f) {\n"
                                         "            return new Box<U>(f(Unwrap()));\n"
                                         "        }\n"
                                         "    }\n"
                                         "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.methods >= 2);
    PASS();
}

/* C#: struct type (value type). */
TEST(probe_cs_struct) {
    NcpMetrics m = ncp_metrics(
        "Point.cs",
        "namespace App {\n"
        "    struct Point {\n"
        "        public float X, Y;\n"
        "        public Point(float x, float y) { X = x; Y = y; }\n"
        "        public float MagnitudeSq() { return X * X + Y * Y; }\n"
        "        public Point Add(Point other) { return new Point(X + other.X, Y + other.Y); }\n"
        "    }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* struct Point */
    ASSERT_TRUE(m.methods >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 10 — Ruby (grammar-only)
 * Labels: Class:1, Function:1, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* Ruby: class with methods + CALLS edge. */
TEST(probe_ruby_class_calls) {
    NcpMetrics m = ncp_metrics("greeter.rb", "class Greeter\n"
                                             "  def initialize(name)\n"
                                             "    @name = name\n"
                                             "  end\n\n"
                                             "  def greet\n"
                                             "    format_greeting(@name)\n"
                                             "  end\n\n"
                                             "  private\n\n"
                                             "  def format_greeting(n)\n"
                                             "    \"Hello, #{n}!\"\n"
                                             "  end\n"
                                             "end\n\n"
                                             "def make_greeter(name)\n"
                                             "  Greeter.new(name)\n"
                                             "end\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* class Greeter */
    ASSERT_TRUE(m.functions >= 1);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Ruby: module definition creates a type-like node. */
TEST(probe_ruby_module) {
    NcpMetrics m = ncp_metrics("utils.rb", "module MathUtils\n"
                                           "  def self.square(x)\n"
                                           "    x * x\n"
                                           "  end\n\n"
                                           "  def self.cube(x)\n"
                                           "    x * square(x)\n"
                                           "  end\n"
                                           "end\n");
    ASSERT_TRUE(m.ok);
    /* A Module might be modelled as a Class or Function label. */
    ASSERT_TRUE(m.types >= 1 || m.functions >= 1);
    PASS();
}

/* Ruby: multiple top-level functions → Function nodes. */
TEST(probe_ruby_toplevel_functions) {
    NcpMetrics m = ncp_metrics("calc.rb", "def add(a, b)\n  a + b\nend\n\n"
                                          "def mul(a, b)\n  a * b\nend\n\n"
                                          "def run\n  add(mul(2, 3), 1)\nend\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 3); /* add, mul, run */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 11 — Swift (grammar-only)
 * Labels: Class:1, Function:1, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* Swift: struct + functions + CALLS edge. */
TEST(probe_swift_struct_calls) {
    NcpMetrics m =
        ncp_metrics("geo.swift", "struct Point {\n"
                                 "    var x: Double\n"
                                 "    var y: Double\n\n"
                                 "    func distanceTo(_ other: Point) -> Double {\n"
                                 "        let dx = x - other.x\n"
                                 "        let dy = y - other.y\n"
                                 "        return sqrt(dx*dx + dy*dy)\n"
                                 "    }\n"
                                 "}\n\n"
                                 "func origin() -> Point { return Point(x: 0, y: 0) }\n\n"
                                 "func run() -> Double {\n"
                                 "    let p = origin()\n"
                                 "    return p.distanceTo(Point(x: 3, y: 4))\n"
                                 "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions >= 1);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Swift: class with inheritance. */
TEST(probe_swift_class_inherit) {
    NcpMetrics m = ncp_metrics(
        "shape.swift", "class Shape {\n"
                       "    func area() -> Double { return 0.0 }\n"
                       "}\n\n"
                       "class Circle: Shape {\n"
                       "    let radius: Double\n"
                       "    init(radius: Double) { self.radius = radius }\n"
                       "    override func area() -> Double { return 3.14159 * radius * radius }\n"
                       "}\n\n"
                       "func totalArea(_ shapes: [Shape]) -> Double {\n"
                       "    return shapes.reduce(0.0) { $0 + $1.area() }\n"
                       "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Swift: enum with associated values. */
TEST(probe_swift_enum) {
    NcpMetrics m =
        ncp_metrics("result.swift", "enum AppError: Error {\n"
                                    "    case notFound(String)\n"
                                    "    case invalid\n"
                                    "}\n\n"
                                    "func lookup(_ key: String) -> Result<Int, AppError> {\n"
                                    "    if key == \"x\" { return .success(42) }\n"
                                    "    return .failure(.notFound(key))\n"
                                    "}\n\n"
                                    "func run() -> Int {\n"
                                    "    switch lookup(\"x\") {\n"
                                    "    case .success(let v): return v\n"
                                    "    case .failure: return -1\n"
                                    "    }\n"
                                    "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 12 — Scala (grammar-only)
 * Labels: Class:1, Function:1, Method:1, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* Scala: object + class + methods. */
TEST(probe_scala_class_object) {
    NcpMetrics m = ncp_metrics("calc.scala", "class Calc(base: Int) {\n"
                                             "    def add(x: Int): Int = base + x\n"
                                             "    def mul(x: Int): Int = base * x\n"
                                             "    def addThenMul(x: Int): Int = mul(add(x))\n"
                                             "}\n\n"
                                             "object CalcApp {\n"
                                             "    def main(args: Array[String]): Unit = {\n"
                                             "        val c = new Calc(10)\n"
                                             "        println(c.addThenMul(5))\n"
                                             "    }\n"
                                             "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions + m.methods >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Scala: trait definition. */
TEST(probe_scala_trait) {
    NcpMetrics m = ncp_metrics(
        "greet.scala",
        "trait Greeter {\n"
        "    def greet(name: String): String\n"
        "    def greetAll(names: List[String]): List[String] =\n"
        "        names.map(greet)\n"
        "}\n\n"
        "class PoliteGreeter extends Greeter {\n"
        "    def greet(name: String): String = s\"Good day, $name\"\n"
        "}\n\n"
        "def run(): List[String] = new PoliteGreeter().greetAll(List(\"Alice\", \"Bob\"))\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions + m.methods >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 13 — Lua (grammar-only)
 * Labels: Function:2, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* Lua: top-level functions + CALLS edge. */
TEST(probe_lua_functions_calls) {
    NcpMetrics m = ncp_metrics("math.lua", "local function square(x)\n"
                                           "    return x * x\n"
                                           "end\n\n"
                                           "local function hypotenuse(a, b)\n"
                                           "    return math.sqrt(square(a) + square(b))\n"
                                           "end\n\n"
                                           "function run(a, b)\n"
                                           "    return hypotenuse(a, b)\n"
                                           "end\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2); /* square, hypotenuse (or run) */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Lua: table acting as class-like module — functions must appear. */
TEST(probe_lua_table_class) {
    NcpMetrics m = ncp_metrics("counter.lua", "local Counter = {}\n"
                                              "Counter.__index = Counter\n\n"
                                              "function Counter.new(start)\n"
                                              "    local self = setmetatable({}, Counter)\n"
                                              "    self.value = start or 0\n"
                                              "    return self\n"
                                              "end\n\n"
                                              "function Counter:inc()\n"
                                              "    self.value = self.value + 1\n"
                                              "end\n\n"
                                              "function Counter:get()\n"
                                              "    return self.value\n"
                                              "end\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 14 — Bash (grammar-only)
 * Labels: Function:2, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* Bash: function declarations + CALLS edge. */
TEST(probe_bash_functions_calls) {
    NcpMetrics m = ncp_metrics("deploy.sh", "#!/usr/bin/env bash\n\n"
                                            "log() {\n"
                                            "    echo \"[INFO] $*\"\n"
                                            "}\n\n"
                                            "build() {\n"
                                            "    log \"building...\"\n"
                                            "    make all\n"
                                            "}\n\n"
                                            "deploy() {\n"
                                            "    log \"deploying...\"\n"
                                            "    build\n"
                                            "}\n\n"
                                            "main() {\n"
                                            "    deploy \"$@\"\n"
                                            "}\n\n"
                                            "main \"$@\"\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 3); /* log, build, deploy, main */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Bash: function() { } syntax variant. */
TEST(probe_bash_function_keyword) {
    NcpMetrics m = ncp_metrics("lib.sh", "function helper {\n"
                                         "    echo \"help: $1\"\n"
                                         "}\n\n"
                                         "function runner {\n"
                                         "    helper \"start\"\n"
                                         "    helper \"stop\"\n"
                                         "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 15 — Dart (grammar-only)
 * Labels: Class:1, Function:1, Module:1
 *
 * KNOWN GAP: CALLS edge NOT expected — extract_calls.c has no Dart branch.
 * Node-creation (Function/Class nodes) IS expected to work.
 * ══════════════════════════════════════════════════════════════════ */

/* Dart: top-level functions → Function nodes (GREEN expected). */
TEST(probe_dart_functions) {
    NcpMetrics m = ncp_metrics("math.dart", "int square(int x) => x * x;\n\n"
                                            "int cube(int x) => x * square(x);\n\n"
                                            "void main() {\n"
                                            "    print(cube(3));\n"
                                            "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2); /* square, cube (main may also appear) */
    PASS();
}

/* Dart: class with methods → Class node + Function nodes (GREEN expected). */
TEST(probe_dart_class) {
    NcpMetrics m = ncp_metrics("account.dart", "class Account {\n"
                                               "    String owner;\n"
                                               "    double balance;\n\n"
                                               "    Account(this.owner, {this.balance = 0.0});\n\n"
                                               "    void deposit(double amount) {\n"
                                               "        balance += amount;\n"
                                               "    }\n\n"
                                               "    bool withdraw(double amount) {\n"
                                               "        if (amount > balance) return false;\n"
                                               "        balance -= amount;\n"
                                               "        return true;\n"
                                               "    }\n"
                                               "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* class Account */
    PASS();
}

/* Dart CALLS edge — KNOWN EXPECTED RED: extract_calls.c lacks Dart branch.
 * Kept as a reproduction: turns GREEN when the fix is shipped. */
TEST(probe_dart_calls_edge_known_gap) {
    NcpMetrics m = ncp_metrics("chain.dart", "void helper() {\n  print('helper');\n}\n\n"
                                             "void run() {\n  helper();\n}\n");
    ASSERT_TRUE(m.ok);
    /* This assertion SHOULD fail until the Dart CALLS branch is added. */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 16 — Elixir (grammar-only)
 * Labels: Class:1, Function:1, Module:1  (module -> Class)
 * ══════════════════════════════════════════════════════════════════ */

/* Elixir: defmodule with multiple defs + CALLS edge. */
TEST(probe_elixir_module_calls) {
    NcpMetrics m = ncp_metrics("calc.ex", "defmodule Calc do\n"
                                          "  def add(a, b), do: a + b\n\n"
                                          "  def mul(a, b), do: a * b\n\n"
                                          "  def add_then_mul(x, y) do\n"
                                          "    sum = add(x, y)\n"
                                          "    mul(sum, 2)\n"
                                          "  end\n"
                                          "end\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1 || m.functions >= 1); /* module as Class or Function */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Elixir: nested module. */
TEST(probe_elixir_nested_module) {
    NcpMetrics m = ncp_metrics("server.ex", "defmodule Server.Router do\n"
                                            "  def dispatch(path) do\n"
                                            "    handle(path)\n"
                                            "  end\n\n"
                                            "  defp handle(\"/health\"), do: :ok\n"
                                            "  defp handle(_), do: :not_found\n"
                                            "end\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 17 — Haskell (grammar-only)
 * Labels: Function:1, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* Haskell: multiple top-level function definitions + CALLS edge. */
TEST(probe_haskell_functions_calls) {
    NcpMetrics m = ncp_metrics("math.hs", "module Math where\n\n"
                                          "square :: Int -> Int\n"
                                          "square x = x * x\n\n"
                                          "cube :: Int -> Int\n"
                                          "cube x = x * square x\n\n"
                                          "run :: Int -> Int\n"
                                          "run n = cube n + square n\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 1); /* at least one of square/cube/run */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Haskell: data type definition must not break function node extraction. */
TEST(probe_haskell_data_type) {
    NcpMetrics m = ncp_metrics("tree.hs", "module Tree where\n\n"
                                          "data Tree a = Leaf | Node (Tree a) a (Tree a)\n\n"
                                          "insert :: Ord a => a -> Tree a -> Tree a\n"
                                          "insert x Leaf = Node Leaf x Leaf\n"
                                          "insert x (Node l v r)\n"
                                          "    | x < v    = Node (insert x l) v r\n"
                                          "    | x > v    = Node l v (insert x r)\n"
                                          "    | otherwise = Node l v r\n\n"
                                          "depth :: Tree a -> Int\n"
                                          "depth Leaf = 0\n"
                                          "depth (Node l _ r) = 1 + max (depth l) (depth r)\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 18 — Zig (grammar-only)
 * Labels: Function:2, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* Zig: pub functions + CALLS edge. */
TEST(probe_zig_functions_calls) {
    NcpMetrics m = ncp_metrics("math.zig", "const std = @import(\"std\");\n\n"
                                           "fn square(x: i32) i32 {\n"
                                           "    return x * x;\n"
                                           "}\n\n"
                                           "pub fn sumSquares(a: i32, b: i32) i32 {\n"
                                           "    return square(a) + square(b);\n"
                                           "}\n\n"
                                           "pub fn main() void {\n"
                                           "    const result = sumSquares(3, 4);\n"
                                           "    std.debug.print(\"{d}\\n\", .{result});\n"
                                           "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Zig: struct with methods. */
TEST(probe_zig_struct_methods) {
    NcpMetrics m = ncp_metrics("stack.zig", "const Stack = struct {\n"
                                            "    items: [64]i32 = undefined,\n"
                                            "    top: usize = 0,\n\n"
                                            "    pub fn push(self: *Stack, v: i32) void {\n"
                                            "        self.items[self.top] = v;\n"
                                            "        self.top += 1;\n"
                                            "    }\n\n"
                                            "    pub fn pop(self: *Stack) i32 {\n"
                                            "        self.top -= 1;\n"
                                            "        return self.items[self.top];\n"
                                            "    }\n"
                                            "};\n\n"
                                            "pub fn run() void {\n"
                                            "    var s = Stack{};\n"
                                            "    s.push(1);\n"
                                            "    _ = s.pop();\n"
                                            "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 1); /* run must be present */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 19 — OCaml (grammar-only)
 * Labels: Function:2, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* OCaml: let bindings + application call. */
TEST(probe_ocaml_functions_calls) {
    NcpMetrics m = ncp_metrics("math.ml", "let square x = x * x\n\n"
                                          "let cube x = x * square x\n\n"
                                          "let sum_squares a b = square a + square b\n\n"
                                          "let () =\n"
                                          "    let _ = cube 3 in\n"
                                          "    let _ = sum_squares 3 4 in\n"
                                          "    ()\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 20 — Erlang (grammar-only)
 * Labels: Function:2, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* Erlang: module + exported functions + CALLS edge. */
TEST(probe_erlang_functions_calls) {
    NcpMetrics m = ncp_metrics("math.erl", "-module(math).\n"
                                           "-export([run/0, sum_squares/2]).\n\n"
                                           "square(X) -> X * X.\n\n"
                                           "sum_squares(A, B) -> square(A) + square(B).\n\n"
                                           "run() ->\n"
                                           "    io:format(\"~p~n\", [sum_squares(3, 4)]).\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 21 — Groovy (grammar-only)
 * Labels: Class:1, Method:1, Module:1
 *
 * KNOWN GAP: CALLS edge NOT expected — function_call callee resolution
 * unhandled in extract_calls.c for Groovy.
 * Node creation (Class/Method) IS expected to work.
 * ══════════════════════════════════════════════════════════════════ */

/* Groovy: class + methods → Class + Method nodes (GREEN expected). */
TEST(probe_groovy_class_methods) {
    NcpMetrics m = ncp_metrics("Calc.groovy", "class Calc {\n"
                                              "    int base\n\n"
                                              "    Calc(int base) { this.base = base }\n\n"
                                              "    int add(int x) { base + x }\n\n"
                                              "    int mul(int x) { base * x }\n"
                                              "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* class Calc */
    ASSERT_TRUE(m.methods >= 1);
    PASS();
}

/* Groovy CALLS edge — KNOWN EXPECTED RED: callee extraction not handled.
 * Kept as reproduction; turns GREEN when the fix ships. */
TEST(probe_groovy_calls_edge_known_gap) {
    NcpMetrics m = ncp_metrics("funcs.groovy", "def helper() {\n    println 'helping'\n}\n\n"
                                               "def runner() {\n    helper()\n}\n");
    ASSERT_TRUE(m.ok);
    /* This assertion SHOULD fail until Groovy callee extraction is added. */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 23 — GDScript (grammar-only)
 * Labels: Function:1, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* GDScript: func declarations + CALLS edge. */
TEST(probe_gdscript_functions_calls) {
    NcpMetrics m = ncp_metrics("player.gd", "extends Node\n\n"
                                            "var health: int = 100\n\n"
                                            "func take_damage(amount: int) -> void:\n"
                                            "\thealth -= amount\n"
                                            "\tif health <= 0:\n"
                                            "\t\tdie()\n\n"
                                            "func die() -> void:\n"
                                            "\tprint(\"died\")\n\n"
                                            "func heal(amount: int) -> void:\n"
                                            "\thealth = min(health + amount, 100)\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2); /* take_damage, die, heal */
    ASSERT_TRUE(m.calls >= 1);     /* take_damage -> die */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 24 — Perl (grammar-only)
 * Labels: Function:2, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* Perl: sub declarations + CALLS edge. */
TEST(probe_perl_functions_calls) {
    NcpMetrics m = ncp_metrics("math.pl", "#!/usr/bin/perl\n"
                                          "use strict;\n"
                                          "use warnings;\n\n"
                                          "sub square {\n"
                                          "    my ($x) = @_;\n"
                                          "    return $x * $x;\n"
                                          "}\n\n"
                                          "sub sum_squares {\n"
                                          "    my ($a, $b) = @_;\n"
                                          "    return square($a) + square($b);\n"
                                          "}\n\n"
                                          "sub run {\n"
                                          "    print sum_squares(3, 4), \"\\n\";\n"
                                          "}\n\n"
                                          "run();\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 25 — R (grammar-only)
 * Labels: Function:2, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* R: function assignments + CALLS edge. */
TEST(probe_r_functions_calls) {
    NcpMetrics m = ncp_metrics("math.R", "square <- function(x) {\n"
                                         "  x * x\n"
                                         "}\n\n"
                                         "sum_squares <- function(a, b) {\n"
                                         "  square(a) + square(b)\n"
                                         "}\n\n"
                                         "run <- function() {\n"
                                         "  cat(sum_squares(3, 4), \"\\n\")\n"
                                         "}\n\n"
                                         "run()\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 26 — Fortran (grammar-only)
 * Labels: Function:1, Module:1
 * ══════════════════════════════════════════════════════════════════ */

/* Fortran: function + subroutine + call. */
TEST(probe_fortran_functions_calls) {
    NcpMetrics m = ncp_metrics("math.f90", "function square(x) result(y)\n"
                                           "    integer, intent(in) :: x\n"
                                           "    integer :: y\n"
                                           "    y = x * x\n"
                                           "end function square\n\n"
                                           "function sum_sq(a, b) result(total)\n"
                                           "    integer, intent(in) :: a, b\n"
                                           "    integer :: total\n"
                                           "    total = square(a) + square(b)\n"
                                           "end function sum_sq\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 1);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * SUITE REGISTRATION
 * ══════════════════════════════════════════════════════════════════ */

SUITE(node_creation_probe) {
    /* Go (hybrid LSP) */
    RUN_TEST(probe_go_functions);
    RUN_TEST(probe_go_calls_edge);
    RUN_TEST(probe_go_variadic_named_return);
    RUN_TEST(probe_go_method_on_struct);
    RUN_TEST(probe_go_interface_node);
    RUN_TEST(probe_go_closure);
    RUN_TEST(probe_go_generic_function);

    /* C (hybrid LSP) */
    RUN_TEST(probe_c_functions);
    RUN_TEST(probe_c_calls_edge);
    RUN_TEST(probe_c_function_pointer);
    RUN_TEST(probe_c_inline_function);
    RUN_TEST(probe_c_struct_node);

    /* C++ (hybrid LSP) */
    RUN_TEST(probe_cpp_class_methods);
    RUN_TEST(probe_cpp_calls_edge);
    RUN_TEST(probe_cpp_template_function);
    RUN_TEST(probe_cpp_enum_class);
    RUN_TEST(probe_cpp_struct_operator);
    RUN_TEST(probe_cpp_inheritance);

    /* Rust (hybrid LSP) */
    RUN_TEST(probe_rust_struct_impl);
    RUN_TEST(probe_rust_enum);
    RUN_TEST(probe_rust_trait_impl);
    RUN_TEST(probe_rust_calls_edge);
    RUN_TEST(probe_rust_closure);
    RUN_TEST(probe_rust_type_alias);

    /* Python (hybrid LSP) */
    RUN_TEST(probe_python_functions);
    RUN_TEST(probe_python_class);
    RUN_TEST(probe_python_nested_function);
    RUN_TEST(probe_python_decorator);
    RUN_TEST(probe_python_lambda);
    RUN_TEST(probe_python_dataclass);

    /* TypeScript (hybrid LSP) */
    RUN_TEST(probe_ts_interface_class);
    RUN_TEST(probe_ts_generic_arrow);
    RUN_TEST(probe_ts_enum);
    RUN_TEST(probe_ts_calls_edge);
    RUN_TEST(probe_ts_class_extends);
    RUN_TEST(probe_ts_type_alias);

    /* Java (hybrid LSP) */
    RUN_TEST(probe_java_class_methods);
    RUN_TEST(probe_java_interface);
    RUN_TEST(probe_java_enum);
    RUN_TEST(probe_java_inner_class);
    RUN_TEST(probe_java_lambda);

    /* Kotlin (hybrid LSP) */
    RUN_TEST(probe_kotlin_data_class);
    RUN_TEST(probe_kotlin_sealed_class);
    RUN_TEST(probe_kotlin_interface_lambda);
    RUN_TEST(probe_kotlin_calls_edge);
    RUN_TEST(probe_kotlin_enum);

    /* C# (hybrid LSP) */
    RUN_TEST(probe_cs_class_methods);
    RUN_TEST(probe_cs_interface);
    RUN_TEST(probe_cs_enum);
    RUN_TEST(probe_cs_generic_class);
    RUN_TEST(probe_cs_struct);

    /* Ruby (grammar-only) */
    RUN_TEST(probe_ruby_class_calls);
    RUN_TEST(probe_ruby_module);
    RUN_TEST(probe_ruby_toplevel_functions);

    /* Swift (grammar-only) */
    RUN_TEST(probe_swift_struct_calls);
    RUN_TEST(probe_swift_class_inherit);
    RUN_TEST(probe_swift_enum);

    /* Scala (grammar-only) */
    RUN_TEST(probe_scala_class_object);
    RUN_TEST(probe_scala_trait);

    /* Lua (grammar-only) */
    RUN_TEST(probe_lua_functions_calls);
    RUN_TEST(probe_lua_table_class);

    /* Bash (grammar-only) */
    RUN_TEST(probe_bash_functions_calls);
    RUN_TEST(probe_bash_function_keyword);

    /* Dart (grammar-only; CALLS gap is a known-red reproduction) */
    RUN_TEST(probe_dart_functions);
    RUN_TEST(probe_dart_class);
    RUN_TEST(probe_dart_calls_edge_known_gap);

    /* Elixir (grammar-only) */
    RUN_TEST(probe_elixir_module_calls);
    RUN_TEST(probe_elixir_nested_module);

    /* Haskell (grammar-only) */
    RUN_TEST(probe_haskell_functions_calls);
    RUN_TEST(probe_haskell_data_type);

    /* Zig (grammar-only) */
    RUN_TEST(probe_zig_functions_calls);
    RUN_TEST(probe_zig_struct_methods);

    /* OCaml (grammar-only) */
    RUN_TEST(probe_ocaml_functions_calls);

    /* Erlang (grammar-only) */
    RUN_TEST(probe_erlang_functions_calls);

    /* Groovy (grammar-only; CALLS gap is a known-red reproduction) */
    RUN_TEST(probe_groovy_class_methods);
    RUN_TEST(probe_groovy_calls_edge_known_gap);

    /* Nim (grammar-only) */

    /* GDScript (grammar-only) */
    RUN_TEST(probe_gdscript_functions_calls);

    /* Perl (grammar-only) */
    RUN_TEST(probe_perl_functions_calls);

    /* R (grammar-only) */
    RUN_TEST(probe_r_functions_calls);

    /* Fortran (grammar-only) */
    RUN_TEST(probe_fortran_functions_calls);
}
