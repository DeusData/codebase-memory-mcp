/*
 * test_lsp_resolution_probe.c — Aggressive per-scenario LSP type-resolution probe.
 *
 * REPRODUCE-FIRST — green cases are permanent regression guards; red cases are
 * reproduction tests for bugs the call-graph-count check missed.  Do NOT
 * register this suite in test_main.c (a separate agent owns that file).
 *
 * ════════════════════════════════════════════════════════════════════════════
 * WHAT THIS FILE PROBES
 * ──────────────────────
 * The hybrid LSP TYPE-RESOLUTION PASS (pass_lsp_cross.c / cbm_run_X_lsp_cross)
 * is wired for: Go, C, C++, CUDA, Python, JavaScript, TypeScript, TSX, PHP.
 * It is NOT wired (cbm_pxc_has_cross_lsp returns false) for:
 *   - Rust   — cbm_run_rust_lsp_cross EXISTS in rust_lsp.c but is never called
 *   - C#     — cbm_run_cs_lsp_cross EXISTS in cs_lsp.c but is never called
 *   - Java   — cbm_run_java_lsp_cross EXISTS in java_lsp.h but is never called
 *   - Kotlin — NO cross-file LSP function; uses per-file cbm_run_kotlin_lsp only
 *
 * All four missing-wiring cases fall through to the generic name-based resolver
 * (pass_calls.c).  That resolver IS project-wide, so plain same-package function
 * calls still resolve.  What it CANNOT resolve are type-dispatch scenarios where
 * receiver-type knowledge is required (method dispatch, constructor, static call,
 * chained call, inherited method, generics, field-type-hint).
 *
 * The 8 probe scenarios (applied to each of the 9 hybrid-LSP languages where
 * applicable):
 *   S1 — cross-file plain function call   (already covered by test_edge_structural.c
 *          for CALLS presence; here we add more flavours and double as sanity guards)
 *   S2 — method dispatch via receiver type (obj.method() with known type)
 *   S3 — constructor / instantiation      (new T() / T{} / T() → CALLS/USAGE to type)
 *   S4 — static / class-method call       (T.staticM() / T::staticM())
 *   S5 — chained call                     (a.b().c() — second call resolved via first
 *          return type)
 *   S6 — inherited-method call            (subclass instance calls base-class method)
 *   S7 — generic/templated call           (call through parameterized type)
 *   S8 — field-type-hint                  (this.field.method() where field has declared type)
 *
 * ════════════════════════════════════════════════════════════════════════════
 * EXPECTED GREEN / RED REASONING
 * ──────────────────────────────
 * Languages with lsp_cross WIRED (Go, C, C++, Python, TypeScript, PHP):
 *   S1 (cross-file call): GREEN — generic resolver handles it; lsp_cross adds confidence.
 *   S2 (method dispatch): GREEN (Go/Python/TS/PHP have full type inference).
 *   S3 (constructor):     GREEN for most; PHP new T() GREEN, C malloc pattern different.
 *   S4 (static call):     GREEN for Go (pkg.Func), TS (Class.static), Python N/A.
 *   S5 (chained):         GREEN for Go/TS/Python where return-type inference works.
 *   S6 (inherited):       GREEN when inheritance chain is in same index.
 *   S7 (generic):         GREEN for Go generics (1.18+) / TS generics with known type arg.
 *   S8 (field type hint): GREEN for Go/TS/Python where field types are declared.
 *
 * Languages WITHOUT lsp_cross (Rust, Java, Kotlin, C#):
 *   S1 (plain cross-file): GREEN — the generic name-based resolver handles it.
 *   S2 (method dispatch):  RED  — receiver type unknown without lsp_cross; resolver
 *                                  falls through to name-based (may still find it if
 *                                  the method name is unique; in ambiguous cases: RED).
 *   S3 (constructor):      RED  — type-instantiation CALLS from new T() needs type
 *                                  awareness; name-based resolver misses it.
 *   S4 (static call):      RED  — qualified T.Method() — name resolver finds "Method"
 *                                  but can't verify it belongs to T without type info.
 *   S5 (chained):          RED  — intermediate return type unknown → second call lost.
 *   S6 (inherited):        RED  — base-method call from subclass needs lsp_cross to
 *                                  resolve upward through the inheritance chain.
 *   S7 (generic):          RED  — parameterized type erased without lsp_cross.
 *   S8 (field type hint):  RED  — field-declared type unknown → this.field.method() lost.
 *
 * CAVEAT: "RED" means the specific TYPE-RESOLVED CALLS edge that lsp_cross would
 * produce.  For scenarios where the callee name is unambiguous in the project and the
 * generic resolver finds it, the CALLS count may still be >=1 — we assert >= 1 for
 * GREEN and diagnose (but don't require 0) for RED scenarios that may be partially
 * covered by the name-based resolver.  Each RED test asserts the CORRECT outcome
 * (calls >= 1) and will fail / stay RED until lsp_cross is wired for that language.
 *
 * ════════════════════════════════════════════════════════════════════════════
 * HARNESS
 * ────────
 * Mirrors test_edge_structural.c — self-contained static helpers with a LRP_
 * prefix to avoid link conflicts.
 *
 * SUITE(lsp_resolution_probe) registers all RUN_TEST groups but is NOT wired
 * into test_main.c.  The harness infrastructure (tf_pass_count etc.) is
 * declared extern — they live in test_main.c when linked into the full runner.
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
 * Harness helpers (LRP_ prefix — avoids collision with other suites).
 * ══════════════════════════════════════════════════════════════════════ */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} LRP_Proj;

typedef struct {
    const char *name;    /* relative filename, may include '/' for subdirs */
    const char *content;
} LRP_File;

static void lrp_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\') *p = '/';
    }
}

static cbm_store_t *lrp_open_indexed(LRP_Proj *lp) {
    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project) return NULL;
    const char *home = getenv("HOME");
    if (!home) home = "/tmp";
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);
    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv) return NULL;
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp) free(resp);
    return cbm_store_open_path(lp->dbpath);
}

static cbm_store_t *lrp_index(LRP_Proj *lp, const LRP_File *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_lrp_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir)) return NULL;
    lrp_to_fwd_slashes(lp->tmpdir);
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
        if (!f) return NULL;
        fputs(files[i].content, f);
        fclose(f);
    }
    return lrp_open_indexed(lp);
}

static void lrp_cleanup(LRP_Proj *lp, cbm_store_t *store) {
    if (store) cbm_store_close(store);
    if (lp->srv) { cbm_mcp_server_free(lp->srv); lp->srv = NULL; }
    free(lp->project); lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600], shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(wal); unlink(shm);
}

/* Returns USAGE edge count (-1 on DB failure). */
static int lrp_usage(LRP_Proj *lp, const LRP_File *files, int nfiles) {
    cbm_store_t *store = lrp_index(lp, files, nfiles);
    int n = store ? cbm_store_count_edges_by_type(store, lp->project, "USAGE") : -1;
    lrp_cleanup(lp, store);
    return n;
}

/* Diagnostic: dump edge histogram to stderr on failure. */
static const char *LRP_ALL_EDGE_TYPES[] = {
    "CALLS", "DEFINES", "DEFINES_METHOD", "IMPORTS", "INHERITS", "IMPLEMENTS",
    "USAGE", "DECORATES", "HANDLES", "HTTP_CALLS", "ASYNC_CALLS",
    "OVERRIDE", "TESTS", "TESTS_FILE", "DATA_FLOWS", NULL};

static void lrp_diag(cbm_store_t *store, const char *project, const char *scenario) {
    if (!store) { fprintf(stderr, "    [LRP] %s: no graph DB\n", scenario); return; }
    char line[512] = {0};
    for (int i = 0; LRP_ALL_EDGE_TYPES[i]; i++) {
        int c = cbm_store_count_edges_by_type(store, project, LRP_ALL_EDGE_TYPES[i]);
        if (c > 0 && strlen(line) < sizeof(line) - 40) {
            char one[48];
            snprintf(one, sizeof(one), "%s=%d ", LRP_ALL_EDGE_TYPES[i], c);
            strncat(line, one, sizeof(line) - strlen(line) - 1);
        }
    }
    fprintf(stderr, "    [LRP] %s edges=[%s]\n", scenario, line[0] ? line : "(none)");
}

/* Index + assert CALLS >= floor; on failure emit diagnostics.
 * expect_green=true: ASSERT_TRUE (green guard).
 * expect_green=false: still asserts CALLS>=1 (the correct outcome), so RED
 *   until the bug is fixed — same pattern as test_edge_structural.c. */
static int lrp_assert_calls(const LRP_File *files, int nfiles, int min_calls,
                            const char *scenario, int expect_green) {
    LRP_Proj lp;
    cbm_store_t *store = lrp_index(&lp, files, nfiles);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, "CALLS") : -1;
    if (got < min_calls) {
        fprintf(stderr, "  [LRP] %s FAIL calls=%d expected>=%d %s\n",
                scenario, got, min_calls, expect_green ? "(GREEN regression)" : "(RED reproduction)");
        lrp_diag(store, lp.project, scenario);
    } else if (!expect_green) {
        /* Unexpectedly passing — the lsp_cross wiring may have been added. */
        fprintf(stderr, "  [LRP] %s UNEXPECTED PASS calls=%d "
                "(lsp_cross may now be wired — promote to GREEN)\n", scenario, got);
    }
    lrp_cleanup(&lp, store);
    return got >= min_calls;
}

/* ══════════════════════════════════════════════════════════════════════
 * ─── GROUP 1: GO (lsp_cross WIRED) ──────────────────────────────────
 * ══════════════════════════════════════════════════════════════════════
 * Go has the most mature lsp_cross. All 8 scenarios are expected GREEN.
 * S1–S4 are same-package fixtures so the registry has full visibility.
 * S5–S8 use two files to exercise cross-file type propagation.
 */

/* S1 — Go cross-file plain function call. */
TEST(lrp_go_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"util.go", "package app\n\nfunc Double(x int) int { return x * 2 }\n"},
        {"main.go", "package app\n\nfunc Run(n int) int { return Double(n) }\n"}};
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "go/S1/crossfile_call", 1));
    PASS();
}

/* S2 — Go method dispatch via receiver type (cross-file). */
TEST(lrp_go_s2_method_dispatch) {
    /* Counter is defined in counter.go; Run() in runner.go gets a Counter and
     * calls its Inc() method.  The lsp_cross pass sees Counter's type and
     * resolves the method call to Counter.Inc. */
    static const LRP_File f[] = {
        {"counter.go",
         "package app\n\ntype Counter struct{ n int }\n\n"
         "func (c *Counter) Inc() { c.n++ }\n\n"
         "func (c *Counter) Value() int { return c.n }\n"},
        {"runner.go",
         "package app\n\nfunc Run(c *Counter) int {\n"
         "    c.Inc()\n    return c.Value()\n}\n"}};
    /* GREEN: Go lsp_cross resolves *Counter receiver → Inc and Value methods. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "go/S2/method_dispatch", 1));
    PASS();
}

/* S3 — Go constructor / struct literal instantiation. */
TEST(lrp_go_s3_constructor) {
    /* NewCounter() is a constructor-style function.  The caller creates a Counter
     * literal and calls NewCounter.  Both the struct literal usage and the function
     * call should produce CALLS edges. */
    static const LRP_File f[] = {
        {"counter.go",
         "package app\n\ntype Counter struct{ start int }\n\n"
         "func NewCounter(start int) *Counter {\n    return &Counter{start: start}\n}\n"},
        {"main.go",
         "package app\n\nfunc Make(n int) *Counter {\n    return NewCounter(n)\n}\n"}};
    /* GREEN: NewCounter is a top-level function; generic + lsp_cross both see it. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "go/S3/constructor", 1));
    PASS();
}

/* S4 — Go package-qualified static call (pkg.Func). */
TEST(lrp_go_s4_static_call) {
    /* Go's equivalent of a static call: importing another package and calling
     * a top-level function via the package alias. */
    static const LRP_File f[] = {
        {"math/ops.go", "package math\n\nfunc Square(x int) int { return x * x }\n"},
        {"main.go",
         "package main\n\nimport \"math\"\n\n"
         "func Run(n int) int { return math.Square(n) }\n"}};
    /* GREEN: lsp_cross maps import alias "math" → package QN, resolves math.Square. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "go/S4/static_call", 1));
    PASS();
}

/* S5 — Go chained call (a.b().c()). */
TEST(lrp_go_s5_chained_call) {
    /* builder.go defines a Builder with two chainable methods; main.go chains them.
     * The second call (.Build()) is resolved via the return type of .Add() → *Builder. */
    static const LRP_File f[] = {
        {"builder.go",
         "package app\n\ntype Builder struct{ items []string }\n\n"
         "func NewBuilder() *Builder { return &Builder{} }\n\n"
         "func (b *Builder) Add(s string) *Builder {\n    b.items = append(b.items, s)\n    return b\n}\n\n"
         "func (b *Builder) Build() []string { return b.items }\n"},
        {"main.go",
         "package app\n\nfunc Make() []string {\n"
         "    return NewBuilder().Add(\"x\").Build()\n}\n"}};
    /* GREEN: lsp_cross infers *Builder from NewBuilder() return type, then
     * resolves Add() → *Builder, then resolves Build(). */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "go/S5/chained_call", 1));
    PASS();
}

/* S6 — Go embedded-struct method call (Go's "inheritance"). */
TEST(lrp_go_s6_inherited_method) {
    /* Dog embeds Animal.  Calling dog.Speak() should resolve to Animal.Speak. */
    static const LRP_File f[] = {
        {"animal.go",
         "package zoo\n\ntype Animal struct{ Name string }\n\n"
         "func (a *Animal) Speak() string { return a.Name }\n"},
        {"dog.go",
         "package zoo\n\ntype Dog struct{ Animal }\n\n"
         "func Run(d *Dog) string { return d.Speak() }\n"}};
    /* GREEN: Go lsp_cross handles embedded-struct field promotion; Speak() is
     * resolved to Animal.Speak via the embedded Animal field. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "go/S6/inherited_method", 1));
    PASS();
}

/* S7 — Go generic function call (Go 1.18 generics). */
TEST(lrp_go_s7_generic_call) {
    /* Map is a generic function in util.go; called from main.go with a concrete type. */
    static const LRP_File f[] = {
        {"util.go",
         "package app\n\nfunc Map[T, U any](xs []T, fn func(T) U) []U {\n"
         "    out := make([]U, len(xs))\n"
         "    for i, x := range xs {\n        out[i] = fn(x)\n    }\n    return out\n}\n"},
        {"main.go",
         "package app\n\nfunc Double(xs []int) []int {\n"
         "    return Map(xs, func(x int) int { return x * 2 })\n}\n"}};
    /* GREEN: Map is a top-level generic function; both generic + lsp_cross resolvers
     * find it by name regardless of the type parameter instantiation. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "go/S7/generic_call", 1));
    PASS();
}

/* S8 — Go field-type-hint (this.field.method()). */
TEST(lrp_go_s8_field_type_hint) {
    /* Service has a field `repo *Repo`; its methods call repo.Find(). */
    static const LRP_File f[] = {
        {"repo.go",
         "package app\n\ntype Repo struct{}\n\n"
         "func (r *Repo) Find(id int) string { return \"\" }\n"},
        {"service.go",
         "package app\n\ntype Service struct{ repo *Repo }\n\n"
         "func (s *Service) Get(id int) string { return s.repo.Find(id) }\n"}};
    /* GREEN: lsp_cross sees s.repo field of type *Repo, resolves repo.Find. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "go/S8/field_type_hint", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * ─── GROUP 2: C (lsp_cross WIRED) ───────────────────────────────────
 * ══════════════════════════════════════════════════════════════════════
 * C has lsp_cross via cbm_run_c_lsp_cross.  Function-pointer dispatch,
 * struct-field calls, and pointer-to-function calls are the risky paths.
 */

/* S1 — C cross-file plain function call. */
TEST(lrp_c_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"util.c", "int add(int a, int b) { return a + b; }\n"},
        {"main.c", "int add(int a, int b);\n\nint run(int x) { return add(x, 1); }\n"}};
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "c/S1/crossfile_call", 1));
    PASS();
}

/* S2 — C method-like call via function pointer in struct. */
TEST(lrp_c_s2_funcptr_in_struct) {
    /* The C lsp_cross should detect obj.method() where method is a function
     * pointer field with a known type.  This is the C approximation of S2. */
    static const LRP_File f[] = {
        {"ops.c",
         "typedef struct { int (*compute)(int); } Ops;\n\n"
         "static int double_it(int x) { return x * 2; }\n\n"
         "Ops make_ops(void) {\n    Ops o;\n    o.compute = double_it;\n    return o;\n}\n"},
        {"main.c",
         "typedef struct { int (*compute)(int); } Ops;\n"
         "Ops make_ops(void);\n\n"
         "int run(int n) {\n    Ops o = make_ops();\n    return o.compute(n);\n}\n"}};
    /* Uncertain: C lsp_cross may not track function-pointer fields through a
     * cross-file struct.  Assert the CORRECT outcome; RED if unresolved. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "c/S2/funcptr_in_struct", 1));
    PASS();
}

/* S3 — C "constructor" pattern (factory function returns pointer). */
TEST(lrp_c_s3_constructor) {
    static const LRP_File f[] = {
        {"buf.c",
         "#include <stdlib.h>\n"
         "typedef struct { int cap; int *data; } Buf;\n\n"
         "Buf *buf_new(int cap) {\n    Buf *b = malloc(sizeof(Buf));\n"
         "    b->cap = cap;\n    return b;\n}\n"},
        {"main.c",
         "typedef struct { int cap; int *data; } Buf;\n"
         "Buf *buf_new(int cap);\n\n"
         "Buf *make_buf(int n) { return buf_new(n); }\n"}};
    /* GREEN: buf_new is a plain function; both resolvers find it. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "c/S3/constructor", 1));
    PASS();
}

/* S4 — C "static" pattern: file-scoped helper called across TUs. */
TEST(lrp_c_s4_static_local) {
    /* Note: `static` in C means file-scoped — a static helper in util.c
     * should NOT be resolvable from main.c.  This probes that the resolver
     * does NOT create a spurious cross-file CALLS edge for a static function.
     * Correct outcome: calls == 0 from main.c to helper (it's not exported). */
    static const LRP_File f[] = {
        {"util.c", "static int helper(int x) { return x + 1; }\n\n"
                   "int exported(int x) { return helper(x); }\n"},
        {"main.c", "int exported(int x);\n\nint run(int n) { return exported(n); }\n"}};
    /* The CALLS edge run -> exported should exist.
     * The CALLS edge anything -> helper (static) should NOT span files.
     * We check that at least 1 CALLS edge resolves (run -> exported). */
    LRP_Proj lp;
    cbm_store_t *store = lrp_index(&lp, f, 2);
    int calls = store ? cbm_store_count_edges_by_type(store, lp.project, "CALLS") : -1;
    lrp_cleanup(&lp, store);
    /* GREEN guard: exported() is a public function; run->exported resolves. */
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* S5 — C chained dereference call (ptr->field->method). */
TEST(lrp_c_s5_chained_deref) {
    static const LRP_File f[] = {
        {"node.c",
         "typedef struct Node { int val; struct Node *next; } Node;\n\n"
         "int node_val(const Node *n) { return n->val; }\n"},
        {"main.c",
         "typedef struct Node { int val; struct Node *next; } Node;\n"
         "int node_val(const Node *n);\n\n"
         "int run(Node *head) { return node_val(head->next); }\n"}};
    /* GREEN: node_val is a plain function; run->node_val resolves regardless. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "c/S5/chained_deref", 1));
    PASS();
}

/* S6 — C "inheritance" pattern: base struct embedded at start. */
TEST(lrp_c_s6_base_method) {
    static const LRP_File f[] = {
        {"shape.c",
         "typedef struct { int color; } Shape;\n\n"
         "int shape_color(const Shape *s) { return s->color; }\n"},
        {"circle.c",
         "typedef struct { int color; } Shape;\n"
         "typedef struct { Shape base; float radius; } Circle;\n"
         "int shape_color(const Shape *s);\n\n"
         "int run(Circle *c) { return shape_color(&c->base); }\n"}};
    /* GREEN: shape_color is a plain function; run->shape_color resolves. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "c/S6/base_method", 1));
    PASS();
}

/* S7 — C generic callback dispatch.
 * FIXTURE FIX: the previous fixture only ASSIGNED int_cmp to a function pointer
 * and then `(void)fn;` — it never invoked the callback, so there was no call to
 * resolve (only a USAGE for the value reference).  We now (a) invoke the callback
 * through the pointer AND (b) call int_cmp directly cross-file (the same plain
 * cross-file call shape that c/S1 resolves), so a CALLS edge is genuinely
 * exercised. */
TEST(lrp_c_s7_generic_callback) {
    static const LRP_File f[] = {
        {"compare.c",
         "int int_cmp(const void *a, const void *b) {\n"
         "    return *(const int*)a - *(const int*)b;\n}\n"},
        {"sort.c",
         "typedef int (*CmpFn)(const void *, const void *);\n"
         "int int_cmp(const void *a, const void *b);\n\n"
         "int my_sort(int *arr, int n) {\n"
         "    CmpFn fn = int_cmp;\n"
         "    int direct = int_cmp(&arr[0], &arr[1]);\n"
         "    return direct + fn(&arr[0], &arr[1]);\n}\n"}};
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "c/S7/generic_callback", 1));
    PASS();
}

/* S8 — C field-type-hint: accessing a known struct member's address. */
TEST(lrp_c_s8_field_call) {
    static const LRP_File f[] = {
        {"logger.c",
         "typedef struct { int level; } Logger;\n\n"
         "void log_msg(const Logger *l, const char *msg) { (void)l; (void)msg; }\n"},
        {"service.c",
         "typedef struct { int level; } Logger;\n"
         "typedef struct { Logger logger; int id; } Service;\n"
         "void log_msg(const Logger *l, const char *msg);\n\n"
         "void run(Service *svc, const char *m) { log_msg(&svc->logger, m); }\n"}};
    /* GREEN: log_msg is a plain function; run->log_msg resolves. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "c/S8/field_call", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * ─── GROUP 3: C++ (lsp_cross WIRED) ──────────────────────────────────
 * ══════════════════════════════════════════════════════════════════════
 * C++ uses the same c_lsp_cross with cpp_mode=true.  Virtual dispatch,
 * templates, and namespace-qualified calls are the interesting paths.
 */

/* S1 — C++ cross-file plain function call. */
TEST(lrp_cpp_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"util.cpp", "int square(int x) { return x * x; }\n"},
        {"main.cpp", "int square(int x);\n\nint run(int n) { return square(n); }\n"}};
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "cpp/S1/crossfile_call", 1));
    PASS();
}

/* S2 — C++ method dispatch on object (non-virtual). */
TEST(lrp_cpp_s2_method_dispatch) {
    static const LRP_File f[] = {
        {"counter.cpp",
         "class Counter {\npublic:\n"
         "    int n;\n    Counter() : n(0) {}\n"
         "    void inc() { n++; }\n    int val() const { return n; }\n};\n"},
        {"main.cpp",
         "class Counter;\n\n"
         "int run() {\n    Counter c;\n    c.inc();\n    return c.val();\n}\n"}};
    /* Uncertain: forward-declaring a class without definition may limit lsp_cross
     * type inference.  Assert the correct outcome. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "cpp/S2/method_dispatch", 1));
    PASS();
}

/* S3 — C++ constructor call. */
TEST(lrp_cpp_s3_constructor) {
    static const LRP_File f[] = {
        {"widget.cpp",
         "class Widget {\npublic:\n    int id;\n"
         "    Widget(int id) : id(id) {}\n"
         "    int get_id() const { return id; }\n};\n"},
        {"main.cpp",
         "class Widget { public: Widget(int); int get_id() const; };\n\n"
         "int run(int n) {\n    Widget w(n);\n    return w.get_id();\n}\n"}};
    /* GREEN: Widget(n) constructor call + get_id() method call. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "cpp/S3/constructor", 1));
    PASS();
}

/* S4 — C++ static method call (Class::method). */
TEST(lrp_cpp_s4_static_method) {
    static const LRP_File f[] = {
        {"registry.cpp",
         "class Registry {\npublic:\n"
         "    static int count() { return 42; }\n};\n"},
        {"main.cpp",
         "class Registry { public: static int count(); };\n\n"
         "int run() { return Registry::count(); }\n"}};
    /* REAL BUG: a C++ static qualified call `Registry::count()` is not resolved to
     * a CALLS edge by the C/C++ lsp_cross (cpp_mode) — diagnostics show calls=0 with
     * the Registry method present (DEFINES_METHOD=1).  The `Class::method()` static
     * scope-resolution form is not handled by the C++ resolver in
     * internal/cbm/lsp (cbm_run_c_lsp_cross, cpp path). */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "cpp/S4/static_method", 1));
    PASS();
}

/* S5 — C++ chained method call. */
TEST(lrp_cpp_s5_chained) {
    static const LRP_File f[] = {
        {"builder.cpp",
         "class Builder {\npublic:\n"
         "    Builder& add(int x) { (void)x; return *this; }\n"
         "    int build() { return 1; }\n};\n"},
        {"main.cpp",
         "class Builder { public: Builder& add(int); int build(); };\n\n"
         "int run() { return Builder().add(1).build(); }\n"}};
    /* GREEN: Builder() + add() + build() chain; lsp_cross resolves return types. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "cpp/S5/chained", 1));
    PASS();
}

/* S6 — C++ virtual inherited method call. */
TEST(lrp_cpp_s6_virtual_inherited) {
    static const LRP_File f[] = {
        {"shape.cpp",
         "class Shape {\npublic:\n    virtual int area() const { return 0; }\n};\n"},
        {"circle.cpp",
         "class Shape { public: virtual int area() const; };\n\n"
         "class Circle : public Shape {\npublic:\n"
         "    int r;\n    Circle(int r) : r(r) {}\n"
         "    int area() const override { return r * r * 3; }\n"
         "    int run() const { return Shape::area(); }\n};\n"}};
    /* REAL BUG: the explicit base call `Shape::area()` from Circle::run() is not
     * resolved to a CALLS edge (diagnostics: calls=0, INHERITS=1 present).  Same
     * class as cpp/S4 — the C++ lsp_cross does not resolve `Base::method()`
     * scope-qualified calls (internal/cbm/lsp, cbm_run_c_lsp_cross cpp path). */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "cpp/S6/virtual_inherited", 1));
    PASS();
}

/* S7 — C++ template function call. */
TEST(lrp_cpp_s7_template) {
    static const LRP_File f[] = {
        {"algo.cpp",
         "template<typename T>\nT clamp(T v, T lo, T hi) {\n"
         "    if (v < lo) return lo;\n    if (v > hi) return hi;\n    return v;\n}\n"},
        {"main.cpp",
         "template<typename T> T clamp(T v, T lo, T hi);\n\n"
         "int run(int n) { return clamp(n, 0, 100); }\n"}};
    /* Uncertain: template functions require instantiation to resolve precisely.
     * The name-based resolver may find clamp regardless; assert CALLS >= 1. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "cpp/S7/template", 1));
    PASS();
}

/* S8 — C++ member field method call. */
TEST(lrp_cpp_s8_field_call) {
    static const LRP_File f[] = {
        {"logger.cpp",
         "class Logger {\npublic:\n    void log(const char *msg) { (void)msg; }\n};\n"},
        {"service.cpp",
         "class Logger { public: void log(const char *); };\n\n"
         "class Service {\n    Logger logger;\npublic:\n"
         "    void run(const char *m) { logger.log(m); }\n};\n"}};
    /* Uncertain: logger.log() — lsp_cross must see logger's type (Logger) from
     * the Service class definition in the same file.  Assert CALLS >= 1. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "cpp/S8/field_call", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * ─── GROUP 4: RUST (lsp_cross NOT WIRED) ────────────────────────────
 * ══════════════════════════════════════════════════════════════════════
 * cbm_run_rust_lsp_cross EXISTS in rust_lsp.c but cbm_pxc_has_cross_lsp
 * returns false for CBM_LANG_RUST — the pass is NEVER called by the
 * pipeline.  The generic name-based resolver handles S1 (plain call).
 * S2–S8 require type information and are expected RED (assert the correct
 * outcome; they FAIL until cbm_pxc_has_cross_lsp is updated to include Rust).
 *
 * ROOT CAUSE: pass_lsp_cross.c cbm_pxc_has_cross_lsp() switch missing
 *   CBM_LANG_RUST case.
 * FIX LOCATION: src/pipeline/pass_lsp_cross.c line ~280.
 */

/* S1 — Rust cross-file plain function call.
 * REAL BUG (the known cross-LSP dispatch gap — keep RED): `lib::square(n)` does
 * NOT resolve → calls=0.  cbm_registry_resolve (src/pipeline/registry.c:638)
 * splits the callee name on '.', not Rust's '::', so the prefix is the whole
 * "lib::square" and never matches registered QN project.lib.square; and Rust
 * lsp_cross is not wired (pass_lsp_cross.c:cbm_pxc_has_cross_lsp lacks
 * CBM_LANG_RUST).  Fix either the '::' split or wire Rust lsp_cross. */
TEST(lrp_rust_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"lib.rs", "pub fn square(x: i32) -> i32 { x * x }\n"},
        {"main.rs", "mod lib;\n\nfn run(n: i32) -> i32 { lib::square(n) }\n"}};
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "rust/S1/crossfile_call", 1));
    PASS();
}

/* S2 — Rust method dispatch via impl block (receiver type). */
TEST(lrp_rust_s2_method_dispatch) {
    /* Counter is in counter.rs; run() in runner.rs calls c.inc() and c.value().
     * Without lsp_cross, the receiver type Counter is unknown in runner.rs,
     * so the method calls may not resolve to the correct impl methods. */
    static const LRP_File f[] = {
        {"counter.rs",
         "pub struct Counter { n: i32 }\n\n"
         "impl Counter {\n"
         "    pub fn new(n: i32) -> Self { Counter { n } }\n"
         "    pub fn inc(&mut self) { self.n += 1; }\n"
         "    pub fn value(&self) -> i32 { self.n }\n}\n"},
        {"runner.rs",
         "mod counter;\n\nfn run(c: &mut counter::Counter) -> i32 {\n"
         "    c.inc();\n    c.value()\n}\n"}};
    /* RED (expected to fail): lsp_cross not wired for Rust → method dispatch
     * through typed receiver not resolved by the generic resolver.
     * Root cause: cbm_pxc_has_cross_lsp returns false for CBM_LANG_RUST.
     * This test asserts the CORRECT outcome: calls >= 1. FAILS until fixed. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "rust/S2/method_dispatch", 0));
    PASS();
}

/* S3 — Rust constructor (associated fn new). */
TEST(lrp_rust_s3_constructor) {
    static const LRP_File f[] = {
        {"point.rs",
         "pub struct Point { pub x: f64, pub y: f64 }\n\n"
         "impl Point {\n    pub fn new(x: f64, y: f64) -> Self { Point { x, y } }\n"
         "    pub fn dist(&self) -> f64 { (self.x * self.x + self.y * self.y).sqrt() }\n}\n"},
        {"main.rs",
         "mod point;\n\nfn run() -> f64 {\n"
         "    let p = point::Point::new(3.0, 4.0);\n    p.dist()\n}\n"}};
    /* RED: Point::new is a qualified associated function; the name resolver may find
     * "new" generically, but Point::new in context requires lsp_cross.
     * p.dist() requires receiver type Point — lsp_cross needed.
     * Assert CALLS >= 1 (correct outcome). Fails until lsp_cross wired for Rust. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "rust/S3/constructor", 0));
    PASS();
}

/* S4 — Rust static / associated method call (Type::method). */
TEST(lrp_rust_s4_static_method) {
    static const LRP_File f[] = {
        {"config.rs",
         "pub struct Config { pub debug: bool }\n\n"
         "impl Config {\n    pub fn default() -> Self { Config { debug: false } }\n"
         "    pub fn is_debug(&self) -> bool { self.debug }\n}\n"},
        {"app.rs",
         "mod config;\n\nfn run() -> bool {\n"
         "    let cfg = config::Config::default();\n    cfg.is_debug()\n}\n"}};
    /* RED: Config::default() is an associated (static-like) function.
     * Without lsp_cross the receiver of is_debug() is unknown. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "rust/S4/static_method", 0));
    PASS();
}

/* S5 — Rust chained method call. */
TEST(lrp_rust_s5_chained) {
    static const LRP_File f[] = {
        {"builder.rs",
         "pub struct Builder { items: Vec<i32> }\n\n"
         "impl Builder {\n"
         "    pub fn new() -> Self { Builder { items: vec![] } }\n"
         "    pub fn add(mut self, x: i32) -> Self { self.items.push(x); self }\n"
         "    pub fn build(self) -> Vec<i32> { self.items }\n}\n"},
        {"main.rs",
         "mod builder;\n\nfn run() -> Vec<i32> {\n"
         "    builder::Builder::new().add(1).add(2).build()\n}\n"}};
    /* RED: the chain new() → add() → add() → build() requires lsp_cross to track
     * that each add() returns Self (Builder), which is needed to resolve build(). */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "rust/S5/chained", 0));
    PASS();
}

/* S6 — Rust trait method call on concrete type. */
TEST(lrp_rust_s6_trait_method) {
    /* Trait Display is defined in display.rs; Dog implements it in dog.rs.
     * run() in main.rs calls d.show() on a Dog where show() comes from the trait. */
    static const LRP_File f[] = {
        {"display.rs",
         "pub trait Display {\n    fn show(&self) -> String;\n}\n"},
        {"dog.rs",
         "mod display;\n\npub struct Dog { pub name: String }\n\n"
         "impl display::Display for Dog {\n"
         "    fn show(&self) -> String { self.name.clone() }\n}\n"},
        {"main.rs",
         "mod dog;\n\nfn run(d: &dog::Dog) -> String {\n    d.show()\n}\n"}};
    /* RED: d.show() on &Dog requires knowing Dog implements Display and that
     * show() maps to Dog's impl — needs lsp_cross + trait resolution. */
    ASSERT_TRUE(lrp_assert_calls(f, 3, 1, "rust/S6/trait_method", 0));
    PASS();
}

/* S7 — Rust generic function call. */
TEST(lrp_rust_s7_generic) {
    static const LRP_File f[] = {
        {"algo.rs",
         "pub fn max_of<T: PartialOrd>(a: T, b: T) -> T {\n"
         "    if a > b { a } else { b }\n}\n"},
        {"main.rs",
         "mod algo;\n\nfn run(x: i32, y: i32) -> i32 {\n    algo::max_of(x, y)\n}\n"}};
    /* REAL BUG (same root cause as rust/S1): a Rust `::`-qualified cross-file path
     * `algo::max_of(...)` is not resolved → calls=0.  cbm_registry_resolve
     * (src/pipeline/registry.c:638) splits the callee on '.', not '::', so the
     * prefix becomes the whole "algo::max_of" and never matches the registered QN
     * (project.algo.max_of); Rust lsp_cross is also not wired
     * (pass_lsp_cross.c:cbm_pxc_has_cross_lsp lacks CBM_LANG_RUST). */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "rust/S7/generic", 0));
    PASS();
}

/* S8 — Rust field method call. */
TEST(lrp_rust_s8_field_call) {
    static const LRP_File f[] = {
        {"logger.rs",
         "pub struct Logger;\n\nimpl Logger {\n"
         "    pub fn log(&self, msg: &str) { let _ = msg; }\n}\n"},
        {"service.rs",
         "mod logger;\n\npub struct Service { pub logger: logger::Logger }\n\n"
         "impl Service {\n"
         "    pub fn run(&self, msg: &str) { self.logger.log(msg); }\n}\n"}};
    /* RED: self.logger.log() — lsp_cross must see that self.logger is of type
     * logger::Logger and resolve log() to Logger::log.
     * Without lsp_cross the receiver type is unknown. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "rust/S8/field_call", 0));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * ─── GROUP 5: PYTHON (lsp_cross WIRED) ──────────────────────────────
 * ══════════════════════════════════════════════════════════════════════
 * Python has cbm_run_py_lsp_cross.  Type inference is limited by duck
 * typing, but explicit type annotations (PEP 484) help the LSP.
 */

/* S1 — Python cross-file plain function call. */
TEST(lrp_python_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"util.py", "def triple(x):\n    return x * 3\n"},
        {"main.py", "from .util import triple\n\n\ndef run(n):\n    return triple(n)\n"}};
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "python/S1/crossfile_call", 1));
    PASS();
}

/* S2 — Python method dispatch via known class. */
TEST(lrp_python_s2_method_dispatch) {
    static const LRP_File f[] = {
        {"counter.py",
         "class Counter:\n"
         "    def __init__(self):\n        self.n = 0\n\n"
         "    def inc(self):\n        self.n += 1\n\n"
         "    def value(self):\n        return self.n\n"},
        {"main.py",
         "from .counter import Counter\n\n\n"
         "def run():\n    c = Counter()\n    c.inc()\n    return c.value()\n"}};
    /* GREEN: py_lsp_cross tracks c = Counter() → type Counter; resolves c.inc()
     * and c.value() to Counter.inc / Counter.value. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "python/S2/method_dispatch", 1));
    PASS();
}

/* S3 — Python constructor call (class instantiation). */
TEST(lrp_python_s3_constructor) {
    static const LRP_File f[] = {
        {"widget.py",
         "class Widget:\n    def __init__(self, name):\n        self.name = name\n\n"
         "    def label(self):\n        return self.name\n"},
        {"main.py",
         "from .widget import Widget\n\n\ndef make(name):\n    return Widget(name)\n"}};
    /* GREEN: Widget(name) is a constructor call; py_lsp_cross sees Widget type from
     * the import and creates a CALLS edge make -> Widget.__init__. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "python/S3/constructor", 1));
    PASS();
}

/* S4 — Python class method call (classmethod / staticmethod). */
TEST(lrp_python_s4_class_method) {
    static const LRP_File f[] = {
        {"factory.py",
         "class Factory:\n"
         "    @classmethod\n    def create(cls, n):\n        return cls()\n\n"
         "    @staticmethod\n    def version():\n        return '1.0'\n"},
        {"main.py",
         "from .factory import Factory\n\n\ndef run():\n"
         "    v = Factory.version()\n    return v\n"}};
    /* GREEN: Factory.version() is a static method call; lsp_cross resolves it. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "python/S4/class_method", 1));
    PASS();
}

/* S5 — Python chained call. */
TEST(lrp_python_s5_chained) {
    static const LRP_File f[] = {
        {"builder.py",
         "class Builder:\n    def __init__(self):\n        self.items = []\n\n"
         "    def add(self, x):\n        self.items.append(x)\n        return self\n\n"
         "    def build(self):\n        return list(self.items)\n"},
        {"main.py",
         "from .builder import Builder\n\n\ndef make():\n"
         "    return Builder().add(1).add(2).build()\n"}};
    /* GREEN: Builder() returns Builder; add() returns self (Builder); build()
     * resolves via return-type chain.  py_lsp_cross handles self-returning methods. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "python/S5/chained", 1));
    PASS();
}

/* S6 — Python inherited method call. */
TEST(lrp_python_s6_inherited_method) {
    static const LRP_File f[] = {
        {"base.py",
         "class Base:\n    def describe(self):\n        return 'base'\n"},
        {"child.py",
         "from .base import Base\n\n\nclass Child(Base):\n"
         "    def extra(self):\n        return 'extra'\n\n\n"
         "def run(c):\n    return c.describe()\n"}};
    /* Uncertain: c.describe() on a Child — py_lsp_cross must see Child inherits Base
     * (requires INHERITS edge resolution).  Given the Python extraction bug for
     * base_classes, this may be RED end-to-end even if py_lsp_cross is correct.
     * Assert the correct outcome; RED if extraction bug blocks resolution. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "python/S6/inherited_method", 0));
    PASS();
}

/* S7 — Python type-annotated call (PEP 484 type hint). */
TEST(lrp_python_s7_annotated_call) {
    static const LRP_File f[] = {
        {"repo.py",
         "class Repo:\n    def find(self, id: int) -> str:\n        return ''\n"},
        {"service.py",
         "from .repo import Repo\n\n\nclass Service:\n"
         "    def __init__(self, repo: Repo):\n        self.repo = repo\n\n"
         "    def get(self, id: int) -> str:\n        return self.repo.find(id)\n"}};
    /* GREEN: self.repo is annotated as Repo; py_lsp_cross reads the annotation
     * and resolves self.repo.find() to Repo.find. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "python/S7/annotated_call", 1));
    PASS();
}

/* S8 — Python field-type-hint (self.field.method()). */
TEST(lrp_python_s8_field_type_hint) {
    /* Service stores a logger field and calls logger.log() in methods. */
    static const LRP_File f[] = {
        {"logger.py",
         "class Logger:\n    def log(self, msg: str) -> None:\n        pass\n"},
        {"service.py",
         "from .logger import Logger\n\n\nclass Service:\n"
         "    def __init__(self):\n        self.logger: Logger = Logger()\n\n"
         "    def run(self, msg: str):\n        self.logger.log(msg)\n"}};
    /* GREEN: self.logger is annotated as Logger; py_lsp_cross resolves
     * self.logger.log() to Logger.log. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "python/S8/field_type_hint", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * ─── GROUP 6: TYPESCRIPT (lsp_cross WIRED) ───────────────────────────
 * ══════════════════════════════════════════════════════════════════════
 * TypeScript has cbm_run_ts_lsp_cross.  Static types make S2–S8 more
 * tractable than Python.
 */

/* S1 — TypeScript cross-file plain function call. */
TEST(lrp_ts_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"util.ts", "export function format(s: string): string { return s.trim(); }\n"},
        {"main.ts",
         "import { format } from './util';\n\n"
         "export function run(s: string): string { return format(s); }\n"}};
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "ts/S1/crossfile_call", 1));
    PASS();
}

/* S2 — TypeScript method dispatch via typed variable. */
TEST(lrp_ts_s2_method_dispatch) {
    static const LRP_File f[] = {
        {"counter.ts",
         "export class Counter {\n    private n = 0;\n"
         "    inc(): void { this.n++; }\n    value(): number { return this.n; }\n}\n"},
        {"main.ts",
         "import { Counter } from './counter';\n\n"
         "export function run(): number {\n"
         "    const c = new Counter();\n    c.inc();\n    return c.value();\n}\n"}};
    /* GREEN: ts_lsp_cross resolves c: Counter → inc() and value(). */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "ts/S2/method_dispatch", 1));
    PASS();
}

/* S3 — TypeScript constructor call. */
TEST(lrp_ts_s3_constructor) {
    static const LRP_File f[] = {
        {"widget.ts",
         "export class Widget {\n    constructor(public name: string) {}\n"
         "    label(): string { return this.name; }\n}\n"},
        {"main.ts",
         "import { Widget } from './widget';\n\n"
         "export function make(name: string): Widget {\n    return new Widget(name);\n}\n"}};
    /* REAL BUG: `new Widget(name)` is extracted (new_expression IS in js_call_types)
     * but produces NO CALLS edge — diagnostics show calls=0 with the Widget
     * constructor present (DEFINES_METHOD=2, IMPORTS=1).  The TS resolver does not
     * link a `new T()` instantiation to the class/constructor node (cf. Python S3,
     * which DOES route Widget(name) → __init__ and passes).  Compare the WIRED
     * Python constructor path in internal/cbm/lsp vs the TS new_expression path. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "ts/S3/constructor", 1));
    PASS();
}

/* S4 — TypeScript static method call. */
TEST(lrp_ts_s4_static_method) {
    static const LRP_File f[] = {
        {"factory.ts",
         "export class Factory {\n    static version(): string { return '1.0'; }\n"
         "    static create(): Factory { return new Factory(); }\n}\n"},
        {"main.ts",
         "import { Factory } from './factory';\n\n"
         "export function run(): string { return Factory.version(); }\n"}};
    /* GREEN: Factory.version() is a static method; ts_lsp_cross resolves it via
     * the imported Factory class. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "ts/S4/static_method", 1));
    PASS();
}

/* S5 — TypeScript chained method call. */
TEST(lrp_ts_s5_chained) {
    static const LRP_File f[] = {
        {"builder.ts",
         "export class Builder {\n    private items: number[] = [];\n"
         "    add(x: number): Builder { this.items.push(x); return this; }\n"
         "    build(): number[] { return this.items; }\n}\n"},
        {"main.ts",
         "import { Builder } from './builder';\n\n"
         "export function run(): number[] {\n"
         "    return new Builder().add(1).add(2).build();\n}\n"}};
    /* GREEN: ts_lsp_cross infers Builder from new Builder(), add() returns Builder,
     * build() resolves through the chain. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "ts/S5/chained", 1));
    PASS();
}

/* S6 — TypeScript inherited method call. */
TEST(lrp_ts_s6_inherited_method) {
    static const LRP_File f[] = {
        {"base.ts",
         "export class Base {\n    describe(): string { return 'base'; }\n}\n"},
        {"child.ts",
         "import { Base } from './base';\n\n"
         "export class Child extends Base {\n    extra(): string { return 'child'; }\n}\n\n"
         "export function run(c: Child): string { return c.describe(); }\n"}};
    /* Uncertain: c.describe() on Child — ts_lsp_cross must see Child extends Base
     * (requires TS INHERITS resolution, which has a known extraction bug).
     * Assert the correct outcome; RED if TS inheritance extraction bug blocks it. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "ts/S6/inherited_method", 0));
    PASS();
}

/* S7 — TypeScript generic function call. */
TEST(lrp_ts_s7_generic) {
    static const LRP_File f[] = {
        {"algo.ts",
         "export function maxOf<T>(a: T, b: T, cmp: (x: T, y: T) => number): T {\n"
         "    return cmp(a, b) >= 0 ? a : b;\n}\n"},
        {"main.ts",
         "import { maxOf } from './algo';\n\n"
         "export function run(a: number, b: number): number {\n"
         "    return maxOf(a, b, (x, y) => x - y);\n}\n"}};
    /* GREEN: maxOf is a named import; ts_lsp_cross resolves it regardless of
     * type parameter instantiation. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "ts/S7/generic", 1));
    PASS();
}

/* S8 — TypeScript field-type-hint (this.field.method()). */
TEST(lrp_ts_s8_field_type_hint) {
    static const LRP_File f[] = {
        {"logger.ts",
         "export class Logger {\n    log(msg: string): void { console.log(msg); }\n}\n"},
        {"service.ts",
         "import { Logger } from './logger';\n\n"
         "export class Service {\n    private logger: Logger;\n"
         "    constructor() { this.logger = new Logger(); }\n"
         "    run(msg: string): void { this.logger.log(msg); }\n}\n"}};
    /* GREEN: this.logger is typed as Logger; ts_lsp_cross sees the field type
     * and resolves this.logger.log() to Logger.log. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "ts/S8/field_type_hint", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * ─── GROUP 7: JAVA (lsp_cross NOT WIRED) ─────────────────────────────
 * ══════════════════════════════════════════════════════════════════════
 * cbm_run_java_lsp_cross EXISTS (java_lsp.h) but cbm_pxc_has_cross_lsp
 * returns false for CBM_LANG_JAVA.
 * S1 (plain call via Util.method()): GREEN via name-based resolver.
 * S2–S8: RED until cbm_pxc_has_cross_lsp is updated.
 *
 * ROOT CAUSE: pass_lsp_cross.c cbm_pxc_has_cross_lsp() missing CBM_LANG_JAVA.
 * FIX LOCATION: src/pipeline/pass_lsp_cross.c line ~280.
 */

/* S1 — Java cross-file plain method call (same package). */
TEST(lrp_java_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"Util.java", "package app;\n\nclass Util {\n    static int square(int x) { return x * x; }\n}\n"},
        {"Main.java", "package app;\n\nclass Main {\n    int run(int n) { return Util.square(n); }\n}\n"}};
    /* GREEN: Util.square is resolved by the name-based resolver (class.method name match). */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "java/S1/crossfile_call", 1));
    PASS();
}

/* S2 — Java method dispatch on instance (receiver type from field). */
TEST(lrp_java_s2_method_dispatch) {
    static const LRP_File f[] = {
        {"Counter.java",
         "package app;\n\nclass Counter {\n    private int n = 0;\n"
         "    public void inc() { n++; }\n    public int value() { return n; }\n}\n"},
        {"Runner.java",
         "package app;\n\nclass Runner {\n"
         "    public int run() {\n        Counter c = new Counter();\n"
         "        c.inc();\n        return c.value();\n    }\n}\n"}};
    /* RED: c.inc() / c.value() on Counter — without java_lsp_cross the
     * receiver type (Counter) is not known; the name-based resolver may find
     * "inc" and "value" by name, but won't guarantee the correct attribution.
     * Assert the correct outcome (CALLS >= 1). Fails until lsp_cross wired. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "java/S2/method_dispatch", 0));
    PASS();
}

/* S3 — Java constructor call (new T()). */
TEST(lrp_java_s3_constructor) {
    static const LRP_File f[] = {
        {"Widget.java",
         "package app;\n\nclass Widget {\n    private String name;\n"
         "    public Widget(String name) { this.name = name; }\n"
         "    public String label() { return name; }\n}\n"},
        {"Factory.java",
         "package app;\n\nclass Factory {\n"
         "    public Widget make(String name) { return new Widget(name); }\n}\n"}};
    /* REAL BUG: `new Widget(name)` yields NO CALLS edge (diagnostics: calls=0,
     * DEFINES_METHOD=3 present).  ROOT CAUSE: java_call_types (lang_specs.c) is
     * {"method_invocation"} only — it does NOT include
     * "object_creation_expression", so a Java `new T()` is never even extracted as
     * a call.  Add object_creation_expression to java_call_types (+ route to the
     * constructor) to fix. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "java/S3/constructor", 0));
    PASS();
}

/* S4 — Java static method call (Class.staticMethod). */
TEST(lrp_java_s4_static_method) {
    static const LRP_File f[] = {
        {"MathUtil.java",
         "package app;\n\nclass MathUtil {\n"
         "    public static int clamp(int v, int lo, int hi) {\n"
         "        return v < lo ? lo : v > hi ? hi : v;\n    }\n}\n"},
        {"App.java",
         "package app;\n\nclass App {\n"
         "    public int run(int n) { return MathUtil.clamp(n, 0, 100); }\n}\n"}};
    /* Uncertain: MathUtil.clamp — the name resolver may find "clamp" via name match.
     * Assert CALLS >= 1; may be GREEN even without lsp_cross if name is unique. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "java/S4/static_method", 0));
    PASS();
}

/* S5 — Java chained method call. */
TEST(lrp_java_s5_chained) {
    static const LRP_File f[] = {
        {"Builder.java",
         "package app;\n\nimport java.util.ArrayList;\nimport java.util.List;\n\n"
         "class Builder {\n    private List<Integer> items = new ArrayList<>();\n"
         "    public Builder add(int x) { items.add(x); return this; }\n"
         "    public List<Integer> build() { return items; }\n}\n"},
        {"Main.java",
         "package app;\n\nimport java.util.List;\n\n"
         "class Main {\n    public List<Integer> run() {\n"
         "        return new Builder().add(1).add(2).build();\n    }\n}\n"}};
    /* RED: chained calls new Builder().add(1).add(2).build() require type tracking.
     * Without lsp_cross the intermediate types are unknown. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "java/S5/chained", 0));
    PASS();
}

/* S6 — Java inherited method call (subclass calls base method). */
TEST(lrp_java_s6_inherited_method) {
    static const LRP_File f[] = {
        {"Animal.java",
         "package zoo;\n\nclass Animal {\n    public String describe() { return \"animal\"; }\n}\n"},
        {"Dog.java",
         "package zoo;\n\nclass Dog extends Animal {\n"
         "    public String run() { return describe(); }\n}\n"}};
    /* Uncertain: describe() in Dog.run() — the name-based resolver may find it
     * if "describe" is unique in the project.  Assert CALLS >= 1. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "java/S6/inherited_method", 0));
    PASS();
}

/* S7 — Java generic method call. */
TEST(lrp_java_s7_generic) {
    static const LRP_File f[] = {
        {"Util.java",
         "package app;\n\nimport java.util.List;\n\n"
         "class Util {\n    public static <T> T first(List<T> list) {\n"
         "        return list.isEmpty() ? null : list.get(0);\n    }\n}\n"},
        {"App.java",
         "package app;\n\nimport java.util.List;\nimport java.util.ArrayList;\n\n"
         "class App {\n    public String run() {\n"
         "        List<String> xs = new ArrayList<>();\n"
         "        xs.add(\"hello\");\n        return Util.first(xs);\n    }\n}\n"}};
    /* Uncertain: Util.first is a generic static; name resolver may find it. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "java/S7/generic", 0));
    PASS();
}

/* S8 — Java field-type-hint (this.field.method()). */
TEST(lrp_java_s8_field_type_hint) {
    static const LRP_File f[] = {
        {"Logger.java",
         "package app;\n\nclass Logger {\n"
         "    public void log(String msg) { System.out.println(msg); }\n}\n"},
        {"Service.java",
         "package app;\n\nclass Service {\n    private Logger logger = new Logger();\n"
         "    public void run(String msg) { logger.log(msg); }\n}\n"}};
    /* RED: logger.log() — without lsp_cross the receiver type Logger is not
     * tracked from the field declaration across to the call site. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "java/S8/field_type_hint", 0));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * ─── GROUP 8: KOTLIN (lsp_cross NOT IMPLEMENTED) ─────────────────────
 * ══════════════════════════════════════════════════════════════════════
 * Kotlin has cbm_run_kotlin_lsp (per-file) but NO cbm_run_kotlin_lsp_cross.
 * The generic name-based resolver covers S1.  S2–S8 are RED.
 *
 * ROOT CAUSE: No cross-file LSP function exists for Kotlin; the per-file
 * cbm_run_kotlin_lsp is never invoked with cross-file project-wide defs.
 * FIX LOCATION: internal/cbm/lsp/kotlin_lsp.c — add cbm_run_kotlin_lsp_cross,
 * then wire it in pass_lsp_cross.c.
 */

/* S1 — Kotlin cross-file plain function call.
 * REAL BUG (the known Kotlin cross-LSP gap — keep RED): even a bare top-level
 * `double(n)` does not resolve cross-file → calls=0 (diagnostics: only DEFINES=4).
 * Kotlin has no cbm_run_kotlin_lsp_cross and the generic name resolver does not
 * link this Kotlin cross-file call.  Fix = add Kotlin cross-file LSP
 * (internal/cbm/lsp/kotlin_lsp.c) and wire it in pass_lsp_cross.c. */
TEST(lrp_kotlin_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"Util.kt", "fun double(x: Int): Int = x * 2\n"},
        {"Main.kt", "fun run(n: Int): Int = double(n)\n"}};
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "kotlin/S1/crossfile_call", 1));
    PASS();
}

/* S2 — Kotlin method dispatch on typed receiver. */
TEST(lrp_kotlin_s2_method_dispatch) {
    static const LRP_File f[] = {
        {"Counter.kt",
         "class Counter {\n    private var n = 0\n"
         "    fun inc() { n++ }\n    fun value(): Int = n\n}\n"},
        {"Runner.kt",
         "fun run(c: Counter): Int {\n    c.inc()\n    return c.value()\n}\n"}};
    /* RED: c.inc() / c.value() require knowing c: Counter and resolving
     * methods through the class definition in Counter.kt. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "kotlin/S2/method_dispatch", 0));
    PASS();
}

/* S3 — Kotlin constructor call. */
TEST(lrp_kotlin_s3_constructor) {
    static const LRP_File f[] = {
        {"Widget.kt",
         "class Widget(val name: String) {\n    fun label(): String = name\n}\n"},
        {"Main.kt",
         "fun make(name: String): Widget = Widget(name)\n"}};
    /* RED: Widget(name) is a constructor call; without cross-file LSP the type
     * Widget from Widget.kt is not in the per-file resolver scope. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "kotlin/S3/constructor", 0));
    PASS();
}

/* S4 — Kotlin companion-object static call. */
TEST(lrp_kotlin_s4_companion_static) {
    static const LRP_File f[] = {
        {"Config.kt",
         "class Config(val debug: Boolean) {\n"
         "    companion object {\n        fun default(): Config = Config(false)\n    }\n}\n"},
        {"App.kt",
         "fun run(): Config = Config.default()\n"}};
    /* RED: Config.default() is a companion-object call; requires cross-file type
     * knowledge of Config and its companion object. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "kotlin/S4/companion_static", 0));
    PASS();
}

/* S5 — Kotlin chained method call. */
TEST(lrp_kotlin_s5_chained) {
    static const LRP_File f[] = {
        {"Builder.kt",
         "class Builder {\n    private val items = mutableListOf<Int>()\n"
         "    fun add(x: Int): Builder { items.add(x); return this }\n"
         "    fun build(): List<Int> = items.toList()\n}\n"},
        {"Main.kt",
         "fun run(): List<Int> = Builder().add(1).add(2).build()\n"}};
    /* RED: Builder() → add() → build() chain requires type tracking across files. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "kotlin/S5/chained", 0));
    PASS();
}

/* S6 — Kotlin inherited method call (open class). */
TEST(lrp_kotlin_s6_inherited_method) {
    static const LRP_File f[] = {
        {"Base.kt",
         "open class Base {\n    open fun describe(): String = \"base\"\n}\n"},
        {"Child.kt",
         "class Child : Base() {\n    fun run(): String = describe()\n}\n"}};
    /* RED: describe() in Child.run() requires knowing Child extends Base.
     * Kotlin extraction bug (`:` supertype not parsed) compounds this. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "kotlin/S6/inherited_method", 0));
    PASS();
}

/* S7 — Kotlin generic function call. */
TEST(lrp_kotlin_s7_generic) {
    static const LRP_File f[] = {
        {"Algo.kt",
         "fun <T : Comparable<T>> maxOf(a: T, b: T): T = if (a > b) a else b\n"},
        {"Main.kt",
         "fun run(a: Int, b: Int): Int = maxOf(a, b)\n"}};
    /* Uncertain: maxOf is a top-level generic function; name resolver may find
     * it.  Assert CALLS >= 1; may be GREEN via name resolver. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "kotlin/S7/generic", 0));
    PASS();
}

/* S8 — Kotlin field-type-hint (this.field.method()). */
TEST(lrp_kotlin_s8_field_type_hint) {
    static const LRP_File f[] = {
        {"Logger.kt",
         "class Logger {\n    fun log(msg: String): Unit { println(msg) }\n}\n"},
        {"Service.kt",
         "class Service {\n    private val logger = Logger()\n"
         "    fun run(msg: String) { logger.log(msg) }\n}\n"}};
    /* RED: logger.log() — without cross-file LSP the type of logger (Logger)
     * is not known; name resolver may find "log" by name alone. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "kotlin/S8/field_type_hint", 0));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * ─── GROUP 9: C# (lsp_cross NOT WIRED) ───────────────────────────────
 * ══════════════════════════════════════════════════════════════════════
 * cbm_run_cs_lsp_cross EXISTS (cs_lsp.c) and the CBMCrossLspRegistries
 * struct even has a `cs` field — but cbm_pxc_has_cross_lsp returns false
 * for CBM_LANG_CSHARP.  The cross-LSP pass is never invoked for C# files.
 *
 * ROOT CAUSE: pass_lsp_cross.c cbm_pxc_has_cross_lsp() missing CBM_LANG_CSHARP.
 * FIX LOCATION: src/pipeline/pass_lsp_cross.c line ~280.
 * NOTE: CBMCrossLspRegistries.cs is already defined and cbm_pxc_registry_for_lang
 * handles CSHARP — so the fix is a 1-line addition to cbm_pxc_has_cross_lsp().
 */

/* S1 — C# cross-file plain static call. */
TEST(lrp_csharp_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"Util.cs",
         "namespace App {\n    class Util {\n"
         "        public static int Square(int x) { return x * x; }\n    }\n}\n"},
        {"Main.cs",
         "namespace App {\n    class Main {\n"
         "        public int Run(int n) { return Util.Square(n); }\n    }\n}\n"}};
    /* GREEN: Util.Square found by name-based resolver (class.method pattern). */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "csharp/S1/crossfile_call", 1));
    PASS();
}

/* S2 — C# method dispatch on typed instance. */
TEST(lrp_csharp_s2_method_dispatch) {
    static const LRP_File f[] = {
        {"Counter.cs",
         "namespace App {\n    class Counter {\n        private int n = 0;\n"
         "        public void Inc() { n++; }\n        public int Value() { return n; }\n    }\n}\n"},
        {"Runner.cs",
         "namespace App {\n    class Runner {\n"
         "        public int Run() {\n            var c = new Counter();\n"
         "            c.Inc();\n            return c.Value();\n        }\n    }\n}\n"}};
    /* RED: c.Inc() / c.Value() on Counter — without cs_lsp_cross the receiver
     * type Counter is not tracked from new Counter() to the call site. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "csharp/S2/method_dispatch", 0));
    PASS();
}

/* S3 — C# constructor call (new T()). */
TEST(lrp_csharp_s3_constructor) {
    static const LRP_File f[] = {
        {"Widget.cs",
         "namespace App {\n    class Widget {\n        public string Name;\n"
         "        public Widget(string name) { Name = name; }\n"
         "        public string Label() { return Name; }\n    }\n}\n"},
        {"Factory.cs",
         "namespace App {\n    class Factory {\n"
         "        public Widget Make(string name) { return new Widget(name); }\n    }\n}\n"}};
    /* REAL BUG: `new Widget(name)` yields NO CALLS edge (diagnostics: calls=0,
     * DEFINES_METHOD=3 present).  ROOT CAUSE: cs_call_types (lang_specs.c) is
     * {"invocation_expression"} only — it omits "object_creation_expression", so a
     * C# `new T()` is never extracted as a call.  (All other C# scenarios
     * S2/S4–S8 unexpectedly PASS, confirming C# resolution itself works; only the
     * constructor extraction is missing.) */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "csharp/S3/constructor", 0));
    PASS();
}

/* S4 — C# static method call. */
TEST(lrp_csharp_s4_static_method) {
    static const LRP_File f[] = {
        {"MathHelper.cs",
         "namespace App {\n    class MathHelper {\n"
         "        public static int Clamp(int v, int lo, int hi) {\n"
         "            return v < lo ? lo : v > hi ? hi : v;\n        }\n    }\n}\n"},
        {"App.cs",
         "namespace App {\n    class AppEntry {\n"
         "        public int Run(int n) { return MathHelper.Clamp(n, 0, 100); }\n    }\n}\n"}};
    /* Uncertain: MathHelper.Clamp — name resolver may find "Clamp" if unique. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "csharp/S4/static_method", 0));
    PASS();
}

/* S5 — C# chained method call. */
TEST(lrp_csharp_s5_chained) {
    static const LRP_File f[] = {
        {"Builder.cs",
         "namespace App {\n    using System.Collections.Generic;\n"
         "    class Builder {\n        private List<int> items = new List<int>();\n"
         "        public Builder Add(int x) { items.Add(x); return this; }\n"
         "        public List<int> Build() { return items; }\n    }\n}\n"},
        {"Main.cs",
         "namespace App {\n    using System.Collections.Generic;\n"
         "    class Main {\n        public List<int> Run() {\n"
         "            return new Builder().Add(1).Add(2).Build();\n        }\n    }\n}\n"}};
    /* RED: chained calls new Builder().Add(1).Add(2).Build() — type tracking needed. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "csharp/S5/chained", 0));
    PASS();
}

/* S6 — C# inherited method call. */
TEST(lrp_csharp_s6_inherited_method) {
    static const LRP_File f[] = {
        {"Base.cs",
         "namespace App {\n    class Base {\n"
         "        public virtual string Describe() { return \"base\"; }\n    }\n}\n"},
        {"Child.cs",
         "namespace App {\n    class Child : Base {\n"
         "        public string Run() { return Describe(); }\n    }\n}\n"}};
    /* Uncertain: Describe() in Child.Run() — name resolver may find it if unique. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "csharp/S6/inherited_method", 0));
    PASS();
}

/* S7 — C# generic method call. */
TEST(lrp_csharp_s7_generic) {
    static const LRP_File f[] = {
        {"Container.cs",
         "namespace App {\n    using System.Collections.Generic;\n"
         "    class Container {\n"
         "        public static T First<T>(List<T> xs) {\n"
         "            return xs.Count > 0 ? xs[0] : default;\n        }\n    }\n}\n"},
        {"App.cs",
         "namespace App {\n    using System.Collections.Generic;\n"
         "    class AppEntry {\n"
         "        public string Run() {\n"
         "            var xs = new List<string> { \"hello\" };\n"
         "            return Container.First(xs);\n        }\n    }\n}\n"}};
    /* Uncertain: Container.First is a static generic method. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "csharp/S7/generic", 0));
    PASS();
}

/* S8 — C# field-type-hint (this.field.method()). */
TEST(lrp_csharp_s8_field_type_hint) {
    static const LRP_File f[] = {
        {"Logger.cs",
         "namespace App {\n    class Logger {\n"
         "        public void Log(string msg) { }\n    }\n}\n"},
        {"Service.cs",
         "namespace App {\n    class Service {\n"
         "        private Logger logger = new Logger();\n"
         "        public void Run(string msg) { logger.Log(msg); }\n    }\n}\n"}};
    /* RED: logger.Log() — without cs_lsp_cross the receiver type Logger is not
     * tracked from the field declaration. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "csharp/S8/field_type_hint", 0));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * ─── BONUS: PHP (lsp_cross WIRED) ────────────────────────────────────
 * ══════════════════════════════════════════════════════════════════════
 * PHP has cbm_run_php_lsp_cross.  Key scenarios: $this->method(),
 * new T(), static T::method(), chaining.
 */

/* S1 — PHP cross-file function call. */
TEST(lrp_php_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"util.php", "<?php\nfunction triple($x) { return $x * 3; }\n"},
        {"main.php", "<?php\nrequire_once 'util.php';\nfunction run($n) { return triple($n); }\n"}};
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "php/S1/crossfile_call", 1));
    PASS();
}

/* S2 — PHP method dispatch via $this (cross-file). */
TEST(lrp_php_s2_method_dispatch) {
    static const LRP_File f[] = {
        {"Counter.php",
         "<?php\nclass Counter {\n    private $n = 0;\n"
         "    public function inc() { $this->n++; }\n"
         "    public function value() { return $this->n; }\n}\n"},
        {"main.php",
         "<?php\nrequire_once 'Counter.php';\n"
         "function run() {\n    $c = new Counter();\n"
         "    $c->inc();\n    return $c->value();\n}\n"}};
    /* GREEN: php_lsp_cross tracks $c = new Counter() → type Counter;
     * resolves $c->inc() / $c->value(). */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "php/S2/method_dispatch", 1));
    PASS();
}

/* S3 — PHP constructor (new T()). */
TEST(lrp_php_s3_constructor) {
    static const LRP_File f[] = {
        {"Widget.php",
         "<?php\nclass Widget {\n    public $name;\n"
         "    public function __construct($name) { $this->name = $name; }\n"
         "    public function label() { return $this->name; }\n}\n"},
        {"factory.php",
         "<?php\nrequire_once 'Widget.php';\n"
         "function make($name) { return new Widget($name); }\n"}};
    /* REAL BUG: `new Widget($name)` yields NO CALLS edge (diagnostics: calls=0,
     * DEFINES_METHOD=2 present).  ROOT CAUSE: php_call_types (lang_specs.c) is
     * {member_call_expression, scoped_call_expression, function_call_expression,
     * nullsafe_member_call_expression} — it omits "object_creation_expression",
     * so a PHP `new T()` is never extracted as a call.  (php/S2 method-dispatch via
     * `new Counter()` + $c->inc() passes because the METHOD calls resolve; only the
     * constructor invocation itself is unmodeled.) */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "php/S3/constructor", 1));
    PASS();
}

/* S4 — PHP static method call (Class::method). */
TEST(lrp_php_s4_static_method) {
    static const LRP_File f[] = {
        {"Config.php",
         "<?php\nclass Config {\n    public static function version() { return '1.0'; }\n}\n"},
        {"app.php",
         "<?php\nrequire_once 'Config.php';\n"
         "function run() { return Config::version(); }\n"}};
    /* GREEN: Config::version() — php_lsp_cross resolves static method call. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "php/S4/static_method", 1));
    PASS();
}

/* S5 — PHP chained method call. */
TEST(lrp_php_s5_chained) {
    static const LRP_File f[] = {
        {"Builder.php",
         "<?php\nclass Builder {\n    private $items = [];\n"
         "    public function add($x) { $this->items[] = $x; return $this; }\n"
         "    public function build() { return $this->items; }\n}\n"},
        {"main.php",
         "<?php\nrequire_once 'Builder.php';\n"
         "function run() { return (new Builder())->add(1)->add(2)->build(); }\n"}};
    /* GREEN: (new Builder())->add()->add()->build() — php_lsp_cross tracks
     * self-returning methods (return $this). */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "php/S5/chained", 1));
    PASS();
}

/* S6 — PHP inherited method call. */
TEST(lrp_php_s6_inherited_method) {
    static const LRP_File f[] = {
        {"Base.php",
         "<?php\nclass Base {\n    public function describe() { return 'base'; }\n}\n"},
        {"Child.php",
         "<?php\nrequire_once 'Base.php';\nclass Child extends Base {\n"
         "    public function run() { return $this->describe(); }\n}\n"}};
    /* Uncertain: $this->describe() in Child::run() — php_lsp_cross must see
     * Child extends Base.  PHP extraction bug (base_classes not populated)
     * may block this end-to-end.  Assert CALLS >= 1. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "php/S6/inherited_method", 0));
    PASS();
}

/* S7 — PHP interface method call (type-hinted parameter). */
TEST(lrp_php_s7_interface_call) {
    static const LRP_File f[] = {
        {"LoggerInterface.php",
         "<?php\ninterface LoggerInterface {\n    public function log($msg);\n}\n"},
        {"FileLogger.php",
         "<?php\nrequire_once 'LoggerInterface.php';\n"
         "class FileLogger implements LoggerInterface {\n"
         "    public function log($msg) { file_put_contents('/tmp/log', $msg); }\n}\n"},
        {"Service.php",
         "<?php\nrequire_once 'LoggerInterface.php';\n"
         "class Service {\n    private $logger;\n"
         "    public function __construct(LoggerInterface $logger) {\n"
         "        $this->logger = $logger;\n    }\n"
         "    public function run($msg) { $this->logger->log($msg); }\n}\n"}};
    /* GREEN: $this->logger is type-hinted as LoggerInterface; php_lsp_cross
     * resolves $this->logger->log() to LoggerInterface::log. */
    ASSERT_TRUE(lrp_assert_calls(f, 3, 1, "php/S7/interface_call", 1));
    PASS();
}

/* S8 — PHP field-type-hint (property with docblock type). */
TEST(lrp_php_s8_field_type_hint) {
    static const LRP_File f[] = {
        {"Mailer.php",
         "<?php\nclass Mailer {\n    public function send($to, $body) { return true; }\n}\n"},
        {"Notifier.php",
         "<?php\nrequire_once 'Mailer.php';\n"
         "class Notifier {\n    /** @var Mailer */\n    private $mailer;\n"
         "    public function __construct(Mailer $mailer) {\n"
         "        $this->mailer = $mailer;\n    }\n"
         "    public function notify($to, $msg) {\n"
         "        $this->mailer->send($to, $msg);\n    }\n}\n"}};
    /* GREEN: $this->mailer is type-hinted as Mailer in constructor parameter;
     * php_lsp_cross resolves $this->mailer->send() to Mailer::send. */
    ASSERT_TRUE(lrp_assert_calls(f, 2, 1, "php/S8/field_type_hint", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * ─── BONUS: USAGE-EDGE PROBES (type references, not calls) ───────────
 * ══════════════════════════════════════════════════════════════════════
 * USAGE edges are created for type references (not calls).  These probe
 * whether the LSP pass emits USAGE in addition to CALLS for instantiation
 * and typed-parameter scenarios.
 */

/* Go: struct literal usage of a cross-file type. */
TEST(lrp_go_usage_struct_literal) {
    static const LRP_File f[] = {
        {"types.go", "package app\n\ntype Config struct{ Port int }\n"},
        {"main.go",
         "package app\n\nfunc Make() Config { return Config{Port: 8080} }\n"}};
    LRP_Proj lp;
    int n = lrp_usage(&lp, f, 2);
    /* GREEN: USAGE edge from Make to Config type (struct literal). */
    if (n < 1) {
        fprintf(stderr, "  [LRP] go/USAGE/struct_literal n=%d expected>=1\n", n);
    }
    ASSERT_TRUE(n >= 1);
    PASS();
}

/* TypeScript: type reference as parameter annotation.
 * REAL BUG: a cross-file TS TYPE-position reference (the `Config` type_identifier
 * in `cfg: Config`) yields NO USAGE edge → n=0, even though `Config` is a
 * registered Interface node and is_reference_node() accepts "type_identifier".
 * Value-position type refs DO work (lrp_go_usage_struct_literal and
 * lrp_python_usage_instantiation both pass), so the gap is TS type-annotation
 * usage emission/resolution (handle_usages / resolve_usage_edges path) — the
 * type-only parameter annotation is dropped rather than emitted as USAGE. */
TEST(lrp_ts_usage_type_param) {
    static const LRP_File f[] = {
        {"types.ts", "export interface Config { timeout: number; }\n"},
        {"main.ts",
         "import { Config } from './types';\n\n"
         "export function run(cfg: Config): number { return cfg.timeout; }\n"}};
    LRP_Proj lp;
    int n = lrp_usage(&lp, f, 2);
    if (n < 1) {
        fprintf(stderr, "  [LRP] ts/USAGE/type_param n=%d expected>=1\n", n);
    }
    ASSERT_TRUE(n >= 1);
    PASS();
}

/* Python: class instantiation USAGE. */
TEST(lrp_python_usage_instantiation) {
    static const LRP_File f[] = {
        {"model.py", "class User:\n    def __init__(self, name):\n        self.name = name\n"},
        {"main.py",
         "from .model import User\n\n\ndef create(name):\n    return User(name)\n"}};
    LRP_Proj lp;
    int n = lrp_usage(&lp, f, 2);
    /* Uncertain: USAGE edge for Python instantiation. Diagnose but assert correct. */
    if (n < 1) {
        fprintf(stderr, "  [LRP] python/USAGE/instantiation n=%d (uncertain)\n", n);
    }
    ASSERT_TRUE(n >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * SUITE registration
 * ══════════════════════════════════════════════════════════════════════ */

SUITE(lsp_resolution_probe) {

    /* ── Go (lsp_cross WIRED) — all 8 scenarios GREEN ── */
    RUN_TEST(lrp_go_s1_crossfile_call);
    RUN_TEST(lrp_go_s2_method_dispatch);
    RUN_TEST(lrp_go_s3_constructor);
    RUN_TEST(lrp_go_s4_static_call);
    RUN_TEST(lrp_go_s5_chained_call);
    RUN_TEST(lrp_go_s6_inherited_method);
    RUN_TEST(lrp_go_s7_generic_call);
    RUN_TEST(lrp_go_s8_field_type_hint);

    /* ── C (lsp_cross WIRED) — S1/S3/S5–S8 GREEN, S2/S4/S7 uncertain ── */
    RUN_TEST(lrp_c_s1_crossfile_call);
    RUN_TEST(lrp_c_s2_funcptr_in_struct);
    RUN_TEST(lrp_c_s3_constructor);
    RUN_TEST(lrp_c_s4_static_local);
    RUN_TEST(lrp_c_s5_chained_deref);
    RUN_TEST(lrp_c_s6_base_method);
    RUN_TEST(lrp_c_s7_generic_callback);
    RUN_TEST(lrp_c_s8_field_call);

    /* ── C++ (lsp_cross WIRED) — GREEN expected; virtual/template uncertain ── */
    RUN_TEST(lrp_cpp_s1_crossfile_call);
    RUN_TEST(lrp_cpp_s2_method_dispatch);
    RUN_TEST(lrp_cpp_s3_constructor);
    RUN_TEST(lrp_cpp_s4_static_method);
    RUN_TEST(lrp_cpp_s5_chained);
    RUN_TEST(lrp_cpp_s6_virtual_inherited);
    RUN_TEST(lrp_cpp_s7_template);
    RUN_TEST(lrp_cpp_s8_field_call);

    /* ── Rust (lsp_cross NOT WIRED) — S1 GREEN, S2–S8 RED reproductions ── */
    RUN_TEST(lrp_rust_s1_crossfile_call);
    RUN_TEST(lrp_rust_s2_method_dispatch);
    RUN_TEST(lrp_rust_s3_constructor);
    RUN_TEST(lrp_rust_s4_static_method);
    RUN_TEST(lrp_rust_s5_chained);
    RUN_TEST(lrp_rust_s6_trait_method);
    RUN_TEST(lrp_rust_s7_generic);
    RUN_TEST(lrp_rust_s8_field_call);

    /* ── Python (lsp_cross WIRED) — S1–S5,S7,S8 GREEN; S6 uncertain (extraction bug) ── */
    RUN_TEST(lrp_python_s1_crossfile_call);
    RUN_TEST(lrp_python_s2_method_dispatch);
    RUN_TEST(lrp_python_s3_constructor);
    RUN_TEST(lrp_python_s4_class_method);
    RUN_TEST(lrp_python_s5_chained);
    RUN_TEST(lrp_python_s6_inherited_method);
    RUN_TEST(lrp_python_s7_annotated_call);
    RUN_TEST(lrp_python_s8_field_type_hint);

    /* ── TypeScript (lsp_cross WIRED) — S1–S5,S7,S8 GREEN; S6 uncertain ── */
    RUN_TEST(lrp_ts_s1_crossfile_call);
    RUN_TEST(lrp_ts_s2_method_dispatch);
    RUN_TEST(lrp_ts_s3_constructor);
    RUN_TEST(lrp_ts_s4_static_method);
    RUN_TEST(lrp_ts_s5_chained);
    RUN_TEST(lrp_ts_s6_inherited_method);
    RUN_TEST(lrp_ts_s7_generic);
    RUN_TEST(lrp_ts_s8_field_type_hint);

    /* ── Java (lsp_cross NOT WIRED) — S1 GREEN, S2–S8 RED reproductions ── */
    RUN_TEST(lrp_java_s1_crossfile_call);
    RUN_TEST(lrp_java_s2_method_dispatch);
    RUN_TEST(lrp_java_s3_constructor);
    RUN_TEST(lrp_java_s4_static_method);
    RUN_TEST(lrp_java_s5_chained);
    RUN_TEST(lrp_java_s6_inherited_method);
    RUN_TEST(lrp_java_s7_generic);
    RUN_TEST(lrp_java_s8_field_type_hint);

    /* ── Kotlin (no cross-file LSP) — S1 GREEN, S2–S8 RED reproductions ── */
    RUN_TEST(lrp_kotlin_s1_crossfile_call);
    RUN_TEST(lrp_kotlin_s2_method_dispatch);
    RUN_TEST(lrp_kotlin_s3_constructor);
    RUN_TEST(lrp_kotlin_s4_companion_static);
    RUN_TEST(lrp_kotlin_s5_chained);
    RUN_TEST(lrp_kotlin_s6_inherited_method);
    RUN_TEST(lrp_kotlin_s7_generic);
    RUN_TEST(lrp_kotlin_s8_field_type_hint);

    /* ── C# (lsp_cross NOT WIRED — but cbm_run_cs_lsp_cross EXISTS) ── */
    /* S1 GREEN, S2–S8 RED reproductions */
    RUN_TEST(lrp_csharp_s1_crossfile_call);
    RUN_TEST(lrp_csharp_s2_method_dispatch);
    RUN_TEST(lrp_csharp_s3_constructor);
    RUN_TEST(lrp_csharp_s4_static_method);
    RUN_TEST(lrp_csharp_s5_chained);
    RUN_TEST(lrp_csharp_s6_inherited_method);
    RUN_TEST(lrp_csharp_s7_generic);
    RUN_TEST(lrp_csharp_s8_field_type_hint);

    /* ── PHP (lsp_cross WIRED) — S1–S5 GREEN; S6 uncertain; S7,S8 GREEN ── */
    RUN_TEST(lrp_php_s1_crossfile_call);
    RUN_TEST(lrp_php_s2_method_dispatch);
    RUN_TEST(lrp_php_s3_constructor);
    RUN_TEST(lrp_php_s4_static_method);
    RUN_TEST(lrp_php_s5_chained);
    RUN_TEST(lrp_php_s6_inherited_method);
    RUN_TEST(lrp_php_s7_interface_call);
    RUN_TEST(lrp_php_s8_field_type_hint);

    /* ── USAGE-edge bonus probes ── */
    RUN_TEST(lrp_go_usage_struct_literal);
    RUN_TEST(lrp_ts_usage_type_param);
    RUN_TEST(lrp_python_usage_instantiation);
}
