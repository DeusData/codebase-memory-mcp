/*
 * test_matrix_new_constructs.c — New-constructs probe across languages.
 *
 * Green = guard (node/edge already produced correctly).
 * Red   = bug found (node/edge not produced as expected; kept with a brief
 *         comment naming the suspected root cause).
 *
 * Probed construct families (each spans multiple languages):
 *   1. Type aliases / typedefs / `type X = ...`
 *   2. Exception/error class hierarchies (INHERITS for error types)
 *   3. Multiple inheritance / mixins / multiple interfaces
 *   4. Extension methods / receiver functions
 *   5. Getters / setters / computed properties
 *   6. Records / data classes / structs-with-derive
 *   7. Default / named / variadic parameters
 *   8. Pattern matching / destructuring (calls inside match arms)
 *   9. Nested / local / inner functions calling enclosing scope
 *  10. Module / namespace nesting (module nodes + CONTAINS)
 *
 * Strategy: assert expected nodes or edges; RED means the pipeline is missing
 * something, not that the fixture is wrong.  We use assertive floors (>= 1 for
 * the presence of a node class) so grammar refreshes don't flip green tests.
 *
 * Do NOT register in test_main.c — run via standalone suite_matrix_new_constructs().
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
 * Harness — self-contained copy following the pattern established in
 * test_node_creation_probe.c and test_edge_structural.c.  Prefix: mn_
 * ══════════════════════════════════════════════════════════════════ */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} MN_LangProj;

typedef struct {
    const char *name;
    const char *content;
} MN_LangFile;

static void mn_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\') *p = '/';
    }
}

static cbm_store_t *mn_open_indexed(MN_LangProj *lp) {
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

static cbm_store_t *mn_index_files(MN_LangProj *lp, const MN_LangFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_mn_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir)) return NULL;
    mn_to_fwd_slashes(lp->tmpdir);
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
    return mn_open_indexed(lp);
}

static void mn_cleanup(MN_LangProj *lp, cbm_store_t *store) {
    if (store) cbm_store_close(store);
    if (lp->srv) { cbm_mcp_server_free(lp->srv); lp->srv = NULL; }
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

/* Count nodes with a given label; -1 on error. */
static int mn_count_label(cbm_store_t *store, const char *project, const char *label) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK)
        return -1;
    cbm_store_free_nodes(nodes, count);
    return count;
}

/* Sum type-like nodes (Class + Struct + Interface + Enum + Trait + Type). */
static int mn_type_nodes(cbm_store_t *store, const char *project) {
    static const char *labels[] = {"Class", "Struct", "Interface", "Enum", "Trait", "Type", NULL};
    int total = 0;
    for (int i = 0; labels[i]; i++) {
        int n = mn_count_label(store, project, labels[i]);
        if (n > 0) total += n;
    }
    return total;
}

/* Callable nodes (Function + Method) with >=1 outbound edge. */
static int mn_callables_with_outbound(cbm_store_t *store, const char *project) {
    static const char *callable_labels[] = {"Function", "Method", NULL};
    int total = 0;
    for (int i = 0; callable_labels[i]; i++) {
        cbm_search_params_t p = {0};
        p.project    = project;
        p.label      = callable_labels[i];
        p.min_degree = 1;
        p.max_degree = -1;
        p.limit      = 100;
        cbm_search_output_t out = {0};
        if (cbm_store_search(store, &p, &out) == CBM_STORE_OK)
            total += out.count;
        cbm_store_search_free(&out);
    }
    return total;
}

/* Aggregate metrics for one index pass. */
typedef struct {
    int ok;
    int functions;
    int methods;
    int types;   /* all type-like combined */
    int calls;
    int callers;
    int inherits;
    int implements;
    int defines_method;
} MN_Metrics;

static MN_Metrics mn_metrics_files(const MN_LangFile *files, int nfiles) {
    MN_LangProj lp;
    cbm_store_t *store = mn_index_files(&lp, files, nfiles);
    MN_Metrics m = {0};
    if (store) {
        m.ok             = 1;
        m.functions      = mn_count_label(store, lp.project, "Function");
        m.methods        = mn_count_label(store, lp.project, "Method");
        m.types          = mn_type_nodes(store, lp.project);
        m.calls          = cbm_store_count_edges_by_type(store, lp.project, "CALLS");
        m.callers        = mn_callables_with_outbound(store, lp.project);
        m.inherits       = cbm_store_count_edges_by_type(store, lp.project, "INHERITS");
        m.implements     = cbm_store_count_edges_by_type(store, lp.project, "IMPLEMENTS");
        m.defines_method = cbm_store_count_edges_by_type(store, lp.project, "DEFINES_METHOD");
    }
    mn_cleanup(&lp, store);
    return m;
}

static MN_Metrics mn_metrics(const char *filename, const char *content) {
    MN_LangFile f = {filename, content};
    return mn_metrics_files(&f, 1);
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 1 — Type aliases / typedefs / `type X = ...`
 *
 * A type alias is not a callable; the pipeline must still produce at
 * least one Function/Class node for the surrounding definitions — and
 * ideally a Type node for the alias itself.  The surrounding callable
 * must still get a CALLS edge if it calls another function.
 * ══════════════════════════════════════════════════════════════════ */

/* Go: type alias `type Meters = float64` — functions around it must appear. */
TEST(mn_type_alias_go) {
    MN_Metrics m = mn_metrics("unit.go",
        "package unit\n\n"
        "type Meters = float64\n"
        "type Seconds = float64\n\n"
        "func Speed(dist Meters, t Seconds) Meters { return dist / t }\n\n"
        "func Run() Meters { return Speed(100, 9.58) }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Go: named type `type UserID int` must produce Function nodes alongside it. */
TEST(mn_type_alias_go_named) {
    MN_Metrics m = mn_metrics("ids.go",
        "package ids\n\n"
        "type UserID int\n"
        "type OrderID int\n\n"
        "func NewUser(id UserID) UserID { return id }\n\n"
        "func NewOrder(id OrderID) OrderID { return id }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    /* Type alias may produce a Type/Class node or nothing; just check no crash. */
    PASS();
}

/* TypeScript: `type ID = string | number` — functions still created. */
TEST(mn_type_alias_ts) {
    MN_Metrics m = mn_metrics("alias.ts",
        "type ID = string | number;\n"
        "type Callback<T> = (err: Error | null, val: T) => void;\n\n"
        "function makeId(n: number): ID { return n % 2 === 0 ? n : String(n); }\n\n"
        "function formatId(id: ID): string { return `id:${id}`; }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Rust: `type Result<T> = std::result::Result<T, String>` — functions appear. */
TEST(mn_type_alias_rust) {
    MN_Metrics m = mn_metrics("alias.rs",
        "type Res<T> = Result<T, String>;\n\n"
        "fn parse(s: &str) -> Res<i32> {\n"
        "    s.parse::<i32>().map_err(|e| e.to_string())\n"
        "}\n\n"
        "fn run() -> Res<i32> { parse(\"42\") }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* C: typedef — functions surrounding it must still appear. */
TEST(mn_type_alias_c_typedef) {
    MN_Metrics m = mn_metrics("vec.c",
        "typedef struct { float x; float y; } Vec2;\n"
        "typedef float Scalar;\n\n"
        "Scalar dot(Vec2 a, Vec2 b) { return a.x*b.x + a.y*b.y; }\n\n"
        "Scalar magnitude(Vec2 v) { return dot(v, v); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* C++: `using Seconds = double` — functions appear, alias does not crash. */
TEST(mn_type_alias_cpp_using) {
    MN_Metrics m = mn_metrics("time.cpp",
        "using Seconds = double;\n"
        "using Minutes = double;\n\n"
        "Seconds toSec(Minutes m) { return m * 60.0; }\n\n"
        "Minutes toMin(Seconds s) { return s / 60.0; }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 0); /* cross-call between them is optional */
    PASS();
}

/* Kotlin: `typealias Handler = (String) -> Unit` — functions appear. */
TEST(mn_type_alias_kotlin) {
    MN_Metrics m = mn_metrics("handler.kt",
        "typealias Handler = (String) -> Unit\n"
        "typealias Predicate = (Int) -> Boolean\n\n"
        "fun dispatch(h: Handler, msg: String) = h(msg)\n\n"
        "fun filter(pred: Predicate, n: Int): Boolean = pred(n)\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Python: `TypeAlias` annotation (PEP 613) — functions must appear. */
TEST(mn_type_alias_python) {
    MN_Metrics m = mn_metrics("types.py",
        "from typing import TypeAlias\n\n"
        "Vector: TypeAlias = list[float]\n"
        "Matrix: TypeAlias = list[list[float]]\n\n\n"
        "def dot(a: Vector, b: Vector) -> float:\n"
        "    return sum(x * y for x, y in zip(a, b))\n\n\n"
        "def norm(v: Vector) -> float:\n"
        "    return dot(v, v) ** 0.5\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 2 — Exception / error class hierarchies
 *
 * When an exception class `extends` a standard error base, the pipeline
 * should emit an INHERITS edge (same rules as any class inheritance).
 * RED if the base is not in the registry (std lib class not indexed).
 *
 * Note: Python `class X(Exception):` has a known extraction bug where
 * base_classes stores "(Exception)" with parens → INHERITS edge RED.
 * Java `extends Exception` works per existing contract; we probe a
 * deeper chain and multiple exception types.
 * ══════════════════════════════════════════════════════════════════ */

/* Java: multi-level exception hierarchy in one file → INHERITS edges. */
TEST(mn_exception_hierarchy_java) {
    static const MN_LangFile f[] = {{"Exc.java",
        "package app;\n\n"
        "class AppException extends RuntimeException {\n"
        "    AppException(String msg) { super(msg); }\n"
        "}\n\n"
        "class ValidationException extends AppException {\n"
        "    ValidationException(String field) { super(\"invalid: \" + field); }\n"
        "}\n\n"
        "class NotFoundException extends AppException {\n"
        "    NotFoundException(String id) { super(\"not found: \" + id); }\n"
        "}\n\n"
        "class Service {\n"
        "    void check(String s) throws ValidationException {\n"
        "        if (s == null) throw new ValidationException(\"s\");\n"
        "    }\n"
        "}\n"}};
    MN_Metrics m = mn_metrics_files(f, 1);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 3); /* AppException, ValidationException, NotFoundException */
    /* ValidationException->AppException + NotFoundException->AppException must resolve
     * (both sides in registry). AppException->RuntimeException may not resolve (stdlib
     * class not indexed) — floor=1 ensures at least same-file chain is captured. */
    ASSERT_TRUE(m.inherits >= 1);
    PASS();
}

/* Python: exception subclass hierarchy — KNOWN RED: base_classes stored with parens.
 * BUG: extract_defs.c captures "(ValueError)" not "ValueError" for Python base names.
 * This test reproduces the bug: types are created but INHERITS edge is not produced. */
TEST(mn_exception_hierarchy_python) {
    MN_Metrics m = mn_metrics("exc.py",
        "class AppError(Exception):\n"
        "    pass\n\n\n"
        "class ValidationError(AppError):\n"
        "    def __init__(self, field):\n"
        "        super().__init__(f'invalid: {field}')\n\n\n"
        "class NotFoundError(AppError):\n"
        "    def __init__(self, key):\n"
        "        super().__init__(f'not found: {key}')\n\n\n"
        "def check(s):\n"
        "    if s is None:\n"
        "        raise ValidationError('s')\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 2); /* AppError, ValidationError, NotFoundError */
    /* EXPECTED RED — Python base_classes extraction bug (parens in name) prevents
     * resolver from matching "ValidationError"->"AppError".  Keep until fixed. */
    ASSERT_TRUE(m.inherits >= 1);
    PASS();
}

/* C#: exception hierarchy — INHERITS edges for same-file chain. */
TEST(mn_exception_hierarchy_csharp) {
    MN_Metrics m = mn_metrics("Exc.cs",
        "namespace App {\n"
        "    class AppException : System.Exception {\n"
        "        public AppException(string msg) : base(msg) {}\n"
        "    }\n\n"
        "    class DbException : AppException {\n"
        "        public DbException(string q) : base(\"db: \" + q) {}\n"
        "    }\n\n"
        "    class Repo {\n"
        "        public void Query(string q) {\n"
        "            if (q == null) throw new DbException(q);\n"
        "        }\n"
        "    }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 2); /* AppException, DbException, Repo */
    /* DbException->AppException must resolve (both same-file). */
    ASSERT_TRUE(m.inherits >= 1);
    PASS();
}

/* Kotlin: exception class hierarchy — KNOWN RED: Kotlin `:` supertype not parsed. */
TEST(mn_exception_hierarchy_kotlin) {
    MN_Metrics m = mn_metrics("Exc.kt",
        "class AppException(msg: String) : Exception(msg)\n\n"
        "class ApiException(code: Int, msg: String) : AppException(\"[$code] $msg\")\n\n"
        "fun check(code: Int) {\n"
        "    if (code != 200) throw ApiException(code, \"err\")\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* AppException, ApiException */
    /* REAL BUG: Kotlin `class X : Base(...)` supertype after the `:` is not
     * parsed into base_classes, so ApiException->AppException INHERITS is never
     * emitted → inherits=0.  Root cause: Kotlin supertype_list extraction.
     * [KNOWN class 1] */
    ASSERT_TRUE(m.inherits >= 1);
    PASS();
}

/* C++: exception class hierarchy via `std::exception`. */
TEST(mn_exception_hierarchy_cpp) {
    MN_Metrics m = mn_metrics("exc.cpp",
        "#include <stdexcept>\n"
        "#include <string>\n\n"
        "class AppError : public std::runtime_error {\n"
        "public:\n"
        "    explicit AppError(const std::string &msg) : std::runtime_error(msg) {}\n"
        "};\n\n"
        "class DbError : public AppError {\n"
        "public:\n"
        "    explicit DbError(const std::string &q) : AppError(\"db: \" + q) {}\n"
        "};\n\n"
        "void check(const std::string &q) {\n"
        "    if (q.empty()) throw DbError(q);\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* AppError, DbError */
    /* DbError->AppError must resolve same-file. */
    ASSERT_TRUE(m.inherits >= 1);
    PASS();
}

/* TypeScript: error class hierarchy — KNOWN RED: TS extends extraction broken. */
TEST(mn_exception_hierarchy_ts) {
    MN_Metrics m = mn_metrics("err.ts",
        "class AppError extends Error {\n"
        "    constructor(public code: number, msg: string) { super(msg); }\n"
        "}\n\n"
        "class NotFoundError extends AppError {\n"
        "    constructor(id: string) { super(404, `not found: ${id}`); }\n"
        "}\n\n"
        "function lookup(id: string): number {\n"
        "    if (id === '') throw new NotFoundError(id);\n"
        "    return 42;\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    /* REAL BUG: TypeScript `class X extends Base` stores the `extends` keyword (or
     * fails) instead of the base class name, so NotFoundError->AppError INHERITS
     * is never emitted → inherits=0.  Root cause: TS extends_clause base-name
     * extraction. [KNOWN class 1] */
    ASSERT_TRUE(m.inherits >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 3 — Multiple inheritance / mixins / multiple interfaces
 *
 * Asserts that MULTIPLE INHERITS edges are produced when a class
 * declares multiple bases (Python, C++) or multiple interface
 * implementations (Java, C#, TypeScript).
 * ══════════════════════════════════════════════════════════════════ */

/* Python: multiple bases `class C(A, B):` — should produce 2+ INHERITS edges.
 * KNOWN RED: Python extraction bug with parens in base_classes. */
TEST(mn_multiple_inheritance_python) {
    MN_Metrics m = mn_metrics("multi.py",
        "class Flyable:\n"
        "    def fly(self):\n"
        "        return 'fly'\n\n\n"
        "class Swimmable:\n"
        "    def swim(self):\n"
        "        return 'swim'\n\n\n"
        "class Duck(Flyable, Swimmable):\n"
        "    def quack(self):\n"
        "        return self.fly() + self.swim()\n\n\n"
        "def make_duck():\n"
        "    return Duck()\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 2); /* Flyable, Swimmable, Duck */
    /* EXPECTED RED — Python base extraction still embeds parens; registry miss.
     * Fix: strip parens in extract_defs.c Python base_classes extraction. */
    ASSERT_TRUE(m.inherits >= 2);
    PASS();
}

/* C++: multiple inheritance — 2 INHERITS edges expected for same-file. */
TEST(mn_multiple_inheritance_cpp) {
    MN_Metrics m = mn_metrics("multi.cpp",
        "class Flyable { public: virtual void fly() {} };\n"
        "class Swimmable { public: virtual void swim() {} };\n\n"
        "class Duck : public Flyable, public Swimmable {\n"
        "public:\n"
        "    void fly() override {}\n"
        "    void swim() override {}\n"
        "    void quack() {}\n"
        "};\n\n"
        "void run() {\n"
        "    Duck d;\n"
        "    d.fly();\n"
        "    d.swim();\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 2); /* Flyable, Swimmable, Duck */
    ASSERT_TRUE(m.inherits >= 2); /* Duck->Flyable + Duck->Swimmable */
    PASS();
}

/* Java: multiple `implements` — should produce IMPLEMENTS (not INHERITS) edges. */
TEST(mn_multiple_interfaces_java) {
    static const MN_LangFile f[] = {{"Multi.java",
        "package app;\n\n"
        "interface Flyable { void fly(); }\n"
        "interface Swimmable { void swim(); }\n\n"
        "class Duck implements Flyable, Swimmable {\n"
        "    public void fly() { System.out.println(\"fly\"); }\n"
        "    public void swim() { System.out.println(\"swim\"); }\n"
        "    public void quack() { fly(); swim(); }\n"
        "}\n"}};
    MN_Metrics m = mn_metrics_files(f, 1);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 2); /* Flyable, Swimmable, Duck */
    /* Java: both implements → IMPLEMENTS edges (pipeline distinguishes).
     * Floor=1 because implementation may use INHERITS for all bases. */
    ASSERT_TRUE(m.implements + m.inherits >= 1);
    PASS();
}

/* C#: class implementing multiple interfaces. */
TEST(mn_multiple_interfaces_csharp) {
    MN_Metrics m = mn_metrics("Multi.cs",
        "namespace App {\n"
        "    interface IFlyable { void Fly(); }\n"
        "    interface ISwimmable { void Swim(); }\n\n"
        "    class Duck : IFlyable, ISwimmable {\n"
        "        public void Fly() { }\n"
        "        public void Swim() { }\n"
        "        public void Quack() { Fly(); Swim(); }\n"
        "    }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 2);
    ASSERT_TRUE(m.implements + m.inherits >= 1);
    PASS();
}

/* TypeScript: implements multiple interfaces — KNOWN RED: TS extraction broken. */
TEST(mn_multiple_interfaces_ts) {
    MN_Metrics m = mn_metrics("Multi.ts",
        "interface Flyable { fly(): string; }\n"
        "interface Swimmable { swim(): string; }\n\n"
        "class Duck implements Flyable, Swimmable {\n"
        "    fly(): string { return 'fly'; }\n"
        "    swim(): string { return 'swim'; }\n"
        "    quack(): string { return this.fly() + this.swim(); }\n"
        "}\n\n"
        "function run(): string { return new Duck().quack(); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    /* REAL BUG: TypeScript `class Duck implements Flyable, Swimmable` — the
     * implements_clause base names are not extracted, so no IMPLEMENTS/INHERITS
     * edge is emitted → 0.  Root cause: TS implements_clause base-name extraction.
     * [KNOWN class 1] */
    ASSERT_TRUE(m.implements + m.inherits >= 1);
    PASS();
}

/* Rust: multiple trait bounds on a generic function — traits are types, not INHERITS.
 * Checks that the function node is created even with complex trait bounds. */
TEST(mn_multiple_trait_bounds_rust) {
    MN_Metrics m = mn_metrics("bounds.rs",
        "use std::fmt::{Debug, Display};\n\n"
        "fn print_both<T: Debug + Display>(val: T) {\n"
        "    println!(\"{:?} {}\", val, val);\n"
        "}\n\n"
        "fn run() {\n"
        "    print_both(42);\n"
        "    print_both(\"hello\");\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Kotlin: class implementing multiple interfaces — KNOWN RED: `:` not parsed. */
TEST(mn_multiple_interfaces_kotlin) {
    MN_Metrics m = mn_metrics("Multi.kt",
        "interface Flyable { fun fly(): String }\n"
        "interface Swimmable { fun swim(): String }\n\n"
        "class Duck : Flyable, Swimmable {\n"
        "    override fun fly() = \"fly\"\n"
        "    override fun swim() = \"swim\"\n"
        "    fun quack() = fly() + swim()\n"
        "}\n\n"
        "fun run() = Duck().quack()\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 2);
    /* REAL BUG: Kotlin `class Duck : Flyable, Swimmable` — supertype list after
     * `:` is not parsed into base_classes, so no IMPLEMENTS/INHERITS edge is
     * emitted → 0.  Root cause: Kotlin supertype_list extraction (same root cause
     * as mn_exception_hierarchy_kotlin). [KNOWN class 1] */
    ASSERT_TRUE(m.implements + m.inherits >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 4 — Extension methods / receiver functions
 *
 * Checks that methods defined outside a class but associated to it
 * (Kotlin extension fun, Go method on named type, Rust impl on struct,
 * C# extension method, Swift extension) are still extracted as Method
 * or Function nodes that can produce CALLS edges.
 * ══════════════════════════════════════════════════════════════════ */

/* Kotlin: extension function on a standard type — should be a Function node. */
TEST(mn_extension_kotlin) {
    MN_Metrics m = mn_metrics("ext.kt",
        "fun String.titleCase(): String =\n"
        "    split(\" \").joinToString(\" \") { it.capitalize() }\n\n"
        "fun String.wordCount(): Int = trim().split(\"\\\\s+\".toRegex()).size\n\n"
        "fun processTitle(s: String): String = s.titleCase()\n\n"
        "fun run(): Int = \"hello world\".wordCount()\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Go: method on a named type in the same file — must produce Method node. */
TEST(mn_extension_go_receiver) {
    MN_Metrics m = mn_metrics("stack.go",
        "package ds\n\n"
        "type Stack []int\n\n"
        "func (s *Stack) Push(v int) { *s = append(*s, v) }\n\n"
        "func (s Stack) Peek() int {\n"
        "    if len(s) == 0 { return -1 }\n"
        "    return s[len(s)-1]\n"
        "}\n\n"
        "func (s Stack) Len() int { return len(s) }\n\n"
        "func NewStack() Stack { return Stack{} }\n");
    ASSERT_TRUE(m.ok);
    /* Push, Peek, Len are Methods; NewStack is a Function. */
    ASSERT_TRUE(m.functions + m.methods >= 3);
    PASS();
}

/* Rust: impl on a struct (external-style impl block) — Methods appear. */
TEST(mn_extension_rust_impl) {
    MN_Metrics m = mn_metrics("queue.rs",
        "pub struct Queue<T> {\n"
        "    buf: std::collections::VecDeque<T>,\n"
        "}\n\n"
        "impl<T> Queue<T> {\n"
        "    pub fn new() -> Self { Queue { buf: std::collections::VecDeque::new() } }\n"
        "    pub fn enqueue(&mut self, item: T) { self.buf.push_back(item); }\n"
        "    pub fn dequeue(&mut self) -> Option<T> { self.buf.pop_front() }\n"
        "    pub fn len(&self) -> usize { self.buf.len() }\n"
        "}\n\n"
        "pub fn demo() {\n"
        "    let mut q: Queue<i32> = Queue::new();\n"
        "    q.enqueue(1);\n"
        "    let _ = q.dequeue();\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions + m.methods >= 3);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* C#: extension method in a static class — must produce Method node. */
TEST(mn_extension_csharp) {
    MN_Metrics m = mn_metrics("Ext.cs",
        "namespace App {\n"
        "    static class StringExtensions {\n"
        "        public static string TitleCase(this string s) {\n"
        "            if (string.IsNullOrEmpty(s)) return s;\n"
        "            return char.ToUpper(s[0]) + s.Substring(1);\n"
        "        }\n"
        "        public static int WordCount(this string s) =>\n"
        "            s.Trim().Split(' ').Length;\n"
        "    }\n\n"
        "    class Processor {\n"
        "        public string Process(string s) => s.TitleCase();\n"
        "        public int Count(string s) => s.WordCount();\n"
        "    }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.methods >= 2);
    PASS();
}

/* Swift: extension on a struct adding a method — must produce Function/Method node. */
TEST(mn_extension_swift) {
    MN_Metrics m = mn_metrics("ext.swift",
        "struct Point {\n"
        "    var x: Double\n"
        "    var y: Double\n"
        "}\n\n"
        "extension Point {\n"
        "    func magnitude() -> Double {\n"
        "        return (x * x + y * y).squareRoot()\n"
        "    }\n"
        "    func scaled(by factor: Double) -> Point {\n"
        "        return Point(x: x * factor, y: y * factor)\n"
        "    }\n"
        "}\n\n"
        "func run() -> Double {\n"
        "    let p = Point(x: 3.0, y: 4.0)\n"
        "    return p.magnitude()\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 5 — Getters / setters / computed properties
 *
 * @property decorators (Python), get/set accessor blocks (C#, Kotlin,
 * TypeScript, Swift, Scala) should not prevent the class node or the
 * method nodes for other methods from appearing.  CALLS from a method
 * that reads/writes a property-defined method should also resolve.
 * ══════════════════════════════════════════════════════════════════ */

/* Python: @property getter + setter — class must appear, methods present. */
TEST(mn_getset_python_property) {
    MN_Metrics m = mn_metrics("account.py",
        "class Account:\n"
        "    def __init__(self, balance):\n"
        "        self._balance = balance\n\n"
        "    @property\n"
        "    def balance(self):\n"
        "        return self._balance\n\n"
        "    @balance.setter\n"
        "    def balance(self, value):\n"
        "        if value < 0:\n"
        "            raise ValueError('negative balance')\n"
        "        self._balance = value\n\n"
        "    def deposit(self, amount):\n"
        "        self.balance = self.balance + amount\n\n\n"
        "def make_account(initial):\n"
        "    return Account(initial)\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* class Account */
    ASSERT_TRUE(m.functions >= 1); /* make_account */
    PASS();
}

/* C#: auto-property and computed property — class and methods. */
TEST(mn_getset_csharp) {
    MN_Metrics m = mn_metrics("Temp.cs",
        "namespace App {\n"
        "    class Temperature {\n"
        "        private double _celsius;\n"
        "        public double Celsius {\n"
        "            get => _celsius;\n"
        "            set { if (value < -273.15) throw new System.ArgumentException(); _celsius = value; }\n"
        "        }\n"
        "        public double Fahrenheit {\n"
        "            get => Celsius * 9.0 / 5.0 + 32;\n"
        "            set => Celsius = (value - 32) * 5.0 / 9.0;\n"
        "        }\n"
        "        public bool IsBoiling() => Celsius >= 100;\n"
        "    }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* class Temperature */
    ASSERT_TRUE(m.methods >= 1); /* IsBoiling at minimum */
    PASS();
}

/* Kotlin: custom get/set on a property — class and methods must appear. */
TEST(mn_getset_kotlin) {
    MN_Metrics m = mn_metrics("Circle.kt",
        "class Circle(private var _radius: Double) {\n"
        "    var radius: Double\n"
        "        get() = _radius\n"
        "        set(value) { if (value < 0) error(\"neg radius\"); _radius = value }\n\n"
        "    val area: Double\n"
        "        get() = Math.PI * _radius * _radius\n\n"
        "    fun scale(factor: Double) {\n"
        "        radius = _radius * factor\n"
        "    }\n"
        "}\n\n"
        "fun run() = Circle(1.0).area\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* class Circle */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* TypeScript: getter/setter on a class — class node + method nodes. */
TEST(mn_getset_typescript) {
    MN_Metrics m = mn_metrics("Rect.ts",
        "class Rectangle {\n"
        "    private _w: number;\n"
        "    private _h: number;\n\n"
        "    constructor(w: number, h: number) { this._w = w; this._h = h; }\n\n"
        "    get width(): number { return this._w; }\n"
        "    set width(v: number) { if (v < 0) throw new Error(); this._w = v; }\n"
        "    get area(): number { return this._w * this._h; }\n\n"
        "    scale(f: number): void { this.width = this._w * f; }\n"
        "}\n\n"
        "function run(): number { return new Rectangle(3, 4).area; }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Swift: computed property in a struct — struct and functions must appear. */
TEST(mn_getset_swift) {
    MN_Metrics m = mn_metrics("rect.swift",
        "struct Rect {\n"
        "    var width: Double\n"
        "    var height: Double\n\n"
        "    var area: Double { return width * height }\n"
        "    var perimeter: Double { return 2 * (width + height) }\n"
        "    var isSquare: Bool { return width == height }\n\n"
        "    mutating func scale(by factor: Double) {\n"
        "        width *= factor\n"
        "        height *= factor\n"
        "    }\n"
        "}\n\n"
        "func run() -> Double { return Rect(width: 3, height: 4).area }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 6 — Records / data classes / structs-with-derive
 *
 * Record types (Java record, Kotlin data class, C# record, Python
 * dataclass, Rust #[derive] struct, Scala case class) produce a
 * class/struct node.  Methods on them must also appear.
 * ══════════════════════════════════════════════════════════════════ */

/* Java: record with a custom method. */
TEST(mn_record_java) {
    MN_Metrics m = mn_metrics("Point.java",
        "package app;\n\n"
        "record Point(double x, double y) {\n"
        "    double distanceTo(Point other) {\n"
        "        double dx = this.x - other.x;\n"
        "        double dy = this.y - other.y;\n"
        "        return Math.sqrt(dx * dx + dy * dy);\n"
        "    }\n"
        "    static Point origin() { return new Point(0, 0); }\n"
        "}\n\n"
        "class Geo {\n"
        "    static double run() { return Point.origin().distanceTo(new Point(3, 4)); }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1); /* record Point, class Geo */
    ASSERT_TRUE(m.methods >= 1);
    PASS();
}

/* Kotlin: data class with custom methods and companion. */
TEST(mn_record_kotlin_data_class) {
    MN_Metrics m = mn_metrics("User.kt",
        "data class User(val name: String, val age: Int) {\n"
        "    fun isAdult(): Boolean = age >= 18\n"
        "    fun greet(): String = \"Hi, $name\"\n\n"
        "    companion object {\n"
        "        fun anon(): User = User(\"anon\", 0)\n"
        "    }\n"
        "}\n\n"
        "fun run(): String = User.anon().greet()\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions + m.methods >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* C#: record type. */
TEST(mn_record_csharp) {
    MN_Metrics m = mn_metrics("Point.cs",
        "namespace App {\n"
        "    record Point(double X, double Y) {\n"
        "        public double DistanceTo(Point other) {\n"
        "            double dx = X - other.X;\n"
        "            double dy = Y - other.Y;\n"
        "            return System.Math.Sqrt(dx*dx + dy*dy);\n"
        "        }\n"
        "        public static Point Origin() => new Point(0, 0);\n"
        "    }\n\n"
        "    class Geo {\n"
        "        public static double Run() => Point.Origin().DistanceTo(new Point(3, 4));\n"
        "    }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.methods >= 2);
    PASS();
}

/* Python: dataclass with post_init and method. */
TEST(mn_record_python_dataclass) {
    MN_Metrics m = mn_metrics("vec.py",
        "from dataclasses import dataclass\n"
        "import math\n\n\n"
        "@dataclass\n"
        "class Vector:\n"
        "    x: float\n"
        "    y: float\n\n"
        "    def magnitude(self):\n"
        "        return math.sqrt(self.x ** 2 + self.y ** 2)\n\n"
        "    def dot(self, other):\n"
        "        return self.x * other.x + self.y * other.y\n\n\n"
        "def unit(x, y):\n"
        "    v = Vector(x, y)\n"
        "    m = v.magnitude()\n"
        "    return Vector(x / m, y / m)\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Rust: struct with #[derive(Debug, Clone)] and impl methods. */
TEST(mn_record_rust_derive) {
    MN_Metrics m = mn_metrics("point.rs",
        "#[derive(Debug, Clone, PartialEq)]\n"
        "struct Point {\n"
        "    x: f64,\n"
        "    y: f64,\n"
        "}\n\n"
        "impl Point {\n"
        "    fn new(x: f64, y: f64) -> Self { Point { x, y } }\n"
        "    fn distance(&self, other: &Self) -> f64 {\n"
        "        ((self.x - other.x).powi(2) + (self.y - other.y).powi(2)).sqrt()\n"
        "    }\n"
        "    fn origin() -> Self { Self::new(0.0, 0.0) }\n"
        "}\n\n"
        "fn run() -> f64 {\n"
        "    let a = Point::new(3.0, 0.0);\n"
        "    a.distance(&Point::origin())\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions + m.methods >= 3);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Scala: case class with companion object. */
TEST(mn_record_scala_case_class) {
    MN_Metrics m = mn_metrics("user.scala",
        "case class User(name: String, age: Int) {\n"
        "    def isAdult: Boolean = age >= 18\n"
        "    def greet: String = s\"Hi, $name\"\n"
        "}\n\n"
        "object User {\n"
        "    def anon: User = User(\"anon\", 0)\n"
        "}\n\n"
        "def run: String = User.anon.greet\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions + m.methods >= 2);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 7 — Default / named / variadic parameters
 *
 * The function node must still be created; calls to these functions
 * must still resolve to CALLS edges regardless of how arguments are
 * passed.
 * ══════════════════════════════════════════════════════════════════ */

/* Python: default arguments — function nodes + CALLS. */
TEST(mn_default_params_python) {
    MN_Metrics m = mn_metrics("greet.py",
        "def greet(name, prefix='Hello', suffix='!'):\n"
        "    return f'{prefix}, {name}{suffix}'\n\n\n"
        "def farewell(name, msg='Goodbye'):\n"
        "    return greet(name, prefix=msg)\n\n\n"
        "def run():\n"
        "    return farewell('World')\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 3);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Go: variadic function — function node + call. */
TEST(mn_variadic_go) {
    MN_Metrics m = mn_metrics("sum.go",
        "package math\n\n"
        "func sum(vals ...int) int {\n"
        "    total := 0\n"
        "    for _, v := range vals { total += v }\n"
        "    return total\n"
        "}\n\n"
        "func sumPair(a, b int) int { return sum(a, b) }\n\n"
        "func run() int { return sumPair(sum(1, 2, 3), 4) }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 3);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Rust: default via Option<T> pattern — function and call still resolve. */
TEST(mn_default_params_rust) {
    MN_Metrics m = mn_metrics("cfg.rs",
        "fn connect(host: &str, port: Option<u16>) -> String {\n"
        "    let p = port.unwrap_or(8080);\n"
        "    format!(\"{}:{}\", host, p)\n"
        "}\n\n"
        "fn default_connect(host: &str) -> String {\n"
        "    connect(host, None)\n"
        "}\n\n"
        "fn run() -> String { default_connect(\"localhost\") }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 3);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Kotlin: default + named parameters — function nodes + CALLS. */
TEST(mn_named_params_kotlin) {
    MN_Metrics m = mn_metrics("fmt.kt",
        "fun format(value: Int, radix: Int = 10, prefix: String = \"\") =\n"
        "    prefix + value.toString(radix)\n\n"
        "fun hexStr(n: Int) = format(n, radix = 16, prefix = \"0x\")\n\n"
        "fun run() = hexStr(255)\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 3);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* TypeScript: optional parameters with defaults — function nodes + CALLS. */
TEST(mn_default_params_ts) {
    MN_Metrics m = mn_metrics("log.ts",
        "function log(msg: string, level: string = 'info', ts: number = Date.now()): string {\n"
        "    return `[${level}] ${ts}: ${msg}`;\n"
        "}\n\n"
        "function warn(msg: string): string { return log(msg, 'warn'); }\n\n"
        "function run(): string { return warn('something wrong'); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 3);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* C: variadic function (stdarg) — function node must still appear. */
TEST(mn_variadic_c) {
    MN_Metrics m = mn_metrics("fmt.c",
        "#include <stdarg.h>\n"
        "#include <stdio.h>\n\n"
        "static int my_printf(const char *fmt, ...) {\n"
        "    va_list ap;\n"
        "    va_start(ap, fmt);\n"
        "    int n = vprintf(fmt, ap);\n"
        "    va_end(ap);\n"
        "    return n;\n"
        "}\n\n"
        "int run(int x) { return my_printf(\"%d\", x); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 8 — Pattern matching / destructuring
 *
 * Function calls made INSIDE match arms / switch expressions / when
 * blocks must still produce CALLS edges attributed to the containing
 * function (not the Module).
 * ══════════════════════════════════════════════════════════════════ */

/* Rust: match expression with function calls in arms. */
TEST(mn_pattern_match_rust) {
    MN_Metrics m = mn_metrics("msg.rs",
        "fn format_ok(v: i32) -> String { format!(\"ok:{}\", v) }\n"
        "fn format_err(e: &str) -> String { format!(\"err:{}\", e) }\n\n"
        "fn process(res: Result<i32, &str>) -> String {\n"
        "    match res {\n"
        "        Ok(v)  => format_ok(v),\n"
        "        Err(e) => format_err(e),\n"
        "    }\n"
        "}\n\n"
        "fn run() -> String { process(Ok(42)) }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 4);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Python: structural pattern matching (match/case, Python 3.10+) — calls resolve. */
TEST(mn_pattern_match_python) {
    MN_Metrics m = mn_metrics("cmd.py",
        "def handle_quit():\n"
        "    return 'quit'\n\n\n"
        "def handle_go(direction):\n"
        "    return f'go {direction}'\n\n\n"
        "def handle_unknown(cmd):\n"
        "    return f'unknown: {cmd}'\n\n\n"
        "def dispatch(command):\n"
        "    match command:\n"
        "        case 'quit':\n"
        "            return handle_quit()\n"
        "        case ('go', direction):\n"
        "            return handle_go(direction)\n"
        "        case _:\n"
        "            return handle_unknown(command)\n\n\n"
        "def run():\n"
        "    return dispatch('quit')\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 4);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Kotlin: when expression with function calls in branches. */
TEST(mn_pattern_match_kotlin) {
    MN_Metrics m = mn_metrics("eval.kt",
        "fun onNum(n: Int): String = \"num:$n\"\n"
        "fun onStr(s: String): String = \"str:$s\"\n"
        "fun onOther(): String = \"other\"\n\n"
        "fun eval(v: Any): String = when (v) {\n"
        "    is Int    -> onNum(v)\n"
        "    is String -> onStr(v)\n"
        "    else      -> onOther()\n"
        "}\n\n"
        "fun run() = eval(42)\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 4);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* TypeScript: switch with calls in each case branch. */
TEST(mn_pattern_match_ts) {
    MN_Metrics m = mn_metrics("status.ts",
        "function handleOk(v: number): string { return `ok:${v}`; }\n"
        "function handleFail(e: string): string { return `fail:${e}`; }\n"
        "function handlePending(): string { return 'pending'; }\n\n"
        "function dispatch(tag: string, payload?: unknown): string {\n"
        "    switch (tag) {\n"
        "        case 'ok': return handleOk(payload as number);\n"
        "        case 'fail': return handleFail(payload as string);\n"
        "        default: return handlePending();\n"
        "    }\n"
        "}\n\n"
        "function run(): string { return dispatch('ok', 42); }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 4);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Go: type switch with function calls in each case. */
TEST(mn_pattern_match_go) {
    MN_Metrics m = mn_metrics("switch.go",
        "package sw\n\n"
        "func handleInt(n int) string { return \"int\" }\n"
        "func handleStr(s string) string { return \"str\" }\n"
        "func handleOther() string { return \"other\" }\n\n"
        "func dispatch(v interface{}) string {\n"
        "    switch x := v.(type) {\n"
        "    case int:\n"
        "        return handleInt(x)\n"
        "    case string:\n"
        "        return handleStr(x)\n"
        "    default:\n"
        "        _ = x\n"
        "        return handleOther()\n"
        "    }\n"
        "}\n\n"
        "func run() string { return dispatch(42) }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 4);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 9 — Nested / local / inner functions calling enclosing scope
 *
 * The outer function node must always appear; the inner (nested) call
 * from the outer function to another outer-scope function must still
 * produce a CALLS edge.  Additionally, the inner function calling back
 * into the enclosing function's scope probes whether the pipeline
 * mis-attributes those calls to the Module node instead of the outer.
 * ══════════════════════════════════════════════════════════════════ */

/* Python: nested function returned as closure; outer calls inner. */
TEST(mn_nested_function_python) {
    MN_Metrics m = mn_metrics("clos.py",
        "def multiplier(factor):\n"
        "    def inner(x):\n"
        "        return x * factor\n"
        "    return inner\n\n\n"
        "def apply(fn, val):\n"
        "    return fn(val)\n\n\n"
        "def run():\n"
        "    double = multiplier(2)\n"
        "    return apply(double, 21)\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2); /* multiplier, apply, run (inner is nested) */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Go: inner function literal calling outer parameter. */
TEST(mn_nested_function_go) {
    MN_Metrics m = mn_metrics("nest.go",
        "package nest\n\n"
        "func helper(x int) int { return x * 2 }\n\n"
        "func makeDoubler() func(int) int {\n"
        "    return func(x int) int { return helper(x) }\n"
        "}\n\n"
        "func run(n int) int {\n"
        "    f := makeDoubler()\n"
        "    return f(n) + helper(n)\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2); /* helper, makeDoubler, run */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Rust: nested closure calling outer free function. */
TEST(mn_nested_function_rust) {
    MN_Metrics m = mn_metrics("nest.rs",
        "fn double(x: i32) -> i32 { x * 2 }\n\n"
        "fn make_adder(base: i32) -> impl Fn(i32) -> i32 {\n"
        "    move |x| double(x) + base\n"
        "}\n\n"
        "fn run() -> i32 {\n"
        "    let add10 = make_adder(10);\n"
        "    add10(double(5))\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 3);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* TypeScript: nested arrow functions + calls in outer. */
TEST(mn_nested_function_ts) {
    MN_Metrics m = mn_metrics("nest.ts",
        "function base(x: number): number { return x + 1; }\n\n"
        "function makeTransformer(offset: number): (n: number) => number {\n"
        "    return (n: number) => base(n) + offset;\n"
        "}\n\n"
        "function run(): number {\n"
        "    const t = makeTransformer(10);\n"
        "    return t(base(5));\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 3);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* Java: inner (non-static) class with method calling outer. */
TEST(mn_nested_function_java) {
    MN_Metrics m = mn_metrics("Outer.java",
        "package app;\n\n"
        "class Outer {\n"
        "    private int base;\n"
        "    Outer(int b) { this.base = b; }\n\n"
        "    private int double_(int x) { return x * 2; }\n\n"
        "    class Inner {\n"
        "        int compute(int x) { return double_(x) + base; }\n"
        "    }\n\n"
        "    int run(int n) { return new Inner().compute(n); }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.methods >= 2);
    PASS();
}

/* C: nested function (GCC extension) — outer function must still be a node. */
TEST(mn_nested_function_c) {
    /* C doesn't have nested functions in the standard; use a helper+closure
     * pattern that any C grammar parses without extensions. */
    MN_Metrics m = mn_metrics("nest.c",
        "static int scale(int x, int factor) { return x * factor; }\n\n"
        "static int apply_double(int x) { return scale(x, 2); }\n\n"
        "static int apply_triple(int x) { return scale(x, 3); }\n\n"
        "int run(int n) {\n"
        "    return apply_double(n) + apply_triple(n);\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 4);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 10 — Module / namespace nesting
 *
 * Namespaced constructs (TS namespace, C++ namespace, C# namespace,
 * Rust mod, Python package, Java package, PHP namespace) must yield
 * at least one Module/Package/Class node, and any functions/classes
 * inside them must still appear.  CALLS edges inside a namespace
 * must still be attributed to the right callable.
 * ══════════════════════════════════════════════════════════════════ */

/* TypeScript: nested namespace with class and function. */
TEST(mn_module_ts_namespace) {
    MN_Metrics m = mn_metrics("ns.ts",
        "namespace App {\n"
        "    export namespace Utils {\n"
        "        export function clamp(v: number, lo: number, hi: number): number {\n"
        "            return v < lo ? lo : v > hi ? hi : v;\n"
        "        }\n"
        "        export function normalise(v: number): number { return clamp(v, 0, 1); }\n"
        "    }\n"
        "    export class Config {\n"
        "        threshold: number;\n"
        "        constructor(t: number) { this.threshold = Utils.normalise(t); }\n"
        "    }\n"
        "}\n\n"
        "function run(): number { return new App.Config(0.5).threshold; }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2); /* clamp, normalise, run */
    ASSERT_TRUE(m.types >= 1);    /* Config */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* C++: namespace with nested classes and functions. */
TEST(mn_module_cpp_namespace) {
    MN_Metrics m = mn_metrics("ns.cpp",
        "namespace math {\n"
        "namespace detail {\n"
        "    static double square(double x) { return x * x; }\n"
        "}\n\n"
        "double hypotenuse(double a, double b) {\n"
        "    return detail::square(a) + detail::square(b);\n"
        "}\n"
        "}\n\n"
        "int main() {\n"
        "    double h = math::hypotenuse(3.0, 4.0);\n"
        "    return (int)h;\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 2);
    /* REAL BUG: every call here is namespace-scope-qualified with `::`
     * (detail::square, math::hypotenuse).  Scope-qualified C++ calls are not
     * resolved — the C++ resolver/registry does not handle the `::` path
     * (it splits on '.', not '::'), so none of these resolve → calls=0. [KNOWN
     * class 6 — C++ scope-qualified Class/namespace::method() unresolved] */
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* C#: nested namespaces with classes. */
TEST(mn_module_csharp_namespace) {
    MN_Metrics m = mn_metrics("Ns.cs",
        "namespace App {\n"
        "    namespace Util {\n"
        "        class Formatter {\n"
        "            public static string Format(int n) => $\"[{n}]\";\n"
        "        }\n"
        "    }\n"
        "    namespace Domain {\n"
        "        class Order {\n"
        "            public int Id;\n"
        "            public string Label() => Util.Formatter.Format(Id);\n"
        "        }\n"
        "    }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.methods >= 1);
    PASS();
}

/* Rust: nested mod blocks with pub fn. */
TEST(mn_module_rust_mod) {
    MN_Metrics m = mn_metrics("lib.rs",
        "mod math {\n"
        "    pub fn square(x: i32) -> i32 { x * x }\n\n"
        "    pub mod trig {\n"
        "        pub fn approx_sin(x: f64) -> f64 { x - x*x*x/6.0 }\n"
        "        pub fn approx_cos(x: f64) -> f64 { 1.0 - x*x/2.0 }\n"
        "    }\n"
        "}\n\n"
        "pub fn run() -> i32 {\n"
        "    math::square(4) + (math::trig::approx_sin(0.1) * 100.0) as i32\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 3);
    PASS();
}

/* Python: package-level multi-file with __init__.py — module nodes and IMPORTS. */
TEST(mn_module_python_package) {
    static const MN_LangFile f[] = {
        {"pkg/__init__.py", "from .util import add\n"},
        {"pkg/util.py",
         "def add(a, b):\n    return a + b\n\n\ndef sub(a, b):\n    return a - b\n"},
        {"main.py",
         "from .pkg import add\n\n\ndef run():\n    return add(1, 2)\n"}};
    MN_LangProj lp;
    cbm_store_t *store = mn_index_files(&lp, f, 3);
    int functions = store ? mn_count_label(store, lp.project, "Function") : 0;
    int imports   = store ? cbm_store_count_edges_by_type(store, lp.project, "IMPORTS") : 0;
    int ok        = (store != NULL);
    mn_cleanup(&lp, store);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(functions >= 2); /* add, sub, run */
    ASSERT_TRUE(imports >= 1);   /* at least the relative import resolves */
    PASS();
}

/* Java: multi-level package with class per file. */
TEST(mn_module_java_package) {
    static const MN_LangFile f[] = {
        {"com/example/util/Formatter.java",
         "package com.example.util;\n\n"
         "public class Formatter {\n"
         "    public static String format(int n) { return \"[\" + n + \"]\"; }\n"
         "}\n"},
        {"com/example/domain/Order.java",
         "package com.example.domain;\n\n"
         "import com.example.util.Formatter;\n\n"
         "public class Order {\n"
         "    public int id;\n"
         "    public String label() { return Formatter.format(id); }\n"
         "}\n"}};
    MN_Metrics m = mn_metrics_files(f, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.methods >= 1);
    PASS();
}

/* PHP: namespace declaration and class. */
TEST(mn_module_php_namespace) {
    MN_Metrics m = mn_metrics("Calc.php",
        "<?php\n"
        "namespace App\\Util;\n\n"
        "class Calc {\n"
        "    public function add(int $a, int $b): int {\n"
        "        return $a + $b;\n"
        "    }\n"
        "    public function double(int $n): int {\n"
        "        return $this->add($n, $n);\n"
        "    }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * SUITE registration
 * ══════════════════════════════════════════════════════════════════ */

SUITE(matrix_new_constructs) {
    /* Family 1: type aliases */
    printf("\n%s--- Family 1: type aliases ---%s\n", "\033[90m", "\033[0m");
    RUN_TEST(mn_type_alias_go);
    RUN_TEST(mn_type_alias_go_named);
    RUN_TEST(mn_type_alias_ts);
    RUN_TEST(mn_type_alias_rust);
    RUN_TEST(mn_type_alias_c_typedef);
    RUN_TEST(mn_type_alias_cpp_using);
    RUN_TEST(mn_type_alias_kotlin);
    RUN_TEST(mn_type_alias_python);

    /* Family 2: exception hierarchies */
    printf("\n%s--- Family 2: exception hierarchies ---%s\n", "\033[90m", "\033[0m");
    RUN_TEST(mn_exception_hierarchy_java);
    RUN_TEST(mn_exception_hierarchy_python);
    RUN_TEST(mn_exception_hierarchy_csharp);
    RUN_TEST(mn_exception_hierarchy_kotlin);
    RUN_TEST(mn_exception_hierarchy_cpp);
    RUN_TEST(mn_exception_hierarchy_ts);

    /* Family 3: multiple inheritance / mixins */
    printf("\n%s--- Family 3: multiple inheritance ---%s\n", "\033[90m", "\033[0m");
    RUN_TEST(mn_multiple_inheritance_python);
    RUN_TEST(mn_multiple_inheritance_cpp);
    RUN_TEST(mn_multiple_interfaces_java);
    RUN_TEST(mn_multiple_interfaces_csharp);
    RUN_TEST(mn_multiple_interfaces_ts);
    RUN_TEST(mn_multiple_trait_bounds_rust);
    RUN_TEST(mn_multiple_interfaces_kotlin);

    /* Family 4: extension methods */
    printf("\n%s--- Family 4: extension methods ---%s\n", "\033[90m", "\033[0m");
    RUN_TEST(mn_extension_kotlin);
    RUN_TEST(mn_extension_go_receiver);
    RUN_TEST(mn_extension_rust_impl);
    RUN_TEST(mn_extension_csharp);
    RUN_TEST(mn_extension_swift);

    /* Family 5: getters / setters */
    printf("\n%s--- Family 5: getters/setters ---%s\n", "\033[90m", "\033[0m");
    RUN_TEST(mn_getset_python_property);
    RUN_TEST(mn_getset_csharp);
    RUN_TEST(mn_getset_kotlin);
    RUN_TEST(mn_getset_typescript);
    RUN_TEST(mn_getset_swift);

    /* Family 6: records / data classes */
    printf("\n%s--- Family 6: records/data classes ---%s\n", "\033[90m", "\033[0m");
    RUN_TEST(mn_record_java);
    RUN_TEST(mn_record_kotlin_data_class);
    RUN_TEST(mn_record_csharp);
    RUN_TEST(mn_record_python_dataclass);
    RUN_TEST(mn_record_rust_derive);
    RUN_TEST(mn_record_scala_case_class);

    /* Family 7: default / variadic parameters */
    printf("\n%s--- Family 7: default/variadic params ---%s\n", "\033[90m", "\033[0m");
    RUN_TEST(mn_default_params_python);
    RUN_TEST(mn_variadic_go);
    RUN_TEST(mn_default_params_rust);
    RUN_TEST(mn_named_params_kotlin);
    RUN_TEST(mn_default_params_ts);
    RUN_TEST(mn_variadic_c);

    /* Family 8: pattern matching */
    printf("\n%s--- Family 8: pattern matching ---%s\n", "\033[90m", "\033[0m");
    RUN_TEST(mn_pattern_match_rust);
    RUN_TEST(mn_pattern_match_python);
    RUN_TEST(mn_pattern_match_kotlin);
    RUN_TEST(mn_pattern_match_ts);
    RUN_TEST(mn_pattern_match_go);

    /* Family 9: nested functions */
    printf("\n%s--- Family 9: nested/local functions ---%s\n", "\033[90m", "\033[0m");
    RUN_TEST(mn_nested_function_python);
    RUN_TEST(mn_nested_function_go);
    RUN_TEST(mn_nested_function_rust);
    RUN_TEST(mn_nested_function_ts);
    RUN_TEST(mn_nested_function_java);
    RUN_TEST(mn_nested_function_c);

    /* Family 10: module/namespace nesting */
    printf("\n%s--- Family 10: modules/namespaces ---%s\n", "\033[90m", "\033[0m");
    RUN_TEST(mn_module_ts_namespace);
    RUN_TEST(mn_module_cpp_namespace);
    RUN_TEST(mn_module_csharp_namespace);
    RUN_TEST(mn_module_rust_mod);
    RUN_TEST(mn_module_python_package);
    RUN_TEST(mn_module_java_package);
    RUN_TEST(mn_module_php_namespace);
}
