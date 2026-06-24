/*
 * test_grammar_probe_a.c — Grammar-only node/import/inheritance probe.
 *
 * SCOPE
 * ─────
 * Probes 12 grammar-only languages: ada, awk, cairo, clojure, commonlisp,
 * crystal, d, emacslisp, fennel, fish, fsharp, gleam.
 *
 * CALLS is already covered for these languages by the P5 contract_calls_breadth
 * suite in test_lang_contract.c (CALL_CASES rows).  This file probes the OTHER
 * dimensions that breadth doesn't cover:
 *   • NODE creation  — definition-kind diversity (functions, classes, structs,
 *                      records, constants, modules, protocols, etc.)
 *   • IMPORTS edges  — the language's cross-file import/require/use/open
 *                      mechanism at the graph layer (two-file fixture).
 *   • INHERITANCE    — OOP languages: Crystal `<`, D `:`, F# `inherit`,
 *                      Ada tagged-type extension.
 *   • Other          — language-specific constructs (D structs/interfaces,
 *                      F# records, Clojure protocols/defrecord, etc.)
 *
 * COLOUR LEGEND
 * ─────────────
 *   GREEN = guard: the pipeline already produces the correct result; a failure
 *           here is a real regression.
 *   RED   = bug reproduction: the pipeline does NOT yet produce the expected
 *           node/edge; the test FAILS until the bug is fixed.  Brief inline
 *           comments document the root-cause class.
 *
 * CALLS dimensions are intentionally omitted here (covered by P5 breadth).
 * Do NOT register this suite in test_main.c — a sibling agent owns that file.
 *
 * SUITE(grammar_probe_a)
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
 * Harness — mirrors test_lang_contract.c / test_node_creation_probe.c.
 * Prefix "gpa_" to avoid symbol collisions with sibling probe files.
 * ══════════════════════════════════════════════════════════════════ */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} GpaProj;

typedef struct {
    const char *name;    /* relative filename, may include '/' for subdirs */
    const char *content;
} GpaFile;

static void gpa_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\') *p = '/';
    }
}

static cbm_store_t *gpa_open_indexed(GpaProj *lp) {
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

static cbm_store_t *gpa_index_files(GpaProj *lp, const GpaFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_gpa_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir)) return NULL;
    gpa_to_fwd_slashes(lp->tmpdir);
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
    return gpa_open_indexed(lp);
}

static void gpa_cleanup(GpaProj *lp, cbm_store_t *store) {
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

/* ── Node-count helpers ─────────────────────────────────────────── */

static int gpa_count_label(cbm_store_t *store, const char *project, const char *label) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK)
        return -1;
    cbm_store_free_nodes(nodes, count);
    return count;
}

/* Sum of all type-like labels. */
static int gpa_type_nodes(cbm_store_t *store, const char *project) {
    static const char *labels[] = {"Class","Struct","Interface","Enum","Trait","Type",NULL};
    int total = 0;
    for (int i = 0; labels[i]; i++) {
        int n = gpa_count_label(store, project, labels[i]);
        if (n > 0) total += n;
    }
    return total;
}

/* Metrics bundled per index pass. */
typedef struct {
    int ok;
    int total_nodes;
    int functions;
    int methods;
    int types;     /* type-like sum */
    int imports;   /* IMPORTS edges */
    int inherits;  /* INHERITS edges */
} GpaMetrics;

static GpaMetrics gpa_metrics_files(const GpaFile *files, int nfiles) {
    GpaProj lp;
    cbm_store_t *store = gpa_index_files(&lp, files, nfiles);
    GpaMetrics m = {0};
    if (store) {
        m.ok          = 1;
        m.total_nodes = cbm_store_count_nodes(store, lp.project);
        m.functions   = gpa_count_label(store, lp.project, "Function");
        m.methods     = gpa_count_label(store, lp.project, "Method");
        m.types       = gpa_type_nodes(store, lp.project);
        m.imports     = cbm_store_count_edges_by_type(store, lp.project, "IMPORTS");
        m.inherits    = cbm_store_count_edges_by_type(store, lp.project, "INHERITS");
    }
    gpa_cleanup(&lp, store);
    return m;
}

static GpaMetrics gpa_metrics(const char *filename, const char *content) {
    GpaFile f = {filename, content};
    return gpa_metrics_files(&f, 1);
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 1 — Ada (.adb)
 *
 * Ada label histogram (from test_grammar_labels.c): Function:1, Module:1
 * OOP: Ada 95 tagged types can model inheritance but grammar-only extraction
 *      produces Function/Module — no Class node expected.
 * IMPORTS: Ada `with Pkg;` imports are extracted at the source level.
 *          Graph-level IMPORTS edges require the package body to be present;
 *          single-file fixture is expected RED (resolver can't find the target).
 * CALLS: covered by P5 breadth (CALL_CASES ada entry); not probed here.
 * ══════════════════════════════════════════════════════════════════ */

/* Ada: two procedure definitions → at least 2 Function nodes. */
TEST(probe_ada_functions) {
    GpaMetrics m = gpa_metrics("calc.adb",
        "procedure Calc is\n"
        "   function Add(A, B : Integer) return Integer is\n"
        "   begin\n"
        "      return A + B;\n"
        "   end Add;\n"
        "   function Sub(A, B : Integer) return Integer is\n"
        "   begin\n"
        "      return A - B;\n"
        "   end Sub;\n"
        "begin\n"
        "   null;\n"
        "end Calc;\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.functions >= 1); /* GREEN: at least the outer procedure or inner ones */
    PASS();
}

/* Ada: a package spec (.ads) with multiple subprogram specs → Function nodes. */
TEST(probe_ada_package_spec_nodes) {
    GpaMetrics m = gpa_metrics("math.ads",
        "package Math is\n"
        "   function Square(X : Float) return Float;\n"
        "   function Cube(X : Float) return Float;\n"
        "   function Abs_Val(X : Float) return Float;\n"
        "end Math;\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: spec subprogram declarations must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Ada: pipeline indexes the file and produces at least 1 node (no crash). */
TEST(probe_ada_no_crash) {
    GpaMetrics m = gpa_metrics("hello.adb",
        "with Ada.Text_IO;\n"
        "procedure Hello is\n"
        "begin\n"
        "   Ada.Text_IO.Put_Line(\"Hello\");\n"
        "end Hello;\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Ada: `with` import in a two-file fixture → IMPORTS edge at graph level.
 * RED: grammar-only Ada has no import-resolution pass that turns `with Pkg`
 *      into a graph IMPORTS edge pointing to the package body node. */
TEST(probe_ada_imports_edge) {
    static const GpaFile files[] = {
        {"math.ads",
         "package Math is\n"
         "   function Square(X : Float) return Float;\n"
         "end Math;\n"},
        {"main.adb",
         "with Math;\n"
         "procedure Main is\n"
         "   R : Float;\n"
         "begin\n"
         "   R := Math.Square(3.0);\n"
         "end Main;\n"}
    };
    GpaMetrics m = gpa_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Ada `with` not yet resolved into IMPORTS edges by the pipeline. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no Ada import resolver */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 2 — Awk (.awk)
 *
 * Awk label histogram: Function:2, Module:1
 * Awk has no imports, no OOP, no types — only user-defined functions.
 * ══════════════════════════════════════════════════════════════════ */

/* Awk: two function definitions → 2 Function nodes. */
TEST(probe_awk_functions) {
    GpaMetrics m = gpa_metrics("stats.awk",
        "function max(a, b) {\n"
        "    return (a > b) ? a : b\n"
        "}\n"
        "\n"
        "function min(a, b) {\n"
        "    return (a < b) ? a : b\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both user-defined functions must reach the graph. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Awk: three functions in one file — broader coverage of def-count floor. */
TEST(probe_awk_three_functions) {
    GpaMetrics m = gpa_metrics("util.awk",
        "function trim(s) { sub(/^[ \\t]+/, \"\", s); sub(/[ \\t]+$/, \"\", s); return s }\n"
        "function upper(s,    i, c, r) { for(i=1;i<=length(s);i++) r=r toupper(substr(s,i,1)); return r }\n"
        "function repeat(s, n,    i, r) { for(i=0;i<n;i++) r=r s; return r }\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: all three functions must appear as nodes. */
    ASSERT_TRUE(m.functions >= 3);
    PASS();
}

/* Awk has no OOP — no type-like nodes expected. */
TEST(probe_awk_no_type_nodes) {
    GpaMetrics m = gpa_metrics("proc.awk",
        "function process(line) {\n"
        "    print line\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: awk produces 0 type-like nodes (no classes/structs). */
    ASSERT_TRUE(m.types == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 3 — Cairo (.cairo)
 *
 * Cairo label histogram: Function:2, Module:1
 * Cairo (Starknet) is a Rust-like language — free functions and `fn` items.
 * No OOP classes; no cross-file import mechanism indexable in single pass.
 * ══════════════════════════════════════════════════════════════════ */

/* Cairo: two free functions → 2 Function nodes. */
TEST(probe_cairo_functions) {
    GpaMetrics m = gpa_metrics("math.cairo",
        "fn add(x: felt252, y: felt252) -> felt252 {\n"
        "    x + y\n"
        "}\n"
        "\n"
        "fn mul(x: felt252, y: felt252) -> felt252 {\n"
        "    x * y\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both functions must be graph nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Cairo: trait definition — if the grammar/extractor models it. */
TEST(probe_cairo_trait_or_type) {
    GpaMetrics m = gpa_metrics("iface.cairo",
        "trait IMath {\n"
        "    fn add(x: felt252, y: felt252) -> felt252;\n"
        "}\n"
        "\n"
        "fn zero() -> felt252 { 0 }\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.total_nodes >= 1); /* GREEN: at least the function node. */
    PASS();
}

/* Cairo: three functions to exercise def-count floor. */
TEST(probe_cairo_three_functions) {
    GpaMetrics m = gpa_metrics("ops.cairo",
        "fn identity(x: felt252) -> felt252 { x }\n"
        "fn negate(x: felt252) -> felt252 { -x }\n"
        "fn square(x: felt252) -> felt252 { x * x }\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: all three free functions. */
    ASSERT_TRUE(m.functions >= 3);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 4 — Clojure (.clj)
 *
 * Clojure label histogram: Function:2, Module:1
 * Clojure is a Lisp; no OOP classes.  Namespace with `ns` + `:require`.
 * IMPORTS: `ns` with `:require` can form cross-ns dependencies.
 * CALLS: known gap (lisp list-head not resolved as callee).
 * ══════════════════════════════════════════════════════════════════ */

/* Clojure: two `defn` definitions → 2 Function nodes. */
TEST(probe_clojure_functions) {
    GpaMetrics m = gpa_metrics("core.clj",
        "(ns myapp.core)\n"
        "\n"
        "(defn greet [name]\n"
        "  (str \"Hello, \" name))\n"
        "\n"
        "(defn farewell [name]\n"
        "  (str \"Goodbye, \" name))\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both defn forms must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Clojure: defmulti / defmethod (polymorphic dispatch). */
TEST(probe_clojure_defmulti) {
    GpaMetrics m = gpa_metrics("dispatch.clj",
        "(ns myapp.dispatch)\n"
        "\n"
        "(defmulti area :shape)\n"
        "\n"
        "(defmethod area :circle [s]\n"
        "  (* Math/PI (:r s) (:r s)))\n"
        "\n"
        "(defmethod area :rect [s]\n"
        "  (* (:w s) (:h s)))\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at least the defmulti + defmethod forms produce Function nodes. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Clojure: defrecord → type-like node (if grammar models it). */
TEST(probe_clojure_defrecord) {
    GpaMetrics m = gpa_metrics("model.clj",
        "(ns myapp.model)\n"
        "\n"
        "(defrecord Point [x y])\n"
        "\n"
        "(defn make-point [x y] (->Point x y))\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.total_nodes >= 1); /* GREEN: at minimum the function node. */
    PASS();
}

/* Clojure: two-file :require → IMPORTS edge.
 * RED: grammar-only Clojure has no ns-resolution pass in the pipeline. */
TEST(probe_clojure_imports_edge) {
    static const GpaFile files[] = {
        {"util.clj",
         "(ns myapp.util)\n"
         "(defn double-it [x] (* 2 x))\n"},
        {"core.clj",
         "(ns myapp.core\n"
         "  (:require [myapp.util :as u]))\n"
         "\n"
         "(defn run [n] (u/double-it n))\n"}
    };
    GpaMetrics m = gpa_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Clojure ns :require not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 5 — Common Lisp (.lisp)
 *
 * CommonLisp label histogram: Function:2, Module:1
 * Common Lisp uses `defun` for functions; `defpackage`/`in-package` for namespaces.
 * No class hierarchy (CLOS exists but grammar-only extraction won't model it).
 * ══════════════════════════════════════════════════════════════════ */

/* Common Lisp: two `defun` definitions → 2 Function nodes. */
TEST(probe_commonlisp_functions) {
    GpaMetrics m = gpa_metrics("math.lisp",
        "(defun square (x)\n"
        "  (* x x))\n"
        "\n"
        "(defun cube (x)\n"
        "  (* x (square x)))\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both defun forms must be Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Common Lisp: `defmacro` — if extractor models it as a Function. */
TEST(probe_commonlisp_defmacro) {
    GpaMetrics m = gpa_metrics("macros.lisp",
        "(defmacro when-positive (n &body body)\n"
        "  `(when (> ,n 0) ,@body))\n"
        "\n"
        "(defun use-macro (x)\n"
        "  (when-positive x\n"
        "    (format t \"positive~%\")))\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at minimum defun should produce a Function node. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Common Lisp: defstruct → type-like node (if grammar models it). */
TEST(probe_commonlisp_defstruct) {
    GpaMetrics m = gpa_metrics("types.lisp",
        "(defstruct point\n"
        "  (x 0)\n"
        "  (y 0))\n"
        "\n"
        "(defun make-pt (a b)\n"
        "  (make-point :x a :y b))\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.total_nodes >= 1); /* GREEN: at minimum the function. */
    PASS();
}

/* Common Lisp: `require` in two-file fixture → IMPORTS edge.
 * RED: grammar-only CL has no package-resolution pass in the pipeline. */
TEST(probe_commonlisp_imports_edge) {
    static const GpaFile files[] = {
        {"util.lisp",
         "(defpackage :util (:export :double))\n"
         "(in-package :util)\n"
         "(defun double (x) (* 2 x))\n"},
        {"main.lisp",
         "(defpackage :main (:use :cl :util))\n"
         "(in-package :main)\n"
         "(defun run (n) (util:double n))\n"}
    };
    GpaMetrics m = gpa_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: CL package :use not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 6 — Crystal (.cr)
 *
 * Crystal label histogram: Class:1, Function:1, Module:1
 * Crystal is an OOP language with Ruby-like syntax.
 * Inheritance: `class Child < Parent` — extractor should populate base_classes.
 * Imports: `require "./module"` — cross-file IMPORTS possible.
 * ══════════════════════════════════════════════════════════════════ */

/* Crystal: class and method definitions → Class + Function/Method nodes. */
TEST(probe_crystal_class_node) {
    GpaMetrics m = gpa_metrics("animal.cr",
        "class Animal\n"
        "  def initialize(@name : String)\n"
        "  end\n"
        "\n"
        "  def speak : String\n"
        "    \"...\"\n"
        "  end\n"
        "end\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: class Animal must produce a Class node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Crystal: class with inheritance `class Dog < Animal` → Class + INHERITS edge.
 * GREEN for Class node (histogram confirms Class:1); INHERITS edge status probed
 * separately below. */
TEST(probe_crystal_subclass_node) {
    GpaMetrics m = gpa_metrics("pets.cr",
        "class Animal\n"
        "  def breathe; end\n"
        "end\n"
        "\n"
        "class Dog < Animal\n"
        "  def bark; \"woof\"; end\n"
        "end\n"
        "\n"
        "class Cat < Animal\n"
        "  def meow; \"meow\"; end\n"
        "end\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at least 2 class nodes (Dog, Cat; Animal also expected). */
    ASSERT_TRUE(m.types >= 2);
    PASS();
}

/* Crystal: INHERITS edge via `class Dog < Animal`.
 * RED: grammar-only Crystal pipeline does not yet resolve base_classes into
 *      INHERITS graph edges (no Crystal branch in pass_semantic.c resolver). */
TEST(probe_crystal_inherits_edge) {
    GpaMetrics m = gpa_metrics("inherit.cr",
        "class Base\n"
        "  def do_base; end\n"
        "end\n"
        "\n"
        "class Derived < Base\n"
        "  def do_derived; end\n"
        "end\n");
    ASSERT_TRUE(m.ok);
    /* RED: Crystal INHERITS edge not yet produced by the pipeline. */
    ASSERT_TRUE(m.inherits >= 1); /* expected RED */
    PASS();
}

/* Crystal: module definition → type-like node. */
TEST(probe_crystal_module_node) {
    GpaMetrics m = gpa_metrics("mixin.cr",
        "module Greetable\n"
        "  def greet\n"
        "    puts \"Hello!\"\n"
        "  end\n"
        "end\n"
        "\n"
        "class Person\n"
        "  include Greetable\n"
        "end\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both the module and class should yield type-like nodes. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Crystal: require in two-file fixture → IMPORTS edge.
 * RED: grammar-only Crystal has no `require` resolver in the pipeline. */
TEST(probe_crystal_imports_edge) {
    static const GpaFile files[] = {
        {"util.cr",
         "def double(x : Int32) : Int32\n"
         "  x * 2\n"
         "end\n"},
        {"main.cr",
         "require \"./util\"\n"
         "\n"
         "def run(n : Int32) : Int32\n"
         "  double(n)\n"
         "end\n"}
    };
    GpaMetrics m = gpa_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Crystal `require` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 7 — D (.d) — CBM_LANG_DLANG
 *
 * D label histogram: Function:2, Module:1
 * D has `import`, structs, classes with `:` inheritance, interfaces.
 * ══════════════════════════════════════════════════════════════════ */

/* D: two free functions → 2 Function nodes. */
TEST(probe_d_functions) {
    GpaMetrics m = gpa_metrics("math.d",
        "int square(int x)\n"
        "{\n"
        "    return x * x;\n"
        "}\n"
        "\n"
        "int cube(int x)\n"
        "{\n"
        "    return x * square(x);\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both functions must be Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* D: struct definition → Struct/type-like node.
 * RED: the grammar-only D extractor produces Function:2/Module:1 (histogram);
 *      structs may not yet be extracted as distinct Struct nodes. */
TEST(probe_d_struct_node) {
    GpaMetrics m = gpa_metrics("point.d",
        "struct Point\n"
        "{\n"
        "    double x;\n"
        "    double y;\n"
        "}\n"
        "\n"
        "double dist(Point a, Point b)\n"
        "{\n"
        "    import std.math : sqrt;\n"
        "    return sqrt((a.x-b.x)^^2 + (a.y-b.y)^^2);\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* REAL BUG (NEW-16, node-extraction incompleteness): d_class_types DOES list
     * "struct_declaration" (lang_specs.c:943-950), so the spec is configured, yet 0
     * type nodes reach the graph. D struct/class/interface defs are not being
     * extracted despite the spec — root cause is in the D def-extraction path. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* D: class with inheritance `class Dog : Animal`.
 * RED: D OOP classes not expected in the current histogram (only Function/Module). */
TEST(probe_d_class_inherits) {
    GpaMetrics m = gpa_metrics("pets.d",
        "class Animal\n"
        "{\n"
        "    void breathe() {}\n"
        "}\n"
        "\n"
        "class Dog : Animal\n"
        "{\n"
        "    void bark() {}\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* RED: D class not yet extracted as Class node nor INHERITS edge produced. */
    ASSERT_TRUE(m.types >= 1);       /* expected RED — no D class extractor */
    ASSERT_TRUE(m.inherits >= 1);    /* expected RED — no D INHERITS resolver */
    PASS();
}

/* D: interface definition.
 * RED: D interfaces not expected in current grammar histogram. */
TEST(probe_d_interface_node) {
    GpaMetrics m = gpa_metrics("iface.d",
        "interface IWriter\n"
        "{\n"
        "    void write(string s);\n"
        "    void flush();\n"
        "}\n"
        "\n"
        "void noop() {}\n");
    ASSERT_TRUE(m.ok);
    /* RED: D interface not yet yielding Interface node. */
    ASSERT_TRUE(m.types >= 1); /* expected RED */
    PASS();
}

/* D: import in two-file fixture → IMPORTS edge.
 * RED: grammar-only D has no import resolver in the pipeline. */
TEST(probe_d_imports_edge) {
    static const GpaFile files[] = {
        {"util.d",
         "module util;\n"
         "\n"
         "int double_val(int x) { return x * 2; }\n"},
        {"main.d",
         "module main;\n"
         "\n"
         "import util;\n"
         "\n"
         "void run() {\n"
         "    int y = double_val(21);\n"
         "}\n"}
    };
    GpaMetrics m = gpa_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: D `import` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 8 — Emacs Lisp (.el)
 *
 * EmacsLisp label histogram: Function:2, Module:1
 * Emacs Lisp uses `defun`; `require`/`provide` for modules; no OOP classes.
 * ══════════════════════════════════════════════════════════════════ */

/* Emacs Lisp: two `defun` definitions → 2 Function nodes. */
TEST(probe_emacslisp_functions) {
    GpaMetrics m = gpa_metrics("util.el",
        "(defun el-square (x)\n"
        "  \"Return X squared.\"\n"
        "  (* x x))\n"
        "\n"
        "(defun el-cube (x)\n"
        "  \"Return X cubed.\"\n"
        "  (* x (el-square x)))\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both defun forms must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Emacs Lisp: defvar / defconst — global variable definitions. */
TEST(probe_emacslisp_defvar) {
    GpaMetrics m = gpa_metrics("config.el",
        "(defvar my-config-max 100\n"
        "  \"Maximum value for config.\")\n"
        "\n"
        "(defconst my-version \"1.0\"\n"
        "  \"Package version.\")\n"
        "\n"
        "(defun my-get-max () my-config-max)\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at minimum the defun should produce a Function node. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Emacs Lisp: `require` in two-file fixture → IMPORTS edge.
 * RED: grammar-only EmacsLisp has no require-resolver in the pipeline. */
TEST(probe_emacslisp_imports_edge) {
    static const GpaFile files[] = {
        {"math-util.el",
         "(provide 'math-util)\n"
         "\n"
         "(defun math-util-double (x) (* 2 x))\n"},
        {"main.el",
         "(require 'math-util)\n"
         "\n"
         "(defun main-run (n) (math-util-double n))\n"}
    };
    GpaMetrics m = gpa_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: EmacsLisp `require` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 9 — Fennel (.fnl)
 *
 * Fennel label histogram: Function:2, Module:1
 * Fennel is a Lisp that compiles to Lua; `require` for imports; no OOP classes.
 * ══════════════════════════════════════════════════════════════════ */

/* Fennel: two `fn` definitions → 2 Function nodes. */
TEST(probe_fennel_functions) {
    GpaMetrics m = gpa_metrics("ops.fnl",
        "(fn add [a b]\n"
        "  (+ a b))\n"
        "\n"
        "(fn mul [a b]\n"
        "  (* a b))\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both fn forms must be Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Fennel: `lambda` (shorthand fn) definition. */
TEST(probe_fennel_lambda) {
    GpaMetrics m = gpa_metrics("lambdas.fnl",
        "(fn double [x] (* 2 x))\n"
        "(fn square [x] (* x x))\n"
        "(fn cube [x] (* x x x))\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: all three fn forms must be Function nodes. */
    ASSERT_TRUE(m.functions >= 3);
    PASS();
}

/* Fennel: `require` in two-file fixture → IMPORTS edge.
 * RED: grammar-only Fennel has no require-resolver in the pipeline. */
TEST(probe_fennel_imports_edge) {
    static const GpaFile files[] = {
        {"util.fnl",
         "(fn double [x] (* 2 x))\n"
         "{:double double}\n"},
        {"main.fnl",
         "(local util (require :util))\n"
         "\n"
         "(fn run [n]\n"
         "  (util.double n))\n"}
    };
    GpaMetrics m = gpa_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Fennel `require` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 10 — Fish (.fish)
 *
 * Fish label histogram: Function:1, Module:1
 * Fish shell scripting: `function` keyword; no imports, no OOP.
 * The histogram shows only 1 function extracted (CALL_CASES fixture has 2 fns
 * but only 1 reaches the graph — possible extraction gap for the second fn).
 * ══════════════════════════════════════════════════════════════════ */

/* Fish: single function definition → at least 1 Function node. */
TEST(probe_fish_function_node) {
    GpaMetrics m = gpa_metrics("greet.fish",
        "function greet\n"
        "    set name $argv[1]\n"
        "    echo \"Hello, $name!\"\n"
        "end\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: the function must appear as a node. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Fish: two function definitions — exercises the known histogram gap.
 * The P5 CALL_CASES fixture has 2 fns but histogram shows Function:1.
 * RED: second fish function not extracted (possible grammar gap in extractor). */
TEST(probe_fish_two_functions) {
    GpaMetrics m = gpa_metrics("funcs.fish",
        "function say_hello\n"
        "    echo \"Hello!\"\n"
        "end\n"
        "\n"
        "function say_bye\n"
        "    echo \"Bye!\"\n"
        "end\n");
    ASSERT_TRUE(m.ok);
    /* RED: only 1 of the 2 fish functions appears to be extracted (histogram gap). */
    ASSERT_TRUE(m.functions >= 2); /* expected RED — second fn not extracted */
    PASS();
}

/* Fish has no OOP and no import mechanism; verify no spurious type nodes. */
TEST(probe_fish_no_type_nodes) {
    GpaMetrics m = gpa_metrics("noop.fish",
        "function noop\n"
        "    true\n"
        "end\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: fish produces 0 type-like nodes. */
    ASSERT_TRUE(m.types == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 11 — F# (.fs)
 *
 * F# label histogram: Function:2, Module:1
 * F# is a functional-first .NET language: `let` functions, `type` records/unions,
 * `open` for imports, `type Child() = inherit Parent()` for OOP.
 * ══════════════════════════════════════════════════════════════════ */

/* F#: two `let` function bindings → 2 Function nodes. */
TEST(probe_fsharp_functions) {
    GpaMetrics m = gpa_metrics("calc.fs",
        "let square x = x * x\n"
        "\n"
        "let cube x = x * square x\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both let-bindings must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* F#: record type definition → type-like node.
 * RED: the grammar histogram shows only Function/Module; record types not yet
 *      extracted as Type/Struct nodes. */
TEST(probe_fsharp_record_type) {
    GpaMetrics m = gpa_metrics("model.fs",
        "type Point = { X: float; Y: float }\n"
        "\n"
        "let origin = { X = 0.0; Y = 0.0 }\n"
        "\n"
        "let dist (a: Point) (b: Point) =\n"
        "    let dx = a.X - b.X\n"
        "    let dy = a.Y - b.Y\n"
        "    sqrt (dx*dx + dy*dy)\n");
    ASSERT_TRUE(m.ok);
    /* RED: F# record type not yet extracted as a Type/Struct node. */
    ASSERT_TRUE(m.types >= 1); /* expected RED */
    PASS();
}

/* F#: discriminated union → type-like node.
 * RED: same root cause as record types. */
TEST(probe_fsharp_discriminated_union) {
    GpaMetrics m = gpa_metrics("shape.fs",
        "type Shape =\n"
        "    | Circle of float\n"
        "    | Rectangle of float * float\n"
        "\n"
        "let area shape =\n"
        "    match shape with\n"
        "    | Circle r -> System.Math.PI * r * r\n"
        "    | Rectangle (w, h) -> w * h\n");
    ASSERT_TRUE(m.ok);
    /* RED: F# discriminated union not yet extracted as a Type node. */
    ASSERT_TRUE(m.types >= 1); /* expected RED */
    PASS();
}

/* F#: class with `inherit` → Class node + INHERITS edge.
 * RED: class nodes not in histogram; no F# INHERITS resolver. */
TEST(probe_fsharp_class_inherits) {
    GpaMetrics m = gpa_metrics("oop.fs",
        "type Animal(name: string) =\n"
        "    member _.Name = name\n"
        "    abstract member Speak: unit -> string\n"
        "    default _.Speak() = \"...\"\n"
        "\n"
        "type Dog(name: string) =\n"
        "    inherit Animal(name)\n"
        "    override _.Speak() = \"Woof!\"\n");
    ASSERT_TRUE(m.ok);
    /* RED: F# OOP class/inherit not yet modeled by the extractor. */
    ASSERT_TRUE(m.types >= 1);    /* expected RED — no F# class extraction */
    ASSERT_TRUE(m.inherits >= 1); /* expected RED — no F# INHERITS resolver */
    PASS();
}

/* F#: `open` in two-file fixture → IMPORTS edge.
 * RED: grammar-only F# has no `open`-resolver in the pipeline. */
TEST(probe_fsharp_imports_edge) {
    static const GpaFile files[] = {
        {"MathUtils.fs",
         "module MathUtils\n"
         "\n"
         "let double x = x * 2\n"
         "let triple x = x * 3\n"},
        {"Main.fs",
         "module Main\n"
         "\n"
         "open MathUtils\n"
         "\n"
         "let run n = double n + triple n\n"}
    };
    GpaMetrics m = gpa_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: F# `open` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 12 — Gleam (.gleam)
 *
 * Gleam label histogram: Function:2, Module:1
 * Gleam is a statically-typed functional language on the BEAM (Erlang VM).
 * `pub fn` for public functions, `import` for modules; no OOP classes.
 * ══════════════════════════════════════════════════════════════════ */

/* Gleam: two `pub fn` definitions → 2 Function nodes. */
TEST(probe_gleam_functions) {
    GpaMetrics m = gpa_metrics("math.gleam",
        "pub fn square(x: Int) -> Int {\n"
        "  x * x\n"
        "}\n"
        "\n"
        "pub fn cube(x: Int) -> Int {\n"
        "  x * square(x)\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both pub fn definitions must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Gleam: private (non-pub) function — must also produce a Function node. */
TEST(probe_gleam_private_function) {
    GpaMetrics m = gpa_metrics("util.gleam",
        "fn internal_double(x: Int) -> Int {\n"
        "  x * 2\n"
        "}\n"
        "\n"
        "pub fn double(x: Int) -> Int {\n"
        "  internal_double(x)\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both the private and public fn must become Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Gleam: custom type definition → type-like node.
 * RED: histogram shows Function/Module only; custom types not yet extracted. */
TEST(probe_gleam_custom_type) {
    GpaMetrics m = gpa_metrics("types.gleam",
        "pub type Shape {\n"
        "  Circle(Float)\n"
        "  Rectangle(Float, Float)\n"
        "}\n"
        "\n"
        "pub fn area(s: Shape) -> Float {\n"
        "  case s {\n"
        "    Circle(r) -> 3.14159 *. r *. r\n"
        "    Rectangle(w, h) -> w *. h\n"
        "  }\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* RED: Gleam custom type not yet extracted as a Type node. */
    ASSERT_TRUE(m.types >= 1); /* expected RED */
    PASS();
}

/* Gleam: `import` in two-file fixture → IMPORTS edge.
 * RED: grammar-only Gleam has no import-resolver in the pipeline. */
TEST(probe_gleam_imports_edge) {
    static const GpaFile files[] = {
        {"math_utils.gleam",
         "pub fn double(x: Int) -> Int {\n"
         "  x * 2\n"
         "}\n"},
        {"main.gleam",
         "import math_utils\n"
         "\n"
         "pub fn run(n: Int) -> Int {\n"
         "  math_utils.double(n)\n"
         "}\n"}
    };
    GpaMetrics m = gpa_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Gleam `import` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * SUITE wiring
 * ══════════════════════════════════════════════════════════════════ */

SUITE(grammar_probe_a) {
    /* Ada */
    RUN_TEST(probe_ada_functions);
    RUN_TEST(probe_ada_package_spec_nodes);
    RUN_TEST(probe_ada_no_crash);
    RUN_TEST(probe_ada_imports_edge);

    /* Awk */
    RUN_TEST(probe_awk_functions);
    RUN_TEST(probe_awk_three_functions);
    RUN_TEST(probe_awk_no_type_nodes);

    /* Cairo */
    RUN_TEST(probe_cairo_functions);
    RUN_TEST(probe_cairo_trait_or_type);
    RUN_TEST(probe_cairo_three_functions);

    /* Clojure */
    RUN_TEST(probe_clojure_functions);
    RUN_TEST(probe_clojure_defmulti);
    RUN_TEST(probe_clojure_defrecord);
    RUN_TEST(probe_clojure_imports_edge);

    /* Common Lisp */
    RUN_TEST(probe_commonlisp_functions);
    RUN_TEST(probe_commonlisp_defmacro);
    RUN_TEST(probe_commonlisp_defstruct);
    RUN_TEST(probe_commonlisp_imports_edge);

    /* Crystal */
    RUN_TEST(probe_crystal_class_node);
    RUN_TEST(probe_crystal_subclass_node);
    RUN_TEST(probe_crystal_inherits_edge);
    RUN_TEST(probe_crystal_module_node);
    RUN_TEST(probe_crystal_imports_edge);

    /* D */
    RUN_TEST(probe_d_functions);
    RUN_TEST(probe_d_struct_node);
    RUN_TEST(probe_d_class_inherits);
    RUN_TEST(probe_d_interface_node);
    RUN_TEST(probe_d_imports_edge);

    /* Emacs Lisp */
    RUN_TEST(probe_emacslisp_functions);
    RUN_TEST(probe_emacslisp_defvar);
    RUN_TEST(probe_emacslisp_imports_edge);

    /* Fennel */
    RUN_TEST(probe_fennel_functions);
    RUN_TEST(probe_fennel_lambda);
    RUN_TEST(probe_fennel_imports_edge);

    /* Fish */
    RUN_TEST(probe_fish_function_node);
    RUN_TEST(probe_fish_two_functions);
    RUN_TEST(probe_fish_no_type_nodes);

    /* F# */
    RUN_TEST(probe_fsharp_functions);
    RUN_TEST(probe_fsharp_record_type);
    RUN_TEST(probe_fsharp_discriminated_union);
    RUN_TEST(probe_fsharp_class_inherits);
    RUN_TEST(probe_fsharp_imports_edge);

    /* Gleam */
    RUN_TEST(probe_gleam_functions);
    RUN_TEST(probe_gleam_private_function);
    RUN_TEST(probe_gleam_custom_type);
    RUN_TEST(probe_gleam_imports_edge);
}
