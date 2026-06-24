/*
 * test_edge_structural.c — Pipeline / edge-CREATION reproduction suite.
 *
 * ════════════════════════════════════════════════════════════════════
 * PURPOSE
 * ────────
 * Comprehensively probe whether the GRAPH PIPELINE creates each non-import
 * edge type for each hybrid-LSP language, with special emphasis on CROSS-FILE
 * scenarios where the resolver must map a bare name in file B to a definition
 * node living in file A.
 *
 * This is distinct from the extraction-level suites (test_extraction.c,
 * test_extraction_inheritance.c, test_extraction_imports.c) which test whether
 * cbm_extract_file() populates the right fields.  Here we run the FULL
 * production pipeline (index_repository via MCP) and assert the resulting
 * graph DB contains the expected edges via cbm_store_count_edges_by_type.
 *
 * The companion file test_edge_imports.c (another workstream) covers IMPORTS
 * edge creation.  This file covers every OTHER edge type.
 *
 * ════════════════════════════════════════════════════════════════════
 * EXPECTED GREEN vs RED — REASONING
 * ──────────────────────────────────
 *
 * CALLS (cross-file):
 *   All 9 hybrid-LSP languages use the generic name-based resolver
 *   (pass_calls.c).  The resolver populates the registry with all defs first,
 *   then matches call sites by name.  Cross-file is structurally identical to
 *   same-file from the resolver's perspective (registry is project-wide).
 *   CALLS resolve at 99.6–100% on real repos, so all 9 languages are expected
 *   GREEN.  Any RED would be a real regression.
 *
 * INHERITS (cross-file class extends):
 *   Resolution requires: (a) base_classes extracted correctly at the
 *   extraction layer, AND (b) the registry resolving that bare name to the
 *   correct Class node in the other file.  Known extraction bugs (documented
 *   in test_extraction_inheritance.c):
 *     Python   — simple `class Dog(Animal):` broken; base_classes holds "(Animal)"
 *                with parens — extraction RED → INHERITS edge RED.
 *     TypeScript — extractor stores "extends" keyword not the type name → RED.
 *     PHP      — base_classes never populated → RED.
 *     Kotlin   — `:` supertype syntax not parsed → RED.
 *   Expected GREEN (extraction works):
 *     Java, C#, C++  — extraction correct per test_extraction_inheritance.c.
 *     Rust     — uses impl_traits not base_classes; IMPLEMENTS path, not
 *                INHERITS.  No INHERITS expected.
 *     Go       — struct embedding / interface satisfaction uses IMPLEMENTS not
 *                INHERITS in the Go semantic pass.
 *
 * IMPLEMENTS (interface):
 *   Rust `impl Trait for Struct` cross-file: trait in a.rs, impl in b.rs.
 *     The resolve_impl_traits pass uses registry lookup for both trait_name and
 *     struct_name.  If both are in the registry (from definitions pass), the edge
 *     is created.  Expected GREEN when both files are indexed.
 *   Java `implements` interface (same-file): extraction stores base_classes; the
 *     semantic pass creates INHERITS for `extends` and IMPLEMENTS for interfaces
 *     (pass_parallel.c line ~1943).  Expected GREEN same-file, RED cross-file
 *     until the resolver properly distinguishes Class vs Interface targets.
 *   Go implicit interface satisfaction: pass_semantic.c cbm_pipeline_implements_go()
 *     is triggered only when the struct's method-set fully covers the interface's.
 *     Expected GREEN when struct methods and interface are in same index.
 *   C# `class X : IFoo`: extraction stores base_classes; same INHERITS/IMPLEMENTS
 *     resolution path as Java.  Same-file expected GREEN.
 *
 * DECORATES (cross-file decorator):
 *   Python: decorator resolved via import map + registry.  Cross-file requires
 *     an IMPORTS edge resolution that in turn requires the decorator to be in the
 *     registry.  Expected GREEN for same-file (existing P6 contract confirms this).
 *     Cross-file expected GREEN if relative import resolves (Python OK for
 *     relative imports per P3).
 *   TypeScript: TS decorator (class decorator).  TS extraction captures
 *     decorators array.  Expected GREEN same-file.
 *   Java annotations: Java extraction may not populate decorators[].  Expected
 *     RED (unconfirmed — annotated as uncertain).
 *   Kotlin annotations: Kotlin extraction status unclear → tentatively RED.
 *   C# attributes: C# extraction unclear → tentatively RED.
 *
 * USAGE (cross-file type usage):
 *   The USAGE pass (pass_usages.c) creates edges when a type/symbol reference
 *   (not a call) resolves in the registry.  Expected GREEN for languages where
 *   usages are extracted (Python, Go, TS).  Uncertain for Java/C#/Kotlin/PHP/Rust
 *   where USAGE extraction coverage is unclear.
 *
 * DATA_FLOWS:
 *   Created by pass_data_flows.c via Route intermediation (HTTP_CALLS + HANDLES
 *   on the same route path).  Not per-language — fixture-driven.  Existing P6
 *   contract covers same-file.  This suite adds a cross-file variant.
 *
 * OVERRIDE (Go interface method):
 *   Created by pass_semantic.c when a Go struct satisfies an interface; each
 *   implementing method gets an OVERRIDE edge to the interface method.  Expected
 *   GREEN for Go with a struct + interface pair in the same index.
 *
 * TESTS (function-level test->production function):
 *   pass_tests.c creates this.  Existing P6 covers Go.  This suite adds Python
 *   and TypeScript cross-file variants.
 *
 * ════════════════════════════════════════════════════════════════════
 * SUITE STRUCTURE
 * ───────────────
 * Edge family                       Languages / cases         Expected
 * ─────────────────────────────────────────────────────────────────
 * CALLS cross-file                  Go/C/C++/Rust/Python/     9 GREEN
 *                                   TS/Java/Kotlin/C#
 * INHERITS cross-file               Java/C#/C++               3 GREEN
 *                                   Python/TS/PHP/Kotlin       4 RED
 * IMPLEMENTS cross-file             Rust                      1 GREEN
 * IMPLEMENTS same-file interface    Java/C#/Go                3 GREEN (uncertain)
 * DECORATES same-file               Python/TS                  2 GREEN
 * DECORATES cross-file              Python                     1 GREEN
 * DECORATES annotations             Java/Kotlin/C#             3 uncertain→RED
 * USAGE cross-file                  Python/TS/Go               3 GREEN (uncertain)
 * DATA_FLOWS cross-file             Python                     1 GREEN
 * OVERRIDE (Go implicit iface)      Go                         1 GREEN
 * TESTS cross-file                  Python/TypeScript          2 GREEN
 *
 * Total test functions: 24
 *
 * NOTE: "Do NOT register in test_main.c" — this suite is SEPARATE from the
 * existing lang_contract suite.  The SUITE(edge_structural) macro defines
 * suite_edge_structural() which can be wired up independently.
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

/* ══════════════════════════════════════════════════════════════════
 * Harness (copy of the pattern from test_lang_contract.c — these
 * helpers are static so each translation unit is self-contained).
 * ══════════════════════════════════════════════════════════════════ */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} ES_LangProj;

typedef struct {
    const char *name;
    const char *content;
} ES_LangFile;

static void es_lc_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\') {
            *p = '/';
        }
    }
}

static cbm_store_t *es_lang_open_indexed(ES_LangProj *lp) {
    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project) {
        return NULL;
    }
    const char *home = getenv("HOME");
    if (!home) {
        home = "/tmp";
    }
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);
    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv) {
        return NULL;
    }
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp) {
        free(resp);
    }
    return cbm_store_open_path(lp->dbpath);
}

static cbm_store_t *es_lang_index_files(ES_LangProj *lp, const ES_LangFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_es_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir)) {
        return NULL;
    }
    es_lc_to_fwd_slashes(lp->tmpdir);
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
        if (!f) {
            return NULL;
        }
        fputs(files[i].content, f);
        fclose(f);
    }
    return es_lang_open_indexed(lp);
}

static void es_lang_cleanup(ES_LangProj *lp, cbm_store_t *store) {
    if (store) {
        cbm_store_close(store);
    }
    if (lp->srv) {
        cbm_mcp_server_free(lp->srv);
        lp->srv = NULL;
    }
    free(lp->project);
    lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600];
    char shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    unlink(wal);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(shm);
}

/* Every graph edge type the pipeline can emit — used in diagnostic dumps. */
static const char *ES_ALL_EDGE_TYPES[] = {
    "CALLS",        "CONFIGURES",    "CONTAINS_FILE", "CONTAINS_FOLDER",
    "DATA_FLOWS",   "DECORATES",     "DEFINES",       "DEFINES_METHOD",
    "DEPENDS_ON",   "FILE_CHANGES_WITH", "GRAPHQL_CALLS", "GRPC_CALLS",
    "HANDLES",      "HTTP_CALLS",    "IMPLEMENTS",    "IMPORTS",
    "INHERITS",     "INFRA_MAPS",    "OVERRIDE",      "SEMANTICALLY_RELATED",
    "SIMILAR_TO",   "TESTS_FILE",    "TESTS",         "TRPC_CALLS",
    "USAGE",        "ASYNC_CALLS",   NULL};

static void es_dump_edge_histogram(cbm_store_t *store, const char *project) {
    if (!store) {
        fprintf(stderr, "      └─ (no graph DB)\n");
        return;
    }
    char line[640] = {0};
    for (int i = 0; ES_ALL_EDGE_TYPES[i]; i++) {
        int c = cbm_store_count_edges_by_type(store, project, ES_ALL_EDGE_TYPES[i]);
        if (c > 0 && strlen(line) < sizeof(line) - 48) {
            char one[64];
            snprintf(one, sizeof(one), "%s=%d ", ES_ALL_EDGE_TYPES[i], c);
            strncat(line, one, sizeof(line) - strlen(line) - 1);
        }
    }
    fprintf(stderr, "      └─ edges: [%s]\n", line[0] ? line : "(none)");
}

/* Index `files`, assert `edge` appears at least `floor` times.
 * On failure, dump the full edge histogram. */
static int es_edge_present(const ES_LangFile *files, int nfiles, const char *edge, int floor) {
    ES_LangProj lp;
    cbm_store_t *store = es_lang_index_files(&lp, files, nfiles);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, edge) : -1;
    if (got < floor) {
        fprintf(stderr, "  [ES-EDGE] FAIL %-20s got=%d expected>=%d\n", edge, got, floor);
        es_dump_edge_histogram(store, lp.project);
    }
    es_lang_cleanup(&lp, store);
    return got >= floor;
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 1: CALLS cross-file
 *
 * Function defined in file A, called from file B.  The registry is
 * project-wide so cross-file is identical to same-file for the resolver.
 * ALL 9 hybrid-LSP languages are expected GREEN.
 * ══════════════════════════════════════════════════════════════════ */

/* Go: caller in main.go, callee in util.go — same package. */
TEST(es_calls_crossfile_go) {
    static const ES_LangFile f[] = {
        {"util.go",
         "package svc\n\nfunc Compute(x int) int {\n\treturn x * 2\n}\n"},
        {"main.go",
         "package svc\n\nfunc Run(y int) int {\n\treturn Compute(y + 1)\n}\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* Run -> Compute */
    PASS();
}

/* C: caller in main.c, callee declared/defined in util.c. */
TEST(es_calls_crossfile_c) {
    static const ES_LangFile f[] = {
        {"util.c",
         "int add(int a, int b) {\n    return a + b;\n}\n"},
        {"main.c",
         "int add(int a, int b);\n\nint run(int x) {\n    return add(x, 1);\n}\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* run -> add */
    PASS();
}

/* C++: caller in main.cpp, callee in util.cpp. */
TEST(es_calls_crossfile_cpp) {
    static const ES_LangFile f[] = {
        {"util.cpp",
         "int multiply(int a, int b) {\n    return a * b;\n}\n"},
        {"main.cpp",
         "int multiply(int a, int b);\n\nint run(int x) {\n    return multiply(x, 3);\n}\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* run -> multiply */
    PASS();
}

/* Rust: caller in main.rs, callee in lib.rs (pub fn). */
TEST(es_calls_crossfile_rust) {
    static const ES_LangFile f[] = {
        {"lib.rs",
         "pub fn square(x: i32) -> i32 {\n    x * x\n}\n"},
        {"main.rs",
         "mod lib;\n\nfn run(n: i32) -> i32 {\n    lib::square(n)\n}\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* run -> square */
    PASS();
}

/* Python: caller in main.py calls function from util.py via relative import. */
TEST(es_calls_crossfile_python) {
    static const ES_LangFile f[] = {
        {"util.py",
         "def transform(x):\n    return x * 3\n"},
        {"main.py",
         "from .util import transform\n\n\ndef run(y):\n    return transform(y)\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* run -> transform */
    PASS();
}

/* TypeScript: caller in main.ts calls function from util.ts. */
TEST(es_calls_crossfile_typescript) {
    static const ES_LangFile f[] = {
        {"util.ts",
         "export function format(s: string): string {\n    return s.trim();\n}\n"},
        {"main.ts",
         "import { format } from './util';\n\n"
         "export function run(input: string): string {\n    return format(input);\n}\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* run -> format */
    PASS();
}

/* Java: caller in Main.java calls static method from Util.java (same package). */
TEST(es_calls_crossfile_java) {
    static const ES_LangFile f[] = {
        {"Util.java",
         "package app;\n\nclass Util {\n    static int square(int x) { return x * x; }\n}\n"},
        {"Main.java",
         "package app;\n\nclass Main {\n    int run(int n) { return Util.square(n); }\n}\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* run -> square */
    PASS();
}

/* Kotlin: caller in Main.kt calls top-level function from Util.kt. */
TEST(es_calls_crossfile_kotlin) {
    static const ES_LangFile f[] = {
        {"Util.kt",
         "fun double(x: Int): Int = x * 2\n"},
        {"Main.kt",
         "fun run(n: Int): Int = double(n)\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* run -> double */
    PASS();
}

/* C#: caller in Main.cs calls static method from Util.cs (same namespace). */
TEST(es_calls_crossfile_csharp) {
    static const ES_LangFile f[] = {
        {"Util.cs",
         "namespace App {\n    class Util {\n"
         "        public static int Square(int x) { return x * x; }\n    }\n}\n"},
        {"Main.cs",
         "namespace App {\n    class Main {\n"
         "        public int Run(int n) { return Util.Square(n); }\n    }\n}\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* Run -> Square */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 2: INHERITS cross-file
 *
 * Subclass in file B extends base class defined in file A.
 * Requires both: (a) correct base_classes extraction AND (b) registry
 * resolving the base name to the correct node.
 *
 * Expected GREEN: Java, C#, C++ (extractors confirmed correct).
 * Expected RED:   Python, TypeScript, PHP, Kotlin (extraction bugs).
 * ══════════════════════════════════════════════════════════════════ */

/* Java cross-file INHERITS — expected GREEN.
 * Animal in Animal.java, Dog extends Animal in Dog.java, same package. */
TEST(es_inherits_crossfile_java) {
    static const ES_LangFile f[] = {
        {"Animal.java",
         "package zoo;\n\nclass Animal {\n    int speak() { return 0; }\n}\n"},
        {"Dog.java",
         "package zoo;\n\nclass Dog extends Animal {\n    int speak() { return 1; }\n}\n"}};
    /* GREEN: Java extraction correct; registry resolves cross-file same-package. */
    ASSERT_TRUE(es_edge_present(f, 2, "INHERITS", 1)); /* Dog -> Animal */
    PASS();
}

/* C# cross-file INHERITS — expected GREEN.
 * Base in Base.cs, Derived extends Base in Derived.cs, same namespace. */
TEST(es_inherits_crossfile_csharp) {
    static const ES_LangFile f[] = {
        {"Base.cs",
         "namespace App {\n    class Base {\n        public int Value() { return 0; }\n    }\n}\n"},
        {"Derived.cs",
         "namespace App {\n    class Derived : Base {\n"
         "        public int Extra() { return 1; }\n    }\n}\n"}};
    /* GREEN: C# extraction confirmed correct. */
    ASSERT_TRUE(es_edge_present(f, 2, "INHERITS", 1)); /* Derived -> Base */
    PASS();
}

/* C++ cross-file INHERITS — expected GREEN.
 * Shape in shape.cpp, Circle extends Shape in circle.cpp. */
TEST(es_inherits_crossfile_cpp) {
    static const ES_LangFile f[] = {
        {"shape.cpp",
         "class Shape {\npublic:\n    virtual int area() { return 0; }\n};\n"},
        {"circle.cpp",
         "class Shape;\n\nclass Circle : public Shape {\npublic:\n"
         "    int area() { return 3; }\n};\n"}};
    /* GREEN: C++ extraction confirmed correct. */
    ASSERT_TRUE(es_edge_present(f, 2, "INHERITS", 1)); /* Circle -> Shape */
    PASS();
}

/* Python cross-file INHERITS — expected RED (extraction bug: base_classes
 * holds "(Animal)" with parens, not "Animal").
 * Reproduction: confirms the end-to-end gap from extraction to graph edge. */
TEST(es_inherits_crossfile_python_red) {
    static const ES_LangFile f[] = {
        {"animal.py",
         "class Animal:\n    def speak(self):\n        return 0\n"},
        {"dog.py",
         "from .animal import Animal\n\n\nclass Dog(Animal):\n    def speak(self):\n        return 1\n"}};
    /* RED: base_classes extraction broken for Python plain identifier nodes.
     * Root cause: collect_bases_from_field does not match bare `identifier`
     * nodes from tree-sitter-python; stores "(Animal)" with parens.
     * Fix location: extract_defs.c collect_bases_from_field / Python case.
     * This test SHOULD FAIL (count=0) until that fix lands. */
    ES_LangProj lp;
    cbm_store_t *store = es_lang_index_files(&lp, f, 2);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, "INHERITS") : -1;
    if (got >= 1) {
        fprintf(stderr, "  [ES-EDGE] UNEXPECTED PASS: Python cross-file INHERITS got=%d "
                        "(extraction bug may have been fixed — promote to GREEN)\n", got);
    } else {
        fprintf(stderr, "  [ES-EDGE] CONFIRMED RED: Python cross-file INHERITS got=%d "
                        "(extraction bug reproduces end-to-end)\n", got);
    }
    es_lang_cleanup(&lp, store);
    /* Assert the CORRECT outcome: edge should be present.
     * This FAILS (RED) until the extraction bug is fixed. */
    ASSERT_TRUE(got >= 1);
    PASS();
}

/* TypeScript cross-file INHERITS — expected RED (extractor stores "extends"
 * keyword instead of the base type name). */
TEST(es_inherits_crossfile_typescript_red) {
    static const ES_LangFile f[] = {
        {"base.ts",
         "export class Base {\n    value(): number { return 0; }\n}\n"},
        {"derived.ts",
         "import { Base } from './base';\n\n"
         "export class Derived extends Base {\n    extra(): number { return 1; }\n}\n"}};
    /* RED: TypeScript extractor stores "extends" keyword in base_classes.
     * Root cause: TS extraction walks the heritage_clause and captures the
     * keyword token rather than the type_identifier that follows it.
     * Fix location: extract_defs.c TypeScript heritage clause handling.
     * This test SHOULD FAIL until fixed. */
    ES_LangProj lp;
    cbm_store_t *store = es_lang_index_files(&lp, f, 2);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, "INHERITS") : -1;
    if (got >= 1) {
        fprintf(stderr, "  [ES-EDGE] UNEXPECTED PASS: TypeScript cross-file INHERITS got=%d "
                        "(promote to GREEN if extraction fixed)\n", got);
    } else {
        fprintf(stderr, "  [ES-EDGE] CONFIRMED RED: TypeScript cross-file INHERITS got=%d\n", got);
    }
    es_lang_cleanup(&lp, store);
    ASSERT_TRUE(got >= 1); /* FAILS (RED) until TS extraction fixed */
    PASS();
}

/* PHP cross-file INHERITS — expected RED (base_classes never populated). */
TEST(es_inherits_crossfile_php_red) {
    static const ES_LangFile f[] = {
        {"Base.php",
         "<?php\nclass Base {\n    public function value() { return 0; }\n}\n"},
        {"Child.php",
         "<?php\nrequire_once 'Base.php';\n\nclass Child extends Base {\n"
         "    public function extra() { return 1; }\n}\n"}};
    /* RED: PHP extractor does not populate base_classes for `extends`.
     * Fix location: extract_defs.c PHP class heritage clause.
     * FAILS (RED) until fixed. */
    ES_LangProj lp;
    cbm_store_t *store = es_lang_index_files(&lp, f, 2);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, "INHERITS") : -1;
    if (got >= 1) {
        fprintf(stderr, "  [ES-EDGE] UNEXPECTED PASS: PHP cross-file INHERITS got=%d "
                        "(promote to GREEN)\n", got);
    } else {
        fprintf(stderr, "  [ES-EDGE] CONFIRMED RED: PHP cross-file INHERITS got=%d\n", got);
    }
    es_lang_cleanup(&lp, store);
    ASSERT_TRUE(got >= 1); /* FAILS (RED) until PHP extraction fixed */
    PASS();
}

/* Kotlin cross-file INHERITS — expected RED (`:` supertype syntax not parsed). */
TEST(es_inherits_crossfile_kotlin_red) {
    static const ES_LangFile f[] = {
        {"Base.kt",
         "open class Base {\n    open fun value(): Int = 0\n}\n"},
        {"Child.kt",
         "class Child : Base() {\n    override fun value(): Int = 1\n}\n"}};
    /* RED: Kotlin extractor does not parse `:` supertype syntax → base_classes empty.
     * Fix location: extract_defs.c Kotlin class body / supertype_list handling.
     * FAILS (RED) until fixed. */
    ES_LangProj lp;
    cbm_store_t *store = es_lang_index_files(&lp, f, 2);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, "INHERITS") : -1;
    if (got >= 1) {
        fprintf(stderr, "  [ES-EDGE] UNEXPECTED PASS: Kotlin cross-file INHERITS got=%d "
                        "(promote to GREEN)\n", got);
    } else {
        fprintf(stderr, "  [ES-EDGE] CONFIRMED RED: Kotlin cross-file INHERITS got=%d\n", got);
    }
    es_lang_cleanup(&lp, store);
    ASSERT_TRUE(got >= 1); /* FAILS (RED) until Kotlin extraction fixed */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 3: IMPLEMENTS cross-file (Rust trait + struct)
 *
 * Trait defined in a.rs, impl block in b.rs.
 * resolve_impl_traits() looks up both trait_name and struct_name in the
 * project-wide registry.  Expected GREEN when both files are indexed.
 * ══════════════════════════════════════════════════════════════════ */

/* Rust cross-file IMPLEMENTS: trait in trait.rs, impl in impl.rs. */
TEST(es_implements_crossfile_rust) {
    static const ES_LangFile f[] = {
        {"trait.rs",
         "pub trait Greet {\n    fn hello(&self) -> String;\n}\n"},
        {"impl.rs",
         "use crate::trait::Greet;\n\npub struct English;\n\n"
         "impl Greet for English {\n"
         "    fn hello(&self) -> String {\n        String::from(\"hi\")\n    }\n}\n"}};
    /* GREEN: resolve_impl_traits() uses project-wide registry; both Greet
     * (trait.rs) and English (impl.rs) should resolve after definitions pass. */
    ASSERT_TRUE(es_edge_present(f, 2, "IMPLEMENTS", 1)); /* English implements Greet */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 4: IMPLEMENTS same-file (interface / implicit satisfaction)
 *
 * Java `implements`, C# `:` interface, Go implicit method-set satisfaction.
 * ══════════════════════════════════════════════════════════════════ */

/* Java same-file class implements interface — edge depends on whether
 * pass_semantic / pass_parallel distinguishes interface target from class
 * target in base_classes.  Uncertain — annotated but tested. */
TEST(es_implements_samefile_java) {
    static const ES_LangFile f[] = {
        {"Service.java",
         "package app;\n\ninterface Runnable {\n    void run();\n}\n\n"
         "class Service implements Runnable {\n    public void run() {}\n}\n"}};
    /* Uncertain: Java extraction stores both `extends` and `implements` targets
     * in base_classes[].  The semantic pass may emit INHERITS (not IMPLEMENTS)
     * for all of them, or it may check the target node's label.  If the
     * Interface node is created and the pass distinguishes it → IMPLEMENTS edge.
     * If not → this reproduces a gap.  Assert the CORRECT outcome. */
    ASSERT_TRUE(es_edge_present(f, 1, "IMPLEMENTS", 1)); /* Service implements Runnable */
    PASS();
}

/* C# same-file class implements interface. */
TEST(es_implements_samefile_csharp) {
    static const ES_LangFile f[] = {
        {"Worker.cs",
         "namespace App {\n    interface IWorker {\n        void Work();\n    }\n\n"
         "    class Worker : IWorker {\n        public void Work() {}\n    }\n}\n"}};
    /* Uncertain: C# `:` syntax used for both inheritance and interface impl.
     * The semantic pass must check whether the resolved node is an Interface.
     * Assert the correct outcome; RED if pass doesn't distinguish. */
    ASSERT_TRUE(es_edge_present(f, 1, "IMPLEMENTS", 1)); /* Worker implements IWorker */
    PASS();
}

/* Go implicit interface satisfaction — pass_semantic.c
 * cbm_pipeline_implements_go() checks method-set coverage. */
TEST(es_implements_go_implicit) {
    static const ES_LangFile f[] = {
        {"iface.go",
         "package app\n\ntype Stringer interface {\n    String() string\n}\n"},
        {"impl.go",
         "package app\n\ntype MyType struct{ val string }\n\n"
         "func (m MyType) String() string {\n    return m.val\n}\n"}};
    /* GREEN: cbm_pipeline_implements_go() finds MyType satisfies Stringer
     * (both methods present in the same index), emits IMPLEMENTS. */
    ASSERT_TRUE(es_edge_present(f, 2, "IMPLEMENTS", 1)); /* MyType implements Stringer */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 5: OVERRIDE (Go interface method)
 *
 * When Go's method-set satisfaction creates IMPLEMENTS, it also creates
 * an OVERRIDE edge from each implementing method to the interface method.
 * ══════════════════════════════════════════════════════════════════ */

TEST(es_override_go_implicit) {
    static const ES_LangFile f[] = {
        {"iface.go",
         "package app\n\ntype Namer interface {\n    Name() string\n}\n"},
        {"impl.go",
         "package app\n\ntype Entity struct{ name string }\n\n"
         "func (e Entity) Name() string {\n    return e.name\n}\n"}};
    /* GREEN: cbm_pipeline_implements_go() emits OVERRIDE for Entity.Name -> Namer.Name. */
    ASSERT_TRUE(es_edge_present(f, 2, "OVERRIDE", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 6: DECORATES same-file and cross-file
 *
 * Python decorators (same-file covered by existing P6; cross-file is new).
 * TypeScript class decorators.
 * Java/Kotlin/C# annotations (uncertain — tested to discover gaps).
 * ══════════════════════════════════════════════════════════════════ */

/* Python DECORATES cross-file: decorator defined in decorators.py,
 * applied in service.py via relative import. */
TEST(es_decorates_crossfile_python) {
    static const ES_LangFile f[] = {
        {"decorators.py",
         "def log_call(func):\n    def wrapper(*args, **kwargs):\n"
         "        return func(*args, **kwargs)\n    return wrapper\n"},
        {"service.py",
         "from .decorators import log_call\n\n\n"
         "@log_call\ndef process(x):\n    return x * 2\n"}};
    /* GREEN: relative import resolves (Python OK per P3), decorator name
     * resolves in registry → DECORATES edge process -> log_call. */
    ASSERT_TRUE(es_edge_present(f, 2, "DECORATES", 1));
    PASS();
}

/* TypeScript class decorator — same-file. */
TEST(es_decorates_samefile_typescript) {
    static const ES_LangFile f[] = {
        {"service.ts",
         "function Injectable(target: any): any { return target; }\n\n"
         "@Injectable\nexport class UserService {\n    getUser(id: string) { return id; }\n}\n"}};
    /* Uncertain: TS extraction may or may not populate decorators[] for class
     * decorators.  Assert the correct outcome; RED if TS class decorator
     * extraction is unimplemented. */
    ASSERT_TRUE(es_edge_present(f, 1, "DECORATES", 1));
    PASS();
}

/* Java annotation — same-file.
 * Annotations are syntactically similar to decorators; uncertain if the
 * pipeline emits a DECORATES edge for Java @Annotation. */
TEST(es_decorates_annotation_java) {
    static const ES_LangFile f[] = {
        {"Service.java",
         "package app;\n\nimport java.lang.annotation.*;\n\n"
         "@Retention(RetentionPolicy.RUNTIME)\n@interface Override {}\n\n"
         "class Service {\n    @Override\n    public String toString() { return \"service\"; }\n}\n"}};
    /* Uncertain/RED: Java extraction likely does not populate decorators[] for
     * @Annotation syntax (no Java branch in the decorator extractor confirmed).
     * This test probes whether any DECORATES edge is created.
     * Expected to be RED (count=0) — reproduces the gap if so. */
    ES_LangProj lp;
    cbm_store_t *store = es_lang_index_files(&lp, f, 1);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, "DECORATES") : -1;
    fprintf(stderr, "  [ES-EDGE] Java annotation DECORATES got=%d (expected 0 if unimplemented; "
                    "promote to GREEN if edge appears)\n", got);
    es_lang_cleanup(&lp, store);
    /* Assert the CORRECT outcome: annotated method should produce DECORATES. */
    ASSERT_TRUE(got >= 1); /* RED until Java annotation extraction implemented */
    PASS();
}

/* Kotlin annotation — same-file. */
TEST(es_decorates_annotation_kotlin) {
    static const ES_LangFile f[] = {
        {"Service.kt",
         "annotation class Log\n\n"
         "@Log\nfun process(x: Int): Int = x * 2\n"}};
    /* Uncertain/RED: Kotlin decorator/annotation extraction unclear.
     * FAILS (RED) until Kotlin annotation DECORATES is implemented. */
    ES_LangProj lp;
    cbm_store_t *store = es_lang_index_files(&lp, f, 1);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, "DECORATES") : -1;
    fprintf(stderr, "  [ES-EDGE] Kotlin annotation DECORATES got=%d\n", got);
    es_lang_cleanup(&lp, store);
    ASSERT_TRUE(got >= 1); /* RED until implemented */
    PASS();
}

/* C# attribute — same-file. */
TEST(es_decorates_attribute_csharp) {
    static const ES_LangFile f[] = {
        {"Service.cs",
         "using System;\n\n[AttributeUsage(AttributeTargets.Method)]\nclass LogAttribute : Attribute {}\n\n"
         "namespace App {\n    class Service {\n        [Log]\n"
         "        public int Process(int x) { return x * 2; }\n    }\n}\n"}};
    /* Uncertain/RED: C# attribute extraction unclear.
     * FAILS (RED) until C# attribute DECORATES is implemented. */
    ES_LangProj lp;
    cbm_store_t *store = es_lang_index_files(&lp, f, 1);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, "DECORATES") : -1;
    fprintf(stderr, "  [ES-EDGE] C# attribute DECORATES got=%d\n", got);
    es_lang_cleanup(&lp, store);
    ASSERT_TRUE(got >= 1); /* RED until implemented */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 7: USAGE cross-file (type/symbol reference)
 *
 * USAGE edges are created when a type or identifier reference (not a call)
 * resolves in the registry.  pass_usages.c handles this.
 * ══════════════════════════════════════════════════════════════════ */

/* Python cross-file USAGE: main.py uses a class from models.py. */
TEST(es_usage_crossfile_python) {
    static const ES_LangFile f[] = {
        {"models.py",
         "class User:\n    def __init__(self, name):\n        self.name = name\n"},
        {"main.py",
         "from .models import User\n\n\ndef create_user(name):\n    return User(name)\n"}};
    /* Uncertain: USAGE edges for type instantiation may or may not be
     * extracted for Python.  Assert the correct outcome. */
    ASSERT_TRUE(es_edge_present(f, 2, "USAGE", 1));
    PASS();
}

/* TypeScript cross-file USAGE: main.ts uses a type from types.ts. */
TEST(es_usage_crossfile_typescript) {
    static const ES_LangFile f[] = {
        {"types.ts",
         "export interface Config {\n    timeout: number;\n}\n"},
        {"main.ts",
         "import { Config } from './types';\n\n"
         "export function create(cfg: Config): string {\n    return String(cfg.timeout);\n}\n"}};
    /* Uncertain: TS USAGE edges for type-only references (interface used as
     * a parameter type annotation).  Assert the correct outcome. */
    ASSERT_TRUE(es_edge_present(f, 2, "USAGE", 1));
    PASS();
}

/* Go cross-file USAGE: main.go references a struct type from types.go. */
TEST(es_usage_crossfile_go) {
    static const ES_LangFile f[] = {
        {"types.go",
         "package app\n\ntype Config struct {\n    Timeout int\n}\n"},
        {"main.go",
         "package app\n\nfunc Create(cfg Config) int {\n    return cfg.Timeout\n}\n"}};
    /* Uncertain: Go USAGE edges for struct type references in function
     * signatures.  Assert the correct outcome. */
    ASSERT_TRUE(es_edge_present(f, 2, "USAGE", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 8: DATA_FLOWS cross-file
 *
 * DATA_FLOWS is created when an HTTP caller (HTTP_CALLS) and a route
 * handler (HANDLES) are linked through the same Route node.  Cross-file
 * variant: handler in app.py, HTTP caller in client.py.
 * ══════════════════════════════════════════════════════════════════ */

TEST(es_data_flows_crossfile_python) {
    static const ES_LangFile f[] = {
        {"app.py",
         "from flask import Flask\n\napp = Flask(__name__)\n\n\n"
         "@app.route(\"/items\")\ndef list_items():\n    return {\"items\": []}\n"},
        {"client.py",
         "def requests_get(url, params=None):\n    return {\"url\": url}\n\n\n"
         "def fetch_items():\n    return requests_get(\"/items\")\n"}};
    /* GREEN: HANDLES created from app.py route decorator; HTTP_CALLS from
     * client.py requests_get("/items"); DATA_FLOWS links them via the same
     * route path.  Cross-file should work as well as same-file (P6 confirms
     * same-file is GREEN). */
    ASSERT_TRUE(es_edge_present(f, 2, "DATA_FLOWS", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 9: TESTS cross-file (additional languages beyond Go)
 *
 * pass_tests.c creates TESTS edges from test functions to production
 * functions when a *_test file calls a function in a non-test file.
 * Existing P6 covers Go.  This suite adds Python and TypeScript.
 * ══════════════════════════════════════════════════════════════════ */

/* Python TESTS cross-file: test_service.py tests service.py. */
TEST(es_tests_crossfile_python) {
    static const ES_LangFile f[] = {
        {"service.py",
         "def add(a, b):\n    return a + b\n\n\ndef multiply(a, b):\n    return a * b\n"},
        {"test_service.py",
         "from service import add, multiply\n\n\n"
         "def test_add():\n    result = add(1, 2)\n    assert result == 3\n\n\n"
         "def test_multiply():\n    result = multiply(2, 3)\n    assert result == 6\n"}};
    /* GREEN: Python test_ prefix convention, TESTS_FILE + TESTS expected. */
    ASSERT_TRUE(es_edge_present(f, 2, "TESTS", 1)); /* test_add->add, test_multiply->multiply */
    PASS();
}

/* TypeScript TESTS cross-file: service.test.ts tests service.ts. */
TEST(es_tests_crossfile_typescript) {
    static const ES_LangFile f[] = {
        {"service.ts",
         "export function divide(a: number, b: number): number {\n    return a / b;\n}\n\n"
         "export function subtract(a: number, b: number): number {\n    return a - b;\n}\n"},
        {"service.test.ts",
         "import { divide, subtract } from './service';\n\n"
         "function testDivide() {\n    const r = divide(6, 2);\n}\n\n"
         "function testSubtract() {\n    const r = subtract(5, 3);\n}\n"}};
    /* GREEN: TS .test. prefix convention; TESTS_FILE + TESTS expected. */
    ASSERT_TRUE(es_edge_present(f, 2, "TESTS", 1)); /* testDivide->divide etc. */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * SUITE registration
 * ══════════════════════════════════════════════════════════════════ */

SUITE(edge_structural) {
    /* ── FAMILY 1: CALLS cross-file (all 9 hybrid-LSP languages) ─ */
    /* All expected GREEN: registry is project-wide, cross-file == same-file. */
    RUN_TEST(es_calls_crossfile_go);
    RUN_TEST(es_calls_crossfile_c);
    RUN_TEST(es_calls_crossfile_cpp);
    RUN_TEST(es_calls_crossfile_rust);
    RUN_TEST(es_calls_crossfile_python);
    RUN_TEST(es_calls_crossfile_typescript);
    RUN_TEST(es_calls_crossfile_java);
    RUN_TEST(es_calls_crossfile_kotlin);
    RUN_TEST(es_calls_crossfile_csharp);

    /* ── FAMILY 2: INHERITS cross-file ────────────────────────── */
    /* GREEN: Java, C#, C++ (extraction confirmed correct). */
    RUN_TEST(es_inherits_crossfile_java);
    RUN_TEST(es_inherits_crossfile_csharp);
    RUN_TEST(es_inherits_crossfile_cpp);
    /* RED: Python, TypeScript, PHP, Kotlin (extraction bugs). */
    RUN_TEST(es_inherits_crossfile_python_red);
    RUN_TEST(es_inherits_crossfile_typescript_red);
    RUN_TEST(es_inherits_crossfile_php_red);
    RUN_TEST(es_inherits_crossfile_kotlin_red);

    /* ── FAMILY 3: IMPLEMENTS cross-file (Rust) ──────────────── */
    /* Expected GREEN: project-wide registry covers both files. */
    RUN_TEST(es_implements_crossfile_rust);

    /* ── FAMILY 4: IMPLEMENTS same-file interface ────────────── */
    /* Uncertain: depends on semantic pass distinguishing Class vs Interface. */
    RUN_TEST(es_implements_samefile_java);
    RUN_TEST(es_implements_samefile_csharp);
    RUN_TEST(es_implements_go_implicit);

    /* ── FAMILY 5: OVERRIDE (Go implicit interface method) ───── */
    /* Expected GREEN: cbm_pipeline_implements_go() emits OVERRIDE. */
    RUN_TEST(es_override_go_implicit);

    /* ── FAMILY 6: DECORATES ─────────────────────────────────── */
    /* GREEN: cross-file Python (relative import + registry resolution). */
    RUN_TEST(es_decorates_crossfile_python);
    /* Uncertain: TypeScript class decorator. */
    RUN_TEST(es_decorates_samefile_typescript);
    /* RED: Java/Kotlin/C# annotation → DECORATES (unimplemented extractors). */
    RUN_TEST(es_decorates_annotation_java);
    RUN_TEST(es_decorates_annotation_kotlin);
    RUN_TEST(es_decorates_attribute_csharp);

    /* ── FAMILY 7: USAGE cross-file ─────────────────────────── */
    /* Uncertain: depends on USAGE extraction for each language. */
    RUN_TEST(es_usage_crossfile_python);
    RUN_TEST(es_usage_crossfile_typescript);
    RUN_TEST(es_usage_crossfile_go);

    /* ── FAMILY 8: DATA_FLOWS cross-file ─────────────────────── */
    /* Expected GREEN: route-intermediated path, language-independent. */
    RUN_TEST(es_data_flows_crossfile_python);

    /* ── FAMILY 9: TESTS cross-file ──────────────────────────── */
    /* Expected GREEN: Python + TypeScript test file conventions. */
    RUN_TEST(es_tests_crossfile_python);
    RUN_TEST(es_tests_crossfile_typescript);
}
