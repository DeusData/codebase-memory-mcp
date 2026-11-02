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
#include "resolver_quality.h"
#include "cbm.h"
#include "pipeline/pipeline_internal.h"
#include "resolve/policy.h"
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
static const char *ES_ALL_EDGE_TYPES[] = {"CALLS",
                                          "CALL_REFERENCE",
                                          "CONFIGURES",
                                          "CONTAINS_FILE",
                                          "CONTAINS_FOLDER",
                                          "DATA_FLOWS",
                                          "DECORATES",
                                          "DEFINES",
                                          "DEFINES_METHOD",
                                          "DEPENDS_ON",
                                          "FILE_CHANGES_WITH",
                                          "GRAPHQL_CALLS",
                                          "GRPC_CALLS",
                                          "HANDLES",
                                          "HTTP_CALLS",
                                          "IMPLEMENTS",
                                          "IMPORTS",
                                          "INHERITS",
                                          "INFRA_MAPS",
                                          "MENTIONS",
                                          "OVERRIDE",
                                          "REFERENCES_FILE",
                                          "SEMANTICALLY_RELATED",
                                          "SIMILAR_TO",
                                          "TESTS_FILE",
                                          "TESTS",
                                          "TRPC_CALLS",
                                          "USAGE",
                                          "ASYNC_CALLS",
                                          NULL};

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

static int es_exact_edge_by_name(const ES_LangFile *files, int nfiles, const char *edge_type,
                                 const char *source_name, const char *target_name) {
    ES_LangProj lp;
    cbm_store_t *store = es_lang_index_files(&lp, files, nfiles);
    cbm_node_t *sources = NULL;
    cbm_node_t *targets = NULL;
    int source_count = 0;
    int target_count = 0;
    int matches = -1;
    if (store &&
        cbm_store_find_nodes_by_name(store, lp.project, source_name, &sources, &source_count) ==
            CBM_STORE_OK &&
        cbm_store_find_nodes_by_name(store, lp.project, target_name, &targets, &target_count) ==
            CBM_STORE_OK &&
        source_count == 1 && target_count == 1) {
        cbm_edge_t *edges = NULL;
        int edge_count = 0;
        if (cbm_store_find_edges_by_source_type(store, sources[0].id, edge_type, &edges,
                                                &edge_count) == CBM_STORE_OK) {
            matches = 0;
            for (int i = 0; i < edge_count; i++) {
                if (edges[i].target_id == targets[0].id) {
                    matches++;
                }
            }
        }
        cbm_store_free_edges(edges, edge_count);
    }
    cbm_store_free_nodes(sources, source_count);
    cbm_store_free_nodes(targets, target_count);
    es_lang_cleanup(&lp, store);
    return matches;
}

/* ══════════════════════════════════════════════════════════════════
 * FAMILY 1: CALLS cross-file
 *
 * Function defined in file A, called from file B.  The registry is
 * project-wide so cross-file is identical to same-file for the resolver.
 * ALL 9 hybrid-LSP languages are expected GREEN.
 * ══════════════════════════════════════════════════════════════════ */

/* These checks require both declarations even for an abstention. An empty
 * graph cannot satisfy a negative, and every outgoing inheritance edge must
 * name the independently specified source file and class. */
static int es_exact_inheritance(cbm_store_t *store, const char *project, const char *source_file,
                                const char *source_name, const char *target_file,
                                const char *target_name) {
    cbm_node_t *sources = NULL, *targets = NULL;
    int source_count = 0, target_count = 0, answer = -1;
    if (!store ||
        cbm_store_find_nodes_by_name(store, project, source_name, &sources, &source_count) !=
            CBM_STORE_OK ||
        cbm_store_find_nodes_by_name(store, project, target_name, &targets, &target_count) !=
            CBM_STORE_OK)
        goto done;
    int64_t source = 0, target = 0;
    for (int i = 0; i < source_count; i++)
        if (sources[i].file_path && sources[i].label &&
            !strcmp(sources[i].file_path, source_file) && !strcmp(sources[i].label, "Class")) {
            if (source)
                goto done;
            source = sources[i].id;
        }
    for (int i = 0; i < target_count; i++)
        if (targets[i].file_path && targets[i].label &&
            !strcmp(targets[i].file_path, target_file) && !strcmp(targets[i].label, "Class")) {
            if (target)
                goto done;
            target = targets[i].id;
        }
    if (!source || !target)
        goto done;
    cbm_edge_t *edges = NULL;
    int count = 0;
    if (cbm_store_find_edges_by_source_type(store, source, "INHERITS", &edges, &count) ==
        CBM_STORE_OK) {
        answer = count;
        for (int i = 0; i < count; i++)
            if (edges[i].target_id != target)
                answer = -1;
    }
    cbm_store_free_edges(edges, count);
done:
    cbm_store_free_nodes(sources, source_count);
    cbm_store_free_nodes(targets, target_count);
    return answer;
}

static bool es_inheritance_venues(const ES_LangFile *files, int count, const char *extension,
                                  const char *source_file, const char *source_name,
                                  const char *target_file, const char *target_name, int expected) {
    enum { PADDING = 55, MAX_FIXTURE = 8 };
    if (count < 1 || count > MAX_FIXTURE)
        return false;
    for (int parallel = 0; parallel < 2; parallel++) {
        ES_LangFile all[MAX_FIXTURE + PADDING];
        char names[PADDING][64], sources[PADDING][96];
        memcpy(all, files, (size_t)count * sizeof(*files));
        for (int i = 0; parallel && i < PADDING; i++) {
            snprintf(names[i], sizeof(names[i]), "padding/file_%02d.%s", i, extension);
            if (!strcmp(extension, "kt"))
                snprintf(sources[i], sizeof(sources[i]),
                         "package structuralpadding\nfun padding%02d(): Int = %d\n", i, i);
            else
                snprintf(sources[i], sizeof(sources[i]), "export const padding = %d;\n", i);
            all[count + i] = (ES_LangFile){names[i], sources[i]};
        }
        ES_LangProj project;
        cbm_store_t *store = es_lang_index_files(&project, all, count + parallel * PADDING);
        int found = es_exact_inheritance(store, project.project, source_file, source_name,
                                         target_file, target_name);
        es_lang_cleanup(&project, store);
        if (found != expected) {
            fprintf(stderr, "  [ES-HERITAGE] %s padding=%d exact=%d expected=%d\n", source_file,
                    parallel * PADDING, found, expected);
            return false;
        }
    }
    return true;
}

TEST(es_inherits_typescript_exact_import_context) {
    static const ES_LangFile files[] = {
        {"base.ts", "export class Base { value(): number { return 0; } }\n"},
        {"derived.ts", "import { Base } from './base';\nexport class Derived extends Base {}\n"},
        {"decoy/base.ts", "export class Base {}\n"}};
    ASSERT_TRUE(
        es_inheritance_venues(files, 3, "ts", "derived.ts", "Derived", "base.ts", "Base", 1));
    PASS();
}

TEST(es_inherits_typescript_missing_import_does_not_borrow_decoy) {
    static const ES_LangFile files[] = {
        {"base.ts", "export class Base {}\n"},
        {"derived.ts",
         "import { Base } from './missing';\nexport class Derived extends Base {}\n"}};
    ASSERT_TRUE(
        es_inheritance_venues(files, 2, "ts", "derived.ts", "Derived", "base.ts", "Base", 0));
    PASS();
}

TEST(es_inherits_typescript_ambiguous_import_context) {
    static const ES_LangFile files[] = {
        {"one.ts", "export class Base {}\n"},
        {"two.ts", "export class Base {}\n"},
        {"derived.ts", "import { Base } from './one';\nimport { Base } from './two';\n"
                       "export class Derived extends Base {}\n"}};
    ASSERT_TRUE(
        es_inheritance_venues(files, 3, "ts", "derived.ts", "Derived", "one.ts", "Base", 0));
    PASS();
}

/* INHERITS lies outside the seven controlled resolver relations. Keep this
 * source-authored obligation in an explicit structural quality denominator.
 * The compact expression-body fixture is valid source, but the current
 * grammar reports errors in both class subtrees. v96 confirmed those exact
 * raw trees after source-digest validation; the parse gate must not be waived.
 * A same-file required edge and snapshot checks prevent blank-graph success. */
static bool es_kotlin_compact_heritage_quality_gap(const ES_LangFile *files, int count) {
    enum { PADDING = 55, MAX_FILES = 8 };
    static const ES_LangFile control = {
        "Quality.kt", "open class QualityBase\nclass QualityChild : QualityBase()\n"};
    if (count < 1 || count >= MAX_FILES)
        return false;
    for (int parallel = 0; parallel < 2; parallel++) {
        ES_LangFile all[MAX_FILES + PADDING];
        memcpy(all, files, (size_t)count * sizeof(*files));
        all[count] = control;
        char names[PADDING][64], contents[PADDING][96];
        for (int i = 0; parallel && i < PADDING; i++) {
            snprintf(names[i], sizeof(names[i]), "padding/gap_%02d.kt", i);
            snprintf(contents[i], sizeof(contents[i]),
                     "package structuralpadding\nfun padding%02d(): Int = %d\n", i, i);
            all[count + 1 + i] = (ES_LangFile){names[i], contents[i]};
        }
        ES_LangProj project;
        cbm_store_t *store = es_lang_index_files(&project, all, count + 1 + parallel * PADDING);
        sqlite3 *db = store ? cbm_store_get_db(store) : NULL;
        bool snapshots = db && rq_generation(db, project.project);
        for (int i = 0; snapshots && i <= count; i++) {
            RQ_Source source = {.path = all[i].name, .text = all[i].content};
            snapshots = rq_snapshot(db, project.project, &source);
        }
        int gap =
            es_exact_inheritance(store, project.project, "Child.kt", "Child", "Base.kt", "Base");
        int required = es_exact_inheritance(store, project.project, "Quality.kt", "QualityChild",
                                            "Quality.kt", "QualityBase");
        es_lang_cleanup(&project, store);
        fprintf(stderr,
                "[RQ-HERITAGE] gap=kotlin-compact-expression-body-parse "
                "genuine=1 found=%d missing=%d required_control=%d padding=%d "
                "tried=raw-tree-and-source-snapshot-audit\n",
                gap, gap == 0 ? 1 : 0, required, parallel * PADDING);
        /* A recovered gap needs source-proof review before promotion; any
         * guessed or wrong-target edge fails es_exact_inheritance above. */
        if (!snapshots || gap != 0 || required != 1)
            return false;
    }
    return true;
}

TEST(es_inherits_kotlin_exact_declared_package_context) {
    static const ES_LangFile default_package[] = {
        {"Base.kt", "open class Base { open fun value(): Int = 0 }\n"},
        {"Child.kt", "class Child : Base() { override fun value(): Int = 1 }\n"},
        {"decoy/Base.kt", "package other\nopen class Base\n"}};
    static const ES_LangFile declared_package[] = {
        {"unrelated/Base.kt", "package zoo\nopen class Base\n"},
        {"different/Child.kt", "package zoo\nclass Child : Base()\n"},
        {"zoo/Base.kt", "package other\nopen class Base\n"}};
    static const ES_LangFile multiline_default[] = {
        {"Base.kt", "open class Base {\n    open fun value(): Int = 0\n}\n"},
        {"Child.kt", "class Child : Base() {\n    override fun value(): Int = 1\n}\n"},
        {"decoy/Base.kt", "package other\nopen class Base\n"}};
    ASSERT_TRUE(es_kotlin_compact_heritage_quality_gap(default_package, 3));
    ASSERT_TRUE(es_inheritance_venues(multiline_default, 3, "kt", "Child.kt", "Child", "Base.kt",
                                      "Base", 1));
    ASSERT_TRUE(es_inheritance_venues(declared_package, 3, "kt", "different/Child.kt", "Child",
                                      "unrelated/Base.kt", "Base", 1));
    PASS();
}

TEST(es_inherits_kotlin_missing_package_member_does_not_borrow_decoy) {
    static const ES_LangFile files[] = {{"zoo/Base.kt", "package other\nopen class Base\n"},
                                        {"Child.kt", "package zoo\nclass Child : Base()\n"}};
    ASSERT_TRUE(
        es_inheritance_venues(files, 2, "kt", "Child.kt", "Child", "zoo/Base.kt", "Base", 0));
    PASS();
}

TEST(es_inherits_kotlin_duplicate_package_member_abstains) {
    static const ES_LangFile files[] = {{"one/Base.kt", "package zoo\nopen class Base\n"},
                                        {"two/Base.kt", "package zoo\nopen class Base\n"},
                                        {"Child.kt", "package zoo\nclass Child : Base()\n"}};
    ASSERT_TRUE(
        es_inheritance_venues(files, 3, "kt", "Child.kt", "Child", "one/Base.kt", "Base", 0));
    PASS();
}

TEST(es_inherits_kotlin_private_package_member_abstains) {
    static const ES_LangFile files[] = {{"Base.kt", "private open class Base\n"},
                                        {"Child.kt", "class Child : Base()\n"}};
    ASSERT_TRUE(es_inheritance_venues(files, 2, "kt", "Child.kt", "Child", "Base.kt", "Base", 0));
    PASS();
}

/* The fixture supplies exact source/target identities. Both pipeline drivers
 * must retain the required control while unsupported bindings remain counted. */
static bool es_binding_venues(const ES_LangFile *files, int count, const char *case_id,
                              const RQ_Expected *expected, size_t expected_count,
                              const RQ_Forbidden *forbidden, size_t forbidden_count) {
    enum { MAX_FILES = 8, MAX_EXPECTED = 8, PADDING = 55 };
    static const ES_LangFile control = {
        "binding_control.mjs", "export function provenTarget() { return 1; }\n"
                               "export function provenCaller() { return provenTarget(); }\n"};
    if (count < 1 || count >= MAX_FILES || expected_count >= MAX_EXPECTED)
        return false;
    for (int parallel = 0; parallel < 2; parallel++) {
        ES_LangFile all[MAX_FILES + PADDING];
        RQ_Source sources[MAX_FILES];
        memcpy(all, files, (size_t)count * sizeof(*files));
        all[count] = control;
        for (int i = 0; i <= count; i++)
            sources[i] = (RQ_Source){.path = all[i].name, .text = all[i].content};
        char names[PADDING][64], texts[PADDING][64];
        for (int i = 0; parallel && i < PADDING; i++) {
            snprintf(names[i], sizeof(names[i]), "padding/pad_%02d.mjs", i);
            snprintf(texts[i], sizeof(texts[i]), "export const padding = %d;\n", i);
            all[count + 1 + i] = (ES_LangFile){names[i], texts[i]};
        }
        RQ_Expected golden[MAX_EXPECTED] = {
            {.case_id = "required-same-pipeline-control",
             .site = {.file = "binding_control.mjs",
                      .caller = "provenCaller",
                      .needle = "provenTarget()",
                      .occurrence_index = 1},
             .relation = "CALLS",
             .target = {.file = "binding_control.mjs",
                        .suffix = "provenTarget",
                        .label = "Function",
                        .declaration_line = 1,
                        .declaration_text = "export function provenTarget() { return 1; }"},
             .mode = RQ_REQUIRED}};
        for (size_t i = 0; i < expected_count; i++)
            golden[i + 1] = expected[i];
        ES_LangProj project;
        cbm_store_t *store = es_lang_index_files(&project, all, count + 1 + parallel * PADDING);
        RQ_Report report;
        bool ok = rq_check(store, project.project, case_id, sources, (size_t)count + 1, golden,
                           expected_count + 1, forbidden, forbidden_count, &report, stderr);
        es_lang_cleanup(&project, store);
        if (!ok)
            return false;
    }
    return true;
}

/* INHERITS/IMPLEMENTS are checked separately from the seven resolver relations.
 * Require both declarations and close the source's entire relation domain. */
static bool es_structural_venues(const ES_LangFile *files, int count, const char *relation,
                                 const char *source_file, const char *source_name,
                                 const char *target_file, const char *target_name, bool required,
                                 const char *gap_id) {
    enum { MAX_FILES = 8, PADDING = 55 };
    static const ES_LangFile control = {"structural_control.mjs",
                                        "export function target() { return 1; }\n"
                                        "export function caller() { return target(); }\n"};
    static const RQ_Expected golden = {
        .case_id = "structural-required-call-control",
        .site = {.file = "structural_control.mjs",
                 .caller = "caller",
                 .needle = "target()",
                 .occurrence_index = 1},
        .relation = "CALLS",
        .target = {.file = "structural_control.mjs",
                   .suffix = "target",
                   .label = "Function",
                   .declaration_line = 1,
                   .declaration_text = "export function target() { return 1; }"},
        .mode = RQ_REQUIRED};
    if (count < 1 || count >= MAX_FILES)
        return false;
    for (int parallel = 0; parallel < 2; parallel++) {
        ES_LangFile all[MAX_FILES + PADDING];
        RQ_Source sources[MAX_FILES];
        memcpy(all, files, (size_t)count * sizeof(*files));
        all[count] = control;
        for (int i = 0; i <= count; i++)
            sources[i] = (RQ_Source){.path = all[i].name, .text = all[i].content};
        char names[PADDING][64], texts[PADDING][64];
        for (int i = 0; parallel && i < PADDING; i++) {
            snprintf(names[i], sizeof(names[i]), "padding/pad_%02d.mjs", i);
            snprintf(texts[i], sizeof(texts[i]), "export const padding = %d;\n", i);
            all[count + 1 + i] = (ES_LangFile){names[i], texts[i]};
        }
        ES_LangProj project;
        cbm_store_t *store = es_lang_index_files(&project, all, count + 1 + parallel * PADDING);
        RQ_Report report;
        bool ok = rq_check(store, project.project, "structural-control", sources, (size_t)count + 1,
                           &golden, 1, NULL, 0, &report, stderr);
        cbm_node_t *from = NULL, *to = NULL;
        int from_count = 0, to_count = 0;
        int64_t source_id = 0, target_id = 0;
        int matched_sources = 0, matched_targets = 0;
        if (!store ||
            cbm_store_find_nodes_by_name(store, project.project, source_name, &from, &from_count) !=
                CBM_STORE_OK ||
            cbm_store_find_nodes_by_name(store, project.project, target_name, &to, &to_count) !=
                CBM_STORE_OK)
            ok = false;
        for (int i = 0; i < from_count; i++)
            if (from[i].file_path && !strcmp(from[i].file_path, source_file) &&
                cbm_label_is_type_like(from[i].label)) {
                source_id = from[i].id;
                matched_sources++;
            }
        for (int i = 0; i < to_count; i++)
            if (to[i].file_path && !strcmp(to[i].file_path, target_file) &&
                cbm_label_is_type_like(to[i].label)) {
                target_id = to[i].id;
                matched_targets++;
            }
        if (matched_sources != 1 || matched_targets != 1)
            ok = false;
        cbm_edge_t *edges = NULL;
        int edge_count = 0, found = 0, wrong = 0;
        if (!source_id || cbm_store_find_edges_by_source_type(store, source_id, relation, &edges,
                                                              &edge_count) != CBM_STORE_OK)
            ok = false;
        for (int i = 0; i < edge_count; i++) {
            if (edges[i].target_id == target_id)
                found++;
            else
                wrong++;
        }
        if (wrong || found != (required ? 1 : 0))
            ok = false;
        fprintf(stderr,
                "[ES-STRUCTURAL] relation=%s source=%s target=%s padding=%d "
                "genuine=%d found=%d missing=%d wrong=%d gap=%s\n",
                relation, source_file, target_file, parallel * PADDING, required || gap_id ? 1 : 0,
                found, (required || gap_id) && !found ? 1 : 0, wrong, gap_id ? gap_id : "none");
        cbm_store_free_edges(edges, edge_count);
        cbm_store_free_nodes(from, from_count);
        cbm_store_free_nodes(to, to_count);
        es_lang_cleanup(&project, store);
        if (!ok)
            return false;
    }
    return true;
}

/* Go: caller in main.go, callee in util.go — same package. */
TEST(es_calls_crossfile_go) {
    static const ES_LangFile f[] = {
        {"util.go", "package svc\n\nfunc Compute(x int) int {\n\treturn x * 2\n}\n"},
        {"main.go", "package svc\n\nfunc Run(y int) int {\n\treturn Compute(y + 1)\n}\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* Run -> Compute */
    PASS();
}

/* C: caller in main.c, callee declared/defined in util.c. */
TEST(es_calls_crossfile_c) {
    static const ES_LangFile f[] = {
        {"util.c", "int add(int a, int b) {\n    return a + b;\n}\n"},
        {"main.c", "int add(int a, int b);\n\nint run(int x) {\n    return add(x, 1);\n}\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* run -> add */
    PASS();
}

/* C++: caller in main.cpp, callee in util.cpp. */
TEST(es_calls_crossfile_cpp) {
    static const ES_LangFile f[] = {
        {"util.cpp", "int multiply(int a, int b) {\n    return a * b;\n}\n"},
        {"main.cpp",
         "int multiply(int a, int b);\n\nint run(int x) {\n    return multiply(x, 3);\n}\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* run -> multiply */
    PASS();
}

/* Rust: caller in main.rs, callee in lib.rs (pub fn). */
TEST(es_calls_crossfile_rust) {
    static const ES_LangFile f[] = {
        {"lib.rs", "pub fn square(x: i32) -> i32 {\n    x * x\n}\n"},
        {"main.rs", "mod lib;\n\nfn run(n: i32) -> i32 {\n    lib::square(n)\n}\n"}};
    static const RQ_Expected uncertain[] = {
        {.case_id = "original-library-root-context",
         .site = {.file = "main.rs", .caller = "run", .needle = "lib::square(n)"},
         .relation = "CALLS",
         .target = {.file = "lib.rs",
                    .suffix = "square",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "pub fn square(x: i32) -> i32 {"},
         .mode = RQ_REQUIRED,
         .gap_id = "rust-library-module-root-context",
         .why = "A lib.rs crate root used as a declared submodule needs explicit crate membership.",
         .tried = "Preserved the original main/lib source and added a required ordinary module "
                  "counterpart."}};
    ASSERT_TRUE(es_binding_venues(f, 2, "rust-library-root-context", uncertain, 1, NULL, 0));
    static const ES_LangFile ordinary[] = {
        {"util.rs", "pub fn square(x: i32) -> i32 { x * x }\n"},
        {"main.rs", "mod util;\nuse crate::util::square;\nfn run(n: i32) -> i32 { square(n) }\n"},
        {"unloaded.rs", "pub fn square(x: i32) -> i32 { 0 }\n"}};
    static const RQ_Expected required[] = {
        {.case_id = "declared-module-exact-free-function",
         .site = {.file = "main.rs", .caller = "run", .needle = "square(n)"},
         .relation = "CALLS",
         .target = {.file = "util.rs",
                    .suffix = "square",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "pub fn square(x: i32) -> i32 { x * x }"},
         .mode = RQ_REQUIRED}};
    ASSERT_TRUE(es_binding_venues(ordinary, 3, "rust-declared-module", required, 1, NULL, 0));
    PASS();
}

/* Python: caller in main.py calls function from util.py via relative import. */
TEST(es_calls_crossfile_python) {
    static const ES_LangFile f[] = {
        {"util.py", "def transform(x):\n    return x * 3\n"},
        {"main.py", "from .util import transform\n\n\ndef run(y):\n    return transform(y)\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* run -> transform */
    PASS();
}

/* TypeScript: caller in main.ts calls function from util.ts. */
TEST(es_calls_crossfile_typescript) {
    static const ES_LangFile f[] = {
        {"util.ts", "export function format(s: string): string {\n    return s.trim();\n}\n"},
        {"main.ts",
         "import { format } from './util';\n\n"
         "export function run(input: string): string {\n    return format(input);\n}\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* run -> format */
    PASS();
}

TEST(es_calls_vue_embedded_issue1410) {
    static const ES_LangFile f[] = {
        {"App.vue", "<template><p>Vue</p></template>\n"
                    "<script setup lang=\"ts\">\n"
                    "function callee(): number { return 42; }\n"
                    "function caller(): number { return callee(); }\n"
                    "</script>\n"},
    };
    ASSERT_EQ(es_exact_edge_by_name(f, 1, "CALLS", "caller", "callee"), 1);
    PASS();
}

TEST(es_calls_svelte_embedded_issue1807) {
    static const ES_LangFile f[] = {
        {"Toggle.svelte", "<script lang=\"ts\">\n"
                          "function callee(): number { return 42; }\n"
                          "function caller(): number { return callee(); }\n"
                          "</script>\n"
                          "<button on:click={caller}>Toggle</button>\n"},
    };
    ASSERT_EQ(es_exact_edge_by_name(f, 1, "CALLS", "caller", "callee"), 1);
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
    static const ES_LangFile f[] = {{"Util.kt", "fun double(x: Int): Int = x * 2\n"},
                                    {"Main.kt", "fun run(n: Int): Int = double(n)\n"}};
    ASSERT_TRUE(es_edge_present(f, 2, "CALLS", 1)); /* run -> double */
    PASS();
}

/* C#: caller in Main.cs calls static method from Util.cs (same namespace). */
TEST(es_calls_crossfile_csharp) {
    static const ES_LangFile f[] = {
        {"Util.cs", "namespace App {\n    class Util {\n"
                    "        public static int Square(int x) { return x * x; }\n    }\n}\n"},
        {"Main.cs", "namespace App {\n    class Main {\n"
                    "        public int Run(int n) { return Util.Square(n); }\n    }\n}\n"}};
    static const RQ_Expected uncertain[] = {
        {.case_id = "original-internal-class-context",
         .site = {.file = "Main.cs", .caller = "Main.Run", .needle = "Util.Square(n)"},
         .relation = "CALLS",
         .target = {.file = "Util.cs",
                    .suffix = "Util.Square",
                    .label = "Method",
                    .declaration_line = 3,
                    .declaration_text =
                        "        public static int Square(int x) { return x * x; }"},
         .mode = RQ_REQUIRED}};
    ASSERT_TRUE(es_binding_venues(f, 2, "csharp-internal-assembly", uncertain, 1, NULL, 0));
    static const ES_LangFile visible[] = {
        {"Util.cs", "namespace App {\npublic class Util {\n"
                    "public static int Square(int x) { return x * x; }\n}\n}\n"},
        {"Main.cs", "namespace App {\npublic class Main {\n"
                    "public int Run(int n) { return Util.Square(n); }\n}\n}\n"},
        {"decoy/Util.cs", "namespace Other {\npublic class Util {\n"
                          "public static int Square(int x) { return 0; }\n}\n}\n"}};
    static const RQ_Expected required[] = {
        {.case_id = "public-class-exact-namespace-member",
         .site = {.file = "Main.cs", .caller = "Main.Run", .needle = "Util.Square(n)"},
         .relation = "CALLS",
         .target = {.file = "Util.cs",
                    .suffix = "Util.Square",
                    .label = "Method",
                    .declaration_line = 3,
                    .declaration_text = "public static int Square(int x) { return x * x; }"},
         .mode = RQ_REQUIRED}};
    ASSERT_TRUE(es_binding_venues(visible, 3, "csharp-public-namespace", required, 1, NULL, 0));
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
        {"Animal.java", "package zoo;\n\nclass Animal {\n    int speak() { return 0; }\n}\n"},
        {"Dog.java",
         "package zoo;\n\nclass Dog extends Animal {\n    int speak() { return 1; }\n}\n"}};
    /* GREEN: Java extraction correct; registry resolves cross-file same-package. */
    ASSERT_TRUE(es_edge_present(f, 2, "INHERITS", 1)); /* Dog -> Animal */
    PASS();
}

/* C# cross-file INHERITS — expected GREEN.
 * Base in Base.cs, Derived extends Base in Derived.cs, same namespace. */
/* These domains cannot establish an exact base even though all names exist
 * in the graph. Every venue also retains the shared required ESM control. */
TEST(es_inherits_csharp_namespace_ambiguity_and_conditional_barriers) {
    static const ES_LangFile wrong_namespace[] = {
        {"Base.cs", "namespace Other { public class Base {} }\n"},
        {"Derived.cs", "namespace App { public class Derived : Base {} }\n"}};
    ASSERT_TRUE(es_structural_venues(wrong_namespace, 2, "INHERITS", "Derived.cs", "Derived",
                                     "Base.cs", "Base", false, NULL));
    static const ES_LangFile duplicate[] = {
        {"Base.cs", "namespace App { public class Base {} }\n"},
        {"duplicate/Base.cs", "namespace App { public class Base {} }\n"},
        {"Derived.cs", "namespace App { public class Derived : Base {} }\n"}};
    ASSERT_TRUE(es_structural_venues(duplicate, 3, "INHERITS", "Derived.cs", "Derived", "Base.cs",
                                     "Base", false, NULL));
    static const ES_LangFile conditional[] = {
        {"Base.cs", "namespace App { public class Base {} }\n"},
        {"Derived.cs", "#if USE_EXTERNAL\nusing Base = External.Base;\n#endif\n"
                       "namespace App { public class Derived : Base {} }\n"}};
    /* WHY: the conditional alias's compilation context is unavailable.
     * Tried the complete raw namespace census; configuration remains open. */
    ASSERT_TRUE(es_structural_venues(conditional, 2, "INHERITS", "Derived.cs", "Derived", "Base.cs",
                                     "Base", false, "csharp-conditional-alias"));
    PASS();
}

TEST(es_inherits_cpp_include_namespace_duplicate_and_inactive_barriers) {
    static const ES_LangFile wrong_namespace[] = {
        {"shape.h", "namespace Other { class Shape {}; }\n"},
        {"circle.cpp", "#include \"shape.h\"\nclass Circle : public Shape {};\n"}};
    ASSERT_TRUE(es_structural_venues(wrong_namespace, 2, "INHERITS", "circle.cpp", "Circle",
                                     "shape.h", "Shape", false, NULL));
    static const ES_LangFile duplicate[] = {{"shape.h", "class Shape {};\n"},
                                            {"other.h", "class Shape {};\n"},
                                            {"circle.cpp",
                                             "#include \"shape.h\"\n#include \"other.h\"\n"
                                             "class Circle : public Shape {};\n"}};
    ASSERT_TRUE(es_structural_venues(duplicate, 3, "INHERITS", "circle.cpp", "Circle", "shape.h",
                                     "Shape", false, NULL));
    static const ES_LangFile inactive[] = {{"shape.h", "class Shape {};\n"},
                                           {"circle.cpp", "#if 0\n#include \"shape.h\"\n#endif\n"
                                                          "class Circle : public Shape {};\n"}};
    ASSERT_TRUE(es_structural_venues(inactive, 2, "INHERITS", "circle.cpp", "Circle", "shape.h",
                                     "Shape", false, NULL));
    PASS();
}

TEST(es_implements_rust_module_duplicate_and_inactive_barriers) {
    static const ES_LangFile undeclared[] = {
        {"main.rs", "mod implementation;\nfn main() {}\n"},
        {"traits.rs", "pub trait Greet { fn hello(&self) -> i32; }\n"},
        {"implementation.rs", "use crate::traits::Greet;\npub struct English;\n"
                              "impl Greet for English { fn hello(&self) -> i32 { 1 } }\n"}};
    ASSERT_TRUE(es_structural_venues(undeclared, 3, "IMPLEMENTS", "implementation.rs", "English",
                                     "traits.rs", "Greet", false, NULL));
    static const ES_LangFile duplicate[] = {
        {"main.rs", "mod one;\nmod two;\nmod implementation;\nfn main() {}\n"},
        {"one.rs", "pub trait Greet { fn hello(&self) -> i32; }\n"},
        {"two.rs", "pub trait Greet { fn hello(&self) -> i32; }\n"},
        {"implementation.rs", "use crate::one::Greet;\nuse crate::two::Greet;\n"
                              "pub struct English;\n"
                              "impl Greet for English { fn hello(&self) -> i32 { 1 } }\n"}};
    ASSERT_TRUE(es_structural_venues(duplicate, 4, "IMPLEMENTS", "implementation.rs", "English",
                                     "one.rs", "Greet", false, NULL));
    static const ES_LangFile inactive[] = {
        {"main.rs", "mod traits;\nmod implementation;\nfn main() {}\n"},
        {"traits.rs", "pub trait Greet { fn hello(&self) -> i32; }\n"},
        {"implementation.rs", "use crate::traits::Greet;\npub struct English;\n"
                              "#[cfg(any())]\n"
                              "impl Greet for English { fn hello(&self) -> i32 { 1 } }\n"}};
    ASSERT_TRUE(es_structural_venues(inactive, 3, "IMPLEMENTS", "implementation.rs", "English",
                                     "traits.rs", "Greet", false, NULL));
    PASS();
}

TEST(es_inherits_crossfile_csharp) {
    static const ES_LangFile f[] = {
        {"Base.cs",
         "namespace App {\n    class Base {\n        public int Value() { return 0; }\n    }\n}\n"},
        {"Derived.cs", "namespace App {\n    class Derived : Base {\n"
                       "        public int Extra() { return 1; }\n    }\n}\n"}};
    /* WHY: implicit internal classes need captured assembly membership.
     * Tried namespace-only lookup; public classes below pin the supported
     * exact namespace obligation without guessing assembly boundaries. */
    ASSERT_TRUE(es_structural_venues(f, 2, "INHERITS", "Derived.cs", "Derived", "Base.cs", "Base",
                                     false, "csharp-internal-assembly"));
    static const ES_LangFile visible[] = {
        {"Base.cs", "namespace App {\npublic class Base { public int Value() { return 0; } }\n}\n"},
        {"Derived.cs", "namespace App {\npublic class Derived : Base { }\n}\n"},
        {"decoy/Base.cs", "namespace Other {\npublic class Base { }\n}\n"}};
    ASSERT_TRUE(es_structural_venues(visible, 3, "INHERITS", "Derived.cs", "Derived", "Base.cs",
                                     "Base", true, NULL));
    PASS();
}

/* C++ cross-file INHERITS — expected GREEN.
 * Shape in shape.cpp, Circle extends Shape in circle.cpp. */
TEST(es_inherits_crossfile_cpp) {
    static const ES_LangFile f[] = {
        {"shape.cpp", "class Shape {\npublic:\n    virtual int area() { return 0; }\n};\n"},
        {"circle.cpp", "class Shape;\n\nclass Circle : public Shape {\npublic:\n"
                       "    int area() { return 3; }\n};\n"}};
    /* A forward declaration cannot supply the complete base class required
     * for inheritance. Another translation unit's namesake is not authority. */
    ASSERT_TRUE(es_structural_venues(f, 2, "INHERITS", "circle.cpp", "Circle", "shape.cpp", "Shape",
                                     false, NULL));
    static const ES_LangFile included[] = {
        {"shape.h", "class Shape { public: virtual int area() { return 0; } };\n"},
        {"circle.cpp", "#include \"shape.h\"\nclass Circle : public Shape {\n"
                       "public: int area() { return 3; }\n};\n"},
        {"unloaded.cpp", "namespace Other { class Shape {}; }\n"}};
    ASSERT_TRUE(es_structural_venues(included, 3, "INHERITS", "circle.cpp", "Circle", "shape.h",
                                     "Shape", true, NULL));
    PASS();
}

/* Python cross-file INHERITS — expected RED (extraction bug: base_classes
 * holds "(Animal)" with parens, not "Animal").
 * Reproduction: confirms the end-to-end gap from extraction to graph edge. */
TEST(es_inherits_crossfile_python_red) {
    static const ES_LangFile f[] = {
        {"animal.py", "class Animal:\n    def speak(self):\n        return 0\n"},
        {"dog.py", "from .animal import Animal\n\n\nclass Dog(Animal):\n    def speak(self):\n     "
                   "   return 1\n"}};
    /* RED: base_classes extraction broken for Python plain identifier nodes.
     * Root cause: collect_bases_from_field does not match bare `identifier`
     * nodes from tree-sitter-python; stores "(Animal)" with parens.
     * Fix location: extract_defs.c collect_bases_from_field / Python case.
     * This test SHOULD FAIL (count=0) until that fix lands. */
    ES_LangProj lp;
    cbm_store_t *store = es_lang_index_files(&lp, f, 2);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, "INHERITS") : -1;
    if (got >= 1) {
        fprintf(stderr,
                "  [ES-EDGE] UNEXPECTED PASS: Python cross-file INHERITS got=%d "
                "(extraction bug may have been fixed — promote to GREEN)\n",
                got);
    } else {
        fprintf(stderr,
                "  [ES-EDGE] CONFIRMED RED: Python cross-file INHERITS got=%d "
                "(extraction bug reproduces end-to-end)\n",
                got);
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
        {"base.ts", "export class Base {\n    value(): number { return 0; }\n}\n"},
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
    int got = es_exact_inheritance(store, lp.project, "derived.ts", "Derived", "base.ts", "Base");
    if (got >= 1) {
        fprintf(stderr,
                "  [ES-EDGE] UNEXPECTED PASS: TypeScript cross-file INHERITS got=%d "
                "(promote to GREEN if extraction fixed)\n",
                got);
    } else {
        fprintf(stderr, "  [ES-EDGE] CONFIRMED RED: TypeScript cross-file INHERITS got=%d\n", got);
    }
    es_lang_cleanup(&lp, store);
    ASSERT_TRUE(got >= 1); /* FAILS (RED) until TS extraction fixed */
    PASS();
}

/* Exact import sites use independent source anchors. Heritage is checked
 * separately because INHERITS is not a controlled resolver relation. */
static bool es_php_inheritance_venues(const ES_LangFile *files, int count,
                                      const RQ_Expected *imports, size_t import_count,
                                      const char *unproven_include, int inheritance) {
    enum { MAX_FILES = 8, PADDING = 55 };
    static const ES_LangFile control = {
        "quality_control.c",
        "void controlTarget(void) {}\nvoid controlCaller(void) { controlTarget(); }\n"};
    if (count < 2 || count >= MAX_FILES || import_count > 4)
        return false;
    for (int parallel = 0; parallel < 2; parallel++) {
        ES_LangFile all[MAX_FILES + PADDING];
        RQ_Source sources[MAX_FILES];
        memcpy(all, files, (size_t)count * sizeof(*files));
        all[count] = control;
        for (int i = 0; i <= count; i++)
            sources[i] = (RQ_Source){.path = all[i].name, .text = all[i].content};
        char names[PADDING][64], contents[PADDING][96];
        for (int i = 0; parallel && i < PADDING; i++) {
            snprintf(names[i], sizeof(names[i]), "padding/file_%02d.php", i);
            snprintf(contents[i], sizeof(contents[i]),
                     "<?php\nfunction padding%02d() { return %d; }\n", i, i);
            all[count + 1 + i] = (ES_LangFile){names[i], contents[i]};
        }
        RQ_Expected expected[5] = {{.case_id = "same-pipeline-required-control",
                                    .site = {.file = "quality_control.c",
                                             .caller = "quality_control.controlCaller",
                                             .needle = "controlTarget()"},
                                    .relation = "CALLS",
                                    .target = {.file = "quality_control.c",
                                               .suffix = "quality_control.controlTarget",
                                               .label = "Function",
                                               .declaration_line = 1,
                                               .declaration_text = "void controlTarget(void) {}"},
                                    .mode = RQ_REQUIRED}};
        for (size_t i = 0; i < import_count; i++)
            expected[i + 1] = imports[i];
        RQ_Forbidden forbidden = {.case_id = "include-without-exact-path-authority",
                                  .site = {.file = "Child.php",
                                           .caller = "Child.php.__file__",
                                           .needle = unproven_include},
                                  .relation = "IMPORTS",
                                  .any_target = true};
        ES_LangProj project;
        cbm_store_t *store = es_lang_index_files(&project, all, count + 1 + parallel * PADDING);
        RQ_Report report;
        bool quality =
            rq_check(store, project.project, "php-include-heritage", sources, (size_t)count + 1,
                     expected, import_count + 1, unproven_include ? &forbidden : NULL,
                     unproven_include ? 1 : 0, &report, stderr);
        int found =
            es_exact_inheritance(store, project.project, "Child.php", "Child", "Base.php", "Base");
        es_lang_cleanup(&project, store);
        if (!quality || found != inheritance) {
            fprintf(stderr, "[ES-PHP] padding=%d inheritance=%d expected=%d\n", parallel * PADDING,
                    found, inheritance);
            return false;
        }
    }
    return true;
}

#define ES_PHP_IMPORT(id, path, qn, text)                                           \
    {.case_id = id,                                                                 \
     .site = {.file = "Child.php", .caller = "Child.php.__file__", .needle = text}, \
     .relation = "IMPORTS",                                                         \
     .target = {.file = path,                                                       \
                .suffix = path ".__file__",                                         \
                .label = "File",                                                    \
                .declaration_line = 1,                                              \
                .declaration_text = "<?php"},                                       \
     .mode = RQ_REQUIRED}

/* The production writer must retain exact include sites after compaction and
 * reject malformed carrier coordinates before constructing path authority. */
TEST(es_php_include_occurrence_spans_and_sealing_guards) {
    static const char source[] = "<?php\nrequire_once __DIR__ . '/Base.php';\n";
    const char *expression = strstr(source, "require_once");
    const uint32_t start = (uint32_t)(expression - source);
    const uint32_t end = (uint32_t)(strchr(expression, ';') - source);
    bool valid = true;
    for (int mode = 0; mode < 4 && valid; mode++) {
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PHP,
                                                 "php-site", "Child.php", 0, NULL, NULL);
        cbm_gbuf_t *graph = cbm_gbuf_new("php-site", "/tmp/php-site");
        valid = result && !result->has_error && result->imports.count == 1 && graph;
        if (valid) {
            cbm_result_compact(result);
            CBMImport *import = &result->imports.items[0];
            valid = import->site_start_byte == start && import->site_end_byte == end &&
                    import->source_origin == CBM_SOURCE_ORIGIN_RAW;
            if (mode == 1)
                import->site_end_byte = import->site_start_byte;
            else if (mode == 2)
                import->site_end_byte = (uint32_t)strlen(source) + 1;
            else if (mode == 3)
                import->source_origin = CBM_SOURCE_ORIGIN_PREPROCESSED;
            const char *from = "php-site.Child.php.__file__";
            const char *to = "php-site.Base.php.__file__";
            valid = valid &&
                    cbm_gbuf_upsert_node(graph, "File", "Child.php", from, "Child.php", 0, 0,
                                         "{}") > 0 &&
                    cbm_gbuf_upsert_node(graph, "File", "Base.php", to, "Base.php", 0, 0, "{}") > 0;
            if (valid) {
                cbm_pipeline_ctx_t context = {.project_name = "php-site", .gbuf = graph};
                const cbm_gbuf_node_t *file = cbm_gbuf_find_by_qn(graph, from);
                int written = cbm_pipeline_php_create_import_edges(&context, result, "Child.php",
                                                                   from, file, NULL);
                valid = written == (mode == 0 ? 1 : 0) && cbm_gbuf_resolutions_own_shared(graph);
                size_t count = 0;
                const cbm_resolve_record_t *rows = cbm_gbuf_resolutions(graph, &count);
                valid = valid && count == 1 && rows && rows[0].emitted == (mode == 0);
                if (valid && mode == 0) {
                    valid = cbm_resolve_record_admit(&rows[0]) && rows[0].start_byte == start &&
                            rows[0].end_byte == end;
                    cbm_resolve_record_t stale = rows[0];
                    stale.start_byte++;
                    valid = valid && !cbm_resolve_record_admit(&stale);
                    stale = rows[0];
                    stale.content_identity = "stale-source-identity";
                    valid = valid && !cbm_resolve_record_admit(&stale);
                } else if (valid) {
                    valid = !cbm_resolve_record_admit(&rows[0]) &&
                            !strcmp(rows[0].result.reason, "import_occurrence_invalid");
                }
            }
        }
        cbm_gbuf_free(graph);
        cbm_free_result(result);
    }
    ASSERT_TRUE(valid);
    PASS();
}

TEST(es_php_include_kind_distinguishes_use_and_unproven_lookup) {
    static const char source[] = "<?php\nuse Vendor\\Base;\nrequire_once 'Base';\n"
                                 "require_once __DIR__ . '/Base.php';\nordinaryExpression();\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PHP, "php-kinds",
                                             "Child.php", 0, NULL, NULL);
    bool valid = result && !result->has_error && result->imports.count == 3;
    if (valid) {
        const CBMImport *imports = result->imports.items;
        valid = imports[0].kind == CBM_IMPORT_KIND_DEFAULT &&
                !strcmp(imports[0].module_path, "Vendor\\Base") &&
                imports[1].kind == CBM_IMPORT_KIND_PHP_INCLUDE_UNPROVEN &&
                !strcmp(imports[1].module_path, "Base") &&
                imports[2].kind == CBM_IMPORT_KIND_LITERAL_RELATIVE_PATH &&
                !strcmp(imports[2].module_path, "Base.php");
    }
    cbm_free_result(result);
    ASSERT_TRUE(valid);
    PASS();
}

TEST(es_inherits_php_bare_name_include_is_not_class_use) {
    static const ES_LangFile files[] = {
        {"Base.php", "<?php\nclass Base {}\n"},
        {"Child.php", "<?php\nrequire_once 'Base';\nclass Child extends Base {}\n"}};
    ASSERT_TRUE(es_php_inheritance_venues(files, 2, NULL, 0, "require_once 'Base'", 0));
    PASS();
}

TEST(es_inherits_php_exact_dir_include_context) {
    static const ES_LangFile files[] = {
        {"Base.php", "<?php\nclass Base { public function value() { return 0; } }\n"},
        {"Child.php", "<?php\nrequire_once __DIR__ . '/Base.php';\nclass Child extends Base {}\n"},
        {"decoy/Base.php", "<?php\nclass Base {}\n"}};
    static const RQ_Expected imports[] = {ES_PHP_IMPORT("dir-anchored-base", "Base.php", "Base",
                                                        "require_once __DIR__ . '/Base.php'")};
    ASSERT_TRUE(es_php_inheritance_venues(files, 3, imports, 1, NULL, 1));
    PASS();
}

TEST(es_inherits_php_namespace_include_does_not_supply_global_class) {
    static const ES_LangFile files[] = {
        {"Base.php", "<?php\nnamespace Other;\nclass Base {}\n"},
        {"Child.php", "<?php\nrequire_once __DIR__ . '/Base.php';\nclass Child extends Base {}\n"}};
    static const RQ_Expected imports[] = {ES_PHP_IMPORT(
        "included-namespace-file", "Base.php", "Base", "require_once __DIR__ . '/Base.php'")};
    ASSERT_TRUE(es_php_inheritance_venues(files, 2, imports, 1, NULL, 0));
    PASS();
}

TEST(es_inherits_php_missing_include_does_not_borrow_decoy) {
    static const ES_LangFile files[] = {
        {"Base.php", "<?php\nclass Base {}\n"},
        {"Child.php",
         "<?php\nrequire_once __DIR__ . '/Missing.php';\nclass Child extends Base {}\n"}};
    ASSERT_TRUE(
        es_php_inheritance_venues(files, 2, NULL, 0, "require_once __DIR__ . '/Missing.php'", 0));
    PASS();
}

TEST(es_inherits_php_dynamic_include_does_not_borrow_decoy) {
    static const ES_LangFile files[] = {
        {"Base.php", "<?php\nclass Base {}\n"},
        {"Child.php", "<?php\nrequire_once $path;\nclass Child extends Base {}\n"}};
    ASSERT_TRUE(es_php_inheritance_venues(files, 2, NULL, 0, "require_once $path", 0));
    PASS();
}

TEST(es_inherits_php_conditional_include_does_not_authorize_class) {
    static const ES_LangFile files[] = {
        {"Base.php", "<?php\nclass Base {}\n"},
        {"Child.php", "<?php\nif ($enabled) { require_once __DIR__ . '/Base.php'; }\n"
                      "class Child extends Base {}\n"}};
    ASSERT_TRUE(
        es_php_inheritance_venues(files, 2, NULL, 0, "require_once __DIR__ . '/Base.php'", 0));
    PASS();
}

TEST(es_inherits_php_duplicate_included_class_abstains) {
    static const ES_LangFile files[] = {
        {"Base.php", "<?php\nclass Base {}\n"},
        {"Other.php", "<?php\nclass BASE {}\n"},
        {"Child.php", "<?php\nrequire_once __DIR__ . '/Base.php';\n"
                      "require_once __DIR__ . '/Other.php';\nclass Child extends Base {}\n"}};
    static const RQ_Expected imports[] = {ES_PHP_IMPORT("first-included-class", "Base.php", "Base",
                                                        "require_once __DIR__ . '/Base.php'"),
                                          ES_PHP_IMPORT("case-insensitive-duplicate", "Other.php",
                                                        "Other",
                                                        "require_once __DIR__ . '/Other.php'")};
    ASSERT_TRUE(es_php_inheritance_venues(files, 3, imports, 2, NULL, 0));
    PASS();
}

TEST(es_inherits_php_effectful_included_source_abstains) {
    static const ES_LangFile files[] = {
        {"Base.php", "<?php\nexit();\nclass Base {}\n"},
        {"Child.php", "<?php\nrequire_once __DIR__ . '/Base.php';\nclass Child extends Base {}\n"}};
    static const RQ_Expected imports[] = {ES_PHP_IMPORT(
        "effectful-included-file", "Base.php", "Base", "require_once __DIR__ . '/Base.php'")};
    ASSERT_TRUE(es_php_inheritance_venues(files, 2, imports, 1, NULL, 0));
    PASS();
}

#undef ES_PHP_IMPORT

/* Bare filenames search PHP's runtime include_path before the calling-file
 * directory. This fixture captures neither include_path nor CWD. Its one
 * import lookup and one inheritance lookup remain unresolved-context quality
 * obligations; a specific Base.php edge would be an unsupported guess.
 * Tried: exact literal lookup, source snapshot and namespace inspection. The
 * separate __DIR__ fixture above pins the source-valid required counterpart. */
TEST(es_inherits_crossfile_php_red) {
    static const ES_LangFile f[] = {
        {"Base.php", "<?php\nclass Base {\n    public function value() { return 0; }\n}\n"},
        {"Child.php", "<?php\nrequire_once 'Base.php';\n\nclass Child extends Base {\n"
                      "    public function extra() { return 1; }\n}\n"}};
    fprintf(stderr, "[RQ-PHP] gap=runtime-include-context unresolved_imports=1 "
                    "unresolved_heritage=1 tried=literal-source-and-namespace-audit\n");
    ASSERT_TRUE(es_php_inheritance_venues(f, 2, NULL, 0, "require_once 'Base.php'", 0));
    PASS();
}

/* Kotlin cross-file INHERITS — expected RED (`:` supertype syntax not parsed). */
TEST(es_inherits_crossfile_kotlin_red) {
    static const ES_LangFile f[] = {
        {"Base.kt", "open class Base {\n    open fun value(): Int = 0\n}\n"},
        {"Child.kt", "class Child : Base() {\n    override fun value(): Int = 1\n}\n"}};
    /* RED: Kotlin extractor does not parse `:` supertype syntax → base_classes empty.
     * Fix location: extract_defs.c Kotlin class body / supertype_list handling.
     * FAILS (RED) until fixed. */
    ES_LangProj lp;
    cbm_store_t *store = es_lang_index_files(&lp, f, 2);
    int got = es_exact_inheritance(store, lp.project, "Child.kt", "Child", "Base.kt", "Base");
    if (got >= 1) {
        fprintf(stderr,
                "  [ES-EDGE] UNEXPECTED PASS: Kotlin cross-file INHERITS got=%d "
                "(promote to GREEN)\n",
                got);
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
        {"trait.rs", "pub trait Greet {\n    fn hello(&self) -> String;\n}\n"},
        {"impl.rs", "use crate::trait::Greet;\n\npub struct English;\n\n"
                    "impl Greet for English {\n"
                    "    fn hello(&self) -> String {\n        String::from(\"hi\")\n    }\n}\n"}};
    /* The original path uses the reserved keyword trait and supplies no
     * crate module tree. Never borrow Greet from an unrelated file. */
    ASSERT_TRUE(es_structural_venues(f, 2, "IMPLEMENTS", "impl.rs", "English", "trait.rs", "Greet",
                                     false, NULL));
    static const ES_LangFile declared[] = {
        {"main.rs", "mod traits;\nmod implementation;\nfn main() {}\n"},
        {"traits.rs", "pub trait Greet { fn hello(&self) -> i32; }\n"},
        {"implementation.rs", "use crate::traits::Greet;\npub struct English;\n"
                              "impl Greet for English { fn hello(&self) -> i32 { 1 } }\n"},
        {"unloaded.rs", "pub trait Greet { fn hello(&self) -> i32; }\n"}};
    ASSERT_TRUE(es_structural_venues(declared, 4, "IMPLEMENTS", "implementation.rs", "English",
                                     "traits.rs", "Greet", true, NULL));
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
        {"Service.java", "package app;\n\ninterface Runnable {\n    void run();\n}\n\n"
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
        {"Worker.cs", "namespace App {\n    interface IWorker {\n        void Work();\n    }\n\n"
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
        {"iface.go", "package app\n\ntype Stringer interface {\n    String() string\n}\n"},
        {"impl.go", "package app\n\ntype MyType struct{ val string }\n\n"
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
        {"iface.go", "package app\n\ntype Namer interface {\n    Name() string\n}\n"},
        {"impl.go", "package app\n\ntype Entity struct{ name string }\n\n"
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
        {"decorators.py", "def log_call(func):\n    def wrapper(*args, **kwargs):\n"
                          "        return func(*args, **kwargs)\n    return wrapper\n"},
        {"service.py", "from .decorators import log_call\n\n\n"
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
        {"Service.java", "package app;\n\nimport java.lang.annotation.*;\n\n"
                         "@Retention(RetentionPolicy.RUNTIME)\n@interface Override {}\n\n"
                         "class Service {\n    @Override\n    public String toString() { return "
                         "\"service\"; }\n}\n"}};
    /* Uncertain/RED: Java extraction likely does not populate decorators[] for
     * @Annotation syntax (no Java branch in the decorator extractor confirmed).
     * This test probes whether any DECORATES edge is created.
     * Expected to be RED (count=0) — reproduces the gap if so. */
    ES_LangProj lp;
    cbm_store_t *store = es_lang_index_files(&lp, f, 1);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, "DECORATES") : -1;
    fprintf(stderr,
            "  [ES-EDGE] Java annotation DECORATES got=%d (expected 0 if unimplemented; "
            "promote to GREEN if edge appears)\n",
            got);
    es_lang_cleanup(&lp, store);
    /* Assert the CORRECT outcome: annotated method should produce DECORATES. */
    ASSERT_TRUE(got >= 1); /* RED until Java annotation extraction implemented */
    PASS();
}

/* Kotlin annotation — same-file. */
TEST(es_decorates_annotation_kotlin) {
    static const ES_LangFile f[] = {{"Service.kt", "annotation class Log\n\n"
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
        {"Service.cs", "using System;\n\n[AttributeUsage(AttributeTargets.Method)]\nclass "
                       "LogAttribute : Attribute {}\n\n"
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

/* Python constructor syntax is a CALLS edge to the materialized Class node. */
TEST(es_usage_crossfile_python) {
    static const ES_LangFile f[] = {
        {"models.py", "class User:\n    def __init__(self, name):\n        self.name = name\n"},
        {"main.py",
         "from .models import User\n\n\ndef create_user(name):\n    return User(name)\n"}};
    ASSERT_EQ(es_exact_edge_by_name(f, 2, "CALLS", "create_user", "User"), 1);
    PASS();
}

/* TypeScript cross-file USAGE: main.ts uses a type from types.ts. */
TEST(es_usage_crossfile_typescript) {
    static const ES_LangFile f[] = {
        {"types.ts", "export interface Config {\n    timeout: number;\n}\n"},
        {"main.ts",
         "import { Config } from './types';\n\n"
         "export function create(cfg: Config): string {\n    return String(cfg.timeout);\n}\n"}};
    static const RQ_Expected expected[] = {
        {.case_id = "parameter-interface-annotation",
         .site = {.file = "main.ts", .caller = "create", .needle = "Config", .occurrence_index = 1},
         .relation = "USES_TYPE",
         .target = {.file = "types.ts",
                    .suffix = "Config",
                    .label = "Interface",
                    .declaration_line = 1,
                    .declaration_text = "export interface Config {"}}};
    static const RQ_Forbidden forbidden[] = {
        {.case_id = "annotation-is-not-value-usage",
         .site = {.file = "main.ts", .caller = "create", .needle = "Config", .occurrence_index = 1},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "field-access-does-not-use-interface-value",
         .site = {.file = "main.ts", .caller = "create", .needle = "cfg.timeout"},
         .relation = "USAGE",
         .target = {.file = "types.ts",
                    .suffix = "Config",
                    .label = "Interface",
                    .declaration_line = 1,
                    .declaration_text = "export interface Config {"}}};
    ASSERT_TRUE(
        es_binding_venues(f, 2, "typescript-parameter-type-role", expected, 1, forbidden, 2));
    PASS();
}

/* Go cross-file USAGE: main.go references a struct type from types.go. */
TEST(es_usage_crossfile_go) {
    static const ES_LangFile f[] = {
        {"types.go", "package app\n\ntype Config struct {\n    Timeout int\n}\n"},
        {"main.go", "package app\n\nfunc Create(cfg Config) int {\n    return cfg.Timeout\n}\n"}};
    static const RQ_Expected expected[] = {
        {.case_id = "parameter-struct-annotation",
         .site = {.file = "main.go", .caller = "Create", .needle = "Config"},
         .relation = "USES_TYPE",
         .target = {.file = "types.go",
                    .suffix = "Config",
                    .label = "Struct",
                    .declaration_line = 3,
                    .declaration_text = "type Config struct {"},
         .mode = RQ_REQUIRED},
        /* cfg is declared Config, and Config declares Timeout: this read is the selected field. */
        {.case_id = "typed-field-value-use",
         .site = {.file = "main.go", .caller = "Create", .needle = "Timeout"},
         .relation = "USAGE",
         .target = {.file = "types.go",
                    .suffix = "Config.Timeout",
                    .label = "Field",
                    .declaration_line = 4,
                    .declaration_text = "    Timeout int"},
         .mode = RQ_REQUIRED}};
    static const RQ_Forbidden forbidden[] = {
        {.case_id = "annotation-is-not-value-usage",
         .site = {.file = "main.go", .caller = "Create", .needle = "Config"},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "field-access-does-not-use-struct-value",
         .site = {.file = "main.go", .caller = "Create", .needle = "cfg.Timeout"},
         .relation = "USAGE",
         .target = {.file = "types.go",
                    .suffix = "Config",
                    .label = "Struct",
                    .declaration_line = 3,
                    .declaration_text = "type Config struct {"}}};
    ASSERT_TRUE(es_binding_venues(f, 2, "go-parameter-type-role", expected, 2, forbidden, 2));
    PASS();
}

#define ES_GO_CONFIG_ANNOTATION(index)                                                     \
    {.case_id = "uses-type-" #index,                                                       \
     .site = {.file = "main.go", .caller = "Build", .needle = "Config", .occurrence_index = index}, \
     .relation = "USES_TYPE",                                                              \
     .target = {.file = "types.go",                                                        \
                .suffix = "Config",                                                        \
                .label = "Struct",                                                         \
                .declaration_line = 3,                                                     \
                .declaration_text = "type Config struct {"},                               \
     .mode = RQ_REQUIRED}
#define ES_GO_CONFIG_NOT_USAGE(index)                                                      \
    {.case_id = "annotation-not-usage-" #index,                                            \
     .site = {.file = "main.go", .caller = "Build", .needle = "Config", .occurrence_index = index}, \
     .relation = "USAGE",                                                                  \
     .any_target = true}

/* USAGE means a value. A written annotation is USES_TYPE alone, in any wrapper. A composite
 * literal builds a value of its type, so it keeps USAGE beside its own USES_TYPE. */
TEST(es_go_annotation_types_are_not_value_usage) {
    static const ES_LangFile f[] = {
        {"types.go", "package app\n\ntype Config struct {\n    Timeout int\n}\n"},
        {"main.go", "package app\n\nfunc Build(base *Config, rest []Config) (Config, error) {\n"
                    "    var next Config\n"
                    "    _ = next\n"
                    "    return Config{base.Timeout}, nil\n}\n"}};
    static const RQ_Expected expected[] = {
        ES_GO_CONFIG_ANNOTATION(0),
        ES_GO_CONFIG_ANNOTATION(1),
        ES_GO_CONFIG_ANNOTATION(2),
        ES_GO_CONFIG_ANNOTATION(3),
        ES_GO_CONFIG_ANNOTATION(4),
        {.case_id = "literal-builds-a-value",
         .site = {.file = "main.go", .caller = "Build", .needle = "Config", .occurrence_index = 4},
         .relation = "USAGE",
         .target = {.file = "types.go",
                    .suffix = "Config",
                    .label = "Struct",
                    .declaration_line = 3,
                    .declaration_text = "type Config struct {"},
         .mode = RQ_REQUIRED},
        {.case_id = "typed-pointer-field-read",
         .site = {.file = "main.go", .caller = "Build", .needle = "Timeout"},
         .relation = "USAGE",
         .target = {.file = "types.go",
                    .suffix = "Config.Timeout",
                    .label = "Field",
                    .declaration_line = 4,
                    .declaration_text = "    Timeout int"},
         .mode = RQ_REQUIRED}};
    static const RQ_Forbidden forbidden[] = {ES_GO_CONFIG_NOT_USAGE(0), ES_GO_CONFIG_NOT_USAGE(1),
                                             ES_GO_CONFIG_NOT_USAGE(2), ES_GO_CONFIG_NOT_USAGE(3)};
    ASSERT_TRUE(es_binding_venues(f, 2, "go-annotation-roles", expected, 7, forbidden, 4));
    PASS();
}

/* A Go element assignment reads its operand and index and rebinds neither. A plain assignment
 * to the same variable is the write. */
TEST(es_go_index_assignment_reads_operand_and_index) {
    static const ES_LangFile f[] = {{"table.go", "package app\n\n"
                                                 "var table = map[string]int{}\n"
                                                 "var key = \"k\"\n\n"
                                                 "func Put(v int) {\n"
                                                 "    table[key] = v\n}\n\n"
                                                 "func Reset() {\n"
                                                 "    table = nil\n}\n"}};
    static const RQ_Target table = {.file = "table.go",
                                    .suffix = "table",
                                    .label = "Variable",
                                    .declaration_line = 3,
                                    .declaration_text = "var table = map[string]int{}"};
    static const RQ_Target key = {.file = "table.go",
                                  .suffix = "key",
                                  .label = "Variable",
                                  .declaration_line = 4,
                                  .declaration_text = "var key = \"k\""};
    const RQ_Expected expected[] = {
        {.case_id = "operand-is-read",
         .site = {.file = "table.go", .caller = "Put", .needle = "table", .occurrence_index = 1},
         .relation = "USAGE",
         .target = table,
         .mode = RQ_REQUIRED},
        {.case_id = "index-is-read",
         .site = {.file = "table.go", .caller = "Put", .needle = "key", .occurrence_index = 1},
         .relation = "USAGE",
         .target = key,
         .mode = RQ_REQUIRED},
        {.case_id = "plain-assignment-writes",
         .site = {.file = "table.go", .caller = "Reset", .needle = "table", .occurrence_index = 2},
         .relation = "WRITES",
         .target = table,
         .mode = RQ_REQUIRED}};
    static const RQ_Forbidden forbidden[] = {
        {.case_id = "element-store-does-not-rebind-operand",
         .site = {.file = "table.go", .caller = "Put", .needle = "table", .occurrence_index = 1},
         .relation = "WRITES",
         .any_target = true},
        {.case_id = "element-store-does-not-rebind-index",
         .site = {.file = "table.go", .caller = "Put", .needle = "key", .occurrence_index = 1},
         .relation = "WRITES",
         .any_target = true}};
    ASSERT_TRUE(es_binding_venues(f, 1, "go-index-assignment-roles", expected, 3, forbidden, 2));
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
        {"app.py", "from flask import Flask\n\napp = Flask(__name__)\n\n\n"
                   "@app.route(\"/items\")\ndef list_items():\n    return {\"items\": []}\n"},
        {"client.py", "def requests_get(url, params=None):\n    return {\"url\": url}\n\n\n"
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
        {"service.test.ts", "import { divide, subtract } from './service';\n\n"
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
    RUN_TEST(es_inherits_typescript_exact_import_context);
    RUN_TEST(es_inherits_typescript_missing_import_does_not_borrow_decoy);
    RUN_TEST(es_inherits_typescript_ambiguous_import_context);
    RUN_TEST(es_inherits_kotlin_exact_declared_package_context);
    RUN_TEST(es_inherits_kotlin_missing_package_member_does_not_borrow_decoy);
    RUN_TEST(es_inherits_kotlin_duplicate_package_member_abstains);
    RUN_TEST(es_inherits_kotlin_private_package_member_abstains);

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
    RUN_TEST(es_calls_vue_embedded_issue1410);
    RUN_TEST(es_calls_svelte_embedded_issue1807);

    /* ── FAMILY 2: INHERITS cross-file ────────────────────────── */
    /* GREEN: Java, C#, C++ (extraction confirmed correct). */
    RUN_TEST(es_inherits_crossfile_java);
    RUN_TEST(es_inherits_csharp_namespace_ambiguity_and_conditional_barriers);
    RUN_TEST(es_inherits_cpp_include_namespace_duplicate_and_inactive_barriers);
    RUN_TEST(es_implements_rust_module_duplicate_and_inactive_barriers);
    RUN_TEST(es_inherits_crossfile_csharp);
    RUN_TEST(es_inherits_crossfile_cpp);
    /* RED: Python, TypeScript, PHP, Kotlin (extraction bugs). */
    RUN_TEST(es_inherits_crossfile_python_red);
    RUN_TEST(es_inherits_crossfile_typescript_red);
    RUN_TEST(es_inherits_crossfile_php_red);
    RUN_TEST(es_php_include_occurrence_spans_and_sealing_guards);
    RUN_TEST(es_php_include_kind_distinguishes_use_and_unproven_lookup);
    RUN_TEST(es_inherits_php_bare_name_include_is_not_class_use);
    RUN_TEST(es_inherits_php_exact_dir_include_context);
    RUN_TEST(es_inherits_php_namespace_include_does_not_supply_global_class);
    RUN_TEST(es_inherits_php_missing_include_does_not_borrow_decoy);
    RUN_TEST(es_inherits_php_dynamic_include_does_not_borrow_decoy);
    RUN_TEST(es_inherits_php_conditional_include_does_not_authorize_class);
    RUN_TEST(es_inherits_php_duplicate_included_class_abstains);
    RUN_TEST(es_inherits_php_effectful_included_source_abstains);

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
    RUN_TEST(es_go_annotation_types_are_not_value_usage);
    RUN_TEST(es_go_index_assignment_reads_operand_and_index);

    /* ── FAMILY 8: DATA_FLOWS cross-file ─────────────────────── */
    /* Expected GREEN: route-intermediated path, language-independent. */
    RUN_TEST(es_data_flows_crossfile_python);

    /* ── FAMILY 9: TESTS cross-file ──────────────────────────── */
    /* Expected GREEN: Python + TypeScript test file conventions. */
    RUN_TEST(es_tests_crossfile_python);
    RUN_TEST(es_tests_crossfile_typescript);
}
