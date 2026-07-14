/*
 * repro_issue842.c -- Reproduce-first / regression guard for bug #842.
 *
 * Issue #842: "graph: same clobber class as #787 in env-access CONFIGURES
 *             sources and resolve_file_throws"
 *
 * Follow-up disclosed during #828's review (stable USAGE-edge ownership,
 * #787): #828 fixed three source-node finders (find_enclosing_node in
 * pass_usages.c, find_source_node in pass_parallel.c, calls_find_source in
 * pass_calls.c) so a class-level construct's enclosing-QN lookup that lands
 * on the shared Folder/Project node (directory-module languages, Java/Go)
 * falls back to the per-file File node instead of conflating every
 * same-package file onto one source. #828 deliberately left two adjacent
 * direct enclosing-QN lookups out of scope:
 *
 *   1. create_env_configures_for_file (pass_definitions.c) — CONFIGURES edge
 *      source for an env-var access.
 *   2. resolve_file_throws (pass_parallel.c) — THROWS/RAISES edge source for
 *      a thrown/raised exception.
 *
 * Both called cbm_gbuf_find_by_qn(enclosing_func_qn) directly, with no guard
 * against the lookup landing on a Folder/Project node — the exact #787 root
 * cause, just unguarded at two more sites.
 *
 * FIXTURE: two directory-module languages, one per call site, both exercising
 * the same "no enclosing function -> enclosing_func_qn falls back to the
 * MODULE QN" condition (helpers.c:900-902), which for a directory-based-module
 * language collides with the shared package Folder node's QN:
 *
 *   - THROWS: two same-package Java files (owner/ServiceA.java,
 *     owner/ServiceB.java), each throwing a shared exception type from a
 *     static initializer block (no enclosing method/constructor).
 *   - CONFIGURES: two same-package Go files (goenv/service_a.go,
 *     goenv/service_b.go), each reading the same env var in a package-level
 *     var declaration (no enclosing function). Go was used here instead of
 *     Java because Java's env-access detection (System.getenv) is separately
 *     broken -- extract_env_key_from_call reads a "function" field that
 *     Java's method_invocation node doesn't have (it uses "object"/"name"
 *     instead), so no EnvVar node is ever created for Java. That is a
 *     pre-existing, unrelated bug in the extraction layer, out of scope here.
 *
 * Correct THROWS sources targeting the ConfigException Class node, and
 * CONFIGURES sources targeting the DB_URL EnvVar node, must each be exactly
 * two distinct files.
 *
 * RED on unfixed HEAD: both same-package files collapse onto the single
 * shared Folder/Module node for their package, so each query returns at most
 * 1 distinct source instead of 2. Confirmed empirically that this only
 * reproduces on resolve_file_throws's parallel (>MIN_FILES_FOR_PARALLEL=50
 * files) worker-merge path -- the small sequential fixture already attributes
 * correctly even on unfixed HEAD, so the THROWS test pads with 60 inert
 * same-package filler classes (see build_parallel_fixture) to force that
 * path, mirroring #787's own two-fixture (sequential + parallel) precedent.
 * create_env_configures_for_file's CONFIGURES bug, by contrast, reproduces on
 * the small sequential fixture already, so the CONFIGURES test needs no
 * padding.
 */

#include <foundation/compat.h>
#include "test_framework.h"
#include "repro_harness.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ── Fixture files ──────────────────────────────────────────────────────── */

/* ConfigException: the type that will be a THROWS target. Named to avoid
 * "Error"/"Panic" so is_checked_exception (pass_parallel.c) classifies it as
 * a checked exception and emits a THROWS edge rather than RAISES. */
static const char k_config_exception[] =
    "package org.example.owner;\n"
    "\n"
    "public class ConfigException extends RuntimeException {\n"
    "    public ConfigException(String m) { super(m); }\n"
    "}\n";

/* ServiceA: env access + throw, both inside a static initializer — no
 * enclosing method, so both extractions record enclosing_func_qn as the
 * MODULE QN (the #787/#842 collision condition). */
static const char k_service_a[] =
    "package org.example.owner;\n"
    "\n"
    "public class ServiceA {\n"
    "    static {\n"
    "        String url = System.getenv(\"DB_URL\");\n"
    "        if (url == null) {\n"
    "            throw new ConfigException(\"DB_URL not set\");\n"
    "        }\n"
    "    }\n"
    "}\n";

/* ServiceB: same shape as ServiceA — same-package, same-class-level-only
 * env access and throw. */
static const char k_service_b[] =
    "package org.example.owner;\n"
    "\n"
    "public class ServiceB {\n"
    "    static {\n"
    "        String url = System.getenv(\"DB_URL\");\n"
    "        if (url == null) {\n"
    "            throw new ConfigException(\"DB_URL not set\");\n"
    "        }\n"
    "    }\n"
    "}\n";

/* Go env-access fixture: two same-package files, each reading DB_URL in a
 * package-level var declaration (no enclosing function -> module QN, the
 * same collision condition as the Java static blocks above). */
static const char k_go_service_a[] =
    "package goenv\n"
    "\n"
    "import \"os\"\n"
    "\n"
    "var dbURLA = os.Getenv(\"DB_URL\")\n";

static const char k_go_service_b[] =
    "package goenv\n"
    "\n"
    "import \"os\"\n"
    "\n"
    "var dbURLB = os.Getenv(\"DB_URL\")\n";

static const RFile k_files[] = {
    {"owner/ConfigException.java", k_config_exception},
    {"owner/ServiceA.java",        k_service_a},
    {"owner/ServiceB.java",        k_service_b},
    {"goenv/service_a.go",         k_go_service_a},
    {"goenv/service_b.go",         k_go_service_b},
};
static const int k_nfiles = (int)(sizeof(k_files) / sizeof(k_files[0]));

/* Expected distinct THROWS source files. */
static const char *k_expected_throws[] = {
    "owner/ServiceA.java",
    "owner/ServiceB.java",
};
static const int k_nexpected_throws = (int)(sizeof(k_expected_throws) / sizeof(k_expected_throws[0]));

/* Expected distinct CONFIGURES source files. */
static const char *k_expected_configures[] = {
    "goenv/service_a.go",
    "goenv/service_b.go",
};
static const int k_nexpected_configures =
    (int)(sizeof(k_expected_configures) / sizeof(k_expected_configures[0]));

/* resolve_file_throws only misattributes on the >MIN_FILES_FOR_PARALLEL=50
 * worker-merge path (pipeline.c) -- confirmed empirically: the 5-file
 * sequential fixture above already attributes THROWS correctly even on
 * unfixed HEAD (resolve_file_rw's shared find_source_node ran the same
 * lookup earlier in the same pass and the by-then-stable Folder/Module node,
 * protected from clobbering since #844, happens not to collide on the
 * sequential path for this construct). This mirrors #787's own "two faces"
 * (sequential upsert vs. parallel merge_update_existing) -- resolve_file_throws
 * apparently only has the parallel face. Padding past the threshold with
 * inert same-package filler classes reproduces it. */
enum { REPRO842_FILLER_COUNT = 60, REPRO842_NAME_SZ = 64, REPRO842_BODY_SZ = 192 };

/* Build the >50-file fixture into files[]/name_bufs[]/body_bufs[] (caller
 * arrays sized k_nfiles + REPRO842_FILLER_COUNT). Returns total file count.
 * Caller frees name_bufs/body_bufs entries [k_nfiles..total). */
static int build_parallel_fixture(RFile *files, char **name_bufs, char **body_bufs) {
    int n = 0;
    for (int i = 0; i < k_nfiles; i++) {
        files[n++] = k_files[i];
    }
    for (int i = 0; i < REPRO842_FILLER_COUNT; i++) {
        char *name = malloc(REPRO842_NAME_SZ);
        char *body = malloc(REPRO842_BODY_SZ);
        if (!name || !body) {
            free(name);
            free(body);
            break;
        }
        snprintf(name, REPRO842_NAME_SZ, "owner/Filler%02d.java", i);
        snprintf(body, REPRO842_BODY_SZ,
                 "package org.example.owner;\n"
                 "\n"
                 "public class Filler%02d {\n"
                 "    private int value%02d;\n"
                 "}\n",
                 i, i);
        name_bufs[n] = name;
        body_bufs[n] = body;
        files[n].name = name;
        files[n].content = body;
        n++;
    }
    return n;
}

/* ── Helper: collect distinct inbound-edge source file_paths for a node ──── */

/*
 * collect_edge_sources: index `files`/`nfiles`, find the node named
 * `node_name` carrying label `node_label`, walk all `edge_type` edges
 * targeting it, and write the distinct source file_paths (up to `cap`) into
 * `out`. DISTINCT-file (set) semantics: ownership is per FILE, so duplicate
 * paths are collapsed. Returns the number of distinct sources found (may be
 * truncated by `cap`), or -1 on setup/lookup failure.
 *
 * The caller is responsible for free()ing each string in out[0..return-1].
 */
static int collect_edge_sources(const RFile *files, int nfiles, const char *node_name,
                                const char *node_label, const char *edge_type, char **out,
                                int cap) {
    RProj lp;
    cbm_store_t *store = rh_index_files(&lp, files, nfiles);
    if (!store) {
        return -1;
    }

    cbm_node_t *candidates = NULL;
    int ncand = 0;
    int rc = cbm_store_find_nodes_by_name(store, lp.project, node_name, &candidates, &ncand);
    if (rc != CBM_STORE_OK || ncand == 0) {
        cbm_store_free_nodes(candidates, ncand);
        rh_cleanup(&lp, store);
        return -1;
    }

    int64_t target_id = 0;
    for (int i = 0; i < ncand; i++) {
        const char *lbl = candidates[i].label ? candidates[i].label : "";
        if (strcmp(lbl, node_label) == 0) {
            target_id = candidates[i].id;
            break;
        }
    }
    cbm_store_free_nodes(candidates, ncand);

    if (!target_id) {
        rh_cleanup(&lp, store);
        return -1;
    }

    cbm_edge_t *edges = NULL;
    int nedges = 0;
    rc = cbm_store_find_edges_by_target_type(store, target_id, edge_type, &edges, &nedges);
    if (rc != CBM_STORE_OK) {
        rh_cleanup(&lp, store);
        return -1;
    }

    int found = 0;
    for (int i = 0; i < nedges && found < cap; i++) {
        cbm_node_t src_node;
        if (cbm_store_find_node_by_id(store, edges[i].source_id, &src_node) != CBM_STORE_OK) {
            continue;
        }
        if (!src_node.file_path || !src_node.file_path[0]) {
            continue;
        }
        /* Set semantics: skip a file_path already collected. */
        int dup = 0;
        for (int j = 0; j < found; j++) {
            if (strcmp(out[j], src_node.file_path) == 0) {
                dup = 1;
                break;
            }
        }
        if (dup) {
            continue;
        }
        out[found++] = strdup(src_node.file_path);
    }

    cbm_store_free_edges(edges, nedges);
    rh_cleanup(&lp, store);
    return found;
}

/* Check that paths[] contains exactly the expected file suffixes, once each.
 * Returns 1 if all expected suffixes appear exactly once, 0 otherwise.
 * Prints a diagnostic on mismatch. */
static int check_sources_exact(char **paths, int count, const char **expected, int nexpected) {
    if (count != nexpected) {
        printf("    source count %d != expected %d\n", count, nexpected);
        for (int i = 0; i < count; i++) {
            printf("      got: %s\n", paths[i] ? paths[i] : "(null)");
        }
        return 0;
    }
    for (int e = 0; e < nexpected; e++) {
        int seen = 0;
        for (int i = 0; i < count; i++) {
            if (paths[i] && strstr(paths[i], expected[e])) {
                seen++;
            }
        }
        if (seen != 1) {
            printf("    expected suffix '%s' appears %d time(s) (want 1)\n", expected[e], seen);
            for (int i = 0; i < count; i++) {
                printf("      got: %s\n", paths[i] ? paths[i] : "(null)");
            }
            return 0;
        }
    }
    return 1;
}

/* ── Tests ──────────────────────────────────────────────────────────────── */

/*
 * repro_issue842_env_configures_exact_sources
 *
 * Class-level env access (static initializer, no enclosing method) must
 * attribute its CONFIGURES edge to its own file's File node, not the shared
 * package Folder node.
 *
 * RED on buggy HEAD: create_env_configures_for_file resolves `src` via a
 * direct cbm_gbuf_find_by_qn(enclosing_func_qn) with no dir-container guard,
 * so both files' static-block env accesses source from the one shared
 * Folder/Module node for org.example.owner — the distinct source set has 1
 * entry (whichever file's Module def was upserted/merged last) instead of 2.
 */
TEST(repro_issue842_env_configures_exact_sources) {
#define MAX_SRCS 16
    char *paths[MAX_SRCS];
    memset(paths, 0, sizeof(paths));

    int count = collect_edge_sources(k_files, k_nfiles, "DB_URL", "EnvVar", "CONFIGURES", paths, MAX_SRCS);
    if (count < 0) {
        FAIL("fixture indexing or DB_URL lookup failed");
    }

    int ok = check_sources_exact(paths, count, k_expected_configures, k_nexpected_configures);

    for (int i = 0; i < MAX_SRCS; i++) {
        free(paths[i]);
        paths[i] = NULL;
    }
#undef MAX_SRCS

    if (!ok) {
        FAIL("CONFIGURES edge source file_paths do not match expected set");
    }
    PASS();
}

/*
 * repro_issue842_throws_exact_sources
 *
 * Class-level throw (static initializer, no enclosing method) must attribute
 * its THROWS edge to its own file's File node, not the shared package Folder
 * node. Padded past MIN_FILES_FOR_PARALLEL=50 (pipeline.c) to force the
 * worker-merge path -- confirmed empirically that resolve_file_throws only
 * misattributes there, not on the small sequential fixture (see
 * build_parallel_fixture's comment).
 *
 * RED on buggy HEAD: resolve_file_throws resolves `src` via a direct
 * cbm_gbuf_find_by_qn(enclosing_func_qn) with no dir-container guard and no
 * File-node fallback at all (a miss just skips the edge) — both files'
 * static-block throws source from the one shared Folder/Module node, so the
 * distinct source set has 1 entry instead of 2.
 */
TEST(repro_issue842_throws_exact_sources) {
#define MAX_SRCS 16
#define MAX_TOTAL_FILES (k_nfiles + REPRO842_FILLER_COUNT)
    RFile files[MAX_TOTAL_FILES];
    char *name_bufs[MAX_TOTAL_FILES];
    char *body_bufs[MAX_TOTAL_FILES];
    memset(name_bufs, 0, sizeof(name_bufs));
    memset(body_bufs, 0, sizeof(body_bufs));
    int total = build_parallel_fixture(files, name_bufs, body_bufs);

    char *paths[MAX_SRCS];
    memset(paths, 0, sizeof(paths));

    int count = collect_edge_sources(files, total, "ConfigException", "Class", "THROWS", paths, MAX_SRCS);

    for (int i = 0; i < total; i++) {
        free(name_bufs[i]);
        free(body_bufs[i]);
    }
#undef MAX_TOTAL_FILES

    if (count < 0) {
        FAIL("fixture indexing or ConfigException lookup failed");
    }

    int ok = check_sources_exact(paths, count, k_expected_throws, k_nexpected_throws);

    for (int i = 0; i < MAX_SRCS; i++) {
        free(paths[i]);
        paths[i] = NULL;
    }
#undef MAX_SRCS

    if (!ok) {
        FAIL("THROWS edge source file_paths do not match expected set");
    }
    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────────────── */
SUITE(repro_issue842) {
    RUN_TEST(repro_issue842_env_configures_exact_sources);
    RUN_TEST(repro_issue842_throws_exact_sources);
}
