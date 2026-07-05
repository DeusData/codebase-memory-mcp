/*
 * repro_issue787.c -- Reproduce-first / regression guard for bug #787.
 *
 * Issue #787: "Nondeterministic Java USAGE-edge misattribution across
 *             same-package files — USAGE sources vary per run"
 *
 * ROOT CAUSE (introduced by PR #667, merge commit 36d83280):
 *   PR #667 switched Java/Go module QNs from filename-stem-based
 *   ("proj.pkg.OwnerController") to DIRECTORY-based ("proj.pkg"), so all
 *   files in the same Java package share a single module QN.  This QN also
 *   collides with the pipeline's Folder node for that directory.
 *
 *   When a source-node finder (find_enclosing_node in pass_usages.c,
 *   find_source_node in pass_parallel.c, calls_find_source in pass_calls.c)
 *   resolves a CLASS-LEVEL usage (e.g., a field type reference
 *   `private IRepository repo;`), the enclosing_func_qn equals the shared
 *   directory module QN.  Looking that QN up in the graph buffer returns the
 *   ONE Folder/Project node shared by every file in the package.  That shared
 *   node's file_path was clobbered by each file's always-emitted Module def —
 *   through TWO code paths:
 *     - sequential: cbm_gbuf_upsert_node updated name/file_path/range in
 *       place (its #667 guard only protected the label);
 *     - parallel:   merge_update_existing applied unconditional "src wins"
 *       when merging worker-local gbufs, even relabelling the Folder node to
 *       Module — and worker merge ORDER varies per run (the race flavor).
 *   USAGE edges from all same-package class-level references then share a
 *   single source node whose file_path is whichever file won the last write.
 *
 *   Spring-petclinic oracle:
 *     MATCH (c {name:'OwnerRepository'})<-[r:USAGE]-(m) RETURN m.file_path
 *   should yield exactly 7 distinct files; HEAD (dcf98dc) returns 4-6 with
 *   bogus entries varying per run.
 *
 * FIX (three co-ordinated parts):
 *   1. graph_buffer.c cbm_gbuf_upsert_node: a Module def colliding with a
 *      Project/Folder node no longer updates ANY field (was: label only).
 *   2. graph_buffer.c merge_update_existing: same guard on the parallel
 *      worker-gbuf merge path.
 *   3. pipeline finders (pass_usages/pass_parallel/pass_calls): a lookup that
 *      lands on a structural directory container (Folder/Project — see
 *      cbm_pipeline_node_is_dir_container) is treated as a miss, falling
 *      through to the per-file File node, so every file's class-level usages
 *      attribute to that file's unique File node.
 *
 * FIXTURE DESIGN (minimal spring-petclinic analog):
 *   Package owner/ — three Java files:
 *     IRepository.java        defines interface IRepository  (the used type)
 *     ServiceA.java           field `IRepository repo;`  (class-level USAGE)
 *     ServiceB.java           field `IRepository repo;`  (class-level USAGE)
 *   Package web/ — one Java file:
 *     WebController.java      field `IRepository repo;`  (cross-package USAGE)
 *
 *   Correct USAGE edges to IRepository must source from EXACTLY:
 *     owner/ServiceA.java, owner/ServiceB.java, web/WebController.java
 *   (3 distinct source files, each appearing exactly once).
 *
 *   Pre-fix: owner/ServiceA.java and owner/ServiceB.java share the same
 *   source node (the "proj.owner" Folder/Module node), so the query returns
 *   at most 2 distinct file_paths (and one of the two is random).
 *   Post-fix: each file gets its own source → 3 distinct, stable file_paths.
 *
 * WHY RED on buggy HEAD:
 *   The stability assertion (same source set across N runs) fires because the
 *   shared Folder node's file_path changes between indexing passes (different
 *   file write orders can produce different results each run).  The exactness
 *   assertion (set equals the expected three files) fires because the owner/
 *   package's two callers are conflated into one node whose file_path is
 *   whichever of ServiceA.java / ServiceB.java was extracted last.
 *
 * NOTE: this test uses N=5 independent full-pipeline index+query rounds so
 *   that any ordering-based nondeterminism has several opportunities to
 *   surface.  Each round opens a fresh in-memory project (different tmpdir →
 *   different project name → completely isolated store).
 */

#include <foundation/compat.h>
#include "test_framework.h"
#include "repro_harness.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ── Fixture files ──────────────────────────────────────────────────────── */

/* IRepository: the type that will be a USAGE target. */
static const char k_irepository[] =
    "package org.example.owner;\n"
    "\n"
    "public interface IRepository {\n"
    "    void save(Object o);\n"
    "    Object findById(int id);\n"
    "}\n";

/* ServiceA: reference to IRepository ONLY at class level (field declaration —
 * no constructor parameter).  A class-level reference's enclosing scope is the
 * MODULE QN, which for Java is the package directory QN shared by every file
 * in owner/ — the collision at the heart of #787.  Keeping the field as the
 * file's only IRepository reference makes the buggy collapse deterministic:
 * ServiceA.java and ServiceB.java always conflate to ONE source node
 * regardless of file processing order, so the distinct-source-file set is
 * short in every ordering (no false green when the "right" file happens to
 * win the last-writer race). */
static const char k_service_a[] =
    "package org.example.owner;\n"
    "\n"
    "public class ServiceA {\n"
    "    private IRepository repo;\n"
    "\n"
    "    public void doA() {\n"
    "        repo.save(\"a\");\n"
    "    }\n"
    "}\n";

/* ServiceB: same shape as ServiceA — class-level-only reference. */
static const char k_service_b[] =
    "package org.example.owner;\n"
    "\n"
    "public class ServiceB {\n"
    "    private IRepository repo;\n"
    "\n"
    "    public void doB() {\n"
    "        repo.save(\"b\");\n"
    "    }\n"
    "}\n";

/* WebController: cross-package reference to IRepository. */
static const char k_web_controller[] =
    "package org.example.web;\n"
    "\n"
    "import org.example.owner.IRepository;\n"
    "\n"
    "public class WebController {\n"
    "    private IRepository repo;\n"
    "\n"
    "    public WebController(IRepository repo) {\n"
    "        this.repo = repo;\n"
    "    }\n"
    "\n"
    "    public void handle() {\n"
    "        repo.save(\"web\");\n"
    "    }\n"
    "}\n";

static const RFile k_files[] = {
    {"owner/IRepository.java",  k_irepository},
    {"owner/ServiceA.java",     k_service_a},
    {"owner/ServiceB.java",     k_service_b},
    {"web/WebController.java",  k_web_controller},
};
static const int k_nfiles = (int)(sizeof(k_files) / sizeof(k_files[0]));

/* The full pipeline switches to the parallel (worker) path above
 * MIN_FILES_FOR_PARALLEL=50 files (pipeline.c). The bug has TWO faces:
 *   - sequential: cbm_gbuf_upsert_node clobbered the Folder node in place;
 *   - parallel:   merge_update_existing clobbered it during worker-local gbuf
 *                 merge (worker order → run-to-run nondeterminism).
 * The 4-file fixture only exercises the sequential face, so a second fixture
 * pads the same core files with FILLER_COUNT inert same-package classes to
 * push the file count over the threshold and exercise the merge face too. */
enum { REPRO787_FILLER_COUNT = 60, REPRO787_NAME_SZ = 64, REPRO787_BODY_SZ = 192 };

/* Build the >50-file fixture into files[]/name_bufs[]/body_bufs[] (caller
 * arrays sized k_nfiles + REPRO787_FILLER_COUNT). Returns total file count.
 * Caller frees name_bufs/body_bufs entries [k_nfiles..total). */
static int build_parallel_fixture(RFile *files, char **name_bufs, char **body_bufs) {
    int n = 0;
    for (int i = 0; i < k_nfiles; i++) {
        files[n++] = k_files[i];
    }
    for (int i = 0; i < REPRO787_FILLER_COUNT; i++) {
        char *name = malloc(REPRO787_NAME_SZ);
        char *body = malloc(REPRO787_BODY_SZ);
        if (!name || !body) {
            free(name);
            free(body);
            break;
        }
        snprintf(name, REPRO787_NAME_SZ, "owner/Filler%02d.java", i);
        snprintf(body, REPRO787_BODY_SZ,
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

/* ── Helper: collect USAGE source file_paths for IRepository ───────────── */

/*
 * collect_usage_sources: index the fixture once, find the IRepository node,
 * walk all USAGE edges that target it, and write the source file_paths
 * (up to `cap`) into `out`.  DISTINCT-file (set) semantics match the petclinic
 * oracle ("exactly 7 user files"): one file may legitimately contribute several
 * USAGE edges from different source nodes (a method-scoped reference sources
 * from the Method node, a class-level field reference from the file-scope
 * node) — ownership is per FILE, so duplicate paths are collapsed.  Returns the
 * number of distinct sources found (may be truncated by `cap`).
 *
 * The caller is responsible for free()ing each string in `out[0..return-1]`.
 * Returns -1 on setup failure.
 */
static int collect_usage_sources_n(const RFile *files, int nfiles, char **out, int cap) {
    RProj lp;
    cbm_store_t *store = rh_index_files(&lp, files, nfiles);
    if (!store) {
        return -1;
    }

    /* Locate the IRepository node by name. */
    cbm_node_t *candidates = NULL;
    int ncand = 0;
    int rc = cbm_store_find_nodes_by_name(store, lp.project, "IRepository",
                                         &candidates, &ncand);
    if (rc != CBM_STORE_OK || ncand == 0) {
        cbm_store_free_nodes(candidates, ncand);
        rh_cleanup(&lp, store);
        return -1;
    }

    /* Pick the Interface / Class node (label check). */
    int64_t target_id = 0;
    for (int i = 0; i < ncand; i++) {
        const char *lbl = candidates[i].label ? candidates[i].label : "";
        if (strcmp(lbl, "Interface") == 0 || strcmp(lbl, "Class") == 0) {
            target_id = candidates[i].id;
            break;
        }
    }
    cbm_store_free_nodes(candidates, ncand);

    if (!target_id) {
        rh_cleanup(&lp, store);
        return -1;
    }

    /* Walk inbound USAGE edges. */
    cbm_edge_t *edges = NULL;
    int nedges = 0;
    rc = cbm_store_find_edges_by_target_type(store, target_id, "USAGE", &edges, &nedges);
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
static int check_sources_exact(char **paths, int count,
                               const char **expected, int nexpected) {
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
            printf("    expected suffix '%s' appears %d time(s) (want 1)\n",
                   expected[e], seen);
            for (int i = 0; i < count; i++) {
                printf("      got: %s\n", paths[i] ? paths[i] : "(null)");
            }
            return 0;
        }
    }
    return 1;
}

/* Expected distinct USAGE source files (shared by all three tests). */
static const char *k_expected[] = {
    "owner/ServiceA.java",
    "owner/ServiceB.java",
    "web/WebController.java",
};
static const int k_nexpected = (int)(sizeof(k_expected) / sizeof(k_expected[0]));

/* ── Tests ──────────────────────────────────────────────────────────────── */

/*
 * repro_issue787_usage_exact_sources
 *
 * Single-run correctness check (sequential pipeline path): after indexing
 * the 4-file fixture exactly once, the USAGE edges targeting IRepository
 * must source from exactly the three expected files.
 *
 * RED on buggy HEAD: the two same-package callers (ServiceA.java, ServiceB.java)
 * collapse onto a single shared Folder/Module node whose file_path is
 * whichever of the package's files was processed last.  The distinct source
 * set has 2 entries instead of 3 (possibly including a bogus non-referencing
 * file such as IRepository.java itself).
 */
TEST(repro_issue787_usage_exact_sources) {
#define MAX_SRCS 16
    char *paths[MAX_SRCS];
    memset(paths, 0, sizeof(paths));

    int count = collect_usage_sources_n(k_files, k_nfiles, paths, MAX_SRCS);
    if (count < 0) {
        FAIL("fixture indexing or IRepository lookup failed");
    }

    int ok = check_sources_exact(paths, count, k_expected, k_nexpected);

    for (int i = 0; i < MAX_SRCS; i++) {
        free(paths[i]);
        paths[i] = NULL;
    }
#undef MAX_SRCS

    if (!ok) {
        FAIL("USAGE edge source file_paths do not match expected set");
    }
    PASS();
}

/*
 * repro_issue787_usage_stable_across_runs
 *
 * Stability check: index the same fixture N=5 times independently (each
 * run uses a fresh tmpdir + fresh DB), collect the USAGE source file_paths,
 * and assert they are IDENTICAL across all runs.
 *
 * RED on buggy HEAD: the shared Folder/Module node for "proj.owner" is
 * upserted once per file in the package; its file_path is set to the last
 * file written.  Across runs the write order may differ (filesystem readdir
 * order, thread scheduling, etc.), so the collapsed source file_path varies,
 * and the set of paths returned by the query changes run to run.
 *
 * Even if the first-run result happened to be correct, the stability
 * assertion would still catch a subsequent run that diverged.
 *
 * N=5 gives five independent opportunities to expose nondeterminism without
 * making the test prohibitively slow (each index+query round is < 1 s).
 */
TEST(repro_issue787_usage_stable_across_runs) {
#define N_RUNS 5
#define MAX_SRCS 16

    /* Collect results across N runs. */
    char *all_paths[N_RUNS][MAX_SRCS];
    int   all_counts[N_RUNS];
    memset(all_paths,  0, sizeof(all_paths));
    memset(all_counts, 0, sizeof(all_counts));

    for (int run = 0; run < N_RUNS; run++) {
        all_counts[run] = collect_usage_sources_n(k_files, k_nfiles, all_paths[run], MAX_SRCS);
        if (all_counts[run] < 0) {
            /* Free already-allocated strings from prior runs. */
            for (int r = 0; r <= run; r++) {
                for (int i = 0; i < MAX_SRCS; i++) { free(all_paths[r][i]); }
            }
            FAIL("fixture indexing or IRepository lookup failed on one run");
        }
    }

    /* Assert all runs agree with run 0. */
    int stable = 1;
    for (int run = 1; run < N_RUNS; run++) {
        if (all_counts[run] != all_counts[0]) {
            printf("    run %d returned %d sources, run 0 returned %d\n",
                   run, all_counts[run], all_counts[0]);
            stable = 0;
            continue;
        }
        /* Each path in run 0 must appear (by suffix) in run `run`. */
        for (int i = 0; i < all_counts[0]; i++) {
            if (!all_paths[0][i]) {
                continue;
            }
            int found = 0;
            for (int j = 0; j < all_counts[run]; j++) {
                if (all_paths[run][j] &&
                    strcmp(all_paths[0][i], all_paths[run][j]) == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                printf("    run %d missing source '%s' (present in run 0)\n",
                       run, all_paths[0][i]);
                stable = 0;
            }
        }
    }

    for (int r = 0; r < N_RUNS; r++) {
        for (int i = 0; i < MAX_SRCS; i++) { free(all_paths[r][i]); }
    }

    if (!stable) {
        FAIL("USAGE source file_paths differ across runs — nondeterministic");
    }

    /* Also check correctness of run 0. */
    char *paths0[MAX_SRCS];
    memset(paths0, 0, sizeof(paths0));
    int count0 = collect_usage_sources_n(k_files, k_nfiles, paths0, MAX_SRCS);
    if (count0 < 0) {
        FAIL("verification re-index failed");
    }
    int ok = check_sources_exact(paths0, count0, k_expected, k_nexpected);
    for (int i = 0; i < MAX_SRCS; i++) { free(paths0[i]); }

    if (!ok) {
        FAIL("USAGE sources are stable but do not match expected set");
    }

#undef N_RUNS
#undef MAX_SRCS
    PASS();
}

/*
 * repro_issue787_usage_stable_parallel
 *
 * PARALLEL-path check: same core fixture padded past MIN_FILES_FOR_PARALLEL
 * (>50 files) so index_repository takes the worker/merge path, repeated N=3
 * times.  Asserts the distinct USAGE source set equals the expected files on
 * every run.
 *
 * RED on buggy HEAD via a DIFFERENT mechanism than the sequential tests:
 * merge_update_existing (graph_buffer.c) applied unconditional "src wins" when
 * a worker-local gbuf's per-file Module def (directory QN) collided with the
 * main gbuf's Folder node, relabelling it Module and setting its file_path to
 * whichever worker merged last — worker scheduling makes the collapsed source
 * file vary RUN TO RUN (the race flavor reported in #787).
 */
TEST(repro_issue787_usage_stable_parallel) {
#define N_RUNS_PAR 3
#define MAX_SRCS 80
    RFile files[(int)(sizeof(k_files) / sizeof(k_files[0])) + REPRO787_FILLER_COUNT];
    char *name_bufs[(int)(sizeof(k_files) / sizeof(k_files[0])) + REPRO787_FILLER_COUNT];
    char *body_bufs[(int)(sizeof(k_files) / sizeof(k_files[0])) + REPRO787_FILLER_COUNT];
    memset(name_bufs, 0, sizeof(name_bufs));
    memset(body_bufs, 0, sizeof(body_bufs));
    int total = build_parallel_fixture(files, name_bufs, body_bufs);

    int all_ok = 1;
    for (int run = 0; run < N_RUNS_PAR && all_ok; run++) {
        char *paths[MAX_SRCS];
        memset(paths, 0, sizeof(paths));
        int count = collect_usage_sources_n(files, total, paths, MAX_SRCS);
        if (count < 0) {
            all_ok = 0;
            printf("    parallel run %d: indexing or lookup failed\n", run + 1);
        } else if (!check_sources_exact(paths, count, k_expected, k_nexpected)) {
            printf("    ^ parallel run %d\n", run + 1);
            all_ok = 0;
        }
        for (int i = 0; i < MAX_SRCS; i++) {
            free(paths[i]);
        }
    }

    for (int i = 0; i < total; i++) {
        free(name_bufs[i]);
        free(body_bufs[i]);
    }
#undef N_RUNS_PAR
#undef MAX_SRCS

    if (!all_ok) {
        FAIL("parallel-path USAGE sources wrong or unstable");
    }
    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────────────── */
SUITE(repro_issue787) {
    RUN_TEST(repro_issue787_usage_exact_sources);
    RUN_TEST(repro_issue787_usage_stable_across_runs);
    RUN_TEST(repro_issue787_usage_stable_parallel);
}
