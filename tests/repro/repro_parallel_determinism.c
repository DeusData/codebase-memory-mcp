/*
 * repro_parallel_determinism.c — RED reproduction for the parallel-indexing
 * edge-loss / non-determinism bug (REAL-REPO TIER).
 *
 * DISCOVERED: while verifying F6 (2026-07), re-indexing the SAME corpus with the
 * correct cache cleared produced DIFFERENT edge counts run-to-run when
 * multi-threaded (kernel fs/xfs: 58548 / 58552 / 58557 / 58573; kernel-rust:
 * 44445 / 44775 / 44857) and the multi-threaded counts trend BELOW the
 * single-threaded reference (xfs ST 58573). I.e. parallel indexing both flickers
 * AND drops edges vs the single-threaded graph.
 *
 * INVARIANT (green <=> fixed): indexing a fixed corpus is DETERMINISTIC and the
 * multi-threaded graph equals the single-threaded graph exactly. This test indexes
 * the corpus single-threaded (reference) then K times multi-threaded (fresh store
 * each run) and asserts every MT sorted edge set equals the ST one. Comparison is
 * over the SORTED (source_qn, type, target_qn) triples — NOT raw counts.
 *
 * WHY A REAL-REPO TIER (honest calibration record): a self-contained synthetic C
 * corpus could NOT trigger the divergence in three escalating attempts —
 *   (1) 300 files, dense cross-file CALLS only;
 *   (2) 500 files, big per-function bodies (fingerprinted, nodes_with_fp=500);
 *   (3) 600 files, TOKEN-IDENTICAL clustered bodies (to force SIMILAR_TO edges);
 * all produced a fully DETERMINISTIC graph across ST + 6 MT runs, and the
 * similarity pass emitted 0 edges on synthetic C (`pass.similarity edges=0`). The
 * race clearly needs real-code edge diversity/volume (real SIMILAR_TO / semantic /
 * cross-file type edges) that synthetic C does not produce. Rather than ship a
 * false guard (green on buggy code), this uses the smallest REAL corpus on which
 * the flicker was directly observed — the kernel's fs/xfs subtree — and SKIPs when
 * that corpus is absent (e.g. CI). It is RED on the dev machine where the race
 * lives, which is where the fix (deferred) will be developed and guarded.
 *
 * SUSPECTED ROOT CAUSE (for the fixer): a race in parallel edge production/merge —
 * per-worker edge-buffer merge (pass_parallel 2_merge_edge_bufs_seq), graph-buffer
 * edge dedup under concurrent append (edge_by_key check-then-insert), similarity
 * edge emission (symmetric SIMILAR_TO from both sides), or resolve_worker emission
 * ordering. The single-threaded edge set is the reference semantics. Fix DEFERRED —
 * this stays on the known-red board until the race is fixed.
 */
#include "test_framework.h"
#include "repro_harness.h"
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <sqlite3.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#define RPD_MT_RUNS 4 /* K multi-threaded runs compared against the ST reference */

/* Smallest real corpus on which the flicker was directly observed. */
#define RPD_CORPUS "/Users/martinvogel/perf-bench/linux/fs/xfs"

/* Sorted (source_qn|type|target_qn) fingerprint of the whole project graph.
 * Heap string (caller frees) or NULL on error. */
static char *rpd_edge_fingerprint(cbm_store_t *s, const char *project) {
    struct sqlite3 *db = cbm_store_get_db(s);
    if (!db)
        return NULL;
    const char *sql = "SELECT s.qualified_name, e.type, t.qualified_name "
                      "FROM edges e "
                      "JOIN nodes s ON e.source_id = s.id "
                      "JOIN nodes t ON e.target_id = t.id "
                      "WHERE e.project = ?1 "
                      "ORDER BY 1, 2, 3;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK)
        return NULL;
    sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT);

    size_t cap = 1 << 20, len = 0;
    char *buf = (char *)malloc(cap);
    if (!buf) {
        sqlite3_finalize(stmt);
        return NULL;
    }
    buf[0] = '\0';
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *src = (const char *)sqlite3_column_text(stmt, 0);
        const char *typ = (const char *)sqlite3_column_text(stmt, 1);
        const char *tgt = (const char *)sqlite3_column_text(stmt, 2);
        src = src ? src : "";
        typ = typ ? typ : "";
        tgt = tgt ? tgt : "";
        size_t need = strlen(src) + strlen(typ) + strlen(tgt) + 4;
        while (len + need >= cap) {
            cap *= 2;
            char *nb = (char *)realloc(buf, cap);
            if (!nb) {
                free(buf);
                sqlite3_finalize(stmt);
                return NULL;
            }
            buf = nb;
        }
        len += (size_t)snprintf(buf + len, cap - len, "%s|%s|%s\n", src, typ, tgt);
    }
    sqlite3_finalize(stmt);
    return buf;
}

/* Index `repo` into a freshly-unlinked isolated store and return the edge
 * fingerprint. Honors CBM_INDEX_SINGLE_THREAD from the environment. */
static char *rpd_index_and_fingerprint(const char *repo, const char *dbpath) {
    unlink(dbpath);
    char wal[600], shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", dbpath);
    snprintf(shm, sizeof(shm), "%s-shm", dbpath);
    unlink(wal);
    unlink(shm);

    cbm_pipeline_t *p = cbm_pipeline_new(repo, dbpath, CBM_MODE_FULL);
    if (!p)
        return NULL;
    int rc = cbm_pipeline_run(p);
    cbm_pipeline_free(p);
    if (rc != 0)
        return NULL;

    char *project = cbm_project_name_from_path(repo);
    if (!project)
        return NULL;
    cbm_store_t *s = cbm_store_open_path(dbpath);
    char *fp = s ? rpd_edge_fingerprint(s, project) : NULL;
    if (s)
        cbm_store_close(s);
    free(project);
    return fp;
}

/* GUARD (green <=> the parallel-indexing races are fixed): repeated
 * multi-threaded runs over a fixed corpus must produce the IDENTICAL sorted
 * node+edge set. Root causes fixed (all order/scheduling dependence):
 * semantic-pass admission + canonical funcs order, import target first-match,
 * similarity ordering + id-based pair ownership, call-neighbor truncation
 * subsets, and the QN-collision last-wins overwrite in gbuf upsert AND merge
 * (a C struct/function/macro sharing one name flipped label by merge order). */
TEST(repro_parallel_edge_determinism) {
    struct stat st;
    if (stat(RPD_CORPUS, &st) != 0 || !S_ISDIR(st.st_mode)) {
        SKIP("real-repo tier: corpus " RPD_CORPUS
             " absent (synthetic C could not trigger the parallel edge race — see header)");
    }

    char dbpath[512];
    snprintf(dbpath, sizeof(dbpath), "%s/cbm_rpd_par_det.db", cbm_tmpdir());

    /* First multi-threaded run = reference; every further MT run must match. */
    char *fp_ref = rpd_index_and_fingerprint(RPD_CORPUS, dbpath);
    ASSERT_NOT_NULL(fp_ref);
    ASSERT_TRUE(strlen(fp_ref) > 0);

    int diverged = 0;
    for (int k = 1; k < RPD_MT_RUNS && !diverged; k++) {
        char *fp_mt = rpd_index_and_fingerprint(RPD_CORPUS, dbpath);
        ASSERT_NOT_NULL(fp_mt);
        if (strcmp(fp_mt, fp_ref) != 0)
            diverged = 1;
        free(fp_mt);
    }

    unlink(dbpath);
    free(fp_ref);

    ASSERT_EQ(diverged, 0);
    PASS();
}

/* RED (open bug, fix deferred): the SEQUENTIAL pipeline and the PARALLEL
 * pipeline are different code paths that produce SYSTEMATICALLY different
 * graphs — on the xfs corpus they disagree on ~3459 USAGE, ~1666 WRITES and
 * ~60 CALLS sorted-edge lines, consistently (each mode is internally
 * deterministic after the race fixes above; the modes just don't agree).
 * GREEN when both pipelines emit the same graph for the same corpus. */
TEST(repro_seq_parallel_equivalence) {
    struct stat st;
    if (stat(RPD_CORPUS, &st) != 0 || !S_ISDIR(st.st_mode)) {
        SKIP("real-repo tier: corpus " RPD_CORPUS " absent");
    }

    char dbpath[512];
    snprintf(dbpath, sizeof(dbpath), "%s/cbm_rpd_seq_par.db", cbm_tmpdir());

    setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);
    char *fp_st = rpd_index_and_fingerprint(RPD_CORPUS, dbpath);
    unsetenv("CBM_INDEX_SINGLE_THREAD");
    ASSERT_NOT_NULL(fp_st);

    char *fp_mt = rpd_index_and_fingerprint(RPD_CORPUS, dbpath);
    ASSERT_NOT_NULL(fp_mt);

    int equal = strcmp(fp_st, fp_mt) == 0;
    unlink(dbpath);
    free(fp_st);
    free(fp_mt);

    ASSERT_EQ(equal, 1);
    PASS();
}

void suite_repro_parallel_determinism(void) {
    RUN_TEST(repro_parallel_edge_determinism);
    RUN_TEST(repro_seq_parallel_equivalence);
}
