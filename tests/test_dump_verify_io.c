/*
 * test_dump_verify_io.c — I/O-level coverage for the #334 plausibility gate.
 *
 * Drives cbm_dump_verify_is_degraded against a real on-disk SQLite store and
 * the actual cbm_store_count_nodes / cbm_store_open_path_query path used by
 * resolve_store(). Includes a fork/crash port of bulk_crash_recovery proving
 * uncommitted WAL frames are discarded on the next open and the gate trips when
 * persisted rows fall short of the committed dump intent.
 *
 * Store-linked (excluded from the fast test-foundation target); runs under the
 * full test-runner. The pure-function matrix lives in test_dump_verify.c.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/dump_verify.h"
#include "test_framework.h"

#include <stdio.h>
#include <stdlib.h>
#include <store/store.h>
#ifndef _WIN32
#include <sys/wait.h>
#include <unistd.h>
#endif

#define DV_PROJECT "io_dump_verify"

static void dv_temp_path(char *buf, size_t n) {
    snprintf(buf, n, "%s/cmm_dump_verify_%d.db", cbm_tmpdir(), (int)getpid());
}

static void dv_cleanup(const char *path) {
    remove(path);
    char aux[640];
    snprintf(aux, sizeof(aux), "%s-wal", path);
    remove(aux);
    snprintf(aux, sizeof(aux), "%s-shm", path);
    remove(aux);
}

/* Commit `count` distinct nodes to the on-disk store at `path`. */
static int dv_write_nodes(const char *path, int count) {
    cbm_store_t *s = cbm_store_open_path(path);
    if (!s)
        return -1;
    if (cbm_store_upsert_project(s, DV_PROJECT, "/tmp/io_dump_verify") != CBM_STORE_OK) {
        cbm_store_close(s);
        return -1;
    }
    cbm_store_begin(s);
    for (int i = 0; i < count; i++) {
        char qn[64];
        snprintf(qn, sizeof(qn), "io.node.%d", i);
        cbm_node_t node = {.project = DV_PROJECT,
                           .label = "Function",
                           .name = "n",
                           .qualified_name = qn,
                           .file_path = "io.c",
                           .start_line = i,
                           .end_line = i};
        if (cbm_store_upsert_node(s, &node) < 0) {
            cbm_store_close(s);
            return -1;
        }
    }
    cbm_store_commit(s);
    cbm_store_close(s);
    return count;
}

/* Persisted node count via the same query path resolve_store() uses. */
static int dv_persisted_nodes(const char *path) {
    cbm_store_t *s = cbm_store_open_path_query(path);
    if (!s)
        return CBM_STORE_ERR;
    int n = cbm_store_count_nodes(s, DV_PROJECT);
    cbm_store_close(s);
    return n;
}

TEST(dump_verify_io_full_persist_ok) {
    char path[640];
    dv_temp_path(path, sizeof(path));
    dv_cleanup(path);

    ASSERT_EQ(dv_write_nodes(path, 200), 200);
    int persisted = dv_persisted_nodes(path);
    ASSERT_EQ(persisted, 200);
    /* committed == persisted: a faithful dump is never degraded. */
    ASSERT_FALSE(cbm_dump_verify_is_degraded(200, persisted, CBM_DUMP_VERIFY_DEFAULT_RATIO,
                                             CBM_DUMP_VERIFY_MIN_FLOOR));

    dv_cleanup(path);
    PASS();
}

TEST(dump_verify_io_shortfall_after_delete) {
    char path[640];
    dv_temp_path(path, sizeof(path));
    dv_cleanup(path);

    ASSERT_EQ(dv_write_nodes(path, 1000), 1000);
    ASSERT_FALSE(cbm_dump_verify_is_degraded(1000, dv_persisted_nodes(path),
                                             CBM_DUMP_VERIFY_DEFAULT_RATIO,
                                             CBM_DUMP_VERIFY_MIN_FLOOR));

    /* Simulate persisted loss: most rows vanish while the committed intent
     * (1000) is unchanged — the #334 silent-degradation signature. */
    cbm_store_t *s = cbm_store_open_path(path);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_exec(s, "DELETE FROM nodes WHERE start_line >= 100"), CBM_STORE_OK);
    cbm_store_close(s);

    int persisted = dv_persisted_nodes(path);
    ASSERT_EQ(persisted, 100);
    ASSERT_TRUE(cbm_dump_verify_is_degraded(1000, persisted, CBM_DUMP_VERIFY_DEFAULT_RATIO,
                                            CBM_DUMP_VERIFY_MIN_FLOOR));

    dv_cleanup(path);
    PASS();
}

#ifndef _WIN32
TEST(dump_verify_io_fork_crash_uncommitted_discarded) {
    char path[640];
    dv_temp_path(path, sizeof(path));
    dv_cleanup(path);

    /* Baseline above the sparse floor is durably committed before the crash. */
    ASSERT_EQ(dv_write_nodes(path, 60), 60);

    pid_t pid = fork();
    ASSERT_TRUE(pid >= 0);
    if (pid == 0) {
        /* Child: write a large uncommitted batch then crash without commit. */
        cbm_store_t *s = cbm_store_open_path(path);
        if (!s)
            _exit(2);
        cbm_store_begin(s);
        for (int i = 1000; i < 6000; i++) {
            char qn[64];
            snprintf(qn, sizeof(qn), "io.node.%d", i);
            cbm_node_t node = {.project = DV_PROJECT,
                               .label = "Function",
                               .name = "n",
                               .qualified_name = qn,
                               .file_path = "io.c",
                               .start_line = i,
                               .end_line = i};
            cbm_store_upsert_node(s, &node);
        }
        /* No commit, no clean close: simulate a crash mid-dump. */
        _exit(0);
    }

    int status = 0;
    ASSERT_TRUE(waitpid(pid, &status, 0) == pid);

    /* WAL recovery discards the child's uncommitted frames: only the baseline
     * survives, so persisted (60) falls far short of the dump intent (5060). */
    int persisted = dv_persisted_nodes(path);
    ASSERT_EQ(persisted, 60);
    ASSERT_TRUE(cbm_dump_verify_is_degraded(5060, persisted, CBM_DUMP_VERIFY_DEFAULT_RATIO,
                                            CBM_DUMP_VERIFY_MIN_FLOOR));

    dv_cleanup(path);
    PASS();
}
#endif /* _WIN32 */

SUITE(dump_verify_io) {
    RUN_TEST(dump_verify_io_full_persist_ok);
    RUN_TEST(dump_verify_io_shortfall_after_delete);
#ifndef _WIN32
    RUN_TEST(dump_verify_io_fork_crash_uncommitted_discarded);
#endif
}
