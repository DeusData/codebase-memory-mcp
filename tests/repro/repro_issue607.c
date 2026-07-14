/*
 * repro_issue607.c -- Reproduce-first / regression guard for bug #607.
 *
 * Issue #607: "installing again via install script is dark pattern:
 *              'rebuild index' message followed by delete index action"
 *
 * ORIGINAL DESTROYING CODE PATH (pre-fix):
 *   src/cli/cli.c  cbm_cmd_install()  printed
 *     "Found %d existing index(es) that must be rebuilt:\n"
 *   then called cbm_remove_indexes(home) which unlinked every .db and NEVER
 *   rebuilt. The word "rebuilt" implied preservation; the action was deletion.
 *   The user's indexed graph was silently, irrecoverably destroyed.
 *
 * APPROVED FIX (#607):
 *   The install-time index handling was extracted into a testable helper:
 *
 *     int cbm_install_handle_existing_indexes(const char *home,
 *                                             bool reset, bool dry_run);
 *
 *   Default (reset=false): PRESERVE the indexes. The helper prints an honest
 *   "Keeping them" message + lists them and returns 1 WITHOUT deleting
 *   anything. Deletion was never a schema requirement (the store uses
 *   CREATE TABLE IF NOT EXISTS, no migrations); re-indexing after install
 *   picks up extraction improvements without destroying data.
 *
 *   Opt-in (reset=true, via `install --reset-indexes`): keep the original
 *   prompt-and-delete behaviour with honest "Delete" wording.
 *
 * WHAT THIS TEST ASSERTS (retargeted to the new behaviour):
 *   1. preserves_index: after the DEFAULT path
 *        cbm_install_handle_existing_indexes(home, reset=false, dry_run=false)
 *      the index DB MUST still exist on disk.
 *        - RED before the fix: the helper did not exist / install deleted the
 *          DB, so the file was gone and the ASSERT_TRUE fired.
 *        - GREEN after the fix: the default path never unlinks, the file
 *          remains, the assertion holds.
 *   2. reset_deletes: the explicit opt-in path
 *        cbm_install_handle_existing_indexes(home, reset=true, dry_run=false)
 *      MUST still delete the DB (proving the destroy primitive is reachable
 *      only behind the explicit flag). The prompt auto-answers "yes" via
 *      CBM_ASSUME_YES so the test is non-interactive.
 *
 * The helper is intentionally NOT declared in cli.h (internal install helper).
 * cli.c is linked into the bug-repro runner ($(CLI_SRCS) is in $(PROD_SRCS)),
 * so we link against it directly with an extern forward declaration below.
 */

#include <foundation/compat.h>
#include "test_framework.h"

#include <cli/cli.h>
#include <store/store.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

/* ── Forward declaration of the internal install helper (the #607 fix) ──
 *
 * Defined non-static in src/cli/cli.c. Not in cli.h (it is an install-time
 * internal), so we declare it here to link against. Default reset=false must
 * PRESERVE; reset=true must DELETE. Returns 1 to proceed, 0 if the user
 * declined the reset prompt.
 */
int cbm_install_handle_existing_indexes(const char *home, bool reset, bool dry_run);

/* Test seam (defined non-static in src/cli/cli.c, not in cli.h): force the
 * auto-answer state so the opt-in reset path's prompt_yn() is confirmed
 * deterministically under a non-interactive (non-TTY) CI stdin.
 *   1 => "yes" (auto), -1 => "no" (auto), 0 => interactive prompt. */
void cbm_set_auto_answer_for_test(int value);

/* ── Helper: check whether a file exists ─────────────────────────── */

static int file_exists_607(const char *path) {
    struct stat st;
    return (stat(path, &st) == 0) ? 1 : 0;
}

#define REPRO607_PROJECT "cbm-repro607-test"

/* Create a real index DB at <tmp_cache>/<REPRO607_PROJECT>.db with one
 * project row, mirroring the state of a user who ran index_repository once.
 * Writes the resulting path into db_path. Returns 1 on success, 0 on setup
 * failure. */
static int repro607_make_index(const char *tmp_cache, char *db_path, size_t db_path_sz) {
    snprintf(db_path, db_path_sz, "%s/%s.db", tmp_cache, REPRO607_PROJECT);

    cbm_store_t *setup_store = cbm_store_open_path(db_path);
    if (!setup_store) {
        return 0;
    }
    int upsert_rc =
        cbm_store_upsert_project(setup_store, REPRO607_PROJECT, "/home/user/my-project");
    cbm_store_close(setup_store);
    return (upsert_rc == CBM_STORE_OK) ? 1 : 0;
}

/* Best-effort cleanup of the temp cache dir + DB sidecar files. */
static void repro607_cleanup(const char *tmp_cache, const char *db_path) {
    unlink(db_path);
    char wal[730], shm[730];
    snprintf(wal, sizeof(wal), "%s-wal", db_path);
    snprintf(shm, sizeof(shm), "%s-shm", db_path);
    unlink(wal);
    unlink(shm);
    rmdir(tmp_cache);
}

/* ── Test 1: default (reset=false) PRESERVES the index ────────────────
 *
 * This is the primary #607 guard. The user is (re)installing; the default
 * MUST keep their indexed graph intact.
 * ─────────────────────────────────────────────────────────────────── */
TEST(repro_issue607_reinstall_preserves_index) {
    /* Redirect CBM_CACHE_DIR to a fresh temp dir so the real user cache is
     * never touched and count_db_indexes()/cbm_list_indexes() see only the
     * DB we create here. */
    char tmp_cache[512];
    snprintf(tmp_cache, sizeof(tmp_cache), "/tmp/cbm_repro607_XXXXXX");
    if (!cbm_mkdtemp(tmp_cache)) {
        ASSERT_NOT_NULL(NULL); /* marks setup failure clearly */
    }

#if defined(_WIN32)
    char ev[600];
    snprintf(ev, sizeof(ev), "CBM_CACHE_DIR=%s", tmp_cache);
    _putenv(ev);
#else
    setenv("CBM_CACHE_DIR", tmp_cache, 1 /* overwrite */);
#endif

    char db_path[700];
    ASSERT_TRUE(repro607_make_index(tmp_cache, db_path, sizeof(db_path)));

    /* Precondition: the DB must exist before we exercise the install path. */
    ASSERT_TRUE(file_exists_607(db_path));

    /* ── The fix under test: DEFAULT install index handling (reset=false) ──
     *
     * Before the fix this path deleted every .db while printing "must be
     * rebuilt". The fix preserves them: the helper lists the indexes and
     * returns 1 (proceed) WITHOUT unlinking anything.
     *
     * dry_run=false so this is the real (non-dry) path — the one that used to
     * call cbm_remove_indexes(). The fix must NOT delete here regardless.
     */
    int proceed =
        cbm_install_handle_existing_indexes(tmp_cache /* fake home */, false /* reset */,
                                            false /* dry_run */);

    /* The default path always proceeds (no prompt, no abort). */
    int proceeded = (proceed == 1);

    /* PRIMARY ASSERTION: the index DB MUST still exist after the default
     * install path. RED on the old code (deleted); GREEN after the fix. */
    int db_exists = file_exists_607(db_path);

    repro607_cleanup(tmp_cache, db_path);

#if defined(_WIN32)
    _putenv("CBM_CACHE_DIR=");
#else
    unsetenv("CBM_CACHE_DIR");
#endif

    ASSERT_TRUE(proceeded);
    ASSERT_TRUE(db_exists);

    PASS();
}

/* ── Test 2: opt-in (reset=true) STILL deletes the index ──────────────
 *
 * Proves the destroy primitive remains reachable ONLY behind the explicit
 * --reset-indexes flag. Auto-answers the delete prompt via CBM_ASSUME_YES so
 * the test stays non-interactive.
 * ─────────────────────────────────────────────────────────────────── */
TEST(repro_issue607_reset_indexes_deletes) {
    char tmp_cache[512];
    snprintf(tmp_cache, sizeof(tmp_cache), "/tmp/cbm_repro607r_XXXXXX");
    if (!cbm_mkdtemp(tmp_cache)) {
        ASSERT_NOT_NULL(NULL);
    }

#if defined(_WIN32)
    char ev[600];
    snprintf(ev, sizeof(ev), "CBM_CACHE_DIR=%s", tmp_cache);
    _putenv(ev);
#else
    setenv("CBM_CACHE_DIR", tmp_cache, 1 /* overwrite */);
#endif

    char db_path[700];
    ASSERT_TRUE(repro607_make_index(tmp_cache, db_path, sizeof(db_path)));
    ASSERT_TRUE(file_exists_607(db_path)); /* precondition: DB exists */

    /* Auto-confirm the destructive prompt so the test is non-interactive
     * under a non-TTY CI stdin (prompt_yn would otherwise default to "no"). */
    cbm_set_auto_answer_for_test(1 /* AUTO_YES */);

    /* Opt-in destructive path: reset=true must delete the index. */
    int proceed =
        cbm_install_handle_existing_indexes(tmp_cache /* fake home */, true /* reset */,
                                            false /* dry_run */);
    int proceeded = (proceed == 1);

    /* After the opt-in reset, the DB must be GONE. */
    int db_exists = file_exists_607(db_path);

    /* Restore interactive default so this state never leaks into other tests. */
    cbm_set_auto_answer_for_test(0 /* prompt */);

    repro607_cleanup(tmp_cache, db_path);

#if defined(_WIN32)
    _putenv("CBM_CACHE_DIR=");
#else
    unsetenv("CBM_CACHE_DIR");
#endif

    ASSERT_TRUE(proceeded);       /* user confirmed → proceed */
    ASSERT_FALSE(db_exists);      /* opt-in path deleted the index */

    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────────── */
SUITE(repro_issue607) {
    RUN_TEST(repro_issue607_reinstall_preserves_index);
    RUN_TEST(repro_issue607_reset_indexes_deletes);
}
