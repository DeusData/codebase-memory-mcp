/*
 * repro_issue557.c -- Reproduce-first case for OPEN bug #557.
 *
 * Issue: #557 -- "cbm v0.8.1 silently deletes project DBs on 'corrupt'
 *                detection -- data loss with no recovery"
 *
 * DESTROYING CODE PATH:
 *   src/mcp/mcp.c  resolve_store()  lines 796-810
 *
 *   The sequence is:
 *     1. resolve_store() opens the project DB with cbm_store_open_path_query().
 *     2. It calls cbm_store_check_integrity() (src/store/store.c:664).
 *        That function returns false when the projects table contains a row
 *        whose root_path does not start with '/', 'A'-'Z', or 'a'-'z' (the
 *        numeric-string corruption pattern -- e.g. "826" -- observed in the
 *        binary and confirmed in the issue report).
 *     3. On false, resolve_store() calls cbm_unlink(path) at mcp.c:803,
 *        then cbm_unlink(wal_path) and cbm_unlink(shm_path) -- with NO rename,
 *        NO backup, NO recovery path.  The user's indexed project is gone.
 *
 * ROOT CAUSE:
 *   "Delete on first suspicion" design in resolve_store().  The unlink is
 *   unconditional and irreversible.  Any false-positive integrity signal
 *   (WAL/SHM leftover after SIGKILL, schema-version drift between standard
 *   and UI binary variants, or a root_path value that happens not to match
 *   the narrow whitelist) causes permanent data loss.
 *
 * EXPECTED (correct) behaviour:
 *   After cbm_store_check_integrity() returns false and resolve_store()
 *   executes its cleanup path, EITHER:
 *     (a) the original DB file must still exist at db_path (zero deletion), OR
 *     (b) a backup file must exist at a nearby path (e.g. "<db_path>.corrupt"
 *         or "<db_path>.bak") so the user can recover the data.
 *   The original DB must NOT be silently destroyed with no recovery path.
 *
 * ACTUAL (buggy) behaviour on v0.8.1:
 *   cbm_unlink(path) at mcp.c:803 destroys the DB file.  After resolve_store()
 *   returns, access(db_path, F_OK) returns -1 (ENOENT) and no backup file
 *   exists -- total data loss.
 *
 * WHY RED on current code:
 *   The final ASSERT_TRUE checks that EITHER db_still_exists OR backup_exists.
 *   On buggy code cbm_unlink() runs with no rename, so both conditions are
 *   false and ASSERT_TRUE fires -- RED.
 *
 * TRIGGER:
 *   We construct the scenario directly at the store API level (no full index
 *   needed -- the integrity check runs before any graph data is consulted):
 *
 *   1. Set CBM_CACHE_DIR to a temp directory so the DB lands in a controlled
 *      location and does not pollute the real cache.
 *   2. Create the DB via cbm_store_open_path() (creates schema + tables).
 *   3. Insert one projects row with root_path = "826" -- the exact numeric
 *      string from the binary evidence in the issue report.  This passes the
 *      "> 5 rows" check (only 1 row) but trips the bad_root_path check in
 *      cbm_store_check_integrity() because '8' is not '/', 'A'-'Z', or 'a'-'z'.
 *   4. Close the store, verify the DB file exists (precondition).
 *   5. Call cbm_mcp_handle_tool(srv, "search_graph", ...) with the project
 *      name.  search_graph resolves the project store via resolve_store(),
 *      which opens the DB, runs the integrity check, detects bad_root_path,
 *      and executes the destroying cbm_unlink() at mcp.c:803.
 *   6. Assert survival: DB file still exists OR a backup exists.
 *
 * NOTE on determinism:
 *   The "826" root_path value is a deterministically planted value -- not
 *   dependent on kill timing or WAL state.  cbm_store_check_integrity() is
 *   a pure SQL query; its result for root_path="826" is guaranteed to be
 *   false on any build.  The trigger is 100% reproducible.
 *
 * FIX LOCATION (not implemented here):
 *   src/mcp/mcp.c  resolve_store()  around line 803:
 *   Replace cbm_unlink(path) with a rename to a timestamped .corrupt path,
 *   then log a prominent error so the user knows where the preserved file is.
 */

#include <foundation/compat.h>
#include "test_framework.h"

#include <store/store.h>
#include <mcp/mcp.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

/* Project name used throughout: must pass cbm_validate_project_name().
 * Kept short and slug-safe so it is valid on every platform. */
#define REPRO557_PROJECT "cbm-repro557-test"

/* ── Helper: check whether a file exists ────────────────────────────── */

static int file_exists(const char *path) {
    struct stat st;
    return (stat(path, &st) == 0) ? 1 : 0;
}

/* ── Test ─────────────────────────────────────────────────────────────
 *
 * repro_issue557_corrupt_db_not_silently_deleted
 *
 * Precondition (must be GREEN to prove the setup is correct):
 *   The DB file exists at db_path after we create and populate it.
 *   If this fires RED, the temp dir or store creation failed -- not #557.
 *
 * The failing assertion (RED on buggy code):
 *   After resolve_store() detects bad_root_path and runs its cleanup path,
 *   EITHER the DB file still exists OR a backup file exists.
 *   On buggy code: neither exists -- ASSERT_TRUE fires.
 * ─────────────────────────────────────────────────────────────────── */

TEST(repro_issue557_corrupt_db_not_silently_deleted) {
    /* ── Step 1: redirect CBM_CACHE_DIR to a temp dir ─────────────────
     *
     * cbm_resolve_cache_dir() checks the CBM_CACHE_DIR env var first.
     * Pointing it at a fresh temp dir ensures:
     *   - the test DB is isolated from the user's real cache
     *   - we know the exact db_path before the MCP call
     *
     * The static buffer in cbm_resolve_cache_dir() is updated on the
     * next call because it re-reads CBM_CACHE_DIR each time.  We must
     * also call cbm_mkdir on the directory before opening the store.
     */
    char tmp_cache[512];
    snprintf(tmp_cache, sizeof(tmp_cache), "/tmp/cbm_repro557_XXXXXX");
    if (!cbm_mkdtemp(tmp_cache)) {
        /* mkdtemp failed -- cannot run the test */
        ASSERT_NOT_NULL(NULL); /* marks setup failure clearly */
    }

    /* Set the env var so all subsequent cbm_resolve_cache_dir() calls
     * return tmp_cache.  setenv is POSIX; Windows uses _putenv_s. */
#if defined(_WIN32)
    char ev[600];
    snprintf(ev, sizeof(ev), "CBM_CACHE_DIR=%s", tmp_cache);
    _putenv(ev);
#else
    setenv("CBM_CACHE_DIR", tmp_cache, 1 /* overwrite */);
#endif

    /* ── Step 2: build the DB path we will inspect ────────────────────
     *
     * project_db_path() in mcp.c computes:  <cache_dir>/<project>.db
     * Mirror the same formula here so db_path matches exactly.
     */
    char db_path[700];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", tmp_cache, REPRO557_PROJECT);

    /* ── Step 3: create the DB via cbm_store_open_path() ──────────────
     *
     * cbm_store_open_path() calls store_open_internal() with
     * SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, then runs init_schema()
     * to create all tables including `projects`.  This gives us a
     * fully-structured DB at db_path.
     */
    cbm_store_t *setup_store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(setup_store); /* precondition: store creation must work */

    /* ── Step 4: insert a project row with a bad root_path ────────────
     *
     * root_path = "826" is the exact numeric string from the binary
     * evidence in the issue report and confirmed by the integrity check
     * SQL in cbm_store_check_integrity():
     *
     *   SELECT root_path FROM projects
     *   WHERE root_path != ''
     *   AND NOT (substr(root_path,1,1) = '/'
     *     OR substr(...) BETWEEN 'A' AND 'Z'
     *     OR substr(...) BETWEEN 'a' AND 'z')
     *   LIMIT 1;
     *
     * '8' does not satisfy any of the three path-start conditions, so
     * the query returns the row and cbm_store_check_integrity() returns
     * false -- which is the exact trigger for the destroying path.
     *
     * cbm_store_upsert_project() is the store's own public API for
     * writing project rows (used by the pipeline on every full index).
     */
    int rc = cbm_store_upsert_project(setup_store, REPRO557_PROJECT, "826");
    ASSERT_EQ(rc, CBM_STORE_OK); /* precondition: row must be written */

    cbm_store_close(setup_store);
    setup_store = NULL;

    /* ── Step 5: verify the DB exists before triggering the MCP path ──
     *
     * This is the precondition that confirms setup succeeded.
     * If this fires RED, something in Steps 2-4 broke -- not #557.
     */
    ASSERT_TRUE(file_exists(db_path)); /* precondition: DB must exist now */

    /* ── Step 6: drive resolve_store() via cbm_mcp_handle_tool ────────
     *
     * search_graph is the lightest query tool that reaches resolve_store().
     * The tool handler calls resolve_store(srv, project) which:
     *   1. Calls cbm_store_open_path_query(path) -- opens read-write/no-create.
     *      The DB was created in step 3 so SQLITE_OPEN_READWRITE succeeds.
     *   2. Calls cbm_store_check_integrity() -- returns false (root_path="826").
     *   3. Closes the store and calls cbm_unlink(path) at mcp.c:803.
     *      Then cbm_unlink(wal_path) and cbm_unlink(shm_path).
     *   4. Returns NULL (resolve_store() returns NULL on corrupt detection).
     *
     * We do not assert anything about the search_graph response -- the
     * response is irrelevant (it will be an error about the project not
     * being found).  What matters is the side-effect on db_path.
     */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv); /* precondition: server must initialise */

    char args[512];
    snprintf(args, sizeof(args),
             "{\"project\":\"%s\","
             "\"query\":\"Function\","
             "\"limit\":1}",
             REPRO557_PROJECT);

    char *resp = cbm_mcp_handle_tool(srv, "search_graph", args);
    /* Response may be NULL or an error string -- we do not assert on it.
     * The side-effect (unlink) is what we are testing. */
    if (resp) {
        free(resp);
    }
    cbm_mcp_server_free(srv);

    /* ── Step 7: PRIMARY ASSERTION -- the DB must survive ─────────────
     *
     * Correct behaviour: the DB is quarantined (renamed to a backup path)
     * rather than silently destroyed.  We accept either:
     *   (a) the original DB still exists at db_path (zero deletion), or
     *   (b) a backup file exists at a conventional backup path.
     *
     * Two conventional backup suffixes from the suggested fix in #557:
     *   "<db_path>.corrupt"  -- timestamped or plain rename
     *   "<db_path>.bak"      -- simpler alternative
     *
     * WHY RED on buggy code:
     *   cbm_unlink(path) at mcp.c:803 removes the file.
     *   No rename to .corrupt or .bak is performed.
     *   db_still_exists == 0 and backup_exists == 0.
     *   ASSERT_TRUE(0) fires -- RED.
     */
    int db_still_exists = file_exists(db_path);

    char backup_corrupt[720], backup_bak[720];
    snprintf(backup_corrupt, sizeof(backup_corrupt), "%s.corrupt", db_path);
    snprintf(backup_bak,     sizeof(backup_bak),     "%s.bak",     db_path);
    int backup_exists = file_exists(backup_corrupt) || file_exists(backup_bak);

    /* Clean up temp dir (best effort -- before the assertion so the dir
     * is removed even when the assertion fails and longjmp unwinds). */
    unlink(db_path);
    unlink(backup_corrupt);
    unlink(backup_bak);
    char wal[730], shm[730];
    snprintf(wal, sizeof(wal), "%s-wal", db_path);
    snprintf(shm, sizeof(shm), "%s-shm", db_path);
    unlink(wal);
    unlink(shm);
    rmdir(tmp_cache);

#if defined(_WIN32)
    _putenv("CBM_CACHE_DIR=");
#else
    unsetenv("CBM_CACHE_DIR");
#endif

    /*
     * THE KEY ASSERTION -- must be RED on unpatched code:
     *
     *   db_still_exists  -- 1 if the DB was preserved in-place (zero-delete fix)
     *   backup_exists    -- 1 if a .corrupt or .bak rename was made (quarantine fix)
     *
     * On buggy code: both are 0 because cbm_unlink() ran with no backup.
     * On fixed code: at least one is 1.
     */
    ASSERT_TRUE(db_still_exists || backup_exists);

    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────────── */
SUITE(repro_issue557) {
    RUN_TEST(repro_issue557_corrupt_db_not_silently_deleted);
}
