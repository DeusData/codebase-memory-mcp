/*
 * repro_issue434.c - Reproduce-first case for OPEN bug #434.
 *
 * Issue: #434 - "cursor | vscode : persistence=true is silently ignored on
 * first artifact creation"
 *
 * Root cause:
 *   In src/pipeline/pipeline_incremental.c, the static function
 *   dump_and_persist() (around line 668) auto-exports the artifact only when
 *   one ALREADY exists on disk:
 *
 *     if (repo_path && cbm_artifact_exists(repo_path)) {
 *         cbm_artifact_export(db_path, repo_path, project, CBM_ARTIFACT_FAST);
 *     }
 *
 *   It never consults p->persistence. So when index_repository is called with
 *   persistence=true for the FIRST time (no prior artifact), the incremental
 *   path skips the export entirely. The full-pipeline path in pipeline.c
 *   correctly gates on p->persistence (line 933: if (p->persistence) {...}),
 *   but cbm_pipeline_run_incremental() calls the local dump_and_persist()
 *   which only checks cbm_artifact_exists(), not the pipeline flag.
 *
 *   The MCP handler in mcp.c (line 2794) further exposes the symptom:
 *     if (persistence && has_artifact) { ... artifact_hint ... }
 *   This condition can never be true on a first run because has_artifact is
 *   checked AFTER the incremental path ran and produced no artifact.
 *
 * Expected (correct) behaviour:
 *   Calling index_repository with persistence=true on a repo that has no
 *   prior artifact MUST create .codebase-memory/graph.db.zst after the run.
 *   cbm_artifact_exists(repo_path) MUST return true after the first
 *   persistence=true index, not only after a second run.
 *
 * Actual (buggy) behaviour:
 *   After the first persistence=true call on a fresh repo, no artifact is
 *   written. cbm_artifact_exists() returns false. Only a SECOND call (when
 *   the artifact now exists from a prior run) writes the file.
 *
 * Why RED on current code:
 *   We call index_repository once with persistence=true on a fresh fixture
 *   repo (no prior artifact). We then assert cbm_artifact_exists() returns
 *   true. On buggy code dump_and_persist() skips the export because
 *   cbm_artifact_exists() was false at the time of the check, so the
 *   assertion fires RED.
 *
 * Fix location (not implemented here):
 *   src/pipeline/pipeline_incremental.c, dump_and_persist():
 *   The function must accept (or read) the pipeline persistence flag and
 *   call cbm_artifact_export() when persistence=true, regardless of whether
 *   an artifact already exists. The existing auto-update branch should be
 *   merged with a new persistence-flag branch so that:
 *     if (repo_path && (persistence || cbm_artifact_exists(repo_path))) {
 *         cbm_artifact_export(...);
 *     }
 *   The pipeline struct's persistence field must be threaded through to
 *   dump_and_persist() (currently it is not passed at all).
 */

#include "test_framework.h"
#include "repro_harness.h"
#include <pipeline/artifact.h>
#include <foundation/compat.h>
#include <foundation/compat_fs.h>

#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ── Test ────────────────────────────────────────────────────────────────── */

TEST(repro_issue434_persistence_honored_on_first_create) {
    /* Set up a minimal fixture repo with one C file so the pipeline has
     * something to index.  We go through the MCP index_repository tool
     * (the production path) so the persistence flag travels through
     * cbm_mcp_get_bool_arg -> cbm_pipeline_set_persistence -> the pipeline. */
    RProj lp;
    memset(&lp, 0, sizeof(lp));

    /* Create a fresh temp directory for the fixture repo */
    snprintf(lp.tmpdir, sizeof(lp.tmpdir), "/tmp/cbm_repro434_XXXXXX");
    if (!cbm_mkdtemp(lp.tmpdir))
        FAIL("cbm_mkdtemp failed");

    /* Write a minimal C source file so discovery finds something */
    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.c", lp.tmpdir);
    FILE *fp = fopen(src_path, "w");
    if (!fp) {
        th_rmtree(lp.tmpdir);
        FAIL("fopen main.c failed");
    }
    fputs("int main(void) { return 0; }\n", fp);
    fclose(fp);

    /* Verify: NO artifact exists before the first run */
    ASSERT_FALSE(cbm_artifact_exists(lp.tmpdir));

    /* Build the MCP JSON args with persistence=true */
    char args[700];
    snprintf(args, sizeof(args),
             "{\"repo_path\":\"%s\",\"persistence\":true}", lp.tmpdir);

    /* Create an MCP server and run index_repository with persistence=true.
     * This is the exact production code path that Cursor/VSCode calls. */
    lp.srv = cbm_mcp_server_new(NULL);
    if (!lp.srv) {
        th_rmtree(lp.tmpdir);
        FAIL("cbm_mcp_server_new failed");
    }

    char *resp = cbm_mcp_handle_tool(lp.srv, "index_repository", args);
    if (resp)
        free(resp);

    /*
     * RED assertion: after a FIRST index_repository call with persistence=true
     * the artifact MUST exist in .codebase-memory/graph.db.zst.
     *
     * On buggy code (pipeline_incremental.c dump_and_persist only checks
     * cbm_artifact_exists() not p->persistence) the artifact is NOT written
     * on the first run, so cbm_artifact_exists() returns false here and this
     * ASSERT fires RED — that is the reproduce-first deliverable.
     *
     * On fixed code the assertion will be GREEN (persistence=true creates
     * the artifact even when no prior artifact existed).
     */
    bool artifact_created = cbm_artifact_exists(lp.tmpdir);

    /* Derive project name before rmtree (still valid as a string after rmtree,
     * but cleaner to resolve while the directory exists) */
    char *proj = cbm_project_name_from_path(lp.tmpdir);

    /* Cleanup before asserting so temp files are always removed */
    if (lp.srv) {
        cbm_mcp_server_free(lp.srv);
        lp.srv = NULL;
    }

    /* Remove the artifact dir and the fixture repo */
    char art_dir[600];
    snprintf(art_dir, sizeof(art_dir), "%s/.codebase-memory", lp.tmpdir);
    th_rmtree(art_dir);
    th_rmtree(lp.tmpdir);

    /* Clean up the cache DB the pipeline wrote */
    if (proj) {
        const char *home = getenv("HOME");
        if (!home) home = "/tmp";
        char dbpath[600];
        snprintf(dbpath, sizeof(dbpath), "%s/.cache/codebase-memory-mcp/%s.db",
                 home, proj);
        unlink(dbpath);
        free(proj);
    }

    ASSERT_TRUE(artifact_created);

    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────────────── */

SUITE(repro_issue434) {
    RUN_TEST(repro_issue434_persistence_honored_on_first_create);
}
