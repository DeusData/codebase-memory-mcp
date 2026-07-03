/*
 * test_index_resilience.c — Stage 2 / Track B guard.
 *
 * A file that fails during indexing (here: exceeds the env-configurable size
 * cap) must be SKIPPED-AND-REPORTED, never silently dropped, and it must NOT
 * take the rest of the repo down with it. This is the genuine guard for the
 * error-surfacing wiring (has_error / read / oversized → cbm_file_error_t →
 * MCP `skipped[]` + `skipped_count` + per-run logfile).
 *
 * These indexes run through the full production MCP `index_repository` flow.
 * With only a handful of files the pipeline takes the SEQUENTIAL path
 * (pass_definitions.c), so this exercises the sequential recording branch on
 * every platform regardless of core count.
 */
#include "test_framework.h"
#include "repro_harness.h" /* RProj, rh_to_fwd_slashes, rh_count_label, rh_cleanup */
#include <yyjson/yyjson.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Local helpers ──────────────────────────────────────────────── */

static void ri_write_text(const char *dir, const char *name, const char *content) {
    char path[700];
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    FILE *f = fopen(path, "wb");
    if (f) {
        fputs(content, f);
        fclose(f);
    }
}

/* Write a comment-only python file padded well past `bytes`. The file is never
 * parsed (the size cap rejects it before extraction), so its content only needs
 * to make it a discoverable .py source that exceeds the cap. */
static void ri_write_big(const char *dir, const char *name, size_t bytes) {
    char path[700];
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    FILE *f = fopen(path, "wb");
    if (!f) {
        return;
    }
    static const char line[] = "# oversized filler line padding this file past the size cap\n";
    size_t linelen = sizeof(line) - 1;
    size_t written = 0;
    while (written < bytes) {
        fwrite(line, 1, linelen, f);
        written += linelen;
    }
    fclose(f);
}

/* Slurp a whole file into a heap buffer (NUL-terminated). NULL on error. */
static char *ri_slurp(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long n = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (n < 0) {
        (void)fclose(f);
        return NULL;
    }
    char *buf = (char *)malloc((size_t)n + 1);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t rd = fread(buf, 1, (size_t)n, f);
    (void)fclose(f);
    buf[rd] = '\0';
    return buf;
}

/* Index the files already written under lp->tmpdir through the production MCP
 * flow, capturing the raw response. Returns the opened graph store (NULL on
 * failure). Mirrors repro_harness.h's rh_open_indexed but keeps the response so
 * we can assert on skipped_count / skipped[] / logfile. */
static cbm_store_t *ri_index_capture(RProj *lp, char **out_resp) {
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
    if (out_resp) {
        *out_resp = resp;
    } else if (resp) {
        free(resp);
    }
    return cbm_store_open_path(lp->dbpath);
}

/* ── Tests ──────────────────────────────────────────────────────── */

/* INV(oversized-reported): with the size cap set LOW, indexing a repo that
 * contains one > cap file plus two good files must:
 *   - complete with status "indexed" (a skip is a handled outcome, not failure),
 *   - report skipped_count >= 1 with the big file in skipped[] at phase "oversized",
 *   - write a per-run logfile (path echoed in the response) that lists the file,
 *   - and STILL index the two good files (their Function nodes are present).
 *
 * Guard property: on the UNWIRED code the big file is silently dropped — there
 * is no skipped_count / skipped[] / logfile — so every assertion below fails.
 */
TEST(index_oversized_file_reported) {
    RProj lp;
    memset(&lp, 0, sizeof(lp));
    snprintf(lp.tmpdir, sizeof(lp.tmpdir), "/tmp/cbm_resil_XXXXXX");
    if (!cbm_mkdtemp(lp.tmpdir)) {
        FAIL("mkdtemp failed");
    }
    rh_to_fwd_slashes(lp.tmpdir);

    ri_write_text(lp.tmpdir, "good.py", "def alpha():\n    return 1\n");
    ri_write_text(lp.tmpdir, "good.go", "package main\n\nfunc beta() int { return 2 }\n");
    ri_write_big(lp.tmpdir, "big.py", (size_t)2 * 1024 * 1024); /* ~2 MiB > 1 MiB cap */

    char logpath[700];
    snprintf(logpath, sizeof(logpath), "%s/skip.log", lp.tmpdir);
    cbm_setenv("CBM_MAX_FILE_BYTES", "1048576", 1); /* 1 MiB cap */
    cbm_setenv("CBM_INDEX_LOG", logpath, 1);        /* deterministic logfile path */

    char *resp = NULL;
    cbm_store_t *store = ri_index_capture(&lp, &resp);

    /* Unset env IMMEDIATELY (before any assert can bail) so a low cap never
     * leaks into other tests in this process — cbm_max_file_bytes() reads env
     * on every file. */
    cbm_unsetenv("CBM_MAX_FILE_BYTES");
    cbm_unsetenv("CBM_INDEX_LOG");

    if (!resp) {
        FAIL("no MCP response");
    }
    if (!store) {
        free(resp);
        FAIL("store did not open");
    }

    yyjson_doc *d = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(d);
    yyjson_val *sc = yyjson_obj_get(yyjson_doc_get_root(d), "structuredContent");
    ASSERT_NOT_NULL(sc);

    /* Status stays "indexed" — the skip is expected + handled. */
    const char *status = yyjson_get_str(yyjson_obj_get(sc, "status"));
    ASSERT_NOT_NULL(status);
    ASSERT_STR_EQ("indexed", status);

    /* At least one skip, surfaced at the top level. */
    int skipped_count = yyjson_get_int(yyjson_obj_get(sc, "skipped_count"));
    ASSERT_GTE(skipped_count, 1);

    /* The big file is listed, at phase "oversized". */
    yyjson_val *skipped = yyjson_obj_get(sc, "skipped");
    ASSERT_NOT_NULL(skipped);
    yyjson_val *files = yyjson_obj_get(skipped, "files");
    ASSERT_NOT_NULL(files);
    int found_big = 0;
    size_t idx = 0;
    size_t fmax = 0;
    yyjson_val *fe = NULL;
    yyjson_arr_foreach(files, idx, fmax, fe) {
        const char *fp = yyjson_get_str(yyjson_obj_get(fe, "path"));
        const char *phase = yyjson_get_str(yyjson_obj_get(fe, "phase"));
        if (fp && strstr(fp, "big.py")) {
            found_big = 1;
            ASSERT_NOT_NULL(phase);
            ASSERT_STR_EQ("oversized", phase);
        }
    }
    ASSERT_TRUE(found_big);

    /* A logfile was written, its path echoed, and it lists the skipped file. */
    const char *logfile = yyjson_get_str(yyjson_obj_get(sc, "logfile"));
    ASSERT_NOT_NULL(logfile);
    ASSERT_STR_EQ(logpath, logfile);
    char *logtext = ri_slurp(logfile);
    ASSERT_NOT_NULL(logtext);
    ASSERT_NOT_NULL(strstr(logtext, "big.py"));
    ASSERT_NOT_NULL(strstr(logtext, "oversized"));
    free(logtext);

    /* The two good files ARE indexed — the skip did not take them down. */
    int funcs = rh_count_label(store, lp.project, "Function");
    ASSERT_GTE(funcs, 2);

    yyjson_doc_free(d);
    free(resp);
    rh_cleanup(&lp, store);
    PASS();
}

/* INV(clean-run): a run with no failures reports skipped_count == 0 and emits
 * NO "skipped" object and NO "logfile" (a logfile is written only on skips). */
TEST(index_clean_run_no_logfile) {
    RProj lp;
    memset(&lp, 0, sizeof(lp));
    snprintf(lp.tmpdir, sizeof(lp.tmpdir), "/tmp/cbm_resil_XXXXXX");
    if (!cbm_mkdtemp(lp.tmpdir)) {
        FAIL("mkdtemp failed");
    }
    rh_to_fwd_slashes(lp.tmpdir);

    ri_write_text(lp.tmpdir, "good.py", "def alpha():\n    return 1\n");
    ri_write_text(lp.tmpdir, "good.go", "package main\n\nfunc beta() int { return 2 }\n");

    /* Defensive: make sure no stray low cap / log override leaks in. */
    cbm_unsetenv("CBM_MAX_FILE_BYTES");
    cbm_unsetenv("CBM_INDEX_LOG");

    char *resp = NULL;
    cbm_store_t *store = ri_index_capture(&lp, &resp);
    if (!resp) {
        FAIL("no MCP response");
    }
    if (!store) {
        free(resp);
        FAIL("store did not open");
    }

    yyjson_doc *d = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(d);
    yyjson_val *sc = yyjson_obj_get(yyjson_doc_get_root(d), "structuredContent");
    ASSERT_NOT_NULL(sc);

    int skipped_count = yyjson_get_int(yyjson_obj_get(sc, "skipped_count"));
    ASSERT_EQ(skipped_count, 0);
    ASSERT_NULL(yyjson_obj_get(sc, "skipped"));
    ASSERT_NULL(yyjson_obj_get(sc, "logfile"));

    int funcs = rh_count_label(store, lp.project, "Function");
    ASSERT_GTE(funcs, 2);

    yyjson_doc_free(d);
    free(resp);
    rh_cleanup(&lp, store);
    PASS();
}

SUITE(index_resilience) {
    RUN_TEST(index_oversized_file_reported);
    RUN_TEST(index_clean_run_no_logfile);
}
