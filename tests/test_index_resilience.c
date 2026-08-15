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
#include <time.h>
#include <limits.h>

/* glibc's _FORTIFY_SOURCE realpath() aborts unless the output buffer is at
 * least PATH_MAX bytes, independent of the actual path length. */
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/* Sleep before rewriting a fixture so its mtime_ns strictly increases and the
 * incremental change classifier reliably sees the edit (10 ms). */
#define INCR_FIX_SLEEP_NS 10000000L

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

    /* Clean parses → no parse-coverage flags either (#963). */
    int pp_count = yyjson_get_int(yyjson_obj_get(sc, "parse_partial_count"));
    ASSERT_EQ(pp_count, 0);
    ASSERT_NULL(yyjson_obj_get(sc, "parse_partial"));

    int funcs = rh_count_label(store, lp.project, "Function");
    ASSERT_GTE(funcs, 2);

    yyjson_doc_free(d);
    free(resp);
    rh_cleanup(&lp, store);
    PASS();
}

/* INV(parse-partial-reported, #963): a file whose parse tree contains
 * ERROR/MISSING regions (here: the preprocessor-blind #ifdef-split-brace C
 * pattern) is INDEXED — not skipped — and the best-effort coverage signal
 * surfaces on every layer:
 *   - skipped_count == 0 (a partial parse is NOT a skip),
 *   - parse_partial_count >= 1 with the file + its line ranges + the
 *     best-effort note in parse_partial{},
 *   - the per-run logfile lists it under phase "parse_partial",
 *   - the File node carries {"parse_incomplete":true,"error_ranges":...},
 *   - clean neighbors still extract.
 *
 * Guard property: on the unwired code there is no parse_partial_count /
 * parse_partial[] / File-node marker — the file silently looks fully indexed.
 */
TEST(index_parse_partial_reported) {
    RProj lp;
    memset(&lp, 0, sizeof(lp));
    snprintf(lp.tmpdir, sizeof(lp.tmpdir), "/tmp/cbm_resil_XXXXXX");
    if (!cbm_mkdtemp(lp.tmpdir)) {
        FAIL("mkdtemp failed");
    }
    rh_to_fwd_slashes(lp.tmpdir);

    /* Both #ifdef branches open `guarded(...) {` sharing ONE close brace —
     * brace-unbalanced for a preprocessor-blind parse → ERROR region. */
    ri_write_text(lp.tmpdir, "split.c",
                  "void ok_before(void) { }\n"
                  "#ifdef FEATURE_A\n"
                  "static int guarded(int x) {\n"
                  "#else\n"
                  "static int guarded_alt(int x) {\n"
                  "#endif\n"
                  "    return x + 1;\n"
                  "}\n");
    ri_write_text(lp.tmpdir, "good.py", "def alpha():\n    return 1\n");

    char logpath[700];
    snprintf(logpath, sizeof(logpath), "%s/coverage.log", lp.tmpdir);
    cbm_setenv("CBM_INDEX_LOG", logpath, 1);

    char *resp = NULL;
    cbm_store_t *store = ri_index_capture(&lp, &resp);
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

    /* Indexed, and NOT counted as a skip. */
    const char *status = yyjson_get_str(yyjson_obj_get(sc, "status"));
    ASSERT_NOT_NULL(status);
    ASSERT_STR_EQ("indexed", status);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(sc, "skipped_count")), 0);

    /* The coverage signal is surfaced with ranges + the best-effort note. */
    ASSERT_GTE(yyjson_get_int(yyjson_obj_get(sc, "parse_partial_count")), 1);
    yyjson_val *pp = yyjson_obj_get(sc, "parse_partial");
    ASSERT_NOT_NULL(pp);
    yyjson_val *files = yyjson_obj_get(pp, "files");
    ASSERT_NOT_NULL(files);
    int found_split = 0;
    size_t idx = 0;
    size_t fmax = 0;
    yyjson_val *fe = NULL;
    yyjson_arr_foreach(files, idx, fmax, fe) {
        const char *fp = yyjson_get_str(yyjson_obj_get(fe, "path"));
        const char *ranges = yyjson_get_str(yyjson_obj_get(fe, "error_ranges"));
        if (fp && strstr(fp, "split.c")) {
            found_split = 1;
            ASSERT_NOT_NULL(ranges);
            ASSERT_GT((int)strlen(ranges), 0);
        }
    }
    ASSERT_TRUE(found_split);
    const char *note = yyjson_get_str(yyjson_obj_get(pp, "note"));
    ASSERT_NOT_NULL(note);
    /* Lean-note contract: best-effort framing + pointer to the full record. */
    ASSERT_NOT_NULL(strstr(note, "best-effort"));
    ASSERT_NOT_NULL(strstr(note, "logfile"));

    /* Logfile lists it under the distinct phase. */
    const char *logfile = yyjson_get_str(yyjson_obj_get(sc, "logfile"));
    ASSERT_NOT_NULL(logfile);
    char *logtext = ri_slurp(logfile);
    ASSERT_NOT_NULL(logtext);
    ASSERT_NOT_NULL(strstr(logtext, "parse_partial"));
    ASSERT_NOT_NULL(strstr(logtext, "split.c"));
    free(logtext);

    /* The signal is persisted in the SEPARATE index_coverage table (never
     * mixed into the graph tables) and reported by index_status,
     * exactly as the index_repository tool description advertises. */
    cbm_coverage_row_t *rows = NULL;
    int cov_count = 0;
    ASSERT_EQ(cbm_store_coverage_get(store, lp.project, &rows, &cov_count), CBM_STORE_OK);
    int marked = 0;
    for (int i = 0; i < cov_count; i++) {
        if (rows[i].rel_path && strstr(rows[i].rel_path, "split.c")) {
            ASSERT_NOT_NULL(rows[i].kind);
            ASSERT_STR_EQ("parse_partial", rows[i].kind);
            ASSERT_NOT_NULL(rows[i].detail);
            ASSERT_GT((int)strlen(rows[i].detail), 0);
            marked = 1;
        }
    }
    cbm_store_free_coverage(rows, cov_count);
    ASSERT_TRUE(marked);

    char qargs[900];
    snprintf(qargs, sizeof(qargs), "{\"project\":\"%s\"}", lp.project);
    char *qresp = cbm_mcp_handle_tool(lp.srv, "index_status", qargs);
    ASSERT_NOT_NULL(qresp);
    ASSERT_NOT_NULL(strstr(qresp, "split.c"));
    ASSERT_NOT_NULL(strstr(qresp, "parse_partial"));
    free(qresp);

    /* get_code_snippet on a symbol from the flagged file carries the
     * correlated coverage note (the result names its file, so the warning is
     * precisely anchored); the note never fires for clean files. */
    snprintf(qargs, sizeof(qargs), "{\"project\":\"%s\",\"qualified_name\":\"ok_before\"}",
             lp.project);
    char *sresp = cbm_mcp_handle_tool(lp.srv, "get_code_snippet", qargs);
    ASSERT_NOT_NULL(sresp);
    ASSERT_NOT_NULL(strstr(sresp, "coverage_note"));
    ASSERT_NOT_NULL(strstr(sresp, "PARTIALLY indexed"));
    free(sresp);
    snprintf(qargs, sizeof(qargs), "{\"project\":\"%s\",\"qualified_name\":\"alpha\"}", lp.project);
    char *cresp2 = cbm_mcp_handle_tool(lp.srv, "get_code_snippet", qargs);
    ASSERT_NOT_NULL(cresp2);
    ASSERT_NULL(strstr(cresp2, "coverage_note")); /* good.py: no note */
    free(cresp2);

    /* The missed GRAPH: the same signal is queryable as a file-structure
     * graph via query_graph(graph="missed") — exactly the query the tool
     * description advertises — and it lives under the shadow project, so the
     * REAL code graph gained no coverage rows. */
    snprintf(qargs, sizeof(qargs),
             "{\"project\":\"%s\",\"graph\":\"missed\",\"query\":\"MATCH (f:File) WHERE "
             "f.kind = \\\"parse_partial\\\" RETURN f.file_path, f.detail\"}",
             lp.project);
    char *gresp = cbm_mcp_handle_tool(lp.srv, "query_graph", qargs);
    ASSERT_NOT_NULL(gresp);
    ASSERT_NOT_NULL(strstr(gresp, "split.c"));
    free(gresp);
    snprintf(qargs, sizeof(qargs),
             "{\"project\":\"%s\",\"query\":\"MATCH (f:File) WHERE f.kind = "
             "\\\"parse_partial\\\" RETURN f.file_path\"}",
             lp.project);
    char *cresp = cbm_mcp_handle_tool(lp.srv, "query_graph", qargs);
    ASSERT_NOT_NULL(cresp);
    ASSERT_NULL(strstr(cresp, "split.c")); /* code graph: no coverage rows */
    free(cresp);

    /* Clean neighbors still extract. */
    int funcs = rh_count_label(store, lp.project, "Function");
    ASSERT_GTE(funcs, 1);

    yyjson_doc_free(d);
    free(resp);
    rh_cleanup(&lp, store);
    PASS();
}

/* INV(parse-partial-clears-on-fix, #963): the persisted coverage signal must
 * stay FRESH — after the broken file is fixed and the project re-indexed
 * (incremental route: the DB already exists), its parse_partial row is gone
 * and index_status reports it no longer. A stale flag on a fixed file
 * would make the whole signal untrustworthy. */
TEST(index_parse_partial_clears_on_fix) {
    RProj lp;
    memset(&lp, 0, sizeof(lp));
    snprintf(lp.tmpdir, sizeof(lp.tmpdir), "/tmp/cbm_resil_XXXXXX");
    if (!cbm_mkdtemp(lp.tmpdir)) {
        FAIL("mkdtemp failed");
    }
    rh_to_fwd_slashes(lp.tmpdir);

    /* The #ifdef-split-brace pattern: an UNRECOVERED miss (the first branch's
     * `guarded` never becomes a def), so the flag survives recovery
     * subtraction. A `def broken(:`-style fixture would NOT work here — its
     * def is recovered and the region is dropped. */
    ri_write_text(lp.tmpdir, "flaky.c",
                  "void ok_before(void) { }\n"
                  "#ifdef A\n"
                  "static int guarded(int x) {\n"
                  "#else\n"
                  "static int guarded_alt(int x) {\n"
                  "#endif\n"
                  "    return x + 1;\n"
                  "}\n");
    ri_write_text(lp.tmpdir, "good.py", "def alpha():\n    return 1\n");

    char *resp = NULL;
    cbm_store_t *store = ri_index_capture(&lp, &resp);
    if (!resp) {
        FAIL("no MCP response");
    }
    free(resp);
    if (!store) {
        FAIL("store did not open");
    }
    cbm_store_close(store);

    /* Flagged after the first (full) index. */
    char qargs[900];
    snprintf(qargs, sizeof(qargs), "{\"project\":\"%s\"}", lp.project);
    char *cov1 = cbm_mcp_handle_tool(lp.srv, "index_status", qargs);
    ASSERT_NOT_NULL(cov1);
    ASSERT_NOT_NULL(strstr(cov1, "flaky.c"));
    free(cov1);

    /* Fix the file; ensure a newer mtime so change detection can't miss it. */
    struct timespec ts = {0, INCR_FIX_SLEEP_NS};
    nanosleep(&ts, NULL);
    ri_write_text(lp.tmpdir, "flaky.c",
                  "void ok_before(void) { }\n"
                  "static int fixed(int x) {\n"
                  "    return x + 1;\n"
                  "}\n");

    /* Re-index WITHOUT deleting the DB → routes through the incremental path. */
    char iargs[700];
    snprintf(iargs, sizeof(iargs), "{\"repo_path\":\"%s\"}", lp.tmpdir);
    char *resp2 = cbm_mcp_handle_tool(lp.srv, "index_repository", iargs);
    ASSERT_NOT_NULL(resp2);
    free(resp2);

    char *cov2 = cbm_mcp_handle_tool(lp.srv, "index_status", qargs);
    ASSERT_NOT_NULL(cov2);
    ASSERT_NULL(strstr(cov2, "flaky.c"));
    free(cov2);

    store = cbm_store_open_path(lp.dbpath);
    if (!store) {
        FAIL("store did not reopen");
    }
    rh_cleanup(&lp, store);
    PASS();
}

/* INV(not-indexed-by-design, #963): files/dirs dropped by ignore rules are a
 * deliberate, deterministic class — reported as such and NEVER mixed into the
 * failure surfaces:
 *   - response: "not_indexed_files" lists the gitignored file with its reason
 *     and the by-design note; "excluded" lists the ignored dir; the failure
 *     surfaces stay clean (skipped_count == 0, parse_partial_count == 0),
 *   - index_status: "not_indexed" carries dirs + files with the note,
 *   - the missed graph (failures only) does NOT contain them — the UI's
 *     report-an-edge-case callout must never fire for gitignored paths,
 *   - the persisted rows survive a re-index (the deleted-file prune must not
 *     eat them — deliberately-unindexed paths have no file_hashes rows). */
TEST(index_not_indexed_by_design_reported) {
    RProj lp;
    memset(&lp, 0, sizeof(lp));
    snprintf(lp.tmpdir, sizeof(lp.tmpdir), "/tmp/cbm_resil_XXXXXX");
    if (!cbm_mkdtemp(lp.tmpdir)) {
        FAIL("mkdtemp failed");
    }
    rh_to_fwd_slashes(lp.tmpdir);

    ri_write_text(lp.tmpdir, ".gitignore", "secret.py\ngenerated/\n");
    ri_write_text(lp.tmpdir, "good.py", "def alpha():\n    return 1\n");
    ri_write_text(lp.tmpdir, "secret.py", "def hidden():\n    return 42\n");
    char gen_dir[700];
    snprintf(gen_dir, sizeof(gen_dir), "%s/generated", lp.tmpdir);
    cbm_mkdir(gen_dir);
    ri_write_text(gen_dir, "gen.py", "def generated():\n    return 3\n");

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

    /* By design ≠ failure: both failure surfaces stay clean. */
    ASSERT_STR_EQ("indexed", yyjson_get_str(yyjson_obj_get(sc, "status")));
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(sc, "skipped_count")), 0);
    ASSERT_EQ(yyjson_get_int(yyjson_obj_get(sc, "parse_partial_count")), 0);

    /* The gitignored FILE is listed with its reason + the by-design note. */
    ASSERT_GTE(yyjson_get_int(yyjson_obj_get(sc, "not_indexed_files_count")), 1);
    yyjson_val *ni = yyjson_obj_get(sc, "not_indexed_files");
    ASSERT_NOT_NULL(ni);
    yyjson_val *files = yyjson_obj_get(ni, "files");
    ASSERT_NOT_NULL(files);
    int found_secret = 0;
    size_t idx = 0;
    size_t fmax = 0;
    yyjson_val *fe = NULL;
    yyjson_arr_foreach(files, idx, fmax, fe) {
        const char *fp = yyjson_get_str(yyjson_obj_get(fe, "path"));
        const char *reason = yyjson_get_str(yyjson_obj_get(fe, "reason"));
        if (fp && strcmp(fp, "secret.py") == 0) {
            found_secret = 1;
            ASSERT_NOT_NULL(reason);
            ASSERT_STR_EQ("gitignore", reason);
        }
    }
    ASSERT_TRUE(found_secret);
    const char *note = yyjson_get_str(yyjson_obj_get(ni, "note"));
    ASSERT_NOT_NULL(note);
    /* Lean-note contract: deliberate-exclusion framing + pointer to the record. */
    ASSERT_NOT_NULL(strstr(note, "by design"));
    ASSERT_NOT_NULL(strstr(note, "logfile"));

    /* The gitignored DIR is in the excluded subtrees list. */
    yyjson_val *excluded = yyjson_obj_get(sc, "excluded");
    ASSERT_NOT_NULL(excluded);
    char *excl_json = yyjson_val_write(excluded, 0, NULL);
    ASSERT_NOT_NULL(excl_json);
    ASSERT_NOT_NULL(strstr(excl_json, "generated"));
    free(excl_json);

    /* index_status carries the persisted by-design section. */
    char qargs[900];
    snprintf(qargs, sizeof(qargs), "{\"project\":\"%s\"}", lp.project);
    char *sresp = cbm_mcp_handle_tool(lp.srv, "index_status", qargs);
    ASSERT_NOT_NULL(sresp);
    ASSERT_NOT_NULL(strstr(sresp, "not_indexed"));
    ASSERT_NOT_NULL(strstr(sresp, "secret.py"));
    ASSERT_NOT_NULL(strstr(sresp, "generated"));
    ASSERT_NOT_NULL(strstr(sresp, "BY DESIGN"));
    free(sresp);

    /* The missed graph shows FAILURES only — no by-design paths, so the UI
     * callout can never fire for them. */
    snprintf(qargs, sizeof(qargs),
             "{\"project\":\"%s\",\"graph\":\"missed\",\"query\":\"MATCH (f) RETURN "
             "f.file_path\"}",
             lp.project);
    char *gresp = cbm_mcp_handle_tool(lp.srv, "query_graph", qargs);
    ASSERT_NOT_NULL(gresp);
    ASSERT_NULL(strstr(gresp, "secret.py"));
    ASSERT_NULL(strstr(gresp, "generated"));
    free(gresp);

    /* Re-index (DB exists → incremental route): the by-design rows survive
     * the rebuild + deleted-file prune and stay fresh. */
    char iargs[700];
    snprintf(iargs, sizeof(iargs), "{\"repo_path\":\"%s\"}", lp.tmpdir);
    char *resp2 = cbm_mcp_handle_tool(lp.srv, "index_repository", iargs);
    ASSERT_NOT_NULL(resp2);
    free(resp2);
    char *sresp2 = cbm_mcp_handle_tool(lp.srv, "index_status", qargs);
    ASSERT_NOT_NULL(sresp2);
    ASSERT_NOT_NULL(strstr(sresp2, "secret.py"));
    ASSERT_NOT_NULL(strstr(sresp2, "generated"));
    free(sresp2);

    /* The ignored constructs are genuinely absent from the graph (label
     * counts include <python-builtins> stubs, so assert by name). */
    cbm_node_t *hits = NULL;
    int hit_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_name(store, lp.project, "hidden", &hits, &hit_count),
              CBM_STORE_OK);
    ASSERT_EQ(hit_count, 0);
    cbm_store_free_nodes(hits, hit_count);
    hits = NULL;
    hit_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_name(store, lp.project, "alpha", &hits, &hit_count),
              CBM_STORE_OK);
    ASSERT_GTE(hit_count, 1); /* the non-ignored neighbor IS indexed */
    cbm_store_free_nodes(hits, hit_count);

    yyjson_doc_free(d);
    free(resp);
    rh_cleanup(&lp, store);
    PASS();
}

/* INV(relative-repo-path, #794): index_repository with repo_path "." must
 * canonicalize to the absolute path BEFORE anything derives from it. On
 * v0.8.1 the literal "." flowed through: the project was named "root", the
 * projects table stored root_path="." verbatim, the post-dump integrity
 * check flagged bad_root_path and AUTO-DELETED the whole DB — while the
 * response still claimed status "indexed". The guard asserts the three
 * things that failure mode breaks: a path-derived project name (not
 * "root"), an absolute stored root_path, and a DB that persists and stays
 * queryable after the run. */
TEST(index_relative_repo_path_canonicalized) {
#ifdef _WIN32
    /* realpath/chdir are POSIX; the assertions are POSIX-path-shaped. The
     * #794 canonicalization logic under guard is platform-independent, so
     * the four Unix legs carry this regression guard. */
    SKIP_PLATFORM("POSIX-only guard (realpath/chdir); #794 logic is platform-independent");
#else
    RProj lp;
    memset(&lp, 0, sizeof(lp));
    snprintf(lp.tmpdir, sizeof(lp.tmpdir), "/tmp/cbm_resil_XXXXXX");
    if (!cbm_mkdtemp(lp.tmpdir)) {
        FAIL("mkdtemp failed");
    }
    rh_to_fwd_slashes(lp.tmpdir);
    ri_write_text(lp.tmpdir, "good.py", "def alpha():\n    return 1\n");

    /* Resolve the canonical form of tmpdir (macOS: /tmp -> /private/tmp) so
     * the derived-name and stored-root assertions compare like with like. */
    char canon[PATH_MAX];
    if (!realpath(lp.tmpdir, canon)) {
        snprintf(canon, sizeof(canon), "%s", lp.tmpdir);
    }
    lp.project = cbm_project_name_from_path(canon);
    if (!lp.project) {
        FAIL("project name derivation failed");
    }

    const char *home = getenv("HOME");
    if (!home) {
        home = "/tmp";
    }
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp.dbpath, sizeof(lp.dbpath), "%s/%s.db", cache_dir, lp.project);
    unlink(lp.dbpath);

    lp.srv = cbm_mcp_server_new(NULL);
    if (!lp.srv) {
        FAIL("server alloc failed");
    }

    /* chdir into the repo and index "." — restore cwd immediately after. */
    char oldcwd[PATH_MAX];
    if (!getcwd(oldcwd, sizeof(oldcwd))) {
        FAIL("getcwd failed");
    }
    if (chdir(lp.tmpdir) != 0) {
        FAIL("chdir failed");
    }
    char *resp = cbm_mcp_handle_tool(lp.srv, "index_repository", "{\"repo_path\":\".\"}");
    int rc_back = chdir(oldcwd);
    if (rc_back != 0) {
        free(resp);
        FAIL("chdir back failed");
    }
    ASSERT_NOT_NULL(resp);

    /* The response names the path-derived project, never the literal-dot
     * artifact "root". */
    ASSERT_NULL(strstr(resp, "\"project\":\"root\""));
    ASSERT_NOT_NULL(strstr(resp, lp.project));
    ASSERT_NOT_NULL(strstr(resp, "\"status\":\"indexed\""));
    free(resp);

    /* The DB survived the run (v0.8.1 auto-deleted it here)... */
    cbm_store_t *store = cbm_store_open_path(lp.dbpath);
    if (!store) {
        FAIL("db missing after relative-path index (auto-clean regression)");
    }

    /* ...with an ABSOLUTE canonical root_path in the projects table... */
    cbm_project_t info = {0};
    ASSERT_EQ(cbm_store_get_project(store, lp.project, &info), CBM_STORE_OK);
    ASSERT_NOT_NULL(info.root_path);
    ASSERT_TRUE(info.root_path[0] == '/');
    ASSERT_STR_EQ(info.root_path, canon);
    free((char *)info.name);
    free((char *)info.indexed_at);
    free((char *)info.root_path);

    /* ...and it stays queryable. */
    cbm_node_t *hits = NULL;
    int hit_count = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_name(store, lp.project, "alpha", &hits, &hit_count),
              CBM_STORE_OK);
    ASSERT_GTE(hit_count, 1);
    cbm_store_free_nodes(hits, hit_count);

    rh_cleanup(&lp, store);
    PASS();
#endif /* !_WIN32 */
}

SUITE(index_resilience) {
    RUN_TEST(index_oversized_file_reported);
    RUN_TEST(index_clean_run_no_logfile);
    RUN_TEST(index_parse_partial_reported);
    RUN_TEST(index_parse_partial_clears_on_fix);
    RUN_TEST(index_not_indexed_by_design_reported);
    RUN_TEST(index_relative_repo_path_canonicalized);
}
