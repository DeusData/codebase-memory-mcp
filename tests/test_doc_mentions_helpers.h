/*
 * test_doc_mentions_helpers.h — helpers shared by the doc-mentions suites
 * (tests/test_doc_mentions.c and one test_doc_mentions_<lang>.c per language
 * leg).
 *
 * Every pipeline helper indexes a real fixture through cbm_pipeline_run and
 * reads the published database, so a test sees what a user's index holds.
 * All functions are static inline, like test_helpers.h: including the header
 * from several test files causes no linker issue, and a file that uses only
 * some of them gets no unused-function warning.
 */
#ifndef TEST_DOC_MENTIONS_HELPERS_H
#define TEST_DOC_MENTIONS_HELPERS_H

#include "../src/foundation/compat.h"
#include "test_helpers.h"

#include "cbm.h"
#include "doclink.h"
#include "foundation/mem_core.h"
#include "mcp/mcp.h"
#include "pipeline/doc_links.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "sqlite3.h"
#include <yyjson/yyjson.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── extraction ──────────────────────────────────────────────────── */

/* Extract one source text as language `lang` under project "p"; the result
 * (tokens in doc_links, scope blob in doc_scope) is freed with
 * cbm_free_result. */
static inline CBMFileResult *dm_extract(const char *src, CBMLanguage lang, const char *rel_path) {
    return cbm_extract_file(src, (int)strlen(src), lang, "p", rel_path, 0, NULL, NULL);
}

/* The first token written as `raw`; NULL when there is none. */
static inline const CBMDocLink *dm_find_token(const CBMFileResult *r, const char *raw) {
    for (int i = 0; i < r->doc_links.count; i++) {
        if (strcmp(r->doc_links.items[i].raw, raw) == 0) {
            return &r->doc_links.items[i];
        }
    }
    return NULL;
}

static inline int dm_count_tokens(const CBMFileResult *r, const char *raw) {
    int n = 0;
    for (int i = 0; i < r->doc_links.count; i++) {
        n += strcmp(r->doc_links.items[i].raw, raw) == 0;
    }
    return n;
}

/* ── the published database ──────────────────────────────────────── */

/* Index `repo` into `db` (full mode; a second call on the same database takes
 * the incremental route). The project name is strdup'ed into *project_out
 * when that is not NULL. Returns the pipeline's result. */
static inline int dm_index(const char *repo, const char *db, char **project_out) {
    cbm_pipeline_t *p = cbm_pipeline_new(repo, db, CBM_MODE_FULL);
    if (!p) {
        return -1;
    }
    int rc = cbm_pipeline_run(p);
    if (project_out) {
        *project_out = strdup(cbm_pipeline_project_name(p));
    }
    cbm_pipeline_free(p);
    return rc;
}

/* Properties of the MENTIONS edge whose endpoints' qualified names END with
 * the given local paths ("Widget", "Helper.Once"); "" when absent; count via
 * *n. */
static inline void dm_edge(const char *db, const char *src_suffix, const char *tgt_suffix,
                           char *props, size_t cap, int *n) {
    props[0] = '\0';
    *n = 0;
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        *n = -1;
        return;
    }
    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT e.properties FROM edges e JOIN nodes s ON s.id = e.source_id "
        "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
        "AND (s.qualified_name LIKE '%.' || ?1) AND (t.qualified_name LIKE '%.' || ?2)";
    if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, src_suffix, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, tgt_suffix, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            (*n)++;
            snprintf(props, cap, "%s", (const char *)sqlite3_column_text(st, 0));
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
}

/* Number of MENTIONS edges leaving the definition whose qualified name ends
 * with `src_suffix`; -1 when the database cannot be read. */
static inline int dm_mentions_from(const char *db, const char *src_suffix) {
    sqlite3 *h = NULL;
    int n = -1;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(h,
                               "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                               "WHERE e.type = 'MENTIONS' AND s.qualified_name LIKE '%.' || ?1",
                               -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, src_suffix, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) {
                n = sqlite3_column_int(st, 0);
            }
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(h);
    return n;
}

/* The single integer a query returns; -1 when it cannot be read. */
static inline int dm_count(const char *db, const char *sql) {
    sqlite3 *h = NULL;
    int n = -1;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK &&
            sqlite3_step(st) == SQLITE_ROW) {
            n = sqlite3_column_int(st, 0);
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(h);
    return n;
}

/* Reason (and, when `syntax` is not NULL, the family name) of the unresolved
 * row with this raw text in this file; "" when there is none. */
static inline void dm_row(const char *db, const char *rel, const char *raw, char *reason,
                          size_t cap, char *syntax, size_t scap) {
    reason[0] = '\0';
    if (syntax) {
        syntax[0] = '\0';
    }
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(h,
                               "SELECT reason, syntax FROM doc_link_unresolved WHERE rel_path = ?1 "
                               "AND raw = ?2",
                               -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, rel, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 2, raw, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) {
                snprintf(reason, cap, "%s", (const char *)sqlite3_column_text(st, 0));
                if (syntax) {
                    snprintf(syntax, scap, "%s", (const char *)sqlite3_column_text(st, 1));
                }
            }
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(h);
}

/* Canonical text of every MENTIONS edge and unresolved row: what two indexes
 * of the same tree (full and incremental, one worker and several) must agree
 * on byte for byte. The caller frees it; NULL when the database cannot be
 * read. */
static inline char *dm_doclink_state(const char *db) {
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return NULL;
    }
    size_t cap = 4096;
    size_t len = 0;
    char *buf = malloc(cap);
    buf[0] = '\0';
    const char *queries[] = {
        "SELECT 'E ' || s.qualified_name || ' -> ' || t.qualified_name || ' ' || e.properties "
        "FROM edges e JOIN nodes s ON s.id = e.source_id JOIN nodes t ON t.id = e.target_id "
        "WHERE e.type = 'MENTIONS' ORDER BY 1",
        "SELECT 'R ' || rel_path || ':' || line || ' ' || syntax || ' [' || raw || '] ' || reason "
        "FROM doc_link_unresolved ORDER BY 1",
    };
    for (size_t q = 0; q < sizeof(queries) / sizeof(queries[0]); q++) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(h, queries[q], -1, &st, NULL) != SQLITE_OK) {
            continue;
        }
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *line = (const char *)sqlite3_column_text(st, 0);
            size_t l = strlen(line);
            if (len + l + 2 > cap) {
                cap = (len + l + 2) * 2;
                buf = realloc(buf, cap);
            }
            memcpy(buf + len, line, l);
            len += l;
            buf[len++] = '\n';
            buf[len] = '\0';
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(h);
    return buf;
}

/* Remove a database and its sidecars. */
static inline void dm_unlink_db(const char *db) {
    char side[600];
    unlink(db);
    snprintf(side, sizeof(side), "%s-wal", db);
    unlink(side);
    snprintf(side, sizeof(side), "%s-shm", db);
    unlink(side);
}

/* ── incremental == full ─────────────────────────────────────────── */

/* One step of an incremental test, after the caller edited the tree: index
 * `repo` incrementally into `inc_db`, then fully into a fresh `full_db`, and
 * require identical MENTIONS edges and unresolved rows AND the expected
 * route. Returns 0, or -1 after printing what differs (`what` names the
 * step). */
static inline int dm_step(const char *repo, const char *inc_db, const char *full_db,
                          const char *what, cbm_incremental_route_t want_route) {
    cbm_pipeline_incremental_test_reset_faults();
    if (dm_index(repo, inc_db, NULL) != 0) {
        printf("  %s: incremental index failed\n", what);
        return -1;
    }
    cbm_incremental_route_t route = cbm_pipeline_incremental_test_last_route();
    dm_unlink_db(full_db);
    if (dm_index(repo, full_db, NULL) != 0) {
        printf("  %s: full index failed\n", what);
        return -1;
    }
    char *inc = dm_doclink_state(inc_db);
    char *full = dm_doclink_state(full_db);
    int rc = 0;
    if (!inc || !full || strcmp(inc, full) != 0) {
        printf("  %s: incremental != full\n--- incremental (route %d)\n%s--- full\n%s", what,
               (int)route, inc ? inc : "(null)", full ? full : "(null)");
        rc = -1;
    } else if (route != want_route) {
        printf("  %s: route %d, expected %d\n", what, (int)route, (int)want_route);
        rc = -1;
    }
    free(inc);
    free(full);
    return rc;
}

/* ── one worker == several workers ───────────────────────────────── */

/* Index `repo` with four workers into `par_db` and with one worker into
 * `seq_db` (the fixture needs more than 50 files, or both take the sequential
 * passes), and require identical MENTIONS edges and unresolved rows.
 * CBM_WORKERS is restored. Returns 0, or -1 after printing what differs. */
static inline int dm_workers_agree(const char *repo, const char *par_db, const char *seq_db) {
    const char *saved_workers = getenv("CBM_WORKERS");
    char *saved_workers_copy = saved_workers ? strdup(saved_workers) : NULL;
    cbm_setenv("CBM_WORKERS", "4", 1);
    int par_rc = dm_index(repo, par_db, NULL);
    cbm_setenv("CBM_WORKERS", "1", 1); /* one worker: the sequential passes */
    int seq_rc = dm_index(repo, seq_db, NULL);
    if (saved_workers_copy) {
        cbm_setenv("CBM_WORKERS", saved_workers_copy, 1);
        free(saved_workers_copy);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    if (par_rc != 0 || seq_rc != 0) {
        printf("  index failed: parallel rc %d, sequential rc %d\n", par_rc, seq_rc);
        return -1;
    }
    char *par = dm_doclink_state(par_db);
    char *seq = dm_doclink_state(seq_db);
    bool same = par && seq && strcmp(par, seq) == 0;
    if (!same) {
        printf("  parallel != sequential\n--- parallel\n%s--- sequential\n%s", par ? par : "(null)",
               seq ? seq : "(null)");
    }
    free(par);
    free(seq);
    return same ? 0 : -1;
}

/* ── scope deltas ────────────────────────────────────────────────── */

/* The names a scope delta reported, joined by ','. */
typedef struct {
    char names[256];
} dm_names_t;

/* cbm_doclink_name_fn collecting into a dm_names_t. */
static inline bool dm_name_put(void *ud, const char *name, size_t len) {
    dm_names_t *n = (dm_names_t *)ud;
    size_t used = strlen(n->names);
    if (used + len + 2 > sizeof(n->names)) {
        return false;
    }
    if (used > 0) {
        n->names[used++] = ',';
    }
    memcpy(n->names + used, name, len);
    n->names[used + len] = '\0';
    return true;
}

enum { DM_DELTA_SCAN_FAILED = -2 };

/* The scope delta between two versions of one file of language `lang` (NULL:
 * the file does not exist), through the language's real scanner and the
 * persisted form of its scope; the removed names joined by ',' in `names`.
 * Returns the cbm_doclink_delta_t, -1 as the hook does, or
 * DM_DELTA_SCAN_FAILED when a version yields no scope. */
static inline int dm_scope_delta(CBMLanguage lang, const char *rel_path, const char *before,
                                 const char *after, char *names, size_t cap) {
    names[0] = '\0';
    CBMFileResult *a = before ? dm_extract(before, lang, rel_path) : NULL;
    CBMFileResult *b = after ? dm_extract(after, lang, rel_path) : NULL;
    char *pa = (a && a->doc_scope) ? cbm_doclink_portable_scope(a->doc_scope) : NULL;
    char *pb = (b && b->doc_scope) ? cbm_doclink_portable_scope(b->doc_scope) : NULL;
    dm_names_t n = {{0}};
    int rc = ((before && !pa) || (after && !pb))
                 ? DM_DELTA_SCAN_FAILED
                 : cbm_doclinks_scope_delta(pa, pb, dm_name_put, &n);
    snprintf(names, cap, "%s", n.names);
    cbm_free(CBM_MEM_CLASS_OTHER, pa);
    cbm_free(CBM_MEM_CLASS_OTHER, pb);
    if (a) {
        cbm_free_result(a);
    }
    if (b) {
        cbm_free_result(b);
    }
    return rc;
}

/* ── index_status ────────────────────────────────────────────────── */

/* The text content of an MCP tool result (the report itself); the caller
 * frees it. */
static inline char *dm_tool_text(const char *mcp_result) {
    yyjson_doc *doc = mcp_result ? yyjson_read(mcp_result, strlen(mcp_result), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *content = root ? yyjson_obj_get(root, "content") : NULL;
    yyjson_val *item = content ? yyjson_arr_get(content, 0) : NULL;
    const char *text = item ? yyjson_get_str(yyjson_obj_get(item, "text")) : NULL;
    char *out = text ? strdup(text) : NULL;
    yyjson_doc_free(doc);
    return out;
}

/* index_status of the project as text, from a server of its own (nothing
 * cached from an earlier call). The project's database is looked up in
 * CBM_CACHE_DIR: point that at a private directory first. */
static inline char *dm_index_status(const char *project, bool full) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return NULL;
    }
    char args[1200];
    snprintf(args, sizeof(args), "{\"project\":\"%s\"%s}", project,
             full ? ",\"diagnostics\":\"full\"" : "");
    char *resp = cbm_mcp_handle_tool(srv, "index_status", args);
    char *text = dm_tool_text(resp);
    free(resp);
    cbm_mcp_server_free(srv);
    return text;
}

/* Every reason the table holds is listed under doc_links.unresolved with its
 * row count. The number of reasons checked; -1 when a line is missing. */
static inline int dm_reason_lines(const char *db, const char *block) {
    sqlite3 *h = NULL;
    int n = -1;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(h,
                               "SELECT reason, COUNT(*) FROM doc_link_unresolved GROUP BY reason",
                               -1, &st, NULL) == SQLITE_OK) {
            n = 0;
            while (n >= 0 && sqlite3_step(st) == SQLITE_ROW) {
                char line[128];
                snprintf(line, sizeof(line), "\n    %s: %d\n",
                         (const char *)sqlite3_column_text(st, 0), sqlite3_column_int(st, 1));
                if (strstr(block, line)) {
                    n++;
                } else {
                    printf("  no line%s  in\n%s\n", line, block);
                    n = -1;
                }
            }
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(h);
    return n;
}

#endif /* TEST_DOC_MENTIONS_HELPERS_H */
