/*
 * test_index_format.c — Guard for the persisted index-format boundary (#769).
 *
 * #1108 changed File-node QNs to keep the file extension, so an index written
 * before it holds collided File identities: badge.component.ts/.html/.scss all
 * stripped to the same stem and only one File node survived per component.
 * Refreshing such an index incrementally would mint new-format QNs only for the
 * files that happened to change, leaving the old collided node behind — a mixed
 * graph with duplicate nodes and stale edges.
 *
 * A stale-format index must therefore be routed through the existing
 * full-reindex path exactly once (which preserves ADR/project metadata per
 * #516), and the rebuilt index must not force a second rebuild on the next
 * unchanged run.
 */
#include "test_framework.h"
#include "repro_harness.h" /* RProj, rh_index_files, rh_count_label, rh_cleanup */
#include "foundation/log.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ── Fixture: one component's siblings + an uncolliding control ─────── */

static const char k_badge_ts[] = "export class BadgeComponent {\n"
                                 "  isHighlighted() { return true; } /* repro-marker */\n"
                                 "}\n";

static const char k_badge_html[] = "<div class=\"badge\">repro-marker</div>\n";

static const char k_badge_scss[] = ".badge { color: red; /* repro-marker */ }\n";

/* help.html shares no stem with anything, so it is searchable both before and
 * after the fix — a control that isolates the collision as the cause. */
static const char k_help_html[] = "<p>repro-marker</p>\n";

static const RFile k_files[] = {
    {"badge/badge.component.ts", k_badge_ts},
    {"badge/badge.component.html", k_badge_html},
    {"badge/badge.component.scss", k_badge_scss},
    {"standalone/help.html", k_help_html},
};
static const int k_nfiles = (int)(sizeof(k_files) / sizeof(k_files[0]));

/* ── Log capture: the routing decision is only visible in the log ───── */

enum { IF_LOG_BUF = 8192 };
static char g_log_buf[IF_LOG_BUF];
static size_t g_log_len;

static void capture_sink(const char *line) {
    size_t n = strlen(line);
    if (g_log_len + n + 2 < sizeof(g_log_buf)) {
        memcpy(g_log_buf + g_log_len, line, n);
        g_log_len += n;
        g_log_buf[g_log_len++] = '\n';
        g_log_buf[g_log_len] = '\0';
    }
}

static void capture_reset(void) {
    g_log_len = 0;
    g_log_buf[0] = '\0';
}

/* Run index_repository through the production MCP flow, capturing the log. */
static char *index_capture(RProj *lp) {
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *prior = getenv("CBM_CACHE_DIR");
    char *saved = prior ? strdup(prior) : NULL;
    cbm_setenv("CBM_CACHE_DIR", lp->cachedir, 1);
    capture_reset();
    cbm_log_set_sink_ex(capture_sink, CBM_LOG_SINK_TEE);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    cbm_log_set_sink(NULL);
    if (saved) {
        cbm_setenv("CBM_CACHE_DIR", saved, 1);
        free(saved);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    return resp;
}
/* ── Test 1: one File node per file, and every file reaches search ──── */

TEST(index_format_siblings_distinct_and_searchable) {
    RProj lp;
    char args[700];
    cbm_store_t *store = rh_index_files(&lp, k_files, k_nfiles);
    ASSERT_NOT_NULL(store);

    /* One File node per file on disk — not one per component stem. */
    ASSERT_EQ(rh_count_label(store, lp.project, "File"), k_nfiles);

    /* cbm_store_list_files is the set search_code scopes grep to: a file with
     * no node carrying its exact path is never opened, and the miss is silent. */
    char **listed = NULL;
    int nlisted = 0;
    ASSERT_EQ(cbm_store_list_files(store, lp.project, &listed, &nlisted), CBM_STORE_OK);
    for (int i = 0; i < k_nfiles; i++) {
        bool found = false;
        for (int j = 0; j < nlisted; j++) {
            if (listed[j] && strcmp(listed[j], k_files[i].name) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            FAIL("indexed file missing from cbm_store_list_files");
        }
    }
    for (int j = 0; j < nlisted; j++) {
        free(listed[j]);
    }
    free(listed);

    /* Every sibling's marker is reachable. .scss/.html have no def nodes, so a
     * hit lands in raw_matches rather than a containing node — mode="files"
     * merges both, so assert against the file list. */
    snprintf(args, sizeof(args),
             "{\"project\":\"%s\",\"pattern\":\"repro-marker\",\"mode\":\"files\"}", lp.project);
    char *saved_dup = getenv("CBM_CACHE_DIR") ? strdup(getenv("CBM_CACHE_DIR")) : NULL;
    cbm_setenv("CBM_CACHE_DIR", lp.cachedir, 1);
    char *resp = cbm_mcp_handle_tool(lp.srv, "search_code", args);
    if (saved_dup) {
        cbm_setenv("CBM_CACHE_DIR", saved_dup, 1);
        free(saved_dup);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    ASSERT_NOT_NULL(resp);
    for (int i = 0; i < k_nfiles; i++) {
        if (!strstr(resp, k_files[i].name)) {
            free(resp);
            FAIL("search_code did not reach an indexed sibling");
        }
    }
    free(resp);

    rh_cleanup(&lp, store);
    PASS();
}

/* ── Test 2: a legacy index rebuilds once, repairs, and settles ─────── */

/* Rewrite the graph into the pre-#1108 shape: the three siblings collapsed onto
 * a single File node keyed by the extension-stripped QN, which is what such an
 * index actually holds on disk. */
static int make_legacy_file_graph(cbm_store_t *store, const char *project, const char *legacy_qn) {
    if (cbm_store_delete_nodes_by_label(store, project, "File") != CBM_STORE_OK) {
        return -1;
    }
    cbm_node_t legacy = {
        .project = project,
        .label = "File",
        .name = "badge.component.html",
        .qualified_name = legacy_qn,
        .file_path = "badge/badge.component.html",
        .properties_json = "{\"extension\":\".html\"}",
    };
    return cbm_store_upsert_node(store, &legacy) > 0 ? 0 : -1;
}

TEST(index_format_legacy_index_rebuilds_and_repairs) {
    char legacy_qn[512];
    RProj lp;

    /* Phase 1: index, then discard the query handle. */
    cbm_store_t *q0 = rh_index_files(&lp, k_files, k_nfiles);
    ASSERT_NOT_NULL(q0);
    cbm_store_close(q0);

    /* Phase 2: fabricate a legacy graph through a writable handle. */
    cbm_store_t *w = cbm_store_open_path(lp.dbpath);
    ASSERT_NOT_NULL(w);
    ASSERT_EQ(cbm_store_adr_store(w, lp.project, "index-format-adr"), CBM_STORE_OK);
    snprintf(legacy_qn, sizeof(legacy_qn), "%s.badge.badge.component.__file__", lp.project);
    ASSERT_EQ(make_legacy_file_graph(w, lp.project, legacy_qn), 0);
    ASSERT_EQ(cbm_store_set_format_version(w, 0), CBM_STORE_OK);
    cbm_store_close(w);

    cbm_store_t *probe = cbm_store_open_path_query(lp.dbpath);
    if (probe)
        cbm_store_close(probe);

    /* Run 1: stale format forces the full-reindex path. */
    char *resp = index_capture(&lp);
    ASSERT_NOT_NULL(resp);
    if (!strstr(g_log_buf, "format_change_reindex")) {
        free(resp);
        FAIL("a stale-format index was not routed through the full-reindex path");
    }
    if (!strstr(resp, "\"format_migration\":true")) {
        free(resp);
        FAIL("index_repository response did not surface format_migration");
    }
    free(resp);

    /* Phase 3: verify the rebuild through a fresh read handle. */
    cbm_store_t *r1 = cbm_store_open_path(lp.dbpath);
    ASSERT_NOT_NULL(r1);
    cbm_node_t stale = {0};
    ASSERT_EQ(cbm_store_find_node_by_qn(r1, lp.project, legacy_qn, &stale), CBM_STORE_NOT_FOUND);
    cbm_node_free_fields(&stale);
    static const char *k_exts[] = {"ts", "html", "scss"};
    for (int i = 0; i < 3; i++) {
        char qn[512];
        snprintf(qn, sizeof(qn), "%s.badge.badge.component.%s.__file__", lp.project, k_exts[i]);
        cbm_node_t n = {0};
        ASSERT_EQ(cbm_store_find_node_by_qn(r1, lp.project, qn, &n), CBM_STORE_OK);
        cbm_node_free_fields(&n);
    }
    ASSERT_EQ(rh_count_label(r1, lp.project, "File"), k_nfiles);
    cbm_adr_t adr = {0};
    ASSERT_EQ(cbm_store_adr_get(r1, lp.project, &adr), CBM_STORE_OK);
    ASSERT_NOT_NULL(adr.content);
    ASSERT(strstr(adr.content, "index-format-adr") != NULL);
    cbm_store_adr_free(&adr);
    int fmt = -1;
    ASSERT_EQ(cbm_store_get_format_version(r1, &fmt), CBM_STORE_OK);
    ASSERT_EQ(fmt, CBM_INDEX_FORMAT_VERSION);
    cbm_store_close(r1);

    /* Run 2: unchanged, current format — no second rebuild. */
    resp = index_capture(&lp);
    ASSERT_NOT_NULL(resp);
    if (strstr(g_log_buf, "format_change_reindex")) {
        free(resp);
        FAIL("a current-format index was rebuilt again");
    }
    if (strstr(resp, "format_migration")) {
        free(resp);
        FAIL("format_migration surfaced on an unchanged run");
    }
    free(resp);

    /* Phase 4: final check, then clean up (rh_cleanup closes r2). */
    cbm_store_t *r2 = cbm_store_open_path(lp.dbpath);
    ASSERT_NOT_NULL(r2);
    ASSERT_EQ(rh_count_label(r2, lp.project, "File"), k_nfiles);
    rh_cleanup(&lp, r2);
    PASS();
}

TEST(index_format_version_one_rebuilds) {
    RProj lp;
    cbm_store_t *initial = rh_index_files(&lp, k_files, k_nfiles);
    ASSERT_NOT_NULL(initial);
    cbm_store_close(initial);

    cbm_store_t *writer = cbm_store_open_path(lp.dbpath);
    ASSERT_NOT_NULL(writer);
    ASSERT_EQ(cbm_store_set_format_version(writer, 1), CBM_STORE_OK);
    cbm_store_close(writer);

    char *resp = index_capture(&lp);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(g_log_buf, "format_change_reindex"));
    ASSERT_NOT_NULL(strstr(resp, "\"format_migration\":true"));
    free(resp);

    cbm_store_t *after_migration = cbm_store_open_path(lp.dbpath);
    ASSERT_NOT_NULL(after_migration);
    int format = -1;
    ASSERT_EQ(cbm_store_get_format_version(after_migration, &format), CBM_STORE_OK);
    ASSERT_EQ(format, CBM_INDEX_FORMAT_VERSION);
    ASSERT_EQ(rh_count_label(after_migration, lp.project, "File"), k_nfiles);
    cbm_store_close(after_migration);

    resp = index_capture(&lp);
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(g_log_buf, "format_change_reindex"));
    ASSERT_NULL(strstr(resp, "format_migration"));
    free(resp);
    cbm_store_t *settled = cbm_store_open_path(lp.dbpath);
    ASSERT_NOT_NULL(settled);
    ASSERT_EQ(rh_count_label(settled, lp.project, "File"), k_nfiles);
    rh_cleanup(&lp, settled);
    PASS();
}

static int swift_collision_migration_case(int scenario) {
    const char *sources[] = {"func pick<T>(_ x: T) { first() }\n"
                             "func pick<T: Equatable>(_ x: T) { second() }\n"
                             "func first() {}\nfunc second() {}\n"
                             "func caller() { pick(value) }\n"
                             "func wrong() { pick(key: 1) }\n",
                             "func pick(_ x: A.Item) { first() }\n"
                             "func pick(_ x: B.Item) { second() }\n"
                             "func first() {}\nfunc second() {}\n"
                             "func caller() { pick(value) }\n"
                             "func wrong() { pick(key: 1) }\n"
                             "enum A { struct Item {} }\nenum B { struct Item {} }\n",
                             "func pick() -> Int { first(); return 1 }\n"
                             "func pick() -> String { second(); return \"x\" }\n"
                             "func first() {}\nfunc second() {}\n"
                             "func caller() { let _: Int = pick() }\n"
                             "func wrong() { pick(key: 1) }\n"};
    const char *legacy_suffix[] = {"pick(_:T)", "pick(_:Item)", "pick()"};
    const RFile files[] = {{"Service.swift", sources[scenario]}};
    RProj lp;
    cbm_store_t *s = rh_index_files(&lp, files, 1);
    ASSERT_NOT_NULL(s);
    cbm_store_close(s);
    s = cbm_store_open_path(lp.dbpath);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_delete_nodes_by_label(s, lp.project, "Function"), CBM_STORE_OK);
    char legacy_qn[512], wrong_qn[512];
    snprintf(legacy_qn, sizeof(legacy_qn), "%s.Service.%s", lp.project, legacy_suffix[scenario]);
    snprintf(wrong_qn, sizeof(wrong_qn), "%s.Service.wrong()", lp.project);
    cbm_node_t legacy = {.project = lp.project,
                         .label = "Function",
                         .name = "pick",
                         .qualified_name = legacy_qn,
                         .file_path = "Service.swift",
                         .start_line = 2,
                         .end_line = 2};
    cbm_node_t wrong = {.project = lp.project,
                        .label = "Function",
                        .name = "wrong",
                        .qualified_name = wrong_qn,
                        .file_path = "Service.swift",
                        .start_line = 6,
                        .end_line = 6};
    int64_t legacy_id = cbm_store_upsert_node(s, &legacy);
    int64_t wrong_id = cbm_store_upsert_node(s, &wrong);
    ASSERT_GT(legacy_id, 0);
    ASSERT_GT(wrong_id, 0);
    cbm_edge_t stale = {.project = lp.project,
                        .source_id = wrong_id,
                        .target_id = legacy_id,
                        .type = "CALLS",
                        .properties_json = "{\"candidates\":1}"};
    ASSERT_GT(cbm_store_insert_edge(s, &stale), 0);
    ASSERT_EQ(cbm_store_set_format_version(s, scenario ? 3 : 2), CBM_STORE_OK);
    cbm_store_close(s);
    for (int run = 0; run < 2; run++) {
        char *resp = index_capture(&lp);
        ASSERT_NOT_NULL(resp);
        if (run == 0) {
            ASSERT_NOT_NULL(strstr(g_log_buf, "format_change_reindex"));
            ASSERT_NOT_NULL(strstr(resp, "\"format_migration\":true"));
        } else {
            ASSERT_NULL(strstr(g_log_buf, "format_change_reindex"));
            ASSERT_NULL(strstr(resp, "format_migration"));
        }
        free(resp);
        s = cbm_store_open_path(lp.dbpath);
        ASSERT_NOT_NULL(s);
        cbm_node_t old = {0};
        ASSERT_EQ(cbm_store_find_node_by_qn(s, lp.project, legacy_qn, &old), CBM_STORE_NOT_FOUND);
        cbm_node_t *picks = NULL;
        int count = 0;
        ASSERT_EQ(cbm_store_find_nodes_by_name(s, lp.project, "pick", &picks, &count),
                  CBM_STORE_OK);
        ASSERT_EQ(count, 2);
        cbm_node_t caller = {0}, bad = {0};
        char qn[512];
        snprintf(qn, sizeof(qn), "%s.Service.caller()", lp.project);
        ASSERT_EQ(cbm_store_find_node_by_qn(s, lp.project, qn, &caller), CBM_STORE_OK);
        ASSERT_EQ(cbm_store_find_node_by_qn(s, lp.project, wrong_qn, &bad), CBM_STORE_OK);
        cbm_edge_t *edges = NULL;
        int n = 0;
        ASSERT_EQ(cbm_store_find_edges_by_source_type(s, bad.id, "CALLS", &edges, &n),
                  CBM_STORE_OK);
        ASSERT_EQ(n, 0);
        cbm_store_free_edges(edges, n);
        ASSERT_NEQ(picks[0].id, picks[1].id);
        ASSERT_NEQ(picks[0].start_line, picks[1].start_line);
        for (int i = 0; i < count; i++) {
            cbm_node_t marker = {0};
            snprintf(qn, sizeof(qn), "%s.Service.%s()", lp.project,
                     picks[i].start_line == 1 ? "first" : "second");
            ASSERT_EQ(cbm_store_find_node_by_qn(s, lp.project, qn, &marker), CBM_STORE_OK);
            ASSERT_EQ(cbm_store_find_edges_by_source_type(s, picks[i].id, "CALLS", &edges, &n),
                      CBM_STORE_OK);
            ASSERT_EQ(n, 1);
            ASSERT_EQ(edges[0].target_id, marker.id);
            cbm_store_free_edges(edges, n);
            cbm_node_free_fields(&marker);
        }
        ASSERT_EQ(cbm_store_find_edges_by_source_type(s, caller.id, "CALLS", &edges, &n),
                  CBM_STORE_OK);
        ASSERT_EQ(n, 2);
        for (int i = 0; i < 2; i++) {
            ASSERT_TRUE(edges[i].target_id == picks[0].id || edges[i].target_id == picks[1].id);
            ASSERT_NOT_NULL(strstr(edges[i].properties_json, "\"candidates\":2"));
            ASSERT_EQ(picks[i].start_line, picks[i].end_line);
            ASSERT_TRUE(picks[i].start_line == 1 || picks[i].start_line == 2);
        }
        cbm_store_free_edges(edges, n);
        cbm_store_free_nodes(picks, count);
        cbm_node_free_fields(&caller);
        cbm_node_free_fields(&bad);
        int format = -1;
        ASSERT_EQ(cbm_store_get_format_version(s, &format), CBM_STORE_OK);
        ASSERT_EQ(format, CBM_INDEX_FORMAT_VERSION);
        cbm_store_close(s);
    }
    rh_cleanup(&lp, NULL);
    PASS();
}

TEST(index_format_swift_collisions_and_stale_calls_rebuild) {
    for (int scenario = 0; scenario < 3; scenario++) {
        ASSERT_EQ(swift_collision_migration_case(scenario), 0);
    }
    PASS();
}

SUITE(index_format) {
    RUN_TEST(index_format_siblings_distinct_and_searchable);
    RUN_TEST(index_format_legacy_index_rebuilds_and_repairs);
    RUN_TEST(index_format_version_one_rebuilds);
    RUN_TEST(index_format_swift_collisions_and_stale_calls_rebuild);
}
