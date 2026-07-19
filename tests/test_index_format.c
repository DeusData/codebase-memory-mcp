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

static const char k_badge_ts[] =
    "export class BadgeComponent {\n"
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
    capture_reset();
    cbm_log_set_sink_ex(capture_sink, CBM_LOG_SINK_TEE);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    cbm_log_set_sink(NULL);
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
    char *resp = cbm_mcp_handle_tool(lp.srv, "search_code", args);
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
    cbm_store_t *store = rh_index_files(&lp, k_files, k_nfiles);
    ASSERT_NOT_NULL(store);

    ASSERT_EQ(cbm_store_adr_store(store, lp.project, "index-format-adr"), CBM_STORE_OK);

    snprintf(legacy_qn, sizeof(legacy_qn), "%s.badge.badge.component.__file__", lp.project);
    ASSERT_EQ(make_legacy_file_graph(store, lp.project, legacy_qn), 0);
    ASSERT_EQ(cbm_store_set_format_version(store, 0), CBM_STORE_OK);
    cbm_store_close(store);

    /* Run 1: the stale format must force the full-reindex path. */
    char *resp = index_capture(&lp);
    ASSERT_NOT_NULL(resp);
    free(resp);
    if (!strstr(g_log_buf, "format_change_reindex")) {
        FAIL("a stale-format index was not routed through the full-reindex path");
    }

    store = cbm_store_open_path(lp.dbpath);
    ASSERT_NOT_NULL(store);

    /* The collided identity is gone, not merely shadowed. */
    cbm_node_t stale = {0};
    ASSERT_EQ(cbm_store_find_node_by_qn(store, lp.project, legacy_qn, &stale),
              CBM_STORE_NOT_FOUND);
    cbm_node_free_fields(&stale);
    /* Each sibling now has its own File node under its exact new QN. */
    static const char *k_exts[] = {"ts", "html", "scss"};
    for (int i = 0; i < 3; i++) {
        char qn[512];
        snprintf(qn, sizeof(qn), "%s.badge.badge.component.%s.__file__", lp.project, k_exts[i]);
        cbm_node_t n = {0};
        ASSERT_EQ(cbm_store_find_node_by_qn(store, lp.project, qn, &n), CBM_STORE_OK);
        cbm_node_free_fields(&n);
    }
    ASSERT_EQ(rh_count_label(store, lp.project, "File"), k_nfiles);

    /* The rebuild preserves ADR/project metadata (#516) ... */
    cbm_adr_t adr = {0};
    ASSERT_EQ(cbm_store_adr_get(store, lp.project, &adr), CBM_STORE_OK);
    ASSERT_NOT_NULL(adr.content);
    ASSERT(strstr(adr.content, "index-format-adr") != NULL);
    cbm_store_adr_free(&adr);
    
    /* ... and stamps the current format so it does not rebuild again. */
    int fmt = -1;
    ASSERT_EQ(cbm_store_get_format_version(store, &fmt), CBM_STORE_OK);
    ASSERT_EQ(fmt, CBM_INDEX_FORMAT_VERSION);
    cbm_store_close(store);

    /* Run 2: unchanged and current-format — no second rebuild. */
    resp = index_capture(&lp);
    ASSERT_NOT_NULL(resp);
    free(resp);
    if (strstr(g_log_buf, "format_change_reindex")) {
        FAIL("a current-format index was rebuilt again");
    }

    store = cbm_store_open_path(lp.dbpath);
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(rh_count_label(store, lp.project, "File"), k_nfiles);
    rh_cleanup(&lp, store);
    PASS();
}

SUITE(index_format) {
    RUN_TEST(index_format_siblings_distinct_and_searchable);
    RUN_TEST(index_format_legacy_index_rebuilds_and_repairs);
}
