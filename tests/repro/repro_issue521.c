/*
 * repro_issue521.c — Reproduce-first case for issue #521.
 *
 * BUG: "Route nodes created from URL strings in config / non-source files"
 *
 * Root cause (pipeline.c:try_upsert_infra_route + helpers.c:is_url_like):
 *
 *   1. extract_unified.c:handle_string_refs() walks every string node in a
 *      YAML file.  Any value containing "://" passes cbm_classify_string()
 *      as CBM_STRREF_URL, landing in CBMFileResult.string_refs.
 *
 *   2. pipeline.c:cbm_pipeline_extract_infra_routes() iterates files that
 *      match is_infra_file() — which includes ".yaml" / ".yml" — and calls
 *      try_upsert_infra_route() for every CBM_STRREF_URL entry whose value
 *      contains "://".
 *
 *   3. try_upsert_infra_route() unconditionally mints a "Route" node:
 *         cbm_gbuf_upsert_node(gbuf, "Route", sr->value, route_qn, ...)
 *      with no check for whether the URL is an upstream-config value (e.g.
 *      an auth-server JWKS URL, a Terraform registry URL, a healthcheck
 *      target) versus an actual route this service exposes.
 *
 * Correct behaviour: a YAML/config file that only contains upstream URL
 * strings (no route-registration syntax, no handler definitions) MUST NOT
 * yield any Route node in the graph.
 *
 * Why RED on current code: try_upsert_infra_route has no guard that
 * prevents minting Route nodes from arbitrary CBM_STRREF_URL values in
 * config files.  Indexing the fixture below produces ≥ 2 Route nodes
 * (one per upstream URL string), so ASSERT_EQ(route_count, 0) FAILS.
 */

#include <foundation/compat.h>
#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <foundation/log.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

/* ── Minimal pipeline harness (mirrors test_grammar_probe_b.c) ───────────── */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} R521Proj;

static void r521_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\') *p = '/';
    }
}

typedef struct {
    const char *name;
    const char *content;
} R521File;

static cbm_store_t *r521_index_files(R521Proj *lp, const R521File *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_r521_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir)) return NULL;
    r521_fwd_slashes(lp->tmpdir);

    for (int i = 0; i < nfiles; i++) {
        char path[700];
        snprintf(path, sizeof(path), "%s/%s", lp->tmpdir, files[i].name);
        /* create any intermediate directories */
        char *slash = strrchr(path, '/');
        if (slash && slash > path + (int)strlen(lp->tmpdir)) {
            *slash = '\0';
            cbm_mkdir_p(path, 0755);
            *slash = '/';
        }
        FILE *f = fopen(path, "wb");
        if (!f) return NULL;
        fputs(files[i].content, f);
        fclose(f);
    }

    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project) return NULL;

    const char *home = getenv("HOME");
    if (!home) home = "/tmp";
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);

    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv) return NULL;

    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp) free(resp);

    return cbm_store_open_path(lp->dbpath);
}

static void r521_cleanup(R521Proj *lp, cbm_store_t *store) {
    if (store) cbm_store_close(store);
    if (lp->srv) { cbm_mcp_server_free(lp->srv); lp->srv = NULL; }
    free(lp->project); lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600], shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(wal); unlink(shm);
}

/* Count Route nodes in the indexed project. Returns -1 on error. */
static int r521_count_routes(cbm_store_t *store, const char *project) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, "Route", &nodes, &count) != CBM_STORE_OK)
        return -1;
    cbm_store_free_nodes(nodes, count);
    return count;
}

/* ── Reproduction test ───────────────────────────────────────────────────── */

/*
 * Fixture: a three-file repo containing ONLY config files.
 *
 *   config.yaml   — application config; values are upstream/external URLs
 *                   (auth server, downstream service).  No handler code.
 *   dependabot.yml — Dependabot config; "registries" block holds a Terraform
 *                    registry URL.  Purely a CI config — no route handlers.
 *   compose.yaml  — Docker Compose; "healthcheck" contains a curl command
 *                    with a localhost URL.  No route-serving code.
 *
 * All three files match is_infra_file() (.yaml / .yml).  Their URL strings
 * pass cbm_classify_string() as CBM_STRREF_URL.  On buggy code,
 * try_upsert_infra_route() mints a Route node for each URL string that
 * contains "://", so the graph gets ≥ 2 spurious Route nodes.
 *
 * Correct behaviour: 0 Route nodes (no route handler exists anywhere).
 * Actual (buggy):    ≥ 2 Route nodes — assertion below is RED.
 */
TEST(repro_issue521_no_route_from_config_url) {
    static const R521File files[] = {
        {
            "config.yaml",
            "auth:\n"
            "  jwks_url: \"https://auth.example.com/.well-known/jwks.json\"\n"
            "upstream:\n"
            "  order_service_url: \"http://order-service:8080/v2/orders/{id}\"\n"
        },
        {
            "dependabot.yml",
            "version: 2\n"
            "registries:\n"
            "  terraform-registry:\n"
            "    type: terraform-registry\n"
            "    url: https://app.terraform.io\n"
            "updates:\n"
            "  - package-ecosystem: terraform\n"
            "    directory: \"/\"\n"
            "    schedule:\n"
            "      interval: weekly\n"
        },
        {
            "compose.yaml",
            "services:\n"
            "  app:\n"
            "    image: myapp:latest\n"
            "    healthcheck:\n"
            "      test: [\"CMD-SHELL\", \"curl --fail http://localhost:9000/ || exit 1\"]\n"
            "      interval: 30s\n"
        },
    };

    R521Proj lp;
    cbm_store_t *store = r521_index_files(&lp, files, 3);
    ASSERT_NOT_NULL(store);

    int route_count = r521_count_routes(store, lp.project);

    /*
     * CORRECT behaviour: no Route node must exist.
     * Upstream/config/healthcheck URLs are not routes this service serves.
     *
     * WHY RED on current code:
     *   pipeline.c:try_upsert_infra_route() calls cbm_gbuf_upsert_node(…,"Route",…)
     *   for every CBM_STRREF_URL string_ref extracted from files matching
     *   is_infra_file() — which includes all three YAML files above.
     *   The function has no guard to reject upstream/config URL values, so
     *   it mints Route nodes for "https://auth.example.com/…", "https://app.terraform.io",
     *   "http://order-service:8080/…", and "http://localhost:9000/" — at
     *   least 2 spurious Route nodes, so route_count > 0, and this ASSERT_EQ
     *   FAILS (RED).
     */
    ASSERT_EQ(route_count, 0);

    r521_cleanup(&lp, store);
    PASS();
}

/* ── Suite ───────────────────────────────────────────────────────────────── */
SUITE(repro_issue521) {
    RUN_TEST(repro_issue521_no_route_from_config_url);
}
