/*
 * test_edge_types_probe.c — Reproduce-first probe of LESS-COMMON edge types.
 *
 * GREEN  = works; kept as regression guard.
 * RED    = found bug (edge not produced); kept as reproduction until fixed.
 *
 * Probed edge types (broadened from P6 in test_lang_contract.c):
 *   HANDLES    — route→handler across web frameworks (Express/Fastify TS, FastAPI/Django,
 *                Go net/http + Gin, Spring Java, ASP.NET C#, Laravel PHP, Rails Ruby,
 *                Actix/Axum Rust).
 *   HTTP_CALLS — outbound HTTP client call (fetch JS, axios TS, requests Python,
 *                net/http Go, HttpClient Java, RestSharp C#, HTTParty Ruby, Guzzle PHP,
 *                reqwest Rust).
 *   ASYNC_CALLS— queue/pubsub dispatch (Celery Python, Sidekiq Ruby, kafkajs TS,
 *                amazon-sqs-go Go, BullMQ JS).
 *   THROWS     — function throws/raises a checked exception (Java, Kotlin, Python,
 *                TypeScript, PHP, C#, Scala). Checked = no "Error"/"Panic" in name.
 *   RAISES     — function raises a runtime error/panic (Python, Kotlin, TypeScript,
 *                C#, PHP). Unchecked = name contains "Error"/"Panic".
 *   WRITES     — variable assignment resolved across function boundary (Python, Go, Rust,
 *                Java, C#, Kotlin). WRITES/READS are parallel-path only (>50 files).
 *   DEFINES_METHOD — class→method across languages (Go, Rust, Java, C#, PHP, Ruby,
 *                    Kotlin, TypeScript, Scala).
 *   OVERRIDE   — Go interface satisfaction (method overrides interface method).
 *                Parallel-path only (>50 files).
 *
 * Edge types that do NOT exist in the schema and are explicitly not probed:
 *   RETURNS / RETURNS_TYPE — no such edge type in the production schema (confirmed
 *     by searching ALL_EDGE_TYPES in test_lang_contract.c and the cli.c edge list).
 *   DATA_FLOWS — already guarded in test_lang_contract.c (contract_edge_data_flows).
 *
 * Path notes:
 *   SEQUENTIAL path (< 50 files): HANDLES, HTTP_CALLS, ASYNC_CALLS, DEFINES_METHOD,
 *     THROWS, RAISES are produced in pass_calls.c / pass_usages.c.
 *   PARALLEL path (≥ 50 files via index_parallel_fixture): WRITES, OVERRIDE and the
 *     parallel variants of THROWS/RAISES/USAGE are exercised here.
 *
 * Registration: NOT registered in test_main.c (per task specification).
 * Run standalone: link the suite and call suite_edge_types_probe() from a main().
 */

#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "resolver_quality.h"
#include "cbm.h"
#include "service_patterns.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <foundation/log.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

/* ── Fixture harness (mirrors test_lang_contract.c) ──────────────────────── */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} EtProj;

typedef struct {
    const char *name;
    const char *content;
} EtFile;

static void et_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\')
            *p = '/';
    }
}

/* Write files, then run index_repository and open the graph DB. */
static cbm_store_t *et_index_files(EtProj *lp, const EtFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_et_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir))
        return NULL;
    et_to_fwd_slashes(lp->tmpdir);

    for (int i = 0; i < nfiles; i++) {
        char path[700];
        snprintf(path, sizeof(path), "%s/%s", lp->tmpdir, files[i].name);
        char *slash = strrchr(path, '/');
        if (slash && slash > path + strlen(lp->tmpdir)) {
            *slash = '\0';
            cbm_mkdir_p(path, 0755);
            *slash = '/';
        }
        FILE *f = fopen(path, "wb");
        if (!f)
            return NULL;
        fputs(files[i].content, f);
        fclose(f);
    }

    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project)
        return NULL;

    const char *home = getenv("HOME");
    if (!home)
        home = "/tmp";
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);

    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv)
        return NULL;

    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp)
        free(resp);

    return cbm_store_open_path(lp->dbpath);
}

static void et_cleanup(EtProj *lp, cbm_store_t *store) {
    if (store)
        cbm_store_close(store);
    if (lp->srv) {
        cbm_mcp_server_free(lp->srv);
        lp->srv = NULL;
    }
    free(lp->project);
    lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600], shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(wal);
    unlink(shm);
}

/* Assert edge_type count >= floor; dump diagnostic on failure. */
static int et_edge_present(const EtFile *files, int nfiles, const char *edge, int floor) {
    EtProj lp;
    cbm_store_t *store = et_index_files(&lp, files, nfiles);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, edge) : -1;
    if (got < floor) {
        fprintf(stderr, "  [ET-EDGE] FAIL %-20s count=%d expected>=%d\n", edge, got, floor);
    }
    et_cleanup(&lp, store);
    return got >= floor;
}

enum { ET_ROUTE_ASSERT_MAX = 16 };

static cbm_store_t *et_index_parallel(EtProj *lp, const EtFile *meaningful, int n_mean);

/* Assert the exact Route node set. Edge-count smoke tests cannot catch partial
 * Spring paths such as "/orders" when the real route is "/api/orders", and a
 * presence-only assertion would still allow stale partial Route nodes to leak.
 * parallel pads the fixture past MIN_FILES_FOR_PARALLEL so the parallel
 * resolver mints the routes; must_qn (optional) is a Route qualified_name that
 * must exist -- the identity cross-repo HTTP matching joins on. */
static int et_routes_exact_mode(const EtFile *files, int nfiles, const char **routes, bool parallel,
                                const char *must_qn) {
    EtProj lp;
    cbm_store_t *store =
        parallel ? et_index_parallel(&lp, files, nfiles) : et_index_files(&lp, files, nfiles);
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    int wanted = 0;
    int found[ET_ROUTE_ASSERT_MAX] = {0};
    int ok = store != NULL;

    while (routes[wanted] && wanted < ET_ROUTE_ASSERT_MAX) {
        wanted++;
    }
    if (routes[wanted]) {
        ok = 0;
    }

    if (!store || cbm_store_find_nodes_by_label(store, lp.project, "Route", &nodes, &node_count) !=
                      CBM_STORE_OK) {
        ok = 0;
    } else {
        if (node_count != wanted) {
            ok = 0;
        }
        for (int wi = 0; wi < wanted; wi++) {
            for (int ni = 0; ni < node_count; ni++) {
                if (nodes[ni].name && strcmp(nodes[ni].name, routes[wi]) == 0) {
                    found[wi] = 1;
                    break;
                }
            }
            if (!found[wi]) {
                ok = 0;
            }
        }
    }

    if (!ok) {
        fprintf(stderr, "  [ET-ROUTE] FAIL expected=%d actual=%d missing:", wanted, node_count);
        for (int wi = 0; wi < wanted; wi++) {
            if (!found[wi]) {
                fprintf(stderr, " %s", routes[wi]);
            }
        }
        fprintf(stderr, " available:");
        for (int ni = 0; ni < node_count && ni < ET_ROUTE_ASSERT_MAX; ni++) {
            fprintf(stderr, " %s", nodes[ni].name ? nodes[ni].name : "<null>");
        }
        fprintf(stderr, "\n");
    }

    if (store && must_qn) {
        cbm_node_t qn_node;
        memset(&qn_node, 0, sizeof(qn_node));
        if (cbm_store_find_node_by_qn(store, lp.project, must_qn, &qn_node) != CBM_STORE_OK) {
            fprintf(stderr, "  [ET-ROUTE] FAIL missing qualified_name %s\n", must_qn);
            ok = 0;
        } else {
            cbm_node_free_fields(&qn_node);
        }
    }

    cbm_store_free_nodes(nodes, node_count);
    et_cleanup(&lp, store);
    return ok;
}

static int et_routes_exact(const EtFile *files, int nfiles, const char **routes) {
    return et_routes_exact_mode(files, nfiles, routes, false, NULL);
}

/* Index meaningful[] plus PARALLEL_PAD_FILES trivial pad files to force the
 * parallel pipeline path (MIN_FILES_FOR_PARALLEL = 50). */
enum { ET_PARALLEL_PAD = 52, ET_PAD_MAX = 68 /* 52 pad + 16 meaningful */ };

static cbm_store_t *et_index_parallel(EtProj *lp, const EtFile *meaningful, int n_mean) {
    static char pad_name[ET_PARALLEL_PAD][48];
    static char pad_body[ET_PARALLEL_PAD][64];
    EtFile files[ET_PAD_MAX] = {0};
    int n = 0;
    for (int i = 0; i < n_mean; i++)
        files[n++] = meaningful[i];
    for (int i = 0; i < ET_PARALLEL_PAD; i++) {
        snprintf(pad_name[i], sizeof(pad_name[i]), "pad/pad_%02d.py", i);
        snprintf(pad_body[i], sizeof(pad_body[i]), "def pad_%02d():\n    return %d\n", i, i);
        files[n].name = pad_name[i];
        files[n].content = pad_body[i];
        n++;
    }
    return et_index_files(lp, files, n);
}

/* #598: a call that resolves to a GraphQL client used to name its Route after
 * the WHOLE operation text (minus a leading "query "/"mutation "), so an
 * anonymous `query { ... }` became one long Route and even a named operation
 * carried its variables and selection set in the name. GraphQL Routes are now
 * keyed by operation name only; anonymous operations share one bounded key per
 * operation type. The Route set is asserted exactly (QN + name). Parallel path
 * only: GRAPHQL_CALLS is emitted by pass_parallel.c. */
typedef struct {
    const char *qn;
    const char *name;
} EtGqlRoute;

static int et_graphql_routes_exact(const EtFile *files, int nfiles, const EtGqlRoute *want,
                                   int nwant) {
    EtProj lp;
    cbm_store_t *store = et_index_parallel(&lp, files, nfiles);
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    int ok = store != NULL;
    int got = 0;
    if (!store || cbm_store_find_nodes_by_label(store, lp.project, "Route", &nodes, &node_count) !=
                      CBM_STORE_OK) {
        ok = 0;
    }
    for (int ni = 0; nodes && ni < node_count; ni++) {
        const char *qn = nodes[ni].qualified_name ? nodes[ni].qualified_name : "";
        const char *name = nodes[ni].name ? nodes[ni].name : "";
        if (strncmp(qn, "__graphql__", 11) != 0) {
            continue;
        }
        got++;
        int matched = 0;
        for (int wi = 0; wi < nwant; wi++) {
            if (strcmp(qn, want[wi].qn) == 0 && strcmp(name, want[wi].name) == 0) {
                matched = 1;
                break;
            }
        }
        /* Operation text (selection sets, variables) must never leak into a key. */
        if (!matched || strchr(qn, '{') || strchr(name, '{') || strlen(qn) > 160) {
            fprintf(stderr, "  [ET-GQL] unexpected Route qn='%s' name='%s'\n", qn, name);
            ok = 0;
        }
    }
    if (got != nwant) {
        fprintf(stderr, "  [ET-GQL] FAIL graphql routes expected=%d actual=%d\n", nwant, got);
        ok = 0;
    }
    int edges = store ? cbm_store_count_edges_by_type(store, lp.project, "GRAPHQL_CALLS") : -1;
    if (edges < nwant) {
        fprintf(stderr, "  [ET-GQL] FAIL GRAPHQL_CALLS=%d expected>=%d\n", edges, nwant);
        ok = 0;
    }
    cbm_store_free_nodes(nodes, node_count);
    et_cleanup(&lp, store);
    return ok;
}

TEST(graphql_route_keyed_by_operation_name_issue598) {
    static const EtFile f[] = {
        {"graphql/client.py", "def gql(query):\n    return query\n"},
        {"api/ops.py",
         "from graphql.client import gql\n\n\n"
         "def get_user():\n"
         "    return gql('query GetUser($id: ID!) { user(id: $id) { name } }')\n\n\n"
         "def update_user():\n"
         "    return gql('mutation UpdateUser($id: ID!) { updateUser(id: $id) { id } }')\n\n\n"
         "def list_users():\n"
         "    return gql('fragment U on User { id } query ListUsers { users { ...U } }')\n\n\n"
         "def on_event():\n"
         "    return gql('subscription OnEvent { event { id } }')\n\n\n"
         "def products():\n"
         "    return gql('query { products(search: \"x\") { items { sku } } }')\n\n\n"
         "def orders():\n"
         "    return gql('{ orders { id total } }')\n\n\n"
         "def logout():\n"
         "    return gql('mutation { logout }')\n"}};
    static const EtGqlRoute want[] = {
        {"__graphql__GetUser", "GetUser"},
        {"__graphql__UpdateUser", "UpdateUser"},
        {"__graphql__ListUsers", "ListUsers"},
        {"__graphql__OnEvent", "OnEvent"},
        /* products + orders: `query { }` and the `{ }` shorthand are both
         * anonymous queries and share one key. */
        {"__graphql__query__anonymous", "(anonymous query)"},
        {"__graphql__mutation__anonymous", "(anonymous mutation)"},
    };
    ASSERT_TRUE(et_graphql_routes_exact(f, (int)(sizeof(f) / sizeof(f[0])), want,
                                        (int)(sizeof(want) / sizeof(want[0]))));
    PASS();
}

/* #598 parser edges the end-to-end fixture cannot reach through one language's
 * string extraction: comments/strings/interpolations that contain braces, a
 * bare word, non-document arguments and name truncation. */
TEST(graphql_operation_identity_parser_issue598) {
    const char *type = NULL;
    char name[16];
    static const struct {
        const char *doc;
        int named;
        const char *type;
        const char *name;
    } cases[] = {
        {"# fetch { all }\nquery Q1 { a }", 1, "query", "Q1"},
        {"${UserFields} query WithFrag($id: ID) { u { ...UserFields } }", 1, "query", "WithFrag"},
        {"fragment F on T @dir(x: \"{\") { a } mutation M2 { b }", 1, "mutation", "M2"},
        {"\"\"\"doc { brace\"\"\" subscription S3 { c }", 1, "subscription", "S3"},
        {"  query  \n ( $x: Int ) { a }", 0, "query", ""},
        {"  { viewer { id } }", 0, "query", ""},
        {"query GetUserByIdentifier { a }", 1, "query", "GetUserByIdenti"}, /* bounded */
        {"data", 0, "operation", ""}, /* a bare word is not evidence of an operation */
        {"https://api.example.com/graphql", 0, "operation", ""},
        {"/graphql", 0, "operation", ""},
        {"", 0, "operation", ""},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        bool named = cbm_service_pattern_graphql_operation(cases[i].doc, &type, name, sizeof(name));
        if ((int)named != cases[i].named || strcmp(type, cases[i].type) != 0 ||
            strcmp(name, cases[i].name) != 0) {
            fprintf(stderr, "  [ET-GQL] case %zu '%s' -> named=%d type=%s name=%s\n", i,
                    cases[i].doc, (int)named, type, name);
            ASSERT_TRUE(0);
        }
    }
    ASSERT_FALSE(cbm_service_pattern_graphql_operation(NULL, &type, name, sizeof(name)));
    ASSERT_STR_EQ(type, "operation");
    PASS();
}

/* #1085: count CALLS edges whose target node has `name`, indexing via the
 * PARALLEL path (et_index_parallel pads to >50 files). The per-file TS resolver
 * used to treat a raw tsconfig alias as a confident target. That false row made
 * the parallel driver skip cross-file resolution, then failed graph target
 * materialization; the guarded JSX carrier correctly refused name-only
 * fallback, so the relationship vanished. Needs >50 files to reproduce. */
static int et_calls_to_name_parallel(const EtFile *meaningful, int n_mean, const char *name) {
    EtProj lp;
    cbm_store_t *store = et_index_parallel(&lp, meaningful, n_mean);
    int hits = 0;
    if (store) {
        cbm_edge_t *edges = NULL;
        int n = 0;
        if (cbm_store_find_edges_by_type(store, lp.project, "CALLS", &edges, &n) == CBM_STORE_OK) {
            for (int i = 0; i < n; i++) {
                cbm_node_t tgt;
                if (cbm_store_find_node_by_id(store, edges[i].target_id, &tgt) != CBM_STORE_OK)
                    continue;
                if (tgt.name && strcmp(tgt.name, name) == 0)
                    hits++;
                cbm_node_free_fields(&tgt);
            }
            cbm_store_free_edges(edges, n);
        }
    }
    et_cleanup(&lp, store);
    return hits;
}

static bool et_jsx_quality(const EtFile *files, int count, const RQ_Source *sources,
                           size_t source_count, const RQ_Expected *expected, size_t expected_count,
                           const RQ_Forbidden *forbidden, size_t forbidden_count, size_t required,
                           const char *name) {
    bool ok = true;
    for (int parallel = 0; parallel < 2; parallel++) {
        EtProj project;
        cbm_store_t *store = parallel ? et_index_parallel(&project, files, count)
                                      : et_index_files(&project, files, count);
        RQ_Report report = {0};
        char suite[128];
        snprintf(suite, sizeof(suite), "%s/%s", name, parallel ? "parallel" : "sequential");
        bool checked = store && rq_check_project_declarations(
                                    store, project.project, suite, sources, source_count, expected,
                                    expected_count, forbidden, forbidden_count, &report, stderr);
        ok = checked && report.genuine == expected_count && report.found == required &&
             report.missing == expected_count - required && ok;
        et_cleanup(&project, store);
    }
    return ok;
}
TEST(calls_jsx_component_via_tsconfig_alias_parallel_issue1085) {
    static const EtFile f[] = {
        {"tsconfig.json", "{ \"compilerOptions\": { \"baseUrl\": \".\", "
                          "\"paths\": { \"@/*\": [\"./src/*\"] } } }\n"},
        {"src/components/ui/kpi-card.tsx",
         "export function KpiCard({ label }: { label: string }) {\n"
         "  return <div>{label}</div>;\n}\n"},
        {"src/app/dashboard-a.tsx",
         "import { KpiCard } from \"@/components/ui/kpi-card\";\n"
         "export function DashboardA() {\n  return <KpiCard label=\"a\" />;\n}\n"},
        {"src/app/dashboard-b.tsx",
         "import { KpiCard } from \"@/components/ui/kpi-card\";\n"
         "export function DashboardB() {\n  return <KpiCard label=\"b\" />;\n}\n"}};
    /* Configuration is written as context, but has no indexed parser snapshot. */
    const RQ_Source sources[] = {{.path = f[1].name, .text = f[1].content},
                                 {.path = f[2].name, .text = f[2].content},
                                 {.path = f[3].name, .text = f[3].content}};
    const RQ_Expected expected[] = {
        {.case_id = "dashboard-a-exact-component",
         .site = {.file = "src/app/dashboard-a.tsx",
                  .caller = "DashboardA",
                  .needle = "<KpiCard label=\"a\" />"},
         .relation = "CALLS",
         .target = {.file = "src/components/ui/kpi-card.tsx",
                    .suffix = "KpiCard",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function KpiCard({ label }: { label: string }) {"},
         .mode = RQ_REQUIRED},
        {.case_id = "dashboard-b-exact-component",
         .site = {.file = "src/app/dashboard-b.tsx",
                  .caller = "DashboardB",
                  .needle = "<KpiCard label=\"b\" />"},
         .relation = "CALLS",
         .target = {.file = "src/components/ui/kpi-card.tsx",
                    .suffix = "KpiCard",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function KpiCard({ label }: { label: string }) {"},
         .mode = RQ_REQUIRED}};
    ASSERT_TRUE(et_jsx_quality(f, 4, sources, 3, expected, 2, NULL, 0, 2, "jsx_alias_1085"));
    /* RED before the fix: 0 (parallel drops alias-JSX). GREEN: exactly one
     * deduplicated edge from each dashboard render. */
    int hits = et_calls_to_name_parallel(f, 4, "KpiCard");
    if (hits != 2) {
        fprintf(stderr,
                "  [1085] FAIL CALLS->KpiCard on parallel path = %d (expected 2); "
                "alias-imported JSX component edges dropped\n",
                hits);
    }
    ASSERT_EQ(hits, 2);
    PASS();
}

/* Whole-tag sites retain exact source bindings; namesakes and mutable/local
 * shadows never borrow the imported component's declaration identity. */
TEST(calls_jsx_named_components_require_exact_binding) {
    static const EtFile files[] = {
        {"component.tsx", "export function Widget({ label }: { label: string }) {\n"
                          "  return <span>{label}</span>;\n}\n"},
        {"decoy.tsx", "export function Widget() { return <span />; }\n"},
        {"hidden.tsx", "function Hidden() { return <span />; }\nexport const marker = 1;\n"},
        {"positive.tsx", "import { Widget as Renamed } from './component';\n"
                         "export function ImportedView() { return <Renamed label=\"ok\" />; }\n"},
        {"local.tsx", "export function LocalWidget() { return <span />; }\n"
                      "export function LocalView() { return <LocalWidget></LocalWidget>; }\n"
                      "export function exactTarget() { return 7; }\n"
                      "export function exactCaller() { return exactTarget(); }\n"},
        {"parameter.tsx",
         "import { Widget } from './component';\n"
         "export function ParameterView(Widget: any) { return <Widget label=\"p\" />; }\n"},
        {"shadow.tsx", "import { Widget } from './component';\n"
                       "export function ShadowView(props: any) {\n"
                       "  const Widget = props.widget;\n  return <Widget label=\"s\" />;\n}\n"},
        {"conflict.tsx",
         "import { Widget } from './component';\nimport { Widget } from './decoy';\n"
         "export function ConflictView() { return <Widget label=\"c\" />; }\n"},
        {"private.tsx", "import { Hidden } from './hidden';\n"
                        "export function PrivateView() { return <Hidden />; }\n"},
        {"intrinsic.tsx", "import { Widget as widget } from './component';\n"
                          "export function IntrinsicView() { return <widget label=\"i\" />; }\n"},
        {"missing.tsx", "import { Widget } from '@missing/component';\n"
                        "export function MissingView() { return <Widget label=\"m\" />; }\n"},
        {"member.tsx", "import * as UI from './component';\n"
                       "export function MemberView() { return <UI.Widget label=\"q\" />; }\n"},
        {"namespace.tsx", "export function NamespaceView() { return <svg:Widget />; }\n"}};
    RQ_Source sources[13];
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++)
        sources[i] = (RQ_Source){.path = files[i].name, .text = files[i].content};
    const RQ_Expected expected[] = {
        {.case_id = "imported-renamed-component",
         .site = {.file = "positive.tsx",
                  .caller = "ImportedView",
                  .needle = "<Renamed label=\"ok\" />"},
         .relation = "CALLS",
         .target = {.file = "component.tsx",
                    .suffix = "component.Widget",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function Widget({ label }: { label: string }) {"},
         .mode = RQ_REQUIRED},
        {.case_id = "local-opening-tag-component",
         .site = {.file = "local.tsx", .caller = "LocalView", .needle = "<LocalWidget>"},
         .relation = "CALLS",
         .target = {.file = "local.tsx",
                    .suffix = "local.LocalWidget",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function LocalWidget() { return <span />; }"},
         .mode = RQ_REQUIRED},
        {.case_id = "ordinary-call-preserved",
         .site = {.file = "local.tsx",
                  .caller = "exactCaller",
                  .needle = "exactTarget()",
                  .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "local.tsx",
                    .suffix = "local.exactTarget",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text = "export function exactTarget() { return 7; }"},
         .mode = RQ_REQUIRED},
        {.case_id = "member-component-quality-gap",
         .site = {.file = "member.tsx",
                  .caller = "MemberView",
                  .needle = "<UI.Widget label=\"q\" />"},
         .relation = "CALLS",
         .target = {.file = "component.tsx",
                    .suffix = "component.Widget",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function Widget({ label }: { label: string }) {"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "jsx-namespace-member-binding-proof",
         .why = "The explicit namespace import and named export identify Widget, but this bridge "
                "proves only simple identifier JSX names. Member lookup requires a separate audit.",
         .tried = "Preserved the namespace member shape beside required imported/local simple "
                  "components and an unrelated namesake; both pipeline drivers use the exact "
                  "source-site oracle. No namespace member is admitted by this bounded bridge."}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "unrelated-namesake",
         .site = {.file = "positive.tsx",
                  .caller = "ImportedView",
                  .needle = "<Renamed label=\"ok\" />"},
         .relation = "CALLS",
         .target = {.file = "decoy.tsx",
                    .suffix = "decoy.Widget",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function Widget() { return <span />; }"}},
        {.case_id = "parameter-shadow",
         .site = {.file = "parameter.tsx",
                  .caller = "ParameterView",
                  .needle = "<Widget label=\"p\" />"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "local-value-shadow",
         .site = {.file = "shadow.tsx", .caller = "ShadowView", .needle = "<Widget label=\"s\" />"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "conflicting-imports",
         .site = {.file = "conflict.tsx",
                  .caller = "ConflictView",
                  .needle = "<Widget label=\"c\" />"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "unexported-component",
         .site = {.file = "private.tsx", .caller = "PrivateView", .needle = "<Hidden />"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "lowercase-intrinsic",
         .site = {.file = "intrinsic.tsx",
                  .caller = "IntrinsicView",
                  .needle = "<widget label=\"i\" />"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "missing-alias-root",
         .site = {.file = "missing.tsx",
                  .caller = "MissingView",
                  .needle = "<Widget label=\"m\" />"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "namespace-syntax",
         .site = {.file = "namespace.tsx", .caller = "NamespaceView", .needle = "<svg:Widget />"},
         .relation = "CALLS",
         .any_target = true},
        {.case_id = "member-unrelated-namesake",
         .site = {.file = "member.tsx",
                  .caller = "MemberView",
                  .needle = "<UI.Widget label=\"q\" />"},
         .relation = "CALLS",
         .target = {.file = "decoy.tsx",
                    .suffix = "decoy.Widget",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function Widget() { return <span />; }"}}};
    ASSERT_TRUE(
        et_jsx_quality(files, 13, sources, 13, expected, 4, forbidden, 9, 3, "jsx_exact_binding"));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  HANDLES — route→handler across web frameworks
 *
 *  Strategy: the route_path is extracted from decorators (Python Flask/FastAPI/
 *  Django, Java Spring) or from the resolved QN matching a framework library id
 *  (Express, Fastify, Gin, ASP.NET MapGet/MapPost, Laravel).  Each fixture uses a
 *  LOCAL wrapper whose QN carries the framework substring so the sequential-path
 *  resolver (pass_calls.c → cbm_service_pattern_match → CBM_SVC_ROUTE_REG) fires.
 *  Decorator-based frameworks (Flask, FastAPI, Django, Spring @RequestMapping) are
 *  handled via extract_route_from_decorators which sets def.route_path in the
 *  extraction result and creates Route+HANDLES during pass_definitions.
 * ══════════════════════════════════════════════════════════════════ */

/* Flask (Python) — already covered in test_lang_contract.c:contract_edge_handles.
 * Included here as baseline sanity guard for this file. */
TEST(handles_flask_python) {
    static const EtFile f[] = {
        {"app.py", "from flask import Flask\n\napp = Flask(__name__)\n\n\n"
                   "@app.route(\"/items\")\ndef list_items():\n    return {\"items\": []}\n\n\n"
                   "@app.route(\"/items/<int:item_id>\")\ndef get_item(item_id):\n    return "
                   "{\"id\": item_id}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 1));
    PASS();
}

/* FastAPI (Python) — @app.get / @app.post decorators */
TEST(handles_fastapi_python) {
    static const EtFile f[] = {
        {"api.py",
         "from fastapi import FastAPI\n\napp = FastAPI()\n\n\n"
         "@app.get(\"/users\")\ndef read_users():\n    return [{\"id\": 1}]\n\n\n"
         "@app.post(\"/users\")\ndef create_user(name: str):\n    return {\"name\": name}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 1));
    PASS();
}

/* DRF @action (Python) — issue #603. Before the try_drf_action_decorator
 * extractor, an @action-decorated ViewSet method carried no route_path, so
 * Phase 2a emitted 0 Route/HANDLES edges (this asserts >=1 → RED without the
 * fix, GREEN with it). Direction-agnostic count via cbm_store_count_edges_by_type. */
TEST(handles_drf_action_python) {
    static const EtFile f[] = {{"viewsets.py",
                                "from rest_framework.decorators import action\n"
                                "from rest_framework.viewsets import ViewSet\n\n"
                                "class CustomerTaskViewSet(ViewSet):\n"
                                "    @action(detail=True, methods=[\"post\"])\n"
                                "    def approve_draft_with_charge(self, request, pk=None):\n"
                                "        pass\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 1));
    PASS();
}

/* Express (JS/TS) — route registration must resolve to a callee QN containing
 * the "express" library substring AND pass the handler as an identifier (not an
 * inline-object method, which is never registered as a resolvable node).  We use
 * top-level wrapper functions defined in an "express"-pathed module so the
 * resolved QN (project.express.router.expressGet) carries the substring → the
 * sequential resolver classifies the call as CBM_SVC_ROUTE_REG and emits
 * Route + HANDLES (mirrors the working handles_gin_go pattern). */
TEST(handles_express_ts) {
    static const EtFile f[] = {
        {"express/router.ts",
         "export function expressGet(p: string, h: any): any { return h; }\n"
         "export function expressPost(p: string, h: any): any { return h; }\n"},
        {"users.ts", "import { expressGet, expressPost } from './express/router';\n\n"
                     "function listUsers(req: any, res: any) {\n    res.json([]);\n}\n\n"
                     "function createUser(req: any, res: any) {\n    res.json({});\n}\n\n"
                     "expressGet('/users', listUsers);\n"
                     "expressPost('/users', createUser);\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "HANDLES", 1));
    PASS();
}

/* Fastify (JS) — same as Express: route registration via top-level wrapper
 * functions whose resolved QN carries the "fastify" substring + identifier
 * handlers.  Inline-object methods (the previous fixture) are never registered,
 * so router.get could not resolve and no HANDLES fired. */
TEST(handles_fastify_js) {
    static const EtFile f[] = {
        {"fastify/server.js",
         "function fastifyGet(p, h) { return h; }\n"
         "function fastifyPost(p, h) { return h; }\n\n"
         "function getHealth(req, reply) { reply.send({ ok: true }); }\n\n"
         "function postOrder(req, reply) { reply.send({ created: true }); }\n\n"
         "fastifyGet('/health', getHealth);\n"
         "fastifyPost('/orders', postOrder);\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 1));
    PASS();
}

/* Go net/http + gin — local gin.Engine wrapper whose QN contains "gin." */
TEST(handles_gin_go) {
    static const EtFile f[] = {{"gin/engine.go",
                                "package gin\n\n"
                                "type Engine struct{}\n\n"
                                "func Default() *Engine {\n    return &Engine{}\n}\n\n"
                                "func (e *Engine) GET(path string, handler interface{}) {}\n"
                                "func (e *Engine) POST(path string, handler interface{}) {}\n"},
                               {"main.go", "package main\n\n"
                                           "import \"gin\"\n\n"
                                           "func listOrders(w interface{}, r interface{}) {}\n\n"
                                           "func createOrder(w interface{}, r interface{}) {}\n\n"
                                           "func main() {\n"
                                           "    r := gin.Default()\n"
                                           "    r.GET(\"/orders\", listOrders)\n"
                                           "    r.POST(\"/orders\", createOrder)\n"
                                           "}\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "HANDLES", 1));
    PASS();
}

/* #686: Go router groups. Fiber (and Gin/Echo, same syntax) build the external
 * path from `grp := app.Group("/admin")` plus `grp.Post("/x")`, so the full
 * path is never one literal at the registration site. The Route must carry
 * the composed path, including nested groups (`v1 := api.Group("/v1")`) and
 * groups built inline (`app.Group("/x").Get(...)`). Controls: a route on the
 * root app keeps its own path, a variable later re-bound to a different group
 * uses the binding in effect at the call, and a non-literal group prefix
 * leaves the route unprefixed (never guessed). Exact Route set on BOTH the
 * sequential and the parallel pipeline. */
static const EtFile et_fiber_groups_issue686[] = {
    {"main.go", "package main\n\n"
                "import \"github.com/gofiber/fiber/v2\"\n\n"
                "func health(c *fiber.Ctx) error { return nil }\n"
                "func getUser(c *fiber.Ctx) error { return nil }\n"
                "func updateCustomer(c *fiber.Ctx) error { return nil }\n"
                "func listOrders(c *fiber.Ctx) error { return nil }\n"
                "func ping(c *fiber.Ctx) error { return nil }\n"
                "func report(c *fiber.Ctx) error { return nil }\n"
                "func dyn(c *fiber.Ctx) error { return nil }\n\n"
                "func main() {\n"
                "    app := fiber.New()\n"
                "    app.Get(\"/health\", health)\n"
                "    api := app.Group(\"/api\")\n"
                "    v1 := api.Group(\"/v1\", authMiddleware)\n"
                "    v1.Get(\"/users/:id\", getUser)\n"
                "    admin := app.Group(\"/admin\")\n"
                "    admin.Post(\"/customers/:id\", updateCustomer)\n"
                "    grp := api.Group(\"/shop\")\n"
                "    grp.Get(\"/orders\", listOrders)\n"
                "    grp = app.Group(\"/internal\")\n"
                "    grp.Get(\"/ping\", ping)\n"
                "    app.Group(\"/reports\").Get(\"/daily\", report)\n"
                "    prefix := \"/p\"\n"
                "    dg := app.Group(prefix)\n"
                "    dg.Get(\"/dyn\", dyn)\n"
                "    app.Listen(\":3000\")\n"
                "}\n\n"
                "func authMiddleware(c *fiber.Ctx) error { return c.Next() }\n"}};

static const char *et_fiber_groups_issue686_routes[] = {"/health",
                                                        "/api/v1/users/:id",
                                                        "/admin/customers/:id",
                                                        "/api/shop/orders",
                                                        "/internal/ping",
                                                        "/reports/daily",
                                                        "/dyn",
                                                        NULL};

TEST(handles_fiber_group_prefix_sequential_issue686) {
    ASSERT_TRUE(et_routes_exact_mode(et_fiber_groups_issue686, 1, et_fiber_groups_issue686_routes,
                                     false, NULL));
    PASS();
}

TEST(handles_fiber_group_prefix_parallel_issue686) {
    ASSERT_TRUE(et_routes_exact_mode(et_fiber_groups_issue686, 1, et_fiber_groups_issue686_routes,
                                     true, NULL));
    PASS();
}

/* #686 control for the same mechanism on Gin: `v1 := r.Group("/v1")` with the
 * idiomatic brace block. */
TEST(handles_gin_group_prefix_issue686) {
    static const EtFile f[] = {{"main.go", "package main\n\n"
                                           "import \"github.com/gin-gonic/gin\"\n\n"
                                           "func listOrders(c *gin.Context) {}\n"
                                           "func createOrder(c *gin.Context) {}\n\n"
                                           "func main() {\n"
                                           "    r := gin.Default()\n"
                                           "    v1 := r.Group(\"/v1\")\n"
                                           "    {\n"
                                           "        v1.GET(\"/orders\", listOrders)\n"
                                           "        v1.POST(\"/orders\", createOrder)\n"
                                           "    }\n"
                                           "}\n"}};
    static const char *routes[] = {"/v1/orders", "/v1/orders", NULL}; /* GET + POST */
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* Spring (Java) — class-level @RequestMapping must prefix method mappings.
 * Reproduce-first: a HANDLES count alone can pass with partial routes
 * ("/orders"), but callers/search_graph need the actual endpoint names
 * ("/api/orders"). */
TEST(handles_spring_java) {
    static const char *routes[] = {"/api/orders", "/api/orders/{id}", NULL};
    static const EtFile f[] = {{"OrderController.java",
                                "package com.example;\n\n"
                                "import org.springframework.web.bind.annotation.RequestMapping;\n"
                                "import org.springframework.web.bind.annotation.GetMapping;\n\n"
                                "@RequestMapping(\"/api\")\npublic class OrderController {\n"
                                "    @GetMapping(\"/orders\")\n"
                                "    public String listOrders() {\n"
                                "        return \"orders\";\n    }\n\n"
                                "    @GetMapping(\"/orders/{id}\")\n"
                                "    public String getOrder(int id) {\n"
                                "        return \"order:\" + id;\n    }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 2));
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* Spring (Java) — the path attribute may sit anywhere in the annotation.
 * Java puts no order on annotation attributes, so `path` after `name`,
 * `produces` and `consumes` is ordinary source. The argument scan stopped
 * after the third attribute, so the path was never read and no Route node
 * formed. A HANDLES count alone cannot catch that, because the class-level
 * @RequestMapping still produces one route on its own. */
TEST(handles_spring_java_path_attribute_fourth) {
    static const char *routes[] = {"/api/orders", NULL};
    static const EtFile f[] = {{"OrderController.java",
                                "package com.example;\n\n"
                                "import org.springframework.web.bind.annotation.RequestMapping;\n"
                                "import org.springframework.web.bind.annotation.GetMapping;\n\n"
                                "@RequestMapping(\"/api\")\npublic class OrderController {\n"
                                "    @GetMapping(name = \"listOrders\",\n"
                                "                produces = \"application/json\",\n"
                                "                consumes = \"application/json\",\n"
                                "                path = \"/orders\")\n"
                                "    public String listOrders() {\n"
                                "        return \"orders\";\n    }\n}\n"}};
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* Spring (Kotlin) — same prefix contract, including Kotlin's named array form
 * for class-level RequestMapping values. */
TEST(handles_spring_kotlin) {
    static const char *routes[] = {"/internal/v1/api/orders", "/internal/v1/api/orders/{id}", NULL};
    static const EtFile f[] = {
        {"OrderController.kt",
         "package com.example\n\n"
         "import org.springframework.web.bind.annotation.RequestMapping\n"
         "import org.springframework.web.bind.annotation.GetMapping\n\n"
         "@RequestMapping(value = [\"/internal/v1/api\"])\nclass OrderController {\n"
         "    @GetMapping(\"/orders\")\n"
         "    fun listOrders(): String {\n"
         "        return \"orders\"\n    }\n\n"
         "    @GetMapping(\"/orders/{id}\")\n"
         "    fun getOrder(id: Int): String {\n"
         "        return \"order:\" + id\n    }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 2));
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* JAX-RS (Java): the verb (@GET) and the path (@Path) are sibling
 * annotations, and the class-level @Path must prefix method-level paths.
 * Reproduce-first (#1005): the first-mapping-annotation scan dropped every
 * method-level @Path, collapsing all methods onto a single "/" Route node;
 * only the exact Route-name set catches that through the emission dedup. */
TEST(handles_jaxrs_java) {
    static const char *routes[] = {"/api/v1/widgets", "/api/v1/widgets/count", NULL};
    static const EtFile f[] = {{"WidgetResource.java",
                                "package com.example;\n\n"
                                "import jakarta.ws.rs.GET;\n"
                                "import jakarta.ws.rs.Path;\n\n"
                                "@Path(\"/api/v1/widgets\")\npublic class WidgetResource {\n"
                                "    @GET\n"
                                "    public String list() {\n"
                                "        return \"widgets\";\n    }\n\n"
                                "    @GET\n"
                                "    @Path(\"/count\")\n"
                                "    public String count() {\n"
                                "        return \"42\";\n    }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 2));
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* JAX-RS (Java): @Path values are URI templates relative to the enclosing
 * resource, and the leading slash is optional. The slash-less spelling must
 * produce the same rooted Route set as the fixture above. */
TEST(handles_jaxrs_java_relative_path) {
    static const char *routes[] = {"/api/v1/widgets", "/api/v1/widgets/count", NULL};
    static const EtFile f[] = {{"WidgetResource.java",
                                "package com.example;\n\n"
                                "import jakarta.ws.rs.GET;\n"
                                "import jakarta.ws.rs.Path;\n\n"
                                "@Path(\"api/v1/widgets\")\npublic class WidgetResource {\n"
                                "    @GET\n"
                                "    public String list() {\n"
                                "        return \"widgets\";\n    }\n\n"
                                "    @GET\n"
                                "    @Path(\"count\")\n"
                                "    public String count() {\n"
                                "        return \"42\";\n    }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 2));
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* Negative control: the relative-template acceptance is scoped to JAX-RS
 * @Path. A slash-less string on a Spring mapping annotation is still not read
 * as a route path, so the handler keeps the "/" fallback and no "/api/orders"
 * Route node appears. */
TEST(handles_spring_java_relative_string_not_route) {
    static const char *routes[] = {"/", NULL};
    static const EtFile f[] = {{"OrderController.java",
                                "package com.example;\n\n"
                                "import org.springframework.web.bind.annotation.RequestMapping;\n"
                                "import org.springframework.web.bind.annotation.GetMapping;\n\n"
                                "@RequestMapping(\"api\")\npublic class OrderController {\n"
                                "    @GetMapping(\"orders\")\n"
                                "    public String listOrders() {\n"
                                "        return \"orders\";\n    }\n}\n"}};
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* JAX-RS (Scala): class-level @Path must be composed with both an empty
 * method path and a non-empty one.  Without the Scala branch in
 * push_method_def, unrelated resources collapse onto the global verb/root
 * Route node. */
TEST(handles_jaxrs_scala) {
    static const char *routes[] = {"/billingAccount", "/billingAccount/{id}",
                                   "/billingAccount/{id}/attachment/{attachmentId}/content", NULL};
    static const EtFile f[] = {{"BillingAccountApiController.scala",
                                "package com.example\n\n"
                                "import jakarta.ws.rs.{GET, POST, Path}\n\n"
                                "@Path(\"/billingAccount\")\nclass BillingAccountApiController {\n"
                                "  @POST\n  @Path(\"\")\n"
                                "  def createBillingAccount(): String = \"created\"\n\n"
                                "  @GET\n  @Path(\"{id}\")\n"
                                "  def retrieveBillingAccount(): String = \"account\"\n\n"
                                "  @GET\n  @Path(\"{id}/attachment/{attachmentId}/content\")\n"
                                "  def retrieveAttachment(): String = \"attachment\"\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 3));
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* Service relations have their own exact route/source oracle: they are not
 * among the shared resolver's seven admitted relations. Named downstream gaps
 * must be promoted when recovered; an unexpected service target is a bug. */
typedef struct {
    const char *case_id;
    RQ_Site site;
    RQ_Target source;
    const char *relation, *route, *route_qn, *property, *value;
    bool required;
    const char *gap_id, *why, *tried;
} EtServiceGolden;

static bool et_service_declaration(cbm_store_t *store, const char *project,
                                   const RQ_Source *sources, size_t source_count,
                                   const RQ_Target *target) {
    if (!rq_target_anchor(target, sources, source_count))
        return false;
    sqlite3_stmt *query = NULL;
    const char *sql = "SELECT file_path,qualified_name,name,label,start_line,end_line FROM nodes "
                      "WHERE project=?1 AND file_path=?2";
    bool ok = sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &query, NULL) == SQLITE_OK &&
              sqlite3_bind_text(query, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
              sqlite3_bind_text(query, 2, target->file, -1, SQLITE_TRANSIENT) == SQLITE_OK;
    int matches = 0, step = SQLITE_ERROR;
    if (ok)
        while ((step = sqlite3_step(query)) == SQLITE_ROW)
            if (rq_target(target, (const char *)sqlite3_column_text(query, 0),
                          (const char *)sqlite3_column_text(query, 1),
                          (const char *)sqlite3_column_text(query, 2),
                          (const char *)sqlite3_column_text(query, 3), sqlite3_column_int(query, 4),
                          sqlite3_column_int(query, 5)))
                matches++;
    ok = ok && step == SQLITE_DONE && matches == 1;
    if (query)
        sqlite3_finalize(query);
    if (!ok)
        fprintf(stderr, "[SERVICE-DECLARATION] missing/ambiguous %s:%s at %d\n", target->file,
                target->suffix, target->declaration_line);
    return ok;
}

static bool et_service_goldens(cbm_store_t *store, const char *project, const char *suite,
                               const RQ_Source *sources, size_t source_count,
                               const EtServiceGolden *want, size_t count) {
    if (!store || count > ET_ROUTE_ASSERT_MAX)
        return false;
    bool ok = true;
    unsigned found[ET_ROUTE_ASSERT_MAX] = {0};
    for (size_t i = 0; i < count; i++) {
        RQ_State anchor = {.site = &want[i].site};
        ok = rq_anchor(&anchor, sources, source_count) &&
             et_service_declaration(store, project, sources, source_count, &want[i].source) &&
             (want[i].required ||
              (rq_text(want[i].gap_id) && rq_text(want[i].why) && rq_text(want[i].tried))) &&
             ok;
    }
    sqlite3_stmt *query = NULL;
    const char *sql =
        "SELECT s.file_path,s.qualified_name,s.name,s.label,s.start_line,s.end_line,"
        "e.type,t.label,t.name,t.qualified_name,"
        "COALESCE(json_extract(e.properties,'$.method'),json_extract(t.properties,'$.method')),"
        "COALESCE(json_extract(e.properties,'$.broker'),json_extract(t.properties,'$.broker')) "
        "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
        "WHERE e.project=?1 AND e.type IN ('HANDLES','HTTP_CALLS','ASYNC_CALLS')";
    bool queried =
        sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &query, NULL) == SQLITE_OK &&
        sqlite3_bind_text(query, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK;
    size_t wrong = 0;
    int step = SQLITE_ERROR;
    if (queried)
        while ((step = sqlite3_step(query)) == SQLITE_ROW) {
            size_t matches = 0;
            for (size_t i = 0; i < count; i++) {
                const EtServiceGolden *g = want + i;
                int property = !strcmp(g->property, "method") ? 10 : 11;
                if (rq_target(&g->source, (const char *)sqlite3_column_text(query, 0),
                              (const char *)sqlite3_column_text(query, 1),
                              (const char *)sqlite3_column_text(query, 2),
                              (const char *)sqlite3_column_text(query, 3),
                              sqlite3_column_int(query, 4), sqlite3_column_int(query, 5)) &&
                    rq_equal((const char *)sqlite3_column_text(query, 6), g->relation) &&
                    rq_equal((const char *)sqlite3_column_text(query, 7), "Route") &&
                    rq_equal((const char *)sqlite3_column_text(query, 8), g->route) &&
                    rq_equal((const char *)sqlite3_column_text(query, 9), g->route_qn) &&
                    rq_equal((const char *)sqlite3_column_text(query, property), g->value)) {
                    found[i]++;
                    matches++;
                }
            }
            if (matches != 1)
                wrong++;
        }
    ok = queried && step == SQLITE_DONE && !wrong && ok;
    if (query)
        sqlite3_finalize(query);
    size_t hits = 0, missing = 0, recovered = 0, required_missing = 0;
    for (size_t i = 0; i < count; i++) {
        hits += found[i] != 0;
        missing += found[i] == 0;
        recovered += found[i] && !want[i].required;
        required_missing += !found[i] && want[i].required;
        ok = found[i] == (want[i].required ? 1u : 0u) && ok;
        if (!found[i])
            fprintf(stderr, "[SERVICE-QUALITY-MISS] %s %s %s\n", suite, want[i].case_id,
                    want[i].required ? "required" : want[i].gap_id);
    }
    fprintf(stderr,
            "[SERVICE-QUALITY] %s genuine=%zu found=%zu missing=%zu wrong=%zu "
            "required_missing=%zu recovered_gaps=%zu\n",
            suite, count, hits, missing, wrong, required_missing, recovered);
    return ok;
}

static bool et_service_quality(const EtFile *files, int count, int config_count,
                               const RQ_Expected *expected, size_t expected_count,
                               const RQ_Forbidden *forbidden, size_t forbidden_count,
                               const EtServiceGolden *service, size_t service_count,
                               const RQ_Target *survive, size_t survive_count, const char *suite,
                               bool stall) {
    if (count < 1 || count > 16 || config_count < 0 || config_count >= count)
        return false;
    RQ_Source sources[16];
    size_t source_count = (size_t)(count - config_count);
    for (size_t i = 0; i < source_count; i++)
        sources[i] = (RQ_Source){.path = files[i].name, .text = files[i].content};
    bool ok = true;
    for (int driver = 0; driver < 2; driver++) {
        EtProj project;
        /* Both celery/app.py and celery/quality_control.py are stalled. The
         * required importer call depends on the latter; original class/method
         * declarations must survive independently. Reset before any assertion. */
        if (stall)
            cbm_setenv("CBM_TEST_WALL_STALL_ON", "celery/", 1);
        cbm_store_t *store = driver ? et_index_parallel(&project, files, count)
                                    : et_index_files(&project, files, count);
        if (stall)
            cbm_unsetenv("CBM_TEST_WALL_STALL_ON");
        char label[128];
        snprintf(label, sizeof(label), "%s/%s", suite, driver ? "parallel" : "sequential");
        RQ_Report report = {0};
        bool calls = store && rq_check_project_declarations(
                                  store, project.project, label, sources, source_count, expected,
                                  expected_count, forbidden, forbidden_count, &report, stderr);
        ok = calls && report.genuine == expected_count &&
             report.found + report.missing == expected_count && ok;
        bool services = store && et_service_goldens(store, project.project, label, sources,
                                                    source_count, service, service_count);
        ok = services && ok;
        for (size_t i = 0; store && i < survive_count; i++)
            ok = et_service_declaration(store, project.project, sources, source_count,
                                        survive + i) &&
                 ok;
        et_cleanup(&project, store);
    }
    return ok;
}

/* The original minimal-API delegate registrations remain exact named gaps.
 * An explicit project supplies one assembly; attribute routing is independently
 * required and never substitutes for either original method-group target. */
TEST(handles_aspnet_csharp) {
    static const EtFile f[] = {
        {"Microsoft/AspNetCore/Builder.cs", "namespace Microsoft.AspNetCore {\n"
                                            "    class WebApp {\n"
                                            "        public static string MapGet(string path, "
                                            "System.Func<string> handler) { return handler(); }\n"
                                            "        public static string MapPost(string path, "
                                            "System.Func<string> handler) { return handler(); }\n"
                                            "    }\n}\n"},
        {"Program.cs", "using Microsoft.AspNetCore;\n\n"
                       "namespace App {\n"
                       "    class Program {\n"
                       "        static void Main() {\n"
                       "            WebApp.MapGet(\"/products\", GetProducts);\n"
                       "            WebApp.MapPost(\"/products\", CreateProduct);\n        }\n"
                       "        static string GetProducts() { return \"[]\"; }\n"
                       "        static string CreateProduct() { return \"{}\" ; }\n    }\n}\n"}};

    const EtFile files[] = {
        f[0],
        f[1],
        {"quality/control.mjs",
         "export function target() { return 7; }\nexport function caller() { return target(); }\n"},
        {"quality/Controller.cs",
         "using Microsoft.AspNetCore.Mvc;\nnamespace Quality {\n    [ApiController]\n    public "
         "class QualityController : ControllerBase {\n        [HttpGet(\"/quality/handlers\")]\n   "
         "     public string Ready() { return \"ok\"; }\n    }\n}\n"},
        {"quality/Foreign.cs",
         "namespace Foreign {\n    public class WebApp {\n        public static string "
         "MapGet(string path, System.Func<string> handler) { return handler(); }\n        public "
         "static string MapPost(string path, System.Func<string> handler) { return handler(); }\n  "
         "  }\n    public class Handlers {\n        public static string GetProducts() { return "
         "\"foreign\"; }\n        public static string CreateProduct() { return \"foreign\"; }\n   "
         " }\n}\n"},
        {"quality.csproj",
         "<Project Sdk=\"Microsoft.NET.Sdk.Web\">\n  "
         "<PropertyGroup><TargetFramework>net8.0</TargetFramework></PropertyGroup>\n</Project>\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "aspnet-map-get",
         .site = {.file = "Program.cs",
                  .caller = "Program.Main",
                  .needle = "WebApp.MapGet(\"/products\", GetProducts)"},
         .relation = "CALLS",
         .target = {.file = "Microsoft/AspNetCore/Builder.cs",
                    .suffix = "WebApp.MapGet",
                    .label = "Method",
                    .declaration_line = 3,
                    .declaration_text = "        public static string MapGet(string path, "
                                        "System.Func<string> handler) { return handler(); }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "csharp-delegate-registration",
         .why = "System.Func<string> method-group applicability and string parameters are outside "
                "the primitive C# proof subset, even with explicit assembly membership.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "aspnet-handler-get",
         .site = {.file = "Program.cs", .caller = "Program.Main", .needle = "GetProducts"},
         .relation = "CALL_REFERENCE",
         .target = {.file = "Program.cs",
                    .suffix = "Program.GetProducts",
                    .label = "Method",
                    .declaration_line = 9,
                    .declaration_text = "        static string GetProducts() { return \"[]\"; }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "csharp-method-group-reference",
         .why = "System.Func<string> method-group applicability and string parameters are outside "
                "the primitive C# proof subset, even with explicit assembly membership.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "aspnet-map-post",
         .site = {.file = "Program.cs",
                  .caller = "Program.Main",
                  .needle = "WebApp.MapPost(\"/products\", CreateProduct)"},
         .relation = "CALLS",
         .target = {.file = "Microsoft/AspNetCore/Builder.cs",
                    .suffix = "WebApp.MapPost",
                    .label = "Method",
                    .declaration_line = 4,
                    .declaration_text = "        public static string MapPost(string path, "
                                        "System.Func<string> handler) { return handler(); }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "csharp-delegate-registration",
         .why = "System.Func<string> method-group applicability and string parameters are outside "
                "the primitive C# proof subset, even with explicit assembly membership.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "aspnet-handler-post",
         .site = {.file = "Program.cs", .caller = "Program.Main", .needle = "CreateProduct"},
         .relation = "CALL_REFERENCE",
         .target = {.file = "Program.cs",
                    .suffix = "Program.CreateProduct",
                    .label = "Method",
                    .declaration_line = 10,
                    .declaration_text =
                        "        static string CreateProduct() { return \"{}\" ; }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "csharp-method-group-reference",
         .why = "System.Func<string> method-group applicability and string parameters are outside "
                "the primitive C# proof subset, even with explicit assembly membership.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "required-exact-call",
         .site = {.file = "quality/control.mjs",
                  .caller = "caller",
                  .needle = "target()",
                  .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "quality/control.mjs",
                    .suffix = "target",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function target() { return 7; }"},
         .mode = RQ_REQUIRED}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "aspnet-foreign-map-get",
         .site = {.file = "Program.cs",
                  .caller = "Program.Main",
                  .needle = "WebApp.MapGet(\"/products\", GetProducts)"},
         .relation = "CALLS",
         .target = {.file = "quality/Foreign.cs",
                    .suffix = "WebApp.MapGet",
                    .label = "Method",
                    .declaration_line = 3,
                    .declaration_text = "        public static string MapGet(string path, "
                                        "System.Func<string> handler) { return handler(); }"}},
        {.case_id = "aspnet-foreign-handler-get",
         .site = {.file = "Program.cs", .caller = "Program.Main", .needle = "GetProducts"},
         .relation = "CALL_REFERENCE",
         .target = {.file = "quality/Foreign.cs",
                    .suffix = "Handlers.GetProducts",
                    .label = "Method",
                    .declaration_line = 7,
                    .declaration_text =
                        "        public static string GetProducts() { return \"foreign\"; }"}},
        {.case_id = "aspnet-foreign-map-post",
         .site = {.file = "Program.cs",
                  .caller = "Program.Main",
                  .needle = "WebApp.MapPost(\"/products\", CreateProduct)"},
         .relation = "CALLS",
         .target = {.file = "quality/Foreign.cs",
                    .suffix = "WebApp.MapPost",
                    .label = "Method",
                    .declaration_line = 4,
                    .declaration_text = "        public static string MapPost(string path, "
                                        "System.Func<string> handler) { return handler(); }"}},
        {.case_id = "aspnet-foreign-handler-post",
         .site = {.file = "Program.cs", .caller = "Program.Main", .needle = "CreateProduct"},
         .relation = "CALL_REFERENCE",
         .target = {.file = "quality/Foreign.cs",
                    .suffix = "Handlers.CreateProduct",
                    .label = "Method",
                    .declaration_line = 8,
                    .declaration_text =
                        "        public static string CreateProduct() { return \"foreign\"; }"}}};
    const EtServiceGolden service[] = {
        {.case_id = "aspnet-handler-route-get",
         .site = {.file = "Program.cs",
                  .caller = "Program.Main",
                  .needle = "WebApp.MapGet(\"/products\", GetProducts)"},
         .source = {.file = "Program.cs",
                    .suffix = "Program.GetProducts",
                    .label = "Method",
                    .declaration_line = 9,
                    .declaration_text = "        static string GetProducts() { return \"[]\"; }"},
         .relation = "HANDLES",
         .route = "/products",
         .route_qn = "__route__GET__/products",
         .property = "method",
         .value = "GET",
         .required = false,
         .gap_id = "aspnet-method-group-route",
         .why = "The route registration cannot borrow an unproved containing-class method group.",
         .tried = "Call binding prerequisites audited in current source; exact downstream route "
                  "and handler identity retained separately from the seven-relation oracle."},
        {.case_id = "aspnet-handler-route-post",
         .site = {.file = "Program.cs",
                  .caller = "Program.Main",
                  .needle = "WebApp.MapPost(\"/products\", CreateProduct)"},
         .source = {.file = "Program.cs",
                    .suffix = "Program.CreateProduct",
                    .label = "Method",
                    .declaration_line = 10,
                    .declaration_text =
                        "        static string CreateProduct() { return \"{}\" ; }"},
         .relation = "HANDLES",
         .route = "/products",
         .route_qn = "__route__POST__/products",
         .property = "method",
         .value = "POST",
         .required = false,
         .gap_id = "aspnet-method-group-route",
         .why = "The route registration cannot borrow an unproved containing-class method group.",
         .tried = "Call binding prerequisites audited in current source; exact downstream route "
                  "and handler identity retained separately from the seven-relation oracle."},
        {.case_id = "required-attribute-handler",
         .site = {.file = "quality/Controller.cs",
                  .caller = "QualityController.Ready",
                  .needle = "[HttpGet(\"/quality/handlers\")]"},
         .source = {.file = "quality/Controller.cs",
                    .suffix = "QualityController.Ready",
                    .label = "Method",
                    .declaration_line = 6,
                    .declaration_text = "        public string Ready() { return \"ok\"; }"},
         .relation = "HANDLES",
         .route = "/quality/handlers",
         .route_qn = "__route__GET__/quality/handlers",
         .property = "method",
         .value = "GET",
         .required = true}};
    ASSERT_TRUE(et_service_quality(files, 6, 1, expected, 5, forbidden, 4, service, 3, NULL, 0,
                                   "handles_aspnet_csharp", false));
    PASS();
}

/* ASP.NET Core attribute routing (C#, #1692) — the reporter's controller shape:
 * stacked class attributes with a relative [Route("api/[controller]")] prefix,
 * verb attributes with and without a template, and a verb + separate method
 * [Route]. Every action got 0 Route nodes: the route extractor knew no
 * HttpGet/HttpPost/Route attribute, and relative templates were dropped for lacking a
 * leading '/'. The exact Route-name set also pins the [controller] token
 * (class name minus "Controller") and the class-prefix join. */
TEST(handles_aspnet_attribute_routes_issue1692) {
    static const char *routes[] = {"/api/Quotes/{symbol}", "/api/Quotes/orders/{id}/confirm",
                                   "/api/Quotes/x/{id}", NULL};
    static const EtFile f[] = {
        {"Controllers/QuotesController.cs",
         "using Microsoft.AspNetCore.Mvc;\n\n"
         "namespace Feeder.Api.Controllers\n{\n"
         "    [ServiceFilter(typeof(ClientIpCheckFilter))]\n"
         "    [Route(\"api/[controller]\")]\n"
         "    [ApiController]\n"
         "    public class QuotesController : ControllerBase\n    {\n"
         "        [HttpGet(\"{symbol}\")]\n"
         "        public IActionResult Get(string symbol) { return Ok(symbol); }\n\n"
         "        [HttpPost]\n"
         "        [Route(\"orders/{id}/confirm\")]\n"
         "        public IActionResult Confirm(int id) { return Ok(id); }\n\n"
         "        [HttpPut(\"x/{id}\")]\n"
         "        public IActionResult Update(int id) { return Ok(id); }\n    }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 3));
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* ASP.NET Core (#1041) — the reporter's shape: [ApiController] before a
 * versioned [Route("api/v1/[controller]")] and an async [HttpPost("login")]
 * action produced no server-side Route node at all. */
TEST(handles_aspnet_attribute_routes_issue1041) {
    static const char *routes[] = {"/api/v1/Auth/login", NULL};
    static const EtFile f[] = {
        {"Controllers/AuthController.cs",
         "using System.Threading.Tasks;\n"
         "using Microsoft.AspNetCore.Mvc;\n\n"
         "[ApiController]\n"
         "[Route(\"api/v1/[controller]\")]\n"
         "public class AuthController : ControllerBase\n{\n"
         "    [HttpPost(\"login\")]\n"
         "    public async Task<IActionResult> Login(LoginRequest req) { return Ok(); }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 1));
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* ASP.NET Core [controller]/[action] token replacement (#1692): the action
 * token is the method name ("Async" suffix trimmed, as ASP.NET Core does by
 * default) or its [ActionName]; [AcceptVerbs] takes its path from Route=. */
TEST(handles_aspnet_route_tokens_issue1692) {
    static const char *routes[] = {"/api/Items/List", "/api/Items/Details/{id}",
                                   "/api/Items/Find/{name}", "/api/Items/archive", NULL};
    static const EtFile f[] = {
        {"ItemsController.cs", "using Microsoft.AspNetCore.Mvc;\n\n"
                               "[Route(\"api/[controller]\")]\n"
                               "public class ItemsController : Controller\n{\n"
                               "    [HttpGet(\"[action]\")]\n"
                               "    public IActionResult List() { return View(); }\n\n"
                               "    [HttpGet(\"[action]/{id}\")]\n"
                               "    public System.Threading.Tasks.Task<IActionResult> "
                               "DetailsAsync(int id) { return null; }\n\n"
                               "    [HttpGet(\"[action]/{name}\")]\n"
                               "    [ActionName(\"Find\")]\n"
                               "    public IActionResult Search(string name) { return View(); }\n\n"
                               "    [AcceptVerbs(\"POST\", \"PUT\", Route = \"archive\")]\n"
                               "    public IActionResult Archive() { return View(); }\n}\n"}};
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* ASP.NET Core absolute override + constraints (#1692): a method template that
 * starts with "/" or "~/" ignores the class prefix; route constraints,
 * optional markers and defaults are stripped to the bare parameter so the
 * Route name matches the clients' "{id}" spelling ("{code:regex(...)}" nests
 * braces). A controller without a class [Route] uses the method template as
 * the whole path. */
TEST(handles_aspnet_override_constraints_issue1692) {
    static const char *routes[] = {
        "/health", "/ping", "/api/Orders/{id}", "/api/Orders/{code}/{page}", "/api/ping", NULL};
    static const EtFile f[] = {
        {"OrdersController.cs",
         "using Microsoft.AspNetCore.Mvc;\n\n"
         "[Route(\"api/[controller]\")]\n"
         "public class OrdersController : ControllerBase\n{\n"
         "    [HttpGet(\"/health\")]\n"
         "    public IActionResult Health() { return Ok(); }\n\n"
         "    [Microsoft.AspNetCore.Mvc.HttpHeadAttribute(\"~/ping\")]\n"
         "    public IActionResult Ping() { return Ok(); }\n\n"
         "    [HttpDelete(\"{id:int:min(1)}\")]\n"
         "    public IActionResult Delete(int id) { return Ok(); }\n\n"
         "    [HttpGet(\"{code:regex(^\\\\d{{3}}$)}/{page=1}\")]\n"
         "    public IActionResult ByCode(string code, int page) { return Ok(); }\n}\n"},
        {"PingController.cs", "using Microsoft.AspNetCore.Mvc;\n\n"
                              "public class PingController : ControllerBase\n{\n"
                              "    [HttpGet(\"api/ping\", Name = \"PingRoute\")]\n"
                              "    public IActionResult Get() { return Ok(); }\n}\n"}};
    ASSERT_TRUE(et_routes_exact(f, 2, routes));
    PASS();
}

/* Control (#1692): a C# class with ordinary attributes — including string
 * arguments that look like paths — must not produce Route nodes. */
TEST(handles_aspnet_non_controller_control_issue1692) {
    static const char *routes[] = {NULL};
    static const EtFile f[] = {{"Helper.cs", "using System;\n\n"
                                             "[Serializable]\n"
                                             "[Description(\"api/helper\")]\n"
                                             "public class Helper\n{\n"
                                             "    [Obsolete(\"/use/other\")]\n"
                                             "    public void Run() { }\n\n"
                                             "    [Fact]\n"
                                             "    [Trait(\"Category\", \"api/[controller]\")]\n"
                                             "    public void Check() { }\n}\n"}};
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* Laravel (PHP) — Route facade whose QN contains "Laravel".
 * REAL BUG: internal/cbm/extract_calls.c:extract_handler_arg only accepts an
 * identifier/member_expression/selector_expression/attribute/field_expression as
 * the handler argument.  Idiomatic Laravel handlers are STRINGS ('showUsers') or
 * arrays ([Ctrl::class,'m']) — PHP also parses a bare callee as a `name` node,
 * which extract_handler_arg does not accept.  So Route::get('/u','showUsers')
 * yields a Route + CALLS but never a HANDLES edge. */
TEST(handles_laravel_php) {
    static const EtFile f[] = {
        {"Laravel/Route.php",
         "<?php\nnamespace Laravel;\n\n"
         "class Route {\n"
         "    public static function get($path, $handler) { return $handler; }\n"
         "    public static function post($path, $handler) { return $handler; }\n}\n"},
        {"routes/web.php", "<?php\nuse Laravel\\Route;\n\n"
                           "function showUsers() { return ['users' => []]; }\n"
                           "function storeUser() { return ['stored' => true]; }\n\n"
                           "Route::get('/users', 'showUsers');\n"
                           "Route::post('/users', 'storeUser');\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "HANDLES", 1));
    PASS();
}

/* #952: facade-style Laravel — the ONLY style real apps use, since the
 * Illuminate facade lives in vendor/ and is never indexed. The callee of
 * `Route::get(...)` (scoped_call_expression) was extracted as bare "get", so
 * the empty-resolution route fallback ("::get" suffix table) never engaged
 * and NO Route node minted — not even for flat registrations. Grouped routes
 * additionally need the enclosing `prefix('/x')->group(...)` chain composed
 * (same class as Spring's #734). Exact-set assertion distinguishes
 * prefix-dropped from missing entirely. */
TEST(handles_laravel_facade_routes_issue952) {
    static const char *routes[] = {"/api/welcome", "/users/me", "/users/login",
                                   "/companies/tenants/list", NULL};
    static const EtFile f[] = {{"routes/api.php",
                                "<?php\nuse Illuminate\\Support\\Facades\\Route;\n\n"
                                "Route::get('/api/welcome', WelcomeController::class);\n"
                                "Route::prefix('/users')->middleware(DomainCheckMiddleware::class)"
                                "->group(function (): void {\n"
                                "    Route::get('/me', GetCurrentUserController::class);\n"
                                "    Route::post('/login', LoginUserController::class);\n"
                                "});\n"
                                "Route::prefix('companies')->group(function (): void {\n"
                                "    Route::prefix('tenants')->group(function (): void {\n"
                                "        Route::get('/list', ListTenantsController::class);\n"
                                "    });\n"
                                "});\n"}};
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* #952 inverse guard: a non-router static call whose method name collides
 * with a route verb (Cache::get) must NOT mint a Route node — the callee
 * qualification is route-table-gated and the path gate requires a leading
 * slash. */
TEST(handles_laravel_facade_no_junk_routes_issue952) {
    static const char *routes[] = {"/real", NULL};
    static const EtFile f[] = {{"routes/api.php",
                                "<?php\nuse Illuminate\\Support\\Facades\\Route;\n"
                                "use Illuminate\\Support\\Facades\\Cache;\n\n"
                                "Route::get('/real', RealController::class);\n"
                                "$v = Cache::get('users.count');\n"
                                "$w = Cache::get('/leading/slash/key');\n"}};
    ASSERT_TRUE(et_routes_exact(f, 1, routes));
    PASS();
}

/* #1146: Laravel 11+ mounts the files named in bootstrap/app.php's
 * `->withRouting(api: ...)` under `apiPrefix` (default 'api'); the `web:` file
 * gets no prefix. The prefix is declared in a DIFFERENT file than the routes,
 * so per-file extraction alone minted `/users/me` where the runtime route is
 * `/api/users/me`, and cross-repo HTTP matching (which joins on the Route
 * qualified_name) never linked real callers. */
#define ET_L11_BOOTSTRAP_HEAD                                     \
    "<?php\n\nuse Illuminate\\Foundation\\Application;\n\n"       \
    "return Application::configure(basePath: dirname(__DIR__))\n" \
    "    ->withRouting(\n"
#define ET_L11_BOOTSTRAP_TAIL                                             \
    "    )\n"                                                             \
    "    ->withMiddleware(function ($middleware) {\n        //\n    })\n" \
    "    ->create();\n"

static const char ET_L11_API_ROUTES[] =
    "<?php\nuse Illuminate\\Support\\Facades\\Route;\n\n"
    "Route::prefix('/users')->middleware('auth')->group(function (): void {\n"
    "    Route::get('/me', [UserController::class, 'me']);\n"
    "});\n"
    "Route::post('/orders', [OrderController::class, 'store']);\n";

static const char ET_L11_WEB_ROUTES[] =
    "<?php\nuse Illuminate\\Support\\Facades\\Route;\n\n"
    "Route::get('/dashboard', [HomeController::class, 'index']);\n";

/* Default convention: `api:` given, no `apiPrefix:` -> '/api'. */
TEST(routes_laravel_withrouting_api_default_prefix_issue1146) {
    static const char *routes[] = {"/api/users/me", "/api/orders", NULL};
    static const EtFile f[] = {{"bootstrap/app.php", ET_L11_BOOTSTRAP_HEAD
                                "        web: __DIR__.'/../routes/web.php',\n"
                                "        api: __DIR__.'/../routes/api.php',\n"
                                "        commands: __DIR__.'/../routes/console.php',\n"
                                "        health: '/up',\n" ET_L11_BOOTSTRAP_TAIL},
                               {"routes/api.php", ET_L11_API_ROUTES}};
    ASSERT_TRUE(et_routes_exact_mode(f, 2, routes, false, "__route__GET__/api/users/me"));
    PASS();
}

/* Explicit `apiPrefix:` (multi-segment, array form of `api:`) replaces the
 * default for every mounted file. */
TEST(routes_laravel_withrouting_api_custom_prefix_issue1146) {
    static const char *routes[] = {"/v1/api/users/me", "/v1/api/orders", "/v1/api/partners", NULL};
    static const EtFile f[] = {
        {"bootstrap/app.php", ET_L11_BOOTSTRAP_HEAD
         "        api: [__DIR__.'/../routes/api.php', __DIR__.'/../routes/partner.php'],\n"
         "        apiPrefix: 'v1/api',\n" ET_L11_BOOTSTRAP_TAIL},
        {"routes/api.php", ET_L11_API_ROUTES},
        {"routes/partner.php", "<?php\nuse Illuminate\\Support\\Facades\\Route;\n\n"
                               "Route::get('/partners', [PartnerController::class, 'index']);\n"}};
    ASSERT_TRUE(et_routes_exact_mode(f, 3, routes, false, "__route__GET__/v1/api/partners"));
    PASS();
}

/* The `web:` file of the same app is NOT mounted under the api prefix. */
TEST(routes_laravel_withrouting_web_no_prefix_issue1146) {
    static const char *routes[] = {"/dashboard", "/api/users/me", "/api/orders", NULL};
    static const EtFile f[] = {
        {"bootstrap/app.php", ET_L11_BOOTSTRAP_HEAD
         "        web: __DIR__.'/../routes/web.php',\n"
         "        api: __DIR__.'/../routes/api.php',\n" ET_L11_BOOTSTRAP_TAIL},
        {"routes/web.php", ET_L11_WEB_ROUTES},
        {"routes/api.php", ET_L11_API_ROUTES}};
    ASSERT_TRUE(et_routes_exact(f, 3, routes));
    PASS();
}

/* Control: the Laravel <= 10 RouteServiceProvider form (no withRouting, the
 * prefix lives on a `->group(base_path(...))` call) is not composed today and
 * must stay exactly as it is -- no filename-based `/api` inference. */
TEST(routes_laravel_routeserviceprovider_control_issue1146) {
    static const char *routes[] = {"/users/me", "/orders", NULL};
    static const EtFile f[] = {
        {"bootstrap/app.php",
         "<?php\n\n$app = new Illuminate\\Foundation\\Application(\n"
         "    $_ENV['APP_BASE_PATH'] ?? dirname(__DIR__)\n);\n\nreturn $app;\n"},
        {"app/Providers/RouteServiceProvider.php",
         "<?php\nnamespace App\\Providers;\n\n"
         "use Illuminate\\Support\\Facades\\Route;\n\n"
         "class RouteServiceProvider extends ServiceProvider {\n"
         "    public function boot(): void {\n"
         "        $this->routes(function () {\n"
         "            Route::middleware('api')->prefix('api')"
         "->group(base_path('routes/api.php'));\n"
         "            Route::middleware('web')->group(base_path('routes/web.php'));\n"
         "        });\n    }\n}\n"},
        {"routes/api.php", ET_L11_API_ROUTES}};
    ASSERT_TRUE(et_routes_exact(f, 3, routes));
    PASS();
}

/* A non-literal `apiPrefix:` is unknown at index time: leave the paths alone
 * rather than guess the default. */
TEST(routes_laravel_withrouting_nonliteral_prefix_issue1146) {
    static const char *routes[] = {"/users/me", "/orders", NULL};
    static const EtFile f[] = {
        {"bootstrap/app.php", ET_L11_BOOTSTRAP_HEAD
         "        api: __DIR__.'/../routes/api.php',\n"
         "        apiPrefix: config('app.api_prefix'),\n" ET_L11_BOOTSTRAP_TAIL},
        {"routes/api.php", ET_L11_API_ROUTES}};
    ASSERT_TRUE(et_routes_exact(f, 2, routes));
    PASS();
}

/* Same contract on the PARALLEL resolver (> 50 files), with the Laravel app
 * in a monorepo subdirectory: the nearest ancestor bootstrap/app.php owns it. */
TEST(routes_laravel_withrouting_parallel_subdir_issue1146) {
    static const char *routes[] = {"/api/users/me", "/api/orders", NULL};
    static const EtFile f[] = {
        {"gateway/bootstrap/app.php", ET_L11_BOOTSTRAP_HEAD
         "        web: __DIR__.'/../routes/web.php',\n"
         "        api: __DIR__.'/../routes/api.php',\n" ET_L11_BOOTSTRAP_TAIL},
        {"gateway/routes/api.php", ET_L11_API_ROUTES}};
    ASSERT_TRUE(et_routes_exact_mode(f, 2, routes, true, "__route__POST__/api/orders"));
    PASS();
}

/* Assert the exact HANDLES edge set as "<handler-QN tail> -> <route name>"
 * pairs. The tail is matched at a segment boundary against the handler's
 * qualified name ("UserController.show" matches
 * "<project>.app.Http.Controllers.UserController.UserController.show"), so a
 * same-named method on another class never satisfies it; the total count must
 * match too, so a fabricated extra edge fails the assertion. */
typedef struct {
    const char *handler_tail;
    const char *route;
} EtHandles;

static int et_qn_has_tail(const char *qn, const char *tail) {
    size_t ql = qn ? strlen(qn) : 0;
    size_t tl = strlen(tail);
    return ql >= tl && strcmp(qn + ql - tl, tail) == 0 && (ql == tl || qn[ql - tl - 1] == '.');
}

/* route_qns (optional, NULL-terminated): Route qualified_names that must
 * exist, so a test asserting that a route gets NO handler cannot pass merely
 * because the route itself was never extracted. */
static int et_handles_exact_routes(const EtFile *files, int nfiles, const EtHandles *want,
                                   int parallel, const char *const *route_qns) {
    EtProj lp;
    cbm_store_t *store =
        parallel ? et_index_parallel(&lp, files, nfiles) : et_index_files(&lp, files, nfiles);
    int wanted = 0;
    int found[ET_ROUTE_ASSERT_MAX] = {0};
    while (want[wanted].handler_tail && wanted < ET_ROUTE_ASSERT_MAX) {
        wanted++;
    }
    cbm_edge_t *edges = NULL;
    int n = 0;
    int ok =
        store != NULL &&
        cbm_store_find_edges_by_type(store, lp.project, "HANDLES", &edges, &n) == CBM_STORE_OK &&
        n == wanted;
    for (int i = 0; store && i < n; i++) {
        cbm_node_t src = {0};
        cbm_node_t tgt = {0};
        int have_src = cbm_store_find_node_by_id(store, edges[i].source_id, &src) == CBM_STORE_OK;
        int have_tgt = cbm_store_find_node_by_id(store, edges[i].target_id, &tgt) == CBM_STORE_OK;
        int matched = 0;
        for (int wi = 0; have_src && have_tgt && wi < wanted; wi++) {
            if (!found[wi] && tgt.name && strcmp(tgt.name, want[wi].route) == 0 &&
                et_qn_has_tail(src.qualified_name, want[wi].handler_tail)) {
                found[wi] = matched = 1;
                break;
            }
        }
        if (!matched) {
            ok = 0;
            fprintf(stderr, "  [ET-HANDLES] unexpected %s -> %s\n",
                    have_src && src.qualified_name ? src.qualified_name : "<?>",
                    have_tgt && tgt.name ? tgt.name : "<?>");
        }
        if (have_src)
            cbm_node_free_fields(&src);
        if (have_tgt)
            cbm_node_free_fields(&tgt);
    }
    for (int wi = 0; wi < wanted; wi++) {
        if (!found[wi]) {
            ok = 0;
            fprintf(stderr, "  [ET-HANDLES] missing %s -> %s\n", want[wi].handler_tail,
                    want[wi].route);
        }
    }
    for (int ri = 0; route_qns && route_qns[ri]; ri++) {
        cbm_node_t route = {0};
        if (!store ||
            cbm_store_find_node_by_qn(store, lp.project, route_qns[ri], &route) != CBM_STORE_OK) {
            ok = 0;
            fprintf(stderr, "  [ET-HANDLES] missing Route %s\n", route_qns[ri]);
        } else {
            cbm_node_free_fields(&route);
        }
    }
    if (!ok) {
        fprintf(stderr, "  [ET-HANDLES] FAIL (%s path) expected=%d actual=%d\n",
                parallel ? "parallel" : "sequential", wanted, n);
    }
    if (edges)
        cbm_store_free_edges(edges, n);
    et_cleanup(&lp, store);
    return ok;
}

static int et_handles_exact(const EtFile *files, int nfiles, const EtHandles *want, int parallel) {
    return et_handles_exact_routes(files, nfiles, want, parallel, NULL);
}

/* The exact DATA_FLOWS set, caller -> handler by QN tail. route_qns (NULL-terminated) must
 * exist, so a test that expects NO flow on a path cannot pass because the caller's or the
 * handler's Route was never created; handles is the exact HANDLES count. */
typedef struct {
    const char *caller_tail;
    const char *handler_tail;
} EtFlow;

static int et_flow_matches(cbm_store_t *store, const cbm_edge_t *e, const EtFlow *want, int wanted,
                           int *found) {
    cbm_node_t src = {0};
    cbm_node_t tgt = {0};
    int have_src = cbm_store_find_node_by_id(store, e->source_id, &src) == CBM_STORE_OK;
    int have_tgt = cbm_store_find_node_by_id(store, e->target_id, &tgt) == CBM_STORE_OK;
    int matched = 0;
    for (int wi = 0; have_src && have_tgt && wi < wanted; wi++) {
        if (!found[wi] && et_qn_has_tail(src.qualified_name, want[wi].caller_tail) &&
            et_qn_has_tail(tgt.qualified_name, want[wi].handler_tail)) {
            found[wi] = matched = 1;
            break;
        }
    }
    if (!matched) {
        fprintf(stderr, "  [ET-FLOWS] unexpected %s -> %s\n",
                have_src && src.qualified_name ? src.qualified_name : "<?>",
                have_tgt && tgt.qualified_name ? tgt.qualified_name : "<?>");
    }
    if (have_src)
        cbm_node_free_fields(&src);
    if (have_tgt)
        cbm_node_free_fields(&tgt);
    return matched;
}

static int et_data_flows_exact(const EtFile *files, int nfiles, const EtFlow *want, int parallel,
                               const char *const *route_qns, int handles) {
    EtProj lp;
    cbm_store_t *store =
        parallel ? et_index_parallel(&lp, files, nfiles) : et_index_files(&lp, files, nfiles);
    int wanted = 0;
    int found[ET_ROUTE_ASSERT_MAX] = {0};
    while (want[wanted].caller_tail && wanted < ET_ROUTE_ASSERT_MAX) {
        wanted++;
    }
    cbm_edge_t *edges = NULL;
    int n = 0;
    int ok =
        store != NULL &&
        cbm_store_find_edges_by_type(store, lp.project, "DATA_FLOWS", &edges, &n) == CBM_STORE_OK &&
        n == wanted;
    for (int i = 0; store && i < n; i++) {
        if (!et_flow_matches(store, &edges[i], want, wanted, found)) {
            ok = 0;
        }
    }
    for (int wi = 0; wi < wanted; wi++) {
        if (!found[wi]) {
            ok = 0;
            fprintf(stderr, "  [ET-FLOWS] missing %s -> %s\n", want[wi].caller_tail,
                    want[wi].handler_tail);
        }
    }
    for (int ri = 0; route_qns && route_qns[ri]; ri++) {
        cbm_node_t route = {0};
        if (!store ||
            cbm_store_find_node_by_qn(store, lp.project, route_qns[ri], &route) != CBM_STORE_OK) {
            ok = 0;
            fprintf(stderr, "  [ET-FLOWS] missing Route %s\n", route_qns[ri]);
        } else {
            cbm_node_free_fields(&route);
        }
    }
    int got_handles = store ? cbm_store_count_edges_by_type(store, lp.project, "HANDLES") : -1;
    if (got_handles != handles) {
        ok = 0;
        fprintf(stderr, "  [ET-FLOWS] HANDLES=%d expected %d\n", got_handles, handles);
    }
    if (!ok) {
        fprintf(stderr, "  [ET-FLOWS] FAIL (%s path) expected=%d actual=%d\n",
                parallel ? "parallel" : "sequential", wanted, n);
    }
    if (edges)
        cbm_store_free_edges(edges, n);
    et_cleanup(&lp, store);
    return ok;
}

/* R1 + R2 (user decision, 2026-10-18): DATA_FLOWS pair a test-file caller only with handlers in its
 * own file, and a test-file handler only with callers in its own file. A Spring controller and a
 * mock controller under src/test/ both answer `@RequestMapping` (any method). A production TS
 * client and a TS test (.test.ts) call them with fetch, which names no method, so every caller and
 * handler meets on the same __route__ANY__ node. Only the production client -> production
 * controller pair is a data flow of the application, on the sequential and the parallel path. */
static const EtFile et_r12_files[] = {
    {"server/OrderController.java",
     "package com.example;\n\n"
     "import org.springframework.web.bind.annotation.RequestMapping;\n"
     "import org.springframework.web.bind.annotation.RestController;\n\n"
     "@RestController\n@RequestMapping(\"/api\")\npublic class OrderController {\n"
     "    @RequestMapping(\"/orders\")\n"
     "    public String orders() {\n        return \"orders\";\n    }\n}\n"},
    {"server/src/test/java/com/example/MockPaymentController.java",
     "package com.example;\n\n"
     "import org.springframework.web.bind.annotation.RequestMapping;\n"
     "import org.springframework.web.bind.annotation.RestController;\n\n"
     "@RestController\n@RequestMapping(\"/api\")\npublic class MockPaymentController {\n"
     "    @RequestMapping(\"/payments\")\n"
     "    public String pay() {\n        return \"paid\";\n    }\n}\n"},
    {"web/src/api.ts",
     "export async function loadOrders() {\n  return fetch(\"/api/orders\");\n}\n\n"
     "export async function loadPayments() {\n  return fetch(\"/api/payments\");\n}\n"},
    {"web/src/api.test.ts",
     "export async function testLoadOrders() {\n  return fetch(\"/api/orders\");\n}\n"}};

static const EtFlow et_r12_flows[] = {{"api.loadOrders", "OrderController.orders"}, {NULL, NULL}};

static const char *const et_r12_routes[] = {"__route__ANY__/api/orders",
                                            "__route__ANY__/api/payments", NULL};

TEST(data_flows_test_files_pair_within_their_file_sequential) {
    ASSERT_TRUE(et_data_flows_exact(et_r12_files, 4, et_r12_flows, 0, et_r12_routes, 2));
    PASS();
}

TEST(data_flows_test_files_pair_within_their_file_parallel) {
    ASSERT_TRUE(et_data_flows_exact(et_r12_files, 4, et_r12_flows, 1, et_r12_routes, 2));
    PASS();
}

/* ASP.NET Core MVC (#1692): a GET form action and its POST submit share one
 * method name, so both overloads collapse into one Method node whose merged
 * properties keep only one route. The parallel resolver emitted each
 * definition's route at node creation and kept both; the sequential one rebuilt
 * routes from the merged properties afterwards and kept only the last (28
 * HANDLES on 6 ASP.NET corpora, 2026-10-18). Both venues keep both. */
TEST(handles_aspnet_overload_verbs_both_venues_issue1692) {
    static const EtHandles want[] = {{"AccountController.Login", "/Account/login"},
                                     {"AccountController.Login", "/Account/login"},
                                     {NULL, NULL}};
    static const char *const route_qns[] = {"__route__GET__/Account/login",
                                            "__route__POST__/Account/login", NULL};
    static const EtFile f[] = {
        {"Controllers/AccountController.cs",
         "using Microsoft.AspNetCore.Mvc;\n\n"
         "namespace Shop.Web.Controllers\n{\n"
         "    [Route(\"[controller]\")]\n"
         "    public class AccountController : Controller\n    {\n"
         "        [HttpGet(\"login\")]\n"
         "        public IActionResult Login() { return View(); }\n\n"
         "        [HttpPost(\"login\")]\n"
         "        public IActionResult Login(LoginModel model) { return Redirect(\"/\"); }\n"
         "    }\n}\n"}};
    ASSERT_TRUE(et_handles_exact_routes(f, 1, want, 0, route_qns));
    ASSERT_TRUE(et_handles_exact_routes(f, 1, want, 1, route_qns));
    PASS();
}

/* #1146: Laravel's two class-based handler forms —
 *   Route::get('/x', [UserController::class, 'show'])   (controller method)
 *   Route::get('/x', GetCurrentUserController::class)   (invokable: __invoke)
 * got no HANDLES edge, because the handler scan only took identifiers and
 * strings. The cross-repo matcher needs HANDLES on the Route, so apps written
 * this way (the #1146 reporter's included) produced 0 CROSS_HTTP_CALLS.
 * Controls: an Admin\UserController::show with the same short name (the `use`
 * statement picks the class), an alias import, a fully-qualified class that is
 * not imported, a package class under a PSR-4 root whose folder does not
 * mirror its namespace (Acme\Blog\ -> packages/blog/src/), a decoy __invoke on
 * another class, and two vendor classes that are not in the repo — those must
 * get no HANDLES edge at all rather than bind to some other class's
 * same-named method. composer.json comes first so the no-composer variant
 * below can index the same fixture without it. */
static const EtFile et_laravel_class_handlers[] = {
    {"composer.json", "{\"autoload\": {\"psr-4\": {\"App\\\\\": \"app/\", "
                      "\"Acme\\\\Blog\\\\\": \"packages/blog/src/\"}}}\n"},
    {"packages/blog/src/Http/PostsController.php",
     "<?php\nnamespace Acme\\Blog\\Http;\n\n"
     "class PostsController {\n"
     "    public function index() { return ['posts' => []]; }\n}\n"},
    {"app/Http/Controllers/UserController.php",
     "<?php\nnamespace App\\Http\\Controllers;\n\n"
     "class UserController {\n"
     "    public function show($id) { return ['id' => $id]; }\n"
     "    public function index() { return []; }\n}\n"},
    {"app/Http/Controllers/Admin/UserController.php",
     "<?php\nnamespace App\\Http\\Controllers\\Admin;\n\n"
     "class UserController {\n"
     "    public function show($id) { return ['admin' => $id]; }\n}\n"},
    {"app/Http/Controllers/PostController.php",
     "<?php\nnamespace App\\Http\\Controllers;\n\n"
     "class PostController {\n"
     "    public function show($id) { return ['post' => $id]; }\n}\n"},
    {"app/Http/Controllers/GetCurrentUserController.php",
     "<?php\nnamespace App\\Http\\Controllers;\n\n"
     "class GetCurrentUserController {\n"
     "    public function __invoke() { return ['me' => true]; }\n}\n"},
    {"app/Http/Controllers/HealthController.php",
     "<?php\nnamespace App\\Http\\Controllers;\n\n"
     "class HealthController {\n"
     "    public function __invoke() { return ['ok' => true]; }\n}\n"},
    {"routes/api.php",
     "<?php\n"
     "use App\\Http\\Controllers\\UserController;\n"
     "use App\\Http\\Controllers\\GetCurrentUserController as CurrentUser;\n"
     "use Acme\\Blog\\Http\\PostsController;\n"
     "use Illuminate\\Support\\Facades\\Route;\n\n"
     "Route::get('/users/{id}', [UserController::class, 'show']);\n"
     "Route::get('/me', CurrentUser::class);\n"
     "Route::get('/posts/{id}', [\\App\\Http\\Controllers\\PostController::class, 'show']);\n"
     "Route::get('/blog', [PostsController::class, 'index']);\n"
     "Route::get('/vendor/{id}', [VendorController::class, 'show']);\n"
     "Route::get('/vendor-ping', VendorInvokable::class);\n"}};

enum { ET_LARAVEL_CLASS_FILES = 8 };

static const EtHandles et_laravel_class_handles[] = {
    {"Controllers.UserController.UserController.show", "/users/{id}"},
    {"GetCurrentUserController.__invoke", "/me"},
    {"PostController.show", "/posts/{id}"},
    {"PostsController.index", "/blog"},
    {NULL, NULL}};

TEST(handles_laravel_class_handlers_issue1146) {
    ASSERT_TRUE(et_handles_exact(et_laravel_class_handlers, ET_LARAVEL_CLASS_FILES,
                                 et_laravel_class_handles, 0));
    PASS();
}

/* Same fixture through the parallel pipeline (> MIN_FILES_FOR_PARALLEL),
 * whose route emitter (pass_parallel.c) is a separate code path. */
TEST(handles_laravel_class_handlers_parallel_issue1146) {
    ASSERT_TRUE(et_handles_exact(et_laravel_class_handlers, ET_LARAVEL_CLASS_FILES,
                                 et_laravel_class_handles, 1));
    PASS();
}

/* Without composer.json there is no PSR-4 map: a class is placed only where
 * its namespace mirrors the folders (App\Http\Controllers -> app/Http/
 * Controllers). The package class, whose folder does not mirror its
 * namespace, then gets no handler rather than a guessed one. */
TEST(handles_laravel_class_handlers_no_composer_issue1146) {
    static const EtHandles want[] = {
        {"Controllers.UserController.UserController.show", "/users/{id}"},
        {"GetCurrentUserController.__invoke", "/me"},
        {"PostController.show", "/posts/{id}"},
        {NULL, NULL}};
    ASSERT_TRUE(
        et_handles_exact(et_laravel_class_handlers + 1, ET_LARAVEL_CLASS_FILES - 1, want, 0));
    PASS();
}

/* A PSR-4 prefix that covers a handler class decides where the class lives,
 * as it does for the class's `use` import (#1186): when composer would load
 * no file holding the member, the route stays without a handler. Both routes
 * below are covered by Acme\Blog\ -> packages/blog/src/:
 *   /blog/missing  MissingController has no class file there at all;
 *   /blog/archive  PostsController's class file exists but has no archive().
 * legacy/Acme/Blog/Http mirrors the namespace and holds both members, so the
 * namespace/folder fallback would bind both routes to a class composer never
 * loads. It must not run for a covered class. These files are added to the
 * fixture above, whose four handlers must keep their edges. */
static const EtFile et_laravel_psr4_absent_extra[] = {
    {"legacy/Acme/Blog/Http/MissingController.php",
     "<?php\nnamespace Acme\\Blog\\Http;\n\n"
     "class MissingController {\n"
     "    public function index() { return ['stale' => true]; }\n}\n"},
    {"legacy/Acme/Blog/Http/PostsController.php",
     "<?php\nnamespace Acme\\Blog\\Http;\n\n"
     "class PostsController {\n"
     "    public function archive() { return ['stale' => true]; }\n}\n"},
    {"routes/blog.php",
     "<?php\n"
     "use Illuminate\\Support\\Facades\\Route;\n\n"
     "Route::get('/blog/missing', [\\Acme\\Blog\\Http\\MissingController::class, 'index']);\n"
     "Route::get('/blog/archive', [\\Acme\\Blog\\Http\\PostsController::class, 'archive']);\n"}};

enum { ET_LARAVEL_PSR4_ABSENT_EXTRA = 3 };

static const char *const et_laravel_psr4_absent_routes[] = {"__route__GET__/blog/missing",
                                                            "__route__GET__/blog/archive", NULL};

static int et_laravel_psr4_absent(int parallel) {
    EtFile f[ET_LARAVEL_CLASS_FILES + ET_LARAVEL_PSR4_ABSENT_EXTRA];
    memcpy(f, et_laravel_class_handlers, sizeof(et_laravel_class_handlers));
    memcpy(f + ET_LARAVEL_CLASS_FILES, et_laravel_psr4_absent_extra,
           sizeof(et_laravel_psr4_absent_extra));
    return et_handles_exact_routes(f, ET_LARAVEL_CLASS_FILES + ET_LARAVEL_PSR4_ABSENT_EXTRA,
                                   et_laravel_class_handles, parallel,
                                   et_laravel_psr4_absent_routes);
}

TEST(handles_laravel_psr4_absent_class_issue1146) {
    ASSERT_TRUE(et_laravel_psr4_absent(0));
    PASS();
}

TEST(handles_laravel_psr4_absent_class_parallel_issue1146) {
    ASSERT_TRUE(et_laravel_psr4_absent(1));
    PASS();
}

/* Assert the exact Route set as "METHOD /path" strings: the method comes from
 * the Route QN ("__route__PUT__/articles/{}"), the path from the node name
 * (the raw composed path). The count must match too, so a junk Route fails. */
static int et_route_set_exact(const EtFile *files, int nfiles, const char **want, int parallel) {
    EtProj lp;
    cbm_store_t *store =
        parallel ? et_index_parallel(&lp, files, nfiles) : et_index_files(&lp, files, nfiles);
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    int wanted = 0;
    int found[ET_ROUTE_ASSERT_MAX] = {0};
    while (want[wanted] && wanted < ET_ROUTE_ASSERT_MAX) {
        wanted++;
    }
    int ok = store != NULL &&
             cbm_store_find_nodes_by_label(store, lp.project, "Route", &nodes, &node_count) ==
                 CBM_STORE_OK &&
             node_count == wanted;
    for (int ni = 0; store && ni < node_count; ni++) {
        char got[512];
        const char *qn = nodes[ni].qualified_name ? nodes[ni].qualified_name : "";
        const char *m = strncmp(qn, "__route__", 9) == 0 ? qn + 9 : qn;
        const char *end = strstr(m, "__");
        snprintf(got, sizeof(got), "%.*s %s", end ? (int)(end - m) : 0, m,
                 nodes[ni].name ? nodes[ni].name : "");
        int matched = 0;
        for (int wi = 0; wi < wanted; wi++) {
            if (!found[wi] && strcmp(got, want[wi]) == 0) {
                found[wi] = matched = 1;
                break;
            }
        }
        if (!matched) {
            ok = 0;
            fprintf(stderr, "  [ET-ROUTESET] unexpected %s\n", got);
        }
    }
    for (int wi = 0; wi < wanted; wi++) {
        if (!found[wi]) {
            ok = 0;
            fprintf(stderr, "  [ET-ROUTESET] missing %s\n", want[wi]);
        }
    }
    if (!ok) {
        fprintf(stderr, "  [ET-ROUTESET] FAIL (%s path) expected=%d actual=%d\n",
                parallel ? "parallel" : "sequential", wanted, node_count);
    }
    cbm_store_free_nodes(nodes, node_count);
    et_cleanup(&lp, store);
    return ok;
}

/* #1146: Laravel writes most route URIs without the leading slash —
 * Route::get('users', ...) serves /users, Route::put('{article}', ...) inside
 * prefix('articles') serves /articles/{article}, and '' is the group root.
 * Both route passes only minted a Route for a first argument starting with
 * '/', so Firefly III's API (258 registrations) and laravel.io's PUT/DELETE
 * API routes had no Route at all. Covers the facade, a facade chain
 * (Route::middleware()->get), the $router instance, the chain-form and the
 * array-form prefix group (Route::group(['prefix' => ...], fn)), and a
 * trailing slash. */
static const EtFile et_laravel_slashless[] = {
    {"routes/api.php",
     "<?php\nuse Illuminate\\Support\\Facades\\Route;\n\n"
     "Route::get('users', 'UserController@index');\n"
     "Route::post('users/', 'UserController@store');\n"
     "Route::prefix('articles')->group(function () {\n"
     "    Route::get('', 'ArticlesController@index');\n"
     "    Route::put('{article}', 'ArticlesController@update');\n"
     "    Route::delete('{article}', 'ArticlesController@delete');\n"
     "});\n"
     "Route::group(['prefix' => 'v1/autocomplete', 'as' => 'api.v1.'], static function (): void {\n"
     "    Route::get('accounts', ['uses' => 'AccountController@accounts', 'as' => 'accounts']);\n"
     "});\n"
     "Route::middleware('auth')->get('dashboard', 'DashboardController@show');\n"
     "$router->get('lumen/users', 'UserController@index');\n"}};

static const char *et_laravel_slashless_routes[] = {"GET /users",
                                                    "POST /users",
                                                    "GET /articles",
                                                    "PUT /articles/{article}",
                                                    "DELETE /articles/{article}",
                                                    "GET /v1/autocomplete/accounts",
                                                    "GET /dashboard",
                                                    "GET /lumen/users",
                                                    NULL};

TEST(routes_laravel_slashless_issue1146) {
    ASSERT_TRUE(et_route_set_exact(et_laravel_slashless, 1, et_laravel_slashless_routes, 0));
    PASS();
}

TEST(routes_laravel_slashless_parallel_issue1146) {
    ASSERT_TRUE(et_route_set_exact(et_laravel_slashless, 1, et_laravel_slashless_routes, 1));
    PASS();
}

/* The string action of a slashless registration — 'index' inside a
 * Route::controller() group, 'UserController@store' — does not say which
 * class owns the method, so resolving it could only guess a same-named
 * function anywhere in the repo (on krayin/laravel-crm 'update' and 'destroy'
 * bound a chart.js function). Such routes are minted without a guessed
 * HANDLES edge. Decoys: a JS function and a PHP method per action name. */
TEST(routes_laravel_slashless_no_guessed_handlers_issue1146) {
    static const char *want[] = {"GET /leads", "PUT /leads/edit/{id}", "POST /users", NULL};
    static const EtFile f[] = {
        {"routes/web.php",
         "<?php\nuse Illuminate\\Support\\Facades\\Route;\n\n"
         "Route::controller(LeadController::class)->prefix('leads')->group(function () {\n"
         "    Route::get('', 'index');\n"
         "    Route::put('edit/{id}', 'update');\n"
         "});\n"
         "Route::post('users', 'UserController@store');\n"},
        {"resources/js/chart.js",
         "function index() { return 1; }\nfunction update() { return 2; }\n"},
        {"app/Http/Controllers/ActivityController.php",
         "<?php\nnamespace App\\Http\\Controllers;\n\n"
         "class ActivityController {\n    public function store() { return 1; }\n}\n"}};
    ASSERT_TRUE(et_route_set_exact(f, 3, want, 0));
    EtProj lp;
    cbm_store_t *store = et_index_files(&lp, f, 3);
    int handles = store ? cbm_store_count_edges_by_type(store, lp.project, "HANDLES") : -1;
    et_cleanup(&lp, store);
    ASSERT_EQ(handles, 0);
    PASS();
}

/* Precision guard for the slashless form: only a Laravel route registration
 * (the Route facade, or $router) has its URI normalised. A string first
 * argument of any other get/post call — a request input, a cache key, config,
 * a collection or session lookup, a property that merely holds a router, a
 * test client call — stays what it is and mints no Route. */
TEST(routes_laravel_slashless_no_junk_issue1146) {
    static const char *want[] = {"GET /real", NULL};
    static const EtFile f[] = {{"routes/web.php",
                                "<?php\nuse Illuminate\\Support\\Facades\\Route;\n"
                                "use Illuminate\\Support\\Facades\\Cache;\n\n"
                                "Route::get('real', 'RealController@show');\n\n"
                                "class UsersController {\n"
                                "    public function index($request) {\n"
                                "        $name = $request->get('name');\n"
                                "        $q = $request->query->get('q');\n"
                                "        $v = Cache::get('users.count');\n"
                                "        $c = config('app.name');\n"
                                "        $k = collect(['a' => 1])->get('a');\n"
                                "        $s = session()->get('key');\n"
                                "        $r = $this->router->get('status');\n"
                                "        $p = $this->post('login', []);\n"
                                "        return $this->get('users');\n"
                                "    }\n}\n"}};
    ASSERT_TRUE(et_route_set_exact(f, 1, want, 0));
    PASS();
}

/* Rails (Ruby) — ActionDispatch router.  The handler MUST be passed as a bare
 * identifier (not the idiomatic `to: 'list_items'` string, which extract_handler_arg
 * cannot capture).  mapper.get resolves by name to the Mapper#get method whose QN
 * carries the "ActionDispatch" route-registration substring → ROUTE_REG → HANDLES. */
TEST(handles_rails_ruby) {
    static const EtFile f[] = {
        {"ActionDispatch/Routing.rb", "module ActionDispatch\n  module Routing\n"
                                      "    class Mapper\n"
                                      "      def get(path, handler); end\n"
                                      "      def post(path, handler); end\n"
                                      "    end\n  end\nend\n"},
        {"config/routes.rb", "require_relative '../ActionDispatch/Routing'\n\n"
                             "mapper = ActionDispatch::Routing::Mapper.new\n\n"
                             "def list_items; end\ndef create_item; end\n\n"
                             "mapper.get '/items', list_items\n"
                             "mapper.post '/items', create_item\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "HANDLES", 1));
    PASS();
}

/* Actix-web (Rust) — route registration via SAME-FILE wrapper functions whose
 * names carry the "actix_web" substring, called by bare name with an identifier
 * handler.  Same-file Rust calls resolve (cf. probe_rust_calls_edge); a
 * cross-file `actix_web::get` would NOT (Rust lsp_cross unwired + '::' path not
 * resolved by the generic resolver). */
TEST(handles_actix_rust) {
    static const EtFile f[] = {{"actix_web_app.rs",
                                "pub fn actix_web_get(path: &str, handler: fn()) -> String {\n"
                                "    format!(\"{}\", path)\n}\n\n"
                                "pub fn actix_web_post(path: &str, handler: fn()) -> String {\n"
                                "    format!(\"{}\", path)\n}\n\n"
                                "fn list_widgets() {}\nfn create_widget() {}\n\n"
                                "fn main() {\n"
                                "    actix_web_get(\"/widgets\", list_widgets);\n"
                                "    actix_web_post(\"/widgets\", create_widget);\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  HTTP_CALLS — outbound HTTP client calls
 *
 *  Strategy: use a local wrapper whose QN carries the library substring
 *  (e.g., "requests_get" contains "requests") — same approach as
 *  test_lang_contract.c:contract_edge_http_calls.
 * ══════════════════════════════════════════════════════════════════ */

/* fetch (JavaScript) — bare `fetch` and `node_fetch` (underscore) match no
 * library id ("node-fetch" has a hyphen that cannot appear in a JS identifier).
 * Use top-level wrapper functions whose names carry a real JS HTTP-client lib id
 * ("undici") and a PLAIN string URL (the previous concatenated URL was not
 * extracted as first_string_arg) — same proven shape as http_calls_axios_ts. */
TEST(http_calls_fetch_js) {
    static const EtFile f[] = {
        {"undici_client.js",
         "function undiciGet(url) { return Promise.resolve({ json: () => ({}) }); }\n"
         "function undiciPost(url, body) { return Promise.resolve({ json: () => ({}) }); }\n\n"
         "async function fetchUser(id) {\n"
         "    return undiciGet('/api/users');\n}\n\n"
         "async function createUser(data) {\n"
         "    return undiciPost('/api/users', data);\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HTTP_CALLS", 1));
    PASS();
}

/* axios (TypeScript) — "axios" substring in QN */
TEST(http_calls_axios_ts) {
    static const EtFile f[] = {
        {"axios.ts", "export function axiosGet(url: string): Promise<any> {\n"
                     "    return Promise.resolve({ data: {} });\n}\n\n"
                     "export function axiosPost(url: string, data: any): Promise<any> {\n"
                     "    return Promise.resolve({ data });\n}\n"},
        {"client.ts", "import { axiosGet, axiosPost } from './axios';\n\n"
                      "export async function getOrders(): Promise<any> {\n"
                      "    return axiosGet('/api/orders');\n}\n\n"
                      "export async function submitOrder(payload: any): Promise<any> {\n"
                      "    return axiosPost('/api/orders', payload);\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "HTTP_CALLS", 1));
    PASS();
}

/* requests (Python) — already in test_lang_contract.c but uses different fixture;
 * this variant tests cross-file import resolution */
TEST(http_calls_requests_python) {
    static const EtFile f[] = {
        {"http/requests_client.py",
         "def requests_get(url, params=None):\n    return {'url': url, 'params': params}\n\n"
         "def requests_post(url, json=None):\n    return {'url': url, 'json': json}\n"},
        {"services/order_service.py",
         "from http.requests_client import requests_get, requests_post\n\n\n"
         "def fetch_order(order_id):\n"
         "    return requests_get('/api/orders', params={'id': order_id})\n\n\n"
         "def place_order(payload):\n"
         "    return requests_post('/api/orders', json=payload)\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "HTTP_CALLS", 1));
    PASS();
}

/* HTTP client (Go) — the "net/http" library id contains a slash that cannot
 * appear in a Go package name, so a `nethttp` package never matched.  Use the
 * "resty" Go HTTP client id instead: the resolved QN (project.resty.client.Get)
 * carries the "resty" substring → HTTP_CALLS. */
TEST(http_calls_nethttp_go) {
    static const EtFile f[] = {
        {"resty/client.go",
         "package resty\n\n"
         "func Get(url string) (interface{}, error) { return nil, nil }\n"
         "func Post(url string, body interface{}) (interface{}, error) { return nil, nil }\n"},
        {"catalog/service.go", "package catalog\n\n"
                               "import \"resty\"\n\n"
                               "func ListProducts() (interface{}, error) {\n"
                               "    return resty.Get(\"/api/products\")\n}\n\n"
                               "func CreateProduct(body interface{}) (interface{}, error) {\n"
                               "    return resty.Post(\"/api/products\", body)\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "HTTP_CALLS", 1));
    PASS();
}

/* RestTemplate (Java) — "RestTemplate" substring in resolved QN */
TEST(http_calls_resttemplate_java) {
    static const EtFile f[] = {
        {"RestTemplate.java",
         "package http;\n\n"
         "public class RestTemplate {\n"
         "    public Object getForObject(String url, Class<?> responseType) { return null; }\n"
         "    public Object postForObject(String url, Object req, Class<?> responseType) { return "
         "null; }\n"
         "}\n"},
        {"OrderClient.java",
         "package client;\n\n"
         "import http.RestTemplate;\n\n"
         "public class OrderClient {\n"
         "    private RestTemplate rest = new RestTemplate();\n\n"
         "    public Object fetchOrders() {\n"
         "        return rest.getForObject(\"/api/orders\", Object.class);\n    }\n\n"
         "    public Object placeOrder(Object req) {\n"
         "        return rest.postForObject(\"/api/orders\", req, Object.class);\n    }\n}\n"}};

    const EtFile files[] = {
        f[0],
        f[1],
        {"quality/axios.mjs", "export function get(value) { return value; }\n"},
        {"quality/service.mjs", "import { get } from './axios.mjs';\nexport function request() { "
                                "return get('/quality-http'); }\n"},
        {"quality/Foreign.java",
         "package foreign;\npublic class RestTemplate {\n    public Object getForObject(String "
         "url, Class<?> responseType) { return null; }\n    public Object postForObject(String "
         "url, Object req, Class<?> responseType) { return null; }\n}\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "resttemplate-fetchOrders",
         .site = {.file = "OrderClient.java",
                  .caller = "OrderClient.fetchOrders",
                  .needle = "rest.getForObject(\"/api/orders\", Object.class)"},
         .relation = "CALLS",
         .target = {.file = "RestTemplate.java",
                    .suffix = "RestTemplate.getForObject",
                    .label = "Method",
                    .declaration_line = 4,
                    .declaration_text = "    public Object getForObject(String url, Class<?> "
                                        "responseType) { return null; }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "java-field-and-named-applicability",
         .why = "Declared member storage and String/Object/Class<?> arguments lack source "
                "type/flow applicability proof.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "resttemplate-placeOrder",
         .site = {.file = "OrderClient.java",
                  .caller = "OrderClient.placeOrder",
                  .needle = "rest.postForObject(\"/api/orders\", req, Object.class)"},
         .relation = "CALLS",
         .target = {.file = "RestTemplate.java",
                    .suffix = "RestTemplate.postForObject",
                    .label = "Method",
                    .declaration_line = 5,
                    .declaration_text = "    public Object postForObject(String url, Object req, "
                                        "Class<?> responseType) { return null; }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "java-field-and-named-applicability",
         .why = "Declared member storage and String/Object/Class<?> arguments lack source "
                "type/flow applicability proof.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "required-service-call",
         .site = {.file = "quality/service.mjs",
                  .caller = "request",
                  .needle = "get('/quality-http')"},
         .relation = "CALLS",
         .target = {.file = "quality/axios.mjs",
                    .suffix = "get",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function get(value) { return value; }"},
         .mode = RQ_REQUIRED}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "resttemplate-foreign-fetchOrders",
         .site = {.file = "OrderClient.java",
                  .caller = "OrderClient.fetchOrders",
                  .needle = "rest.getForObject(\"/api/orders\", Object.class)"},
         .relation = "CALLS",
         .target = {.file = "quality/Foreign.java",
                    .suffix = "RestTemplate.getForObject",
                    .label = "Method",
                    .declaration_line = 3,
                    .declaration_text = "    public Object getForObject(String url, Class<?> "
                                        "responseType) { return null; }"}},
        {.case_id = "resttemplate-foreign-placeOrder",
         .site = {.file = "OrderClient.java",
                  .caller = "OrderClient.placeOrder",
                  .needle = "rest.postForObject(\"/api/orders\", req, Object.class)"},
         .relation = "CALLS",
         .target = {.file = "quality/Foreign.java",
                    .suffix = "RestTemplate.postForObject",
                    .label = "Method",
                    .declaration_line = 4,
                    .declaration_text = "    public Object postForObject(String url, Object req, "
                                        "Class<?> responseType) { return null; }"}}};
    const EtServiceGolden service[] = {
        {.case_id = "resttemplate-route-fetchOrders",
         .site = {.file = "OrderClient.java",
                  .caller = "OrderClient.fetchOrders",
                  .needle = "rest.getForObject(\"/api/orders\", Object.class)"},
         .source = {.file = "OrderClient.java",
                    .suffix = "OrderClient.fetchOrders",
                    .label = "Method",
                    .declaration_line = 8,
                    .declaration_text = "    public Object fetchOrders() {"},
         .relation = "HTTP_CALLS",
         .route = "/api/orders",
         .route_qn = "__route__GET__/api/orders",
         .property = "method",
         .value = "GET",
         .required = false,
         .gap_id = "resttemplate-receiver-service",
         .why = "The literal endpoint is genuine; its declared field receiver and argument "
                "applicability remain unproved.",
         .tried = "Call binding prerequisites audited in current source; exact downstream route "
                  "and handler identity retained separately from the seven-relation oracle."},
        {.case_id = "resttemplate-route-placeOrder",
         .site = {.file = "OrderClient.java",
                  .caller = "OrderClient.placeOrder",
                  .needle = "rest.postForObject(\"/api/orders\", req, Object.class)"},
         .source = {.file = "OrderClient.java",
                    .suffix = "OrderClient.placeOrder",
                    .label = "Method",
                    .declaration_line = 12,
                    .declaration_text = "    public Object placeOrder(Object req) {"},
         .relation = "HTTP_CALLS",
         .route = "/api/orders",
         .route_qn = "__route__POST__/api/orders",
         .property = "method",
         .value = "POST",
         .required = false,
         .gap_id = "resttemplate-receiver-service",
         .why = "The literal endpoint is genuine; its declared field receiver and argument "
                "applicability remain unproved.",
         .tried = "Call binding prerequisites audited in current source; exact downstream route "
                  "and handler identity retained separately from the seven-relation oracle."},
        {.case_id = "required-service-route",
         .site = {.file = "quality/service.mjs",
                  .caller = "request",
                  .needle = "get('/quality-http')"},
         .source = {.file = "quality/service.mjs",
                    .suffix = "request",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text =
                        "export function request() { return get('/quality-http'); }"},
         .relation = "HTTP_CALLS",
         .route = "/quality-http",
         .route_qn = "__route__GET__/quality-http",
         .property = "method",
         .value = "GET",
         .required = true}};
    ASSERT_TRUE(et_service_quality(files, 5, 0, expected, 3, forbidden, 2, service, 3, NULL, 0,
                                   "http_calls_resttemplate_java", false));
    PASS();
}

/* RestSharp (C#) — static RestClient.Get call under a RestSharp/ path so the
 * resolved QN carries the "RestSharp" substring.
 * REAL BUG (CONFIRMED: still HTTP_CALLS=0 after switching to a static call that
 * C# resolves, cf. S4).  Even though the call resolves and the QN contains
 * "RestSharp", no HTTP_CALLS edge is emitted — the C# resolved-call path is not
 * routed through cbm_service_pattern_match for HTTP classification (the C# lsp
 * resolver likely emits the call without going through emit_classified_edge's
 * service-pattern branch, or the resolved QN it returns lacks the path prefix). */
TEST(http_calls_restsharp_csharp) {
    static const EtFile f[] = {
        {"RestSharp/Client.cs",
         "namespace RestSharp {\n"
         "    public class RestClient {\n"
         "        public static string Get(string url) { return \"\"; }\n"
         "        public static string Post(string url, object body) { return \"\"; }\n    }\n}\n"},
        {"Services/ProductService.cs",
         "using RestSharp;\n\n"
         "namespace Services {\n"
         "    class ProductService {\n"
         "        public string GetProducts() { return RestClient.Get(\"/products\"); }\n"
         "        public string AddProduct(object p) { return RestClient.Post(\"/products\", p); "
         "}\n"
         "    }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "HTTP_CALLS", 1));
    PASS();
}

/* HTTParty (Ruby) — "HTTParty" substring in resolved QN */
TEST(http_calls_httparty_ruby) {
    static const EtFile f[] = {
        {"HTTParty.rb", "module HTTParty\n"
                        "  def self.get(url, opts = {}); end\n"
                        "  def self.post(url, opts = {}); end\nend\n"},
        {"user_client.rb",
         "require_relative 'HTTParty'\n\n"
         "def fetch_users\n  HTTParty.get('/api/users')\nend\n\n"
         "def create_user(body)\n  HTTParty.post('/api/users', body: body)\nend\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "HTTP_CALLS", 1));
    PASS();
}

/* Property injection and typed/default arguments are separate quality gaps
 * from the required simple typed receiver with fixed untyped arguments. */
TEST(http_calls_guzzle_php) {
    static const EtFile f[] = {
        {"GuzzleHttp/Client.php",
         "<?php\nnamespace GuzzleHttp;\n\n"
         "class Client {\n"
         "    public function get(string $uri): string { return ''; }\n"
         "    public function post(string $uri, array $opts = []): string { return ''; }\n}\n"},
        {"Services/OrderService.php",
         "<?php\nuse GuzzleHttp\\Client;\n\n"
         "class OrderService {\n"
         "    private $client;\n\n"
         "    public function __construct(Client $client) { $this->client = $client; }\n\n"
         "    public function getOrders(): string { return $this->client->get('/orders'); }\n"
         "    public function createOrder(array $data): string {\n"
         "        return $this->client->post('/orders', ['json' => $data]);\n    }\n}\n"}};

    const EtFile files[] = {
        f[0],
        f[1],
        {"GuzzleHttp/ProofClient.php", "<?php\nnamespace GuzzleHttp;\nclass ProofClient {\n    "
                                       "public function get($uri) { return $uri; }\n}\n"},
        {"Quality/Caller.php",
         "<?php\nnamespace Quality;\nuse GuzzleHttp\\ProofClient;\nfunction request(ProofClient "
         "$client) { return $client->get('/quality-guzzle'); }\n"},
        {"Quality/Foreign.php", "<?php\nnamespace Foreign;\nclass Client {\n    public function "
                                "get(string $uri): string { return ''; }\n    public function "
                                "post(string $uri, array $opts = []): string { return ''; }\n}\n"},
        {"composer.json", "{\"autoload\":{\"psr-4\":{\"GuzzleHttp\\\\\":\"GuzzleHttp/"
                          "\",\"Quality\\\\\":\"Quality/\"},\"classmap\":[\"Services/\"]}}\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "guzzle-getOrders",
         .site = {.file = "Services/OrderService.php",
                  .caller = "OrderService.getOrders",
                  .needle = "$this->client->get('/orders')"},
         .relation = "CALLS",
         .target = {.file = "GuzzleHttp/Client.php",
                    .suffix = "Client.get",
                    .label = "Method",
                    .declaration_line = 5,
                    .declaration_text =
                        "    public function get(string $uri): string { return ''; }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "php-property-flow-typed-default-args",
         .why = "Constructor-injected property flow and typed/default argument applicability are "
                "not certified by the simple typed-parameter receiver bridge.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "guzzle-createOrder",
         .site = {.file = "Services/OrderService.php",
                  .caller = "OrderService.createOrder",
                  .needle = "$this->client->post('/orders', ['json' => $data])"},
         .relation = "CALLS",
         .target =
             {.file = "GuzzleHttp/Client.php",
              .suffix = "Client.post",
              .label = "Method",
              .declaration_line = 6,
              .declaration_text =
                  "    public function post(string $uri, array $opts = []): string { return ''; }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "php-property-flow-typed-default-args",
         .why = "Constructor-injected property flow and typed/default argument applicability are "
                "not certified by the simple typed-parameter receiver bridge.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "required-guzzle-simple-receiver",
         .site = {.file = "Quality/Caller.php",
                  .caller = "request",
                  .needle = "$client->get('/quality-guzzle')"},
         .relation = "CALLS",
         .target = {.file = "GuzzleHttp/ProofClient.php",
                    .suffix = "ProofClient.get",
                    .label = "Method",
                    .declaration_line = 4,
                    .declaration_text = "    public function get($uri) { return $uri; }"},
         .mode = RQ_REQUIRED}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "guzzle-foreign-getOrders",
         .site = {.file = "Services/OrderService.php",
                  .caller = "OrderService.getOrders",
                  .needle = "$this->client->get('/orders')"},
         .relation = "CALLS",
         .target = {.file = "Quality/Foreign.php",
                    .suffix = "Client.get",
                    .label = "Method",
                    .declaration_line = 4,
                    .declaration_text =
                        "    public function get(string $uri): string { return ''; }"}},
        {.case_id = "guzzle-foreign-createOrder",
         .site = {.file = "Services/OrderService.php",
                  .caller = "OrderService.createOrder",
                  .needle = "$this->client->post('/orders', ['json' => $data])"},
         .relation = "CALLS",
         .target = {.file = "Quality/Foreign.php",
                    .suffix = "Client.post",
                    .label = "Method",
                    .declaration_line = 5,
                    .declaration_text = "    public function post(string $uri, array $opts = []): "
                                        "string { return ''; }"}}};
    const EtServiceGolden service[] = {
        {.case_id = "guzzle-route-getOrders",
         .site = {.file = "Services/OrderService.php",
                  .caller = "OrderService.getOrders",
                  .needle = "$this->client->get('/orders')"},
         .source = {.file = "Services/OrderService.php",
                    .suffix = "OrderService.getOrders",
                    .label = "Method",
                    .declaration_line = 9,
                    .declaration_text = "    public function getOrders(): string { return "
                                        "$this->client->get('/orders'); }"},
         .relation = "HTTP_CALLS",
         .route = "/orders",
         .route_qn = "__route__GET__/orders",
         .property = "method",
         .value = "GET",
         .required = false,
         .gap_id = "guzzle-property-service",
         .why = "The original property receiver cannot borrow the simple-parameter receiver proof.",
         .tried = "Call binding prerequisites audited in current source; exact downstream route "
                  "and handler identity retained separately from the seven-relation oracle."},
        {.case_id = "guzzle-route-createOrder",
         .site = {.file = "Services/OrderService.php",
                  .caller = "OrderService.createOrder",
                  .needle = "$this->client->post('/orders', ['json' => $data])"},
         .source = {.file = "Services/OrderService.php",
                    .suffix = "OrderService.createOrder",
                    .label = "Method",
                    .declaration_line = 10,
                    .declaration_text = "    public function createOrder(array $data): string {"},
         .relation = "HTTP_CALLS",
         .route = "/orders",
         .route_qn = "__route__POST__/orders",
         .property = "method",
         .value = "POST",
         .required = false,
         .gap_id = "guzzle-property-service",
         .why = "The original property receiver cannot borrow the simple-parameter receiver proof.",
         .tried = "Call binding prerequisites audited in current source; exact downstream route "
                  "and handler identity retained separately from the seven-relation oracle."},
        {.case_id = "required-guzzle-classification",
         .site = {.file = "Quality/Caller.php",
                  .caller = "request",
                  .needle = "$client->get('/quality-guzzle')"},
         .source = {.file = "Quality/Caller.php",
                    .suffix = "request",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "function request(ProofClient $client) { return "
                                        "$client->get('/quality-guzzle'); }"},
         .relation = "HTTP_CALLS",
         .route = "/quality-guzzle",
         .route_qn = "__route__GET__/quality-guzzle",
         .property = "method",
         .value = "GET",
         .required = true}};
    ASSERT_TRUE(et_service_quality(files, 6, 1, expected, 3, forbidden, 2, service, 3, NULL, 0,
                                   "http_calls_guzzle_php", false));
    PASS();
}

/* reqwest (Rust) — Rust lsp_cross is not wired AND the generic resolver cannot
 * resolve a `::`-qualified cross-file path (cbm_registry_resolve splits on '.',
 * not '::'), so reqwest::get never resolved.  Use SAME-FILE bare-name wrapper
 * functions whose names carry the "reqwest" substring (same-file Rust calls do
 * resolve, cf. probe_rust_calls_edge) → QN contains "reqwest" → HTTP_CALLS. */
TEST(http_calls_reqwest_rust) {
    static const EtFile f[] = {
        {"reqwest_api.rs",
         "pub fn reqwest_get(url: &str) -> String {\n    url.to_string()\n}\n\n"
         "pub fn reqwest_post(url: &str, body: &str) -> String {\n    format!(\"{}{}\", url, "
         "body)\n}\n\n"
         "pub fn fetch_items() -> String {\n    reqwest_get(\"/api/items\")\n}\n\n"
         "pub fn push_item(body: &str) -> String {\n    reqwest_post(\"/api/items\", body)\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HTTP_CALLS", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  ASYNC_CALLS — message/queue/pubsub dispatch
 *
 *  Strategy: local wrappers whose QN carries the broker substring
 *  (celery, Sidekiq, kafkajs, sqs, bullmq).
 * ══════════════════════════════════════════════════════════════════ */

/* Celery (Python) — "celery" substring in resolved QN */
TEST(async_calls_celery_python) {
    static const EtFile f[] = {
        {"celery/app.py",
         "class Celery:\n"
         "    def task(self):\n        def decorator(fn): return fn\n        return decorator\n\n"
         "    def send_task(self, name, args=None): return (name, args)\n\n"
         "app = Celery()\n"},
        {"tasks/order_tasks.py", "from celery.app import app\n\n\n"
                                 "def dispatch_order_created(order_id):\n"
                                 "    return app.send_task('order_created', args=[order_id])\n\n\n"
                                 "def dispatch_order_shipped(order_id):\n"
                                 "    return app.send_task('order_shipped', args=[order_id])\n"}};

    const EtFile files[] = {
        f[0],
        f[1],
        {"celery/quality_control.py", "def publish(topic):\n    return topic\n"},
        {"quality_client.py", "from celery.quality_control import publish\n\ndef "
                              "invoke_control():\n    return publish('quality-control')\n"},
        {"quality_decoy.py",
         "class Celery:\n    def send_task(self, name, args=None): return (name, args)\n"},
        {"celery/__init__.py", "\"\"\"Local task fixture package.\"\"\"\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "celery-order_created",
         .site = {.file = "tasks/order_tasks.py",
                  .caller = "dispatch_order_created",
                  .needle = "app.send_task('order_created', args=[order_id])"},
         .relation = "CALLS",
         .target = {.file = "celery/app.py",
                    .suffix = "Celery.send_task",
                    .label = "Method",
                    .declaration_line = 6,
                    .declaration_text =
                        "    def send_task(self, name, args=None): return (name, args)"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "python-imported-instance-keyword-call",
         .why = "Imported mutable instance identity and keyword/default arguments are outside the "
                "receiver-only Python method proof subset.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "celery-order_shipped",
         .site = {.file = "tasks/order_tasks.py",
                  .caller = "dispatch_order_shipped",
                  .needle = "app.send_task('order_shipped', args=[order_id])"},
         .relation = "CALLS",
         .target = {.file = "celery/app.py",
                    .suffix = "Celery.send_task",
                    .label = "Method",
                    .declaration_line = 6,
                    .declaration_text =
                        "    def send_task(self, name, args=None): return (name, args)"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "python-imported-instance-keyword-call",
         .why = "Imported mutable instance identity and keyword/default arguments are outside the "
                "receiver-only Python method proof subset.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "required-celery-free-function",
         .site = {.file = "quality_client.py",
                  .caller = "invoke_control",
                  .needle = "publish('quality-control')"},
         .relation = "CALLS",
         .target = {.file = "celery/quality_control.py",
                    .suffix = "publish",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "def publish(topic):"},
         .mode = RQ_REQUIRED}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "celery-foreign-order_created",
         .site = {.file = "tasks/order_tasks.py",
                  .caller = "dispatch_order_created",
                  .needle = "app.send_task('order_created', args=[order_id])"},
         .relation = "CALLS",
         .target = {.file = "quality_decoy.py",
                    .suffix = "Celery.send_task",
                    .label = "Method",
                    .declaration_line = 2,
                    .declaration_text =
                        "    def send_task(self, name, args=None): return (name, args)"}},
        {.case_id = "celery-foreign-order_shipped",
         .site = {.file = "tasks/order_tasks.py",
                  .caller = "dispatch_order_shipped",
                  .needle = "app.send_task('order_shipped', args=[order_id])"},
         .relation = "CALLS",
         .target = {.file = "quality_decoy.py",
                    .suffix = "Celery.send_task",
                    .label = "Method",
                    .declaration_line = 2,
                    .declaration_text =
                        "    def send_task(self, name, args=None): return (name, args)"}}};
    const EtServiceGolden service[] = {
        {.case_id = "celery-topic-order_created",
         .site = {.file = "tasks/order_tasks.py",
                  .caller = "dispatch_order_created",
                  .needle = "app.send_task('order_created', args=[order_id])"},
         .source = {.file = "tasks/order_tasks.py",
                    .suffix = "dispatch_order_created",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "def dispatch_order_created(order_id):"},
         .relation = "ASYNC_CALLS",
         .route = "order_created",
         .route_qn = "__route__celery__order_created",
         .property = "broker",
         .value = "celery",
         .required = false,
         .gap_id = "celery-imported-instance-service",
         .why = "The literal topic is known, but the remote app instance and send_task argument "
                "binding remain unproved.",
         .tried = "Call binding prerequisites audited in current source; exact downstream route "
                  "and handler identity retained separately from the seven-relation oracle."},
        {.case_id = "celery-topic-order_shipped",
         .site = {.file = "tasks/order_tasks.py",
                  .caller = "dispatch_order_shipped",
                  .needle = "app.send_task('order_shipped', args=[order_id])"},
         .source = {.file = "tasks/order_tasks.py",
                    .suffix = "dispatch_order_shipped",
                    .label = "Function",
                    .declaration_line = 8,
                    .declaration_text = "def dispatch_order_shipped(order_id):"},
         .relation = "ASYNC_CALLS",
         .route = "order_shipped",
         .route_qn = "__route__celery__order_shipped",
         .property = "broker",
         .value = "celery",
         .required = false,
         .gap_id = "celery-imported-instance-service",
         .why = "The literal topic is known, but the remote app instance and send_task argument "
                "binding remain unproved.",
         .tried = "Call binding prerequisites audited in current source; exact downstream route "
                  "and handler identity retained separately from the seven-relation oracle."},
        {.case_id = "required-celery-classification",
         .site = {.file = "quality_client.py",
                  .caller = "invoke_control",
                  .needle = "publish('quality-control')"},
         .source = {.file = "quality_client.py",
                    .suffix = "invoke_control",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text = "def invoke_control():"},
         .relation = "ASYNC_CALLS",
         .route = "quality-control",
         .route_qn = "__route__celery__quality-control",
         .property = "broker",
         .value = "celery",
         .required = true}};
    const RQ_Target survive[] = {
        {.file = "celery/app.py",
         .suffix = "Celery",
         .label = "Class",
         .declaration_line = 1,
         .declaration_text = "class Celery:"},
        {.file = "celery/app.py",
         .suffix = "Celery.send_task",
         .label = "Method",
         .declaration_line = 6,
         .declaration_text = "    def send_task(self, name, args=None): return (name, args)"}};
    ASSERT_TRUE(et_service_quality(files, 6, 0, expected, 3, forbidden, 2, service, 3, survive, 2,
                                   "async_calls_celery_python", false));
    PASS();
}

/* WHY: scheduling delay must not drop parseable declarations. The deterministic
 * wall-offset seam now covers celery/app.py and celery/quality_control.py; the
 * required imported publish call depends on the latter, while exact original
 * Celery/send_task declarations must also survive. Tried: member calls alone
 * conflate parse survival with unsupported imported-instance/keyword binding.
 * The original two task calls remain independently counted quality gaps.
 * No real delay or probability is involved; CBM_ENABLE_TEST_SEAMS is required
 * to bind the CPU-budget regression, as in the original test-runner contract. */
TEST(async_calls_celery_wall_stall_seam) {
    static const EtFile f[] = {
        {"celery/app.py",
         "class Celery:\n"
         "    def task(self):\n        def decorator(fn): return fn\n        return decorator\n\n"
         "    def send_task(self, name, args=None): return (name, args)\n\n"
         "app = Celery()\n"},
        {"tasks/order_tasks.py", "from celery.app import app\n\n\n"
                                 "def dispatch_order_created(order_id):\n"
                                 "    return app.send_task('order_created', args=[order_id])\n\n\n"
                                 "def dispatch_order_shipped(order_id):\n"
                                 "    return app.send_task('order_shipped', args=[order_id])\n"}};

    const EtFile files[] = {
        f[0],
        f[1],
        {"celery/quality_control.py", "def publish(topic):\n    return topic\n"},
        {"quality_client.py", "from celery.quality_control import publish\n\ndef "
                              "invoke_control():\n    return publish('quality-control')\n"},
        {"quality_decoy.py",
         "class Celery:\n    def send_task(self, name, args=None): return (name, args)\n"},
        {"celery/__init__.py", "\"\"\"Local task fixture package.\"\"\"\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "celery-order_created",
         .site = {.file = "tasks/order_tasks.py",
                  .caller = "dispatch_order_created",
                  .needle = "app.send_task('order_created', args=[order_id])"},
         .relation = "CALLS",
         .target = {.file = "celery/app.py",
                    .suffix = "Celery.send_task",
                    .label = "Method",
                    .declaration_line = 6,
                    .declaration_text =
                        "    def send_task(self, name, args=None): return (name, args)"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "python-imported-instance-keyword-call",
         .why = "Imported mutable instance identity and keyword/default arguments are outside the "
                "receiver-only Python method proof subset.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "celery-order_shipped",
         .site = {.file = "tasks/order_tasks.py",
                  .caller = "dispatch_order_shipped",
                  .needle = "app.send_task('order_shipped', args=[order_id])"},
         .relation = "CALLS",
         .target = {.file = "celery/app.py",
                    .suffix = "Celery.send_task",
                    .label = "Method",
                    .declaration_line = 6,
                    .declaration_text =
                        "    def send_task(self, name, args=None): return (name, args)"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "python-imported-instance-keyword-call",
         .why = "Imported mutable instance identity and keyword/default arguments are outside the "
                "receiver-only Python method proof subset.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "required-stalled-module-call",
         .site = {.file = "quality_client.py",
                  .caller = "invoke_control",
                  .needle = "publish('quality-control')"},
         .relation = "CALLS",
         .target = {.file = "celery/quality_control.py",
                    .suffix = "publish",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "def publish(topic):"},
         .mode = RQ_REQUIRED}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "celery-foreign-order_created",
         .site = {.file = "tasks/order_tasks.py",
                  .caller = "dispatch_order_created",
                  .needle = "app.send_task('order_created', args=[order_id])"},
         .relation = "CALLS",
         .target = {.file = "quality_decoy.py",
                    .suffix = "Celery.send_task",
                    .label = "Method",
                    .declaration_line = 2,
                    .declaration_text =
                        "    def send_task(self, name, args=None): return (name, args)"}},
        {.case_id = "celery-foreign-order_shipped",
         .site = {.file = "tasks/order_tasks.py",
                  .caller = "dispatch_order_shipped",
                  .needle = "app.send_task('order_shipped', args=[order_id])"},
         .relation = "CALLS",
         .target = {.file = "quality_decoy.py",
                    .suffix = "Celery.send_task",
                    .label = "Method",
                    .declaration_line = 2,
                    .declaration_text =
                        "    def send_task(self, name, args=None): return (name, args)"}}};
    const EtServiceGolden service[] = {
        {.case_id = "celery-topic-order_created",
         .site = {.file = "tasks/order_tasks.py",
                  .caller = "dispatch_order_created",
                  .needle = "app.send_task('order_created', args=[order_id])"},
         .source = {.file = "tasks/order_tasks.py",
                    .suffix = "dispatch_order_created",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "def dispatch_order_created(order_id):"},
         .relation = "ASYNC_CALLS",
         .route = "order_created",
         .route_qn = "__route__celery__order_created",
         .property = "broker",
         .value = "celery",
         .required = false,
         .gap_id = "celery-imported-instance-service",
         .why = "The literal topic is known, but the remote app instance and send_task argument "
                "binding remain unproved.",
         .tried = "Call binding prerequisites audited in current source; exact downstream route "
                  "and handler identity retained separately from the seven-relation oracle."},
        {.case_id = "celery-topic-order_shipped",
         .site = {.file = "tasks/order_tasks.py",
                  .caller = "dispatch_order_shipped",
                  .needle = "app.send_task('order_shipped', args=[order_id])"},
         .source = {.file = "tasks/order_tasks.py",
                    .suffix = "dispatch_order_shipped",
                    .label = "Function",
                    .declaration_line = 8,
                    .declaration_text = "def dispatch_order_shipped(order_id):"},
         .relation = "ASYNC_CALLS",
         .route = "order_shipped",
         .route_qn = "__route__celery__order_shipped",
         .property = "broker",
         .value = "celery",
         .required = false,
         .gap_id = "celery-imported-instance-service",
         .why = "The literal topic is known, but the remote app instance and send_task argument "
                "binding remain unproved.",
         .tried = "Call binding prerequisites audited in current source; exact downstream route "
                  "and handler identity retained separately from the seven-relation oracle."},
        {.case_id = "required-celery-classification",
         .site = {.file = "quality_client.py",
                  .caller = "invoke_control",
                  .needle = "publish('quality-control')"},
         .source = {.file = "quality_client.py",
                    .suffix = "invoke_control",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text = "def invoke_control():"},
         .relation = "ASYNC_CALLS",
         .route = "quality-control",
         .route_qn = "__route__celery__quality-control",
         .property = "broker",
         .value = "celery",
         .required = true}};
    const RQ_Target survive[] = {
        {.file = "celery/app.py",
         .suffix = "Celery",
         .label = "Class",
         .declaration_line = 1,
         .declaration_text = "class Celery:"},
        {.file = "celery/app.py",
         .suffix = "Celery.send_task",
         .label = "Method",
         .declaration_line = 6,
         .declaration_text = "    def send_task(self, name, args=None): return (name, args)"}};
    ASSERT_TRUE(et_service_quality(files, 6, 0, expected, 3, forbidden, 2, service, 3, survive, 2,
                                   "async_calls_celery_wall_stall_seam", true));
    PASS();
}

/* Sidekiq (Ruby) — "Sidekiq" substring in resolved QN */
TEST(async_calls_sidekiq_ruby) {
    static const EtFile f[] = {
        {"Sidekiq/Worker.rb", "module Sidekiq\n  module Worker\n"
                              "    def self.perform_async(*args); end\n  end\nend\n"},
        {"workers/notification_worker.rb",
         "require_relative '../Sidekiq/Worker'\n\n"
         "class NotificationWorker\n"
         "  include Sidekiq::Worker\n\n"
         "  def self.enqueue_welcome(user_id)\n"
         "    Sidekiq::Worker.perform_async('welcome', user_id)\n  end\n\n"
         "  def self.enqueue_alert(user_id)\n"
         "    Sidekiq::Worker.perform_async('alert', user_id)\n  end\nend\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "ASYNC_CALLS", 1));
    PASS();
}

/* KafkaJS (TypeScript) — "kafkajs" substring in resolved QN */
TEST(async_calls_kafkajs_ts) {
    static const EtFile f[] = {
        {"kafkajs/producer.ts",
         "export function kafkajsProduce(topic: string, message: any): void {}\n"},
        {"events/order_events.ts", "import { kafkajsProduce } from '../kafkajs/producer';\n\n"
                                   "export function emitOrderCreated(orderId: string): void {\n"
                                   "    kafkajsProduce('order-created', { orderId });\n}\n\n"
                                   "export function emitOrderCancelled(orderId: string): void {\n"
                                   "    kafkajsProduce('order-cancelled', { orderId });\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "ASYNC_CALLS", 1));
    PASS();
}

/* The original folder layout alone does not establish Go module membership.
 * Retain it as an unconfigured negative, then supply the two explicit module
 * manifests without changing either original source file. */
static bool et_sqs_configured(const EtFile *original, bool configured, bool parallel) {
    static const EtFile extras[] = {
        {"quality_control.mjs", "export function exact_target() { return 7; }\n"
                                "export function exact_caller() { return exact_target(); }\n"},
        {"other/decoy.go", "package other\n\ntype SQS struct{}\n"
                           "func New() *SQS { return &SQS{} }\n"
                           "func (s *SQS) SendMessage() {}\n"},
        {"go.mod", "module example.com/application\n\ngo 1.22\n\n"
                   "require aws-sdk-go v0.0.0\nreplace aws-sdk-go => ./aws-sdk-go\n"},
        {"aws-sdk-go/go.mod", "module aws-sdk-go\n\ngo 1.22\n"}};
    EtFile files[6] = {original[0], original[1], extras[0], extras[1], extras[2], extras[3]};
    RQ_Source sources[4];
    for (size_t i = 0; i < 4; i++)
        sources[i] = (RQ_Source){.path = files[i].name, .text = files[i].content};
    static const RQ_Expected expected[] = {
        {.case_id = "required-control",
         .site = {.file = "quality_control.mjs",
                  .caller = "exact_caller",
                  .needle = "exact_target()",
                  .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "quality_control.mjs",
                    .suffix = "exact_target",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function exact_target() { return 7; }"}},
        {.case_id = "configured-sqs-constructor-factory",
         .site = {.file = "queue/dispatcher.go",
                  .caller = "DispatchOrderEvent",
                  .needle = "sqs.New()"},
         .relation = "CALLS",
         .target = {.file = "aws-sdk-go/service/sqs/api.go",
                    .suffix = "New",
                    .label = "Function",
                    .declaration_line = 6,
                    .declaration_text = "func New() *SQS { return &SQS{} }"}},
        {.case_id = "configured-sqs-returned-receiver",
         .site = {.file = "queue/dispatcher.go",
                  .caller = "DispatchOrderEvent",
                  .needle = "client.SendMessage(&sqs.SendMessageInput{\n"
                            "        QueueUrl:    queueUrl,\n"
                            "        MessageBody: body,\n    })"},
         .relation = "CALLS",
         .target = {
             .file = "aws-sdk-go/service/sqs/api.go",
             .suffix = "SQS.SendMessage",
             .label = "Method",
             .declaration_line = 8,
             .declaration_text =
                 "func (s *SQS) SendMessage(input *SendMessageInput) error { return nil }"}}};
    RQ_Forbidden forbidden[2] = {
        {.case_id = "factory-no-folder-guess",
         .site = expected[1].site,
         .relation = "CALLS",
         .target = {.file = "other/decoy.go",
                    .suffix = "New",
                    .label = "Function",
                    .declaration_line = 4,
                    .declaration_text = "func New() *SQS { return &SQS{} }"}},
        {.case_id = "method-no-namesake-guess",
         .site = expected[2].site,
         .relation = "CALLS",
         .target = {.file = "other/decoy.go",
                    .suffix = "SQS.SendMessage",
                    .label = "Method",
                    .declaration_line = 5,
                    .declaration_text = "func (s *SQS) SendMessage() {}"}}};
    if (!configured)
        forbidden[0].any_target = forbidden[1].any_target = true;
    EtProj project;
    cbm_store_t *store = parallel ? et_index_parallel(&project, files, configured ? 6 : 4)
                                  : et_index_files(&project, files, configured ? 6 : 4);
    RQ_Report report = {0};
    bool ok =
        store && rq_check_project_declarations(
                     store, project.project, configured ? "sqs-configured" : "sqs-unconfigured",
                     sources, 4, expected, configured ? 3 : 1, forbidden, 2, &report, stderr);
    ok = ok && report.genuine == (configured ? 3 : 1) && report.found == report.genuine &&
         report.missing == 0 && report.wrong == 0;
    /* queueUrl is the existing symbolic-expression route contract, not a
     * claim that its runtime queue address is known. Check the exact source,
     * broker and route identity independently from the callable oracle. */
    sqlite3_stmt *query = NULL;
    const char *sql =
        "SELECT s.file_path,s.name,t.label,t.name,t.qualified_name,"
        "json_extract(e.properties,'$.broker'),json_extract(e.properties,'$.url_path') "
        "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
        "WHERE e.project=?1 AND e.type='ASYNC_CALLS'";
    bool queried =
        store && sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &query, NULL) == SQLITE_OK &&
        sqlite3_bind_text(query, 1, project.project, -1, SQLITE_TRANSIENT) == SQLITE_OK;
    int count = 0, step = SQLITE_ERROR;
    if (queried)
        while ((step = sqlite3_step(query)) == SQLITE_ROW) {
            count++;
            const char *values[] = {"queue/dispatcher.go",
                                    "DispatchOrderEvent",
                                    "Route",
                                    "queueUrl",
                                    "__route__sqs__queueUrl",
                                    "sqs",
                                    "queueUrl"};
            for (int i = 0; i < 7; i++) {
                const char *value = (const char *)sqlite3_column_text(query, i);
                ok = value && !strcmp(value, values[i]) && ok;
            }
        }
    ok = ok && queried && step == SQLITE_DONE && count == (configured ? 1 : 0);
    if (query)
        sqlite3_finalize(query);
    if (!ok)
        fprintf(stderr, "[SQS] configured=%d parallel=%d async=%d genuine=%zu found=%zu\n",
                configured, parallel, count, report.genuine, report.found);
    et_cleanup(&project, store);
    return ok;
}

TEST(async_calls_sqs_go) {
    static const EtFile f[] = {
        {"aws-sdk-go/service/sqs/api.go",
         "package sqs\n\n"
         "type SendMessageInput struct{ QueueUrl string; MessageBody string }\n"
         "type SQS struct{}\n\n"
         "func New() *SQS { return &SQS{} }\n\n"
         "func (s *SQS) SendMessage(input *SendMessageInput) error { return nil }\n"},
        {"queue/dispatcher.go", "package queue\n\n"
                                "import \"aws-sdk-go/service/sqs\"\n\n"
                                "func DispatchOrderEvent(queueUrl, body string) error {\n"
                                "    client := sqs.New()\n"
                                "    return client.SendMessage(&sqs.SendMessageInput{\n"
                                "        QueueUrl:    queueUrl,\n"
                                "        MessageBody: body,\n    })\n}\n"}};
    for (int parallel = 0; parallel < 2; parallel++) {
        ASSERT_TRUE(et_sqs_configured(f, false, parallel != 0));
        ASSERT_TRUE(et_sqs_configured(f, true, parallel != 0));
    }
    PASS();
}

/* BullMQ (JavaScript) — "bullmq" substring in resolved QN */
TEST(async_calls_bullmq_js) {
    static const EtFile f[] = {
        {"bullmq/queue.js", "class bullmqQueue {\n"
                            "    add(jobName, data) { return { jobName, data }; }\n}\n\n"
                            "module.exports = { bullmqQueue };\n"},
        {"jobs/mailer.js", "const { bullmqQueue } = require('./bullmq/queue');\n\n"
                           "const mailQueue = new bullmqQueue();\n\n"
                           "function scheduleWelcomeEmail(userId) {\n"
                           "    return mailQueue.add('welcome-email', { userId });\n}\n\n"
                           "function schedulePasswordReset(userId) {\n"
                           "    return mailQueue.add('password-reset', { userId });\n}\n"}};

    const EtFile files[] = {
        f[0],
        f[1],
        {"quality/bullmq.mjs", "export function publish(value) { return value; }\n"},
        {"quality/service.mjs", "import { publish } from './bullmq.mjs';\nexport function "
                                "request() { return publish('quality-bullmq'); }\n"},
        {"quality/Foreign.js", "class bullmqQueue {\n    add(jobName, data) { return { jobName, "
                               "data }; }\n}\nmodule.exports = { bullmqQueue };\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "bullmq-valid-welcome-email",
         .site = {.file = "jobs/mailer.js",
                  .caller = "scheduleWelcomeEmail",
                  .needle = "mailQueue.add('welcome-email', { userId })"},
         .relation = "CALLS",
         .target = {.file = "bullmq/queue.js",
                    .suffix = "bullmqQueue.add",
                    .label = "Method",
                    .declaration_line = 2,
                    .declaration_text = "    add(jobName, data) { return { jobName, data }; }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "javascript-commonjs-instance-export",
         .why = "Destructured require, CommonJS exported constructor identity and JavaScript "
                "new-expression receiver flow lack complete proof.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "bullmq-valid-password-reset",
         .site = {.file = "jobs/mailer.js",
                  .caller = "schedulePasswordReset",
                  .needle = "mailQueue.add('password-reset', { userId })"},
         .relation = "CALLS",
         .target = {.file = "bullmq/queue.js",
                    .suffix = "bullmqQueue.add",
                    .label = "Method",
                    .declaration_line = 2,
                    .declaration_text = "    add(jobName, data) { return { jobName, data }; }"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "javascript-commonjs-instance-export",
         .why = "Destructured require, CommonJS exported constructor identity and JavaScript "
                "new-expression receiver flow lack complete proof.",
         .tried =
             "Exact source audit: the complete binding/proof prerequisite is unsupported; original "
             "fixture and required independent control retained. No permissive namesake fallback."},
        {.case_id = "required-service-call",
         .site = {.file = "quality/service.mjs",
                  .caller = "request",
                  .needle = "publish('quality-bullmq')"},
         .relation = "CALLS",
         .target = {.file = "quality/bullmq.mjs",
                    .suffix = "publish",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function publish(value) { return value; }"},
         .mode = RQ_REQUIRED}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "bullmq-foreign-welcome-email",
         .site = {.file = "jobs/mailer.js",
                  .caller = "scheduleWelcomeEmail",
                  .needle = "mailQueue.add('welcome-email', { userId })"},
         .relation = "CALLS",
         .target = {.file = "quality/Foreign.js",
                    .suffix = "bullmqQueue.add",
                    .label = "Method",
                    .declaration_line = 2,
                    .declaration_text = "    add(jobName, data) { return { jobName, data }; }"}},
        {.case_id = "bullmq-foreign-password-reset",
         .site = {.file = "jobs/mailer.js",
                  .caller = "schedulePasswordReset",
                  .needle = "mailQueue.add('password-reset', { userId })"},
         .relation = "CALLS",
         .target = {.file = "quality/Foreign.js",
                    .suffix = "bullmqQueue.add",
                    .label = "Method",
                    .declaration_line = 2,
                    .declaration_text = "    add(jobName, data) { return { jobName, data }; }"}}};
    const EtServiceGolden service[] = {
        {.case_id = "bullmq-topic-welcome-email",
         .site = {.file = "jobs/mailer.js",
                  .caller = "scheduleWelcomeEmail",
                  .needle = "mailQueue.add('welcome-email', { userId })"},
         .source = {.file = "jobs/mailer.js",
                    .suffix = "scheduleWelcomeEmail",
                    .label = "Function",
                    .declaration_line = 5,
                    .declaration_text = "function scheduleWelcomeEmail(userId) {"},
         .relation = "ASYNC_CALLS",
         .route = "welcome-email",
         .route_qn = "__route__bullmq__welcome-email",
         .property = "broker",
         .value = "bullmq",
         .required = false,
         .gap_id = "bullmq-commonjs-instance-service",
         .why = "The corrected ../ module path is exact; CommonJS constructor/receiver authority "
                "is not yet supported.",
         .tried = "Call binding prerequisites audited in current source; exact downstream route "
                  "and handler identity retained separately from the seven-relation oracle."},
        {.case_id = "bullmq-topic-password-reset",
         .site = {.file = "jobs/mailer.js",
                  .caller = "schedulePasswordReset",
                  .needle = "mailQueue.add('password-reset', { userId })"},
         .source = {.file = "jobs/mailer.js",
                    .suffix = "schedulePasswordReset",
                    .label = "Function",
                    .declaration_line = 9,
                    .declaration_text = "function schedulePasswordReset(userId) {"},
         .relation = "ASYNC_CALLS",
         .route = "password-reset",
         .route_qn = "__route__bullmq__password-reset",
         .property = "broker",
         .value = "bullmq",
         .required = false,
         .gap_id = "bullmq-commonjs-instance-service",
         .why = "The corrected ../ module path is exact; CommonJS constructor/receiver authority "
                "is not yet supported.",
         .tried = "Call binding prerequisites audited in current source; exact downstream route "
                  "and handler identity retained separately from the seven-relation oracle."},
        {.case_id = "required-service-route",
         .site = {.file = "quality/service.mjs",
                  .caller = "request",
                  .needle = "publish('quality-bullmq')"},
         .source = {.file = "quality/service.mjs",
                    .suffix = "request",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text =
                        "export function request() { return publish('quality-bullmq'); }"},
         .relation = "ASYNC_CALLS",
         .route = "quality-bullmq",
         .route_qn = "__route__bullmq__quality-bullmq",
         .property = "broker",
         .value = "bullmq",
         .required = true}};
    const RQ_Forbidden invalid[] = {
        {.case_id = "bullmq-invalid-path-welcome-email",
         .site = {.file = "jobs/mailer.js",
                  .caller = "scheduleWelcomeEmail",
                  .needle = "mailQueue.add('welcome-email', { userId })"},
         .relation = "CALLS",
         .target = {.file = "bullmq/queue.js",
                    .suffix = "bullmqQueue.add",
                    .label = "Method",
                    .declaration_line = 2,
                    .declaration_text = "    add(jobName, data) { return { jobName, data }; }"},
         .any_target = true},
        {.case_id = "bullmq-invalid-path-password-reset",
         .site = {.file = "jobs/mailer.js",
                  .caller = "schedulePasswordReset",
                  .needle = "mailQueue.add('password-reset', { userId })"},
         .relation = "CALLS",
         .target = {.file = "bullmq/queue.js",
                    .suffix = "bullmqQueue.add",
                    .label = "Method",
                    .declaration_line = 2,
                    .declaration_text = "    add(jobName, data) { return { jobName, data }; }"},
         .any_target = true}};
    const RQ_Expected control[] = {
        {.case_id = "required-service-call",
         .site = {.file = "quality/service.mjs",
                  .caller = "request",
                  .needle = "publish('quality-bullmq')"},
         .relation = "CALLS",
         .target = {.file = "quality/bullmq.mjs",
                    .suffix = "publish",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function publish(value) { return value; }"},
         .mode = RQ_REQUIRED}};
    const EtServiceGolden control_service[] = {
        {.case_id = "required-service-route",
         .site = {.file = "quality/service.mjs",
                  .caller = "request",
                  .needle = "publish('quality-bullmq')"},
         .source = {.file = "quality/service.mjs",
                    .suffix = "request",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text =
                        "export function request() { return publish('quality-bullmq'); }"},
         .relation = "ASYNC_CALLS",
         .route = "quality-bullmq",
         .route_qn = "__route__bullmq__quality-bullmq",
         .property = "broker",
         .value = "bullmq",
         .required = true}};
    ASSERT_TRUE(et_service_quality(files, 5, 0, control, 1, invalid, 2, control_service, 1, NULL, 0,
                                   "bullmq-invalid-path", false));
    EtFile corrected[5];
    memcpy(corrected, files, sizeof(corrected));
    corrected[1].content =
        "const { bullmqQueue } = require('../bullmq/queue');\n\nconst mailQueue = new "
        "bullmqQueue();\n\nfunction scheduleWelcomeEmail(userId) {\n    return "
        "mailQueue.add('welcome-email', { userId });\n}\n\nfunction schedulePasswordReset(userId) "
        "{\n    return mailQueue.add('password-reset', { userId });\n}\n";
    ASSERT_TRUE(et_service_quality(corrected, 5, 0, expected, 3, forbidden, 2, service, 3, NULL, 0,
                                   "bullmq-valid-path", false));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  THROWS — checked exceptions (no "Error"/"Panic" in name)
 *
 *  Parallel-path only (> 50 files). THROWS/RAISES require:
 *    1. The exception CLASS is defined as a node in the registry.
 *    2. The throw is extracted (throw_node_types or throws_clause_field).
 *    3. pass_parallel resolves the enclosing_func_qn + exception_name.
 *
 *  All three conditions must hold for the edge to appear. The fixtures
 *  define the exception class in the same file so same-module resolution
 *  succeeds.
 * ══════════════════════════════════════════════════════════════════ */

/* Java — explicit `throw new NotFoundException()` + class in same file.
 * is_checked_exception("NotFoundException") = true (no "Error"/"Panic"). */
TEST(throws_java) {
    static const EtFile meaningful[] = {
        {"Service.java",
         "package app;\n\n"
         "class NotFoundException extends Exception {\n"
         "    public NotFoundException(String msg) { super(msg); }\n}\n\n"
         "class OrderService {\n"
         "    public String findOrder(int id) throws NotFoundException {\n"
         "        if (id < 0) {\n"
         "            throw new NotFoundException(\"Order not found: \" + id);\n        }\n"
         "        return \"order:\" + id;\n    }\n\n"
         "    public String getUser(int id) throws NotFoundException {\n"
         "        if (id == 0) throw new NotFoundException(\"No user zero\");\n"
         "        return \"user:\" + id;\n    }\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int throws = store ? cbm_store_count_edges_by_type(store, lp.project, "THROWS") : -1;
    if (throws < 1) {
        fprintf(stderr, "  [ET-THROWS] Java: THROWS=%d\n", throws);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(throws >= 1);
    PASS();
}

/* Kotlin — `throw NotFoundException(...)` inside a function.
 * REAL BUG (Kotlin-specific): every other language (Java/Python/TS/C#/PHP/Scala)
 * passes the identical throws fixture shape, but Kotlin yields THROWS=0.  The
 * exception class is created+registered (defines_method_kotlin proves Kotlin
 * class/method extraction works), so the gap is in the Kotlin throw path:
 * internal/cbm/extract_semantic.c:resolve_exception_name does not recover the
 * callee identifier from a Kotlin `throw_expression`→`call_expression` (the
 * Kotlin call callee is the first child, not on a "function"/"type" field), so
 * the THROWS edge resolution never gets a usable exception name. */
TEST(throws_kotlin) {
    static const EtFile meaningful[] = {
        {"Service.kt", "class NotFoundException(msg: String) : Exception(msg)\n\n"
                       "fun findItem(id: Int): String {\n"
                       "    if (id < 0) throw NotFoundException(\"item not found: $id\")\n"
                       "    return \"item:$id\"\n}\n\n"
                       "fun getCategory(name: String): String {\n"
                       "    if (name.isEmpty()) throw NotFoundException(\"category missing\")\n"
                       "    return name\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int throws = store ? cbm_store_count_edges_by_type(store, lp.project, "THROWS") : -1;
    if (throws < 1) {
        fprintf(stderr, "  [ET-THROWS] Kotlin: THROWS=%d\n", throws);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(throws >= 1);
    PASS();
}

/* Python — `raise ValidationException(...)` — checked (no Error/Panic) */
TEST(throws_python) {
    static const EtFile meaningful[] = {
        {"validate.py", "class ValidationException(Exception):\n"
                        "    pass\n\n\n"
                        "def validate_email(email):\n"
                        "    if '@' not in email:\n"
                        "        raise ValidationException('invalid email: ' + email)\n"
                        "    return email\n\n\n"
                        "def validate_age(age):\n"
                        "    if age < 0:\n"
                        "        raise ValidationException('age must be non-negative')\n"
                        "    return age\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int throws = store ? cbm_store_count_edges_by_type(store, lp.project, "THROWS") : -1;
    if (throws < 1) {
        fprintf(stderr, "  [ET-THROWS] Python: THROWS=%d\n", throws);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(throws >= 1);
    PASS();
}

/* TypeScript — `throw new HttpException(...)` — checked (no Error/Panic) */
TEST(throws_typescript) {
    static const EtFile meaningful[] = {
        {"exceptions.ts", "export class HttpException {\n"
                          "    constructor(public status: number, public message: string) {}\n}\n"},
        {"controller.ts", "import { HttpException } from './exceptions';\n\n"
                          "export function getUser(id: number): string {\n"
                          "    if (id <= 0) throw new HttpException(404, 'User not found');\n"
                          "    return `user:${id}`;\n}\n\n"
                          "export function updateUser(id: number, name: string): string {\n"
                          "    if (!name) throw new HttpException(400, 'Name required');\n"
                          "    return `updated:${id}`;\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int throws = store ? cbm_store_count_edges_by_type(store, lp.project, "THROWS") : -1;
    if (throws < 1) {
        fprintf(stderr, "  [ET-THROWS] TypeScript: THROWS=%d\n", throws);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(throws >= 1);
    PASS();
}

/* C# — `throw new NotFoundException(...)` — checked (no Error/Panic) */
TEST(throws_csharp) {
    static const EtFile meaningful[] = {
        {"Services.cs",
         "using System;\n\n"
         "namespace App {\n"
         "    class NotFoundException : Exception {\n"
         "        public NotFoundException(string msg) : base(msg) {}\n    }\n\n"
         "    class UserService {\n"
         "        public string GetUser(int id) {\n"
         "            if (id <= 0) throw new NotFoundException($\"User {id} not found\");\n"
         "            return $\"user:{id}\";\n        }\n\n"
         "        public string DeleteUser(int id) {\n"
         "            if (id <= 0) throw new NotFoundException($\"Cannot delete {id}\");\n"
         "            return \"deleted\";\n        }\n    }\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int throws = store ? cbm_store_count_edges_by_type(store, lp.project, "THROWS") : -1;
    if (throws < 1) {
        fprintf(stderr, "  [ET-THROWS] C#: THROWS=%d\n", throws);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(throws >= 1);
    PASS();
}

/* PHP — `throw new NotFoundException(...)` — checked */
TEST(throws_php) {
    static const EtFile meaningful[] = {
        {"Repository.php",
         "<?php\n\n"
         "class NotFoundException extends \\Exception {\n"
         "    public function __construct(string $msg) { parent::__construct($msg); }\n}\n\n"
         "class UserRepository {\n"
         "    public function find(int $id): string {\n"
         "        if ($id <= 0) throw new NotFoundException(\"User $id not found\");\n"
         "        return \"user:$id\";\n    }\n\n"
         "    public function remove(int $id): void {\n"
         "        if ($id <= 0) throw new NotFoundException(\"Cannot remove $id\");\n"
         "    }\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int throws = store ? cbm_store_count_edges_by_type(store, lp.project, "THROWS") : -1;
    if (throws < 1) {
        fprintf(stderr, "  [ET-THROWS] PHP: THROWS=%d\n", throws);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(throws >= 1);
    PASS();
}

/* Scala — `throw new RecordException(...)` — checked */
TEST(throws_scala) {
    static const EtFile meaningful[] = {
        {"Records.scala", "class RecordException(msg: String) extends Exception(msg)\n\n"
                          "def parseRecord(raw: String): String = {\n"
                          "    if (raw.isEmpty) throw new RecordException(\"empty record\")\n"
                          "    raw.trim\n}\n\n"
                          "def validateRecord(raw: String): Boolean = {\n"
                          "    if (raw.length < 2) throw new RecordException(\"too short\")\n"
                          "    true\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int throws = store ? cbm_store_count_edges_by_type(store, lp.project, "THROWS") : -1;
    if (throws < 1) {
        fprintf(stderr, "  [ET-THROWS] Scala: THROWS=%d\n", throws);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(throws >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  RAISES — runtime errors/panics (name contains "Error" or "Panic")
 *  is_checked_exception() returns false → edge type = RAISES.
 * ══════════════════════════════════════════════════════════════════ */

/* Python — `raise ValueError(...)` — runtime exception */
TEST(raises_python) {
    static const EtFile meaningful[] = {{"parser.py",
                                         "class ValueError(Exception):\n    pass\n\n\n"
                                         "def parse_int(s):\n"
                                         "    if not s.isdigit():\n"
                                         "        raise ValueError('not a digit: ' + s)\n"
                                         "    return int(s)\n\n\n"
                                         "def parse_float(s):\n"
                                         "    try:\n        return float(s)\n"
                                         "    except Exception:\n"
                                         "        raise ValueError('not a float: ' + s)\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int raises = store ? cbm_store_count_edges_by_type(store, lp.project, "RAISES") : -1;
    if (raises < 1) {
        fprintf(stderr, "  [ET-RAISES] Python: RAISES=%d\n", raises);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(raises >= 1);
    PASS();
}

/* TypeScript — `throw new TypeError(...)` — runtime exception */
TEST(raises_typescript) {
    static const EtFile meaningful[] = {
        {"validators.ts", "export class TypeError {\n"
                          "    constructor(public message: string) {}\n}\n"},
        {"parser.ts", "import { TypeError } from './validators';\n\n"
                      "export function parseNumber(val: unknown): number {\n"
                      "    if (typeof val !== 'number') throw new TypeError('not a number');\n"
                      "    return val as number;\n}\n\n"
                      "export function parseString(val: unknown): string {\n"
                      "    if (typeof val !== 'string') throw new TypeError('not a string');\n"
                      "    return val as string;\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int raises = store ? cbm_store_count_edges_by_type(store, lp.project, "RAISES") : -1;
    if (raises < 1) {
        fprintf(stderr, "  [ET-RAISES] TypeScript: RAISES=%d\n", raises);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(raises >= 1);
    PASS();
}

/* Kotlin — `throw IllegalArgumentError(...)` — runtime (name has "Error").
 * REAL BUG (same Kotlin throw-extraction gap as throws_kotlin): RAISES=0 while
 * every other language passes the identical fixture shape.  Root cause:
 * internal/cbm/extract_semantic.c:resolve_exception_name fails to extract the
 * exception identifier from a Kotlin throw_expression→call_expression. */
TEST(raises_kotlin) {
    static const EtFile meaningful[] = {
        {"Errors.kt", "class IllegalArgumentError(msg: String) : RuntimeException(msg)\n\n"
                      "fun requirePositive(n: Int): Int {\n"
                      "    if (n <= 0) throw IllegalArgumentError(\"expected positive, got $n\")\n"
                      "    return n\n}\n\n"
                      "fun requireNonEmpty(s: String): String {\n"
                      "    if (s.isEmpty()) throw IllegalArgumentError(\"string is empty\")\n"
                      "    return s\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int raises = store ? cbm_store_count_edges_by_type(store, lp.project, "RAISES") : -1;
    if (raises < 1) {
        fprintf(stderr, "  [ET-RAISES] Kotlin: RAISES=%d\n", raises);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(raises >= 1);
    PASS();
}

/* C# — `throw new ArgumentError(...)` — runtime (name has "Error") */
TEST(raises_csharp) {
    static const EtFile meaningful[] = {
        {"Guards.cs",
         "using System;\n\n"
         "namespace App {\n"
         "    class ArgumentError : Exception {\n"
         "        public ArgumentError(string msg) : base(msg) {}\n    }\n\n"
         "    static class Guard {\n"
         "        public static void NotNull(object obj, string name) {\n"
         "            if (obj == null) throw new ArgumentError($\"{name} must not be null\");\n"
         "        }\n"
         "        public static void Positive(int n, string name) {\n"
         "            if (n <= 0) throw new ArgumentError($\"{name} must be positive\");\n"
         "        }\n    }\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int raises = store ? cbm_store_count_edges_by_type(store, lp.project, "RAISES") : -1;
    if (raises < 1) {
        fprintf(stderr, "  [ET-RAISES] C#: RAISES=%d\n", raises);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(raises >= 1);
    PASS();
}

/* PHP — `throw new RuntimeError(...)` — runtime (name has "Error") */
TEST(raises_php) {
    static const EtFile meaningful[] = {
        {"Exceptions.php",
         "<?php\n\n"
         "class RuntimeError extends \\RuntimeException {\n"
         "    public function __construct(string $msg) { parent::__construct($msg); }\n}\n\n"
         "function divide(int $a, int $b): float {\n"
         "    if ($b === 0) throw new RuntimeError('division by zero');\n"
         "    return $a / $b;\n}\n\n"
         "function sqrt_positive(float $n): float {\n"
         "    if ($n < 0) throw new RuntimeError('sqrt of negative');\n"
         "    return sqrt($n);\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int raises = store ? cbm_store_count_edges_by_type(store, lp.project, "RAISES") : -1;
    if (raises < 1) {
        fprintf(stderr, "  [ET-RAISES] PHP: RAISES=%d\n", raises);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(raises >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  WRITES — variable assignment: function writes to a named var
 *  that resolves to a Variable node in the same or adjacent file.
 *  Parallel-path only (> 50 files).
 *
 *  REAL BUG (all five WRITES below — single shared root cause):
 *    Variable nodes ARE created (cbm_gbuf_upsert_node, "Variable" label) but are
 *    NEVER added to the resolver registry — both register_and_link_def()
 *    (src/pipeline/pass_parallel.c:781) and process_def()
 *    (src/pipeline/pass_definitions.c:262) only cbm_registry_add() the labels
 *    Function/Method/Class/Interface.  resolve_file_rw() resolves the written
 *    var via cbm_registry_resolve(var_name) → always empty → no target node →
 *    no WRITES edge for ANY language.  Fix = register "Variable" (and "Field")
 *    defs so rw resolution can find them.  The fixtures correctly write to
 *    module-level vars / fields that DO become nodes; the gap is registration. */

/* A bare assignment in a Python function binds a local name. The module
 * variable is a forbidden WRITES target until an explicit global declaration
 * changes the binding. Keep both sides in an independent source oracle. */
TEST(writes_python) {
    static const char source[] = "registry = {}\n"
                                 "def register(key, value):\n"
                                 "    registry = {key: value}\n"
                                 "    return registry\n"
                                 "def clear_registry():\n"
                                 "    registry = {}\n";
    static const EtFile files[] = {{"state.py", source}};
    static const RQ_Source sources[] = {{.path = "state.py", .text = source}};
    static const RQ_Forbidden forbidden[] = {{.case_id = "function_assignment_is_local",
                                              .site = {.file = "state.py",
                                                       .caller = "state.register",
                                                       .needle = "registry",
                                                       .occurrence_index = 1},
                                              .relation = "WRITES",
                                              .target = {.file = "state.py",
                                                         .suffix = "state.registry",
                                                         .label = "Variable",
                                                         .declaration_line = 1,
                                                         .declaration_text = "registry = {}"}},
                                             {.case_id = "second_function_assignment_is_local",
                                              .site = {.file = "state.py",
                                                       .caller = "state.clear_registry",
                                                       .needle = "registry",
                                                       .occurrence_index = 4},
                                              .relation = "WRITES",
                                              .target = {.file = "state.py",
                                                         .suffix = "state.registry",
                                                         .label = "Variable",
                                                         .declaration_line = 1,
                                                         .declaration_text = "registry = {}"}}};
    EtProj lp;
    cbm_store_t *store = et_index_parallel(&lp, files, 1);
    RQ_Report report;
    bool ok = rq_check(store, lp.project, "python_local_write_binding", sources, 1, NULL, 0,
                       forbidden, 2, &report, stderr);
    et_cleanup(&lp, store);
    ASSERT(ok);
    ASSERT_EQ(report.wrong, 0);
    PASS();
}

TEST(writes_python_explicit_global_missing_is_quality) {
    static const char source[] = "registry = {}\n"
                                 "def register(key, value):\n"
                                 "    global registry\n"
                                 "    registry = {key: value}\n"
                                 "def clear_registry():\n"
                                 "    global registry\n"
                                 "    registry = {}\n";
    static const EtFile files[] = {{"state.py", source}};
    static const RQ_Source sources[] = {{.path = "state.py", .text = source}};
    static const RQ_Expected expected[] = {
        {.case_id = "explicit_global_assignment",
         .site = {.file = "state.py",
                  .caller = "state.register",
                  .needle = "registry",
                  .occurrence_index = 2},
         .relation = "WRITES",
         .target = {.file = "state.py",
                    .suffix = "state.registry",
                    .label = "Variable",
                    .declaration_line = 1,
                    .declaration_text = "registry = {}"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "python_global_write_binding",
         .why = "Explicit global binding is not implemented by the audited binding adapter.",
         .tried = "Literal module declaration and explicit global assignment; no dynamic namespace "
                  "access."},
        {.case_id = "explicit_global_reset",
         .site = {.file = "state.py",
                  .caller = "state.clear_registry",
                  .needle = "registry",
                  .occurrence_index = 5},
         .relation = "WRITES",
         .target = {.file = "state.py",
                    .suffix = "state.registry",
                    .label = "Variable",
                    .declaration_line = 1,
                    .declaration_text = "registry = {}"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "python_global_write_binding",
         .why = "Explicit global binding is not implemented by the audited binding adapter.",
         .tried = "Literal empty-dictionary reset under an explicit global declaration."}};
    EtProj lp;
    cbm_store_t *store = et_index_parallel(&lp, files, 1);
    RQ_Report report;
    bool ok = rq_check(store, lp.project, "python_global_write_quality", sources, 1, expected, 2,
                       NULL, 0, &report, stderr);
    et_cleanup(&lp, store);
    ASSERT(ok);
    ASSERT_EQ(report.genuine, 2);
    ASSERT_EQ(report.missing, 2);
    ASSERT_EQ(report.wrong, 0);
    PASS();
}

/* The selected reset/set occurrence is an exact write obligation. Other
 * callers remain outside this bounded oracle. A supported call and a foreign
 * same-named declaration guard against an empty or guessed graph. */
static bool et_write_quality(const EtFile *original, int count, const EtFile *decoy,
                             const RQ_Expected *gap, const RQ_Target *wrong_target,
                             const char *suite) {
    if (count < 1 || count > 13)
        return false;
    EtFile files[16];
    RQ_Source sources[16];
    memcpy(files, original, (size_t)count * sizeof(*files));
    files[count++] = *decoy;
    files[count++] = (EtFile){
        "quality_control.js",
        "export function target() { return 1; }\nexport function caller() { return target(); }\n"};
    for (int i = 0; i < count; i++)
        sources[i] = (RQ_Source){.path = files[i].name, .text = files[i].content};
    const RQ_Expected expected[] = {
        *gap,
        {.case_id = "required-exact-call-control",
         .site = {.file = "quality_control.js",
                  .caller = "quality_control.caller",
                  .needle = "target()",
                  .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "quality_control.js",
                    .suffix = "quality_control.target",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function target() { return 1; }"},
         .mode = RQ_REQUIRED}};
    const RQ_Forbidden forbidden = {.case_id = "foreign-write-target",
                                    .site = gap->site,
                                    .relation = "WRITES",
                                    .target = *wrong_target};
    for (int driver = 0; driver < 2; driver++) {
        EtProj lp;
        cbm_store_t *store =
            driver ? et_index_parallel(&lp, files, count) : et_index_files(&lp, files, count);
        if (!store) {
            et_cleanup(&lp, store);
            return false;
        }
        RQ_Report report;
        bool ok = rq_check_project_declarations(store, lp.project, suite, sources, (size_t)count,
                                                expected, 2, &forbidden, 1, &report, stderr);
        et_cleanup(&lp, store);
        size_t found = gap->mode == RQ_REQUIRED ? 2 : 1;
        if (!ok || report.genuine != 2 || report.found != found || report.missing != 2 - found)
            return false;
    }
    return true;
}

/* Go — short_var_declaration and assignment_statement */
TEST(writes_go) {
    static const EtFile meaningful[] = {{"cache.go", "package cache\n\n"
                                                     "var store map[string]string\n\n"
                                                     "func Set(key, value string) {\n"
                                                     "    store = make(map[string]string)\n"
                                                     "    store[key] = value\n}\n\n"
                                                     "func Reset() {\n"
                                                     "    store = nil\n}\n"}};
    const EtFile decoy = {"decoy/cache.go", "package foreign\nvar store map[string]string\n"};
    const RQ_Expected gap = {
        .case_id = "writes_go-exact-declaration-write",
        .site = {.file = "cache.go",
                 .caller = "cache.Reset",
                 .needle = "store",
                 .occurrence_index = 3},
        .relation = "WRITES",
        .target = {.file = "cache.go",
                   .suffix = "cache.store",
                   .label = "Variable",
                   .declaration_line = 3,
                   .declaration_text = "var store map[string]string"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "go-package-variable-write",
        .why = "Reset assigns the declared package variable directly; it has no local shadow. The "
               "Go bridge does not yet prove write-target occurrences.",
        .tried = "The original source is unchanged. This exact assignment is measured in both "
                 "pipeline paths alongside a required supported call and a forbidden foreign "
                 "target; no edge-count substitute can satisfy it."};
    const RQ_Target wrong = {.file = "decoy/cache.go",
                             .suffix = "cache.store",
                             .label = "Variable",
                             .declaration_line = 2,
                             .declaration_text = "var store map[string]string"};
    ASSERT_TRUE(et_write_quality(meaningful, 1, &decoy, &gap, &wrong, "writes_go"));
    PASS();
}

/* Java — field assignment inside a method */
TEST(writes_java) {
    static const EtFile meaningful[] = {
        {"Counter.java", "package app;\n\n"
                         "class Counter {\n"
                         "    private int count = 0;\n\n"
                         "    public void increment() {\n"
                         "        count = count + 1;\n    }\n\n"
                         "    public void reset() {\n"
                         "        count = 0;\n    }\n\n"
                         "    public int get() {\n        return count;\n    }\n}\n"}};
    const EtFile decoy = {"Other.java", "package app;\nclass Other { int count = 7; }\n"};
    const RQ_Expected gap = {
        .case_id = "writes_java-exact-declaration-write",
        .site = {.file = "Counter.java",
                 .caller = "Counter.reset",
                 .needle = "count",
                 .occurrence_index = 3},
        .relation = "WRITES",
        .target = {.file = "Counter.java",
                   .suffix = "Counter.count",
                   .label = "Field",
                   .declaration_line = 4,
                   .declaration_text = "    private int count = 0;"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "java-declared-field-write",
        .why = "reset writes the field in its own Counter class. A count field in Other is a "
               "distinct declaration. Java write-target proofs are not yet admitted.",
        .tried = "The original source is unchanged. This exact assignment is measured in both "
                 "pipeline paths alongside a required supported call and a forbidden foreign "
                 "target; no edge-count substitute can satisfy it."};
    const RQ_Target wrong = {.file = "Other.java",
                             .suffix = "Other.count",
                             .label = "Field",
                             .declaration_line = 2,
                             .declaration_text = "class Other { int count = 7; }"};
    ASSERT_TRUE(et_write_quality(meaningful, 1, &decoy, &gap, &wrong, "writes_java"));
    PASS();
}

/* Rust — local variable assignment (assignment_expression) */
TEST(writes_rust) {
    static const EtFile meaningful[] = {{"accumulator.rs",
                                         "pub struct Accumulator {\n    pub total: i64,\n}\n\n"
                                         "impl Accumulator {\n"
                                         "    pub fn add(&mut self, n: i64) {\n"
                                         "        let total = self.total + n;\n"
                                         "        self.total = total;\n    }\n\n"
                                         "    pub fn clear(&mut self) {\n"
                                         "        self.total = 0;\n    }\n}\n"}};
    const EtFile decoy = {"decoy.rs", "pub struct Other {\n    pub total: i64,\n}\n"};
    const RQ_Expected gap = {
        .case_id = "writes_rust-exact-declaration-write",
        .site = {.file = "accumulator.rs",
                 .caller = "Accumulator.clear",
                 .needle = "total",
                 .occurrence_index = 5},
        .relation = "WRITES",
        .target = {.file = "accumulator.rs",
                   .suffix = "Accumulator.total",
                   .label = "Field",
                   .declaration_line = 2,
                   .declaration_text = "    pub total: i64,"},
        .mode = RQ_REQUIRED,
        .gap_id = "rust-declared-self-field-write",
        .why =
            "clear assigns the field of its explicit &mut self receiver in impl Accumulator. The "
            "unrelated Other field is not its owner. The receiver type names the owner.",
        .tried = "The original source is unchanged. This exact assignment is measured in both "
                 "pipeline paths alongside a required supported call and a forbidden foreign "
                 "target; no edge-count substitute can satisfy it."};
    const RQ_Target wrong = {.file = "decoy.rs",
                             .suffix = "Other.total",
                             .label = "Field",
                             .declaration_line = 2,
                             .declaration_text = "    pub total: i64,"};
    ASSERT_TRUE(et_write_quality(meaningful, 1, &decoy, &gap, &wrong, "writes_rust"));
    PASS();
}

/* C# — property assignment inside methods */
TEST(writes_csharp) {
    static const EtFile meaningful[] = {{"Config.cs",
                                         "namespace App {\n"
                                         "    class Config {\n"
                                         "        public string Host = \"localhost\";\n"
                                         "        public int Port = 8080;\n\n"
                                         "        public void SetHost(string host) {\n"
                                         "            Host = host;\n        }\n\n"
                                         "        public void SetPort(int port) {\n"
                                         "            Port = port;\n        }\n    }\n}\n"}};
    const EtFile decoy = {"Other.cs",
                          "namespace App {\n    class Other { public int Port = 7; }\n}\n"};
    const RQ_Expected gap = {
        .case_id = "writes_csharp-exact-declaration-write",
        .site = {.file = "Config.cs",
                 .caller = "Config.SetPort",
                 .needle = "Port",
                 .occurrence_index = 2},
        .relation = "WRITES",
        .target = {.file = "Config.cs",
                   .suffix = "Config.Port",
                   .label = "Field",
                   .declaration_line = 4,
                   .declaration_text = "        public int Port = 8080;"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "csharp-declared-field-write",
        .why = "SetPort writes the Port field of its containing Config class; the lower-case port "
               "is its parameter. C# write-target proofs are not yet admitted.",
        .tried = "The original source is unchanged. This exact assignment is measured in both "
                 "pipeline paths alongside a required supported call and a forbidden foreign "
                 "target; no edge-count substitute can satisfy it."};
    const RQ_Target wrong = {.file = "Other.cs",
                             .suffix = "Other.Port",
                             .label = "Field",
                             .declaration_line = 2,
                             .declaration_text = "    class Other { public int Port = 7; }"};
    ASSERT_TRUE(et_write_quality(meaningful, 1, &decoy, &gap, &wrong, "writes_csharp"));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  DEFINES_METHOD — Class/Struct→Method (structural)
 *
 *  Sequential path. A Class node with methods must produce DEFINES_METHOD
 *  edges when the method's parent_class_qn resolves to the Class node.
 *  The P6 contract in test_lang_contract.c only uses Python; we broaden
 *  to Go (via pass_semantic.c receiver methods), Rust, Java, C#, PHP,
 *  Ruby, Kotlin, TypeScript, and Scala.
 * ══════════════════════════════════════════════════════════════════ */

/* Go — struct with methods.
 * REAL BUG: Go receiver methods are labelled "Method" with def.receiver set
 * (internal/cbm/extract_defs.c:2042-2047) but def.parent_class is NEVER derived
 * from the receiver type.  DEFINES_METHOD is only emitted when label=="Method"
 * AND parent_class resolves (pass_definitions.c:273 / pass_parallel.c:794), so
 * Go struct methods get no DEFINES_METHOD edge.  Fix = set def.parent_class to
 * the receiver type's QN for Go methods. */
TEST(defines_method_go) {
    static const EtFile f[] = {
        {"service.go", "package svc\n\n"
                       "type OrderService struct {\n    db interface{}\n}\n\n"
                       "func (s *OrderService) Create(name string) string {\n    return name\n}\n\n"
                       "func (s *OrderService) Delete(id int) bool {\n    return id > 0\n}\n\n"
                       "func (s *OrderService) List() []string {\n    return nil\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "DEFINES_METHOD", 1));
    PASS();
}

/* Rust — struct impl methods */
TEST(defines_method_rust) {
    static const EtFile f[] = {
        {"product.rs",
         "pub struct Product {\n    pub name: String,\n    pub price: f64,\n}\n\n"
         "impl Product {\n"
         "    pub fn new(name: &str, price: f64) -> Self {\n"
         "        Product { name: name.to_owned(), price }\n    }\n\n"
         "    pub fn discount(&self, pct: f64) -> f64 {\n"
         "        self.price * (1.0 - pct)\n    }\n\n"
         "    pub fn is_free(&self) -> bool {\n        self.price == 0.0\n    }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "DEFINES_METHOD", 1));
    PASS();
}

/* Java — class with instance and static methods */
TEST(defines_method_java) {
    static const EtFile f[] = {{"Account.java",
                                "package bank;\n\n"
                                "public class Account {\n"
                                "    private double balance;\n\n"
                                "    public Account(double initial) { this.balance = initial; }\n\n"
                                "    public void deposit(double amount) { balance += amount; }\n"
                                "    public boolean withdraw(double amount) {\n"
                                "        if (amount > balance) return false;\n"
                                "        balance -= amount;\n        return true;\n    }\n"
                                "    public double getBalance() { return balance; }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "DEFINES_METHOD", 1));
    PASS();
}

/* C# — class with multiple methods */
TEST(defines_method_csharp) {
    static const EtFile f[] = {{"Queue.cs",
                                "using System.Collections.Generic;\n\n"
                                "namespace Collections {\n"
                                "    public class Queue<T> {\n"
                                "        private List<T> items = new List<T>();\n\n"
                                "        public void Enqueue(T item) { items.Add(item); }\n"
                                "        public T Dequeue() {\n"
                                "            T item = items[0];\n"
                                "            items.RemoveAt(0);\n"
                                "            return item;\n        }\n"
                                "        public int Count() { return items.Count; }\n    }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "DEFINES_METHOD", 1));
    PASS();
}

/* PHP — class with methods */
TEST(defines_method_php) {
    static const EtFile f[] = {{"Cart.php",
                                "<?php\n\n"
                                "class Cart {\n"
                                "    private array $items = [];\n\n"
                                "    public function add(string $sku, int $qty): void {\n"
                                "        $this->items[$sku] = $qty;\n    }\n\n"
                                "    public function remove(string $sku): void {\n"
                                "        unset($this->items[$sku]);\n    }\n\n"
                                "    public function total(): int {\n"
                                "        return array_sum($this->items);\n    }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "DEFINES_METHOD", 1));
    PASS();
}

/* Ruby — class with instance methods */
TEST(defines_method_ruby) {
    static const EtFile f[] = {{"stack.rb", "class Stack\n"
                                            "  def initialize\n    @items = []\n  end\n\n"
                                            "  def push(item)\n    @items.push(item)\n  end\n\n"
                                            "  def pop\n    @items.pop\n  end\n\n"
                                            "  def peek\n    @items.last\n  end\n\n"
                                            "  def empty?\n    @items.empty?\n  end\nend\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "DEFINES_METHOD", 1));
    PASS();
}

/* Kotlin — class with methods */
TEST(defines_method_kotlin) {
    static const EtFile f[] = {
        {"Wallet.kt", "class Wallet(private var balance: Double) {\n"
                      "    fun deposit(amount: Double) {\n        balance += amount\n    }\n\n"
                      "    fun withdraw(amount: Double): Boolean {\n"
                      "        if (amount > balance) return false\n"
                      "        balance -= amount\n        return true\n    }\n\n"
                      "    fun getBalance(): Double = balance\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "DEFINES_METHOD", 1));
    PASS();
}

/* TypeScript — class with methods and constructor */
TEST(defines_method_typescript) {
    static const EtFile f[] = {
        {"Logger.ts", "export class Logger {\n"
                      "    private prefix: string;\n\n"
                      "    constructor(prefix: string) {\n        this.prefix = prefix;\n    }\n\n"
                      "    info(msg: string): void {\n        console.log(`[${this.prefix}] INFO: "
                      "${msg}`);\n    }\n\n"
                      "    warn(msg: string): void {\n        console.warn(`[${this.prefix}] WARN: "
                      "${msg}`);\n    }\n\n"
                      "    error(msg: string): void {\n        console.error(`[${this.prefix}] "
                      "ERR: ${msg}`);\n    }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "DEFINES_METHOD", 1));
    PASS();
}

/* Scala — class with methods */
TEST(defines_method_scala) {
    static const EtFile f[] = {
        {"Buffer.scala",
         "class Buffer[T] {\n"
         "    private var items: List[T] = List.empty\n\n"
         "    def append(item: T): Unit = {\n        items = items :+ item\n    }\n\n"
         "    def prepend(item: T): Unit = {\n        items = item :: items\n    }\n\n"
         "    def size: Int = items.length\n\n"
         "    def toList: List[T] = items\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "DEFINES_METHOD", 1));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  OVERRIDE — Go implicit interface satisfaction.
 *  Produced by pass_semantic.c:cbm_pipeline_implements_go().
 *  Parallel-path only (> 50 files).
 *
 *  REAL BUG (same root cause as defines_method_go): cbm_pipeline_implements_go
 *  (src/pipeline/pass_semantic.c:276-301) discovers interface methods AND struct
 *  methods exclusively via DEFINES_METHOD edges.  Because Go receiver methods get
 *  no parent_class → no DEFINES_METHOD (see defines_method_go), the interface has
 *  zero DEFINES_METHOD edges (continue at line 281) and the struct's method set is
 *  invisible → neither IMPLEMENTS nor OVERRIDE is ever emitted.  Diagnostics
 *  confirm OVERRIDE=0 IMPLEMENTS=0.  Fix the Go receiver→parent_class gap first.
 * ══════════════════════════════════════════════════════════════════ */

TEST(override_go_interface) {
    static const EtFile meaningful[] = {{"shapes.go",
                                         "package shapes\n\n"
                                         "type Shape interface {\n"
                                         "    Area() float64\n    Perimeter() float64\n}\n\n"
                                         "type Circle struct {\n    Radius float64\n}\n\n"
                                         "func (c *Circle) Area() float64 {\n"
                                         "    return 3.14159 * c.Radius * c.Radius\n}\n\n"
                                         "func (c *Circle) Perimeter() float64 {\n"
                                         "    return 2.0 * 3.14159 * c.Radius\n}\n\n"
                                         "type Rectangle struct {\n    Width, Height float64\n}\n\n"
                                         "func (r *Rectangle) Area() float64 {\n"
                                         "    return r.Width * r.Height\n}\n\n"
                                         "func (r *Rectangle) Perimeter() float64 {\n"
                                         "    return 2.0 * (r.Width + r.Height)\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int override_edges = store ? cbm_store_count_edges_by_type(store, lp.project, "OVERRIDE") : -1;
    int implements = store ? cbm_store_count_edges_by_type(store, lp.project, "IMPLEMENTS") : -1;
    if (override_edges < 1 || implements < 1) {
        fprintf(stderr, "  [ET-OVERRIDE] Go: OVERRIDE=%d IMPLEMENTS=%d\n", override_edges,
                implements);
    }
    et_cleanup(&lp, store);
    /* Both IMPLEMENTS (struct→interface) and OVERRIDE (method→interface-method) expected. */
    ASSERT_TRUE(override_edges >= 1);
    ASSERT_TRUE(implements >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  ROUTE REGISTRATION vs the #725 cross-language guard.
 *
 *  The route classifier recognises a registration by its callee
 *  (`Route::get`, `$router->get`, `app.get`) plus a path-shaped first
 *  argument. The registry still resolves the callee's bare name first, and in
 *  a mixed-language repo with imports it binds `get` by suffix_match to a
 *  same-named definition in ANOTHER language (a JS `get`, a Python `get`).
 *  The #725 guard rightly refuses that binding — but it dropped the whole
 *  call, before either resolver reached the route classification, so every
 *  GET registration vanished while POST (no `post` definition to collide
 *  with) survived: krayin/laravel-crm minted 1 of its 127 GET routes.
 *  The guard must keep dropping the false CALLS edge; the registration must
 *  survive. Each fixture also carries calls the guard must still drop.
 * ══════════════════════════════════════════════════════════════════ */

/* Index `files` (padded onto the parallel path when `parallel`), then require
 * the exact Route-name set `routes` AND zero CALLS edges into any definition
 * named `get`: every `get` in these fixtures lives in a different language
 * than its would-be callers, so any such edge is the binding #725 refuses. */
static int et_xlang_routes(const EtFile *files, int nfiles, bool parallel, const char **routes) {
    EtProj lp;
    cbm_store_t *store =
        parallel ? et_index_parallel(&lp, files, nfiles) : et_index_files(&lp, files, nfiles);
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    int wanted = 0;
    int ok = store != NULL;
    while (routes[wanted]) {
        wanted++;
    }
    if (!store || cbm_store_find_nodes_by_label(store, lp.project, "Route", &nodes, &node_count) !=
                      CBM_STORE_OK) {
        ok = 0;
    } else if (node_count != wanted) {
        ok = 0;
    }
    for (int wi = 0; wi < wanted; wi++) {
        int hit = 0;
        for (int ni = 0; ni < node_count; ni++) {
            if (nodes[ni].name && strcmp(nodes[ni].name, routes[wi]) == 0) {
                hit = 1;
            }
        }
        if (!hit) {
            fprintf(stderr, "  [ET-XLANG] missing Route %s\n", routes[wi]);
            ok = 0;
        }
    }
    if (!ok) {
        fprintf(stderr, "  [ET-XLANG] expected=%d actual=%d available:", wanted, node_count);
        for (int ni = 0; ni < node_count; ni++) {
            fprintf(stderr, " %s", nodes[ni].name ? nodes[ni].name : "<null>");
        }
        fprintf(stderr, "\n");
    }
    int false_calls = 0;
    cbm_node_t *gets = NULL;
    int get_count = 0;
    if (store &&
        cbm_store_find_nodes_by_name(store, lp.project, "get", &gets, &get_count) == CBM_STORE_OK) {
        for (int gi = 0; gi < get_count; gi++) {
            cbm_edge_t *in = NULL;
            int in_count = 0;
            if (cbm_store_find_edges_by_target_type(store, gets[gi].id, "CALLS", &in, &in_count) ==
                CBM_STORE_OK) {
                false_calls += in_count;
                cbm_store_free_edges(in, in_count);
            }
        }
        cbm_store_free_nodes(gets, get_count);
    }
    if (false_calls != 0) {
        fprintf(stderr, "  [ET-XLANG] %d cross-language CALLS into `get`\n", false_calls);
        ok = 0;
    }
    cbm_store_free_nodes(nodes, node_count);
    et_cleanup(&lp, store);
    return ok;
}

/* Laravel: facade `Route::get` and `$router->get` beside two JS `get`
 * definitions. Controls: POST is unchanged, and a bare PHP `get('/x')` (no
 * route-registration callee) stays dropped — no Route, no CALLS into JS. */
static const EtFile et_xlang_laravel[] = {
    {"app/Http/Controllers/LeadController.php", "<?php\nnamespace App\\Http\\Controllers;\n\n"
                                                "class LeadController\n{\n"
                                                "    public function index() { return []; }\n"
                                                "    public function store() { return []; }\n}\n"},
    {"routes/web.php", "<?php\nuse Illuminate\\Support\\Facades\\Route;\n"
                       "use App\\Http\\Controllers\\LeadController;\n\n"
                       "Route::get('/leads', [LeadController::class, 'index']);\n"
                       "Route::post('/leads/store', [LeadController::class, 'store']);\n"
                       "$router->get('/reports', [LeadController::class, 'index']);\n\n"
                       "function report_file() { return get('/files/report'); }\n"},
    {"resources/js/http.js", "export function get(url) {\n  return url;\n}\n"},
    {"resources/js/store.js", "export class Store {\n  get(key) {\n    return key;\n  }\n}\n"},
};
static const char *et_xlang_laravel_routes[] = {"/leads", "/leads/store", "/reports", NULL};

TEST(routes_laravel_get_survives_xlang_guard) {
    ASSERT_TRUE(et_xlang_routes(et_xlang_laravel,
                                (int)(sizeof(et_xlang_laravel) / sizeof(et_xlang_laravel[0])),
                                false, et_xlang_laravel_routes));
    PASS();
}

TEST(routes_laravel_get_survives_xlang_guard_parallel) {
    ASSERT_TRUE(et_xlang_routes(et_xlang_laravel,
                                (int)(sizeof(et_xlang_laravel) / sizeof(et_xlang_laravel[0])), true,
                                et_xlang_laravel_routes));
    PASS();
}

/* Express: `app.get` beside two Python `get` definitions. Controls: POST is
 * unchanged; a member `memo.get(key)` (route suffix, no path) and a bare
 * `get('/cache/key')` stay dropped — no Route, no CALLS into Python. */
static const EtFile et_xlang_express[] = {
    {"web/server.js", "const express = require('express');\n"
                      "const { health } = require('./handlers');\n\n"
                      "const app = express();\n\n"
                      "app.get('/health', health);\n"
                      "app.post('/items', health);\n\n"
                      "function readCache(memo, key) {\n"
                      "  return memo.get(key) || get('/cache/key');\n"
                      "}\n"
                      "module.exports = { readCache };\n"},
    {"web/handlers.js",
     "function health(req, res) {\n  return res;\n}\nmodule.exports = { health };\n"},
    {"tools/cache.py", "def get(key):\n    return key\n"},
    {"tools/store.py", "class Store:\n    def get(self, key):\n        return key\n"},
};
static const char *et_xlang_express_routes[] = {"/health", "/items", NULL};

TEST(routes_express_get_survives_xlang_guard) {
    ASSERT_TRUE(et_xlang_routes(et_xlang_express,
                                (int)(sizeof(et_xlang_express) / sizeof(et_xlang_express[0])),
                                false, et_xlang_express_routes));
    PASS();
}

TEST(routes_express_get_survives_xlang_guard_parallel) {
    ASSERT_TRUE(et_xlang_routes(et_xlang_express,
                                (int)(sizeof(et_xlang_express) / sizeof(et_xlang_express[0])), true,
                                et_xlang_express_routes));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SUITE
 * ══════════════════════════════════════════════════════════════════ */

SUITE(edge_types_probe) {
    /* HANDLES — route→handler across web frameworks */
    RUN_TEST(calls_jsx_component_via_tsconfig_alias_parallel_issue1085);
    RUN_TEST(calls_jsx_named_components_require_exact_binding);
    RUN_TEST(graphql_route_keyed_by_operation_name_issue598);
    RUN_TEST(graphql_operation_identity_parser_issue598);
    RUN_TEST(handles_flask_python);
    RUN_TEST(handles_fastapi_python);
    RUN_TEST(handles_drf_action_python);
    RUN_TEST(handles_express_ts);
    RUN_TEST(handles_fastify_js);
    RUN_TEST(handles_gin_go);
    RUN_TEST(handles_fiber_group_prefix_sequential_issue686);
    RUN_TEST(handles_fiber_group_prefix_parallel_issue686);
    RUN_TEST(handles_gin_group_prefix_issue686);
    RUN_TEST(handles_spring_java);
    RUN_TEST(data_flows_test_files_pair_within_their_file_sequential);
    RUN_TEST(data_flows_test_files_pair_within_their_file_parallel);
    RUN_TEST(handles_spring_java_path_attribute_fourth);
    RUN_TEST(handles_spring_kotlin);
    RUN_TEST(handles_jaxrs_java);
    RUN_TEST(handles_jaxrs_java_relative_path);
    RUN_TEST(handles_spring_java_relative_string_not_route);
    RUN_TEST(handles_jaxrs_scala);
    RUN_TEST(handles_aspnet_csharp);
    RUN_TEST(handles_aspnet_attribute_routes_issue1692);
    RUN_TEST(handles_aspnet_attribute_routes_issue1041);
    RUN_TEST(handles_aspnet_route_tokens_issue1692);
    RUN_TEST(handles_aspnet_override_constraints_issue1692);
    RUN_TEST(handles_aspnet_non_controller_control_issue1692);
    RUN_TEST(handles_aspnet_overload_verbs_both_venues_issue1692);
    RUN_TEST(handles_laravel_php);
    RUN_TEST(handles_laravel_facade_routes_issue952);
    RUN_TEST(handles_laravel_facade_no_junk_routes_issue952);
    RUN_TEST(routes_laravel_withrouting_api_default_prefix_issue1146);
    RUN_TEST(routes_laravel_withrouting_api_custom_prefix_issue1146);
    RUN_TEST(routes_laravel_withrouting_web_no_prefix_issue1146);
    RUN_TEST(routes_laravel_routeserviceprovider_control_issue1146);
    RUN_TEST(routes_laravel_withrouting_nonliteral_prefix_issue1146);
    RUN_TEST(routes_laravel_withrouting_parallel_subdir_issue1146);
    RUN_TEST(handles_laravel_class_handlers_issue1146);
    RUN_TEST(handles_laravel_class_handlers_parallel_issue1146);
    RUN_TEST(handles_laravel_class_handlers_no_composer_issue1146);
    RUN_TEST(handles_laravel_psr4_absent_class_issue1146);
    RUN_TEST(handles_laravel_psr4_absent_class_parallel_issue1146);
    RUN_TEST(routes_laravel_slashless_issue1146);
    RUN_TEST(routes_laravel_slashless_parallel_issue1146);
    RUN_TEST(routes_laravel_slashless_no_guessed_handlers_issue1146);
    RUN_TEST(routes_laravel_slashless_no_junk_issue1146);
    RUN_TEST(handles_rails_ruby);
    RUN_TEST(handles_actix_rust);

    /* HTTP_CALLS — outbound HTTP clients (9 libraries × languages) */
    RUN_TEST(http_calls_fetch_js);
    RUN_TEST(http_calls_axios_ts);
    RUN_TEST(http_calls_requests_python);
    RUN_TEST(http_calls_nethttp_go);
    RUN_TEST(http_calls_resttemplate_java);
    RUN_TEST(http_calls_restsharp_csharp);
    RUN_TEST(http_calls_httparty_ruby);
    RUN_TEST(http_calls_guzzle_php);
    RUN_TEST(http_calls_reqwest_rust);

    /* ASYNC_CALLS — message queue dispatch (5 brokers × languages) */
    RUN_TEST(async_calls_celery_python);
    RUN_TEST(async_calls_celery_wall_stall_seam);
    RUN_TEST(async_calls_sidekiq_ruby);
    RUN_TEST(async_calls_kafkajs_ts);
    RUN_TEST(async_calls_sqs_go);
    RUN_TEST(async_calls_bullmq_js);

    /* THROWS — checked exceptions (7 languages, parallel path) */
    RUN_TEST(throws_java);
    RUN_TEST(throws_kotlin);
    RUN_TEST(throws_python);
    RUN_TEST(throws_typescript);
    RUN_TEST(throws_csharp);
    RUN_TEST(throws_php);
    RUN_TEST(throws_scala);

    /* RAISES — runtime errors (5 languages, parallel path) */
    RUN_TEST(raises_python);
    RUN_TEST(raises_typescript);
    RUN_TEST(raises_kotlin);
    RUN_TEST(raises_csharp);
    RUN_TEST(raises_php);

    /* WRITES — variable assignment (5 languages, parallel path) */
    RUN_TEST(writes_python);
    RUN_TEST(writes_python_explicit_global_missing_is_quality);
    RUN_TEST(writes_go);
    RUN_TEST(writes_java);
    RUN_TEST(writes_rust);
    RUN_TEST(writes_csharp);

    /* DEFINES_METHOD — class→method (9 languages, sequential path) */
    RUN_TEST(defines_method_go);
    RUN_TEST(defines_method_rust);
    RUN_TEST(defines_method_java);
    RUN_TEST(defines_method_csharp);
    RUN_TEST(defines_method_php);
    RUN_TEST(defines_method_ruby);
    RUN_TEST(defines_method_kotlin);
    RUN_TEST(defines_method_typescript);
    RUN_TEST(defines_method_scala);

    /* OVERRIDE — Go interface method override (parallel path) */
    RUN_TEST(override_go_interface);

    /* Route registration vs the #725 cross-language guard (both resolvers) */
    RUN_TEST(routes_laravel_get_survives_xlang_guard);
    RUN_TEST(routes_laravel_get_survives_xlang_guard_parallel);
    RUN_TEST(routes_express_get_survives_xlang_guard);
    RUN_TEST(routes_express_get_survives_xlang_guard_parallel);
}
