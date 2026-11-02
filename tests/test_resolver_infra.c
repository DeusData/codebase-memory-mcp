/* Independent source fixtures for infrastructure proof adapters. Required
 * references are fixed before indexing; every unexpected target is a bug. */
#include "test_framework.h"
#include "test_helpers.h"
#include "foundation/compat.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline.h"
#include "pipeline/shared_resolver.h"
#include "store/store.h"
#include "sqlite3.h"

#include <string.h>

typedef struct {
    const char *path, *source;
} infra_fixture_file_t;

static bool infra_fixture(const infra_fixture_file_t *files, size_t file_count,
                          const char *const *queries, size_t query_count, int *values) {
    const char *root = th_mktempdir("cbm_resolver_infra");
    if (!root)
        return false;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool ok = cbm_mkdir_p(repo, 0755);
    for (size_t i = 0; ok && i < file_count; i++) {
        char path[1024], directory[1024];
        int written = snprintf(path, sizeof(path), "%s/%s", repo, files[i].path);
        ok = written > 0 && (size_t)written < sizeof(path);
        if (!ok)
            break;
        memcpy(directory, path, (size_t)written + 1);
        char *slash = strrchr(directory, '/');
        if (slash)
            *slash = '\0';
        ok = cbm_mkdir_p(directory, 0755) && th_write_file(path, files[i].source) == 0;
    }
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    ok = pipeline && cbm_pipeline_set_project_name(pipeline, "infra-proof") &&
         cbm_pipeline_run(pipeline) == 0;
    cbm_pipeline_free(pipeline);
    cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
    ok = ok && store;
    for (size_t i = 0; ok && i < query_count; i++) {
        sqlite3_stmt *statement = NULL;
        ok = sqlite3_prepare_v2(cbm_store_get_db(store), queries[i], -1, &statement, NULL) ==
                 SQLITE_OK &&
             sqlite3_step(statement) == SQLITE_ROW;
        if (ok)
            values[i] = sqlite3_column_int(statement, 0);
        sqlite3_finalize(statement);
    }
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    return ok;
}

TEST(resolver_infra_terraform_literal_dependencies_are_required) {
    const infra_fixture_file_t files[] = {
        {"main.tf", "module \"lib\" {\n source = \"./lib\"\n}\n"
                    "output \"selected\" {\n value = module.lib.answer\n}\n"},
        {"lib/main.tf", "output \"answer\" {\n value = 1\n}\n"},
        {"decoy/main.tf", "output \"answer\" {\n value = 2\n}\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE s.file_path='main.tf' AND e.type='IMPORTS' AND "
        "t.label='Folder' AND t.file_path='lib'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE s.file_path='main.tf' AND e.type='USAGE' AND "
        "t.file_path='lib/main.tf' AND t.name='output.answer'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE s.file_path='main.tf' AND e.type IN ('IMPORTS','USAGE') AND "
        "t.file_path LIKE 'decoy%'"};
    int found[3] = {-1, -1, -1};
    ASSERT(infra_fixture(files, 3, queries, 3, found));
    ASSERT_EQ(found[0], 1);
    ASSERT_EQ(found[1], 1);
    ASSERT_EQ(found[2], 0);
    PASS();
}

TEST(resolver_infra_terraform_duplicate_and_missing_names_are_counted) {
    const infra_fixture_file_t files[] = {{"main.tf",
                                           "output \"one\" {\n value = var.duplicate\n}\n"
                                           "output \"two\" {\n value = var.missing\n}\n"},
                                          {"first.tf", "variable \"duplicate\" {}\n"},
                                          {"second.tf", "variable \"duplicate\" {}\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE "
        "s.file_path='main.tf' AND e.type='USAGE'",
        "SELECT count(*) FROM resolver_occurrences WHERE file_path='main.tf' AND "
        "relation='USAGE' AND emitted=0 AND binding=2 AND "
        "reason='terraform_duplicate_declaration'",
        "SELECT count(*) FROM resolver_occurrences WHERE file_path='main.tf' AND "
        "relation='USAGE' AND emitted=0 AND binding=0 AND "
        "spelling='var.missing' AND reason='terraform_module_declaration_lookup'"};
    int found[3] = {-1, -1, -1};
    ASSERT(infra_fixture(files, 3, queries, 3, found));
    ASSERT_EQ(found[0], 0);
    ASSERT_EQ(found[1], 1);
    ASSERT_EQ(found[2], 1);
    PASS();
}

TEST(resolver_infra_terraform_conflicting_sources_are_unsupported) {
    const infra_fixture_file_t files[] = {
        {"main.tf", "module \"ambiguous\" {\n source = \"./first\"\n"
                    " source = \"./second\"\n}\n"
                    "module \"dynamic\" {\n source = var.path\n}\n"},
        {"first/main.tf", "output \"answer\" {\n value = 1\n}\n"},
        {"second/main.tf", "output \"answer\" {\n value = 2\n}\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE "
        "s.file_path='main.tf' AND e.type='IMPORTS'",
        "SELECT count(*) FROM resolver_occurrences WHERE file_path='main.tf' AND "
        "relation='IMPORTS' AND emitted=0 AND analysis=0 AND "
        "reason='terraform_literal_module_source'"};
    int found[2] = {-1, -1};
    ASSERT(infra_fixture(files, 3, queries, 2, found));
    ASSERT_EQ(found[0], 0);
    ASSERT_EQ(found[1], 2);
    PASS();
}

TEST(resolver_infra_ansible_duplicate_names_and_listener_membership) {
    const infra_fixture_file_t files[] = {
        {"site.yml", "- hosts: all\n  tasks:\n    - name: ambiguous\n      debug: msg=x\n"
                     "      notify: reload\n    - name: broadcast\n      debug: msg=y\n"
                     "      notify: changed\n  handlers:\n    - name: reload\n      debug: msg=a\n"
                     "    - name: reload\n      debug: msg=b\n"
                     "    - name: first listener\n      debug: msg=c\n      listen: changed\n"
                     "    - name: second listener\n      debug: msg=d\n      listen: changed\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE "
        "s.file_path='site.yml' AND s.name='ambiguous' AND e.type='CALLS'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE s.name='broadcast' AND e.type='CALLS' AND "
        "t.name IN ('first listener','second listener')",
        "SELECT count(*) FROM resolver_occurrences WHERE relation='CALLS' AND emitted=0 "
        "AND binding=2 AND reason='ansible_duplicate_handler_name'"};
    int found[3] = {-1, -1, -1};
    ASSERT(infra_fixture(files, 1, queries, 3, found));
    ASSERT_EQ(found[0], 0);
    ASSERT_EQ(found[1], 2);
    ASSERT_EQ(found[2], 1);
    PASS();
}

TEST(resolver_infra_ansible_plays_and_unknown_dependencies_stay_separate) {
    const infra_fixture_file_t files[] = {
        {"site.yml", "- hosts: first\n  handlers:\n    - name: reload\n      debug: msg=x\n"
                     "- hosts: second\n  tasks:\n    - name: missing handler\n      debug: msg=y\n"
                     "      notify: reload\n    - import_tasks: actual.yml\n"
                     "    - import_tasks: missing.yml\n    - import_tasks: \"{{ dynamic }}\"\n"},
        {"actual.yml", "- name: included\n  debug: msg=z\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges WHERE type='CALLS'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE e.type='IMPORTS' AND s.file_path='site.yml' AND "
        "t.file_path='actual.yml'",
        "SELECT count(*) FROM resolver_occurrences WHERE file_path='site.yml' AND "
        "relation='IMPORTS' AND emitted=0 AND reason='ansible_literal_dependency_lookup'",
        "SELECT count(*) FROM resolver_occurrences WHERE file_path='site.yml' AND "
        "relation='CALLS' AND emitted=0 AND reason='ansible_handler_not_resolved'"};
    int found[4] = {-1, -1, -1, -1};
    ASSERT(infra_fixture(files, 2, queries, 4, found));
    ASSERT_EQ(found[0], 0);
    ASSERT_EQ(found[1], 1);
    ASSERT_EQ(found[2], 2);
    ASSERT_EQ(found[3], 1);
    PASS();
}

TEST(resolver_infra_ansible_duplicate_keys_and_unmodeled_escapes_abstain) {
    const infra_fixture_file_t files[] = {
        {"duplicate.yml", "- hosts: all\n  tasks:\n    - import_tasks: first.yml\n"
                          "      import_tasks: second.yml\n"},
        {"escape.yml", "- hosts: all\n  tasks:\n    - import_tasks: \"\\u0066irst.yml\"\n"},
        {"good.yml", "- hosts: all\n  tasks:\n    - import_tasks: first.yml\n"},
        {"first.yml", "- debug: msg=first\n"},
        {"second.yml", "- debug: msg=second\n"},
        {"u0066irst.yml", "- debug: msg=decoy\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE e.type='IMPORTS' "
        "AND s.file_path IN ('duplicate.yml','escape.yml')",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE e.type='IMPORTS' AND s.file_path='good.yml' AND "
        "t.file_path='first.yml'",
        "SELECT count(*) FROM resolver_occurrences WHERE relation='IMPORTS' AND emitted=0 "
        "AND analysis=0 AND file_path IN ('duplicate.yml','escape.yml') AND "
        "reason='ansible_literal_dependency_lookup'"};
    int found[3] = {-1, -1, -1};
    ASSERT(infra_fixture(files, 6, queries, 3, found));
    ASSERT_EQ(found[0], 0);
    ASSERT_EQ(found[1], 1);
    ASSERT_EQ(found[2], 3);
    PASS();
}

/* ── Route registrations ─────────────────────────────────────────────
 * A Route is no declaration, so these CALLS are the one stated exception to
 * "CALLS means the declaration a language rule selected". A real registration
 * keeps its edge; a client call, a store key or a string method never gets one.
 * Forms are the ones real corpora spell; each driver must agree. */
#define ROUTE_EDGES(path)                                                                     \
    "SELECT count(*) FROM edges e JOIN nodes t ON t.id=e.target_id WHERE e.type='CALLS' AND " \
    "t.label='Route' AND t.name='" path "' AND "                                              \
    "json_extract(e.properties,'$.via')='route_registration'"
#define ROUTE_NODES(path) "SELECT count(*) FROM nodes WHERE label='Route' AND name='" path "'"
#define EDGELESS_ROUTES                                                                  \
    "SELECT count(*) FROM nodes n WHERE label='Route' AND NOT EXISTS (SELECT 1 FROM edges " \
    "e WHERE e.source_id=n.id OR e.target_id=n.id)"

enum { ROUTE_PAD_FILES = 52, ROUTE_MAX_FILES = 64, ROUTE_MAX_CHECKS = 48 };

typedef struct {
    const char *sql;
    int expected;
} route_check_t;

/* Run the fixture through one driver: sequential, or parallel past its file floor. */
static bool route_driver(const infra_fixture_file_t *files, size_t file_count,
                         const route_check_t *checks, size_t check_count, bool parallel) {
    infra_fixture_file_t all[ROUTE_MAX_FILES + ROUTE_PAD_FILES];
    char names[ROUTE_PAD_FILES][32];
    char bodies[ROUTE_PAD_FILES][64];
    if (file_count > ROUTE_MAX_FILES || check_count > ROUTE_MAX_CHECKS)
        return false;
    size_t total = 0;
    for (; total < file_count; total++)
        all[total] = files[total];
    for (int i = 0; parallel && i < ROUTE_PAD_FILES; i++) {
        snprintf(names[i], sizeof(names[i]), "pad/pad%02d.py", i);
        snprintf(bodies[i], sizeof(bodies[i]), "def pad_%02d():\n    return %d\n", i, i);
        all[total++] = (infra_fixture_file_t){names[i], bodies[i]};
    }
    const char *queries[ROUTE_MAX_CHECKS];
    int values[ROUTE_MAX_CHECKS];
    for (size_t i = 0; i < check_count; i++) {
        queries[i] = checks[i].sql;
        values[i] = -1;
    }
    if (parallel) {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
        cbm_setenv("CBM_WORKERS", "4", 1);
    } else {
        cbm_unsetenv("CBM_WORKERS");
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);
    }
    bool ok = infra_fixture(all, total, queries, check_count, values);
    cbm_unsetenv("CBM_WORKERS");
    cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    for (size_t i = 0; ok && i < check_count; i++) {
        if (values[i] != checks[i].expected) {
            fprintf(stderr, "[ROUTES %s] %s: got %d, want %d\n",
                    parallel ? "parallel" : "sequential", checks[i].sql, values[i],
                    checks[i].expected);
            ok = false;
        }
    }
    return ok;
}

static bool route_both_drivers(const infra_fixture_file_t *files, size_t file_count,
                               const route_check_t *checks, size_t check_count) {
    bool sequential = route_driver(files, file_count, checks, check_count, false);
    bool parallel = route_driver(files, file_count, checks, check_count, true);
    return sequential && parallel;
}

TEST(resolver_routes_express_registrations_keep_their_edge) {
    const infra_fixture_file_t files[] = {
        {"server.js",
         "var express = require('express');\n"
         "var app = module.exports = express();\n"
         "var router = express.Router();\n"
         "function listUsers(req, res) { res.json([]); }\n"
         "app.get('/users', listUsers);\n"
         "app.post('/inline', function (req, res) { res.send('x'); });\n"
         "router.use('/mounted', listUsers);\n"
         "app.all('/every', listUsers);\n"
         "app.use(/^\\/regex/, listUsers);\n"
         "router.route('/chained').get(listUsers);\n"},
        {"routes.js",
         "module.exports = function (app) {\n"
         "  function show(req, res) {}\n"
         "  app.get('/param-app', show);\n"
         "};\n"},
        {"fast.js", "const fastify = require('fastify')({ logger: true });\n"
                    "fastify.get('/fast', function (req, reply) {});\n"},
        {"alias.mjs", "import make from 'express';\n"
                      "const server = make();\n"
                      "server.get('/aliased', function (req, res) {});\n"},
        {"probe.test.js",
         "var request = require('supertest');\n"
         "var path = require('path');\n"
         "function probe(app, res, client, http, done) {\n"
         "  request(app).get('/api/users').expect(200, done);\n"
         "  res.redirect('/login');\n"
         "  res.attachment('/path/to/image.png');\n"
         "  path.join('/fixtures', 'a');\n"
         "  client.get('/cfg', done);\n"
         "  http.post('/things', { a: 1 });\n"
         "  new Map().delete('/key');\n"
         "  app.get('/no-handler');\n"
         "}\n"}};
    const route_check_t checks[] = {
        {ROUTE_EDGES("/users"), 1},
        {ROUTE_EDGES("/inline"), 1},
        {ROUTE_EDGES("/mounted"), 1},
        {ROUTE_EDGES("/every"), 1},
        {ROUTE_EDGES("/^\\/regex/"), 1},
        {ROUTE_EDGES("/chained"), 1},
        {ROUTE_EDGES("/param-app"), 1},
        {ROUTE_EDGES("/fast"), 1},
        {ROUTE_EDGES("/aliased"), 1},
        {ROUTE_EDGES("/api/users"), 0},
        {ROUTE_EDGES("/login"), 0},
        {ROUTE_EDGES("/path/to/image.png"), 0},
        {ROUTE_EDGES("/fixtures"), 0},
        {ROUTE_EDGES("/cfg"), 0},
        {ROUTE_EDGES("/things"), 0},
        {ROUTE_EDGES("/key"), 0},
        {ROUTE_EDGES("/no-handler"), 0},
        {ROUTE_NODES("/api/users"), 0},
        {ROUTE_NODES("/login"), 0},
        {ROUTE_NODES("/fixtures"), 0},
        {ROUTE_NODES("/key"), 0},
        {EDGELESS_ROUTES, 0},
        /* A registration's handler keeps its link to the Route. */
        {"SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
         "t.id=e.target_id WHERE e.type='HANDLES' AND s.name='listUsers' AND t.name='/users'",
         1}};
    ASSERT(route_both_drivers(files, 5, checks, sizeof(checks) / sizeof(checks[0])));
    PASS();
}

/* An edge from a registering module is the only link an inline handler has. */
TEST(resolver_routes_inline_handler_route_is_reached_from_its_registrar) {
    const infra_fixture_file_t files[] = {
        {"app.js", "const express = require('express');\nconst app = express();\n"
                   "function start() {\n"
                   "  app.get('/x', function () {});\n"
                   "}\n"}};
    const route_check_t checks[] = {
        {"SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
         "t.id=e.target_id WHERE e.type='CALLS' AND s.name='start' AND t.label='Route' AND "
         "t.name='/x'",
         1},
        {EDGELESS_ROUTES, 0}};
    ASSERT(route_both_drivers(files, 1, checks, 2));
    PASS();
}

/* `express-routes.ts` puts the framework in every QN of the file: a call that
 * resolves there is not a registration because of what its target is named. */
TEST(resolver_routes_a_framework_in_a_qualified_name_proves_nothing) {
    const infra_fixture_file_t files[] = {
        {"express/util.js",
         "function notify(path, cb) { return path; }\n"
         "function fetchIt(url) { return url; }\n"
         "module.exports = { notify, fetchIt };\n"},
        {"express/run.js", "const { notify, fetchIt } = require('./util');\n"
                           "function go(res) {\n"
                           "  notify('/notify', go);\n"
                           "  fetchIt('/fetch');\n"
                           "}\n"}};
    const route_check_t checks[] = {{ROUTE_EDGES("/notify"), 0},
                                    {ROUTE_EDGES("/fetch"), 0},
                                    {EDGELESS_ROUTES, 0}};
    ASSERT(route_both_drivers(files, 2, checks, 3));
    PASS();
}

TEST(resolver_routes_python_decorators_and_framework_methods) {
    const infra_fixture_file_t files[] = {
        {"web.py", "from flask import Flask, Blueprint\n"
                   "from fastapi import FastAPI, APIRouter\n"
                   "app = Flask(__name__)\n"
                   "bp = Blueprint('b', __name__)\n"
                   "router = APIRouter()\n"
                   "api = FastAPI()\n"
                   "@app.route('/home')\ndef home():\n    return 1\n"
                   "@bp.get('/bp-get')\ndef bp_get():\n    return 1\n"
                   "@router.post('/router-post')\ndef router_post():\n    return 1\n"
                   "def view():\n    return 1\n"
                   "app.add_url_rule('/rule', 'view', view)\n"
                   "api.include_router(router, prefix='/prefix')\n"
                   "api.mount('/static', view)\n"},
        {"tests/test_app.py", "def test_route(app, client, rf):\n"
                              "    @app.route('/fixture')\n"
                              "    def index():\n"
                              "        return 1\n"
                              "    client.get('/c1')\n"
                              "    client.post('/c2', data={'a': 1})\n"
                              "    app.test_client().get('/c3')\n"
                              "    rf.delete('/c5')\n"
                              "    flask.redirect('/c6')\n"}};
    const route_check_t checks[] = {
        {ROUTE_EDGES("/home"), 1},      {ROUTE_EDGES("/bp-get"), 1},
        {ROUTE_EDGES("/router-post"), 1}, {ROUTE_EDGES("/rule"), 1},
        {ROUTE_EDGES("/prefix"), 1},    {ROUTE_EDGES("/static"), 1},
        {ROUTE_EDGES("/fixture"), 1},   {ROUTE_EDGES("/c1"), 0},
        {ROUTE_EDGES("/c2"), 0},        {ROUTE_EDGES("/c3"), 0},
        {ROUTE_EDGES("/c5"), 0},        {ROUTE_EDGES("/c6"), 0},
        {ROUTE_NODES("/c1"), 0},        {ROUTE_NODES("/c6"), 0},
        {EDGELESS_ROUTES, 0}};
    ASSERT(route_both_drivers(files, 2, checks, sizeof(checks) / sizeof(checks[0])));
    PASS();
}

TEST(resolver_routes_go_receivers_are_bound_by_the_file) {
    const infra_fixture_file_t files[] = {
        {"main.go",
         "package main\n\n"
         "import (\n\t\"net/http\"\n\t\"github.com/gin-gonic/gin\"\n"
         "\t\"github.com/go-chi/chi/v5\"\n)\n\n"
         "func h(w http.ResponseWriter, r *http.Request) {}\n"
         "func gh(c *gin.Context) {}\n\n"
         "func register(prefix string, count int, r chi.Router) {\n\tr.Get(\"/chi\", h)\n}\n\n"
         "func main() {\n"
         "\tmux := http.NewServeMux()\n"
         "\tmux.HandleFunc(\"/mux\", h)\n"
         "\thttp.Handle(\"/std\", http.HandlerFunc(h))\n"
         "\tg := gin.Default()\n"
         "\tv1 := g.Group(\"/v1\")\n"
         "\tv1.GET(\"/gin\", gh)\n"
         "\tws := new(restful.WebService)\n"
         "\tws.Route(ws.GET(\"/restful\").To(gh))\n"
         "\tother.GET(\"/not-a-builder\").Do(gh)\n"
         "}\n"},
        {"client_test.go",
         "package main\n\n"
         "import \"net/http\"\n\n"
         "func probe(s store, c client) {\n"
         "\thttp.Get(\"/x1\")\n"
         "\ts.Get(\"/x2\", false, false)\n"
         "\tc.Get(ctx, \"/x3\")\n"
         "\tc.Post(\"/x4\", \"application/json\", body)\n"
         "\ts.Delete(\"/x5\", false, false)\n"
         "}\n"}};
    const route_check_t checks[] = {
        {ROUTE_EDGES("/restful"), 1},
        {ROUTE_EDGES("/not-a-builder"), 0},
        {ROUTE_EDGES("/chi"), 1},    {ROUTE_EDGES("/mux"), 1}, {ROUTE_EDGES("/std"), 1},
        {ROUTE_EDGES("/v1/gin"), 1}, {ROUTE_EDGES("/x1"), 0},  {ROUTE_EDGES("/x2"), 0},
        {ROUTE_EDGES("/x3"), 0},     {ROUTE_EDGES("/x4"), 0},  {ROUTE_EDGES("/x5"), 0},
        {ROUTE_NODES("/x2"), 0},     {ROUTE_NODES("/x5"), 0},  {EDGELESS_ROUTES, 0}};
    ASSERT(route_both_drivers(files, 2, checks, sizeof(checks) / sizeof(checks[0])));
    PASS();
}

TEST(resolver_routes_framework_forms_in_other_languages) {
    const infra_fixture_file_t files[] = {
        {"routes/web.php", "<?php\n"
                           "use Illuminate\\Support\\Facades\\Route;\n"
                           "Route::get('/php', [HomeController::class, 'index']);\n"
                           "Route::middleware('auth')->post('/php-chain', [C::class, 'store']);\n"
                           "Http::get('/php-client');\n"
                           "Cache::get('/php-cache', $fallback);\n"
                           "$client->get('/php-var', $options);\n"},
        {"Program.cs", "var app = WebApplication.Create(args);\n"
                       "app.MapGet(\"/cs\", () => \"x\");\n"
                       "client.Get(\"/cs-client\");\n"},
        {"main.rs", "fn routes() -> Router {\n"
                    "    Router::new().route(\"/rs\", get(handler))\n"
                    "}\n"
                    "fn call(c: Client) {\n"
                    "    c.post(\"/rs-client\", body);\n"
                    "}\n"},
        {"App.java", "class App {\n"
                     "  void run(Components c) {\n"
                     "    RoutingDsl.fromComponents(c).GET(\"/java\").routingTo(r -> ok());\n"
                     "    routingDsl.POST(\"/java-field\").routingTo(r -> ok());\n"
                     "    Paths.get(\"/tmp/picture\", name);\n"
                     "  }\n"
                     "}\n"},
        {"lib/thing.rb", "class Thing\n"
                         "  def strip(title)\n"
                         "    title.delete('/', keep)\n"
                         "  end\n"
                         "end\n"}};
    const route_check_t checks[] = {
        {ROUTE_EDGES("/php"), 1},          {ROUTE_EDGES("/php-chain"), 1},
        {ROUTE_EDGES("/cs"), 1},           {ROUTE_EDGES("/rs"), 1},
        {ROUTE_EDGES("/java"), 1},         {ROUTE_EDGES("/java-field"), 1},
        {ROUTE_EDGES("/php-client"), 0},
        {ROUTE_EDGES("/php-cache"), 0},    {ROUTE_EDGES("/php-var"), 0},
        {ROUTE_EDGES("/cs-client"), 0},    {ROUTE_EDGES("/rs-client"), 0},
        {ROUTE_EDGES("/tmp/picture"), 0},  {ROUTE_EDGES("/"), 0},
        {EDGELESS_ROUTES, 0}};
    ASSERT(route_both_drivers(files, 5, checks, sizeof(checks) / sizeof(checks[0])));
    PASS();
}

/* Sinatra's DSL has no receiver: its block is the handler and the file names the
 * framework. A rack-test `get '/x'` has no block, and elsewhere `get` means nothing. */
TEST(resolver_routes_sinatra_dsl_needs_the_framework_and_a_block) {
    const infra_fixture_file_t files[] = {
        {"app.rb", "require 'sinatra/base'\n"
                   "class App < Sinatra::Base\n"
                   "  get '/hello' do\n    'hi'\n  end\n"
                   "  post('/form') { 'ok' }\n"
                   "end\n"},
        {"app_test.rb", "require 'sinatra/base'\n"
                        "class AppTest < Minitest::Test\n"
                        "  def test_it\n    get '/probe'\n    post '/submit', name: 'x'\n  end\n"
                        "end\n"},
        {"lib/other.rb", "class Other\n"
                         "  def go\n    get('/unrelated') { 1 }\n  end\n"
                         "end\n"}};
    const route_check_t checks[] = {
        {ROUTE_EDGES("/hello"), 1},    {ROUTE_EDGES("/form"), 1},
        {ROUTE_EDGES("/probe"), 0},    {ROUTE_EDGES("/submit"), 0},
        {ROUTE_EDGES("/unrelated"), 0}, {ROUTE_NODES("/unrelated"), 0},
        {"SELECT count(*) FROM nodes WHERE label='Route' AND qualified_name LIKE "
         "'__route__GET__/hello'",
         1},
        {EDGELESS_ROUTES, 0}};
    ASSERT(route_both_drivers(files, 3, checks, sizeof(checks) / sizeof(checks[0])));
    PASS();
}

/* A handler written as a method value has no name the registry can bind. What
 * the resolver proved inside the registration's span is its HANDLES target. */
TEST(resolver_routes_method_value_handler_comes_from_the_proved_reference) {
    const infra_fixture_file_t files[] = {
        {"main.go", "package main\n\n"
                    "import \"net/http\"\n\n"
                    "type Server struct{}\n\n"
                    "func (s *Server) Health(w http.ResponseWriter, r *http.Request) {}\n\n"
                    "type Other struct{}\n\n"
                    "func (o *Other) Stats(w http.ResponseWriter, r *http.Request) {}\n\n"
                    "func run(unknown Unknown) {\n"
                    "\ts := &Server{}\n"
                    "\tmux := http.NewServeMux()\n"
                    "\tmux.HandleFunc(\"/health\", s.Health)\n"
                    "\tmux.HandleFunc(\"/stats\", unknown.Stats)\n"
                    "}\n"}};
#define HANDLES_INTO(handler, route)                                                     \
    "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "     \
    "t.id=e.target_id WHERE e.type='HANDLES' AND s.name='" handler "' AND t.name='" route "'"
    const route_check_t checks[] = {{ROUTE_EDGES("/health"), 1},
                                    {HANDLES_INTO("Health", "/health"), 1},
                                    /* a real route, a receiver of no known type: no name guess */
                                    {ROUTE_EDGES("/stats"), 1},
                                    {HANDLES_INTO("Stats", "/stats"), 0}};
    ASSERT(route_both_drivers(files, 1, checks, 4));
    PASS();
}

/* The binding is read from the AST's call site, never from text a comment or a
 * string could hold. */
TEST(resolver_routes_a_commented_binding_binds_nothing) {
    const infra_fixture_file_t files[] = {
        {"probe.js", "// var api = express();\n"
                     "var note = 'var api = express();';\n"
                     "function go(api, h) {\n"
                     "  api.get('/commented', h);\n"
                     "}\n"}};
    const route_check_t checks[] = {{ROUTE_EDGES("/commented"), 0}, {ROUTE_NODES("/commented"), 0}};
    ASSERT(route_both_drivers(files, 1, checks, 2));
    PASS();
}

/* Projection keeps a registration by its producer's marker AND the target's label.
 * Each near miss below is an unproven controlled edge and must go. */
TEST(resolver_routes_projection_exempts_only_the_marked_edge_into_a_route) {
    cbm_gbuf_t *graph = cbm_gbuf_new("proj", "/tmp");
    ASSERT_NOT_NULL(graph);
    int64_t fn = cbm_gbuf_upsert_node(graph, "Function", "f", "proj.f", "a.js", 1, 2, "{}");
    int64_t other = cbm_gbuf_upsert_node(graph, "Function", "g", "proj.g", "a.js", 3, 4, "{}");
    int64_t third = cbm_gbuf_upsert_node(graph, "Function", "h", "proj.h", "a.js", 5, 6, "{}");
    int64_t real = cbm_gbuf_upsert_node(graph, "Route", "/real", "__route__GET__/real", "", 0, 0, "{}");
    int64_t bare = cbm_gbuf_upsert_node(graph, "Route", "/bare", "__route__GET__/bare", "", 0, 0, "{}");
    int64_t wrong = cbm_gbuf_upsert_node(graph, "Route", "/wrong", "__route__GET__/wrong", "", 0, 0, "{}");
    int64_t nested = cbm_gbuf_upsert_node(graph, "Route", "/nested", "__route__GET__/nested", "", 0, 0, "{}");
    int64_t quoted = cbm_gbuf_upsert_node(graph, "Route", "/quoted", "__route__GET__/quoted", "", 0, 0, "{}");
    static const char marked[] = "{\"callee\":\"app.get\",\"via\":\"route_registration\"}";
    ASSERT(cbm_gbuf_insert_edge(graph, fn, real, "CALLS", marked) > 0);
    /* An ordinary call forged with the marker: its target is no Route. */
    ASSERT(cbm_gbuf_insert_edge(graph, fn, other, "CALLS", marked) > 0);
    /* A Route without the marker, and with another producer's. */
    ASSERT(cbm_gbuf_insert_edge(graph, fn, bare, "CALLS", "{\"callee\":\"x.get\"}") > 0);
    ASSERT(cbm_gbuf_insert_edge(graph, fn, wrong, "CALLS", "{\"via\":\"arg_url\"}") > 0);
    /* The marker only inside another value, escaped or nested. */
    ASSERT(cbm_gbuf_insert_edge(graph, fn, nested, "CALLS",
                                "{\"args\":{\"via\":\"route_registration\"}}") > 0);
    ASSERT(cbm_gbuf_insert_edge(graph, fn, quoted, "CALLS",
                                "{\"callee\":\"\\\"via\\\":\\\"route_registration\\\"\"}") > 0);
    /* Not a CALLS edge at all. */
    ASSERT(cbm_gbuf_insert_edge(graph, fn, real, "USAGE", marked) > 0);
    /* A plain ordinary call. */
    ASSERT(cbm_gbuf_insert_edge(graph, fn, third, "CALLS", "{}") > 0);
    /* A relation projection does not control stays. */
    ASSERT(cbm_gbuf_insert_edge(graph, fn, real, "HANDLES", "{}") > 0);
    ASSERT(cbm_shared_resolver_project(graph));
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(graph, "CALLS"), 1);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(graph, "USAGE"), 0);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(graph, "HANDLES"), 1);
    const cbm_gbuf_edge_t **kept = NULL;
    int count = 0;
    ASSERT_EQ(cbm_gbuf_find_edges_by_target_type(graph, real, "CALLS", &kept, &count), 0);
    ASSERT_EQ(count, 1);
    /* Projection runs twice in a full index: the second pass keeps it too. */
    ASSERT(cbm_shared_resolver_project(graph));
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(graph, "CALLS"), 1);
    cbm_gbuf_free(graph);
    PASS();
}

/* ── Producers outside the language resolvers ───────────────────────── */

/* An empty file declares nothing. One used to abort the whole index. */
TEST(resolver_infra_empty_files_do_not_abort_the_index) {
    const infra_fixture_file_t files[] = {
        {"tf/empty.tf", ""},
        {"tf/main.tf", "variable \"region\" {}\nresource \"aws_vpc\" \"main\" {\n"
                       "  cidr_block = var.region\n}\n"},
        {"tg/terragrunt.hcl", ""},
        {"k/kustomization.yaml", ""},
        {"c/docker-compose.yml", ""},
        {"h/Chart.yaml", ""},
        {"h/templates/_helpers.tpl", ""},
        {"roles/r/tasks/main.yml", ""},
        {"roles/r/handlers/main.yml", ""},
        {"Dockerfile", ""}};
    const char *queries[] = {"SELECT count(*) FROM edges e JOIN nodes t ON t.id=e.target_id "
                             "WHERE e.type='USAGE' AND t.name='variable.region'",
                             "SELECT count(*) FROM nodes WHERE file_path='tf/empty.tf'"};
    int found[2] = {-1, -1};
    ASSERT(infra_fixture(files, sizeof(files) / sizeof(files[0]), queries, 2, found));
    ASSERT_EQ(found[0], 1);
    PASS();
}

/* A handler in one role that cannot be named closes that role's lookups only. */
TEST(resolver_infra_ansible_handler_gap_stays_in_its_scope) {
    const infra_fixture_file_t files[] = {
        {"roles/gap/handlers/main.yml",
         "- name: \"restart {{ service }}\"\n  debug: msg=x\n- name: reload\n  debug: msg=y\n"},
        {"roles/gap/tasks/main.yml",
         "- name: in gap\n  debug: msg=a\n  notify: reload\n"},
        {"roles/ok/handlers/main.yml", "- name: reload\n  debug: msg=b\n"},
        {"roles/ok/tasks/main.yml", "- name: in ok\n  debug: msg=c\n  notify: reload\n"},
        {"site.yml", "- hosts: all\n  roles:\n    - gap\n    - ok\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE e.type='CALLS' "
        "AND s.name='in ok'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE e.type='CALLS' "
        "AND s.name='in gap'"};
    int found[2] = {-1, -1};
    ASSERT(infra_fixture(files, 5, queries, 2, found));
    ASSERT_EQ(found[0], 1);
    /* The unnamed handler could be the one notified: its own role abstains. */
    ASSERT_EQ(found[1], 0);
    PASS();
}

#define K8S_USAGE(source, target)                                                               \
    "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "            \
    "t.id=e.target_id WHERE e.type='USAGE' AND s.label='Resource' AND t.label='Resource' AND " \
    "s.name='" source "' AND t.name='" target "'"

/* The file's own document stream is its apply unit; a kustomization or a chart
 * declares a build across files; a directory declares nothing. */
TEST(resolver_infra_k8s_references_name_one_object_in_a_declared_scope) {
    const infra_fixture_file_t files[] = {
        /* the same document stream */
        {"stream/app.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: in-stream\nspec:\n  template:\n"
         "    spec:\n      serviceAccountName: stream-sa\n      containers:\n      - name: c\n"
         "        image: x/y:1\n---\napiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: "
         "stream-sa\n---\napiVersion: rbac.authorization.k8s.io/v1\nkind: RoleBinding\nmetadata:\n"
         "  name: stream-rb\nroleRef:\n  apiGroup: rbac.authorization.k8s.io\n  kind: Role\n"
         "  name: stream-role\nsubjects:\n- kind: ServiceAccount\n  name: stream-sa\n---\n"
         "apiVersion: rbac.authorization.k8s.io/v1\nkind: Role\nmetadata:\n  name: stream-role\n"},
        /* a kustomization across files */
        {"built/kustomization.yaml", "resources:\n- deploy.yaml\n- sa.yaml\n"},
        {"built/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: in-build\nspec:\n  template:\n"
         "    spec:\n      serviceAccountName: built-sa\n      containers:\n      - name: c\n"
         "        image: x/y:1\n"},
        {"built/sa.yaml", "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: built-sa\n"},
        /* a directory: no manifest says these deploy together */
        {"bare/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: in-dir\nspec:\n  template:\n"
         "    spec:\n      serviceAccountName: dir-sa\n      containers:\n      - name: c\n"
         "        image: x/y:1\n"},
        {"bare/sa.yaml", "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: dir-sa\n"},
        /* a name in another namespace is another object */
        {"ns/app.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: in-ns\n  namespace: one\nspec:\n"
         "  template:\n    spec:\n      serviceAccountName: ns-sa\n      containers:\n"
         "      - name: c\n        image: x/y:1\n---\napiVersion: v1\nkind: ServiceAccount\n"
         "metadata:\n  name: ns-sa\n  namespace: two\n"},
        /* two declarations in one build are alternatives */
        {"twice/kustomization.yaml", "resources:\n- deploy.yaml\n- one.yaml\n- two.yaml\n"},
        {"twice/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: in-twice\nspec:\n  template:\n"
         "    spec:\n      serviceAccountName: twice-sa\n      containers:\n      - name: c\n"
         "        image: x/y:1\n"},
        {"twice/one.yaml", "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: twice-sa\n"},
        {"twice/two.yaml", "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: twice-sa\n"}};
    const char *queries[] = {K8S_USAGE("Deployment/in-stream", "ServiceAccount/stream-sa"),
                             K8S_USAGE("RoleBinding/stream-rb", "Role/stream-role"),
                             K8S_USAGE("RoleBinding/stream-rb", "ServiceAccount/stream-sa"),
                             K8S_USAGE("Deployment/in-build", "ServiceAccount/built-sa"),
                             K8S_USAGE("Deployment/in-dir", "ServiceAccount/dir-sa"),
                             K8S_USAGE("Deployment/in-ns", "ServiceAccount/ns-sa"),
                             K8S_USAGE("Deployment/in-twice", "ServiceAccount/twice-sa")};
    const int expected[] = {1, 1, 1, 1, 0, 0, 0};
    int found[7];
    for (int i = 0; i < 7; i++)
        found[i] = -1;
    ASSERT(infra_fixture(files, sizeof(files) / sizeof(files[0]), queries, 7, found));
    for (int i = 0; i < 7; i++) {
        if (found[i] != expected[i])
            fprintf(stderr, "[K8S] %s: got %d, want %d\n", queries[i], found[i], expected[i]);
        ASSERT_EQ(found[i], expected[i]);
    }
    PASS();
}

/* The decoys test_resolver_k8s pins at occurrence level, at edge level: a namespace
 * only one side declares, a file's own copy against a declared build's, and a build
 * this reader cannot enumerate. */
TEST(resolver_infra_k8s_references_abstain_on_their_decoys) {
    const infra_fixture_file_t files[] = {
        {"unset/kustomization.yaml", "resources:\n- account.yaml\n- secret.yaml\n"},
        {"unset/account.yaml", "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: unset\n"
                               "imagePullSecrets:\n- name: unset-secret\n"},
        {"unset/secret.yaml", "apiVersion: v1\nkind: Secret\nmetadata:\n  name: unset-secret\n"
                              "  namespace: app\n"},
        {"both/kustomization.yaml", "resources:\n- account.yaml\n- secret.yaml\n"},
        {"both/account.yaml", "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: both\n"
                              "  namespace: app\nimagePullSecrets:\n- name: both-secret\n---\n"
                              "apiVersion: v1\nkind: Secret\nmetadata:\n  name: both-secret\n"
                              "  namespace: app\n"},
        {"both/secret.yaml", "apiVersion: v1\nkind: Secret\nmetadata:\n  name: both-secret\n"
                             "  namespace: app\n"},
        {"kustomization.yaml", "resources:\n- open/account.yaml\n- root-secret.yaml\n"},
        {"root-secret.yaml", "apiVersion: v1\nkind: Secret\nmetadata:\n  name: open-secret\n"
                             "  namespace: app\n"},
        {"open/kustomization.yaml", "resources: *manifest_files\n"},
        {"open/account.yaml", "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: open\n"
                              "  namespace: app\nimagePullSecrets:\n- name: open-secret\n"},
        {"open/secret-b.yaml", "apiVersion: v1\nkind: Secret\nmetadata:\n  name: open-secret\n"
                               "  namespace: app\n"}};
#define K8S_OUT_OF(source)                                                                     \
    "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE e.type='USAGE' AND " \
    "s.name='" source "'"
    const char *queries[] = {K8S_OUT_OF("ServiceAccount/unset"), K8S_OUT_OF("ServiceAccount/both"),
                             K8S_OUT_OF("ServiceAccount/open")};
    int found[3] = {-1, -1, -1};
    ASSERT(infra_fixture(files, sizeof(files) / sizeof(files[0]), queries, 3, found));
    ASSERT_EQ(found[0], 0);
    ASSERT_EQ(found[1], 0);
    ASSERT_EQ(found[2], 0);
    PASS();
}

/* A cluster has one CRD per group and kind. A sole copy is that CRD; a declared
 * build selects among copies; otherwise several copies draw nothing. */
TEST(resolver_infra_k8s_custom_resource_finds_its_crd_by_identity) {
#define CRD_DOC(group, kind, plural)                                                         \
    "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata:\n  name: " \
    plural "." group "\nspec:\n  group: " group "\n  names:\n    kind: " kind                  \
           "\n    plural: " plural "\n  scope: Namespaced\n"
    const infra_fixture_file_t files[] = {
        {"crds/sole.yaml", CRD_DOC("a.example.com", "Sole", "soles")},
        {"far/away/sole-cr.yaml",
         "apiVersion: a.example.com/v1\nkind: Sole\nmetadata:\n  name: s1\n"},
        {"one/crd.yaml", CRD_DOC("b.example.com", "Copied", "copieds")},
        {"two/crd.yaml", CRD_DOC("b.example.com", "Copied", "copieds")},
        {"three/copied-cr.yaml",
         "apiVersion: b.example.com/v1\nkind: Copied\nmetadata:\n  name: c1\n"},
        {"sel/crd.yaml", CRD_DOC("c.example.com", "Picked", "pickeds")},
        {"other/crd.yaml", CRD_DOC("c.example.com", "Picked", "pickeds")},
        {"sel/picked-cr.yaml",
         "apiVersion: c.example.com/v1\nkind: Picked\nmetadata:\n  name: p1\n"},
        {"sel/kustomization.yaml", "resources:\n- crd.yaml\n- picked-cr.yaml\n"},
        /* the object's own build holds two copies, so it selects neither */
        {"dup/one.yaml", CRD_DOC("d.example.com", "Dup", "dups")},
        {"dup/two.yaml", CRD_DOC("d.example.com", "Dup", "dups")},
        {"dup/dup-cr.yaml", "apiVersion: d.example.com/v1\nkind: Dup\nmetadata:\n  name: d1\n"},
        {"dup/kustomization.yaml", "resources:\n- one.yaml\n- two.yaml\n- dup-cr.yaml\n"},
        /* a same-named kind of another group is another type */
        {"grp/other-group-cr.yaml",
         "apiVersion: z.example.com/v1\nkind: Sole\nmetadata:\n  name: z1\n"}};
    const char *queries[] = {
        K8S_USAGE("Sole/s1", "CustomResourceDefinition/soles.a.example.com"),
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE e.type='USAGE' AND "
        "s.name='Copied/c1'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE e.type='USAGE' AND s.name='Picked/p1' AND t.file_path='sel/crd.yaml'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE e.type='USAGE' AND s.name='Picked/p1' AND "
        "t.file_path='other/crd.yaml'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE e.type='USAGE' AND "
        "s.name='Sole/z1'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE e.type='USAGE' AND "
        "s.name='Dup/d1'"};
    const int expected[] = {1, 0, 1, 0, 0, 0};
    int found[6] = {-1, -1, -1, -1, -1, -1};
    ASSERT(infra_fixture(files, sizeof(files) / sizeof(files[0]), queries, 6, found));
    for (int i = 0; i < 6; i++) {
        if (found[i] != expected[i])
            fprintf(stderr, "[CRD] %s: got %d, want %d\n", queries[i], found[i], expected[i]);
        ASSERT_EQ(found[i], expected[i]);
    }
    PASS();
}

/* A named template is the chart family's sole `define` of that name. */
TEST(resolver_infra_helm_include_binds_a_sole_definition_in_its_family) {
    const infra_fixture_file_t files[] = {
        {"app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n"},
        {"app/templates/_helpers.tpl",
         "{{- define \"app.fullname\" -}}app{{- end }}\n"
         "{{- define \"app.twice\" -}}a{{- end }}\n"},
        {"app/templates/_more.tpl", "{{- define \"app.twice\" -}}b{{- end }}\n"},
        {"app/templates/deployment.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n"
         "  labels:\n    a: {{ include \"app.fullname\" . }}\n"
         "    b: {{ include \"app.twice\" . }}\n"
         "    c: {{ include \"other.only\" . }}\n"},
        {"other/Chart.yaml", "apiVersion: v2\nname: other\nversion: 0.1.0\n"},
        {"other/templates/_helpers.tpl", "{{- define \"other.only\" -}}o{{- end }}\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes t ON t.id=e.target_id WHERE e.type='CALLS' AND "
        "t.name='app.fullname' AND json_extract(e.properties,'$.strategy')='helm_include'",
        "SELECT count(*) FROM edges e JOIN nodes t ON t.id=e.target_id WHERE e.type='CALLS' AND "
        "t.name='app.twice'",
        "SELECT count(*) FROM edges e JOIN nodes t ON t.id=e.target_id WHERE e.type='CALLS' AND "
        "t.name='other.only'"};
    int found[3] = {-1, -1, -1};
    ASSERT(infra_fixture(files, 6, queries, 3, found));
    ASSERT_EQ(found[0], 1);
    ASSERT_EQ(found[1], 0);
    ASSERT_EQ(found[2], 0);
    PASS();
}

/* A view's lineage: the relation its own script creates. Another script's
 * same-named relation is reached only through a search path this index lacks. */
TEST(resolver_infra_sql_lineage_binds_within_its_script) {
    const infra_fixture_file_t files[] = {
        {"schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n"
                       "CREATE VIEW active_users AS SELECT id FROM users;\n"
                       "CREATE VIEW names AS SELECT name FROM active_users;\n"},
        {"other.sql", "CREATE VIEW remote_users AS SELECT id FROM users;\n"},
        {"app.py", "def users():\n    return 1\n\ndef caller():\n    return users()\n"},
        /* orders has no other declaration, so only the script boundary keeps this out */
        {"orders.sql", "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n"},
        {"report.sql", "CREATE VIEW remote_orders AS SELECT id FROM orders;\n"},
        /* a dbt model binds across files by dbt's own ref() */
        {"stg_items.sql", "SELECT id FROM {{ source('raw', 'items') }}\n"},
        {"items_enriched.sql", "SELECT id FROM {{ ref('stg_items') }}\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE e.type='USAGE' AND s.name='active_users' AND t.name='users' AND "
        "t.label='Table'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE e.type='USAGE' AND s.name='names' AND t.name='active_users' AND "
        "t.label='View'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE e.type='USAGE' AND "
        "s.name='remote_users'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE s.name='caller' AND t.label='Table'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE e.type='USAGE' AND "
        "s.name='remote_orders'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE e.type='USAGE' AND s.name='items_enriched' AND "
        "t.name='stg_items' AND t.label='Model'"};
    int found[6] = {-1, -1, -1, -1, -1, -1};
    ASSERT(infra_fixture(files, 7, queries, 6, found));
    ASSERT_EQ(found[0], 1);
    ASSERT_EQ(found[1], 1);
    ASSERT_EQ(found[2], 0);
    ASSERT_EQ(found[3], 0);
    ASSERT_EQ(found[4], 0);
    ASSERT_EQ(found[5], 1);
    PASS();
}

/* A .env value the reader cannot follow spoils that variable, not the file. */
TEST(resolver_infra_compose_env_gap_stays_with_its_variable) {
    const infra_fixture_file_t files[] = {
        {".env", "IMAGE_NAME=ghcr.io/acme/demo\nDEMO_VERSION=1.2\n"
                 "ADDR=http://${UNSET_HOST}:80\nOPAQUE=http://${UNSET_HOST}\n"},
        {"compose.yaml", "services:\n"
                         "  known:\n    image: ${IMAGE_NAME}:${DEMO_VERSION}-ad\n"
                         "  spoiled:\n    image: ${OPAQUE:-acme/fallback}\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE e.type='USAGE' AND s.name='Service/known' AND "
        "t.name='ghcr.io/acme/demo'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE e.type='USAGE' AND "
        "s.name='Service/spoiled'"};
    int found[2] = {-1, -1};
    ASSERT(infra_fixture(files, 2, queries, 2, found));
    ASSERT_EQ(found[0], 1);
    ASSERT_EQ(found[1], 0);
    PASS();
}

/* Every row of projection's table needs its marker and its labels together. */
TEST(resolver_infra_projection_rows_need_marker_and_labels_together) {
    cbm_gbuf_t *graph = cbm_gbuf_new("proj", "/tmp");
    ASSERT_NOT_NULL(graph);
    int64_t fn = cbm_gbuf_upsert_node(graph, "Function", "f", "proj.f", "a.py", 1, 2, "{}");
    int64_t res = cbm_gbuf_upsert_node(graph, "Resource", "R/r", "proj.r", "a.yaml", 1, 2, "{}");
    int64_t res2 = cbm_gbuf_upsert_node(graph, "Resource", "R/s", "proj.s", "a.yaml", 3, 4, "{}");
    int64_t var = cbm_gbuf_upsert_node(graph, "Variable", "v", "proj.v", "a.py", 3, 3, "{}");
    int64_t tpl = cbm_gbuf_upsert_node(graph, "Function", "t", "proj.t", "a.tpl", 1, 1, "{}");
    int64_t tbl = cbm_gbuf_upsert_node(graph, "Table", "tb", "proj.tb", "a.sql", 1, 1, "{}");
    int64_t mod = cbm_gbuf_upsert_node(graph, "Module", "m", "proj.m", "v.yaml", 1, 1, "{}");
    static const char helm[] = "{\"strategy\":\"helm_include\"}";
    static const char lineage[] = "{\"via\":\"sql_relation\"}";
    static const char values[] = "{\"via\":\"helm_release_values\"}";
    /* kept */
    ASSERT(cbm_gbuf_insert_edge(graph, res, tpl, "CALLS", helm) > 0);
    ASSERT(cbm_gbuf_insert_edge(graph, res, res2, "USAGE", "{\"ref\":\"crd\"}") > 0);
    ASSERT(cbm_gbuf_insert_edge(graph, fn, tbl, "USAGE", lineage) > 0);
    ASSERT(cbm_gbuf_insert_edge(graph, res, mod, "IMPORTS", values) > 0);
    /* gone: the marker on the wrong target, the wrong source, or the wrong relation */
    ASSERT(cbm_gbuf_insert_edge(graph, fn, var, "CALLS", helm) > 0);
    ASSERT(cbm_gbuf_insert_edge(graph, fn, res, "USAGE", "{\"ref\":\"crd\"}") > 0);
    ASSERT(cbm_gbuf_insert_edge(graph, res, fn, "USAGE", "{\"ref\":\"crd\"}") > 0);
    ASSERT(cbm_gbuf_insert_edge(graph, fn, var, "USAGE", lineage) > 0);
    ASSERT(cbm_gbuf_insert_edge(graph, fn, tbl, "READS", lineage) > 0);
    ASSERT(cbm_gbuf_insert_edge(graph, fn, tpl, "IMPORTS", values) > 0);
    /* gone: the labels without the marker */
    ASSERT(cbm_gbuf_insert_edge(graph, res2, res, "USAGE", "{}") > 0);
    ASSERT(cbm_gbuf_insert_edge(graph, fn, tbl, "CALLS", "{}") > 0);
    ASSERT(cbm_gbuf_insert_edge(graph, fn, tpl, "CALLS", "{\"strategy\":\"unique_name\"}") > 0);
    ASSERT(cbm_gbuf_insert_edge(graph, fn, mod, "IMPORTS", "{\"local_name\":\"values\"}") > 0);
    ASSERT(cbm_shared_resolver_project(graph));
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(graph, "CALLS"), 1);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(graph, "USAGE"), 2);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(graph, "IMPORTS"), 1);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(graph, "READS"), 0);
    cbm_gbuf_free(graph);
    PASS();
}

SUITE(resolver_infra) {
    RUN_TEST(resolver_infra_terraform_literal_dependencies_are_required);
    RUN_TEST(resolver_infra_terraform_duplicate_and_missing_names_are_counted);
    RUN_TEST(resolver_infra_terraform_conflicting_sources_are_unsupported);
    RUN_TEST(resolver_infra_ansible_duplicate_names_and_listener_membership);
    RUN_TEST(resolver_infra_ansible_plays_and_unknown_dependencies_stay_separate);
    RUN_TEST(resolver_infra_ansible_duplicate_keys_and_unmodeled_escapes_abstain);
    RUN_TEST(resolver_routes_express_registrations_keep_their_edge);
    RUN_TEST(resolver_routes_inline_handler_route_is_reached_from_its_registrar);
    RUN_TEST(resolver_routes_a_framework_in_a_qualified_name_proves_nothing);
    RUN_TEST(resolver_routes_python_decorators_and_framework_methods);
    RUN_TEST(resolver_routes_go_receivers_are_bound_by_the_file);
    RUN_TEST(resolver_routes_framework_forms_in_other_languages);
    RUN_TEST(resolver_routes_sinatra_dsl_needs_the_framework_and_a_block);
    RUN_TEST(resolver_routes_method_value_handler_comes_from_the_proved_reference);
    RUN_TEST(resolver_routes_a_commented_binding_binds_nothing);
    RUN_TEST(resolver_routes_projection_exempts_only_the_marked_edge_into_a_route);
    RUN_TEST(resolver_infra_empty_files_do_not_abort_the_index);
    RUN_TEST(resolver_infra_ansible_handler_gap_stays_in_its_scope);
    RUN_TEST(resolver_infra_k8s_references_name_one_object_in_a_declared_scope);
    RUN_TEST(resolver_infra_k8s_references_abstain_on_their_decoys);
    RUN_TEST(resolver_infra_k8s_custom_resource_finds_its_crd_by_identity);
    RUN_TEST(resolver_infra_helm_include_binds_a_sole_definition_in_its_family);
    RUN_TEST(resolver_infra_sql_lineage_binds_within_its_script);
    RUN_TEST(resolver_infra_compose_env_gap_stays_with_its_variable);
    RUN_TEST(resolver_infra_projection_rows_need_marker_and_labels_together);
}
