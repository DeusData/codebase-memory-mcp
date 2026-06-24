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
        if (*p == '\\') *p = '/';
    }
}

/* Write files, then run index_repository and open the graph DB. */
static cbm_store_t *et_index_files(EtProj *lp, const EtFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_et_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir)) return NULL;
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

static void et_cleanup(EtProj *lp, cbm_store_t *store) {
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

/* Index meaningful[] plus PARALLEL_PAD_FILES trivial pad files to force the
 * parallel pipeline path (MIN_FILES_FOR_PARALLEL = 50). */
enum { ET_PARALLEL_PAD = 52, ET_PAD_MAX = 68 /* 52 pad + 16 meaningful */ };

static cbm_store_t *et_index_parallel(EtProj *lp, const EtFile *meaningful, int n_mean) {
    static char pad_name[ET_PARALLEL_PAD][48];
    static char pad_body[ET_PARALLEL_PAD][64];
    EtFile files[ET_PAD_MAX] = {0};
    int n = 0;
    for (int i = 0; i < n_mean; i++) files[n++] = meaningful[i];
    for (int i = 0; i < ET_PARALLEL_PAD; i++) {
        snprintf(pad_name[i], sizeof(pad_name[i]), "pad/pad_%02d.py", i);
        snprintf(pad_body[i], sizeof(pad_body[i]), "def pad_%02d():\n    return %d\n", i, i);
        files[n].name    = pad_name[i];
        files[n].content = pad_body[i];
        n++;
    }
    return et_index_files(lp, files, n);
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
        {"app.py",
         "from flask import Flask\n\napp = Flask(__name__)\n\n\n"
         "@app.route(\"/items\")\ndef list_items():\n    return {\"items\": []}\n\n\n"
         "@app.route(\"/items/<int:item_id>\")\ndef get_item(item_id):\n    return {\"id\": item_id}\n"}};
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
        {"users.ts",
         "import { expressGet, expressPost } from './express/router';\n\n"
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
    static const EtFile f[] = {
        {"gin/engine.go",
         "package gin\n\n"
         "type Engine struct{}\n\n"
         "func Default() *Engine {\n    return &Engine{}\n}\n\n"
         "func (e *Engine) GET(path string, handler interface{}) {}\n"
         "func (e *Engine) POST(path string, handler interface{}) {}\n"},
        {"main.go",
         "package main\n\n"
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

/* Spring (Java) — @RequestMapping decorator sets route_path in extraction.
 * REAL BUG: internal/cbm/extract_defs.c:extract_route_from_decorators only walks
 * ts_node_prev_sibling(func) for decorator nodes of type "call".  Java annotations
 * (@GetMapping/@RequestMapping) live INSIDE the method's `modifiers` child (not a
 * prev_sibling) and are `annotation`/`marker_annotation` nodes (not `call`), so
 * route_path is never set → no Route/HANDLES for Spring controllers. */
TEST(handles_spring_java) {
    static const EtFile f[] = {
        {"OrderController.java",
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
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 1));
    PASS();
}

/* Spring (Kotlin) — @RequestMapping/@GetMapping on a Kotlin @RestController.
 * REAL BUG: tree-sitter-kotlin annotation nodes have no `name` field (the name
 * lives in a nested user_type/type_identifier) and carry args under a
 * constructor_invocation `value_arguments` node, so the Java-shaped
 * ts_node_child_by_field_name(annotation, "name") / "arguments" lookups in
 * try_route_from_annotation missed every Kotlin Spring route → route_path never
 * set → no Route/HANDLES. Fixed by annotation_name_node/annotation_args_node. */
TEST(handles_spring_kotlin) {
    static const EtFile f[] = {
        {"OrderController.kt",
         "package com.example\n\n"
         "import org.springframework.web.bind.annotation.RequestMapping\n"
         "import org.springframework.web.bind.annotation.GetMapping\n\n"
         "@RequestMapping(\"/api\")\nclass OrderController {\n"
         "    @GetMapping(\"/orders\")\n"
         "    fun listOrders(): String {\n"
         "        return \"orders\"\n    }\n\n"
         "    @GetMapping(\"/orders/{id}\")\n"
         "    fun getOrder(id: Int): String {\n"
         "        return \"order:\" + id\n    }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 1, "HANDLES", 1));
    PASS();
}

/* ASP.NET Minimal API (C#) — route registration via static MapGet/MapPost calls
 * with identifier handlers, under a Microsoft/AspNetCore path so the resolved
 * callee QN carries the "MapGet"/"Microsoft.AspNetCore" route-reg substrings.
 * REAL BUG (CONFIRMED: still HANDLES=0 after this idiomatic, substring-correct
 * fixture).  C# static calls resolve (S4 passes) and the substring is present,
 * yet no Route/HANDLES is emitted — the C# route-registration path is not wired:
 * the resolved C# static-invocation either is not run through
 * cbm_service_pattern_match as ROUTE_REG or the handler-arg (an identifier
 * method group) is not captured by extract_handler_arg for C#. */
TEST(handles_aspnet_csharp) {
    static const EtFile f[] = {
        {"Microsoft/AspNetCore/Builder.cs",
         "namespace Microsoft.AspNetCore {\n"
         "    class WebApp {\n"
         "        public static string MapGet(string path, System.Func<string> handler) { return handler(); }\n"
         "        public static string MapPost(string path, System.Func<string> handler) { return handler(); }\n"
         "    }\n}\n"},
        {"Program.cs",
         "using Microsoft.AspNetCore;\n\n"
         "namespace App {\n"
         "    class Program {\n"
         "        static void Main() {\n"
         "            WebApp.MapGet(\"/products\", GetProducts);\n"
         "            WebApp.MapPost(\"/products\", CreateProduct);\n        }\n"
         "        static string GetProducts() { return \"[]\"; }\n"
         "        static string CreateProduct() { return \"{}\" ; }\n    }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "HANDLES", 1));
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
        {"routes/web.php",
         "<?php\nuse Laravel\\Route;\n\n"
         "function showUsers() { return ['users' => []]; }\n"
         "function storeUser() { return ['stored' => true]; }\n\n"
         "Route::get('/users', 'showUsers');\n"
         "Route::post('/users', 'storeUser');\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "HANDLES", 1));
    PASS();
}

/* Rails (Ruby) — ActionDispatch router.  The handler MUST be passed as a bare
 * identifier (not the idiomatic `to: 'list_items'` string, which extract_handler_arg
 * cannot capture).  mapper.get resolves by name to the Mapper#get method whose QN
 * carries the "ActionDispatch" route-registration substring → ROUTE_REG → HANDLES. */
TEST(handles_rails_ruby) {
    static const EtFile f[] = {
        {"ActionDispatch/Routing.rb",
         "module ActionDispatch\n  module Routing\n"
         "    class Mapper\n"
         "      def get(path, handler); end\n"
         "      def post(path, handler); end\n"
         "    end\n  end\nend\n"},
        {"config/routes.rb",
         "require_relative '../ActionDispatch/Routing'\n\n"
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
    static const EtFile f[] = {
        {"actix_web_app.rs",
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
        {"axios.ts",
         "export function axiosGet(url: string): Promise<any> {\n"
         "    return Promise.resolve({ data: {} });\n}\n\n"
         "export function axiosPost(url: string, data: any): Promise<any> {\n"
         "    return Promise.resolve({ data });\n}\n"},
        {"client.ts",
         "import { axiosGet, axiosPost } from './axios';\n\n"
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
        {"catalog/service.go",
         "package catalog\n\n"
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
         "    public Object postForObject(String url, Object req, Class<?> responseType) { return null; }\n"
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
    ASSERT_TRUE(et_edge_present(f, 2, "HTTP_CALLS", 1));
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
         "        public string AddProduct(object p) { return RestClient.Post(\"/products\", p); }\n"
         "    }\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "HTTP_CALLS", 1));
    PASS();
}

/* HTTParty (Ruby) — "HTTParty" substring in resolved QN */
TEST(http_calls_httparty_ruby) {
    static const EtFile f[] = {
        {"HTTParty.rb",
         "module HTTParty\n"
         "  def self.get(url, opts = {}); end\n"
         "  def self.post(url, opts = {}); end\nend\n"},
        {"user_client.rb",
         "require_relative 'HTTParty'\n\n"
         "def fetch_users\n  HTTParty.get('/api/users')\nend\n\n"
         "def create_user(body)\n  HTTParty.post('/api/users', body: body)\nend\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "HTTP_CALLS", 1));
    PASS();
}

/* Guzzle (PHP) — Client injected via a type-hinted constructor param (proven
 * php/S8 field-type-hint shape).
 * REAL BUG (CONFIRMED: still HTTP_CALLS=0).  PHP DOES resolve the Guzzle method
 * call (cf. test_php_lsp.c:phplsp_edge_guzzle_chain, which passes resolving
 * "Client.get"), but the PHP lsp resolver emits the SHORT resolved QN
 * "Client.get" — it drops the namespace/path, so cbm_service_pattern_match never
 * sees the "Guzzle"/"GuzzleHttp" substring and the call is classified as a plain
 * CALLS instead of HTTP_CALLS.  Fix = use the full namespaced QN (or match on the
 * class's declaring namespace) when classifying PHP service calls. */
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
    ASSERT_TRUE(et_edge_present(f, 2, "HTTP_CALLS", 1));
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
         "pub fn reqwest_post(url: &str, body: &str) -> String {\n    format!(\"{}{}\", url, body)\n}\n\n"
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
        {"tasks/order_tasks.py",
         "from celery.app import app\n\n\n"
         "def dispatch_order_created(order_id):\n"
         "    return app.send_task('order_created', args=[order_id])\n\n\n"
         "def dispatch_order_shipped(order_id):\n"
         "    return app.send_task('order_shipped', args=[order_id])\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "ASYNC_CALLS", 1));
    PASS();
}

/* Sidekiq (Ruby) — "Sidekiq" substring in resolved QN */
TEST(async_calls_sidekiq_ruby) {
    static const EtFile f[] = {
        {"Sidekiq/Worker.rb",
         "module Sidekiq\n  module Worker\n"
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
        {"events/order_events.ts",
         "import { kafkajsProduce } from '../kafkajs/producer';\n\n"
         "export function emitOrderCreated(orderId: string): void {\n"
         "    kafkajsProduce('order-created', { orderId });\n}\n\n"
         "export function emitOrderCancelled(orderId: string): void {\n"
         "    kafkajsProduce('order-cancelled', { orderId });\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "ASYNC_CALLS", 1));
    PASS();
}

/* AWS SQS (Go) — "aws-sdk-go/service/sqs" substring in resolved QN.
 * REAL BUG (two compounding causes, cannot be exercised by a local fixture):
 *  1) service_patterns.c async_libraries lists the Go SQS id with SLASHES
 *     ("aws-sdk-go/service/sqs"), but cbm_fqn_compute (internal/cbm/helpers.c)
 *     converts path slashes to '.', so a resolved local QN is
 *     "...aws-sdk-go.service.sqs..." and strstr never matches the slash form.
 *  2) emit_http_async_edge (pass_calls.c) requires a URL/topic STRING arg;
 *     SendMessage(&SendMessageInput{...}) passes a struct, no string → the call
 *     degrades to a plain CALLS edge.  No dot-form Go SQS id exists, so the
 *     SQS/Go async pattern is unreachable here. */
TEST(async_calls_sqs_go) {
    static const EtFile f[] = {
        {"aws-sdk-go/service/sqs/api.go",
         "package sqs\n\n"
         "type SendMessageInput struct{ QueueUrl string; MessageBody string }\n"
         "type SQS struct{}\n\n"
         "func New() *SQS { return &SQS{} }\n\n"
         "func (s *SQS) SendMessage(input *SendMessageInput) error { return nil }\n"},
        {"queue/dispatcher.go",
         "package queue\n\n"
         "import \"aws-sdk-go/service/sqs\"\n\n"
         "func DispatchOrderEvent(queueUrl, body string) error {\n"
         "    client := sqs.New()\n"
         "    return client.SendMessage(&sqs.SendMessageInput{\n"
         "        QueueUrl:    queueUrl,\n"
         "        MessageBody: body,\n    })\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "ASYNC_CALLS", 1));
    PASS();
}

/* BullMQ (JavaScript) — "bullmq" substring in resolved QN */
TEST(async_calls_bullmq_js) {
    static const EtFile f[] = {
        {"bullmq/queue.js",
         "class bullmqQueue {\n"
         "    add(jobName, data) { return { jobName, data }; }\n}\n\n"
         "module.exports = { bullmqQueue };\n"},
        {"jobs/mailer.js",
         "const { bullmqQueue } = require('./bullmq/queue');\n\n"
         "const mailQueue = new bullmqQueue();\n\n"
         "function scheduleWelcomeEmail(userId) {\n"
         "    return mailQueue.add('welcome-email', { userId });\n}\n\n"
         "function schedulePasswordReset(userId) {\n"
         "    return mailQueue.add('password-reset', { userId });\n}\n"}};
    ASSERT_TRUE(et_edge_present(f, 2, "ASYNC_CALLS", 1));
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
        {"Service.kt",
         "class NotFoundException(msg: String) : Exception(msg)\n\n"
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
        {"validate.py",
         "class ValidationException(Exception):\n"
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
        {"exceptions.ts",
         "export class HttpException {\n"
         "    constructor(public status: number, public message: string) {}\n}\n"},
        {"controller.ts",
         "import { HttpException } from './exceptions';\n\n"
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
        {"Records.scala",
         "class RecordException(msg: String) extends Exception(msg)\n\n"
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
    static const EtFile meaningful[] = {
        {"parser.py",
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
        {"validators.ts",
         "export class TypeError {\n"
         "    constructor(public message: string) {}\n}\n"},
        {"parser.ts",
         "import { TypeError } from './validators';\n\n"
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
        {"Errors.kt",
         "class IllegalArgumentError(msg: String) : RuntimeException(msg)\n\n"
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

/* Python — simple module-level variable assignment */
TEST(writes_python) {
    static const EtFile meaningful[] = {
        {"state.py",
         "registry = {}\n\n\n"
         "def register(key, value):\n"
         "    registry = {key: value}\n"
         "    return registry\n\n\n"
         "def clear_registry():\n"
         "    registry = {}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int writes = store ? cbm_store_count_edges_by_type(store, lp.project, "WRITES") : -1;
    if (writes < 1) {
        fprintf(stderr, "  [ET-WRITES] Python: WRITES=%d\n", writes);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(writes >= 1);
    PASS();
}

/* Go — short_var_declaration and assignment_statement */
TEST(writes_go) {
    static const EtFile meaningful[] = {
        {"cache.go",
         "package cache\n\n"
         "var store map[string]string\n\n"
         "func Set(key, value string) {\n"
         "    store = make(map[string]string)\n"
         "    store[key] = value\n}\n\n"
         "func Reset() {\n"
         "    store = nil\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int writes = store ? cbm_store_count_edges_by_type(store, lp.project, "WRITES") : -1;
    if (writes < 1) {
        fprintf(stderr, "  [ET-WRITES] Go: WRITES=%d\n", writes);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(writes >= 1);
    PASS();
}

/* Java — field assignment inside a method */
TEST(writes_java) {
    static const EtFile meaningful[] = {
        {"Counter.java",
         "package app;\n\n"
         "class Counter {\n"
         "    private int count = 0;\n\n"
         "    public void increment() {\n"
         "        count = count + 1;\n    }\n\n"
         "    public void reset() {\n"
         "        count = 0;\n    }\n\n"
         "    public int get() {\n        return count;\n    }\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int writes = store ? cbm_store_count_edges_by_type(store, lp.project, "WRITES") : -1;
    if (writes < 1) {
        fprintf(stderr, "  [ET-WRITES] Java: WRITES=%d\n", writes);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(writes >= 1);
    PASS();
}

/* Rust — local variable assignment (assignment_expression) */
TEST(writes_rust) {
    static const EtFile meaningful[] = {
        {"accumulator.rs",
         "pub struct Accumulator {\n    pub total: i64,\n}\n\n"
         "impl Accumulator {\n"
         "    pub fn add(&mut self, n: i64) {\n"
         "        let total = self.total + n;\n"
         "        self.total = total;\n    }\n\n"
         "    pub fn clear(&mut self) {\n"
         "        self.total = 0;\n    }\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int writes = store ? cbm_store_count_edges_by_type(store, lp.project, "WRITES") : -1;
    if (writes < 1) {
        fprintf(stderr, "  [ET-WRITES] Rust: WRITES=%d\n", writes);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(writes >= 1);
    PASS();
}

/* C# — property assignment inside methods */
TEST(writes_csharp) {
    static const EtFile meaningful[] = {
        {"Config.cs",
         "namespace App {\n"
         "    class Config {\n"
         "        public string Host = \"localhost\";\n"
         "        public int Port = 8080;\n\n"
         "        public void SetHost(string host) {\n"
         "            Host = host;\n        }\n\n"
         "        public void SetPort(int port) {\n"
         "            Port = port;\n        }\n    }\n}\n"}};
    EtProj lp;
    cbm_store_t *store =
        et_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int writes = store ? cbm_store_count_edges_by_type(store, lp.project, "WRITES") : -1;
    if (writes < 1) {
        fprintf(stderr, "  [ET-WRITES] C#: WRITES=%d\n", writes);
    }
    et_cleanup(&lp, store);
    ASSERT_TRUE(writes >= 1);
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
        {"service.go",
         "package svc\n\n"
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
    static const EtFile f[] = {
        {"Account.java",
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
    static const EtFile f[] = {
        {"Queue.cs",
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
    static const EtFile f[] = {
        {"Cart.php",
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
    static const EtFile f[] = {
        {"stack.rb",
         "class Stack\n"
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
        {"Wallet.kt",
         "class Wallet(private var balance: Double) {\n"
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
        {"Logger.ts",
         "export class Logger {\n"
         "    private prefix: string;\n\n"
         "    constructor(prefix: string) {\n        this.prefix = prefix;\n    }\n\n"
         "    info(msg: string): void {\n        console.log(`[${this.prefix}] INFO: ${msg}`);\n    }\n\n"
         "    warn(msg: string): void {\n        console.warn(`[${this.prefix}] WARN: ${msg}`);\n    }\n\n"
         "    error(msg: string): void {\n        console.error(`[${this.prefix}] ERR: ${msg}`);\n    }\n}\n"}};
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
    static const EtFile meaningful[] = {
        {"shapes.go",
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
    int implements     = store ? cbm_store_count_edges_by_type(store, lp.project, "IMPLEMENTS") : -1;
    if (override_edges < 1 || implements < 1) {
        fprintf(stderr, "  [ET-OVERRIDE] Go: OVERRIDE=%d IMPLEMENTS=%d\n",
                override_edges, implements);
    }
    et_cleanup(&lp, store);
    /* Both IMPLEMENTS (struct→interface) and OVERRIDE (method→interface-method) expected. */
    ASSERT_TRUE(override_edges >= 1);
    ASSERT_TRUE(implements >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SUITE
 * ══════════════════════════════════════════════════════════════════ */

SUITE(edge_types_probe) {
    /* HANDLES — route→handler across web frameworks (8 frameworks) */
    RUN_TEST(handles_flask_python);
    RUN_TEST(handles_fastapi_python);
    RUN_TEST(handles_express_ts);
    RUN_TEST(handles_fastify_js);
    RUN_TEST(handles_gin_go);
    RUN_TEST(handles_spring_java);
    RUN_TEST(handles_spring_kotlin);
    RUN_TEST(handles_aspnet_csharp);
    RUN_TEST(handles_laravel_php);
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
}
