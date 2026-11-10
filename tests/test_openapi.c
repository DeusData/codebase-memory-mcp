/*
 * test_openapi.c -- OpenAPI / Swagger documents: each operation a Route named
 * as code names it, each operationId a reference to its handler (openapi.c).
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "test_doc_mentions_helpers.h"
#include "cbm.h"
#include "discover/discover.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const CBMDefinition *oa_route(const CBMFileResult *r, const char *qn) {
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (d->qualified_name && strcmp(d->qualified_name, qn) == 0) {
            return d;
        }
    }
    printf("  no definition %s\n", qn);
    return NULL;
}

static const char *oa_prop(const CBMDefinition *d, const char *key) {
    for (int i = 0; d && d->extra_props && d->extra_props[i]; i += 2) {
        if (strcmp(d->extra_props[i], key) == 0) {
            return d->extra_props[i + 1];
        }
    }
    return NULL;
}

static int oa_count(const CBMFileResult *r, const char *label) {
    int n = 0;
    for (int i = 0; i < r->defs.count; i++) {
        n += r->defs.items[i].label && strcmp(r->defs.items[i].label, label) == 0;
    }
    return n;
}

static int oa_link(const CBMFileResult *r, const char *source_qn, const char *raw) {
    int n = 0;
    for (int i = 0; i < r->doc_links.count; i++) {
        const CBMDocLink *l = &r->doc_links.items[i];
        n += l->syntax == CBM_DOCLINK_OPENAPI_OPERATION_ID &&
             strcmp(l->source_qn, source_qn) == 0 && strcmp(l->raw, raw) == 0;
    }
    if (n != 1) {
        printf("  link %s -> %s: %d found\n", source_qn, raw, n);
    }
    return n;
}

static const char OA_YAML[] = "openapi: 3.0.0\n"
                              "info:\n"
                              "  title: Users\n"
                              "paths:\n"
                              "  /users/{id}:\n"
                              "    x-openapi-router-controller: app.api.users\n"
                              "    get:\n"
                              "      operationId: get_user\n"
                              "    delete:\n"
                              "      operationId: deleteUser\n"
                              "      x-openapi-router-controller: app.admin\n"
                              "  /users:\n"
                              "    post:\n"
                              "      operationId: \"app.api.users.create_user\"\n"
                              "    parameters: []\n"
                              "  /health:\n"
                              "    get:\n"
                              "      summary: no id\n";

TEST(openapi_yaml) {
    CBMFileResult *r = dm_extract(OA_YAML, CBM_LANG_YAML, "api/openapi.yaml");
    ASSERT_NOT_NULL(r);
    const CBMDefinition *g = oa_route(r, "__route__GET__/users/{}");
    ASSERT_NOT_NULL(g);
    ASSERT_STR_EQ(g->label, "Route");
    ASSERT_STR_EQ(g->name, "/users/{id}");
    ASSERT_STR_EQ(oa_prop(g, "method"), "GET");
    ASSERT_STR_EQ(oa_prop(g, "source"), "openapi");
    ASSERT_STR_EQ(oa_prop(g, "operation_id"), "get_user");
    ASSERT_NOT_NULL(oa_route(r, "__route__DELETE__/users/{}"));
    ASSERT_NOT_NULL(oa_route(r, "__route__POST__/users"));
    ASSERT_NULL(oa_prop(oa_route(r, "__route__GET__/health"), "operation_id"));
    ASSERT_EQ(oa_count(r, "Route"), 4); /* `parameters` is no operation */
    /* the controller names the module of a bare id; a dotted id is whole */
    ASSERT_EQ(oa_link(r, "__route__GET__/users/{}", "app.api.users.get_user"), 1);
    ASSERT_EQ(oa_link(r, "__route__DELETE__/users/{}", "app.admin.deleteUser"), 1);
    ASSERT_EQ(oa_link(r, "__route__POST__/users", "app.api.users.create_user"), 1);
    ASSERT_EQ(r->doc_links.count, 3);
    cbm_free_result(r);

    /* YAML that describes no API is none */
    r = dm_extract("paths:\n  /x:\n    get:\n      operationId: x\n", CBM_LANG_YAML, "conf.yaml");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(oa_count(r, "Route"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(openapi_json) {
    static const char spec[] =
        "{\"swagger\": \"2.0\", \"info\": {\"title\": \"Pets\"},\n"
        " \"paths\": {\"/pets\": {\"get\": {\"operationId\": \"listPets\"},\n"
        "  \"post\": {\"operationId\": \"pets.create\"}}}}\n";
    /* openapi.json: its operations, not a node per key */
    CBMFileResult *r = dm_extract(spec, CBM_LANG_OPENAPI, "openapi.json");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(oa_route(r, "__route__GET__/pets"));
    ASSERT_EQ(oa_link(r, "__route__GET__/pets", "listPets"), 1);
    ASSERT_EQ(oa_link(r, "__route__POST__/pets", "pets.create"), 1);
    ASSERT_EQ(oa_count(r, "Variable"), 0);
    cbm_free_result(r);
    /* a JSON file of another name that describes an API */
    r = dm_extract(spec, CBM_LANG_JSON, "spec/pets-api.json");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(oa_count(r, "Route"), 2);
    cbm_free_result(r);
    /* the document names it */
    ASSERT_EQ(cbm_language_for_filename("openapi.json"), CBM_LANG_OPENAPI);
    ASSERT_EQ(cbm_language_for_filename("swagger.json"), CBM_LANG_OPENAPI);
    PASS();
}

/* MENTIONS edges from the Route `route_qn` to a node whose QN ends with `tgt`. */
static int oa_db_mentions(const char *db, const char *route_qn, const char *tgt) {
    sqlite3 *h = NULL;
    int n = 0;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return -1;
    }
    sqlite3_stmt *st = NULL;
    const char *sql = "SELECT count(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                      "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
                      "AND s.qualified_name = ?1 AND t.qualified_name LIKE '%' || ?2";
    if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, route_qn, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, tgt, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            n = sqlite3_column_int(st, 0);
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
    return n;
}

/* MENTIONS edges from the Route `route_qn` whose properties hold `part`. */
static int oa_db_mentions_props(const char *db, const char *route_qn, const char *part) {
    sqlite3 *h = NULL;
    int n = 0;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return -1;
    }
    sqlite3_stmt *st = NULL;
    const char *sql = "SELECT count(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                      "WHERE e.type = 'MENTIONS' AND s.qualified_name = ?1 "
                      "AND e.properties LIKE '%' || ?2 || '%'";
    if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, route_qn, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, part, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            n = sqlite3_column_int(st, 0);
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
    return n;
}

static int oa_db_nodes(const char *db, const char *label, const char *qn) {
    sqlite3 *h = NULL;
    int n = 0;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return -1;
    }
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(h, "SELECT count(*) FROM nodes WHERE label = ?1 AND qualified_name = ?2",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, label, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, qn, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            n = sqlite3_column_int(st, 0);
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
    return n;
}

TEST(openapi_graph) {
    dm_ship_held_families(); /* the operationId family is held until its audit */
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_oa_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    th_write_file(TH_PATH(repo, "app/api/users.py"),
                  "def get_user(id):\n    return id\n\n\ndef create_user():\n    return 1\n");
    th_write_file(TH_PATH(repo, "api/openapi.yaml"), OA_YAML);
    th_write_file(TH_PATH(repo, "openapi.json"),
                  "{\"openapi\": \"3.1.0\", \"paths\": {\"/pets\": {\"get\": "
                  "{\"operationId\": \"listPets\"}}, \"/dup\": {\"get\": "
                  "{\"operationId\": \"dup\"}}, \"/none\": {\"get\": "
                  "{\"operationId\": \"noSuchHandler\"}}}}\n");
    th_write_file(TH_PATH(repo, "app/pets.py"), "def listPets():\n    return []\n\n\ndef dup():\n"
                                                "    return 1\n");
    th_write_file(TH_PATH(repo, "app/other.py"), "def dup():\n    return 2\n");
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    ASSERT_EQ(oa_db_nodes(db, "Route", "__route__GET__/users/{}"), 1);
    ASSERT_EQ(oa_db_nodes(db, "Route", "__route__GET__/pets"), 1);
    /* a dotted operationId names its handler */
    ASSERT_EQ(oa_db_mentions(db, "__route__GET__/users/{}", "users.get_user"), 1);
    ASSERT_EQ(oa_db_mentions(db, "__route__POST__/users", "users.create_user"), 1);
    /* through the API document, whichever resolver read its file (a YAML one) */
    ASSERT_EQ(oa_db_mentions_props(db, "__route__POST__/users", "\"via\":\"openapi\""), 1);
    ASSERT_EQ(oa_db_mentions_props(db, "__route__POST__/users", "\"tier\":\"exact\""), 1);
    ASSERT_EQ(oa_db_mentions_props(db, "__route__GET__/pets", "\"tier\":\"unique\""), 1);
    /* a bare id names the one function of that name */
    ASSERT_EQ(oa_db_mentions(db, "__route__GET__/pets", "pets.listPets"), 1);
    /* a module with no such function; a bare id two functions carry; one none does */
    ASSERT_EQ(oa_db_mentions(db, "__route__DELETE__/users/{}", ""), 0);
    ASSERT_EQ(oa_db_mentions(db, "__route__GET__/dup", ""), 0);
    ASSERT_EQ(oa_db_mentions(db, "__route__GET__/none", ""), 0);
    th_cleanup(tmp);
    cbm_doclink_test_reset_ships();
    PASS();
}

SUITE(openapi) {
    RUN_TEST(openapi_yaml);
    RUN_TEST(openapi_json);
    RUN_TEST(openapi_graph);
}
