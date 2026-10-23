/*
 * neo4j_client.c — Neo4j config, credential cache and Cypher execution.
 */
#include "neo4j/neo4j_client.h"
#include "neo4j/neo4j_http.h"

#include "foundation/compat_thread.h"
#include "foundation/platform.h"

#include <ctype.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define N4_DEFAULT_USER "neo4j"
#define N4_DEFAULT_DATABASE "neo4j"
#define N4_CRED_CACHE_MAX 16

struct cbm_neo4j {
    cbm_neo4j_http_t *http;
    char uri[CBM_NEO4J_URI_MAX];
    char commit_path[CBM_NEO4J_DATABASE_MAX + 32];
    char err[CBM_NEO4J_ERR_MAX];
};

/* ── Credential cache ───────────────────────────────────────────── */

typedef struct {
    char uri[CBM_NEO4J_URI_MAX];
    char user[CBM_NEO4J_USER_MAX];
    char password[CBM_NEO4J_PASSWORD_MAX];
} n4_cred_t;

static cbm_mutex_t g_cred_mu;
static atomic_int g_cred_mu_state; /* 0 = uninitialized, 1 = initializing, 2 = ready */
static n4_cred_t g_creds[N4_CRED_CACHE_MAX];
static int g_cred_count;

static void cred_lock(void) {
    /* Daemon request threads can race to the first use. */
    int expected = 0;
    if (atomic_compare_exchange_strong(&g_cred_mu_state, &expected, 1)) {
        cbm_mutex_init(&g_cred_mu);
        atomic_store(&g_cred_mu_state, 2);
    } else {
        while (atomic_load(&g_cred_mu_state) != 2) {
            /* another thread is finishing the one-time init */
        }
    }
    cbm_mutex_lock(&g_cred_mu);
}

static void copy_str(char *dst, size_t dst_sz, const char *src) {
    (void)snprintf(dst, dst_sz, "%s", src ? src : "");
}

static bool cred_lookup(const char *uri, const char *user, char *out, size_t out_sz) {
    bool found = false;
    cred_lock();
    for (int i = 0; i < g_cred_count; i++) {
        if (strcmp(g_creds[i].uri, uri) == 0 && strcmp(g_creds[i].user, user) == 0) {
            copy_str(out, out_sz, g_creds[i].password);
            found = true;
            break;
        }
    }
    cbm_mutex_unlock(&g_cred_mu);
    return found;
}

void cbm_neo4j_config_remember(const cbm_neo4j_config_t *cfg) {
    if (!cfg || !cfg->uri[0] || !cfg->password[0]) {
        return;
    }
    cred_lock();
    int slot = -1;
    for (int i = 0; i < g_cred_count; i++) {
        if (strcmp(g_creds[i].uri, cfg->uri) == 0 && strcmp(g_creds[i].user, cfg->user) == 0) {
            slot = i;
            break;
        }
    }
    if (slot < 0) {
        slot = g_cred_count < N4_CRED_CACHE_MAX ? g_cred_count++ : 0;
    }
    copy_str(g_creds[slot].uri, sizeof(g_creds[slot].uri), cfg->uri);
    copy_str(g_creds[slot].user, sizeof(g_creds[slot].user), cfg->user);
    copy_str(g_creds[slot].password, sizeof(g_creds[slot].password), cfg->password);
    cbm_mutex_unlock(&g_cred_mu);
}

void cbm_neo4j_config_wipe(cbm_neo4j_config_t *cfg) {
    if (cfg) {
        volatile char *p = cfg->password;
        for (size_t i = 0; i < sizeof(cfg->password); i++) {
            p[i] = 0;
        }
    }
}

/* ── Config ─────────────────────────────────────────────────────── */

static bool n4_nonempty(const char *s) {
    return s && *s;
}

static void resolve_password(cbm_neo4j_config_t *out, const char *explicit_pw) {
    char buf[CBM_NEO4J_PASSWORD_MAX];
    /* CBM_NEO4J_PASSWORD is only ever set by the index supervisor for one
     * worker and carries that request's --graph-password, so it outranks the
     * user's ambient NEO4J_PASSWORD. */
    if (n4_nonempty(explicit_pw)) {
        copy_str(out->password, sizeof(out->password), explicit_pw);
    } else if (cbm_safe_getenv(CBM_NEO4J_PASSWORD_ENV, buf, sizeof(buf), NULL) && buf[0]) {
        copy_str(out->password, sizeof(out->password), buf);
    } else if (cbm_safe_getenv("NEO4J_PASSWORD", buf, sizeof(buf), NULL) && buf[0]) {
        copy_str(out->password, sizeof(out->password), buf);
    } else {
        (void)cred_lookup(out->uri, out->user, out->password, sizeof(out->password));
    }
    memset(buf, 0, sizeof(buf));
}

static void resolve_from_env(char *dst, size_t dst_sz, const char *explicit_val,
                             const char *env_name, const char *dflt) {
    char buf[CBM_NEO4J_URI_MAX];
    if (n4_nonempty(explicit_val)) {
        copy_str(dst, dst_sz, explicit_val);
    } else if (cbm_safe_getenv(env_name, buf, sizeof(buf), NULL) && buf[0]) {
        copy_str(dst, dst_sz, buf);
    } else {
        copy_str(dst, dst_sz, dflt);
    }
}

int cbm_neo4j_config_resolve(const char *uri, const char *user, const char *password,
                             const char *database, cbm_neo4j_config_t *out, char *err,
                             size_t err_sz) {
    memset(out, 0, sizeof(*out));
    char uri_buf[CBM_NEO4J_URI_MAX];
    resolve_from_env(uri_buf, sizeof(uri_buf), uri, "NEO4J_URI", "");
    if (!uri_buf[0]) {
        (void)snprintf(err, err_sz, "graph_db URL is required");
        return -1;
    }
    if (strlen(uri_buf) >= sizeof(out->uri)) {
        (void)snprintf(err, err_sz, "graph_db URL too long");
        return -1;
    }
    cbm_neo4j_url_t parsed;
    if (cbm_neo4j_url_parse(uri_buf, &parsed, err, err_sz) != 0) {
        return -1;
    }
    copy_str(out->uri, sizeof(out->uri), uri_buf);
    resolve_from_env(out->user, sizeof(out->user), user, "NEO4J_USER", "");
    if (!out->user[0]) {
        /* NEO4J_USERNAME is what the official drivers and Docker image use. */
        resolve_from_env(out->user, sizeof(out->user), NULL, "NEO4J_USERNAME", N4_DEFAULT_USER);
    }
    resolve_from_env(out->database, sizeof(out->database), database, "NEO4J_DATABASE",
                     N4_DEFAULT_DATABASE);
    for (const char *p = out->database; *p; p++) {
        if (!isalnum((unsigned char)*p) && *p != '.' && *p != '-' && *p != '_') {
            (void)snprintf(err, err_sz, "invalid graph_database name '%s'", out->database);
            return -1;
        }
    }
    resolve_password(out, password);
    return 0;
}

char *cbm_neo4j_config_to_meta(const cbm_neo4j_config_t *cfg) {
    yyjson_mut_doc *d = yyjson_mut_doc_new(NULL);
    if (!d) {
        return NULL;
    }
    yyjson_mut_val *o = yyjson_mut_obj(d);
    yyjson_mut_doc_set_root(d, o);
    yyjson_mut_obj_add_str(d, o, "kind", "neo4j");
    yyjson_mut_obj_add_strcpy(d, o, "uri", cfg->uri);
    yyjson_mut_obj_add_strcpy(d, o, "user", cfg->user);
    yyjson_mut_obj_add_strcpy(d, o, "database", cfg->database);
    char *json = yyjson_mut_write(d, 0, NULL);
    yyjson_mut_doc_free(d);
    return json;
}

int cbm_neo4j_config_from_meta(const char *meta_json, cbm_neo4j_config_t *out, char *err,
                               size_t err_sz) {
    if (!meta_json || !*meta_json) {
        (void)snprintf(err, err_sz, "project has no graph backend metadata");
        return -1;
    }
    yyjson_doc *d = yyjson_read(meta_json, strlen(meta_json), 0);
    yyjson_val *o = d ? yyjson_doc_get_root(d) : NULL;
    const char *kind = yyjson_get_str(yyjson_obj_get(o, "kind"));
    if (!o || !kind || strcmp(kind, "neo4j") != 0) {
        yyjson_doc_free(d);
        (void)snprintf(err, err_sz, "unsupported graph backend metadata");
        return -1;
    }
    /* Use stored uri/user/database verbatim — env overrides apply only to the
     * password, so a stale NEO4J_URI can never redirect an indexed project. */
    memset(out, 0, sizeof(*out));
    copy_str(out->uri, sizeof(out->uri), yyjson_get_str(yyjson_obj_get(o, "uri")));
    copy_str(out->user, sizeof(out->user), yyjson_get_str(yyjson_obj_get(o, "user")));
    copy_str(out->database, sizeof(out->database), yyjson_get_str(yyjson_obj_get(o, "database")));
    yyjson_doc_free(d);
    if (!out->uri[0]) {
        (void)snprintf(err, err_sz, "graph backend metadata has no uri");
        return -1;
    }
    if (!out->database[0]) {
        copy_str(out->database, sizeof(out->database), N4_DEFAULT_DATABASE);
    }
    resolve_password(out, NULL);
    return 0;
}

/* ── Client ─────────────────────────────────────────────────────── */

cbm_neo4j_t *cbm_neo4j_connect(const cbm_neo4j_config_t *cfg, char *err, size_t err_sz) {
    cbm_neo4j_url_t url;
    if (cbm_neo4j_url_parse(cfg->uri, &url, err, err_sz) != 0) {
        return NULL;
    }
    cbm_neo4j_t *c = calloc(1, sizeof(*c));
    if (!c) {
        return NULL;
    }
    c->http = cbm_neo4j_http_open(&url, cfg->user, cfg->password, err, err_sz);
    if (!c->http) {
        free(c);
        return NULL;
    }
    copy_str(c->uri, sizeof(c->uri), cfg->uri);
    (void)snprintf(c->commit_path, sizeof(c->commit_path), "/db/%s/tx/commit",
                   cfg->database[0] ? cfg->database : N4_DEFAULT_DATABASE);
    return c;
}

void cbm_neo4j_close(cbm_neo4j_t *c) {
    if (!c) {
        return;
    }
    cbm_neo4j_http_close(c->http);
    free(c);
}

const char *cbm_neo4j_error(const cbm_neo4j_t *c) {
    return c ? c->err : "no Neo4j client";
}

const char *cbm_neo4j_uri(const cbm_neo4j_t *c) {
    return c ? c->uri : "";
}

int cbm_neo4j_batch_init(cbm_neo4j_batch_t *b) {
    memset(b, 0, sizeof(*b));
    b->doc = yyjson_mut_doc_new(NULL);
    if (!b->doc) {
        return -1;
    }
    yyjson_mut_val *root = yyjson_mut_obj(b->doc);
    yyjson_mut_doc_set_root(b->doc, root);
    b->statements = yyjson_mut_arr(b->doc);
    yyjson_mut_obj_add_val(b->doc, root, "statements", b->statements);
    return 0;
}

yyjson_mut_val *cbm_neo4j_batch_add(cbm_neo4j_batch_t *b, const char *cypher) {
    yyjson_mut_val *st = yyjson_mut_obj(b->doc);
    yyjson_mut_obj_add_strcpy(b->doc, st, "statement", cypher);
    yyjson_mut_val *params = yyjson_mut_obj(b->doc);
    yyjson_mut_obj_add_val(b->doc, st, "parameters", params);
    yyjson_mut_val *contents = yyjson_mut_arr(b->doc);
    yyjson_mut_arr_add_str(b->doc, contents, "row");
    yyjson_mut_obj_add_val(b->doc, st, "resultDataContents", contents);
    yyjson_mut_arr_append(b->statements, st);
    b->count++;
    return params;
}

void cbm_neo4j_batch_free(cbm_neo4j_batch_t *b) {
    if (b && b->doc) {
        yyjson_mut_doc_free(b->doc);
        b->doc = NULL;
    }
}

static void set_err(cbm_neo4j_t *c, const char *fmt, const char *a, const char *b2) {
    (void)snprintf(c->err, sizeof(c->err), fmt, a ? a : "", b2 ? b2 : "");
}

/* NULL when results holds exactly one well-formed entry per statement sent:
 * {"columns": [...], "data": [{"row": [one value per column]}, ...]}.
 * Otherwise a short description of the first defect. */
const char *cbm_neo4j_result_shape_error(yyjson_val *results, int statements) {
    if (!yyjson_is_arr(results)) {
        return "no results array";
    }
    if (yyjson_arr_size(results) != (size_t)statements) {
        return "result count does not match the statements sent";
    }
    size_t idx;
    size_t max;
    yyjson_val *res;
    yyjson_arr_foreach(results, idx, max, res) {
        yyjson_val *columns = yyjson_obj_get(res, "columns");
        yyjson_val *data = yyjson_obj_get(res, "data");
        if (!yyjson_is_arr(columns) || !yyjson_is_arr(data)) {
            return "result without columns/data arrays";
        }
        size_t ncols = yyjson_arr_size(columns);
        size_t ri;
        size_t rmax;
        yyjson_val *entry;
        yyjson_arr_foreach(data, ri, rmax, entry) {
            yyjson_val *row = yyjson_obj_get(entry, "row");
            if (!yyjson_is_arr(row) || yyjson_arr_size(row) != ncols) {
                return "row does not match its columns";
            }
        }
    }
    return NULL;
}

int cbm_neo4j_batch_run(cbm_neo4j_t *c, cbm_neo4j_batch_t *b, cbm_neo4j_result_t *out) {
    if (out) {
        memset(out, 0, sizeof(*out));
    }
    if (!c || !b || !b->doc) {
        return -1;
    }
    c->err[0] = '\0';
    size_t body_len = 0;
    char *body = yyjson_mut_write(b->doc, 0, &body_len);
    if (!body) {
        set_err(c, "failed to serialize Neo4j request%s%s", NULL, NULL);
        return -1;
    }
    int status = 0;
    char *resp = NULL;
    size_t resp_len = 0;
    char terr[CBM_NEO4J_ERR_MAX];
    terr[0] = '\0';
    int rc = cbm_neo4j_http_post(c->http, c->commit_path, body, body_len, &status, &resp, &resp_len,
                                 terr, sizeof(terr));
    free(body);
    if (rc != 0) {
        set_err(c, "%s%s", terr[0] ? terr : "Neo4j request failed", NULL);
        free(resp);
        return -1;
    }
    if (status == 401 || status == 403) {
        set_err(c,
                "Neo4j rejected the credentials for %s (HTTP 401/403); check "
                "--graph-user/--graph-password or NEO4J_USER/NEO4J_PASSWORD%s",
                c->uri, NULL);
        free(resp);
        return -1;
    }
    if (status == 404) {
        set_err(c,
                "Neo4j endpoint %s not found (HTTP 404): wrong database name or not a "
                "Neo4j HTTP port%s",
                c->commit_path, NULL);
        free(resp);
        return -1;
    }
    yyjson_doc *d = yyjson_read(resp, resp_len, 0);
    free(resp);
    yyjson_val *root = d ? yyjson_doc_get_root(d) : NULL;
    if (!root) {
        char sc[16];
        (void)snprintf(sc, sizeof(sc), "%d", status);
        set_err(c, "unparseable Neo4j response (HTTP %s)%s", sc, NULL);
        yyjson_doc_free(d);
        return -1;
    }
    yyjson_val *errors = yyjson_obj_get(root, "errors");
    if (errors && yyjson_arr_size(errors) > 0) {
        yyjson_val *e0 = yyjson_arr_get_first(errors);
        set_err(c, "Neo4j error %s: %s", yyjson_get_str(yyjson_obj_get(e0, "code")),
                yyjson_get_str(yyjson_obj_get(e0, "message")));
        yyjson_doc_free(d);
        return -1;
    }
    if (status < 200 || status >= 300) {
        char sc[16];
        (void)snprintf(sc, sizeof(sc), "%d", status);
        set_err(c, "Neo4j returned HTTP %s%s", sc, NULL);
        yyjson_doc_free(d);
        return -1;
    }
    /* A 2xx body must still be a complete transactional result. Anything
     * else (a proxy or another service answering 200, a truncated reply)
     * would read as "no rows" -- an empty graph to every reader, and a
     * generation of 0 to the sync -- so it is an error, never a success. */
    const char *shape_err = cbm_neo4j_result_shape_error(yyjson_obj_get(root, "results"), b->count);
    if (shape_err) {
        set_err(c, "malformed Neo4j response from %s: %s", c->uri, shape_err);
        yyjson_doc_free(d);
        return -1;
    }
    if (out) {
        out->doc = d;
        out->results = yyjson_obj_get(root, "results");
    } else {
        yyjson_doc_free(d);
    }
    return 0;
}

int cbm_neo4j_ping(cbm_neo4j_t *c) {
    cbm_neo4j_batch_t b;
    if (cbm_neo4j_batch_init(&b) != 0) {
        return -1;
    }
    (void)cbm_neo4j_batch_add(&b, "RETURN 1");
    int rc = cbm_neo4j_batch_run(c, &b, NULL);
    cbm_neo4j_batch_free(&b);
    return rc;
}

yyjson_val *cbm_neo4j_result_data(const cbm_neo4j_result_t *r, size_t stmt_index) {
    if (!r || !r->results) {
        return NULL;
    }
    yyjson_val *res = yyjson_arr_get(r->results, stmt_index);
    return res ? yyjson_obj_get(res, "data") : NULL;
}

yyjson_val *cbm_neo4j_row_col(yyjson_val *data_entry, size_t col) {
    return yyjson_arr_get(yyjson_obj_get(data_entry, "row"), col);
}

void cbm_neo4j_result_free(cbm_neo4j_result_t *r) {
    if (r && r->doc) {
        yyjson_doc_free(r->doc);
        r->doc = NULL;
        r->results = NULL;
    }
}

int64_t cbm_neo4j_val_int(yyjson_val *v, int64_t fallback) {
    if (yyjson_is_int(v)) {
        return yyjson_get_sint(v);
    }
    if (yyjson_is_real(v)) {
        return (int64_t)yyjson_get_real(v);
    }
    if (yyjson_is_str(v)) {
        return strtoll(yyjson_get_str(v), NULL, 10);
    }
    return fallback;
}

char *cbm_neo4j_val_strdup(yyjson_val *v, const char *fallback) {
    const char *s = NULL;
    if (yyjson_is_str(v)) {
        s = yyjson_get_str(v);
    } else if (v && !yyjson_is_null(v)) {
        /* Non-string values (numbers, lists, maps) come back as their JSON. */
        return yyjson_val_write(v, 0, NULL);
    } else {
        s = fallback;
    }
    if (!s) {
        return NULL;
    }
    size_t n = strlen(s);
    char *d = malloc(n + 1);
    if (d) {
        memcpy(d, s, n + 1);
    }
    return d;
}

bool cbm_neo4j_ident_ok(const char *s) {
    if (!s || !*s || strlen(s) > 128) {
        return false;
    }
    if (!isalpha((unsigned char)s[0]) && s[0] != '_') {
        return false;
    }
    for (const char *p = s; *p; p++) {
        if (!isalnum((unsigned char)*p) && *p != '_') {
            return false;
        }
    }
    return true;
}
