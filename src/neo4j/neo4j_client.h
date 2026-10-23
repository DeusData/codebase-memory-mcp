/*
 * neo4j_client.h — Neo4j connection config + Cypher execution over the
 * transactional HTTP endpoint (POST /db/<database>/tx/commit).
 *
 * A request carries one or more statements that Neo4j runs in a single
 * transaction; results come back per statement as rows of JSON values.
 */
#ifndef CBM_NEO4J_CLIENT_H
#define CBM_NEO4J_CLIENT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "yyjson/yyjson.h"

#define CBM_NEO4J_URI_MAX 1024
#define CBM_NEO4J_USER_MAX 256
#define CBM_NEO4J_PASSWORD_MAX 512
#define CBM_NEO4J_DATABASE_MAX 128
#define CBM_NEO4J_ERR_MAX 1024

/* Worker hand-off: the daemon passes the password to the index worker in
 * this environment variable instead of on the worker's command line. */
#define CBM_NEO4J_PASSWORD_ENV "CBM_NEO4J_PASSWORD"

/* store_meta key holding the project's graph backend (no password). */
#define CBM_NEO4J_META_KEY "graph_backend"

typedef struct cbm_neo4j_config {
    char uri[CBM_NEO4J_URI_MAX];
    char user[CBM_NEO4J_USER_MAX];
    char password[CBM_NEO4J_PASSWORD_MAX];
    char database[CBM_NEO4J_DATABASE_MAX];
} cbm_neo4j_config_t;

/* Build a config from explicit values (index_repository's graph_* args).
 * uri is required. Missing user/password/database fall back to NEO4J_USER /
 * NEO4J_PASSWORD (then CBM_NEO4J_PASSWORD, then the in-process credential
 * cache) / NEO4J_DATABASE, then to "neo4j" / "" / "neo4j".
 * Returns 0, or -1 with err filled. */
int cbm_neo4j_config_resolve(const char *uri, const char *user, const char *password,
                             const char *database, cbm_neo4j_config_t *out, char *err,
                             size_t err_sz);

/* Serialize the config for store_meta (password omitted). malloc'd. */
char *cbm_neo4j_config_to_meta(const cbm_neo4j_config_t *cfg);

/* Parse a store_meta value back into a config, resolving the password the
 * same way cbm_neo4j_config_resolve does. Returns 0, or -1 with err. */
int cbm_neo4j_config_from_meta(const char *meta_json, cbm_neo4j_config_t *out, char *err,
                               size_t err_sz);

/* Remember the password for (uri, user) for the life of the process so read
 * tools served by this daemon can reconnect after an index call. */
void cbm_neo4j_config_remember(const cbm_neo4j_config_t *cfg);

/* Overwrite the password in memory. */
void cbm_neo4j_config_wipe(cbm_neo4j_config_t *cfg);

typedef struct cbm_neo4j cbm_neo4j_t;

/* Create a client (no network I/O yet). Returns NULL with err on bad URL. */
cbm_neo4j_t *cbm_neo4j_connect(const cbm_neo4j_config_t *cfg, char *err, size_t err_sz);
void cbm_neo4j_close(cbm_neo4j_t *c);
const char *cbm_neo4j_error(const cbm_neo4j_t *c);
const char *cbm_neo4j_uri(const cbm_neo4j_t *c);

/* Round-trip "RETURN 1" to verify reachability, credentials and database. */
int cbm_neo4j_ping(cbm_neo4j_t *c);

/* ── Statement batches ─────────────────────────────────────────── */

typedef struct {
    yyjson_mut_doc *doc;
    yyjson_mut_val *statements;
    int count;
} cbm_neo4j_batch_t;

typedef struct {
    yyjson_doc *doc;
    yyjson_val *results; /* array, one entry per statement */
} cbm_neo4j_result_t;

int cbm_neo4j_batch_init(cbm_neo4j_batch_t *b);
/* Append a statement; returns its parameters object (owned by b->doc). */
yyjson_mut_val *cbm_neo4j_batch_add(cbm_neo4j_batch_t *b, const char *cypher);
void cbm_neo4j_batch_free(cbm_neo4j_batch_t *b);

/* Run all statements in one transaction. out may be NULL. Returns 0, or -1
 * with cbm_neo4j_error() describing the transport or Cypher failure. */
int cbm_neo4j_batch_run(cbm_neo4j_t *c, cbm_neo4j_batch_t *b, cbm_neo4j_result_t *out);

/* Convenience: one statement. params may be NULL; it must belong to b's doc,
 * so build it via cbm_neo4j_batch_add and run with cbm_neo4j_batch_run. */

/* NULL when a 2xx body's "results" holds exactly one well-formed entry per
 * statement ({"columns":[...], "data":[{"row":[one value per column]}]});
 * otherwise a short description of the defect. cbm_neo4j_batch_run refuses
 * any response this rejects, so a malformed reply is never "no rows". */
const char *cbm_neo4j_result_shape_error(yyjson_val *results, int statements);

/* Rows of statement i: array of {"row":[...]} objects, or NULL. */
yyjson_val *cbm_neo4j_result_data(const cbm_neo4j_result_t *r, size_t stmt_index);
/* Column j of a data entry. */
yyjson_val *cbm_neo4j_row_col(yyjson_val *data_entry, size_t col);
void cbm_neo4j_result_free(cbm_neo4j_result_t *r);

/* Value helpers tolerant of null / wrong type. */
int64_t cbm_neo4j_val_int(yyjson_val *v, int64_t fallback);
/* malloc'd copy of a string value; non-strings are serialized as JSON;
 * null → NULL (or strdup(fallback) when fallback != NULL). */
char *cbm_neo4j_val_strdup(yyjson_val *v, const char *fallback);

/* True when s is a safe bare identifier for a label / relationship type. */
bool cbm_neo4j_ident_ok(const char *s);

#endif /* CBM_NEO4J_CLIENT_H */
