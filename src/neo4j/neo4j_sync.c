/*
 * neo4j_sync.c — Mirror a published project database into Neo4j.
 *
 * Two ways to bring Neo4j up to date with the local SQLite index:
 *
 *   full   Upload every node and edge as a new generation, flip
 *          CbmProject.gen to it, then delete the old generation.
 *   delta  Upload only what changed since the last sync, in place, inside
 *          ONE Neo4j transaction (one /tx/commit request), so readers see
 *          either the old graph or the new one, never a mix.
 *
 * The delta needs to know exactly what Neo4j holds. After every successful
 * sync a local state file "<db>.neo4j-state.db" records, per node and edge,
 * its id and a hash of the row that was uploaded (plus a node's label and an
 * edge's endpoints and type, which the delta needs to find it in Neo4j). The
 * next sync merge-joins the project's rows against that record by id: a new
 * id is an added row, a missing id a removed one, and a different hash a
 * changed one (a node keeps its Neo4j identity; a changed edge is re-created).
 *
 * The comparison is by id AND full row content, so it is correct for any
 * local database, even one written from scratch: re-indexing rewrites the
 * SQLite file each time (a new db_uid) but keeps the ids of unchanged rows,
 * and should ids ever be renumbered, the delta just grows past its limit.
 * What it does rely on is that the record describes what Neo4j serves, so a
 * delta is used only when Neo4j's CbmProject still carries the generation,
 * graph number and counts the record was written for -- re-checked inside
 * the delta transaction itself, which fails (and rolls back) if anything
 * moved. Otherwise, or when the change is too large for one transaction, the
 * full upload runs and rewrites the record.
 */
#include "neo4j/neo4j_sync.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "store/store.h"

#include "sqlite3.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define N4_SYNC_BATCH_ROWS 2000
#define N4_DELETE_BATCH 10000
#define N4_SIDECAR_SUFFIX ".graph.json"
#define N4_STATE_SUFFIX ".neo4j-state.db"
#define N4_SIDECAR_MAX (64 * 1024)
#define N4_GEN_MAX 128
#define N4_PATH_MAX 4096

/* Delta limits. Rows = node/edge operations sent; bytes = their approximate
 * payload. A larger change runs as a full upload (batched, not one
 * transaction). CBM_NEO4J_DELTA_MAX_ROWS overrides the row limit; 0 turns
 * the delta off. */
#define N4_DELTA_MAX_ROWS_DEFAULT 50000
#define N4_DELTA_MAX_BYTES (32LL * 1024 * 1024)
#define N4_DELTA_ENV "CBM_NEO4J_DELTA_MAX_ROWS"

/* Layout of the local delta state file. Bump to force one full upload. */
#define N4_STATE_FORMAT 1

/* Columns written explicitly; a flattened property never overwrites them. */
static const char *const k_reserved_node_keys[] = {
    "db",         "gen",      "project",         "id",
    "label",      "name",     "qualified_name",  "file_path",
    "start_line", "end_line", "properties_json", "prop_keys",
    NULL};
static const char *const k_reserved_edge_keys[] = {"id", "type", "properties_json", "prop_keys",
                                                   NULL};

/* Layout version of a synced graph (CbmProject.sync_format). Bump when the
 * stored shape changes; an older graph is re-uploaded on the next sync even
 * if the SQLite generation is unchanged. 2: prop_keys on nodes and edges. */
#define N4_SYNC_FORMAT 2

static void sync_err(char *err, size_t err_sz, const char *what, const char *detail) {
    if (err && err_sz) {
        (void)snprintf(err, err_sz, "%s%s%s", what, detail ? ": " : "", detail ? detail : "");
    }
}

/* What the calling thread's last sync did (cbm_neo4j_last_sync_stats). */
static CBM_TLS cbm_neo4j_sync_stats_t g_last_stats;

void cbm_neo4j_last_sync_stats(cbm_neo4j_sync_stats_t *out) {
    if (out) {
        *out = g_last_stats;
    }
}

static void stats_reset(void) {
    memset(&g_last_stats, 0, sizeof(g_last_stats));
}

int cbm_neo4j_sync_stats_format(const cbm_neo4j_sync_stats_t *st, char *out, size_t out_sz) {
    if (!st || !out || out_sz == 0) {
        return -1;
    }
    int n = 0;
    switch (st->mode) {
    case CBM_NEO4J_SYNC_DELTA:
        n = snprintf(out, out_sz,
                     "delta: nodes +%" PRId64 " ~%" PRId64 " -%" PRId64 ", edges +%" PRId64
                     " ~%" PRId64 " -%" PRId64 " (graph: %" PRId64 " nodes, %" PRId64 " edges)",
                     st->nodes_added, st->nodes_changed, st->nodes_removed, st->edges_added,
                     st->edges_changed, st->edges_removed, st->nodes_total, st->edges_total);
        break;
    case CBM_NEO4J_SYNC_FULL:
        n = snprintf(out, out_sz, "full: %" PRId64 " nodes, %" PRId64 " edges%s%s%s",
                     st->nodes_total, st->edges_total, st->detail[0] ? " (" : "", st->detail,
                     st->detail[0] ? ")" : "");
        break;
    case CBM_NEO4J_SYNC_SKIPPED:
        n = snprintf(out, out_sz, "unchanged: Neo4j already holds this generation");
        break;
    default:
        n = snprintf(out, out_sz, "%s", "");
        break;
    }
    return n < 0 ? -1 : 0;
}

/* ── Sidecar marker ─────────────────────────────────────────────── */

int cbm_neo4j_sidecar_path(const char *db_path, char *out, size_t out_sz) {
    if (!db_path || !*db_path) {
        return -1;
    }
    int n = snprintf(out, out_sz, "%s%s", db_path, N4_SIDECAR_SUFFIX);
    return (n > 0 && (size_t)n < out_sz) ? 0 : -1;
}

int cbm_neo4j_state_path(const char *db_path, char *out, size_t out_sz) {
    if (!db_path || !*db_path) {
        return -1;
    }
    int n = snprintf(out, out_sz, "%s%s", db_path, N4_STATE_SUFFIX);
    return (n > 0 && (size_t)n < out_sz) ? 0 : -1;
}

int cbm_neo4j_sidecar_read(const char *db_path, cbm_neo4j_config_t *cfg, char *synced_generation,
                           size_t synced_sz, char *err, size_t err_sz) {
    char path[N4_PATH_MAX];
    if (synced_generation && synced_sz) {
        synced_generation[0] = '\0';
    }
    if (cbm_neo4j_sidecar_path(db_path, path, sizeof(path)) != 0) {
        return 1;
    }
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return 1;
    }
    char *buf = malloc(N4_SIDECAR_MAX + 1);
    if (!buf) {
        (void)fclose(f);
        return -1;
    }
    size_t n = fread(buf, 1, N4_SIDECAR_MAX, f);
    (void)fclose(f);
    buf[n] = '\0';
    int rc = cbm_neo4j_config_from_meta(buf, cfg, err, err_sz);
    if (rc == 0 && synced_generation && synced_sz) {
        yyjson_doc *d = yyjson_read(buf, n, 0);
        const char *g = yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(d), "synced_generation"));
        (void)snprintf(synced_generation, synced_sz, "%s", g ? g : "");
        yyjson_doc_free(d);
    }
    free(buf);
    return rc == 0 ? 0 : -1;
}

int cbm_neo4j_sidecar_locate(const char *db_path, const char *project, char *out, size_t out_sz) {
    char path[4096];
    if (cbm_neo4j_sidecar_path(db_path, path, sizeof(path)) == 0) {
        FILE *f = cbm_fopen(path, "rb");
        if (f) {
            (void)fclose(f);
            (void)snprintf(out, out_sz, "%s", db_path);
            return 0;
        }
    }
    if (!project || !project[0]) {
        return 1;
    }
    /* The database was found under another file name (the #704 internal-name
     * scan): its marker still carries the project's canonical name. */
    size_t dir_len = 0;
    for (const char *p = db_path; *p; p++) {
        if (*p == '/' || *p == '\\') {
            dir_len = (size_t)(p - db_path) + 1;
        }
    }
    char canonical[N4_PATH_MAX];
    int n = snprintf(canonical, sizeof(canonical), "%.*s%s.db", (int)dir_len, db_path, project);
    if (n <= 0 || (size_t)n >= sizeof(canonical) ||
        cbm_neo4j_sidecar_path(canonical, path, sizeof(path)) != 0) {
        return 1;
    }
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return 1;
    }
    (void)fclose(f);
    (void)snprintf(out, out_sz, "%s", canonical);
    return 0;
}

int cbm_neo4j_sidecar_field(const char *db_path, const char *key, char *out, size_t out_sz) {
    char path[N4_PATH_MAX];
    out[0] = '\0';
    if (cbm_neo4j_sidecar_path(db_path, path, sizeof(path)) != 0) {
        return -1;
    }
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return -1;
    }
    char *buf = malloc(N4_SIDECAR_MAX + 1);
    if (!buf) {
        (void)fclose(f);
        return -1;
    }
    size_t n = fread(buf, 1, N4_SIDECAR_MAX, f);
    (void)fclose(f);
    yyjson_doc *d = yyjson_read(buf, n, 0);
    free(buf);
    const char *v = yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(d), key));
    (void)snprintf(out, out_sz, "%s", v ? v : "");
    yyjson_doc_free(d);
    return 0;
}

int cbm_neo4j_sidecar_write(const char *db_path, const cbm_neo4j_config_t *cfg,
                            const char *synced_generation) {
    return cbm_neo4j_sidecar_write_status(db_path, cfg, synced_generation, NULL);
}

static int sidecar_write_ex(const char *db_path, const cbm_neo4j_config_t *cfg,
                            const char *synced_generation, const char *last_sync_error,
                            const char *last_sync) {
    char path[N4_PATH_MAX];
    char tmp[N4_PATH_MAX + 8];
    if (cbm_neo4j_sidecar_path(db_path, path, sizeof(path)) != 0) {
        return -1;
    }
    (void)snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    char *meta = cbm_neo4j_config_to_meta(cfg);
    if (!meta) {
        return -1;
    }
    yyjson_doc *src = yyjson_read(meta, strlen(meta), 0);
    free(meta);
    yyjson_mut_doc *d = yyjson_doc_mut_copy(src, NULL);
    yyjson_doc_free(src);
    if (!d) {
        return -1;
    }
    yyjson_mut_obj_add_strcpy(d, yyjson_mut_doc_get_root(d), "synced_generation",
                              synced_generation ? synced_generation : "");
    if (last_sync_error && last_sync_error[0]) {
        yyjson_mut_obj_add_strcpy(d, yyjson_mut_doc_get_root(d), "last_sync_error",
                                  last_sync_error);
    }
    if (last_sync && last_sync[0]) {
        yyjson_mut_obj_add_strcpy(d, yyjson_mut_doc_get_root(d), "last_sync", last_sync);
    }
    size_t len = 0;
    char *json = yyjson_mut_write(d, YYJSON_WRITE_PRETTY, &len);
    yyjson_mut_doc_free(d);
    if (!json) {
        return -1;
    }
    FILE *f = cbm_fopen(tmp, "wb");
    if (!f) {
        free(json);
        return -1;
    }
    bool ok = fwrite(json, 1, len, f) == len;
    ok = (fclose(f) == 0) && ok;
    free(json);
    if (!ok || cbm_rename_replace(tmp, path) != 0) {
        (void)cbm_unlink(tmp);
        return -1;
    }
    return 0;
}

int cbm_neo4j_sidecar_write_status(const char *db_path, const cbm_neo4j_config_t *cfg,
                                   const char *synced_generation, const char *last_sync_error) {
    return sidecar_write_ex(db_path, cfg, synced_generation, last_sync_error, NULL);
}

/* Remove the delta state file (and a leftover temporary). The next sync of
 * the project is then a full upload. */
static void state_remove(const char *db_path) {
    char path[N4_PATH_MAX];
    char tmp[N4_PATH_MAX + 8];
    if (cbm_neo4j_state_path(db_path, path, sizeof(path)) != 0) {
        return;
    }
    (void)cbm_unlink(path);
    (void)snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    (void)cbm_unlink(tmp);
    (void)snprintf(tmp, sizeof(tmp), "%s-journal", path);
    (void)cbm_unlink(tmp);
}

int cbm_neo4j_sidecar_remove(const char *db_path) {
    char path[N4_PATH_MAX];
    if (cbm_neo4j_sidecar_path(db_path, path, sizeof(path)) != 0) {
        return -1;
    }
    (void)cbm_unlink(path);
    state_remove(db_path);
    return 0;
}

/* ── Statement helpers ──────────────────────────────────────────── */

static int run_simple(cbm_neo4j_t *c, const char *cypher, const char *db, int64_t gen,
                      cbm_neo4j_result_t *out) {
    cbm_neo4j_batch_t b;
    if (cbm_neo4j_batch_init(&b) != 0) {
        return -1;
    }
    yyjson_mut_val *params = cbm_neo4j_batch_add(&b, cypher);
    if (db) {
        yyjson_mut_obj_add_strcpy(b.doc, params, "db", db);
    }
    yyjson_mut_obj_add_int(b.doc, params, "gen", gen);
    int rc = cbm_neo4j_batch_run(c, &b, out);
    cbm_neo4j_batch_free(&b);
    return rc;
}

int cbm_neo4j_ensure_schema(cbm_neo4j_t *c) {
    static const char *const stmts[] = {
        "CREATE CONSTRAINT cbm_project_name IF NOT EXISTS FOR (p:CbmProject) REQUIRE p.name IS "
        "UNIQUE",
        "CREATE INDEX cbm_node_db IF NOT EXISTS FOR (n:CbmNode) ON (n.db)",
        "CREATE INDEX cbm_node_id IF NOT EXISTS FOR (n:CbmNode) ON (n.db, n.gen, n.id)",
        "CREATE INDEX cbm_node_qn IF NOT EXISTS FOR (n:CbmNode) ON (n.db, n.gen, n.qualified_name)",
        "CREATE INDEX cbm_node_name IF NOT EXISTS FOR (n:CbmNode) ON (n.db, n.gen, n.name)",
        "CREATE INDEX cbm_node_file IF NOT EXISTS FOR (n:CbmNode) ON (n.db, n.gen, n.file_path)",
        "CREATE INDEX cbm_node_label IF NOT EXISTS FOR (n:CbmNode) ON (n.db, n.gen, n.label)",
        NULL};
    /* Schema statements cannot share a transaction with each other in every
     * Neo4j version, so send them one per request. */
    for (int i = 0; stmts[i]; i++) {
        cbm_neo4j_batch_t b;
        if (cbm_neo4j_batch_init(&b) != 0) {
            return -1;
        }
        (void)cbm_neo4j_batch_add(&b, stmts[i]);
        int rc = cbm_neo4j_batch_run(c, &b, NULL);
        cbm_neo4j_batch_free(&b);
        if (rc != 0) {
            return -1;
        }
    }
    /* Best effort: wait for freshly created indexes before bulk MATCHes. */
    cbm_neo4j_batch_t b;
    if (cbm_neo4j_batch_init(&b) == 0) {
        (void)cbm_neo4j_batch_add(&b, "CALL db.awaitIndexes(300)");
        (void)cbm_neo4j_batch_run(c, &b, NULL);
        cbm_neo4j_batch_free(&b);
    }
    return 0;
}

static bool key_reserved(const char *k, const char *const *reserved) {
    for (int i = 0; reserved[i]; i++) {
        if (strcmp(k, reserved[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Copy top-level scalar members of properties_json into obj. Arrays of
 * scalars are kept too (Neo4j stores homogeneous lists); nested objects stay
 * only inside properties_json. prop_keys lists every top-level key -- the
 * set SQLite's json_each reports for get_graph_schema -- flattened or not. */
static void flatten_props(yyjson_mut_doc *d, yyjson_mut_val *obj, const char *json,
                          const char *const *reserved) {
    yyjson_mut_val *prop_keys = yyjson_mut_arr(d);
    yyjson_mut_obj_add_val(d, obj, "prop_keys", prop_keys);
    if (!json || !*json || strcmp(json, "{}") == 0) {
        return;
    }
    yyjson_doc *pd = yyjson_read(json, strlen(json), 0);
    yyjson_val *root = pd ? yyjson_doc_get_root(pd) : NULL;
    if (!yyjson_is_obj(root)) {
        yyjson_doc_free(pd);
        return;
    }
    size_t idx;
    size_t max;
    yyjson_val *k;
    yyjson_val *v;
    yyjson_obj_foreach(root, idx, max, k, v) {
        const char *key = yyjson_get_str(k);
        if (key) {
            yyjson_mut_arr_add_strcpy(d, prop_keys, key);
        }
        if (!key || key_reserved(key, reserved)) {
            continue;
        }
        if (yyjson_is_str(v) || yyjson_is_num(v) || yyjson_is_bool(v)) {
            yyjson_mut_obj_add(obj, yyjson_mut_strcpy(d, key), yyjson_val_mut_copy(d, v));
        } else if (yyjson_is_arr(v) && yyjson_arr_size(v) > 0) {
            /* Only homogeneous string or number lists are valid Neo4j values. */
            yyjson_val *first = yyjson_arr_get_first(v);
            bool all_str = true;
            bool all_num = true;
            size_t ai;
            size_t amax;
            yyjson_val *av;
            yyjson_arr_foreach(v, ai, amax, av) {
                all_str = all_str && yyjson_is_str(av);
                all_num = all_num && yyjson_is_num(av);
            }
            if ((all_str || all_num) && first) {
                yyjson_mut_obj_add(obj, yyjson_mut_strcpy(d, key), yyjson_val_mut_copy(d, v));
            }
        }
    }
    yyjson_doc_free(pd);
}

static const char *col_text(sqlite3_stmt *st, int i) {
    const char *s = (const char *)sqlite3_column_text(st, i);
    return s ? s : "";
}

/* ── Row encoding (shared by the full and the delta upload) ─────── */

/* Column order of every node / edge read below (node_row, node_hash, ...). */
#define N4_NODE_SELECT                                                                   \
    "SELECT id, project, label, name, qualified_name, file_path, start_line, end_line, " \
    "properties FROM nodes"
#define N4_EDGE_SELECT "SELECT id, source_id, target_id, type, properties FROM edges"

static void node_create_cypher(char *out, size_t sz, const char *label) {
    if (cbm_neo4j_ident_ok(label)) {
        (void)snprintf(out, sz, "UNWIND $rows AS r CREATE (n:CbmNode:`%s`) SET n = r", label);
    } else {
        (void)snprintf(out, sz, "UNWIND $rows AS r CREATE (n:CbmNode) SET n = r");
    }
}

/* Types that are not plain identifiers land on CBM_EDGE with the real type
 * kept in the `type` property. */
static const char *edge_rel_type(const char *type) {
    return cbm_neo4j_ident_ok(type) ? type : "CBM_EDGE";
}

static void edge_create_cypher(char *out, size_t sz, const char *type) {
    (void)snprintf(out, sz,
                   "UNWIND $rows AS r "
                   "MATCH (a:CbmNode {db: $db, gen: $gen, id: r.s}) "
                   "MATCH (b:CbmNode {db: $db, gen: $gen, id: r.t}) "
                   "CREATE (a)-[e:`%s`]->(b) SET e = r.p",
                   edge_rel_type(type));
}

/* The Neo4j properties of the node in st's current row. */
static void node_row(yyjson_mut_doc *d, yyjson_mut_val *row, sqlite3_stmt *st, const char *db,
                     int64_t gen) {
    const char *props = col_text(st, 8);
    /* Flattened properties first so explicit columns win on collision. */
    flatten_props(d, row, props, k_reserved_node_keys);
    yyjson_mut_obj_add_strcpy(d, row, "db", db);
    yyjson_mut_obj_add_int(d, row, "gen", gen);
    yyjson_mut_obj_add_int(d, row, "id", sqlite3_column_int64(st, 0));
    yyjson_mut_obj_add_strcpy(d, row, "project", col_text(st, 1));
    yyjson_mut_obj_add_strcpy(d, row, "label", col_text(st, 2));
    yyjson_mut_obj_add_strcpy(d, row, "name", col_text(st, 3));
    yyjson_mut_obj_add_strcpy(d, row, "qualified_name", col_text(st, 4));
    yyjson_mut_obj_add_strcpy(d, row, "file_path", col_text(st, 5));
    yyjson_mut_obj_add_int(d, row, "start_line", sqlite3_column_int(st, 6));
    yyjson_mut_obj_add_int(d, row, "end_line", sqlite3_column_int(st, 7));
    yyjson_mut_obj_add_strcpy(d, row, "properties_json", props[0] ? props : "{}");
}

/* {s, t, p}: the endpoints and Neo4j properties of the edge in st's row. */
static void edge_row(yyjson_mut_doc *d, yyjson_mut_val *row, sqlite3_stmt *st) {
    yyjson_mut_obj_add_int(d, row, "s", sqlite3_column_int64(st, 1));
    yyjson_mut_obj_add_int(d, row, "t", sqlite3_column_int64(st, 2));
    yyjson_mut_val *p = yyjson_mut_obj(d);
    const char *props = col_text(st, 4);
    flatten_props(d, p, props, k_reserved_edge_keys);
    yyjson_mut_obj_add_int(d, p, "id", sqlite3_column_int64(st, 0));
    yyjson_mut_obj_add_strcpy(d, p, "type", col_text(st, 3));
    yyjson_mut_obj_add_strcpy(d, p, "properties_json", props[0] ? props : "{}");
    yyjson_mut_obj_add_val(d, row, "p", p);
}

/* FNV-1a 64 over length-prefixed fields: equal hash <=> (practically) equal
 * row. The state file is local to this machine, so byte order is moot. */
#define N4_FNV_OFFSET 0xcbf29ce484222325ULL
#define N4_FNV_PRIME 0x100000001b3ULL

static uint64_t h_bytes(uint64_t h, const void *p, size_t n) {
    const unsigned char *b = p;
    for (size_t i = 0; i < n; i++) {
        h ^= b[i];
        h *= N4_FNV_PRIME;
    }
    return h;
}

static uint64_t h_str(uint64_t h, const char *s) {
    uint64_t len = (uint64_t)strlen(s);
    h = h_bytes(h, &len, sizeof(len));
    return h_bytes(h, s, (size_t)len);
}

static uint64_t h_int(uint64_t h, int64_t v) {
    return h_bytes(h, &v, sizeof(v));
}

/* Every column the Neo4j node is built from (node_row). */
static int64_t node_hash(sqlite3_stmt *st) {
    uint64_t h = N4_FNV_OFFSET;
    for (int i = 1; i <= 5; i++) {
        h = h_str(h, col_text(st, i));
    }
    h = h_int(h, sqlite3_column_int64(st, 6));
    h = h_int(h, sqlite3_column_int64(st, 7));
    return (int64_t)h_str(h, col_text(st, 8));
}

/* Every column the Neo4j edge is built from (edge_row). */
static int64_t edge_hash(sqlite3_stmt *st) {
    uint64_t h = N4_FNV_OFFSET;
    h = h_int(h, sqlite3_column_int64(st, 1));
    h = h_int(h, sqlite3_column_int64(st, 2));
    h = h_str(h, col_text(st, 3));
    return (int64_t)h_str(h, col_text(st, 4));
}

/* Rough JSON size of a row, for the delta's payload limit. */
static int64_t row_bytes(sqlite3_stmt *st, int ncols) {
    int64_t n = 64;
    for (int i = 0; i < ncols; i++) {
        n += sqlite3_column_bytes(st, i);
    }
    return n;
}

/* ── Delta state file ───────────────────────────────────────────── */

static const char k_state_schema[] =
    "CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);"
    "CREATE TABLE nodes(id INTEGER PRIMARY KEY, h INTEGER NOT NULL, label TEXT NOT NULL);"
    "CREATE TABLE edges(id INTEGER PRIMARY KEY, h INTEGER NOT NULL, s INTEGER NOT NULL,"
    " t INTEGER NOT NULL, type TEXT NOT NULL);";

typedef struct {
    char generation[N4_GEN_MAX]; /* SQLite generation Neo4j was brought to */
    int64_t gen;                 /* CbmProject.gen it lives under */
    int64_t nodes;
    int64_t edges;
    char uri[CBM_NEO4J_URI_MAX];
} n4_state_meta_t;

static int state_meta_put(sqlite3 *db, const char *k, const char *v) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO meta(k, v) VALUES(?1, ?2)", -1, &st, NULL) !=
        SQLITE_OK) {
        return -1;
    }
    (void)sqlite3_bind_text(st, 1, k, -1, SQLITE_STATIC);
    (void)sqlite3_bind_text(st, 2, v, -1, SQLITE_STATIC);
    int rc = sqlite3_step(st) == SQLITE_DONE ? 0 : -1;
    sqlite3_finalize(st);
    return rc;
}

static int state_meta_put_int(sqlite3 *db, const char *k, int64_t v) {
    char buf[32];
    (void)snprintf(buf, sizeof(buf), "%" PRId64, v);
    return state_meta_put(db, k, buf);
}

static int state_meta_write(sqlite3 *db, const n4_state_meta_t *m) {
    return (state_meta_put_int(db, "format", N4_STATE_FORMAT) == 0 &&
            state_meta_put_int(db, "sync_format", N4_SYNC_FORMAT) == 0 &&
            state_meta_put(db, "generation", m->generation) == 0 &&
            state_meta_put_int(db, "gen", m->gen) == 0 &&
            state_meta_put_int(db, "nodes", m->nodes) == 0 &&
            state_meta_put_int(db, "edges", m->edges) == 0 &&
            state_meta_put(db, "uri", m->uri) == 0)
               ? 0
               : -1;
}

/* Fill m from the state file; -1 when a key is missing or the file was
 * written in another layout. */
static int state_meta_read(sqlite3 *db, n4_state_meta_t *m) {
    memset(m, 0, sizeof(*m));
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT k, v FROM meta", -1, &st, NULL) != SQLITE_OK) {
        return -1;
    }
    int seen = 0;
    int64_t format = -1;
    int64_t sync_format = -1;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *k = col_text(st, 0);
        const char *v = col_text(st, 1);
        if (strcmp(k, "format") == 0) {
            format = strtoll(v, NULL, 10);
            seen |= 1;
        } else if (strcmp(k, "sync_format") == 0) {
            sync_format = strtoll(v, NULL, 10);
            seen |= 2;
        } else if (strcmp(k, "generation") == 0) {
            (void)snprintf(m->generation, sizeof(m->generation), "%s", v);
            seen |= 4;
        } else if (strcmp(k, "gen") == 0) {
            m->gen = strtoll(v, NULL, 10);
            seen |= 8;
        } else if (strcmp(k, "nodes") == 0) {
            m->nodes = strtoll(v, NULL, 10);
            seen |= 16;
        } else if (strcmp(k, "edges") == 0) {
            m->edges = strtoll(v, NULL, 10);
            seen |= 32;
        } else if (strcmp(k, "uri") == 0) {
            (void)snprintf(m->uri, sizeof(m->uri), "%s", v);
            seen |= 64;
        }
    }
    sqlite3_finalize(st);
    return (seen == 127 && format == N4_STATE_FORMAT && sync_format == N4_SYNC_FORMAT) ? 0 : -1;
}

/* Built alongside a full upload, published (renamed into place) only once
 * the upload and the old generation's removal both succeeded. Any failure
 * just leaves no state file: the next sync is full again. */
typedef struct {
    sqlite3 *db;
    sqlite3_stmt *ins_node;
    sqlite3_stmt *ins_edge;
    char path[N4_PATH_MAX];
    char tmp[N4_PATH_MAX + 8];
    bool failed;
} n4_state_writer_t;

static void state_writer_close(n4_state_writer_t *w) {
    if (w->ins_node) {
        sqlite3_finalize(w->ins_node);
    }
    if (w->ins_edge) {
        sqlite3_finalize(w->ins_edge);
    }
    if (w->db) {
        sqlite3_close(w->db);
    }
    w->ins_node = NULL;
    w->ins_edge = NULL;
    w->db = NULL;
}

static void state_writer_open(n4_state_writer_t *w, const char *db_path) {
    memset(w, 0, sizeof(*w));
    if (cbm_neo4j_state_path(db_path, w->path, sizeof(w->path)) != 0) {
        w->failed = true;
        return;
    }
    (void)snprintf(w->tmp, sizeof(w->tmp), "%s.tmp", w->path);
    (void)cbm_unlink(w->tmp);
    if (sqlite3_open_v2(w->tmp, &w->db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) !=
            SQLITE_OK ||
        sqlite3_exec(w->db, "PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;", NULL, NULL,
                     NULL) != SQLITE_OK ||
        sqlite3_exec(w->db, k_state_schema, NULL, NULL, NULL) != SQLITE_OK ||
        sqlite3_exec(w->db, "BEGIN", NULL, NULL, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(w->db, "INSERT INTO nodes(id, h, label) VALUES(?1, ?2, ?3)", -1,
                           &w->ins_node, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(w->db, "INSERT INTO edges(id, h, s, t, type) VALUES(?1, ?2, ?3, ?4, ?5)",
                           -1, &w->ins_edge, NULL) != SQLITE_OK) {
        cbm_log_warn("neo4j.sync.state", "phase", "open", "err",
                     w->db ? sqlite3_errmsg(w->db) : "open failed");
        w->failed = true;
    }
}

static void state_add_node(n4_state_writer_t *w, sqlite3_stmt *st) {
    if (!w || w->failed) {
        return;
    }
    (void)sqlite3_bind_int64(w->ins_node, 1, sqlite3_column_int64(st, 0));
    (void)sqlite3_bind_int64(w->ins_node, 2, node_hash(st));
    (void)sqlite3_bind_text(w->ins_node, 3, col_text(st, 2), -1, SQLITE_TRANSIENT);
    w->failed = sqlite3_step(w->ins_node) != SQLITE_DONE;
    (void)sqlite3_reset(w->ins_node);
}

static void state_add_edge(n4_state_writer_t *w, sqlite3_stmt *st) {
    if (!w || w->failed) {
        return;
    }
    (void)sqlite3_bind_int64(w->ins_edge, 1, sqlite3_column_int64(st, 0));
    (void)sqlite3_bind_int64(w->ins_edge, 2, edge_hash(st));
    (void)sqlite3_bind_int64(w->ins_edge, 3, sqlite3_column_int64(st, 1));
    (void)sqlite3_bind_int64(w->ins_edge, 4, sqlite3_column_int64(st, 2));
    (void)sqlite3_bind_text(w->ins_edge, 5, col_text(st, 3), -1, SQLITE_TRANSIENT);
    w->failed = sqlite3_step(w->ins_edge) != SQLITE_DONE;
    (void)sqlite3_reset(w->ins_edge);
}

/* Drop the half-built file. The published one (if any) is left alone: it
 * still describes the Neo4j graph when the full upload failed and rolled
 * back, and is refused by the delta's checks when it did not. */
static void state_writer_discard(n4_state_writer_t *w) {
    state_writer_close(w);
    if (w->tmp[0]) {
        (void)cbm_unlink(w->tmp);
    }
}

static void state_writer_publish(n4_state_writer_t *w, const n4_state_meta_t *m) {
    bool ok = !w->failed && state_meta_write(w->db, m) == 0 &&
              sqlite3_exec(w->db, "COMMIT", NULL, NULL, NULL) == SQLITE_OK;
    if (!ok && w->db) {
        cbm_log_warn("neo4j.sync.state", "phase", "write", "err", sqlite3_errmsg(w->db));
    }
    state_writer_close(w);
    if (!ok || cbm_rename_replace(w->tmp, w->path) != 0) {
        (void)cbm_unlink(w->tmp);
        (void)cbm_unlink(w->path); /* stale now: the graph moved on */
    }
}

/* ── Full upload ────────────────────────────────────────────────── */

typedef struct {
    cbm_neo4j_t *c;
    const char *db;
    int64_t gen;
    cbm_neo4j_batch_t batch;
    yyjson_mut_val *rows;
    int row_count;
    char group[256]; /* label or edge type of the open batch */
    bool is_edge;
    int64_t total;
} n4_stream_t;

static int stream_open_batch(n4_stream_t *s, const char *group) {
    if (cbm_neo4j_batch_init(&s->batch) != 0) {
        return -1;
    }
    (void)snprintf(s->group, sizeof(s->group), "%s", group);
    char cypher[768];
    if (!s->is_edge) {
        node_create_cypher(cypher, sizeof(cypher), group);
    } else {
        edge_create_cypher(cypher, sizeof(cypher), group);
    }
    yyjson_mut_val *params = cbm_neo4j_batch_add(&s->batch, cypher);
    yyjson_mut_obj_add_strcpy(s->batch.doc, params, "db", s->db);
    yyjson_mut_obj_add_int(s->batch.doc, params, "gen", s->gen);
    s->rows = yyjson_mut_arr(s->batch.doc);
    yyjson_mut_obj_add_val(s->batch.doc, params, "rows", s->rows);
    s->row_count = 0;
    return 0;
}

static int stream_flush(n4_stream_t *s) {
    if (!s->batch.doc) {
        return 0;
    }
    int rc = s->row_count > 0 ? cbm_neo4j_batch_run(s->c, &s->batch, NULL) : 0;
    s->total += s->row_count;
    cbm_neo4j_batch_free(&s->batch);
    s->rows = NULL;
    s->row_count = 0;
    s->group[0] = '\0';
    return rc;
}

/* Returns the row object to fill, flushing/reopening on group change or when
 * the batch is full. NULL on failure. */
static yyjson_mut_val *stream_row(n4_stream_t *s, const char *group) {
    if (s->batch.doc && (strcmp(s->group, group) != 0 || s->row_count >= N4_SYNC_BATCH_ROWS)) {
        if (stream_flush(s) != 0) {
            return NULL;
        }
    }
    if (!s->batch.doc && stream_open_batch(s, group) != 0) {
        return NULL;
    }
    yyjson_mut_val *row = yyjson_mut_obj(s->batch.doc);
    yyjson_mut_arr_append(s->rows, row);
    s->row_count++;
    return row;
}

static int sync_nodes(n4_stream_t *s, sqlite3 *db, n4_state_writer_t *w, char *err, size_t err_sz) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, N4_NODE_SELECT " ORDER BY label, id", -1, &st, NULL) != SQLITE_OK) {
        sync_err(err, err_sz, "read nodes", sqlite3_errmsg(db));
        return -1;
    }
    int rc = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        yyjson_mut_val *row = stream_row(s, col_text(st, 2));
        if (!row) {
            sync_err(err, err_sz, "write nodes to Neo4j", cbm_neo4j_error(s->c));
            rc = -1;
            break;
        }
        node_row(s->batch.doc, row, st, s->db, s->gen);
        state_add_node(w, st);
    }
    sqlite3_finalize(st);
    if (rc == 0 && stream_flush(s) != 0) {
        sync_err(err, err_sz, "write nodes to Neo4j", cbm_neo4j_error(s->c));
        rc = -1;
    }
    cbm_neo4j_batch_free(&s->batch);
    return rc;
}

static int sync_edges(n4_stream_t *s, sqlite3 *db, n4_state_writer_t *w, char *err, size_t err_sz) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, N4_EDGE_SELECT " ORDER BY type, id", -1, &st, NULL) != SQLITE_OK) {
        sync_err(err, err_sz, "read edges", sqlite3_errmsg(db));
        return -1;
    }
    int rc = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        yyjson_mut_val *row = stream_row(s, col_text(st, 3));
        if (!row) {
            sync_err(err, err_sz, "write edges to Neo4j", cbm_neo4j_error(s->c));
            rc = -1;
            break;
        }
        edge_row(s->batch.doc, row, st);
        state_add_edge(w, st);
    }
    sqlite3_finalize(st);
    if (rc == 0 && stream_flush(s) != 0) {
        sync_err(err, err_sz, "write edges to Neo4j", cbm_neo4j_error(s->c));
        rc = -1;
    }
    cbm_neo4j_batch_free(&s->batch);
    return rc;
}

/* Delete nodes of db matching `where` (a predicate on n using $gen) in
 * bounded transactions. */
static int delete_loop(cbm_neo4j_t *c, const char *db, const char *where, int64_t gen) {
    char cypher[512];
    (void)snprintf(cypher, sizeof(cypher),
                   "MATCH (n:CbmNode {db: $db}) WHERE %s WITH n LIMIT %d DETACH DELETE n "
                   "RETURN count(*)",
                   where, N4_DELETE_BATCH);
    for (;;) {
        cbm_neo4j_result_t r;
        if (run_simple(c, cypher, db, gen, &r) != 0) {
            return -1;
        }
        yyjson_val *data = cbm_neo4j_result_data(&r, 0);
        int64_t n = cbm_neo4j_val_int(cbm_neo4j_row_col(yyjson_arr_get_first(data), 0), 0);
        cbm_neo4j_result_free(&r);
        if (n <= 0) {
            return 0;
        }
    }
}

/* Parameters every CbmProject update sets from the local project row. */
static void add_project_params(yyjson_mut_doc *d, yyjson_mut_val *params, const char *project,
                               const char *generation, const cbm_project_t *proj, int64_t nodes,
                               int64_t edges) {
    yyjson_mut_obj_add_int(d, params, "format", N4_SYNC_FORMAT);
    yyjson_mut_obj_add_strcpy(d, params, "db", project);
    yyjson_mut_obj_add_strcpy(d, params, "generation", generation);
    yyjson_mut_obj_add_strcpy(d, params, "root", proj && proj->root_path ? proj->root_path : "");
    yyjson_mut_obj_add_strcpy(d, params, "indexed_at",
                              proj && proj->indexed_at ? proj->indexed_at : "");
    yyjson_mut_obj_add_int(d, params, "nodes", nodes);
    yyjson_mut_obj_add_int(d, params, "edges", edges);
}

static int full_sync(cbm_neo4j_t *c, sqlite3 *db, const char *db_path, const char *project,
                     const char *generation, const cbm_project_t *proj, const char *reason,
                     char *err, size_t err_sz) {
    /* Current generation; a CbmProject is created on first sync. */
    cbm_neo4j_result_t r;
    if (run_simple(c, "MERGE (p:CbmProject {name: $db}) RETURN coalesce(p.gen, 0)", project, 0,
                   &r) != 0) {
        sync_err(err, err_sz, "read Neo4j project state", cbm_neo4j_error(c));
        return -1;
    }
    /* The cleanup below deletes every generation except old_gen, so old_gen
     * must be the published one: exactly one integer row, never a default. */
    yyjson_val *gen_rows = cbm_neo4j_result_data(&r, 0);
    yyjson_val *gen_val = cbm_neo4j_row_col(yyjson_arr_get_first(gen_rows), 0);
    if (yyjson_arr_size(gen_rows) != 1 || !yyjson_is_int(gen_val)) {
        cbm_neo4j_result_free(&r);
        sync_err(err, err_sz, "read Neo4j project state", "unexpected generation reply");
        return -1;
    }
    int64_t old_gen = yyjson_get_sint(gen_val);
    cbm_neo4j_result_free(&r);
    int64_t new_gen = old_gen + 1;

    /* Leftovers of an interrupted earlier sync. */
    if (delete_loop(c, project, "n.gen <> $gen", old_gen) != 0) {
        sync_err(err, err_sz, "clean up stale Neo4j generation", cbm_neo4j_error(c));
        return -1;
    }

    n4_state_writer_t w;
    state_writer_open(&w, db_path);
    n4_stream_t s;
    memset(&s, 0, sizeof(s));
    s.c = c;
    s.db = project;
    s.gen = new_gen;
    if (sync_nodes(&s, db, &w, err, err_sz) != 0) {
        goto fail_partial;
    }
    int64_t node_total = s.total;
    s.total = 0;
    s.is_edge = true;
    if (sync_edges(&s, db, &w, err, err_sz) != 0) {
        goto fail_partial;
    }
    int64_t edge_total = s.total;

    /* Flip readers to the new generation. */
    cbm_neo4j_batch_t b;
    if (cbm_neo4j_batch_init(&b) != 0) {
        goto fail_partial;
    }
    yyjson_mut_val *params =
        cbm_neo4j_batch_add(&b, "MATCH (p:CbmProject {name: $db}) SET p.gen = $gen, "
                                "p.generation = $generation, p.root_path = $root, "
                                "p.indexed_at = $indexed_at, p.node_count = $nodes, "
                                "p.edge_count = $edges, p.sync_format = $format");
    add_project_params(b.doc, params, project, generation, proj, node_total, edge_total);
    yyjson_mut_obj_add_int(b.doc, params, "gen", new_gen);
    int flip_rc = cbm_neo4j_batch_run(c, &b, NULL);
    cbm_neo4j_batch_free(&b);
    if (flip_rc != 0) {
        sync_err(err, err_sz, "publish Neo4j generation", cbm_neo4j_error(c));
        goto fail_partial;
    }

    /* Old generation is now unreachable for readers; reclaim it. A failure
     * here leaves garbage that the next sync removes -- which must then be a
     * full one, so no state file is published. */
    if (delete_loop(c, project, "n.gen <> $gen", new_gen) != 0) {
        cbm_log_warn("neo4j.sync", "phase", "delete_old_generation", "err", cbm_neo4j_error(c));
        state_writer_discard(&w);
        state_remove(db_path);
    } else {
        n4_state_meta_t m;
        memset(&m, 0, sizeof(m));
        (void)snprintf(m.generation, sizeof(m.generation), "%s", generation);
        m.gen = new_gen;
        m.nodes = node_total;
        m.edges = edge_total;
        (void)snprintf(m.uri, sizeof(m.uri), "%s", cbm_neo4j_uri(c));
        state_writer_publish(&w, &m);
    }
    g_last_stats.mode = CBM_NEO4J_SYNC_FULL;
    g_last_stats.nodes_added = node_total;
    g_last_stats.edges_added = edge_total;
    g_last_stats.nodes_total = node_total;
    g_last_stats.edges_total = edge_total;
    (void)snprintf(g_last_stats.detail, sizeof(g_last_stats.detail), "%s", reason ? reason : "");
    return 0;

fail_partial:
    state_writer_discard(&w);
    /* Drop what was written for the new generation; readers never saw it. */
    (void)delete_loop(c, project, "n.gen = $gen", new_gen);
    return -1;
}

/* ── Delta upload ───────────────────────────────────────────────── */

/* Operation kinds, in the order their statements run inside the delta's one
 * transaction: edges out first, then nodes, then the new rows. */
typedef enum { OP_EDEL = 0, OP_NDEL, OP_NUPD, OP_NADD, OP_EADD, OP_COUNT } n4_op_t;

typedef struct {
    n4_op_t op;
    char group[256]; /* label / edge type the statement targets */
    char old[256];   /* OP_NUPD: the label the node had */
    yyjson_mut_val *rows;
} n4_bucket_t;

typedef struct {
    yyjson_mut_doc *doc;
    n4_bucket_t *v;
    int n;
    int cap;
    int64_t rows;  /* node / edge operations queued */
    int64_t bytes; /* their approximate payload */
} n4_delta_t;

static yyjson_mut_val *delta_rows(n4_delta_t *d, n4_op_t op, const char *group, const char *old) {
    for (int i = 0; i < d->n; i++) {
        n4_bucket_t *b = &d->v[i];
        if (b->op == op && strcmp(b->group, group) == 0 && strcmp(b->old, old) == 0) {
            return b->rows;
        }
    }
    if (d->n == d->cap) {
        int cap = d->cap ? d->cap * 2 : 16;
        n4_bucket_t *nv = realloc(d->v, (size_t)cap * sizeof(*nv));
        if (!nv) {
            return NULL;
        }
        d->v = nv;
        d->cap = cap;
    }
    n4_bucket_t *b = &d->v[d->n++];
    memset(b, 0, sizeof(*b));
    b->op = op;
    (void)snprintf(b->group, sizeof(b->group), "%s", group);
    (void)snprintf(b->old, sizeof(b->old), "%s", old);
    b->rows = yyjson_mut_arr(d->doc);
    return b->rows;
}

/* Cypher of one bucket's statement. */
static void delta_cypher(const n4_bucket_t *b, char *out, size_t sz) {
    switch (b->op) {
    case OP_EDEL:
        (void)snprintf(out, sz,
                       "UNWIND $rows AS r "
                       "MATCH (a:CbmNode {db: $db, gen: $gen, id: r.s})-[e:`%s`]->"
                       "(b:CbmNode {db: $db, gen: $gen, id: r.t}) WHERE e.id = r.id DELETE e",
                       edge_rel_type(b->group));
        break;
    case OP_NDEL:
        (void)snprintf(out, sz,
                       "UNWIND $rows AS i MATCH (n:CbmNode {db: $db, gen: $gen, id: i}) "
                       "DETACH DELETE n");
        break;
    case OP_NUPD: {
        /* SET n = map replaces every property (labels are not properties);
         * a changed label is moved like the full upload would have set it. */
        bool moved = strcmp(b->old, b->group) != 0;
        char rm[300] = "";
        char add[300] = "";
        if (moved && cbm_neo4j_ident_ok(b->old)) {
            (void)snprintf(rm, sizeof(rm), " REMOVE n:`%s`", b->old);
        }
        if (moved && cbm_neo4j_ident_ok(b->group)) {
            (void)snprintf(add, sizeof(add), " SET n:`%s`", b->group);
        }
        (void)snprintf(out, sz,
                       "UNWIND $rows AS r MATCH (n:CbmNode {db: $db, gen: $gen, id: r.id}) "
                       "SET n = r.p%s%s",
                       rm, add);
        break;
    }
    case OP_NADD:
        node_create_cypher(out, sz, b->group);
        break;
    case OP_EADD:
        edge_create_cypher(out, sz, b->group);
        break;
    default:
        out[0] = '\0';
        break;
    }
}

/* Merge-join the project's nodes against the state file by id. Fills the
 * delta buckets and temp.d_nodes (what to write back to the state after
 * Neo4j commits). 0, 1 when the delta grew past max_rows / the byte limit,
 * -1 on a SQLite error. */
static int delta_scan_nodes(sqlite3 *pdb, sqlite3 *sdb, n4_delta_t *d, const char *project,
                            int64_t gen, int64_t max_rows, int64_t *total) {
    sqlite3_stmt *cur = NULL;
    sqlite3_stmt *old = NULL;
    sqlite3_stmt *rec = NULL;
    int rc = -1;
    if (sqlite3_prepare_v2(pdb, N4_NODE_SELECT " ORDER BY id", -1, &cur, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(sdb, "SELECT id, h, label FROM main.nodes ORDER BY id", -1, &old,
                           NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(sdb, "INSERT INTO temp.d_nodes(id, h, label, op) VALUES(?1,?2,?3,?4)",
                           -1, &rec, NULL) != SQLITE_OK) {
        goto done;
    }
    bool have_cur = sqlite3_step(cur) == SQLITE_ROW;
    bool have_old = sqlite3_step(old) == SQLITE_ROW;
    *total = 0;
    while (have_cur || have_old) {
        int64_t cid = have_cur ? sqlite3_column_int64(cur, 0) : INT64_MAX;
        int64_t oid = have_old ? sqlite3_column_int64(old, 0) : INT64_MAX;
        if (have_cur && cid <= oid) {
            (*total)++;
            int64_t h = node_hash(cur);
            bool added = cid < oid;
            if (added || h != sqlite3_column_int64(old, 1)) {
                const char *label = col_text(cur, 2);
                yyjson_mut_val *row = yyjson_mut_obj(d->doc);
                if (added) {
                    yyjson_mut_val *rows = delta_rows(d, OP_NADD, label, "");
                    if (!rows) {
                        goto done;
                    }
                    node_row(d->doc, row, cur, project, gen);
                    yyjson_mut_arr_append(rows, row);
                    g_last_stats.nodes_added++;
                } else {
                    yyjson_mut_val *rows = delta_rows(d, OP_NUPD, label, col_text(old, 2));
                    if (!rows) {
                        goto done;
                    }
                    yyjson_mut_val *p = yyjson_mut_obj(d->doc);
                    node_row(d->doc, p, cur, project, gen);
                    yyjson_mut_obj_add_int(d->doc, row, "id", cid);
                    yyjson_mut_obj_add_val(d->doc, row, "p", p);
                    yyjson_mut_arr_append(rows, row);
                    g_last_stats.nodes_changed++;
                }
                (void)sqlite3_bind_int64(rec, 1, cid);
                (void)sqlite3_bind_int64(rec, 2, h);
                (void)sqlite3_bind_text(rec, 3, label, -1, SQLITE_TRANSIENT);
                (void)sqlite3_bind_int(rec, 4, 1);
                if (sqlite3_step(rec) != SQLITE_DONE) {
                    goto done;
                }
                (void)sqlite3_reset(rec);
                d->rows++;
                d->bytes += row_bytes(cur, 9);
            }
            have_cur = sqlite3_step(cur) == SQLITE_ROW;
            if (!added) {
                have_old = sqlite3_step(old) == SQLITE_ROW;
            }
        } else {
            /* oid < cid: the node is gone. Its edges were removed with it
             * (ON DELETE CASCADE) and are deleted by the edge scan. */
            yyjson_mut_val *rows = delta_rows(d, OP_NDEL, "", "");
            if (!rows) {
                goto done;
            }
            yyjson_mut_arr_add_int(d->doc, rows, oid);
            (void)sqlite3_bind_int64(rec, 1, oid);
            (void)sqlite3_bind_int64(rec, 2, 0);
            (void)sqlite3_bind_text(rec, 3, "", -1, SQLITE_STATIC);
            (void)sqlite3_bind_int(rec, 4, 0);
            if (sqlite3_step(rec) != SQLITE_DONE) {
                goto done;
            }
            (void)sqlite3_reset(rec);
            g_last_stats.nodes_removed++;
            d->rows++;
            have_old = sqlite3_step(old) == SQLITE_ROW;
        }
        if (d->rows > max_rows || d->bytes > N4_DELTA_MAX_BYTES) {
            rc = 1;
            goto done;
        }
    }
    rc = 0;
done:
    if (cur) {
        sqlite3_finalize(cur);
    }
    if (old) {
        sqlite3_finalize(old);
    }
    if (rec) {
        sqlite3_finalize(rec);
    }
    return rc;
}

/* As delta_scan_nodes, for edges. A changed edge is deleted (found by its
 * recorded endpoints and type) and created again: an edge's endpoints and
 * type cannot change in place in Neo4j. */
static int delta_scan_edges(sqlite3 *pdb, sqlite3 *sdb, n4_delta_t *d, int64_t max_rows,
                            int64_t *total) {
    sqlite3_stmt *cur = NULL;
    sqlite3_stmt *old = NULL;
    sqlite3_stmt *rec = NULL;
    int rc = -1;
    if (sqlite3_prepare_v2(pdb, N4_EDGE_SELECT " ORDER BY id", -1, &cur, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(sdb, "SELECT id, h, s, t, type FROM main.edges ORDER BY id", -1, &old,
                           NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(sdb,
                           "INSERT INTO temp.d_edges(id, h, s, t, type, op) "
                           "VALUES(?1,?2,?3,?4,?5,?6)",
                           -1, &rec, NULL) != SQLITE_OK) {
        goto done;
    }
    bool have_cur = sqlite3_step(cur) == SQLITE_ROW;
    bool have_old = sqlite3_step(old) == SQLITE_ROW;
    *total = 0;
    while (have_cur || have_old) {
        int64_t cid = have_cur ? sqlite3_column_int64(cur, 0) : INT64_MAX;
        int64_t oid = have_old ? sqlite3_column_int64(old, 0) : INT64_MAX;
        bool gone = false;
        bool add = false;
        int64_t h = 0;
        if (have_cur && cid <= oid) {
            (*total)++;
            h = edge_hash(cur);
            add = cid < oid || h != sqlite3_column_int64(old, 1);
            gone = cid == oid && add; /* changed: the recorded edge goes too */
        } else {
            gone = true;
        }
        if (gone) {
            yyjson_mut_val *rows = delta_rows(d, OP_EDEL, col_text(old, 4), "");
            if (!rows) {
                goto done;
            }
            yyjson_mut_val *row = yyjson_mut_obj(d->doc);
            yyjson_mut_obj_add_int(d->doc, row, "s", sqlite3_column_int64(old, 2));
            yyjson_mut_obj_add_int(d->doc, row, "t", sqlite3_column_int64(old, 3));
            yyjson_mut_obj_add_int(d->doc, row, "id", oid);
            yyjson_mut_arr_append(rows, row);
            d->rows++;
        }
        if (add) {
            yyjson_mut_val *rows = delta_rows(d, OP_EADD, col_text(cur, 3), "");
            if (!rows) {
                goto done;
            }
            yyjson_mut_val *row = yyjson_mut_obj(d->doc);
            edge_row(d->doc, row, cur);
            yyjson_mut_arr_append(rows, row);
            d->rows++;
            d->bytes += row_bytes(cur, 5);
        }
        if (add || gone) {
            bool upsert = add;
            (void)sqlite3_bind_int64(rec, 1, upsert ? cid : oid);
            (void)sqlite3_bind_int64(rec, 2, upsert ? h : 0);
            (void)sqlite3_bind_int64(rec, 3, upsert ? sqlite3_column_int64(cur, 1) : 0);
            (void)sqlite3_bind_int64(rec, 4, upsert ? sqlite3_column_int64(cur, 2) : 0);
            (void)sqlite3_bind_text(rec, 5, upsert ? col_text(cur, 3) : "", -1, SQLITE_TRANSIENT);
            (void)sqlite3_bind_int(rec, 6, upsert ? 1 : 0);
            if (sqlite3_step(rec) != SQLITE_DONE) {
                goto done;
            }
            (void)sqlite3_reset(rec);
            if (add && gone) {
                g_last_stats.edges_changed++;
            } else if (add) {
                g_last_stats.edges_added++;
            } else {
                g_last_stats.edges_removed++;
            }
        }
        bool step_cur = have_cur && cid <= oid;
        bool step_old = have_old && oid <= cid;
        if (step_cur) {
            have_cur = sqlite3_step(cur) == SQLITE_ROW;
        }
        if (step_old) {
            have_old = sqlite3_step(old) == SQLITE_ROW;
        }
        if (d->rows > max_rows || d->bytes > N4_DELTA_MAX_BYTES) {
            rc = 1;
            goto done;
        }
    }
    rc = 0;
done:
    if (cur) {
        sqlite3_finalize(cur);
    }
    if (old) {
        sqlite3_finalize(old);
    }
    if (rec) {
        sqlite3_finalize(rec);
    }
    return rc;
}

/* Write the applied delta back into the state file. A failure removes the
 * file (the next sync is full): Neo4j is already correct. */
static void delta_record(sqlite3 *sdb, const char *db_path, const n4_state_meta_t *m) {
    static const char apply[] =
        "BEGIN;"
        "DELETE FROM main.nodes WHERE id IN (SELECT id FROM temp.d_nodes WHERE op = 0);"
        "INSERT OR REPLACE INTO main.nodes(id, h, label)"
        " SELECT id, h, label FROM temp.d_nodes WHERE op = 1;"
        "DELETE FROM main.edges WHERE id IN (SELECT id FROM temp.d_edges WHERE op = 0);"
        "INSERT OR REPLACE INTO main.edges(id, h, s, t, type)"
        " SELECT id, h, s, t, type FROM temp.d_edges WHERE op = 1;";
    bool ok = sqlite3_exec(sdb, apply, NULL, NULL, NULL) == SQLITE_OK &&
              state_meta_write(sdb, m) == 0 &&
              sqlite3_exec(sdb, "COMMIT", NULL, NULL, NULL) == SQLITE_OK;
    if (!ok) {
        cbm_log_warn("neo4j.sync.state", "phase", "record_delta", "err", sqlite3_errmsg(sdb));
        (void)sqlite3_exec(sdb, "ROLLBACK", NULL, NULL, NULL);
        sqlite3_close(sdb);
        state_remove(db_path);
        return;
    }
    sqlite3_close(sdb);
}

static int64_t delta_max_rows(void) {
    char buf[32];
    const char *v = cbm_safe_getenv(N4_DELTA_ENV, buf, sizeof(buf), NULL);
    if (!v || !v[0]) {
        return N4_DELTA_MAX_ROWS_DEFAULT;
    }
    char *end = NULL;
    long long n = strtoll(v, &end, 10);
    return (end && *end == '\0' && n >= 0) ? (int64_t)n : N4_DELTA_MAX_ROWS_DEFAULT;
}

#ifdef CBM_ENABLE_TEST_SEAMS
static CBM_TLS void (*g_delta_hook)(void *ctx);
static CBM_TLS void *g_delta_hook_ctx;

void cbm_neo4j_test_set_delta_hook(void (*fn)(void *ctx), void *ctx) {
    g_delta_hook = fn;
    g_delta_hook_ctx = ctx;
}
#endif

/* Bring Neo4j to the project's current rows by sending only what changed.
 * 0 = applied. 1 = not applicable (reason says why; nothing was written).
 * -1 = Neo4j refused the delta transaction, which rolled back (reason). */
static int try_delta(cbm_neo4j_t *c, sqlite3 *pdb, const char *db_path, const char *project,
                     const char *generation, const cbm_project_t *proj, char *reason,
                     size_t reason_sz) {
    reason[0] = '\0';
    int64_t max_rows = delta_max_rows();
    if (max_rows <= 0) {
        (void)snprintf(reason, reason_sz, "delta sync disabled (%s=0)", N4_DELTA_ENV);
        return 1;
    }
    char spath[N4_PATH_MAX];
    if (cbm_neo4j_state_path(db_path, spath, sizeof(spath)) != 0 || !cbm_file_exists(spath)) {
        (void)snprintf(reason, reason_sz, "first sync: no record of the Neo4j graph yet");
        return 1;
    }
    sqlite3 *sdb = NULL;
    n4_state_meta_t m;
    if (sqlite3_open_v2(spath, &sdb, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK ||
        state_meta_read(sdb, &m) != 0) {
        (void)snprintf(reason, reason_sz, "unreadable or outdated sync record");
        sqlite3_close(sdb);
        state_remove(db_path);
        return 1;
    }
    if (strcmp(m.uri, cbm_neo4j_uri(c)) != 0) {
        (void)snprintf(reason, reason_sz, "sync record is for another Neo4j server");
        sqlite3_close(sdb);
        return 1;
    }
    /* Neo4j must still serve exactly the graph the record describes. */
    cbm_neo4j_result_t r;
    if (run_simple(c,
                   "MATCH (p:CbmProject {name: $db}) RETURN p.gen, p.generation, "
                   "coalesce(p.sync_format, 1), p.node_count, p.edge_count",
                   project, 0, &r) != 0) {
        (void)snprintf(reason, reason_sz, "could not read the Neo4j project state");
        sqlite3_close(sdb);
        return 1;
    }
    yyjson_val *row = yyjson_arr_get_first(cbm_neo4j_result_data(&r, 0));
    const char *rgen = yyjson_get_str(cbm_neo4j_row_col(row, 1));
    bool matches = row && cbm_neo4j_val_int(cbm_neo4j_row_col(row, 0), -1) == m.gen && rgen &&
                   strcmp(rgen, m.generation) == 0 &&
                   cbm_neo4j_val_int(cbm_neo4j_row_col(row, 2), 1) == N4_SYNC_FORMAT &&
                   cbm_neo4j_val_int(cbm_neo4j_row_col(row, 3), -1) == m.nodes &&
                   cbm_neo4j_val_int(cbm_neo4j_row_col(row, 4), -1) == m.edges;
    cbm_neo4j_result_free(&r);
    if (!matches) {
        (void)snprintf(reason, reason_sz, "Neo4j graph differs from the last sync record");
        sqlite3_close(sdb);
        return 1;
    }

    if (sqlite3_exec(sdb,
                     "CREATE TEMP TABLE d_nodes(id INTEGER PRIMARY KEY, h INTEGER, label TEXT,"
                     " op INTEGER);"
                     "CREATE TEMP TABLE d_edges(id INTEGER PRIMARY KEY, h INTEGER, s INTEGER,"
                     " t INTEGER, type TEXT, op INTEGER);",
                     NULL, NULL, NULL) != SQLITE_OK) {
        (void)snprintf(reason, reason_sz, "sync record: %s", sqlite3_errmsg(sdb));
        sqlite3_close(sdb);
        return 1;
    }

    cbm_neo4j_batch_t b;
    if (cbm_neo4j_batch_init(&b) != 0) {
        (void)snprintf(reason, reason_sz, "out of memory");
        sqlite3_close(sdb);
        return 1;
    }
    n4_delta_t d;
    memset(&d, 0, sizeof(d));
    d.doc = b.doc;
    int64_t node_total = 0;
    int64_t edge_total = 0;
    int scan = delta_scan_nodes(pdb, sdb, &d, project, m.gen, max_rows, &node_total);
    if (scan == 0) {
        scan = delta_scan_edges(pdb, sdb, &d, max_rows, &edge_total);
    }
    if (scan != 0) {
        if (scan == 1) {
            (void)snprintf(reason, reason_sz,
                           "change too large for one transaction (over %" PRId64
                           " rows or %lld MB; %s)",
                           max_rows, (long long)(N4_DELTA_MAX_BYTES / (1024 * 1024)), N4_DELTA_ENV);
        } else {
            (void)snprintf(reason, reason_sz, "compare with sync record: %s", sqlite3_errmsg(sdb));
        }
        free(d.v);
        cbm_neo4j_batch_free(&b);
        sqlite3_close(sdb);
        return 1;
    }

    /* One request = one transaction. The guard re-checks, under the write
     * lock it takes on CbmProject, that nobody moved the graph since the
     * check above: otherwise 1 / 0 fails the transaction and nothing of it
     * is applied. */
    yyjson_mut_val *gp = cbm_neo4j_batch_add(
        &b, "OPTIONAL MATCH (p:CbmProject {name: $db}) "
            "WITH p, CASE WHEN p IS NOT NULL AND p.gen = $gen AND p.generation = $prev "
            "AND p.node_count = $nodes AND p.edge_count = $edges THEN 1 ELSE 1 / $zero END AS ok "
            "SET p.generation = p.generation RETURN ok");
    yyjson_mut_obj_add_strcpy(b.doc, gp, "db", project);
    yyjson_mut_obj_add_int(b.doc, gp, "gen", m.gen);
    yyjson_mut_obj_add_strcpy(b.doc, gp, "prev", m.generation);
    yyjson_mut_obj_add_int(b.doc, gp, "nodes", m.nodes);
    yyjson_mut_obj_add_int(b.doc, gp, "edges", m.edges);
    yyjson_mut_obj_add_int(b.doc, gp, "zero", 0);
    for (int op = 0; op < OP_COUNT; op++) {
        for (int i = 0; i < d.n; i++) {
            n4_bucket_t *bk = &d.v[i];
            if ((int)bk->op != op || yyjson_mut_arr_size(bk->rows) == 0) {
                continue;
            }
            char cypher[1024];
            delta_cypher(bk, cypher, sizeof(cypher));
            yyjson_mut_val *params = cbm_neo4j_batch_add(&b, cypher);
            yyjson_mut_obj_add_strcpy(b.doc, params, "db", project);
            yyjson_mut_obj_add_int(b.doc, params, "gen", m.gen);
            yyjson_mut_obj_add_val(b.doc, params, "rows", bk->rows);
        }
    }
    yyjson_mut_val *pp =
        cbm_neo4j_batch_add(&b, "MATCH (p:CbmProject {name: $db}) SET p.generation = $generation, "
                                "p.root_path = $root, p.indexed_at = $indexed_at, "
                                "p.node_count = $nodes, p.edge_count = $edges, "
                                "p.sync_format = $format");
    add_project_params(b.doc, pp, project, generation, proj, node_total, edge_total);
#ifdef CBM_ENABLE_TEST_SEAMS
    if (g_delta_hook) {
        g_delta_hook(g_delta_hook_ctx); /* another writer, between check and commit */
    }
#endif
    int run_rc = cbm_neo4j_batch_run(c, &b, NULL);
    free(d.v);
    cbm_neo4j_batch_free(&b);
    if (run_rc != 0) {
        (void)snprintf(reason, reason_sz, "Neo4j rejected the delta: %s", cbm_neo4j_error(c));
        sqlite3_close(sdb);
        return -1;
    }

    n4_state_meta_t next = m;
    (void)snprintf(next.generation, sizeof(next.generation), "%s", generation);
    next.nodes = node_total;
    next.edges = edge_total;
    delta_record(sdb, db_path, &next);
    g_last_stats.mode = CBM_NEO4J_SYNC_DELTA;
    g_last_stats.nodes_total = node_total;
    g_last_stats.edges_total = edge_total;
    return 0;
}

/* ── Sync entry points ──────────────────────────────────────────── */

int cbm_neo4j_sync_db(cbm_neo4j_t *c, const char *db_path, const char *project,
                      char *generation_out, size_t generation_sz, char *err, size_t err_sz) {
    stats_reset();
    if (cbm_neo4j_ensure_schema(c) != 0) {
        sync_err(err, err_sz, "create Neo4j indexes", cbm_neo4j_error(c));
        return -1;
    }
    cbm_store_t *store = cbm_store_open_path_query(db_path);
    if (!store) {
        sync_err(err, err_sz, "open project database", db_path);
        return -1;
    }
    char generation[N4_GEN_MAX] = "";
    (void)cbm_store_generation(store, generation, sizeof(generation));
    cbm_project_t proj = {0};
    bool have_proj = cbm_store_get_project(store, project, &proj) == CBM_STORE_OK;
    sqlite3 *db = cbm_store_get_db(store);

    char reason[256] = "";
    int rc = try_delta(c, db, db_path, project, generation, have_proj ? &proj : NULL, reason,
                       sizeof(reason));
    if (rc != 0) {
        if (rc < 0) {
            cbm_log_warn("neo4j.sync", "project", project, "phase", "delta", "err", reason);
        }
        /* Counters of an abandoned delta scan are not what happened. */
        stats_reset();
        rc = full_sync(c, db, db_path, project, generation, have_proj ? &proj : NULL, reason, err,
                       err_sz);
    }
    if (rc == 0) {
        char summary[512];
        (void)cbm_neo4j_sync_stats_format(&g_last_stats, summary, sizeof(summary));
        cbm_log_info("neo4j.sync", "project", project, "uri", cbm_neo4j_uri(c), "status", "ok",
                     "result", summary);
        if (generation_out && generation_sz) {
            (void)snprintf(generation_out, generation_sz, "%s", generation);
        }
    }
    if (have_proj) {
        cbm_project_free_fields(&proj);
    }
    cbm_store_close(store);
    return rc;
}

/* The SQLite generation Neo4j currently publishes for project ("" when the
 * project is absent, or was synced in an older layout and must be re-sent).
 * 0, or -1 on a transport/Cypher failure. */
static int remote_generation(cbm_neo4j_t *c, const char *project, char *out, size_t out_sz) {
    out[0] = '\0';
    cbm_neo4j_result_t r;
    if (run_simple(
            c, "MATCH (p:CbmProject {name: $db}) RETURN p.generation, coalesce(p.sync_format, 1)",
            project, 0, &r) != 0) {
        return -1;
    }
    yyjson_val *row = yyjson_arr_get_first(cbm_neo4j_result_data(&r, 0));
    yyjson_val *v = cbm_neo4j_row_col(row, 0);
    bool current_format = cbm_neo4j_val_int(cbm_neo4j_row_col(row, 1), 1) >= N4_SYNC_FORMAT;
    (void)snprintf(out, out_sz, "%s", current_format && yyjson_is_str(v) ? yyjson_get_str(v) : "");
    cbm_neo4j_result_free(&r);
    return 0;
}

int cbm_neo4j_drop_project(cbm_neo4j_t *c, const char *project, char *err, size_t err_sz) {
    if (delete_loop(c, project, "true", 0) != 0 ||
        run_simple(c, "MATCH (p:CbmProject {name: $db}) DELETE p", project, 0, NULL) != 0) {
        sync_err(err, err_sz, "delete project from Neo4j", cbm_neo4j_error(c));
        return -1;
    }
    return 0;
}

int cbm_neo4j_after_publish(const char *db_path, const char *project,
                            const cbm_neo4j_config_t *explicit_cfg, bool disable, char *err,
                            size_t err_sz) {
    if (err && err_sz) {
        err[0] = '\0';
    }
    stats_reset();
    if (disable) {
        (void)cbm_neo4j_sidecar_remove(db_path);
        return 0;
    }
    cbm_neo4j_config_t cfg;
    char synced[N4_GEN_MAX] = "";
    bool explicit_req = explicit_cfg != NULL;
    /* True when a marker for this same target already exists: a failed sync
     * is then recorded in it, so it stays visible after this run. */
    bool marked_target = !explicit_req;
    if (explicit_req) {
        cfg = *explicit_cfg;
        /* Same target as last time: keep the recorded generation so an
         * unchanged re-index skips the upload. */
        cbm_neo4j_config_t prev;
        char perr[256];
        if (cbm_neo4j_sidecar_read(db_path, &prev, synced, sizeof(synced), perr, sizeof(perr)) !=
                0 ||
            strcmp(prev.uri, cfg.uri) != 0 || strcmp(prev.database, cfg.database) != 0) {
            synced[0] = '\0';
        } else {
            marked_target = true;
        }
        cbm_neo4j_config_wipe(&prev);
    } else {
        int rc = cbm_neo4j_sidecar_read(db_path, &cfg, synced, sizeof(synced), err, err_sz);
        if (rc == 1) {
            return 0; /* plain SQLite project */
        }
        if (rc != 0) {
            cbm_log_error("neo4j.sync", "project", project, "err", err);
            return -1;
        }
    }

    /* Skip the upload when Neo4j already holds this exact generation. */
    char current[N4_GEN_MAX] = "";
    cbm_store_t *store = cbm_store_open_path_query(db_path);
    if (store) {
        (void)cbm_store_generation(store, current, sizeof(current));
        cbm_store_close(store);
    }

    int result = 0;
    cbm_neo4j_t *c = cbm_neo4j_connect(&cfg, err, err_sz);
    if (!c) {
        result = -1;
    } else {
        /* The marker only records what was last sent. Skip the upload only
         * when Neo4j itself still publishes this exact generation: the graph
         * may have been deleted or overwritten there since (a cleared
         * database must be refilled by the next run, not reported current).
         * The lookup also proves reachability and credentials. */
        bool up_to_date = false;
        if (current[0] && strcmp(current, synced) == 0) {
            char remote[N4_GEN_MAX] = "";
            if (remote_generation(c, project, remote, sizeof(remote)) != 0) {
                sync_err(err, err_sz, "connect to Neo4j", cbm_neo4j_error(c));
                result = -1;
            } else {
                up_to_date = strcmp(remote, current) == 0;
            }
        }
        if (result == 0 && up_to_date) {
            g_last_stats.mode = CBM_NEO4J_SYNC_SKIPPED;
        }
        if (result == 0 && !up_to_date) {
            char done_gen[N4_GEN_MAX] = "";
            if (cbm_neo4j_sync_db(c, db_path, project, done_gen, sizeof(done_gen), err, err_sz) !=
                0) {
                result = -1;
            } else {
                (void)snprintf(synced, sizeof(synced), "%s", done_gen);
            }
        }
    }
    cbm_neo4j_close(c);

    if (result == 0) {
        if (explicit_req) {
            cbm_neo4j_config_remember(&cfg);
        }
        char summary[512] = "";
        (void)cbm_neo4j_sync_stats_format(&g_last_stats, summary, sizeof(summary));
        if (sidecar_write_ex(db_path, &cfg, synced, NULL, summary) != 0) {
            sync_err(err, err_sz, "write graph backend marker", db_path);
            result = -1;
        }
    } else {
        cbm_log_error("neo4j.sync", "project", project, "err", err ? err : "");
        /* Keep the last good synced_generation and record why this sync
         * failed; the next successful sync rewrites the marker without it. */
        if (marked_target) {
            (void)cbm_neo4j_sidecar_write_status(db_path, &cfg, synced,
                                                 err && err[0] ? err : "Neo4j sync failed");
        }
    }
    cbm_neo4j_config_wipe(&cfg);
    /* A Neo4j-backed project is indexed into Neo4j or not at all: an inherited
     * (watcher / plain re-index) sync failure fails the run like an explicit
     * one. Reads stay refused until a sync succeeds (no local fallback). */
    return result;
}
