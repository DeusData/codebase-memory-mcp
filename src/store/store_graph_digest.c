/* Canonical stored-content digest. This module establishes no provenance. */
#include "store/store_graph_digest.h"

#include "foundation/sha256.h"

#include <limits.h>
#include <sqlite3.h>
#include <string.h>

enum {
    GD_NULL = 0,
    GD_INTEGER = 1,
    GD_TEXT = 2,
    GD_BLOB = 3,
    GD_ALLOW_NULL = 1 << GD_NULL,
    GD_ALLOW_INTEGER = 1 << GD_INTEGER,
    GD_ALLOW_TEXT = 1 << GD_TEXT,
    GD_ALLOW_BLOB = 1 << GD_BLOB,
    GD_TABLE_COUNT = 10,
    GD_MAX_COLUMNS = 10,
    GD_COVERAGE_META = 7,
    GD_STORE_META = 8,
    GD_UNRESOLVED_CANDIDATES = 9,
    GD_SCHEMA_DECLARED_TYPE = 2,
    GD_SCHEMA_PRIMARY_KEY = 5,
    GD_SCHEMA_HIDDEN = 6,
    GD_CHUNK_BYTES = 65536
};

typedef struct {
    const char *name;
    unsigned char types;
    unsigned char kind;
    int primary_key;
} gd_column_t;

typedef struct {
    const char *name;
    const gd_column_t *columns;
    int column_count;
    const char *column_query;
    const char *row_query;
} gd_table_t;

#define GD_COLUMNS(a) ((int)(sizeof(a) / sizeof((a)[0])))
#define GD_I GD_ALLOW_INTEGER
#define GD_T GD_ALLOW_TEXT
#define GD_N GD_ALLOW_NULL
#define GD_B GD_ALLOW_BLOB

static const gd_column_t gd_projects[] = {
    {"name", GD_T, 0, 1}, {"indexed_at", GD_T, 0, 0}, {"root_path", GD_T, 0, 0}};
static const gd_column_t gd_file_hashes[] = {{"project", GD_T, 0, 1},
                                             {"rel_path", GD_T, 0, 2},
                                             {"sha256", GD_T, 0, 0},
                                             {"mtime_ns", GD_I, 0, 0},
                                             {"size", GD_I, 0, 0}};
static const gd_column_t gd_nodes[] = {{"id", GD_I, 0, 1},
                                       {"project", GD_T, 0, 0},
                                       {"label", GD_T, 0, 0},
                                       {"name", GD_T, 0, 0},
                                       {"qualified_name", GD_T, 0, 0},
                                       {"file_path", GD_T | GD_N, 0, 0},
                                       {"start_line", GD_I | GD_N, 0, 0},
                                       {"end_line", GD_I | GD_N, 0, 0},
                                       {"properties", GD_T | GD_N, 0, 0}};
static const gd_column_t gd_edges[] = {{"id", GD_I, 0, 1},
                                       {"project", GD_T, 0, 0},
                                       {"source_id", GD_I, 0, 0},
                                       {"target_id", GD_I, 0, 0},
                                       {"type", GD_T, 0, 0},
                                       {"properties", GD_T | GD_N, 0, 0},
                                       {"url_path_gen", GD_T | GD_N, 2, 0},
                                       {"local_name_gen", GD_T | GD_N, 2, 0}};
static const gd_column_t gd_project_summaries[] = {{"project", GD_T, 0, 1},
                                                   {"summary", GD_T, 0, 0},
                                                   {"source_hash", GD_T, 0, 0},
                                                   {"created_at", GD_T, 0, 0},
                                                   {"updated_at", GD_T, 0, 0}};
static const gd_column_t gd_lsp_surface[] = {
    {"project", GD_T, 0, 1},   {"rel_path", GD_T, 0, 2},         {"surface_sha", GD_T, 0, 0},
    {"defs_json", GD_T, 0, 0}, {"ref_bloom", GD_B | GD_N, 0, 0}, {"config_ctx", GD_T, 0, 0}};
static const gd_column_t gd_index_coverage[] = {{"project", GD_T, 0, 1},
                                                {"rel_path", GD_T, 0, 2},
                                                {"kind", GD_T, 0, 3},
                                                {"detail", GD_T | GD_N, 0, 0}};
static const gd_column_t gd_index_coverage_meta[] = {{"project", GD_T, 0, 1},
                                                     {"generation", GD_T, 0, 0},
                                                     {"index_mode", GD_T, 0, 0},
                                                     {"recorded_at", GD_T, 0, 0},
                                                     {"recording_status", GD_T, 0, 0},
                                                     {"ignored_files_stored", GD_I, 0, 0},
                                                     {"ignored_files_total", GD_I, 0, 0},
                                                     {"coverage_version", GD_I, 0, 0},
                                                     {"hash_records_complete", GD_I, 0, 0},
                                                     {"unresolved_calls_complete", GD_I, 0, 0}};
static const gd_column_t gd_store_meta[] = {{"k", GD_T, 0, 1}, {"v", GD_T, 0, 0}};

static const gd_column_t gd_unresolved_candidates[] = {{"project", GD_T, 0, 1},
                                                       {"candidate", GD_T, 0, 2},
                                                       {"rel_path", GD_T, 0, 3},
                                                       {"kind", GD_T, 0, 0}};

/* These descriptors and SQL strings are immutable; all other state is local. */
static const gd_table_t gd_tables[GD_TABLE_COUNT] = {
    {"projects", gd_projects, GD_COLUMNS(gd_projects), "PRAGMA main.table_xinfo('projects');",
     "SELECT name,indexed_at,root_path FROM main.projects ORDER BY CAST(name AS BLOB) ASC;"},
    {"file_hashes", gd_file_hashes, GD_COLUMNS(gd_file_hashes),
     "PRAGMA main.table_xinfo('file_hashes');",
     "SELECT project,rel_path,sha256,mtime_ns,size FROM main.file_hashes "
     "ORDER BY CAST(project AS BLOB) ASC,CAST(rel_path AS BLOB) ASC;"},
    {"nodes", gd_nodes, GD_COLUMNS(gd_nodes), "PRAGMA main.table_xinfo('nodes');",
     "SELECT id,project,label,name,qualified_name,file_path,start_line,end_line,properties "
     "FROM main.nodes ORDER BY id ASC;"},
    {"edges", gd_edges, GD_COLUMNS(gd_edges), "PRAGMA main.table_xinfo('edges');",
     "SELECT id,project,source_id,target_id,type,properties,url_path_gen,local_name_gen "
     "FROM main.edges ORDER BY id ASC;"},
    {"project_summaries", gd_project_summaries, GD_COLUMNS(gd_project_summaries),
     "PRAGMA main.table_xinfo('project_summaries');",
     "SELECT project,summary,source_hash,created_at,updated_at FROM main.project_summaries "
     "ORDER BY CAST(project AS BLOB) ASC;"},
    {"lsp_surface", gd_lsp_surface, GD_COLUMNS(gd_lsp_surface),
     "PRAGMA main.table_xinfo('lsp_surface');",
     "SELECT project,rel_path,surface_sha,defs_json,ref_bloom,config_ctx FROM main.lsp_surface "
     "ORDER BY CAST(project AS BLOB) ASC,CAST(rel_path AS BLOB) ASC;"},
    {"index_coverage", gd_index_coverage, GD_COLUMNS(gd_index_coverage),
     "PRAGMA main.table_xinfo('index_coverage');",
     "SELECT project,rel_path,kind,detail FROM main.index_coverage "
     "ORDER BY CAST(project AS BLOB) ASC,CAST(rel_path AS BLOB) ASC,CAST(kind AS BLOB) ASC;"},
    {"index_coverage_meta", gd_index_coverage_meta, GD_COLUMNS(gd_index_coverage_meta) - 1,
     "PRAGMA main.table_xinfo('index_coverage_meta');",
     "SELECT project,generation,index_mode,recorded_at,recording_status,ignored_files_stored,"
     "ignored_files_total,coverage_version,hash_records_complete FROM main.index_coverage_meta "
     "ORDER BY CAST(project AS BLOB) ASC;"},
    {"store_meta", gd_store_meta, GD_COLUMNS(gd_store_meta),
     "PRAGMA main.table_xinfo('store_meta');",
     "SELECT k,v FROM main.store_meta ORDER BY CAST(k AS BLOB) ASC;"},
    {"index_unresolved_candidates", gd_unresolved_candidates, GD_COLUMNS(gd_unresolved_candidates),
     "PRAGMA main.table_xinfo('index_unresolved_candidates');",
     "SELECT project,candidate,rel_path,kind FROM main.index_unresolved_candidates "
     "ORDER BY CAST(project AS BLOB) ASC,CAST(candidate AS BLOB) ASC,"
     "CAST(rel_path AS BLOB) ASC,CAST(kind AS BLOB) ASC;"}};

/* Preserve the legacy canonical stream; newer metadata binds the independent
 * capture completeness column as well. Both schemas are checked exactly. */
static const gd_table_t gd_coverage_meta_current = {
    "index_coverage_meta", gd_index_coverage_meta, GD_COLUMNS(gd_index_coverage_meta),
    "PRAGMA main.table_xinfo('index_coverage_meta');",
    "SELECT project,generation,index_mode,recorded_at,recording_status,ignored_files_stored,"
    "ignored_files_total,coverage_version,hash_records_complete,unresolved_calls_complete "
    "FROM main.index_coverage_meta ORDER BY CAST(project AS BLOB) ASC;"};

#undef GD_B
#undef GD_N
#undef GD_T
#undef GD_I
#undef GD_COLUMNS

typedef struct {
    cbm_store_read_scope_t *scope;
    sqlite3 *db;
    sqlite3_stmt *stmt;
    cbm_sha256_ctx sha;
    cbm_store_graph_digest_limits_t limits;
    cbm_store_graph_digest_status_t status;
    uint64_t rows;
    uint64_t bytes;
    bool have_uid;
    bool have_counter;
    bool have_unresolved_completeness;
} gd_context_t;

static const gd_table_t *gd_table(const gd_context_t *g, int index) {
    return index == GD_COVERAGE_META && g->have_unresolved_completeness ? &gd_coverage_meta_current
                                                                        : &gd_tables[index];
}

typedef struct {
    const unsigned char *data;
    size_t length;
} gd_text_t;

static bool gd_fail(gd_context_t *g, cbm_store_graph_digest_status_t status) {
    if (g->status == CBM_STORE_GRAPH_DIGEST_OK) {
        g->status = status;
        if (g->scope) {
            (void)cbm_store_read_scope_fail(g->scope, status == CBM_STORE_GRAPH_DIGEST_CANCELLED
                                                          ? CBM_STORE_CANCELLED
                                                          : CBM_STORE_ERR);
        }
    }
    return false;
}

static bool gd_check(gd_context_t *g) {
    if (g->scope) {
        int status = cbm_store_read_scope_check(g->scope);
        if (status != CBM_STORE_OK) {
            return gd_fail(g, status == CBM_STORE_CANCELLED ? CBM_STORE_GRAPH_DIGEST_CANCELLED
                                                            : CBM_STORE_GRAPH_DIGEST_ERROR);
        }
    }
    return g->status == CBM_STORE_GRAPH_DIGEST_OK;
}

static void gd_sql_failure(gd_context_t *g, int rc) {
    if (rc == SQLITE_INTERRUPT) {
        (void)gd_check(g); /* Only a D5-latched cancellation earns CANCELLED. */
    }
    (void)gd_fail(g, CBM_STORE_GRAPH_DIGEST_ERROR);
}

static bool gd_prepare(gd_context_t *g, const char *sql) {
    if (!gd_check(g)) {
        return false;
    }
    if (g->stmt) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_ERROR);
    }
    int rc = sqlite3_prepare_v2(g->db, sql, -1, &g->stmt, NULL);
    if (rc != SQLITE_OK) {
        gd_sql_failure(g, rc);
    }
    return gd_check(g);
}

static int gd_step(gd_context_t *g) {
    if (!gd_check(g)) {
        return SQLITE_ERROR;
    }
    int rc = sqlite3_step(g->stmt);
    if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
        gd_sql_failure(g, rc);
    }
    return gd_check(g) ? rc : SQLITE_ERROR;
}

/* Destruction is unconditional, including after the scope has been poisoned. */
static bool gd_finalize(gd_context_t *g) {
    if (g->stmt) {
        (void)gd_check(g);
        sqlite3_stmt *stmt = g->stmt;
        g->stmt = NULL;
        int rc = sqlite3_finalize(stmt);
        if (rc != SQLITE_OK) {
            gd_sql_failure(g, rc);
        }
        (void)gd_check(g);
    }
    return g->status == CBM_STORE_GRAPH_DIGEST_OK;
}

static bool gd_bind_name(gd_context_t *g, const char *name) {
    int rc = sqlite3_bind_text(g->stmt, 1, name, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        gd_sql_failure(g, rc);
    }
    return g->status == CBM_STORE_GRAPH_DIGEST_OK;
}

/* No conversion is allowed to erase the distinction between TEXT and BLOB. */
static bool gd_read_text(gd_context_t *g, int column, gd_text_t *out) {
    if (sqlite3_column_type(g->stmt, column) != SQLITE_TEXT) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
    }
    const unsigned char *data = sqlite3_column_text(g->stmt, column);
    int length = sqlite3_column_bytes(g->stmt, column);
    if (!data || length < 0 || sqlite3_errcode(g->db) == SQLITE_NOMEM) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_ERROR);
    }
    out->data = data;
    out->length = (size_t)length;
    return true;
}

static bool gd_text_eq(gd_text_t text, const char *literal) {
    size_t length = strlen(literal); /* Only internal constant strings. */
    return text.length == length && memcmp(text.data, literal, length) == 0;
}

static bool gd_text_ascii_eq(gd_text_t text, const char *literal) {
    size_t length = strlen(literal);
    if (text.length != length) {
        return false;
    }
    for (size_t i = 0; i < length; i++) {
        unsigned char c = text.data[i];
        unsigned char d = (unsigned char)literal[i];
        if (c >= 'A' && c <= 'Z') {
            c = (unsigned char)(c + ('a' - 'A'));
        }
        if (d >= 'A' && d <= 'Z') {
            d = (unsigned char)(d + ('a' - 'A'));
        }
        if (c != d) {
            return false;
        }
    }
    return true;
}

static int gd_table_index(gd_text_t name) {
    for (int i = 0; i < GD_TABLE_COUNT; i++) {
        if (gd_text_ascii_eq(name, gd_tables[i].name)) {
            return i;
        }
    }
    return -1;
}

static bool gd_bytes(gd_context_t *g, const void *data, size_t length) {
    if (!gd_check(g)) {
        return false;
    }
    uint64_t count = (uint64_t)length;
    if ((size_t)count != length || count > g->limits.max_framed_bytes - g->bytes) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_LIMIT);
    }
    if (length && !data) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_ERROR);
    }
    const unsigned char *p = data;
    while (length) {
        size_t chunk = length > GD_CHUNK_BYTES ? GD_CHUNK_BYTES : length;
        if (!gd_check(g)) {
            return false;
        }
        cbm_sha256_update(&g->sha, p, chunk);
        g->bytes += (uint64_t)chunk;
        if (!gd_check(g)) {
            return false;
        }
        p += chunk;
        length -= chunk;
    }
    return true;
}

static bool gd_u8(gd_context_t *g, unsigned char value) {
    return gd_bytes(g, &value, 1);
}

static void gd_pack_u64(uint64_t value, unsigned char bytes[8]) {
    for (unsigned int i = 0; i < 8; i++) {
        bytes[7 - i] = (unsigned char)(value >> (8 * i));
    }
}

static bool gd_u32(gd_context_t *g, uint32_t value) {
    unsigned char bytes[4];
    for (unsigned int i = 0; i < 4; i++) {
        bytes[3 - i] = (unsigned char)(value >> (8 * i));
    }
    return gd_bytes(g, bytes, sizeof(bytes));
}

static bool gd_u64(gd_context_t *g, uint64_t value) {
    unsigned char bytes[8];
    gd_pack_u64(value, bytes);
    return gd_bytes(g, bytes, sizeof(bytes));
}

static bool gd_value(gd_context_t *g, unsigned char tag, const void *data, size_t length) {
    uint64_t count = (uint64_t)length;
    if ((size_t)count != length) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_LIMIT);
    }
    return gd_u8(g, tag) && gd_u64(g, count) && gd_bytes(g, data, length);
}

static bool gd_literal(gd_context_t *g, const char *literal) {
    return gd_value(g, GD_TEXT, literal, strlen(literal));
}

static bool gd_encoding(gd_context_t *g) {
    if (!gd_prepare(g, "PRAGMA main.encoding;")) {
        return false;
    }
    if (sqlite3_column_count(g->stmt) != 1 || gd_step(g) != SQLITE_ROW) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
    }
    gd_text_t encoding;
    if (!gd_read_text(g, 0, &encoding)) {
        return false;
    }
    if (!gd_text_eq(encoding, "UTF-8") || gd_step(g) != SQLITE_DONE) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
    }
    return gd_finalize(g);
}

static bool gd_table_kinds(gd_context_t *g, bool present[GD_TABLE_COUNT]) {
    if (!gd_prepare(g, "PRAGMA main.table_list;")) {
        return false;
    }
    /* Unsupported PRAGMAs may return DONE without an error or columns. */
    if (sqlite3_column_count(g->stmt) < 3) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
    }
    int rc;
    while ((rc = gd_step(g)) == SQLITE_ROW) {
        gd_text_t schema, name, type;
        if (!gd_read_text(g, 0, &schema) || !gd_read_text(g, 1, &name) ||
            !gd_read_text(g, 2, &type)) {
            return false;
        }
        if (!gd_text_eq(schema, "main")) {
            return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
        }
        int index = gd_table_index(name);
        if (index >= 0) {
            if (present[index] || !gd_text_eq(name, gd_tables[index].name) ||
                !gd_text_eq(type, "table")) {
                return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
            }
            present[index] = true;
        }
    }
    if (rc != SQLITE_DONE || !gd_finalize(g)) {
        return false;
    }
    for (int i = 0; i < GD_STORE_META; i++) {
        if (!present[i]) {
            return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
        }
    }
    if (!gd_prepare(g, "SELECT name FROM temp.sqlite_schema WHERE type IN ('table','view');")) {
        return false;
    }
    while ((rc = gd_step(g)) == SQLITE_ROW) {
        gd_text_t name;
        if (!gd_read_text(g, 0, &name)) {
            return false;
        }
        if (gd_table_index(name) >= 0) {
            return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
        }
    }
    return rc == SQLITE_DONE && gd_finalize(g);
}

static bool gd_column_schema(gd_context_t *g, const gd_column_t *expected) {
    if (sqlite3_column_type(g->stmt, GD_SCHEMA_PRIMARY_KEY) != SQLITE_INTEGER ||
        sqlite3_column_type(g->stmt, GD_SCHEMA_HIDDEN) != SQLITE_INTEGER ||
        sqlite3_column_int64(g->stmt, GD_SCHEMA_PRIMARY_KEY) != expected->primary_key ||
        sqlite3_column_int64(g->stmt, GD_SCHEMA_HIDDEN) != expected->kind) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
    }
    if (expected->primary_key && expected->types == GD_ALLOW_INTEGER) {
        gd_text_t declared_type;
        if (!gd_read_text(g, GD_SCHEMA_DECLARED_TYPE, &declared_type)) {
            return false;
        }
        if (!gd_text_ascii_eq(declared_type, "INTEGER")) {
            return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
        }
    }
    return true;
}

static bool gd_columns(gd_context_t *g, const gd_table_t *table) {
    bool seen[GD_MAX_COLUMNS] = {false};
    bool coverage_meta = table == &gd_tables[GD_COVERAGE_META];
    int allowed_columns =
        coverage_meta ? gd_coverage_meta_current.column_count : table->column_count;
    if (!gd_prepare(g, table->column_query)) {
        return false;
    }
    if (sqlite3_column_count(g->stmt) != 7) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
    }
    int rc;
    int count = 0;
    while ((rc = gd_step(g)) == SQLITE_ROW) {
        gd_text_t name;
        if (!gd_read_text(g, 1, &name)) {
            return false;
        }
        int column = -1;
        for (int i = 0; i < allowed_columns; i++) {
            if (gd_text_eq(name, table->columns[i].name)) {
                column = i;
                break;
            }
        }
        if (column < 0 || seen[column] || count == allowed_columns ||
            !gd_column_schema(g, &table->columns[column])) {
            return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
        }
        seen[column] = true;
        count++;
    }
    if (rc != SQLITE_DONE || count < table->column_count) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
    }
    for (int i = 0; i < table->column_count; i++) {
        if (!seen[i]) {
            return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
        }
    }
    if (coverage_meta) {
        g->have_unresolved_completeness = seen[table->column_count];
    }
    return gd_finalize(g);
}

static bool gd_project(gd_context_t *g, const unsigned char *project, size_t length) {
    if (!gd_prepare(g, "SELECT 1 FROM main.projects WHERE CAST(name AS BLOB)=?1 LIMIT 2;")) {
        return false;
    }
    int rc = sqlite3_bind_blob(g->stmt, 1, project, (int)length, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        gd_sql_failure(g, rc);
        return false;
    }
    rc = gd_step(g);
    if (rc == SQLITE_DONE) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_PROJECT_MISSING);
    }
    if (rc != SQLITE_ROW) {
        return false;
    }
    rc = gd_step(g);
    if (rc == SQLITE_ROW) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
    }
    return rc == SQLITE_DONE && gd_finalize(g);
}

static bool gd_validate_schema(gd_context_t *g, bool present[GD_TABLE_COUNT],
                               const unsigned char *project, size_t length) {
    if (!gd_encoding(g) || !gd_table_kinds(g, present)) {
        return false;
    }
    for (int i = 0; i < GD_TABLE_COUNT; i++) {
        if (present[i] && !gd_columns(g, &gd_tables[i])) {
            return false;
        }
    }
    return gd_project(g, project, length);
}

static bool gd_table_header(gd_context_t *g, int index, bool present) {
    const gd_table_t *table = gd_table(g, index);
    if (!gd_u8(g, 0x20) || !gd_u32(g, (uint32_t)index) || !gd_literal(g, table->name) ||
        !gd_u32(g, (uint32_t)table->column_count)) {
        return false;
    }
    for (int i = 0; i < table->column_count; i++) {
        const gd_column_t *column = &table->columns[i];
        if (!gd_literal(g, column->name) || !gd_u8(g, column->types) || !gd_u8(g, column->kind)) {
            return false;
        }
    }
    if (!gd_u8(g, present ? 1 : 0)) {
        return false;
    }
    if (!present) {
        return gd_value(g, GD_NULL, NULL, 0);
    }
    if (!gd_prepare(g, "SELECT name,type,sql FROM main.sqlite_schema "
                       "WHERE name=?1 COLLATE NOCASE AND type IN ('table','view');") ||
        !gd_bind_name(g, table->name)) {
        return false;
    }
    if (gd_step(g) != SQLITE_ROW) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
    }
    gd_text_t name, type, sql;
    if (!gd_read_text(g, 0, &name) || !gd_read_text(g, 1, &type) || !gd_read_text(g, 2, &sql)) {
        return false;
    }
    if (!gd_text_eq(name, table->name) || !gd_text_eq(type, "table")) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
    }
    if (!gd_value(g, GD_TEXT, sql.data, sql.length)) {
        return false;
    }
    if (gd_step(g) != SQLITE_DONE) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
    }
    return gd_finalize(g);
}

static bool gd_generation(gd_context_t *g) {
    gd_text_t key, value;
    if (!gd_read_text(g, 0, &key) || !gd_read_text(g, 1, &value)) {
        return false;
    }
    if (gd_text_eq(key, "db_uid")) {
        if (g->have_uid || value.length != 16) {
            return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
        }
        for (size_t i = 0; i < value.length; i++) {
            unsigned char c = value.data[i];
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
            }
        }
        g->have_uid = true;
    } else if (gd_text_eq(key, "mutation_gen")) {
        if (g->have_counter || value.length == 0 || value.length > 20 ||
            (value.length > 1 && value.data[0] == '0')) {
            return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
        }
        uint64_t number = 0;
        for (size_t i = 0; i < value.length; i++) {
            unsigned char c = value.data[i];
            if (c < '0' || c > '9') {
                return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
            }
            uint64_t digit = (uint64_t)(c - '0');
            if (number > (UINT64_MAX - digit) / 10) {
                return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
            }
            number = number * 10 + digit;
        }
        g->have_counter = true;
    }
    return true;
}

static bool gd_column_value(gd_context_t *g, int index, const gd_column_t *column) {
    int sql_type = sqlite3_column_type(g->stmt, index);
    unsigned char tag;
    switch (sql_type) {
    case SQLITE_NULL:
        tag = GD_NULL;
        break;
    case SQLITE_INTEGER:
        tag = GD_INTEGER;
        break;
    case SQLITE_TEXT:
        tag = GD_TEXT;
        break;
    case SQLITE_BLOB:
        tag = GD_BLOB;
        break;
    default:
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
    }
    if (!(column->types & (1u << tag))) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
    }
    if (tag == GD_NULL) {
        return gd_value(g, tag, NULL, 0);
    }
    if (tag == GD_INTEGER) {
        unsigned char bytes[8];
        gd_pack_u64((uint64_t)sqlite3_column_int64(g->stmt, index), bytes);
        return gd_value(g, tag, bytes, sizeof(bytes));
    }
    if (tag == GD_TEXT) {
        gd_text_t value;
        return gd_read_text(g, index, &value) && gd_value(g, tag, value.data, value.length);
    }
    const void *blob = sqlite3_column_blob(g->stmt, index);
    int length = sqlite3_column_bytes(g->stmt, index);
    if (length < 0 || (length && !blob) || sqlite3_errcode(g->db) == SQLITE_NOMEM) {
        return gd_fail(g, CBM_STORE_GRAPH_DIGEST_ERROR);
    }
    return gd_value(g, tag, blob, (size_t)length);
}

static bool gd_table_rows(gd_context_t *g, int index, bool present) {
    uint64_t rows = 0;
    const gd_table_t *table = gd_table(g, index);
    if (present) {
        if (!gd_prepare(g, table->row_query)) {
            return false;
        }
        if (sqlite3_column_count(g->stmt) != table->column_count) {
            return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
        }
        int rc;
        while ((rc = gd_step(g)) == SQLITE_ROW) {
            if (!gd_check(g)) {
                return false;
            }
            if (g->rows == g->limits.max_rows || g->rows == UINT64_MAX || rows == UINT64_MAX) {
                return gd_fail(g, CBM_STORE_GRAPH_DIGEST_LIMIT);
            }
            if (index == GD_STORE_META && !gd_generation(g)) {
                return false;
            }
            if (!gd_u8(g, 0x21) || !gd_u32(g, (uint32_t)table->column_count)) {
                return false;
            }
            for (int i = 0; i < table->column_count; i++) {
                if (!gd_column_value(g, i, &table->columns[i])) {
                    return false;
                }
            }
            rows++;
            g->rows++;
        }
        if (rc != SQLITE_DONE || !gd_finalize(g)) {
            return false;
        }
        if (index == GD_STORE_META && (!g->have_uid || !g->have_counter)) {
            return gd_fail(g, CBM_STORE_GRAPH_DIGEST_SCHEMA);
        }
    }
    return gd_u8(g, 0x22) && gd_u64(g, rows);
}

static bool gd_stream_tables(gd_context_t *g, const bool present[GD_TABLE_COUNT]) {
    for (int i = 0; i < GD_TABLE_COUNT; i++) {
        /* Legacy stores retain their byte-for-byte canonical stream. */
        if (i == GD_UNRESOLVED_CANDIDATES && !present[i]) {
            continue;
        }
        if (!gd_table_header(g, i, present[i]) || !gd_table_rows(g, i, present[i])) {
            return false;
        }
    }
    return true;
}

static bool gd_project_argument(gd_context_t *g, const unsigned char *project, size_t length) {
    while (length) {
        size_t chunk = length > GD_CHUNK_BYTES ? GD_CHUNK_BYTES : length;
        if (!gd_check(g)) {
            return false;
        }
        if (memchr(project, 0, chunk)) {
            return gd_fail(g, CBM_STORE_GRAPH_DIGEST_INVALID);
        }
        if (!gd_check(g)) {
            return false;
        }
        project += chunk;
        length -= chunk;
    }
    return true;
}

cbm_store_graph_digest_status_t cbm_store_graph_digest(
    cbm_store_read_scope_t *scope, const unsigned char *project, size_t project_len,
    const cbm_store_graph_digest_limits_t *limits, cbm_store_graph_digest_t *out) {
    if (out) {
        memset(out, 0, sizeof(*out));
    }
    gd_context_t g = {0};
    cbm_store_graph_digest_t result;
    memset(&result, 0, sizeof(result));
    bool complete = false;
    g.scope = scope;
    if (scope && !gd_check(&g)) {
        return g.status;
    }
    if (!scope || !project || !project_len || project_len > INT_MAX || !limits || !out ||
        !limits->max_rows || !limits->max_framed_bytes ||
        limits->max_framed_bytes > UINT64_MAX / 8) {
        (void)gd_fail(&g, CBM_STORE_GRAPH_DIGEST_INVALID);
        return g.status;
    }
    if (!gd_project_argument(&g, project, project_len)) {
        return g.status;
    }
    g.limits = *limits;
    cbm_store_t *store = cbm_store_read_scope_store(scope);
    g.db = store ? cbm_store_get_db(store) : NULL;
    if (!g.db) {
        (void)gd_fail(&g, CBM_STORE_GRAPH_DIGEST_ERROR);
        return g.status;
    }
    bool present[GD_TABLE_COUNT] = {false};
    if (!gd_validate_schema(&g, present, project, project_len)) {
        goto done;
    }
    cbm_sha256_init(&g.sha);
    static const unsigned char domain[] = "cbm-store-graph-digest";
    if (!gd_bytes(&g, domain, sizeof(domain)) || !gd_u32(&g, CBM_STORE_GRAPH_DIGEST_VERSION) ||
        !gd_u8(&g, 0x10) || !gd_value(&g, GD_TEXT, project, project_len)) {
        goto done;
    }
    if (!gd_stream_tables(&g, present)) {
        goto done;
    }
    if (!gd_u8(&g, 0x7f) || !gd_u64(&g, g.rows) || !gd_check(&g)) {
        goto done;
    }
    result.version = CBM_STORE_GRAPH_DIGEST_VERSION;
    result.rows = g.rows;
    result.framed_bytes = g.bytes;
    cbm_sha256_final(&g.sha, result.sha256);
    complete = gd_check(&g);

done:
    (void)gd_finalize(&g);
    if (!complete && g.status == CBM_STORE_GRAPH_DIGEST_OK) {
        (void)gd_fail(&g, CBM_STORE_GRAPH_DIGEST_ERROR);
    }
    if (complete && gd_check(&g)) {
        memcpy(out, &result, sizeof(*out));
    } else {
        memset(out, 0, sizeof(*out));
    }
    return g.status;
}
