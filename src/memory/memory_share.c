/*
 * memory_share.c — Deterministic Global Memory export/import and Git transport.
 *
 * The portable format contains logical memory rows plus immutable raw objects.
 * Derived FTS and code-graph projections are intentionally omitted.  Imports
 * merge rows into the live store; they never install or replace a database.
 */

#include "memory/memory_share.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/platform.h"
#include "foundation/sha256.h"
#include "foundation/str_util.h"
#include "foundation/subprocess.h"

#include <sqlite3.h>
#include <yyjson/yyjson.h>

#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#ifdef _WIN32
#include <io.h>
#include <process.h>
#define share_getpid _getpid
#define share_fsync _commit
#else
#include <fcntl.h>
#include <unistd.h>
#define share_getpid getpid
#define share_fsync fsync
#endif

enum {
    MEM_SHARE_PATH_CAP = 4096,
    MEM_SHARE_NAME_CAP = 256,
    MEM_SHARE_SQL_CAP = 32768,
    MEM_SHARE_RAW_MAX = 64 * 1024 * 1024,
    MEM_SHARE_BUNDLE_MAX = 512 * 1024 * 1024,
    MEM_SHARE_DIR_MODE = 0700,
};

typedef struct {
    const char *json_key;
    const char *table;
} memory_table_spec_t;

/* Dependency order is also import order.  All shared entities use stable text
 * identifiers.  Local coordination/materialization tables (state, outbox,
 * dirty, documents, FTS) are not portable state. */
static const memory_table_spec_t MEMORY_TABLES[] = {
    {"sources", "memory_sources"},
    {"pages", "memory_pages"},
    {"revisions", "memory_revisions"},
    {"claims", "memory_claims"},
    {"claim_revisions", "memory_claim_revisions"},
    {"decisions", "memory_decisions"},
    {"experiences", "memory_experiences"},
    {"preferences", "memory_preferences"},
    {"code_refs", "memory_code_refs"},
    {"proposals", "memory_proposals"},
    {"operations", "memory_operations"},
    {"activities", "memory_activities"},
    /* Relations come last because they may point at any graph entity,
     * including an Activity.  This also lets conflict blocking propagate to
     * every endpoint before an edge is considered. */
    {"relations", "memory_relations"},
};

static const size_t MEMORY_TABLE_COUNT = sizeof(MEMORY_TABLES) / sizeof(MEMORY_TABLES[0]);

typedef struct {
    char *name;
    int pk_order;
} memory_column_t;

typedef struct {
    memory_column_t *items;
    int count;
} memory_columns_t;

typedef enum {
    MERGE_REJECT = 0,
    MERGE_KEEP_LOCAL,
    MERGE_KEEP_REMOTE,
    MERGE_NEWEST,
} merge_policy_t;

static char *share_strdup(const char *s) {
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    char *copy = malloc(len + 1);
    if (copy) {
        memcpy(copy, s, len + 1);
    }
    return copy;
}

static char *share_json_result(bool ok, const char *error, const char *path, int64_t epoch,
                               int added, int skipped, int updated, int conflicts) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_bool(doc, root, "ok", ok);
    if (error) {
        yyjson_mut_obj_add_strcpy(doc, root, "error", error);
    }
    if (path) {
        yyjson_mut_obj_add_strcpy(doc, root, "path", path);
    }
    if (epoch >= 0) {
        yyjson_mut_obj_add_sint(doc, root, "snapshot_epoch", epoch);
    }
    if (added >= 0) {
        yyjson_mut_obj_add_int(doc, root, "added", added);
        yyjson_mut_obj_add_int(doc, root, "skipped", skipped);
        yyjson_mut_obj_add_int(doc, root, "updated", updated);
        yyjson_mut_obj_add_int(doc, root, "conflicts", conflicts);
    }
    size_t len = 0;
    char *json = yyjson_mut_write(doc, 0, &len);
    yyjson_mut_doc_free(doc);
    return json;
}

static yyjson_doc *parse_args(const char *args_json) {
    const char *json = args_json && args_json[0] ? args_json : "{}";
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc || !yyjson_is_obj(yyjson_doc_get_root(doc))) {
        if (doc) {
            yyjson_doc_free(doc);
        }
        return NULL;
    }
    return doc;
}

static const char *json_string(yyjson_val *obj, const char *key) {
    yyjson_val *value = obj ? yyjson_obj_get(obj, key) : NULL;
    return value && yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
}

static bool json_bool(yyjson_val *obj, const char *key, bool fallback) {
    yyjson_val *value = obj ? yyjson_obj_get(obj, key) : NULL;
    return value && yyjson_is_bool(value) ? yyjson_get_bool(value) : fallback;
}

static bool path_join(char *out, size_t cap, const char *a, const char *b) {
    if (!out || !a || !b) {
        return false;
    }
    int n = snprintf(out, cap, "%s/%s", a, b);
    return n >= 0 && (size_t)n < cap;
}

static bool parent_dir(const char *path, char *out, size_t cap) {
    if (!path || !out || cap == 0) {
        return false;
    }
    int n = snprintf(out, cap, "%s", path);
    if (n < 0 || (size_t)n >= cap) {
        return false;
    }
    char *slash = strrchr(out, '/');
#ifdef _WIN32
    char *backslash = strrchr(out, '\\');
    if (backslash && (!slash || backslash > slash)) {
        slash = backslash;
    }
#endif
    if (!slash) {
        return false;
    }
    *slash = '\0';
    return out[0] != '\0';
}

static bool regular_file_size(const char *path, size_t max_size, size_t *out_size) {
    struct stat st;
    if (!path || stat(path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
        (uint64_t)st.st_size > max_size) {
        return false;
    }
    if (out_size) {
        *out_size = (size_t)st.st_size;
    }
    return true;
}

static unsigned char *read_file(const char *path, size_t max_size, size_t *out_len) {
    size_t len = 0;
    if (!regular_file_size(path, max_size, &len)) {
        return NULL;
    }
    FILE *file = cbm_fopen(path, "rb");
    if (!file) {
        return NULL;
    }
    unsigned char *data = malloc(len + 1);
    if (!data) {
        fclose(file);
        return NULL;
    }
    size_t got = len ? fread(data, 1, len, file) : 0;
    int close_rc = fclose(file);
    if (got != len || close_rc != 0) {
        free(data);
        return NULL;
    }
    data[len] = 0;
    if (out_len) {
        *out_len = len;
    }
    return data;
}

static int write_atomic(const char *path, const void *data, size_t len) {
    char dir[MEM_SHARE_PATH_CAP];
    if (!parent_dir(path, dir, sizeof(dir)) || !cbm_mkdir_p(dir, MEM_SHARE_DIR_MODE)) {
        return -1;
    }
    char temp[MEM_SHARE_PATH_CAP];
    FILE *file = NULL;
    for (unsigned int attempt = 0; attempt < 16 && !file; attempt++) {
        int n = snprintf(temp, sizeof(temp), "%s.tmp.%ld.%llu.%u", path, (long)share_getpid(),
                         (unsigned long long)cbm_now_ns(), attempt);
        if (n < 0 || (size_t)n >= sizeof(temp)) {
            return -1;
        }
        /* C11 exclusive-create mode: two Agents can never share a temp file. */
        file = cbm_fopen(temp, "wbx");
        if (!file && errno != EEXIST) {
            return -1;
        }
    }
    if (!file) {
        return -1;
    }
    size_t wrote = len ? fwrite(data, 1, len, file) : 0;
    bool failed = wrote != len || fflush(file) != 0;
    if (!failed && share_fsync(cbm_fileno(file)) != 0) {
        failed = true;
    }
    if (fclose(file) != 0) {
        failed = true;
    }
    if (failed) {
        cbm_unlink(temp);
        return -1;
    }
#ifndef _WIN32
    if (chmod(temp, 0600) != 0) {
        cbm_unlink(temp);
        return -1;
    }
#endif
    if (cbm_rename_replace(temp, path) != 0) {
        cbm_unlink(temp);
        return -1;
    }
#ifndef _WIN32
    /* Persist the rename itself when the filesystem supports directory fsync. */
    int dir_fd = open(dir, O_RDONLY);
    if (dir_fd >= 0) {
        (void)fsync(dir_fd);
        (void)close(dir_fd);
    }
#endif
    return 0;
}

static char hex_digit(unsigned int value) {
    return "0123456789abcdef"[value & 0x0fU];
}

static char *hex_encode(const void *input, size_t len) {
    if (len > (SIZE_MAX - 1) / 2) {
        return NULL;
    }
    const unsigned char *data = input;
    char *hex = malloc(len * 2 + 1);
    if (!hex) {
        return NULL;
    }
    for (size_t i = 0; i < len; i++) {
        hex[i * 2] = hex_digit(data[i] >> 4U);
        hex[i * 2 + 1] = hex_digit(data[i]);
    }
    hex[len * 2] = '\0';
    return hex;
}

static int hex_value(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

static unsigned char *hex_decode(const char *hex, size_t *out_len) {
    if (!hex) {
        return NULL;
    }
    size_t len = strlen(hex);
    if ((len & 1U) != 0 || len / 2 > MEM_SHARE_RAW_MAX) {
        return NULL;
    }
    unsigned char *data = malloc(len / 2 + 1);
    if (!data) {
        return NULL;
    }
    for (size_t i = 0; i < len; i += 2) {
        int high = hex_value(hex[i]);
        int low = hex_value(hex[i + 1]);
        if (high < 0 || low < 0) {
            free(data);
            return NULL;
        }
        data[i / 2] = (unsigned char)((high << 4) | low);
    }
    data[len / 2] = 0;
    if (out_len) {
        *out_len = len / 2;
    }
    return data;
}

static bool object_relpath_valid(const char *relative, const char *content_hash) {
    static const char prefix[] = "raw/objects/";
    const size_t prefix_len = sizeof(prefix) - 1;
    const size_t minimum_len = prefix_len + 2U + 1U + CBM_SHA256_HEX_LEN;
    if (!relative || !content_hash || strlen(content_hash) != CBM_SHA256_HEX_LEN ||
        strlen(relative) < minimum_len || strncmp(relative, prefix, prefix_len) != 0) {
        return false;
    }
    const char *dir = relative + prefix_len;
    if (hex_value(dir[0]) < 0 || hex_value(dir[1]) < 0 || dir[2] != '/') {
        return false;
    }
    const char *name = dir + 3;
    for (int i = 0; i < CBM_SHA256_HEX_LEN; i++) {
        if (hex_value(name[i]) < 0 || tolower((unsigned char)name[i]) != content_hash[i]) {
            return false;
        }
    }
    if (tolower((unsigned char)dir[0]) != content_hash[0] ||
        tolower((unsigned char)dir[1]) != content_hash[1]) {
        return false;
    }
    if (name[CBM_SHA256_HEX_LEN] == '\0') {
        return true;
    }
    if (name[CBM_SHA256_HEX_LEN] != '.') {
        return false;
    }
    for (const char *p = name + CBM_SHA256_HEX_LEN + 1; *p; p++) {
        if (!isalnum((unsigned char)*p) && *p != '_' && *p != '-' && *p != '.') {
            return false;
        }
    }
    return name[CBM_SHA256_HEX_LEN + 1] != '\0';
}

static bool page_path_component_valid(const char *value) {
    if (!value || !value[0] || strlen(value) >= MEM_SHARE_NAME_CAP || strcmp(value, ".") == 0 ||
        strcmp(value, "..") == 0) {
        return false;
    }
    for (const unsigned char *p = (const unsigned char *)value; *p; p++) {
        if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') ||
              *p == '-' || *p == '_')) {
            return false;
        }
    }
    return true;
}

static bool portable_entity_id_valid(const char *value) {
    if (!value || !value[0] || strlen(value) >= 96) {
        return false;
    }
    for (const unsigned char *p = (const unsigned char *)value; *p; p++) {
        if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') ||
              *p == '_' || *p == '-' || *p == '.' || *p == ':')) {
            return false;
        }
    }
    return true;
}

static bool table_exists(sqlite3 *db, const char *table) {
    sqlite3_stmt *statement = NULL;
    bool exists = false;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1 LIMIT 1",
                           -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, table, -1, SQLITE_STATIC);
        exists = sqlite3_step(statement) == SQLITE_ROW;
    }
    sqlite3_finalize(statement);
    return exists;
}

static void columns_free(memory_columns_t *columns) {
    if (!columns) {
        return;
    }
    for (int i = 0; i < columns->count; i++) {
        free(columns->items[i].name);
    }
    free(columns->items);
    memset(columns, 0, sizeof(*columns));
}

static bool table_columns(sqlite3 *db, const char *table, memory_columns_t *out) {
    memset(out, 0, sizeof(*out));
    char *sql = sqlite3_mprintf("PRAGMA table_info(\"%w\")", table);
    if (!sql) {
        return false;
    }
    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db, sql, -1, &statement, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        return false;
    }
    while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
        memory_column_t *next = realloc(out->items, (size_t)(out->count + 1) * sizeof(*out->items));
        if (!next) {
            sqlite3_finalize(statement);
            columns_free(out);
            return false;
        }
        out->items = next;
        const unsigned char *name = sqlite3_column_text(statement, 1);
        out->items[out->count].name = share_strdup(name ? (const char *)name : "");
        out->items[out->count].pk_order = sqlite3_column_int(statement, 5);
        if (!out->items[out->count].name) {
            sqlite3_finalize(statement);
            columns_free(out);
            return false;
        }
        out->count++;
    }
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE && out->count > 0;
}

static bool sql_append(char *sql, size_t cap, const char *text) {
    size_t used = strlen(sql);
    size_t add = strlen(text);
    if (used + add >= cap) {
        return false;
    }
    memcpy(sql + used, text, add + 1);
    return true;
}

static bool sql_append_identifier(char *sql, size_t cap, const char *identifier) {
    if (!sql_append(sql, cap, "\"")) {
        return false;
    }
    for (const char *p = identifier; *p; p++) {
        if (*p == '"' && !sql_append(sql, cap, "\"")) {
            return false;
        }
        char one[2] = {*p, '\0'};
        if (!sql_append(sql, cap, one)) {
            return false;
        }
    }
    return sql_append(sql, cap, "\"");
}

static yyjson_mut_val *sqlite_value_json(yyjson_mut_doc *doc, sqlite3_stmt *statement, int col) {
    switch (sqlite3_column_type(statement, col)) {
    case SQLITE_NULL:
        return yyjson_mut_null(doc);
    case SQLITE_INTEGER:
        return yyjson_mut_sint(doc, sqlite3_column_int64(statement, col));
    case SQLITE_FLOAT:
        return yyjson_mut_real(doc, sqlite3_column_double(statement, col));
    case SQLITE_BLOB: {
        const void *data = sqlite3_column_blob(statement, col);
        int len = sqlite3_column_bytes(statement, col);
        char *hex = hex_encode(data, len > 0 ? (size_t)len : 0);
        if (!hex) {
            return NULL;
        }
        yyjson_mut_val *wrapped = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strncpy(doc, wrapped, "$hex", hex, strlen(hex));
        free(hex);
        return wrapped;
    }
    case SQLITE_TEXT:
    default: {
        const char *text = (const char *)sqlite3_column_text(statement, col);
        int len = sqlite3_column_bytes(statement, col);
        return yyjson_mut_strncpy(doc, text ? text : "", len > 0 ? (size_t)len : 0);
    }
    }
}

static bool build_select_sql(const char *table, const memory_columns_t *columns, char *sql,
                             size_t cap) {
    sql[0] = '\0';
    if (!sql_append(sql, cap, "SELECT ")) {
        return false;
    }
    for (int i = 0; i < columns->count; i++) {
        if (i && !sql_append(sql, cap, ",")) {
            return false;
        }
        if (!sql_append_identifier(sql, cap, columns->items[i].name)) {
            return false;
        }
    }
    if (!sql_append(sql, cap, " FROM ") || !sql_append_identifier(sql, cap, table) ||
        !sql_append(sql, cap, " ORDER BY ")) {
        return false;
    }
    bool have_pk = false;
    for (int order = 1; order <= columns->count; order++) {
        for (int i = 0; i < columns->count; i++) {
            if (columns->items[i].pk_order != order) {
                continue;
            }
            if (have_pk && !sql_append(sql, cap, ",")) {
                return false;
            }
            if (!sql_append_identifier(sql, cap, columns->items[i].name)) {
                return false;
            }
            have_pk = true;
        }
    }
    if (!have_pk) {
        for (int i = 0; i < columns->count; i++) {
            if (i && !sql_append(sql, cap, ",")) {
                return false;
            }
            if (!sql_append_identifier(sql, cap, columns->items[i].name)) {
                return false;
            }
        }
    }
    return true;
}

static yyjson_mut_val *export_table(yyjson_mut_doc *doc, sqlite3 *db,
                                    const memory_table_spec_t *spec, yyjson_mut_val *table_map,
                                    bool *ok) {
    yyjson_mut_val *rows = yyjson_mut_arr(doc);
    if (!table_exists(db, spec->table)) {
        return rows;
    }
    memory_columns_t columns;
    if (!table_columns(db, spec->table, &columns)) {
        *ok = false;
        return rows;
    }
    yyjson_mut_obj_add_strcpy(doc, table_map, spec->json_key, spec->table);
    char sql[MEM_SHARE_SQL_CAP];
    if (!build_select_sql(spec->table, &columns, sql, sizeof(sql))) {
        columns_free(&columns);
        *ok = false;
        return rows;
    }
    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db, sql, -1, &statement, NULL);
    if (rc != SQLITE_OK) {
        columns_free(&columns);
        *ok = false;
        return rows;
    }
    while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
        yyjson_mut_val *row = yyjson_mut_obj(doc);
        for (int i = 0; i < columns.count; i++) {
            yyjson_mut_val *value = sqlite_value_json(doc, statement, i);
            if (!value) {
                *ok = false;
                break;
            }
            yyjson_mut_val *key = yyjson_mut_strcpy(doc, columns.items[i].name);
            if (!key || !yyjson_mut_obj_add(row, key, value)) {
                *ok = false;
                break;
            }
        }
        if (!*ok) {
            break;
        }
        yyjson_mut_arr_add_val(rows, row);
    }
    if (rc != SQLITE_DONE) {
        *ok = false;
    }
    sqlite3_finalize(statement);
    columns_free(&columns);
    return rows;
}

static yyjson_mut_val *export_raw_objects(yyjson_mut_doc *doc, sqlite3 *db, const char *home,
                                          bool *ok) {
    yyjson_mut_val *objects = yyjson_mut_arr(doc);
    if (!table_exists(db, "memory_sources")) {
        return objects;
    }
    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(
        db, "SELECT object_relpath,content_hash FROM memory_sources ORDER BY source_id", -1,
        &statement, NULL);
    if (rc != SQLITE_OK) {
        *ok = false;
        return objects;
    }
    while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
        const char *relative = (const char *)sqlite3_column_text(statement, 0);
        const char *expected_hash = (const char *)sqlite3_column_text(statement, 1);
        char absolute[MEM_SHARE_PATH_CAP];
        size_t len = 0;
        if (!object_relpath_valid(relative, expected_hash) ||
            !path_join(absolute, sizeof(absolute), home, relative)) {
            *ok = false;
            break;
        }
        /* A source path must resolve underneath the canonical raw root.  This
         * rejects file and directory symlinks that escape the memory home. */
        char raw_root[MEM_SHARE_PATH_CAP];
        char canonical_root[MEM_SHARE_PATH_CAP];
        char canonical_file[MEM_SHARE_PATH_CAP];
        if (!path_join(raw_root, sizeof(raw_root), home, "raw/objects") ||
            !cbm_canonical_path(raw_root, canonical_root, sizeof(canonical_root)) ||
            !cbm_canonical_path(absolute, canonical_file, sizeof(canonical_file)) ||
            strlen(canonical_file) <= strlen(canonical_root) ||
            strncmp(canonical_file, canonical_root, strlen(canonical_root)) != 0 ||
            (canonical_file[strlen(canonical_root)] != '/' &&
             canonical_file[strlen(canonical_root)] != '\\')) {
            *ok = false;
            break;
        }
        unsigned char *data = read_file(absolute, MEM_SHARE_RAW_MAX, &len);
        if (!data) {
            *ok = false;
            break;
        }
        char hash[CBM_SHA256_HEX_LEN + 1];
        cbm_sha256_hex(data, len, hash);
        if (strcmp(hash, expected_hash) != 0) {
            free(data);
            *ok = false;
            break;
        }
        char *hex = hex_encode(data, len);
        free(data);
        if (!hex) {
            *ok = false;
            break;
        }
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, item, "path", relative);
        yyjson_mut_obj_add_strcpy(doc, item, "sha256", hash);
        yyjson_mut_obj_add_str(doc, item, "encoding", "hex");
        yyjson_mut_obj_add_strncpy(doc, item, "content", hex, strlen(hex));
        yyjson_mut_arr_add_val(objects, item);
        free(hex);
    }
    if (rc != SQLITE_DONE) {
        *ok = false;
    }
    sqlite3_finalize(statement);
    return objects;
}

static bool default_export_path(cbm_memory_t *memory, char *out, size_t cap) {
    return path_join(out, cap, cbm_memory_home(memory), "export/memory-export.json");
}

char *cbm_memory_export_json(cbm_memory_t *memory, const char *args_json) {
    if (!memory) {
        return share_json_result(false, "memory handle is required", NULL, -1, -1, 0, 0, 0);
    }
    yyjson_doc *args_doc = parse_args(args_json);
    if (!args_doc) {
        return share_json_result(false, "invalid JSON arguments", NULL, -1, -1, 0, 0, 0);
    }
    yyjson_val *args = yyjson_doc_get_root(args_doc);
    const char *requested = json_string(args, "path");
    char path[MEM_SHARE_PATH_CAP];
    if (requested) {
        if (snprintf(path, sizeof(path), "%s", requested) < 0 ||
            strlen(requested) >= sizeof(path)) {
            yyjson_doc_free(args_doc);
            return share_json_result(false, "export path is too long", NULL, -1, -1, 0, 0, 0);
        }
    } else if (!default_export_path(memory, path, sizeof(path))) {
        yyjson_doc_free(args_doc);
        return share_json_result(false, "cannot construct export path", NULL, -1, -1, 0, 0, 0);
    }
    yyjson_doc_free(args_doc);

    sqlite3 *db = cbm_memory_db(memory);
    if (!db || sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL) != SQLITE_OK) {
        return share_json_result(false, "memory database is unavailable", path, -1, -1, 0, 0, 0);
    }
    /* Read the epoch only after BEGIN: all table rows and the epoch now come
     * from one SQLite snapshot even while another Agent commits concurrently. */
    int64_t epoch = cbm_memory_snapshot_epoch(memory);
    if (epoch < 0) {
        (void)sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return share_json_result(false, "cannot read memory snapshot", path, -1, -1, 0, 0, 0);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        (void)sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "schema", CBM_MEMORY_BUNDLE_SCHEMA);
    yyjson_mut_obj_add_int(doc, root, "bundle_version", CBM_MEMORY_BUNDLE_VERSION);
    yyjson_mut_obj_add_int(doc, root, "memory_schema_version", CBM_MEMORY_SCHEMA_VERSION);
    yyjson_mut_obj_add_sint(doc, root, "snapshot_epoch", epoch);
    yyjson_mut_val *table_map = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, root, "tables", table_map);

    bool ok = true;
    for (size_t i = 0; i < MEMORY_TABLE_COUNT; i++) {
        yyjson_mut_val *rows = export_table(doc, db, &MEMORY_TABLES[i], table_map, &ok);
        yyjson_mut_obj_add_val(doc, root, MEMORY_TABLES[i].json_key, rows);
        if (!ok) {
            break;
        }
    }
    if (ok) {
        yyjson_mut_val *raw = export_raw_objects(doc, db, cbm_memory_home(memory), &ok);
        yyjson_mut_obj_add_val(doc, root, "raw_objects", raw);
    }
    if (!ok) {
        (void)sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        yyjson_mut_doc_free(doc);
        return share_json_result(false, "failed to build deterministic export", path, epoch, -1, 0,
                                 0, 0);
    }
    if (sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) {
        (void)sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        yyjson_mut_doc_free(doc);
        return share_json_result(false, "failed to close export snapshot", path, epoch, -1, 0, 0,
                                 0);
    }

    size_t len = 0;
    char *json = yyjson_mut_write(doc, 0, &len);
    yyjson_mut_doc_free(doc);
    if (!json) {
        return NULL;
    }
    int rc = write_atomic(path, json, len);
    char digest[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(json, len, digest);
    free(json);
    if (rc != 0) {
        return share_json_result(false, "failed to atomically write export", path, epoch, -1, 0, 0,
                                 0);
    }

    yyjson_mut_doc *result_doc = yyjson_mut_doc_new(NULL);
    if (!result_doc) {
        return NULL;
    }
    yyjson_mut_val *result = yyjson_mut_obj(result_doc);
    yyjson_mut_doc_set_root(result_doc, result);
    yyjson_mut_obj_add_bool(result_doc, result, "ok", true);
    yyjson_mut_obj_add_strcpy(result_doc, result, "path", path);
    yyjson_mut_obj_add_sint(result_doc, result, "snapshot_epoch", epoch);
    yyjson_mut_obj_add_strcpy(result_doc, result, "sha256", digest);
    yyjson_mut_obj_add_uint(result_doc, result, "bytes", len);
    char *response = yyjson_mut_write(result_doc, 0, NULL);
    yyjson_mut_doc_free(result_doc);
    return response;
}

/* ── Logical merge import ─────────────────────────────────────── */

typedef struct {
    char **items;
    int count;
    int cap;
} conflict_list_t;

typedef struct {
    char **tables;
    char **ids;
    int count;
    int cap;
} blocked_list_t;

static void conflict_list_free(conflict_list_t *list) {
    if (!list) {
        return;
    }
    for (int i = 0; i < list->count; i++) {
        free(list->items[i]);
    }
    free(list->items);
    memset(list, 0, sizeof(*list));
}

static bool conflict_list_add(conflict_list_t *list, const char *table, const char *key) {
    if (list->count == list->cap) {
        int next_cap = list->cap ? list->cap * 2 : 8;
        char **next = realloc(list->items, (size_t)next_cap * sizeof(*next));
        if (!next) {
            return false;
        }
        list->items = next;
        list->cap = next_cap;
    }
    size_t needed = strlen(table) + strlen(key) + 64;
    char *json = malloc(needed);
    if (!json) {
        return false;
    }
    /* Table and generated key only contain the allowlisted identifier/hex
     * alphabet, so direct formatting cannot inject JSON. */
    (void)snprintf(json, needed, "{\"op\":\"import_conflict\",\"table\":\"%s\",\"key\":\"%s\"}",
                   table, key);
    list->items[list->count++] = json;
    return true;
}

static void blocked_list_free(blocked_list_t *list) {
    if (!list) {
        return;
    }
    for (int i = 0; i < list->count; i++) {
        free(list->tables[i]);
        free(list->ids[i]);
    }
    free(list->tables);
    free(list->ids);
    memset(list, 0, sizeof(*list));
}

static bool blocked_list_add(blocked_list_t *list, const char *table, const char *id) {
    if (!table || !id || !id[0]) {
        return true;
    }
    if (list->count == list->cap) {
        int next_cap = list->cap ? list->cap * 2 : 8;
        char **next_tables = realloc(list->tables, (size_t)next_cap * sizeof(*next_tables));
        if (!next_tables) {
            return false;
        }
        list->tables = next_tables;
        char **next_ids = realloc(list->ids, (size_t)next_cap * sizeof(*next_ids));
        if (!next_ids) {
            return false;
        }
        list->ids = next_ids;
        list->cap = next_cap;
    }
    list->tables[list->count] = share_strdup(table);
    list->ids[list->count] = share_strdup(id);
    if (!list->tables[list->count] || !list->ids[list->count]) {
        free(list->tables[list->count]);
        free(list->ids[list->count]);
        return false;
    }
    list->count++;
    return true;
}

static bool blocked_list_has(const blocked_list_t *list, const char *table, const char *id) {
    if (!id) {
        return false;
    }
    for (int i = 0; i < list->count; i++) {
        if (strcmp(list->tables[i], table) == 0 && strcmp(list->ids[i], id) == 0) {
            return true;
        }
    }
    return false;
}

static const char *kind_table(const char *kind) {
    if (!kind) {
        return NULL;
    }
    if (strcmp(kind, "source") == 0)
        return "memory_sources";
    if (strcmp(kind, "page") == 0)
        return "memory_pages";
    if (strcmp(kind, "revision") == 0)
        return "memory_revisions";
    if (strcmp(kind, "claim") == 0)
        return "memory_claims";
    if (strcmp(kind, "decision") == 0)
        return "memory_decisions";
    if (strcmp(kind, "experience") == 0)
        return "memory_experiences";
    if (strcmp(kind, "preference") == 0)
        return "memory_preferences";
    if (strcmp(kind, "code_ref") == 0)
        return "memory_code_refs";
    if (strcmp(kind, "activity") == 0)
        return "memory_activities";
    return NULL;
}

static bool row_depends_on_blocked(const char *table, yyjson_val *row,
                                   const blocked_list_t *blocked) {
    if (strcmp(table, "memory_sources") == 0) {
        return blocked_list_has(blocked, "memory_sources", json_string(row, "revision_of"));
    }
    if (strcmp(table, "memory_revisions") == 0) {
        return blocked_list_has(blocked, "memory_pages", json_string(row, "page_id"));
    }
    if (strcmp(table, "memory_claim_revisions") == 0) {
        return blocked_list_has(blocked, "memory_claims", json_string(row, "claim_id"));
    }
    if (strcmp(table, "memory_relations") == 0) {
        const char *source_table = kind_table(json_string(row, "source_kind"));
        const char *target_table = kind_table(json_string(row, "target_kind"));
        return (source_table &&
                blocked_list_has(blocked, source_table, json_string(row, "source_id"))) ||
               (target_table &&
                blocked_list_has(blocked, target_table, json_string(row, "target_id")));
    }
    if (strcmp(table, "memory_operations") == 0) {
        return blocked_list_has(blocked, "memory_proposals", json_string(row, "proposal_id"));
    }
    if (strcmp(table, "memory_activities") == 0) {
        return blocked_list_has(blocked, "memory_proposals", json_string(row, "proposal_id")) ||
               blocked_list_has(blocked, "memory_operations", json_string(row, "operation_id"));
    }
    return false;
}

static merge_policy_t parse_policy(const char *policy, bool *valid) {
    *valid = true;
    if (!policy || strcmp(policy, "reject") == 0) {
        return MERGE_REJECT;
    }
    if (strcmp(policy, "keep_local") == 0) {
        return MERGE_KEEP_LOCAL;
    }
    if (strcmp(policy, "keep_remote") == 0) {
        return MERGE_KEEP_REMOTE;
    }
    if (strcmp(policy, "newest") == 0) {
        return MERGE_NEWEST;
    }
    *valid = false;
    return MERGE_REJECT;
}

static bool bundle_header_valid(yyjson_val *root, const char **error) {
    const char *schema = json_string(root, "schema");
    yyjson_val *bundle_version = yyjson_obj_get(root, "bundle_version");
    yyjson_val *memory_version = yyjson_obj_get(root, "memory_schema_version");
    if (!schema || strcmp(schema, CBM_MEMORY_BUNDLE_SCHEMA) != 0) {
        *error = "unsupported bundle schema";
        return false;
    }
    if (!bundle_version || yyjson_get_int(bundle_version) != CBM_MEMORY_BUNDLE_VERSION) {
        *error = "unsupported bundle version";
        return false;
    }
    if (!memory_version || yyjson_get_int(memory_version) > CBM_MEMORY_SCHEMA_VERSION ||
        yyjson_get_int(memory_version) < 1) {
        *error = "unsupported memory schema version";
        return false;
    }
    for (size_t i = 0; i < MEMORY_TABLE_COUNT; i++) {
        yyjson_val *rows = yyjson_obj_get(root, MEMORY_TABLES[i].json_key);
        if (!rows || !yyjson_is_arr(rows)) {
            *error = "bundle is missing a required logical table";
            return false;
        }
    }
    return true;
}

static bool safe_object_target(const char *home, const char *relative, const char *hash,
                               char target[MEM_SHARE_PATH_CAP]) {
    if (!object_relpath_valid(relative, hash) ||
        !path_join(target, MEM_SHARE_PATH_CAP, home, relative)) {
        return false;
    }
    char root[MEM_SHARE_PATH_CAP];
    char root_canon[MEM_SHARE_PATH_CAP];
    char dir[MEM_SHARE_PATH_CAP];
    char dir_canon[MEM_SHARE_PATH_CAP];
    if (!path_join(root, sizeof(root), home, "raw/objects") ||
        !cbm_mkdir_p(root, MEM_SHARE_DIR_MODE) || !parent_dir(target, dir, sizeof(dir)) ||
        !cbm_mkdir_p(dir, MEM_SHARE_DIR_MODE) ||
        !cbm_canonical_path(root, root_canon, sizeof(root_canon)) ||
        !cbm_canonical_path(dir, dir_canon, sizeof(dir_canon))) {
        return false;
    }
    size_t root_len = strlen(root_canon);
    return strlen(dir_canon) > root_len && strncmp(dir_canon, root_canon, root_len) == 0 &&
           (dir_canon[root_len] == '/' || dir_canon[root_len] == '\\');
}

/* Validate every raw entry before writing any object. */
static bool validate_raw_bundle(yyjson_val *root, const char **error) {
    yyjson_val *objects = yyjson_obj_get(root, "raw_objects");
    if (!objects || !yyjson_is_arr(objects)) {
        *error = "raw_objects must be an array";
        return false;
    }
    size_t index, max;
    yyjson_val *item;
    yyjson_arr_foreach(objects, index, max, item) {
        const char *path = json_string(item, "path");
        const char *hash = json_string(item, "sha256");
        const char *encoding = json_string(item, "encoding");
        const char *content = json_string(item, "content");
        if (!yyjson_is_obj(item) || !object_relpath_valid(path, hash) || !encoding ||
            strcmp(encoding, "hex") != 0 || !content || strlen(content) > MEM_SHARE_RAW_MAX * 2U) {
            *error = "invalid raw object entry";
            return false;
        }
        size_t len = 0;
        unsigned char *data = hex_decode(content, &len);
        if (!data) {
            *error = "invalid raw object encoding";
            return false;
        }
        char actual[CBM_SHA256_HEX_LEN + 1];
        cbm_sha256_hex(data, len, actual);
        free(data);
        if (strcmp(actual, hash) != 0) {
            *error = "raw object hash mismatch";
            return false;
        }
    }
    return true;
}

static bool install_raw_bundle(cbm_memory_t *memory, yyjson_val *root, int *added, int *skipped,
                               const char **error) {
    yyjson_val *objects = yyjson_obj_get(root, "raw_objects");
    size_t index, max;
    yyjson_val *item;
    yyjson_arr_foreach(objects, index, max, item) {
        const char *path = json_string(item, "path");
        const char *hash = json_string(item, "sha256");
        const char *content = json_string(item, "content");
        char target[MEM_SHARE_PATH_CAP];
        if (!safe_object_target(cbm_memory_home(memory), path, hash, target)) {
            *error = "raw object path escapes memory home";
            return false;
        }
        size_t existing_len = 0;
        unsigned char *existing = read_file(target, MEM_SHARE_RAW_MAX, &existing_len);
        if (existing) {
            char existing_hash[CBM_SHA256_HEX_LEN + 1];
            cbm_sha256_hex(existing, existing_len, existing_hash);
            free(existing);
            if (strcmp(existing_hash, hash) != 0) {
                *error = "immutable raw object collision";
                return false;
            }
            (*skipped)++;
            continue;
        }
        size_t len = 0;
        unsigned char *data = hex_decode(content, &len);
        if (!data || write_atomic(target, data, len) != 0) {
            free(data);
            *error = "failed to install raw object";
            return false;
        }
        free(data);
        (*added)++;
    }
    return true;
}

static yyjson_val *json_blob_hex(yyjson_val *value) {
    return value && yyjson_is_obj(value) ? yyjson_obj_get(value, "$hex") : NULL;
}

static int bind_json_value(sqlite3_stmt *statement, int parameter, yyjson_val *value) {
    if (!value || yyjson_is_null(value)) {
        return sqlite3_bind_null(statement, parameter);
    }
    if (yyjson_is_bool(value)) {
        return sqlite3_bind_int(statement, parameter, yyjson_get_bool(value) ? 1 : 0);
    }
    if (yyjson_is_int(value)) {
        return sqlite3_bind_int64(statement, parameter, yyjson_get_sint(value));
    }
    if (yyjson_is_uint(value)) {
        uint64_t number = yyjson_get_uint(value);
        if (number > INT64_MAX) {
            return SQLITE_RANGE;
        }
        return sqlite3_bind_int64(statement, parameter, (int64_t)number);
    }
    if (yyjson_is_real(value)) {
        return sqlite3_bind_double(statement, parameter, yyjson_get_real(value));
    }
    if (yyjson_is_str(value)) {
        return sqlite3_bind_text(statement, parameter, yyjson_get_str(value),
                                 (int)yyjson_get_len(value), SQLITE_TRANSIENT);
    }
    yyjson_val *hex = json_blob_hex(value);
    if (hex && yyjson_is_str(hex)) {
        size_t len = 0;
        unsigned char *data = hex_decode(yyjson_get_str(hex), &len);
        if (!data) {
            return SQLITE_MISMATCH;
        }
        int rc = sqlite3_bind_blob(statement, parameter, data, (int)len, free);
        if (rc != SQLITE_OK) {
            free(data);
        }
        return rc;
    }
    return SQLITE_MISMATCH;
}

static bool sqlite_value_equals_json(sqlite3_stmt *statement, int column, yyjson_val *value) {
    if (!value || yyjson_is_null(value)) {
        return sqlite3_column_type(statement, column) == SQLITE_NULL;
    }
    if (yyjson_is_bool(value)) {
        return sqlite3_column_type(statement, column) == SQLITE_INTEGER &&
               sqlite3_column_int(statement, column) == (yyjson_get_bool(value) ? 1 : 0);
    }
    if (yyjson_is_int(value)) {
        return sqlite3_column_type(statement, column) == SQLITE_INTEGER &&
               sqlite3_column_int64(statement, column) == yyjson_get_sint(value);
    }
    if (yyjson_is_uint(value)) {
        return sqlite3_column_type(statement, column) == SQLITE_INTEGER &&
               yyjson_get_uint(value) <= INT64_MAX &&
               sqlite3_column_int64(statement, column) == (int64_t)yyjson_get_uint(value);
    }
    if (yyjson_is_real(value)) {
        return sqlite3_column_type(statement, column) == SQLITE_FLOAT &&
               sqlite3_column_double(statement, column) == yyjson_get_real(value);
    }
    if (yyjson_is_str(value)) {
        const char *text = (const char *)sqlite3_column_text(statement, column);
        int len = sqlite3_column_bytes(statement, column);
        return sqlite3_column_type(statement, column) == SQLITE_TEXT &&
               (size_t)len == yyjson_get_len(value) &&
               memcmp(text ? text : "", yyjson_get_str(value), (size_t)len) == 0;
    }
    yyjson_val *hex = json_blob_hex(value);
    if (hex && yyjson_is_str(hex)) {
        size_t len = 0;
        unsigned char *data = hex_decode(yyjson_get_str(hex), &len);
        if (!data) {
            return false;
        }
        const void *local = sqlite3_column_blob(statement, column);
        int local_len = sqlite3_column_bytes(statement, column);
        bool equal = sqlite3_column_type(statement, column) == SQLITE_BLOB &&
                     (size_t)local_len == len && memcmp(local, data, len) == 0;
        free(data);
        return equal;
    }
    return false;
}

static bool column_is_pk(const memory_columns_t *columns, int index) {
    return columns->items[index].pk_order > 0;
}

static int prepare_existing(sqlite3 *db, const char *table, const memory_columns_t *columns,
                            yyjson_val *row, sqlite3_stmt **out) {
    char sql[MEM_SHARE_SQL_CAP] = "SELECT ";
    int pk_count = 0;
    for (int i = 0; i < columns->count; i++) {
        if (i && !sql_append(sql, sizeof(sql), ",")) {
            return -1;
        }
        if (!sql_append_identifier(sql, sizeof(sql), columns->items[i].name)) {
            return -1;
        }
        if (column_is_pk(columns, i)) {
            pk_count++;
        }
    }
    if (pk_count == 0 || !sql_append(sql, sizeof(sql), " FROM ") ||
        !sql_append_identifier(sql, sizeof(sql), table) ||
        !sql_append(sql, sizeof(sql), " WHERE ")) {
        return -1;
    }
    int seen = 0;
    for (int order = 1; order <= pk_count; order++) {
        for (int i = 0; i < columns->count; i++) {
            if (columns->items[i].pk_order != order) {
                continue;
            }
            if (seen && !sql_append(sql, sizeof(sql), " AND ")) {
                return -1;
            }
            if (!sql_append_identifier(sql, sizeof(sql), columns->items[i].name) ||
                !sql_append(sql, sizeof(sql), "=?")) {
                return -1;
            }
            char number[32];
            (void)snprintf(number, sizeof(number), "%d", seen + 1);
            if (!sql_append(sql, sizeof(sql), number)) {
                return -1;
            }
            seen++;
        }
    }
    sqlite3_stmt *statement = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    int parameter = 1;
    for (int order = 1; order <= pk_count; order++) {
        for (int i = 0; i < columns->count; i++) {
            if (columns->items[i].pk_order != order) {
                continue;
            }
            yyjson_val *value = yyjson_obj_get(row, columns->items[i].name);
            if (!value || bind_json_value(statement, parameter++, value) != SQLITE_OK) {
                sqlite3_finalize(statement);
                return -1;
            }
        }
    }
    int rc = sqlite3_step(statement);
    if (rc == SQLITE_ROW) {
        *out = statement;
        return 1;
    }
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE ? 0 : -1;
}

static int prepare_existing_natural(sqlite3 *db, const char *table, const memory_columns_t *columns,
                                    yyjson_val *row, sqlite3_stmt **out) {
    const char *keys[6] = {0};
    int key_count = 0;
    bool current_relation = false;
    if (strcmp(table, "memory_sources") == 0) {
        keys[key_count++] = "content_hash";
    } else if (strcmp(table, "memory_pages") == 0) {
        keys[key_count++] = "slug";
    } else if (strcmp(table, "memory_revisions") == 0) {
        keys[key_count++] = "page_id";
        keys[key_count++] = "revision";
    } else if (strcmp(table, "memory_relations") == 0 &&
               yyjson_is_null(yyjson_obj_get(row, "recorded_to"))) {
        keys[key_count++] = "source_kind";
        keys[key_count++] = "source_id";
        keys[key_count++] = "target_kind";
        keys[key_count++] = "target_id";
        keys[key_count++] = "type";
        keys[key_count++] = "relation_key";
        current_relation = true;
    } else if (strcmp(table, "memory_code_refs") == 0) {
        keys[key_count++] = "project";
        keys[key_count++] = "ref_kind";
        keys[key_count++] = "qualified_name";
        keys[key_count++] = "file_path";
    } else if (strcmp(table, "memory_activities") == 0) {
        keys[key_count++] = "operation_id";
    }
    if (key_count == 0) {
        return 0;
    }

    char sql[MEM_SHARE_SQL_CAP] = "SELECT ";
    for (int i = 0; i < columns->count; i++) {
        if ((i && !sql_append(sql, sizeof(sql), ",")) ||
            !sql_append_identifier(sql, sizeof(sql), columns->items[i].name)) {
            return -1;
        }
    }
    if (!sql_append(sql, sizeof(sql), " FROM ") ||
        !sql_append_identifier(sql, sizeof(sql), table) ||
        !sql_append(sql, sizeof(sql), " WHERE ")) {
        return -1;
    }
    for (int i = 0; i < key_count; i++) {
        if ((i && !sql_append(sql, sizeof(sql), " AND ")) ||
            !sql_append_identifier(sql, sizeof(sql), keys[i]) ||
            !sql_append(sql, sizeof(sql), "=?")) {
            return -1;
        }
        char number[32];
        (void)snprintf(number, sizeof(number), "%d", i + 1);
        if (!sql_append(sql, sizeof(sql), number)) {
            return -1;
        }
    }
    if (current_relation && !sql_append(sql, sizeof(sql), " AND recorded_to IS NULL")) {
        return -1;
    }
    sqlite3_stmt *statement = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    for (int i = 0; i < key_count; i++) {
        yyjson_val *value = yyjson_obj_get(row, keys[i]);
        if (!value || bind_json_value(statement, i + 1, value) != SQLITE_OK) {
            sqlite3_finalize(statement);
            return -1;
        }
    }
    int rc = sqlite3_step(statement);
    if (rc == SQLITE_ROW) {
        *out = statement;
        return 1;
    }
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE ? 0 : -1;
}

static bool existing_row_equals(sqlite3_stmt *existing, const memory_columns_t *columns,
                                yyjson_val *row) {
    for (int i = 0; i < columns->count; i++) {
        yyjson_val *value = yyjson_obj_get(row, columns->items[i].name);
        if (!value || !sqlite_value_equals_json(existing, i, value)) {
            return false;
        }
    }
    return true;
}

static bool primary_key_equals(sqlite3_stmt *existing, const memory_columns_t *columns,
                               yyjson_val *row) {
    for (int i = 0; i < columns->count; i++) {
        if (column_is_pk(columns, i)) {
            yyjson_val *value = yyjson_obj_get(row, columns->items[i].name);
            if (!value || !sqlite_value_equals_json(existing, i, value)) {
                return false;
            }
        }
    }
    return true;
}

static int insert_row(sqlite3 *db, const char *table, const memory_columns_t *columns,
                      yyjson_val *row) {
    char sql[MEM_SHARE_SQL_CAP] = "INSERT INTO ";
    if (!sql_append_identifier(sql, sizeof(sql), table) || !sql_append(sql, sizeof(sql), "(")) {
        return -1;
    }
    int field_count = 0;
    for (int i = 0; i < columns->count; i++) {
        if (!yyjson_obj_get(row, columns->items[i].name)) {
            continue;
        }
        if (field_count && !sql_append(sql, sizeof(sql), ",")) {
            return -1;
        }
        if (!sql_append_identifier(sql, sizeof(sql), columns->items[i].name)) {
            return -1;
        }
        field_count++;
    }
    if (field_count == 0 || !sql_append(sql, sizeof(sql), ") VALUES(")) {
        return -1;
    }
    for (int i = 0; i < field_count; i++) {
        char parameter[32];
        (void)snprintf(parameter, sizeof(parameter), "%s?%d", i ? "," : "", i + 1);
        if (!sql_append(sql, sizeof(sql), parameter)) {
            return -1;
        }
    }
    if (!sql_append(sql, sizeof(sql), ")")) {
        return -1;
    }
    sqlite3_stmt *statement = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    int parameter = 1;
    for (int i = 0; i < columns->count; i++) {
        yyjson_val *value = yyjson_obj_get(row, columns->items[i].name);
        if (value && bind_json_value(statement, parameter++, value) != SQLITE_OK) {
            sqlite3_finalize(statement);
            return -1;
        }
    }
    int rc = sqlite3_step(statement);
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE ? 0 : -1;
}

static int update_row(sqlite3 *db, const char *table, const memory_columns_t *columns,
                      yyjson_val *row) {
    char sql[MEM_SHARE_SQL_CAP] = "UPDATE ";
    if (!sql_append_identifier(sql, sizeof(sql), table) || !sql_append(sql, sizeof(sql), " SET ")) {
        return -1;
    }
    int set_count = 0;
    int pk_count = 0;
    for (int i = 0; i < columns->count; i++) {
        yyjson_val *value = yyjson_obj_get(row, columns->items[i].name);
        if (column_is_pk(columns, i)) {
            pk_count++;
        } else if (value) {
            if (set_count && !sql_append(sql, sizeof(sql), ",")) {
                return -1;
            }
            if (!sql_append_identifier(sql, sizeof(sql), columns->items[i].name) ||
                !sql_append(sql, sizeof(sql), "=?")) {
                return -1;
            }
            char parameter[32];
            (void)snprintf(parameter, sizeof(parameter), "%d", set_count + 1);
            if (!sql_append(sql, sizeof(sql), parameter)) {
                return -1;
            }
            set_count++;
        }
    }
    if (set_count == 0 || pk_count == 0 || !sql_append(sql, sizeof(sql), " WHERE ")) {
        return -1;
    }
    int seen = 0;
    for (int order = 1; order <= pk_count; order++) {
        for (int i = 0; i < columns->count; i++) {
            if (columns->items[i].pk_order != order) {
                continue;
            }
            if (seen && !sql_append(sql, sizeof(sql), " AND ")) {
                return -1;
            }
            if (!sql_append_identifier(sql, sizeof(sql), columns->items[i].name) ||
                !sql_append(sql, sizeof(sql), "=?")) {
                return -1;
            }
            char parameter[32];
            (void)snprintf(parameter, sizeof(parameter), "%d", set_count + seen + 1);
            if (!sql_append(sql, sizeof(sql), parameter)) {
                return -1;
            }
            seen++;
        }
    }

    sqlite3_stmt *statement = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    int parameter = 1;
    for (int i = 0; i < columns->count; i++) {
        yyjson_val *value = yyjson_obj_get(row, columns->items[i].name);
        if (!column_is_pk(columns, i) && value &&
            bind_json_value(statement, parameter++, value) != SQLITE_OK) {
            sqlite3_finalize(statement);
            return -1;
        }
    }
    for (int order = 1; order <= pk_count; order++) {
        for (int i = 0; i < columns->count; i++) {
            if (columns->items[i].pk_order == order) {
                yyjson_val *value = yyjson_obj_get(row, columns->items[i].name);
                if (!value || bind_json_value(statement, parameter++, value) != SQLITE_OK) {
                    sqlite3_finalize(statement);
                    return -1;
                }
            }
        }
    }
    int rc = sqlite3_step(statement);
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE ? 0 : -1;
}

static void conflict_key(const char *table, yyjson_val *row, char out[CBM_SHA256_HEX_LEN + 1]) {
    char *row_json = yyjson_val_write(row, 0, NULL);
    cbm_sha256_ctx hash;
    unsigned char digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_init(&hash);
    cbm_sha256_update(&hash, table, strlen(table));
    if (row_json) {
        cbm_sha256_update(&hash, row_json, strlen(row_json));
    }
    cbm_sha256_final(&hash, digest);
    free(row_json);
    for (int i = 0; i < CBM_SHA256_DIGEST_LEN; i++) {
        out[i * 2] = hex_digit(digest[i] >> 4U);
        out[i * 2 + 1] = hex_digit(digest[i]);
    }
    out[CBM_SHA256_HEX_LEN] = '\0';
}

static int column_index(const memory_columns_t *columns, const char *name) {
    for (int i = 0; i < columns->count; i++) {
        if (strcmp(columns->items[i].name, name) == 0) {
            return i;
        }
    }
    return -1;
}

static bool code_ref_status_only(sqlite3_stmt *existing, const memory_columns_t *columns,
                                 yyjson_val *row) {
    static const char *const mutable[] = {
        "last_resolved_at", "resolution_status", "revision", "updated_epoch", "updated_at",
    };
    for (int i = 0; i < columns->count; i++) {
        yyjson_val *value = yyjson_obj_get(row, columns->items[i].name);
        if (!value || sqlite_value_equals_json(existing, i, value)) {
            continue;
        }
        bool allowed = false;
        for (size_t j = 0; j < sizeof(mutable) / sizeof(mutable[0]); j++) {
            if (strcmp(columns->items[i].name, mutable[j]) == 0) {
                allowed = true;
                break;
            }
        }
        if (!allowed) {
            return false;
        }
    }
    return true;
}

static bool incoming_is_newer(sqlite3_stmt *existing, const memory_columns_t *columns,
                              yyjson_val *row) {
    int epoch_col = column_index(columns, "updated_epoch");
    yyjson_val *epoch = yyjson_obj_get(row, "updated_epoch");
    if (epoch_col >= 0 && epoch && yyjson_is_num(epoch)) {
        uint64_t remote =
            yyjson_is_uint(epoch) ? yyjson_get_uint(epoch) : (uint64_t)yyjson_get_sint(epoch);
        return remote > (uint64_t)sqlite3_column_int64(existing, epoch_col);
    }
    int time_col = column_index(columns, "updated_at");
    yyjson_val *updated = yyjson_obj_get(row, "updated_at");
    if (time_col >= 0 && updated && yyjson_is_str(updated)) {
        const char *local = (const char *)sqlite3_column_text(existing, time_col);
        return !local || strcmp(yyjson_get_str(updated), local) > 0;
    }
    return false;
}

static int64_t json_int64(yyjson_val *value, int64_t fallback) {
    if (!value) {
        return fallback;
    }
    if (yyjson_is_uint(value)) {
        uint64_t number = yyjson_get_uint(value);
        return number <= INT64_MAX ? (int64_t)number : fallback;
    }
    return yyjson_is_int(value) ? yyjson_get_sint(value) : fallback;
}

/* A page pointer can advance only along a complete incoming base_revision
 * chain rooted at the local current revision.  Divergent same-page edits are
 * semantic conflicts regardless of keep_remote/newest. */
static bool page_can_fast_forward(yyjson_val *bundle_root, sqlite3_stmt *existing,
                                  const memory_columns_t *columns, yyjson_val *page) {
    int current_col = column_index(columns, "current_revision");
    const char *page_id = json_string(page, "page_id");
    int64_t target = json_int64(yyjson_obj_get(page, "current_revision"), -1);
    if (current_col < 0 || !page_id || target < 0) {
        return false;
    }
    static const char *const immutable_path_fields[] = {"slug", "page_kind"};
    for (size_t i = 0; i < sizeof(immutable_path_fields) / sizeof(immutable_path_fields[0]); i++) {
        int field = column_index(columns, immutable_path_fields[i]);
        yyjson_val *incoming = yyjson_obj_get(page, immutable_path_fields[i]);
        if (field < 0 || !incoming || !sqlite_value_equals_json(existing, field, incoming)) {
            return false;
        }
    }
    int64_t current = sqlite3_column_int64(existing, current_col);
    if (target <= current) {
        return false;
    }
    yyjson_val *revisions = yyjson_obj_get(bundle_root, "revisions");
    if (!revisions || !yyjson_is_arr(revisions)) {
        return false;
    }
    memory_columns_t revision_columns = {0};
    if (!table_columns(sqlite3_db_handle(existing), "memory_revisions", &revision_columns)) {
        return false;
    }
    const char *target_revision_id = NULL;
    bool valid = true;
    while (current < target) {
        bool found = false;
        size_t index, max;
        yyjson_val *revision;
        yyjson_arr_foreach(revisions, index, max, revision) {
            const char *revision_page = json_string(revision, "page_id");
            int64_t base = json_int64(yyjson_obj_get(revision, "base_revision"), -1);
            int64_t number = json_int64(yyjson_obj_get(revision, "revision"), -1);
            if (revision_page && strcmp(revision_page, page_id) == 0 && base == current &&
                number == current + 1) {
                sqlite3_stmt *local_revision = NULL;
                int local_found =
                    prepare_existing_natural(sqlite3_db_handle(existing), "memory_revisions",
                                             &revision_columns, revision, &local_revision);
                if (local_found < 0 ||
                    (local_found == 1 &&
                     !existing_row_equals(local_revision, &revision_columns, revision))) {
                    sqlite3_finalize(local_revision);
                    valid = false;
                    found = true;
                    break;
                }
                sqlite3_finalize(local_revision);
                current = number;
                if (current == target) {
                    target_revision_id = json_string(revision, "revision_id");
                }
                found = true;
                break;
            }
        }
        if (!found || !valid) {
            valid = false;
            break;
        }
    }
    const char *page_revision_id = json_string(page, "current_revision_id");
    valid = valid && current == target && target_revision_id && page_revision_id &&
            strcmp(target_revision_id, page_revision_id) == 0;
    columns_free(&revision_columns);
    return valid;
}

static const char *row_primary_id(const memory_columns_t *columns, yyjson_val *row) {
    for (int order = 1; order <= columns->count; order++) {
        for (int i = 0; i < columns->count; i++) {
            if (columns->items[i].pk_order == order) {
                yyjson_val *value = yyjson_obj_get(row, columns->items[i].name);
                return value && yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
            }
        }
    }
    return NULL;
}

/* Returns 0 on success, 1 when reject policy encountered a conflict, -1 on
 * malformed input/SQL failure. */
static int import_table(sqlite3 *db, yyjson_val *bundle_root, const memory_table_spec_t *spec,
                        merge_policy_t policy, int *added, int *skipped, int *updated,
                        conflict_list_t *conflicts, blocked_list_t *blocked) {
    yyjson_val *rows = yyjson_obj_get(bundle_root, spec->json_key);
    if (!rows || !yyjson_is_arr(rows) || !table_exists(db, spec->table)) {
        return rows && !yyjson_is_arr(rows) ? -1 : 0;
    }
    memory_columns_t columns;
    if (!table_columns(db, spec->table, &columns)) {
        return -1;
    }
    size_t index, max;
    yyjson_val *row;
    yyjson_arr_foreach(rows, index, max, row) {
        if (!yyjson_is_obj(row)) {
            columns_free(&columns);
            return -1;
        }
        if (row_depends_on_blocked(spec->table, row, blocked)) {
            char key[CBM_SHA256_HEX_LEN + 1];
            conflict_key(spec->table, row, key);
            if (!conflict_list_add(conflicts, spec->table, key) ||
                !blocked_list_add(blocked, spec->table, row_primary_id(&columns, row))) {
                columns_free(&columns);
                return -1;
            }
            (*skipped)++;
            continue;
        }
        sqlite3_stmt *existing = NULL;
        int found = prepare_existing(db, spec->table, &columns, row, &existing);
        if (found < 0) {
            columns_free(&columns);
            return -1;
        }
        if (found == 0) {
            found = prepare_existing_natural(db, spec->table, &columns, row, &existing);
            if (found < 0) {
                columns_free(&columns);
                return -1;
            }
        }
        if (found == 0) {
            if (insert_row(db, spec->table, &columns, row) != 0) {
                columns_free(&columns);
                return -1;
            }
            (*added)++;
            continue;
        }
        if (existing_row_equals(existing, &columns, row)) {
            sqlite3_finalize(existing);
            (*skipped)++;
            continue;
        }

        bool same_primary_key = primary_key_equals(existing, &columns, row);
        bool safe_update = false;
        if ((policy == MERGE_KEEP_REMOTE || policy == MERGE_NEWEST) && same_primary_key &&
            strcmp(spec->table, "memory_pages") == 0) {
            safe_update = page_can_fast_forward(bundle_root, existing, &columns, row);
        } else if ((policy == MERGE_KEEP_REMOTE || policy == MERGE_NEWEST) && same_primary_key &&
                   strcmp(spec->table, "memory_code_refs") == 0 &&
                   code_ref_status_only(existing, &columns, row)) {
            safe_update = policy == MERGE_KEEP_REMOTE || incoming_is_newer(existing, &columns, row);
        }
        sqlite3_finalize(existing);

        if (safe_update) {
            if (update_row(db, spec->table, &columns, row) != 0) {
                columns_free(&columns);
                return -1;
            }
            (*updated)++;
            continue;
        }

        char key[CBM_SHA256_HEX_LEN + 1];
        conflict_key(spec->table, row, key);
        if (!conflict_list_add(conflicts, spec->table, key)) {
            columns_free(&columns);
            return -1;
        }
        /* A semantic conflict blocks dependent rows even when it was found by
         * primary key.  Otherwise a kept-local page/source could still admit
         * remote revisions or relationships that were authored against the
         * rejected version. */
        if (!blocked_list_add(blocked, spec->table, row_primary_id(&columns, row))) {
            columns_free(&columns);
            return -1;
        }
        if (policy == MERGE_REJECT) {
            columns_free(&columns);
            return 1;
        }
        /* keep_local, and unsafe keep_remote/newest, retain local semantic
         * state and persist a conflict proposal after this transaction. */
        (*skipped)++;
    }
    columns_free(&columns);
    return 0;
}

/* The domain schema intentionally keeps polymorphic relation endpoints out of
 * SQLite foreign keys.  Validate those invariants explicitly before an import
 * transaction becomes visible, so an untrusted/truncated bundle can never
 * commit canonical rows that only fail later during projection rebuild. */
static bool import_state_integrity_ok(sqlite3 *db, const char *home) {
    static const char *const checks[] = {
        "SELECT 1 FROM pragma_foreign_key_check LIMIT 1;",
        "WITH entities(kind,id) AS ("
        " SELECT 'source',source_id FROM memory_sources UNION ALL"
        " SELECT 'page',page_id FROM memory_pages UNION ALL"
        " SELECT 'revision',revision_id FROM memory_revisions UNION ALL"
        " SELECT 'claim',claim_id FROM memory_claims UNION ALL"
        " SELECT 'decision',decision_id FROM memory_decisions UNION ALL"
        " SELECT 'experience',experience_id FROM memory_experiences UNION ALL"
        " SELECT 'preference',preference_id FROM memory_preferences UNION ALL"
        " SELECT 'code_ref',code_ref_id FROM memory_code_refs UNION ALL"
        " SELECT 'activity',activity_id FROM memory_activities)"
        " SELECT 1 FROM memory_relations r WHERE"
        " NOT EXISTS(SELECT 1 FROM entities e WHERE e.kind=r.source_kind AND e.id=r.source_id)"
        " OR NOT EXISTS(SELECT 1 FROM entities e WHERE e.kind=r.target_kind AND e.id=r.target_id)"
        " LIMIT 1;",
        "SELECT 1 FROM memory_pages p WHERE"
        " (p.current_revision=0 AND p.current_revision_id IS NOT NULL) OR"
        " (p.current_revision>0 AND NOT EXISTS(SELECT 1 FROM memory_revisions r"
        "  WHERE r.page_id=p.page_id AND r.revision=p.current_revision"
        "  AND r.revision_id=p.current_revision_id)) LIMIT 1;",
        "SELECT 1 FROM memory_revisions r WHERE"
        " (r.revision=1 AND r.base_revision<>0) OR"
        " (r.revision>1 AND (r.base_revision<>r.revision-1 OR NOT EXISTS("
        "  SELECT 1 FROM memory_revisions parent WHERE parent.page_id=r.page_id"
        "  AND parent.revision=r.base_revision))) LIMIT 1;",
        "WITH RECURSIVE lineage(start_id,current_id,depth) AS ("
        " SELECT source_id,revision_of,1 FROM memory_sources WHERE revision_of IS NOT NULL"
        " UNION ALL SELECT l.start_id,s.revision_of,l.depth+1 FROM lineage l"
        " JOIN memory_sources s ON s.source_id=l.current_id"
        " WHERE s.revision_of IS NOT NULL AND l.depth<64)"
        " SELECT 1 FROM lineage WHERE (current_id=start_id AND depth>1)"
        " OR (depth=64 AND EXISTS(SELECT 1 FROM memory_sources s"
        " WHERE s.source_id=current_id AND s.revision_of IS NOT NULL)) LIMIT 1;",
        "SELECT 1 FROM memory_operations o WHERE NOT EXISTS("
        " SELECT 1 FROM memory_proposals p WHERE p.proposal_id=o.proposal_id) LIMIT 1;",
        "SELECT 1 FROM memory_activities a WHERE a.proposal_id IS NOT NULL AND (NOT EXISTS("
        " SELECT 1 FROM memory_operations o WHERE o.operation_id=a.operation_id)"
        " OR NOT EXISTS(SELECT 1 FROM memory_proposals p"
        " WHERE p.proposal_id=a.proposal_id)) LIMIT 1;",
    };
    for (size_t i = 0; i < sizeof(checks) / sizeof(checks[0]); i++) {
        sqlite3_stmt *statement = NULL;
        if (sqlite3_prepare_v2(db, checks[i], -1, &statement, NULL) != SQLITE_OK) {
            sqlite3_finalize(statement);
            return false;
        }
        int rc = sqlite3_step(statement);
        sqlite3_finalize(statement);
        if (rc != SQLITE_DONE) {
            return false; /* SQLITE_ROW means the invariant was violated. */
        }
    }
    sqlite3_stmt *entity_ids = NULL;
    if (sqlite3_prepare_v2(db,
                           "SELECT source_id FROM memory_sources UNION ALL"
                           " SELECT page_id FROM memory_pages UNION ALL"
                           " SELECT revision_id FROM memory_revisions UNION ALL"
                           " SELECT claim_id FROM memory_claims UNION ALL"
                           " SELECT decision_id FROM memory_decisions UNION ALL"
                           " SELECT experience_id FROM memory_experiences UNION ALL"
                           " SELECT preference_id FROM memory_preferences UNION ALL"
                           " SELECT code_ref_id FROM memory_code_refs UNION ALL"
                           " SELECT activity_id FROM memory_activities;",
                           -1, &entity_ids, NULL) != SQLITE_OK) {
        sqlite3_finalize(entity_ids);
        return false;
    }
    int entity_rc = SQLITE_ROW;
    while ((entity_rc = sqlite3_step(entity_ids)) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(entity_ids, 0);
        if (!portable_entity_id_valid(id)) {
            sqlite3_finalize(entity_ids);
            return false;
        }
    }
    sqlite3_finalize(entity_ids);
    if (entity_rc != SQLITE_DONE) {
        return false;
    }
    sqlite3_stmt *pages = NULL;
    if (sqlite3_prepare_v2(db, "SELECT slug,page_kind FROM memory_pages ORDER BY page_id;", -1,
                           &pages, NULL) != SQLITE_OK) {
        sqlite3_finalize(pages);
        return false;
    }
    int page_rc = SQLITE_ROW;
    while ((page_rc = sqlite3_step(pages)) == SQLITE_ROW) {
        const char *slug = (const char *)sqlite3_column_text(pages, 0);
        const char *kind = (const char *)sqlite3_column_text(pages, 1);
        if (!page_path_component_valid(slug) || !page_path_component_valid(kind)) {
            sqlite3_finalize(pages);
            return false;
        }
    }
    sqlite3_finalize(pages);
    if (page_rc != SQLITE_DONE) {
        return false;
    }
    sqlite3_stmt *sources = NULL;
    if (sqlite3_prepare_v2(db,
                           "SELECT object_relpath,content_hash,byte_size"
                           " FROM memory_sources ORDER BY source_id;",
                           -1, &sources, NULL) != SQLITE_OK) {
        sqlite3_finalize(sources);
        return false;
    }
    int source_rc = SQLITE_ROW;
    while ((source_rc = sqlite3_step(sources)) == SQLITE_ROW) {
        const char *relative = (const char *)sqlite3_column_text(sources, 0);
        const char *expected = (const char *)sqlite3_column_text(sources, 1);
        sqlite3_int64 expected_size = sqlite3_column_int64(sources, 2);
        char target[MEM_SHARE_PATH_CAP];
        char raw_root[MEM_SHARE_PATH_CAP];
        char canonical_root[MEM_SHARE_PATH_CAP];
        char canonical_target[MEM_SHARE_PATH_CAP];
        size_t len = 0;
        if (!safe_object_target(home, relative, expected, target) ||
            !path_join(raw_root, sizeof(raw_root), home, "raw/objects") ||
            !cbm_canonical_path(raw_root, canonical_root, sizeof(canonical_root)) ||
            !cbm_canonical_path(target, canonical_target, sizeof(canonical_target)) ||
            strlen(canonical_target) <= strlen(canonical_root) ||
            strncmp(canonical_target, canonical_root, strlen(canonical_root)) != 0 ||
            (canonical_target[strlen(canonical_root)] != '/' &&
             canonical_target[strlen(canonical_root)] != '\\')) {
            sqlite3_finalize(sources);
            return false;
        }
        unsigned char *data = read_file(target, MEM_SHARE_RAW_MAX, &len);
        char actual[CBM_SHA256_HEX_LEN + 1];
        if (!data || expected_size < 0 || (uint64_t)expected_size != (uint64_t)len) {
            free(data);
            sqlite3_finalize(sources);
            return false;
        }
        cbm_sha256_hex(data, len, actual);
        free(data);
        if (strcmp(actual, expected) != 0) {
            sqlite3_finalize(sources);
            return false;
        }
    }
    sqlite3_finalize(sources);
    if (source_rc != SQLITE_DONE) {
        return false;
    }
    return true;
}

static char *persist_conflict_proposal(cbm_memory_t *memory, const conflict_list_t *conflicts,
                                       const char *bundle_hash) {
    if (!memory || !conflicts || conflicts->count == 0) {
        return NULL;
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    char proposal_id[MEM_SHARE_NAME_CAP];
    (void)snprintf(proposal_id, sizeof(proposal_id), "import-conflict:%s", bundle_hash);
    yyjson_mut_val *operations = yyjson_mut_arr(doc);
    for (int i = 0; i < conflicts->count; i++) {
        yyjson_doc *operation_doc =
            yyjson_read(conflicts->items[i], strlen(conflicts->items[i]), 0);
        if (!operation_doc) {
            yyjson_mut_doc_free(doc);
            return NULL;
        }
        yyjson_mut_val *operation = yyjson_val_mut_copy(doc, yyjson_doc_get_root(operation_doc));
        yyjson_doc_free(operation_doc);
        if (!operation) {
            yyjson_mut_doc_free(doc);
            return NULL;
        }
        yyjson_mut_arr_add_val(operations, operation);
    }
    yyjson_mut_obj_add_str(doc, root, "kind", "import_conflict");
    yyjson_mut_obj_add_val(doc, root, "conflicts", operations);
    char *conflict_json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    if (!conflict_json) {
        return NULL;
    }
    char now[64];
    time_t timestamp = time(NULL);
    struct tm utc;
    cbm_gmtime_r(&timestamp, &utc);
    (void)strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", &utc);
    sqlite3 *db = cbm_memory_db(memory);
    sqlite3_stmt *statement = NULL;
    const char *sql =
        "INSERT OR IGNORE INTO memory_proposals("
        " proposal_id,agent_id,session_id,base_epoch,status,operations_json,"
        " expected_revisions_json,reason,created_at,resolved_at,conflict_json)"
        " VALUES(?1,'memory-share','import',?2,'conflicted',json_extract(?3,'$.conflicts'),'{}',"
        " 'Semantic conflicts retained during import',?4,?4,?3)";
    bool inserted = false;
    if (db && sqlite3_prepare_v2(db, sql, -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, proposal_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(statement, 2, cbm_memory_snapshot_epoch(memory));
        sqlite3_bind_text(statement, 3, conflict_json, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 4, now, -1, SQLITE_TRANSIENT);
        inserted = sqlite3_step(statement) == SQLITE_DONE;
    }
    sqlite3_finalize(statement);
    free(conflict_json);
    if (!inserted) {
        return NULL;
    }
    yyjson_mut_doc *result_doc = yyjson_mut_doc_new(NULL);
    if (!result_doc) {
        return NULL;
    }
    yyjson_mut_val *result = yyjson_mut_obj(result_doc);
    yyjson_mut_doc_set_root(result_doc, result);
    yyjson_mut_obj_add_bool(result_doc, result, "ok", true);
    yyjson_mut_obj_add_strcpy(result_doc, result, "proposal_id", proposal_id);
    yyjson_mut_obj_add_str(result_doc, result, "status", "conflicted");
    char *response = yyjson_mut_write(result_doc, 0, NULL);
    yyjson_mut_doc_free(result_doc);
    return response;
}

static char *import_response(bool ok, const char *error, const char *path, int64_t epoch, int added,
                             int skipped, int updated, const conflict_list_t *conflicts,
                             const char *proposal_json, bool derived_views_ready) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_bool(doc, root, "ok", ok);
    if (error) {
        yyjson_mut_obj_add_strcpy(doc, root, "error", error);
    }
    if (path) {
        yyjson_mut_obj_add_strcpy(doc, root, "path", path);
    }
    if (epoch >= 0) {
        yyjson_mut_obj_add_sint(doc, root, "snapshot_epoch", epoch);
    }
    yyjson_mut_obj_add_int(doc, root, "added", added);
    yyjson_mut_obj_add_int(doc, root, "skipped", skipped);
    yyjson_mut_obj_add_int(doc, root, "updated", updated);
    yyjson_mut_obj_add_int(doc, root, "conflicts", conflicts ? conflicts->count : 0);
    yyjson_mut_obj_add_bool(doc, root, "derived_views_ready", derived_views_ready);
    if (proposal_json) {
        yyjson_doc *proposal_doc = yyjson_read(proposal_json, strlen(proposal_json), 0);
        if (proposal_doc) {
            yyjson_mut_val *proposal = yyjson_val_mut_copy(doc, yyjson_doc_get_root(proposal_doc));
            if (proposal) {
                yyjson_mut_obj_add_val(doc, root, "conflict_proposal", proposal);
            }
            yyjson_doc_free(proposal_doc);
        }
    }
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

char *cbm_memory_import_json(cbm_memory_t *memory, const char *args_json) {
    if (!memory) {
        return share_json_result(false, "memory handle is required", NULL, -1, 0, 0, 0, 0);
    }
    yyjson_doc *args_doc = parse_args(args_json);
    if (!args_doc) {
        return share_json_result(false, "invalid JSON arguments", NULL, -1, 0, 0, 0, 0);
    }
    yyjson_val *args = yyjson_doc_get_root(args_doc);
    const char *requested = json_string(args, "path");
    const char *policy_name = json_string(args, "policy");
    bool policy_valid = false;
    merge_policy_t policy = parse_policy(policy_name, &policy_valid);
    char path[MEM_SHARE_PATH_CAP];
    if (!policy_valid) {
        yyjson_doc_free(args_doc);
        return share_json_result(false, "invalid merge policy", NULL, -1, 0, 0, 0, 0);
    }
    if (requested) {
        int n = snprintf(path, sizeof(path), "%s", requested);
        if (n < 0 || (size_t)n >= sizeof(path)) {
            yyjson_doc_free(args_doc);
            return share_json_result(false, "import path is too long", NULL, -1, 0, 0, 0, 0);
        }
    } else if (!default_export_path(memory, path, sizeof(path))) {
        yyjson_doc_free(args_doc);
        return share_json_result(false, "cannot construct import path", NULL, -1, 0, 0, 0, 0);
    }
    yyjson_doc_free(args_doc);

    size_t bundle_len = 0;
    unsigned char *bundle = read_file(path, MEM_SHARE_BUNDLE_MAX, &bundle_len);
    if (!bundle) {
        return share_json_result(false, "cannot read import bundle", path, -1, 0, 0, 0, 0);
    }
    char bundle_hash[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(bundle, bundle_len, bundle_hash);
    yyjson_doc *bundle_doc = yyjson_read((const char *)bundle, bundle_len, 0);
    free(bundle);
    if (!bundle_doc || !yyjson_is_obj(yyjson_doc_get_root(bundle_doc))) {
        if (bundle_doc) {
            yyjson_doc_free(bundle_doc);
        }
        return share_json_result(false, "invalid import bundle JSON", path, -1, 0, 0, 0, 0);
    }
    yyjson_val *root = yyjson_doc_get_root(bundle_doc);
    const char *error = NULL;
    if (!bundle_header_valid(root, &error) || !validate_raw_bundle(root, &error)) {
        yyjson_doc_free(bundle_doc);
        return share_json_result(false, error, path, -1, 0, 0, 0, 0);
    }

    int added = 0;
    int skipped = 0;
    int updated = 0;
    if (!install_raw_bundle(memory, root, &added, &skipped, &error)) {
        yyjson_doc_free(bundle_doc);
        return share_json_result(false, error, path, -1, added, skipped, updated, 0);
    }

    sqlite3 *db = cbm_memory_db(memory);
    conflict_list_t conflicts = {0};
    blocked_list_t blocked = {0};
    if (!db || sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) != SQLITE_OK ||
        sqlite3_exec(db, "PRAGMA defer_foreign_keys=ON;", NULL, NULL, NULL) != SQLITE_OK) {
        yyjson_doc_free(bundle_doc);
        return share_json_result(false, "cannot begin memory import", path, -1, added, skipped,
                                 updated, 0);
    }
    int db_added = 0;
    int db_skipped = 0;
    int db_updated = 0;
    int import_rc = 0;
    for (size_t i = 0; i < MEMORY_TABLE_COUNT && import_rc == 0; i++) {
        import_rc = import_table(db, root, &MEMORY_TABLES[i], policy, &db_added, &db_skipped,
                                 &db_updated, &conflicts, &blocked);
    }
    if (import_rc == 0 &&
        sqlite3_exec(
            db,
            "INSERT INTO memory_outbox(kind,aggregate_id,revision,payload_json,state,created_at)"
            " SELECT 'wiki_materialize',page_id,current_revision,"
            " json_object('page_id',page_id,'revision',current_revision),'pending',"
            " strftime('%Y-%m-%dT%H:%M:%SZ','now') FROM memory_pages"
            " WHERE current_revision>0"
            " ON CONFLICT(kind,aggregate_id,revision) DO UPDATE SET state='pending',"
            " lease_owner=NULL,lease_until=NULL,last_error=NULL,processed_at=NULL"
            " WHERE memory_outbox.state IN ('done','failed');",
            NULL, NULL, NULL) != SQLITE_OK) {
        import_rc = -1;
        error = "failed to enqueue imported wiki materialization";
    }
    if (import_rc == 0 && !import_state_integrity_ok(db, cbm_memory_home(memory))) {
        import_rc = -1;
        error = "import would violate canonical memory integrity";
    }
    if (import_rc != 0) {
        (void)sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        yyjson_doc_free(bundle_doc);
        char *response =
            import_response(false,
                            import_rc == 1 ? "semantic conflict rejected"
                                           : (error ? error : "failed to merge import rows"),
                            path, cbm_memory_snapshot_epoch(memory), added, skipped + db_skipped,
                            updated, &conflicts, NULL, false);
        conflict_list_free(&conflicts);
        blocked_list_free(&blocked);
        return response;
    }
    bool db_changed = db_added > 0 || db_updated > 0;
    if (db_changed &&
        sqlite3_exec(db,
                     "UPDATE memory_state SET memory_epoch=memory_epoch+1,"
                     " updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE singleton=1;",
                     NULL, NULL, NULL) != SQLITE_OK) {
        (void)sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        yyjson_doc_free(bundle_doc);
        int conflict_count = conflicts.count;
        conflict_list_free(&conflicts);
        blocked_list_free(&blocked);
        return share_json_result(false, "failed to advance memory epoch", path, -1, added,
                                 skipped + db_skipped, updated, conflict_count);
    }
    if (sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) {
        (void)sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        yyjson_doc_free(bundle_doc);
        int conflict_count = conflicts.count;
        conflict_list_free(&conflicts);
        blocked_list_free(&blocked);
        return share_json_result(false, "failed to commit memory import", path, -1, added,
                                 skipped + db_skipped, updated, conflict_count);
    }
    yyjson_doc_free(bundle_doc);

    added += db_added;
    skipped += db_skipped;
    updated += db_updated;
    bool projection_ok = !db_changed || cbm_memory_rebuild_projection(memory) == 0;
    bool materialization_ok = cbm_memory_materialize_pending(memory) >= 0;
    char *proposal = persist_conflict_proposal(memory, &conflicts, bundle_hash);
    int64_t epoch = cbm_memory_snapshot_epoch(memory);
    bool derived_ok = projection_ok && materialization_ok;
    char *response = import_response(
        derived_ok,
        !projection_ok ? "projection rebuild failed"
                       : (!materialization_ok ? "wiki materialization failed" : NULL),
        path, epoch, added, skipped, updated, &conflicts, proposal, derived_ok);
    free(proposal);
    conflict_list_free(&conflicts);
    blocked_list_free(&blocked);
    return response;
}

/* ── Git bundle transport ─────────────────────────────────────── */

enum {
    MEM_SHARE_GIT_TIMEOUT_MS = 30000,
    MEM_SHARE_GIT_MAX_ARGS = 32,
};

typedef struct {
    int run_rc;
    cbm_proc_result_t process;
} share_git_result_t;

static bool resolve_git_binary(char out[MEM_SHARE_PATH_CAP]) {
#ifdef _WIN32
    (void)snprintf(out, MEM_SHARE_PATH_CAP, "git");
    return true; /* CreateProcess performs the platform executable search. */
#else
    char configured[MEM_SHARE_PATH_CAP] = "";
    if (cbm_safe_getenv("CBM_GIT_BIN", configured, sizeof(configured), NULL) &&
        configured[0] == '/' && access(configured, X_OK) == 0 &&
        cbm_canonical_path(configured, out, MEM_SHARE_PATH_CAP)) {
        return true;
    }
    char path_env[MEM_SHARE_PATH_CAP] = "";
    const char *path = cbm_safe_getenv("PATH", path_env, sizeof(path_env),
                                       "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin");
    if (!path) {
        return false;
    }
    const char *cursor = path;
    while (*cursor) {
        const char *end = strchr(cursor, ':');
        size_t dir_len = end ? (size_t)(end - cursor) : strlen(cursor);
        /* Ignore relative/empty PATH elements: resolving executables through
         * the current directory would make a repository-controlled `git`
         * executable part of the trust boundary. */
        if (dir_len > 1 && cursor[0] == '/' && dir_len + 5 < MEM_SHARE_PATH_CAP) {
            char candidate[MEM_SHARE_PATH_CAP];
            (void)snprintf(candidate, sizeof(candidate), "%.*s/git", (int)dir_len, cursor);
            struct stat st;
            if (stat(candidate, &st) == 0 && S_ISREG(st.st_mode) && access(candidate, X_OK) == 0 &&
                cbm_canonical_path(candidate, out, MEM_SHARE_PATH_CAP)) {
                return true;
            }
        }
        if (!end) {
            break;
        }
        cursor = end + 1;
    }
    return false;
#endif
}

static share_git_result_t git_run(const char *workdir, const char *const *args) {
    char git_binary[MEM_SHARE_PATH_CAP];
    if (!resolve_git_binary(git_binary)) {
        share_git_result_t missing = {
            .run_rc = -1,
            .process = {.outcome = CBM_PROC_SPAWN_FAILED, .exit_code = -1, .term_signal = 0},
        };
        return missing;
    }
    const char *argv[MEM_SHARE_GIT_MAX_ARGS];
    int count = 0;
    argv[count++] = git_binary;
    argv[count++] = "-c";
    argv[count++] = "credential.interactive=never";
    argv[count++] = "-c";
    argv[count++] = "core.askPass=";
    argv[count++] = "-c";
    argv[count++] = "core.sshCommand=ssh -oBatchMode=yes";
    argv[count++] = "-C";
    argv[count++] = workdir;
    for (int i = 0; args && args[i]; i++) {
        if (count + 1 >= MEM_SHARE_GIT_MAX_ARGS) {
            share_git_result_t overflow = {
                .run_rc = -1,
                .process = {.outcome = CBM_PROC_SPAWN_FAILED, .exit_code = -1, .term_signal = 0},
            };
            return overflow;
        }
        argv[count++] = args[i];
    }
    argv[count] = NULL;
    cbm_proc_opts_t options = {
        .bin = git_binary,
        .argv = argv,
        .log_file = NULL,
        .on_log_line = NULL,
        .log_ud = NULL,
        .quiet_timeout_ms = MEM_SHARE_GIT_TIMEOUT_MS,
        .delete_log_on_exit = false,
    };
    share_git_result_t result = {0};
    result.process.outcome = CBM_PROC_SPAWN_FAILED;
    result.process.exit_code = -1;
    result.run_rc = cbm_subprocess_run(&options, &result.process);
    return result;
}

static bool git_ok(share_git_result_t result) {
    return result.run_rc == 0 && result.process.outcome == CBM_PROC_CLEAN &&
           result.process.exit_code == 0;
}

static bool git_ref_valid(const char *value, bool remote_name) {
    if (!value || !value[0] || value[0] == '-' || value[0] == '/' || strstr(value, "..") ||
        strstr(value, "@{") || strchr(value, '\\') || strchr(value, ' ')) {
        return false;
    }
    for (const char *p = value; *p; p++) {
        if (!isalnum((unsigned char)*p) && *p != '_' && *p != '-' && *p != '.' &&
            (!remote_name && *p != '/')) {
            return false;
        }
    }
    size_t len = strlen(value);
    return value[len - 1] != '.' && value[len - 1] != '/';
}

static bool remote_url_has_credentials(const char *url) {
    const char *scheme = strstr(url, "://");
    if (!scheme) {
        const char *at = strchr(url, '@');
        if (!at) {
            return false;
        }
        /* scp-style user@host:path is allowed; password/userinfo encoding is not. */
        for (const char *p = url; p < at; p++) {
            if (*p == ':' || *p == '%') {
                return true;
            }
        }
        return false;
    }
    const char *authority = scheme + 3;
    const char *slash = strchr(authority, '/');
    const char *at = strchr(authority, '@');
    if (!at || (slash && at > slash)) {
        return false;
    }
    if (strncmp(url, "ssh://", 6) != 0) {
        return true;
    }
    /* ssh://user@host is valid; ssh://user:password@host and percent-encoded
     * userinfo are not. */
    for (const char *p = authority; p < at; p++) {
        if (*p == ':' || *p == '%') {
            return true;
        }
    }
    return false;
}

static bool remote_url_safe(const char *url, bool allow_local) {
    if (!url || !url[0] || url[0] == '-' || !cbm_validate_shell_arg(url) ||
        remote_url_has_credentials(url) || strchr(url, '?') || strchr(url, '#')) {
        return false;
    }
    for (const char *p = url; *p; p++) {
        if ((unsigned char)*p < 0x20 || *p == 0x7f) {
            return false;
        }
    }
    if (strncmp(url, "https://", 8) == 0 || strncmp(url, "ssh://", 6) == 0) {
        return true;
    }
    const char *colon = strchr(url, ':');
    const char *slash = strchr(url, '/');
    if (colon && (!slash || colon < slash) && strchr(url, '@') && colon[1]) {
        return true; /* git@github.com:owner/repository.git */
    }
    if (!allow_local) {
        return false;
    }
#ifdef _WIN32
    if (isalpha((unsigned char)url[0]) && url[1] == ':' && (url[2] == '/' || url[2] == '\\')) {
        return true;
    }
#else
    if (url[0] == '/') {
        return true;
    }
#endif
    return strncmp(url, "file://", 7) == 0;
}

static bool sync_paths(cbm_memory_t *memory, char sync_dir[MEM_SHARE_PATH_CAP],
                       char bundle_path[MEM_SHARE_PATH_CAP]) {
    return path_join(sync_dir, MEM_SHARE_PATH_CAP, cbm_memory_home(memory), "sync") &&
           path_join(bundle_path, MEM_SHARE_PATH_CAP, sync_dir, CBM_MEMORY_EXPORT_FILENAME);
}

static bool git_repository_exists(const char *sync_dir) {
    const char *args[] = {"rev-parse", "--is-inside-work-tree", NULL};
    return git_ok(git_run(sync_dir, args));
}

static bool git_head_exists(const char *sync_dir) {
    const char *args[] = {"rev-parse", "--verify", "HEAD", NULL};
    return git_ok(git_run(sync_dir, args));
}

static bool git_remote_exists(const char *sync_dir, const char *remote_name) {
    const char *args[] = {"remote", "get-url", remote_name, NULL};
    return git_ok(git_run(sync_dir, args));
}

static bool ensure_sync_repository(const char *sync_dir, const char *branch,
                                   share_git_result_t *last_result) {
    if (!cbm_mkdir_p(sync_dir, MEM_SHARE_DIR_MODE)) {
        return false;
    }
#ifndef _WIN32
    (void)chmod(sync_dir, MEM_SHARE_DIR_MODE);
#endif
    if (!git_repository_exists(sync_dir)) {
        const char *init_args[] = {"init", NULL};
        *last_result = git_run(sync_dir, init_args);
        if (!git_ok(*last_result)) {
            return false;
        }
        const char *branch_args[] = {"checkout", "-B", branch, NULL};
        *last_result = git_run(sync_dir, branch_args);
        if (!git_ok(*last_result)) {
            return false;
        }
    }
    const char *name_args[] = {"config", "user.name", "codebase-memory-mcp", NULL};
    *last_result = git_run(sync_dir, name_args);
    if (!git_ok(*last_result)) {
        return false;
    }
    const char *email_args[] = {"config", "user.email", "memory@localhost", NULL};
    *last_result = git_run(sync_dir, email_args);
    return git_ok(*last_result);
}

static char *call_export_path(cbm_memory_t *memory, const char *path) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "path", path);
    char *args = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    if (!args) {
        return NULL;
    }
    char *result = cbm_memory_export_json(memory, args);
    free(args);
    return result;
}

static char *call_import_path(cbm_memory_t *memory, const char *path, const char *policy) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "path", path);
    yyjson_mut_obj_add_strcpy(doc, root, "policy", policy ? policy : "reject");
    char *args = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    if (!args) {
        return NULL;
    }
    char *result = cbm_memory_import_json(memory, args);
    free(args);
    return result;
}

static bool json_result_ok(const char *json) {
    if (!json) {
        return false;
    }
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) {
        return false;
    }
    yyjson_val *ok = yyjson_obj_get(yyjson_doc_get_root(doc), "ok");
    bool result = ok && yyjson_is_bool(ok) && yyjson_get_bool(ok);
    yyjson_doc_free(doc);
    return result;
}

/* Export canonical memory, stage it, and create a local transport commit only
 * when the bundle changed. */
static bool stage_bundle_commit(cbm_memory_t *memory, const char *sync_dir, const char *bundle_path,
                                share_git_result_t *last_result, bool *committed,
                                char **export_result) {
    *committed = false;
    *export_result = call_export_path(memory, bundle_path);
    if (!json_result_ok(*export_result)) {
        return false;
    }
    const char *add_args[] = {"add", "--", CBM_MEMORY_EXPORT_FILENAME, NULL};
    *last_result = git_run(sync_dir, add_args);
    if (!git_ok(*last_result)) {
        return false;
    }
    const char *quiet_args[] = {"diff", "--cached", "--quiet", "--", CBM_MEMORY_EXPORT_FILENAME,
                                NULL};
    *last_result = git_run(sync_dir, quiet_args);
    if (git_ok(*last_result)) {
        return true;
    }
    if (last_result->process.outcome != CBM_PROC_EXIT_NONZERO ||
        last_result->process.exit_code != 1) {
        return false;
    }
    const char *commit_args[] = {
        "commit", "-m", "Update Global Memory bundle", "--", CBM_MEMORY_EXPORT_FILENAME, NULL};
    *last_result = git_run(sync_dir, commit_args);
    *committed = git_ok(*last_result);
    return *committed;
}

static bool configure_remote(const char *sync_dir, const char *remote_name, const char *url,
                             share_git_result_t *last_result) {
    if (git_remote_exists(sync_dir, remote_name)) {
        const char *set_args[] = {"remote", "set-url", remote_name, url, NULL};
        *last_result = git_run(sync_dir, set_args);
    } else {
        const char *add_args[] = {"remote", "add", remote_name, url, NULL};
        *last_result = git_run(sync_dir, add_args);
    }
    return git_ok(*last_result);
}

static char *sync_response(bool ok, const char *action, const char *error, const char *sync_dir,
                           bool initialized, bool head, bool remote, bool committed,
                           share_git_result_t process, const char *nested_key,
                           const char *nested_json) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_bool(doc, root, "ok", ok);
    yyjson_mut_obj_add_strcpy(doc, root, "action", action ? action : "");
    if (error) {
        yyjson_mut_obj_add_strcpy(doc, root, "error", error);
    }
    yyjson_mut_obj_add_strcpy(doc, root, "sync_dir", sync_dir ? sync_dir : "");
    yyjson_mut_obj_add_bool(doc, root, "initialized", initialized);
    yyjson_mut_obj_add_bool(doc, root, "head_exists", head);
    yyjson_mut_obj_add_bool(doc, root, "remote_configured", remote);
    yyjson_mut_obj_add_bool(doc, root, "committed", committed);
    yyjson_mut_obj_add_strcpy(doc, root, "process_outcome",
                              cbm_proc_outcome_str(process.process.outcome));
    yyjson_mut_obj_add_int(doc, root, "exit_code", process.process.exit_code);
    yyjson_mut_obj_add_int(doc, root, "term_signal", process.process.term_signal);
    if (nested_key && nested_json) {
        yyjson_doc *nested_doc = yyjson_read(nested_json, strlen(nested_json), 0);
        if (nested_doc) {
            yyjson_mut_val *nested = yyjson_val_mut_copy(doc, yyjson_doc_get_root(nested_doc));
            if (nested) {
                yyjson_mut_obj_add_val(doc, root, nested_key, nested);
            }
            yyjson_doc_free(nested_doc);
        }
    }
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

char *cbm_memory_sync_json(cbm_memory_t *memory, const char *args_json) {
    share_git_result_t last = {
        .run_rc = 0,
        .process = {.outcome = CBM_PROC_CLEAN, .exit_code = 0, .term_signal = 0},
    };
    if (!memory) {
        return sync_response(false, "", "memory handle is required", "", false, false, false, false,
                             last, NULL, NULL);
    }
    yyjson_doc *args_doc = parse_args(args_json);
    if (!args_doc) {
        return sync_response(false, "", "invalid JSON arguments", cbm_memory_home(memory), false,
                             false, false, false, last, NULL, NULL);
    }
    yyjson_val *args = yyjson_doc_get_root(args_doc);
    const char *action_arg = json_string(args, "action");
    const char *remote_url = json_string(args, "remote");
    const char *remote_name = json_string(args, "remote_name");
    const char *branch = json_string(args, "branch");
    const char *policy = json_string(args, "policy");
    bool allow_local = json_bool(args, "allow_local_remote", false);
    char action_storage[MEM_SHARE_NAME_CAP];
    const char *action_value = action_arg ? action_arg : "status";
    int action_len = snprintf(action_storage, sizeof(action_storage), "%s", action_value);
    if (action_len < 0 || (size_t)action_len >= sizeof(action_storage)) {
        yyjson_doc_free(args_doc);
        return sync_response(false, "", "sync action is too long", cbm_memory_home(memory), false,
                             false, false, false, last, NULL, NULL);
    }
    /* Responses are commonly built after args_doc is released.  Own the
     * action string locally instead of retaining a yyjson-backed pointer. */
    const char *action = action_storage;
    remote_name = remote_name ? remote_name : "origin";
    branch = branch ? branch : "cbm-memory";
    policy = policy ? policy : "reject";
    bool policy_valid = false;
    (void)parse_policy(policy, &policy_valid);

    char sync_dir[MEM_SHARE_PATH_CAP];
    char bundle_path[MEM_SHARE_PATH_CAP];
    if (!sync_paths(memory, sync_dir, bundle_path) || !git_ref_valid(remote_name, true) ||
        !git_ref_valid(branch, false) || !policy_valid) {
        yyjson_doc_free(args_doc);
        return sync_response(false, action, "invalid sync path, ref, or merge policy", "", false,
                             false, false, false, last, NULL, NULL);
    }
    if (remote_url && !remote_url_safe(remote_url, allow_local)) {
        yyjson_doc_free(args_doc);
        return sync_response(false, action, "unsafe or unsupported remote URL", sync_dir, false,
                             false, false, false, last, NULL, NULL);
    }

    if (strcmp(action, "status") == 0) {
        const char *probe_args[] = {"rev-parse", "--is-inside-work-tree", NULL};
        last = git_run(sync_dir, probe_args);
        bool initialized = git_ok(last);
        bool head = initialized && git_head_exists(sync_dir);
        bool remote = initialized && git_remote_exists(sync_dir, remote_name);
        yyjson_doc_free(args_doc);
        return sync_response(true, action, NULL, sync_dir, initialized, head, remote, false, last,
                             NULL, NULL);
    }

    if (strcmp(action, "init") != 0 && strcmp(action, "configure_remote") != 0 &&
        strcmp(action, "pull") != 0 && strcmp(action, "push") != 0) {
        yyjson_doc_free(args_doc);
        return sync_response(false, action, "unsupported sync action", sync_dir, false, false,
                             false, false, last, NULL, NULL);
    }

    if (!ensure_sync_repository(sync_dir, branch, &last)) {
        yyjson_doc_free(args_doc);
        return sync_response(false, action, "failed to initialize sync repository", sync_dir,
                             git_repository_exists(sync_dir), false, false, false, last, NULL,
                             NULL);
    }

    if (remote_url && !configure_remote(sync_dir, remote_name, remote_url, &last)) {
        yyjson_doc_free(args_doc);
        return sync_response(false, action, "failed to configure remote", sync_dir, true,
                             git_head_exists(sync_dir), false, false, last, NULL, NULL);
    }
    bool remote_configured = git_remote_exists(sync_dir, remote_name);

    if (strcmp(action, "configure_remote") == 0) {
        yyjson_doc_free(args_doc);
        if (!remote_url) {
            return sync_response(false, action, "remote URL is required", sync_dir, true,
                                 git_head_exists(sync_dir), remote_configured, false, last, NULL,
                                 NULL);
        }
        return sync_response(true, action, NULL, sync_dir, true, git_head_exists(sync_dir),
                             remote_configured, false, last, NULL, NULL);
    }

    char *export_result = NULL;
    bool committed = false;
    if (!stage_bundle_commit(memory, sync_dir, bundle_path, &last, &committed, &export_result)) {
        yyjson_doc_free(args_doc);
        char *response = sync_response(false, action, "failed to stage local memory bundle",
                                       sync_dir, true, git_head_exists(sync_dir), remote_configured,
                                       committed, last, "export", export_result);
        free(export_result);
        return response;
    }

    if (strcmp(action, "init") == 0) {
        yyjson_doc_free(args_doc);
        char *response =
            sync_response(true, action, NULL, sync_dir, true, git_head_exists(sync_dir),
                          remote_configured, committed, last, "export", export_result);
        free(export_result);
        return response;
    }

    if (!remote_configured) {
        yyjson_doc_free(args_doc);
        char *response = sync_response(false, action, "no Git remote is configured", sync_dir, true,
                                       git_head_exists(sync_dir), false, committed, last, "export",
                                       export_result);
        free(export_result);
        return response;
    }

    if (strcmp(action, "push") == 0) {
        const char *push_args[] = {"push", "--set-upstream", remote_name, branch, NULL};
        last = git_run(sync_dir, push_args);
        yyjson_doc_free(args_doc);
        char *response = sync_response(
            git_ok(last), action, git_ok(last) ? NULL : "Git push failed", sync_dir, true,
            git_head_exists(sync_dir), true, committed, last, "export", export_result);
        free(export_result);
        return response;
    }

    /* Pull fetches a remote bundle into a detached temporary worktree.  The
     * live sync worktree and DB are untouched until logical import succeeds. */
    char remote_ref[MEM_SHARE_PATH_CAP];
    char fetch_refspec[MEM_SHARE_PATH_CAP];
    int rr = snprintf(remote_ref, sizeof(remote_ref), "refs/remotes/%s/%s", remote_name, branch);
    int fr = snprintf(fetch_refspec, sizeof(fetch_refspec), "%s:%s", branch, remote_ref);
    if (rr < 0 || fr < 0 || (size_t)rr >= sizeof(remote_ref) ||
        (size_t)fr >= sizeof(fetch_refspec)) {
        yyjson_doc_free(args_doc);
        free(export_result);
        return sync_response(false, action, "remote ref is too long", sync_dir, true, true, true,
                             committed, last, NULL, NULL);
    }
    const char *fetch_args[] = {"fetch", "--no-tags", remote_name, fetch_refspec, NULL};
    last = git_run(sync_dir, fetch_args);
    if (!git_ok(last)) {
        yyjson_doc_free(args_doc);
        char *response = sync_response(false, action, "Git fetch failed", sync_dir, true,
                                       git_head_exists(sync_dir), true, committed, last, "export",
                                       export_result);
        free(export_result);
        return response;
    }

    char pull_dir[MEM_SHARE_PATH_CAP];
    int pd =
        snprintf(pull_dir, sizeof(pull_dir), "%s/export/sync-pull.%ld.%llu",
                 cbm_memory_home(memory), (long)share_getpid(), (unsigned long long)cbm_now_ns());
    if (pd < 0 || (size_t)pd >= sizeof(pull_dir)) {
        yyjson_doc_free(args_doc);
        free(export_result);
        return sync_response(false, action, "temporary pull path is too long", sync_dir, true, true,
                             true, committed, last, NULL, NULL);
    }
    const char *worktree_args[] = {"worktree", "add", "--detach", pull_dir, remote_ref, NULL};
    last = git_run(sync_dir, worktree_args);
    if (!git_ok(last)) {
        yyjson_doc_free(args_doc);
        char *response = sync_response(false, action, "cannot create remote bundle worktree",
                                       sync_dir, true, git_head_exists(sync_dir), true, committed,
                                       last, "export", export_result);
        free(export_result);
        return response;
    }
    char remote_bundle[MEM_SHARE_PATH_CAP];
    bool remote_path_ok =
        path_join(remote_bundle, sizeof(remote_bundle), pull_dir, CBM_MEMORY_EXPORT_FILENAME);
    char *import_result = remote_path_ok ? call_import_path(memory, remote_bundle, policy) : NULL;
    bool import_ok = json_result_ok(import_result);
    const char *remove_args[] = {"worktree", "remove", "--force", pull_dir, NULL};
    share_git_result_t remove_result = git_run(sync_dir, remove_args);
    if (!git_ok(remove_result) && import_ok) {
        import_ok = false;
    }
    if (!import_ok) {
        yyjson_doc_free(args_doc);
        char *response = sync_response(
            false, action, "remote bundle import failed", sync_dir, true, git_head_exists(sync_dir),
            true, committed, git_ok(remove_result) ? last : remove_result, "import", import_result);
        free(import_result);
        free(export_result);
        return response;
    }

    /* Record remote ancestry without ever Git-merging bundle contents.  The
     * ours strategy joins histories; the logical importer already merged data. */
    if (git_head_exists(sync_dir)) {
        const char *merge_args[] = {
            "merge", "-s", "ours", "--no-edit", "--allow-unrelated-histories", remote_ref, NULL};
        last = git_run(sync_dir, merge_args);
    } else {
        const char *checkout_args[] = {"checkout", "-B", branch, remote_ref, NULL};
        last = git_run(sync_dir, checkout_args);
    }
    if (!git_ok(last)) {
        yyjson_doc_free(args_doc);
        char *response = sync_response(false, action, "failed to join remote history", sync_dir,
                                       true, git_head_exists(sync_dir), true, committed, last,
                                       "import", import_result);
        free(import_result);
        free(export_result);
        return response;
    }

    free(export_result);
    export_result = NULL;
    bool merged_commit = false;
    if (!stage_bundle_commit(memory, sync_dir, bundle_path, &last, &merged_commit,
                             &export_result)) {
        yyjson_doc_free(args_doc);
        char *response = sync_response(false, action, "failed to commit merged bundle", sync_dir,
                                       true, true, true, committed, last, "import", import_result);
        free(import_result);
        free(export_result);
        return response;
    }
    committed = committed || merged_commit;
    yyjson_doc_free(args_doc);
    char *response = sync_response(true, action, NULL, sync_dir, true, true, true, committed, last,
                                   "import", import_result);
    free(import_result);
    free(export_result);
    return response;
}
