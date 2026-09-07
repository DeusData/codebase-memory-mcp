/*
 * atlas_api.c — CBM Atlas data services: the Modules tree and the Symbol
 * bundle. Pure JSON producers over an open store (handlers stay in
 * http_server.c; tests call these directly).
 */
#include "foundation/constants.h"
#include "ui/atlas.h"
#include "foundation/hash_table.h"

#include <sqlite3.h>
#include <yyjson/yyjson.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Small helpers ────────────────────────────────────────────── */

static char *aa_strdup(const char *s) {
    if (!s)
        return NULL;
    size_t len = strlen(s) + 1;
    char *copy = malloc(len);
    if (copy)
        memcpy(copy, s, len);
    return copy;
}

/* Normalize a folder path: "" and "." mean the root; strip trailing '/'. */
static void aa_norm_path(const char *in, char *out, size_t cap) {
    if (!in || !in[0] || strcmp(in, ".") == 0) {
        out[0] = '\0';
        return;
    }
    snprintf(out, cap, "%s", in);
    size_t len = strlen(out);
    while (len > 0 && out[len - 1] == '/')
        out[--len] = '\0';
}

/* The next path component of `full` under folder `prefix` ("" = root).
 * Returns false when `full` is not under `prefix`. `is_direct_file` is true
 * when `full` names a file directly inside `prefix` (no further '/'). */
static bool aa_child_component(const char *full, const char *prefix, char *comp, size_t cap,
                               bool *is_direct_file) {
    size_t plen = strlen(prefix);
    const char *rest = full;
    if (plen > 0) {
        if (strncmp(full, prefix, plen) != 0 || full[plen] != '/')
            return false;
        rest = full + plen + 1;
    }
    if (!rest[0])
        return false;
    const char *slash = strchr(rest, '/');
    size_t clen = slash ? (size_t)(slash - rest) : strlen(rest);
    if (clen == 0 || clen >= cap)
        return false;
    memcpy(comp, rest, clen);
    comp[clen] = '\0';
    *is_direct_file = slash == NULL;
    return true;
}

/* ── /api/tree ────────────────────────────────────────────────── */

typedef struct {
    char *name;
    long long symbols;
    int files;
    int missed;
    int region;      /* dominant region id (majority by file), -1 unknown */
    int region_best; /* votes for the dominant region so far */
    bool is_file;
} aa_tree_child_t;

enum { AA_TREE_MAX_CHILDREN = 4096 };

typedef struct {
    aa_tree_child_t *items;
    int count;
    int cap;
    CBMHashTable *by_name; /* name → index+1 (keys borrowed from items) */
    int dropped;           /* children beyond the cap (reported, not silent) */
} aa_children_t;

static aa_tree_child_t *aa_child(aa_children_t *set, const char *name, bool is_file) {
    /* Files and folders can share a name; disambiguate the map key. */
    char key[CBM_SZ_512];
    snprintf(key, sizeof(key), "%c%s", is_file ? 'f' : 'd', name);
    void *slot = cbm_ht_get(set->by_name, key);
    if (slot)
        return &set->items[(int)(intptr_t)slot - 1];
    if (set->count >= AA_TREE_MAX_CHILDREN) {
        set->dropped++;
        return NULL;
    }
    if (set->count >= set->cap) {
        int nc = set->cap ? set->cap * 2 : 64;
        aa_tree_child_t *grown = realloc(set->items, (size_t)nc * sizeof(aa_tree_child_t));
        if (!grown)
            return NULL;
        /* The by_name table stores indices, so growth is safe. */
        set->items = grown;
        set->cap = nc;
    }
    aa_tree_child_t *child = &set->items[set->count];
    memset(child, 0, sizeof(*child));
    child->name = aa_strdup(name);
    child->region = -1;
    child->is_file = is_file;
    if (!child->name)
        return NULL;
    set->count++;
    char *owned_key = aa_strdup(key);
    if (owned_key)
        cbm_ht_set(set->by_name, owned_key, (void *)(intptr_t)set->count);
    return child;
}

static void aa_collect_keys(const char *key, void *value, void *userdata) {
    (void)value;
    /* Keys were heap copies owned by us; collect for freeing. */
    struct {
        char **keys;
        int count;
        int cap;
    } *box = userdata;
    if (box->count >= box->cap)
        return;
    box->keys[box->count++] = (char *)key;
}

static void aa_children_free(aa_children_t *set) {
    struct {
        char **keys;
        int count;
        int cap;
    } box;
    box.cap = set->count * 2 + 8;
    box.keys = malloc((size_t)box.cap * sizeof(char *));
    box.count = 0;
    if (set->by_name && box.keys)
        cbm_ht_foreach(set->by_name, aa_collect_keys, &box);
    for (int i = 0; i < box.count; i++)
        free(box.keys[i]);
    free(box.keys);
    cbm_ht_free(set->by_name);
    for (int i = 0; i < set->count; i++)
        free(set->items[i].name);
    free(set->items);
    memset(set, 0, sizeof(*set));
}

static int aa_child_cmp(const void *a, const void *b) {
    const aa_tree_child_t *ca = a, *cb = b;
    if (ca->is_file != cb->is_file)
        return ca->is_file - cb->is_file; /* folders first */
    if (cb->symbols != ca->symbols)
        return cb->symbols > ca->symbols ? 1 : -1;
    return strcmp(ca->name, cb->name);
}

char *cbm_atlas_tree_json(cbm_store_t *store, const char *project, const char *path) {
    if (!store || !project)
        return NULL;
    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db)
        return NULL;
    char prefix[CBM_SZ_512];
    aa_norm_path(path, prefix, sizeof(prefix));

    aa_children_t set;
    memset(&set, 0, sizeof(set));
    set.by_name = cbm_ht_create(256);
    if (!set.by_name)
        return NULL;

    long long total_symbols = 0;
    int total_files = 0;

    /* One pass: per-file symbol counts, folded into direct children. */
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
                           "SELECT file_path, COUNT(*) FROM nodes WHERE project=?1 AND "
                           "file_path IS NOT NULL AND file_path != '' GROUP BY file_path",
                           -1, &st, NULL) != SQLITE_OK) {
        aa_children_free(&set);
        return NULL;
    }
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *file = (const char *)sqlite3_column_text(st, 0);
        long long symbols = sqlite3_column_int64(st, 1);
        char comp[CBM_SZ_256];
        bool direct = false;
        if (!file || !aa_child_component(file, prefix, comp, sizeof(comp), &direct))
            continue;
        total_symbols += symbols;
        total_files++;
        aa_tree_child_t *child = aa_child(&set, comp, direct);
        if (!child)
            continue;
        child->symbols += symbols;
        child->files++;
        int region = cbm_layout_region_for_file(store, project, file, NULL);
        if (region >= 0) {
            /* Majority vote, Boyer-Moore style: cheap and order-stable
             * enough for a display hint. */
            if (child->region == region)
                child->region_best++;
            else if (child->region_best == 0) {
                child->region = region;
                child->region_best = 1;
            } else {
                child->region_best--;
            }
        }
    }
    sqlite3_finalize(st);

    /* Missed coverage: File nodes of the shadow project under each child. */
    char shadow[CBM_SZ_512];
    cbm_store_coverage_shadow_project(shadow, sizeof(shadow), project);
    if (sqlite3_prepare_v2(db,
                           "SELECT file_path FROM nodes WHERE project=?1 AND label='File' AND "
                           "file_path IS NOT NULL",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, shadow, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *file = (const char *)sqlite3_column_text(st, 0);
            char comp[CBM_SZ_256];
            bool direct = false;
            if (!file || !aa_child_component(file, prefix, comp, sizeof(comp), &direct))
                continue;
            /* Only annotate children that exist in the code graph. */
            char key[CBM_SZ_512];
            snprintf(key, sizeof(key), "%c%s", direct ? 'f' : 'd', comp);
            void *slot = cbm_ht_get(set.by_name, key);
            if (slot)
                set.items[(int)(intptr_t)slot - 1].missed++;
        }
        sqlite3_finalize(st);
    }

    qsort(set.items, (size_t)set.count, sizeof(aa_tree_child_t), aa_child_cmp);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "path", prefix);
    yyjson_mut_obj_add_int(doc, root, "files", total_files);
    yyjson_mut_obj_add_int(doc, root, "symbols", total_symbols);
    if (set.dropped > 0)
        yyjson_mut_obj_add_int(doc, root, "children_dropped", set.dropped);
    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int i = 0; i < set.count; i++) {
        const aa_tree_child_t *child = &set.items[i];
        yyjson_mut_val *obj = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, obj, "name", child->name);
        char full[CBM_SZ_512];
        if (prefix[0])
            snprintf(full, sizeof(full), "%s/%s", prefix, child->name);
        else
            snprintf(full, sizeof(full), "%s", child->name);
        yyjson_mut_obj_add_strcpy(doc, obj, "path", full);
        yyjson_mut_obj_add_str(doc, obj, "kind", child->is_file ? "file" : "dir");
        yyjson_mut_obj_add_int(doc, obj, "symbols", child->symbols);
        yyjson_mut_obj_add_int(doc, obj, "files", child->files);
        if (child->missed > 0)
            yyjson_mut_obj_add_int(doc, obj, "missed", child->missed);
        /* Region id only — the client already holds region names from the
         * regions payload. */
        if (child->region >= 0)
            yyjson_mut_obj_add_int(doc, obj, "region", child->region);
        yyjson_mut_arr_append(arr, obj);
    }
    yyjson_mut_obj_add_val(doc, root, "children", arr);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    aa_children_free(&set);
    return json;
}

/* ── /api/symbol ──────────────────────────────────────────────── */

/* Call-ish edge families shown on the symbol page, most certain first. */
#define AA_CALLISH_TYPES "'CALLS','CALL_REFERENCE','USAGE','HTTP_CALLS','ASYNC_CALLS'"

static void aa_add_conn_rows(yyjson_mut_doc *doc, yyjson_mut_val *parent, sqlite3_stmt *st,
                             cbm_store_t *store, const char *project) {
    yyjson_mut_val *items = yyjson_mut_arr(doc);
    while (sqlite3_step(st) == SQLITE_ROW) {
        yyjson_mut_val *row = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, row, "type", (const char *)sqlite3_column_text(st, 0));
        yyjson_mut_obj_add_int(doc, row, "id", sqlite3_column_int64(st, 1));
        yyjson_mut_obj_add_strcpy(doc, row, "label", (const char *)sqlite3_column_text(st, 2));
        yyjson_mut_obj_add_strcpy(doc, row, "name", (const char *)sqlite3_column_text(st, 3));
        const char *file = (const char *)sqlite3_column_text(st, 4);
        if (file)
            yyjson_mut_obj_add_strcpy(doc, row, "file_path", file);
        int line = sqlite3_column_int(st, 5);
        if (line > 0)
            yyjson_mut_obj_add_int(doc, row, "start_line", line);
        const char *qn = (const char *)sqlite3_column_text(st, 6);
        if (qn)
            yyjson_mut_obj_add_strcpy(doc, row, "qualified_name", qn);
        if (file && store) {
            int region = cbm_layout_region_for_file(store, project, file, NULL);
            if (region >= 0)
                yyjson_mut_obj_add_int(doc, row, "region", region);
        }
        yyjson_mut_arr_append(items, row);
    }
    yyjson_mut_obj_add_val(doc, parent, "items", items);
}

/* callers (inbound=true) or callees of `id`, with totals and pagination. */
static void aa_add_connections(yyjson_mut_doc *doc, yyjson_mut_val *root, struct sqlite3 *db,
                               cbm_store_t *store, const char *project, int64_t id, bool inbound,
                               int limit, int offset) {
    yyjson_mut_val *section = yyjson_mut_obj(doc);
    const char *count_sql =
        inbound ? "SELECT e.type, COUNT(*) FROM edges e WHERE e.project=?1 AND e.target_id=?2 "
                  "AND e.type IN (" AA_CALLISH_TYPES ") GROUP BY e.type"
                : "SELECT e.type, COUNT(*) FROM edges e WHERE e.project=?1 AND e.source_id=?2 "
                  "AND e.type IN (" AA_CALLISH_TYPES ") GROUP BY e.type";
    sqlite3_stmt *st = NULL;
    long long total = 0;
    yyjson_mut_val *by_type = yyjson_mut_obj(doc);
    if (sqlite3_prepare_v2(db, count_sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 2, id);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *type = (const char *)sqlite3_column_text(st, 0);
            long long count = sqlite3_column_int64(st, 1);
            /* Copy the key: yyjson borrows add_*() keys, and this one dies
             * with the statement. */
            if (type)
                yyjson_mut_obj_add(by_type, yyjson_mut_strcpy(doc, type),
                                   yyjson_mut_sint(doc, count));
            total += count;
        }
        sqlite3_finalize(st);
    }
    yyjson_mut_obj_add_int(doc, section, "total", total);
    yyjson_mut_obj_add_val(doc, section, "by_type", by_type);
    yyjson_mut_obj_add_int(doc, section, "limit", limit);
    yyjson_mut_obj_add_int(doc, section, "offset", offset);

    const char *rows_sql =
        inbound ? "SELECT e.type, n.id, n.label, n.name, n.file_path, n.start_line, "
                  "n.qualified_name FROM edges e JOIN nodes n ON n.project=e.project AND "
                  "n.id=e.source_id WHERE e.project=?1 AND e.target_id=?2 AND e.type IN "
                  "(" AA_CALLISH_TYPES ") ORDER BY CASE e.type WHEN 'CALLS' THEN 0 WHEN "
                  "'CALL_REFERENCE' THEN 1 ELSE 2 END, n.id LIMIT ?3 OFFSET ?4"
                : "SELECT e.type, n.id, n.label, n.name, n.file_path, n.start_line, "
                  "n.qualified_name FROM edges e JOIN nodes n ON n.project=e.project AND "
                  "n.id=e.target_id WHERE e.project=?1 AND e.source_id=?2 AND e.type IN "
                  "(" AA_CALLISH_TYPES ") ORDER BY CASE e.type WHEN 'CALLS' THEN 0 WHEN "
                  "'CALL_REFERENCE' THEN 1 ELSE 2 END, n.id LIMIT ?3 OFFSET ?4";
    if (sqlite3_prepare_v2(db, rows_sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 2, id);
        sqlite3_bind_int(st, 3, limit);
        sqlite3_bind_int(st, 4, offset);
        aa_add_conn_rows(doc, section, st, store, project);
        sqlite3_finalize(st);
    }

    /* The shape of what the page cut, not just its size: the hidden
     * remainder grouped by file (top 10 files). A bare "+N more" hides the
     * answer to "who calls this" on every high-degree node. */
    if (total > offset + limit) {
        const char *tail_sql =
            inbound ? "SELECT file_path, COUNT(*) FROM (SELECT n.file_path AS file_path FROM edges "
                      "e JOIN nodes n ON n.project=e.project AND n.id=e.source_id WHERE "
                      "e.project=?1 AND e.target_id=?2 AND e.type IN (" AA_CALLISH_TYPES ") ORDER "
                      "BY CASE e.type WHEN 'CALLS' THEN 0 WHEN 'CALL_REFERENCE' THEN 1 ELSE 2 END, "
                      "n.id LIMIT -1 OFFSET ?3) WHERE file_path IS NOT NULL GROUP BY file_path "
                      "ORDER BY COUNT(*) DESC, file_path LIMIT 10"
                    : "SELECT file_path, COUNT(*) FROM (SELECT n.file_path AS file_path FROM edges "
                      "e JOIN nodes n ON n.project=e.project AND n.id=e.target_id WHERE "
                      "e.project=?1 AND e.source_id=?2 AND e.type IN (" AA_CALLISH_TYPES ") ORDER "
                      "BY CASE e.type WHEN 'CALLS' THEN 0 WHEN 'CALL_REFERENCE' THEN 1 ELSE 2 END, "
                      "n.id LIMIT -1 OFFSET ?3) WHERE file_path IS NOT NULL GROUP BY file_path "
                      "ORDER BY COUNT(*) DESC, file_path LIMIT 10";
        if (sqlite3_prepare_v2(db, tail_sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
            sqlite3_bind_int64(st, 2, id);
            sqlite3_bind_int(st, 3, offset + limit);
            yyjson_mut_val *tail = yyjson_mut_arr(doc);
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *file = (const char *)sqlite3_column_text(st, 0);
                if (!file)
                    continue;
                yyjson_mut_val *row = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, row, "file", file);
                yyjson_mut_obj_add_int(doc, row, "count", sqlite3_column_int64(st, 1));
                yyjson_mut_arr_append(tail, row);
            }
            sqlite3_finalize(st);
            yyjson_mut_obj_add_val(doc, section, "overflow_by_file", tail);
        }
    }
    yyjson_mut_obj_add_val(doc, root, inbound ? "callers" : "callees", section);
}

char *cbm_atlas_symbol_json(cbm_store_t *store, const char *project, int64_t node_id,
                            const char *qualified_name, int limit, int offset) {
    if (!store || !project)
        return NULL;
    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db)
        return NULL;
    if (limit <= 0 || limit > 500)
        limit = 50;
    if (offset < 0)
        offset = 0;

    sqlite3_stmt *st = NULL;
    const char *node_sql =
        node_id >= 0 ? "SELECT id, label, name, qualified_name, file_path, start_line, end_line, "
                       "json_extract(properties,'$.docstring'), "
                       "json_extract(properties,'$.is_entry_point'), "
                       "json_extract(properties,'$.is_test'), "
                       "json_extract(properties,'$.is_exported') "
                       "FROM nodes WHERE project=?1 AND id=?2"
                     : "SELECT id, label, name, qualified_name, file_path, start_line, end_line, "
                       "json_extract(properties,'$.docstring'), "
                       "json_extract(properties,'$.is_entry_point'), "
                       "json_extract(properties,'$.is_test'), "
                       "json_extract(properties,'$.is_exported') "
                       "FROM nodes WHERE project=?1 AND qualified_name=?2 ORDER BY id LIMIT 1";
    if (sqlite3_prepare_v2(db, node_sql, -1, &st, NULL) != SQLITE_OK)
        return NULL;
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    if (node_id >= 0)
        sqlite3_bind_int64(st, 2, node_id);
    else
        sqlite3_bind_text(st, 2, qualified_name ? qualified_name : "", -1, SQLITE_STATIC);
    if (sqlite3_step(st) != SQLITE_ROW) {
        sqlite3_finalize(st);
        return NULL;
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_val *node = yyjson_mut_obj(doc);
    int64_t id = sqlite3_column_int64(st, 0);
    yyjson_mut_obj_add_int(doc, node, "id", id);
    yyjson_mut_obj_add_strcpy(doc, node, "label", (const char *)sqlite3_column_text(st, 1));
    yyjson_mut_obj_add_strcpy(doc, node, "name", (const char *)sqlite3_column_text(st, 2));
    const char *qn = (const char *)sqlite3_column_text(st, 3);
    if (qn)
        yyjson_mut_obj_add_strcpy(doc, node, "qualified_name", qn);
    const char *file = (const char *)sqlite3_column_text(st, 4);
    if (file)
        yyjson_mut_obj_add_strcpy(doc, node, "file_path", file);
    int start_line = sqlite3_column_int(st, 5);
    int end_line = sqlite3_column_int(st, 6);
    if (start_line > 0)
        yyjson_mut_obj_add_int(doc, node, "start_line", start_line);
    if (end_line > 0)
        yyjson_mut_obj_add_int(doc, node, "end_line", end_line);
    const char *docstring = (const char *)sqlite3_column_text(st, 7);
    if (docstring && docstring[0])
        yyjson_mut_obj_add_strcpy(doc, node, "docstring", docstring);
    if (sqlite3_column_int(st, 8))
        yyjson_mut_obj_add_bool(doc, node, "is_entry", true);
    if (sqlite3_column_int(st, 9))
        yyjson_mut_obj_add_bool(doc, node, "is_test", true);
    if (sqlite3_column_int(st, 10))
        yyjson_mut_obj_add_bool(doc, node, "is_exported", true);
    char *file_copy = aa_strdup(file);
    sqlite3_finalize(st);
    yyjson_mut_obj_add_val(doc, root, "node", node);

    /* Region membership (cache-backed). */
    if (file_copy) {
        char *region_name = NULL;
        int region = cbm_layout_region_for_file(store, project, file_copy, &region_name);
        if (region >= 0) {
            yyjson_mut_val *robj = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_int(doc, robj, "id", region);
            if (region_name)
                yyjson_mut_obj_add_strcpy(doc, robj, "name", region_name);
            yyjson_mut_obj_add_val(doc, root, "region", robj);
        }
        free(region_name);
    }

    aa_add_connections(doc, root, db, store, project, id, true, limit, offset);
    aa_add_connections(doc, root, db, store, project, id, false, limit, offset);

    /* A panel that can only ever say "none" should not render: tell the
     * client whether this project has DATA_FLOWS edges at all. */
    {
        sqlite3_stmt *pst = NULL;
        bool any = false;
        if (sqlite3_prepare_v2(db,
                               "SELECT 1 FROM edges WHERE project=?1 AND type='DATA_FLOWS' "
                               "LIMIT 1",
                               -1, &pst, NULL) == SQLITE_OK) {
            sqlite3_bind_text(pst, 1, project, -1, SQLITE_STATIC);
            any = sqlite3_step(pst) == SQLITE_ROW;
            sqlite3_finalize(pst);
        }
        yyjson_mut_obj_add_bool(doc, root, "project_has_data_flows", any);
    }

    /* Tests exercising this symbol (inbound TESTS edges). */
    if (sqlite3_prepare_v2(db,
                           "SELECT n.id, n.name, n.file_path FROM edges e JOIN nodes n ON "
                           "n.project=e.project AND n.id=e.source_id WHERE e.project=?1 AND "
                           "e.target_id=?2 AND e.type='TESTS' ORDER BY n.id LIMIT 50",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 2, id);
        yyjson_mut_val *tests = yyjson_mut_arr(doc);
        while (sqlite3_step(st) == SQLITE_ROW) {
            yyjson_mut_val *row = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_int(doc, row, "id", sqlite3_column_int64(st, 0));
            yyjson_mut_obj_add_strcpy(doc, row, "name", (const char *)sqlite3_column_text(st, 1));
            const char *tf = (const char *)sqlite3_column_text(st, 2);
            if (tf)
                yyjson_mut_obj_add_strcpy(doc, row, "file_path", tf);
            yyjson_mut_arr_append(tests, row);
        }
        sqlite3_finalize(st);
        yyjson_mut_obj_add_val(doc, root, "tests", tests);
    }

    /* Co-change partners of the symbol's FILE (either direction). */
    if (file_copy &&
        sqlite3_prepare_v2(
            db,
            "SELECT o.file_path, json_extract(e.properties,'$.coupling_score') FROM nodes f "
            "JOIN edges e ON e.project=f.project AND e.type='FILE_CHANGES_WITH' AND "
            "(e.source_id=f.id OR e.target_id=f.id) JOIN nodes o ON o.project=f.project AND "
            "o.id=CASE WHEN e.source_id=f.id THEN e.target_id ELSE e.source_id END "
            "WHERE f.project=?1 AND f.label='File' AND f.file_path=?2 ORDER BY 2 DESC "
            "LIMIT 20",
            -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, file_copy, -1, SQLITE_STATIC);
        yyjson_mut_val *cochange = yyjson_mut_arr(doc);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *other = (const char *)sqlite3_column_text(st, 0);
            if (!other)
                continue;
            yyjson_mut_val *row = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(doc, row, "file_path", other);
            double score = sqlite3_column_double(st, 1);
            if (score > 0)
                yyjson_mut_obj_add_real(doc, row, "score", score);
            yyjson_mut_arr_append(cochange, row);
        }
        sqlite3_finalize(st);
        yyjson_mut_obj_add_val(doc, root, "co_change", cochange);
    }

    /* Data flows (the "follow the value" family): in and out, with the
     * arg→param detail the pipeline stored on the edge. */
    static const char *const data_sql[2] = {
        "SELECT n.id, n.label, n.name, n.file_path, e.properties FROM edges e JOIN nodes n "
        "ON n.project=e.project AND n.id=e.source_id WHERE e.project=?1 AND e.target_id=?2 "
        "AND e.type='DATA_FLOWS' ORDER BY n.id LIMIT 25",
        "SELECT n.id, n.label, n.name, n.file_path, e.properties FROM edges e JOIN nodes n "
        "ON n.project=e.project AND n.id=e.target_id WHERE e.project=?1 AND e.source_id=?2 "
        "AND e.type='DATA_FLOWS' ORDER BY n.id LIMIT 25",
    };
    static const char *const data_field[2] = {"data_in", "data_out"};
    for (int direction = 0; direction < 2; direction++) {
        if (sqlite3_prepare_v2(db, data_sql[direction], -1, &st, NULL) != SQLITE_OK)
            continue;
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 2, id);
        yyjson_mut_val *arr = yyjson_mut_arr(doc);
        while (sqlite3_step(st) == SQLITE_ROW) {
            yyjson_mut_val *row = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_int(doc, row, "id", sqlite3_column_int64(st, 0));
            yyjson_mut_obj_add_strcpy(doc, row, "label", (const char *)sqlite3_column_text(st, 1));
            yyjson_mut_obj_add_strcpy(doc, row, "name", (const char *)sqlite3_column_text(st, 2));
            const char *df = (const char *)sqlite3_column_text(st, 3);
            if (df)
                yyjson_mut_obj_add_strcpy(doc, row, "file_path", df);
            const char *props = (const char *)sqlite3_column_text(st, 4);
            if (props && props[0] && strcmp(props, "{}") != 0) {
                yyjson_doc *pdoc = yyjson_read(props, strlen(props), 0);
                if (pdoc) {
                    yyjson_mut_val *copy = yyjson_val_mut_copy(doc, yyjson_doc_get_root(pdoc));
                    if (copy)
                        yyjson_mut_obj_add_val(doc, row, "detail", copy);
                    yyjson_doc_free(pdoc);
                }
            }
            yyjson_mut_arr_append(arr, row);
        }
        sqlite3_finalize(st);
        yyjson_mut_obj_add_val(doc, root, data_field[direction], arr);
    }

    /* Recent change history + ownership of the file — the rationale proxy
     * (the literature's #1 hardest question). */
    if (file_copy) {
        char *history = cbm_atlas_file_history_json(store, project, file_copy);
        if (history) {
            yyjson_doc *hdoc = yyjson_read(history, strlen(history), 0);
            if (hdoc) {
                yyjson_mut_val *copy = yyjson_val_mut_copy(doc, yyjson_doc_get_root(hdoc));
                if (copy)
                    yyjson_mut_obj_add_val(doc, root, "file_history", copy);
                yyjson_doc_free(hdoc);
            }
            free(history);
        }
    }

    /* Near-clones (SIMILAR_TO either direction). */
    if (sqlite3_prepare_v2(
            db,
            "SELECT n.id, n.name, n.file_path, json_extract(e.properties,'$.score') FROM "
            "edges e JOIN nodes n ON n.project=e.project AND n.id=CASE WHEN e.source_id=?2 "
            "THEN e.target_id ELSE e.source_id END WHERE e.project=?1 AND "
            "e.type='SIMILAR_TO' AND (e.source_id=?2 OR e.target_id=?2) ORDER BY n.id "
            "LIMIT 20",
            -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 2, id);
        yyjson_mut_val *similar = yyjson_mut_arr(doc);
        while (sqlite3_step(st) == SQLITE_ROW) {
            yyjson_mut_val *row = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_int(doc, row, "id", sqlite3_column_int64(st, 0));
            yyjson_mut_obj_add_strcpy(doc, row, "name", (const char *)sqlite3_column_text(st, 1));
            const char *sf = (const char *)sqlite3_column_text(st, 2);
            if (sf)
                yyjson_mut_obj_add_strcpy(doc, row, "file_path", sf);
            double score = sqlite3_column_double(st, 3);
            if (score > 0)
                yyjson_mut_obj_add_real(doc, row, "score", score);
            yyjson_mut_arr_append(similar, row);
        }
        sqlite3_finalize(st);
        yyjson_mut_obj_add_val(doc, root, "similar", similar);
    }

    free(file_copy);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

/* ── Scent: per-region hit counts for a symbol-name search ────────
 * The search-scent idea: a query should be answerable from the coarsest
 * view, so region blobs and list rows carry match counts before any drill.
 * Case-insensitive substring match on node name, bucketed by region via the
 * cached file->region map. */

char *cbm_atlas_scent_json(cbm_store_t *store, const char *project, const char *query) {
    if (!store || !project || !query || strlen(query) < 2)
        return NULL;
    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db)
        return NULL;

    enum { SCENT_MAX_MATCHES = 50000, SCENT_MAX_REGIONS = 4096 };
    long long *counts = calloc(SCENT_MAX_REGIONS, sizeof(long long));
    if (!counts)
        return NULL;
    long long total = 0, unmapped = 0;
    bool capped = false;

    /* Escape LIKE wildcards so a literal "_" in the query stays literal. */
    char pattern[512];
    {
        size_t w = 0;
        pattern[w++] = '%';
        for (const char *c = query; *c && w < sizeof(pattern) - 3; c++) {
            if (*c == '%' || *c == '_' || *c == '\\')
                pattern[w++] = '\\';
            pattern[w++] = *c;
        }
        pattern[w++] = '%';
        pattern[w] = '\0';
    }

    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
                           "SELECT file_path FROM nodes WHERE project=?1 AND name LIKE ?2 "
                           "ESCAPE '\\' COLLATE NOCASE AND file_path IS NOT NULL "
                           "AND label NOT IN ('File','Folder')",
                           -1, &st, NULL) != SQLITE_OK) {
        free(counts);
        return NULL;
    }
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, pattern, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (total >= SCENT_MAX_MATCHES) {
            capped = true;
            break;
        }
        const char *fp = (const char *)sqlite3_column_text(st, 0);
        total++;
        int region = fp ? cbm_layout_region_for_file(store, project, fp, NULL) : -1;
        if (region >= 0 && region < SCENT_MAX_REGIONS)
            counts[region]++;
        else
            unmapped++;
    }
    sqlite3_finalize(st);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "query", query);
    yyjson_mut_obj_add_int(doc, root, "total", total);
    yyjson_mut_obj_add_int(doc, root, "unmapped", unmapped);
    yyjson_mut_obj_add_bool(doc, root, "capped", capped);
    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int i = 0; i < SCENT_MAX_REGIONS; i++) {
        if (counts[i] == 0)
            continue;
        yyjson_mut_val *row = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, row, "region", i);
        yyjson_mut_obj_add_int(doc, row, "count", counts[i]);
        yyjson_mut_arr_append(arr, row);
    }
    yyjson_mut_obj_add_val(doc, root, "regions", arr);
    free(counts);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

/* ── Bridges: the nodes that stitch regions together ──────────────
 * Ranked by DISTINCT foreign regions a node CALLS INTO (cross-call count as
 * tiebreak) — orchestrators, the seam metric. Inbound direction is
 * deliberately ignored (a logger called from everywhere is a utility hub,
 * already listed under hubs) and test files are excluded (a test harness
 * calls everything). */

enum { BR_MAX_TRACK = 8, BR_TOP = 5, BR_EDGE_CAP = 2000000 };

typedef struct {
    int64_t id;
    char key[24];              /* the ht borrows key pointers — the node owns its key */
    int regions[BR_MAX_TRACK]; /* distinct foreign regions (first 8) */
    int region_count;
    long long cross_calls;
} br_node_t;

static void br_touch(br_node_t *node, int foreign_region) {
    node->cross_calls++;
    for (int i = 0; i < node->region_count; i++)
        if (node->regions[i] == foreign_region)
            return;
    if (node->region_count < BR_MAX_TRACK)
        node->regions[node->region_count++] = foreign_region;
}

char *cbm_atlas_bridges_json(cbm_store_t *store, const char *project) {
    if (!store || !project)
        return NULL;
    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db)
        return NULL;

    CBMHashTable *ht = cbm_ht_create(4096); /* id (as %lld string) → br_node_t* */
    if (!ht)
        return NULL;
    br_node_t **all = NULL;
    int all_count = 0, all_cap = 0;

    sqlite3_stmt *st = NULL;
    long long scanned = 0;
    if (sqlite3_prepare_v2(db,
                           "SELECT e.source_id, ns.file_path, e.target_id, nt.file_path FROM "
                           "edges e JOIN nodes ns ON ns.project=e.project AND ns.id=e.source_id "
                           "JOIN nodes nt ON nt.project=e.project AND nt.id=e.target_id WHERE "
                           "e.project=?1 AND e.type='CALLS'",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW && scanned < BR_EDGE_CAP) {
            scanned++;
            const char *sf = (const char *)sqlite3_column_text(st, 1);
            const char *tf = (const char *)sqlite3_column_text(st, 3);
            if (!sf || !tf)
                continue;
            if (cbm_is_test_file_path(sf))
                continue;
            int sr = cbm_layout_region_for_file(store, project, sf, NULL);
            int tr = cbm_layout_region_for_file(store, project, tf, NULL);
            if (sr < 0 || tr < 0 || sr == tr)
                continue;
            int64_t ids[1] = {sqlite3_column_int64(st, 0)};
            int foreign[1] = {tr};
            for (int k = 0; k < 1; k++) {
                char key[24];
                snprintf(key, sizeof(key), "%lld", (long long)ids[k]);
                br_node_t *node = cbm_ht_get(ht, key);
                if (!node) {
                    if (all_count >= all_cap) {
                        int nc = all_cap ? all_cap * 2 : 256;
                        br_node_t **grown = realloc(all, (size_t)nc * sizeof(br_node_t *));
                        if (!grown)
                            continue;
                        all = grown;
                        all_cap = nc;
                    }
                    node = calloc(1, sizeof(br_node_t));
                    if (!node)
                        continue;
                    node->id = ids[k];
                    snprintf(node->key, sizeof(node->key), "%s", key);
                    cbm_ht_set(ht, node->key, node);
                    all[all_count++] = node;
                }
                br_touch(node, foreign[k]);
            }
        }
        sqlite3_finalize(st);
    }

    /* Top BR_TOP by (region_count, cross_calls) — selection sort is fine. */
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "cross_edges_scanned", scanned);
    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int pick = 0; pick < BR_TOP; pick++) {
        int best = -1;
        for (int i = 0; i < all_count; i++) {
            if (!all[i])
                continue;
            if (best < 0 || all[i]->region_count > all[best]->region_count ||
                (all[i]->region_count == all[best]->region_count &&
                 all[i]->cross_calls > all[best]->cross_calls))
                best = i;
        }
        if (best < 0 || all[best]->region_count < 2)
            break;
        br_node_t *node = all[best];
        all[best] = NULL;
        sqlite3_stmt *nst = NULL;
        if (sqlite3_prepare_v2(db,
                               "SELECT name, file_path, qualified_name FROM nodes WHERE "
                               "project=?1 AND id=?2 AND label NOT IN ('File','Folder') LIMIT 1",
                               -1, &nst, NULL) == SQLITE_OK) {
            sqlite3_bind_text(nst, 1, project, -1, SQLITE_STATIC);
            sqlite3_bind_int64(nst, 2, node->id);
            if (sqlite3_step(nst) == SQLITE_ROW) {
                yyjson_mut_val *row = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_int(doc, row, "id", node->id);
                yyjson_mut_obj_add_strcpy(doc, row, "name",
                                          (const char *)sqlite3_column_text(nst, 0));
                const char *file = (const char *)sqlite3_column_text(nst, 1);
                if (file)
                    yyjson_mut_obj_add_strcpy(doc, row, "file_path", file);
                const char *qn = (const char *)sqlite3_column_text(nst, 2);
                if (qn)
                    yyjson_mut_obj_add_strcpy(doc, row, "qualified_name", qn);
                yyjson_mut_obj_add_int(doc, row, "regions", node->region_count);
                yyjson_mut_obj_add_int(doc, row, "cross_calls", node->cross_calls);
                yyjson_mut_arr_append(arr, row);
            } else {
                pick--; /* a File node slipped in — skip without burning a slot */
            }
            sqlite3_finalize(nst);
        }
        free(node);
    }
    yyjson_mut_obj_add_val(doc, root, "bridges", arr);
    for (int i = 0; i < all_count; i++)
        free(all[i]);
    free(all);
    cbm_ht_free(ht);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

/* ── Blast: bucket a file list by region ──────────────────────────
 * The Changes tab hands the impacted files here; the answer is the
 * graph-native review signal "this change touches N regions". */

char *cbm_atlas_blast_json(cbm_store_t *store, const char *project, const char *files_csv) {
    if (!store || !project || !files_csv || !files_csv[0])
        return NULL;

    enum { BL_MAX_REGIONS = 4096, BL_MAX_FILES = 200 };
    long long *counts = calloc(BL_MAX_REGIONS, sizeof(long long));
    char **names = calloc(BL_MAX_REGIONS, sizeof(char *));
    if (!counts || !names) {
        free(counts);
        free(names);
        return NULL;
    }
    int files = 0, unmapped = 0;
    bool truncated = false;

    char *copy = strdup(files_csv);
    if (!copy) {
        free(counts);
        free(names);
        return NULL;
    }
    char *cursor = copy;
    while (cursor && *cursor) {
        char *comma = strchr(cursor, ',');
        if (comma)
            *comma = '\0';
        if (*cursor) {
            if (files >= BL_MAX_FILES) {
                truncated = true;
                break;
            }
            files++;
            char *region_name = NULL; /* strdup'd by the lookup — we own it */
            int region = cbm_layout_region_for_file(store, project, cursor, &region_name);
            if (region >= 0 && region < BL_MAX_REGIONS) {
                counts[region]++;
                if (!names[region] && region_name) {
                    names[region] = region_name;
                    region_name = NULL;
                }
            } else {
                unmapped++;
            }
            free(region_name);
        }
        cursor = comma ? comma + 1 : NULL;
    }
    free(copy);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "files", files);
    yyjson_mut_obj_add_int(doc, root, "unmapped", unmapped);
    yyjson_mut_obj_add_bool(doc, root, "truncated", truncated);
    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int i = 0; i < BL_MAX_REGIONS; i++) {
        if (counts[i] == 0)
            continue;
        yyjson_mut_val *row = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, row, "region", i);
        if (names[i])
            yyjson_mut_obj_add_strcpy(doc, row, "name", names[i]);
        yyjson_mut_obj_add_int(doc, row, "count", counts[i]);
        yyjson_mut_arr_append(arr, row);
        free(names[i]);
    }
    yyjson_mut_obj_add_val(doc, root, "regions", arr);
    free(counts);
    free(names);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}
