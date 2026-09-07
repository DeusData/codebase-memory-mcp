/* atlas_why.c — the Why view: "when does this run?"
 *
 * The condition→action abstraction (Pennington 1987) served on demand: for a
 * symbol, its callers with the SYNTACTIC guard chain around each call site —
 * the enclosing if/else/switch/loop/ternary conditions, outermost first,
 * collected by walking the tree-sitter parse of the caller's file upward
 * from the call line. Guards are honest approximations: what the code
 * lexically wraps the call in, never proven path conditions.
 *
 * Zero indexing cost: CALLS edges already carry the call-site line, and the
 * embedded grammars parse the few caller files live per request. */

#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "ui/atlas.h"

#include "cbm.h"
#include "discover/discover.h"

#include <sqlite3.h>
#include <tree_sitter/api.h>
#include <yyjson/yyjson.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Per-request parse cache ────────────────────────────────────── */

enum {
    WHY_MAX_FILES = 32,       /* distinct caller files parsed per request */
    WHY_MAX_SOURCE = 2 << 20, /* skip files above 2 MB */
    WHY_MAX_GUARDS = 12,      /* ancestor conditions kept per call site */
    WHY_MAX_COND = 160,       /* condition text cap (chars) */
    WHY_MAX_ENTRIES = 25,     /* callers/callees per page */
    WHY_PARSE_TIMEOUT_US = 2000000,
};

typedef struct {
    char *file_path; /* project-relative */
    char *source;
    int source_len;
    CBMFileResult *result; /* owns the retained TSTree */
} why_file_t;

typedef struct {
    why_file_t files[WHY_MAX_FILES];
    int count;
    const char *root_path;
    const char *project;
} why_cache_t;

static why_file_t *why_file_get(why_cache_t *cache, const char *file_path) {
    for (int i = 0; i < cache->count; i++)
        if (strcmp(cache->files[i].file_path, file_path) == 0)
            return cache->files[i].source ? &cache->files[i] : NULL;
    if (cache->count >= WHY_MAX_FILES)
        return NULL;
    why_file_t *slot = &cache->files[cache->count++];
    slot->file_path = strdup(file_path);
    if (!slot->file_path)
        return NULL;

    char full[CBM_SZ_2K];
    snprintf(full, sizeof(full), "%s/%s", cache->root_path, file_path);
    FILE *fp = cbm_fopen(full, "rb");
    if (!fp)
        return NULL; /* slot stays as a negative cache entry */
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    if (size <= 0 || size > WHY_MAX_SOURCE) {
        fclose(fp);
        return NULL;
    }
    char *source = malloc((size_t)size + 1);
    if (!source) {
        fclose(fp);
        return NULL;
    }
    size_t got = fread(source, 1, (size_t)size, fp);
    fclose(fp);
    source[got] = '\0';

    const char *dot = strrchr(file_path, '.');
    CBMLanguage lang = cbm_language_for_extension(dot ? dot : "");
    if (lang == CBM_LANG_COUNT) {
        free(source);
        return NULL;
    }
    CBMFileResult *result = cbm_extract_file(source, (int)got, lang, cache->project, file_path,
                                             WHY_PARSE_TIMEOUT_US, NULL, NULL);
    if (!result || !result->cached_tree) {
        if (result)
            cbm_free_result(result);
        free(source);
        return NULL;
    }
    slot->source = source;
    slot->source_len = (int)got;
    slot->result = result;
    return slot;
}

static void why_cache_free(why_cache_t *cache) {
    for (int i = 0; i < cache->count; i++) {
        free(cache->files[i].file_path);
        free(cache->files[i].source);
        if (cache->files[i].result)
            cbm_free_result(cache->files[i].result);
    }
    cache->count = 0;
}

/* ── Guard extraction ───────────────────────────────────────────── */

typedef struct {
    const char *kind; /* "if" | "else" | "case" | "loop" | "ternary" | "catch" */
    char cond[WHY_MAX_COND];
    bool negated; /* call sits in the alternative/else arm of the condition */
} why_guard_t;

/* Copy node source text with whitespace collapsed, capped with a … marker. */
static void why_node_text(const char *source, int source_len, TSNode node, char *out, size_t cap) {
    uint32_t start = ts_node_start_byte(node);
    uint32_t end = ts_node_end_byte(node);
    if (end > (uint32_t)source_len)
        end = (uint32_t)source_len;
    size_t w = 0;
    bool in_space = false;
    for (uint32_t i = start; i < end && w + 4 < cap; i++) {
        char c = source[i];
        if (c == '\n' || c == '\t' || c == '\r' || c == ' ') {
            in_space = true;
            continue;
        }
        if (in_space && w > 0)
            out[w++] = ' ';
        in_space = false;
        out[w++] = c;
    }
    if (w + 4 >= cap && end > start)
        memcpy(out + w, "…", strlen("…") + 1);
    else
        out[w] = '\0';
    /* Strip one layer of wrapping parens for readability. */
    size_t len = strlen(out);
    if (len >= 2 && out[0] == '(' && out[len - 1] == ')') {
        memmove(out, out + 1, len - 2);
        out[len - 2] = '\0';
    }
}

static bool why_type_has(const char *type, const char *needle) {
    return strstr(type, needle) != NULL;
}

/* Walk ancestors from the call site to the enclosing function, collecting
 * conditions innermost-first (the caller reverses for display). Returns the
 * guard count; sets *in_loop when any ancestor is a loop. */
static int why_guards_at(const char *source, int source_len, TSNode at, why_guard_t *guards,
                         bool *in_loop) {
    int count = 0;
    *in_loop = false;
    TSNode prev = at;
    for (TSNode node = ts_node_parent(at); !ts_node_is_null(node);
         prev = node, node = ts_node_parent(node)) {
        const char *type = ts_node_type(node);
        if (why_type_has(type, "function") || why_type_has(type, "method") ||
            why_type_has(type, "lambda") || why_type_has(type, "closure"))
            break;
        if (count >= WHY_MAX_GUARDS)
            continue; /* keep scanning for the function boundary + loops */

        const char *kind = NULL;
        TSNode cond = ts_node_child_by_field_name(node, "condition", 9);
        bool negated = false;

        if (why_type_has(type, "if_") || strcmp(type, "if") == 0 || why_type_has(type, "elif")) {
            kind = "if";
            TSNode alt = ts_node_child_by_field_name(node, "alternative", 11);
            if (!ts_node_is_null(alt) && ts_node_start_byte(alt) <= ts_node_start_byte(prev) &&
                ts_node_end_byte(prev) <= ts_node_end_byte(alt))
                negated = true;
        } else if (why_type_has(type, "conditional_expression") || why_type_has(type, "ternary")) {
            kind = "ternary";
            TSNode alt = ts_node_child_by_field_name(node, "alternative", 11);
            if (!ts_node_is_null(alt) && ts_node_start_byte(alt) <= ts_node_start_byte(prev) &&
                ts_node_end_byte(prev) <= ts_node_end_byte(alt))
                negated = true;
        } else if (why_type_has(type, "case") || why_type_has(type, "when_")) {
            kind = "case";
            if (ts_node_is_null(cond))
                cond = ts_node_child_by_field_name(node, "value", 5);
        } else if (why_type_has(type, "switch") || why_type_has(type, "match")) {
            /* The switch head itself: only worth a guard when the case level
             * was missing (some grammars nest cases invisibly). */
            kind = "switch";
        } else if (why_type_has(type, "while") || why_type_has(type, "for") ||
                   why_type_has(type, "do_statement") || why_type_has(type, "loop") ||
                   why_type_has(type, "repeat")) {
            kind = "loop";
            *in_loop = true;
        } else if (why_type_has(type, "catch") || why_type_has(type, "except") ||
                   why_type_has(type, "rescue")) {
            kind = "catch";
        }
        if (!kind)
            continue;
        /* Skip switch heads when a case already carries the discriminant. */
        if (strcmp(kind, "switch") == 0 && count > 0 && strcmp(guards[count - 1].kind, "case") == 0)
            continue;

        why_guard_t *guard = &guards[count];
        guard->kind = kind;
        guard->negated = negated;
        guard->cond[0] = '\0';
        if (!ts_node_is_null(cond))
            why_node_text(source, source_len, cond, guard->cond, sizeof(guard->cond));
        count++;
    }
    return count;
}

/* Locate the call at `line` (1-based) and emit its guards into `arr`.
 * Returns false when the file could not be parsed. */
static bool why_emit_guards(why_cache_t *cache, const char *file_path, int line,
                            yyjson_mut_doc *doc, yyjson_mut_val *entry) {
    why_file_t *file = why_file_get(cache, file_path);
    if (!file)
        return false;
    TSNode root = ts_tree_root_node(file->result->cached_tree);
    TSPoint start = {(uint32_t)(line > 0 ? line - 1 : 0), 0};
    TSPoint end = {(uint32_t)(line > 0 ? line - 1 : 0), UINT32_MAX};
    TSNode at = ts_node_descendant_for_point_range(root, start, end);
    if (ts_node_is_null(at))
        return false;

    why_guard_t guards[WHY_MAX_GUARDS];
    bool in_loop = false;
    int count = why_guards_at(file->source, file->source_len, at, guards, &in_loop);

    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int i = count - 1; i >= 0; i--) { /* outermost first */
        yyjson_mut_val *row = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, row, "kind", guards[i].kind);
        if (guards[i].cond[0])
            yyjson_mut_obj_add_strcpy(doc, row, "cond", guards[i].cond);
        if (guards[i].negated)
            yyjson_mut_obj_add_bool(doc, row, "negated", true);
        yyjson_mut_arr_append(arr, row);
    }
    yyjson_mut_obj_add_val(doc, entry, "guards", arr);
    if (in_loop)
        yyjson_mut_obj_add_bool(doc, entry, "loop", true);
    return true;
}

/* ── Shared plumbing ────────────────────────────────────────────── */

static bool why_project_root(struct sqlite3 *db, const char *project, char *root, size_t root_cap) {
    root[0] = '\0';
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT root_path FROM projects WHERE name=?1", -1, &st, NULL) !=
        SQLITE_OK)
        return false;
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *r = (const char *)sqlite3_column_text(st, 0);
        if (r)
            snprintf(root, root_cap, "%s", r);
    }
    sqlite3_finalize(st);
    return root[0] != '\0';
}

static int why_edge_line(const char *properties_json) {
    if (!properties_json)
        return -1;
    yyjson_doc *doc = yyjson_read(properties_json, strlen(properties_json), 0);
    if (!doc)
        return -1;
    int line = (int)yyjson_get_int(yyjson_obj_get(yyjson_doc_get_root(doc), "line"));
    yyjson_doc_free(doc);
    return line > 0 ? line : -1;
}

/* Dynamic-dispatch uncertainty: how many candidate targets the resolver saw
 * for this call site (1 = certain, >1 = the ◇ case). */
static int why_edge_candidates(const char *properties_json) {
    if (!properties_json)
        return 0;
    yyjson_doc *doc = yyjson_read(properties_json, strlen(properties_json), 0);
    if (!doc)
        return 0;
    int candidates = (int)yyjson_get_int(yyjson_obj_get(yyjson_doc_get_root(doc), "candidates"));
    yyjson_doc_free(doc);
    return candidates;
}

/* ── The Why endpoint ───────────────────────────────────────────── */

char *cbm_atlas_why_json(cbm_store_t *store, const char *project, int64_t node_id,
                         const char *qualified_name, bool upward) {
    if (!store || !project)
        return NULL;
    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db)
        return NULL;

    /* Resolve the symbol. */
    sqlite3_stmt *st = NULL;
    int64_t id = node_id;
    char sym_name[256] = {0};
    char sym_file[1024] = {0};
    const char *resolve_sql =
        id >= 0 ? "SELECT id, name, file_path FROM nodes WHERE project=?1 AND id=?2 LIMIT 1"
                : "SELECT id, name, file_path FROM nodes WHERE project=?1 AND "
                  "qualified_name=?2 ORDER BY id LIMIT 1";
    if (sqlite3_prepare_v2(db, resolve_sql, -1, &st, NULL) != SQLITE_OK)
        return NULL;
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    if (id >= 0)
        sqlite3_bind_int64(st, 2, id);
    else if (qualified_name)
        sqlite3_bind_text(st, 2, qualified_name, -1, SQLITE_STATIC);
    bool found = false;
    if (sqlite3_step(st) == SQLITE_ROW) {
        found = true;
        id = sqlite3_column_int64(st, 0);
        const char *n = (const char *)sqlite3_column_text(st, 1);
        const char *f = (const char *)sqlite3_column_text(st, 2);
        if (n)
            snprintf(sym_name, sizeof(sym_name), "%s", n);
        if (f)
            snprintf(sym_file, sizeof(sym_file), "%s", f);
    }
    sqlite3_finalize(st);
    if (!found)
        return NULL;

    char root[CBM_SZ_1K];
    bool have_root = why_project_root(db, project, root, sizeof(root));

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *jroot = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, jroot);
    yyjson_mut_val *sym = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_int(doc, sym, "id", id);
    yyjson_mut_obj_add_strcpy(doc, sym, "name", sym_name);
    if (sym_file[0])
        yyjson_mut_obj_add_strcpy(doc, sym, "file_path", sym_file);
    yyjson_mut_obj_add_val(doc, jroot, "symbol", sym);
    yyjson_mut_obj_add_str(doc, jroot, "direction", upward ? "up" : "down");

    why_cache_t cache = {0};
    cache.root_path = root;
    cache.project = project;

    /* Callers (up) or callees (down), with the edge's call-site line.
     * For `up`, the call site lives in the OTHER node's file; for `down`,
     * every call site lives in this symbol's own file. */
    const char *rows_sql =
        upward ? "SELECT n.id, n.name, n.qualified_name, n.file_path, e.properties, "
                 "(SELECT COUNT(*) FROM edges e2 WHERE e2.project=e.project AND "
                 "e2.target_id=n.id AND e2.type='CALLS') FROM edges e JOIN nodes n ON "
                 "n.project=e.project AND n.id=e.source_id WHERE e.project=?1 AND "
                 "e.target_id=?2 AND e.type='CALLS' ORDER BY n.id LIMIT ?3"
               : "SELECT n.id, n.name, n.qualified_name, n.file_path, e.properties, "
                 "(SELECT COUNT(*) FROM edges e2 WHERE e2.project=e.project AND "
                 "e2.source_id=n.id AND e2.type='CALLS') FROM edges e JOIN nodes n ON "
                 "n.project=e.project AND n.id=e.target_id WHERE e.project=?1 AND "
                 "e.source_id=?2 AND e.type='CALLS' ORDER BY n.id LIMIT ?3";
    long long total = 0;
    {
        const char *count_sql =
            upward ? "SELECT COUNT(*) FROM edges WHERE project=?1 AND target_id=?2 AND "
                     "type='CALLS'"
                   : "SELECT COUNT(*) FROM edges WHERE project=?1 AND source_id=?2 AND "
                     "type='CALLS'";
        if (sqlite3_prepare_v2(db, count_sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
            sqlite3_bind_int64(st, 2, id);
            if (sqlite3_step(st) == SQLITE_ROW)
                total = sqlite3_column_int64(st, 0);
            sqlite3_finalize(st);
        }
    }
    yyjson_mut_obj_add_int(doc, jroot, "total", total);

    yyjson_mut_val *entries = yyjson_mut_arr(doc);
    int parse_failures = 0;
    if (sqlite3_prepare_v2(db, rows_sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 2, id);
        sqlite3_bind_int(st, 3, WHY_MAX_ENTRIES);
        while (sqlite3_step(st) == SQLITE_ROW) {
            yyjson_mut_val *entry = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_int(doc, entry, "id", sqlite3_column_int64(st, 0));
            const char *n = (const char *)sqlite3_column_text(st, 1);
            const char *qn = (const char *)sqlite3_column_text(st, 2);
            const char *f = (const char *)sqlite3_column_text(st, 3);
            const char *props = (const char *)sqlite3_column_text(st, 4);
            yyjson_mut_obj_add_strcpy(doc, entry, "name", n ? n : "?");
            if (qn)
                yyjson_mut_obj_add_strcpy(doc, entry, "qualified_name", qn);
            if (f)
                yyjson_mut_obj_add_strcpy(doc, entry, "file_path", f);
            yyjson_mut_obj_add_int(doc, entry, "more", sqlite3_column_int64(st, 5));
            int line = why_edge_line(props);
            if (line > 0)
                yyjson_mut_obj_add_int(doc, entry, "line", line);
            int candidates = why_edge_candidates(props);
            if (candidates > 1)
                yyjson_mut_obj_add_int(doc, entry, "candidates", candidates);
            /* The file holding the call site. */
            const char *site_file = upward ? f : (sym_file[0] ? sym_file : NULL);
            bool ok = false;
            if (have_root && site_file && line > 0)
                ok = why_emit_guards(&cache, site_file, line, doc, entry);
            if (!ok) {
                yyjson_mut_obj_add_val(doc, entry, "guards", yyjson_mut_arr(doc));
                yyjson_mut_obj_add_bool(doc, entry, "guards_unavailable", true);
                parse_failures++;
            }
            yyjson_mut_arr_append(entries, entry);
        }
        sqlite3_finalize(st);
    }
    yyjson_mut_obj_add_val(doc, jroot, "entries", entries);
    yyjson_mut_obj_add_int(doc, jroot, "guards_unavailable", parse_failures);
    yyjson_mut_obj_add_bool(doc, jroot, "sources_readable", have_root);

    why_cache_free(&cache);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

/* ── Guards for one explicit call site (A→B trace hops) ─────────── */

char *cbm_atlas_callsite_guards_json(cbm_store_t *store, const char *project, const char *file_path,
                                     int line) {
    if (!store || !project || !file_path || line <= 0)
        return NULL;
    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db)
        return NULL;
    char root[CBM_SZ_1K];
    if (!why_project_root(db, project, root, sizeof(root)))
        return NULL;
    why_cache_t cache = {0};
    cache.root_path = root;
    cache.project = project;

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *entry = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, entry);
    bool ok = why_emit_guards(&cache, file_path, line, doc, entry);
    why_cache_free(&cache);
    if (!ok) {
        yyjson_mut_doc_free(doc);
        return NULL;
    }
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}
