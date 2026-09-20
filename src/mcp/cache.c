/* cache.c — Read-only cache inventory and lease-protected project pruning. */
#include "cache.h"
#include "foundation/compat.h"
#include "foundation/mem_core.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/platform.h"
#include "foundation/str_util.h"
#include <sqlite3.h>
#include <yyjson/yyjson.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum {
    CACHE_PATH_EXTRA = 2,
    CACHE_DECIMAL_BASE = 10,
    CACHE_SECONDS_SECOND = 1,
    CACHE_DURATION_SUFFIX_LENGTH = 1,
    CACHE_SQL_NUL_TERMINATED = -1,
    CACHE_SECONDS_MINUTE = 60,
    CACHE_SECONDS_HOUR = 3600,
    CACHE_SECONDS_DAY = 86400,
    CACHE_SECONDS_WEEK = 604800,
    CACHE_DB_SUFFIX_LENGTH = 3,
    CACHE_SIDECAR_SUFFIX_LENGTH = 4,
    CACHE_COLUMN_ROOT = 1,
    CACHE_COLUMN_INDEXED_AT = 2,
    CACHE_COLUMN_INDEXED_SECONDS = 3,
    CACHE_OPTION_MISSING = 1,
    CACHE_OPTION_DRY_RUN = 2,
    CACHE_OPTION_AGE = 4,
    CACHE_OPTION_ORPHANS = 8
};

typedef struct {
    bool missing_root;
    bool orphan_sidecars;
    bool dry_run;
    int64_t older_than;
} cache_options_t;

typedef struct {
    char *name;
    char *root;
    char *indexed_at;
    int64_t indexed_seconds;
    int64_t bytes;
    const char *root_status;
    const char *problem;
} cache_record_t;

static const char *const cache_suffixes[] = {"", "-wal", "-shm"};

static char *cache_path(const char *directory, const char *file, const char *suffix) {
    size_t size = strlen(directory) + strlen(file) + strlen(suffix) + CACHE_PATH_EXTRA;
    char *path = cbm_alloc(CBM_MEM_CLASS_OTHER, size);
    if (path) {
        (void)snprintf(path, size, "%s/%s%s", directory, file, suffix);
    }
    return path;
}

static void cache_record_free(cache_record_t *record) {
    cbm_free(CBM_MEM_CLASS_OTHER, record->name);
    cbm_free(CBM_MEM_CLASS_OTHER, record->root);
    cbm_free(CBM_MEM_CLASS_OTHER, record->indexed_at);
    memset(record, 0, sizeof(*record));
}

static bool cache_duration(const char *text, int64_t *seconds) {
    if (!text || text[0] < '0' || text[0] > '9') {
        return false;
    }
    errno = 0;
    char *end = NULL;
    long long value = strtoll(text, &end, CACHE_DECIMAL_BASE);
    if (errno || value <= 0 || !end || !end[0] || end[CACHE_DURATION_SUFFIX_LENGTH]) {
        return false;
    }
    int64_t scale;
    switch (*end) {
    case 's':
        scale = CACHE_SECONDS_SECOND;
        break;
    case 'm':
        scale = CACHE_SECONDS_MINUTE;
        break;
    case 'h':
        scale = CACHE_SECONDS_HOUR;
        break;
    case 'd':
        scale = CACHE_SECONDS_DAY;
        break;
    case 'w':
        scale = CACHE_SECONDS_WEEK;
        break;
    default:
        return false;
    }
    if (!scale || value > INT64_MAX / scale) {
        return false;
    }
    *seconds = (int64_t)value * scale;
    return true;
}

static bool cache_parse_option(const char *name, yyjson_val *value, cache_options_t *options) {
    if (strcmp(name, "missing_root") == 0 && yyjson_is_bool(value)) {
        options->missing_root = yyjson_get_bool(value);
        return true;
    }
    if (strcmp(name, "orphan_sidecars") == 0 && yyjson_is_bool(value)) {
        options->orphan_sidecars = yyjson_get_bool(value);
        return true;
    }
    if (strcmp(name, "dry_run") == 0 && yyjson_is_bool(value)) {
        options->dry_run = yyjson_get_bool(value);
        return true;
    }
    return strcmp(name, "older_than") == 0 && yyjson_is_str(value) &&
           cache_duration(yyjson_get_str(value), &options->older_than);
}

static const char *cache_parse_options(const char *args, bool prune, cache_options_t *options) {
    yyjson_doc *doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    const char *error = NULL;
    if (!yyjson_is_obj(root)) {
        error = "arguments must be an object";
    } else {
        yyjson_obj_iter iter = yyjson_obj_iter_with(root);
        yyjson_val *key;
        unsigned seen = 0;
        while ((key = yyjson_obj_iter_next(&iter))) {
            const char *name = yyjson_get_str(key);
            yyjson_val *value = yyjson_obj_iter_get_val(key);
            unsigned bit = 0;
            if (strcmp(name, "missing_root") == 0) {
                bit = CACHE_OPTION_MISSING;
            } else if (strcmp(name, "dry_run") == 0) {
                bit = CACHE_OPTION_DRY_RUN;
            } else if (strcmp(name, "older_than") == 0) {
                bit = CACHE_OPTION_AGE;
            } else if (strcmp(name, "orphan_sidecars") == 0) {
                bit = CACHE_OPTION_ORPHANS;
            }
            if (seen & bit) {
                error = "duplicate cache option";
                break;
            }
            seen |= bit;
            if (!prune || !cache_parse_option(name, value, options)) {
                error = "invalid option: expected booleans or a positive duration such as 30d";
                break;
            }
        }
        if (!error && prune && !options->missing_root && !options->older_than &&
            !options->orphan_sidecars) {
            error = "prune requires missing_root, older_than, or orphan_sidecars";
        }
    }
    if (!error && options->orphan_sidecars && (options->missing_root || options->older_than)) {
        error = "orphan_sidecars must be used alone: orphan files have no root or index timestamp";
    }
    yyjson_doc_free(doc);
    return error;
}

static void cache_read_root_status(cache_record_t *record) {
    if (!record->problem && record->root && record->root[0]) {
        cbm_path_info_t info;
        int status = cbm_path_info_utf8(record->root, &info);
        if (status == CBM_PATH_INFO_OK && !info.is_symlink) {
            record->root_status = info.is_directory ? "present" : "missing";
        } else if (status == CBM_PATH_INFO_ABSENT) {
            record->root_status = "missing";
        } else if (status == CBM_PATH_INFO_OK && info.is_symlink && cbm_is_dir(record->root)) {
            record->root_status = "present";
        }
        /* Inaccessible paths and unresolved links remain unknown. Use the
         * UTF-8 wrapper: narrow CRT stat can misclassify Windows paths. */
    }
}

static void cache_read_metadata(const char *directory, const char *file, cache_record_t *record) {
    /* SQLite NOFOLLOW rejects links in any path component, including macOS
     * /var -> /private/var. Resolve only the directory: the database itself
     * must still pass NOFOLLOW, even if replaced after the inventory check. */
    char canonical[CBM_SZ_4K];
    char *path = cbm_canonical_path(directory, canonical, sizeof(canonical))
                     ? cache_path(canonical, file, "")
                     : NULL;
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    int rc = path ? sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOFOLLOW, NULL)
                  : SQLITE_NOMEM;
    cbm_free(CBM_MEM_CLASS_OTHER, path);
    if (rc == SQLITE_OK) {
        rc = sqlite3_prepare_v2(
            db,
            "SELECT name, root_path, indexed_at, CAST(strftime('%s', indexed_at) AS INTEGER) "
            "FROM projects WHERE instr(name, '::') = 0",
            CACHE_SQL_NUL_TERMINATED, &stmt, NULL);
    }
    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(stmt, 0);
        const char *root = (const char *)sqlite3_column_text(stmt, CACHE_COLUMN_ROOT);
        const char *indexed = (const char *)sqlite3_column_text(stmt, CACHE_COLUMN_INDEXED_AT);
        record->name = name ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, name) : NULL;
        record->root = root ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, root) : NULL;
        record->indexed_at = indexed ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, indexed) : NULL;
        record->indexed_seconds = sqlite3_column_int64(stmt, CACHE_COLUMN_INDEXED_SECONDS);
        size_t length = strlen(file) - CACHE_DB_SUFFIX_LENGTH;
        if (!record->name || !record->root || !record->indexed_at) {
            record->problem = "unreadable_metadata";
        } else if (sqlite3_step(stmt) != SQLITE_DONE || strlen(record->name) != length ||
                   strncmp(record->name, file, length) != 0 ||
                   !cbm_validate_project_name(record->name)) {
            record->problem = "ambiguous_project_identity";
        }
    } else {
        record->problem = "unreadable_metadata";
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

/* Read only metadata, without schema migration, recovery, or opening a graph store.
 * Reject ambiguous legacy files: filename and primary identity must agree for
 * the mutation lease to protect the same database as the rest of the server. */
static void cache_read_record(const char *directory, const char *file, cache_record_t *record) {
    memset(record, 0, sizeof(*record));
    record->root_status = "unknown";
    for (size_t i = 0; i < sizeof(cache_suffixes) / sizeof(cache_suffixes[0]); i++) {
        char *path = cache_path(directory, file, cache_suffixes[i]);
        cbm_path_info_t info;
        int status = path ? cbm_path_info_utf8(path, &info) : CBM_PATH_INFO_UNAVAILABLE;
        cbm_free(CBM_MEM_CLASS_OTHER, path);
        if (status == CBM_PATH_INFO_OK && info.is_regular && !info.is_symlink) {
            record->bytes += info.size;
        } else if (i == 0 || status != CBM_PATH_INFO_ABSENT) {
            record->problem = "unavailable_or_nonregular_file";
        }
    }
    if (record->problem) {
        return;
    }
    cache_read_metadata(directory, file, record);
    cache_read_root_status(record);
}

static bool cache_matches(const cache_record_t *record, const cache_options_t *options,
                          int64_t now) {
    return !record->problem &&
           (!options->missing_root || strcmp(record->root_status, "missing") == 0) &&
           (!options->older_than ||
            (record->indexed_seconds > 0 && record->indexed_seconds <= now &&
             now - record->indexed_seconds > options->older_than));
}

/* Delete DB first. If that fails, leave its WAL intact. Count only bytes from
 * successful unlinks, and report partial failures rather than claiming success. */
static bool cache_remove(const char *directory, const char *file, int64_t *removed_bytes) {
    bool ok = true;
    for (size_t i = 0; i < sizeof(cache_suffixes) / sizeof(cache_suffixes[0]); i++) {
        char *path = cache_path(directory, file, cache_suffixes[i]);
        cbm_path_info_t info;
        int status = path ? cbm_path_info_utf8(path, &info) : CBM_PATH_INFO_UNAVAILABLE;
        if (status == CBM_PATH_INFO_ABSENT && i > 0) {
            cbm_free(CBM_MEM_CLASS_OTHER, path);
            continue;
        }
        bool removed = status == CBM_PATH_INFO_OK && info.is_regular && !info.is_symlink &&
                       cbm_unlink(path) == 0;
        if (removed) {
            *removed_bytes += info.size;
        } else {
            ok = false;
        }
        cbm_free(CBM_MEM_CLASS_OTHER, path);
        if (!removed && i == 0) {
            break;
        }
    }
    return ok;
}

/* Immediate children only. Do not follow links or silently include log-tree
 * sizes: this inventory is directly comparable to a directory listing. */
typedef enum {
    CACHE_ENTRY_PROJECT_DB,
    CACHE_ENTRY_INTERNAL_DB,
    CACHE_ENTRY_WAL,
    CACHE_ENTRY_SHM,
    CACHE_ENTRY_DIRECTORY,
    CACHE_ENTRY_SYMLINK,
    CACHE_ENTRY_OTHER,
    CACHE_ENTRY_UNAVAILABLE,
    CACHE_ENTRY_KIND_COUNT
} cache_entry_kind_t;

typedef struct {
    int64_t counts[CACHE_ENTRY_KIND_COUNT];
    int64_t bytes[CACHE_ENTRY_KIND_COUNT];
    int64_t orphan_sidecars;
    int64_t orphan_bytes;
} cache_inventory_t;

static bool cache_ends_with(const char *name, const char *suffix) {
    size_t length = strlen(name);
    size_t suffix_length = strlen(suffix);
    return length >= suffix_length && strcmp(name + length - suffix_length, suffix) == 0;
}

static void cache_inventory_add(cache_inventory_t *inventory, const char *directory,
                                const char *file) {
    char *path = cache_path(directory, file, "");
    cbm_path_info_t info = {0};
    int status = path ? cbm_path_info_utf8(path, &info) : CBM_PATH_INFO_UNAVAILABLE;
    cache_entry_kind_t kind = CACHE_ENTRY_OTHER;
    if (status != CBM_PATH_INFO_OK) {
        kind = CACHE_ENTRY_UNAVAILABLE;
    } else if (info.is_symlink) {
        kind = CACHE_ENTRY_SYMLINK;
    } else if (info.is_directory) {
        kind = CACHE_ENTRY_DIRECTORY;
    } else if (info.is_regular) {
        if (cache_ends_with(file, ".db")) {
            kind = file[0] == '_' ? CACHE_ENTRY_INTERNAL_DB : CACHE_ENTRY_PROJECT_DB;
        } else if (cache_ends_with(file, ".db-wal") || cache_ends_with(file, ".db-shm")) {
            kind = cache_ends_with(file, "-wal") ? CACHE_ENTRY_WAL : CACHE_ENTRY_SHM;
            /* Missing parent DB is an observation, not permission to delete:
             * a writer may be publishing a replacement database. */
            path[strlen(path) - CACHE_SIDECAR_SUFFIX_LENGTH] = '\0';
            cbm_path_info_t parent;
            if (cbm_path_info_utf8(path, &parent) == CBM_PATH_INFO_ABSENT) {
                inventory->orphan_sidecars++;
                inventory->orphan_bytes += info.size;
            }
        }
    }
    inventory->counts[kind]++;
    if (status == CBM_PATH_INFO_OK && info.is_regular && !info.is_symlink) {
        inventory->bytes[kind] += info.size;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, path);
}

static void cache_inventory_json(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                 const cache_inventory_t *inventory) {
    static const char *const names[CACHE_ENTRY_KIND_COUNT] = {
        "project_database", "internal_database", "wal",   "shm",
        "directory",        "symlink",           "other", "unavailable"};
    yyjson_mut_val *inventory_json = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, root, "directory_inventory", inventory_json);
    yyjson_mut_obj_add_str(doc, inventory_json, "scope",
                           "immediate children, including hidden entries; "
                           "logical regular-file bytes; no recursion or symlink following");
    yyjson_mut_val *categories = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, inventory_json, "categories", categories);
    int64_t entries = 0;
    int64_t bytes = 0;
    for (int i = 0; i < CACHE_ENTRY_KIND_COUNT; i++) {
        yyjson_mut_val *category = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_val(doc, categories, names[i], category);
        yyjson_mut_obj_add_int(doc, category, "entry_count", inventory->counts[i]);
        yyjson_mut_obj_add_int(doc, category, "size_bytes", inventory->bytes[i]);
        entries += inventory->counts[i];
        bytes += inventory->bytes[i];
    }
    yyjson_mut_obj_add_int(doc, inventory_json, "entry_count", entries);
    yyjson_mut_obj_add_int(doc, inventory_json, "regular_file_bytes", bytes);
    yyjson_mut_obj_add_int(doc, inventory_json, "orphan_sidecar_count", inventory->orphan_sidecars);
    yyjson_mut_obj_add_int(doc, inventory_json, "orphan_sidecar_bytes", inventory->orphan_bytes);
    yyjson_mut_obj_add_bool(doc, inventory_json, "complete",
                            inventory->counts[CACHE_ENTRY_UNAVAILABLE] == 0);
}

typedef struct {
    int64_t count;
    int64_t valid;
    int64_t bytes;
    int64_t missing;
    int64_t candidates;
    int64_t candidate_bytes;
    int64_t deleted;
    int64_t removed_bytes;
    int64_t failed;
    int64_t busy;
} cache_totals_t;

static const char *cache_prune_record(const char *directory, const char *file,
                                      const cache_record_t *record, const cache_options_t *options,
                                      const cbm_cache_ops_t *ops, int64_t now,
                                      cache_totals_t *totals) {
    const char *status;
    if (!ops->try_begin(ops->context, record->name)) {
        status = "busy_or_cancelled";
        totals->busy++;
    } else {
        cache_record_t current;
        cache_read_record(directory, file, &current);
        if (!cache_matches(&current, options, now) || strcmp(current.name, record->name) != 0) {
            status = "changed";
        } else {
            if (ops->before_delete) {
                ops->before_delete(ops->context, record->name);
            }
            bool removed = cache_remove(directory, file, &totals->removed_bytes);
            status = removed ? "deleted" : "delete_failed";
            totals->deleted += removed;
            totals->failed += !removed;
            if (removed && ops->after_delete) {
                ops->after_delete(ops->context, record->name);
            }
        }
        cache_record_free(&current);
        ops->end(ops->context, record->name);
    }
    return status;
}

static void cache_process_record(const char *directory, const char *file,
                                 const cache_options_t *options, bool prune,
                                 const cbm_cache_ops_t *ops, int64_t now, yyjson_mut_doc *doc,
                                 yyjson_mut_val *records, cache_totals_t *totals) {
    cache_record_t record;
    cache_read_record(directory, file, &record);
    totals->count++;
    totals->bytes += record.bytes;
    totals->valid += !record.problem;
    totals->missing += strcmp(record.root_status, "missing") == 0;
    bool match = prune && !options->orphan_sidecars && cache_matches(&record, options, now);
    const char *status = record.problem ? record.problem : "kept";
    if (match) {
        totals->candidates++;
        totals->candidate_bytes += record.bytes;
        status = "would_delete";
        if (!options->dry_run) {
            status = cache_prune_record(directory, file, &record, options, ops, now, totals);
        }
    }
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, item, "db_file", file);
    if (record.name) {
        yyjson_mut_obj_add_strcpy(doc, item, "project", record.name);
    }
    if (record.root) {
        yyjson_mut_obj_add_strcpy(doc, item, "root_path", record.root);
    }
    if (record.indexed_at) {
        yyjson_mut_obj_add_strcpy(doc, item, "indexed_at", record.indexed_at);
    }
    yyjson_mut_obj_add_str(doc, item, "root_status", record.root_status);
    yyjson_mut_obj_add_int(doc, item, "size_bytes", record.bytes);
    yyjson_mut_obj_add_str(doc, item, "status", status);
    yyjson_mut_arr_add_val(records, item);
    cache_record_free(&record);
}

/* A sidecar has no metadata once its database is gone. Derive only the
 * project lock key from the validated basename, never a source-root path. */
static char *cache_sidecar_project(const char *file) {
    if (file[0] == '_' ||
        (!cache_ends_with(file, ".db-wal") && !cache_ends_with(file, ".db-shm"))) {
        return NULL;
    }
    char *project = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, file);
    if (project) {
        project[strlen(project) - CACHE_DB_SUFFIX_LENGTH - CACHE_SIDECAR_SUFFIX_LENGTH] = '\0';
        if (!cbm_validate_project_name(project)) {
            cbm_free(CBM_MEM_CLASS_OTHER, project);
            project = NULL;
        }
    }
    return project;
}

static bool cache_orphan_info(const char *path, const char *database, cbm_path_info_t *info) {
    cbm_path_info_t parent;
    return cbm_path_info_utf8(database, &parent) == CBM_PATH_INFO_ABSENT &&
           cbm_path_info_utf8(path, info) == CBM_PATH_INFO_OK && info->is_regular &&
           !info->is_symlink;
}

static const char *cache_remove_orphan(const char *path, const char *database, const char *project,
                                       const cbm_cache_ops_t *ops, cache_totals_t *totals) {
    if (!ops->try_begin(ops->context, project)) {
        totals->busy++;
        return "busy_or_cancelled";
    }
    cbm_path_info_t info;
    const char *status = "changed";
    if (cache_orphan_info(path, database, &info)) {
        if (ops->before_delete) {
            ops->before_delete(ops->context, project);
        }
        /* Closing a stale store can checkpoint or remove its sidecars. */
        if (cache_orphan_info(path, database, &info)) {
            if (cbm_unlink(path) == 0) {
                totals->deleted++;
                totals->removed_bytes += info.size;
                status = "deleted";
                if (ops->after_delete) {
                    ops->after_delete(ops->context, project);
                }
            } else {
                totals->failed++;
                status = "delete_failed";
            }
        }
    }
    ops->end(ops->context, project);
    return status;
}

static void cache_process_orphan(const char *directory, const char *file,
                                 const cache_options_t *options, const cbm_cache_ops_t *ops,
                                 yyjson_mut_doc *doc, yyjson_mut_val *records,
                                 cache_totals_t *totals) {
    char *project = cache_sidecar_project(file);
    if (!project) {
        return;
    }
    char *path = cache_path(directory, file, "");
    char *database = cache_path(directory, project, ".db");
    cbm_path_info_t info;
    if (!path || !database) {
        totals->failed++;
    } else if (cache_orphan_info(path, database, &info)) {
        totals->candidates++;
        totals->candidate_bytes += info.size;
        const char *status = options->dry_run
                                 ? "would_delete"
                                 : cache_remove_orphan(path, database, project, ops, totals);
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, item, "file", file);
        yyjson_mut_obj_add_strcpy(doc, item, "project", project);
        yyjson_mut_obj_add_int(doc, item, "size_bytes", info.size);
        yyjson_mut_obj_add_str(doc, item, "status", status);
        yyjson_mut_arr_add_val(records, item);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, database);
    cbm_free(CBM_MEM_CLASS_OTHER, path);
    cbm_free(CBM_MEM_CLASS_OTHER, project);
}

static void cache_scan(const char *directory, cbm_dir_t *dir, const cache_options_t *options,
                       bool prune, const cbm_cache_ops_t *ops, yyjson_mut_doc *doc,
                       yyjson_mut_val *root, bool *is_error) {
    yyjson_mut_obj_add_strcpy(doc, root, "cache_dir", directory);
    yyjson_mut_obj_add_str(doc, root, "size_scope",
                           "project databases including WAL/SHM; logical bytes");
    yyjson_mut_obj_add_str(doc, root, "age_basis", "indexed_at (not last access)");
    if (prune) {
        yyjson_mut_obj_add_bool(doc, root, "dry_run", options->dry_run);
        yyjson_mut_obj_add_bool(doc, root, "orphan_sidecars", options->orphan_sidecars);
        yyjson_mut_obj_add_str(doc, root, "count_unit",
                               options->orphan_sidecars ? "sidecar_files" : "project_databases");
        yyjson_mut_obj_add_bool(doc, root, "missing_root", options->missing_root);
        yyjson_mut_obj_add_int(doc, root, "older_than_seconds", options->older_than);
        yyjson_mut_obj_add_str(doc, root, "match", "all");
    }
    yyjson_mut_val *records = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, root, "projects", records);
    yyjson_mut_val *sidecars = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, root, "sidecars", sidecars);
    cache_totals_t totals = {0};
    int64_t now = (int64_t)time(NULL);
    cache_inventory_t inventory = {0};
    cbm_dirent_t *entry;
    while (dir && (entry = cbm_readdir(dir))) {
        if (strcmp(entry->name, ".") == 0 || strcmp(entry->name, "..") == 0) {
            continue;
        }
        cache_inventory_add(&inventory, directory, entry->name);
        if (prune && options->orphan_sidecars) {
            cache_process_orphan(directory, entry->name, options, ops, doc, sidecars, &totals);
        }
        size_t length = strlen(entry->name);
        if (entry->name[0] == '_' || length <= CACHE_DB_SUFFIX_LENGTH ||
            strcmp(entry->name + length - CACHE_DB_SUFFIX_LENGTH, ".db") != 0) {
            continue;
        }
        cache_process_record(directory, entry->name, options, prune, ops, now, doc, records,
                             &totals);
    }
    cbm_closedir(dir);
    cache_inventory_json(doc, root, &inventory);
    yyjson_mut_obj_add_bool(doc, root, "has_more", false);
    yyjson_mut_obj_add_int(doc, root, "returned", totals.count);
    yyjson_mut_obj_add_int(doc, root, "database_count", totals.count);
    yyjson_mut_obj_add_int(doc, root, "project_count", totals.valid);
    yyjson_mut_obj_add_int(doc, root, "uninspectable_count", totals.count - totals.valid);
    yyjson_mut_obj_add_int(doc, root, "size_bytes", totals.bytes);
    yyjson_mut_obj_add_int(doc, root, "missing_root_count", totals.missing);
    if (prune) {
        yyjson_mut_obj_add_int(doc, root, "candidate_count", totals.candidates);
        yyjson_mut_obj_add_int(doc, root, "candidate_bytes", totals.candidate_bytes);
        yyjson_mut_obj_add_int(doc, root, "deleted_count", totals.deleted);
        yyjson_mut_obj_add_int(doc, root, "removed_bytes", totals.removed_bytes);
        yyjson_mut_obj_add_int(doc, root, "failed_count", totals.failed);
        yyjson_mut_obj_add_int(doc, root, "busy_count", totals.busy);
        *is_error = totals.failed > 0 || totals.busy > 0;
    }
}

char *cbm_cache_run(const char *directory, const char *args, bool prune, const cbm_cache_ops_t *ops,
                    bool *is_error) {
    *is_error = false;
    cache_options_t options = {0};
    const char *error = cache_parse_options(args ? args : "{}", prune, &options);
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        *is_error = true;
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    if (!error && prune && !options.dry_run && (!ops || !ops->try_begin || !ops->end)) {
        error = "prune requires a project mutation guard";
    }
    cbm_dir_t *dir = NULL;
    if (!error) {
        if (!directory || !directory[0]) {
            error = "cache directory is unavailable";
        } else {
            dir = cbm_opendir(directory);
            if (!dir) {
                /* cbm_opendir uses Win32 APIs on Windows, which do not set
                 * errno. Only a confirmed absent path is an empty cache. */
                cbm_path_info_t info;
                if (cbm_path_info_utf8(directory, &info) != CBM_PATH_INFO_ABSENT) {
                    error = "cannot read cache directory";
                }
            }
        }
    }
    if (error) {
        yyjson_mut_obj_add_str(doc, root, "error", error);
        *is_error = true;
    } else {
        cache_scan(directory, dir, &options, prune, ops, doc, root, is_error);
    }
    char *json = yyjson_mut_write(doc, YYJSON_WRITE_PRETTY, NULL);
    yyjson_mut_doc_free(doc);
    if (!json) {
        *is_error = true;
    }
    return json;
}
