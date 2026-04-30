/*
 * artifact.c — Persistent artifact export/import for team sharing.
 *
 * Export: strip indexes → VACUUM INTO temp → zstd compress → write .zst + metadata
 * Import: decompress → write to cache → open (auto-creates indexes) → integrity check
 */
#include "foundation/constants.h"

enum {
    ART_DIR_PERMS = 0755,
    ART_ZSTD_FAST = 3,
    ART_ZSTD_BEST = 9,
    ART_RATIO_SCALE = 10, /* multiply ratio by 10 for integer logging */
    ART_NUL = 1,          /* NUL terminator byte */
};
#define ART_BYTES_PER_MB ((size_t)1024 * 1024)

#include "pipeline/artifact.h"
#include "store/store.h"
#include "foundation/platform.h"
#include "foundation/compat_fs.h"
#include "foundation/compat.h"
#include "foundation/log.h"

#include "zstd_store.h"

#include <sqlite3.h>
#include <yyjson/yyjson.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

/* ── Helpers ──────────────────────────────────────────────────────── */

/* Thread-local rotating buffers for small int→string conversions (logging).
 * Rotating allows multiple itoa_buf() calls in a single log statement. */
enum { ART_RING = 4, ART_RING_MASK = 3 };
static const char *itoa_buf(int v) {
    static _Thread_local char bufs[ART_RING][CBM_SZ_32];
    static _Thread_local int idx = 0;
    int i = idx;
    idx = (idx + ART_NUL) & ART_RING_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", v);
    return bufs[i];
}

/* Build path: <repo>/.codebase-memory/<name> into caller-owned buf. */
static void artifact_path(char *buf, size_t bufsz, const char *repo_path, const char *name) {
    snprintf(buf, bufsz, "%s/%s/%s", repo_path, CBM_ARTIFACT_DIR, name);
}

/* Read entire file into malloc'd buffer. Sets *out_len. Returns NULL on error. */
static char *read_file_alloc(const char *path, size_t *out_len) {
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        return NULL;
    }
    (void)fseek(fp, 0, SEEK_END);
    long sz = ftell(fp);
    if (sz <= 0) {
        (void)fclose(fp);
        return NULL;
    }
    (void)fseek(fp, 0, SEEK_SET);
    char *buf = malloc((size_t)sz);
    if (!buf) {
        (void)fclose(fp);
        return NULL;
    }
    size_t rd = fread(buf, ART_NUL, (size_t)sz, fp);
    (void)fclose(fp);
    if ((long)rd != sz) {
        free(buf);
        return NULL;
    }
    *out_len = (size_t)sz;
    return buf;
}

/* Write buffer to file atomically (write to tmp, rename). Returns 0 on success. */
static int write_file_atomic(const char *path, const char *data, size_t len) {
    char tmp[CBM_SZ_4K];
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    FILE *fp = fopen(tmp, "wb");
    if (!fp) {
        return CBM_NOT_FOUND;
    }
    size_t wr = fwrite(data, ART_NUL, len, fp);
    (void)fclose(fp);
    if (wr != len) {
        cbm_unlink(tmp);
        return CBM_NOT_FOUND;
    }
    if (rename(tmp, path) != 0) {
        cbm_unlink(tmp);
        return CBM_NOT_FOUND;
    }
    return 0;
}

/* Get current git HEAD hash. buf must be >= CBM_SZ_64. Returns false on error. */
static bool git_head_hash(const char *repo_path, char *buf, size_t bufsz) {
    char cmd[CBM_SZ_1K];
    snprintf(cmd, sizeof(cmd), "git -C '%s' rev-parse HEAD 2>/dev/null", repo_path);
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        buf[0] = '\0';
        return false;
    }
    buf[0] = '\0';
    if (fgets(buf, (int)bufsz, fp)) {
        /* Strip trailing newline */
        size_t len = strlen(buf);
        while (len > 0 && (buf[len - ART_NUL] == '\n' || buf[len - ART_NUL] == '\r')) {
            buf[--len] = '\0';
        }
    }
    (void)cbm_pclose(fp);
    return buf[0] != '\0';
}

/* Generate ISO 8601 timestamp into buf. */
static void iso_timestamp(char *buf, size_t bufsz) {
    time_t now = time(NULL);
    struct tm tm;
#ifdef _WIN32
    gmtime_s(&tm, &now);
#else
    gmtime_r(&now, &tm);
#endif
    (void)strftime(buf, bufsz, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

/* ── Metadata read/write ─────────────────────────────────────────── */

/* Read schema_version from artifact.json. Returns -1 if missing/invalid. */
static int read_metadata_version(const char *repo_path) {
    char meta_path[CBM_SZ_4K];
    artifact_path(meta_path, sizeof(meta_path), repo_path, CBM_ARTIFACT_META);

    size_t len = 0;
    char *json = read_file_alloc(meta_path, &len);
    if (!json) {
        return CBM_NOT_FOUND;
    }

    yyjson_doc *doc = yyjson_read(json, len, 0);
    free(json);
    if (!doc) {
        return CBM_NOT_FOUND;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *ver = yyjson_obj_get(root, "schema_version");
    int version = ver ? yyjson_get_int(ver) : CBM_NOT_FOUND;
    yyjson_doc_free(doc);
    return version;
}

/* Read original_size from artifact.json. Returns 0 on error. */
static size_t read_metadata_original_size(const char *repo_path) {
    char meta_path[CBM_SZ_4K];
    artifact_path(meta_path, sizeof(meta_path), repo_path, CBM_ARTIFACT_META);

    size_t len = 0;
    char *json = read_file_alloc(meta_path, &len);
    if (!json) {
        return 0;
    }

    yyjson_doc *doc = yyjson_read(json, len, 0);
    free(json);
    if (!doc) {
        return 0;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, "original_size");
    size_t result = val ? (size_t)yyjson_get_uint(val) : 0;
    yyjson_doc_free(doc);
    return result;
}

/* Write artifact.json metadata. */
static int write_metadata(const char *repo_path, const char *project_name, int nodes, int edges,
                          size_t original_size, size_t compressed_size, int compression_level) {
    char commit[CBM_SZ_64] = "";
    git_head_hash(repo_path, commit, sizeof(commit));

    char ts[CBM_SZ_64];
    iso_timestamp(ts, sizeof(ts));

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_int(doc, root, "schema_version", CBM_ARTIFACT_SCHEMA_VERSION);
    yyjson_mut_obj_add_str(doc, root, "commit", commit);
    yyjson_mut_obj_add_str(doc, root, "indexed_at", ts);
    yyjson_mut_obj_add_str(doc, root, "project", project_name);
    yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, root, "edges", edges);
    yyjson_mut_obj_add_uint(doc, root, "original_size", (uint64_t)original_size);
    yyjson_mut_obj_add_uint(doc, root, "compressed_size", (uint64_t)compressed_size);
    yyjson_mut_obj_add_int(doc, root, "compression_level", compression_level);

    size_t json_len = 0;
    char *json = yyjson_mut_write(doc, YYJSON_WRITE_PRETTY, &json_len);
    yyjson_mut_doc_free(doc);
    if (!json) {
        return CBM_NOT_FOUND;
    }

    char meta_path[CBM_SZ_4K];
    artifact_path(meta_path, sizeof(meta_path), repo_path, CBM_ARTIFACT_META);
    int rc = write_file_atomic(meta_path, json, json_len);
    free(json);
    return rc;
}

/* ── .gitattributes setup ────────────────────────────────────────── */

static void ensure_gitattributes(const char *repo_path) {
    char ga_path[CBM_SZ_4K];
    artifact_path(ga_path, sizeof(ga_path), repo_path, ".gitattributes");

    struct stat st;
    if (stat(ga_path, &st) == 0) {
        return; /* already exists */
    }

    FILE *fp = fopen(ga_path, "w");
    if (fp) {
        (void)fputs("# Auto-generated by codebase-memory-mcp\n"
                    "# Prevent merge conflicts on compressed artifact\n" CBM_ARTIFACT_FILENAME
                    " merge=ours binary\n",
                    fp);
        (void)fclose(fp);
    }

    /* Best-effort: configure merge driver */
    char cmd[CBM_SZ_1K];
    snprintf(cmd, sizeof(cmd), "git -C '%s' config merge.ours.driver true 2>/dev/null", repo_path);
    FILE *p = cbm_popen(cmd, "r");
    if (p) {
        (void)cbm_pclose(p);
    }
}

/* ── Index stripping ─────────────────────────────────────────────── */

/* SQL to drop all user-created indexes (not autoindexes, not FTS5). */
static const char *DROP_INDEXES_SQL = "DROP INDEX IF EXISTS idx_nodes_label;"
                                      "DROP INDEX IF EXISTS idx_nodes_name;"
                                      "DROP INDEX IF EXISTS idx_nodes_file;"
                                      "DROP INDEX IF EXISTS idx_edges_source;"
                                      "DROP INDEX IF EXISTS idx_edges_target;"
                                      "DROP INDEX IF EXISTS idx_edges_type;"
                                      "DROP INDEX IF EXISTS idx_edges_target_type;"
                                      "DROP INDEX IF EXISTS idx_edges_source_type;"
                                      "DROP INDEX IF EXISTS idx_edges_url_path;";

/* ── Export helpers ───────────────────────────────────────────────── */

/* Prepare a stripped DB copy for best-quality export.
 * VACUUM INTO → drop indexes → VACUUM. Returns malloc'd buffer or NULL. */
static char *prepare_stripped_db(const char *db_path, size_t *out_size) {
    char tmp_path[CBM_SZ_4K];
    snprintf(tmp_path, sizeof(tmp_path), "%s/cbm_artifact_tmp.db", cbm_tmpdir());
    cbm_unlink(tmp_path);

    /* VACUUM INTO: clean compacted copy. Use raw sqlite3 to bypass store authorizer
     * (which blocks ATTACH, used internally by VACUUM INTO). */
    sqlite3 *raw_db = NULL;
    if (sqlite3_open_v2(db_path, &raw_db, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK) {
        cbm_log_error("artifact.export", "err", "open_source_db");
        sqlite3_close(raw_db);
        return NULL;
    }

    char vacuum_sql[CBM_SZ_4K];
    snprintf(vacuum_sql, sizeof(vacuum_sql), "VACUUM INTO '%s';", tmp_path);
    char *errmsg = NULL;
    int vrc = sqlite3_exec(raw_db, vacuum_sql, NULL, NULL, &errmsg);
    sqlite3_close(raw_db);

    if (vrc != SQLITE_OK) {
        cbm_log_error("artifact.export", "err", "vacuum_into");
        sqlite3_free(errmsg);
        cbm_unlink(tmp_path);
        return NULL;
    }

    /* Strip indexes from the copy for better compression. */
    sqlite3 *tmp_db = NULL;
    if (sqlite3_open_v2(tmp_path, &tmp_db, SQLITE_OPEN_READWRITE, NULL) == SQLITE_OK) {
        sqlite3_exec(tmp_db, DROP_INDEXES_SQL, NULL, NULL, NULL);
        sqlite3_exec(tmp_db, "VACUUM;", NULL, NULL, NULL);
        sqlite3_close(tmp_db);
    }

    char *data = read_file_alloc(tmp_path, out_size);
    cbm_unlink(tmp_path);

    /* Clean up WAL/SHM from temp */
    char wal[CBM_SZ_4K];
    char shm[CBM_SZ_4K];
    snprintf(wal, sizeof(wal), "%s-wal", tmp_path);
    snprintf(shm, sizeof(shm), "%s-shm", tmp_path);
    cbm_unlink(wal);
    cbm_unlink(shm);
    return data;
}

/* ── Export ───────────────────────────────────────────────────────── */

int cbm_artifact_export(const char *db_path, const char *repo_path, const char *project_name,
                        int quality) {
    if (!db_path || !repo_path || !project_name) {
        return CBM_NOT_FOUND;
    }

    /* Ensure .codebase-memory/ directory exists */
    char art_dir[CBM_SZ_4K];
    snprintf(art_dir, sizeof(art_dir), "%s/%s", repo_path, CBM_ARTIFACT_DIR);
    cbm_mkdir_p(art_dir, ART_DIR_PERMS);

    size_t db_size = 0;
    char *db_data = NULL;
    int compression_level = ART_ZSTD_FAST;

    if (quality == CBM_ARTIFACT_BEST) {
        compression_level = ART_ZSTD_BEST;
        db_data = prepare_stripped_db(db_path, &db_size);
    } else {
        db_data = read_file_alloc(db_path, &db_size);
    }

    if (!db_data || db_size == 0) {
        free(db_data);
        cbm_log_error("artifact.export", "err", "read_db");
        return CBM_NOT_FOUND;
    }

    /* Compress with zstd */
    size_t bound = cbm_zstd_compress_bound((int)db_size);
    char *compressed = malloc(bound);
    if (!compressed) {
        free(db_data);
        return CBM_NOT_FOUND;
    }

    int clen = cbm_zstd_compress(db_data, (int)db_size, compressed, (int)bound, compression_level);
    free(db_data);

    if (clen <= 0) {
        free(compressed);
        cbm_log_error("artifact.export", "err", "zstd_compress");
        return CBM_NOT_FOUND;
    }

    /* Write compressed artifact */
    char zst_path[CBM_SZ_4K];
    artifact_path(zst_path, sizeof(zst_path), repo_path, CBM_ARTIFACT_FILENAME);
    int wrc = write_file_atomic(zst_path, compressed, (size_t)clen);
    free(compressed);

    if (wrc != 0) {
        cbm_log_error("artifact.export", "err", "write_artifact");
        return CBM_NOT_FOUND;
    }

    /* Get node/edge counts for metadata */
    int nodes = 0;
    int edges = 0;
    cbm_store_t *count_store = cbm_store_open_path(db_path);
    if (count_store) {
        nodes = cbm_store_count_nodes(count_store, project_name);
        edges = cbm_store_count_edges(count_store, project_name);
        cbm_store_close(count_store);
    }

    /* Write metadata */
    write_metadata(repo_path, project_name, nodes, edges, db_size, (size_t)clen, compression_level);

    /* Ensure .gitattributes for merge conflict prevention */
    ensure_gitattributes(repo_path);

    double ratio = db_size > 0 ? (double)db_size / (double)clen : 0.0;
    cbm_log_info("artifact.export", "quality", quality == CBM_ARTIFACT_BEST ? "best" : "fast",
                 "original_mb", itoa_buf((int)(db_size / ART_BYTES_PER_MB)), "compressed_mb",
                 itoa_buf((int)((size_t)clen / ART_BYTES_PER_MB)), "ratio",
                 itoa_buf((int)(ratio * ART_RATIO_SCALE)));

    return 0;
}

/* ── Import ──────────────────────────────────────────────────────── */

int cbm_artifact_import(const char *repo_path, const char *cache_db_path) {
    if (!repo_path || !cache_db_path) {
        return CBM_NOT_FOUND;
    }

    /* Check schema version compatibility */
    int version = read_metadata_version(repo_path);
    if (version < 0 || version > CBM_ARTIFACT_SCHEMA_VERSION) {
        cbm_log_info("artifact.import", "skip", "schema_version_mismatch", "artifact_ver",
                     itoa_buf(version), "current_ver", itoa_buf(CBM_ARTIFACT_SCHEMA_VERSION));
        return CBM_NOT_FOUND;
    }

    /* Get original_size for decompression buffer */
    size_t original_size = read_metadata_original_size(repo_path);
    if (original_size == 0) {
        cbm_log_error("artifact.import", "err", "missing_original_size");
        return CBM_NOT_FOUND;
    }

    /* Read compressed artifact */
    char zst_path[CBM_SZ_4K];
    artifact_path(zst_path, sizeof(zst_path), repo_path, CBM_ARTIFACT_FILENAME);

    size_t clen = 0;
    char *compressed = read_file_alloc(zst_path, &clen);
    if (!compressed) {
        cbm_log_error("artifact.import", "err", "read_artifact");
        return CBM_NOT_FOUND;
    }

    /* Decompress */
    char *decompressed = malloc(original_size);
    if (!decompressed) {
        free(compressed);
        return CBM_NOT_FOUND;
    }

    int dlen = cbm_zstd_decompress(compressed, (int)clen, decompressed, (int)original_size);
    free(compressed);

    if (dlen <= 0) {
        free(decompressed);
        cbm_log_error("artifact.import", "err", "zstd_decompress");
        return CBM_NOT_FOUND;
    }

    /* Write to temp file, then rename for atomicity */
    char tmp_path[CBM_SZ_4K];
    snprintf(tmp_path, sizeof(tmp_path), "%s.import_tmp", cache_db_path);

    /* Ensure cache directory exists */
    char cache_dir[CBM_SZ_1K];
    snprintf(cache_dir, sizeof(cache_dir), "%s", cache_db_path);
    char *last_slash = strrchr(cache_dir, '/');
    if (last_slash) {
        *last_slash = '\0';
        cbm_mkdir_p(cache_dir, ART_DIR_PERMS);
    }

    int wrc = write_file_atomic(tmp_path, decompressed, (size_t)dlen);
    free(decompressed);

    if (wrc != 0) {
        cbm_log_error("artifact.import", "err", "write_temp_db");
        return CBM_NOT_FOUND;
    }

    /* Open with cbm_store_open_path to auto-create missing indexes + FTS5 */
    cbm_store_t *store = cbm_store_open_path(tmp_path);
    if (!store) {
        cbm_log_error("artifact.import", "err", "open_imported_db");
        cbm_unlink(tmp_path);
        return CBM_NOT_FOUND;
    }

    /* Integrity check — refuse corrupted artifacts */
    if (!cbm_store_check_integrity(store)) {
        cbm_log_error("artifact.import", "err", "integrity_check_failed");
        cbm_store_close(store);
        cbm_unlink(tmp_path);
        return CBM_NOT_FOUND;
    }

    cbm_store_close(store);

    /* Atomic rename to final path */
    if (rename(tmp_path, cache_db_path) != 0) {
        cbm_log_error("artifact.import", "err", "rename_to_cache");
        cbm_unlink(tmp_path);
        return CBM_NOT_FOUND;
    }

    /* Clean up any stale WAL/SHM from the temp open */
    char wal[CBM_SZ_4K];
    char shm[CBM_SZ_4K];
    snprintf(wal, sizeof(wal), "%s-wal", tmp_path);
    snprintf(shm, sizeof(shm), "%s-shm", tmp_path);
    cbm_unlink(wal);
    cbm_unlink(shm);

    cbm_log_info("artifact.import", "db", cache_db_path, "size_mb",
                 itoa_buf((int)((size_t)dlen / ART_BYTES_PER_MB)));

    return 0;
}

/* ── Existence check ─────────────────────────────────────────────── */

bool cbm_artifact_exists(const char *repo_path) {
    if (!repo_path) {
        return false;
    }

    char zst_path[CBM_SZ_4K];
    artifact_path(zst_path, sizeof(zst_path), repo_path, CBM_ARTIFACT_FILENAME);

    struct stat st;
    if (stat(zst_path, &st) != 0 || st.st_size == 0) {
        return false;
    }

    /* Check schema version is compatible */
    int version = read_metadata_version(repo_path);
    return version >= 0 && version <= CBM_ARTIFACT_SCHEMA_VERSION;
}

/* ── Commit hash extraction ──────────────────────────────────────── */

char *cbm_artifact_commit(const char *repo_path) {
    if (!repo_path) {
        return NULL;
    }

    char meta_path[CBM_SZ_4K];
    artifact_path(meta_path, sizeof(meta_path), repo_path, CBM_ARTIFACT_META);

    size_t len = 0;
    char *json = read_file_alloc(meta_path, &len);
    if (!json) {
        return NULL;
    }

    yyjson_doc *doc = yyjson_read(json, len, 0);
    free(json);
    if (!doc) {
        return NULL;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, "commit");
    char *result = NULL;
    if (val) {
        const char *s = yyjson_get_str(val);
        if (s && s[0]) {
            size_t slen = strlen(s);
            result = malloc(slen + ART_NUL);
            if (result) {
                memcpy(result, s, slen + ART_NUL);
            }
        }
    }
    yyjson_doc_free(doc);
    return result;
}
