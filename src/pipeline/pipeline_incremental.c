/*
 * pipeline_incremental.c — Disk-based incremental re-indexing.
 *
 * Operates on the existing SQLite DB directly (not RAM-first graph buffer).
 * Compares file mtime+size against stored hashes to classify changed/unchanged.
 * Deletes changed files' nodes (edges cascade via ON DELETE CASCADE),
 * re-parses only changed files through passes into a temp graph buffer,
 * then merges new nodes/edges into the disk DB. Persists updated hashes.
 *
 * Called from pipeline.c when a DB with stored hashes already exists.
 */
#include "foundation/constants.h"

enum { INCR_RING_BUF = 4, INCR_RING_MASK = 3, INCR_TS_BUF = 24 };
#include "pipeline/pipeline.h"
#include <stdio.h>
#include <time.h>
#include "pipeline/lsp_surface.h"
#include "pipeline/pass_lsp_cross.h"
#include "sqlite3.h"
#include "yyjson/yyjson.h"
#include "pipeline/pipeline_internal.h"
#include "store/store.h"
#include "graph_buffer/graph_buffer.h"
#include "discover/discover.h"
#include "foundation/log.h"
#include "foundation/hash_table.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/platform.h"
#include "foundation/sha256.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <stdatomic.h>
#include <stdint.h>

/* ── Constants ───────────────────────────────────────────────────── */

#define CBM_MS_PER_SEC 1000.0
#define CBM_NS_PER_MS 1000000.0
#define CBM_NS_PER_SEC 1000000000LL

#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
/* One-shot fault injection for the parallel incremental result cache. The
 * production build has no hook or branch at this allocation site. */
static atomic_bool g_incr_test_fail_result_cache_alloc = false;
static atomic_bool g_incr_test_force_legacy_partial = false;
static atomic_int g_incr_test_last_route = CBM_INCREMENTAL_ROUTE_NONE;

void cbm_pipeline_incremental_test_fail_result_cache_alloc_once(void) {
    atomic_store(&g_incr_test_fail_result_cache_alloc, true);
}

void cbm_pipeline_incremental_test_force_legacy_partial_once(void) {
    atomic_store(&g_incr_test_force_legacy_partial, true);
}

cbm_incremental_route_t cbm_pipeline_incremental_test_last_route(void) {
    return (cbm_incremental_route_t)atomic_load(&g_incr_test_last_route);
}

void cbm_pipeline_incremental_test_reset_faults(void) {
    atomic_store(&g_incr_test_fail_result_cache_alloc, false);
    atomic_store(&g_incr_test_force_legacy_partial, false);
    atomic_store(&g_incr_test_last_route, CBM_INCREMENTAL_ROUTE_NONE);
    cbm_pipeline_persist_test_reset_faults();
}

static bool incr_test_take_result_cache_alloc_failure(void) {
    return atomic_exchange(&g_incr_test_fail_result_cache_alloc, false);
}

static bool incr_test_take_force_legacy_partial(void) {
    return atomic_exchange(&g_incr_test_force_legacy_partial, false);
}

static void incr_test_set_last_route(cbm_incremental_route_t route) {
    atomic_store(&g_incr_test_last_route, route);
}
#endif

/* ── Timing helper (same as pipeline.c) ──────────────────────────── */

static double elapsed_ms(struct timespec start) {
    struct timespec now;
    cbm_clock_gettime(CLOCK_MONOTONIC, &now);
    double s = (double)(now.tv_sec - start.tv_sec);
    double ns = (double)(now.tv_nsec - start.tv_nsec);
    return (s * CBM_MS_PER_SEC) + (ns / CBM_NS_PER_MS);
}

/* itoa into static buffer — matches pipeline.c helper */
static const char *itoa_buf(int v) {
    static _Thread_local char buf[INCR_RING_BUF][INCR_TS_BUF];
    static _Thread_local int idx = 0;
    idx = (idx + SKIP_ONE) & INCR_RING_MASK;
    snprintf(buf[idx], sizeof(buf[idx]), "%d", v);
    return buf[idx];
}

/* ── Platform-portable mtime_ns ──────────────────────────────────── */

static int64_t stat_mtime_ns(const struct stat *st) {
#ifdef __APPLE__
    return ((int64_t)st->st_mtimespec.tv_sec * CBM_NS_PER_SEC) + (int64_t)st->st_mtimespec.tv_nsec;
#elif defined(_WIN32)
    return (int64_t)st->st_mtime * CBM_NS_PER_SEC;
#else
    return ((int64_t)st->st_mtim.tv_sec * CBM_NS_PER_SEC) + (int64_t)st->st_mtim.tv_nsec;
#endif
}

static const char *incr_mode_name(int mode) {
    switch (mode) {
    case CBM_MODE_FULL:
        return "full";
    case CBM_MODE_MODERATE:
        return "moderate";
    case CBM_MODE_FAST:
        return "fast";
    default:
        return "unknown";
    }
}

/* ── Exact semantic manifest ───────────────────────────────────── */

typedef struct {
    cbm_file_hash_t *items;
    int count;
    int cap;
    CBMHashTable *seen_paths;
} semantic_manifest_builder_t;

static int semantic_manifest_cmp(const void *a, const void *b) {
    const cbm_file_hash_t *ma = a;
    const cbm_file_hash_t *mb = b;
    return strcmp(ma->rel_path, mb->rel_path);
}

static bool semantic_manifest_is_virtual_path(const char *rel_path) {
    return rel_path &&
           strncmp(rel_path, CBM_SEMANTIC_INPUT_PREFIX, strlen(CBM_SEMANTIC_INPUT_PREFIX)) == 0;
}

void cbm_pipeline_free_semantic_manifest(cbm_file_hash_t *manifest, int count) {
    if (!manifest) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free((void *)manifest[i].project);
        free((void *)manifest[i].rel_path);
        free((void *)manifest[i].sha256);
    }
    free(manifest);
}

/* Hash a stable file generation. A concurrent edit is retried once; a second
 * race fails closed so a mixed-generation manifest can never be published. */
static int semantic_manifest_hash_file(const char *abs_path, char out[CBM_SHA256_HEX_LEN + 1],
                                       int64_t *mtime_ns, int64_t *size) {
    for (int attempt = 0; attempt < 2; attempt++) {
        cbm_path_info_t before;
        if (cbm_path_info_utf8(abs_path, &before) != 0 || !before.is_regular || before.is_symlink) {
            return CBM_NOT_FOUND;
        }
        FILE *f = cbm_fopen(abs_path, "rb");
        if (!f) {
            return CBM_NOT_FOUND;
        }
        cbm_sha256_ctx sha;
        cbm_sha256_init(&sha);
        unsigned char buf[CBM_SZ_64K];
        size_t n;
        while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
            cbm_sha256_update(&sha, buf, n);
        }
        bool read_ok = !ferror(f);
        if (fclose(f) != 0) {
            read_ok = false;
        }
        cbm_path_info_t after;
        if (!read_ok || cbm_path_info_utf8(abs_path, &after) != 0 || !after.is_regular ||
            after.is_symlink) {
            return CBM_NOT_FOUND;
        }
        if (before.size != after.size || before.mtime_ns != after.mtime_ns) {
            continue;
        }
        uint8_t digest[CBM_SHA256_DIGEST_LEN];
        cbm_sha256_final(&sha, digest);
        static const char hex[] = "0123456789abcdef";
        for (int i = 0; i < CBM_SHA256_DIGEST_LEN; i++) {
            out[i * 2] = hex[digest[i] >> 4];
            out[i * 2 + 1] = hex[digest[i] & 0x0f];
        }
        out[CBM_SHA256_HEX_LEN] = '\0';
        *mtime_ns = after.mtime_ns;
        *size = after.size;
        return 0;
    }
    cbm_log_warn("semantic_manifest.raced", "path", abs_path);
    return CBM_NOT_FOUND;
}

static int semantic_manifest_add_digest(semantic_manifest_builder_t *builder, const char *project,
                                        const char *rel_path, const char *sha, int64_t mtime_ns,
                                        int64_t size) {
    if (!builder || !project || !rel_path || !rel_path[0] || !sha ||
        strlen(sha) != CBM_SHA256_HEX_LEN || cbm_ht_has(builder->seen_paths, rel_path)) {
        return CBM_NOT_FOUND;
    }
    for (int i = 0; i < CBM_SHA256_HEX_LEN; i++) {
        if (!((sha[i] >= '0' && sha[i] <= '9') || (sha[i] >= 'a' && sha[i] <= 'f'))) {
            return CBM_NOT_FOUND;
        }
    }
    if (builder->count == builder->cap) {
        int new_cap = builder->cap > 0 ? builder->cap * 2 : CBM_SZ_64;
        cbm_file_hash_t *grown = realloc(builder->items, (size_t)new_cap * sizeof(*builder->items));
        if (!grown) {
            return CBM_NOT_FOUND;
        }
        builder->items = grown;
        builder->cap = new_cap;
    }
    char *project_copy = strdup(project);
    char *path_copy = strdup(rel_path);
    char *sha_copy = strdup(sha);
    if (!project_copy || !path_copy || !sha_copy) {
        free(project_copy);
        free(path_copy);
        free(sha_copy);
        return CBM_NOT_FOUND;
    }
    uint32_t before_count = cbm_ht_count(builder->seen_paths);
    cbm_ht_set(builder->seen_paths, path_copy, path_copy);
    if (cbm_ht_count(builder->seen_paths) != before_count + 1U) {
        free(project_copy);
        free(path_copy);
        free(sha_copy);
        return CBM_NOT_FOUND;
    }
    builder->items[builder->count++] = (cbm_file_hash_t){
        .project = project_copy,
        .rel_path = path_copy,
        .sha256 = sha_copy,
        .mtime_ns = mtime_ns,
        .size = size,
    };
    return 0;
}

static int semantic_manifest_add(semantic_manifest_builder_t *builder, const char *project,
                                 const char *rel_path, const char *abs_path) {
    if (!rel_path || !rel_path[0]) {
        return 0;
    }
    /* Reserved rows are synthetic and can never be supplied by discovery. */
    if (semantic_manifest_is_virtual_path(rel_path)) {
        return CBM_NOT_FOUND;
    }
    if (cbm_ht_has(builder->seen_paths, rel_path)) {
        return 0;
    }
    char sha[CBM_SHA256_HEX_LEN + 1];
    int64_t mtime_ns = 0;
    int64_t size = 0;
    if (semantic_manifest_hash_file(abs_path, sha, &mtime_ns, &size) != 0) {
        cbm_log_error("semantic_manifest.err", "path", abs_path);
        return CBM_NOT_FOUND;
    }
    return semantic_manifest_add_digest(builder, project, rel_path, sha, mtime_ns, size);
}

static void semantic_manifest_git_digest(const cbm_git_context_t *git_ctx,
                                         char out[CBM_SHA256_HEX_LEN + 1]) {
    static const char domain[] = "cbm-semantic-git-context-v1";
    cbm_sha256_ctx sha;
    cbm_sha256_init(&sha);
    cbm_sha256_update(&sha, domain, sizeof(domain));
#define HASH_GIT_BOOL(field)                                      \
    do {                                                          \
        const char value = git_ctx && git_ctx->field ? '1' : '0'; \
        cbm_sha256_update(&sha, #field, sizeof(#field));          \
        cbm_sha256_update(&sha, &value, sizeof(value));           \
    } while (0)
#define HASH_GIT_STRING(field)                                               \
    do {                                                                     \
        const char *value = git_ctx && git_ctx->field ? git_ctx->field : ""; \
        cbm_sha256_update(&sha, #field, sizeof(#field));                     \
        cbm_sha256_update(&sha, value, strlen(value) + 1);                   \
    } while (0)
    HASH_GIT_BOOL(is_git);
    HASH_GIT_BOOL(is_worktree);
    HASH_GIT_BOOL(is_detached);
    HASH_GIT_BOOL(root_exists);
    HASH_GIT_STRING(branch);
    HASH_GIT_STRING(branch_slug);
    HASH_GIT_STRING(head_sha);
    HASH_GIT_STRING(base_sha);
#undef HASH_GIT_BOOL
#undef HASH_GIT_STRING
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&sha, digest);
    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < CBM_SHA256_DIGEST_LEN; i++) {
        out[i * 2] = hex[digest[i] >> 4];
        out[i * 2 + 1] = hex[digest[i] & 0x0f];
    }
    out[CBM_SHA256_HEX_LEN] = '\0';
}

static bool semantic_manifest_package_control(const char *name) {
    if (!name) {
        return false;
    }
    size_t len = strlen(name);
    return strcmp(name, "package.json") == 0 || strcmp(name, "go.mod") == 0 ||
           strcmp(name, "Cargo.toml") == 0 || strcmp(name, "pyproject.toml") == 0 ||
           strcmp(name, "composer.json") == 0 || strcmp(name, "pubspec.yaml") == 0 ||
           strcmp(name, "pom.xml") == 0 || strcmp(name, "build.gradle") == 0 ||
           strcmp(name, "build.gradle.kts") == 0 || strcmp(name, "mix.exs") == 0 ||
           (len >= 8 && strcmp(name + len - 8, ".gemspec") == 0);
}

static int semantic_manifest_walk_controls(semantic_manifest_builder_t *builder,
                                           const char *project, const char *abs_dir,
                                           const char *rel_dir, int depth, char **excluded_dirs,
                                           int excluded_count) {
    enum { MANIFEST_WALK_MAX_DEPTH = 64 };
    if (depth >= MANIFEST_WALK_MAX_DEPTH) {
        return 0;
    }
    cbm_dir_t *dir = cbm_opendir(abs_dir);
    if (!dir) {
        return CBM_NOT_FOUND;
    }
    int rc = 0;
    cbm_dirent_t *entry;
    while (rc == 0 && (entry = cbm_readdir(dir)) != NULL) {
        const char *name = entry->name;
        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
            continue;
        }
        char abs_path[CBM_SZ_4K];
        char rel_path[CBM_SZ_4K];
        int abs_n = snprintf(abs_path, sizeof(abs_path), "%s/%s", abs_dir, name);
        int rel_n = rel_dir && rel_dir[0]
                        ? snprintf(rel_path, sizeof(rel_path), "%s/%s", rel_dir, name)
                        : snprintf(rel_path, sizeof(rel_path), "%s", name);
        if (abs_n < 0 || abs_n >= (int)sizeof(abs_path) || rel_n < 0 ||
            rel_n >= (int)sizeof(rel_path)) {
            rc = CBM_NOT_FOUND;
            break;
        }
        cbm_path_info_t path_info;
        if (cbm_path_info_utf8(abs_path, &path_info) != 0) {
            rc = CBM_NOT_FOUND;
            break;
        }
        if (path_info.is_symlink) {
            continue;
        }
        if (path_info.is_directory) {
            if (cbm_should_skip_dir(name, CBM_MODE_FULL) ||
                cbm_pipeline_relpath_is_excluded(rel_path, excluded_dirs, excluded_count)) {
                continue;
            }
            rc = semantic_manifest_walk_controls(builder, project, abs_path, rel_path, depth + 1,
                                                 excluded_dirs, excluded_count);
            continue;
        }
        bool root_control =
            (!rel_dir || !rel_dir[0]) &&
            (strcmp(name, ".cbmignore") == 0 || strcmp(name, ".codebase-memory.json") == 0);
        if (path_info.is_regular && (strcmp(name, ".gitignore") == 0 || root_control ||
                                     semantic_manifest_package_control(name))) {
            rc = semantic_manifest_add(builder, project, rel_path, abs_path);
        }
    }
    cbm_closedir(dir);
    return rc;
}

/* Parallel pre-hash of the discovery list. Hashing dominates manifest cost
 * (tens of thousands of file reads; single-threaded it was the second-
 * largest block of a kernel-scale incremental run after publication), and
 * semantic_manifest_hash_file is pure per-file work, so the files fan out
 * across workers while ASSEMBLY stays serial and in discovery order — the
 * manifest bytes are identical to the serial build's, only sooner. */
typedef struct {
    const cbm_file_info_t *files;
    int file_count;
    int worker_id;
    int worker_count;
    char (*shas)[CBM_SHA256_HEX_LEN + 1];
    int64_t *mtimes;
    int64_t *sizes;
    int8_t *rcs;
} manifest_hash_worker_t;

static void *manifest_hash_worker(void *arg) {
    manifest_hash_worker_t *w = (manifest_hash_worker_t *)arg;
    for (int i = w->worker_id; i < w->file_count; i += w->worker_count) {
        if (!w->files[i].rel_path || !w->files[i].rel_path[0]) {
            w->rcs[i] = 1; /* assembly skips exactly as the serial path did */
            continue;
        }
        w->rcs[i] = semantic_manifest_hash_file(w->files[i].path, w->shas[i], &w->mtimes[i],
                                                &w->sizes[i]) == 0
                        ? 0
                        : -1;
    }
    return NULL;
}

enum { MANIFEST_PARALLEL_MIN_FILES = 64 };

int cbm_pipeline_build_semantic_manifest(const char *project, const char *repo_path,
                                         const cbm_file_info_t *files, int file_count,
                                         char **excluded_dirs, int excluded_count,
                                         const cbm_git_context_t *git_ctx,
                                         const cbm_userconfig_t *userconfig, cbm_file_hash_t **out,
                                         int *out_count) {
    if (!project || !repo_path || !out || !out_count || file_count < 0 ||
        (file_count > 0 && !files)) {
        return CBM_NOT_FOUND;
    }
    *out = NULL;
    *out_count = 0;
    semantic_manifest_builder_t builder = {0};
    uint64_t reserve = (uint64_t)(file_count > 0 ? file_count : 0) * 2U + CBM_SZ_64;
    if (reserve > UINT32_MAX) {
        reserve = UINT32_MAX;
    }
    builder.seen_paths = cbm_ht_create((uint32_t)reserve);
    if (!builder.seen_paths) {
        return CBM_NOT_FOUND;
    }
    int rc = 0;
    char git_digest[CBM_SHA256_HEX_LEN + 1];
    char absent_config_digest[CBM_SHA256_HEX_LEN + 1];
    semantic_manifest_git_digest(git_ctx, git_digest);
    cbm_sha256_hex("cbm-userconfig-unavailable-v1", strlen("cbm-userconfig-unavailable-v1"),
                   absent_config_digest);
    const char *global_config_digest =
        userconfig ? userconfig->global_source_sha256 : absent_config_digest;
    const char *project_config_digest =
        userconfig ? userconfig->project_source_sha256 : absent_config_digest;
    rc = semantic_manifest_add_digest(&builder, project, CBM_SEMANTIC_INPUT_GIT_CONTEXT, git_digest,
                                      0, 0);
    if (rc == 0) {
        rc = semantic_manifest_add_digest(&builder, project, CBM_SEMANTIC_INPUT_GLOBAL_CONFIG,
                                          global_config_digest, 0, 0);
    }
    if (rc == 0) {
        rc = semantic_manifest_add_digest(&builder, project, CBM_SEMANTIC_INPUT_PROJECT_CONFIG,
                                          project_config_digest, 0, 0);
    }
    struct timespec t_hash;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t_hash);
    int hash_workers = cbm_default_worker_count(true);
    if (rc == 0 && file_count >= MANIFEST_PARALLEL_MIN_FILES && hash_workers > SKIP_ONE) {
        char (*shas)[CBM_SHA256_HEX_LEN + 1] = malloc((size_t)file_count * sizeof(*shas));
        int64_t *mtimes = calloc((size_t)file_count, sizeof(int64_t));
        int64_t *sizes = calloc((size_t)file_count, sizeof(int64_t));
        int8_t *rcs = calloc((size_t)file_count, sizeof(int8_t));
        cbm_thread_t *threads = calloc((size_t)hash_workers, sizeof(cbm_thread_t));
        manifest_hash_worker_t *workers = calloc((size_t)hash_workers, sizeof(*workers));
        if (shas && mtimes && sizes && rcs && threads && workers) {
            int spawned = 0;
            for (int w = 0; w < hash_workers; w++) {
                workers[w] = (manifest_hash_worker_t){.files = files,
                                                      .file_count = file_count,
                                                      .worker_id = w,
                                                      .worker_count = hash_workers,
                                                      .shas = shas,
                                                      .mtimes = mtimes,
                                                      .sizes = sizes,
                                                      .rcs = rcs};
                if (cbm_thread_create(&threads[w], 0, manifest_hash_worker, &workers[w]) != 0) {
                    break;
                }
                spawned++;
            }
            /* Workers that failed to spawn leave their stride to this
             * thread: run the remaining strides inline so every index is
             * covered exactly once. */
            for (int w = spawned; w < hash_workers; w++) {
                workers[w] = (manifest_hash_worker_t){.files = files,
                                                      .file_count = file_count,
                                                      .worker_id = w,
                                                      .worker_count = hash_workers,
                                                      .shas = shas,
                                                      .mtimes = mtimes,
                                                      .sizes = sizes,
                                                      .rcs = rcs};
                (void)manifest_hash_worker(&workers[w]);
            }
            for (int w = 0; w < spawned; w++) {
                cbm_thread_join(&threads[w]);
            }
            for (int i = 0; rc == 0 && i < file_count; i++) {
                if (rcs[i] == 1) {
                    continue;
                }
                if (rcs[i] != 0) {
                    cbm_log_error("semantic_manifest.err", "path", files[i].path);
                    rc = CBM_NOT_FOUND;
                    break;
                }
                if (cbm_ht_has(builder.seen_paths, files[i].rel_path)) {
                    continue;
                }
                rc = semantic_manifest_add_digest(&builder, project, files[i].rel_path, shas[i],
                                                  mtimes[i], sizes[i]);
            }
        } else {
            for (int i = 0; rc == 0 && i < file_count; i++) {
                rc = semantic_manifest_add(&builder, project, files[i].rel_path, files[i].path);
            }
        }
        free(shas);
        free(mtimes);
        free(sizes);
        free(rcs);
        free(threads);
        free(workers);
    } else {
        for (int i = 0; rc == 0 && i < file_count; i++) {
            rc = semantic_manifest_add(&builder, project, files[i].rel_path, files[i].path);
        }
    }
    cbm_log_info("pass.timing", "pass", "semantic_manifest_hash", "files", itoa_buf(file_count),
                 "workers", itoa_buf(hash_workers), "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t_hash)));
    if (rc == 0) {
        rc = semantic_manifest_walk_controls(&builder, project, repo_path, "", 0, excluded_dirs,
                                             excluded_count);
    }
    if (rc == 0) {
        cbm_path_alias_collection_t *aliases =
            cbm_load_path_aliases_excluded(repo_path, excluded_dirs, excluded_count);
        if (aliases) {
            for (int i = 0; rc == 0 && i < aliases->count; i++) {
                const char *rel = aliases->scopes[i].source_rel_path;
                if (!rel) {
                    continue;
                }
                char abs_path[CBM_SZ_4K];
                int n = snprintf(abs_path, sizeof(abs_path), "%s/%s", repo_path, rel);
                rc = (n < 0 || n >= (int)sizeof(abs_path))
                         ? CBM_NOT_FOUND
                         : semantic_manifest_add(&builder, project, rel, abs_path);
            }
            cbm_path_alias_collection_free(aliases);
        }
    }
    if (rc != 0) {
        cbm_ht_free(builder.seen_paths);
        cbm_pipeline_free_semantic_manifest(builder.items, builder.count);
        return rc;
    }
    cbm_ht_free(builder.seen_paths);
    qsort(builder.items, (size_t)builder.count, sizeof(*builder.items), semantic_manifest_cmp);
    *out = builder.items;
    *out_count = builder.count;
    return 0;
}

bool cbm_pipeline_semantic_manifests_equal(const cbm_file_hash_t *left, int left_count,
                                           const cbm_file_hash_t *right, int right_count) {
    if (left_count < 0 || right_count < 0 || left_count != right_count ||
        (left_count > 0 && (!left || !right))) {
        return false;
    }
    uint64_t reserve = (uint64_t)(right_count > 0 ? right_count : 0) * 2U + CBM_SZ_64;
    if (reserve > UINT32_MAX) {
        reserve = UINT32_MAX;
    }
    CBMHashTable *by_path = cbm_ht_create((uint32_t)reserve);
    if (!by_path) {
        return false;
    }
    bool equal = true;
    for (int i = 0; equal && i < right_count; i++) {
        if (!right[i].rel_path || !right[i].sha256 || cbm_ht_has(by_path, right[i].rel_path)) {
            equal = false;
            break;
        }
        uint32_t before = cbm_ht_count(by_path);
        cbm_ht_set(by_path, right[i].rel_path, (void *)right[i].sha256);
        equal = cbm_ht_count(by_path) == before + 1U;
    }
    for (int i = 0; equal && i < left_count; i++) {
        if (!left[i].rel_path || !left[i].sha256) {
            equal = false;
            break;
        }
        const char *sha = cbm_ht_get(by_path, left[i].rel_path);
        equal = sha && strcmp(sha, left[i].sha256) == 0;
    }
    cbm_ht_free(by_path);
    return equal;
}

int cbm_pipeline_build_fresh_semantic_manifest(const char *project, const char *repo_path, int mode,
                                               cbm_file_hash_t **out, int *out_count) {
    if (!project || !repo_path || !out || !out_count) {
        return CBM_NOT_FOUND;
    }
    *out = NULL;
    *out_count = 0;
    cbm_discover_opts_t opts = {
        .mode = (cbm_index_mode_t)mode,
        .ignore_file = NULL,
        .max_file_size = 0,
    };
    cbm_file_info_t *fresh_files = NULL;
    int fresh_file_count = 0;
    char **fresh_excluded = NULL;
    int fresh_excluded_count = 0;
    cbm_ignored_file_t *fresh_ignored = NULL;
    int fresh_ignored_count = 0;
    int fresh_ignored_total = 0;
    cbm_userconfig_t *fresh_userconfig = cbm_userconfig_load(repo_path);
    cbm_git_context_t fresh_git_ctx = {0};
    const cbm_userconfig_t *previous_userconfig = cbm_get_user_lang_config();
    int rc = fresh_userconfig ? cbm_git_context_resolve(repo_path, &fresh_git_ctx) : CBM_NOT_FOUND;
    if (rc == 0) {
        cbm_set_user_lang_config(fresh_userconfig);
        rc = cbm_discover_ex2(repo_path, &opts, &fresh_files, &fresh_file_count, &fresh_excluded,
                              &fresh_excluded_count, &fresh_ignored, &fresh_ignored_count,
                              &fresh_ignored_total);
    }
    if (rc == 0) {
        rc = cbm_pipeline_build_semantic_manifest(project, repo_path, fresh_files, fresh_file_count,
                                                  fresh_excluded, fresh_excluded_count,
                                                  &fresh_git_ctx, fresh_userconfig, out, out_count);
    }
    cbm_set_user_lang_config(previous_userconfig);
    cbm_git_context_free(&fresh_git_ctx);
    cbm_userconfig_free(fresh_userconfig);
    cbm_discover_free(fresh_files, fresh_file_count);
    cbm_discover_free_excluded(fresh_excluded, fresh_excluded_count);
    cbm_discover_free_ignored(fresh_ignored, fresh_ignored_count);
    return rc;
}

/* ── File classification ─────────────────────────────────────────── */

/* Classify discovered files against stored hashes using mtime+size.
 * Returns a boolean array: changed[i] = true if files[i] needs re-parsing.
 * Caller must free the returned array. */
static bool *classify_files(cbm_file_info_t *files, int file_count, cbm_file_hash_t *stored,
                            int stored_count, int *out_changed, int *out_unchanged) {
    bool *changed = calloc((size_t)file_count, sizeof(bool));
    if (!changed) {
        return NULL;
    }

    int n_changed = 0;
    int n_unchanged = 0;

    /* Build lookup: rel_path -> stored hash */
    CBMHashTable *ht =
        cbm_ht_create(stored_count > 0 ? (size_t)stored_count * PAIR_LEN : CBM_SZ_64);
    for (int i = 0; i < stored_count; i++) {
        cbm_ht_set(ht, stored[i].rel_path, &stored[i]);
    }

    for (int i = 0; i < file_count; i++) {
        cbm_file_hash_t *h = cbm_ht_get(ht, files[i].rel_path);
        if (!h) {
            /* New file */
            changed[i] = true;
            n_changed++;
            continue;
        }

        struct stat st;
        if (stat(files[i].path, &st) != 0) {
            changed[i] = true;
            n_changed++;
            continue;
        }

        if (stat_mtime_ns(&st) != h->mtime_ns || st.st_size != h->size) {
            changed[i] = true;
            n_changed++;
        } else {
            n_unchanged++;
        }
    }

    cbm_ht_free(ht);
    *out_changed = n_changed;
    *out_unchanged = n_unchanged;
    return changed;
}

/* Classify stored files that are absent from current discovery. Returns the
 * count of truly-deleted files (output via out_deleted) and ALSO collects
 * mode-skipped files into out_mode_skipped (caller frees both).
 *
 * A stored file is classified as:
 *   - "deleted"      — `stat()` returns ENOENT or ENOTDIR. Its nodes will
 *                       be purged and its hash row dropped.
 *   - "mode-skipped" — `stat()` succeeds. The file exists on disk but the
 *                       current discovery pass didn't visit it (e.g. excluded
 *                       by FAST_SKIP_DIRS in fast/moderate mode). Its nodes
 *                       must be preserved AND its hash row must be carried
 *                       forward into the new DB so subsequent reindexes can
 *                       still see it as "known" rather than treating it as
 *                       new-or-deleted.
 *
 * Without this distinction, a fast-mode reindex after a full-mode index
 * would silently purge every file under `tools/`, `scripts/`, `bin/`,
 * `build/`, `docs/`, `__tests__/`, etc. — see task
 * claude-connectors/codebase-memory-index-repository-is-destructive-...
 * and the 2026-04-13 Skyline incident (packages/mcp/src/tools/ vanished
 * from a live graph mid-session).
 *
 * Mode-skipped hash preservation is the second half of the additive-merge
 * contract: dump_and_persist re-upserts these hash rows so the next reindex
 * can correctly detect a real on-disk deletion of a mode-skipped file (as
 * opposed to seeing it as "never existed" → noop → orphaned graph nodes).
 *
 * Fail-safe rules (preserve nodes on uncertainty):
 *   - repo_path NULL → log error and preserve everything (return 0
 *     deletions, empty mode_skipped). The caller contract is that
 *     repo_path is required; a NULL means a misconfigured pipeline,
 *     not a deletion signal.
 *   - snprintf truncation (combined path ≥ CBM_SZ_4K) → preserve. We can't
 *     reliably stat a truncated path. Treat as mode-skipped.
 *   - stat() errno != ENOENT/ENOTDIR (EACCES, EIO, ELOOP, transient NFS,
 *     etc.) → preserve. The file may exist; we just can't see it right now.
 *     Treat as mode-skipped.
 *
 * Note: we use stat() (not lstat()) on purpose. A symlink whose target was
 * deleted should be classified as deleted from the indexer's perspective
 * because the indexer follows symlinks during discovery — a stale symlink
 * has no source to parse. */
static int find_deleted_files(const char *repo_path, cbm_file_info_t *files, int file_count,
                              cbm_file_hash_t *stored, int stored_count, char ***out_deleted,
                              cbm_file_hash_t **out_mode_skipped, int *out_mode_skipped_count) {
    *out_deleted = NULL;
    *out_mode_skipped = NULL;
    *out_mode_skipped_count = 0;

    if (!repo_path) {
        /* Misconfigured pipeline. Preserve everything rather than risk
         * silently re-introducing the destructive overwrite this function
         * was rewritten to prevent. */
        cbm_log_error("incremental.err", "msg", "find_deleted_files_null_repo_path");
        return 0;
    }

    CBMHashTable *current = cbm_ht_create((size_t)file_count * PAIR_LEN);
    for (int i = 0; i < file_count; i++) {
        cbm_ht_set(current, files[i].rel_path, &files[i]);
    }

    int del_count = 0;
    int del_cap = CBM_SZ_64;
    char **deleted = malloc((size_t)del_cap * sizeof(char *));
    if (!deleted) {
        cbm_log_error("incremental.err", "msg", "find_deleted_files_oom");
        cbm_ht_free(current);
        return 0;
    }

    int ms_count = 0;
    int ms_cap = CBM_SZ_64;
    cbm_file_hash_t *mode_skipped = malloc((size_t)ms_cap * sizeof(cbm_file_hash_t));
    if (!mode_skipped) {
        cbm_log_error("incremental.err", "msg", "find_deleted_files_oom_ms");
        free(deleted);
        cbm_ht_free(current);
        return 0;
    }

    for (int i = 0; i < stored_count; i++) {
        if (semantic_manifest_is_virtual_path(stored[i].rel_path)) {
            continue; /* synthetic semantic inputs are not repository files */
        }
        if (cbm_ht_get(current, stored[i].rel_path)) {
            continue; /* still visited by current pass */
        }
        /* Not in current discovery — check if it's truly deleted or just
         * mode-skipped (excluded by FAST_SKIP_DIRS etc.). */
        bool preserve = false;
        char abs_path[CBM_SZ_4K];
        int n = snprintf(abs_path, sizeof(abs_path), "%s/%s", repo_path, stored[i].rel_path);
        if (n < 0 || n >= (int)sizeof(abs_path)) {
            /* Truncation or encoding error — can't reliably stat. Preserve. */
            cbm_log_warn("incremental.path_truncated", "rel_path", stored[i].rel_path);
            preserve = true;
        } else {
            struct stat st;
            if (stat(abs_path, &st) == 0) {
                /* File exists on disk — mode-skipped, not deleted. */
                preserve = true;
            } else if (errno != ENOENT && errno != ENOTDIR) {
                /* Transient or permission error — fail safe by preserving.
                 * EACCES, EIO, ELOOP, ENAMETOOLONG, etc. */
                cbm_log_warn("incremental.stat_uncertain", "rel_path", stored[i].rel_path, "errno",
                             itoa_buf(errno));
                preserve = true;
            }
        }

        if (preserve) {
            /* Carry forward the existing hash row so subsequent reindexes
             * can correctly classify this file. */
            if (ms_count >= ms_cap) {
                ms_cap *= PAIR_LEN;
                cbm_file_hash_t *tmp = realloc(mode_skipped, (size_t)ms_cap * sizeof(*tmp));
                if (!tmp) {
                    cbm_log_error("incremental.err", "msg", "find_deleted_files_realloc_oom_ms");
                    break;
                }
                mode_skipped = tmp;
            }
            char *rp = strdup(stored[i].rel_path);
            char *sh = stored[i].sha256 ? strdup(stored[i].sha256) : NULL;
            if (!rp || (stored[i].sha256 && !sh)) {
                /* OOM mid-record. Drop this entry rather than persist a
                 * row with a NULL rel_path that would silently fail the
                 * NOT NULL constraint in upsert and reintroduce the
                 * orphaned-node bug. */
                cbm_log_error("incremental.err", "msg", "find_deleted_files_strdup_oom", "rel_path",
                              stored[i].rel_path);
                free(rp);
                free(sh);
                break;
            }
            mode_skipped[ms_count].project = NULL; /* unused by upsert API */
            mode_skipped[ms_count].rel_path = rp;
            mode_skipped[ms_count].sha256 = sh;
            mode_skipped[ms_count].mtime_ns = stored[i].mtime_ns;
            mode_skipped[ms_count].size = stored[i].size;
            ms_count++;
            continue;
        }

        /* File is truly gone — record for purge. */
        if (del_count >= del_cap) {
            del_cap *= PAIR_LEN;
            char **tmp = realloc(deleted, (size_t)del_cap * sizeof(char *));
            if (!tmp) {
                cbm_log_error("incremental.err", "msg", "find_deleted_files_realloc_oom");
                break;
            }
            deleted = tmp;
        }
        deleted[del_count++] = strdup(stored[i].rel_path);
    }

    cbm_ht_free(current);
    *out_deleted = deleted;
    *out_mode_skipped = mode_skipped;
    *out_mode_skipped_count = ms_count;
    return del_count;
}

/* Free a mode_skipped array allocated by find_deleted_files. */
static void free_mode_skipped(cbm_file_hash_t *ms, int count) {
    if (!ms) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free((void *)ms[i].rel_path);
        free((void *)ms[i].sha256);
    }
    free(ms);
}

/* ── Inbound cross-file edge preservation (incremental correctness) ──
 *
 * The purge step (cbm_gbuf_delete_by_file) removes a changed file's nodes,
 * and the cascade then drops every edge referencing them — INCLUDING inbound
 * edges whose source lives in an UNCHANGED file (e.g. StudyService.grade ->
 * SM2.review, or a Folder -> File containment edge). Because incremental only
 * re-parses the changed files, the resolution passes never regenerate those
 * inbound edges, so the graph silently loses cross-file CALLS / CALL_REFERENCE / USAGE /
 * CONTAINS_FILE / INHERITS / ... edges on every edit and diverges from a
 * clean full reindex (which resolves every file).
 *
 * Fix: snapshot the inbound cross-file edges into changed files BEFORE the
 * purge, keyed by endpoint qualified_name (stable across re-parse), then
 * re-link them AFTER re-resolution + post-passes. Notes:
 *   - Only edges whose target is in a changed file and whose source is NOT
 *     are snapshotted; edges out of a changed file are regenerated when that
 *     file is re-resolved.
 *   - Edge types recomputed wholesale by post-passes (SIMILAR_TO,
 *     SEMANTICALLY_RELATED) are skipped — re-linking a stale snapshot could
 *     add edges a full reindex would not produce.
 *   - cbm_gbuf_insert_edge dedups, so re-linking an edge the resolver already
 *     recreated is a harmless no-op.
 *   - A target whose qualified_name no longer exists (symbol deleted or
 *     renamed by the edit) is dropped — matching full-reindex semantics. */

typedef struct {
    char *source_qn;
    char *target_qn;
    char *type;
    char *props;
} cbm_saved_edge_t;

typedef struct {
    cbm_gbuf_t *gbuf;
    CBMHashTable *changed_paths; /* rel_path -> non-NULL sentinel (membership set) */
    cbm_saved_edge_t *items;
    int count;
    int cap;
} cbm_edge_capture_t;

/* Edge types that must NOT be re-linked from the pre-purge snapshot, because a
 * full reindex (re)computes them via a pass whose result can differ from the
 * snapshot — restoring a stale copy could leave wrong properties or even an
 * edge a full reindex would not produce:
 *   - SIMILAR_TO / SEMANTICALLY_RELATED: rebuilt wholesale by the incremental
 *     post-passes (similarity / semantic_edges) over a drifting corpus.
 *   - FILE_CHANGES_WITH (git-history coupling) and DATA_FLOWS (route data flow):
 *     produced only by full-pipeline post-passes (githistory / route_nodes)
 *     that do NOT run during incremental; they remain a known incremental
 *     limitation rather than something to restore stale.
 * Every other edge type IS safe to re-link, by one of two routes that both
 * match a full reindex: edges re-emitted by the per-file resolution passes that
 * run incrementally (CALLS, CALL_REFERENCE, USAGE, DEFINES, DEFINES_METHOD, INHERITS,
 * IMPLEMENTS) are deduped on re-link, while structural containment edges
 * (CONTAINS_FILE, CONTAINS_FOLDER) — which the full-only structure pass does
 * NOT regenerate incrementally — are preserved precisely by this snapshot. */
static bool incr_edge_type_is_recomputed(const char *type) {
    return type && (strcmp(type, "SIMILAR_TO") == 0 || strcmp(type, "SEMANTICALLY_RELATED") == 0 ||
                    strcmp(type, "FILE_CHANGES_WITH") == 0 || strcmp(type, "DATA_FLOWS") == 0);
}

/* cbm_gbuf_foreach_edge visitor: snapshot inbound cross-file edges into
 * changed files so they survive the purge and can be re-linked afterward. */
static void incr_capture_inbound_edge(const cbm_gbuf_edge_t *edge, void *userdata) {
    cbm_edge_capture_t *cap = (cbm_edge_capture_t *)userdata;
    if (incr_edge_type_is_recomputed(edge->type)) {
        return;
    }
    const cbm_gbuf_node_t *src = cbm_gbuf_find_by_id(cap->gbuf, edge->source_id);
    const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_id(cap->gbuf, edge->target_id);
    if (!src || !tgt || !src->qualified_name || !tgt->qualified_name || !src->file_path ||
        !tgt->file_path) {
        return;
    }
    /* Keep only edges that the purge would orphan permanently: target is in a
     * changed file (its node is deleted + re-created), source is NOT (its file
     * is never re-parsed, so the resolver won't regenerate the edge). */
    if (!cbm_ht_get(cap->changed_paths, tgt->file_path) ||
        cbm_ht_get(cap->changed_paths, src->file_path)) {
        return;
    }
    if (cap->count >= cap->cap) {
        int ncap = (cap->cap > 0) ? cap->cap * PAIR_LEN : CBM_SZ_64;
        cbm_saved_edge_t *tmp = realloc(cap->items, (size_t)ncap * sizeof(*tmp));
        if (!tmp) {
            cbm_log_warn("incremental.edge_snapshot_oom", "captured", itoa_buf(cap->count));
            return; /* best-effort: stop capturing, keep what we have */
        }
        cap->items = tmp;
        cap->cap = ncap;
    }
    cbm_saved_edge_t *s = &cap->items[cap->count];
    s->source_qn = strdup(src->qualified_name);
    s->target_qn = strdup(tgt->qualified_name);
    s->type = strdup(edge->type);
    s->props = strdup(edge->properties_json ? edge->properties_json : "{}");
    if (!s->source_qn || !s->target_qn || !s->type || !s->props) {
        free(s->source_qn);
        free(s->target_qn);
        free(s->type);
        free(s->props);
        return;
    }
    cap->count++;
}

/* Re-link snapshotted inbound edges to the freshly re-created target nodes.
 * Returns the number of edges re-linked. */
static int incr_restore_inbound_edges(cbm_gbuf_t *gbuf, cbm_edge_capture_t *cap) {
    int restored = 0;
    for (int i = 0; i < cap->count; i++) {
        cbm_saved_edge_t *s = &cap->items[i];
        const cbm_gbuf_node_t *src = cbm_gbuf_find_by_qn(gbuf, s->source_qn);
        const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_qn(gbuf, s->target_qn);
        if (src && tgt) {
            cbm_gbuf_insert_edge(gbuf, src->id, tgt->id, s->type, s->props);
            restored++;
        }
    }
    return restored;
}

static void incr_free_edge_capture(cbm_edge_capture_t *cap) {
    for (int i = 0; i < cap->count; i++) {
        free(cap->items[i].source_qn);
        free(cap->items[i].target_qn);
        free(cap->items[i].type);
        free(cap->items[i].props);
    }
    free(cap->items);
    cap->items = NULL;
    cap->count = 0;
    cap->cap = 0;
}

/* ── Persist file hashes ─────────────────────────────────────────── */

/* ── Registry seed visitor ────────────────────────────────────────── */

/* Labels the full-index definition pass seeds into the registry. Incremental
 * re-resolution must see the SAME symbol set, or it diverges from a clean full
 * reindex: seeding extra container nodes (File / Module / Folder / ...) lets a
 * type usage like `Word` resolve to the same-named Module node instead of the
 * Class node. Membership is defined once by cbm_label_is_registry_symbol
 * (helpers.c) — the same predicate pass_definitions.c / pass_parallel.c seed
 * through, so divergence is impossible by construction. */
static bool incr_label_is_registry_symbol(const char *label) {
    return cbm_label_is_registry_symbol(label);
}

/* Callback for cbm_gbuf_foreach_node: seed the registry with the existing
 * project's definition symbols so the resolver can match cross-file symbols
 * during incremental. Mirrors the full-index registry contents exactly so an
 * incremental re-resolve picks the same nodes a full reindex would. */
static void registry_visitor(const cbm_gbuf_node_t *node, void *userdata) {
    cbm_registry_t *r = (cbm_registry_t *)userdata;
    if (!incr_label_is_registry_symbol(node->label)) {
        return;
    }
    cbm_registry_add(r, node->name, node->qualified_name, node->label);
}

static void free_incremental_result_cache(CBMFileResult **cache, int count) {
    if (!cache) {
        return;
    }
    for (int i = 0; i < count; i++) {
        cbm_free_result(cache[i]);
    }
    free(cache);
}

/* ── Closure repair (reverse-dependency incremental) ─────────────
 *
 * Routing model: the semantic manifest names WHAT changed; the previous
 * generation's graph names WHO consumed it. A body edit leaves a file's
 * persisted LSP surface byte-identical, so only the file itself is
 * recomputed; a surface change pulls in exactly the files with edges into
 * it. Per-file extraction is a pure function of file content, so an
 * unchanged dependent can never be surface-changed in turn — the closure is
 * structurally depth-1 and needs no fixpoint. Every uncertain case declines
 * to a FULL rebuild: new files, added definition names (yesterday's graph
 * cannot know tomorrow's referencers), virtual/config manifest entries,
 * missing or unreadable surface rows, dependents outside discovery, or a
 * closure above the budget. Declining is never wrong — it is exactly
 * today's behaviour. */

enum {
    CLOSURE_BUDGET_PERCENT = 30,
    /* A percentage-only budget starves small repositories (1 changed file in
     * a 3-file project is 33%). Closures at or under this absolute size are
     * always worth attempting; the percentage governs at scale, where it is
     * the honest signal that a rebuild costs less than a repair. */
    CLOSURE_BUDGET_FLOOR_FILES = 8,
};

typedef struct {
    /* Deserialized surfaces of the files NOT being re-parsed, registered
     * into the cross registries exactly as a full build would register
     * their fresh parses. Strings live in `arena`. */
    CBMLSPDef *base_defs;
    int base_def_count;
    CBMArena arena;
    /* Filled by run_extract_resolve: the re-parsed files' new surface rows
     * (heap; ownership passes to the publish merge) and the module strings
     * resolve borrowed (freed after publish). */
    cbm_lsp_surface_row_t *fresh_rows;
    int fresh_count;
    char **def_modules;
    int def_module_count;
    /* Parallel rehydration: per-worker arenas owning the deserialized
     * def strings. The resolvers and the patch borrow those strings, so
     * the arenas live exactly as long as cr.arena itself. */
    CBMArena *rehydrate_arenas;
    int rehydrate_arena_count;
} closure_resolve_t;

typedef struct {
    cbm_file_info_t *files; /* heap array; entries borrow strings from the
                             * caller's discovery array */
    int count;
    int n_changed;
    int n_dependents;
    cbm_lsp_surface_row_t *stored_rows; /* whole previous generation */
    int stored_count;
} closure_plan_t;

static void closure_plan_free(closure_plan_t *plan) {
    free(plan->files);
    cbm_store_free_lsp_surfaces(plan->stored_rows, plan->stored_count);
    memset(plan, 0, sizeof(*plan));
}

/* Short names a surface JSON defines ("lsp"[].sn plus "reg"[].n), as a
 * heap-keyed set. Returns NULL on parse failure — callers decline to FULL. */
static CBMHashTable *surface_name_set(const char *defs_json) {
    yyjson_doc *doc = yyjson_read(defs_json, strlen(defs_json), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *lsp = root ? yyjson_obj_get(root, "lsp") : NULL;
    yyjson_val *reg = root ? yyjson_obj_get(root, "reg") : NULL;
    CBMHashTable *set = cbm_ht_create(CBM_SZ_64);
    if (!set) {
        yyjson_doc_free(doc);
        return NULL;
    }
    const char *arr_keys[] = {"sn", "n"};
    yyjson_val *arrs[] = {lsp, reg};
    for (int a = 0; a < 2; a++) {
        if (!arrs[a] || !yyjson_is_arr(arrs[a])) {
            continue;
        }
        size_t idx, max;
        yyjson_val *item;
        yyjson_arr_foreach(arrs[a], idx, max, item) {
            const char *name = yyjson_get_str(yyjson_obj_get(item, arr_keys[a]));
            if (name && name[0] && !cbm_ht_get(set, name)) {
                char *copy = strdup(name);
                if (copy) {
                    cbm_ht_set(set, copy, copy);
                }
            }
        }
    }
    yyjson_doc_free(doc);
    return set;
}

static void surface_name_free_visitor(const char *key, void *value, void *userdata) {
    (void)key;
    (void)userdata;
    free(value); /* value IS the heap key copy */
}

static void surface_name_set_free(CBMHashTable *set) {
    if (!set) {
        return;
    }
    cbm_ht_foreach(set, surface_name_free_visitor, NULL);
    cbm_ht_free(set);
}

typedef struct {
    CBMHashTable *stored_set;
    bool added;
} surface_added_check_t;

static void surface_added_visitor(const char *key, void *value, void *userdata) {
    (void)value;
    surface_added_check_t *check = (surface_added_check_t *)userdata;
    if (!check->added && !cbm_ht_get(check->stored_set, key)) {
        check->added = true;
    }
}

/* Does `fresh_json` define any short name absent from `stored_json`? An
 * added name can re-route references in files that have NO edge into this
 * file yet (call-the-helper-you-just-wrote, shadowing), which the
 * edge-derived dependent set cannot see — those cases go to FULL. */
static int surface_added_names(const char *stored_json, const char *fresh_json, bool *out_added) {
    *out_added = true; /* fail closed */
    CBMHashTable *stored_set = surface_name_set(stored_json);
    CBMHashTable *fresh_set = surface_name_set(fresh_json);
    int rc = (stored_set && fresh_set) ? 0 : -1;
    if (rc == 0) {
        surface_added_check_t check = {.stored_set = stored_set, .added = false};
        cbm_ht_foreach(fresh_set, surface_added_visitor, &check);
        *out_added = check.added;
    }
    surface_name_set_free(stored_set);
    surface_name_set_free(fresh_set);
    return rc;
}

/* Run parallel or sequential extract+resolve for changed files. Any failure
 * aborts before persistence: the caller discards this in-memory graph and
 * preserves the old on-disk database and its retryable hashes. */
static int run_extract_resolve(cbm_pipeline_ctx_t *ctx, cbm_file_info_t *changed_files, int ci,
                               closure_resolve_t *closure) {
    struct timespec t;

    /* Per-file LSP always runs (every mode). Cross-file LSP: the legacy
     * partial route still skips it (NULL cross_registries below); the
     * closure-repair route rebuilds the registries from persisted surfaces +
     * this run's fresh parses and resolves with them — the same Tier-2 path
     * a full build takes, which is what makes its output converge. */

#define MIN_FILES_FOR_PARALLEL_INCR 50
    int worker_count = cbm_default_worker_count(true);
    bool use_parallel =
        closure != NULL || (worker_count > SKIP_ONE && ci > MIN_FILES_FOR_PARALLEL_INCR);

    if (use_parallel) {
        cbm_log_info("incremental.mode", "mode", "parallel", "workers", itoa_buf(worker_count),
                     "changed", itoa_buf(ci));

        _Atomic int64_t shared_ids;
        atomic_init(&shared_ids, cbm_gbuf_next_id(ctx->gbuf));

        CBMFileResult **cache;
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
        if (incr_test_take_result_cache_alloc_failure()) {
            cache = NULL;
        } else
#endif
        {
            cache = (CBMFileResult **)calloc((size_t)ci, sizeof(CBMFileResult *));
        }
        if (!cache) {
            cbm_log_error("incremental.err", "phase", "result_cache_alloc");
            return CBM_NOT_FOUND;
        }

        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        int rc = cbm_parallel_extract(ctx, changed_files, ci, cache, &shared_ids, worker_count);
        if (rc == 0) {
            rc = cbm_pipeline_check_cancel(ctx);
        }
        cbm_log_info("pass.timing", "pass", "incr_extract", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
        if (rc != 0) {
            free_incremental_result_cache(cache, ci);
            return rc;
        }
        cbm_gbuf_set_next_id(ctx->gbuf, atomic_load(&shared_ids));

        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_build_registry_from_cache(ctx, changed_files, ci, cache);
        if (rc == 0) {
            rc = cbm_pipeline_check_cancel(ctx);
        }
        cbm_log_info("pass.timing", "pass", "incr_registry", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
        if (rc != 0) {
            free_incremental_result_cache(cache, ci);
            return rc;
        }

        /* Registry construction can materialize serial resource nodes after
         * extraction established the workers' shared allocator. Hand the main
         * buffer's monotonic watermark back to the workers before resolve-time
         * synthetic nodes are created. */
        int64_t registry_next_id = cbm_gbuf_next_id(ctx->gbuf);
        if (registry_next_id > atomic_load(&shared_ids)) {
            atomic_store(&shared_ids, registry_next_id);
        }

        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        CBMLSPDef *all_defs = NULL;
        int all_def_count = 0;
        char **def_modules = NULL;
        CBMModuleDefIndex *module_def_index = NULL;
        CBMCrossLspRegistries cross_registries = {0};
        CBMCrossLspRegistries *registries_arg = NULL;
        if (closure) {
            /* Fresh surfaces for the re-parsed files, from the same collect
             * path a full build uses; then registries over stored + fresh. */
            def_modules = (char **)calloc((size_t)ci, sizeof(char *));
            int *def_starts = (int *)calloc((size_t)ci + 1, sizeof(int));
            int fresh_count = 0;
            CBMLSPDef *fresh_defs =
                def_modules && def_starts
                    ? cbm_pxc_collect_all_defs(cache, changed_files, ci, ctx->project_name,
                                               def_modules, &fresh_count, def_starts)
                    : NULL;
            if ((fresh_defs || fresh_count == 0) && def_starts &&
                cbm_lsp_surface_build_rows(ctx->project_name, cache, changed_files, ci, fresh_defs,
                                           def_starts, &closure->fresh_rows,
                                           &closure->fresh_count) != 0) {
                closure->fresh_rows = NULL;
                closure->fresh_count = 0;
            }
            free(def_starts);
            all_def_count = closure->base_def_count + fresh_count;
            if (all_def_count > 0) {
                all_defs = (CBMLSPDef *)cbm_arena_alloc(&closure->arena,
                                                        (size_t)all_def_count * sizeof(CBMLSPDef));
            }
            if (all_defs) {
                if (closure->base_def_count > 0) {
                    memcpy(all_defs, closure->base_defs,
                           (size_t)closure->base_def_count * sizeof(CBMLSPDef));
                }
                if (fresh_count > 0) {
                    memcpy(all_defs + closure->base_def_count, fresh_defs,
                           (size_t)fresh_count * sizeof(CBMLSPDef));
                }
                module_def_index = cbm_pxc_build_module_def_index(all_defs, all_def_count);
                /* Tier-2 shared registries are an amortization: the full
                 * pipeline pays one build over all defs to make 85k per-file
                 * resolves O(1). A floor-sized closure resolves a handful of
                 * files, so the build (minutes-scale over multi-million-def
                 * corpora) can never pay for itself — the per-file fallback
                 * path, filtered through module_def_index, is the SAME
                 * pre-Tier-2 resolution code the full pipeline still uses
                 * for languages without a shared registry, so convergence
                 * is unaffected; only the amortization strategy changes. */
                if (ci > CLOSURE_BUDGET_FLOOR_FILES) {
                    CBMArena *xa = &closure->arena;
                    cross_registries.go = cbm_go_build_cross_registry(xa, all_defs, all_def_count);
                    cross_registries.python =
                        cbm_py_build_cross_registry(xa, all_defs, all_def_count);
                    cross_registries.c = cbm_c_build_cross_registry(xa, all_defs, all_def_count);
                    cross_registries.cs = cbm_cs_build_cross_registry(xa, all_defs, all_def_count);
                    cross_registries.ts = cbm_ts_build_cross_registry(xa, all_defs, all_def_count);
                    cross_registries.java =
                        cbm_java_build_cross_registry(xa, all_defs, all_def_count);
                    registries_arg = &cross_registries;
                }
            } else {
                all_def_count = 0;
            }
            free(fresh_defs);
            /* The resolve workers borrow def_modules strings; ownership moves
             * to the closure ctx so they outlive resolve + calls. */
            closure->def_modules = def_modules;
            closure->def_module_count = ci;
            def_modules = NULL;
            cbm_log_info("incremental.closure_registries", "base",
                         itoa_buf(closure->base_def_count), "fresh", itoa_buf(fresh_count),
                         "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
        }
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_parallel_resolve(ctx, changed_files, ci, cache, &shared_ids, worker_count,
                                  all_defs, all_def_count, closure ? closure->def_modules : NULL,
                                  module_def_index, registries_arg);
        if (module_def_index) {
            cbm_pxc_free_module_def_index(module_def_index);
        }
        if (rc == 0) {
            rc = cbm_pipeline_check_cancel(ctx);
        }
        cbm_log_info("pass.timing", "pass", "incr_resolve", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
        cbm_gbuf_set_next_id(ctx->gbuf, atomic_load(&shared_ids));
        free_incremental_result_cache(cache, ci);
        return rc;
    } else {
        cbm_log_info("incremental.mode", "mode", "sequential", "changed", itoa_buf(ci));

        /* Keep the definition pass's transformed/extracted result alive for
         * the following resolution passes. This mirrors the full sequential
         * pipeline and is required for transform-only inputs such as
         * ObjectScript Studio Export XML, whose original source cannot be
         * re-extracted directly by the call/usage/semantic passes. */
        CBMFileResult **prior_cache = ctx->result_cache;
        CBMFileResult **cache = prior_cache;
        bool owns_cache = false;
        if (!cache && ci > 0) {
            cache = (CBMFileResult **)calloc((size_t)ci, sizeof(CBMFileResult *));
            if (!cache) {
                cbm_log_error("incremental.err", "phase", "result_cache_alloc");
                return CBM_NOT_FOUND;
            }
            ctx->result_cache = cache;
            owns_cache = true;
        }
        int rc = cbm_pipeline_pass_definitions(ctx, changed_files, ci);
        if (rc == 0) {
            rc = cbm_pipeline_check_cancel(ctx);
        }
        if (rc == 0) {
            rc = cbm_pipeline_pass_calls(ctx, changed_files, ci);
        }
        if (rc == 0) {
            rc = cbm_pipeline_check_cancel(ctx);
        }
        if (rc == 0) {
            rc = cbm_pipeline_pass_usages(ctx, changed_files, ci);
        }
        if (rc == 0) {
            rc = cbm_pipeline_check_cancel(ctx);
        }
        if (rc == 0) {
            rc = cbm_pipeline_pass_semantic(ctx, changed_files, ci);
        }
        if (rc == 0) {
            rc = cbm_pipeline_check_cancel(ctx);
        }
        if (owns_cache) {
            free_incremental_result_cache(cache, ci);
            ctx->result_cache = prior_cache;
        }
        return rc;
    }
}

/* Run post-extraction passes (tests, decorator tags, configlink). */
static int run_postpasses(cbm_pipeline_ctx_t *ctx, cbm_file_info_t *changed_files, int ci,
                          const char *project) {
    struct timespec t;

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    int rc = cbm_pipeline_pass_tests(ctx, changed_files, ci);
    cbm_log_info("pass.timing", "pass", "incr_tests", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
    if (rc != 0 || cbm_pipeline_check_cancel(ctx)) {
        return rc != 0 ? rc : CBM_NOT_FOUND;
    }

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    rc = cbm_pipeline_pass_decorator_tags(ctx->gbuf, project);
    cbm_log_info("pass.timing", "pass", "incr_decorator_tags", "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t)));
    if (rc < 0 || cbm_pipeline_check_cancel(ctx)) {
        return rc < 0 ? rc : CBM_NOT_FOUND;
    }

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    rc = cbm_pipeline_pass_configlink(ctx);
    cbm_log_info("pass.timing", "pass", "incr_configlink", "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t)));
    if (rc < 0 || cbm_pipeline_check_cancel(ctx)) {
        return rc < 0 ? rc : CBM_NOT_FOUND;
    }

    /* SIMILAR_TO + SEMANTICALLY_RELATED edges only in moderate/full modes */
    if (ctx->mode <= CBM_MODE_MODERATE) {
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_pipeline_pass_similarity(ctx);
        cbm_log_info("pass.timing", "pass", "incr_similarity", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
        if (rc < 0 || cbm_pipeline_check_cancel(ctx)) {
            return rc < 0 ? rc : CBM_NOT_FOUND;
        }

        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_pipeline_pass_semantic_edges(ctx);
        cbm_log_info("pass.timing", "pass", "incr_semantic_edges", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
        if (rc < 0 || cbm_pipeline_check_cancel(ctx)) {
            return rc < 0 ? rc : CBM_NOT_FOUND;
        }
    }
    return 0;
}
/* Publish the test-only legacy partial result through the same atomic
 * generation boundary as full indexing. */
static int dump_and_persist(cbm_gbuf_t *gbuf, const char *db_path, const char *project,
                            atomic_int *cancelled, const cbm_file_hash_t *manifest,
                            int manifest_count, const char *adr_content,
                            const cbm_coverage_row_t *cov, int cov_count,
                            const cbm_coverage_meta_t *meta_template,
                            const cbm_lsp_surface_row_t *surface_rows, int surface_row_count) {
    struct timespec t;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    cbm_pipeline_generation_t generation = {
        .gbuf = gbuf,
        .final_db_path = db_path,
        .project = project,
        .cancelled = cancelled,
        .manifest = manifest,
        .manifest_count = manifest_count,
        .adr_content = adr_content,
        .coverage = cov,
        .coverage_count = cov_count,
        .coverage_meta = meta_template ? *meta_template : (cbm_coverage_meta_t){0},
        .surface_rows = surface_rows,
        .surface_row_count = surface_row_count,
    };
    int rc = cbm_pipeline_publish_generation(&generation);
    cbm_log_info("incremental.dump", "rc", itoa_buf(rc), "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t)));
    if (rc != 0) {
        return rc;
    }
    return 0;
}

/* Parallel base-def rehydration: rows are independent, so the JSON decode
 * fans out across workers on a stride; assembly back into one array stays
 * in ROW ORDER, so the registration input is byte-for-byte the sequence
 * the serial loop produced. Measured 35s serial at kernel scale (2.37M
 * defs) -- the single largest delta block after the query-plan fixes. */
typedef struct {
    const cbm_lsp_surface_row_t *const *rows;
    int row_count;
    int worker_id;
    int worker_count;
    CBMArena *arena;      /* this worker's arena (owned by cr) */
    CBMLSPDef **row_defs; /* per-row result pointers */
    int *row_counts;      /* per-row def counts; -1 = decode failure */
} rehydrate_worker_t;

static void *rehydrate_worker(void *arg) {
    rehydrate_worker_t *w = (rehydrate_worker_t *)arg;
    for (int i = w->worker_id; i < w->row_count; i += w->worker_count) {
        CBMLSPDef *defs = NULL;
        int count = cbm_lsp_surface_defs_from_json(w->arena, w->rows[i]->defs_json, &defs);
        w->row_defs[i] = defs;
        w->row_counts[i] = count;
    }
    return NULL;
}

/* Probe-extract a set of files into a throwaway graph to compute their
 * FRESH surfaces without touching the real state. Parses the changed files
 * once more than strictly necessary (the executor re-extracts them into the
 * real graph after the purge); the changed set is small by construction and
 * correctness stays trivially reasoned. Returns heap rows via out. */
static int closure_probe_surfaces(cbm_pipeline_t *p, const char *project,
                                  cbm_file_info_t *probe_files, int probe_count,
                                  cbm_lsp_surface_row_t **out_rows, int *out_count) {
    *out_rows = NULL;
    *out_count = 0;
    cbm_gbuf_t *probe_gbuf = cbm_gbuf_new(project, cbm_pipeline_repo_path(p));
    if (!probe_gbuf) {
        return -1;
    }
    char **excluded_dirs = NULL;
    int excluded_count = 0;
    cbm_pipeline_get_excluded(p, &excluded_dirs, &excluded_count);
    cbm_path_alias_collection_t *aliases =
        cbm_load_path_aliases_excluded(cbm_pipeline_repo_path(p), excluded_dirs, excluded_count);
    cbm_pipeline_ctx_t probe_ctx = {
        .project_name = project,
        .repo_path = cbm_pipeline_repo_path(p),
        .gbuf = probe_gbuf,
        .registry = NULL,
        .cancelled = cbm_pipeline_cancelled_ptr(p),
        .pipeline = NULL, /* probe must not record file errors twice */
        .mode = cbm_pipeline_get_mode(p),
        .path_aliases = aliases,
        .excluded_dirs = excluded_dirs,
        .excluded_count = excluded_count,
    };
    CBMFileResult **cache = (CBMFileResult **)calloc((size_t)probe_count, sizeof(CBMFileResult *));
    int rc = cache ? 0 : -1;
    if (rc == 0) {
        _Atomic int64_t probe_ids;
        atomic_init(&probe_ids, cbm_gbuf_next_id(probe_gbuf));
        rc = cbm_parallel_extract(&probe_ctx, probe_files, probe_count, cache, &probe_ids,
                                  cbm_default_worker_count(true));
    }
    if (rc == 0) {
        char **def_modules = (char **)calloc((size_t)probe_count, sizeof(char *));
        int *def_starts = (int *)calloc((size_t)probe_count + 1, sizeof(int));
        int def_count = 0;
        CBMLSPDef *defs = NULL;
        if (def_modules && def_starts) {
            defs = cbm_pxc_collect_all_defs(cache, probe_files, probe_count, project, def_modules,
                                            &def_count, def_starts);
            rc = cbm_lsp_surface_build_rows(project, cache, probe_files, probe_count, defs,
                                            def_starts, out_rows, out_count);
        } else {
            rc = -1;
        }
        free(defs);
        free(def_starts);
        if (def_modules) {
            for (int i = 0; i < probe_count; i++) {
                free(def_modules[i]);
            }
            free(def_modules);
        }
    }
    if (cache) {
        free_incremental_result_cache(cache, probe_count);
    }
    /* Parallel extraction builds the process-global package map; the probe
     * owns it exactly as the real extraction paths do. Leaving it behind
     * leaked one map per probed run (LSan, macOS CI). */
    cbm_pkgmap_free(cbm_pipeline_get_pkgmap());
    cbm_pipeline_set_pkgmap(NULL);
    cbm_path_alias_collection_free(aliases);
    cbm_gbuf_free(probe_gbuf);
    return rc;
}

/* Is this manifest row a path-alias config (tsconfig/jsconfig class)? Exact
 * match against the loaded alias collection's config paths, plus a basename
 * fallback so a REMOVED config (absent from the fresh collection) still
 * classifies. Alias configs are the one config class whose effect is
 * per-scope rather than global, which is what makes governed-closure repair
 * sound for them and for nothing else. */
static bool closure_is_alias_config(const cbm_path_alias_collection_t *aliases,
                                    const char *rel_path) {
    if (!rel_path) {
        return false;
    }
    if (aliases) {
        for (int i = 0; i < aliases->count; i++) {
            if (aliases->scopes[i].source_rel_path &&
                strcmp(aliases->scopes[i].source_rel_path, rel_path) == 0) {
                return true;
            }
        }
    }
    const char *slash = strrchr(rel_path, '/');
    const char *base = slash ? slash + 1 : rel_path;
    return strcmp(base, "tsconfig.json") == 0 || strcmp(base, "jsconfig.json") == 0;
}

/* Every discovered file under the config's directory joins the closure:
 * their RESOLUTION consumed the config even though their bytes (and thus
 * surfaces) are unchanged, so they re-extract and re-resolve but never
 * propagate further. Over-inclusion (nested scopes) is safe; the budget
 * still bounds the total. */
static void closure_seed_governed(CBMHashTable *closure_set, const cbm_file_info_t *files,
                                  int file_count, const char *config_rel_path) {
    const char *slash = strrchr(config_rel_path, '/');
    size_t dir_len = slash ? (size_t)(slash - config_rel_path + 1) : 0;
    for (int i = 0; i < file_count; i++) {
        if (dir_len > 0 && strncmp(files[i].rel_path, config_rel_path, dir_len) != 0) {
            continue;
        }
        if (!cbm_ht_get(closure_set, files[i].rel_path)) {
            cbm_ht_set(closure_set, files[i].rel_path, (void *)files[i].rel_path);
        }
    }
}

/* Decide whether this manifest delta is closure-repairable, and if so build
 * the plan. Returns 1 with *plan filled, or 0 (decline → FULL) with the
 * reason logged. Every early exit declines; nothing here mutates state. */
static int closure_try_plan(cbm_pipeline_t *p, cbm_store_t *store, const char *project,
                            cbm_file_info_t *files, int file_count, const cbm_file_hash_t *stored,
                            int stored_count, const cbm_file_hash_t *fresh, int fresh_count,
                            closure_plan_t *plan) {
    memset(plan, 0, sizeof(*plan));
    struct timespec t;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    const char *decline = NULL;

    /* Index the fresh manifest and discovery list by rel_path. */
    CBMHashTable *fresh_by_path = cbm_ht_create((size_t)fresh_count * PAIR_LEN + CBM_SZ_64);
    CBMHashTable *files_by_path = cbm_ht_create((size_t)file_count * PAIR_LEN + CBM_SZ_64);
    CBMHashTable *stored_by_path = cbm_ht_create((size_t)stored_count * PAIR_LEN + CBM_SZ_64);
    CBMHashTable *closure_set = cbm_ht_create(CBM_SZ_64);
    char **changed_paths = NULL;
    char **dependents = NULL;
    int dependent_count = 0;
    cbm_lsp_surface_row_t *probe_rows = NULL;
    int probe_count = 0;
    cbm_file_info_t *probe_files = NULL;
    cbm_path_alias_collection_t *plan_aliases = NULL;
    int n_changed = 0;
    int n_deleted = 0;
    if (!fresh_by_path || !files_by_path || !stored_by_path || !closure_set) {
        decline = "alloc";
        goto done;
    }
    for (int i = 0; i < file_count; i++) {
        cbm_ht_set(files_by_path, files[i].rel_path, &files[i]);
    }
    for (int i = 0; i < fresh_count; i++) {
        cbm_ht_set(fresh_by_path, fresh[i].rel_path, (void *)&fresh[i]);
    }
    for (int i = 0; i < stored_count; i++) {
        cbm_ht_set(stored_by_path, stored[i].rel_path, (void *)&stored[i]);
    }

    /* Delta classification straight from the semantic manifests (content
     * hashes, not mtimes). Virtual entries are config/lockfile digests —
     * their effect on resolution is not file-local, so any delta there
     * declines. New files decline: yesterday's graph cannot know who would
     * reference a file that did not exist. */
    changed_paths = (char **)calloc((size_t)fresh_count + (size_t)stored_count, sizeof(char *));
    if (!changed_paths) {
        decline = "alloc";
        goto done;
    }
    plan_aliases = cbm_load_path_aliases_excluded(cbm_pipeline_repo_path(p), NULL, 0);
    bool governed_seeded = false;
    for (int i = 0; i < fresh_count; i++) {
        const cbm_file_hash_t *old_row = cbm_ht_get(stored_by_path, fresh[i].rel_path);
        bool synthetic = semantic_manifest_is_virtual_path(fresh[i].rel_path);
        bool discovered = cbm_ht_get(files_by_path, fresh[i].rel_path) != NULL;
        bool delta = !old_row || strcmp(old_row->sha256, fresh[i].sha256) != 0;
        if (!delta) {
            continue;
        }
        if (synthetic) {
            decline = "semantic_input_changed";
            goto done;
        }
        if (discovered) {
            if (!old_row) {
                decline = "new_file";
                goto done;
            }
            changed_paths[n_changed++] = (char *)fresh[i].rel_path;
            continue;
        }
        if (closure_is_alias_config(plan_aliases, fresh[i].rel_path)) {
            /* Config content (or a new config) re-routes resolution for the
             * files it governs; their surfaces are untouched, so they join
             * the closure directly and propagate nowhere. */
            closure_seed_governed(closure_set, files, file_count, fresh[i].rel_path);
            governed_seeded = true;
            continue;
        }
        decline = "control_file_changed";
        goto done;
    }
    for (int i = 0; i < stored_count; i++) {
        if (!cbm_ht_get(fresh_by_path, stored[i].rel_path)) {
            if (!cbm_ht_get(files_by_path, stored[i].rel_path) &&
                closure_is_alias_config(plan_aliases, stored[i].rel_path)) {
                /* Removed alias config: its governed files resolve without
                 * it now; the config's own manifest row simply disappears. */
                closure_seed_governed(closure_set, files, file_count, stored[i].rel_path);
                governed_seeded = true;
                continue;
            }
            /* Gone from the manifest: deleted or newly ignored. Its nodes
             * will be purged; files with edges into it must re-resolve. */
            changed_paths[n_changed + n_deleted] = (char *)stored[i].rel_path;
            n_deleted++;
        }
    }
    if (n_changed == 0 && n_deleted == 0 && !governed_seeded) {
        decline = "no_file_delta";
        goto done;
    }

    /* Load the previous generation's surfaces and probe the changed files'
     * fresh ones. Missing rows fail closed. */
    if (cbm_store_get_lsp_surfaces(store, project, &plan->stored_rows, &plan->stored_count) !=
            CBM_STORE_OK ||
        plan->stored_count == 0) {
        decline = "no_surface_rows";
        goto done;
    }
    CBMHashTable *rows_by_path = cbm_ht_create((size_t)plan->stored_count * PAIR_LEN);
    if (!rows_by_path) {
        decline = "alloc";
        goto done;
    }
    for (int i = 0; i < plan->stored_count; i++) {
        cbm_ht_set(rows_by_path, plan->stored_rows[i].rel_path, &plan->stored_rows[i]);
    }

    probe_files =
        (cbm_file_info_t *)calloc((size_t)(n_changed ? n_changed : 1), sizeof(cbm_file_info_t));
    if (!probe_files) {
        cbm_ht_free(rows_by_path);
        decline = "alloc";
        goto done;
    }
    for (int i = 0; i < n_changed; i++) {
        cbm_file_info_t *info = cbm_ht_get(files_by_path, changed_paths[i]);
        if (!info) {
            cbm_ht_free(rows_by_path);
            decline = "changed_not_discovered";
            goto done;
        }
        probe_files[i] = *info;
    }
    if (n_changed > 0 && closure_probe_surfaces(p, project, probe_files, n_changed, &probe_rows,
                                                &probe_count) != 0) {
        cbm_ht_free(rows_by_path);
        decline = "probe_failed";
        goto done;
    }

    /* Early cutoff per changed file; surface-changed files seed the
     * dependent query. Deleted files always seed it. */
    int n_surface_changed = 0;
    const char **dep_targets =
        (const char **)calloc((size_t)(n_changed + n_deleted), sizeof(char *));
    if (!dep_targets) {
        cbm_ht_free(rows_by_path);
        decline = "alloc";
        goto done;
    }
    int dep_target_count = 0;
    for (int i = 0; i < n_changed; i++) {
        const cbm_lsp_surface_row_t *stored_row = cbm_ht_get(rows_by_path, changed_paths[i]);
        const cbm_lsp_surface_row_t *fresh_row = NULL;
        for (int j = 0; j < probe_count; j++) {
            if (strcmp(probe_rows[j].rel_path, changed_paths[i]) == 0) {
                fresh_row = &probe_rows[j];
                break;
            }
        }
        if (!stored_row || !fresh_row) {
            free(dep_targets);
            cbm_ht_free(rows_by_path);
            decline = "missing_surface_row";
            goto done;
        }
        if (strcmp(stored_row->surface_sha, fresh_row->surface_sha) == 0) {
            continue; /* body edit: the file re-resolves, nobody else does */
        }
        bool added = false;
        if (surface_added_names(stored_row->defs_json, fresh_row->defs_json, &added) != 0 ||
            added) {
            free(dep_targets);
            cbm_ht_free(rows_by_path);
            decline = "added_definition_names";
            goto done;
        }
        n_surface_changed++;
        dep_targets[dep_target_count++] = changed_paths[i];
    }
    for (int i = 0; i < n_deleted; i++) {
        dep_targets[dep_target_count++] = changed_paths[n_changed + i];
    }
    cbm_ht_free(rows_by_path);

    if (dep_target_count > 0 &&
        cbm_store_get_dependent_files(store, project, dep_targets, dep_target_count, &dependents,
                                      &dependent_count) != CBM_STORE_OK) {
        free(dep_targets);
        decline = "dependent_query_failed";
        goto done;
    }
    free(dep_targets);

    /* Closure = changed ∪ dependents. Every member must be in the current
     * discovery (a dependent outside it cannot be re-resolved). */
    for (int i = 0; i < n_changed; i++) {
        if (!cbm_ht_get(closure_set, changed_paths[i])) {
            cbm_ht_set(closure_set, changed_paths[i], changed_paths[i]);
        }
    }
    for (int i = 0; i < dependent_count; i++) {
        if (!cbm_ht_get(files_by_path, dependents[i])) {
            decline = "dependent_not_discovered";
            goto done;
        }
        if (!cbm_ht_get(closure_set, dependents[i])) {
            cbm_ht_set(closure_set, dependents[i], dependents[i]);
        }
    }
    /* closure_count == 0 is legitimate: a deleted-only delta with no
     * dependents has nothing to re-parse, but the purge itself still needs
     * the executor. */
    int closure_count = (int)cbm_ht_count(closure_set);
    if (closure_count > CLOSURE_BUDGET_FLOOR_FILES &&
        closure_count * 100 > file_count * CLOSURE_BUDGET_PERCENT) {
        decline = "budget_exceeded";
        goto done;
    }

    plan->files = (cbm_file_info_t *)calloc((size_t)(closure_count ? closure_count : 1),
                                            sizeof(cbm_file_info_t));
    if (!plan->files) {
        decline = "alloc";
        goto done;
    }
    plan->count = 0;
    for (int i = 0; i < file_count; i++) {
        if (cbm_ht_get(closure_set, files[i].rel_path)) {
            plan->files[plan->count++] = files[i];
        }
    }
    plan->n_changed = n_changed;
    plan->n_dependents = dependent_count;
    /* Two calls, not one: itoa_buf recycles a four-slot ring, so six
     * conversions in a single call print two corrupted fields (observed:
     * surface_changed reporting the elapsed-ms value at kernel scale). */
    cbm_log_info("incremental.closure_plan", "changed", itoa_buf(n_changed), "surface_changed",
                 itoa_buf(n_surface_changed), "deleted", itoa_buf(n_deleted), "dependents",
                 itoa_buf(dependent_count));
    cbm_log_info("incremental.closure_plan_done", "closure", itoa_buf(plan->count), "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t)));

done:
    cbm_path_alias_collection_free(plan_aliases);
    free(probe_files);
    cbm_store_free_lsp_surfaces(probe_rows, probe_count);
    cbm_store_free_dependent_files(dependents, dependent_count);
    free(changed_paths);
    cbm_ht_free(fresh_by_path);
    cbm_ht_free(files_by_path);
    cbm_ht_free(stored_by_path);
    cbm_ht_free(closure_set);
    if (decline) {
        cbm_log_info("incremental.closure_decline", "reason", decline, "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
        cbm_store_free_lsp_surfaces(plan->stored_rows, plan->stored_count);
        memset(plan, 0, sizeof(*plan));
        return 0;
    }
    return 1;
}

/* ── Delta-repair orchestration (closure route) ──────────────────
 *
 * The closure route's executor: clone the live generation, repair the
 * closure against proxy nodes carrying real database ids, patch the clone
 * transactionally, publish through the shared staged tail. No full graph
 * load, no full dump. EVERY failure discards the stage and returns
 * CBM_PIPELINE_FORCE_FULL_REINDEX: the live database is never touched, so
 * a full rebuild always self-heals whatever the delta could not do.
 *
 * Takes ownership of changed_files/deleted/mode_skipped/old_cov/plan and
 * closes `store` (the routing read connection). */
static int run_closure_delta(cbm_pipeline_t *p, const char *db_path, const char *project,
                             const cbm_file_hash_t *baseline_manifest, int baseline_count,
                             closure_plan_t *plan, cbm_file_info_t *changed_files, int ci,
                             char **deleted, int deleted_count, cbm_file_hash_t *mode_skipped,
                             int mode_skipped_count, cbm_coverage_row_t *old_cov, int old_cov_count,
                             cbm_store_t *route_store, struct timespec t0) {
    struct timespec t;
    cbm_store_close(route_store);

    int result = CBM_PIPELINE_FORCE_FULL_REINDEX;
    char *stage = NULL;
    cbm_store_t *staging = NULL;
    cbm_delta_saved_edge_t *snapshot = NULL;
    int snapshot_count = 0;
    cbm_gbuf_t *gbuf = NULL;
    cbm_registry_t *registry = NULL;
    cbm_path_alias_collection_t *path_aliases = NULL;
    CBMHashTable *stale_surface_set = NULL;
    closure_resolve_t cr = {0};
    bool cr_arena_live = false;
    const char **purge_paths = NULL;
    cbm_file_hash_t *manifest = NULL;
    int manifest_count = 0;
    cbm_coverage_row_t *cov = NULL;
    int cov_n = 0;

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    if (cbm_delta_stage_clone(db_path, &stage) != 0) {
        goto out;
    }
    cbm_log_info("delta.clone", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    staging = cbm_store_open_path(stage);
    if (!staging) {
        goto out;
    }
    cbm_log_info("delta.staging_open", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));

    /* Purge set = closure files + deleted files. */
    int purge_count = ci + deleted_count;
    purge_paths = (const char **)calloc((size_t)(purge_count ? purge_count : 1), sizeof(char *));
    if (!purge_paths) {
        goto out;
    }
    for (int i = 0; i < ci; i++) {
        purge_paths[i] = changed_files[i].rel_path;
    }
    for (int i = 0; i < deleted_count; i++) {
        purge_paths[ci + i] = deleted[i];
    }
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    if (cbm_delta_snapshot_inbound(staging, project, purge_paths, purge_count, &snapshot,
                                   &snapshot_count) != 0) {
        goto out;
    }
    if (cbm_delta_purge(staging, project, purge_paths, purge_count) != 0) {
        goto out;
    }
    cbm_log_info("delta.snapshot_purge", "captured", itoa_buf(snapshot_count), "purged_files",
                 itoa_buf(purge_count), "elapsed_ms", itoa_buf((int)elapsed_ms(t)));

    /* Small graph: proxies with real ids, then the name registry from them. */
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    gbuf = cbm_gbuf_new(project, cbm_pipeline_repo_path(p));
    if (!gbuf) {
        goto out;
    }
    int64_t max_db_id = cbm_delta_preseed(staging, project, gbuf);
    if (max_db_id < 0) {
        goto out;
    }
    registry = cbm_registry_new();
    if (!registry) {
        goto out;
    }
    cbm_gbuf_foreach_node(gbuf, registry_visitor, registry);
    cbm_log_info("delta.preseed_done", "registry", itoa_buf(cbm_registry_size(registry)),
                 "elapsed_ms", itoa_buf((int)elapsed_ms(t)));

    /* Stale-surface set + rehydrated base defs, exactly as the plan computed
     * them. Owned keys: deleted[] strings die with this function's frees. */
    stale_surface_set = cbm_ht_create((size_t)(purge_count)*PAIR_LEN + CBM_SZ_64);
    if (!stale_surface_set) {
        goto out;
    }
    for (int i = 0; i < purge_count; i++) {
        char *key = strdup(purge_paths[i]);
        if (!key) {
            goto out;
        }
        cbm_ht_set(stale_surface_set, key, key);
    }
    cbm_arena_init(&cr.arena);
    cr_arena_live = true;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    {
        /* Eligible rows first (skip everything being re-parsed or deleted),
         * then fan the JSON decode out across workers. */
        const cbm_lsp_surface_row_t **elig = (const cbm_lsp_surface_row_t **)calloc(
            (size_t)(plan->stored_count ? plan->stored_count : 1), sizeof(*elig));
        if (!elig) {
            goto out;
        }
        int elig_count = 0;
        for (int i = 0; i < plan->stored_count; i++) {
            const cbm_lsp_surface_row_t *row = &plan->stored_rows[i];
            if (!cbm_ht_get(stale_surface_set, row->rel_path)) {
                elig[elig_count++] = row;
            }
        }
        int workers = cbm_default_worker_count(true);
        if (workers < 1) {
            workers = 1;
        }
        if (workers > elig_count) {
            workers = elig_count ? elig_count : 1;
        }
        CBMLSPDef **row_defs =
            (CBMLSPDef **)calloc((size_t)(elig_count ? elig_count : 1), sizeof(*row_defs));
        int *row_counts = (int *)calloc((size_t)(elig_count ? elig_count : 1), sizeof(int));
        cr.rehydrate_arenas = (CBMArena *)calloc((size_t)workers, sizeof(CBMArena));
        cbm_thread_t *threads = (cbm_thread_t *)calloc((size_t)workers, sizeof(cbm_thread_t));
        rehydrate_worker_t *wargs = (rehydrate_worker_t *)calloc((size_t)workers, sizeof(*wargs));
        if (!row_defs || !row_counts || !cr.rehydrate_arenas || !threads || !wargs) {
            free(elig);
            free(row_defs);
            free(row_counts);
            free(threads);
            free(wargs);
            goto out;
        }
        cr.rehydrate_arena_count = workers;
        for (int w = 0; w < workers; w++) {
            cbm_arena_init(&cr.rehydrate_arenas[w]);
        }
        int spawned = 0;
        for (int w = 0; w < workers; w++) {
            wargs[w] = (rehydrate_worker_t){.rows = elig,
                                            .row_count = elig_count,
                                            .worker_id = w,
                                            .worker_count = workers,
                                            .arena = &cr.rehydrate_arenas[w],
                                            .row_defs = row_defs,
                                            .row_counts = row_counts};
            if (cbm_thread_create(&threads[w], 0, rehydrate_worker, &wargs[w]) != 0) {
                break;
            }
            spawned++;
        }
        for (int w = spawned; w < workers; w++) {
            (void)rehydrate_worker(&wargs[w]);
        }
        for (int w = 0; w < spawned; w++) {
            cbm_thread_join(&threads[w]);
        }
        free(threads);
        free(wargs);
        bool decode_ok = true;
        int64_t total = 0;
        for (int i = 0; i < elig_count; i++) {
            if (row_counts[i] < 0) {
                cbm_log_error("delta.rehydrate_failed", "path", elig[i]->rel_path);
                decode_ok = false;
                break;
            }
            total += row_counts[i];
        }
        if (decode_ok && total > 0) {
            cr.base_defs =
                (CBMLSPDef *)cbm_arena_alloc(&cr.arena, (size_t)total * sizeof(CBMLSPDef));
            if (!cr.base_defs) {
                decode_ok = false;
            } else {
                for (int i = 0; i < elig_count; i++) {
                    if (row_counts[i] > 0) {
                        memcpy(cr.base_defs + cr.base_def_count, row_defs[i],
                               (size_t)row_counts[i] * sizeof(CBMLSPDef));
                        cr.base_def_count += row_counts[i];
                    }
                }
            }
        }
        free(elig);
        free(row_defs);
        free(row_counts);
        if (!decode_ok) {
            goto out;
        }
    }
    cbm_log_info("delta.rehydrate", "defs", itoa_buf(cr.base_def_count), "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t)));

    char **excluded_dirs = NULL;
    int excluded_count = 0;
    cbm_pipeline_get_excluded(p, &excluded_dirs, &excluded_count);
    path_aliases =
        cbm_load_path_aliases_excluded(cbm_pipeline_repo_path(p), excluded_dirs, excluded_count);
    cbm_pipeline_ctx_t ctx = {
        .project_name = project,
        .repo_path = cbm_pipeline_repo_path(p),
        .gbuf = gbuf,
        .registry = registry,
        .cancelled = cbm_pipeline_cancelled_ptr(p),
        .pipeline = p,
        .mode = cbm_pipeline_get_mode(p),
        .path_aliases = path_aliases,
        .excluded_dirs = excluded_dirs,
        .excluded_count = excluded_count,
    };
    for (int i = 0; i < ci; i++) {
        char *file_qn = cbm_pipeline_fqn_compute(project, changed_files[i].rel_path, "__file__");
        if (file_qn) {
            const char *rel = changed_files[i].rel_path;
            const char *slash = strrchr(rel, '/');
            const char *basename = slash ? slash + SKIP_ONE : rel;
            char props[CBM_SZ_256];
            const char *ext = strrchr(basename, '.');
            snprintf(props, sizeof(props), "{\"extension\":\"%s\"}", ext ? ext : "");
            cbm_gbuf_upsert_node(gbuf, "File", basename, file_qn, rel, 0, 0, props);
            free(file_qn);
        }
    }

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    int phase_rc = run_extract_resolve(&ctx, changed_files, ci, &cr);
    if (phase_rc == 0) {
        phase_rc = cbm_pipeline_pass_k8s(&ctx, changed_files, ci);
    }
    if (phase_rc == 0) {
        phase_rc = cbm_pipeline_check_cancel(&ctx);
    }
    if (phase_rc == 0) {
        phase_rc = run_postpasses(&ctx, changed_files, ci, project);
    }
    if (ctx.return_type_table) {
        for (int i = 0; i < ctx.return_type_table->count; i++) {
            free((void *)ctx.return_type_table->entries[i].return_type);
        }
        free((void *)ctx.return_type_table);
    }
    if (ctx.macro_table) {
        free((void *)ctx.macro_table);
    }
    cbm_pkgmap_free(cbm_pipeline_get_pkgmap());
    cbm_pipeline_set_pkgmap(NULL);
    if (phase_rc != 0) {
        cbm_log_error("delta.err", "phase", "extract_resolve", "rc", itoa_buf(phase_rc));
        goto out;
    }
    cbm_log_info("delta.repair", "files", itoa_buf(ci), "elapsed_ms", itoa_buf((int)elapsed_ms(t)));

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    if (cbm_delta_patch(staging, project, gbuf, max_db_id, snapshot, snapshot_count) != 0) {
        goto out;
    }
    cbm_log_info("delta.patch_done", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));

    /* Coverage: previous failure rows for files not re-extracted + this
     * run's fresh entries — same merge the legacy tail performs. */
    cbm_file_error_t *run_errs = NULL;
    int run_err_count = 0;
    cbm_pipeline_get_file_errors(p, &run_errs, &run_err_count);
    char **run_excluded = NULL;
    int run_excluded_count = 0;
    cbm_pipeline_get_excluded(p, &run_excluded, &run_excluded_count);
    cbm_ignored_file_t *run_ignored = NULL;
    int run_ignored_count = 0;
    int run_ignored_total = 0;
    cbm_pipeline_get_ignored(p, &run_ignored, &run_ignored_count, &run_ignored_total);
    int cov_cap = old_cov_count + run_err_count + run_excluded_count + run_ignored_count;
    bool coverage_rows_available = cov_cap == 0;
    if (cov_cap > 0) {
        cov = (cbm_coverage_row_t *)malloc((size_t)cov_cap * sizeof(*cov));
        coverage_rows_available = cov != NULL;
    }
    if (cov) {
        for (int i = 0; i < old_cov_count; i++) {
            bool by_design = old_cov[i].kind && strncmp(old_cov[i].kind, "not_indexed", 11) == 0;
            if (!by_design && old_cov[i].rel_path &&
                !cbm_ht_get(stale_surface_set, old_cov[i].rel_path)) {
                cov[cov_n++] = old_cov[i];
            }
        }
        for (int i = 0; i < run_err_count; i++) {
            cov[cov_n].rel_path = run_errs[i].path;
            cov[cov_n].kind = run_errs[i].phase;
            cov[cov_n].detail = run_errs[i].reason;
            cov_n++;
        }
        for (int i = 0; i < run_excluded_count; i++) {
            cov[cov_n].rel_path = run_excluded[i];
            cov[cov_n].kind = "not_indexed_dir";
            cov[cov_n].detail = "excluded subtree";
            cov_n++;
        }
        for (int i = 0; i < run_ignored_count; i++) {
            cov[cov_n].rel_path = run_ignored[i].rel_path;
            cov[cov_n].kind = "not_indexed_file";
            cov[cov_n].detail = run_ignored[i].reason;
            cov_n++;
        }
    }

    /* Publication race gate: identical to every other route. */
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    cbm_pipeline_persist_test_run_before_final_manifest();
#endif
    if (cbm_pipeline_build_fresh_semantic_manifest(project, cbm_pipeline_repo_path(p),
                                                   cbm_pipeline_get_mode(p), &manifest,
                                                   &manifest_count) != 0 ||
        !cbm_pipeline_semantic_manifests_equal(baseline_manifest, baseline_count, manifest,
                                               manifest_count)) {
        cbm_log_warn("delta.abort", "reason", "semantic_inputs_changed");
        result = CBM_PIPELINE_ABORT_PRESERVE_DB;
        goto out;
    }

    /* Surfaces in place: delete the purged files' rows and upsert only
     * the repaired files' fresh ones inside the staging store. The other
     * ~all rows are already correct in the clone; rewriting them was the
     * largest single block of a kernel-scale delta publish. */
    {
        sqlite3 *sdb = cbm_store_get_db(staging);
        sqlite3_stmt *del = NULL;
        bool surf_ok = sdb != NULL;
        if (surf_ok && sqlite3_prepare_v2(sdb,
                                          "DELETE FROM lsp_surface WHERE project = ?1"
                                          " AND rel_path = ?2",
                                          CBM_NOT_FOUND, &del, NULL) == SQLITE_OK) {
            for (int i = 0; surf_ok && i < ci + deleted_count; i++) {
                sqlite3_reset(del);
                sqlite3_bind_text(del, 1, project, CBM_NOT_FOUND, SQLITE_TRANSIENT);
                sqlite3_bind_text(del, 2, purge_paths[i], CBM_NOT_FOUND, SQLITE_TRANSIENT);
                surf_ok = sqlite3_step(del) == SQLITE_DONE;
            }
            sqlite3_finalize(del);
        } else {
            surf_ok = false;
        }
        if (!surf_ok || cbm_store_upsert_lsp_surface_batch(staging, cr.fresh_rows,
                                                           cr.fresh_count) != CBM_STORE_OK) {
            goto out;
        }
    }

    /* Committed counts for the #334 plausibility gate. */
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    cbm_pipeline_set_committed_counts(p, cbm_store_count_nodes(staging, project),
                                      cbm_store_count_edges(staging, project));
    cbm_log_info("delta.committed_counts", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));

    {
        int index_mode = cbm_pipeline_get_mode(p);
        cbm_pipeline_generation_t generation = {
            .gbuf = NULL,
            .final_db_path = db_path,
            .project = project,
            .cancelled = cbm_pipeline_cancelled_ptr(p),
            .manifest = manifest,
            .manifest_count = manifest_count,
            .adr_content = NULL, /* the clone already carries the ADR rows */
            .coverage = cov,
            .coverage_count = cov_n,
            .coverage_meta =
                {
                    .index_mode = incr_mode_name(index_mode),
                    .recording_status =
                        !coverage_rows_available
                            ? "unavailable"
                            : (run_ignored_total > run_ignored_count ? "truncated" : "complete"),
                    .ignored_files_stored = run_ignored_count,
                    .ignored_files_total = run_ignored_total,
                    .coverage_version = CBM_SEMANTIC_INDEX_VERSION,
                    .hash_records_complete = true,
                },
            .surface_rows = NULL,
            .surface_row_count = 0,
            .surfaces_in_place = true,
        };
        cbm_store_close(staging);
        staging = NULL;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        result = cbm_pipeline_publish_staged(stage, &generation, false, true);
        cbm_log_info("delta.publish_total", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
        stage = NULL; /* publish_staged owns and frees it on every path */
    }
    cbm_log_info("incremental.done_delta", "elapsed_ms", itoa_buf((int)elapsed_ms(t0)));

out:
    if (staging) {
        cbm_store_close(staging);
    }
    if (stage) {
        cbm_pipeline_discard_stage(stage);
        free(stage);
    }
    if (result == CBM_PIPELINE_FORCE_FULL_REINDEX) {
        cbm_log_warn("delta.fallback", "reason", "delta_failed_full_rebuild");
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
        incr_test_set_last_route(CBM_INCREMENTAL_ROUTE_FORCED_FULL);
#endif
    }
    cbm_delta_free_snapshot(snapshot, snapshot_count);
    surface_name_set_free(stale_surface_set);
    if (cr_arena_live) {
        cbm_store_free_lsp_surfaces(cr.fresh_rows, cr.fresh_count);
        for (int i = 0; i < cr.def_module_count; i++) {
            free(cr.def_modules[i]);
        }
        free(cr.def_modules);
        for (int i = 0; i < cr.rehydrate_arena_count; i++) {
            cbm_arena_destroy(&cr.rehydrate_arenas[i]);
        }
        free(cr.rehydrate_arenas);
        cbm_arena_destroy(&cr.arena);
    }
    cbm_pipeline_free_semantic_manifest(manifest, manifest_count);
    free(cov);
    free(purge_paths);
    if (registry) {
        cbm_registry_free(registry);
    }
    cbm_path_alias_collection_free(path_aliases);
    if (gbuf) {
        cbm_gbuf_free(gbuf);
    }
    closure_plan_free(plan);
    free(changed_files);
    for (int i = 0; i < deleted_count; i++) {
        free(deleted[i]);
    }
    free(deleted);
    free_mode_skipped(mode_skipped, mode_skipped_count);
    cbm_store_free_coverage(old_cov, old_cov_count);
    return result;
}

/* ── Incremental pipeline entry point ────────────────────────────── */

int cbm_pipeline_run_incremental(cbm_pipeline_t *p, const char *db_path, cbm_file_info_t *files,
                                 int file_count, const cbm_file_hash_t *baseline_manifest,
                                 int baseline_count, bool force_full_on_mismatch) {
    struct timespec t0;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t0);
    closure_plan_t closure_plan = {0};
    bool closure_active = false;

    const char *project = cbm_pipeline_project_name(p);

    bool force_legacy_partial = false;
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    force_legacy_partial = incr_test_take_force_legacy_partial();
    incr_test_set_last_route(CBM_INCREMENTAL_ROUTE_NONE);
#endif

    /* Exact semantic-manifest routing is read-only. Do not reopen a sealed
     * generation in WAL mode merely to prove that it is already current.
     * Only the legacy partial TEST route needs a writable connection, so the
     * writable branch exists only in test-API builds. */
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    cbm_store_t *store =
        force_legacy_partial ? cbm_store_open_path(db_path) : cbm_store_open_path_query(db_path);
#else
    cbm_store_t *store = cbm_store_open_path_query(db_path);
#endif
    if (!store) {
        cbm_log_error("incremental.err", "msg", "open_db_failed", "path", db_path);
        return CBM_NOT_FOUND;
    }

    /* Load stored file hashes */
    cbm_file_hash_t *stored = NULL;
    int stored_count = 0;
    if (cbm_store_get_file_hashes(store, project, &stored, &stored_count) != CBM_STORE_OK) {
        cbm_log_error("incremental.err", "msg", "manifest_read_failed", "project", project);
        cbm_store_close(store);
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }

    /* Production incremental routing is deliberately binary: an exact
     * semantic-manifest match is a no-op; any byte/set/mode/migration delta
     * rebuilds the complete graph. This avoids partial LSP/package/config
     * convergence gaps while retaining the common unchanged-repo fast path. */
    if (!force_legacy_partial) {
        cbm_coverage_meta_t meta = {0};
        int meta_rc = cbm_store_coverage_meta_get(store, project, &meta);
        const char *mode_name = incr_mode_name(cbm_pipeline_get_mode(p));
        bool metadata_current = meta_rc == CBM_STORE_OK &&
                                meta.coverage_version == CBM_SEMANTIC_INDEX_VERSION &&
                                meta.hash_records_complete && meta.index_mode &&
                                strcmp(meta.index_mode, mode_name) == 0;
        bool exact = metadata_current &&
                     cbm_pipeline_semantic_manifests_equal(stored, stored_count, baseline_manifest,
                                                           baseline_count);
        cbm_store_coverage_meta_clear(&meta);
        if (exact) {
            cbm_store_free_file_hashes(stored, stored_count);
            cbm_store_close(store);
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
            incr_test_set_last_route(CBM_INCREMENTAL_ROUTE_NOOP);
#endif
            cbm_log_info("incremental.noop", "reason", "semantic_manifest_equal");
            return 0;
        }
        if (force_full_on_mismatch) {
            cbm_store_free_file_hashes(stored, stored_count);
            cbm_store_close(store);
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
            incr_test_set_last_route(CBM_INCREMENTAL_ROUTE_FORCED_FULL);
#endif
            cbm_log_info("incremental.force_full", "reason", "mode_downgrade_changed");
            return CBM_PIPELINE_FORCE_FULL_REINDEX;
        }
        /* Manifest delta. Closure repair recomputes exactly the changed
         * files plus the recorded consumers of any changed SURFACE; every
         * uncertain case declines inside the planner, making the pre-plan
         * behaviour (full rebuild) the unconditional fallback. */
        if (metadata_current &&
            closure_try_plan(p, store, project, files, file_count, stored, stored_count,
                             baseline_manifest, baseline_count, &closure_plan)) {
            closure_active = true;
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
            incr_test_set_last_route(CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR);
#endif
            cbm_log_info("incremental.route", "route", "closure_repair", "files",
                         itoa_buf(closure_plan.count));
            /* Fall through into the shared repair tail below. */
        } else {
            cbm_store_free_file_hashes(stored, stored_count);
            cbm_store_close(store);
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
            incr_test_set_last_route(CBM_INCREMENTAL_ROUTE_FORCED_FULL);
#endif
            cbm_log_info("incremental.force_full", "reason", "semantic_manifest_changed");
            return CBM_PIPELINE_FORCE_FULL_REINDEX;
        }
    }

    /* Classify files. The closure route already knows its re-parse set (the
     * plan); the legacy test route keeps its historical mtime+size classify. */
    int n_changed = 0;
    int n_unchanged = 0;
    bool *is_changed = NULL;
    if (closure_active) {
        n_changed = closure_plan.count;
    } else {
        is_changed =
            classify_files(files, file_count, stored, stored_count, &n_changed, &n_unchanged);
    }

    /* Classify stored files absent from current discovery: truly-deleted
     * (purge) vs mode-skipped (preserve nodes AND hash rows). */
    char **deleted = NULL;
    cbm_file_hash_t *mode_skipped = NULL;
    int mode_skipped_count = 0;
    int deleted_count =
        find_deleted_files(cbm_pipeline_repo_path(p), files, file_count, stored, stored_count,
                           &deleted, &mode_skipped, &mode_skipped_count);

    cbm_log_info("incremental.classify", "changed", itoa_buf(n_changed), "unchanged",
                 itoa_buf(n_unchanged), "deleted", itoa_buf(deleted_count), "mode_skipped",
                 itoa_buf(mode_skipped_count));

    /* Fast path: nothing changed → skip. The on-disk DB is left untouched,
     * which means existing hash rows (including for any mode-skipped files
     * that were already preserved by an earlier run) remain intact. */
    if (!closure_active && n_changed == 0 && deleted_count == 0) {
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
        incr_test_set_last_route(CBM_INCREMENTAL_ROUTE_NOOP);
#endif
        cbm_log_info("incremental.noop", "reason", "no_changes");
        free(is_changed);
        free(deleted);
        free_mode_skipped(mode_skipped, mode_skipped_count);
        cbm_store_free_file_hashes(stored, stored_count);
        cbm_store_close(store);
        return 0;
    }

#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    if (!closure_active) {
        incr_test_set_last_route(CBM_INCREMENTAL_ROUTE_LEGACY_PARTIAL);
    }
#endif

    cbm_store_free_file_hashes(stored, stored_count);

    /* Coverage rows (#963): the dump below rebuilds the DB file, wiping the
     * separate index_coverage table — capture the previous rows now (store
     * still open) so entries for files NOT re-extracted this run survive. */
    cbm_coverage_row_t *old_cov = NULL;
    int old_cov_count = 0;
    if (cbm_store_coverage_get(store, project, &old_cov, &old_cov_count) != CBM_STORE_OK) {
        cbm_log_error("incremental.err", "msg", "coverage_read_failed", "project", project);
        cbm_store_free_coverage(old_cov, old_cov_count);
        free(is_changed);
        for (int i = 0; i < deleted_count; i++) {
            free(deleted[i]);
        }
        free(deleted);
        free_mode_skipped(mode_skipped, mode_skipped_count);
        cbm_store_close(store);
        closure_plan_free(&closure_plan);
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }

    /* Build list of changed files */
    cbm_file_info_t *changed_files =
        (n_changed > 0) ? malloc((size_t)n_changed * sizeof(cbm_file_info_t)) : NULL;
    int ci = 0;
    if (closure_active) {
        for (int i = 0; i < closure_plan.count; i++) {
            changed_files[ci++] = closure_plan.files[i];
        }
    } else {
        for (int i = 0; i < file_count; i++) {
            if (is_changed[i]) {
                changed_files[ci++] = files[i];
            }
        }
    }
    free(is_changed);

    cbm_log_info("incremental.reparse", "files", itoa_buf(ci));

    if (closure_active) {
        /* The delta executor owns everything passed and never touches the
         * live database; its every failure falls back to a full rebuild. */
        return run_closure_delta(p, db_path, project, baseline_manifest, baseline_count,
                                 &closure_plan, changed_files, ci, deleted, deleted_count,
                                 mode_skipped, mode_skipped_count, old_cov, old_cov_count, store,
                                 t0);
    }

    struct timespec t;

    /* Step 1: Load existing graph into RAM */
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    cbm_gbuf_t *existing = cbm_gbuf_new(project, cbm_pipeline_repo_path(p));
    int load_rc = cbm_gbuf_load_from_db(existing, db_path, project);
    cbm_log_info("incremental.load_db", "rc", itoa_buf(load_rc), "nodes",
                 itoa_buf(cbm_gbuf_node_count(existing)), "edges",
                 itoa_buf(cbm_gbuf_edge_count(existing)), "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t)));

    if (load_rc != 0) {
        cbm_log_error("incremental.err", "msg", "load_db_failed");
        cbm_gbuf_free(existing);
        free(changed_files);
        for (int i = 0; i < deleted_count; i++) {
            free(deleted[i]);
        }
        free(deleted);
        free_mode_skipped(mode_skipped, mode_skipped_count);
        cbm_store_free_coverage(old_cov, old_cov_count);
        cbm_store_close(store);
        closure_plan_free(&closure_plan);
        return CBM_NOT_FOUND;
    }

    char *saved_adr = NULL;
    cbm_adr_t existing_adr = {0};
    int adr_rc = cbm_store_adr_get(store, project, &existing_adr);
    if (adr_rc == CBM_STORE_OK) {
        bool had_adr_content = existing_adr.content != NULL;
        if (had_adr_content) {
            saved_adr = strdup(existing_adr.content);
        }
        cbm_store_adr_free(&existing_adr);
        if (had_adr_content && !saved_adr) {
            cbm_gbuf_free(existing);
            free(changed_files);
            for (int i = 0; i < deleted_count; i++) {
                free(deleted[i]);
            }
            free(deleted);
            free_mode_skipped(mode_skipped, mode_skipped_count);
            cbm_store_free_coverage(old_cov, old_cov_count);
            cbm_store_close(store);
            closure_plan_free(&closure_plan);
            return CBM_PIPELINE_ABORT_PRESERVE_DB;
        }
    } else if (adr_rc != CBM_STORE_NOT_FOUND) {
        cbm_store_adr_free(&existing_adr);
        cbm_gbuf_free(existing);
        free(changed_files);
        for (int i = 0; i < deleted_count; i++) {
            free(deleted[i]);
        }
        free(deleted);
        free_mode_skipped(mode_skipped, mode_skipped_count);
        cbm_store_free_coverage(old_cov, old_cov_count);
        cbm_store_close(store);
        closure_plan_free(&closure_plan);
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }

    cbm_store_close(store);

    /* Snapshot inbound cross-file edges into changed files BEFORE purging, so
     * the cascade delete doesn't permanently drop edges whose source lives in
     * an unchanged (never-re-parsed) file. Re-linked after re-resolution. */
    cbm_edge_capture_t edge_cap = {0};
    edge_cap.gbuf = existing;
    {
        CBMHashTable *changed_paths = cbm_ht_create(ci > 0 ? (size_t)ci * PAIR_LEN : CBM_SZ_64);
        for (int i = 0; i < ci; i++) {
            cbm_ht_set(changed_paths, changed_files[i].rel_path, &changed_files[i]);
        }
        edge_cap.changed_paths = changed_paths;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        cbm_gbuf_foreach_edge(existing, incr_capture_inbound_edge, &edge_cap);
        edge_cap.changed_paths = NULL;
        cbm_ht_free(changed_paths); /* keys borrowed from changed_files; not freed here */
    }
    cbm_log_info("incremental.edge_snapshot", "captured", itoa_buf(edge_cap.count), "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t)));

    /* Step 2: Purge stale nodes */
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    for (int i = 0; i < ci; i++) {
        cbm_gbuf_delete_by_file(existing, changed_files[i].rel_path);
    }
    for (int i = 0; i < deleted_count; i++) {
        cbm_gbuf_delete_by_file(existing, deleted[i]);
        free(deleted[i]);
    }
    free(deleted);
    cbm_log_info("incremental.purge", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));

    /* Step 3-5: Registry + extract + resolve */
    cbm_registry_t *registry = cbm_registry_new();
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    cbm_gbuf_foreach_node(existing, registry_visitor, registry);
    cbm_log_info("incremental.registry_seed", "symbols", itoa_buf(cbm_registry_size(registry)),
                 "elapsed_ms", itoa_buf((int)elapsed_ms(t)));

    /* Discovery exclusions (gitignore + skip dirs) captured by the run that
     * routed here. Borrowed from the pipeline so the auxiliary repo walks
     * (pkgmap via merge_pkg_entries, path aliases) skip excluded subtrees on
     * incremental runs too — same borrow as the full path (#792/#804). */
    char **excluded_dirs = NULL;
    int excluded_count = 0;
    cbm_pipeline_get_excluded(p, &excluded_dirs, &excluded_count);

    cbm_path_alias_collection_t *path_aliases =
        cbm_load_path_aliases_excluded(cbm_pipeline_repo_path(p), excluded_dirs, excluded_count);

    cbm_pipeline_ctx_t ctx = {
        .project_name = project,
        .repo_path = cbm_pipeline_repo_path(p),
        .gbuf = existing,
        .registry = registry,
        .cancelled = cbm_pipeline_cancelled_ptr(p),
        .pipeline = p, /* so passes can record per-file skips (Track B) */
        .mode = cbm_pipeline_get_mode(p),
        .path_aliases = path_aliases,
        .excluded_dirs = excluded_dirs,
        .excluded_count = excluded_count,
    };

    for (int i = 0; i < ci; i++) {
        char *file_qn = cbm_pipeline_fqn_compute(project, changed_files[i].rel_path, "__file__");
        if (file_qn) {
            /* #994: the name must be the BASENAME with extension props,
             * mirroring the full build's File node (pipeline.c) — upserts
             * match by QN, so any other name here renames the node in place
             * and the incremental graph diverges from a full build. */
            const char *rel = changed_files[i].rel_path;
            const char *slash = strrchr(rel, '/');
            const char *basename = slash ? slash + SKIP_ONE : rel;
            char props[CBM_SZ_256];
            const char *ext = strrchr(basename, '.');
            snprintf(props, sizeof(props), "{\"extension\":\"%s\"}", ext ? ext : "");
            cbm_gbuf_upsert_node(existing, "File", basename, file_qn, rel, 0, 0, props);
            free(file_qn);
        }
    }

    int phase_rc = run_extract_resolve(&ctx, changed_files, ci, NULL);
    if (phase_rc == 0) {
        phase_rc = cbm_pipeline_pass_k8s(&ctx, changed_files, ci);
    }
    if (phase_rc == 0) {
        phase_rc = cbm_pipeline_check_cancel(&ctx);
    }
    if (phase_rc == 0) {
        phase_rc = run_postpasses(&ctx, changed_files, ci, project);
    }

    /* Free ObjectScript tables built by pass_calls during run_extract_resolve. */
    if (ctx.return_type_table) {
        for (int i = 0; i < ctx.return_type_table->count; i++) {
            free((void *)ctx.return_type_table->entries[i].return_type);
        }
        free((void *)ctx.return_type_table);
        ctx.return_type_table = NULL;
    }
    if (ctx.macro_table) {
        free((void *)ctx.macro_table);
        ctx.macro_table = NULL;
    }

    /* Parallel extraction builds the process-global package map. Match the
     * full pipeline's ownership boundary on both success and failure. */
    cbm_pkgmap_free(cbm_pipeline_get_pkgmap());
    cbm_pipeline_set_pkgmap(NULL);

    if (phase_rc != 0) {
        cbm_log_error("incremental.err", "phase", "extract_resolve", "rc", itoa_buf(phase_rc));
        free(changed_files);
        cbm_registry_free(registry);
        cbm_path_alias_collection_free(path_aliases);
        incr_free_edge_capture(&edge_cap);
        cbm_store_free_coverage(old_cov, old_cov_count);
        free_mode_skipped(mode_skipped, mode_skipped_count);
        free(saved_adr);
        cbm_gbuf_free(existing);
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }

    /* Coverage rows (#963): merge = previous FAILURE rows for files NOT
     * re-extracted this run + this run's fresh entries (changed files replace
     * their old rows — a file that parses cleanly now simply contributes
     * nothing, so its stale flag dies here). By-design not_indexed_* rows are
     * NOT carried over: discovery runs completely on every route, so this
     * run's excluded dirs + ignored files are the fresh, authoritative set.
     * Rows for deleted files are pruned against file_hashes inside the
     * replace. Borrowed strings: old_cov and the pipeline own them past the
     * dump_and_persist call below. */
    cbm_file_error_t *run_errs = NULL;
    int run_err_count = 0;
    cbm_pipeline_get_file_errors(p, &run_errs, &run_err_count);
    char **run_excluded = NULL;
    int run_excluded_count = 0;
    cbm_pipeline_get_excluded(p, &run_excluded, &run_excluded_count);
    cbm_ignored_file_t *run_ignored = NULL;
    int run_ignored_count = 0;
    int run_ignored_total = 0;
    cbm_pipeline_get_ignored(p, &run_ignored, &run_ignored_count, &run_ignored_total);
    cbm_coverage_row_t *cov = NULL;
    int cov_n = 0;
    int cov_cap = old_cov_count + run_err_count + run_excluded_count + run_ignored_count;
    if (cov_cap > 0) {
        cov = (cbm_coverage_row_t *)malloc((size_t)cov_cap * sizeof(*cov));
    }
    bool coverage_rows_available = cov_cap == 0 || cov != NULL;
    if (cov) {
        CBMHashTable *changed_set = cbm_ht_create(ci > 0 ? (size_t)ci * PAIR_LEN : CBM_SZ_64);
        for (int i = 0; i < ci; i++) {
            cbm_ht_set(changed_set, changed_files[i].rel_path, &changed_files[i]);
        }
        for (int i = 0; i < old_cov_count; i++) {
            bool by_design = old_cov[i].kind && strncmp(old_cov[i].kind, "not_indexed", 11) == 0;
            if (!by_design && old_cov[i].rel_path &&
                !cbm_ht_get(changed_set, old_cov[i].rel_path)) {
                cov[cov_n++] = old_cov[i];
            }
        }
        cbm_ht_free(changed_set);
        for (int i = 0; i < run_err_count; i++) {
            cov[cov_n].rel_path = run_errs[i].path;
            cov[cov_n].kind = run_errs[i].phase;
            cov[cov_n].detail = run_errs[i].reason;
            cov_n++;
        }
        for (int i = 0; i < run_excluded_count; i++) {
            cov[cov_n].rel_path = run_excluded[i];
            cov[cov_n].kind = "not_indexed_dir";
            cov[cov_n].detail = "excluded subtree";
            cov_n++;
        }
        for (int i = 0; i < run_ignored_count; i++) {
            cov[cov_n].rel_path = run_ignored[i].rel_path;
            cov[cov_n].kind = "not_indexed_file";
            cov[cov_n].detail = run_ignored[i].reason;
            cov_n++;
        }
    }

    free(changed_files);
    cbm_registry_free(registry);
    cbm_path_alias_collection_free(path_aliases);

    /* Re-link inbound cross-file edges that the purge orphaned. Runs after
     * re-resolution AND post-passes so the freshly re-created target nodes
     * exist and nothing downstream clobbers the restored edges; insert_edge
     * dedups, so any edge the resolver already recreated is a no-op. */
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    int relinked = incr_restore_inbound_edges(existing, &edge_cap);
    cbm_log_info("incremental.edge_relink", "relinked", itoa_buf(relinked), "captured",
                 itoa_buf(edge_cap.count), "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
    incr_free_edge_capture(&edge_cap);

    cbm_file_hash_t *manifest = NULL;
    int manifest_count = 0;
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    cbm_pipeline_persist_test_run_before_final_manifest();
#endif
    int manifest_rc = cbm_pipeline_build_fresh_semantic_manifest(
        project, cbm_pipeline_repo_path(p), cbm_pipeline_get_mode(p), &manifest, &manifest_count);
    if (manifest_rc != 0 || !cbm_pipeline_semantic_manifests_equal(
                                baseline_manifest, baseline_count, manifest, manifest_count)) {
        cbm_log_warn("incremental.abort", "reason", "semantic_inputs_changed");
        cbm_pipeline_free_semantic_manifest(manifest, manifest_count);
        free(cov);
        cbm_store_free_coverage(old_cov, old_cov_count);
        free_mode_skipped(mode_skipped, mode_skipped_count);
        free(saved_adr);
        cbm_gbuf_free(existing);
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }

    /* Step 7: atomically publish the complete staged generation. */
    /* Record committed counts before dump_and_persist (whose dump frees the
     * gbuf node index, zeroing the count) so the #334 plausibility gate also
     * covers incremental reindexes, not just full ones. */
    cbm_pipeline_set_committed_counts(p, cbm_gbuf_node_count(existing),
                                      cbm_gbuf_edge_count(existing));
    int index_mode = cbm_pipeline_get_mode(p);
    cbm_coverage_meta_t coverage_meta = {
        .index_mode = incr_mode_name(index_mode),
        .recording_status =
            !coverage_rows_available
                ? "unavailable"
                : (run_ignored_total > run_ignored_count ? "truncated" : "complete"),
        .ignored_files_stored = run_ignored_count,
        .ignored_files_total = run_ignored_total,
        .coverage_version = CBM_SEMANTIC_INDEX_VERSION,
        .hash_records_complete = true,
    };
    /* Publish surfaces: the surviving previous rows plus this run's fresh
     * ones (closure route). The legacy test route publishes none — its
     * re-parsed files have no codec output, and publishing a stale row
     * would satisfy a future closure plan with yesterday's surface; an
     * empty table just routes the next incremental to a full rebuild. */
    int persist_rc =
        dump_and_persist(existing, db_path, project, cbm_pipeline_cancelled_ptr(p), manifest,
                         manifest_count, saved_adr, cov, cov_n, &coverage_meta, NULL, 0);
    cbm_pipeline_free_semantic_manifest(manifest, manifest_count);
    free(saved_adr);
    free(cov);
    cbm_store_free_coverage(old_cov, old_cov_count);
    free_mode_skipped(mode_skipped, mode_skipped_count);
    cbm_gbuf_free(existing);

    cbm_log_info("incremental.done", "elapsed_ms", itoa_buf((int)elapsed_ms(t0)));
    return persist_rc;
}
