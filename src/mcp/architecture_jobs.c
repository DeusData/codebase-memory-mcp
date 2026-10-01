#include "mcp/architecture_jobs.h"
#include "store/architecture_projection.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/platform.h"
#include <sqlite3.h>
#include <yyjson/yyjson.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

enum {
    ARCH_CACHE_ENTRIES = 4,
    ARCH_RESULT_BYTES = 8 * 1024 * 1024,
    ARCH_DEADLINE_MS = 5000,
    ARCH_RETRY_MS = 500,
    ARCH_FAILED_CACHE_MS = 5000,
    ARCH_GENERATION_BYTES = 512,
    ARCH_STAMP_BYTES = 160,
};

typedef struct {
    char *path;
    char *project;
    char *indexed_at;
    char generation[ARCH_GENERATION_BYTES];
    char db_generation[64];
    char stamp[ARCH_STAMP_BYTES];
    int64_t entry_node_id;
    int64_t target_node_id;
    bool include_behavior_evidence;
    char *result;
    const char *error;
    uint64_t touched_ms;
} arch_job_t;

struct cbm_architecture_jobs {
    cbm_mutex_t mutex;
    cbm_thread_t thread;
    atomic_bool cancelled;
    bool started;
    bool done;
    arch_job_t active;
    arch_job_t cache[ARCH_CACHE_ENTRIES];
};

static char *copy_string(const char *text) {
    size_t size = strlen(text) + 1;
    char *copy = malloc(size);
    if (copy) {
        memcpy(copy, text, size);
    }
    return copy;
}

static void clear_job(arch_job_t *job) {
    free(job->path);
    free(job->project);
    free(job->indexed_at);
    free(job->result);
    memset(job, 0, sizeof(*job));
}

/* Include WAL metadata: indexed_at alone has second resolution and does not
 * identify commits made by every writer. Never hash or scan the database on
 * the HTTP thread. Unavailable metadata invalidates rather than reuses. */
static bool file_stamp(const char *path, char *out, size_t size) {
    cbm_path_info_t db = {0}, wal = {0};
    char sidecar[4096];
    int n = snprintf(sidecar, sizeof(sidecar), "%s-wal", path);
    if (n < 0 || (size_t)n >= sizeof(sidecar) ||
        cbm_path_info_utf8(path, &db) != CBM_PATH_INFO_OK || !db.is_regular) {
        return false;
    }
    int wal_rc = cbm_path_info_utf8(sidecar, &wal);
    if (wal_rc != CBM_PATH_INFO_OK && wal_rc != CBM_PATH_INFO_ABSENT) {
        return false;
    }
    snprintf(out, size, "%" PRId64 ":%" PRId64 ":%d:%" PRId64 ":%" PRId64, db.size, db.mtime_ns,
             wal_rc, wal.size, wal.mtime_ns);
    return true;
}

static int64_t data_version(sqlite3 *db) {
    sqlite3_stmt *stmt = NULL;
    int64_t version = -1;
    if (sqlite3_prepare_v2(db, "PRAGMA data_version", -1, &stmt, NULL) == SQLITE_OK &&
        sqlite3_step(stmt) == SQLITE_ROW) {
        version = sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return version;
}

static bool is_cancelled(void *context) {
    cbm_architecture_jobs_t *jobs = context;
    return atomic_load(&jobs->cancelled);
}

static void *run_projection(void *context) {
    cbm_architecture_jobs_t *jobs = context;
    arch_job_t *job = &jobs->active;
    char before[ARCH_STAMP_BYTES] = {0};
    char *result = NULL;
    const char *error = NULL;
    cbm_store_t *store = NULL;
    uint64_t deadline = cbm_now_ms() + ARCH_DEADLINE_MS;
    if (!file_stamp(job->path, before, sizeof(before)) || strcmp(before, job->stamp) != 0) {
        error = "The index changed before analysis. Request the current generation.";
    } else {
        store = cbm_store_open_path_query(job->path);
        if (!store) {
            error = "Could not open a read-only index snapshot.";
        }
    }
    if (store) {
        sqlite3 *db = cbm_store_get_db(store);
        sqlite3_busy_timeout(db, 100);
        cbm_project_t project = {0};
        char db_generation[64] = {0};
        if (sqlite3_exec(db, "BEGIN", NULL, NULL, NULL) != SQLITE_OK ||
            cbm_store_generation(store, db_generation, sizeof(db_generation)) != CBM_STORE_OK ||
            strcmp(db_generation, job->db_generation) != 0 ||
            cbm_store_get_project(store, job->project, &project) != CBM_STORE_OK ||
            strcmp(project.indexed_at ? project.indexed_at : "", job->indexed_at) != 0) {
            error = "The project generation is no longer available.";
        } else {
            cbm_architecture_projection_options_t options = {0};
            options.entry_node_id = job->entry_node_id;
            options.target_node_id = job->target_node_id;
            options.include_behavior_evidence = job->include_behavior_evidence;
            options.deadline_ms = deadline;
            options.cancel = is_cancelled;
            options.cancel_context = jobs;
            int rc = cbm_store_architecture_projection(store, job->project, &options, &result);
            if (rc != CBM_STORE_OK || !result) {
                error = "Architecture analysis was interrupted or could not read the index.";
            } else if (strlen(result) > ARCH_RESULT_BYTES) {
                error = "Architecture output exceeded the response budget.";
            }
        }
        cbm_project_free_fields(&project);
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        cbm_store_close(store);
        char after[ARCH_STAMP_BYTES] = {0};
        if (!file_stamp(job->path, after, sizeof(after)) || strcmp(before, after) != 0) {
            error = "The index changed during analysis. Request the current generation.";
        }
    }
    if (error) {
        free(result);
        result = NULL;
    }
    cbm_mutex_lock(&jobs->mutex);
    job->result = result;
    job->error = error;
    job->touched_ms = cbm_now_ms();
    jobs->done = true;
    cbm_mutex_unlock(&jobs->mutex);
    return NULL;
}

cbm_architecture_jobs_t *cbm_architecture_jobs_new(void) {
    cbm_architecture_jobs_t *jobs = calloc(1, sizeof(*jobs));
    if (jobs) {
        cbm_mutex_init(&jobs->mutex);
        atomic_init(&jobs->cancelled, false);
    }
    return jobs;
}

void cbm_architecture_jobs_free(cbm_architecture_jobs_t *jobs) {
    if (!jobs) {
        return;
    }
    atomic_store(&jobs->cancelled, true);
    if (jobs->started) {
        cbm_thread_join(&jobs->thread);
    }
    clear_job(&jobs->active);
    for (int i = 0; i < ARCH_CACHE_ENTRIES; i++) {
        clear_job(&jobs->cache[i]);
    }
    cbm_mutex_destroy(&jobs->mutex);
    free(jobs);
}

static char *envelope(const char *status, const char *generation, const char *result,
                      const char *error) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", status);
    yyjson_mut_obj_add_str(doc, root, "generation", generation ? generation : "");
    if (strcmp(status, "pending") == 0) {
        yyjson_mut_obj_add_int(doc, root, "retry_after_ms", ARCH_RETRY_MS);
    }
    if (error) {
        yyjson_mut_obj_add_str(doc, root, "error", error);
    }
    yyjson_doc *payload = result ? yyjson_read(result, strlen(result), 0) : NULL;
    if (payload) {
        yyjson_mut_obj_add_val(doc, root, "result",
                               yyjson_val_mut_copy(doc, yyjson_doc_get_root(payload)));
    }
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_doc_free(payload);
    yyjson_mut_doc_free(doc);
    return json;
}

char *cbm_architecture_jobs_request(cbm_architecture_jobs_t *jobs, cbm_store_t *store,
                                    const char *project, int64_t entry_node_id) {
    return cbm_architecture_jobs_request_query(jobs, store, project, entry_node_id, 0, NULL, false);
}

char *cbm_architecture_jobs_request_query(cbm_architecture_jobs_t *jobs, cbm_store_t *store,
                                          const char *project, int64_t entry_node_id,
                                          int64_t target_node_id, const char *expected_generation,
                                          bool include_behavior_evidence) {
    if (!jobs || !store || !project || entry_node_id < 0 || target_node_id < 0 ||
        (target_node_id && !entry_node_id)) {
        return envelope("failed", "", NULL, "Invalid architecture request.");
    }
    const char *path = cbm_store_db_path(store);
    char stamp[ARCH_STAMP_BYTES], generation[ARCH_GENERATION_BYTES], db_generation[64];
    if (!path || !file_stamp(path, stamp, sizeof(stamp))) {
        return envelope("failed", "", NULL, "Architecture analysis needs a persisted index.");
    }
    cbm_project_t metadata = {0};
    if (cbm_store_get_project(store, project, &metadata) != CBM_STORE_OK) {
        return envelope("failed", "", NULL, "Project not found in this index.");
    }
    sqlite3 *db = cbm_store_get_db(store);
    int64_t version = data_version(db);
    if (version < 0 ||
        cbm_store_generation(store, db_generation, sizeof(db_generation)) != CBM_STORE_OK) {
        cbm_project_free_fields(&metadata);
        return envelope("failed", "", NULL, "Could not identify the index generation.");
    }
    snprintf(generation, sizeof(generation), "%s:%s:%" PRId64 ":%d", db_generation, stamp, version,
             sqlite3_total_changes(db));
    if (expected_generation && strcmp(expected_generation, generation) != 0) {
        cbm_project_free_fields(&metadata);
        return envelope("failed", generation, NULL,
                        "The selected graph generation changed. Reload the overview.");
    }
    cbm_mutex_lock(&jobs->mutex);
    if (jobs->started && jobs->done) {
        cbm_thread_join(&jobs->thread);
        jobs->started = false;
        int oldest = 0;
        for (int i = 1; i < ARCH_CACHE_ENTRIES; i++) {
            if (jobs->cache[i].touched_ms < jobs->cache[oldest].touched_ms) {
                oldest = i;
            }
        }
        clear_job(&jobs->cache[oldest]);
        jobs->cache[oldest] = jobs->active;
        memset(&jobs->active, 0, sizeof(jobs->active));
    }
    for (int i = 0; i < ARCH_CACHE_ENTRIES; i++) {
        arch_job_t *cached = &jobs->cache[i];
        if (cached->project && strcmp(cached->project, project) == 0 &&
            strcmp(cached->path, path) == 0 && strcmp(cached->generation, generation) == 0 &&
            cached->entry_node_id == entry_node_id && cached->target_node_id == target_node_id &&
            cached->include_behavior_evidence == include_behavior_evidence &&
            (!cached->error || cbm_now_ms() - cached->touched_ms < ARCH_FAILED_CACHE_MS)) {
            char *reply = envelope(cached->error ? "failed" : "ready", generation, cached->result,
                                   cached->error);
            if (!cached->error) {
                cached->touched_ms = cbm_now_ms();
            }
            cbm_mutex_unlock(&jobs->mutex);
            cbm_project_free_fields(&metadata);
            return reply;
        }
    }
    const char *error = NULL;
    if (!jobs->started) {
        arch_job_t *job = &jobs->active;
        job->path = copy_string(path);
        job->project = copy_string(project);
        job->indexed_at = copy_string(metadata.indexed_at ? metadata.indexed_at : "");
        job->entry_node_id = entry_node_id;
        job->target_node_id = target_node_id;
        job->include_behavior_evidence = include_behavior_evidence;
        snprintf(job->generation, sizeof(job->generation), "%s", generation);
        snprintf(job->db_generation, sizeof(job->db_generation), "%s", db_generation);
        snprintf(job->stamp, sizeof(job->stamp), "%s", stamp);
        jobs->done = false;
        if (!job->path || !job->project || !job->indexed_at ||
            cbm_thread_create(&jobs->thread, 0, run_projection, jobs) != 0) {
            clear_job(job);
            error = "Could not start architecture analysis.";
        } else {
            jobs->started = true;
        }
    }
    char *reply = envelope(error ? "failed" : "pending", generation, NULL, error);
    cbm_mutex_unlock(&jobs->mutex);
    cbm_project_free_fields(&metadata);
    return reply;
}
