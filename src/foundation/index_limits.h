/*
 * index_limits.h — Unified, validated resource policy for repository indexing.
 */
#ifndef CBM_INDEX_LIMITS_H
#define CBM_INDEX_LIMITS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

enum { CBM_INDEX_DENIED_ROOTS_CAP = 4096 };

typedef struct {
    uint64_t max_files;
    uint64_t max_directories;
    uint64_t max_entries;
    uint64_t max_depth;
    uint64_t max_source_bytes;
    uint64_t max_file_bytes;
    uint64_t scan_timeout_ms;
    int cpu_cores;
    int concurrent_jobs;
    uint64_t memory_limit_bytes;
    uint64_t max_db_bytes;
    uint64_t max_staging_bytes;
    uint64_t max_task_temp_bytes;
    uint64_t cache_max_bytes;
    uint64_t min_free_disk_bytes;
    uint64_t max_duration_ms;
    bool low_priority;
    char denied_roots[CBM_INDEX_DENIED_ROOTS_CAP];
} cbm_index_limits_t;

/* Populate the safe, platform-independent defaults documented in
 * docs/INDEX_RESOURCE_LIMITS.md. */
void cbm_index_limits_defaults(cbm_index_limits_t *limits);

/* True only for public index-resource configuration keys. */
bool cbm_index_limits_is_config_key(const char *key);
size_t cbm_index_limits_config_key_count(void);
const char *cbm_index_limits_config_key_at(size_t index);
bool cbm_index_limits_format_value(const cbm_index_limits_t *limits, const char *key, char *value,
                                   size_t value_size);
bool cbm_index_limits_default_value(const char *key, char *value, size_t value_size);

/* Parse and atomically apply one public configuration value. MiB and seconds
 * keys are converted to bytes and milliseconds with overflow checks. On
 * failure, limits is unchanged and error receives a user-facing explanation. */
bool cbm_index_limits_set(cbm_index_limits_t *limits, const char *key, const char *value,
                          char *error, size_t error_size);

/* Clamp the configured per-worker ceiling to a detected allocator/host budget.
 * A zero detected budget means detection is unavailable, not zero capacity. */
uint64_t cbm_index_effective_memory_limit(uint64_t configured_bytes,
                                          uint64_t detected_budget_bytes);

/* Reject roots that are too broad to be repositories. All paths must already
 * be absolute and canonical. configured_roots is a semicolon-separated list
 * of exact roots; descendants remain eligible for indexing. */
bool cbm_index_root_allowed(const char *repo_root, const char *home_root, const char *cache_root,
                            const char *configured_roots, char *reason, size_t reason_size);

#endif /* CBM_INDEX_LIMITS_H */
