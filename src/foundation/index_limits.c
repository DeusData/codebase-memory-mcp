/*
 * index_limits.c — Unified, validated resource policy for repository indexing.
 */
#include "foundation/index_limits.h"

#include <errno.h>
#include <ctype.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CBM_MIB (UINT64_C(1024) * UINT64_C(1024))

typedef enum {
    INDEX_LIMIT_U64,
    INDEX_LIMIT_INT,
    INDEX_LIMIT_BOOL,
    INDEX_LIMIT_STRING,
} index_limit_value_type_t;

typedef struct {
    const char *key;
    size_t offset;
    index_limit_value_type_t type;
    uint64_t scale;
    uint64_t maximum;
} index_limit_spec_t;

#define U64_SPEC(config_key, field, multiplier, max_value) \
    {config_key, offsetof(cbm_index_limits_t, field), INDEX_LIMIT_U64, multiplier, max_value}
#define INT_SPEC(config_key, field, max_value) \
    {config_key, offsetof(cbm_index_limits_t, field), INDEX_LIMIT_INT, 1, max_value}
#define BOOL_SPEC(config_key, field) \
    {config_key, offsetof(cbm_index_limits_t, field), INDEX_LIMIT_BOOL, 1, 1}
#define STRING_SPEC(config_key, field)                                       \
    {config_key, offsetof(cbm_index_limits_t, field), INDEX_LIMIT_STRING, 1, \
     CBM_INDEX_DENIED_ROOTS_CAP - 1}

static const index_limit_spec_t INDEX_LIMIT_SPECS[] = {
    U64_SPEC("index_max_files", max_files, 1, UINT64_C(10000000)),
    U64_SPEC("index_max_directories", max_directories, 1, UINT64_C(1000000)),
    U64_SPEC("index_max_entries", max_entries, 1, UINT64_C(100000000)),
    U64_SPEC("index_max_depth", max_depth, 1, UINT64_C(1024)),
    U64_SPEC("index_max_source_mb", max_source_bytes, CBM_MIB, UINT64_C(1048576)),
    U64_SPEC("index_max_file_mb", max_file_bytes, CBM_MIB, UINT64_C(1048576)),
    U64_SPEC("index_scan_timeout_seconds", scan_timeout_ms, UINT64_C(1000), UINT64_C(86400)),
    INT_SPEC("index_cpu_cores", cpu_cores, 256),
    INT_SPEC("index_concurrent_jobs", concurrent_jobs, 32),
    U64_SPEC("index_memory_limit_mb", memory_limit_bytes, CBM_MIB, UINT64_C(1048576)),
    U64_SPEC("index_max_db_mb", max_db_bytes, CBM_MIB, UINT64_C(1048576)),
    U64_SPEC("index_max_staging_mb", max_staging_bytes, CBM_MIB, UINT64_C(1048576)),
    U64_SPEC("index_max_task_temp_mb", max_task_temp_bytes, CBM_MIB, UINT64_C(1048576)),
    U64_SPEC("index_cache_max_mb", cache_max_bytes, CBM_MIB, UINT64_C(1048576)),
    U64_SPEC("index_min_free_disk_mb", min_free_disk_bytes, CBM_MIB, UINT64_C(1048576)),
    U64_SPEC("index_max_duration_seconds", max_duration_ms, UINT64_C(1000), UINT64_C(604800)),
    BOOL_SPEC("index_low_priority", low_priority),
    STRING_SPEC("index_denied_roots", denied_roots),
};

static const index_limit_spec_t *find_spec(const char *key) {
    if (!key) {
        return NULL;
    }
    for (size_t i = 0; i < sizeof(INDEX_LIMIT_SPECS) / sizeof(INDEX_LIMIT_SPECS[0]); i++) {
        if (strcmp(key, INDEX_LIMIT_SPECS[i].key) == 0) {
            return &INDEX_LIMIT_SPECS[i];
        }
    }
    return NULL;
}

static void set_error(char *error, size_t error_size, const char *message, const char *key) {
    if (!error || error_size == 0) {
        return;
    }
    (void)snprintf(error, error_size, message, key ? key : "(null)");
}

void cbm_index_limits_defaults(cbm_index_limits_t *limits) {
    if (!limits) {
        return;
    }
    *limits = (cbm_index_limits_t){
        .max_files = UINT64_C(100000),
        .max_directories = UINT64_C(20000),
        .max_entries = UINT64_C(500000),
        .max_depth = UINT64_C(64),
        .max_source_bytes = UINT64_C(4096) * CBM_MIB,
        .max_file_bytes = UINT64_C(64) * CBM_MIB,
        .scan_timeout_ms = UINT64_C(30000),
        .cpu_cores = 4,
        .concurrent_jobs = 1,
        .memory_limit_bytes = UINT64_C(8192) * CBM_MIB,
        .max_db_bytes = UINT64_C(16384) * CBM_MIB,
        .max_staging_bytes = UINT64_C(20480) * CBM_MIB,
        .max_task_temp_bytes = UINT64_C(24576) * CBM_MIB,
        .cache_max_bytes = UINT64_C(32768) * CBM_MIB,
        .min_free_disk_bytes = UINT64_C(4096) * CBM_MIB,
        .max_duration_ms = UINT64_C(3600000),
        .low_priority = true,
    };
}

bool cbm_index_limits_is_config_key(const char *key) {
    return find_spec(key) != NULL;
}

size_t cbm_index_limits_config_key_count(void) {
    return sizeof(INDEX_LIMIT_SPECS) / sizeof(INDEX_LIMIT_SPECS[0]);
}

const char *cbm_index_limits_config_key_at(size_t index) {
    return index < cbm_index_limits_config_key_count() ? INDEX_LIMIT_SPECS[index].key : NULL;
}

bool cbm_index_limits_format_value(const cbm_index_limits_t *limits, const char *key, char *value,
                                   size_t value_size) {
    const index_limit_spec_t *spec = find_spec(key);
    if (!limits || !spec || !value || value_size == 0) {
        return false;
    }
    const unsigned char *field = (const unsigned char *)limits + spec->offset;
    int written = -1;
    if (spec->type == INDEX_LIMIT_BOOL) {
        written = snprintf(value, value_size, "%s", *(const bool *)field ? "true" : "false");
    } else if (spec->type == INDEX_LIMIT_STRING) {
        written = snprintf(value, value_size, "%s", (const char *)field);
    } else if (spec->type == INDEX_LIMIT_INT) {
        written = snprintf(value, value_size, "%d", *(const int *)field);
    } else {
        written = snprintf(value, value_size, "%llu",
                           (unsigned long long)(*(const uint64_t *)field / spec->scale));
    }
    return written >= 0 && (size_t)written < value_size;
}

bool cbm_index_limits_default_value(const char *key, char *value, size_t value_size) {
    cbm_index_limits_t defaults;
    cbm_index_limits_defaults(&defaults);
    return cbm_index_limits_format_value(&defaults, key, value, value_size);
}

static bool parse_positive_u64(const char *value, uint64_t maximum, uint64_t *parsed) {
    if (!value || !value[0] || value[0] == '-') {
        return false;
    }
    errno = 0;
    char *end = NULL;
    unsigned long long number = strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || number == 0 || (uint64_t)number > maximum) {
        return false;
    }
    *parsed = (uint64_t)number;
    return true;
}

static bool parse_bool(const char *value, bool *parsed) {
    if (value && (strcmp(value, "true") == 0 || strcmp(value, "1") == 0)) {
        *parsed = true;
        return true;
    }
    if (value && (strcmp(value, "false") == 0 || strcmp(value, "0") == 0)) {
        *parsed = false;
        return true;
    }
    return false;
}

bool cbm_index_limits_set(cbm_index_limits_t *limits, const char *key, const char *value,
                          char *error, size_t error_size) {
    if (!limits) {
        set_error(error, error_size, "missing resource policy for %s", key);
        return false;
    }
    const index_limit_spec_t *spec = find_spec(key);
    if (!spec) {
        set_error(error, error_size, "unknown index resource key: %s", key);
        return false;
    }

    cbm_index_limits_t updated = *limits;
    unsigned char *field = (unsigned char *)&updated + spec->offset;
    if (spec->type == INDEX_LIMIT_BOOL) {
        bool parsed = false;
        if (!parse_bool(value, &parsed)) {
            set_error(error, error_size, "%s must be true or false", key);
            return false;
        }
        *(bool *)field = parsed;
    } else if (spec->type == INDEX_LIMIT_STRING) {
        size_t length = value ? strlen(value) : 0;
        if (!value || length > spec->maximum) {
            set_error(error, error_size, "%s must be a bounded path list", key);
            return false;
        }
        (void)memcpy(field, value, length + 1);
    } else {
        uint64_t parsed = 0;
        if (!parse_positive_u64(value, spec->maximum, &parsed)) {
            set_error(error, error_size, "%s must be a positive integer within its supported range",
                      key);
            return false;
        }
        if (parsed > UINT64_MAX / spec->scale) {
            set_error(error, error_size, "%s is too large after unit conversion", key);
            return false;
        }
        uint64_t resolved = parsed * spec->scale;
        if (spec->type == INDEX_LIMIT_INT) {
            if (resolved > INT_MAX) {
                set_error(error, error_size, "%s exceeds the integer range", key);
                return false;
            }
            *(int *)field = (int)resolved;
        } else {
            *(uint64_t *)field = resolved;
        }
    }

    *limits = updated;
    if (error && error_size > 0) {
        error[0] = '\0';
    }
    return true;
}

uint64_t cbm_index_effective_memory_limit(uint64_t configured_bytes,
                                          uint64_t detected_budget_bytes) {
    if (detected_budget_bytes == 0 || configured_bytes <= detected_budget_bytes) {
        return configured_bytes;
    }
    return detected_budget_bytes;
}

static bool path_separator(char value) {
    return value == '/' || value == '\\';
}

static size_t comparable_path_length(const char *path) {
    size_t length = path ? strlen(path) : 0;
    while (length > 1 && path_separator(path[length - 1])) {
#ifdef _WIN32
        if (length == 3 && isalpha((unsigned char)path[0]) && path[1] == ':') {
            break;
        }
#endif
        length--;
    }
    return length;
}

static bool path_character_equal(char left, char right) {
    if (path_separator(left) && path_separator(right)) {
        return true;
    }
#ifdef _WIN32
    return tolower((unsigned char)left) == tolower((unsigned char)right);
#else
    return left == right;
#endif
}

static bool same_canonical_path(const char *left, const char *right) {
    if (!left || !left[0] || !right || !right[0]) {
        return false;
    }
    size_t left_length = comparable_path_length(left);
    size_t right_length = comparable_path_length(right);
    if (left_length != right_length) {
        return false;
    }
    for (size_t i = 0; i < left_length; i++) {
        if (!path_character_equal(left[i], right[i])) {
            return false;
        }
    }
    return true;
}

static bool filesystem_root(const char *path) {
    size_t length = comparable_path_length(path);
    if (length == 1 && path_separator(path[0])) {
        return true;
    }
#ifdef _WIN32
    if (length == 3 && isalpha((unsigned char)path[0]) && path[1] == ':' &&
        path_separator(path[2])) {
        return true;
    }
    if (length > 2 && path_separator(path[0]) && path_separator(path[1])) {
        int components = 0;
        bool in_component = false;
        for (size_t i = 2; i < length; i++) {
            if (path_separator(path[i])) {
                in_component = false;
            } else if (!in_component) {
                components++;
                in_component = true;
            }
        }
        return components <= 2;
    }
#endif
    return false;
}

static void set_root_reason(char *reason, size_t reason_size, const char *value) {
    if (!reason || reason_size == 0) {
        return;
    }
    (void)snprintf(reason, reason_size, "%s", value);
}

static bool configured_root_matches(const char *repo_root, const char *configured_roots) {
    if (!configured_roots || !configured_roots[0]) {
        return false;
    }

    char roots[CBM_INDEX_DENIED_ROOTS_CAP];
    size_t length = strlen(configured_roots);
    if (length >= sizeof(roots)) {
        return true;
    }
    (void)memcpy(roots, configured_roots, length + 1);

    char *save = NULL;
    for (char *root = strtok_r(roots, ";", &save); root; root = strtok_r(NULL, ";", &save)) {
        while (isspace((unsigned char)*root)) {
            root++;
        }
        char *end = root + strlen(root);
        while (end > root && isspace((unsigned char)end[-1])) {
            *--end = '\0';
        }
        if (root[0] && same_canonical_path(repo_root, root)) {
            return true;
        }
    }
    return false;
}

bool cbm_index_root_allowed(const char *repo_root, const char *home_root, const char *cache_root,
                            const char *configured_roots, char *reason, size_t reason_size) {
    set_root_reason(reason, reason_size, "");
    if (!repo_root || !repo_root[0]) {
        set_root_reason(reason, reason_size, "invalid_root");
        return false;
    }
    if (filesystem_root(repo_root)) {
        set_root_reason(reason, reason_size, "filesystem_root");
        return false;
    }
    if (same_canonical_path(repo_root, home_root)) {
        set_root_reason(reason, reason_size, "home_directory");
        return false;
    }
    if (same_canonical_path(repo_root, cache_root)) {
        set_root_reason(reason, reason_size, "cache_directory");
        return false;
    }
    if (configured_root_matches(repo_root, configured_roots)) {
        set_root_reason(reason, reason_size, "configured_root");
        return false;
    }
    return true;
}
