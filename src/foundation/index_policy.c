#include "foundation/index_policy.h"

#include <stddef.h>
#include <stdio.h>
#include <string.h>

typedef struct {
    const char *key;
    size_t field_offset;
    uint64_t minimum;
    uint64_t maximum;
    uint64_t multiplier;
} index_policy_metadata_t;

static const index_policy_metadata_t INDEX_POLICY_METADATA[] = {
    {CBM_INDEX_CONFIG_MAX_FILES, offsetof(cbm_index_resource_policy_t, max_files), 1,
     CBM_INDEX_MAX_FILES_VALUE, 1},
    {CBM_INDEX_CONFIG_MAX_SOURCE_MB, offsetof(cbm_index_resource_policy_t, max_source_bytes), 1,
     CBM_INDEX_MAX_SOURCE_MB_VALUE, CBM_INDEX_MIB_BYTES},
    {CBM_INDEX_CONFIG_MAX_RSS_MB, offsetof(cbm_index_resource_policy_t, max_rss_bytes),
     CBM_INDEX_MIN_RSS_MB_VALUE, CBM_INDEX_MAX_RSS_MB_VALUE, CBM_INDEX_MIB_BYTES},
    {CBM_INDEX_CONFIG_MAX_DURATION_SECONDS, offsetof(cbm_index_resource_policy_t, max_duration_ms),
     1, CBM_INDEX_MAX_DURATION_SECONDS_VALUE, UINT64_C(1000)},
    {CBM_INDEX_CONFIG_CACHE_MAX_MB, offsetof(cbm_index_resource_policy_t, max_cache_bytes), 1,
     CBM_INDEX_MAX_STORAGE_MB_VALUE, CBM_INDEX_MIB_BYTES},
    {CBM_INDEX_CONFIG_MIN_FREE_DISK_MB, offsetof(cbm_index_resource_policy_t, min_free_disk_bytes),
     1, CBM_INDEX_MAX_STORAGE_MB_VALUE, CBM_INDEX_MIB_BYTES},
};

static void set_error(char *error, size_t error_size, const char *key, uint64_t minimum,
                      uint64_t maximum) {
    if (error && error_size > 0) {
        (void)snprintf(error, error_size, "%s must be off or an integer from %llu to %llu", key,
                       (unsigned long long)minimum, (unsigned long long)maximum);
    }
}

static bool parse_bounded_uint64(const char *value, uint64_t minimum, uint64_t maximum,
                                 uint64_t *parsed) {
    if (!value || !value[0] || !parsed) {
        return false;
    }
    uint64_t result = 0;
    for (const unsigned char *cursor = (const unsigned char *)value; *cursor; cursor++) {
        if (*cursor < '0' || *cursor > '9') {
            return false;
        }
        uint64_t digit = (uint64_t)(*cursor - '0');
        if (result > (maximum - digit) / 10U) {
            return false;
        }
        result = result * 10U + digit;
    }
    if (result < minimum) {
        return false;
    }
    *parsed = result;
    return true;
}

void cbm_index_policy_init(cbm_index_resource_policy_t *policy) {
    if (policy) {
        *policy = (cbm_index_resource_policy_t){0};
    }
}

bool cbm_index_policy_enabled(const cbm_index_resource_policy_t *policy) {
    return cbm_index_policy_discovery_enabled(policy) || cbm_index_policy_worker_enabled(policy) ||
           cbm_index_policy_storage_enabled(policy);
}

bool cbm_index_policy_discovery_enabled(const cbm_index_resource_policy_t *policy) {
    return policy && (policy->max_files.enabled || policy->max_source_bytes.enabled);
}

bool cbm_index_policy_worker_enabled(const cbm_index_resource_policy_t *policy) {
    return policy && (policy->max_rss_bytes.enabled || policy->max_duration_ms.enabled);
}

bool cbm_index_policy_storage_enabled(const cbm_index_resource_policy_t *policy) {
    return policy && (policy->max_cache_bytes.enabled || policy->min_free_disk_bytes.enabled ||
                      policy->max_final_db_bytes.enabled || policy->max_staging_bytes.enabled ||
                      policy->max_task_temp_bytes.enabled);
}

static uint64_t add_saturated(uint64_t left, uint64_t right) {
    return left > UINT64_MAX - right ? UINT64_MAX : left + right;
}

static bool storage_within_max(cbm_index_resource_t resource, uint64_t observed,
                               const cbm_index_limit_u64_t *limit,
                               cbm_index_resource_violation_t *violation) {
    if (!limit->enabled || observed <= limit->value) {
        return true;
    }
    *violation = (cbm_index_resource_violation_t){
        .resource = resource,
        .observed = observed,
        .limit = limit->value,
    };
    return false;
}

bool cbm_index_policy_check_storage(const cbm_index_resource_policy_t *policy,
                                    const cbm_index_storage_sample_t *sample,
                                    cbm_index_resource_violation_t *violation) {
    if (!policy || !sample || !violation) {
        return false;
    }
    *violation = (cbm_index_resource_violation_t){0};
    uint64_t remaining_cache = sample->current_cache_bytes > sample->replaceable_old_bytes
                                   ? sample->current_cache_bytes - sample->replaceable_old_bytes
                                   : 0;
    uint64_t projected_cache = add_saturated(remaining_cache, sample->operation_bytes);
    if (!storage_within_max(CBM_INDEX_RESOURCE_CACHE_BYTES, projected_cache,
                            &policy->max_cache_bytes, violation)) {
        return false;
    }
    if (policy->min_free_disk_bytes.enabled &&
        sample->free_disk_bytes < policy->min_free_disk_bytes.value) {
        *violation = (cbm_index_resource_violation_t){
            .resource = CBM_INDEX_RESOURCE_FREE_DISK_BYTES,
            .observed = sample->free_disk_bytes,
            .limit = policy->min_free_disk_bytes.value,
        };
        return false;
    }
    return storage_within_max(CBM_INDEX_RESOURCE_FINAL_DB_BYTES, sample->final_db_bytes,
                              &policy->max_final_db_bytes, violation) &&
           storage_within_max(CBM_INDEX_RESOURCE_STAGING_BYTES, sample->staging_bytes,
                              &policy->max_staging_bytes, violation) &&
           storage_within_max(CBM_INDEX_RESOURCE_TASK_TEMP_BYTES, sample->task_temp_bytes,
                              &policy->max_task_temp_bytes, violation);
}

size_t cbm_index_policy_key_count(void) {
    return sizeof(INDEX_POLICY_METADATA) / sizeof(INDEX_POLICY_METADATA[0]);
}

const char *cbm_index_policy_key_at(size_t index) {
    return index < cbm_index_policy_key_count() ? INDEX_POLICY_METADATA[index].key : NULL;
}

const char *cbm_index_policy_default_value(const char *key) {
    for (size_t index = 0; index < cbm_index_policy_key_count(); index++) {
        if (key && strcmp(key, INDEX_POLICY_METADATA[index].key) == 0) {
            return "off";
        }
    }
    return NULL;
}

bool cbm_index_policy_set(cbm_index_resource_policy_t *policy, const char *key, const char *value,
                          char *error, size_t error_size) {
    if (error && error_size > 0) {
        error[0] = '\0';
    }
    if (!policy || !key || !value) {
        set_error(error, error_size, key ? key : "index resource limit", 0, 0);
        return false;
    }

    const index_policy_metadata_t *metadata = NULL;
    for (size_t index = 0; index < cbm_index_policy_key_count(); index++) {
        if (strcmp(key, INDEX_POLICY_METADATA[index].key) == 0) {
            metadata = &INDEX_POLICY_METADATA[index];
            break;
        }
    }
    if (!metadata) {
        set_error(error, error_size, key, 0, 0);
        return false;
    }
    cbm_index_limit_u64_t *target =
        (cbm_index_limit_u64_t *)((unsigned char *)policy + metadata->field_offset);

    cbm_index_limit_u64_t candidate = {0};
    if (strcmp(value, "off") != 0) {
        uint64_t parsed = 0;
        if (!parse_bounded_uint64(value, metadata->minimum, metadata->maximum, &parsed)) {
            set_error(error, error_size, key, metadata->minimum, metadata->maximum);
            return false;
        }
        candidate.enabled = true;
        candidate.value = parsed * metadata->multiplier;
    }
    *target = candidate;
    return true;
}

bool cbm_index_policy_format(const cbm_index_resource_policy_t *policy, const char *key, char *out,
                             size_t out_size) {
    if (!policy || !key || !out || out_size == 0) {
        return false;
    }
    const index_policy_metadata_t *metadata = NULL;
    for (size_t index = 0; index < cbm_index_policy_key_count(); index++) {
        if (strcmp(key, INDEX_POLICY_METADATA[index].key) == 0) {
            metadata = &INDEX_POLICY_METADATA[index];
            break;
        }
    }
    if (!metadata) {
        return false;
    }
    const cbm_index_limit_u64_t *limit =
        (const cbm_index_limit_u64_t *)((const unsigned char *)policy + metadata->field_offset);
    int length = limit->enabled
                     ? snprintf(out, out_size, "%llu",
                                (unsigned long long)(limit->value / metadata->multiplier))
                     : snprintf(out, out_size, "off");
    return length >= 0 && (size_t)length < out_size;
}

const char *cbm_index_resource_name(cbm_index_resource_t resource) {
    switch (resource) {
    case CBM_INDEX_RESOURCE_FILES:
        return "files";
    case CBM_INDEX_RESOURCE_SOURCE_BYTES:
        return "source_bytes";
    case CBM_INDEX_RESOURCE_RSS_BYTES:
        return "rss_bytes";
    case CBM_INDEX_RESOURCE_DURATION_MS:
        return "duration_ms";
    case CBM_INDEX_RESOURCE_CACHE_BYTES:
        return "cache_bytes";
    case CBM_INDEX_RESOURCE_FREE_DISK_BYTES:
        return "free_disk_bytes";
    case CBM_INDEX_RESOURCE_FINAL_DB_BYTES:
        return "final_db_bytes";
    case CBM_INDEX_RESOURCE_STAGING_BYTES:
        return "staging_bytes";
    case CBM_INDEX_RESOURCE_TASK_TEMP_BYTES:
        return "task_temp_bytes";
    case CBM_INDEX_RESOURCE_NONE:
    default:
        return "unknown";
    }
}

const char *cbm_index_resource_unit(cbm_index_resource_t resource) {
    if (resource == CBM_INDEX_RESOURCE_FILES) {
        return "files";
    }
    if (resource == CBM_INDEX_RESOURCE_DURATION_MS) {
        return "milliseconds";
    }
    return "bytes";
}

const char *cbm_index_resource_config_key(cbm_index_resource_t resource) {
    switch (resource) {
    case CBM_INDEX_RESOURCE_FILES:
        return CBM_INDEX_CONFIG_MAX_FILES;
    case CBM_INDEX_RESOURCE_SOURCE_BYTES:
        return CBM_INDEX_CONFIG_MAX_SOURCE_MB;
    case CBM_INDEX_RESOURCE_RSS_BYTES:
        return CBM_INDEX_CONFIG_MAX_RSS_MB;
    case CBM_INDEX_RESOURCE_DURATION_MS:
        return CBM_INDEX_CONFIG_MAX_DURATION_SECONDS;
    case CBM_INDEX_RESOURCE_CACHE_BYTES:
        return CBM_INDEX_CONFIG_CACHE_MAX_MB;
    case CBM_INDEX_RESOURCE_FREE_DISK_BYTES:
        return CBM_INDEX_CONFIG_MIN_FREE_DISK_MB;
    case CBM_INDEX_RESOURCE_FINAL_DB_BYTES:
    case CBM_INDEX_RESOURCE_STAGING_BYTES:
    case CBM_INDEX_RESOURCE_TASK_TEMP_BYTES:
        return "index_resource_profile";
    case CBM_INDEX_RESOURCE_NONE:
    default:
        return "index_resource_limit";
    }
}
