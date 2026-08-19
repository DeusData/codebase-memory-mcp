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

static cbm_index_limit_u64_t limit_value(uint64_t value) {
    return (cbm_index_limit_u64_t){.enabled = true, .value = value};
}

static uint64_t mib(uint64_t value) {
    return value * CBM_INDEX_MIB_BYTES;
}

static void apply_profile_baseline(cbm_index_resource_policy_t *policy,
                                   cbm_index_resource_profile_t profile) {
    policy->max_files = (cbm_index_limit_u64_t){0};
    policy->max_directories = (cbm_index_limit_u64_t){0};
    policy->max_entries = (cbm_index_limit_u64_t){0};
    policy->max_depth = (cbm_index_limit_u64_t){0};
    policy->max_source_bytes = (cbm_index_limit_u64_t){0};
    policy->discovery_deadline_ms = (cbm_index_limit_u64_t){0};
    policy->max_rss_bytes = (cbm_index_limit_u64_t){0};
    policy->max_duration_ms = (cbm_index_limit_u64_t){0};
    policy->max_cache_bytes = (cbm_index_limit_u64_t){0};
    policy->min_free_disk_bytes = (cbm_index_limit_u64_t){0};
    policy->max_final_db_bytes = (cbm_index_limit_u64_t){0};
    policy->max_staging_bytes = (cbm_index_limit_u64_t){0};
    policy->max_task_temp_bytes = (cbm_index_limit_u64_t){0};
    policy->profile = profile;
    if (profile == CBM_INDEX_PROFILE_BALANCED) {
        policy->max_files = limit_value(UINT64_C(500000));
        policy->max_directories = limit_value(UINT64_C(250000));
        policy->max_entries = limit_value(UINT64_C(2000000));
        policy->max_depth = limit_value(UINT64_C(128));
        policy->max_source_bytes = limit_value(mib(UINT64_C(65536)));
        policy->discovery_deadline_ms = limit_value(UINT64_C(1800000));
        policy->max_rss_bytes = limit_value(mib(UINT64_C(8192)));
        policy->max_duration_ms = limit_value(UINT64_C(7200000));
        policy->max_final_db_bytes = limit_value(mib(UINT64_C(65536)));
        policy->max_staging_bytes = limit_value(mib(UINT64_C(81920)));
        policy->max_task_temp_bytes = limit_value(mib(UINT64_C(98304)));
        policy->max_cache_bytes = limit_value(mib(UINT64_C(131072)));
        policy->min_free_disk_bytes = limit_value(mib(UINT64_C(4096)));
    } else if (profile == CBM_INDEX_PROFILE_STRICT) {
        policy->max_files = limit_value(UINT64_C(100000));
        policy->max_directories = limit_value(UINT64_C(20000));
        policy->max_entries = limit_value(UINT64_C(500000));
        policy->max_depth = limit_value(UINT64_C(64));
        policy->max_source_bytes = limit_value(mib(UINT64_C(4096)));
        policy->discovery_deadline_ms = limit_value(UINT64_C(30000));
        policy->max_rss_bytes = limit_value(mib(UINT64_C(8192)));
        policy->max_duration_ms = limit_value(UINT64_C(3600000));
        policy->max_final_db_bytes = limit_value(mib(UINT64_C(16384)));
        policy->max_staging_bytes = limit_value(mib(UINT64_C(20480)));
        policy->max_task_temp_bytes = limit_value(mib(UINT64_C(24576)));
        policy->max_cache_bytes = limit_value(mib(UINT64_C(32768)));
        policy->min_free_disk_bytes = limit_value(mib(UINT64_C(4096)));
    }
}

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

bool cbm_index_policy_set_profile(cbm_index_resource_policy_t *policy, const char *value,
                                  char *error, size_t error_size) {
    if (error && error_size > 0) {
        error[0] = '\0';
    }
    cbm_index_resource_profile_t profile;
    if (value && strcmp(value, "off") == 0) {
        profile = CBM_INDEX_PROFILE_OFF;
    } else if (value && strcmp(value, "balanced") == 0) {
        profile = CBM_INDEX_PROFILE_BALANCED;
    } else if (value && strcmp(value, "strict") == 0) {
        profile = CBM_INDEX_PROFILE_STRICT;
    } else {
        if (error && error_size > 0) {
            (void)snprintf(error, error_size, "%s must be off, balanced, or strict",
                           CBM_INDEX_CONFIG_RESOURCE_PROFILE);
        }
        return false;
    }
    if (!policy) {
        return false;
    }

    cbm_index_resource_policy_t candidate = *policy;
    cbm_index_limit_u64_t
        overrides[sizeof(INDEX_POLICY_METADATA) / sizeof(INDEX_POLICY_METADATA[0])];
    for (size_t index = 0; index < cbm_index_policy_key_count(); index++) {
        const cbm_index_limit_u64_t *source =
            (const cbm_index_limit_u64_t *)((const unsigned char *)policy +
                                            INDEX_POLICY_METADATA[index].field_offset);
        overrides[index] = *source;
    }
    apply_profile_baseline(&candidate, profile);
    for (size_t index = 0; index < cbm_index_policy_key_count(); index++) {
        if ((candidate.override_mask & (UINT64_C(1) << index)) == 0) {
            continue;
        }
        cbm_index_limit_u64_t *target =
            (cbm_index_limit_u64_t *)((unsigned char *)&candidate +
                                      INDEX_POLICY_METADATA[index].field_offset);
        *target = overrides[index];
    }
    *policy = candidate;
    return true;
}

void cbm_index_policy_finalize(cbm_index_resource_policy_t *policy, uint64_t host_memory_bytes,
                               uint64_t soft_budget_bytes) {
    if (!policy) {
        return;
    }
    uint64_t host_cap = host_memory_bytes > 0
                            ? (host_memory_bytes / 5U) * 4U + ((host_memory_bytes % 5U) * 4U) / 5U
                            : 0;
    const uint64_t rss_override_bit = UINT64_C(1) << 2U;
    if ((policy->override_mask & rss_override_bit) == 0) {
        if (policy->profile == CBM_INDEX_PROFILE_BALANCED) {
            /* Balanced guards the host, not the workload. Any fixed ceiling
             * below what this machine can actually finish would reject indexes
             * that succeed today — the recorded large-repository runs peak far
             * above any round number worth hard-coding. The host share is the
             * only honest balanced answer; the fixed floor is a fallback for
             * hosts whose memory cannot be read. */
            uint64_t doubled =
                soft_budget_bytes > UINT64_MAX / 2U ? UINT64_MAX : soft_budget_bytes * 2U;
            uint64_t fallback = doubled > mib(UINT64_C(8192)) ? doubled : mib(UINT64_C(8192));
            policy->max_rss_bytes = limit_value(host_cap > 0 ? host_cap : fallback);
        } else if (policy->profile == CBM_INDEX_PROFILE_STRICT) {
            /* Strict holds the worker to the daemon's own soft budget, so a
             * repository larger than that budget fails fast and attributed
             * instead of being finished at the host's expense. */
            uint64_t baseline = soft_budget_bytes > 0 ? soft_budget_bytes : mib(UINT64_C(8192));
            if (baseline > mib(UINT64_C(8192))) {
                baseline = mib(UINT64_C(8192));
            }
            policy->max_rss_bytes = limit_value(baseline);
        }
    }
    if (policy->max_rss_bytes.enabled && host_cap > 0 && policy->max_rss_bytes.value > host_cap) {
        policy->max_rss_bytes.value = host_cap;
    }
    if (policy->max_rss_bytes.enabled) {
        uint64_t minimum = mib(CBM_INDEX_MIN_RSS_MB_VALUE);
        uint64_t maximum = mib(CBM_INDEX_MAX_RSS_MB_VALUE);
        if (policy->max_rss_bytes.value < minimum) {
            policy->max_rss_bytes.value = minimum;
        } else if (policy->max_rss_bytes.value > maximum) {
            policy->max_rss_bytes.value = maximum;
        }
    }
}

const char *cbm_index_policy_profile_name(const cbm_index_resource_policy_t *policy) {
    if (!policy || policy->profile == CBM_INDEX_PROFILE_OFF) {
        return "off";
    }
    return policy->profile == CBM_INDEX_PROFILE_BALANCED ? "balanced" : "strict";
}

const char *cbm_index_policy_source_name(const cbm_index_resource_policy_t *policy) {
    bool profile = policy && policy->profile != CBM_INDEX_PROFILE_OFF;
    bool override = policy && policy->override_mask != 0;
    if (profile && override) {
        return "profile+override";
    }
    if (profile) {
        return "profile";
    }
    return override ? "override" : "off";
}

bool cbm_index_policy_is_config_key(const char *key) {
    if (key && strcmp(key, CBM_INDEX_CONFIG_RESOURCE_PROFILE) == 0) {
        return true;
    }
    for (size_t index = 0; key && index < cbm_index_policy_key_count(); index++) {
        if (strcmp(key, INDEX_POLICY_METADATA[index].key) == 0) {
            return true;
        }
    }
    return false;
}

bool cbm_index_policy_enabled(const cbm_index_resource_policy_t *policy) {
    return cbm_index_policy_discovery_enabled(policy) || cbm_index_policy_worker_enabled(policy) ||
           cbm_index_policy_storage_enabled(policy);
}

bool cbm_index_policy_discovery_enabled(const cbm_index_resource_policy_t *policy) {
    return policy && (policy->max_files.enabled || policy->max_directories.enabled ||
                      policy->max_entries.enabled || policy->max_depth.enabled ||
                      policy->max_source_bytes.enabled || policy->discovery_deadline_ms.enabled);
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
    policy->override_mask |= UINT64_C(1) << (size_t)(metadata - INDEX_POLICY_METADATA);
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
    case CBM_INDEX_RESOURCE_DIRECTORIES:
        return "directories";
    case CBM_INDEX_RESOURCE_ENTRIES:
        return "entries";
    case CBM_INDEX_RESOURCE_DEPTH:
        return "depth";
    case CBM_INDEX_RESOURCE_DISCOVERY_DURATION_MS:
        return "discovery_duration_ms";
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
    if (resource == CBM_INDEX_RESOURCE_DISCOVERY_DURATION_MS) {
        return "milliseconds";
    }
    if (resource == CBM_INDEX_RESOURCE_DIRECTORIES || resource == CBM_INDEX_RESOURCE_ENTRIES ||
        resource == CBM_INDEX_RESOURCE_DEPTH) {
        return "count";
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
    case CBM_INDEX_RESOURCE_DIRECTORIES:
    case CBM_INDEX_RESOURCE_ENTRIES:
    case CBM_INDEX_RESOURCE_DEPTH:
    case CBM_INDEX_RESOURCE_DISCOVERY_DURATION_MS:
        return CBM_INDEX_CONFIG_RESOURCE_PROFILE;
    case CBM_INDEX_RESOURCE_NONE:
    default:
        return "index_resource_limit";
    }
}
