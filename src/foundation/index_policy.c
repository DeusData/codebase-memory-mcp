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
    if (!policy) {
        return false;
    }
    for (size_t index = 0; index < cbm_index_policy_key_count(); index++) {
        const cbm_index_limit_u64_t *limit =
            (const cbm_index_limit_u64_t *)((const unsigned char *)policy +
                                            INDEX_POLICY_METADATA[index].field_offset);
        if (limit->enabled) {
            return true;
        }
    }
    return false;
}

bool cbm_index_policy_discovery_enabled(const cbm_index_resource_policy_t *policy) {
    return policy && (policy->max_files.enabled || policy->max_source_bytes.enabled);
}

bool cbm_index_policy_worker_enabled(const cbm_index_resource_policy_t *policy) {
    return policy && (policy->max_rss_bytes.enabled || policy->max_duration_ms.enabled);
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
    case CBM_INDEX_RESOURCE_NONE:
    default:
        return "index_resource_limit";
    }
}
