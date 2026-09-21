#include "foundation/index_policy.h"

#include <stdio.h>
#include <string.h>

static const char *const INDEX_POLICY_KEYS[] = {
    CBM_INDEX_CONFIG_MAX_FILES,
    CBM_INDEX_CONFIG_MAX_SOURCE_MB,
};

static void set_error(char *error, size_t error_size, const char *key, uint64_t maximum) {
    if (error && error_size > 0) {
        (void)snprintf(error, error_size, "%s must be off or an integer from 1 to %llu", key,
                       (unsigned long long)maximum);
    }
}

static bool parse_bounded_uint64(const char *value, uint64_t maximum, uint64_t *parsed) {
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
    if (result == 0) {
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
    return policy && (policy->max_files.enabled || policy->max_source_bytes.enabled);
}

size_t cbm_index_policy_key_count(void) {
    return sizeof(INDEX_POLICY_KEYS) / sizeof(INDEX_POLICY_KEYS[0]);
}

const char *cbm_index_policy_key_at(size_t index) {
    return index < cbm_index_policy_key_count() ? INDEX_POLICY_KEYS[index] : NULL;
}

const char *cbm_index_policy_default_value(const char *key) {
    for (size_t index = 0; index < cbm_index_policy_key_count(); index++) {
        if (key && strcmp(key, INDEX_POLICY_KEYS[index]) == 0) {
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
        set_error(error, error_size, key ? key : "index resource limit", 0);
        return false;
    }

    cbm_index_limit_u64_t *target = NULL;
    uint64_t maximum = 0;
    uint64_t multiplier = 1;
    if (strcmp(key, CBM_INDEX_CONFIG_MAX_FILES) == 0) {
        target = &policy->max_files;
        maximum = CBM_INDEX_MAX_FILES_VALUE;
    } else if (strcmp(key, CBM_INDEX_CONFIG_MAX_SOURCE_MB) == 0) {
        target = &policy->max_source_bytes;
        maximum = CBM_INDEX_MAX_SOURCE_MB_VALUE;
        multiplier = CBM_INDEX_MIB_BYTES;
    } else {
        set_error(error, error_size, key, 0);
        return false;
    }

    cbm_index_limit_u64_t candidate = {0};
    if (strcmp(value, "off") != 0) {
        uint64_t parsed = 0;
        if (!parse_bounded_uint64(value, maximum, &parsed)) {
            set_error(error, error_size, key, maximum);
            return false;
        }
        candidate.enabled = true;
        candidate.value = parsed * multiplier;
    }
    *target = candidate;
    return true;
}

bool cbm_index_policy_format(const cbm_index_resource_policy_t *policy, const char *key, char *out,
                             size_t out_size) {
    if (!policy || !key || !out || out_size == 0) {
        return false;
    }
    const cbm_index_limit_u64_t *limit = NULL;
    uint64_t divisor = 1;
    if (strcmp(key, CBM_INDEX_CONFIG_MAX_FILES) == 0) {
        limit = &policy->max_files;
    } else if (strcmp(key, CBM_INDEX_CONFIG_MAX_SOURCE_MB) == 0) {
        limit = &policy->max_source_bytes;
        divisor = CBM_INDEX_MIB_BYTES;
    } else {
        return false;
    }
    int length = limit->enabled
                     ? snprintf(out, out_size, "%llu", (unsigned long long)(limit->value / divisor))
                     : snprintf(out, out_size, "off");
    return length >= 0 && (size_t)length < out_size;
}

const char *cbm_index_resource_name(cbm_index_resource_t resource) {
    switch (resource) {
    case CBM_INDEX_RESOURCE_FILES:
        return "files";
    case CBM_INDEX_RESOURCE_SOURCE_BYTES:
        return "source_bytes";
    case CBM_INDEX_RESOURCE_NONE:
    default:
        return "unknown";
    }
}

const char *cbm_index_resource_unit(cbm_index_resource_t resource) {
    return resource == CBM_INDEX_RESOURCE_FILES ? "files" : "bytes";
}

const char *cbm_index_resource_config_key(cbm_index_resource_t resource) {
    switch (resource) {
    case CBM_INDEX_RESOURCE_FILES:
        return CBM_INDEX_CONFIG_MAX_FILES;
    case CBM_INDEX_RESOURCE_SOURCE_BYTES:
        return CBM_INDEX_CONFIG_MAX_SOURCE_MB;
    case CBM_INDEX_RESOURCE_NONE:
    default:
        return "index_resource_limit";
    }
}
