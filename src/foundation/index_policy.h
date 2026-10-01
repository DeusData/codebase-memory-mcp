#ifndef CBM_INDEX_POLICY_H
#define CBM_INDEX_POLICY_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define CBM_INDEX_CONFIG_MAX_FILES "index_max_files"
#define CBM_INDEX_CONFIG_MAX_SOURCE_MB "index_max_source_mb"

#define CBM_INDEX_MAX_FILES_VALUE UINT64_C(10000000)
#define CBM_INDEX_MAX_SOURCE_MB_VALUE UINT64_C(1048576)
#define CBM_INDEX_MIB_BYTES UINT64_C(1048576)

typedef struct {
    bool enabled;
    uint64_t value;
} cbm_index_limit_u64_t;

typedef struct {
    cbm_index_limit_u64_t max_files;
    cbm_index_limit_u64_t max_source_bytes;
} cbm_index_resource_policy_t;

typedef enum {
    CBM_INDEX_RESOURCE_NONE = 0,
    CBM_INDEX_RESOURCE_FILES,
    CBM_INDEX_RESOURCE_SOURCE_BYTES,
} cbm_index_resource_t;

typedef struct {
    cbm_index_resource_t resource;
    uint64_t observed;
    uint64_t limit;
} cbm_index_resource_violation_t;

void cbm_index_policy_init(cbm_index_resource_policy_t *policy);
bool cbm_index_policy_enabled(const cbm_index_resource_policy_t *policy);

size_t cbm_index_policy_key_count(void);
const char *cbm_index_policy_key_at(size_t index);
const char *cbm_index_policy_default_value(const char *key);

bool cbm_index_policy_set(cbm_index_resource_policy_t *policy, const char *key, const char *value,
                          char *error, size_t error_size);
bool cbm_index_policy_format(const cbm_index_resource_policy_t *policy, const char *key, char *out,
                             size_t out_size);

const char *cbm_index_resource_name(cbm_index_resource_t resource);
const char *cbm_index_resource_unit(cbm_index_resource_t resource);
const char *cbm_index_resource_config_key(cbm_index_resource_t resource);

#endif
