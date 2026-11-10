#ifndef CBM_DISCOVER_POLICY_INTERNAL_H
#define CBM_DISCOVER_POLICY_INTERNAL_H
#include <stdbool.h>

/* Pure legacy policy predicates; callers provide live terminated strings. */
bool cbm_discover_path_has_skip_suffix(const char *relative_path);
bool cbm_discover_is_safety_core_directory(const char *name);
bool cbm_discover_path_in_jvm_package(const char *relative_path);

#endif
