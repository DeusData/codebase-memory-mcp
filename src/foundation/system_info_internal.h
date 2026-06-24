/*
 * system_info_internal.h — Internal helpers exposed for testing.
 *
 * These functions are implementation details of system_info.c; they are
 * declared here only so that test_platform.c can drive them against a
 * fake cgroup filesystem. Production code outside system_info.c should
 * use the public APIs in platform.h instead.
 */
#ifndef CBM_FOUNDATION_SYSTEM_INFO_INTERNAL_H
#define CBM_FOUNDATION_SYSTEM_INFO_INTERNAL_H

#include <stddef.h>

#ifdef __linux__

/*
 * Effective CPU count for the cgroup rooted at `cgroup_root`.
 *
 * Reads (in order):
 *   1. cgroup v2: "<cgroup_root>/cpu.max"            ("<quota> <period>" or "max ...")
 *   2. cgroup v1: "<cgroup_root>/cpu/cpu.cfs_quota_us" + ".../cpu.cfs_period_us"
 *
 * Returns ceil(quota / period) (>= 1) when a valid CPU quota is in place.
 * Returns -1 when no cgroup limit is present (caller should fall back to
 * sysconf(_SC_NPROCESSORS_ONLN)).
 */
int cbm_detect_cgroup_cpus(const char *cgroup_root);

/*
 * Effective memory limit (bytes) for the cgroup rooted at `cgroup_root`.
 *
 * Reads (in order):
 *   1. cgroup v2: "<cgroup_root>/memory.max"             ("max" or integer bytes)
 *   2. cgroup v1: "<cgroup_root>/memory/memory.limit_in_bytes"
 *
 * Returns the byte count when a finite limit is in place. Returns 0 when
 * no cgroup limit is present, the limit is "max"/unlimited, or the value
 * is so large it represents the cgroup-v1 "unlimited" sentinel.
 */
size_t cbm_detect_cgroup_mem(const char *cgroup_root);

#endif /* __linux__ */

#endif /* CBM_FOUNDATION_SYSTEM_INFO_INTERNAL_H */
