/* Internal seams shared by platform implementations and focused tests. */
#ifndef CBM_PLATFORM_INTERNAL_H
#define CBM_PLATFORM_INTERNAL_H

#include <stdbool.h>
#include <stdint.h>

typedef enum {
    CBM_PLATFORM_PROCESS_GROUP_UNKNOWN = 0,
    CBM_PLATFORM_PROCESS_GROUP_QUIESCED,
    CBM_PLATFORM_PROCESS_GROUP_ACTIVE,
} cbm_platform_process_group_state_t;

/* Convert a monotonic counter to nanoseconds using its ticks-per-second
 * frequency. Kept outside the Windows guard so arithmetic edge cases can be
 * verified on every supported build host. */
uint64_t cbm_platform_scale_counter_ns(uint64_t counter, uint64_t frequency);

/* Parse Linux /proc/<pid>/stat after the command name, whose parentheses and
 * spaces make field-splitting from the left incorrect. Kept platform-neutral so
 * the Linux parser contract is tested on macOS and Windows build hosts too. */
bool cbm_platform_parse_proc_stat_group(const char *stat_line, int64_t *process_group,
                                        bool *execution_quiescent);

/* Inspect whether a POSIX process group has any member that can still execute.
 * UNKNOWN is fail-closed: the platform lacks a process table, access was denied,
 * or a snapshot could not be read consistently. Windows subprocess containment
 * uses Job Objects instead and therefore returns UNKNOWN here. */
cbm_platform_process_group_state_t cbm_platform_process_group_state(int64_t pgid);

#endif /* CBM_PLATFORM_INTERNAL_H */
