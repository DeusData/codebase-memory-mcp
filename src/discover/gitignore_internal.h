#ifndef CBM_GITIGNORE_INTERNAL_H
#define CBM_GITIGNORE_INTERNAL_H
#include "discover/gitignore_checked.h"
#include "foundation/arena.h"

typedef struct {
    char *pattern;
    bool negated;
    bool dir_only;
    bool rooted;
} gi_pattern_t;

typedef struct {
    const char *text;
    size_t length;
    bool negated;
    bool dir_only;
    bool rooted;
} gi_view_t;

struct cbm_ignore_checked {
    CBMArena arena;
    cbm_ignore_checked_limits_t limits;
    cbm_ignore_checked_usage_t usage;
    cbm_ignore_checked_error_t error;
};

struct cbm_ignore_program {
    const cbm_ignore_checked_t *owner;
    size_t count;
    gi_pattern_t rows[];
};

typedef struct {
    cbm_ignore_checked_t *owner;
    const cbm_ignore_checked_control_t *control;
    size_t offset;
    unsigned ticks;
    unsigned depth;
} gi_guard_t;

enum { GI_POLL_UNITS = 4096, GI_MATCH_MAX_STEPS = 20000, GI_ARENA_ALIGN = 7 };
enum { GI_FAULT_NONE, GI_FAULT_PROGRAM, GI_FAULT_PATTERN };

void gi_error(cbm_ignore_checked_error_t *error, cbm_ignore_checked_status_t status,
              cbm_ignore_cap_t cap, size_t offset);
bool gi_fail(gi_guard_t *guard, cbm_ignore_checked_status_t status, cbm_ignore_cap_t cap);
bool gi_active(const gi_guard_t *guard);
bool gi_poll(gi_guard_t *guard);
bool gi_work(gi_guard_t *guard, size_t units);
bool gi_bytes(gi_guard_t *guard, size_t bytes);
bool gi_pattern_reserve(gi_guard_t *guard);
bool gi_begin(cbm_ignore_checked_t *owner, const cbm_ignore_checked_control_t *control,
              gi_guard_t *guard);
cbm_ignore_checked_status_t gi_finish(const cbm_ignore_checked_t *owner,
                                      cbm_ignore_checked_error_t *error);
void *gi_allocate(gi_guard_t *guard, size_t bytes, bool simulate_oom);
bool gi_zero(gi_guard_t *guard, void *storage, size_t bytes);
bool gi_normalize(const char *line, size_t length, gi_view_t *view, gi_guard_t *guard);
int gi_core_match(const gi_pattern_t *rows, size_t count, const char *path, bool is_directory,
                  gi_guard_t *guard);
#endif
