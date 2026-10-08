/* Checked in-memory ignore parsing and matching; no filesystem/MCP dependency. */
#ifndef CBM_GITIGNORE_CHECKED_H
#define CBM_GITIGNORE_CHECKED_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct cbm_ignore_checked cbm_ignore_checked_t;
typedef struct cbm_ignore_program cbm_ignore_program_t;

typedef enum {
    CBM_IGNORE_CHECKED_OK = 0,
    CBM_IGNORE_CHECKED_INVALID,
    CBM_IGNORE_CHECKED_UNSUPPORTED,
    CBM_IGNORE_CHECKED_LIMIT,
    CBM_IGNORE_CHECKED_OOM,
    CBM_IGNORE_CHECKED_CANCELLED,
    CBM_IGNORE_CHECKED_DEADLINE
} cbm_ignore_checked_status_t;

typedef enum {
    CBM_IGNORE_CAP_NONE = 0,
    CBM_IGNORE_CAP_BYTES,
    CBM_IGNORE_CAP_PATTERNS,
    CBM_IGNORE_CAP_WORK,
    CBM_IGNORE_CAP_ARENA,
    CBM_IGNORE_CAP_DEPTH,
    CBM_IGNORE_CAP_ATTEMPT,
    CBM_IGNORE_CAP_REPRESENTATION
} cbm_ignore_cap_t;

typedef enum {
    CBM_IGNORE_REINCLUDED = -1,
    CBM_IGNORE_NO_OPINION = 0,
    CBM_IGNORE_IGNORED = 1,
    CBM_IGNORE_UNAVAILABLE = 2 /* every failure clears the decision to this */
} cbm_ignore_decision_t;

enum { CBM_IGNORE_CHECKED_DEPTH_MAX = 32 };

typedef struct {
    uint64_t max_bytes;     /* aggregate parse lengths and match path lengths + NUL */
    uint64_t max_work;      /* aggregate charged work, including parsing */
    size_t max_patterns;    /* cumulative reservations; positive, <= INT_MAX */
    size_t max_arena_bytes; /* cumulative logical requests, not resident memory */
    unsigned max_depth;     /* 1..CBM_IGNORE_CHECKED_DEPTH_MAX active glob entries */
} cbm_ignore_checked_limits_t;

typedef struct {
    uint64_t deadline_ms;             /* positive absolute cbm_now_ms deadline */
    bool (*cancelled)(void *context); /* optional prompt, pure, non-reentrant */
    void *context;                    /* borrowed for this synchronous call only */
} cbm_ignore_checked_control_t;

typedef struct {
    cbm_ignore_checked_status_t status;
    cbm_ignore_cap_t cap; /* LIMIT only; otherwise NONE */
    size_t byte_offset;   /* input offset when known, otherwise SIZE_MAX */
    char diagnostic[128]; /* bounded owned text, never input contents */
} cbm_ignore_checked_error_t;

typedef struct {
    uint64_t bytes_reserved;
    uint64_t work_used;
    size_t patterns_reserved;
    size_t arena_requested_bytes;
    cbm_ignore_checked_status_t terminal_status;
} cbm_ignore_checked_usage_t;

typedef struct {
    const cbm_ignore_program_t *program; /* borrowed until owner free */
    size_t pattern_count;                /* zero is successful empty/comment-only normalization */
} cbm_ignore_checked_parse_result_t;

/* All caps positive; copies limits. Initializes optional error and clears *out.
 * No callbacks/IO. Failure publishes no owner. Uses a CBMArena, including owner.
 */
cbm_ignore_checked_status_t cbm_ignore_checked_open(const cbm_ignore_checked_limits_t *limits,
                                                    cbm_ignore_checked_t **out,
                                                    cbm_ignore_checked_error_t *error);

/* Exact span: NULL permitted only for length==0; terminator not required.
 * Embedded NUL => UNSUPPORTED, never truncation. Result is required/cleared.
 * Copies normalized patterns; caller may discard bytes after return.
 * Every started failure is terminal for owner; earlier programs remain allocated
 * but no further parse/match can succeed. No partial program is published.
 */
cbm_ignore_checked_status_t cbm_ignore_checked_parse(cbm_ignore_checked_t *owner, const void *bytes,
                                                     size_t length,
                                                     const cbm_ignore_checked_control_t *control,
                                                     cbm_ignore_checked_parse_result_t *out,
                                                     cbm_ignore_checked_error_t *error);

/* program must belong to this live owner. path is readable for length+1 bytes,
 * with path[length]==0 and no earlier NUL. Empty path is permitted. No decoding
 * or path normalization. Inputs borrowed for this call; no match allocation.
 * Required decision is initialized to UNAVAILABLE and set only on full success.
 */
cbm_ignore_checked_status_t cbm_ignore_checked_match(
    cbm_ignore_checked_t *owner, const cbm_ignore_program_t *program, const char *path,
    size_t length, bool is_directory, const cbm_ignore_checked_control_t *control,
    cbm_ignore_decision_t *decision, cbm_ignore_checked_error_t *error);

/* Read-only; clears output, false for NULL owner/output. Does not consume quotas,
 * poll or clear a terminal failure. No operation may overlap any other call.
 */
bool cbm_ignore_checked_usage(const cbm_ignore_checked_t *owner, cbm_ignore_checked_usage_t *out);
/* NULL-safe; releases all programs/arena at once. No callback/IO or cancellation. */
void cbm_ignore_checked_free(cbm_ignore_checked_t *owner);

/* Test-only per-call allocation failure seams.
 * No persistent/global fault, raw allocator replacement or production option.
 */
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
typedef enum {
    CBM_IGNORE_PARSE_FAULT_NONE = 0,
    CBM_IGNORE_PARSE_FAULT_PROGRAM,
    CBM_IGNORE_PARSE_FAULT_PATTERN
} cbm_ignore_parse_fault_t;

/* Simulate NULL at the owner's first arena allocation, after normal quota checks. */
cbm_ignore_checked_status_t cbm_ignore_checked_open_oom_for_tests(
    const cbm_ignore_checked_limits_t *limits, cbm_ignore_checked_t **out,
    cbm_ignore_checked_error_t *error);

/* PROGRAM: program/header+row-storage allocation. PATTERN: first effective
 * pattern-string allocation. Unreached boundary has no effect. Same normal
 * implementation/error path; no extra allocations or changed matching logic.
 */
cbm_ignore_checked_status_t cbm_ignore_checked_parse_fault_for_tests(
    cbm_ignore_checked_t *owner, const void *bytes, size_t length,
    const cbm_ignore_checked_control_t *control, cbm_ignore_parse_fault_t fault,
    cbm_ignore_checked_parse_result_t *out, cbm_ignore_checked_error_t *error);
#endif

#endif
