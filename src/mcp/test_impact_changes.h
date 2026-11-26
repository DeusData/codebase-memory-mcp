#ifndef CBM_TEST_IMPACT_CHANGES_H
#define CBM_TEST_IMPACT_CHANGES_H

#include "mcp/test_impact.h"

#include <stdbool.h>
#include <stddef.h>

/* Pure reconciliation of pinned --name-status -z --no-renames and -U0 bytes.
 * This is not a graph/coverage identity or provenance certificate. */
typedef struct cbm_changes cbm_changes_t;

typedef enum { CBM_CHANGES_OK = 0, CBM_CHANGES_INVALID, CBM_CHANGES_OOM } cbm_changes_status_t;

typedef enum {
    CBM_CHANGES_UNKNOWN = 0,
    CBM_CHANGES_EMPTY,
    CBM_CHANGES_NONEMPTY
} cbm_changes_state_t;

typedef enum { CBM_CHANGE_HUNKS = 0, CBM_CHANGE_WHOLE_FILE } cbm_change_evidence_t;

enum {
    CBM_CHANGE_NO_HUNKS = 1u << 0,
    CBM_CHANGE_BINARY = 1u << 1,
    CBM_CHANGE_MODE = 1u << 2,
    CBM_CHANGE_TYPE = 1u << 3,
    CBM_CHANGE_UNRECONCILED = 1u << 4
};

enum {
    CBM_CHANGES_PATCH_INCOMPLETE = 1u << 0,
    CBM_CHANGES_PATCH_MISSING_PATH = 1u << 1,
    CBM_CHANGES_PATCH_EXTRA_PATH = 1u << 2,
    CBM_CHANGES_PATCH_DUPLICATE_PATH = 1u << 3,
    CBM_CHANGES_PATCH_FLAG_MISMATCH = 1u << 4,
    CBM_CHANGES_PATCH_WITHOUT_NAMES = 1u << 5
};

typedef struct {
    const unsigned char *path; /* owned NUL-terminated bytes; length is authoritative */
    size_t path_length;
    char status; /* A/M/D/T only */
    cbm_change_evidence_t evidence;
    unsigned reasons;
    const cbm_diff_file_t *patch_file; /* only HUNKS evidence exposes a pointer */
} cbm_change_file_t;

/* Clears *out. Invalid authoritative records expose no prefix. Exact inputs
 * and nested diff owner live until free. Own OOM or NULL diff parse returns
 * OOM/no owner. Legacy reader may mask header OOM as incomplete: such a result
 * retains paths but no hunk pointers and requires outer broad/full fallback.
 * NULL input is allowed only at zero length. No I/O, path expansion or recoding. */
cbm_changes_status_t cbm_changes_parse(const unsigned char *name_status, size_t name_status_length,
                                       const unsigned char *patch, size_t patch_length,
                                       cbm_changes_t **out);
void cbm_changes_free(cbm_changes_t *changes);

/* Complete authoritative inventory, sorted by unsigned path bytes. Never
 * interpret count==0 or NULL patch_file as proof of no changes. */
const cbm_change_file_t *cbm_changes_files(const cbm_changes_t *changes, size_t *count);
/* EMPTY requires exact empty names AND exact empty complete patch. NULL and
 * empty names conflicting with nonempty patch return UNKNOWN. */
cbm_changes_state_t cbm_changes_state(const cbm_changes_t *changes);
bool cbm_changes_patch_reconciled(const cbm_changes_t *changes);
/* True only for reconciled all-hunk evidence or verified EMPTY. Whole-file
 * evidence requires later conservative file-level handling, never omission. */
bool cbm_changes_can_narrow(const cbm_changes_t *changes);
unsigned cbm_changes_issues(const cbm_changes_t *changes);

#endif
