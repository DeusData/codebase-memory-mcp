/* DRAFT internal pre-language inventory filter; parent review required.
 * Proposed destination: src/discover/inventory_filter.h.
 * No language classification, full-discovery success, pipeline or admission API.
 * Shared inventory data types can be included by the later inventory.h rather
 * than redeclared. Normative behavior is in contract.txt.
 */
#ifndef CBM_DISCOVER_INVENTORY_FILTER_H
#define CBM_DISCOVER_INVENTORY_FILTER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum {
    CBM_INVENTORY_OK = 0,
    CBM_INVENTORY_INVALID,
    CBM_INVENTORY_STATE,
    CBM_INVENTORY_CHANGED,
    CBM_INVENTORY_UNSUPPORTED,
    CBM_INVENTORY_LIMIT,
    CBM_INVENTORY_OOM,
    CBM_INVENTORY_IO,
    CBM_INVENTORY_CANCELLED,
    CBM_INVENTORY_DEADLINE
} cbm_inventory_status_t;

typedef struct {
    cbm_inventory_status_t status;
    size_t file_index;     /* SIZE_MAX unless a particular input row is known. */
    bool cleanup_required; /* Independent provider cleanup obligation. */
    char diagnostic[256];  /* Owned bounded generic text, no input contents. */
} cbm_inventory_error_t;

typedef struct {
    const unsigned char *data;
    size_t length;
} cbm_inventory_path_t;

typedef struct {
    cbm_inventory_path_t path; /* data[length]==0; no earlier NUL. */
    uint32_t git_mode;         /* 0100644 or 0100755. */
    char oid[65];              /* First NUL at 40 or 64; preceding lowercase hex only. */
    uint64_t content_length;
    unsigned char content_sha256[32];
} cbm_inventory_file_t;

typedef struct {
    uint64_t deadline_ms;             /* Positive absolute cbm_now_ms deadline. */
    bool (*cancelled)(void *context); /* Optional prompt, pure, non-reentrant. */
    void *context;                    /* Borrowed only until prepare returns. */
} cbm_inventory_control_t;

typedef struct {
    size_t max_files;              /* Positive, <=INT_MAX; complete inventory, not survivors. */
    size_t max_directories;        /* Positive, <=INT_MAX; root plus proper prefixes. */
    size_t max_arena_bytes;        /* Total cumulative logical requests/reservations. */
    size_t max_ignore_arena_bytes; /* Positive reserved slice <= total. */
    size_t max_control_file_bytes;
    uint64_t max_control_total_bytes;
    size_t max_ignore_patterns;    /* Positive, <=INT_MAX. */
    size_t max_probe_prefix_bytes; /* Copied/validated; unused before language. */
    uint64_t max_ignore_work;      /* Exclusive checked parse+match allowance. */
    uint64_t max_verified_file_reads;
    uint64_t max_verified_content_bytes;
} cbm_inventory_limits_t;

/* Contractual full-file verification followed by min(capacity,length) exact
 * prefix bytes. OK publishes copied; error publishes copied==0. No terminator.
 * Serial; all inputs borrowed. An error may require caller-owned native cleanup.
 * This callback type cannot authenticate a synthetic provider.
 */
typedef cbm_inventory_status_t (*cbm_inventory_read_fn)(void *context, size_t file_index,
                                                        unsigned char *prefix, size_t capacity,
                                                        size_t *copied,
                                                        const cbm_inventory_control_t *control,
                                                        cbm_inventory_error_t *error);

typedef struct {
    const char *native_root;
    const cbm_inventory_file_t *files;
    size_t file_count;
    unsigned char manifest_sha256[32]; /* Copied opaque binding, not authenticated. */
    cbm_inventory_read_fn read;
    void *read_context;
} cbm_inventory_source_t;

/* Separate internal type: never cast to the future final disposition enum. */
typedef enum {
    CBM_INVENTORY_FILTER_NEEDS_LANGUAGE = 0,
    CBM_INVENTORY_FILTER_IGNORED_FILE,
    CBM_INVENTORY_FILTER_EXCLUDED_SUBTREE
} cbm_inventory_filter_disposition_t;

typedef enum {
    CBM_INVENTORY_FILTER_REASON_NONE = 0,
    CBM_INVENTORY_FILTER_REASON_DIRECTORY_BUILTIN,
    CBM_INVENTORY_FILTER_REASON_DIRECTORY_SUFFIX,
    CBM_INVENTORY_FILTER_REASON_GITIGNORE,
    CBM_INVENTORY_FILTER_REASON_CBMIGNORE,
    CBM_INVENTORY_FILTER_REASON_IGNORED_SUFFIX,
    CBM_INVENTORY_FILTER_REASON_SKIP_LIST,
    CBM_INVENTORY_FILTER_REASON_FAST_PATTERN
} cbm_inventory_filter_reason_t;

enum {
    CBM_INVENTORY_FILTER_ROLE_GITIGNORE = 1u << 0,
    CBM_INVENTORY_FILTER_ROLE_CBMIGNORE = 1u << 1,
    CBM_INVENTORY_FILTER_ROLE_PHYSICAL_PROJECT_CONFIG = 1u << 2
};

typedef struct {
    size_t file_index;
    cbm_inventory_filter_disposition_t disposition;
    cbm_inventory_filter_reason_t reason;
    unsigned roles;
    cbm_inventory_path_t excluded_ancestor; /* Otherwise {NULL,0}. */
} cbm_inventory_filter_row_t;

typedef enum {
    CBM_INVENTORY_CONTROL_GITIGNORE = 0,
    CBM_INVENTORY_CONTROL_CBMIGNORE
} cbm_inventory_control_kind_t;

typedef enum {
    CBM_INVENTORY_CONTROL_ABSENT = 0,
    CBM_INVENTORY_CONTROL_APPLIED_EMPTY,
    CBM_INVENTORY_CONTROL_APPLIED
} cbm_inventory_control_outcome_t;

typedef struct {
    cbm_inventory_control_kind_t kind;
    cbm_inventory_path_t directory; /* Empty root or reached directory. */
    size_t file_index;              /* SIZE_MAX iff ABSENT. */
    cbm_inventory_control_outcome_t outcome;
    size_t effective_patterns;
} cbm_inventory_control_row_t;

typedef struct {
    uint64_t verified_file_reads_reserved;
    uint64_t verified_content_bytes_reserved;
    uint64_t control_bytes_reserved;
    uint64_t ignore_bytes_reserved;
    uint64_t ignore_work_used;
    size_t ignore_patterns_reserved;
    size_t ignore_arena_requested_bytes;
    size_t non_ignore_arena_requested_bytes;
    size_t arena_requested_bytes;   /* D+C actual logical requests. */
    size_t arena_budget_used_bytes; /* D+I unavailable quota. */
} cbm_inventory_filter_usage_t;

typedef struct cbm_inventory_filter cbm_inventory_filter_t;

typedef struct {
    const char *native_root;
    const cbm_inventory_file_t *files;
    const cbm_inventory_filter_row_t *rows;
    size_t file_count;      /* Exactly the complete input count. */
    size_t directory_count; /* Includes excluded directories and root. */
    const cbm_inventory_control_row_t *controls;
    size_t control_count;
    const cbm_inventory_path_t *excluded_directories; /* Minimal roots only. */
    size_t excluded_count;
    unsigned char manifest_sha256[32];
} cbm_inventory_filter_view_t;

/* Synchronous FULL/no-size-skip pre-language pass. Clears out/error first.
 * Required source/limits/control/out; read callback required even for empty input.
 * No config/global/Git/directory enumeration/stat/non-control content read.
 * Success owns all returned metadata/rows/strings and one checked-ignore owner;
 * retains neither callbacks nor caller contexts. Failure: out==NULL, no partial
 * view. Native cleanup remains the provider caller's responsibility.
 */
cbm_inventory_status_t cbm_inventory_filter_prepare(const cbm_inventory_source_t *source,
                                                    const cbm_inventory_limits_t *limits,
                                                    const cbm_inventory_control_t *control,
                                                    cbm_inventory_filter_t **out,
                                                    cbm_inventory_error_t *error);

/* Borrowed read-only view until free; NULL for NULL. No IO/poll/allocation. */
const cbm_inventory_filter_view_t *cbm_inventory_filter_view(const cbm_inventory_filter_t *owner);

/* Clears out; false for NULL owner/output. Copies usage or original limits.
 * These are diagnostics, never a capability to replenish/reset a live owner.
 */
bool cbm_inventory_filter_usage(const cbm_inventory_filter_t *owner,
                                cbm_inventory_filter_usage_t *out);
bool cbm_inventory_filter_limits(const cbm_inventory_filter_t *owner, cbm_inventory_limits_t *out);

/* NULL-safe; frees checked owner before the containing arena; no callback/IO. */
void cbm_inventory_filter_free(cbm_inventory_filter_t *owner);

#endif
