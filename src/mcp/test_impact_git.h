#ifndef CBM_TEST_IMPACT_GIT_H
#define CBM_TEST_IMPACT_GIT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Query-local, committed-only facts. Requires the optional stdout_file
 * subprocess seam. No shell, checkout, index mutation, fetch or global state. */
typedef enum {
    CBM_GIT_FACTS_OK = 0,
    CBM_GIT_FACTS_INVALID,
    CBM_GIT_FACTS_OOM,
    CBM_GIT_FACTS_IO,
    CBM_GIT_FACTS_SPAWN,
    CBM_GIT_FACTS_COMMAND,
    CBM_GIT_FACTS_CANCELLED,
    CBM_GIT_FACTS_DEADLINE,
    CBM_GIT_FACTS_LIMIT,
    CBM_GIT_FACTS_SUPERVISION,
    CBM_GIT_FACTS_UNSUPPORTED,
    CBM_GIT_FACTS_NO_MERGE_BASE,
    CBM_GIT_FACTS_AMBIGUOUS_MERGE_BASE,
    CBM_GIT_FACTS_IDENTITY_MISMATCH
} cbm_git_facts_status_t;

typedef struct {
    cbm_git_facts_status_t status;
    int exit_code;        /* -1 when unavailable */
    char diagnostic[512]; /* owned bounded text, never a borrowed pointer */
} cbm_git_facts_error_t;

typedef bool (*cbm_git_facts_cancel_fn)(void *context);

typedef struct {
    const char *root;     /* absolute worktree top-level; Git may normalize its spelling */
    const char *base_ref; /* one commit-ish to resolve once; never a first-parent substitute */
    const char *git_executable; /* trusted internal resolved absolute native binary, not a
                                 * user override; .exe on Windows */
    const char *expected_head;  /* optional full OID; mismatch rejects snapshot creation */
    uint64_t deadline_ms;       /* absolute cbm_now_ms() deadline; required, not a quiet timeout */
    unsigned command_limit;     /* positive query-wide command count */
    size_t stdout_limit;        /* positive per-command exact-byte cap */
    size_t stderr_limit;        /* positive per-command diagnostic-byte cap */
    size_t total_output_limit;  /* positive cumulative stdout+stderr cap across all commands */
    cbm_git_facts_cancel_fn cancelled; /* optional, must return promptly */
    void *cancel_context; /* borrowed until facts_free; callback runs on caller thread */
} cbm_git_facts_options_t;

typedef struct {
    const char *root; /* Git-reported absolute top-level, possibly normalized from options.root */
    const char *git_dir;
    const char *common_dir;
    unsigned oid_hex_length; /* 40 (SHA-1) or 64 (SHA-256) */
    char head[65];
    char base[65];
    char merge_base[65]; /* unique ACTUAL merge-base --all result */
} cbm_git_facts_identity_t;

typedef struct cbm_git_facts cbm_git_facts_t;

/* Copies option strings and owns all returned data in a CBMArena. Shallow
 * repositories and missing/ambiguous common ancestors fail closed. All later
 * source/diff operations use pinned OIDs, even if refs or worktree files change.
 * Cancellation and LIMIT are sticky for this handle: later valid operations
 * return the latched error without starting work or allocating path copies.
 * Error always initializes when supplied; NULL means no usable snapshot. */
cbm_git_facts_t *cbm_git_facts_open(const cbm_git_facts_options_t *options,
                                    cbm_git_facts_error_t *error);
void cbm_git_facts_free(cbm_git_facts_t *facts);
const cbm_git_facts_identity_t *cbm_git_facts_identity(const cbm_git_facts_t *facts);

typedef enum {
    CBM_GIT_ANCESTRY_ERROR = -1,
    CBM_GIT_ANCESTRY_NO = 0,
    CBM_GIT_ANCESTRY_YES = 1
} cbm_git_ancestry_t;

/* Full OIDs of the snapshot's object format only. Normal exit 1 alone means NO;
 * unavailable history/objects, cancellation, caps and other failures mean ERROR. */
cbm_git_ancestry_t cbm_git_facts_is_ancestor(cbm_git_facts_t *facts, const char *ancestor_oid,
                                             const char *descendant_oid,
                                             cbm_git_facts_error_t *error);

typedef enum { CBM_GIT_REV_HEAD = 0, CBM_GIT_REV_BASE, CBM_GIT_REV_MERGE_BASE } cbm_git_revision_t;

typedef struct {
    const unsigned char *data;
    size_t length;
} cbm_git_bytes_t;

typedef enum { CBM_GIT_TREE_BLOB = 1, CBM_GIT_TREE_COMMIT = 2 } cbm_git_tree_object_type_t;

typedef struct {
    const char *path; /* owned NUL-terminated bytes; path_length is authoritative */
    size_t path_length;
    uint32_t mode; /* 0100644/0100755 regular, 0120000 symlink, 0160000 gitlink */
    cbm_git_tree_object_type_t object_type;
    char oid[65];
} cbm_git_tree_entry_t;

typedef struct {
    const cbm_git_tree_entry_t *entries;
    size_t count;
} cbm_git_tree_inventory_t;

/* Complete recursive non-directory inventory of pinned HEAD or actual merge
 * base only (BASE is INVALID). Includes symlink blobs and gitlink commits;
 * callers decide which listed entries are relevant and unsupported. Literal
 * path bytes, including non-UTF8/platform-unrepresentable names, are retained.
 * No worktree/index inventory, filtering, path globbing or object-content read.
 * Entries sort by unsigned path bytes, shorter equal prefix first. Invalid
 * records/duplicate paths fail COMMAND; no successful prefix is exposed.
 * Empty success is {NULL,0}; every failure clears out. All data lives until
 * facts_free. Existing guards, cancellation/deadline and query budgets apply. */
bool cbm_git_facts_inventory(cbm_git_facts_t *facts, cbm_git_revision_t revision,
                             cbm_git_tree_inventory_t *out, cbm_git_facts_error_t *error);

typedef struct {
    cbm_git_bytes_t bytes;
    char oid[65];
    uint32_t mode; /* Git tree mode in ordinary octal notation, e.g. 0100644 */
} cbm_git_blob_t;

typedef enum {
    CBM_GIT_BLOB_ERROR = -1,
    CBM_GIT_BLOB_ABSENT = 0,
    CBM_GIT_BLOB_FOUND = 1
} cbm_git_blob_status_t;

/* Literal repository-relative path bytes with explicit length, no NUL or
 * absolute/dot/dot-dot components. Only regular blob modes are supported;
 * symlinks, directories, gitlinks and unsupported argument encodings are ERROR.
 * ABSENT requires a successful exact lookup in the pinned tree. Clears out on
 * ABSENT/ERROR. No filesystem glob expansion, filters or symlink traversal. */
cbm_git_blob_status_t cbm_git_facts_read_blob(cbm_git_facts_t *facts, cbm_git_revision_t revision,
                                              const char *path, size_t path_length,
                                              cbm_git_blob_t *out, cbm_git_facts_error_t *error);

typedef struct {
    cbm_git_bytes_t patch;       /* pinned existing test-impact diff flags, including -U0 */
    cbm_git_bytes_t name_status; /* --name-status -z --no-renames, exact Git bytes */
} cbm_git_diff_t;

/* Unique pinned merge-base -> pinned HEAD only. Includes changed-path metadata
 * so binary/mode/deletion-only changes cannot masquerade as an empty patch.
 * Attributes may affect text/binary presentation and hunk headers; consumers
 * must conservatively select a changed file without usable text hunks.
 * Clears out on any failure. Returned bytes live until facts_free. */
bool cbm_git_facts_diff(cbm_git_facts_t *facts, cbm_git_diff_t *out, cbm_git_facts_error_t *error);

/* Compare full commit A to pinned HEAD only if A is an ancestor of the pinned
 * actual merge-base. A is an explicit 40/64-byte hex span, copied before work.
 * Same-owner calls are serialized; callbacks must not re-enter facts operations.
 * This shares the existing owner, guards, executor and budgets. No patch is read.
 *
 * Only OK publishes the complete AMDT/NUL raw name/status stream. NOT_ANCESTOR
 * is a normal exit-1 result with error.status OK and zero output, not an error
 * or a poisoned owner. ERROR also clears output. Existing sticky errors apply.
 * All successful buffers remain valid until facts_free, including earlier diff
 * results. Empty OK has length zero, with either NULL or retained capture data.
 *
 * Output path bytes use raw inventory structural rules on every platform.
 * No sort/deduplication or uniqueness/origin certificate is supplied; consumers
 * validate cross-record identity separately. Empty A-to-H cannot determine the
 * request's M-to-H has_changes, and this API does not authenticate artifact A. */
typedef enum {
    CBM_GIT_ANCESTOR_CHANGES_ERROR = -1,
    CBM_GIT_ANCESTOR_CHANGES_NOT_ANCESTOR = 0,
    CBM_GIT_ANCESTOR_CHANGES_OK = 1
} cbm_git_ancestor_changes_status_t;

cbm_git_ancestor_changes_status_t cbm_git_facts_ancestor_changes(cbm_git_facts_t *facts,
                                                                 const char *artifact_oid,
                                                                 size_t artifact_oid_length,
                                                                 cbm_git_bytes_t *name_status,
                                                                 cbm_git_facts_error_t *error);

typedef struct {
    cbm_git_revision_t revision; /* HEAD or actual MERGE_BASE; BASE is INVALID */
    const size_t *indices;       /* indices into that revision's canonical pinned inventory */
    size_t count; /* duplicates allowed; NULL indices allowed exactly when count==0 */
} cbm_git_blob_batch_request_t;

typedef struct {
    size_t max_entries;     /* positive; bounds request.count, including duplicates */
    size_t max_input_bytes; /* positive; cumulative OID+LF stdin bytes for this call */
    size_t
        max_arena_bytes; /* positive; all Git-facts-owned logical arena requests during this call */
} cbm_git_blob_batch_limits_t;

typedef struct {
    size_t inventory_index;
    const cbm_git_tree_entry_t *entry; /* facts-owned canonical path/mode/type/OID */
    cbm_git_bytes_t bytes;             /* exact blob contents; NOT promised NUL-terminated */
} cbm_git_blob_batch_item_t;

typedef struct {
    cbm_git_revision_t revision;
    const char *commit; /* facts-owned full pinned OID; also present on empty success */
    const cbm_git_blob_batch_item_t *items;
    size_t count;
} cbm_git_blob_batch_t;

/* Read selected regular blobs without caller-supplied path/OID pairs. Internally
 * obtain/cache a completely validated inventory; indices have the same unsigned
 * raw-path ordering as cbm_git_facts_inventory(). Allow 0100644 and 0100755 only.
 * Symlink/gitlink selections are UNSUPPORTED; any index>=inventory.count is
 * INVALID. Validate the entire selection before any cat-file command.
 *
 * Preserve caller order and every duplicate selection. Distinct paths and modes
 * sharing one OID remain distinct items. Payload storage may alias between them;
 * pointer identity is not an API promise. All returned pointers, including earlier
 * successful results, remain immutable and valid until cbm_git_facts_free().
 * No one-process-per-file fallback, shell, source filters or worktree lookup.
 *
 * A synchronous all-or-error operation. Clear *out before other validation when
 * out is non-NULL; initialize optional error on every call. Only true publishes
 * items; false leaves every out field zero, including commit/items/count. Empty
 * success has items=NULL,count=0 and the selected pinned revision/commit; it does
 * not assert that the pinned tree itself is empty. Input views must be valid and
 * immutable until return; callbacks may not mutate inputs or re-enter facts.
 * Same-owner calls are serialized; different owners are independent.
 *
 * Native cat-file preflights ALL unique selected OIDs before ANY payload command.
 * Exact frame sizes must fit the existing per-command stdout cap; an oversized
 * single frame is LIMIT before payload capture. Output is then captured in
 * bounded batches, and every count/order/OID/type/size/separator/EOF is validated.
 * Malformed/missing frames are COMMAND, never ABSENT or a successful prefix.
 *
 * Existing command/output/cancellation/deadline/history/topology rules apply,
 * including on inventory-cache hits and empty requests. Preflight headers,
 * payload framing, diagnostics and guard output all consume existing budgets.
 * New positive limits are per call and cannot raise those shared limits. LIMIT
 * and cancellation use existing sticky owner errors; prior results survive.
 * Parsing/copying/writing work polls at byte chunks no larger than 65536, and
 * before final publication. A failure never retries after an owner latch.
 *
 * Input files are private, closed and immutable before spawn, and retained until
 * terminal tree quiescence. Failed containment returns SUPERVISION and retains
 * the whole capture directory, including stdin, under existing diagnostics.
 * No legacy read_blob/inventory/diff/ancestor-change semantics are changed. */
bool cbm_git_facts_read_blob_batch(cbm_git_facts_t *facts,
                                   const cbm_git_blob_batch_request_t *request,
                                   const cbm_git_blob_batch_limits_t *limits,
                                   cbm_git_blob_batch_t *out, cbm_git_facts_error_t *error);

#ifdef CBM_ENABLE_TEST_SEAMS
/* Deterministic wrappers around the actual production header/frame parsers.
 * No Git, filesystem, environment, runtime fake transport, or facts owner.
 * Required expected OIDs are full canonical lowercase strings of width 40/64.
 * Header wrapper consumes exactly the first header, allowing following payload.
 * It clears both outputs first; valid size overflow is LIMIT, bad grammar COMMAND.
 * Capture wrapper validates exact count/order/EOF. In preflight mode sizes may
 * be NULL (unknown); otherwise each parsed size must match expected_sizes[i].
 * Payload mode requires expected_sizes for nonzero count. Empty capture/count
 * is valid. Bad pointer/width arguments are INVALID. No partial output views. */
bool cbm_git_facts_test_batch_header(cbm_git_bytes_t capture, const char *expected_oid,
                                     size_t oid_hex_length, size_t *object_size,
                                     size_t *header_length, cbm_git_facts_error_t *error);
bool cbm_git_facts_test_batch_capture(cbm_git_bytes_t capture, size_t oid_hex_length,
                                      const char *const *expected_oids,
                                      const size_t *expected_sizes, size_t count, bool has_payload,
                                      cbm_git_facts_error_t *error);
#endif

/* Local Git identity/content does not authenticate a coverage artifact or
 * establish that any index/graph generation covers this snapshot. */
#endif
