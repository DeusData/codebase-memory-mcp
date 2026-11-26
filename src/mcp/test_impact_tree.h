/* Internal pinned-tree materializer. No trust or admission claim. */
#ifndef CBM_TEST_IMPACT_TREE_H
#define CBM_TEST_IMPACT_TREE_H

#include "mcp/test_impact_git.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct cbm_pinned_tree cbm_pinned_tree_t;

typedef enum {
    CBM_PINNED_TREE_OK = 0,
    CBM_PINNED_TREE_INVALID,
    CBM_PINNED_TREE_UNSUPPORTED,
    CBM_PINNED_TREE_COLLISION,
    CBM_PINNED_TREE_LIMIT,
    CBM_PINNED_TREE_OOM,
    CBM_PINNED_TREE_IO,
    CBM_PINNED_TREE_GIT,
    CBM_PINNED_TREE_CANCELLED,
    CBM_PINNED_TREE_DEADLINE,
    CBM_PINNED_TREE_CHANGED,
    CBM_PINNED_TREE_CLEANUP_REQUIRED
} cbm_pinned_tree_status_t;

typedef struct {
    cbm_pinned_tree_status_t status;
    /* Primary construction/verification cause when cleanup also failed;
     * otherwise equal to status. Never a borrowed diagnostic or path. */
    cbm_pinned_tree_status_t cause;
    cbm_git_facts_error_t git; /* initialized; meaningful for GIT cause */
    char diagnostic[512];
    char cleanup_path[4096]; /* owned child root only; empty if none remains */
} cbm_pinned_tree_error_t;

typedef struct {
    size_t max_files;                       /* positive; complete non-directory inventory */
    size_t max_directories;                 /* positive; includes the owned root */
    size_t max_total_content_bytes;         /* positive; each distinct path counts in full */
    size_t max_relative_path_bytes;         /* positive; 1..4095, excludes terminator */
    size_t max_arena_bytes;                 /* positive; logical materializer allocation requests */
    cbm_git_blob_batch_limits_t blob_batch; /* all positive; separate facts-owned budget */
} cbm_pinned_tree_limits_t;

typedef struct {
    uint64_t deadline_ms; /* positive absolute cbm_now_ms deadline for this operation */
    cbm_git_facts_cancel_fn cancelled; /* optional, prompt, pure predicate */
    void *cancel_context;              /* borrowed only until synchronous operation returns */
} cbm_pinned_tree_control_t;

typedef struct {
    cbm_git_facts_t *facts;      /* borrowed/serialized only for create; not retained */
    cbm_git_revision_t revision; /* HEAD or actual MERGE_BASE only */
    /* Existing absolute private physical directory. POSIX rejects extended
     * access ACLs and every nonempty default ACL; only an absent or minimal
     * mode-equivalent Linux
     * access ACL is allowed. Required inspection failure is UNSUPPORTED.
     * Caller-parent ACLs are never modified. See normative native policy. */
    const char *private_parent;
    cbm_pinned_tree_limits_t limits;
    cbm_pinned_tree_control_t control;
} cbm_pinned_tree_options_t;

typedef struct {
    const unsigned char *path; /* owned exact Git bytes plus convenience NUL */
    size_t path_length;
    uint32_t git_mode; /* 0100644 or 0100755, independent of native permissions */
    char oid[65];
    uint64_t content_length;
    unsigned char content_sha256[32];
} cbm_pinned_tree_file_t;

typedef struct {
    const char *root; /* owned native absolute root, <=4095 bytes excluding NUL */
    const cbm_git_facts_identity_t *identity; /* deep copy; not authenticated */
    cbm_git_revision_t revision;
    const char *commit;                  /* full selected pinned OID */
    const cbm_pinned_tree_file_t *files; /* unsigned raw-path order; NULL when empty */
    size_t file_count;
    size_t directory_count; /* root plus unique proper path prefixes */
    uint64_t total_content_bytes;
    /* Symlink (0120000) and submodule (0160000) entries of the revision. They
     * are not materialized and not in `files`: discovery never indexes a
     * symlink, and a submodule's content is not in this repository. Any other
     * non-regular entry still fails the pin. */
    size_t skipped_link_count;
    unsigned char manifest_sha256[32]; /* exact framing in normative text */
} cbm_pinned_tree_view_t;

/* Clear *out first and initialize error when supplied. Only OK yields a READY
 * owner/view. Ordinary failure yields NULL after checked cleanup. If cleanup
 * fails, return CLEANUP_REQUIRED and a disposal-only owner through *out; all
 * views are unavailable, but close() may be retried. Thus callers must dispose
 * any non-NULL *out even after failure. No materialized file is executed.
 * Establish/check native owner/mode/ACL policy before any payload bytes.
 * Source buffers/option strings may be discarded and facts freed on return. */
cbm_pinned_tree_status_t cbm_pinned_tree_create(const cbm_pinned_tree_options_t *options,
                                                cbm_pinned_tree_t **out,
                                                cbm_pinned_tree_error_t *error);

/* NULL unless READY. Borrowed immutable view until close or the next valid
 * verify/read_prefix call. No getter can turn a failed/disposal-only owner into a usable tree. */
const cbm_pinned_tree_view_t *cbm_pinned_tree_view(const cbm_pinned_tree_t *tree);

/* Recheck exact complete inventory, bytes, identities and native mode/ACL policy after
 * consumers are quiescent; does not prove quiescence itself. No Git/facts use.
 * Invalid arguments leave READY unchanged. Once valid verification starts,
 * any non-OK result permanently invalidates views; only close remains valid.
 * Uses preallocated bounded scratch, retains no callback/context. */
cbm_pinned_tree_status_t cbm_pinned_tree_verify(cbm_pinned_tree_t *tree,
                                                const cbm_pinned_tree_control_t *control,
                                                cbm_pinned_tree_error_t *error);

/* Verified selected-file prefix read.
 * Serialized READY-only operation. Verify the full indexed file and native
 * policy of its parent/ancestor chain before publishing at most capacity bytes.
 * No NUL conversion; copied is required and cleared on entry, error optional.
 * Invalid arguments and the preflight content cap leave READY unchanged.
 * A valid started failure leaves the owner disposal-only; only close is valid.
 * Writable outputs, control/context and borrowed owner storage must not alias.
 * All operands remain live until return. Violating nonaliasing is invalid use; no overlap detector
 * is provided.
 */
cbm_pinned_tree_status_t cbm_pinned_tree_read_prefix(cbm_pinned_tree_t *tree, size_t file_index,
                                                     uint64_t max_content_bytes,
                                                     unsigned char *prefix, size_t capacity,
                                                     size_t *copied,
                                                     const cbm_pinned_tree_control_t *control,
                                                     cbm_pinned_tree_error_t *error);

/* Test-build-only boundary simulations. No global state/raw handles.
 * Faults affect only read_prefix and are consumed by its next valid started call.
 */
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
typedef enum {
    CBM_PINNED_TREE_READ_FAULT_NONE = 0,
    CBM_PINNED_TREE_READ_FAULT_READ,
    CBM_PINNED_TREE_READ_FAULT_EOF,
    CBM_PINNED_TREE_READ_FAULT_CLOSE
} cbm_pinned_tree_read_fault_t;

/* READY-only: false/unchanged for NULL, disposal-only or invalid enum. NONE clears
 * a pending fault. No overlap with another owner operation. No IO/allocation. */
bool cbm_pinned_tree_test_set_read_fault(cbm_pinned_tree_t *tree,
                                         cbm_pinned_tree_read_fault_t fault);
#endif

/* Explicit, checked disposal. Requires all consumers/children quiescent and no
 * overlapping owner operation. Ignores cancellation/deadlines; does not run
 * processes. Removes only this owner's tracked resources, never unexpected
 * descendants or a replacement object at an owned path. On OK frees owner and
 * sets *tree=NULL. On CLEANUP_REQUIRED retains a disposal-only owner for retry,
 * with bounded cleanup_path. NULL tree-pointer is INVALID; *tree==NULL is OK.
 * No unreported free/destructor exists and no process-global cleanup is used. */
cbm_pinned_tree_status_t cbm_pinned_tree_close(cbm_pinned_tree_t **tree,
                                               cbm_pinned_tree_error_t *error);

#endif
