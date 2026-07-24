/*
 * activation_transaction.h -- Transactional binary activation primitives.
 *
 * This is an internal CLI module.  It deliberately knows nothing about daemon
 * coordination or editor configuration: callers must acquire the maintenance
 * barrier before commit and retain it until finalize/rollback completes.
 */
#ifndef CBM_ACTIVATION_TRANSACTION_H
#define CBM_ACTIVATION_TRANSACTION_H

#include <stdbool.h>
#include <stddef.h>

typedef struct cbm_activation_transaction cbm_activation_transaction_t;

typedef enum {
    CBM_ACTIVATION_TRANSACTION_OK = 0,
    /* Windows could not unlink an inactive backup (normally because the old
     * executable image is still mapped) but safely registered it for deletion
     * at reboot.  The committed activation remains valid. */
    CBM_ACTIVATION_TRANSACTION_DEFERRED = 1,
    CBM_ACTIVATION_TRANSACTION_INVALID_ARGUMENT = -1,
    CBM_ACTIVATION_TRANSACTION_NO_MEMORY = -2,
    CBM_ACTIVATION_TRANSACTION_IO = -3,
    CBM_ACTIVATION_TRANSACTION_INVALID_STATE = -4,
    /* The post-commit validator rejected the candidate and rollback succeeded. */
    CBM_ACTIVATION_TRANSACTION_VALIDATION_FAILED = -5,
    /* The target changed, but restoring the retained backup also failed. */
    CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED = -6,
} cbm_activation_transaction_status_t;

typedef bool (*cbm_activation_transaction_validator_fn)(const char *target_path, void *context);

/* Test-only seam: invoked after an absent target has been revalidated and
 * immediately before its staged candidate is published.  Production callers
 * leave this unset. */
typedef void (*cbm_activation_transaction_before_absent_publish_for_test_fn)(
    const char *target_path, void *context);
void cbm_activation_transaction_set_before_absent_publish_for_test(
    cbm_activation_transaction_before_absent_publish_for_test_fn hook, void *context);

/* Stage a candidate beside target_path (therefore on the same filesystem).
 * The staged file is private to the current account and executable. */
cbm_activation_transaction_status_t cbm_activation_transaction_stage_bytes(
    const char *target_path, const void *candidate, size_t candidate_size,
    cbm_activation_transaction_t **transaction_out);

/* Copy candidate_path into a private executable stage beside target_path. */
cbm_activation_transaction_status_t cbm_activation_transaction_stage_file(
    const char *target_path, const char *candidate_path,
    cbm_activation_transaction_t **transaction_out);

/* Prepare an atomic removal.  A missing target is a valid no-op transaction. */
cbm_activation_transaction_status_t cbm_activation_transaction_stage_removal(
    const char *target_path, cbm_activation_transaction_t **transaction_out);

/* Atomically publish the candidate (or remove the target), retaining any old
 * target at backup_path.  If validator rejects the post-commit state, this
 * function rolls back before returning VALIDATION_FAILED. */
cbm_activation_transaction_status_t cbm_activation_transaction_commit(
    cbm_activation_transaction_t *transaction, cbm_activation_transaction_validator_fn validator,
    void *validator_context);

/* Restore the retained target after a successful commit. */
cbm_activation_transaction_status_t cbm_activation_transaction_rollback(
    cbm_activation_transaction_t *transaction);

/* Accept the committed state and delete the retained backup.  On Windows,
 * DEFERRED means deletion was safely registered for reboot; deferred_path
 * remains available for logging until close(). */
cbm_activation_transaction_status_t cbm_activation_transaction_finalize(
    cbm_activation_transaction_t *transaction);

/* Close an object.  An uncommitted object is cleanly aborted; a committed but
 * unfinalized object is rolled back.  On cleanup failure, ownership stays with
 * the caller so paths and rollback can be retried. */
cbm_activation_transaction_status_t cbm_activation_transaction_close(
    cbm_activation_transaction_t **transaction_io);

const char *cbm_activation_transaction_target_path(const cbm_activation_transaction_t *transaction);
const char *cbm_activation_transaction_staged_path(const cbm_activation_transaction_t *transaction);
const char *cbm_activation_transaction_backup_path(const cbm_activation_transaction_t *transaction);
const char *cbm_activation_transaction_deferred_path(
    const cbm_activation_transaction_t *transaction);

const char *cbm_activation_transaction_status_message(cbm_activation_transaction_status_t status);

/* Which security predicate refused the most recent transaction, as
 * "predicate (os N)", or "" when nothing refused since the last prepare.
 * The predicates refuse without a usable OS last-error, so this is the only
 * way a caller can say WHY staging failed. Reset by every prepare/stage
 * entry; single-threaded like the rest of the transaction API. */
const char *cbm_activation_transaction_refusal_note(void);

#endif /* CBM_ACTIVATION_TRANSACTION_H */
