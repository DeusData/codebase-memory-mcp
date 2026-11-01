/*
 * test_impact_classify.h — the classified ledger of a pinned HEAD inventory.
 *
 * The inventory filter leaves every file it keeps as "needs language". This
 * pass gives each of them the language discovery gives the same file: the
 * shared rule of cbm_language_classify, applied to the base name and, where
 * the name is not enough, to the first bytes of the verified git blob. What
 * the ledger holds is what a frozen build indexes and what the reconciliation
 * compares against.
 *
 * A blob that cannot be read is an error of the whole pass, never a default
 * language: a ledger that quietly classified a file as something else would
 * index a different graph than the one the selection claims to use.
 */
#ifndef CBM_TEST_IMPACT_CLASSIFY_H
#define CBM_TEST_IMPACT_CLASSIFY_H

#include "discover/test_conventions.h"
#include "discover/userconfig.h"
#include "mcp/test_impact.h"
#include "mcp/test_impact_inventory.h"
#include "mcp/test_impact_tree.h"

typedef struct {
    size_t file_index;    /* index in the pinned tree and the inventory view */
    CBMLanguage language; /* CBM_LANG_COUNT: no supported language, not indexed */
} cbm_test_impact_classified_t;

typedef struct {
    uint64_t verified_file_reads;    /* blobs read by this pass */
    uint64_t verified_content_bytes; /* their full lengths: each read verifies the whole blob */
    size_t arena_requested_bytes;    /* the ledger's own memory */
} cbm_test_impact_classify_usage_t;

typedef struct cbm_test_impact_ledger cbm_test_impact_ledger_t;

/* Classify every "needs language" survivor of `inventory`, reading from
 * `tree`, the READY pinned tree the inventory was prepared from, under the
 * snapshot's language config `config` (NULL: no user overrides). Reads,
 * content bytes and ledger memory are charged to what the inventory's limits
 * leave after the filter; a probe longer than max_probe_prefix_bytes is a
 * LIMIT. Clears *out and initializes *error first. On failure *out stays
 * NULL; a started failed read may leave the tree disposal-only, exactly as
 * for the filter's own reads. */
cbm_inventory_status_t cbm_test_impact_classify(const cbm_test_impact_inventory_t *inventory,
                                                cbm_pinned_tree_t *tree,
                                                const cbm_userconfig_t *config,
                                                const cbm_inventory_control_t *control,
                                                cbm_test_impact_ledger_t **out,
                                                cbm_inventory_error_t *error);

/* Rows in file-index order, one per survivor. Borrowed until the ledger is
 * freed. */
const cbm_test_impact_classified_t *cbm_test_impact_ledger_rows(
    const cbm_test_impact_ledger_t *ledger, size_t *count);

/* The manifest the inventory was bound to, so a consumer can check that the
 * ledger and a tree describe the same snapshot. */
const unsigned char *cbm_test_impact_ledger_manifest(const cbm_test_impact_ledger_t *ledger);

bool cbm_test_impact_ledger_usage(const cbm_test_impact_ledger_t *ledger,
                                  cbm_test_impact_classify_usage_t *out);

void cbm_test_impact_ledger_free(cbm_test_impact_ledger_t *ledger);

/* The runnable test registry of the same snapshot: the finished test model
 * (mcp/test_impact.h) built under `declarations` (the snapshot's config, as
 * the frozen build was given it) from the full verified blobs of every ledger
 * file in a language those declarations cover (C for the c-cbm preset). More
 * files rather than fewer: no path filter narrows what is scanned. Blob reads
 * are charged to what the inventory's limits leave after the filter and the
 * ledger; a read that fails or does not fit is an error, never a smaller
 * registry. The model may still report mapping uncertainty: callers must
 * require cbm_test_model_complete() before narrowing anything with it. */
cbm_inventory_status_t cbm_test_impact_registry(
    const cbm_test_impact_ledger_t *ledger, const cbm_test_impact_inventory_t *inventory,
    cbm_pinned_tree_t *tree, const cbm_test_declarations_t *declarations,
    const cbm_inventory_control_t *control, cbm_test_model_t **out, cbm_inventory_error_t *error);

#endif /* CBM_TEST_IMPACT_CLASSIFY_H */
