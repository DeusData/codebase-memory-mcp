/* Internal pinned HEAD tree to pre-language inventory bridge.
 * Source: src/mcp/test_impact_inventory.h.
 * Normative bridge behavior: accompanying contract.txt.
 */
#ifndef CBM_TEST_IMPACT_INVENTORY_H
#define CBM_TEST_IMPACT_INVENTORY_H

#include "discover/inventory_filter.h"
#include "mcp/test_impact_tree.h"

typedef struct cbm_test_impact_inventory cbm_test_impact_inventory_t;

typedef struct {
    size_t attachment_arena_requested_bytes; /* A, including freed temporary inputs. */
    cbm_inventory_filter_usage_t filter;     /* Existing D/C/I and read/work ledgers. */
    size_t arena_requested_bytes;            /* A+D+C, actual logical requests. */
    size_t arena_budget_used_bytes;          /* A+D+I, unavailable quota. */
} cbm_test_impact_inventory_usage_t;

/* Synchronous HEAD-only bridge from the complete READY native inventory.
 * Requires tree/limits/control/out. Clears out and initializes optional error.
 * Borrows the native owner; never verifies/closes/frees it independently.
 * No provider argument: reads bind exact indices to this tree's read_prefix.
 * Success owns one existing filter and original limits/deadline/accounting.
 * Retains no native owner/storage, callback, control object or context.
 * Failure publishes NULL; native disposal remains caller-owned, including
 * after a read makes the native owner disposal-only. No automatic retry.
 */
cbm_inventory_status_t cbm_test_impact_inventory_prepare(cbm_pinned_tree_t *tree,
                                                         const cbm_inventory_limits_t *limits,
                                                         const cbm_inventory_control_t *control,
                                                         cbm_test_impact_inventory_t **out,
                                                         cbm_inventory_error_t *error);

/* Existing filter view, borrowed until wrapper free; NULL for NULL owner.
 * NEEDS_LANGUAGE survivors remain unfinished. No IO/poll/allocation.
 */
const cbm_inventory_filter_view_t *cbm_test_impact_inventory_view(
    const cbm_test_impact_inventory_t *owner);

/* Clear out first; false for NULL owner/output. Copies diagnostics only.
 * limits returns the caller's original limits, including original total T.
 * The wrapped filter privately received T-A with unchanged I exactly once.
 */
bool cbm_test_impact_inventory_usage(const cbm_test_impact_inventory_t *owner,
                                     cbm_test_impact_inventory_usage_t *out);
bool cbm_test_impact_inventory_limits(const cbm_test_impact_inventory_t *owner,
                                      cbm_inventory_limits_t *out);

/* NULL-safe. Frees the wrapped filter, then wrapper arena. No native IO,
 * callbacks or tree disposal. Cannot overlap getters or another operation.
 */
void cbm_test_impact_inventory_free(cbm_test_impact_inventory_t *owner);

#endif
