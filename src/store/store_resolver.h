#ifndef CBM_STORE_RESOLVER_H
#define CBM_STORE_RESOLVER_H

#include "store.h"
#include "resolve/record.h"

/* Generation metadata: replace only in the private staging database before
 * its atomic publication. No schema table means unavailable, never zero. */
int cbm_store_resolver_replace(cbm_store_t *store, const char *project,
                               const cbm_resolve_record_t *rows, size_t count);
int cbm_store_resolver_summary(cbm_store_t *store, const char *project,
                               cbm_resolve_counts_t *counts, bool *present);
/* Ordered, bounded diagnostics for abstained occurrences. Borrowed row strings
 * remain valid only during the visitor call. A callback failure aborts the read.
 * Use summary's generation availability before requesting rows. */
typedef bool (*cbm_store_resolver_visit_fn)(const cbm_resolve_record_t *row, void *context);
int cbm_store_resolver_visit_misses(cbm_store_t *store, const char *project, size_t offset,
                                    size_t limit, cbm_store_resolver_visit_fn visit, void *context,
                                    size_t *returned);

/* Current closure repair does not index negative binding dependencies. A
 * generation with resolver obligations must rebuild after a manifest change. */
int cbm_store_resolver_requires_full(cbm_store_t *store, const char *project, bool *required);

#endif
