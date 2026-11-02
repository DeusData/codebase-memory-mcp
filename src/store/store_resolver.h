#ifndef CBM_STORE_RESOLVER_H
#define CBM_STORE_RESOLVER_H

#include "store.h"
#include "resolve/record.h"

/* Generation metadata: replace only in the private staging database before
 * its atomic publication. No schema table means unavailable, never zero. */
int cbm_store_resolver_replace(cbm_store_t *store, const char *project,
                               const cbm_resolve_record_t *rows, size_t count);
int cbm_store_resolver_replace_generation(cbm_store_t *store, const char *project,
                                          const cbm_resolve_record_t *rows, size_t count,
                                          const cbm_resolve_file_t *files, size_t file_count,
                                          const cbm_resolve_summary_t *summaries,
                                          size_t summary_count);
/* The same write for a private staging store that a failure discards. Rows commit
 * in batches, so the WAL stays small instead of holding a second copy of them. A
 * partial generation can never pass for a whole one: the receipt is written last. */
/* The rest of a generation whose rows were written into the file as it was built. */
typedef struct cbm_resolver_stage cbm_resolver_stage_t;
int cbm_store_resolver_publish_stage(cbm_store_t *store, const char *project,
                                     const cbm_resolver_stage_t *stage);
/* workers: threads for the digests of a big write; 1 or less runs on this thread. */
int cbm_store_resolver_publish_generation(cbm_store_t *store, const char *project,
                                          const cbm_resolve_record_t *rows, size_t count,
                                          const cbm_resolve_file_t *files, size_t file_count,
                                          const cbm_resolve_summary_t *summaries,
                                          size_t summary_count, int workers);
/* Fixed-size schema/receipt check, independent of occurrence count. Missing or
 * subsequently modified generation metadata is unavailable and needs rebuild. */
int cbm_store_resolver_current(cbm_store_t *store, const char *project, bool *present);
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
