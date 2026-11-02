#ifndef CBM_PIPELINE_BINDING_CACHE_H
#define CBM_PIPELINE_BINDING_CACHE_H

#include "pipeline_internal.h"
#include "resolve/binding.h"

/* A file's binding index is a parse and a whole-tree walk, and four single-threaded
 * preparation passes each needed it: ECMAScript surfaces, the definition census, the
 * shared resolver and the semantic bindings. On a TypeScript repository that was 40 of
 * its 120 seconds. Workers build every index once before those passes and each pass
 * reads the same one. An index is a pure function of the file's extraction result and
 * bytes, so a pass cannot tell which index it got: a file with no entry, which happens
 * when memory is short or results are parked on disk, is built by the pass as before. */
typedef struct cbm_binding_cache cbm_binding_cache_t;

cbm_binding_cache_t *cbm_binding_cache_build(const cbm_pipeline_ctx_t *ctx,
                                             const cbm_file_info_t *files, int file_count,
                                             CBMFileResult **cache, int worker_count);
void cbm_binding_cache_free(cbm_binding_cache_t *cache);

/* The index the caller would have built for this file and result, or NULL when the caller
 * must build it. declared says whether the caller's build passes the manifest's module
 * declaration. Valid until the cache is freed; a reader is one thread at a time. */
cbm_resolve_binding_index_t *cbm_binding_cache_get(const cbm_binding_cache_t *cache,
                                                   const cbm_file_info_t *file,
                                                   const CBMFileResult *result, bool declared);
/* The shared resolver is the last reader of all but TypeScript's indexes, and the memory
 * it would keep them through is what the workers run in next. */
void cbm_binding_cache_release_after_resolver(cbm_binding_cache_t *cache);

#endif
