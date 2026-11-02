#include "pipeline/binding_cache.h"

#include "foundation/compat.h"
#include "foundation/constants.h"
#include "foundation/mem.h"
#include "foundation/mem_core.h"
#include "foundation/slab_alloc.h"
#include "pipeline/import_resolve.h"
#include "pipeline/shared_resolver.h"
#include "pipeline/worker_pool.h"
#include "resolve/stats.h"

#include <stdatomic.h>

typedef struct {
    CBMArena arena;
    cbm_resolve_binding_index_t *index;
} binding_entry_t;

struct cbm_binding_cache {
    const cbm_file_info_t *files;
    CBMFileResult **results;
    cbm_resolve_stats_t *stats;
    int file_count;
    binding_entry_t **entries;
    _Atomic int next;
    _Atomic size_t retained;
    size_t limit;
};

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Gets answered from the cache, in this process. */
_Atomic size_t cbm_binding_cache_test_hits;
#endif

/* What the indexes may hold together. Past it a file simply has no entry. */
enum { BINDING_CACHE_BUDGET_SHARE = 8 };

static bool binding_language(CBMLanguage language) {
    return language == CBM_LANG_PYTHON || language == CBM_LANG_JAVASCRIPT ||
           language == CBM_LANG_TYPESCRIPT || language == CBM_LANG_TSX ||
           language == CBM_LANG_ARKTS || language == CBM_LANG_DART || language == CBM_LANG_RUST;
}

static void cache_file(cbm_binding_cache_t *cache, int f) {
    const cbm_file_info_t *file = &cache->files[f];
    const CBMFileResult *result = cache->results[f];
    if (!result || !binding_language(file->language))
        return;
    if (atomic_load_explicit(&cache->retained, memory_order_relaxed) >= cache->limit)
        return;
    size_t length = result->source_len > 0 ? (size_t)result->source_len : 0;
    char *owned = result->source ? NULL : cbm_shared_resolver_load_source(file->path, &length);
    const char *source = result->source ? result->source : owned;
    /* A file that changed on disk or cannot be read stays with the passes, which each
     * decide what that means for them. */
    if (cbm_result_source_matches(result, source, length, CBM_SOURCE_ORIGIN_RAW)) {
        binding_entry_t *entry = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, sizeof(*entry));
        if (entry) {
            cbm_arena_init_lazy(&entry->arena, CBM_SZ_4K);
            bool declared = file->language == CBM_LANG_JAVASCRIPT &&
                            cbm_pipeline_node_declared_module(file->rel_path);
            entry->index = cbm_resolve_binding_build_declared_module(
                &entry->arena, result, file->language, source, length, declared);
            if (entry->index) {
                cbm_resolve_binding_release_source(entry->index);
                atomic_fetch_add_explicit(&cache->retained, cbm_arena_total(&entry->arena),
                                          memory_order_relaxed);
                cache->entries[f] = entry;
            } else {
                cbm_arena_destroy(&entry->arena);
                cbm_free(CBM_MEM_CLASS_SEMANTIC, entry);
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, owned);
}

static void cache_worker(int worker, void *context) {
    (void)worker;
    cbm_binding_cache_t *cache = context;
    cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(cache->stats);
    for (;;) {
        int f = atomic_fetch_add_explicit(&cache->next, 1, memory_order_relaxed);
        if (f >= cache->file_count)
            break;
        cache_file(cache, f);
    }
    cbm_resolve_stats_bind(previous);
    /* A worker's slab pages must not keep an owner pointer into its dead TLS. */
    cbm_slab_destroy_thread();
}

cbm_binding_cache_t *cbm_binding_cache_build(const cbm_pipeline_ctx_t *ctx,
                                             const cbm_file_info_t *files, int file_count,
                                             CBMFileResult **cache, int worker_count) {
    /* Parked results come back at a new address, and the store exists because memory is
     * already short. */
    if (!ctx || ctx->spill || worker_count <= 1 || !files || !cache || file_count <= 0)
        return NULL;
    bool relevant = false;
    for (int f = 0; !relevant && f < file_count; f++)
        relevant = cache[f] && binding_language(files[f].language);
    if (!relevant)
        return NULL;
    cbm_binding_cache_t *built = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, sizeof(*built));
    binding_entry_t **entries =
        cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)file_count * sizeof(*entries));
    if (!built || !entries) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, built);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, entries);
        return NULL;
    }
    size_t budget = cbm_mem_budget();
    built->files = files;
    built->results = cache;
    built->stats = ctx->resolver_stats;
    built->file_count = file_count;
    built->entries = entries;
    built->limit = budget ? budget / BINDING_CACHE_BUDGET_SHARE : SIZE_MAX;
    cbm_parallel_for_opts_t options = {.max_workers = worker_count, .force_pthreads = false};
    cbm_parallel_for(worker_count, cache_worker, built, options);
    return built;
}

void cbm_binding_cache_free(cbm_binding_cache_t *cache) {
    if (!cache)
        return;
    for (int f = 0; f < cache->file_count; f++) {
        if (cache->entries[f]) {
            cbm_arena_destroy(&cache->entries[f]->arena);
            cbm_free(CBM_MEM_CLASS_SEMANTIC, cache->entries[f]);
        }
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, cache->entries);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, cache);
}

void cbm_binding_cache_release_after_resolver(cbm_binding_cache_t *cache) {
    if (!cache)
        return;
    for (int f = 0; f < cache->file_count; f++) {
        CBMLanguage language = cache->files[f].language;
        if (cache->entries[f] && language != CBM_LANG_TYPESCRIPT && language != CBM_LANG_TSX) {
            cbm_arena_destroy(&cache->entries[f]->arena);
            cbm_free(CBM_MEM_CLASS_SEMANTIC, cache->entries[f]);
            cache->entries[f] = NULL;
        }
    }
}

cbm_resolve_binding_index_t *cbm_binding_cache_get(const cbm_binding_cache_t *cache,
                                                   const cbm_file_info_t *file,
                                                   const CBMFileResult *result, bool declared) {
    if (!cache || file < cache->files || file >= cache->files + cache->file_count)
        return NULL;
    size_t f = (size_t)(file - cache->files);
    const binding_entry_t *entry = cache->results[f] == result ? cache->entries[f] : NULL;
    cbm_resolve_binding_index_t *index = entry ? entry->index : NULL;
    /* Only a script that nothing but its manifest makes a module differs by this flag. */
    if (index && !declared && cbm_resolve_binding_needs_declared_module(index))
        index = NULL;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    if (index)
        atomic_fetch_add_explicit(&cbm_binding_cache_test_hits, 1, memory_order_relaxed);
#endif
    return index;
}
