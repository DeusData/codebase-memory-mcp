#include "pipeline/parse_batch.h"

#include "foundation/mem_core.h"
#include "foundation/slab_alloc.h"
#include "lang_specs.h"
#include "result_spill.h"
#include "pipeline/shared_resolver.h"
#include "pipeline/worker_pool.h"

#include <stdatomic.h>
#include <string.h>

/* Files in memory at once: enough that no worker waits on another's long file. */
enum { PARSE_BATCH_FILES_PER_WORKER = 8 };

typedef struct {
    int file;
    char *owned;
    cbm_parse_bound_t bound;
} parse_entry_t;

struct cbm_parse_batch {
    const cbm_pipeline_ctx_t *ctx;
    const cbm_file_info_t *files;
    CBMFileResult **cache;
    cbm_parse_batch_mode_fn mode;
    int file_count, workers, next_file;
    parse_entry_t *entries;
    int capacity, count, cursor;
    _Atomic int next_job;
};

static bool parse_file_present(const cbm_parse_batch_t *batch, int f) {
    return batch->cache[f] || (batch->ctx->spill && cbm_result_spill_has(batch->ctx->spill, f));
}

static void parse_job(const cbm_parse_batch_t *batch, parse_entry_t *entry, TSParser *parser) {
    const cbm_file_info_t *file = &batch->files[entry->file];
    const CBMFileResult *result = batch->cache[entry->file];
    size_t length = result && result->source_len > 0 ? (size_t)result->source_len : 0;
    const char *source = result ? result->source : NULL;
    if (!source)
        source = entry->owned = cbm_shared_resolver_load_source(file->path, &length);
    const TSLanguage *language = cbm_ts_language(file->language);
    if (!source || length > UINT32_MAX || !language || !ts_parser_set_language(parser, language))
        return;
    cbm_parse_mode_t mode = batch->mode(file->language);
    TSParseOptions options = {0};
    TSTree *tree = mode == CBM_PARSE_C
                       ? cbm_parse_c_source(parser, source, (uint32_t)length, options)
                   : mode == CBM_PARSE_TERMINATED
                       ? cbm_parse_source(parser, source, (uint32_t)length, options)
                       : ts_parser_parse_string(parser, NULL, source, (uint32_t)length);
    entry->bound = (cbm_parse_bound_t){.mode = mode,
                                       .language = language,
                                       .source = source,
                                       .length = (uint32_t)length,
                                       .tree = tree};
}

static void parse_worker(int worker, void *context) {
    (void)worker;
    cbm_parse_batch_t *batch = context;
    TSParser *parser = ts_parser_new();
    if (parser) {
        for (;;) {
            int job = atomic_fetch_add_explicit(&batch->next_job, 1, memory_order_relaxed);
            if (job >= batch->count)
                break;
            parse_job(batch, &batch->entries[job], parser);
        }
        ts_parser_delete(parser);
    }
    /* A helper thread ends here; the trees it made are freed by the pass's thread. */
    cbm_slab_destroy_thread();
}

cbm_parse_batch_t *cbm_parse_batch_new(const cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                       int file_count, CBMFileResult **cache,
                                       cbm_parse_batch_mode_fn mode) {
    if (!ctx || ctx->prepare_workers <= 1 || !files || !cache || !mode)
        return NULL;
    bool relevant = false;
    for (int f = 0; !relevant && f < file_count; f++)
        relevant = mode(files[f].language) != CBM_PARSE_NONE;
    if (!relevant)
        return NULL;
    cbm_parse_batch_t *batch = cbm_calloc(CBM_MEM_CLASS_EXTRACT, sizeof(*batch));
    if (!batch)
        return NULL;
    batch->capacity = ctx->prepare_workers * PARSE_BATCH_FILES_PER_WORKER;
    batch->entries =
        cbm_calloc(CBM_MEM_CLASS_EXTRACT, (size_t)batch->capacity * sizeof(*batch->entries));
    if (!batch->entries) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, batch);
        return NULL;
    }
    batch->ctx = ctx;
    batch->files = files;
    batch->cache = cache;
    batch->mode = mode;
    batch->file_count = file_count;
    batch->workers = ctx->prepare_workers;
    return batch;
}

static void entry_release(parse_entry_t *entry) {
    if (entry->bound.tree)
        ts_tree_delete(entry->bound.tree);
    cbm_free(CBM_MEM_CLASS_EXTRACT, entry->owned);
    *entry = (parse_entry_t){0};
}

/* The next stretch of wanted files, from file on. */
static void parse_batch_fill(cbm_parse_batch_t *batch, int file) {
    batch->count = 0;
    batch->cursor = 0;
    int f = file;
    for (; f < batch->file_count && batch->count < batch->capacity; f++) {
        if (batch->mode(batch->files[f].language) != CBM_PARSE_NONE && parse_file_present(batch, f))
            batch->entries[batch->count++] = (parse_entry_t){.file = f};
    }
    batch->next_file = f;
    if (!batch->count)
        return;
    atomic_init(&batch->next_job, 0);
    cbm_parallel_for_opts_t options = {.max_workers = batch->workers, .force_pthreads = false};
    cbm_parallel_for(batch->workers, parse_worker, batch, options);
}

void cbm_parse_batch_at(cbm_parse_batch_t *batch, int file) {
    if (!batch)
        return;
    cbm_parse_bind(NULL);
    while (batch->cursor < batch->count && batch->entries[batch->cursor].file < file)
        entry_release(&batch->entries[batch->cursor++]);
    if (batch->cursor == batch->count && file >= batch->next_file)
        parse_batch_fill(batch, file);
    if (batch->cursor < batch->count && batch->entries[batch->cursor].file == file)
        cbm_parse_bind(&batch->entries[batch->cursor].bound);
}

void cbm_parse_batch_free(cbm_parse_batch_t *batch) {
    if (!batch)
        return;
    cbm_parse_bind(NULL);
    for (; batch->cursor < batch->count; batch->cursor++)
        entry_release(&batch->entries[batch->cursor]);
    cbm_free(CBM_MEM_CLASS_EXTRACT, batch->entries);
    cbm_free(CBM_MEM_CLASS_EXTRACT, batch);
}
