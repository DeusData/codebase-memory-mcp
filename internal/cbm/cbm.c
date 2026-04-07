#include "cbm.h"
#include "arena.h" // CBMArena, cbm_arena_init/alloc/strdup/destroy
#include "helpers.h"
#include "lang_specs.h"
#include "extract_unified.h"
#include "lsp/go_lsp.h"
#include "lsp/c_lsp.h"
#include "preprocessor.h"
#include "foundation/compat.h"
#include "tree_sitter/api.h" // TSParser, TSNode, TSTree, TSInput, TSLanguage, TSPoint, TSParseOptions, TSParseState
#include "foundation/constants.h"
#include <stdint.h> // uint32_t, uint64_t, int64_t
#include <stdlib.h>
#include <string.h>
#include <time.h> // struct timespec, CLOCK_MONOTONIC

// Atomic counters for profiling parse vs extraction time (nanoseconds).
// Accessed from multiple threads; using _Atomic for safe accumulation.
#include <stdatomic.h>
static _Atomic uint64_t total_parse_ns = 0;
static _Atomic uint64_t total_extract_ns = 0;
static _Atomic uint64_t total_lsp_ns = 0;
static _Atomic uint64_t total_preprocess_ns = 0;
static _Atomic uint64_t total_files_preprocessed = 0;
static _Atomic uint64_t total_files = 0;

#define NSEC_PER_SEC 1000000000ULL
#define USEC_TO_NSEC 1000ULL
/* Use compat.h's cbm_clock_gettime which accepts CLOCK_MONOTONIC (value
 * varies by platform: 1 on Linux/Windows, 6 on macOS). We pass the
 * platform value via the compat.h fallback. */
#ifdef __APPLE__
#define CBM_CLOCK_MONO 6
#else
#define CBM_CLOCK_MONO 1
#endif

static uint64_t now_ns(void) {
    struct timespec ts;
    cbm_clock_gettime(CBM_CLOCK_MONO, &ts);
    return ((uint64_t)ts.tv_sec * NSEC_PER_SEC) + (uint64_t)ts.tv_nsec;
}

// cbm_get_profile returns accumulated parse/extract times and file count.
void cbm_get_profile(cbm_profile_out_t out) {
    *out.parse_ns = atomic_load(&total_parse_ns);
    *out.extract_ns = atomic_load(&total_extract_ns);
    *out.files = atomic_load(&total_files);
}

uint64_t cbm_get_lsp_ns(void) {
    return atomic_load(&total_lsp_ns);
}

uint64_t cbm_get_preprocess_ns(void) {
    return atomic_load(&total_preprocess_ns);
}

uint64_t cbm_get_files_preprocessed(void) {
    return atomic_load(&total_files_preprocessed);
}

// cbm_reset_profile zeros the profiling counters.
void cbm_reset_profile(void) {
    atomic_store(&total_parse_ns, 0);
    atomic_store(&total_extract_ns, 0);
    atomic_store(&total_lsp_ns, 0);
    atomic_store(&total_preprocess_ns, 0);
    atomic_store(&total_files_preprocessed, 0);
    atomic_store(&total_files, 0);
}

// --- Growable array push functions ---

#define GROW_ARRAY(arr, arena)                                                                   \
    do {                                                                                         \
        if ((arr)->count >= (arr)->cap) {                                                        \
            int new_cap = (arr)->cap == 0 ? CBM_SZ_32 : (arr)->cap * PAIR_LEN;                   \
            void *new_items = cbm_arena_alloc((arena), (size_t)new_cap * sizeof(*(arr)->items)); \
            if (!new_items)                                                                      \
                return;                                                                          \
            if ((arr)->items && (arr)->count > 0) {                                              \
                memcpy(new_items, (arr)->items, (size_t)(arr)->count * sizeof(*(arr)->items));   \
            }                                                                                    \
            (arr)->items = new_items;                                                            \
            (arr)->cap = new_cap;                                                                \
        }                                                                                        \
    } while (0)

void cbm_defs_push(CBMDefArray *arr, CBMArena *a, CBMDefinition def) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = def;
}

void cbm_calls_push(CBMCallArray *arr, CBMArena *a, CBMCall call) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = call;
}

void cbm_imports_push(CBMImportArray *arr, CBMArena *a, CBMImport imp) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = imp;
}

void cbm_usages_push(CBMUsageArray *arr, CBMArena *a, CBMUsage usage) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = usage;
}

void cbm_throws_push(CBMThrowArray *arr, CBMArena *a, CBMThrow thr) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = thr;
}

void cbm_rw_push(CBMRWArray *arr, CBMArena *a, CBMReadWrite rw) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = rw;
}

void cbm_typerefs_push(CBMTypeRefArray *arr, CBMArena *a, CBMTypeRef tr) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = tr;
}

void cbm_envaccess_push(CBMEnvAccessArray *arr, CBMArena *a, CBMEnvAccess ea) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = ea;
}

void cbm_typeassign_push(CBMTypeAssignArray *arr, CBMArena *a, CBMTypeAssign ta) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = ta;
}

void cbm_stringref_push(CBMStringRefArray *arr, CBMArena *a, CBMStringRef sr) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = sr;
}

void cbm_infrabinding_push(CBMInfraBindingArray *arr, CBMArena *a, CBMInfraBinding ib) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = ib;
}

void cbm_impltrait_push(CBMImplTraitArray *arr, CBMArena *a, CBMImplTrait it) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = it;
}

void cbm_resolvedcall_push(CBMResolvedCallArray *arr, CBMArena *a, CBMResolvedCall rc) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = rc;
}

void cbm_channels_push(CBMChannelArray *arr, CBMArena *a, CBMChannel ch) {
    GROW_ARRAY(arr, a);
    arr->items[arr->count++] = ch;
}

// --- String input reader (for parse_with_options) ---

typedef struct {
    const char *string;
    uint32_t length;
} CBMStringInput;

static const char *cbm_string_read(void *payload, uint32_t byte, TSPoint point,
                                   uint32_t *bytes_read) {
    (void)point;
    CBMStringInput *self = (CBMStringInput *)payload;
    if (byte >= self->length) {
        *bytes_read = 0;
        return "";
    }
    *bytes_read = self->length - byte;
    return self->string + byte;
}

// --- Parse timeout callback ---

static bool cbm_timeout_cb(TSParseState *state) {
    uint64_t deadline = *(uint64_t *)state->payload;
    return now_ns() > deadline;
}

// --- Thread-local parser pool ---
// TSParser is not thread-safe, but can be reused across files on the same thread.
// We keep one parser per thread, and just switch language as needed.
// This avoids ~70K ts_parser_new()/ts_parser_delete() cycles on large repos.

static CBM_TLS TSParser *tl_parser = NULL;
static CBM_TLS CBMLanguage tl_parser_lang = CBM_LANG_COUNT; // invalid sentinel

// Get or create a thread-local parser configured for the given language.
static TSParser *get_thread_parser(const TSLanguage *ts_lang, CBMLanguage lang) {
    if (!tl_parser) {
        tl_parser = ts_parser_new();
        if (!tl_parser) {
            return NULL;
        }
        tl_parser_lang = CBM_LANG_COUNT;
    }
    if (tl_parser_lang != lang) {
        ts_parser_set_language(tl_parser, ts_lang);
        tl_parser_lang = lang;
    }
    return tl_parser;
}

// --- Init/Shutdown ---

static int cbm_initialized = 0;

int cbm_init(void) {
    if (cbm_initialized) {
        return 0;
    }
    enum { CBM_INIT_DONE = 1 };
    cbm_initialized = CBM_INIT_DONE;
    return 0;
}

void cbm_reset_thread_parser(void) {
    // Release parser's internal slab-allocated subtrees (stack, cached token).
    // Must be called BEFORE cbm_slab_reset_thread() to avoid corrupting
    // live slab chunks that the parser still references.
    if (tl_parser) {
        ts_parser_reset(tl_parser);
    }
}

void cbm_destroy_thread_parser(void) {
    // Full cleanup: delete the parser. Call on worker thread exit.
    if (tl_parser) {
        ts_parser_delete(tl_parser);
        tl_parser = NULL;
        tl_parser_lang = CBM_LANG_COUNT;
    }
}

void cbm_shutdown(void) {
    // Clean up thread-local parser for the calling thread.
    // Note: other threads' TLS parsers are freed when those threads exit.
    cbm_destroy_thread_parser();
    cbm_initialized = 0;
}

// --- Main extraction function ---

CBMFileResult *cbm_extract_file(const char *source, int source_len, CBMLanguage language,
                                const char *project, const char *rel_path, int64_t timeout_micros,
                                const char **extra_defines, const char **include_paths) {
    // Allocate result on heap (arena inside for all string data)
    enum { SINGLE = 1 };
    CBMFileResult *result = (CBMFileResult *)calloc(SINGLE, sizeof(CBMFileResult));
    if (!result) {
        return NULL;
    }

    cbm_arena_init(&result->arena);
    CBMArena *a = &result->arena;

    // Get language spec
    const CBMLangSpec *spec = cbm_lang_spec(language);
    if (!spec) {
        result->has_error = true;
        result->error_msg = cbm_arena_strdup(a, "unsupported language");
        return result;
    }

    // Get tree-sitter language
    const TSLanguage *ts_lang = cbm_ts_language(language);
    if (!ts_lang) {
        result->has_error = true;
        result->error_msg = cbm_arena_strdup(a, "no tree-sitter grammar");
        return result;
    }

    // Get thread-local parser (reused across files on same thread)
    TSParser *parser = get_thread_parser(ts_lang, language);
    if (!parser) {
        result->has_error = true;
        result->error_msg = cbm_arena_strdup(a, "parser alloc failed");
        return result;
    }

    // Reset parser state from any previous parse (cancellation flags etc.)
    ts_parser_reset(parser);

    uint64_t t0 = now_ns();

    // Build string input + timeout options for parse_with_options
    CBMStringInput str_input = {source, (uint32_t)source_len};
    TSInput ts_input = {
        &str_input,
        cbm_string_read,
        TSInputEncodingUTF8,
        NULL,
    };

    TSParseOptions opts = {0};
    uint64_t deadline_ns = 0; // cppcheck-suppress unreadVariable
    if (timeout_micros > 0) {
        deadline_ns = t0 + ((uint64_t)timeout_micros * USEC_TO_NSEC);
        opts.payload = &deadline_ns;
        opts.progress_callback = cbm_timeout_cb;
    }

    TSTree *tree = ts_parser_parse_with_options(parser, NULL, ts_input, opts);
    uint64_t t1 = now_ns();

    if (!tree) {
        result->has_error = true;
        result->error_msg =
            cbm_arena_strdup(a, timeout_micros > 0 ? "parse timeout" : "parse failed");
        return result;
    }

    TSNode root = ts_tree_root_node(tree);

    // Compute module QN
    result->module_qn = cbm_fqn_module(a, project, rel_path);
    result->is_test_file = cbm_is_test_file(rel_path, language);

    // Build extraction context
    CBMExtractCtx ctx = {
        .arena = a,
        .result = result,
        .source = source,
        .source_len = source_len,
        .language = language,
        .project = project,
        .rel_path = rel_path,
        .module_qn = result->module_qn,
        .root = root,
    };

    // Run extractors: defs + imports use separate walks (unique recursion patterns),
    // then a single unified cursor walk handles the remaining 7 extractors.
    cbm_extract_definitions(&ctx);
    cbm_extract_imports(&ctx);
    cbm_extract_unified(&ctx);

    // Channel detection (Socket.IO / EventEmitter) — JS/TS only.
    cbm_extract_channels(&ctx);

    // K8s / Kustomize semantic pass (additional structured extraction for YAML-based infra files).
    if (ctx.language == CBM_LANG_KUSTOMIZE || ctx.language == CBM_LANG_K8S) {
        cbm_extract_k8s(&ctx);
    }

    // LSP type-aware call resolution
    uint64_t lsp_start = now_ns();
    if (language == CBM_LANG_GO) {
        cbm_run_go_lsp(a, result, source, source_len, root);
    }
    if (language == CBM_LANG_C || language == CBM_LANG_CPP || language == CBM_LANG_CUDA) {
        cbm_run_c_lsp(a, result, source, source_len, root, language != CBM_LANG_C);
    }
    atomic_fetch_add(&total_lsp_ns, now_ns() - lsp_start);

    // Second pass: preprocess C/C++/CUDA and extract additional macro-hidden calls.
    // Defs keep original-source line numbers; only CALLS are extracted from expanded source.
    if (language == CBM_LANG_C || language == CBM_LANG_CPP || language == CBM_LANG_CUDA) {
        uint64_t pp_start = now_ns();
        char *expanded = cbm_preprocess(source, source_len, rel_path, extra_defines, include_paths,
                                        language != CBM_LANG_C);
        if (expanded) {
            int expanded_len = (int)strlen(expanded);
            // Record calls count before second pass
            int calls_before = result->calls.count;

            // Parse expanded source with fresh tree
            TSParser *pp_parser = get_thread_parser(ts_lang, language);
            if (pp_parser) {
                ts_parser_reset(pp_parser);
                CBMStringInput pp_input = {expanded, (uint32_t)expanded_len};
                TSInput pp_ts_input = {
                    &pp_input,
                    cbm_string_read,
                    TSInputEncodingUTF8,
                    NULL,
                };
                TSParseOptions pp_opts = {0};
                TSTree *pp_tree =
                    ts_parser_parse_with_options(pp_parser, NULL, pp_ts_input, pp_opts);
                if (pp_tree) {
                    TSNode pp_root = ts_tree_root_node(pp_tree);

                    // Build context for expanded source — extract only calls via unified extractor
                    CBMExtractCtx pp_ctx = {
                        .arena = a,
                        .result = result,
                        .source = expanded,
                        .source_len = expanded_len,
                        .language = language,
                        .project = project,
                        .rel_path = rel_path,
                        .module_qn = result->module_qn,
                        .root = pp_root,
                    };
                    // Re-run unified extraction on expanded source.
                    // This adds macro-expanded calls; duplicates with original calls are
                    // harmless (pipeline deduplicates by caller+callee).
                    cbm_extract_unified(&pp_ctx);

                    // Also run LSP on expanded source for additional type-resolved calls
                    // (language is already C/C++/CUDA — checked in enclosing block)
                    cbm_run_c_lsp(a, result, expanded, expanded_len, pp_root,
                                  language != CBM_LANG_C);

                    ts_tree_delete(pp_tree);
                }
            }
            cbm_preprocess_free(expanded);
            atomic_fetch_add(&total_files_preprocessed, 1);
            (void)calls_before; // used for future logging
        }
        atomic_fetch_add(&total_preprocess_ns, now_ns() - pp_start);
    }

    uint64_t t2 = now_ns();

    result->imports_count = result->imports.count;

    // Accumulate profiling counters
    atomic_fetch_add(&total_parse_ns, t1 - t0);
    atomic_fetch_add(&total_extract_ns, t2 - t1);
    atomic_fetch_add(&total_files, 1);

    // Retain tree for cross-file LSP reuse (caller frees via cbm_free_tree)
    result->cached_tree = tree;
    result->cached_lang = language;
    return result;
}

void cbm_free_result(CBMFileResult *result) {
    if (!result) {
        return;
    }
    if (result->cached_tree) {
        ts_tree_delete(result->cached_tree);
        result->cached_tree = NULL;
    }
    cbm_arena_destroy(&result->arena);
    free(result);
}

void cbm_free_tree(CBMFileResult *result) {
    if (result && result->cached_tree) {
        ts_tree_delete(result->cached_tree);
        result->cached_tree = NULL;
    }
}

void cbm_free_tree_ptr(TSTree *tree) {
    if (tree) {
        ts_tree_delete(tree);
    }
}
