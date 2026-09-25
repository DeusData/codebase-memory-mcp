/* Full declaration set for the same CBMArena, and it must precede cbm.h:
 * internal/cbm/arena.h declares a subset and the two share the CBM_ARENA_H
 * guard, so whichever is included first is the one this file sees. */
#include "foundation/arena.h"      // cbm_arena_init_sized
#include "foundation/mem_core.h"   // class accounting for the bound allocators
#include "foundation/mem_events.h" // waste sanitizer: the bound allocators bypass every observer
#include "foundation/log.h"        // cbm_log_warn -- extract.lsp.skipped
#include "cbm.h"
#include "arena.h" // CBMArena, cbm_arena_init/alloc/strdup/destroy
#include "helpers.h"
#include "lang_specs.h"
#include "extract_unified.h"
#include "lsp/go_lsp.h"
#include "lsp/c_lsp.h"
#include "lsp/php_lsp.h"
#include "lsp/perl_lsp.h"
#include "lsp/py_lsp.h"
#include "lsp/ts_lsp.h"
#include "lsp/cs_lsp.h"
#include "lsp/java_lsp.h"
#include "lsp/kotlin_lsp.h"
#include "lsp/rust_lsp.h"
#include "preprocessor.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"  // cbm_fopen — crash-supervisor per-file marker write
#include "foundation/hash_table.h" // CBMHashTable — crash-supervisor quarantine set
#include "tree_sitter/api.h" // TSParser, TSNode, TSTree, TSInput, TSLanguage, TSPoint, TSParseOptions, TSParseState
#include "foundation/constants.h"
#include "mimalloc.h" // mi_malloc/mi_calloc/mi_realloc/mi_free/mi_usable_size — bind 3rd-party allocators (#424)
#if defined(CBM_BIND_TS_ALLOCATOR) && CBM_BIND_TS_ALLOCATOR
#include "sqlite3.h" // sqlite3_mem_methods, sqlite3_config, SQLITE_CONFIG_MALLOC — bind sqlite to mimalloc
#endif
#include <stdint.h> // uint32_t, uint64_t, int64_t
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
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

// C/C++ preprocessor #define macros are extracted as Macro nodes (#375). On a
// macro-dense codebase (e.g. the Linux kernel: ~2.4M macros, 49% of all nodes)
// this is the dominant extraction cost, so it is gated to the full/advanced
// index modes. Default ON to preserve behavior for direct callers/tests; the
// pipeline sets it from the index mode before extraction. Set once pre-extract,
// read-only during, so a relaxed atomic is sufficient.
static _Atomic int g_extract_macros = 1;
void cbm_set_macro_extraction(int enabled) {
    atomic_store_explicit(&g_extract_macros, enabled ? 1 : 0, memory_order_relaxed);
}
int cbm_macro_extraction_enabled(void) {
    return atomic_load_explicit(&g_extract_macros, memory_order_relaxed);
}

#define NSEC_PER_SEC 1000000000ULL
#define USEC_TO_NSEC 1000ULL
/* Use compat.h's cbm_clock_gettime which accepts CLOCK_MONOTONIC (value
 * varies by platform: 1 on Linux/Windows, 6 on macOS). We pass the
 * platform value via the compat.h fallback. */
#if defined(CLOCK_MONOTONIC)
#define CBM_CLOCK_MONO CLOCK_MONOTONIC
#elif defined(__APPLE__)
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

/* A definition name never spans lines. Config extractors hand over a node's
 * whole text — a YAML block key, a Makefile recipe (`endif\n\n$(obj)/x.h`), a
 * ktest .conf directive — and the line break rode into the graph (2026-09-16
 * probe: 8,301 Field nodes on elastic/elasticsearch, 91 kernel nodes). Cut at
 * the first line break and drop trailing blanks; the qualified name carries
 * the same text as its last segment and is cut the same way. */
static const char *cbm_first_line(CBMArena *a, const char *text) {
    if (!text) {
        return text;
    }
    const char *nl = strpbrk(text, "\r\n");
    if (!nl) {
        return text;
    }
    size_t n = (size_t)(nl - text);
    while (n > 0 && (text[n - 1] == ' ' || text[n - 1] == '\t')) {
        n--;
    }
    char *cut = (char *)cbm_arena_alloc(a, n + 1);
    if (!cut) {
        return text;
    }
    memcpy(cut, text, n);
    cut[n] = '\0';
    return cut;
}

/* JS/TS files: a definition named by a literal token is the walker naming a
 * function or variable from an object key or a numeric pattern element —
 * `{}` (1,534 incoming CALLS on microsoft/TypeScript's test baselines), `1`
 * (38 nodes), a quoted string. An identifier starts with a letter, `_`, `$`,
 * `#` (private members) or a non-ASCII byte; anything else is not a name. */
static bool cbm_js_family_path(const char *path) {
    if (!path) {
        return false;
    }
    const char *dot = strrchr(path, '.');
    if (!dot) {
        return false;
    }
    static const char *const exts[] = {".js",  ".mjs", ".cjs", ".jsx", ".ts",
                                       ".mts", ".cts", ".tsx", ".ets", NULL};
    for (int i = 0; exts[i]; i++) {
        if (strcmp(dot, exts[i]) == 0) {
            return true;
        }
    }
    return false;
}

static bool cbm_js_name_is_junk(const char *name) {
    if (!name || !name[0]) {
        return false; /* empty names are handled by the callers' own rules */
    }
    unsigned char c = (unsigned char)name[0];
    bool identifier_start = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' ||
                            c == '$' || c == '#' || c >= 0x80;
    /* Member names JS spells without an identifier start are still names:
     * computed keys (`[Symbol.iterator]`), string-literal keys (`"my-key"`,
     * `'x'`) and escaped identifiers (`А`). Measured on the TypeScript
     * corpus: 1,531 `[…]` members, 97 quoted members, 154 escaped
     * identifiers — all real definitions. What remains ({…} patterns, numeric
     * literals, parenthesised types, `...rest`) is a token, not a name. */
    bool member_key_start = c == '[' || c == '"' || c == '\'' || c == '\\';
    return !identifier_start && !member_key_start;
}

void cbm_defs_push(CBMDefArray *arr, CBMArena *a, CBMDefinition def) {
    def.name = cbm_first_line(a, def.name);
    def.qualified_name = cbm_first_line(a, def.qualified_name);
    if (cbm_js_family_path(def.file_path) && cbm_js_name_is_junk(def.name)) {
        return;
    }
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

/* Budget for the tree-sitter progress callback. The PRIMARY gate is per-thread
 * CPU time: a worker descheduled under CI contention burns WALL time but not
 * CPU, so a starved-but-parseable file must NOT be abandoned merely because the
 * 5 s budget elapsed in wall-clock against near-zero CPU. That false "parse
 * timeout" silently dropped a file's defs and, with them, every cross-file edge
 * those defs anchored (e.g. a Celery.send_task definition backing an ASYNC_CALLS
 * edge). A generous WALL ceiling stays as a backstop so a genuinely
 * stuck/spinning parse still terminates in bounded time. */
#define CBM_PARSE_WALL_CEILING_FACTOR 12ULL /* ~60 s ceiling for the 5 s CPU budget */
/* The unified walk has NO budget: it walks every node of every file.
 *
 * It used to stop on a thread-CPU deadline, which made the GRAPH a function of
 * how fast the machine happened to be running. Two indexes of the same C#
 * corpus, same machine, minutes apart, stopped the walk of
 * src/tests/JIT/jit64/opt/cse/hugeSimpleExpr1.cs at node 2,660,352 and at node
 * 2,574,336 (measured 2026-09-19). That file's tail is all field assignments,
 * so the two runs disagreed by 10 WRITES edges: a graph that differed run to
 * run for a reason no user could see or reproduce. A budget in NODES would at
 * least have been reproducible, but it still answers "what is in this repo?"
 * with "depends how much we felt like reading" — so there is no budget at all.
 * Everything the walk does is a pure function of the tree, and now so is when
 * it stops: at the end.
 *
 * What the deadline was protecting against is real and is handled where it
 * belongs — in the cost per node, not in a clock. Generated blobs (2.7-8.2 M
 * node trees) used to cost 18-48 us per node because usage stamping called
 * ts_node_parent, which descends from the root; the walk now carries its own
 * parent chain, so a deep tree costs what a shallow one does.
 *
 * CBM_WALK_MAX_NODES reinstates a budget for an operator who needs one
 * (0 = the default, no budget). */
#define CBM_WALK_MAX_NODES_DEFAULT 0u

/* Read fresh each call, like cbm_max_file_bytes: cheap next to a file walk, and
 * no memoized copy to go stale between runs in the same process. */
static uint32_t cbm_walk_max_nodes(void) {
    const char *raw = getenv("CBM_WALK_MAX_NODES");
    if (raw && raw[0]) {
        errno = 0;
        char *end = NULL;
        unsigned long v = strtoul(raw, &end, 10);
        if (errno == 0 && end != raw && *end == '\0' && v <= UINT32_MAX) {
            return (uint32_t)v; /* 0 is a legitimate choice: no budget */
        }
        /* Unparseable / out-of-range → the default, never an accidental 0. */
    }
    return CBM_WALK_MAX_NODES_DEFAULT;
}

typedef struct {
    uint64_t cpu_deadline_ns; // trip once this thread's CPU time passes it
    uint64_t wall_ceiling_ns; // hard wall backstop for a spinning/stuck parse
    /* Set by the callback when IT ended the parse. ts_parser_parse_with_options
     * returns NULL for several reasons and the budget is only one of them, but
     * the failure used to be reported as "parse timeout" whenever a budget was
     * configured at all. A 7-line .properties file was accused of timing out
     * (2026-09-19); whatever really happened to it, saying "timeout" sent the
     * next reader looking at the clock. */
    bool tripped;
} CBMParseBudget;

#ifdef CBM_ENABLE_TEST_SEAMS
/* Deterministic RED-repro seam (armed by CBM_TEST_WALL_STALL_ON in
 * cbm_extract_file_ex): when set, the timeout callback reads the WALL clock this
 * many ns ahead of reality while CPU time is untouched — emulating a worker
 * descheduled long enough for the old wall-only budget to elapse against
 * near-zero CPU. With the CPU-time budget the parse still completes; a wall-only
 * / tight-ceiling budget drops the file. Thread-local so it cannot leak across
 * worker threads. Compiled only into seam-enabled test artifacts. */
static CBM_TLS uint64_t tl_parse_wall_seam_offset_ns = 0;
#endif

/* tree-sitter's TSProgressCallback mandates a non-const TSParseState*; a const
 * parameter here would not match the opts.progress_callback assignment below. */
// cppcheck-suppress constParameterCallback
static bool cbm_timeout_cb(TSParseState *state) {
    CBMParseBudget *budget = (CBMParseBudget *)state->payload;
    uint64_t wall = now_ns();
#ifdef CBM_ENABLE_TEST_SEAMS
    wall += tl_parse_wall_seam_offset_ns;
#endif
    bool over =
        cbm_thread_cpu_time_ns() > budget->cpu_deadline_ns || wall > budget->wall_ceiling_ns;
    if (over) {
        budget->tripped = true; /* so a NULL tree can name its real cause */
    }
    return over;
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

// --- Allocator binding (defense-in-depth, #424) ---

/* Bind tree-sitter and sqlite3 to mimalloc explicitly so a correct
 * binary does NOT depend on the fragile MI_OVERRIDE symbol override. Under
 * MI_OVERRIDE=1 — particularly the Windows static-MinGW link with
 * --allow-multiple-definition — `malloc`/`free` can resolve to DIFFERENT
 * allocators (mimalloc vs the CRT) inside third-party libs, so a block
 * allocated by mimalloc gets freed by the CRT (or vice-versa), corrupting the
 * heap freelist (#424). Binding each library through one explicit allocator
 * eliminates that mismatch class generically, on every platform.
 *
 * Guarded to the production build (CBM_BIND_TS_ALLOCATOR=1, which CFLAGS_PROD
 * defines alongside MI_OVERRIDE=1). The test build is CRT + ASan, where binding
 * to mimalloc would mismatch ASan/CRT frees — there these binds compile to
 * no-ops and the build stays unchanged. */

/* SQLite on a dedicated mimalloc heap per thread: ON only in the index worker,
 * whose default heap holds the graph (SQLite churn on that heap paid a page
 * walk per allocation: 132 s vs 9.7 s on the kernel's coverage publish). OFF
 * everywhere else: the daemon runs a thread per connection, and a heap
 * created per such thread pins every SQLite block the shared connection
 * keeps (page cache, statement cache) to pages nobody's heap owns any more --
 * the Linux soak grew 180 KB per query, 11 -> 144 MB in ten minutes, where
 * the default thread heap had been flat (2026-09-14). */
static _Atomic int g_sqlite_dedicated_heap;

void cbm_sqlite_dedicated_heap(bool on) {
    atomic_store_explicit(&g_sqlite_dedicated_heap, on ? 1 : 0, memory_order_relaxed);
}

#if defined(CBM_BIND_TS_ALLOCATOR) && CBM_BIND_TS_ALLOCATOR
#include <assert.h>

/* sqlite3 mem methods backed by mimalloc. sqlite's xMalloc/xRealloc/xSize use
 * `int` sizes; wrap with size_t casts. xRoundup rounds to an 8-byte boundary
 * (sqlite requires 8-byte-aligned roundup, and mimalloc honors that alignment).
 * Field order matches struct sqlite3_mem_methods exactly:
 * xMalloc, xFree, xRealloc, xSize, xRoundup, xInit, xShutdown, pAppData. */
/* Profiled: these bindings bypass the malloc interposer entirely, so without a
 * hook here the biggest per-request allocations in the process — SQLite's page
 * cache and its query working set — are invisible to the attribution profile
 * (#581). */
/* SQLite allocates from a mimalloc heap of its own, one per thread. Its page
 * queues then hold SQLite blocks only. Sharing the thread's default heap with
 * the graph -- 40M+ blocks, millions of pages once the graph moved onto the
 * core -- made every statement-journal chunk of the coverage publish step pay
 * a walk over the graph's pages: 132 s on the kernel where v0.10.8, whose
 * graph lived outside mimalloc, took 9.7 s (sampled 2026-09-14). mi_free
 * works across heaps, so xFree and cross-thread frees are unchanged; a
 * thread's heap is released with the thread. */
static _Thread_local mi_heap_t *tl_sqlite_heap;

/* NULL = the calling thread's default heap (mi_malloc); see the switch above. */
static mi_heap_t *sqlite_heap(void) {
    if (!atomic_load_explicit(&g_sqlite_dedicated_heap, memory_order_relaxed)) {
        return NULL;
    }
    if (!tl_sqlite_heap) {
        tl_sqlite_heap = mi_heap_new();
    }
    return tl_sqlite_heap;
}

#if defined(CBM_MEMWASTE) && CBM_MEMWASTE
#define BOUND_ALLOC(block, n, flags)                                                             \
    do {                                                                                         \
        if ((block) && cbm_memev_enabled()) {                                                    \
            cbm_memev_alloc_ex((block), (n), mi_usable_size(block), __builtin_return_address(0), \
                               (unsigned)(flags));                                               \
        }                                                                                        \
    } while (0)
#define BOUND_REALLOC(old_block, grown, n)                                      \
    do {                                                                        \
        if ((grown) && cbm_memev_enabled()) {                                   \
            cbm_memev_realloc((old_block), (grown), (n), mi_usable_size(grown), \
                              __builtin_return_address(0));                     \
        }                                                                       \
    } while (0)
#define BOUND_FREE(block) cbm_memev_free(block)
#else
#define BOUND_ALLOC(block, n, flags) ((void)0)
#define BOUND_REALLOC(old_block, grown, n) ((void)0)
#define BOUND_FREE(block) ((void)0)
#endif

static void *cbm_sqlite_malloc(int n) {
    mi_heap_t *heap = sqlite_heap();
    void *block = heap ? mi_heap_malloc(heap, (size_t)n) : mi_malloc((size_t)n);
    if (block) {
        cbm_mem_class_add_external(CBM_MEM_CLASS_STORE, mi_usable_size(block));
    }
    BOUND_ALLOC(block, (size_t)n, 0);
    return block;
}
static void cbm_sqlite_free(void *p) {
    if (p) {
        cbm_mem_class_remove_external(CBM_MEM_CLASS_STORE, mi_usable_size(p));
        BOUND_FREE(p);
    }
    mi_free(p);
}
static void *cbm_sqlite_realloc(void *p, int n) {
    size_t old_size = p ? mi_usable_size(p) : 0;
    mi_heap_t *heap = sqlite_heap();
    CBM_MEMEV_BACKING(1);
    void *grown = heap ? mi_heap_realloc(heap, p, (size_t)n) : mi_realloc(p, (size_t)n);
    CBM_MEMEV_BACKING(-1);
    if (grown) {
        cbm_mem_class_remove_external(CBM_MEM_CLASS_STORE, old_size);
        cbm_mem_class_add_external(CBM_MEM_CLASS_STORE, mi_usable_size(grown));
    }
    BOUND_REALLOC(p, grown, (size_t)n);
    return grown;
}
static int cbm_sqlite_size(void *p) {
    return (int)mi_usable_size(p);
}
static int cbm_sqlite_roundup(int n) {
    return (n + 7) & ~7; /* round up to 8-byte boundary */
}
/* Same reasoning as the sqlite bindings: tree-sitter allocates its parse trees
 * through these, and they too skip the interposer. */
static void *cbm_ts_malloc(size_t n) {
    void *block = mi_malloc(n);
    if (block) {
        cbm_mem_class_add_external(CBM_MEM_CLASS_TS_TREE, mi_usable_size(block));
    }
    BOUND_ALLOC(block, n, 0);
    return block;
}
static void *cbm_ts_calloc(size_t count, size_t size) {
    void *block = mi_calloc(count, size);
    if (block) {
        cbm_mem_class_add_external(CBM_MEM_CLASS_TS_TREE, mi_usable_size(block));
    }
    BOUND_ALLOC(block, count * size, CBM_MEMEV_ZEROED);
    return block;
}
static void *cbm_ts_realloc(void *p, size_t n) {
    size_t old_size = p ? mi_usable_size(p) : 0;
    CBM_MEMEV_BACKING(1);
    void *grown = mi_realloc(p, n);
    CBM_MEMEV_BACKING(-1);
    if (grown) {
        cbm_mem_class_remove_external(CBM_MEM_CLASS_TS_TREE, old_size);
        cbm_mem_class_add_external(CBM_MEM_CLASS_TS_TREE, mi_usable_size(grown));
    }
    BOUND_REALLOC(p, grown, n);
    return grown;
}
static void cbm_ts_free(void *p) {
    if (p) {
        cbm_mem_class_remove_external(CBM_MEM_CLASS_TS_TREE, mi_usable_size(p));
        BOUND_FREE(p);
    }
    mi_free(p);
}

static int cbm_sqlite_meminit(void *appdata) {
    (void)appdata;
    return SQLITE_OK;
}
static void cbm_sqlite_memshutdown(void *appdata) {
    (void)appdata;
}
#endif /* CBM_BIND_TS_ALLOCATOR */

void cbm_alloc_init(void) {
#if defined(CBM_BIND_TS_ALLOCATOR) && CBM_BIND_TS_ALLOCATOR
    static int alloc_bound = 0; /* single-threaded startup; plain int is fine */
    if (alloc_bound) {
        return;
    }
    alloc_bound = 1;

    /* tree-sitter runtime (was previously bound in cbm_init; consolidated here). */
    ts_set_allocator(cbm_ts_malloc, cbm_ts_calloc, cbm_ts_realloc, cbm_ts_free);

    /* sqlite3. SQLITE_CONFIG_MALLOC MUST run before sqlite3_initialize / the
     * first sqlite3_open* — otherwise sqlite3_config returns SQLITE_MISUSE
     * silently and the binding is ignored. cbm_alloc_init() runs as the very
     * first statement of main(), before cbm_mcp_server_new → cbm_store_open*. */
    static sqlite3_mem_methods cbm_sqlite_mem = {
        cbm_sqlite_malloc,      /* xMalloc */
        cbm_sqlite_free,        /* xFree */
        cbm_sqlite_realloc,     /* xRealloc */
        cbm_sqlite_size,        /* xSize */
        cbm_sqlite_roundup,     /* xRoundup */
        cbm_sqlite_meminit,     /* xInit */
        cbm_sqlite_memshutdown, /* xShutdown */
        NULL,                   /* pAppData */
    };
    int sqlite_rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &cbm_sqlite_mem);
    assert(sqlite_rc == SQLITE_OK && "SQLITE_CONFIG_MALLOC must run before sqlite3_initialize");
    (void)sqlite_rc;
#endif /* CBM_BIND_TS_ALLOCATOR */
}

// --- Init/Shutdown ---

static int cbm_initialized = 0;

int cbm_init(void) {
    if (cbm_initialized) {
        return 0;
    }
    enum { CBM_INIT_DONE = 1 };
    cbm_initialized = CBM_INIT_DONE;
    /* Defense-in-depth allocator binds (idempotent). main() calls cbm_alloc_init
     * first; this covers non-main entry points (pipeline passes call cbm_init).
     * For sqlite the SQLITE_CONFIG_MALLOC bind only takes effect if it runs
     * before sqlite initializes — main() guarantees that ordering; here it is a
     * best-effort idempotent re-assert for paths that never hit main(). */
    cbm_alloc_init();
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

enum { FIELD_CACHE_SLOTS = 512, FIELD_NAME_CACHED_MAX = 31 };
typedef struct {
    const TSLanguage *lang;
    uint32_t len;
    TSFieldId id;
    char name[FIELD_NAME_CACHED_MAX + 1];
} field_id_slot_t;
/* On the HEAP behind a thread-local pointer, not a thread-local array: glibc
 * takes the static TLS block out of each thread's stack allocation, so a 24 KB
 * array here is 24 KB every thread must fit — and the threads that ask for a
 * small stack (the 64 KB parent-death watchdog) then fail to start at all,
 * with EINVAL surfacing nowhere near the growth that caused it. See
 * cbm_thread_create's fallback for the other half of that lesson. */
static CBM_TLS field_id_slot_t *tl_field_ids;

static field_id_slot_t *field_cache(void) {
    if (!tl_field_ids) {
        tl_field_ids = cbm_calloc(CBM_MEM_CLASS_OTHER, FIELD_CACHE_SLOTS * sizeof(field_id_slot_t));
    }
    return tl_field_ids;
}

static void cbm_ts_field_cache_release_thread(void) {
    cbm_free(CBM_MEM_CLASS_OTHER, tl_field_ids);
    tl_field_ids = NULL;
}

TSNode cbm_ts_child_by_field_name(TSNode node, const char *name, uint32_t name_length) {
    if (!name || name_length == 0 || name_length > FIELD_NAME_CACHED_MAX || ts_node_is_null(node)) {
        return (ts_node_child_by_field_name)(node, name, name_length); /* the real one */
    }
    field_id_slot_t *cache = field_cache();
    if (!cache) {
        return (ts_node_child_by_field_name)(node, name, name_length); /* uncached, still correct */
    }
    const TSLanguage *lang = ts_node_language(node);
    uint64_t h = 0xcbf29ce484222325ULL ^ (uint64_t)(uintptr_t)lang;
    for (uint32_t i = 0; i < name_length; i++) {
        h ^= (uint8_t)name[i];
        h *= 0x100000001b3ULL;
    }
    field_id_slot_t *slot = &cache[h & (FIELD_CACHE_SLOTS - 1)];
    if (slot->lang != lang || slot->len != name_length ||
        memcmp(slot->name, name, name_length) != 0) {
        /* The content is compared, never just the pointer: callers also pass
         * names built in reused buffers. */
        slot->id = ts_language_field_id_for_name(lang, name, name_length);
        slot->lang = lang;
        slot->len = name_length;
        memcpy(slot->name, name, name_length);
    }
    return ts_node_child_by_field_id(node, slot->id);
}

static CBM_TLS TSTreeCursor tl_cursor;
static CBM_TLS bool tl_cursor_live = false;

TSTreeCursor *cbm_thread_cursor(TSNode node) {
    if (tl_cursor_live) {
        ts_tree_cursor_reset(&tl_cursor, node);
    } else {
        tl_cursor = ts_tree_cursor_new(node);
        tl_cursor_live = true;
    }
    return &tl_cursor;
}

/* Per-depth cursors for recursive walks (cbm_cursor_acquire). A walk at depth d
 * holds slot d while it recurses into depth d + 1; the LSP call walkers used to
 * create and delete a cursor at EVERY node -- 49.8 M cursor stacks on the Go
 * corpus (waste sanitizer, 2026-09-17). Deeper than the pool: private cursors. */
enum { CURSOR_POOL_DEPTH = 128 };
/* Heap-backed for the same reason as the field cache: 4 KB of cursors plus
 * their state in static TLS is 4 KB charged to every thread in the image, and
 * it is the kind of growth that makes a small-stack thread refuse to start. */
typedef struct {
    TSTreeCursor cursors[CURSOR_POOL_DEPTH];
    uint8_t state[CURSOR_POOL_DEPTH]; /* bit0 live, bit1 busy */
} cursor_pool_t;
static CBM_TLS cursor_pool_t *tl_cursor_pool;
enum { CURSOR_LIVE = 1, CURSOR_BUSY = 2 };

TSTreeCursor *cbm_cursor_acquire(cbm_cursor_lease_t *lease, int depth, TSNode node) {
    if (!tl_cursor_pool) {
        tl_cursor_pool = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(cursor_pool_t));
    }
    cursor_pool_t *pool = tl_cursor_pool;
    if (pool && depth >= 0 && depth < CURSOR_POOL_DEPTH && !(pool->state[depth] & CURSOR_BUSY)) {
        if (pool->state[depth] & CURSOR_LIVE) {
            ts_tree_cursor_reset(&pool->cursors[depth], node);
        } else {
            pool->cursors[depth] = ts_tree_cursor_new(node);
        }
        pool->state[depth] = CURSOR_LIVE | CURSOR_BUSY;
        lease->slot = depth;
        lease->cursor = &pool->cursors[depth];
        return lease->cursor;
    }
    /* No pool (or the slot is taken): a private cursor is always correct. */
    lease->private_cursor = ts_tree_cursor_new(node);
    lease->slot = -1;
    lease->cursor = &lease->private_cursor;
    return lease->cursor;
}

void cbm_cursor_release(cbm_cursor_lease_t *lease) {
    if (lease->slot < 0) {
        ts_tree_cursor_delete(&lease->private_cursor);
    } else if (tl_cursor_pool) {
        tl_cursor_pool->state[lease->slot] &= (uint8_t)~CURSOR_BUSY;
    }
    lease->cursor = NULL;
}

void cbm_destroy_thread_parser(void) {
    // Full cleanup: delete the parser. Call on worker thread exit.
    if (tl_parser) {
        ts_parser_delete(tl_parser);
        tl_parser = NULL;
        tl_parser_lang = CBM_LANG_COUNT;
    }
    if (tl_cursor_live) {
        ts_tree_cursor_delete(&tl_cursor);
        tl_cursor_live = false;
    }
    if (tl_cursor_pool) {
        bool busy = false;
        for (int d = 0; d < CURSOR_POOL_DEPTH; d++) {
            /* a busy slot belongs to a walk still on this stack: leave it */
            if (tl_cursor_pool->state[d] == CURSOR_LIVE) {
                ts_tree_cursor_delete(&tl_cursor_pool->cursors[d]);
                tl_cursor_pool->state[d] = 0;
            } else if (tl_cursor_pool->state[d] & CURSOR_BUSY) {
                busy = true;
            }
        }
        /* Outstanding leases point INTO this block, so it is only released once
         * no walk still holds a slot. */
        if (!busy) {
            cbm_free(CBM_MEM_CLASS_OTHER, tl_cursor_pool);
            tl_cursor_pool = NULL;
        }
    }
    cbm_ts_field_cache_release_thread();
}

void cbm_shutdown(void) {
    // Clean up thread-local parser for the calling thread.
    // Note: other threads' TLS parsers are freed when those threads exit.
    cbm_destroy_thread_parser();
    cbm_initialized = 0;
}

// --- Bottleneck call-name classification (language-agnostic heuristics) ---

// Case-insensitive equality for short callee names.
static bool name_ieq(const char *a, const char *b) {
    for (; *a && *b; a++, b++) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) {
            return false;
        }
    }
    return *a == '\0' && *b == '\0';
}

static bool name_in_set(const char *name, const char *const *set) {
    for (const char *const *s = set; *s; s++) {
        if (name_ieq(name, *s)) {
            return true;
        }
    }
    return false;
}

// Linear-scan / membership calls: a hit inside a loop is the textbook hidden
// O(n^2) (cf. Olivo et al., PLDI'15) that syntactic loop-depth alone misses.
static bool is_linear_scan_name(const char *n) {
    static const char *const set[] = {"find",    "indexof",   "contains", "includes", "search",
                                      "lookup",  "strstr",    "strchr",   "strrchr",  "memchr",
                                      "find_if", "findindex", "count",    "index",    NULL};
    return name_in_set(n, set);
}

// Allocation / growable-append calls: repeated inside a loop is the classic
// accidental reallocation / string-concat O(n^2). Names are deliberately
// conservative; meaningless in some languages → simply never matches there.
static bool is_alloc_name(const char *n) {
    static const char *const set[] = {"malloc",  "calloc",    "realloc",      "strdup", "strndup",
                                      "append",  "push_back", "emplace_back", "concat", "strcat",
                                      "strncat", "push",      "pushback",     NULL};
    return name_in_set(n, set);
}

// Extract the receiver identifier from a def's receiver text — Go's
// "(s *Store)" / "(s Store)" → "s". Stores the identifier start in *out and
// returns its length; returns 0 for unnamed receivers ("(*Store)", "(Store)"),
// where no second token follows the identifier (a lone token is the TYPE, not
// a name — such methods have no receiver variable to call through anyway).
static size_t receiver_ident(const char *recv_text, const char **out) {
    const char *p = recv_text;
    if (*p == '(') {
        p++;
    }
    while (*p == ' ' || *p == '\t') {
        p++;
    }
    const char *start = p;
    while ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') ||
           *p == '_') {
        p++;
    }
    size_t len = (size_t)(p - start);
    if (len == 0) {
        return 0; // "(*Store)": leading '*', no identifier
    }
    while (*p == ' ' || *p == '\t') {
        p++;
    }
    if (*p == ')' || *p == '\0') {
        return 0; // "(Store)": single token is the type, receiver unnamed
    }
    *out = start;
    return len;
}

// Whether a callee expression targets the same instance/class as the enclosing
// def, i.e. counts as genuine self-recursion rather than a same-named call on a
// different receiver. callee_name may be bare ("recur") or qualified
// ("self.recur", "this.recur", "super().save", "axios.get", "self.obj.recur").
//
// Bare names have no receiver → assume self-call (free function calling itself
// by bare name; preserves prior behavior). Qualified names: the receiver chain
// is everything before the LAST '.', and the WHOLE chain must name the same
// object — self/this/cls/@self, or the enclosing def's own receiver identifier
// (Go: `s` in `func (s *Store) save()`, from CBMDefinition.receiver). Matching
// the whole chain (not its first segment) keeps self.obj.recur() out: it
// targets self's FIELD obj, a different object. super() is the parent class and
// any other receiver (axios, console, ...) a different target. See #599.
static bool is_self_receiver(const char *callee_name, const char *def_receiver) {
    if (!callee_name || !callee_name[0]) {
        return false;
    }
    const char *dot = strrchr(callee_name, '.');
    if (!dot) {
        return true; // bare name → self-recursion candidate
    }
    size_t rlen = (size_t)(dot - callee_name);
    static const char *const self_receivers[] = {"self", "this", "cls", "@self", NULL};
    for (int i = 0; self_receivers[i]; i++) {
        size_t sl = strlen(self_receivers[i]);
        if (rlen == sl && strncmp(callee_name, self_receivers[i], sl) == 0) {
            return true;
        }
    }
    if (def_receiver) {
        const char *rid = NULL;
        size_t ril = receiver_ident(def_receiver, &rid);
        if (ril > 0 && ril == rlen && strncmp(callee_name, rid, ril) == 0) {
            return true; // call through the enclosing method's own receiver
        }
    }
    return false; // super() / axios / console / self.obj / any other receiver
}

// Count parameters from a signature string like "(int a, Foo* b, cb (*)(int,int))".
// Fallback for languages where param_names isn't populated (e.g. C keeps only the
// signature text). Counts commas at the top paren level; treats "()"/"(void)" as 0.
// Approximate by design (a structural smell, not an exact arity).
static int count_params_from_signature(const char *sig) {
    if (!sig) {
        return 0;
    }
    const char *p = sig;
    while (*p && *p != '(') {
        p++;
    }
    if (*p != '(') {
        return 0;
    }
    p++;
    const char *list = p;
    int depth = 0;
    int commas = 0;
    bool any = false;
    for (; *p; p++) {
        char ch = *p;
        if (ch == '(' || ch == '[' || ch == '{' || ch == '<') {
            depth++;
        } else if (ch == ')') {
            if (depth == 0) {
                break;
            }
            depth--;
        } else if (ch == ']' || ch == '}' || ch == '>') {
            if (depth > 0) {
                depth--;
            }
        } else if (ch == ',' && depth == 0) {
            commas++;
        } else if (!isspace((unsigned char)ch)) {
            any = true;
        }
    }
    if (!any) {
        return 0; /* "()" */
    }
    if (commas == 0) {
        while (*list == ' ' || *list == '\t') {
            list++;
        }
        if (strncmp(list, "void", 4) == 0 &&
            (list[4] == ')' || list[4] == ' ' || list[4] == '\0')) {
            return 0; /* C "(void)" */
        }
    }
    return commas + 1;
}

// --- Main extraction function ---

/* Test-only deterministic fault injection for the crash/hang supervisor tests.
 * Gated entirely behind env vars that are never set in production; a matching
 * rel_path either aborts (a fault signal the supervisor classifies as a crash)
 * or spins forever (an external-scanner infinite loop the quiet-timeout kills).
 * This gives an honest guard — green iff the supervisor actually contains a real
 * fault — instead of a fixture that may stop faulting once a root cause is fixed. */
/* Crash-supervisor per-file marker JOURNAL (Stage 3c skip-and-continue,
 * parallel-safe). Recovery re-runs are PARALLEL (there are no sequential
 * production runs), so a single overwrite-style marker would race across
 * workers and — worse — go stale during non-extract phases, blaming
 * whatever file was extracted LAST (that mis-quarantined four innocent
 * ms-typescript fixtures, one 15-minute retry at a time). Instead every
 * worker APPENDS one short line per event: "S <rel_path>" when it STARTS
 * work on a file, "D <rel_path>" when it finishes it. A single short
 * append of one line is atomic in practice on every target platform, and
 * the parent discards a torn final line by design. The parent's suspect
 * set after a crash/hang = files with an S but no D — exactly the
 * in-flight set; a file is only quarantined after appearing in the
 * suspect set of TWO CONSECUTIVE failed runs, so a stale or merely
 * unlucky in-flight file is never quarantined alone. The env var is set
 * solely by the supervisor during recovery — a no-op on normal runs. */
static void cbm_index_mark(const char *rel_path, char event) {
    const char *mf = getenv("CBM_INDEX_MARKER_FILE");
    if (!mf || !mf[0] || !rel_path || !rel_path[0]) {
        return;
    }
    FILE *f = cbm_fopen(mf, "ab");
    if (f) {
        (void)fprintf(f, "%c %s\n", event, rel_path);
        (void)fclose(f);
    }
}

void cbm_index_mark_start(const char *rel_path) {
    cbm_index_mark(rel_path, 'S');
}

void cbm_index_mark_done(const char *rel_path) {
    cbm_index_mark(rel_path, 'D');
}

/* ── Crash-quarantine set (Stage 3c skip-and-continue) ──────────────────────
 * After a crash the supervisor re-runs the worker single-threaded, passing
 * CBM_INDEX_QUARANTINE_FILE — a newline-delimited list of repo-relative paths
 * that already crashed the indexer and MUST NOT be extracted again. Owned here,
 * next to the other env-driven extract hooks (marker + fault injector), so the
 * single hard guard lives at the one choke point every pass funnels through
 * (cbm_extract_file): whether a pass re-extracts from disk on a cache miss
 * (sequential pass_calls/usages/semantic) or extracts fresh, a quarantined file
 * short-circuits to an empty result and never reaches the parser/crash. The
 * pipeline extract loops separately REPORT the skip as phase="crash" via
 * cbm_index_is_quarantined() so the crasher surfaces in the response skipped[].
 * Loaded once, lazily; read-only after load (safe for the parallel workers,
 * though recovery runs single-threaded). Unset env ⇒ empty set ⇒ cheap no-op. */
static CBMHashTable *g_quarantine_set = NULL;
enum { CBM_QSET_UNINIT = 0, CBM_QSET_INITING = 1, CBM_QSET_INITED = 2 };
static atomic_int g_quarantine_state = CBM_QSET_UNINIT;

static void cbm_quarantine_load(void) {
    const char *qf = getenv("CBM_INDEX_QUARANTINE_FILE");
    if (!qf || !qf[0]) {
        return; /* normal path: empty set */
    }
    FILE *f = cbm_fopen(qf, "rb");
    if (!f) {
        return;
    }
    CBMHashTable *set = cbm_ht_create(16);
    if (!set) {
        (void)fclose(f);
        return;
    }
    char line[2048];
    while (fgets(line, sizeof(line), f)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }
        /* Line format: "path\tphase" where phase is "crash" or "hang". A bare
         * "path" line (no tab) is tolerated and defaults to phase "crash" for
         * backward compatibility with older quarantine files. */
        char *tab = strchr(line, '\t');
        const char *phase = "crash";
        if (tab) {
            *tab = '\0';
            if (tab[1]) {
                phase = tab + 1;
            }
        }
        if (line[0] == '\0') {
            continue; /* empty path (line began with a tab) — skip */
        }
        /* The table borrows the key + value pointers, so dup both. Intentionally
         * never freed: the set lives for the whole (short-lived worker) process.
         * The value stores the phase so cbm_index_quarantine_phase() can report
         * "crash" vs "hang"; membership (cbm_index_is_quarantined) is value != NULL. */
        char *pval = cbm_strdup(phase);
        if (!pval) {
            continue;
        }
        if (cbm_ht_has(set, line)) {
            /* Duplicate path line: reuse the stored key (the table borrows key
             * pointers, so a fresh copy would leak on replace) and free the
             * value it displaces. */
            free(cbm_ht_set(set, line, (void *)pval));
        } else {
            char *key = cbm_strdup(line);
            if (key) {
                cbm_ht_set(set, key, (void *)pval);
            } else {
                /* Partial failure: don't leak the value copy. */
                free(pval);
            }
        }
    }
    (void)fclose(f);
    g_quarantine_set = set;
}

bool cbm_index_is_quarantined(const char *rel_path) {
    if (!rel_path || !rel_path[0]) {
        return false;
    }
    int state = atomic_load(&g_quarantine_state);
    if (state != CBM_QSET_INITED) {
        /* First caller wins the CAS and loads; racers spin until INITED.
         * Same once-init pattern as cbm_ui_log_init (http_server.c). */
        state = CBM_QSET_UNINIT;
        if (atomic_compare_exchange_strong(&g_quarantine_state, &state, CBM_QSET_INITING)) {
            cbm_quarantine_load();
            atomic_store(&g_quarantine_state, CBM_QSET_INITED);
        } else {
            while (atomic_load(&g_quarantine_state) != CBM_QSET_INITED) {
                cbm_usleep(1000); /* 1ms */
            }
        }
    }
    return g_quarantine_set && cbm_ht_has(g_quarantine_set, rel_path);
}

const char *cbm_index_quarantine_phase(const char *rel_path) {
    /* cbm_index_is_quarantined drives the lazy once-load and returns true only
     * when the set is loaded and holds rel_path — so on true, g_quarantine_set is
     * non-NULL and the stored value is the phase string ("crash"/"hang"). */
    if (!cbm_index_is_quarantined(rel_path)) {
        return NULL;
    }
    return (const char *)cbm_ht_get(g_quarantine_set, rel_path);
}

#ifdef CBM_ENABLE_TEST_SEAMS
/* Deterministic supervisor fault injection belongs only in explicitly
 * seam-enabled test artifacts. Ordinary release binaries must never expose a
 * filename-selected abort or infinite loop through environment variables. */
static void cbm_test_fault_inject(const char *rel_path) {
    if (!rel_path || !rel_path[0]) {
        return;
    }
    const char *crash_on = getenv("CBM_TEST_CRASH_ON");
    if (crash_on && crash_on[0] && strstr(rel_path, crash_on)) {
        abort(); /* SIGABRT → WIFSIGNALED → classified as a crash */
    }
    const char *hang_on = getenv("CBM_TEST_HANG_ON");
    if (hang_on && hang_on[0] && strstr(rel_path, hang_on)) {
        for (;;) {
            /* Busy-spin: the supervisor's quiet-timeout kills + reports us. */
        }
    }
    const char *exit_on = getenv("CBM_TEST_EXIT_ON");
    if (exit_on && exit_on[0] && strstr(rel_path, exit_on)) {
        exit(1); /* Nonzero exit code → CBM_PROC_EXIT_NONZERO → classified as "error" */
    }
}
#endif

/* Pre-parse nesting guard for pathologically nested input. tree-sitter's GLR
 * parser recurses once per nesting level inside stack_node_add_link
 * (vendored ts_runtime/src/stack.c) while merging ambiguous parse-stack heads.
 * The Perl grammar is genuinely ambiguous for `f(...)` (function call vs.
 * bareword), so a deeply nested call chain `f(f(f(...)))` drives that recursion
 * as deep as the nesting and overflows a small (1 MB Windows) stack *during the
 * parse* — before any of the LSP walk-depth guards can fire. Unambiguous
 * grammars (C/Java/Python) keep a single stack head and don't hit this, which is
 * why only Perl crashed on the Windows/ARM CI runners.
 *
 * This is a workaround: the proper fix is bounding the GLR stack-merge recursion
 * inside the vendored tree-sitter runtime, tracked upstream as #913. Remove this
 * guard once that lands.
 *
 * cbm_source_nesting_exceeds scans the raw bytes for the maximum bracket-nesting
 * depth and returns true as soon as it passes the cap (early-exit, O(n)). Real
 * source never nests brackets this deep, so a file that does is skipped as a
 * parse error (zero edges — graceful degradation, never a crash). Brackets in
 * strings/comments are counted too: the only consequence of a false positive is
 * skipping one absurd file, so string-awareness is not worth the cost. */
#define CBM_PERL_MAX_PARSE_NESTING 128

static bool cbm_source_nesting_exceeds(const char *source, int source_len, int cap) {
    int depth = 0;
    for (int i = 0; i < source_len; i++) {
        char c = source[i];
        if (c == '(' || c == '[' || c == '{') {
            if (++depth > cap) {
                return true;
            }
        } else if ((c == ')' || c == ']' || c == '}') && depth > 0) {
            depth--;
        }
    }
    return false;
}

/* Best-effort parse-coverage collection (#963). Walks only the has_error paths
 * of the tree and records the 1-based line ranges of the TOP-MOST ERROR/MISSING
 * nodes (does not descend into an error subtree — one range per failed region).
 * Bounded by CBM_MAX_ERROR_REGIONS so pathological input can't blow up the
 * output. The ranges mark where constructs were dropped; they are a detection
 * aid, never a completeness proof.
 *
 * `dropped` counts the ranges the cap threw away. It exists so a clipped list
 * cannot read as a complete one: cbm_error_ranges_str turns a non-zero count
 * into a trailing "+<N>" marker. Phase 2 split one whole-file range into many
 * small ones, which pushed real files straight into a cap that used to be
 * unreachable, so the clip is live behaviour and not a theoretical limit. */
#define CBM_MAX_ERROR_REGIONS 256
typedef struct {
    uint32_t starts[CBM_MAX_ERROR_REGIONS];
    uint32_t ends[CBM_MAX_ERROR_REGIONS];
    int count;
    int dropped;
} cbm_error_regions_t;

static void cbm_error_regions_push(cbm_error_regions_t *acc, TSNode n) {
    TSPoint start = ts_node_start_point(n);
    TSPoint end = ts_node_end_point(n);
    uint32_t start_line = start.row + 1;
    uint32_t end_line = end.row + 1;

    /* A node that ends at column 0 stopped right after the previous line's
     * newline, so it holds no text on the row it points at. Counting that row
     * named a line past the end of the file whenever the region ran to EOF:
     * scripts/setup-windows.ps1 has 326 lines and reported "245-327". */
    if (end.column == 0 && end.row > start.row) {
        end_line = end.row;
    }

    /* One line can carry several error nodes, and repeating the same line range
     * says nothing new. Line 113 of scripts/setup-windows.ps1 has two error
     * nodes, at columns 25-29 and 31-32, and the report read "113-113,113-113".
     * Drop the repeat.
     *
     * Only an EXACT repeat of the range already open is dropped. Do not merge
     * ranges that merely overlap. Each range is judged separately later by
     * cbm_region_is_recovered, which asks whether definitions starting inside
     * that range cover it. Two ranges with the same numbers always get the same
     * verdict, so collapsing them changes nothing. Two DIFFERENT ranges do not:
     * merging 3-3 into 2-3 hands the wider range's covering definition to an
     * error the definition does not explain, and a real parse failure then
     * disappears from the report. tests/test_parse_coverage.c pins that case in
     * perl_malformed_source_remains_partial_issue1838.
     *
     * This runs BEFORE the cap check, so a dropped repeat never counts as a
     * range the cap threw away. */
    if (acc->count > 0 && start_line == acc->starts[acc->count - 1] &&
        end_line == acc->ends[acc->count - 1]) {
        return;
    }

    if (acc->count >= CBM_MAX_ERROR_REGIONS) {
        acc->dropped++;
        return;
    }
    acc->starts[acc->count] = start_line;
    acc->ends[acc->count] = end_line;
    acc->count++;
}

/* #1610: a file that does not end with a newline leaves the grammar's
 * mandatory line terminator MISSING. That node is ZERO-WIDTH and sits at EOF.
 *
 * It is not a miss. The parser consumed no source for it — start_byte ==
 * end_byte — so by construction nothing was dropped: no construct can live in
 * a zero-byte span, and every real instruction above it parsed normally. This
 * is a property of the grammar's terminator rule, not of the file.
 *
 * Flagging it made the verdict arbitrary. Grammars whose terminator token is
 * VISIBLE (dockerfile, tcl, fish, gomod, hyprlang) reported parse_partial for
 * a missing final newline; grammars whose terminator is HIDDEN (ini, fsharp,
 * beancount, requirements, gitignore, sshconfig, kconfig) reported nothing for
 * exactly the same omission, because a hidden node is invisible to
 * ts_node_child(). Whether a user was told their file was partially parsed
 * depended on a grammar-authoring accident.
 *
 * The cost was not cosmetic: a phantom parse_partial writes a
 * "<project>::missed" shadow row, and until #1609 that row made the project
 * fail cross-repo validation as both source and target.
 *
 * Deliberately narrow — ZERO-WIDTH AT EOF ONLY. A MISSING or ERROR node with
 * WIDTH still counts even at EOF (a Makefile whose last recipe line is
 * unterminated really does lose the recipe), and anything before EOF is
 * untouched.
 *
 * #1746: the Dockerfile grammar places that zero-width missing newline before
 * trailing whitespace rather than at raw EOF. Preserve the broad exact-EOF
 * rule above; only extend it past blanks when the missing token is specifically
 * a newline. */
static bool cbm_is_blank_not_newline(char c) {
    return c == ' ' || c == '\t' || c == '\v' || c == '\f' || c == '\r';
}

static bool cbm_is_eof_terminator_miss(TSNode n, const char *source, int source_len) {
    if (!ts_node_is_missing(n) || source_len < 0) {
        return false;
    }
    uint32_t start = ts_node_start_byte(n);
    uint32_t end = ts_node_end_byte(n);
    if (start != end || end > (uint32_t)source_len) {
        return false;
    }
    if (end == (uint32_t)source_len) {
        return true;
    }
    if (!source || strcmp(ts_node_type(n), "\n") != 0) {
        return false;
    }
    for (uint32_t i = end; i < (uint32_t)source_len; i++) {
        if (!cbm_is_blank_not_newline(source[i])) {
            return false;
        }
    }
    return true;
}

/* Walks to the end even after the cap is full, so `dropped` is the real number
 * of ranges lost rather than a lower bound. This costs little: the walk never
 * descends into an ERROR subtree — it records the top-most node and moves on —
 * so it only visits the spine of nodes that contain an error, plus one level. */
static void cbm_collect_error_regions(TSNode n, cbm_error_regions_t *acc, const char *source,
                                      int source_len) {
    uint32_t k = ts_node_child_count(n);
    for (uint32_t i = 0; i < k; i++) {
        TSNode c = ts_node_child(n, i);
        if (ts_node_is_missing(c) || strcmp(ts_node_type(c), "ERROR") == 0) {
            if (cbm_is_eof_terminator_miss(c, source, source_len)) {
                continue; /* absent final newline only — nothing was dropped */
            }
            cbm_error_regions_push(acc, c); /* top-most region; do not descend */
        } else if (ts_node_has_error(c)) {
            cbm_collect_error_regions(c, acc, source, source_len);
        }
    }
}

/* ── Phase 2 line map: what the preprocessed parse already explained ───────
 *
 * The raw parse is preprocessor-blind. When an #ifdef splits a brace it sees
 * both branches at once, the braces do not balance, and the ERROR node
 * swallows the whole construct — at file scope it swallows the whole FILE.
 * The second parse, on preprocessed source, does not have that problem: the
 * preprocessor already picked one branch, so that parse is clean.
 *
 * So we build one byte per ORIGINAL line and use it to cut the raw ranges
 * down to the lines the second parse cannot vouch for. Lines in the branch
 * the preprocessor threw away never appear in the second parse at all, so
 * they stay flagged — which is right, because they really are missing from
 * the graph.
 *
 * CBM_LINE_PP_PARSED — the preprocessed parse covered this original line and
 *                      found no error on it. Nothing here was dropped.
 * CBM_LINE_NO_CODE   — the line is empty, is only a comment, or is a
 *                      preprocessor directive. A reported range must never
 *                      begin or end on one.
 *
 *                      Directives are in this set because the preprocessor
 *                      CONSUMES them: no directive line ever survives into
 *                      the expanded text, so the second parse can never
 *                      vouch for one, and treating that silence as a miss
 *                      would flag every #include block in the file. The
 *                      known cost is a #define that the raw parse really did
 *                      drop: it no longer shows up on its own. That trade is
 *                      deliberate — it removes far more noise than signal. */
enum { CBM_LINE_PP_PARSED = 1u, CBM_LINE_NO_CODE = 2u };

/* True when the line's first non-blank character starts a preprocessor
 * directive. */
static bool cbm_is_directive_line(const char *line, int len) {
    int i = 0;
    while (i < len && (line[i] == ' ' || line[i] == '\t')) {
        i++;
    }
    return i < len && line[i] == '#';
}

/* True when the line ends with a backslash, so the directive carries on to
 * the next line. */
static bool cbm_line_continues(const char *line, int len) {
    int end = len;
    while (end > 0 && (line[end - 1] == ' ' || line[end - 1] == '\t' || line[end - 1] == '\r')) {
        end--;
    }
    return end > 0 && line[end - 1] == '\\';
}

/* Set CBM_LINE_NO_CODE on every line of `src` that holds no construct.
 * One pass over the file. Carries block-comment state across lines so a line
 * in the middle of a comment counts as no-code too. */
static void cbm_mark_no_code_lines(const char *src, int src_len, uint8_t *map,
                                   uint32_t line_count) {
    bool in_block = false;
    bool in_directive = false;
    uint32_t line = 1;
    int i = 0;
    while (i <= src_len && line <= line_count) {
        int end = i;
        while (end < src_len && src[end] != '\n') {
            end++;
        }
        bool has_code = false;
        bool line_starts_in_block = in_block;
        for (int j = i; j < end; j++) {
            if (in_block) {
                if (src[j] == '*' && j + 1 < end && src[j + 1] == '/') {
                    in_block = false;
                    j++;
                }
                continue;
            }
            if (src[j] == '/' && j + 1 < end && src[j + 1] == '*') {
                in_block = true;
                j++;
                continue;
            }
            if (src[j] == '/' && j + 1 < end && src[j + 1] == '/') {
                break; /* rest of the line is a comment */
            }
            if (src[j] != ' ' && src[j] != '\t' && src[j] != '\r') {
                has_code = true;
            }
        }
        bool directive =
            !line_starts_in_block && (in_directive || cbm_is_directive_line(src + i, end - i));
        if (!has_code || directive) {
            map[line] |= CBM_LINE_NO_CODE;
        }
        in_directive = directive && cbm_line_continues(src + i, end - i);
        line++;
        i = end + 1;
    }
}

/* Paint CBM_LINE_PP_PARSED for every original line the preprocessed parse
 * covered without an error on it.
 *
 * Step 1 marks the EXPANDED rows that sit under an ERROR/MISSING node.
 * Step 2 walks the expanded lines and, for each one that is unmarked, belongs
 * to the file itself (not an included header) and maps back to a real
 * original line, records that original line as parsed. */
static void cbm_mark_pp_error_rows(TSNode n, uint8_t *rows, uint32_t row_count, const char *src,
                                   int src_len) {
    uint32_t k = ts_node_child_count(n);
    for (uint32_t i = 0; i < k; i++) {
        TSNode c = ts_node_child(n, i);
        if (ts_node_is_missing(c) || strcmp(ts_node_type(c), "ERROR") == 0) {
            if (cbm_is_eof_terminator_miss(c, src, src_len)) {
                continue; /* absent final newline only — nothing was dropped */
            }
            uint32_t s = ts_node_start_point(c).row + 1;
            uint32_t e = ts_node_end_point(c).row + 1;
            for (uint32_t r = s; r <= e && r <= row_count; r++) {
                rows[r] = 1;
            }
        } else if (ts_node_has_error(c)) {
            cbm_mark_pp_error_rows(c, rows, row_count, src, src_len);
        }
    }
}

/* Recovery subtraction (#963): tree-sitter error recovery plus the
 * ERROR-descending def walker often still extract constructs INSIDE a failed
 * region (verified: a function in an #ifdef-split ERROR region and even a
 * `def broken(:` both came back as defs). The lines a definition that STARTS
 * inside the region covers are therefore in the graph, and only the lines no
 * definition covers can honestly be called missed. Container defs
 * (Module/Package) are ignored: a file-spanning Module node is not evidence
 * the region's constructs survived.
 *
 * This used to be all-or-nothing — a region stayed flagged whole unless every
 * line was covered. On torvalds/linux (2026-09-16 probe) one ERROR node that
 * swallowed the second half of kernel/sched/core.c (lines 5522–11284) survived
 * because of the comment and macro lines between its 286 extracted functions,
 * and the coverage report told a reader 68 % of the scheduler was unindexed.
 * Now the uncovered gaps are reported instead, and a gap holding only blank,
 * comment or preprocessor lines is not a miss at all. */
static uint32_t *cbm_line_offsets(const char *src, int src_len, uint32_t *out_lines) {
    uint32_t lines = 1;
    for (int i = 0; i < src_len; i++) {
        if (src[i] == '\n') {
            lines++;
        }
    }
    uint32_t *offs =
        (uint32_t *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)(lines + 1) * sizeof(uint32_t));
    if (!offs) {
        *out_lines = 0;
        return NULL;
    }
    uint32_t n = 0;
    offs[n++] = 0;
    for (int i = 0; i < src_len && n < lines; i++) {
        if (src[i] == '\n') {
            offs[n++] = (uint32_t)i + 1;
        }
    }
    offs[n] = (uint32_t)src_len;
    *out_lines = n;
    return offs;
}

/* A 1-based line that holds nothing a reader would call missed code: blank,
 * a comment line, or a preprocessor directive (the import pass owns #include;
 * #if/#endif carry no construct of their own). */
static bool cbm_line_is_inert(const char *src, const uint32_t *offs, uint32_t nlines,
                              uint32_t line) {
    if (!src || !offs || line == 0 || line > nlines) {
        return true;
    }
    const char *p = src + offs[line - 1];
    const char *end = src + offs[line];
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\r')) {
        p++;
    }
    if (p >= end || *p == '\n') {
        return true;
    }
    if (p[0] == '/' && p + 1 < end && (p[1] == '/' || p[1] == '*')) {
        return true;
    }
    return p[0] == '*' || p[0] == '#';
}

static void cbm_regions_emit_gap(cbm_error_regions_t *out, uint32_t gs, uint32_t ge,
                                 const char *src, const uint32_t *offs, uint32_t nlines) {
    bool live = false;
    for (uint32_t l = gs; l <= ge && !live; l++) {
        live = !cbm_line_is_inert(src, offs, nlines, l);
    }
    if (!live) {
        return;
    }
    if (out->count < CBM_MAX_ERROR_REGIONS) {
        out->starts[out->count] = gs;
        out->ends[out->count] = ge;
        out->count++;
    } else {
        out->dropped++;
    }
}

/* Replace [rs, re] by the sub-ranges no in-region definition covers. */
static void cbm_region_uncovered_gaps(uint32_t rs, uint32_t re, const CBMDefArray *defs,
                                      const char *src, const uint32_t *offs, uint32_t nlines,
                                      cbm_error_regions_t *out) {
    enum { MAX_COVER_DEFS = 1024 };
    uint32_t starts[MAX_COVER_DEFS];
    uint32_t ends[MAX_COVER_DEFS];
    int n = 0;
    for (int i = 0; i < defs->count && n < MAX_COVER_DEFS; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (!d->label || strcmp(d->label, "Module") == 0 || strcmp(d->label, "Package") == 0) {
            continue;
        }
        if (d->start_line < rs || d->start_line > re) {
            continue; /* recovery evidence must originate inside the region */
        }
        /* A one-line definition salvaged from inside an error region is the
         * walker's guess at broken text (`def x(:` comes back as a def, a
         * mis-parsed `static _Thread_local int x` as a Variable named `int`,
         * and Python's synthetic builtins all sit on line 1); only a construct
         * whose body spans lines proves it was understood. Conservative: real
         * one-line variables and macros inside a failed region keep their line
         * flagged. */
        if (d->end_line <= d->start_line) {
            continue;
        }
        starts[n] = d->start_line;
        ends[n] = d->end_line;
        n++;
    }
    if (n == 0) {
        cbm_regions_emit_gap(out, rs, re, src, offs, nlines);
        return;
    }
    /* Insertion-sort by start, then sweep and emit the gaps in [rs, re]. */
    for (int i = 1; i < n; i++) {
        uint32_t s = starts[i];
        uint32_t e = ends[i];
        int j = i - 1;
        while (j >= 0 && starts[j] > s) {
            starts[j + 1] = starts[j];
            ends[j + 1] = ends[j];
            j--;
        }
        starts[j + 1] = s;
        ends[j + 1] = e;
    }
    uint32_t cursor = rs;
    for (int i = 0; i < n; i++) {
        if (starts[i] > cursor) {
            cbm_regions_emit_gap(out, cursor, starts[i] - 1, src, offs, nlines);
        }
        if (ends[i] + 1 > cursor) {
            cursor = ends[i] + 1;
        }
    }
    if (cursor <= re) {
        cbm_regions_emit_gap(out, cursor, re, src, offs, nlines);
    }
}

/* #961: true when 1-based `line` of `src` contains `name` (used to verify a
 * def recovered from EXPANDED source really lives on that ORIGINAL line —
 * rejects header-inlined defs whose physical expanded lines alias unrelated
 * raw lines when compile_commands include paths are present). */
static bool cbm_line_contains(const char *src, int src_len, uint32_t line, const char *name) {
    if (!src || !name || !name[0] || line == 0) {
        return false;
    }
    uint32_t cur = 1;
    int i = 0;
    while (i < src_len && cur < line) {
        if (src[i] == '\n') {
            cur++;
        }
        i++;
    }
    if (cur != line) {
        return false;
    }
    int end = i;
    while (end < src_len && src[end] != '\n') {
        end++;
    }
    size_t nlen = strlen(name);
    for (int j = i; j + (int)nlen <= end; j++) {
        if (strncmp(src + j, name, nlen) == 0) {
            return true;
        }
    }
    return false;
}

static bool cbm_identifier_char(char c) {
    return isalnum((unsigned char)c) || c == '_';
}

/* Verify that the mapped original span contains callable-definition syntax,
 * not merely the name at a macro invocation or call site. */
static bool cbm_span_contains_callable_def(const char *src, int src_len, uint32_t start_line,
                                           uint32_t end_line, const char *name) {
    if (!src || src_len <= 0 || !name || !name[0] || start_line == 0 || end_line < start_line) {
        return false;
    }
    int span_start = 0;
    uint32_t line = 1;
    while (span_start < src_len && line < start_line) {
        if (src[span_start++] == '\n') {
            line++;
        }
    }
    if (line != start_line) {
        return false;
    }
    int span_end = span_start;
    while (span_end < src_len && line <= end_line) {
        if (src[span_end++] == '\n') {
            line++;
        }
    }
    size_t name_len = strlen(name);
    for (int pos = span_start; pos + (int)name_len <= span_end; pos++) {
        if (strncmp(src + pos, name, name_len) != 0 ||
            (pos > 0 && cbm_identifier_char(src[pos - 1])) ||
            (pos + (int)name_len < src_len && cbm_identifier_char(src[pos + name_len]))) {
            continue;
        }
        int before = pos;
        while (before > span_start && isspace((unsigned char)src[before - 1])) {
            before--;
        }
        if (before > span_start && strchr("(,=!?[.", src[before - 1])) {
            continue;
        }
        int open = pos + (int)name_len;
        while (open < span_end && isspace((unsigned char)src[open])) {
            open++;
        }
        if (open >= span_end || src[open] != '(') {
            continue;
        }
        int depth = 0;
        int close = -1;
        for (int i = open; i < span_end; i++) {
            if (src[i] == '(') {
                depth++;
            } else if (src[i] == ')' && --depth == 0) {
                close = i;
                break;
            }
        }
        if (close < 0) {
            continue;
        }
        for (int i = close + 1; i < span_end; i++) {
            if (src[i] == '{') {
                return true;
            }
            if (src[i] == ';') {
                break;
            }
        }
    }
    return false;
}

/* #1989: type-def counterpart of cbm_span_contains_callable_def. A rescued
 * class/struct/enum definition has its name followed by a base clause (':') or
 * a body ('{'), not a parameter list, so the callable validator would reject
 * every type def pulled from the expanded tree. */
static bool cbm_span_contains_type_def(const char *src, int src_len, uint32_t start_line,
                                       uint32_t end_line, const char *name) {
    if (!src || src_len <= 0 || !name || !name[0] || start_line == 0 || end_line < start_line) {
        return false;
    }
    int span_start = 0;
    uint32_t line = 1;
    while (span_start < src_len && line < start_line) {
        if (src[span_start++] == '\n') {
            line++;
        }
    }
    if (line != start_line) {
        return false;
    }
    int span_end = span_start;
    while (span_end < src_len && line <= end_line) {
        if (src[span_end++] == '\n') {
            line++;
        }
    }
    size_t name_len = strlen(name);
    for (int pos = span_start; pos + (int)name_len <= span_end; pos++) {
        if (strncmp(src + pos, name, name_len) != 0 ||
            (pos > 0 && cbm_identifier_char(src[pos - 1])) ||
            (pos + (int)name_len < src_len && cbm_identifier_char(src[pos + name_len]))) {
            continue;
        }
        int open = pos + (int)name_len;
        while (open < span_end && isspace((unsigned char)src[open])) {
            open++;
        }
        // Base clause (": public B"), body ("{"), or template args ("<...").
        if (open < span_end && (src[open] == ':' || src[open] == '{' || src[open] == '<')) {
            return true;
        }
    }
    return false;
}

/* #1989: label buckets for the misparse-correction gates. A raw def extracted
 * from a broken `class MOD_API Foo` shape lands under a callable label while
 * the expanded tree yields a proper type label for the same name. */
static bool cbm_def_label_is_type(const char *label) {
    return label != NULL && (strcmp(label, "Class") == 0 || strcmp(label, "Enum") == 0 ||
                             strcmp(label, "Interface") == 0 || strcmp(label, "Type") == 0 ||
                             strcmp(label, "Struct") == 0 || strcmp(label, "Union") == 0);
}

static bool cbm_def_label_is_callable(const char *label) {
    return label != NULL && (strcmp(label, "Function") == 0 || strcmp(label, "Method") == 0);
}

/* Remap an expanded-source definition back to the original input file. Every
 * line in the definition must be attributable to the main file; generated
 * macro bodies, included headers, and ambiguous spans fail closed. */
static bool cbm_remap_preprocessed_def(CBMDefinition *def, const CBMPreprocessedSource *pp) {
    if (!def || !pp || !pp->original_line_by_expanded_line || !pp->belongs_to_main_file ||
        def->start_line == 0 || def->end_line < def->start_line ||
        def->end_line > (uint32_t)pp->expanded_line_count) {
        return false;
    }

    uint32_t original_start = pp->original_line_by_expanded_line[def->start_line];
    uint32_t original_end = pp->original_line_by_expanded_line[def->end_line];
    if (!original_start || !original_end || original_end < original_start) {
        return false;
    }
    for (uint32_t line = def->start_line; line <= def->end_line; line++) {
        if (!pp->belongs_to_main_file[line] || !pp->original_line_by_expanded_line[line]) {
            return false;
        }
    }

    def->start_line = original_start;
    def->end_line = original_end;
    def->lines = (int)(original_end - original_start + 1);
    return true;
}

static void cbm_subtract_recovered_regions(cbm_error_regions_t *regs, const CBMDefArray *defs,
                                           const char *src, int src_len) {
    if (regs->count <= 0) {
        return;
    }
    uint32_t nlines = 0;
    uint32_t *offs = cbm_line_offsets(src, src_len, &nlines);
    cbm_error_regions_t out = {{0}, {0}, 0, regs->dropped};
    for (int i = 0; i < regs->count; i++) {
        cbm_region_uncovered_gaps(regs->starts[i], regs->ends[i], defs, src, offs, nlines, &out);
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, offs);
    *regs = out;
}

/* #1071: a function-like macro invocation whose argument is a type token
 * (e.g. ALLOC(int, n)) makes tree-sitter's C/C++ grammar emit an ERROR node — it
 * parses `int` in expression position — which would be recorded as a parse_partial
 * coverage gap. But the macro is #defined in THIS file, so nothing is actually
 * missing from the graph; it's a benign call the grammar can't parse without the
 * preprocessor. True if the [start_line, end_line] span contains a call `NAME(` to
 * a file-defined function-like macro (Macro label + a parameter signature). */
static bool cbm_byte_span_is_macro_invocation(const char *src, int src_len, int span_start,
                                              int span_end, const CBMDefArray *defs) {
    if (!src || src_len <= 0 || !defs || span_start < 0 || span_end > src_len ||
        span_start >= span_end) {
        return false;
    }
    for (int di = 0; di < defs->count; di++) {
        const CBMDefinition *d = &defs->items[di];
        /* Function-like macros only: an object-like macro (#define PI 3.14) has no
         * parameter signature and can't be mistaken for a call. */
        if (!d->label || strcmp(d->label, "Macro") != 0 || !d->signature || !d->name ||
            !d->name[0]) {
            continue;
        }
        int nlen = (int)strlen(d->name);
        for (int pos = span_start; pos + nlen <= span_end; pos++) {
            if (strncmp(src + pos, d->name, (size_t)nlen) != 0 ||
                (pos > 0 && cbm_identifier_char(src[pos - 1])) ||
                (pos + nlen < src_len && cbm_identifier_char(src[pos + nlen]))) {
                continue;
            }
            int open = pos + nlen;
            while (open < span_end && isspace((unsigned char)src[open])) {
                open++;
            }
            if (open < span_end && src[open] == '(') {
                return true; /* NAME( ... ) — an invocation of this file's macro */
            }
        }
    }
    return false;
}

/* Byte offset where every 1-based line starts, so finding a line's span costs
 * one table read instead of a walk from the start of the file.
 *
 * The table holds line_count + 2 entries. Entry [L] is where line L starts, and
 * the last entry is the end of the source, which gives the final line somewhere
 * to stop. A line the file never reaches starts at the end of the source, so its
 * span is empty and nothing can match inside it — the same answer the walk gives.
 *
 * Returns NULL when the allocation fails; a caller then falls back to the walk. */
static int *cbm_build_line_offsets(const char *src, int src_len, uint32_t line_count) {
    int *offsets = (int *)malloc(((size_t)line_count + 2) * sizeof(int));
    if (!offsets) {
        return NULL;
    }
    for (uint32_t l = 0; l <= line_count + 1; l++) {
        offsets[l] = src_len;
    }
    offsets[0] = 0;
    offsets[1] = 0;
    uint32_t line = 1;
    for (int i = 0; i < src_len; i++) {
        if (src[i] != '\n') {
            continue;
        }
        line++;
        if (line > line_count + 1) {
            break;
        }
        offsets[line] = i + 1;
    }
    return offsets;
}

/* Same question by line number, for the few callers that ask about one region
 * rather than every line of a file. This form walks the source to find the span,
 * which is why the per-line caller below uses a table instead. */
static bool cbm_span_is_macro_invocation(const char *src, int src_len, uint32_t start_line,
                                         uint32_t end_line, const CBMDefArray *defs) {
    if (!src || src_len <= 0 || !defs || start_line == 0 || end_line < start_line) {
        return false;
    }
    int span_start = 0;
    uint32_t line = 1;
    while (span_start < src_len && line < start_line) {
        if (src[span_start++] == '\n') {
            line++;
        }
    }
    if (line != start_line) {
        return false;
    }
    int span_end = span_start;
    while (span_end < src_len && line <= end_line) {
        if (src[span_end++] == '\n') {
            line++;
        }
    }
    return cbm_byte_span_is_macro_invocation(src, src_len, span_start, span_end, defs);
}

/* True if [rs, re] is fully enclosed by an extracted callable definition (a
 * Function/Method body). A macro invocation INSIDE a real function body is an
 * expression-level use where nothing is missing (#1071). A TOP-LEVEL invocation
 * is different: the macro may itself expand to a definition that the original
 * span doesn't contain (#949), which must stay flagged. Restricting the #1071
 * suppression to in-body calls keeps that #949 gap honest and fails safe. */
static bool cbm_region_inside_callable(uint32_t rs, uint32_t re, const CBMDefArray *defs) {
    for (int i = 0; i < defs->count; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (!d->label) {
            continue;
        }
        if (strcmp(d->label, "Function") != 0 && strcmp(d->label, "Method") != 0 &&
            strcmp(d->label, "Constructor") != 0 && strcmp(d->label, "Destructor") != 0) {
            continue;
        }
        if (d->start_line <= rs && d->end_line >= re && d->end_line > d->start_line) {
            return true;
        }
    }
    return false;
}

static void cbm_subtract_macro_invocation_regions(cbm_error_regions_t *regs,
                                                  const CBMDefArray *defs, const char *src,
                                                  int src_len) {
    int kept = 0;
    for (int i = 0; i < regs->count; i++) {
        bool benign =
            cbm_span_is_macro_invocation(src, src_len, regs->starts[i], regs->ends[i], defs) &&
            cbm_region_inside_callable(regs->starts[i], regs->ends[i], defs);
        if (!benign) {
            regs->starts[kept] = regs->starts[i];
            regs->ends[kept] = regs->ends[i];
            kept++;
        }
    }
    regs->count = kept;
}

/* Push [start, end] after trimming no-code lines off both ends. A run made
 * only of directives, comments or blank lines disappears entirely — there was
 * never a construct on it to lose. */
static void cbm_push_trimmed_run(cbm_error_regions_t *out, uint32_t start, uint32_t end,
                                 const uint8_t *map, uint32_t line_count) {
    while (start <= end && start <= line_count && (map[start] & CBM_LINE_NO_CODE)) {
        start++;
    }
    while (end >= start && end <= line_count && (map[end] & CBM_LINE_NO_CODE)) {
        end--;
    }
    if (start > end) {
        return; /* nothing but blank, comment or directive lines — no construct lost */
    }
    if (out->count >= CBM_MAX_ERROR_REGIONS) {
        out->dropped++;
        return;
    }
    out->starts[out->count] = start;
    out->ends[out->count] = end;
    out->count++;
}

/* #949: a top-level macro invocation is the one place where a clean second
 * parse proves nothing. The macro can expand to a whole definition, and the
 * recovery walker deliberately refuses to adopt that definition because it is
 * absent from the original span. So the expanded line parses fine while the
 * construct really is missing from the graph, and the line must stay flagged.
 * An invocation INSIDE a function body is the benign #1071 case and is left
 * alone here — cbm_subtract_macro_invocation_regions handles it later. */
static bool cbm_line_is_toplevel_macro_call(const char *src, int src_len, uint32_t line,
                                            const int *line_offsets, const CBMDefArray *defs) {
    bool is_call = line_offsets
                       ? cbm_byte_span_is_macro_invocation(src, src_len, line_offsets[line],
                                                           line_offsets[line + 1], defs)
                       : cbm_span_is_macro_invocation(src, src_len, line, line, defs);
    return is_call && !cbm_region_inside_callable(line, line, defs);
}

/* Cut every raw region down to the lines the preprocessed parse could not
 * vouch for. Each region becomes zero or more smaller ranges: one per run of
 * consecutive lines that the second parse did not cover cleanly.
 *
 * This is what collapses a whole-file range on a file whose only real problem
 * is an #ifdef splitting a brace. It deliberately does NOT clear the region
 * outright — the branch the preprocessor discarded is genuinely absent from
 * the graph and must stay flagged. */
static void cbm_refine_regions_with_pp_lines(cbm_error_regions_t *regs, const uint8_t *map,
                                             uint32_t line_count, const char *src, int src_len,
                                             const CBMDefArray *defs) {
    cbm_error_regions_t out = {{0}, {0}, 0, regs->dropped};
    /* One offset table for the whole file. The macro check below runs once per
     * line, and without the table each of those calls walks the source from byte
     * 0 to find its line — bytes times lines, on exactly the whole-file-error
     * shape this refinement exists to narrow. */
    int *line_offsets = cbm_build_line_offsets(src, src_len, line_count);
    for (int i = 0; i < regs->count; i++) {
        uint32_t run_start = 0;
        uint32_t run_end = 0;
        uint32_t end = regs->ends[i] < line_count ? regs->ends[i] : line_count;
        for (uint32_t line = regs->starts[i]; line <= end; line++) {
            if ((map[line] & CBM_LINE_PP_PARSED) &&
                !cbm_line_is_toplevel_macro_call(src, src_len, line, line_offsets, defs)) {
                if (run_start != 0) {
                    cbm_push_trimmed_run(&out, run_start, run_end, map, line_count);
                    run_start = 0;
                }
            } else {
                if (run_start == 0) {
                    run_start = line;
                }
                run_end = line;
            }
        }
        if (run_start != 0) {
            cbm_push_trimmed_run(&out, run_start, run_end, map, line_count);
        }
    }
    free(line_offsets);
    *regs = out;
}

/* Serialize collected regions as "start-end,start-end,..." into the arena. */
/* Share of a file one range must cover before the range stops being advice and
 * becomes noise. 80% is well clear of anything real: the widest single range in
 * this repo covers 25.5% of its file, and the next widest 3.9%. */
#define CBM_UNUSABLE_PCT 80

/* Number of 1-based lines in `src`. A file that does not end with a newline
 * still has a last line, so the count is separators plus one. */
static uint32_t cbm_count_lines(const char *src, int src_len) {
    uint32_t n = 1;
    for (int i = 0; i < src_len; i++) {
        if (src[i] == '\n' && i + 1 < src_len) {
            n++;
        }
    }
    return n;
}

/* Serialize collected regions as "start-end,start-end,...", with a trailing
 * ",+<N>" when the cap threw N ranges away.
 *
 * The marker must stay a SUFFIX and nothing else. Every reader stops at the
 * first token that is not a range, so a marker in the middle of a string
 * silently hides everything after it. objectscript_export_append_error_ranges
 * strips markers before joining two parts for exactly that reason.
 *
 * N can be non-zero while the kept list is short, because the recovery and
 * macro rules run after collection and remove ranges the cap never saw. That
 * still reports honestly: the cap bound, so what was lost is unknown. */
static const char *cbm_error_ranges_str(CBMArena *a, const cbm_error_regions_t *regs) {
    if (regs->count <= 0 && regs->dropped <= 0) {
        return NULL;
    }
    enum { RANGE_MAX = 24 }; /* "4294967295-4294967295," */
    char *buf = (char *)cbm_arena_alloc(a, (size_t)(regs->count + 1) * RANGE_MAX);
    if (!buf) {
        return NULL;
    }
    size_t off = 0;
    for (int i = 0; i < regs->count; i++) {
        off += (size_t)snprintf(buf + off, RANGE_MAX, "%s%u-%u", i ? "," : "", regs->starts[i],
                                regs->ends[i]);
    }
    if (regs->dropped > 0) {
        snprintf(buf + off, RANGE_MAX, "%s+%d", off ? "," : "", regs->dropped);
    }
    return buf;
}

/* Public entry: run the extraction and journal completion. The DONE mark on
 * every ordinary return (including error/timeout results) tells the crash
 * supervisor this file did NOT kill the worker — only a file whose S has no
 * D is a crash/hang suspect. */
CBMFileResult *cbm_extract_file(const char *source, int source_len, CBMLanguage language,
                                const char *project, const char *rel_path, int64_t timeout_micros,
                                const char **extra_defines, const char **include_paths) {
    CBMFileResult *r =
        cbm_extract_file_ex(source, source_len, language, project, rel_path, timeout_micros,
                            extra_defines, include_paths, NULL, NULL);
    return r;
}

/* Initial block for the per-file traversal scratch arena, chosen by measuring
 * arena_grow on a 14k-file TypeScript tree: it fires on one file in 12,000 at
 * both this size and at 1 MB, and on most files at 256 KB, where the two
 * channel walks alone are exactly 262144 bytes. 512 KB therefore buys the same
 * growth behaviour as 1 MB for half the resident block per worker. It is also
 * exactly MI_LARGE_MAX_OBJ_SIZE in the vendored mimalloc
 * (vendored/mimalloc/include/mimalloc/types.h:426, MI_LARGE_PAGE_SIZE/8 with
 * MI_ENABLE_LARGE_PAGES defaulting to 1 at :115 and not overridden here), so
 * the block is still bin-allocated from a large page. Growth is not free at
 * this size for the same reason: arena_grow doubles to 1 MiB, which is above
 * that bound and so a singleton OS allocation. One file in twelve thousand
 * pays it, which is why the cost is accepted. */
enum { CBM_EXTRACT_SCRATCH_BLOCK = CBM_SZ_512 * CBM_SZ_1K };
enum { CBM_EXTRACT_SCRATCH_KEEP_BYTES = 4 * CBM_SZ_1K * CBM_SZ_1K };

static CBMFileResult *extract_file_ex_body(const char *source, int source_len, CBMLanguage language,
                                           const char *project, const char *rel_path,
                                           int64_t timeout_micros, const char **extra_defines,
                                           const char **include_paths,
                                           const CBMMacroTable *macro_table,
                                           const CBMReturnTypeTable *return_type_table,
                                           CBMArena *scratch) {
    // Allocate result on heap (arena inside for all string data)
    CBMFileResult *result = cbm_result_alloc();
    if (!result) {
        return NULL;
    }

    cbm_work_arena_take(&result->arena);
    CBMArena *a = &result->arena;

    /* Crash-quarantine hard guard (Stage 3c): a file the supervisor pinned as a
     * crasher must NEVER be parsed again. Return a clean empty result BEFORE the
     * marker write and fault injector so no pass (including sequential re-extract
     * passes that miss the result cache) can crash on it. The pipeline extract
     * loops separately record it as a phase="crash" skip. Checked before the
     * marker so quarantined files never overwrite it — the marker keeps pointing
     * at the real (non-quarantined) file being processed when a crash hits. */
    if (cbm_index_is_quarantined(rel_path)) {
        return result;
    }

    cbm_index_mark_start(rel_path);
#ifdef CBM_ENABLE_TEST_SEAMS
    cbm_test_fault_inject(rel_path);
#endif

    // Get language spec
    const CBMLangSpec *spec = cbm_lang_spec(language);
    if (!spec) {
        result->has_error = true;
        result->error_msg = cbm_arena_strdup(a, "unsupported language");
        cbm_index_mark_done(rel_path);
        return result;
    }

    // Get tree-sitter language
    const TSLanguage *ts_lang = cbm_ts_language(language);
    if (!ts_lang) {
        result->has_error = true;
        result->error_msg = cbm_arena_strdup(a, "no tree-sitter grammar");
        cbm_index_mark_done(rel_path);
        return result;
    }

    // Skip pathologically nested Perl before tree-sitter's recursive GLR stack
    // merge overflows a small stack during the parse (see
    // cbm_source_nesting_exceeds). Scoped to Perl: its ambiguous call grammar is
    // the only one that drives that recursion to the nesting depth.
    if (language == CBM_LANG_PERL &&
        cbm_source_nesting_exceeds(source, source_len, CBM_PERL_MAX_PARSE_NESTING)) {
        result->has_error = true;
        result->error_msg = cbm_arena_strdup(a, "perl source nesting too deep; skipped");
        return result;
    }

    // Get thread-local parser (reused across files on same thread)
    TSParser *parser = get_thread_parser(ts_lang, language);
    if (!parser) {
        result->has_error = true;
        result->error_msg = cbm_arena_strdup(a, "parser alloc failed");
        cbm_index_mark_done(rel_path);
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
    CBMParseBudget budget = {0}; // cppcheck-suppress unreadVariable
    uint64_t budget_ns = 0;
    if (timeout_micros > 0) {
        budget_ns = (uint64_t)timeout_micros * USEC_TO_NSEC;
        // Descheduling burns wall time but not CPU: gate on this thread's CPU
        // time so a starved-but-parseable file is not abandoned, with a generous
        // wall ceiling as a backstop against a genuinely spinning/stuck parse.
        budget.cpu_deadline_ns = cbm_thread_cpu_time_ns() + budget_ns;
        budget.wall_ceiling_ns = t0 + budget_ns * CBM_PARSE_WALL_CEILING_FACTOR;
        opts.payload = &budget;
        opts.progress_callback = cbm_timeout_cb;
#ifdef CBM_ENABLE_TEST_SEAMS
        tl_parse_wall_seam_offset_ns = 0;
        const char *stall_on = getenv("CBM_TEST_WALL_STALL_ON");
        if (stall_on && stall_on[0] && rel_path && strstr(rel_path, stall_on)) {
            // Push the wall reading past the 1x budget (the old wall-only budget
            // trips) but well under the generous ceiling (the CPU-time budget
            // survives): budget + 1 s, deterministic, no real timing involved.
            tl_parse_wall_seam_offset_ns = budget_ns + NSEC_PER_SEC;
        }
#endif
    }

    TSTree *tree = ts_parser_parse_with_options(parser, NULL, ts_input, opts);
    uint64_t t1 = now_ns();
#ifdef CBM_ENABLE_TEST_SEAMS
    t1 += tl_parse_wall_seam_offset_ns; /* the stall seam inflates every wall reading */
#endif

    if (!tree) {
        result->has_error = true;
        /* Only the budget's own callback may call this a timeout. Any other
         * NULL is a parse failure, and saying so is the difference between a
         * reader chasing the clock and a reader chasing the real cause. */
        result->error_msg = cbm_arena_strdup(a, budget.tripped ? "parse timeout" : "parse failed");
        cbm_index_mark_done(rel_path);
        return result;
    }

    TSNode root = ts_tree_root_node(tree);

    /* No file is disqualified from the LSP walks by how long its parse took.
     *
     * The rule used to be "a parse that spent more than half its budget means
     * this tree is too heavy for the walks" — a clock deciding which files get
     * type-aware refinement, so the same repo could come back with different
     * graphs. It was added on 2026-09-14 against a 23 MB single-expression C#
     * JIT test that cost 354 s in the per-file walk and then crashed the
     * cross-file resolve.
     *
     * Removing it was checked against exactly that file and that path
     * (2026-09-19): the whole 12k-file C# corpus twice, cross-file resolve
     * included, and hugeexpr1.cs on its own — no crash, same 1,224,981 nodes /
     * 5,794,873 edges as with the rule in place, same ~100 s. Synthetic C# with
     * valid nesting 10,000 levels deep indexes fine; past roughly that depth
     * tree-sitter itself stops producing a usable tree, so a tree deep enough
     * to endanger a recursive walk never reaches one. The 354 s is gone for a
     * different reason: the walk that spent it was C#'s own, climbing with
     * ts_node_parent, and it now uses the cursor (extract_usages.c).
     *
     * CBM_TEST_LSP_SKIP_ON still names a file for the tests that pin the
     * skipped-file behaviour itself. */
#ifdef CBM_ENABLE_TEST_SEAMS
    {
        const char *skip_on = getenv("CBM_TEST_LSP_SKIP_ON");
        if (skip_on && skip_on[0] && rel_path && strstr(rel_path, skip_on)) {
            result->lsp_skipped = true; /* the test names the file; no timing involved */
            cbm_log_warn("extract.lsp.skipped", "reason", "test_seam", "path",
                         rel_path ? rel_path : "");
        }
    }
#endif

    // Compute module QN. Java/Go derive the module from the CONTAINING
    // DIRECTORY (package semantics) rather than baking the filename stem in,
    // so def QNs, the LSP caller_qn, and the textual calls-enclosing QN all
    // agree (e.g. Outer.java -> module "proj", not "proj.Outer"). Other
    // languages are unchanged.
    result->module_qn = cbm_fqn_module_source_lang(a, project, rel_path, language);
    result->is_test_file = cbm_is_test_file(rel_path, language);

    // Build extraction context
    CBMExtractCtx ctx = {
        .arena = a,
        .scratch = scratch,
        .result = result,
        .source = source,
        .source_len = source_len,
        .language = language,
        .project = project,
        .rel_path = rel_path,
        .module_qn = result->module_qn,
        .root = root,
        .macro_table = macro_table,
        .return_type_table = return_type_table,
        .walk_budget_nodes = cbm_walk_max_nodes(),
    };

    // Run extractors: defs + imports use separate walks (unique recursion patterns),
    // then a single unified cursor walk handles the remaining 7 extractors.
    cbm_extract_definitions(&ctx);
    cbm_extract_imports(&ctx);
    cbm_extract_unified(&ctx);
    result->tree_nodes = ts_node_descendant_count(root);
    result->walk_nodes_visited = ctx.walk_nodes_visited;
    if (ctx.walk_budget_exhausted) {
        result->walk_truncated = true;
        result->lsp_skipped = true;
        cbm_log_warn("extract.walk.truncated", "reason", "node_budget", "path",
                     rel_path ? rel_path : "");
    }

    // Channel detection (Socket.IO / EventEmitter) — JS/TS only.
    cbm_extract_channels(&ctx);

    // K8s / Kustomize semantic pass (additional structured extraction for YAML-based infra files).
    if (ctx.language == CBM_LANG_KUSTOMIZE || ctx.language == CBM_LANG_K8S) {
        cbm_extract_k8s(&ctx);
    }

    // dbt lineage pass: a dbt model's dependencies live in Jinja ({{ ref(...) }}),
    // which the SQL grammar cannot read. Self-gated — SQL files only, and only
    // those carrying a real dbt builtin call.
    if (ctx.language == CBM_LANG_SQL) {
        cbm_extract_dbt(&ctx);
    }

    // LSP type-aware call/usage resolution (per-file). Runs in every mode;
    // refines the tree-sitter + textual-resolution graph with type info.
    uint64_t lsp_start = now_ns();
    if (!result->lsp_skipped) {
        if (language == CBM_LANG_GO) {
            cbm_run_go_lsp(a, result, source, source_len, root);
        }
        if (language == CBM_LANG_C || language == CBM_LANG_CPP || language == CBM_LANG_CUDA) {
            cbm_run_c_lsp(a, result, source, source_len, root, language != CBM_LANG_C,
                          CBM_SOURCE_ORIGIN_RAW);
        }
        if (language == CBM_LANG_PHP) {
            cbm_run_php_lsp(a, result, source, source_len, root);
        }
        if (language == CBM_LANG_PERL) {
            cbm_run_perl_lsp(a, result, source, source_len, root);
        }
        if (language == CBM_LANG_PYTHON) {
            cbm_run_py_lsp(a, result, source, source_len, root);
        }
        if (language == CBM_LANG_JAVASCRIPT || language == CBM_LANG_TYPESCRIPT ||
            language == CBM_LANG_TSX) {
            bool js_mode = (language == CBM_LANG_JAVASCRIPT);
            // jsx_mode: TSX always; .jsx in the JS bucket also enables it.
            bool jsx_mode = (language == CBM_LANG_TSX);
            if (language == CBM_LANG_JAVASCRIPT && rel_path) {
                size_t rl = strlen(rel_path);
                if (rl >= 4 && strcmp(rel_path + rl - 4, ".jsx") == 0)
                    jsx_mode = true;
            }
            // dts_mode: ".d.ts" suffix (TypeScript only).
            bool dts_mode = false;
            if (language == CBM_LANG_TYPESCRIPT && rel_path) {
                size_t rl = strlen(rel_path);
                if (rl >= 5 && strcmp(rel_path + rl - 5, ".d.ts") == 0)
                    dts_mode = true;
            }
            cbm_run_ts_lsp(a, result, source, source_len, root, js_mode, jsx_mode, dts_mode);
        }
        if (language == CBM_LANG_CSHARP) {
            cbm_run_cs_lsp(a, result, source, source_len, root);
        }
        if (language == CBM_LANG_JAVA) {
            cbm_run_java_lsp(a, result, source, source_len, root);
        }
        if (language == CBM_LANG_KOTLIN) {
            cbm_run_kotlin_lsp(a, result, source, source_len, root);
        }
        if (language == CBM_LANG_RUST) {
            cbm_run_rust_lsp(a, result, source, source_len, root);
        }
    }
    atomic_fetch_add(&total_lsp_ns, now_ns() - lsp_start);

    // Calls extracted so far all carry ORIGINAL-source line numbers; the C/C++
    // preprocessor second pass below appends calls with EXPANDED-source lines,
    // which must not be used for the def line-range attribution of the bottleneck
    // metrics. Remember the boundary.
    int orig_calls_count = result->calls.count;

    /* Phase 2 line map, built by the second (preprocessed) pass below and read
     * by the parse-coverage block near the end of this function. Stays NULL
     * for every language that has no second pass, which leaves the coverage
     * signal exactly as it was. Arena-allocated so it outlives the
     * preprocessed source and its tree. */
    uint8_t *pp_line_map = NULL;
    uint32_t pp_line_map_lines = 0;

    // Second pass: preprocess C/C++/CUDA and extract additional macro-hidden calls.
    // Defs keep original-source line numbers; only CALLS are extracted from expanded source.
    if (language == CBM_LANG_C || language == CBM_LANG_CPP || language == CBM_LANG_CUDA) {
        uint64_t pp_start = now_ns();
        /* #1989: collect build-system export-macro candidates from the raw
         * source. They are predefined empty in the expanded buffer (see
         * preprocessor.cpp) so `class MOD_API Foo` parses with its real name,
         * and the rescue loop below can adopt corrected type defs. Bounded:
         * at most CBM_EXPORT_MACRO_MAX names per file. */
        char export_cands[CBM_EXPORT_MACRO_MAX][CBM_EXPORT_MACRO_NAME_MAX];
        int export_cand_count =
            cbm_export_macro_candidates(source, source_len, export_cands, CBM_EXPORT_MACRO_MAX);
        CBMPreprocessedSource *preprocessed = cbm_preprocess_with_map(
            source, source_len, rel_path, extra_defines, include_paths, language != CBM_LANG_C);
        if (preprocessed && preprocessed->source) {
            char *expanded = preprocessed->source;
            int expanded_len = (int)strlen(expanded);
            // Record every site-bearing array boundary before the second pass.
            // Numeric byte spans in `expanded` are not raw-source coordinates.
            int calls_before = result->calls.count;
            int usages_before = result->usages.count;
            int resolved_before = result->resolved_calls.count;

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
                        .scratch = scratch,
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

                    /* Stamp parser carriers before C-LSP performs any
                     * origin-sensitive rewrite. Numeric spans in the expanded
                     * buffer may collide with unrelated raw-source spans. */
                    for (int i = calls_before; i < result->calls.count; i++) {
                        result->calls.items[i].source_origin = CBM_SOURCE_ORIGIN_PREPROCESSED;
                    }
                    for (int i = usages_before; i < result->usages.count; i++) {
                        result->usages.items[i].source_origin = CBM_SOURCE_ORIGIN_PREPROCESSED;
                    }

                    // Also run LSP on expanded source for additional type-resolved
                    // calls (language is already C/C++/CUDA — checked in enclosing
                    // block). Runs in every mode.
                    cbm_run_c_lsp(a, result, expanded, expanded_len, pp_root,
                                  language != CBM_LANG_C, CBM_SOURCE_ORIGIN_PREPROCESSED);

                    /* All C-LSP emitters stamp origin directly so rewrite-time
                     * comparisons are already safe. Keep this boundary sweep as
                     * a defensive invariant for any future emitter added to the
                     * C resolver. */
                    for (int i = resolved_before; i < result->resolved_calls.count; i++) {
                        result->resolved_calls.items[i].source_origin =
                            CBM_SOURCE_ORIGIN_PREPROCESSED;
                    }

                    /* #961: a def whose body braces are split across
                     * #ifdef/#else branches parses as an ERROR region on the
                     * RAW source (both branches present at once -> unbalanced
                     * braces), so the raw defs walk silently dropped it. The
                     * expanded tree parses clean (simplecpp picked one
                     * branch) and same-file token lines stay aligned, so
                     * recover defs from it — adopting ONLY those that
                     * intersect a raw ERROR region, whose name is visible on
                     * the raw source line, and whose QN the raw pass did not
                     * already extract.
                     *
                     * #1989: build-system export macros (<MOD>_API,
                     * <lib>_EXPORT, ...) are empty on the real compile line but
                     * opaque to tree-sitter, so `class MOD_API Foo` misparses
                     * with the macro as the type name — often with NO raw ERROR
                     * region at all (the class_specifier "succeeds" with the
                     * wrong name; enums and free functions do error out). When
                     * export-macro candidates were collected for this file, also
                     * adopt expanded defs that replace a raw def literally named
                     * as a candidate, or that correct a raw misparse label (the
                     * raw pass extracted the same name as Function/Method from
                     * the broken function_definition shape; the expanded tree
                     * yields a proper type def). Superseded raw defs are
                     * dropped so the corrected definition is not shadowed by
                     * the qualified-name dedup. With no candidates the block
                     * reduces to the original #961 behavior. */
                    if (ts_node_has_error(root) || export_cand_count > 0) {
                        cbm_error_regions_t raw_regs = {{0}, {0}, 0, 0};
                        cbm_collect_error_regions(root, &raw_regs, source, source_len);
                        if (raw_regs.count > 0 || export_cand_count > 0) {
                            int defs_before = result->defs.count;
                            cbm_extract_definitions(&pp_ctx);
                            int w = defs_before;
                            char *superseded = NULL;
                            if (export_cand_count > 0 && defs_before > 0) {
                                superseded =
                                    (char *)cbm_calloc(CBM_MEM_CLASS_EXTRACT, (size_t)defs_before);
                            }
                            /* #1989: spans of defs already adopted from the
                             * expanded tree. A def nested inside one of these
                             * (an inline method of a rescued class) was dropped
                             * by the raw pass — the misparsed
                             * `class MOD_API Foo {...}` parsed as a
                             * function_definition, and nested defs in its body
                             * are never walked. Bounded: one span per adopted
                             * def, capped at 64. */
                            struct {
                                uint32_t start;
                                uint32_t end;
                            } rescued_spans[64];
                            int rescued_count = 0;
                            for (int i = defs_before; i < result->defs.count; i++) {
                                CBMDefinition *d = &result->defs.items[i];
                                bool adopt = false;
                                int supersede_j = -1;
                                if (cbm_remap_preprocessed_def(d, preprocessed)) {
                                    for (int rj = 0; rj < raw_regs.count && !adopt; rj++) {
                                        if (d->start_line <= raw_regs.ends[rj] &&
                                            d->end_line >= raw_regs.starts[rj]) {
                                            adopt = true;
                                        }
                                    }
                                    if (!adopt && export_cand_count > 0 && d->name) {
                                        for (int ci = 0; ci < export_cand_count && !adopt; ci++) {
                                            for (int j = 0; j < defs_before && !adopt; j++) {
                                                CBMDefinition *r = &result->defs.items[j];
                                                if (!r->name ||
                                                    strcmp(r->name, export_cands[ci]) != 0) {
                                                    continue;
                                                }
                                                if (r->start_line <= d->end_line &&
                                                    d->start_line <= r->end_line &&
                                                    cbm_span_contains_type_def(
                                                        source, source_len, d->start_line,
                                                        d->end_line, d->name)) {
                                                    adopt = true;
                                                    supersede_j = j;
                                                }
                                            }
                                        }
                                        if (!adopt && d->label && cbm_def_label_is_type(d->label)) {
                                            for (int j = 0; j < defs_before && !adopt; j++) {
                                                CBMDefinition *r = &result->defs.items[j];
                                                /* Widened past callables (#1989): the
                                                 * misparse also mints value-label raw
                                                 * defs for types (`class X_API FEmpty {}`
                                                 * -> Variable FEmpty). Same name,
                                                 * overlapping span, raw label is NOT a
                                                 * type => raw is the misparse artifact. */
                                                if (!r->name || !r->label || !d->name ||
                                                    strcmp(r->name, d->name) != 0 ||
                                                    cbm_def_label_is_type(r->label)) {
                                                    continue;
                                                }
                                                if (r->start_line <= d->end_line &&
                                                    d->start_line <= r->end_line) {
                                                    adopt = true;
                                                    supersede_j = j;
                                                }
                                            }
                                        }
                                        /* Nested rescue (#1989): a def nested inside
                                         * an already-adopted def's span — the inline
                                         * method of a rescued class, dropped by the
                                         * raw pass because the class parsed as a
                                         * function body. The same validation gates
                                         * (name-on-line, shape, QN dedup) still run
                                         * below before it lands. */
                                        if (!adopt && rescued_count > 0) {
                                            for (int k = 0; k < rescued_count && !adopt; k++) {
                                                if (d->start_line >= rescued_spans[k].start &&
                                                    d->end_line <= rescued_spans[k].end) {
                                                    adopt = true;
                                                }
                                            }
                                        }
                                    }
                                }
                                if (adopt &&
                                    (!d->name ||
                                     !cbm_line_contains(source, source_len, d->start_line,
                                                        d->name) ||
                                     (!cbm_span_contains_callable_def(source, source_len,
                                                                      d->start_line, d->end_line,
                                                                      d->name) &&
                                      !cbm_span_contains_type_def(source, source_len, d->start_line,
                                                                  d->end_line, d->name)))) {
                                    adopt = false;
                                }
                                for (int j = 0; j < defs_before && adopt; j++) {
                                    if (j == supersede_j || (superseded && superseded[j])) {
                                        continue; // being replaced by this very def
                                    }
                                    const char *q = result->defs.items[j].qualified_name;
                                    if (q && d->qualified_name &&
                                        strcmp(q, d->qualified_name) == 0) {
                                        /* #1989: a raw def holding the same QN is
                                         * usually a reject — unless it is the
                                         * misparse artifact this very def came to
                                         * replace (raw "Function FCalc" from the
                                         * broken `class MOD_API FCalc` shape vs the
                                         * rescued "Class FCalc"). Same name, same
                                         * span, raw label is not a type: supersede
                                         * instead of dropping the correction. */
                                        CBMDefinition *r = &result->defs.items[j];
                                        if (d->label && cbm_def_label_is_type(d->label) &&
                                            r->label && r->name && d->name &&
                                            strcmp(r->name, d->name) == 0 &&
                                            !cbm_def_label_is_type(r->label) &&
                                            r->start_line == d->start_line &&
                                            r->end_line == d->end_line && superseded) {
                                            superseded[j] = 1;
                                            continue;
                                        }
                                        adopt = false;
                                    }
                                }
                                if (adopt) {
                                    if (supersede_j >= 0 && supersede_j < defs_before &&
                                        superseded) {
                                        superseded[supersede_j] = 1;
                                    }
                                    /* Phantom-artifact suppression (#1989): on the
                                     * raw tree `class MOD_API Foo : public Base
                                     * {...}` parses as a function_definition whose
                                     * declarator is the BASE-CLASS name, so the raw
                                     * pass mints a bogus Function def named after
                                     * the base spanning the whole class. Fingerprint:
                                     * a raw callable def with an IDENTICAL span to
                                     * the adopted type def whose name is one of the
                                     * adopted def's base classes. A real method is
                                     * a strict subset of the class span and never
                                     * carries a base-class name. */
                                    if (d->label && cbm_def_label_is_type(d->label) && superseded &&
                                        d->base_classes) {
                                        for (int j = 0; j < defs_before; j++) {
                                            CBMDefinition *r = &result->defs.items[j];
                                            if (!r->label || !r->name ||
                                                !cbm_def_label_is_callable(r->label) ||
                                                r->start_line != d->start_line ||
                                                r->end_line != d->end_line) {
                                                continue;
                                            }
                                            for (const char **b = d->base_classes; *b; b++) {
                                                if (strcmp(*b, r->name) == 0) {
                                                    superseded[j] = 1;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    if (rescued_count < 64) {
                                        rescued_spans[rescued_count].start = d->start_line;
                                        rescued_spans[rescued_count].end = d->end_line;
                                        rescued_count++;
                                    }
                                    result->defs.items[w++] = *d;
                                }
                            }
                            if (superseded) {
                                int rw = 0;
                                for (int j = 0; j < defs_before; j++) {
                                    if (!superseded[j]) {
                                        result->defs.items[rw++] = result->defs.items[j];
                                    }
                                }
                                int shift = defs_before - rw;
                                if (shift > 0 && w > defs_before) {
                                    memmove(&result->defs.items[rw],
                                            &result->defs.items[defs_before],
                                            (size_t)(w - defs_before) * sizeof(CBMDefinition));
                                }
                                w -= shift;
                                cbm_free(CBM_MEM_CLASS_EXTRACT, superseded);
                            }
                            result->defs.count = w;
                        }
                    }

                    /* Build the original-line map before the expanded tree
                     * goes away. Skipped when the expanded parse is itself a
                     * total loss (root is ERROR), because then it vouches for
                     * nothing and there is no refinement to make. */
                    if (strcmp(ts_node_type(pp_root), "ERROR") != 0) {
                        uint32_t orig_lines = 1;
                        for (int ci = 0; ci < source_len; ci++) {
                            if (source[ci] == '\n') {
                                orig_lines++;
                            }
                        }
                        uint8_t *map = (uint8_t *)cbm_arena_alloc(a, (size_t)orig_lines + 2);
                        int exp_lines = preprocessed->expanded_line_count;
                        uint8_t *bad_rows =
                            exp_lines > 0 ? (uint8_t *)calloc((size_t)exp_lines + 2, 1) : NULL;
                        if (map && bad_rows) {
                            memset(map, 0, (size_t)orig_lines + 2);
                            cbm_mark_no_code_lines(source, source_len, map, orig_lines);
                            cbm_mark_pp_error_rows(pp_root, bad_rows, (uint32_t)exp_lines, expanded,
                                                   expanded_len);
                            /* Walk the expanded text once. An expanded line
                             * only vouches for its original line when it
                             * actually HAS content: the preprocessor emits a
                             * blank line where it dropped a branch, and a
                             * blank line proves nothing about the code that
                             * used to be there. */
                            uint32_t eline = 1;
                            bool eline_has_text = false;
                            for (int ci = 0; ci <= expanded_len; ci++) {
                                if (ci < expanded_len && expanded[ci] != '\n') {
                                    char ch = expanded[ci];
                                    if (ch != ' ' && ch != '\t' && ch != '\r') {
                                        eline_has_text = true;
                                    }
                                    continue;
                                }
                                if (eline_has_text && (int)eline <= exp_lines && !bad_rows[eline] &&
                                    preprocessed->belongs_to_main_file[eline]) {
                                    uint32_t orig =
                                        preprocessed->original_line_by_expanded_line[eline];
                                    if (orig >= 1 && orig <= orig_lines) {
                                        map[orig] |= CBM_LINE_PP_PARSED;
                                    }
                                }
                                eline++;
                                eline_has_text = false;
                            }
                            pp_line_map = map;
                            pp_line_map_lines = orig_lines;
                        }
                        free(bad_rows);
                    }

                    ts_tree_delete(pp_tree);
                }
            }
            cbm_preprocessed_source_free(preprocessed);
            atomic_fetch_add(&total_files_preprocessed, 1);
            (void)calls_before; // used for future logging
        } else {
            cbm_preprocessed_source_free(preprocessed);
        }
        atomic_fetch_add(&total_preprocess_ns, now_ns() - pp_start);
    }

    // Bottleneck call-context metrics. Each call is attributed to the INNERMOST
    // enclosing Function/Method def by source-line range (defs and calls in one
    // CBMFileResult share the same file). Range matching is used instead of
    // enclosing_func_qn string matching because some grammars (notably C, whose
    // function_definition has no "name" field) attribute the call's scope to the
    // module rather than the function — line ranges are unambiguous and
    // language-agnostic. Bounded per file (defs x calls), not a repo-scale scan.
    int def_count = result->defs.count;
    bool *has_self = def_count > 0 ? calloc((size_t)def_count, sizeof(bool)) : NULL;
    bool *has_guarded = def_count > 0 ? calloc((size_t)def_count, sizeof(bool)) : NULL;

    // param_count is a standalone structural smell (independent of calls). Prefer
    // the parsed param_names array; fall back to counting from the signature text
    // for languages (e.g. C) that populate only the signature.
    for (int di = 0; di < def_count; di++) {
        CBMDefinition *d = &result->defs.items[di];
        int pc = 0;
        if (d->param_names) {
            while (d->param_names[pc]) {
                pc++;
            }
        }
        if (pc == 0 && d->signature) {
            pc = count_params_from_signature(d->signature);
        }
        /* Neither source is available for a language whose def head carries the
         * arity structurally rather than as a named parameter list (Elixir).
         * Its extractor sets param_count directly, and this pass must not
         * silently zero it -- the count feeds the structural-smell metric and,
         * for such a language, is the only record of arity outside the QN. */
        if (pc == 0) {
            pc = d->param_count;
        }
        d->param_count = pc;
    }

    for (int ci = 0; ci < orig_calls_count; ci++) {
        const CBMCall *c = &result->calls.items[ci];
        if (!c->callee_name || c->start_line <= 0) {
            continue;
        }
        // Innermost enclosing Function/Method def by line range (smallest span).
        int best = -1;
        int best_span = -1;
        for (int di = 0; di < def_count; di++) {
            const CBMDefinition *d = &result->defs.items[di];
            if (!d->name || !d->label ||
                (strcmp(d->label, "Function") != 0 && strcmp(d->label, "Method") != 0)) {
                continue;
            }
            if ((int)d->start_line <= c->start_line && c->start_line <= (int)d->end_line) {
                int span = (int)d->end_line - (int)d->start_line;
                if (best < 0 || span < best_span) {
                    best_span = span;
                    best = di;
                }
            }
        }
        if (best < 0) {
            continue;
        }
        CBMDefinition *d = &result->defs.items[best];
        // callee_name may be bare ("recur") or qualified ("self.recur",
        // "super().save", "axios.get"). A short-name match alone is not
        // self-recursion: the callee must also target the same object
        // (is_self_receiver), or super().save() inside save and axios.get
        // inside get are false positives (#599).
        const char *dot = strrchr(c->callee_name, '.');
        const char *callee_short = dot ? dot + 1 : c->callee_name;
        bool in_loop = c->loop_depth > 0;

        if (strcmp(callee_short, d->name) == 0 && is_self_receiver(c->callee_name, d->receiver)) {
            // Direct self-recursion. The call graph omits self-edges (pass_calls
            // skips source==target), so detect it here; seeds "recursive".
            d->is_recursive = true;
            if (has_self) {
                has_self[best] = true;
            }
            if (in_loop) {
                d->recursion_in_loop = true; // recursion compounded by a loop
            }
            if (c->branch_depth > 0 && has_guarded) {
                has_guarded[best] = true; // a self-call guarded by some conditional
            }
        }
        if (in_loop && is_linear_scan_name(callee_short)) {
            d->linear_scan_in_loop++; // hidden O(n^2): linear scan inside a loop
        }
        if (in_loop && is_alloc_name(callee_short)) {
            d->alloc_in_loop++; // repeated allocation/append inside a loop
        }
    }

    // Recursive with no self-call guarded by any conditional → no obvious base
    // case on the recursive path: a stronger "potentially unbounded" signal.
    for (int di = 0; di < def_count; di++) {
        if (has_self && has_self[di] && !(has_guarded && has_guarded[di])) {
            result->defs.items[di].unguarded_recursion = true;
        }
    }
    free(has_self);
    free(has_guarded);

    uint64_t t2 = now_ns();

    /* Best-effort parse-coverage signal (#963): flag files whose tree contains
     * ERROR/MISSING regions. Computed AFTER extraction so definite recovery is
     * subtracted first — a region fully re-extracted as definitions is not a
     * miss, and a fully recovered file is not flagged at all. Detection aid
     * only: the absence of this flag is NOT a completeness guarantee. */
    if (ts_node_has_error(root)) {
        cbm_error_regions_t regs = {{0}, {0}, 0, 0};
        if (strcmp(ts_node_type(root), "ERROR") == 0) {
            cbm_error_regions_push(&regs, root); /* whole file unparseable */
        } else {
            cbm_collect_error_regions(root, &regs, source, source_len);
        }
        /* Recovery subtraction runs on the RAW ranges, before the Phase 2
         * refinement below. Its evidence is a whole definition that starts
         * inside the range, so it has to be asked while the range still
         * matches the construct. Ask it after the refinement and an #ifdef
         * splitting a brace inside a recovered function looks unrecovered:
         * the refinement keeps only the discarded branch, the function starts
         * above it, and the evidence falls outside the range. */
        cbm_subtract_recovered_regions(&regs, &result->defs, source, source_len);
        /* Phase 2: cut what is left down to the lines the preprocessed parse
         * could not explain. */
        if (pp_line_map) {
            cbm_refine_regions_with_pp_lines(&regs, pp_line_map, pp_line_map_lines, source,
                                             source_len, &result->defs);
        }
        /* #1071: don't flag a benign function-like-macro call (defined in-file)
         * that tree-sitter can't parse without the preprocessor. Runs AFTER the
         * refinement, because its evidence is per-line: a narrow range points at
         * the call itself instead of the whole blob around it. */
        cbm_subtract_macro_invocation_regions(&regs, &result->defs, source, source_len);
        /* A file whose kept list is empty but whose cap still bound is NOT clean:
         * the ranges the cap threw away were never judged by the two rules
         * above, so nothing proves they were recovered. Flag it. */
        if (regs.count > 0 || regs.dropped > 0) {
            result->parse_incomplete = true;
            result->error_region_count = regs.count;
            result->error_ranges = cbm_error_ranges_str(a, &regs);
            /* One range covering nearly the whole file is not advice, it is
             * noise: "look at lines 1 to 13047" of a 13046-line file tells a
             * reader nothing they did not already know. Mark those separately
             * so the report can say "read the source" instead. See
             * parse_unusable in cbm.h for which files land here and why. */
            if (regs.count == 1 && regs.dropped == 0) {
                uint32_t total = cbm_count_lines(source, source_len);
                uint32_t span = regs.ends[0] - regs.starts[0] + 1;
                if (total > 0 && span * 100 >= total * CBM_UNUSABLE_PCT) {
                    result->parse_unusable = true;
                }
            }
        }
    }

    result->imports_count = result->imports.count;

    // Accumulate profiling counters
    atomic_fetch_add(&total_parse_ns, t1 - t0);
    atomic_fetch_add(&total_extract_ns, t2 - t1);
    atomic_fetch_add(&total_files, 1);

    // Retain tree for cross-file LSP reuse (caller frees via cbm_free_tree)
    result->cached_tree = tree;
    result->cached_lang = language;
    cbm_index_mark_done(rel_path);
    return result;
}

/* ── Per-worker working arena (see cbm.h) ───────────────────────────── */
static CBM_TLS CBMArena tl_work_arena;
static CBM_TLS bool tl_work_arena_live = false;

/* The traversal scratch arena is kept per worker the same way. A thread keeps
 * one only after it has given a working arena back (so only pipeline workers,
 * which call cbm_work_arena_release when they end), and only while it holds no
 * more than CBM_EXTRACT_SCRATCH_KEEP_BYTES (an outsized file's arena is not
 * worth holding). Before this every file created and destroyed a
 * 512 KB block -- 28,145 of them on the Go corpus, 14 GB allocated, 99.9 %
 * never written and 7,136 never touched (waste sanitizer, 2026-09-17). */
/* The parked arena lives on the HEAP, reached through a thread-local pointer,
 * not inline in thread-local storage: a CBMArena is ~4 KB (256 block pointers
 * and 256 sizes), and static TLS is charged to every thread AND taken out of
 * each thread's stack allocation — enough of it and a small-stack thread stops
 * being creatable at all (PR #2233). The holder is allocated once per thread
 * and reused, so parking still costs no allocation per file. */
static CBM_TLS CBMArena *tl_scratch_slot;
static CBM_TLS bool tl_scratch_live = false;
static CBM_TLS bool tl_scratch_keep = false;

void cbm_work_arena_take(CBMArena *into) {
    if (tl_work_arena_live) {
        *into = tl_work_arena;
        tl_work_arena_live = false;
        cbm_arena_rewind(into);
        return;
    }
    cbm_arena_init(into);
}

void cbm_work_arena_release(void) {
    if (tl_work_arena_live) {
        cbm_arena_destroy(&tl_work_arena);
        tl_work_arena_live = false;
    }
    if (tl_scratch_live && tl_scratch_slot) {
        cbm_arena_destroy(tl_scratch_slot);
        tl_scratch_live = false;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, tl_scratch_slot);
    tl_scratch_slot = NULL;
    cbm_result_compact_release_thread();
    tl_scratch_keep = false;
}

bool cbm_work_arena_keeping(void) {
    return tl_scratch_keep;
}

void cbm_work_arena_keep_begin(void) {
    tl_scratch_keep = true;
}

void cbm_work_arena_give(CBMArena *from) {
    if (!from || from->nblocks == 0) {
        return;
    }
    tl_scratch_keep = true; /* a pipeline worker: its release call will come */
    if (tl_work_arena_live || cbm_arena_capacity(from) > (size_t)CBM_WORK_ARENA_KEEP_BYTES) {
        cbm_arena_destroy(from);
        return;
    }
    tl_work_arena = *from;
    tl_work_arena_live = true;
    memset(from, 0, sizeof(*from));
}

/* Public entry. Owns the traversal scratch arena for the whole of one file's
 * extraction: created here, handed to the body as ctx->scratch, destroyed on
 * the way out. The body has seven early returns, so bracketing it in a wrapper
 * is what keeps that to one create and one destroy. The arena is LAZY: it takes
 * its first block when a traversal stack first needs one, so a file that builds
 * no stack (most non-code files) costs nothing -- opened eagerly, 3.3 GB of
 * 512 KB blocks were never read or written on the Go corpus (waste sanitizer,
 * 2026-09-17). If the block cannot be allocated, the stack's allocation fails
 * and it stops growing, exactly as on any later out-of-memory. */
CBMFileResult *cbm_extract_file_ex(const char *source, int source_len, CBMLanguage language,
                                   const char *project, const char *rel_path,
                                   int64_t timeout_micros, const char **extra_defines,
                                   const char **include_paths, const CBMMacroTable *macro_table,
                                   const CBMReturnTypeTable *return_type_table) {
    CBMArena scratch;
    if (tl_scratch_live && tl_scratch_slot) {
        scratch = *tl_scratch_slot;
        tl_scratch_live = false;
        cbm_arena_rewind(&scratch);
    } else {
        cbm_arena_init_lazy(&scratch, CBM_EXTRACT_SCRATCH_BLOCK);
    }
    CBMFileResult *result = extract_file_ex_body(source, source_len, language, project, rel_path,
                                                 timeout_micros, extra_defines, include_paths,
                                                 macro_table, return_type_table, &scratch);
    /* !tl_scratch_live: a nested extraction (an embedded language inside this
     * file) may already have parked its own; never overwrite it. */
    /* Kept up to CBM_EXTRACT_SCRATCH_KEEP_BYTES, grown blocks included: the
     * definitions walk draws its frames from this arena too, so a file with
     * many definitions grows it past the first block, and dropping every grown
     * arena turned those files into fresh 512 KB blocks for the next file. */
    if (tl_scratch_keep && !tl_scratch_live && scratch.nblocks > 0 &&
        cbm_arena_capacity(&scratch) <= (size_t)CBM_EXTRACT_SCRATCH_KEEP_BYTES) {
        if (!tl_scratch_slot) {
            tl_scratch_slot = cbm_alloc(CBM_MEM_CLASS_OTHER, sizeof(CBMArena));
        }
        if (tl_scratch_slot) {
            *tl_scratch_slot = scratch;
            tl_scratch_live = true;
        } else {
            cbm_arena_destroy(&scratch); /* no holder: keep nothing, stay correct */
        }
    } else {
        cbm_arena_destroy(&scratch);
    }
    return result;
}

CBMFileResult *cbm_result_alloc(void) {
    /* The one raw allocation of a result: cbm_free_result releases it with
     * the matching free. Extraction and the spill loader both come here. */
    enum { SINGLE = 1 };
    return (CBMFileResult *)calloc(SINGLE, sizeof(CBMFileResult));
}

void cbm_result_release_owned(CBMFileResult *result) {
    if (!result) {
        return;
    }
    for (int i = 0; i < result->owned_result_count; i++) {
        cbm_free_result(result->owned_results[i]);
    }
    free(result->owned_results);
    result->owned_results = NULL;
    result->owned_result_count = 0;
}

void cbm_free_result(CBMFileResult *result) {
    if (!result) {
        return;
    }
    if (result->cached_tree) {
        ts_tree_delete(result->cached_tree);
        result->cached_tree = NULL;
    }
    cbm_result_release_owned(result);
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
