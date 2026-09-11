/* Full declaration set for the same CBMArena, and it must precede cbm.h:
 * internal/cbm/arena.h declares a subset and the two share the CBM_ARENA_H
 * guard, so whichever is included first is the one this file sees. */
#include "foundation/arena.h" // cbm_arena_init_sized
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
static void *cbm_sqlite_malloc(int n) {
    void *block = mi_malloc((size_t)n);
    return block;
}
static void cbm_sqlite_free(void *p) {
    mi_free(p);
}
static void *cbm_sqlite_realloc(void *p, int n) {
    if (p) {}
    void *grown = mi_realloc(p, (size_t)n);
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
    return block;
}
static void *cbm_ts_calloc(size_t count, size_t size) {
    void *block = mi_calloc(count, size);
    return block;
}
static void *cbm_ts_realloc(void *p, size_t n) {
    if (p) {}
    void *grown = mi_realloc(p, n);
    return grown;
}
static void cbm_ts_free(void *p) {
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
 * `def broken(:` both came back as defs). A region whose every line is
 * covered by definitions that START inside it is definitely recovered — its
 * constructs ARE in the graph — so flagging it would be a false miss.
 * Container defs (Module/Package) are ignored: a file-spanning Module node is
 * not evidence the region's constructs survived. Conservative: partially
 * covered regions stay flagged. */
static bool cbm_region_is_recovered(uint32_t rs, uint32_t re, const CBMDefArray *defs) {
    enum { MAX_COVER_DEFS = 256 };
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
        starts[n] = d->start_line;
        ends[n] = d->end_line < d->start_line ? d->start_line : d->end_line;
        n++;
    }
    if (n == 0) {
        return false;
    }
    /* Insertion-sort by start, then sweep for gaps in [rs, re]. */
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
    uint32_t covered_to = rs - 1;
    for (int i = 0; i < n; i++) {
        if (starts[i] > covered_to + 1) {
            return false; /* uncovered gap */
        }
        if (ends[i] > covered_to) {
            covered_to = ends[i];
        }
    }
    return covered_to >= re;
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

static void cbm_subtract_recovered_regions(cbm_error_regions_t *regs, const CBMDefArray *defs) {
    int kept = 0;
    for (int i = 0; i < regs->count; i++) {
        if (!cbm_region_is_recovered(regs->starts[i], regs->ends[i], defs)) {
            regs->starts[kept] = regs->starts[i];
            regs->ends[kept] = regs->ends[i];
            kept++;
        }
    }
    regs->count = kept;
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

static CBMFileResult *extract_file_ex_body(const char *source, int source_len, CBMLanguage language,
                                           const char *project, const char *rel_path,
                                           int64_t timeout_micros, const char **extra_defines,
                                           const char **include_paths,
                                           const CBMMacroTable *macro_table,
                                           const CBMReturnTypeTable *return_type_table,
                                           CBMArena *scratch) {
    // Allocate result on heap (arena inside for all string data)
    enum { SINGLE = 1 };
    CBMFileResult *result = (CBMFileResult *)calloc(SINGLE, sizeof(CBMFileResult));
    if (!result) {
        return NULL;
    }

    cbm_arena_init(&result->arena);
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
        cbm_index_mark_done(rel_path);
        return result;
    }

    TSNode root = ts_tree_root_node(tree);

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

    // dbt lineage pass: a dbt model's dependencies live in Jinja ({{ ref(...) }}),
    // which the SQL grammar cannot read. Self-gated — SQL files only, and only
    // those carrying a real dbt builtin call.
    if (ctx.language == CBM_LANG_SQL) {
        cbm_extract_dbt(&ctx);
    }

    // LSP type-aware call/usage resolution (per-file). Runs in every mode;
    // refines the tree-sitter + textual-resolution graph with type info.
    uint64_t lsp_start = now_ns();
    {
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
                     * already extract. */
                    if (ts_node_has_error(root)) {
                        cbm_error_regions_t raw_regs = {{0}, {0}, 0, 0};
                        cbm_collect_error_regions(root, &raw_regs, source, source_len);
                        if (raw_regs.count > 0) {
                            int defs_before = result->defs.count;
                            cbm_extract_definitions(&pp_ctx);
                            int w = defs_before;
                            for (int i = defs_before; i < result->defs.count; i++) {
                                CBMDefinition *d = &result->defs.items[i];
                                bool adopt = false;
                                if (cbm_remap_preprocessed_def(d, preprocessed)) {
                                    for (int rj = 0; rj < raw_regs.count && !adopt; rj++) {
                                        if (d->start_line <= raw_regs.ends[rj] &&
                                            d->end_line >= raw_regs.starts[rj]) {
                                            adopt = true;
                                        }
                                    }
                                }
                                if (adopt && (!d->name ||
                                              !cbm_line_contains(source, source_len, d->start_line,
                                                                 d->name) ||
                                              !cbm_span_contains_callable_def(
                                                  source, source_len, d->start_line, d->end_line,
                                                  d->name))) {
                                    adopt = false;
                                }
                                for (int j = 0; j < defs_before && adopt; j++) {
                                    const char *q = result->defs.items[j].qualified_name;
                                    if (q && d->qualified_name &&
                                        strcmp(q, d->qualified_name) == 0) {
                                        adopt = false;
                                    }
                                }
                                if (adopt) {
                                    result->defs.items[w++] = *d;
                                }
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
        cbm_subtract_recovered_regions(&regs, &result->defs);
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

/* Public entry. Owns the traversal scratch arena for the whole of one file's
 * extraction: created here, handed to the body as ctx->scratch, destroyed on
 * the way out. The body has seven early returns, so bracketing it in a wrapper
 * is what keeps that to one create and one destroy. If the arena cannot be
 * created, the body is handed NULL and the traversal stacks fall back to the
 * result arena, which is what shipped before #1997. */
CBMFileResult *cbm_extract_file_ex(const char *source, int source_len, CBMLanguage language,
                                   const char *project, const char *rel_path,
                                   int64_t timeout_micros, const char **extra_defines,
                                   const char **include_paths, const CBMMacroTable *macro_table,
                                   const CBMReturnTypeTable *return_type_table) {
    CBMArena scratch;
    cbm_arena_init_sized(&scratch, CBM_EXTRACT_SCRATCH_BLOCK);
    CBMFileResult *result = extract_file_ex_body(
        source, source_len, language, project, rel_path, timeout_micros, extra_defines,
        include_paths, macro_table, return_type_table, scratch.nblocks > 0 ? &scratch : NULL);
    cbm_arena_destroy(&scratch);
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
    for (int i = 0; i < result->owned_result_count; i++) {
        cbm_free_result(result->owned_results[i]);
    }
    free(result->owned_results);
    result->owned_results = NULL;
    result->owned_result_count = 0;
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
