
/*
 * registry.c — Function/Method/Class registry for call resolution.
 *
 * Indexes all callable symbols by qualified name and simple name.
 * Resolution uses a prioritized strategy chain:
 *   1. Import map lookup
 *   2. Same-module match
 *   3. Unique name (single candidate project-wide)
 *   4. Suffix match with import distance scoring
 */
#include "foundation/constants.h"

enum { REG_INIT_CAP = 16, REG_MIN_CANDIDATES = 3, REG_RESOLVED = 1, REG_SUFFIX_ALLOC = 2 };
/* Names with more registered definitions than this are unresolvable by name
 * alone: candidate_count_penalty already floors their confidence to ~3/count
 * (<= 0.006 at 256), so the emitted edge is noise — while the candidate walk
 * (reachability + scoring per candidate, re-done per file) is the dominant
 * resolution cost on identifier-dense repos. On the Linux kernel, 274 names
 * exceed 256 candidates ("list_head" 7188, "flags" 5520, "dev" 4374, ...) and
 * accounted for ~900 s of the 987 s usage-resolution CPU. Bail out early. */
enum { REG_MAX_CANDIDATES = 256 };
#define REG_FULL_CONF 1.0
#define REG_HALF_PENALTY 0.5

#define DEFAULT_CONFIDENCE 0.5
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "cbm.h"               /* cbm_label_is_relation — the resolve-time relation veto */
#include "foundation/compat.h" /* CBM_TLS */
#include "foundation/hash_table.h"
#include "foundation/dyn_array.h"
#include "foundation/platform.h"
#include "callable_sig.h" /* cbm_qn_callable_base_len_named: overloads share the name key */

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Confidence score → human-readable band label. */
#define CONF_BAND_HIGH 0.7
#define CONF_BAND_MEDIUM 0.45
#define CONF_BAND_SPECULATIVE 0.25

const char *cbm_confidence_band(double score) {
    if (score >= CONF_BAND_HIGH) {
        return "high";
    }
    if (score >= CONF_BAND_MEDIUM) {
        return "medium";
    }
    if (score >= CONF_BAND_SPECULATIVE) {
        return "speculative";
    }
    return "";
}

/* ── Resolution confidence scores ────────────────────────────────── */
/* Strategy 1: import_map — direct import → high confidence */
#define CONF_IMPORT_MAP 0.95
#define CONF_IMPORT_MAP_SUFFIX 0.85
/* Strategy 2: same_module — same file/package → high confidence */
#define CONF_SAME_MODULE 0.90
/* Strategy 3: unique_name — only one candidate project-wide */
#define CONF_UNIQUE_NAME 0.75
/* Strategy 4: suffix_match — multiple candidates, filtered */
#define CONF_SUFFIX_MATCH 0.55
/* Fuzzy fallback: lower confidence */
#define CONF_FUZZY_SINGLE 0.40
#define CONF_FUZZY_MULTI 0.30
/* Candidate count penalty cap */
#define CANDIDATE_PENALTY_CAP 3.0

/* ── Internal types ──────────────────────────────────────────────── */

/* Array of QN strings for byName index, with the test/mock verdict of each
 * name cached beside it: the scorer asked is_test_qn for every candidate of
 * every unresolved call, and the answer depends only on the name, which never
 * changes once registered (waste sanitizer scaling lane, 2026-09-17). Field
 * order and names match CBM_DYN_ARRAY(char *) so cbm_da_push works on it. */
typedef struct {
    char **items;
    int count;
    int cap;
    uint8_t *is_test; /* is_test_qn(items[i]), one byte per entry */
    int is_test_cap;
    /* Language of the file that defined items[i] (a CBMLanguage; CBM_LANG_COUNT
     * = unknown, compatible with every caller), one byte per entry. Grown in
     * lockstep with is_test, so is_test_cap bounds it too. */
    uint8_t *lang;
} qn_array_t;

_Static_assert(CBM_LANG_COUNT <= UINT8_MAX, "CBMLanguage must fit the per-entry lang byte");

struct cbm_registry {
    /* Interned label strings (<=~30 distinct labels; owned here, freed in
     * _free). The exact map's VALUES point into this pool instead of one
     * strdup per registered definition (~8.5M strdups on the kernel). */
    char *label_pool[64];
    int label_pool_n;
    /* exact: qualifiedName → label string (heap-owned copies) */
    CBMHashTable *exact;

    /* byName: simpleName → qn_array_t* (heap-owned) */
    CBMHashTable *by_name;
};

/* ── Helpers ─────────────────────────────────────────────────────── */

/* Extract the last path segment from a QN. Returns pointer into s.
 * Recognizes both '.' (most langs) and Rust/C++ '::' separators, so a
 * scoped callee like "lib::square" yields "square" rather than the whole
 * scoped path (which never matches the by-name index). */
static const char *simple_name(const char *qn) {
    const char *dot = strrchr(qn, '.');
    const char *seg = dot ? dot + SKIP_ONE : qn;
    /* Find the last "::" and, if it sits after the last '.', use the
     * segment following it. */
    const char *colons = NULL;
    for (const char *p = qn; (p = strstr(p, "::")) != NULL; p += 2) {
        colons = p;
    }
    if (colons && colons + 2 > seg) {
        seg = colons + 2;
    }
    return seg;
}

/* Extract everything before the last dot. Returns heap-allocated string. */

/* Count common dot-separated prefix segments. One pass over the shared prefix:
 * the same answer as measuring each segment of both strings with strchr and
 * strlen and comparing them, which walked every candidate's whole name per
 * call (the scaling lane's x3.4, waste sanitizer 2026-09-17). */
static int common_prefix_len(const char *a, const char *b) {
    if (!a || !b) {
        return 0;
    }
    int count = 0;
    size_t i = 0;
    while (a[i] != '\0' && b[i] != '\0') { /* a segment starts here in both */
        while (a[i] == b[i] && a[i] != '.' && a[i] != '\0') {
            i++;
        }
        bool a_ends = a[i] == '.' || a[i] == '\0';
        bool b_ends = b[i] == '.' || b[i] == '\0';
        if (!a_ends || !b_ends) {
            break; /* the segments differ in content or in length */
        }
        count++;
        if (a[i] == '\0' || b[i] == '\0') {
            break; /* one name stops here: no further segment to compare */
        }
        i++; /* both stand on '.': step into the next segment */
    }
    return count;
}

enum { REG_TEST_PENALTY = 1000 };

/* Check if a qualified name looks like a test/mock path: does it contain any of
 * Test, test, Mock, mock, Stub, stub, Fake, fake, Fixture, spec? One pass that
 * dispatches on the first character instead of ten strstr scans of the whole
 * name (this ran for every candidate of every unresolved call: the scaling
 * lane's x4.0, waste sanitizer 2026-09-17). */
static bool is_test_qn(const char *qn) {
    if (!qn) {
        return false;
    }
    for (const char *p = qn; *p; p++) {
        switch (*p) {
        case 'T':
            if (strncmp(p, "Test", 4) == 0) {
                return true;
            }
            break;
        case 't':
            if (strncmp(p, "test", 4) == 0) {
                return true;
            }
            break;
        case 'M':
            if (strncmp(p, "Mock", 4) == 0) {
                return true;
            }
            break;
        case 'm':
            if (strncmp(p, "mock", 4) == 0) {
                return true;
            }
            break;
        case 'S':
            if (strncmp(p, "Stub", 4) == 0) {
                return true;
            }
            break;
        case 's':
            if (strncmp(p, "stub", 4) == 0 || strncmp(p, "spec", 4) == 0) {
                return true;
            }
            break;
        case 'F':
            if (strncmp(p, "Fake", 4) == 0 || strncmp(p, "Fixture", 7) == 0) {
                return true;
            }
            break;
        case 'f':
            if (strncmp(p, "fake", 4) == 0) {
                return true;
            }
            break;
        default:
            break;
        }
    }
    return false;
}

/* The cached test verdicts, only when they cover every entry: a failed growth
 * leaves the cache short, and then the scorer asks per candidate as before. */
static const uint8_t *qn_test_flags(const qn_array_t *arr) {
    return arr->is_test && arr->is_test_cap >= arr->count ? arr->is_test : NULL;
}

/* The cached candidate languages, under the same coverage rule: both side
 * arrays grow together and share is_test_cap. */
static const uint8_t *qn_lang_flags(const qn_array_t *arr) {
    return arr->lang && arr->is_test_cap >= arr->count ? arr->lang : NULL;
}

/* Score a candidate for tiebreaking. Higher = better.
 * Layer 1: Non-test code preferred over test code (+1000)
 * Layer 2: Namespace proximity via common prefix length (+plen)
 * `is_test` is the cached verdict when the caller has one (-1: ask). */
static int candidate_score(const char *candidate_qn, const char *module_qn, int is_test) {
    int score = 0;
    bool test = is_test >= 0 ? is_test != 0 : is_test_qn(candidate_qn);
    if (!test) {
        score += REG_TEST_PENALTY;
    }
    score += common_prefix_len(candidate_qn, module_qn);
    return score;
}

/* How plainly a candidate is something a call can target, lower = better: a
 * function or method; then any other definition (a class for a constructor
 * call, a macro, a type); then a candidate whose label is unknown; last a
 * variable or field, which a call reaches only when it holds a function. */
enum {
    REG_RANK_CALLABLE = 0,
    REG_RANK_OTHER_DEF = 1,
    REG_RANK_UNKNOWN = 2,
    REG_RANK_DATA = 3,
};

static int callable_rank(const cbm_registry_t *r, const char *qn) {
    const char *label = r ? cbm_registry_label_of(r, qn) : NULL;
    if (!label) {
        return REG_RANK_UNKNOWN;
    }
    if (strcmp(label, "Function") == 0 || strcmp(label, "Method") == 0) {
        return REG_RANK_CALLABLE;
    }
    if (strcmp(label, "Variable") == 0 || strcmp(label, "Field") == 0) {
        return REG_RANK_DATA;
    }
    return REG_RANK_OTHER_DEF;
}

/* A .NET reference assembly (`src/libraries/<Asm>/ref/<Asm>.cs`) declares an
 * assembly's public surface with `throw null` bodies in one shallow file; a
 * call that could equally mean the implementation means the implementation.
 * C# only: elsewhere a `ref` directory is an ordinary name. */
static bool csharp_reference_stub(const char *qn, uint8_t lang) {
    return lang == CBM_LANG_CSHARP && strstr(qn, ".ref.") != NULL;
}

/* Total order among candidates that tie on candidate_score. The registry's
 * by-name bucket is in registration order, which follows the file list; a
 * tie broken by bucket position made the resolved target a function of
 * that order (kernel: 632 CALLS edges differed between two indexes of the
 * same tree, `dev_name` flipping between twenty same-named struct fields
 * and unresolved, `sg_set_buf` between include/linux and tools/virtio).
 * The rule, a pure function of the candidate set (O9):
 *   1. the more plainly callable candidate (`dev_name` the function over the
 *      struct fields; Kotlin `where(...)` the method over the property);
 *   2. for C#, an implementation over a reference-assembly stub;
 *   3. the lexicographically smaller QN (include/linux over tools/virtio).
 * Nesting depth is deliberately not a criterion: "least nested wins" sent
 * django's `Model.objects.filter` to the template `Library.filter`, one
 * segment shallower than `QuerySet.filter` (2026-10-10 audit). */
static bool candidate_outranks_on_tie(const cbm_registry_t *r, const char *candidate,
                                      uint8_t candidate_lang, const char *best, uint8_t best_lang) {
    int ck = callable_rank(r, candidate);
    int bk = callable_rank(r, best);
    if (ck != bk) {
        return ck < bk;
    }
    bool cs = csharp_reference_stub(candidate, candidate_lang);
    bool bs = csharp_reference_stub(best, best_lang);
    if (cs != bs) {
        return !cs;
    }
    return strcmp(candidate, best) < 0;
}

/* Pick candidate with highest composite score (test-deprioritization + namespace
 * proximity), ties settled by candidate_outranks_on_tie. `is_test_flags` and
 * `lang_flags` are the candidates' cached test verdicts and languages, or NULL
 * when the caller has none (a filtered subset carries its own copies). */
static const char *best_by_import_distance(const cbm_registry_t *r, const char **candidates,
                                           const uint8_t *is_test_flags, const uint8_t *lang_flags,
                                           int count, const char *module_qn) {
    const char *best = NULL;
    uint8_t best_lang = (uint8_t)CBM_LANG_COUNT;
    int best_score = CBM_NOT_FOUND;
    for (int i = 0; i < count; i++) {
        int score =
            candidate_score(candidates[i], module_qn, is_test_flags ? (int)is_test_flags[i] : -1);
        uint8_t lang = lang_flags ? lang_flags[i] : (uint8_t)CBM_LANG_COUNT;
        if (score > best_score ||
            (score == best_score && best &&
             candidate_outranks_on_tie(r, candidates[i], lang, best, best_lang))) {
            best_score = score;
            best = candidates[i];
            best_lang = lang;
        }
    }
    return best;
}

/* ── Per-file is_import_reachable memoization cache ───────────────
 *
 * The hot path on kubernetes spends ~50% of resolve_calls CPU in
 * is_import_reachable's O(candidates × imports × strstr) scan. The
 * SAME candidate_qn is re-evaluated dozens of times per file because
 * the same callee_name appears in multiple call sites and each lookup
 * re-checks all candidates for that name.
 *
 * Cache life-cycle is per-FILE (because import_vals changes between
 * files). resolve_file_calls calls _begin at file entry and _end at
 * file exit. Thread-local so each worker has its own cache without
 * contention. */
static CBM_TLS CBMHashTable *_reach_cache = NULL;
/* The cache's key copies live in one per-file arena, released with the cache:
 * a strdup per memoized candidate was 12.5 M malloc + free pairs on the Go
 * corpus (waste sanitizer, 2026-09-17). Keys are still copies -- the public
 * cbm_registry_is_import_reachable may be handed a caller's buffer. */
static CBM_TLS CBMArena _reach_keys;
static CBM_TLS bool _reach_keys_live = false;
/* The arena opens on the first memoized key, with a small first block that
 * doubles: opened eagerly at the 64 KB default, the per-file arenas were 1.4 GB
 * of cumulative allocation on the Go corpus, almost all of it never written
 * (waste sanitizer, 2026-09-17). */
enum { REACH_KEYS_FIRST_BLOCK = 4096 };

/* Sentinels stored as values in the cache. NULL means "not cached".
 * We need two distinct non-NULL pointers to encode true/false. */
#define REACH_CACHE_TRUE ((void *)(uintptr_t)1)
#define REACH_CACHE_FALSE ((void *)(uintptr_t)2)

void cbm_registry_reach_cache_begin(int estimated_capacity) {
    if (_reach_cache) {
        /* Defensive: caller forgot to call _end. Clear and reuse. */
        cbm_ht_clear(_reach_cache);
        if (_reach_keys_live) {
            cbm_arena_reset(&_reach_keys);
        }
        return;
    }
    if (estimated_capacity < 16)
        estimated_capacity = 16;
    _reach_cache = cbm_ht_create((uint32_t)estimated_capacity);
}

void cbm_registry_reach_cache_end(void) {
    if (!_reach_cache)
        return;
    cbm_ht_free(_reach_cache);
    _reach_cache = NULL;
    if (_reach_keys_live) {
        cbm_arena_destroy(&_reach_keys);
        _reach_keys_live = false;
    }
}

/* ── Per-file import-map prefix → module-QN hash ──────────────────
 *
 * resolve_import_map does a linear strcmp scan over the per-file
 * import list for every call (and every usage, throw, rw, def). On
 * kubernetes typical files have 20-30 imports and ~200 calls. That's
 * 4000-6000 strcmps per file × 20K files = ~80M strcmps.
 *
 * Build a hash from prefix → module_qn ONCE per file at file entry.
 * Each lookup becomes O(1). Keys and values are BORROWED from the
 * caller's import_map — they outlive the cache scope. */
static CBM_TLS CBMHashTable *_import_map_cache = NULL;

void cbm_registry_import_map_cache_begin(const char **keys, const char **vals, int count) {
    if (_import_map_cache) {
        cbm_ht_free(_import_map_cache);
        _import_map_cache = NULL;
    }
    if (!keys || !vals || count <= 0)
        return;
    _import_map_cache = cbm_ht_create((uint32_t)count * 2u + 8u);
    if (!_import_map_cache)
        return;
    for (int i = 0; i < count; i++) {
        if (keys[i] && vals[i]) {
            cbm_ht_set(_import_map_cache, keys[i], (void *)(uintptr_t)vals[i]);
        }
    }
}

void cbm_registry_import_map_cache_end(void) {
    if (!_import_map_cache)
        return;
    /* Keys/values borrowed from caller — no free callback needed. */
    cbm_ht_free(_import_map_cache);
    _import_map_cache = NULL;
}

/* ── Per-file full-result cache for cbm_registry_resolve ──────────
 *
 * THE big one: 98.7% of resolve_calls CPU on kubernetes lives inside
 * cbm_registry_resolve's strategy chain (suffix_match iterating
 * hundreds of candidates per common name like "Get"/"Add"/"New"
 * × 1M LSP-miss calls = ~800s of work).
 *
 * Since module_qn is CONSTANT per file, the resolution for a given
 * callee_name within a file is also constant — same name resolves
 * to the same QN every time. Cache the full cbm_resolution_t result
 * keyed by callee_name; first lookup does the full chain, repeats
 * are O(1). On K8s a typical file has ~200 calls but only ~50
 * unique callee_names → ~75% hit rate → ~75% of the resolve cost
 * eliminated. */
typedef struct {
    cbm_resolution_t res;
} resolve_cache_entry_t;

static CBM_TLS CBMHashTable *_resolve_cache = NULL;
/* Entries are malloc'd; keys are strdup'd. Both freed in _end. */

static void resolve_cache_free_entry(const char *key, void *val, void *ud) {
    (void)ud;
    free((char *)key);
    free(val);
}

void cbm_registry_resolve_cache_begin(int estimated_capacity) {
    if (_resolve_cache) {
        cbm_ht_foreach(_resolve_cache, resolve_cache_free_entry, NULL);
        cbm_ht_free(_resolve_cache);
        _resolve_cache = NULL;
    }
    if (estimated_capacity < 32)
        estimated_capacity = 32;
    _resolve_cache = cbm_ht_create((uint32_t)estimated_capacity);
}

void cbm_registry_resolve_cache_end(void) {
    if (!_resolve_cache)
        return;
    cbm_ht_foreach(_resolve_cache, resolve_cache_free_entry, NULL);
    cbm_ht_free(_resolve_cache);
    _resolve_cache = NULL;
}

/* Check if candidate's module prefix appears in import map values.
 * Uses stack buffer to avoid malloc/free per call in hot resolution loop.
 * Per-file memoization via TLS cache: repeated lookups of the same
 * candidate_qn (same name appears in many call sites) become O(1)
 * after the first computation. */
static bool is_import_reachable(const char *candidate_qn, const char **import_vals,
                                int import_count) {
    if (_reach_cache) {
        void *cached = cbm_ht_get(_reach_cache, candidate_qn);
        if (cached == REACH_CACHE_TRUE)
            return true;
        if (cached == REACH_CACHE_FALSE)
            return false;
    }

    char cand_mod[CBM_SZ_512];
    const char *last = strrchr(candidate_qn, '.');
    if (last) {
        size_t len = (size_t)(last - candidate_qn);
        if (len >= sizeof(cand_mod)) {
            len = sizeof(cand_mod) - SKIP_ONE;
        }
        memcpy(cand_mod, candidate_qn, len);
        cand_mod[len] = '\0';
    } else {
        snprintf(cand_mod, sizeof(cand_mod), "%s", candidate_qn);
    }
    bool reachable = false;
    for (int i = 0; i < import_count; i++) {
        if (strstr(cand_mod, import_vals[i]) || strstr(import_vals[i], cand_mod)) {
            reachable = true;
            break;
        }
    }

    if (_reach_cache) {
        if (!_reach_keys_live) {
            cbm_arena_init_sized(&_reach_keys, REACH_KEYS_FIRST_BLOCK);
            _reach_keys_live = true;
        }
        char *kdup = cbm_arena_strdup(&_reach_keys, candidate_qn);
        if (kdup) {
            cbm_ht_set(_reach_cache, kdup, reachable ? REACH_CACHE_TRUE : REACH_CACHE_FALSE);
        }
    }
    return reachable;
}

/* Scale confidence inversely with candidate count. */
static double candidate_count_penalty(double base, int count) {
    if (count <= REG_MIN_CANDIDATES) {
        return base;
    }
    return base * fmin(REG_FULL_CONF, CANDIDATE_PENALTY_CAP / (double)count);
}

static cbm_resolution_t empty_result(void) {
    cbm_resolution_t r = {0};
    return r;
}

/* ── Perl builtin guard (#459 follow-up: call-graph noise) ──────────
 * Curated subset of perlfunc core builtins. When a Perl CALL resolves
 * only by the generic short-name matcher (no LSP, no import, after the
 * same-module/name-lookup chain), a builtin name like `push`/`shift`/
 * `keys` must NOT be wired to a project sub that merely shares the name
 * — that is virtually always a false positive. A genuine intra-project
 * call is resolved by earlier (LSP/textual) stages before this guard.
 * MUST stay sorted ASCII-ascending for bsearch. */
static const char *const PERL_BUILTINS[] = {
    "abs",       "atan2",   "binmode", "bless",     "caller",   "chdir",    "chmod",   "chomp",
    "chop",      "chown",   "chr",     "chroot",    "close",    "closedir", "cos",     "defined",
    "delete",    "die",     "do",      "each",      "eof",      "eval",     "exec",    "exists",
    "exit",      "fork",    "gmtime",  "goto",      "grep",     "hex",      "index",   "int",
    "join",      "keys",    "last",    "lc",        "lcfirst",  "length",   "local",   "localtime",
    "log",       "lstat",   "map",     "mkdir",     "my",       "next",     "oct",     "open",
    "opendir",   "ord",     "our",     "pop",       "pos",      "print",    "printf",  "push",
    "quotemeta", "rand",    "read",    "readdir",   "readline", "redo",     "ref",     "rename",
    "require",   "return",  "reverse", "rindex",    "rmdir",    "say",      "scalar",  "seek",
    "shift",     "sin",     "sleep",   "sort",      "splice",   "split",    "sprintf", "sqrt",
    "srand",     "stat",    "substr",  "system",    "time",     "uc",       "ucfirst", "undef",
    "unlink",    "unshift", "values",  "wantarray", "warn",     "write",
};

static int perl_builtin_cmp(const void *key, const void *elem) {
    return strcmp((const char *)key, *(const char *const *)elem);
}

/* True if `name` is one of the curated Perl core builtins. Used to suppress
 * generic-resolver CALLS edges from Perl builtin invocations to project subs
 * that happen to share the builtin's name. Perl-scoped: callers gate on the
 * file language so no other language's resolution is affected. */
bool cbm_perl_is_builtin(const char *name) {
    if (!name || !name[0]) {
        return false;
    }
    return bsearch(name, PERL_BUILTINS, sizeof(PERL_BUILTINS) / sizeof(PERL_BUILTINS[0]),
                   sizeof(PERL_BUILTINS[0]), perl_builtin_cmp) != NULL;
}

/* Decide whether a *resolved* Perl call edge is generic-resolver noise that
 * should be suppressed (#476). Returns true only for Perl, only for a builtin
 * invocation or a method call, and only when the registry landed the match via
 * a WEAK short-name strategy. High-confidence import/same-module strategies
 * (same_module, import_map, import_map_suffix) are KEPT so a genuine same-file
 * or imported call to a builtin-named sub still resolves — only the weak
 * short-name guesses (suffix_match, unique_name) are dropped. `strategy` is the
 * cbm_resolution_t.strategy of a non-empty match;
 * NULL/empty (no match) returns false. Pure + side-effect-free so the
 * suppression contract is unit-testable without a full pipeline. */
bool cbm_perl_suppress_generic_match(bool is_perl, bool is_method, const char *callee_name,
                                     const char *strategy) {
    if (!is_perl) {
        return false;
    }
    if (!(is_method || cbm_perl_is_builtin(callee_name))) {
        return false;
    }
    if (!strategy || !strategy[0]) {
        return false;
    }
    if (strcmp(strategy, "same_module") == 0 || strcmp(strategy, "import_map") == 0 ||
        strcmp(strategy, "import_map_suffix") == 0) {
        return false; /* high-confidence import/same-module match — keep the genuine edge */
    }
    return true; /* weak short-name match (suffix_match / unique_name / …) → drop */
}

/* Dynamic-language analogue of the Perl guard above (#592/#606/#1276
 * direction; precedent #477). A member call `x.foo()` reaches the weak textual
 * cascade ONLY when the language's LSP could not resolve the receiver type —
 * type-resolved calls win via lsp_* strategies before the registry runs.
 * Binding such a call to a project symbol by a weak short-name strategy
 * fabricates a CALLS edge (`re.test()` -> SalesforceRestClient.test,
 * `accelerator.print()` -> MockAccelerator.print). Drop ONLY the weak
 * strategies; keep import/same-module/qualified-tail matches and every lsp_*
 * strategy. Uses an EXPLICIT drop-list (not keep-list + default-drop) because
 * the parallel resolver runs lsp_* strategies through the same guard variable —
 * a default-drop would silently kill lsp_ts_method. Pure + side-effect-free so
 * the contract is unit-testable without a full pipeline.
 *
 * `enabled` is the CALLER's per-language gate, deliberately kept OUT of this
 * helper: the guard applies only to the language set each call site enumerates
 * (today Python plus the JS/TS family including ArkTS). Widening it is a
 * per-language decision made at the call sites in pass_calls.c and
 * pass_parallel.c, which MUST stay in lockstep — a gate added to only one of
 * them diverges the sequential and parallel resolvers. */
/* The weak short-name strategies that actually reach the call-resolution
 * guards: the registry's suffix_match / unique_name and the parallel
 * field_type_hint. "fuzzy" is listed as defensive insurance only —
 * cbm_registry_fuzzy_resolve is not wired into the sequential/parallel resolvers
 * today, so it never reaches these helpers, but naming it keeps a future wiring
 * from silently reintroducing the noise. Everything else — same_module /
 * import_map / import_map_suffix / qualified_suffix / callee_suffix /
 * service_pattern / lsp_* — is a receiver- or import-aware match and is KEPT.
 *
 * Shared by BOTH weak-call guards below so the drop-list exists exactly once: a
 * list that drifted between the member guard and the local-binding guard would
 * make the two disagree about what "weak" means. */
static bool weak_short_name_strategy(const char *strategy) {
    if (!strategy || !strategy[0]) {
        return false;
    }
    return strcmp(strategy, "suffix_match") == 0 || strcmp(strategy, "unique_name") == 0 ||
           strcmp(strategy, "field_type_hint") == 0 || strcmp(strategy, "fuzzy") == 0;
}

bool cbm_suppress_weak_member_match(bool enabled, bool is_method, const char *strategy) {
    if (!enabled || !is_method) {
        return false;
    }
    return weak_short_name_strategy(strategy);
}

/* ── Python builtin-type members ─────────────────────────────────────────
 * Methods of Python's builtin types (str, bytes, list, dict, set, file objects)
 * plus the builtin functions that appear as attribute calls. Like PERL_BUILTINS
 * this is a fact about the LANGUAGE, not about corpus fashion: a member call
 * whose name is a builtin's own method — `parts.extend(...)`, `line.strip()`,
 * `d.items()` — is that builtin's method whenever the receiver is untyped, so a
 * project symbol that merely shares the spelling must not bind. MUST stay
 * sorted ASCII-ascending for bsearch. */
static const char *const PYTHON_BUILTIN_MEMBERS[] = {
    "add",        "append",       "capitalize",
    "casefold",   "center",       "clear",
    "close",      "copy",         "count",
    "decode",     "difference",   "discard",
    "encode",     "endswith",     "expandtabs",
    "extend",     "fileno",       "find",
    "flush",      "format",       "get",
    "index",      "insert",       "intersection",
    "isalnum",    "isalpha",      "isdecimal",
    "isdigit",    "isidentifier", "islower",
    "isnumeric",  "isprintable",  "isspace",
    "istitle",    "isupper",      "issubset",
    "issuperset", "items",        "join",
    "keys",       "ljust",        "lower",
    "lstrip",     "partition",    "pop",
    "popitem",    "print",        "read",
    "readable",   "readline",     "readlines",
    "remove",     "replace",      "reverse",
    "rfind",      "rindex",       "rjust",
    "rpartition", "rsplit",       "rstrip",
    "seek",       "setdefault",   "sort",
    "split",      "splitlines",   "startswith",
    "strip",      "swapcase",     "symmetric_difference",
    "tell",       "title",        "truncate",
    "union",      "update",       "upper",
    "values",     "writable",     "write",
    "writelines", "zfill",
};

static int python_builtin_member_cmp(const void *key, const void *elem) {
    return strcmp((const char *)key, *(const char *const *)elem);
}

bool cbm_python_is_builtin_member(const char *name) {
    if (!name || !name[0]) {
        return false;
    }
    return bsearch(name, PYTHON_BUILTIN_MEMBERS,
                   sizeof(PYTHON_BUILTIN_MEMBERS) / sizeof(PYTHON_BUILTIN_MEMBERS[0]),
                   sizeof(PYTHON_BUILTIN_MEMBERS[0]), python_builtin_member_cmp) != NULL;
}

/* The member guard's one exemption. A Python member call whose receiver could
 * not be typed still carries real evidence when three facts line up: the callee
 * name has exactly ONE definition in the whole project (strategy unique_name),
 * the receiver is an attribute chain rooted at self/cls — an object the class
 * owns, not a parameter handed in — and the name is not a builtin type's own
 * method. `self.compiler.apply_converters()` names the only apply_converters in
 * django and nothing else it could mean exists. Measured on django/django
 * (v0.10.8 → v0.11.0 A/B, 2026-09-16): the unconditional guard dropped 800 such
 * self-rooted edges, every sampled one a real call in the source. A bare
 * parameter receiver (`accelerator.backward()`, #1276's own case) carries no
 * ownership evidence and stays suppressed, as does every builtin-member name
 * (`self.parts.extend()`). Python only: the JS/TS family keeps the TS-LSP trade
 * recorded in the resolution probe. Pure; unit-tested in test_registry.c. */
bool cbm_weak_member_unique_name_exempt(bool is_python, bool receiver_is_self_attribute,
                                        const char *callee_name, const char *strategy) {
    if (!is_python || !receiver_is_self_attribute) {
        return false;
    }
    if (!strategy || strcmp(strategy, "unique_name") != 0) {
        return false;
    }
    if (!callee_name || !callee_name[0]) {
        return false;
    }
    return !cbm_python_is_builtin_member(simple_name(callee_name));
}

/* Bare-call counterpart of the member guard above. A Python call `foo()` whose
 * callee identifier is bound as a parameter of an enclosing scope cannot be the
 * module-level `foo`: the parameter shadows it for the whole body. Binding such
 * a call to a project Function/Method by a weak short-name strategy fabricates
 * the edge by construction (`def _run_with_heavy_slot(run): run()` ->
 * SatoriLive.run).
 *
 * This is deliberately NOT keyed on the callee's spelling. A list of
 * "generic-looking" names (get / run / execute) asserts that certain spellings
 * are usually noise, which is a claim about corpus fashion rather than about
 * what the resolver knew — and it ages invisibly, because nothing fails when the
 * distribution shifts, the graph just quietly loses different edges. A parameter
 * binding is a fact about THIS file's scope, decidable outright.
 *
 * `enabled` is the caller's per-language gate, kept out of the helper for the
 * same reason as the member guard: the two call sites in pass_calls.c and
 * pass_parallel.c MUST enumerate the identical language set, or the sequential
 * and parallel resolvers diverge. Pure; unit-tested in test_registry.c. */
bool cbm_suppress_weak_local_binding_call(bool enabled, bool callee_is_locally_bound,
                                          const char *strategy) {
    if (!enabled || !callee_is_locally_bound) {
        return false;
    }
    return weak_short_name_strategy(strategy);
}

/* Import-binding counterpart of the parameter guard above (#2127). An import
 * binds the identifier for the whole module; when that import is external it
 * never materializes, and the chain falls through to project-wide short-name
 * strategies (`from unittest.mock import patch; patch()` -> a REST view's
 * `PkgConfigView.patch`). The caller decides whether the binding contradicts
 * the target (cbm_python_import_binding_contradicts: a SCOPE FACT about this
 * file, not a spelling list); only weak strategies are dropped, so import_map /
 * same_module / lsp_* edges are untouched. Pure; unit-tested in test_registry.c. */
bool cbm_suppress_weak_import_bound_call(bool enabled, bool import_binding_contradicts,
                                         const char *strategy) {
    if (!enabled || !import_binding_contradicts) {
        return false;
    }
    return weak_short_name_strategy(strategy);
}

static bool js_ts_family(CBMLanguage lang) {
    return lang == CBM_LANG_JAVASCRIPT || lang == CBM_LANG_TYPESCRIPT || lang == CBM_LANG_TSX ||
           lang == CBM_LANG_ARKTS;
}

/* C and C++ are one family for cross-language checks: .h maps to CBM_LANG_CPP
 * in the extension table, so a .c file referencing a symbol declared in its
 * own header would otherwise read as a language boundary. */
static bool c_cpp_family(CBMLanguage lang) {
    return lang == CBM_LANG_C || lang == CBM_LANG_CPP;
}

/* ── Cross-language resolution compatibility ─────────────────────────
 * A name-based registry guess (qualified_suffix / unique_name / suffix_match)
 * carries no import or type evidence, so its answer only stands when the
 * caller's language can actually call the target by that name
 * (cbm_registry_name_guess_vetoed). same_module joins them: module QNs drop
 * the extension, so tests/x.c and tests/x.py share one module, and a call in
 * the C file found the Python function by name inside it. Families are the interop
 * groups the resolvers already honour, plus the well-known interop pairs:
 *   - JS family: JS/TS/TSX/ArkTS (the #725 and #1928 guards) and the script
 *     hosts whose calls ARE JS/TS (HTML/Vue/Svelte/Astro, the weak-member
 *     guard's embedded-script set; QML embeds JS and imports .js files).
 *   - C family: C/C++ (.h maps to C++, the #1928 guard), plus CUDA and ObjC,
 *     which call C/C++ declarations from shared headers directly.
 *   - JVM: Java/Kotlin/Scala/Groovy call each other's classes directly (the
 *     JVM cross-file LSP pairs Java with Kotlin).
 *   - .NET: C#/F#. PHP: PHP/Blade. Go: Go/templ (templ compiles to Go).
 *     Lua: Lua/Luau/Teal. BEAM: Erlang/Elixir/Gleam. Shell: Bash/Zsh.
 *     SQL: SQL/PL-SQL. Salesforce: Apex/SOQL/SOSL. HDL: Verilog/SystemVerilog.
 *     CFML: tag and script dialects. ObjectScript: UDL/routine/export.
 *   - Swift additionally calls the C family (C and ObjC headers import into
 *     Swift as native declarations), and Python calls ObjectScript (IRIS
 *     Embedded Python, iris.cls(...), #1260) and back.
 * Every other language is its own family. An unknown language on either side
 * (CBM_LANG_COUNT) is compatible with everything: without evidence, no filter.
 * The languages are the DETECTED languages of the calling file and of the
 * defining file (not a filename guess: .m is MATLAB by extension but ObjC by
 * content), so full, parallel and incremental registries agree. */
enum {
    XL_FAM_NONE = 0,
    XL_FAM_JS,
    XL_FAM_C,
    XL_FAM_JVM,
    XL_FAM_DOTNET,
    XL_FAM_PHP,
    XL_FAM_GO,
    XL_FAM_LUA,
    XL_FAM_BEAM,
    XL_FAM_SHELL,
    XL_FAM_SQL,
    XL_FAM_SALESFORCE,
    XL_FAM_HDL,
    XL_FAM_CFML,
    XL_FAM_OBJECTSCRIPT,
};

static int xl_family(CBMLanguage lang) {
    switch (lang) {
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
    case CBM_LANG_ARKTS:
    case CBM_LANG_HTML:
    case CBM_LANG_VUE:
    case CBM_LANG_SVELTE:
    case CBM_LANG_ASTRO:
    case CBM_LANG_QML:
        return XL_FAM_JS;
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_CUDA:
    case CBM_LANG_OBJC:
        return XL_FAM_C;
    case CBM_LANG_JAVA:
    case CBM_LANG_KOTLIN:
    case CBM_LANG_SCALA:
    case CBM_LANG_GROOVY:
        return XL_FAM_JVM;
    case CBM_LANG_CSHARP:
    case CBM_LANG_FSHARP:
        return XL_FAM_DOTNET;
    case CBM_LANG_PHP:
    case CBM_LANG_BLADE:
        return XL_FAM_PHP;
    case CBM_LANG_GO:
    case CBM_LANG_TEMPL:
        return XL_FAM_GO;
    case CBM_LANG_LUA:
    case CBM_LANG_LUAU:
    case CBM_LANG_TEAL:
        return XL_FAM_LUA;
    case CBM_LANG_ERLANG:
    case CBM_LANG_ELIXIR:
    case CBM_LANG_GLEAM:
        return XL_FAM_BEAM;
    case CBM_LANG_BASH:
    case CBM_LANG_ZSH:
        return XL_FAM_SHELL;
    case CBM_LANG_SQL:
    case CBM_LANG_PLSQL:
        return XL_FAM_SQL;
    case CBM_LANG_APEX:
    case CBM_LANG_SOQL:
    case CBM_LANG_SOSL:
        return XL_FAM_SALESFORCE;
    case CBM_LANG_VERILOG:
    case CBM_LANG_SYSTEMVERILOG:
        return XL_FAM_HDL;
    case CBM_LANG_CFML:
    case CBM_LANG_CFSCRIPT:
        return XL_FAM_CFML;
    case CBM_LANG_OBJECTSCRIPT_UDL:
    case CBM_LANG_OBJECTSCRIPT_ROUTINE:
    case CBM_LANG_OBJECTSCRIPT_EXPORT:
        return XL_FAM_OBJECTSCRIPT;
    default:
        return XL_FAM_NONE;
    }
}

bool cbm_lang_resolution_compatible(CBMLanguage caller, CBMLanguage target) {
    if (caller < 0 || caller >= CBM_LANG_COUNT || target < 0 || target >= CBM_LANG_COUNT) {
        return true; /* unknown on either side: no evidence to filter on */
    }
    if (caller == target) {
        return true;
    }
    int cf = xl_family(caller);
    int tf = xl_family(target);
    if (cf != XL_FAM_NONE && cf == tf) {
        return true;
    }
    /* Swift imports C and ObjC headers as native declarations. IRIS Embedded
     * Python and ObjectScript call each other's classes by name
     * (iris.cls("Pkg.A").Run(), #1260). */
    return (caller == CBM_LANG_SWIFT && tf == XL_FAM_C) ||
           (target == CBM_LANG_SWIFT && cf == XL_FAM_C) ||
           (caller == CBM_LANG_PYTHON && tf == XL_FAM_OBJECTSCRIPT) ||
           (target == CBM_LANG_PYTHON && cf == XL_FAM_OBJECTSCRIPT);
}

/* Synthetic definition sources minted by a language's own LSP layer
 * (py_builtins.c, kotlin_builtins.c). They have no real file to detect, but
 * their language is a fact of the injecting LSP. */
CBMLanguage cbm_registry_synthetic_path_language(const char *file_path) {
    if (!file_path) {
        return CBM_LANG_COUNT;
    }
    if (strcmp(file_path, "<python-builtins>") == 0) {
        return CBM_LANG_PYTHON;
    }
    if (strcmp(file_path, "<kotlin-builtins>") == 0) {
        return CBM_LANG_KOTLIN;
    }
    return CBM_LANG_COUNT;
}

static const char *path_basename(const char *path) {
    if (!path || !path[0]) {
        return path;
    }
    const char *slash = strrchr(path, '/');
#ifdef _WIN32
    const char *bslash = strrchr(path, '\\');
    if (bslash && (!slash || bslash > slash)) {
        slash = bslash;
    }
#endif
    return slash ? slash + 1 : path;
}

/* Build and configuration languages have no cross-language call semantics: a
 * Makefile's `$(eval ...)` or a CMake `function(...)` names nothing in a C
 * file, so a bare-name bind into another language is always a collision
 * (2026-09-16 probe: kernel Makefile targets bound to `sk_psock.eval`). */
static bool build_config_language(CBMLanguage lang) {
    return lang == CBM_LANG_MAKEFILE || lang == CBM_LANG_CMAKE || lang == CBM_LANG_YAML ||
           lang == CBM_LANG_TOML || lang == CBM_LANG_JSON || lang == CBM_LANG_INI ||
           lang == CBM_LANG_DOCKERFILE;
}

bool cbm_suppress_cross_language_suffix_match(CBMLanguage caller_lang, const char *target_file_path,
                                              const char *strategy) {
    /* Two same-named symbols in different languages: suffix_match picks one
     * winner by import-distance and attaches every bare-name call to it
     * (#725, Bash/Python main, JS/Python commit). unique_name is the
     * candidates==1 case (#1572) and is not this guard — except for a build
     * or configuration caller, where even a unique match into another
     * language is a collision by construction. */
    if (!strategy) {
        return false;
    }
    bool config_caller = build_config_language(caller_lang);
    if (strcmp(strategy, "suffix_match") != 0 &&
        !(config_caller && strcmp(strategy, "unique_name") == 0)) {
        return false;
    }
    if (caller_lang == CBM_LANG_COUNT || !target_file_path || !target_file_path[0]) {
        return false;
    }
    CBMLanguage target_lang = cbm_language_for_filename(path_basename(target_file_path));
    if (target_lang == CBM_LANG_COUNT) {
        return false;
    }
    if (caller_lang == target_lang) {
        return false;
    }
    if (js_ts_family(caller_lang) && js_ts_family(target_lang)) {
        return false;
    }
    return true;
}

bool cbm_suppress_cross_language_ref(CBMLanguage caller_lang, const char *target_file_path) {
    /* #1928: USAGE / WRITES / READS analog of the CALLS guard above. A
     * variable or field reference resolved by the short-name registry must
     * not cross a language boundary: unlike CALLS, a reference edge carries
     * no import-closure evidence at all — a Go test's local `event` and an
     * eBPF C probe's automatic `event` share nothing but the spelling, so
     * EVERY registry strategy is a bare-name guess here and none is exempt.
     * LSP-backed semantic references resolve before the registry fallback
     * and never reach this predicate, which is where a genuine cross-language
     * binding (a future cgo resolver) would live. The JS/TS family keeps its
     * exemption (.js/.ts/.d.ts pairs legitimately share symbols), and C/C++
     * count as one family (.h maps to CBM_LANG_CPP). */
    if (caller_lang == CBM_LANG_COUNT || !target_file_path || !target_file_path[0]) {
        return false;
    }
    CBMLanguage target_lang = cbm_language_for_filename(path_basename(target_file_path));
    if (target_lang == CBM_LANG_COUNT) {
        return false;
    }
    if (caller_lang == target_lang) {
        return false;
    }
    if (js_ts_family(caller_lang) && js_ts_family(target_lang)) {
        return false;
    }
    if (c_cpp_family(caller_lang) && c_cpp_family(target_lang)) {
        return false;
    }
    return true;
}

bool cbm_go_suppress_bare_field_ref(bool is_go, bool is_member_access, const char *target_label) {
    /* #1942/#1962: a bare Go identifier can never denote a struct field —
     * field access is always a selector expression (x.f). The extractor
     * strips the receiver before the resolver runs (resolve_lhs_write_name
     * records the trailing field name; is_reference_node records the inner
     * field_identifier), so the reference TEXT is always dot-less and cannot
     * carry the distinction — the recorded is_member_access shape can. Only a
     * reference that was never the member half of a selector is refused a
     * Field bind. Go-gated: a C#/Java/C++/Python method body legitimately
     * references its own members bare (cp_reads_writes_cs_static_field pins
     * that shape as required), so a global veto would break those languages. */
    if (!is_go || is_member_access || !target_label) {
        return false;
    }
    return strcmp(target_label, "Field") == 0;
}

cbm_field_call_policy_t cbm_call_onto_field_policy(CBMLanguage caller_lang, const char *callee_text,
                                                   const char *target_label,
                                                   const char *target_file_path) {
    /* The short-name registry holds Field nodes, so a call spelled like some
     * struct member can resolve onto that Field. Three shapes are decided here:
     *   - a caller in another language: nothing but the spelling connects a
     *     Bash `command` or a Python `sorted()` to a C struct member. The
     *     unique_name exemption of the CALLS guard (#1572) is about functions;
     *     for a Field the #1928 reference rule applies, with its JS/TS and
     *     C/C++ family exemptions;
     *   - a bare C call (`socket(...)`, a function-pointer parameter
     *     `want(x)`): a bare C name is never a struct member;
     *   - a C or C++ member call (`cb.close(ctx)`, `text.size()`): the type of
     *     the object decides which struct's member is called, never the
     *     member name alone. The C LSP publishes owners in both modes.
     * A bare C++ call keeps its resolution (a method body reaches its own
     * members without a selector), and so does every other language. */
    if (!target_label || strcmp(target_label, "Field") != 0) {
        return CBM_FIELD_CALL_KEEP;
    }
    if (cbm_suppress_cross_language_ref(caller_lang, target_file_path)) {
        return CBM_FIELD_CALL_DROP;
    }
    if (caller_lang != CBM_LANG_C && caller_lang != CBM_LANG_CPP) {
        return CBM_FIELD_CALL_KEEP;
    }
    if (callee_text && (strchr(callee_text, '.') || strstr(callee_text, "->"))) {
        return CBM_FIELD_CALL_BY_OWNER;
    }
    return caller_lang == CBM_LANG_C ? CBM_FIELD_CALL_DROP : CBM_FIELD_CALL_KEEP;
}

bool cbm_c_arrow_member_call(CBMLanguage caller_lang, const char *callee_text) {
    if (caller_lang != CBM_LANG_C && caller_lang != CBM_LANG_CPP) {
        return false;
    }
    return callee_text != NULL && strstr(callee_text, "->") != NULL;
}

bool cbm_c_member_rule_file(CBMLanguage lang, const char *rel_path) {
    if (lang == CBM_LANG_C) {
        return true;
    }
    if (lang != CBM_LANG_CPP || !rel_path) {
        return false;
    }
    size_t len = strlen(rel_path);
    return len > PAIR_LEN && strcmp(rel_path + len - PAIR_LEN, ".h") == 0;
}

bool cbm_c_member_binds_by_owner(bool is_c, bool is_member_access) {
    /* The member half of `a.b` / `a->b` is not a name in any C scope: only the
     * type of `a` gives `b` a meaning. Handing the bare `b` to the short-name
     * registry bound whichever Field (or function, or variable) was called
     * `b` -- on this repository 9,150 USAGE and 2,810 WRITES edges onto
     * Fields, 3,976 of the USAGE edges on a name several structs share. Such a
     * reference binds through the C LSP's field-owner rows; an object the LSP
     * could not type binds only a member name that is unique in the project
     * (cbm_registry_unique_field_qn), never one of several.
     *
     * C-gated like the Go rule above: a C++ or Objective-C method body
     * reaches its own members without a selector, and their member accesses
     * keep the existing resolution until their LSPs publish owners too. */
    return is_c && is_member_access;
}

/* ── Lifecycle ──────────────────────────────────────────────────── */

cbm_registry_t *cbm_registry_new(void) {
    cbm_registry_t *r = calloc(CBM_ALLOC_ONE, sizeof(cbm_registry_t));
    if (!r) {
        return NULL;
    }
    r->exact = cbm_ht_create(CBM_SZ_1K);
    r->by_name = cbm_ht_create(CBM_SZ_512);
    return r;
}

static void free_label(const char *key, void *value, void *ud) {
    (void)ud;
    (void)value; /* interned in the registry's label_pool */
    free((void *)key);
}

static void free_qn_array(const char *key, void *value, void *ud) {
    (void)ud;
    qn_array_t *arr = value;
    if (arr) {
        /* items borrow the exact map's keys — freed there, not here */
        cbm_free(CBM_MEM_CLASS_DYN_ARRAY, arr->is_test);
        cbm_free(CBM_MEM_CLASS_DYN_ARRAY, arr->lang);
        cbm_da_free(arr);
        free(arr);
    }
    free((void *)key);
}

void cbm_registry_free(cbm_registry_t *r) {
    if (!r) {
        return;
    }
    /* by_name first: its items borrow exact's keys. */
    cbm_ht_foreach(r->by_name, free_qn_array, NULL);
    cbm_ht_free(r->by_name);
    cbm_ht_foreach(r->exact, free_label, NULL);
    cbm_ht_free(r->exact);
    for (int i = 0; i < r->label_pool_n; i++) {
        free(r->label_pool[i]);
    }
    free(r);
}

/* ── Registration ────────────────────────────────────────────────── */

/* Record `owned_qn` in the by-name bucket for `key`, keeping the bucket's
 * is_test flags in step with its entries.
 *
 * No array dedup needed: cbm_registry_add's exact-map check guarantees the QN
 * is new, and it calls this at most once per distinct key. */
static void index_under_name(cbm_registry_t *r, const char *key, const char *owned_qn,
                             CBMLanguage lang) {
    qn_array_t *arr = cbm_ht_get(r->by_name, key);
    if (!arr) {
        arr = calloc(CBM_ALLOC_ONE, sizeof(qn_array_t));
        cbm_ht_set(r->by_name, strdup(key), arr);
    }
    int before = arr->count;
    cbm_da_push(arr, (char *)owned_qn);
    if (arr->count == before) {
        return; /* the name could not be recorded: no verdict to cache */
    }
    if (arr->count > arr->is_test_cap) {
        int want = arr->cap > 0 ? arr->cap : arr->count;
        uint8_t *grown =
            cbm_realloc(CBM_MEM_CLASS_DYN_ARRAY, arr->is_test, (size_t)want * sizeof(uint8_t));
        if (grown) {
            arr->is_test = grown;
            uint8_t *grown_lang =
                cbm_realloc(CBM_MEM_CLASS_DYN_ARRAY, arr->lang, (size_t)want * sizeof(uint8_t));
            /* Both side arrays share is_test_cap: on failure it stays below
             * count, so this name serves neither cached test verdicts nor
             * language filtering, exactly the unfiltered answer. */
            if (grown_lang) {
                arr->lang = grown_lang;
                arr->is_test_cap = want;
            }
        }
    }
    if (arr->count <= arr->is_test_cap) {
        arr->is_test[arr->count - SKIP_ONE] = is_test_qn(owned_qn) ? 1 : 0;
        arr->lang[arr->count - SKIP_ONE] =
            (uint8_t)(lang >= 0 && lang < CBM_LANG_COUNT ? lang : CBM_LANG_COUNT);
    }
}

void cbm_registry_add(cbm_registry_t *r, const char *name, const char *qualified_name,
                      const char *label) {
    cbm_registry_add_lang(r, name, qualified_name, label, CBM_LANG_COUNT);
}

void cbm_registry_add_lang(cbm_registry_t *r, const char *name, const char *qualified_name,
                           const char *label, CBMLanguage lang) {
    if (!r || !qualified_name || !label) {
        return;
    }

    /* Check for duplicate */
    if (cbm_ht_get(r->exact, qualified_name)) {
        return;
    }

    /* Intern the label (bounded set; linear scan is fine at this size). */
    const char *interned = NULL;
    for (int i = 0; i < r->label_pool_n; i++) {
        if (strcmp(r->label_pool[i], label) == 0) {
            interned = r->label_pool[i];
            break;
        }
    }
    if (!interned && r->label_pool_n < (int)(sizeof(r->label_pool) / sizeof(r->label_pool[0]))) {
        r->label_pool[r->label_pool_n] = strdup(label);
        interned = r->label_pool[r->label_pool_n];
        r->label_pool_n++;
    }
    if (!interned) {
        return; /* pool exhausted (cannot happen with sane label sets) */
    }

    /* Store in exact map: QN → interned label. The key is the registry's ONE
     * owned copy of the QN; by_name below borrows it (same lifetime) instead
     * of a second strdup — this pair of copies was ~280 MB on the kernel. */
    cbm_ht_set(r->exact, strdup(qualified_name), (void *)interned);
    const char *owned_qn = cbm_ht_get_key(r->exact, qualified_name);

    /* Index the symbol under the QN's last dot segment, and additionally under
     * the name its caller passed when that segment carries a '#' fence.
     *
     * The derived key is the one every language has always used and it stays
     * unconditional, so a grammar whose QNs carry no '#' is byte-identical to
     * before this commit. The second key exists for one shape: a fenced tail is
     * a literal string no bare callee is ever written as. A QN such as
     * "proj.lib.add#cfg(test)" (Rust cfg twins were minted that way until they
     * became variants of one definition) would otherwise be filed under the
     * whole string "add#cfg(test)" — simple_name() has no '#' handling — where
     * no bare `add` callee could reach it.
     *
     * Gating on the fence rather than on the file's language is what makes
     * "unchanged for every other grammar" checkable instead of asserted. Every
     * by-name lookup keys through simple_name(), which splits on '.' and "::"
     * only, so a passed name that differs from the derived key by anything
     * OTHER than a fence is a key no lookup can reach: an HCL block's
     * "resource.aws_instance.web" and a TOML table's "tool.poetry.dependencies"
     * are both already indexed under the tail a reference spells, and adding
     * their dotted form would only widen the bucket the scorer walks.
     *
     * `name` is NULL or empty only for callers that have no symbol name to
     * give; those have the derived key and nothing else.
     *
     * A signature-qualified callable (#2061) is indexed by its bare `name` in
     * place of the derived key, so every overload shares one bucket; any other
     * QN keeps its historical last-segment key. */
    const char *derived = simple_name(qualified_name);
    const char *primary =
        name && cbm_qn_callable_base_len_named(owned_qn, name) < strlen(owned_qn) ? name : derived;
    index_under_name(r, primary, owned_qn, lang);
    /* '#' is a QN fence; no extractor mints one for a REGISTERED definition
     * today (Rust cfg twins are variants of one plain QN; a C macro's QN ends in
     * "#macro", but Macro is no registry label, so it never gets here). A
     * grammar that starts
     * minting a '#' opts into this second key by doing so, whatever it means by
     * the fence: its symbols become reachable under the passed name as well,
     * and they share that name's bucket with everything else filed under it.
     * The passed name is compared with the key already written: a
     * signature-qualified callable is filed under `name` already, and its
     * capped suffix may carry a '#' of its own. */
    if (name && name[0] && strchr(derived, '#') && strcmp(name, primary) != 0) {
        index_under_name(r, name, owned_qn, lang);
    }
}

/* ── Lookup ──────────────────────────────────────────────────────── */

bool cbm_registry_exists(const cbm_registry_t *r, const char *qn) {
    if (!r || !qn) {
        return false;
    }
    return cbm_ht_get(r->exact, qn) != NULL;
}

const char *cbm_registry_label_of(const cbm_registry_t *r, const char *qn) {
    if (!r || !qn) {
        return NULL;
    }
    return cbm_ht_get(r->exact, qn);
}

const char *cbm_registry_value_target(const cbm_registry_t *r, const char *qn) {
    const char *label = cbm_registry_label_of(r, qn);
    if (!label || strcmp(label, "Method") != 0) {
        return qn;
    }
    char twin[CBM_SZ_1K];
    int n = snprintf(twin, sizeof(twin), "%s#field", qn);
    if (n < 0 || (size_t)n >= sizeof(twin)) {
        return qn;
    }
    const char *owned = cbm_ht_get_key(r->exact, twin);
    return owned ? owned : qn;
}

int cbm_registry_find_by_name(const cbm_registry_t *r, const char *name, const char ***out,
                              int *count) {
    if (!r || !out || !count) {
        return CBM_NOT_FOUND;
    }
    qn_array_t *arr = cbm_ht_get(r->by_name, name);
    if (arr && arr->count > 0) {
        *out = (const char **)arr->items;
        *count = arr->count;
    } else {
        *out = NULL;
        *count = 0;
    }
    return 0;
}

const char *cbm_registry_unique_field_qn(const cbm_registry_t *r, const char *member_name) {
    /* A member name that exactly one Field in the whole project carries names
     * that Field without any choice being made. This is what an untyped C
     * member access may still bind: the defect in bare-name member resolution
     * was the arbitrary pick among SEVERAL same-named members (kernel: 359,236
     * of the edges a typed-only rule dropped were onto such a unique name).
     * Only Fields are candidates -- a function or variable of the same name
     * can never be what `a.b` means -- and two of them bind nothing. */
    const char **qns = NULL;
    int count = 0;
    if (!member_name || cbm_registry_find_by_name(r, member_name, &qns, &count) != 0 || !qns) {
        return NULL;
    }
    const char *only = NULL;
    for (int i = 0; i < count; i++) {
        const char *label = qns[i] ? cbm_registry_label_of(r, qns[i]) : NULL;
        if (!label || strcmp(label, "Field") != 0) {
            continue;
        }
        if (only) {
            return NULL;
        }
        only = qns[i];
    }
    return only;
}

int cbm_registry_size(const cbm_registry_t *r) {
    return r ? (int)cbm_ht_count(r->exact) : 0;
}

/* ── Resolution ──────────────────────────────────────────────────── */

/* Callback context for import_map_suffix scan */
/* Strategy 1: Import map lookup (exact → suffix fallback) */
static cbm_resolution_t resolve_import_map(const cbm_registry_t *r, const char *prefix,
                                           const char *suffix, const char **keys, const char **vals,
                                           int map_count) {
    if (!keys || !vals || map_count <= 0) {
        return empty_result();
    }

    /* Find prefix in import map keys. Prefer the per-file TLS hash
     * cache (O(1)) over the linear scan when available. */
    const char *resolved = NULL;
    if (_import_map_cache) {
        resolved = (const char *)cbm_ht_get(_import_map_cache, prefix);
    } else {
        for (int i = 0; i < map_count; i++) {
            if (strcmp(keys[i], prefix) == 0) {
                resolved = vals[i];
                break;
            }
        }
    }
    if (!resolved) {
        return empty_result();
    }

    /* Build candidate: resolved.suffix or resolved.prefix.
     * When the callee has a dot ("pkg.Func"), prefix="pkg" is the import key
     * and suffix="Func" is the function name, so the target QN is
     * resolved.Func. When the callee is bare ("requireAdmin"), prefix IS the
     * function name and suffix is NULL, so the target QN must be
     * resolved.requireAdmin — not just resolved, which would point at the
     * module node and miss the function entirely. */
    /* Direct hit ONLY for suffix-less callees (an aliased direct-symbol
     * import called bare: `from m import f as g; g()` — #875/#979; Yui
     * `import execute as bridge_execute`). With a suffix present
     * (`imported.method()`), returning the bare base here would swallow
     * the suffix and bind the call to the imported symbol's own node
     * (a Variable/Class/module) instead of base.method — exactly the
     * mis-resolution the comment above warns about. That regressed
     * django-scale graphs by ~11K CALLS/TESTS edges (Signal.send calls
     * degraded to edges onto the signal variables themselves). #1000 */
    if (!suffix || !suffix[0]) {
        const char *direct = cbm_ht_get_key(r->exact, resolved);
        if (direct) {
            return (cbm_resolution_t){direct, "import_map", CONF_IMPORT_MAP, REG_RESOLVED};
        }
    }
    char candidate[CBM_SZ_512];
    if (suffix && suffix[0]) {
        snprintf(candidate, sizeof(candidate), "%s.%s", resolved, suffix);
    } else {
        snprintf(candidate, sizeof(candidate), "%s.%s", resolved, prefix);
    }
    /* Use cbm_ht_get_key to get the persistent heap-owned key string */
    const char *stored_key = cbm_ht_get_key(r->exact, candidate);
    if (stored_key) {
        return (cbm_resolution_t){stored_key, "import_map", CONF_IMPORT_MAP, REG_RESOLVED};
    }

    /* import_map_suffix fallback: find a QN starting with resolved+"." and
     * ending with "."+suffix. Any such QN's last segment equals the last
     * segment of suffix, so probe the by_name index and tail-check the (few)
     * candidates instead of cbm_ht_foreach over the WHOLE exact table — that
     * scan ran per unresolved call and dominated elasticsearch's resolve
     * phase (94% of samples: 700k-entry foreach + strlen per entry). */
    if (suffix && suffix[0]) {
        char resolved_dot[CBM_SZ_512];
        char dot_suffix[CBM_SZ_256];
        snprintf(resolved_dot, sizeof(resolved_dot), "%s.", resolved);
        snprintf(dot_suffix, sizeof(dot_suffix), ".%s", suffix);
        qn_array_t *arr = cbm_ht_get(r->by_name, simple_name(suffix));
        if (arr) {
            size_t rd_len = strlen(resolved_dot);
            size_t ds_len = strlen(dot_suffix);
            for (int i = 0; i < arr->count; i++) {
                const char *qn = arr->items[i];
                size_t klen = strlen(qn);
                if (klen >= rd_len + ds_len && strncmp(qn, resolved_dot, rd_len) == 0 &&
                    strcmp(qn + klen - ds_len, dot_suffix) == 0) {
                    return (cbm_resolution_t){qn, "import_map_suffix", CONF_IMPORT_MAP_SUFFIX,
                                              REG_RESOLVED};
                }
            }
        }
    }
    return empty_result();
}

/* Strategy 2: Same-module match */
static cbm_resolution_t resolve_same_module(const cbm_registry_t *r, const char *callee_name,
                                            const char *suffix, const char *module_qn) {
    char candidate[CBM_SZ_512];
    snprintf(candidate, sizeof(candidate), "%s.%s", module_qn, callee_name);
    const char *stored_key = cbm_ht_get_key(r->exact, candidate);
    if (stored_key) {
        return (cbm_resolution_t){stored_key, "same_module", CONF_SAME_MODULE, REG_RESOLVED};
    }
    if (suffix && suffix[0]) {
        snprintf(candidate, sizeof(candidate), "%s.%s", module_qn, suffix);
        stored_key = cbm_ht_get_key(r->exact, candidate);
        if (stored_key) {
            return (cbm_resolution_t){stored_key, "same_module", CONF_SAME_MODULE, REG_RESOLVED};
        }
    }
    return empty_result();
}

/* Strategy 4: multiple candidates with import filtering. */
static cbm_resolution_t resolve_multi_with_imports(const cbm_registry_t *r, const qn_array_t *arr,
                                                   const char *module_qn, const char **import_vals,
                                                   int import_count) {
    const char *filtered[CBM_SZ_256];
    uint8_t filtered_test[CBM_SZ_256];
    uint8_t filtered_lang[CBM_SZ_256];
    const uint8_t *flags = qn_test_flags(arr);
    const uint8_t *langs = qn_lang_flags(arr);
    int fcount = 0;
    for (int i = 0; i < arr->count && fcount < CBM_SZ_256; i++) {
        if (is_import_reachable(arr->items[i], import_vals, import_count)) {
            if (flags) {
                filtered_test[fcount] = flags[i];
            }
            if (langs) {
                filtered_lang[fcount] = langs[i];
            }
            filtered[fcount] = arr->items[i];
            fcount++;
        }
    }
    if (fcount == SKIP_ONE) {
        double conf = candidate_count_penalty(CONF_SUFFIX_MATCH, arr->count);
        return (cbm_resolution_t){filtered[0], "suffix_match", conf, arr->count};
    }
    if (fcount > SKIP_ONE) {
        const char *best = best_by_import_distance(r, filtered, flags ? filtered_test : NULL,
                                                   langs ? filtered_lang : NULL, fcount, module_qn);
        if (best) {
            double conf = candidate_count_penalty(CONF_SUFFIX_MATCH, fcount);
            return (cbm_resolution_t){best, "suffix_match", conf, fcount};
        }
    }
    /* No import-reachable — use all candidates with penalty */
    const char *best = best_by_import_distance(r, (const char **)arr->items, qn_test_flags(arr),
                                               qn_lang_flags(arr), arr->count, module_qn);
    if (best) {
        double conf = candidate_count_penalty(CONF_SUFFIX_MATCH * REG_HALF_PENALTY, arr->count);
        return (cbm_resolution_t){best, "suffix_match", conf, arr->count};
    }
    return empty_result();
}

/* Confidence for a full qualified-tail match (Strategy 3.5). A package- or
 * namespace-qualified callee that uniquely matches one candidate's full tail is
 * as trustworthy as a same-module hit. */
#define CONF_QUALIFIED_SUFFIX 0.90

/* When a callee is package/namespace-qualified (Foo::Bar::sub or Foo.Bar.sub),
 * disambiguate among same-simple-name candidates by matching the FULL qualified
 * tail against each candidate QN at a segment boundary. Returns the sole
 * candidate whose QN equals or ends with ".<dotted-callee>", or NULL when zero
 * or several candidates match (the caller then falls back to bare-name scoring).
 *
 * Fixes qualified cross-file calls collapsing onto one namespace when the bare
 * symbol name is defined in several — e.g. Perl's Foo::Bar::run and Foo::Baz::run
 * both reduce to "run", so the bare-name scorer would route every caller to a
 * single winner. Language agnostic: callees with no separator return NULL and
 * leave behavior unchanged. */
static const char *qualified_suffix_match(const qn_array_t *arr, const char *callee_name) {
    /* Normalize "::" → "." so the tail composes with dotted candidate QNs. */
    char dotted[CBM_SZ_512];
    size_t w = 0;
    for (const char *s = callee_name; *s && w + SKIP_ONE < sizeof(dotted);) {
        if (s[0] == ':' && s[1] == ':') {
            dotted[w++] = '.';
            s += 2;
        } else {
            dotted[w++] = *s++;
        }
    }
    dotted[w] = '\0';
    /* Must be qualified (contain a '.') — a bare name matches every candidate
     * and carries no disambiguating signal. */
    if (!strchr(dotted, '.')) {
        return NULL;
    }
    const char *match = NULL;
    for (int i = 0; i < arr->count; i++) {
        const char *qn = arr->items[i];
        size_t qlen = strlen(qn);
        if (qlen < w) {
            continue;
        }
        const char *tail = qn + (qlen - w);
        if (strcmp(tail, dotted) != 0) {
            continue;
        }
        /* Segment boundary: tail is the whole QN or is preceded by '.'. */
        if (tail != qn && tail[-1] != '.') {
            continue;
        }
        if (match) {
            return NULL; /* ambiguous — more than one qualified tail matches */
        }
        match = qn;
    }
    return match;
}

/* A dotted callee whose FIRST segment starts upper-case names a type — URLSession,
 * Calendar, JSONEncoder. That receiver chain is evidence the bare-name scorers
 * throw away, and throwing it away binds Foundation's URLSession.shared.data to
 * a project's own PickedFile.data: high confidence, and nothing in the graph
 * shows it is wrong. Require instead that the candidate's own parent segment
 * appears somewhere in the chain. Calendar.utcGregorian.startOfDayUTC resolving
 * to AuthDTOs.Calendar.startOfDayUTC passes, because Calendar is in the chain.
 *
 * Only an upper-case first segment is guarded. A lower-case root names a value
 * (vm.load, http.Get, os.path.join) whose declared type the chain does not
 * show, so the chain proves nothing there and the call passes through
 * unchanged. A callee with no separator passes through as well.
 *
 * Language agnostic by design: the registry holds no language, and every
 * language that writes receiver chains gains the same protection. */
static bool receiver_chain_admits(const char *callee_name, const char *candidate_qn) {
    /* Normalize "::" -> "." so the chain composes with dotted candidate QNs,
     * the same way qualified_suffix_match does. */
    char dotted[CBM_SZ_512];
    size_t w = 0;
    for (const char *s = callee_name; *s && w + SKIP_ONE < sizeof(dotted);) {
        if (s[0] == ':' && s[1] == ':') {
            dotted[w++] = '.';
            s += 2;
        } else {
            dotted[w++] = *s++;
        }
    }
    dotted[w] = '\0';

    const char *last_dot = strrchr(dotted, '.');
    if (!last_dot) {
        return true; /* bare name — no receiver chain to judge */
    }
    if (dotted[0] < 'A' || dotted[0] > 'Z') {
        return true; /* lower-case root names a value, not a type */
    }
    /* A name written in capitals with underscores is a constant holding a
     * value, not a type: ISO_4217_URL.lower is a string's own method. JSON and
     * URL carry no underscore and stay guarded. */
    int has_underscore = 0;
    int all_caps = 1;
    for (const char *c = dotted; c < last_dot && *c != '.'; c++) {
        if (*c == '_') {
            has_underscore = 1;
        } else if (*c >= 'a' && *c <= 'z') {
            all_caps = 0;
            break;
        }
    }
    if (all_caps && has_underscore) {
        return true;
    }

    /* The candidate's ancestry: every segment before its final name. */
    const char *cand_last = strrchr(candidate_qn, '.');
    if (!cand_last || cand_last == candidate_qn) {
        return true; /* top-level candidate — no ancestry to look for */
    }

    /* Walk the chain — every segment before the final callee name. A trailing
     * "()" is dropped so JSONEncoder().encode reads as JSONEncoder. A chain
     * segment admits the candidate when it names ANY segment of the candidate's
     * ancestry, not only the immediate parent: Settings.builder().putList binds
     * settings.Settings.Builder.putList because Settings owns Builder, and
     * RequestOptions.DEFAULT.toBuilder().setWarningsHandler binds
     * RequestOptions.Builder.setWarningsHandler the same way. Measured on
     * elastic/elasticsearch: the parent-only rule refused 9,473 unique-name
     * calls, most of them exactly these factory chains into a nested Builder.
     * A foreign root still finds nothing in the ancestry — Base64.getEncoder
     * does not admit DocOffsetsCodec.getEncoder, URLSession.shared.data does
     * not admit PickedFile.data — so the #1893 refusals hold. */
    for (const char *seg = dotted; seg < last_dot;) {
        const char *end = strchr(seg, '.');
        size_t len = (size_t)(end - seg);
        if (len >= 2 && seg[len - 2] == '(' && seg[len - 1] == ')') {
            len -= 2; /* an empty "()" — JSONEncoder().encode names JSONEncoder */
        }
        for (const char *anc = candidate_qn; anc < cand_last;) {
            const char *anc_end = strchr(anc, '.');
            size_t anc_len = (size_t)(anc_end - anc);
            if (anc_len == len && len > 0 && strncmp(seg, anc, len) == 0) {
                return true;
            }
            anc = anc_end + SKIP_ONE;
        }
        seg = end + SKIP_ONE;
    }
    return false;
}

/* Strategy 3+4: Name lookup + suffix match */
static cbm_resolution_t resolve_name_lookup(const cbm_registry_t *r, const char *callee_name,
                                            const char *module_qn, const char **import_vals,
                                            int import_count) {
    const char *lookup = simple_name(callee_name);
    qn_array_t *arr = cbm_ht_get(r->by_name, lookup);
    if (!arr || arr->count == 0) {
        return empty_result();
    }
    if (arr->count > REG_MAX_CANDIDATES) {
        return empty_result(); /* unresolvably ambiguous — see REG_MAX_CANDIDATES */
    }

    /* Strategy 3.5: a qualified callee disambiguates among multiple same-name
     * candidates by full qualified tail, before bare-name scoring collapses
     * them onto a single winner. */
    if (arr->count > 1) {
        const char *q = qualified_suffix_match(arr, callee_name);
        if (q) {
            return (cbm_resolution_t){q, "qualified_suffix", CONF_QUALIFIED_SUFFIX, REG_RESOLVED};
        }
    }

    /* Strategy 3: unique name */
    if (arr->count == SKIP_ONE) {
        if (!receiver_chain_admits(callee_name, arr->items[0])) {
            return empty_result();
        }
        double conf = CONF_UNIQUE_NAME;
        if (import_vals && import_count > 0 &&
            !is_import_reachable(arr->items[0], import_vals, import_count)) {
            conf *= DEFAULT_CONFIDENCE;
        }
        return (cbm_resolution_t){arr->items[0], "unique_name", conf, REG_RESOLVED};
    }

    /* Strategy 4: multiple candidates */
    if (import_vals && import_count > 0) {
        return resolve_multi_with_imports(r, arr, module_qn, import_vals, import_count);
    }
    const char *best = best_by_import_distance(r, (const char **)arr->items, qn_test_flags(arr),
                                               qn_lang_flags(arr), arr->count, module_qn);
    if (best) {
        if (!receiver_chain_admits(callee_name, best)) {
            return empty_result();
        }
        double conf = candidate_count_penalty(CONF_SUFFIX_MATCH, arr->count);
        return (cbm_resolution_t){best, "suffix_match", conf, arr->count};
    }
    return empty_result();
}

/* The strategy chain shared by both public resolve variants (no caching here —
 * cbm_registry_resolve owns the per-file cache). */
static cbm_resolution_t registry_resolve_chain(const cbm_registry_t *r, const char *callee_name,
                                               const char *module_qn, const char **import_map_keys,
                                               const char **import_map_vals, int import_map_count) {
    /* Split callee at the first path separator: "pkg.Func" → prefix="pkg",
     * suffix="Func".  Rust/C++ use "::" rather than ".", so honor whichever
     * separator appears first ("lib::square" → prefix="lib", suffix="square").
     * Both separators are unambiguous, so handling "::" never affects "."-only
     * callees. */
    char prefix[CBM_SZ_256] = {0};
    const char *suffix = NULL;
    const char *dot = strchr(callee_name, '.');
    const char *colons = strstr(callee_name, "::");
    const char *sep = dot;
    size_t sep_len = SKIP_ONE; /* length of '.' */
    if (colons && (!sep || colons < sep)) {
        sep = colons;
        sep_len = 2; /* length of "::" */
    }
    if (sep) {
        size_t plen = sep - callee_name;
        if (plen >= sizeof(prefix)) {
            plen = sizeof(prefix) - SKIP_ONE;
        }
        memcpy(prefix, callee_name, plen);
        prefix[plen] = '\0';
        suffix = sep + sep_len;
    } else {
        snprintf(prefix, sizeof(prefix), "%s", callee_name);
    }

    /* Strategy 1: import map */
    cbm_resolution_t res =
        resolve_import_map(r, prefix, suffix, import_map_keys, import_map_vals, import_map_count);
    if (!(res.qualified_name && res.qualified_name[0])) {
        /* Strategy 2: same module */
        res = resolve_same_module(r, callee_name, suffix, module_qn);
    }
    if (!(res.qualified_name && res.qualified_name[0])) {
        /* Strategy 3+4: name lookup */
        res = resolve_name_lookup(r, callee_name, module_qn, import_map_vals, import_map_count);
    }
    return res;
}

/* The recorded language of the registry entry `qn` (the owned key pointer a
 * resolution returns), or CBM_LANG_COUNT when unknown. The by-name bucket is
 * at most REG_MAX_CANDIDATES long whenever a name strategy produced `qn`. */
static CBMLanguage registry_entry_language(const cbm_registry_t *r, const char *qn) {
    const qn_array_t *arr = cbm_ht_get(r->by_name, simple_name(qn));
    if (!arr || !arr->lang || arr->is_test_cap < arr->count) {
        return CBM_LANG_COUNT;
    }
    for (int i = 0; i < arr->count; i++) {
        if (arr->items[i] == qn || strcmp(arr->items[i], qn) == 0) {
            return (CBMLanguage)arr->lang[i];
        }
    }
    return CBM_LANG_COUNT;
}

/* The cross-language veto (see cbm_lang_resolution_compatible). A name-only
 * answer carries no import or module evidence, so it only stands when the
 * caller's language can call the target: a JS `app.get` must not bind
 * Python's builtins.dict.get because it is the one "get" in the project.
 * Callers turn a vetoed answer into an EMPTY resolution BEFORE their empty-
 * resolution fallbacks, so route registration / HTTP classification still
 * run. A veto, not a re-pick among the remaining candidates: re-picking
 * promotes the next same-language short-name guess (measured on n8n: +3.1k
 * CALLS, 88% of them jest `describe()` bound to a project method).
 * field_type_hint is included: it re-picks by type-name substring across ALL
 * candidates, language-blind. */
bool cbm_registry_name_guess_vetoed(const cbm_registry_t *r, CBMLanguage caller_lang,
                                    const cbm_resolution_t *res) {
    if (!r || !res || caller_lang < 0 || caller_lang >= CBM_LANG_COUNT || !res->qualified_name ||
        !res->qualified_name[0] || !res->strategy) {
        return false;
    }
    if (strcmp(res->strategy, "unique_name") != 0 && strcmp(res->strategy, "suffix_match") != 0 &&
        strcmp(res->strategy, "qualified_suffix") != 0 &&
        strcmp(res->strategy, "field_type_hint") != 0 &&
        strcmp(res->strategy, "same_module") != 0) {
        return false;
    }
    return !cbm_lang_resolution_compatible(caller_lang,
                                           registry_entry_language(r, res->qualified_name));
}

cbm_resolution_t cbm_registry_resolve(const cbm_registry_t *r, const char *callee_name,
                                      const char *module_qn, const char **import_map_keys,
                                      const char **import_map_vals, int import_map_count) {
    if (!r || !callee_name) {
        return empty_result();
    }

    /* Per-file cache: same callee_name in N call sites → 1 chain walk
     * + N-1 O(1) hash hits. module_qn is constant per file so the
     * cache key only needs callee_name. */
    if (_resolve_cache) {
        resolve_cache_entry_t *cached =
            (resolve_cache_entry_t *)cbm_ht_get(_resolve_cache, callee_name);
        if (cached) {
            return cached->res;
        }
    }

    cbm_resolution_t res = registry_resolve_chain(r, callee_name, module_qn, import_map_keys,
                                                  import_map_vals, import_map_count);

    /* Data relations (Table/View) are lineage-only registry members: common
     * table names (users, orders, config) collide with code identifiers across
     * every language, so the DEFAULT resolve never returns them — a veto, not a
     * re-route, so a name-collision does not fall through to a weaker strategy.
     * Every consumer (CALLS/USAGE/READS/WRITES/THROWS/handlers/decorators,
     * present and future) is thereby relation-safe by construction. The SQL
     * lineage path opts in via cbm_registry_resolve_lineage. */
    if (res.qualified_name && res.qualified_name[0] &&
        cbm_label_is_relation(cbm_registry_label_of(r, res.qualified_name))) {
        res = empty_result();
    }

    /* Cache the result (including empty — caching the negative answer
     * is just as valuable; same name asks the same question). */
    if (_resolve_cache) {
        resolve_cache_entry_t *e = (resolve_cache_entry_t *)malloc(sizeof(*e));
        if (e) {
            e->res = res;
            char *kdup = strdup(callee_name);
            if (kdup) {
                cbm_ht_set(_resolve_cache, kdup, e);
            } else {
                free(e);
            }
        }
    }
    return res;
}

/* ── Class-qualified route handlers (PHP, #1146) ─────────────────── */

/* Method QNs are path-based: <project>.<dirs>.<file stem>.<Class>.<method>.
 * A PHP class is named by its namespace instead, so a "Ns\\Class::method"
 * handler is placed on a QN by what places the class file itself. */
#define CONF_PSR4_MEMBER 0.95
#define CONF_NAMESPACE_PATH_MEMBER 0.90

static bool seg_eq_nocase(const char *a, size_t alen, const char *b, size_t blen) {
    if (alen != blen) {
        return false;
    }
    for (size_t i = 0; i < alen; i++) {
        char ca = (char)((a[i] >= 'A' && a[i] <= 'Z') ? a[i] - 'A' + 'a' : a[i]);
        char cb = (char)((b[i] >= 'A' && b[i] <= 'Z') ? b[i] - 'A' + 'a' : b[i]);
        if (ca != cb) {
            return false;
        }
    }
    return true;
}

/* Does qn end with ".<cls>.<member>"? Returns the offset of that tail or -1. */
static int qn_class_member_tail(const char *qn, const char *cls, size_t cls_len,
                                const char *member) {
    size_t qlen = strlen(qn);
    size_t mlen = strlen(member);
    if (qlen < cls_len + mlen + PAIR_LEN) {
        return CBM_NOT_FOUND;
    }
    const char *m = qn + qlen - mlen;
    const char *c = m - SKIP_ONE - cls_len;
    if (strcmp(m, member) != 0 || m[-SKIP_ONE] != '.' || strncmp(c, cls, cls_len) != 0 ||
        c[-SKIP_ONE] != '.') {
        return CBM_NOT_FOUND;
    }
    return (int)(c - SKIP_ONE - qn);
}

/* PSR-4: the class file Composer's autoloader loads for the FQN, the class it
 * defines, and that class's member — through the PSR-4 import resolver
 * (cbm_pipeline_psr4_member_qn), so a root whose folder does not mirror its
 * namespace (Acme\\Blog\\ -> packages/blog/src) places the class as well.
 * Returns its tri-state outcome; *out_qn is the member QN when RESOLVED. */
static cbm_psr4_member_t psr4_member_qn(const cbm_gbuf_t *gbuf, const char *fqn, size_t fqn_len,
                                        const char *member, const char **out_qn) {
    char cls_fqn[CBM_SZ_512];
    *out_qn = NULL;
    if (!gbuf || fqn_len >= sizeof(cls_fqn)) {
        return CBM_PSR4_MEMBER_NOT_COVERED;
    }
    memcpy(cls_fqn, fqn, fqn_len);
    cls_fqn[fqn_len] = '\0';
    return cbm_pipeline_psr4_member_qn(gbuf, cls_fqn, member, out_qn);
}

/* Do the directory segments of qn (between the project root segment and the
 * file stem, i.e. before offset tail = ".<Class>.<member>") end with every
 * segment of the namespace fqn[0, ns_len)? Case-insensitive: App\\Http is
 * conventionally app/Http. An empty namespace is trivially aligned. */
static bool namespace_matches_dirs(const char *qn, int tail, const char *fqn, size_t ns_len) {
    const char *root_dot = strchr(qn, '.');
    const char *stem_end = qn + tail;
    if (!root_dot || stem_end <= root_dot) {
        return false;
    }
    const char *dir_end = stem_end - SKIP_ONE; /* back over the file stem */
    while (*dir_end != '.') {
        dir_end--;
    }
    size_t ns_end = ns_len;
    while (ns_end > 0) {
        size_t ns_start = ns_end;
        while (ns_start > 0 && fqn[ns_start - SKIP_ONE] != '\\') {
            ns_start--;
        }
        if (dir_end <= root_dot) {
            return false; /* more namespace segments than directories */
        }
        const char *dir_start = dir_end;
        while (dir_start[-SKIP_ONE] != '.') {
            dir_start--;
        }
        if (!seg_eq_nocase(fqn + ns_start, ns_end - ns_start, dir_start,
                           (size_t)(dir_end - dir_start))) {
            return false;
        }
        dir_end = dir_start - SKIP_ONE;
        ns_end = ns_start > 0 ? ns_start - SKIP_ONE : 0;
    }
    return true;
}

/* Namespace/path alignment: the one "<Class>.<member>" candidate that sits
 * where its namespace says. NULL when none or several do. */
static const char *aligned_member_qn(const cbm_registry_t *r, const char *fqn, size_t ns_len,
                                     const char *cls, size_t cls_len, const char *member) {
    const qn_array_t *arr = cbm_ht_get(r->by_name, member);
    const char *match = NULL;
    for (int i = 0; arr && i < arr->count; i++) {
        const char *qn = arr->items[i];
        int tail = qn_class_member_tail(qn, cls, cls_len, member);
        if (tail == CBM_NOT_FOUND || !namespace_matches_dirs(qn, tail, fqn, ns_len)) {
            continue;
        }
        if (match) {
            return NULL; /* ambiguous */
        }
        match = qn;
    }
    return match;
}

cbm_resolution_t cbm_registry_resolve_handler(const cbm_registry_t *r, const char *handler_ref,
                                              const char *module_qn, const char **import_map_keys,
                                              const char **import_map_vals, int import_map_count,
                                              const cbm_gbuf_t *gbuf) {
    const char *sep = handler_ref ? strstr(handler_ref, "::") : NULL;
    if (!r || !sep) {
        return cbm_registry_resolve(r, handler_ref, module_qn, import_map_keys, import_map_vals,
                                    import_map_count);
    }
    const char *member = sep + PAIR_LEN;
    size_t fqn_len = (size_t)(sep - handler_ref);
    size_t ns_len = fqn_len;
    while (ns_len > 0 && handler_ref[ns_len - SKIP_ONE] != '\\') {
        ns_len--;
    }
    const char *cls = handler_ref + ns_len;
    size_t cls_len = fqn_len - ns_len;
    ns_len = ns_len > 0 ? ns_len - SKIP_ONE : 0; /* drop the separator before the class */
    if (cls_len == 0 || !member[0]) {
        return empty_result();
    }
    const char *qn = NULL;
    switch (psr4_member_qn(gbuf, handler_ref, fqn_len, member, &qn)) {
    case CBM_PSR4_MEMBER_RESOLVED:
        return (cbm_resolution_t){qn, "php_psr4", CONF_PSR4_MEMBER, REG_RESOLVED};
    case CBM_PSR4_MEMBER_UNRESOLVED:
        /* A covered class lives where PSR-4 says or nowhere; a namespace-
         * mirroring folder elsewhere would be a guessed edge (#1186). */
        return empty_result();
    case CBM_PSR4_MEMBER_NOT_COVERED:
        break;
    }
    qn = aligned_member_qn(r, handler_ref, ns_len, cls, cls_len, member);
    if (qn) {
        return (cbm_resolution_t){qn, "php_namespace_path", CONF_NAMESPACE_PATH_MEMBER,
                                  REG_RESOLVED};
    }
    return empty_result();
}

cbm_resolution_t cbm_registry_resolve_lineage(const cbm_registry_t *r, const char *callee_name,
                                              const char *module_qn, const char **import_map_keys,
                                              const char **import_map_vals, int import_map_count) {
    if (!r || !callee_name) {
        return empty_result();
    }
    /* Relation-permitting variant for SQL FROM/JOIN lineage usages ONLY.
     * Deliberately uncached: the per-file cache is keyed by bare callee_name
     * and stores the relation-vetoed answer of the default variant — sharing
     * it would poison one variant with the other's semantics. SQL files hold
     * few distinct relation refs, so the chain walk stays cheap. */
    return registry_resolve_chain(r, callee_name, module_qn, import_map_keys, import_map_vals,
                                  import_map_count);
}

/* ── Fuzzy Resolve ──────────────────────────────────────────────── */

/* Filter candidates by import reachability. Returns count of reachable. */
static int filter_import_reachable(const char **candidates, int count, const char **import_vals,
                                   int import_count, const char **out, int max_out) {
    int n = 0;
    for (int i = 0; i < count && n < max_out; i++) {
        if (is_import_reachable(candidates[i], import_vals, import_count)) {
            out[n++] = candidates[i];
        }
    }
    return n;
}

cbm_fuzzy_result_t cbm_registry_fuzzy_resolve(const cbm_registry_t *r, const char *callee_name,
                                              const char *module_qn, const char **import_map_keys,
                                              const char **import_map_vals, int import_map_count) {
    (void)import_map_keys;
    cbm_fuzzy_result_t no_match = {{0}, false};
    if (!r || !callee_name) {
        return no_match;
    }

    /* Extract simple name (last segment after dots) */
    const char *lookup = simple_name(callee_name);
    qn_array_t *arr = cbm_ht_get(r->by_name, lookup);
    if (!arr || arr->count == 0) {
        return no_match;
    }
    if (arr->count > REG_MAX_CANDIDATES) {
        return no_match; /* unresolvably ambiguous — see REG_MAX_CANDIDATES */
    }

    bool have_imports = (import_map_vals && import_map_count > 0);

    /* Single candidate */
    if (arr->count == SKIP_ONE) {
        double conf = CONF_FUZZY_SINGLE;
        if (have_imports &&
            !is_import_reachable(arr->items[0], import_map_vals, import_map_count)) {
            conf *= DEFAULT_CONFIDENCE;
        }
        return (cbm_fuzzy_result_t){{arr->items[0], "fuzzy", conf, REG_RESOLVED}, true};
    }

    /* Multiple candidates: filter by import reachability */
    const char *filtered[CBM_SZ_256];
    int fcount = arr->count;
    const char **fptr = (const char **)arr->items;

    if (have_imports) {
        fcount = filter_import_reachable((const char **)arr->items, arr->count, import_map_vals,
                                         import_map_count, filtered, CBM_SZ_256);
        fptr = filtered;
    }

    if (fcount == 0) {
        /* No import-reachable — use originals with penalty */
        const char *best = best_by_import_distance(r, (const char **)arr->items, qn_test_flags(arr),
                                                   qn_lang_flags(arr), arr->count, module_qn);
        if (!best) {
            return no_match;
        }
        return (cbm_fuzzy_result_t){
            {best, "fuzzy",
             candidate_count_penalty(CONF_FUZZY_MULTI * REG_HALF_PENALTY, arr->count), arr->count},
            true};
    }
    if (fcount == SKIP_ONE) {
        return (cbm_fuzzy_result_t){
            {fptr[0], "fuzzy", candidate_count_penalty(CONF_FUZZY_SINGLE, arr->count), arr->count},
            true};
    }
    const bool all = fptr == (const char **)arr->items;
    const char *best = best_by_import_distance(r, fptr, all ? qn_test_flags(arr) : NULL,
                                               all ? qn_lang_flags(arr) : NULL, fcount, module_qn);
    if (!best) {
        return no_match;
    }
    return (cbm_fuzzy_result_t){
        {best, "fuzzy", candidate_count_penalty(CONF_FUZZY_MULTI, fcount), fcount}, true};
}

/* ── FindEndingWith ─────────────────────────────────────────────── */

struct few_ctx {
    const char *target; /* ".suffix" */
    size_t target_len;
    const char **results;
    int count;
    int cap;
};

static void few_scan(const char *key, void *value, void *ud) {
    (void)value;
    struct few_ctx *ctx = ud;
    size_t klen = strlen(key);
    if (klen >= ctx->target_len && strcmp(key + klen - ctx->target_len, ctx->target) == 0) {
        if (ctx->count >= ctx->cap) {
            ctx->cap = ctx->cap ? ctx->cap * PAIR_LEN : REG_INIT_CAP;
            ctx->results = safe_realloc(ctx->results, (size_t)ctx->cap * sizeof(char *));
        }
        ctx->results[ctx->count++] = key;
    }
}

int cbm_registry_find_ending_with(const cbm_registry_t *r, const char *suffix, const char ***out) {
    if (!r || !suffix || !out) {
        if (out) {
            *out = NULL;
        }
        return 0;
    }

    /* Build ".suffix" target */
    size_t slen = strlen(suffix);
    char *target = malloc(slen + REG_SUFFIX_ALLOC);
    target[0] = '.';
    memcpy(target + SKIP_ONE, suffix, slen + SKIP_ONE);

    struct few_ctx ctx = {target, slen + SKIP_ONE, NULL, 0, 0};
    cbm_ht_foreach(r->exact, few_scan, &ctx);

    free(target);
    *out = ctx.results;
    return ctx.count;
}

/* Public wrapper for is_import_reachable (testing). */
bool cbm_registry_is_import_reachable(const char *candidate_qn, const char **import_vals,
                                      int import_count) {
    return is_import_reachable(candidate_qn, import_vals, import_count);
}
