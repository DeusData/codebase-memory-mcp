
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
#include "foundation/compat.h" /* CBM_TLS */
#include "foundation/hash_table.h"
#include "foundation/dyn_array.h"
#include "foundation/platform.h"

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

/* Array of QN strings for byName index */
typedef CBM_DYN_ARRAY(char *) qn_array_t;

struct cbm_registry {
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

/* Count common dot-separated prefix segments. */
static int common_prefix_len(const char *a, const char *b) {
    if (!a || !b) {
        return 0;
    }
    int count = 0;
    while (*a && *b) {
        /* Find next segment in each */
        const char *adot = strchr(a, '.');
        const char *bdot = strchr(b, '.');
        size_t alen = adot ? (size_t)(adot - a) : strlen(a);
        size_t blen = bdot ? (size_t)(bdot - b) : strlen(b);
        if (alen != blen || memcmp(a, b, alen) != 0) {
            break;
        }
        count++;
        a += alen + (adot ? SKIP_ONE : 0);
        b += blen + (bdot ? SKIP_ONE : 0);
        if (!adot || !bdot) {
            break;
        }
    }
    return count;
}

enum { REG_TEST_PENALTY = 1000 };

/* Check if a qualified name looks like a test/mock path. */
static bool is_test_qn(const char *qn) {
    if (!qn) {
        return false;
    }
    return (strstr(qn, "Test") != NULL || strstr(qn, "test") != NULL ||
            strstr(qn, "Mock") != NULL || strstr(qn, "mock") != NULL ||
            strstr(qn, "Stub") != NULL || strstr(qn, "stub") != NULL ||
            strstr(qn, "Fake") != NULL || strstr(qn, "fake") != NULL ||
            strstr(qn, "Fixture") != NULL || strstr(qn, "spec") != NULL);
}

/* Score a candidate for tiebreaking. Higher = better.
 * Layer 1: Non-test code preferred over test code (+1000)
 * Layer 2: Namespace proximity via common prefix length (+plen) */
static int candidate_score(const char *candidate_qn, const char *module_qn) {
    int score = 0;
    if (!is_test_qn(candidate_qn)) {
        score += REG_TEST_PENALTY;
    }
    score += common_prefix_len(candidate_qn, module_qn);
    return score;
}

/* Pick candidate with highest composite score (test-deprioritization + namespace proximity). */
static const char *best_by_import_distance(const char **candidates, int count,
                                           const char *module_qn) {
    const char *best = NULL;
    int best_score = CBM_NOT_FOUND;
    for (int i = 0; i < count; i++) {
        int score = candidate_score(candidates[i], module_qn);
        if (score > best_score) {
            best_score = score;
            best = candidates[i];
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

/* Sentinels stored as values in the cache. NULL means "not cached".
 * We need two distinct non-NULL pointers to encode true/false. */
#define REACH_CACHE_TRUE ((void *)(uintptr_t)1)
#define REACH_CACHE_FALSE ((void *)(uintptr_t)2)

static void reach_cache_free_key(const char *key, void *val, void *ud) {
    (void)val;
    (void)ud;
    free((char *)key);
}

void cbm_registry_reach_cache_begin(int estimated_capacity) {
    if (_reach_cache) {
        /* Defensive: caller forgot to call _end. Clear and reuse. */
        cbm_ht_foreach(_reach_cache, reach_cache_free_key, NULL);
        cbm_ht_clear(_reach_cache);
        return;
    }
    if (estimated_capacity < 16)
        estimated_capacity = 16;
    _reach_cache = cbm_ht_create((uint32_t)estimated_capacity);
}

void cbm_registry_reach_cache_end(void) {
    if (!_reach_cache)
        return;
    cbm_ht_foreach(_reach_cache, reach_cache_free_key, NULL);
    cbm_ht_free(_reach_cache);
    _reach_cache = NULL;
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
        char *kdup = strdup(candidate_qn);
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
    free((void *)key);
    free(value);
}

static void free_qn_array(const char *key, void *value, void *ud) {
    (void)ud;
    qn_array_t *arr = value;
    if (arr) {
        for (int i = 0; i < arr->count; i++) {
            free(arr->items[i]);
        }
        cbm_da_free(arr);
        free(arr);
    }
    free((void *)key);
}

void cbm_registry_free(cbm_registry_t *r) {
    if (!r) {
        return;
    }
    cbm_ht_foreach(r->exact, free_label, NULL);
    cbm_ht_free(r->exact);
    cbm_ht_foreach(r->by_name, free_qn_array, NULL);
    cbm_ht_free(r->by_name);
    free(r);
}

/* ── Registration ────────────────────────────────────────────────── */

void cbm_registry_add(cbm_registry_t *r, const char *name, const char *qualified_name,
                      const char *label) {
    (void)name;
    if (!r || !qualified_name || !label) {
        return;
    }

    /* Check for duplicate */
    if (cbm_ht_get(r->exact, qualified_name)) {
        return;
    }

    /* Store in exact map: QN → label */
    cbm_ht_set(r->exact, strdup(qualified_name), strdup(label));

    /* Index by simple name.
     * No array dedup needed: exact-map check above guarantees uniqueness. */
    const char *simple = simple_name(qualified_name);
    qn_array_t *arr = cbm_ht_get(r->by_name, simple);
    if (!arr) {
        arr = calloc(CBM_ALLOC_ONE, sizeof(qn_array_t));
        cbm_ht_set(r->by_name, strdup(simple), arr);
    }
    cbm_da_push(arr, strdup(qualified_name));
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
static cbm_resolution_t resolve_multi_with_imports(const qn_array_t *arr, const char *module_qn,
                                                   const char **import_vals, int import_count) {
    const char *filtered[CBM_SZ_256];
    int fcount = 0;
    for (int i = 0; i < arr->count && fcount < CBM_SZ_256; i++) {
        if (is_import_reachable(arr->items[i], import_vals, import_count)) {
            filtered[fcount++] = arr->items[i];
        }
    }
    if (fcount == SKIP_ONE) {
        double conf = candidate_count_penalty(CONF_SUFFIX_MATCH, arr->count);
        return (cbm_resolution_t){filtered[0], "suffix_match", conf, arr->count};
    }
    if (fcount > SKIP_ONE) {
        const char *best = best_by_import_distance(filtered, fcount, module_qn);
        if (best) {
            double conf = candidate_count_penalty(CONF_SUFFIX_MATCH, fcount);
            return (cbm_resolution_t){best, "suffix_match", conf, fcount};
        }
    }
    /* No import-reachable — use all candidates with penalty */
    const char *best = best_by_import_distance((const char **)arr->items, arr->count, module_qn);
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
        double conf = CONF_UNIQUE_NAME;
        if (import_vals && import_count > 0 &&
            !is_import_reachable(arr->items[0], import_vals, import_count)) {
            conf *= DEFAULT_CONFIDENCE;
        }
        return (cbm_resolution_t){arr->items[0], "unique_name", conf, REG_RESOLVED};
    }

    /* Strategy 4: multiple candidates */
    if (import_vals && import_count > 0) {
        return resolve_multi_with_imports(arr, module_qn, import_vals, import_count);
    }
    const char *best = best_by_import_distance((const char **)arr->items, arr->count, module_qn);
    if (best) {
        double conf = candidate_count_penalty(CONF_SUFFIX_MATCH, arr->count);
        return (cbm_resolution_t){best, "suffix_match", conf, arr->count};
    }
    return empty_result();
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
        const char *best =
            best_by_import_distance((const char **)arr->items, arr->count, module_qn);
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
    const char *best = best_by_import_distance(fptr, fcount, module_qn);
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
