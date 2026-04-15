
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
#define REG_FULL_CONF 1.0
#define REG_HALF_PENALTY 0.5

#define DEFAULT_CONFIDENCE 0.5
#include "pipeline/pipeline.h"
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

/* Extract last dot-separated segment from a QN. Returns pointer into s. */
static const char *simple_name(const char *qn) {
    const char *last = strrchr(qn, '.');
    return last ? last + SKIP_ONE : qn;
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

/* Check if candidate's module prefix appears in import map values.
 * Uses stack buffer to avoid malloc/free per call in hot resolution loop. */
static bool is_import_reachable(const char *candidate_qn, const char **import_vals,
                                int import_count) {
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
    for (int i = 0; i < import_count; i++) {
        if (strstr(cand_mod, import_vals[i]) || strstr(import_vals[i], cand_mod)) {
            return true;
        }
    }
    return false;
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
struct ims_ctx {
    const char *resolved_dot; /* "proj.other." */
    size_t resolved_dot_len;
    const char *dot_suffix; /* ".Foo" */
    size_t dot_suffix_len;
    const char *found_key;
};

static void ims_scan(const char *key, void *value, void *ud) {
    (void)value;
    struct ims_ctx *ctx = ud;
    if (ctx->found_key) {
        return; /* already found */
    }
    size_t klen = strlen(key);
    if (klen >= ctx->resolved_dot_len + ctx->dot_suffix_len &&
        strncmp(key, ctx->resolved_dot, ctx->resolved_dot_len) == 0 &&
        strcmp(key + klen - ctx->dot_suffix_len, ctx->dot_suffix) == 0) {
        ctx->found_key = key;
    }
}

/* Strategy 1: Import map lookup (exact → suffix fallback) */
static cbm_resolution_t resolve_import_map(const cbm_registry_t *r, const char *prefix,
                                           const char *suffix, const char **keys, const char **vals,
                                           int map_count) {
    if (!keys || !vals || map_count <= 0) {
        return empty_result();
    }

    /* Find prefix in import map keys */
    const char *resolved = NULL;
    for (int i = 0; i < map_count; i++) {
        if (strcmp(keys[i], prefix) == 0) {
            resolved = vals[i];
            break;
        }
    }
    if (!resolved) {
        return empty_result();
    }

    /* Build candidate: resolved.suffix or just resolved */
    char candidate[CBM_SZ_512];
    if (suffix && suffix[0]) {
        snprintf(candidate, sizeof(candidate), "%s.%s", resolved, suffix);
    } else {
        snprintf(candidate, sizeof(candidate), "%s", resolved);
    }
    /* Use cbm_ht_get_key to get the persistent heap-owned key string */
    const char *stored_key = cbm_ht_get_key(r->exact, candidate);
    if (stored_key) {
        return (cbm_resolution_t){stored_key, "import_map", CONF_IMPORT_MAP, REG_RESOLVED};
    }

    /* import_map_suffix fallback: scan for QNs starting with resolved+"."
     * and ending with "."+suffix */
    if (suffix && suffix[0]) {
        char resolved_dot[CBM_SZ_512];
        char dot_suffix[CBM_SZ_256];
        snprintf(resolved_dot, sizeof(resolved_dot), "%s.", resolved);
        snprintf(dot_suffix, sizeof(dot_suffix), ".%s", suffix);
        struct ims_ctx ctx = {
            .resolved_dot = resolved_dot,
            .resolved_dot_len = strlen(resolved_dot),
            .dot_suffix = dot_suffix,
            .dot_suffix_len = strlen(dot_suffix),
            .found_key = NULL,
        };
        cbm_ht_foreach(r->exact, ims_scan, &ctx);
        if (ctx.found_key) {
            return (cbm_resolution_t){ctx.found_key, "import_map_suffix", CONF_IMPORT_MAP_SUFFIX,
                                      REG_RESOLVED};
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

/* Strategy 3+4: Name lookup + suffix match */
static cbm_resolution_t resolve_name_lookup(const cbm_registry_t *r, const char *callee_name,
                                            const char *module_qn, const char **import_vals,
                                            int import_count) {
    const char *lookup = simple_name(callee_name);
    qn_array_t *arr = cbm_ht_get(r->by_name, lookup);
    if (!arr || arr->count == 0) {
        return empty_result();
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

    /* Split callee: "pkg.Func" → prefix="pkg", suffix="Func" */
    char prefix[CBM_SZ_256] = {0};
    const char *suffix = NULL;
    const char *dot = strchr(callee_name, '.');
    if (dot) {
        size_t plen = dot - callee_name;
        if (plen >= sizeof(prefix)) {
            plen = sizeof(prefix) - SKIP_ONE;
        }
        memcpy(prefix, callee_name, plen);
        prefix[plen] = '\0';
        suffix = dot + SKIP_ONE;
    } else {
        snprintf(prefix, sizeof(prefix), "%s", callee_name);
    }

    /* Strategy 1: import map */
    cbm_resolution_t res =
        resolve_import_map(r, prefix, suffix, import_map_keys, import_map_vals, import_map_count);
    if (res.qualified_name && res.qualified_name[0]) {
        return res;
    }

    /* Strategy 2: same module */
    res = resolve_same_module(r, callee_name, suffix, module_qn);
    if (res.qualified_name && res.qualified_name[0]) {
        return res;
    }

    /* Strategy 3+4: name lookup */
    return resolve_name_lookup(r, callee_name, module_qn, import_map_vals, import_map_count);
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
