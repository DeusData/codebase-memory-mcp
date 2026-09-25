/*
 * pass_configlink.c — Config ↔ Code linking strategies (pre-dump pass).
 *
 * Three strategies link config files to code symbols:
 *   1. Key→Symbol: normalized config key matches code function/variable name
 *   2. Dep→Import: package manifest dependency matches IMPORTS edge target
 *   3. File→Ref: source code string literal references config file path
 *
 * Operates on the graph buffer before dump to .db file.
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/compat.h"

#include <stdint.h>
#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "foundation/platform.h" /* cbm_default_worker_count */
#include "pipeline/worker_pool.h"
#include "foundation/compat_regex.h"

/* ── Config link confidence scores ───────────────────────────────── */
/* Strategy 1: Key→Symbol matching */
#define CONF_KEY_EXACT 0.85
#define CONF_KEY_SUBSTRING 0.75
/* Strategy 2: Dep→Import matching */
#define CONF_DEP_EXACT 0.95
#define CONF_DEP_QN_SUBSTR 0.80
/* Strategy 3: File→Ref matching */
#define CONF_FILE_FULLPATH 0.90
#define CONF_FILE_BASENAME 0.70

/* ── Manifest / dep section tables ──────────────────────────────── */

static bool is_manifest_file(const char *basename) {
    static const char *names[] = {"Cargo.toml",       "package.json",  "go.mod",
                                  "requirements.txt", "Gemfile",       "build.gradle",
                                  "pom.xml",          "composer.json", NULL};
    for (int i = 0; names[i]; i++) {
        if (strcmp(basename, names[i]) == 0) {
            return true;
        }
    }
    return false;
}

static bool is_dep_section(const char *s) {
    static const char *secs[] = {"dependencies",     "devdependencies",    "peerdependencies",
                                 "dev-dependencies", "build-dependencies", NULL};
    for (int i = 0; secs[i]; i++) {
        if (cbm_strcasestr(s, secs[i]) != NULL) {
            return true;
        }
    }
    return false;
}

/* ── Strategy 1: Config Key → Code Symbol ───────────────────────── */

/* Canonical candidate order (determinism). Both collectors below fill a
 * fixed-capacity array and stop at max_out; the label indexes they walk are
 * gbuf insertion order = parallel-extraction merge order, which varies run to
 * run. On a repo with more candidates than the cap, sorting by a pure content
 * key first is what keeps the surviving set — and therefore the emitted
 * CONFIGURES edges — a function of the inputs rather than of worker
 * scheduling. Tie-breaks stay content-only: node ids are handed out in merge
 * order, so an id tie-break belongs in no canonical comparator. */
enum {
    CANON_CMP_LESS = -1,   /* qsort: left sorts before right */
    CANON_CMP_GREATER = 1, /* qsort: left sorts after right */
    CANON_CAP_BUF = 32     /* decimal rendering of a cap value */
};

static int cmp_node_ptr_canonical(const void *pa, const void *pb) {
    const cbm_gbuf_node_t *a = *(const cbm_gbuf_node_t *const *)pa;
    const cbm_gbuf_node_t *b = *(const cbm_gbuf_node_t *const *)pb;
    const char *qa = a->qualified_name ? a->qualified_name : "";
    const char *qb = b->qualified_name ? b->qualified_name : "";
    int r = strcmp(qa, qb);
    if (r != 0) {
        return r;
    }
    const char *fa = a->file_path ? a->file_path : "";
    const char *fb = b->file_path ? b->file_path : "";
    r = strcmp(fa, fb);
    if (r != 0) {
        return r;
    }
    if (a->start_line != b->start_line) {
        return a->start_line < b->start_line ? CANON_CMP_LESS : CANON_CMP_GREATER;
    }
    const char *na = a->name ? a->name : "";
    const char *nb = b->name ? b->name : "";
    return strcmp(na, nb);
}

/* A filled-to-capacity collector dropped candidates; say so rather than
 * truncating silently. */
static void log_candidate_truncation(const char *side, int cap) {
    char cap_buf[CANON_CAP_BUF];
    snprintf(cap_buf, sizeof(cap_buf), "%d", cap);
    cbm_log_info("configlinker.truncated", "side", side, "cap", cap_buf);
}

/* Heap copy of `nodes` sorted by cmp_node_ptr_canonical. Returns NULL (and
 * leaves the caller on the unsorted borrowed array) only on allocation
 * failure, which degrades determinism but never correctness. */
static const cbm_gbuf_node_t **canonical_node_copy(const cbm_gbuf_node_t *const *nodes, int count) {
    if (!nodes || count <= 0) {
        return NULL;
    }
    const cbm_gbuf_node_t **sorted = malloc((size_t)count * sizeof(*sorted));
    if (!sorted) {
        return NULL;
    }
    memcpy(sorted, nodes, (size_t)count * sizeof(*sorted));
    qsort(sorted, (size_t)count, sizeof(*sorted), cmp_node_ptr_canonical);
    return sorted;
}

typedef struct {
    int64_t node_id;
    char normalized[CBM_SZ_256];
    char name[CBM_SZ_256];
} config_entry_t;

/* Collect config Variable nodes with ≥2 tokens, each ≥3 chars. */
static int collect_config_entries(const cbm_gbuf_node_t *const *vars, int var_count,
                                  config_entry_t *out, int max_out) {
    int n = 0;
    const cbm_gbuf_node_t **sorted = canonical_node_copy(vars, var_count);
    if (sorted) {
        vars = sorted;
    }
    for (int i = 0; i < var_count && n < max_out; i++) {
        if (!cbm_has_config_extension(vars[i]->file_path)) {
            continue;
        }

        char norm[CBM_SZ_256];
        int tokens = cbm_normalize_config_key(vars[i]->name, norm, sizeof(norm));
        if (tokens < PAIR_LEN) {
            continue;
        }

        /* Check all tokens ≥3 chars */
        bool all_long = true;
        const char *p = norm;
        while (*p) {
            const char *end = strchr(p, '_');
            size_t tlen = end ? (size_t)(end - p) : strlen(p);
            if (tlen < CBM_SZ_3) {
                all_long = false;
                break;
            }
            p = end ? end + SKIP_ONE : p + tlen;
        }
        if (!all_long) {
            continue;
        }

        out[n].node_id = vars[i]->id;
        snprintf(out[n].normalized, sizeof(out[n].normalized), "%s", norm);
        snprintf(out[n].name, sizeof(out[n].name), "%s", vars[i]->name);
        n++;
    }
    if (n == max_out) {
        log_candidate_truncation("config", max_out);
    }
    free((void *)sorted);
    return n;
}

/* Collect code nodes (Function/Variable/Class/Struct) not from config files. */
typedef struct {
    int64_t node_id;
    char normalized[CBM_SZ_256];
} code_entry_t;

static int collect_code_entries(cbm_gbuf_t *gb, code_entry_t *out, int max_out) {
    int n = 0;
    /* "Struct" alongside "Class": a config key may name a Go/Rust/Swift/D struct
     * type, which is now labelled "Struct" — keep it linkable. */
    static const char *labels[] = {"Function", "Variable", "Class", "Struct", NULL};

    for (int li = 0; labels[li] && n < max_out; li++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(gb, labels[li], &nodes, &count) != 0) {
            continue;
        }

        /* Canonical order before the cap — see cmp_node_ptr_canonical. The cap
         * spans the whole label list, so a later label can be cut mid-group;
         * sorting per group keeps that cut a pure function of content. */
        const cbm_gbuf_node_t **sorted = canonical_node_copy(nodes, count);
        const cbm_gbuf_node_t *const *scan = sorted ? sorted : nodes;

        for (int i = 0; i < count && n < max_out; i++) {
            if (cbm_has_config_extension(scan[i]->file_path)) {
                continue;
            }

            char norm[CBM_SZ_256];
            int tokens = cbm_normalize_config_key(scan[i]->name, norm, sizeof(norm));
            if (tokens == 0 || norm[0] == '\0') {
                continue;
            }

            out[n].node_id = scan[i]->id;
            snprintf(out[n].normalized, sizeof(out[n].normalized), "%s", norm);
            n++;
        }
        /* gbuf data is borrowed — only the sorted copy is owned */
        free((void *)sorted);
    }
    if (n == max_out) {
        log_candidate_truncation("code", max_out);
    }
    return n;
}

/* The key x symbol comparison of strategy 1, per config key: every pair's
 * verdict depends only on its two strings, so rows run in parallel and the
 * edges are inserted afterwards in the original (config, code) order. One
 * thread compared up to 4,096 x 8,192 pairs -- configlink's 0.93 s on the Go
 * corpus (profile, 2026-09-17). A pair whose lengths rule both verdicts out
 * skips the string calls: an exact match needs equal lengths, a substring one
 * no longer than the code key. */
typedef struct {
    int co;
    double confidence;
} key_match_t;

typedef struct {
    key_match_t *items;
    int count;
    int cap;
} key_match_list_t;

typedef struct {
    const config_entry_t *config;
    const size_t *config_len;
    int config_count;
    const code_entry_t *code;
    const size_t *code_len;
    int code_count;
    key_match_list_t *lists;
    _Atomic int next;
    _Atomic bool failed;
} key_match_job_t;

static double key_pair_confidence(const char *config_norm, size_t config_len, const char *code_norm,
                                  size_t code_len) {
    if (config_len > code_len) {
        return 0.0;
    }
    if (config_len == code_len && memcmp(config_norm, code_norm, code_len) == 0) {
        return CONF_KEY_EXACT; /* Exact match */
    }
    if (strstr(code_norm, config_norm) != NULL) {
        return CONF_KEY_SUBSTRING; /* Substring match */
    }
    return 0.0;
}

static void key_match_worker(int worker_id, void *arg) {
    (void)worker_id;
    key_match_job_t *job = (key_match_job_t *)arg;
    while (!atomic_load_explicit(&job->failed, memory_order_relaxed)) {
        int ci = atomic_fetch_add_explicit(&job->next, SKIP_ONE, memory_order_relaxed);
        if (ci >= job->config_count) {
            break;
        }
        key_match_list_t *list = &job->lists[ci];
        for (int co = 0; co < job->code_count; co++) {
            double confidence = key_pair_confidence(job->config[ci].normalized, job->config_len[ci],
                                                    job->code[co].normalized, job->code_len[co]);
            if (confidence <= 0.0) {
                continue;
            }
            if (list->count == list->cap) {
                int cap = list->cap ? list->cap * PAIR_LEN : CBM_SZ_16;
                key_match_t *grown =
                    cbm_realloc(CBM_MEM_CLASS_OTHER, list->items, (size_t)cap * sizeof(*grown));
                if (!grown) {
                    atomic_store_explicit(&job->failed, true, memory_order_relaxed);
                    return;
                }
                list->items = grown;
                list->cap = cap;
            }
            list->items[list->count].co = co;
            list->items[list->count].confidence = confidence;
            list->count++;
        }
    }
}

static int strategy_key_symbols(cbm_gbuf_t *gb) {
    /* Get all Variable nodes */
    const cbm_gbuf_node_t **vars = NULL;
    int var_count = 0;
    if (cbm_gbuf_find_by_label(gb, "Variable", &vars, &var_count) != 0) {
        return 0;
    }

    config_entry_t config_entries[CBM_SZ_4K];
    int config_count = collect_config_entries(vars, var_count, config_entries, CBM_SZ_4K);

    if (config_count == 0) {
        return 0;
    }

    code_entry_t code_entries[CBM_SZ_8K];
    int code_count = collect_code_entries(gb, code_entries, CBM_SZ_8K);

    int edge_count = 0;

    size_t *config_len = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)config_count * sizeof(size_t));
    size_t *code_len =
        cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)(code_count > 0 ? code_count : 1) * sizeof(size_t));
    key_match_list_t *lists =
        cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)config_count * sizeof(key_match_list_t));
    bool parallel_ok = config_len && code_len && lists;
    if (parallel_ok) {
        for (int ci = 0; ci < config_count; ci++) {
            config_len[ci] = strlen(config_entries[ci].normalized);
        }
        for (int co = 0; co < code_count; co++) {
            code_len[co] = strlen(code_entries[co].normalized);
        }
        key_match_job_t job = {
            .config = config_entries,
            .config_len = config_len,
            .config_count = config_count,
            .code = code_entries,
            .code_len = code_len,
            .code_count = code_count,
            .lists = lists,
        };
        atomic_init(&job.next, 0);
        atomic_init(&job.failed, false);
        int workers = cbm_default_worker_count(false);
        cbm_parallel_for(
            workers, key_match_worker, &job,
            (cbm_parallel_for_opts_t){.max_workers = workers, .force_pthreads = false});
        parallel_ok = !atomic_load_explicit(&job.failed, memory_order_relaxed);
    }
    if (parallel_ok) {
        for (int ci = 0; ci < config_count; ci++) {
            for (int m = 0; m < lists[ci].count; m++) {
                const key_match_t *km = &lists[ci].items[m];
                char props[CBM_SZ_512];
                snprintf(props, sizeof(props),
                         "{\"strategy\":\"key_symbol\",\"confidence\":%.2f,\"config_key\":\"%s\"}",
                         km->confidence, config_entries[ci].name);

                cbm_gbuf_insert_edge(gb, code_entries[km->co].node_id, config_entries[ci].node_id,
                                     "CONFIGURES", props);
                edge_count++;
            }
        }
    }
    for (int ci = 0; lists && ci < config_count; ci++) {
        cbm_free(CBM_MEM_CLASS_OTHER, lists[ci].items);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, lists);
    cbm_free(CBM_MEM_CLASS_OTHER, config_len);
    cbm_free(CBM_MEM_CLASS_OTHER, code_len);
    if (parallel_ok) {
        return edge_count;
    }

    /* Allocation failed: the one-thread comparison, as before. */
    for (int ci = 0; ci < config_count; ci++) {
        for (int co = 0; co < code_count; co++) {
            double confidence = 0.0;

            if (strcmp(config_entries[ci].normalized, code_entries[co].normalized) == 0) {
                /* Exact match */
                confidence = CONF_KEY_EXACT;
            } else if (strstr(code_entries[co].normalized, config_entries[ci].normalized) != NULL) {
                /* Substring match */
                confidence = CONF_KEY_SUBSTRING;
            }

            if (confidence > 0.0) {
                char props[CBM_SZ_512];
                snprintf(props, sizeof(props),
                         "{\"strategy\":\"key_symbol\",\"confidence\":%.2f,\"config_key\":\"%s\"}",
                         confidence, config_entries[ci].name);

                cbm_gbuf_insert_edge(gb, code_entries[co].node_id, config_entries[ci].node_id,
                                     "CONFIGURES", props);
                edge_count++;
            }
        }
    }

    return edge_count;
}

/* ── Strategy 2: Dependency → Import ────────────────────────────── */

typedef struct {
    int64_t node_id;
    char name[CBM_SZ_256];
} dep_entry_t;

/* Extract basename from a file path. */
static const char *path_basename(const char *path) {
    if (!path) {
        return "";
    }
    const char *slash = strrchr(path, '/');
    return slash ? slash + SKIP_ONE : path;
}

/* Check if a Cargo.toml QN contains a dependency section in any dotted part. */
static bool is_cargo_dep_section(const char *qn) {
    char qn_copy[CBM_SZ_512];
    snprintf(qn_copy, sizeof(qn_copy), "%s", qn);
    char *saveptr = NULL;
    char *part = strtok_r(qn_copy, ".", &saveptr);
    while (part) {
        char lower[CBM_SZ_128];
        size_t plen = strlen(part);
        if (plen >= sizeof(lower)) {
            plen = sizeof(lower) - SKIP_ONE;
        }
        for (size_t j = 0; j < plen; j++) {
            lower[j] = (char)tolower((unsigned char)part[j]);
        }
        lower[plen] = '\0';

        static const char *dep_secs[] = {"dependencies",       "devdependencies",
                                         "peerdependencies",   "dev-dependencies",
                                         "build-dependencies", NULL};
        for (int k = 0; dep_secs[k]; k++) {
            if (strcmp(lower, dep_secs[k]) == 0) {
                return true;
            }
        }
        part = strtok_r(NULL, ".", &saveptr);
    }
    return false;
}

static int collect_manifest_deps(const cbm_gbuf_node_t *const *vars, int var_count,
                                 dep_entry_t *out, int max_out) {
    int n = 0;
    for (int i = 0; i < var_count && n < max_out; i++) {
        const char *base = path_basename(vars[i]->file_path);
        if (!is_manifest_file(base)) {
            continue;
        }

        bool is_dep = vars[i]->qualified_name && is_dep_section(vars[i]->qualified_name);

        if (!is_dep && strcmp(base, "Cargo.toml") == 0 && vars[i]->qualified_name) {
            is_dep = is_cargo_dep_section(vars[i]->qualified_name);
        }

        if (is_dep) {
            out[n].node_id = vars[i]->id;
            snprintf(out[n].name, sizeof(out[n].name), "%s", vars[i]->name);
            n++;
        }
    }
    return n;
}

/* Lowercase a string into buf. */
static void lowercase_into(char *buf, size_t bufsize, const char *src) {
    size_t len = src ? strlen(src) : 0;
    for (size_t j = 0; j < len && j < bufsize - SKIP_ONE; j++) {
        buf[j] = (char)tolower((unsigned char)src[j]);
    }
    buf[len < bufsize ? len : bufsize - SKIP_ONE] = '\0';
}

/* One IMPORTS edge prepared for matching: its endpoints resolved and its
 * target's name and qualified name lowercased ONCE (same truncation as before:
 * 255 and 511 bytes). Resolving both nodes and lowercasing both strings inside
 * the dep x import loop was the whole cost of configlink -- 0.9 s on the Go
 * corpus, and cbm_gbuf_find_by_id's x3.1 super-linear scaling (waste
 * sanitizer, 2026-09-17). */
typedef struct {
    int64_t source_id;
    const char *target_lower; /* into one shared block */
    const char *qn_lower;     /* NULL when the target has no qualified name */
} dep_import_t;

/* Match a dep name (lowercased) against a prepared import target.
 * Returns confidence > 0 on match, 0 on no match. */
static double match_dep_to_import(const dep_import_t *imp, const char *dep_lower) {
    if (strcmp(imp->target_lower, dep_lower) == 0) {
        return CONF_DEP_EXACT;
    }
    if (imp->qn_lower && strstr(imp->qn_lower, dep_lower) != NULL) {
        return CONF_DEP_QN_SUBSTR;
    }
    return 0.0;
}

/* Resolve and lowercase every IMPORTS edge whose endpoints both exist, in edge
 * order. Returns the count (0 with *out NULL on allocation failure). */
static int prepare_dep_imports(cbm_gbuf_t *gb, const cbm_gbuf_edge_t *const *imports,
                               int import_count, dep_import_t **out, char **block_out) {
    *out = NULL;
    *block_out = NULL;
    enum { NAME_CAP = CBM_SZ_256, QN_CAP = CBM_SZ_512 };
    size_t bytes = 0;
    for (int ii = 0; ii < import_count; ii++) {
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(gb, imports[ii]->target_id);
        const cbm_gbuf_node_t *source =
            target ? cbm_gbuf_find_by_id(gb, imports[ii]->source_id) : NULL;
        if (!target || !source) {
            continue;
        }
        size_t nlen = target->name ? strlen(target->name) : 0;
        bytes += (nlen < NAME_CAP ? nlen : NAME_CAP - SKIP_ONE) + SKIP_ONE;
        if (target->qualified_name) {
            size_t qlen = strlen(target->qualified_name);
            bytes += (qlen < QN_CAP ? qlen : QN_CAP - SKIP_ONE) + SKIP_ONE;
        }
    }
    dep_import_t *items = cbm_alloc(
        CBM_MEM_CLASS_OTHER, (size_t)(import_count > 0 ? import_count : 1) * sizeof(dep_import_t));
    char *block = cbm_alloc(CBM_MEM_CLASS_OTHER, bytes > 0 ? bytes : 1);
    if (!items || !block) {
        cbm_free(CBM_MEM_CLASS_OTHER, items);
        cbm_free(CBM_MEM_CLASS_OTHER, block);
        return 0;
    }
    int n = 0;
    char *w = block;
    for (int ii = 0; ii < import_count; ii++) {
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(gb, imports[ii]->target_id);
        const cbm_gbuf_node_t *source =
            target ? cbm_gbuf_find_by_id(gb, imports[ii]->source_id) : NULL;
        if (!target || !source) {
            continue;
        }
        size_t nlen = target->name ? strlen(target->name) : 0;
        size_t ncap = (nlen < NAME_CAP ? nlen : NAME_CAP - SKIP_ONE) + SKIP_ONE;
        lowercase_into(w, ncap, target->name);
        items[n].target_lower = w;
        w += ncap;
        items[n].qn_lower = NULL;
        if (target->qualified_name) {
            size_t qlen = strlen(target->qualified_name);
            size_t qcap = (qlen < QN_CAP ? qlen : QN_CAP - SKIP_ONE) + SKIP_ONE;
            lowercase_into(w, qcap, target->qualified_name);
            items[n].qn_lower = w;
            w += qcap;
        }
        items[n].source_id = source->id;
        n++;
    }
    *out = items;
    *block_out = block;
    return n;
}

static int strategy_dep_imports(cbm_gbuf_t *gb) {
    const cbm_gbuf_node_t **vars = NULL;
    int var_count = 0;
    if (cbm_gbuf_find_by_label(gb, "Variable", &vars, &var_count) != 0) {
        return 0;
    }

    dep_entry_t deps[CBM_SZ_2K];
    int dep_count = collect_manifest_deps(vars, var_count, deps, CBM_SZ_2K);

    if (dep_count == 0) {
        return 0;
    }

    /* Get all IMPORTS edges */
    const cbm_gbuf_edge_t **imports = NULL;
    int import_count = 0;
    if (cbm_gbuf_find_edges_by_type(gb, "IMPORTS", &imports, &import_count) != 0) {
        return 0;
    }

    int edge_count = 0;
    dep_import_t *prepared = NULL;
    char *prepared_block = NULL;
    int prepared_count = prepare_dep_imports(gb, imports, import_count, &prepared, &prepared_block);

    for (int di = 0; di < dep_count && prepared; di++) {
        char dep_lower[CBM_SZ_256];
        lowercase_into(dep_lower, sizeof(dep_lower), deps[di].name);

        for (int ii = 0; ii < prepared_count; ii++) {
            double confidence = match_dep_to_import(&prepared[ii], dep_lower);
            if (confidence > 0.0) {
                char props[CBM_SZ_512];
                snprintf(
                    props, sizeof(props),
                    "{\"strategy\":\"dependency_import\",\"confidence\":%.2f,\"dep_name\":\"%s\"}",
                    confidence, deps[di].name);

                cbm_gbuf_insert_edge(gb, prepared[ii].source_id, deps[di].node_id, "CONFIGURES",
                                     props);
                edge_count++;
            }
        }
    }

    /* gbuf data is borrowed; only the prepared copies are owned */
    cbm_free(CBM_MEM_CLASS_OTHER, prepared);
    cbm_free(CBM_MEM_CLASS_OTHER, prepared_block);
    return edge_count;
}

/* ── Strategy 3: Config File Path → Code String Reference ───────── */

typedef struct {
    const char *key;
    int64_t node_id;
} path_map_t;

/* Match a ref_path against config module maps. Returns target node_id (0 = no match). */

int cbm_pipeline_pass_configlink(cbm_pipeline_ctx_t *ctx) {
    cbm_gbuf_t *gb = ctx->gbuf;
    /* Early exit: check if any config files exist in the project. */
    bool has_config = false;

    const cbm_gbuf_node_t **vars_check = NULL;
    int var_check_count = 0;
    if (!has_config && cbm_gbuf_find_by_label(gb, "Variable", &vars_check, &var_check_count) == 0) {
        for (int i = 0; i < var_check_count; i++) {
            if (cbm_has_config_extension(vars_check[i]->file_path)) {
                has_config = true;
                break;
            }
        }
    }

    if (!has_config) {
        const cbm_gbuf_node_t **mods_check = NULL;
        int mod_check_count = 0;
        if (cbm_gbuf_find_by_label(gb, "Module", &mods_check, &mod_check_count) == 0) {
            for (int i = 0; i < mod_check_count; i++) {
                if (cbm_has_config_extension(mods_check[i]->file_path)) {
                    has_config = true;
                    break;
                }
            }
        }
    }

    if (!has_config) {
        cbm_log_info("configlinker.skip", "reason", "no_config_files");
        return 0;
    }

    char buf1[CBM_SZ_16];
    char buf2[CBM_SZ_16];
    char buf3[CBM_SZ_16];
    char buf4[CBM_SZ_16];

    int key_edges = strategy_key_symbols(gb);
    snprintf(buf1, sizeof(buf1), "%d", key_edges);
    cbm_log_info("configlinker.strategy", "name", "key_symbol", "edges", buf1);

    int dep_edges = strategy_dep_imports(gb);
    snprintf(buf2, sizeof(buf2), "%d", dep_edges);
    cbm_log_info("configlinker.strategy", "name", "dep_import", "edges", buf2);

    int ref_edges = 0;
    if (ctx->repo_path) {
        /* File refs: no longer reads from disk — config file path matching
         * is handled by CONFIGURES edges created during resolution. */
        ref_edges = 0;
    }
    snprintf(buf3, sizeof(buf3), "%d", ref_edges);
    cbm_log_info("configlinker.strategy", "name", "file_ref", "edges", buf3);

    snprintf(buf4, sizeof(buf4), "%d", key_edges + dep_edges + ref_edges);
    cbm_log_info("configlinker.done", "total", buf4);

    return key_edges + dep_edges + ref_edges;
}
