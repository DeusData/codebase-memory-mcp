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
#include "foundation/arena.h"
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

/* Canonical candidate order (determinism). The label indexes the collectors
 * walk are gbuf insertion order = parallel-extraction merge order, which
 * varies run to run. Sorting by a pure content key first keeps the candidate
 * order -- and so the order edges are inserted in -- a function of the inputs
 * rather than of worker scheduling. (The collectors once stopped at a cap,
 * where this order also decided WHICH candidates survived; no cap cuts them
 * now.) Tie-breaks stay content-only: node ids are handed out in merge order,
 * so an id tie-break belongs in no canonical comparator. */
enum {
    CANON_CMP_LESS = -1,  /* qsort: left sorts before right */
    CANON_CMP_GREATER = 1 /* qsort: left sorts after right */
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

/* Infrastructure files: YAML, JSON and HCL (Terraform, OpenTofu, tfvars). A
 * key_symbol pair with either side in one links by exact name and only inside
 * the config file's component (user decision 2026-10-10): in 17 infra
 * repositories 4 of 40 sampled key_symbol edges were right, all of them exact
 * and within one directory tree; the rest were substring and cross-component
 * coincidences. Other config formats keep exact + substring matching. */
static bool cl_is_infra_path(const char *path) {
    static const char *const exts[] = {".yaml", ".yml",  ".json",   ".tf",
                                       ".hcl",  ".tofu", ".tfvars", NULL};
    const char *dot = path ? strrchr(path, '.') : NULL;
    const char *slash = path ? strrchr(path, '/') : NULL;
    if (!dot || (slash && dot < slash)) {
        return false;
    }
    for (int i = 0; exts[i]; i++) {
        if (strcmp(dot, exts[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Length of the config file's component directory: its own directory, or the
 * one around it when that is a conventional config folder; 0 = the repo root. */
static size_t cl_component_len(const char *path) {
    static const char *const config_dirs[] = {"config", "configs", "conf", "settings", "etc", NULL};
    const char *slash = path ? strrchr(path, '/') : NULL;
    if (!slash) {
        return 0;
    }
    size_t dir_len = (size_t)(slash - path);
    const char *base = path;
    for (const char *p = path; p < slash; p++) {
        if (*p == '/') {
            base = p + SKIP_ONE;
        }
    }
    size_t base_len = (size_t)(slash - base);
    for (int i = 0; config_dirs[i]; i++) {
        if (base_len == strlen(config_dirs[i]) && strncmp(base, config_dirs[i], base_len) == 0) {
            return base == path ? 0 : (size_t)(base - path) - SKIP_ONE;
        }
    }
    return dir_len;
}

typedef struct {
    int64_t node_id;
    const char *file_path; /* borrowed from the graph buffer for the pass */
    const char *name;      /* borrowed */
    char *normalized;      /* in the strategy's arena */
    size_t len;
    size_t component_len; /* config entries: cl_component_len(file_path) */
    bool infra;
} cl_entry_t;

typedef struct {
    cl_entry_t *items;
    int count;
    int cap;
} cl_entries_t;

static bool cl_entries_push(cl_entries_t *entries, const cbm_gbuf_node_t *node, const char *norm,
                            CBMArena *arena) {
    if (entries->count == entries->cap) {
        int cap = entries->cap ? entries->cap * PAIR_LEN : CBM_SZ_1K;
        cl_entry_t *grown =
            cbm_realloc(CBM_MEM_CLASS_OTHER, entries->items, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        entries->items = grown;
        entries->cap = cap;
    }
    char *normalized = cbm_arena_strdup(arena, norm);
    if (!normalized) {
        return false;
    }
    entries->items[entries->count++] = (cl_entry_t){
        .node_id = node->id,
        .file_path = node->file_path ? node->file_path : "",
        .name = node->name ? node->name : "",
        .normalized = normalized,
        .len = strlen(normalized),
        .component_len = cl_component_len(node->file_path),
        .infra = cl_is_infra_path(node->file_path),
    };
    return true;
}

/* Collect config Variable nodes with ≥2 tokens, each ≥3 chars. */
static bool collect_config_entries(const cbm_gbuf_node_t *const *vars, int var_count,
                                   cl_entries_t *out, CBMArena *arena) {
    const cbm_gbuf_node_t **sorted = canonical_node_copy(vars, var_count);
    if (sorted) {
        vars = sorted;
    }
    bool ok = true;
    for (int i = 0; i < var_count && ok; i++) {
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
        if (all_long) {
            ok = cl_entries_push(out, vars[i], norm, arena);
        }
    }
    free((void *)sorted);
    return ok;
}

static bool cl_is_hcl_path(const char *path) {
    static const char *const exts[] = {".tf", ".hcl", ".tofu", ".tfvars", NULL};
    const char *dot = path ? strrchr(path, '.') : NULL;
    for (int i = 0; dot && exts[i]; i++) {
        if (strcmp(dot, exts[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* The name an HCL symbol matches a config key by. A config value sets a
 * Terraform/OpenTofu `variable` block, whose Class is named `variable.<name>`:
 * it matches by <name>. Outputs, modules, resources and a .tfvars file's own
 * assignments are not what a config key sets; they take no part (NULL). */
static const char *cl_hcl_variable_name(const cbm_gbuf_node_t *node) {
    static const char prefix[] = "variable.";
    if (!node->label || strcmp(node->label, "Class") != 0 || !node->name ||
        strncmp(node->name, prefix, sizeof(prefix) - SKIP_ONE) != 0) {
        return NULL;
    }
    return node->name + sizeof(prefix) - SKIP_ONE;
}

/* Collect code nodes (Function/Variable/Class/Struct) not from config files. */
static bool collect_code_entries(cbm_gbuf_t *gb, cl_entries_t *out, CBMArena *arena) {
    /* "Struct" alongside "Class": a config key may name a Go/Rust/Swift/D struct
     * type, which is now labelled "Struct" — keep it linkable. */
    static const char *labels[] = {"Function", "Variable", "Class", "Struct", NULL};

    bool ok = true;
    for (int li = 0; labels[li] && ok; li++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(gb, labels[li], &nodes, &count) != 0) {
            continue;
        }

        /* Canonical order -- see cmp_node_ptr_canonical. */
        const cbm_gbuf_node_t **sorted = canonical_node_copy(nodes, count);
        const cbm_gbuf_node_t *const *scan = sorted ? sorted : nodes;

        for (int i = 0; i < count && ok; i++) {
            if (cbm_has_config_extension(scan[i]->file_path)) {
                continue;
            }

            const char *match_name = scan[i]->name;
            if (cl_is_hcl_path(scan[i]->file_path)) {
                match_name = cl_hcl_variable_name(scan[i]);
                if (!match_name) {
                    continue;
                }
            }

            char norm[CBM_SZ_256];
            int tokens = cbm_normalize_config_key(match_name, norm, sizeof(norm));
            if (tokens == 0 || norm[0] == '\0') {
                continue;
            }
            ok = cl_entries_push(out, scan[i], norm, arena);
        }
        /* gbuf data is borrowed — only the sorted copy is owned */
        free((void *)sorted);
    }
    return ok;
}

/* Code entries (indices, ascending) under one index key. */
typedef struct {
    int *items;
    int count;
    int cap;
} cl_posting_t;

static bool cl_posting_add(CBMHashTable *index, const char *key, int idx) {
    cl_posting_t *posting = cbm_ht_get(index, key);
    if (!posting) {
        posting = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*posting));
        if (!posting) {
            return false;
        }
        cbm_ht_set(index, key, posting);
    }
    if (posting->count && posting->items[posting->count - 1] == idx) {
        return true; /* the same trigram twice in one name */
    }
    if (posting->count == posting->cap) {
        int cap = posting->cap ? posting->cap * PAIR_LEN : CBM_SZ_16;
        int *grown = cbm_realloc(CBM_MEM_CLASS_OTHER, posting->items, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        posting->items = grown;
        posting->cap = cap;
    }
    posting->items[posting->count++] = idx;
    return true;
}

static void cl_posting_free(const char *key, void *value, void *userdata) {
    (void)key;
    (void)userdata;
    cl_posting_t *posting = value;
    cbm_free(CBM_MEM_CLASS_OTHER, posting->items);
    cbm_free(CBM_MEM_CLASS_OTHER, posting);
}

enum { CL_TRIGRAM = 3 };

/* Exact index: normalized name -> code entries. Trigram index: every
 * 3-character run of a non-infra code entry's normalized name -> entries, so a
 * key's substring candidates are the entries holding its rarest trigram
 * instead of every code entry (cost follows the matches, no cap needed). */
static bool cl_build_indexes(const cl_entries_t *code, CBMHashTable *exact, CBMHashTable *trigrams,
                             CBMArena *arena) {
    for (int k = 0; k < code->count; k++) {
        const cl_entry_t *e = &code->items[k];
        if (!cl_posting_add(exact, e->normalized, k)) {
            return false;
        }
        if (e->infra) {
            continue; /* infra symbols link by exact name only */
        }
        for (size_t i = 0; i + CL_TRIGRAM <= e->len; i++) {
            char tri[CL_TRIGRAM + SKIP_ONE];
            memcpy(tri, e->normalized + i, CL_TRIGRAM);
            tri[CL_TRIGRAM] = '\0';
            const char *key = cbm_ht_get_key(trigrams, tri);
            if (!key) {
                key = cbm_arena_strdup(arena, tri);
                if (!key) {
                    return false;
                }
            }
            if (!cl_posting_add(trigrams, key, k)) {
                return false;
            }
        }
    }
    return true;
}

/* A config key's component holds the code file. */
static bool cl_in_component(const cl_entry_t *config, const cl_entry_t *code) {
    size_t n = config->component_len;
    return n == 0 ||
           (strncmp(code->file_path, config->file_path, n) == 0 && code->file_path[n] == '/');
}

/* The key x symbol matches of strategy 1, per config key: exact matches from
 * the exact index, substring matches among the entries holding the key's
 * rarest trigram. Rows run in parallel; each row's matches are in ascending
 * code order, and the edges are inserted afterwards in (config, code) order. */
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
    const cl_entries_t *config;
    const cl_entries_t *code;
    const CBMHashTable *exact;
    const CBMHashTable *trigrams;
    key_match_list_t *lists;
    _Atomic int next;
    _Atomic bool failed;
} key_match_job_t;

static bool key_match_push(key_match_list_t *list, int co, double confidence) {
    if (list->count == list->cap) {
        int cap = list->cap ? list->cap * PAIR_LEN : CBM_SZ_16;
        key_match_t *grown =
            cbm_realloc(CBM_MEM_CLASS_OTHER, list->items, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        list->items = grown;
        list->cap = cap;
    }
    list->items[list->count].co = co;
    list->items[list->count].confidence = confidence;
    list->count++;
    return true;
}

/* The trigram posting of `key` with the fewest entries; NULL when some
 * trigram occurs in no entry (then no entry holds the key). */
static const cl_posting_t *cl_rarest_trigram(const CBMHashTable *trigrams, const char *key,
                                             size_t len) {
    const cl_posting_t *rarest = NULL;
    for (size_t i = 0; i + CL_TRIGRAM <= len; i++) {
        char tri[CL_TRIGRAM + SKIP_ONE];
        memcpy(tri, key + i, CL_TRIGRAM);
        tri[CL_TRIGRAM] = '\0';
        const cl_posting_t *posting = cbm_ht_get(trigrams, tri);
        if (!posting) {
            return NULL;
        }
        if (!rarest || posting->count < rarest->count) {
            rarest = posting;
        }
    }
    return rarest;
}

static bool key_match_row(const key_match_job_t *job, int ci, key_match_list_t *list) {
    const cl_entry_t *cfg = &job->config->items[ci];
    const cl_posting_t *exact = cbm_ht_get(job->exact, cfg->normalized);
    const cl_posting_t *subs =
        cfg->infra ? NULL : cl_rarest_trigram(job->trigrams, cfg->normalized, cfg->len);
    int ei = 0;
    int si = 0;
    int ecount = exact ? exact->count : 0;
    int scount = subs ? subs->count : 0;
    /* Merge both ascending candidate lists so the row stays in code order. */
    while (ei < ecount || si < scount) {
        bool take_exact = si >= scount || (ei < ecount && exact->items[ei] <= subs->items[si]);
        int co = take_exact ? exact->items[ei++] : subs->items[si++];
        const cl_entry_t *code = &job->code->items[co];
        double confidence = 0.0;
        if (take_exact) {
            bool infra = cfg->infra || code->infra;
            confidence = (!infra || cl_in_component(cfg, code)) ? CONF_KEY_EXACT : 0.0;
        } else if (code->len > cfg->len && strstr(code->normalized, cfg->normalized)) {
            confidence = CONF_KEY_SUBSTRING;
        }
        if (confidence > 0.0 && !key_match_push(list, co, confidence)) {
            return false;
        }
    }
    return true;
}

static void key_match_worker(int worker_id, void *arg) {
    (void)worker_id;
    key_match_job_t *job = (key_match_job_t *)arg;
    while (!atomic_load_explicit(&job->failed, memory_order_relaxed)) {
        int ci = atomic_fetch_add_explicit(&job->next, SKIP_ONE, memory_order_relaxed);
        if (ci >= job->config->count) {
            break;
        }
        if (!key_match_row(job, ci, &job->lists[ci])) {
            atomic_store_explicit(&job->failed, true, memory_order_relaxed);
            return;
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

    CBMArena arena;
    cbm_arena_init(&arena);
    cl_entries_t config = {0};
    cl_entries_t code = {0};
    CBMHashTable *exact = NULL;
    CBMHashTable *trigrams = NULL;
    key_match_list_t *lists = NULL;
    int edge_count = 0;

    bool ok = collect_config_entries(vars, var_count, &config, &arena);
    if (ok && config.count > 0) {
        ok = collect_code_entries(gb, &code, &arena);
        exact = ok ? cbm_ht_create(0) : NULL;
        trigrams = exact ? cbm_ht_create(0) : NULL;
        ok = trigrams && cl_build_indexes(&code, exact, trigrams, &arena);
        lists = ok ? cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)config.count * sizeof(*lists)) : NULL;
        ok = lists != NULL;
    }
    if (ok && lists) {
        key_match_job_t job = {
            .config = &config,
            .code = &code,
            .exact = exact,
            .trigrams = trigrams,
            .lists = lists,
        };
        atomic_init(&job.next, 0);
        atomic_init(&job.failed, false);
        int workers = cbm_default_worker_count(false);
        cbm_parallel_for(
            workers, key_match_worker, &job,
            (cbm_parallel_for_opts_t){.max_workers = workers, .force_pthreads = false});
        ok = !atomic_load_explicit(&job.failed, memory_order_relaxed);
    }
    if (ok && lists) {
        for (int ci = 0; ci < config.count; ci++) {
            for (int m = 0; m < lists[ci].count; m++) {
                const key_match_t *km = &lists[ci].items[m];
                char props[CBM_SZ_512];
                snprintf(props, sizeof(props),
                         "{\"strategy\":\"key_symbol\",\"confidence\":%.2f,\"config_key\":\"%s\"}",
                         km->confidence, config.items[ci].name);
                cbm_gbuf_insert_edge(gb, code.items[km->co].node_id, config.items[ci].node_id,
                                     "CONFIGURES", props);
                edge_count++;
            }
        }
    } else if (!ok) {
        char cfg_buf[CBM_SZ_16];
        char code_buf[CBM_SZ_16];
        snprintf(cfg_buf, sizeof(cfg_buf), "%d", config.count);
        snprintf(code_buf, sizeof(code_buf), "%d", code.count);
        cbm_log_warn("configlinker.key_symbol.oom", "config", cfg_buf, "code", code_buf);
    }

    for (int ci = 0; lists && ci < config.count; ci++) {
        cbm_free(CBM_MEM_CLASS_OTHER, lists[ci].items);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, lists);
    if (exact) {
        cbm_ht_foreach(exact, cl_posting_free, NULL);
        cbm_ht_free(exact);
    }
    if (trigrams) {
        cbm_ht_foreach(trigrams, cl_posting_free, NULL);
        cbm_ht_free(trigrams);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, config.items);
    cbm_free(CBM_MEM_CLASS_OTHER, code.items);
    cbm_arena_destroy(&arena);
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
