/*
 * pass_importance.c — Index-time per-symbol importance score (weighted degree).
 *
 * Predump pass. Computes an importance score for every Function/Method/Class
 * node and stores it as a numeric "importance" key inside the node's EXISTING
 * properties_json blob — no new column, no schema change, and therefore no
 * CBM_INDEX_FORMAT_VERSION bump. Indexes written by older builds simply lack
 * the key; every consumer must tolerate its absence.
 *
 * Model (weighted degree, Aider-repomap shaped):
 *
 *   importance = sqrt(num_refs) * priv * generic * distinct * test_penalty
 *
 *     num_refs      incoming CALLS + USAGE edges. 0 -> sqrt(0) = 0.
 *     priv     0.1  name is private (leading underscore)
 *     generic  0.1  the name is defined in >= 5 DISTINCT files
 *     distinct 10   name is snake_case or camelCase AND len >= 8
 *     test_penalty
 *              0.1  the symbol is test scaffolding — either the target of an
 *                   incoming TESTS edge (a production helper exercised by a
 *                   test), or it lives in a test file per the graph's own
 *                   canonical cbm_is_test_path() classifier (the same one
 *                   pass_tests uses).
 *
 * Transitive importance (PageRank) is deliberately NOT built here: weighted
 * degree is what this pass claims, and a transitive refinement has to earn
 * itself against a fixed judgment set before it ships.
 *
 * ── Cost ─────────────────────────────────────────────────────────────
 * The generic-name multiplier needs |{ file a name is defined in }|. The
 * obvious shape — for each node, walk its whole same-name group and
 * pairwise-compare file paths — is O(k^2) per node and therefore O(k^3) per
 * same-name group of size k. That is not a theoretical concern: on a large
 * Java corpus a single name group (`toString`, k≈4.6k) costs ~2 minutes, and
 * a handful of such names alone multiplies total index time several-fold.
 *
 * This implementation is linear in the graph instead:
 *   - the distinct-file count is computed ONCE PER DISTINCT NAME and memoized
 *     (g_imp_name_counts), so a group of size k is scanned once, not k times;
 *   - within a group, file paths are deduplicated through a hash set rather
 *     than pairwise strcmp.
 * Total distinct-file work is therefore Σ over distinct names of k = O(N).
 * Both gbuf lookups it rests on (find_by_name, find_edges_by_target_type) are
 * hash-indexed, so the per-node loop is O(1) amortised per node.
 *
 * g_importance_name_visits counts same-name-group member visits and is GATED
 * in the complexity suite: it must stay linear (ratio ~2) under a corpus
 * doubling. A regression to the per-node scan lands at ~4 and fails the gate.
 *
 * ── Ordering (load-bearing) ──────────────────────────────────────────
 * Registered LAST among the predump passes, and last in the incremental
 * post-pass sequence, so every edge type the score reads (CALLS, USAGE from
 * extraction; TESTS from pass_tests) already exists. A pass ordered earlier
 * would silently score everything at num_refs = 0 with no test penalty.
 */
#include "foundation/constants.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "store/store.h"
#include "cbm.h"
#include "sqlite3.h"

#include <ctype.h>
#include <math.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>

/* Work counters — deltas are read by the complexity suite's linearity gate.
 * Never reset here: callers snapshot before/after so nested runs compose. */
_Atomic uint64_t g_importance_nodes = 0;
_Atomic uint64_t g_importance_name_visits = 0;
_Atomic uint64_t g_importance_store_rows = 0;

enum {
    CBM_IMPORTANCE_GENERIC_MIN_FILES = 5, /* name defined in >= N files -> generic */
    CBM_IMPORTANCE_DISTINCT_MIN_LEN = 8,  /* distinctive-identifier length floor */
};
static const double CBM_IMPORTANCE_PRIV_MUL = 0.1;
static const double CBM_IMPORTANCE_GENERIC_MUL = 0.1;
static const double CBM_IMPORTANCE_DISTINCT_MUL = 10.0;
static const double CBM_IMPORTANCE_TEST_MUL = 0.1;

/* The node labels that carry an importance score. File/Module and other
 * non-symbol nodes are deliberately excluded. */
static const char *const CBM_IMPORTANCE_LABELS[] = {"Function", "Method", "Class"};
enum { CBM_IMPORTANCE_LABEL_COUNT = 3 };

/* Int → string for structured logging (thread-safe ring buffer). */
static const char *itoa_imp(uint64_t val) {
    enum { RING = 2, MASK = 1 };
    static CBM_TLS char bufs[RING][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%llu", (unsigned long long)val);
    return bufs[i];
}

static bool name_is_private(const char *name) {
    return name != NULL && name[0] == '_';
}

/* Distinctive = (snake_case OR camelCase) AND length >= 8. snake_case is an
 * embedded '_'; camelCase is a lower->upper "hump". A plain len>=8 lowercase
 * word (no '_', no hump) is NOT distinctive. */
static bool name_is_distinctive(const char *name) {
    if (!name) {
        return false;
    }
    size_t len = strlen(name);
    if (len < (size_t)CBM_IMPORTANCE_DISTINCT_MIN_LEN) {
        return false;
    }
    if (strchr(name, '_') != NULL) {
        return true; /* snake_case */
    }
    for (size_t i = 1; i < len; i++) {
        if (islower((unsigned char)name[i - 1]) && isupper((unsigned char)name[i])) {
            return true; /* camelCase hump */
        }
    }
    return false;
}

/* ── JSON property write-back ─────────────────────────────────────────
 *
 * properties_json is a flat object here (same assumption the sibling passes'
 * json_get_int/json_get_bool helpers make), but the value scanner below is
 * written to survive nested values so a future richer blob cannot be
 * truncated by an in-place overwrite. */

/* Return a pointer just past the JSON value beginning at p (first non-space
 * char of the value). NULL if the value is unterminated/malformed. */
static const char *imp_value_end(const char *p) {
    if (*p == '"') {
        p++;
        while (*p) {
            if (*p == '\\' && p[1]) {
                p += 2;
                continue;
            }
            if (*p == '"') {
                return p + 1;
            }
            p++;
        }
        return NULL;
    }
    if (*p == '{' || *p == '[') {
        int depth = 0;
        bool in_str = false;
        while (*p) {
            if (in_str) {
                if (*p == '\\' && p[1]) {
                    p += 2;
                    continue;
                }
                if (*p == '"') {
                    in_str = false;
                }
            } else if (*p == '"') {
                in_str = true;
            } else if (*p == '{' || *p == '[') {
                depth++;
            } else if (*p == '}' || *p == ']') {
                depth--;
                if (depth == 0) {
                    return p + 1;
                }
            }
            p++;
        }
        return NULL;
    }
    while (*p && *p != ',' && *p != '}' && *p != ']') {
        p++;
    }
    return p;
}

/* Find `"key"` where it is genuinely in KEY position: preceded (modulo
 * whitespace) by '{' or ',' and followed (modulo whitespace) by ':'. A bare
 * strstr would also match the text inside a string VALUE. Returns a pointer
 * to the opening quote, or NULL. */
static const char *imp_find_key(const char *json, const char *key) {
    char pat[CBM_SZ_64];
    int n = snprintf(pat, sizeof(pat), "\"%s\"", key);
    if (n < 0 || (size_t)n >= sizeof(pat)) {
        return NULL;
    }
    const char *p = json;
    while ((p = strstr(p, pat)) != NULL) {
        const char *before = p;
        while (before > json && isspace((unsigned char)before[-1])) {
            before--;
        }
        char prev = (before > json) ? before[-1] : '\0';
        const char *after = p + n;
        while (isspace((unsigned char)*after)) {
            after++;
        }
        if ((prev == '{' || prev == ',') && *after == ':') {
            return p;
        }
        p += n;
    }
    return NULL;
}

/* Produce a copy of `json` carrying the numeric "importance" key set to
 * `score`, or NULL if `json` is not a JSON object (caller leaves it alone) or
 * on allocation failure. Caller owns the result.
 *
 * IDEMPOTENT BY CONTRACT. Nodes reach this function already carrying an
 * "importance" key on every re-scoring route: the legacy-partial incremental
 * path rehydrates them via cbm_gbuf_load_from_db, and the store-level
 * recompute reads them straight out of SQLite. A pure append would emit
 * {"importance":1.0,...,"importance":2.0} — silent property corruption that no
 * build or schema check would catch. So: overwrite an existing key in place,
 * append only when the key is genuinely absent.
 *
 * SINGLE DEFINITION. Both scoring routes (the in-memory gbuf pass and the
 * SQL-level store recompute) write through this one function, so the JSON
 * shape cannot drift between them. */
char *cbm_pipeline_importance_set_prop(const char *json, double score) {
    const char *old = json ? json : "{}";
    size_t olen = strlen(old);
    if (olen < 2 || old[olen - 1] != '}') {
        return NULL; /* not a JSON object — leave untouched */
    }

    char val[CBM_SZ_32];
    int vn = snprintf(val, sizeof(val), "%.6f", score);
    if (vn < 0 || (size_t)vn >= sizeof(val)) {
        return NULL;
    }

    const char *k = imp_find_key(old, "importance");
    if (k) {
        const char *colon = strchr(k, ':');
        if (!colon) {
            return NULL; /* imp_find_key guarantees one, but never trust a raw scan */
        }
        const char *v = colon + 1;
        while (isspace((unsigned char)*v)) {
            v++;
        }
        const char *vend = imp_value_end(v);
        if (!vend) {
            return NULL;
        }
        size_t head = (size_t)(v - old);
        size_t tail = strlen(vend);
        char *neu = malloc(head + (size_t)vn + tail + 1);
        if (!neu) {
            return NULL;
        }
        memcpy(neu, old, head);
        memcpy(neu + head, val, (size_t)vn);
        memcpy(neu + head + (size_t)vn, vend, tail + 1);
        return neu;
    }

    char frag[CBM_SZ_64];
    int fn = snprintf(frag, sizeof(frag), "%s\"importance\":%s}", (olen == 2) ? "" : ",", val);
    if (fn < 0 || (size_t)fn >= sizeof(frag)) {
        return NULL;
    }
    char *neu = malloc(olen - 1 + (size_t)fn + 1);
    if (!neu) {
        return NULL;
    }
    memcpy(neu, old, olen - 1); /* copy without the trailing '}' */
    memcpy(neu + olen - 1, frag, (size_t)fn + 1);
    return neu;
}

/* gbuf-node convenience wrapper around cbm_pipeline_importance_set_prop. */
void cbm_pipeline_importance_append_prop(cbm_gbuf_node_t *node, double score) {
    if (!node) {
        return;
    }
    char *neu = cbm_pipeline_importance_set_prop(node->properties_json, score);
    if (!neu) {
        return;
    }
    free(node->properties_json);
    node->properties_json = neu;
}

/* ── The scoring rule ─────────────────────────────────────────────────
 *
 * SINGLE DEFINITION, deliberately. There are two routes that score nodes —
 * the in-memory gbuf pass (full index, legacy-partial incremental) and the
 * SQL-level recompute over the staging store (closure-delta incremental) —
 * and they gather their inputs very differently. Only the GATHERING differs;
 * the rule itself lives here alone, so the two routes cannot drift apart in
 * what a score means. The equivalence test in tests/test_importance.c guards
 * the remaining freedom: that both gatherings produce the same inputs for the
 * same graph. */
double cbm_pipeline_importance_score(const cbm_importance_inputs_t *in) {
    if (!in) {
        return 0.0;
    }
    double score = sqrt((double)(in->num_refs > 0 ? in->num_refs : 0));
    if (name_is_private(in->name)) {
        score *= CBM_IMPORTANCE_PRIV_MUL;
    }
    if (in->name_distinct_files >= CBM_IMPORTANCE_GENERIC_MIN_FILES) {
        score *= CBM_IMPORTANCE_GENERIC_MUL;
    }
    if (name_is_distinctive(in->name)) {
        score *= CBM_IMPORTANCE_DISTINCT_MUL;
    }
    if (cbm_is_test_path(in->file_path) || in->tests_target) {
        score *= CBM_IMPORTANCE_TEST_MUL;
    }
    return score;
}

/* ── Distinct-file counting (the linear core) ─────────────────────────── */

typedef struct {
    CBMHashTable *counts; /* name -> (void *)(intptr_t)(distinct_files + 1) */
    CBMHashTable *paths;  /* scratch file-path set, cleared between names */
} imp_name_index_t;

/* Distinct files a name is DEFINED in. Counts distinct file paths, not
 * distinct nodes — two defs of one name in the same file count once.
 * Computed once per distinct name; every later node in the group is a hash
 * hit. Name keys are borrowed from the gbuf nodes, which outlive the pass. */
static int imp_distinct_file_count(const cbm_gbuf_t *gb, const char *name, imp_name_index_t *ix) {
    if (!name || !*name || !ix || !ix->counts) {
        return 0;
    }
    void *cached = cbm_ht_get(ix->counts, name);
    if (cached) {
        return (int)((intptr_t)cached - 1);
    }

    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    int distinct = 0;
    if (cbm_gbuf_find_by_name(gb, name, &nodes, &count) == 0 && count > 0) {
        atomic_fetch_add_explicit(&g_importance_name_visits, (uint64_t)count, memory_order_relaxed);
        if (ix->paths) {
            cbm_ht_clear(ix->paths);
            for (int i = 0; i < count; i++) {
                const char *fp = nodes[i]->file_path;
                if (!fp || cbm_ht_has(ix->paths, fp)) {
                    continue;
                }
                cbm_ht_set(ix->paths, fp, (void *)(intptr_t)1);
                distinct++;
            }
        } else {
            /* Scratch set unavailable (allocation failure): the generic-name
             * multiplier degrades to "never generic" rather than falling back
             * to a pairwise scan — a silent quadratic is worse than a missing
             * 0.1 multiplier. */
            distinct = 0;
        }
    }
    cbm_ht_set(ix->counts, name, (void *)(intptr_t)(distinct + 1));
    return distinct;
}

static int incoming_edge_count(const cbm_gbuf_t *gb, int64_t id, const char *type) {
    const cbm_gbuf_edge_t **edges = NULL;
    int ne = 0;
    if (cbm_gbuf_find_edges_by_target_type(gb, id, type, &edges, &ne) != 0) {
        return 0;
    }
    return ne;
}

void cbm_pipeline_pass_importance(cbm_pipeline_ctx_t *ctx) {
    if (!ctx || !ctx->gbuf) {
        return;
    }
    cbm_gbuf_t *gb = ctx->gbuf;

    /* Modest reservations, deliberately not sized from node_count: the memo
     * holds one entry per DISTINCT name, far fewer than nodes, and reserving
     * buckets for every node would cost hundreds of MB of transient peak on a
     * multi-million-node graph to save a handful of amortised rehashes. */
    imp_name_index_t ix = {
        .counts = cbm_ht_create(CBM_SZ_4K),
        .paths = cbm_ht_create(CBM_SZ_256),
    };
    if (!ix.counts) {
        cbm_ht_free(ix.paths);
        cbm_log_error("pass.importance", "msg", "name_index_alloc_failed");
        return;
    }

    uint64_t updated = 0;
    for (int li = 0; li < CBM_IMPORTANCE_LABEL_COUNT; li++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(gb, CBM_IMPORTANCE_LABELS[li], &nodes, &count) != 0) {
            continue;
        }
        for (int i = 0; i < count; i++) {
            cbm_gbuf_node_t *n = (cbm_gbuf_node_t *)nodes[i];

            /* Gather only — the rule itself lives in one place. */
            cbm_importance_inputs_t in = {
                .name = n->name,
                .file_path = n->file_path,
                .num_refs = incoming_edge_count(gb, n->id, "CALLS") +
                            incoming_edge_count(gb, n->id, "USAGE"),
                .name_distinct_files = imp_distinct_file_count(gb, n->name, &ix),
                .tests_target = incoming_edge_count(gb, n->id, "TESTS") > 0,
            };
            cbm_pipeline_importance_append_prop(n, cbm_pipeline_importance_score(&in));
            updated++;
        }
    }

    cbm_ht_free(ix.paths);
    cbm_ht_free(ix.counts);
    atomic_fetch_add_explicit(&g_importance_nodes, updated, memory_order_relaxed);
    cbm_log_info("pass.importance", "symbols", itoa_imp(updated));
}

/* ── Store-level recompute (closure-delta incremental) ────────────────
 *
 * WHY THIS EXISTS. The closure-delta route never materialises the project
 * graph in RAM: cbm_delta_preseed fills the delta gbuf with *proxy* nodes
 * (id, label, name, qn, file_path — no edges, empty properties), and only the
 * re-extracted files' symbols carry real edges. Scoring there reads a
 * near-zero in-degree for a symbol referenced hundreds of times project-wide,
 * and cbm_delta_patch then persists exactly those fresh rows. Loading the full
 * graph to fix that would give back the point of the delta route, so the
 * recompute runs where the complete graph already exists: in SQL, over the
 * staging store, AFTER cbm_delta_patch has inserted the new nodes and
 * re-linked the snapshotted inbound edges.
 *
 * WHY IT CANNOT DRIFT FROM THE IN-MEMORY PASS. The scoring rule and the JSON
 * write-back are not reimplemented here. They are registered as SQLite
 * user-defined functions that call cbm_pipeline_importance_score() and
 * cbm_pipeline_importance_set_prop() — the same C code the gbuf pass runs.
 * SQL supplies only the three aggregates (in-degree, TESTS presence, distinct
 * files per name), each one indexed GROUP BY. What remains free is the
 * GATHERING, and the full-vs-incremental equivalence test guards that.
 *
 * The whole project is rescored, not just the changed files: distinct-files-
 * per-name is a global input, so editing one file can legitimately move a name
 * across the generic-name threshold for every other definition of that name. A
 * scoped recompute would leave those stale — and would fail the equivalence
 * test, correctly. */

/* UDF: cbm_imp_score(name, file_path, num_refs, name_files, tests_target). */
static void sql_importance_score(sqlite3_context *c, int argc, sqlite3_value **argv) {
    if (argc != 5) {
        sqlite3_result_error(c, "cbm_imp_score arity", -1);
        return;
    }
    cbm_importance_inputs_t in = {
        .name = (const char *)sqlite3_value_text(argv[0]),
        .file_path = (const char *)sqlite3_value_text(argv[1]),
        .num_refs = sqlite3_value_int(argv[2]),
        .name_distinct_files = sqlite3_value_int(argv[3]),
        .tests_target = sqlite3_value_int(argv[4]) != 0,
    };
    sqlite3_result_double(c, cbm_pipeline_importance_score(&in));
}

/* UDF: cbm_imp_set(properties, score) → properties with "importance" set.
 * A non-object blob yields NULL; the UPDATE's COALESCE then leaves that row
 * untouched — the same tolerance the in-memory writer has. */
static void sql_importance_set(sqlite3_context *c, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(c, "cbm_imp_set arity", -1);
        return;
    }
    const char *props = (const char *)sqlite3_value_text(argv[0]);
    char *neu = cbm_pipeline_importance_set_prop(props, sqlite3_value_double(argv[1]));
    if (!neu) {
        sqlite3_result_null(c);
        return;
    }
    sqlite3_result_text(c, neu, -1, SQLITE_TRANSIENT);
    free(neu);
}

/* The aggregate CTE is MATERIALIZED on purpose: it reads `nodes` while the
 * UPDATE writes `nodes`, and materialising settles that ordering explicitly
 * rather than relying on the planner to choose it. */
static const char *const kImportanceRecomputeSQL =
    "WITH agg AS MATERIALIZED ("
    "  SELECT n.id AS id,"
    "         cbm_imp_score(n.name, n.file_path, COALESCE(d.refs, 0),"
    "                       COALESCE(f.files, 0), COALESCE(d.tests, 0)) AS score"
    "  FROM nodes n"
    "  LEFT JOIN (SELECT target_id AS id,"
    "                    SUM(CASE WHEN type IN ('CALLS','USAGE') THEN 1 ELSE 0 END) AS refs,"
    "                    SUM(CASE WHEN type = 'TESTS' THEN 1 ELSE 0 END) AS tests"
    "             FROM edges WHERE project = ?1 GROUP BY target_id) d ON d.id = n.id"
    "  LEFT JOIN (SELECT name, COUNT(DISTINCT file_path) AS files"
    "             FROM nodes WHERE project = ?1 GROUP BY name) f ON f.name = n.name"
    "  WHERE n.project = ?1 AND n.label IN ('Function','Method','Class')"
    ")"
    "UPDATE nodes SET properties = COALESCE(cbm_imp_set(properties, agg.score), properties)"
    " FROM agg WHERE nodes.id = agg.id";

int cbm_pipeline_importance_recompute_store(cbm_store_t *store, const char *project) {
    if (!store || !project) {
        return -1;
    }
    sqlite3 *db = cbm_store_get_db(store);
    if (!db) {
        return -1;
    }
    if (sqlite3_create_function(db, "cbm_imp_score", 5, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                                sql_importance_score, NULL, NULL) != SQLITE_OK ||
        sqlite3_create_function(db, "cbm_imp_set", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                                sql_importance_set, NULL, NULL) != SQLITE_OK) {
        cbm_log_error("importance.store_udf_failed", "err", sqlite3_errmsg(db));
        return -1;
    }
    if (cbm_store_begin(store) != CBM_STORE_OK) {
        return -1;
    }
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, kImportanceRecomputeSQL, -1, &st, NULL) != SQLITE_OK) {
        cbm_log_error("importance.store_prepare_failed", "err", sqlite3_errmsg(db));
        cbm_store_rollback(store);
        return -1;
    }
    sqlite3_bind_text(st, 1, project, -1, SQLITE_TRANSIENT);
    int step_rc = sqlite3_step(st);
    sqlite3_finalize(st);
    if (step_rc != SQLITE_DONE) {
        cbm_log_error("importance.store_step_failed", "err", sqlite3_errmsg(db));
        cbm_store_rollback(store);
        return -1;
    }
    int changed = sqlite3_changes(db);
    if (cbm_store_commit(store) != CBM_STORE_OK) {
        return -1;
    }
    uint64_t rows = (uint64_t)(changed > 0 ? changed : 0);
    atomic_fetch_add_explicit(&g_importance_store_rows, rows, memory_order_relaxed);
    cbm_log_info("pass.importance_store", "symbols", itoa_imp(rows));
    return 0;
}
