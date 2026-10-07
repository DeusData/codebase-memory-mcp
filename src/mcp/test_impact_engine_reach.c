/*
 * test_impact_engine_reach.c — from seeds to the selection's inputs: the
 * fixture scan, the walk (rule B: the static walk without the process lift),
 * reach rows for every registered test, and whole-suite triggers.
 *
 * Ports replay4.py run() 474-489 (cases, suites and runner as sinks), 584-615
 * (fixtures by path literal) and 616-621 (header file-level escalation), and
 * score4.py static_nolift (reached and changed suite bodies run whole; a
 * reached runner main runs everything).
 */
#include "mcp/test_impact_engine_internal.h"

#include "foundation/hash_table.h"
#include "foundation/mem_core.h"
#include "store/store_impact.h"

#include <stdlib.h>
#include <string.h>

/* The c-cbm runner's process-entry handlers: the runner re-executes itself
 * with a role argument and these run product code in that child. They are
 * reached, never expanded (their caller is the runner's main). */
#define TE_RUNNER_ROLE_PREFIX "tf_maybe_run_"

/* SPAWNS only carries reach from a Process node, which enters the walk
 * through te_spawn_lift alone. */
static const char *const te_walk_types[] = {"CALLS",          "ASYNC_CALLS", "USAGE",
                                            "CALL_REFERENCE", "READS",       "SPAWNS"};

typedef struct {
    int64_t *items;
    int count;
    int cap;
} te_ids_t;

static bool te_ids_push(te_ctx_t *c, te_ids_t *a, int64_t id) {
    if (!te_grow((void **)&a->items, &a->cap, a->count, sizeof(*a->items))) {
        return te_oom(c);
    }
    a->items[a->count++] = id;
    return true;
}

static bool te_is_function(const cbm_ti_node_t *n) {
    return strcmp(n->label, "Function") == 0 || strcmp(n->label, "Method") == 0;
}

/* The function nodes of a test case or suite of `file`: those whose
 * definition starts on the line of its TEST(...) / SUITE(...) — graphs name
 * them differently (`x` without configured declarations, `TEST_x` with) —
 * else every function named `name`. *count of them, ids appended. */
static bool te_named_match(const cbm_ti_node_t *n, int pass, int line, const char *name) {
    return te_is_function(n) && (pass == 0 ? n->start_line == line : strcmp(n->name, name) == 0);
}

static bool te_named(te_ctx_t *c, const char *file, const char *name, int line, te_ids_t *out,
                     int *count) {
    bool ok = true;
    int n = 0;
    const cbm_ti_node_t *nodes = cbm_ti_graph_file(c->graph, file, &n, &ok);
    if (!ok) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE, "graph read failed") &&
               false;
    }
    *count = 0;
    for (int pass = 0; pass < 2 && *count == 0; pass++) {
        for (int i = 0; i < n; i++) {
            /* Any span of a definition names it by its start line, the
             * primary one or a variant (graph_buffer.c "Definition
             * variants"); each definition counts once. */
            if (!te_named_match(&nodes[i], pass, line, name)) {
                continue;
            }
            bool counted = false;
            for (int k = 0; k < i && !counted; k++) {
                counted = nodes[k].id == nodes[i].id && te_named_match(&nodes[k], pass, line, name);
            }
            if (counted) {
                continue;
            }
            (*count)++;
            if (out && !te_ids_push(c, out, nodes[i].id)) {
                return false;
            }
        }
    }
    return true;
}

static bool te_is_runner_suite(te_ctx_t *c, const char *name) {
    int n = 0;
    const cbm_test_runner_suite_t *runner = cbm_test_model_runner_suites(c->model, &n);
    for (int i = 0; i < n; i++) {
        if (strcmp(runner[i].name, name) == 0) {
            return true;
        }
    }
    return false;
}

/* ── Fixtures (replay4 584-615) ──────────────────────────────────── */

/* Test files: the files that define a suite. */
static bool te_test_file(te_ctx_t *c, const char *path) {
    int n = 0;
    const cbm_test_suite_t *suites = cbm_test_model_suites(c->model, &n);
    for (int i = 0; i < n; i++) {
        if (strcmp(suites[i].file, path) == 0) {
            return true;
        }
    }
    return false;
}

static int te_case_index(te_ctx_t *c, const char *file, const char *name) {
    int n = 0;
    const cbm_test_case_t *cases = cbm_test_model_cases(c->model, &n);
    for (int i = 0; i < n; i++) {
        if (strcmp(cases[i].file, file) == 0 && strcmp(cases[i].name, name) == 0) {
            return i;
        }
    }
    return -1;
}

/* Every line of a test file that contains `needle`: its enclosing
 * definition is a fixture case or a seed. */
static bool te_scan_literal(te_ctx_t *c, const char *needle, te_ids_t *seeds, bool *cases,
                            bool *hit) {
    int files = cbm_ti_source_file_count(c->source);
    size_t nlen = strlen(needle);
    for (int f = 0; f < files; f++) {
        const char *path = cbm_ti_source_path(c->source, f);
        if (!te_test_file(c, path)) {
            continue;
        }
        int lines = cbm_ti_source_line_count(c->source, f);
        for (int line = 1; line <= lines; line++) {
            size_t len = 0;
            const char *text = cbm_ti_source_line(c->source, f, line, &len);
            bool found = false;
            for (size_t i = 0; !found && nlen && i + nlen <= len; i++) {
                found = memcmp(text + i, needle, nlen) == 0;
            }
            if (!found) {
                continue;
            }
            bool ok = true;
            const cbm_ti_node_t *d = cbm_ti_graph_enclosing(c->graph, path, line, &ok);
            if (!ok) {
                return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                                   "graph read failed") &&
                       false;
            }
            if (!d) {
                continue;
            }
            *hit = true;
            int ci = te_is_function(d) ? te_case_index(c, path, d->name) : -1;
            if (ci >= 0) {
                cases[ci] = true;
            } else if (!te_ids_push(c, seeds, d->id)) {
                return false;
            }
        }
    }
    return true;
}

/* One fixture path: the path itself, then each parent down to the rule's
 * key segment; each with and without its first segment. The first key some
 * test file mentions decides. */
static bool te_fixture(te_ctx_t *c, const char *path, int min_segments, te_ids_t *seeds,
                       bool *cases, bool *hit) {
    char key[4096];
    size_t len = strlen(path);
    if (len >= sizeof(key)) {
        return true; /* unresolved: run-all through the rule */
    }
    memcpy(key, path, len + 1);
    int segments = 1;
    for (size_t i = 0; i < len; i++) {
        segments += key[i] == '/';
    }
    *hit = false;
    while (!*hit && segments >= min_segments) {
        const char *rest = strchr(key, '/');
        if (!te_scan_literal(c, key, seeds, cases, hit) ||
            (rest && !te_scan_literal(c, rest + 1, seeds, cases, hit))) {
            return false;
        }
        char *cut = strrchr(key, '/');
        if (!cut) {
            break;
        }
        *cut = '\0';
        segments--;
    }
    return true;
}

bool te_fixtures(te_ctx_t *c, int64_t **extra_seeds, int *extra_count, bool **fixture_cases) {
    int case_count = 0;
    (void)cbm_test_model_cases(c->model, &case_count);
    bool *cases = cbm_arena_calloc(&c->arena, (size_t)case_count + 1);
    te_ids_t seeds = {0};
    if (!cases) {
        return te_oom(c);
    }
    int rule_count = 0;
    const cbm_test_rule_t *rules = cbm_test_policy_rules(c->policy, &rule_count);
    for (int i = 0; i < c->fixture_count; i++) {
        int r = c->fixture_rules[i];
        bool hit = false;
        if (!te_fixture(c, c->fixture_paths[i], rules[r].key_segment + 1, &seeds, cases, &hit)) {
            cbm_free(CBM_MEM_CLASS_EXTRACT, seeds.items);
            return false;
        }
        for (int m = 0; !hit && m < c->match_count; m++) {
            if (c->matches[m].rule_index == r) {
                c->matches[m].resolved = false; /* no test names it: run-all */
            }
        }
    }
    *extra_seeds = seeds.items;
    *extra_count = seeds.count;
    *fixture_cases = cases;
    return true;
}

/* ── The walk ────────────────────────────────────────────────────── */

typedef struct {
    te_ids_t sinks;
    te_ids_t runner_main;
} te_sinks_t;

static bool te_runner_sinks(te_ctx_t *c, te_sinks_t *s) {
    int n = 0;
    const cbm_test_runner_suite_t *runner = cbm_test_model_runner_suites(c->model, &n);
    for (int i = 0; i < n; i++) {
        if (!runner[i].file ||
            (i > 0 && runner[i - 1].file && strcmp(runner[i - 1].file, runner[i].file) == 0)) {
            continue;
        }
        bool ok = true;
        int count = 0;
        const cbm_ti_node_t *nodes = cbm_ti_graph_file(c->graph, runner[i].file, &count, &ok);
        if (!ok) {
            return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                               "graph read failed") &&
                   false;
        }
        for (int k = 0; k < count; k++) {
            if (!te_is_function(&nodes[k]) || nodes[k].variant_span) {
                continue;
            }
            bool main_fn = strcmp(nodes[k].name, "main") == 0;
            bool role = strncmp(nodes[k].name, TE_RUNNER_ROLE_PREFIX,
                                sizeof(TE_RUNNER_ROLE_PREFIX) - 1) == 0;
            if ((main_fn || role) && !te_ids_push(c, &s->sinks, nodes[k].id)) {
                return false;
            }
            if (main_fn && !te_ids_push(c, &s->runner_main, nodes[k].id)) {
                return false;
            }
        }
    }
    return true;
}

static bool te_collect_sinks(te_ctx_t *c, te_sinks_t *s) {
    int n = 0;
    int count = 0;
    const cbm_test_case_t *cases = cbm_test_model_cases(c->model, &n);
    for (int i = 0; i < n; i++) {
        if (!te_named(c, cases[i].file, cases[i].name, cases[i].start_line, &s->sinks, &count)) {
            return false;
        }
    }
    const cbm_test_suite_t *suites = cbm_test_model_suites(c->model, &n);
    for (int i = 0; i < n; i++) {
        if (!te_named(c, suites[i].file, suites[i].name, suites[i].start_line, &s->sinks, &count)) {
            return false;
        }
    }
    return te_runner_sinks(c, s);
}

static bool te_walk(te_ctx_t *c, const te_sinks_t *sinks, const int64_t *extra, int extra_count,
                    cbm_impact_walk_t **out) {
    cbm_impact_policy_t policy = {.project = "test-impact",
                                  .edge_types = te_walk_types,
                                  .edge_type_count =
                                      (int)(sizeof(te_walk_types) / sizeof(te_walk_types[0])),
                                  .follow_routes = true};
    *out = NULL;
    if (cbm_impact_walk_open_scoped(c->scope, &policy, out) != CBM_STORE_OK) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_ENGINE_SATURATED,
                           "the impact walk could not start") &&
               false;
    }
    int seed_count = 0;
    const int64_t *seeds = cbm_ti_seeds_nodes(c->seeds, &seed_count);
    bool ok =
        cbm_impact_walk_add_sinks(*out, sinks->sinks.items, sinks->sinks.count) == CBM_STORE_OK;
    if (ok && seed_count) {
        ok = cbm_impact_walk_run(*out, seeds, seed_count) == CBM_STORE_OK;
    }
    if (ok && extra_count) {
        ok = cbm_impact_walk_run(*out, extra, extra_count) == CBM_STORE_OK;
    }
    if (!ok) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_ENGINE_SATURATED,
                           "the impact walk failed") &&
               false;
    }
    return true;
}

/* ── Parse gaps (user decision 2026-10-04: handled per file) ────────
 * A partial parse leaves a few lines (index_coverage `parse_partial`, detail
 * "a-b,c-d") the graph has no edges from. A gap can only hide references
 * written in its own lines, so when a gap's text names a definition the walk
 * reached, the definition around the gap is reached too (its whole file when
 * the gap lies outside every definition) and the walk goes on, to a
 * fixpoint. Only C-family sources are read; tests are C. More, never fewer. */

typedef struct {
    const char *file;
    int start;
    int end;
    bool fired;
} te_gap_t;

typedef struct {
    te_gap_t *items;
    int count;
    int cap;
} te_gaps_t;

static bool te_gap_push(te_ctx_t *c, te_gaps_t *gaps, const char *file, int start, int end) {
    if (!te_grow((void **)&gaps->items, &gaps->cap, gaps->count, sizeof(*gaps->items))) {
        return te_oom(c);
    }
    gaps->items[gaps->count++] = (te_gap_t){.file = file, .start = start, .end = end};
    return true;
}

/* "a-b,c-d,e" line ranges. */
static bool te_parse_ranges(te_ctx_t *c, te_gaps_t *gaps, const char *file, const char *detail) {
    const char *p = detail;
    while (p && *p) {
        char *end = NULL;
        long a = strtol(p, &end, 10);
        long b = a;
        if (end == p) {
            break;
        }
        if (*end == '-') {
            p = end + 1;
            b = strtol(p, &end, 10);
        }
        if (a > 0 && b >= a && b - a < 100000 && !te_gap_push(c, gaps, file, (int)a, (int)b)) {
            return false;
        }
        p = *end == ',' ? end + 1 : NULL;
    }
    return true;
}

static bool te_load_gaps(te_ctx_t *c, te_gaps_t *gaps) {
    cbm_coverage_row_t *rows = NULL;
    int n = 0;
    if (cbm_store_coverage_get(c->store, "test-impact", &rows, &n) != CBM_STORE_OK) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                           "the graph's parse gaps could not be read") &&
               false;
    }
    bool ok = true;
    for (int i = 0; ok && i < n; i++) {
        if (!rows[i].kind || strcmp(rows[i].kind, "parse_partial") != 0 || !rows[i].rel_path ||
            cbm_ti_source_find(c->source, rows[i].rel_path) < 0) {
            continue;
        }
        const char *file = cbm_arena_strdup(&c->arena, rows[i].rel_path);
        ok =
            file ? te_parse_ranges(c, gaps, file, rows[i].detail ? rows[i].detail : "") : te_oom(c);
    }
    cbm_store_free_coverage(rows, n);
    return ok;
}

/* The short names of everything the walk reached (last QN segment). */
static bool te_reached_names(te_ctx_t *c, cbm_impact_walk_t *walk, CBMHashTable *names) {
    const cbm_impact_hit_t *hits = cbm_impact_walk_hits(walk);
    int n = cbm_impact_walk_count(walk);
    for (int i = 0; i < n; i++) {
        const char *qn = hits[i].qualified_name;
        const char *dot = qn ? strrchr(qn, '.') : NULL;
        const char *name = dot ? dot + 1 : qn;
        if (!name || !*name || cbm_ht_get(names, name)) {
            continue;
        }
        const char *owned = cbm_arena_strdup(&c->arena, name);
        if (!owned) {
            return te_oom(c);
        }
        cbm_ht_set(names, owned, (void *)1);
    }
    return true;
}

static bool te_gap_names_reached(te_ctx_t *c, const te_gap_t *g, CBMHashTable *names) {
    int file = cbm_ti_source_find(c->source, g->file);
    for (int line = g->start; line <= g->end; line++) {
        size_t len = 0;
        const char *text = cbm_ti_source_line(c->source, file, line, &len);
        for (size_t i = 0; text && i < len;) {
            char ch = text[i];
            bool start = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_';
            if (!start) {
                i++;
                continue;
            }
            size_t e = i;
            char word[256];
            while (e < len &&
                   ((text[e] >= 'a' && text[e] <= 'z') || (text[e] >= 'A' && text[e] <= 'Z') ||
                    (text[e] >= '0' && text[e] <= '9') || text[e] == '_')) {
                e++;
            }
            if (e - i < sizeof(word)) {
                memcpy(word, text + i, e - i);
                word[e - i] = '\0';
                if (cbm_ht_get(names, word)) {
                    return true;
                }
            }
            i = e;
        }
    }
    return false;
}

/* Seeds for a gap that fired: the definition around it, else its file. */
static bool te_gap_seeds(te_ctx_t *c, const te_gap_t *g, te_ids_t *out) {
    bool ok = true;
    const cbm_ti_node_t *d = cbm_ti_graph_enclosing(c->graph, g->file, g->start, &ok);
    if (ok && d) {
        return te_ids_push(c, out, d->id);
    }
    int n = 0;
    const cbm_ti_node_t *nodes = ok ? cbm_ti_graph_file(c->graph, g->file, &n, &ok) : NULL;
    for (int i = 0; ok && i < n; i++) {
        ok = nodes[i].variant_span || te_ids_push(c, out, nodes[i].id);
    }
    return ok || te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE, "graph read failed");
}

static bool te_parse_gaps(te_ctx_t *c, cbm_impact_walk_t *walk) {
    te_gaps_t gaps = {0};
    CBMHashTable *names = NULL;
    bool ok = te_load_gaps(c, &gaps);
    bool grew = ok && gaps.count > 0;
    while (ok && grew) {
        grew = false;
        if (names) {
            cbm_ht_free(names);
        }
        names = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 0);
        te_ids_t seeds = {0};
        ok = names && te_reached_names(c, walk, names);
        for (int i = 0; ok && i < gaps.count; i++) {
            if (!gaps.items[i].fired && te_gap_names_reached(c, &gaps.items[i], names)) {
                gaps.items[i].fired = true;
                ok = te_gap_seeds(c, &gaps.items[i], &seeds);
            }
        }
        if (ok && seeds.count) {
            ok = cbm_impact_walk_run(walk, seeds.items, seeds.count) == CBM_STORE_OK ||
                 (te_fallback(c, CBM_TEST_RESULT_FALLBACK_ENGINE_SATURATED,
                              "the impact walk failed") &&
                  false);
            grew = ok;
        }
        cbm_free(CBM_MEM_CLASS_EXTRACT, seeds.items);
    }
    if (names) {
        cbm_ht_free(names);
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, gaps.items);
    return ok;
}

/* ── Reach rows and triggers ─────────────────────────────────────── */

static bool te_trigger(te_ctx_t *c, const char *suite, bool reached, bool changed, bool rule);

/* Whether a suite of `file` registers tests the model cannot name (a macro
 * registers them) or is uncertain: its cases cannot be narrowed one by one. */
static bool te_whole_only_file(te_ctx_t *c, const char *file) {
    int n = 0;
    const cbm_test_suite_t *suites = cbm_test_model_suites(c->model, &n);
    for (int i = 0; i < n; i++) {
        if ((suites[i].macro_registrations || suites[i].uncertain) &&
            strcmp(suites[i].file, file) == 0) {
            return true;
        }
    }
    return false;
}

/* A reached or changed case of such a file runs every suite of the file
 * whole: a macro may register it in any of them (the reference walked on
 * from such a case to its suite). */
static bool te_trigger_file_suites(te_ctx_t *c, const char *file, bool reached, bool changed) {
    int n = 0;
    const cbm_test_suite_t *suites = cbm_test_model_suites(c->model, &n);
    for (int i = 0; i < n; i++) {
        if (strcmp(suites[i].file, file) == 0 &&
            !te_trigger(c, suites[i].name, reached, changed, false)) {
            return false;
        }
    }
    return true;
}

static bool te_reach_rows(te_ctx_t *c, cbm_impact_walk_t *walk, const bool *fixture_cases) {
    int n = 0;
    const cbm_test_case_t *cases = cbm_test_model_cases(c->model, &n);
    int changed_n = 0;
    const int *changed = cbm_ti_seeds_changed_cases(c->seeds, &changed_n);
    c->reach = cbm_arena_calloc(&c->arena, (size_t)(n ? n : 1) * sizeof(*c->reach));
    /* The graph node each row's name mapped to (0 = none or several). */
    int64_t *row_id = cbm_arena_calloc(&c->arena, (size_t)(n ? n : 1) * sizeof(*row_id));
    if (!c->reach || !row_id) {
        return te_oom(c);
    }
    for (int i = 0; i < n; i++) {
        int at = -1;
        for (int k = 0; k < c->reach_count; k++) {
            if (strcmp(c->reach[k].file, cases[i].file) == 0 &&
                strcmp(c->reach[k].test, cases[i].name) == 0) {
                at = k;
            }
        }
        te_ids_t ids = {0};
        int count = 0;
        if (!te_named(c, cases[i].file, cases[i].name, cases[i].start_line, &ids, &count)) {
            cbm_free(CBM_MEM_CLASS_EXTRACT, ids.items);
            return false;
        }
        bool reached = false;
        for (int k = 0; k < ids.count; k++) {
            reached = reached || cbm_impact_walk_reached(walk, ids.items[k]);
        }
        int64_t id = count == 1 && ids.count == 1 ? ids.items[0] : 0;
        cbm_free(CBM_MEM_CLASS_EXTRACT, ids.items);
        bool was_changed = fixture_cases[i];
        for (int k = 0; k < changed_n; k++) {
            was_changed = was_changed || changed[k] == i;
        }
        if (te_whole_only_file(c, cases[i].file)) {
            if ((reached || was_changed) &&
                !te_trigger_file_suites(c, cases[i].file, reached, was_changed)) {
                return false;
            }
            continue;
        }
        if (at >= 0) {
            /* The same name twice in one file is ambiguous and never narrowed,
             * unless both are the same graph node: one test written once per
             * #if/#elif branch is one definition with variants (graph_buffer.c
             * "Definition variants"), as the graph already counts it. */
            if (id == 0 || row_id[at] != id) {
                c->reach[at].mapped = false;
            }
            c->reach[at].reached = c->reach[at].reached || reached;
            c->reach[at].changed = c->reach[at].changed || was_changed;
            continue;
        }
        row_id[c->reach_count] = id;
        c->reach[c->reach_count++] = (cbm_test_reach_t){.file = cases[i].file,
                                                        .test = cases[i].name,
                                                        .mapped = count == 1,
                                                        .reached = reached,
                                                        .changed = was_changed};
    }
    return true;
}

static bool te_trigger(te_ctx_t *c, const char *suite, bool reached, bool changed, bool rule) {
    if (!te_is_runner_suite(c, suite)) {
        return true; /* outside this runner's selection */
    }
    if (!te_grow((void **)&c->triggers, &c->trigger_cap, c->trigger_count, sizeof(*c->triggers))) {
        return te_oom(c);
    }
    c->triggers[c->trigger_count++] = (cbm_test_suite_trigger_t){
        .suite = suite, .reached = reached, .changed = changed, .rule = rule};
    return true;
}

static bool te_suite_triggers(te_ctx_t *c, cbm_impact_walk_t *walk) {
    int n = 0;
    const cbm_test_suite_t *suites = cbm_test_model_suites(c->model, &n);
    int changed_n = 0;
    const int *changed = cbm_ti_seeds_changed_suites(c->seeds, &changed_n);
    for (int i = 0; i < n; i++) {
        te_ids_t ids = {0};
        int count = 0;
        if (!te_named(c, suites[i].file, suites[i].name, suites[i].start_line, &ids, &count)) {
            cbm_free(CBM_MEM_CLASS_EXTRACT, ids.items);
            return false;
        }
        bool reached = false;
        for (int k = 0; k < ids.count; k++) {
            reached = reached || cbm_impact_walk_reached(walk, ids.items[k]);
        }
        cbm_free(CBM_MEM_CLASS_EXTRACT, ids.items);
        bool was_changed = false;
        for (int k = 0; k < changed_n; k++) {
            was_changed = was_changed || changed[k] == i;
        }
        if ((reached || was_changed) &&
            !te_trigger(c, suites[i].name, reached, was_changed, false)) {
            return false;
        }
    }
    return true;
}

/* Suites a `suites` rule names, among the runner's. */
static bool te_rule_triggers(te_ctx_t *c) {
    int n = 0;
    const cbm_test_runner_suite_t *runner = cbm_test_model_runner_suites(c->model, &n);
    int rule_count = 0;
    const cbm_test_rule_t *rules = cbm_test_policy_rules(c->policy, &rule_count);
    for (int m = 0; m < c->match_count; m++) {
        int r = c->matches[m].rule_index;
        if (rules[r].action != CBM_TEST_RULE_SUITES) {
            continue;
        }
        for (int i = 0; i < n; i++) {
            bool selected = false;
            if (!cbm_test_policy_rule_selects_suite(c->policy, r, runner[i].name, runner[i].perf,
                                                    &selected)) {
                return te_fallback(c, CBM_TEST_RESULT_FALLBACK_POLICY_EVALUATION_FAILED,
                                   "suite rule evaluation failed");
            }
            if (selected && !te_trigger(c, runner[i].name, false, false, true)) {
                return false;
            }
        }
    }
    return true;
}

/* A header changed at file level: every suite of every file including it
 * runs whole (replay4 616-621). */
static bool te_header_triggers(te_ctx_t *c) {
    int headers_n = 0;
    const char *const *headers = cbm_ti_seeds_file_level_headers(c->seeds, &headers_n);
    if (!headers_n) {
        return true;
    }
    int files = cbm_ti_source_file_count(c->source);
    bool *marked = cbm_arena_calloc(&c->arena, (size_t)files + 1);
    if (!marked) {
        return te_oom(c);
    }
    for (int h = 0; h < headers_n; h++) {
        int at = cbm_ti_source_find(c->source, headers[h]);
        if (at >= 0) {
            marked[at] = true;
        }
    }
    cbm_ti_source_include_closure(c->source, marked);
    int n = 0;
    const cbm_test_suite_t *suites = cbm_test_model_suites(c->model, &n);
    for (int i = 0; i < n; i++) {
        int at = cbm_ti_source_find(c->source, suites[i].file);
        if (at >= 0 && marked[at] && !te_trigger(c, suites[i].name, true, false, false)) {
            return false;
        }
    }
    return true;
}

/* ── Spawned processes (src/pipeline/pass_spawns.c) ─────────────────
 * A test that starts a program runs code no CALLS edge leads to: the program
 * runs its main, and main reaches everything. The graph does not resolve
 * which program a Process node is, so when the walk reaches a `main` outside
 * the tests (the product's entry), every Process node joins the walk as a
 * further seed, and SPAWNS, walked from the process to whoever starts it,
 * carries the reach to the tests that spawn. More, never fewer. */
static bool te_spawn_lift(te_ctx_t *c, cbm_impact_walk_t *walk) {
    cbm_node_t *mains = NULL;
    int main_count = 0;
    if (cbm_store_find_nodes_by_name(c->store, "test-impact", "main", &mains, &main_count) !=
        CBM_STORE_OK) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE, "graph read failed") &&
               false;
    }
    bool product_main = false;
    for (int i = 0; i < main_count && !product_main; i++) {
        product_main = mains[i].label && strcmp(mains[i].label, "Function") == 0 &&
                       mains[i].file_path && !te_test_file(c, mains[i].file_path) &&
                       cbm_impact_walk_reached(walk, mains[i].id);
    }
    cbm_store_free_nodes(mains, main_count);
    if (!product_main) {
        return true;
    }
    cbm_node_t *processes = NULL;
    int process_count = 0;
    if (cbm_store_find_nodes_by_label(c->store, "test-impact", "Process", &processes,
                                      &process_count) != CBM_STORE_OK) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE, "graph read failed") &&
               false;
    }
    te_ids_t seeds = {0};
    bool ok = true;
    for (int i = 0; ok && i < process_count; i++) {
        ok = te_ids_push(c, &seeds, processes[i].id);
    }
    cbm_store_free_nodes(processes, process_count);
    if (ok && seeds.count && cbm_impact_walk_run(walk, seeds.items, seeds.count) != CBM_STORE_OK) {
        ok = te_fallback(c, CBM_TEST_RESULT_FALLBACK_ENGINE_SATURATED, "the impact walk failed") &&
             false;
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, seeds.items);
    return ok;
}

bool te_reach(te_ctx_t *c) {
    int64_t *extra = NULL;
    int extra_count = 0;
    bool *fixture_cases = NULL;
    te_sinks_t sinks = {0};
    cbm_impact_walk_t *walk = NULL;
    bool ok = te_fixtures(c, &extra, &extra_count, &fixture_cases) && te_collect_sinks(c, &sinks) &&
              te_walk(c, &sinks, extra, extra_count, &walk) && te_parse_gaps(c, walk);
    if (ok) {
        for (int i = 0; i < sinks.runner_main.count; i++) {
            if (cbm_impact_walk_reached(walk, sinks.runner_main.items[i])) {
                ok = te_fallback(c, CBM_TEST_RESULT_FALLBACK_RUNNER_REACHED,
                                 "the change reaches the test runner's main");
                break;
            }
        }
    }
    /* After the runner check: the runner re-executing itself is a spawn too,
     * and reaching its main through that is no change to the runner. */
    ok = ok && te_spawn_lift(c, walk);
    ok = ok && te_reach_rows(c, walk, fixture_cases) && te_suite_triggers(c, walk) &&
         te_rule_triggers(c) && te_header_triggers(c);
    c->static_complete = ok;
    cbm_impact_walk_close(walk);
    cbm_free(CBM_MEM_CLASS_EXTRACT, extra);
    cbm_free(CBM_MEM_CLASS_EXTRACT, sinks.sinks.items);
    cbm_free(CBM_MEM_CLASS_EXTRACT, sinks.runner_main.items);
    return ok;
}
