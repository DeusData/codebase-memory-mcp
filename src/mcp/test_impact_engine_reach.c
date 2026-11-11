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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The c-cbm runner's child-process roles: a test re-executes the runner with
 * argv[1] = a role literal, and the runner's main dispatches it from its role
 * table — `TF_ROLE(phase, "literal", handler)` entries in a runner file — by
 * an exact argv[1] match, so a handler only ever runs in its own child. A
 * handler is a sink (reached, never expanded into main): reaching it selects
 * the tests that name its literal (te_role_lift). A handler the table does
 * not name, or a TF_ROLE line that does not parse, is no sink: reaching it
 * reaches main, which runs everything. */
#define TE_ROLE_MACRO "TF_ROLE("

/* SPAWNS carries reach from a Process node, which enters the walk through
 * te_spawn_lift alone, and from a script of the repository (its Module /
 * File, pass_shell.c) to whoever runs it. */
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

/* Whether `path` is a runner file: one whose main runs suites. */
static bool te_runner_file(te_ctx_t *c, const char *path) {
    int n = 0;
    const cbm_test_runner_suite_t *runner = cbm_test_model_runner_suites(c->model, &n);
    for (int i = 0; path && i < n; i++) {
        if (runner[i].file && strcmp(runner[i].file, path) == 0) {
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

static bool te_ids_has(const te_ids_t *a, int64_t id) {
    for (int i = 0; a && i < a->count; i++) {
        if (a->items[i] == id) {
            return true;
        }
    }
    return false;
}

static const char *te_skip_space(const char *p, const char *end) {
    while (p < end && (*p == ' ' || *p == '\t')) {
        p++;
    }
    return p;
}

static bool te_ident_char(char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
           ch == '_';
}

/* A `#define NAME ...` line: 1 with NAME copied out, 0 for any other line,
 * -1 for a define whose name does not fit or does not parse. */
static int te_define_name(const char *text, size_t len, char *name, size_t cap) {
    static const char keyword[] = "define";
    size_t klen = sizeof(keyword) - 1;
    const char *end = text + len;
    const char *p = te_skip_space(text, end);
    if (p >= end || *p != '#') {
        return 0;
    }
    p = te_skip_space(p + 1, end);
    if ((size_t)(end - p) <= klen || memcmp(p, keyword, klen) != 0 ||
        (p[klen] != ' ' && p[klen] != '\t')) {
        return 0;
    }
    p = te_skip_space(p + klen, end);
    const char *start = p;
    while (p < end && te_ident_char(*p)) {
        p++;
    }
    if (p == start || (size_t)(p - start) >= cap) {
        return -1;
    }
    memcpy(name, start, (size_t)(p - start));
    name[p - start] = '\0';
    return 1;
}

/* Whether `needle` occurs in the line; an identifier only as a whole word. */
static bool te_line_names(const char *text, size_t len, const char *needle, size_t nlen,
                          bool ident) {
    for (size_t i = 0; nlen && i + nlen <= len; i++) {
        if (memcmp(text + i, needle, nlen) == 0 &&
            (!ident || ((i == 0 || !te_ident_char(text[i - 1])) &&
                        (i + nlen == len || !te_ident_char(text[i + nlen]))))) {
            return true;
        }
    }
    return false;
}

/* A macro that names a role literal through further macros: past this depth
 * the tests that use it are unknown, and everything runs. */
#define TE_DEFINE_DEPTH_MAX 4

/* Every line of a test file that contains `needle`: its enclosing
 * definition is a fixture case or a seed — unless it is one of `exclude`
 * (a role handler naming its own literal is not a test that spawns it).
 * A `#define NAME "literal"` line names nobody: the code that uses NAME does,
 * so the scan follows NAME (a whole word) instead, through macros that name
 * macros. A define it cannot follow runs everything. In a role's scan
 * (`runner_cases`: the walk's sinks) a runner file's helper — the runner's own
 * use of a role's marker — still seeds the walk but names no spawner: there,
 * only a test case does (a sink, found by its start line like every case). */
static bool te_scan_text(te_ctx_t *c, const char *needle, bool ident, int depth, te_ids_t *seeds,
                         bool *cases, bool *hit, const te_ids_t *exclude,
                         const te_ids_t *runner_cases) {
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
            if (!text || !te_line_names(text, len, needle, nlen, ident)) {
                continue;
            }
            char name[256];
            int define = te_define_name(text, len, name, sizeof(name));
            if (define > 0 && strcmp(name, needle) == 0) {
                continue; /* the definition of the macro being followed */
            }
            if (define < 0 || (define > 0 && depth >= TE_DEFINE_DEPTH_MAX)) {
                char note[400];
                snprintf(note, sizeof(note),
                         "%s:%d defines a macro naming %s that the scan cannot follow", path, line,
                         needle);
                return te_fallback(c, CBM_TEST_RESULT_FALLBACK_SELECTION_UNAVAILABLE, note);
            }
            if (define > 0) {
                if (!te_scan_text(c, name, true, depth + 1, seeds, cases, hit, exclude,
                                  runner_cases)) {
                    return false;
                }
                continue;
            }
            bool ok = true;
            const cbm_ti_node_t *d = cbm_ti_graph_enclosing(c->graph, path, line, &ok);
            if (!ok) {
                return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                                   "graph read failed") &&
                       false;
            }
            if (!d || te_ids_has(exclude, d->id)) {
                continue;
            }
            int ci = te_is_function(d) ? te_case_index(c, path, d->name) : -1;
            if (!runner_cases || !te_runner_file(c, path) || te_ids_has(runner_cases, d->id)) {
                *hit = true;
            }
            if (ci >= 0) {
                cases[ci] = true;
            } else if (!te_ids_push(c, seeds, d->id)) {
                return false;
            }
        }
    }
    return true;
}

static bool te_scan_literal(te_ctx_t *c, const char *needle, te_ids_t *seeds, bool *cases,
                            bool *hit) {
    return te_scan_text(c, needle, false, 0, seeds, cases, hit, NULL, NULL);
}

/* The scan for a role's literal: `exclude` holds the handlers and main,
 * `sinks` the walk's sinks (every test case among them). */
static bool te_scan_literal_ex(te_ctx_t *c, const char *needle, te_ids_t *seeds, bool *cases,
                               bool *hit, const te_ids_t *exclude, const te_ids_t *sinks) {
    return te_scan_text(c, needle, false, 0, seeds, cases, hit, exclude, sinks);
}

/* One fixture path: the path itself, then each parent down to the rule's
 * key segment; each with and without its first segment. The first key some
 * test file mentions decides. */
#define TE_FIXTURE_KEY_MAX 4096

static bool te_fixture(te_ctx_t *c, const char *path, int min_segments, te_ids_t *seeds,
                       bool *cases, bool *hit) {
    char key[TE_FIXTURE_KEY_MAX];
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
        const char *path = c->fixture_paths[i];
        int min_segments = rules[r].key_segment + 1;
        if (rules[r].key_path) {
            min_segments = 1; /* the path itself only: never a parent directory */
            for (const char *p = path; *p; p++) {
                min_segments += *p == '/';
            }
        }
        bool hit = false;
        if (!te_fixture(c, path, min_segments, &seeds, cases, &hit)) {
            cbm_free(CBM_MEM_CLASS_EXTRACT, seeds.items);
            return false;
        }
        if (!hit && rules[r].unreferenced_ignore && strlen(path) < TE_FIXTURE_KEY_MAX) {
            continue; /* scanned, and no test names it: nothing can depend on it */
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
    const char *literal; /* what a test that starts the role's child names */
    int64_t id;          /* the handler's function node; one handler may have several */
    bool reached;
    bool scanned;
    bool named; /* some test or helper names the literal */
} te_role_t;

typedef struct {
    te_ids_t sinks;
    te_ids_t runner_main;
    te_role_t *roles;
    int role_count;
    int role_cap;
} te_sinks_t;

/* One `TF_ROLE(phase, "literal", handler)` line. false: no entry here (the
 * macro's own #define included); *bad: an entry that does not parse. */
static bool te_role_parse(const char *text, size_t len, char *literal, size_t literal_cap,
                          char *handler, size_t handler_cap, bool *bad) {
    *bad = false;
    const char *end = text + len;
    const char *p = te_skip_space(text, end);
    size_t macro_len = sizeof(TE_ROLE_MACRO) - 1;
    if (p >= end || *p == '#') {
        return false;
    }
    const char *at = NULL;
    for (const char *q = p; !at && (size_t)(end - q) >= macro_len; q++) {
        at = memcmp(q, TE_ROLE_MACRO, macro_len) == 0 ? q : NULL;
    }
    if (!at) {
        return false;
    }
    *bad = true;
    p = at + macro_len;
    while (p < end && *p != ',') {
        p++; /* the phase */
    }
    p = te_skip_space(p + 1, end);
    if (p >= end || *p != '"') {
        return true;
    }
    const char *lit = ++p;
    while (p < end && *p != '"' && *p != '\\') {
        p++;
    }
    if (p >= end || *p != '"' || p == lit || (size_t)(p - lit) >= literal_cap) {
        return true;
    }
    memcpy(literal, lit, (size_t)(p - lit));
    literal[p - lit] = '\0';
    p = te_skip_space(p + 1, end);
    if (p >= end || *p != ',') {
        return true;
    }
    p = te_skip_space(p + 1, end);
    const char *name = p;
    while (p < end && ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
                       (*p >= '0' && *p <= '9') || *p == '_')) {
        p++;
    }
    if (p == name || (size_t)(p - name) >= handler_cap) {
        return true;
    }
    memcpy(handler, name, (size_t)(p - name));
    handler[p - name] = '\0';
    p = te_skip_space(p, end);
    *bad = p >= end || *p != ')';
    return true;
}

/* The handler's function node: the one Function of that name in a runner or
 * test file. 0 when there is none or more than one. */
static bool te_role_handler(te_ctx_t *c, const char *name, int64_t *id) {
    cbm_node_t *nodes = NULL;
    int n = 0;
    *id = 0;
    if (cbm_store_find_nodes_by_name(c->store, "test-impact", name, &nodes, &n) != CBM_STORE_OK) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE, "graph read failed") &&
               false;
    }
    int found = 0;
    int runners = 0;
    const cbm_test_runner_suite_t *runner = cbm_test_model_runner_suites(c->model, &runners);
    for (int i = 0; i < n; i++) {
        bool in_tests = nodes[i].file_path && te_test_file(c, nodes[i].file_path);
        for (int r = 0; !in_tests && nodes[i].file_path && r < runners; r++) {
            in_tests = runner[r].file && strcmp(runner[r].file, nodes[i].file_path) == 0;
        }
        if (in_tests && nodes[i].label && strcmp(nodes[i].label, "Function") == 0) {
            *id = nodes[i].id;
            found++;
        }
    }
    cbm_store_free_nodes(nodes, n);
    if (found != 1) {
        *id = 0;
    }
    return true;
}

/* The role table of one runner file: each entry's handler becomes a sink.
 * main dispatches through the table, so no edge leads from main to a handler:
 * an entry the engine cannot read, or whose handler it cannot find, hides that
 * role's spawners, and everything runs. */
static bool te_runner_roles(te_ctx_t *c, const char *file, te_sinks_t *s) {
    int f = cbm_ti_source_find(c->source, file);
    int lines = f >= 0 ? cbm_ti_source_line_count(c->source, f) : 0;
    for (int line = 1; line <= lines; line++) {
        size_t len = 0;
        const char *text = cbm_ti_source_line(c->source, f, line, &len);
        char literal[256];
        char handler[256];
        bool bad = false;
        if (!text ||
            !te_role_parse(text, len, literal, sizeof(literal), handler, sizeof(handler), &bad)) {
            continue; /* not an entry */
        }
        char note[400];
        if (bad) {
            snprintf(note, sizeof(note), "%s:%d: a role table entry the engine cannot read", file,
                     line);
            return te_fallback(c, CBM_TEST_RESULT_FALLBACK_SELECTION_UNAVAILABLE, note);
        }
        int64_t id = 0;
        if (!te_role_handler(c, handler, &id)) {
            return false;
        }
        if (!id) {
            snprintf(note, sizeof(note),
                     "%s:%d: the role handler %s is no single function of the tests", file, line,
                     handler);
            return te_fallback(c, CBM_TEST_RESULT_FALLBACK_SELECTION_UNAVAILABLE, note);
        }
        if (!te_grow((void **)&s->roles, &s->role_cap, s->role_count, sizeof(*s->roles))) {
            return te_oom(c);
        }
        const char *owned = cbm_arena_strdup(&c->arena, literal);
        if (!owned) {
            return te_oom(c);
        }
        s->roles[s->role_count++] = (te_role_t){.literal = owned, .id = id};
        if (!te_ids_push(c, &s->sinks, id)) {
            return false;
        }
    }
    return true;
}

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
            if (!te_is_function(&nodes[k]) || nodes[k].variant_span ||
                strcmp(nodes[k].name, "main") != 0) {
                continue;
            }
            if (!te_ids_push(c, &s->sinks, nodes[k].id) ||
                !te_ids_push(c, &s->runner_main, nodes[k].id)) {
                return false;
            }
        }
        if (!te_runner_roles(c, runner[i].file, s)) {
            return false;
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
    /* A change in one platform variant of a function (foo_win.c / foo_posix.c)
     * reaches whoever calls any of its variants: a caller binds to one of
     * them, and the build of another platform runs another. */
    cbm_impact_policy_t policy = {.project = "test-impact",
                                  .edge_types = te_walk_types,
                                  .edge_type_count =
                                      (int)(sizeof(te_walk_types) / sizeof(te_walk_types[0])),
                                  .follow_routes = true,
                                  .follow_variant_keys = true};
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

/* ── Child-process roles (TF_ROLE) ───────────────────────────────────
 * A reached role handler runs in the child its tests spawn by its literal.
 * Mark the reached handlers, and every handler that calls a reached one (its
 * child runs it in process); scan each role's literal in the test files —
 * handlers and the runner's main excluded, so a handler naming its own
 * literal is no spawner — selecting the tests that name it directly and
 * walking on from helpers that do. A reached role no test names runs
 * everything. Repeat until nothing new is reached. */
static bool te_role_mark(te_ctx_t *c, cbm_impact_walk_t *walk, te_sinks_t *s, bool *grew) {
    for (int i = 0; i < s->role_count; i++) {
        if (!s->roles[i].reached && cbm_impact_walk_reached(walk, s->roles[i].id)) {
            s->roles[i].reached = *grew = true;
        }
    }
    for (bool chained = true; chained;) {
        chained = false;
        for (int i = 0; i < s->role_count; i++) {
            if (!s->roles[i].reached) {
                continue;
            }
            cbm_edge_t *edges = NULL;
            int n = 0;
            if (cbm_store_find_edges_by_target_type(c->store, s->roles[i].id, "CALLS", &edges,
                                                    &n) != CBM_STORE_OK) {
                return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                                   "graph read failed") &&
                       false;
            }
            for (int e = 0; e < n; e++) {
                for (int k = 0; k < s->role_count; k++) {
                    if (!s->roles[k].reached && s->roles[k].id == edges[e].source_id) {
                        s->roles[k].reached = chained = *grew = true;
                    }
                }
            }
            cbm_store_free_edges(edges, n);
        }
    }
    return true;
}

/* A reached handler none of whose literals any test names: its spawners are
 * unknown, so everything runs. One named literal names the handler's spawners
 * (the index worker: "--index-worker" or the CBM_INDEX_SUPERVISOR switch). */
static bool te_roles_named(te_ctx_t *c, const te_sinks_t *s) {
    for (int i = 0; i < s->role_count; i++) {
        const te_role_t *r = &s->roles[i];
        bool named = !r->reached;
        for (int k = 0; !named && k < s->role_count; k++) {
            named = s->roles[k].id == r->id && s->roles[k].named;
        }
        if (!named) {
            char note[320];
            snprintf(note, sizeof(note),
                     "the change reaches the test-runner role %s, which no test names", r->literal);
            return te_fallback(c, CBM_TEST_RESULT_FALLBACK_RUNNER_REACHED, note);
        }
    }
    return true;
}

static bool te_role_lift(te_ctx_t *c, cbm_impact_walk_t *walk, te_sinks_t *s, bool *cases) {
    te_ids_t handlers = {0};
    bool ok = true;
    for (int i = 0; ok && i < s->role_count; i++) {
        ok = te_ids_push(c, &handlers, s->roles[i].id);
    }
    for (int i = 0; ok && i < s->runner_main.count; i++) {
        ok = te_ids_push(c, &handlers, s->runner_main.items[i]);
    }
    for (bool grew = ok; ok && grew;) {
        grew = false;
        ok = te_role_mark(c, walk, s, &grew);
        for (int i = 0; ok && i < s->role_count; i++) {
            te_role_t *r = &s->roles[i];
            if (!r->reached || r->scanned) {
                continue;
            }
            r->scanned = true;
            te_ids_t seeds = {0};
            ok = te_scan_literal_ex(c, r->literal, &seeds, cases, &r->named, &handlers, &s->sinks);
            if (ok && seeds.count) {
                ok = cbm_impact_walk_run(walk, seeds.items, seeds.count) == CBM_STORE_OK ||
                     (te_fallback(c, CBM_TEST_RESULT_FALLBACK_ENGINE_SATURATED,
                                  "the impact walk failed") &&
                      false);
                grew = true;
            }
            cbm_free(CBM_MEM_CLASS_EXTRACT, seeds.items);
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, handlers.items);
    return ok && te_roles_named(c, s);
}

/* The last two segments of a qualified name (`test_main.main`), or all of it. */
static const char *te_short_qn(const char *qn) {
    const char *last = strrchr(qn, '.');
    if (!last) {
        return qn;
    }
    const char *start = qn;
    for (const char *p = qn; p < last; p++) {
        if (*p == '.') {
            start = p + 1;
        }
    }
    return start;
}

/* prefix + the call chain the walk took from the changed code to `id`, in call
 * order (`id` first: "test_main.main -> a.b -> ... -> changed.fn"), cut short
 * with "..." when it does not fit. */
static void te_call_chain(const cbm_impact_walk_t *walk, int64_t id, const char *prefix, char *out,
                          size_t cap) {
    int used = snprintf(out, cap, "%s", prefix);
    for (int step = 0; id && used >= 0 && (size_t)used < cap && step < 64; step++) {
        const cbm_impact_hit_t *hit = cbm_impact_walk_hit(walk, id);
        if (!hit) {
            break;
        }
        const char *name = hit->qualified_name ? te_short_qn(hit->qualified_name) : "?";
        int n = snprintf(out + used, cap - (size_t)used, "%s%s", step ? " -> " : "", name);
        if (n < 0 || (size_t)n >= cap - (size_t)used) {
            if (cap > 4) {
                memcpy(out + cap - 4, "...", 4);
            }
            return;
        }
        used += n;
        id = hit->via_id;
    }
}

/* Whether the walk reaches the runner's main through code that can run, and
 * through what: *way_in is main itself (a changed main, or a step that is no
 * edge: a platform variant, a route), the reached node main has a walked edge
 * to, or 0. An edge into a Field named like a suite main runs is no way in:
 * the C extractor takes the macro argument of RUN_SELECTED_SUITE(mcp) for a
 * use of some same-named data member (`char mcp[]` in an unrelated struct).
 * Nor is an edge into a sink (a role handler main calls, a suite, a test):
 * the walk selects whoever runs it and never expands it. */
static bool te_main_reached(te_ctx_t *c, const cbm_impact_walk_t *walk, const te_sinks_t *sinks,
                            int64_t main_id, int64_t *way_in) {
    *way_in = 0;
    const cbm_impact_hit_t *self = cbm_impact_walk_hit(walk, main_id);
    if (!self) {
        return true;
    }
    bool edge_step = false;
    for (size_t t = 0; self->via_edge && t < sizeof(te_walk_types) / sizeof(te_walk_types[0]);
         t++) {
        edge_step = edge_step || strcmp(self->via_edge, te_walk_types[t]) == 0;
    }
    if (!edge_step) {
        *way_in = main_id;
        return true;
    }
    cbm_edge_t *edges = NULL;
    int n = 0;
    if (cbm_store_find_edges_by_source(c->store, main_id, &edges, &n) != CBM_STORE_OK) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE, "graph read failed") &&
               false;
    }
    for (int e = 0; !*way_in && e < n; e++) {
        const cbm_impact_hit_t *to = cbm_impact_walk_hit(walk, edges[e].target_id);
        bool walked = false;
        for (size_t t = 0;
             to && edges[e].type && t < sizeof(te_walk_types) / sizeof(te_walk_types[0]); t++) {
            walked = walked || strcmp(edges[e].type, te_walk_types[t]) == 0;
        }
        const char *name = walked && to->qualified_name ? strrchr(to->qualified_name, '.') : NULL;
        bool suite_arg =
            name && to->label && strcmp(to->label, "Field") == 0 && te_is_runner_suite(c, name + 1);
        if (walked && !suite_arg && !te_ids_has(&sinks->sinks, edges[e].target_id)) {
            *way_in = edges[e].target_id;
        }
    }
    cbm_store_free_edges(edges, n);
    return true;
}

/* Role lifts and parse gaps feed each other: a gap that a lift's walk names
 * can reach a further handler, and a lift can reach a further gap. Both run
 * until the walk stops growing (it only grows, so this ends). */
static bool te_lift_fixpoint(te_ctx_t *c, cbm_impact_walk_t *walk, te_sinks_t *sinks,
                             bool *fixture_cases) {
    bool ok = true;
    for (int before = -1; ok && cbm_impact_walk_count(walk) != before;) {
        before = cbm_impact_walk_count(walk);
        ok = te_parse_gaps(c, walk) && te_role_lift(c, walk, sinks, fixture_cases);
    }
    return ok;
}

bool te_reach(te_ctx_t *c) {
    int64_t *extra = NULL;
    int extra_count = 0;
    bool *fixture_cases = NULL;
    te_sinks_t sinks = {0};
    cbm_impact_walk_t *walk = NULL;
    bool ok = te_fixtures(c, &extra, &extra_count, &fixture_cases) && te_collect_sinks(c, &sinks) &&
              te_walk(c, &sinks, extra, extra_count, &walk) &&
              te_lift_fixpoint(c, walk, &sinks, fixture_cases);
    for (int i = 0; ok && i < sinks.runner_main.count; i++) {
        int64_t way_in = 0;
        ok = te_main_reached(c, walk, &sinks, sinks.runner_main.items[i], &way_in);
        if (ok && way_in) {
            const cbm_impact_hit_t *m = cbm_impact_walk_hit(walk, sinks.runner_main.items[i]);
            const char *main_name =
                m && m->qualified_name ? te_short_qn(m->qualified_name) : "main";
            bool direct = way_in == sinks.runner_main.items[i];
            char note[400];
            int used =
                snprintf(note, sizeof(note), "the change reaches the test runner's main: %s%s",
                         direct ? "" : main_name, direct ? "" : " -> ");
            if (used < 0) {
                note[0] = '\0';
                used = 0;
            } else if ((size_t)used >= sizeof(note)) {
                used = (int)sizeof(note) - 1; /* a truncated prefix: no room for the chain */
            }
            te_call_chain(walk, way_in, "", note + used, sizeof(note) - (size_t)used);
            ok = te_fallback(c, CBM_TEST_RESULT_FALLBACK_RUNNER_REACHED, note);
            break;
        }
    }
    /* After the runner check: the runner re-executing itself is a spawn too,
     * and reaching its main through that is no change to the runner. What the
     * spawn lift reaches is lifted like the rest: a handler that starts the
     * product is first reached here. */
    ok = ok && te_spawn_lift(c, walk) && te_lift_fixpoint(c, walk, &sinks, fixture_cases);
    ok = ok && te_reach_rows(c, walk, fixture_cases) && te_suite_triggers(c, walk) &&
         te_rule_triggers(c) && te_header_triggers(c);
    c->static_complete = ok;
    cbm_impact_walk_close(walk);
    cbm_free(CBM_MEM_CLASS_EXTRACT, extra);
    cbm_free(CBM_MEM_CLASS_EXTRACT, sinks.sinks.items);
    cbm_free(CBM_MEM_CLASS_EXTRACT, sinks.runner_main.items);
    cbm_free(CBM_MEM_CLASS_EXTRACT, sinks.roles);
    return ok;
}
