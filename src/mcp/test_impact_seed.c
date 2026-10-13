/*
 * test_impact_seed.c — what a change seeds in the graph.
 *
 * Each step names the lines of the reference (scratchpad tia3/replay4.py) it
 * ports, so a differential check can be read side by side.
 */
#include "mcp/test_impact_seed.h"

#include "foundation/arena.h"
#include "foundation/hash_table.h"
#include "foundation/mem_core.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Growable arrays (memory core) ───────────────────────────────── */

static bool ts_grow(void **items, int *cap, int count, size_t size) {
    if (count < *cap) {
        return true;
    }
    int next = *cap ? *cap * 2 : 16;
    void *grown = cbm_realloc(CBM_MEM_CLASS_EXTRACT, *items, (size_t)next * size);
    if (!grown) {
        return false;
    }
    *items = grown;
    *cap = next;
    return true;
}

typedef struct {
    int64_t *items;
    int count;
    int cap;
} ts_ids_t;

typedef struct {
    int *items;
    int count;
    int cap;
} ts_ints_t;

typedef struct {
    const char **items;
    int count;
    int cap;
} ts_strs_t;

static bool ts_ids_push(ts_ids_t *a, int64_t v) {
    if (!ts_grow((void **)&a->items, &a->cap, a->count, sizeof(*a->items))) {
        return false;
    }
    a->items[a->count++] = v;
    return true;
}

static bool ts_ints_push(ts_ints_t *a, int v) {
    if (!ts_grow((void **)&a->items, &a->cap, a->count, sizeof(*a->items))) {
        return false;
    }
    a->items[a->count++] = v;
    return true;
}

static bool ts_strs_push(ts_strs_t *a, const char *v) {
    if (!ts_grow((void **)&a->items, &a->cap, a->count, sizeof(*a->items))) {
        return false;
    }
    a->items[a->count++] = v;
    return true;
}

/* ── The graph, per file ─────────────────────────────────────────── */

typedef struct {
    cbm_ti_node_t *nodes;
    int count;
} ts_file_t;

struct cbm_ti_graph {
    cbm_store_t *store;
    const char *project;
    CBMArena arena;
    CBMHashTable *files; /* path -> ts_file_t* (arena) */
    CBMHashTable *names; /* name -> 1 present / 2 absent */
};

static bool ts_seedless(const char *label) {
    static const char *const seedless[] = {"File",    "Folder", "Project", "Module",  "Package",
                                           "Section", "Route",  "Process", "Resource"};
    for (size_t i = 0; i < sizeof(seedless) / sizeof(seedless[0]); i++) {
        if (label && strcmp(label, seedless[i]) == 0) {
            return true;
        }
    }
    return false;
}

cbm_ti_graph_t *cbm_ti_graph_new(cbm_store_t *store, const char *project) {
    if (!store || !project) {
        return NULL;
    }
    cbm_ti_graph_t *g = cbm_alloc(CBM_MEM_CLASS_EXTRACT, sizeof(*g));
    if (!g) {
        return NULL;
    }
    memset(g, 0, sizeof(*g));
    cbm_arena_init(&g->arena);
    g->store = store;
    g->project = cbm_arena_strdup(&g->arena, project);
    g->files = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 0);
    g->names = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 0);
    if (!g->project || !g->files || !g->names) {
        cbm_ti_graph_free(g);
        return NULL;
    }
    return g;
}

void cbm_ti_graph_free(cbm_ti_graph_t *g) {
    if (!g) {
        return;
    }
    if (g->files) {
        cbm_ht_free(g->files);
    }
    if (g->names) {
        cbm_ht_free(g->names);
    }
    cbm_arena_destroy(&g->arena);
    cbm_free(CBM_MEM_CLASS_EXTRACT, g);
}

static int ts_node_compare(const void *left, const void *right) {
    const cbm_ti_node_t *a = left;
    const cbm_ti_node_t *b = right;
    if (a->start_line != b->start_line) {
        return a->start_line < b->start_line ? -1 : 1;
    }
    if (a->end_line != b->end_line) {
        return a->end_line < b->end_line ? -1 : 1;
    }
    return a->id < b->id ? -1 : (a->id > b->id ? 1 : 0);
}

static bool ts_node_from_row(cbm_ti_graph_t *g, cbm_ti_node_t *n, const cbm_node_t *row) {
    n->id = row->id;
    n->label = cbm_arena_strdup(&g->arena, row->label ? row->label : "");
    n->name = cbm_arena_strdup(&g->arena, row->name ? row->name : "");
    n->qualified_name = cbm_arena_strdup(&g->arena, row->qualified_name ? row->qualified_name : "");
    n->start_line = row->start_line;
    n->end_line = row->end_line;
    n->variant_span = false;
    return n->label && n->name && n->qualified_name;
}

static bool ts_has_span(const cbm_ti_node_t *nodes, int count, const cbm_node_t *row) {
    for (int i = 0; i < count; i++) {
        if (nodes[i].id == row->id && nodes[i].start_line == row->start_line &&
            nodes[i].end_line == row->end_line) {
            return true;
        }
    }
    return false;
}

/* The file's definitions, each with every span it has in the file: a
 * definition written once per #if branch or platform file is ONE node
 * (graph_buffer.c "Definition variants") whose own span is one variant, so
 * its other variants here (other branches, or a node whose file_path names
 * another platform file) join as further spans of the same node. A change
 * in any variant then seeds that definition. */
static ts_file_t *ts_load_file(cbm_ti_graph_t *g, const char *file) {
    cbm_node_t *rows = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_file(g->store, g->project, file, &rows, &count) != CBM_STORE_OK) {
        return NULL;
    }
    cbm_node_t *spans = NULL;
    int span_count = 0;
    if (cbm_store_find_variant_spans_by_file(g->store, g->project, file, &spans, &span_count) !=
        CBM_STORE_OK) {
        cbm_store_free_nodes(rows, count);
        return NULL;
    }
    int total = count + span_count;
    ts_file_t *f = cbm_arena_calloc(&g->arena, sizeof(*f));
    cbm_ti_node_t *nodes = cbm_arena_alloc(&g->arena, (size_t)(total ? total : 1) * sizeof(*nodes));
    const char *key = cbm_arena_strdup(&g->arena, file);
    bool ok = f && nodes && key;
    int kept = 0;
    for (int i = 0; ok && i < count; i++) {
        if (!ts_seedless(rows[i].label)) {
            ok = ts_node_from_row(g, &nodes[kept++], &rows[i]);
        }
    }
    for (int i = 0; ok && i < span_count; i++) {
        if (!ts_seedless(spans[i].label) && !ts_has_span(nodes, kept, &spans[i])) {
            bool listed = false;
            for (int j = 0; j < kept && !listed; j++) {
                listed = nodes[j].id == spans[i].id;
            }
            ok = ts_node_from_row(g, &nodes[kept], &spans[i]);
            nodes[kept++].variant_span = listed;
        }
    }
    cbm_store_free_nodes(rows, count);
    cbm_store_free_nodes(spans, span_count);
    if (!ok) {
        return NULL;
    }
    if (kept > 1) {
        qsort(nodes, (size_t)kept, sizeof(*nodes), ts_node_compare);
    }
    f->nodes = nodes;
    f->count = kept;
    cbm_ht_set(g->files, key, f);
    return f;
}

const cbm_ti_node_t *cbm_ti_graph_file(cbm_ti_graph_t *g, const char *file, int *count, bool *ok) {
    *count = 0;
    *ok = g && file;
    if (!*ok) {
        return NULL;
    }
    ts_file_t *f = cbm_ht_get(g->files, file);
    if (!f) {
        f = ts_load_file(g, file);
    }
    if (!f) {
        *ok = false;
        return NULL;
    }
    *count = f->count;
    return f->nodes;
}

/* replay4 `_enclosing`: the smallest span containing the line; among equal
 * spans the first in id order (the reference iterates the store's rows). */
const cbm_ti_node_t *cbm_ti_graph_enclosing(cbm_ti_graph_t *g, const char *file, int line,
                                            bool *ok) {
    int count = 0;
    const cbm_ti_node_t *nodes = cbm_ti_graph_file(g, file, &count, ok);
    const cbm_ti_node_t *best = NULL;
    for (int i = 0; i < count; i++) {
        const cbm_ti_node_t *n = &nodes[i];
        if (n->start_line > line || n->end_line < line) {
            continue;
        }
        int span = n->end_line - n->start_line;
        int best_span = best ? best->end_line - best->start_line : 0;
        if (!best || span < best_span || (span == best_span && n->id < best->id)) {
            best = n;
        }
    }
    return best;
}

bool cbm_ti_graph_has_name(cbm_ti_graph_t *g, const char *name, bool *ok) {
    *ok = g && name;
    if (!*ok) {
        return false;
    }
    intptr_t known = (intptr_t)cbm_ht_get(g->names, name);
    if (known) {
        return known == 1;
    }
    cbm_node_t *rows = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_name(g->store, g->project, name, &rows, &count) != CBM_STORE_OK) {
        *ok = false;
        return false;
    }
    cbm_store_free_nodes(rows, count);
    const char *key = cbm_arena_strdup(&g->arena, name);
    if (!key) {
        *ok = false;
        return false;
    }
    cbm_ht_set(g->names, key, (void *)(intptr_t)(count > 0 ? 1 : 2));
    return count > 0;
}

/* ── The seeding state ───────────────────────────────────────────── */

struct cbm_ti_seeds {
    CBMArena arena;
    ts_ids_t nodes;
    ts_ints_t cases;
    ts_ints_t suites;
    ts_strs_t file_level;
    ts_strs_t deleted;
};

typedef struct {
    const cbm_ti_seed_input_t *in;
    cbm_ti_seeds_t *out;
    CBMHashTable *seen_nodes;   /* "id" -> 1 */
    CBMHashTable *whole;        /* path -> 1 */
    ts_strs_t whole_list;       /* paths in insertion order */
    CBMHashTable *deleted;      /* name -> 1 */
    CBMHashTable *case_index;   /* "file\x1fname" -> case index + 1 */
    CBMHashTable *suite_index;  /* "file\x1fname" -> suite index + 1 */
    CBMHashTable *changed_path; /* path -> 1 */
    bool *case_changed;
    bool *suite_changed;
    int case_count;
    int suite_count;
    cbm_ti_seed_status_t status;
} ts_state_t;

static bool ts_fail(ts_state_t *st, cbm_ti_seed_status_t status) {
    if (st->status == CBM_TI_SEED_OK) {
        st->status = status;
    }
    return false;
}

static const char *ts_key(ts_state_t *st, const char *file, const char *name) {
    size_t a = strlen(file);
    size_t b = strlen(name);
    char *key = cbm_arena_alloc(&st->out->arena, a + b + 2);
    if (!key) {
        return NULL;
    }
    memcpy(key, file, a);
    key[a] = '\x1f';
    memcpy(key + a + 1, name, b + 1);
    return key;
}

static bool ts_add_node(ts_state_t *st, int64_t id) {
    char key[32];
    snprintf(key, sizeof(key), "%lld", (long long)id);
    if (cbm_ht_get(st->seen_nodes, key)) {
        return true;
    }
    const char *owned = cbm_arena_strdup(&st->out->arena, key);
    if (!owned || !ts_ids_push(&st->out->nodes, id)) {
        return ts_fail(st, CBM_TI_SEED_OOM);
    }
    cbm_ht_set(st->seen_nodes, owned, (void *)1);
    return true;
}

static bool ts_add_whole(ts_state_t *st, const char *path) {
    if (cbm_ht_get(st->whole, path)) {
        return true;
    }
    const char *owned = cbm_arena_strdup(&st->out->arena, path);
    if (!owned || !ts_strs_push(&st->whole_list, owned)) {
        return ts_fail(st, CBM_TI_SEED_OOM);
    }
    cbm_ht_set(st->whole, owned, (void *)1);
    return true;
}

static bool ts_add_deleted(ts_state_t *st, const char *line, cbm_ti_span_t span) {
    char *name = cbm_arena_strndup(&st->out->arena, line + span.start, span.len);
    if (!name) {
        return ts_fail(st, CBM_TI_SEED_OOM);
    }
    if (!cbm_ht_get(st->deleted, name)) {
        cbm_ht_set(st->deleted, name, (void *)1);
        if (!ts_strs_push(&st->out->deleted, name)) {
            return ts_fail(st, CBM_TI_SEED_OOM);
        }
    }
    return true;
}

/* A test case or suite body is reported, never seeded (replay4 558-566). */
static int ts_case_of(ts_state_t *st, const char *file, const cbm_ti_node_t *n) {
    if (strcmp(n->label, "Function") != 0 && strcmp(n->label, "Method") != 0) {
        return -1;
    }
    char stack[512];
    size_t a = strlen(file);
    size_t b = strlen(n->name);
    if (a + b + 2 > sizeof(stack)) {
        return -1;
    }
    memcpy(stack, file, a);
    stack[a] = '\x1f';
    memcpy(stack + a + 1, n->name, b + 1);
    return (int)(intptr_t)cbm_ht_get(st->case_index, stack) - 1;
}

static int ts_suite_of(ts_state_t *st, const char *file, const cbm_ti_node_t *n) {
    if (strcmp(n->label, "Function") != 0 && strcmp(n->label, "Method") != 0) {
        return -1;
    }
    char stack[512];
    size_t a = strlen(file);
    size_t b = strlen(n->name);
    if (a + b + 2 > sizeof(stack)) {
        return -1;
    }
    memcpy(stack, file, a);
    stack[a] = '\x1f';
    memcpy(stack + a + 1, n->name, b + 1);
    return (int)(intptr_t)cbm_ht_get(st->suite_index, stack) - 1;
}

/* The names a removed line declared (replay4 548-552): define, tag
 * declaration or prototype, when the line does not end in ';'. Every
 * pattern that matches adds its name. */
static bool ts_note_removed_line(ts_state_t *st, const char *line) {
    size_t n = strlen(line);
    size_t e = n;
    while (e > 0 && (line[e - 1] == ' ' || line[e - 1] == '\t' || line[e - 1] == '\r' ||
                     line[e - 1] == '\f' || line[e - 1] == '\v')) {
        e--;
    }
    if (e > 0 && line[e - 1] == ';') {
        return true;
    }
    cbm_ti_span_t span;
    if (cbm_ti_match_define(line, n, &span, NULL) && !ts_add_deleted(st, line, span)) {
        return false;
    }
    if (cbm_ti_match_tag_declaration(line, n, &span) && !ts_add_deleted(st, line, span)) {
        return false;
    }
    if (cbm_ti_match_prototype(line, n, &span) && !ts_add_deleted(st, line, span)) {
        return false;
    }
    return true;
}

static bool ts_comment_only(const cbm_diff_hunk_t *h) {
    for (int i = 0; i < h->added_count; i++) {
        if (!cbm_ti_line_is_comment(h->added[i], strlen(h->added[i]))) {
            return false;
        }
    }
    for (int i = 0; i < h->removed_count; i++) {
        if (!cbm_ti_line_is_comment(h->removed[i], strlen(h->removed[i]))) {
            return false;
        }
    }
    return true;
}

/* A seed, or a changed case/suite (replay4 558-566). */
static bool ts_take_definition(ts_state_t *st, const char *file, const cbm_ti_node_t *n) {
    int c = ts_case_of(st, file, n);
    if (c >= 0) {
        st->case_changed[c] = true;
        return true;
    }
    int s = ts_suite_of(st, file, n);
    if (s >= 0) {
        st->suite_changed[s] = true;
        return true;
    }
    return ts_add_node(st, n->id);
}

/* The definition around a pure deletion: at the line the removal follows,
 * else the next one (replay4 546). */
static const cbm_ti_node_t *ts_around_deletion(ts_state_t *st, const char *file, int line,
                                               bool *ok) {
    const cbm_ti_node_t *d = cbm_ti_graph_enclosing(st->in->graph, file, line, ok);
    if (!d && *ok) {
        d = cbm_ti_graph_enclosing(st->in->graph, file, line + 1, ok);
    }
    return d;
}

/* One hunk of a source (not header) file (replay4 540-566). */
static bool ts_source_hunk(ts_state_t *st, const char *file, const cbm_diff_hunk_t *h) {
    if (ts_comment_only(h)) {
        return true;
    }
    bool ok = true;
    int overlapped = 0;
    if (h->count > 0) {
        int count = 0;
        const cbm_ti_node_t *nodes = cbm_ti_graph_file(st->in->graph, file, &count, &ok);
        int last = h->start + h->count - 1;
        for (int i = 0; ok && i < count; i++) {
            if (nodes[i].start_line <= last && nodes[i].end_line >= h->start) {
                overlapped++;
                ok = ts_take_definition(st, file, &nodes[i]);
            }
        }
    } else {
        const cbm_ti_node_t *d = ts_around_deletion(st, file, h->start, &ok);
        if (d && ok) {
            overlapped = 1;
            ok = ts_take_definition(st, file, d);
        }
    }
    if (!ok) {
        return st->status == CBM_TI_SEED_OK ? ts_fail(st, CBM_TI_SEED_STORE) : false;
    }
    for (int i = 0; i < h->removed_count; i++) {
        if (!ts_note_removed_line(st, h->removed[i])) {
            return false;
        }
    }
    return overlapped ? true : ts_add_whole(st, file);
}

/* The names a deleted file declared (replay4 521-528). */
static bool ts_deleted_file(ts_state_t *st, const cbm_ti_change_t *c) {
    const char *p = c->base_text ? c->base_text : "";
    const char *end = p + c->base_len;
    while (p <= end) {
        const char *nl = memchr(p, '\n', (size_t)(end - p));
        size_t len = nl ? (size_t)(nl - p) : (size_t)(end - p);
        char *line = cbm_arena_strndup(&st->out->arena, p, len);
        if (!line) {
            return ts_fail(st, CBM_TI_SEED_OOM);
        }
        if (!ts_note_removed_line(st, line)) {
            return false;
        }
        if (!nl) {
            break;
        }
        p = nl + 1;
    }
    return true;
}

/* ── Header symbol propagation (replay4 header_seeds, 290-374) ──── */

typedef struct {
    CBMHashTable *set; /* name -> 1 */
    ts_strs_t list;
} ts_names_t;

static bool ts_names_add(ts_state_t *st, ts_names_t *names, const char *name, size_t len) {
    char *copy = cbm_arena_strndup(&st->out->arena, name, len);
    if (!copy) {
        return ts_fail(st, CBM_TI_SEED_OOM);
    }
    if (cbm_ht_get(names->set, copy)) {
        return true;
    }
    if (!ts_strs_push(&names->list, copy)) {
        return ts_fail(st, CBM_TI_SEED_OOM);
    }
    cbm_ht_set(names->set, copy, (void *)1);
    return true;
}

static bool ts_names_has(const ts_names_t *names, const char *name) {
    return cbm_ht_get(names->set, name) != NULL;
}

/* Classify the lines of one header hunk. false = error; *file_level set when
 * a line cannot be classified or is a preprocessor conditional. */
static bool ts_header_hunk(ts_state_t *st, const char *header, const cbm_diff_hunk_t *h,
                           ts_names_t *changed, bool *file_level) {
    bool ok = true;
    int overlap_count = 0;
    if (h->count > 0) {
        int count = 0;
        const cbm_ti_node_t *nodes = cbm_ti_graph_file(st->in->graph, header, &count, &ok);
        int last = h->start + h->count - 1;
        for (int i = 0; ok && i < count; i++) {
            if (nodes[i].start_line <= last && nodes[i].end_line >= h->start) {
                overlap_count++;
                ok = ts_names_add(st, changed, nodes[i].name, strlen(nodes[i].name));
            }
        }
    }
    int total = h->added_count + h->removed_count;
    for (int k = 0; ok && k < total; k++) {
        const char *ln = k < h->added_count ? h->added[k] : h->removed[k - h->added_count];
        size_t len = strlen(ln);
        if (cbm_ti_line_is_comment(ln, len)) {
            continue;
        }
        if (cbm_ti_line_is_pp_condition(ln, len)) {
            *file_level = true;
            continue;
        }
        cbm_ti_span_t span;
        if (cbm_ti_match_declaration(ln, len, &span)) {
            ok = ts_names_add(st, changed, ln + span.start, span.len);
            continue;
        }
        if (h->count > 0 && overlap_count > 0) {
            continue; /* inside a definition at head: named by the overlap */
        }
        if (h->count == 0) {
            const cbm_ti_node_t *d = ts_around_deletion(st, header, h->start, &ok);
            if (d && ok) {
                ok = ts_names_add(st, changed, d->name, strlen(d->name));
                continue;
            }
        }
        if (ok) {
            *file_level = true;
        }
    }
    if (!ok && st->status == CBM_TI_SEED_OK) {
        ts_fail(st, CBM_TI_SEED_STORE);
    }
    return ok;
}

static bool ts_starts_with(const char *s, const char *prefix) {
    return strncmp(s, prefix, strlen(prefix)) == 0;
}

static bool ts_ends_with(const char *s, const char *suffix) {
    size_t a = strlen(s);
    size_t b = strlen(suffix);
    return a >= b && strcmp(s + a - b, suffix) == 0;
}

static bool ts_macro_hit(const cbm_ti_macro_t *m, const ts_names_t *changed) {
    for (int i = 0; i < m->token_count; i++) {
        if (ts_names_has(changed, m->tokens[i])) {
            return true;
        }
    }
    for (int k = 0; k < changed->list.count; k++) {
        const char *c = changed->list.items[k];
        for (int i = 0; i < m->paste_prefix_count; i++) {
            if (ts_starts_with(c, m->paste_prefixes[i])) {
                return true;
            }
        }
        for (int i = 0; i < m->paste_suffix_count; i++) {
            if (ts_ends_with(c, m->paste_suffixes[i])) {
                return true;
            }
        }
    }
    return false;
}

/* Close the changed names over the headers' macros (replay4 328-345). */
static bool ts_macro_fixpoint(ts_state_t *st, ts_names_t *changed) {
    int count = 0;
    const cbm_ti_macro_t *macros = cbm_ti_source_macros(st->in->source, &count);
    bool grew = true;
    while (grew) {
        grew = false;
        for (int i = 0; i < count; i++) {
            if (ts_names_has(changed, macros[i].name) || !ts_macro_hit(&macros[i], changed)) {
                continue;
            }
            if (!ts_names_add(st, changed, macros[i].name, strlen(macros[i].name))) {
                return false;
            }
            grew = true;
        }
    }
    return true;
}

static bool ts_is_function(const cbm_ti_node_t *n) {
    return strcmp(n->label, "Function") == 0 || strcmp(n->label, "Method") == 0;
}

/* One occurrence of a propagated name in a file including the header
 * (replay4 355-373). */
static bool ts_header_occurrence(ts_state_t *st, int file, int line, ts_names_t *work) {
    cbm_ti_source_t *src = st->in->source;
    const char *path = cbm_ti_source_path(src, file);
    bool ok = true;
    const cbm_ti_node_t *d = cbm_ti_graph_enclosing(st->in->graph, path, line, &ok);
    if (!ok) {
        return ts_fail(st, CBM_TI_SEED_STORE);
    }
    if (d) {
        if (!ts_add_node(st, d->id)) {
            return false;
        }
        if (cbm_ti_source_is_header(src, file) && !ts_is_function(d) && d->name[0]) {
            return ts_names_add(st, work, d->name, strlen(d->name));
        }
        return true;
    }
    if (!cbm_ti_source_is_header(src, file)) {
        return ts_add_whole(st, path);
    }
    size_t len = 0;
    const char *text = cbm_ti_source_line(src, file, line, &len);
    cbm_ti_span_t span;
    if (text && cbm_ti_match_declaration(text, len, &span)) {
        return ts_names_add(st, work, text + span.start, span.len);
    }
    return true;
}

/* Seed what mentions a propagated name inside the include closure; names
 * found on the way join the work list (replay4 346-374). The work list is the
 * set of names in insertion order, so each is scanned once. */
static bool ts_header_scan(ts_state_t *st, const bool *closure, ts_names_t *work) {
    cbm_ti_source_t *src = st->in->source;
    if (!cbm_ti_source_build_occurrences(src)) {
        return ts_fail(st, CBM_TI_SEED_OOM);
    }
    for (int k = 0; k < work->list.count; k++) {
        const char *name = work->list.items[k];
        int count = 0;
        const cbm_ti_place_t *places = cbm_ti_source_occurrences(src, name, strlen(name), &count);
        for (int i = 0; i < count; i++) {
            if (closure[places[i].file] &&
                !ts_header_occurrence(st, places[i].file, places[i].line, work)) {
                return false;
            }
        }
    }
    return true;
}

static bool ts_header(ts_state_t *st, const cbm_ti_change_t *c) {
    ts_names_t changed = {.set = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 0)};
    bool *closure = NULL;
    bool file_level = false;
    bool ok = changed.set != NULL || ts_fail(st, CBM_TI_SEED_OOM);
    for (int i = 0; ok && i < c->hunk_count; i++) {
        ok = ts_header_hunk(st, c->path, &c->hunks[i], &changed, &file_level);
    }
    if (ok && (file_level || c->whole_file)) {
        const char *owned = cbm_arena_strdup(&st->out->arena, c->path);
        ok = (owned && ts_strs_push(&st->out->file_level, owned)) || ts_fail(st, CBM_TI_SEED_OOM);
    } else if (ok) {
        int files = cbm_ti_source_file_count(st->in->source);
        int at = cbm_ti_source_find(st->in->source, c->path);
        closure = cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)(files ? files : 1) * sizeof(bool));
        ok = closure != NULL || ts_fail(st, CBM_TI_SEED_OOM);
        if (ok) {
            memset(closure, 0, (size_t)(files ? files : 1) * sizeof(bool));
            if (at >= 0) {
                closure[at] = true;
            }
            cbm_ti_source_include_closure(st->in->source, closure);
            ok = ts_macro_fixpoint(st, &changed) && ts_header_scan(st, closure, &changed);
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, closure);
    cbm_free(CBM_MEM_CLASS_EXTRACT, changed.list.items);
    if (changed.set) {
        cbm_ht_free(changed.set);
    }
    return ok;
}

/* ── Deleted names (replay4 name_reference_seeds, 377-389) ────────── */

static bool ts_name_references(ts_state_t *st) {
    cbm_ti_source_t *src = st->in->source;
    bool built = false;
    for (int k = 0; k < st->out->deleted.count; k++) {
        const char *name = st->out->deleted.items[k];
        if (cbm_ti_is_keyword(name, strlen(name))) {
            continue;
        }
        bool ok = true;
        if (cbm_ti_graph_has_name(st->in->graph, name, &ok) || !ok) {
            if (!ok) {
                return ts_fail(st, CBM_TI_SEED_STORE);
            }
            continue; /* still defined at head: an ordinary seed already */
        }
        if (!built && !cbm_ti_source_build_occurrences(src)) {
            return ts_fail(st, CBM_TI_SEED_OOM);
        }
        built = true;
        int count = 0;
        const cbm_ti_place_t *places = cbm_ti_source_occurrences(src, name, strlen(name), &count);
        for (int i = 0; i < count; i++) {
            const char *path = cbm_ti_source_path(src, places[i].file);
            const cbm_ti_node_t *d =
                cbm_ti_graph_enclosing(st->in->graph, path, places[i].line, &ok);
            if (!ok) {
                return ts_fail(st, CBM_TI_SEED_STORE);
            }
            if (d ? !ts_add_node(st, d->id)
                  : (!cbm_ti_source_is_header(src, places[i].file) && !ts_add_whole(st, path))) {
                return false;
            }
        }
    }
    return true;
}

/* Every definition of a whole file is a seed; a case in a changed file is a
 * changed test (replay4 577-581). Suites and cases are seeded too: the walk
 * holds them as reached sinks. */
static bool ts_whole_files(ts_state_t *st) {
    for (int k = 0; k < st->whole_list.count; k++) {
        const char *path = st->whole_list.items[k];
        bool ok = true;
        int count = 0;
        const cbm_ti_node_t *nodes = cbm_ti_graph_file(st->in->graph, path, &count, &ok);
        if (!ok) {
            return ts_fail(st, CBM_TI_SEED_STORE);
        }
        bool changed = cbm_ht_get(st->changed_path, path) != NULL;
        for (int i = 0; i < count; i++) {
            int c = ts_case_of(st, path, &nodes[i]);
            if (c >= 0 && changed) {
                st->case_changed[c] = true;
            }
            if (!ts_add_node(st, nodes[i].id)) {
                return false;
            }
        }
    }
    return true;
}

/* ── Driver ──────────────────────────────────────────────────────── */

static bool ts_index_model(ts_state_t *st) {
    const cbm_test_case_t *cases = cbm_test_model_cases(st->in->model, &st->case_count);
    const cbm_test_suite_t *suites = cbm_test_model_suites(st->in->model, &st->suite_count);
    st->case_changed = cbm_arena_calloc(&st->out->arena, (size_t)st->case_count + 1);
    st->suite_changed = cbm_arena_calloc(&st->out->arena, (size_t)st->suite_count + 1);
    if (!st->case_changed || !st->suite_changed) {
        return ts_fail(st, CBM_TI_SEED_OOM);
    }
    for (int i = 0; i < st->case_count; i++) {
        const char *key = ts_key(st, cases[i].file, cases[i].name);
        if (!key) {
            return ts_fail(st, CBM_TI_SEED_OOM);
        }
        cbm_ht_set(st->case_index, key, (void *)(intptr_t)(i + 1));
    }
    for (int i = 0; i < st->suite_count; i++) {
        const char *key = ts_key(st, suites[i].file, suites[i].name);
        if (!key) {
            return ts_fail(st, CBM_TI_SEED_OOM);
        }
        cbm_ht_set(st->suite_index, key, (void *)(intptr_t)(i + 1));
    }
    for (int i = 0; i < st->in->change_count; i++) {
        cbm_ht_set(st->changed_path, st->in->changes[i].path, (void *)1);
    }
    return true;
}

static bool ts_change(ts_state_t *st, const cbm_ti_change_t *c) {
    if (c->deleted) {
        return ts_deleted_file(st, c);
    }
    size_t n = strlen(c->path);
    bool header = (n >= 2 && strcmp(c->path + n - 2, ".h") == 0) ||
                  (n >= 4 && strcmp(c->path + n - 4, ".hpp") == 0) ||
                  (n >= 3 && strcmp(c->path + n - 3, ".hh") == 0);
    if (header) {
        return ts_header(st, c);
    }
    if (c->whole_file) {
        return ts_add_whole(st, c->path); /* no hunks to read: more, never fewer */
    }
    for (int i = 0; i < c->hunk_count; i++) {
        if (!ts_source_hunk(st, c->path, &c->hunks[i])) {
            return false;
        }
    }
    return true;
}

static int ts_id_compare(const void *left, const void *right) {
    int64_t a = *(const int64_t *)left;
    int64_t b = *(const int64_t *)right;
    return a < b ? -1 : (a > b ? 1 : 0);
}

static bool ts_collect(ts_state_t *st) {
    if (st->out->nodes.count > 1) {
        qsort(st->out->nodes.items, (size_t)st->out->nodes.count, sizeof(int64_t), ts_id_compare);
    }
    for (int i = 0; i < st->case_count; i++) {
        if (st->case_changed[i] && !ts_ints_push(&st->out->cases, i)) {
            return ts_fail(st, CBM_TI_SEED_OOM);
        }
    }
    for (int i = 0; i < st->suite_count; i++) {
        if (st->suite_changed[i] && !ts_ints_push(&st->out->suites, i)) {
            return ts_fail(st, CBM_TI_SEED_OOM);
        }
    }
    return true;
}

static void ts_state_free(ts_state_t *st) {
    CBMHashTable *tables[] = {st->seen_nodes, st->whole,       st->deleted,
                              st->case_index, st->suite_index, st->changed_path};
    for (size_t i = 0; i < sizeof(tables) / sizeof(tables[0]); i++) {
        if (tables[i]) {
            cbm_ht_free(tables[i]);
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, st->whole_list.items);
}

cbm_ti_seed_status_t cbm_ti_seed(const cbm_ti_seed_input_t *in, cbm_ti_seeds_t **out) {
    if (!out) {
        return CBM_TI_SEED_INVALID;
    }
    *out = NULL;
    if (!in || !in->model || !in->graph || !in->source || (in->change_count && !in->changes)) {
        return CBM_TI_SEED_INVALID;
    }
    cbm_ti_seeds_t *seeds = cbm_alloc(CBM_MEM_CLASS_EXTRACT, sizeof(*seeds));
    if (!seeds) {
        return CBM_TI_SEED_OOM;
    }
    memset(seeds, 0, sizeof(*seeds));
    cbm_arena_init(&seeds->arena);
    ts_state_t st = {.in = in, .out = seeds};
    st.seen_nodes = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 0);
    st.whole = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 0);
    st.deleted = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 0);
    st.case_index = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 0);
    st.suite_index = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 0);
    st.changed_path = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 0);
    bool ok = (st.seen_nodes && st.whole && st.deleted && st.case_index && st.suite_index &&
               st.changed_path) ||
              ts_fail(&st, CBM_TI_SEED_OOM);
    ok = ok && ts_index_model(&st);
    for (int i = 0; ok && i < in->change_count; i++) {
        ok = ts_change(&st, &in->changes[i]);
    }
    ok = ok && ts_name_references(&st) && ts_whole_files(&st) && ts_collect(&st);
    ts_state_free(&st);
    if (!ok) {
        cbm_ti_seeds_free(seeds);
        return st.status == CBM_TI_SEED_OK ? CBM_TI_SEED_OOM : st.status;
    }
    *out = seeds;
    return CBM_TI_SEED_OK;
}

void cbm_ti_seeds_free(cbm_ti_seeds_t *s) {
    if (!s) {
        return;
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, s->nodes.items);
    cbm_free(CBM_MEM_CLASS_EXTRACT, s->cases.items);
    cbm_free(CBM_MEM_CLASS_EXTRACT, s->suites.items);
    cbm_free(CBM_MEM_CLASS_EXTRACT, s->file_level.items);
    cbm_free(CBM_MEM_CLASS_EXTRACT, s->deleted.items);
    cbm_arena_destroy(&s->arena);
    cbm_free(CBM_MEM_CLASS_EXTRACT, s);
}

const int64_t *cbm_ti_seeds_nodes(const cbm_ti_seeds_t *s, int *count) {
    *count = s ? s->nodes.count : 0;
    return s ? s->nodes.items : NULL;
}

const int *cbm_ti_seeds_changed_cases(const cbm_ti_seeds_t *s, int *count) {
    *count = s ? s->cases.count : 0;
    return s ? s->cases.items : NULL;
}

const int *cbm_ti_seeds_changed_suites(const cbm_ti_seeds_t *s, int *count) {
    *count = s ? s->suites.count : 0;
    return s ? s->suites.items : NULL;
}

const char *const *cbm_ti_seeds_file_level_headers(const cbm_ti_seeds_t *s, int *count) {
    *count = s ? s->file_level.count : 0;
    return s ? s->file_level.items : NULL;
}

const char *const *cbm_ti_seeds_deleted_names(const cbm_ti_seeds_t *s, int *count) {
    *count = s ? s->deleted.count : 0;
    return s ? s->deleted.items : NULL;
}
