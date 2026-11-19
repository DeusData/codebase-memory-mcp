/*
 * extract_scope.c — what one file defines and includes, in source order: the
 * per-file facts of the shared cross-file layer (cbm.h, CBMScopeEvent).
 *
 * One walk, the same for every language with a row below. A row names syntax
 * only: which node kinds define a type (and the label of the definition the
 * extractor made for it), which are include statements, which are qualified
 * type references, which statement names the file's package, and which type
 * kind names the types nested in it. A definition event is the definition
 * result->defs already holds (same start line, name and label); a
 * definition-shaped node without one is an ASSIGN (a name defined here that
 * the graph has no node for). Include events are result->imports, matched by
 * the statement's start byte (the import parser records it). Every event
 * takes effect after its statement: its position is the node's end byte. A
 * parse error is an OPAQUE event, and what it holds is CONDITIONAL. Nothing
 * is resolved here.
 *
 * Lifted from the parked include-scope branch (round 5): its neutral walk and
 * its Protobuf handlers. The shell and Make handlers and their event kinds
 * come with their own rows.
 */
#include "cbm.h"
#include "arena.h"
#include "helpers.h"
#include "tree_sitter/api.h"
#include "foundation/constants.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef enum {
    SC_ROLE_NONE = 0,
    SC_ROLE_TYPE,    /* defines a type (Protobuf message, enum) */
    SC_ROLE_INCLUDE, /* an include statement, when an import starts here */
    SC_ROLE_QREF,    /* a qualified type reference: its text, qualifier included */
} sc_role_t;

typedef struct {
    const char *kind;
    sc_role_t role;
    const char *label; /* TYPE: the label of the definition the extractor makes for it */
} sc_rule_t;

typedef struct {
    CBMLanguage lang;
    const sc_rule_t *rules;
    const char *const *name_kinds; /* the kinds of the child naming a definition */
    const char *package_kind;      /* the statement naming the file's package; NULL: none */
    const char *nest_kind;         /* a type kind whose nested types it names; NULL: none */
    bool public_reexports;         /* `import public` re-exports what it imports */
} sc_lang_t;

static const sc_rule_t SC_PROTO_RULES[] = {
    {"message", SC_ROLE_TYPE, "Class"}, {"enum", SC_ROLE_TYPE, "Class"},
    {"import", SC_ROLE_INCLUDE, NULL},  {"message_or_enum_type", SC_ROLE_QREF, NULL},
    {NULL, SC_ROLE_NONE, NULL},
};
static const char *const SC_PROTO_NAMES[] = {"message_name", "enum_name", NULL};

static const sc_lang_t SC_LANGS[] = {
    {CBM_LANG_PROTOBUF, SC_PROTO_RULES, SC_PROTO_NAMES, "package", "message", true},
};

static _Atomic uint64_t g_scope_nodes_visited = 0;

uint64_t cbm_extract_scope_nodes_visited(void) {
    return atomic_load_explicit(&g_scope_nodes_visited, memory_order_relaxed);
}

static const sc_lang_t *sc_lang(CBMLanguage lang) {
    for (size_t i = 0; i < sizeof(SC_LANGS) / sizeof(SC_LANGS[0]); i++) {
        if (SC_LANGS[i].lang == lang) {
            return &SC_LANGS[i];
        }
    }
    return NULL;
}

bool cbm_extract_scope_has_row(CBMLanguage lang) {
    return sc_lang(lang) != NULL;
}

/* ── Definitions by start line, imports by start byte ─────────────── */

typedef struct {
    uint32_t key; /* a definition's start line, an import's start byte */
    int index;
} sc_ref_t;

static int sc_ref_cmp(const void *a, const void *b) {
    const sc_ref_t *x = a;
    const sc_ref_t *y = b;
    if (x->key != y->key) {
        return x->key < y->key ? -1 : 1;
    }
    return (x->index > y->index) - (x->index < y->index);
}

/* The first entry of refs[0..n) whose key is at least `key`. */
static int sc_lower_bound(const sc_ref_t *refs, int n, uint32_t key, uint64_t *steps) {
    int lo = 0;
    int hi = n;
    while (lo < hi) {
        int mid = lo + (hi - lo) / PAIR_LEN;
        (*steps)++;
        if (refs[mid].key < key) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo;
}

typedef struct {
    CBMExtractCtx *ctx;
    const sc_lang_t *lang;
    sc_ref_t *defs; /* result->defs by start line */
    int ndefs;
    sc_ref_t *imps; /* result->imports with a module path, by start byte */
    int nimps;
    const char *pkg; /* the file's package ("" when none) */
    uint32_t depth;  /* the cursor depth of the node being visited */
    uint64_t steps;  /* index probes (growth tests read them with the nodes) */
    /* the enclosing nesting types of the cursor, outermost first (their full
     * names and depths): a nested type's full name in O(1) */
    const char **nest_full;
    uint32_t *nest_depth;
    uint32_t nnest;
    uint32_t cap_nest;
    /* depths of the parse errors above the cursor: what they hold may or may
     * not be what the source means */
    uint32_t *cond_depth;
    uint32_t ncond;
    uint32_t cap_cond;
} sc_walk_t;

/* The definition of `label` named `name` that starts on `line`; -1 when there
 * is none. */
static int sc_find_def(sc_walk_t *w, uint32_t line, const char *name, const char *label) {
    for (int i = sc_lower_bound(w->defs, w->ndefs, line, &w->steps);
         i < w->ndefs && w->defs[i].key == line; i++) {
        w->steps++;
        const CBMDefinition *d = &w->ctx->result->defs.items[w->defs[i].index];
        if (d->label && strcmp(d->label, label) == 0 && d->name && strcmp(d->name, name) == 0) {
            return w->defs[i].index;
        }
    }
    return -1;
}

/* ── Events ────────────────────────────────────────────────────────── */

static void sc_emit(sc_walk_t *w, uint8_t kind, const char *name, const char *qual, int32_t index,
                    uint32_t pos, uint8_t flags) {
    if (w->ncond > 0) {
        flags |= (uint8_t)CBM_SCOPE_CONDITIONAL;
    }
    CBMScopeEvent ev = {
        .name = name, .qual = qual, .pos = pos, .index = index, .kind = kind, .flags = flags};
    cbm_scope_events_push(&w->ctx->result->scope, w->ctx->arena, ev);
}

/* The child naming a definition: its first named child of a name kind. */
static TSNode sc_name_node(const sc_walk_t *w, TSNode n) {
    TSNode none = {{0}, NULL, NULL};
    for (const char *const *k = w->lang->name_kinds; k && *k; k++) {
        uint32_t c = ts_node_named_child_count(n);
        for (uint32_t i = 0; i < c; i++) {
            TSNode ch = ts_node_named_child(n, i);
            if (strcmp(ts_node_type(ch), *k) == 0) {
                return ch;
            }
        }
    }
    return none;
}

static const char *sc_text(const sc_walk_t *w, TSNode n) {
    return ts_node_is_null(n) ? NULL : cbm_node_text(w->ctx->arena, n, w->ctx->source);
}

/* A definition's full name: package, enclosing nesting types and the name
 * ("a.b.Outer.Inner"); NULL for a row without packages and nesting, or inside
 * a nesting type whose own full name is unknown. */
static const char *sc_full_name(sc_walk_t *w, const char *name) {
    if (!w->lang->package_kind && !w->lang->nest_kind) {
        return NULL;
    }
    const char *outer = w->nnest > 0 ? w->nest_full[w->nnest - SKIP_ONE] : w->pkg;
    if (!outer) {
        return NULL;
    }
    return outer[0] ? cbm_arena_sprintf(w->ctx->arena, "%s.%s", outer, name)
                    : cbm_arena_strdup(w->ctx->arena, name);
}

/* Grow a pair of parallel arrays (pointer-sized and uint32 entries). */
static bool sc_grow(CBMArena *a, void **ptrs, size_t ptr_size, uint32_t **depths, uint32_t n,
                    uint32_t *cap) {
    if (n < *cap) {
        return true;
    }
    uint32_t ncap = *cap ? *cap * PAIR_LEN : CBM_SZ_16;
    void *np = ptrs ? cbm_arena_alloc(a, ncap * ptr_size) : NULL;
    uint32_t *nd = cbm_arena_alloc(a, ncap * sizeof(*nd));
    if ((ptrs && !np) || !nd) {
        return false;
    }
    if (n) {
        if (ptrs) {
            memcpy(np, *ptrs, n * ptr_size);
        }
        memcpy(nd, *depths, n * sizeof(*nd));
    }
    if (ptrs) {
        *ptrs = np;
    }
    *depths = nd;
    *cap = ncap;
    return true;
}

/* A nesting type's full name (NULL: unknown) for the types nested in it. */
static bool sc_push_nest(sc_walk_t *w, const char *full) {
    if (!sc_grow(w->ctx->arena, (void **)&w->nest_full, sizeof(*w->nest_full), &w->nest_depth,
                 w->nnest, &w->cap_nest)) {
        return false;
    }
    w->nest_full[w->nnest] = full;
    w->nest_depth[w->nnest] = w->depth;
    w->nnest++;
    return true;
}

static bool sc_type(sc_walk_t *w, TSNode n, const sc_rule_t *rule) {
    const char *name = sc_text(w, sc_name_node(w, n));
    const char *full = name ? sc_full_name(w, name) : NULL;
    if (w->lang->nest_kind && strcmp(ts_node_type(n), w->lang->nest_kind) == 0 &&
        !sc_push_nest(w, full)) {
        return false; /* what is nested in it is named from here */
    }
    if (!name) {
        return true;
    }
    uint32_t line = ts_node_start_point(n).row + TS_LINE_OFFSET;
    int d = sc_find_def(w, line, name, rule->label);
    sc_emit(w, d >= 0 ? (uint8_t)CBM_SCOPE_DEF : (uint8_t)CBM_SCOPE_ASSIGN, name,
            d >= 0 ? full : NULL, d, ts_node_end_byte(n), 0);
    return true;
}

/* An include statement: one INCLUDE event per import row it made. */
static void sc_include(sc_walk_t *w, TSNode n) {
    uint32_t at = ts_node_start_byte(n);
    uint8_t flags = 0;
    if (w->lang->public_reexports) {
        /* `import public "x.proto";`: the grammar's own `public` token (a
         * comment holding the word is no keyword) */
        uint32_t nc = ts_node_child_count(n);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode ch = ts_node_child(n, i);
            if (!ts_node_is_named(ch) && strcmp(ts_node_type(ch), "public") == 0) {
                flags |= CBM_SCOPE_REEXPORT;
                break;
            }
        }
    }
    /* the importing file's package, which its references are relative to */
    const char *pkg = w->lang->package_kind ? w->pkg : NULL;
    for (int k = sc_lower_bound(w->imps, w->nimps, at, &w->steps);
         k < w->nimps && w->imps[k].key == at; k++) {
        w->steps++;
        sc_emit(w, CBM_SCOPE_INCLUDE, NULL, pkg, w->imps[k].index, ts_node_end_byte(n), flags);
    }
}

/* A qualified type reference as written, blanks and comments between its
 * parts dropped: "google.protobuf.Empty", ".a.B", "Empty". */
static void sc_qref(sc_walk_t *w, TSNode n) {
    uint32_t b = ts_node_start_byte(n);
    uint32_t e = ts_node_end_byte(n);
    char *text = cbm_arena_alloc(w->ctx->arena, (size_t)(e - b) + SKIP_ONE);
    if (!text) {
        return;
    }
    size_t k = 0;
    uint32_t c = ts_node_child_count(n);
    for (uint32_t i = 0; i < c; i++) {
        TSNode ch = ts_node_child(n, i);
        if (strcmp(ts_node_type(ch), "comment") == 0) {
            continue;
        }
        uint32_t cb = ts_node_start_byte(ch);
        uint32_t ce = ts_node_end_byte(ch);
        memcpy(text + k, w->ctx->source + cb, ce - cb);
        k += ce - cb;
    }
    text[k] = '\0';
    if (k > 0) {
        sc_emit(w, CBM_SCOPE_QREF, text, NULL, (int32_t)b, e, 0);
    }
}

/* Whether the walk descends into n; false also when memory ran out (the
 * caller stops the walk). */
static bool sc_visit(sc_walk_t *w, TSNode n, bool *failed) {
    if (!ts_node_is_named(n)) {
        return true; /* a keyword token (`import`, `message`) is no statement of its kind */
    }
    const char *k = ts_node_type(n);
    for (const sc_rule_t *r = w->lang->rules; r->kind; r++) {
        if (strcmp(r->kind, k) != 0) {
            continue;
        }
        switch (r->role) {
        case SC_ROLE_TYPE:
            *failed = !sc_type(w, n, r);
            return true;
        case SC_ROLE_INCLUDE:
            sc_include(w, n);
            return true;
        case SC_ROLE_QREF:
            sc_qref(w, n);
            return false;
        case SC_ROLE_NONE:
        default:
            return true;
        }
    }
    return true;
}

/* Source order; one statement's events by kind, index, then name: a total
 * order, so the array never depends on the sort's stability. */
static int sc_event_cmp(const void *a, const void *b) {
    const CBMScopeEvent *x = a;
    const CBMScopeEvent *y = b;
    if (x->pos != y->pos) {
        return x->pos < y->pos ? -1 : 1;
    }
    if (x->kind != y->kind) {
        return x->kind < y->kind ? -1 : 1;
    }
    if (x->index != y->index) {
        return x->index < y->index ? -1 : 1;
    }
    return strcmp(x->name ? x->name : "", y->name ? y->name : "");
}

/* Leaving every node at `depth` or deeper: their parse errors and nesting
 * types no longer enclose the cursor. */
static void sc_leave(sc_walk_t *w, uint32_t depth) {
    while (w->ncond > 0 && w->cond_depth[w->ncond - SKIP_ONE] >= depth) {
        w->ncond--;
    }
    while (w->nnest > 0 && w->nest_depth[w->nnest - SKIP_ONE] >= depth) {
        w->nnest--;
    }
}

/* Depth-first over the tree with a cursor (no recursion). Returns the nodes
 * visited; stops early only when memory runs out. */
static uint64_t sc_walk_tree(sc_walk_t *w, TSNode root) {
    uint64_t visited = 0;
    TSTreeCursor cur = ts_tree_cursor_new(root);
    uint32_t depth = 0;
    for (;;) {
        TSNode n = ts_tree_cursor_current_node(&cur);
        visited++;
        sc_leave(w, depth);
        w->depth = depth;
        if (strcmp(ts_node_type(n), "ERROR") == 0) {
            /* a parse the facts cannot trust: what follows is not proven, and
             * what it holds may or may not be meant */
            if (!sc_grow(w->ctx->arena, NULL, 0, &w->cond_depth, w->ncond, &w->cap_cond)) {
                break;
            }
            w->cond_depth[w->ncond++] = depth;
            sc_emit(w, CBM_SCOPE_OPAQUE, NULL, NULL, -1, ts_node_end_byte(n), 0);
        }
        bool failed = false;
        bool descend = sc_visit(w, n, &failed);
        if (failed) {
            break;
        }
        if (descend && ts_tree_cursor_goto_first_child(&cur)) {
            depth++;
            continue;
        }
        bool moved = false;
        while (!moved) {
            if (ts_tree_cursor_goto_next_sibling(&cur)) {
                moved = true;
            } else if (depth == 0 || !ts_tree_cursor_goto_parent(&cur)) {
                break;
            } else {
                depth--;
            }
        }
        if (!moved) {
            break;
        }
    }
    ts_tree_cursor_delete(&cur);
    return visited;
}

/* The file's package: the first named child of its package statement. */
static const char *sc_package(sc_walk_t *w, TSNode root) {
    uint32_t c = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < c; i++) {
        TSNode ch = ts_node_named_child(root, i);
        if (strcmp(ts_node_type(ch), w->lang->package_kind) == 0 &&
            ts_node_named_child_count(ch) > 0) {
            const char *pk = sc_text(w, ts_node_named_child(ch, 0));
            return pk ? pk : "";
        }
    }
    return "";
}

void cbm_extract_scope(CBMExtractCtx *ctx) {
    const sc_lang_t *lang = ctx ? sc_lang(ctx->language) : NULL;
    if (!lang || ts_node_is_null(ctx->root)) {
        return;
    }
    sc_walk_t w = {.ctx = ctx, .lang = lang, .pkg = ""};
    if (lang->package_kind) {
        w.pkg = sc_package(&w, ctx->root);
    }
    const CBMDefArray *defs = &ctx->result->defs;
    if (defs->count > 0) {
        w.defs = cbm_arena_alloc(ctx->arena, (size_t)defs->count * sizeof(*w.defs));
        if (!w.defs) {
            return;
        }
        for (int i = 0; i < defs->count; i++) {
            w.defs[w.ndefs++] = (sc_ref_t){defs->items[i].start_line, i};
        }
        qsort(w.defs, (size_t)w.ndefs, sizeof(*w.defs), sc_ref_cmp);
    }
    const CBMImportArray *imps = &ctx->result->imports;
    if (imps->count > 0) {
        w.imps = cbm_arena_alloc(ctx->arena, (size_t)imps->count * sizeof(*w.imps));
        if (!w.imps) {
            return;
        }
        for (int i = 0; i < imps->count; i++) {
            if (imps->items[i].module_path) {
                w.imps[w.nimps++] = (sc_ref_t){imps->items[i].start_byte, i};
            }
        }
        qsort(w.imps, (size_t)w.nimps, sizeof(*w.imps), sc_ref_cmp);
    }
    uint64_t visited = sc_walk_tree(&w, ctx->root);
    if (ts_node_has_error(ctx->root)) {
        /* a tree with an error the walk may not meet as an ERROR node (a
         * MISSING token the parser inserted): the compiler rejects the file,
         * so nothing in it is proven */
        sc_emit(&w, CBM_SCOPE_OPAQUE, NULL, NULL, -1, ts_node_end_byte(ctx->root), 0);
    }
    atomic_fetch_add_explicit(&g_scope_nodes_visited, visited + w.steps, memory_order_relaxed);
    CBMScopeEventArray *ev = &ctx->result->scope;
    if (ev->count > SKIP_ONE) {
        qsort(ev->items, (size_t)ev->count, sizeof(*ev->items), sc_event_cmp);
    }
}
