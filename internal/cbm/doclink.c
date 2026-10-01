/*
 * doclink.c — doc-comment references, extraction half: the language table,
 * the doc-line side map and the per-file driver. See doclink.h.
 */
#include "doclink.h"

#include "arena.h"
#include "foundation/constants.h"
#include "foundation/mem_core.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

/* ── Link families ───────────────────────────────────────────────────
 *
 * One row per CBMDocLinkSyntax value, generated from the family list in
 * doclink.h (language, name, external, and the ship gate). */

static const CBMDocLinkFamily DOCLINK_FAMILIES[CBM_DOCLINK_SYNTAX_COUNT] = {
#define DOCLINK_FAMILY_ROW(id, lang, name, external, ships) [id] = {lang, name, external, ships},
    CBM_DOCLINK_FAMILY_LIST(DOCLINK_FAMILY_ROW)
#undef DOCLINK_FAMILY_ROW
};

const CBMDocLinkFamily *cbm_doclink_family(int syntax) {
    if (syntax <= CBM_DOCLINK_NONE || syntax >= CBM_DOCLINK_SYNTAX_COUNT ||
        !DOCLINK_FAMILIES[syntax].name) {
        return NULL;
    }
    return &DOCLINK_FAMILIES[syntax];
}

const char *cbm_doclink_syntax_name(int syntax) {
    const CBMDocLinkFamily *f = cbm_doclink_family(syntax);
    return f ? f->name : "";
}

bool cbm_doclink_syntax_is_external(int syntax) {
    const CBMDocLinkFamily *f = cbm_doclink_family(syntax);
    return f && f->external;
}

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
enum { DOCLINK_SHIP_AS_TABLE = 0, DOCLINK_SHIP_ON, DOCLINK_SHIP_OFF };
static _Atomic unsigned char doclink_ship_override[CBM_DOCLINK_SYNTAX_COUNT];

void cbm_doclink_test_set_ships(int syntax, bool ships) {
    if (cbm_doclink_family(syntax)) {
        atomic_store(&doclink_ship_override[syntax], ships ? DOCLINK_SHIP_ON : DOCLINK_SHIP_OFF);
    }
}

void cbm_doclink_test_reset_ships(void) {
    for (int i = 0; i < CBM_DOCLINK_SYNTAX_COUNT; i++) {
        atomic_store(&doclink_ship_override[i], DOCLINK_SHIP_AS_TABLE);
    }
}
#endif

bool cbm_doclink_syntax_ships(int syntax) {
    const CBMDocLinkFamily *f = cbm_doclink_family(syntax);
    if (!f) {
        return false;
    }
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    unsigned char forced = atomic_load(&doclink_ship_override[syntax]);
    if (forced != DOCLINK_SHIP_AS_TABLE) {
        return forced == DOCLINK_SHIP_ON;
    }
#endif
    return f->ships;
}

/* ── Language table ──────────────────────────────────────────────── */

/* One row per language with a doc-reference parser: everything the extraction
 * half needs from a language leg. */
typedef struct {
    CBMLanguage lang;
    /* One documented definition's doc text -> tokens (cbm_doclinks_push). */
    void (*parse_doc)(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                      uint32_t doc_line);
    /* The file's scope blob, or NULL: the language's resolver needs none. */
    const char *(*scan_scope)(CBMExtractCtx *ctx);
    /* The tag line that blob starts with, and the blob as it is persisted
     * (line numbers dropped). NULL: the blob is persisted as it is. */
    const char *scope_tag;
    char *(*portable_scope)(const char *scope);
    /* Labels that twin another definition of the same name and line (C#
     * fields and constants are also emitted as module-level Variables with
     * the same doc): only the first label carries the doc's references. */
    const char *twin_label;
    const char *twin_of;
} doclink_lang_t;

static const doclink_lang_t DOCLINK_LANGS[] = {
    {.lang = CBM_LANG_CSHARP,
     .parse_doc = cbm_doclink_cs_parse_doc,
     .scan_scope = cbm_doclink_cs_scan_scope,
     .scope_tag = CBM_DOCLINK_CS_SCOPE_TAG,
     .portable_scope = cbm_doclink_cs_portable_scope,
     .twin_label = "Variable",
     .twin_of = "Field"},
};

static const doclink_lang_t *doclink_lang(CBMLanguage lang) {
    for (size_t i = 0; i < sizeof(DOCLINK_LANGS) / sizeof(DOCLINK_LANGS[0]); i++) {
        if (DOCLINK_LANGS[i].lang == lang) {
            return &DOCLINK_LANGS[i];
        }
    }
    return NULL;
}

bool cbm_doclink_lang_supported(CBMLanguage lang) {
    return doclink_lang(lang) != NULL;
}

/* ── Doc-line side map ───────────────────────────────────────────── */

typedef struct {
    const char *doc;
    uint32_t line;
} doc_line_ent_t;

typedef struct {
    doc_line_ent_t *items;
    int count;
    int cap;
    bool sorted;
} doc_line_map_t;

enum { DOC_LINE_MAP_INIT = 64 };

static CBMArena *doclink_scratch(CBMExtractCtx *ctx) {
    return ctx->scratch ? ctx->scratch : ctx->arena;
}

void cbm_doclink_note_doc_line(CBMExtractCtx *ctx, const char *doc, uint32_t line) {
    if (!ctx || !doc || !cbm_doclink_lang_supported(ctx->language)) {
        return;
    }
    CBMArena *a = doclink_scratch(ctx);
    doc_line_map_t *m = (doc_line_map_t *)ctx->doc_lines;
    if (!m) {
        m = (doc_line_map_t *)cbm_arena_alloc(a, sizeof(*m));
        if (!m) {
            return;
        }
        memset(m, 0, sizeof(*m));
        ctx->doc_lines = m;
    }
    if (m->count >= m->cap) {
        int ncap = m->cap ? m->cap * PAIR_LEN : DOC_LINE_MAP_INIT;
        doc_line_ent_t *grown = (doc_line_ent_t *)cbm_arena_alloc(a, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return; /* the reference keeps its definition's line instead */
        }
        if (m->count > 0) {
            memcpy(grown, m->items, (size_t)m->count * sizeof(*grown));
        }
        m->items = grown;
        m->cap = ncap;
    }
    m->items[m->count].doc = doc;
    m->items[m->count].line = line;
    m->count++;
    m->sorted = false;
}

static int doc_line_cmp(const void *a, const void *b) {
    uintptr_t x = (uintptr_t)((const doc_line_ent_t *)a)->doc;
    uintptr_t y = (uintptr_t)((const doc_line_ent_t *)b)->doc;
    return (x > y) - (x < y);
}

/* Start line of `doc`, or 0 when it was not built by doc_run_text (then the
 * caller falls back to the definition's own line). */
static uint32_t doc_line_of(CBMExtractCtx *ctx, const char *doc) {
    doc_line_map_t *m = (doc_line_map_t *)ctx->doc_lines;
    if (!m || m->count == 0) {
        return 0;
    }
    if (!m->sorted) {
        qsort(m->items, (size_t)m->count, sizeof(m->items[0]), doc_line_cmp);
        m->sorted = true;
    }
    int lo = 0;
    int hi = m->count - SKIP_ONE;
    uintptr_t key = (uintptr_t)doc;
    while (lo <= hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        uintptr_t v = (uintptr_t)m->items[mid].doc;
        if (v == key) {
            return m->items[mid].line;
        }
        if (v < key) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid - SKIP_ONE;
        }
    }
    return 0;
}

/* ── Persisted scope ─────────────────────────────────────────────── */

char *cbm_doclink_portable_scope(const char *scope) {
    if (!scope) {
        return NULL;
    }
    for (size_t i = 0; i < sizeof(DOCLINK_LANGS) / sizeof(DOCLINK_LANGS[0]); i++) {
        const doclink_lang_t *L = &DOCLINK_LANGS[i];
        if (!L->scope_tag || !L->portable_scope) {
            continue;
        }
        size_t tl = strlen(L->scope_tag);
        if (strncmp(scope, L->scope_tag, tl) == 0 && (scope[tl] == '\n' || scope[tl] == '\0')) {
            return L->portable_scope(scope);
        }
    }
    return cbm_mem_strdup(CBM_MEM_CLASS_OTHER, scope);
}

/* ── Driver ──────────────────────────────────────────────────────── */

enum { DOCLINK_INIT_CAP = 16 };

void cbm_doclinks_push(CBMDocLinkArray *arr, CBMArena *a, CBMDocLink link) {
    if (arr->count >= arr->cap) {
        int ncap = arr->cap ? arr->cap * PAIR_LEN : DOCLINK_INIT_CAP;
        CBMDocLink *grown = (CBMDocLink *)cbm_arena_alloc(a, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return;
        }
        if (arr->count > 0) {
            memcpy(grown, arr->items, (size_t)arr->count * sizeof(*grown));
        }
        arr->items = grown;
        arr->cap = ncap;
    }
    arr->items[arr->count++] = link;
}

typedef struct {
    uint32_t line;
    const char *name;
} doclink_twin_key_t;

static int twin_key_cmp(const void *a, const void *b) {
    const doclink_twin_key_t *x = (const doclink_twin_key_t *)a;
    const doclink_twin_key_t *y = (const doclink_twin_key_t *)b;
    if (x->line != y->line) {
        return x->line < y->line ? -1 : 1;
    }
    return strcmp(x->name, y->name);
}

/* The (line, name) keys of the definitions a twin label duplicates, sorted;
 * NULL when there are none. */
static doclink_twin_key_t *twin_keys(CBMExtractCtx *ctx, const char *label, int *out_count) {
    *out_count = 0;
    const CBMDefArray *defs = &ctx->result->defs;
    int n = 0;
    for (int i = 0; i < defs->count; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (d->label && d->name && strcmp(d->label, label) == 0) {
            n++;
        }
    }
    if (n == 0) {
        return NULL;
    }
    doclink_twin_key_t *keys = (doclink_twin_key_t *)cbm_arena_alloc(
        doclink_scratch(ctx), (size_t)n * sizeof(doclink_twin_key_t));
    if (!keys) {
        return NULL;
    }
    int k = 0;
    for (int i = 0; i < defs->count; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (d->label && d->name && strcmp(d->label, label) == 0) {
            keys[k].line = d->start_line;
            keys[k].name = d->name;
            k++;
        }
    }
    qsort(keys, (size_t)k, sizeof(keys[0]), twin_key_cmp);
    *out_count = k;
    return keys;
}

void cbm_doclinks_extract(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->result) {
        return;
    }
    const doclink_lang_t *L = doclink_lang(ctx->language);
    if (!L) {
        return;
    }
    int twin_count = 0;
    doclink_twin_key_t *twins = L->twin_label ? twin_keys(ctx, L->twin_of, &twin_count) : NULL;
    CBMDefArray *defs = &ctx->result->defs;
    for (int i = 0; i < defs->count; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (!d->docstring || !d->docstring[0] || !d->qualified_name || !d->name) {
            continue;
        }
        if (twins && d->label && strcmp(d->label, L->twin_label) == 0) {
            doclink_twin_key_t key = {d->start_line, d->name};
            if (bsearch(&key, twins, (size_t)twin_count, sizeof(twins[0]), twin_key_cmp)) {
                continue; /* the Field twin carries these references */
            }
        }
        uint32_t doc_line = doc_line_of(ctx, d->docstring);
        L->parse_doc(ctx, d, d->docstring, doc_line ? doc_line : d->start_line);
    }
    if (L->scan_scope) {
        ctx->result->doc_scope = L->scan_scope(ctx);
    }
}
