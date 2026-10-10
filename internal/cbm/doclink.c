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

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
static _Atomic int doc_alloc_fail_after[CBM_DOCLINK_ALLOC_KINDS];

void cbm_doclink_test_fail_alloc_after(int kind, int nth) {
    if (kind >= 0 && kind < CBM_DOCLINK_ALLOC_KINDS) {
        atomic_store(&doc_alloc_fail_after[kind], nth);
    }
}

bool cbm_doclink_test_fail_alloc(int kind) {
    if (kind < 0 || kind >= CBM_DOCLINK_ALLOC_KINDS) {
        return false;
    }
    int n = atomic_load(&doc_alloc_fail_after[kind]);
    while (n > 0) {
        if (atomic_compare_exchange_weak(&doc_alloc_fail_after[kind], &n, n - 1)) {
            return n == 1;
        }
    }
    return false;
}

void cbm_doclink_test_reset_alloc(void) {
    for (int i = 0; i < CBM_DOCLINK_ALLOC_KINDS; i++) {
        atomic_store(&doc_alloc_fail_after[i], 0);
    }
}

static _Atomic uint64_t doc_work_copied;
static _Atomic uint64_t doc_work_parse_input;
static _Atomic uint64_t doc_work_cleaned;

void cbm_doclink_test_doc_work_reset(void) {
    atomic_store(&doc_work_copied, 0);
    atomic_store(&doc_work_parse_input, 0);
    atomic_store(&doc_work_cleaned, 0);
}

void cbm_doclink_test_doc_work(uint64_t *copied, uint64_t *parse_input, uint64_t *cleaned) {
    *copied = atomic_load(&doc_work_copied);
    *parse_input = atomic_load(&doc_work_parse_input);
    *cleaned = atomic_load(&doc_work_cleaned);
}

void cbm_doclink_test_note_doc_work(uint64_t copied, uint64_t parse_input, uint64_t cleaned) {
    atomic_fetch_add(&doc_work_copied, copied);
    atomic_fetch_add(&doc_work_parse_input, parse_input);
    atomic_fetch_add(&doc_work_cleaned, cleaned);
}
#endif

/* ── Link families ───────────────────────────────────────────────────
 *
 * One row per CBMDocLinkSyntax value, generated from the family list in
 * doclink.h (language, name, external, and the ship gate). */

static const CBMDocLinkFamily DOCLINK_FAMILIES[CBM_DOCLINK_SYNTAX_COUNT] = {
#define DOCLINK_FAMILY_ROW(id, lang, name, external, ships, edge) \
    [id] = {lang, name, external, ships, edge},
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

const char *cbm_doclink_syntax_edge(int syntax) {
    const CBMDocLinkFamily *f = cbm_doclink_family(syntax);
    return f && f->edge ? f->edge : "MENTIONS";
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
    /* A document language: the whole file is the documentation, so one scan
     * of its bytes replaces parse_doc, the file-level doc and the scope. */
    void (*scan_file)(CBMExtractCtx *ctx);
} doclink_lang_t;

static const doclink_lang_t DOCLINK_LANGS[] = {
    {.lang = CBM_LANG_CSHARP,
     .parse_doc = cbm_doclink_cs_parse_doc,
     .scan_scope = cbm_doclink_cs_scan_scope,
     .scope_tag = CBM_DOCLINK_CS_SCOPE_TAG,
     .portable_scope = cbm_doclink_cs_portable_scope,
     .twin_label = "Variable",
     .twin_of = "Field"},
    /* MSBuild project files: no references, but a scope the C# resolver reads */
    {.lang = CBM_LANG_XML,
     .parse_doc = cbm_doclink_cs_project_parse_doc,
     .scan_scope = cbm_doclink_cs_project_scan_scope,
     .scope_tag = CBM_DOCLINK_CS_SCOPE_TAG,
     .portable_scope = cbm_doclink_cs_portable_scope},
    {.lang = CBM_LANG_MARKDOWN, .scan_file = cbm_doclink_md_scan_file},
    {.lang = CBM_LANG_RST, .scan_file = cbm_doclink_rst_scan_file},
    /* Python: no doc references yet, only the scope the reST resolver reads
     * (package re-exports, Sphinx conf.py settings) */
    {.lang = CBM_LANG_PYTHON,
     .scan_scope = cbm_doclink_py_scan_scope,
     .scope_tag = CBM_DOCLINK_PY_SCOPE_TAG},
    /* YAML: the Antora component and playbook settings the AsciiDoc resolver
     * reads */
    {.lang = CBM_LANG_YAML,
     .scan_scope = cbm_doclink_antora_scan_scope,
     .scope_tag = CBM_DOCLINK_ADOC_SCOPE_TAG},
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
    const doclink_lang_t *L = doclink_lang(lang);
    return L && (L->parse_doc || L->scan_file);
}

/* ── Doc-line side map ───────────────────────────────────────────── */

typedef struct {
    const char *doc;
    uint32_t line;
    bool parsed; /* its references were taken, for the first definition that has it */
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

/* The map could not hold a doc: its references then take their definition's
 * line, and a doc shared by several declarators is taken once per declarator.
 * The layer must not say ok over that (CBMDocLinkArray.failed). */
static void doc_line_lost(CBMExtractCtx *ctx) {
    if (ctx->result) {
        ctx->result->doc_links.failed = true;
    }
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
            doc_line_lost(ctx);
            return;
        }
        memset(m, 0, sizeof(*m));
        ctx->doc_lines = m;
    }
    if (m->count >= m->cap) {
        int ncap = m->cap ? m->cap * PAIR_LEN : DOC_LINE_MAP_INIT;
        doc_line_ent_t *grown;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
        if (cbm_doclink_test_fail_alloc(CBM_DOCLINK_ALLOC_DOC_LINE)) {
            grown = NULL;
        } else
#endif
        {
            grown = (doc_line_ent_t *)cbm_arena_alloc(a, (size_t)ncap * sizeof(*grown));
        }
        if (!grown) {
            doc_line_lost(ctx);
            return;
        }
        if (m->count > 0) {
            memcpy(grown, m->items, (size_t)m->count * sizeof(*grown));
        }
        m->items = grown;
        m->cap = ncap;
    }
    m->items[m->count++] = (doc_line_ent_t){.doc = doc, .line = line};
    m->sorted = false;
}

static int doc_line_cmp(const void *a, const void *b) {
    uintptr_t x = (uintptr_t)((const doc_line_ent_t *)a)->doc;
    uintptr_t y = (uintptr_t)((const doc_line_ent_t *)b)->doc;
    return (x > y) - (x < y);
}

/* Per-file metadata for this exact immutable doc pointer, never for a line. */
static doc_line_ent_t *doc_info_of(CBMExtractCtx *ctx, const char *doc) {
    doc_line_map_t *m = (doc_line_map_t *)ctx->doc_lines;
    if (!m || m->count == 0) {
        return NULL;
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
            return &m->items[mid];
        }
        if (v < key) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid - SKIP_ONE;
        }
    }
    return NULL;
}

/* A doc not built by doc_run_text keeps the definition's own line. */
static uint32_t doc_line_of(CBMExtractCtx *ctx, const char *doc) {
    const doc_line_ent_t *info = doc_info_of(ctx, doc);
    return info ? info->line : 0;
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
        CBMDocLink *grown;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
        if (cbm_doclink_test_fail_alloc(CBM_DOCLINK_ALLOC_TOKENS)) {
            grown = NULL;
        } else
#endif
        {
            grown = (CBMDocLink *)cbm_arena_alloc(a, (size_t)ncap * sizeof(*grown));
        }
        if (!grown) {
            arr->failed = true; /* the token is lost: the layer must not say ok */
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
    if (L->scan_file) {
        L->scan_file(ctx);
        return;
    }
    if (!L->parse_doc) { /* a scope, and no references (Python) */
        ctx->result->doc_scope = L->scan_scope ? L->scan_scope(ctx) : NULL;
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
                /* the Field twin carries these references -- and so those of
                 * the declarators after it, which share this doc text */
                doc_line_ent_t *taken = doc_info_of(ctx, d->docstring);
                if (taken) {
                    taken->parsed = true;
                }
                continue;
            }
        }
        doc_line_ent_t *info = doc_info_of(ctx, d->docstring);
        uint32_t doc_line = info ? info->line : 0;
        bool csharp = ctx->language == CBM_LANG_CSHARP;
        if (csharp && info && info->parsed) {
            /* One C# doc comment documents every declarator of a field
             * declaration (`int a, b, c;` share its text, extract_defs.c): its
             * references are taken once, from the first declarator. Taken
             * again per declarator, the tokens, their resolutions and the
             * rows grow with references x declarators. */
            continue;
        }
        if (csharp) {
            if (!cbm_doclink_cs_parse_doc_checked(ctx, d, d->docstring,
                                                  doc_line ? doc_line : d->start_line)) {
                ctx->result->doc_links.failed = true; /* a reference value was lost */
            }
        } else {
            L->parse_doc(ctx, d, d->docstring, doc_line ? doc_line : d->start_line);
        }
        if (info) {
            info->parsed = true;
        }
    }
    /* The file's own doc: its references belong to the file. The parser gets
     * a definition-shaped stand-in for the file; what it pushes is marked, and
     * the resolving half takes the File node as the source. No qualified name
     * for that node is computed on this side. */
    const char *file_doc = ctx->result->module_doc;
    if (file_doc && file_doc[0]) {
        CBMDocLinkArray *arr = &ctx->result->doc_links;
        int first = arr->count;
        CBMDefinition file_def = {
            .name = ctx->rel_path,
            .qualified_name = ctx->module_qn ? ctx->module_qn : "",
            .label = "File",
            .file_path = ctx->rel_path,
            .start_line = SKIP_ONE,
            .end_line = SKIP_ONE,
            .docstring = file_doc,
        };
        uint32_t doc_line = doc_line_of(ctx, file_doc);
        L->parse_doc(ctx, &file_def, file_doc, doc_line ? doc_line : file_def.start_line);
        for (int i = first; i < arr->count; i++) {
            arr->items[i].flags |= CBM_DOCLINK_FLAG_FILE;
        }
    }
    if (L->scan_scope) {
        ctx->result->doc_scope = L->scan_scope(ctx);
    }
}
