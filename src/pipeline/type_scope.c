/*
 * type_scope.c — declared types resolved once, in the scope that wrote them.
 * See type_scope.h for the contract.
 *
 * Cost: per file, one hash of its import map, one hash of its own definitions
 * (rows with lexical scopes) and one memo of the spellings it writes; per
 * definition, one scan of its type texts and one walk up its enclosing defs
 * that stops at the first one already walked. Every lookup is an exact hash
 * probe (no scan, no short-name candidate list), so the step is linear in the
 * bytes of declared type text and of the file's definition names.
 */
#include "pipeline/type_scope.h"

#include "foundation/constants.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Language descriptions ────────────────────────────────────────── */

/* Go: every type-like declaration has a method set (a Go alias carries its
 * target through alias_target, which the registrar follows). */
static const char *const go_labels[] = {"Struct", "Interface", "Type", "Class", "Enum", NULL};
/* Python: what py_register_lsp_defs registers as a type. */
static const char *const py_labels[] = {"Class", "Interface", "Protocol", "Type", NULL};

static const cbm_type_scope_lang_t type_scope_langs[] = {
    {.lang = CBM_LANG_GO,
     .labels = go_labels,
     .engine_qualifies_own_module = true,
     .imports_bind_modules = true,
     .return_types = true,
     .param_types = true,
     .field_types = true},
    {.lang = CBM_LANG_PYTHON,
     .quoted_names = true,
     .labels = py_labels,
     .engine_qualifies_own_module = false,
     .lexical_scopes = true,
     .return_types = true,
     .param_types = true},
};

const cbm_type_scope_lang_t *cbm_type_scope_lang(CBMLanguage lang) {
    for (size_t i = 0; i < sizeof(type_scope_langs) / sizeof(type_scope_langs[0]); i++) {
        if (type_scope_langs[i].lang == lang) {
            return &type_scope_langs[i];
        }
    }
    return NULL;
}

/* ── Work counter (test builds) ───────────────────────────────────── */

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
static _Atomic uint64_t g_type_scope_work;

uint64_t cbm_type_scope_test_work(void) {
    return atomic_load_explicit(&g_type_scope_work, memory_order_relaxed);
}

#define TYPE_SCOPE_WORK(n) \
    atomic_fetch_add_explicit(&g_type_scope_work, (uint64_t)(n), memory_order_relaxed)
#else
#define TYPE_SCOPE_WORK(n) ((void)0)
#endif

/* ── Per-file state ───────────────────────────────────────────────── */

/* Distinct, never-dereferenced values: "two imports bind this local to
 * different targets", a memoized miss, and the two nesting verdicts. */
static char g_ambiguous_mark;
static char g_miss_mark;
static char g_nest_open_mark;
static char g_nest_local_mark;
static char g_bound_once_mark;
static char g_bound_many_mark;
#define TYPE_SCOPE_BOUND_ONCE ((void *)&g_bound_once_mark)
#define TYPE_SCOPE_BOUND_MANY ((void *)&g_bound_many_mark)
#define TYPE_SCOPE_AMBIGUOUS ((void *)&g_ambiguous_mark)
#define TYPE_SCOPE_MISS ((void *)&g_miss_mark)
#define TYPE_SCOPE_NEST_OPEN ((void *)&g_nest_open_mark)   /* module or class bodies only */
#define TYPE_SCOPE_NEST_LOCAL ((void *)&g_nest_local_mark) /* inside a function */

struct cbm_type_scope_file {
    const cbm_type_scope_lang_t *lang;
    cbm_type_scope_t scope;
    size_t scope_len;      /* the module's symbol scope (a package without __init__) */
    CBMHashTable *imports; /* local -> import QN, or TYPE_SCOPE_AMBIGUOUS */
    CBMHashTable *memo;    /* spelling -> QN, or TYPE_SCOPE_MISS */
    CBMHashTable *seen;    /* spelling -> serial of the last def that wrote it */
    /* lexical scopes only */
    CBMHashTable *defs;          /* file def QN -> index + 1 into scope.file_defs */
    CBMHashTable *tparams;       /* every type parameter name the file declares */
    CBMHashTable *nesting;       /* def QN -> TYPE_SCOPE_NEST_OPEN / _LOCAL (memo) */
    CBMHashTable *bindings;      /* module-level name -> count of binding facts (1, 2+) */
    bool closed;                 /* the module's bindings cannot be listed: no identity */
    uintptr_t serial;            /* the def being attached, from 1 */
    const char *enclosing_class; /* the def's immediately enclosing class, or NULL */
    CBMArena keys;               /* memo keys and candidate strings */
};

static bool type_scope_function_label(const char *label) {
    return label && (strcmp(label, "Function") == 0 || strcmp(label, "Method") == 0);
}

/* Count the module-level bindings of every name from the ordered namespace
 * facts; a star import, a parse error or an incomplete record closes the
 * file. */
static bool type_scope_index_module_facts(cbm_type_scope_file_t *f) {
    const CBMPyNamespaceFacts *facts = f->scope.module_facts;
    f->bindings = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, (uint32_t)(facts ? facts->count + 1 : 1));
    if (!f->bindings) {
        return false;
    }
    if (!facts || facts->status != CBM_PY_NS_COMPLETE) {
        f->closed = true;
        return true;
    }
    for (int i = 0; i < facts->count && !f->closed; i++) {
        const CBMPyNamespaceFact *fact = &facts->items[i];
        TYPE_SCOPE_WORK(1);
        switch (fact->kind) {
        case CBM_PY_NS_IMPORT_STAR:
            f->closed = true; /* binds names nobody listed */
            break;
        case CBM_PY_NS_UNKNOWN:
            /* An effect or a compound statement: the import map and the
             * engine read past those as well. A parse error leaves the file's
             * bindings unknown. */
            f->closed = fact->reason == CBM_PY_NS_REASON_PARSE;
            break;
        case CBM_PY_NS_OWN_DEF:
        case CBM_PY_NS_IMPORT_MODULE:
        case CBM_PY_NS_IMPORT_NAME:
        case CBM_PY_NS_SHADOW:
        case CBM_PY_NS_DELETE:
            if (fact->local_name && fact->local_name[0]) {
                bool again = cbm_ht_get(f->bindings, fact->local_name) != NULL;
                /* keys borrow the facts, which outlive the file state */
                cbm_ht_set(f->bindings, fact->local_name,
                           again ? TYPE_SCOPE_BOUND_MANY : TYPE_SCOPE_BOUND_ONCE);
            }
            break;
        default:
            break; /* __all__ facts bind no name */
        }
    }
    return true;
}

/* True when the module binds this head more than once. */
static bool type_scope_rebound(const cbm_type_scope_file_t *f, const char *head) {
    TYPE_SCOPE_WORK(1);
    return f->bindings && cbm_ht_get(f->bindings, head) == TYPE_SCOPE_BOUND_MANY;
}

/* Hash the file's own definitions and its type parameter names. */
static bool type_scope_index_file_defs(cbm_type_scope_file_t *f) {
    int n = f->scope.file_def_count;
    f->defs = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, (uint32_t)(n + 1));
    f->tparams = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 8);
    f->nesting = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, (uint32_t)(n + 1));
    if (!f->defs || !f->tparams || !f->nesting || !type_scope_index_module_facts(f)) {
        return false;
    }
    for (int i = 0; i < n; i++) {
        const CBMDefinition *d = &f->scope.file_defs[i];
        if (d->qualified_name && !cbm_ht_get(f->defs, d->qualified_name)) {
            TYPE_SCOPE_WORK(1);
            cbm_ht_set(f->defs, d->qualified_name, (void *)(uintptr_t)(i + 1));
        }
        for (const char *p = d->type_params; p && *p;) {
            const char *bar = strchr(p, '|');
            size_t len = bar ? (size_t)(bar - p) : strlen(p);
            char *name = len ? cbm_arena_strndup(&f->keys, p, len) : NULL;
            TYPE_SCOPE_WORK(len + 1);
            if (name && !cbm_ht_get(f->tparams, name)) {
                cbm_ht_set(f->tparams, name, name);
            }
            p = bar ? bar + 1 : p + len;
        }
    }
    return true;
}

cbm_type_scope_file_t *cbm_type_scope_file_begin(const cbm_type_scope_lang_t *lang,
                                                 const cbm_type_scope_t *scope) {
    if (!lang || !scope || !scope->reg || !scope->module_qn) {
        return NULL;
    }
    cbm_type_scope_file_t *f = cbm_calloc(CBM_MEM_CLASS_EXTRACT, sizeof(*f));
    if (!f) {
        return NULL;
    }
    f->lang = lang;
    f->scope = *scope;
    f->scope_len = cbm_fqn_symbol_scope_len(scope->module_qn);
    f->imports = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, (uint32_t)(scope->imp_count + 1));
    f->memo = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 32);
    f->seen = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 32);
    cbm_arena_init_lazy(&f->keys, CBM_SZ_4K);
    if (!f->imports || !f->memo || !f->seen ||
        (lang->lexical_scopes && !type_scope_index_file_defs(f))) {
        cbm_type_scope_file_end(f);
        return NULL;
    }
    for (int i = 0; i < scope->imp_count; i++) {
        const char *key = scope->imp_keys ? scope->imp_keys[i] : NULL;
        const char *val = scope->imp_vals ? scope->imp_vals[i] : NULL;
        if (!key || !val || !key[0]) {
            continue;
        }
        TYPE_SCOPE_WORK(1);
        void *prev = cbm_ht_get(f->imports, key);
        if (!prev) {
            cbm_ht_set(f->imports, key, (void *)val);
        } else if (prev != TYPE_SCOPE_AMBIGUOUS && strcmp((const char *)prev, val) != 0) {
            cbm_ht_set(f->imports, key, TYPE_SCOPE_AMBIGUOUS);
        }
    }
    return f;
}

void cbm_type_scope_file_end(cbm_type_scope_file_t *file) {
    if (!file) {
        return;
    }
    cbm_ht_free(file->imports); /* NULL-safe */
    cbm_ht_free(file->memo);
    cbm_ht_free(file->seen);
    cbm_ht_free(file->defs);
    cbm_ht_free(file->tparams);
    cbm_ht_free(file->nesting);
    cbm_ht_free(file->bindings);
    cbm_arena_destroy(&file->keys);
    cbm_free(CBM_MEM_CLASS_EXTRACT, file);
}

/* ── Resolution ───────────────────────────────────────────────────── */

/* `qn` when it names a project definition of an allowed label, else NULL. */
static const char *type_scope_target(const cbm_type_scope_file_t *f, const char *qn) {
    TYPE_SCOPE_WORK(1);
    const char *label = cbm_registry_label_of(f->scope.reg, qn);
    if (!label) {
        return NULL;
    }
    for (const char *const *l = f->lang->labels; *l; l++) {
        if (strcmp(*l, label) == 0) {
            return qn;
        }
    }
    return NULL;
}

/* The candidate QN for `dotted` through the import that binds its head, or
 * NULL. *bound reports whether an import binds the head at all (then the
 * file's own module does not decide the name). An import names a project
 * definition or nothing: a path that names no definition (a module outside
 * the project, a name a package re-exports) is no answer. */
static const char *type_scope_via_import(cbm_type_scope_file_t *f, const char *dotted,
                                         size_t head_len, bool *bound, bool *ambiguous) {
    *bound = false;
    *ambiguous = false;
    const char *dot = dotted[head_len] == '.' ? dotted + head_len : NULL;
    if (!dot && f->lang->imports_bind_modules) {
        return NULL; /* a bare name never resolves through a package import */
    }
    char head[CBM_SZ_256];
    if (head_len == 0 || head_len >= sizeof(head)) {
        return NULL;
    }
    memcpy(head, dotted, head_len);
    head[head_len] = '\0';
    TYPE_SCOPE_WORK(1);
    void *v = cbm_ht_get(f->imports, head);
    if (!v) {
        return NULL;
    }
    *bound = true;
    if (v == TYPE_SCOPE_AMBIGUOUS) {
        *ambiguous = true;
        return NULL;
    }
    const char *val = (const char *)v;
    if (dot) {
        /* `pkg.T`: the import binds the module, the rest names inside it. */
        const char *qn = cbm_arena_sprintf(&f->keys, "%s.%s", val, dot + 1);
        return qn ? type_scope_target(f, qn) : NULL;
    }
    /* A bare name: the import binds the type itself, or (a module-valued
     * binding, Python's `from m import T` whose edge names the module) the
     * type of that name inside it. Both existing and differing is no answer. */
    const char *direct = type_scope_target(f, val);
    const char *member_qn = cbm_arena_sprintf(&f->keys, "%s.%s", val, head);
    const char *member = member_qn ? type_scope_target(f, member_qn) : NULL;
    if (direct && member && strcmp(direct, member) != 0) {
        *ambiguous = true;
        return NULL;
    }
    return direct ? direct : member;
}

/* The memo / seen key of a spelling: the spelling itself, NUL-terminated in a
 * caller buffer (a name too long for it has no identity). */
static bool type_scope_key(const char *spelling, size_t len, char *buf, size_t bufsz) {
    if (len == 0 || len >= bufsz) {
        return false;
    }
    memcpy(buf, spelling, len);
    buf[len] = '\0';
    return true;
}

static size_t type_scope_head_len(const char *spelling, size_t len) {
    const char *dot = memchr(spelling, '.', len);
    return dot ? (size_t)(dot - spelling) : len;
}

const char *cbm_type_scope_resolve(cbm_type_scope_file_t *f, const char *spelling, size_t len) {
    char probe[CBM_SZ_512];
    if (!f || !spelling || !type_scope_key(spelling, len, probe, sizeof(probe))) {
        return NULL;
    }
    TYPE_SCOPE_WORK(1);
    void *memo = cbm_ht_get(f->memo, probe);
    if (memo) {
        return memo == TYPE_SCOPE_MISS ? NULL : (const char *)memo;
    }
    char *key = cbm_arena_strndup(&f->keys, spelling, len);
    if (!key) {
        return NULL;
    }
    const char *result = NULL;
    const char *dotted = key; /* `T` or `pkg.T`, NUL-terminated */
    size_t head_len = type_scope_head_len(dotted, len);
    if (f->bindings) {
        /* A head the module binds more than once has no one answer. */
        char head[CBM_SZ_256];
        if (!type_scope_key(dotted, head_len, head, sizeof(head)) || type_scope_rebound(f, head)) {
            cbm_ht_set(f->memo, key, TYPE_SCOPE_MISS);
            return NULL;
        }
    }
    bool bound = false;
    bool ambiguous = false;
    const char *via_import = type_scope_via_import(f, dotted, head_len, &bound, &ambiguous);
    /* A Python package's names live under the package, not its __init__. */
    const char *mod = f->scope.module_qn;
    int scope_len = (int)f->scope_len;
    const char *own_qn = cbm_arena_sprintf(&f->keys, "%.*s.%s", scope_len, mod, dotted);
    const char *own = own_qn ? type_scope_target(f, own_qn) : NULL;
    if (bound && !ambiguous) {
        /* The import decides, unless the own module binds the head as well
         * (any definition of that name: a class, a function, a TypeVar
         * variable): two bindings of one name, whose winner is source order. */
        const char *head_qn =
            cbm_arena_sprintf(&f->keys, "%.*s.%.*s", scope_len, mod, (int)head_len, dotted);
        TYPE_SCOPE_WORK(1);
        bool own_binds = head_qn && cbm_registry_exists(f->scope.reg, head_qn);
        bool same = via_import && own && strcmp(own, via_import) == 0;
        result = own_binds && !same ? NULL : via_import;
    } else if (!bound) {
        result = own;
    }
    cbm_ht_set(f->memo, key, result ? (void *)result : TYPE_SCOPE_MISS);
    return result;
}

/* ── The def's own enclosing scopes (lexical rows) ───────────────── */

/* Whether the definition `qn` lies inside a function. inside(q) is true when
 * q's parent is a function, false when the parent is the module, and
 * inside(parent) when the parent is a class of this file; a parent QN this
 * file does not define is no proof of a module or class body, so it counts as
 * inside (no identity). The walk stops at the first ancestor already judged,
 * and every class it passes is judged on the way, so each definition of the
 * file is walked through once. */
static bool type_scope_function_local(cbm_type_scope_file_t *f, const char *qn) {
    const char **passed = NULL; /* classes walked through, not yet judged */
    int count = 0;
    int cap = 0;
    bool inside = true;
    const char *cur = qn;
    for (;;) {
        const char *dot = strrchr(cur, '.');
        size_t parent_len = dot ? (size_t)(dot - cur) : 0;
        if (parent_len <= f->scope_len) {
            inside = false; /* cur is a module-level definition */
            break;
        }
        char *parent = cbm_arena_strndup(&f->keys, cur, parent_len);
        TYPE_SCOPE_WORK(parent_len + 1);
        if (!parent) {
            break;
        }
        void *known = cbm_ht_get(f->nesting, parent);
        if (known) {
            inside = known == TYPE_SCOPE_NEST_LOCAL;
            break;
        }
        uintptr_t at = (uintptr_t)cbm_ht_get(f->defs, parent);
        if (!at || type_scope_function_label(f->scope.file_defs[at - 1].label)) {
            break; /* inside a function, or under a parent this file lacks */
        }
        if (count == cap) {
            int grown_cap = cap ? cap * 2 : 8;
            const char **grown =
                (const char **)cbm_arena_alloc(&f->keys, (size_t)grown_cap * sizeof(*grown));
            if (!grown) {
                break;
            }
            if (count) {
                memcpy(grown, passed, (size_t)count * sizeof(*grown));
            }
            passed = grown;
            cap = grown_cap;
        }
        passed[count++] = parent; /* a class: inside(cur) == inside(parent) */
        cur = parent;
    }
    for (int i = 0; i < count; i++) {
        cbm_ht_set(f->nesting, passed[i], inside ? TYPE_SCOPE_NEST_LOCAL : TYPE_SCOPE_NEST_OPEN);
    }
    return inside;
}

/* The def's immediately enclosing class: its parent QN when that is a
 * non-function definition of this file, else NULL. */
static const char *type_scope_enclosing_class(cbm_type_scope_file_t *f, const char *qn) {
    const char *dot = strrchr(qn, '.');
    size_t parent_len = dot ? (size_t)(dot - qn) : 0;
    if (parent_len <= f->scope_len) {
        return NULL;
    }
    char *parent = cbm_arena_strndup(&f->keys, qn, parent_len);
    TYPE_SCOPE_WORK(parent_len + 1);
    uintptr_t at = parent ? (uintptr_t)cbm_ht_get(f->defs, parent) : 0;
    if (!at || type_scope_function_label(f->scope.file_defs[at - 1].label)) {
        return NULL;
    }
    return parent;
}

/* True when the def's own scopes bind the head of this spelling before the
 * module does: a type parameter the file declares, or a name the enclosing
 * class body declares. */
static bool type_scope_shadowed(cbm_type_scope_file_t *f, const char *name, size_t len) {
    size_t head_len = type_scope_head_len(name, len);
    char head[CBM_SZ_256];
    if (!type_scope_key(name, head_len, head, sizeof(head))) {
        return true;
    }
    TYPE_SCOPE_WORK(1);
    if (cbm_ht_get(f->tparams, head)) {
        return true;
    }
    if (!f->enclosing_class) {
        return false;
    }
    const char *member = cbm_arena_sprintf(&f->keys, "%s.%s", f->enclosing_class, head);
    TYPE_SCOPE_WORK(1);
    return !member || cbm_ht_get(f->defs, member) != NULL;
}

/* ── Reduction of a type text to the names it writes ──────────────── */

static bool type_scope_ident_start(unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '$' || c >= 0x80;
}

static bool type_scope_ident_char(unsigned char c) {
    return type_scope_ident_start(c) || (c >= '0' && c <= '9');
}

typedef void (*type_scope_name_fn)(const char *name, size_t len, void *ud);

/* Call fn for every type name the text writes: a run of identifier
 * characters joined by `.`. A quoted run is a literal (skipped) unless
 * quoted_names; a name that is only a member access (one `.` before it) or a
 * label (one `:` after it: a property, a named parameter) is not a type
 * name. */
static void type_scope_names(const cbm_type_scope_lang_t *lang, const char *text,
                             type_scope_name_fn fn, void *ud) {
    size_t n = text ? strlen(text) : 0;
    TYPE_SCOPE_WORK(n);
    size_t i = 0;
    while (i < n) {
        unsigned char c = (unsigned char)text[i];
        if ((c == '"' || c == '\'' || c == '`') && !lang->quoted_names) {
            size_t close = i + 1;
            while (close < n && text[close] != (char)c) {
                close++;
            }
            i = close < n ? close + 1 : n;
            continue;
        }
        if (!type_scope_ident_start(c)) {
            i++;
            continue;
        }
        size_t start = i;
        while (i < n && type_scope_ident_char((unsigned char)text[i])) {
            i++;
        }
        for (;;) {
            if (i + 1 < n && text[i] == '.' && type_scope_ident_start((unsigned char)text[i + 1])) {
                i++;
            } else {
                break;
            }
            while (i < n && type_scope_ident_char((unsigned char)text[i])) {
                i++;
            }
        }
        bool member = start > 0 && text[start - 1] == '.' && (start < 2 || text[start - 2] != '.');
        size_t after = i;
        while (after < n && text[after] == ' ') {
            after++;
        }
        bool label = after < n && text[after] == ':' && (after + 1 >= n || text[after + 1] != ':');
        if (!member && !label) {
            fn(text + start, i - start, ud);
        }
    }
}

/* ── Attaching identities to a definition ─────────────────────────── */

typedef struct {
    cbm_type_scope_file_t *file;
    char *buf; /* "spelling=QN|..." being built (heap) */
    size_t len;
    size_t cap;
    bool failed;
} type_scope_build_t;

/* True when the current def already wrote this spelling (one hash probe; the
 * seen table remembers, per spelling, the last def that wrote it). */
static bool type_scope_seen(cbm_type_scope_file_t *f, const char *name, size_t len) {
    char probe[CBM_SZ_512];
    if (!type_scope_key(name, len, probe, sizeof(probe))) {
        return true; /* too long to key: never an identity */
    }
    TYPE_SCOPE_WORK(1);
    void *last = cbm_ht_get(f->seen, probe);
    if (last == (void *)f->serial) {
        return true;
    }
    const char *key = last ? cbm_ht_get_key(f->seen, probe) : cbm_arena_strdup(&f->keys, probe);
    if (key) {
        cbm_ht_set(f->seen, key, (void *)f->serial);
    }
    return false;
}

static bool type_scope_append(type_scope_build_t *b, const char *s, size_t n) {
    if (b->len + n + 1 > b->cap) {
        size_t cap = b->cap ? b->cap * 2 : 128;
        while (cap < b->len + n + 1) {
            cap *= 2;
        }
        char *grown = cbm_realloc(CBM_MEM_CLASS_EXTRACT, b->buf, cap);
        if (!grown) {
            b->failed = true;
            return false;
        }
        b->buf = grown;
        b->cap = cap;
    }
    memcpy(b->buf + b->len, s, n);
    b->len += n;
    b->buf[b->len] = '\0';
    return true;
}

static void type_scope_on_name(const char *name, size_t len, void *ud) {
    type_scope_build_t *b = (type_scope_build_t *)ud;
    if (b->failed || type_scope_seen(b->file, name, len)) {
        return;
    }
    if (b->file->lang->lexical_scopes && type_scope_shadowed(b->file, name, len)) {
        return;
    }
    const char *qn = cbm_type_scope_resolve(b->file, name, len);
    if (!qn) {
        return;
    }
    if (b->file->lang->engine_qualifies_own_module) {
        /* The engine already qualifies this spelling to exactly this QN. */
        const char *mod = b->file->scope.module_qn;
        size_t ml = strlen(mod);
        if (strncmp(qn, mod, ml) == 0 && qn[ml] == '.' && strlen(qn + ml + 1) == len &&
            memcmp(qn + ml + 1, name, len) == 0) {
            return;
        }
    }
    if ((b->len && !type_scope_append(b, "|", 1)) || !type_scope_append(b, name, len) ||
        !type_scope_append(b, "=", 1) || !type_scope_append(b, qn, strlen(qn))) {
        return;
    }
}

/* Each '|'-separated piece of `text`; with after_colon, only the part after
 * the first ':' of each piece (field_defs' "name:type"). */
static void type_scope_scan_pieces(type_scope_build_t *b, const char *text, bool after_colon) {
    if (!text || !text[0]) {
        return;
    }
    size_t n = strlen(text);
    char *copy = cbm_arena_strndup(&b->file->keys, text, n);
    if (!copy) {
        return;
    }
    char *piece = copy;
    for (size_t i = 0; i <= n; i++) {
        if (copy[i] != '|' && copy[i] != '\0') {
            continue;
        }
        copy[i] = '\0';
        const char *t = piece;
        if (after_colon) {
            const char *colon = strchr(piece, ':');
            t = colon ? colon + 1 : NULL;
        }
        if (t && t[0]) {
            type_scope_names(b->file->lang, t, type_scope_on_name, b);
        }
        piece = copy + i + 1;
    }
}

void cbm_type_scope_attach(cbm_type_scope_file_t *file, CBMArena *arena, CBMLSPDef *def) {
    if (!file || !arena || !def) {
        return;
    }
    const cbm_type_scope_lang_t *lang = file->lang;
    file->serial++;
    file->enclosing_class = NULL;
    if (lang->lexical_scopes) {
        /* A module whose bindings cannot be listed (a star import, an effect
         * binding unknown names), and a def inside a function (it sees locals
         * the step cannot list): neither gets one. */
        if (file->closed || !def->qualified_name ||
            type_scope_function_local(file, def->qualified_name)) {
            return;
        }
        file->enclosing_class = type_scope_enclosing_class(file, def->qualified_name);
    }
    type_scope_build_t b = {.file = file};
    if (lang->return_types) {
        type_scope_scan_pieces(&b, def->return_types, false);
    }
    if (lang->param_types && def->signature_param_types) {
        for (int i = 0; i < def->signature_param_count; i++) {
            const char *t = def->signature_param_types[i];
            if (t && strcmp(t, "?") != 0) {
                type_scope_names(lang, t, type_scope_on_name, &b);
            }
        }
    }
    if (lang->field_types) {
        type_scope_scan_pieces(&b, def->field_defs, true);
    }
    if (lang->embedded_types) {
        type_scope_scan_pieces(&b, def->embedded_types, false);
    }
    if (b.len > 0 && !b.failed) {
        def->type_scope = cbm_arena_strndup(arena, b.buf, b.len);
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, b.buf);
}
