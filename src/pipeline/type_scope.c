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
#include "graph_buffer/graph_buffer.h" /* the module check of a target */
#include "lsp/java_lsp.h"              /* cbm_java_lang_type (the Java row) */

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
/* Java: what cbm_java_register_lsp_defs registers as a type. */
static const char *const java_labels[] = {"Class", "Interface", "Enum", "Type", NULL};

/* One row per language whose engine reads identities. A row states every
 * field; a further language plugs in by appending its row. */
static const cbm_type_scope_lang_t type_scope_langs[] = {
    {.lang = CBM_LANG_GO,
     .scope_source = CBM_TYPE_SCOPE_MODULE_IMPORTS,
     .quoted_names = false,
     .colon_paths = false,
     .labels = go_labels,
     .engine_qualifies_own_module = true,
     .imports_bind_modules = true,
     .lexical_scopes = false,
     .open_imports = false, /* a dot import binds "."; no spelling names it */
     .implicit_external = NULL,
     .nested_types = false,
     .return_types = true,
     .param_types = true,
     .field_types = true,
     .embedded_types = true},
    {.lang = CBM_LANG_PYTHON,
     .scope_source = CBM_TYPE_SCOPE_MODULE_IMPORTS,
     .quoted_names = true,
     .colon_paths = false,
     .labels = py_labels,
     .engine_qualifies_own_module = false,
     .imports_bind_modules = false,
     .lexical_scopes = true,
     .open_imports = false, /* a star import closes the file (module facts) */
     .implicit_external = NULL,
     .nested_types = false, /* a class body's names shadow: no identity */
     .return_types = true,
     .param_types = true,
     .field_types = false, /* folded as resolved QNs (pass_lsp_cross.c) */
     .embedded_types = false /* bases arrive as resolved QNs */},
    /* Java: its import map is built from its own import rows over the run's
     * packages (the engine row's scope_imports_from_rows); its registry keys
     * namespace QNs, so every identity is stored (the collector rewrites them
     * into that form with the defs). */
    {.lang = CBM_LANG_JAVA,
     .scope_source = CBM_TYPE_SCOPE_MODULE_IMPORTS,
     .quoted_names = false,
     .colon_paths = false,
     .labels = java_labels,
     .engine_qualifies_own_module = false,
     .imports_bind_modules = false, /* `import a.B;` binds the type B */
     .lexical_scopes = false,
     .open_imports = true, /* `import a.*;` and `import static a.B.*;` */
     .implicit_external = cbm_java_lang_type,
     .nested_types = true,
     .type_params_shadow = true, /* `class Box<Item>`: Item is no project class */
     .return_types = true,
     .param_types = true,
     .field_types = true,
     .embedded_types = true},
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
static char g_overload_mark;
static char g_overload_differ_mark;
#define TYPE_SCOPE_BOUND_ONCE ((void *)&g_bound_once_mark)
#define TYPE_SCOPE_BOUND_MANY ((void *)&g_bound_many_mark)
#define TYPE_SCOPE_AMBIGUOUS ((void *)&g_ambiguous_mark)
#define TYPE_SCOPE_MISS ((void *)&g_miss_mark)
#define TYPE_SCOPE_NEST_OPEN ((void *)&g_nest_open_mark)   /* module or class bodies only */
#define TYPE_SCOPE_NEST_LOCAL ((void *)&g_nest_local_mark) /* inside a function */
#define TYPE_SCOPE_OVERLOAD ((void *)&g_overload_mark)     /* declared with @overload */
#define TYPE_SCOPE_OVERLOAD_DIFFER ((void *)&g_overload_differ_mark)
#define TYPE_SCOPE_ALIAS_MAX 256 /* a decorator name read through the imports */

struct cbm_type_scope_file {
    const cbm_type_scope_lang_t *lang;
    cbm_type_scope_t scope;
    size_t scope_len;      /* the module's symbol scope (a package without __init__) */
    CBMHashTable *imports; /* local -> import QN, or TYPE_SCOPE_AMBIGUOUS */
    CBMHashTable *memo;    /* spelling -> QN, or TYPE_SCOPE_MISS */
    CBMHashTable *seen;    /* spelling -> serial of the last def that wrote it */
    /* lexical scopes only */
    CBMHashTable *defs;          /* file def QN -> index + 1 into scope.file_defs */
    CBMHashTable *class_binds;   /* "<class QN>.<name>" a class body binds (not a def) */
    CBMHashTable *overloaded;    /* QN some def declares with @overload -> the return text
                                  * all its defs share, or TYPE_SCOPE_OVERLOAD_DIFFER */
    CBMHashTable *ovl_alias;     /* local name a module-level `from typing import overload
                                  * as X` binds */
    CBMHashTable *tparams;       /* every type parameter name the file declares */
    CBMHashTable *nesting;       /* def QN -> TYPE_SCOPE_NEST_OPEN / _LOCAL (memo) */
    CBMHashTable *bindings;      /* module-level name -> count of binding facts (1, 2+) */
    bool closed;                 /* the module's bindings cannot be listed: no identity */
    uintptr_t serial;            /* the def being attached, from 1 */
    const char *enclosing_class; /* the def's immediately enclosing class, or NULL */
    /* open_imports only: the packages the file's open imports name, each once,
     * in import-map order. */
    const char **open;
    int open_count;
    /* nested_types and lexical nesting: "<enclosing QN>\x1f<name>" -> the
     * answer through that enclosing scope and its outer ones (a QN, or
     * TYPE_SCOPE_MISS). */
    CBMHashTable *nest_memo;
    const char *nest_start; /* the def being attached: its innermost enclosing scope */
    /* the calling-file table (cbm_type_scope_file_imports), built once */
    const char **fi_locals;
    const char **fi_qns;
    int fi_count;
    bool fi_built;
    CBMArena keys; /* memo keys and candidate strings */
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

/* Each name a class body binds besides its definitions, keyed as a member of
 * the class ("<class QN>.<name>", the key type_scope_shadowed probes). */
static void type_scope_index_class_bindings(cbm_type_scope_file_t *f, const CBMDefinition *d) {
    const char *bound = d->type_facts ? d->type_facts->class_bindings : NULL;
    for (const char *p = bound; p && *p && d->qualified_name;) {
        const char *bar = strchr(p, '|');
        size_t len = bar ? (size_t)(bar - p) : strlen(p);
        TYPE_SCOPE_WORK(len + 1);
        char *key =
            len ? cbm_arena_sprintf(&f->keys, "%s.%.*s", d->qualified_name, (int)len, p) : NULL;
        if (key && !cbm_ht_get(f->class_binds, key)) {
            cbm_ht_set(f->class_binds, key, key);
        }
        p = bar ? bar + 1 : p + len;
    }
}

/* A name some function or class body of the file declares `global` and binds
 * is rebound in the module by that body (when it runs, or at import for a
 * class body): like a name the module binds twice, it gets no identity. */
static void type_scope_index_global_names(cbm_type_scope_file_t *f, const CBMDefinition *d) {
    const char *names = d->type_facts ? d->type_facts->global_names : NULL;
    for (const char *p = names; p && *p;) {
        const char *bar = strchr(p, '|');
        size_t len = bar ? (size_t)(bar - p) : strlen(p);
        TYPE_SCOPE_WORK(len + 1);
        char *name = len ? cbm_arena_strndup(&f->keys, p, len) : NULL;
        if (name) {
            cbm_ht_set(f->bindings, name, TYPE_SCOPE_BOUND_MANY);
        }
        p = bar ? bar + 1 : p + len;
    }
}

/* The local names the module's imports bind to typing's overload under another
 * name (`from typing import overload as ovl`, typing_extensions too). */
static void type_scope_index_ovl_alias(cbm_type_scope_file_t *f) {
    const CBMPyNamespaceFacts *facts = f->scope.module_facts;
    for (int i = 0; facts && i < facts->count; i++) {
        const CBMPyNamespaceFact *x = &facts->items[i];
        TYPE_SCOPE_WORK(1);
        if (x->kind == CBM_PY_NS_IMPORT_NAME && x->relative_level == 0 && x->local_name &&
            x->module_name && x->member_name && strcmp(x->member_name, "overload") == 0 &&
            (strcmp(x->module_name, "typing") == 0 ||
             strcmp(x->module_name, "typing_extensions") == 0)) {
            cbm_ht_set(f->ovl_alias, x->local_name, (void *)x->local_name);
        }
    }
}

/* Whether a decorator's text is typing's @overload: its last dotted name is
 * `overload` (`@overload`, `@typing.overload`, `@t.overload`), or it is a
 * plain name the module imports as typing's overload (`@ovl`). */
static bool type_scope_overload_decorator(const cbm_type_scope_file_t *f, const char *text) {
    while (text && (*text == '@' || *text == ' ' || *text == '\t')) {
        text++;
    }
    if (!text || !*text) {
        return false;
    }
    size_t len = strcspn(text, "( \t\r\n");
    const char *name = text;
    for (size_t i = 0; i < len; i++) {
        if (text[i] == '.') {
            name = text + i + 1;
        }
    }
    if ((size_t)(text + len - name) == strlen("overload") &&
        memcmp(name, "overload", strlen("overload")) == 0) {
        return true;
    }
    char local[TYPE_SCOPE_ALIAS_MAX];
    if (name != text || len >= sizeof(local) || cbm_ht_count(f->ovl_alias) == 0) {
        return false;
    }
    memcpy(local, text, len);
    local[len] = '\0';
    return cbm_ht_get(f->ovl_alias, local) != NULL;
}

/* A function with @overload declarations: callers see the overloads, which
 * pick the result from the arguments, so the implementation's annotations are
 * not its type at a call, nor is any one overload's -- unless all of its
 * definitions declare the same return type (whichever overload a call picks,
 * that is the type). Where they differ its QN takes no identity. */
static void type_scope_index_overload(cbm_type_scope_file_t *f, const CBMDefinition *d) {
    for (int i = 0; d->decorators && d->decorators[i] && d->qualified_name; i++) {
        TYPE_SCOPE_WORK(1);
        if (type_scope_overload_decorator(f, d->decorators[i])) {
            cbm_ht_set(f->overloaded, d->qualified_name, TYPE_SCOPE_OVERLOAD);
            return;
        }
    }
}

/* Second pass: whether the definitions of each overloaded QN agree on the
 * return type text (none counts as a text of its own). */
static void type_scope_compare_overloads(cbm_type_scope_file_t *f) {
    int n = cbm_ht_count(f->overloaded) ? f->scope.file_def_count : 0;
    for (int i = 0; i < n; i++) {
        const CBMDefinition *d = &f->scope.file_defs[i];
        void *seen = d->qualified_name ? cbm_ht_get(f->overloaded, d->qualified_name) : NULL;
        const char *ret = d->return_type ? d->return_type : "";
        TYPE_SCOPE_WORK(strlen(ret) + 1);
        if (seen == TYPE_SCOPE_OVERLOAD) {
            cbm_ht_set(f->overloaded, d->qualified_name, (void *)ret);
        } else if (seen && seen != TYPE_SCOPE_OVERLOAD_DIFFER && strcmp((const char *)seen, ret)) {
            cbm_ht_set(f->overloaded, d->qualified_name, TYPE_SCOPE_OVERLOAD_DIFFER);
        }
    }
}

/* One definition's type parameter names into f->tparams. */
static void type_scope_index_type_params(cbm_type_scope_file_t *f, const CBMDefinition *d) {
    for (const char *p = d->type_facts ? d->type_facts->type_params : NULL; p && *p;) {
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

/* Hash the file's own definitions, its type parameter names and the names
 * its class bodies bind. */
static bool type_scope_index_file_defs(cbm_type_scope_file_t *f) {
    int n = f->scope.file_def_count;
    f->defs = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, (uint32_t)(n + 1));
    f->class_binds = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 8);
    f->overloaded = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 8);
    f->ovl_alias = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 8);
    f->tparams = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 8);
    f->nesting = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, (uint32_t)(n + 1));
    if (!f->defs || !f->class_binds || !f->overloaded || !f->ovl_alias || !f->tparams ||
        !f->nesting || !type_scope_index_module_facts(f)) {
        return false;
    }
    type_scope_index_ovl_alias(f);
    for (int i = 0; i < n; i++) {
        const CBMDefinition *d = &f->scope.file_defs[i];
        if (d->qualified_name && !cbm_ht_get(f->defs, d->qualified_name)) {
            TYPE_SCOPE_WORK(1);
            cbm_ht_set(f->defs, d->qualified_name, (void *)(uintptr_t)(i + 1));
        }
        type_scope_index_class_bindings(f, d);
        type_scope_index_global_names(f, d);
        type_scope_index_overload(f, d);
        type_scope_index_type_params(f, d);
    }
    type_scope_compare_overloads(f);
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
    bool nests = lang->nested_types || lang->scope_source == CBM_TYPE_SCOPE_LEXICAL_NESTING;
    f->nest_memo = nests ? cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 32) : NULL;
    cbm_arena_init_lazy(&f->keys, CBM_SZ_4K);
    if (lang->open_imports && scope->imp_count > 0) {
        f->open = cbm_arena_alloc(&f->keys, (size_t)scope->imp_count * sizeof(*f->open));
    }
    if (!f->imports || !f->memo || !f->seen || (nests && !f->nest_memo) ||
        (lang->open_imports && scope->imp_count > 0 && !f->open) ||
        (lang->lexical_scopes && !type_scope_index_file_defs(f))) {
        cbm_type_scope_file_end(f);
        return NULL;
    }
    if (lang->type_params_shadow && !lang->lexical_scopes) {
        f->tparams = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 8);
        if (!f->tparams) {
            cbm_type_scope_file_end(f);
            return NULL;
        }
        for (int i = 0; i < scope->file_def_count && scope->file_defs; i++) {
            type_scope_index_type_params(f, &scope->file_defs[i]);
        }
    }
    for (int i = 0; i < scope->imp_count; i++) {
        const char *key = scope->imp_keys ? scope->imp_keys[i] : NULL;
        const char *val = scope->imp_vals ? scope->imp_vals[i] : NULL;
        if (!key || !val || !key[0]) {
            continue;
        }
        TYPE_SCOPE_WORK(1);
        if (lang->open_imports && strcmp(key, CBM_TYPE_SCOPE_OPEN_KEY) == 0) {
            /* An open import: its package, once (one probe of the memo). */
            char *dedupe = cbm_arena_sprintf(&f->keys, "\x1f%s", val);
            if (dedupe && !cbm_ht_get(f->memo, dedupe)) {
                cbm_ht_set(f->memo, dedupe, (void *)val);
                f->open[f->open_count++] = val;
            }
            continue;
        }
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
    cbm_ht_free(file->class_binds);
    cbm_ht_free(file->overloaded);
    cbm_ht_free(file->ovl_alias);
    cbm_ht_free(file->tparams);
    cbm_ht_free(file->nesting);
    cbm_ht_free(file->bindings);
    cbm_ht_free(file->nest_memo);
    cbm_arena_destroy(&file->keys);
    cbm_free(CBM_MEM_CLASS_EXTRACT, file);
}

/* ── Resolution ───────────────────────────────────────────────────── */

/* `qn` when it names a project definition of an allowed label, else NULL. A
 * QN the graph holds as a module (a class and a module of one QN) is none:
 * the two share their members' QNs. */
static const char *type_scope_target(const cbm_type_scope_file_t *f, const char *qn) {
    TYPE_SCOPE_WORK(1);
    const char *label = cbm_registry_label_of(f->scope.reg, qn);
    if (!label) {
        return NULL;
    }
    for (const char *const *l = f->lang->labels; *l; l++) {
        if (strcmp(*l, label) == 0) {
            const cbm_gbuf_node_t *node = f->lang->lexical_scopes && f->scope.graph
                                              ? cbm_gbuf_find_by_qn(f->scope.graph, qn)
                                              : NULL;
            return node && node->label && strcmp(node->label, "Module") == 0 ? NULL : qn;
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

/* open_imports / implicit_external: the one target `<package>.<dotted>` names
 * across the file's open packages, for a name the own module and the named
 * imports leave unbound. A head the own module declares (any label) shadows
 * the open imports; a head the implicit external scope declares, and two
 * different targets, are no answer. */
static const char *type_scope_via_open(cbm_type_scope_file_t *f, const char *dotted,
                                       size_t head_len) {
    const char *mod = f->scope.module_qn;
    int scope_len = (int)f->scope_len;
    const char *head_qn =
        cbm_arena_sprintf(&f->keys, "%.*s.%.*s", scope_len, mod, (int)head_len, dotted);
    TYPE_SCOPE_WORK(1);
    if (!head_qn || cbm_registry_exists(f->scope.reg, head_qn)) {
        return NULL;
    }
    if (f->lang->implicit_external && f->lang->implicit_external(dotted, head_len)) {
        return NULL;
    }
    const char *found = NULL;
    for (int i = 0; i < f->open_count; i++) {
        const char *qn = NULL;
        if (f->open[i][0] == CBM_TYPE_SCOPE_OPEN_PACKAGE) {
            /* A package: one lookup, however many modules declare it. */
            bool two = false;
            qn = f->scope.open_package
                     ? f->scope.open_package(f->scope.open_package_ud, f->open[i] + 1, dotted, &two)
                     : NULL;
            if (two) {
                return NULL;
            }
        } else {
            qn = cbm_arena_sprintf(&f->keys, "%s.%s", f->open[i], dotted);
        }
        const char *hit = qn ? type_scope_target(f, qn) : NULL;
        if (hit && found && strcmp(hit, found) != 0) {
            return NULL;
        }
        found = hit ? hit : found;
    }
    return found;
}

/* CBM_TYPE_SCOPE_INCLUDE_CLOSURE: the head through the caller's include
 * closure, the rest named inside it. */
static const char *type_scope_via_visible(cbm_type_scope_file_t *f, const char *dotted,
                                          size_t head_len) {
    if (!f->scope.visible || head_len == 0) {
        return NULL;
    }
    TYPE_SCOPE_WORK(1);
    const char *head = f->scope.visible(f->scope.visible_ud, dotted, head_len);
    if (!head) {
        return NULL;
    }
    if (dotted[head_len] != '.') {
        return type_scope_target(f, head);
    }
    const char *qn = cbm_arena_sprintf(&f->keys, "%s.%s", head, dotted + head_len + 1);
    return qn ? type_scope_target(f, qn) : NULL;
}

/* Nested scopes (nested_types, lexical nesting): the innermost scope, from
 * `start` outward through its QN prefixes no shorter than `min_len` bytes,
 * that declares `head` ("<scope>.<head>" names a project definition of any
 * label), or NULL. Memoized per (scope, head) for every scope the walk passes,
 * so a name written at nesting depth d costs at most d probes once per
 * file. */
static const char *type_scope_declaring_scope(cbm_type_scope_file_t *f, const char *start,
                                              size_t min_len, const char *head, size_t head_len) {
    const char **passed = NULL; /* memo keys walked through, not yet answered */
    int count = 0;
    int cap = 0;
    const char *answer = NULL;
    size_t cur_len = start ? strlen(start) : 0;
    while (cur_len >= min_len && cur_len > 0) {
        char *key =
            cbm_arena_sprintf(&f->keys, "%.*s\x1f%.*s", (int)cur_len, start, (int)head_len, head);
        TYPE_SCOPE_WORK(cur_len + head_len + 1);
        if (!key) {
            break;
        }
        void *known = cbm_ht_get(f->nest_memo, key);
        if (known) {
            answer = known == TYPE_SCOPE_MISS ? NULL : (const char *)known;
            break;
        }
        char *qn =
            cbm_arena_sprintf(&f->keys, "%.*s.%.*s", (int)cur_len, start, (int)head_len, head);
        if (qn && cbm_registry_exists(f->scope.reg, qn)) {
            answer = cbm_arena_strndup(&f->keys, start, cur_len);
            cbm_ht_set(f->nest_memo, key, answer ? (void *)answer : TYPE_SCOPE_MISS);
            break;
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
        passed[count++] = key;
        const char *dot = NULL;
        for (size_t i = cur_len; i > 0; i--) {
            if (start[i - 1] == '.') {
                dot = start + i - 1;
                break;
            }
        }
        cur_len = dot ? (size_t)(dot - start) : 0;
    }
    for (int i = 0; i < count; i++) {
        cbm_ht_set(f->nest_memo, passed[i], answer ? (void *)answer : TYPE_SCOPE_MISS);
    }
    return answer;
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

/* The spelling with its parts joined by `.` (colon_paths: `a::B` is
 * `a.B`), NUL-terminated in the file's arena; *out_len its length. */
static const char *type_scope_dotted(cbm_type_scope_file_t *f, const char *spelling, size_t len,
                                     size_t *out_len) {
    char *out = cbm_arena_strndup(&f->keys, spelling, len);
    size_t n = len;
    if (out && f->lang->colon_paths) {
        size_t at = 0;
        for (size_t i = 0; i < len; i++) {
            if (spelling[i] == ':' && i + 1 < len && spelling[i + 1] == ':') {
                out[at++] = '.';
                i++;
            } else {
                out[at++] = spelling[i];
            }
        }
        out[at] = '\0';
        n = at;
    }
    *out_len = n;
    return out;
}

/* Lexical nesting resolves in the definition's scopes only (the attach). */
static bool type_scope_needs_def(const cbm_type_scope_lang_t *lang) {
    return lang->scope_source == CBM_TYPE_SCOPE_LEXICAL_NESTING;
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
    size_t dotted_len = len;
    /* `T` or `pkg.T`, NUL-terminated */
    const char *dotted = f->lang->colon_paths ? type_scope_dotted(f, key, len, &dotted_len) : key;
    if (!dotted || type_scope_needs_def(f->lang)) {
        cbm_ht_set(f->memo, key, TYPE_SCOPE_MISS);
        return NULL;
    }
    size_t head_len = type_scope_head_len(dotted, dotted_len);
    if (f->lang->scope_source == CBM_TYPE_SCOPE_INCLUDE_CLOSURE) {
        result = type_scope_via_visible(f, dotted, head_len);
        cbm_ht_set(f->memo, key, result ? (void *)result : TYPE_SCOPE_MISS);
        return result;
    }
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
        if (!result && (f->open_count > 0 || f->lang->implicit_external)) {
            result = type_scope_via_open(f, dotted, head_len);
        }
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
 * class body declares (a nested definition) or binds (an assignment, an
 * import, a loop or with or except target...). */
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
    return !member || cbm_ht_get(f->defs, member) != NULL ||
           cbm_ht_get(f->class_binds, member) != NULL;
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
            } else if (lang->colon_paths && i + 2 < n && text[i] == ':' && text[i + 1] == ':' &&
                       type_scope_ident_start((unsigned char)text[i + 2])) {
                i += 2;
            } else {
                break;
            }
            while (i < n && type_scope_ident_char((unsigned char)text[i])) {
                i++;
            }
        }
        if (lang->colon_paths && start >= 2 && text[start - 1] == ':' && text[start - 2] == ':') {
            start -= 2; /* `::T`, the global scope's: kept whole, it has no identity */
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

/* True when `label` is one the row's identities may name (a type the engine
 * registers). */
static bool type_scope_label_allowed(const cbm_type_scope_lang_t *lang, const char *label) {
    for (const char *const *l = lang->labels; label && l && *l; l++) {
        if (strcmp(*l, label) == 0) {
            return true;
        }
    }
    return false;
}

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

/* nested_types / lexical nesting: the answer through the definition's
 * enclosing scopes (from file->nest_start outward). *decided: an enclosing
 * scope declares the head, so its answer (possibly none) is final; a
 * lexical-nesting row resolves through nested scopes only. */
static const char *type_scope_nested(cbm_type_scope_file_t *f, const char *name, size_t len,
                                     bool *decided) {
    bool lexical = type_scope_needs_def(f->lang);
    *decided = lexical;
    size_t dotted_len = len;
    const char *dotted = type_scope_dotted(f, name, len, &dotted_len);
    size_t head_len = dotted ? type_scope_head_len(dotted, dotted_len) : 0;
    if (!dotted || head_len == 0) {
        *decided = true; /* `::T`, or no memory: no identity */
        return NULL;
    }
    size_t min_len = lexical ? f->scope_len : f->scope_len + 1;
    const char *scope = type_scope_declaring_scope(f, f->nest_start, min_len, dotted, head_len);
    if (!scope) {
        return NULL;
    }
    *decided = true;
    const char *qn = cbm_arena_sprintf(&f->keys, "%s.%s", scope, dotted);
    return qn ? type_scope_target(f, qn) : NULL;
}

static void type_scope_on_name(const char *name, size_t len, void *ud) {
    type_scope_build_t *b = (type_scope_build_t *)ud;
    if (b->failed || type_scope_seen(b->file, name, len)) {
        return;
    }
    if (b->file->lang->lexical_scopes && type_scope_shadowed(b->file, name, len)) {
        return;
    }
    if (b->file->lang->type_params_shadow && b->file->tparams) {
        char head[CBM_SZ_256];
        size_t head_len = type_scope_head_len(name, len);
        TYPE_SCOPE_WORK(1);
        if (!type_scope_key(name, head_len, head, sizeof(head)) ||
            cbm_ht_get(b->file->tparams, head)) {
            return; /* a type parameter of the file: no identity */
        }
    }
    bool decided = false;
    const char *qn = b->file->nest_memo ? type_scope_nested(b->file, name, len, &decided) : NULL;
    if (!decided) {
        qn = cbm_type_scope_resolve(b->file, name, len);
    }
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
    file->nest_start = NULL;
    if (file->nest_memo && def->qualified_name) {
        /* A type's own texts are written inside it (nested_types); everything
         * else, and every lexical-nesting def, inside its parent. */
        bool inside_self =
            !type_scope_needs_def(lang) && type_scope_label_allowed(lang, def->label);
        const char *dot = strrchr(def->qualified_name, '.');
        file->nest_start = inside_self ? def->qualified_name
                           : dot       ? cbm_arena_strndup(&file->keys, def->qualified_name,
                                                           (size_t)(dot - def->qualified_name))
                                       : NULL;
    }
    if (lang->lexical_scopes) {
        /* A module whose bindings cannot be listed (a star import, an effect
         * binding unknown names), and a def inside a function (it sees locals
         * the step cannot list): neither gets one. */
        if (file->closed || !def->qualified_name ||
            type_scope_function_local(file, def->qualified_name) ||
            cbm_ht_get(file->overloaded, def->qualified_name) == TYPE_SCOPE_OVERLOAD_DIFFER) {
            return; /* (an overloaded function: type_scope_index_overload) */
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

/* ── The calling file's own identities ───────────────────────────── */

int cbm_type_scope_file_imports(cbm_type_scope_file_t *file, const char *const **out_locals,
                                const char *const **out_qns) {
    if (out_locals) {
        *out_locals = NULL;
    }
    if (out_qns) {
        *out_qns = NULL;
    }
    if (!file) {
        return 0;
    }
    if (!file->fi_built) {
        file->fi_built = true;
        /* Only an import-map scope has named imports to verify, and a module
         * whose bindings cannot be listed has none. */
        int n = file->lang->scope_source == CBM_TYPE_SCOPE_MODULE_IMPORTS && !file->closed
                    ? file->scope.imp_count
                    : 0;
        CBMHashTable *done =
            n > 0 ? cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, (uint32_t)(n + 1)) : NULL;
        file->fi_locals = done ? cbm_arena_alloc(&file->keys, (size_t)n * sizeof(char *)) : NULL;
        file->fi_qns = done ? cbm_arena_alloc(&file->keys, (size_t)n * sizeof(char *)) : NULL;
        for (int i = 0; file->fi_locals && file->fi_qns && i < n; i++) {
            const char *local = file->scope.imp_keys ? file->scope.imp_keys[i] : NULL;
            TYPE_SCOPE_WORK(1);
            if (!local || !local[0] || cbm_ht_get(done, local) ||
                (file->lang->open_imports && strcmp(local, CBM_TYPE_SCOPE_OPEN_KEY) == 0)) {
                continue;
            }
            cbm_ht_set(done, local, (void *)local);
            const char *qn = cbm_type_scope_resolve(file, local, strlen(local));
            if (qn) {
                file->fi_locals[file->fi_count] = local;
                file->fi_qns[file->fi_count] = qn;
                file->fi_count++;
            }
        }
        cbm_ht_free(done); /* NULL-safe */
    }
    if (out_locals) {
        *out_locals = (const char *const *)file->fi_locals;
    }
    if (out_qns) {
        *out_qns = (const char *const *)file->fi_qns;
    }
    return file->fi_count;
}
