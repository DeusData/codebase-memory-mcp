/*
 * test_type_scope.c — declared types resolved once, in the scope that wrote
 * them (src/pipeline/type_scope.h).
 *
 * Unit tests pin the shared step's answers against a hand-built registry: an
 * identity is stored only when the declaring file's scope gives exactly one
 * answer and that answer is a project type. Growth tests count the work of
 * every loop the step adds at two sizes. Pipeline tests index small fixtures
 * per engine (a caller, a file declaring a function or field whose type it
 * imports, the file defining that type, and a same-named decoy) with one
 * worker and with four, above the parallel threshold; the shapes the review
 * of the first cut found false edges in get no edge; and every incremental
 * route gives the full index's graph.
 */
#include "test_framework.h"
#include "test_helpers.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/arena.h"
#include "lsp/type_rep.h"
#include "pipeline/lsp_surface.h"
#include "pipeline/pass_lsp_cross.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h" /* incremental route seam */
#include "pipeline/type_scope.h"
#include "sqlite3.h"
#include "store/store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Unit: the shared step ─────────────────────────────────────────── */

typedef struct {
    cbm_registry_t *reg;
    CBMArena arena;
} ts_unit_t;

static void ts_unit_begin(ts_unit_t *u) {
    u->reg = cbm_registry_new();
    cbm_arena_init(&u->arena);
    cbm_registry_add(u->reg, "Store", "p.a.Store", "Class", false);
    cbm_registry_add(u->reg, "Store", "p.b.Store", "Class", false);
    cbm_registry_add(u->reg, "Result", "p.c.Result", "Variable", false);
    cbm_registry_add(u->reg, "make", "p.c.make", "Function", false);
    cbm_registry_add(u->reg, "Local", "p.app.Local", "Class", false);
}

static void ts_unit_end(ts_unit_t *u) {
    cbm_registry_free(u->reg);
    cbm_arena_destroy(&u->arena);
}

/* A Python module's namespace facts: each name bound once by an import. */
typedef struct {
    CBMPyNamespaceFact items[16];
    CBMPyNamespaceFacts facts;
} ts_facts_t;

static const CBMPyNamespaceFacts *ts_facts(ts_facts_t *f, const char *const *bound, int n) {
    memset(f, 0, sizeof(*f));
    for (int i = 0; i < n && i < 16; i++) {
        f->items[i] = (CBMPyNamespaceFact){.kind = CBM_PY_NS_IMPORT_NAME, .local_name = bound[i]};
    }
    f->facts = (CBMPyNamespaceFacts){.version = CBM_PY_NAMESPACE_FACTS_VERSION,
                                     .language = CBM_LANG_PYTHON,
                                     .status = CBM_PY_NS_COMPLETE,
                                     .items = f->items,
                                     .count = n < 16 ? n : 16,
                                     .cap = 16};
    return &f->facts;
}

typedef struct {
    CBMLanguage lang;
    const char *def_qn; /* default p.app.f */
    const char *ret;
    const char *const *keys;
    const char *const *vals;
    int n;
    const CBMDefinition *file_defs;
    int file_def_count;
    const CBMPyNamespaceFacts *facts; /* Python; NULL = one complete empty record */
} ts_attach_t;

/* The type_scope one def gets in module p.app. */
static const char *ts_unit_attach_with(ts_unit_t *u, const ts_attach_t *in) {
    ts_facts_t empty;
    cbm_type_scope_t scope = {.reg = u->reg,
                              .module_qn = "p.app",
                              .imp_keys = in->keys,
                              .imp_vals = in->vals,
                              .imp_count = in->n,
                              .file_defs = in->file_defs,
                              .file_def_count = in->file_def_count,
                              .module_facts = in->facts ? in->facts : ts_facts(&empty, NULL, 0)};
    cbm_type_scope_file_t *f = cbm_type_scope_file_begin(cbm_type_scope_lang(in->lang), &scope);
    if (!f) {
        return "<no file state>";
    }
    CBMLSPDef def = {0};
    def.qualified_name = in->def_qn ? in->def_qn : "p.app.f";
    def.short_name = "f";
    def.label = "Function";
    def.return_types = in->ret;
    def.lang = in->lang;
    cbm_type_scope_attach(f, &u->arena, &def);
    cbm_type_scope_file_end(f);
    return def.type_scope;
}

static const char *ts_unit_attach(ts_unit_t *u, CBMLanguage lang, const char *ret,
                                  const char *const *keys, const char *const *vals, int n) {
    ts_attach_t in = {.lang = lang, .ret = ret, .keys = keys, .vals = vals, .n = n};
    return ts_unit_attach_with(u, &in);
}

TEST(type_scope_import_binding_is_the_identity) {
    ts_unit_t u;
    ts_unit_begin(&u);
    /* `from p.a import Store`-shaped (a module-valued binding) and a direct
     * one; the same-named Store of the other module is never chosen. */
    const char *keys[] = {"Store"};
    const char *mod_vals[] = {"p.a"};
    const char *direct_vals[] = {"p.b.Store"};
    ts_facts_t facts;
    ts_attach_t in = {.lang = CBM_LANG_PYTHON,
                      .ret = "Optional[Store]",
                      .keys = keys,
                      .vals = mod_vals,
                      .n = 1,
                      .facts = ts_facts(&facts, keys, 1)};
    ASSERT_STR_EQ(ts_unit_attach_with(&u, &in), "Store=p.a.Store");
    in.vals = direct_vals;
    in.ret = "list[Store]";
    ASSERT_STR_EQ(ts_unit_attach_with(&u, &in), "Store=p.b.Store");
    /* The own module declares Store as well: two bindings of one name. */
    cbm_registry_add(u.reg, "Store", "p.app.Store", "Class", false);
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    ts_unit_end(&u);
    PASS();
}

TEST(type_scope_dotted_spelling_through_its_head) {
    ts_unit_t u;
    ts_unit_begin(&u);
    const char *keys[] = {"a"};
    const char *vals[] = {"p.a"};
    /* Go `*a.Store`, `[]a.Store`, a map value: one identity for `a.Store`. */
    const char *got =
        ts_unit_attach(&u, CBM_LANG_GO, "*a.Store|[]a.Store|map[string]a.Store", keys, vals, 1);
    ASSERT_STR_EQ(got, "a.Store=p.a.Store");
    /* A name the head's module does not declare: nothing. */
    ASSERT_NULL(ts_unit_attach(&u, CBM_LANG_GO, "*a.Missing", keys, vals, 1));
    ts_unit_end(&u);
    PASS();
}

TEST(type_scope_stores_nothing_without_one_answer) {
    ts_unit_t u;
    ts_unit_begin(&u);
    ts_facts_t facts;
    /* Two imports bind Store to different targets: ambiguous. */
    const char *keys2[] = {"Store", "Store"};
    const char *vals2[] = {"p.a.Store", "p.b.Store"};
    const char *one[] = {"Store"};
    ts_attach_t in = {.lang = CBM_LANG_PYTHON,
                      .ret = "Store",
                      .keys = keys2,
                      .vals = vals2,
                      .n = 2,
                      .facts = ts_facts(&facts, one, 1)};
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    /* Not found anywhere in scope. */
    ASSERT_NULL(ts_unit_attach(&u, CBM_LANG_PYTHON, "Nowhere", NULL, NULL, 0));
    /* Bound, but to something no members are looked up on: a variable and a
     * function. */
    const char *rk[] = {"Result", "make"};
    const char *rv[] = {"p.c.Result", "p.c.make"};
    ts_attach_t bound = {.lang = CBM_LANG_PYTHON,
                         .ret = "Result[make]",
                         .keys = rk,
                         .vals = rv,
                         .n = 2,
                         .facts = ts_facts(&facts, rk, 2)};
    ASSERT_NULL(ts_unit_attach_with(&u, &bound));
    ts_unit_end(&u);
    PASS();
}

/* Finding 6: an import that names no project definition (the standard
 * library, a third-party package) is no identity, and the engine keeps its
 * own handling of the spelling (`Self`). */
TEST(type_scope_names_outside_the_project_get_none) {
    ts_unit_t u;
    ts_unit_begin(&u);
    ts_facts_t facts;
    const char *keys[] = {"Self", "logging", "pathlib"};
    /* What the import map makes of them: project-prefixed paths no node has. */
    const char *vals[] = {"p.typing.Self", "p.logging", "p.pathlib"};
    ts_attach_t in = {.lang = CBM_LANG_PYTHON,
                      .ret = "Self",
                      .keys = keys,
                      .vals = vals,
                      .n = 3,
                      .facts = ts_facts(&facts, keys, 3)};
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    in.ret = "logging.Logger";
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    in.ret = "pathlib.Path";
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    ts_unit_end(&u);
    PASS();
}

/* Go imports bind packages: a bare name never resolves through one, so a
 * type parameter (never dotted) named like an import cannot take a type of
 * that package; and an own-module name is left to the engine. */
TEST(type_scope_go_bare_names_and_own_module) {
    ts_unit_t u;
    ts_unit_begin(&u);
    cbm_registry_add(u.reg, "T", "p.tpkg.T", "Struct", false);
    const char *keys[] = {"T"};
    const char *vals[] = {"p.tpkg"};
    ASSERT_NULL(ts_unit_attach(&u, CBM_LANG_GO, "T", keys, vals, 1));
    ASSERT_STR_EQ(ts_unit_attach(&u, CBM_LANG_GO, "*T.T", keys, vals, 1), "T.T=p.tpkg.T");
    ASSERT_NULL(ts_unit_attach(&u, CBM_LANG_GO, "*Local", NULL, NULL, 0));
    ts_unit_end(&u);
    PASS();
}

/* Python's cross registrar keeps bare spellings, so an own-module identity
 * is stored (a forward reference in quotes too). */
TEST(type_scope_python_own_module) {
    ts_unit_t u;
    ts_unit_begin(&u);
    ASSERT_STR_EQ(ts_unit_attach(&u, CBM_LANG_PYTHON, "Optional[\"Local\"]", NULL, NULL, 0),
                  "Local=p.app.Local");
    ts_unit_end(&u);
    PASS();
}

/* Finding 3: a method's annotation is looked up in its class body first. A
 * name the enclosing class declares (a nested class, a class attribute) gets
 * no identity, and a def inside a function none at all. */
TEST(type_scope_python_enclosing_scopes) {
    ts_unit_t u;
    ts_unit_begin(&u);
    cbm_registry_add(u.reg, "Config", "p.app.Config", "Class", false);
    cbm_registry_add(u.reg, "Config", "p.app.Model.Config", "Class", false);
    cbm_registry_add(u.reg, "Alias", "p.app.Alias", "Class", false);
    const CBMDefinition defs[] = {
        {.qualified_name = "p.app.Config", .label = "Class"},
        {.qualified_name = "p.app.Alias", .label = "Class"},
        {.qualified_name = "p.app.Model", .label = "Class"},
        {.qualified_name = "p.app.Model.Config", .label = "Class"},
        {.qualified_name = "p.app.Model.Alias", .label = "Variable"},
        {.qualified_name = "p.app.Model.cfg", .label = "Method"},
        {.qualified_name = "p.app.Model.alias", .label = "Method"},
        {.qualified_name = "p.app.Model.Config.owner", .label = "Method"},
        {.qualified_name = "p.app.outer", .label = "Function"},
        {.qualified_name = "p.app.outer.inner", .label = "Function"},
        {.qualified_name = "p.app.top", .label = "Function"},
    };
    int n = (int)(sizeof(defs) / sizeof(defs[0]));
    ts_attach_t in = {
        .lang = CBM_LANG_PYTHON, .file_defs = defs, .file_def_count = n, .ret = "Config"};
    in.def_qn = "p.app.Model.cfg"; /* Model.Config */
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    in.def_qn = "p.app.Model.alias";
    in.ret = "Alias"; /* the class attribute Model.Alias */
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    in.def_qn = "p.app.outer.inner"; /* inside a function: its locals */
    in.ret = "Config";
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    /* The module scope answers where nothing encloses the name. */
    in.def_qn = "p.app.top";
    ASSERT_STR_EQ(ts_unit_attach_with(&u, &in), "Config=p.app.Config");
    in.def_qn = "p.app.Model.Config.owner"; /* its class declares no `Alias` */
    in.ret = "Alias";
    ASSERT_STR_EQ(ts_unit_attach_with(&u, &in), "Alias=p.app.Alias");
    ts_unit_end(&u);
    PASS();
}

/* Round 3, N2: a name the class body binds without defining it (`Config =
 * Other`, an import, a loop, with or except target) is the class attribute in
 * its methods' annotations, so it gets no identity; a name the body does not
 * bind keeps the module's. */
TEST(type_scope_python_class_body_bindings) {
    ts_unit_t u;
    ts_unit_begin(&u);
    cbm_registry_add(u.reg, "Config", "p.app.Config", "Class", false);
    cbm_registry_add(u.reg, "Keep", "p.app.Keep", "Class", false);
    static const CBMDefTypeFacts model_binds = {.class_bindings = "x|Config|Other"};
    const CBMDefinition defs[] = {
        {.qualified_name = "p.app.Config", .label = "Class"},
        {.qualified_name = "p.app.Keep", .label = "Class"},
        {.qualified_name = "p.app.Model", .label = "Class", .type_facts = &model_binds},
        {.qualified_name = "p.app.Model.cfg", .label = "Method"},
        {.qualified_name = "p.app.top", .label = "Function"},
    };
    ts_attach_t in = {.lang = CBM_LANG_PYTHON,
                      .file_defs = defs,
                      .file_def_count = (int)(sizeof(defs) / sizeof(defs[0])),
                      .def_qn = "p.app.Model.cfg",
                      .ret = "Optional[Config]"};
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    in.ret = "Keep";
    ASSERT_STR_EQ(ts_unit_attach_with(&u, &in), "Keep=p.app.Keep");
    in.def_qn = "p.app.top"; /* outside the class: the module's Config */
    in.ret = "Config";
    ASSERT_STR_EQ(ts_unit_attach_with(&u, &in), "Config=p.app.Config");
    ts_unit_end(&u);
    PASS();
}

/* Type parameters: a PEP 695 parameter of the file (`def f[T]`,
 * `class C[T]`) and a TypeVar the module assigns over an import never take a
 * real type's identity. */
TEST(type_scope_python_type_parameters) {
    ts_unit_t u;
    ts_unit_begin(&u);
    cbm_registry_add(u.reg, "T", "p.app.T", "Class", false);
    cbm_registry_add(u.reg, "Item", "p.a.Item", "Class", false);
    static const CBMDefTypeFacts wrap_params = {.type_params = "T"};
    static const CBMDefTypeFacts box_params = {.type_params = "K|T"};
    const CBMDefinition defs[] = {
        {.qualified_name = "p.app.T", .label = "Class"},
        {.qualified_name = "p.app.wrap", .label = "Function", .type_facts = &wrap_params},
        {.qualified_name = "p.app.Box", .label = "Class", .type_facts = &box_params},
        {.qualified_name = "p.app.Box.get", .label = "Method"},
    };
    ts_attach_t in = {.lang = CBM_LANG_PYTHON,
                      .file_defs = defs,
                      .file_def_count = 4,
                      .def_qn = "p.app.wrap",
                      .ret = "T"};
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    in.def_qn = "p.app.Box.get";
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    /* `from p.a import Item` then `Item = TypeVar("Item")`: bound twice. */
    CBMPyNamespaceFact twice[] = {
        {.kind = CBM_PY_NS_IMPORT_NAME, .local_name = "Item"},
        {.kind = CBM_PY_NS_SHADOW, .local_name = "Item", .reason = CBM_PY_NS_REASON_VALUE}};
    CBMPyNamespaceFacts facts = {.version = CBM_PY_NAMESPACE_FACTS_VERSION,
                                 .language = CBM_LANG_PYTHON,
                                 .status = CBM_PY_NS_COMPLETE,
                                 .items = twice,
                                 .count = 2,
                                 .cap = 2};
    const char *keys[] = {"Item"};
    const char *vals[] = {"p.a.Item"};
    ts_attach_t tv = {.lang = CBM_LANG_PYTHON,
                      .ret = "Item",
                      .keys = keys,
                      .vals = vals,
                      .n = 1,
                      .facts = &facts};
    ASSERT_NULL(ts_unit_attach_with(&u, &tv));
    facts.count = 1; /* the import alone decides */
    ASSERT_STR_EQ(ts_unit_attach_with(&u, &tv), "Item=p.a.Item");
    ts_unit_end(&u);
    PASS();
}

/* A module whose bindings cannot be listed gets no identity: a star import,
 * a parse error, an incomplete record. */
TEST(type_scope_python_unlisted_bindings_get_none) {
    ts_unit_t u;
    ts_unit_begin(&u);
    CBMPyNamespaceFact star[] = {{.kind = CBM_PY_NS_IMPORT_STAR, .module_name = "p.c"}};
    CBMPyNamespaceFacts facts = {.version = CBM_PY_NAMESPACE_FACTS_VERSION,
                                 .language = CBM_LANG_PYTHON,
                                 .status = CBM_PY_NS_COMPLETE,
                                 .items = star,
                                 .count = 1,
                                 .cap = 1};
    ts_attach_t in = {.lang = CBM_LANG_PYTHON, .ret = "Local", .facts = &facts};
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    CBMPyNamespaceFact parse[] = {{.kind = CBM_PY_NS_UNKNOWN, .reason = CBM_PY_NS_REASON_PARSE}};
    facts.items = parse;
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    /* An effect (a call) does not hide bindings from the step. */
    parse[0].reason = CBM_PY_NS_REASON_EFFECT;
    ASSERT_STR_EQ(ts_unit_attach_with(&u, &in), "Local=p.app.Local");
    facts.status = CBM_PY_NS_INCOMPLETE;
    ASSERT_NULL(ts_unit_attach_with(&u, &in));
    ts_unit_end(&u);
    PASS();
}

/* ── The engine table of the cross-file pass ─────────────────────── */

/* Every language that had a cross-file walk has exactly one engine row, and
 * no other language has one; the engines that built a shared registry with
 * the others still do, in the same order, and only they; an engine reads
 * identities exactly where the step has a description of its language. */
TEST(type_scope_engine_table_rows) {
    static const CBMLanguage served[] = {
        CBM_LANG_GO,         CBM_LANG_C,          CBM_LANG_CPP, CBM_LANG_CUDA, CBM_LANG_PYTHON,
        CBM_LANG_JAVASCRIPT, CBM_LANG_TYPESCRIPT, CBM_LANG_TSX, CBM_LANG_PHP,  CBM_LANG_CSHARP,
        CBM_LANG_JAVA,       CBM_LANG_KOTLIN,     CBM_LANG_RUST, CBM_LANG_RUBY};
    int served_count = 0;
    for (int lang = 0; lang < CBM_LANG_COUNT; lang++) {
        bool expect = false;
        for (size_t i = 0; i < sizeof(served) / sizeof(served[0]); i++) {
            expect = expect || served[i] == (CBMLanguage)lang;
        }
        served_count += expect ? 1 : 0;
        ASSERT_EQ(cbm_pxc_has_cross_lsp((CBMLanguage)lang), expect);
        ASSERT_EQ(cbm_pxc_engine_of_lang((CBMLanguage)lang) >= 0, expect);
        ASSERT_EQ(cbm_pxc_wants_identities((CBMLanguage)lang),
                  cbm_type_scope_lang((CBMLanguage)lang) != NULL);
    }
    ASSERT_EQ(served_count, 14);
    ASSERT(cbm_pxc_wants_identities(CBM_LANG_GO));
    ASSERT(cbm_pxc_wants_identities(CBM_LANG_PYTHON));
    /* One row, one registry: C, C++ and CUDA share one, as do JavaScript,
     * TypeScript and TSX; Java and Kotlin do not. */
    int c_row = cbm_pxc_engine_of_lang(CBM_LANG_C);
    int js_row = cbm_pxc_engine_of_lang(CBM_LANG_JAVASCRIPT);
    ASSERT_EQ(cbm_pxc_engine_of_lang(CBM_LANG_CPP), c_row);
    ASSERT_EQ(cbm_pxc_engine_of_lang(CBM_LANG_CUDA), c_row);
    ASSERT_EQ(cbm_pxc_engine_of_lang(CBM_LANG_TYPESCRIPT), js_row);
    ASSERT_EQ(cbm_pxc_engine_of_lang(CBM_LANG_TSX), js_row);
    ASSERT_NEQ(cbm_pxc_engine_of_lang(CBM_LANG_JAVA), cbm_pxc_engine_of_lang(CBM_LANG_KOTLIN));
    ASSERT_LTE(cbm_pxc_engine_count(), CBM_PXC_MAX_ENGINES);
    ASSERT_NULL(cbm_pxc_engine_name(cbm_pxc_engine_count()));
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMCrossLspRegistries regs;
    memset(&regs, 0, sizeof(regs));
    char built[128] = "";
    for (int e = 0; e < cbm_pxc_engine_count(); e++) {
        if (cbm_pxc_build_engine_registry(e, &arena, NULL, 0, &regs)) {
            ASSERT_NOT_NULL(regs.by_engine[e]);
            size_t at = strlen(built);
            snprintf(built + at, sizeof(built) - at, "%s%s", at ? " " : "", cbm_pxc_engine_name(e));
        } else {
            ASSERT_NULL(regs.by_engine[e]);
        }
    }
    ASSERT_STR_EQ(built, "go python c cs ts java ruby");
    ASSERT(cbm_pxc_registry_for_lang(&regs, CBM_LANG_CUDA) == regs.by_engine[c_row]);
    ASSERT(cbm_pxc_registry_for_lang(&regs, CBM_LANG_TSX) == regs.by_engine[js_row]);
    ASSERT_NULL(cbm_pxc_registry_for_lang(&regs, CBM_LANG_RUST)); /* lazy */
    ASSERT_NULL(cbm_pxc_registry_for_lang(&regs, CBM_LANG_KOTLIN));
    ASSERT_NULL(cbm_pxc_registry_for_lang(&regs, CBM_LANG_PHP));
    ASSERT(cbm_pxc_registry_for_lang(&regs, CBM_LANG_RUBY) ==
           regs.by_engine[cbm_pxc_engine_of_lang(CBM_LANG_RUBY)]);
    ASSERT_NOT_NULL(cbm_pxc_registry_for_lang(&regs, CBM_LANG_RUBY));
    ASSERT_NULL(cbm_pxc_registry_for_lang(NULL, CBM_LANG_GO));
    cbm_arena_destroy(&arena);
    PASS();
}

/* ── Row fields for the further engines (hand-made rows) ──────────── */

static const char *const ts_row_labels[] = {"Class", "Interface", NULL};

/* A row of an import-map language whose engine does not qualify its own
 * module (every identity is stored), with the given extras. */
static cbm_type_scope_lang_t ts_row(void) {
    cbm_type_scope_lang_t row = {.lang = CBM_LANG_JAVA,
                                 .scope_source = CBM_TYPE_SCOPE_MODULE_IMPORTS,
                                 .labels = ts_row_labels,
                                 .return_types = true,
                                 .param_types = true,
                                 .field_types = true,
                                 .embedded_types = true};
    return row;
}

/* The type_scope of one def (qn, label, return text) under `row` in module
 * p.app, with the scope's extras (visible) from `extra` when given. */
static const char *ts_row_attach(ts_unit_t *u, const cbm_type_scope_lang_t *row, const char *def_qn,
                                 const char *label, const char *ret, const char *const *keys,
                                 const char *const *vals, int n, const cbm_type_scope_t *extra) {
    cbm_type_scope_t scope = {
        .reg = u->reg, .module_qn = "p.app", .imp_keys = keys, .imp_vals = vals, .imp_count = n};
    if (extra) {
        scope.visible = extra->visible;
        scope.visible_ud = extra->visible_ud;
    }
    cbm_type_scope_file_t *f = cbm_type_scope_file_begin(row, &scope);
    if (!f) {
        return "<no file state>";
    }
    CBMLSPDef def = {0};
    def.qualified_name = def_qn;
    def.short_name = "f";
    def.label = label;
    def.return_types = ret;
    cbm_type_scope_attach(f, &u->arena, &def);
    cbm_type_scope_file_end(f);
    return def.type_scope;
}

/* open_imports: a name the own module and the named imports leave unbound is
 * the one type of that name across the open packages, or nothing. */
TEST(type_scope_open_imports_one_answer_or_none) {
    ts_unit_t u;
    ts_unit_begin(&u);
    cbm_registry_add(u.reg, "Widget", "p.x.Widget", "Class", false);
    cbm_registry_add(u.reg, "Widget", "p.y.Widget", "Class", false);
    cbm_registry_add(u.reg, "Gadget", "p.y.Gadget", "Class", false);
    cbm_registry_add(u.reg, "Local", "p.x.Local", "Class", false);
    cbm_registry_add(u.reg, "Thing", "p.app.Thing", "Function", false);
    cbm_registry_add(u.reg, "Thing", "p.x.Thing", "Class", false);
    cbm_type_scope_lang_t row = ts_row();
    row.open_imports = true;
    const char *keys[] = {"*", "*", "*"};
    const char *vals[] = {"p.x", "p.y", "p.x"};
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.f", "Function", "Gadget", keys, vals, 3, NULL),
                  "Gadget=p.y.Gadget");
    /* Two open packages declare Widget: none. */
    ASSERT_NULL(ts_row_attach(&u, &row, "p.app.f", "Function", "Widget", keys, vals, 3, NULL));
    /* The own module shadows the open packages: its Local, its Thing (a
     * function, no type: none). */
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.f", "Function", "Local", keys, vals, 3, NULL),
                  "Local=p.app.Local");
    ASSERT_NULL(ts_row_attach(&u, &row, "p.app.f", "Function", "Thing", keys, vals, 3, NULL));
    /* A named import shadows them too. */
    const char *nkeys[] = {"*", "*", "Widget"};
    const char *nvals[] = {"p.x", "p.y", "p.x.Widget"};
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.f", "Function", "Widget", nkeys, nvals, 3, NULL),
                  "Widget=p.x.Widget");
    /* Without the flag "*" is an ordinary local name: Gadget is unbound. */
    row.open_imports = false;
    ASSERT_NULL(ts_row_attach(&u, &row, "p.app.f", "Function", "Gadget", keys, vals, 3, NULL));
    ts_unit_end(&u);
    PASS();
}

static bool ts_implicit_string(const char *name, size_t len) {
    return len == 6 && memcmp(name, "String", 6) == 0;
}

/* implicit_external: a name the language's implicit external scope declares
 * (java.lang.String) is no project type of an open package. */
TEST(type_scope_implicit_external_scope_gets_none) {
    ts_unit_t u;
    ts_unit_begin(&u);
    cbm_registry_add(u.reg, "String", "p.y.String", "Class", false);
    cbm_registry_add(u.reg, "Gadget", "p.y.Gadget", "Class", false);
    cbm_type_scope_lang_t row = ts_row();
    row.open_imports = true;
    const char *keys[] = {"*"};
    const char *vals[] = {"p.y"};
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.f", "Function", "String", keys, vals, 1, NULL),
                  "String=p.y.String");
    row.implicit_external = ts_implicit_string;
    ASSERT_NULL(ts_row_attach(&u, &row, "p.app.f", "Function", "String", keys, vals, 1, NULL));
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.f", "Function", "Gadget", keys, vals, 1, NULL),
                  "Gadget=p.y.Gadget");
    /* The own module's String still shadows the implicit one. */
    cbm_registry_add(u.reg, "String", "p.app.String", "Class", false);
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.f", "Function", "String", keys, vals, 1, NULL),
                  "String=p.app.String");
    ts_unit_end(&u);
    PASS();
}

/* nested_types: `Inner` written inside `Outer` is `Outer.Inner`, innermost
 * enclosing type first, before the module. */
TEST(type_scope_nested_types_innermost_first) {
    ts_unit_t u;
    ts_unit_begin(&u);
    cbm_registry_add(u.reg, "Outer", "p.app.Outer", "Class", false);
    cbm_registry_add(u.reg, "Inner", "p.app.Outer.Inner", "Class", false);
    cbm_registry_add(u.reg, "Leaf", "p.app.Outer.Inner.Leaf", "Class", false);
    cbm_registry_add(u.reg, "Deep", "p.app.Outer.Deep", "Class", false);
    cbm_registry_add(u.reg, "Inner", "p.app.Outer.Deep.Inner", "Class", false);
    cbm_registry_add(u.reg, "Inner", "p.app.Inner", "Class", false);
    cbm_registry_add(u.reg, "Gizmo", "p.app.Outer.Gizmo", "Method", false);
    cbm_registry_add(u.reg, "Gizmo", "p.app.Gizmo", "Class", false);
    cbm_type_scope_lang_t row = ts_row();
    row.nested_types = true;
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.Outer.m", "Method", "Inner", NULL, NULL, 0, NULL),
                  "Inner=p.app.Outer.Inner");
    ASSERT_STR_EQ(
        ts_row_attach(&u, &row, "p.app.Outer.Deep.n", "Method", "Inner", NULL, NULL, 0, NULL),
        "Inner=p.app.Outer.Deep.Inner");
    /* A type's own texts are written inside it. */
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.Outer", "Class", "Inner", NULL, NULL, 0, NULL),
                  "Inner=p.app.Outer.Inner");
    ASSERT_STR_EQ(
        ts_row_attach(&u, &row, "p.app.Outer.m", "Method", "Inner.Leaf", NULL, NULL, 0, NULL),
        "Inner.Leaf=p.app.Outer.Inner.Leaf");
    /* At module level, the module's. */
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.f", "Function", "Inner", NULL, NULL, 0, NULL),
                  "Inner=p.app.Inner");
    /* The enclosing type declares Gizmo as a method: the module's Gizmo is
     * hidden, and a method is no type: nothing. */
    ASSERT_NULL(ts_row_attach(&u, &row, "p.app.Outer.m", "Method", "Gizmo", NULL, NULL, 0, NULL));
    /* Without the flag the module answers. */
    row.nested_types = false;
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.Outer.m", "Method", "Inner", NULL, NULL, 0, NULL),
                  "Inner=p.app.Inner");
    ts_unit_end(&u);
    PASS();
}

static const char *ts_visible_widget(void *ud, const char *name, size_t len) {
    (void)ud;
    return len == 6 && memcmp(name, "Widget", 6) == 0 ? "p.h.Widget" : NULL;
}

/* scope_source INCLUDE_CLOSURE: the caller's closure answers the head. */
TEST(type_scope_include_closure_source) {
    ts_unit_t u;
    ts_unit_begin(&u);
    cbm_registry_add(u.reg, "Widget", "p.h.Widget", "Class", false);
    cbm_registry_add(u.reg, "Part", "p.h.Widget.Part", "Class", false);
    cbm_registry_add(u.reg, "Gadget", "p.app.Gadget", "Class", false);
    cbm_type_scope_lang_t row = ts_row();
    row.scope_source = CBM_TYPE_SCOPE_INCLUDE_CLOSURE;
    cbm_type_scope_t extra = {.visible = ts_visible_widget};
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.f", "Function", "Widget *", NULL, NULL, 0, &extra),
                  "Widget=p.h.Widget");
    ASSERT_STR_EQ(
        ts_row_attach(&u, &row, "p.app.f", "Function", "Widget.Part", NULL, NULL, 0, &extra),
        "Widget.Part=p.h.Widget.Part");
    /* Only the closure answers: the own module's Gadget is not visible by
     * module, and without a closure nothing is. */
    ASSERT_NULL(ts_row_attach(&u, &row, "p.app.f", "Function", "Gadget", NULL, NULL, 0, &extra));
    ASSERT_NULL(ts_row_attach(&u, &row, "p.app.f", "Function", "Widget", NULL, NULL, 0, NULL));
    ts_unit_end(&u);
    PASS();
}

/* scope_source LEXICAL_NESTING with `::` paths (Ruby constants): the
 * namespaces enclosing the def, innermost first, then the file's top level;
 * `::X` and a name nothing nests get none. */
TEST(type_scope_lexical_nesting_source) {
    ts_unit_t u;
    ts_unit_begin(&u);
    cbm_registry_add(u.reg, "A", "p.app.A", "Class", false);
    cbm_registry_add(u.reg, "B", "p.app.A.B", "Class", false);
    cbm_registry_add(u.reg, "X", "p.app.A.X", "Class", false);
    cbm_registry_add(u.reg, "X", "p.app.X", "Class", false);
    cbm_registry_add(u.reg, "Y", "p.app.A.B.Y", "Class", false);
    cbm_type_scope_lang_t row = ts_row();
    row.scope_source = CBM_TYPE_SCOPE_LEXICAL_NESTING;
    row.colon_paths = true;
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.A.B.m", "Method", "X", NULL, NULL, 0, NULL),
                  "X=p.app.A.X");
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.A.B.m", "Method", "Y", NULL, NULL, 0, NULL),
                  "Y=p.app.A.B.Y");
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.A.B.m", "Method", "A::X", NULL, NULL, 0, NULL),
                  "A::X=p.app.A.X");
    ASSERT_STR_EQ(ts_row_attach(&u, &row, "p.app.f", "Function", "X", NULL, NULL, 0, NULL),
                  "X=p.app.X");
    ASSERT_NULL(ts_row_attach(&u, &row, "p.app.A.B.m", "Method", "::X", NULL, NULL, 0, NULL));
    ASSERT_NULL(ts_row_attach(&u, &row, "p.app.A.B.m", "Method", "Z", NULL, NULL, 0, NULL));
    /* Outside a definition (no nesting) the row answers nothing. */
    ts_facts_t empty;
    cbm_type_scope_t scope = {
        .reg = u.reg, .module_qn = "p.app", .module_facts = ts_facts(&empty, NULL, 0)};
    cbm_type_scope_file_t *f = cbm_type_scope_file_begin(&row, &scope);
    ASSERT_NOT_NULL(f);
    ASSERT_NULL(cbm_type_scope_resolve(f, "X", 1));
    cbm_type_scope_file_end(f);
    ts_unit_end(&u);
    PASS();
}

/* colon_paths: `ns::T` is one spelling, its QN joined with `.`; without the
 * flag `ns` and `T` are two names. */
TEST(type_scope_colon_paths) {
    ts_unit_t u;
    ts_unit_begin(&u);
    cbm_registry_add(u.reg, "T", "p.app.ns.T", "Class", false);
    cbm_registry_add(u.reg, "vector", "p.app.std.vector", "Class", false);
    cbm_type_scope_lang_t row = ts_row();
    row.colon_paths = true;
    ASSERT_STR_EQ(
        ts_row_attach(&u, &row, "p.app.f", "Function", "std::vector<ns::T>", NULL, NULL, 0, NULL),
        "std::vector=p.app.std.vector|ns::T=p.app.ns.T");
    row.colon_paths = false;
    ASSERT_NULL(ts_row_attach(&u, &row, "p.app.f", "Function", "ns::T", NULL, NULL, 0, NULL));
    ts_unit_end(&u);
    PASS();
}

/* The calling file's own table: each local its named imports bind, with the
 * type identity it resolves to; Go binds packages (none); the open key and
 * a non-type are left out. */
TEST(type_scope_calling_file_imports) {
    ts_unit_t u;
    ts_unit_begin(&u);
    ts_facts_t facts;
    const char *keys[] = {"Store", "make", "Store", "Other", "Local"};
    const char *vals[] = {"p.a.Store", "p.c.make", "p.a.Store", "p.z.Other", "p.app.Local"};
    const char *bound[] = {"Store", "make", "Other", "Local"};
    cbm_type_scope_t scope = {.reg = u.reg,
                              .module_qn = "p.app",
                              .imp_keys = keys,
                              .imp_vals = vals,
                              .imp_count = 5,
                              .module_facts = ts_facts(&facts, bound, 4)};
    cbm_type_scope_file_t *f =
        cbm_type_scope_file_begin(cbm_type_scope_lang(CBM_LANG_PYTHON), &scope);
    ASSERT_NOT_NULL(f);
    const char *const *locals = NULL;
    const char *const *qns = NULL;
    ASSERT_EQ(cbm_type_scope_file_imports(f, &locals, &qns), 2);
    ASSERT_STR_EQ(locals[0], "Store");
    ASSERT_STR_EQ(qns[0], "p.a.Store");
    ASSERT_STR_EQ(locals[1], "Local");
    ASSERT_STR_EQ(qns[1], "p.app.Local");
    /* Built once: the same arrays again. */
    const char *const *again = NULL;
    ASSERT_EQ(cbm_type_scope_file_imports(f, &again, NULL), 2);
    ASSERT(again == locals);
    cbm_type_scope_file_end(f);
    /* A module whose bindings cannot be listed (a star import) has none. */
    facts.items[4] = (CBMPyNamespaceFact){.kind = CBM_PY_NS_IMPORT_STAR};
    facts.facts.count = 5;
    f = cbm_type_scope_file_begin(cbm_type_scope_lang(CBM_LANG_PYTHON), &scope);
    ASSERT_NOT_NULL(f);
    ASSERT_EQ(cbm_type_scope_file_imports(f, &locals, &qns), 0);
    cbm_type_scope_file_end(f);
    const char *gkeys[] = {"a"};
    const char *gvals[] = {"p.a"};
    cbm_type_scope_t go = {
        .reg = u.reg, .module_qn = "p.app", .imp_keys = gkeys, .imp_vals = gvals, .imp_count = 1};
    f = cbm_type_scope_file_begin(cbm_type_scope_lang(CBM_LANG_GO), &go);
    ASSERT_NOT_NULL(f);
    ASSERT_EQ(cbm_type_scope_file_imports(f, &locals, &qns), 0);
    cbm_type_scope_file_end(f);
    ASSERT_EQ(cbm_type_scope_file_imports(NULL, &locals, &qns), 0);
    ASSERT_NULL(locals);
    /* An open row leaves its open key out. */
    cbm_type_scope_lang_t row = ts_row();
    row.open_imports = true;
    const char *okeys[] = {"*", "Store"};
    const char *ovals[] = {"p.a", "p.a.Store"};
    cbm_type_scope_t open = {
        .reg = u.reg, .module_qn = "p.app", .imp_keys = okeys, .imp_vals = ovals, .imp_count = 2};
    f = cbm_type_scope_file_begin(&row, &open);
    ASSERT_NOT_NULL(f);
    ASSERT_EQ(cbm_type_scope_file_imports(f, &locals, &qns), 1);
    ASSERT_STR_EQ(locals[0], "Store");
    cbm_type_scope_file_end(f);
    ts_unit_end(&u);
    PASS();
}

/* A walk reads its calling file's table from the same scope the collector
 * attaches with: none without a context or for an engine that reads no
 * identities. */
TEST(type_scope_calling_file_scope_per_engine) {
    ts_unit_t u;
    ts_unit_begin(&u);
    ts_facts_t facts;
    const char *keys[] = {"Store"};
    const char *vals[] = {"p.a.Store"};
    CBMFileResult *fr = calloc(1, sizeof(*fr));
    ASSERT_NOT_NULL(fr);
    fr->py_namespace = *ts_facts(&facts, keys, 1);
    cbm_pipeline_ctx_t *ctx = calloc(1, sizeof(*ctx));
    ASSERT_NOT_NULL(ctx);
    ctx->registry = u.reg;
    cbm_type_scope_file_t *f = cbm_pxc_file_scope(ctx, CBM_LANG_PYTHON, fr, "p.app", keys, vals, 1);
    ASSERT_NOT_NULL(f);
    const char *const *locals = NULL;
    const char *const *qns = NULL;
    ASSERT_EQ(cbm_type_scope_file_imports(f, &locals, &qns), 1);
    ASSERT_STR_EQ(qns[0], "p.a.Store");
    cbm_type_scope_file_end(f);
    ASSERT_NULL(cbm_pxc_file_scope(NULL, CBM_LANG_PYTHON, fr, "p.app", keys, vals, 1));
    ASSERT_NULL(cbm_pxc_file_scope(ctx, CBM_LANG_CPP, fr, "p.app", keys, vals, 1));
    ASSERT_NULL(cbm_pxc_file_scope(ctx, CBM_LANG_JAVA, fr, "p.app", keys, vals, 1));
    free(ctx);
    free(fr);
    ts_unit_end(&u);
    PASS();
}

/* Counted work of the step for `defs` methods four types deep, each writing
 * `names` member-type names (nested_types), or the same names through
 * `open` open packages (open_imports). */
static uint64_t ts_row_work(int defs, int names, int open, bool nested) {
    ts_unit_t u;
    ts_unit_begin(&u);
    cbm_registry_add(u.reg, "N0", "p.app.N0", "Class", false);
    cbm_registry_add(u.reg, "N1", "p.app.N0.N1", "Class", false);
    cbm_registry_add(u.reg, "N2", "p.app.N0.N1.N2", "Class", false);
    cbm_registry_add(u.reg, "N3", "p.app.N0.N1.N2.N3", "Class", false);
    char buf[64];
    size_t text_cap = (size_t)names * 16 + 1;
    char *text = calloc(1, text_cap);
    const char *keys[16];
    const char *vals[16];
    char owned[16][16];
    open = open < 16 ? open : 16;
    for (int i = 0; i < names; i++) {
        snprintf(buf, sizeof(buf), nested ? "p.app.N0.M%d" : "p.o0.M%d", i);
        cbm_registry_add(u.reg, "M", buf, "Class", false);
        size_t at = strlen(text);
        snprintf(text + at, text_cap - at, "%sM%d", i ? "|" : "", i);
    }
    for (int i = 0; i < open; i++) {
        snprintf(owned[i], sizeof(owned[i]), "p.o%d", i);
        keys[i] = "*";
        vals[i] = owned[i];
    }
    cbm_type_scope_lang_t row = ts_row();
    row.nested_types = nested;
    row.open_imports = !nested;
    cbm_type_scope_t scope = {
        .reg = u.reg, .module_qn = "p.app", .imp_keys = keys, .imp_vals = vals, .imp_count = open};
    cbm_type_scope_file_t *f = cbm_type_scope_file_begin(&row, &scope);
    uint64_t before = cbm_type_scope_test_work();
    for (int d = 0; f && d < defs; d++) {
        snprintf(buf, sizeof(buf), "p.app.N0.N1.N2.N3.m%d", d);
        CBMLSPDef def = {.qualified_name = buf, .short_name = "m", .label = "Method"};
        def.return_types = text;
        cbm_type_scope_attach(f, &u.arena, &def);
    }
    uint64_t work = cbm_type_scope_test_work() - before;
    cbm_type_scope_file_end(f);
    free(text);
    ts_unit_end(&u);
    return work;
}

TEST(type_scope_nested_and_open_work_grow_linearly) {
    struct {
        const char *what;
        uint64_t small;
        uint64_t large;
    } cases[] = {
        {"nested, defs", ts_row_work(500, 40, 0, true), ts_row_work(1000, 40, 0, true)},
        {"nested, names", ts_row_work(50, 400, 0, true), ts_row_work(50, 800, 0, true)},
        {"open, defs", ts_row_work(500, 40, 8, false), ts_row_work(1000, 40, 8, false)},
        {"open, names", ts_row_work(50, 400, 8, false), ts_row_work(50, 800, 8, false)},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        double ratio = (double)cases[i].large / (double)(cases[i].small ? cases[i].small : 1);
        printf("    %s: %llu -> %llu (x%.2f)\n", cases[i].what, (unsigned long long)cases[i].small,
               (unsigned long long)cases[i].large, ratio);
        ASSERT_GT(cases[i].small, 0);
        ASSERT(ratio > 1.7 && ratio < 2.3);
    }
    PASS();
}

/* The fold's rows keep each engine's present case: Go and C fold their
 * structs' fields in declaration order; CUDA files fold none. */
static const char *ts_folded_fields(CBMLanguage lang, const char *rel, const char *src,
                                    const char *owner_label, char *out, size_t cap) {
    out[0] = '\0';
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src), lang, "p", rel, 0, NULL, NULL);
    if (!r) {
        return "<no result>";
    }
    CBMFileResult *cache[] = {r};
    char rel_path[32];
    snprintf(rel_path, sizeof(rel_path), "%s", rel);
    cbm_file_info_t files[] = {{.rel_path = rel_path, .language = lang}};
    char *modules[1] = {NULL};
    int count = 0;
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMLSPDef *defs =
        cbm_pxc_collect_all_defs(NULL, &arena, cache, files, 1, "p", modules, &count, NULL);
    for (int i = 0; defs && i < count; i++) {
        if (defs[i].label && strcmp(defs[i].label, owner_label) == 0) {
            snprintf(out, cap, "%s", defs[i].field_defs ? defs[i].field_defs : "(none)");
            break;
        }
    }
    free(defs);
    free(modules[0]);
    cbm_arena_destroy(&arena);
    cbm_free_result(r);
    return out;
}

TEST(type_scope_field_fold_rows) {
    char got[128];
    ASSERT_STR_EQ(ts_folded_fields(CBM_LANG_GO, "a.go",
                                   "package p\ntype T struct {\n\tA int\n\tB *T\n}\n", "Struct",
                                   got, sizeof(got)),
                  "A:int|B:*T");
    const char *c_src = "struct point { int x; double y; };\n";
    ASSERT_STR_EQ(ts_folded_fields(CBM_LANG_C, "a.c", c_src, "Class", got, sizeof(got)),
                  "x:int|y:double");
    ASSERT_STR_EQ(ts_folded_fields(CBM_LANG_CUDA, "a.cu", c_src, "Class", got, sizeof(got)),
                  "(none)");
    PASS();
}

/* ── Growth: counted work at two sizes ─────────────────────────────── */

static bool ts_linear(uint64_t small, uint64_t large) {
    /* Twice the input: close to twice the work (names grow by a digit). */
    return small > 0 && large * 10 >= small * 18 && large * 10 <= small * 23;
}

/* `defs` defs in one file, each writing its own `a.T<i>` (no memo hit), or
 * one def writing `defs` distinct names. */
static uint64_t ts_step_work(int defs, bool one_def) {
    ts_unit_t u;
    ts_unit_begin(&u);
    char qn[64];
    for (int i = 0; i < defs; i++) {
        snprintf(qn, sizeof(qn), "p.a.T%d", i);
        cbm_registry_add(u.reg, qn + 4, qn, "Struct", false);
    }
    const char *keys[] = {"a"};
    const char *vals[] = {"p.a"};
    cbm_type_scope_t scope = {
        .reg = u.reg, .module_qn = "p.app", .imp_keys = keys, .imp_vals = vals, .imp_count = 1};
    cbm_type_scope_file_t *f = cbm_type_scope_file_begin(cbm_type_scope_lang(CBM_LANG_GO), &scope);
    char *all = (char *)calloc((size_t)defs, 16);
    size_t at = 0;
    uint64_t before = cbm_type_scope_test_work();
    for (int i = 0; i < defs; i++) {
        char text[32];
        snprintf(text, sizeof(text), "*a.T%d", i);
        if (one_def) {
            at += (size_t)snprintf(all + at, 16, "%s%s", at ? "|" : "", text);
            continue;
        }
        CBMLSPDef def = {.qualified_name = "p.app.f", .short_name = "f", .label = "Function"};
        def.return_types = text;
        def.lang = CBM_LANG_GO;
        cbm_type_scope_attach(f, &u.arena, &def);
    }
    if (one_def) {
        CBMLSPDef def = {.qualified_name = "p.app.f", .short_name = "f", .label = "Function"};
        def.return_types = all;
        def.lang = CBM_LANG_GO;
        cbm_type_scope_attach(f, &u.arena, &def);
    }
    uint64_t work = cbm_type_scope_test_work() - before;
    free(all);
    cbm_type_scope_file_end(f);
    ts_unit_end(&u);
    return work;
}

TEST(type_scope_step_work_grows_linearly) {
    uint64_t defs_small = ts_step_work(1000, false);
    uint64_t defs_large = ts_step_work(2000, false);
    uint64_t names_small = ts_step_work(1000, true);
    uint64_t names_large = ts_step_work(2000, true);
    printf("  step work: defs 1000=%llu 2000=%llu, names in one def 1000=%llu 2000=%llu\n",
           (unsigned long long)defs_small, (unsigned long long)defs_large,
           (unsigned long long)names_small, (unsigned long long)names_large);
    ASSERT_TRUE(ts_linear(defs_small, defs_large));
    ASSERT_TRUE(ts_linear(names_small, names_large));
    PASS();
}

/* The engines' lookup: one def with k identities, k lookups. */
static uint64_t ts_lookup_work(int k) {
    CBMArena a;
    cbm_arena_init(&a);
    size_t cap = (size_t)k * 40 + 1;
    char *scope = (char *)calloc(cap, 1);
    size_t at = 0;
    for (int i = 0; i < k; i++) {
        at += (size_t)snprintf(scope + at, cap - at, "%sa.T%d=p.a.T%d", at ? "|" : "", i, i);
    }
    uint64_t before = cbm_type_scope_index_test_work();
    const cbm_type_scope_index_t *ix = cbm_type_scope_index(&a, scope);
    int found = 0;
    for (int i = 0; i < k; i++) {
        char name[32];
        int n = snprintf(name, sizeof(name), "a.T%d", i);
        found += cbm_type_scope_find(ix, name, (size_t)n) != NULL;
    }
    uint64_t work = cbm_type_scope_index_test_work() - before;
    free(scope);
    cbm_arena_destroy(&a);
    return found == k ? work : 0;
}

TEST(type_scope_lookup_work_grows_linearly) {
    uint64_t small = ts_lookup_work(1000);
    uint64_t large = ts_lookup_work(2000);
    printf("  lookup work: 1000 identities=%llu 2000=%llu\n", (unsigned long long)small,
           (unsigned long long)large);
    ASSERT_TRUE(ts_linear(small, large));
    PASS();
}

/* The per-file selection's worklist. A chain (module i's def names module
 * i-1's type) and a fan (n modules' defs all name one module of n defs): the
 * fan walked that module once per identity before. */
static uint64_t ts_reach_work(int n, bool fan) {
    CBMArena a;
    cbm_arena_init(&a);
    int count = fan ? 2 * n : n;
    CBMLSPDef *defs = (CBMLSPDef *)calloc((size_t)count, sizeof(CBMLSPDef));
    for (int i = 0; i < count; i++) {
        CBMLSPDef *d = &defs[i];
        bool hub = fan && i >= n; /* the fan's target module, n defs */
        int mod = hub ? n : i;
        d->def_module_qn = cbm_arena_sprintf(&a, "p.m%d", mod);
        d->qualified_name = cbm_arena_sprintf(&a, "p.m%d.T%d", mod, i);
        d->short_name = "T";
        d->label = hub || !fan ? "Struct" : "Function";
        d->lang = CBM_LANG_GO;
        if (!hub && (fan || i > 0)) {
            int target = fan ? n : i - 1;
            int target_def = fan ? n : i - 1;
            d->type_scope = cbm_arena_sprintf(&a, "x.T=p.m%d.T%d", target, target_def);
        }
    }
    CBMModuleDefIndex *idx = cbm_pxc_build_module_def_index(defs, count);
    char own[32];
    snprintf(own, sizeof(own), "p.m%d", fan ? 0 : n - 1);
    /* The fan's caller imports every holder module. */
    const char **imps = (const char **)calloc((size_t)n, sizeof(char *));
    for (int i = 0; i < n; i++) {
        imps[i] = cbm_arena_sprintf(&a, "p.m%d", i);
    }
    uint64_t before = cbm_pxc_test_reach_work();
    int out = 0;
    bool ok = false;
    CBMLSPDef *sel = cbm_pxc_filter_defs_for_file(idx, defs, CBM_LANG_GO, NULL, own,
                                                  fan ? imps : NULL, fan ? n : 0, &out, &ok);
    uint64_t work = cbm_pxc_test_reach_work() - before;
    free(sel);
    free(imps);
    cbm_pxc_free_module_def_index(idx);
    free(defs);
    cbm_arena_destroy(&a);
    return ok && out == count ? work : 0;
}

TEST(type_scope_reach_work_grows_linearly) {
    uint64_t chain_small = ts_reach_work(1000, false);
    uint64_t chain_large = ts_reach_work(2000, false);
    uint64_t fan_small = ts_reach_work(1000, true);
    uint64_t fan_large = ts_reach_work(2000, true);
    printf("  reach work: chain 1000=%llu 2000=%llu, fan 1000=%llu 2000=%llu\n",
           (unsigned long long)chain_small, (unsigned long long)chain_large,
           (unsigned long long)fan_small, (unsigned long long)fan_large);
    ASSERT_TRUE(ts_linear(chain_small, chain_large));
    ASSERT_TRUE(ts_linear(fan_small, fan_large));
    PASS();
}

/* The planner's identity gate over n stored rows, each holding one identity,
 * after a change of the type one of them names. */
static uint64_t ts_decline_work(int n, const char **reason) {
    cbm_lsp_surface_row_t *rows = (cbm_lsp_surface_row_t *)calloc((size_t)n, sizeof(*rows));
    for (int i = 0; i < n; i++) {
        char *json = (char *)malloc(256);
        snprintf(json, 256,
                 "{\"v\":5,\"lang\":%d,\"lsp\":[{\"qn\":\"p.f%d.New\",\"sn\":\"New\",\"lb\":"
                 "\"Function\",\"lg\":%d,\"ty\":\"store.T%d=p.store.T%d\"}],\"reg\":[],\"py\":null}",
                 (int)CBM_LANG_GO, i, (int)CBM_LANG_GO, i, i);
        rows[i].defs_json = json;
    }
    char before_row[256];
    snprintf(before_row, sizeof(before_row),
             "{\"v\":5,\"lang\":%d,\"lsp\":[{\"qn\":\"p.store.T%d\",\"sn\":\"T%d\",\"lb\":\"Struct\","
             "\"lg\":%d,\"al\":\"Impl\"}],\"reg\":[],\"py\":null}",
             (int)CBM_LANG_GO, n - 1, n - 1, (int)CBM_LANG_GO);
    char after_row[256];
    snprintf(after_row, sizeof(after_row),
             "{\"v\":5,\"lang\":%d,\"lsp\":[{\"qn\":\"p.store.T%d\",\"sn\":\"T%d\",\"lb\":\"Struct\","
             "\"lg\":%d,\"al\":null}],\"reg\":[],\"py\":null}",
             (int)CBM_LANG_GO, n - 1, n - 1, (int)CBM_LANG_GO);
    const char *old_rows[] = {before_row};
    const char *new_rows[] = {after_row};
    uint64_t work = 0;
    *reason = cbm_lsp_surface_identity_decline(old_rows, new_rows, 1, rows, n, &work);
    for (int i = 0; i < n; i++) {
        free((void *)rows[i].defs_json);
    }
    free(rows);
    return work;
}

TEST(type_scope_identity_gate_work_grows_linearly) {
    const char *small_reason = NULL;
    const char *large_reason = NULL;
    uint64_t small = ts_decline_work(1000, &small_reason);
    uint64_t large = ts_decline_work(2000, &large_reason);
    printf("  identity gate work: 1000 rows=%llu 2000=%llu (%s)\n", (unsigned long long)small,
           (unsigned long long)large, large_reason ? large_reason : "kept");
    ASSERT_STR_EQ(small_reason, "identity_target_changed");
    ASSERT_STR_EQ(large_reason, "identity_target_changed");
    ASSERT_TRUE(ts_linear(small, large));
    PASS();
}

/* ── Pipeline fixtures ─────────────────────────────────────────────── */

/* CALLS edges from a node named `src` to the node whose QN ends in
 * `tgt_qn_tail` whose properties contain `fragment` (NULL = any). */
static int ts_calls_to(cbm_store_t *s, const char *project, const char *src,
                       const char *tgt_qn_tail, const char *fragment) {
    cbm_edge_t *edges = NULL;
    int count = 0;
    if (cbm_store_find_edges_by_type(s, project, "CALLS", &edges, &count) != CBM_STORE_OK) {
        return -1;
    }
    int matches = 0;
    size_t tail = strlen(tgt_qn_tail);
    for (int i = 0; i < count; i++) {
        cbm_node_t a = {0};
        cbm_node_t b = {0};
        bool ok = cbm_store_find_node_by_id(s, edges[i].source_id, &a) == CBM_STORE_OK &&
                  cbm_store_find_node_by_id(s, edges[i].target_id, &b) == CBM_STORE_OK;
        size_t qn_len = ok && b.qualified_name ? strlen(b.qualified_name) : 0;
        if (ok && a.name && strcmp(a.name, src) == 0 && qn_len >= tail &&
            strcmp(b.qualified_name + qn_len - tail, tgt_qn_tail) == 0 &&
            (qn_len == tail || b.qualified_name[qn_len - tail - 1] == '.') &&
            (!fragment || (edges[i].properties_json && strstr(edges[i].properties_json, fragment)))) {
            matches++;
        }
        cbm_node_free_fields(&a);
        cbm_node_free_fields(&b);
    }
    if (edges) {
        cbm_store_free_edges(edges, count);
    }
    return matches;
}

#define TS_LSP "\"strategy\":\"lsp_"

typedef struct {
    const char *rel;
    const char *content;
} ts_file_t;

static void ts_write(const char *tmp, const ts_file_t *files) {
    for (int i = 0; files && files[i].rel; i++) {
        th_write_file(TH_PATH(tmp, files[i].rel), files[i].content);
    }
}

/* Filler files that put a fixture above the parallel threshold. */
static void ts_pad(const char *tmp, CBMLanguage lang, int count) {
    for (int i = 0; i < count; i++) {
        char rel[64];
        char body[128];
        if (lang == CBM_LANG_GO) {
            snprintf(rel, sizeof(rel), "fill/f%d.go", i);
            snprintf(body, sizeof(body), "package fill\n\nfunc F%d() int { return %d }\n", i, i);
        } else {
            snprintf(rel, sizeof(rel), "fill/f%d.py", i);
            snprintf(body, sizeof(body), "def f%d():\n    return %d\n", i, i);
        }
        th_write_file(TH_PATH(tmp, rel), body);
    }
}

enum { TS_PAD_FILES = 52 }; /* above the parallel threshold (50 files) */

/* Full index of `tmp` with CBM_WORKERS=workers into tmp/<db_name>. */
static cbm_store_t *ts_index_workers(const char *tmp, const char *db_name, int workers,
                                     char *project, size_t project_size) {
    char *saved = NULL;
    const char *old = getenv("CBM_WORKERS");
    saved = old ? strdup(old) : NULL;
    char w[16];
    snprintf(w, sizeof(w), "%d", workers);
    cbm_setenv("CBM_WORKERS", w, 1);
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/%s", tmp, db_name);
    cbm_pipeline_t *p = cbm_pipeline_new(tmp, db_path, CBM_MODE_FULL);
    int rc = p ? cbm_pipeline_run(p) : -1;
    if (p) {
        snprintf(project, project_size, "%s", cbm_pipeline_project_name(p));
    }
    cbm_pipeline_free(p);
    if (saved) {
        cbm_setenv("CBM_WORKERS", saved, 1);
        free(saved);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    return rc == 0 ? cbm_store_open_path(db_path) : NULL;
}

/* One fixture indexed with one worker and with four (both above the parallel
 * threshold); `check` returns 0 when the graph is right. */
typedef int (*ts_check_fn)(cbm_store_t *s, const char *project, const char *label);

static int ts_twins(const char *name, const ts_file_t *files, CBMLanguage pad_lang,
                    ts_check_fn check) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_tscope_%s_XXXXXX", name);
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }
    ts_write(tmp, files);
    ts_pad(tmp, pad_lang, TS_PAD_FILES);
    int bad = 0;
    for (int run = 0; run < 2 && !bad; run++) {
        int workers = run == 0 ? 1 : 4;
        char project[256] = {0};
        cbm_store_t *s = ts_index_workers(tmp, run == 0 ? "seq.db" : "par.db", workers, project,
                                          sizeof(project));
        if (!s) {
            bad = 1;
            break;
        }
        bad = check(s, project, run == 0 ? "1 worker" : "4 workers");
        cbm_store_close(s);
    }
    th_rmtree(tmp);
    ASSERT_EQ(bad, 0);
    PASS();
}

/* Go: app calls fixture.New() (returns *store.Store) and reads
 * fixture.NewHolder().S (a field of type *store.Store); fixture.go imports
 * store. decoy/ declares a same-named Store with the same method. */
static const ts_file_t go_scope_files[] = {
    {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
    {"store/store.go", "package store\n\ntype Store struct{ n int }\n\n"
                       "func (s *Store) Get(k string) string { return k }\n"},
    {"decoy/store.go", "package decoy\n\ntype Store struct{ n int }\n\n"
                       "func (s *Store) Get(k string) string { return k }\n"},
    {"fixture/fixture.go", "package fixture\n\nimport \"example.com/demo/store\"\n\n"
                           "type Holder struct {\n\tS *store.Store\n}\n\n"
                           "func New() *store.Store { return &store.Store{} }\n\n"
                           "func NewHolder() Holder { return Holder{} }\n"},
    {"app/app.go", "package app\n\nimport \"example.com/demo/fixture\"\n\n"
                   "func Run() string {\n\ts := fixture.New()\n\treturn s.Get(\"x\")\n}\n\n"
                   "func Field() string {\n\th := fixture.NewHolder()\n\treturn h.S.Get(\"y\")\n}\n"},
    {NULL, NULL}};

static int go_scope_check(cbm_store_t *s, const char *project, const char *label) {
    int ret_hit = ts_calls_to(s, project, "Run", "store.Store.Get", TS_LSP);
    int ret_decoy = ts_calls_to(s, project, "Run", "decoy.Store.Get", NULL);
    int field_hit = ts_calls_to(s, project, "Field", "store.Store.Get", TS_LSP);
    int field_decoy = ts_calls_to(s, project, "Field", "decoy.Store.Get", NULL);
    printf("  go (%s): return=%d decoy=%d field=%d decoy=%d\n", label, ret_hit, ret_decoy,
           field_hit, field_decoy);
    return !(ret_hit == 1 && ret_decoy == 0 && field_hit == 1 && field_decoy == 0);
}

TEST(type_scope_go_return_and_field_types) {
    return ts_twins("go", go_scope_files, CBM_LANG_GO, go_scope_check);
}

/* Go: a type parameter named like an import (`import T "…/tpkg"`, then
 * `func Wrap[T any](x T) T`) is the parameter inside Wrap, not tpkg's T. */
static const ts_file_t go_tparam_files[] = {
    {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
    {"tpkg/t.go", "package tpkg\n\ntype T struct{ n int }\n\nfunc (t T) M() string { return \"t\" }\n"},
    {"other/x.go", "package other\n\ntype X struct{ n int }\n\nfunc (x X) M() string { return \"x\" }\n"},
    {"fixture/fixture.go", "package fixture\n\nimport T \"example.com/demo/tpkg\"\n\n"
                           "var Keep T.T\n\nfunc Wrap[T any](x T) T { return x }\n"},
    {"app/app.go", "package app\n\nimport (\n\t\"example.com/demo/fixture\"\n"
                   "\t\"example.com/demo/other\"\n)\n\n"
                   "func Run() string { return fixture.Wrap(other.X{}).M() }\n"},
    {NULL, NULL}};

static int go_tparam_check(cbm_store_t *s, const char *project, const char *label) {
    int wrong = ts_calls_to(s, project, "Run", "tpkg.T.M", NULL);
    printf("  go type parameter (%s): edge to tpkg.T.M=%d\n", label, wrong);
    return wrong != 0;
}

TEST(type_scope_go_type_parameter_is_no_import) {
    return ts_twins("gotp", go_tparam_files, CBM_LANG_GO, go_tparam_check);
}

/* Python: app.py calls fixture.make() (-> Store, imported by fixture.py from
 * pkg.store) and fixture.local() (-> Local, fixture.py's own class). decoy.py
 * declares a same-named Store with the same method. */
static const ts_file_t py_scope_files[] = {
    {"pkg/__init__.py", ""},
    {"pkg/store.py", "class Store:\n    def get(self, k):\n        return k\n"},
    {"pkg/decoy.py", "class Store:\n    def get(self, k):\n        return k\n"},
    {"pkg/fixture.py", "from pkg.store import Store\n\n\n"
                       "class Local:\n    def ping(self):\n        return 1\n\n\n"
                       "def make() -> Store:\n    return Store()\n\n\n"
                       "def local() -> Local:\n    return Local()\n"},
    {"pkg/app.py", "from pkg.fixture import make, local\n\n\n"
                   "def run():\n    s = make()\n    return s.get(\"x\")\n\n\n"
                   "def own():\n    v = local()\n    return v.ping()\n"},
    {NULL, NULL}};

static int py_scope_check(cbm_store_t *s, const char *project, const char *label) {
    int ret_hit = ts_calls_to(s, project, "run", "pkg.store.Store.get", TS_LSP);
    int ret_decoy = ts_calls_to(s, project, "run", "pkg.decoy.Store.get", NULL);
    int own_hit = ts_calls_to(s, project, "own", "pkg.fixture.Local.ping", TS_LSP);
    printf("  python (%s): return=%d decoy=%d own=%d\n", label, ret_hit, ret_decoy, own_hit);
    return !(ret_hit == 1 && ret_decoy == 0 && own_hit == 1);
}

TEST(type_scope_python_return_types) {
    return ts_twins("py", py_scope_files, CBM_LANG_PYTHON, py_scope_check);
}

/* Finding 3: `def cfg(self) -> Config` in a class with a nested `Config`
 * names Model.Config, never the module-level Config. */
static const ts_file_t py_nested_files[] = {
    {"pkg/__init__.py", ""},
    {"pkg/mod.py", "class Config:\n    def load(self):\n        return 1\n\n\n"
                   "class Model:\n"
                   "    class Config:\n        def load(self):\n            return 2\n\n"
                   "    def cfg(self) -> Config:\n        return Model.Config()\n"},
    {"pkg/use.py", "from pkg.mod import Model\n\n\n"
                   "def run():\n    m = Model()\n    return m.cfg().load()\n"},
    {NULL, NULL}};

static int py_nested_check(cbm_store_t *s, const char *project, const char *label) {
    int wrong = ts_calls_to(s, project, "run", "pkg.mod.Config.load", NULL);
    printf("  python nested class (%s): edge to module-level Config.load=%d\n", label, wrong);
    return wrong != 0;
}

TEST(type_scope_python_nested_class_is_not_the_module_class) {
    return ts_twins("pynest", py_nested_files, CBM_LANG_PYTHON, py_nested_check);
}

/* Round 3, N2 (the review's py-classattr): a class body that binds a name
 * without defining it -- `Config = OtherConfig`, an import, an annotated
 * assignment, a for, with or except target, a walrus -- makes that name the
 * class attribute in its methods' annotations (Python: get_type_hints gives
 * OtherConfig, Other, None, 1...). None of them may become the module-level
 * class of that name; a name the body does not bind (Keep) still does. */
static const ts_file_t py_classbind_files[] = {
    {"pkg/__init__.py", ""},
    {"pkg/other.py", "class OtherConfig:\n    def load(self):\n        return 'other'\n\n\n"
                     "class Other:\n    def load(self):\n        return 'other'\n"},
    {"pkg/mod.py",
     "import contextlib\nfrom pkg.other import Other, OtherConfig\n\n\n"
     "class Config:\n    def load(self):\n        return 'module'\n\n\n"
     "class A:\n    def load(self):\n        return 'A'\n\n\n"
     "class B:\n    def load(self):\n        return 'B'\n\n\n"
     "class C:\n    def load(self):\n        return 'C'\n\n\n"
     "class D:\n    def load(self):\n        return 'D'\n\n\n"
     "class F:\n    def load(self):\n        return 'F'\n\n\n"
     "class Keep:\n    def load(self):\n        return 'keep'\n\n\n"
     "class Model:\n"
     "    Config = OtherConfig\n"
     "    Keep: type\n" /* an annotation alone binds nothing: Keep stays the module's */
     "    from pkg.other import Other as A\n"
     "    B: type = Other\n"
     "    for C in (1,):\n        pass\n"
     "    with contextlib.nullcontext() as D:\n        pass\n"
     "    (F := 1)\n\n"
     "    def cfg(self) -> Config:\n        return OtherConfig()\n\n"
     "    def ma(self) -> A:\n        return Other()\n\n"
     "    def mb(self) -> B:\n        return Other()\n\n"
     "    def mc(self) -> C:\n        return None\n\n"
     "    def md(self) -> D:\n        return None\n\n"
     "    def mf(self) -> F:\n        return None\n\n"
     "    def keep(self) -> Keep:\n        return Keep()\n"},
    {"pkg/use.py", "from pkg.mod import Model\n\n\n"
                   "def run():\n    return Model().cfg().load()\n\n\n"
                   "def run_a():\n    return Model().ma().load()\n\n\n"
                   "def run_b():\n    return Model().mb().load()\n\n\n"
                   "def run_c():\n    return Model().mc().load()\n\n\n"
                   "def run_d():\n    return Model().md().load()\n\n\n"
                   "def run_f():\n    return Model().mf().load()\n\n\n"
                   "def run_keep():\n    return Model().keep().load()\n"},
    {NULL, NULL}};

static int py_classbind_check(cbm_store_t *s, const char *project, const char *label) {
    int wrong = ts_calls_to(s, project, "run", "pkg.mod.Config.load", NULL) +
                ts_calls_to(s, project, "run_a", "pkg.mod.A.load", NULL) +
                ts_calls_to(s, project, "run_b", "pkg.mod.B.load", NULL) +
                ts_calls_to(s, project, "run_c", "pkg.mod.C.load", NULL) +
                ts_calls_to(s, project, "run_d", "pkg.mod.D.load", NULL) +
                ts_calls_to(s, project, "run_f", "pkg.mod.F.load", NULL);
    int keep = ts_calls_to(s, project, "run_keep", "pkg.mod.Keep.load", NULL);
    printf("  python class-body bindings (%s): module-class edges=%d, keep=%d\n", label, wrong,
           keep);
    return !(wrong == 0 && keep == 1);
}

TEST(type_scope_python_class_body_bindings_shadow_the_module) {
    return ts_twins("pybind", py_classbind_files, CBM_LANG_PYTHON, py_classbind_check);
}

/* Round 3 (a class and a module of one QN): pkg/a/__init__.py defines class
 * Thing beside the module pkg/a/Thing.py; both share the QN pkg.a.Thing and
 * their members `run` share pkg.a.Thing.run, so no member edge through an
 * identity Thing=pkg.a.Thing is that class's (the graph keeps the module's
 * nodes). Python: `from pkg.a import Thing` is the class while nothing imports
 * the submodule, and the submodule once something does. The partner: a class
 * of the same __init__ with no module of its QN (Other) is reached through
 * the same kind of identity. */
static const ts_file_t py_classmod_files[] = {
    {"pkg/__init__.py", ""},
    {"pkg/a/__init__.py", "class Thing:\n    def run(self):\n        return 'class'\n\n\n"
                          "class Other:\n    def run(self):\n        return 'other'\n"},
    {"pkg/a/Thing.py", "def run():\n    return 'module'\n"},
    {"pkg/b.py", "from pkg.a import Other, Thing\n\n\ndef make() -> Thing:\n    return Thing()\n\n\n"
                 "def make_other() -> Other:\n    return Other()\n"},
    {"pkg/c.py", "from pkg.b import make, make_other\n\n\ndef go():\n    return make().run()\n\n\n"
                 "def go_other():\n    return make_other().run()\n"},
    {NULL, NULL}};

static int py_classmod_check(cbm_store_t *s, const char *project, const char *label) {
    int member = ts_calls_to(s, project, "go", "pkg.a.Thing.run", NULL);
    int other = ts_calls_to(s, project, "go_other", "pkg.a.Other.run", NULL);
    printf("  python class and module of one QN (%s): go -> Thing.run=%d, go_other -> Other.run=%d\n",
           label, member, other);
    return !(member == 0 && other == 1);
}

TEST(type_scope_python_class_and_module_of_one_qn) {
    return ts_twins("pyclassmod", py_classmod_files, CBM_LANG_PYTHON, py_classmod_check);
}

/* `global R` in a class body (run at import) or a function body that binds R
 * rebinds the module's R, so `def make_r() -> R` after it names whatever that
 * body bound (python3: pkg.b.R for the class body; for the function, whether
 * it ran). No identity is taken from the import. A module that only imports R
 * keeps it (run_a), and so does one whose function declares `global R` and
 * only reads it (run_p). */
static const ts_file_t py_global_files[] = {
    {"pkg/__init__.py", ""},
    {"pkg/a.py", "class R:\n    def save(self):\n        return 'a'\n"},
    {"pkg/b.py", "class R:\n    def save(self):\n        return 'b'\n"},
    {"pkg/mod.py", "from pkg.a import R\n\n\nclass Rebind:\n    global R\n    from pkg.b import R\n\n\n"
                   "def make_r() -> R:\n    return R()\n"},
    {"pkg/mod2.py", "from pkg.a import R\n\n\ndef setup():\n    global R\n    from pkg.b import R\n\n\n"
                    "def make_s() -> R:\n    return R()\n"},
    {"pkg/mod3.py", "from pkg.a import R\n\n\ndef make_a() -> R:\n    return R()\n"},
    {"pkg/mod4.py", "from pkg.a import R\n\n\ndef peek():\n    global R\n    return R.__name__\n\n\n"
                    "def make_p() -> R:\n    return R()\n"},
    {"pkg/use.py", "from pkg.mod import make_r\nfrom pkg.mod2 import make_s\nfrom pkg.mod3 import make_a\n"
                   "from pkg.mod4 import make_p\n\n\n"
                   "def run_r():\n    return make_r().save()\n\n\n"
                   "def run_s():\n    return make_s().save()\n\n\n"
                   "def run_a():\n    return make_a().save()\n\n\n"
                   "def run_p():\n    return make_p().save()\n"},
    {NULL, NULL}};

static int py_global_check(cbm_store_t *s, const char *project, const char *label) {
    int wrong = ts_calls_to(s, project, "run_r", "pkg.a.R.save", NULL) +
                ts_calls_to(s, project, "run_s", "pkg.a.R.save", NULL);
    int kept = ts_calls_to(s, project, "run_a", "pkg.a.R.save", NULL);
    int read = ts_calls_to(s, project, "run_p", "pkg.a.R.save", NULL);
    printf("  python global rebinding (%s): rebound edges=%d, plain import=%d, global read=%d\n",
           label, wrong, kept, read);
    return !(wrong == 0 && kept == 1 && read == 1);
}

TEST(type_scope_python_global_rebinding_takes_no_identity) {
    return ts_twins("pyglobal", py_global_files, CBM_LANG_PYTHON, py_global_check);
}

/* `except ValueError as E` unbinds E when the clause
 * ends, and `D = Other; del D` leaves D unbound: methods defined after them
 * name the module's E and D (python3 agrees). A `del` after a method that saw
 * the binding (`D2 = Other`, `def md2(self) -> D2`, `del D2`) keeps the
 * abstention: evaluated at definition time (Python before 3.14) the annotation
 * is Other, evaluated lazily (3.14 on) the module's D2 -- no one answer. */
static const ts_file_t py_unbind_files[] = {
    {"pkg/__init__.py", ""},
    {"pkg/mod.py", "class Other:\n    def load(self):\n        return 'other'\n\n\n"
                   "class D:\n    def load(self):\n        return 'D'\n\n\n"
                   "class E:\n    def load(self):\n        return 'E'\n\n\n"
                   "class D2:\n    def load(self):\n        return 'D2'\n\n\n"
                   "class Model:\n"
                   "    try:\n        raise ValueError()\n    except ValueError as E:\n        pass\n"
                   "    D = Other\n    del D\n    D2 = Other\n\n"
                   "    def md2(self) -> D2:\n        return D2()\n\n"
                   "    del D2\n\n"
                   "    def me(self) -> E:\n        return E()\n\n"
                   "    def md(self) -> D:\n        return D()\n"},
    {"pkg/use.py", "from pkg.mod import Model\n\n\n"
                   "def run_e():\n    return Model().me().load()\n\n\n"
                   "def run_d():\n    return Model().md().load()\n\n\n"
                   "def run_d2():\n    return Model().md2().load()\n"},
    {NULL, NULL}};

static int py_unbind_check(cbm_store_t *s, const char *project, const char *label) {
    int e = ts_calls_to(s, project, "run_e", "pkg.mod.E.load", NULL);
    int d = ts_calls_to(s, project, "run_d", "pkg.mod.D.load", NULL);
    int d2 = ts_calls_to(s, project, "run_d2", "pkg.mod.D2.load", NULL) +
             ts_calls_to(s, project, "run_d2", "pkg.mod.Other.load", NULL);
    printf("  python class-body unbinding (%s): E=%d D=%d, D2 (bound at definition)=%d\n", label, e,
           d, d2);
    return !(e == 1 && d == 1 && d2 == 0);
}

TEST(type_scope_python_unbound_class_names_are_the_module_names) {
    return ts_twins("pyunbind", py_unbind_files, CBM_LANG_PYTHON, py_unbind_check);
}

/* A method declared with @overload (the shape of scrapy's Response.replace).
 * Callers see the overloads, which pick the result from the arguments
 * (`replace(cls=Text)` is a Text, python3 runs Text.css); the
 * implementation's `-> Response` is no call's type, so no identity is taken
 * from any of its signatures. A plain method keeps its own (run_plain), and so
 * does an overloaded one whose definitions all return Response (run_opts, the
 * shape of sqlalchemy's Connection.execution_options). */
static const ts_file_t py_overload_files[] = {
    {"pkg/__init__.py", ""},
    {"pkg/resp.py", "from __future__ import annotations\n\nfrom typing import Self, TypeVar, overload\n\n"
                    "T = TypeVar(\"T\", bound=\"Response\")\n\n\n"
                    "class Response:\n    def css(self):\n        return 'base'\n\n"
                    "    @overload\n    def replace(self, cls: type[T]) -> T: ...\n\n"
                    "    @overload\n    def replace(self, cls: None = None) -> Self: ...\n\n"
                    "    def replace(self, cls=None) -> Response:\n"
                    "        return (cls or type(self))()\n\n"
                    "    @overload\n    def opts(self, *, a: int) -> Response: ...\n\n"
                    "    @overload\n    def opts(self, **kw: int) -> Response: ...\n\n"
                    "    def opts(self, **kw) -> Response:\n        return Response()\n\n"
                    "    def plain(self) -> Response:\n        return Response()\n\n\n"
                    "class Text(Response):\n    def css(self):\n        return 'text'\n"},
    {"pkg/make.py", "from pkg.resp import Response\n\n\ndef make() -> Response:\n    return Response()\n"},
    {"pkg/use.py", "from pkg.make import make\nfrom pkg.resp import Text\n\n\n"
                   "def run():\n    return make().replace(cls=Text).css()\n\n\n"
                   "def run_plain():\n    return make().plain().css()\n\n\n"
                   "def run_opts():\n    return make().opts(a=1).css()\n"},
    {NULL, NULL}};

static int py_overload_check(cbm_store_t *s, const char *project, const char *label) {
    int wrong = ts_calls_to(s, project, "run", "pkg.resp.Response.css", NULL);
    int plain = ts_calls_to(s, project, "run_plain", "pkg.resp.Response.css", NULL);
    int agreed = ts_calls_to(s, project, "run_opts", "pkg.resp.Response.css", NULL);
    printf("  python overloaded method (%s): run -> Response.css=%d, run_plain=%d, run_opts=%d\n",
           label, wrong, plain, agreed);
    return !(wrong == 0 && plain == 1 && agreed == 1);
}

TEST(type_scope_python_overloaded_method_takes_no_identity) {
    return ts_twins("pyoverload", py_overload_files, CBM_LANG_PYTHON, py_overload_check);
}

/* The same through an import under another name (`from typing import
 * overload as ovl`, `@ovl`): the name is read through the module's imports
 * (python3: `replace(cls=AliasText)` runs AliasText.css). A method of the
 * same class under a plain decorator keeps its identity (run_kept, python3
 * Alias.css). */
static const ts_file_t py_overload_alias_files[] = {
    {"pkg/__init__.py", ""},
    {"pkg/resp.py", "from __future__ import annotations\n\nfrom typing import Self, TypeVar\n"
                    "from typing import overload as ovl\n\n"
                    "T = TypeVar(\"T\", bound=\"Alias\")\n\n\n"
                    "def keep(fn):\n    return fn\n\n\n"
                    "class Alias:\n    def css(self):\n        return 'alias'\n\n"
                    "    @ovl\n    def replace(self, cls: type[T]) -> T: ...\n\n"
                    "    @ovl\n    def replace(self, cls: None = None) -> Self: ...\n\n"
                    "    def replace(self, cls=None) -> Alias:\n"
                    "        return (cls or type(self))()\n\n"
                    "    @keep\n    def kept(self) -> Alias:\n        return Alias()\n\n\n"
                    "class AliasText(Alias):\n    def css(self):\n        return 'text'\n"},
    {"pkg/make.py", "from pkg.resp import Alias\n\n\ndef make() -> Alias:\n    return Alias()\n"},
    {"pkg/use.py", "from pkg.make import make\nfrom pkg.resp import AliasText\n\n\n"
                   "def run_alias():\n    return make().replace(cls=AliasText).css()\n\n\n"
                   "def run_kept():\n    return make().kept().css()\n"},
    {NULL, NULL}};

static int py_overload_alias_check(cbm_store_t *s, const char *project, const char *label) {
    int wrong = ts_calls_to(s, project, "run_alias", "pkg.resp.Alias.css", NULL);
    int kept = ts_calls_to(s, project, "run_kept", "pkg.resp.Alias.css", NULL);
    printf("  python overload under another name (%s): run_alias -> Alias.css=%d, run_kept=%d\n",
           label, wrong, kept);
    return !(wrong == 0 && kept == 1);
}

TEST(type_scope_python_overload_imported_under_another_name) {
    return ts_twins("pyovlalias", py_overload_alias_files, CBM_LANG_PYTHON,
                    py_overload_alias_check);
}

/* A struct's embedded-type text is built once, with one buffer: the
 * extractor's work for n embedded types is linear in n. */
static uint64_t ts_go_embedded_work(int n) {
    size_t cap = (size_t)n * 24 + 256;
    char *src = (char *)calloc(cap, 1);
    if (!src) {
        return 0;
    }
    size_t at = (size_t)snprintf(src, cap,
                                 "package p\n\nimport \"example.com/demo/parts\"\n\ntype S struct {\n");
    for (int i = 0; i < n; i++) {
        at += (size_t)snprintf(src + at, cap - at, "\tparts.E%d\n", i);
    }
    at += (size_t)snprintf(src + at, cap - at, "}\n");
    uint64_t before = cbm_extract_test_go_embedded_work();
    CBMFileResult *r =
        cbm_extract_file(src, (int)at, CBM_LANG_GO, "proj", "p/s.go", 0, NULL, NULL);
    uint64_t work = cbm_extract_test_go_embedded_work() - before;
    /* the whole list is there: n spellings, the last one included */
    char last[32];
    snprintf(last, sizeof(last), "parts.E%d", n - 1);
    bool whole = false;
    for (int i = 0; r && i < r->defs.count; i++) {
        const CBMDefTypeFacts *f = r->defs.items[i].type_facts;
        whole = whole || (f && f->go_embedded && strstr(f->go_embedded, last));
    }
    cbm_free_result(r);
    free(src);
    return whole ? work : 0;
}

TEST(type_scope_go_embedded_text_work_grows_linearly) {
    uint64_t small = ts_go_embedded_work(2000);
    uint64_t mid = ts_go_embedded_work(4000);
    uint64_t large = ts_go_embedded_work(8000);
    printf("  go embedded text work: 2000 embeds=%llu 4000=%llu 8000=%llu\n",
           (unsigned long long)small, (unsigned long long)mid, (unsigned long long)large);
    ASSERT_TRUE(ts_linear(small, mid));
    ASSERT_TRUE(ts_linear(mid, large));
    PASS();
}

/* Finding 6: `-> Self` chains across files keep main's edges; `Self` comes
 * from typing, which the project does not define. A decoy declares the same
 * method names. */
static const ts_file_t py_self_files[] = {
    {"pkg/__init__.py", ""},
    {"pkg/builder.py", "from typing import Self\n\n\n"
                       "class Builder:\n"
                       "    def a(self) -> Self:\n        return self\n\n"
                       "    def b(self) -> Self:\n        return self\n\n"
                       "    def done(self) -> int:\n        return 1\n"},
    {"pkg/app.py", "from pkg.builder import Builder\n\n\n"
                   "def run():\n    x = Builder()\n    return x.a().b().done()\n"},
    {"pkg/decoy.py", "class Other:\n"
                     "    def a(self):\n        return self\n\n"
                     "    def b(self):\n        return self\n\n"
                     "    def done(self):\n        return 2\n"},
    {NULL, NULL}};

static int py_self_check(cbm_store_t *s, const char *project, const char *label) {
    int a = ts_calls_to(s, project, "run", "pkg.builder.Builder.a", NULL);
    int b = ts_calls_to(s, project, "run", "pkg.builder.Builder.b", NULL);
    int done = ts_calls_to(s, project, "run", "pkg.builder.Builder.done", NULL);
    int decoy = ts_calls_to(s, project, "run", "pkg.decoy.Other.done", NULL) +
                ts_calls_to(s, project, "run", "pkg.decoy.Other.b", NULL);
    printf("  python Self chain (%s): a=%d b=%d done=%d decoy=%d\n", label, a, b, done, decoy);
    return !(a == 1 && b == 1 && done == 1 && decoy == 0);
}

TEST(type_scope_python_self_chain_keeps_its_edges) {
    return ts_twins("pyself", py_self_files, CBM_LANG_PYTHON, py_self_check);
}

/* Type parameters: `def wrap[Item](x: Item) -> Item` in a file that also
 * imports a class Item; the caller passes an Other. */
static const ts_file_t py_tparam_files[] = {
    {"pkg/__init__.py", ""},
    {"pkg/item.py", "class Item:\n    def save(self):\n        return 1\n"},
    {"pkg/other.py", "class Other:\n    def save(self):\n        return 2\n"},
    {"pkg/box.py", "from pkg.item import Item\n\n\n"
                   "def touch(i: Item) -> None:\n    i.save()\n\n\n"
                   "def wrap[Item](x: Item) -> Item:\n    return x\n"},
    {"pkg/app.py", "from pkg.box import wrap\nfrom pkg.other import Other\n\n\n"
                   "def go():\n    o = wrap(Other())\n    return o.save()\n"},
    {NULL, NULL}};

static int py_tparam_check(cbm_store_t *s, const char *project, const char *label) {
    int wrong = ts_calls_to(s, project, "go", "pkg.item.Item.save", NULL);
    printf("  python type parameter (%s): edge to pkg.item.Item.save=%d\n", label, wrong);
    return wrong != 0;
}

TEST(type_scope_python_type_parameter_is_no_import) {
    return ts_twins("pytp", py_tparam_files, CBM_LANG_PYTHON, py_tparam_check);
}

/* ── Go registrars: fields ────────────────────────────────────────── */

/* Resolve `source` (module test.main or `module_qn`) against `defs` with the
 * per-file registrar or the shared one; the number of resolved calls to
 * `callee`. */
static int ts_go_registry_hits(bool shared, const char *source, const char *module_qn,
                               CBMLSPDef *defs, int def_count, const char **imp_names,
                               const char **imp_qns, int imp_count, const char *callee) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    if (shared) {
        CBMTypeRegistry *reg = cbm_go_build_cross_registry(&arena, defs, def_count);
        if (reg) {
            cbm_run_go_lsp_cross_with_registry(&arena, source, (int)strlen(source), module_qn,
                                               NULL, reg, imp_names, imp_qns, imp_count, NULL,
                                               &out);
        }
    } else {
        cbm_run_go_lsp_cross(&arena, source, (int)strlen(source), module_qn, NULL, defs,
                             def_count, imp_names, imp_qns, imp_count, NULL, &out);
    }
    int hits = 0;
    for (int i = 0; i < out.count; i++) {
        if (out.items[i].callee_qn && strcmp(out.items[i].callee_qn, callee) == 0) {
            hits++;
        }
    }
    cbm_arena_destroy(&arena);
    return hits;
}

/* A method of Configuration registered before the struct makes a receiver
 * placeholder first; after finalize a type lookup answers with the last
 * entry of that name, so the struct's fields must sit on its own entry. They
 * sat on the placeholder, and every field chain through such a struct died
 * (helm's `cfg.Releases.Get`). The field type also comes through an identity
 * (`store.Storage`). */
static int go_registry_field_case(bool shared) {
    const char *source = "package main\n\n"
                         "import \"p/cfg\"\n\n"
                         "func run(c *cfg.Configuration) string {\n"
                         "\treturn c.Releases.Get(\"x\")\n}\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.run",
         .short_name = "run",
         .label = "Function",
         .def_module_qn = "test.main",
         .lang = CBM_LANG_GO},
        {.qualified_name = "p.cfg.Configuration.hook",
         .short_name = "hook",
         .label = "Method",
         .def_module_qn = "p.cfg",
         .receiver_type = "p.cfg.Configuration",
         .lang = CBM_LANG_GO},
        {.qualified_name = "p.cfg.Configuration",
         .short_name = "Configuration",
         .label = "Struct",
         .def_module_qn = "p.cfg",
         .field_defs = "Releases:*store.Storage",
         .type_scope = "store.Storage=p.store.Storage",
         .lang = CBM_LANG_GO},
        {.qualified_name = "p.store.Storage",
         .short_name = "Storage",
         .label = "Struct",
         .def_module_qn = "p.store",
         .lang = CBM_LANG_GO},
        {.qualified_name = "p.store.Storage.Get",
         .short_name = "Get",
         .label = "Method",
         .def_module_qn = "p.store",
         .receiver_type = "p.store.Storage",
         .return_types = "string",
         .lang = CBM_LANG_GO},
    };
    const char *imp_names[] = {"cfg"};
    const char *imp_qns[] = {"p.cfg"};
    int hits = ts_go_registry_hits(shared, source, "test.main", defs, 5, imp_names, imp_qns, 1,
                                   "p.store.Storage.Get");
    printf("  go registry (%s): run -> Storage.Get resolved %d\n", shared ? "shared" : "per-file",
           hits);
    ASSERT_EQ(hits, 1);
    PASS();
}

TEST(type_scope_go_fields_survive_an_earlier_receiver_per_file) {
    return go_registry_field_case(false);
}

TEST(type_scope_go_fields_survive_an_earlier_receiver_shared) {
    return go_registry_field_case(true);
}

/* `package svc` and `package svc_test` in one directory both declare a
 * struct S (one QN): which S a lookup answers with depends on file order, so
 * neither's fields are registered, and the answer is the same in both
 * orders. */
static int go_shared_qn_case(bool shared) {
    const char *source = "package svc_test\n\n"
                         "func run(s *S) string { return s.X.Get() }\n";
    CBMLSPDef real_s = {.qualified_name = "p.svc.S",
                        .short_name = "S",
                        .label = "Struct",
                        .def_module_qn = "p.svc",
                        .field_defs = "X:*real.Store",
                        .type_scope = "real.Store=p.real.Store",
                        .lang = CBM_LANG_GO};
    CBMLSPDef fake_s = real_s;
    fake_s.field_defs = "X:*fake.Store";
    fake_s.type_scope = "fake.Store=p.fake.Store";
    CBMLSPDef rest[] = {
        {.qualified_name = "p.real.Store", .short_name = "Store", .label = "Struct",
         .def_module_qn = "p.real", .lang = CBM_LANG_GO},
        {.qualified_name = "p.real.Store.Get", .short_name = "Get", .label = "Method",
         .def_module_qn = "p.real", .receiver_type = "p.real.Store", .return_types = "string",
         .lang = CBM_LANG_GO},
        {.qualified_name = "p.fake.Store", .short_name = "Store", .label = "Struct",
         .def_module_qn = "p.fake", .lang = CBM_LANG_GO},
        {.qualified_name = "p.fake.Store.Get", .short_name = "Get", .label = "Method",
         .def_module_qn = "p.fake", .receiver_type = "p.fake.Store", .return_types = "string",
         .lang = CBM_LANG_GO},
    };
    int hits[2][2];
    for (int order = 0; order < 2; order++) {
        CBMLSPDef defs[6];
        defs[0] = order ? fake_s : real_s;
        defs[1] = order ? real_s : fake_s;
        memcpy(defs + 2, rest, sizeof(rest));
        hits[order][0] = ts_go_registry_hits(shared, source, "p.svc", defs, 6, NULL, NULL, 0,
                                             "p.real.Store.Get");
        hits[order][1] = ts_go_registry_hits(shared, source, "p.svc", defs, 6, NULL, NULL, 0,
                                             "p.fake.Store.Get");
    }
    printf("  go shared QN (%s): order A real=%d fake=%d, order B real=%d fake=%d\n",
           shared ? "shared" : "per-file", hits[0][0], hits[0][1], hits[1][0], hits[1][1]);
    ASSERT_EQ(hits[0][0] + hits[0][1] + hits[1][0] + hits[1][1], 0);
    /* Build variants of one struct (prometheus' labels.Builder): the same
     * fields with the same identities give one answer in every order, so
     * the fields stay. */
    CBMLSPDef variants[6] = {real_s, real_s};
    memcpy(variants + 2, rest, sizeof(rest));
    int same = ts_go_registry_hits(shared, source, "p.svc", variants, 6, NULL, NULL, 0,
                                   "p.real.Store.Get");
    /* Variants that differ elsewhere (prometheus' labels.Builder: one adds
     * fields) keep the field they declare alike, in both orders. */
    CBMLSPDef wider = real_s;
    wider.field_defs = "X:*real.Store|Extra:int";
    int partial[2];
    for (int order = 0; order < 2; order++) {
        CBMLSPDef defs[6] = {order ? wider : real_s, order ? real_s : wider};
        memcpy(defs + 2, rest, sizeof(rest));
        partial[order] = ts_go_registry_hits(shared, source, "p.svc", defs, 6, NULL, NULL, 0,
                                             "p.real.Store.Get");
    }
    /* The same text naming another type in each (`store` is real in one,
     * fake in the other): no agreement. */
    CBMLSPDef via_real = real_s;
    via_real.field_defs = "X:*store.Store";
    via_real.type_scope = "store.Store=p.real.Store";
    CBMLSPDef via_fake = via_real;
    via_fake.type_scope = "store.Store=p.fake.Store";
    CBMLSPDef split[6] = {via_real, via_fake};
    memcpy(split + 2, rest, sizeof(rest));
    int split_hits = ts_go_registry_hits(shared, source, "p.svc", split, 6, NULL, NULL, 0,
                                         "p.real.Store.Get") +
                     ts_go_registry_hits(shared, source, "p.svc", split, 6, NULL, NULL, 0,
                                         "p.fake.Store.Get");
    printf("  go shared QN (%s): same fields real=%d, agreed field %d/%d, same text other "
           "identity %d\n",
           shared ? "shared" : "per-file", same, partial[0], partial[1], split_hits);
    ASSERT_EQ(same, 1);
    ASSERT_EQ(partial[0], 1);
    ASSERT_EQ(partial[1], 1);
    ASSERT_EQ(split_hits, 0);
    PASS();
}

TEST(type_scope_go_shared_type_qn_keeps_no_fields_per_file) {
    return go_shared_qn_case(false);
}

TEST(type_scope_go_shared_type_qn_keeps_no_fields_shared) {
    return go_shared_qn_case(true);
}

/* ── Go: closure parameters ───────────────────────────────────────── */

/* A closure parameter is typed wherever the literal stands, here as a
 * composite literal's field value (a test table), not only where the literal
 * is evaluated. decoy/ declares the same Store with the same method. */
static const ts_file_t go_closure_files[] = {
    {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
    {"store/store.go", "package store\n\ntype Store struct{ n int }\n\n"
                       "func (s *Store) Get(k string) string { return k }\n"},
    {"decoy/store.go", "package decoy\n\ntype Store struct{ n int }\n\n"
                       "func (s *Store) Get(k string) string { return k }\n"},
    {"app/app.go", "package app\n\nimport \"example.com/demo/store\"\n\n"
                   "type stub struct {\n\trun func(s *store.Store) string\n}\n\n"
                   "func Closure() string {\n"
                   "\ttt := []stub{{run: func(s *store.Store) string { return s.Get(\"x\") }}}\n"
                   "\treturn tt[0].run(nil)\n}\n"},
    {NULL, NULL}};

static int go_closure_check(cbm_store_t *s, const char *project, const char *label) {
    int closure = ts_calls_to(s, project, "Closure", "store.Store.Get", TS_LSP);
    int decoy = ts_calls_to(s, project, "Closure", "decoy.Store.Get", NULL);
    printf("  go closure (%s): closure=%d decoy=%d\n", label, closure, decoy);
    return !(closure == 1 && decoy == 0);
}

TEST(type_scope_go_closure_parameters_in_composite_literals) {
    return ts_twins("goclos", go_closure_files, CBM_LANG_GO, go_closure_check);
}

/* ── Go: package-level values ─────────────────────────────────────── */

/* How many Variable nodes named `name` carry a `return_type` property: a
 * value's type crosses files through the LSP defs only, never as a graph
 * property (Variable nodes carry none on any other language). */
static int ts_variable_return_type_props(cbm_store_t *s, const char *project, const char *name) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_name(s, project, name, &nodes, &count) != CBM_STORE_OK) {
        return -1;
    }
    int hits = 0;
    for (int i = 0; i < count; i++) {
        if (nodes[i].label && strcmp(nodes[i].label, "Variable") == 0 && nodes[i].properties_json &&
            strstr(nodes[i].properties_json, "\"return_type\"")) {
            hits++;
        }
    }
    cbm_store_free_nodes(nodes, count);
    return hits;
}

/* Package-level values of another package typed by their declaration or
 * initializer (`&T{}`, `T{}`, `new(T)`, a typed const and the const block's
 * implicit repetition). decoy/ declares the same names. */
#define TS_GO_VALUES_STORE                                                                         \
    "type Store struct{ n int }\n\nfunc (s *Store) Get(k string) string { return k }\n\n"         \
    "type Kind string\n\nfunc (k Kind) String() string { return string(k) }\n"
static const ts_file_t go_values_files[] = {
    {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
    {"store/store.go", "package store\n\n" TS_GO_VALUES_STORE},
    {"decoy/store.go", "package decoy\n\n" TS_GO_VALUES_STORE},
    {"conf/conf.go", "package conf\n\nimport \"example.com/demo/store\"\n\n"
                     "var Default = &store.Store{}\n\nvar Plain = store.Store{}\n\n"
                     "var Fresh = new(store.Store)\n\nconst Primary store.Kind = \"primary\"\n\n"
                     "const (\n\tFirst store.Kind = \"a\"\n\tSecond\n)\n"},
    {"app/app.go", "package app\n\nimport \"example.com/demo/conf\"\n\n"
                   "func Pointer() string { return conf.Default.Get(\"a\") }\n\n"
                   "func Value() string { return conf.Plain.Get(\"b\") }\n\n"
                   "func Fresh() string { return conf.Fresh.Get(\"c\") }\n\n"
                   "func Typed() string { return conf.Primary.String() }\n\n"
                   "func Repeated() string { return conf.Second.String() }\n"},
    {NULL, NULL}};

static int go_values_check(cbm_store_t *s, const char *project, const char *label) {
    int pointer = ts_calls_to(s, project, "Pointer", "store.Store.Get", TS_LSP);
    int value = ts_calls_to(s, project, "Value", "store.Store.Get", TS_LSP);
    int fresh = ts_calls_to(s, project, "Fresh", "store.Store.Get", TS_LSP);
    int typed = ts_calls_to(s, project, "Typed", "store.Kind.String", TS_LSP);
    int repeated = ts_calls_to(s, project, "Repeated", "store.Kind.String", TS_LSP);
    int decoy = ts_calls_to(s, project, "Pointer", "decoy.Store.Get", NULL) +
                ts_calls_to(s, project, "Value", "decoy.Store.Get", NULL) +
                ts_calls_to(s, project, "Fresh", "decoy.Store.Get", NULL) +
                ts_calls_to(s, project, "Typed", "decoy.Kind.String", NULL) +
                ts_calls_to(s, project, "Repeated", "decoy.Kind.String", NULL);
    /* A value is no callee: no CALLS edge reaches a Variable node. */
    int to_value = ts_calls_to(s, project, "Pointer", "conf.Default", NULL) +
                   ts_calls_to(s, project, "Value", "conf.Plain", NULL);
    int props = ts_variable_return_type_props(s, project, "Default") +
                ts_variable_return_type_props(s, project, "Primary");
    printf("  go values (%s): pointer=%d value=%d new=%d typed=%d repeated=%d decoy=%d "
           "to_value=%d return_type_props=%d\n",
           label, pointer, value, fresh, typed, repeated, decoy, to_value, props);
    return !(pointer == 1 && value == 1 && fresh == 1 && typed == 1 && repeated == 1 &&
             decoy == 0 && to_value == 0 && props == 0);
}

TEST(type_scope_go_package_values) {
    return ts_twins("govals", go_values_files, CBM_LANG_GO, go_values_check);
}

/* Finding 4: `package svc` and `package svc_test` in one directory declare
 * a value of one name (svc.go: real, a_test.go: fake); b_test.go uses the
 * fake one, use.go the real one. Their values share a directory QN, so
 * neither is typed, in either file order; and a function value called
 * through `conf.Hook()` keeps main's edge (the value is no call target of
 * the LSP). */
static int go_test_package_case(bool tests_last) {
    const char *a_test = tests_last ? "svc/za_test.go" : "svc/a_test.go";
    const char *b_test = tests_last ? "svc/zb_test.go" : "svc/b_test.go";
    const ts_file_t files[] = {
        {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
        {"conf/conf.go", "package conf\n\nfunc impl() string { return \"x\" }\n\n"
                         "var Hook func() string = impl\n"},
        {"app/app.go", "package app\n\nimport \"example.com/demo/conf\"\n\n"
                       "func Run() string { return conf.Hook() }\n"},
        {"real/real.go",
         "package real\n\ntype Store struct{ n int }\n\nfunc (s *Store) Get() string { return \"r\" }\n"},
        {"fake/fake.go",
         "package fake\n\ntype Store struct{ n int }\n\nfunc (s *Store) Get() string { return \"f\" }\n"},
        {"svc/svc.go", "package svc\n\nimport \"example.com/demo/real\"\n\n"
                       "var defaultStore = &real.Store{}\n\n"
                       "func Keep() *real.Store { return defaultStore }\n"},
        {"svc/use.go", "package svc\n\nfunc Use() string { return defaultStore.Get() }\n"},
        {a_test, "package svc_test\n\nimport \"example.com/demo/fake\"\n\n"
                 "var defaultStore = &fake.Store{}\n"},
        {b_test, "package svc_test\n\nfunc helper() string { return defaultStore.Get() }\n"},
        {NULL, NULL}};
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_tscope_gotest_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("tmpdir");
    }
    ts_write(tmp, files);
    ts_pad(tmp, CBM_LANG_GO, TS_PAD_FILES);
    int bad = 0;
    for (int run = 0; run < 2; run++) {
        char project[256] = {0};
        cbm_store_t *s = ts_index_workers(tmp, run ? "par.db" : "seq.db", run ? 4 : 1, project,
                                          sizeof(project));
        if (!s) {
            bad++;
            continue;
        }
        int helper_real = ts_calls_to(s, project, "helper", "real.Store.Get", NULL);
        int helper_fake = ts_calls_to(s, project, "helper", "fake.Store.Get", NULL);
        int use_fake = ts_calls_to(s, project, "Use", "fake.Store.Get", NULL);
        int use_real = ts_calls_to(s, project, "Use", "real.Store.Get", NULL);
        int hook = ts_calls_to(s, project, "Run", "conf.Hook", NULL);
        int hook_main = ts_calls_to(s, project, "Run", "conf.Hook", "\"strategy\":\"import_map\"");
        printf("  go test package (%s, tests %s): helper real=%d fake=%d, Use real=%d fake=%d, "
               "Run->conf.Hook=%d (import_map %d)\n",
               run ? "4 workers" : "1 worker", tests_last ? "last" : "first", helper_real,
               helper_fake, use_real, use_fake, hook, hook_main);
        bad += helper_real + helper_fake + use_fake + use_real != 0 || hook != 1 || hook_main != 1;
        cbm_store_close(s);
    }
    th_rmtree(tmp);
    ASSERT_EQ(bad, 0);
    PASS();
}

TEST(type_scope_go_test_package_values_share_no_name) {
    ASSERT_EQ(go_test_package_case(false), 0);
    ASSERT_EQ(go_test_package_case(true), 0);
    PASS();
}

/* Build variants of one value (`//go:build linux` and `!linux` files that
 * both declare `var Default = &store.Store{}`) give one answer, so the value
 * stays typed. */
static const ts_file_t go_variant_value_files[] = {
    {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
    {"store/store.go", "package store\n\ntype Store struct{ n int }\n\n"
                       "func (s *Store) Get(k string) string { return k }\n"},
    {"conf/conf_linux.go", "//go:build linux\n\npackage conf\n\nimport \"example.com/demo/store\"\n\n"
                           "var Default = &store.Store{}\n"},
    {"conf/conf_other.go", "//go:build !linux\n\npackage conf\n\nimport \"example.com/demo/store\"\n\n"
                           "var Default = &store.Store{}\n"},
    {"app/app.go", "package app\n\nimport \"example.com/demo/conf\"\n\n"
                   "func Pointer() string { return conf.Default.Get(\"a\") }\n"},
    {NULL, NULL}};

static int go_variant_value_check(cbm_store_t *s, const char *project, const char *label) {
    int hit = ts_calls_to(s, project, "Pointer", "store.Store.Get", TS_LSP);
    printf("  go variant values (%s): pointer=%d\n", label, hit);
    return hit != 1;
}

TEST(type_scope_go_build_variant_values_stay_typed) {
    return ts_twins("govar", go_variant_value_files, CBM_LANG_GO, go_variant_value_check);
}

/* ── Go: embedded-member promotion ────────────────────────────────── */

/* A struct of another file embeds `base.Base` (an imported package's type)
 * and `*Local` (its own package's); the promoted methods are found through
 * the declaring file's scope. decoy/ declares the same Base with the same
 * method. */
static const ts_file_t go_embed_files[] = {
    {"go.mod", "module example.com/emb\n\ngo 1.21\n"},
    {"base/base.go", "package base\n\ntype Base struct{ n int }\n\n"
                     "func (b *Base) Hello() string { return \"x\" }\n"},
    {"decoy/base.go", "package decoy\n\ntype Base struct{ n int }\n\n"
                      "func (b *Base) Hello() string { return \"y\" }\n"},
    {"outer/outer.go", "package outer\n\nimport \"example.com/emb/base\"\n\n"
                       "type Outer struct {\n\tbase.Base\n\tname string\n}\n\n"
                       "type Local struct{ k int }\n\nfunc (l *Local) Bye() string { return \"z\" }\n\n"
                       "type Both struct {\n\t*Local\n}\n"},
    {"app/app.go", "package app\n\nimport \"example.com/emb/outer\"\n\n"
                   "func Run(o *outer.Outer) string { return o.Hello() }\n\n"
                   "func Run2(b *outer.Both) string { return b.Bye() }\n"},
    {NULL, NULL}};

static int go_embed_check(cbm_store_t *s, const char *project, const char *label) {
    int imported = ts_calls_to(s, project, "Run", "base.Base.Hello", TS_LSP);
    int decoy = ts_calls_to(s, project, "Run", "decoy.Base.Hello", NULL);
    int own = ts_calls_to(s, project, "Run2", "outer.Local.Bye", TS_LSP);
    printf("  go embedded (%s): imported=%d decoy=%d own=%d\n", label, imported, decoy, own);
    return !(imported == 1 && decoy == 0 && own == 1);
}

TEST(type_scope_go_embedded_members_promote) {
    return ts_twins("goemb", go_embed_files, CBM_LANG_GO, go_embed_check);
}

/* Finding 5: Go selects the member at the shallowest depth, and a tie there
 * is no selection. Across files (the review's go-embed: Outer embeds base.A,
 * which embeds Inner with Hello, and base.B with Hello: B.Hello at depth 1
 * wins over Inner.Hello at depth 2), in one file the same, a tie of two
 * embedded types at one depth, and an outer field of the member's name. */
static const ts_file_t go_depth_files[] = {
    {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
    {"base/base.go", "package base\n\ntype Inner struct{ n int }\n\n"
                     "func (i *Inner) Hello() string { return \"inner\" }\n\n"
                     "type A struct{ Inner }\n\n"
                     "type B struct{ k int }\n\n"
                     "func (b *B) Hello() string { return \"b\" }\n\n"
                     "type C struct{ k int }\n\n"
                     "func (c *C) Hello() string { return \"c\" }\n"},
    {"outer/outer.go", "package outer\n\nimport \"example.com/demo/base\"\n\n"
                       "type Outer struct {\n\tbase.A\n\tbase.B\n}\n\n"
                       "type Tie struct {\n\tbase.B\n\tbase.C\n}\n\n"
                       "type Shadow struct {\n\tHello func() string\n\tbase.B\n}\n"},
    {"app/app.go", "package app\n\nimport \"example.com/demo/outer\"\n\n"
                   "func Run(o *outer.Outer) string { return o.Hello() }\n\n"
                   "func Tied(t *outer.Tie) string { return t.Hello() }\n\n"
                   "func Field(s *outer.Shadow) string { return s.Hello() }\n"},
    {"local/local.go", "package local\n\ntype Inner struct{ n int }\n\n"
                       "func (i *Inner) Hello() string { return \"inner\" }\n\n"
                       "type A struct{ Inner }\n\n"
                       "type B struct{ k int }\n\n"
                       "func (b *B) Hello() string { return \"b\" }\n\n"
                       "type Outer struct {\n\tA\n\tB\n}\n\n"
                       "func Here(o *Outer) string { return o.Hello() }\n"},
    {NULL, NULL}};

static int go_depth_check(cbm_store_t *s, const char *project, const char *label) {
    int shallow = ts_calls_to(s, project, "Run", "base.B.Hello", NULL);
    int deep = ts_calls_to(s, project, "Run", "base.Inner.Hello", NULL);
    int tied = ts_calls_to(s, project, "Tied", "base.B.Hello", NULL) +
               ts_calls_to(s, project, "Tied", "base.C.Hello", NULL);
    int shadowed = ts_calls_to(s, project, "Field", "base.B.Hello", NULL);
    int here_shallow = ts_calls_to(s, project, "Here", "local.B.Hello", NULL);
    int here_deep = ts_calls_to(s, project, "Here", "local.Inner.Hello", NULL);
    printf("  go promotion depth (%s): Run B=%d Inner=%d, tie=%d, field=%d, one file B=%d "
           "Inner=%d\n",
           label, shallow, deep, tied, shadowed, here_shallow, here_deep);
    return !(deep == 0 && shallow <= 1 && tied == 0 && shadowed == 0 && here_deep == 0 &&
             here_shallow <= 1);
}

TEST(type_scope_go_promotion_takes_the_shallowest_depth) {
    return ts_twins("godepth", go_depth_files, CBM_LANG_GO, go_depth_check);
}

/* Round 3, N1: the unqualified type name of an embedded field is a field of
 * its depth (Go spec). srv.Server{ Config; base.Handler } with Handler's own
 * field `Config *base.Config`: s.Config is srv.Config at depth 0, in another
 * package and in one of srv's files (main's right edge there stays); an
 * embedded func type `Logger` shadows base.W's method Logger one level down;
 * the same in one file; an embedded type the project does not have
 * (sync.Mutex) still shadows a deeper `Mutex`; and build variants of one
 * struct, one of which declares a field `Item` the other does not, select no
 * `Item` at all: `v.Item` is base.Item on a linux build and Deep's Item2
 * elsewhere (`go run` on darwin: item2), so no one edge is right. Each "no
 * edge" case has a positive partner: a promoted method of Outer (Ping), the
 * explicit path to the deeper Mutex (Lock2), a field both variants declare
 * alike (UseSame); Here's edge is the embedded Cfg's. `go run` agrees. */
static const ts_file_t go_embfield_files[] = {
    {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
    {"base/base.go", "package base\n\ntype Config struct{ n int }\n\n"
                     "func (c *Config) Validate() string { return \"base\" }\n\n"
                     "type Handler struct {\n\tConfig *Config\n}\n\n"
                     "type W struct{ n int }\n\nfunc (w W) Logger(s string) string { return s }\n\n"
                     "func (w W) Ping() string { return \"p\" }\n\n"
                     "type M struct{ n int }\n\nfunc (m *M) Lock() {}\n\n"
                     "type Locker struct {\n\tMutex *M\n}\n\n"
                     "type Item struct{ n int }\n\nfunc (i *Item) Do() string { return \"item\" }\n\n"
                     "type Item2 struct{ n int }\n\nfunc (i *Item2) Do() string { return \"item2\" }\n\n"
                     "type Deep struct {\n\tItem *Item2\n}\n"},
    {"srv/config.go", "package srv\n\ntype Config struct{ port int }\n\n"
                      "func (c Config) Validate() string { return \"srv\" }\n"},
    {"srv/server.go", "package srv\n\nimport (\n\t\"sync\"\n\n\t\"example.com/demo/base\"\n)\n\n"
                      "type Server struct {\n\tConfig\n\tbase.Handler\n}\n\n"
                      "type Logger func(string) string\n\n"
                      "type Outer struct {\n\tLogger\n\tbase.W\n}\n\n"
                      "type U struct {\n\tsync.Mutex\n\tbase.Locker\n}\n"},
    {"srv/local.go", "package srv\n\nfunc Local(s *Server) string { return s.Config.Validate() }\n"},
    {"srv/v_linux.go", "//go:build linux\n\npackage srv\n\nimport \"example.com/demo/base\"\n\n"
                       "type V struct {\n\tItem *base.Item\n\tSame *base.Item\n\tbase.Deep\n}\n"},
    {"srv/v_other.go", "//go:build !linux\n\npackage srv\n\nimport \"example.com/demo/base\"\n\n"
                       "type V struct {\n\tSame *base.Item\n\tbase.Deep\n}\n"},
    {"app/app.go", "package app\n\nimport \"example.com/demo/srv\"\n\n"
                   "func Run(s *srv.Server) string { return s.Config.Validate() }\n\n"
                   "func Log(o srv.Outer) string { return o.Logger(\"x\") }\n\n"
                   "func Lock(u *srv.U) { u.Mutex.Lock() }\n\n"
                   "func Use(v *srv.V) string { return v.Item.Do() }\n\n"
                   "func Ping(o srv.Outer) string { return o.Ping() }\n\n"
                   "func Lock2(u *srv.U) { u.Locker.Mutex.Lock() }\n\n"
                   "func UseSame(v *srv.V) string { return v.Same.Do() }\n"},
    {"one/one.go", "package one\n\ntype Cfg struct{ n int }\n\nfunc (c Cfg) Check() string { return \"cfg\" }\n\n"
                   "type Other struct{ n int }\n\nfunc (o Other) Check() string { return \"other\" }\n\n"
                   "type H struct {\n\tCfg Other\n}\n\n"
                   "type S struct {\n\tCfg\n\tH\n}\n\n"
                   "func Here(s *S) string { return s.Cfg.Check() }\n"},
    {NULL, NULL}};

static int go_embfield_check(cbm_store_t *s, const char *project, const char *label) {
    int run_right = ts_calls_to(s, project, "Run", "srv.Config.Validate", NULL);
    int local_right = ts_calls_to(s, project, "Local", "srv.Config.Validate", NULL);
    int here_right = ts_calls_to(s, project, "Here", "one.Cfg.Check", NULL);
    int ping = ts_calls_to(s, project, "Ping", "base.W.Ping", NULL);
    int lock2 = ts_calls_to(s, project, "Lock2", "base.M.Lock", NULL);
    int same = ts_calls_to(s, project, "UseSame", "base.Item.Do", NULL);
    int wrong = ts_calls_to(s, project, "Run", "base.Config.Validate", NULL) +
                ts_calls_to(s, project, "Local", "base.Config.Validate", NULL) +
                ts_calls_to(s, project, "Log", "base.W.Logger", NULL) +
                ts_calls_to(s, project, "Lock", "base.M.Lock", NULL) +
                ts_calls_to(s, project, "Here", "one.Other.Check", NULL);
    /* the answer of a build: Item on linux, Item2 elsewhere */
    int by_build = ts_calls_to(s, project, "Use", "base.Item2.Do", NULL) +
                   ts_calls_to(s, project, "Use", "base.Item.Do", NULL);
    printf("  go embedded field names (%s): Run=%d Local=%d Here=%d Ping=%d Lock2=%d UseSame=%d, "
           "wrong=%d, by build=%d\n",
           label, run_right, local_right, here_right, ping, lock2, same, wrong, by_build);
    return !(run_right == 1 && local_right == 1 && here_right == 1 && ping == 1 && lock2 == 1 &&
             same == 1 && wrong == 0 && by_build == 0);
}

TEST(type_scope_go_embedded_field_names_are_fields) {
    return ts_twins("goembfield", go_embfield_files, CBM_LANG_GO, go_embfield_check);
}

/* Round 3, N3: a type the function declares itself -- a local `type T` and a
 * type parameter, of a function, of a generic receiver, of a generic type --
 * is never the package's T, whose embedded p1.E would promote Hello: not in
 * the walker (Local, Generic, Direct: main's own false `Direct -> m.T.Own`
 * goes too) and not through another package's signatures (Call through a
 * generic result, Boxed through a generic method's result). Pkg uses the
 * package's T and keeps its promoted edge. `go run` agrees on each. */
static const ts_file_t go_shadowtype_files[] = {
    {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
    {"p1/e.go", "package p1\n\ntype E struct{}\n\nfunc (E) Hello() string { return \"p1\" }\n"},
    {"p2/e.go", "package p2\n\ntype E struct{}\n\nfunc (E) Hello() string { return \"p2\" }\n\n"
                "func (E) Own() string { return \"p2own\" }\n"},
    {"m/t.go", "package m\n\nimport \"example.com/demo/p1\"\n\ntype T struct{ p1.E }\n\n"
               "func (T) Own() string { return \"m\" }\n"},
    {"m/use.go", "package m\n\nimport \"example.com/demo/p2\"\n\n"
                 "func Local() string {\n\ttype T struct{ p2.E }\n\tvar t T\n\treturn t.Hello()\n}\n\n"
                 "func Generic[T interface{ Hello() string }](x T) string { return x.Hello() }\n\n"
                 "func Direct[T interface{ Own() string }](x T) string { return x.Own() }\n\n"
                 "func Pkg() string {\n\tvar t T\n\treturn t.Hello()\n}\n\n"
                 "func First[T any](xs []T) T { return xs[0] }\n\n"
                 "type Box[T any] struct{ V T }\n\n"
                 "func (b Box[T]) Get() T { return b.V }\n"},
    {"app/app.go", "package app\n\nimport (\n\t\"example.com/demo/m\"\n\t\"example.com/demo/p2\"\n)\n\n"
                   "func Call() string { return m.First([]p2.E{{}}).Hello() }\n\n"
                   "func Boxed() string { return m.Box[p2.E]{}.Get().Hello() }\n\n"
                   "func Field(b m.Box[p2.E]) string { return b.V.Hello() }\n"},
    {NULL, NULL}};

static int go_shadowtype_check(cbm_store_t *s, const char *project, const char *label) {
    int wrong = ts_calls_to(s, project, "Local", "p1.E.Hello", NULL) +
                ts_calls_to(s, project, "Generic", "p1.E.Hello", NULL) +
                ts_calls_to(s, project, "Direct", "m.T.Own", NULL) +
                ts_calls_to(s, project, "Call", "p1.E.Hello", NULL) +
                ts_calls_to(s, project, "Boxed", "p1.E.Hello", NULL) +
                ts_calls_to(s, project, "Field", "p1.E.Hello", NULL);
    int pkg = ts_calls_to(s, project, "Pkg", "p1.E.Hello", NULL);
    /* the signatures are read: the calls themselves resolve */
    int direct = ts_calls_to(s, project, "Call", "m.First", NULL) +
                 ts_calls_to(s, project, "Boxed", "m.Box.Get", NULL);
    printf("  go function-declared types (%s): wrong=%d, package T Pkg=%d, First/Get=%d\n", label,
           wrong, pkg, direct);
    return !(wrong == 0 && pkg == 1 && direct == 2);
}

TEST(type_scope_go_function_declared_types_are_not_the_package_types) {
    return ts_twins("goshadow", go_shadowtype_files, CBM_LANG_GO, go_shadowtype_check);
}

/* A local type declared in an inner block or in a
 * closure shadows the package's type only there (Go's block scope): after the
 * `if` block and outside the closure, `T` is the package's again, whose
 * embedded p1.E promotes Hello (`go run`: Block p1, Clo p2p1). Inside the
 * closure the local T (embedding p2.E) gets no package edge. */
static const ts_file_t go_blocktype_files[] = {
    {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
    {"p1/e.go", "package p1\n\ntype E struct{}\n\nfunc (E) Hello() string { return \"p1\" }\n"},
    {"p2/e.go", "package p2\n\ntype E struct{}\n\nfunc (E) Hello() string { return \"p2\" }\n"},
    {"m/t.go", "package m\n\nimport \"example.com/demo/p1\"\n\ntype T struct{ p1.E }\n"},
    {"m/use.go", "package m\n\nimport \"example.com/demo/p2\"\n\n"
                 "func Block(c bool) string {\n\tif c {\n\t\ttype T struct{ p2.E }\n\t\tvar t T\n"
                 "\t\t_ = t\n\t}\n\tvar t T\n\treturn t.Hello()\n}\n\n"
                 "func Clo() string {\n\tf := func() string {\n\t\ttype T struct{ p2.E }\n"
                 "\t\tvar t T\n\t\treturn t.Hello()\n\t}\n\tvar t T\n\treturn f() + t.Hello()\n}\n"},
    {NULL, NULL}};

static int go_blocktype_check(cbm_store_t *s, const char *project, const char *label) {
    int block = ts_calls_to(s, project, "Block", "p1.E.Hello", NULL);
    int clo = ts_calls_to(s, project, "Clo", "p1.E.Hello", NULL);
    printf("  go block-local types (%s): Block p1=%d, Clo p1=%d\n", label, block, clo);
    return !(block == 1 && clo == 1);
}

TEST(type_scope_go_local_types_shadow_only_their_block) {
    return ts_twins("goblock", go_blocktype_files, CBM_LANG_GO, go_blocktype_check);
}

/* Round 3, the 63-field limit beside N4: a struct of 66 named fields whose
 * last two are typed. Every route registers every field now. */
static const ts_file_t go_wide_files[] = {
    {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
    {"kind/kind.go", "package kind\n\ntype K struct{}\n\nfunc (K) Name() string { return \"k\" }\n\n"
                     "type L struct{}\n\nfunc (L) Name() string { return \"l\" }\n"},
    {"app/app.go", "package app\n\nimport \"example.com/demo/wide\"\n\n"
                   "func Run(w *wide.Wide) string { return w.Last.Name() }\n"},
    {NULL, NULL}};

static int go_wide_check(cbm_store_t *s, const char *project, const char *label) {
    int run = ts_calls_to(s, project, "Run", "kind.L.Name", NULL);
    int use = ts_calls_to(s, project, "Use", "kind.K.Name", NULL) +
              ts_calls_to(s, project, "Use", "kind.L.Name", NULL);
    printf("  go wide struct (%s): Run=%d Use=%d\n", label, run, use);
    return !(run == 1 && use == 2);
}

static char g_go_wide_source[4096];
static char g_go_wide_edited[4096];

static const char *go_wide_source(char *buf, size_t cap, bool edited) {
    size_t at = (size_t)snprintf(buf, cap,
                                 "package wide\n\nimport \"example.com/demo/kind\"\n\ntype Wide struct {\n");
    for (int i = 0; i < 64; i++) {
        at += (size_t)snprintf(buf + at, cap - at, "\tF%d int\n", i);
    }
    snprintf(buf + at, cap - at,
             "\tLate kind.K\n\tLast kind.L\n}\n\nfunc Use(w *Wide) string {\n\t%s\n}\n",
             edited ? "s := w.Late.Name() + w.Last.Name()\n\treturn s"
                    : "return w.Late.Name() + w.Last.Name()");
    return buf;
}

static void go_wide_fixture(ts_file_t *files) {
    int n = (int)(sizeof(go_wide_files) / sizeof(go_wide_files[0])) - 1;
    memcpy(files, go_wide_files, (size_t)n * sizeof(ts_file_t));
    files[n] = (ts_file_t){"wide/wide.go",
                           go_wide_source(g_go_wide_source, sizeof(g_go_wide_source), false)};
    files[n + 1] = (ts_file_t){NULL, NULL};
}

TEST(type_scope_go_struct_fields_have_no_limit) {
    ts_file_t files[8];
    go_wide_fixture(files);
    return ts_twins("gowide", files, CBM_LANG_GO, go_wide_check);
}

/* ── Full index versus every incremental route ───────────────────── */

static void ts_listing_append(sqlite3 *db, const char *sql, const char *project, char **buf,
                              size_t *len) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        return;
    }
    sqlite3_bind_text(st, 1, project, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *row = (const char *)sqlite3_column_text(st, 0);
        size_t n = row ? strlen(row) : 0;
        char *grown = (char *)realloc(*buf, *len + n + 2);
        if (!grown) {
            break;
        }
        *buf = grown;
        memcpy(*buf + *len, row ? row : "", n);
        *len += n;
        (*buf)[(*len)++] = '\n';
        (*buf)[*len] = '\0';
    }
    sqlite3_finalize(st);
}

/* Every node (label, QN) and edge (type, source QN, target QN, properties)
 * of the project, sorted: equal listings are equal graphs. A call's `args`
 * are left out: on main already the closure route renders them differently
 * from a full index (quote style, key order), whatever the resolver did. */
static char *ts_listing(const char *db_path, const char *project) {
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return NULL;
    }
    char *buf = (char *)calloc(1, 1);
    size_t len = 0;
    ts_listing_append(db,
                      "SELECT label||'|'||qualified_name FROM nodes WHERE project=?1 ORDER BY 1",
                      project, &buf, &len);
    ts_listing_append(db,
                      "SELECT e.type||'|'||s.qualified_name||'|'||t.qualified_name||'|'||"
                      "CASE WHEN json_valid(e.properties) THEN json_remove(e.properties, '$.args') "
                      "ELSE coalesce(e.properties, '') END FROM edges e "
                      "JOIN nodes s ON s.id=e.source_id "
                      "JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 ORDER BY 1",
                      project, &buf, &len);
    sqlite3_close(db);
    return buf;
}

/* Print the lines one listing has and the other lacks (a failing case). */
static void ts_listing_diff(const char *a, const char *b, const char *tag) {
    for (const char *line = a; line && *line;) {
        const char *nl = strchr(line, '\n');
        size_t n = nl ? (size_t)(nl - line) : strlen(line);
        bool found = false;
        for (const char *p = b; p && *p && !found;) {
            const char *pnl = strchr(p, '\n');
            size_t pn = pnl ? (size_t)(pnl - p) : strlen(p);
            found = pn == n && memcmp(p, line, n) == 0;
            p = pnl ? pnl + 1 : p + pn;
        }
        if (!found) {
            printf("    only %s: %.*s\n", tag, (int)n, line);
        }
        line = nl ? nl + 1 : line + n;
    }
}

typedef struct {
    cbm_incremental_route_t route;
    cbm_incremental_closure_plan_t plan;
    bool same_graph;
    int before_bad; /* the `before` check on the first index (0 = right) */
    int after_bad;  /* the `after` check on the repaired graph (0 = right) */
} ts_incr_result_t;

/* Run `check` on one database of the harness: 0 when it holds (or there is
 * no check), else nonzero (also when the index named no project). */
static int ts_db_check(const char *db, const char *project, ts_check_fn check, const char *label) {
    if (!check || !project[0]) {
        return check ? 1 : 0;
    }
    cbm_store_t *s = cbm_store_open_path(db);
    int bad = s ? check(s, project, label) : 1;
    if (s) {
        cbm_store_close(s);
    }
    return bad;
}

/* Index `files` (plus `pad` Go or Python fillers), apply `edits`, index again
 * incrementally into the same database, then index the edited tree afresh;
 * the two graphs must be equal whatever route the incremental run took.
 * `before` (optional) checks the first index, `after` the repaired graph. */
static ts_incr_result_t ts_incremental_checked(const ts_file_t *files, CBMLanguage pad_lang,
                                               int pad, const ts_file_t *edits, const char *tag,
                                               ts_check_fn before, ts_check_fn after) {
    ts_incr_result_t r = {.route = CBM_INCREMENTAL_ROUTE_NONE};
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_tscope_incr_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        return r;
    }
    ts_write(tmp, files);
    ts_pad(tmp, pad_lang, pad);
    char db[512];
    snprintf(db, sizeof(db), "%s/incr.db", tmp);
    cbm_pipeline_t *base = cbm_pipeline_new(tmp, db, CBM_MODE_FULL);
    char project[256] = {0};
    if (base && cbm_pipeline_run(base) == 0) {
        snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(base));
    }
    cbm_pipeline_free(base);
    r.before_bad = ts_db_check(db, project, before, "before the edit");

    ts_write(tmp, edits);
    cbm_pipeline_incremental_test_reset_faults();
    cbm_pipeline_t *incr = cbm_pipeline_new(tmp, db, CBM_MODE_FULL);
    int rc = incr ? cbm_pipeline_run(incr) : -1;
    r.route = cbm_pipeline_incremental_test_last_route();
    r.plan = cbm_pipeline_incremental_test_last_closure_plan();
    cbm_pipeline_free(incr);
    r.after_bad = rc == 0 ? ts_db_check(db, project, after, "repaired") : 1;

    char full_db[512];
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    cbm_pipeline_t *full = cbm_pipeline_new(tmp, full_db, CBM_MODE_FULL);
    int full_rc = full ? cbm_pipeline_run(full) : -1;
    cbm_pipeline_free(full);

    char *repaired = rc == 0 && project[0] ? ts_listing(db, project) : NULL;
    char *fresh = full_rc == 0 && project[0] ? ts_listing(full_db, project) : NULL;
    r.same_graph = repaired && fresh && strcmp(repaired, fresh) == 0;
    printf("  incremental %s: route=%d plan files=%d surface_changed=%d dependents=%d "
           "decline=%s same_graph=%d\n",
           tag, (int)r.route, r.plan.files, r.plan.surface_changed, r.plan.dependents,
           r.plan.decline ? r.plan.decline : "-", (int)r.same_graph);
    if (!r.same_graph && repaired && fresh) {
        ts_listing_diff(repaired, fresh, "incremental");
        ts_listing_diff(fresh, repaired, "full");
    }
    free(repaired);
    free(fresh);
    th_rmtree(tmp);
    return r;
}

static ts_incr_result_t ts_incremental(const ts_file_t *files, CBMLanguage pad_lang, int pad,
                                       const ts_file_t *edits, const char *tag) {
    return ts_incremental_checked(files, pad_lang, pad, edits, tag, NULL, NULL);
}

/* Only the caller is re-parsed (a closure of one file: the per-file Go
 * registrar): fixture.go's identities come back from its stored row. */
TEST(type_scope_incremental_caller_edit_per_file_go) {
    const ts_file_t edit[] = {
        {"app/app.go", "package app\n\nimport \"example.com/demo/fixture\"\n\n"
                       "func Run() string {\n\ts := fixture.New()\n\tv := s.Get(\"x\")\n\treturn v\n}\n\n"
                       "func Field() string {\n\th := fixture.NewHolder()\n\treturn h.S.Get(\"y\")\n}\n"},
        {NULL, NULL}};
    ts_incr_result_t r = ts_incremental(go_scope_files, CBM_LANG_GO, 0, edit, "go caller edit");
    ASSERT_EQ(r.route, CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR);
    ASSERT_EQ(r.plan.files, 1);
    ASSERT_TRUE(r.same_graph);
    PASS();
}

/* The same through package-level values: the per-file Go registrar types
 * `conf.Default` from conf.go's stored row. */
TEST(type_scope_incremental_caller_edit_per_file_go_values) {
    const ts_file_t edit[] = {
        {"app/app.go", "package app\n\nimport \"example.com/demo/conf\"\n\n"
                       "func Pointer() string {\n\tv := conf.Default.Get(\"a\")\n\treturn v\n}\n\n"
                       "func Value() string { return conf.Plain.Get(\"b\") }\n\n"
                       "func Fresh() string { return conf.Fresh.Get(\"c\") }\n\n"
                       "func Typed() string { return conf.Primary.String() }\n\n"
                       "func Repeated() string { return conf.Second.String() }\n"},
        {NULL, NULL}};
    ts_incr_result_t r =
        ts_incremental(go_values_files, CBM_LANG_GO, 0, edit, "go values caller edit");
    ASSERT_EQ(r.route, CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR);
    ASSERT_EQ(r.plan.files, 1);
    ASSERT_TRUE(r.same_graph);
    PASS();
}

/* Finding 9: a body edit of the file that holds the identities, with twelve
 * dependents in a 40-file tree (the review's go-incr), stays a one-file
 * repair as on main: the probe and the stored row hash the same bytes. */
static const ts_file_t go_incr_files[] = {
    {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
    {"store/store.go", "package store\n\ntype Store struct{ n int }\n\n"
                       "func (s *Store) Get(k string) string { return k }\n"},
    {"fixture/fixture.go", "package fixture\n\nimport \"example.com/demo/store\"\n\n"
                           "type Holder struct {\n\tS *store.Store\n}\n\n"
                           "func New() *store.Store { return &store.Store{} }\n\n"
                           "func NewHolder() Holder { return Holder{} }\n"},
    {"app0/app.go", "package app0\n\nimport \"example.com/demo/fixture\"\n\n"
                    "func Run() string {\n\ts := fixture.New()\n\treturn s.Get(\"x\")\n}\n"},
    {"app1/app.go", "package app1\n\nimport \"example.com/demo/fixture\"\n\n"
                    "func Run() string {\n\ts := fixture.New()\n\treturn s.Get(\"x\")\n}\n"},
    {"app2/app.go", "package app2\n\nimport \"example.com/demo/fixture\"\n\n"
                    "func Run() string {\n\ts := fixture.New()\n\treturn s.Get(\"x\")\n}\n"},
    {"app3/app.go", "package app3\n\nimport \"example.com/demo/fixture\"\n\n"
                    "func Run() string {\n\ts := fixture.New()\n\treturn s.Get(\"x\")\n}\n"},
    {"app4/app.go", "package app4\n\nimport \"example.com/demo/fixture\"\n\n"
                    "func Run() string {\n\ts := fixture.New()\n\treturn s.Get(\"x\")\n}\n"},
    {"app5/app.go", "package app5\n\nimport \"example.com/demo/fixture\"\n\n"
                    "func Run() string {\n\ts := fixture.New()\n\treturn s.Get(\"x\")\n}\n"},
    {"app6/app.go", "package app6\n\nimport \"example.com/demo/fixture\"\n\n"
                    "func Run() string {\n\ts := fixture.New()\n\treturn s.Get(\"x\")\n}\n"},
    {"app7/app.go", "package app7\n\nimport \"example.com/demo/fixture\"\n\n"
                    "func Run() string {\n\ts := fixture.New()\n\treturn s.Get(\"x\")\n}\n"},
    {"app8/app.go", "package app8\n\nimport \"example.com/demo/fixture\"\n\n"
                    "func Run() string {\n\ts := fixture.New()\n\treturn s.Get(\"x\")\n}\n"},
    {"app9/app.go", "package app9\n\nimport \"example.com/demo/fixture\"\n\n"
                    "func Run() string {\n\ts := fixture.New()\n\treturn s.Get(\"x\")\n}\n"},
    {"app10/app.go", "package app10\n\nimport \"example.com/demo/fixture\"\n\n"
                     "func Run() string {\n\ts := fixture.New()\n\treturn s.Get(\"x\")\n}\n"},
    {"app11/app.go", "package app11\n\nimport \"example.com/demo/fixture\"\n\n"
                     "func Run() string {\n\ts := fixture.New()\n\treturn s.Get(\"x\")\n}\n"},
    {NULL, NULL}};

TEST(type_scope_incremental_body_edit_stays_one_file) {
    const ts_file_t edit[] = {
        {"fixture/fixture.go", "package fixture\n\nimport \"example.com/demo/store\"\n\n"
                               "type Holder struct {\n\tS *store.Store\n}\n\n"
                               "func New() *store.Store {\n\ts := &store.Store{}\n\treturn s\n}\n\n"
                               "func NewHolder() Holder { return Holder{} }\n"},
        {NULL, NULL}};
    ts_incr_result_t r = ts_incremental(go_incr_files, CBM_LANG_GO, 25, edit, "go body edit");
    ASSERT_EQ(r.route, CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR);
    ASSERT_EQ(r.plan.files, 1);
    ASSERT_EQ(r.plan.surface_changed, 0);
    ASSERT_EQ(r.plan.dependents, 0);
    ASSERT_TRUE(r.same_graph);
    PASS();
}

/* Finding 7, Go: the type an identity names changes in its declaring file
 * while every name stays (an alias that becomes a defined type, a type that
 * becomes a variable), and a caller two imports away resolved through it.
 * The methods live in other files of the package, so the caller is no
 * dependent of the edited file. The doc-link scope records an exported
 * type's kind and alias target, so main already declines these; the graph
 * must still be the full index's. */
static const ts_file_t go_alias_files[] = {
    {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
    {"store/b.go", "package store\n\ntype Impl struct{ n int }\n\ntype Impl2 struct{ n int }\n\n"
                   "type T = Impl\n"},
    {"store/b2.go", "package store\n\nfunc (i Impl) M() string { return \"impl\" }\n"},
    {"store/b3.go", "package store\n\nfunc (i Impl2) M() string { return \"impl2\" }\n"},
    {"decoy/d.go", "package decoy\n\ntype T struct{ n int }\n\nfunc (t T) M() string { return \"d\" }\n"},
    {"fixture/a.go", "package fixture\n\nimport \"example.com/demo/store\"\n\n"
                     "func Make() store.T { return store.T{} }\n"},
    {"app/c.go", "package app\n\nimport \"example.com/demo/fixture\"\n\n"
                 "func Run() string { return fixture.Make().M() }\n"},
    {NULL, NULL}};

/* The change the doc-link scope does not see (it records an exported type's
 * name, kind, alias target and field names, never a field's type): the
 * struct an identity names gets another field type. The caller resolved
 * `fixture.Get().S.Get()` through fixture.go's identity `model.Holder` and
 * Holder's field; it has no edge into model/b.go. */
TEST(type_scope_incremental_named_type_field_change_matches_full) {
    const ts_file_t files[] = {
        {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
        {"store/store.go", "package store\n\ntype Store struct{ n int }\n\ntype Other struct{ n int }\n"},
        {"store/m1.go", "package store\n\nfunc (s *Store) Get() string { return \"s\" }\n"},
        {"store/m2.go", "package store\n\nfunc (o *Other) Get() string { return \"o\" }\n"},
        {"model/b.go", "package model\n\nimport \"example.com/demo/store\"\n\n"
                       "type Holder struct {\n\tS *store.Store\n}\n"},
        {"fixture/a.go", "package fixture\n\nimport \"example.com/demo/model\"\n\n"
                         "func Get() model.Holder { return model.Holder{} }\n"},
        {"app/c.go", "package app\n\nimport \"example.com/demo/fixture\"\n\n"
                     "func Run() string { return fixture.Get().S.Get() }\n"},
        {NULL, NULL}};
    const ts_file_t edit[] = {
        {"model/b.go", "package model\n\nimport \"example.com/demo/store\"\n\n"
                       "type Holder struct {\n\tS *store.Other\n}\n"},
        {NULL, NULL}};
    ts_incr_result_t r = ts_incremental(files, CBM_LANG_GO, 0, edit, "go named type field change");
    ASSERT_TRUE(r.same_graph);
    ASSERT_STR_EQ(r.plan.decline, "identity_target_changed");
    PASS();
}

TEST(type_scope_incremental_type_shape_changes_match_full) {
    const ts_file_t to_defined[] = {
        {"store/b.go", "package store\n\ntype Impl struct{ n int }\n\ntype Impl2 struct{ n int }\n\n"
                       "type T Impl\n"},
        {NULL, NULL}};
    ts_incr_result_t r =
        ts_incremental(go_alias_files, CBM_LANG_GO, 0, to_defined, "go alias->defined type");
    ASSERT_TRUE(r.same_graph);
    ASSERT_EQ(r.route, CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    const ts_file_t to_var[] = {
        {"store/b.go", "package store\n\ntype Impl struct{ n int }\n\ntype Impl2 struct{ n int }\n\n"
                       "var T = Impl{}\n"},
        {NULL, NULL}};
    r = ts_incremental(go_alias_files, CBM_LANG_GO, 0, to_var, "go type->var");
    ASSERT_TRUE(r.same_graph);
    ASSERT_EQ(r.route, CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    PASS();
}

/* The shape only the identity gate catches: the struct an identity names
 * embeds another package's type of the same name (`p1.E` -> `p2.E`, both
 * imported already). The doc-link scope sees the same type, kind and field
 * names; the caller reached the promoted method through fixture.go's
 * identity and has no edge into model/b.go, so a repair would keep its stale
 * edge to p1.E.Hello. */
TEST(type_scope_incremental_named_type_embed_change_matches_full) {
    const ts_file_t files[] = {
        {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
        {"p1/e.go", "package p1\n\ntype E struct{ n int }\n\nfunc (e *E) Hello() string { return \"1\" }\n"},
        {"p2/e.go", "package p2\n\ntype E struct{ n int }\n\nfunc (e *E) Hello() string { return \"2\" }\n"},
        {"model/b.go", "package model\n\nimport (\n\t\"example.com/demo/p1\"\n\t\"example.com/demo/p2\"\n)\n\n"
                       "var _ p2.E\n\ntype T struct {\n\tp1.E\n}\n"},
        {"fixture/a.go", "package fixture\n\nimport \"example.com/demo/model\"\n\n"
                         "func Make() *model.T { return &model.T{} }\n"},
        {"app/c.go", "package app\n\nimport \"example.com/demo/fixture\"\n\n"
                     "func Run() string { return fixture.Make().Hello() }\n"},
        {NULL, NULL}};
    const ts_file_t edit[] = {
        {"model/b.go", "package model\n\nimport (\n\t\"example.com/demo/p1\"\n\t\"example.com/demo/p2\"\n)\n\n"
                       "var _ p1.E\n\ntype T struct {\n\tp2.E\n}\n"},
        {NULL, NULL}};
    ts_incr_result_t r = ts_incremental(files, CBM_LANG_GO, 0, edit, "go named type embed change");
    ASSERT_TRUE(r.same_graph);
    ASSERT_STR_EQ(r.plan.decline, "identity_target_changed");
    PASS();
}

/* Round 3, N4 (the review's go-emb16): a body edit of the file declaring a
 * struct of 17 embedded types is a one-file repair, and the repaired graph is
 * the full index's: the per-file route takes the struct's embedded types from
 * its definition, as the full index does, all of them (it kept 15). */
static char g_go_emb_parts[4096];
static char g_go_emb_big[2048];
static char g_go_emb_big_edited[2048];

static void go_emb_sources(void) {
    size_t at = (size_t)snprintf(g_go_emb_parts, sizeof(g_go_emb_parts), "package big\n\n");
    size_t fa = 0;
    char fields[512];
    for (int i = 0; i < 17; i++) {
        at += (size_t)snprintf(g_go_emb_parts + at, sizeof(g_go_emb_parts) - at,
                               "type E%d struct{}\n\nfunc (E%d) M%d() string { return \"%d\" }\n\n", i,
                               i, i, i);
        fa += (size_t)snprintf(fields + fa, sizeof(fields) - fa, "\tE%d\n", i);
    }
    snprintf(g_go_emb_big, sizeof(g_go_emb_big),
             "package big\n\ntype Big struct {\n%s}\n\n"
             "func Use(b *Big) string {\n\treturn b.M16() + b.M14()\n}\n",
             fields);
    snprintf(g_go_emb_big_edited, sizeof(g_go_emb_big_edited),
             "package big\n\ntype Big struct {\n%s}\n\n"
             "func Use(b *Big) string {\n\ts := b.M16() + b.M14()\n\treturn s\n}\n",
             fields);
}

/* The repaired graph has the edge through the 17th embedded type. */
static int go_emb16_after(cbm_store_t *s, const char *project, const char *label) {
    int m16 = ts_calls_to(s, project, "Use", "big.E16.M16", NULL);
    int m14 = ts_calls_to(s, project, "Use", "big.E14.M14", NULL);
    printf("  go 17 embedded types (%s): Use -> E16.M16=%d E14.M14=%d\n", label, m16, m14);
    return !(m16 == 1 && m14 == 1);
}

TEST(type_scope_incremental_many_embedded_types_match_full) {
    go_emb_sources();
    const ts_file_t files[] = {
        {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
        {"big/parts.go", g_go_emb_parts},
        {"big/big.go", g_go_emb_big},
        {"big/use2.go", "package big\n\nfunc Use2(b *Big) string { return b.M16() }\n"},
        {"decoy/d.go", "package decoy\n\ntype D struct{}\n\nfunc (D) M16() string { return \"d\" }\n\n"
                       "func (D) M14() string { return \"d\" }\n"},
        {"app/app.go", "package app\n\nimport \"example.com/demo/big\"\n\n"
                       "func Run(b *big.Big) string { return b.M16() }\n"},
        {NULL, NULL}};
    const ts_file_t edit[] = {{"big/big.go", g_go_emb_big_edited}, {NULL, NULL}};
    ts_incr_result_t r = ts_incremental_checked(files, CBM_LANG_GO, 40, edit, "go 17 embedded types",
                                                NULL, go_emb16_after);
    ASSERT_EQ(r.route, CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR);
    ASSERT_EQ(r.plan.files, 1);
    ASSERT_TRUE(r.same_graph);
    ASSERT_EQ(r.after_bad, 0);
    PASS();
}

/* The same for a struct of 66 fields (the 63-field limit beside N4): the
 * repaired graph is the full index's and has the calls through fields 65
 * and 66 (both routes cut at 63 before). */
static int go_wide_after(cbm_store_t *s, const char *project, const char *label) {
    int use = ts_calls_to(s, project, "Use", "kind.K.Name", NULL) +
              ts_calls_to(s, project, "Use", "kind.L.Name", NULL);
    printf("  go 66 fields (%s): Use through fields 65 and 66=%d\n", label, use);
    return use != 2;
}

TEST(type_scope_incremental_many_fields_match_full) {
    ts_file_t files[8];
    go_wide_fixture(files);
    const ts_file_t edit[] = {
        {"wide/wide.go", go_wide_source(g_go_wide_edited, sizeof(g_go_wide_edited), true)},
        {NULL, NULL}};
    ts_incr_result_t r = ts_incremental_checked(files, CBM_LANG_GO, 40, edit, "go 66 fields", NULL,
                                                go_wide_after);
    ASSERT_EQ(r.route, CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR);
    ASSERT_EQ(r.plan.files, 1);
    ASSERT_TRUE(r.same_graph);
    ASSERT_EQ(r.after_bad, 0);
    PASS();
}

/* The gate is narrow: a signature change of another definition in the
 * declaring file stays a repair. */
TEST(type_scope_incremental_other_def_edit_stays_a_repair) {
    const ts_file_t edit[] = {
        {"store/b.go", "package store\n\ntype Impl struct{ n int }\n\ntype T = Impl\n\n"
                       "func Helper(n int) int { return n }\n"},
        {NULL, NULL}};
    const ts_file_t files[] = {
        {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
        {"store/b.go", "package store\n\ntype Impl struct{ n int }\n\ntype T = Impl\n\n"
                       "func Helper() int { return 1 }\n"},
        {"store/b2.go", "package store\n\nfunc (i Impl) M() string { return \"impl\" }\n"},
        {"fixture/a.go", "package fixture\n\nimport \"example.com/demo/store\"\n\n"
                         "func Make() store.T { return store.T{} }\n"},
        {"app/c.go", "package app\n\nimport \"example.com/demo/fixture\"\n\n"
                     "func Run() string { return fixture.Make().M() }\n"},
        {NULL, NULL}};
    ts_incr_result_t r = ts_incremental(files, CBM_LANG_GO, 0, edit, "go other def edit");
    ASSERT_EQ(r.route, CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR);
    ASSERT_TRUE(r.same_graph);
    PASS();
}

/* Python: a caller's body edit re-resolves that file alone with the per-file
 * Python registrar, reading fixture.py's identities from its stored row; a
 * body edit of fixture.py itself stays a one-file repair too. */
TEST(type_scope_incremental_python_body_edits) {
    const ts_file_t caller[] = {
        {"pkg/app.py", "from pkg.fixture import make, local\n\n\n"
                       "def run():\n    s = make()\n    v = s.get(\"x\")\n    return v\n\n\n"
                       "def own():\n    v = local()\n    return v.ping()\n"},
        {NULL, NULL}};
    ts_incr_result_t r = ts_incremental(py_scope_files, CBM_LANG_PYTHON, 0, caller, "py caller");
    ASSERT_EQ(r.route, CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR);
    ASSERT_EQ(r.plan.files, 1);
    ASSERT_TRUE(r.same_graph);
    const ts_file_t declaring[] = {
        {"pkg/fixture.py", "from pkg.store import Store\n\n\n"
                           "class Local:\n    def ping(self):\n        return 1\n\n\n"
                           "def make() -> Store:\n    s = Store()\n    return s\n\n\n"
                           "def local() -> Local:\n    return Local()\n"},
        {NULL, NULL}};
    r = ts_incremental(py_scope_files, CBM_LANG_PYTHON, 0, declaring, "py declaring body");
    ASSERT_EQ(r.route, CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR);
    ASSERT_EQ(r.plan.files, 1);
    ASSERT_EQ(r.plan.surface_changed, 0);
    ASSERT_TRUE(r.same_graph);
    PASS();
}

/* Finding 7, Python: the declaring module's name changes from an alias to a
 * class (and a class to a function), a caller two imports away. */
TEST(type_scope_incremental_python_declaring_change_matches_full) {
    const ts_file_t files[] = {
        {"pkg/__init__.py", ""},
        {"pkg/b.py", "class Impl:\n    def m(self):\n        return 1\n\n\nT = Impl\n"},
        {"pkg/a.py", "from pkg.b import T\n\n\ndef make() -> T:\n    return T()\n"},
        {"pkg/c.py", "from pkg.a import make\n\n\ndef run():\n    return make().m()\n"},
        {NULL, NULL}};
    const ts_file_t to_class[] = {
        {"pkg/b.py", "class Impl:\n    def m(self):\n        return 1\n\n\n"
                     "class T:\n    def m(self):\n        return 2\n"},
        {NULL, NULL}};
    ts_incr_result_t r = ts_incremental(files, CBM_LANG_PYTHON, 0, to_class, "py alias->class");
    ASSERT_TRUE(r.same_graph);
    const ts_file_t start_class[] = {
        {"pkg/__init__.py", ""},
        {"pkg/b.py", "class T:\n    def m(self):\n        return 1\n"},
        {"pkg/a.py", "from pkg.b import T\n\n\ndef make() -> T:\n    return T()\n"},
        {"pkg/c.py", "from pkg.a import make\n\n\ndef run():\n    return make().m()\n"},
        {NULL, NULL}};
    const ts_file_t to_function[] = {
        {"pkg/b.py", "def T():\n    return 1\n\n\ndef m():\n    return 2\n"}, {NULL, NULL}};
    r = ts_incremental(start_class, CBM_LANG_PYTHON, 0, to_function, "py class->function");
    ASSERT_TRUE(r.same_graph);
    PASS();
}

/* Round 3, N2 on the incremental routes: a class-body edit that binds the
 * name a method annotation uses (`marker = OtherConfig` becomes `Config =
 * OtherConfig`: same lines, same bytes otherwise) changes the class's
 * identities. The class's bindings are part of its stored row, so the run is
 * not a one-file repair that keeps the caller's stale `run ->
 * pkg.mod.Config.load`: before the edit the edge is there (Config is the
 * module's), after it the repaired graph has none (python3: OtherConfig). */
static int py_classedit_before(cbm_store_t *s, const char *project, const char *label) {
    int edge = ts_calls_to(s, project, "run", "pkg.mod.Config.load", NULL);
    printf("  py class-body edit (%s): run -> Config.load=%d\n", label, edge);
    return edge != 1;
}

static int py_classedit_after(cbm_store_t *s, const char *project, const char *label) {
    int edge = ts_calls_to(s, project, "run", "pkg.mod.Config.load", NULL);
    printf("  py class-body edit (%s): run -> Config.load=%d\n", label, edge);
    return edge != 0;
}

TEST(type_scope_incremental_python_class_body_binding_matches_full) {
    const ts_file_t files[] = {
        {"pkg/__init__.py", ""},
        {"pkg/other.py", "class OtherConfig:\n    def load(self):\n        return 'other'\n"},
        {"pkg/mod.py", "from pkg.other import OtherConfig\n\n\n"
                       "class Config:\n    def load(self):\n        return 'module'\n\n\n"
                       "class Model:\n    marker = OtherConfig\n\n"
                       "    def cfg(self) -> Config:\n        return Config()\n"},
        {"pkg/use.py", "from pkg.mod import Model\n\n\ndef run():\n    return Model().cfg().load()\n"},
        {NULL, NULL}};
    const ts_file_t edit[] = {
        {"pkg/mod.py", "from pkg.other import OtherConfig\n\n\n"
                       "class Config:\n    def load(self):\n        return 'module'\n\n\n"
                       "class Model:\n    Config = OtherConfig\n\n"
                       "    def cfg(self) -> Config:\n        return Config()\n"},
        {NULL, NULL}};
    ts_incr_result_t r = ts_incremental_checked(files, CBM_LANG_PYTHON, 0, edit,
                                                "py class-body binding", py_classedit_before,
                                                py_classedit_after);
    ASSERT_TRUE(r.same_graph);
    ASSERT_EQ(r.before_bad, 0);
    ASSERT_EQ(r.after_bad, 0);
    PASS();
}

/* A function body edit (same lines) that declares the module's R `global`
 * and binds it: the module's annotation `-> R` loses its identity. The
 * function's global names are part of its stored row, so the run is not a
 * one-file repair that keeps the caller's stale `run -> pkg.a.R.save`. */
static int py_globaledit_before(cbm_store_t *s, const char *project, const char *label) {
    int edge = ts_calls_to(s, project, "run", "pkg.a.R.save", NULL);
    printf("  py global edit (%s): run -> pkg.a.R.save=%d\n", label, edge);
    return edge != 1;
}

static int py_globaledit_after(cbm_store_t *s, const char *project, const char *label) {
    int edge = ts_calls_to(s, project, "run", "pkg.a.R.save", NULL);
    printf("  py global edit (%s): run -> pkg.a.R.save=%d\n", label, edge);
    return edge != 0;
}

TEST(type_scope_incremental_python_global_binding_matches_full) {
    const ts_file_t files[] = {
        {"pkg/__init__.py", ""},
        {"pkg/a.py", "class R:\n    def save(self):\n        return 'a'\n"},
        {"pkg/b.py", "class R:\n    def save(self):\n        return 'b'\n"},
        {"pkg/mod.py", "from pkg.a import R\n\n\ndef setup():\n    r = None\n    from pkg.b import R\n\n\n"
                       "def make() -> R:\n    return R()\n"},
        {"pkg/use.py", "from pkg.mod import make\n\n\ndef run():\n    return make().save()\n"},
        {NULL, NULL}};
    const ts_file_t edit[] = {
        {"pkg/mod.py", "from pkg.a import R\n\n\ndef setup():\n    global R\n    from pkg.b import R\n\n\n"
                       "def make() -> R:\n    return R()\n"},
        {NULL, NULL}};
    ts_incr_result_t r = ts_incremental_checked(files, CBM_LANG_PYTHON, 0, edit, "py global binding",
                                                py_globaledit_before, py_globaledit_after);
    ASSERT_TRUE(r.same_graph);
    ASSERT_EQ(r.before_bad, 0);
    ASSERT_EQ(r.after_bad, 0);
    PASS();
}

SUITE(type_scope) {
    RUN_TEST(type_scope_import_binding_is_the_identity);
    RUN_TEST(type_scope_dotted_spelling_through_its_head);
    RUN_TEST(type_scope_stores_nothing_without_one_answer);
    RUN_TEST(type_scope_names_outside_the_project_get_none);
    RUN_TEST(type_scope_go_bare_names_and_own_module);
    RUN_TEST(type_scope_python_own_module);
    RUN_TEST(type_scope_python_enclosing_scopes);
    RUN_TEST(type_scope_python_class_body_bindings);
    RUN_TEST(type_scope_python_type_parameters);
    RUN_TEST(type_scope_python_unlisted_bindings_get_none);
    RUN_TEST(type_scope_engine_table_rows);
    RUN_TEST(type_scope_open_imports_one_answer_or_none);
    RUN_TEST(type_scope_implicit_external_scope_gets_none);
    RUN_TEST(type_scope_nested_types_innermost_first);
    RUN_TEST(type_scope_include_closure_source);
    RUN_TEST(type_scope_lexical_nesting_source);
    RUN_TEST(type_scope_colon_paths);
    RUN_TEST(type_scope_calling_file_imports);
    RUN_TEST(type_scope_calling_file_scope_per_engine);
    RUN_TEST(type_scope_nested_and_open_work_grow_linearly);
    RUN_TEST(type_scope_field_fold_rows);
    RUN_TEST(type_scope_step_work_grows_linearly);
    RUN_TEST(type_scope_lookup_work_grows_linearly);
    RUN_TEST(type_scope_reach_work_grows_linearly);
    RUN_TEST(type_scope_identity_gate_work_grows_linearly);
    RUN_TEST(type_scope_go_return_and_field_types);
    RUN_TEST(type_scope_go_type_parameter_is_no_import);
    RUN_TEST(type_scope_python_return_types);
    RUN_TEST(type_scope_python_nested_class_is_not_the_module_class);
    RUN_TEST(type_scope_python_class_body_bindings_shadow_the_module);
    RUN_TEST(type_scope_python_class_and_module_of_one_qn);
    RUN_TEST(type_scope_python_global_rebinding_takes_no_identity);
    RUN_TEST(type_scope_python_unbound_class_names_are_the_module_names);
    RUN_TEST(type_scope_python_overloaded_method_takes_no_identity);
    RUN_TEST(type_scope_python_overload_imported_under_another_name);
    RUN_TEST(type_scope_python_self_chain_keeps_its_edges);
    RUN_TEST(type_scope_python_type_parameter_is_no_import);
    RUN_TEST(type_scope_go_fields_survive_an_earlier_receiver_per_file);
    RUN_TEST(type_scope_go_fields_survive_an_earlier_receiver_shared);
    RUN_TEST(type_scope_go_shared_type_qn_keeps_no_fields_per_file);
    RUN_TEST(type_scope_go_shared_type_qn_keeps_no_fields_shared);
    RUN_TEST(type_scope_go_closure_parameters_in_composite_literals);
    RUN_TEST(type_scope_go_package_values);
    RUN_TEST(type_scope_go_test_package_values_share_no_name);
    RUN_TEST(type_scope_go_build_variant_values_stay_typed);
    RUN_TEST(type_scope_go_embedded_members_promote);
    RUN_TEST(type_scope_go_promotion_takes_the_shallowest_depth);
    RUN_TEST(type_scope_go_embedded_field_names_are_fields);
    RUN_TEST(type_scope_go_function_declared_types_are_not_the_package_types);
    RUN_TEST(type_scope_go_local_types_shadow_only_their_block);
    RUN_TEST(type_scope_go_embedded_text_work_grows_linearly);
    RUN_TEST(type_scope_go_struct_fields_have_no_limit);
    RUN_TEST(type_scope_incremental_caller_edit_per_file_go);
    RUN_TEST(type_scope_incremental_caller_edit_per_file_go_values);
    RUN_TEST(type_scope_incremental_body_edit_stays_one_file);
    RUN_TEST(type_scope_incremental_named_type_field_change_matches_full);
    RUN_TEST(type_scope_incremental_named_type_embed_change_matches_full);
    RUN_TEST(type_scope_incremental_many_embedded_types_match_full);
    RUN_TEST(type_scope_incremental_many_fields_match_full);
    RUN_TEST(type_scope_incremental_python_class_body_binding_matches_full);
    RUN_TEST(type_scope_incremental_python_global_binding_matches_full);
    RUN_TEST(type_scope_incremental_type_shape_changes_match_full);
    RUN_TEST(type_scope_incremental_other_def_edit_stays_a_repair);
    RUN_TEST(type_scope_incremental_python_body_edits);
    RUN_TEST(type_scope_incremental_python_declaring_change_matches_full);
}
