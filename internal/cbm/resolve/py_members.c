#include "py_members.h"
#include "foundation/hash_table.h"
#include "foundation/mem_core.h"
#include <stdlib.h>
#include <string.h>

/* Finite analysis per occurrence. Past either limit the receiver is unknown, the
 * same answer every run: local rebinding cycles and attributes that read each
 * other would otherwise branch without end. */
enum { PY_MAX_DEPTH = 24, PY_MAX_MRO = 512, PY_MAX_REF_STEPS = 64, PY_MAX_EVAL_STEPS = 2048 };

typedef enum {
    PY_REF_NONE = 0,
    PY_REF_KEY,     /* a shared solver fact */
    PY_REF_QN,      /* a declaration in the capturing file */
    PY_REF_SELF,    /* an instance of the class named by name */
    PY_REF_CLASS,   /* the class named by name */
    PY_REF_SUPER,   /* lookup after the class named by name */
    PY_REF_UNBOUND, /* a name no scope binds */
    PY_REF_JOIN     /* assignments of one local, which must agree */
} py_ref_kind_t;

struct py_join;

/* A value reachable from any file: a root plus member and call steps. */
typedef struct {
    py_ref_kind_t kind;
    const char *name;
    const char *tail;
    const struct py_join *join;
} py_ref_t;

/* x = Store(), x = x.chain(): the values a local holds. A self arm is the steps
 * applied to the local itself, which must lead back to the same class. */
typedef struct py_join {
    py_ref_t *arms;
    uint32_t arm_count;
    const char **self_tails;
    uint32_t self_count;
} py_join_t;

typedef enum {
    PY_MEMBER_FUNCTION = 0,
    PY_MEMBER_PROPERTY, /* reading it runs the function */
    PY_MEMBER_VALUE,
    PY_MEMBER_DECORATED, /* awaits the project-wide decorator decision */
    PY_MEMBER_OPAQUE     /* present, but its identity is not provable */
} py_member_kind_t;

typedef struct {
    py_ref_t ref;          /* the decorator, without a trailing call */
    bool applied;          /* `@deco(...)` rather than `@deco` */
    const char *canonical; /* imported spelling, for stdlib facts */
} py_decorator_t;

typedef struct py_decorated {
    const char *target;
    py_decorator_t *decorators;
    uint32_t count;
    bool is_class;
    struct py_decorated *next;
} py_decorated_t;

typedef struct py_class py_class_t;

typedef struct {
    py_ref_t result;
    unsigned shape;
    py_ref_t delegate;
    bool delegate_applied;
    /* Unannotated: the class every return statement agrees on, settled at seal. */
    py_ref_t *returned;
    uint32_t returned_count;
    const py_class_t *inferred;
    unsigned char inference; /* 0 pending, 1 in progress, 2 settled */
} py_function_t;

typedef struct {
    const char *name, *target;
    py_member_kind_t kind;
    py_ref_t value;    /* return type, property type, or class attribute value */
    bool none_default; /* `x = None` in the class body: instances store the real value */
    bool annotated;    /* its value comes from an annotation */
} py_member_t;

typedef struct {
    const char *name;
    py_ref_t value;
    bool none;
} py_attribute_t;

struct py_class {
    const char *qualified_name;
    py_ref_t *bases;
    uint32_t base_count;
    uint64_t subscripted; /* bit per base: written Base[...] */
    bool unknown_base, metaclass;
    py_member_t *members; /* sorted by name */
    uint32_t member_count;
    py_attribute_t *attributes; /* sorted by name */
    uint32_t attribute_count;
    /* Linearization, and how much of it is certain: up to the first class whose
     * own attributes are unknown. */
    const struct py_class **mro;
    uint32_t mro_count, reliable;
    const struct py_class **direct; /* resolved written bases, opaque ones included */
    uint32_t direct_count;
    const struct py_class *first; /* the first written base, when it resolved */
    bool opaque, duplicate, visiting, sealed, intercepts, custom_new;
    /* A base here or in the ancestry is unknown and could reorder what follows. */
    bool uncertain;
};

struct cbm_py_members {
    CBMArena arena;
    CBMHashTable *classes;   /* qualified name -> py_class_t */
    CBMHashTable *opaque;    /* spelling -> py_class_t */
    CBMHashTable *values;    /* module variable -> py_ref_t */
    CBMHashTable *functions; /* function -> py_function_t */
    CBMHashTable *kept;      /* decorated declaration -> keep verdict */
    CBMHashTable *rebound;   /* every decorated declaration */
    py_decorated_t *decorated;
    bool decided;
};

cbm_py_members_t *cbm_py_members_new(void) {
    cbm_py_members_t *members = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, sizeof(*members));
    if (!members)
        return NULL;
    cbm_arena_init(&members->arena);
    members->classes = cbm_ht_create(128);
    members->opaque = cbm_ht_create(32);
    members->values = cbm_ht_create(128);
    members->functions = cbm_ht_create(128);
    members->kept = cbm_ht_create(128);
    members->rebound = cbm_ht_create(128);
    if (!members->classes || !members->opaque || !members->values || !members->functions ||
        !members->kept || !members->rebound) {
        cbm_py_members_free(members);
        return NULL;
    }
    return members;
}

void cbm_py_members_free(cbm_py_members_t *members) {
    if (!members)
        return;
    cbm_ht_free(members->classes);
    cbm_ht_free(members->opaque);
    cbm_ht_free(members->values);
    cbm_ht_free(members->functions);
    cbm_ht_free(members->kept);
    cbm_ht_free(members->rebound);
    cbm_arena_destroy(&members->arena);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, members);
}

/* ── References ──────────────────────────────────────────────────────── */

static const char *py_keep(cbm_py_members_t *members, const char *text) {
    return text ? cbm_arena_strdup(&members->arena, text) : NULL;
}

/* Reduce a chain to its root, in the scope it was written in. A local name with
 * one binder is replaced by what it was bound to, so the result no longer
 * depends on this file's lexical state. */
static py_ref_t py_ref_of_type(const cbm_resolve_binding_index_t *index, CBMArena *arena,
                               const cbm_py_import_lookup_t *imports, const char *chain,
                               const char *rest, uint32_t scope, unsigned depth, unsigned *steps);

static py_ref_t py_ref_of_chain(const cbm_resolve_binding_index_t *index, CBMArena *arena,
                                const cbm_py_import_lookup_t *imports, const char *chain,
                                uint32_t scope, uint32_t at, unsigned depth, unsigned *steps) {
    py_ref_t none = {PY_REF_NONE, NULL, NULL, NULL};
    if (!chain || depth > PY_MAX_DEPTH || ++*steps > PY_MAX_REF_STEPS)
        return none;
    size_t length = strcspn(chain, ".(");
    const char *rest = chain + length;
    /* super and type are the builtins only while nothing in scope binds the name. */
    bool builtin = rest[0] == '(' &&
                   ((length == 5 && !strncmp(chain, "super", 5)) ||
                    (length == 4 && !strncmp(chain, "type", 4))) &&
                   cbm_resolve_binding_python_root(index, chain, length, scope, at).kind ==
                       CBM_PY_ROOT_UNBOUND;
    if (builtin && length == 5) {
        if (rest[1] == ')') {
            const char *owner = cbm_resolve_binding_python_method_owner(index, scope);
            return owner ? (py_ref_t){PY_REF_SUPER, owner, rest + 2, NULL} : none;
        }
        /* super(Owner, self): the first argument names the class. */
        const char *comma = strchr(rest, ',');
        const char *close = comma ? strchr(comma, ')') : NULL;
        if (!close)
            return none;
        const char *joined = cbm_arena_sprintf(arena, "%.*s.<super>%s", (int)(comma - rest - 1),
                                               rest + 1, close + 1);
        return py_ref_of_chain(index, arena, imports, joined, scope, at, depth + 1, steps);
    }
    if (builtin && rest[1] != ')') {
        const char *close = strchr(rest, ')');
        if (!close)
            return none;
        const char *joined = cbm_arena_sprintf(arena, "%.*s.__class__%s", (int)(close - rest - 1),
                                               rest + 1, close + 1);
        return py_ref_of_chain(index, arena, imports, joined, scope, at, depth + 1, steps);
    }
    /* functools.partial(f, ...) called is f called. The value is f's for that
     * one purpose; member reads through a function stay unknown anyway. */
    const char *open = strchr(chain, '(');
    if (open && open[1] != ')' && open > chain) {
        const char *spelled = cbm_arena_strndup(arena, chain, (size_t)(open - chain));
        if (spelled && (!strcmp(spelled, "partial") || !strcmp(spelled, "functools.partial")) &&
            cbm_resolve_binding_python_import_is(index, spelled, "functools.partial")) {
            int nesting = 0;
            const char *close = open;
            for (; *close; close++) {
                nesting += (*close == '(') - (*close == ')');
                if (!nesting)
                    break;
            }
            if (!*close || close[1])
                return none;
            const char *wrapped = cbm_arena_strndup(arena, open + 1, (size_t)(close - open - 1));
            return py_ref_of_chain(index, arena, imports, wrapped, scope, at, depth + 1, steps);
        }
    }
    cbm_resolve_python_root_t root =
        cbm_resolve_binding_python_root(index, chain, length, scope, at);
    switch (root.kind) {
    case CBM_PY_ROOT_IMPORT: {
        const char *remainder = NULL;
        const char *key =
            imports->import_key(imports->context, arena, root.nested, chain, &remainder);
        return key ? (py_ref_t){PY_REF_KEY, key, remainder, NULL} : none;
    }
    case CBM_PY_ROOT_DECLARATION:
    case CBM_PY_ROOT_MODULE_VALUE:
        return (py_ref_t){PY_REF_QN, root.target, rest, NULL};
    case CBM_PY_ROOT_ASSIGNED_MANY: {
        py_join_t *join = cbm_arena_calloc(arena, sizeof(*join));
        py_ref_t *arms = cbm_arena_calloc(arena, (root.chain_count + 1) * sizeof(*arms));
        const char **tails = cbm_arena_calloc(arena, (root.chain_count + 1) * sizeof(*tails));
        if (!join || !arms || !tails)
            return none;
        for (uint32_t i = 0; i < root.chain_count; i++) {
            const char *arm = root.chains[i];
            if (!strncmp(arm, chain, length) && (!arm[length] || strchr(".(", arm[length]))) {
                tails[join->self_count++] = arm + length;
                continue;
            }
            arms[join->arm_count] =
                py_ref_of_chain(index, arena, imports, arm, root.chain_scope, 0, depth + 1, steps);
            if (arms[join->arm_count].kind == PY_REF_NONE)
                return none;
            join->arm_count++;
        }
        if (!join->arm_count)
            return none;
        join->arms = arms;
        join->self_tails = tails;
        return (py_ref_t){.kind = PY_REF_JOIN, .join = join, .tail = rest};
    }
    case CBM_PY_ROOT_ASSIGNED:
        return root.chain ? py_ref_of_chain(index, arena, imports,
                                            cbm_arena_sprintf(arena, "%s%s", root.chain, rest),
                                            root.chain_scope, 0, depth + 1, steps)
                          : none;
    case CBM_PY_ROOT_TYPED:
        return root.chain ? py_ref_of_type(index, arena, imports, root.chain, rest,
                                           root.chain_scope, depth + 1, steps)
                          : none;
    case CBM_PY_ROOT_ANNOTATION_IMPORT:
        return none; /* no runtime value: only py_ref_of_type reads through it */
    case CBM_PY_ROOT_SELF_PARAMETER:
        return root.owner ? (py_ref_t){PY_REF_SELF, root.owner, rest, NULL} : none;
    case CBM_PY_ROOT_CLASS_PARAMETER:
        return root.owner ? (py_ref_t){PY_REF_CLASS, root.owner, rest, NULL} : none;
    case CBM_PY_ROOT_UNBOUND: {
        const char *name = cbm_arena_strndup(arena, chain, length);
        const char *key =
            name && imports->star_key ? imports->star_key(imports->context, arena, name) : NULL;
        return key ? (py_ref_t){PY_REF_KEY, key, rest, NULL}
                   : (name ? (py_ref_t){PY_REF_UNBOUND, name, rest, NULL} : none);
    }
    case CBM_PY_ROOT_UNKNOWN:
        break;
    }
    return none;
}

/* An instance of the class an annotation names, followed by further steps. The
 * annotation's root may be a TYPE_CHECKING import; a runtime chain's may not. */
static py_ref_t py_ref_of_type(const cbm_resolve_binding_index_t *index, CBMArena *arena,
                               const cbm_py_import_lookup_t *imports, const char *chain,
                               const char *rest, uint32_t scope, unsigned depth, unsigned *steps) {
    py_ref_t none = {PY_REF_NONE, NULL, NULL, NULL};
    if (!chain)
        return none;
    size_t length = strcspn(chain, ".(");
    cbm_resolve_python_root_t root =
        cbm_resolve_binding_python_root(index, chain, length, scope, 0);
    const char *typed = cbm_arena_sprintf(arena, "%s()%s", chain, rest ? rest : "");
    if (!typed)
        return none;
    if (root.kind != CBM_PY_ROOT_ANNOTATION_IMPORT)
        return py_ref_of_chain(index, arena, imports, typed, scope, 0, depth, steps);
    const char *remainder = NULL;
    const char *key = imports->import_key(imports->context, arena, root.nested, typed, &remainder);
    return key ? (py_ref_t){PY_REF_KEY, key, remainder, NULL} : none;
}

static py_ref_t py_keep_ref(cbm_py_members_t *members, py_ref_t ref) {
    ref.name = py_keep(members, ref.name);
    ref.tail = py_keep(members, ref.tail ? ref.tail : "");
    if (ref.join) {
        py_join_t *join = cbm_arena_calloc(&members->arena, sizeof(*join));
        py_ref_t *arms = cbm_arena_calloc(&members->arena, ref.join->arm_count * sizeof(*arms));
        const char **tails =
            cbm_arena_calloc(&members->arena, (ref.join->self_count + 1) * sizeof(*tails));
        if (!join || !arms || !tails)
            return (py_ref_t){PY_REF_NONE, NULL, NULL, NULL};
        for (uint32_t i = 0; i < ref.join->arm_count; i++)
            arms[i] = py_keep_ref(members, ref.join->arms[i]);
        for (uint32_t i = 0; i < ref.join->self_count; i++)
            tails[i] = py_keep(members, ref.join->self_tails[i]);
        *join = (py_join_t){arms, ref.join->arm_count, tails, ref.join->self_count};
        ref.join = join;
    }
    return ref;
}

/* The value a chain denotes, kept across files. */
static py_ref_t py_capture(cbm_py_members_t *members, const cbm_resolve_binding_index_t *index,
                           CBMArena *arena, const cbm_py_import_lookup_t *imports,
                           const char *chain, uint32_t scope) {
    unsigned steps = 0;
    return py_keep_ref(members, py_ref_of_chain(index, arena, imports, chain, scope, 0, 0, &steps));
}

/* The instance a type annotation names. */
static py_ref_t py_capture_type(cbm_py_members_t *members, const cbm_resolve_binding_index_t *index,
                                CBMArena *arena, const cbm_py_import_lookup_t *imports,
                                const char *chain, uint32_t scope) {
    unsigned steps = 0;
    return py_keep_ref(members,
                       py_ref_of_type(index, arena, imports, chain, NULL, scope, 0, &steps));
}

/* ── Capture ─────────────────────────────────────────────────────────── */

static int py_compare_member(const void *left, const void *right) {
    return strcmp(((const py_member_t *)left)->name, ((const py_member_t *)right)->name);
}

static int py_compare_attribute(const void *left, const void *right) {
    return strcmp(((const py_attribute_t *)left)->name, ((const py_attribute_t *)right)->name);
}

/* The import path a decorator's root name came from, joined with its tail. */
static bool py_builtin_decorator(const cbm_resolve_binding_index_t *index, const char *chain,
                                 uint32_t scope, const char *spelling) {
    if (strcmp(chain, spelling))
        return false;
    return cbm_resolve_binding_python_root(index, chain, strlen(chain), scope, 0).kind ==
           CBM_PY_ROOT_UNBOUND;
}

/* `@name.setter` and friends rebind the same property. */
static bool py_property_accessor(const char *chain, const char *name) {
    size_t length = strlen(name);
    return !strncmp(chain, name, length) &&
           (!strcmp(chain + length, ".setter") || !strcmp(chain + length, ".getter") ||
            !strcmp(chain + length, ".deleter"));
}

/* Functions keep their identity through staticmethod and classmethod. A property
 * runs on read. Anything else rebinds the name to an unproven value. */
static py_member_kind_t py_member_kind(const cbm_resolve_binding_index_t *index,
                                       const cbm_resolve_python_member_t *member) {
    if (member->kind == CBM_PY_MEMBER_OPAQUE)
        return PY_MEMBER_OPAQUE;
    if (member->kind == CBM_PY_MEMBER_VALUE)
        return PY_MEMBER_VALUE;
    py_member_kind_t kind = PY_MEMBER_FUNCTION;
    for (const char *const *decorator = member->decorators; decorator && *decorator; decorator++) {
        if (py_builtin_decorator(index, *decorator, member->owner_scope, "staticmethod") ||
            py_builtin_decorator(index, *decorator, member->owner_scope, "classmethod"))
            continue;
        if (py_builtin_decorator(index, *decorator, member->owner_scope, "property") ||
            py_property_accessor(*decorator, member->name)) {
            kind = PY_MEMBER_PROPERTY;
            continue;
        }
        return PY_MEMBER_DECORATED;
    }
    return kind;
}

static py_class_t *py_class_for_scope(py_class_t **by_scope, size_t count, uint32_t scope) {
    return scope < count ? by_scope[scope] : NULL;
}

/* Sort a class's members and collapse duplicates: a getter with its setter stays
 * one property; any other repetition is not one provable declaration. */
static void py_finish_members(py_class_t *entry) {
    if (entry->member_count > 1)
        qsort(entry->members, entry->member_count, sizeof(*entry->members), py_compare_member);
    uint32_t used = 0;
    for (uint32_t i = 0; i < entry->member_count; i++) {
        py_member_t *member = &entry->members[i];
        if (used && !strcmp(entry->members[used - 1].name, member->name)) {
            py_member_t *prior = &entry->members[used - 1];
            if (prior->kind != PY_MEMBER_PROPERTY || member->kind != PY_MEMBER_PROPERTY)
                prior->kind = PY_MEMBER_OPAQUE;
            continue;
        }
        entry->members[used++] = *member;
    }
    entry->member_count = used;
    if (entry->attribute_count > 1)
        qsort(entry->attributes, entry->attribute_count, sizeof(*entry->attributes),
              py_compare_attribute);
}

/* A function's annotated return, returned values and decorator shape. Two defs of
 * one qualified name prove none of them. */
static bool py_note_function(cbm_py_members_t *members, const cbm_resolve_binding_index_t *index,
                             CBMArena *scratch, const cbm_py_import_lookup_t *imports,
                             const cbm_resolve_python_member_t *member) {
    py_function_t *known = cbm_ht_get(members->functions, member->target);
    if (known) {
        *known = (py_function_t){.result = {PY_REF_NONE, NULL, NULL, NULL}, .inference = 2};
        return true;
    }
    py_function_t *function = cbm_arena_calloc(&members->arena, sizeof(*function));
    const char *key = py_keep(members, member->target);
    if (!function || !key)
        return false;
    function->result = py_capture_type(members, index, scratch, imports, member->return_chain,
                                       member->chain_scope);
    function->shape = member->shape;
    function->delegate_applied = member->delegate_applied;
    function->delegate =
        py_capture(members, index, scratch, imports, member->delegate_chain, member->chain_scope);
    uint32_t count = 0;
    while (member->returned && member->returned[count])
        count++;
    if (count) {
        function->returned = cbm_arena_calloc(&members->arena, count * sizeof(*function->returned));
        if (!function->returned)
            return false;
        for (uint32_t i = 0; i < count; i++)
            function->returned[i] = py_capture(members, index, scratch, imports,
                                               member->returned[i], member->returned_scope);
        function->returned_count = count;
    }
    cbm_ht_set(members->functions, key, function);
    return cbm_ht_get(members->functions, key) == function;
}

bool cbm_py_members_add_file(cbm_py_members_t *members, const cbm_resolve_binding_index_t *index,
                             const cbm_py_import_lookup_t *imports) {
    size_t classes = cbm_resolve_binding_python_class_count(index);
    size_t declared = cbm_resolve_binding_python_member_count(index);
    CBMArena scratch;
    cbm_arena_init(&scratch);
    size_t highest = 0;
    for (size_t c = 0; c < classes; c++) {
        uint32_t scope = cbm_resolve_binding_python_class_at(index, c)->scope;
        if (scope >= highest)
            highest = (size_t)scope + 1;
    }
    py_class_t **by_scope =
        highest ? cbm_arena_calloc(&scratch, highest * sizeof(*by_scope)) : NULL;
    bool ok = !highest || by_scope;
    uint32_t *member_room =
        highest ? cbm_arena_calloc(&scratch, highest * sizeof(*member_room)) : NULL;
    uint32_t *attribute_room =
        highest ? cbm_arena_calloc(&scratch, highest * sizeof(*attribute_room)) : NULL;
    ok = ok && (!highest || (member_room && attribute_room));
    for (size_t m = 0; ok && m < declared; m++) {
        const cbm_resolve_python_member_t *member = cbm_resolve_binding_python_member_at(index, m);
        if (member->owner_scope >= highest)
            continue;
        if (member->kind == CBM_PY_MEMBER_INSTANCE)
            attribute_room[member->owner_scope]++;
        else
            member_room[member->owner_scope]++;
    }
    for (size_t c = 0; ok && c < classes; c++) {
        const cbm_resolve_python_class_t *source = cbm_resolve_binding_python_class_at(index, c);
        py_class_t *entry = cbm_ht_get(members->classes, source->qualified_name);
        if (entry) {
            /* Two declarations of one qualified name: neither is the class. */
            entry->duplicate = true;
            continue;
        }
        entry = cbm_arena_calloc(&members->arena, sizeof(*entry));
        const char *key = py_keep(members, source->qualified_name);
        if (!entry || !key) {
            ok = false;
            break;
        }
        entry->qualified_name = key;
        entry->unknown_base = source->unknown_base;
        entry->subscripted = source->subscripted;
        entry->metaclass = source->metaclass;
        entry->base_count = source->base_count;
        entry->bases =
            source->base_count
                ? cbm_arena_calloc(&members->arena, source->base_count * sizeof(*entry->bases))
                : NULL;
        uint32_t room = member_room[source->scope], attributes = attribute_room[source->scope];
        entry->members =
            room ? cbm_arena_calloc(&members->arena, room * sizeof(*entry->members)) : NULL;
        entry->attributes =
            attributes ? cbm_arena_calloc(&members->arena, attributes * sizeof(*entry->attributes))
                       : NULL;
        if ((source->base_count && !entry->bases) || (room && !entry->members) ||
            (attributes && !entry->attributes)) {
            ok = false;
            break;
        }
        for (uint32_t b = 0; b < source->base_count; b++)
            entry->bases[b] = py_capture(members, index, &scratch, imports, source->bases[b],
                                         source->header_scope);
        cbm_ht_set(members->classes, key, entry);
        if (cbm_ht_get(members->classes, key) != entry) {
            ok = false;
            break;
        }
        by_scope[source->scope] = entry;
    }
    for (size_t m = 0; ok && m < declared; m++) {
        const cbm_resolve_python_member_t *member = cbm_resolve_binding_python_member_at(index, m);
        py_class_t *entry = py_class_for_scope(by_scope, highest, member->owner_scope);
        if (!entry) {
            /* A function outside any class: its annotated return and decorator
             * shape are all callers need. Two defs of one name prove neither. */
            ok = py_note_function(members, index, &scratch, imports, member);
            continue;
        }
        if (member->kind == CBM_PY_MEMBER_INSTANCE) {
            py_attribute_t *attribute = &entry->attributes[entry->attribute_count++];
            attribute->name = py_keep(members, member->name);
            attribute->none = member->none_value;
            attribute->value = member->type_chain
                                   ? py_capture_type(members, index, &scratch, imports,
                                                     member->type_chain, member->chain_scope)
                                   : py_capture(members, index, &scratch, imports,
                                                member->value_chain, member->chain_scope);
            continue;
        }
        py_member_t *slot = &entry->members[entry->member_count++];
        slot->name = py_keep(members, member->name);
        slot->target = py_keep(members, member->target);
        slot->kind = py_member_kind(index, member);
        slot->none_default = member->kind == CBM_PY_MEMBER_VALUE && member->none_value;
        slot->annotated = member->type_chain != NULL;
        slot->value = member->kind == CBM_PY_MEMBER_FUNCTION
                          ? py_capture_type(members, index, &scratch, imports, member->return_chain,
                                            member->chain_scope)
                      : member->type_chain
                          ? py_capture_type(members, index, &scratch, imports, member->type_chain,
                                            member->chain_scope)
                          : py_capture(members, index, &scratch, imports, member->value_chain,
                                       member->chain_scope);
        if (!slot->name || !slot->target) {
            ok = false;
            break;
        }
        if (member->kind == CBM_PY_MEMBER_FUNCTION)
            ok = py_note_function(members, index, &scratch, imports, member);
    }
    for (size_t c = 0; ok && c < classes; c++) {
        const cbm_resolve_python_class_t *source = cbm_resolve_binding_python_class_at(index, c);
        py_class_t *entry = by_scope[source->scope];
        if (entry)
            py_finish_members(entry);
    }
    size_t decorated = ok ? cbm_resolve_binding_python_decorated_count(index) : 0;
    for (size_t d = 0; ok && d < decorated; d++) {
        const cbm_resolve_python_decorated_t *item =
            cbm_resolve_binding_python_decorated_at(index, d);
        py_decorated_t *entry = cbm_arena_calloc(&members->arena, sizeof(*entry));
        uint32_t count = 0;
        while (item->decorators[count])
            count++;
        py_decorator_t *decorators =
            count ? cbm_arena_calloc(&members->arena, count * sizeof(*decorators)) : NULL;
        if (!entry || (count && !decorators) || !item->target) {
            ok = !item->target;
            continue;
        }
        for (uint32_t i = 0; i < count; i++) {
            const char *chain = item->decorators[i];
            size_t length = strlen(chain);
            bool applied = length > 2 && !strcmp(chain + length - 2, "()");
            const char *bare = applied ? cbm_arena_strndup(&scratch, chain, length - 2) : chain;
            decorators[i].applied = applied;
            /* The written form says applied or bare; the name is what stdlib facts key on. */
            const char *imported = item->canonical[i];
            size_t spelled = imported ? strlen(imported) : 0;
            decorators[i].canonical =
                py_keep(members, applied && spelled > 2 && !strcmp(imported + spelled - 2, "()")
                                     ? cbm_arena_strndup(&scratch, imported, spelled - 2)
                                     : imported);
            decorators[i].ref = strcmp(chain, "?") ? py_capture(members, index, &scratch, imports,
                                                                bare, item->scope)
                                                   : (py_ref_t){PY_REF_NONE, NULL, NULL, NULL};
        }
        *entry = (py_decorated_t){.target = py_keep(members, item->target),
                                  .decorators = decorators,
                                  .count = count,
                                  .is_class = item->is_class,
                                  .next = members->decorated};
        members->decorated = entry;
        cbm_ht_set(members->rebound, entry->target, entry);
    }
    /* A module-level variable bound once holds that instance everywhere. */
    size_t names = ok ? cbm_resolve_binding_python_namespace_size(index) : 0;
    for (size_t n = 0; ok && n < names; n++) {
        const char *name = cbm_resolve_binding_python_namespace_name(index, n, NULL);
        if (!name)
            continue;
        cbm_resolve_python_root_t root =
            cbm_resolve_binding_python_root(index, name, strlen(name), 0, 0);
        if (root.kind != CBM_PY_ROOT_MODULE_VALUE || !root.chain || !root.target ||
            cbm_ht_get(members->values, root.target))
            continue;
        py_ref_t *ref = cbm_arena_alloc(&members->arena, sizeof(*ref));
        const char *key = py_keep(members, root.target);
        if (!ref || !key) {
            ok = false;
            break;
        }
        *ref = root.value_typed ? py_capture_type(members, index, &scratch, imports, root.chain, 0)
                                : py_capture(members, index, &scratch, imports, root.chain, 0);
        cbm_ht_set(members->values, key, ref);
    }
    cbm_arena_destroy(&scratch);
    return ok;
}

/* ── Values ──────────────────────────────────────────────────────────── */

typedef enum {
    PY_VALUE_UNKNOWN = 0,
    PY_VALUE_MODULE,
    PY_VALUE_CLASS,
    PY_VALUE_INSTANCE,
    PY_VALUE_SUPER,
    PY_VALUE_FUNCTION,
    PY_VALUE_EXTERNAL
} py_value_kind_t;

typedef struct {
    py_value_kind_t kind;
    const py_class_t *cls;
    const char *qualified_name;
    const py_ref_t *result;  /* annotated return of a function */
    const char *declaration; /* the member read last, for a reference to it */
    const char *owner;
} py_value_t;

typedef struct {
    const cbm_py_members_t *members;
    const cbm_py_members_env_t *env;
    CBMArena *arena;
    unsigned *steps;
} py_eval_t;

static py_value_t py_eval_ref(const py_eval_t *eval, const py_ref_t *ref, unsigned depth);

static const py_class_t *py_class(const cbm_py_members_t *members, const char *qualified_name) {
    const py_class_t *entry = qualified_name ? cbm_ht_get(members->classes, qualified_name) : NULL;
    return entry && !entry->duplicate ? entry : NULL;
}

static const py_ref_t *py_function_result(const cbm_py_members_t *members,
                                          const char *qualified_name) {
    const py_function_t *function = cbm_ht_get(members->functions, qualified_name);
    return function ? &function->result : NULL;
}

static py_value_t py_value_of_declaration(const py_eval_t *eval, const char *qualified_name,
                                          unsigned depth) {
    py_value_t value = {.kind = PY_VALUE_UNKNOWN};
    /* A decorator that does not keep the identity rebound this name to its result. */
    if (eval->members->decided && cbm_ht_get(eval->members->rebound, qualified_name) &&
        !cbm_ht_get(eval->members->kept, qualified_name))
        return value;
    const py_class_t *cls = py_class(eval->members, qualified_name);
    if (cls)
        return (py_value_t){.kind = PY_VALUE_CLASS, .cls = cls};
    if (cbm_ht_get(eval->members->classes, qualified_name))
        return value; /* duplicate class declarations */
    const py_ref_t *variable = cbm_ht_get(eval->members->values, qualified_name);
    if (variable)
        return depth > PY_MAX_DEPTH ? value : py_eval_ref(eval, variable, depth + 1);
    const char *label = eval->env->label(eval->env->context, qualified_name);
    if (label && (!strcmp(label, "Function") || !strcmp(label, "Method")))
        return (py_value_t){.kind = PY_VALUE_FUNCTION,
                            .qualified_name = qualified_name,
                            .result = py_function_result(eval->members, qualified_name)};
    return value;
}

static const py_member_t *py_own_member(const py_class_t *cls, const char *name) {
    if (!cls->member_count)
        return NULL;
    py_member_t key = {.name = name};
    return bsearch(&key, cls->members, cls->member_count, sizeof(*cls->members), py_compare_member);
}

/* A leading double underscore mangles to the class it is written in, so such a
 * name is only ever the writing class's own member. */
static bool py_mangled(const char *name) {
    size_t length = strlen(name);
    return length > 2 && name[0] == '_' && name[1] == '_' &&
           !(length > 4 && name[length - 1] == '_' && name[length - 2] == '_');
}

/* First declaration in the certain part of the linearization. A class that
 * intercepts every attribute read cannot be read through statically. */
static const py_member_t *py_lookup(const py_class_t *cls, const char *name, uint32_t first,
                                    const py_class_t **owner) {
    if (!cls->sealed || cls->intercepts)
        return NULL;
    if (py_mangled(name)) {
        const py_member_t *own = first ? NULL : py_own_member(cls, name);
        *owner = cls;
        return own;
    }
    for (uint32_t i = first; i < cls->reliable; i++) {
        const py_member_t *member = py_own_member(cls->mro[i], name);
        if (member) {
            *owner = cls->mro[i];
            return member;
        }
    }
    return NULL;
}

/* What instances store under this name, across the certain linearization. All
 * typed stores must agree; a None store says nothing about the class. */
static bool py_instance_attribute(const py_eval_t *eval, const py_class_t *cls, const char *name,
                                  py_value_t *out, unsigned depth) {
    bool present = false, unknown = false;
    py_value_t agreed = {.kind = PY_VALUE_UNKNOWN};
    for (uint32_t i = 0; i < cls->reliable && !unknown; i++) {
        const py_class_t *owner = cls->mro[i];
        py_attribute_t key = {.name = name};
        const py_attribute_t *found =
            owner->attribute_count ? bsearch(&key, owner->attributes, owner->attribute_count,
                                             sizeof(*owner->attributes), py_compare_attribute)
                                   : NULL;
        if (!found)
            continue;
        present = true;
        while (found > owner->attributes && !strcmp(found[-1].name, name))
            found--;
        for (; found < owner->attributes + owner->attribute_count && !strcmp(found->name, name);
             found++) {
            if (found->none)
                continue;
            py_value_t value = py_eval_ref(eval, &found->value, depth + 1);
            if (value.kind != PY_VALUE_INSTANCE ||
                (agreed.kind != PY_VALUE_UNKNOWN && agreed.cls != value.cls)) {
                unknown = true;
                break;
            }
            agreed = value;
        }
    }
    *out = unknown ? (py_value_t){.kind = PY_VALUE_UNKNOWN} : agreed;
    return present;
}

/* A metaclass or __init_subclass__ can replace what a class body assigned: a Django field
 * reads as its column value, an Enum member as the enum. A base we cannot resolve at all
 * could be anything. An external class is taken to keep it, as unittest.TestCase keeps
 * rf = RequestFactory(); the known exceptions from the stdlib count as a metaclass. */
static bool py_class_body_rewritable(const py_class_t *cls) {
    if (cls->uncertain)
        return true;
    for (uint32_t i = 0; i < cls->mro_count; i++) {
        const py_class_t *entry = cls->mro[i];
        if (entry->metaclass || py_own_member(entry, "__init_subclass__"))
            return true;
    }
    return false;
}

static py_value_t py_read_member(const py_eval_t *eval, const py_value_t *value, const char *name,
                                 unsigned depth) {
    py_value_t unknown = {.kind = PY_VALUE_UNKNOWN};
    const py_class_t *cls = value->cls;
    if (value->kind == PY_VALUE_INSTANCE && !strcmp(name, "__class__"))
        return (py_value_t){.kind = PY_VALUE_CLASS, .cls = cls};
    if (value->kind == PY_VALUE_CLASS && !strcmp(name, "<super>"))
        return (py_value_t){.kind = PY_VALUE_SUPER, .cls = cls};
    const py_class_t *owner = NULL;
    const py_member_t *member = py_lookup(cls, name, value->kind == PY_VALUE_SUPER ? 1 : 0, &owner);
    if (value->kind == PY_VALUE_INSTANCE) {
        py_value_t stored;
        if (py_instance_attribute(eval, cls, name, &stored, depth)) {
            if (member && member->kind != PY_MEMBER_PROPERTY && !member->none_default)
                return unknown;
            if (!member || member->none_default)
                return stored;
        }
    }
    if (!member) {
        /* A class declared inside this class has its own table entry. */
        const py_class_t *nested = py_class(
            eval->members, cbm_arena_sprintf(eval->arena, "%s.%s", cls->qualified_name, name));
        return nested ? (py_value_t){.kind = PY_VALUE_CLASS, .cls = nested} : unknown;
    }
    switch (member->kind) {
    case PY_MEMBER_FUNCTION:
        return (py_value_t){.kind = PY_VALUE_FUNCTION,
                            .qualified_name = member->target,
                            .result = &member->value,
                            .declaration = member->target,
                            .owner = owner->qualified_name};
    case PY_MEMBER_PROPERTY: {
        py_value_t read = value->kind == PY_VALUE_INSTANCE || value->kind == PY_VALUE_SUPER
                              ? py_eval_ref(eval, &member->value, depth + 1)
                              : unknown;
        read.declaration = member->target;
        read.owner = owner->qualified_name;
        return read;
    }
    case PY_MEMBER_VALUE: {
        py_value_t read = py_eval_ref(eval, &member->value, depth + 1);
        /* An annotation states what a read yields, whatever the machinery does. */
        return read.kind == PY_VALUE_INSTANCE && !member->annotated && py_class_body_rewritable(cls)
                   ? unknown
                   : read;
    }
    case PY_MEMBER_DECORATED:
    case PY_MEMBER_OPAQUE:
        return unknown;
    }
    return unknown;
}

/* Settle what an unannotated function returns: every returned value must be an
 * instance of one class. Runs once for each function, during sealing, so workers
 * only read the answer; a recursive path through a function still being settled
 * proves nothing. */
static void py_infer_returns(const py_eval_t *eval, py_function_t *function, unsigned depth) {
    function->inference = 1;
    const py_class_t *agreed = NULL;
    bool known = function->returned_count > 0 && depth <= PY_MAX_DEPTH;
    for (uint32_t i = 0; known && i < function->returned_count; i++) {
        py_value_t value = py_eval_ref(eval, &function->returned[i], depth + 1);
        known = value.kind == PY_VALUE_INSTANCE && (!agreed || agreed == value.cls);
        agreed = value.cls;
    }
    function->inferred = known ? agreed : NULL;
    function->inference = 2;
}

static py_value_t py_call(const py_eval_t *eval, const py_value_t *value, unsigned depth) {
    py_value_t unknown = {.kind = PY_VALUE_UNKNOWN};
    switch (value->kind) {
    case PY_VALUE_CLASS:
        /* __new__ can return anything. */
        return value->cls->custom_new ? unknown
                                      : (py_value_t){.kind = PY_VALUE_INSTANCE, .cls = value->cls};
    case PY_VALUE_FUNCTION: {
        if (value->result && value->result->kind != PY_REF_NONE)
            return py_eval_ref(eval, value->result, depth + 1);
        py_function_t *function = cbm_ht_get(eval->members->functions, value->qualified_name);
        if (function && function->inference == 0)
            py_infer_returns(eval, function, depth);
        return function && function->inferred
                   ? (py_value_t){.kind = PY_VALUE_INSTANCE, .cls = function->inferred}
                   : unknown;
    }
    case PY_VALUE_INSTANCE: {
        const py_class_t *owner = NULL;
        const py_member_t *call = py_lookup(value->cls, "__call__", 0, &owner);
        return call && call->kind == PY_MEMBER_FUNCTION ? py_eval_ref(eval, &call->value, depth + 1)
                                                        : unknown;
    }
    default:
        return unknown;
    }
}

static py_value_t py_apply_tail(const py_eval_t *eval, py_value_t value, const char *tail,
                                unsigned depth) {
    py_value_t unknown = {.kind = PY_VALUE_UNKNOWN};
    while (tail && *tail) {
        if (tail[0] == '(' && tail[1] == ')') {
            value = py_call(eval, &value, depth);
            tail += 2;
            continue;
        }
        if (tail[0] != '.')
            return unknown;
        tail++;
        size_t length = strcspn(tail, ".(");
        const char *name = cbm_arena_strndup(eval->arena, tail, length);
        if (!name)
            return unknown;
        tail += length;
        switch (value.kind) {
        case PY_VALUE_MODULE: {
            const char *target = NULL;
            bool module = false;
            if (!eval->env->module_member(eval->env->context, eval->arena, value.qualified_name,
                                          name, &target, &module))
                return unknown;
            value = module ? (py_value_t){.kind = PY_VALUE_MODULE, .qualified_name = target}
                           : py_value_of_declaration(eval, target, depth + 1);
            break;
        }
        case PY_VALUE_CLASS:
        case PY_VALUE_INSTANCE:
        case PY_VALUE_SUPER:
            value = py_read_member(eval, &value, name, depth);
            break;
        case PY_VALUE_EXTERNAL:
            break;
        default:
            return unknown;
        }
    }
    return value;
}

static py_value_t py_eval_ref(const py_eval_t *eval, const py_ref_t *ref, unsigned depth) {
    py_value_t value = {.kind = PY_VALUE_UNKNOWN};
    if (!ref || depth > PY_MAX_DEPTH || ++*eval->steps > PY_MAX_EVAL_STEPS)
        return value;
    switch (ref->kind) {
    case PY_REF_KEY: {
        const char *target = NULL;
        bool module = false, external = false;
        if (!eval->env->import_value(eval->env->context, ref->name, &target, &module, &external))
            return external ? (py_value_t){.kind = PY_VALUE_EXTERNAL} : value;
        value = module ? (py_value_t){.kind = PY_VALUE_MODULE, .qualified_name = target}
                       : py_value_of_declaration(eval, target, depth + 1);
        break;
    }
    case PY_REF_QN:
        value = py_value_of_declaration(eval, ref->name, depth + 1);
        break;
    case PY_REF_SELF:
    case PY_REF_CLASS:
    case PY_REF_SUPER: {
        const py_class_t *cls = py_class(eval->members, ref->name);
        if (!cls)
            return value;
        value = (py_value_t){.kind = ref->kind == PY_REF_SELF    ? PY_VALUE_INSTANCE
                                     : ref->kind == PY_REF_CLASS ? PY_VALUE_CLASS
                                                                 : PY_VALUE_SUPER,
                             .cls = cls};
        break;
    }
    case PY_REF_JOIN: {
        const py_class_t *agreed = NULL;
        for (uint32_t i = 0; i < ref->join->arm_count; i++) {
            py_value_t arm = py_eval_ref(eval, &ref->join->arms[i], depth + 1);
            if (arm.kind != PY_VALUE_INSTANCE || (agreed && agreed != arm.cls))
                return value;
            agreed = arm.cls;
        }
        value = (py_value_t){.kind = PY_VALUE_INSTANCE, .cls = agreed};
        for (uint32_t i = 0; i < ref->join->self_count; i++) {
            py_value_t step = py_apply_tail(eval, value, ref->join->self_tails[i], depth + 1);
            if (step.kind != PY_VALUE_INSTANCE || step.cls != agreed)
                return (py_value_t){.kind = PY_VALUE_UNKNOWN};
        }
        break;
    }
    case PY_REF_UNBOUND:
    case PY_REF_NONE:
        return value;
    }
    return py_apply_tail(eval, value, ref->tail, depth + 1);
}

/* ── Decorators ──────────────────────────────────────────────────────── */

typedef enum { PY_KEEP_NONE = 0, PY_KEEP_IDENTITY, PY_KEEP_PROPERTY } py_keep_t;

/* Standard library decorators whose result is the function or class itself, or a
 * thin wrapper that calls it. Facts about those modules, not about this project. */
static bool py_stdlib_keeps_identity(const char *canonical, bool applied) {
    static const char *const direct[] = {"abc.abstractmethod",
                                         "typing.final",
                                         "typing.override",
                                         "typing.no_type_check",
                                         "typing_extensions.final",
                                         "typing_extensions.override",
                                         "functools.lru_cache",
                                         "functools.cache",
                                         "functools.total_ordering",
                                         "contextlib.contextmanager",
                                         "contextlib.asynccontextmanager",
                                         "dataclasses.dataclass",
                                         "enum.unique",
                                         "typing.runtime_checkable",
                                         "atexit.register",
                                         "typing_extensions.runtime_checkable",
                                         NULL};
    static const char *const called[] = {"functools.lru_cache", "functools.wraps",
                                         "dataclasses.dataclass", NULL};
    for (const char *const *name = applied ? called : direct; *name; name++)
        if (!strcmp(*name, canonical))
            return true;
    return false;
}

static bool py_stdlib_property(const char *canonical) {
    return !strcmp(canonical, "functools.cached_property") ||
           !strcmp(canonical, "abc.abstractproperty");
}

/* The source proves it keeps identity in this form, or hands the function on to a
 * decorator that does. */
static bool py_function_keeps(const py_eval_t *eval, const char *qualified_name, bool applied,
                              unsigned depth) {
    const py_function_t *function = cbm_ht_get(eval->members->functions, qualified_name);
    if (!function || depth > 4)
        return false;
    if (function->shape & (applied ? CBM_PY_SHAPE_FACTORY : CBM_PY_SHAPE_DIRECT))
        return true;
    if (applied || !(function->shape & CBM_PY_SHAPE_DELEGATES))
        return false;
    py_value_t next = py_eval_ref(eval, &function->delegate, depth + 1);
    return next.kind == PY_VALUE_FUNCTION &&
           py_function_keeps(eval, next.qualified_name, function->delegate_applied, depth + 1);
}

static py_keep_t py_decorator_verdict(const py_eval_t *eval, const py_decorator_t *decorator) {
    py_ref_t bare = decorator->ref;
    if (bare.kind == PY_REF_UNBOUND && (!bare.tail || !bare.tail[0])) {
        if (decorator->applied)
            return PY_KEEP_NONE;
        if (!strcmp(bare.name, "staticmethod") || !strcmp(bare.name, "classmethod"))
            return PY_KEEP_IDENTITY;
        return !strcmp(bare.name, "property") ? PY_KEEP_PROPERTY : PY_KEEP_NONE;
    }
    py_value_t value = py_eval_ref(eval, &bare, 0);
    if (value.kind == PY_VALUE_EXTERNAL && decorator->canonical) {
        if (py_stdlib_keeps_identity(decorator->canonical, decorator->applied))
            return PY_KEEP_IDENTITY;
        return !decorator->applied && py_stdlib_property(decorator->canonical) ? PY_KEEP_PROPERTY
                                                                               : PY_KEEP_NONE;
    }
    if (value.kind == PY_VALUE_FUNCTION)
        return py_function_keeps(eval, value.qualified_name, decorator->applied, 0)
                   ? PY_KEEP_IDENTITY
                   : PY_KEEP_NONE;
    /* A classmethod or staticmethod subclass binds the same function. */
    for (uint32_t i = 0;
         value.kind == PY_VALUE_CLASS && !decorator->applied && i < value.cls->base_count; i++) {
        const py_ref_t *base = &value.cls->bases[i];
        if (base->kind == PY_REF_UNBOUND && (!base->tail || !base->tail[0]) &&
            (!strcmp(base->name, "classmethod") || !strcmp(base->name, "staticmethod")))
            return PY_KEEP_IDENTITY;
    }
    /* A descriptor class: `@cached_property` reads run the function once. */
    if (value.kind == PY_VALUE_CLASS && !decorator->applied && py_own_member(value.cls, "__get__"))
        return PY_KEEP_PROPERTY;
    return PY_KEEP_NONE;
}

bool cbm_py_members_decide_decorated(cbm_py_members_t *members, const cbm_py_members_env_t *env,
                                     cbm_py_keep_fn keep, void *context) {
    CBMArena scratch;
    cbm_arena_init(&scratch);
    unsigned steps = 0;
    py_eval_t eval = {.members = members, .env = env, .arena = &scratch, .steps = &steps};
    bool ok = true;
    for (py_decorated_t *item = members->decorated; ok && item; item = item->next) {
        py_keep_t verdict = PY_KEEP_IDENTITY;
        steps = 0;
        for (uint32_t i = 0; i < item->count && verdict != PY_KEEP_NONE; i++) {
            py_keep_t one = py_decorator_verdict(&eval, &item->decorators[i]);
            verdict = one == PY_KEEP_NONE       ? PY_KEEP_NONE
                      : one == PY_KEEP_PROPERTY ? PY_KEEP_PROPERTY
                                                : verdict;
        }
        if (verdict == PY_KEEP_NONE)
            continue;
        cbm_ht_set(members->kept, item->target, (void *)(uintptr_t)verdict);
        ok = cbm_ht_get(members->kept, item->target) == (void *)(uintptr_t)verdict;
        if (ok && verdict == PY_KEEP_IDENTITY)
            ok = keep(context, item->target, item->is_class);
    }
    cbm_arena_destroy(&scratch);
    members->decided = true;
    return ok;
}

void cbm_py_members_apply_decorated(const cbm_py_members_t *members,
                                    cbm_resolve_binding_index_t *index) {
    size_t count = members ? cbm_resolve_binding_python_decorated_count(index) : 0;
    for (size_t d = 0; d < count; d++) {
        const cbm_resolve_python_decorated_t *item =
            cbm_resolve_binding_python_decorated_at(index, d);
        if (item->target && (uintptr_t)cbm_ht_get(members->kept, item->target) == PY_KEEP_IDENTITY)
            cbm_resolve_binding_python_keep_decorated(index, item);
    }
}

/* Decided declarations now have their final member kinds. */
static void py_settle_member_kinds(cbm_py_members_t *members, py_class_t *cls) {
    for (uint32_t i = 0; i < cls->member_count; i++) {
        py_member_t *member = &cls->members[i];
        if (member->kind != PY_MEMBER_DECORATED)
            continue;
        uintptr_t verdict = (uintptr_t)cbm_ht_get(members->kept, member->target);
        member->kind = verdict == PY_KEEP_IDENTITY   ? PY_MEMBER_FUNCTION
                       : verdict == PY_KEEP_PROPERTY ? PY_MEMBER_PROPERTY
                                                     : PY_MEMBER_OPAQUE;
    }
}

/* ── Linearization ───────────────────────────────────────────────────── */

/* Classes whose ancestry stays out of project code. Being sealed is what lets a
 * lookup trust the project classes linearized before them. */
static bool py_builtin_class(const char *name) {
    static const char *const names[] = {"object",
                                        "type",
                                        "int",
                                        "float",
                                        "complex",
                                        "bool",
                                        "str",
                                        "bytes",
                                        "bytearray",
                                        "list",
                                        "tuple",
                                        "dict",
                                        "set",
                                        "frozenset",
                                        "property",
                                        "classmethod",
                                        "staticmethod",
                                        "BaseException",
                                        "Exception",
                                        "ArithmeticError",
                                        "AssertionError",
                                        "AttributeError",
                                        "BufferError",
                                        "EOFError",
                                        "ImportError",
                                        "IndexError",
                                        "KeyError",
                                        "LookupError",
                                        "MemoryError",
                                        "NameError",
                                        "OSError",
                                        "IOError",
                                        "RuntimeError",
                                        "NotImplementedError",
                                        "StopIteration",
                                        "SyntaxError",
                                        "SystemError",
                                        "TypeError",
                                        "UnicodeError",
                                        "ValueError",
                                        "Warning",
                                        "DeprecationWarning",
                                        "RuntimeWarning",
                                        "UserWarning",
                                        "ZeroDivisionError",
                                        "ConnectionError",
                                        "TimeoutError",
                                        "FileNotFoundError",
                                        "PermissionError",
                                        "KeyboardInterrupt",
                                        "SystemExit",
                                        NULL};
    for (size_t i = 0; names[i]; i++)
        if (!strcmp(names[i], name))
            return true;
    return false;
}

static py_class_t *py_opaque_class(cbm_py_members_t *members, const char *spelling) {
    py_class_t *entry = cbm_ht_get(members->opaque, spelling);
    if (entry)
        return entry;
    entry = cbm_arena_calloc(&members->arena, sizeof(*entry));
    const char *key = py_keep(members, spelling);
    if (!entry || !key)
        return NULL;
    entry->qualified_name = key;
    entry->opaque = true;
    entry->sealed = true;
    cbm_ht_set(members->opaque, key, entry);
    return cbm_ht_get(members->opaque, key) == entry ? entry : NULL;
}

/* The C3 merge. False when no consistent order exists, which Python itself
 * rejects at class creation. Opaque classes take part as single entries. */
static bool py_c3_merge(CBMArena *arena, const py_class_t *cls, const py_class_t ***lists,
                        uint32_t *counts, uint32_t list_count, const py_class_t ***out,
                        uint32_t *out_count) {
    size_t total = 1;
    for (uint32_t i = 0; i < list_count; i++)
        total += counts[i];
    if (total > PY_MAX_MRO)
        return false;
    const py_class_t **result = cbm_arena_alloc(arena, total * sizeof(*result));
    uint32_t *heads = cbm_arena_calloc(arena, (list_count + 1) * sizeof(*heads));
    if (!result || !heads)
        return false;
    uint32_t used = 0;
    result[used++] = cls;
    for (;;) {
        bool remaining = false;
        const py_class_t *chosen = NULL;
        for (uint32_t i = 0; i < list_count && !chosen; i++) {
            if (heads[i] >= counts[i])
                continue;
            remaining = true;
            const py_class_t *candidate = lists[i][heads[i]];
            bool in_tail = false;
            for (uint32_t j = 0; j < list_count && !in_tail; j++)
                for (uint32_t k = heads[j] + 1; k < counts[j] && !in_tail; k++)
                    in_tail = lists[j][k] == candidate;
            if (!in_tail)
                chosen = candidate;
        }
        if (!remaining)
            break;
        if (!chosen)
            return false;
        result[used++] = chosen;
        for (uint32_t i = 0; i < list_count; i++)
            if (heads[i] < counts[i] && lists[i][heads[i]] == chosen)
                heads[i]++;
    }
    *out = result;
    *out_count = used;
    return true;
}

/* Resolve one written base. Sets *known to a project class, or returns an opaque
 * entry for one that cannot reach project code. Otherwise *unsealed. */
static const py_class_t *py_resolve_base(const py_eval_t *eval, cbm_py_members_t *members,
                                         const py_ref_t *base, bool *skip, bool *unsealed) {
    *skip = false;
    if (base->kind == PY_REF_UNBOUND && (!base->tail || !base->tail[0])) {
        if (!strcmp(base->name, "object")) {
            *skip = true;
            return NULL;
        }
        if (py_builtin_class(base->name))
            return py_opaque_class(members, base->name);
    }
    py_value_t value = py_eval_ref(eval, base, 0);
    if (value.kind == PY_VALUE_CLASS)
        return value.cls;
    if (value.kind == PY_VALUE_EXTERNAL && base->kind == PY_REF_KEY)
        return py_opaque_class(members, base->name);
    *unsealed = true;
    return NULL;
}

static void py_seal_class(const py_eval_t *eval, cbm_py_members_t *members, py_class_t *cls,
                          unsigned depth);

static void py_seal_fallback(py_class_t *cls, CBMArena *arena) {
    /* Only the class's own table is certain. */
    const py_class_t **mro = cbm_arena_alloc(arena, sizeof(*mro));
    if (mro) {
        mro[0] = cls;
        cls->mro = mro;
        cls->mro_count = 1;
        cls->reliable = 1;
    }
    cls->sealed = mro != NULL;
}

/* When an ancestor's bases are unknown, its ancestry can reorder the rest. A
 * single-inheritance prefix keeps its place regardless, and so does the first written
 * base behind it: C3 puts that directly after its class, or refuses the class. What
 * follows a first base of several can be overtaken, by a subclass of its parent. */
static uint32_t py_single_chain(const py_class_t *cls, const py_class_t **mro, uint32_t count) {
    uint32_t reliable = 1;
    const py_class_t *current = cls;
    while (reliable < count && !current->opaque && !current->unknown_base && current->first &&
           mro[reliable] == current->first && !mro[reliable]->opaque) {
        uint32_t written = current->base_count;
        current = mro[reliable++];
        if (written != 1)
            break;
    }
    return reliable;
}

/* Base[int] asks the class, and a __class_getitem__ can answer with anything. */
static bool py_answers_subscript(const py_class_t *cls) {
    for (uint32_t i = 0; i < cls->mro_count; i++)
        if (py_own_member(cls->mro[i], "__class_getitem__"))
            return true;
    return false;
}

static void py_seal_class(const py_eval_t *eval, cbm_py_members_t *members, py_class_t *cls,
                          unsigned depth) {
    if (cls->sealed)
        return;
    if (cls->visiting || depth > PY_MAX_DEPTH) {
        /* Python rejects an inheritance cycle at runtime; stay with what is written. */
        return;
    }
    cls->visiting = true;
    CBMArena *arena = &members->arena;
    uint32_t written = cls->base_count;
    const py_class_t ***lists = cbm_arena_calloc(arena, (written + 1) * sizeof(*lists));
    uint32_t *counts = cbm_arena_calloc(arena, (written + 1) * sizeof(*counts));
    const py_class_t **direct = written ? cbm_arena_calloc(arena, written * sizeof(*direct)) : NULL;
    bool unsealed = cls->unknown_base, inherited = false;
    uint32_t list_count = 0, direct_count = 0;
    if (!lists || !counts || (written && !direct)) {
        cls->visiting = false;
        py_seal_fallback(cls, arena);
        return;
    }
    for (uint32_t b = 0; b < written; b++) {
        bool skip = false;
        const py_class_t *base = py_resolve_base(eval, members, &cls->bases[b], &skip, &unsealed);
        if (skip)
            continue;
        if (!base || base == cls) {
            unsealed = true;
            continue;
        }
        py_seal_class(eval, members, (py_class_t *)base, depth + 1);
        if (!base->sealed ||
            (b < 64 && ((cls->subscripted >> b) & 1) && py_answers_subscript(base))) {
            unsealed = true;
            continue;
        }
        inherited |= base->uncertain;
        if (b == 0)
            cls->first = base;
        lists[list_count] = base->mro;
        counts[list_count] = base->mro_count;
        list_count++;
        direct[direct_count++] = base;
    }
    if (direct_count) {
        lists[list_count] = direct;
        counts[list_count] = direct_count;
        list_count++;
    }
    const py_class_t **mro = NULL;
    uint32_t mro_count = 0;
    if (!py_c3_merge(arena, cls, lists, counts, list_count, &mro, &mro_count)) {
        cls->visiting = false;
        py_seal_fallback(cls, arena);
        return;
    }
    cls->mro = mro;
    cls->mro_count = mro_count;
    cls->direct = direct;
    cls->direct_count = direct_count;
    uint32_t reliable = 1;
    while (reliable < mro_count && !mro[reliable]->opaque)
        reliable++;
    cls->uncertain = unsealed || inherited;
    if (cls->uncertain)
        reliable = py_single_chain(cls, mro, mro_count);
    cls->reliable = reliable;
    for (uint32_t i = 0; i < reliable; i++) {
        cls->intercepts |= py_own_member(mro[i], "__getattribute__") != NULL;
        cls->custom_new |= py_own_member(mro[i], "__new__") != NULL;
    }
    cls->visiting = false;
    cls->sealed = true;
}

typedef struct {
    py_class_t **items;
    size_t count;
} py_class_list_t;

static void py_collect_class(const char *key, void *value, void *context) {
    (void)key;
    py_class_list_t *list = context;
    list->items[list->count++] = value;
}

static int py_compare_class(const void *left, const void *right) {
    return strcmp((*(py_class_t *const *)left)->qualified_name,
                  (*(py_class_t *const *)right)->qualified_name);
}

typedef struct {
    const char **keys;
    size_t count;
} py_key_list_t;

static void py_collect_key(const char *key, void *value, void *context) {
    (void)value;
    py_key_list_t *list = context;
    list->keys[list->count++] = key;
}

static int py_compare_key(const void *left, const void *right) {
    return strcmp(*(const char *const *)left, *(const char *const *)right);
}

/* In a fixed order, so a recursion cycle settles the same way in every run. */
static void py_settle_returns(const py_eval_t *eval, cbm_py_members_t *members) {
    size_t count = cbm_ht_count(members->functions);
    const char **keys = count ? cbm_arena_alloc(&members->arena, count * sizeof(*keys)) : NULL;
    if (!keys) {
        return;
    }
    py_key_list_t list = {.keys = keys};
    cbm_ht_foreach(members->functions, py_collect_key, &list);
    qsort(keys, list.count, sizeof(*keys), py_compare_key);
    for (size_t i = 0; i < list.count; i++) {
        py_function_t *function = cbm_ht_get(members->functions, keys[i]);
        *eval->steps = 0;
        if (function && function->inference == 0)
            py_infer_returns(eval, function, 0);
    }
}

bool cbm_py_members_seal(cbm_py_members_t *members, const cbm_py_members_env_t *env) {
    size_t count = cbm_ht_count(members->classes);
    if (!count)
        return true;
    py_class_t **items = cbm_arena_alloc(&members->arena, count * sizeof(*items));
    if (!items)
        return false;
    py_class_list_t list = {.items = items};
    cbm_ht_foreach(members->classes, py_collect_class, &list);
    /* Deterministic independent of hash order: an unresolved cycle seals the
     * same classes the same way in every run. */
    qsort(items, list.count, sizeof(*items), py_compare_class);
    for (size_t i = 0; i < list.count; i++)
        py_settle_member_kinds(members, items[i]);
    CBMArena scratch;
    cbm_arena_init(&scratch);
    unsigned steps = 0;
    py_eval_t eval = {.members = members, .env = env, .arena = &scratch, .steps = &steps};
    for (size_t i = 0; i < list.count; i++) {
        steps = 0;
        py_seal_class(&eval, members, items[i], 0);
    }
    for (size_t i = 0; i < list.count; i++)
        if (!items[i]->sealed)
            py_seal_fallback(items[i], &members->arena);
    py_settle_returns(&eval, members);
    cbm_arena_destroy(&scratch);
    return true;
}

const char *cbm_py_members_base(const cbm_py_members_t *members, const char *qualified_name,
                                size_t offset) {
    const py_class_t *cls = members ? py_class(members, qualified_name) : NULL;
    if (!cls)
        return NULL;
    for (uint32_t i = 0; i < cls->direct_count; i++) {
        if (cls->direct[i]->opaque)
            continue;
        if (!offset--)
            return cls->direct[i]->qualified_name;
    }
    return NULL;
}

/* ── Occurrences ─────────────────────────────────────────────────────── */

bool cbm_py_members_resolve(const cbm_py_members_t *members, const cbm_py_members_env_t *env,
                            const cbm_py_import_lookup_t *imports,
                            const cbm_resolve_binding_index_t *index, CBMArena *arena,
                            const char *name, uint32_t start, uint32_t end, CBMSourceOrigin origin,
                            bool invocation, cbm_resolve_result_t *out) {
    const char *chain = NULL;
    uint32_t scope = 0;
    if (!members || !name ||
        !cbm_resolve_binding_python_site(index, name, start, end, origin, &chain, &scope))
        return false;
    unsigned ref_steps = 0, eval_steps = 0;
    py_ref_t ref = py_ref_of_chain(index, arena, imports, chain, scope, 0, 0, &ref_steps);
    if (ref.kind == PY_REF_NONE || ref.kind == PY_REF_UNBOUND)
        return false;
    py_eval_t eval = {.members = members, .env = env, .arena = arena, .steps = &eval_steps};
    py_value_t value = py_eval_ref(&eval, &ref, 0);
    /* A plain name for a declaration is lexical lookup's: it knows source order.
     * This path adds only what a name holds. */
    if (ref.kind == PY_REF_QN && (!ref.tail || !ref.tail[0]) &&
        ((value.kind == PY_VALUE_FUNCTION && !strcmp(value.qualified_name, ref.name)) ||
         (value.kind == PY_VALUE_CLASS && !strcmp(value.cls->qualified_name, ref.name))))
        return false;
    const char *target = NULL;
    const char *owner = value.owner;
    bool class_target = false, property_read = false;
    if (!invocation && value.declaration) {
        target = value.declaration;
        property_read = value.kind != PY_VALUE_FUNCTION;
    } else if (value.kind == PY_VALUE_FUNCTION) {
        target = value.qualified_name;
    } else if (value.kind == PY_VALUE_CLASS) {
        target = value.cls->qualified_name;
        class_target = true;
    } else if (invocation && value.kind == PY_VALUE_INSTANCE) {
        const py_class_t *holder = NULL;
        const py_member_t *call = py_lookup(value.cls, "__call__", 0, &holder);
        if (call && call->kind == PY_MEMBER_FUNCTION) {
            target = call->target;
            owner = holder->qualified_name;
        }
    }
    if (!target)
        return false;
    *out =
        cbm_resolve_binding_python_member(index, name, start, end, origin, owner ? owner : target,
                                          target, class_target, property_read);
    return true;
}
