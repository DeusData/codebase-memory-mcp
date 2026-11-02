#include "source_adapter_internal.h"
#include "../lang_specs.h"
#include "../helpers.h"
#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include "syntax_index.h"

enum { SOURCE_MAX_VISITS = 4000000, SOURCE_MAX_DEPTH = 400 };

struct cbm_source_table {
    CBMArena arena;
    CBMHashTable *decls;
    CBMHashTable *qn_owners;
    const CBMHashTable *packages;
    bool incomplete[CBM_LANG_COUNT];
    bool failed;
};

typedef struct {
    uint32_t start, end;
} source_damage_t;

struct cbm_source_unit {
    cbm_source_site_t *sites;
    size_t count, capacity;
    cbm_source_extra_call_t *extras;
    size_t extra_count, extra_capacity;
    source_damage_t *damage; /* sorted, disjoint: no site overlapping one is decided */
    size_t damage_count, damage_capacity;
    const char *failure;
};

static const cbm_source_adapter_t *adapter_for(CBMLanguage language) {
    static const cbm_source_adapter_t *const adapters[] = {
        &cbm_source_adapter_php,     &cbm_source_adapter_elixir, &cbm_source_adapter_erlang,
        &cbm_source_adapter_perl,    &cbm_source_adapter_ruby,   &cbm_source_adapter_zig,
        &cbm_source_adapter_haskell, &cbm_source_adapter_ocaml,  &cbm_source_adapter_dart,
        &cbm_source_adapter_swift,   &cbm_source_adapter_scala,
    };
    for (size_t i = 0; i < sizeof(adapters) / sizeof(adapters[0]); i++)
        if (adapters[i]->language == language)
            return adapters[i];
    return NULL;
}

bool cbm_source_adapter_supported(CBMLanguage language) {
    return adapter_for(language) != NULL;
}

bool cbm_source_adapter_one_name_one_node(CBMLanguage language) {
    const cbm_source_adapter_t *adapter = adapter_for(language);
    return adapter && adapter->one_name_one_node;
}

bool cbm_source_adapter_decides_references(CBMLanguage language) {
    const cbm_source_adapter_t *adapter = adapter_for(language);
    return adapter && adapter->decides_references;
}

bool cbm_source_adapter_has_binding_rules(CBMLanguage language) {
    const cbm_source_adapter_t *adapter = adapter_for(language);
    return adapter && adapter->before_binding_rules;
}

void cbm_source_table_set_packages(cbm_source_table_t *table, const CBMHashTable *packages) {
    if (table)
        table->packages = packages;
}

const char *cbm_source_table_base(const cbm_source_table_t *table, CBMLanguage language,
                                  const char *qualified_name, size_t offset) {
    const cbm_source_adapter_t *adapter = adapter_for(language);
    if (!table || !adapter || !adapter->base || !qualified_name)
        return NULL;
    const cbm_source_decl_t *decl = cbm_ht_get(table->qn_owners, qualified_name);
    return decl ? adapter->base(table, decl, offset) : NULL;
}

const char *cbm_source_table_package(const cbm_source_table_t *table, const char *name) {
    return table && table->packages && name ? cbm_ht_get(table->packages, name) : NULL;
}

cbm_source_table_t *cbm_source_table_new(void) {
    cbm_source_table_t *table = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, sizeof(*table));
    if (!table)
        return NULL;
    cbm_arena_init(&table->arena);
    table->decls = cbm_ht_create(256);
    table->qn_owners = cbm_ht_create(256);
    if (!table->decls || !table->qn_owners) {
        cbm_source_table_free(table);
        return NULL;
    }
    return table;
}

void cbm_source_table_free(cbm_source_table_t *table) {
    if (!table)
        return;
    cbm_ht_free(table->decls);
    cbm_ht_free(table->qn_owners);
    cbm_arena_destroy(&table->arena);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, table);
}

bool cbm_source_table_incomplete(const cbm_source_table_t *table, CBMLanguage language) {
    return !table || language < 0 || language >= CBM_LANG_COUNT || table->failed ||
           table->incomplete[language];
}

TSNode cbm_source_named_field(TSNode node, const char *name) {
    TSNode found = (TSNode){0};
    if (ts_node_is_null(node))
        return found;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            const char *field = ts_tree_cursor_current_field_name(&cursor);
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (field && !strcmp(field, name) && ts_node_is_named(child)) {
                found = child;
                break;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    return found;
}

const char *cbm_source_text(cbm_source_walk_t *walk, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    return end > start && end <= walk->source_len
               ? cbm_arena_strndup(walk->arena, walk->source + start, end - start)
               : NULL;
}

const char *cbm_source_lower(cbm_source_walk_t *walk, const char *text) {
    if (!text)
        return NULL;
    char *copy = cbm_arena_strdup(walk->arena, text);
    if (copy)
        for (char *p = copy; *p; p++)
            *p = (char)tolower((unsigned char)*p);
    return copy;
}

void cbm_source_fail(cbm_source_walk_t *walk, const char *reason) {
    if (!walk->failure)
        walk->failure = reason;
}

bool cbm_source_step(cbm_source_walk_t *walk, unsigned depth) {
    if (walk->failure)
        return false;
    if (++walk->visits > SOURCE_MAX_VISITS || depth > SOURCE_MAX_DEPTH) {
        cbm_source_fail(walk, "source_adapter_walk_limit");
        return false;
    }
    return true;
}

bool cbm_source_token(TSNode node, const char *token) {
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (!ts_node_is_named(child) && !strcmp(ts_node_type(child), token))
            return true;
    }
    return false;
}

typedef struct source_def_link {
    const CBMDefinition *def;
    struct source_def_link *next;
} source_def_link_t;

/* One join per declaration must not rescan every definition of a large file. */
static bool source_def_index(cbm_source_walk_t *walk) {
    if (walk->def_index)
        return true;
    walk->def_index = cbm_ht_create(64);
    if (!walk->def_index)
        return false;
    const CBMDefArray *defs = &walk->result->defs;
    for (int d = 0; d < defs->count; d++) {
        const CBMDefinition *def = &defs->items[d];
        if (!def->name || !def->qualified_name)
            continue;
        source_def_link_t *link = cbm_arena_alloc(walk->arena, sizeof(*link));
        if (!link)
            return false;
        link->def = def;
        link->next = cbm_ht_get(walk->def_index, def->name);
        cbm_ht_set(walk->def_index, def->name, link);
    }
    return true;
}

const char *cbm_source_def_qn_label(cbm_source_walk_t *walk, const char *name, uint32_t line,
                                    const char *label) {
    if (!name || !source_def_index(walk))
        return NULL;
    const char *found = NULL;
    uint32_t best = UINT32_MAX;
    for (const source_def_link_t *link = cbm_ht_get(walk->def_index, name); link;
         link = link->next) {
        const CBMDefinition *def = link->def;
        if (def->start_line > line || def->end_line < line)
            continue;
        if (label && (!def->label || strcmp(def->label, label)))
            continue;
        uint32_t span = def->end_line - def->start_line;
        if (span < best) {
            best = span;
            found = def->qualified_name;
        }
    }
    return found;
}

const char *cbm_source_def_qn_of_name(cbm_source_walk_t *walk, const char *name) {
    if (!name || !source_def_index(walk))
        return NULL;
    const char *found = NULL;
    for (const source_def_link_t *link = cbm_ht_get(walk->def_index, name); link;
         link = link->next) {
        if (found && strcmp(found, link->def->qualified_name))
            return NULL;
        found = link->def->qualified_name;
    }
    return found;
}

const char *cbm_source_def_qn(cbm_source_walk_t *walk, const char *name, uint32_t line) {
    return cbm_source_def_qn_label(walk, name, line, NULL);
}

const char *cbm_source_table_intern(cbm_source_walk_t *walk, const char *text) {
    cbm_source_table_t *table = walk->writable;
    return text && table ? cbm_arena_strdup(&table->arena, text) : NULL;
}

void *cbm_source_table_alloc(cbm_source_walk_t *walk, size_t size) {
    cbm_source_table_t *table = walk->writable;
    return table ? cbm_arena_calloc(&table->arena, size) : NULL;
}

/* U+001F cannot occur in any identifier of a supported language. */
static const char *decl_key(CBMArena *arena, CBMLanguage language, const char *owner,
                            const char *name) {
    return cbm_arena_sprintf(arena, "%d\x1f%s\x1f%s", (int)language, owner ? owner : "",
                             name ? name : "");
}

cbm_source_decl_t *cbm_source_declare(cbm_source_walk_t *walk, const char *owner, const char *name,
                                      const char *qn, uint32_t flags, uint32_t line) {
    cbm_source_table_t *table = walk->writable;
    if (!table || !name)
        return NULL;
    const char *key = decl_key(&table->arena, walk->language, owner, name);
    cbm_source_decl_t *decl = cbm_arena_calloc(&table->arena, sizeof(*decl));
    if (!key || !decl) {
        table->failed = true;
        return NULL;
    }
    *decl = (cbm_source_decl_t){.owner = cbm_source_table_intern(walk, owner ? owner : ""),
                                .name = cbm_source_table_intern(walk, name),
                                .qn = qn ? cbm_source_table_intern(walk, qn) : NULL,
                                .file = cbm_source_table_intern(walk, walk->file ? walk->file : ""),
                                .param_count = -1,
                                .arity_min = -1,
                                .arity_max = -1,
                                .flags = flags,
                                .line = line};
    if (!decl->owner || !decl->name || !decl->file || (qn && !decl->qn)) {
        table->failed = true;
        return NULL;
    }
    /* A QN that omits its owner can name declarations of two owners; then it
     * denotes neither of them exactly. */
    if (decl->qn) {
        cbm_source_decl_t *first = cbm_ht_get(table->qn_owners, decl->qn);
        if (!first) {
            cbm_ht_set(table->qn_owners, decl->qn, decl);
            if (cbm_ht_get(table->qn_owners, decl->qn) != decl) {
                table->failed = true;
                return NULL;
            }
        } else if (strcmp(first->owner, decl->owner) ||
                   (strcmp(first->name, decl->name) &&
                    !(first->flags & decl->flags & CBM_SOURCE_DECL_F_TYPE))) {
            /* One owner's two type-like declarations are one node: a Scala class and its
             * companion object. */
            first->flags |= CBM_SOURCE_DECL_F_QN_SHARED;
            decl->flags |= CBM_SOURCE_DECL_F_QN_SHARED;
        }
    }
    cbm_source_decl_t *head = cbm_ht_get(table->decls, key);
    if (!head) {
        cbm_ht_set(table->decls, key, decl);
        if (cbm_ht_get(table->decls, key) != decl) {
            table->failed = true;
            return NULL;
        }
        return decl;
    }
    (head->last ? head->last : head)->next = decl;
    head->last = decl;
    return decl;
}

const cbm_source_decl_t *cbm_source_lookup_in(const cbm_source_walk_t *walk, CBMLanguage language,
                                              const char *owner, const char *name) {
    if (!walk->table || !name)
        return NULL;
    /* Short keys avoid arena growth in the hottest path of the adapters. */
    char buffer[768];
    int length =
        snprintf(buffer, sizeof(buffer), "%d\x1f%s\x1f%s", (int)language, owner ? owner : "", name);
    if (length < 0 || (size_t)length >= sizeof(buffer))
        return NULL;
    return cbm_ht_get(walk->table->decls, buffer);
}

const cbm_source_decl_t *cbm_source_lookup(const cbm_source_walk_t *walk, const char *owner,
                                           const char *name) {
    return cbm_source_lookup_in(walk, walk->language, owner, name);
}

const char *cbm_source_unique_qn(const cbm_source_decl_t *first, const char **reason) {
    *reason = "source_declaration_absent";
    if (!first)
        return NULL;
    /* Clauses and arities of one name share one graph node: equal QN is one
     * declaration target. A different QN is a different declaration. */
    for (const cbm_source_decl_t *decl = first; decl; decl = decl->next) {
        if (!decl->qn) {
            *reason = "source_declaration_not_materialized";
            return NULL;
        }
        if (decl->flags & CBM_SOURCE_DECL_F_QN_SHARED) {
            *reason = "source_graph_identity_shared_by_owners";
            return NULL;
        }
        if (strcmp(decl->qn, first->qn)) {
            *reason = "source_declaration_ambiguous";
            return NULL;
        }
    }
    *reason = "source_declaration_unique";
    return first->qn;
}

void cbm_source_emit(cbm_source_walk_t *walk, uint32_t start, uint32_t end,
                     cbm_source_site_kind_t kind, const char *target, cbm_resolve_rule_t rule,
                     const char *domain, const char *reason) {
    cbm_source_emit_facts(walk, start, end, kind, target, rule, domain, reason, 0);
}

void cbm_source_emit_facts(cbm_source_walk_t *walk, uint32_t start, uint32_t end,
                           cbm_source_site_kind_t kind, const char *target, cbm_resolve_rule_t rule,
                           const char *domain, const char *reason, uint32_t facts) {
    cbm_source_unit_t *unit = walk->unit;
    if (!unit || end <= start)
        return;
    if (walk->extra_name) {
        if (!target)
            return;
        if (unit->extra_count == unit->extra_capacity) {
            size_t capacity = unit->extra_capacity ? unit->extra_capacity * 2 : 32;
            cbm_source_extra_call_t *extras =
                cbm_arena_alloc(walk->arena, capacity * sizeof(*extras));
            if (!extras) {
                cbm_source_fail(walk, "source_adapter_allocation");
                return;
            }
            if (unit->extra_count)
                memcpy(extras, unit->extras, unit->extra_count * sizeof(*extras));
            unit->extras = extras;
            unit->extra_capacity = capacity;
        }
        unit->extras[unit->extra_count++] =
            (cbm_source_extra_call_t){.scope = walk->extra_scope ? walk->extra_scope : "",
                                      .name = walk->extra_name,
                                      .start = start,
                                      .end = end,
                                      .kind = kind,
                                      .line = walk->extra_line};
    }
    if (unit->count == unit->capacity) {
        size_t capacity = unit->capacity ? unit->capacity * 2 : 64;
        cbm_source_site_t *sites = cbm_arena_alloc(walk->arena, capacity * sizeof(*sites));
        if (!sites) {
            cbm_source_fail(walk, "source_adapter_allocation");
            return;
        }
        if (unit->count)
            memcpy(sites, unit->sites, unit->count * sizeof(*sites));
        unit->sites = sites;
        unit->capacity = capacity;
    }
    unit->sites[unit->count++] =
        (cbm_source_site_t){.start = start,
                            .end = end,
                            .kind = kind,
                            .outcome = {.target = target,
                                        .rule = target ? rule : CBM_RESOLVE_RULE_NONE,
                                        .domain = domain,
                                        .reason = reason,
                                        .facts = target ? facts : 0}};
}

static TSTree *parse_source(const CBMFileResult *result, CBMLanguage language, const char *source,
                            size_t source_len, TSTree **owned) {
    *owned = NULL;
    if (result->cached_tree && result->cached_lang == language)
        return (TSTree *)result->cached_tree;
    TSParser *parser = ts_parser_new();
    if (!parser)
        return NULL;
    TSTree *tree = NULL;
    if (ts_parser_set_language(parser, cbm_ts_language(language)))
        tree = cbm_parse_plain(parser, source, (uint32_t)source_len);
    ts_parser_delete(parser);
    *owned = tree;
    return tree;
}

static const char *source_gate(const cbm_source_adapter_t *adapter, const CBMFileResult *result,
                               const char *source, size_t source_len) {
    if (!result || !source || !result->module_qn || source_len > UINT32_MAX)
        return "source_adapter_input_unavailable";
    if (result->has_error || result->parse_unusable ||
        (result->parse_incomplete && !adapter->contains_parse_damage))
        return "source_adapter_parse_incomplete";
    return NULL;
}

/* A grammar rejects a little valid code, and one rejection must not cost the whole file.
 * What a rejection can break decides how far its damage reaches. Inside a function it can
 * hide locals, so that whole function decides nothing. At class or file level it hides
 * only what it spans, unless it may have changed the structure around it: an unbalanced
 * delimiter or block keyword moves everything after it to another scope, a declaration
 * keyword means a declaration was lost, and a MISSING token means an opener never closed.
 * Those leave the file as a whole undecided, as before. */
enum { SOURCE_DAMAGE_MAX_DEPTH = 400 };

typedef struct {
    CBMArena *arena; /* NULL: only ask whether the file can be trusted at all */
    cbm_source_unit_t *unit;
    const char *source;
    const CBMLangSpec *spec;
} source_damage_scan_t;

static bool source_word_in(const char *text, size_t length, const char *const *words) {
    for (size_t at = 0; at < length;) {
        if (!isalpha((unsigned char)text[at]) && text[at] != '_') {
            at++;
            continue;
        }
        size_t end = at;
        while (end < length && (isalnum((unsigned char)text[end]) || text[end] == '_'))
            end++;
        for (size_t w = 0; words[w]; w++)
            if (strlen(words[w]) == end - at && !strncmp(text + at, words[w], end - at))
                return true;
        at = end;
    }
    return false;
}

static bool source_delimiters_balance(const char *text, size_t length) {
    int round = 0, square = 0, curly = 0;
    for (size_t i = 0; i < length; i++) {
        round += (text[i] == '(') - (text[i] == ')');
        square += (text[i] == '[') - (text[i] == ']');
        curly += (text[i] == '{') - (text[i] == '}');
        if (round < 0 || square < 0 || curly < 0)
            return false;
    }
    return !round && !square && !curly;
}

static bool source_damage_add(source_damage_scan_t *scan, TSNode node) {
    if (!scan->arena)
        return true;
    cbm_source_unit_t *unit = scan->unit;
    if (unit->damage_count == unit->damage_capacity) {
        size_t capacity = unit->damage_capacity ? unit->damage_capacity * 2 : 8;
        source_damage_t *items = cbm_arena_alloc(scan->arena, capacity * sizeof(*items));
        if (!items)
            return false;
        if (unit->damage_count)
            memcpy(items, unit->damage, unit->damage_count * sizeof(*items));
        unit->damage = items;
        unit->damage_capacity = capacity;
    }
    unit->damage[unit->damage_count++] =
        (source_damage_t){ts_node_start_byte(node), ts_node_end_byte(node)};
    return true;
}

/* True when the damage below this node is contained. Reads only the nodes that hold an
 * error, so a clean subtree costs nothing. */
static bool source_damage_scan(source_damage_scan_t *scan, TSNode node, bool in_function,
                               unsigned depth) {
    static const char *const blocks[] = {"end", NULL};
    static const char *const declarations[] = {
        "class",   "object",   "trait",    "def",  "defp", "defmodule",
        "module",  "sub",      "struct",   "fn",   "impl", "interface",
        "package", "function", "defmacro", "enum", NULL};
    if (depth > SOURCE_DAMAGE_MAX_DEPTH || ts_node_is_missing(node))
        return false;
    if (ts_node_is_error(node)) {
        const char *text = scan->source + ts_node_start_byte(node);
        size_t length = ts_node_end_byte(node) - ts_node_start_byte(node);
        if (!source_delimiters_balance(text, length) || source_word_in(text, length, blocks) ||
            (!in_function && source_word_in(text, length, declarations)))
            return false;
        return in_function || source_damage_add(scan, node);
    }
    if (!ts_node_has_error(node))
        return true;
    bool function = !in_function && cbm_kind_in_set(node, scan->spec->function_node_types);
    if (function && !source_damage_add(scan, node))
        return false;
    bool contained = true;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            contained = source_damage_scan(scan, ts_tree_cursor_current_node(&cursor),
                                           in_function || function, depth + 1);
        } while (contained && ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    return contained;
}

static bool source_tree_usable(const cbm_source_adapter_t *adapter, TSTree *tree, CBMArena *arena,
                               cbm_source_unit_t *unit, const char *source) {
    if (!tree)
        return false;
    TSNode root = ts_tree_root_node(tree);
    if (!ts_node_has_error(root))
        return true;
    if (!adapter->contains_parse_damage)
        return false;
    source_damage_scan_t scan = {
        .arena = arena, .unit = unit, .source = source, .spec = cbm_lang_spec(adapter->language)};
    return scan.spec && source_damage_scan(&scan, root, false, 0);
}

bool cbm_source_collect(cbm_source_table_t *table, const CBMFileResult *result,
                        CBMLanguage language, const char *file, const char *source,
                        size_t source_len) {
    const cbm_source_adapter_t *adapter = adapter_for(language);
    if (!table || !adapter)
        return false;
    bool gated = source_gate(adapter, result, source, source_len) != NULL;
    if (gated && (!adapter->collect_unusable || !result || !source || !result->module_qn ||
                  source_len > UINT32_MAX)) {
        /* Its declarations remain unknown competitors of every other file. */
        table->incomplete[language] = true;
        return !table->failed;
    }
    TSTree *owned = NULL;
    TSTree *tree = parse_source(result, language, source, source_len, &owned);
    bool usable = !gated && source_tree_usable(adapter, tree, NULL, NULL, source);
    if (!usable && (!tree || !adapter->collect_unusable)) {
        table->incomplete[language] = true;
        if (owned)
            ts_tree_delete(owned);
        return !table->failed;
    }
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_source_walk_t walk = {.arena = &arena,
                              .result = result,
                              .language = language,
                              .file = file,
                              .source = source,
                              .source_len = source_len,
                              .writable = table,
                              .root = ts_tree_root_node(tree)};
    cbm_syntax_index_t syntax;
    cbm_syntax_index_begin(&syntax, tree);
    (usable ? adapter->collect : adapter->collect_unusable)(&walk);
    cbm_syntax_index_end(&syntax);
    cbm_ht_free(walk.def_index);
    if (walk.failure)
        table->incomplete[language] = true;
    cbm_arena_destroy(&arena);
    if (owned)
        ts_tree_delete(owned);
    return !table->failed;
}

static int compare_site(const void *left, const void *right) {
    const cbm_source_site_t *a = left, *b = right;
    if (a->start != b->start)
        return a->start < b->start ? -1 : 1;
    if (a->end != b->end)
        return a->end < b->end ? -1 : 1;
    if (a->kind != b->kind)
        return a->kind < b->kind ? -1 : 1;
    return 0;
}

cbm_source_unit_t *cbm_source_unit_build(CBMArena *arena, const cbm_source_table_t *table,
                                         const CBMFileResult *result, CBMLanguage language,
                                         const char *file, const char *source, size_t source_len) {
    cbm_source_unit_t *unit = arena ? cbm_arena_calloc(arena, sizeof(*unit)) : NULL;
    if (!unit)
        return NULL;
    const cbm_source_adapter_t *adapter = adapter_for(language);
    if (!adapter || !table) {
        unit->failure = "source_adapter_unavailable";
        return unit;
    }
    unit->failure = source_gate(adapter, result, source, source_len);
    if (unit->failure)
        return unit;
    TSTree *owned = NULL;
    TSTree *tree = parse_source(result, language, source, source_len, &owned);
    if (!source_tree_usable(adapter, tree, arena, unit, source)) {
        unit->failure = "source_adapter_parse_incomplete";
        if (owned)
            ts_tree_delete(owned);
        return unit;
    }
    cbm_source_walk_t walk = {.arena = arena,
                              .result = result,
                              .language = language,
                              .file = file,
                              .source = source,
                              .source_len = source_len,
                              .table = table,
                              .unit = unit,
                              .root = ts_tree_root_node(tree)};
    cbm_syntax_index_t syntax;
    cbm_syntax_index_begin(&syntax, tree);
    adapter->resolve(&walk);
    cbm_syntax_index_end(&syntax);
    cbm_ht_free(walk.def_index);
    if (walk.failure) {
        unit->failure = walk.failure;
        unit->count = 0;
    }
    if (owned)
        ts_tree_delete(owned);
    if (unit->count > 1)
        qsort(unit->sites, unit->count, sizeof(*unit->sites), compare_site);
    return unit;
}

const cbm_source_extra_call_t *cbm_source_unit_extra_calls(const cbm_source_unit_t *unit,
                                                           size_t *count) {
    *count = unit && !unit->failure ? unit->extra_count : 0;
    return *count ? unit->extras : NULL;
}

void cbm_source_unit_site(const cbm_source_unit_t *unit, uint32_t start, uint32_t end,
                          cbm_source_site_kind_t kind, cbm_source_outcome_t *out) {
    *out = (cbm_source_outcome_t){.reason = "source_adapter_unavailable"};
    if (!unit)
        return;
    if (unit->failure) {
        out->reason = unit->failure;
        return;
    }
    /* The first damage ending after this site's start is the only one it can overlap. */
    size_t low = 0, high = unit->damage_count;
    while (low < high) {
        size_t middle = low + (high - low) / 2;
        if (unit->damage[middle].end <= start)
            low = middle + 1;
        else
            high = middle;
    }
    if (low < unit->damage_count && unit->damage[low].start < end) {
        out->reason = "source_adapter_parse_incomplete";
        return;
    }
    out->reason = "source_occurrence_not_modelled";
    if (!unit->count)
        return;
    cbm_source_site_t key = {.start = start, .end = end, .kind = kind};
    const cbm_source_site_t *found =
        bsearch(&key, unit->sites, unit->count, sizeof(*unit->sites), compare_site);
    if (!found)
        return;
    /* Two rows for one site that disagree are no decision at all. */
    const cbm_source_site_t *first = found, *last = found;
    while (first > unit->sites && compare_site(first - 1, &key) == 0)
        first--;
    while (last + 1 < unit->sites + unit->count && compare_site(last + 1, &key) == 0)
        last++;
    for (const cbm_source_site_t *site = first; site <= last; site++) {
        bool same = (!site->outcome.target && !found->outcome.target) ||
                    (site->outcome.target && found->outcome.target &&
                     !strcmp(site->outcome.target, found->outcome.target));
        if (!same) {
            out->reason = "source_occurrence_conflict";
            return;
        }
    }
    *out = found->outcome;
}
