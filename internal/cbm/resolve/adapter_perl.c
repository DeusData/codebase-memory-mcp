/* Perl binding rules, from perlsub, perlmod and perlobj.
 *
 * A named sub belongs to the package current where it is compiled, or the
 * package it spells. `name(...)` looks in the current package's symbol table,
 * which holds its own subs and what `use` imported into it; `A::B::name(...)`
 * looks in A::B. Subs have a namespace of their own, so no `my $name` shadows
 * one. A builtin is not overridden by an ordinary sub of the same name: only an
 * import or the `&name` form reaches the sub, so a bare builtin name abstains.
 *
 * `Class->m` starts method resolution at Class, then walks @ISA depth first, the
 * default MRO. A package outside the project ends that walk as unknown. @ISA is
 * read from `use parent`, `use base`, `use Mojo::Base 'X'` (documented as parent
 * plus strict) and literal @ISA assignments.
 *
 * `$self->m` and `$class->m` use the invocant convention perlobj documents: the
 * first statement of a sub in package P takes the first argument into that exact
 * name, and the sub never rebinds it. A sub reached as a method has an invocant
 * that isa P, so P's chain is what static rules can select from. Override in a
 * subclass is runtime dispatch.
 *
 * `shift->m` and `$_[0]->m` read that same first argument directly. They count when
 * nothing has shifted it away first, in a named sub of a package other than main that
 * the project never calls as a plain function or exports. Only such a sub is known to
 * be reached as a method; `_select($list)` hands over whatever it was given. The sub
 * given for an operator in `use overload` gets the object first, by that pragma's own
 * contract, and `my ($self, $x) = (shift, shift)` takes the first argument too.
 *
 * An import binds only through Exporter: the exporting package uses Exporter,
 * has no sub import of its own, and lists the name in @EXPORT or @EXPORT_OK. What it
 * lists can be a name it imported itself, which is then that module's export.
 *
 * `\&name` takes a reference to the sub the same lookup selects, without calling it. Under a
 * conditional or an operator the value is one of several, so the site only mentions the sub:
 * the carrier from extraction says which.
 *
 * Literal glob assignment and lexical `my sub` replace a name, so that name
 * abstains. A glob assigned from a computed name can touch any package anywhere
 * in the program; that is outside what a static rule can bound and is listed as
 * a known limit of this adapter's capability. */
#include "source_adapter_internal.h"
#include <stdio.h>
#include <stdlib.h>

enum { PERL_MAX_BASES = 32 };

#define PERL_EXPORTER_KEY "&exporter"
#define PERL_EXPORT_PREFIX "&export:"
#define PERL_IMPORTED_PREFIX "&imported:"
#define PERL_FUNCTION_CALL_PREFIX "&function:"

typedef struct perl_import {
    const char *package; /* importing package */
    const char *module;
    const char *name; /* NULL means the module's default @EXPORT */
    struct perl_import *next;
} perl_import_t;

typedef struct {
    cbm_source_walk_t *walk;
    const char *package;
    perl_import_t *imports;
    CBMHashTable *function_calls; /* phase one: "package\x1fname" already noted in this file */
    uint32_t first_shift;         /* byte of the sub's first shift of its arguments */
    bool reads_invocant;          /* shift-> and $_[0]-> name the invocant here */
    bool overload_list;           /* directly in the list of a `use overload` */
} perl_ctx_t;

static const char *perl_field_text(perl_ctx_t *ctx, TSNode node, const char *name) {
    return cbm_source_text(ctx->walk, cbm_source_field(node, name));
}

static bool perl_builtin(const char *name) {
    static const char *const names[] = {
        "abs",     "accept",   "alarm",     "atan2",    "bind",      "binmode",   "bless",
        "caller",  "chdir",    "chmod",     "chomp",    "chop",      "chown",     "chr",
        "chroot",  "close",    "closedir",  "connect",  "cos",       "crypt",     "defined",
        "delete",  "die",      "do",        "dump",     "each",      "eof",       "eval",
        "exec",    "exists",   "exit",      "exp",      "fc",        "fcntl",     "fileno",
        "flock",   "fork",     "format",    "formline", "getc",      "getlogin",  "getpeername",
        "getpgrp", "getppid",  "glob",      "gmtime",   "goto",      "grep",      "hex",
        "index",   "int",      "ioctl",     "join",     "keys",      "kill",      "last",
        "lc",      "lcfirst",  "length",    "link",     "listen",    "local",     "localtime",
        "lock",    "log",      "lstat",     "map",      "mkdir",     "next",      "oct",
        "open",    "opendir",  "ord",       "pack",     "pipe",      "pop",       "pos",
        "print",   "printf",   "prototype", "push",     "quotemeta", "rand",      "read",
        "readdir", "readline", "readlink",  "readpipe", "recv",      "redo",      "ref",
        "rename",  "require",  "reset",     "return",   "reverse",   "rewinddir", "rindex",
        "rmdir",   "say",      "scalar",    "seek",     "seekdir",   "select",    "send",
        "setpgrp", "shift",    "shutdown",  "sin",      "sleep",     "socket",    "socketpair",
        "sort",    "splice",   "split",     "sprintf",  "sqrt",      "srand",     "stat",
        "study",   "substr",   "symlink",   "syscall",  "sysopen",   "sysread",   "sysseek",
        "system",  "syswrite", "tell",      "telldir",  "tie",       "tied",      "time",
        "times",   "truncate", "uc",        "ucfirst",  "umask",     "undef",     "unlink",
        "unpack",  "unshift",  "untie",     "use",      "utime",     "values",    "vec",
        "wait",    "waitpid",  "wantarray", "warn",     "write",     NULL};
    for (size_t i = 0; names[i]; i++)
        if (!strcmp(name, names[i]))
            return true;
    return false;
}

/* Splits `A::B::name` into owner and leaf. `::name` is main; a bare name stays
 * in the given package. */
static void perl_split(perl_ctx_t *ctx, const char *text, const char *package, const char **owner,
                       const char **leaf) {
    const char *last = NULL;
    for (const char *p = text; (p = strstr(p, "::")); p += 2)
        last = p;
    if (!last) {
        *owner = package;
        *leaf = text;
        return;
    }
    *leaf = last + 2;
    *owner =
        last == text ? "main" : cbm_arena_strndup(ctx->walk->arena, text, (size_t)(last - text));
}

/* Whitespace-separated words of qw(...), or the strings of a list. A value this
 * walk cannot read as a literal reports false. */
typedef void (*perl_word_fn)(perl_ctx_t *ctx, const char *word, void *user);

static void perl_words_of_text(perl_ctx_t *ctx, const char *text, perl_word_fn fn, void *user) {
    const char *p = text;
    while (p && *p) {
        while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')
            p++;
        const char *start = p;
        while (*p && *p != ' ' && *p != '\t' && *p != '\n' && *p != '\r')
            p++;
        if (p > start) {
            const char *word = cbm_arena_strndup(ctx->walk->arena, start, (size_t)(p - start));
            if (word)
                fn(ctx, word, user);
        }
    }
}

static bool perl_literal_words(perl_ctx_t *ctx, TSNode node, perl_word_fn fn, void *user) {
    if (cbm_source_is(node, "quoted_word_list")) {
        perl_words_of_text(ctx, perl_field_text(ctx, node, "content"), fn, user);
        return true;
    }
    if (cbm_source_is(node, "string_literal") ||
        cbm_source_is(node, "interpolated_string_literal")) {
        const char *content = perl_field_text(ctx, node, "content");
        if (content)
            fn(ctx, content, user);
        return content != NULL;
    }
    if (cbm_source_is(node, "autoquoted_bareword")) {
        const char *content = cbm_source_text(ctx->walk, node);
        if (content)
            fn(ctx, content, user);
        return content != NULL;
    }
    if (cbm_source_is(node, "list_expression") || cbm_source_is(node, "parenthesized_expression")) {
        bool ok = true;
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++)
            ok &= perl_literal_words(ctx, ts_node_named_child(node, i), fn, user);
        return ok;
    }
    return false;
}

typedef struct {
    const char *words[PERL_MAX_BASES];
    size_t count;
} perl_words_t;

static void perl_gather_word(perl_ctx_t *ctx, const char *word, void *user) {
    (void)ctx;
    perl_words_t *list = user;
    if (list->count < PERL_MAX_BASES)
        list->words[list->count++] = word;
}

static TSNode perl_use_arguments(TSNode use) {
    /* use_statement is [module, optional arguments]. */
    uint32_t count = ts_node_named_child_count(use);
    TSNode module = cbm_source_field(use, "module");
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(use, i);
        if (!ts_node_eq(child, module) && !cbm_source_is(child, "comment"))
            return child;
    }
    return (TSNode){0};
}

static void perl_set_bases(perl_ctx_t *ctx, const char *package, const char *const *words,
                           size_t count, uint32_t line, bool open) {
    cbm_source_decl_t *record =
        cbm_source_declare(ctx->walk, package, "", NULL,
                           CBM_SOURCE_DECL_F_TYPE | (open ? CBM_SOURCE_DECL_F_OPEN : 0), line);
    if (!record)
        return;
    const char **bases = cbm_source_table_alloc(ctx->walk, (count + 1) * sizeof(*bases));
    if (!bases)
        return;
    size_t used = 0;
    for (size_t i = 0; i < count; i++)
        if (words[i][0] != '-') /* use parent -norequire / use base flags */
            bases[used++] = cbm_source_table_intern(ctx->walk, words[i]);
    bases[used] = NULL;
    record->bases = bases;
}

static void perl_note_exporter(perl_ctx_t *ctx, const char *package, uint32_t line) {
    (void)cbm_source_declare(ctx->walk, package, PERL_EXPORTER_KEY, NULL, CBM_SOURCE_DECL_F_VALUE,
                             line);
}

/* A name this package imported is in its symbol table, so it can export it onward. */
// cppcheck-suppress constParameterCallback
static void perl_declare_imported(perl_ctx_t *ctx, const char *word, void *user) {
    const char *name = word[0] == '&' ? word + 1 : word;
    const char *key = cbm_arena_sprintf(ctx->walk->arena, "%s%s", PERL_IMPORTED_PREFIX, name);
    cbm_source_decl_t *decl =
        key ? cbm_source_declare(ctx->walk, ctx->package, key, NULL, CBM_SOURCE_DECL_F_VALUE, 0)
            : NULL;
    if (decl)
        decl->type = cbm_source_table_intern(ctx->walk, (const char *)user);
}

/* `use parent`, `use base` and Mojo::Base set @ISA at compile time. */
static void perl_collect_use(perl_ctx_t *ctx, TSNode use) {
    const char *module = perl_field_text(ctx, use, "module");
    uint32_t line = ts_node_start_point(use).row + 1;
    if (!module)
        return;
    TSNode arguments = perl_use_arguments(use);
    if (!strcmp(module, "Exporter")) {
        perl_note_exporter(ctx, ctx->package, line);
        return;
    }
    bool mojo = !strcmp(module, "Mojo::Base");
    if (strcmp(module, "parent") && strcmp(module, "base") && !mojo) {
        if (!ts_node_is_null(arguments))
            (void)perl_literal_words(ctx, arguments, perl_declare_imported, (void *)module);
        return;
    }
    perl_words_t words = {0};
    bool literal =
        ts_node_is_null(arguments) || perl_literal_words(ctx, arguments, perl_gather_word, &words);
    if (mojo) {
        /* -base means the Mojo::Base class itself, -strict and -signatures add no
         * parent, and -role composes methods this walk does not model. */
        perl_words_t parents = {0};
        bool open = !literal;
        for (size_t i = 0; i < words.count; i++) {
            if (!strcmp(words.words[i], "-base"))
                perl_gather_word(ctx, "Mojo::Base", &parents);
            else if (!strcmp(words.words[i], "-role"))
                open = true;
            else if (words.words[i][0] != '-')
                perl_gather_word(ctx, words.words[i], &parents);
        }
        perl_set_bases(ctx, ctx->package, parents.words, parents.count, line, open);
        return;
    }
    for (size_t i = 0; i < words.count; i++)
        if (!strcmp(words.words[i], "Exporter"))
            perl_note_exporter(ctx, ctx->package, line);
    perl_set_bases(ctx, ctx->package, words.words, words.count, line, !literal);
}

// cppcheck-suppress constParameterCallback
static void perl_declare_export(perl_ctx_t *ctx, const char *word, void *user) {
    const char *package = user;
    const char *name = word[0] == '&' ? word + 1 : word;
    const char *key = cbm_arena_sprintf(ctx->walk->arena, "%s%s", PERL_EXPORT_PREFIX, name);
    if (key)
        (void)cbm_source_declare(ctx->walk, package, key, NULL, CBM_SOURCE_DECL_F_VALUE, 0);
}

static void perl_collect_array(perl_ctx_t *ctx, const char *array, TSNode right, uint32_t line) {
    if (!array)
        return;
    if (!strcmp(array, "ISA")) {
        perl_words_t words = {0};
        bool literal = perl_literal_words(ctx, right, perl_gather_word, &words);
        for (size_t i = 0; i < words.count; i++)
            if (!strcmp(words.words[i], "Exporter"))
                perl_note_exporter(ctx, ctx->package, line);
        perl_set_bases(ctx, ctx->package, words.words, words.count, line, !literal);
    } else if (!strcmp(array, "EXPORT") || !strcmp(array, "EXPORT_OK")) {
        (void)perl_literal_words(ctx, right, perl_declare_export, (void *)ctx->package);
    }
}

/* A literal glob assignment replaces that sub for the whole program. */
static void perl_collect_glob(perl_ctx_t *ctx, TSNode glob) {
    TSNode target = ts_node_named_child(glob, 0);
    const char *text = NULL;
    if (cbm_source_is(target, "varname")) {
        text = cbm_source_text(ctx->walk, target);
    } else if (cbm_source_is(target, "glob_deref_expression")) {
        TSNode block = ts_node_named_child(target, 0);
        TSNode statement = ts_node_is_null(block) ? (TSNode){0} : ts_node_named_child(block, 0);
        TSNode literal =
            ts_node_is_null(statement) ? (TSNode){0} : ts_node_named_child(statement, 0);
        if ((cbm_source_is(literal, "string_literal") ||
             cbm_source_is(literal, "interpolated_string_literal")) &&
            ts_node_named_child_count(literal) == 1)
            text = perl_field_text(ctx, literal, "content");
    }
    if (!text)
        return;
    const char *owner = NULL, *leaf = NULL;
    perl_split(ctx, text, ctx->package, &owner, &leaf);
    (void)cbm_source_declare(ctx->walk, owner, leaf, NULL, CBM_SOURCE_DECL_F_OPEN,
                             ts_node_start_point(glob).row + 1);
}

static void perl_collect_sub(perl_ctx_t *ctx, TSNode node) {
    const char *text = perl_field_text(ctx, node, "name");
    if (!text)
        return;
    uint32_t line = ts_node_start_point(node).row + 1;
    const char *owner = NULL, *leaf = NULL;
    perl_split(ctx, text, ctx->package, &owner, &leaf);
    /* A lexical sub hides the name wherever it is in scope; this walk does not
     * track that scope, so the name decides nothing. */
    if (cbm_source_token(node, "my") || cbm_source_token(node, "state")) {
        (void)cbm_source_declare(ctx->walk, owner, leaf, NULL, CBM_SOURCE_DECL_F_OPEN, line);
        return;
    }
    if (ts_node_is_null(cbm_source_field(node, "body")))
        return; /* A forward declaration defines nothing. */
    (void)cbm_source_declare(ctx->walk, owner, leaf,
                             cbm_source_def_qn_label(ctx->walk, text, line, "Function"),
                             CBM_SOURCE_DECL_F_PUBLIC, line);
}

/* Which subs the project calls as plain functions, once per file and name. */
static void perl_note_function_call(perl_ctx_t *ctx, TSNode call) {
    const char *raw = cbm_source_text(ctx->walk, cbm_source_field(call, "function"));
    if (!raw)
        return;
    const char *owner = NULL, *leaf = NULL;
    perl_split(ctx, raw[0] == '&' ? raw + 1 : raw, ctx->package, &owner, &leaf);
    const char *seen = cbm_arena_sprintf(ctx->walk->arena, "%s\x1f%s", owner, leaf);
    if (!seen || !ctx->function_calls || cbm_ht_get(ctx->function_calls, seen))
        return;
    cbm_ht_set(ctx->function_calls, seen, (void *)seen);
    const char *key = cbm_arena_sprintf(ctx->walk->arena, "%s%s", PERL_FUNCTION_CALL_PREFIX, leaf);
    if (key)
        (void)cbm_source_declare(ctx->walk, owner, key, NULL, CBM_SOURCE_DECL_F_VALUE,
                                 ts_node_start_point(call).row + 1);
}

static void perl_collect_node(perl_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child))
                continue;
            if (cbm_source_is(child, "package_statement")) {
                const char *package = perl_field_text(ctx, child, "name");
                if (!package)
                    continue;
                (void)cbm_source_declare(ctx->walk, package, "", NULL, CBM_SOURCE_DECL_F_TYPE,
                                         ts_node_start_point(child).row + 1);
                uint32_t parts = ts_node_named_child_count(child);
                TSNode block = (TSNode){0};
                for (uint32_t i = 0; i < parts; i++)
                    if (cbm_source_is(ts_node_named_child(child, i), "block"))
                        block = ts_node_named_child(child, i);
                if (ts_node_is_null(block)) {
                    ctx->package = package; /* Until the next package statement. */
                } else {
                    const char *saved = ctx->package;
                    ctx->package = package;
                    perl_collect_node(ctx, block, depth + 1);
                    ctx->package = saved;
                }
                continue;
            }
            if (cbm_source_is(child, "use_statement")) {
                perl_collect_use(ctx, child);
                continue;
            }
            if (cbm_source_is(child, "subroutine_declaration_statement")) {
                perl_collect_sub(ctx, child);
            } else if (cbm_source_is(child, "assignment_expression")) {
                TSNode left = cbm_source_named_field(child, "left");
                if (cbm_source_is(left, "localization_expression"))
                    left = ts_node_named_child(left, 0);
                if (cbm_source_is(left, "glob")) {
                    perl_collect_glob(ctx, left);
                } else if (cbm_source_is(left, "variable_declaration") ||
                           cbm_source_is(left, "array")) {
                    TSNode array =
                        cbm_source_is(left, "array") ? left : cbm_source_field(left, "variable");
                    if (cbm_source_is(array, "array"))
                        perl_collect_array(
                            ctx, cbm_source_text(ctx->walk, ts_node_named_child(array, 0)),
                            cbm_source_named_field(child, "right"),
                            ts_node_start_point(child).row + 1);
                }
            } else if (cbm_source_is(child, "function_call_expression")) {
                perl_note_function_call(ctx, child);
            } else if (cbm_source_is(child, "ambiguous_function_call_expression")) {
                perl_note_function_call(ctx, child);
                /* `push @ISA, 'X'` */
                const char *function = perl_field_text(ctx, child, "function");
                TSNode arguments = cbm_source_field(child, "arguments");
                if (function && !strcmp(function, "push") &&
                    cbm_source_is(arguments, "list_expression") &&
                    ts_node_named_child_count(arguments) > 1) {
                    const char *array =
                        cbm_source_text(ctx->walk, ts_node_named_child(arguments, 0));
                    if (array && !strcmp(array, "@ISA")) {
                        perl_words_t words = {0};
                        uint32_t count = ts_node_named_child_count(arguments);
                        bool literal = true;
                        for (uint32_t i = 1; i < count; i++)
                            literal &= perl_literal_words(ctx, ts_node_named_child(arguments, i),
                                                          perl_gather_word, &words);
                        perl_set_bases(ctx, ctx->package, words.words, words.count,
                                       ts_node_start_point(child).row + 1, !literal);
                    }
                }
            }
            perl_collect_node(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void perl_collect(cbm_source_walk_t *walk) {
    perl_ctx_t ctx = {.walk = walk, .package = "main", .function_calls = cbm_ht_create(64)};
    perl_collect_node(&ctx, walk->root, 0);
    cbm_ht_free(ctx.function_calls);
}

/* ── Phase two ─────────────────────────────────────────────────────── */

typedef enum { PERL_FOUND, PERL_ABSENT, PERL_OPEN, PERL_AMBIGUOUS } perl_lookup_t;

static perl_lookup_t perl_own_sub(const perl_ctx_t *ctx, const char *package, const char *name,
                                  const cbm_source_decl_t **out) {
    const cbm_source_decl_t *first = cbm_source_lookup(ctx->walk, package, name);
    if (!first)
        return PERL_ABSENT;
    const char *reason = NULL;
    if (!cbm_source_unique_qn(first, &reason))
        return PERL_AMBIGUOUS;
    *out = first;
    return PERL_FOUND;
}

static perl_lookup_t perl_find_method(const perl_ctx_t *ctx, const char *package, const char *name,
                                      cbm_source_chain_t *chain, const cbm_source_decl_t **out) {
    if (!cbm_source_chain_first_visit(chain, package))
        return chain->overflow ? PERL_OPEN : PERL_ABSENT;
    const cbm_source_decl_t *records = cbm_source_lookup(ctx->walk, package, "");
    if (!records)
        return PERL_OPEN; /* Not a project package: it may define the method. */
    perl_lookup_t own = perl_own_sub(ctx, package, name, out);
    if (own != PERL_ABSENT)
        return own;
    /* @ISA is one array, however many statements of the package set it. */
    for (const cbm_source_decl_t *record = records; record; record = record->next) {
        if (record->flags & CBM_SOURCE_DECL_F_OPEN)
            return PERL_OPEN;
        for (size_t i = 0; record->bases && record->bases[i]; i++) {
            perl_lookup_t found = perl_find_method(ctx, record->bases[i], name, chain, out);
            if (found != PERL_ABSENT)
                return found;
        }
    }
    return PERL_ABSENT;
}

static void perl_note_import_word(perl_ctx_t *ctx, const char *word, void *user) {
    perl_ctx_t *owner = ctx;
    const char **pair = user; /* {package, module} */
    perl_import_t *import = cbm_arena_alloc(owner->walk->arena, sizeof(*import));
    if (!import)
        return;
    const char *name = word[0] == '&' ? word + 1 : word;
    *import = (perl_import_t){pair[0], pair[1], name, owner->imports};
    owner->imports = import;
}

/* Imports hold for the package wherever they appear in it: `use` runs at compile
 * time and the call binds at runtime. First pass only, so the order is free. */
static void perl_gather_imports(perl_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child))
                continue;
            if (cbm_source_is(child, "package_statement")) {
                const char *package = perl_field_text(ctx, child, "name");
                uint32_t parts = ts_node_named_child_count(child);
                TSNode block = (TSNode){0};
                for (uint32_t i = 0; i < parts; i++)
                    if (cbm_source_is(ts_node_named_child(child, i), "block"))
                        block = ts_node_named_child(child, i);
                if (ts_node_is_null(block)) {
                    ctx->package = package ? package : ctx->package;
                } else {
                    const char *saved = ctx->package;
                    ctx->package = package ? package : saved;
                    perl_gather_imports(ctx, block, depth + 1);
                    ctx->package = saved;
                }
                continue;
            }
            if (cbm_source_is(child, "use_statement")) {
                const char *module = perl_field_text(ctx, child, "module");
                TSNode arguments = perl_use_arguments(child);
                const char *pair[2] = {ctx->package, module};
                if (module && ts_node_is_null(arguments)) {
                    perl_import_t *import = cbm_arena_alloc(ctx->walk->arena, sizeof(*import));
                    if (import) {
                        *import = (perl_import_t){ctx->package, module, NULL, ctx->imports};
                        ctx->imports = import;
                    }
                } else if (module) {
                    (void)perl_literal_words(ctx, arguments, perl_note_import_word, pair);
                }
                continue;
            }
            perl_gather_imports(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

/* What a module exports under a name: its own sub, or one it imported in turn. A chain
 * of modules visits each once, so a cycle ends and the cost is the chain's length. */
static bool perl_exported_sub(const perl_ctx_t *ctx, const char *module, const char *name,
                              cbm_source_chain_t *chain, const cbm_source_decl_t **out) {
    char key[256];
    int length = snprintf(key, sizeof(key), "%s%s", PERL_EXPORT_PREFIX, name);
    if (length < 0 || (size_t)length >= sizeof(key) ||
        !cbm_source_chain_first_visit(chain, module) ||
        !cbm_source_lookup(ctx->walk, module, PERL_EXPORTER_KEY) ||
        cbm_source_lookup(ctx->walk, module, "import") ||
        !cbm_source_lookup(ctx->walk, module, key))
        return false;
    perl_lookup_t own = perl_own_sub(ctx, module, name, out);
    if (own != PERL_ABSENT)
        return own == PERL_FOUND;
    length = snprintf(key, sizeof(key), "%s%s", PERL_IMPORTED_PREFIX, name);
    if (length < 0 || (size_t)length >= sizeof(key))
        return false;
    const cbm_source_decl_t *imported = cbm_source_lookup(ctx->walk, module, key);
    return imported && !imported->next && imported->type &&
           perl_exported_sub(ctx, imported->type, name, chain, out);
}

/* The named export of an Exporter-based module, or nothing. */
static const char *perl_imported_target(perl_ctx_t *ctx, const char *name, const char **module) {
    const char *selected = NULL;
    for (const perl_import_t *import = ctx->imports; import; import = import->next) {
        if (strcmp(import->package, ctx->package))
            continue;
        if (import->name && strcmp(import->name, name))
            continue;
        const cbm_source_decl_t *decl = NULL;
        cbm_source_chain_t chain = {0};
        if (!perl_exported_sub(ctx, import->module, name, &chain, &decl))
            continue;
        if (selected && strcmp(selected, decl->qn))
            return NULL;
        selected = decl->qn;
        *module = import->module;
    }
    return selected;
}

static void perl_emit(perl_ctx_t *ctx, TSNode node, const char *target, cbm_resolve_rule_t rule,
                      const char *domain, const char *reason) {
    cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_CALL, target, rule, domain, reason);
}

/* A sub named in the current package or spelled with its own. */
static void perl_emit_sub(perl_ctx_t *ctx, TSNode node, cbm_source_site_kind_t kind,
                          const char *text) {
    const char *owner = NULL, *leaf = NULL;
    perl_split(ctx, text, ctx->package, &owner, &leaf);
    bool qualified = strstr(text, "::") != NULL;
    const cbm_source_decl_t *decl = NULL;
    perl_lookup_t own = perl_own_sub(ctx, owner, leaf, &decl);
    if (own == PERL_FOUND) {
        cbm_source_emit_node(ctx->walk, node, kind, decl->qn,
                             qualified ? CBM_RESOLVE_RULE_LANGUAGE_BINDING
                                       : CBM_RESOLVE_RULE_SCOPE_BINDING,
                             owner, qualified ? "perl_qualified_sub" : "perl_current_package_sub");
        return;
    }
    if (own == PERL_AMBIGUOUS) {
        cbm_source_emit_node(ctx->walk, node, kind, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                             "perl_sub_name_not_one_declaration");
        return;
    }
    if (!qualified) {
        const char *module = NULL;
        const char *target = perl_imported_target(ctx, leaf, &module);
        if (target) {
            cbm_source_emit_node(ctx->walk, node, kind, target, CBM_RESOLVE_RULE_IMPORT_BINDING,
                                 module, "perl_exporter_import");
            return;
        }
    }
    cbm_source_emit_node(ctx->walk, node, kind, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                         "perl_sub_not_in_package");
}

static void perl_visit_function_call(perl_ctx_t *ctx, TSNode node) {
    TSNode function = cbm_source_field(node, "function");
    const char *raw = cbm_source_text(ctx->walk, function);
    if (!raw)
        return;
    bool ampersand = raw[0] == '&';
    const char *text = ampersand ? raw + 1 : raw;
    TSNode parent = ts_node_parent(node);
    /* `defined &name` and `exists &name` ask about the sub without calling it. */
    if (ampersand && cbm_source_is(parent, "func1op_call_expression")) {
        perl_emit(ctx, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "perl_sub_mentioned_not_called");
        return;
    }
    const char *owner = NULL, *leaf = NULL;
    perl_split(ctx, text, ctx->package, &owner, &leaf);
    bool qualified = strstr(text, "::") != NULL;
    if (!qualified && !ampersand && perl_builtin(leaf)) {
        perl_emit(ctx, node, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                  "perl_builtin_not_overridden_by_sub");
        return;
    }
    if (qualified && (!strcmp(owner, "CORE") || !strncmp(owner, "CORE::", 6))) {
        perl_emit(ctx, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "perl_core_namespace");
        return;
    }
    perl_emit_sub(ctx, node, CBM_SOURCE_SITE_CALL, text);
}

/* `\&name`: the sub the lookup selects, referred to rather than called. */
static void perl_visit_code_reference(perl_ctx_t *ctx, TSNode node) {
    const char *text = cbm_source_text(ctx->walk, node);
    const char *amp = text ? strchr(text, '&') : NULL;
    if (!amp || amp == text || amp[-1] != '\\')
        return;
    const char *name = amp + 1;
    for (const char *p = name; *p; p++)
        if (!(*p == ':' || *p == '_' || (*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'z') ||
              (*p >= 'A' && *p <= 'Z')))
            return; /* `\&$name` and `\&{...}` compute it. */
    if (name[0])
        perl_emit_sub(ctx, node, CBM_SOURCE_SITE_REFERENCE, name);
}

static bool perl_shifts_arguments(const perl_ctx_t *ctx, TSNode node);

/* The invocant variable a sub declares in its first statement or signature. */
static const char *perl_invocant(perl_ctx_t *ctx, TSNode sub) {
    uint32_t parts = ts_node_named_child_count(sub);
    for (uint32_t i = 0; i < parts; i++) {
        TSNode signature = ts_node_named_child(sub, i);
        if (!cbm_source_is(signature, "signature"))
            continue;
        TSNode first = ts_node_named_child(signature, 0);
        TSNode variable = ts_node_is_null(first) ? (TSNode){0} : ts_node_named_child(first, 0);
        return cbm_source_is(variable, "scalar") ? cbm_source_text(ctx->walk, variable) : NULL;
    }
    TSNode body = cbm_source_field(sub, "body");
    TSNode statement = ts_node_is_null(body) ? (TSNode){0} : ts_node_named_child(body, 0);
    TSNode assign = ts_node_is_null(statement) ? (TSNode){0} : ts_node_named_child(statement, 0);
    if (!cbm_source_is(assign, "assignment_expression"))
        return NULL;
    TSNode left = cbm_source_named_field(assign, "left");
    if (!cbm_source_is(left, "variable_declaration"))
        return NULL;
    TSNode value = cbm_source_named_field(assign, "right");
    const char *right = cbm_source_text(ctx->walk, value);
    TSNode single = cbm_source_field(left, "variable");
    if (cbm_source_is(single, "scalar"))
        return right && !strcmp(right, "shift") ? cbm_source_text(ctx->walk, single) : NULL;
    TSNode first = cbm_source_named_field(left, "variables");
    if (!cbm_source_is(first, "scalar"))
        return NULL;
    bool takes_first = right && !strcmp(right, "@_");
    /* `my ($self, $name) = (shift, shift);` */
    if (cbm_source_is(value, "list_expression") && ts_node_named_child_count(value))
        takes_first = perl_shifts_arguments(ctx, ts_node_named_child(value, 0));
    return takes_first ? cbm_source_text(ctx->walk, first) : NULL;
}

static bool perl_left_binds(perl_ctx_t *ctx, TSNode left, const char *variable, unsigned depth) {
    if (ts_node_is_null(left) || depth > 16)
        return false;
    if (cbm_source_is(left, "scalar")) {
        const char *text = cbm_source_text(ctx->walk, left);
        return text && !strcmp(text, variable);
    }
    /* `$self->{x} = 1` writes through the invocant; it does not rebind it. */
    if (!cbm_source_is(left, "variable_declaration") && !cbm_source_is(left, "list_expression") &&
        !cbm_source_is(left, "parenthesized_expression"))
        return false;
    uint32_t count = ts_node_named_child_count(left);
    for (uint32_t i = 0; i < count; i++)
        if (perl_left_binds(ctx, ts_node_named_child(left, i), variable, depth + 1))
            return true;
    return false;
}

static bool perl_rebinds(perl_ctx_t *ctx, TSNode node, const char *variable, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return true;
    if (cbm_source_is(node, "assignment_expression") &&
        perl_left_binds(ctx, cbm_source_named_field(node, "left"), variable, 0))
        return true;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        if (perl_rebinds(ctx, ts_node_named_child(node, i), variable, depth + 1))
            return true;
    return false;
}

static bool perl_has_signature(TSNode sub) {
    bool signature = false;
    TSTreeCursor cursor = ts_tree_cursor_new(sub);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            signature |= cbm_source_is(ts_tree_cursor_current_node(&cursor), "signature");
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    return signature;
}

/* The statement that took the invocant is the one write that is allowed. */
static bool perl_body_rebinds(perl_ctx_t *ctx, TSNode sub, TSNode body, const char *variable) {
    bool signature = perl_has_signature(sub);
    uint32_t count = ts_node_named_child_count(body);
    for (uint32_t i = signature ? 0 : 1; i < count; i++)
        if (perl_rebinds(ctx, ts_node_named_child(body, i), variable, 0))
            return true;
    return false;
}

/* `shift`, `shift()`, `shift @_` or `shift(@_)`: the sub's arguments, not another array. */
static bool perl_shifts_arguments(const perl_ctx_t *ctx, TSNode node) {
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    size_t length = end - start;
    const char *text = ctx->walk->source + start;
    if (length < 5 || length > 9 || strncmp(text, "shift", 5) ||
        strstr(ts_node_type(node), "bareword"))
        return false;
    return length == 5 || (length == 7 && !strncmp(text + 5, "()", 2)) ||
           (length == 8 && !strncmp(text + 5, " @_", 3)) ||
           (length == 9 && !strncmp(text + 5, "(@_)", 4));
}

/* The first shift of a sub's own arguments; a nested sub has an @_ of its own. */
static uint32_t perl_first_shift(perl_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth) ||
        cbm_source_is(node, "anonymous_subroutine_expression") ||
        cbm_source_is(node, "subroutine_declaration_statement"))
        return UINT32_MAX;
    if (ts_node_is_named(node) && perl_shifts_arguments(ctx, node))
        return ts_node_start_byte(node);
    uint32_t first = UINT32_MAX;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            first = perl_first_shift(ctx, ts_tree_cursor_current_node(&cursor), depth + 1);
        } while (first == UINT32_MAX && ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    return first;
}

/* Reached only as a method, by what the project's own calls and exports show. */
static bool perl_only_a_method(const perl_ctx_t *ctx, const char *name) {
    char key[256];
    int length = snprintf(key, sizeof(key), "%s%s", PERL_FUNCTION_CALL_PREFIX, name);
    if (length < 0 || (size_t)length >= sizeof(key) ||
        cbm_source_lookup(ctx->walk, ctx->package, key))
        return false;
    length = snprintf(key, sizeof(key), "%s%s", PERL_EXPORT_PREFIX, name);
    return length > 0 && (size_t)length < sizeof(key) &&
           !cbm_source_lookup(ctx->walk, ctx->package, key);
}

static bool perl_reads_invocant(const perl_ctx_t *ctx, TSNode invocant) {
    if (!ctx->reads_invocant)
        return false;
    if (perl_shifts_arguments(ctx, invocant))
        return ts_node_start_byte(invocant) == ctx->first_shift;
    uint32_t start = ts_node_start_byte(invocant);
    return ts_node_end_byte(invocant) - start == 5 &&
           !strncmp(ctx->walk->source + start, "$_[0]", 5) && start < ctx->first_shift;
}

static void perl_visit_method_call(perl_ctx_t *ctx, TSNode node, const char *invocant_var) {
    TSNode method = cbm_source_field(node, "method");
    TSNode invocant = cbm_source_field(node, "invocant");
    const char *name = cbm_source_is(method, "method") && !ts_node_named_child_count(method)
                           ? cbm_source_text(ctx->walk, method)
                           : NULL;
    if (!name) {
        perl_emit(ctx, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "perl_dynamic_method_name");
        return;
    }
    const char *start = NULL;
    if (cbm_source_is(invocant, "bareword")) {
        const char *text = cbm_source_text(ctx->walk, invocant);
        size_t length = text ? strlen(text) : 0;
        /* `Foo::Bar::->new` is the same class spelled unambiguously. */
        start = length > 2 && !strcmp(text + length - 2, "::")
                    ? cbm_arena_strndup(ctx->walk->arena, text, length - 2)
                    : text;
    } else if (cbm_source_is(invocant, "string_literal")) {
        start = perl_field_text(ctx, invocant, "content");
    } else if (cbm_source_is(invocant, "func0op_call_expression")) {
        const char *text = cbm_source_text(ctx->walk, invocant);
        start = text && !strcmp(text, "__PACKAGE__") ? ctx->package : NULL;
    } else if (cbm_source_is(invocant, "scalar") && invocant_var) {
        const char *text = cbm_source_text(ctx->walk, invocant);
        if (text && !strcmp(text, invocant_var))
            start = ctx->package;
    }
    if (!start && perl_reads_invocant(ctx, invocant))
        start = ctx->package;
    if (!start) {
        perl_emit(ctx, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "perl_invocant_class_unknown");
        return;
    }
    const cbm_source_decl_t *decl = NULL;
    perl_lookup_t found = PERL_ABSENT;
    cbm_source_chain_t chain = {0};
    if (!strncmp(name, "SUPER::", 7)) {
        /* SUPER is the parents of the package the call is compiled in. */
        const char *member = name + 7;
        const cbm_source_decl_t *records = cbm_source_lookup(ctx->walk, ctx->package, "");
        for (const cbm_source_decl_t *record = records; record && found == PERL_ABSENT;
             record = record->next) {
            if (record->flags & CBM_SOURCE_DECL_F_OPEN) {
                found = PERL_OPEN;
                break;
            }
            for (size_t i = 0; record->bases && record->bases[i] && found == PERL_ABSENT; i++)
                found = perl_find_method(ctx, record->bases[i], member, &chain, &decl);
        }
    } else if (strstr(name, "::")) {
        perl_emit(ctx, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "perl_qualified_method_name");
        return;
    } else {
        found = perl_find_method(ctx, start, name, &chain, &decl);
    }
    switch (found) {
    case PERL_FOUND:
        perl_emit(ctx, node, decl->qn, CBM_RESOLVE_RULE_TYPE_MEMBER, start,
                  "perl_method_resolution_order");
        break;
    case PERL_ABSENT:
        perl_emit(ctx, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "perl_method_absent_from_chain");
        break;
    case PERL_OPEN:
        perl_emit(ctx, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "perl_method_chain_leaves_project");
        break;
    case PERL_AMBIGUOUS:
        perl_emit(ctx, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "perl_method_not_one_declaration");
        break;
    }
}

static void perl_visit(perl_ctx_t *ctx, TSNode node, const char *invocant_var, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child))
                continue;
            if (cbm_source_is(child, "package_statement")) {
                const char *package = perl_field_text(ctx, child, "name");
                uint32_t parts = ts_node_named_child_count(child);
                TSNode block = (TSNode){0};
                for (uint32_t i = 0; i < parts; i++)
                    if (cbm_source_is(ts_node_named_child(child, i), "block"))
                        block = ts_node_named_child(child, i);
                if (ts_node_is_null(block)) {
                    ctx->package = package ? package : ctx->package;
                } else {
                    const char *saved = ctx->package;
                    ctx->package = package ? package : saved;
                    perl_visit(ctx, block, NULL, depth + 1);
                    ctx->package = saved;
                }
                continue;
            }
            if (cbm_source_is(child, "subroutine_declaration_statement")) {
                const char *variable = perl_invocant(ctx, child);
                bool conventional =
                    variable && (!strcmp(variable, "$self") || !strcmp(variable, "$class"));
                TSNode body = cbm_source_field(child, "body");
                if (conventional && !ts_node_is_null(body) &&
                    perl_body_rebinds(ctx, child, body, variable))
                    conventional = false;
                if (!ts_node_is_null(body)) {
                    uint32_t saved_shift = ctx->first_shift;
                    bool saved_reads = ctx->reads_invocant;
                    const char *name = perl_field_text(ctx, child, "name");
                    ctx->first_shift = perl_first_shift(ctx, body, depth + 1);
                    ctx->reads_invocant = name && strcmp(ctx->package, "main") &&
                                          !strstr(name, "::") && !perl_has_signature(child) &&
                                          perl_only_a_method(ctx, name);
                    perl_visit(ctx, body, conventional ? variable : NULL, depth + 1);
                    ctx->first_shift = saved_shift;
                    ctx->reads_invocant = saved_reads;
                }
                continue;
            }
            /* An anonymous sub closes over $self, but its @_ is its own. */
            if (cbm_source_is(child, "anonymous_subroutine_expression")) {
                bool saved_reads = ctx->reads_invocant, saved_list = ctx->overload_list;
                uint32_t saved_shift = ctx->first_shift;
                TSNode body = ts_node_named_child(child, 0);
                ctx->reads_invocant = saved_list && strcmp(ctx->package, "main");
                ctx->overload_list = false;
                ctx->first_shift = ctx->reads_invocant && !ts_node_is_null(body)
                                       ? perl_first_shift(ctx, body, depth + 1)
                                       : saved_shift;
                perl_visit(ctx, child, invocant_var, depth + 1);
                ctx->reads_invocant = saved_reads;
                ctx->overload_list = saved_list;
                ctx->first_shift = saved_shift;
                continue;
            }
            if (cbm_source_is(child, "use_statement")) {
                const char *pragma = cbm_source_text(ctx->walk, ts_node_named_child(child, 0));
                bool saved_list = ctx->overload_list;
                ctx->overload_list = pragma && !strcmp(pragma, "overload");
                perl_visit(ctx, child, invocant_var, depth + 1);
                ctx->overload_list = saved_list;
                continue;
            }
            if (cbm_source_is(child, "refgen_expression"))
                perl_visit_code_reference(ctx, child);
            if (cbm_source_is(child, "function_call_expression") ||
                cbm_source_is(child, "ambiguous_function_call_expression"))
                perl_visit_function_call(ctx, child);
            else if (cbm_source_is(child, "method_call_expression"))
                perl_visit_method_call(ctx, child, invocant_var);
            perl_visit(ctx, child, invocant_var, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void perl_resolve(cbm_source_walk_t *walk) {
    perl_ctx_t ctx = {.walk = walk, .package = "main"};
    perl_gather_imports(&ctx, walk->root, 0);
    ctx.package = "main";
    perl_visit(&ctx, walk->root, NULL, 0);
}

const cbm_source_adapter_t cbm_source_adapter_perl = {.language = CBM_LANG_PERL,
                                                      .collect = perl_collect,
                                                      .resolve = perl_resolve,
                                                      .decides_references = true,
                                                      .contains_parse_damage = true};
