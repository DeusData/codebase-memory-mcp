/*
 * perl_stdlib_data.c — hand-written Perl stdlib + CPAN type data.
 *
 * Strategy mirrors php_stdlib_data.c (docs/PLAN_PHP_LSP_INTEGRATION.md §6):
 *   1. perlfunc core built-ins (print, bless, ref, ...) registered as global,
 *      package-less functions reachable from any namespace.
 *   2. Curated, corpus-driven CPAN OOP modules (Scalar::Util, List::Util,
 *      Carp, POSIX, Storable, Data::Dumper) registered as module-qualified
 *      functions.
 *
 * Module-qualified functions use dotted QNs (Foo.Bar.func) to match
 * perl_pkg_to_dot (Foo::Bar -> Foo.Bar) so an Exporter import map
 * (plan 22-03) can resolve `use Scalar::Util qw(blessed)` to these symbols.
 *
 * Return types are left UNKNOWN (cbm_type_unknown) for v1: real signature
 * inference is out of scope here — this seed only provides a baseline symbol
 * table for the resolver. Moose meta stubs (has/extends/with) are deferred
 * (Open Question #4).
 */

#include "../type_rep.h"
#include "../type_registry.h"
#include "../../arena.h"
#include "../perl_lsp.h"
#include <string.h>

#define MIXED cbm_type_unknown()

/* Register a global (package-less) built-in function returning `ret_type_`.
 * Reachable from any package — short_name == qualified_name (bare name). */
#define REG_BUILTIN(name_, ret_type_)                                                           \
    do {                                                                                        \
        memset(&rf, 0, sizeof(rf));                                                             \
        rf.min_params = -1;                                                                     \
        rf.qualified_name = (name_);                                                            \
        rf.short_name = (name_);                                                                \
        {                                                                                       \
            const CBMType **rets = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets)); \
            rets[0] = (ret_type_);                                                              \
            rets[1] = NULL;                                                                     \
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);                              \
        }                                                                                       \
        cbm_registry_add_func(reg, rf);                                                         \
    } while (0)

/* Register a module-qualified function (an exported sub, not a method).
 * `module_dot_` is the dotted package QN (e.g. "Scalar.Util"); `name_` is the
 * bare sub name. QN becomes "Scalar.Util.blessed"; short_name stays bare so an
 * Exporter import map can resolve `use Scalar::Util qw(blessed)`. */
#define REG_FUNC(module_dot_, name_, ret_type_)                                                 \
    do {                                                                                        \
        memset(&rf, 0, sizeof(rf));                                                             \
        rf.min_params = -1;                                                                     \
        rf.qualified_name = cbm_arena_sprintf(arena, "%s.%s", (module_dot_), (name_));          \
        rf.short_name = (name_);                                                                \
        {                                                                                       \
            const CBMType **rets = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets)); \
            rets[0] = (ret_type_);                                                              \
            rets[1] = NULL;                                                                     \
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);                              \
        }                                                                                       \
        cbm_registry_add_func(reg, rf);                                                         \
    } while (0)

void cbm_perl_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena) {
    CBMRegisteredFunc rf;

    /* ── perlfunc core built-ins (global, package-less) ─────────────
     * Source: RESEARCH.md L365 (perldoc perlfunc core list). Reachable from
     * any package; return types unknown for v1. */
    REG_BUILTIN("print", MIXED);
    REG_BUILTIN("printf", MIXED);
    REG_BUILTIN("sprintf", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("open", MIXED);
    REG_BUILTIN("close", MIXED);
    REG_BUILTIN("push", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("pop", MIXED);
    REG_BUILTIN("shift", MIXED);
    REG_BUILTIN("unshift", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("map", MIXED);
    REG_BUILTIN("grep", MIXED);
    REG_BUILTIN("sort", MIXED);
    REG_BUILTIN("join", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("split", MIXED);
    REG_BUILTIN("length", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("substr", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("chomp", MIXED);
    REG_BUILTIN("chop", MIXED);
    REG_BUILTIN("die", MIXED);
    REG_BUILTIN("warn", MIXED);
    REG_BUILTIN("ref", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("bless", MIXED);
    REG_BUILTIN("defined", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("exists", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("delete", MIXED);
    REG_BUILTIN("scalar", MIXED);
    REG_BUILTIN("keys", MIXED);
    REG_BUILTIN("values", MIXED);
    REG_BUILTIN("each", MIXED);

    /* ── Scalar::Util ───────────────────────────────────────────────
     * Source: RESEARCH.md L366. Exported subs; module QN "Scalar.Util". */
    REG_FUNC("Scalar.Util", "blessed", MIXED);
    REG_FUNC("Scalar.Util", "reftype", cbm_type_builtin(arena, "string"));
    REG_FUNC("Scalar.Util", "weaken", MIXED);

    /* ── List::Util ─────────────────────────────────────────────────
     * Source: RESEARCH.md L366. Module QN "List.Util". */
    REG_FUNC("List.Util", "sum", MIXED);
    REG_FUNC("List.Util", "max", MIXED);
    REG_FUNC("List.Util", "min", MIXED);
    REG_FUNC("List.Util", "first", MIXED);
    REG_FUNC("List.Util", "reduce", MIXED);

    /* ── Carp ───────────────────────────────────────────────────────
     * Source: RESEARCH.md L367. Module QN "Carp". */
    REG_FUNC("Carp", "croak", MIXED);
    REG_FUNC("Carp", "carp", MIXED);
    REG_FUNC("Carp", "confess", MIXED);
    REG_FUNC("Carp", "cluck", MIXED);

    /* ── POSIX (commonly-imported entry points) ─────────────────────
     * Source: RESEARCH.md L367. Module QN "POSIX". */
    REG_FUNC("POSIX", "floor", MIXED);
    REG_FUNC("POSIX", "ceil", MIXED);
    REG_FUNC("POSIX", "strftime", cbm_type_builtin(arena, "string"));
    REG_FUNC("POSIX", "INT_MAX", cbm_type_builtin(arena, "int"));

    /* ── Storable ───────────────────────────────────────────────────
     * Source: RESEARCH.md L367. Module QN "Storable". */
    REG_FUNC("Storable", "dclone", MIXED);
    REG_FUNC("Storable", "freeze", cbm_type_builtin(arena, "string"));
    REG_FUNC("Storable", "thaw", MIXED);
    REG_FUNC("Storable", "nstore", MIXED);
    REG_FUNC("Storable", "retrieve", MIXED);

    /* ── Data::Dumper ───────────────────────────────────────────────
     * Source: RESEARCH.md L367. Module QN "Data.Dumper". */
    REG_FUNC("Data.Dumper", "Dumper", cbm_type_builtin(arena, "string"));
}
