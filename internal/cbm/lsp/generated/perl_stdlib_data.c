/*
 * perl_stdlib_data.c — Perl stdlib + CPAN type data (stub).
 *
 * Strategy mirrors php_stdlib_data.c:
 *   1. perlfunc builtins (print, bless, ref, ...) as global functions.
 *   2. Curated, corpus-driven CPAN types (Moose, Moo, DBI, ...).
 *
 * This file is a placeholder: it registers just a couple of builtins so the
 * cbm_perl_stdlib_register symbol exists and links through the lsp_all.c
 * unity build. The full perlfunc + CPAN seed lands in a later plan.
 *
 * TODO(plan 22-02): full perlfunc + CPAN seed
 */

#include "../type_rep.h"
#include "../type_registry.h"
#include "../../arena.h"
#include "../perl_lsp.h"
#include <string.h>

#define MIXED cbm_type_unknown()

/* Register a global (package-less) builtin function returning `ret_type_`. */
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

void cbm_perl_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena) {
    CBMRegisteredFunc rf;

    /* ── placeholder perlfunc builtins ──────────────────────────── */
    REG_BUILTIN("print", MIXED);
    REG_BUILTIN("bless", MIXED);
    REG_BUILTIN("ref", MIXED);
}
