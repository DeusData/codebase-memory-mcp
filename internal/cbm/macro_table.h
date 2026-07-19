#pragma once
#include <stddef.h>
#include "arena.h"

#define CBM_MACRO_MAX_PARAMS 4
#define CBM_MACRO_TABLE_CAP 4096

typedef struct {
    const char *name;
    int param_count;
    const char *param_names[CBM_MACRO_MAX_PARAMS];
    const char *expansion;
    const char *resolved_callee;
} CBMMacroEntry;

typedef struct CBMMacroTable {
    CBMMacroEntry entries[CBM_MACRO_TABLE_CAP];
    int count;
    CBMArena arena;
} CBMMacroTable;

// Add an entry. Silently drops on overflow.
void cbm_macro_table_add(CBMMacroTable *t, CBMArena *arena, const char *name, int param_count,
                         const char **param_names, const char *expansion,
                         const char *resolved_callee);

// Look up by name. Returns NULL if not found.
const CBMMacroEntry *cbm_macro_table_find(const CBMMacroTable *t, const char *name);

// Parse a single .inc file content into the table (arena-allocated strings).
void cbm_parse_inc_file(CBMMacroTable *t, CBMArena *arena, const char *content);

// Expand a macro call: substitute args into expansion text.
// Returns arena-allocated expanded text, or NULL if no expansion.
char *cbm_macro_expand(CBMArena *arena, const CBMMacroEntry *entry, const char **args,
                       int arg_count);

// Extract a callee name from expanded text (looks for ##class(X).Method or $$Label^Routine).
// Returns arena-allocated "X.Method" or "Label^Routine", or NULL.
char *cbm_macro_extract_callee(CBMArena *arena, const char *expansion);

// Allocate and populate a new table with the hardcoded system macros.
// Caller owns the table (stack or heap).
void cbm_macro_table_init_system(CBMMacroTable *t);

// Destroy the arena inside t and free t itself. NULL-safe.
void cbm_macro_table_free(CBMMacroTable *t);
