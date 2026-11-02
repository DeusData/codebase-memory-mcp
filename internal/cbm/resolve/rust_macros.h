#ifndef CBM_RESOLVE_RUST_MACROS_H
#define CBM_RESOLVE_RUST_MACROS_H

#include "../arena.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Macros whose definition says what their input is. tokio declares most of its tree inside
 *
 *     macro_rules! cfg_rt { ($($item:item)*) => { $( #[cfg(feature = "rt")] $item )* } }
 *
 * whose expansion is its input items, in order, with attributes added. So `cfg_rt! { pub mod
 * runtime; }` declares module runtime, and that is a fact read from the definition. Its tests call
 *
 *     macro_rules! assert_ok { ($e:expr) => {{ match $e { ... } }}; ($e:expr, $($msg:tt)+) => ... }
 *
 * whose arguments are expressions of the caller, evaluated. Every whole-file parse reads such a
 * file through a same-length view: for items the macro's name, `!` and delimiters are blanked, for
 * expressions the name and `!`, which leaves a parenthesized expression. The extractor, the name
 * rules and the occurrence walker then see plain code at its real offsets. What the macro's value
 * is stays unknown: `expressions` lists those parentheses for the walker.
 *
 * A name is such a macro only when every `macro_rules!` of that name in the invoking file's package
 * says so, so whichever one textual scope selects, the meaning is the same. Another package's macro
 * counts only when exported and named through its crate: `use tokio_test::assert_ok` or
 * `tokio_test::assert_ok!`. A file that imports a package's macro name from a crate the project
 * does not hold keeps it opaque.
 *
 * pin_project_lite::pin_project! is the one macro read from its contract instead, in a file that
 * names that crate: its input is the struct or enum it declares, plus a PinnedDrop impl. */
typedef struct cbm_rust_item_macros cbm_rust_item_macros_t;

cbm_rust_item_macros_t *cbm_rust_item_macros_new(void);
void cbm_rust_item_macros_free(cbm_rust_item_macros_t *table);

/* Every Cargo.toml of the project first, so a source file finds its package and crate. */
bool cbm_rust_item_macros_add_manifest(cbm_rust_item_macros_t *table, const char *rel,
                                       const char *text, size_t length);
bool cbm_rust_item_macros_add_source(cbm_rust_item_macros_t *table, const char *rel,
                                     const char *source, size_t length);

/* The table every parse reads, owned from here on; a previous one is freed and NULL ends it.
 * One active pipeline at a time, like the package map, and read-only while workers run. */
void cbm_rust_item_macros_install(cbm_rust_item_macros_t *table);

/* A same-length copy of the source with this file's readable macros unwrapped, or NULL when it has
 * none. The caller frees it with cbm_free(CBM_MEM_CLASS_EXTRACT, ...). Linear in length. */
char *cbm_rust_item_view(const char *rel, const char *source, size_t length);

typedef struct {
    /* Outermost item wrappers as [start, end) byte pairs in source order. Their items are
     * conditional, since these macros exist to add #[cfg]. */
    const uint32_t *wrapped;
    int nwrapped;
    /* Where each expression macro's `(` stands, in source order. */
    const uint32_t *expressions;
    int nexpressions;
} cbm_rust_item_spans_t;

bool cbm_rust_item_spans(CBMArena *arena, const char *rel, const char *source, size_t length,
                         cbm_rust_item_spans_t *out);

#endif
