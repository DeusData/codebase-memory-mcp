/*
 * doclink.h — doc-comment references, extraction half.
 *
 * A definition's complete doc comment (extract_defs.c stores it whole) can
 * name other code: C# `<see cref="..."/>`, Java `{@link ...}`, Rust intra-doc
 * links, and so on. Extraction only FINDS those references; whether one names
 * a graph node is decided later, per file, by the pipeline's resolver
 * (src/pipeline/doc_links.c), which turns them into MENTIONS edges or
 * doc_link_unresolved rows.
 *
 * A language leg plugs in at two places of this half, and nowhere else:
 *   - its link families: lines of CBM_DOCLINK_FAMILY_LIST below (enum value,
 *     name, ship gate);
 *   - one row of the language table in doclink.c:
 *       parse_doc       one documented definition's doc text -> CBMDocLink
 *                       tokens
 *       scan_scope      the file's doc-link SCOPE: a language-tagged text blob
 *                       with what other files' resolution needs from this file
 *                       (C#: namespaces, usings, type and member
 *                       declarations). A pure function of the file's bytes,
 *                       persisted with the file's LSP surface, and so
 *                       identical on full and incremental runs. Optional.
 *       scope_tag,      the blob's tag line and its persisted form (line
 *       portable_scope  numbers dropped). Optional.
 * A language without a row produces no tokens and no scope. The resolving
 * half's hooks are a cbm_doclink_resolver_t (src/pipeline/doc_links.h).
 */
#ifndef CBM_DOCLINK_H
#define CBM_DOCLINK_H

#include "cbm.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Every link family, across languages: one line per (language, reference
 * form), and the ONE place a language leg adds its families --
 *
 *   X(enum value, language, name, external, ships)
 *
 *   name      the "syntax" MENTIONS-edge property and doc_link_unresolved
 *             column, so it is public surface; with the language it is the
 *             tier a release audit judges. Names repeat across languages
 *             (`see` is C#'s, Java's, Kotlin's ...): the value, not the name,
 *             identifies the family, and a row's language is its file's.
 *   external  outside the repository by construction (a URL): never looked up
 *   ships     the SHIP GATE. true: a resolved reference becomes a MENTIONS
 *             edge. false: the tier is below the audit's bar; its references
 *             still resolve, but a resolved one stays a doc_link_unresolved
 *             row with reason below_bar_tier (an unresolved one keeps its own
 *             reason). Nothing else about the family changes.
 *
 * The enum below and the family table (doclink.c) are both generated from this
 * list, so a value cannot exist without its name and its gate. */
#define CBM_DOCLINK_FAMILY_LIST(X)                                           \
    /* csharp */                                                             \
    X(CBM_DOCLINK_CS_SEE, CBM_LANG_CSHARP, "see", false, true)               \
    X(CBM_DOCLINK_CS_SEEALSO, CBM_LANG_CSHARP, "seealso", false, true)       \
    X(CBM_DOCLINK_CS_EXCEPTION, CBM_LANG_CSHARP, "exception", false, true)   \
    X(CBM_DOCLINK_CS_INHERITDOC, CBM_LANG_CSHARP, "inheritdoc", false, true) \
    /* any language (CBM_LANG_COUNT): a URL is never an edge */              \
    X(CBM_DOCLINK_HREF, CBM_LANG_COUNT, "href", true, false)

typedef enum {
    CBM_DOCLINK_NONE = 0,
#define CBM_DOCLINK_FAMILY_VALUE(id, lang, name, external, ships) id,
    CBM_DOCLINK_FAMILY_LIST(CBM_DOCLINK_FAMILY_VALUE)
#undef CBM_DOCLINK_FAMILY_VALUE
        CBM_DOCLINK_SYNTAX_COUNT
} CBMDocLinkSyntax;

/* One link family: a line of CBM_DOCLINK_FAMILY_LIST. */
typedef struct {
    CBMLanguage lang; /* the language whose doc comments write it; CBM_LANG_COUNT: any */
    const char *name;
    bool external;
    bool ships;
} CBMDocLinkFamily;

/* The family of a syntax value; NULL for a value that names none. */
const CBMDocLinkFamily *cbm_doclink_family(int syntax);

/* "see", "seealso", "exception", "inheritdoc", "href"; "" for an unknown value. */
const char *cbm_doclink_syntax_name(int syntax);

/* True for a syntax whose reference is outside the repository by construction
 * (a URL): the resolver records it as `external` without a lookup. */
bool cbm_doclink_syntax_is_external(int syntax);

/* True when resolved references of this family become edges (the ship gate). */
bool cbm_doclink_syntax_ships(int syntax);

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Test seam: ship or hold back one family whatever the table says, until
 * cbm_doclink_test_reset_ships. Test builds only. */
void cbm_doclink_test_set_ships(int syntax, bool ships);
void cbm_doclink_test_reset_ships(void);
#endif

/* True when `lang` has a doc-reference parser. */
bool cbm_doclink_lang_supported(CBMLanguage lang);

/* Remember that the doc text `doc` (as returned by the doc-comment extractor)
 * starts at 1-based source line `line`. Called by doc_run_text; a no-op for
 * languages without a parser. */
void cbm_doclink_note_doc_line(CBMExtractCtx *ctx, const char *doc, uint32_t line);

/* Harvest the references of every documented definition into
 * ctx->result->doc_links and the file's scope into ctx->result->doc_scope.
 * Called once per file at the end of extraction, with the tree still alive. */
void cbm_doclinks_extract(CBMExtractCtx *ctx);

/* Append one token (copies nothing: `raw` must live in the result arena). */
void cbm_doclinks_push(CBMDocLinkArray *arr, CBMArena *a, CBMDocLink link);

/* The scope as persisted with the file's LSP surface: line numbers zeroed, so
 * an edit that only moves lines leaves it (and the surface hash) unchanged.
 * Other files' references never read a file's lines; only the file's own
 * references do, and those are resolved from its fresh extraction. Returns a
 * memory-core block (release with cbm_free(CBM_MEM_CLASS_OTHER, p)), NULL on
 * allocation failure. */
char *cbm_doclink_portable_scope(const char *scope);

/* ── C# (doclink_cs.c) ─────────────────────────────────────────────── */

void cbm_doclink_cs_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                              uint32_t doc_line);
const char *cbm_doclink_cs_scan_scope(CBMExtractCtx *ctx);
char *cbm_doclink_cs_portable_scope(const char *scope);

/* Normalize one C# parameter type as written in a declaration or a cref
 * parameter list: attributes, ref/out/in/params/this/scoped modifiers, type
 * arguments, namespaces, nullable markers and a trailing parameter name are
 * dropped, BCL names map to their keyword (Int32 -> int), array and pointer
 * suffixes stay. "?" when nothing is left. Writes a NUL-terminated string into
 * `out` (truncated at cap) and returns its length. */
size_t cbm_doclink_cs_norm_type(const char *in, size_t len, char *out, size_t cap);

/* The C# scope blob starts with this tag line. */
#define CBM_DOCLINK_CS_SCOPE_TAG "cs1"

#endif /* CBM_DOCLINK_H */
