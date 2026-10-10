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
 *       scan_file       instead of parse_doc, for a DOCUMENT language whose
 *                       text is the documentation (Markdown): one pass over
 *                       the file's bytes pushes every reference, with the
 *                       section it is written in as its source. Optional.
 *     State a language's hooks need between their calls for one file lives in
 *     ctx->doclink_state (cbm.h), never in a static or thread-local.
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
 *   X(enum value, language, name, external, ships, edge)
 *
 *   name      the "syntax" edge property and doc_link_unresolved
 *             column, so it is public surface; with the language it is the
 *             tier a release audit judges. Names repeat across languages
 *             (`see` is C#'s, Java's, Kotlin's ...): the value, not the name,
 *             identifies the family, and a row's language is its file's.
 *   external  outside the repository by construction (a URL): never looked up
 *   ships     the SHIP GATE. true: a resolved reference becomes a MENTIONS
 *             edge. false: the tier is below the audit's bar; its references
 *             still resolve, but a resolved one stays a doc_link_unresolved
 *             row with reason below_bar_tier (an unresolved one keeps its own
 *             reason). Nothing else about the family changes. A document
 *             family ships once a held-out audit passed it: below the bar
 *             (AsciiDoc code spans naming paths, AsciiDoc code names, reST
 *             object directives; Markdown code spans naming a file or one
 *             directory alone, split off the Markdown code-path tier after
 *             its held-out failure; ADR supersedes words in running prose,
 *             split off after three held-out censuses failed on them), or
 *             not yet audited for want of held-out occurrences (reST
 *             include, kernel_doc; AsciiDoc attribute) means false.
 *             Markdown code paths without the bare part, reST
 *             literalinclude and code paths, PDF qualified names after the
 *             owner rule, and ADR supersedes statements (the phrase opens
 *             its line or field and names a record: 80 of 81 on a fresh
 *             census) passed supplementary held-out audits.
 *   edge      the type of the edge a resolved reference becomes: MENTIONS
 *             (documentation names code), SUPERSEDES (an ADR states that it
 *             supersedes another). The edge's source is always the
 *             definition the reference is written in, so a file owns the
 *             edges its text produces, whatever their type.
 *
 * The enum below and the family table (doclink.c) are both generated from this
 * list, so a value cannot exist without its name and its gate. */
#define CBM_DOCLINK_FAMILY_LIST(X)                                                             \
    /* csharp */                                                                               \
    X(CBM_DOCLINK_CS_SEE, CBM_LANG_CSHARP, "see", false, true, "MENTIONS")                     \
    X(CBM_DOCLINK_CS_SEEALSO, CBM_LANG_CSHARP, "seealso", false, true, "MENTIONS")             \
    X(CBM_DOCLINK_CS_EXCEPTION, CBM_LANG_CSHARP, "exception", false, true, "MENTIONS")         \
    X(CBM_DOCLINK_CS_INHERITDOC, CBM_LANG_CSHARP, "inheritdoc", false, true, "MENTIONS")       \
    /* markdown (doclink_md.c): what a document's text names explicitly */                     \
    X(CBM_DOCLINK_MD_LINK, CBM_LANG_MARKDOWN, "link", false, true, "MENTIONS")                 \
    X(CBM_DOCLINK_MD_PATH, CBM_LANG_MARKDOWN, "path", false, true, "MENTIONS")                 \
    X(CBM_DOCLINK_MD_CODE_PATH, CBM_LANG_MARKDOWN, "code_path", false, true, "MENTIONS")       \
    /* a code span naming a file or directory alone (`config.yaml`, `doc/`) */                 \
    X(CBM_DOCLINK_MD_BARE_PATH, CBM_LANG_MARKDOWN, "bare_path", false, false, "MENTIONS")      \
    X(CBM_DOCLINK_MD_CODE_NAME, CBM_LANG_MARKDOWN, "code_name", false, true, "MENTIONS")       \
    /* an ADR's own "supersedes" / "replaces" statement, opening its line or field */          \
    /* and naming a record directly (doc_adr.c): ADR -> ADR */                                 \
    X(CBM_DOCLINK_MD_SUPERSEDES, CBM_LANG_MARKDOWN, "supersedes", false, true, "SUPERSEDES")   \
    /* the same words in running prose ("this record supersedes ADR-3") */                     \
    X(CBM_DOCLINK_MD_SUPERSEDES_PROSE, CBM_LANG_MARKDOWN, "supersedes_prose", false, false,    \
      "SUPERSEDES")                                                                            \
    /* pdf (doc_pdf.c): structural mentions in a page's text; pdf_name (bare      */           \
    /* names) only decides which line fragments a cross-line join replaces        */           \
    X(CBM_DOCLINK_PDF_PATH, CBM_LANG_PDF, "pdf_path", false, true, "MENTIONS")                 \
    X(CBM_DOCLINK_PDF_FILE, CBM_LANG_PDF, "pdf_file", false, true, "MENTIONS")                 \
    X(CBM_DOCLINK_PDF_QN, CBM_LANG_PDF, "pdf_qn", false, true, "MENTIONS")                     \
    X(CBM_DOCLINK_PDF_NAME, CBM_LANG_PDF, "pdf_name", false, false, "MENTIONS")                \
    /* restructuredtext (doclink_rst.c): Sphinx domain markup, directives, inline literals */  \
    X(CBM_DOCLINK_RST_ROLE, CBM_LANG_RST, "role", false, true, "MENTIONS")                     \
    X(CBM_DOCLINK_RST_OBJECT, CBM_LANG_RST, "object", false, true, "MENTIONS")                 \
    X(CBM_DOCLINK_RST_AUTODOC, CBM_LANG_RST, "autodoc", false, true, "MENTIONS")               \
    X(CBM_DOCLINK_RST_INCLUDE, CBM_LANG_RST, "include", false, false, "MENTIONS")              \
    X(CBM_DOCLINK_RST_LITERALINCLUDE, CBM_LANG_RST, "literalinclude", false, true, "MENTIONS") \
    X(CBM_DOCLINK_RST_KERNEL_DOC, CBM_LANG_RST, "kernel_doc", false, false, "MENTIONS")        \
    X(CBM_DOCLINK_RST_CODE_PATH, CBM_LANG_RST, "code_path", false, true, "MENTIONS")           \
    X(CBM_DOCLINK_RST_CODE_NAME, CBM_LANG_RST, "code_name", false, true, "MENTIONS")           \
    /* asciidoc (doc_adoc.c): includes, Antora API attributes, monospace spans */              \
    X(CBM_DOCLINK_ADOC_INCLUDE, CBM_LANG_ASCIIDOC, "include", false, true, "MENTIONS")         \
    X(CBM_DOCLINK_ADOC_ATTRIBUTE, CBM_LANG_ASCIIDOC, "attribute", false, false, "MENTIONS")    \
    X(CBM_DOCLINK_ADOC_CODE_PATH, CBM_LANG_ASCIIDOC, "code_path", false, false, "MENTIONS")    \
    X(CBM_DOCLINK_ADOC_CODE_NAME, CBM_LANG_ASCIIDOC, "code_name", false, false, "MENTIONS")    \
    /* any language (CBM_LANG_COUNT): a URL is never an edge */                                \
    X(CBM_DOCLINK_HREF, CBM_LANG_COUNT, "href", true, false, "MENTIONS")

typedef enum {
    CBM_DOCLINK_NONE = 0,
#define CBM_DOCLINK_FAMILY_VALUE(id, lang, name, external, ships, edge) id,
    CBM_DOCLINK_FAMILY_LIST(CBM_DOCLINK_FAMILY_VALUE)
#undef CBM_DOCLINK_FAMILY_VALUE
        CBM_DOCLINK_SYNTAX_COUNT
} CBMDocLinkSyntax;

/* CBMDocLink.flags. */
enum {
    /* The reference is written in the FILE's own doc (CBMFileResult.module_doc:
     * a Go package comment, Rust `//!` inner docs), not in a definition's. Its
     * edge source is the file's File node, which the resolving half looks up
     * from the file it is resolving; `source_qn` is not the source then. Set
     * by the driver (cbm_doclinks_extract) on what a parser pushes for the
     * file-level doc -- a parser never sets it. A scan_file hook sets it on a
     * reference written before the document's first section. */
    CBM_DOCLINK_FLAG_FILE = 1,
    /* A PDF mention that runs across a line break, proposed as one reference.
     * `raw` holds its forms (separated by 0x1F) and, after 0x1E, the indices
     * of the line-local tokens it replaces when it resolves (doc_pdf.c). */
    CBM_DOCLINK_FLAG_JOIN = 2,
};

/* One link family: a line of CBM_DOCLINK_FAMILY_LIST. */
typedef struct {
    CBMLanguage lang; /* the language whose doc comments write it; CBM_LANG_COUNT: any */
    const char *name;
    bool external;
    bool ships;
    const char *edge; /* the type of the edge a resolved reference becomes */
} CBMDocLinkFamily;

/* "MENTIONS" or "SUPERSEDES"; "MENTIONS" for an unknown value. */
const char *cbm_doclink_syntax_edge(int syntax);

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
 * Called once per file at the end of extraction, with the tree still alive.
 *
 * A file-level doc (ctx->result->module_doc) goes through the same parse_doc
 * hook once more: `def` is then a stand-in for the FILE (label "File", name
 * and file_path the relative path, qualified_name the file's module QN,
 * start_line 1) and `doc_line` the doc's first line. The parser fills its
 * tokens exactly as for a definition; the driver marks them
 * CBM_DOCLINK_FLAG_FILE afterwards. */
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

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Fail one selected allocation attempt, then automatically disarm. */
enum {
    CBM_DOCLINK_ALLOC_SPAN,
    CBM_DOCLINK_ALLOC_TEXT,
    CBM_DOCLINK_ALLOC_VALUE,
    CBM_DOCLINK_ALLOC_TOKENS,
    CBM_DOCLINK_ALLOC_SCOPE,    /* the C# scope scan, as if its builder ran out */
    CBM_DOCLINK_ALLOC_PROJECT,  /* the project-file scan, likewise */
    CBM_DOCLINK_ALLOC_DOC_LINE, /* the doc-line map's growth */
    CBM_DOCLINK_ALLOC_KINDS,
};
void cbm_doclink_test_fail_alloc_after(int kind, int nth);
bool cbm_doclink_test_fail_alloc(int kind);
void cbm_doclink_test_reset_alloc(void);

/* The C# scope scan of the file at `rel_path` writes, after its records, one
 * the resolver refuses -- as if writer and reader disagreed (NULL or "":
 * none). What is stored for the file and what this run resolves both carry
 * it. */
void cbm_doclink_cs_test_spoil_scope(const char *rel_path);

/* Actual comment memcpy bytes, input bytes submitted to the C# lexical parser,
 * and bytes allocated for cleaned reference values; separate from token output. */
void cbm_doclink_test_doc_work_reset(void);
void cbm_doclink_test_doc_work(uint64_t *copied, uint64_t *parse_input, uint64_t *cleaned);
void cbm_doclink_test_note_doc_work(uint64_t copied, uint64_t parse_input, uint64_t cleaned);
#endif

/* ── C# (doclink_cs.c) ─────────────────────────────────────────────── */

void cbm_doclink_cs_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                              uint32_t doc_line);
/* False if allocating a value or appending a token failed. Only a complete
 * lexical result can be shared with another definition. */
bool cbm_doclink_cs_parse_doc_checked(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                                      uint32_t doc_line);
const char *cbm_doclink_cs_scan_scope(CBMExtractCtx *ctx);
char *cbm_doclink_cs_portable_scope(const char *scope);

/* MSBuild project files (*.csproj, *.props, *.targets) set the global usings
 * of a C# project, so they have a scope blob too: the C# resolver evaluates
 * those blobs and never opens a file. The blob carries the C# tag. The scan
 * is gated by the file's name: any other XML file costs nothing and has no
 * blob. A project file holds no doc references: its parse_doc does nothing. */
void cbm_doclink_cs_project_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                                      uint32_t doc_line);
const char *cbm_doclink_cs_project_scan_scope(CBMExtractCtx *ctx);

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Test seam: what the C# scope scans since the last reset cost -- the text
 * positions, brace-stack entries and modifier children visited, and the bytes
 * they took from the scratch arena. A test holds these against the size of its input, so that a
 * scan whose cost grows faster than its input fails without a clock. Test
 * builds only. */
void cbm_doclink_cs_test_cost_reset(void);
void cbm_doclink_cs_test_cost(uint64_t *text_steps, uint64_t *scratch_bytes);
/* Completed C# scope-builder bytes since the cost reset, including any embedded
 * terminator. A direct-extraction test compares this with the returned C string. */
uint64_t cbm_doclink_cs_test_scope_bytes(void);
/* Import-index construction and a one-shot candidate-buffer failure. Query
 * visits remain in the pipeline's existing test_work counter. */
uint64_t cbm_doclink_cs_test_index_work(void);
void cbm_doclink_cs_test_fail_candidate_alloc(bool enabled);
bool cbm_doclink_cs_test_candidate_alloc_failed(void);
#endif

/* Normalize one C# parameter type as written in a declaration or a cref
 * parameter list: attributes, ref/out/in/params/this/scoped modifiers, type
 * arguments, namespaces, nullable markers and a trailing parameter name are
 * dropped, BCL names map to their keyword (Int32 -> int), array and pointer
 * suffixes stay. "?" (a type nothing is known about) when nothing is left, when
 * the text is too long to be a type, or when the result does not fit `out`:
 * never a cut name. Writes a NUL-terminated string and returns its length. */
size_t cbm_doclink_cs_norm_type(const char *in, size_t len, char *out, size_t cap);

/* The C# scope blob starts with this tag line. */
#define CBM_DOCLINK_CS_SCOPE_TAG "cs1"

/* ── Markdown (doclink_md.c) ───────────────────────────────────────── */

/* The scan_file hook: every explicit reference of a Markdown or MDX file (a
 * link destination, a bare path, a path or a qualified name in a code span),
 * each with the Section definition it is written under as its source. */
void cbm_doclink_md_scan_file(CBMExtractCtx *ctx);

/* What a code span's text names. */
typedef enum {
    CBM_DOCLINK_MD_NONE = 0,  /* nothing linkable (a bare name, a command, a snippet, ...) */
    CBM_DOCLINK_MD_FILE,      /* a path with a directory part, or a dot file */
    CBM_DOCLINK_MD_FILENAME,  /* a file name alone: it names a file of the document's
                               * own directory, never one found elsewhere */
    CBM_DOCLINK_MD_DIR,       /* a directory */
    CBM_DOCLINK_MD_QUALIFIED, /* a qualified name of two or more identifiers
                               * (`a.b`, `a::b`, `A#b`, `A\B`, `mod:attr`) */
} CBMDocLinkMdShape;

typedef struct {
    CBMDocLinkMdShape shape;
    const char *path;    /* NUL-terminated, in the caller's buffer; for QUALIFIED the
                          * identifiers joined by '.' */
    const char *member;  /* the text after a `path::`, NUL-terminated in the same
                          * buffer: the member the path's file declares; NULL: none */
    uint32_t first_line; /* a line range written with the path (`#L3-L9`, `:3-9`, `:3`); */
    uint32_t last_line;  /* 0 and 0 when there is none */
    bool instance;       /* QUALIFIED: written through an instance (`self.x`, `this.x`):
                          * the qualifier is no type, so it never names a field */
    bool colon;          /* QUALIFIED: written `module:attr` (Python's entry-point form) */
} CBMDocLinkMdPath;

/* Classify the text of a code span. Returns false, with shape NONE, when it
 * names no path and no qualified name, or when `buf` (cap bytes) cannot hold
 * it: a name is never cut. Pure: the same text always gives the same answer. */
bool cbm_doclink_md_classify_span(const char *text, size_t len, char *buf, size_t cap,
                                  CBMDocLinkMdPath *out);

/* Parse a `#L3`, `#L3-L9` or `#L3C2-L9C5` fragment (without the `#`): true
 * and the lines, or false. */
bool cbm_doclink_md_line_fragment(const char *frag, uint32_t *first, uint32_t *last);

/* ── ADRs (doc_adr.c) ──────────────────────────────────────────────── */

/* When the Markdown file being extracted is an architecture decision record:
 * push its ADR definition (name its canonical id, qualified name
 * "<module>.__adr__", facts as extra properties) and a `supersedes` token for
 * each of its own supersedes / replaces statements. Called by the Markdown
 * scan, after the Section definitions exist. */
void cbm_adr_extract(CBMExtractCtx *ctx);

/* The qualified-name tail of an ADR node: "<module QN>" + this. */
#define CBM_ADR_QN_NAME "__adr__"

/* ── reStructuredText (doclink_rst.c) and Python's scope (doclink_py.c) ── */

/* The scan_file hook of a reStructuredText document: a Section per title
 * (the text up to the next title as its docstring) and its references to
 * code -- Sphinx roles and object directives, autodoc, include /
 * literalinclude / kernel-doc paths, inline literals naming a path or a
 * qualified name -- each with the section it is written in as its source. */
void cbm_doclink_rst_scan_file(CBMExtractCtx *ctx);

/* The scope blob of a Python file, which the reST resolver reads: a package
 * `__init__.py`'s top-level explicit `from x import y [as z]` (what the
 * package re-exports), and a Sphinx `conf.py`'s primary domain, extlink roles
 * to repository paths and intersphinx names (read as text, never run). NULL
 * for every other file. */
const char *cbm_doclink_py_scan_scope(CBMExtractCtx *ctx);

/* The Python scope blob starts with this tag line. */
#define CBM_DOCLINK_PY_SCOPE_TAG "py1"

/* ── AsciiDoc (doc_adoc.c) ─────────────────────────────────────────── */

/* AsciiDoc documents: a Section per heading and the references to code
 * (include::, Antora API attributes, monospace spans) as doc-link tokens.
 * Runs instead of a grammar for CBM_LANG_ASCIIDOC. */
void cbm_adoc_extract_document(CBMExtractCtx *ctx);

/* The scope blob of a YAML file, which the AsciiDoc resolver reads: an
 * antora.yml's component name, asciidoc attributes and collector scans, an
 * antora-playbook*.yml's attributes (read as text). NULL for every other
 * file. */
const char *cbm_doclink_antora_scan_scope(CBMExtractCtx *ctx);

/* The Antora scope blob starts with this tag line. */
#define CBM_DOCLINK_ADOC_SCOPE_TAG "ad1"

/* PDF documents (doc_pdf.c): a Section per page (name "page N", the page's
 * text as docstring, property page) and the pages' structural code mentions
 * as doc-link tokens. Runs instead of a grammar for CBM_LANG_PDF. */
void cbm_pdf_extract_document(CBMExtractCtx *ctx);
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Test seam: scan one page's text as cbm_pdf_extract_document does (tokens
 * into ctx->result->doc_links), without a PDF around it. */
void cbm_pdf_test_scan_page(CBMExtractCtx *ctx, const char *text, size_t len, uint32_t page,
                            const char *page_qn);
/* candidates the scanner's joins have compared so far (a cost counter) */
size_t cbm_pdf_test_join_steps(void);
#endif

#endif /* CBM_DOCLINK_H */
