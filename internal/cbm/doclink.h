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
 *       loose_docs      after the definitions and the file's own doc: the
 *                       file's other doc comments, which the graph keeps no
 *                       node for. What it pushes with the file stand-in
 *                       belongs to the file (C: a prototype's doc); it may push
 *                       with a definition of the file instead (PHP: a
 *                       property's doc belongs to its class). Optional.
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
#define CBM_DOCLINK_FAMILY_LIST(X)                                                               \
    /* csharp */                                                                                 \
    X(CBM_DOCLINK_CS_SEE, CBM_LANG_CSHARP, "see", false, true, "MENTIONS")                       \
    X(CBM_DOCLINK_CS_SEEALSO, CBM_LANG_CSHARP, "seealso", false, true, "MENTIONS")               \
    X(CBM_DOCLINK_CS_EXCEPTION, CBM_LANG_CSHARP, "exception", false, true, "MENTIONS")           \
    X(CBM_DOCLINK_CS_INHERITDOC, CBM_LANG_CSHARP, "inheritdoc", false, true, "MENTIONS")         \
    /* go (doclink_go.c): [Name] doc links; held-out audit passed */                             \
    X(CBM_DOCLINK_GO_DOC_LINK, CBM_LANG_GO, "doc_link", false, true, "MENTIONS")                 \
    /* java and kotlin (doclink_jvm.c): Javadoc and KDoc; held-out audit passed */               \
    X(CBM_DOCLINK_JAVA_LINK, CBM_LANG_JAVA, "link", false, true, "MENTIONS")                     \
    X(CBM_DOCLINK_JAVA_LINKPLAIN, CBM_LANG_JAVA, "linkplain", false, true, "MENTIONS")           \
    X(CBM_DOCLINK_JAVA_SEE, CBM_LANG_JAVA, "see", false, true, "MENTIONS")                       \
    X(CBM_DOCLINK_JAVA_THROWS, CBM_LANG_JAVA, "throws", false, true, "MENTIONS")                 \
    X(CBM_DOCLINK_JAVA_VALUE, CBM_LANG_JAVA, "value", false, true, "MENTIONS")                   \
    X(CBM_DOCLINK_KT_KDOC_LINK, CBM_LANG_KOTLIN, "kdoc_link", false, true, "MENTIONS")           \
    X(CBM_DOCLINK_KT_SEE, CBM_LANG_KOTLIN, "see", false, true, "MENTIONS")                       \
    X(CBM_DOCLINK_KT_THROWS, CBM_LANG_KOTLIN, "throws", false, true, "MENTIONS")                 \
    X(CBM_DOCLINK_KT_SAMPLE, CBM_LANG_KOTLIN, "sample", false, true, "MENTIONS")                 \
    /* rust (doclink_rs.c): intra-doc links; held-out audit passed */                            \
    X(CBM_DOCLINK_RS_INTRA_DOC, CBM_LANG_RUST, "intra_doc", false, true, "MENTIONS")             \
    X(CBM_DOCLINK_RS_LINK_DEF, CBM_LANG_RUST, "link_def", false, true, "MENTIONS")               \
    /* perl (doclink_perl.c): POD L<> codes; held-out audit passed */                            \
    X(CBM_DOCLINK_PERL_POD_L, CBM_LANG_PERL, "pod_L", false, true, "MENTIONS")                   \
    /* typescript and javascript (doclink_ts.c): JSDoc/TSDoc; {@link} passed its held-out */     \
    /* audit; {@linkcode} (10) and @see (18) had too few held-out links to pass: held */         \
    X(CBM_DOCLINK_TS_LINK, CBM_LANG_TYPESCRIPT, "link", false, true, "MENTIONS")                 \
    X(CBM_DOCLINK_TS_LINKCODE, CBM_LANG_TYPESCRIPT, "linkcode", false, false, "MENTIONS")        \
    X(CBM_DOCLINK_TS_SEE, CBM_LANG_TYPESCRIPT, "see", false, false, "MENTIONS")                  \
    /* php (doclink_php.c): PHPDoc @see and {@see}; held-out audit passed */                     \
    X(CBM_DOCLINK_PHP_SEE, CBM_LANG_PHP, "see", false, true, "MENTIONS")                         \
    /* c (doclink_c.c): Doxygen @ref and @see in .c and .h files; held-out audit passed */       \
    X(CBM_DOCLINK_C_REF, CBM_LANG_C, "doxygen_ref", false, true, "MENTIONS")                     \
    X(CBM_DOCLINK_C_SEE, CBM_LANG_C, "see", false, true, "MENTIONS")                             \
    /* markdown (doclink_md.c): what a document's text names explicitly */                       \
    X(CBM_DOCLINK_MD_LINK, CBM_LANG_MARKDOWN, "link", false, true, "MENTIONS")                   \
    X(CBM_DOCLINK_MD_PATH, CBM_LANG_MARKDOWN, "path", false, true, "MENTIONS")                   \
    X(CBM_DOCLINK_MD_CODE_PATH, CBM_LANG_MARKDOWN, "code_path", false, true, "MENTIONS")         \
    /* a code span naming a file or directory alone (`config.yaml`, `doc/`) */                   \
    X(CBM_DOCLINK_MD_BARE_PATH, CBM_LANG_MARKDOWN, "bare_path", false, false, "MENTIONS")        \
    X(CBM_DOCLINK_MD_CODE_NAME, CBM_LANG_MARKDOWN, "code_name", false, true, "MENTIONS")         \
    /* an ADR's own "supersedes" / "replaces" statement, opening its line or field */            \
    /* and naming a record directly (doc_adr.c): ADR -> ADR */                                   \
    X(CBM_DOCLINK_MD_SUPERSEDES, CBM_LANG_MARKDOWN, "supersedes", false, true, "SUPERSEDES")     \
    /* the same words in running prose ("this record supersedes ADR-3") */                       \
    X(CBM_DOCLINK_MD_SUPERSEDES_PROSE, CBM_LANG_MARKDOWN, "supersedes_prose", false, false,      \
      "SUPERSEDES")                                                                              \
    /* pdf (doc_pdf.c): structural mentions in a page's text; pdf_name (bare      */             \
    /* names) only decides which line fragments a cross-line join replaces        */             \
    X(CBM_DOCLINK_PDF_PATH, CBM_LANG_PDF, "pdf_path", false, true, "MENTIONS")                   \
    X(CBM_DOCLINK_PDF_FILE, CBM_LANG_PDF, "pdf_file", false, true, "MENTIONS")                   \
    X(CBM_DOCLINK_PDF_QN, CBM_LANG_PDF, "pdf_qn", false, true, "MENTIONS")                       \
    X(CBM_DOCLINK_PDF_NAME, CBM_LANG_PDF, "pdf_name", false, false, "MENTIONS")                  \
    /* restructuredtext (doclink_rst.c): Sphinx domain markup, directives, inline literals */    \
    X(CBM_DOCLINK_RST_ROLE, CBM_LANG_RST, "role", false, true, "MENTIONS")                       \
    X(CBM_DOCLINK_RST_OBJECT, CBM_LANG_RST, "object", false, true, "MENTIONS")                   \
    X(CBM_DOCLINK_RST_AUTODOC, CBM_LANG_RST, "autodoc", false, true, "MENTIONS")                 \
    X(CBM_DOCLINK_RST_INCLUDE, CBM_LANG_RST, "include", false, false, "MENTIONS")                \
    X(CBM_DOCLINK_RST_LITERALINCLUDE, CBM_LANG_RST, "literalinclude", false, true, "MENTIONS")   \
    X(CBM_DOCLINK_RST_KERNEL_DOC, CBM_LANG_RST, "kernel_doc", false, false, "MENTIONS")          \
    X(CBM_DOCLINK_RST_CODE_PATH, CBM_LANG_RST, "code_path", false, true, "MENTIONS")             \
    X(CBM_DOCLINK_RST_CODE_NAME, CBM_LANG_RST, "code_name", false, true, "MENTIONS")             \
    /* asciidoc (doc_adoc.c): includes, Antora API attributes, monospace spans */                \
    X(CBM_DOCLINK_ADOC_INCLUDE, CBM_LANG_ASCIIDOC, "include", false, true, "MENTIONS")           \
    X(CBM_DOCLINK_ADOC_ATTRIBUTE, CBM_LANG_ASCIIDOC, "attribute", false, false, "MENTIONS")      \
    X(CBM_DOCLINK_ADOC_CODE_PATH, CBM_LANG_ASCIIDOC, "code_path", false, false, "MENTIONS")      \
    X(CBM_DOCLINK_ADOC_CODE_NAME, CBM_LANG_ASCIIDOC, "code_name", false, false, "MENTIONS")      \
    /* diagrams (diagram.c): an element's explicit link (or a PlantUML !include),  */            \
    /* read as a Markdown link; held until their held-out audit passes             */            \
    X(CBM_DOCLINK_DG_MERMAID_CLICK, CBM_LANG_MERMAID, "mermaid_click", false, false, "MENTIONS") \
    X(CBM_DOCLINK_DG_PLANTUML_URL, CBM_LANG_DIAGRAM, "plantuml_url", false, false, "MENTIONS")   \
    X(CBM_DOCLINK_DG_PLANTUML_INCLUDE, CBM_LANG_DIAGRAM, "plantuml_include", false, false,       \
      "MENTIONS")                                                                                \
    X(CBM_DOCLINK_DG_DRAWIO_LINK, CBM_LANG_DIAGRAM, "drawio_link", false, false, "MENTIONS")     \
    X(CBM_DOCLINK_DG_EXCALIDRAW_LINK, CBM_LANG_DIAGRAM, "excalidraw_link", false, false,         \
      "MENTIONS")                                                                                \
    X(CBM_DOCLINK_DG_DOT_URL, CBM_LANG_DIAGRAM, "dot_url", false, false, "MENTIONS")             \
    X(CBM_DOCLINK_DG_D2_LINK, CBM_LANG_DIAGRAM, "d2_link", false, false, "MENTIONS")             \
    /* openapi (openapi.c): an operation's operationId, read as a qualified code      */         \
    /* name; held until its held-out audit passes                                   */           \
    X(CBM_DOCLINK_OPENAPI_OPERATION_ID, CBM_LANG_OPENAPI, "operation_id", false, false,          \
      "MENTIONS")                                                                                \
    /* any language (CBM_LANG_COUNT): a URL is never an edge */                                  \
    X(CBM_DOCLINK_HREF, CBM_LANG_COUNT, "href", true, false, "MENTIONS")

typedef enum {
    CBM_DOCLINK_NONE = 0,
#define CBM_DOCLINK_FAMILY_VALUE(id, lang, name, external, ships, edge) id,
    CBM_DOCLINK_FAMILY_LIST(CBM_DOCLINK_FAMILY_VALUE)
#undef CBM_DOCLINK_FAMILY_VALUE
        CBM_DOCLINK_SYNTAX_COUNT
} CBMDocLinkSyntax;

/* The Markdown family a reference of another document reads as, wherever it
 * is written (CBM_DOCLINK_NONE for none): a diagram's explicit link
 * (diagram.c) is a link, an OpenAPI operationId (openapi.c) a code name. */
static inline int cbm_doclink_md_twin(int syntax) {
    if (syntax >= CBM_DOCLINK_DG_MERMAID_CLICK && syntax <= CBM_DOCLINK_DG_D2_LINK) {
        return CBM_DOCLINK_MD_LINK;
    }
    return syntax == CBM_DOCLINK_OPENAPI_OPERATION_ID ? CBM_DOCLINK_MD_CODE_NAME : CBM_DOCLINK_NONE;
}

/* What an edge of these families says it came through: the kind of source,
 * whichever document holds it (NULL: the resolver's own name). */
static inline const char *cbm_doclink_syntax_via(int syntax) {
    if (syntax >= CBM_DOCLINK_DG_MERMAID_CLICK && syntax <= CBM_DOCLINK_DG_D2_LINK) {
        return "diagram";
    }
    return syntax == CBM_DOCLINK_OPENAPI_OPERATION_ID ? "openapi" : NULL;
}

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
/* ── Go (doclink_go.c) ─────────────────────────────────────────────── */

void cbm_doclink_go_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                              uint32_t doc_line);
const char *cbm_doclink_go_scan_scope(CBMExtractCtx *ctx);
char *cbm_doclink_go_portable_scope(const char *scope);

/* A go.mod file has no doc links; its scope is the module path, in the Go
 * scope format, so that import paths map to the repository's directories. */
void cbm_doclink_gomod_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                                 uint32_t doc_line);
const char *cbm_doclink_gomod_scan_scope(CBMExtractCtx *ctx);

/* The parts of a Go doc link as go/doc/comment splits it (docLink,
 * splitDocName): `pkg`, `recv` and `name` are slices of the reference text,
 * each possibly empty. [Name] {name}; [Recv.Name] {recv, name}; [pkg] {pkg};
 * [pkg.Name] {pkg, name}; [pkg.Recv.Name] {pkg, recv, name}. */
typedef struct {
    const char *pkg; /* a package name, or an import path when it has a '/' */
    size_t pkg_len;
    const char *recv;
    size_t recv_len;
    const char *name;
    size_t name_len;
} CBMDocLinkGoRef;

/* The longest reference text, and the longest name, the Go leg handles. A
 * longer bracket text made of doc-link characters is harvested clipped to at
 * most one byte more than this, and the resolver reports it as unparseable:
 * a reference is never resolved by its beginning. */
enum { CBM_DOCLINK_GO_REF_MAX = 512 };

/* Split the text between the brackets (a leading `*` is skipped). False for
 * text that go/doc/comment could not take for a doc link whatever the package
 * declares or imports (nothing is harvested from it), and for text longer than
 * CBM_DOCLINK_GO_REF_MAX. */
bool cbm_doclink_go_parse_ref(const char *raw, size_t len, CBMDocLinkGoRef *out);

/* The Go scope blob (Go files and go.mod files) starts with this tag line. */
#define CBM_DOCLINK_GO_SCOPE_TAG "go1"
/* ── Java and Kotlin (doclink_jvm.c) ───────────────────────────────── */

void cbm_doclink_java_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                                uint32_t doc_line);
void cbm_doclink_kotlin_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                                  uint32_t doc_line);
const char *cbm_doclink_java_scan_scope(CBMExtractCtx *ctx);
const char *cbm_doclink_kotlin_scan_scope(CBMExtractCtx *ctx);
char *cbm_doclink_jvm_portable_scope(const char *scope);

/* Normalize one Java parameter type as written in a declaration or in a
 * Javadoc `#member(...)` list: annotations, `final`, type arguments, package
 * and outer-type qualifiers and a trailing parameter name are dropped,
 * `...` becomes `[]`, array brackets stay. "?" when nothing is left. Writes a
 * NUL-terminated string into `out` (truncated at cap) and returns its
 * length. */
size_t cbm_doclink_java_norm_type(const char *in, size_t len, char *out, size_t cap);

/* Java and Kotlin files share one scope tag: one resolver indexes both. */
#define CBM_DOCLINK_JVM_SCOPE_TAG "jvm1"

/* The longest reference a Java or Kotlin token may carry: a longer one is cut
 * just past this length at extraction and reported as unparseable. */
#define CBM_DOCLINK_JVM_REF_MAX 1000
/* ── Rust (doclink_rs.c) ───────────────────────────────────────────── */

void cbm_doclink_rs_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                              uint32_t doc_line);
const char *cbm_doclink_rs_scan_scope(CBMExtractCtx *ctx);
char *cbm_doclink_rs_portable_scope(const char *scope);

/* A Cargo manifest is a TOML file the indexer extracts anyway: its scope blob
 * (package, targets, dependencies) is what the Rust resolver knows of crates,
 * so nothing is read from disk when references are resolved. Any other TOML
 * file has no scope, and no TOML file has references. */
void cbm_doclink_rs_manifest_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def,
                                       const char *doc, uint32_t doc_line);
const char *cbm_doclink_rs_manifest_scan_scope(CBMExtractCtx *ctx);

/* The Rust scope blob (a source file's and a manifest's) starts with this tag
 * line. */
#define CBM_DOCLINK_RS_SCOPE_TAG "rs1"

enum {
    CBM_DOCLINK_RS_MAX_SEGS = 16, /* path segments of one reference */
    CBM_DOCLINK_RS_SEG_CAP = 96,  /* bytes of one segment, NUL included */
    CBM_DOCLINK_RS_RAW_CAP = 512, /* bytes of a reference as stored, NUL included */
};

/* What a rustdoc disambiguator (`struct@`, `fn@` ...) or suffix (`()`, `!`)
 * asks of the entity a path names. */
typedef enum {
    CBM_DOCLINK_RS_DIS_NONE = 0,
    CBM_DOCLINK_RS_DIS_STRUCT,
    CBM_DOCLINK_RS_DIS_ENUM,
    CBM_DOCLINK_RS_DIS_TRAIT,
    CBM_DOCLINK_RS_DIS_UNION,
    CBM_DOCLINK_RS_DIS_TYALIAS,
    CBM_DOCLINK_RS_DIS_MOD,
    CBM_DOCLINK_RS_DIS_CONST,
    CBM_DOCLINK_RS_DIS_STATIC,
    CBM_DOCLINK_RS_DIS_FN,         /* `fn@`, `function@`, `method@`, and the `()` suffix */
    CBM_DOCLINK_RS_DIS_DERIVE,     /* a derive macro */
    CBM_DOCLINK_RS_DIS_FIELD,      /* a struct or union field */
    CBM_DOCLINK_RS_DIS_VARIANT,    /* an enum variant */
    CBM_DOCLINK_RS_DIS_MACRO_BANG, /* the `!` suffix: a function-like macro */
    CBM_DOCLINK_RS_DIS_TYPE_NS,    /* `type@`: anything of the type namespace */
    CBM_DOCLINK_RS_DIS_VALUE_NS,   /* `value@` */
    CBM_DOCLINK_RS_DIS_MACRO_NS,   /* `macro@` */
    CBM_DOCLINK_RS_DIS_PRIM,       /* `prim@`, `primitive@` */
} CBMDocLinkRsDis;

/* One intra-doc link destination, taken apart. */
typedef struct {
    char segs[CBM_DOCLINK_RS_MAX_SEGS][CBM_DOCLINK_RS_SEG_CAP];
    int nsegs;
    bool global; /* a leading `::`: the first segment names a crate */
    int dis;     /* CBMDocLinkRsDis */
} CBMDocLinkRsPath;

typedef enum {
    CBM_DOCLINK_RS_PATH_OK = 0,
    CBM_DOCLINK_RS_PATH_NOT_LINK, /* rustdoc does not treat this text as an intra-doc link */
    CBM_DOCLINK_RS_PATH_MALFORMED /* a link rustdoc reports as broken syntax */
} CBMDocLinkRsPathStatus;

/* Take a link destination apart the way rustdoc's `preprocess_link` does
 * (backticks and `#fragment` already removed): disambiguator or suffix,
 * generics stripped, `::` segments. `lenient` is rustdoc's rule for text that
 * may be an ordinary link or plain brackets (an inline or defined
 * destination, or an undefined shortcut without backticks): there a character
 * no path can hold means NOT_LINK instead of MALFORMED. `len` bytes of `raw`
 * are read. */
CBMDocLinkRsPathStatus cbm_doclink_rs_parse_path(const char *raw, size_t len, bool lenient,
                                                 CBMDocLinkRsPath *out);
/* ── TypeScript and JavaScript (doclink_ts.c) ──────────────────────── */

void cbm_doclink_ts_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                              uint32_t doc_line);
const char *cbm_doclink_ts_scan_scope(CBMExtractCtx *ctx);
char *cbm_doclink_ts_portable_scope(const char *scope);

/* TS, TSX and JS files share one scope tag: one resolver indexes them all. */
#define CBM_DOCLINK_TS_SCOPE_TAG "ts1"

/* The longest reference a TS/JS token keeps; a longer one is cut just past
 * this length and the resolver reports it as unparseable. */
enum { CBM_DOCLINK_TS_REF_MAX = 512 };

/* Asked by a loose_docs hook for a doc comment on lines [first, last]: true
 * when a definition's doc or the file's own doc there was already taken. */
typedef bool (*cbm_doclink_taken_fn)(const CBMExtractCtx *ctx, uint32_t first, uint32_t last);

/* ── PHP (doclink_php.c) ───────────────────────────────────────────── */

void cbm_doclink_php_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                               uint32_t doc_line);
const char *cbm_doclink_php_scan_scope(CBMExtractCtx *ctx);
char *cbm_doclink_php_portable_scope(const char *scope);
/* Doc blocks on properties, class constants and enum cases (they have no
 * node): their references belong to the type around them. */
void cbm_doclink_php_loose_docs(CBMExtractCtx *ctx, const CBMDefinition *file_def,
                                cbm_doclink_taken_fn taken);

#define CBM_DOCLINK_PHP_SCOPE_TAG "php1"

/* The longest reference a PHP token keeps; a longer one is cut just past this
 * length and the resolver reports it as unparseable. */
enum { CBM_DOCLINK_PHP_REF_MAX = 512 };

/* One `use` import of a PHP namespace block: kind 'c' (a class or namespace),
 * 'f' (a function), 'k' (a constant); the name without a leading backslash. */
typedef struct {
    const char *alias;
    size_t alias_len;
    const char *name;
    size_t name_len;
    char kind;
} cbm_doclink_php_use_t;

/* The fully qualified name (no leading backslash) of a type name `ref` (n
 * bytes) written in namespace `ns` (ns_len 0: the global one) under these
 * imports, as PHP resolves it: a leading backslash is taken as written,
 * `namespace\` stands for the current namespace, else the first segment is
 * replaced when it is the alias of a class import (compared without case),
 * else the namespace is put in front. Writes NUL-terminated into out (cap
 * bytes); returns the length, 0 when it does not fit. */
size_t cbm_doclink_php_qualify(char *out, size_t cap, const char *ns, size_t ns_len,
                               const cbm_doclink_php_use_t *uses, int nuses, const char *ref,
                               size_t n);

/* ── C (doclink_c.c) ───────────────────────────────────────────────── */

void cbm_doclink_c_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                             uint32_t doc_line);
void cbm_doclink_c_loose_docs(CBMExtractCtx *ctx, const CBMDefinition *file_def,
                              cbm_doclink_taken_fn taken);
const char *cbm_doclink_c_scan_scope(CBMExtractCtx *ctx);

/* C files and the headers C++ parses share one scope tag: one resolver
 * indexes them. The blob holds no line: it is its own persisted form. */
#define CBM_DOCLINK_C_SCOPE_TAG "c1"

/* The longest reference a C token keeps; a longer one is cut just past this
 * length and the resolver reports it as unparseable. */
enum { CBM_DOCLINK_C_REF_MAX = 256 };

/* ── Perl POD (doclink_perl.c) ─────────────────────────────────────── */

void cbm_doclink_perl_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                                uint32_t doc_line);
const char *cbm_doclink_perl_scan_scope(CBMExtractCtx *ctx);

/* The Perl scope blob starts with this tag line. */
#define CBM_DOCLINK_PERL_SCOPE_TAG "perl1"

enum {
    /* Longest L<> content kept as a token's raw text; a longer one is cut and
     * marked malformed (its raw text then starts with `L<`). */
    CBM_DOCLINK_PERL_RAW_MAX = 512,
    /* Buffer for a page name, a section text or a heading key. */
    CBM_DOCLINK_PERL_KEY_MAX = 256,
};

/* A POD text as it renders: formatting codes replaced by their content
 * (X<> and Z<> by nothing, E<> by its character), white-space runs as one
 * space, none at the ends. Headings and the section part of a link are
 * compared in this form. Writes a NUL-terminated string into `out` (cut at
 * cap - 1, never inside a UTF-8 character) and returns its length. */
size_t cbm_doclink_perl_plain_text(const char *in, size_t len, char *out, size_t cap);

typedef enum {
    CBM_PERL_LINK_UNPARSEABLE = 0, /* malformed, nested L<>, or no page name */
    CBM_PERL_LINK_PAGE,            /* L<Module::Name> or L<Module::Name/section> */
    CBM_PERL_LINK_SECTION,         /* L</section>, L<"section">: the same page */
    CBM_PERL_LINK_MAN,             /* L<name(3)>: a man page */
} CBMPerlLinkKind;

/* One pod_L token's raw text, split the way perlpodspec splits it. */
typedef struct {
    CBMPerlLinkKind kind;
    char page[CBM_DOCLINK_PERL_KEY_MAX];    /* "" for a section of the same page */
    char section[CBM_DOCLINK_PERL_KEY_MAX]; /* "" when only a page is named */
} CBMPerlLink;

/* Split a pod_L token's raw text: the first top-level `|` ends the link text,
 * the first top-level `/` after it separates page and section, quotes around
 * a section are dropped. False (kind UNPARSEABLE) for anything else. */
bool cbm_doclink_perl_parse_link(const char *raw, CBMPerlLink *out);

#endif /* CBM_DOCLINK_H */
