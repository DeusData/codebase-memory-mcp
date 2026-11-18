#ifndef CBM_SCOPE_CLOSURE_H
#define CBM_SCOPE_CLOSURE_H

/*
 * scope_closure.h — which definitions named N a file sees, unordered mode:
 * the closure and the lookup of the shared cross-file layer.
 *
 * A file sees its own definitions, those of its package mates where the row
 * says a package is visible, and what each file it includes exports. A file
 * exports its own definitions and what its re-exporting includes export
 * (Protobuf `import public`); a plain include is not passed on. The closure is
 * never listed per file: re-exporting includes are collapsed into strongly
 * connected components, and a lookup of N searches backwards from the
 * components of N's defining files along re-export edges, once per name
 * (memoised per name and component).
 *
 * The lookup answers, in its own terms (possible against proven):
 *   PROVEN         exactly one visible definition, the closure complete, the
 *                  name not bound locally, the definition not conditional;
 *   POSSIBLE       exactly one visible definition otherwise (a source on the way
 *                  is unresolved or outside the repository, opaque, a prelude);
 *   NO_ANSWER      none visible with an incomplete closure, or two or more;
 *   PROVEN_ABSENT  none visible, the closure complete (counted only).
 * `why` says what kept an answer from being proven; drawing is the caller's.
 *
 * Threads: built and sealed on one thread; a lookup writes the memo, so
 * lookups run on one thread (or each worker brings its own scope).
 */

#include "cbm.h"

#include <stdbool.h>
#include <stdint.h>

/* One language's scope row. */
typedef struct {
    CBMLanguage lang;
    bool package_visible;    /* a file sees its package mates' definitions without an include */
    bool prelude_external;   /* names may come from an implicit external prelude */
    bool ambiguity_is_error; /* two visible definitions do not compile: one visible past an
                              * unresolved or external source is still the one */
    bool qualified_refs;     /* a reference's qualifier is read against full names, from the
                              * referring file's package outward (Protobuf) */
} cbm_closure_lang_t;

/* The row of a language, or NULL. */
const cbm_closure_lang_t *cbm_closure_lang(CBMLanguage lang);

typedef enum {
    CBM_CLOSURE_NO_ANSWER = 0,
    CBM_CLOSURE_PROVEN,
    CBM_CLOSURE_POSSIBLE,
    CBM_CLOSURE_PROVEN_ABSENT,
} cbm_closure_verdict_t;

/* Why an answer is not proven (several may hold). */
enum {
    CBM_CLOSURE_WHY_UNRESOLVED = 1u << 0,  /* an include on the way names no repository file */
    CBM_CLOSURE_WHY_OPAQUE = 1u << 1,      /* a file on the way has a part the facts cannot read */
    CBM_CLOSURE_WHY_PRELUDE = 1u << 2,     /* the row's names may come from a prelude */
    CBM_CLOSURE_WHY_LOCAL = 1u << 3,       /* the reference is bound by a parameter or local */
    CBM_CLOSURE_WHY_CONDITIONAL = 1u << 4, /* the one definition may not be in effect */
    CBM_CLOSURE_WHY_AMBIGUOUS = 1u << 5,   /* two or more definitions are visible */
    CBM_CLOSURE_WHY_NO_NODE = 1u << 6,     /* the one definition has no graph node */
};

/* How the answer's definition is visible. */
enum { CBM_CLOSURE_VIA_OWN = 1, CBM_CLOSURE_VIA_INCLUDE = 2, CBM_CLOSURE_VIA_PACKAGE = 3 };

/* The lookup key of one site. */
typedef struct {
    const char *name; /* the last identifier */
    const char *qual; /* the qualifier as written ("common", ".a.b"), or NULL */
    uint32_t pos;     /* the site's byte (ordered rows; unused in unordered mode) */
    uint8_t ns;       /* namespace (unused in unordered mode) */
    uint8_t flags;    /* CBM_CLOSURE_REF_* */
} cbm_closure_ref_t;

enum { CBM_CLOSURE_REF_LOCAL = 1u << 0 /* bound by a parameter or a local */ };

typedef struct {
    cbm_closure_verdict_t verdict;
    const char *qn; /* the definition's qualified name (PROVEN, POSSIBLE), else NULL */
    int file;       /* its file, else -1 */
    int visible;    /* definitions named N visible from the file (2 means two or more) */
    bool complete;  /* every source on the way is a repository file the facts read whole */
    uint32_t why;   /* CBM_CLOSURE_WHY_* */
    uint8_t via;    /* CBM_CLOSURE_VIA_* of the answer */
} cbm_closure_answer_t;

typedef struct cbm_closure cbm_closure_t;

cbm_closure_t *cbm_closure_new(void);
void cbm_closure_free(cbm_closure_t *s);

/* A file of a language with a row; returns its index, -1 on failure.
 * `package` NULL or "" when it has none. */
int cbm_closure_add_file(cbm_closure_t *s, const char *rel, const cbm_closure_lang_t *row,
                         const char *package);
/* A definition of `file`: its name, full name (or NULL), graph qualified name
 * (NULL: the graph has no node for it) and whether it is conditional. */
bool cbm_closure_add_def(cbm_closure_t *s, int file, const char *name, const char *full,
                         const char *qn, bool conditional);
/* An include of `file`: the included file, or -1 when it names no repository
 * file; re-exporting or plain. */
bool cbm_closure_add_include(cbm_closure_t *s, int file, int target, bool reexport);
/* A part of `file` the facts cannot read (an OPAQUE event). */
bool cbm_closure_add_opaque(cbm_closure_t *s, int file);

/* After the last file: components, completeness and the by-name index.
 * Returns 0, or -1 on allocation failure. No file is added after it. */
int cbm_closure_seal(cbm_closure_t *s);

/* What `ref` names from `file`. */
cbm_closure_answer_t cbm_closure_lookup(cbm_closure_t *s, int file, const cbm_closure_ref_t *ref);

/* Counted work so far (components, reach steps, lookup steps): growth tests. */
uint64_t cbm_closure_work(const cbm_closure_t *s);

#endif /* CBM_SCOPE_CLOSURE_H */
