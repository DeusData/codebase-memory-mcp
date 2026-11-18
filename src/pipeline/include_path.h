#ifndef CBM_INCLUDE_PATH_H
#define CBM_INCLUDE_PATH_H

/*
 * include_path.h — the path rule: which repository file an import statement
 * names, for the languages whose imports name files by path (a row below).
 *
 * The rule reads the import's text as a path and finds the one repository file
 * it names: the bases a row allows (the importing file's directory, the
 * repository root), "exactly one" where two bases could both name a file, and
 * a whole-path tail (the one listed file whose path is the import's path or
 * ends with "/" + it). Existence is read from the run's File nodes and from a
 * listing of every file under the repository named like what a row's import
 * can name (names only, nothing read; `.git` and symbolic links not entered),
 * walked once per run on the first question that needs it. Uniqueness is a
 * statement about the repository, not about what the indexer keeps: a file
 * in a skipped directory counts. A listing that cannot complete answers none.
 *
 * Lifted from the parked include-scope branch (round 5, include_scope.c
 * section 2): its universe, bases, exactly-one, tail index and import node;
 * the shell and Make evaluated paths and run directories stay there.
 *
 * Threads: one universe per run. Every question is asked on one thread (the
 * import edges and the sequential import map); nothing is read concurrently.
 */

#include "cbm.h"
#include "foundation/sha256.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline_internal.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* One language whose imports name files by path. */
typedef struct {
    CBMLanguage lang;
    const char *const *suffixes; /* file names the listing keeps: ending in one of these */
    const char *const *prefixes; /* ... or starting with one of these */
    /* an import whose path does not end in one of `suffixes` is not a path
     * the rule reads (Protobuf: a row that names no `.proto` file) */
    bool path_needs_suffix;
    bool base_file_dir;   /* relative to the importing file's directory */
    bool base_root;       /* relative to the repository root */
    bool exactly_one;     /* two bases naming two files: none */
    bool whole_path_tail; /* first, the one listed file that is the path or ends with it */
} cbm_include_path_row_t;

/* The row of a language, or NULL. */
const cbm_include_path_row_t *cbm_include_path_row(CBMLanguage lang);

/* True for a file name the listing keeps (a name some row's import can name). */
bool cbm_include_path_listed_name(const char *name);

typedef struct cbm_include_universe cbm_include_universe_t;

/* A universe for the repository at repo_path; nothing is walked yet. NULL on
 * allocation failure. */
cbm_include_universe_t *cbm_include_universe_new(const char *repo_path);
void cbm_include_universe_free(cbm_include_universe_t *u);

/* Directory entries the listing read, and tail-index keys made and probed:
 * the counted work of growth tests. */
uint64_t cbm_include_universe_visited(const cbm_include_universe_t *u);
uint64_t cbm_include_universe_tail_steps(const cbm_include_universe_t *u);

typedef enum {
    CBM_INCLUDE_PATH_NOT_A_PATH = 0, /* no row, or not a path the row reads: main decides */
    CBM_INCLUDE_PATH_FILE,           /* names one repository file (path in the buffer) */
    CBM_INCLUDE_PATH_NONE,           /* a path that names no file the rule can prove */
} cbm_include_path_verdict_t;

/* The file the import `imp` of the file `rel` names, written into buf.
 * `graph`/`project`: the run's File nodes, which count as existing. */
cbm_include_path_verdict_t cbm_include_path_resolve(cbm_include_universe_t *u,
                                                    const cbm_gbuf_t *graph, const char *project,
                                                    const char *rel, CBMLanguage lang,
                                                    const CBMImport *imp, char *buf, size_t buf_sz);

/* The IMPORTS target of an import, for resolve_import_strategies: true when
 * the path rule decides it, with *out the target node (the file's Module node
 * when it has one, else its File node). False: main's strategies decide. */
bool cbm_include_path_decides(const cbm_pipeline_ctx_t *ctx, const char *rel, CBMLanguage lang,
                              const CBMImport *imp, const cbm_gbuf_node_t **out);

/* The semantic-manifest digest of the listing: every listed path, sorted, or
 * a fixed marker when the listing cannot complete. Walks the repository.
 * Returns 0, or -1 on allocation failure. */
int cbm_include_path_listing_digest(const char *repo_path, char out[CBM_SHA256_HEX_LEN + 1]);

#endif /* CBM_INCLUDE_PATH_H */
