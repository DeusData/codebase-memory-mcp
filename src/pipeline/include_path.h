#ifndef CBM_INCLUDE_PATH_H
#define CBM_INCLUDE_PATH_H

/*
 * include_path.h — the path rule: which repository file an import statement
 * names, for the languages whose imports name files by path (a row below).
 *
 * The rule reads the import's text as a path and finds the one indexed file it
 * names: the bases a row allows (the importing file's directory, the
 * repository root), "exactly one" where two bases could both name a file, and
 * a whole-path tail (the one indexed file whose path is the import's path or
 * ends with "/" + it). The universe is the run's indexed file list (the files
 * discovery kept, on every route), never a walk of the disk: a copy of a file
 * in an ignored or skipped directory (node_modules, a .gitignore'd tree) is
 * not seen, so an import only such a copy could satisfy names no file.
 *
 * A pick that depends on what the index cannot know is a guess, its own
 * verdict, never FILE: the file beside a Protobuf importer when several
 * indexed files end with the path (protoc reads the one under its -I root),
 * and a Make include from a makefile below the root (GNU make reads relative
 * paths from the directory it runs in). A guess decides no IMPORTS edge (main
 * decides) and resolves no include of the shared layer.
 *
 * Lifted from the parked include-scope branch (round 5, include_scope.c
 * section 2): its bases, exactly-one, tail index and import node; the shell
 * and Make evaluated paths and run directories stay there.
 *
 * Threads: a universe is built once, from the file list, before any question,
 * and is read-only afterwards: a question writes nothing (sealed at creation).
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
    /* a virtual path (protoc): `.` and `..` segments, `//` and backslashes are
     * refused by the tool, so the rule reads no such path */
    bool virtual_path;
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

/* The universe of the run's indexed files (`files`, the discovered list of
 * the run on every route): every file whose name a row's import can name.
 * Built here, read-only afterwards. NULL on allocation failure. */
cbm_include_universe_t *cbm_include_universe_new(const cbm_file_info_t *files, int count);
void cbm_include_universe_free(cbm_include_universe_t *u);

/* Tail-index keys made while building: the counted work of growth tests (a
 * question makes none). */
uint64_t cbm_include_universe_tail_steps(const cbm_include_universe_t *u);

typedef enum {
    CBM_INCLUDE_PATH_NOT_A_PATH = 0, /* no row, or not a path the row reads: main decides */
    CBM_INCLUDE_PATH_FILE,           /* names one indexed file (path in the buffer) */
    CBM_INCLUDE_PATH_NONE,           /* a path whose file the rule cannot tell (several, unknown) */
    /* proven: no indexed file is that path or ends with it (letter case
     * ignored) */
    CBM_INCLUDE_PATH_NO_FILE,
    /* a file picked by an assumption the index cannot check (path in the
     * buffer): the include root or the directory make runs in. Not FILE: main
     * decides the edge, the shared layer takes the include as unresolved. */
    CBM_INCLUDE_PATH_GUESS,
} cbm_include_path_verdict_t;

/* The file the import `imp` of the file `rel` names, written into buf.
 * `graph`/`project` take no part: a file exists when its exact path is listed
 * (a File node found by name may be another file's: a.b.proto and a/b.proto
 * share one name). */
cbm_include_path_verdict_t cbm_include_path_resolve(const cbm_include_universe_t *u,
                                                    const cbm_gbuf_t *graph, const char *project,
                                                    const char *rel, CBMLanguage lang,
                                                    const CBMImport *imp, char *buf, size_t buf_sz);

/* The IMPORTS target of an import, for resolve_import_strategies: true when
 * the path rule decides it, with *out the target node (the file's Module node
 * when it has one, else its File node) or NULL: no edge (the named file has no
 * node, or no indexed file is named; a symbol is never the target of a
 * statement that names a file). False: main's strategies propose a target (a
 * guess is not a decision), which cbm_include_path_accepts judges. */
bool cbm_include_path_decides(const cbm_pipeline_ctx_t *ctx, const char *rel, CBMLanguage lang,
                              const CBMImport *imp, const cbm_gbuf_node_t **out);

/* When the path rule does not decide (a guess, several files, a name the
 * listing does not keep, an incomplete listing), a target main's strategies
 * propose for an import of a path-row language: true only when its file_path
 * is the named path or ends with it at a '/' boundary, or is one of the rule's
 * own candidates (the path from the importer's directory or the root, `.` and
 * `..` folded). The bytes are compared exactly: a file of another letter case
 * names the file on a case-insensitive disk only, so it stays no edge. False:
 * no edge (the dotted name strategies would reach a/b.mk for `a.b.mk`). True
 * for any other language and for no target. */
bool cbm_include_path_accepts(const cbm_pipeline_ctx_t *ctx, const char *rel, CBMLanguage lang,
                              const CBMImport *imp, const cbm_gbuf_node_t *target);

/* The semantic-manifest key of the universe: every indexed path a row's
 * import can name, sorted, hashed. Reads the list only (no disk). Returns 0,
 * or -1 on allocation failure. */
int cbm_include_path_inventory_digest(const cbm_file_info_t *files, int count,
                                      char out[CBM_SHA256_HEX_LEN + 1]);

#endif /* CBM_INCLUDE_PATH_H */
