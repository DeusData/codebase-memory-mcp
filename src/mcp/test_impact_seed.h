/*
 * test_impact_seed.h — what a change seeds in the graph (smart-ci-design 5.1,
 * 5.8), for detect_changes scope:"tests".
 *
 * A port of the measured reference (scratchpad tia3/replay4.py, seeding and
 * header symbol propagation), rule for rule:
 *   - a hunk with lines at head seeds the definitions it overlaps; a hunk of
 *     comment lines only seeds nothing; a pure deletion seeds the definition
 *     around the deletion point; a hunk that overlaps no definition seeds its
 *     whole file;
 *   - the names a `-` line declared and head no longer defines are looked up
 *     by text: every definition whose lines mention one is a seed
 *     (NAME_REFERENCE), and a mention outside any definition of a source file
 *     seeds that file whole; a deleted file contributes every name it had;
 *   - a header change names what it changes (declared names, overlapped
 *     definitions), closes those names over the headers' macros (pastes
 *     included), and seeds every definition that mentions one in a file
 *     including the header; a line it cannot classify, or any preprocessor
 *     conditional, makes the header "file level": every test file including it
 *     runs its suites whole;
 *   - a changed test case or suite body is reported as such, never seeded.
 * Every approximation selects more tests, never fewer.
 */
#ifndef CBM_TEST_IMPACT_SEED_H
#define CBM_TEST_IMPACT_SEED_H

#include "mcp/test_impact.h"
#include "mcp/test_impact_source.h"
#include "store/store.h"

#include <stdbool.h>
#include <stdint.h>

/* The graph of the snapshot, as the seeding reads it. */
typedef struct cbm_ti_graph cbm_ti_graph_t;

typedef struct {
    int64_t id;
    const char *label;
    const char *name;
    const char *qualified_name;
    int start_line;
    int end_line;
    /* A further span of a definition already listed (another #if branch or
     * platform-file variant, graph_buffer.c "Definition variants"): it maps
     * lines to the node, but enumerating definitions counts the node once. */
    bool variant_span;
} cbm_ti_node_t;

/* Borrows the store until free. Nodes of a file are read once, on first use. */
cbm_ti_graph_t *cbm_ti_graph_new(cbm_store_t *store, const char *project);
void cbm_ti_graph_free(cbm_ti_graph_t *g);
/* The seedable definitions of a file (every label but File, Folder, Project,
 * Module, Package, Section, Route, Resource), ordered by (start, end, id).
 * NULL with *ok=false when the store failed: a failed read is never "none". */
const cbm_ti_node_t *cbm_ti_graph_file(cbm_ti_graph_t *g, const char *file, int *count, bool *ok);
/* The innermost definition containing the line: smallest span, then the
 * earliest start, then the smallest id. NULL when none (or *ok=false). */
const cbm_ti_node_t *cbm_ti_graph_enclosing(cbm_ti_graph_t *g, const char *file, int line,
                                            bool *ok);
/* Whether any node of the snapshot has this name (deleted-name check). */
bool cbm_ti_graph_has_name(cbm_ti_graph_t *g, const char *name, bool *ok);

/* One changed path the seeding reads: C-family, with its hunks unless the
 * change has no usable text (then whole_file). */
typedef struct {
    const char *path;
    bool deleted;    /* gone at head: only its base names matter */
    bool whole_file; /* no hunks to read: seed the whole file */
    const cbm_diff_hunk_t *hunks;
    int hunk_count;
    /* For a deleted file: its text at the merge base (names it declared). */
    const char *base_text;
    size_t base_len;
} cbm_ti_change_t;

typedef struct {
    const cbm_ti_change_t *changes;
    int change_count;
    const cbm_test_model_t *model; /* cases and suites at head */
    cbm_ti_graph_t *graph;
    cbm_ti_source_t *source; /* finished; occurrences built on demand */
} cbm_ti_seed_input_t;

typedef struct cbm_ti_seeds cbm_ti_seeds_t;

typedef enum {
    CBM_TI_SEED_OK = 0,
    CBM_TI_SEED_INVALID,
    CBM_TI_SEED_STORE, /* the graph could not be read */
    CBM_TI_SEED_OOM
} cbm_ti_seed_status_t;

cbm_ti_seed_status_t cbm_ti_seed(const cbm_ti_seed_input_t *in, cbm_ti_seeds_t **out);
void cbm_ti_seeds_free(cbm_ti_seeds_t *seeds);

/* Seed node ids, ascending, unique. */
const int64_t *cbm_ti_seeds_nodes(const cbm_ti_seeds_t *s, int *count);
/* Model cases whose definition a hunk touched (indices into the model's
 * cases), and suites whose body a hunk touched (indices into its suites). */
const int *cbm_ti_seeds_changed_cases(const cbm_ti_seeds_t *s, int *count);
const int *cbm_ti_seeds_changed_suites(const cbm_ti_seeds_t *s, int *count);
/* Headers changed at file level (paths). */
const char *const *cbm_ti_seeds_file_level_headers(const cbm_ti_seeds_t *s, int *count);
/* Names head no longer defines that were looked up by text. */
const char *const *cbm_ti_seeds_deleted_names(const cbm_ti_seeds_t *s, int *count);

#endif /* CBM_TEST_IMPACT_SEED_H */
