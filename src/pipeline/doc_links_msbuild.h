/*
 * doc_links_msbuild.h — MSBuild global usings of a C# project (R1), read from
 * the scope blobs of the repository's project files. Private to the C# leg
 * (doc_links_cs.c); no file is opened here.
 */
#ifndef CBM_PIPELINE_DOC_LINKS_MSBUILD_H
#define CBM_PIPELINE_DOC_LINKS_MSBUILD_H

#include <stdbool.h>
#include <stdint.h>

/* The project files of one repository. */
typedef struct cbm_msb cbm_msb_t;

/* True when `scope` is the scope blob of an MSBuild project file (written by
 * cbm_doclink_cs_project_scan_scope), not of a C# source file. */
bool cbm_msb_is_project_scope(const char *scope);

cbm_msb_t *cbm_msb_new(void);
void cbm_msb_free(cbm_msb_t *m);

/* Add the project file `rel_path` (repository-relative, '/'-separated) with
 * its blob; both are copied. false when memory ran out. */
bool cbm_msb_add(cbm_msb_t *m, const char *rel_path, const char *scope);

/* True when `rel_path` was added. */
bool cbm_msb_has(const cbm_msb_t *m, const char *rel_path);

/* False when the project file `rel_path` names an SDK that compiles nothing
 * (Microsoft.Build.NoTargets, Microsoft.Build.Traversal: projects that run
 * build steps or build other projects). Such a file is the project of no
 * source file. True for every other project file, and for one that was not
 * added: nothing is known about it. */
bool cbm_msb_compiles(const cbm_msb_t *m, const char *rel_path);

typedef struct {
    char kind;         /* n: a namespace, s: a type (using static), a: an alias */
    const char *alias; /* kind a: the alias; "" otherwise */
    const char *target;
} cbm_msb_using_t;

typedef struct {
    cbm_msb_using_t *usings; /* sorted, without duplicates; NULL when there are none */
    int count;
    /* A <Using> item or the ImplicitUsings switch could not be evaluated: the
     * project may have a global using this list lacks. */
    bool open;
    int unevaluable; /* conditions, values and constructs that were not evaluated */
    int outside;     /* imports of files the index does not hold */
    void *mem;
} cbm_msb_result_t;

/* The global usings the project file `project_rel` gives its C# files: the
 * nearest Directory.Build.props above it, the file itself, the nearest
 * Directory.Build.targets, and what they import, evaluated in that order.
 * false when memory ran out or `project_rel` was not added. The result is
 * released with cbm_msb_result_free. */
bool cbm_msb_eval(const cbm_msb_t *m, const char *project_rel, cbm_msb_result_t *out);
void cbm_msb_result_free(cbm_msb_result_t *r);

/* A single-caller evaluation context; the model must outlive it. The first
 * completed normal/poison effect per imported or nearest file is retained.
 * Exact dependencies validate reuse; mismatches interpret normally without
 * replacing that variant. Ordered persistent state shares unchanged subtrees
 * across roots. Project-local evaluation remains call-local, and source item
 * spans are evaluated with final properties using one first-variant result
 * cache. Results own an independent publication block. Nearest-file metadata
 * includes absent files. Model additions invalidate the complete reuse epoch.
 * Large overlapping effects or changed inputs can still require more work. */
typedef struct cbm_msb_eval_context cbm_msb_eval_context_t;
cbm_msb_eval_context_t *cbm_msb_eval_context_new(const cbm_msb_t *m);
void cbm_msb_eval_context_free(cbm_msb_eval_context_t *context);
bool cbm_msb_eval_context_eval(cbm_msb_eval_context_t *context, const char *project_rel,
                               cbm_msb_result_t *out);

/* Operation labels stay available to the module in production builds;
 * the controls and failure behavior exist only with test seams enabled. */
typedef enum {
    CBM_MSB_ITEM_FAIL_NONE = 0,
    CBM_MSB_ITEM_FAIL_CAPTURE_ALLOC = 1,
    CBM_MSB_ITEM_FAIL_UNIQUE_INSERT = 2,
    CBM_MSB_ITEM_FAIL_APPLY_ALLOC = 3,
    CBM_MSB_ITEM_FAIL_PUBLISH_ALLOC = 4,
} cbm_msb_item_fail_operation_t;

typedef enum {
    CBM_MSB_NODE_FAIL_NONE = 0,
    CBM_MSB_NODE_FAIL_CAPTURE = 1,
    CBM_MSB_NODE_FAIL_OVERLAY = 2,
    CBM_MSB_NODE_FAIL_DEPENDENCY = 3,
    CBM_MSB_NODE_FAIL_APPLY = 4,
} cbm_msb_node_fail_operation_t;

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Records consumed and peak owned string/array storage: arena capacities
 * plus requested heap bytes, including result publication and directory
 * memo payloads, packed state-pool capacities (including free slots), symbol
 * storage and retained component/value/rope objects. Immutable input,
 * hash-table bookkeeping and allocator
 * metadata are excluded. */
void cbm_msb_test_cost_reset(void);
void cbm_msb_test_cost(uint64_t *records, uint64_t *peak_bytes);
/* Deterministic work units since cost_reset: one per record, logical hash
 * operation or property cleanup entry; bytes copied/appended by expression,
 * property, string, array and publication operations; dependency comparison
 * bytes and current-entry validation visits; owner initialization,
 * transfer and clearing bytes; unique-item key construction, comparison and
 * filtering operations; and bytes processed when forming property keys and
 * walking directory paths; persistent-node lookup, overlay, masking,
 * validation, source-span composition, allocation and iterative release.
 * Test-only revision auditing is observation, not evaluator work. Hash operations
 * count API probes, not internal bucket visits. Failed copies add no bytes.
 * This measures selected operations, not time or every CPU instruction. */
uint64_t cbm_msb_test_work(void);
/* Actual directory candidate probes made by msb_nearest since cost_reset. */
uint64_t cbm_msb_test_nearest_steps(void);
void cbm_msb_test_fail_value_alloc_after(int nth);
bool cbm_msb_test_value_alloc_failed(void);
void cbm_msb_test_fail_prop_insert_after(int nth);
bool cbm_msb_test_prop_insert_failed(void);
uint64_t cbm_msb_test_value_live_bytes(void);
/* One-shot failure of the nth real operation of the selected kind. Zero
 * disables it; failed insertions skip the actual set and require verification. */
void cbm_msb_test_fail_item_operation(cbm_msb_item_fail_operation_t operation, int nth);
bool cbm_msb_test_item_operation_failed(void);

/* One-shot failure immediately before acquiring a real component node slot.
 * Empty/identity fast paths consume no allocation. NONE/nonpositive nth disable. */
void cbm_msb_test_fail_node_alloc(cbm_msb_node_fail_operation_t operation, int nth);
bool cbm_msb_test_node_alloc_failed(void);

/* Observations at real state operations; reset clears counters only, never
 * revision IDs, pool history, witness metadata or armed fault controls. */
typedef struct {
    uint64_t allocations;
    uint64_t slot_reuses;
    uint64_t witnessed_slot_reuses;
    uint64_t same_length_value_changes;
    uint64_t witness_skips;
    uint64_t revision_errors;
} cbm_msb_state_test_stats_t;
void cbm_msb_test_state_stats(cbm_msb_state_test_stats_t *out);

#endif

#endif /* CBM_PIPELINE_DOC_LINKS_MSBUILD_H */
