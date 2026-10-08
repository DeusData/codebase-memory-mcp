/*
 * test_impact.h — which tests does a change reach? (detect_changes scope:"tests")
 *
 * Part 1, the test model: what a repository's test sources define and what
 * its runner registers, read from the source text by convention.
 *
 * Suite membership comes from the registration itself — the name inside
 * RUN_TEST(...) and the TEST of that name in the same file — and never from a
 * resolver edge. An edge can be missing or point at a same-named symbol
 * elsewhere; the registration is what the compiler sees, and a selection that
 * names a test its suite never runs fails the run (tests/test_main.c).
 */
#ifndef CBM_MCP_TEST_IMPACT_H
#define CBM_MCP_TEST_IMPACT_H

#include "discover/test_conventions.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* The spellings a test framework uses. A NULL entry is a convention the
 * framework does not have. */
typedef struct {
    const char *case_macro;           /* TEST(name) { ... } */
    const char *suite_macro;          /* SUITE(name) { ... } */
    const char *suite_fn_prefix;      /* void suite_name(void) { ... }: what SUITE expands to */
    const char *run_macro;            /* RUN_TEST(name) inside a suite body */
    const char *suite_run_macro;      /* RUN_SELECTED_SUITE(name) in the runner */
    const char *suite_run_perf_macro; /* RUN_SELECTED_SUITE_PERF(name): a perf suite */
} cbm_test_conventions_t;

/* This repository's own C test framework (tests/test_framework.h). */
const cbm_test_conventions_t *cbm_test_conventions_cbm(void);

typedef struct {
    const char *file; /* as given to cbm_test_model_add_source */
    const char *name;
    int start_line; /* the line of TEST(name) */
    int end_line;   /* the line of the closing brace of its body */
    /* Defined under a preprocessor condition (an include guard is not one):
     * some builds do not have this test. */
    bool conditional;
} cbm_test_case_t;

typedef struct {
    const char *file;
    const char *name;
    int start_line;
    int end_line;
    /* The file defines a macro that registers tests. The suite may then run
     * tests the model cannot name, so it can only be selected whole. */
    bool macro_registrations;
    /* The model could not read this suite's file with certainty (for
     * example tests defined by a macro). The suite can only run whole; other
     * suites stay narrowable (cbm_test_model_narrowable). */
    bool uncertain;
} cbm_test_suite_t;

typedef struct {
    const char *file;
    const char *suite;
    const char *test;
    int line;
    bool conditional; /* registered under a preprocessor condition */
    bool resolved;    /* a case of that name is defined in the same file */
} cbm_test_registration_t;

typedef struct {
    const char *name;
    bool perf;
    const char *file; /* the runner file that registers it */
    int line;
} cbm_test_runner_suite_t;

typedef struct cbm_test_model cbm_test_model_t;

/* NULL when out of memory. The conventions are copied. */
cbm_test_model_t *cbm_test_model_new(const cbm_test_conventions_t *conventions);

/* Read one source file. `text` need not be NUL-terminated; (NULL, 0) is empty.
 * false = invalid input/state or out of memory. A successful read can still
 * leave parse uncertainty; it does not establish completeness. */
bool cbm_test_model_add_source(cbm_test_model_t *m, const char *file, const char *text, size_t len);

/* Order and link what was read. Call once, after the last file and before any
 * row accessors. Mapping and declaration accessors also work before finish.
 * false = out of memory. */
bool cbm_test_model_finish(cbm_test_model_t *m);

void cbm_test_model_free(cbm_test_model_t *m);

/* True only after finish and no sticky parse/lexical/unsupported-form
 * uncertainty in any added source. Rows remain available for diagnostics
 * when false, but cannot justify narrowing. This concerns the supplied
 * source text only: callers must separately establish that the external
 * file/runner inventory is complete and conventions are applicable. */
bool cbm_test_model_complete(const cbm_test_model_t *m);

/* True after finish when every uncertainty the model found is scoped to the
 * suites of the file it was found in (those suites are marked `uncertain` and
 * can only run whole). Uncertainty in a file that registers runner suites, in
 * a file with no suite of its own, in the configuration, or a failure keeps
 * this false, and nothing may be narrowed. complete() implies narrowable(). */
bool cbm_test_model_narrowable(const cbm_test_model_t *m);

/* Descriptor mode is independent of the legacy six-spelling constructor.
 * Copies every original record, ordered name_args, optional template and
 * preset switch. NULL means invalid input or allocation failure. Unsupported
 * mapping produces an owned diagnostic and prevents complete() from being true.
 * Exact supported explicit case template: "{suite}:{name}". Missing templates
 * do not acquire a default. Multi-component names are retained but unsupported.
 * c-cbm supplies C declarations plus the existing suite_ function form. */
typedef enum {
    CBM_TEST_MODEL_MAPPING_OK = 0,
    CBM_TEST_MODEL_MAPPING_INVALID_INPUT,
    CBM_TEST_MODEL_MAPPING_UNSUPPORTED_LANGUAGE,
    CBM_TEST_MODEL_MAPPING_UNSUPPORTED_PRESET,
    CBM_TEST_MODEL_MAPPING_UNSUPPORTED_NAME,
    CBM_TEST_MODEL_MAPPING_UNSUPPORTED_TEMPLATE,
    CBM_TEST_MODEL_MAPPING_AMBIGUOUS,
    CBM_TEST_MODEL_MAPPING_MISSING_ARGUMENT,
    CBM_TEST_MODEL_MAPPING_UNSUPPORTED_ARGUMENT,
    CBM_TEST_MODEL_MAPPING_MALFORMED_INVOCATION,
    CBM_TEST_MODEL_MAPPING_UNSUPPORTED_FORM
} cbm_test_model_mapping_status_t;

typedef struct {
    cbm_test_model_mapping_status_t reason;
    int declaration_index; /* original zero-based record index; -1 for builtin/input */
    const char *file;      /* borrowed model-owned copy, or NULL for config issues */
    int line;              /* 1-based source line, or 0 for config/input issues */
} cbm_test_model_mapping_issue_t;

cbm_test_model_t *cbm_test_model_new_declarations(const cbm_test_declarations_t *declarations);
/* Descriptor mode requires explicit language. Accepted canonical config names:
 * c/cpp/cuda; also accepts the discover API's C/C++/CUDA display names, and c++.
 * This adapter does not handle native presets. Applicable gtest in C++/CUDA
 * prevents narrowing; unrelated default native presets do not poison C.
 * false means invalid state/input or allocation failure. Mapping uncertainty
 * may return true: callers MUST require model_complete() before narrowing. */
bool cbm_test_model_add_source_language(cbm_test_model_t *model, const char *file,
                                        const char *language, const char *text, size_t len);
/* Canonically smallest issue by (file, line, declaration_index, reason), not
 * discovery order. Strings and original declaration rows live until model free.
 * NULL model yields INVALID_INPUT and initializes issue when supplied. */
cbm_test_model_mapping_status_t cbm_test_model_mapping_status(
    const cbm_test_model_t *model, cbm_test_model_mapping_issue_t *issue);
const cbm_test_declaration_t *cbm_test_model_declarations(const cbm_test_model_t *model,
                                                          int *count);

/* Cases, suites and registrations are ordered by (file, line); runner suites by
 * name, each once. The order never depends on the order the files were added
 * in. */
const cbm_test_case_t *cbm_test_model_cases(const cbm_test_model_t *m, int *count);
const cbm_test_suite_t *cbm_test_model_suites(const cbm_test_model_t *m, int *count);
const cbm_test_registration_t *cbm_test_model_registrations(const cbm_test_model_t *m, int *count);
const cbm_test_runner_suite_t *cbm_test_model_runner_suites(const cbm_test_model_t *m, int *count);

/* ── Part 2: the change ──────────────────────────────────────────────
 * A unified diff with zero context lines, as git writes it under the pinned
 * flags below, read into files and hunks. The reader takes text, not a
 * repository, so the selection is a function of the diff bytes. */

/* The flags that make `git diff` output independent of the user's git
 * settings: no external diff or textconv driver, no renames, no color, one
 * algorithm, fixed prefixes, zero context. Spliced in as
 * `git <CBM_TEST_IMPACT_GIT_PINS> diff <CBM_TEST_IMPACT_DIFF_FLAGS> A B`. */
#define CBM_TEST_IMPACT_GIT_PINS                                        \
    "-c core.quotePath=false -c color.ui=never -c diff.noprefix=false " \
    "-c diff.mnemonicPrefix=false"
#define CBM_TEST_IMPACT_DIFF_FLAGS                                                \
    "--no-ext-diff --no-textconv --no-color --no-renames --diff-algorithm=myers " \
    "--unified=0 --inter-hunk-context=0 --src-prefix=a/ --dst-prefix=b/"

typedef struct {
    /* Lines on the new side: [start, start + count). A hunk that only removes
     * lines has count 0, and start is then the line the removal follows. */
    int start;
    int count;
    const char *const *added; /* the new lines, without the leading '+' */
    int added_count;
    const char *const *removed; /* the old lines, without the leading '-' */
    int removed_count;
} cbm_diff_hunk_t;

typedef struct {
    const char *path;
    bool created; /* the file did not exist before */
    bool deleted; /* the file does not exist any more */
    bool binary;  /* git printed no text for it: there are no hunks to read */
    const cbm_diff_hunk_t *hunks;
    int hunk_count;
} cbm_diff_file_t;

typedef struct cbm_diff cbm_diff_t;

/* NULL when out of memory. A diff the reader does not fully understand still
 * parses; cbm_diff_complete then says so. */
cbm_diff_t *cbm_diff_parse(const char *text, size_t len);
void cbm_diff_free(cbm_diff_t *d);

/* Files ordered by path. */
const cbm_diff_file_t *cbm_diff_files(const cbm_diff_t *d, int *count);

/* false = something in the text was not understood (a quoted path, a hunk
 * shorter than its header says, a line outside any file). What was read is
 * then not the whole change, and nothing may be narrowed by it. */
bool cbm_diff_complete(const cbm_diff_t *d);

/* Coverage-map format 1 from scripts/test-impact/coverage-map.py. These are
 * internal data accessors, not an admission decision: callers must separately
 * verify metadata, provenance, ancestry, age and harness/build compatibility.
 * Missing tests and incomplete test/setup rows cannot justify narrowing. */
typedef struct {
    int id;
    const char *file; /* empty when only an external profile name was available */
    const char *name;
} cbm_coverage_function_t;

typedef struct {
    const char *suite;
    const char *name; /* "*" is between-test setup for the entire suite */
    bool complete;
    const char *reason;
    const int *function_ids; /* ascending, unique; IDs index the function table */
    int function_count;
} cbm_coverage_test_t;

typedef struct cbm_coverage_map cbm_coverage_map_t;

/* Explicit single-image profile identity format; no admission is implied. */
typedef enum {
    CBM_COVERAGE_FORMAT_NONE = 0,
    CBM_COVERAGE_FORMAT_FUNCTIONS = 1,
    CBM_COVERAGE_FORMAT_PROFILES = 2
} cbm_coverage_format_t;

typedef struct {
    char image_sha256[65]; /* lowercase SHA-256 of the one immutable image */
} cbm_coverage_profile_binding_t;

typedef struct {
    int id;                    /* dense 0-based; row index */
    const unsigned char *name; /* owned bytes; NOT a C-string contract */
    size_t name_length;        /* positive; embedded NUL is representable */
    uint64_t function_hash;    /* all 64 bits, without masking */
    uint64_t counter_count;    /* positive shape; no counter values stored */
} cbm_coverage_profile_t;

typedef struct {
    uint64_t max_input_bytes; /* profiles_len + tests_len; <= UINT64_MAX/8 */
    uint64_t max_items;       /* exactly 1 header + profile rows + test rows + ID references */
    size_t max_alloc_bytes;   /* cumulative logical allocation requests, including scratch */
    int max_ids;              /* positive, <= INT_MAX; dense universe cardinality */
} cbm_coverage_parse_limits_t;

typedef bool (*cbm_coverage_parse_cancel_fn)(void *context);

typedef enum {
    CBM_COVERAGE_PARSE_OK = 0,
    CBM_COVERAGE_PARSE_INVALID,     /* bad pointers/limits, invalid C call */
    CBM_COVERAGE_PARSE_FORMAT,      /* malformed/noncanonical identities or invalid test rows */
    CBM_COVERAGE_PARSE_UNSUPPORTED, /* well-formed recognized header, wire version != 2 */
    CBM_COVERAGE_PARSE_LIMIT,       /* byte/item/ID/logical allocation/representation limit */
    CBM_COVERAGE_PARSE_CANCELLED,
    CBM_COVERAGE_PARSE_OOM
} cbm_coverage_parse_status_t;

/* profiles.tsv strict header/rows are specified in the companion proposal.
 * Clear *out before all other validation. profiles must contain its header;
 * tests=NULL is allowed only with tests_len=0. No NUL terminators required.
 * limits and out are required; every limit is positive. Callback may be NULL.
 * Inputs/limits/context remain immutable for this synchronous call; callback
 * is prompt, non-reentrant and cannot mutate/free inputs or call this owner.
 * Poll before parsing, within <=65536 bytes or <=4096 visited elements,
 * including hash/copy/comparison/sort work, and before publishing success.
 * Every non-OK result leaves *out=NULL and frees all call-owned allocations.
 * OK owns all data in CBMArena storage and permits unobserved IDs explicitly.
 * OK never establishes image/universe/row completeness or artifact admission.
 * Use existing cbm_coverage_map_free; no shared mutable or cancellation state. */
cbm_coverage_parse_status_t cbm_coverage_map_parse_v2(const void *profiles, size_t profiles_len,
                                                      const char *tests, size_t tests_len,
                                                      const cbm_coverage_parse_limits_t *limits,
                                                      cbm_coverage_parse_cancel_fn cancelled,
                                                      void *cancel_context,
                                                      cbm_coverage_map_t **out);

/* O(1) borrowed views until map_free. NULL map -> NONE / 0 / NULL.
 * Generic count and exact digest work for formats 1 and 2. No reserialization:
 * v1 digest = original functions.tsv; v2 digest = entire original profiles.tsv,
 * including binding header. SHA-256 is a lowercase 64-hex C string. */
cbm_coverage_format_t cbm_coverage_map_format(const cbm_coverage_map_t *map);
int cbm_coverage_map_id_count(const cbm_coverage_map_t *map);
const char *cbm_coverage_map_identity_sha256(const cbm_coverage_map_t *map);

/* Format 2 only; other formats/NULL -> NULL and, for profiles, count=0.
 * count may be NULL. For a zero-ID v2 map profiles returns NULL/count=0,
 * but binding remains available. Records remain in dense ID order. */
const cbm_coverage_profile_binding_t *cbm_coverage_map_profile_binding(
    const cbm_coverage_map_t *map);
const cbm_coverage_profile_t *cbm_coverage_map_profiles(const cbm_coverage_map_t *map, int *count);

/* Existing test/find_test/intersection/free APIs retain their row semantics.
 * On v2, old functions() returns NULL/count=0, find_function() returns NULL,
 * functions_sha256() returns NULL, metadata_matches() returns false, and
 * check_receipt() returns INVALID|METADATA before interpreting v1 evidence.
 * All format-1 behavior, uniqueness and observed-ID guards stay unchanged. */

/* Explicit byte lengths; inputs need not be NUL-terminated. LF and CRLF are
 * accepted. NULL means invalid/incomplete syntax, inconsistent references, or
 * allocation failure; no partial map is returned. Does not read meta.json. */
cbm_coverage_map_t *cbm_coverage_map_parse(const char *functions, size_t functions_len,
                                           const char *tests, size_t tests_len);
void cbm_coverage_map_free(cbm_coverage_map_t *map);
/* Exact original functions.tsv byte digest; borrowed until map_free. */
const char *cbm_coverage_map_functions_sha256(const cbm_coverage_map_t *map);
const cbm_coverage_function_t *cbm_coverage_map_functions(const cbm_coverage_map_t *map,
                                                          int *count);
/* Rows are ordered by (suite, name), independent of input order. */
const cbm_coverage_test_t *cbm_coverage_map_tests(const cbm_coverage_map_t *map, int *count);
const cbm_coverage_function_t *cbm_coverage_map_find_function(const cbm_coverage_map_t *map,
                                                              const char *file, const char *name);
const cbm_coverage_test_t *cbm_coverage_map_find_test(const cbm_coverage_map_t *map,
                                                      const char *suite, const char *name);
/* Coverage intersection only. false does not establish that a test is safe
 * to omit: static reachability, incomplete/setup rows and other rules still apply. */
bool cbm_coverage_test_intersects(const cbm_coverage_test_t *test, const int *changed_ids,
                                  int changed_count);

/* Cross-check format-1 metadata against every parsed row and the trusted
 * artifact manifest's commit. Does not establish age, ancestry, harness/build
 * compatibility, provenance or expected runner membership; all remain required.
 * Valid incomplete rows stay incomplete and cannot justify narrowing. */
bool cbm_coverage_map_metadata_matches(const cbm_coverage_map_t *map, const char *metadata,
                                       size_t len, const char *expected_commit);

/* Query-local project configuration. No process environment or global config
 * participates. Only the fields consumed by this API are validated here;
 * the selection engine must validate its additional rules before narrowing. */
typedef struct cbm_test_config cbm_test_config_t;

/* Read one regular JSON file, at most 64 KiB. optional permits an absent
 * default file only; malformed, unreadable and oversized files always fail.
 * NULL means invalid configuration or allocation failure. */
cbm_test_config_t *cbm_test_config_load(const char *path, bool optional);
void cbm_test_config_free(cbm_test_config_t *config);
bool cbm_test_config_is_test_file(const cbm_test_config_t *config, const char *file);
/* Exact source-byte digest (or a domain-separated absent-file digest). */
const char *cbm_test_config_digest(const cbm_test_config_t *config);

/* Pure hybrid-selection kernel. It performs no I/O and does not admit an
 * artifact. The adapter must establish the evidence flags before calling it.
 * A false completeness flag is uncertainty, never proof of no changes. */
typedef struct {
    const char *file;
    const char *test;
    /* An unambiguous case mapping in the complete static traversal. Merely
     * finding a graph node is insufficient. Missing rows mean unknown. */
    bool mapped;
    bool reached;
    bool changed;
} cbm_test_reach_t;

enum {
    CBM_TEST_SELECT_CHANGED = 1U << 0,
    CBM_TEST_SELECT_STATIC = 1U << 1,
    CBM_TEST_SELECT_COVERAGE = 1U << 2,
    CBM_TEST_SELECT_UNMAPPED = 1U << 3,
    CBM_TEST_SELECT_COVERAGE_UNKNOWN = 1U << 4,
    CBM_TEST_SELECT_SETUP_HIT = 1U << 5,
    CBM_TEST_SELECT_SETUP_UNKNOWN = 1U << 6,
    CBM_TEST_SELECT_INVENTORY_UNKNOWN = 1U << 7,
    CBM_TEST_SELECT_CONDITIONAL = 1U << 8,
    CBM_TEST_SELECT_ARTIFACT_REJECTED = 1U << 9,
    CBM_TEST_SELECT_DIFF_INCOMPLETE = 1U << 10,
    CBM_TEST_SELECT_STATIC_INCOMPLETE = 1U << 11,
    CBM_TEST_SELECT_CHANGE_IDENTITY_UNKNOWN = 1U << 12,
    CBM_TEST_SELECT_INVALID_INPUT = 1U << 13,
    CBM_TEST_SELECT_CHANGED_UNREGISTERED = 1U << 14,
    CBM_TEST_SELECT_RULE = 1U << 15
};

/* Reached/changed suite bodies and explicit suite rules select the whole
 * suite even when no individual case is reached. Names come from the runner. */
typedef struct {
    const char *suite;
    bool reached;
    bool changed;
    bool rule;
} cbm_test_suite_trigger_t;

typedef struct {
    const cbm_test_model_t *model; /* finished, borrowed for this call only */
    const cbm_coverage_map_t *coverage;
    const cbm_test_reach_t *reach;
    int reach_count;
    const cbm_test_suite_trigger_t *suite_triggers;
    int suite_trigger_count;
    const int *changed_function_ids;
    int changed_function_count;
    bool has_changes;
    bool diff_complete;
    bool inventory_complete;        /* external file/runner inventory, separate from model parse */
    bool static_complete;           /* complete seeds and uncapped no-lift traversal */
    bool coverage_admitted;         /* independently verified artifact admission */
    bool coverage_changes_complete; /* includes old-side/deleted identities */
} cbm_test_selection_input_t;

typedef struct {
    const char *name;
    bool whole;
    unsigned reasons; /* union of whole-suite causes and retained-test reasons */
} cbm_test_selected_suite_t;

typedef struct {
    const char *suite;
    const char *test;
    const char *file;
    unsigned reasons;
} cbm_test_selected_case_t;

typedef struct cbm_test_selection cbm_test_selection_t;

/* NULL means allocation failure: callers MUST use their full-run fallback.
 * No successful partial result is returned. Results own all their strings.
 * A run-all result has no suite/case rows; its reason mask is nonzero.
 * Whole suites dominate individual rows. Selected conditional registrations
 * use whole suites until the runner has an explicit optional-test contract. */
cbm_test_selection_t *cbm_test_select(const cbm_test_selection_input_t *input);
void cbm_test_selection_free(cbm_test_selection_t *selection);
unsigned cbm_test_selection_run_all(const cbm_test_selection_t *selection);
/* Only selected suites/cases appear, ordered by name and (suite,test). */
const cbm_test_selected_suite_t *cbm_test_selection_suites(const cbm_test_selection_t *selection,
                                                           int *count);
const cbm_test_selected_case_t *cbm_test_selection_cases(const cbm_test_selection_t *selection,
                                                         int *count);

/* Coverage receipt-policy checks. This is NOT artifact retrieval or proof of
 * provenance: the adapter must independently establish the context below.
 * SHA-256 strings use 64 lowercase hex digits and bind exact file bytes. */
typedef struct {
    const char *commit;
    const char *functions_sha256;
    const char *tests_sha256;
    const char *metadata_sha256;
    const char *graph_sha256;
    const char *compatibility_sha256;
    /* Oldest retained test/setup observation, including complete empty rows.
     * A full-refresh baseline is valid only if all retained rows were refreshed.
     * Incremental publication MUST NOT advance this timestamp by itself. */
    int64_t oldest_observation_at;
} cbm_coverage_receipt_t;

typedef struct {
    /* Authenticates the entire receipt tuple and expected producer/repository,
     * not merely a download location. Never populated from artifact JSON. */
    bool source_verified;
    const char *trusted_commit;
    /* Evidence is for this exact pair, in this one frozen comparison. Git
     * errors/unknown ancestry are false; replacement objects must be disabled. */
    bool ancestor_verified;
    const char *ancestor_commit;
    const char *ancestor_merge_base;
    const char *merge_base;
    /* Independently measured from the exact team graph imported for this map,
     * before the adapter incrementally updates it to the PR head. */
    const char *graph_commit;
    const char *graph_sha256;
    /* Independently computed with the same versioned input encoding as the
     * producer, covering harness, runner, spawn helpers, config, build flags,
     * platform and toolchain. Equality cannot establish those inputs itself. */
    const char *compatibility_sha256;
    int64_t now;
} cbm_coverage_receipt_context_t;

enum {
    CBM_COVERAGE_RECEIPT_INVALID = 1U << 0,
    CBM_COVERAGE_RECEIPT_SOURCE = 1U << 1,
    CBM_COVERAGE_RECEIPT_ANCESTRY = 1U << 2,
    CBM_COVERAGE_RECEIPT_COMMIT = 1U << 3,
    CBM_COVERAGE_RECEIPT_CONTENT = 1U << 4,
    CBM_COVERAGE_RECEIPT_AGE = 1U << 5,
    CBM_COVERAGE_RECEIPT_COMPATIBILITY = 1U << 6,
    CBM_COVERAGE_RECEIPT_METADATA = 1U << 7
};

/* Zero means every receipt-policy check passed against the supplied evidence.
 * Newest eligible ancestor discovery, authenticated receipt delivery, actual
 * ancestry checks, graph freshness at head and runner inventory stay with the
 * adapter. A nonzero result prohibits coverage-based omission. */
unsigned cbm_coverage_map_check_receipt(const cbm_coverage_map_t *map, const char *metadata,
                                        size_t metadata_len, const cbm_coverage_receipt_t *receipt,
                                        const cbm_coverage_receipt_context_t *context);

/* Exact bytes retained by the query-local loader; borrowed until config free.
 * An optional absent file is distinct from an invalid config (NULL). */
const char *cbm_test_config_source(const cbm_test_config_t *config, size_t *len, bool *absent);

typedef enum {
    CBM_TEST_RULE_RUN_ALL,
    CBM_TEST_RULE_LANES,
    CBM_TEST_RULE_SUITES,
    CBM_TEST_RULE_REFERENCING_TESTS,
    CBM_TEST_RULE_IGNORE
} cbm_test_rule_action_t;

typedef struct {
    const char *id;
    cbm_test_rule_action_t action;
    const char *const *paths;
    int path_count;
    const char *const *suites;
    int suite_count;
    const char *const *lanes;
    int lane_count;
    int key_segment; /* zero-based path_segment:N; -1 for other actions */
    bool builtin;
} cbm_test_rule_t;

typedef struct {
    const char *name;
    const char *const *suites;
    int suite_count;
    const char *const *exclude_suites;
    int exclude_count;
    const char *const *signals;
    int signal_count;
    bool default_run;
    bool narrow; /* omitted => false; never changes lane activation itself */
} cbm_test_lane_t;

typedef struct cbm_test_policy cbm_test_policy_t;

/* Parse rules/lanes from the exact loaded source into an independent owner.
 * NULL means invalid consumed policy or allocation/expansion failure; no
 * partial policy escapes. Presets/conventions/signals definitions are left to
 * their adapters. Existing seed-only config loading does not call this API.
 * Missing policy uses conservative built-ins and one unit lane (narrow=true,
 * all suites except @perf). Project rules precede built-ins, FIRST MATCH wins.
 * Pattern grammar: positive gitignore-style globs plus comma brace alternatives
 * (including nested/adjacent groups); no shell evaluation, ranges or regex.
 * Expansion is bounded; overflow rejects the whole policy. */
cbm_test_policy_t *cbm_test_policy_new(const cbm_test_config_t *config);
void cbm_test_policy_free(cbm_test_policy_t *policy);
const cbm_test_rule_t *cbm_test_policy_rules(const cbm_test_policy_t *policy, int *count);
const cbm_test_lane_t *cbm_test_policy_lanes(const cbm_test_policy_t *policy, int *count);
const char *cbm_test_policy_digest(const cbm_test_policy_t *policy);
/* test_impact.coverage.compatibility_paths in config order: the files besides
 * the measured code whose bytes a coverage map depends on (harness, runner,
 * spawn helpers, build flags). NULL with *count 0 when none are configured. */
const char *const *cbm_test_policy_compatibility_paths(const cbm_test_policy_t *policy, int *count);

/* Successful classification returns true and either the first matched rule
 * or NULL for no match. false is an evaluation error, NEVER permission to
 * fall through or omit. Target applicability is resolved separately: an
 * unavailable target does not permit consulting a later rule. */
bool cbm_test_policy_match_path(const cbm_test_policy_t *policy, const char *path,
                                const cbm_test_rule_t **rule);
/* Match one known suite against lane membership or a suites rule. @perf is a
 * semantic selector. These functions do not establish inventory completeness,
 * platform applicability, or whether a lane is activated. false means error. */
bool cbm_test_policy_lane_selects_suite(const cbm_test_policy_t *policy, int lane,
                                        const char *suite, bool perf, bool *selected);
bool cbm_test_policy_rule_selects_suite(const cbm_test_policy_t *policy, int rule,
                                        const char *suite, bool perf, bool *selected);

#endif /* CBM_MCP_TEST_IMPACT_H */
