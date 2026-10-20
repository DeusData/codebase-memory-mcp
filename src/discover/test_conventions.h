#ifndef CBM_TEST_CONVENTIONS_H
#define CBM_TEST_CONVENTIONS_H
#include <stdbool.h>
#include <stddef.h>

/* Internal immutable descriptor snapshot for the approved tests.conventions
 * and tests.presets schema. This parser classifies no source and creates no
 * TESTS/macro edges. Consumers must separately verify that they can implement
 * every applicable declaration; an unsupported record is never ignored to
 * justify narrowing. */
typedef enum {
    CBM_TEST_DECL_CASE,
    CBM_TEST_DECL_SUITE,
    CBM_TEST_DECL_REGISTRATION,
    CBM_TEST_DECL_SUITE_REGISTRATION
} cbm_test_decl_role_t;

typedef struct {
    const char *language;
    cbm_test_decl_role_t role;
    const char *define_macro; /* definition roles only */
    const int *name_args;     /* definition roles; preserve order */
    int name_arg_count;
    const char *runner_id;  /* optional case template; NULL != guessed default */
    const char *macro;      /* registration roles only */
    int test_arg;           /* registration; -1 otherwise */
    int suite_arg;          /* suite_registration; -1 otherwise */
    const char *perf_macro; /* optional suite_registration alternate */
} cbm_test_declaration_t;

typedef enum {
    CBM_TEST_PRESET_C_CBM,
    CBM_TEST_PRESET_PYTEST,
    CBM_TEST_PRESET_GO,
    CBM_TEST_PRESET_JUNIT,
    CBM_TEST_PRESET_GTEST,
    CBM_TEST_PRESET_RUST,
    CBM_TEST_PRESET_JEST,
    CBM_TEST_PRESET_COUNT
} cbm_test_preset_t;

typedef struct cbm_test_declarations cbm_test_declarations_t;

/* Full project JSON, explicit bytes, max64KiB, independently owned. absent
 * requires len=0 (NULL bytes allowed); malformed or unsupported consumed
 * syntax => NULL. No path read, global state, env or shell evaluation.
 * Missing tests/conventions => empty custom list. Missing preset switches
 * preserve existing defaults (c-cbm false; six language presets true).
 * A missing impact section is distinct from a configured one with defaults.
 * runner_id is retained as opaque data; adapters must validate its template
 * language and produce an error for missing/unsupported required mappings. */
cbm_test_declarations_t *cbm_test_declarations_parse(const char *bytes, size_t len, bool absent);
void cbm_test_declarations_free(cbm_test_declarations_t *declarations);
bool cbm_test_declarations_configured(const cbm_test_declarations_t *declarations);
const cbm_test_declaration_t *cbm_test_declarations_items(
    const cbm_test_declarations_t *declarations, int *count);
/* false for invalid object/enum; clears enabled first. */
bool cbm_test_declarations_preset(const cbm_test_declarations_t *declarations,
                                  cbm_test_preset_t preset, bool *enabled);
const char *cbm_test_declarations_digest(const cbm_test_declarations_t *declarations);

#endif
