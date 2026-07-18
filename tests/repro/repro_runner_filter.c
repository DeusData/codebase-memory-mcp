/* Reproductions for the cumulative repro runner itself. */
#include "test_framework.h"
#include "repro_runner.h"
#include "foundation/compat.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int selector_delimiter(char value) {
    return value == ',' || value == ' ' || value == '\t' || value == '\r' || value == '\n';
}

static int suite_name_contains_token(const char *name, const char *token, size_t token_len) {
    size_t name_len = strlen(name);
    if (token_len > name_len) {
        return 0;
    }
    for (size_t offset = 0; offset + token_len <= name_len; offset++) {
        if (strncmp(name + offset, token, token_len) == 0) {
            return 1;
        }
    }
    return 0;
}

int cbm_suite_enabled(const char *name) {
    const char *only = getenv("CBM_REPRO_ONLY");
    if (!only || !*only) {
        return 1;
    }

    const char *cursor = only;
    while (*cursor) {
        while (*cursor && selector_delimiter(*cursor)) {
            cursor++;
        }
        const char *token = cursor;
        while (*cursor && !selector_delimiter(*cursor)) {
            cursor++;
        }
        size_t token_len = (size_t)(cursor - token);
        if (token_len > 0 && suite_name_contains_token(name, token, token_len)) {
            return 1;
        }
    }
    return 0;
}

static char *save_selector(void) {
    const char *prior = getenv("CBM_REPRO_ONLY");
    return prior ? cbm_strdup(prior) : NULL;
}

static void restore_selector(char *saved) {
    if (saved) {
        cbm_setenv("CBM_REPRO_ONLY", saved, 1);
        free(saved);
    } else {
        cbm_unsetenv("CBM_REPRO_ONLY");
    }
}

/* The documented selector accepts suite-name substrings. */
TEST(repro_runner_filter_accepts_suite_substring) {
    char *saved = save_selector();
    cbm_setenv("CBM_REPRO_ONLY", "call_argument", 1);
    int enabled = cbm_suite_enabled("repro_call_argument_usages");
    restore_selector(saved);
    if (!enabled)
        fprintf(stderr, "  [repro-filter] invariant=suite_substring_not_matched\n");
    ASSERT_TRUE(enabled);
    PASS();
}

TEST(repro_runner_filter_accepts_comma_list) {
    char *saved = save_selector();
    cbm_setenv("CBM_REPRO_ONLY", "language_registry,call_argument", 1);
    int first = cbm_suite_enabled("repro_language_registry");
    int second = cbm_suite_enabled("repro_call_argument_usages");
    restore_selector(saved);
    if (!first || !second)
        fprintf(stderr, "  [repro-filter] invariant=comma_list_not_matched\n");
    ASSERT_TRUE(first && second);
    PASS();
}

TEST(repro_runner_filter_accepts_space_list) {
    char *saved = save_selector();
    cbm_setenv("CBM_REPRO_ONLY", "language_registry call_argument", 1);
    int first = cbm_suite_enabled("repro_language_registry");
    int second = cbm_suite_enabled("repro_call_argument_usages");
    restore_selector(saved);
    if (!first || !second)
        fprintf(stderr, "  [repro-filter] invariant=space_list_not_matched\n");
    ASSERT_TRUE(first && second);
    PASS();
}

TEST(repro_runner_filter_rejects_nonmatch) {
    char *saved = save_selector();
    cbm_setenv("CBM_REPRO_ONLY", "language_registry", 1);
    int enabled = cbm_suite_enabled("repro_call_argument_usages");
    restore_selector(saved);
    if (enabled)
        fprintf(stderr, "  [repro-filter] invariant=nonmatch_enabled\n");
    ASSERT_FALSE(enabled);
    PASS();
}

TEST(repro_runner_filter_unset_or_empty_enables_all) {
    char *saved = save_selector();
    cbm_unsetenv("CBM_REPRO_ONLY");
    int unset_enabled = cbm_suite_enabled("repro_call_argument_usages");
    cbm_setenv("CBM_REPRO_ONLY", "", 1);
    int empty_enabled = cbm_suite_enabled("repro_call_argument_usages");
    restore_selector(saved);
    if (!unset_enabled || !empty_enabled)
        fprintf(stderr, "  [repro-filter] invariant=unset_or_empty_disabled\n");
    ASSERT_TRUE(unset_enabled && empty_enabled);
    PASS();
}

SUITE(repro_runner_filter) {
    RUN_TEST(repro_runner_filter_accepts_suite_substring);
    RUN_TEST(repro_runner_filter_accepts_comma_list);
    RUN_TEST(repro_runner_filter_accepts_space_list);
    RUN_TEST(repro_runner_filter_rejects_nonmatch);
    RUN_TEST(repro_runner_filter_unset_or_empty_enables_all);
}
