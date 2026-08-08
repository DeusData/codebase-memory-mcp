/*
 * Production-configuration entry point for the native extraction benchmarks.
 *
 * The ordinary test runner intentionally enables test seams and uses -O1 so
 * sanitizers and fault injection stay useful.  Performance comparisons need
 * the release compiler flags, allocator binding, and production source set;
 * Makefile.cbm therefore links this small selector with those exact inputs.
 */
#include "test_framework.h"
#include "cbm.h"
#include <time.h>

int tf_pass_count = 0;
int tf_fail_count = 0;
int tf_skip_count = 0;
int tf_filter_count = 0;

const char *tf_repository_root(void) {
    return NULL;
}

extern void suite_cs_lsp_bench(void);
extern void suite_py_lsp_bench(void);
extern void suite_py_lsp_scale(void);

static const char *shared_bench_source =
    "{\n"
    "  \"projects\": [\n"
    "    {\"name\": \"alpha\", \"enabled\": true, \"weights\": [1, 2, 3, 5, 8]},\n"
    "    {\"name\": \"beta\", \"enabled\": false, \"weights\": [13, 21, 34]},\n"
    "    {\"name\": \"gamma\", \"enabled\": true, \"weights\": [55, 89]}\n"
    "  ],\n"
    "  \"limits\": {\"workers\": 8, \"timeout_ms\": 3000},\n"
    "  \"features\": {\"calls\": true, \"usages\": true, \"types\": true}\n"
    "}\n";

static double elapsed_ms(struct timespec start, struct timespec end) {
    double seconds = (double)(end.tv_sec - start.tv_sec);
    double nanoseconds = (double)(end.tv_nsec - start.tv_nsec);
    return seconds * 1000.0 + nanoseconds / 1000000.0;
}

TEST(shared_parse_baseline) {
    enum { ITERATIONS = 1024 };
    int definitions = -1;
    int calls = -1;
    int resolved = -1;
    int usages = -1;
    int type_refs = -1;
    int read_write = -1;
    struct timespec start;
    struct timespec end;

    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
        CBMFileResult *result =
            cbm_extract_file(shared_bench_source, (int)strlen(shared_bench_source), CBM_LANG_JSON,
                             "bench", "shared.json", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);

        if (iteration == 0) {
            definitions = result->defs.count;
            calls = result->calls.count;
            resolved = result->resolved_calls.count;
            usages = result->usages.count;
            type_refs = result->type_refs.count;
            read_write = result->rw.count;
        }
        bool output_matches =
            definitions == result->defs.count && calls == result->calls.count &&
            resolved == result->resolved_calls.count && usages == result->usages.count &&
            type_refs == result->type_refs.count && read_write == result->rw.count;
        cbm_free_result(result);
        ASSERT_TRUE(output_matches);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    int lines = 0;
    for (const char *cursor = shared_bench_source; *cursor != '\0'; ++cursor) {
        if (*cursor == '\n')
            lines++;
    }
    double milliseconds = elapsed_ms(start, end);
    int resolution_percent = calls > 0 ? resolved * 100 / calls : 0;
    printf("    bench: %d lines, %d defs, %d calls, %d resolved (%d%%), "
           "%d usages, %d type_refs, %d rw, %.2f ms\n",
           lines, definitions, calls, resolved, resolution_percent, usages, type_refs, read_write,
           milliseconds);
    PASS();
}

SUITE(shared_parse_baseline) {
    RUN_TEST(shared_parse_baseline);
}

static void print_usage(const char *program) {
    fprintf(stderr,
            "usage: %s {shared_parse_baseline|cs_lsp_bench|py_lsp_bench|py_lsp_scale|all}\n",
            program);
}

static void print_profile(void) {
    uint64_t parse_ns = 0;
    uint64_t extract_ns = 0;
    uint64_t files = 0;
    uint64_t lsp_ns = cbm_get_lsp_ns();
    cbm_get_profile((cbm_profile_out_t){
        .parse_ns = &parse_ns,
        .extract_ns = &extract_ns,
        .files = &files,
    });
    uint64_t non_lsp_ns = extract_ns >= lsp_ns ? extract_ns - lsp_ns : 0;
    printf("    profile: files=%llu parse=%.3fms non_lsp=%.3fms lsp=%.3fms total_extract=%.3fms\n",
           (unsigned long long)files, (double)parse_ns / 1000000.0, (double)non_lsp_ns / 1000000.0,
           (double)lsp_ns / 1000000.0, (double)extract_ns / 1000000.0);
}

int main(int argc, char **argv) {
    if (argc != 2) {
        print_usage(argv[0]);
        return 2;
    }

    if (strcmp(argv[1], "shared_parse_baseline") == 0 || strcmp(argv[1], "all") == 0) {
        cbm_reset_profile();
        RUN_SUITE(shared_parse_baseline);
        print_profile();
    }
    if (strcmp(argv[1], "cs_lsp_bench") == 0 || strcmp(argv[1], "all") == 0) {
        cbm_reset_profile();
        RUN_SUITE(cs_lsp_bench);
        print_profile();
    }
    if (strcmp(argv[1], "py_lsp_bench") == 0 || strcmp(argv[1], "all") == 0) {
        cbm_reset_profile();
        RUN_SUITE(py_lsp_bench);
        print_profile();
    }
    if (strcmp(argv[1], "py_lsp_scale") == 0 || strcmp(argv[1], "all") == 0) {
        cbm_reset_profile();
        RUN_SUITE(py_lsp_scale);
        print_profile();
    }
    if (strcmp(argv[1], "shared_parse_baseline") != 0 && strcmp(argv[1], "cs_lsp_bench") != 0 &&
        strcmp(argv[1], "py_lsp_bench") != 0 && strcmp(argv[1], "py_lsp_scale") != 0 &&
        strcmp(argv[1], "all") != 0) {
        print_usage(argv[0]);
        return 2;
    }

    TEST_SUMMARY();
}
