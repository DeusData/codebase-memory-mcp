/* Independent contract tests for the single-image format-2 coverage consumer. */
#include "test_framework.h"
#include <foundation/arena.h>
#include <foundation/compat_fs.h>
#include <mcp/test_impact.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define PV2_IMAGE "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
#define PV2_UPPER_IMAGE "0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef"
#define PV2_HEADER(n) "CBM_PROFILE_MAP\t2\t" PV2_IMAGE "\t" n "\n"
#define PV2_HASH "784ae7d054308c51cab4db32e73312ae3aaedb346aafd0806826a529c280b249"
#define PV2_SIZE(s) (sizeof(s) - 1)

_Static_assert(PV2_SIZE(PV2_UPPER_IMAGE) == 64, "uppercase image fixture must have 64 hex bytes");

static const char pv2_golden[] =
    PV2_HEADER("3")
    "0\t610062\t0000000000000000\t1\n"
    "1\t610062\t8000000000000000\t2\n"
    "2\tff\tffffffffffffffff\t18446744073709551615\n";
static const char pv2_tests[] = "core:*\tcomplete\t\t\ncore:seen\tcomplete\t\t0 1\n";
static const char pv2_one[] = PV2_HEADER("1") "0\t61\t0000000000000018\t1\n";
static const char pv2_one_tests[] = "s:*\tcomplete\t\t0\n";
static const char pv2_empty[] = PV2_HEADER("0");

/* Existing native v1 evidence: none of these controls calls an additive API. */
static const char pv2_v1_functions[] = "0\tsrc/source.c\tone\n1\tsrc/source.c\ttwo\n";
static const char pv2_v1_tests[] =
    "alpha:*\tcomplete\t\t1\nalpha:one\tcomplete\t\t0\nalpha:two\tcomplete\t\t1\n";
static const char pv2_v1_meta[] =
    "{\"format\":1,\"commit\":\"1111111111111111111111111111111111111111\",\"functions\":2,"
    "\"tests\":2,\"incomplete\":0,\"platform\":\"fixture\",\"llvm_profdata\":\"fixture\","
    "\"suites\":[{\"suite\":\"alpha\",\"tests\":2,\"incomplete\":0,\"exit\":0}]}\n";

static cbm_coverage_receipt_t pv2_receipt(void) {
    return (cbm_coverage_receipt_t){
        .commit = "1111111111111111111111111111111111111111",
        .functions_sha256 = "9fb6ff76736fdda3558510f3732c3af3be254476dc29e479bc1e2aa67db14c91",
        .tests_sha256 = "9301b8969a11a4b758f4d0d7128ba71bc00a90da1b6f89acbf175988abef362c",
        .metadata_sha256 = "ebdeebf656b0eace38a267f808365b6fea7eefb89f9feabd41c70d4d2cf156aa",
        .graph_sha256 = "4444444444444444444444444444444444444444444444444444444444444444",
        .compatibility_sha256 = "3333333333333333333333333333333333333333333333333333333333333333",
        .oldest_observation_at = INT64_C(2000000000),
    };
}

static cbm_coverage_receipt_context_t pv2_receipt_context(void) {
    return (cbm_coverage_receipt_context_t){
        .source_verified = true,
        .trusted_commit = "1111111111111111111111111111111111111111",
        .ancestor_verified = true,
        .ancestor_commit = "1111111111111111111111111111111111111111",
        .ancestor_merge_base = "2222222222222222222222222222222222222222",
        .merge_base = "2222222222222222222222222222222222222222",
        .graph_commit = "1111111111111111111111111111111111111111",
        .graph_sha256 = "4444444444444444444444444444444444444444444444444444444444444444",
        .compatibility_sha256 = "3333333333333333333333333333333333333333333333333333333333333333",
        .now = INT64_C(2000000000) + 604800,
    };
}

static cbm_coverage_map_t *pv2_v1_map(void) {
    return cbm_coverage_map_parse(pv2_v1_functions, PV2_SIZE(pv2_v1_functions),
                                  pv2_v1_tests, PV2_SIZE(pv2_v1_tests));
}

static bool pv2_native_controls(void) {
    cbm_coverage_map_t *map = pv2_v1_map();
    if (!map)
        return false;
    cbm_coverage_receipt_t receipt = pv2_receipt();
    cbm_coverage_receipt_context_t context = pv2_receipt_context();
    int count = -1;
    const cbm_coverage_function_t *rows = cbm_coverage_map_functions(map, &count);
    bool ok = rows && count == 2 && !strcmp(rows[1].name, "two") &&
              cbm_coverage_map_check_receipt(map, pv2_v1_meta, PV2_SIZE(pv2_v1_meta),
                                             &receipt, &context) == 0;
    cbm_coverage_map_free(map);
    const char orphan[] = "alpha:*\tcomplete\t\t0\n";
    map = cbm_coverage_map_parse(pv2_v1_functions, PV2_SIZE(pv2_v1_functions),
                                 orphan, PV2_SIZE(orphan));
    ok = (map == NULL) && ok;
    cbm_coverage_map_free(map);
    const char duplicate[] = "0\tsrc/source.c\tone\n1\tsrc/source.c\tone\n";
    map = cbm_coverage_map_parse(duplicate, PV2_SIZE(duplicate),
                                 pv2_v1_tests, PV2_SIZE(pv2_v1_tests));
    ok = (map == NULL) && ok;
    cbm_coverage_map_free(map);
    return ok;
}

static cbm_coverage_parse_limits_t pv2_limits(void) {
    return (cbm_coverage_parse_limits_t){
        .max_input_bytes = UINT64_C(16) * 1024 * 1024,
        .max_items = UINT64_C(1000000),
        .max_alloc_bytes = (size_t)64 * 1024 * 1024,
        .max_ids = 4096,
    };
}

static cbm_coverage_parse_status_t pv2_parse(const void *p, size_t n, const char *t, size_t m,
                                            cbm_coverage_map_t **out) {
    cbm_coverage_parse_limits_t limits = pv2_limits();
    return cbm_coverage_map_parse_v2(p, n, t, m, &limits, NULL, NULL, out);
}

static bool pv2_one_shape(const cbm_coverage_map_t *map) {
    int count = -1;
    const cbm_coverage_profile_t *rows = cbm_coverage_map_profiles(map, &count);
    const cbm_coverage_profile_binding_t *binding = cbm_coverage_map_profile_binding(map);
    return rows && count == 1 && rows[0].id == 0 && rows[0].name &&
           rows[0].name_length == 1 && rows[0].name[0] == 'a' &&
           rows[0].function_hash == UINT64_C(24) && rows[0].counter_count == 1 &&
           binding && !strcmp(binding->image_sha256, PV2_IMAGE);
}

static bool pv2_feature_control(void) {
    cbm_coverage_map_t *map = NULL;
    cbm_coverage_parse_status_t status = pv2_parse(pv2_one, PV2_SIZE(pv2_one),
                                                  pv2_one_tests, PV2_SIZE(pv2_one_tests), &map);
    bool ok = status == CBM_COVERAGE_PARSE_OK && pv2_one_shape(map);
    cbm_coverage_map_free(map);
    return ok;
}

/* A real live guard, never an invented pointer, verifies clearing and isolation. */
static bool pv2_failure(const void *p, size_t n, const char *t, size_t m,
                        const cbm_coverage_parse_limits_t *limits,
                        cbm_coverage_parse_status_t expected, cbm_coverage_map_t *guard) {
    cbm_coverage_map_t *out = guard;
    cbm_coverage_parse_status_t status =
        cbm_coverage_map_parse_v2(p, n, t, m, limits, NULL, NULL, &out);
    bool ok = status == expected && out == NULL;
    if (out != guard)
        cbm_coverage_map_free(out);
    return ok;
}

static bool pv2_golden_shape(const cbm_coverage_map_t *map) {
    static const unsigned char name[] = {'a', 0, 'b'};
    int count = -1;
    const cbm_coverage_profile_t *rows = cbm_coverage_map_profiles(map, &count);
    if (!rows || count != 3 || cbm_coverage_map_id_count(map) != 3)
        return false;
    for (int i = 0; i < 2; ++i) {
        if (rows[i].id != i || !rows[i].name || rows[i].name_length != sizeof(name) ||
            memcmp(rows[i].name, name, sizeof(name)))
            return false;
    }
    const cbm_coverage_profile_binding_t *binding = cbm_coverage_map_profile_binding(map);
    const char *digest = cbm_coverage_map_identity_sha256(map);
    return rows[0].function_hash == 0 && rows[0].counter_count == 1 &&
           rows[1].function_hash == UINT64_C(0x8000000000000000) &&
           rows[1].counter_count == 2 && rows[2].id == 2 && rows[2].name &&
           rows[2].name_length == 1 && rows[2].name[0] == 255 &&
           rows[2].function_hash == UINT64_MAX && rows[2].counter_count == UINT64_MAX &&
           binding && !strcmp(binding->image_sha256, PV2_IMAGE) &&
           digest && !strcmp(digest, PV2_HASH);
}

TEST(test_profile_v2_exact_golden_and_unobserved_ids) {
    ASSERT_TRUE(pv2_native_controls());
    ASSERT_TRUE(pv2_feature_control());
    cbm_coverage_map_t *map = NULL;
    cbm_coverage_parse_status_t status = pv2_parse(pv2_golden, PV2_SIZE(pv2_golden),
                                                  pv2_tests, PV2_SIZE(pv2_tests), &map);
    bool shape = status == CBM_COVERAGE_PARSE_OK && pv2_golden_shape(map);
    bool format = cbm_coverage_map_format(map) == CBM_COVERAGE_FORMAT_PROFILES;
    int count = -1;
    const cbm_coverage_test_t *tests = map ? cbm_coverage_map_tests(map, &count) : NULL;
    bool rows = tests && count == 2 && !strcmp(tests[0].suite, "core") &&
                !strcmp(tests[0].name, "*") && tests[0].complete &&
                tests[0].function_count == 0 && !strcmp(tests[1].name, "seen") &&
                tests[1].function_count == 2 && tests[1].function_ids[0] == 0 &&
                tests[1].function_ids[1] == 1;
    int unobserved = 2;
    int observed = 1;
    bool hits = rows && !cbm_coverage_test_intersects(&tests[0], &unobserved, 1) &&
                !cbm_coverage_test_intersects(&tests[1], &unobserved, 1) &&
                cbm_coverage_test_intersects(&tests[1], &observed, 1);
    cbm_coverage_map_free(map);
    ASSERT_EQ(PV2_SIZE(pv2_golden), 184);
    ASSERT_EQ(PV2_SIZE(pv2_tests), 42);
    ASSERT_TRUE(shape && format && rows && hits);
    PASS();
}

TEST(test_profile_v2_identity_order_and_duplicates) {
    ASSERT_TRUE(pv2_native_controls());
    ASSERT_TRUE(pv2_feature_control());
    const char ordered[] = PV2_HEADER("6")
        "0\t61\t0000000000000000\t1\n1\t61\t8000000000000000\t2\n"
        "2\t610062\t0000000000000000\t1\n3\t6161\t0000000000000000\t1\n"
        "4\t80\t0000000000000000\t1\n5\tff\tffffffffffffffff\t1\n";
    cbm_coverage_map_t *map = NULL;
    cbm_coverage_parse_status_t status = pv2_parse(ordered, PV2_SIZE(ordered), NULL, 0, &map);
    int count = -1;
    const cbm_coverage_profile_t *rows = cbm_coverage_map_profiles(map, &count);
    bool positive = status == CBM_COVERAGE_PARSE_OK && rows && count == 6 &&
                    rows[0].name_length == 1 && rows[2].name_length == 3 &&
                    rows[4].name[0] == 128 && rows[5].name[0] == 255;
    cbm_coverage_map_free(map);
    ASSERT_TRUE(positive);
    const char *bad[] = {
        PV2_HEADER("2") "0\t61\t0000000000000000\t1\n1\t61\t0000000000000000\t2\n",
        PV2_HEADER("2") "0\t61\t8000000000000000\t1\n1\t61\t0000000000000000\t1\n",
        PV2_HEADER("2") "0\t610062\t0000000000000000\t1\n1\t61\t0000000000000000\t1\n",
        PV2_HEADER("2") "0\tff\t0000000000000000\t1\n1\t80\t0000000000000000\t1\n",
        PV2_HEADER("2") "0\t61\t0000000000000000\t1\n2\t62\t0000000000000000\t1\n",
        PV2_HEADER("2") "0\t61\t0000000000000000\t1\n0\t62\t0000000000000000\t1\n",
        PV2_HEADER("1") "1\t61\t0000000000000000\t1\n",
        PV2_HEADER("2") "0\t61\t0000000000000000\t1\n",
        PV2_HEADER("0") "0\t61\t0000000000000000\t1\n",
    };
    cbm_coverage_map_t *guard = pv2_v1_map();
    ASSERT_NOT_NULL(guard);
    cbm_coverage_parse_limits_t limits = pv2_limits();
    bool rejected = true;
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); ++i) {
        bool one = pv2_failure(bad[i], strlen(bad[i]), NULL, 0, &limits,
                                CBM_COVERAGE_PARSE_FORMAT, guard);
        if (!one)
            printf("  profile identity case %zu failed\n", i);
        rejected = one && rejected;
    }
    cbm_coverage_map_free(guard);
    ASSERT_TRUE(rejected);
    PASS();
}

typedef struct {
    const char *text;
    size_t size;
    cbm_coverage_parse_status_t expected;
} pv2_bad_wire_t;
#define PV2_BAD(s) {s, sizeof(s) - 1, CBM_COVERAGE_PARSE_FORMAT}

TEST(test_profile_v2_strict_wire_and_all_truncations) {
    ASSERT_TRUE(pv2_native_controls());
    ASSERT_TRUE(pv2_feature_control());
    static const pv2_bad_wire_t bad[] = {
        PV2_BAD(""), PV2_BAD("\xef\xbb\xbf" PV2_HEADER("0")),
        PV2_BAD("OTHER\t2\t" PV2_IMAGE "\t0\n"),
        PV2_BAD("CBM_PROFILE_MAP\t02\t" PV2_IMAGE "\t0\n"),
        PV2_BAD("CBM_PROFILE_MAP\t18446744073709551616\t" PV2_IMAGE "\t0\n"),
        PV2_BAD("CBM_PROFILE_MAP\t2\tshort\t0\n"),
        PV2_BAD("CBM_PROFILE_MAP\t2\t" PV2_UPPER_IMAGE "\t0\n"),
        PV2_BAD(PV2_HEADER("00")), PV2_BAD(PV2_HEADER("-1")),
        PV2_BAD(PV2_HEADER("18446744073709551616")),
        PV2_BAD("CBM_PROFILE_MAP\t2\t" PV2_IMAGE "\t0\textra\n"),
        PV2_BAD("CBM_PROFILE_MAP\t2\t" PV2_IMAGE "\t0\r\n"),
        PV2_BAD(PV2_HEADER("0") "\n"), PV2_BAD(PV2_HEADER("0") "trailer\n"),
        PV2_BAD(PV2_HEADER("0") "\0"),
        PV2_BAD(PV2_HEADER("1") "00\t61\t0000000000000000\t1\n"),
        PV2_BAD(PV2_HEADER("1") "0\t6\t0000000000000000\t1\n"),
        PV2_BAD(PV2_HEADER("1") "0\t6A\t0000000000000000\t1\n"),
        PV2_BAD(PV2_HEADER("1") "0\tgg\t0000000000000000\t1\n"),
        PV2_BAD(PV2_HEADER("1") "0\t\t0000000000000000\t1\n"),
        PV2_BAD(PV2_HEADER("1") "0\t61\t00000000000000FF\t1\n"),
        PV2_BAD(PV2_HEADER("1") "0\t61\t000000000000000\t1\n"),
        PV2_BAD(PV2_HEADER("1") "0\t61\t10000000000000000\t1\n"),
        PV2_BAD(PV2_HEADER("1") "0\t61\t0x00000000000000\t1\n"),
        PV2_BAD(PV2_HEADER("1") "0\t61\t0000000000000000\t0\n"),
        PV2_BAD(PV2_HEADER("1") "0\t61\t0000000000000000\t01\n"),
        PV2_BAD(PV2_HEADER("1") "0\t61\t0000000000000000\t+1\n"),
        PV2_BAD(PV2_HEADER("1") "0\t61\t0000000000000000\t 1\n"),
        PV2_BAD(PV2_HEADER("1") "0\t61\t0000000000000000\t18446744073709551616\n"),
        PV2_BAD(PV2_HEADER("1") "0\t61\t0000000000000000\t1\textra\n"),
        PV2_BAD(PV2_HEADER("1") "0\t61\t0000000000000000\t1\r\n"),
        PV2_BAD(PV2_HEADER("1") "0\t61\t0000000000000000\t1\0\n"),
        PV2_BAD(PV2_HEADER("0") PV2_HEADER("0")),
        {"CBM_PROFILE_MAP\t3\t" PV2_IMAGE "\t0\n",
         sizeof("CBM_PROFILE_MAP\t3\t" PV2_IMAGE "\t0\n") - 1,
         CBM_COVERAGE_PARSE_UNSUPPORTED},
        {"CBM_PROFILE_MAP\t3\t" PV2_IMAGE "\t0\nnot-interpreted\n",
         sizeof("CBM_PROFILE_MAP\t3\t" PV2_IMAGE "\t0\nnot-interpreted\n") - 1,
         CBM_COVERAGE_PARSE_UNSUPPORTED},
    };
    cbm_coverage_map_t *guard = pv2_v1_map();
    ASSERT_NOT_NULL(guard);
    cbm_coverage_parse_limits_t limits = pv2_limits();
    bool rejected = true;
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); ++i) {
        bool one = pv2_failure(bad[i].text, bad[i].size, NULL, 0, &limits,
                                bad[i].expected, guard);
        if (!one)
            printf("  profile wire case %zu failed\n", i);
        rejected = one && rejected;
    }
    /* Every strict prefix of this declared-three-record table is incomplete. */
    for (size_t size = 0; size < PV2_SIZE(pv2_golden); ++size) {
        bool one = pv2_failure(pv2_golden, size, NULL, 0, &limits,
                                CBM_COVERAGE_PARSE_FORMAT, guard);
        if (!one)
            printf("  profile table truncation %zu failed\n", size);
        rejected = one && rejected;
    }
    cbm_coverage_map_free(guard);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(test_profile_v2_test_rows_and_zero_universe) {
    ASSERT_TRUE(pv2_native_controls());
    ASSERT_TRUE(pv2_feature_control());
    const char good[] =
        "z:*\tcomplete\t\t\r\na:t:part\tcomplete\t\t00 01\r\n"
        "a:missing\tincomplete\tmissing\rprofile\t\r\na:*\tcomplete\t\t0\n"
        "a:empty\tcomplete\t\t\n\xff:*\tcomplete\t\t\n\x80:*\tcomplete\t\t\n";
    cbm_coverage_map_t *map = NULL;
    cbm_coverage_parse_status_t status = pv2_parse(pv2_golden, PV2_SIZE(pv2_golden),
                                                  good, PV2_SIZE(good), &map);
    int count = -1;
    const cbm_coverage_test_t *rows = map ? cbm_coverage_map_tests(map, &count) : NULL;
    const cbm_coverage_test_t *part = map ? cbm_coverage_map_find_test(map, "a", "t:part") : NULL;
    const cbm_coverage_test_t *missing = map ? cbm_coverage_map_find_test(map, "a", "missing") : NULL;
    bool positive = status == CBM_COVERAGE_PARSE_OK && rows && count == 7 &&
                    !strcmp(rows[0].suite, "a") && !strcmp(rows[0].name, "*") &&
                    !strcmp(rows[4].suite, "z") && (unsigned char)rows[5].suite[0] == 128 &&
                    (unsigned char)rows[6].suite[0] == 255 && part && part->complete &&
                    part->function_count == 2 && part->function_ids[0] == 0 &&
                    part->function_ids[1] == 1 && missing && !missing->complete &&
                    !strcmp(missing->reason, "missing\rprofile") &&
                    cbm_coverage_map_find_test(map, "absent", "x") == NULL;
    cbm_coverage_map_free(map);
    ASSERT_TRUE(positive);
    const char *empty_rows[] = {"", "s:*\tcomplete\t\t\n",
                               "s:*\tincomplete\tunknown\t\ns:t\tcomplete\t\t\n"};
    bool zero_ok = true;
    for (size_t i = 0; i < sizeof(empty_rows) / sizeof(empty_rows[0]); ++i) {
        map = NULL;
        status = pv2_parse(pv2_empty, PV2_SIZE(pv2_empty), empty_rows[i], strlen(empty_rows[i]), &map);
        count = -1;
        const cbm_coverage_profile_t *profiles = cbm_coverage_map_profiles(map, &count);
        zero_ok = status == CBM_COVERAGE_PARSE_OK && map && count == 0 && !profiles &&
                  cbm_coverage_map_profile_binding(map) && cbm_coverage_map_id_count(map) == 0 && zero_ok;
        cbm_coverage_map_free(map);
    }
    map = NULL;
    status = pv2_parse(pv2_golden, PV2_SIZE(pv2_golden), NULL, 0, &map);
    if (map)
        cbm_coverage_map_tests(map, &count);
    bool no_rows = status == CBM_COVERAGE_PARSE_OK && pv2_golden_shape(map) && count == 0;
    cbm_coverage_map_free(map);
    ASSERT_TRUE(zero_ok && no_rows);
    static const pv2_bad_wire_t bad[] = {
        PV2_BAD("s:t\tcomplete\t\t0\n"),
        PV2_BAD("s:*\tcomplete\t\t0\ns:*\tcomplete\t\t1\n"),
        PV2_BAD("s:*\tcomplete\t\t0\ns:t\tcomplete\t\t1\ns:t\tcomplete\t\t1\n"),
        PV2_BAD("s:*\tcomplete\t\t0 0\n"), PV2_BAD("s:*\tcomplete\t\t1 0\n"),
        PV2_BAD("s:*\tcomplete\t\t3\n"), PV2_BAD("s:*\tcomplete\t\t-1\n"),
        PV2_BAD("s:*\tcomplete\t\t+1\n"), PV2_BAD("s:*\tcomplete\t\t2147483648\n"),
        PV2_BAD("s:*\tcomplete\t\t18446744073709551616\n"),
        PV2_BAD("s:*\tcomplete\t\t 0\n"), PV2_BAD("s:*\tcomplete\t\t0 \n"),
        PV2_BAD("s:*\tcomplete\t\t0  1\n"), PV2_BAD("s:*\tcomplete\t\t0x\n"),
        PV2_BAD("s:*\tcomplete\tproblem\t0\n"), PV2_BAD("s:*\tincomplete\t\t0\n"),
        PV2_BAD("s:*\tunknown\t\t0\n"), PV2_BAD("s\tcomplete\t\t0\n"),
        PV2_BAD(":*\tcomplete\t\t0\n"), PV2_BAD("s:\tcomplete\t\t0\n"),
        PV2_BAD("s:*\tcomplete\t\t0\textra\n"), PV2_BAD("s:*\tcomplete\t\t0"),
        PV2_BAD("s:*\tcomplete\t\t0\0 1\n"),
    };
    cbm_coverage_map_t *guard = pv2_v1_map();
    ASSERT_NOT_NULL(guard);
    cbm_coverage_parse_limits_t limits = pv2_limits();
    bool rejected = true;
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); ++i) {
        bool one = pv2_failure(pv2_golden, PV2_SIZE(pv2_golden), bad[i].text, bad[i].size,
                                &limits, CBM_COVERAGE_PARSE_FORMAT, guard);
        if (!one)
            printf("  profile test-row case %zu failed\n", i);
        rejected = one && rejected;
    }
    cbm_coverage_map_free(guard);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(test_profile_v2_limits_and_invalid_arguments) {
    ASSERT_TRUE(pv2_native_controls());
    ASSERT_TRUE(pv2_feature_control());
    cbm_coverage_parse_limits_t exact = pv2_limits();
    exact.max_input_bytes = 226;
    exact.max_items = 8;
    exact.max_ids = 3;
    cbm_coverage_map_t *map = NULL;
    cbm_coverage_parse_status_t status = cbm_coverage_map_parse_v2(
        pv2_golden, PV2_SIZE(pv2_golden), pv2_tests, PV2_SIZE(pv2_tests), &exact, NULL, NULL, &map);
    bool boundary = status == CBM_COVERAGE_PARSE_OK && pv2_golden_shape(map);
    cbm_coverage_map_free(map);
    ASSERT_TRUE(boundary);
    cbm_coverage_parse_limits_t empty_limit = exact;
    empty_limit.max_input_bytes = 85;
    empty_limit.max_items = 1;
    empty_limit.max_ids = 1;
    map = NULL;
    status = cbm_coverage_map_parse_v2(pv2_empty, PV2_SIZE(pv2_empty), NULL, 0,
                                       &empty_limit, NULL, NULL, &map);
    bool empty_boundary = status == CBM_COVERAGE_PARSE_OK && map &&
                          cbm_coverage_map_id_count(map) == 0;
    cbm_coverage_map_free(map);
    ASSERT_TRUE(empty_boundary);
    const char repeated[] = "s:*\tcomplete\t\t0\ns:t\tcomplete\t\t0\n";
    cbm_coverage_parse_limits_t repeated_limit = pv2_limits();
    repeated_limit.max_items = 6; /* header + profile + two rows + two references */
    map = NULL;
    status = cbm_coverage_map_parse_v2(pv2_one, PV2_SIZE(pv2_one), repeated,
                                       PV2_SIZE(repeated), &repeated_limit, NULL, NULL, &map);
    bool repeated_boundary = status == CBM_COVERAGE_PARSE_OK && pv2_one_shape(map);
    cbm_coverage_map_free(map);
    ASSERT_TRUE(repeated_boundary);
    cbm_coverage_map_t *guard = pv2_v1_map();
    ASSERT_NOT_NULL(guard);
    bool ok = true;
    repeated_limit.max_items = 5;
    ok = pv2_failure(pv2_one, PV2_SIZE(pv2_one), repeated, PV2_SIZE(repeated),
                       &repeated_limit, CBM_COVERAGE_PARSE_LIMIT, guard) && ok;
    for (int kind = 0; kind < 4; ++kind) {
        cbm_coverage_parse_limits_t limit = exact;
        if (kind == 0) limit.max_input_bytes--;
        if (kind == 1) limit.max_items--;
        if (kind == 2) limit.max_ids--;
        if (kind == 3) limit.max_alloc_bytes = 1;
        ok = pv2_failure(pv2_golden, PV2_SIZE(pv2_golden), pv2_tests, PV2_SIZE(pv2_tests),
                           &limit, CBM_COVERAGE_PARSE_LIMIT, guard) && ok;
    }
    empty_limit.max_input_bytes = 84;
    ok = pv2_failure(pv2_empty, PV2_SIZE(pv2_empty), NULL, 0, &empty_limit,
                       CBM_COVERAGE_PARSE_LIMIT, guard) && ok;
    for (int kind = 0; kind < 6; ++kind) {
        cbm_coverage_parse_limits_t limit = exact;
        if (kind == 0) limit.max_input_bytes = 0;
        if (kind == 1) limit.max_items = 0;
        if (kind == 2) limit.max_ids = 0;
        if (kind == 3) limit.max_alloc_bytes = 0;
        if (kind == 4) limit.max_ids = -1;
        if (kind == 5) limit.max_input_bytes = UINT64_MAX / 8 + 1;
        ok = pv2_failure(pv2_golden, PV2_SIZE(pv2_golden), pv2_tests, PV2_SIZE(pv2_tests),
                           &limit, CBM_COVERAGE_PARSE_INVALID, guard) && ok;
    }
    ok = pv2_failure(NULL, 0, NULL, 0, &exact, CBM_COVERAGE_PARSE_INVALID, guard) && ok;
    ok = pv2_failure(pv2_golden, PV2_SIZE(pv2_golden), NULL, 1, &exact,
                       CBM_COVERAGE_PARSE_INVALID, guard) && ok;
    ok = pv2_failure(pv2_golden, PV2_SIZE(pv2_golden), NULL, 0, NULL,
                       CBM_COVERAGE_PARSE_INVALID, guard) && ok;
    status = cbm_coverage_map_parse_v2(pv2_golden, PV2_SIZE(pv2_golden), NULL, 0,
                                       &exact, NULL, NULL, NULL);
    ok = status == CBM_COVERAGE_PARSE_INVALID && ok;
    ok = cbm_coverage_map_find_function(guard, "src/source.c", "two") != NULL && ok;
    cbm_coverage_map_free(guard);
    ASSERT_TRUE(ok);
    PASS();
}

typedef struct { size_t calls; size_t stop_at; } pv2_cancel_t;

static bool pv2_cancel(void *context) {
    pv2_cancel_t *state = context;
    state->calls++;
    return state->stop_at && state->calls >= state->stop_at;
}

typedef struct { char *data; size_t used; size_t capacity; } pv2_buffer_t;

static bool pv2_append(pv2_buffer_t *buffer, const char *text, size_t size) {
    if (size > buffer->capacity - buffer->used)
        return false;
    memcpy(buffer->data + buffer->used, text, size);
    buffer->used += size;
    return true;
}

static bool pv2_stress_input(CBMArena *arena, pv2_buffer_t *profiles, pv2_buffer_t *tests) {
    const size_t name_size = 70000;
    profiles->capacity = 300000;
    tests->capacity = 90000;
    profiles->data = cbm_arena_alloc(arena, profiles->capacity);
    tests->data = cbm_arena_alloc(arena, tests->capacity);
    if (!profiles->data || !tests->data)
        return false;
    const char header[] = PV2_HEADER("2");
    if (!pv2_append(profiles, header, PV2_SIZE(header)))
        return false;
    for (int row = 0; row < 2; ++row) {
        const char *start = row ? "1\t" : "0\t";
        if (!pv2_append(profiles, start, 2))
            return false;
        for (size_t i = 0; i < name_size; ++i) {
            if (!pv2_append(profiles, "61", 2))
                return false;
        }
        if (row && !pv2_append(profiles, "62", 2))
            return false;
        const char suffix[] = "\t0000000000000000\t1\n";
        if (!pv2_append(profiles, suffix, PV2_SIZE(suffix)))
            return false;
    }
    const char setup[] = "s:*\tcomplete\t\t0\ns:why\tincomplete\t";
    if (!pv2_append(tests, setup, PV2_SIZE(setup)))
        return false;
    for (size_t i = 0; i < name_size; ++i) {
        if (!pv2_append(tests, "r", 1))
            return false;
    }
    if (!pv2_append(tests, "\t1\n", 3))
        return false;
    for (int i = 127; i >= 0; --i) {
        char row[64];
        int size = snprintf(row, sizeof(row), "s:t%03d\tcomplete\t\t0 1\n", i);
        if (size < 0 || (size_t)size >= sizeof(row) || !pv2_append(tests, row, (size_t)size))
            return false;
    }
    return true;
}

static bool pv2_repeated_bytes(const unsigned char *bytes, size_t count, unsigned char value) {
    if (!bytes)
        return false;
    for (size_t i = 0; i < count; ++i) {
        if (bytes[i] != value)
            return false;
    }
    return true;
}

static bool pv2_stress_profiles(const cbm_coverage_map_t *map) {
    if (!map || cbm_coverage_map_format(map) != CBM_COVERAGE_FORMAT_PROFILES ||
        cbm_coverage_map_id_count(map) != 2)
        return false;
    int count = -1;
    const cbm_coverage_profile_t *rows = cbm_coverage_map_profiles(map, &count);
    const cbm_coverage_profile_binding_t *binding = cbm_coverage_map_profile_binding(map);
    if (!rows || count != 2 || !binding || strcmp(binding->image_sha256, PV2_IMAGE))
        return false;
    for (int i = 0; i < 2; ++i) {
        if (rows[i].id != i || rows[i].name_length != 70000 + (size_t)i ||
            rows[i].function_hash != 0 || rows[i].counter_count != 1 ||
            !pv2_repeated_bytes(rows[i].name, 70000, 'a'))
            return false;
    }
    return rows[1].name[70000] == 'b';
}

static bool pv2_stress_setup(const cbm_coverage_test_t *row) {
    return !strcmp(row->name, "*") && row->complete && row->reason[0] == '\0' &&
           row->function_count == 1 && row->function_ids && row->function_ids[0] == 0;
}

static bool pv2_stress_why(const cbm_coverage_test_t *row) {
    if (strcmp(row->name, "why") || row->complete || row->function_count != 1 ||
        !row->function_ids || row->function_ids[0] != 1)
        return false;
    return pv2_repeated_bytes((const unsigned char *)row->reason, 70000, 'r') &&
           row->reason[70000] == '\0';
}

static bool pv2_stress_cases(const cbm_coverage_test_t *rows) {
    for (int i = 0; i < 128; ++i) {
        char expected[8];
        if (snprintf(expected, sizeof(expected), "t%03d", i) != 4)
            return false;
        const cbm_coverage_test_t *row = &rows[i];
        if (strcmp(row->name, expected) || !row->complete || row->reason[0] != '\0' ||
            row->function_count != 2 || !row->function_ids ||
            row->function_ids[0] != 0 || row->function_ids[1] != 1)
            return false;
    }
    return true;
}

static bool pv2_stress_map(const cbm_coverage_map_t *map) {
    if (!pv2_stress_profiles(map))
        return false;
    int count = -1;
    const cbm_coverage_test_t *rows = cbm_coverage_map_tests(map, &count);
    if (!rows || count != 130)
        return false;
    for (int i = 0; i < count; ++i) {
        if (!rows[i].suite || strcmp(rows[i].suite, "s") || !rows[i].name || !rows[i].reason)
            return false;
    }
    return pv2_stress_setup(&rows[0]) && pv2_stress_cases(&rows[1]) && pv2_stress_why(&rows[129]);
}

TEST(test_profile_v2_cancellation_and_long_fields) {
    ASSERT_TRUE(pv2_native_controls());
    ASSERT_TRUE(pv2_feature_control());
    CBMArena arena;
    cbm_arena_init(&arena);
    pv2_buffer_t profiles = {0}, tests = {0};
    bool built = pv2_stress_input(&arena, &profiles, &tests);
    if (!built) {
        cbm_arena_destroy(&arena);
        FAIL("could not allocate bounded cancellation fixture");
    }
    cbm_coverage_parse_limits_t limits = pv2_limits();
    pv2_cancel_t control = {0};
    cbm_coverage_map_t *map = NULL;
    cbm_coverage_parse_status_t status = cbm_coverage_map_parse_v2(
        profiles.data, profiles.used, tests.data, tests.used, &limits, pv2_cancel, &control, &map);
    bool positive = status == CBM_COVERAGE_PARSE_OK && pv2_stress_map(map) && control.calls >= 2;
    cbm_coverage_map_free(map);
    if (!positive) {
        cbm_arena_destroy(&arena);
        FAIL("uncancelled long-field control failed");
    }
    bool cancelled_ok = true;
    size_t positions[11] = {1, 2};
    for (size_t i = 0; i < 9; ++i)
        positions[i + 2] = 1 + (control.calls - 1) / 8 * i;
    positions[10] = control.calls;
    for (size_t i = 0; i < sizeof(positions) / sizeof(positions[0]); ++i) {
        bool duplicate = false;
        for (size_t j = 0; j < i; ++j)
            duplicate = duplicate || positions[j] == positions[i];
        if (duplicate)
            continue;
        pv2_cancel_t cancel = {.stop_at = positions[i]};
        map = NULL;
        status = cbm_coverage_map_parse_v2(profiles.data, profiles.used, tests.data, tests.used,
                                           &limits, pv2_cancel, &cancel, &map);
        /* Counts/phases are not API: require cancellation whenever the predicate
         * actually fired; a shorter successful traversal must remain complete. */
        bool one = cancel.calls >= cancel.stop_at
                       ? status == CBM_COVERAGE_PARSE_CANCELLED && map == NULL
                       : status == CBM_COVERAGE_PARSE_OK && pv2_stress_map(map);
        if (positions[i] <= 2)
            one = one && status == CBM_COVERAGE_PARSE_CANCELLED;
        cancelled_ok = one && cancelled_ok;
        cbm_coverage_map_free(map);
    }
    map = NULL;
    status = pv2_parse(profiles.data, profiles.used, tests.data, tests.used, &map);
    bool recovery = status == CBM_COVERAGE_PARSE_OK && pv2_stress_map(map);
    cbm_coverage_map_free(map);
    cbm_arena_destroy(&arena);
    ASSERT_TRUE(cancelled_ok && recovery);
    PASS();
}

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    bool ready;
    bool release;
    int wait_error;
    bool retained_ok;
    cbm_coverage_parse_status_t status;
    cbm_coverage_map_t *map;
} pv2_held_owner_t;

static void *pv2_held_owner(void *context) {
    pv2_held_owner_t *held = context;
    held->status = pv2_parse(pv2_one, PV2_SIZE(pv2_one), pv2_one_tests,
                              PV2_SIZE(pv2_one_tests), &held->map);
    pthread_mutex_lock(&held->mutex);
    held->ready = true;
    pthread_cond_broadcast(&held->condition);
    while (!held->release && !held->wait_error)
        held->wait_error = pthread_cond_wait(&held->condition, &held->mutex);
    held->retained_ok = held->status == CBM_COVERAGE_PARSE_OK && pv2_one_shape(held->map);
    cbm_coverage_map_t *map = held->map;
    held->map = NULL;
    pthread_mutex_unlock(&held->mutex);
    cbm_coverage_map_free(map);
    return NULL;
}

static bool pv2_owner_isolation(void) {
    pv2_held_owner_t held = {0};
    if (pthread_mutex_init(&held.mutex, NULL))
        return false;
    if (pthread_cond_init(&held.condition, NULL)) {
        pthread_mutex_destroy(&held.mutex);
        return false;
    }
    pthread_t thread;
    int created = pthread_create(&thread, NULL, pv2_held_owner, &held);
    if (created) {
        pthread_cond_destroy(&held.condition);
        pthread_mutex_destroy(&held.mutex);
        return false;
    }
    pthread_mutex_lock(&held.mutex);
    int waited = 0;
    while (!held.ready && !waited)
        waited = pthread_cond_wait(&held.condition, &held.mutex);
    bool first = !waited && !held.wait_error && held.status == CBM_COVERAGE_PARSE_OK &&
                 pv2_one_shape(held.map);
    cbm_coverage_map_t *second = NULL;
    cbm_coverage_parse_status_t status = pv2_parse(pv2_golden, PV2_SIZE(pv2_golden),
                                                  pv2_tests, PV2_SIZE(pv2_tests), &second);
    bool distinct = first && status == CBM_COVERAGE_PARSE_OK && second != held.map &&
                    pv2_golden_shape(second);
    cbm_coverage_map_t *cancelled = second;
    pv2_cancel_t cancel = {.stop_at = 1};
    cbm_coverage_parse_limits_t limits = pv2_limits();
    status = cbm_coverage_map_parse_v2(pv2_golden, PV2_SIZE(pv2_golden), pv2_tests,
                                       PV2_SIZE(pv2_tests), &limits, pv2_cancel, &cancel, &cancelled);
    bool isolated = status == CBM_COVERAGE_PARSE_CANCELLED && !cancelled &&
                    pv2_golden_shape(second) && first && pv2_one_shape(held.map);
    if (cancelled != second)
        cbm_coverage_map_free(cancelled);
    cbm_coverage_map_free(second);
    held.release = true;
    pthread_cond_broadcast(&held.condition);
    pthread_mutex_unlock(&held.mutex);
    int joined = pthread_join(thread, NULL);
    bool retained = held.retained_ok && !held.wait_error;
    int cond_destroyed = pthread_cond_destroy(&held.condition);
    int mutex_destroyed = pthread_mutex_destroy(&held.mutex);
    return first && distinct && isolated && retained && !joined && !cond_destroyed && !mutex_destroyed;
}

TEST(test_profile_v2_owned_bytes_and_held_owner_isolation) {
    ASSERT_TRUE(pv2_native_controls());
    ASSERT_TRUE(pv2_feature_control());
    CBMArena arena;
    cbm_arena_init(&arena);
    char *profiles = cbm_arena_alloc(&arena, PV2_SIZE(pv2_golden));
    char *tests = cbm_arena_alloc(&arena, PV2_SIZE(pv2_tests));
    if (!profiles || !tests) {
        cbm_arena_destroy(&arena);
        FAIL("could not allocate ownership fixture");
    }
    memcpy(profiles, pv2_golden, PV2_SIZE(pv2_golden));
    memcpy(tests, pv2_tests, PV2_SIZE(pv2_tests));
    cbm_coverage_map_t *map = NULL;
    cbm_coverage_parse_status_t status = pv2_parse(profiles, PV2_SIZE(pv2_golden), tests,
                                                  PV2_SIZE(pv2_tests), &map);
    memset(profiles, 0xa5, PV2_SIZE(pv2_golden));
    memset(tests, 0x5a, PV2_SIZE(pv2_tests));
    cbm_arena_destroy(&arena);
    const cbm_coverage_test_t *row = map ? cbm_coverage_map_find_test(map, "core", "seen") : NULL;
    bool retained = status == CBM_COVERAGE_PARSE_OK && pv2_golden_shape(map) && row &&
                    row->complete && !strcmp(row->reason, "") && row->function_count == 2 &&
                    row->function_ids[0] == 0 && row->function_ids[1] == 1;
    cbm_coverage_map_free(map);
    ASSERT_TRUE(retained);
    ASSERT_TRUE(pv2_owner_isolation());
    PASS();
}

TEST(test_profile_v2_legacy_accessors_and_receipt_boundary) {
    ASSERT_TRUE(pv2_native_controls());
    ASSERT_TRUE(pv2_feature_control());
    cbm_coverage_map_t *legacy = pv2_v1_map();
    ASSERT_NOT_NULL(legacy);
    int count = -1;
    const cbm_coverage_profile_t *profiles = cbm_coverage_map_profiles(legacy, &count);
    const char *generic = cbm_coverage_map_identity_sha256(legacy);
    const char *original = cbm_coverage_map_functions_sha256(legacy);
    bool legacy_ok = cbm_coverage_map_format(legacy) == CBM_COVERAGE_FORMAT_FUNCTIONS &&
                     cbm_coverage_map_id_count(legacy) == 2 && !profiles && count == 0 &&
                     !cbm_coverage_map_profile_binding(legacy) && generic && original &&
                     !strcmp(generic, original) && !strcmp(original, pv2_receipt().functions_sha256);
    cbm_coverage_map_free(legacy);
    ASSERT_TRUE(legacy_ok);
    cbm_coverage_map_t *map = NULL;
    cbm_coverage_parse_status_t status = pv2_parse(pv2_golden, PV2_SIZE(pv2_golden),
                                                  pv2_tests, PV2_SIZE(pv2_tests), &map);
    count = -1;
    const cbm_coverage_function_t *functions = map ? cbm_coverage_map_functions(map, &count) : NULL;
    cbm_coverage_receipt_t receipt = pv2_receipt();
    cbm_coverage_receipt_context_t context = pv2_receipt_context();
    bool unavailable = status == CBM_COVERAGE_PARSE_OK && map && !functions && count == 0 &&
                       !cbm_coverage_map_find_function(map, "src/source.c", "one") &&
                       !cbm_coverage_map_functions_sha256(map) &&
                       !cbm_coverage_map_metadata_matches(map, pv2_v1_meta,
                                                           PV2_SIZE(pv2_v1_meta), receipt.commit);
    unsigned refused = cbm_coverage_map_check_receipt(map, pv2_v1_meta, PV2_SIZE(pv2_v1_meta),
                                                       &receipt, &context);
    unsigned refused_null = cbm_coverage_map_check_receipt(map, NULL, 0, NULL, NULL);
    cbm_coverage_map_free(map);
    count = 123;
    bool null_ok = cbm_coverage_map_format(NULL) == CBM_COVERAGE_FORMAT_NONE &&
                   cbm_coverage_map_id_count(NULL) == 0 && !cbm_coverage_map_identity_sha256(NULL) &&
                   !cbm_coverage_map_profile_binding(NULL) &&
                   !cbm_coverage_map_profiles(NULL, &count) && count == 0 &&
                   !cbm_coverage_map_profiles(NULL, NULL);
    ASSERT_TRUE(unavailable && null_ok);
    ASSERT_EQ(refused, CBM_COVERAGE_RECEIPT_INVALID | CBM_COVERAGE_RECEIPT_METADATA);
    ASSERT_EQ(refused_null, CBM_COVERAGE_RECEIPT_INVALID | CBM_COVERAGE_RECEIPT_METADATA);
    PASS();
}

/* These exact files are also checked against the pure Python producer output. */
static bool pv2_shared_read(const char *path, void *bytes, size_t expected) {
    FILE *file = cbm_fopen(path, "rb");
    if (!file)
        return false;
    size_t count = fread(bytes, 1, expected, file);
    int extra = fgetc(file);
    bool ok = count == expected && extra == EOF && !ferror(file);
    if (fclose(file) != 0)
        ok = false;
    return ok;
}

static bool pv2_shared_binding(const cbm_coverage_map_t *map) {
    const cbm_coverage_profile_binding_t *binding = cbm_coverage_map_profile_binding(map);
    const char *digest = cbm_coverage_map_identity_sha256(map);
    return cbm_coverage_map_format(map) == CBM_COVERAGE_FORMAT_PROFILES &&
           cbm_coverage_map_id_count(map) == 3 && binding &&
           !strcmp(binding->image_sha256, PV2_IMAGE) && digest &&
           !strcmp(digest, "ecc78b966bef4eb27a144eba98c2794ebf5f31a318f269e5e21d3453afd02d76");
}

static bool pv2_shared_profiles(const cbm_coverage_map_t *map) {
    static const unsigned char opaque_name[] = {0xff, ':', '\t', ' ', 'z'};
    int count = -1;
    const cbm_coverage_profile_t *profiles = cbm_coverage_map_profiles(map, &count);
    if (!profiles || count != 3 || !pv2_shared_binding(map))
        return false;
    for (int i = 0; i < 2; ++i) {
        if (profiles[i].id != i || !profiles[i].name || profiles[i].name_length != 1 ||
            profiles[i].name[0] != 'a')
            return false;
    }
    return profiles[0].function_hash == 0 && profiles[0].counter_count == 2 &&
           profiles[1].function_hash == UINT64_C(0x8000000000000000) &&
           profiles[1].counter_count == 1 && profiles[2].id == 2 &&
           profiles[2].name && profiles[2].name_length == sizeof(opaque_name) &&
           !memcmp(profiles[2].name, opaque_name, sizeof(opaque_name)) &&
           profiles[2].function_hash == UINT64_MAX && profiles[2].counter_count == 2;
}

static bool pv2_shared_empty_row(const cbm_coverage_test_t *row, const char *name,
                                 bool complete, const char *reason) {
    return row->suite && !strcmp(row->suite, "s\xfe") && row->name &&
           !strcmp(row->name, name) && row->complete == complete && row->reason &&
           !strcmp(row->reason, reason) && row->function_count == 0;
}

static bool pv2_shared_hit_row(const cbm_coverage_test_t *row) {
    return row->suite && !strcmp(row->suite, "s\xfe") && row->name &&
           !strcmp(row->name, "hit:raw\xfd") && row->complete && row->reason &&
           !strcmp(row->reason, "") && row->function_count == 2 && row->function_ids &&
           row->function_ids[0] == 0 && row->function_ids[1] == 2;
}

static bool pv2_shared_intersections(const cbm_coverage_test_t *rows) {
    int unobserved = 1;
    for (int i = 0; i < 4; ++i) {
        if (cbm_coverage_test_intersects(&rows[i], &unobserved, 1))
            return false;
    }
    for (int id = 0; id <= 2; id += 2) {
        for (int i = 0; i < 4; ++i) {
            if (cbm_coverage_test_intersects(&rows[i], &id, 1) != (i == 1))
                return false;
        }
    }
    return true;
}

static bool pv2_shared_tests(const cbm_coverage_map_t *map) {
    int count = -1;
    const cbm_coverage_test_t *rows = cbm_coverage_map_tests(map, &count);
    if (!rows || count != 4)
        return false;
    return pv2_shared_empty_row(&rows[0], "*", true, "") &&
           pv2_shared_hit_row(&rows[1]) &&
           pv2_shared_empty_row(&rows[2], "missing", false, "missing_parent") &&
           pv2_shared_empty_row(&rows[3], "quiet", true, "") &&
           cbm_coverage_map_find_test(map, "s\xfe", "hit:raw\xfd") == &rows[1] &&
           pv2_shared_intersections(rows);
}

TEST(test_profile_v2_shared_producer_fixture) {
    ASSERT_TRUE(pv2_native_controls());
    ASSERT_TRUE(pv2_feature_control());
    unsigned char profiles[165];
    char tests[100];
    if (!pv2_shared_read("tests/fixtures/profile_map_v2/profiles.tsv", profiles, sizeof(profiles)))
        FAIL("shared profiles.tsv fixture is missing, unreadable, or not exactly 165 bytes");
    if (!pv2_shared_read("tests/fixtures/profile_map_v2/tests.tsv", tests, sizeof(tests)))
        FAIL("shared tests.tsv fixture is missing, unreadable, or not exactly 100 bytes");
    cbm_coverage_map_t *map = NULL;
    cbm_coverage_parse_status_t status =
        pv2_parse(profiles, sizeof(profiles), tests, sizeof(tests), &map);
    bool profiles_ok = status == CBM_COVERAGE_PARSE_OK && pv2_shared_profiles(map);
    bool tests_ok = status == CBM_COVERAGE_PARSE_OK && pv2_shared_tests(map);
    cbm_coverage_map_free(map);
    ASSERT_TRUE(profiles_ok);
    ASSERT_TRUE(tests_ok);
    PASS();
}

SUITE(test_impact_profiles) {
    RUN_TEST(test_profile_v2_exact_golden_and_unobserved_ids);
    RUN_TEST(test_profile_v2_identity_order_and_duplicates);
    RUN_TEST(test_profile_v2_strict_wire_and_all_truncations);
    RUN_TEST(test_profile_v2_test_rows_and_zero_universe);
    RUN_TEST(test_profile_v2_limits_and_invalid_arguments);
    RUN_TEST(test_profile_v2_cancellation_and_long_fields);
    RUN_TEST(test_profile_v2_owned_bytes_and_held_owner_isolation);
    RUN_TEST(test_profile_v2_legacy_accessors_and_receipt_boundary);
    RUN_TEST(test_profile_v2_shared_producer_fixture);
}
