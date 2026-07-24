/*
 * test_windows_launcher_state.c — Permanent Windows launcher state contract.
 *
 * This suite exercises the stable public byte contract for the managed Windows
 * layout: a tiny launcher reads one fixed current-v1 record and resolves the
 * immutable payload generation
 * relative to its own location. Keep these pure helpers platform-independent
 * so the record format and command policy are exercised on every CI host.
 */
#include "test_framework.h"

#include <cli/windows_launcher_state.h>

#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <wchar.h>

static const char launcher_sha256[] = "0123456789abcdef0123456789abcdef"
                                      "0123456789abcdef0123456789abcdef";

static cbm_windows_current_v1_t launcher_valid_state(void) {
    cbm_windows_current_v1_t state;
    memset(&state, 0, sizeof(state));
    state.launcher_abi_min = 1;
    state.launcher_abi_max = 3;
    state.payload_size = UINT64_C(0x0102030405060708);
    memcpy(state.payload_sha256, launcher_sha256, sizeof(launcher_sha256));
    return state;
}

static bool launcher_memory_is_zero(const void *memory, size_t size) {
    const uint8_t *bytes = memory;
    for (size_t index = 0; index < size; index++) {
        if (bytes[index] != 0) {
            return false;
        }
    }
    return true;
}

static void launcher_put_u32_le(uint8_t *out, uint32_t value) {
    out[0] = (uint8_t)value;
    out[1] = (uint8_t)(value >> 8);
    out[2] = (uint8_t)(value >> 16);
    out[3] = (uint8_t)(value >> 24);
}

static void launcher_put_u64_le(uint8_t *out, uint64_t value) {
    for (unsigned int index = 0; index < 8U; index++) {
        out[index] = (uint8_t)(value >> (index * 8U));
    }
}

static cbm_windows_release_descriptor_v1_t launcher_valid_descriptor(void) {
    cbm_windows_release_descriptor_v1_t descriptor;
    memset(&descriptor, 0, sizeof(descriptor));
    descriptor.launcher_abi = 2U;
    descriptor.payload_launcher_abi_min = 1U;
    descriptor.payload_launcher_abi_max = 3U;
    descriptor.payload_size = UINT64_C(0x0102030405060708);
    memcpy(descriptor.payload_sha256, launcher_sha256, sizeof(launcher_sha256));
    return descriptor;
}

TEST(windows_release_descriptor_v1_encoding_is_exact_and_round_trips) {
    cbm_windows_release_descriptor_v1_t descriptor = launcher_valid_descriptor();
    uint8_t actual[CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE];
    uint8_t expected[CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE];
    memset(expected, 0, sizeof(expected));
    memcpy(expected, "CBMWRD1\0", 8U);
    launcher_put_u32_le(expected + 8U, 1U);
    launcher_put_u32_le(expected + 12U, CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE);
    launcher_put_u32_le(expected + 16U, descriptor.launcher_abi);
    launcher_put_u32_le(expected + 20U, descriptor.payload_launcher_abi_min);
    launcher_put_u32_le(expected + 24U, descriptor.payload_launcher_abi_max);
    launcher_put_u64_le(expected + 32U, descriptor.payload_size);
    memcpy(expected + 40U, launcher_sha256, 64U);
    ASSERT_TRUE(cbm_windows_release_descriptor_v1_encode(&descriptor, actual));
    ASSERT_MEM_EQ(actual, expected, sizeof(actual));

    cbm_windows_release_descriptor_v1_t decoded;
    ASSERT_TRUE(cbm_windows_release_descriptor_v1_decode(actual, sizeof(actual), &decoded));
    ASSERT_EQ(decoded.launcher_abi, descriptor.launcher_abi);
    ASSERT_EQ(decoded.payload_launcher_abi_min, descriptor.payload_launcher_abi_min);
    ASSERT_EQ(decoded.payload_launcher_abi_max, descriptor.payload_launcher_abi_max);
    ASSERT_EQ(decoded.payload_size, descriptor.payload_size);
    ASSERT_STR_EQ(decoded.payload_sha256, descriptor.payload_sha256);
    PASS();
}

TEST(windows_release_descriptor_v1_rejects_noncanonical_or_incompatible) {
    cbm_windows_release_descriptor_v1_t descriptor = launcher_valid_descriptor();
    uint8_t record[CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE];
    ASSERT_TRUE(cbm_windows_release_descriptor_v1_encode(&descriptor, record));
    record[28] = 1U;
    ASSERT_FALSE(cbm_windows_release_descriptor_v1_decode(record, sizeof(record), &descriptor));
    record[28] = 0U;
    record[104] = 1U;
    ASSERT_FALSE(cbm_windows_release_descriptor_v1_decode(record, sizeof(record), &descriptor));

    descriptor = launcher_valid_descriptor();
    descriptor.launcher_abi = 4U;
    ASSERT_FALSE(cbm_windows_release_descriptor_v1_encode(&descriptor, record));
    descriptor = launcher_valid_descriptor();
    descriptor.payload_launcher_abi_min = 0U;
    ASSERT_FALSE(cbm_windows_release_descriptor_v1_encode(&descriptor, record));
    PASS();
}

TEST(windows_transition_plan_requires_a_crash_safe_compatibility_bridge) {
    cbm_windows_current_v1_t current = launcher_valid_state();
    cbm_windows_release_descriptor_v1_t candidate = launcher_valid_descriptor();

    current.launcher_abi_min = 1U;
    current.launcher_abi_max = 1U;
    candidate.launcher_abi = 1U;
    candidate.payload_launcher_abi_min = 1U;
    candidate.payload_launcher_abi_max = 1U;
    ASSERT_EQ(cbm_windows_transition_plan(&current, &candidate),
              CBM_WINDOWS_TRANSITION_LAUNCHER_FIRST);

    candidate.launcher_abi = 2U;
    candidate.payload_launcher_abi_min = 1U;
    candidate.payload_launcher_abi_max = 2U;
    ASSERT_EQ(cbm_windows_transition_plan(&current, &candidate),
              CBM_WINDOWS_TRANSITION_CURRENT_FIRST);

    current.launcher_abi_min = 1U;
    current.launcher_abi_max = 2U;
    candidate.payload_launcher_abi_min = 2U;
    candidate.payload_launcher_abi_max = 2U;
    ASSERT_EQ(cbm_windows_transition_plan(&current, &candidate),
              CBM_WINDOWS_TRANSITION_LAUNCHER_FIRST);

    current.launcher_abi_min = 1U;
    current.launcher_abi_max = 1U;
    ASSERT_EQ(cbm_windows_transition_plan(&current, &candidate),
              CBM_WINDOWS_TRANSITION_INCOMPATIBLE);

    current.launcher_abi_min = 1U;
    current.launcher_abi_max = 3U;
    candidate.launcher_abi = 4U;
    candidate.payload_launcher_abi_min = 2U;
    candidate.payload_launcher_abi_max = 4U;
    ASSERT_EQ(cbm_windows_transition_plan(&current, &candidate),
              CBM_WINDOWS_TRANSITION_INCOMPATIBLE);
    ASSERT_EQ(cbm_windows_transition_plan(NULL, &candidate), CBM_WINDOWS_TRANSITION_INCOMPATIBLE);
    PASS();
}

TEST(windows_current_v1_encoding_is_exact_fixed_little_endian_record) {
    cbm_windows_current_v1_t state = launcher_valid_state();
    state.launcher_abi_min = UINT32_C(0x01020304);
    state.launcher_abi_max = UINT32_C(0x05060708);

    uint8_t actual[CBM_WINDOWS_CURRENT_V1_SIZE];
    memset(actual, 0xa5, sizeof(actual));
    ASSERT_TRUE(cbm_windows_current_v1_encode(&state, actual));

    uint8_t expected[128] = {0};
    const uint8_t magic[8] = {'C', 'B', 'M', 'C', 'U', 'R', '1', '\0'};
    memcpy(expected, magic, sizeof(magic));
    launcher_put_u32_le(expected + 8, 1);
    launcher_put_u32_le(expected + 12, 128);
    launcher_put_u32_le(expected + 16, state.launcher_abi_min);
    launcher_put_u32_le(expected + 20, state.launcher_abi_max);
    expected[24] = 0x08;
    expected[25] = 0x07;
    expected[26] = 0x06;
    expected[27] = 0x05;
    expected[28] = 0x04;
    expected[29] = 0x03;
    expected[30] = 0x02;
    expected[31] = 0x01;
    memcpy(expected + 32, launcher_sha256, 64);

    ASSERT_EQ(CBM_WINDOWS_CURRENT_V1_SIZE, 128);
    ASSERT_MEM_EQ(actual, expected, sizeof(expected));
    ASSERT_TRUE(launcher_memory_is_zero(actual + 96, 32));
    PASS();
}

TEST(windows_current_v1_round_trip_preserves_authenticated_payload_fields) {
    cbm_windows_current_v1_t input = launcher_valid_state();
    uint8_t record[CBM_WINDOWS_CURRENT_V1_SIZE];
    ASSERT_TRUE(cbm_windows_current_v1_encode(&input, record));

    cbm_windows_current_v1_t output;
    memset(&output, 0xa5, sizeof(output));
    ASSERT_TRUE(cbm_windows_current_v1_decode(record, sizeof(record), &output));
    ASSERT_EQ(output.launcher_abi_min, input.launcher_abi_min);
    ASSERT_EQ(output.launcher_abi_max, input.launcher_abi_max);
    ASSERT_EQ(output.payload_size, input.payload_size);
    ASSERT_STR_EQ(output.payload_sha256, input.payload_sha256);
    ASSERT_EQ(output.payload_sha256[64], '\0');
    PASS();
}

TEST(windows_current_v1_launcher_abi_range_is_inclusive_and_fail_closed) {
    cbm_windows_current_v1_t state = launcher_valid_state();
    ASSERT_FALSE(cbm_windows_current_v1_supports_launcher_abi(&state, 0));
    ASSERT_TRUE(cbm_windows_current_v1_supports_launcher_abi(&state, 1));
    ASSERT_TRUE(cbm_windows_current_v1_supports_launcher_abi(&state, 2));
    ASSERT_TRUE(cbm_windows_current_v1_supports_launcher_abi(&state, 3));
    ASSERT_FALSE(cbm_windows_current_v1_supports_launcher_abi(&state, 4));
    ASSERT_FALSE(cbm_windows_current_v1_supports_launcher_abi(NULL, 2));
    PASS();
}

TEST(windows_current_v1_encoder_rejects_noncanonical_state) {
    cbm_windows_current_v1_t state = launcher_valid_state();
    uint8_t record[CBM_WINDOWS_CURRENT_V1_SIZE];

    ASSERT_FALSE(cbm_windows_current_v1_encode(NULL, record));
    ASSERT_FALSE(cbm_windows_current_v1_encode(&state, NULL));

    state.launcher_abi_min = 0;
    ASSERT_FALSE(cbm_windows_current_v1_encode(&state, record));
    state = launcher_valid_state();
    state.launcher_abi_min = 4;
    state.launcher_abi_max = 3;
    ASSERT_FALSE(cbm_windows_current_v1_encode(&state, record));
    state = launcher_valid_state();
    state.payload_size = 0;
    ASSERT_FALSE(cbm_windows_current_v1_encode(&state, record));
    state = launcher_valid_state();
    state.payload_sha256[0] = 'A';
    ASSERT_FALSE(cbm_windows_current_v1_encode(&state, record));
    state = launcher_valid_state();
    state.payload_sha256[17] = 'g';
    ASSERT_FALSE(cbm_windows_current_v1_encode(&state, record));
    state = launcher_valid_state();
    memset(state.payload_sha256, 'a', sizeof(state.payload_sha256));
    ASSERT_FALSE(cbm_windows_current_v1_encode(&state, record));
    PASS();
}

static int launcher_assert_decode_rejected(const uint8_t *record, size_t size) {
    cbm_windows_current_v1_t output;
    memset(&output, 0xa5, sizeof(output));
    if (cbm_windows_current_v1_decode(record, size, &output)) {
        return 1;
    }
    /* A rejected attacker-controlled record must not leave partial state that
     * a caller could accidentally consume. */
    return launcher_memory_is_zero(&output, sizeof(output)) ? 0 : 1;
}

TEST(windows_current_v1_decoder_rejects_every_noncanonical_field) {
    cbm_windows_current_v1_t state = launcher_valid_state();
    uint8_t valid[CBM_WINDOWS_CURRENT_V1_SIZE + 1];
    memset(valid, 0, sizeof(valid));
    ASSERT_TRUE(cbm_windows_current_v1_encode(&state, valid));

    ASSERT_EQ(launcher_assert_decode_rejected(valid, 127), 0);
    ASSERT_EQ(launcher_assert_decode_rejected(valid, 129), 0);
    ASSERT_FALSE(cbm_windows_current_v1_decode(NULL, 128, &state));
    ASSERT_FALSE(cbm_windows_current_v1_decode(valid, 128, NULL));

    uint8_t damaged[CBM_WINDOWS_CURRENT_V1_SIZE];
    memcpy(damaged, valid, sizeof(damaged));
    damaged[0] ^= 1;
    ASSERT_EQ(launcher_assert_decode_rejected(damaged, sizeof(damaged)), 0);

    memcpy(damaged, valid, sizeof(damaged));
    launcher_put_u32_le(damaged + 8, 2);
    ASSERT_EQ(launcher_assert_decode_rejected(damaged, sizeof(damaged)), 0);

    memcpy(damaged, valid, sizeof(damaged));
    launcher_put_u32_le(damaged + 12, 127);
    ASSERT_EQ(launcher_assert_decode_rejected(damaged, sizeof(damaged)), 0);

    memcpy(damaged, valid, sizeof(damaged));
    launcher_put_u32_le(damaged + 16, 0);
    ASSERT_EQ(launcher_assert_decode_rejected(damaged, sizeof(damaged)), 0);

    memcpy(damaged, valid, sizeof(damaged));
    launcher_put_u32_le(damaged + 16, 4);
    launcher_put_u32_le(damaged + 20, 3);
    ASSERT_EQ(launcher_assert_decode_rejected(damaged, sizeof(damaged)), 0);

    memcpy(damaged, valid, sizeof(damaged));
    memset(damaged + 24, 0, 8);
    ASSERT_EQ(launcher_assert_decode_rejected(damaged, sizeof(damaged)), 0);

    memcpy(damaged, valid, sizeof(damaged));
    damaged[32] = 'A';
    ASSERT_EQ(launcher_assert_decode_rejected(damaged, sizeof(damaged)), 0);

    memcpy(damaged, valid, sizeof(damaged));
    damaged[63] = 'g';
    ASSERT_EQ(launcher_assert_decode_rejected(damaged, sizeof(damaged)), 0);

    memcpy(damaged, valid, sizeof(damaged));
    damaged[96] = 1;
    ASSERT_EQ(launcher_assert_decode_rejected(damaged, sizeof(damaged)), 0);
    PASS();
}

TEST(windows_generation_path_is_launcher_relative_for_custom_unicode_directory) {
    const wchar_t launcher[] =
        L"D:\\CBM Custom\\M\u00e4rtin\\\u5de5\u5177\\codebase-memory-mcp.exe";
    const wchar_t expected[] = L"D:\\CBM Custom\\M\u00e4rtin\\\u5de5\u5177\\.cbm\\generations\\"
                               L"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\\"
                               L"codebase-memory-mcp.payload.exe";
    wchar_t actual[512];
    ASSERT_TRUE(cbm_windows_generation_payload_path(launcher, launcher_sha256, actual,
                                                    sizeof(actual) / sizeof(actual[0])));
    ASSERT_EQ(wcscmp(actual, expected), 0);

    const wchar_t root_launcher[] = L"C:\\codebase-memory-mcp.exe";
    const wchar_t root_expected[] =
        L"C:\\.cbm\\generations\\"
        L"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\\"
        L"codebase-memory-mcp.payload.exe";
    ASSERT_TRUE(cbm_windows_generation_payload_path(root_launcher, launcher_sha256, actual,
                                                    sizeof(actual) / sizeof(actual[0])));
    ASSERT_EQ(wcscmp(actual, root_expected), 0);
    PASS();
}

TEST(windows_generation_launcher_path_shares_the_payload_generation) {
    const wchar_t launcher[] =
        L"D:\\CBM Custom\\M\u00e4rtin\\\u5de5\u5177\\codebase-memory-mcp.exe";
    const wchar_t expected[] = L"D:\\CBM Custom\\M\u00e4rtin\\\u5de5\u5177\\.cbm\\generations\\"
                               L"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\\"
                               L"codebase-memory-mcp.exe";
    wchar_t actual[512];
    ASSERT_TRUE(cbm_windows_generation_launcher_path(launcher, launcher_sha256, actual,
                                                     sizeof(actual) / sizeof(actual[0])));
    ASSERT_EQ(wcscmp(actual, expected), 0);

    const wchar_t root_launcher[] = L"C:\\codebase-memory-mcp.exe";
    const wchar_t root_expected[] =
        L"C:\\.cbm\\generations\\"
        L"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\\"
        L"codebase-memory-mcp.exe";
    ASSERT_TRUE(cbm_windows_generation_launcher_path(root_launcher, launcher_sha256, actual,
                                                     sizeof(actual) / sizeof(actual[0])));
    ASSERT_EQ(wcscmp(actual, root_expected), 0);
    PASS();
}

TEST(windows_retired_state_path_is_unique_safe_and_launcher_relative) {
    const wchar_t launcher[] =
        L"D:\\CBM Custom\\M\u00e4rtin\\\u5de5\u5177\\codebase-memory-mcp.exe";
    const wchar_t expected[] =
        L"D:\\CBM Custom\\M\u00e4rtin\\\u5de5\u5177\\.cbm-retired-v1-"
        L"0123456789abcdef-4242";
    wchar_t actual[512];
    ASSERT_TRUE(cbm_windows_retired_state_path(launcher, launcher_sha256, 4242U, actual,
                                               sizeof(actual) / sizeof(actual[0])));
    ASSERT_EQ(wcscmp(actual, expected), 0);
    ASSERT_FALSE(cbm_windows_retired_state_path(launcher, launcher_sha256, 0U, actual,
                                                sizeof(actual) / sizeof(actual[0])));
    ASSERT_EQ(actual[0], L'\0');
    ASSERT_FALSE(cbm_windows_retired_state_path(L"codebase-memory-mcp.exe", launcher_sha256, 4242U,
                                                actual, sizeof(actual) / sizeof(actual[0])));
    ASSERT_EQ(actual[0], L'\0');
    ASSERT_FALSE(cbm_windows_retired_state_path(launcher, launcher_sha256, 4242U, actual, 32U));
    ASSERT_EQ(actual[0], L'\0');
    PASS();
}

TEST(windows_generation_path_rejects_ambiguous_or_truncated_inputs) {
    wchar_t output[512];
    memset(output, 0xa5, sizeof(output));
    ASSERT_FALSE(cbm_windows_generation_payload_path(L"codebase-memory-mcp.exe", launcher_sha256,
                                                     output, sizeof(output) / sizeof(output[0])));
    ASSERT_EQ(output[0], L'\0');

    ASSERT_FALSE(cbm_windows_generation_payload_path(L"C:\\codebase-memory-mcp.exe",
                                                     launcher_sha256, output, 12));
    ASSERT_EQ(output[0], L'\0');

    char uppercase[65];
    memcpy(uppercase, launcher_sha256, sizeof(uppercase));
    uppercase[0] = 'A';
    ASSERT_FALSE(cbm_windows_generation_payload_path(L"C:\\codebase-memory-mcp.exe", uppercase,
                                                     output, sizeof(output) / sizeof(output[0])));
    ASSERT_EQ(output[0], L'\0');

    ASSERT_FALSE(cbm_windows_generation_launcher_path(L"codebase-memory-mcp.exe", launcher_sha256,
                                                      output, sizeof(output) / sizeof(output[0])));
    ASSERT_EQ(output[0], L'\0');
    ASSERT_FALSE(cbm_windows_generation_launcher_path(L"C:\\codebase-memory-mcp.exe",
                                                      launcher_sha256, output, 12));
    ASSERT_EQ(output[0], L'\0');
    ASSERT_FALSE(cbm_windows_generation_launcher_path(L"C:\\codebase-memory-mcp.exe", uppercase,
                                                      output, sizeof(output) / sizeof(output[0])));
    ASSERT_EQ(output[0], L'\0');
    PASS();
}

TEST(windows_launcher_action_classifier_matches_top_level_dispatch) {
    const char *ordinary_mcp[] = {"codebase-memory-mcp.exe"};
    const char *ordinary_cli[] = {"codebase-memory-mcp.exe", "cli", "search_graph",
                                  "{\"query\":\"update\"}"};
    const char *ordinary_install[] = {"codebase-memory-mcp.exe", "install", "update"};
    const char *ordinary_help[] = {"codebase-memory-mcp.exe", "--help", "update"};
    const char *update[] = {"codebase-memory-mcp.exe", "--profile", "update", "--yes"};
    const char *uninstall[] = {"codebase-memory-mcp.exe", "ignored", "uninstall", "--yes"};
    const char *first_wins[] = {"codebase-memory-mcp.exe", "update", "uninstall"};

    ASSERT_EQ(cbm_windows_launcher_classify_action(1, ordinary_mcp),
              CBM_WINDOWS_LAUNCHER_ACTION_ORDINARY);
    ASSERT_EQ(cbm_windows_launcher_classify_action(4, ordinary_cli),
              CBM_WINDOWS_LAUNCHER_ACTION_ORDINARY);
    ASSERT_EQ(cbm_windows_launcher_classify_action(3, ordinary_install),
              CBM_WINDOWS_LAUNCHER_ACTION_ORDINARY);
    ASSERT_EQ(cbm_windows_launcher_classify_action(3, ordinary_help),
              CBM_WINDOWS_LAUNCHER_ACTION_ORDINARY);
    ASSERT_EQ(cbm_windows_launcher_classify_action(4, update), CBM_WINDOWS_LAUNCHER_ACTION_UPDATE);
    ASSERT_EQ(cbm_windows_launcher_classify_action(4, uninstall),
              CBM_WINDOWS_LAUNCHER_ACTION_UNINSTALL);
    ASSERT_EQ(cbm_windows_launcher_classify_action(3, first_wins),
              CBM_WINDOWS_LAUNCHER_ACTION_UPDATE);
    ASSERT_EQ(cbm_windows_launcher_classify_action(0, NULL), CBM_WINDOWS_LAUNCHER_ACTION_ORDINARY);
    PASS();
}

TEST(windows_portable_payload_rejects_only_managed_mutations) {
    ASSERT_TRUE(cbm_windows_launcher_action_allowed(CBM_WINDOWS_LAUNCHER_ACTION_ORDINARY, false));
    ASSERT_FALSE(cbm_windows_launcher_action_allowed(CBM_WINDOWS_LAUNCHER_ACTION_UPDATE, false));
    ASSERT_FALSE(cbm_windows_launcher_action_allowed(CBM_WINDOWS_LAUNCHER_ACTION_UNINSTALL, false));

    ASSERT_TRUE(cbm_windows_launcher_action_allowed(CBM_WINDOWS_LAUNCHER_ACTION_ORDINARY, true));
    ASSERT_TRUE(cbm_windows_launcher_action_allowed(CBM_WINDOWS_LAUNCHER_ACTION_UPDATE, true));
    ASSERT_TRUE(cbm_windows_launcher_action_allowed(CBM_WINDOWS_LAUNCHER_ACTION_UNINSTALL, true));
    ASSERT_FALSE(cbm_windows_launcher_action_allowed((cbm_windows_launcher_action_t)99, true));
    PASS();
}

SUITE(windows_launcher_state) {
    RUN_TEST(windows_release_descriptor_v1_encoding_is_exact_and_round_trips);
    RUN_TEST(windows_release_descriptor_v1_rejects_noncanonical_or_incompatible);
    RUN_TEST(windows_transition_plan_requires_a_crash_safe_compatibility_bridge);
    RUN_TEST(windows_current_v1_encoding_is_exact_fixed_little_endian_record);
    RUN_TEST(windows_current_v1_round_trip_preserves_authenticated_payload_fields);
    RUN_TEST(windows_current_v1_launcher_abi_range_is_inclusive_and_fail_closed);
    RUN_TEST(windows_current_v1_encoder_rejects_noncanonical_state);
    RUN_TEST(windows_current_v1_decoder_rejects_every_noncanonical_field);
    RUN_TEST(windows_generation_path_is_launcher_relative_for_custom_unicode_directory);
    RUN_TEST(windows_generation_launcher_path_shares_the_payload_generation);
    RUN_TEST(windows_retired_state_path_is_unique_safe_and_launcher_relative);
    RUN_TEST(windows_generation_path_rejects_ambiguous_or_truncated_inputs);
    RUN_TEST(windows_launcher_action_classifier_matches_top_level_dispatch);
    RUN_TEST(windows_portable_payload_rejects_only_managed_mutations);
}
