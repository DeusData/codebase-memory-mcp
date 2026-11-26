/* Independent format-2 coverage-origin contracts. Admission flags below are
 * synthetic unit-test evidence, never a real producer/provider verification. */
#include "test_framework.h"
#include "../src/mcp/test_impact_origins.h"
#include "../src/foundation/arena.h"
#include "../src/foundation/sha256.h"
#include "../src/foundation/compat_thread.h"
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static const char ov_functions[] =
    "0\tsrc/a.c\tshared\n1\tinc/common.h\tshared\n2\t\t__external_fallback\n"
    "3\tsrc/generated.c\tgenerated\n";
static const char ov_tests[] =
    "core:alpha\tcomplete\t\t0 2 3\ncore:empty\tcomplete\t\t\ncore:*\tcomplete\t\t1\n";
static const char ov_empty_tests[] = "core:empty\tcomplete\t\t\ncore:*\tcomplete\t\t\n";
static const char ov_lf_sha[] = "2e7f7a09b3da6912f8198faf3d6a8ec2de60d8cd632d9d282b05512463f1ef84";
static const char ov_crlf_sha[] = "32836d30f88a9e9ebdad43359f7bf86b245004e6411f6cc00e86d0df732ae46e";
static const char ov_wire_sha[] = "7a75653ffd6aaded0582dbf559ce346bfa7fa2266593ba90c0bd6b3eee7f027a";
static const char ov_wire_hex[] =
    "63626d2d636f7665726167652d6f726967696e73000000000200000009746573742d7265706f0111"
    "111111111111111111111111111111111111112e7f7a09b3da6912f8198faf3d6a8ec2de60d8cd63"
    "2d9d282b05512463f1ef840000000444444444444444444444444444444444444444444444444444"
    "44444444444444555555555555555555555555555555555555555555555555555555555555555501"
    "01666666666666666666666666666666666666666666666666666666666666666600000001000000"
    "077372632f612e6302000081a47777777777777777777777777777777777777777010f0188888888"
    "88888888888888888888888888888888888888888888888888888888000000020000000100000003"
    "7f";

typedef struct {
    const unsigned char *path;
    size_t path_length;
    unsigned state;
    uint32_t mode;
    unsigned disposition;
    unsigned edits;
    bool proof;
    const uint32_t *ids;
    size_t count;
} ov_row_t;

typedef struct {
    size_t path, state, mode, disposition, edits, proof, id_count, ids;
} ov_layout_t;

typedef struct {
    CBMArena arena;
    cbm_coverage_map_t *map;
    cbm_changes_t *changes;
    cbm_coverage_origin_context_t context;
    cbm_coverage_origin_input_t input;
    cbm_coverage_origin_limits_t limits;
    unsigned char *wire;
    size_t length, capacity;
    bool encoded, universe, universe_proof;
    size_t global_flag, global_proof, path_count;
    ov_layout_t rows[32];
    int function_count;
} ov_fixture_t;

static void ov_hash(const void *bytes, size_t length, unsigned char out[32]) {
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    if (length)
        cbm_sha256_update(&hash, bytes, length);
    cbm_sha256_final(&hash, out);
}

static void *ov_copy(ov_fixture_t *f, const void *bytes, size_t length) {
    unsigned char *copy = cbm_arena_alloc(&f->arena, length + 1);
    if (!copy)
        return NULL;
    if (length)
        memcpy(copy, bytes, length);
    copy[length] = 0;
    return copy;
}

static void ov_close(ov_fixture_t *f) {
    cbm_changes_free(f->changes);
    cbm_coverage_map_free(f->map);
    cbm_arena_destroy(&f->arena);
    memset(f, 0, sizeof(*f));
}

static bool ov_names(ov_fixture_t *f, const void *ah, size_t ah_length,
                     const void *mh, size_t mh_length, bool unknown_request) {
    unsigned char *a = ov_copy(f, ah, ah_length), *m = ov_copy(f, mh, mh_length);
    if (!a || !m)
        return false;
    cbm_changes_free(f->changes);
    f->changes = NULL;
    /* NULL+zero spans are a valid empty diff. An explicit incomplete patch
     * is needed for a real UNKNOWN D4a owner with an empty path inventory. */
    static const unsigned char incomplete_patch[] = {0};
    cbm_changes_status_t status = cbm_changes_parse(
        m, mh_length, unknown_request ? incomplete_patch : (const unsigned char *)"",
        unknown_request ? sizeof(incomplete_patch) : 0, &f->changes);
    if (status != CBM_CHANGES_OK || !f->changes)
        return false;
    if (unknown_request && !(cbm_changes_issues(f->changes) & CBM_CHANGES_PATCH_INCOMPLETE))
        return false;
    f->input.artifact_to_head = (cbm_coverage_origin_bytes_t){a, ah_length};
    f->input.merge_base_to_head = (cbm_coverage_origin_bytes_t){m, mh_length};
    f->input.request_changes = f->changes;
    ov_hash(a, ah_length, f->context.artifact_to_head_sha256);
    ov_hash(m, mh_length, f->context.merge_base_to_head_sha256);
    return cbm_changes_state(f->changes) ==
           (unknown_request ? CBM_CHANGES_UNKNOWN :
            mh_length ? CBM_CHANGES_NONEMPTY : CBM_CHANGES_EMPTY);
}

static bool ov_open(ov_fixture_t *f, const char *functions, size_t function_bytes,
                    const char *tests, size_t test_bytes) {
    memset(f, 0, sizeof(*f));
    cbm_arena_init_lazy(&f->arena, 4096);
    char *owned_functions = ov_copy(f, functions, function_bytes);
    char *owned_tests = ov_copy(f, tests, test_bytes);
    unsigned char *key = ov_copy(f, "test-repo", 9);
    if (!owned_functions || !owned_tests || !key)
        return false;
    f->map = cbm_coverage_map_parse(owned_functions, function_bytes, owned_tests, test_bytes);
    if (!f->map)
        return false;
    (void)cbm_coverage_map_functions(f->map, &f->function_count);
    f->context.artifact_admitted = true;
    f->context.origin_source_verified = true;
    f->context.origin_attestations_verified = true;
    f->context.comparisons_verified = true;
    f->context.binding.repository_key = (cbm_coverage_origin_bytes_t){key, 9};
    f->context.binding.object_format = CBM_COVERAGE_ORIGIN_GIT_SHA1;
    memset(f->context.binding.artifact.bytes, 0x11, 20);
    memset(f->context.binding.merge_base.bytes, 0x22, 20);
    memset(f->context.binding.head.bytes, 0x33, 20);
    ov_hash(functions, function_bytes, f->context.binding.functions_sha256);
    f->context.binding.manifest_version = 2;
    memset(f->context.binding.compatibility_sha256, 0x44, 32);
    memset(f->context.binding.producer_profile_sha256, 0x55, 32);
    f->input.coverage = f->map;
    f->input.context = &f->context;
    f->limits = (cbm_coverage_origin_limits_t){
        .max_input_bytes = 8U * 1024U * 1024U, .max_items = 10000000,
        .max_alloc_bytes = 32U * 1024U * 1024U, .max_result_ids = INT_MAX};
    f->universe = f->universe_proof = true;
    return ov_names(f, "", 0, "", 0, false);
}

static bool ov_default(ov_fixture_t *f) {
    return ov_open(f, ov_functions, sizeof(ov_functions) - 1, ov_tests, sizeof(ov_tests) - 1);
}

static ov_row_t ov_row(const char *path, unsigned state, unsigned disposition,
                       unsigned edits, const uint32_t *ids, size_t count) {
    return (ov_row_t){(const unsigned char *)path, strlen(path), state, 0100644,
                      disposition, edits, true, ids, count};
}

static void ov_put(ov_fixture_t *f, const void *data, size_t n) {
    if (!f->encoded || n > f->capacity - f->length) {
        f->encoded = false;
        return;
    }
    if (n)
        memcpy(f->wire + f->length, data, n);
    f->length += n;
}

static void ov_u8(ov_fixture_t *f, unsigned n) {
    unsigned char byte = (unsigned char)n;
    ov_put(f, &byte, 1);
}

static void ov_u32(ov_fixture_t *f, uint32_t n) {
    unsigned char bytes[] = {(unsigned char)(n >> 24), (unsigned char)(n >> 16),
                             (unsigned char)(n >> 8), (unsigned char)n};
    ov_put(f, bytes, sizeof(bytes));
}

static void ov_proof(ov_fixture_t *f, bool present, unsigned char marker) {
    ov_u8(f, present ? 1 : 0);
    if (present) {
        unsigned char bytes[32];
        memset(bytes, marker, sizeof(bytes));
        ov_put(f, bytes, sizeof(bytes));
    }
}

static void ov_rehash(ov_fixture_t *f) {
    f->input.manifest = (cbm_coverage_origin_bytes_t){f->wire, f->length};
    ov_hash(f->wire, f->length, f->context.binding.manifest_sha256);
}

/* Deliberately simple field writer, independent of the production parser. */
static bool ov_wire(ov_fixture_t *f, const ov_row_t *rows, size_t count) {
    if (count > 32)
        return false;
    f->capacity = 512;
    for (size_t i = 0; i < count; i++)
        f->capacity += rows[i].path_length + 128 + rows[i].count * 4;
    f->wire = cbm_arena_alloc(&f->arena, f->capacity);
    f->length = 0;
    f->encoded = f->wire != NULL;
    memset(f->rows, 0, sizeof(f->rows));
    ov_put(f, "cbm-coverage-origins", sizeof("cbm-coverage-origins"));
    ov_u32(f, 2);
    ov_u32(f, (uint32_t)f->context.binding.repository_key.length);
    ov_put(f, f->context.binding.repository_key.data, f->context.binding.repository_key.length);
    ov_u8(f, f->context.binding.object_format);
    size_t width = f->context.binding.object_format == CBM_COVERAGE_ORIGIN_GIT_SHA1 ? 20 : 32;
    ov_put(f, f->context.binding.artifact.bytes, width);
    ov_put(f, f->context.binding.functions_sha256, 32);
    ov_u32(f, (uint32_t)f->function_count);
    ov_put(f, f->context.binding.compatibility_sha256, 32);
    ov_put(f, f->context.binding.producer_profile_sha256, 32);
    f->global_flag = f->length;
    ov_u8(f, f->universe ? 1 : 0);
    f->global_proof = f->length;
    ov_proof(f, f->universe_proof, 0x66);
    f->path_count = f->length;
    ov_u32(f, (uint32_t)count);
    for (size_t i = 0; i < count; i++) {
        const ov_row_t *r = &rows[i];
        ov_layout_t *l = &f->rows[i];
        ov_u32(f, (uint32_t)r->path_length);
        l->path = f->length;
        ov_put(f, r->path, r->path_length);
        l->state = f->length;
        ov_u8(f, r->state);
        if (r->state == 2 || r->state == 3) {
            l->mode = f->length;
            ov_u32(f, r->mode);
            unsigned char oid[32];
            memset(oid, 0x77, sizeof(oid));
            ov_put(f, oid, width);
        }
        l->disposition = f->length;
        ov_u8(f, r->disposition);
        l->edits = f->length;
        ov_u8(f, r->edits);
        l->proof = f->length;
        ov_proof(f, r->proof, 0x88);
        if (r->disposition != CBM_COVERAGE_ORIGIN_ALL) {
            l->id_count = f->length;
            ov_u32(f, (uint32_t)r->count);
            l->ids = f->length;
            for (size_t j = 0; j < r->count; j++)
                ov_u32(f, r->ids[j]);
        }
    }
    ov_u8(f, 0x7f);
    if (!f->encoded)
        return false;
    ov_rehash(f);
    return true;
}

static bool ov_ids_equal(const int *got, size_t count, const int *want, size_t wanted) {
    if (count != wanted || (count && !got))
        return false;
    for (size_t i = 0; i < count; i++)
        if (got[i] != want[i])
            return false;
    return true;
}

static bool ov_path_equal(const cbm_coverage_origin_path_t *path,
                          const void *bytes, size_t length) {
    return path && path->path.data && path->path.length == length &&
           memcmp(path->path.data, bytes, length) == 0 && path->path.data[length] == 0;
}

static bool ov_success(ov_fixture_t *f, unsigned reasons, bool broad,
                       const int *ids, size_t count) {
    cbm_coverage_origin_join_t *join = NULL;
    cbm_coverage_origin_status_t status = cbm_coverage_origin_join(
        &f->input, &f->limits, NULL, NULL, &join);
    int got_count = -1;
    const int *got = cbm_coverage_origin_join_ids(join, &got_count);
    bool complete = cbm_coverage_origin_join_complete(join);
    bool actual_broad = cbm_coverage_origin_join_broad_fallback_required(join);
    bool ok = status == CBM_COVERAGE_ORIGIN_OK && join && got_count >= 0 &&
              ov_ids_equal(got, (size_t)got_count, ids, count) &&
              cbm_coverage_origin_join_reasons(join) == reasons && complete == (reasons == 0) &&
              actual_broad == broad &&
              cbm_coverage_origin_join_can_narrow(join) == (complete && !actual_broad) &&
              cbm_coverage_origin_join_request_state(join) == cbm_changes_state(f->changes);
    cbm_coverage_origin_join_free(join);
    return ok;
}

static bool ov_error(ov_fixture_t *f, cbm_coverage_origin_status_t expected) {
    cbm_coverage_origin_join_t *join = NULL;
    cbm_coverage_origin_status_t status = cbm_coverage_origin_join(
        &f->input, &f->limits, NULL, NULL, &join);
    bool ok = status == expected && join == NULL;
    cbm_coverage_origin_join_free(join);
    return ok;
}

static bool ov_hex_equal(const unsigned char *bytes, size_t count, const char *hex) {
    static const char digits[] = "0123456789abcdef";
    if (strlen(hex) != count * 2)
        return false;
    for (size_t i = 0; i < count; i++)
        if (hex[i * 2] != digits[bytes[i] >> 4] || hex[i * 2 + 1] != digits[bytes[i] & 15])
            return false;
    return true;
}

TEST(origin_v2_wire_and_actual_map_digest) {
    ov_fixture_t f;
    bool ready = ov_default(&f);
    static const unsigned char names[] = "M\0src/a.c\0";
    static const uint32_t listed[] = {1, 3};
    static const int wanted[] = {1, 3};
    ov_row_t row = ov_row("src/a.c", 2, CBM_COVERAGE_ORIGIN_COMPLETE_IDS, 15, listed, 2);
    bool control = ready && f.function_count == 4 &&
                   ov_names(&f, names, sizeof(names) - 1, names, sizeof(names) - 1, false) &&
                   ov_wire(&f, &row, 1);
    bool golden = control && f.length == 281 && ov_hex_equal(f.wire, f.length, ov_wire_hex);
    char digest[65];
    if (control)
        cbm_sha256_hex(f.wire, f.length, digest);
    golden = golden && strcmp(digest, ov_wire_sha) == 0;
    const char *actual = ready ? cbm_coverage_map_functions_sha256(f.map) : NULL;
    bool getter = actual && strcmp(actual, ov_lf_sha) == 0;
    bool ok = control && ov_success(&f, 0, false, wanted, 2);
    if (control) {
        f.context.binding.object_format = CBM_COVERAGE_ORIGIN_GIT_SHA256;
        memset(f.context.binding.artifact.bytes, 0x11, 32);
        memset(f.context.binding.merge_base.bytes, 0x22, 32);
        memset(f.context.binding.head.bytes, 0x33, 32);
        ok = ov_wire(&f, &row, 1) && ov_success(&f, 0, false, wanted, 2) && ok;
        char crlf[sizeof(ov_functions) * 2];
        size_t at = 0;
        for (size_t i = 0; i < sizeof(ov_functions) - 1; i++) {
            if (ov_functions[i] == '\n')
                crlf[at++] = '\r';
            crlf[at++] = ov_functions[i];
        }
        cbm_coverage_map_t *other = cbm_coverage_map_parse(crlf, at, ov_tests, sizeof(ov_tests) - 1);
        const char *other_sha = cbm_coverage_map_functions_sha256(other);
        getter = other && other_sha && strcmp(other_sha, ov_crlf_sha) == 0 && getter;
        f.input.coverage = other;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_BINDING) && ok;
        f.input.coverage = f.map;
        cbm_coverage_map_free(other);
    }
    ov_close(&f);
    ASSERT_TRUE(control);
    ASSERT_TRUE(golden);
    ASSERT_TRUE(getter);
    ASSERT_TRUE(ok);
    PASS();
}

TEST(origin_v2_union_and_request_state) {
    ov_fixture_t f;
    bool ready = ov_default(&f);
    static const unsigned char ah[] = "M\0new.c\0D\0old.c\0";
    static const unsigned char mh[] = "M\0reverted.c\0M\0new.c\0";
    static const uint32_t a[] = {0}, b[] = {1}, c[] = {2};
    static const int wanted[] = {0, 1, 2};
    ov_row_t rows[] = {ov_row("new.c", 2, 1, 15, a, 1), ov_row("old.c", 2, 1, 15, b, 1),
                       ov_row("reverted.c", 2, 1, 15, c, 1)};
    bool control = ready && ov_names(&f, ah, sizeof(ah) - 1, mh, sizeof(mh) - 1, false) &&
                   ov_wire(&f, rows, 3);
    bool ok = control && ov_success(&f, 0, false, wanted, 3);
    cbm_coverage_origin_join_t *join = NULL;
    if (control) {
        cbm_coverage_origin_status_t status = cbm_coverage_origin_join(
            &f.input, &f.limits, NULL, NULL, &join);
        size_t count = 0;
        const cbm_coverage_origin_path_t *paths = cbm_coverage_origin_join_paths(join, &count);
        ok = status == CBM_COVERAGE_ORIGIN_OK && count == 3 && paths &&
             ov_path_equal(&paths[0], "new.c", 5) && paths[0].comparisons == 3 &&
             paths[0].artifact_status == 'M' && paths[0].merge_base_status == 'M' &&
             ov_path_equal(&paths[1], "old.c", 5) && paths[1].comparisons == 1 &&
             paths[1].artifact_status == 'D' && paths[1].merge_base_status == 0 &&
             ov_path_equal(&paths[2], "reverted.c", 10) && paths[2].comparisons == 2 &&
             paths[2].artifact_status == 0 && paths[2].merge_base_status == 'M' && ok;
        cbm_coverage_origin_join_free(join);
        static const unsigned char reverted[] = "M\0reverted.c\0";
        ok = ov_names(&f, "", 0, reverted, sizeof(reverted) - 1, false) &&
             ov_wire(&f, rows, 3) && ov_success(&f, 0, false, (const int[]){2}, 1) && ok;
        rows[2].state = 1; /* AH empty proves same presence at A and H. */
        ok = ov_wire(&f, rows, 3) && ov_error(&f, CBM_COVERAGE_ORIGIN_BINDING) && ok;
        rows[2].state = 2;
        ok = ov_wire(&f, rows, 3) && ov_success(&f, 0, false, (const int[]){2}, 1) && ok;
        f.context.binding.merge_base = f.context.binding.head;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_BINDING) && ok;
        memset(f.context.binding.merge_base.bytes, 0x22, 20);
        f.context.binding.artifact = f.context.binding.merge_base;
        ok = ov_wire(&f, rows, 3) && ov_error(&f, CBM_COVERAGE_ORIGIN_BINDING) && ok;
        f.context.binding.artifact = f.context.binding.head;
        ok = ov_wire(&f, rows, 3) && ov_error(&f, CBM_COVERAGE_ORIGIN_BINDING) && ok;
        memset(f.context.binding.artifact.bytes, 0x11, 20);
        ok = ov_names(&f, "", 0, "", 0, true) && ov_wire(&f, rows, 3) &&
             ov_success(&f, CBM_COVERAGE_ORIGIN_REQUEST_UNKNOWN, false, NULL, 0) && ok;
        static const unsigned char one[] = "M\0new.c\0", gone[] = "D\0new.c\0";
        ok = ov_names(&f, gone, sizeof(gone) - 1, one, sizeof(one) - 1, false) &&
             ov_wire(&f, rows, 3) && ov_error(&f, CBM_COVERAGE_ORIGIN_BINDING) && ok;
        ok = ov_names(&f, one, sizeof(one) - 1, one, sizeof(one) - 1, false) &&
             ov_wire(&f, rows, 3) && ov_success(&f, 0, false, (const int[]){0}, 1) && ok;
        /* A separately valid D4a owner with a different inventory cannot bind. */
        cbm_changes_t *wrong = NULL;
        bool wrong_ready = cbm_changes_parse(reverted, sizeof(reverted) - 1,
            (const unsigned char *)"", 0, &wrong) == CBM_CHANGES_OK && wrong;
        f.input.request_changes = wrong;
        ok = wrong_ready && ov_error(&f, CBM_COVERAGE_ORIGIN_BINDING) && ok;
        f.input.request_changes = f.changes;
        cbm_changes_free(wrong);
        /* Matching equal-commit cases remain valid, not a blanket refusal. */
        f.context.binding.artifact = f.context.binding.merge_base;
        ok = ov_wire(&f, rows, 3) && ov_success(&f, 0, false, (const int[]){0}, 1) && ok;
        memset(f.context.binding.artifact.bytes, 0x11, 20);
        f.context.binding.merge_base = f.context.binding.head;
        ok = ov_names(&f, one, sizeof(one) - 1, "", 0, false) &&
             ov_wire(&f, rows, 3) && ov_success(&f, 0, false, (const int[]){0}, 1) && ok;
        f.context.binding.artifact = f.context.binding.head;
        ok = ov_names(&f, "", 0, "", 0, false) && ov_wire(&f, rows, 3) &&
             ov_success(&f, 0, false, NULL, 0) && ok;
    }
    ov_close(&f);
    ASSERT_TRUE(control);
    ASSERT_TRUE(ok);
    PASS();
}

TEST(origin_v2_many_to_many_and_unknown_ids) {
    ov_fixture_t f;
    bool ready = ov_default(&f);
    static const unsigned char names[] = "M\0macro.h\0M\0generated.c\0M\0inline.h\0";
    static const uint32_t ids_a[] = {0, 2}, ids_b[] = {1, 2}, ids_c[] = {2, 3};
    static const int wanted[] = {0, 1, 2, 3};
    ov_row_t rows[] = {ov_row("generated.c", 2, 1, 15, ids_a, 2),
                       ov_row("inline.h", 2, 1, 15, ids_b, 2),
                       ov_row("macro.h", 2, 1, 15, ids_c, 2)};
    bool control = ready && ov_names(&f, names, sizeof(names) - 1, names, sizeof(names) - 1, false) &&
                   ov_wire(&f, rows, 3);
    bool ok = control && ov_success(&f, 0, false, wanted, 4);
    if (control) {
        rows[1].disposition = CBM_COVERAGE_ORIGIN_UNKNOWN;
        rows[1].proof = false;
        ok = ov_wire(&f, rows, 3) && ov_success(&f, CBM_COVERAGE_ORIGIN_CLAIM_UNKNOWN,
                                               false, wanted, 4) && ok;
        cbm_coverage_origin_join_t *join = NULL;
        cbm_coverage_origin_status_t status = cbm_coverage_origin_join(
            &f.input, &f.limits, NULL, NULL, &join);
        size_t count = 0;
        const cbm_coverage_origin_path_t *paths = cbm_coverage_origin_join_paths(join, &count);
        ok = status == CBM_COVERAGE_ORIGIN_OK && count == 3 && paths &&
             ov_ids_equal(paths[1].function_ids, paths[1].function_count,
                          (const int[]){1, 2}, 2) &&
             paths[1].disposition == CBM_COVERAGE_ORIGIN_UNKNOWN &&
             paths[1].reasons == CBM_COVERAGE_ORIGIN_CLAIM_UNKNOWN && ok;
        cbm_coverage_origin_join_free(join);
    }
    ov_close(&f);
    ASSERT_TRUE(control);
    ASSERT_TRUE(ok);
    PASS();
}

TEST(origin_v2_edit_class_obligations) {
    ov_fixture_t f;
    bool ready = ov_default(&f), ok = ready;
    static const uint32_t id[] = {1};
    static const int wanted[] = {1};
    const char statuses[] = {'A', 'M', 'D', 'T'};
    const unsigned bits[] = {1, 2, 4, 8};
    bool control = ready;
    for (size_t i = 0; ready && i < 4; i++) {
        unsigned char names[] = {'M', 0, 'f', 0};
        names[0] = (unsigned char)statuses[i];
        ov_row_t row = ov_row("f", statuses[i] == 'A' ? 1 : 2, 1, bits[i], id, 1);
        bool setup = ov_names(&f, names, sizeof(names), names, sizeof(names), false) &&
                     ov_wire(&f, &row, 1);
        control = setup && control;
        ok = setup && ov_success(&f, 0, false, wanted, 1) && ok;
        row.edits = 0;
        ok = ov_wire(&f, &row, 1) &&
             ov_success(&f, CBM_COVERAGE_ORIGIN_EDIT_UNSUPPORTED, false, wanted, 1) && ok;
    }
    if (ready) {
        static const unsigned char ah[] = "A\0f\0", mh[] = "M\0f\0";
        ov_row_t row = ov_row("f", 1, 1, CBM_COVERAGE_ORIGIN_EDIT_A, id, 1);
        ok = ov_names(&f, ah, sizeof(ah) - 1, mh, sizeof(mh) - 1, false) &&
             ov_wire(&f, &row, 1) &&
             ov_success(&f, CBM_COVERAGE_ORIGIN_EDIT_UNSUPPORTED, false, wanted, 1) && ok;
        row.edits = CBM_COVERAGE_ORIGIN_EDIT_A | CBM_COVERAGE_ORIGIN_EDIT_M;
        ok = ov_wire(&f, &row, 1) && ov_success(&f, 0, false, wanted, 1) && ok;
        /* M covers binary/mode/symlink changes without consulting hunk evidence. */
        static const unsigned char modified[] = "M\0f\0";
        row.state = 2;
        row.mode = 0120000;
        row.edits = CBM_COVERAGE_ORIGIN_EDIT_M;
        ok = ov_names(&f, modified, sizeof(modified) - 1, modified, sizeof(modified) - 1, false) &&
             ov_wire(&f, &row, 1) && ov_success(&f, 0, false, wanted, 1) &&
             !cbm_changes_can_narrow(f.changes) && ok;
        row.mode = 0100644;
        ok = ov_wire(&f, &row, 1) && ov_success(&f, 0, false, wanted, 1) && ok;
        const char *patches[] = {
            "diff --git a/f b/f\nold mode 100644\nnew mode 100755\n",
            "diff --git a/f b/f\nBinary files a/f and b/f differ\n"};
        const unsigned evidence[] = {CBM_CHANGE_MODE, CBM_CHANGE_BINARY};
        for (size_t i = 0; i < 2; i++) {
            cbm_changes_t *with_patch = NULL;
            bool parsed = cbm_changes_parse(modified, sizeof(modified) - 1,
                (const unsigned char *)patches[i], strlen(patches[i]), &with_patch) ==
                CBM_CHANGES_OK && with_patch;
            size_t count = 0;
            const cbm_change_file_t *files = cbm_changes_files(with_patch, &count);
            bool patch_control = parsed && count == 1 && files &&
                (files[0].reasons & evidence[i]) != 0 && !cbm_changes_can_narrow(with_patch);
            f.input.request_changes = with_patch;
            ok = patch_control && ov_success(&f, 0, false, wanted, 1) && ok;
            f.input.request_changes = f.changes;
            cbm_changes_free(with_patch);
        }
        static const unsigned char type_change[] = "T\0f\0";
        row.state = 3;
        row.mode = 0160000;
        row.edits = CBM_COVERAGE_ORIGIN_EDIT_T;
        ok = ov_names(&f, type_change, sizeof(type_change) - 1,
                      type_change, sizeof(type_change) - 1, false) &&
             ov_wire(&f, &row, 1) && ov_success(&f, 0, false, wanted, 1) && ok;
    }
    ov_close(&f);
    ASSERT_TRUE(control);
    ASSERT_TRUE(ok);
    PASS();
}

TEST(origin_v2_optional_header_and_certified_zero) {
    ov_fixture_t f;
    bool ready = ov_default(&f);
    static const unsigned char names[] = "A\0optional.h\0";
    ov_row_t row = ov_row("optional.h", 1, 1, CBM_COVERAGE_ORIGIN_EDIT_A, NULL, 0);
    bool control = ready && ov_names(&f, names, sizeof(names) - 1, names, sizeof(names) - 1, false) &&
                   ov_wire(&f, &row, 1);
    bool ok = control && ov_success(&f, 0, false, NULL, 0);
    if (control) {
        ok = ov_wire(&f, NULL, 0) &&
             ov_success(&f, CBM_COVERAGE_ORIGIN_PATH_MISSING, false, NULL, 0) && ok;
        row.disposition = CBM_COVERAGE_ORIGIN_UNKNOWN;
        ok = ov_wire(&f, &row, 1) &&
             ov_success(&f, CBM_COVERAGE_ORIGIN_CLAIM_UNKNOWN, false, NULL, 0) && ok;
        row.disposition = CBM_COVERAGE_ORIGIN_COMPLETE_IDS;
        row.proof = false;
        ok = ov_wire(&f, &row, 1) &&
             ov_success(&f, CBM_COVERAGE_ORIGIN_CLAIM_UNPROVEN, false, NULL, 0) && ok;
        row.proof = true;
        row.state = 0;
        ok = ov_wire(&f, &row, 1) &&
             ov_success(&f, CBM_COVERAGE_ORIGIN_PRESENCE_UNKNOWN, false, NULL, 0) && ok;
        row.state = 2; /* AH=A contradicts tracked presence at A. */
        ok = ov_wire(&f, &row, 1) && ov_error(&f, CBM_COVERAGE_ORIGIN_BINDING) && ok;
        row.state = 1;
        f.universe_proof = false;
        ok = ov_wire(&f, &row, 1) &&
             ov_success(&f, CBM_COVERAGE_ORIGIN_PROFILE_UNIVERSE_UNKNOWN, false, NULL, 0) && ok;
        f.universe_proof = true;
        ok = ov_wire(&f, &row, 1) && ov_success(&f, 0, false, NULL, 0) &&
             cbm_changes_state(f.changes) == CBM_CHANGES_NONEMPTY && ok;
    }
    ov_close(&f);
    ASSERT_TRUE(control);
    ASSERT_TRUE(ok);
    PASS();
}

TEST(origin_v2_all_sticky_broad_floor) {
    ov_fixture_t f;
    bool ready = ov_default(&f);
    static const unsigned char ah[] = "M\0all.c\0M\0last.c\0";
    static const uint32_t one[] = {1}, all_ids[] = {0, 1, 2, 3};
    static const int wanted[] = {0, 1, 2, 3};
    ov_row_t rows[] = {ov_row("all.c", 2, CBM_COVERAGE_ORIGIN_ALL, 15, NULL, 0),
                       ov_row("last.c", 2, CBM_COVERAGE_ORIGIN_COMPLETE_IDS, 15, one, 1)};
    bool control = ready && ov_names(&f, ah, sizeof(ah) - 1, "", 0, false) && ov_wire(&f, rows, 2);
    const cbm_coverage_test_t *empty = ready ? cbm_coverage_map_find_test(f.map, "core", "empty") : NULL;
    bool empty_control = empty && empty->complete && empty->function_count == 0 &&
                         !cbm_coverage_test_intersects(empty, wanted, 4);
    bool ok = control && ov_success(&f, 0, true, wanted, 4) &&
              cbm_changes_state(f.changes) == CBM_CHANGES_EMPTY;
    if (control) {
        rows[0].proof = false;
        rows[0].state = 0;
        rows[0].edits = 0;
        ok = ov_wire(&f, rows, 2) && ov_success(&f,
             CBM_COVERAGE_ORIGIN_CLAIM_UNPROVEN | CBM_COVERAGE_ORIGIN_PRESENCE_UNKNOWN |
             CBM_COVERAGE_ORIGIN_EDIT_UNSUPPORTED, true, wanted, 4) && ok;
        rows[0].proof = true;
        rows[0].state = 2;
        rows[0].edits = 15;
        static const unsigned char last[] = "M\0last.c\0";
        ok = ov_names(&f, last, sizeof(last) - 1, last, sizeof(last) - 1, false) &&
             ov_wire(&f, rows, 2) && ov_success(&f, 0, false, (const int[]){1}, 1) && ok;
        rows[1].disposition = CBM_COVERAGE_ORIGIN_ALL;
        ok = ov_names(&f, ah, sizeof(ah) - 1, ah, sizeof(ah) - 1, false) &&
             ov_wire(&f, rows, 2) && ov_success(&f, 0, true, wanted, 4) && ok;
        rows[0].disposition = rows[1].disposition = CBM_COVERAGE_ORIGIN_COMPLETE_IDS;
        rows[0].ids = rows[1].ids = all_ids;
        rows[0].count = rows[1].count = 4;
        ok = ov_wire(&f, rows, 2) && ov_success(&f, 0, false, wanted, 4) && ok;
    }
    ov_close(&f);
    ov_fixture_t zero;
    bool zero_ready = ov_open(&zero, "", 0, ov_empty_tests, sizeof(ov_empty_tests) - 1);
    static const unsigned char add[] = "A\0optional.h\0";
    ov_row_t all = ov_row("optional.h", 1, CBM_COVERAGE_ORIGIN_ALL, 15, NULL, 0);
    bool zero_control = zero_ready && zero.function_count == 0 &&
                        ov_names(&zero, add, sizeof(add) - 1, add, sizeof(add) - 1, false) &&
                        ov_wire(&zero, &all, 1);
    bool zero_ok = zero_control && ov_success(&zero, 0, true, NULL, 0);
    if (zero_control) {
        zero.universe_proof = false;
        zero_ok = ov_wire(&zero, &all, 1) && ov_success(&zero,
            CBM_COVERAGE_ORIGIN_PROFILE_UNIVERSE_UNKNOWN, true, NULL, 0) && zero_ok;
        zero_ok = ov_names(&zero, "", 0, "", 0, false) && ov_wire(&zero, NULL, 0) &&
            ov_success(&zero, CBM_COVERAGE_ORIGIN_PROFILE_UNIVERSE_UNKNOWN,
                       false, NULL, 0) && zero_ok;
        zero.universe_proof = true;
        zero_ok = ov_wire(&zero, NULL, 0) && ov_success(&zero, 0, false, NULL, 0) && zero_ok;
    }
    ov_close(&zero);
    ASSERT_TRUE(control);
    ASSERT_TRUE(empty_control);
    ASSERT_TRUE(zero_control);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(zero_ok);
    PASS();
}

TEST(origin_v2_exact_reason_masks) {
    ov_fixture_t f;
    bool ready = ov_default(&f);
    static const unsigned char names[] = "M\0f\0";
    static const uint32_t id[] = {2};
    static const int wanted[] = {2};
    ov_row_t base = ov_row("f", 2, 1, 15, id, 1);
    bool control = ready && ov_names(&f, names, sizeof(names) - 1, names, sizeof(names) - 1, false) &&
                   ov_wire(&f, &base, 1);
    bool ok = control && ov_success(&f, 0, false, wanted, 1);
    for (int variant = 0; control && variant < 6; variant++) {
        ov_row_t row = base;
        unsigned expected = 0;
        f.universe = f.universe_proof = true;
        if (variant == 0) { f.universe = false; expected = CBM_COVERAGE_ORIGIN_PROFILE_UNIVERSE_UNKNOWN; }
        if (variant == 1) { f.universe_proof = false; expected = CBM_COVERAGE_ORIGIN_PROFILE_UNIVERSE_UNKNOWN; }
        if (variant == 2) { row.state = 0; expected = CBM_COVERAGE_ORIGIN_PRESENCE_UNKNOWN; }
        if (variant == 3) { row.disposition = 0; row.proof = false; expected = CBM_COVERAGE_ORIGIN_CLAIM_UNKNOWN; }
        if (variant == 4) { row.proof = false; expected = CBM_COVERAGE_ORIGIN_CLAIM_UNPROVEN; }
        if (variant == 5) { row.edits = 0; expected = CBM_COVERAGE_ORIGIN_EDIT_UNSUPPORTED; }
        ok = ov_wire(&f, &row, 1) && ov_success(&f, expected, false, wanted, 1) && ok;
    }
    if (control) {
        f.universe = f.universe_proof = true;
        ok = ov_wire(&f, NULL, 0) && ov_success(&f, CBM_COVERAGE_ORIGIN_PATH_MISSING,
                                               false, NULL, 0) && ok;
        cbm_coverage_origin_join_t *join = NULL;
        cbm_coverage_origin_status_t status = cbm_coverage_origin_join(
            &f.input, &f.limits, NULL, NULL, &join);
        size_t n = 0;
        const cbm_coverage_origin_path_t *p = cbm_coverage_origin_join_paths(join, &n);
        ok = status == CBM_COVERAGE_ORIGIN_OK && p && n == 1 &&
             p[0].reasons == CBM_COVERAGE_ORIGIN_PATH_MISSING &&
             p[0].disposition == CBM_COVERAGE_ORIGIN_UNKNOWN && p[0].supported_edits == 0 && ok;
        cbm_coverage_origin_join_free(join);
        ov_row_t rows[] = {base, ov_row("z-unqueried", 0, 0, 0, id, 1)};
        rows[1].proof = false;
        ok = ov_wire(&f, rows, 2) && ov_success(&f, 0, false, wanted, 1) && ok;
        rows[0].state = 0;
        rows[0].disposition = CBM_COVERAGE_ORIGIN_UNKNOWN;
        rows[0].edits = 0;
        rows[0].proof = false;
        f.universe = false;
        unsigned combined = CBM_COVERAGE_ORIGIN_PROFILE_UNIVERSE_UNKNOWN |
            CBM_COVERAGE_ORIGIN_PRESENCE_UNKNOWN | CBM_COVERAGE_ORIGIN_CLAIM_UNKNOWN |
            CBM_COVERAGE_ORIGIN_EDIT_UNSUPPORTED | CBM_COVERAGE_ORIGIN_REQUEST_UNKNOWN;
        ok = ov_names(&f, names, sizeof(names) - 1, "", 0, true) && ov_wire(&f, rows, 2) &&
             ov_success(&f, combined, false, wanted, 1) && ok;
        ok = ov_names(&f, "", 0, "", 0, false) && ov_wire(&f, rows, 2) &&
             ov_success(&f, CBM_COVERAGE_ORIGIN_PROFILE_UNIVERSE_UNKNOWN, false, NULL, 0) && ok;
    }
    ov_close(&f);
    ASSERT_TRUE(control);
    ASSERT_TRUE(ok);
    PASS();
}

static bool ov_records(ov_fixture_t *f, const ov_row_t *rows, size_t count,
                        const unsigned char **out, size_t *length) {
    size_t n = 0;
    for (size_t i = 0; i < count; i++)
        n += rows[i].path_length + 3;
    unsigned char *bytes = cbm_arena_alloc(&f->arena, n + 1);
    if (!bytes)
        return false;
    size_t at = 0;
    for (size_t i = count; i > 0; i--) {
        const ov_row_t *r = &rows[i - 1];
        bytes[at++] = 'M'; bytes[at++] = 0;
        memcpy(bytes + at, r->path, r->path_length);
        at += r->path_length;
        bytes[at++] = 0;
    }
    bytes[at] = 0;
    *out = bytes;
    *length = at;
    return true;
}

TEST(origin_v2_strict_wire_and_raw_streams) {
    ov_fixture_t f;
    bool ready = ov_default(&f);
    static const uint32_t id[] = {1};
    static const int wanted[] = {1};
    static const unsigned char low[] = {'x', '/', 0x7f};
    static const unsigned char nonutf[] = {'x', '/', 0x80, 0xff};
    ov_row_t raw[] = {ov_row(":-*.c", 2, 1, 15, id, 1),
                      ov_row("back\\slash.c", 2, 1, 15, id, 1),
                      ov_row("tab\tline\n.c", 2, 1, 15, id, 1),
                      ov_row("unused-low", 2, 1, 15, id, 1),
                      ov_row("unused-high", 2, 1, 15, id, 1)};
    raw[3].path = low; raw[3].path_length = sizeof(low);
    raw[4].path = nonutf; raw[4].path_length = sizeof(nonutf);
    const size_t raw_count = sizeof(raw) / sizeof(raw[0]);
    const unsigned char *records = NULL;
    size_t record_bytes = 0;
    bool control = ready && ov_records(&f, raw, raw_count, &records, &record_bytes) &&
                   ov_names(&f, records, record_bytes, records, record_bytes, false) &&
                   ov_wire(&f, raw, raw_count);
    bool ok = control && ov_success(&f, 0, false, wanted, 1);
    if (control) {
        cbm_coverage_origin_join_t *join = NULL;
        cbm_coverage_origin_status_t status = cbm_coverage_origin_join(
            &f.input, &f.limits, NULL, NULL, &join);
        size_t count = 0;
        const cbm_coverage_origin_path_t *paths = cbm_coverage_origin_join_paths(join, &count);
        bool paths_ok = status == CBM_COVERAGE_ORIGIN_OK && paths && count == raw_count;
        for (size_t i = 0; paths_ok && i < raw_count; i++)
            paths_ok = ov_path_equal(&paths[i], raw[i].path, raw[i].path_length);
        ok = paths_ok && ok;
        cbm_coverage_origin_join_free(join);
        static const unsigned char name[] = "M\0f\0";
        static const uint32_t two_ids[] = {1, 3};
        ov_row_t row = ov_row("f", 2, 1, 15, two_ids, 2);
        ok = ov_names(&f, name, sizeof(name) - 1, name, sizeof(name) - 1, false) &&
             ov_wire(&f, &row, 1) && ov_success(&f, 0, false, (const int[]){1, 3}, 2) && ok;
        size_t full_length = f.length;
        for (size_t cut = 1; cut < full_length; cut++) {
            f.length = cut;
            ov_rehash(&f);
            ok = ov_error(&f, CBM_COVERAGE_ORIGIN_FORMAT) && ok;
        }
        f.length = full_length;
        ov_rehash(&f);
        size_t positions[] = {0, sizeof("cbm-coverage-origins") + 3,
                               sizeof("cbm-coverage-origins") + 8 +
                                   f.context.binding.repository_key.length,
                               f.global_flag, f.global_proof, f.rows[0].state,
                               f.rows[0].mode + 3, f.rows[0].disposition,
                               f.rows[0].edits, f.rows[0].proof};
        unsigned char values[] = {'x', 1, 3, 2, 2, 4, 0, 3, 16, 2};
        for (size_t i = 0; i < sizeof(positions) / sizeof(positions[0]); i++) {
            unsigned char saved = f.wire[positions[i]];
            f.wire[positions[i]] = values[i];
            ov_rehash(&f);
            ok = ov_error(&f, CBM_COVERAGE_ORIGIN_FORMAT) && ok;
            f.wire[positions[i]] = saved;
        }
        f.wire[f.length++] = 0;
        ov_rehash(&f);
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_FORMAT) && ok;
        f.length--;
        ov_rehash(&f);
        static const uint32_t duplicate[] = {1, 1}, unsorted[] = {3, 1}, outside[] = {4};
        const uint32_t *bad_ids[] = {duplicate, unsorted, outside};
        for (size_t i = 0; i < 3; i++) {
            row.ids = bad_ids[i]; row.count = i == 2 ? 1 : 2;
            ok = ov_wire(&f, &row, 1) && ov_error(&f, CBM_COVERAGE_ORIGIN_FORMAT) && ok;
        }
        row.ids = id; row.count = 1;
        ov_row_t duplicate_rows[] = {row, row};
        ok = ov_wire(&f, duplicate_rows, 2) && ov_error(&f, CBM_COVERAGE_ORIGIN_FORMAT) && ok;
        ov_row_t bad_order[] = {ov_row("z", 2, 1, 15, id, 1), row};
        ok = ov_wire(&f, bad_order, 2) && ov_error(&f, CBM_COVERAGE_ORIGIN_FORMAT) && ok;
        ov_row_t hidden[] = {row, ov_row("z", 0, 0, 0, outside, 1)};
        ok = ov_wire(&f, hidden, 2) && ov_error(&f, CBM_COVERAGE_ORIGIN_FORMAT) && ok;
        const char *unsafe_paths[] = {"", "/", "/f", "f/", "a//f", "./f", "a/../f", "a/./f"};
        for (size_t i = 0; i < sizeof(unsafe_paths) / sizeof(unsafe_paths[0]); i++) {
            ov_row_t bad_path = ov_row(unsafe_paths[i], 2, 1, 15, id, 1);
            ok = ov_wire(&f, &bad_path, 1) && ov_error(&f, CBM_COVERAGE_ORIGIN_FORMAT) && ok;
        }
        static const unsigned char embedded_nul[] = {'f', 0, 'x'};
        ov_row_t bad_path = row;
        bad_path.path = embedded_nul; bad_path.path_length = sizeof(embedded_nul);
        ok = ov_wire(&f, &bad_path, 1) && ov_error(&f, CBM_COVERAGE_ORIGIN_FORMAT) && ok;
        static const unsigned char bad0[] = "M\0f", bad1[] = "R100\0f\0",
            bad2[] = "MM\0f\0", bad3[] = "M\0\0", bad4[] = "M\0a/../f\0",
            bad5[] = "M\0a//f\0", bad6[] = "M\0/f\0", bad7[] = "M\0f/\0",
            bad8[] = "M\0f\0M\0f\0", bad9[] = "M\0./f\0", bad10[] = "X\0f\0";
        struct { const unsigned char *bytes; size_t length; } bad[] = {
            {bad0, sizeof(bad0) - 1}, {bad1, sizeof(bad1) - 1}, {bad2, sizeof(bad2) - 1},
            {bad3, sizeof(bad3) - 1}, {bad4, sizeof(bad4) - 1}, {bad5, sizeof(bad5) - 1},
            {bad6, sizeof(bad6) - 1}, {bad7, sizeof(bad7) - 1}, {bad8, sizeof(bad8) - 1},
            {bad9, sizeof(bad9) - 1}, {bad10, sizeof(bad10) - 1}};
        ok = ov_wire(&f, &row, 1) && ov_success(&f, 0, false, wanted, 1) && ok;
        cbm_coverage_origin_bytes_t saved = f.input.artifact_to_head;
        for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            f.input.artifact_to_head = (cbm_coverage_origin_bytes_t){bad[i].bytes, bad[i].length};
            ov_hash(bad[i].bytes, bad[i].length, f.context.artifact_to_head_sha256);
            ok = ov_error(&f, CBM_COVERAGE_ORIGIN_FORMAT) && ok;
        }
        f.input.artifact_to_head = saved;
        ov_hash(saved.data, saved.length, f.context.artifact_to_head_sha256);
        cbm_coverage_origin_bytes_t saved_mh = f.input.merge_base_to_head;
        f.input.merge_base_to_head = (cbm_coverage_origin_bytes_t){bad8, sizeof(bad8) - 1};
        ov_hash(bad8, sizeof(bad8) - 1, f.context.merge_base_to_head_sha256);
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_FORMAT) && ok;
        f.input.merge_base_to_head = saved_mh;
        ov_hash(saved_mh.data, saved_mh.length, f.context.merge_base_to_head_sha256);
        ok = ov_success(&f, 0, false, wanted, 1) && ok;
    }
    ov_close(&f);
    ASSERT_TRUE(control);
    ASSERT_TRUE(ok);
    PASS();
}

TEST(origin_v2_admission_and_tuple_binding) {
    ov_fixture_t f;
    bool ready = ov_default(&f);
    static const unsigned char names[] = "M\0f\0";
    static const uint32_t id[] = {1};
    static const int wanted[] = {1};
    ov_row_t row = ov_row("f", 2, 1, 15, id, 1);
    bool control = ready && ov_names(&f, names, sizeof(names) - 1, names, sizeof(names) - 1, false) &&
                   ov_wire(&f, &row, 1);
    bool ok = control && ov_success(&f, 0, false, wanted, 1);
    if (control) {
        bool *flags[] = {&f.context.artifact_admitted, &f.context.origin_source_verified,
                         &f.context.origin_attestations_verified, &f.context.comparisons_verified};
        for (size_t i = 0; i < 4; i++) {
            *flags[i] = false;
            ok = ov_error(&f, CBM_COVERAGE_ORIGIN_UNVERIFIED) && ok;
            *flags[i] = true;
        }
        unsigned char *digests[] = {f.context.binding.functions_sha256,
            f.context.binding.manifest_sha256, f.context.binding.compatibility_sha256,
            f.context.binding.producer_profile_sha256, f.context.artifact_to_head_sha256,
            f.context.merge_base_to_head_sha256, f.context.binding.artifact.bytes};
        for (size_t i = 0; i < sizeof(digests) / sizeof(digests[0]); i++) {
            digests[i][0] ^= 1;
            ok = ov_error(&f, CBM_COVERAGE_ORIGIN_BINDING) && ok;
            digests[i][0] ^= 1;
        }
        cbm_coverage_origin_bytes_t key = f.context.binding.repository_key;
        f.context.binding.repository_key =
            (cbm_coverage_origin_bytes_t){(const unsigned char *)"other-repo", 10};
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_BINDING) && ok;
        f.context.binding.repository_key = key;
        f.context.binding.object_format = CBM_COVERAGE_ORIGIN_GIT_SHA256;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_BINDING) && ok;
        f.context.binding.object_format = CBM_COVERAGE_ORIGIN_GIT_SHA1;
        f.context.binding.artifact.bytes[31] = 1;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_INVALID) && ok;
        f.context.binding.artifact.bytes[31] = 0;
        f.context.binding.manifest_version = 1;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_INVALID) && ok;
        f.context.binding.manifest_version = 2;
        int count = f.function_count;
        f.function_count = 3; /* independently wrong header count; selected ID remains valid */
        ok = ov_wire(&f, &row, 1) && ov_error(&f, CBM_COVERAGE_ORIGIN_BINDING) && ok;
        f.function_count = count;
        ok = ov_wire(&f, &row, 1) && ov_success(&f, 0, false, wanted, 1) && ok;
        cbm_coverage_origin_join_t *join = NULL;
        cbm_coverage_origin_status_t status = cbm_coverage_origin_join(
            &f.input, &f.limits, NULL, NULL, &join);
        const cbm_coverage_origin_binding_t *bound = cbm_coverage_origin_join_binding(join);
        ok = status == CBM_COVERAGE_ORIGIN_OK && bound && bound->repository_key.length == 9 &&
             memcmp(bound->repository_key.data, "test-repo", 9) == 0 &&
             memcmp(bound->artifact.bytes, f.context.binding.artifact.bytes, 32) == 0 &&
             memcmp(bound->merge_base.bytes, f.context.binding.merge_base.bytes, 32) == 0 &&
             memcmp(bound->head.bytes, f.context.binding.head.bytes, 32) == 0 &&
             memcmp(bound->functions_sha256, f.context.binding.functions_sha256, 32) == 0 &&
             memcmp(bound->manifest_sha256, f.context.binding.manifest_sha256, 32) == 0 &&
             memcmp(bound->compatibility_sha256, f.context.binding.compatibility_sha256, 32) == 0 &&
             memcmp(bound->producer_profile_sha256, f.context.binding.producer_profile_sha256, 32) == 0 &&
             bound->manifest_version == 2 && bound->object_format == 1 && ok;
        /* A nonempty caller output slot is cleared before even NULL-input
         * validation, while its previously returned owner remains caller-owned. */
        cbm_coverage_origin_join_t *previous = join;
        if (previous) {
            status = cbm_coverage_origin_join(NULL, &f.limits, NULL, NULL, &join);
            ok = status == CBM_COVERAGE_ORIGIN_INVALID && join == NULL &&
                 cbm_coverage_origin_join_can_narrow(previous) && ok;
            if (join != previous)
                cbm_coverage_origin_join_free(join);
        }
        cbm_coverage_origin_join_free(previous);
        const cbm_coverage_map_t *saved_map = f.input.coverage;
        const cbm_changes_t *saved_changes = f.input.request_changes;
        const cbm_coverage_origin_context_t *saved_context = f.input.context;
        f.input.coverage = NULL;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_INVALID) && ok;
        f.input.coverage = saved_map;
        f.input.request_changes = NULL;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_INVALID) && ok;
        f.input.request_changes = saved_changes;
        f.input.context = NULL;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_INVALID) && ok;
        f.input.context = saved_context;
        static const unsigned char opaque_key[] = {'r', 0, 0xff};
        f.context.binding.repository_key =
            (cbm_coverage_origin_bytes_t){opaque_key, sizeof(opaque_key)};
        ok = ov_wire(&f, &row, 1) && ov_success(&f, 0, false, wanted, 1) && ok;
        join = NULL;
        status = cbm_coverage_origin_join(&f.input, &f.limits, NULL, NULL, &join);
        bound = cbm_coverage_origin_join_binding(join);
        ok = status == CBM_COVERAGE_ORIGIN_OK && bound &&
             bound->repository_key.length == sizeof(opaque_key) &&
             memcmp(bound->repository_key.data, opaque_key, sizeof(opaque_key)) == 0 && ok;
        cbm_coverage_origin_join_free(join);
    }
    ov_close(&f);
    ASSERT_TRUE(control);
    ASSERT_TRUE(ok);
    PASS();
}

TEST(origin_v2_resource_limits) {
    ov_fixture_t f;
    bool ready = ov_default(&f);
    static const unsigned char names[] = "M\0f\0";
    static const uint32_t id[] = {1}, all_ids[] = {0, 1, 2, 3};
    static const int wanted[] = {1};
    ov_row_t rows[] = {ov_row("f", 2, 1, 15, id, 1), ov_row("z", 0, 0, 0, all_ids, 4)};
    bool control = ready && ov_names(&f, names, sizeof(names) - 1, names, sizeof(names) - 1, false) &&
                   ov_wire(&f, rows, 2);
    bool ok = control && ov_success(&f, 0, false, wanted, 1);
    if (control) {
        cbm_coverage_origin_limits_t ample = f.limits;
        uint64_t exact = f.input.manifest.length + f.input.artifact_to_head.length +
                         f.input.merge_base_to_head.length + f.context.binding.repository_key.length;
        f.limits.max_input_bytes = exact;
        f.limits.max_result_ids = 1; /* Unqueried IDs must not consume union cardinality. */
        ok = ov_success(&f, 0, false, wanted, 1) && ok;
        f.limits.max_input_bytes = exact - 1;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_LIMIT) && ok;
        f.limits = ample;
        f.limits.max_items = 1;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_LIMIT) && ok;
        f.limits = ample;
        f.limits.max_alloc_bytes = 1;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_LIMIT) && ok;
        for (int i = 0; i < 6; i++) {
            f.limits = ample;
            if (i == 0) f.limits.max_input_bytes = 0;
            if (i == 1) f.limits.max_items = 0;
            if (i == 2) f.limits.max_alloc_bytes = 0;
            if (i == 3) f.limits.max_result_ids = 0;
            if (i == 4) f.limits.max_input_bytes = UINT64_MAX / 8 + 1;
            if (i == 5) f.limits.max_result_ids = (size_t)INT_MAX + 1;
            ok = ov_error(&f, CBM_COVERAGE_ORIGIN_INVALID) && ok;
        }
        f.limits = ample;
        rows[0].disposition = CBM_COVERAGE_ORIGIN_ALL;
        ok = ov_wire(&f, rows, 2) &&
             ov_success(&f, 0, true, (const int[]){0, 1, 2, 3}, 4) && ok;
        f.limits.max_result_ids = 3;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_LIMIT) && ok;
        f.limits = ample;
        ok = ov_success(&f, 0, true, (const int[]){0, 1, 2, 3}, 4) && ok;
        /* The cap applies to the distinct global union, not each row alone. */
        static const unsigned char pair_names[] = "M\0f\0M\0g\0";
        static const uint32_t other_id[] = {2};
        ov_row_t singletons[] = {ov_row("f", 2, 1, 15, id, 1),
                                 ov_row("g", 2, 1, 15, other_id, 1)};
        bool singleton_control = ov_names(&f, pair_names, sizeof(pair_names) - 1,
            pair_names, sizeof(pair_names) - 1, false) && ov_wire(&f, singletons, 2);
        control = singleton_control && control;
        if (singleton_control) {
            f.limits.max_result_ids = 2;
            ok = ov_success(&f, 0, false, (const int[]){1, 2}, 2) && ok;
            f.limits.max_result_ids = 1;
            ok = ov_error(&f, CBM_COVERAGE_ORIGIN_LIMIT) && ok;
            f.limits.max_result_ids = 2;
            ok = ov_success(&f, 0, false, (const int[]){1, 2}, 2) && ok;
            singletons[1].ids = id;
            f.limits.max_result_ids = 1;
            ok = ov_wire(&f, singletons, 2) && ov_success(&f, 0, false, wanted, 1) && ok;
        }
    }
    ov_close(&f);
    /* A tiny manifest/empty query still has to charge validation of a borrowed
     * large map. Build a real valid map; never pass a fabricated huge span. */
    CBMArena generation;
    cbm_arena_init_lazy(&generation, 4096);
    const int rows_count = 4097;
    size_t capacity = (size_t)rows_count * 48;
    char *functions = cbm_arena_alloc(&generation, capacity);
    size_t used = 0;
    bool generated = functions != NULL;
    for (int i = 0; generated && i < rows_count; i++) {
        int n = snprintf(functions + used, capacity - used, "%d\tf%d.c\tfn%d\n", i, i, i);
        generated = n > 0 && (size_t)n < capacity - used;
        if (generated)
            used += (size_t)n;
    }
    /* The existing map contract requires every table ID to be observed and
     * every suite to have setup. This is synthetic fixture data, not admission. */
    size_t observation_capacity = (size_t)rows_count * 12 + 128;
    char *observations = cbm_arena_alloc(&generation, observation_capacity);
    size_t observation_used = 0;
    generated = observations && generated;
    if (generated) {
        int n = snprintf(observations, observation_capacity, "core:observed\tcomplete\t\t");
        generated = n > 0 && (size_t)n < observation_capacity;
        if (generated)
            observation_used = (size_t)n;
    }
    for (int i = 0; generated && i < rows_count; i++) {
        int n = snprintf(observations + observation_used,
                         observation_capacity - observation_used, "%s%d", i ? " " : "", i);
        generated = n > 0 && (size_t)n < observation_capacity - observation_used;
        if (generated)
            observation_used += (size_t)n;
    }
    if (generated) {
        int n = snprintf(observations + observation_used,
                         observation_capacity - observation_used,
                         "\ncore:empty\tcomplete\t\t\ncore:*\tcomplete\t\t\n");
        generated = n > 0 && (size_t)n < observation_capacity - observation_used;
        if (generated)
            observation_used += (size_t)n;
    }
    ov_fixture_t large;
    memset(&large, 0, sizeof(large));
    bool large_control = generated && ov_open(&large, functions, used,
                                              observations, observation_used) &&
                         large.function_count == rows_count && ov_wire(&large, NULL, 0);
    if (large_control) {
        const cbm_coverage_test_t *observed = cbm_coverage_map_find_test(large.map, "core", "observed");
        const cbm_coverage_test_t *setup = cbm_coverage_map_find_test(large.map, "core", "*");
        large_control = observed && observed->complete && observed->function_ids &&
                        observed->function_count == rows_count && setup &&
                        setup->complete && setup->function_count == 0;
        for (int i = 0; large_control && i < rows_count; i++)
            large_control = observed->function_ids[i] == i;
    }
    cbm_arena_destroy(&generation);
    bool large_ok = large_control && ov_success(&large, 0, false, NULL, 0);
    if (large_control) {
        large.limits.max_items = (uint64_t)rows_count - 1;
        large_ok = ov_error(&large, CBM_COVERAGE_ORIGIN_LIMIT) && large_ok;
    }
    ov_close(&large);
    ASSERT_TRUE(control);
    ASSERT_TRUE(large_control);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(large_ok);
    PASS();
}

typedef struct { size_t calls, cancel_at; } ov_cancel_t;
static bool ov_cancel(void *opaque) {
    ov_cancel_t *c = opaque;
    c->calls++;
    return c->cancel_at && c->calls >= c->cancel_at;
}

typedef struct {
    cbm_mutex_t *start;
    const cbm_coverage_origin_input_t *input;
    const cbm_coverage_origin_limits_t *limits;
    ov_cancel_t cancellation;
    cbm_coverage_origin_status_t status;
    cbm_coverage_origin_join_t *result;
} ov_call_t;

static void *ov_call_thread(void *opaque) {
    ov_call_t *call = opaque;
    cbm_mutex_lock(call->start);
    cbm_mutex_unlock(call->start);
    call->status = cbm_coverage_origin_join(call->input, call->limits, ov_cancel,
                                          &call->cancellation, &call->result);
    return NULL;
}

/* The held start gate permits concurrent scheduling without assuming internal
 * overlap. No callback waits, scheduler ordering, sleeps or timing assertions. */
static bool ov_independent_calls(ov_fixture_t *f) {
    cbm_mutex_t start;
    cbm_mutex_init(&start);
    cbm_mutex_lock(&start);
    ov_call_t healthy = {.start = &start, .input = &f->input, .limits = &f->limits};
    ov_call_t cancelled = {.start = &start, .input = &f->input, .limits = &f->limits,
                           .cancellation = {.cancel_at = 1}};
    cbm_thread_t threads[2];
    bool first = cbm_thread_create(&threads[0], 0, ov_call_thread, &healthy) == 0;
    bool second = cbm_thread_create(&threads[1], 0, ov_call_thread, &cancelled) == 0;
    cbm_mutex_unlock(&start);
    bool first_joined = !first || cbm_thread_join(&threads[0]) == 0;
    bool second_joined = !second || cbm_thread_join(&threads[1]) == 0;
    cbm_mutex_destroy(&start);
    int count = 0;
    const int *ids = cbm_coverage_origin_join_ids(healthy.result, &count);
    bool ok = first && second && first_joined && second_joined &&
              healthy.status == CBM_COVERAGE_ORIGIN_OK && healthy.result &&
              cbm_coverage_origin_join_can_narrow(healthy.result) && count == 2 &&
              ov_ids_equal(ids, (size_t)count, (const int[]){1, 3}, 2) &&
              cancelled.status == CBM_COVERAGE_ORIGIN_CANCELLED &&
              cancelled.result == NULL && cancelled.cancellation.calls > 0;
    cbm_coverage_origin_join_free(healthy.result);
    cbm_coverage_origin_join_free(cancelled.result);
    return ok;
}

TEST(origin_v2_cancellation_ownership_and_null_views) {
    ov_fixture_t f;
    bool ready = ov_default(&f);
    size_t path_length = 70001;
    unsigned char *path = ready ? cbm_arena_alloc(&f.arena, path_length + 1) : NULL;
    if (path) {
        memset(path, 'x', path_length);
        path[path_length - 1] = 'A'; path[path_length] = 0;
    }
    static const uint32_t listed[] = {1, 3};
    ov_row_t row = ov_row("unused", 2, 1, 15, listed, 2);
    row.path = path; row.path_length = path_length;
    unsigned char *second = ready ? cbm_arena_alloc(&f.arena, path_length + 1) : NULL;
    if (second && path) {
        memcpy(second, path, path_length + 1);
        second[path_length - 1] = 'B';
    }
    ov_row_t rows[] = {row, row};
    rows[1].path = second;
    const unsigned char *names = NULL;
    size_t names_length = 0;
    bool control = ready && path && second && ov_records(&f, rows, 2, &names, &names_length) &&
                   ov_names(&f, names, names_length, names, names_length, false) &&
                   ov_wire(&f, rows, 2);
    ov_cancel_t observed = {0};
    cbm_coverage_origin_join_t *join = NULL;
    cbm_coverage_origin_status_t status = control ? cbm_coverage_origin_join(
        &f.input, &f.limits, ov_cancel, &observed, &join) : CBM_COVERAGE_ORIGIN_INVALID;
    bool ok = status == CBM_COVERAGE_ORIGIN_OK && join && observed.calls > 0;
    cbm_coverage_origin_join_free(join);
    join = NULL;
    if (control && observed.calls) {
        size_t positions[] = {1, observed.calls / 2 + 1, observed.calls};
        for (size_t i = 0; i < 3; i++) {
            ov_cancel_t cancellation = {.cancel_at = positions[i]};
            status = cbm_coverage_origin_join(&f.input, &f.limits, ov_cancel, &cancellation, &join);
            ok = status == CBM_COVERAGE_ORIGIN_CANCELLED && join == NULL &&
                 cancellation.calls >= positions[i] && ok;
            cbm_coverage_origin_join_free(join);
            join = NULL;
        }
        ok = ov_independent_calls(&f) && ok;
        /* A separate call on the same immutable borrowed owners remains usable. */
        ok = ov_success(&f, 0, false, (const int[]){1, 3}, 2) && ok;
        status = cbm_coverage_origin_join(&f.input, &f.limits, NULL, NULL, &join);
        ok = status == CBM_COVERAGE_ORIGIN_OK && join && ok;
    }
    if (control) {
        memset(f.wire, 0xa5, f.length);
        memset((void *)f.input.artifact_to_head.data, 0xa5, f.input.artifact_to_head.length);
        memset((void *)f.input.merge_base_to_head.data, 0xa5, f.input.merge_base_to_head.length);
        memset((void *)f.context.binding.repository_key.data, 0xa5, 9);
        memset(path, 0xa5, path_length);
        memset(second, 0xa5, path_length);
    }
    ov_close(&f);
    if (join) {
        size_t count = 0;
        const cbm_coverage_origin_path_t *paths = cbm_coverage_origin_join_paths(join, &count);
        const cbm_coverage_origin_binding_t *bound = cbm_coverage_origin_join_binding(join);
        int surviving_count = -1;
        const int *surviving_ids = cbm_coverage_origin_join_ids(join, &surviving_count);
        bool owned = surviving_count == 2 &&
                     ov_ids_equal(surviving_ids, (size_t)surviving_count,
                                  (const int[]){1, 3}, 2) && count == 2 && paths && paths[0].path.length == path_length &&
                     paths[0].path.data && paths[0].path.data[path_length] == 0 &&
                     paths[0].path.data[path_length - 1] == 'A' &&
                     paths[1].path.length == path_length && paths[1].path.data &&
                     paths[1].path.data[path_length] == 0 &&
                     paths[1].path.data[path_length - 1] == 'B' && bound &&
                     bound->repository_key.length == 9 &&
                     memcmp(bound->repository_key.data, "test-repo", 9) == 0 &&
                     ov_ids_equal(paths[0].function_ids, paths[0].function_count,
                                  (const int[]){1, 3}, 2) &&
                     ov_ids_equal(paths[1].function_ids, paths[1].function_count,
                                  (const int[]){1, 3}, 2);
        for (size_t i = 0; owned && i + 1 < path_length; i++)
            owned = paths[0].path.data[i] == 'x' && paths[1].path.data[i] == 'x';
        ok = owned && cbm_coverage_origin_join_can_narrow(join) && ok;
    }
    cbm_coverage_origin_join_free(join);
    int ids_count = -1;
    size_t paths_count = SIZE_MAX;
    bool null_contract = cbm_coverage_origin_join_binding(NULL) == NULL &&
        cbm_coverage_origin_join_ids(NULL, &ids_count) == NULL && ids_count == 0 &&
        cbm_coverage_origin_join_paths(NULL, &paths_count) == NULL && paths_count == 0 &&
        !cbm_coverage_origin_join_complete(NULL) &&
        !cbm_coverage_origin_join_broad_fallback_required(NULL) &&
        !cbm_coverage_origin_join_can_narrow(NULL) &&
        cbm_coverage_origin_join_reasons(NULL) == CBM_COVERAGE_ORIGIN_REQUEST_UNKNOWN &&
        cbm_coverage_origin_join_request_state(NULL) == CBM_CHANGES_UNKNOWN &&
        cbm_coverage_map_functions_sha256(NULL) == NULL;
    cbm_coverage_origin_join_free(NULL);
    ASSERT_TRUE(control);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(null_contract);
    PASS();
}

/* Generic identity-table compatibility. These synthetic context flags are
 * unit fixtures, not evidence of artifact admission or producer soundness. */
#define OVP_IMAGE "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
static const char ovp_profiles[] =
    "CBM_PROFILE_MAP\t2\t" OVP_IMAGE "\t2\n"
    "0\t61\t0000000000000000\t1\n"
    "1\t620063\t8000000000000000\t2\n";
static const char ovp_profiles_sha[] =
    "10de0263d12f9388a74967ae61a6fae824f7017e3563527e2a5e56efc9fdc258";

static bool ovp_legacy_control(void) {
    ov_fixture_t f;
    bool ready = ov_default(&f);
    static const unsigned char names[] = "M\0f\0";
    static const uint32_t id[] = {1};
    ov_row_t row = ov_row("f", 2, CBM_COVERAGE_ORIGIN_COMPLETE_IDS,
                          CBM_COVERAGE_ORIGIN_EDIT_M, id, 1);
    const char *digest = ready ? cbm_coverage_map_identity_sha256(f.map) : NULL;
    bool ok = ready && cbm_coverage_map_format(f.map) == CBM_COVERAGE_FORMAT_FUNCTIONS &&
              cbm_coverage_map_id_count(f.map) == 4 && digest &&
              strcmp(digest, ov_lf_sha) == 0 &&
              ov_names(&f, names, sizeof(names) - 1, names, sizeof(names) - 1, false) &&
              ov_wire(&f, &row, 1) && ov_success(&f, 0, false, (const int[]){1}, 1);
    ov_close(&f);
    return ok;
}

static bool ovp_empty_observations(const cbm_coverage_map_t *map) {
    const cbm_coverage_test_t *empty = cbm_coverage_map_find_test(map, "core", "empty");
    const cbm_coverage_test_t *setup = cbm_coverage_map_find_test(map, "core", "*");
    return empty && empty->complete && empty->function_count == 0 &&
           setup && setup->complete && setup->function_count == 0 &&
           !cbm_coverage_test_intersects(empty, (const int[]){0, 1}, 2) &&
           !cbm_coverage_test_intersects(setup, (const int[]){0, 1}, 2);
}

static bool ovp_parse(const void *bytes, size_t length, cbm_coverage_map_t **out) {
    const cbm_coverage_parse_limits_t limits = {
        .max_input_bytes = 8U * 1024U * 1024U, .max_items = 100000,
        .max_alloc_bytes = 32U * 1024U * 1024U, .max_ids = 8192};
    cbm_coverage_parse_status_t status = cbm_coverage_map_parse_v2(
        bytes, length, ov_empty_tests, sizeof(ov_empty_tests) - 1,
        &limits, NULL, NULL, out);
    return status == CBM_COVERAGE_PARSE_OK && *out;
}

static bool ovp_map_control(const cbm_coverage_map_t *map, const void *bytes,
                            size_t length, int count, const char *image) {
    int legacy_count = -1, profile_count = -1;
    const cbm_coverage_function_t *legacy = cbm_coverage_map_functions(map, &legacy_count);
    const cbm_coverage_profile_t *profiles = cbm_coverage_map_profiles(map, &profile_count);
    const cbm_coverage_profile_binding_t *binding = cbm_coverage_map_profile_binding(map);
    const char *digest = cbm_coverage_map_identity_sha256(map);
    char expected[65];
    cbm_sha256_hex(bytes, length, expected);
    return map && cbm_coverage_map_format(map) == CBM_COVERAGE_FORMAT_PROFILES &&
           cbm_coverage_map_id_count(map) == count && profile_count == count &&
           (count ? profiles != NULL : profiles == NULL) && binding &&
           strcmp(binding->image_sha256, image) == 0 && digest &&
           strcmp(digest, expected) == 0 && !legacy && legacy_count == 0 &&
           cbm_coverage_map_functions_sha256(map) == NULL && ovp_empty_observations(map);
}

/* Retain the unchanged native helper, replacing its map only after a genuine
 * format-2 parse succeeds. No fake map, projection or observation is created. */
static bool ovp_open(ov_fixture_t *f, const void *bytes, size_t length, int count) {
    if (!ov_default(f))
        return false;
    cbm_coverage_map_t *map = NULL;
    bool ready = ovp_parse(bytes, length, &map) &&
                 ovp_map_control(map, bytes, length, count, OVP_IMAGE);
    if (!ready) {
        cbm_coverage_map_free(map);
        return false;
    }
    cbm_coverage_map_free(f->map);
    f->map = map;
    f->input.coverage = map;
    f->function_count = count;
    ov_hash(bytes, length, f->context.binding.functions_sha256);
    return true;
}

static bool ovp_two_profiles(const cbm_coverage_map_t *map, unsigned variant) {
    int count = -1;
    const cbm_coverage_profile_t *rows = cbm_coverage_map_profiles(map, &count);
    const unsigned char a[] = {variant == 1 ? 'A' : 'a'};
    const unsigned char b[] = {'b', variant == 2 ? 0xff : 0, 'c'};
    return rows && count == 2 && rows[0].id == 0 && rows[1].id == 1 &&
           rows[0].name_length == 1 && rows[0].name && memcmp(rows[0].name, a, 1) == 0 &&
           rows[1].name_length == 3 && rows[1].name && memcmp(rows[1].name, b, 3) == 0 &&
           rows[0].function_hash == 0 && rows[0].counter_count == 1 &&
           rows[1].function_hash == (variant == 3 ? 0 : UINT64_C(0x8000000000000000)) &&
           rows[1].counter_count == (variant == 4 ? 3U : 2U);
}

static bool ovp_one_row(ov_fixture_t *f, unsigned disposition,
                        const uint32_t *ids, size_t count) {
    static const unsigned char names[] = "M\0f\0";
    ov_row_t row = ov_row("f", 2, disposition, CBM_COVERAGE_ORIGIN_EDIT_M, ids, count);
    return ov_names(f, names, sizeof(names) - 1, names, sizeof(names) - 1, false) &&
           ov_wire(f, &row, 1);
}

static bool ovp_binding_equal(const cbm_coverage_origin_binding_t *a,
                              const cbm_coverage_origin_binding_t *b) {
    return a && b && a->repository_key.length == b->repository_key.length &&
           a->repository_key.data && b->repository_key.data &&
           memcmp(a->repository_key.data, b->repository_key.data, a->repository_key.length) == 0 &&
           a->object_format == b->object_format && a->manifest_version == b->manifest_version &&
           memcmp(a->artifact.bytes, b->artifact.bytes, 32) == 0 &&
           memcmp(a->merge_base.bytes, b->merge_base.bytes, 32) == 0 &&
           memcmp(a->head.bytes, b->head.bytes, 32) == 0 &&
           memcmp(a->functions_sha256, b->functions_sha256, 32) == 0 &&
           memcmp(a->manifest_sha256, b->manifest_sha256, 32) == 0 &&
           memcmp(a->compatibility_sha256, b->compatibility_sha256, 32) == 0 &&
           memcmp(a->producer_profile_sha256, b->producer_profile_sha256, 32) == 0;
}

static bool ovp_one_result(const ov_fixture_t *f, const cbm_coverage_origin_join_t *join,
                           unsigned disposition, bool broad, const int *ids, size_t count) {
    int id_count = -1;
    size_t path_count = 0;
    const int *actual = cbm_coverage_origin_join_ids(join, &id_count);
    const cbm_coverage_origin_path_t *paths = cbm_coverage_origin_join_paths(join, &path_count);
    const cbm_coverage_origin_binding_t *binding = cbm_coverage_origin_join_binding(join);
    bool common = join && cbm_coverage_origin_join_complete(join) &&
        cbm_coverage_origin_join_reasons(join) == 0 &&
        cbm_coverage_origin_join_broad_fallback_required(join) == broad &&
        cbm_coverage_origin_join_can_narrow(join) == !broad &&
        cbm_coverage_origin_join_request_state(join) == CBM_CHANGES_NONEMPTY &&
        id_count >= 0 && ov_ids_equal(actual, (size_t)id_count, ids, count) &&
        ovp_binding_equal(binding, &f->context.binding);
    return common && paths && path_count == 1 && ov_path_equal(paths, "f", 1) &&
        paths[0].comparisons == (CBM_COVERAGE_ORIGIN_FROM_ARTIFACT |
                                 CBM_COVERAGE_ORIGIN_FROM_MERGE_BASE) &&
        paths[0].artifact_status == 'M' && paths[0].merge_base_status == 'M' &&
        paths[0].disposition == disposition && paths[0].supported_edits == CBM_COVERAGE_ORIGIN_EDIT_M &&
        paths[0].reasons == 0 && ov_ids_equal(paths[0].function_ids, paths[0].function_count, ids, count);
}

TEST(origin_profiles_unobserved_identity) {
    ASSERT_TRUE(ovp_legacy_control());
    ov_fixture_t f;
    bool parser = ovp_open(&f, ovp_profiles, sizeof(ovp_profiles) - 1, 2) &&
                  ovp_two_profiles(f.map, 5) &&
                  strcmp(cbm_coverage_map_identity_sha256(f.map), ovp_profiles_sha) == 0;
    bool fixture = parser && ovp_one_row(&f, CBM_COVERAGE_ORIGIN_COMPLETE_IDS,
                                        (const uint32_t[]){1}, 1);
    cbm_coverage_origin_join_t *join = NULL;
    cbm_coverage_origin_status_t status = fixture ? cbm_coverage_origin_join(
        &f.input, &f.limits, NULL, NULL, &join) : CBM_COVERAGE_ORIGIN_INVALID;
    bool ok = status == CBM_COVERAGE_ORIGIN_OK &&
              ovp_one_result(&f, join, CBM_COVERAGE_ORIGIN_COMPLETE_IDS, false,
                              (const int[]){1}, 1) && ovp_empty_observations(f.map);
    cbm_coverage_origin_join_free(join);
    ov_close(&f);
    ASSERT_TRUE(parser);
    ASSERT_TRUE(fixture);
    ASSERT_TRUE(ok);
    PASS();
}

static bool ovp_missing_evidence(ov_fixture_t *f, ov_row_t row) {
    bool ok = ov_wire(f, NULL, 0) &&
        ov_success(f, CBM_COVERAGE_ORIGIN_PATH_MISSING, false, NULL, 0);
    row.proof = false;
    ok = ov_wire(f, &row, 1) &&
         ov_success(f, CBM_COVERAGE_ORIGIN_CLAIM_UNPROVEN, false, NULL, 0) && ok;
    row.proof = true;
    row.disposition = CBM_COVERAGE_ORIGIN_UNKNOWN;
    ok = ov_wire(f, &row, 1) &&
         ov_success(f, CBM_COVERAGE_ORIGIN_CLAIM_UNKNOWN, false, NULL, 0) && ok;
    row.disposition = CBM_COVERAGE_ORIGIN_COMPLETE_IDS;
    f->universe_proof = false;
    ok = ov_wire(f, &row, 1) && ov_success(f,
         CBM_COVERAGE_ORIGIN_PROFILE_UNIVERSE_UNKNOWN, false, NULL, 0) && ok;
    f->universe_proof = true;
    f->universe = false;
    ok = ov_wire(f, &row, 1) && ov_success(f,
         CBM_COVERAGE_ORIGIN_PROFILE_UNIVERSE_UNKNOWN, false, NULL, 0) && ok;
    f->universe = true;
    return ok;
}

static bool ovp_floor_cases(ov_fixture_t *f, int count) {
    static const unsigned char names[] = "M\0f\0";
    static const int all[] = {0, 1};
    ov_row_t row = ov_row("f", 2, CBM_COVERAGE_ORIGIN_COMPLETE_IDS,
                          CBM_COVERAGE_ORIGIN_EDIT_M, NULL, 0);
    bool ok = ov_names(f, names, sizeof(names) - 1, names, sizeof(names) - 1, false) &&
              ov_wire(f, &row, 1) && ov_success(f, 0, false, NULL, 0) &&
              cbm_changes_state(f->changes) == CBM_CHANGES_NONEMPTY;
    row.disposition = CBM_COVERAGE_ORIGIN_ALL;
    ok = ov_wire(f, &row, 1) && ov_success(f, 0, true, all, (size_t)count) &&
         ovp_empty_observations(f->map) && ok;
    f->universe_proof = false;
    ok = ov_wire(f, &row, 1) && ov_success(f,
         CBM_COVERAGE_ORIGIN_PROFILE_UNIVERSE_UNKNOWN, true, all, (size_t)count) && ok;
    f->universe_proof = true;
    row.disposition = CBM_COVERAGE_ORIGIN_COMPLETE_IDS;
    ok = ovp_missing_evidence(f, row) && ok;
    /* Nonempty AH does not change the independently established empty MH. */
    ok = ov_names(f, names, sizeof(names) - 1, "", 0, false) &&
         ov_wire(f, &row, 1) && ov_success(f, 0, false, NULL, 0) &&
         cbm_changes_state(f->changes) == CBM_CHANGES_EMPTY && ok;
    row.disposition = CBM_COVERAGE_ORIGIN_ALL;
    return ov_wire(f, &row, 1) && ov_success(f, 0, true, all, (size_t)count) && ok;
}

TEST(origin_profiles_zero_and_broad_floors) {
    ASSERT_TRUE(ovp_legacy_control());
    static const char zero_table[] = "CBM_PROFILE_MAP\t2\t" OVP_IMAGE "\t0\n";
    ov_fixture_t zero, nonzero;
    bool zero_parser = ovp_open(&zero, zero_table, sizeof(zero_table) - 1, 0);
    bool nonzero_parser = ovp_open(&nonzero, ovp_profiles, sizeof(ovp_profiles) - 1, 2) &&
                          ovp_two_profiles(nonzero.map, 5);
    bool zero_ok = zero_parser && ovp_floor_cases(&zero, 0);
    bool nonzero_ok = nonzero_parser && ovp_floor_cases(&nonzero, 2);
    ov_close(&zero);
    ov_close(&nonzero);
    ASSERT_TRUE(zero_parser);
    ASSERT_TRUE(nonzero_parser);
    ASSERT_TRUE(zero_ok);
    ASSERT_TRUE(nonzero_ok);
    PASS();
}

static bool ovp_mutate(char *table, unsigned variant, char image[65]) {
    memcpy(table, ovp_profiles, sizeof(ovp_profiles));
    memcpy(image, OVP_IMAGE, 65);
    char *first = strstr(table, "0\t61\t0000000000000000\t1\n");
    char *second = strstr(table, "1\t620063\t8000000000000000\t2\n");
    if (!first || !second || variant > 4)
        return false;
    if (variant == 0) {
        table[sizeof("CBM_PROFILE_MAP\t2\t") - 1] = '1';
        image[0] = '1';
    }
    if (variant == 1)
        first[2] = '4'; /* raw a -> A; remains before raw b-NUL-c */
    if (variant == 2)
        memcpy(second + 4, "ff", 2); /* raw b-NUL-c -> b-FF-c */
    if (variant == 3)
        second[9] = '0'; /* change only bit 63 of the full hash */
    if (variant == 4)
        second[26] = '3'; /* shape 2 -> 3, no counter values involved */
    return true;
}

TEST(origin_profiles_exact_table_binding) {
    ASSERT_TRUE(ovp_legacy_control());
    ov_fixture_t f;
    bool parser = ovp_open(&f, ovp_profiles, sizeof(ovp_profiles) - 1, 2) &&
                  ovp_two_profiles(f.map, 5);
    bool fixture = parser && ovp_one_row(&f, CBM_COVERAGE_ORIGIN_COMPLETE_IDS,
                                        (const uint32_t[]){1}, 1);
    bool positive = fixture && ov_success(&f, 0, false, (const int[]){1}, 1);
    bool alternatives = parser, rejected = positive;
    for (unsigned variant = 0; fixture && variant < 5; variant++) {
        char table[sizeof(ovp_profiles)], image[65];
        cbm_coverage_map_t *other = NULL;
        bool valid = ovp_mutate(table, variant, image) &&
                     ovp_parse(table, sizeof(table) - 1, &other) &&
                     ovp_map_control(other, table, sizeof(table) - 1, 2, image) &&
                     ovp_two_profiles(other, variant) &&
                     strcmp(cbm_coverage_map_identity_sha256(other), ovp_profiles_sha) != 0;
        alternatives = valid && alternatives;
        /* Only the actual map changes: manifest and trusted digest stay fixed. */
        f.input.coverage = other;
        rejected = valid && ov_error(&f, CBM_COVERAGE_ORIGIN_BINDING) && rejected;
        f.input.coverage = f.map;
        cbm_coverage_map_free(other);
    }
    bool recovery = fixture && ov_success(&f, 0, false, (const int[]){1}, 1);
    ov_close(&f);
    ASSERT_TRUE(parser);
    ASSERT_TRUE(fixture);
    ASSERT_TRUE(alternatives);
    ASSERT_TRUE(positive);
    ASSERT_TRUE(rejected);
    ASSERT_TRUE(recovery);
    PASS();
}

static bool ovp_generated_table(CBMArena *arena, int count, char **table, size_t *length) {
    *table = NULL;
    *length = 0;
    if (count < 0 || count > 4097)
        return false;
    size_t capacity = 128 + (size_t)count * 64;
    char *bytes = cbm_arena_alloc(arena, capacity);
    if (!bytes)
        return false;
    int n = snprintf(bytes, capacity, "CBM_PROFILE_MAP\t2\t%s\t%d\n", OVP_IMAGE, count);
    if (n <= 0 || (size_t)n >= capacity)
        return false;
    size_t used = (size_t)n;
    static const char digits[] = "0123456789abcdef";
    for (int i = 0; i < count; i++) {
        char raw[7], hex[13];
        if (snprintf(raw, sizeof(raw), "p%05d", i) != 6)
            return false;
        for (size_t j = 0; j < 6; j++) {
            hex[j * 2] = digits[(unsigned char)raw[j] >> 4];
            hex[j * 2 + 1] = digits[(unsigned char)raw[j] & 15];
        }
        hex[12] = 0;
        n = snprintf(bytes + used, capacity - used, "%d\t%s\t%016llx\t%d\n",
                     i, hex, (unsigned long long)i, i + 1);
        if (n <= 0 || (size_t)n >= capacity - used)
            return false;
        used += (size_t)n;
    }
    *table = bytes;
    *length = used;
    return true;
}

static bool ovp_generated_profiles(const cbm_coverage_map_t *map, int wanted) {
    int count = -1;
    const cbm_coverage_profile_t *rows = cbm_coverage_map_profiles(map, &count);
    if (count != wanted || (count && !rows))
        return false;
    for (int i = 0; i < count; i++) {
        char name[7];
        if (snprintf(name, sizeof(name), "p%05d", i) != 6 || rows[i].id != i ||
            rows[i].name_length != 6 || !rows[i].name || memcmp(rows[i].name, name, 6) != 0 ||
            rows[i].function_hash != (uint64_t)i || rows[i].counter_count != (uint64_t)i + 1)
            return false;
    }
    return true;
}

static bool ovp_empty_result(const ov_fixture_t *f, const cbm_coverage_origin_join_t *join) {
    int count = -1;
    size_t paths = SIZE_MAX;
    (void)cbm_coverage_origin_join_ids(join, &count);
    (void)cbm_coverage_origin_join_paths(join, &paths);
    return join && count == 0 && paths == 0 && cbm_coverage_origin_join_complete(join) &&
           cbm_coverage_origin_join_reasons(join) == 0 &&
           !cbm_coverage_origin_join_broad_fallback_required(join) &&
           cbm_coverage_origin_join_can_narrow(join) &&
           cbm_coverage_origin_join_request_state(join) == CBM_CHANGES_EMPTY &&
           ovp_binding_equal(cbm_coverage_origin_join_binding(join), &f->context.binding);
}

static bool ovp_item_probe(ov_fixture_t *f, uint64_t cap, bool *success) {
    uint64_t saved = f->limits.max_items;
    f->limits.max_items = cap;
    cbm_coverage_origin_join_t *join = NULL;
    cbm_coverage_origin_status_t status = cbm_coverage_origin_join(
        &f->input, &f->limits, NULL, NULL, &join);
    *success = status == CBM_COVERAGE_ORIGIN_OK;
    bool ok = *success ? ovp_empty_result(f, join) :
                        status == CBM_COVERAGE_ORIGIN_LIMIT && join == NULL;
    cbm_coverage_origin_join_free(join);
    f->limits.max_items = saved;
    return ok;
}

/* At most 22 calls: one bounded upper control, <=20 bisections, exact control.
 * No callback phase, allocation count or undocumented absolute tariff is used. */
static bool ovp_min_items(ov_fixture_t *f, uint64_t *minimum) {
    uint64_t low = 1, high = UINT64_C(1) << 20;
    bool success = false;
    if (!ovp_item_probe(f, high, &success) || !success)
        return false;
    unsigned iterations = 0;
    while (low < high && iterations < 20) {
        uint64_t middle = low + (high - low) / 2;
        if (!ovp_item_probe(f, middle, &success))
            return false;
        if (success)
            high = middle;
        else
            low = middle + 1;
        iterations++;
    }
    if (low != high || !ovp_item_probe(f, low, &success) || !success)
        return false;
    *minimum = low;
    return true;
}

static bool ovp_tariff_pair(int count, uint64_t *native_min, uint64_t *profile_min,
                            bool *fixtures) {
    ov_fixture_t native, profile;
    memset(&profile, 0, sizeof(profile));
    bool native_ready = count ? ov_default(&native) :
        ov_open(&native, "", 0, ov_empty_tests, sizeof(ov_empty_tests) - 1);
    char *table = NULL;
    size_t length = 0;
    bool generated = native_ready && ovp_generated_table(&native.arena, count, &table, &length);
    bool profile_ready = generated && ovp_open(&profile, table, length, count) &&
                         ovp_generated_profiles(profile.map, count);
    *fixtures = native_ready && profile_ready && native.function_count == count &&
                ov_wire(&native, NULL, 0) && ov_wire(&profile, NULL, 0);
    bool ok = *fixtures && ovp_min_items(&native, native_min) &&
              ovp_min_items(&profile, profile_min) && *native_min == *profile_min;
    if (ok && count) {
        bool native_success = true, profile_success = true;
        ok = *native_min > 1 &&
             ovp_item_probe(&native, *native_min - 1, &native_success) && !native_success &&
             ovp_item_probe(&profile, *profile_min - 1, &profile_success) && !profile_success;
    }
    ov_close(&profile);
    ov_close(&native);
    return ok;
}

static bool ovp_cancel_samples(ov_fixture_t *f, const int *all, size_t count) {
    cbm_coverage_origin_join_t *join = NULL;
    ov_cancel_t observed = {0};
    cbm_coverage_origin_status_t status = cbm_coverage_origin_join(
        &f->input, &f->limits, ov_cancel, &observed, &join);
    bool ok = status == CBM_COVERAGE_ORIGIN_OK && observed.calls > 0 &&
              ovp_one_result(f, join, CBM_COVERAGE_ORIGIN_ALL, true, all, count);
    cbm_coverage_origin_join_free(join);
    join = NULL;
    if (!ok)
        return false;
    size_t positions[] = {1, observed.calls / 4 + 1, observed.calls / 2 + 1,
                          observed.calls - observed.calls / 4, observed.calls};
    for (size_t i = 0; i < sizeof(positions) / sizeof(positions[0]); i++) {
        ov_cancel_t cancellation = {.cancel_at = positions[i]};
        status = cbm_coverage_origin_join(&f->input, &f->limits, ov_cancel, &cancellation, &join);
        if (i == 0)
            ok = status == CBM_COVERAGE_ORIGIN_CANCELLED && join == NULL && ok;
        if (status == CBM_COVERAGE_ORIGIN_CANCELLED)
            ok = join == NULL && cancellation.calls >= positions[i] && ok;
        else
            ok = status == CBM_COVERAGE_ORIGIN_OK && cancellation.calls < positions[i] &&
                 ovp_one_result(f, join, CBM_COVERAGE_ORIGIN_ALL, true, all, count) && ok;
        cbm_coverage_origin_join_free(join);
        join = NULL;
    }
    status = cbm_coverage_origin_join(&f->input, &f->limits, NULL, NULL, &join);
    ok = status == CBM_COVERAGE_ORIGIN_OK &&
         ovp_one_result(f, join, CBM_COVERAGE_ORIGIN_ALL, true, all, count) && ok;
    cbm_coverage_origin_join_free(join);
    return ok;
}

static bool ovp_large_budget_case(bool *fixture) {
    CBMArena arena;
    cbm_arena_init_lazy(&arena, 4096);
    const int count = 4097;
    char *table = NULL;
    size_t length = 0;
    ov_fixture_t f;
    memset(&f, 0, sizeof(f));
    int *all = cbm_arena_alloc(&arena, (size_t)count * sizeof(int));
    bool generated = all && ovp_generated_table(&arena, count, &table, &length);
    for (int i = 0; generated && i < count; i++)
        all[i] = i;
    *fixture = generated && ovp_open(&f, table, length, count) &&
               ovp_generated_profiles(f.map, count) && ov_wire(&f, NULL, 0);
    bool ok = *fixture && ov_success(&f, 0, false, NULL, 0);
    if (*fixture) {
        uint64_t ample_items = f.limits.max_items;
        f.limits.max_items = (uint64_t)count - 1;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_LIMIT) && ok;
        f.limits.max_items = ample_items;
        ok = ovp_one_row(&f, CBM_COVERAGE_ORIGIN_ALL, NULL, 0) && ok;
        f.limits.max_result_ids = (size_t)count;
        ok = ov_success(&f, 0, true, all, (size_t)count) && ok;
        f.limits.max_result_ids = (size_t)count - 1;
        ok = ov_error(&f, CBM_COVERAGE_ORIGIN_LIMIT) && ok;
        f.limits.max_result_ids = (size_t)count;
        ok = ovp_cancel_samples(&f, all, (size_t)count) && ovp_empty_observations(f.map) && ok;
    }
    ov_close(&f);
    cbm_arena_destroy(&arena);
    return ok;
}

TEST(origin_profiles_item_and_cancellation_compatibility) {
    ASSERT_TRUE(ovp_legacy_control());
    uint64_t native_zero = 0, profile_zero = 0, native_four = 0, profile_four = 0;
    bool zero_fixtures = false, four_fixtures = false, large_fixture = false;
    bool zero = ovp_tariff_pair(0, &native_zero, &profile_zero, &zero_fixtures);
    bool four = ovp_tariff_pair(4, &native_four, &profile_four, &four_fixtures);
    bool large = ovp_large_budget_case(&large_fixture);
    ASSERT_TRUE(zero_fixtures);
    ASSERT_TRUE(four_fixtures);
    ASSERT_TRUE(large_fixture);
    ASSERT_TRUE(zero);
    ASSERT_TRUE(four);
    ASSERT_TRUE(native_four > native_zero);
    ASSERT_TRUE(profile_four > profile_zero);
    ASSERT_TRUE(large);
    PASS();
}

static bool ovp_legacy_orphan_guard(void) {
    static const char functions[] = "0\tf.c\tfn\n";
    static const char observed[] = "core:*\tcomplete\t\t\ncore:seen\tcomplete\t\t0\n";
    cbm_coverage_map_t *valid = cbm_coverage_map_parse(functions, sizeof(functions) - 1,
                                                    observed, sizeof(observed) - 1);
    const cbm_coverage_test_t *seen = cbm_coverage_map_find_test(valid, "core", "seen");
    bool positive = valid && cbm_coverage_map_id_count(valid) == 1 && seen && seen->complete &&
                    seen->function_count == 1 && seen->function_ids && seen->function_ids[0] == 0;
    cbm_coverage_map_t *orphan = cbm_coverage_map_parse(functions, sizeof(functions) - 1,
                                                     ov_empty_tests, sizeof(ov_empty_tests) - 1);
    bool ok = positive && orphan == NULL;
    cbm_coverage_map_free(orphan);
    cbm_coverage_map_free(valid);
    return ok;
}

TEST(origin_profiles_trust_is_not_parser_success) {
    ASSERT_TRUE(ovp_legacy_control());
    ASSERT_TRUE(ovp_legacy_orphan_guard());
    ov_fixture_t f;
    bool parser = ovp_open(&f, ovp_profiles, sizeof(ovp_profiles) - 1, 2) &&
                  ovp_two_profiles(f.map, 5);
    bool fixture = parser && ovp_one_row(&f, CBM_COVERAGE_ORIGIN_COMPLETE_IDS,
                                        (const uint32_t[]){1}, 1);
    bool positive = fixture && ov_success(&f, 0, false, (const int[]){1}, 1);
    bool refused = positive;
    if (fixture) {
        cbm_coverage_origin_context_t trusted = f.context;
        for (unsigned flag = 0; flag < 4; flag++) {
            f.context = trusted;
            if (flag == 0) f.context.artifact_admitted = false;
            if (flag == 1) f.context.origin_source_verified = false;
            if (flag == 2) f.context.origin_attestations_verified = false;
            if (flag == 3) f.context.comparisons_verified = false;
            refused = ov_error(&f, CBM_COVERAGE_ORIGIN_UNVERIFIED) && refused;
        }
        f.context = trusted;
        /* This legacy checker explicitly refuses v2; it cannot set the flag. */
        unsigned receipt = cbm_coverage_map_check_receipt(f.map, "", 0, NULL, NULL);
        refused = receipt == (CBM_COVERAGE_RECEIPT_INVALID | CBM_COVERAGE_RECEIPT_METADATA) && refused;
        f.context.artifact_admitted = false;
        refused = ov_error(&f, CBM_COVERAGE_ORIGIN_UNVERIFIED) && refused;
        f.context = trusted;
    }
    bool recovery = fixture && ov_success(&f, 0, false, (const int[]){1}, 1);
    ov_close(&f);
    ASSERT_TRUE(parser);
    ASSERT_TRUE(fixture);
    ASSERT_TRUE(positive);
    ASSERT_TRUE(refused);
    ASSERT_TRUE(recovery);
    PASS();
}

/* Authorized follow-on: isolate the selector's ID-domain validation from
 * origin-join support. Synthetic admission below is not a provider result. */
#define OVP_SELECTOR_CASES \
    "core:gap\tincomplete\tmissing profile\t\n" \
    "core:hit\tcomplete\t\t0\ncore:quiet\tcomplete\t\t1\n"
static const char *const ovp_selector_rows[] = {
    "core:*\tcomplete\t\t\n" OVP_SELECTOR_CASES,
    "core:*\tcomplete\t\t0\n" OVP_SELECTOR_CASES,
    "core:*\tincomplete\tmissing setup\t\n" OVP_SELECTOR_CASES};

static cbm_test_model_t *ovp_selector_model(void) {
    static const char source[] =
        "TEST(hit) {}\nTEST(quiet) {}\nTEST(gap) {}\nTEST(missing) {}\n"
        "SUITE(core) { RUN_TEST(hit); RUN_TEST(quiet); RUN_TEST(gap); RUN_TEST(missing); }\n"
        "void main(void) { RUN_SELECTED_SUITE(core); }\n";
    cbm_test_model_t *model = cbm_test_model_new(cbm_test_conventions_cbm());
    bool ready = model && cbm_test_model_add_source(model, "tests/profile_cases.c",
        source, sizeof(source) - 1) && cbm_test_model_finish(model) && cbm_test_model_complete(model);
    if (!ready) {
        cbm_test_model_free(model);
        return NULL;
    }
    int count = -1;
    const cbm_test_registration_t *registrations = cbm_test_model_registrations(model, &count);
    ready = ready && registrations && count == 4;
    for (int i = 0; ready && i < count; i++)
        ready = registrations[i].resolved && !registrations[i].conditional;
    const cbm_test_runner_suite_t *suites = cbm_test_model_runner_suites(model, &count);
    ready = ready && suites && count == 1 && strcmp(suites[0].name, "core") == 0 && !suites[0].perf;
    if (!ready) {
        cbm_test_model_free(model);
        return NULL;
    }
    return model;
}

static bool ovp_selector_observations(const cbm_coverage_map_t *map, unsigned variant) {
    const cbm_coverage_test_t *setup = cbm_coverage_map_find_test(map, "core", "*");
    const cbm_coverage_test_t *hit = cbm_coverage_map_find_test(map, "core", "hit");
    const cbm_coverage_test_t *quiet = cbm_coverage_map_find_test(map, "core", "quiet");
    const cbm_coverage_test_t *gap = cbm_coverage_map_find_test(map, "core", "gap");
    bool cases = hit && hit->complete && hit->function_count == 1 && hit->function_ids &&
        hit->function_ids[0] == 0 && quiet && quiet->complete && quiet->function_count == 1 &&
        quiet->function_ids && quiet->function_ids[0] == 1 && gap && !gap->complete &&
        gap->function_count == 0 && cbm_coverage_map_find_test(map, "core", "missing") == NULL;
    if (!cases || !setup || setup->complete != (variant != 2))
        return false;
    if (variant == 1)
        return setup->function_count == 1 && setup->function_ids && setup->function_ids[0] == 0;
    return setup->function_count == 0;
}

static bool ovp_selector_map(bool profiles, unsigned variant, cbm_coverage_map_t **out) {
    *out = NULL;
    if (variant >= sizeof(ovp_selector_rows) / sizeof(ovp_selector_rows[0]))
        return false;
    const char *rows = ovp_selector_rows[variant];
    if (profiles) {
        const cbm_coverage_parse_limits_t limits = {.max_input_bytes = 4096, .max_items = 100,
            .max_alloc_bytes = 1024U * 1024U, .max_ids = 2};
        cbm_coverage_parse_status_t status = cbm_coverage_map_parse_v2(
            ovp_profiles, sizeof(ovp_profiles) - 1, rows, strlen(rows), &limits, NULL, NULL, out);
        if (status != CBM_COVERAGE_PARSE_OK || !*out || !ovp_two_profiles(*out, 5))
            return false;
        const char *digest = cbm_coverage_map_identity_sha256(*out);
        if (!digest || strcmp(digest, ovp_profiles_sha) != 0)
            return false;
    } else {
        static const char functions[] = "0\tf.c\tchanged\n1\tg.c\tquiet\n";
        *out = cbm_coverage_map_parse(functions, sizeof(functions) - 1, rows, strlen(rows));
    }
    return *out && cbm_coverage_map_id_count(*out) == 2 &&
        cbm_coverage_map_format(*out) == (profiles ? CBM_COVERAGE_FORMAT_PROFILES :
                                                   CBM_COVERAGE_FORMAT_FUNCTIONS) &&
        ovp_selector_observations(*out, variant);
}

static bool ovp_selected_cases(const cbm_test_selection_t *result) {
    int count = -1;
    const cbm_test_selected_case_t *cases = cbm_test_selection_cases(result, &count);
    static const char *const names[] = {"gap", "hit", "missing"};
    static const unsigned reasons[] = {CBM_TEST_SELECT_COVERAGE_UNKNOWN,
        CBM_TEST_SELECT_COVERAGE, CBM_TEST_SELECT_COVERAGE_UNKNOWN};
    if (!cases || count != 3)
        return false;
    for (int i = 0; i < count; i++)
        if (strcmp(cases[i].suite, "core") != 0 || strcmp(cases[i].test, names[i]) != 0 ||
            strcmp(cases[i].file, "tests/profile_cases.c") != 0 || cases[i].reasons != reasons[i])
            return false;
    const cbm_test_selected_suite_t *suites = cbm_test_selection_suites(result, &count);
    return suites && count == 1 && strcmp(suites[0].name, "core") == 0 && !suites[0].whole &&
        suites[0].reasons == (CBM_TEST_SELECT_COVERAGE | CBM_TEST_SELECT_COVERAGE_UNKNOWN);
}

static bool ovp_selection_result(const cbm_test_selection_t *result, unsigned variant) {
    if (!result)
        return false;
    unsigned run_all = cbm_test_selection_run_all(result);
    int count = -1;
    if (variant == 3) {
        (void)cbm_test_selection_cases(result, &count);
        bool empty = count == 0;
        (void)cbm_test_selection_suites(result, &count);
        return run_all == CBM_TEST_SELECT_INVALID_INPUT && empty && count == 0;
    }
    if (run_all)
        return false;
    if (variant == 0)
        return ovp_selected_cases(result);
    const cbm_test_selected_suite_t *suites = cbm_test_selection_suites(result, &count);
    unsigned expected = variant == 1 ? CBM_TEST_SELECT_SETUP_HIT : CBM_TEST_SELECT_SETUP_UNKNOWN;
    bool whole = suites && count == 1 && strcmp(suites[0].name, "core") == 0 &&
                 suites[0].whole && (suites[0].reasons & expected) != 0;
    (void)cbm_test_selection_cases(result, &count);
    return whole && count == 0;
}

static bool ovp_select(cbm_test_model_t *model, cbm_coverage_map_t *map, unsigned variant) {
    cbm_test_reach_t reach[] = {
        {"tests/profile_cases.c", "hit", true, false, false},
        {"tests/profile_cases.c", "quiet", true, false, false},
        {"tests/profile_cases.c", "gap", true, false, false},
        {"tests/profile_cases.c", "missing", true, false, false}};
    int changed = variant == 3 ? 2 : 0;
    cbm_test_selection_input_t input = {.model = model, .coverage = map, .reach = reach,
        .reach_count = 4, .changed_function_ids = &changed, .changed_function_count = 1,
        .has_changes = true, .diff_complete = true, .inventory_complete = true,
        .static_complete = true, .coverage_admitted = true, .coverage_changes_complete = true};
    cbm_test_selection_t *result = cbm_test_select(&input);
    bool ok = ovp_selection_result(result, variant);
    cbm_test_selection_free(result);
    return ok;
}

TEST(origin_profiles_selector_generic_id_domain) {
    /* No origin-join precondition: this independently binds the selector seam. */
    cbm_test_model_t *model = ovp_selector_model();
    cbm_coverage_map_t *native = NULL;
    bool native_parser = model && ovp_selector_map(false, 0, &native);
    bool native_ok = native_parser && ovp_select(model, native, 0) && ovp_select(model, native, 3);
    cbm_coverage_map_free(native);
    bool parsers = model != NULL, selected = native_ok;
    for (unsigned variant = 0; model && variant < 3; variant++) {
        cbm_coverage_map_t *map = NULL;
        bool parser = ovp_selector_map(true, variant, &map);
        parsers = parser && parsers;
        selected = parser && ovp_select(model, map, variant) && selected;
        if (variant == 0)
            selected = parser && ovp_select(model, map, 3) && selected;
        cbm_coverage_map_free(map);
    }
    cbm_test_model_free(model);
    ASSERT_TRUE(native_parser);
    ASSERT_TRUE(native_ok);
    ASSERT_TRUE(parsers);
    ASSERT_TRUE(selected);
    PASS();
}

SUITE(test_impact_origins) {
    RUN_TEST(origin_v2_wire_and_actual_map_digest);
    RUN_TEST(origin_v2_union_and_request_state);
    RUN_TEST(origin_v2_many_to_many_and_unknown_ids);
    RUN_TEST(origin_v2_edit_class_obligations);
    RUN_TEST(origin_v2_optional_header_and_certified_zero);
    RUN_TEST(origin_v2_all_sticky_broad_floor);
    RUN_TEST(origin_v2_exact_reason_masks);
    RUN_TEST(origin_v2_strict_wire_and_raw_streams);
    RUN_TEST(origin_v2_admission_and_tuple_binding);
    RUN_TEST(origin_v2_resource_limits);
    RUN_TEST(origin_v2_cancellation_ownership_and_null_views);
    RUN_TEST(origin_profiles_unobserved_identity);
    RUN_TEST(origin_profiles_zero_and_broad_floors);
    RUN_TEST(origin_profiles_exact_table_binding);
    RUN_TEST(origin_profiles_item_and_cancellation_compatibility);
    RUN_TEST(origin_profiles_trust_is_not_parser_success);
    RUN_TEST(origin_profiles_selector_generic_id_domain);
}
