/*
 * test_zstd.c — Tests for zstd compression wrappers.
 */
#include "test_framework.h"

extern int cbm_zstd_compress(const char *src, int srcLen, char *dst, int dstCap, int level);
extern int cbm_zstd_decompress(const char *src, int srcLen, char *dst, int dstCap);
extern size_t cbm_zstd_compress_bound(int inputSize);

TEST(zstd_roundtrip) {
    const char *data = "Hello, zstd compression roundtrip test!";
    int len = (int)strlen(data);

    size_t bound = cbm_zstd_compress_bound(len);
    ASSERT_GT((int)bound, 0);

    char *cbuf = malloc(bound);
    ASSERT_NOT_NULL(cbuf);

    int clen = cbm_zstd_compress(data, len, cbuf, (int)bound, 3);
    ASSERT_GT(clen, 0);

    char *dbuf = malloc(len);
    ASSERT_NOT_NULL(dbuf);

    int dlen = cbm_zstd_decompress(cbuf, clen, dbuf, len);
    ASSERT_EQ(dlen, len);
    ASSERT_MEM_EQ(dbuf, data, len);

    free(cbuf);
    free(dbuf);
    PASS();
}

TEST(zstd_roundtrip_large) {
    int len = 100000;
    char *data = malloc(len);
    ASSERT_NOT_NULL(data);

    /* Repetitive data — should compress well */
    for (int i = 0; i < len; i++) {
        data[i] = "function_name_pattern_abcdef"[i % 28];
    }

    size_t bound = cbm_zstd_compress_bound(len);
    char *cbuf = malloc(bound);
    ASSERT_NOT_NULL(cbuf);

    int clen = cbm_zstd_compress(data, len, cbuf, (int)bound, 9);
    ASSERT_GT(clen, 0);
    /* Repetitive data should compress at least 2:1 */
    ASSERT_LT(clen, len / 2);

    char *dbuf = malloc(len);
    ASSERT_NOT_NULL(dbuf);

    int dlen = cbm_zstd_decompress(cbuf, clen, dbuf, len);
    ASSERT_EQ(dlen, len);
    ASSERT_MEM_EQ(dbuf, data, len);

    free(data);
    free(cbuf);
    free(dbuf);
    PASS();
}

TEST(zstd_compress_levels) {
    const char *data = "test data for different compression levels";
    int len = (int)strlen(data);
    size_t bound = cbm_zstd_compress_bound(len);
    char *cbuf = malloc(bound);
    ASSERT_NOT_NULL(cbuf);

    /* Both level 3 (fast) and level 9 (best) should produce valid output */
    int clen3 = cbm_zstd_compress(data, len, cbuf, (int)bound, 3);
    ASSERT_GT(clen3, 0);

    int clen9 = cbm_zstd_compress(data, len, cbuf, (int)bound, 9);
    ASSERT_GT(clen9, 0);

    free(cbuf);
    PASS();
}

TEST(zstd_decompress_too_small_output) {
    const char *data = "this is test data that will be compressed";
    int len = (int)strlen(data);
    size_t bound = cbm_zstd_compress_bound(len);
    char *cbuf = malloc(bound);
    ASSERT_NOT_NULL(cbuf);

    int clen = cbm_zstd_compress(data, len, cbuf, (int)bound, 3);
    ASSERT_GT(clen, 0);

    /* Try decompressing with too-small output buffer — should return 0 (error) */
    char small[4];
    int dlen = cbm_zstd_decompress(cbuf, clen, small, 4);
    ASSERT_EQ(dlen, 0);

    free(cbuf);
    PASS();
}

TEST(zstd_bound_positive) {
    ASSERT_GT((int)cbm_zstd_compress_bound(1), 0);
    ASSERT_GT((int)cbm_zstd_compress_bound(100), 0);
    ASSERT_GT((int)cbm_zstd_compress_bound(1000000), 0);
    PASS();
}

SUITE(zstd) {
    RUN_TEST(zstd_roundtrip);
    RUN_TEST(zstd_roundtrip_large);
    RUN_TEST(zstd_compress_levels);
    RUN_TEST(zstd_decompress_too_small_output);
    RUN_TEST(zstd_bound_positive);
}
