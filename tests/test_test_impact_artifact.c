/*
 * test_test_impact_artifact.c — the team artifact receipt and its digests.
 */
#include "test_framework.h"
#include "test_helpers.h"

#include "mcp/test_impact_artifact.h"

#include <stdio.h>
#include <string.h>

static bool tia_write(const char *dir, const char *name, const char *text) {
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    FILE *f = fopen(path, "wb");
    if (!f) {
        return false;
    }
    bool ok = fputs(text, f) >= 0;
    return fclose(f) == 0 && ok;
}

static cbm_ti_receipt_t tia_receipt(bool coverage) {
    cbm_ti_receipt_t r = {0};
    memset(r.commit, 'a', 40);
    memset(r.graph_sha256, 'b', 64);
    memset(r.graph_content_sha256, '9', 64);
    memset(r.graph_topology_sha256, '8', 64);
    snprintf(r.platform, sizeof(r.platform), "linux-x86_64-clang");
    r.has_coverage = coverage;
    if (coverage) {
        memset(r.functions_sha256, 'c', 64);
        memset(r.tests_sha256, 'd', 64);
        memset(r.metadata_sha256, 'e', 64);
        memset(r.compatibility_sha256, 'f', 64);
        r.oldest_observation_at = 1791150000;
    }
    return r;
}

TEST(test_impact_artifact_receipt_round_trips) {
    char *dir = th_mktempdir("cbm_tia_rt");
    ASSERT_NOT_NULL(dir);
    for (int coverage = 0; coverage < 2; coverage++) {
        cbm_ti_receipt_t in = tia_receipt(coverage);
        ASSERT_TRUE(cbm_ti_receipt_write(dir, &in));
        cbm_ti_receipt_t out;
        ASSERT_EQ(cbm_ti_receipt_read(dir, &out), CBM_TI_RECEIPT_OK);
        ASSERT_STR_EQ(out.commit, in.commit);
        ASSERT_STR_EQ(out.graph_sha256, in.graph_sha256);
        ASSERT_STR_EQ(out.graph_content_sha256, in.graph_content_sha256);
        ASSERT_STR_EQ(out.graph_topology_sha256, in.graph_topology_sha256);
        ASSERT_STR_EQ(out.platform, in.platform);
        ASSERT_EQ(out.has_coverage, in.has_coverage);
        ASSERT_STR_EQ(out.functions_sha256, in.functions_sha256);
        ASSERT_STR_EQ(out.compatibility_sha256, in.compatibility_sha256);
        ASSERT_EQ(out.oldest_observation_at, in.oldest_observation_at);
    }
    th_rmtree(dir);
    PASS();
}

TEST(test_impact_artifact_receipt_is_strict) {
    char *dir = th_mktempdir("cbm_tia_strict");
    ASSERT_NOT_NULL(dir);
    cbm_ti_receipt_t out;
    ASSERT_EQ(cbm_ti_receipt_read(dir, &out), CBM_TI_RECEIPT_ABSENT);
    static const char *const bad[] = {
        "not json",
        "{}",
        /* unknown schema */
        "{\"schema\":\"cbm.test_impact.artifact.v2\",\"commit\":\""
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"graph_sha256\":\""
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"graph_content_"
        "sha256\":\"9999999999999999999999999999999999999999999999999999999999999999\","
        "\"graph_topology_sha256\":"
        "\"8888888888888888888888888888888888888888888888888888888888888888\",\"platform\":\"p\","
        "\"coverage\":null}",
        /* a commit of the wrong length */
        "{\"schema\":\"cbm.test_impact.artifact.v1\",\"commit\":\"abc\",\"graph_sha256\":\""
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"graph_content_"
        "sha256\":\"9999999999999999999999999999999999999999999999999999999999999999\","
        "\"graph_topology_sha256\":"
        "\"8888888888888888888888888888888888888888888888888888888888888888\",\"platform\":\"p\","
        "\"coverage\":null}",
        /* uppercase digest */
        "{\"schema\":\"cbm.test_impact.artifact.v1\",\"commit\":\""
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"graph_sha256\":\""
        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB\",\"graph_content_"
        "sha256\":\"9999999999999999999999999999999999999999999999999999999999999999\","
        "\"graph_topology_sha256\":"
        "\"8888888888888888888888888888888888888888888888888888888888888888\",\"platform\":\"p\","
        "\"coverage\":null}",
        /* coverage without its observation time */
        "{\"schema\":\"cbm.test_impact.artifact.v1\",\"commit\":\""
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"graph_sha256\":\""
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"graph_content_"
        "sha256\":\"9999999999999999999999999999999999999999999999999999999999999999\","
        "\"graph_topology_sha256\":"
        "\"8888888888888888888888888888888888888888888888888888888888888888\",\"platform\":\"p\","
        "\"coverage\":{\"functions_sha256\":\""
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\",\"tests_sha256\":\""
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\",\"metadata_sha256\":\""
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\","
        "\"compatibility_sha256\":\""
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\"}}",
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        ASSERT_TRUE(tia_write(dir, CBM_TI_RECEIPT_FILE, bad[i]));
        ASSERT_EQ(cbm_ti_receipt_read(dir, &out), CBM_TI_RECEIPT_INVALID);
        ASSERT_EQ(out.commit[0], '\0');
    }
    th_rmtree(dir);
    PASS();
}

TEST(test_impact_artifact_compatibility_digest_binds_every_input) {
    char *dir = th_mktempdir("cbm_tia_compat");
    ASSERT_NOT_NULL(dir);
    ASSERT_TRUE(tia_write(dir, "harness.h", "int x;\n"));
    const char *paths[] = {"harness.h", "missing.mk"};
    char base[65], again[65], other[65];
    ASSERT_TRUE(cbm_ti_compatibility_digest(dir, paths, 2, "linux", base));
    ASSERT_TRUE(cbm_ti_compatibility_digest(dir, paths, 2, "linux", again));
    ASSERT_STR_EQ(base, again);
    /* another platform */
    ASSERT_TRUE(cbm_ti_compatibility_digest(dir, paths, 2, "darwin", other));
    ASSERT_TRUE(strcmp(base, other) != 0);
    /* a byte of a listed file */
    ASSERT_TRUE(tia_write(dir, "harness.h", "int y;\n"));
    ASSERT_TRUE(cbm_ti_compatibility_digest(dir, paths, 2, "linux", other));
    ASSERT_TRUE(strcmp(base, other) != 0);
    ASSERT_TRUE(tia_write(dir, "harness.h", "int x;\n"));
    /* an absent listed file appearing, even empty */
    ASSERT_TRUE(tia_write(dir, "missing.mk", ""));
    ASSERT_TRUE(cbm_ti_compatibility_digest(dir, paths, 2, "linux", other));
    ASSERT_TRUE(strcmp(base, other) != 0);
    /* the list itself */
    ASSERT_TRUE(cbm_ti_compatibility_digest(dir, paths, 1, "linux", other));
    ASSERT_TRUE(strcmp(base, other) != 0);
    /* an empty listed path is refused, not hashed */
    const char *empty[] = {""};
    ASSERT_FALSE(cbm_ti_compatibility_digest(dir, empty, 1, "linux", other));
    th_rmtree(dir);
    PASS();
}

TEST(test_impact_artifact_file_digest_is_exact_bytes) {
    char *dir = th_mktempdir("cbm_tia_sha");
    ASSERT_NOT_NULL(dir);
    ASSERT_TRUE(tia_write(dir, "f", "abc"));
    char path[1024];
    snprintf(path, sizeof(path), "%s/f", dir);
    char hex[65];
    ASSERT_TRUE(cbm_ti_sha256_file(path, hex));
    ASSERT_STR_EQ(hex, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    snprintf(path, sizeof(path), "%s/absent", dir);
    ASSERT_FALSE(cbm_ti_sha256_file(path, hex));
    th_rmtree(dir);
    PASS();
}

SUITE(test_impact_artifact) {
    RUN_TEST(test_impact_artifact_receipt_round_trips);
    RUN_TEST(test_impact_artifact_receipt_is_strict);
    RUN_TEST(test_impact_artifact_compatibility_digest_binds_every_input);
    RUN_TEST(test_impact_artifact_file_digest_is_exact_bytes);
}
