/*
 * test_platform.c — RED phase tests for foundation/platform.
 */
#include "test_framework.h"
#include "../src/foundation/compat.h" /* cbm_setenv / cbm_unsetenv (Windows-portable) */
#include "../src/foundation/platform.h"
#include "../src/foundation/system_info_internal.h"
#include <stdlib.h>
#include <unistd.h>

#ifdef __linux__
/* Linux-only cgroup tests need stdio for FILE*, stdlib for mkdtemp,
 * string for strncpy/strchr, sys/stat for mkdir, dirent for the
 * shell-free recursive teardown. */
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#endif

TEST(platform_now_ns) {
    uint64_t t1 = cbm_now_ns();
    ASSERT_GT(t1, 0);
    /* Busy-wait a tiny bit */
    for (volatile int i = 0; i < 100000; i++) {}
    uint64_t t2 = cbm_now_ns();
    ASSERT_GT(t2, t1);
    PASS();
}

TEST(platform_now_ms) {
    uint64_t t1 = cbm_now_ms();
    ASSERT_GT(t1, 0);
    PASS();
}

TEST(platform_nprocs) {
    int n = cbm_nprocs();
    ASSERT_GT(n, 0);
    ASSERT_LT(n, 10000); /* sanity */
    PASS();
}

TEST(platform_file_exists) {
    /* This test file should exist */
    ASSERT_TRUE(cbm_file_exists("tests/test_platform.c"));
    ASSERT_FALSE(cbm_file_exists("nonexistent_file_xyz.txt"));
    PASS();
}

TEST(platform_is_dir) {
    ASSERT_TRUE(cbm_is_dir("tests"));
    ASSERT_FALSE(cbm_is_dir("tests/test_platform.c"));
    ASSERT_FALSE(cbm_is_dir("nonexistent_dir"));
    PASS();
}

TEST(platform_file_size) {
    int64_t sz = cbm_file_size("tests/test_platform.c");
    ASSERT_GT(sz, 0);
    ASSERT_EQ(cbm_file_size("nonexistent_file_xyz.txt"), -1);
    PASS();
}

TEST(platform_mmap) {
    /* mmap this test file and verify first bytes */
    size_t sz = 0;
    void *data = cbm_mmap_read("tests/test_platform.c", &sz);
    ASSERT_NOT_NULL(data);
    ASSERT_GT(sz, 0);
    /* First line should be the comment */
    ASSERT(memcmp(data, "/*", 2) == 0);
    cbm_munmap(data, sz);
    PASS();
}

TEST(platform_mmap_nonexistent) {
    size_t sz = 0;
    void *data = cbm_mmap_read("nonexistent_xyz.txt", &sz);
    ASSERT_NULL(data);
    PASS();
}

/*
 * CBM_WORKERS env override for cbm_default_worker_count.
 *
 * Containers running cbm on a host with more CPUs than the cgroup's
 * effective quota currently see ~host_cpu workers spawned because
 * sysconf(_SC_NPROCESSORS_ONLN) is not cgroup-aware (see GitHub
 * issue for the cgroup-detection ask). CBM_WORKERS is the smaller,
 * explicit-override path that ships independently.
 */
TEST(platform_default_workers_env_override) {
    cbm_setenv("CBM_WORKERS", "4", 1);
    int n = cbm_default_worker_count(true);
    ASSERT_EQ(n, 4);
    /* initial=false should also honor the explicit override. */
    int m = cbm_default_worker_count(false);
    ASSERT_EQ(m, 4);
    cbm_unsetenv("CBM_WORKERS");
    PASS();
}

TEST(platform_default_workers_env_invalid) {
    /* Out-of-range values (< 1 or > 256) and non-numeric strings
     * fall back to the sysconf-derived default. */
    int baseline = cbm_default_worker_count(true);
    ASSERT_GT(baseline, 0);

    cbm_setenv("CBM_WORKERS", "0", 1);
    ASSERT_EQ(cbm_default_worker_count(true), baseline);

    cbm_setenv("CBM_WORKERS", "-1", 1);
    ASSERT_EQ(cbm_default_worker_count(true), baseline);

    cbm_setenv("CBM_WORKERS", "9999", 1);
    ASSERT_EQ(cbm_default_worker_count(true), baseline);

    cbm_setenv("CBM_WORKERS", "not-a-number", 1);
    ASSERT_EQ(cbm_default_worker_count(true), baseline);

    cbm_unsetenv("CBM_WORKERS");
    PASS();
}

TEST(platform_default_workers_env_unset) {
    /* When CBM_WORKERS is unset the result matches today's behaviour
     * (info.total_cores for initial=true, perf_cores-1 for false). */
    cbm_unsetenv("CBM_WORKERS");
    cbm_system_info_t info = cbm_system_info();
    ASSERT_EQ(cbm_default_worker_count(true), info.total_cores);
    PASS();
}

/* ── cgroup-aware detection (Linux only) ─────────────────────────── */

#ifdef __linux__

/* Create a unique tmp directory the caller will own; returns 0 on success. */
static int cgroup_test_setup(char *root, size_t root_sz) {
    strncpy(root, "/tmp/cbm_cgroup_test_XXXXXX", root_sz);
    return mkdtemp(root) != NULL ? 0 : -1;
}

/* Write `content` to "<root>/<relpath>". Creates parent subdir if needed.
 * Returns 0 on success, -1 on any failure. */
static int cgroup_test_write(const char *root, const char *relpath, const char *content) {
    char path[1024];
    const char *slash = strchr(relpath, '/');
    if (slash != NULL) {
        char subdir[1024];
        size_t n = (size_t)(slash - relpath);
        if (n >= sizeof(subdir)) {
            return -1;
        }
        memcpy(subdir, relpath, n);
        subdir[n] = '\0';
        snprintf(path, sizeof(path), "%s/%s", root, subdir);
        (void)mkdir(path, S_IRWXU);
    }
    snprintf(path, sizeof(path), "%s/%s", root, relpath);
    FILE *fp = fopen(path, "we");
    if (fp == NULL) {
        return -1;
    }
    size_t n = strlen(content);
    int rc = (fwrite(content, 1, n, fp) == n) ? 0 : -1;
    fclose(fp);
    return rc;
}

/* Recursively remove a tmp dir created by cgroup_test_setup. Best-effort.
 * Uses opendir/unlink/rmdir rather than system("rm -rf ...") to avoid
 * spawning a shell from the test binary. */
static void cgroup_test_teardown(const char *root) {
    DIR *d = opendir(root);
    if (d != NULL) {
        struct dirent *ent;
        while ((ent = readdir(d)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) {
                continue;
            }
            char child[1024];
            snprintf(child, sizeof(child), "%s/%s", root, ent->d_name);
            struct stat st;
            if (stat(child, &st) == 0 && S_ISDIR(st.st_mode)) {
                cgroup_test_teardown(child); /* recurse into subdir */
            } else {
                (void)unlink(child);
            }
        }
        closedir(d);
    }
    (void)rmdir(root);
}

TEST(cgroup_v2_cpu_quota) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* 200ms quota in a 100ms period → 2 effective CPUs. */
    ASSERT_EQ(cgroup_test_write(root, "cpu.max", "200000 100000\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), 2);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v2_cpu_quota_rounds_up) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* 150ms quota / 100ms period = 1.5 → ceil = 2. */
    ASSERT_EQ(cgroup_test_write(root, "cpu.max", "150000 100000\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), 2);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v2_cpu_unlimited) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    ASSERT_EQ(cgroup_test_write(root, "cpu.max", "max 100000\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), -1);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v1_cpu_quota) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    ASSERT_EQ(cgroup_test_write(root, "cpu/cpu.cfs_quota_us", "200000"), 0);
    ASSERT_EQ(cgroup_test_write(root, "cpu/cpu.cfs_period_us", "100000"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), 2);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v1_cpu_unlimited) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* quota=-1 is the cgroup-v1 sentinel for "no quota". */
    ASSERT_EQ(cgroup_test_write(root, "cpu/cpu.cfs_quota_us", "-1"), 0);
    ASSERT_EQ(cgroup_test_write(root, "cpu/cpu.cfs_period_us", "100000"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), -1);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_no_cpu_files) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* Empty tmp dir: no v2 file, no v1 file → fall through to sysconf. */
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), -1);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v2_mem) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* 2 GiB. */
    ASSERT_EQ(cgroup_test_write(root, "memory.max", "2147483648\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)2147483648UL);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v2_mem_unlimited) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    ASSERT_EQ(cgroup_test_write(root, "memory.max", "max\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)0);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v1_mem) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* 1 GiB. */
    ASSERT_EQ(cgroup_test_write(root, "memory/memory.limit_in_bytes", "1073741824"), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)1073741824UL);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v1_mem_unlimited_sentinel) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* cgroup v1 reports a huge near-ULLONG_MAX value when unlimited
     * (PAGE_COUNTER_MAX). Our parser treats anything >= ULLONG_MAX/2
     * as effectively unlimited. */
    ASSERT_EQ(cgroup_test_write(root, "memory/memory.limit_in_bytes", "9223372036854775807"), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)0);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_no_mem_files) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)0);
    cgroup_test_teardown(root);
    PASS();
}

#endif /* __linux__ */

SUITE(platform) {
    RUN_TEST(platform_now_ns);
    RUN_TEST(platform_now_ms);
    RUN_TEST(platform_nprocs);
    RUN_TEST(platform_file_exists);
    RUN_TEST(platform_is_dir);
    RUN_TEST(platform_file_size);
    RUN_TEST(platform_mmap);
    RUN_TEST(platform_mmap_nonexistent);
    RUN_TEST(platform_default_workers_env_override);
    RUN_TEST(platform_default_workers_env_invalid);
    RUN_TEST(platform_default_workers_env_unset);
#ifdef __linux__
    RUN_TEST(cgroup_v2_cpu_quota);
    RUN_TEST(cgroup_v2_cpu_quota_rounds_up);
    RUN_TEST(cgroup_v2_cpu_unlimited);
    RUN_TEST(cgroup_v1_cpu_quota);
    RUN_TEST(cgroup_v1_cpu_unlimited);
    RUN_TEST(cgroup_no_cpu_files);
    RUN_TEST(cgroup_v2_mem);
    RUN_TEST(cgroup_v2_mem_unlimited);
    RUN_TEST(cgroup_v1_mem);
    RUN_TEST(cgroup_v1_mem_unlimited_sentinel);
    RUN_TEST(cgroup_no_mem_files);
#endif
}
