/* Reproduction for temporary directories leaked by repro_harness failures. */
#include "test_framework.h"
#include "repro_harness.h"

#include <stdio.h>
#include <sys/stat.h>

static int cleanup_path_exists(const char *path) {
    struct stat st;
    return path && path[0] && stat(path, &st) == 0;
}

static int cleanup_write_sentinel(const char *path) {
    FILE *file = cbm_fopen(path, "wb");
    if (!file) {
        return 0;
    }
    int wrote = fputs("sentinel\n", file) >= 0;
    int closed = fclose(file) == 0;
    return wrote && closed;
}

TEST(repro_harness_cleans_temp_project_after_fixture_write_failure) {
    RProj project;
    const RFile invalid_file = {
        "", /* Resolves to the fresh project directory, so fopen must fail. */
        "fixture content",
    };

    cbm_store_t *store = rh_index_files(&project, &invalid_file, 1);

    char tmpdir[sizeof(project.tmpdir)];
    char cachedir[sizeof(project.cachedir)];
    snprintf(tmpdir, sizeof(tmpdir), "%s", project.tmpdir);
    snprintf(cachedir, sizeof(cachedir), "%s", project.cachedir);

    int temp_project_was_created = tmpdir[0] != '\0';
    int tmpdir_remained = cleanup_path_exists(tmpdir);
    int cachedir_remained = cleanup_path_exists(cachedir);

    /* This failure happens before dbpath is initialized. Keep rh_cleanup's WAL
     * cleanup absolute so the RED guard cannot unlink a relative workspace file. */
    if (!project.dbpath[0]) {
        snprintf(project.dbpath, sizeof(project.dbpath), "/tmp/cbm_repro_cleanup_guard_%ld.db",
                 (long)getpid());
    }
    rh_cleanup(&project, store);

    int explicit_cleanup_left_tmpdir = cleanup_path_exists(tmpdir);
    int explicit_cleanup_left_cachedir = cleanup_path_exists(cachedir);

    if (store != NULL) {
        fprintf(stderr, "  [repro-harness-cleanup] invariant=invalid_fixture_was_indexed\n");
        FAIL("invalid fixture path must make rh_index_files return NULL");
    }
    if (!temp_project_was_created) {
        fprintf(stderr,
                "  [repro-harness-cleanup] invariant=temp_project_not_created_before_failure\n");
        FAIL("test did not reach the post-mkdtemp fixture-write failure path");
    }
    if (explicit_cleanup_left_tmpdir || explicit_cleanup_left_cachedir) {
        fprintf(stderr,
                "  [repro-harness-cleanup] invariant=test_cleanup_failed tmpdir=%d cachedir=%d\n",
                explicit_cleanup_left_tmpdir, explicit_cleanup_left_cachedir);
        FAIL("rh_cleanup did not remove the test's temporary directories");
    }
    if (tmpdir_remained || cachedir_remained) {
        fprintf(stderr,
                "  [repro-harness-cleanup] invariant=helper_returned_with_temp_state "
                "tmpdir=%d cachedir=%d\n",
                tmpdir_remained, cachedir_remained);
        FAIL("rh_index_files must clean temporary directories before returning NULL");
    }

    PASS();
}

TEST(repro_harness_cleanup_empty_dbpath_preserves_relative_sidecars) {
    /* Empty dbpath must be a no-op for sidecars, never a cwd-relative unlink. */
    char original_cwd[1024];
    char private_dir[256] = "/tmp/cbm_repro_empty_dbpath_XXXXXX";
    _Static_assert(sizeof(private_dir) >= 256,
                   "cbm_mkdtemp requires a 256-byte caller buffer on Windows");
    int entered_private_dir = 0;
    int wal_created = 0;
    int shm_created = 0;
    int wal_survived = 0;
    int shm_survived = 0;

    if (!getcwd(original_cwd, sizeof(original_cwd))) {
        FAIL("test could not record the original working directory");
    }
    if (!cbm_mkdtemp(private_dir)) {
        FAIL("test could not create a private temporary working directory");
    }

    entered_private_dir = chdir(private_dir) == 0;
    if (entered_private_dir) {
        wal_created = cleanup_write_sentinel("-wal");
        shm_created = cleanup_write_sentinel("-shm");
        if (wal_created && shm_created) {
            RProj project = {0};
            rh_cleanup(&project, NULL);
            wal_survived = cleanup_path_exists("-wal");
            shm_survived = cleanup_path_exists("-shm");
        }
    }

    int restored_cwd = chdir(original_cwd) == 0;
    int removed_private_dir = th_rmtree(private_dir) == 0;

    if (!entered_private_dir || !wal_created || !shm_created) {
        fprintf(stderr,
                "  [repro-harness-cleanup] invariant=sentinel_setup_failed entered=%d "
                "wal_created=%d shm_created=%d\n",
                entered_private_dir, wal_created, shm_created);
        FAIL("test could not establish the empty-dbpath sidecar preconditions");
    }
    if (!restored_cwd || !removed_private_dir) {
        fprintf(stderr,
                "  [repro-harness-cleanup] invariant=test_cleanup_failed restored_cwd=%d "
                "removed_private_dir=%d\n",
                restored_cwd, removed_private_dir);
        FAIL("test did not restore and remove its private working directory");
    }
    if (!wal_survived || !shm_survived) {
        fprintf(stderr,
                "  [repro-harness-cleanup] invariant=empty_dbpath_unlinked_relative_sidecar "
                "wal_survived=%d shm_survived=%d\n",
                wal_survived, shm_survived);
        FAIL("rh_cleanup with an empty dbpath must not unlink relative -wal/-shm files");
    }

    PASS();
}

SUITE(repro_harness_cleanup) {
    RUN_TEST(repro_harness_cleans_temp_project_after_fixture_write_failure);
    RUN_TEST(repro_harness_cleanup_empty_dbpath_preserves_relative_sidecars);
}
