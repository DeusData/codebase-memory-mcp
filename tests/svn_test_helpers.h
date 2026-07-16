#ifndef SVN_TEST_HELPERS_H
#define SVN_TEST_HELPERS_H

#include "../src/foundation/platform.h"
#include "../src/watcher/svn_state.h"
#include "test_helpers.h"

typedef struct {
    char root[CBM_SZ_4K];
    char repository[CBM_SZ_4K];
    char working_copy[CBM_SZ_4K];
    char source[CBM_SZ_4K];
    cbm_svn_client_t client;
} th_svn_fixture_t;

static inline void th_svn_fixture_cleanup(th_svn_fixture_t *fixture) {
    if (fixture) {
        th_cleanup(fixture->root);
    }
}

static inline int th_svn_fixture_init(th_svn_fixture_t *fixture, const char *prefix,
                                      const char *source_name, const char *source_content) {
    if (!fixture || !prefix) {
        return -1;
    }
    memset(fixture, 0, sizeof(*fixture));
    char *root = th_mktempdir(prefix);
    if (!root) {
        return -1;
    }
    snprintf(fixture->root, sizeof(fixture->root), "%s", root);
    snprintf(fixture->repository, sizeof(fixture->repository), "%s/repository", fixture->root);
    snprintf(fixture->working_copy, sizeof(fixture->working_copy), "%s/working-copy",
             fixture->root);
    if (cbm_svn_client_init(fixture->root, &fixture->client) != CBM_SVN_PROBE_OK) {
        th_svn_fixture_cleanup(fixture);
        return -1;
    }

    char svnadmin[CBM_SZ_4K];
    snprintf(svnadmin, sizeof(svnadmin), "%s", fixture->client.executable);
    cbm_normalize_path_sep(svnadmin);
    char *slash = strrchr(svnadmin, '/');
    if (!slash) {
        th_svn_fixture_cleanup(fixture);
        return -1;
    }
#ifdef _WIN32
    snprintf(slash + 1, (size_t)(svnadmin + sizeof(svnadmin) - slash - 1), "svnadmin.exe");
#else
    snprintf(slash + 1, (size_t)(svnadmin + sizeof(svnadmin) - slash - 1), "svnadmin");
#endif
    const char *create_args[] = {svnadmin, "create", fixture->repository, NULL};
    if (cbm_exec_no_shell(create_args) != 0) {
        th_svn_fixture_cleanup(fixture);
        return -1;
    }

    char normalized_repository[CBM_SZ_4K];
    snprintf(normalized_repository, sizeof(normalized_repository), "%s", fixture->repository);
    cbm_normalize_path_sep(normalized_repository);
    char repository_url[CBM_SZ_8K];
#ifdef _WIN32
    snprintf(repository_url, sizeof(repository_url), "file:///%s", normalized_repository);
#else
    snprintf(repository_url, sizeof(repository_url), "file://%s", normalized_repository);
#endif
    const char *checkout_args[] = {fixture->client.executable, "checkout", "--non-interactive",
                                   repository_url, fixture->working_copy, NULL};
    if (cbm_exec_no_shell(checkout_args) != 0) {
        th_svn_fixture_cleanup(fixture);
        return -1;
    }
    if (!source_name) {
        return 0;
    }

    snprintf(fixture->source, sizeof(fixture->source), "%s/%s", fixture->working_copy,
             source_name);
    if (th_write_file(fixture->source, source_content) != 0) {
        th_svn_fixture_cleanup(fixture);
        return -1;
    }
    const char *add_args[] = {fixture->client.executable, "add", "--non-interactive",
                              fixture->source, NULL};
    const char *commit_args[] = {fixture->client.executable, "commit", "--non-interactive", "-m",
                                 "initial", fixture->working_copy, NULL};
    if (cbm_exec_no_shell(add_args) != 0 || cbm_exec_no_shell(commit_args) != 0) {
        th_svn_fixture_cleanup(fixture);
        return -1;
    }
    return 0;
}

#endif /* SVN_TEST_HELPERS_H */
