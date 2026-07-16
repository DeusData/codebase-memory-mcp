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
    char repository_arg[CBM_SZ_4K];
    char working_copy_arg[CBM_SZ_4K];
    if (!cbm_svn_format_path_arg(&fixture->client, fixture->repository, repository_arg,
                                 sizeof(repository_arg)) ||
        !cbm_svn_format_path_arg(&fixture->client, fixture->working_copy, working_copy_arg,
                                 sizeof(working_copy_arg))) {
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
    const char *create_args[] = {svnadmin, "create", repository_arg, NULL};
    if (cbm_exec_no_shell(create_args) != 0) {
        th_svn_fixture_cleanup(fixture);
        return -1;
    }

    char repository_url[CBM_SZ_8K];
#ifdef _WIN32
    const char *file_prefix = fixture->client.uses_posix_paths ? "file://" : "file:///";
#else
    const char *file_prefix = "file://";
#endif
    int url_length =
        snprintf(repository_url, sizeof(repository_url), "%s%s", file_prefix, repository_arg);
    if (url_length < 0 || (size_t)url_length >= sizeof(repository_url)) {
        th_svn_fixture_cleanup(fixture);
        return -1;
    }
    const char *checkout_args[] = {fixture->client.executable, "checkout",
                                   "--non-interactive",        repository_url,
                                   working_copy_arg,           NULL};
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
    char source_arg[CBM_SZ_4K];
    if (!cbm_svn_format_path_arg(&fixture->client, fixture->source, source_arg,
                                 sizeof(source_arg))) {
        th_svn_fixture_cleanup(fixture);
        return -1;
    }
    const char *add_args[] = {fixture->client.executable, "add", "--non-interactive", source_arg,
                              NULL};
    const char *commit_args[] = {
        fixture->client.executable, "commit", "--non-interactive", "-m", "initial",
        working_copy_arg,           NULL};
    if (cbm_exec_no_shell(add_args) != 0 || cbm_exec_no_shell(commit_args) != 0) {
        th_svn_fixture_cleanup(fixture);
        return -1;
    }
    return 0;
}

#endif /* SVN_TEST_HELPERS_H */
