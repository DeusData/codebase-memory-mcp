#ifndef CBM_WATCHER_SVN_STATE_H
#define CBM_WATCHER_SVN_STATE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#ifdef CBM_SVN_STATE_ENABLE_TEST_API
#include <stdio.h>
#endif

#include "foundation/constants.h"

typedef enum {
    CBM_SVN_PROBE_UNCERTAIN = -1,
    CBM_SVN_PROBE_NOT_WORKING_COPY = 0,
    CBM_SVN_PROBE_OK = 1,
} cbm_svn_probe_result_t;

typedef struct {
    char executable[CBM_SZ_4K];
    bool uses_posix_paths;
} cbm_svn_client_t;

typedef struct {
    uint64_t semantic_signature;
    uint64_t content_signature;
    uint64_t bytes_hashed;
    bool has_local_changes;
    int entry_count;
    int candidate_count;
} cbm_svn_observation_t;

/* Resolve and pin a system SVN client without executing project-local code. */
cbm_svn_probe_result_t cbm_svn_client_init(const char *root_path, cbm_svn_client_t *client);

/* Format a local path for the pinned SVN client's runtime. */
bool cbm_svn_format_path_arg(const cbm_svn_client_t *client, const char *path, char *out,
                             size_t out_size);

/* Parse one local `svn status --xml --verbose` stream. */
#ifdef CBM_SVN_STATE_ENABLE_TEST_API
cbm_svn_probe_result_t cbm_svn_parse_status_stream(FILE *stream, const char *root_path,
                                                   cbm_svn_observation_t *observation);
bool cbm_svn_test_executable_uses_posix_paths(const char *executable);
#endif

/* Observe a working copy with one local, non-interactive SVN process. */
cbm_svn_probe_result_t cbm_svn_probe(const cbm_svn_client_t *client, const char *root_path,
                                     cbm_svn_observation_t *observation);

#endif /* CBM_WATCHER_SVN_STATE_H */
