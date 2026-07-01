#ifndef CBM_GIT_SNAPSHOT_H
#define CBM_GIT_SNAPSHOT_H

#include <stdbool.h>

#include "foundation/constants.h"

enum {
    CBM_GIT_DIRTY_HASH_HEX_LEN = 16,
    CBM_GIT_DIRTY_HASH_BUFSZ = CBM_GIT_DIRTY_HASH_HEX_LEN + 1,
};

typedef enum {
    CBM_GIT_SNAPSHOT_HEAD = 1u << 0,
    CBM_GIT_SNAPSHOT_DIRTY = 1u << 1,
    CBM_GIT_SNAPSHOT_FILE_COUNT = 1u << 2,
} cbm_git_snapshot_flags_t;

typedef struct {
    bool path_supported;
    bool is_git;
    int dirty_bytes;
    int file_count;
    char head[CBM_SZ_64];
    char dirty_hash[CBM_GIT_DIRTY_HASH_BUFSZ];
} cbm_git_snapshot_t;

bool cbm_git_snapshot_path_supported(const char *repo_path);
int cbm_git_snapshot_read(const char *repo_path, unsigned flags, cbm_git_snapshot_t *out);

#endif
