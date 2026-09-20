/* Project cache inspection and conditional collection. */
#ifndef CBM_CLI_CACHE_H
#define CBM_CLI_CACHE_H

#include <stdbool.h>

typedef struct {
    void *context;
    bool (*cancelled)(void *context);
    bool (*try_begin)(void *context, const char *project);
    void (*end)(void *context, const char *project);
    /* Called under the lease, after eligibility has been rechecked. */
    void (*before_delete)(void *context, const char *project);
    void (*after_delete)(void *context, const char *project);
    /* Optional fault seam; production uses cbm_unlink. */
    int (*unlink_file)(void *context, const char *path);
} cbm_cache_ops_t;

/* Returns malloc-owned JSON. Stats and dry runs never invoke deletion callbacks.
 * Real pruning requires a complete mutation guard. Conditions are conjunctive. */
char *cbm_cache_run(const char *directory, const char *args, bool prune, const cbm_cache_ops_t *ops,
                    bool *is_error);

/* Strict CLI flags; no dependency on MCP tool schemas. Returns malloc-owned JSON. */
char *cbm_cache_cli_args(bool prune, int argc, char **argv);

#endif
