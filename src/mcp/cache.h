/* Project cache inspection and conditional collection. */
#ifndef CBM_MCP_CACHE_H
#define CBM_MCP_CACHE_H

#include <stdbool.h>

typedef struct {
    void *context;
    bool (*try_begin)(void *context, const char *project);
    void (*end)(void *context, const char *project);
    /* Called under the lease, after eligibility has been rechecked. */
    void (*before_delete)(void *context, const char *project);
    void (*after_delete)(void *context, const char *project);
} cbm_cache_ops_t;

/* Returns malloc-owned JSON. Stats and dry runs never invoke deletion callbacks.
 * Real pruning requires a complete mutation guard. Conditions are conjunctive. */
char *cbm_cache_run(const char *directory, const char *args, bool prune, const cbm_cache_ops_t *ops,
                    bool *is_error);

#endif
