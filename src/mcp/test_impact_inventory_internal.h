#ifndef CBM_TEST_IMPACT_INVENTORY_INTERNAL_H
#define CBM_TEST_IMPACT_INVENTORY_INTERNAL_H

#include "mcp/test_impact_inventory.h"
#include "foundation/arena.h"

enum { CNI_PATH_MAX = 4095, CNI_BYTE_GAP = 65536, CNI_EVENT_GAP = 4096, CNI_COPY_CHUNK = 4096 };

struct cbm_test_impact_inventory {
    CBMArena arena;
    cbm_inventory_filter_t *filter;
    cbm_inventory_limits_t limits;
    uint64_t deadline_ms;
    cbm_test_impact_inventory_usage_t usage;
};

/* All borrowed values and temporary records are confined to this synchronous call. */
typedef struct {
    cbm_test_impact_inventory_t *owner;
    CBMArena temporary;
    const cbm_inventory_control_t *control;
    cbm_inventory_error_t error;
    size_t file_index, byte_gap, event_gap;
} cni_context;

typedef struct {
    cni_context *context;
    cbm_pinned_tree_t *tree;
    const cbm_inventory_file_t *files;
    size_t file_count;
    uint64_t content_bound;
} cni_reader;

bool cni_fail(cni_context *c, cbm_inventory_status_t status);
bool cni_poll(cni_context *c);
bool cni_bytes(cni_context *c, size_t count);
bool cni_event(cni_context *c);
bool cni_copy(cni_context *c, void *out, const void *in, size_t length);
void *cni_alloc(cni_context *c, CBMArena *arena, size_t count, size_t width);
bool cni_binding(cni_context *c, const cbm_pinned_tree_view_t *view);
bool cni_snapshot(cni_context *c, const cbm_pinned_tree_view_t *view,
                  cbm_inventory_source_t *source);
cbm_inventory_status_t cni_read(void *context, size_t index, unsigned char *prefix, size_t capacity,
                                size_t *copied, const cbm_inventory_control_t *control,
                                cbm_inventory_error_t *error);

#endif
