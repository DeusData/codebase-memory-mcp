#ifndef CBM_INVENTORY_INTERNAL_H
#define CBM_INVENTORY_INTERNAL_H

#include "discover/inventory_filter.h"
#include "discover/gitignore_checked.h"
#include "foundation/arena.h"

enum { CIF_PATH_MAX = 4095, CIF_BYTE_GAP = 65536, CIF_EVENT_GAP = 4096, CIF_COPY_CHUNK = 4096 };

typedef struct {
    cbm_inventory_path_t path;
    size_t parent, next, first_directory, last_directory, first_file, last_file;
    size_t excluded;
    cbm_inventory_filter_reason_t reason;
    const cbm_ignore_program_t *program;
} cif_directory;

struct cbm_inventory_filter {
    CBMArena arena;
    cbm_inventory_limits_t limits;
    uint64_t deadline_ms;
    cbm_inventory_filter_usage_t usage;
    cbm_inventory_filter_view_t view;
    cbm_inventory_file_t *files;
    cbm_inventory_filter_row_t *rows;
    cif_directory *directories;
    size_t *file_next;
    cbm_inventory_control_row_t *controls;
    cbm_inventory_control_row_t cbm_control;
    cbm_inventory_path_t *exclusions;
    cbm_ignore_checked_t *ignore;
    const cbm_ignore_program_t *cbm_program;
    unsigned char *scratch;
    size_t scratch_capacity;
};

/* Only this synchronous stack context borrows caller functions and contexts. */
typedef struct {
    cbm_inventory_filter_t *owner;
    const cbm_inventory_source_t *source;
    const cbm_inventory_control_t *control;
    cbm_inventory_error_t error;
    size_t file_index, byte_gap, event_gap;
} cif_context;

bool cif_fail(cif_context *c, cbm_inventory_status_t status);
bool cif_poll(cif_context *c);
bool cif_bytes(cif_context *c, size_t count);
bool cif_event(cif_context *c);
bool cif_copy(cif_context *c, void *destination, const void *source, size_t length);
void *cif_alloc(cif_context *c, size_t count, size_t width);
unsigned char *cif_string(cif_context *c, const unsigned char *source, size_t length);
bool cif_compare(cif_context *c, cbm_inventory_path_t a, cbm_inventory_path_t b, int *result);
bool cif_checked(cif_context *c, cbm_ignore_checked_status_t status);
cbm_ignore_checked_control_t cif_ignore_control(const cif_context *c);
bool cif_metadata(cif_context *c);
bool cif_directories(cif_context *c);
bool cif_find_file(cif_context *c, cbm_inventory_path_t path, size_t *index);
bool cif_find_directory(cif_context *c, cbm_inventory_path_t path, size_t *index);
bool cif_load_controls(cif_context *c, size_t directory);
bool cif_filter(cif_context *c);

#endif
