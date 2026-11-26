#ifndef TEST_INVENTORY_FILTER_INTERNAL_H
#define TEST_INVENTORY_FILTER_INTERNAL_H
#include "test_framework.h"
#include "test_helpers.h"
#include "discover/inventory_filter.h"
#include "discover/discover.h"
#include "foundation/sha256.h"
#include "mcp/test_impact_tree.h"
#include <limits.h>
#include <stdint.h>
#define IF_CHECK(c)                                                                     \
    do {                                                                                \
        if (!(c)) {                                                                     \
            fprintf(stderr, "inventory assertion %s:%d: %s\n", __FILE__, __LINE__, #c); \
            goto done;                                                                  \
        }                                                                               \
    } while (0)
enum { IF_ROWS = 40, IF_PATH = 128, IF_BYTES = 512, IF_CALLS = 80 };
typedef struct {
    char root[4096], paths[IF_ROWS][IF_PATH];
    unsigned char bytes[IF_ROWS][IF_BYTES];
    cbm_inventory_file_t files[IF_ROWS];
    cbm_inventory_source_t source;
    cbm_inventory_limits_t limits;
    cbm_inventory_control_t control;
    cbm_inventory_filter_t *owner;
    size_t calls, indices[IF_CALLS], capacities[IF_CALLS];
    size_t fail_index;
    cbm_inventory_status_t returned, reported;
    bool cleanup, reply_bytes, overflowed;
    size_t reply_copied;
    cbm_pinned_tree_t *tree;
} if_fixture;
typedef struct {
    const char *path;
    cbm_inventory_filter_disposition_t disposition;
    cbm_inventory_filter_reason_t reason;
    unsigned roles;
    const char *ancestor;
} if_row;
typedef struct {
    cbm_inventory_control_kind_t kind;
    const char *directory;
    size_t index;
    cbm_inventory_control_outcome_t outcome;
    size_t patterns;
} if_control_row;
void if_init(if_fixture *f);
bool if_add(if_fixture *f, const char *path, const void *bytes, size_t size);
bool if_text(if_fixture *f, const char *path, const char *bytes);
bool if_prepare(if_fixture *f);
bool if_error(if_fixture *f, cbm_inventory_status_t expected, size_t index, bool cleanup);
/* Exact helper above includes SIZE_MAX. These explicit variants never accept an out-of-range row.
 */
bool if_error_known_row(if_fixture *f, cbm_inventory_status_t expected, bool cleanup);
/* Only for operations whose internal allocation/match phase may have no current row. */
bool if_error_during_work(if_fixture *f, cbm_inventory_status_t expected, bool cleanup);
int if_finish(if_fixture *f, int result);
bool if_rows(const if_fixture *f, const if_row *rows, size_t count);
bool if_controls(const if_fixture *f, const if_control_row *rows, size_t count);
bool if_reads(const if_fixture *f, const size_t *indices, size_t count);
void if_reset_reads(if_fixture *f);
bool if_dependency_control(void);
cbm_inventory_status_t if_read(void *, size_t, unsigned char *, size_t, size_t *,
                               const cbm_inventory_control_t *, cbm_inventory_error_t *);
int if_case_policy(void);
int if_case_controls(void);
int if_case_structure(void);
int if_case_namespace(void);
int if_case_required_arguments(void);
int if_case_provider(void);
int if_case_ownership(void);
int if_case_limits(void);
int if_case_cancel(void);
int if_case_native(void);
int if_case_native_faults(void);
#endif
