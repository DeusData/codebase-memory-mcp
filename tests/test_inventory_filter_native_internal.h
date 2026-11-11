#ifndef TEST_INVENTORY_FILTER_NATIVE_INTERNAL_H
#define TEST_INVENTORY_FILTER_NATIVE_INTERNAL_H
#include "test_inventory_filter_internal.h"
#include "if_native_git_path.h"
#include "foundation/subprocess.h"
enum { IF_NATIVE_PATH = 4096 };
typedef struct {
    if_fixture input;
    char home[IF_NATIVE_PATH], repo[IF_NATIVE_PATH], parent[IF_NATIVE_PATH];
    char git[IF_NATIVE_PATH], capture[IF_NATIVE_PATH], log[IF_NATIVE_PATH], payload[IF_NATIVE_PATH];
    char head[65];
    cbm_git_facts_t *facts;
    bool unquiesced, close_attempted;
} if_native;
/* Locator returns a candidate; genuine Git commands remain the positive control. */
_Noreturn void if_native_stop(const if_native *n, const char *reason);
bool if_native_start(if_native *n);
bool if_native_tree(if_native *n);
bool if_native_dependency(if_native *n);
bool if_native_close(if_native *n);
int if_native_finish(if_native *n, int result);
bool if_native_join(char *out, const char *base, const char *rel);
bool if_native_private_parent(if_native *n);
bool if_native_git(if_native *n, const char *const *tail, char *out, size_t capacity);
#endif
