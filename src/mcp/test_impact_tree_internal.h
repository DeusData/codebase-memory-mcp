#ifndef CBM_TEST_IMPACT_TREE_INTERNAL_H
#define CBM_TEST_IMPACT_TREE_INTERNAL_H

#ifdef _WIN32
/* FILE_ID_INFO (stable 128-bit file identity) is a Windows 8 API; pin it
 * before any system header so toolchains with an older default see it. */
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0602
#endif
#endif
#ifndef _WIN32
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE
#endif
#endif

#include "mcp/test_impact_tree.h"
#include "foundation/arena.h"
#include "foundation/sha256.h"
#include <string.h>
#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
typedef HANDLE tpt_handle;
#define TPT_INVALID_HANDLE INVALID_HANDLE_VALUE
typedef struct {
    ULONGLONG volume;
    unsigned char file[16];
} tpt_identity;
#else
#include <sys/types.h>
#include <dirent.h>
typedef int tpt_handle;
#define TPT_INVALID_HANDLE (-1)
typedef struct {
    dev_t device;
    ino_t inode;
} tpt_identity;
#endif

enum { TPT_PATH_CAP = 4096, TPT_CHUNK = 65536, TPT_SECURITY_CAP = 65536 };
typedef struct {
    const char *path;
    size_t length, parent, file;
    bool directory, created, identified, seen;
    tpt_identity identity;
    tpt_handle handle;
} tpt_object;

struct cbm_pinned_tree {
    CBMArena arena;
    size_t allocated;
    cbm_pinned_tree_limits_t limits;
    cbm_pinned_tree_view_t view;
    cbm_git_facts_identity_t identity;
    cbm_pinned_tree_file_t *files;
    tpt_object *objects;
    size_t object_count;
    size_t *indices, *ancestors;
    tpt_object parent;
    char *parent_path, *supplied_parent, *root_path, *path_scratch, *name_scratch;
    unsigned char *io_scratch, *security_scratch, *enumeration_scratch;
    char basename[42];
    bool ready, close_uncertain;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    cbm_pinned_tree_read_fault_t read_fault;
#endif
#ifdef _WIN32
    wchar_t *wide_path, *wide_parent, *wide_root;
    HANDLE token, enumeration, probe, impersonation;
    void *token_user;
    SECURITY_DESCRIPTOR descriptor;
    ACL *owned_acl;
    SECURITY_ATTRIBUTES attributes;
#else
    DIR *enumeration;
    int enumeration_fd;
    void *pending_acl;
    uid_t uid;
#endif
};

typedef struct {
    cbm_pinned_tree_t *tree;
    const cbm_pinned_tree_control_t *control;
    cbm_pinned_tree_error_t error;
    bool building;
    bool close_failed; /* passive observation; prefix read must not retry that close */
} tpt_context;

typedef struct {
    const cbm_git_bytes_t *expected; /* construction readback only */
    unsigned char *prefix;
    size_t capacity;
    bool bounded;               /* exact remaining length plus one-byte EOF probe */
    bool read_fault, eof_fault; /* one call-local simulation, otherwise false */
} tpt_stream_options;

bool tpt_fail(tpt_context *c, cbm_pinned_tree_status_t status, const char *message);
void tpt_error_init(cbm_pinned_tree_error_t *error);
bool tpt_poll(tpt_context *c);
bool tpt_after_facts(tpt_context *c, bool ok, const cbm_git_facts_error_t *error);
void *tpt_alloc(tpt_context *c, size_t count, size_t size);
bool tpt_copy(tpt_context *c, void *target, const void *source, size_t size);
bool tpt_equal(tpt_context *c, const void *a, const void *b, size_t size);
bool tpt_length(tpt_context *c, const char *s, size_t max, size_t *out);
char *tpt_string(tpt_context *c, const char *s, size_t size);
bool tpt_oid(tpt_context *c, const char *oid, unsigned width);
int tpt_compare(tpt_context *c, const unsigned char *a, size_t an, const unsigned char *b,
                size_t bn);
bool tpt_hash(tpt_context *c, cbm_sha256_ctx *hash, const void *bytes, size_t length);
bool tpt_options_valid(const cbm_pinned_tree_options_t *o);
bool tpt_plan(tpt_context *c, const cbm_pinned_tree_options_t *o,
              const cbm_git_tree_inventory_t *inventory);
bool tpt_identity_copy(tpt_context *c, const cbm_pinned_tree_options_t *o);
bool tpt_batch(tpt_context *c, const cbm_pinned_tree_options_t *o, cbm_git_blob_batch_t *out);
bool tpt_manifest(tpt_context *c);
bool tpt_path_valid(tpt_context *c, const unsigned char *path, size_t length);
bool tpt_parent_syntax(tpt_context *c, const char *path);
const char *tpt_leaf(const tpt_object *object);
tpt_object *tpt_parent(cbm_pinned_tree_t *tree, const tpt_object *object);
bool tpt_join(tpt_context *c, const tpt_object *object);
bool tpt_chain(tpt_context *c, const tpt_object *object);
bool tpt_native_prepare(tpt_context *c);
bool tpt_native_create(tpt_context *c, tpt_object *object);
bool tpt_native_check(tpt_context *c, tpt_object *object, bool writing);
bool tpt_native_identity(tpt_context *c, tpt_object *object, bool *absent);
bool tpt_native_write(tpt_context *c, tpt_object *object, cbm_git_bytes_t bytes);
bool tpt_native_read(tpt_context *c, tpt_object *object, size_t *read_count);
bool tpt_native_read_bounded(tpt_context *c, tpt_object *object, size_t request,
                             size_t *read_count);
bool tpt_native_finish(tpt_context *c, tpt_object *object);
bool tpt_native_close(tpt_context *c, tpt_handle *handle);
bool tpt_native_enumerate(tpt_context *c, size_t directory);
bool tpt_native_remove(tpt_context *c, tpt_object *object);
bool tpt_native_release(tpt_context *c);
bool tpt_native_acl(tpt_context *c, tpt_object *object, unsigned mode, bool changed);
bool tpt_native_id_equal(tpt_identity a, tpt_identity b);
bool tpt_record_identity(tpt_context *c, tpt_object *object, tpt_identity identity);
bool tpt_readback(tpt_context *c, tpt_object *object, const cbm_git_bytes_t *expected);
/* File already opened/checked at offset zero; never closes or publishes copied. */
bool tpt_read_stream(tpt_context *c, tpt_object *object, const tpt_stream_options *options);
bool tpt_audit(tpt_context *c);
bool tpt_entry(tpt_context *c, size_t directory, const char *name, size_t length);
bool tpt_cleanup(tpt_context *c);
bool tpt_absent_disposable(tpt_context *c, const tpt_object *object);
bool tpt_build(tpt_context *c, const cbm_git_blob_batch_t *batch);
void tpt_dispose_memory(cbm_pinned_tree_t *tree);
#ifdef _WIN32
bool tpt_wide(tpt_context *c, const char *source, wchar_t *target);
bool tpt_utf8(tpt_context *c, const wchar_t *source, char *target);
bool tpt_win_security(tpt_context *c);
bool tpt_win_effective(tpt_context *c);
bool tpt_win_path(tpt_context *c, const tpt_object *object);
bool tpt_win_error(tpt_context *c, DWORD code, bool creating);
#else
bool tpt_posix_error(tpt_context *c, int code, bool creating);
#endif
#endif
