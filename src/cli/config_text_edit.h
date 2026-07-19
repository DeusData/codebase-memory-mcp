/*
 * config_text_edit.h — Safe edits for managed instruction text.
 */
#ifndef CBM_CLI_CONFIG_TEXT_EDIT_H
#define CBM_CLI_CONFIG_TEXT_EDIT_H

#include <stddef.h>

int cbm_text_upsert_managed_block(const char *file_path, const char *begin_marker,
                                  const char *end_marker, const char *owned_content);
int cbm_text_upsert_managed_block_limited(const char *file_path, const char *begin_marker,
                                          const char *end_marker, const char *owned_content,
                                          size_t max_document_bytes);
int cbm_text_remove_managed_block(const char *file_path, const char *begin_marker,
                                  const char *end_marker);

/* Whole-document writes revalidate both destination and synced temporary-file
 * identity/bytes immediately before publication. Portable platforms without
 * path-identity CAS retain a narrow interval before replacement or deletion. */
int cbm_text_write_owned_document(const char *file_path, const char *owned_content);
/* Replace one whole document only when its current bytes still equal the
 * caller's snapshot. expected_content == NULL means the caller observed a
 * missing file and expected_length must be zero. */
int cbm_text_write_owned_document_if_unchanged(const char *file_path, const char *owned_content,
                                               const char *expected_content,
                                               size_t expected_length);
int cbm_text_create_owned_document(const char *file_path, const char *owned_content);
int cbm_text_ensure_owned_document(const char *file_path, const char *owned_content);
/* Create current content, preserve it when already current, or atomically
 * upgrade only an exact byte-for-byte previously released document. Returns
 * 1 for user-modified/unowned content. */
int cbm_text_migrate_owned_document(const char *file_path, const char *current_content,
                                    const char *const *released_contents, size_t released_count);
/* As above, but publish the owned document with the requested low permission
 * bits as part of the same atomic replacement. The mode must include owner-read
 * permission. Existing POSIX documents must be owned by the effective user.
 * On Windows the mode is validated but otherwise ignored because command
 * scripts do not use POSIX execute bits. */
int cbm_text_migrate_owned_document_mode(const char *file_path, const char *current_content,
                                         const char *const *released_contents,
                                         size_t released_count, unsigned int mode);
/* Returns 0 when removed/missing, 1 when a regular document exists but is not
 * byte-for-byte owned by the caller, and -1 for unsafe state or I/O failure. */
int cbm_text_remove_owned_document(const char *file_path, const char *expected_owned_content);
/* Remove current or any exact released document; preserve all other bytes. */
int cbm_text_remove_owned_document_any(const char *file_path, const char *current_content,
                                       const char *const *released_contents, size_t released_count);

#ifdef CBM_TEXT_EDIT_ENABLE_TEST_API
typedef void (*cbm_text_precommit_test_hook_t)(const char *file_path, void *context);
void cbm_text_set_precommit_hook_for_testing(cbm_text_precommit_test_hook_t hook, void *context);
/* Runs after the first stale-snapshot check and before the final identity
 * revalidation used for publication or exact deletion. */
void cbm_text_set_prepublish_hook_for_testing(cbm_text_precommit_test_hook_t hook, void *context);
/* Runs after the synced temporary descriptor is closed but before its pathname
 * is reopened and matched against the descriptor-bound snapshot. */
void cbm_text_set_temp_closed_hook_for_testing(cbm_text_precommit_test_hook_t hook, void *context);
#endif

#endif
