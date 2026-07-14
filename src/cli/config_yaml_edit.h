/*
 * config_yaml_edit.h — Conservative, structure-preserving YAML config edits.
 */
#ifndef CBM_CONFIG_YAML_EDIT_H
#define CBM_CONFIG_YAML_EDIT_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * All functions return 0 on success, including an idempotent no-op, and -1
 * for invalid/unsupported YAML, invalid arguments, overflow, or an I/O error.
 * Existing targets must be single-link regular files. Symlinks, reparse
 * points, directories, devices, and unsafe POSIX metadata fail closed. Before
 * replacement, the editor verifies that both the file identity and bytes still
 * match the version it read. POSIX replacements preserve owner, group, and
 * permission bits and sync the parent directory.
 * Cooperating editor processes are serialized across read, transform, final
 * verification, and replacement. POSIX uses an adjacent persistent
 * "<file_path>.cbm-yaml.lock" regular file, created with mode 0600 and held by
 * a non-blocking advisory lock; release unlocks and closes it without removing
 * the pathname, and process exit releases the lock automatically. Windows uses
 * an adjacent temporary lock directory removed by its verified open handle.
 * Contention and unsafe lock metadata fail closed.
 * Initially missing targets use no-replace publication. A non-cooperating
 * writer can still race an existing-target replacement in the narrow interval
 * after the final verification on platforms without destination-identity CAS.
 *
 * Windows rejects reparse points and uses ReplaceFileW for existing files, but
 * POSIX owner/group/mode semantics do not apply there. Windows ACL durability
 * is delegated to ReplaceFileW and volume write-through behavior.
 *
 * entry_block is the complete YAML content beneath the generated
 * "  entry_key:" line. Every non-empty line must already be indented by at
 * least four spaces. A final newline is optional. The editor validates the
 * trusted block and converts its line endings to those used by file_path.
 * Inline comments on field lines are rejected so a raw dynamic value cannot
 * be silently truncated at '#'. Every dynamic scalar interpolated into an
 * entry_block must first be encoded with
 * cbm_yaml_encode_double_quoted_scalar(). Full-line comments remain allowed.
 *
 * entry_key names an explicitly installer-managed mapping entry. Updating it
 * replaces that entry's complete child block; sibling entries and surrounding
 * comments remain user-owned and are preserved. Callers must not use a key
 * whose child fields should remain independently user-owned.
 */
int cbm_yaml_upsert_mapping_entry(const char *file_path, const char *section_key,
                                  const char *entry_key, const char *entry_block);
int cbm_yaml_remove_mapping_entry(const char *file_path, const char *section_key,
                                  const char *entry_key);

/* Ownership-aware variants for installer-managed mapping entries.
 * canonical_entry_block follows the same validated format as entry_block
 * above. A missing entry is created by upsert and is a successful no-op for
 * removal. A same-name entry is owned only when its complete header and body
 * equal the canonical rendering; otherwise both operations preserve it and
 * return CBM_YAML_IDENTITY_EDIT_FOREIGN. Errors return
 * CBM_YAML_IDENTITY_EDIT_ERROR. */
int cbm_yaml_upsert_owned_mapping_entry(const char *file_path, const char *section_key,
                                        const char *entry_key, const char *canonical_entry_block);
int cbm_yaml_remove_owned_mapping_entry(const char *file_path, const char *section_key,
                                        const char *entry_key, const char *canonical_entry_block);

/* Identity-aware editing of one mapping item in a nested block sequence.
 * sequence_path names the mapping keys from the document root through the
 * sequence key, for example {"hooks", "pre_llm_call"}. Missing mappings and
 * the sequence are created conservatively.
 *
 * canonical_item is exactly one mapping sequence item written at column zero:
 * its first line starts "- ", continuation fields are indented by two spaces,
 * and a final newline is optional. The editor validates it and reindents it to
 * sequence_path. identity_key and identity_scalar identify ownership inside
 * that mapping item. identity_scalar and every dynamic scalar in
 * canonical_item must already be encoded by the caller with
 * cbm_yaml_encode_double_quoted_scalar().
 *
 * Upsert is byte-idempotent when the canonical item already exists. If an item
 * with the same decoded identity exists but differs from canonical_item, both
 * upsert and removal preserve the document and return
 * CBM_YAML_IDENTITY_EDIT_FOREIGN. Removal deletes only the exact canonical
 * item; an absent identity is a successful no-op. Structural ambiguity,
 * unsupported YAML, unsafe filesystem state, invalid arguments, and I/O or
 * concurrency failures return CBM_YAML_IDENTITY_EDIT_ERROR byte-identically. */
enum {
    CBM_YAML_IDENTITY_EDIT_ERROR = -1,
    CBM_YAML_IDENTITY_EDIT_OK = 0,
    CBM_YAML_IDENTITY_EDIT_FOREIGN = 1
};
int cbm_yaml_upsert_mapping_sequence_item(const char *file_path, const char *const *sequence_path,
                                          size_t sequence_path_len, const char *identity_key,
                                          const char *identity_scalar, const char *canonical_item);
int cbm_yaml_remove_mapping_sequence_item(const char *file_path, const char *const *sequence_path,
                                          size_t sequence_path_len, const char *identity_key,
                                          const char *identity_scalar, const char *canonical_item);

/* Add or remove one exact string value in a top-level YAML string-list key. */
int cbm_yaml_upsert_string_list_item(const char *file_path, const char *key, const char *item);
int cbm_yaml_remove_string_list_item(const char *file_path, const char *key, const char *item);

/* Encode one non-empty UTF-8 value as a YAML double-quoted scalar. Spaces,
 * '#', ':', and Unicode are preserved; quotes and backslashes are escaped.
 * Newlines, control bytes, invalid UTF-8, and oversized values are rejected.
 * On success, *encoded_out is heap-allocated and must be freed by the caller.
 * On failure, *encoded_out is set to NULL. */
int cbm_yaml_encode_double_quoted_scalar(const char *value, char **encoded_out);

#ifdef CBM_YAML_ENABLE_TEST_API
/* Deterministic concurrency seam for standalone editor tests. The hook runs
 * after the temporary replacement is synced and immediately before the stale
 * content/identity check. Production callers must not enable this API. */
typedef void (*cbm_yaml_precommit_test_hook_t)(const char *file_path, void *context);
void cbm_yaml_set_precommit_hook_for_testing(cbm_yaml_precommit_test_hook_t hook, void *context);
/* Runs after the first stale-snapshot check and before final destination and
 * temporary-file identity revalidation. */
void cbm_yaml_set_prepublish_hook_for_testing(cbm_yaml_precommit_test_hook_t hook, void *context);
/* Runs after the adjacent lock object's initial identity is captured and
 * before locking and final ownership, mode, and handle identity verification. */
typedef void (*cbm_yaml_lock_postcreate_test_hook_t)(const char *lock_path, void *context);
void cbm_yaml_set_lock_postcreate_hook_for_testing(cbm_yaml_lock_postcreate_test_hook_t hook,
                                                   void *context);
#endif

#ifdef __cplusplus
}
#endif

#endif /* CBM_CONFIG_YAML_EDIT_H */
