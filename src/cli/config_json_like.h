/*
 * config_json_like.h — Structure-preserving JSON/JSONC/JSON5 config edits.
 */
#ifndef CBM_CONFIG_JSON_LIKE_H
#define CBM_CONFIG_JSON_LIKE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Editors only rewrite missing paths or regular, single-link config files.
 * POSIX symlinks and Windows reparse points fail closed. Existing POSIX
 * owner/group/mode metadata is retained across the atomic replacement. The
 * destination and synced temporary file are identity/byte revalidated
 * immediately before publication. Portable platforms without a path-identity
 * CAS still have a narrow interval between final verification and replacement.
 *
 * Insert or replace entry_key inside the object at object_path.
 * entry_json must contain exactly one strict JSON value.
 * Returns 0 on success and -1 on invalid input or an I/O error. */
int cbm_json_like_upsert_entry(const char *file_path, const char *const *object_path,
                               size_t path_len, const char *entry_key, const char *entry_json);

/* Safely read one regular, single-link document. Returns 0 with malloc-owned
 * content, 1 when missing, and -1 for unsafe filesystem state or I/O failure. */
int cbm_json_like_read_document(const char *file_path, char **content_out, size_t *length_out);

/* Read one uniquely named value from object_path without mutating the file.
 * On success, value_json_out receives a malloc-owned, NUL-terminated exact
 * JSON/JSONC/JSON5 source slice and value_length_out its byte length. Returns
 * 1 when the file, path, or entry is missing and -1 for ambiguity, malformed
 * input, unsafe filesystem state, or I/O failure. */
int cbm_json_like_get_raw_entry(const char *file_path, const char *const *object_path,
                                size_t path_len, const char *entry_key, char **value_json_out,
                                size_t *value_length_out);

/* Exact object-entry schema matching on a caller-owned document snapshot.
 * Every object member must match one field exactly once; unknown and duplicate
 * members produce CBM_JSON_LIKE_OBJECT_MISMATCH. STRING fields may require an
 * exact decoded value or capture their decoded value for caller validation.
 *
 * This API intentionally accepts document bytes rather than a path so callers
 * can validate the same snapshot supplied to an *_if_unchanged mutation. */
typedef enum {
    CBM_JSON_LIKE_VALUE_STRING,
    CBM_JSON_LIKE_VALUE_EMPTY_ARRAY,
    CBM_JSON_LIKE_VALUE_SINGLE_STRING_ARRAY,
} cbm_json_like_value_shape_t;

enum {
    CBM_JSON_LIKE_FIELD_REQUIRED = 1U << 0U,
    CBM_JSON_LIKE_FIELD_CAPTURE_STRING = 1U << 1U,
};

typedef struct {
    const char *key;
    cbm_json_like_value_shape_t shape;
    const char *expected_string;
    unsigned flags;
} cbm_json_like_object_field_t;

enum {
    CBM_JSON_LIKE_OBJECT_MATCH = 0,
    CBM_JSON_LIKE_OBJECT_MISSING = 1,
    CBM_JSON_LIKE_OBJECT_MISMATCH = 2,
};

/* captured_string_out receives malloc-owned decoded content only on MATCH.
 * Returns one of CBM_JSON_LIKE_OBJECT_* or -1 for malformed input. */
int cbm_json_like_match_object_entry(const char *document, size_t document_length,
                                     const char *const *object_path, size_t path_len,
                                     const char *entry_key,
                                     const cbm_json_like_object_field_t *fields, size_t field_count,
                                     char **captured_string_out);

/* Conditional variants used when a caller must semantically inspect an exact
 * snapshot before writing. expected_content == NULL means the file was
 * missing. A changed document is rejected without mutation. */
int cbm_json_like_upsert_entry_if_unchanged(const char *file_path, const char *const *object_path,
                                            size_t path_len, const char *entry_key,
                                            const char *entry_json, const char *expected_content,
                                            size_t expected_length);

/* Remove entry_key from the object at object_path. A missing path or entry is
 * a successful no-op. Returns 0 on success and -1 on invalid input or I/O. */
int cbm_json_like_remove_entry(const char *file_path, const char *const *object_path,
                               size_t path_len, const char *entry_key);
int cbm_json_like_remove_entry_if_unchanged(const char *file_path, const char *const *object_path,
                                            size_t path_len, const char *entry_key,
                                            const char *expected_content, size_t expected_length);

/* Add string_value once to the array named array_key in the object at
 * object_path. Missing objects and the array are created. The raw string is
 * JSON-escaped; a pre-existing exact string is a no-op. */
int cbm_json_like_add_unique_string_at_path(const char *file_path, const char *const *object_path,
                                            size_t path_len, const char *array_key,
                                            const char *string_value);

/* Remove the exact string_value from the array at object_path/array_key.
 * Missing files, paths, arrays, and strings are successful no-ops. */
int cbm_json_like_remove_string_at_path(const char *file_path, const char *const *object_path,
                                        size_t path_len, const char *array_key,
                                        const char *string_value);

/* Read a string from the object at object_path. On success, value_out receives
 * a malloc-owned decoded UTF-8 string. Returns 0 when found, 1 when the file,
 * path, or key is missing, and -1 for invalid input, ambiguity, a non-string
 * value, unsafe filesystem state, or I/O failure. */
int cbm_json_like_get_string_at_path(const char *file_path, const char *const *object_path,
                                     size_t path_len, const char *string_key, char **value_out);

/* Top-level convenience wrapper for cbm_json_like_add_unique_string_at_path. */
int cbm_json_like_add_unique_string(const char *file_path, const char *array_key,
                                    const char *string_value);

/* Top-level convenience wrapper for cbm_json_like_remove_string_at_path. */
int cbm_json_like_remove_string(const char *file_path, const char *array_key,
                                const char *string_value);

#ifdef CBM_JSON_LIKE_ENABLE_TEST_API
/* Deterministic concurrency seam for the standalone editor tests. The hook is
 * invoked after the replacement has been synced but before the stale-snapshot
 * check. Production callers must not enable this API. */
typedef void (*cbm_json_like_precommit_test_hook_t)(const char *file_path, void *context);
void cbm_json_like_set_precommit_hook_for_testing(cbm_json_like_precommit_test_hook_t hook,
                                                  void *context);
/* Invoked after the first stale-snapshot check. The editor performs a final
 * destination and temporary-file identity check after the hook and
 * immediately before publication. */
void cbm_json_like_set_prepublish_hook_for_testing(cbm_json_like_precommit_test_hook_t hook,
                                                   void *context);
#endif

#ifdef __cplusplus
}
#endif

#endif /* CBM_CONFIG_JSON_LIKE_H */
