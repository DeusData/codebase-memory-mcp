/*
 * windows_launcher_state.h -- Stable state shared by the Windows launcher and
 * the CBM payload.
 *
 * The on-disk current-v1 record is deliberately fixed-size and contains no
 * native C layout.  Keep its codec platform-independent: release compatibility
 * is a byte contract, not a compiler/architecture contract.
 */
#ifndef CBM_WINDOWS_LAUNCHER_STATE_H
#define CBM_WINDOWS_LAUNCHER_STATE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <wchar.h>

#define CBM_WINDOWS_CURRENT_V1_SIZE 128U
#define CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE 128U
#ifndef CBM_WINDOWS_LAUNCHER_ABI_CURRENT
#define CBM_WINDOWS_LAUNCHER_ABI_CURRENT 1U
#endif
#ifndef CBM_WINDOWS_PAYLOAD_LAUNCHER_ABI_MIN
#define CBM_WINDOWS_PAYLOAD_LAUNCHER_ABI_MIN 1U
#endif
#ifndef CBM_WINDOWS_PAYLOAD_LAUNCHER_ABI_MAX
#define CBM_WINDOWS_PAYLOAD_LAUNCHER_ABI_MAX 1U
#endif
#define CBM_WINDOWS_LAUNCHER_PATH_CAP 32768U

/* Hex-char count of the payload digest embedded in a retired-state directory
 * name. 16 chars (64 bits) uniquely identifies the retired generation while
 * keeping the retired path short enough for the managed uninstall's
 * handle-based directory rename at the deepest supported install depth. */
#define CBM_WINDOWS_RETIRED_TAG_HEX 16U

typedef struct {
    uint32_t launcher_abi_min;
    uint32_t launcher_abi_max;
    uint64_t payload_size;
    char payload_sha256[65];
} cbm_windows_current_v1_t;

typedef struct {
    uint32_t launcher_abi;
    uint32_t payload_launcher_abi_min;
    uint32_t payload_launcher_abi_max;
    uint64_t payload_size;
    char payload_sha256[65];
} cbm_windows_release_descriptor_v1_t;

typedef enum {
    CBM_WINDOWS_TRANSITION_INCOMPATIBLE = 0,
    CBM_WINDOWS_TRANSITION_LAUNCHER_FIRST = 1,
    CBM_WINDOWS_TRANSITION_CURRENT_FIRST = 2,
} cbm_windows_transition_plan_t;

typedef enum {
    CBM_WINDOWS_LAUNCHER_ACTION_ORDINARY = 0,
    CBM_WINDOWS_LAUNCHER_ACTION_UPDATE = 1,
    CBM_WINDOWS_LAUNCHER_ACTION_UNINSTALL = 2,
} cbm_windows_launcher_action_t;

bool cbm_windows_current_v1_encode(const cbm_windows_current_v1_t *state,
                                   uint8_t out[CBM_WINDOWS_CURRENT_V1_SIZE]);
bool cbm_windows_current_v1_decode(const uint8_t *record, size_t record_size,
                                   cbm_windows_current_v1_t *state_out);
bool cbm_windows_current_v1_supports_launcher_abi(const cbm_windows_current_v1_t *state,
                                                  uint32_t launcher_abi);

bool cbm_windows_release_descriptor_v1_encode(const cbm_windows_release_descriptor_v1_t *descriptor,
                                              uint8_t out[CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE]);
bool cbm_windows_release_descriptor_v1_decode(const uint8_t *record, size_t record_size,
                                              cbm_windows_release_descriptor_v1_t *descriptor_out);
cbm_windows_transition_plan_t cbm_windows_transition_plan(
    const cbm_windows_current_v1_t *current, const cbm_windows_release_descriptor_v1_t *candidate);

/* Resolve the immutable generation pair using the canonical launcher's
 * directory.  A managed generation contains exactly these two executables:
 * the payload and the launcher's hard-link backing. */
bool cbm_windows_generation_payload_path(const wchar_t *canonical_launcher_path,
                                         const char payload_sha256[65], wchar_t *path_out,
                                         size_t path_capacity);
bool cbm_windows_generation_launcher_path(const wchar_t *canonical_launcher_path,
                                          const char payload_sha256[65], wchar_t *path_out,
                                          size_t path_capacity);
/* Derive the race-free retired state sibling shared by the uninstall payload
 * (its own PID) and supervising launcher (the authenticated child PID). */
bool cbm_windows_retired_state_path(const wchar_t *canonical_launcher_path,
                                    const char payload_sha256[65], uint32_t payload_pid,
                                    wchar_t *path_out, size_t path_capacity);

/* Match main's top-level dispatch.  Tokens after a mode selector (cli,
 * install, config, hook-augment, help/version) are opaque user input. */
cbm_windows_launcher_action_t cbm_windows_launcher_classify_action(int argc,
                                                                   const char *const argv[]);
bool cbm_windows_launcher_action_allowed(cbm_windows_launcher_action_t action, bool managed);

/* Trusted launch data delivered over a launcher-owned inherited named pipe.
 * An absent context is valid and returns true with present=false (direct
 * portable payload).  An advertised but invalid context fails closed. */
typedef struct {
    bool present;
    bool managed;
    bool private_activation;
    cbm_windows_launcher_action_t action;
    uint64_t payload_size;
    char expected_payload_sha256[65];
    wchar_t canonical_launcher_path[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    /* Opaque one-shot authority retained only until startup validation has
     * completed.  Callers must use context_complete, never inspect it. */
    uintptr_t _authority_handle;
} cbm_windows_launcher_context_t;

bool cbm_windows_launcher_context_consume(cbm_windows_launcher_context_t *context_out, char *error,
                                          size_t error_size);
bool cbm_windows_launcher_context_complete(cbm_windows_launcher_context_t *context, bool accepted,
                                           char *error, size_t error_size);

/* Windows managed-install primitives.  They fail closed on other platforms.
 * Diagnostics are optional and always NUL-terminated when capacity is nonzero.
 */
bool cbm_windows_launcher_capability_probe(const wchar_t *target_directory,
                                           const wchar_t *launcher_candidate, char *error,
                                           size_t error_size);
bool cbm_windows_launcher_file_secure(const wchar_t *launcher_path, char *error, size_t error_size);
/* Find the unique managed-generation backing for canonical_launcher_path.
 * The canonical name and backing must be the same file identity with exactly
 * two links.  The backing need not be in the current payload generation while
 * a crash-safe cross-ABI transition is between its two publication steps. */
bool cbm_windows_managed_launcher_backing(const wchar_t *canonical_launcher_path,
                                          wchar_t *backing_path_out, size_t backing_path_capacity,
                                          char *error, size_t error_size);
bool cbm_windows_release_descriptor_probe(const wchar_t *launcher_candidate,
                                          cbm_windows_release_descriptor_v1_t *descriptor_out,
                                          char *error, size_t error_size);
bool cbm_windows_current_v1_write_atomic(const wchar_t *canonical_launcher_path,
                                         const cbm_windows_current_v1_t *state, char *error,
                                         size_t error_size);
/* Publish an exact-one-link immutable generation backing at the canonical
 * name by hard-linking a same-directory stage and POSIX-renaming that stage.
 * Success leaves canonical and backing as the same exact-two-link file. */
bool cbm_windows_launcher_replace_atomic(const wchar_t *target_path, const wchar_t *backing_path,
                                         char *error, size_t error_size);
bool cbm_windows_launcher_remove_posix(const wchar_t *target_path, char *error, size_t error_size);
/* Retire .cbm to its generation/PID-qualified sibling, then unlink canonical
 * as the final uninstall commit.  A failed unlink restores .cbm. */
bool cbm_windows_launcher_uninstall_commit(const wchar_t *canonical_launcher_path,
                                           const char payload_sha256[65], char *error,
                                           size_t error_size);
bool cbm_windows_generation_rollback_if_unreferenced(const wchar_t *canonical_launcher_path,
                                                     const char payload_sha256[65],
                                                     bool created_by_activation, char *error,
                                                     size_t error_size);
bool cbm_windows_generations_prune(const wchar_t *canonical_launcher_path, size_t *removed_out,
                                   char *error, size_t error_size);

#endif /* CBM_WINDOWS_LAUNCHER_STATE_H */
