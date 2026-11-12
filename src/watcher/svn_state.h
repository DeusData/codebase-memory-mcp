/*
 * svn_state.h -- Subversion working-copy observation core (#1127, part A).
 *
 * Two pieces, and neither of them runs a process:
 *
 *   cbm_svn_client_init      resolves and pins a system `svn` client from the
 *                            absolute PATH entries, refusing any candidate that
 *                            lives under the watched root or the current
 *                            directory. It only inspects files; it never
 *                            executes the client.
 *
 *   cbm_svn_parse_status_stream
 *                            reads one `svn status --xml --verbose` document
 *                            from a FILE * and folds it into two signatures: a
 *                            semantic one over every reported entry, and a
 *                            content one over the indexable files the status
 *                            reports as changed, added or unversioned.
 *
 * Nothing calls this yet. Running the client (a bounded subprocess writing a
 * capture file) and the opt-in watcher strategy are a separate change.
 */
#ifndef CBM_WATCHER_SVN_STATE_H
#define CBM_WATCHER_SVN_STATE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "foundation/constants.h"

typedef enum {
    /* The answer is unknown: unparseable or truncated output, a path that
     * escapes the root, an I/O error. The caller keeps its previous state. */
    CBM_SVN_PROBE_UNCERTAIN = -1,
    /* A well-formed status document with no entries: not a working copy. */
    CBM_SVN_PROBE_NOT_WORKING_COPY = 0,
    CBM_SVN_PROBE_OK = 1,
} cbm_svn_probe_result_t;

/* How the pinned client spells local paths. Native Win32 builds take drive
 * paths; MSYS2 and Cygwin builds are POSIX programs that need their runtime's
 * mount spelling. Decided by the runtime DLL that sits beside the executable,
 * never by where the client is installed. Always NATIVE outside Windows. */
typedef enum {
    CBM_SVN_PATHS_NATIVE = 0,
    CBM_SVN_PATHS_MSYS,   /* msys-2.0.dll: C:/x -> /c/x */
    CBM_SVN_PATHS_CYGWIN, /* cygwin1.dll: C:/x -> /cygdrive/c/x (the default prefix) */
} cbm_svn_path_style_t;

typedef struct {
    char executable[CBM_SZ_4K];
    cbm_svn_path_style_t path_style;
} cbm_svn_client_t;

typedef struct {
    uint64_t semantic_signature;
    uint64_t content_signature;
    uint64_t bytes_hashed;
    bool has_local_changes;
    int entry_count;     /* status entries folded into semantic_signature */
    int candidate_count; /* files whose bytes were folded into content_signature */
    /* Working-copy content that is skipped, never followed and never fatal:
     * one odd file must not turn the whole observation UNCERTAIN. */
    int skipped_symlinks;   /* symlinks and reparse points */
    int skipped_special;    /* FIFOs, sockets, devices */
    int skipped_unreadable; /* permission and sharing failures */
    int skipped_oversize;   /* over cbm_max_file_bytes(), by size or by bytes read */
} cbm_svn_observation_t;

/* Resolve and pin a system SVN client without executing anything. */
cbm_svn_probe_result_t cbm_svn_client_init(const char *root_path, cbm_svn_client_t *client);

/* Format a local path for the pinned client's runtime (see path_style). */
bool cbm_svn_format_path_arg(const cbm_svn_client_t *client, const char *path, char *out,
                             size_t out_size);

/* Parse one `svn status --xml --verbose` document. root_path is the working
 * copy root; status_root is the path argument the client was given (the
 * cbm_svn_format_path_arg spelling, or NULL when it equals root_path). Entry
 * paths may be spelled relative to either. On OK, *observation is filled; on
 * any other result it is left untouched. */
cbm_svn_probe_result_t cbm_svn_parse_status_stream(FILE *stream, const char *root_path,
                                                   const char *status_root,
                                                   cbm_svn_observation_t *observation);

#ifdef CBM_SVN_STATE_ENABLE_TEST_API
/* The Windows path-style rule, callable on every platform. */
cbm_svn_path_style_t cbm_svn_test_path_style_for_executable(const char *executable);
/* Called after a file is opened and measured, before its bytes are read, so
 * a test can grow the file past the cap at exactly that point. NULL clears. */
void cbm_svn_test_set_after_measure_hook(void (*hook)(const char *relative_path));
#endif

#endif /* CBM_WATCHER_SVN_STATE_H */
