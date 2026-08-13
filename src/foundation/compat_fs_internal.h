/*
 * compat_fs_internal.h — Internal platform helpers.
 *
 * These functions are implementation details shared by the few production
 * modules that must retain a native handle across an operation and by focused
 * tests. Other production code should use the portable APIs in compat_fs.h.
 */
#ifndef CBM_FOUNDATION_COMPAT_FS_INTERNAL_H
#define CBM_FOUNDATION_COMPAT_FS_INTERNAL_H

#ifdef _WIN32

#include <stdbool.h>
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <wchar.h>

/*
 * Windows reports the same short-lived rename conflicts at multiple
 * publication surfaces. Keep their classification in one place so callers
 * cannot drift on which errors are safe to retry or route through the
 * handle-based POSIX-semantics fallback.
 */
bool cbm_windows_replace_error_is_transient(DWORD error);

/*
 * Atomically replace destination_path with the already-open source handle
 * using FileRenameInfoEx POSIX semantics. The source handle must include
 * DELETE access. This is the Windows compatibility seam for destinations
 * whose previous generation is still open with FILE_SHARE_DELETE. It never
 * closes source; the caller retains handle ownership on success and failure.
 */
bool cbm_windows_replace_open_file(HANDLE source, const wchar_t *destination_path,
                                   DWORD *platform_error);

/*
 * Build a properly-quoted Windows command line from a NULL-terminated
 * argv array. This is the quoting step underlying cbm_exec_no_shell on
 * Windows: it is what turns {"taskkill", "/FI", "IMAGENAME eq foo.exe"}
 * into `taskkill /FI "IMAGENAME eq foo.exe"` rather than three bare
 * tokens (the #697 regression).
 *
 * Quoting follows the MSVC/CommandLineToArgvW convention: an argument is
 * wrapped in double-quotes when it is empty or contains a space, tab, or
 * double-quote; backslashes immediately before a quote (literal or the
 * closing one) are doubled, and embedded double-quotes are escaped with a
 * backslash.
 *
 * Returns a heap-allocated wide string the caller must free(), or NULL on
 * allocation failure.
 */
wchar_t *cbm_build_cmdline(const char *const *argv);

/*
 * Test hook for the isolated popen path (#798): returns 1 when the most
 * recent cbm_popen(..., "r") stream was produced by the isolated
 * CreateProcessW + PROC_THREAD_ATTRIBUTE_HANDLE_LIST spawn, 0 otherwise
 * (e.g. a non-read mode routed to _popen, or a failed isolated spawn).
 * Not synchronized across threads; intended for single-threaded test
 * assertions only.
 */
int cbm_popen_last_was_isolated(void);

#endif /* _WIN32 */

#endif /* CBM_FOUNDATION_COMPAT_FS_INTERNAL_H */
