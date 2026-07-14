/*
 * compat_fs_internal.h — Internal helpers exposed for testing.
 *
 * These functions are implementation details of compat_fs.c; they are
 * declared here only so that the test suite can drive them directly.
 * Production code outside compat_fs.c should use the public APIs in
 * compat_fs.h instead.
 */
#ifndef CBM_FOUNDATION_COMPAT_FS_INTERNAL_H
#define CBM_FOUNDATION_COMPAT_FS_INTERNAL_H

#ifdef _WIN32

#include <wchar.h>

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
