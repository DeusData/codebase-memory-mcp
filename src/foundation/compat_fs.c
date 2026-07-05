/*
 * compat_fs.c — Portable file system operations.
 *
 * POSIX: direct wrappers around opendir/readdir/closedir, popen/pclose, mkdir, unlink.
 * Windows: FindFirstFile/FindNextFile, _popen/_pclose, _mkdir, _unlink.
 */
#include "foundation/constants.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_fs_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32

/* ── Windows implementation ────────────────────────────────── */

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <direct.h> /* _wmkdir */
#include <errno.h>  /* errno for spawn-failure logging */
#include <fcntl.h>  /* _O_RDONLY */
#include <io.h>     /* _wunlink, _open_osfhandle, _close */
#include <stdint.h> /* intptr_t */
#include "foundation/log.h"
#include "foundation/win_utf8.h"

struct cbm_dir {
    HANDLE find_handle;
    WIN32_FIND_DATAW find_data;
    wchar_t wide_pattern[CBM_PATH_MAX];
    cbm_dirent_t entry;
    bool first;
    bool done;
};

cbm_dir_t *cbm_opendir(const char *path) {
    if (!path) {
        return NULL;
    }
    wchar_t *wpath = cbm_utf8_to_wide(path);
    if (!wpath) {
        return NULL;
    }

    size_t wlen = wcslen(wpath);
    if (wlen == 0 || wlen + 2 >= CBM_PATH_MAX) {
        free(wpath);
        return NULL;
    }

    cbm_dir_t *d = (cbm_dir_t *)calloc(CBM_ALLOC_ONE, sizeof(cbm_dir_t));
    if (!d) {
        free(wpath);
        return NULL;
    }

    wmemcpy(d->wide_pattern, wpath, wlen + 1);
    wchar_t *p = d->wide_pattern + wlen - SKIP_ONE;
    if (*p != L'\\' && *p != L'/') {
        ++p;
        *p++ = L'\\';
    } else {
        ++p;
    }
    *p++ = L'*';
    *p = L'\0';
    free(wpath);

    d->find_handle = FindFirstFileW(d->wide_pattern, &d->find_data);
    if (d->find_handle == INVALID_HANDLE_VALUE) {
        free(d);
        return NULL;
    }
    d->first = true;
    d->done = false;
    return d;
}

cbm_dirent_t *cbm_readdir(cbm_dir_t *d) {
    if (!d || d->done) {
        return NULL;
    }
    if (!d->first) {
        if (!FindNextFileW(d->find_handle, &d->find_data)) {
            d->done = true;
            return NULL;
        }
    }
    d->first = false;

    while (d->find_data.cFileName[0] == L'.' &&
           (d->find_data.cFileName[1] == L'\0' ||
            (d->find_data.cFileName[1] == L'.' && d->find_data.cFileName[2] == L'\0'))) {
        if (!FindNextFileW(d->find_handle, &d->find_data)) {
            d->done = true;
            return NULL;
        }
    }

    char *u8 = cbm_wide_to_utf8(d->find_data.cFileName);
    if (!u8) {
        d->done = true;
        return NULL;
    }
    size_t nlen = strlen(u8);
    if (nlen >= CBM_DIRENT_NAME_MAX) {
        nlen = CBM_DIRENT_NAME_MAX - SKIP_ONE;
    }
    memcpy(d->entry.name, u8, nlen);
    d->entry.name[nlen] = '\0';
    free(u8);
    d->entry.is_dir = (d->find_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
    d->entry.d_type = 0;
    return &d->entry;
}

void cbm_closedir(cbm_dir_t *d) {
    if (d) {
        if (d->find_handle != INVALID_HANDLE_VALUE) {
            FindClose(d->find_handle);
        }
        free(d);
    }
}

/* Windows _popen replacement that inherits ONLY the child's stdout pipe.
 *
 * The CRT's _popen uses CreateProcess(bInheritHandles=TRUE), which leaks EVERY
 * inheritable handle we hold into the child — listening/client sockets, the
 * Winsock/AFD helper handles created by WSAStartup, the MCP stdio pipe, etc.
 * When the child is git-for-Windows (MSYS2/Cygwin runtime), its startup walks
 * every inherited handle and calls NtQueryObject on each to classify it; on an
 * inherited socket/AFD handle NtQueryObject deadlocks. Since our UI server runs
 * requests on a single thread, that wedges the whole server (list_projects,
 * which shells out to git per project, never returns → the web UI hangs).
 *
 * The fix: spawn via CreateProcessW with STARTUPINFOEXW + an explicit
 * PROC_THREAD_ATTRIBUTE_HANDLE_LIST containing only the stdout write-end and a
 * NUL handle for stdin/stderr. Nothing else crosses into git, so there is no
 * foreign handle to deadlock on. POSIX popen() already sets O_CLOEXEC on its
 * pipe, so the POSIX path is unchanged.
 *
 * There is deliberately NO fallback to _popen when the isolated spawn fails:
 * falling back would silently re-arm the deadlock. cbm_popen logs a structured
 * warning and returns NULL instead (every call site handles NULL). */

enum { CBM_POPEN_MAX = 16 };
static struct {
    FILE *fp;
    HANDLE proc;
} g_popen_tab[CBM_POPEN_MAX];
static CRITICAL_SECTION g_popen_lock;
static INIT_ONCE g_popen_once = INIT_ONCE_STATIC_INIT;

/* Test hook (declared in compat_fs_internal.h): 1 when the most recent
 * cbm_popen(..., "r") stream came from the isolated spawn. Test-only
 * observable; not synchronized across threads. */
static volatile LONG g_popen_last_isolated = 0;

int cbm_popen_last_was_isolated(void) {
    return (int)g_popen_last_isolated;
}

static BOOL CALLBACK cbm_popen_init(PINIT_ONCE once, PVOID param, PVOID *ctx) {
    (void)once;
    (void)param;
    (void)ctx;
    InitializeCriticalSection(&g_popen_lock);
    return TRUE;
}

/* Resolve the shell explicitly — %COMSPEC%, else <system dir>\cmd.exe — so it
 * can be passed as lpApplicationName and CreateProcess never walks the search
 * path (no cmd.exe planting from a hostile CWD). Heap string; caller frees. */
static wchar_t *cbm_resolve_comspec(void) {
    wchar_t buf[MAX_PATH];
    const wchar_t suffix[] = L"\\cmd.exe";
    DWORD n = GetEnvironmentVariableW(L"COMSPEC", buf, MAX_PATH);
    if (n == 0 || n >= MAX_PATH) {
        UINT sn = GetSystemDirectoryW(buf, MAX_PATH);
        if (sn == 0 || (size_t)sn + wcslen(suffix) >= MAX_PATH) {
            return NULL;
        }
        wmemcpy(buf + sn, suffix, wcslen(suffix) + 1);
    }
    return _wcsdup(buf);
}

/* On failure returns NULL with *stage naming the failing step and *gle the
 * GetLastError value captured at that step (0 when errno is the signal). */
static FILE *cbm_popen_isolated(const char *cmd, const char **stage, DWORD *gle) {
    *stage = "";
    *gle = 0;
    InitOnceExecuteOnce(&g_popen_once, cbm_popen_init, NULL, NULL);

    SECURITY_ATTRIBUTES sa;
    sa.nLength = sizeof(sa);
    sa.lpSecurityDescriptor = NULL;
    sa.bInheritHandle = TRUE;

    HANDLE rd = NULL, wr = NULL;
    if (!CreatePipe(&rd, &wr, &sa, 0)) {
        *stage = "pipe";
        *gle = GetLastError();
        return NULL;
    }
    /* The parent read-end must never cross into the child. */
    SetHandleInformation(rd, HANDLE_FLAG_INHERIT, 0);

    /* NUL for the child's stdin/stderr so it never touches our real stdin
     * pipe. If NUL cannot be opened, fail: STARTF_USESTDHANDLES slots must
     * never carry INVALID_HANDLE_VALUE. */
    HANDLE nul = CreateFileW(L"NUL", GENERIC_READ | GENERIC_WRITE,
                             FILE_SHARE_READ | FILE_SHARE_WRITE, &sa, OPEN_EXISTING, 0, NULL);
    if (nul == INVALID_HANDLE_VALUE) {
        *stage = "nul";
        *gle = GetLastError();
        CloseHandle(rd);
        CloseHandle(wr);
        return NULL;
    }

    HANDLE inherit[2];
    inherit[0] = wr;
    inherit[1] = nul;

    SIZE_T attr_sz = 0;
    InitializeProcThreadAttributeList(NULL, 1, 0, &attr_sz);
    LPPROC_THREAD_ATTRIBUTE_LIST attr = (LPPROC_THREAD_ATTRIBUTE_LIST)malloc(attr_sz);
    BOOL attr_init = attr && InitializeProcThreadAttributeList(attr, 1, 0, &attr_sz);
    BOOL prepared =
        attr_init && UpdateProcThreadAttribute(attr, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, inherit,
                                               sizeof(inherit), NULL, NULL);
    DWORD attr_gle = prepared ? 0 : GetLastError();

    STARTUPINFOEXW si;
    ZeroMemory(&si, sizeof(si));
    si.StartupInfo.cb = sizeof(si);
    si.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    si.StartupInfo.hStdInput = nul;
    si.StartupInfo.hStdOutput = wr;
    si.StartupInfo.hStdError = nul;
    si.lpAttributeList = attr;

    /* Run through cmd.exe /c so command quoting and `2>NUL` behave as under
     * _popen. The command line is heap-composed (no fixed-size truncation)
     * and widened via UTF-8 so non-ASCII repo paths survive intact. */
    wchar_t *app = cbm_resolve_comspec();
    wchar_t *wcmdline = NULL;
    if (app) {
        size_t u8len = strlen(cmd) + sizeof("cmd.exe /c ");
        char *u8 = (char *)malloc(u8len);
        if (u8) {
            snprintf(u8, u8len, "cmd.exe /c %s", cmd);
            wcmdline = cbm_utf8_to_wide(u8);
            free(u8);
        }
    }

    PROCESS_INFORMATION pi;
    ZeroMemory(&pi, sizeof(pi));
    BOOL created = FALSE;
    if (!prepared) {
        *stage = "attr";
        *gle = attr_gle;
    } else if (!app || !wcmdline) {
        *stage = "cmdline";
        *gle = ERROR_NOT_ENOUGH_MEMORY;
    } else {
        created = CreateProcessW(app, wcmdline, NULL, NULL, TRUE, EXTENDED_STARTUPINFO_PRESENT,
                                 NULL, NULL, &si.StartupInfo, &pi);
        if (!created) {
            *stage = "spawn";
            *gle = GetLastError();
        }
    }

    free(app);
    free(wcmdline);
    if (attr) {
        if (attr_init) {
            DeleteProcThreadAttributeList(attr);
        }
        free(attr);
    }
    CloseHandle(wr); /* the child owns the write-end now */
    CloseHandle(nul);
    if (!created) {
        CloseHandle(rd);
        return NULL;
    }
    CloseHandle(pi.hThread);

    int fd = _open_osfhandle((intptr_t)rd, _O_RDONLY);
    if (fd == -1) {
        *stage = "osfhandle";
        CloseHandle(rd);
        CloseHandle(pi.hProcess);
        return NULL;
    }
    FILE *fp = _fdopen(fd, "r"); /* takes ownership of fd/rd */
    if (!fp) {
        *stage = "fdopen";
        _close(fd);
        CloseHandle(pi.hProcess);
        return NULL;
    }

    EnterCriticalSection(&g_popen_lock);
    for (int i = 0; i < CBM_POPEN_MAX; i++) {
        if (!g_popen_tab[i].fp) {
            g_popen_tab[i].fp = fp;
            g_popen_tab[i].proc = pi.hProcess;
            LeaveCriticalSection(&g_popen_lock);
            return fp;
        }
    }
    LeaveCriticalSection(&g_popen_lock);
    /* Table full (shouldn't happen): don't leak the process handle. */
    *stage = "table";
    CloseHandle(pi.hProcess);
    fclose(fp);
    return NULL;
}

FILE *cbm_popen(const char *cmd, const char *mode) {
    /* Our git shell-outs are all read-mode; they MUST use the isolated
     * spawn. On failure, log and fail the call — never fall back to
     * _popen, whose full handle inheritance re-arms the UI hang (#798). */
    if (mode && mode[0] == 'r' && mode[1] == '\0') {
        const char *stage = "";
        DWORD gle = 0;
        FILE *fp = cbm_popen_isolated(cmd, &stage, &gle);
        g_popen_last_isolated = (fp != NULL);
        if (!fp) {
            char glebuf[CBM_SZ_16];
            char errnobuf[CBM_SZ_16];
            snprintf(glebuf, sizeof(glebuf), "%lu", (unsigned long)gle);
            snprintf(errnobuf, sizeof(errnobuf), "%d", errno);
            cbm_log_warn("compat.popen_isolated_failed", "stage", stage, "gle", glebuf, "errno",
                         errnobuf);
        }
        return fp;
    }
    g_popen_last_isolated = 0;
    return _popen(cmd, mode);
}

int cbm_pclose(FILE *f) {
    InitOnceExecuteOnce(&g_popen_once, cbm_popen_init, NULL, NULL);

    HANDLE proc = NULL;
    EnterCriticalSection(&g_popen_lock);
    for (int i = 0; i < CBM_POPEN_MAX; i++) {
        if (g_popen_tab[i].fp == f) {
            proc = g_popen_tab[i].proc;
            g_popen_tab[i].fp = NULL;
            g_popen_tab[i].proc = NULL;
            break;
        }
    }
    LeaveCriticalSection(&g_popen_lock);

    if (!proc) {
        return _pclose(f); /* opened via _popen (non-read mode) */
    }
    fclose(f);
    WaitForSingleObject(proc, INFINITE);
    DWORD code = 0;
    BOOL got = GetExitCodeProcess(proc, &code);
    CloseHandle(proc);
    return got ? (int)code : -1;
}

FILE *cbm_fopen(const char *path, const char *mode) {
    wchar_t *wpath = cbm_utf8_to_wide(path);
    if (!wpath) {
        return NULL;
    }
    wchar_t *wmode = cbm_utf8_to_wide(mode);
    if (!wmode) {
        free(wpath);
        return NULL;
    }
    FILE *f = _wfopen(wpath, wmode);
    free(wpath);
    free(wmode);
    return f;
}

bool cbm_mkdir_p(const char *path, int mode) {
    (void)mode;
    wchar_t *wpath = cbm_utf8_to_wide(path);
    if (!wpath) {
        return false;
    }

    if (_wmkdir(wpath) == 0) {
        free(wpath);
        return true;
    }
    size_t wlen = wcslen(wpath);
    wchar_t *tmp = (wchar_t *)malloc((wlen + 1) * sizeof(wchar_t));
    if (!tmp) {
        free(wpath);
        return false;
    }
    wmemcpy(tmp, wpath, wlen + 1);
    for (wchar_t *p = tmp + SKIP_ONE; *p; p++) {
        if (*p == L'/' || *p == L'\\') {
            *p = L'\0';
            _wmkdir(tmp);
            *p = L'\\';
        }
    }
    bool ok = _wmkdir(tmp) == 0 || GetLastError() == ERROR_ALREADY_EXISTS;
    free(tmp);
    free(wpath);
    return ok;
}

int cbm_unlink(const char *path) {
    wchar_t *wpath = cbm_utf8_to_wide(path);
    if (!wpath) {
        return CBM_NOT_FOUND;
    }
    int ret = _wunlink(wpath);
    free(wpath);
    return ret;
}

int cbm_rmdir(const char *path) {
    wchar_t *wpath = cbm_utf8_to_wide(path);
    if (!wpath) {
        return CBM_NOT_FOUND;
    }
    int ret = _wrmdir(wpath);
    free(wpath);
    return ret;
}

/* Build a properly-quoted Windows command line from an argv array.
 * Returns a heap-allocated wide string, or NULL on allocation failure.
 * Quoting follows the MSVC CRT convention: arguments containing spaces,
 * tabs, or double-quotes are wrapped in double-quotes, with backslashes
 * before a closing quote doubled and the quote itself escaped. Argument
 * bytes are treated as UTF-8 and converted to wide via cbm_utf8_to_wide,
 * so non-ASCII arguments (e.g. a non-ASCII %USERPROFILE%) survive intact.
 * Declared in compat_fs_internal.h so the test suite can drive it. */
wchar_t *cbm_build_cmdline(const char *const *argv) {
    /* First pass: compute required buffer size. */
    size_t total = 1; /* NUL terminator */
    for (int i = 0; argv[i]; i++) {
        const char *arg = argv[i];
        bool needs_quote = (arg[0] == '\0');
        for (const char *p = arg; *p; p++) {
            if (*p == ' ' || *p == '\t' || *p == '"') {
                needs_quote = true;
            }
        }
        if (i > 0) {
            total++; /* space separator */
        }
        if (needs_quote) {
            total += 2; /* opening and closing quote */
            size_t backslashes = 0;
            for (const char *p = arg; *p; p++) {
                if (*p == '\\') {
                    backslashes++;
                } else if (*p == '"') {
                    total += backslashes + 1; /* double backslashes + escape backslash */
                    backslashes = 0;
                } else {
                    backslashes = 0;
                }
                total++;
            }
            /* Trailing backslashes before closing quote must be doubled. */
            total += backslashes;
        } else {
            total += strlen(arg);
        }
    }

    /* Build the quoted command line in UTF-8 first, then widen it as a
     * whole via cbm_utf8_to_wide. Every character the quoting logic acts
     * on (space, tab, '"', '\\') is ASCII and, by UTF-8's design, never
     * appears inside a multibyte sequence, so operating on raw bytes here
     * is safe and keeps multibyte argument bytes intact for conversion. */
    char *buf = (char *)malloc(total);
    if (!buf) {
        return NULL;
    }

    /* Second pass: write the command line bytes. */
    char *w = buf;
    for (int i = 0; argv[i]; i++) {
        const char *arg = argv[i];
        bool needs_quote = (arg[0] == '\0');
        for (const char *p = arg; *p; p++) {
            if (*p == ' ' || *p == '\t' || *p == '"') {
                needs_quote = true;
                break;
            }
        }
        if (i > 0) {
            *w++ = ' ';
        }
        if (needs_quote) {
            *w++ = '"';
            size_t backslashes = 0;
            for (const char *p = arg; *p; p++) {
                if (*p == '\\') {
                    backslashes++;
                    *w++ = '\\';
                } else if (*p == '"') {
                    /* Double the preceding backslashes, then escape the quote. */
                    for (size_t b = 0; b < backslashes; b++) {
                        *w++ = '\\';
                    }
                    *w++ = '\\';
                    *w++ = '"';
                    backslashes = 0;
                } else {
                    backslashes = 0;
                    *w++ = *p;
                }
            }
            /* Double trailing backslashes before the closing quote. */
            for (size_t b = 0; b < backslashes; b++) {
                *w++ = '\\';
            }
            *w++ = '"';
        } else {
            for (const char *p = arg; *p; p++) {
                *w++ = *p;
            }
        }
    }
    *w = '\0';

    wchar_t *out = cbm_utf8_to_wide(buf);
    free(buf);
    return out;
}

int cbm_exec_no_shell(const char *const *argv) {
    if (!argv || !argv[0]) {
        return CBM_NOT_FOUND;
    }

    wchar_t *cmdline = cbm_build_cmdline(argv);
    if (!cmdline) {
        return CBM_NOT_FOUND;
    }

    STARTUPINFOW si;
    PROCESS_INFORMATION pi;
    memset(&si, 0, sizeof(si));
    memset(&pi, 0, sizeof(pi));
    si.cb = sizeof(si);

    if (!CreateProcessW(NULL, cmdline, NULL, NULL, FALSE, 0, NULL, NULL, &si, &pi)) {
        free(cmdline);
        return CBM_NOT_FOUND;
    }
    free(cmdline);

    WaitForSingleObject(pi.hProcess, INFINITE);
    DWORD exit_code = (DWORD)CBM_NOT_FOUND;
    GetExitCodeProcess(pi.hProcess, &exit_code);
    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);
    return (int)exit_code;
}

#else /* POSIX */

/* ── POSIX implementation ────────────────────────────────── */

#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

struct cbm_dir {
    DIR *dir;
    cbm_dirent_t entry;
};

cbm_dir_t *cbm_opendir(const char *path) {
    if (!path) {
        return NULL;
    }
    DIR *dir = opendir(path);
    if (!dir) {
        return NULL;
    }
    cbm_dir_t *d = (cbm_dir_t *)calloc(CBM_ALLOC_ONE, sizeof(cbm_dir_t));
    if (!d) {
        closedir(dir);
        return NULL;
    }
    d->dir = dir;
    return d;
}

cbm_dirent_t *cbm_readdir(cbm_dir_t *d) {
    if (!d || !d->dir) {
        return NULL;
    }
    struct dirent *de;
    while ((de = readdir(d->dir)) != NULL) {
        /* Skip "." and ".." */
        if (de->d_name[0] == '.' &&
            (de->d_name[SKIP_ONE] == '\0' ||
             (de->d_name[SKIP_ONE] == '.' && de->d_name[PAIR_LEN] == '\0'))) {
            continue;
        }
        size_t nlen = strlen(de->d_name);
        if (nlen >= CBM_DIRENT_NAME_MAX) {
            nlen = CBM_DIRENT_NAME_MAX - SKIP_ONE;
        }
        memcpy(d->entry.name, de->d_name, nlen);
        d->entry.name[nlen] = '\0';
        d->entry.is_dir = (de->d_type == DT_DIR);
        d->entry.d_type = de->d_type;
        return &d->entry;
    }
    return NULL;
}

void cbm_closedir(cbm_dir_t *d) {
    if (d) {
        if (d->dir) {
            closedir(d->dir);
        }
        free(d);
    }
}

FILE *cbm_popen(const char *cmd, const char *mode) {
    return popen(cmd, mode);
}

int cbm_pclose(FILE *f) {
    return pclose(f);
}

FILE *cbm_fopen(const char *path, const char *mode) {
    return fopen(path, mode);
}

bool cbm_mkdir_p(const char *path, int mode) {
    /* Try direct mkdir first */
    if (mkdir(path, (mode_t)mode) == 0) {
        return true;
    }
    /* Walk path and create each component */
    char *tmp = strdup(path);
    if (!tmp) {
        return false;
    }
    for (char *p = tmp + SKIP_ONE; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, (mode_t)mode); /* ignore intermediate errors */
            *p = '/';
        }
    }
    bool ok = (mkdir(tmp, (mode_t)mode) == 0 || errno == EEXIST) != 0;
    free(tmp);
    return ok;
}

int cbm_unlink(const char *path) {
    return unlink(path);
}

int cbm_rmdir(const char *path) {
    return rmdir(path);
}

int cbm_exec_no_shell(const char *const *argv) {
    if (!argv || !argv[0]) {
        return CBM_NOT_FOUND;
    }
    pid_t pid = fork();
    if (pid < 0) {
        return CBM_NOT_FOUND;
    }
    if (pid == 0) {
        /* Child: exec directly — no shell interpretation */
        /* 127 = standard "command not found" exit code (POSIX convention) */
        enum { EXEC_NOT_FOUND = 127 };
        execvp(argv[0], (char *const *)argv);
        _exit(EXEC_NOT_FOUND);
    }
    /* Parent: wait for child */
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        return CBM_NOT_FOUND;
    }
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    return CBM_NOT_FOUND; /* killed by signal */
}

#endif /* _WIN32 */
