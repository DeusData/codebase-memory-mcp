/*
 * compat_fs.c — Portable file system operations.
 *
 * POSIX: direct wrappers around opendir/readdir/closedir, popen/pclose, mkdir, unlink.
 * Windows: FindFirstFile/FindNextFile, _popen/_pclose, _mkdir, _unlink.
 */
#include "foundation/constants.h"
#include "foundation/compat_fs.h"

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
#include <fcntl.h>  /* _O_RDONLY */
#include <io.h>     /* _wunlink, _open_osfhandle, _close */
#include <stdint.h> /* intptr_t */
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
 * The fix: spawn via CreateProcess with STARTUPINFOEX + an explicit
 * PROC_THREAD_ATTRIBUTE_HANDLE_LIST containing only the stdout write-end and a
 * NUL handle for stdin/stderr. Nothing else crosses into git, so there is no
 * foreign handle to deadlock on. POSIX popen() already sets O_CLOEXEC on its
 * pipe, so the POSIX path is unchanged. */

enum { CBM_POPEN_MAX = 16 };
static struct {
    FILE *fp;
    HANDLE proc;
} g_popen_tab[CBM_POPEN_MAX];
static CRITICAL_SECTION g_popen_lock;
static INIT_ONCE g_popen_once = INIT_ONCE_STATIC_INIT;

static BOOL CALLBACK cbm_popen_init(PINIT_ONCE once, PVOID param, PVOID *ctx) {
    (void)once;
    (void)param;
    (void)ctx;
    InitializeCriticalSection(&g_popen_lock);
    return TRUE;
}

static FILE *cbm_popen_isolated(const char *cmd) {
    InitOnceExecuteOnce(&g_popen_once, cbm_popen_init, NULL, NULL);

    SECURITY_ATTRIBUTES sa;
    sa.nLength = sizeof(sa);
    sa.lpSecurityDescriptor = NULL;
    sa.bInheritHandle = TRUE;

    HANDLE rd = NULL, wr = NULL;
    if (!CreatePipe(&rd, &wr, &sa, 0)) {
        return NULL;
    }
    /* The parent read-end must never cross into the child. */
    SetHandleInformation(rd, HANDLE_FLAG_INHERIT, 0);

    /* NUL for the child's stdin/stderr so it never touches our real stdin pipe. */
    HANDLE nul = CreateFileA("NUL", GENERIC_READ | GENERIC_WRITE,
                             FILE_SHARE_READ | FILE_SHARE_WRITE, &sa, OPEN_EXISTING, 0, NULL);

    HANDLE inherit[2];
    DWORD ninherit = 0;
    inherit[ninherit++] = wr;
    if (nul != INVALID_HANDLE_VALUE) {
        inherit[ninherit++] = nul;
    }

    SIZE_T attr_sz = 0;
    InitializeProcThreadAttributeList(NULL, 1, 0, &attr_sz);
    LPPROC_THREAD_ATTRIBUTE_LIST attr = (LPPROC_THREAD_ATTRIBUTE_LIST)malloc(attr_sz);
    BOOL prepared = attr && InitializeProcThreadAttributeList(attr, 1, 0, &attr_sz) &&
                    UpdateProcThreadAttribute(attr, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, inherit,
                                              ninherit * sizeof(HANDLE), NULL, NULL);

    STARTUPINFOEXA si;
    ZeroMemory(&si, sizeof(si));
    si.StartupInfo.cb = sizeof(si);
    si.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    si.StartupInfo.hStdInput = nul;
    si.StartupInfo.hStdOutput = wr;
    si.StartupInfo.hStdError = nul;
    si.lpAttributeList = attr;

    /* Run through cmd.exe /c so command quoting and `2>NUL` behave as under _popen. */
    char cmdline[2048];
    int n = snprintf(cmdline, sizeof(cmdline), "cmd.exe /c %s", cmd);

    PROCESS_INFORMATION pi;
    ZeroMemory(&pi, sizeof(pi));
    BOOL created = prepared && n > 0 && n < (int)sizeof(cmdline) &&
                   CreateProcessA(NULL, cmdline, NULL, NULL, TRUE, EXTENDED_STARTUPINFO_PRESENT,
                                  NULL, NULL, &si.StartupInfo, &pi);

    if (attr) {
        DeleteProcThreadAttributeList(attr);
        free(attr);
    }
    CloseHandle(wr); /* the child owns the write-end now */
    if (nul != INVALID_HANDLE_VALUE) {
        CloseHandle(nul);
    }
    if (!created) {
        CloseHandle(rd);
        return NULL;
    }
    CloseHandle(pi.hThread);

    int fd = _open_osfhandle((intptr_t)rd, _O_RDONLY);
    if (fd == -1) {
        CloseHandle(rd);
        CloseHandle(pi.hProcess);
        return NULL;
    }
    FILE *fp = _fdopen(fd, "r"); /* takes ownership of fd/rd */
    if (!fp) {
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
    CloseHandle(pi.hProcess);
    fclose(fp);
    return NULL;
}

FILE *cbm_popen(const char *cmd, const char *mode) {
    /* Our git shell-outs are all read-mode; only those need the isolation. */
    if (mode && mode[0] == 'r' && mode[1] == '\0') {
        FILE *fp = cbm_popen_isolated(cmd);
        if (fp) {
            return fp;
        }
        /* fall through to _popen if isolated spawn failed for any reason */
    }
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
        return _pclose(f); /* opened via the _popen fallback */
    }
    fclose(f);
    WaitForSingleObject(proc, INFINITE);
    DWORD code = 0;
    GetExitCodeProcess(proc, &code);
    CloseHandle(proc);
    return (int)code;
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

int cbm_exec_no_shell(const char *const *argv) {
    if (!argv || !argv[0]) {
        return CBM_NOT_FOUND;
    }
    return (int)_spawnvp(_P_WAIT, argv[0], argv);
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
