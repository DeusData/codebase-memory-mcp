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
#include <io.h>     /* _wunlink */
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

FILE *cbm_popen(const char *cmd, const char *mode) {
    return _popen(cmd, mode);
}

int cbm_pclose(FILE *f) {
    return _pclose(f);
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
