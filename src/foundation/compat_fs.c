/*
 * compat_fs.c — Portable file system operations.
 *
 * POSIX: direct wrappers around opendir/readdir/closedir, popen/pclose, mkdir, unlink.
 * Windows: FindFirstFile/FindNextFile, _popen/_pclose, _mkdir, _unlink.
 */
#include "foundation/compat_fs.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32

/* ── Windows implementation ───────────────────────────────────── */

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <direct.h> /* _mkdir */
#include <io.h>     /* _unlink */

struct cbm_dir {
    HANDLE find_handle;
    WIN32_FIND_DATAA find_data;
    cbm_dirent_t entry;
    bool first;
    bool done;
};

cbm_dir_t *cbm_opendir(const char *path) {
    if (!path) {
        return NULL;
    }
    /* Build search pattern: "path\*" */
    size_t len = strlen(path);
    char *pattern = (char *)malloc(len + 3);
    if (!pattern) {
        return NULL;
    }
    memcpy(pattern, path, len);
    if (len > 0 && path[len - 1] != '\\' && path[len - 1] != '/') {
        pattern[len++] = '\\';
    }
    pattern[len++] = '*';
    pattern[len] = '\0';

    cbm_dir_t *d = (cbm_dir_t *)calloc(1, sizeof(cbm_dir_t));
    if (!d) {
        free(pattern);
        return NULL;
    }

    d->find_handle = FindFirstFileA(pattern, &d->find_data);
    free(pattern);
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
        if (!FindNextFileA(d->find_handle, &d->find_data)) {
            d->done = true;
            return NULL;
        }
    }
    d->first = false;

    /* Skip "." and ".." */
    while (d->find_data.cFileName[0] == '.' &&
           (d->find_data.cFileName[1] == '\0' ||
            (d->find_data.cFileName[1] == '.' && d->find_data.cFileName[2] == '\0'))) {
        if (!FindNextFileA(d->find_handle, &d->find_data)) {
            d->done = true;
            return NULL;
        }
    }

    size_t nlen = strlen(d->find_data.cFileName);
    if (nlen >= CBM_DIRENT_NAME_MAX) {
        nlen = CBM_DIRENT_NAME_MAX - 1;
    }
    memcpy(d->entry.name, d->find_data.cFileName, nlen);
    d->entry.name[nlen] = '\0';
    d->entry.is_dir = (d->find_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
    d->entry.d_type = 0; /* Not meaningful on Windows */
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

bool cbm_mkdir_p(const char *path, int mode) {
    (void)mode; /* Windows ignores POSIX permissions */
    /* Simple recursive mkdir: try creating, if fail walk parents */
    if (_mkdir(path) == 0) {
        return true;
    }
    /* Walk path and create each component */
    char *tmp = _strdup(path);
    if (!tmp) {
        return false;
    }
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/' || *p == '\\') {
            *p = '\0';
            _mkdir(tmp); /* ignore errors for intermediate dirs */
            *p = '\\';
        }
    }
    bool ok = _mkdir(tmp) == 0 || GetLastError() == ERROR_ALREADY_EXISTS;
    free(tmp);
    return ok;
}

int cbm_unlink(const char *path) {
    return _unlink(path);
}

int cbm_rmdir(const char *path) {
    return _rmdir(path);
}

int cbm_exec_no_shell(const char *const *argv) {
    if (!argv || !argv[0]) {
        return -1;
    }
    return (int)_spawnvp(_P_WAIT, argv[0], argv);
}

#else /* POSIX */

/* ── POSIX implementation ─────────────────────────────────────── */

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
    cbm_dir_t *d = (cbm_dir_t *)calloc(1, sizeof(cbm_dir_t));
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
            (de->d_name[1] == '\0' || (de->d_name[1] == '.' && de->d_name[2] == '\0'))) {
            continue;
        }
        size_t nlen = strlen(de->d_name);
        if (nlen >= CBM_DIRENT_NAME_MAX) {
            nlen = CBM_DIRENT_NAME_MAX - 1;
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
    // NOLINTNEXTLINE(cert-env33-c) — popen needed for git commands
    return popen(cmd, mode);
}

int cbm_pclose(FILE *f) {
    return pclose(f);
}

bool cbm_mkdir_p(const char *path, int mode) {
    /* Try direct mkdir first */
    if (mkdir(path, (mode_t)mode) == 0) {
        return true;
    }
    /* Walk path and create each component */
    // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
    char *tmp = strdup(path);
    if (!tmp) {
        return false;
    }
    for (char *p = tmp + 1; *p; p++) {
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
        return -1;
    }
    pid_t pid = fork();
    if (pid < 0) {
        return -1;
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
        return -1;
    }
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    return -1; /* killed by signal */
}

#endif /* _WIN32 */
