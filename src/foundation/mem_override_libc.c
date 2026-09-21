/*
 * mem_override_libc.c — the CPU lane of the waste sanitizer: byte work and
 * system calls, counted per call site.
 *
 * WHAT IT COUNTS. memcpy, memmove, memset, memcmp, strlen, strcmp, strncmp
 * (calls and bytes), read/write/pread/pwrite/fread/fwrite (bytes and tiny
 * calls), open/stat/fopen/opendir (failures and the same path again), readdir,
 * and pthread_mutex_lock (contended acquisitions; POSIX only -- Windows locks
 * are counted where cbm_mutex_lock takes them).
 * Calls the compiler inlines (small constant-size copies) are not calls and are
 * not counted: what is counted is exactly the work that reaches the library.
 *
 * HOW IT LISTENS, per platform
 *   Linux, Windows  the linker's --wrap redirects every reference in OUR
 *                   objects to __wrap_<name>; __real_<name> is the original.
 *                   The flavour adds the flags (Makefile.cbm, MEMWASTE=1).
 *   macOS           ld has no --wrap and dyld ignores __interpose tuples in the
 *                   main executable (proven 2026-09-17). Instead this file
 *                   DEFINES the symbols: references in our objects bind to the
 *                   definitions in our own image, and each definition reaches
 *                   libSystem's through dlsym(RTLD_NEXT). Calls made inside
 *                   libSystem keep libSystem's own functions. Fortified builds
 *                   call __memcpy_chk and friends, so those are defined too.
 *
 * The CALLER's return address is the site: this file is the observation point.
 * Compiled only into the `memwaste` flavour; dormant unless CBM_MEMWASTE=1.
 * Everything here forwards unconditionally: counting can only ever be skipped,
 * never the work.
 */
#if defined(CBM_MEMWASTE) && CBM_MEMWASTE

#include "foundation/mem_events.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if !defined(_WIN32)
#include <pthread.h>
#endif

#if defined(__clang__)
#pragma clang attribute push(__attribute__((no_sanitize("coverage"))), apply_to = function)
#endif

#define SITE() __builtin_return_address(0)

static inline void note_io(cbm_work_kind_t kind, void *site, long long got, size_t asked) {
    if (!cbm_memev_enabled()) {
        return;
    }
    uint64_t bytes = got > 0 ? (uint64_t)got : 0;
    cbm_work_note(kind, site, bytes, (asked > 0 && asked < CBM_WORK_TINY_IO) ? 1 : 0, 0);
}

/* Access lane: a library call's reads and writes, charged to its caller. */
#define ACC_LOAD(p, n) CBM_MEMEV_ACCESS((p), (n), false, SITE())
#define ACC_STORE(p, n) CBM_MEMEV_ACCESS((p), (n), true, SITE())

static inline void note_bytes(cbm_work_kind_t kind, void *site, size_t n) {
    if (cbm_memev_enabled()) {
        cbm_work_note(kind, site, n, 0, 0);
    }
}

/* ══════════════════════════════════════════════════════════════════════ */
#if defined(__APPLE__)

#include <dirent.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#undef memcpy
#undef memmove
#undef memset
#undef strlen
#undef strcmp
#undef strncmp
#undef memcmp

/* Resolved in a constructor. Until then the byte functions run a plain loop
 * (dlsym and the dynamic loader call them before any constructor), marked
 * no_builtin so the optimiser cannot turn the loop back into a call to itself. */
static void *(*r_memcpy)(void *, const void *, size_t);
static void *(*r_memmove)(void *, const void *, size_t);
static void *(*r_memset)(void *, int, size_t);
static int (*r_memcmp)(const void *, const void *, size_t);
static size_t (*r_strlen)(const char *);
static int (*r_strcmp)(const char *, const char *);
static int (*r_strncmp)(const char *, const char *, size_t);
static void *(*r_memcpy_chk)(void *, const void *, size_t, size_t);
static void *(*r_memmove_chk)(void *, const void *, size_t, size_t);
static void *(*r_memset_chk)(void *, int, size_t, size_t);
static ssize_t (*r_read)(int, void *, size_t);
static ssize_t (*r_write)(int, const void *, size_t);
static ssize_t (*r_pread)(int, void *, size_t, off_t);
static ssize_t (*r_pwrite)(int, const void *, size_t, off_t);
static int (*r_open)(const char *, int, ...);
static int (*r_stat)(const char *, struct stat *);
static int (*r_lstat)(const char *, struct stat *);
static FILE *(*r_fopen)(const char *, const char *);
static size_t (*r_fread)(void *, size_t, size_t, FILE *);
static size_t (*r_fwrite)(const void *, size_t, size_t, FILE *);
static DIR *(*r_opendir)(const char *);
static struct dirent *(*r_readdir)(DIR *);
static int (*r_mutex_lock)(pthread_mutex_t *);
static int (*r_mutex_trylock)(pthread_mutex_t *);

__attribute__((constructor(101))) static void libc_resolve(void) {
    r_memcpy = dlsym(RTLD_NEXT, "memcpy");
    r_memmove = dlsym(RTLD_NEXT, "memmove");
    r_memset = dlsym(RTLD_NEXT, "memset");
    r_memcmp = dlsym(RTLD_NEXT, "memcmp");
    r_strlen = dlsym(RTLD_NEXT, "strlen");
    r_strcmp = dlsym(RTLD_NEXT, "strcmp");
    r_strncmp = dlsym(RTLD_NEXT, "strncmp");
    r_memcpy_chk = dlsym(RTLD_NEXT, "__memcpy_chk");
    r_memmove_chk = dlsym(RTLD_NEXT, "__memmove_chk");
    r_memset_chk = dlsym(RTLD_NEXT, "__memset_chk");
    r_read = dlsym(RTLD_NEXT, "read");
    r_write = dlsym(RTLD_NEXT, "write");
    r_pread = dlsym(RTLD_NEXT, "pread");
    r_pwrite = dlsym(RTLD_NEXT, "pwrite");
    r_open = dlsym(RTLD_NEXT, "open");
    r_stat = dlsym(RTLD_NEXT, "stat");
    r_lstat = dlsym(RTLD_NEXT, "lstat");
    r_fopen = dlsym(RTLD_NEXT, "fopen");
    r_fread = dlsym(RTLD_NEXT, "fread");
    r_fwrite = dlsym(RTLD_NEXT, "fwrite");
    r_opendir = dlsym(RTLD_NEXT, "opendir");
    r_readdir = dlsym(RTLD_NEXT, "readdir");
    r_mutex_lock = dlsym(RTLD_NEXT, "pthread_mutex_lock");
    r_mutex_trylock = dlsym(RTLD_NEXT, "pthread_mutex_trylock");
}

/* System calls are only reachable after libc_resolve; a call from an earlier
 * constructor resolves its own target once. */
#define RESOLVED(ptr, name) ((ptr) ? (ptr) : ((ptr) = dlsym(RTLD_NEXT, name)))

/* Spelled through a macro only because the three builtin names plus this
 * signature come to 117 columns, and a wrapped attribute is the one thing
 * clang-format 20 and 22 lay out differently -- CI runs 20, the local lane runs
 * Homebrew's 22, so the wrapped form passed here and failed there. Fitting the
 * line under the 100-column limit leaves no wrapping decision to disagree on.
 * The siblings below fit as they are and stay inline. */
#define NO_BUILTIN_MEM __attribute__((no_builtin("memcpy", "memmove", "memset")))

NO_BUILTIN_MEM static void *slow_move(void *d, const void *s, size_t n) {
    unsigned char *dd = (unsigned char *)d;
    const unsigned char *ss = (const unsigned char *)s;
    if (dd < ss) {
        for (size_t i = 0; i < n; i++) {
            dd[i] = ss[i];
        }
    } else {
        for (size_t i = n; i > 0; i--) {
            dd[i - 1] = ss[i - 1];
        }
    }
    return d;
}

__attribute__((no_builtin("memset"))) static void *slow_set(void *d, int c, size_t n) {
    unsigned char *dd = (unsigned char *)d;
    for (size_t i = 0; i < n; i++) {
        dd[i] = (unsigned char)c;
    }
    return d;
}

__attribute__((no_builtin("strlen"))) static size_t slow_strlen(const char *s) {
    size_t i = 0;
    while (s[i]) {
        i++;
    }
    return i;
}

__attribute__((no_builtin("memcmp", "strcmp", "strncmp"))) static int slow_ncmp(const char *a,
                                                                                const char *b,
                                                                                size_t n,
                                                                                bool stop_at_nul) {
    for (size_t i = 0; i < n; i++) {
        unsigned char x = (unsigned char)a[i];
        unsigned char y = (unsigned char)b[i];
        if (x != y) {
            return x < y ? -1 : 1;
        }
        if (stop_at_nul && x == 0) {
            return 0;
        }
    }
    return 0;
}

void *(memcpy)(void *restrict d, const void *restrict s, size_t n) {
    note_bytes(CBM_WORK_MEMCPY, SITE(), n);
    ACC_LOAD(s, n);
    ACC_STORE(d, n);
    return r_memcpy ? r_memcpy(d, s, n) : slow_move(d, s, n);
}

void *(memmove)(void *d, const void *s, size_t n) {
    note_bytes(CBM_WORK_MEMMOVE, SITE(), n);
    ACC_LOAD(s, n);
    ACC_STORE(d, n);
    return r_memmove ? r_memmove(d, s, n) : slow_move(d, s, n);
}

void *(memset)(void *d, int c, size_t n) {
    note_bytes(CBM_WORK_MEMSET, SITE(), n);
    ACC_STORE(d, n);
    return r_memset ? r_memset(d, c, n) : slow_set(d, c, n);
}

int(memcmp)(const void *a, const void *b, size_t n) {
    note_bytes(CBM_WORK_MEMCMP, SITE(), n);
    ACC_LOAD(a, n);
    ACC_LOAD(b, n);
    return r_memcmp ? r_memcmp(a, b, n) : slow_ncmp((const char *)a, (const char *)b, n, false);
}

size_t(strlen)(const char *s) {
    size_t n = r_strlen ? r_strlen(s) : slow_strlen(s);
    if (cbm_memev_enabled()) {
        cbm_work_note_strlen(SITE(), s, n);
        ACC_LOAD(s, n + 1);
    }
    return n;
}

int(strcmp)(const char *a, const char *b) {
    note_bytes(CBM_WORK_STRCMP, SITE(), 0);
    ACC_LOAD(a, 1);
    ACC_LOAD(b, 1);
    return r_strcmp ? r_strcmp(a, b) : slow_ncmp(a, b, (size_t)-1, true);
}

int(strncmp)(const char *a, const char *b, size_t n) {
    note_bytes(CBM_WORK_STRNCMP, SITE(), n);
    ACC_LOAD(a, 1);
    ACC_LOAD(b, 1);
    return r_strncmp ? r_strncmp(a, b, n) : slow_ncmp(a, b, n, true);
}

void *__memcpy_chk(void *d, const void *s, size_t n, size_t dn) {
    note_bytes(CBM_WORK_MEMCPY, SITE(), n);
    ACC_LOAD(s, n);
    ACC_STORE(d, n);
    return RESOLVED(r_memcpy_chk, "__memcpy_chk")(d, s, n, dn);
}

void *__memmove_chk(void *d, const void *s, size_t n, size_t dn) {
    note_bytes(CBM_WORK_MEMMOVE, SITE(), n);
    ACC_LOAD(s, n);
    ACC_STORE(d, n);
    return RESOLVED(r_memmove_chk, "__memmove_chk")(d, s, n, dn);
}

void *__memset_chk(void *d, int c, size_t n, size_t dn) {
    note_bytes(CBM_WORK_MEMSET, SITE(), n);
    ACC_STORE(d, n);
    return RESOLVED(r_memset_chk, "__memset_chk")(d, c, n, dn);
}

ssize_t read(int fd, void *buf, size_t n) {
    ssize_t got = RESOLVED(r_read, "read")(fd, buf, n);
    note_io(CBM_WORK_READ, SITE(), (long long)got, n);
    ACC_STORE(buf, got > 0 ? (size_t)got : 0);
    return got;
}

ssize_t write(int fd, const void *buf, size_t n) {
    ssize_t put = RESOLVED(r_write, "write")(fd, buf, n);
    note_io(CBM_WORK_WRITE, SITE(), (long long)put, n);
    ACC_LOAD(buf, put > 0 ? (size_t)put : 0);
    return put;
}

ssize_t pread(int fd, void *buf, size_t n, off_t off) {
    ssize_t got = RESOLVED(r_pread, "pread")(fd, buf, n, off);
    note_io(CBM_WORK_PREAD, SITE(), (long long)got, n);
    ACC_STORE(buf, got > 0 ? (size_t)got : 0);
    return got;
}

ssize_t pwrite(int fd, const void *buf, size_t n, off_t off) {
    ssize_t put = RESOLVED(r_pwrite, "pwrite")(fd, buf, n, off);
    note_io(CBM_WORK_PWRITE, SITE(), (long long)put, n);
    ACC_LOAD(buf, put > 0 ? (size_t)put : 0);
    return put;
}

int open(const char *path, int flags, ...) {
    int mode = 0;
    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, flags);
        mode = va_arg(ap, int);
        va_end(ap);
    }
    int fd = RESOLVED(r_open, "open")(path, flags, mode);
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_OPEN, SITE(), path, fd < 0);
    }
    return fd;
}

int stat(const char *path, struct stat *st) {
    int rc = RESOLVED(r_stat, "stat")(path, st);
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_STAT, SITE(), path, rc != 0);
    }
    return rc;
}

int lstat(const char *path, struct stat *st) {
    int rc = RESOLVED(r_lstat, "lstat")(path, st);
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_STAT, SITE(), path, rc != 0);
    }
    return rc;
}

FILE *fopen(const char *restrict path, const char *restrict mode) {
    cbm_memev_library_enter(SITE());
    FILE *f = RESOLVED(r_fopen, "fopen")(path, mode);
    cbm_memev_library_leave();
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_FOPEN, SITE(), path, f == NULL);
    }
    return f;
}

size_t fread(void *restrict buf, size_t size, size_t count, FILE *restrict f) {
    cbm_memev_library_enter(SITE());
    size_t got = RESOLVED(r_fread, "fread")(buf, size, count, f);
    cbm_memev_library_leave();
    note_io(CBM_WORK_FREAD, SITE(), (long long)(got * size), size * count);
    ACC_STORE(buf, got * size);
    return got;
}

size_t fwrite(const void *restrict buf, size_t size, size_t count, FILE *restrict f) {
    cbm_memev_library_enter(SITE());
    size_t put = RESOLVED(r_fwrite, "fwrite")(buf, size, count, f);
    cbm_memev_library_leave();
    note_io(CBM_WORK_FWRITE, SITE(), (long long)(put * size), size * count);
    ACC_LOAD(buf, put * size);
    return put;
}

DIR *opendir(const char *path) {
    cbm_memev_library_enter(SITE());
    DIR *d = RESOLVED(r_opendir, "opendir")(path);
    cbm_memev_library_leave();
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_OPENDIR, SITE(), path, d == NULL);
    }
    return d;
}

struct dirent *readdir(DIR *d) {
    note_bytes(CBM_WORK_READDIR, SITE(), 0);
    cbm_memev_library_enter(SITE());
    struct dirent *ent = RESOLVED(r_readdir, "readdir")(d);
    cbm_memev_library_leave();
    return ent;
}

int cbm_memev_mutex_lock(void *pthread_mutex, void *site) {
    pthread_mutex_t *m = (pthread_mutex_t *)pthread_mutex;
    int (*lock)(pthread_mutex_t *) = RESOLVED(r_mutex_lock, "pthread_mutex_lock");
    if (!cbm_memev_enabled()) {
        return lock(m);
    }
    int rc = RESOLVED(r_mutex_trylock, "pthread_mutex_trylock")(m);
    bool contended = rc == EBUSY;
    if (contended) {
        rc = lock(m);
    }
    cbm_work_note(CBM_WORK_MUTEX, site, 0, contended ? 1 : 0, 0);
    return rc;
}

int pthread_mutex_lock(pthread_mutex_t *m) {
    return cbm_memev_mutex_lock(m, SITE());
}

/* ══════════════════════════════════════════════════════════════════════ */
#elif defined(__linux__)

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

void *__real_memcpy(void *, const void *, size_t);
void *__real_memmove(void *, const void *, size_t);
void *__real_memset(void *, int, size_t);
int __real_memcmp(const void *, const void *, size_t);
size_t __real_strlen(const char *);
int __real_strcmp(const char *, const char *);
int __real_strncmp(const char *, const char *, size_t);
void *__real___memcpy_chk(void *, const void *, size_t, size_t);
void *__real___memmove_chk(void *, const void *, size_t, size_t);
void *__real___memset_chk(void *, int, size_t, size_t);
ssize_t __real_read(int, void *, size_t);
ssize_t __real___read_chk(int, void *, size_t, size_t);
ssize_t __real_write(int, const void *, size_t);
ssize_t __real_pread(int, void *, size_t, off_t);
ssize_t __real_pwrite(int, const void *, size_t, off_t);
ssize_t __real_pread64(int, void *, size_t, off_t);
ssize_t __real_pwrite64(int, const void *, size_t, off_t);
int __real_open(const char *, int, ...);
int __real_open64(const char *, int, ...);
int __real___open_2(const char *, int);
int __real___open64_2(const char *, int);
int __real_stat(const char *, struct stat *);
int __real_lstat(const char *, struct stat *);
FILE *__real_fopen(const char *, const char *);
FILE *__real_fopen64(const char *, const char *);
size_t __real_fread(void *, size_t, size_t, FILE *);
size_t __real___fread_chk(void *, size_t, size_t, size_t, FILE *);
size_t __real_fwrite(const void *, size_t, size_t, FILE *);
DIR *__real_opendir(const char *);
struct dirent *__real_readdir(DIR *);
int __real_pthread_mutex_lock(pthread_mutex_t *);

void *__wrap_memcpy(void *d, const void *s, size_t n) {
    note_bytes(CBM_WORK_MEMCPY, SITE(), n);
    ACC_LOAD(s, n);
    ACC_STORE(d, n);
    return __real_memcpy(d, s, n);
}
void *__wrap_memmove(void *d, const void *s, size_t n) {
    note_bytes(CBM_WORK_MEMMOVE, SITE(), n);
    ACC_LOAD(s, n);
    ACC_STORE(d, n);
    return __real_memmove(d, s, n);
}
void *__wrap_memset(void *d, int c, size_t n) {
    note_bytes(CBM_WORK_MEMSET, SITE(), n);
    ACC_STORE(d, n);
    return __real_memset(d, c, n);
}
int __wrap_memcmp(const void *a, const void *b, size_t n) {
    note_bytes(CBM_WORK_MEMCMP, SITE(), n);
    ACC_LOAD(a, n);
    ACC_LOAD(b, n);
    return __real_memcmp(a, b, n);
}
size_t __wrap_strlen(const char *s) {
    size_t n = __real_strlen(s);
    if (cbm_memev_enabled()) {
        cbm_work_note_strlen(SITE(), s, n);
        ACC_LOAD(s, n + 1);
    }
    return n;
}
int __wrap_strcmp(const char *a, const char *b) {
    note_bytes(CBM_WORK_STRCMP, SITE(), 0);
    ACC_LOAD(a, 1);
    ACC_LOAD(b, 1);
    return __real_strcmp(a, b);
}
int __wrap_strncmp(const char *a, const char *b, size_t n) {
    note_bytes(CBM_WORK_STRNCMP, SITE(), n);
    ACC_LOAD(a, 1);
    ACC_LOAD(b, 1);
    return __real_strncmp(a, b, n);
}
void *__wrap___memcpy_chk(void *d, const void *s, size_t n, size_t dn) {
    note_bytes(CBM_WORK_MEMCPY, SITE(), n);
    ACC_LOAD(s, n);
    ACC_STORE(d, n);
    return __real___memcpy_chk(d, s, n, dn);
}
void *__wrap___memmove_chk(void *d, const void *s, size_t n, size_t dn) {
    note_bytes(CBM_WORK_MEMMOVE, SITE(), n);
    ACC_LOAD(s, n);
    ACC_STORE(d, n);
    return __real___memmove_chk(d, s, n, dn);
}
void *__wrap___memset_chk(void *d, int c, size_t n, size_t dn) {
    note_bytes(CBM_WORK_MEMSET, SITE(), n);
    ACC_STORE(d, n);
    return __real___memset_chk(d, c, n, dn);
}
ssize_t __wrap_read(int fd, void *buf, size_t n) {
    ssize_t got = __real_read(fd, buf, n);
    note_io(CBM_WORK_READ, SITE(), (long long)got, n);
    ACC_STORE(buf, got > 0 ? (size_t)got : 0);
    return got;
}
ssize_t __wrap___read_chk(int fd, void *buf, size_t n, size_t bn) {
    ssize_t got = __real___read_chk(fd, buf, n, bn);
    note_io(CBM_WORK_READ, SITE(), (long long)got, n);
    ACC_STORE(buf, got > 0 ? (size_t)got : 0);
    return got;
}
ssize_t __wrap_write(int fd, const void *buf, size_t n) {
    ssize_t put = __real_write(fd, buf, n);
    note_io(CBM_WORK_WRITE, SITE(), (long long)put, n);
    ACC_LOAD(buf, put > 0 ? (size_t)put : 0);
    return put;
}
ssize_t __wrap_pread(int fd, void *buf, size_t n, off_t off) {
    ssize_t got = __real_pread(fd, buf, n, off);
    note_io(CBM_WORK_PREAD, SITE(), (long long)got, n);
    ACC_STORE(buf, got > 0 ? (size_t)got : 0);
    return got;
}
ssize_t __wrap_pwrite(int fd, const void *buf, size_t n, off_t off) {
    ssize_t put = __real_pwrite(fd, buf, n, off);
    note_io(CBM_WORK_PWRITE, SITE(), (long long)put, n);
    ACC_LOAD(buf, put > 0 ? (size_t)put : 0);
    return put;
}
ssize_t __wrap_pread64(int fd, void *buf, size_t n, off_t off) {
    ssize_t got = __real_pread64(fd, buf, n, off);
    note_io(CBM_WORK_PREAD, SITE(), (long long)got, n);
    ACC_STORE(buf, got > 0 ? (size_t)got : 0);
    return got;
}
ssize_t __wrap_pwrite64(int fd, const void *buf, size_t n, off_t off) {
    ssize_t put = __real_pwrite64(fd, buf, n, off);
    note_io(CBM_WORK_PWRITE, SITE(), (long long)put, n);
    ACC_LOAD(buf, put > 0 ? (size_t)put : 0);
    return put;
}
static int open_mode(int flags, va_list ap) {
    return (flags & O_CREAT) ? va_arg(ap, int) : 0;
}
int __wrap_open(const char *path, int flags, ...) {
    va_list ap;
    va_start(ap, flags);
    int mode = open_mode(flags, ap);
    va_end(ap);
    int fd = __real_open(path, flags, mode);
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_OPEN, SITE(), path, fd < 0);
    }
    return fd;
}
int __wrap_open64(const char *path, int flags, ...) {
    va_list ap;
    va_start(ap, flags);
    int mode = open_mode(flags, ap);
    va_end(ap);
    int fd = __real_open64(path, flags, mode);
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_OPEN, SITE(), path, fd < 0);
    }
    return fd;
}
int __wrap___open_2(const char *path, int flags) {
    int fd = __real___open_2(path, flags);
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_OPEN, SITE(), path, fd < 0);
    }
    return fd;
}
int __wrap___open64_2(const char *path, int flags) {
    int fd = __real___open64_2(path, flags);
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_OPEN, SITE(), path, fd < 0);
    }
    return fd;
}
int __wrap_stat(const char *path, struct stat *st) {
    int rc = __real_stat(path, st);
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_STAT, SITE(), path, rc != 0);
    }
    return rc;
}
int __wrap_lstat(const char *path, struct stat *st) {
    int rc = __real_lstat(path, st);
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_STAT, SITE(), path, rc != 0);
    }
    return rc;
}
FILE *__wrap_fopen(const char *path, const char *mode) {
    FILE *f = __real_fopen(path, mode);
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_FOPEN, SITE(), path, f == NULL);
    }
    return f;
}
FILE *__wrap_fopen64(const char *path, const char *mode) {
    FILE *f = __real_fopen64(path, mode);
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_FOPEN, SITE(), path, f == NULL);
    }
    return f;
}
size_t __wrap_fread(void *buf, size_t size, size_t count, FILE *f) {
    size_t got = __real_fread(buf, size, count, f);
    note_io(CBM_WORK_FREAD, SITE(), (long long)(got * size), size * count);
    ACC_STORE(buf, got * size);
    return got;
}
size_t __wrap___fread_chk(void *buf, size_t bn, size_t size, size_t count, FILE *f) {
    size_t got = __real___fread_chk(buf, bn, size, count, f);
    note_io(CBM_WORK_FREAD, SITE(), (long long)(got * size), size * count);
    ACC_STORE(buf, got * size);
    return got;
}
size_t __wrap_fwrite(const void *buf, size_t size, size_t count, FILE *f) {
    size_t put = __real_fwrite(buf, size, count, f);
    note_io(CBM_WORK_FWRITE, SITE(), (long long)(put * size), size * count);
    ACC_LOAD(buf, put * size);
    return put;
}
DIR *__wrap_opendir(const char *path) {
    DIR *d = __real_opendir(path);
    if (cbm_memev_enabled()) {
        cbm_work_note_path(CBM_WORK_OPENDIR, SITE(), path, d == NULL);
    }
    return d;
}
struct dirent *__wrap_readdir(DIR *d) {
    note_bytes(CBM_WORK_READDIR, SITE(), 0);
    return __real_readdir(d);
}

int cbm_memev_mutex_lock(void *pthread_mutex, void *site) {
    pthread_mutex_t *m = (pthread_mutex_t *)pthread_mutex;
    if (!cbm_memev_enabled()) {
        return __real_pthread_mutex_lock(m);
    }
    int rc = pthread_mutex_trylock(m);
    bool contended = rc == EBUSY;
    if (contended) {
        rc = __real_pthread_mutex_lock(m);
    }
    cbm_work_note(CBM_WORK_MUTEX, site, 0, contended ? 1 : 0, 0);
    return rc;
}

int __wrap_pthread_mutex_lock(pthread_mutex_t *m) {
    return cbm_memev_mutex_lock(m, SITE());
}

/* ══════════════════════════════════════════════════════════════════════ */
#elif defined(_WIN32)

#include <wchar.h>

void *__real_memcpy(void *, const void *, size_t);
void *__real_memmove(void *, const void *, size_t);
void *__real_memset(void *, int, size_t);
int __real_memcmp(const void *, const void *, size_t);
size_t __real_strlen(const char *);
int __real_strcmp(const char *, const char *);
int __real_strncmp(const char *, const char *, size_t);
FILE *__real__wfopen(const wchar_t *, const wchar_t *);
size_t __real_fread(void *, size_t, size_t, FILE *);
size_t __real_fwrite(const void *, size_t, size_t, FILE *);

void *__wrap_memcpy(void *d, const void *s, size_t n) {
    note_bytes(CBM_WORK_MEMCPY, SITE(), n);
    ACC_LOAD(s, n);
    ACC_STORE(d, n);
    return __real_memcpy(d, s, n);
}
void *__wrap_memmove(void *d, const void *s, size_t n) {
    note_bytes(CBM_WORK_MEMMOVE, SITE(), n);
    ACC_LOAD(s, n);
    ACC_STORE(d, n);
    return __real_memmove(d, s, n);
}
void *__wrap_memset(void *d, int c, size_t n) {
    note_bytes(CBM_WORK_MEMSET, SITE(), n);
    ACC_STORE(d, n);
    return __real_memset(d, c, n);
}
int __wrap_memcmp(const void *a, const void *b, size_t n) {
    note_bytes(CBM_WORK_MEMCMP, SITE(), n);
    ACC_LOAD(a, n);
    ACC_LOAD(b, n);
    return __real_memcmp(a, b, n);
}
size_t __wrap_strlen(const char *s) {
    size_t n = __real_strlen(s);
    if (cbm_memev_enabled()) {
        cbm_work_note_strlen(SITE(), s, n);
        ACC_LOAD(s, n + 1);
    }
    return n;
}
int __wrap_strcmp(const char *a, const char *b) {
    note_bytes(CBM_WORK_STRCMP, SITE(), 0);
    ACC_LOAD(a, 1);
    ACC_LOAD(b, 1);
    return __real_strcmp(a, b);
}
int __wrap_strncmp(const char *a, const char *b, size_t n) {
    note_bytes(CBM_WORK_STRNCMP, SITE(), n);
    ACC_LOAD(a, 1);
    ACC_LOAD(b, 1);
    return __real_strncmp(a, b, n);
}
/* A wide path's hash is taken over its UTF-16 units squeezed to bytes: equal
 * paths hash equal, which is all the repeat detector needs. */
FILE *__wrap__wfopen(const wchar_t *path, const wchar_t *mode) {
    FILE *f = __real__wfopen(path, mode);
    if (cbm_memev_enabled()) {
        char narrow[1024];
        size_t i = 0;
        for (; path && path[i] && i + 1 < sizeof(narrow); i++) {
            narrow[i] = (char)(path[i] & 0x7F ? path[i] : '?');
        }
        narrow[i] = '\0';
        cbm_work_note_path(CBM_WORK_FOPEN, SITE(), narrow, f == NULL);
    }
    return f;
}
size_t __wrap_fread(void *buf, size_t size, size_t count, FILE *f) {
    size_t got = __real_fread(buf, size, count, f);
    note_io(CBM_WORK_FREAD, SITE(), (long long)(got * size), size * count);
    ACC_STORE(buf, got * size);
    return got;
}
size_t __wrap_fwrite(const void *buf, size_t size, size_t count, FILE *f) {
    size_t put = __real_fwrite(buf, size, count, f);
    note_io(CBM_WORK_FWRITE, SITE(), (long long)(put * size), size * count);
    ACC_LOAD(buf, put * size);
    return put;
}

#endif /* platform */

#if defined(__clang__)
#pragma clang attribute pop
#endif

#else
typedef int cbm_mem_override_libc_unused_t;
#endif /* CBM_MEMWASTE */
