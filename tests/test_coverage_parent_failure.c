/* Native soft-limit fixture; production never consumes this fixture's mode. */
#if defined(CBM_TEST_COVERAGE) && !defined(_WIN32)
#include "test_framework.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static bool restricted_requested;
static bool fixture_entered;
static volatile unsigned int count_sink;

__attribute__((noinline)) void cbm_cov_parent_failure_child_count(void) {
    count_sink++;
}

int tf_coverage_parent_failure_args(int *argc, char **argv) {
    int position = -1;
    for (int i = 1; i < *argc; i++) {
        if (strcmp(argv[i], "--coverage-marker-open-emfile") == 0) {
            if (position >= 0)
                goto invalid;
            position = i;
        }
    }
    if (position < 0)
        return 0;
    if (*argc != 3 || strcmp(argv[position == 1 ? 2 : 1], "coverage_parent_failure") != 0)
        goto invalid;
    for (int i = position; i < *argc; i++)
        argv[i] = argv[i + 1];
    (*argc)--;
    restricted_requested = true;
    return 0;
invalid:
    fprintf(stderr,
            "coverage parent fixture usage error: restricted option requires only its suite\n");
    return -1;
}

void tf_coverage_parent_failure_finish(void) {
    if (restricted_requested && !fixture_entered) {
        fprintf(stderr, "coverage parent fixture usage error: requested fixture did not enter\n");
        tf_fail_count++;
    }
}

typedef struct {
    int control_errno;
    bool valid;
    struct rlimit observed;
    struct rlimit restored;
} ChildReceipt;

typedef struct {
    pid_t pid;
    int ack[2];
    int gate[2];
    struct rlimit saved;
    struct rlimit parent_restored;
    bool parent_restore_pending;
    bool parent_restore_failed;
    bool reaped;
    int status;
    char control[4096];
    char marker[4096];
    ChildReceipt child;
} Fixture;

static bool same_limit(const struct rlimit *a, const struct rlimit *b) {
    return a->rlim_cur == b->rlim_cur && a->rlim_max == b->rlim_max;
}

static bool transfer(int fd, void *bytes, size_t size, bool writing) {
    unsigned char *cursor = bytes;
    while (size) {
        struct pollfd item = {.fd = fd, .events = writing ? POLLOUT : POLLIN};
        int ready;
        do {
            ready = poll(&item, 1, 60000);
        } while (ready < 0 && errno == EINTR);
        if (ready <= 0)
            return false;
        ssize_t n;
        do {
            n = writing ? write(fd, cursor, size) : read(fd, cursor, size);
        } while (n < 0 && errno == EINTR);
        if (n <= 0)
            return false;
        cursor += (size_t)n;
        size -= (size_t)n;
    }
    return true;
}

static bool close_fd(int *fd) {
    if (*fd < 0)
        return true;
    int owned = *fd;
    *fd = -1;
    return close(owned) == 0;
}

static bool restore_parent(Fixture *f) {
    if (f->parent_restore_pending) {
        f->parent_restore_pending = false;
        if (setrlimit(RLIMIT_NOFILE, &f->saved) != 0) {
            f->parent_restore_failed = true;
            return false;
        }
    }
    if (getrlimit(RLIMIT_NOFILE, &f->parent_restored) != 0 ||
        !same_limit(&f->saved, &f->parent_restored)) {
        f->parent_restore_failed = true;
        return false;
    }
    return true;
}

static void child_run(Fixture *f) __attribute__((noreturn));
static void child_run(Fixture *f) {
    ChildReceipt receipt = {0};
    bool ok = close_fd(&f->ack[0]);
    if (!close_fd(&f->gate[1]))
        ok = false;
    if (getrlimit(RLIMIT_NOFILE, &receipt.observed) != 0)
        ok = false;
    errno = 0;
    int fd = open(f->control, O_RDONLY);
    receipt.control_errno = fd < 0 ? errno : 0;
    if (fd >= 0 && close(fd) != 0)
        ok = false;
    if (restricted_requested && setrlimit(RLIMIT_NOFILE, &f->saved) != 0)
        ok = false;
    if (getrlimit(RLIMIT_NOFILE, &receipt.restored) != 0 ||
        !same_limit(&f->saved, &receipt.restored))
        ok = false;
    if (receipt.observed.rlim_max != f->saved.rlim_max ||
        receipt.observed.rlim_cur != (restricted_requested ? 0 : f->saved.rlim_cur) ||
        receipt.control_errno != (restricted_requested ? EMFILE : 0))
        ok = false;
    receipt.valid = ok;
    bool sent = transfer(f->ack[1], &receipt, sizeof(receipt), true);
    if (!sent || !ok)
        _exit(90);
    char release = 0;
    if (!transfer(f->gate[0], &release, 1, false) || release != 'G')
        _exit(91);
    if (!close_fd(&f->ack[1]) || !close_fd(&f->gate[0]))
        _exit(92);
    cbm_cov_parent_failure_child_count();
    _exit(0);
}

static int marker_present(const Fixture *f) {
    struct stat info;
    if (lstat(f->marker, &info) == 0)
        return S_ISREG(info.st_mode) ? 1 : -1;
    return errno == ENOENT ? 0 : -1;
}

static bool event(const Fixture *f, const char *stage, int marker, int exit_code) {
    return fprintf(stderr,
                   "COV_PARENT {\"mode\":\"%s\",\"stage\":\"%s\",\"owner_pid\":%ld,"
                   "\"child_pid\":%ld,\"control_errno\":%d,\"marker\":%d,\"exit_code\":%d,"
                   "\"saved_soft\":%ju,\"saved_hard\":%ju,\"parent_soft\":%ju,"
                   "\"parent_hard\":%ju,\"child_seen_soft\":%ju,\"child_seen_hard\":%ju,"
                   "\"child_restored_soft\":%ju,\"child_restored_hard\":%ju}\n",
                   restricted_requested ? "emfile" : "control", stage, (long)getpid(), (long)f->pid,
                   f->child.control_errno, marker, exit_code, (uintmax_t)f->saved.rlim_cur,
                   (uintmax_t)f->saved.rlim_max, (uintmax_t)f->parent_restored.rlim_cur,
                   (uintmax_t)f->parent_restored.rlim_max, (uintmax_t)f->child.observed.rlim_cur,
                   (uintmax_t)f->child.observed.rlim_max, (uintmax_t)f->child.restored.rlim_cur,
                   (uintmax_t)f->child.restored.rlim_max) > 0 &&
           fflush(stderr) == 0;
}

static bool setup(Fixture *f, const char *root) {
    int n = snprintf(f->control, sizeof(f->control), "%s/parent-control.XXXXXX", root);
    if (n <= 0 || (size_t)n >= sizeof(f->control))
        return false;
    int fd = mkstemp(f->control);
    if (fd < 0)
        return false;
    struct stat info;
    bool ok = fstat(fd, &info) == 0 && S_ISREG(info.st_mode);
    if (close(fd) != 0)
        ok = false;
    return ok && pipe(f->ack) == 0 && pipe(f->gate) == 0 &&
           getrlimit(RLIMIT_NOFILE, &f->saved) == 0 && fflush(NULL) == 0;
}

static bool launch(Fixture *f) {
    if (restricted_requested) {
        struct rlimit limited = f->saved, observed;
        limited.rlim_cur = 0;
        f->parent_restore_pending = true;
        if (setrlimit(RLIMIT_NOFILE, &limited) != 0 || getrlimit(RLIMIT_NOFILE, &observed) != 0 ||
            !same_limit(&limited, &observed))
            return false;
    }
    f->pid = fork();
    if (f->pid == 0)
        child_run(f);
    /* Restore before closing handles, formatting paths, or inspecting the child. */
    bool restored = restore_parent(f);
    return restored && f->pid > 0 && close_fd(&f->ack[1]) && close_fd(&f->gate[0]);
}

static bool reap(Fixture *f, bool terminate) {
    if (f->pid < 0 || f->reaped)
        return true;
    bool ok = true;
    if (terminate && kill(f->pid, SIGKILL) != 0 && errno != ESRCH)
        ok = false;
    pid_t result;
    do {
        result = waitpid(f->pid, &f->status, 0);
    } while (result < 0 && errno == EINTR);
    if (result == f->pid || (result < 0 && errno == ECHILD))
        f->reaped = true;
    return result == f->pid && ok;
}

static bool observe_and_release(Fixture *f, const char *root, const char *test) {
    int n = snprintf(f->marker, sizeof(f->marker), "%s/%s/%s.%ld.forked", root, tf_current_suite,
                     test, (long)f->pid);
    if (n <= 0 || (size_t)n >= sizeof(f->marker) ||
        !transfer(f->ack[0], &f->child, sizeof(f->child), false) || !f->child.valid)
        return false;
    int held = marker_present(f);
    if (held != (restricted_requested ? 0 : 1) || !event(f, "held", held, -1))
        return false;
    char release = 'G';
    if (!transfer(f->gate[1], &release, 1, true) || !reap(f, false) || !WIFEXITED(f->status) ||
        WEXITSTATUS(f->status) != 0 || !restore_parent(f))
        return false;
    int final = marker_present(f);
    return final == 0 && event(f, "reaped", final, 0);
}

static int fixture(const char *test) {
    Fixture f = {.pid = -1, .ack = {-1, -1}, .gate = {-1, -1}};
    const char *root = getenv("CBM_TEST_COVERAGE_DIR");
    if (!root || root[0] != '/' || !tf_current_suite)
        return 1;
    struct sigaction ignore = {.sa_handler = SIG_IGN}, previous;
    (void)sigemptyset(&ignore.sa_mask);
    if (sigaction(SIGPIPE, &ignore, &previous) != 0)
        return 1;
    bool ok = setup(&f, root) && launch(&f) && observe_and_release(&f, root, test);
    if (f.parent_restore_pending && !restore_parent(&f))
        ok = false;
    if (!reap(&f, true))
        ok = false;
    if (!close_fd(&f.ack[0]))
        ok = false;
    if (!close_fd(&f.ack[1]))
        ok = false;
    if (!close_fd(&f.gate[0]))
        ok = false;
    if (!close_fd(&f.gate[1]))
        ok = false;
    if (sigaction(SIGPIPE, &previous, NULL) != 0)
        ok = false;
    if (!ok || f.parent_restore_failed)
        fprintf(stderr, "coverage parent fixture failed: native protocol or restoration\n");
    if (f.parent_restore_failed) {
        (void)fflush(stderr);
        _Exit(2); /* Never continue the runner with an unverified parent limit. */
    }
    return ok ? 0 : 1;
}

TEST(coverage_marker_creation) {
    fixture_entered = true;
    return fixture(__func__ + 5);
}
TEST(coverage_marker_sentinel) {
    PASS();
}
SUITE(coverage_parent_failure) {
    RUN_TEST(coverage_marker_creation);
    RUN_TEST(coverage_marker_sentinel);
}
#else
typedef int coverage_parent_failure_disabled_translation_unit;
#endif
