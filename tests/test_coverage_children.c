/* Genuine fork/exec fixtures. Marker feature verdicts belong to the external driver. */
#if defined(CBM_TEST_COVERAGE) && !defined(_WIN32)
#include "test_framework.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;
static const char *child_runner;
static volatile unsigned int child_counter_sink;

__attribute__((noinline)) void cbm_cov_child_count_lower(void) {
    child_counter_sink++;
}
__attribute__((noinline)) void cbm_cov_child_count_upper(void) {
    child_counter_sink++;
}
__attribute__((noinline)) void cbm_cov_child_count_normal(void) {
    child_counter_sink++;
}
__attribute__((noinline)) void cbm_cov_child_count_failed_exec(void) {
    child_counter_sink++;
}
__attribute__((noinline)) void cbm_cov_child_count_setup(void) {
    child_counter_sink++;
}

enum ChildOperation {
    LOWER,
    UPPER,
    NORMAL,
    EXEC_VE,
    EXEC_V,
    EXEC_VP,
    EXEC_L,
    FAILED_EXEC,
    SETUP_SUCCESS,
    SETUP_KILL
};
typedef struct {
    const char *id;
    enum ChildOperation operation;
    bool obstruct;
    pid_t pid;
    int ack[2];
    int gate[2];
    char profile[4096];
    char marker[4096];
    char receiver_dir[4096];
    char receiver_profile[4096];
    char missing[4096];
    struct stat obstruction;
    bool owns_obstruction;
} Child;

static bool transfer(int fd, char *value, bool writing) {
    struct pollfd pollfd = {.fd = fd, .events = writing ? POLLOUT : POLLIN};
    int ready;
    do {
        ready = poll(&pollfd, 1, 60000);
    } while (ready < 0 && errno == EINTR);
    if (ready <= 0)
        return false;
    ssize_t count;
    do {
        count = writing ? write(fd, value, 1) : read(fd, value, 1);
    } while (count < 0 && errno == EINTR);
    return count == 1;
}

static bool send_byte(int fd, char value) {
    return transfer(fd, &value, true);
}
static bool expect_byte(int fd, char expected) {
    char value = 0;
    return transfer(fd, &value, false) && value == expected;
}

static int marker_present(const Child *child) {
    struct stat info;
    if (lstat(child->marker, &info) == 0)
        return S_ISREG(info.st_mode) ? 1 : -1;
    return errno == ENOENT ? 0 : -1;
}

static bool note(const Child *child, const char *stage, int marker, int code) {
    return fprintf(stderr,
                   "COV_CHILD {\"id\":\"%s\",\"pid\":%ld,\"stage\":\"%s\","
                   "\"marker\":%d,\"code\":%d}\n",
                   child->id, (long)child->pid, stage, marker, code) > 0 &&
           fflush(stderr) == 0;
}

static bool path_fits(int length, size_t capacity) {
    return length > 0 && (size_t)length < capacity;
}

static bool prepare_receiver(Child *child, const char *root) {
    if (!path_fits(
            snprintf(child->receiver_dir, sizeof(child->receiver_dir), "%s/_receiver.XXXXXX", root),
            sizeof(child->receiver_dir)) ||
        !mkdtemp(child->receiver_dir))
        return false;
    return path_fits(snprintf(child->receiver_profile, sizeof(child->receiver_profile),
                              "%s/receiver.profraw", child->receiver_dir),
                     sizeof(child->receiver_profile)) &&
           path_fits(snprintf(child->missing, sizeof(child->missing), "%s/missing-executable",
                              child->receiver_dir),
                     sizeof(child->missing));
}

static int execute(enum ChildOperation operation, const char *path, char **args) {
    switch (operation) {
    case EXEC_VE:
        return execve(path, args, environ);
    case EXEC_V:
        return execv(path, args);
    case EXEC_VP:
        return execvp(path, args);
    case EXEC_L:
        return execl(path, args[0], args[1], args[2], args[3], (char *)NULL);
    default:
        return -2;
    }
}

static void child_body(Child *child) __attribute__((noreturn));
static void child_body(Child *child) {
    if (close(child->ack[0]) != 0 || close(child->gate[1]) != 0 || !send_byte(child->ack[1], 'R') ||
        !expect_byte(child->gate[0], 'G'))
        _exit(90);
    char ack[32], gate[32];
    (void)snprintf(ack, sizeof(ack), "%d", child->ack[1]);
    (void)snprintf(gate, sizeof(gate), "%d", child->gate[0]);
    char *args[] = {(char *)child_runner, "--coverage-child-receiver", ack, gate, NULL};
    if (child->operation >= EXEC_VE && child->operation <= EXEC_L) {
        if (unsetenv("CBM_TEST_COVERAGE_DIR") != 0 ||
            setenv("LLVM_PROFILE_FILE", child->receiver_profile, 1) != 0)
            _exit(91);
        (void)execute(child->operation, child_runner, args);
        _exit(92);
    }
    if (child->operation == FAILED_EXEC) {
        for (int i = 0; i < 4; i++) {
            errno = 0;
            int result = execute((enum ChildOperation)(EXEC_VE + i), child->missing, args);
            if (result != -1 || errno != ENOENT || !send_byte(child->ack[1], (char)('0' + i)) ||
                !expect_byte(child->gate[0], 'G'))
                _exit(93);
        }
        cbm_cov_child_count_failed_exec();
        _exit(0);
    }
    switch (child->operation) {
    case LOWER:
        cbm_cov_child_count_lower();
        _exit(0);
    case UPPER:
        cbm_cov_child_count_upper();
        _Exit(0);
    case NORMAL:
        cbm_cov_child_count_normal();
        exit(0);
    case SETUP_SUCCESS:
        cbm_cov_child_count_setup();
        _exit(0);
    default:
        _exit(94);
    }
}

static bool close_fd(int *fd) {
    if (*fd < 0)
        return true;
    int value = *fd;
    *fd = -1;
    return close(value) == 0;
}

static bool reap(Child *child, bool terminate, int *status, bool *reaped) {
    if (child->pid < 0)
        return true;
    bool ok = true;
    if (terminate && kill(child->pid, SIGKILL) != 0 && errno != ESRCH)
        ok = false;
    pid_t result;
    do {
        result = waitpid(child->pid, status, 0);
    } while (result < 0 && errno == EINTR);
    if (result == child->pid || (result < 0 && errno == ECHILD))
        *reaped = true;
    return result == child->pid && ok;
}

static bool profile_paths(Child *child, const char *root, const char *stem) {
    return path_fits(snprintf(child->profile, sizeof(child->profile), "%s/%s/%s.%ld.profraw", root,
                              tf_current_suite, stem, (long)child->pid),
                     sizeof(child->profile)) &&
           path_fits(snprintf(child->marker, sizeof(child->marker), "%s/%s/%s.%ld.forked", root,
                              tf_current_suite, stem, (long)child->pid),
                     sizeof(child->marker));
}

static bool hold_obstruction(Child *child) {
    if (mkdir(child->profile, 0700) != 0)
        return false;
    child->owns_obstruction = true;
    if (lstat(child->profile, &child->obstruction) != 0 || !S_ISDIR(child->obstruction.st_mode))
        return false;
    return note(child, "obstacle_held", -1, 1);
}

static bool remove_obstruction(Child *child) {
    struct stat now;
    if (!child->owns_obstruction || lstat(child->profile, &now) != 0 || !S_ISDIR(now.st_mode) ||
        now.st_dev != child->obstruction.st_dev || now.st_ino != child->obstruction.st_ino ||
        !note(child, "obstacle_reaped", -1, 1))
        return false;
    /* rmdir removes only our same, still-empty directory, after child reap and receipt. */
    if (rmdir(child->profile) != 0)
        return false;
    child->owns_obstruction = false;
    return note(child, "obstacle_removed", -1, 1);
}

static bool held_protocol(Child *child) {
    if (child->operation >= EXEC_VE && child->operation <= EXEC_L) {
        if (!expect_byte(child->ack[0], 'E'))
            return false;
        int marker = marker_present(child);
        return marker >= 0 && note(child, "exec_held", marker, 0) && send_byte(child->gate[1], 'G');
    }
    if (child->operation == FAILED_EXEC) {
        const char *stages[] = {"execve_failed", "execv_failed", "execvp_failed", "execl_failed"};
        for (int i = 0; i < 4; i++) {
            if (!expect_byte(child->ack[0], (char)('0' + i)))
                return false;
            int marker = marker_present(child);
            if (marker < 0 || !note(child, stages[i], marker, ENOENT) ||
                !send_byte(child->gate[1], 'G'))
                return false;
        }
    }
    return true;
}

static bool start_child(Child *child, const char *root) {
    if (child->operation >= EXEC_VE && child->operation <= FAILED_EXEC &&
        !prepare_receiver(child, root))
        return false;
    if (pipe(child->ack) != 0 || pipe(child->gate) != 0 || fflush(NULL) != 0)
        return false;
    child->pid = fork();
    if (child->pid == 0)
        child_body(child);
    return child->pid > 0 && close_fd(&child->ack[1]) && close_fd(&child->gate[0]);
}

static bool ready_child(Child *child, const char *root, const char *stem) {
    if (!profile_paths(child, root, stem) || !expect_byte(child->ack[0], 'R'))
        return false;
    int initial = marker_present(child);
    if (!note(child, "ready", initial, 0) || initial != 1)
        return false;
    return !child->obstruct || hold_obstruction(child);
}

static bool finish_child(Child *child, int *status, bool *reaped) {
    if (child->operation == SETUP_KILL) {
        if (!reap(child, true, status, reaped))
            return false;
    } else {
        if (!send_byte(child->gate[1], 'G') || !held_protocol(child) ||
            !reap(child, false, status, reaped))
            return false;
    }
    bool expected = child->operation == SETUP_KILL
                        ? WIFSIGNALED(*status) && WTERMSIG(*status) == SIGKILL
                        : WIFEXITED(*status) && WEXITSTATUS(*status) == 0;
    if (!expected || (child->obstruct && !remove_obstruction(child)))
        return false;
    int final = marker_present(child);
    return final >= 0 &&
           note(child, "reaped", final,
                WIFSIGNALED(*status) ? 128 + WTERMSIG(*status) : WEXITSTATUS(*status));
}

static int run_child(const char *stem, const char *id, enum ChildOperation operation,
                     bool obstruct) {
    Child child = {.id = id,
                   .operation = operation,
                   .obstruct = obstruct,
                   .pid = -1,
                   .ack = {-1, -1},
                   .gate = {-1, -1}};
    const char *root = getenv("CBM_TEST_COVERAGE_DIR");
    if (!root || root[0] != '/' || !child_runner || child_runner[0] != '/')
        return 1;
    struct sigaction ignore = {.sa_handler = SIG_IGN}, previous;
    (void)sigemptyset(&ignore.sa_mask);
    if (sigaction(SIGPIPE, &ignore, &previous) != 0)
        return 1;
    bool reaped = false;
    int status = 0;
    bool ok = start_child(&child, root) && ready_child(&child, root, stem) &&
              finish_child(&child, &status, &reaped);
    if (child.pid > 0 && !reaped && !reap(&child, true, &status, &reaped))
        ok = false;
    if (!close_fd(&child.ack[0]))
        ok = false;
    if (!close_fd(&child.ack[1]))
        ok = false;
    if (!close_fd(&child.gate[0]))
        ok = false;
    if (!close_fd(&child.gate[1]))
        ok = false;
    if (sigaction(SIGPIPE, &previous, NULL) != 0)
        ok = false;
    if (!ok)
        fprintf(stderr, "coverage child fixture failed: %s\n", id);
    return ok ? 0 : 1;
}

static int descriptor(const char *text) {
    char *end = NULL;
    errno = 0;
    long value = strtol(text, &end, 10);
    if (errno || !text[0] || *end || value < 3 || value > INT_MAX)
        return -1;
    return fcntl((int)value, F_GETFD) >= 0 ? (int)value : -1;
}

int tf_coverage_children_dispatch(int argc, char **argv) {
    child_runner = argc > 0 && argv ? argv[0] : NULL;
    if (argc != 4 || strcmp(argv[1], "--coverage-child-receiver") != 0)
        return -1;
    int ack = descriptor(argv[2]), gate = descriptor(argv[3]);
    if (ack < 0 || gate < 0 || ack == gate)
        return 95;
    bool ok = send_byte(ack, 'E') && expect_byte(gate, 'G');
    if (!close_fd(&ack))
        ok = false;
    if (!close_fd(&gate))
        ok = false;
    return ok ? 0 : 96;
}

TEST(coverage_explicit_success) {
    if (run_child(__func__ + 5, "lower", LOWER, false) ||
        run_child(__func__ + 5, "upper", UPPER, false))
        return 1;
    PASS();
}
TEST(coverage_explicit_write_failure) {
    if (run_child(__func__ + 5, "lower", LOWER, true) ||
        run_child(__func__ + 5, "upper", UPPER, true))
        return 1;
    PASS();
}
TEST(coverage_normal_exit) {
    if (run_child(__func__ + 5, "normal_ok", NORMAL, false) ||
        run_child(__func__ + 5, "normal_bad", NORMAL, true))
        return 1;
    PASS();
}
TEST(coverage_exec_wrappers) {
    const char *ids[] = {"execve", "execv", "execvp", "execl"};
    for (int i = 0; i < 4; i++) {
        if (run_child(__func__ + 5, ids[i], (enum ChildOperation)(EXEC_VE + i), false))
            return 1;
    }
    if (run_child(__func__ + 5, "failed_exec", FAILED_EXEC, false))
        return 1;
    PASS();
}
TEST(coverage_setup_first) {
    PASS();
}
TEST(coverage_setup_second) {
    PASS();
}

SUITE(coverage_children) {
    RUN_TEST(coverage_explicit_success);
    RUN_TEST(coverage_explicit_write_failure);
    RUN_TEST(coverage_normal_exit);
    RUN_TEST(coverage_exec_wrappers);
}
SUITE(coverage_setup_success) {
    int before = tf_pass_count;
    RUN_TEST(coverage_setup_first);
    if (tf_pass_count > before && run_child("_setup.child", "setup_ok", SETUP_SUCCESS, false))
        tf_fail_count++;
    RUN_TEST(coverage_setup_second);
}
SUITE(coverage_setup_uncertain) {
    int before = tf_pass_count;
    RUN_TEST(coverage_setup_first);
    if (tf_pass_count > before && run_child("_setup.child", "setup_killed", SETUP_KILL, false))
        tf_fail_count++;
    RUN_TEST(coverage_setup_second);
}
#else
typedef int coverage_children_disabled_translation_unit;
#endif
