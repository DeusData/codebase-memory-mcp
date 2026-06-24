/*
 * test_log.c — RED phase tests for foundation/log.
 *
 * Testing logging is tricky since output goes to stderr.
 * We redirect stderr to a pipe and read back the output.
 */
#include "test_framework.h"
#include "../src/foundation/log.h"
#include "../src/foundation/compat.h"
#include <stdbool.h>
#ifndef _WIN32
#include <unistd.h>
#include <fcntl.h>
#else
#include <io.h>
#include <fcntl.h>
#endif

/* Simple strstr wrapper used by log tests (avoids circular dep on str_util) */
static inline bool cbm_str_contains_raw(const char *s, const char *sub) {
    return strstr(s, sub) != NULL;
}

static char log_buf[4096];
static int saved_stderr;
static int pipe_fds[2];

static void capture_start(void) {
    fflush(stderr);
    saved_stderr = dup(STDERR_FILENO);
    cbm_pipe(pipe_fds);
#ifndef _WIN32
    /* Set read end to non-blocking */
    fcntl(pipe_fds[0], F_SETFL, O_NONBLOCK);
#endif
    dup2(pipe_fds[1], STDERR_FILENO);
    close(pipe_fds[1]);
}

static const char *capture_end(void) {
    fflush(stderr);
    dup2(saved_stderr, STDERR_FILENO);
    close(saved_stderr);
    ssize_t n = read(pipe_fds[0], log_buf, sizeof(log_buf) - 1);
    close(pipe_fds[0]);
    if (n < 0)
        n = 0;
    log_buf[n] = '\0';
    return log_buf;
}

TEST(log_level_default) {
    /* Default level should be INFO */
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_INFO);
    PASS();
}

TEST(log_level_set) {
    cbm_log_set_level(CBM_LOG_WARN);
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_WARN);
    cbm_log_set_level(CBM_LOG_INFO); /* restore */
    PASS();
}

TEST(log_info_output) {
    cbm_log_set_level(CBM_LOG_DEBUG);
    capture_start();
    cbm_log_info("test.msg", "key1", "val1", "key2", "val2");
    const char *output = capture_end();
    cbm_log_set_level(CBM_LOG_INFO);

    ASSERT(cbm_str_contains_raw(output, "level=info"));
    ASSERT(cbm_str_contains_raw(output, "msg=test.msg"));
    ASSERT(cbm_str_contains_raw(output, "key1=val1"));
    ASSERT(cbm_str_contains_raw(output, "key2=val2"));
    PASS();
}

TEST(log_filtered_by_level) {
    cbm_log_set_level(CBM_LOG_WARN);
    capture_start();
    cbm_log_info("should.not.appear");
    const char *output = capture_end();
    cbm_log_set_level(CBM_LOG_INFO);

    /* Should be empty — info is below warn threshold */
    ASSERT_EQ(strlen(output), 0);
    PASS();
}

TEST(log_error_output) {
    cbm_log_set_level(CBM_LOG_DEBUG);
    capture_start();
    cbm_log_error("critical.fail", "err", "OOM");
    const char *output = capture_end();
    cbm_log_set_level(CBM_LOG_INFO);

    ASSERT(cbm_str_contains_raw(output, "level=error"));
    ASSERT(cbm_str_contains_raw(output, "msg=critical.fail"));
    ASSERT(cbm_str_contains_raw(output, "err=OOM"));
    PASS();
}

TEST(log_int_helper) {
    cbm_log_set_level(CBM_LOG_DEBUG);
    capture_start();
    cbm_log_int(CBM_LOG_INFO, "pass.timing", "elapsed_ms", 42);
    const char *output = capture_end();
    cbm_log_set_level(CBM_LOG_INFO);

    ASSERT(cbm_str_contains_raw(output, "elapsed_ms=42"));
    PASS();
}

/* CBM_LOG_LEVEL parsing — distilled from #414 (closes #413). */
TEST(log_level_from_env_textual) {
    cbm_setenv("CBM_LOG_LEVEL", "error", 1);
    cbm_log_init_from_env();
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_ERROR);

    cbm_setenv("CBM_LOG_LEVEL", "debug", 1);
    cbm_log_init_from_env();
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_DEBUG);

    cbm_setenv("CBM_LOG_LEVEL", "none", 1);
    cbm_log_init_from_env();
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_NONE);

    /* Matching is case-insensitive */
    cbm_setenv("CBM_LOG_LEVEL", "WARN", 1);
    cbm_log_init_from_env();
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_WARN);

    cbm_setenv("CBM_LOG_LEVEL", "Info", 1);
    cbm_log_init_from_env();
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_INFO);

    cbm_unsetenv("CBM_LOG_LEVEL");
    cbm_log_set_level(CBM_LOG_INFO); /* restore */
    PASS();
}

TEST(log_level_from_env_numeric) {
    /* 0=debug 1=info 2=warn 3=error 4=none — mirrors CBMLogLevel */
    cbm_setenv("CBM_LOG_LEVEL", "0", 1);
    cbm_log_init_from_env();
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_DEBUG);

    cbm_setenv("CBM_LOG_LEVEL", "3", 1);
    cbm_log_init_from_env();
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_ERROR);

    cbm_setenv("CBM_LOG_LEVEL", "4", 1);
    cbm_log_init_from_env();
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_NONE);

    /* Out-of-range numeric is ignored — level unchanged */
    cbm_log_set_level(CBM_LOG_INFO);
    cbm_setenv("CBM_LOG_LEVEL", "5", 1);
    cbm_log_init_from_env();
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_INFO);

    cbm_unsetenv("CBM_LOG_LEVEL");
    cbm_log_set_level(CBM_LOG_INFO); /* restore */
    PASS();
}

TEST(log_level_from_env_invalid_ignored) {
    /* Unknown string and empty/unset both leave the level unchanged (fail-open) */
    cbm_log_set_level(CBM_LOG_WARN);
    cbm_setenv("CBM_LOG_LEVEL", "verbose", 1);
    cbm_log_init_from_env();
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_WARN);

    cbm_setenv("CBM_LOG_LEVEL", "", 1);
    cbm_log_init_from_env();
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_WARN);

    cbm_unsetenv("CBM_LOG_LEVEL");
    cbm_log_init_from_env();
    ASSERT_EQ(cbm_log_get_level(), CBM_LOG_WARN);

    cbm_log_set_level(CBM_LOG_INFO); /* restore */
    PASS();
}

SUITE(log) {
    RUN_TEST(log_level_default);
    RUN_TEST(log_level_set);
    RUN_TEST(log_info_output);
    RUN_TEST(log_filtered_by_level);
    RUN_TEST(log_error_output);
    RUN_TEST(log_int_helper);
    RUN_TEST(log_level_from_env_textual);
    RUN_TEST(log_level_from_env_numeric);
    RUN_TEST(log_level_from_env_invalid_ignored);
}
