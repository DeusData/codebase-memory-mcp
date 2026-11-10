/* Execute only existing canonical suites; no shell or private child command. */
#include "test_test_impact_runner_filter.h"
#include "test_helpers.h"

static const char *rf_binary;

void tf_test_impact_runner_filter_set_binary(const char *path) {
    rf_binary = path;
}

typedef struct {
    const char *name;
    char *value;
    bool present;
} rf_environment_t;

static bool rf_environment_save(rf_environment_t *saved, const char *name) {
    saved->name = name;
    const char *value = getenv(name);
    saved->present = value != NULL;
    saved->value = value ? cbm_strdup(value) : NULL;
    return !saved->present || saved->value != NULL;
}

static bool rf_environment_restore(rf_environment_t *saved) {
    if (!saved->name) {
        return true;
    }
    bool ok = false;
    if (saved->present) {
        ok = saved->value && cbm_setenv(saved->name, saved->value, 1) == 0;
    } else {
        ok = cbm_unsetenv(saved->name) == 0;
    }
    free(saved->value);
    memset(saved, 0, sizeof(*saved));
    return ok;
}

static bool rf_write_filter(const rf_fixture_t *fixture) {
    FILE *file = cbm_fopen(fixture->filter_path, "wb");
    if (!file) {
        return false;
    }
    bool written =
        fwrite(fixture->filter, 1, fixture->filter_length, file) == fixture->filter_length;
    bool closed = fclose(file) == 0;
    return written && closed;
}

static bool rf_spawn(const rf_fixture_t *fixture, const char *const *argv, bool filtered,
                     cbm_subprocess_t **process) {
    rf_environment_t saved[3] = {0};
    bool captured = rf_environment_save(&saved[0], "CBM_TEST_ONLY") &&
                    rf_environment_save(&saved[1], "CBM_TEST_ONLY_FILE") &&
                    rf_environment_save(&saved[2], "CBM_TEST_COVERAGE_DIR");
    /* The child keeps its inherited LLVM profile, attributed to this test.
     * It must not start a second per-test collection with that continuous file. */
    bool set = captured && cbm_unsetenv("CBM_TEST_COVERAGE_DIR") == 0 &&
               cbm_setenv("CBM_TEST_ONLY", "", 1) == 0 &&
               cbm_setenv("CBM_TEST_ONLY_FILE", filtered ? fixture->filter_path : "", 1) == 0;
    cbm_proc_opts_t options = {
        .bin = rf_binary,
        .argv = argv,
        .log_file = fixture->log_path,
        .quiet_timeout_ms = 10000,
        .cancel_grace_ms = 1000,
    };
    int spawned = set ? cbm_subprocess_spawn(&options, process) : -1;
    /* Restore before polling or assertions, including failed spawn/set paths. */
    bool first = rf_environment_restore(&saved[0]);
    bool second = rf_environment_restore(&saved[1]);
    bool third = rf_environment_restore(&saved[2]);
    return spawned == 0 && first && second && third;
}

static bool rf_wait(cbm_subprocess_t *process, bool setup_ok, cbm_proc_result_t *result) {
    uint64_t started = cbm_now_ms();
    bool cancelled = !setup_ok;
    if (cancelled) {
        (void)cbm_subprocess_request_cancel(process);
    }
    for (;;) {
        cbm_proc_poll_t state = cbm_subprocess_poll(process, result);
        if (state == CBM_PROC_POLL_TERMINAL) {
            if (!result->tree_quiesced || result->supervision_failed) {
                fprintf(stderr,
                        "runner-filter: critical child containment failure; retaining files\n");
                exit(2);
            }
            cbm_subprocess_destroy(process);
            return !cancelled;
        }
        uint64_t elapsed = cbm_now_ms() - started;
        if (state == CBM_PROC_POLL_ERROR || elapsed >= 30000) {
            cancelled = true;
            (void)cbm_subprocess_request_cancel(process);
        }
        if (elapsed >= 35000) {
            /* Never destroy a running owner or remove files under a live child. */
            fprintf(stderr, "runner-filter: child did not quiesce; retaining owner and files\n");
            exit(2);
        }
        cbm_usleep(1000);
    }
}

static bool rf_read_output(const char *path, char *text, size_t capacity) {
    FILE *file = cbm_fopen(path, "rb");
    if (!file) {
        return false;
    }
    size_t length = fread(text, 1, capacity - 1, file);
    bool complete = !ferror(file) && fgetc(file) == EOF && !ferror(file);
    bool closed = fclose(file) == 0;
    size_t kept = 0;
    for (size_t i = 0; i < length; i++) {
        if (text[i] != '\r') {
            text[kept++] = text[i];
        }
    }
    text[kept] = '\0';
    return complete && closed && memchr(text, '\0', kept) == NULL;
}

static void rf_count_output(rf_run_t *run) {
    run->passed = -1;
    const char *line = run->text;
    while (*line) {
        const char *end = strchr(line, '\n');
        size_t length = end ? (size_t)(end - line) : strlen(line);
        if (length >= 4 && memcmp(line + length - 4, "PASS", 4) == 0) {
            run->pass_lines++;
        }
        int count = -1;
        int used = 0;
        if (sscanf(line, " %d passed%n", &count, &used) == 1 && used > 0) {
            run->passed = count;
        }
        line += length + (end != NULL);
    }
}

bool rf_run_child(rf_fixture_t *fixture, const char *suite, const char *other, bool filtered,
                  rf_run_t *run) {
    memset(run, 0, sizeof(*run));
    run->passed = -1;
    if (!rf_binary || !rf_binary[0] ||
        (filtered && (!fixture->filter || !fixture->filter_length || !rf_write_filter(fixture)))) {
        return false;
    }
    const char *argv[] = {rf_binary, suite, other, NULL};
    cbm_subprocess_t *process = NULL;
    bool spawned = rf_spawn(fixture, argv, filtered, &process);
    if (!process) {
        return false;
    }
    bool ended = rf_wait(process, spawned, &run->process);
    bool read = rf_read_output(fixture->log_path, run->text, sizeof(run->text));
    if (read) {
        rf_count_output(run);
    }
    return spawned && ended && read;
}
