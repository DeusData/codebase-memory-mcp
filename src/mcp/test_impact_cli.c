/*
 * test_impact_cli.c — `codebase-memory-mcp test-impact <publish|select>`.
 *
 * The CI surface of the test-impact engine, in process (no daemon):
 *   publish  builds the team artifact bundle of the checked-out commit;
 *   select   answers which tests to run for the change against a base ref.
 *
 * A bundle takes part in a selection only with --artifact-verified, which the
 * CI job passes after it fetched the bundle from this repository's
 * main-branch producer run whose head is --artifact-commit. Nothing inside a
 * bundle can assert that.
 */
#include "mcp/test_impact_engine.h"

#include "foundation/compat_fs.h"
#include "foundation/platform.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { TIC_DEFAULT_DEADLINE_S = 1800 };

typedef struct {
    const char *name;
    const char **value; /* NULL: a flag */
    bool *flag;
} tic_option_t;

static bool tic_parse(int argc, char **argv, const tic_option_t *options, size_t count) {
    for (int i = 0; i < argc; i++) {
        const tic_option_t *match = NULL;
        for (size_t o = 0; o < count; o++) {
            if (strcmp(argv[i], options[o].name) == 0) {
                match = &options[o];
            }
        }
        if (!match) {
            (void)fprintf(stderr, "test-impact: unknown argument %s\n", argv[i]);
            return false;
        }
        if (match->flag) {
            *match->flag = true;
            continue;
        }
        if (i + 1 >= argc) {
            (void)fprintf(stderr, "test-impact: %s needs a value\n", argv[i]);
            return false;
        }
        *match->value = argv[++i];
    }
    return true;
}

static int64_t tic_number(const char *text, int64_t fallback) {
    if (!text || !*text) {
        return fallback;
    }
    char *end = NULL;
    long long value = strtoll(text, &end, 10);
    return end && !*end && value > 0 ? (int64_t)value : -1;
}

/* <cache>/test-impact unless --work names another private parent. */
static const char *tic_work(const char *requested, char *buf, size_t cap) {
    if (requested) {
        return requested;
    }
    snprintf(buf, cap, "%s/test-impact", cbm_resolve_cache_dir());
    return cbm_mkdir_p(buf, 0700) ? buf : NULL;
}

static int tic_usage(void) {
    (void)fprintf(stderr,
                  "usage: codebase-memory-mcp test-impact publish --repo DIR --out DIR\n"
                  "           [--previous BUNDLE] [--coverage DIR --observed-at EPOCH]\n"
                  "           [--platform LABEL] [--config FILE] [--work DIR] [--deadline-s N]\n"
                  "       codebase-memory-mcp test-impact select --repo DIR --base REF\n"
                  "           [--config FILE] [--artifact BUNDLE --artifact-commit SHA\n"
                  "           --artifact-verified] [--platform LABEL] [--work DIR]\n"
                  "           [--deadline-s N]\n");
    return 2;
}

static int tic_publish(int argc, char **argv) {
    const char *repo = NULL, *out = NULL, *previous = NULL, *coverage = NULL;
    const char *observed = NULL, *platform = NULL, *work = NULL, *deadline = NULL;
    const char *config = NULL;
    const tic_option_t options[] = {{"--config", &config, NULL},
                                    {"--repo", &repo, NULL},
                                    {"--out", &out, NULL},
                                    {"--previous", &previous, NULL},
                                    {"--coverage", &coverage, NULL},
                                    {"--observed-at", &observed, NULL},
                                    {"--platform", &platform, NULL},
                                    {"--work", &work, NULL},
                                    {"--deadline-s", &deadline, NULL}};
    if (!tic_parse(argc, argv, options, sizeof(options) / sizeof(options[0])) || !repo || !out) {
        return tic_usage();
    }
    int64_t observed_at = tic_number(observed, 0);
    int64_t seconds = tic_number(deadline, TIC_DEFAULT_DEADLINE_S);
    char work_buf[4096];
    const char *work_parent = tic_work(work, work_buf, sizeof(work_buf));
    if (observed_at < 0 || seconds < 0 || !work_parent) {
        return tic_usage();
    }
    cbm_test_impact_publish_t request = {.repo_root = repo,
                                         .work_parent = work_parent,
                                         .out_dir = out,
                                         .previous_dir = previous,
                                         .coverage_dir = coverage,
                                         .observed_at = observed_at,
                                         .platform = platform,
                                         .config_path = config,
                                         .deadline_ms = cbm_now_ms() + (uint64_t)seconds * 1000ULL};
    char diagnostic[512] = "";
    cbm_test_impact_status_t status =
        cbm_test_impact_publish(&request, diagnostic, sizeof(diagnostic));
    (void)fprintf(stderr, "test-impact publish: status %d%s%s\n", (int)status,
                  diagnostic[0] ? ": " : "", diagnostic);
    return status == CBM_TEST_IMPACT_OK ? 0 : 1;
}

static int tic_select(int argc, char **argv) {
    const char *repo = NULL, *base = NULL, *config = NULL, *artifact = NULL;
    const char *artifact_commit = NULL, *platform = NULL, *work = NULL, *deadline = NULL;
    bool verified = false;
    const tic_option_t options[] = {{"--repo", &repo, NULL},
                                    {"--base", &base, NULL},
                                    {"--config", &config, NULL},
                                    {"--artifact", &artifact, NULL},
                                    {"--artifact-commit", &artifact_commit, NULL},
                                    {"--artifact-verified", NULL, &verified},
                                    {"--platform", &platform, NULL},
                                    {"--work", &work, NULL},
                                    {"--deadline-s", &deadline, NULL}};
    if (!tic_parse(argc, argv, options, sizeof(options) / sizeof(options[0])) || !repo || !base) {
        return tic_usage();
    }
    int64_t seconds = tic_number(deadline, TIC_DEFAULT_DEADLINE_S);
    char work_buf[4096];
    const char *work_parent = tic_work(work, work_buf, sizeof(work_buf));
    if (seconds < 0 || !work_parent) {
        return tic_usage();
    }
    cbm_test_impact_request_t request = {.repo_root = repo,
                                         .base_ref = base,
                                         .work_parent = work_parent,
                                         .config_path = config,
                                         .deadline_ms = cbm_now_ms() + (uint64_t)seconds * 1000ULL,
                                         .artifact_dir = artifact,
                                         .artifact_commit = artifact_commit,
                                         .artifact_verified = verified,
                                         .platform = platform};
    cbm_test_result_t *result = NULL;
    char diagnostic[512] = "";
    cbm_test_impact_status_t status =
        cbm_test_impact_run(&request, &result, diagnostic, sizeof(diagnostic));
    if (diagnostic[0]) {
        (void)fprintf(stderr, "test-impact select: %s\n", diagnostic);
    }
    if (status != CBM_TEST_IMPACT_OK) {
        (void)fprintf(stderr, "test-impact select: status %d\n", (int)status);
        return 1;
    }
    size_t length = 0;
    const char *json = cbm_test_result_json(result, &length);
    bool ok = json && fwrite(json, 1, length, stdout) == length && fputc('\n', stdout) != EOF;
    cbm_test_result_free(result);
    return ok ? 0 : 1;
}

int cbm_test_impact_cli(int argc, char **argv) {
    if (argc < 1) {
        return tic_usage();
    }
    if (strcmp(argv[0], "publish") == 0) {
        return tic_publish(argc - 1, argv + 1);
    }
    if (strcmp(argv[0], "select") == 0) {
        return tic_select(argc - 1, argv + 1);
    }
    return tic_usage();
}
