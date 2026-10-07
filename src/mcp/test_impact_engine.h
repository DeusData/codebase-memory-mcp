/*
 * test_impact_engine.h — detect_changes scope:"tests", end to end.
 *
 * One request pins the change (merge base .. HEAD of a git worktree), builds
 * the graph and the runnable test registry of exactly the HEAD snapshot,
 * seeds the graph from the change, walks what depends on the seeds, maps
 * reached tests to their suites and lanes, and returns the result codec's
 * answer (cbm.test_impact). The selection rule is the measured rule B:
 * a test runs when the static walk reaches it, when it changed, or when its
 * recorded coverage meets the change.
 *
 * Missing or doubtful evidence never narrows: git, configuration, graph,
 * registry or coverage that cannot be established turns into a run-all
 * answer that names why. Only a malformed request, exhausted memory or the
 * deadline end the request without an answer.
 */
#ifndef CBM_TEST_IMPACT_ENGINE_H
#define CBM_TEST_IMPACT_ENGINE_H

#include "mcp/test_impact_result.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
    const char *repo_root; /* absolute top level of the git worktree */
    const char *base_ref;  /* commit-ish the change merges into */
    /* A private directory (mode 0700) for the pinned snapshot and its graph;
     * the request creates and removes its own entries inside. */
    const char *work_parent;
    /* NULL: the test_impact section of the merge base's .codebase-memory.json
     * (a change cannot loosen its own selection). Otherwise that file. */
    const char *config_path;
    uint64_t deadline_ms; /* absolute cbm_now_ms() deadline */

    /* Optional team artifact bundle (test_impact_artifact.h) of the merge
     * base. It takes part ONLY with artifact_verified: the caller fetched it
     * from this repository's main-branch producer whose head is
     * artifact_commit. Its graph then becomes the build's incremental base
     * and its coverage map may be admitted (receipt policy, age, ancestry,
     * compatibility). Without it, or on any doubt, the graph is built from
     * scratch and no coverage is admitted: every suite runs whole. */
    const char *artifact_dir;
    const char *artifact_commit;
    bool artifact_verified;
    /* Platform/toolchain label of the leg the answer is for; part of the
     * coverage compatibility digest, so a map narrows only its own platform. */
    const char *platform;
} cbm_test_impact_request_t;

typedef enum {
    CBM_TEST_IMPACT_OK = 0, /* an answer: selected, nothing, or run-all with reasons */
    CBM_TEST_IMPACT_INVALID,
    CBM_TEST_IMPACT_OOM,
    CBM_TEST_IMPACT_CANCELLED
} cbm_test_impact_status_t;

/* *out owns the answer (cbm_test_result_json). diagnostic receives a short
 * reason for a non-OK status and for every run-all fallback. */
cbm_test_impact_status_t cbm_test_impact_run(const cbm_test_impact_request_t *request,
                                             cbm_test_result_t **out, char *diagnostic,
                                             size_t diagnostic_len);

/* The absolute path of the `git` the engine runs: the first regular,
 * executable `git` (`git.exe` on Windows) on PATH. false when none. */
bool cbm_test_impact_find_git(char *out, size_t out_len);

/* Builds the team artifact bundle of the checked-out commit of repo_root:
 * the commit's frozen graph (incrementally from previous_dir's graph when that
 * bundle is given and verifies), the coverage map of this commit when given,
 * and receipt.json binding all of it. out_dir must not exist. */
typedef struct {
    const char *repo_root;
    const char *work_parent;
    const char *out_dir;
    const char *previous_dir; /* optional: a bundle of an ancestor */
    const char *coverage_dir; /* optional: functions.tsv, tests.tsv, meta.json */
    /* When the map was recorded (seconds since the epoch). 0 carries the
     * previous bundle's oldest observation forward (an incremental merge never
     * advances it). */
    int64_t observed_at;
    const char *platform;
    /* NULL: the commit's own .codebase-memory.json. Otherwise that file, as
     * for a selection (its coverage.compatibility_paths go into the receipt). */
    const char *config_path;
    uint64_t deadline_ms;
} cbm_test_impact_publish_t;

cbm_test_impact_status_t cbm_test_impact_publish(const cbm_test_impact_publish_t *request,
                                                 char *diagnostic, size_t diagnostic_len);

/* `codebase-memory-mcp test-impact <publish|select> ...` (test_impact_cli.c);
 * argv starts at the subcommand. The process exit code. */
int cbm_test_impact_cli(int argc, char **argv);

#endif /* CBM_TEST_IMPACT_ENGINE_H */
