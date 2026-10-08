/* Independent local-object fixtures for mcp/test_impact_git.h.
 * No implementation helpers or command-count assumptions are used. */
#include "test_framework.h"
#include "test_helpers.h"
#include <cli/cli.h>
#include <foundation/arena.h>
#include <foundation/subprocess.h>
#include <mcp/test_impact_git.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

enum { GF_PATH = 4096, GF_SMALL = 512 };
static const unsigned char gf_binary[] = {'A', 0, '\r', '\n', '\n', 'Z'};
static const unsigned char gf_new_binary[] = {'N', 0, 'B'};
static const char gf_old_text[] = "before first\ncontext\nbefore last\n";
static const char gf_head_text[] = "after first\ncontext\nafter last\n";
static const char gf_base_text[] = "base branch only\n";
static const char *gf_runner_binary;

typedef struct {
    char home[GF_PATH], repo[GF_PATH], git[GF_PATH];
    char capture[GF_PATH], log[GF_PATH], object_file[GF_PATH];
    char a[65], p[65], q[65], head[65], base[65];
    char binary[65], newer[65], old_text[65], head_text[65], base_text[65], attrs[65], link[65];
    unsigned oid_length;
} gf_fixture_t;

static bool gf_path(char *out, const char *root, const char *leaf) {
    int n = snprintf(out, GF_PATH, "%s/%s", root, leaf);
    return n > 0 && n < GF_PATH;
}

static bool gf_absolute(const char *path) {
#ifdef _WIN32
    return path && ((path[0] && path[1] == ':' && (path[2] == '/' || path[2] == '\\')) ||
                    (path[0] == '\\' && path[1] == '\\'));
#else
    return path && path[0] == '/';
#endif
}

static bool gf_write(const char *path, const void *bytes, size_t length) {
    FILE *file = cbm_fopen(path, "wb");
    if (!file)
        return false;
    bool ok = fwrite(bytes, 1, length, file) == length;
    if (fclose(file) != 0)
        ok = false;
    return ok;
}

/* Native executable fixture for Git's optional diff/textconv command. Extra
 * arguments added by Git are deliberately ignored. Normal harness entry keeps
 * argv[0] for a private executable copy; neither mode changes the environment. */
const char *tf_runner_image(int argc, char **argv); /* test_main.c */
int tf_maybe_run_git_facts_diff_probe(int argc, char **argv);
int tf_maybe_run_git_facts_diff_probe(int argc, char **argv) {
    if (argc >= 2 && argv && strcmp(argv[1], "__cbm_git_facts_diff_probe") == 0) {
        return argc >= 3 && gf_write(argv[2], "invoked\n", 8) ? 0 : 96;
    }
    if (argc == 2 && argv && strcmp(argv[1], "__cbm_git_facts_hostile_diff_probe") == 0) {
        /* Only this isolated child mutates its environment, before harness threads start.
         * Re-enter the existing exact-diff test; the parent process remains untouched.
         * An inherited CBM_TEST_ONLY_FILE (a narrowed CI run) is cleared: the runner
         * unions it with CBM_TEST_ONLY, and one naming this suite would re-run the
         * spawning test, which spawns again. */
        if (cbm_setenv("GIT_DIFF_OPTS", "--unified=999", 1) != 0 ||
            cbm_setenv("CBM_TEST_ONLY_FILE", "", 1) != 0 ||
            cbm_setenv(
                "CBM_TEST_ONLY",
                "test_impact_git:test_git_facts_diff_uses_merge_base_zero_context_and_nul_metadata",
                1) != 0) {
            return 96;
        }
        argv[1] = "test_impact_git";
    }
    gf_runner_binary = tf_runner_image(argc, argv);
    return -1;
}

/* Git's configured external-driver values are shell command strings. Quote
 * only fixture-owned paths; Git queries themselves always use native argv. */
static bool gf_driver_command(char *out, size_t capacity, const char *binary, const char *marker) {
    const char *parts[] = {binary, "__cbm_git_facts_diff_probe", marker};
    size_t used = 0;
    for (size_t part = 0; part < 3; part++) {
        if (used + 2 >= capacity)
            return false;
        if (part)
            out[used++] = ' ';
        out[used++] = '\'';
        for (const char *p = parts[part]; *p; p++) {
            const char *escaped = *p == '\'' ? "'\\''" : NULL;
            size_t length = escaped ? 4 : 1;
            if (used + length + 2 >= capacity)
                return false;
            if (escaped)
                memcpy(out + used, escaped, length);
            else
                out[used] = *p;
            used += length;
        }
        out[used++] = '\'';
    }
    if (used >= capacity)
        return false;
    out[used] = '\0';
    return true;
}

static bool gf_git_exit(gf_fixture_t *fx, const char *const *tail, int expected_exit, char *output,
                        size_t capacity) {
    const char *argv[64] = {
        fx->git, "-C", fx->repo, "-c", "commit.gpgSign=false", "-c", "core.autocrlf=false"};
    size_t n = 7;
    for (size_t i = 0; tail[i]; i++) {
        if (n + 1 >= sizeof(argv) / sizeof(argv[0]))
            return false;
        argv[n++] = tail[i];
    }
    argv[n] = NULL;
    /* No quiet timeout: the fixture waits for Git to finish, and the suite's
     * own ceiling is the backstop. A diff-driver control runs a fresh copy of
     * the test runner under `git diff`; on a loaded or Windows host that copy
     * took over 10 s to start and finish, and a 10 s quiet timeout killed Git
     * (outcome HANG) -- the timeout, not the assertion, decided the test. */
    cbm_proc_opts_t opts = {.bin = fx->git,
                            .argv = argv,
                            .stdout_file = fx->capture,
                            .log_file = fx->log,
                            .strip_git_repo_env = true};
    cbm_proc_result_t result = {0};
    cbm_proc_outcome_t expected_outcome =
        expected_exit == 0 ? CBM_PROC_CLEAN : CBM_PROC_EXIT_NONZERO;
    if (cbm_subprocess_run(&opts, &result) != 0 || result.outcome != expected_outcome ||
        result.exit_code != expected_exit || !result.tree_quiesced)
        return false;
    if (!output)
        return true;
    FILE *file = cbm_fopen(fx->capture, "rb");
    if (!file || capacity == 0) {
        if (file)
            (void)fclose(file);
        return false;
    }
    size_t used = fread(output, 1, capacity - 1, file);
    int extra = fgetc(file);
    bool ok = extra == EOF && !ferror(file);
    if (fclose(file) != 0)
        ok = false;
    output[used] = '\0';
    return ok;
}

static bool gf_git(gf_fixture_t *fx, const char *const *tail, char *output, size_t capacity) {
    return gf_git_exit(fx, tail, 0, output, capacity);
}

static bool gf_oid(gf_fixture_t *fx, const char *const *argv, char out[65]) {
    char text[80];
    if (!gf_git(fx, argv, text, sizeof(text)))
        return false;
    size_t n = strlen(text);
    if (n && text[n - 1] == '\n')
        text[--n] = '\0';
    if (n && text[n - 1] == '\r')
        text[--n] = '\0';
    if (n != fx->oid_length)
        return false;
    for (size_t i = 0; i < n; i++) {
        if (!((text[i] >= '0' && text[i] <= '9') || (text[i] >= 'a' && text[i] <= 'f')))
            return false;
    }
    memcpy(out, text, n + 1);
    return true;
}

static bool gf_object(gf_fixture_t *fx, const char *type, const void *bytes, size_t length,
                      char out[65]) {
    const char *argv[] = {"hash-object", "-w", "--no-filters",  "-t",
                          type,          "--", fx->object_file, NULL};
    return gf_write(fx->object_file, bytes, length) && gf_oid(fx, argv, out);
}

static bool gf_index(gf_fixture_t *fx, const char *mode, const char *oid, const char *path) {
    char entry[GF_SMALL];
    int n = snprintf(entry, sizeof(entry), "%s,%s,%s", mode, oid, path);
    if (n <= 0 || (size_t)n >= sizeof(entry))
        return false;
    const char *argv[] = {"update-index", "--add", "--cacheinfo", entry, NULL};
    return gf_git(fx, argv, NULL, 0);
}

static bool gf_tree(gf_fixture_t *fx, int version, char out[65]) {
    const char *empty[] = {"read-tree", "--empty", NULL};
    const char *write[] = {"write-tree", NULL};
    if (!gf_git(fx, empty, NULL, 0))
        return false;
    const char *regular[] = {"binary.bin", ":colon.bin", "-dash.bin", "[glob]*.bin",
                             "dir/file.bin"};
    for (size_t i = 0; i < sizeof(regular) / sizeof(regular[0]); i++) {
        if (!gf_index(fx, "100644", fx->binary, regular[i]))
            return false;
    }
    const char *text = version == 1 ? fx->head_text : version == 2 ? fx->base_text : fx->old_text;
    if (!gf_index(fx, "100644", text, "p.txt") ||
        !gf_index(fx, "100644", fx->attrs, ".gitattributes") ||
        !gf_index(fx, "120000", fx->link, "link"))
        return false;
    if (version == 1) {
        if (!gf_index(fx, "100644", fx->newer, "new.bin") ||
            !gf_index(fx, "160000", fx->a, "submodule"))
            return false;
    } else if (!gf_index(fx, "100644", fx->binary, "deleted.bin"))
        return false;
    return gf_oid(fx, write, out);
}

static bool gf_commit(gf_fixture_t *fx, const char *tree, const char *parent_one,
                      const char *parent_two, const char *label, char out[65]) {
    char commit[1024];
    int n = snprintf(
        commit, sizeof(commit),
        "tree %s\n%s%s%s%s%s%s"
        "author Git Facts Fixture <fixture@example.invalid> 946684800 +0000\n"
        "committer Git Facts Fixture <fixture@example.invalid> 946684800 +0000\n\n%s\n",
        tree, parent_one ? "parent " : "", parent_one ? parent_one : "", parent_one ? "\n" : "",
        parent_two ? "parent " : "", parent_two ? parent_two : "", parent_two ? "\n" : "", label);
    return n > 0 && (size_t)n < sizeof(commit) && gf_object(fx, "commit", commit, (size_t)n, out);
}

static bool gf_ref(gf_fixture_t *fx, const char *name, const char *oid) {
    const char *argv[] = {"update-ref", name, oid, NULL};
    return gf_git(fx, argv, NULL, 0);
}

static bool gf_config(gf_fixture_t *fx, const char *key, const char *value) {
    const char *argv[] = {"config", "--local", key, value, NULL};
    return gf_git(fx, argv, NULL, 0);
}

/* A--P---M(topic/HEAD)
 *  \   /
 *   Q--+--B(base)
 * merge-base(M,B)=Q, which deliberately differs from first-parent(M)=P.
 * Raw commit objects give fixed timestamps/identities without changing env. */
static bool gf_fixture_open(gf_fixture_t *fx, bool sha256) {
    memset(fx, 0, sizeof(*fx));
    const char *resolved = cbm_find_cli("git", cbm_get_home_dir());
    if (!resolved || !gf_absolute(resolved) || strlen(resolved) >= sizeof(fx->git))
        return false;
    memcpy(fx->git, resolved, strlen(resolved) + 1);
    const char *home = th_mktempdir("cbm-git-facts");
    if (!home || strlen(home) >= sizeof(fx->home))
        return false;
    memcpy(fx->home, home, strlen(home) + 1);
    bool ok = gf_path(fx->repo, home, "repo") && gf_path(fx->capture, home, "stdout.bin") &&
              gf_path(fx->log, home, "stderr.log") && gf_path(fx->object_file, home, "object.bin");
    fx->oid_length = sha256 ? 64U : 40U;
    if (ok)
        ok = th_mkdir_p(fx->repo) == 0;
    const char *init[] = {"-c",
                          "init.templateDir=",
                          "init",
                          "--quiet",
                          sha256 ? "--object-format=sha256" : "--object-format=sha1",
                          "--initial-branch=topic",
                          NULL};
    const char attrs[] = "p.txt diff=fixturetrap\n";
    if (ok)
        ok = gf_git(fx, init, NULL, 0) && gf_config(fx, "core.protectNTFS", "false") &&
             gf_config(fx, "core.protectHFS", "false") && gf_config(fx, "core.autocrlf", "false") &&
             gf_config(fx, "core.worktree", fx->repo) &&
             gf_object(fx, "blob", gf_binary, sizeof(gf_binary), fx->binary) &&
             gf_object(fx, "blob", gf_new_binary, sizeof(gf_new_binary), fx->newer) &&
             gf_object(fx, "blob", gf_old_text, sizeof(gf_old_text) - 1, fx->old_text) &&
             gf_object(fx, "blob", gf_head_text, sizeof(gf_head_text) - 1, fx->head_text) &&
             gf_object(fx, "blob", gf_base_text, sizeof(gf_base_text) - 1, fx->base_text) &&
             gf_object(fx, "blob", attrs, sizeof(attrs) - 1, fx->attrs) &&
             gf_object(fx, "blob", "p.txt", 5, fx->link);
    char tree_a[65], tree_h[65], tree_b[65];
    if (ok)
        ok = gf_tree(fx, 0, tree_a) && gf_commit(fx, tree_a, NULL, NULL, "A", fx->a) &&
             gf_commit(fx, tree_a, fx->a, NULL, "P", fx->p) &&
             gf_commit(fx, tree_a, fx->a, NULL, "Q", fx->q) && gf_tree(fx, 1, tree_h) &&
             gf_commit(fx, tree_h, fx->p, fx->q, "M", fx->head) && gf_tree(fx, 2, tree_b) &&
             gf_commit(fx, tree_b, fx->q, NULL, "B", fx->base) &&
             gf_ref(fx, "refs/heads/topic", fx->head) && gf_ref(fx, "refs/heads/base", fx->base);
    if (!ok) {
        fprintf(stderr, "Git facts fixture setup failed (%s, native Git: %s)\n",
                sha256 ? "required SHA-256" : "SHA-1", fx->git);
        (void)th_rmtree(fx->home);
    }
    return ok;
}

static cbm_git_facts_options_t gf_options(gf_fixture_t *fx) {
    return (cbm_git_facts_options_t){.root = fx->repo,
                                     .base_ref = "refs/heads/base",
                                     .git_executable = fx->git,
                                     .expected_head = fx->head,
                                     .deadline_ms = cbm_now_ms() + 60000U,
                                     .command_limit = 256,
                                     .stdout_limit = 1024 * 1024,
                                     .stderr_limit = 65536,
                                     .total_output_limit = 4 * 1024 * 1024};
}

static bool gf_bytes_equal(cbm_git_bytes_t bytes, const void *expected, size_t length) {
    return bytes.length == length &&
           (length == 0 || (bytes.data && memcmp(bytes.data, expected, length) == 0));
}

static bool gf_contains(cbm_git_bytes_t bytes, const char *needle) {
    size_t length = strlen(needle);
    if (!bytes.data || length > bytes.length)
        return false;
    for (size_t i = 0; i <= bytes.length - length; i++) {
        if (memcmp(bytes.data + i, needle, length) == 0)
            return true;
    }
    return false;
}

static bool gf_blob_cleared(const cbm_git_blob_t *blob) {
    return !blob->bytes.data && blob->bytes.length == 0 && blob->oid[0] == '\0' && blob->mode == 0;
}

TEST(test_git_facts_pins_actual_merge_base_and_both_refs) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    const cbm_git_facts_identity_t *id = facts ? cbm_git_facts_identity(facts) : NULL;
    bool matched = id && id->oid_hex_length == 40 && gf_absolute(id->root) && id->git_dir &&
                   id->common_dir && strcmp(id->head, fx.head) == 0 &&
                   strcmp(id->base, fx.base) == 0 && strcmp(id->merge_base, fx.q) == 0 &&
                   strcmp(id->merge_base, fx.p) != 0;
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_EQ(error.status, CBM_GIT_FACTS_OK);
    ASSERT_TRUE(matched);
    PASS();
}

TEST(test_git_facts_ref_index_and_worktree_mutation_cannot_change_snapshot) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    cbm_git_diff_t before = {0}, after = {0};
    bool before_ok = facts && cbm_git_facts_diff(facts, &before, &error);
    unsigned char patch[16384], names[1024];
    bool copied = before_ok && before.patch.data && before.patch.length > 0 &&
                  before.name_status.data && before.name_status.length > 0 &&
                  before.patch.length <= sizeof(patch) &&
                  before.name_status.length <= sizeof(names);
    size_t patch_length = copied ? before.patch.length : 0;
    size_t names_length = copied ? before.name_status.length : 0;
    if (copied) {
        memcpy(patch, before.patch.data, patch_length);
        memcpy(names, before.name_status.data, names_length);
    }
    char path[GF_PATH];
    bool mutated = gf_path(path, fx.repo, "p.txt") && gf_write(path, "dirty\n", 6) &&
                   gf_index(&fx, "100644", fx.base_text, "p.txt") &&
                   gf_ref(&fx, "refs/heads/topic", fx.a) && gf_ref(&fx, "refs/heads/base", fx.p);
    bool after_ok = facts && cbm_git_facts_diff(facts, &after, &error);
    cbm_git_blob_t blob = {0};
    cbm_git_blob_status_t read =
        facts ? cbm_git_facts_read_blob(facts, CBM_GIT_REV_HEAD, "p.txt", 5, &blob, &error)
              : CBM_GIT_BLOB_ERROR;
    bool same = copied && after_ok && gf_bytes_equal(after.patch, patch, patch_length) &&
                gf_bytes_equal(after.name_status, names, names_length) &&
                read == CBM_GIT_BLOB_FOUND &&
                gf_bytes_equal(blob.bytes, gf_head_text, sizeof(gf_head_text) - 1);
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(before_ok);
    ASSERT_TRUE(mutated);
    ASSERT_TRUE(same);
    PASS();
}

TEST(test_git_facts_reads_exact_binary_literal_paths_and_all_revisions) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    const char *paths[] = {"binary.bin", ":colon.bin", "-dash.bin", "[glob]*.bin", "dir/file.bin"};
    bool exact = facts != NULL;
    for (size_t i = 0; facts && i < sizeof(paths) / sizeof(paths[0]); i++) {
        cbm_git_blob_t blob = {0};
        exact = cbm_git_facts_read_blob(facts, CBM_GIT_REV_HEAD, paths[i], strlen(paths[i]), &blob,
                                        &error) == CBM_GIT_BLOB_FOUND &&
                exact;
        exact = gf_bytes_equal(blob.bytes, gf_binary, sizeof(gf_binary)) && blob.mode == 0100644 &&
                strcmp(blob.oid, fx.binary) == 0 && exact;
    }
    const char *texts[] = {gf_head_text, gf_base_text, gf_old_text};
    for (int i = 0; facts && i < 3; i++) {
        const char path[] = {'p', '.', 't', 'x',
                             't', '!', '!'}; /* explicit length, no terminator */
        cbm_git_blob_t blob = {0};
        exact = cbm_git_facts_read_blob(facts, (cbm_git_revision_t)i, path, 5, &blob, &error) ==
                    CBM_GIT_BLOB_FOUND &&
                gf_bytes_equal(blob.bytes, texts[i], strlen(texts[i])) && exact;
    }
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(exact);
    PASS();
}

TEST(test_git_facts_distinguishes_absent_invalid_and_nonregular_paths) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    cbm_git_blob_t blob;
    memset(&blob, 0xa5, sizeof(blob));
    cbm_git_blob_status_t absent =
        facts ? cbm_git_facts_read_blob(facts, CBM_GIT_REV_HEAD, "missing.bin", 11, &blob, &error)
              : CBM_GIT_BLOB_ERROR;
    bool absent_clear = gf_blob_cleared(&blob);
    const char *invalid[] = {"",        "/p.txt", "./p.txt", "dir/../p.txt",
                             "p\0.txt", "link",   "dir",     "submodule"};
    const size_t lengths[] = {0, 6, 7, 12, 6, 4, 3, 9};
    bool rejected = facts != NULL;
    for (size_t i = 0; facts && i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        memset(&blob, 0xa5, sizeof(blob));
        rejected = cbm_git_facts_read_blob(facts, CBM_GIT_REV_HEAD, invalid[i], lengths[i], &blob,
                                           &error) == CBM_GIT_BLOB_ERROR &&
                   gf_blob_cleared(&blob) && rejected;
    }
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_EQ(absent, CBM_GIT_BLOB_ABSENT);
    ASSERT_TRUE(absent_clear);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(test_git_facts_ancestry_distinguishes_no_from_invalid_and_missing_objects) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    char missing[65];
    memset(missing, 'f', fx.oid_length);
    missing[fx.oid_length] = '\0';
    cbm_git_ancestry_t yes = facts ? cbm_git_facts_is_ancestor(facts, fx.q, fx.head, &error) : -1;
    cbm_git_ancestry_t no = facts ? cbm_git_facts_is_ancestor(facts, fx.base, fx.head, &error) : -1;
    cbm_git_ancestry_t invalid =
        facts ? cbm_git_facts_is_ancestor(facts, "HEAD", fx.head, &error) : 0;
    cbm_git_ancestry_t absent =
        facts ? cbm_git_facts_is_ancestor(facts, missing, fx.head, &error) : 0;
    cbm_git_ancestry_t noncommit =
        facts ? cbm_git_facts_is_ancestor(facts, fx.binary, fx.head, &error) : 0;
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_EQ(yes, CBM_GIT_ANCESTRY_YES);
    ASSERT_EQ(no, CBM_GIT_ANCESTRY_NO);
    ASSERT_EQ(invalid, CBM_GIT_ANCESTRY_ERROR);
    ASSERT_EQ(absent, CBM_GIT_ANCESTRY_ERROR);
    ASSERT_EQ(noncommit, CBM_GIT_ANCESTRY_ERROR);
    PASS();
}

TEST(test_git_facts_diff_uses_merge_base_zero_context_and_nul_metadata) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    bool configured =
        gf_config(&fx, "diff.interHunkContext", "100") && gf_config(&fx, "diff.renames", "true");
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    cbm_git_diff_t diff = {0};
    bool diff_ok = facts && cbm_git_facts_diff(facts, &diff, &error);
    const char names[] = "D\0deleted.bin\0A\0new.bin\0M\0p.txt\0A\0submodule\0";
    bool metadata = diff_ok && gf_bytes_equal(diff.name_status, names, sizeof(names) - 1);
    bool patch =
        diff_ok && gf_contains(diff.patch, "@@ -1 +1 @@") &&
        gf_contains(diff.patch, "@@ -3 +3 @@") && gf_contains(diff.patch, "-before first\n") &&
        gf_contains(diff.patch, "+after first\n") && !gf_contains(diff.patch, "base branch only") &&
        !gf_contains(diff.patch, "\n context\n");
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(configured);
    ASSERT_TRUE(diff_ok);
    ASSERT_TRUE(metadata);
    ASSERT_TRUE(patch);
    PASS();
}

TEST(test_git_facts_rejects_malformed_options_and_expected_head_mismatch) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    cbm_git_facts_options_t good = gf_options(&fx);
    bool invalid = true;
    for (int i = 0; i < 9; i++) {
        cbm_git_facts_options_t opts = good;
        if (i == 0)
            opts.root = NULL;
        if (i == 1)
            opts.root = "relative";
        if (i == 2)
            opts.base_ref = NULL;
        if (i == 3)
            opts.git_executable = "git";
        if (i == 4)
            opts.deadline_ms = 0;
        if (i == 5)
            opts.command_limit = 0;
        if (i == 6)
            opts.stdout_limit = 0;
        if (i == 7)
            opts.stderr_limit = 0;
        if (i == 8)
            opts.total_output_limit = 0;
        cbm_git_facts_error_t error;
        memset(&error, 0xa5, sizeof(error));
        cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
        invalid = !facts && error.status == CBM_GIT_FACTS_INVALID &&
                  memchr(error.diagnostic, '\0', sizeof(error.diagnostic)) && invalid;
        cbm_git_facts_free(facts);
    }
    good.expected_head = fx.a;
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&good, &error);
    bool mismatch = !facts && error.status == CBM_GIT_FACTS_IDENTITY_MISMATCH;
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(invalid);
    ASSERT_TRUE(mismatch);
    PASS();
}

static bool gf_cancelled(void *context) {
    return *(const bool *)context;
}

TEST(test_git_facts_precancel_and_expired_deadline_are_deterministic) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    cbm_git_facts_options_t opts = gf_options(&fx);
    bool cancelled = true;
    opts.cancelled = gf_cancelled;
    opts.cancel_context = &cancelled;
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    bool cancel_ok = !facts && error.status == CBM_GIT_FACTS_CANCELLED;
    cbm_git_facts_free(facts);
    opts.cancelled = NULL;
    opts.deadline_ms = 1; /* fixed past absolute deadline, not a measured race */
    facts = cbm_git_facts_open(&opts, &error);
    bool deadline_ok = !facts && error.status == CBM_GIT_FACTS_DEADLINE;
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(cancel_ok);
    ASSERT_TRUE(deadline_ok);
    PASS();
}

TEST(test_git_facts_command_and_output_caps_fail_closed) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    bool limited = true;
    for (int i = 0; i < 3; i++) {
        cbm_git_facts_options_t opts = gf_options(&fx);
        if (i == 0)
            opts.command_limit = 1;
        if (i == 1)
            opts.stdout_limit = 1;
        if (i == 2)
            opts.total_output_limit = 1;
        cbm_git_facts_error_t error;
        cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
        limited = !facts && error.status == CBM_GIT_FACTS_LIMIT && limited;
        cbm_git_facts_free(facts);
    }
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(limited);
    PASS();
}

TEST(test_git_facts_required_sha256_objects_and_identity) {
    gf_fixture_t fx;
    /* Required fixture: unsupported installed Git fails setup explicitly. */
    ASSERT_TRUE(gf_fixture_open(&fx, true));
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    const cbm_git_facts_identity_t *id = facts ? cbm_git_facts_identity(facts) : NULL;
    bool identity = id && id->oid_hex_length == 64 && strlen(id->head) == 64 &&
                    strcmp(id->head, fx.head) == 0 && strcmp(id->base, fx.base) == 0 &&
                    strcmp(id->merge_base, fx.q) == 0;
    cbm_git_blob_t blob = {0};
    bool exact = facts &&
                 cbm_git_facts_read_blob(facts, CBM_GIT_REV_HEAD, ":colon.bin", 10, &blob,
                                         &error) == CBM_GIT_BLOB_FOUND &&
                 strlen(blob.oid) == 64 && gf_bytes_equal(blob.bytes, gf_binary, sizeof(gf_binary));
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(identity);
    ASSERT_TRUE(exact);
    PASS();
}

TEST(test_git_facts_never_runs_external_diff_or_textconv) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    char binary[GF_PATH], marker[GF_PATH], command[GF_PATH * 3];
    bool prepared = gf_path(binary, fx.home,
#ifdef _WIN32
                            "diff-probe.exe"
#else
                            "diff-probe"
#endif
                            ) &&
                    gf_path(marker, fx.home, "driver-invoked") && gf_runner_binary;
    if (prepared) {
        cbm_normalize_path_sep(binary);
        cbm_normalize_path_sep(marker);
        prepared = cbm_copy_file(gf_runner_binary, binary) == 0;
    }
    if (prepared)
        th_make_executable(binary);
    char attributes_path[GF_PATH];
    static const char attributes[] = "p.txt diff=fixturetrap\n";
    prepared = prepared && gf_path(attributes_path, fx.repo, ".gitattributes") &&
               gf_write(attributes_path, attributes, sizeof(attributes) - 1) &&
               gf_driver_command(command, sizeof(command), binary, marker);
    bool control[2] = {false}, isolated[2] = {false};
    for (int i = 0; prepared && i < 2; i++) {
        const char *key = i == 0 ? "diff.external" : "diff.fixturetrap.textconv";
        bool configured = gf_config(&fx, key, command);
        const char *probe[] = {
            "diff", i == 0 ? "--ext-diff" : "--textconv", fx.q, fx.head, "--", "p.txt", NULL};
        bool probe_ok = configured && gf_git(&fx, probe, NULL, 0);
        bool marker_exists = cbm_file_exists(marker);
        control[i] = probe_ok && marker_exists;
        if (!control[i]) {
            fprintf(stderr, "Git driver control %d: configured=%d command=%d marker=%d\n", i,
                    configured, probe_ok, marker_exists);
            FILE *diagnostic = cbm_fopen(fx.log, "rb");
            if (diagnostic) {
                char detail[512];
                size_t length = fread(detail, 1, sizeof(detail) - 1, diagnostic);
                detail[length] = '\0';
                fprintf(stderr, "Git driver fixture stderr: %s\n", detail);
                (void)fclose(diagnostic);
            }
        }
        bool reset = cbm_unlink(marker) == 0;
        cbm_git_facts_options_t opts = gf_options(&fx);
        cbm_git_facts_error_t error;
        cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
        cbm_git_diff_t diff = {0};
        isolated[i] = reset && facts && cbm_git_facts_diff(facts, &diff, &error) &&
                      gf_contains(diff.patch, "+after first\n") && !cbm_file_exists(marker);
        cbm_git_facts_free(facts);
        const char *unset[] = {"config", "--local", "--unset", key, NULL};
        prepared = gf_git(&fx, unset, NULL, 0) && prepared;
    }
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(prepared);
    ASSERT_TRUE(control[0]);
    ASSERT_TRUE(control[1]);
    ASSERT_TRUE(isolated[0]);
    ASSERT_TRUE(isolated[1]);
    PASS();
}

TEST(test_git_facts_rejects_local_grafts_that_change_ancestry) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    char info[GF_PATH], path[GF_PATH], graft[160];
    int length = snprintf(graft, sizeof(graft), "%s %s\n", fx.head, fx.a);
    bool written = length > 0 && (size_t)length < sizeof(graft) &&
                   gf_path(info, fx.repo, ".git/info") && th_mkdir_p(info) == 0 &&
                   gf_path(path, info, "grafts") && gf_write(path, graft, (size_t)length);
    const char *merge[] = {"merge-base", "--all", fx.head, fx.base, NULL};
    char altered[65];
    bool graft_effective = written && gf_oid(&fx, merge, altered) && strcmp(altered, fx.a) == 0 &&
                           strcmp(altered, fx.q) != 0;
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    bool rejected = !facts && error.status == CBM_GIT_FACTS_UNSUPPORTED;
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(graft_effective);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(test_git_facts_rejects_no_common_ancestor) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    char tree_spec[80], tree[65], orphan[65], control_output[8];
    int n = snprintf(tree_spec, sizeof(tree_spec), "%s^{tree}", fx.a);
    const char *resolve_tree[] = {"rev-parse", "--verify", tree_spec, NULL};
    bool created = n > 0 && (size_t)n < sizeof(tree_spec) && gf_oid(&fx, resolve_tree, tree) &&
                   gf_commit(&fx, tree, NULL, NULL, "unrelated-root", orphan) &&
                   gf_ref(&fx, "refs/heads/base", orphan);
    const char *merge[] = {"merge-base", "--all", fx.head, orphan, NULL};
    bool unrelated = created &&
                     gf_git_exit(&fx, merge, 1, control_output, sizeof(control_output)) &&
                     control_output[0] == '\0';
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    bool rejected = !facts && error.status == CBM_GIT_FACTS_NO_MERGE_BASE;
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(unrelated);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(test_git_facts_rejects_multiple_criss_cross_merge_bases) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    char tree_spec[80], tree[65], other_merge[65], control_output[160];
    int n = snprintf(tree_spec, sizeof(tree_spec), "%s^{tree}", fx.head);
    const char *resolve_tree[] = {"rev-parse", "--verify", tree_spec, NULL};
    /* Existing HEAD merges P,Q. A separate Q,P merge makes P and Q exactly
     * the two best common ancestors; neither is an ancestor of the other. */
    bool created = n > 0 && (size_t)n < sizeof(tree_spec) && gf_oid(&fx, resolve_tree, tree) &&
                   gf_commit(&fx, tree, fx.q, fx.p, "independent-reverse-merge", other_merge) &&
                   gf_ref(&fx, "refs/heads/base", other_merge);
    const char *merge[] = {"merge-base", "--all", fx.head, other_merge, NULL};
    char first[65], second[65], extra;
    bool two = created && gf_git(&fx, merge, control_output, sizeof(control_output)) &&
               sscanf(control_output, "%64s %64s %c", first, second, &extra) == 2 &&
               ((strcmp(first, fx.p) == 0 && strcmp(second, fx.q) == 0) ||
                (strcmp(first, fx.q) == 0 && strcmp(second, fx.p) == 0));
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    bool rejected = !facts && error.status == CBM_GIT_FACTS_AMBIGUOUS_MERGE_BASE;
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(two);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(test_git_facts_rejects_local_shallow_state) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    char path[GF_PATH], contents[80], control_output[16];
    int n = snprintf(contents, sizeof(contents), "%s\n", fx.q);
    bool written = n > 0 && (size_t)n < sizeof(contents) &&
                   gf_path(path, fx.repo, ".git/shallow") && gf_write(path, contents, (size_t)n);
    const char *probe[] = {"rev-parse", "--is-shallow-repository", NULL};
    bool shallow =
        written && gf_git(&fx, probe, control_output, sizeof(control_output)) &&
        (strcmp(control_output, "true\n") == 0 || strcmp(control_output, "true\r\n") == 0);
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    bool rejected = !facts && error.status == CBM_GIT_FACTS_UNSUPPORTED;
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(shallow);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(test_git_facts_rejects_history_override_added_after_open) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    bool opened = facts && error.status == CBM_GIT_FACTS_OK;
    bool before =
        facts && cbm_git_facts_is_ancestor(facts, fx.q, fx.head, &error) == CBM_GIT_ANCESTRY_YES;
    char info[GF_PATH], path[GF_PATH], graft[160], altered[65];
    int n = snprintf(graft, sizeof(graft), "%s %s\n", fx.head, fx.a);
    bool written = opened && n > 0 && (size_t)n < sizeof(graft) &&
                   gf_path(info, fx.repo, ".git/info") && th_mkdir_p(info) == 0 &&
                   gf_path(path, info, "grafts") && gf_write(path, graft, (size_t)n);
    const char *merge[] = {"merge-base", "--all", fx.head, fx.base, NULL};
    bool override_active = written && gf_oid(&fx, merge, altered) && strcmp(altered, fx.a) == 0;
    cbm_git_ancestry_t after =
        facts ? cbm_git_facts_is_ancestor(facts, fx.q, fx.head, &error) : CBM_GIT_ANCESTRY_NO;
    bool rejected = after == CBM_GIT_ANCESTRY_ERROR && error.status == CBM_GIT_FACTS_UNSUPPORTED;
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(opened);
    ASSERT_TRUE(before);
    ASSERT_TRUE(override_active);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(test_git_facts_cancellation_after_open_is_latched) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    bool cancelled = false;
    cbm_git_facts_options_t opts = gf_options(&fx);
    opts.cancelled = gf_cancelled;
    opts.cancel_context = &cancelled;
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    bool opened = facts && error.status == CBM_GIT_FACTS_OK;
    bool before =
        facts && cbm_git_facts_is_ancestor(facts, fx.q, fx.head, &error) == CBM_GIT_ANCESTRY_YES;
    cancelled = true;
    cbm_git_diff_t diff;
    memset(&diff, 0xa5, sizeof(diff));
    bool diff_ok = facts && cbm_git_facts_diff(facts, &diff, &error);
    bool first = facts && !diff_ok && error.status == CBM_GIT_FACTS_CANCELLED && !diff.patch.data &&
                 diff.patch.length == 0 && !diff.name_status.data && diff.name_status.length == 0;
    cancelled = false; /* the same cancelled handle must not resume */
    cbm_git_blob_t blob;
    memset(&blob, 0xa5, sizeof(blob));
    cbm_git_blob_status_t read =
        facts ? cbm_git_facts_read_blob(facts, CBM_GIT_REV_HEAD, "p.txt", 5, &blob, &error)
              : CBM_GIT_BLOB_FOUND;
    bool latched = read == CBM_GIT_BLOB_ERROR && error.status == CBM_GIT_FACTS_CANCELLED &&
                   gf_blob_cleared(&blob);
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(opened);
    ASSERT_TRUE(before);
    ASSERT_TRUE(first);
    ASSERT_TRUE(latched);
    PASS();
}

/* Insert before SUITE(test_impact_git), then register this test in that suite.
 * Reuses the existing independent fixture helpers; no new implementation seam. */
TEST(test_git_facts_gitfile_redirection_cannot_certify_false_ancestry) {
    gf_fixture_t original, redirected;
    ASSERT_TRUE(gf_fixture_open(&original, false));
    bool second_ready = gf_fixture_open(&redirected, false);
    if (!second_ready) {
        int cleanup = th_rmtree(original.home);
        ASSERT_EQ(cleanup, 0);
        ASSERT_TRUE(second_ready);
    }
    /* Objects are generated from identical fixed bytes in both native repos.
     * Their identities therefore stay the same while their topology differs. */
    bool same_objects = strcmp(original.head, redirected.head) == 0 &&
                        strcmp(original.base, redirected.base) == 0 &&
                        strcmp(original.q, redirected.q) == 0;
    char original_git[GF_PATH];
    bool path_ready = gf_path(original_git, original.home, "original-git");
    const char *relocate[] = {
        "-c", "init.templateDir=", "init", "--quiet", "--separate-git-dir", original_git, NULL};
    bool relocated = same_objects && path_ready && gf_git(&original, relocate, NULL, 0);
    cbm_git_facts_options_t opts = gf_options(&original);
    cbm_git_facts_error_t error = {0};
    cbm_git_facts_t *facts = relocated ? cbm_git_facts_open(&opts, &error) : NULL;
    bool opened = facts && error.status == CBM_GIT_FACTS_OK;
    bool originally_no = facts &&
                         cbm_git_facts_is_ancestor(facts, original.base, original.head, &error) ==
                             CBM_GIT_ANCESTRY_NO &&
                         error.status == CBM_GIT_FACTS_OK;

    char target_git[GF_PATH], info[GF_PATH], graft_path[GF_PATH], gitfile_path[GF_PATH];
    char graft[160], gitfile[GF_PATH + 32];
    int graft_length = snprintf(graft, sizeof(graft), "%s %s\n", original.head, original.base);
    bool target_ready = opened && gf_path(target_git, redirected.repo, ".git") &&
                        gf_path(info, target_git, "info") && th_mkdir_p(info) == 0 &&
                        gf_path(graft_path, info, "grafts") && graft_length > 0 &&
                        (size_t)graft_length < sizeof(graft) &&
                        gf_write(graft_path, graft, (size_t)graft_length);
    int gitfile_length =
        target_ready ? snprintf(gitfile, sizeof(gitfile), "gitdir: %s\n", target_git) : -1;
    bool repointed = target_ready && gitfile_length > 0 &&
                     (size_t)gitfile_length < sizeof(gitfile) &&
                     gf_path(gitfile_path, original.repo, ".git") &&
                     gf_write(gitfile_path, gitfile, (size_t)gitfile_length);
    const char *control[] = {"--no-replace-objects", "merge-base",  "--is-ancestor",
                             original.base,          original.head, NULL};
    /* -C discovery now returns YES through B's graft, even with the adapter's
     * no-replace flag. The original separate Git directory is left intact. */
    bool redirected_yes = repointed && gf_git(&original, control, NULL, 0);
    cbm_git_ancestry_t after =
        facts ? cbm_git_facts_is_ancestor(facts, original.base, original.head, &error)
              : CBM_GIT_ANCESTRY_YES;
    bool retained_or_rejected =
        (after == CBM_GIT_ANCESTRY_NO && error.status == CBM_GIT_FACTS_OK) ||
        (after == CBM_GIT_ANCESTRY_ERROR && error.status != CBM_GIT_FACTS_OK);
    cbm_git_facts_free(facts);
    int cleanup_original = th_rmtree(original.home);
    int cleanup_redirected = th_rmtree(redirected.home);
    ASSERT_EQ(cleanup_original, 0);
    ASSERT_EQ(cleanup_redirected, 0);
    ASSERT_TRUE(same_objects);
    ASSERT_TRUE(relocated);
    ASSERT_TRUE(opened);
    ASSERT_TRUE(originally_no);
    ASSERT_TRUE(redirected_yes);
    ASSERT_TRUE(retained_or_rejected);
    PASS();
}

TEST(test_git_facts_inherited_diff_options_cannot_override_zero_context) {
    const char *home = th_mktempdir("cbm-git-diff-env");
    ASSERT_NOT_NULL(home);
    char log[GF_PATH];
    bool prepared = gf_runner_binary && gf_path(log, home, "child.log");
    const char *argv[] = {gf_runner_binary, "__cbm_git_facts_hostile_diff_probe", NULL};
    cbm_proc_opts_t options = {
        .bin = gf_runner_binary, .argv = argv, .log_file = log, .quiet_timeout_ms = 60000};
    cbm_proc_result_t result = {0};
    int launched = prepared ? cbm_subprocess_run(&options, &result) : -1;
    bool passed = launched == 0 && result.outcome == CBM_PROC_CLEAN && result.exit_code == 0 &&
                  result.tree_quiesced;
    if (!passed && prepared) {
        FILE *file = cbm_fopen(log, "rb");
        if (file) {
            char detail[4096];
            size_t n = fread(detail, 1, sizeof(detail) - 1, file);
            detail[n] = '\0';
            fprintf(stderr, "Isolated hostile-diff child: %s\n", detail);
            (void)fclose(file);
        }
    }
    int cleanup = th_rmtree(home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(prepared);
    ASSERT_EQ(launched, 0);
    ASSERT_TRUE(passed);
    PASS();
}

/* A narrowed CI run hands every runner process a CBM_TEST_ONLY_FILE. The
 * hostile-diff child must still run exactly its own test: the runner unions
 * the file with CBM_TEST_ONLY, so an inherited file would add its tests, and
 * one naming this suite would make the child re-run the spawning test. */
#define GF_OTHER_TEST "test_git_facts_rejects_malformed_options_and_expected_head_mismatch"
TEST(test_git_facts_hostile_probe_ignores_an_inherited_selection_file) {
    const char *home = th_mktempdir("cbm-git-diff-only");
    ASSERT_NOT_NULL(home);
    char log[GF_PATH];
    char only[GF_PATH];
    static const char token[] = "test_impact_git:" GF_OTHER_TEST "\n";
    bool prepared = gf_runner_binary && gf_path(log, home, "child.log") &&
                    gf_path(only, home, "only.txt") && gf_write(only, token, sizeof(token) - 1);
    const char *inherited = getenv("CBM_TEST_ONLY_FILE");
    char *saved = inherited ? cbm_strdup(inherited) : NULL;
    bool set = prepared && (!inherited || saved) && cbm_setenv("CBM_TEST_ONLY_FILE", only, 1) == 0;
    const char *argv[] = {gf_runner_binary, "__cbm_git_facts_hostile_diff_probe", NULL};
    cbm_proc_opts_t options = {
        .bin = gf_runner_binary, .argv = argv, .log_file = log, .quiet_timeout_ms = 60000};
    cbm_proc_result_t result = {0};
    int launched = set ? cbm_subprocess_run(&options, &result) : -1;
    bool restored = saved ? cbm_setenv("CBM_TEST_ONLY_FILE", saved, 1) == 0
                          : cbm_unsetenv("CBM_TEST_ONLY_FILE") == 0;
    free(saved);
    char detail[16384] = "";
    FILE *file = launched == 0 ? cbm_fopen(log, "rb") : NULL;
    if (file) {
        size_t n = fread(detail, 1, sizeof(detail) - 1, file);
        detail[n] = '\0';
        (void)fclose(file);
    }
    bool own_test_only = launched == 0 && result.outcome == CBM_PROC_CLEAN &&
                         result.exit_code == 0 &&
                         strstr(detail, "test_git_facts_diff_uses_merge_base_zero_context") &&
                         !strstr(detail, GF_OTHER_TEST);
    if (!own_test_only && launched == 0) {
        fprintf(stderr, "Isolated hostile-diff child: %s\n", detail);
    }
    int cleanup = th_rmtree(home);
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(set);
    ASSERT_TRUE(restored);
    ASSERT_EQ(launched, 0);
    ASSERT_TRUE(own_test_only);
    PASS();
}
#undef GF_OTHER_TEST

/* Independent D1 inventory tests. Reuse the native local-object gf_* fixture
 * helpers above; construct byte-only names in Git objects, never on disk. */
typedef struct {
    const char *path;
    size_t length;
    uint32_t mode;
    cbm_git_tree_object_type_t type;
    const char *oid;
} gfi_expected_t;

static bool gfi_matches(const cbm_git_tree_inventory_t *inventory,
                         const gfi_expected_t *expected, size_t count) {
    if (inventory->count != count || (count && !inventory->entries))
        return false;
    if (!count)
        return inventory->entries == NULL;
    for (size_t i = 0; i < count; i++) {
        const cbm_git_tree_entry_t *entry = &inventory->entries[i];
        size_t oid_length = strlen(expected[i].oid);
        if (!entry->path || entry->path_length != expected[i].length ||
            memcmp(entry->path, expected[i].path, expected[i].length) != 0 ||
            entry->path[entry->path_length] != '\0' || entry->mode != expected[i].mode ||
            entry->object_type != expected[i].type || oid_length >= sizeof(entry->oid) ||
            entry->oid[oid_length] != '\0' || memcmp(entry->oid, expected[i].oid, oid_length) != 0)
            return false;
    }
    return true;
}

static bool gfi_cleared(const cbm_git_tree_inventory_t *inventory) {
    return inventory->entries == NULL && inventory->count == 0;
}

static cbm_git_tree_inventory_t gfi_poisoned(void) {
    static const cbm_git_tree_entry_t sentinel = {.path = "old-prefix", .path_length = 10};
    return (cbm_git_tree_inventory_t){.entries = &sentinel, .count = 1};
}

/* Tree object encoding is Git's fixture format: ASCII octal mode, space,
 * basename bytes, NUL, raw object ID. Records supplied here are preordered in
 * native Git tree order; the inventory oracle below is independently literal. */
typedef struct {
    const char *mode;
    const char *name;
    size_t length;
    const char *oid;
} gfi_raw_entry_t;

static int gfi_hex(unsigned char ch) {
    if (ch >= '0' && ch <= '9')
        return ch - '0';
    if (ch >= 'a' && ch <= 'f')
        return ch - 'a' + 10;
    return -1;
}

static bool gfi_raw_tree_format(gf_fixture_t *fx, const gfi_raw_entry_t *rows, size_t count,
                                 bool validate_format, char out[65]) {
    size_t bytes = 0, oid_bytes = fx->oid_length / 2;
    for (size_t i = 0; i < count; i++) {
        size_t mode = strlen(rows[i].mode);
        if (!rows[i].length || rows[i].length > SIZE_MAX - mode - oid_bytes - 2 ||
            bytes > SIZE_MAX - (mode + rows[i].length + oid_bytes + 2) ||
            strlen(rows[i].oid) != fx->oid_length)
            return false;
        bytes += mode + rows[i].length + oid_bytes + 2;
    }
    CBMArena arena;
    cbm_arena_init_lazy(&arena, 4096);
    unsigned char *buffer = cbm_arena_alloc(&arena, bytes ? bytes : 1);
    bool ok = buffer != NULL;
    size_t used = 0;
    for (size_t i = 0; i < count && ok; i++) {
        size_t mode = strlen(rows[i].mode);
        memcpy(buffer + used, rows[i].mode, mode); used += mode;
        buffer[used++] = ' ';
        memcpy(buffer + used, rows[i].name, rows[i].length); used += rows[i].length;
        buffer[used++] = 0;
        for (size_t j = 0; j < oid_bytes; j++) {
            int high = gfi_hex((unsigned char)rows[i].oid[2 * j]);
            int low = gfi_hex((unsigned char)rows[i].oid[2 * j + 1]);
            if (high < 0 || low < 0) { ok = false; break; }
            buffer[used++] = (unsigned char)((high << 4) | low);
        }
    }
    ok = ok && used == bytes;
    if (ok && validate_format) {
        ok = gf_object(fx, "tree", buffer, bytes, out);
    } else if (ok) {
        /* One synthetic output-parser fixture intentionally exceeds Git fsck's
         * pathname recommendation. Keep its raw tree bytes; never materialize
         * that name in the filesystem or relax normal fixture validation. */
        const char *argv[] = {"hash-object", "-w", "--no-filters", "--literally",
                              "-t", "tree", "--", fx->object_file, NULL};
        ok = gf_write(fx->object_file, buffer, bytes) && gf_oid(fx, argv, out);
    }
    cbm_arena_destroy(&arena);
    return ok;
}

static bool gfi_raw_tree(gf_fixture_t *fx, const gfi_raw_entry_t *rows, size_t count,
                          char out[65]) {
    return gfi_raw_tree_format(fx, rows, count, true, out);
}

static bool gfi_head_tree(gf_fixture_t *fx, const char *tree) {
    char head[65];
    if (!gf_commit(fx, tree, fx->head, NULL, "inventory head", head) ||
        !gf_ref(fx, "refs/heads/topic", head))
        return false;
    memcpy(fx->head, head, fx->oid_length + 1);
    return true;
}

#define GFI_BLOB(path_, oid_) {path_, sizeof(path_) - 1, 0100644, CBM_GIT_TREE_BLOB, oid_}

TEST(test_git_facts_inventory_pins_complete_head_and_actual_merge_base) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    const char *read_head[] = {"read-tree", fx.head, NULL};
    const char *write_tree[] = {"write-tree", NULL};
    char tree[65];
    bool configured = gf_git(&fx, read_head, NULL, 0) &&
        gf_index(&fx, "100755", fx.binary, "run.sh") &&
        gf_oid(&fx, write_tree, tree) && gfi_head_tree(&fx, tree);
    const gfi_expected_t head_rows[] = {
        GFI_BLOB("-dash.bin", fx.binary), GFI_BLOB(".gitattributes", fx.attrs),
        GFI_BLOB(":colon.bin", fx.binary), GFI_BLOB("[glob]*.bin", fx.binary),
        GFI_BLOB("binary.bin", fx.binary), GFI_BLOB("dir/file.bin", fx.binary),
        {"link", 4, 0120000, CBM_GIT_TREE_BLOB, fx.link},
        GFI_BLOB("new.bin", fx.newer), GFI_BLOB("p.txt", fx.head_text),
        {"run.sh", 6, 0100755, CBM_GIT_TREE_BLOB, fx.binary},
        {"submodule", 9, 0160000, CBM_GIT_TREE_COMMIT, fx.a}
    };
    const gfi_expected_t base_rows[] = {
        GFI_BLOB("-dash.bin", fx.binary), GFI_BLOB(".gitattributes", fx.attrs),
        GFI_BLOB(":colon.bin", fx.binary), GFI_BLOB("[glob]*.bin", fx.binary),
        GFI_BLOB("binary.bin", fx.binary), GFI_BLOB("deleted.bin", fx.binary),
        GFI_BLOB("dir/file.bin", fx.binary),
        {"link", 4, 0120000, CBM_GIT_TREE_BLOB, fx.link}, GFI_BLOB("p.txt", fx.old_text)
    };
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = configured ? cbm_git_facts_open(&opts, &error) : NULL;
    const cbm_git_facts_identity_t *id = facts ? cbm_git_facts_identity(facts) : NULL;
    bool opened = id && strcmp(id->head, fx.head) == 0 && strcmp(id->merge_base, fx.q) == 0 &&
        strcmp(id->merge_base, fx.p) != 0;
    char product[GF_PATH], untracked[GF_PATH];
    bool missing = gf_path(product, fx.repo, "p.txt") && !cbm_file_exists(product);
    cbm_git_tree_inventory_t head = gfi_poisoned(), base = gfi_poisoned();
    bool first = facts && cbm_git_facts_inventory(facts, CBM_GIT_REV_HEAD, &head, &error) &&
        error.status == CBM_GIT_FACTS_OK && gfi_matches(&head, head_rows, 11);
    const char *empty_index[] = {"read-tree", "--empty", NULL};
    bool mutated = missing && gf_git(&fx, empty_index, NULL, 0) &&
        gf_write(product, "ambient edit\n", 13) && gf_path(untracked, fx.repo, "untracked.c") &&
        gf_write(untracked, "not in commit\n", 14) && gf_ref(&fx, "refs/heads/topic", fx.a) &&
        gf_ref(&fx, "refs/heads/base", fx.a);
    bool merge_base = facts && cbm_git_facts_inventory(facts, CBM_GIT_REV_MERGE_BASE, &base, &error) &&
        error.status == CBM_GIT_FACTS_OK && gfi_matches(&base, base_rows, 9);
    cbm_git_tree_inventory_t again = gfi_poisoned();
    bool pinned = facts && cbm_git_facts_inventory(facts, CBM_GIT_REV_HEAD, &again, &error) &&
        error.status == CBM_GIT_FACTS_OK && gfi_matches(&again, head_rows, 11);
    /* Earlier views remain owned by the same handle after later operations. */
    bool retained = first && merge_base && gfi_matches(&head, head_rows, 11) &&
        gfi_matches(&base, base_rows, 9);
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0); ASSERT_TRUE(configured); ASSERT_TRUE(opened); ASSERT_TRUE(missing);
    ASSERT_TRUE(mutated); ASSERT_TRUE(first); ASSERT_TRUE(merge_base);
    ASSERT_TRUE(pinned); ASSERT_TRUE(retained);
    PASS();
}

TEST(test_git_facts_inventory_preserves_literal_bytes_and_sha256) {
    for (int algorithm = 0; algorithm < 2; algorithm++) {
        gf_fixture_t fx;
        ASSERT_TRUE(gf_fixture_open(&fx, algorithm != 0)); /* SHA-256 is required, never skipped. */
        char nested[65], tree[65];
        const gfi_raw_entry_t child[] = {{"100644", "leaf\nname", 9, fx.binary}};
        bool configured = gfi_raw_tree(&fx, child, 1, nested);
        const gfi_raw_entry_t raw[] = {
            {"100644", "a", 1, fx.binary}, {"100644", "aa", 2, fx.newer},
            {"40000", "dir", 3, nested}, {"100644", "line\nname", 9, fx.binary},
            {"100644", "tab\tname", 8, fx.binary}, {"100644", "z", 1, fx.binary},
            {"100644", "\x80" "high", 5, fx.binary}, {"100644", "\xff" "tail", 5, fx.newer}
        };
        configured = configured && gfi_raw_tree(&fx, raw, 8, tree) && gfi_head_tree(&fx, tree);
        const gfi_expected_t expected[] = {
            GFI_BLOB("a", fx.binary), GFI_BLOB("aa", fx.newer),
            GFI_BLOB("dir/leaf\nname", fx.binary), GFI_BLOB("line\nname", fx.binary),
            GFI_BLOB("tab\tname", fx.binary), GFI_BLOB("z", fx.binary),
            GFI_BLOB("\x80" "high", fx.binary), GFI_BLOB("\xff" "tail", fx.newer)
        };
        cbm_git_facts_options_t opts = gf_options(&fx);
        cbm_git_facts_error_t error;
        cbm_git_facts_t *facts = configured ? cbm_git_facts_open(&opts, &error) : NULL;
        const cbm_git_facts_identity_t *id = facts ? cbm_git_facts_identity(facts) : NULL;
        bool opened = id && id->oid_hex_length == (algorithm ? 64U : 40U);
        cbm_git_tree_inventory_t inventory = gfi_poisoned();
        bool exact = facts && cbm_git_facts_inventory(facts, CBM_GIT_REV_HEAD, &inventory, &error) &&
            error.status == CBM_GIT_FACTS_OK && gfi_matches(&inventory, expected, 8);
        cbm_git_facts_free(facts);
        int cleanup = th_rmtree(fx.home);
        if (!configured || !opened || !exact)
            fprintf(stderr, "Git inventory byte-path fixture failed (%s)\n", algorithm ? "SHA-256" : "SHA-1");
        ASSERT_EQ(cleanup, 0); ASSERT_TRUE(configured); ASSERT_TRUE(opened); ASSERT_TRUE(exact);
    }
    PASS();
}

TEST(test_git_facts_inventory_empty_and_invalid_revision_clear_output) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    const char *empty_index[] = {"read-tree", "--empty", NULL};
    const char *write_tree[] = {"write-tree", NULL};
    char tree[65];
    bool configured = gf_git(&fx, empty_index, NULL, 0) && gf_oid(&fx, write_tree, tree) &&
        gfi_head_tree(&fx, tree) && gf_ref(&fx, "refs/heads/base", fx.head);
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = configured ? cbm_git_facts_open(&opts, &error) : NULL;
    const cbm_git_facts_identity_t *id = facts ? cbm_git_facts_identity(facts) : NULL;
    bool opened = id && strcmp(id->merge_base, fx.head) == 0;
    cbm_git_tree_inventory_t inventory = gfi_poisoned();
    bool head_empty = facts && cbm_git_facts_inventory(facts, CBM_GIT_REV_HEAD, &inventory, &error) &&
        error.status == CBM_GIT_FACTS_OK && gfi_cleared(&inventory);
    inventory = gfi_poisoned();
    bool invalid = facts && !cbm_git_facts_inventory(facts, CBM_GIT_REV_BASE, &inventory, &error) &&
        error.status == CBM_GIT_FACTS_INVALID && gfi_cleared(&inventory);
    inventory = gfi_poisoned();
    bool base_empty = facts && cbm_git_facts_inventory(facts, CBM_GIT_REV_MERGE_BASE, &inventory, &error) &&
        error.status == CBM_GIT_FACTS_OK && gfi_cleared(&inventory);
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0); ASSERT_TRUE(configured); ASSERT_TRUE(opened);
    ASSERT_TRUE(head_empty); ASSERT_TRUE(invalid); ASSERT_TRUE(base_empty);
    PASS();
}

TEST(test_git_facts_inventory_cancellation_clears_prefix_and_latches) {
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    bool cancelled = false;
    cbm_git_facts_options_t opts = gf_options(&fx);
    opts.cancelled = gf_cancelled;
    opts.cancel_context = &cancelled;
    cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    bool opened = facts && error.status == CBM_GIT_FACTS_OK;
    cbm_git_blob_t blob;
    bool control = facts && cbm_git_facts_read_blob(facts, CBM_GIT_REV_HEAD, "binary.bin", 10,
        &blob, &error) == CBM_GIT_BLOB_FOUND && gf_bytes_equal(blob.bytes, gf_binary, sizeof(gf_binary));
    cbm_git_tree_inventory_t before = gfi_poisoned();
    bool inventoried = facts && cbm_git_facts_inventory(facts, CBM_GIT_REV_HEAD, &before, &error) &&
        error.status == CBM_GIT_FACTS_OK && before.count == 10 && before.entries;
    cancelled = true;
    cbm_git_tree_inventory_t rejected = gfi_poisoned();
    bool first = facts && !cbm_git_facts_inventory(facts, CBM_GIT_REV_HEAD, &rejected, &error) &&
        error.status == CBM_GIT_FACTS_CANCELLED && gfi_cleared(&rejected);
    cancelled = false; /* Explicit state transition; no timing window or racing thread. */
    rejected = gfi_poisoned();
    bool latched = facts && !cbm_git_facts_inventory(facts, CBM_GIT_REV_MERGE_BASE, &rejected, &error) &&
        error.status == CBM_GIT_FACTS_CANCELLED && gfi_cleared(&rejected);
    memset(&blob, 0xa5, sizeof(blob));
    bool existing_latch = facts && cbm_git_facts_read_blob(facts, CBM_GIT_REV_HEAD, "binary.bin", 10,
        &blob, &error) == CBM_GIT_BLOB_ERROR && error.status == CBM_GIT_FACTS_CANCELLED &&
        gf_blob_cleared(&blob);
    cbm_git_facts_free(facts);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0); ASSERT_TRUE(opened); ASSERT_TRUE(control); ASSERT_TRUE(inventoried);
    ASSERT_TRUE(first); ASSERT_TRUE(latched); ASSERT_TRUE(existing_latch);
    PASS();
}

TEST(test_git_facts_inventory_output_limit_has_no_prefix_and_latches) {
    enum { ITEMS = 512, NAME_LENGTH = 80, QUERY_CAP = 32768 };
    gf_fixture_t fx;
    ASSERT_TRUE(gf_fixture_open(&fx, false));
    char names[ITEMS][NAME_LENGTH + 1];
    gfi_raw_entry_t rows[ITEMS];
    gfi_expected_t expected[ITEMS];
    bool configured = true;
    for (int i = 0; i < ITEMS; i++) {
        int n = snprintf(names[i], sizeof(names[i]), "entry-%04d-", i);
        if (n <= 0 || n > NAME_LENGTH) { configured = false; break; }
        memset(names[i] + n, 'x', (size_t)(NAME_LENGTH - n)); names[i][NAME_LENGTH] = '\0';
        rows[i] = (gfi_raw_entry_t){"100644", names[i], NAME_LENGTH, fx.binary};
        expected[i] = (gfi_expected_t){names[i], NAME_LENGTH, 0100644, CBM_GIT_TREE_BLOB, fx.binary};
    }
    char tree[65];
    configured = configured && (size_t)ITEMS * NAME_LENGTH > QUERY_CAP &&
        gfi_raw_tree(&fx, rows, ITEMS, tree) && gfi_head_tree(&fx, tree);
    cbm_git_facts_options_t opts = gf_options(&fx);
    cbm_git_facts_error_t error;
    cbm_git_facts_t *ample = configured ? cbm_git_facts_open(&opts, &error) : NULL;
    bool ample_open = ample && error.status == CBM_GIT_FACTS_OK;
    cbm_git_tree_inventory_t complete = gfi_poisoned();
    bool control = ample && cbm_git_facts_inventory(ample, CBM_GIT_REV_HEAD, &complete, &error) &&
        error.status == CBM_GIT_FACTS_OK && gfi_matches(&complete, expected, ITEMS);
    cbm_git_facts_free(ample);
    /* Aggregate byte budget: independent of how many native commands the API
     * uses. The literal path bytes alone exceed this cap. Open must succeed. */
    opts = gf_options(&fx);
    opts.total_output_limit = QUERY_CAP;
    cbm_git_facts_t *limited = configured ? cbm_git_facts_open(&opts, &error) : NULL;
    bool limited_open = limited && error.status == CBM_GIT_FACTS_OK;
    cbm_git_tree_inventory_t inventory = gfi_poisoned();
    bool limit = limited && !cbm_git_facts_inventory(limited, CBM_GIT_REV_HEAD, &inventory, &error) &&
        error.status == CBM_GIT_FACTS_LIMIT && gfi_cleared(&inventory);
    inventory = gfi_poisoned();
    bool latched = limited && !cbm_git_facts_inventory(limited, CBM_GIT_REV_MERGE_BASE, &inventory, &error) &&
        error.status == CBM_GIT_FACTS_LIMIT && gfi_cleared(&inventory);
    cbm_git_blob_t blob;
    memset(&blob, 0xa5, sizeof(blob));
    bool existing_latch = limited && cbm_git_facts_read_blob(limited, CBM_GIT_REV_HEAD, "entry-0000-", 11,
        &blob, &error) == CBM_GIT_BLOB_ERROR && error.status == CBM_GIT_FACTS_LIMIT &&
        gf_blob_cleared(&blob);
    cbm_git_facts_free(limited);
    int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0); ASSERT_TRUE(configured); ASSERT_TRUE(ample_open); ASSERT_TRUE(control);
    ASSERT_TRUE(limited_open); ASSERT_TRUE(limit); ASSERT_TRUE(latched); ASSERT_TRUE(existing_latch);
    PASS();
}

/* Independent D1b tests. Native objects/argv helpers above remain unchanged. */
static const unsigned char gfa_old_names[] = "D\0deleted.bin\0A\0new.bin\0M\0p.txt\0A\0submodule\0";
static const unsigned char gfa_renamed_names[] = "A\0new.c\0D\0old.c\0D\0removed.c\0";
static const unsigned char gfa_merge_names[] = "M\0new.c\0M\0revert.c\0";
static cbm_git_bytes_t gfa_poisoned(void) {
    static const unsigned char stale[] = "stale";
    return (cbm_git_bytes_t){stale, sizeof(stale)};
}
static bool gfa_clear(cbm_git_bytes_t out) { return out.data == NULL && out.length == 0; }
static bool gfa_clean_error(const cbm_git_facts_error_t *e) {
    return e->status == CBM_GIT_FACTS_OK && e->exit_code == -1 && e->diagnostic[0] == '\0';
}
static bool gfa_linear(gf_fixture_t *fx, const char *tree_a, const char *tree_m, const char *tree_h) {
    char a[65], m[65], h[65];
    bool ok = gf_commit(fx, tree_a, NULL, NULL, "ancestor A", a) &&
        gf_commit(fx, tree_m, a, NULL, "ancestor M", m) &&
        gf_commit(fx, tree_h, m, NULL, "ancestor H", h) &&
        gf_ref(fx, "refs/heads/topic", h) && gf_ref(fx, "refs/heads/base", m);
    if (ok) {
        memcpy(fx->a, a, fx->oid_length + 1); memcpy(fx->q, m, fx->oid_length + 1);
        memcpy(fx->base, m, fx->oid_length + 1); memcpy(fx->head, h, fx->oid_length + 1);
    }
    return ok;
}
static bool gfa_rename_history(gf_fixture_t *fx) {
    const gfi_raw_entry_t a[] = {
        {"100644", "old.c", 5, fx->old_text}, {"100644", "removed.c", 9, fx->binary},
        {"100644", "revert.c", 8, fx->old_text}
    };
    const gfi_raw_entry_t m[] = {
        {"100644", "new.c", 5, fx->old_text}, {"100644", "revert.c", 8, fx->head_text}
    };
    const gfi_raw_entry_t h[] = {
        {"100644", "new.c", 5, fx->head_text}, {"100644", "revert.c", 8, fx->old_text}
    };
    char ta[65], tm[65], th[65];
    return gfi_raw_tree(fx, a, 3, ta) && gfi_raw_tree(fx, m, 2, tm) &&
        gfi_raw_tree(fx, h, 2, th) && gfa_linear(fx, ta, tm, th) &&
        gf_config(fx, "diff.renames", "true");
}

TEST(test_git_ancestor_changes_requires_ancestor_of_actual_merge_base) {
    gf_fixture_t fx; ASSERT_TRUE(gf_fixture_open(&fx, false));
    cbm_git_facts_options_t opts = gf_options(&fx); cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error);
    const cbm_git_facts_identity_t *id = facts ? cbm_git_facts_identity(facts) : NULL;
    bool identity = id && strcmp(id->merge_base, fx.q) == 0 && strcmp(id->merge_base, fx.p) != 0;
    bool controls = facts && cbm_git_facts_is_ancestor(facts, fx.p, fx.head, &error) == CBM_GIT_ANCESTRY_YES &&
        cbm_git_facts_is_ancestor(facts, fx.p, fx.q, &error) == CBM_GIT_ANCESTRY_NO;
    cbm_git_bytes_t out = gfa_poisoned(); memset(&error, 0xa5, sizeof(error));
    int no = facts ? cbm_git_facts_ancestor_changes(facts, fx.p, fx.oid_length, &out, &error) : -9;
    bool normal_no = no == CBM_GIT_ANCESTOR_CHANGES_NOT_ANCESTOR && gfa_clear(out) && gfa_clean_error(&error);
    out = gfa_poisoned(); memset(&error, 0xa5, sizeof(error));
    int head = facts ? cbm_git_facts_ancestor_changes(facts, fx.head, fx.oid_length, &out, &error) : -9;
    bool head_no = head == CBM_GIT_ANCESTOR_CHANGES_NOT_ANCESTOR && gfa_clear(out) && gfa_clean_error(&error);
    out = gfa_poisoned(); memset(&error, 0xa5, sizeof(error));
    int yes = facts ? cbm_git_facts_ancestor_changes(facts, fx.a, fx.oid_length, &out, &error) : -9;
    bool reused = yes == CBM_GIT_ANCESTOR_CHANGES_OK && gfa_clean_error(&error) &&
        gf_bytes_equal(out, gfa_old_names, sizeof(gfa_old_names)-1);
    cbm_git_facts_free(facts); int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0); ASSERT_TRUE(identity); ASSERT_TRUE(controls);
    ASSERT_TRUE(normal_no); ASSERT_TRUE(head_no); ASSERT_TRUE(reused); PASS();
}

TEST(test_git_ancestor_changes_preserves_earlier_paths_reversion_and_pinned_head) {
    gf_fixture_t fx; ASSERT_TRUE(gf_fixture_open(&fx, false));
    bool configured = gfa_rename_history(&fx);
    cbm_git_facts_options_t opts = gf_options(&fx); cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = configured ? cbm_git_facts_open(&opts, &error) : NULL;
    const cbm_git_facts_identity_t *id = facts ? cbm_git_facts_identity(facts) : NULL;
    bool identity = id && strcmp(id->merge_base, fx.q) == 0 && strcmp(id->head, fx.head) == 0;
    cbm_git_diff_t old = {0};
    bool legacy = facts && cbm_git_facts_diff(facts, &old, &error) &&
        gf_bytes_equal(old.name_status, gfa_merge_names, sizeof(gfa_merge_names)-1);
    char path[GF_PATH]; const char *empty[] = {"read-tree", "--empty", NULL};
    bool moved = legacy && gf_ref(&fx, "refs/heads/topic", fx.a) &&
        gf_ref(&fx, "refs/heads/base", fx.a) && gf_git(&fx, empty, NULL, 0) &&
        gf_index(&fx, "100644", fx.binary, "ambient.c") &&
        gf_path(path, fx.repo, "new.c") && gf_write(path, "ambient\n", 8);
    cbm_git_bytes_t out = gfa_poisoned();
    int rc = facts ? cbm_git_facts_ancestor_changes(facts, fx.a, fx.oid_length, &out, &error) : -9;
    bool exact = rc == CBM_GIT_ANCESTOR_CHANGES_OK && gfa_clean_error(&error) &&
        gf_bytes_equal(out, gfa_renamed_names, sizeof(gfa_renamed_names)-1);
    cbm_git_diff_t again = {0}; bool unchanged = facts && cbm_git_facts_diff(facts, &again, &error) &&
        gf_bytes_equal(again.name_status, gfa_merge_names, sizeof(gfa_merge_names)-1);
    cbm_git_facts_free(facts); int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0); ASSERT_TRUE(configured); ASSERT_TRUE(identity); ASSERT_TRUE(legacy);
    ASSERT_TRUE(moved); ASSERT_TRUE(exact); ASSERT_TRUE(unchanged); PASS();
}

TEST(test_git_ancestor_changes_equalities_and_empty_reversion_are_distinct) {
    gf_fixture_t fx; ASSERT_TRUE(gf_fixture_open(&fx, false));
    cbm_git_facts_options_t opts = gf_options(&fx); cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = cbm_git_facts_open(&opts, &error); cbm_git_diff_t old = {0};
    bool legacy = facts && cbm_git_facts_diff(facts, &old, &error);
    cbm_git_bytes_t out = gfa_poisoned();
    bool equal_m = facts && cbm_git_facts_ancestor_changes(facts, fx.q, fx.oid_length, &out, &error) == CBM_GIT_ANCESTOR_CHANGES_OK &&
        gfa_clean_error(&error) && legacy && gf_bytes_equal(out, old.name_status.data, old.name_status.length);
    cbm_git_facts_free(facts);
    bool base_at_head = gf_ref(&fx, "refs/heads/base", fx.head);
    opts = gf_options(&fx); facts = base_at_head ? cbm_git_facts_open(&opts, &error) : NULL;
    const cbm_git_facts_identity_t *id = facts ? cbm_git_facts_identity(facts) : NULL;
    bool identity = id && strcmp(id->head, id->merge_base) == 0;
    out = gfa_poisoned(); bool all_equal = facts && cbm_git_facts_ancestor_changes(facts, fx.head, fx.oid_length, &out, &error) == CBM_GIT_ANCESTOR_CHANGES_OK &&
        gfa_clean_error(&error) && out.length == 0;
    cbm_git_facts_free(facts);
    /* Empty A->H with nonempty M->H: empty output cannot clear has_changes. */
    const gfi_raw_entry_t a[] = {{"100644", "revert.c", 8, fx.old_text}};
    const gfi_raw_entry_t m[] = {{"100644", "revert.c", 8, fx.head_text}};
    char ta[65], tm[65]; bool history = gfi_raw_tree(&fx, a, 1, ta) && gfi_raw_tree(&fx, m, 1, tm) && gfa_linear(&fx, ta, tm, ta);
    opts = gf_options(&fx); facts = history ? cbm_git_facts_open(&opts, &error) : NULL;
    old = (cbm_git_diff_t){0}; const char changed[] = "M\0revert.c\0";
    bool nonempty = facts && cbm_git_facts_diff(facts, &old, &error) && gf_bytes_equal(old.name_status, changed, sizeof(changed)-1);
    out = gfa_poisoned(); bool reverted = facts && cbm_git_facts_ancestor_changes(facts, fx.a, fx.oid_length, &out, &error) == CBM_GIT_ANCESTOR_CHANGES_OK &&
        gfa_clean_error(&error) && out.length == 0;
    cbm_git_facts_free(facts); int cleanup = th_rmtree(fx.home);
    ASSERT_EQ(cleanup, 0); ASSERT_TRUE(legacy); ASSERT_TRUE(equal_m); ASSERT_TRUE(base_at_head);
    ASSERT_TRUE(identity); ASSERT_TRUE(all_equal); ASSERT_TRUE(history); ASSERT_TRUE(nonempty); ASSERT_TRUE(reverted); PASS();
}

TEST(test_git_ancestor_changes_validates_full_commit_object_and_argument_span) {
    gf_fixture_t fx; ASSERT_TRUE(gf_fixture_open(&fx, false));
    char expression[96], tree[65], tag[65], tag_bytes[512], missing[65];
    int n = snprintf(expression, sizeof(expression), "%s^{tree}", fx.head);
    const char *tree_args[] = {"rev-parse", "--verify", expression, NULL};
    bool tree_ok = n > 0 && (size_t)n < sizeof(expression) && gf_oid(&fx, tree_args, tree);
    n = snprintf(tag_bytes, sizeof(tag_bytes), "object %s\ntype commit\ntag fixture\ntagger Git Facts Fixture <fixture@example.invalid> 946684800 +0000\n\nfixture tag\n", fx.a);
    bool tag_ok = n > 0 && (size_t)n < sizeof(tag_bytes) && gf_object(&fx, "tag", tag_bytes, (size_t)n, tag);
    memset(missing, '0', fx.oid_length); missing[fx.oid_length] = '\0';
    const char *missing_type[] = {"cat-file", "-t", missing, NULL};
    bool absent_control = gf_git_exit(&fx, missing_type, 128, NULL, 0);
    cbm_git_facts_options_t opts = gf_options(&fx); cbm_git_facts_error_t error;
    cbm_git_facts_t *facts = tree_ok && tag_ok && absent_control ? cbm_git_facts_open(&opts, &error) : NULL;
    bool opened = facts != NULL, invalid = opened;
    for (unsigned i=0; facts && i<12; i++) {
        char input[65]; memset(input, 'a', sizeof(input)); memcpy(input, fx.a, fx.oid_length);
        const char *arg = input; size_t len = fx.oid_length;
        if (i==0) arg=NULL;
        if (i==1) len=0;
        if (i==2) len=fx.oid_length-1;
        if (i==3) len=64; /* SHA-256 width is invalid for this SHA-1 owner. */
        if (i==4) input[4]='\0';
        if (i==5) input[4]='g';
        if (i==6) input[4]=' ';
        if (i==7) input[4]='^';
        if (i==8) { arg="HEAD"; len=4; }
        if (i==9) input[0]='-';
        cbm_git_bytes_t out=gfa_poisoned(); memset(&error,0xa5,sizeof(error));
        int rc=cbm_git_facts_ancestor_changes(i==10?NULL:facts,arg,len,i==11?NULL:&out,&error);
        invalid = rc==CBM_GIT_ANCESTOR_CHANGES_ERROR && error.status==CBM_GIT_FACTS_INVALID &&
            (i==11 || gfa_clear(out)) && invalid;
    }
    const char *wrong_type[] = {tree, fx.binary, tag}; bool rejected=opened;
    for (size_t i=0; facts && i<3; i++) {
        cbm_git_bytes_t out=gfa_poisoned(); memset(&error,0xa5,sizeof(error));
        int rc=cbm_git_facts_ancestor_changes(facts,wrong_type[i],fx.oid_length,&out,&error);
        rejected = rc==CBM_GIT_ANCESTOR_CHANGES_ERROR && error.status==CBM_GIT_FACTS_INVALID && gfa_clear(out) && rejected;
    }
    cbm_git_bytes_t out=gfa_poisoned(); int absent=facts?cbm_git_facts_ancestor_changes(facts,missing,fx.oid_length,&out,&error):-9;
    bool missing_error=absent==CBM_GIT_ANCESTOR_CHANGES_ERROR && error.status==CBM_GIT_FACTS_COMMAND && gfa_clear(out);
    out=gfa_poisoned(); bool reusable=facts && cbm_git_facts_ancestor_changes(facts,fx.a,fx.oid_length,&out,NULL)==CBM_GIT_ANCESTOR_CHANGES_OK &&
        gf_bytes_equal(out,gfa_old_names,sizeof(gfa_old_names)-1);
    cbm_git_facts_free(facts); int cleanup=th_rmtree(fx.home);
    ASSERT_EQ(cleanup,0); ASSERT_TRUE(tree_ok); ASSERT_TRUE(tag_ok); ASSERT_TRUE(absent_control); ASSERT_TRUE(opened);
    ASSERT_TRUE(invalid); ASSERT_TRUE(rejected); ASSERT_TRUE(missing_error); ASSERT_TRUE(reusable); PASS();
}

typedef struct { char *input; size_t length; bool armed, overwritten; } gfa_borrow_t;
/* Prompt and non-reentrant: poisons only the caller's borrowed bytes at the first gate. */
static bool gfa_poison_borrowed_oid(void *context) {
    gfa_borrow_t *c=context;
    if(c->armed && !c->overwritten) { memset(c->input,'#',c->length);c->overwritten=true; }
    return false;
}
TEST(test_git_ancestor_changes_sha256_raw_paths_and_local_oid_copy) {
    enum { LONG_PATH=65537, EXPECTED_CAP=LONG_PATH+1024 };
    for(int algorithm=0;algorithm<2;algorithm++) {
        gf_fixture_t fx; ASSERT_TRUE(gf_fixture_open(&fx,algorithm!=0));
        char long_name[LONG_PATH+1];memset(long_name,'z',LONG_PATH);long_name[LONG_PATH]='\0';
        const gfi_raw_entry_t a[] = {
            {"100644","binary",6,fx.binary},{"100644","deleted",7,fx.binary},
            {"160000","gitlink",7,fx.a},{"100644","mode",4,fx.binary},{"100644","type",4,fx.binary}
        };
        const gfi_raw_entry_t h[] = {
            {"100644","-dash",5,fx.binary},{"100644",":colon",6,fx.binary},{"100644","[glob]*",7,fx.binary},
            {"100644","back\\slash",10,fx.binary},{"100644","binary",6,fx.newer},
            {"160000","gitlink",7,fx.p},{"100644","line\nname",9,fx.binary},{"100755","mode",4,fx.binary},
            {"100644","tab\tname",8,fx.binary},{"120000","type",4,fx.link},
            {"100644",long_name,LONG_PATH,fx.binary},{"100644","\x80" "high",5,fx.binary}
        };
        char ta[65],th[65];bool history=gfi_raw_tree(&fx,a,5,ta) && gfi_raw_tree_format(&fx,h,12,false,th) && gfa_linear(&fx,ta,ta,th);
        const char prefix[]="A\0-dash\0A\0:colon\0A\0[glob]*\0A\0back\\slash\0M\0binary\0D\0deleted\0M\0gitlink\0A\0line\nname\0M\0mode\0A\0tab\tname\0T\0type\0A\0";
        const char suffix[]="A\0\x80" "high\0";
        unsigned char expected[EXPECTED_CAP];size_t used=0;
        memcpy(expected+used,prefix,sizeof(prefix)-1);used+=sizeof(prefix)-1;
        memcpy(expected+used,long_name,LONG_PATH);used+=LONG_PATH;expected[used++]=0;
        memcpy(expected+used,suffix,sizeof(suffix)-1);used+=sizeof(suffix)-1;
        char caller_oid[64];memset(caller_oid,'?',sizeof(caller_oid));memcpy(caller_oid,fx.a,fx.oid_length);
        unsigned uppercase=0;
        for(unsigned i=0;i<fx.oid_length;i++)if(caller_oid[i]>='a' && caller_oid[i]<='f'){caller_oid[i]=(char)(caller_oid[i]-'a'+'A');uppercase++;}
        gfa_borrow_t borrow={.input=caller_oid,.length=fx.oid_length};
        cbm_git_facts_options_t opts=gf_options(&fx);opts.cancelled=gfa_poison_borrowed_oid;opts.cancel_context=&borrow;
        cbm_git_facts_error_t error;cbm_git_facts_t *facts=history?cbm_git_facts_open(&opts,&error):NULL;
        const cbm_git_facts_identity_t *id=facts?cbm_git_facts_identity(facts):NULL;
        bool identity=id && id->oid_hex_length==(algorithm?64U:40U) && strcmp(id->merge_base,fx.q)==0;
        cbm_git_bytes_t out=gfa_poisoned();borrow.armed=true;
        int rc=facts?cbm_git_facts_ancestor_changes(facts,caller_oid,fx.oid_length,&out,&error):-9;
        borrow.armed=false;
        bool exact=rc==CBM_GIT_ANCESTOR_CHANGES_OK && gfa_clean_error(&error) && gf_bytes_equal(out,expected,used);
        memset(caller_oid,'!',sizeof(caller_oid));bool retained=exact && gf_bytes_equal(out,expected,used);
        cbm_git_bytes_t again=gfa_poisoned();
        bool equal_m=facts && cbm_git_facts_ancestor_changes(facts,fx.q,fx.oid_length,&again,&error)==CBM_GIT_ANCESTOR_CHANGES_OK && gf_bytes_equal(again,expected,used);
        bool wrong_width=true;
        if(algorithm) { again=gfa_poisoned();int bad=cbm_git_facts_ancestor_changes(facts,fx.a,40,&again,&error);wrong_width=bad==CBM_GIT_ANCESTOR_CHANGES_ERROR && error.status==CBM_GIT_FACTS_INVALID && gfa_clear(again); }
        cbm_git_facts_free(facts);int cleanup=th_rmtree(fx.home);
        ASSERT_EQ(cleanup,0);ASSERT_TRUE(history);ASSERT_TRUE(identity);ASSERT_TRUE(uppercase>0);ASSERT_TRUE(borrow.overwritten);
        ASSERT_TRUE(exact);ASSERT_TRUE(retained);ASSERT_TRUE(equal_m);ASSERT_TRUE(wrong_width);
    }
    PASS();
}

TEST(test_git_ancestor_changes_repeats_preserve_all_previous_owner_buffers) {
    gf_fixture_t fx;ASSERT_TRUE(gf_fixture_open(&fx,false));bool configured=gfa_rename_history(&fx);
    cbm_git_facts_options_t opts=gf_options(&fx);cbm_git_facts_error_t error;
    cbm_git_facts_t *facts=configured?cbm_git_facts_open(&opts,&error):NULL;cbm_git_diff_t legacy={0};
    bool old=facts && cbm_git_facts_diff(facts,&legacy,&error);
    unsigned char patch[16384],names[1024];bool copied=old && legacy.patch.length>0 && legacy.patch.length<=sizeof(patch) && legacy.name_status.length<=sizeof(names);
    if(copied){memcpy(patch,legacy.patch.data,legacy.patch.length);memcpy(names,legacy.name_status.data,legacy.name_status.length);}
    const unsigned char *old_patch=legacy.patch.data,*old_names=legacy.name_status.data;
    cbm_git_bytes_t output=gfa_poisoned();bool first=facts && cbm_git_facts_ancestor_changes(facts,fx.a,fx.oid_length,&output,&error)==CBM_GIT_ANCESTOR_CHANGES_OK;
    cbm_git_bytes_t alias_a=output;
    bool second=facts && cbm_git_facts_ancestor_changes(facts,fx.q,fx.oid_length,&output,&error)==CBM_GIT_ANCESTOR_CHANGES_OK;
    cbm_git_bytes_t alias_m=output;
    bool no=facts && cbm_git_facts_ancestor_changes(facts,fx.head,fx.oid_length,&output,&error)==CBM_GIT_ANCESTOR_CHANGES_NOT_ANCESTOR && gfa_clear(output) && gfa_clean_error(&error);
    bool invalid=facts && cbm_git_facts_ancestor_changes(facts,"HEAD",4,&output,&error)==CBM_GIT_ANCESTOR_CHANGES_ERROR && error.status==CBM_GIT_FACTS_INVALID && gfa_clear(output);
    cbm_git_diff_t later={0};bool repeated=facts && cbm_git_facts_diff(facts,&later,&error);
    bool retained=copied && first && second && legacy.patch.data==old_patch && legacy.name_status.data==old_names &&
        gf_bytes_equal(legacy.patch,patch,legacy.patch.length) && gf_bytes_equal(legacy.name_status,names,legacy.name_status.length) &&
        gf_bytes_equal(alias_a,gfa_renamed_names,sizeof(gfa_renamed_names)-1) &&
        gf_bytes_equal(alias_m,gfa_merge_names,sizeof(gfa_merge_names)-1) && repeated &&
        gf_bytes_equal(later.patch,patch,legacy.patch.length) && gf_bytes_equal(later.name_status,names,legacy.name_status.length);
    cbm_git_facts_free(facts);int cleanup=th_rmtree(fx.home);
    ASSERT_EQ(cleanup,0);ASSERT_TRUE(configured);ASSERT_TRUE(old);ASSERT_TRUE(copied);ASSERT_TRUE(first);ASSERT_TRUE(second);
    ASSERT_TRUE(no);ASSERT_TRUE(invalid);ASSERT_TRUE(repeated);ASSERT_TRUE(retained);PASS();
}

TEST(test_git_ancestor_changes_cancellation_is_shared_terminal_and_preserves_views) {
    gf_fixture_t fx;ASSERT_TRUE(gf_fixture_open(&fx,false));bool cancelled=false;
    cbm_git_facts_options_t opts=gf_options(&fx);opts.cancelled=gf_cancelled;opts.cancel_context=&cancelled;
    cbm_git_facts_error_t error;cbm_git_facts_t *facts=cbm_git_facts_open(&opts,&error);
    cbm_git_bytes_t old=gfa_poisoned();bool control=facts && cbm_git_facts_ancestor_changes(facts,fx.q,fx.oid_length,&old,&error)==CBM_GIT_ANCESTOR_CHANGES_OK &&
        gf_bytes_equal(old,gfa_old_names,sizeof(gfa_old_names)-1);
    cancelled=true;cbm_git_bytes_t out=gfa_poisoned();
    bool rejected=facts && cbm_git_facts_ancestor_changes(facts,fx.a,fx.oid_length,&out,&error)==CBM_GIT_ANCESTOR_CHANGES_ERROR &&
        error.status==CBM_GIT_FACTS_CANCELLED && gfa_clear(out);
    cancelled=false;out=gfa_poisoned();
    bool sticky=facts && cbm_git_facts_ancestor_changes(facts,fx.q,fx.oid_length,&out,&error)==CBM_GIT_ANCESTOR_CHANGES_ERROR &&
        error.status==CBM_GIT_FACTS_CANCELLED && gfa_clear(out);
    cbm_git_diff_t diff;memset(&diff,0xa5,sizeof(diff));
    bool shared=facts && !cbm_git_facts_diff(facts,&diff,&error) && error.status==CBM_GIT_FACTS_CANCELLED &&
        gfa_clear(diff.patch) && gfa_clear(diff.name_status);
    bool retained=control && gf_bytes_equal(old,gfa_old_names,sizeof(gfa_old_names)-1);
    cbm_git_facts_free(facts);int cleanup=th_rmtree(fx.home);
    ASSERT_EQ(cleanup,0);ASSERT_TRUE(control);ASSERT_TRUE(rejected);ASSERT_TRUE(sticky);ASSERT_TRUE(shared);ASSERT_TRUE(retained);PASS();
}

TEST(test_git_ancestor_changes_output_and_cumulative_limits_publish_no_prefix) {
    enum { ITEMS=512, LENGTH=80, CAP=32768 };
    gf_fixture_t fx;ASSERT_TRUE(gf_fixture_open(&fx,false));
    char names[ITEMS][LENGTH+1];gfi_raw_entry_t rows[ITEMS];bool configured=true;
    for(int i=0;i<ITEMS;i++) {
        int n=snprintf(names[i],sizeof(names[i]),"entry-%04d-",i);
        if(n<=0 || n>LENGTH){configured=false;break;}
        memset(names[i]+n,'x',(size_t)(LENGTH-n));names[i][LENGTH]='\0';
        rows[i]=(gfi_raw_entry_t){"100644",names[i],LENGTH,fx.binary};
    }
    char tree[65];configured=configured && gfi_raw_tree(&fx,rows,ITEMS,tree) && gfi_head_tree(&fx,tree);
    cbm_git_facts_options_t opts=gf_options(&fx);cbm_git_facts_error_t error;
    cbm_git_facts_t *ample=configured?cbm_git_facts_open(&opts,&error):NULL;cbm_git_bytes_t complete=gfa_poisoned();
    bool control=ample && cbm_git_facts_ancestor_changes(ample,fx.q,fx.oid_length,&complete,&error)==CBM_GIT_ANCESTOR_CHANGES_OK &&
        gfa_clean_error(&error) && complete.length>CAP && complete.data;
    bool all_entries=control;
    for(int i=0;i<ITEMS && all_entries;i++) {char record[LENGTH+3];record[0]='A';record[1]=0;memcpy(record+2,names[i],LENGTH);record[LENGTH+2]=0;
        bool found=false;for(size_t j=0;j+sizeof(record)<=complete.length;j++)if(memcmp(complete.data+j,record,sizeof(record))==0){found=true;break;}all_entries=found;}
    cbm_git_facts_free(ample);
    bool caps=true;
    for(int cap=0;cap<2;cap++) {
        opts=gf_options(&fx);if(cap==0)opts.stdout_limit=CAP;else opts.total_output_limit=CAP;
        cbm_git_facts_t *facts=configured?cbm_git_facts_open(&opts,&error):NULL;
        bool opened=facts && error.status==CBM_GIT_FACTS_OK;cbm_git_blob_t blob={0};
        bool earlier=facts && cbm_git_facts_read_blob(facts,CBM_GIT_REV_MERGE_BASE,"binary.bin",10,&blob,&error)==CBM_GIT_BLOB_FOUND &&
            gf_bytes_equal(blob.bytes,gf_binary,sizeof(gf_binary));
        cbm_git_bytes_t out=gfa_poisoned();bool limit=facts && cbm_git_facts_ancestor_changes(facts,fx.q,fx.oid_length,&out,&error)==CBM_GIT_ANCESTOR_CHANGES_ERROR &&
            error.status==CBM_GIT_FACTS_LIMIT && gfa_clear(out);
        out=gfa_poisoned();bool sticky=facts && cbm_git_facts_ancestor_changes(facts,fx.a,fx.oid_length,&out,&error)==CBM_GIT_ANCESTOR_CHANGES_ERROR && error.status==CBM_GIT_FACTS_LIMIT && gfa_clear(out);
        bool retained=earlier && gf_bytes_equal(blob.bytes,gf_binary,sizeof(gf_binary));
        cbm_git_tree_inventory_t inventory=gfi_poisoned();bool shared=facts && !cbm_git_facts_inventory(facts,CBM_GIT_REV_HEAD,&inventory,&error) && error.status==CBM_GIT_FACTS_LIMIT && gfi_cleared(&inventory);
        caps=opened && earlier && limit && sticky && retained && shared && caps;cbm_git_facts_free(facts);
    }
    int cleanup=th_rmtree(fx.home);ASSERT_EQ(cleanup,0);ASSERT_TRUE(configured);ASSERT_TRUE(control);ASSERT_TRUE(all_entries);ASSERT_TRUE(caps);PASS();
}

TEST(test_git_ancestor_changes_repeated_calls_share_existing_command_budget) {
    gf_fixture_t fx;ASSERT_TRUE(gf_fixture_open(&fx,false));
    cbm_git_facts_options_t opts=gf_options(&fx);opts.command_limit=64;
    cbm_git_facts_error_t error;cbm_git_facts_t *facts=cbm_git_facts_open(&opts,&error);
    bool opened=facts && error.status==CBM_GIT_FACTS_OK,prior=opened;cbm_git_blob_t kept={0};
    for(unsigned i=0;i<3 && facts;i++)prior=cbm_git_facts_read_blob(facts,CBM_GIT_REV_HEAD,"binary.bin",10,&kept,&error)==CBM_GIT_BLOB_FOUND &&
        gf_bytes_equal(kept.bytes,gf_binary,sizeof(gf_binary)) && prior;
    bool limit=false,unexpected=false;unsigned successes=0;cbm_git_bytes_t first={0};
    for(unsigned i=0;i<=opts.command_limit && facts;i++) {
        cbm_git_bytes_t out=gfa_poisoned();int rc=cbm_git_facts_ancestor_changes(facts,fx.q,fx.oid_length,&out,&error);
        if(rc==CBM_GIT_ANCESTOR_CHANGES_OK) {
            if(!gfa_clean_error(&error) || !gf_bytes_equal(out,gfa_old_names,sizeof(gfa_old_names)-1)){unexpected=true;break;}
            if(successes==0) { first=out; }
            successes++;
        } else {limit=rc==CBM_GIT_ANCESTOR_CHANGES_ERROR && error.status==CBM_GIT_FACTS_LIMIT && gfa_clear(out);break;}
    }
    cbm_git_bytes_t out=gfa_poisoned();bool sticky=facts && cbm_git_facts_ancestor_changes(facts,fx.a,fx.oid_length,&out,&error)==CBM_GIT_ANCESTOR_CHANGES_ERROR && error.status==CBM_GIT_FACTS_LIMIT && gfa_clear(out);
    cbm_git_blob_t rejected;memset(&rejected,0xa5,sizeof(rejected));bool shared=facts && cbm_git_facts_read_blob(facts,CBM_GIT_REV_HEAD,"binary.bin",10,&rejected,&error)==CBM_GIT_BLOB_ERROR && error.status==CBM_GIT_FACTS_LIMIT && gf_blob_cleared(&rejected);
    bool retained=successes>0 && gf_bytes_equal(first,gfa_old_names,sizeof(gfa_old_names)-1) && gf_bytes_equal(kept.bytes,gf_binary,sizeof(gf_binary));
    cbm_git_facts_free(facts);int cleanup=th_rmtree(fx.home);
    ASSERT_EQ(cleanup,0);ASSERT_TRUE(opened);ASSERT_TRUE(prior);ASSERT_TRUE(successes>0);ASSERT_TRUE(!unexpected);ASSERT_TRUE(limit);ASSERT_TRUE(sticky);ASSERT_TRUE(shared);ASSERT_TRUE(retained);PASS();
}

static bool gfa_topology_stays_pinned(void) {
    gf_fixture_t original,redirected;
    if(!gf_fixture_open(&original,false))return false;
    if(!gf_fixture_open(&redirected,false)){(void)th_rmtree(original.home);return false;}
    bool same=strcmp(original.q,redirected.q)==0 && strcmp(original.p,redirected.p)==0;
    char original_git[GF_PATH];bool named=gf_path(original_git,original.home,"ancestor-original-git");
    const char *relocate[]={"-c","init.templateDir=","init","--quiet","--separate-git-dir",original_git,NULL};
    bool relocated=same && named && gf_git(&original,relocate,NULL,0);
    cbm_git_facts_options_t opts=gf_options(&original);cbm_git_facts_error_t error;
    cbm_git_facts_t *facts=relocated?cbm_git_facts_open(&opts,&error):NULL;
    cbm_git_bytes_t out=gfa_poisoned();
    bool before=facts && cbm_git_facts_ancestor_changes(facts,original.p,original.oid_length,&out,&error)==CBM_GIT_ANCESTOR_CHANGES_NOT_ANCESTOR && gfa_clear(out) && gfa_clean_error(&error);
    char target[GF_PATH],info[GF_PATH],graft_path[GF_PATH],gitfile_path[GF_PATH];
    char graft[160],gitfile[GF_PATH+32];int n=snprintf(graft,sizeof(graft),"%s %s\n",original.q,original.p);
    bool grafted=n>0 && (size_t)n<sizeof(graft) && gf_path(target,redirected.repo,".git") &&
        gf_path(info,target,"info") && th_mkdir_p(info)==0 && gf_path(graft_path,info,"grafts") && gf_write(graft_path,graft,(size_t)n);
    n=grafted?snprintf(gitfile,sizeof(gitfile),"gitdir: %s\n",target):-1;
    bool repointed=n>0 && (size_t)n<sizeof(gitfile) && gf_path(gitfile_path,original.repo,".git") && gf_write(gitfile_path,gitfile,(size_t)n);
    const char *control[]={"--no-replace-objects","merge-base","--is-ancestor",original.p,original.q,NULL};
    bool false_yes=repointed && gf_git(&original,control,NULL,0);
    out=gfa_poisoned();int after=facts?cbm_git_facts_ancestor_changes(facts,original.p,original.oid_length,&out,&error):-9;
    bool safe=gfa_clear(out) && ((after==CBM_GIT_ANCESTOR_CHANGES_NOT_ANCESTOR && gfa_clean_error(&error)) ||
        (after==CBM_GIT_ANCESTOR_CHANGES_ERROR && error.status!=CBM_GIT_FACTS_OK));
    cbm_git_facts_free(facts);int one=th_rmtree(original.home),two=th_rmtree(redirected.home);
    return same && relocated && before && grafted && repointed && false_yes && safe && one==0 && two==0;
}

TEST(test_git_ancestor_changes_history_and_external_driver_guards_apply) {
    /* A=M must still reach the history guard; equality is not a cached answer. */
    for(int shallow=0;shallow<2;shallow++) {
        gf_fixture_t fx;ASSERT_TRUE(gf_fixture_open(&fx,false));
        cbm_git_facts_options_t opts=gf_options(&fx);cbm_git_facts_error_t error;cbm_git_facts_t *facts=cbm_git_facts_open(&opts,&error);
        cbm_git_bytes_t out=gfa_poisoned();bool control=facts && cbm_git_facts_ancestor_changes(facts,fx.q,fx.oid_length,&out,&error)==CBM_GIT_ANCESTOR_CHANGES_OK && gf_bytes_equal(out,gfa_old_names,sizeof(gfa_old_names)-1);
        char info[GF_PATH],path[GF_PATH],content[160];
        int n=shallow?snprintf(content,sizeof(content),"%s\n",fx.head):
            snprintf(content,sizeof(content),"%s %s\n",fx.head,fx.a);
        bool changed=n>0 && (size_t)n<sizeof(content) && (shallow?
            gf_path(path,fx.repo,".git/shallow"):
            (gf_path(info,fx.repo,".git/info") && th_mkdir_p(info)==0 && gf_path(path,info,"grafts"))) && gf_write(path,content,(size_t)n);
        bool effective=false;
        if(shallow){const char *args[]={"rev-parse","--is-shallow-repository",NULL};char text[32];effective=changed && gf_git(&fx,args,text,sizeof(text)) && strcmp(text,"true\n")==0;}
        else {const char *args[]={"merge-base","--all",fx.head,fx.base,NULL};char altered[65];effective=changed && gf_oid(&fx,args,altered) && strcmp(altered,fx.a)==0;}
        out=gfa_poisoned();bool rejected=facts && cbm_git_facts_ancestor_changes(facts,fx.q,fx.oid_length,&out,&error)==CBM_GIT_ANCESTOR_CHANGES_ERROR && error.status==CBM_GIT_FACTS_UNSUPPORTED && gfa_clear(out);
        cbm_git_facts_free(facts);int cleanup=th_rmtree(fx.home);ASSERT_EQ(cleanup,0);ASSERT_TRUE(control);ASSERT_TRUE(changed);ASSERT_TRUE(effective);ASSERT_TRUE(rejected);
    }
    ASSERT_TRUE(gfa_topology_stays_pinned());
    gf_fixture_t fx;ASSERT_TRUE(gf_fixture_open(&fx,false));
    char binary[GF_PATH],marker[GF_PATH],command[GF_PATH*3],attributes_path[GF_PATH];
    bool prepared=gf_path(binary,fx.home,
#ifdef _WIN32
        "ancestor-diff-probe.exe"
#else
        "ancestor-diff-probe"
#endif
    ) && gf_path(marker,fx.home,"ancestor-driver-invoked") && gf_runner_binary;
    if(prepared){cbm_normalize_path_sep(binary);cbm_normalize_path_sep(marker);prepared=cbm_copy_file(gf_runner_binary,binary)==0;}
    if(prepared)th_make_executable(binary);
    const char attributes[]="p.txt diff=fixturetrap\n";
    prepared=prepared && gf_path(attributes_path,fx.repo,".gitattributes") && gf_write(attributes_path,attributes,sizeof(attributes)-1) && gf_driver_command(command,sizeof(command),binary,marker);
    bool controls[2]={false},isolated[2]={false};
    for(int i=0;prepared && i<2;i++) {
        const char *key=i==0?"diff.external":"diff.fixturetrap.textconv";
        const char *probe[]={"diff",i==0?"--ext-diff":"--textconv",fx.q,fx.head,"--","p.txt",NULL};
        controls[i]=gf_config(&fx,key,command) && gf_git(&fx,probe,NULL,0) && cbm_file_exists(marker);
        bool reset=controls[i] && cbm_unlink(marker)==0;
        cbm_git_facts_options_t opts=gf_options(&fx);cbm_git_facts_error_t error;cbm_git_facts_t *facts=cbm_git_facts_open(&opts,&error);
        cbm_git_bytes_t out=gfa_poisoned();isolated[i]=reset && facts && cbm_git_facts_ancestor_changes(facts,fx.a,fx.oid_length,&out,&error)==CBM_GIT_ANCESTOR_CHANGES_OK &&
            gf_bytes_equal(out,gfa_old_names,sizeof(gfa_old_names)-1) && !cbm_file_exists(marker);
        cbm_git_facts_free(facts);const char *unset[]={"config","--local","--unset",key,NULL};prepared=gf_git(&fx,unset,NULL,0) && prepared;
    }
    int cleanup=th_rmtree(fx.home);ASSERT_EQ(cleanup,0);ASSERT_TRUE(prepared);ASSERT_TRUE(controls[0]);ASSERT_TRUE(controls[1]);ASSERT_TRUE(isolated[0]);ASSERT_TRUE(isolated[1]);PASS();
}

/* Independent D1c cases. These helpers build native fixture objects only;
 * no adapter implementation helpers, private command counts or fake Git. */
enum { GFB_COLON, GFB_GLOB, GFB_EXEC, GFB_EMPTY, GFB_HEADER, GFB_LARGE_A,
       GFB_LARGE_B, GFB_LARGE_C, GFB_LINE, GFB_LINK, GFB_TEXT, GFB_SUBMODULE,
       GFB_TAB, GFB_UTF, GFB_COUNT, GFB_BODY = 70001 };
static const unsigned char gfb_binary[] = {0, 'N', '\r', '\n', 0x1a, 0xff, '\n', 'Z'};
static const char gfb_header_body[] =
    "1111111111111111111111111111111111111111 blob 0\n\nnot-a-frame\n";
static const char gfb_utf[] = {'u', 't', 'f', '-', (char)0x80, (char)0xff};
typedef struct {
    gf_fixture_t git;
    CBMArena arena;
    unsigned char *large[3];
    char empty_oid[65], binary_oid[65], header_oid[65], large_oid[3][65];
    gfi_expected_t expected[GFB_COUNT];
} gfb_fixture_t;

static bool gfb_open(gfb_fixture_t *f, bool sha256) {
    memset(f, 0, sizeof(*f));
    cbm_arena_init_lazy(&f->arena, 4096);
    if (!gf_fixture_open(&f->git, sha256))
        return false;
    bool ok = gf_object(&f->git, "blob", "", 0, f->empty_oid) &&
        gf_object(&f->git, "blob", gfb_binary, sizeof(gfb_binary), f->binary_oid) &&
        gf_object(&f->git, "blob", gfb_header_body, sizeof(gfb_header_body)-1, f->header_oid);
    for (int j = 0; ok && j < 3; j++) {
        f->large[j] = cbm_arena_alloc(&f->arena, GFB_BODY);
        ok = f->large[j] != NULL;
        for (size_t i = 0; ok && i < GFB_BODY; i++)
            f->large[j][i] = (unsigned char)((i + (size_t)j * 31) % 251);
        if (ok)
            ok = gf_object(&f->git, "blob", f->large[j], GFB_BODY, f->large_oid[j]);
    }
    const char *paths[] = {":colon", "[g]*", "back\\slash", "empty", "header", "large-a",
        "large-b", "large-c", "line\nbreak", "link", "p.txt", "submodule", "tab\tname", gfb_utf};
    const char *oids[] = {f->binary_oid, f->binary_oid, f->binary_oid, f->empty_oid,
        f->header_oid, f->large_oid[0], f->large_oid[1], f->large_oid[2], f->binary_oid,
        f->git.link, f->git.head_text, f->git.a, f->binary_oid, f->binary_oid};
    gfi_raw_entry_t rows[GFB_COUNT];
    for (size_t i = 0; i < GFB_COUNT; i++) {
        uint32_t mode = i == GFB_EXEC ? 0100755 : i == GFB_LINK ? 0120000 :
                        i == GFB_SUBMODULE ? 0160000 : 0100644;
        const char *mode_text = i == GFB_EXEC ? "100755" : i == GFB_LINK ? "120000" :
                                i == GFB_SUBMODULE ? "160000" : "100644";
        size_t length = i == GFB_UTF ? sizeof(gfb_utf) : strlen(paths[i]);
        f->expected[i] = (gfi_expected_t){paths[i], length, mode,
            i == GFB_SUBMODULE ? CBM_GIT_TREE_COMMIT : CBM_GIT_TREE_BLOB, oids[i]};
        rows[i] = (gfi_raw_entry_t){mode_text, paths[i], length, oids[i]};
    }
    char th[65], tm[65], q[65], h[65], b[65];
    ok = ok && gfi_raw_tree(&f->git, rows, GFB_COUNT, th);
    rows[GFB_TEXT].oid = f->git.old_text;
    ok = ok && gfi_raw_tree(&f->git, rows, GFB_COUNT, tm) &&
        gf_commit(&f->git, tm, f->git.a, NULL, "batch Q", q) &&
        gf_commit(&f->git, th, f->git.p, q, "batch H", h) &&
        gf_commit(&f->git, tm, q, NULL, "batch B", b) &&
        gf_ref(&f->git, "refs/heads/topic", h) && gf_ref(&f->git, "refs/heads/base", b);
    if (ok) {
        memcpy(f->git.q, q, f->git.oid_length+1);
        memcpy(f->git.head, h, f->git.oid_length+1);
        memcpy(f->git.base, b, f->git.oid_length+1);
    }
    return ok;
}

static int gfb_close(gfb_fixture_t *f) {
    cbm_arena_destroy(&f->arena);
    return f->git.home[0] ? th_rmtree(f->git.home) : 0;
}

static cbm_git_blob_batch_limits_t gfb_limits(void) {
    return (cbm_git_blob_batch_limits_t){.max_entries=1024, .max_input_bytes=1024*1024,
                                        .max_arena_bytes=32U*1024U*1024U};
}

static cbm_git_blob_batch_t gfb_poisoned(void) {
    static const cbm_git_blob_batch_item_t old = {.inventory_index=123};
    return (cbm_git_blob_batch_t){.revision=CBM_GIT_REV_MERGE_BASE,
                                .commit="old", .items=&old, .count=1};
}

static bool gfb_cleared(cbm_git_blob_batch_t out) {
    return out.revision == 0 && out.commit == NULL && out.items == NULL && out.count == 0;
}

static cbm_git_bytes_t gfb_expected_bytes(gfb_fixture_t *f, cbm_git_revision_t rev, size_t index) {
    if (index == GFB_EMPTY) return (cbm_git_bytes_t){NULL,0};
    if (index == GFB_HEADER) return (cbm_git_bytes_t){(const unsigned char *)gfb_header_body,
                                                    sizeof(gfb_header_body)-1};
    if (index >= GFB_LARGE_A && index <= GFB_LARGE_C)
        return (cbm_git_bytes_t){f->large[index-GFB_LARGE_A],GFB_BODY};
    if (index == GFB_TEXT) {
        const char *text = rev == CBM_GIT_REV_HEAD ? gf_head_text : gf_old_text;
        return (cbm_git_bytes_t){(const unsigned char *)text,strlen(text)};
    }
    return (cbm_git_bytes_t){gfb_binary,sizeof(gfb_binary)};
}

static bool gfb_matches(gfb_fixture_t *f, cbm_git_revision_t rev, const size_t *indices,
                         size_t count, const cbm_git_blob_batch_t *out) {
    const char *commit = rev == CBM_GIT_REV_HEAD ? f->git.head : f->git.q;
    if (out->revision != rev || !out->commit || strcmp(out->commit,commit) ||
        out->count != count || (count ? !out->items : out->items != NULL)) return false;
    for (size_t i=0;i<count;i++) {
        size_t index=indices[i];
        if (index>=GFB_COUNT || index==GFB_LINK || index==GFB_SUBMODULE) return false;
        const cbm_git_blob_batch_item_t *item=&out->items[i];
        const cbm_git_tree_entry_t *e=item->entry;
        const gfi_expected_t *want=&f->expected[index];
        const char *oid=index==GFB_TEXT && rev==CBM_GIT_REV_MERGE_BASE ? f->git.old_text : want->oid;
        cbm_git_bytes_t bytes=gfb_expected_bytes(f,rev,index);
        if (item->inventory_index!=index || !e || !e->path ||
            e->path_length!=want->length || memcmp(e->path,want->path,want->length) ||
            e->path[e->path_length]!=0 || e->mode!=want->mode || e->object_type!=CBM_GIT_TREE_BLOB ||
            strcmp(e->oid,oid) || !gf_bytes_equal(item->bytes,bytes.data,bytes.length)) return false;
    }
    return true;
}

/* A failed Git facts call names its stage and error: these fixtures fail
 * intermittently on macos-15-intel CI only, and a bare ASSERT left nothing to
 * attribute. */
static void gfb_report(const char *stage, const cbm_git_facts_error_t *e) {
    fprintf(stderr, "Git facts %s failed: status=%d exit=%d diagnostic=%.300s\n", stage, e->status,
            e->exit_code, e->diagnostic);
}

static bool gfb_inventory_control(gfb_fixture_t *f, cbm_git_facts_t *facts) {
    cbm_git_facts_error_t error;
    memset(&error, 0, sizeof(error));
    cbm_git_tree_inventory_t inventory={0};
    const cbm_git_facts_identity_t *id=cbm_git_facts_identity(facts);
    if (!id || strcmp(id->merge_base,f->git.q)!=0 || strcmp(id->merge_base,f->git.p)==0) {
        fprintf(stderr, "Git facts identity control failed: identity=%d\n", id != NULL);
        return false;
    }
    if (!cbm_git_facts_inventory(facts,CBM_GIT_REV_HEAD,&inventory,&error)) {
        gfb_report("inventory", &error);
        return false;
    }
    return gfi_matches(&inventory,f->expected,GFB_COUNT);
}

static bool gfb_good(gfb_fixture_t *f, cbm_git_facts_t *facts, cbm_git_revision_t rev,
                     const size_t *indices, size_t count, const cbm_git_blob_batch_limits_t *limits,
                     cbm_git_blob_batch_t *keep) {
    cbm_git_blob_batch_request_t request={rev,indices,count};
    cbm_git_blob_batch_t out=gfb_poisoned(); cbm_git_facts_error_t error;
    memset(&error,0xa5,sizeof(error));
    bool read=cbm_git_facts_read_blob_batch(facts,&request,limits,&out,&error);
    if (!read || !gfa_clean_error(&error))
        gfb_report(read ? "blob batch (unclean error)" : "blob batch", &error);
    bool ok=read && gfa_clean_error(&error) && gfb_matches(f,rev,indices,count,&out);
    if (keep) *keep=out;
    return ok;
}

static bool gfb_error(cbm_git_facts_t *facts, const cbm_git_blob_batch_request_t *request,
                      const cbm_git_blob_batch_limits_t *limits, cbm_git_facts_status_t expected) {
    cbm_git_blob_batch_t out=gfb_poisoned(); cbm_git_facts_error_t error;
    memset(&error,0xa5,sizeof(error));
    return !cbm_git_facts_read_blob_batch(facts,request,limits,&out,&error) &&
        error.status==expected && gfb_cleared(out);
}

TEST(test_git_blob_batch_native_bytes_and_both_object_formats) {
    bool setups=true, exact=true, clean=true;
    for (int algorithm=0;algorithm<2;algorithm++) {
        gfb_fixture_t f; bool configured=gfb_open(&f,algorithm!=0);
        cbm_git_facts_options_t opts=gf_options(&f.git); cbm_git_facts_error_t error;
        cbm_git_facts_t *facts=configured?cbm_git_facts_open(&opts,&error):NULL;
        bool control=facts && gfb_inventory_control(&f,facts);
        const size_t head[]={GFB_EMPTY,GFB_COLON,GFB_HEADER,GFB_LARGE_A,GFB_EXEC,GFB_TEXT};
        const size_t merge[]={GFB_TEXT,GFB_LARGE_B,GFB_EMPTY};
        cbm_git_blob_batch_limits_t limits=gfb_limits();
        exact=control && gfb_good(&f,facts,CBM_GIT_REV_HEAD,head,6,&limits,NULL) &&
            gfb_good(&f,facts,CBM_GIT_REV_MERGE_BASE,merge,3,&limits,NULL) && exact;
        setups=configured && control && setups;
        cbm_git_facts_free(facts); clean=gfb_close(&f)==0 && clean;
    }
    ASSERT_TRUE(clean); ASSERT_TRUE(setups); ASSERT_TRUE(exact); PASS();
}

TEST(test_git_blob_batch_keeps_request_order_duplicates_and_raw_paths) {
    gfb_fixture_t f; bool configured=gfb_open(&f,true);
    cbm_git_facts_options_t opts=gf_options(&f.git); cbm_git_facts_error_t error;
    cbm_git_facts_t *facts=configured?cbm_git_facts_open(&opts,&error):NULL;
    bool control=facts && gfb_inventory_control(&f,facts);
    const size_t indices[]={GFB_UTF,GFB_EXEC,GFB_COLON,GFB_TAB,GFB_LINE,GFB_GLOB,GFB_EXEC,GFB_UTF};
    cbm_git_blob_batch_limits_t limits=gfb_limits(); cbm_git_blob_batch_t out={0};
    bool exact=control && gfb_good(&f,facts,CBM_GIT_REV_HEAD,indices,8,&limits,&out);
    bool distinct=exact && out.items[1].entry->mode==0100755 && out.items[2].entry->mode==0100644 &&
        strcmp(out.items[1].entry->oid,out.items[2].entry->oid)==0 &&
        out.items[1].entry->path_length!=out.items[2].entry->path_length;
    cbm_git_facts_free(facts); int cleanup=gfb_close(&f);
    ASSERT_EQ(cleanup,0); ASSERT_TRUE(configured); ASSERT_TRUE(control); ASSERT_TRUE(exact);
    ASSERT_TRUE(distinct); PASS();
}

TEST(test_git_blob_batch_pinned_revisions_and_cache_history_guards) {
    gfb_fixture_t f; bool configured=gfb_open(&f,false);
    cbm_git_facts_options_t opts=gf_options(&f.git); cbm_git_facts_error_t error;
    cbm_git_facts_t *facts=configured?cbm_git_facts_open(&opts,&error):NULL;
    const size_t index=GFB_TEXT; cbm_git_blob_batch_limits_t limits=gfb_limits();
    bool control=facts && gfb_inventory_control(&f,facts);
    bool before=control && gfb_good(&f,facts,CBM_GIT_REV_HEAD,&index,1,&limits,NULL);
    char path[GF_PATH];
    const char *empty_index[]={"read-tree","--empty",NULL};
    bool mutation=configured && gf_path(path,f.git.repo,"p.txt") && gf_write(path,"ambient\n",8) &&
        gf_git(&f.git,empty_index,NULL,0) && gf_ref(&f.git,"refs/heads/topic",f.git.a) &&
        gf_ref(&f.git,"refs/heads/base",f.git.p);
    bool pinned=before && mutation && gfb_good(&f,facts,CBM_GIT_REV_HEAD,&index,1,&limits,NULL) &&
        gfb_good(&f,facts,CBM_GIT_REV_MERGE_BASE,&index,1,&limits,NULL);
    char graft[160],info[GF_PATH];int n=snprintf(graft,sizeof(graft),"%s %s\n",f.git.head,f.git.a);
    bool written=control && n>0 && (size_t)n<sizeof(graft) &&
        gf_path(info,f.git.repo,".git/info") && th_mkdir_p(info)==0 &&
        gf_path(path,info,"grafts") && gf_write(path,graft,(size_t)n);
    cbm_git_blob_batch_request_t request={CBM_GIT_REV_HEAD,&index,1};
    cbm_git_blob_batch_request_t empty={CBM_GIT_REV_HEAD,NULL,0};
    bool guarded=written && gfb_error(facts,&request,&limits,CBM_GIT_FACTS_UNSUPPORTED) &&
        gfb_error(facts,&empty,&limits,CBM_GIT_FACTS_UNSUPPORTED);
    bool removed=written && th_unlink_force(path)==0;
    cbm_git_facts_free(facts);
    bool restored=removed && gf_ref(&f.git,"refs/heads/topic",f.git.head) &&
        gf_ref(&f.git,"refs/heads/base",f.git.base);
    opts=gf_options(&f.git);facts=restored?cbm_git_facts_open(&opts,&error):NULL;
    if (restored && !facts) gfb_report("open (shallow control)", &error);
    bool shallow_before=facts && gfb_good(&f,facts,CBM_GIT_REV_HEAD,&index,1,&limits,NULL);
    n=snprintf(graft,sizeof(graft),"%s\n",f.git.q);
    bool shallow_written=restored && n>0 && (size_t)n<sizeof(graft) &&
        gf_path(path,f.git.repo,".git/shallow") && gf_write(path,graft,(size_t)n);
    const char *shallow_probe[]={"rev-parse","--is-shallow-repository",NULL};
    char shallow_value[16];
    bool shallow_control=shallow_written && gf_git(&f.git,shallow_probe,shallow_value,sizeof(shallow_value)) &&
        (strcmp(shallow_value,"true\n")==0 || strcmp(shallow_value,"true\r\n")==0);
    bool shallow_guarded=shallow_control && gfb_error(facts,&request,&limits,CBM_GIT_FACTS_UNSUPPORTED) &&
        gfb_error(facts,&empty,&limits,CBM_GIT_FACTS_UNSUPPORTED);
    bool shallow_removed=shallow_written && th_unlink_force(path)==0;
    cbm_git_facts_free(facts);
    opts=gf_options(&f.git);facts=shallow_removed?cbm_git_facts_open(&opts,&error):NULL;
    bool topology_before=facts && gfb_good(&f,facts,CBM_GIT_REV_HEAD,&index,1,&limits,NULL);
    char alternate[GF_PATH],common_text[GF_PATH+2];
    bool alternate_ready=shallow_removed && gf_path(alternate,f.git.home,"alternate-common") &&
        th_mkdir_p(alternate)==0;
    n=alternate_ready?snprintf(common_text,sizeof(common_text),"%s\n",alternate):-1;
    bool topology_written=alternate_ready && n>0 && (size_t)n<sizeof(common_text) &&
        gf_path(path,f.git.repo,".git/commondir") && gf_write(path,common_text,(size_t)n);
    cbm_git_blob_batch_t rejected=gfb_poisoned();
    bool topology_guarded=topology_written &&
        !cbm_git_facts_read_blob_batch(facts,&request,&limits,&rejected,&error) &&
        error.status!=CBM_GIT_FACTS_OK && gfb_cleared(rejected);
    rejected=gfb_poisoned();
    topology_guarded=topology_written &&
        !cbm_git_facts_read_blob_batch(facts,&empty,&limits,&rejected,&error) &&
        error.status!=CBM_GIT_FACTS_OK && gfb_cleared(rejected) && topology_guarded;
    bool topology_removed=topology_written && th_unlink_force(path)==0;
    cbm_git_facts_free(facts);
    /* Equal pinned commits retain the requested revision, even if cache is shared. */
    bool equal_refs=topology_removed && gf_ref(&f.git,"refs/heads/topic",f.git.head) &&
        gf_ref(&f.git,"refs/heads/base",f.git.head);
    opts=gf_options(&f.git); facts=equal_refs?cbm_git_facts_open(&opts,&error):NULL;
    char old_q[65];memcpy(old_q,f.git.q,sizeof(old_q));memcpy(f.git.q,f.git.head,sizeof(f.git.q));
    const size_t equal_index=GFB_HEADER;
    bool equal=facts && gfb_good(&f,facts,CBM_GIT_REV_HEAD,&equal_index,1,&limits,NULL) &&
        gfb_good(&f,facts,CBM_GIT_REV_MERGE_BASE,&equal_index,1,&limits,NULL) &&
        gfb_good(&f,facts,CBM_GIT_REV_HEAD,NULL,0,&limits,NULL) &&
        gfb_good(&f,facts,CBM_GIT_REV_MERGE_BASE,NULL,0,&limits,NULL);
    memcpy(f.git.q,old_q,sizeof(f.git.q));
    cbm_git_facts_free(facts);int cleanup=gfb_close(&f);
    ASSERT_EQ(cleanup,0);ASSERT_TRUE(configured);ASSERT_TRUE(control);ASSERT_TRUE(before);
    ASSERT_TRUE(mutation);ASSERT_TRUE(pinned);ASSERT_TRUE(written);ASSERT_TRUE(guarded);
    ASSERT_TRUE(removed);ASSERT_TRUE(restored);ASSERT_TRUE(shallow_before);
    ASSERT_TRUE(shallow_control);ASSERT_TRUE(shallow_guarded);ASSERT_TRUE(shallow_removed);
    ASSERT_TRUE(topology_before);ASSERT_TRUE(topology_written);ASSERT_TRUE(topology_guarded);
    ASSERT_TRUE(topology_removed);ASSERT_TRUE(equal_refs);ASSERT_TRUE(equal);PASS();
}

TEST(test_git_blob_batch_validates_full_selection_and_empty_requests) {
    gfb_fixture_t f;bool configured=gfb_open(&f,false);
    cbm_git_facts_options_t opts=gf_options(&f.git);cbm_git_facts_error_t error;
    cbm_git_facts_t *facts=configured?cbm_git_facts_open(&opts,&error):NULL;
    bool control=facts && gfb_inventory_control(&f,facts);
    cbm_git_blob_batch_limits_t limits=gfb_limits();const size_t good=GFB_COLON;
    bool positive=control && gfb_good(&f,facts,CBM_GIT_REV_HEAD,&good,1,&limits,NULL);
    bool ok=positive;
    size_t indices[]={good,GFB_COUNT};cbm_git_blob_batch_request_t request={CBM_GIT_REV_HEAD,indices,2};
    ok=control && gfb_error(facts,&request,&limits,CBM_GIT_FACTS_INVALID) && ok;
    indices[1]=GFB_LINK;ok=control && gfb_error(facts,&request,&limits,CBM_GIT_FACTS_UNSUPPORTED) && ok;
    indices[1]=GFB_SUBMODULE;ok=control && gfb_error(facts,&request,&limits,CBM_GIT_FACTS_UNSUPPORTED) && ok;
    request=(cbm_git_blob_batch_request_t){CBM_GIT_REV_BASE,&good,1};
    ok=control && gfb_error(facts,&request,&limits,CBM_GIT_FACTS_INVALID) && ok;
    request.revision=(cbm_git_revision_t)99;
    ok=control && gfb_error(facts,&request,&limits,CBM_GIT_FACTS_INVALID) && ok;
    request=(cbm_git_blob_batch_request_t){CBM_GIT_REV_HEAD,NULL,1};
    ok=control && gfb_error(facts,&request,&limits,CBM_GIT_FACTS_INVALID) && ok;
    request=(cbm_git_blob_batch_request_t){CBM_GIT_REV_HEAD,&good,1};
    ok=control && gfb_error(NULL,&request,&limits,CBM_GIT_FACTS_INVALID) &&
        gfb_error(facts,NULL,&limits,CBM_GIT_FACTS_INVALID) &&
        gfb_error(facts,&request,NULL,CBM_GIT_FACTS_INVALID) && ok;
    for(int i=0;control && i<3;i++) {
        cbm_git_blob_batch_limits_t zero=limits;
        if(i==0)zero.max_entries=0;
        if(i==1)zero.max_input_bytes=0;
        if(i==2)zero.max_arena_bytes=0;
        ok=gfb_error(facts,&request,&zero,CBM_GIT_FACTS_INVALID) && ok;
    }
    bool no_out=control && !cbm_git_facts_read_blob_batch(facts,&request,&limits,NULL,&error) &&
        error.status==CBM_GIT_FACTS_INVALID;
    bool reused=control && gfb_good(&f,facts,CBM_GIT_REV_HEAD,&good,1,&limits,NULL) &&
        gfb_good(&f,facts,CBM_GIT_REV_HEAD,NULL,0,&limits,NULL) &&
        gfb_good(&f,facts,CBM_GIT_REV_MERGE_BASE,NULL,0,&limits,NULL);
    cbm_git_facts_free(facts);int cleanup=gfb_close(&f);
    ASSERT_EQ(cleanup,0);ASSERT_TRUE(configured);ASSERT_TRUE(control);ASSERT_TRUE(positive);
    ASSERT_TRUE(ok);ASSERT_TRUE(no_out);ASSERT_TRUE(reused);PASS();
}

static size_t gfb_frame_size(size_t width, size_t size) {
    size_t digits=1;
    for(size_t n=size;n>=10;n/=10)digits++;
    return width+1+4+1+digits+1+size+1;
}

TEST(test_git_blob_batch_exact_frame_caps_and_native_split_batches) {
    gfb_fixture_t f;bool configured=gfb_open(&f,false);
    const size_t indices[]={GFB_LARGE_C,GFB_LARGE_A,GFB_LARGE_B};
    cbm_git_blob_batch_limits_t limits=gfb_limits();
    size_t exact=gfb_frame_size(f.git.oid_length,GFB_BODY);
    bool controls=configured, outcomes=true;
    for(int variant=0;configured && variant<3;variant++) {
        cbm_git_facts_options_t opts=gf_options(&f.git);
        opts.stdout_limit=variant==2?exact-1:exact;
        cbm_git_facts_error_t error;cbm_git_facts_t *facts=cbm_git_facts_open(&opts,&error);
        if (!facts) gfb_report("open (budget variant)", &error);
        bool opened=facts && gfb_inventory_control(&f,facts);
        controls=opened && controls;
        if(variant==0)outcomes=opened &&
            gfb_good(&f,facts,CBM_GIT_REV_HEAD,indices,1,&limits,NULL) && outcomes;
        if(variant==1)outcomes=opened &&
            gfb_good(&f,facts,CBM_GIT_REV_HEAD,indices,3,&limits,NULL) && outcomes;
        if(variant==2) {
            const size_t mixed[]={GFB_EMPTY,GFB_COLON,GFB_LARGE_C};
            cbm_git_blob_batch_request_t request={CBM_GIT_REV_HEAD,mixed,3};
            bool limited=opened && gfb_error(facts,&request,&limits,CBM_GIT_FACTS_LIMIT);
            cbm_git_blob_batch_request_t empty={CBM_GIT_REV_HEAD,NULL,0};
            outcomes=limited && gfb_error(facts,&empty,&limits,CBM_GIT_FACTS_LIMIT) && outcomes;
        }
        cbm_git_facts_free(facts);
    }
    int cleanup=gfb_close(&f);
    ASSERT_EQ(cleanup,0);ASSERT_TRUE(configured);ASSERT_TRUE(controls);ASSERT_TRUE(outcomes);PASS();
}

TEST(test_git_blob_batch_per_call_and_existing_shared_budgets) {
    gfb_fixture_t f;bool configured=gfb_open(&f,false);
    const size_t repeated[]={GFB_COLON,GFB_EXEC,GFB_HEADER,GFB_COLON};
    /* Four entries, but exactly two distinct OIDs and two physical phases. */
    size_t exact_input=2U*2U*(f.git.oid_length+1U);
    cbm_git_blob_batch_request_t request={CBM_GIT_REV_HEAD,repeated,4};
    cbm_git_blob_batch_limits_t ample=gfb_limits();bool controls=configured, outcomes=true;
    for(int variant=0;configured && variant<4;variant++) {
        cbm_git_facts_options_t opts=gf_options(&f.git);cbm_git_facts_error_t error;
        cbm_git_facts_t *facts=cbm_git_facts_open(&opts,&error);
        cbm_git_blob_t retained={0};
        bool before=facts && gfb_inventory_control(&f,facts) &&
            cbm_git_facts_read_blob(facts,CBM_GIT_REV_HEAD,"p.txt",5,&retained,&error)==CBM_GIT_BLOB_FOUND &&
            gf_bytes_equal(retained.bytes,gf_head_text,sizeof(gf_head_text)-1);
        controls=before && controls;
        cbm_git_blob_batch_limits_t limit=ample;
        if(variant==0) {
            limit.max_entries=4;limit.max_input_bytes=exact_input;
            outcomes=before && gfb_good(&f,facts,CBM_GIT_REV_HEAD,repeated,4,&limit,NULL) &&
                gfb_good(&f,facts,CBM_GIT_REV_HEAD,repeated,4,&limit,NULL) && outcomes;
        } else {
            cbm_git_blob_batch_request_t chosen=request;
            if(variant==1)limit.max_entries=3;
            if(variant==2)limit.max_input_bytes=exact_input-1;
            if(variant==3) {
                /* No selected objects/items: guard/helper allocations still count. */
                chosen=(cbm_git_blob_batch_request_t){CBM_GIT_REV_HEAD,NULL,0};
                bool empty_control=before && gfb_good(&f,facts,CBM_GIT_REV_HEAD,NULL,0,&ample,NULL);
                outcomes=empty_control && outcomes;
                limit.max_arena_bytes=1;
            }
            bool limited=before && gfb_error(facts,&chosen,&limit,CBM_GIT_FACTS_LIMIT);
            outcomes=limited && gfb_error(facts,&request,&ample,CBM_GIT_FACTS_LIMIT) && outcomes;
        }
        outcomes=gf_bytes_equal(retained.bytes,gf_head_text,sizeof(gf_head_text)-1) && outcomes;
        cbm_git_facts_free(facts);
    }
    /* Repeated successful calls cannot reset shared native-command/output caps.
     * Bounds below use public limits and a minimum of one real command/body per
     * success, never an exact internal command count or phase callback number. */
    for(int budget=0;configured && budget<2;budget++) {
        cbm_git_facts_options_t opts=gf_options(&f.git);
        if(budget==0)opts.command_limit=64;
        else opts.total_output_limit=3U*GFB_BODY+32768U;
        cbm_git_facts_error_t error;cbm_git_facts_t *facts=cbm_git_facts_open(&opts,&error);
        cbm_git_blob_t retained={0};
        bool before=facts && cbm_git_facts_read_blob(facts,CBM_GIT_REV_HEAD,"p.txt",5,&retained,&error)==CBM_GIT_BLOB_FOUND &&
            gf_bytes_equal(retained.bytes,gf_head_text,sizeof(gf_head_text)-1);
        controls=before && controls;
        size_t index=budget==0?GFB_HEADER:GFB_LARGE_A;
        cbm_git_blob_batch_request_t one={CBM_GIT_REV_HEAD,&index,1};
        cbm_git_blob_batch_t first={0};unsigned successes=0;
        bool limited=false, unexpected=false;
        unsigned bound=budget==0?opts.command_limit+1U:5U;
        for(unsigned i=0;before && i<bound;i++) {
            cbm_git_blob_batch_t out=gfb_poisoned();
            if(cbm_git_facts_read_blob_batch(facts,&one,&ample,&out,&error)) {
                if(!gfa_clean_error(&error) || !gfb_matches(&f,CBM_GIT_REV_HEAD,&index,1,&out)) {
                    unexpected=true;break;
                }
                if(successes==0)first=out;
                successes++;
            } else {
                limited=error.status==CBM_GIT_FACTS_LIMIT && gfb_cleared(out);break;
            }
        }
        bool sticky=before && gfb_error(facts,&one,&ample,CBM_GIT_FACTS_LIMIT);
        bool retained_ok=successes>0 && gfb_matches(&f,CBM_GIT_REV_HEAD,&index,1,&first) &&
            gf_bytes_equal(retained.bytes,gf_head_text,sizeof(gf_head_text)-1);
        outcomes=before && successes>0 && !unexpected && limited && sticky && retained_ok && outcomes;
        cbm_git_facts_free(facts);
    }
    int cleanup=gfb_close(&f);
    ASSERT_EQ(cleanup,0);ASSERT_TRUE(configured);ASSERT_TRUE(controls);ASSERT_TRUE(outcomes);PASS();
}

static bool gfb_header_error(cbm_git_bytes_t bytes,const char *oid,size_t width,
                             cbm_git_facts_status_t expected) {
    size_t size=123, header=456;cbm_git_facts_error_t error;
    memset(&error,0xa5,sizeof(error));
    return !cbm_git_facts_test_batch_header(bytes,oid,width,&size,&header,&error) &&
        error.status==expected && size==0 && header==0;
}

TEST(test_git_blob_batch_strict_headers_and_native_missing_object) {
    bool grammar=true;
    for(size_t width=40;width<=64;width+=24) {
        char oid[65];memset(oid,'a',width);oid[width]=0;
        char capture[256];int n=snprintf(capture,sizeof(capture),"%s blob 3\nXYZ\n",oid);
        size_t size=123,header=456;cbm_git_facts_error_t error;
        bool positive=n>0 && (size_t)n<sizeof(capture) &&
            cbm_git_facts_test_batch_header((cbm_git_bytes_t){(const unsigned char *)capture,(size_t)n},
                oid,width,&size,&header,&error) && size==3 && header==width+8 && gfa_clean_error(&error);
        grammar=positive && grammar;
        const char *suffixes[]={" blob 01\n"," blob -1\n"," blob +1\n"," blob  1\n",
            " blob 1\r\n"," blob 1 extra\n"," missing\n"," tree 1\n"," blob 1", "\tblob 1\n"};
        for(size_t i=0;i<sizeof(suffixes)/sizeof(suffixes[0]);i++) {
            n=snprintf(capture,sizeof(capture),"%s%s",oid,suffixes[i]);
            grammar=n>0 && (size_t)n<sizeof(capture) &&
                gfb_header_error((cbm_git_bytes_t){(const unsigned char *)capture,(size_t)n},
                                 oid,width,CBM_GIT_FACTS_COMMAND) && grammar;
        }
        n=snprintf(capture,sizeof(capture),"%s blob %zu0\n",oid,SIZE_MAX);
        grammar=n>0 && (size_t)n<sizeof(capture) &&
            gfb_header_error((cbm_git_bytes_t){(const unsigned char *)capture,(size_t)n},oid,width,CBM_GIT_FACTS_LIMIT) && grammar;
        n=snprintf(capture,sizeof(capture),"%s blob %zu0x\n",oid,SIZE_MAX);
        grammar=n>0 && (size_t)n<sizeof(capture) &&
            gfb_header_error((cbm_git_bytes_t){(const unsigned char *)capture,(size_t)n},oid,width,CBM_GIT_FACTS_COMMAND) && grammar;
        n=snprintf(capture,sizeof(capture),"%s blob 0\n",oid);
        capture[0]='b';
        grammar=gfb_header_error((cbm_git_bytes_t){(const unsigned char *)capture,(size_t)n},oid,width,CBM_GIT_FACTS_COMMAND) && grammar;
        capture[0]='A';
        grammar=gfb_header_error((cbm_git_bytes_t){(const unsigned char *)capture,(size_t)n},oid,width,CBM_GIT_FACTS_COMMAND) && grammar;
        capture[0]='a';
        grammar=gfb_header_error((cbm_git_bytes_t){(const unsigned char *)capture,(size_t)n},oid,39,CBM_GIT_FACTS_INVALID) &&
            gfb_header_error((cbm_git_bytes_t){NULL,1},oid,width,CBM_GIT_FACTS_INVALID) && grammar;
        size=123;header=456;
        grammar=!cbm_git_facts_test_batch_header(
            (cbm_git_bytes_t){(const unsigned char *)capture,(size_t)n},oid,width,NULL,&header,&error) &&
            error.status==CBM_GIT_FACTS_INVALID && header==0 && grammar;
        grammar=!cbm_git_facts_test_batch_header(
            (cbm_git_bytes_t){(const unsigned char *)capture,(size_t)n},oid,width,&size,NULL,&error) &&
            error.status==CBM_GIT_FACTS_INVALID && size==0 && grammar;
        oid[0]='A';
        grammar=gfb_header_error((cbm_git_bytes_t){(const unsigned char *)capture,(size_t)n},oid,width,CBM_GIT_FACTS_INVALID) && grammar;
    }
    gfb_fixture_t f;bool configured=gfb_open(&f,false);
    cbm_git_facts_options_t opts=gf_options(&f.git);cbm_git_facts_error_t error;
    cbm_git_facts_t *facts=configured?cbm_git_facts_open(&opts,&error):NULL;
    cbm_git_blob_t native={0};
    bool control=facts && gfb_inventory_control(&f,facts) &&
        cbm_git_facts_read_blob(facts,CBM_GIT_REV_HEAD,"header",6,&native,&error)==CBM_GIT_BLOB_FOUND &&
        gf_bytes_equal(native.bytes,gfb_header_body,sizeof(gfb_header_body)-1);
    const size_t index=GFB_HEADER;cbm_git_blob_batch_limits_t limits=gfb_limits();
    bool batch=control && gfb_good(&f,facts,CBM_GIT_REV_HEAD,&index,1,&limits,NULL);
    char object[GF_PATH];int n=snprintf(object,sizeof(object),"%s/.git/objects/%.2s/%s",
                                       f.git.repo,f.header_oid,f.header_oid+2);
    bool removed=control && n>0 && (size_t)n<sizeof(object) && th_unlink_force(object)==0;
    cbm_git_blob_batch_request_t request={CBM_GIT_REV_HEAD,&index,1};
    bool missing=removed && gfb_error(facts,&request,&limits,CBM_GIT_FACTS_COMMAND);
    bool retained=control && gf_bytes_equal(native.bytes,gfb_header_body,sizeof(gfb_header_body)-1);
    cbm_git_facts_free(facts);int cleanup=gfb_close(&f);
    ASSERT_EQ(cleanup,0);ASSERT_TRUE(configured);ASSERT_TRUE(control);ASSERT_TRUE(grammar);
    ASSERT_TRUE(batch);ASSERT_TRUE(removed);ASSERT_TRUE(missing);ASSERT_TRUE(retained);PASS();
}

typedef struct { bool armed; size_t calls, cancel_at; } gfb_cancel_t;
static bool gfb_cancel(void *opaque) {
    gfb_cancel_t *c=opaque;
    if(!c->armed)return false;
    c->calls++;
    return c->cancel_at && c->calls>=c->cancel_at;
}

TEST(test_git_blob_batch_cancellation_is_sticky_and_owner_local) {
    gfb_fixture_t f;bool configured=gfb_open(&f,false);
    const size_t index=GFB_LARGE_A;cbm_git_blob_batch_limits_t limits=gfb_limits();
    cbm_git_blob_batch_request_t request={CBM_GIT_REV_HEAD,&index,1};
    gfb_cancel_t observed={0};cbm_git_facts_options_t opts=gf_options(&f.git);
    opts.cancelled=gfb_cancel;opts.cancel_context=&observed;
    cbm_git_facts_error_t error;cbm_git_facts_t *healthy=configured?cbm_git_facts_open(&opts,&error):NULL;
    bool control=healthy && gfb_inventory_control(&f,healthy);
    observed.armed=true;
    bool positive=control && gfb_good(&f,healthy,CBM_GIT_REV_HEAD,&index,1,&limits,NULL) && observed.calls>=2;
    observed.armed=false;
    bool stopped=true;
    /* Entry and final publication require at least two polls on any successful
     * nonempty call. These two early thresholds do not identify a phase or rely
     * on the process-supervision loop's variable callback count. */
    for(size_t threshold=1;configured && threshold<=2;threshold++) {
        gfb_cancel_t cancel={0};opts=gf_options(&f.git);opts.cancelled=gfb_cancel;opts.cancel_context=&cancel;
        cbm_git_facts_t *facts=cbm_git_facts_open(&opts,&error);
        const size_t small=GFB_COLON;cbm_git_blob_batch_t kept={0};
        bool before=facts && gfb_good(&f,facts,CBM_GIT_REV_HEAD,&small,1,&limits,&kept);
        cancel.armed=true;cancel.cancel_at=threshold;
        bool rejected=before && gfb_error(facts,&request,&limits,CBM_GIT_FACTS_CANCELLED);
        cancel.armed=false;
        cbm_git_blob_batch_request_t empty={CBM_GIT_REV_HEAD,NULL,0};
        bool latched=rejected && gfb_error(facts,&empty,&limits,CBM_GIT_FACTS_CANCELLED) &&
            gfb_error(facts,&request,&limits,CBM_GIT_FACTS_CANCELLED);
        bool retained=before && gfb_matches(&f,CBM_GIT_REV_HEAD,&small,1,&kept);
        stopped=before && rejected && latched && retained && stopped;
        cbm_git_facts_free(facts);
    }
    bool independent=control && gfb_good(&f,healthy,CBM_GIT_REV_HEAD,&index,1,&limits,NULL);
    cbm_git_facts_free(healthy);
    /* Existing deterministic expired-open boundary; no live-clock race. */
    opts=gf_options(&f.git);opts.deadline_ms=1;
    cbm_git_facts_t *expired=configured?cbm_git_facts_open(&opts,&error):NULL;
    bool deadline=configured && !expired && error.status==CBM_GIT_FACTS_DEADLINE;
    cbm_git_facts_free(expired);int cleanup=gfb_close(&f);
    ASSERT_EQ(cleanup,0);ASSERT_TRUE(configured);ASSERT_TRUE(control);ASSERT_TRUE(positive);
    ASSERT_TRUE(stopped);ASSERT_TRUE(independent);ASSERT_TRUE(deadline);PASS();
}

static cbm_git_bytes_t gfb_frame(CBMArena *arena,const char *oid,const void *body,size_t length,
                                bool payload) {
    char header[128];int n=snprintf(header,sizeof(header),"%s blob %zu\n",oid,length);
    if(n<=0 || (size_t)n>=sizeof(header) || length>SIZE_MAX-(size_t)n-1)
        return (cbm_git_bytes_t){NULL,0};
    size_t total=(size_t)n+(payload?length+1:0);
    unsigned char *bytes=cbm_arena_alloc(arena,total);
    if(!bytes)return (cbm_git_bytes_t){NULL,0};
    memcpy(bytes,header,(size_t)n);
    if(payload) {
        if(length)memcpy(bytes+n,body,length);
        bytes[total-1]='\n';
    }
    return (cbm_git_bytes_t){bytes,total};
}

static cbm_git_bytes_t gfb_concat(CBMArena *arena,cbm_git_bytes_t a,cbm_git_bytes_t b) {
    if(a.length>SIZE_MAX-b.length)return (cbm_git_bytes_t){NULL,0};
    size_t size=a.length+b.length;unsigned char *bytes=cbm_arena_alloc(arena,size?size:1);
    if(!bytes)return (cbm_git_bytes_t){NULL,0};
    if(a.length)memcpy(bytes,a.data,a.length);
    if(b.length)memcpy(bytes+a.length,b.data,b.length);
    return (cbm_git_bytes_t){bytes,size};
}

static bool gfb_capture_error(cbm_git_bytes_t capture,size_t width,const char *const *oids,
                              const size_t *sizes,size_t count,bool payload,
                              cbm_git_facts_status_t expected) {
    cbm_git_facts_error_t error;memset(&error,0xa5,sizeof(error));
    return !cbm_git_facts_test_batch_capture(capture,width,oids,sizes,count,payload,&error) &&
        error.status==expected;
}

TEST(test_git_blob_batch_exact_payload_frames_and_no_prefix_acceptance) {
    bool controls=true, outcomes=true;
    for(size_t width=40;width<=64;width+=24) {
        CBMArena arena;cbm_arena_init_lazy(&arena,4096);
        char a_oid[65],b_oid[65];memset(a_oid,'a',width);memset(b_oid,'b',width);
        a_oid[width]=b_oid[width]=0;
        const char *oids[]={a_oid,b_oid};
        const size_t sizes[]={sizeof(gfb_binary),sizeof(gfb_header_body)-1};
        cbm_git_bytes_t a=gfb_frame(&arena,a_oid,gfb_binary,sizes[0],true);
        cbm_git_bytes_t b=gfb_frame(&arena,b_oid,gfb_header_body,sizes[1],true);
        bool allocated=a.data && b.data;
        cbm_git_bytes_t complete=allocated?gfb_concat(&arena,a,b):(cbm_git_bytes_t){NULL,0};
        cbm_git_facts_error_t error;memset(&error,0xa5,sizeof(error));
        bool positive=complete.data && cbm_git_facts_test_batch_capture(complete,width,oids,sizes,2,true,&error) &&
            gfa_clean_error(&error);
        controls=positive && controls;
        if(complete.data) {
            for(size_t cut=0;cut<complete.length;cut++)
                outcomes=gfb_capture_error((cbm_git_bytes_t){complete.data,cut},width,oids,sizes,2,true,CBM_GIT_FACTS_COMMAND) && outcomes;
            cbm_git_bytes_t reversed=gfb_concat(&arena,b,a),duplicate=gfb_concat(&arena,a,a),extra=gfb_concat(&arena,complete,a);
            outcomes=reversed.data && duplicate.data && extra.data &&
                gfb_capture_error(reversed,width,oids,sizes,2,true,CBM_GIT_FACTS_COMMAND) &&
                gfb_capture_error(duplicate,width,oids,sizes,2,true,CBM_GIT_FACTS_COMMAND) &&
                gfb_capture_error(extra,width,oids,sizes,2,true,CBM_GIT_FACTS_COMMAND) && outcomes;
            unsigned char *mutable=cbm_arena_alloc(&arena,complete.length+1);
            outcomes=mutable!=NULL && outcomes;
            if(mutable) {
                memcpy(mutable,complete.data,complete.length);mutable[a.length-1]=0;
                outcomes=gfb_capture_error((cbm_git_bytes_t){mutable,complete.length},width,oids,sizes,2,true,CBM_GIT_FACTS_COMMAND) && outcomes;
                memcpy(mutable,complete.data,complete.length);mutable[0]='c';
                outcomes=gfb_capture_error((cbm_git_bytes_t){mutable,complete.length},width,oids,sizes,2,true,CBM_GIT_FACTS_COMMAND) && outcomes;
                memcpy(mutable,complete.data,complete.length);mutable[width+1]='t';
                outcomes=gfb_capture_error((cbm_git_bytes_t){mutable,complete.length},width,oids,sizes,2,true,CBM_GIT_FACTS_COMMAND) && outcomes;
                memcpy(mutable,complete.data,complete.length);mutable[complete.length]='\n';
                outcomes=gfb_capture_error((cbm_git_bytes_t){mutable,complete.length+1},width,oids,sizes,2,true,CBM_GIT_FACTS_COMMAND) && outcomes;
            }
            size_t wrong_sizes[]={sizes[0]+1,sizes[1]};
            outcomes=gfb_capture_error(complete,width,oids,wrong_sizes,2,true,CBM_GIT_FACTS_COMMAND) &&
                gfb_capture_error(complete,width,oids,NULL,2,true,CBM_GIT_FACTS_INVALID) &&
                gfb_capture_error(complete,width,NULL,sizes,2,true,CBM_GIT_FACTS_INVALID) &&
                gfb_capture_error(complete,39,oids,sizes,2,true,CBM_GIT_FACTS_INVALID) && outcomes;
        }
        cbm_git_bytes_t ah=gfb_frame(&arena,a_oid,NULL,sizes[0],false),bh=gfb_frame(&arena,b_oid,NULL,sizes[1],false);
        cbm_git_bytes_t preflight=ah.data && bh.data?gfb_concat(&arena,ah,bh):(cbm_git_bytes_t){NULL,0};
        bool check=preflight.data && cbm_git_facts_test_batch_capture(preflight,width,oids,NULL,2,false,&error) &&
            cbm_git_facts_test_batch_capture(preflight,width,oids,sizes,2,false,&error);
        size_t wrong[]={sizes[0],sizes[1]+1};
        outcomes=check && gfb_capture_error(preflight,width,oids,wrong,2,false,CBM_GIT_FACTS_COMMAND) && outcomes;
        cbm_git_bytes_t empty_object=gfb_frame(&arena,a_oid,NULL,0,true);
        size_t zero=0;
        outcomes=empty_object.data && cbm_git_facts_test_batch_capture(empty_object,width,oids,&zero,1,true,&error) &&
            cbm_git_facts_test_batch_capture((cbm_git_bytes_t){NULL,0},width,NULL,NULL,0,true,&error) &&
            cbm_git_facts_test_batch_capture((cbm_git_bytes_t){NULL,0},width,NULL,NULL,0,false,&error) &&
            gfb_capture_error(empty_object,width,NULL,NULL,0,true,CBM_GIT_FACTS_COMMAND) && outcomes;
        cbm_arena_destroy(&arena);
    }
    ASSERT_TRUE(controls);ASSERT_TRUE(outcomes);PASS();
}

TEST(test_git_blob_batch_owner_lifetimes_and_legacy_views_survive) {
    gfb_fixture_t f;bool configured=gfb_open(&f,false);
    cbm_git_facts_options_t opts=gf_options(&f.git);cbm_git_facts_error_t error;
    cbm_git_facts_t *facts=configured?cbm_git_facts_open(&opts,&error):NULL;
    cbm_git_tree_inventory_t inventory={0};cbm_git_blob_t blob={0};cbm_git_diff_t diff={0};
    cbm_git_bytes_t ancestor={0};
    static const unsigned char changed[]="M\0p.txt\0";
    bool legacy=facts && cbm_git_facts_inventory(facts,CBM_GIT_REV_HEAD,&inventory,&error) &&
        gfi_matches(&inventory,f.expected,GFB_COUNT) &&
        cbm_git_facts_read_blob(facts,CBM_GIT_REV_HEAD,"p.txt",5,&blob,&error)==CBM_GIT_BLOB_FOUND &&
        gf_bytes_equal(blob.bytes,gf_head_text,sizeof(gf_head_text)-1) &&
        cbm_git_facts_diff(facts,&diff,&error) && gf_bytes_equal(diff.name_status,changed,sizeof(changed)-1) &&
        cbm_git_facts_ancestor_changes(facts,f.git.q,f.git.oid_length,&ancestor,&error)==CBM_GIT_ANCESTOR_CHANGES_OK &&
        gf_bytes_equal(ancestor,changed,sizeof(changed)-1);
    unsigned char *patch=legacy?cbm_arena_alloc(&f.arena,diff.patch.length?diff.patch.length:1):NULL;
    bool copied=patch!=NULL && diff.patch.length>0;
    if(copied)memcpy(patch,diff.patch.data,diff.patch.length);
    CBMArena input;cbm_arena_init_lazy(&input,4096);
    size_t *indices=cbm_arena_alloc(&input,3*sizeof(*indices));
    static const size_t wanted[]={GFB_LARGE_A,GFB_UTF,GFB_EMPTY};
    if(indices)memcpy(indices,wanted,sizeof(wanted));
    cbm_git_blob_batch_limits_t limits=gfb_limits();cbm_git_blob_batch_t first={0},second={0};
    bool one=legacy && indices && gfb_good(&f,facts,CBM_GIT_REV_HEAD,indices,3,&limits,&first);
    if(indices)for(size_t i=0;i<3;i++)indices[i]=SIZE_MAX;
    cbm_arena_destroy(&input);
    const size_t merge[]={GFB_TEXT,GFB_HEADER};
    bool two=legacy && gfb_good(&f,facts,CBM_GIT_REV_MERGE_BASE,merge,2,&limits,&second);
    size_t invalid=GFB_COUNT;cbm_git_blob_batch_request_t request={CBM_GIT_REV_HEAD,&invalid,1};
    bool ordinary=legacy && gfb_error(facts,&request,&limits,CBM_GIT_FACTS_INVALID);
    const size_t next=GFB_EXEC;request=(cbm_git_blob_batch_request_t){CBM_GIT_REV_HEAD,&next,1};
    cbm_git_blob_batch_t optional=gfb_poisoned();
    bool again=legacy && cbm_git_facts_read_blob_batch(facts,&request,&limits,&optional,NULL) &&
        gfb_matches(&f,CBM_GIT_REV_HEAD,&next,1,&optional);
    cbm_git_blob_batch_limits_t limited=limits;limited.max_entries=1;
    request=(cbm_git_blob_batch_request_t){CBM_GIT_REV_HEAD,wanted,3};
    bool stopped=legacy && gfb_error(facts,&request,&limited,CBM_GIT_FACTS_LIMIT);
    bool stable=one && two && copied && gfb_matches(&f,CBM_GIT_REV_HEAD,wanted,3,&first) &&
        gfb_matches(&f,CBM_GIT_REV_MERGE_BASE,merge,2,&second) &&
        gfi_matches(&inventory,f.expected,GFB_COUNT) &&
        gf_bytes_equal(blob.bytes,gf_head_text,sizeof(gf_head_text)-1) &&
        gf_bytes_equal(diff.patch,patch,diff.patch.length) &&
        gf_bytes_equal(diff.name_status,changed,sizeof(changed)-1) &&
        gf_bytes_equal(ancestor,changed,sizeof(changed)-1);
    cbm_git_facts_free(facts);int cleanup=gfb_close(&f);
    ASSERT_EQ(cleanup,0);ASSERT_TRUE(configured);ASSERT_TRUE(legacy);ASSERT_TRUE(copied);
    ASSERT_TRUE(one);ASSERT_TRUE(two);ASSERT_TRUE(ordinary);ASSERT_TRUE(again);ASSERT_TRUE(stopped);
    ASSERT_TRUE(stable);PASS();
}

SUITE(test_impact_git) {
    RUN_TEST(test_git_facts_inventory_pins_complete_head_and_actual_merge_base);
    RUN_TEST(test_git_facts_inventory_preserves_literal_bytes_and_sha256);
    RUN_TEST(test_git_facts_inventory_empty_and_invalid_revision_clear_output);
    RUN_TEST(test_git_facts_inventory_cancellation_clears_prefix_and_latches);
    RUN_TEST(test_git_facts_inventory_output_limit_has_no_prefix_and_latches);
    RUN_TEST(test_git_facts_inherited_diff_options_cannot_override_zero_context);
    RUN_TEST(test_git_facts_hostile_probe_ignores_an_inherited_selection_file);
    RUN_TEST(test_git_facts_gitfile_redirection_cannot_certify_false_ancestry);
    RUN_TEST(test_git_facts_pins_actual_merge_base_and_both_refs);
    RUN_TEST(test_git_facts_ref_index_and_worktree_mutation_cannot_change_snapshot);
    RUN_TEST(test_git_facts_reads_exact_binary_literal_paths_and_all_revisions);
    RUN_TEST(test_git_facts_distinguishes_absent_invalid_and_nonregular_paths);
    RUN_TEST(test_git_facts_ancestry_distinguishes_no_from_invalid_and_missing_objects);
    RUN_TEST(test_git_facts_diff_uses_merge_base_zero_context_and_nul_metadata);
    RUN_TEST(test_git_facts_rejects_malformed_options_and_expected_head_mismatch);
    RUN_TEST(test_git_facts_precancel_and_expired_deadline_are_deterministic);
    RUN_TEST(test_git_facts_command_and_output_caps_fail_closed);
    RUN_TEST(test_git_facts_required_sha256_objects_and_identity);
    RUN_TEST(test_git_facts_never_runs_external_diff_or_textconv);
    RUN_TEST(test_git_facts_rejects_local_grafts_that_change_ancestry);
    RUN_TEST(test_git_facts_rejects_no_common_ancestor);
    RUN_TEST(test_git_facts_rejects_multiple_criss_cross_merge_bases);
    RUN_TEST(test_git_facts_rejects_local_shallow_state);
    RUN_TEST(test_git_facts_rejects_history_override_added_after_open);
    RUN_TEST(test_git_facts_cancellation_after_open_is_latched);
    RUN_TEST(test_git_ancestor_changes_requires_ancestor_of_actual_merge_base);
    RUN_TEST(test_git_ancestor_changes_preserves_earlier_paths_reversion_and_pinned_head);
    RUN_TEST(test_git_ancestor_changes_equalities_and_empty_reversion_are_distinct);
    RUN_TEST(test_git_ancestor_changes_validates_full_commit_object_and_argument_span);
    RUN_TEST(test_git_ancestor_changes_sha256_raw_paths_and_local_oid_copy);
    RUN_TEST(test_git_ancestor_changes_repeats_preserve_all_previous_owner_buffers);
    RUN_TEST(test_git_ancestor_changes_cancellation_is_shared_terminal_and_preserves_views);
    RUN_TEST(test_git_ancestor_changes_output_and_cumulative_limits_publish_no_prefix);
    RUN_TEST(test_git_ancestor_changes_repeated_calls_share_existing_command_budget);
    RUN_TEST(test_git_ancestor_changes_history_and_external_driver_guards_apply);
    RUN_TEST(test_git_blob_batch_native_bytes_and_both_object_formats);
    RUN_TEST(test_git_blob_batch_keeps_request_order_duplicates_and_raw_paths);
    RUN_TEST(test_git_blob_batch_pinned_revisions_and_cache_history_guards);
    RUN_TEST(test_git_blob_batch_validates_full_selection_and_empty_requests);
    RUN_TEST(test_git_blob_batch_exact_frame_caps_and_native_split_batches);
    RUN_TEST(test_git_blob_batch_per_call_and_existing_shared_budgets);
    RUN_TEST(test_git_blob_batch_strict_headers_and_native_missing_object);
    RUN_TEST(test_git_blob_batch_cancellation_is_sticky_and_owner_local);
    RUN_TEST(test_git_blob_batch_exact_payload_frames_and_no_prefix_acceptance);
    RUN_TEST(test_git_blob_batch_owner_lifetimes_and_legacy_views_survive);
}
