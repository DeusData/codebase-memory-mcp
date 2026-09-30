/*
 * test_workspace.c — workspace boundary policy.
 *
 * The policy is a pure function over an already-canonicalized path, so these are
 * table tests with no filesystem involved. home_dir and cache_dir are injected.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "foundation/workspace.h"
#include "foundation/compat_fs.h"
#include <stdio.h>
#include <string.h>

static const char *HOME = "/Users/dev";
static const char *CACHE = "/Users/dev/.cache/codebase-memory-mcp";

TEST(ws_depth_counts_components_below_the_volume) {
    ASSERT_EQ(cbm_workspace_path_depth("/"), 0);
    ASSERT_EQ(cbm_workspace_path_depth("/etc"), 1);
    ASSERT_EQ(cbm_workspace_path_depth("/etc/"), 1);
    ASSERT_EQ(cbm_workspace_path_depth("/Users/dev"), 2);
    ASSERT_EQ(cbm_workspace_path_depth("/Users//dev///x"), 3);
    /* macOS firmlinks: the /private prefix must not inflate depth, or "/etc"
     * resolves to "/private/etc" and passes a minimum of two. */
    ASSERT_EQ(cbm_workspace_path_depth("/private/etc"), 1);
    ASSERT_EQ(cbm_workspace_path_depth("/private/tmp/proj"), 2);
    ASSERT_EQ(cbm_workspace_path_depth("/private"), 0);
    /* Drive-relative, so an ordinary Windows workspace is one deep. */
    ASSERT_EQ(cbm_workspace_path_depth("C:/"), 0);
    ASSERT_EQ(cbm_workspace_path_depth("D:/repos"), 1);
    ASSERT_EQ(cbm_workspace_path_depth("D:\\repos\\app"), 2);
    /* A UNC share root is the share itself. */
    ASSERT_EQ(cbm_workspace_path_depth("//srv/share"), 0);
    ASSERT_EQ(cbm_workspace_path_depth("//srv/share/proj"), 1);
    PASS();
}

TEST(ws_volume_roots_are_absolutely_denied) {
    ASSERT_EQ(cbm_workspace_classify_root("/", HOME, CACHE), CBM_WS_DENY_ABSOLUTE);
    ASSERT_EQ(cbm_workspace_classify_root("C:/", HOME, CACHE), CBM_WS_DENY_ABSOLUTE);
    ASSERT_EQ(cbm_workspace_classify_root("C:\\", HOME, CACHE), CBM_WS_DENY_ABSOLUTE);
    ASSERT_EQ(cbm_workspace_classify_root("//srv/share", HOME, CACHE), CBM_WS_DENY_ABSOLUTE);
    /* "/private" carries no components of its own once the macOS firmlink prefix
     * is discounted, so it is a volume root rather than merely shallow. */
    ASSERT_EQ(cbm_workspace_classify_root("/private", HOME, CACHE), CBM_WS_DENY_ABSOLUTE);
    ASSERT_FALSE(cbm_workspace_verdict_is_overridable(CBM_WS_DENY_ABSOLUTE));
    PASS();
}

/* A relative or empty path is not a usable root and must not fall through as
 * allowed just because no rule matched it. */
TEST(ws_non_absolute_paths_are_denied) {
    ASSERT_EQ(cbm_workspace_classify_root("", HOME, CACHE), CBM_WS_DENY_ABSOLUTE);
    ASSERT_EQ(cbm_workspace_classify_root("relative/path", HOME, CACHE), CBM_WS_DENY_ABSOLUTE);
    ASSERT_EQ(cbm_workspace_classify_root(NULL, HOME, CACHE), CBM_WS_DENY_ABSOLUTE);
    PASS();
}

/* One depth rule refuses every POSIX top-level tree without a list to maintain.
 * This is the whole reason depth carries its weight. */
TEST(ws_posix_top_level_trees_are_too_shallow) {
    static const char *const shallow[] = {"/etc",         "/home",       "/Users", "/var",
                                          "/opt",         "/srv",        "/usr",   "/private/etc",
                                          "/private/var", "/private/tmp"};
    for (size_t i = 0; i < sizeof(shallow) / sizeof(shallow[0]); i++) {
        ASSERT_EQ(cbm_workspace_classify_root(shallow[i], HOME, CACHE), CBM_WS_DENY_TOO_SHALLOW);
    }
    ASSERT_FALSE(cbm_workspace_verdict_is_overridable(CBM_WS_DENY_TOO_SHALLOW));
    PASS();
}

/* Legitimately shallow project roots must survive: these are the false positives
 * a blanket depth rule would cause, which is why Windows counts drive-relative. */
TEST(ws_legitimate_shallow_roots_are_allowed) {
    ASSERT_EQ(cbm_workspace_classify_root("/opt/sdk", HOME, CACHE), CBM_WS_ALLOW);
    ASSERT_EQ(cbm_workspace_classify_root("/srv/protos", HOME, CACHE), CBM_WS_ALLOW);
    ASSERT_EQ(cbm_workspace_classify_root("D:/repos", HOME, CACHE), CBM_WS_ALLOW);
    ASSERT_EQ(cbm_workspace_classify_root("//srv/share/proj", HOME, CACHE), CBM_WS_ALLOW);
    ASSERT_EQ(cbm_workspace_classify_root("/Users/dev/dev/app", HOME, CACHE), CBM_WS_ALLOW);
    PASS();
}

/* $HOME is depth 2 on both macOS and Linux, so depth cannot see it. */
TEST(ws_home_itself_is_sensitive_but_subdirs_are_fine) {
    ASSERT_EQ(cbm_workspace_classify_root("/Users/dev", HOME, CACHE), CBM_WS_DENY_SENSITIVE);
    ASSERT_EQ(cbm_workspace_classify_root("/Users/dev/", HOME, CACHE), CBM_WS_DENY_SENSITIVE);
    ASSERT_EQ(cbm_workspace_classify_root("/Users/dev/projects", HOME, CACHE), CBM_WS_ALLOW);
    /* A sibling that merely shares a prefix is not the home directory. */
    ASSERT_EQ(cbm_workspace_classify_root("/Users/developer", HOME, CACHE), CBM_WS_ALLOW);
    ASSERT_TRUE(cbm_workspace_verdict_is_overridable(CBM_WS_DENY_SENSITIVE));
    PASS();
}

/* The enumeration bypass: a credential directory passes every breadth rule, so
 * it has to be named. Matched on any component, so subdirectories go too. */
TEST(ws_credential_directories_are_sensitive_at_any_depth) {
    ASSERT_EQ(cbm_workspace_classify_root("/Users/dev/.ssh", HOME, CACHE), CBM_WS_DENY_SENSITIVE);
    ASSERT_EQ(cbm_workspace_classify_root("/Users/dev/.ssh/keys", HOME, CACHE),
              CBM_WS_DENY_SENSITIVE);
    ASSERT_EQ(cbm_workspace_classify_root("/Users/dev/.aws", HOME, CACHE), CBM_WS_DENY_SENSITIVE);
    ASSERT_EQ(cbm_workspace_classify_root("/Users/dev/.gnupg", HOME, CACHE), CBM_WS_DENY_SENSITIVE);
    ASSERT_EQ(cbm_workspace_classify_root("/Users/dev/.kube", HOME, CACHE), CBM_WS_DENY_SENSITIVE);
    ASSERT_EQ(cbm_workspace_classify_root("/Users/dev/Library/Keychains", HOME, CACHE),
              CBM_WS_DENY_SENSITIVE);
    /* A name that merely contains a listed one is a different directory. */
    ASSERT_EQ(cbm_workspace_classify_root("/Users/dev/.sshconfig", HOME, CACHE), CBM_WS_ALLOW);
    PASS();
}

TEST(ws_windows_system_trees_are_sensitive) {
    ASSERT_EQ(cbm_workspace_classify_root("C:/Windows", HOME, CACHE), CBM_WS_DENY_SENSITIVE);
    ASSERT_EQ(cbm_workspace_classify_root("C:/Users", HOME, CACHE), CBM_WS_DENY_SENSITIVE);
    /* THE regression that matters on Windows: every user's work lives under
     * C:\Users\<name>, so matching "Users" against every component would refuse
     * every ordinary project path — including a CI runner's own workspace. Only
     * the tree root is refused. */
    ASSERT_EQ(cbm_workspace_classify_root("C:/Users/dev/projects/app", HOME, CACHE), CBM_WS_ALLOW);
    ASSERT_EQ(cbm_workspace_classify_root("C:/Users/runneradmin/work/repo", HOME, CACHE),
              CBM_WS_ALLOW);
    /* System trees are refused at any depth inside them, not just at the root. */
    ASSERT_EQ(cbm_workspace_classify_root("C:/Windows/System32", HOME, CACHE),
              CBM_WS_DENY_SENSITIVE);
    /* A project merely named after one is not one. */
    ASSERT_EQ(cbm_workspace_classify_root("C:/dev/Windows-app", HOME, CACHE), CBM_WS_ALLOW);
    ASSERT_EQ(cbm_workspace_classify_root("C:/ProgramData", HOME, CACHE), CBM_WS_DENY_SENSITIVE);
    ASSERT_EQ(cbm_workspace_classify_root("C:/Program Files/app", HOME, CACHE),
              CBM_WS_DENY_SENSITIVE);
    /* Case-insensitive: NTFS is, so a case-flipped name must not slip past. */
    ASSERT_EQ(cbm_workspace_classify_root("C:/WINDOWS", HOME, CACHE), CBM_WS_DENY_SENSITIVE);
    ASSERT_EQ(cbm_workspace_classify_root("C:/users/dev/.SSH", HOME, CACHE), CBM_WS_DENY_SENSITIVE);
    PASS();
}

TEST(ws_windows_user_programs_tree_has_exact_sensitive_boundaries) {
    static const char *const sensitive[] = {
        "C:/Users/dev/AppData/Local/Programs",
        "C:/Users/dev/AppData/Local/Programs/Antigravity",
        "D:/Users/runneradmin/AppData/Local/Programs/Editor",
        "c:\\uSeRs\\Dev\\aPpDaTa\\LoCaL\\pRoGrAmS\\IDE",
    };
    for (size_t i = 0; i < sizeof(sensitive) / sizeof(sensitive[0]); i++) {
        ASSERT_EQ(cbm_workspace_classify_root(sensitive[i], HOME, CACHE),
                  CBM_WS_DENY_SENSITIVE);
    }

    static const char *const allowed[] = {
        "C:/Users/dev/projects/app",
        "C:/Users/dev/AppData/Local",
        "C:/Users/dev/AppData/Local/Programs-src",
        "C:/Users/dev/AppData/Local/ProgramsBackup",
        "C:/workspace/Users/dev/AppData/Local/Programs",
        "C:/Users/dev/team/AppData/Local/Programs",
    };
    for (size_t i = 0; i < sizeof(allowed) / sizeof(allowed[0]); i++) {
        ASSERT_EQ(cbm_workspace_classify_root(allowed[i], HOME, CACHE), CBM_WS_ALLOW);
    }
    PASS();
}

TEST(ws_sensitive_root_explicit_approval_is_preserved) {
    char root[256];
    char *created = th_mktempdir("cbm_ws_403_home");
    ASSERT_NOT_NULL(created);
    snprintf(root, sizeof(root), "%s", created);

    char cache[256];
    created = th_mktempdir("cbm_ws_403");
    ASSERT_NOT_NULL(created);
    snprintf(cache, sizeof(cache), "%s", created);

    char err[1024];
    ASSERT_FALSE(cbm_workspace_root_allowed(root, root, cache, NULL, err, sizeof(err)));
    ASSERT_NOT_NULL(strstr(err, "--approve-sensitive"));
    ASSERT_TRUE(cbm_workspace_grant_add(cache, root, root, true, err, sizeof(err)));
    ASSERT_TRUE(cbm_workspace_root_allowed(root, root, cache, NULL, err, sizeof(err)));

    th_cleanup(root);
    th_cleanup(cache);
    PASS();
}

TEST(ws_sensitive_approval_upgrades_existing_ordinary_exact_grant) {
    char root[256];
    char *created = th_mktempdir("cbm_ws_403_exact");
    ASSERT_NOT_NULL(created);
    snprintf(root, sizeof(root), "%s", created);

    char cache[256];
    created = th_mktempdir("cbm_ws_403_exact_cache");
    ASSERT_NOT_NULL(created);
    snprintf(cache, sizeof(cache), "%s", created);

    char err[1024];
    ASSERT_TRUE(cbm_workspace_grant_add(cache, NULL, root, false, err, sizeof(err)));
    ASSERT_FALSE(cbm_workspace_root_allowed(root, root, cache, NULL, err, sizeof(err)));

    ASSERT_TRUE(cbm_workspace_grant_add(cache, root, root, true, err, sizeof(err)));
    ASSERT_TRUE(cbm_workspace_root_allowed(root, root, cache, NULL, err, sizeof(err)));
    /* A repeated explicit approval must recognize the marked exact grant rather
     * than append another exception. */
    ASSERT_TRUE(cbm_workspace_grant_add(cache, root, root, true, err, sizeof(err)));

    char listed[4096];
    char expected[4096];
    ASSERT_TRUE(cbm_workspace_grant_list(cache, listed, sizeof(listed)));
    snprintf(expected, sizeof(expected), "%s\n(approved) %s\n", root, root);
    ASSERT_STR_EQ(listed, expected);

    th_cleanup(root);
    th_cleanup(cache);
    PASS();
}

TEST(ws_sensitive_approval_adds_exact_exception_under_ordinary_ancestor) {
    char ancestor[256];
    char *created = th_mktempdir("cbm_ws_403_ancestor");
    ASSERT_NOT_NULL(created);
    snprintf(ancestor, sizeof(ancestor), "%s", created);

    char cache[256];
    created = th_mktempdir("cbm_ws_403_ancestor_cache");
    ASSERT_NOT_NULL(created);
    snprintf(cache, sizeof(cache), "%s", created);

    char sensitive[1024];
    snprintf(sensitive, sizeof(sensitive), "%s/private-project", ancestor);
    ASSERT_EQ(cbm_mkdir(sensitive), 0);
    char err[1024];
    ASSERT_TRUE(cbm_workspace_grant_add(cache, NULL, ancestor, false, err, sizeof(err)));
    ASSERT_FALSE(cbm_workspace_root_allowed(sensitive, sensitive, cache, NULL, err, sizeof(err)));

    ASSERT_TRUE(cbm_workspace_grant_add(cache, sensitive, sensitive, true, err, sizeof(err)));
    ASSERT_TRUE(
        cbm_workspace_root_allowed(sensitive, sensitive, cache, NULL, err, sizeof(err)));
    ASSERT_TRUE(cbm_workspace_grant_add(cache, sensitive, sensitive, true, err, sizeof(err)));

    char listed[4096];
    char expected[4096];
    ASSERT_TRUE(cbm_workspace_grant_list(cache, listed, sizeof(listed)));
    snprintf(expected, sizeof(expected), "%s\n(approved) %s\n", ancestor, sensitive);
    ASSERT_STR_EQ(listed, expected);

    th_cleanup(ancestor);
    th_cleanup(cache);
    PASS();
}

/* POSIX is case-sensitive, so a differently-cased directory is a different one
 * and must not be refused. */
TEST(ws_posix_matching_is_case_sensitive) {
    ASSERT_EQ(cbm_workspace_classify_root("/Users/dev/.SSH", HOME, CACHE), CBM_WS_ALLOW);
    PASS();
}

/* Absent injected context, the checks that depend on it simply do not fire —
 * they must not crash or deny everything. */
TEST(ws_null_context_disables_dependent_checks) {
    ASSERT_EQ(cbm_workspace_classify_root("/Users/dev", NULL, NULL), CBM_WS_ALLOW);
    /* Depth and volume rules are context-free and still apply. */
    ASSERT_EQ(cbm_workspace_classify_root("/etc", NULL, NULL), CBM_WS_DENY_TOO_SHALLOW);
    ASSERT_EQ(cbm_workspace_classify_root("/", NULL, NULL), CBM_WS_DENY_ABSOLUTE);
    PASS();
}

TEST(ws_every_verdict_has_a_reason) {
    ASSERT_NOT_NULL(cbm_workspace_verdict_reason(CBM_WS_ALLOW));
    ASSERT_NOT_NULL(cbm_workspace_verdict_reason(CBM_WS_DENY_TOO_SHALLOW));
    ASSERT_NOT_NULL(cbm_workspace_verdict_reason(CBM_WS_DENY_ABSOLUTE));
    ASSERT_NOT_NULL(cbm_workspace_verdict_reason(CBM_WS_DENY_SENSITIVE));
    /* Reasons are user-facing; a bare enum name would not help anyone. */
    ASSERT_TRUE(strlen(cbm_workspace_verdict_reason(CBM_WS_DENY_TOO_SHALLOW)) > 20);
    PASS();
}

/* ── Per-project request manifest ───────────────────────────────────────── */

static void ws_write(const char *path, const char *body) {
    FILE *f = cbm_fopen(path, "wb");
    if (f) {
        (void)fputs(body, f);
        (void)fclose(f);
    }
}

TEST(ws_manifest_absent_is_not_an_error) {
    char *base = th_mktempdir("cbm_ws_m0");
    ASSERT(base != NULL);
    cbm_ws_manifest_t m;
    ASSERT_TRUE(cbm_workspace_manifest_read(base, &m));
    ASSERT_FALSE(m.present);
    ASSERT_EQ(m.count, 0);
    th_cleanup(base);
    PASS();
}

TEST(ws_manifest_parses_entries_and_skips_comments) {
    char *base = th_mktempdir("cbm_ws_m1");
    ASSERT(base != NULL);
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s", base, CBM_WS_MANIFEST_NAME);
    ws_write(path, "# a comment\n/opt/sdk\n\n/srv/protos\n");
    cbm_ws_manifest_t m;
    ASSERT_TRUE(cbm_workspace_manifest_read(base, &m));
    ASSERT_TRUE(m.present);
    ASSERT_EQ(m.count, 2);
    ASSERT_STR_EQ(m.entries[0], "/opt/sdk");
    ASSERT_STR_EQ(m.entries[1], "/srv/protos");
    ASSERT_EQ((int)strlen(m.digest), 64);
    th_cleanup(base);
    PASS();
}

/* A control character is how a crafted entry would smuggle a second value past a
 * line reader — the same shape as the newline splitting in the scoped file list. */
TEST(ws_manifest_rejects_control_characters) {
    char *base = th_mktempdir("cbm_ws_m2");
    ASSERT(base != NULL);
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s", base, CBM_WS_MANIFEST_NAME);
    ws_write(path, "/opt/sdk\t\x01evil\n");
    cbm_ws_manifest_t m;
    ASSERT_FALSE(cbm_workspace_manifest_read(base, &m));
    ASSERT_EQ(m.count, 0);
    th_cleanup(base);
    PASS();
}

/* THE property the design rests on: a manifest grants nothing until a person
 * approves it, and editing it lapses that approval rather than inheriting it. */
TEST(ws_manifest_approval_is_keyed_to_content) {
    char *base = th_mktempdir("cbm_ws_m3");
    char *cache = th_mktempdir("cbm_ws_m3c");
    ASSERT(base != NULL);
    ASSERT(cache != NULL);
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s", base, CBM_WS_MANIFEST_NAME);
    ws_write(path, "/opt/sdk\n");

    cbm_ws_manifest_t before;
    ASSERT_TRUE(cbm_workspace_manifest_read(base, &before));
    /* Unapproved grants nothing. */
    ASSERT_FALSE(cbm_workspace_manifest_is_approved(cache, base, &before));

    char err[1024];
    ASSERT_TRUE(cbm_workspace_manifest_approve(cache, HOME, base, err, sizeof(err)));
    ASSERT_TRUE(cbm_workspace_manifest_is_approved(cache, base, &before));

    /* Widen the requests, as a `git pull` would. Approval must lapse. */
    ws_write(path, "/opt/sdk\n/srv/protos\n");
    cbm_ws_manifest_t after;
    ASSERT_TRUE(cbm_workspace_manifest_read(base, &after));
    ASSERT_TRUE(strcmp(before.digest, after.digest) != 0);
    ASSERT_FALSE(cbm_workspace_manifest_is_approved(cache, base, &after));

    th_cleanup(base);
    th_cleanup(cache);
    PASS();
}

/* Approving a manifest must not become a route around the breadth policy. */
TEST(ws_manifest_approval_refuses_overbroad_requests) {
    char *base = th_mktempdir("cbm_ws_m4");
    char *cache = th_mktempdir("cbm_ws_m4c");
    ASSERT(base != NULL);
    ASSERT(cache != NULL);
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s", base, CBM_WS_MANIFEST_NAME);
    ws_write(path, "/etc\n");
    char err[1024];
    ASSERT_FALSE(cbm_workspace_manifest_approve(cache, HOME, base, err, sizeof(err)));
    ASSERT_TRUE(strstr(err, "too broad") != NULL);
    th_cleanup(base);
    th_cleanup(cache);
    PASS();
}

SUITE(workspace) {
    RUN_TEST(ws_manifest_absent_is_not_an_error);
    RUN_TEST(ws_manifest_parses_entries_and_skips_comments);
    RUN_TEST(ws_manifest_rejects_control_characters);
    RUN_TEST(ws_manifest_approval_is_keyed_to_content);
    RUN_TEST(ws_manifest_approval_refuses_overbroad_requests);
    RUN_TEST(ws_depth_counts_components_below_the_volume);
    RUN_TEST(ws_volume_roots_are_absolutely_denied);
    RUN_TEST(ws_non_absolute_paths_are_denied);
    RUN_TEST(ws_posix_top_level_trees_are_too_shallow);
    RUN_TEST(ws_legitimate_shallow_roots_are_allowed);
    RUN_TEST(ws_home_itself_is_sensitive_but_subdirs_are_fine);
    RUN_TEST(ws_credential_directories_are_sensitive_at_any_depth);
    RUN_TEST(ws_windows_system_trees_are_sensitive);
    RUN_TEST(ws_windows_user_programs_tree_has_exact_sensitive_boundaries);
    RUN_TEST(ws_sensitive_root_explicit_approval_is_preserved);
    RUN_TEST(ws_sensitive_approval_upgrades_existing_ordinary_exact_grant);
    RUN_TEST(ws_sensitive_approval_adds_exact_exception_under_ordinary_ancestor);
    RUN_TEST(ws_posix_matching_is_case_sensitive);
    RUN_TEST(ws_null_context_disables_dependent_checks);
    RUN_TEST(ws_every_verdict_has_a_reason);
}
