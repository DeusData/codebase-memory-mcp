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
#include "foundation/platform.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#endif

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
    /* th_mktempdir returns one static buffer: copy each result at once. */
    char base[256];
    char cache[256];
    char *created = th_mktempdir("cbm_ws_m3");
    ASSERT(created != NULL);
    snprintf(base, sizeof(base), "%s", created);
    created = th_mktempdir("cbm_ws_m3c");
    ASSERT(created != NULL);
    snprintf(cache, sizeof(cache), "%s", created);

    /* Entries must name existing directories. */
    char sdk[1024];
    char protos[1024];
    snprintf(sdk, sizeof(sdk), "%s/sdk", base);
    snprintf(protos, sizeof(protos), "%s/protos", base);
    ASSERT_EQ(cbm_mkdir(sdk), 0);
    ASSERT_EQ(cbm_mkdir(protos), 0);

    char path[1024];
    char body[4096];
    snprintf(path, sizeof(path), "%s/%s", base, CBM_WS_MANIFEST_NAME);
    snprintf(body, sizeof(body), "%s\n", sdk);
    ws_write(path, body);

    cbm_ws_manifest_t before;
    ASSERT_TRUE(cbm_workspace_manifest_read(base, &before));
    /* Unapproved grants nothing. */
    ASSERT_FALSE(cbm_workspace_manifest_is_approved(cache, base, &before));

    char err[1024];
    ASSERT_TRUE(cbm_workspace_manifest_approve(cache, HOME, base, err, sizeof(err)));
    ASSERT_TRUE(cbm_workspace_manifest_is_approved(cache, base, &before));

    /* Widen the requests, as a `git pull` would. Approval must lapse. */
    snprintf(body, sizeof(body), "%s\n%s\n", sdk, protos);
    ws_write(path, body);
    cbm_ws_manifest_t after;
    ASSERT_TRUE(cbm_workspace_manifest_read(base, &after));
    ASSERT_TRUE(strcmp(before.digest, after.digest) != 0);
    ASSERT_FALSE(cbm_workspace_manifest_is_approved(cache, base, &after));

    th_cleanup(base);
    th_cleanup(cache);
    PASS();
}

/* Approving a manifest must not become a route around the breadth policy. The
 * entry has to exist, so the overbroad directory is "/etc" on POSIX and, on
 * Windows, where one component below the drive is already an ordinary
 * workspace, the drive root of the temporary directory. */
TEST(ws_manifest_approval_refuses_overbroad_requests) {
    char base[256];
    char cache[256];
    char *created = th_mktempdir("cbm_ws_m4");
    ASSERT(created != NULL);
    snprintf(base, sizeof(base), "%s", created);
    created = th_mktempdir("cbm_ws_m4c");
    ASSERT(created != NULL);
    snprintf(cache, sizeof(cache), "%s", created);

    char path[1024];
    char body[1024];
    const char *refusal = NULL;
    snprintf(path, sizeof(path), "%s/%s", base, CBM_WS_MANIFEST_NAME);
#ifdef _WIN32
    snprintf(body, sizeof(body), "%.3s\n", base);
    refusal = cbm_workspace_verdict_reason(CBM_WS_DENY_ABSOLUTE);
#else
    snprintf(body, sizeof(body), "/etc\n");
    refusal = cbm_workspace_verdict_reason(CBM_WS_DENY_TOO_SHALLOW);
#endif
    ws_write(path, body);
    char err[1024];
    ASSERT_FALSE(cbm_workspace_manifest_approve(cache, HOME, base, err, sizeof(err)));
    ASSERT_TRUE(strstr(err, refusal) != NULL);
    th_cleanup(base);
    th_cleanup(cache);
    PASS();
}

/* ── Environment helpers ────────────────────────────────────────────────── */

static char *ws_env_save(const char *name) {
    const char *value = getenv(name);
    return value ? strdup(value) : NULL;
}

static void ws_env_restore(const char *name, char *saved) {
    if (saved) {
        (void)cbm_setenv(name, saved, 1);
        free(saved);
    } else {
        (void)cbm_unsetenv(name);
    }
}

/* The policy compares the home directory against canonical paths, so the
 * helper must hand out the canonical form. A plain temp directory already
 * shows the difference: on macOS /tmp is a firmlink to /private/tmp, and on
 * Windows the temp path may carry a short (8.3) component. */
TEST(ws_home_dir_is_resolved) {
    char *created = th_mktempdir("cbm_ws_home");
    ASSERT_NOT_NULL(created);
    char real[256];
    snprintf(real, sizeof(real), "%s", created);
    char expected[4096];
    ASSERT_TRUE(cbm_canonical_path(real, expected, sizeof(expected)));
    cbm_normalize_path_sep(expected);

    char *saved_home = ws_env_save("HOME");
    ASSERT_EQ(cbm_setenv("HOME", real, 1), 0);
    const char *home = cbm_workspace_home_dir();
    bool resolved = home && strcmp(home, expected) == 0;
    ws_env_restore("HOME", saved_home);
    th_cleanup(real);

    ASSERT_TRUE(resolved);
    PASS();
}

/* A home directory reached through a link — "/home" kept on another volume,
 * an account whose HOME is itself a link — is still the home directory when
 * a caller presents its resolved path, and must be refused as such. */
TEST(ws_linked_home_classified_as_home) {
#ifdef _WIN32
    /* symlink() does not exist on Windows, and creating a symbolic link there
     * needs a privilege an ordinary account (and the CI runner) does not hold.
     * A directory junction (cmd.exe mklink /J) is the Windows shape of the
     * same case and would need a cmd.exe fixture; the resolution the case
     * depends on is covered on Windows by ws_home_dir_is_resolved, and the
     * link itself is exercised on POSIX. */
    SKIP_PLATFORM("Windows: symlink() unavailable; links need a privilege");
#else
    char *created = th_mktempdir("cbm_ws_linked_home");
    ASSERT_NOT_NULL(created);
    char base[256];
    snprintf(base, sizeof(base), "%s", created);
    char real[512];
    char link[512];
    snprintf(real, sizeof(real), "%s/real", base);
    snprintf(link, sizeof(link), "%s/link", base);
    ASSERT_EQ(cbm_mkdir(real), 0);
    ASSERT_EQ(symlink(real, link), 0);

    char canonical_real[4096];
    ASSERT_TRUE(cbm_canonical_path(real, canonical_real, sizeof(canonical_real)));
    /* The fixture proves something only when the two spellings differ. */
    ASSERT_TRUE(strcmp(link, canonical_real) != 0);

    char *saved_home = ws_env_save("HOME");
    ASSERT_EQ(cbm_setenv("HOME", link, 1), 0);
    const char *home = cbm_workspace_home_dir();
    bool resolved = home && strcmp(home, canonical_real) == 0;
    cbm_ws_verdict_t verdict = cbm_workspace_classify_root(canonical_real, home, NULL);
    ws_env_restore("HOME", saved_home);
    th_cleanup(base);

    ASSERT_TRUE(resolved);
    ASSERT_EQ(verdict, CBM_WS_DENY_SENSITIVE);
    PASS();
#endif
}

/* ── Manifest entries are judged in their resolved form ──────────────────── */

enum { WS_T_PATH = 4096 };

/* th_mktempdir hands back one static buffer, so copy at once — and in canonical
 * form, because the policy compares resolved spellings and /tmp is a link on
 * macOS. */
static bool ws_tempdir_canonical(const char *prefix, char *out, size_t out_sz) {
    char *created = th_mktempdir(prefix);
    return created && cbm_canonical_path(created, out, out_sz) == 1;
}

static bool ws_join(char *out, size_t out_sz, const char *a, const char *b) {
    int n = snprintf(out, out_sz, "%s/%s", a, b);
    return n > 0 && (size_t)n < out_sz;
}

/* Create <parent>/<name> and return it in canonical form. */
static bool ws_mkdir_canonical(const char *parent, const char *name, char *out, size_t out_sz) {
    char made[WS_T_PATH];
    if (!ws_join(made, sizeof(made), parent, name) || cbm_mkdir(made) != 0) {
        return false;
    }
    return cbm_canonical_path(made, out, out_sz) == 1;
}

static void ws_manifest_write(const char *base, const char *body) {
    char path[WS_T_PATH];
    if (ws_join(path, sizeof(path), base, CBM_WS_MANIFEST_NAME)) {
        ws_write(path, body);
    }
}

static void ws_manifest_write_line(const char *base, const char *entry) {
    char body[WS_T_PATH];
    int n = snprintf(body, sizeof(body), "%s\n", entry);
    if (n > 0 && (size_t)n < sizeof(body)) {
        ws_manifest_write(base, body);
    }
}

/* Put the manifest on record as approved by writing the store line directly,
 * in the form cbm_workspace_manifest_approve writes ("<digest> <project>").
 * This is the record an approval leaves behind when the entry was judged by
 * its spelling, and the use-time check must stand on its own against it. */
static bool ws_manifest_record_approval(const char *cache, const char *base) {
    cbm_ws_manifest_t m;
    if (!cbm_workspace_manifest_read(base, &m) || !m.present) {
        return false;
    }
    char store[WS_T_PATH];
    char line[WS_T_PATH];
    if (!ws_join(store, sizeof(store), cache, "approved_manifests")) {
        return false;
    }
    int n = snprintf(line, sizeof(line), "%s %s\n", m.digest, base);
    if (n <= 0 || (size_t)n >= sizeof(line)) {
        return false;
    }
    ws_write(store, line);
    return cbm_workspace_manifest_is_approved(cache, base, &m);
}

/* The directory a manifest entry names is the one that would be indexed, so
 * that is the form the policy judges — at approval and again at use. Spelled
 * through `..`, an entry reads as an ordinary path several components deep
 * while naming the home directory or the volume root. */
TEST(ws_manifest_entry_classified_in_resolved_form) {
    char base[WS_T_PATH];
    char cache[WS_T_PATH];
    ASSERT_TRUE(ws_tempdir_canonical("cbm_ws_m5", base, sizeof(base)));
    ASSERT_TRUE(ws_tempdir_canonical("cbm_ws_m5c", cache, sizeof(cache)));

    char home[WS_T_PATH];
    char src[WS_T_PATH];
    char home_sub[WS_T_PATH];
    ASSERT_TRUE(ws_mkdir_canonical(base, "home", home, sizeof(home)));
    ASSERT_TRUE(ws_mkdir_canonical(base, "src", src, sizeof(src)));
    ASSERT_TRUE(ws_mkdir_canonical(home, "notes", home_sub, sizeof(home_sub)));

    char entry[WS_T_PATH];
    char expected[WS_T_PATH];
    char err[1024];

    /* "<base>/src/../home" is the home directory. */
    ASSERT_TRUE(ws_join(entry, sizeof(entry), src, "../home"));
    ws_manifest_write_line(base, entry);
    ASSERT_FALSE(cbm_workspace_manifest_approve(cache, home, base, err, sizeof(err)));
    ASSERT_NOT_NULL(strstr(err, entry));
    ASSERT(snprintf(expected, sizeof(expected), "(resolves to %s)", home) > 0);
    ASSERT_NOT_NULL(strstr(err, expected));
    ASSERT_NOT_NULL(strstr(err, cbm_workspace_verdict_reason(CBM_WS_DENY_SENSITIVE)));

    /* An approval already on record for this spelling does not outrank the
     * use-time check. */
    ASSERT_TRUE(ws_manifest_record_approval(cache, base));
    ASSERT_FALSE(cbm_workspace_manifest_allows(cache, home, base, home_sub));

    /* Enough `..` segments reach the volume root from anywhere. */
    ASSERT_TRUE(ws_join(entry, sizeof(entry), base, "../../../../../../../../../../../.."));
    ws_manifest_write_line(base, entry);
    ASSERT_FALSE(cbm_workspace_manifest_approve(cache, home, base, err, sizeof(err)));
    ASSERT_NOT_NULL(strstr(err, entry));
    ASSERT_NOT_NULL(strstr(err, cbm_workspace_verdict_reason(CBM_WS_DENY_ABSOLUTE)));

    th_cleanup(base);
    th_cleanup(cache);
    PASS();
}

/* An entry that does not resolve to a directory is refused at approval, naming
 * the entry; at use it simply matches nothing, and the other entries of the
 * same manifest keep working. */
TEST(ws_manifest_unresolvable_entry_refused) {
    char base[WS_T_PATH];
    char cache[WS_T_PATH];
    ASSERT_TRUE(ws_tempdir_canonical("cbm_ws_m6", base, sizeof(base)));
    ASSERT_TRUE(ws_tempdir_canonical("cbm_ws_m6c", cache, sizeof(cache)));

    char entry[WS_T_PATH];
    char err[1024];

    /* Missing. */
    ASSERT_TRUE(ws_join(entry, sizeof(entry), base, "missing"));
    ws_manifest_write_line(base, entry);
    ASSERT_FALSE(cbm_workspace_manifest_approve(cache, HOME, base, err, sizeof(err)));
    ASSERT_NOT_NULL(strstr(err, entry));
    ASSERT_NOT_NULL(strstr(err, "must name an existing directory"));

    /* Present, but a file. */
    ASSERT_TRUE(ws_join(entry, sizeof(entry), base, "notes.txt"));
    ws_write(entry, "not a directory\n");
    ws_manifest_write_line(base, entry);
    ASSERT_FALSE(cbm_workspace_manifest_approve(cache, HOME, base, err, sizeof(err)));
    ASSERT_NOT_NULL(strstr(err, entry));
    ASSERT_NOT_NULL(strstr(err, "is not a directory"));

    /* Relative: the file is read by a long-lived process whose working
     * directory is unrelated to the project, so there is no base to resolve
     * it against. */
    ws_manifest_write_line(base, "src");
    ASSERT_FALSE(cbm_workspace_manifest_approve(cache, HOME, base, err, sizeof(err)));
    ASSERT_NOT_NULL(strstr(err, "requested path src:"));
    ASSERT_NOT_NULL(strstr(err, "must be an absolute path"));

    /* At use: a directory that disappeared after approval matches nothing and
     * does not take the rest of the manifest with it. */
    char present[WS_T_PATH];
    char present_sub[WS_T_PATH];
    char body[WS_T_PATH];
    ASSERT_TRUE(ws_mkdir_canonical(base, "present", present, sizeof(present)));
    ASSERT_TRUE(ws_mkdir_canonical(present, "inc", present_sub, sizeof(present_sub)));
    ASSERT_TRUE(ws_join(entry, sizeof(entry), base, "gone"));
    ASSERT(snprintf(body, sizeof(body), "%s\n%s\n", entry, present) > 0);
    ws_manifest_write(base, body);
    ASSERT_TRUE(ws_manifest_record_approval(cache, base));
    ASSERT_TRUE(cbm_workspace_manifest_allows(cache, HOME, base, present_sub));
    ASSERT_FALSE(cbm_workspace_manifest_allows(cache, HOME, base, base));

    th_cleanup(base);
    th_cleanup(cache);
    PASS();
}

/* The ordinary shapes keep their result: an absolute existing directory, an
 * entry equal to the project root, an entry outside every root in the grant
 * store (the manifest is consulted on its own), and on POSIX a link to an
 * ordinary directory. */
TEST(ws_manifest_ordinary_entries_unchanged) {
    char base[WS_T_PATH];
    char cache[WS_T_PATH];
    ASSERT_TRUE(ws_tempdir_canonical("cbm_ws_m7", base, sizeof(base)));
    ASSERT_TRUE(ws_tempdir_canonical("cbm_ws_m7c", cache, sizeof(cache)));

    char sdk[WS_T_PATH];
    char sdk_inc[WS_T_PATH];
    char granted[WS_T_PATH];
    ASSERT_TRUE(ws_mkdir_canonical(base, "sdk", sdk, sizeof(sdk)));
    ASSERT_TRUE(ws_mkdir_canonical(sdk, "inc", sdk_inc, sizeof(sdk_inc)));
    ASSERT_TRUE(ws_mkdir_canonical(base, "granted", granted, sizeof(granted)));

    char err[1024];
    /* The grant store names a root the sdk is not under. */
    ASSERT_TRUE(cbm_workspace_grant_add(cache, HOME, granted, false, err, sizeof(err)));

    ws_manifest_write_line(base, sdk);
    ASSERT_TRUE(cbm_workspace_manifest_approve(cache, HOME, base, err, sizeof(err)));
    ASSERT_STR_EQ(err, "");
    ASSERT_TRUE(cbm_workspace_manifest_allows(cache, HOME, base, sdk_inc));
    ASSERT_FALSE(cbm_workspace_manifest_allows(cache, HOME, base, granted));

    /* Equal to the project root. */
    ws_manifest_write_line(base, base);
    ASSERT_TRUE(cbm_workspace_manifest_approve(cache, HOME, base, err, sizeof(err)));
    ASSERT_TRUE(cbm_workspace_manifest_allows(cache, HOME, base, base));
    ASSERT_TRUE(cbm_workspace_manifest_allows(cache, HOME, base, sdk_inc));

#ifndef _WIN32
    /* A link to an ordinary directory is that directory. Windows is not
     * covered here: symlink(2) has no counterpart in the test helpers, and
     * CreateSymbolicLinkW needs a privilege the CI runner does not hold; the
     * resolved-form rules themselves are exercised on every platform by the
     * `..` cases above. */
    char alias[WS_T_PATH];
    ASSERT_TRUE(ws_join(alias, sizeof(alias), base, "alias"));
    ASSERT_EQ(symlink(sdk, alias), 0);
    ws_manifest_write_line(base, alias);
    ASSERT_TRUE(cbm_workspace_manifest_approve(cache, HOME, base, err, sizeof(err)));
    ASSERT_TRUE(cbm_workspace_manifest_allows(cache, HOME, base, sdk_inc));
#endif

    th_cleanup(base);
    th_cleanup(cache);
    PASS();
}

#ifndef _WIN32
/* A link is judged by where it leads. Windows is not covered: symlink(2) has
 * no counterpart in the test helpers and CreateSymbolicLinkW needs a privilege
 * the CI runner does not hold; the resolved-form rules are exercised on every
 * platform by ws_manifest_entry_classified_in_resolved_form. */
TEST(ws_manifest_link_to_sensitive_directory_refused) {
    char base[WS_T_PATH];
    char cache[WS_T_PATH];
    ASSERT_TRUE(ws_tempdir_canonical("cbm_ws_m8", base, sizeof(base)));
    ASSERT_TRUE(ws_tempdir_canonical("cbm_ws_m8c", cache, sizeof(cache)));

    char home[WS_T_PATH];
    char home_sub[WS_T_PATH];
    char keys[WS_T_PATH];
    char keys_sub[WS_T_PATH];
    ASSERT_TRUE(ws_mkdir_canonical(base, "home", home, sizeof(home)));
    ASSERT_TRUE(ws_mkdir_canonical(home, "notes", home_sub, sizeof(home_sub)));
    ASSERT_TRUE(ws_mkdir_canonical(base, ".ssh", keys, sizeof(keys)));
    ASSERT_TRUE(ws_mkdir_canonical(keys, "hosts", keys_sub, sizeof(keys_sub)));

    char to_home[WS_T_PATH];
    char to_keys[WS_T_PATH];
    ASSERT_TRUE(ws_join(to_home, sizeof(to_home), base, "to-home"));
    ASSERT_TRUE(ws_join(to_keys, sizeof(to_keys), base, "to-keys"));
    ASSERT_EQ(symlink(home, to_home), 0);
    ASSERT_EQ(symlink(keys, to_keys), 0);

    char expected[WS_T_PATH];
    char err[1024];

    ws_manifest_write_line(base, to_home);
    ASSERT_FALSE(cbm_workspace_manifest_approve(cache, home, base, err, sizeof(err)));
    ASSERT_NOT_NULL(strstr(err, to_home));
    ASSERT(snprintf(expected, sizeof(expected), "(resolves to %s)", home) > 0);
    ASSERT_NOT_NULL(strstr(err, expected));
    ASSERT_NOT_NULL(strstr(err, cbm_workspace_verdict_reason(CBM_WS_DENY_SENSITIVE)));

    ws_manifest_write_line(base, to_keys);
    ASSERT_FALSE(cbm_workspace_manifest_approve(cache, home, base, err, sizeof(err)));
    ASSERT_NOT_NULL(strstr(err, to_keys));
    ASSERT(snprintf(expected, sizeof(expected), "(resolves to %s)", keys) > 0);
    ASSERT_NOT_NULL(strstr(err, expected));
    ASSERT_NOT_NULL(strstr(err, cbm_workspace_verdict_reason(CBM_WS_DENY_SENSITIVE)));

    /* On record as approved: the use-time check still refuses both. */
    char body[WS_T_PATH];
    ASSERT(snprintf(body, sizeof(body), "%s\n%s\n", to_home, to_keys) > 0);
    ws_manifest_write(base, body);
    ASSERT_TRUE(ws_manifest_record_approval(cache, base));
    ASSERT_FALSE(cbm_workspace_manifest_allows(cache, home, base, home_sub));
    ASSERT_FALSE(cbm_workspace_manifest_allows(cache, home, base, keys_sub));

    th_cleanup(base);
    th_cleanup(cache);
    PASS();
}

/* Approval binds to the manifest text; where a link leads is read again each
 * time the manifest is used. A link approved while it led to an ordinary
 * directory and later pointed at the home directory grants nothing there.
 * POSIX only, for the reason given at the test above. */
TEST(ws_manifest_link_retarget_is_seen_at_use) {
    char base[WS_T_PATH];
    char cache[WS_T_PATH];
    ASSERT_TRUE(ws_tempdir_canonical("cbm_ws_m9", base, sizeof(base)));
    ASSERT_TRUE(ws_tempdir_canonical("cbm_ws_m9c", cache, sizeof(cache)));

    char shared[WS_T_PATH];
    char shared_sub[WS_T_PATH];
    char home[WS_T_PATH];
    char home_sub[WS_T_PATH];
    ASSERT_TRUE(ws_mkdir_canonical(base, "shared", shared, sizeof(shared)));
    ASSERT_TRUE(ws_mkdir_canonical(shared, "proto", shared_sub, sizeof(shared_sub)));
    ASSERT_TRUE(ws_mkdir_canonical(base, "home", home, sizeof(home)));
    ASSERT_TRUE(ws_mkdir_canonical(home, "notes", home_sub, sizeof(home_sub)));

    char extra[WS_T_PATH];
    ASSERT_TRUE(ws_join(extra, sizeof(extra), base, "extra"));
    ASSERT_EQ(symlink(shared, extra), 0);

    char err[1024];
    ws_manifest_write_line(base, extra);
    ASSERT_TRUE(cbm_workspace_manifest_approve(cache, home, base, err, sizeof(err)));
    ASSERT_TRUE(cbm_workspace_manifest_allows(cache, home, base, shared_sub));

    /* Same text, new target. */
    ASSERT_EQ(unlink(extra), 0);
    ASSERT_EQ(symlink(home, extra), 0);
    cbm_ws_manifest_t m;
    ASSERT_TRUE(cbm_workspace_manifest_read(base, &m));
    ASSERT_TRUE(cbm_workspace_manifest_is_approved(cache, base, &m));

    ASSERT_FALSE(cbm_workspace_manifest_allows(cache, home, base, home_sub));
    ASSERT_FALSE(cbm_workspace_manifest_allows(cache, home, base, shared_sub));

    th_cleanup(base);
    th_cleanup(cache);
    PASS();
}
#endif /* _WIN32 */

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
    RUN_TEST(ws_home_dir_is_resolved);
    RUN_TEST(ws_linked_home_classified_as_home);
    RUN_TEST(ws_manifest_entry_classified_in_resolved_form);
    RUN_TEST(ws_manifest_unresolvable_entry_refused);
    RUN_TEST(ws_manifest_ordinary_entries_unchanged);
#ifndef _WIN32
    RUN_TEST(ws_manifest_link_to_sensitive_directory_refused);
    RUN_TEST(ws_manifest_link_retarget_is_seen_at_use);
#endif
}
