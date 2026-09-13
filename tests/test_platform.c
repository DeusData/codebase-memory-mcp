/*
 * test_platform.c — RED phase tests for foundation/platform.
 */
#include "test_framework.h"
#include "../src/foundation/compat.h" /* cbm_setenv / cbm_unsetenv (Windows-portable) */
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/constants.h"
#include "../src/foundation/compat_thread.h"
#include "../src/foundation/platform.h"
#include "../src/foundation/platform_internal.h"
#include "../src/foundation/system_info_internal.h"
#include <stdatomic.h>
#include <stdlib.h>
#include <unistd.h>
#ifndef _WIN32
#include <sys/stat.h> /* chmod for the symlink-policy mode legs */
#endif

#ifdef __linux__
/* Linux-only cgroup tests need stdio for FILE*, stdlib for mkdtemp,
 * string for strncpy/strchr, sys/stat for mkdir, dirent for the
 * shell-free recursive teardown. */
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#endif

enum { PLATFORM_TIME_THREADS = 8 };
enum { PLATFORM_MKDTEMP_THREADS = 8, PLATFORM_MKDTEMP_ITERATIONS = 32 };

#include <stdio.h>
#include <string.h>

/* Worker staging files land under CBM_CACHE_DIR, which users may place at
 * non-ASCII paths. On Windows the templates must round-trip through the wide
 * APIs; the ANSI CRT (_mktemp/_open) mangles UTF-8 bytes and fails. */
/* Store DB paths embed full repo-path-derived project names; deep repo or
 * runner paths overflow the legacy 260-char limit. The compat file layer must
 * carry absolute paths of any length (extended-length \\?\ form on
 * Windows). */
TEST(platform_file_apis_survive_max_path_overflow) {
    char base[CBM_SZ_512];
    int written = snprintf(base, sizeof(base), "/tmp/cbm-longpath-XXXXXX");
    ASSERT_TRUE(written > 0 && written < (int)sizeof(base));
    ASSERT_NOT_NULL(cbm_mkdtemp(base));

    enum { LONG_SEGMENTS = 5 };
    static const char segment[] =
        "segment-abcdefghijklmnopqrstuvwxyz0123456789-abcdefghijklmnop";
    char deep[CBM_SZ_1K];
    written = snprintf(deep, sizeof(deep), "%s", base);
    ASSERT_TRUE(written > 0 && written < (int)sizeof(deep));
    for (int index = 0; index < LONG_SEGMENTS; index++) {
        written = snprintf(deep + strlen(deep), sizeof(deep) - strlen(deep), "/%s", segment);
        ASSERT_TRUE(written > 0);
    }
    ASSERT_TRUE(strlen(deep) > 300);
    ASSERT_TRUE(cbm_mkdir_p(deep, 0700));

    char file_path[CBM_SZ_1K];
    written = snprintf(file_path, sizeof(file_path), "%s/store.db", deep);
    ASSERT_TRUE(written > 0 && written < (int)sizeof(file_path));
    FILE *file = cbm_fopen(file_path, "wb");
    ASSERT_NOT_NULL(file);
    ASSERT_GTE(fputs("deep\n", file), 0);
    ASSERT_EQ(fclose(file), 0);
    ASSERT_TRUE(cbm_file_exists(file_path));
    ASSERT_TRUE(cbm_is_dir(deep));
    ASSERT_EQ(cbm_unlink(file_path), 0);
    char cursor[CBM_SZ_1K];
    (void)snprintf(cursor, sizeof(cursor), "%s", deep);
    for (int index = 0; index < LONG_SEGMENTS; index++) {
        ASSERT_EQ(cbm_rmdir(cursor), 0);
        char *slash = strrchr(cursor, '/');
        ASSERT_NOT_NULL(slash);
        *slash = '\0';
    }
    (void)cbm_rmdir(base);
    PASS();
}

/* A directory reached through a symlink the invoking account owns is that
 * account's own arrangement ONLY where the caller says so. The plain walk
 * (cbm_mkdir_p) keeps refusing such a link: it is what repository-derived
 * paths use, where a user-owned link can be a checked-in file. A caller that
 * opts in with CBM_MKDIR_FOLLOW_OWNED -- paths rooted in HOME, XDG or the
 * cache, the dotfile-manager shape ~/.config/opencode -> /mnt/... -- walks
 * through it and creates the missing tail INSIDE the target. Even opted in, a
 * dangling link or a link to a regular file fails closed with nothing created
 * on either side. */
TEST(platform_mkdir_p_follows_own_symlink_only_when_opted_in) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX symlink ownership contract");
#else
    char base[CBM_SZ_512];
    int written = snprintf(base, sizeof(base), "/tmp/cbm-ownlink-XXXXXX");
    ASSERT_TRUE(written > 0 && written < (int)sizeof(base));
    ASSERT_NOT_NULL(cbm_mkdtemp(base));

    char target[CBM_SZ_1K];
    char link[CBM_SZ_1K];
    char through_link[CBM_SZ_1K];
    char child[CBM_SZ_1K];
    char created[CBM_SZ_1K];
    (void)snprintf(target, sizeof(target), "%s/target", base);
    (void)snprintf(link, sizeof(link), "%s/link", base);
    (void)snprintf(through_link, sizeof(through_link), "%s/child/grandchild", link);
    (void)snprintf(child, sizeof(child), "%s/child", target);
    (void)snprintf(created, sizeof(created), "%s/child/grandchild", target);
    ASSERT_TRUE(cbm_mkdir_p(target, 0700));
    ASSERT_EQ(symlink(target, link), 0);

    /* "Own" means owned by the invoking account. Root owns everything it
     * creates and root-owned links are trusted by BOTH walks, so under euid 0
     * the link is given to a non-root account to keep the contrast real: then
     * the plain walk refuses it, and so does the opted-in walk, because a
     * privileged walk never follows a user's link. */
    bool privileged = geteuid() == 0;
    enum { LINK_OWNER_UID = 65533 };
    if (privileged) {
        ASSERT_EQ(lchown(link, LINK_OWNER_UID, LINK_OWNER_UID), 0);
    }

    /* The plain walk refuses the user-owned link and creates nothing. */
    ASSERT_FALSE(cbm_mkdir_p(through_link, 0700));
    ASSERT_FALSE(cbm_is_dir(child));

    if (privileged) {
        ASSERT_FALSE(cbm_mkdir_p_ex(through_link, 0700, CBM_MKDIR_FOLLOW_OWNED));
        ASSERT_FALSE(cbm_is_dir(child));
        /* Root-owned again: infrastructure, followed by both walks as always. */
        ASSERT_EQ(lchown(link, 0, 0), 0);
        ASSERT_TRUE(cbm_mkdir_p(through_link, 0700));
        ASSERT_TRUE(cbm_is_dir(created));
    } else {
        /* The opted-in walk follows it into the target. */
        ASSERT_TRUE(cbm_mkdir_p_ex(through_link, 0700, CBM_MKDIR_FOLLOW_OWNED));
        ASSERT_TRUE(cbm_is_dir(created));
    }

    char dangling[CBM_SZ_1K];
    char through_dangling[CBM_SZ_1K];
    char dangling_target[CBM_SZ_1K];
    (void)snprintf(dangling, sizeof(dangling), "%s/dangling", base);
    (void)snprintf(through_dangling, sizeof(through_dangling), "%s/child", dangling);
    (void)snprintf(dangling_target, sizeof(dangling_target), "%s/missing", base);
    ASSERT_EQ(symlink("missing", dangling), 0);
    ASSERT_FALSE(cbm_mkdir_p_ex(through_dangling, 0700, CBM_MKDIR_FOLLOW_OWNED));
    ASSERT_FALSE(cbm_is_dir(dangling_target));

    /* An own link to a regular FILE is not a directory to walk into: refused,
     * and the file it points at is left exactly as it was. */
    char file_target[CBM_SZ_1K];
    char file_link[CBM_SZ_1K];
    char through_file_link[CBM_SZ_1K];
    (void)snprintf(file_target, sizeof(file_target), "%s/file", base);
    (void)snprintf(file_link, sizeof(file_link), "%s/file-link", base);
    (void)snprintf(through_file_link, sizeof(through_file_link), "%s/child", file_link);
    FILE *file = cbm_fopen(file_target, "wb");
    ASSERT_NOT_NULL(file);
    ASSERT_GTE(fputs("data\n", file), 0);
    ASSERT_EQ(fclose(file), 0);
    ASSERT_EQ(symlink(file_target, file_link), 0);
    ASSERT_FALSE(cbm_mkdir_p_ex(through_file_link, 0700, CBM_MKDIR_FOLLOW_OWNED));
    ASSERT_FALSE(cbm_is_dir(file_target));
    ASSERT_EQ(cbm_file_size(file_target), 5);

    ASSERT_EQ(cbm_unlink(file_link), 0);
    ASSERT_EQ(cbm_unlink(file_target), 0);
    ASSERT_EQ(cbm_unlink(dangling), 0);
    ASSERT_EQ(cbm_unlink(link), 0);
    ASSERT_EQ(cbm_rmdir(created), 0);
    ASSERT_EQ(cbm_rmdir(child), 0);
    ASSERT_EQ(cbm_rmdir(target), 0);
    ASSERT_EQ(cbm_rmdir(base), 0);
    PASS();
#endif
}

/* A followed link is resolved from its own text, so the text has to be
 * resolved the way the kernel resolves it: a relative text against the link's
 * directory (never the process's working directory), an absolute text as is,
 * and links inside the text followed in turn. The test changes into an
 * unrelated directory first so a CWD-relative resolution would miss. */
TEST(platform_mkdir_p_resolves_link_text_from_the_link_directory) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX symlink ownership contract");
#else
    char base[CBM_SZ_512];
    int written = snprintf(base, sizeof(base), "/tmp/cbm-linktext-XXXXXX");
    ASSERT_TRUE(written > 0 && written < (int)sizeof(base));
    ASSERT_NOT_NULL(cbm_mkdtemp(base));
    char elsewhere[CBM_SZ_1K];
    char target[CBM_SZ_1K];
    char nested[CBM_SZ_1K];
    (void)snprintf(elsewhere, sizeof(elsewhere), "%s/elsewhere", base);
    (void)snprintf(target, sizeof(target), "%s/dir/target", base);
    (void)snprintf(nested, sizeof(nested), "%s/dir/nested", base);
    ASSERT_TRUE(cbm_mkdir_p(elsewhere, 0700));
    ASSERT_TRUE(cbm_mkdir_p(target, 0700));
    ASSERT_TRUE(cbm_mkdir_p(nested, 0700));

    char saved_cwd[CBM_SZ_1K];
    ASSERT_NOT_NULL(getcwd(saved_cwd, sizeof(saved_cwd)));
    ASSERT_EQ(chdir(elsewhere), 0);

    /* relative text, sibling: dir/relative -> target */
    char link[CBM_SZ_1K];
    char through[CBM_SZ_1K];
    char created[CBM_SZ_1K];
    (void)snprintf(link, sizeof(link), "%s/dir/relative", base);
    ASSERT_EQ(symlink("target", link), 0);
    (void)snprintf(through, sizeof(through), "%s/child-relative", link);
    (void)snprintf(created, sizeof(created), "%s/child-relative", target);
    ASSERT_TRUE(cbm_mkdir_p_ex(through, 0700, CBM_MKDIR_FOLLOW_OWNED));
    ASSERT_TRUE(cbm_is_dir(created));
    ASSERT_EQ(cbm_rmdir(created), 0);
    ASSERT_EQ(cbm_unlink(link), 0);

    /* relative text through the parent: dir/nested/up -> ../target */
    (void)snprintf(link, sizeof(link), "%s/up", nested);
    ASSERT_EQ(symlink("../target", link), 0);
    (void)snprintf(through, sizeof(through), "%s/child-up", link);
    (void)snprintf(created, sizeof(created), "%s/child-up", target);
    ASSERT_TRUE(cbm_mkdir_p_ex(through, 0700, CBM_MKDIR_FOLLOW_OWNED));
    ASSERT_TRUE(cbm_is_dir(created));
    ASSERT_EQ(cbm_rmdir(created), 0);
    ASSERT_EQ(cbm_unlink(link), 0);

    /* absolute text, and a chain whose text names another link */
    (void)snprintf(link, sizeof(link), "%s/absolute", base);
    ASSERT_EQ(symlink(target, link), 0);
    char chain[CBM_SZ_1K];
    (void)snprintf(chain, sizeof(chain), "%s/chain", base);
    ASSERT_EQ(symlink("absolute", chain), 0);
    (void)snprintf(through, sizeof(through), "%s/child-chain", chain);
    (void)snprintf(created, sizeof(created), "%s/child-chain", target);
    ASSERT_TRUE(cbm_mkdir_p_ex(through, 0700, CBM_MKDIR_FOLLOW_OWNED));
    ASSERT_TRUE(cbm_is_dir(created));
    ASSERT_EQ(cbm_rmdir(created), 0);
    ASSERT_EQ(cbm_unlink(chain), 0);
    ASSERT_EQ(cbm_unlink(link), 0);

    ASSERT_EQ(chdir(saved_cwd), 0);
    ASSERT_EQ(cbm_rmdir(target), 0);
    ASSERT_EQ(cbm_rmdir(nested), 0);
    char *slash = strrchr(target, '/');
    ASSERT_NOT_NULL(slash);
    *slash = '\0';
    ASSERT_EQ(cbm_rmdir(target), 0); /* dir */
    ASSERT_EQ(cbm_rmdir(elsewhere), 0);
    ASSERT_EQ(cbm_rmdir(base), 0);
    PASS();
#endif
}

/* The trust an opted-in walk extends to a symlink is bounded on BOTH ends of
 * the link. What the follow lands on must be a directory owned by root or the
 * invoking user and not world-writable unless sticky; the mode legs bind on
 * every run. Two more legs need root, because a non-root process cannot give
 * a file away: a target owned by another account is refused even behind a
 * root-owned link, and a link owned by a non-root account is refused when the
 * walk itself runs as root, so a privileged install never follows a user's
 * link. Each refusal is followed by restoring the state and walking again,
 * which pins that state as the reason for the refusal. */
TEST(platform_mkdir_p_symlink_trust_is_bounded_by_owner_and_mode) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX symlink ownership contract");
#else
    char base[CBM_SZ_512];
    int written = snprintf(base, sizeof(base), "/tmp/cbm-linkowner-XXXXXX");
    ASSERT_TRUE(written > 0 && written < (int)sizeof(base));
    ASSERT_NOT_NULL(cbm_mkdtemp(base));

    char target[CBM_SZ_1K];
    char link[CBM_SZ_1K];
    (void)snprintf(target, sizeof(target), "%s/target", base);
    (void)snprintf(link, sizeof(link), "%s/link", base);
    ASSERT_TRUE(cbm_mkdir_p(target, 0700));
    ASSERT_EQ(symlink(target, link), 0);

    static const char *const children[] = {"control",   "world-writable", "sticky",
                                           "private",   "foreign-target", "target-restored",
                                           "user-link", "link-restored"};
    enum {
        CHILD_CONTROL,
        CHILD_WORLD_WRITABLE,
        CHILD_STICKY,
        CHILD_PRIVATE,
        CHILD_FOREIGN_TARGET,
        CHILD_TARGET_RESTORED,
        CHILD_USER_LINK,
        CHILD_LINK_RESTORED
    };
    char through_link[CBM_SZ_1K];
    char created[CBM_SZ_1K];
#define LINKOWNER_PATHS(index)                                                              \
    do {                                                                                    \
        (void)snprintf(through_link, sizeof(through_link), "%s/%s", link, children[index]); \
        (void)snprintf(created, sizeof(created), "%s/%s", target, children[index]);         \
    } while (0)

    LINKOWNER_PATHS(CHILD_CONTROL);
    ASSERT_TRUE(cbm_mkdir_p_ex(through_link, 0700, CBM_MKDIR_FOLLOW_OWNED));
    ASSERT_TRUE(cbm_is_dir(created));

    /* World-writable target without the sticky bit: anyone could pre-plant
     * entries in it, so it is refused. Sticky (the /tmp shape) is fine, and
     * so is an ordinary private directory. */
    ASSERT_EQ(chmod(target, 0777), 0);
    LINKOWNER_PATHS(CHILD_WORLD_WRITABLE);
    ASSERT_FALSE(cbm_mkdir_p_ex(through_link, 0700, CBM_MKDIR_FOLLOW_OWNED));
    ASSERT_FALSE(cbm_is_dir(created));
    ASSERT_EQ(chmod(target, 01777), 0);
    LINKOWNER_PATHS(CHILD_STICKY);
    ASSERT_TRUE(cbm_mkdir_p_ex(through_link, 0700, CBM_MKDIR_FOLLOW_OWNED));
    ASSERT_TRUE(cbm_is_dir(created));
    ASSERT_EQ(chmod(target, 0755), 0);
    LINKOWNER_PATHS(CHILD_PRIVATE);
    ASSERT_TRUE(cbm_mkdir_p_ex(through_link, 0700, CBM_MKDIR_FOLLOW_OWNED));
    ASSERT_TRUE(cbm_is_dir(created));

    if (geteuid() == 0) {
        enum { FOREIGN_UID = 65534, USER_UID = 65533 };

        /* Root-owned link, target owned by another account: refused. */
        ASSERT_EQ(chown(target, FOREIGN_UID, FOREIGN_UID), 0);
        LINKOWNER_PATHS(CHILD_FOREIGN_TARGET);
        ASSERT_FALSE(cbm_mkdir_p_ex(through_link, 0700, CBM_MKDIR_FOLLOW_OWNED));
        ASSERT_FALSE(cbm_is_dir(created));
        ASSERT_EQ(chown(target, 0, 0), 0);
        LINKOWNER_PATHS(CHILD_TARGET_RESTORED);
        ASSERT_TRUE(cbm_mkdir_p_ex(through_link, 0700, CBM_MKDIR_FOLLOW_OWNED));
        ASSERT_TRUE(cbm_is_dir(created));

        /* Link AND target owned by a non-root account -- the shape of a
         * privileged install into that account's home -- walked as root:
         * refused even when opted in, because root trusts only root-owned
         * links. */
        ASSERT_EQ(lchown(link, USER_UID, USER_UID), 0);
        ASSERT_EQ(chown(target, USER_UID, USER_UID), 0);
        LINKOWNER_PATHS(CHILD_USER_LINK);
        ASSERT_FALSE(cbm_mkdir_p_ex(through_link, 0700, CBM_MKDIR_FOLLOW_OWNED));
        ASSERT_FALSE(cbm_is_dir(created));
        ASSERT_EQ(lchown(link, 0, 0), 0);
        ASSERT_EQ(chown(target, 0, 0), 0);
        LINKOWNER_PATHS(CHILD_LINK_RESTORED);
        ASSERT_TRUE(cbm_mkdir_p_ex(through_link, 0700, CBM_MKDIR_FOLLOW_OWNED));
        ASSERT_TRUE(cbm_is_dir(created));
    }
#undef LINKOWNER_PATHS

    for (size_t index = 0; index < sizeof(children) / sizeof(children[0]); index++) {
        (void)snprintf(created, sizeof(created), "%s/%s", target, children[index]);
        if (cbm_is_dir(created)) {
            ASSERT_EQ(cbm_rmdir(created), 0);
        }
    }
    ASSERT_EQ(cbm_unlink(link), 0);
    ASSERT_EQ(cbm_rmdir(target), 0);
    ASSERT_EQ(cbm_rmdir(base), 0);
    PASS();
#endif
}

/* The policy is per CALL SITE, not per path. The same user-owned link is
 * followed by the opted-in walk a configuration root uses and refused by the
 * plain walk a checkout uses: `~/.claude -> ~/dotfiles/claude` works for the
 * installer, while a repository that ships `.codebase-memory -> <a directory
 * the user owns>` cannot redirect the artifact writer. Under euid 0 the
 * checked-in link is demoted to a non-root owner, since root-owned links are
 * trusted by both walks and a privileged walk never follows a user's link. */
TEST(platform_mkdir_p_follow_owned_is_per_call_site) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX symlink ownership contract");
#else
    char base[CBM_SZ_512];
    int written = snprintf(base, sizeof(base), "/tmp/cbm-linksite-XXXXXX");
    ASSERT_TRUE(written > 0 && written < (int)sizeof(base));
    ASSERT_NOT_NULL(cbm_mkdtemp(base));

    /* The dotfile-manager shape under a home directory. */
    char dotfiles[CBM_SZ_1K];
    char claude_link[CBM_SZ_1K];
    char agents_through_link[CBM_SZ_1K];
    char agents_in_dotfiles[CBM_SZ_1K];
    (void)snprintf(dotfiles, sizeof(dotfiles), "%s/home/dotfiles/claude", base);
    (void)snprintf(claude_link, sizeof(claude_link), "%s/home/.claude", base);
    (void)snprintf(agents_through_link, sizeof(agents_through_link), "%s/agents", claude_link);
    (void)snprintf(agents_in_dotfiles, sizeof(agents_in_dotfiles), "%s/agents", dotfiles);
    ASSERT_TRUE(cbm_mkdir_p(dotfiles, 0700));
    ASSERT_EQ(symlink(dotfiles, claude_link), 0);
    ASSERT_TRUE(cbm_mkdir_p_ex(agents_through_link, 0700, CBM_MKDIR_FOLLOW_OWNED));
    ASSERT_TRUE(cbm_is_dir(agents_in_dotfiles));

    /* A checked-in link inside a repository, pointing at a directory the
     * same user owns: the plain walk refuses it and creates nothing behind
     * it. The identical link IS followed once opted in, so the refusal comes
     * from the call site's policy and nothing else. */
    char outside[CBM_SZ_1K];
    char artifact_link[CBM_SZ_1K];
    char artifacts_through_link[CBM_SZ_1K];
    char artifacts_outside[CBM_SZ_1K];
    (void)snprintf(outside, sizeof(outside), "%s/outside", base);
    (void)snprintf(artifact_link, sizeof(artifact_link), "%s/repo/.codebase-memory", base);
    (void)snprintf(artifacts_through_link, sizeof(artifacts_through_link), "%s/artifacts",
                   artifact_link);
    (void)snprintf(artifacts_outside, sizeof(artifacts_outside), "%s/artifacts", outside);
    char repo[CBM_SZ_1K];
    (void)snprintf(repo, sizeof(repo), "%s/repo", base);
    ASSERT_TRUE(cbm_mkdir_p(outside, 0700));
    ASSERT_TRUE(cbm_mkdir_p(repo, 0700));
    ASSERT_EQ(symlink(outside, artifact_link), 0);
    /* Git creates the checked-in link owned by whoever cloned. A root run
     * must model that account as non-root: root-owned links are trusted
     * infrastructure for both walks, which is the pre-existing contract. */
    bool privileged = geteuid() == 0;
    enum { CLONING_UID = 65533 };
    if (privileged) {
        ASSERT_EQ(lchown(artifact_link, CLONING_UID, CLONING_UID), 0);
    }
    ASSERT_FALSE(cbm_mkdir_p(artifacts_through_link, 0700));
    ASSERT_FALSE(cbm_is_dir(artifacts_outside));
    if (!privileged) {
        /* Same link, opted in: followed. Not under euid 0, where a privileged
         * walk never follows a user's link whatever the call site says. */
        ASSERT_TRUE(cbm_mkdir_p_ex(artifacts_through_link, 0700, CBM_MKDIR_FOLLOW_OWNED));
        ASSERT_TRUE(cbm_is_dir(artifacts_outside));
        ASSERT_EQ(cbm_rmdir(artifacts_outside), 0);
    }

    ASSERT_EQ(cbm_unlink(artifact_link), 0);
    ASSERT_EQ(cbm_rmdir(repo), 0);
    ASSERT_EQ(cbm_rmdir(outside), 0);
    ASSERT_EQ(cbm_rmdir(agents_in_dotfiles), 0);
    ASSERT_EQ(cbm_unlink(claude_link), 0);
    ASSERT_EQ(cbm_rmdir(dotfiles), 0);
    char *slash = strrchr(dotfiles, '/');
    ASSERT_NOT_NULL(slash);
    *slash = '\0';
    ASSERT_EQ(cbm_rmdir(dotfiles), 0); /* home/dotfiles */
    slash = strrchr(dotfiles, '/');
    ASSERT_NOT_NULL(slash);
    *slash = '\0';
    ASSERT_EQ(cbm_rmdir(dotfiles), 0); /* home */
    ASSERT_EQ(cbm_rmdir(base), 0);
    PASS();
#endif
}

TEST(platform_mkstemp_and_mkdtemp_survive_non_ascii_directory) {
    char base[CBM_SZ_256];
    int written = snprintf(base, sizeof(base), "/tmp/cbm-utf8-Ã©Ã¨-XXXXXX");
    ASSERT_TRUE(written > 0 && written < (int)sizeof(base));
    ASSERT_NOT_NULL(cbm_mkdtemp(base));

    char file_template[CBM_SZ_512];
    written = snprintf(file_template, sizeof(file_template), "%s/.probe-XXXXXX", base);
    ASSERT_TRUE(written > 0 && written < (int)sizeof(file_template));
    int descriptor = cbm_mkstemp(file_template);
    bool created = descriptor >= 0;
    if (created) {
#ifdef _WIN32
        _close(descriptor);
#else
        close(descriptor);
#endif
        (void)cbm_unlink(file_template);
    }
    (void)cbm_rmdir(base);
    ASSERT_TRUE(created);
    /* The returned path must keep the caller's UTF-8 directory intact. */
    ASSERT_NOT_NULL(strstr(file_template, "Ã©Ã¨"));
    PASS();
}

typedef struct {
    atomic_int *ready;
    atomic_bool *go;
    int worker_index;
    int created;
    bool ok;
    char paths[PLATFORM_MKDTEMP_ITERATIONS][CBM_SZ_512];
} platform_mkdtemp_worker_t;

static void *platform_mkdtemp_concurrent_worker(void *opaque) {
    platform_mkdtemp_worker_t *worker = opaque;
    (void)atomic_fetch_add_explicit(worker->ready, 1, memory_order_acq_rel);
    while (!atomic_load_explicit(worker->go, memory_order_acquire)) {
        atomic_signal_fence(memory_order_seq_cst);
    }
    worker->ok = true;
    for (int index = 0; index < PLATFORM_MKDTEMP_ITERATIONS; index++) {
        int written =
            snprintf(worker->paths[index], sizeof(worker->paths[index]),
                     "/tmp/cbm-mkdtemp-concurrent-%d-%d-XXXXXX", worker->worker_index, index);
        if (written <= 0 || written >= (int)sizeof(worker->paths[index]) ||
            !cbm_mkdtemp(worker->paths[index])) {
            worker->ok = false;
            break;
        }
        worker->created++;
    }
    return NULL;
}

/* MCP daemon sessions can enter cbm_mkdtemp simultaneously. Every request must
 * retain its own expanded path and create a distinct directory. */
TEST(platform_mkdtemp_is_thread_safe) {
    cbm_thread_t threads[PLATFORM_MKDTEMP_THREADS];
    platform_mkdtemp_worker_t workers[PLATFORM_MKDTEMP_THREADS] = {0};
    atomic_int ready;
    atomic_bool go;
    atomic_init(&ready, 0);
    atomic_init(&go, false);

    int started = 0;
    for (; started < PLATFORM_MKDTEMP_THREADS; started++) {
        workers[started].ready = &ready;
        workers[started].go = &go;
        workers[started].worker_index = started;
        if (cbm_thread_create(&threads[started], 0, platform_mkdtemp_concurrent_worker,
                              &workers[started]) != 0) {
            break;
        }
    }
    while (started == PLATFORM_MKDTEMP_THREADS &&
           atomic_load_explicit(&ready, memory_order_acquire) != PLATFORM_MKDTEMP_THREADS) {
        atomic_signal_fence(memory_order_seq_cst);
    }
    atomic_store_explicit(&go, true, memory_order_release);
    for (int index = 0; index < started; index++) {
        (void)cbm_thread_join(&threads[index]);
    }

    bool all_ok = started == PLATFORM_MKDTEMP_THREADS;
    for (int index = 0; index < started; index++) {
        all_ok =
            all_ok && workers[index].ok && workers[index].created == PLATFORM_MKDTEMP_ITERATIONS;
        for (int path_index = 0; path_index < workers[index].created; path_index++) {
            all_ok = all_ok && cbm_is_dir(workers[index].paths[path_index]);
            (void)cbm_rmdir(workers[index].paths[path_index]);
        }
    }
    ASSERT_TRUE(all_ok);
    PASS();
}

TEST(platform_counter_scaling_avoids_intermediate_overflow) {
    const uint64_t frequency = UINT64_C(10000000);
    const uint64_t counter = UINT64_C(20000000000);

    ASSERT(cbm_platform_scale_counter_ns(counter, frequency) == UINT64_C(2000000000000));
    ASSERT(cbm_platform_scale_counter_ns(UINT64_MAX, UINT64_MAX) == UINT64_C(1000000000));
    ASSERT(cbm_platform_scale_counter_ns(UINT64_MAX, 1) == UINT64_MAX);
    PASS();
}

TEST(platform_counter_scaling_preserves_monotonic_deadlines) {
    const uint64_t frequency = UINT64_C(10000000);
    const uint64_t counter = UINT64_MAX / UINT64_C(1000000000);
    const uint64_t now_ns = cbm_platform_scale_counter_ns(counter, frequency);
    const uint64_t next_ns = cbm_platform_scale_counter_ns(counter + 1, frequency);
    const uint64_t deadline_ns = cbm_platform_scale_counter_ns(counter + 5 * frequency, frequency);

    ASSERT(next_ns >= now_ns);
    ASSERT(deadline_ns > now_ns);
    ASSERT(deadline_ns - now_ns == UINT64_C(5000000000));
    PASS();
}

typedef struct {
    atomic_int *ready;
    atomic_bool *go;
    uint64_t value;
} platform_time_worker_t;

static void *platform_time_first_call(void *opaque) {
    platform_time_worker_t *worker = opaque;
    (void)atomic_fetch_add_explicit(worker->ready, 1, memory_order_acq_rel);
    while (!atomic_load_explicit(worker->go, memory_order_acquire)) {
        atomic_signal_fence(memory_order_seq_cst);
    }
    worker->value = cbm_now_ns();
    return NULL;
}

TEST(platform_now_ns_concurrent_first_call) {
    cbm_thread_t threads[PLATFORM_TIME_THREADS];
    platform_time_worker_t workers[PLATFORM_TIME_THREADS];
    atomic_int ready;
    atomic_bool go;
    atomic_init(&ready, 0);
    atomic_init(&go, false);
    size_t created = 0;
    for (; created < PLATFORM_TIME_THREADS; created++) {
        workers[created] = (platform_time_worker_t){
            .ready = &ready,
            .go = &go,
        };
        if (cbm_thread_create(&threads[created], 0, platform_time_first_call, &workers[created]) !=
            0) {
            break;
        }
    }
    while (created == PLATFORM_TIME_THREADS &&
           atomic_load_explicit(&ready, memory_order_acquire) != PLATFORM_TIME_THREADS) {
        atomic_signal_fence(memory_order_seq_cst);
    }
    atomic_store_explicit(&go, true, memory_order_release);
    for (size_t index = 0; index < created; index++) {
        (void)cbm_thread_join(&threads[index]);
    }

    ASSERT_EQ(created, PLATFORM_TIME_THREADS);
    for (size_t index = 0; index < created; index++) {
        ASSERT_GT(workers[index].value, 0);
    }
    PASS();
}

TEST(platform_now_ns) {
    uint64_t t1 = cbm_now_ns();
    ASSERT_GT(t1, 0);
    /* Busy-wait a tiny bit */
    for (volatile int i = 0; i < 100000; i++) {}
    uint64_t t2 = cbm_now_ns();
    ASSERT_GT(t2, t1);
    PASS();
}

TEST(platform_now_ms) {
    uint64_t t1 = cbm_now_ms();
    ASSERT_GT(t1, 0);
    PASS();
}

TEST(platform_nprocs) {
    int n = cbm_nprocs();
    ASSERT_GT(n, 0);
    ASSERT_LT(n, 10000); /* sanity */
    PASS();
}

TEST(platform_file_exists) {
    /* This test file should exist */
    ASSERT_TRUE(cbm_file_exists("tests/test_platform.c"));
    ASSERT_FALSE(cbm_file_exists("nonexistent_file_xyz.txt"));
    PASS();
}

TEST(platform_is_dir) {
    ASSERT_TRUE(cbm_is_dir("tests"));
    ASSERT_FALSE(cbm_is_dir("tests/test_platform.c"));
    ASSERT_FALSE(cbm_is_dir("nonexistent_dir"));
    PASS();
}

TEST(platform_file_size) {
    int64_t sz = cbm_file_size("tests/test_platform.c");
    ASSERT_GT(sz, 0);
    ASSERT_EQ(cbm_file_size("nonexistent_file_xyz.txt"), -1);
    PASS();
}

TEST(platform_mmap) {
    /* mmap this test file and verify first bytes */
    size_t sz = 0;
    void *data = cbm_mmap_read("tests/test_platform.c", &sz);
    ASSERT_NOT_NULL(data);
    ASSERT_GT(sz, 0);
    /* First line should be the comment */
    ASSERT(memcmp(data, "/*", 2) == 0);
    cbm_munmap(data, sz);
    PASS();
}

TEST(platform_mmap_nonexistent) {
    size_t sz = 0;
    void *data = cbm_mmap_read("nonexistent_xyz.txt", &sz);
    ASSERT_NULL(data);
    PASS();
}

typedef struct {
    const char *home;
    const char *cache;
    atomic_int *ready;
    atomic_int *release;
} platform_path_thread_result_t;

static void *platform_capture_path_buffers(void *opaque) {
    platform_path_thread_result_t *result = opaque;
    result->home = cbm_get_home_dir();
    result->cache = cbm_resolve_cache_dir();
    atomic_fetch_add_explicit(result->ready, 1, memory_order_release);
    while (atomic_load_explicit(result->release, memory_order_acquire) == 0) {
        cbm_usleep(1000);
    }
    return NULL;
}

/* The daemon dispatches different sessions concurrently. Path helpers may
 * retain their historical return-pointer API, but each live thread needs
 * separate storage; process-global mutable buffers are a C data race. */
TEST(platform_path_helpers_use_per_thread_storage) {
    const char *saved_home = getenv("HOME");
    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_home_copy = saved_home ? strdup(saved_home) : NULL;
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    ASSERT_EQ(cbm_setenv("HOME", "/tmp/cbm-platform-thread-home", 1), 0);
    ASSERT_EQ(cbm_setenv("CBM_CACHE_DIR", "/tmp/cbm-platform-thread-cache", 1), 0);

    atomic_int ready;
    atomic_int release;
    atomic_init(&ready, 0);
    atomic_init(&release, 0);
    platform_path_thread_result_t results[2] = {
        {.ready = &ready, .release = &release},
        {.ready = &ready, .release = &release},
    };
    cbm_thread_t threads[2];
    bool started0 =
        cbm_thread_create(&threads[0], 0, platform_capture_path_buffers, &results[0]) == 0;
    bool started1 =
        cbm_thread_create(&threads[1], 0, platform_capture_path_buffers, &results[1]) == 0;
    for (int spins = 0; started0 && started1 && spins < 5000 &&
                        atomic_load_explicit(&ready, memory_order_acquire) < 2;
         spins++) {
        cbm_usleep(1000);
    }
    bool both_ready = atomic_load_explicit(&ready, memory_order_acquire) == 2;
    bool separate_home =
        both_ready && results[0].home && results[1].home && results[0].home != results[1].home;
    bool separate_cache =
        both_ready && results[0].cache && results[1].cache && results[0].cache != results[1].cache;
    atomic_store_explicit(&release, 1, memory_order_release);
    if (started0) {
        (void)cbm_thread_join(&threads[0]);
    }
    if (started1) {
        (void)cbm_thread_join(&threads[1]);
    }

    if (saved_home_copy) {
        (void)cbm_setenv("HOME", saved_home_copy, 1);
    } else {
        (void)cbm_unsetenv("HOME");
    }
    if (saved_cache_copy) {
        (void)cbm_setenv("CBM_CACHE_DIR", saved_cache_copy, 1);
    } else {
        (void)cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(saved_home_copy);
    free(saved_cache_copy);

    ASSERT_TRUE(started0);
    ASSERT_TRUE(started1);
    ASSERT_TRUE(both_ready);
    ASSERT_TRUE(separate_home);
    ASSERT_TRUE(separate_cache);
    PASS();
}

/* A configured cache root is an identity boundary. Truncating an oversized
 * value and silently using the prefix would admit/log one root while placing
 * data somewhere the user never selected. Reject it instead of falling back. */
TEST(platform_cache_dir_rejects_truncated_override) {
    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    size_t length = 5000U;
    char *value = malloc(length + 1U);
    ASSERT_NOT_NULL(value);
    memcpy(value, "/tmp/", 5U);
    memset(value + 5U, 'a', length - 5U);
    value[length] = '\0';
    ASSERT_EQ(cbm_setenv("CBM_CACHE_DIR", value, 1), 0);

    const char *resolved = cbm_resolve_cache_dir();

    if (saved_copy) {
        (void)cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
    } else {
        (void)cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(saved_copy);
    free(value);
    ASSERT_NULL(resolved);
    PASS();
}

/* cbm_env_long reads a whole number, or says it could not.
 *
 * atoi and atol answer 0 for text they cannot read, and 0 is a real setting at
 * every place this project reads a number out of the environment. So the
 * helper reports whether the read worked instead of folding a failure into a
 * value that looks fine. */
TEST(platform_env_long_reads_a_clean_number) {
    const char *name = "CBM_TEST_ENV_LONG";
    char *saved = getenv(name) ? strdup(getenv(name)) : NULL;
    long out = 0;

    ASSERT_EQ(cbm_setenv(name, "42", 1), 0);
    ASSERT_TRUE(cbm_env_long(name, &out));
    ASSERT_EQ(out, 42);

    /* Zero is a real answer, not a failure. This is the case that made
     * CBM_INDEX_MAX_RESTARTS=0 mean 100 restarts. */
    ASSERT_EQ(cbm_setenv(name, "0", 1), 0);
    out = 999;
    ASSERT_TRUE(cbm_env_long(name, &out));
    ASSERT_EQ(out, 0);

    ASSERT_EQ(cbm_setenv(name, "-7", 1), 0);
    ASSERT_TRUE(cbm_env_long(name, &out));
    ASSERT_EQ(out, -7);

    if (saved) {
        (void)cbm_setenv(name, saved, 1);
        free(saved);
    } else {
        (void)cbm_unsetenv(name);
    }
    PASS();
}

TEST(platform_env_long_refuses_what_it_cannot_read) {
    const char *name = "CBM_TEST_ENV_LONG";
    char *saved = getenv(name) ? strdup(getenv(name)) : NULL;

    /* Every one of these used to answer 0 through atol. */
    const char *unreadable[] = {
        "abc",  "30s",  " 30", "30 ", "",    "1e3",
        "0x10", "+ 30", "--3", "3.5", "99999999999999999999999999",
    };
    for (size_t i = 0; i < sizeof(unreadable) / sizeof(unreadable[0]); i++) {
        ASSERT_EQ(cbm_setenv(name, unreadable[i], 1), 0);
        long out = 1234; /* a value the helper must not touch */
        if (cbm_env_long(name, &out)) {
            printf("  \"%s\" was read as %ld\n", unreadable[i], out);
        }
        ASSERT_TRUE(!cbm_env_long(name, &out));
        ASSERT_EQ(out, 1234);
    }

    /* A variable nobody set answers false too. */
    ASSERT_EQ(cbm_unsetenv(name), 0);
    long out = 555;
    ASSERT_TRUE(!cbm_env_long(name, &out));
    ASSERT_EQ(out, 555);

    /* A NULL destination is refused rather than written through. */
    ASSERT_EQ(cbm_setenv(name, "5", 1), 0);
    ASSERT_TRUE(!cbm_env_long(name, NULL));

    if (saved) {
        (void)cbm_setenv(name, saved, 1);
        free(saved);
    } else {
        (void)cbm_unsetenv(name);
    }
    PASS();
}

#ifdef _WIN32
/* cbm_safe_getenv reads Windows' wide environment as UTF-8. Its matching
 * setter must update that same wide environment; _putenv_s alone interprets
 * UTF-8 path bytes through the active ANSI code page. */
TEST(platform_setenv_preserves_utf8_in_wide_environment) {
    static const char utf8[] = "C:/cbm-cache-\xce\x94-\xe6\x97\xa5\xe6\x9c\xac";
    static const wchar_t wide[] = L"C:/cbm-cache-\u0394-\u65e5\u672c";
    ASSERT_EQ(cbm_setenv("CBM_CACHE_DIR", utf8, 1), 0);
    wchar_t observed_wide[128];
    ASSERT_EQ(GetEnvironmentVariableW(L"CBM_CACHE_DIR", observed_wide, 128), (DWORD)(wcslen(wide)));
    ASSERT_EQ(wcscmp(observed_wide, wide), 0);
    char observed_utf8[128];
    ASSERT_NOT_NULL(cbm_safe_getenv("CBM_CACHE_DIR", observed_utf8, sizeof(observed_utf8), NULL));
    ASSERT_STR_EQ(observed_utf8, utf8);
    (void)cbm_unsetenv("CBM_CACHE_DIR");
    PASS();
}

/* Empty and absent variables have different fallback semantics. In
 * particular, an explicitly empty CBM_CACHE_DIR means "use the default"; it
 * must not be misreported as a failed wide-environment read. Unset is also
 * required to stay idempotent because test and process cleanup call it after
 * partially initialized paths. */
TEST(platform_windows_empty_environment_is_read_and_unset_idempotently) {
    ASSERT_EQ(cbm_setenv("CBM_CACHE_DIR", "", 1), 0);
    char observed[9] = "sentinel";
    ASSERT_NOT_NULL(cbm_safe_getenv("CBM_CACHE_DIR", observed, sizeof(observed), "fallback"));
    ASSERT_STR_EQ(observed, "");
    ASSERT_EQ(cbm_unsetenv("CBM_CACHE_DIR"), 0);
    ASSERT_EQ(cbm_unsetenv("CBM_CACHE_DIR"), 0);
    PASS();
}
#endif

/*
 * CBM_WORKERS env override for cbm_default_worker_count.
 *
 * Containers running cbm on a host with more CPUs than the cgroup's
 * effective quota currently see ~host_cpu workers spawned because
 * sysconf(_SC_NPROCESSORS_ONLN) is not cgroup-aware (see GitHub
 * issue for the cgroup-detection ask). CBM_WORKERS is the smaller,
 * explicit-override path that ships independently.
 */
TEST(platform_default_workers_env_override) {
    cbm_setenv("CBM_WORKERS", "4", 1);
    int n = cbm_default_worker_count(true);
    ASSERT_EQ(n, 4);
    /* initial=false should also honor the explicit override. */
    int m = cbm_default_worker_count(false);
    ASSERT_EQ(m, 4);
    cbm_unsetenv("CBM_WORKERS");
    PASS();
}

TEST(platform_default_workers_env_invalid) {
    /* Out-of-range values (< 1 or > 256) and non-numeric strings
     * fall back to the sysconf-derived default. */
    int baseline = cbm_default_worker_count(true);
    ASSERT_GT(baseline, 0);

    cbm_setenv("CBM_WORKERS", "0", 1);
    ASSERT_EQ(cbm_default_worker_count(true), baseline);

    cbm_setenv("CBM_WORKERS", "-1", 1);
    ASSERT_EQ(cbm_default_worker_count(true), baseline);

    cbm_setenv("CBM_WORKERS", "9999", 1);
    ASSERT_EQ(cbm_default_worker_count(true), baseline);

    cbm_setenv("CBM_WORKERS", "not-a-number", 1);
    ASSERT_EQ(cbm_default_worker_count(true), baseline);

    cbm_unsetenv("CBM_WORKERS");
    PASS();
}

TEST(platform_default_workers_env_unset) {
    /* When CBM_WORKERS is unset the result matches today's behaviour
     * (info.total_cores for initial=true, perf_cores-1 for false). */
    cbm_unsetenv("CBM_WORKERS");
    cbm_system_info_t info = cbm_system_info();
    ASSERT_EQ(cbm_default_worker_count(true), info.total_cores);
    PASS();
}

TEST(platform_system_info) {
    cbm_system_info_t info = cbm_system_info();
    ASSERT_GT(info.total_cores, 0);
    ASSERT_GT(info.total_ram, 0);
    PASS();
}

/* ── cgroup-aware detection (Linux only) ─────────────────────────── */

#ifdef __linux__

/* Create a unique tmp directory the caller will own; returns 0 on success. */
static int cgroup_test_setup(char *root, size_t root_sz) {
    strncpy(root, "/tmp/cbm_cgroup_test_XXXXXX", root_sz);
    return mkdtemp(root) != NULL ? 0 : -1;
}

/* Write `content` to "<root>/<relpath>". Creates parent subdir if needed.
 * Returns 0 on success, -1 on any failure. */
static int cgroup_test_write(const char *root, const char *relpath, const char *content) {
    char path[1024];
    const char *slash = strchr(relpath, '/');
    if (slash != NULL) {
        char subdir[1024];
        size_t n = (size_t)(slash - relpath);
        if (n >= sizeof(subdir)) {
            return -1;
        }
        memcpy(subdir, relpath, n);
        subdir[n] = '\0';
        snprintf(path, sizeof(path), "%s/%s", root, subdir);
        (void)mkdir(path, S_IRWXU);
    }
    snprintf(path, sizeof(path), "%s/%s", root, relpath);
    FILE *fp = fopen(path, "we");
    if (fp == NULL) {
        return -1;
    }
    size_t n = strlen(content);
    int rc = (fwrite(content, 1, n, fp) == n) ? 0 : -1;
    fclose(fp);
    return rc;
}

/* Recursively remove a tmp dir created by cgroup_test_setup. Best-effort.
 * Uses opendir/unlink/rmdir rather than system("rm -rf ...") to avoid
 * spawning a shell from the test binary. */
static void cgroup_test_teardown(const char *root) {
    DIR *d = opendir(root);
    if (d != NULL) {
        struct dirent *ent;
        while ((ent = readdir(d)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) {
                continue;
            }
            char child[1024];
            snprintf(child, sizeof(child), "%s/%s", root, ent->d_name);
            struct stat st;
            if (stat(child, &st) == 0 && S_ISDIR(st.st_mode)) {
                cgroup_test_teardown(child); /* recurse into subdir */
            } else {
                (void)unlink(child);
            }
        }
        closedir(d);
    }
    (void)rmdir(root);
}

TEST(cgroup_v2_cpu_quota) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* 200ms quota in a 100ms period → 2 effective CPUs. */
    ASSERT_EQ(cgroup_test_write(root, "cpu.max", "200000 100000\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), 2);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v2_cpu_quota_rounds_up) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* 150ms quota / 100ms period = 1.5 → ceil = 2. */
    ASSERT_EQ(cgroup_test_write(root, "cpu.max", "150000 100000\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), 2);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v2_cpu_unlimited) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    ASSERT_EQ(cgroup_test_write(root, "cpu.max", "max 100000\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), -1);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v1_cpu_quota) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    ASSERT_EQ(cgroup_test_write(root, "cpu/cpu.cfs_quota_us", "200000"), 0);
    ASSERT_EQ(cgroup_test_write(root, "cpu/cpu.cfs_period_us", "100000"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), 2);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v1_cpu_unlimited) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* quota=-1 is the cgroup-v1 sentinel for "no quota". */
    ASSERT_EQ(cgroup_test_write(root, "cpu/cpu.cfs_quota_us", "-1"), 0);
    ASSERT_EQ(cgroup_test_write(root, "cpu/cpu.cfs_period_us", "100000"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), -1);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_no_cpu_files) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* Empty tmp dir: no v2 file, no v1 file → fall through to sysconf. */
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), -1);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v2_mem) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* 2 GiB. */
    ASSERT_EQ(cgroup_test_write(root, "memory.max", "2147483648\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)2147483648UL);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v2_mem_unlimited) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    ASSERT_EQ(cgroup_test_write(root, "memory.max", "max\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)0);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v1_mem) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* 1 GiB. */
    ASSERT_EQ(cgroup_test_write(root, "memory/memory.limit_in_bytes", "1073741824"), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)1073741824UL);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v1_mem_unlimited_sentinel) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* cgroup v1 reports a huge near-ULLONG_MAX value when unlimited
     * (PAGE_COUNTER_MAX). Our parser treats anything >= ULLONG_MAX/2
     * as effectively unlimited. */
    ASSERT_EQ(cgroup_test_write(root, "memory/memory.limit_in_bytes", "9223372036854775807"), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)0);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_no_mem_files) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)0);
    cgroup_test_teardown(root);
    PASS();
}

#endif /* __linux__ */

SUITE(platform) {
    RUN_TEST(platform_file_apis_survive_max_path_overflow);
    RUN_TEST(platform_mkdir_p_follows_own_symlink_only_when_opted_in);
    RUN_TEST(platform_mkdir_p_symlink_trust_is_bounded_by_owner_and_mode);
    RUN_TEST(platform_mkdir_p_follow_owned_is_per_call_site);
    RUN_TEST(platform_mkdir_p_resolves_link_text_from_the_link_directory);
    RUN_TEST(platform_mkstemp_and_mkdtemp_survive_non_ascii_directory);
    RUN_TEST(platform_mkdtemp_is_thread_safe);
    RUN_TEST(platform_counter_scaling_avoids_intermediate_overflow);
    RUN_TEST(platform_counter_scaling_preserves_monotonic_deadlines);
    RUN_TEST(platform_now_ns_concurrent_first_call);
    RUN_TEST(platform_now_ns);
    RUN_TEST(platform_now_ms);
    RUN_TEST(platform_nprocs);
    RUN_TEST(platform_file_exists);
    RUN_TEST(platform_is_dir);
    RUN_TEST(platform_file_size);
    RUN_TEST(platform_mmap);
    RUN_TEST(platform_mmap_nonexistent);
    RUN_TEST(platform_path_helpers_use_per_thread_storage);
    RUN_TEST(platform_cache_dir_rejects_truncated_override);
#ifdef _WIN32
    RUN_TEST(platform_setenv_preserves_utf8_in_wide_environment);
    RUN_TEST(platform_windows_empty_environment_is_read_and_unset_idempotently);
#endif
    RUN_TEST(platform_default_workers_env_override);
    RUN_TEST(platform_default_workers_env_invalid);
    RUN_TEST(platform_default_workers_env_unset);
    RUN_TEST(platform_env_long_reads_a_clean_number);
    RUN_TEST(platform_env_long_refuses_what_it_cannot_read);
    RUN_TEST(platform_system_info);
#ifdef __linux__
    RUN_TEST(cgroup_v2_cpu_quota);
    RUN_TEST(cgroup_v2_cpu_quota_rounds_up);
    RUN_TEST(cgroup_v2_cpu_unlimited);
    RUN_TEST(cgroup_v1_cpu_quota);
    RUN_TEST(cgroup_v1_cpu_unlimited);
    RUN_TEST(cgroup_no_cpu_files);
    RUN_TEST(cgroup_v2_mem);
    RUN_TEST(cgroup_v2_mem_unlimited);
    RUN_TEST(cgroup_v1_mem);
    RUN_TEST(cgroup_v1_mem_unlimited_sentinel);
    RUN_TEST(cgroup_no_mem_files);
#endif
}
