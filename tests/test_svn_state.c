/*
 * test_svn_state.c -- the Subversion observation core (src/watcher/svn_state.c).
 *
 * No test here needs a Subversion install: status documents are written in the
 * layout `svn status --xml --verbose` produces, and the client tests resolve
 * fake executables that are never run. Fixtures that drive a real `svn` belong
 * with the watcher integration, once the test images carry Subversion.
 */
#include "../src/watcher/svn_state.h"
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/compat_thread.h"
#include "../src/foundation/platform.h"
#include "test_framework.h"
#include "test_helpers.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef _WIN32
#define FAKE_SVN_NAME "svn.exe"
#define PATH_LIST_DELIM ";"
#else
#define FAKE_SVN_NAME "svn"
#define PATH_LIST_DELIM ":"
#endif

/* ── Status documents ──────────────────────────────────────────── */

typedef struct {
    const char *path;
    const char *item;
} svn_entry_t;

static void write_xml_text(FILE *file, const char *text) {
    for (const char *cursor = text; *cursor; cursor++) {
        if (*cursor == '&') {
            fputs("&amp;", file);
        } else if (*cursor == '<') {
            fputs("&lt;", file);
        } else if (*cursor == '"') {
            fputs("&quot;", file);
        } else {
            fputc(*cursor, file);
        }
    }
}

/* One document in the layout svn 1.14 writes: an XML declaration, one
 * attribute per line, explicit closing tags. */
static int write_status(const char *xml_path, const char *target, const svn_entry_t *entries,
                        size_t count) {
    FILE *file = cbm_fopen(xml_path, "wb");
    if (!file) {
        return -1;
    }
    fputs("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<status>\n<target\n   path=\"", file);
    write_xml_text(file, target);
    fputs("\">\n", file);
    for (size_t i = 0; i < count; i++) {
        bool versioned =
            strcmp(entries[i].item, "unversioned") != 0 && strcmp(entries[i].item, "ignored") != 0;
        fputs("<entry\n   path=\"", file);
        write_xml_text(file, entries[i].path);
        fprintf(file, "\">\n<wc-status\n   props=\"none\"\n   item=\"%s\"", entries[i].item);
        if (versioned) {
            fputs("\n   revision=\"3\"", file);
        }
        fputs(">\n</wc-status>\n</entry>\n", file);
    }
    fputs("</target>\n</status>\n", file);
    return fclose(file) == 0 ? 0 : -1;
}

static int write_raw(const char *xml_path, const char *xml) {
    FILE *file = cbm_fopen(xml_path, "wb");
    if (!file) {
        return -1;
    }
    fputs(xml, file);
    return fclose(file) == 0 ? 0 : -1;
}

static cbm_svn_probe_result_t parse_file(const char *root, const char *xml_path,
                                         cbm_svn_observation_t *observation) {
    FILE *stream = cbm_fopen(xml_path, "rb");
    if (!stream) {
        return (cbm_svn_probe_result_t)99;
    }
    cbm_svn_probe_result_t result = cbm_svn_parse_status_stream(stream, root, NULL, observation);
    fclose(stream);
    return result;
}

static cbm_svn_probe_result_t parse_entries(const char *root, const char *xml_path,
                                            const svn_entry_t *entries, size_t count,
                                            cbm_svn_observation_t *observation) {
    if (write_status(xml_path, root, entries, count) != 0) {
        return (cbm_svn_probe_result_t)99;
    }
    return parse_file(root, xml_path, observation);
}

static cbm_svn_probe_result_t parse_xml(const char *root, const char *xml_path, const char *xml,
                                        cbm_svn_observation_t *observation) {
    if (write_raw(xml_path, xml) != 0) {
        return (cbm_svn_probe_result_t)99;
    }
    return parse_file(root, xml_path, observation);
}

/* A temp base holding the working copy root ("wc") and, beside it, the
 * status document, so the document is never part of the tree it describes. */
typedef struct {
    char base[1024];
    char root[1024];
    char xml[1024];
} wc_fixture_t;

static bool wc_fixture_init(wc_fixture_t *fixture, const char *prefix) {
    memset(fixture, 0, sizeof(*fixture));
    char *base = th_mktempdir(prefix);
    if (!base) {
        return false;
    }
    snprintf(fixture->base, sizeof(fixture->base), "%s", base);
    snprintf(fixture->root, sizeof(fixture->root), "%s/wc", fixture->base);
    snprintf(fixture->xml, sizeof(fixture->xml), "%s/status.xml", fixture->base);
    return cbm_mkdir_p(fixture->root, 0755);
}

static bool wc_write(const wc_fixture_t *fixture, const char *relative, const char *content) {
    char path[2048];
    snprintf(path, sizeof(path), "%s/%s", fixture->root, relative);
    return th_write_file(path, content) == 0;
}

static bool wc_mkdir(const wc_fixture_t *fixture, const char *relative) {
    char path[2048];
    snprintf(path, sizeof(path), "%s/%s", fixture->root, relative);
    return cbm_mkdir_p(path, 0755);
}

static void wc_path(const wc_fixture_t *fixture, const char *relative, char *out, size_t size) {
    snprintf(out, size, "%s/%s", fixture->root, relative);
}

/* ── Parser ────────────────────────────────────────────────────── */

TEST(parse_content_edit_changes_content_not_semantic_signature) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_content"));
    ASSERT_TRUE(wc_write(&fixture, "source.c", "int value = 1;\n"));
    char source[2048];
    wc_path(&fixture, "source.c", source, sizeof(source));
    svn_entry_t entries[] = {{source, "modified"}};

    cbm_svn_observation_t before = {0};
    ASSERT_EQ(parse_entries(fixture.root, fixture.xml, entries, 1, &before), CBM_SVN_PROBE_OK);
    ASSERT_TRUE(wc_write(&fixture, "source.c", "int value = 2;\n"));
    cbm_svn_observation_t after = {0};
    ASSERT_EQ(parse_entries(fixture.root, fixture.xml, entries, 1, &after), CBM_SVN_PROBE_OK);

    ASSERT_EQ(before.semantic_signature, after.semantic_signature);
    ASSERT_NEQ(before.content_signature, after.content_signature);
    ASSERT_TRUE(before.has_local_changes);
    ASSERT_EQ(after.entry_count, 1);
    ASSERT_EQ(after.candidate_count, 1);
    ASSERT_EQ(after.bytes_hashed, 15);
    th_cleanup(fixture.base);
    PASS();
}

TEST(parse_rejects_declarations_and_truncated_documents) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_xml"));
    cbm_svn_observation_t observation = {0};
    /* An external entity, a bare declaration, and a document cut off mid-entry. */
    ASSERT_EQ(parse_xml(fixture.root, fixture.xml,
                        "<?xml version=\"1.0\"?><!DOCTYPE status [<!ENTITY x SYSTEM "
                        "\"file:///etc/passwd\">]><status><target path=\".\"/></status>",
                        &observation),
              CBM_SVN_PROBE_UNCERTAIN);
    ASSERT_EQ(parse_xml(fixture.root, fixture.xml,
                        "<?xml version=\"1.0\"?><!DOCTYPE/><status><target "
                        "path=\".\"></target></status>",
                        &observation),
              CBM_SVN_PROBE_UNCERTAIN);
    ASSERT_EQ(parse_xml(fixture.root, fixture.xml,
                        "<?xml version=\"1.0\"?><status><target path=\".\"><entry "
                        "path=\"source.c\"><wc-status item=\"modified\"/>",
                        &observation),
              CBM_SVN_PROBE_UNCERTAIN);
    th_cleanup(fixture.base);
    PASS();
}

TEST(parse_clean_props_and_externals_are_not_local_changes) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_clean"));
    char external[2048];
    wc_path(&fixture, "external", external, sizeof(external));
    char xml[8192];
    snprintf(xml, sizeof(xml),
             "<?xml version=\"1.0\"?><status><target path=\"%s\">"
             "<entry path=\"%s\"><wc-status item=\"normal\" props=\"normal\" "
             "revision=\"4\"/></entry>"
             "<entry path=\"%s\"><wc-status item=\"external\" props=\"none\"/></entry>"
             "</target></status>",
             fixture.root, fixture.root, external);
    cbm_svn_observation_t observation = {0};
    ASSERT_EQ(parse_xml(fixture.root, fixture.xml, xml, &observation), CBM_SVN_PROBE_OK);
    ASSERT_FALSE(observation.has_local_changes);
    ASSERT_EQ(observation.entry_count, 2);
    th_cleanup(fixture.base);
    PASS();
}

TEST(parse_reads_svn_1_14_layout_with_nested_commit_and_lock) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_layout"));
    ASSERT_TRUE(wc_write(&fixture, "src/main.c", "int main(void) { return 0; }\n"));
    ASSERT_TRUE(wc_write(&fixture, "src/util.c", "int util;\n"));
    ASSERT_TRUE(wc_write(&fixture, "notes/todo.py", "x = 1\n"));
    char xml[16384];
    snprintf(xml, sizeof(xml),
             "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
             "<status>\n"
             "<target\n   path=\"%s\">\n"
             "<entry\n   path=\"%s\">\n"
             "<wc-status\n   item=\"normal\"\n   revision=\"3\"\n   props=\"none\">\n"
             "<commit\n   revision=\"3\">\n<author>alice</author>\n"
             "<date>2026-10-19T20:00:00.000000Z</date>\n</commit>\n"
             "</wc-status>\n</entry>\n"
             "<entry\n   path=\"%s/src/main.c\">\n"
             "<wc-status\n   props=\"none\"\n   item=\"modified\"\n   revision=\"3\">\n"
             "<commit\n   revision=\"2\">\n<author>bob &amp; carol</author>\n"
             "<date>2026-10-18T09:30:00.000000Z</date>\n</commit>\n"
             "<lock>\n<token>opaquelocktoken:0a1b</token>\n<owner>bob</owner>\n"
             "<created>2026-10-18T09:31:00.000000Z</created>\n</lock>\n"
             "</wc-status>\n</entry>\n"
             "<entry\n   path=\"%s/src/util.c\">\n"
             "<wc-status\n   props=\"modified\"\n   item=\"normal\"\n   revision=\"3\">\n"
             "<commit\n   revision=\"1\">\n<author>alice</author>\n"
             "<date>2026-10-17T08:00:00.000000Z</date>\n</commit>\n"
             "</wc-status>\n</entry>\n"
             "<entry\n   path=\"%s/notes\">\n"
             "<wc-status\n   props=\"none\"\n   item=\"unversioned\">\n"
             "</wc-status>\n</entry>\n"
             "</target>\n"
             "</status>\n",
             fixture.root, fixture.root, fixture.root, fixture.root, fixture.root);
    cbm_svn_observation_t observation = {0};
    ASSERT_EQ(parse_xml(fixture.root, fixture.xml, xml, &observation), CBM_SVN_PROBE_OK);
    ASSERT_EQ(observation.entry_count, 4);
    ASSERT_TRUE(observation.has_local_changes);
    /* main.c (modified) and notes/todo.py (unversioned directory); util.c has
     * only a property change, so its bytes are not read. */
    ASSERT_EQ(observation.candidate_count, 2);
    ASSERT_EQ(observation.bytes_hashed, 29 + 6);
    th_cleanup(fixture.base);
    PASS();
}

TEST(parse_empty_target_is_not_a_working_copy) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_not_wc"));
    cbm_svn_observation_t observation = {0};
    observation.entry_count = 77;
    ASSERT_EQ(parse_entries(fixture.root, fixture.xml, NULL, 0, &observation),
              CBM_SVN_PROBE_NOT_WORKING_COPY);
    ASSERT_EQ(observation.entry_count, 77); /* untouched */
    th_cleanup(fixture.base);
    PASS();
}

TEST(parse_rejects_entries_outside_the_root) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_escape"));
    char outside[2048];
    snprintf(outside, sizeof(outside), "%s/outside.c", fixture.base);
    ASSERT_EQ(th_write_file(outside, "int secret;\n"), 0);

    cbm_svn_observation_t observation = {0};
    svn_entry_t dotdot[] = {{"../outside.c", "modified"}};
    ASSERT_EQ(parse_entries(fixture.root, fixture.xml, dotdot, 1, &observation),
              CBM_SVN_PROBE_UNCERTAIN);
    svn_entry_t absolute[] = {{outside, "modified"}};
    ASSERT_EQ(parse_entries(fixture.root, fixture.xml, absolute, 1, &observation),
              CBM_SVN_PROBE_UNCERTAIN);
    th_cleanup(fixture.base);
    PASS();
}

/* ── Tree walk ─────────────────────────────────────────────────── */

TEST(walk_skips_discovery_excluded_candidate_directory) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_excluded"));
    ASSERT_TRUE(wc_write(&fixture, "node_modules/ignored.js", "function Hidden() {}\n"));
    char directory[2048];
    wc_path(&fixture, "node_modules", directory, sizeof(directory));
    svn_entry_t entries[] = {{directory, "ignored"}};
    cbm_svn_observation_t observation = {0};
    ASSERT_EQ(parse_entries(fixture.root, fixture.xml, entries, 1, &observation), CBM_SVN_PROBE_OK);
    ASSERT_EQ(observation.candidate_count, 0);
    ASSERT_EQ(observation.bytes_hashed, 0);
    th_cleanup(fixture.base);
    PASS();
}

#ifndef _WIN32
/* Symlinks are skipped and counted wherever they appear -- inside an
 * unversioned tree, as a reported entry, and as an ancestor of one -- and
 * nothing they point at is read. */
TEST(walk_skips_and_counts_symlinks_without_following) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_links"));
    char outside_dir[2048];
    char outside_file[2048];
    snprintf(outside_dir, sizeof(outside_dir), "%s/outside", fixture.base);
    snprintf(outside_file, sizeof(outside_file), "%s/outside/secret.c", fixture.base);
    ASSERT_TRUE(cbm_mkdir_p(outside_dir, 0755));
    ASSERT_EQ(th_write_file(outside_file, "int secret_outside_the_root;\n"), 0);
    char inner[2048];
    snprintf(inner, sizeof(inner), "%s/outside/inner.c", fixture.base);
    ASSERT_EQ(th_write_file(inner, "int inner_outside_the_root;\n"), 0);

    ASSERT_TRUE(wc_write(&fixture, "untracked/real.c", "int real;\n"));
    char link_path[2048];
    wc_path(&fixture, "untracked/link_file.c", link_path, sizeof(link_path));
    ASSERT_EQ(symlink(outside_file, link_path), 0);
    wc_path(&fixture, "untracked/link_dir", link_path, sizeof(link_path));
    ASSERT_EQ(symlink(outside_dir, link_path), 0);
    wc_path(&fixture, "direct_link.c", link_path, sizeof(link_path));
    ASSERT_EQ(symlink(outside_file, link_path), 0);
    wc_path(&fixture, "link_parent", link_path, sizeof(link_path));
    ASSERT_EQ(symlink(outside_dir, link_path), 0);

    char untracked[2048];
    char direct[2048];
    char through[2048];
    wc_path(&fixture, "untracked", untracked, sizeof(untracked));
    wc_path(&fixture, "direct_link.c", direct, sizeof(direct));
    wc_path(&fixture, "link_parent/inner.c", through, sizeof(through));
    svn_entry_t entries[] = {
        {untracked, "unversioned"}, {direct, "unversioned"}, {through, "modified"}};
    cbm_svn_observation_t observation = {0};
    ASSERT_EQ(parse_entries(fixture.root, fixture.xml, entries, 3, &observation), CBM_SVN_PROBE_OK);
    ASSERT_EQ(observation.candidate_count, 1);
    ASSERT_EQ(observation.bytes_hashed, 10); /* real.c only */
    ASSERT_EQ(observation.skipped_symlinks, 4);
    th_cleanup(fixture.base);
    PASS();
}

/* A FIFO is skipped and counted, and opening it never waits for a writer. */
TEST(walk_skips_and_counts_fifos_without_blocking) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_fifo"));
    ASSERT_TRUE(wc_write(&fixture, "untracked/ok.c", "int ok;\n"));
    char fifo[2048];
    wc_path(&fixture, "untracked/pipe.c", fifo, sizeof(fifo));
    ASSERT_EQ(mkfifo(fifo, 0644), 0);
    char direct[2048];
    wc_path(&fixture, "direct_pipe.c", direct, sizeof(direct));
    ASSERT_EQ(mkfifo(direct, 0644), 0);

    char untracked[2048];
    wc_path(&fixture, "untracked", untracked, sizeof(untracked));
    svn_entry_t entries[] = {{untracked, "unversioned"}, {direct, "unversioned"}};
    cbm_svn_observation_t observation = {0};
    ASSERT_EQ(parse_entries(fixture.root, fixture.xml, entries, 2, &observation), CBM_SVN_PROBE_OK);
    ASSERT_EQ(observation.candidate_count, 1);
    ASSERT_EQ(observation.skipped_special, 2);
    th_cleanup(fixture.base);
    PASS();
}

/* Under euid 0 permissions do not make a file unreadable; then the file is
 * simply read, and the assertion that matters -- never UNCERTAIN -- holds. */
TEST(walk_skips_and_counts_unreadable_files) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_unreadable"));
    ASSERT_TRUE(wc_write(&fixture, "fine.c", "int fine;\n"));
    ASSERT_TRUE(wc_write(&fixture, "locked.c", "int locked;\n"));
    char fine[2048];
    char locked[2048];
    wc_path(&fixture, "fine.c", fine, sizeof(fine));
    wc_path(&fixture, "locked.c", locked, sizeof(locked));
    ASSERT_EQ(chmod(locked, 0000), 0);
    bool unreadable = access(locked, R_OK) != 0;

    svn_entry_t entries[] = {{fine, "modified"}, {locked, "modified"}};
    cbm_svn_observation_t observation = {0};
    cbm_svn_probe_result_t result =
        parse_entries(fixture.root, fixture.xml, entries, 2, &observation);
    (void)chmod(locked, 0644);
    ASSERT_EQ(result, CBM_SVN_PROBE_OK);
    ASSERT_EQ(observation.skipped_unreadable, unreadable ? 1 : 0);
    ASSERT_EQ(observation.candidate_count, unreadable ? 1 : 2);
    th_cleanup(fixture.base);
    PASS();
}

TEST(walk_accepts_colon_in_posix_names) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_colon"));
    ASSERT_TRUE(wc_write(&fixture, "pkg/a:b.py", "x = 1\n"));
    char path[2048];
    wc_path(&fixture, "pkg/a:b.py", path, sizeof(path));
    svn_entry_t entries[] = {{path, "added"}};
    cbm_svn_observation_t observation = {0};
    ASSERT_EQ(parse_entries(fixture.root, fixture.xml, entries, 1, &observation), CBM_SVN_PROBE_OK);
    ASSERT_EQ(observation.candidate_count, 1);
    ASSERT_EQ(observation.bytes_hashed, 6);
    th_cleanup(fixture.base);
    PASS();
}

TEST(walk_accepts_backslash_in_posix_names) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_backslash"));
    ASSERT_TRUE(wc_write(&fixture, "back\\slash.c", "int b;\n"));
    char path[2048];
    wc_path(&fixture, "back\\slash.c", path, sizeof(path));
    svn_entry_t entries[] = {{path, "unversioned"}};
    cbm_svn_observation_t observation = {0};
    ASSERT_EQ(parse_entries(fixture.root, fixture.xml, entries, 1, &observation), CBM_SVN_PROBE_OK);
    ASSERT_EQ(observation.candidate_count, 1);
    ASSERT_EQ(observation.bytes_hashed, 7);
    th_cleanup(fixture.base);
    PASS();
}
#endif /* !_WIN32 */

static bool set_max_file_bytes(const char *value, char *saved, size_t saved_size, bool *had) {
    const char *current = getenv("CBM_MAX_FILE_BYTES");
    *had = current != NULL;
    snprintf(saved, saved_size, "%s", current ? current : "");
    return cbm_setenv("CBM_MAX_FILE_BYTES", value, 1) == 0;
}

static void restore_max_file_bytes(const char *saved, bool had) {
    if (had) {
        (void)cbm_setenv("CBM_MAX_FILE_BYTES", saved, 1);
    } else {
        (void)cbm_unsetenv("CBM_MAX_FILE_BYTES");
    }
}

TEST(walk_skips_file_measured_over_the_size_cap) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_oversize"));
    ASSERT_TRUE(wc_write(&fixture, "big.c", "/* 0123456789 0123456789 0123456789 0123456789 */\n"));
    ASSERT_TRUE(wc_write(&fixture, "small.c", "int s;\n"));
    char big[2048];
    char small[2048];
    wc_path(&fixture, "big.c", big, sizeof(big));
    wc_path(&fixture, "small.c", small, sizeof(small));
    svn_entry_t entries[] = {{big, "modified"}, {small, "modified"}};

    char saved[64];
    bool had = false;
    ASSERT_TRUE(set_max_file_bytes("32", saved, sizeof(saved), &had));
    cbm_svn_observation_t observation = {0};
    cbm_svn_probe_result_t result =
        parse_entries(fixture.root, fixture.xml, entries, 2, &observation);
    restore_max_file_bytes(saved, had);

    ASSERT_EQ(result, CBM_SVN_PROBE_OK);
    ASSERT_EQ(observation.skipped_oversize, 1);
    ASSERT_EQ(observation.candidate_count, 1);
    ASSERT_EQ(observation.bytes_hashed, 7);
    th_cleanup(fixture.base);
    PASS();
}

static char g_grow_root[1024];

/* Runs after the file was opened and measured under the cap: make it larger
 * than the cap before a single byte is read. */
static void grow_after_measure(const char *relative_path) {
    char path[2048];
    snprintf(path, sizeof(path), "%s/%s", g_grow_root, relative_path);
    FILE *file = cbm_fopen(path, "ab");
    if (file) {
        for (int i = 0; i < 64; i++) {
            fputs("/* growing past the cap after it was measured */\n", file);
        }
        fclose(file);
    }
}

TEST(walk_caps_the_bytes_actually_read) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_grow"));
    ASSERT_TRUE(wc_write(&fixture, "grows.c", "int g;\n"));
    char grows[2048];
    wc_path(&fixture, "grows.c", grows, sizeof(grows));
    snprintf(g_grow_root, sizeof(g_grow_root), "%s", fixture.root);
    svn_entry_t entries[] = {{grows, "modified"}};

    char saved[64];
    bool had = false;
    ASSERT_TRUE(set_max_file_bytes("64", saved, sizeof(saved), &had));
    cbm_svn_test_set_after_measure_hook(grow_after_measure);
    cbm_svn_observation_t observation = {0};
    cbm_svn_probe_result_t result =
        parse_entries(fixture.root, fixture.xml, entries, 1, &observation);
    cbm_svn_test_set_after_measure_hook(NULL);
    restore_max_file_bytes(saved, had);

    ASSERT_EQ(result, CBM_SVN_PROBE_OK);
    ASSERT_EQ(observation.skipped_oversize, 1);
    ASSERT_EQ(observation.candidate_count, 0);
    ASSERT_EQ(observation.bytes_hashed, 0);
    th_cleanup(fixture.base);
    PASS();
}

typedef struct {
    const char *root;
    const char *xml;
    cbm_svn_probe_result_t result;
    cbm_svn_observation_t observation;
} walk_job_t;

static void *walk_job_run(void *arg) {
    walk_job_t *job = (walk_job_t *)arg;
    job->result = parse_file(job->root, job->xml, &job->observation);
    return NULL;
}

enum { DEEP_TREE_LEVELS = 64, SMALL_THREAD_STACK = 128 * 1024 };

/* Tree depth must never reach the native stack. The walk runs on a 128 KB
 * thread through 64 nested unversioned directories: a walker that recursed
 * with path buffers per level needs several times that stack. */
TEST(walk_deep_unversioned_tree_on_a_small_stack) {
    wc_fixture_t fixture;
    ASSERT_TRUE(wc_fixture_init(&fixture, "cbm_svn_deep"));
    char relative[1024] = "deep";
    for (int level = 0; level < DEEP_TREE_LEVELS; level++) {
        size_t used = strlen(relative);
        ASSERT_LT(used + 3, sizeof(relative));
        memcpy(relative + used, "/d", 3);
    }
    ASSERT_TRUE(wc_mkdir(&fixture, relative));
    char leaf[1100];
    snprintf(leaf, sizeof(leaf), "%s/leaf.c", relative);
    ASSERT_TRUE(wc_write(&fixture, leaf, "int leaf;\n"));
    char deep[2048];
    wc_path(&fixture, "deep", deep, sizeof(deep));
    svn_entry_t entries[] = {{deep, "unversioned"}};
    ASSERT_EQ(write_status(fixture.xml, fixture.root, entries, 1), 0);

    walk_job_t job = {fixture.root, fixture.xml, (cbm_svn_probe_result_t)99, {0}};
    cbm_thread_t thread;
    ASSERT_EQ(cbm_thread_create(&thread, SMALL_THREAD_STACK, walk_job_run, &job), 0);
    ASSERT_EQ(cbm_thread_join(&thread), 0);
    ASSERT_EQ(job.result, CBM_SVN_PROBE_OK);
    ASSERT_EQ(job.observation.candidate_count, 1);
    ASSERT_EQ(job.observation.bytes_hashed, 10);
    th_cleanup(fixture.base);
    PASS();
}

/* ── Client resolution ─────────────────────────────────────────── */

static bool make_fake_svn(const char *directory, bool executable) {
    if (!cbm_mkdir_p(directory, 0755)) {
        return false;
    }
    char path[2048];
    snprintf(path, sizeof(path), "%s/%s", directory, FAKE_SVN_NAME);
    /* Never executed: resolution only inspects the file. */
    if (th_write_file(path, "#!/bin/sh\nexit 99\n") != 0) {
        return false;
    }
#ifndef _WIN32
    return chmod(path, executable ? 0755 : 0644) == 0;
#else
    (void)executable;
    return true;
#endif
}

static bool canonical_fake(const char *directory, char *out, size_t size) {
    char path[2048];
    snprintf(path, sizeof(path), "%s/%s", directory, FAKE_SVN_NAME);
    if (!cbm_canonical_path(path, out, size)) {
        return false;
    }
    cbm_normalize_path_sep(out);
    return true;
}

/* Resolve with PATH = path_value from working directory cwd, then restore
 * both before returning, so a failed assertion never leaks either. */
static cbm_svn_probe_result_t init_with(const char *path_value, const char *cwd, const char *root,
                                        cbm_svn_client_t *client) {
    char original_cwd[4096];
    if (!getcwd(original_cwd, sizeof(original_cwd))) {
        return (cbm_svn_probe_result_t)99;
    }
    const char *current = getenv("PATH");
    char *original_path = current ? strdup(current) : NULL;
    if (current && !original_path) {
        return (cbm_svn_probe_result_t)99;
    }
    cbm_svn_probe_result_t result = (cbm_svn_probe_result_t)99;
    if (chdir(cwd) == 0) {
        if (cbm_setenv("PATH", path_value, 1) == 0) {
            result = cbm_svn_client_init(root, client);
        }
        if (chdir(original_cwd) != 0) {
            result = (cbm_svn_probe_result_t)98;
        }
    }
    if (original_path) {
        (void)cbm_setenv("PATH", original_path, 1);
    } else {
        (void)cbm_unsetenv("PATH");
    }
    free(original_path);
    return result;
}

typedef struct {
    char base[1024];
    char watched[1100];
    char hostile[1100];
    char trusted[1100];
    char elsewhere[1100]; /* a working directory that contains no candidate */
} client_fixture_t;

static bool client_fixture_init(client_fixture_t *fixture, const char *prefix) {
    memset(fixture, 0, sizeof(*fixture));
    char *base = th_mktempdir(prefix);
    if (!base) {
        return false;
    }
    snprintf(fixture->base, sizeof(fixture->base), "%s", base);
    snprintf(fixture->watched, sizeof(fixture->watched), "%s/watched", fixture->base);
    snprintf(fixture->hostile, sizeof(fixture->hostile), "%s/hostile", fixture->base);
    snprintf(fixture->trusted, sizeof(fixture->trusted), "%s/trusted", fixture->base);
    snprintf(fixture->elsewhere, sizeof(fixture->elsewhere), "%s/elsewhere", fixture->base);
    return cbm_mkdir_p(fixture->watched, 0755) && cbm_mkdir_p(fixture->elsewhere, 0755) &&
           make_fake_svn(fixture->hostile, true) && make_fake_svn(fixture->trusted, true);
}

TEST(client_init_refuses_svn_in_the_current_directory) {
    client_fixture_t fixture;
    ASSERT_TRUE(client_fixture_init(&fixture, "cbm_svn_client_cwd"));
    char path_value[4096];
    snprintf(path_value, sizeof(path_value), "%s" PATH_LIST_DELIM "%s", fixture.hostile,
             fixture.trusted);
    cbm_svn_client_t client;
    ASSERT_EQ(init_with(path_value, fixture.hostile, fixture.watched, &client), CBM_SVN_PROBE_OK);
    char expected[4096];
    ASSERT_TRUE(canonical_fake(fixture.trusted, expected, sizeof(expected)));
    ASSERT_STR_EQ(client.executable, expected);
    th_cleanup(fixture.base);
    PASS();
}

TEST(client_init_refuses_svn_under_the_watched_root) {
    client_fixture_t fixture;
    ASSERT_TRUE(client_fixture_init(&fixture, "cbm_svn_client_root"));
    char planted[1200];
    snprintf(planted, sizeof(planted), "%s/tools/bin", fixture.watched);
    ASSERT_TRUE(make_fake_svn(planted, true));
    char path_value[4096];
    snprintf(path_value, sizeof(path_value), "%s" PATH_LIST_DELIM "%s", planted, fixture.trusted);
    cbm_svn_client_t client;
    ASSERT_EQ(init_with(path_value, fixture.elsewhere, fixture.watched, &client), CBM_SVN_PROBE_OK);
    char expected[4096];
    ASSERT_TRUE(canonical_fake(fixture.trusted, expected, sizeof(expected)));
    ASSERT_STR_EQ(client.executable, expected);
    th_cleanup(fixture.base);
    PASS();
}

TEST(client_init_ignores_relative_path_entries) {
    client_fixture_t fixture;
    ASSERT_TRUE(client_fixture_init(&fixture, "cbm_svn_client_rel"));
    char inner[1200];
    char sibling[1200];
    snprintf(inner, sizeof(inner), "%s/inner", fixture.base);
    snprintf(sibling, sizeof(sibling), "%s/sibling", fixture.base);
    ASSERT_TRUE(cbm_mkdir_p(inner, 0755));
    ASSERT_TRUE(make_fake_svn(sibling, true));
    /* "../sibling" resolves outside both the root and the working directory,
     * so only the absolute-entry rule refuses it. */
    char path_value[4096];
    snprintf(path_value, sizeof(path_value), "../sibling" PATH_LIST_DELIM "%s", fixture.trusted);
    cbm_svn_client_t client;
    ASSERT_EQ(init_with(path_value, inner, fixture.watched, &client), CBM_SVN_PROBE_OK);
    char expected[4096];
    ASSERT_TRUE(canonical_fake(fixture.trusted, expected, sizeof(expected)));
    ASSERT_STR_EQ(client.executable, expected);
    th_cleanup(fixture.base);
    PASS();
}

TEST(client_init_is_uncertain_when_every_candidate_is_refused) {
    client_fixture_t fixture;
    ASSERT_TRUE(client_fixture_init(&fixture, "cbm_svn_client_none"));
    char path_value[4096];
    snprintf(path_value, sizeof(path_value), "%s" PATH_LIST_DELIM ".", fixture.hostile);
    cbm_svn_client_t client;
    ASSERT_EQ(init_with(path_value, fixture.hostile, fixture.watched, &client),
              CBM_SVN_PROBE_UNCERTAIN);
    ASSERT_STR_EQ(client.executable, "");
    th_cleanup(fixture.base);
    PASS();
}

#ifndef _WIN32
TEST(client_init_skips_svn_without_execute_permission) {
    client_fixture_t fixture;
    ASSERT_TRUE(client_fixture_init(&fixture, "cbm_svn_client_noexec"));
    char plain[1200];
    snprintf(plain, sizeof(plain), "%s/plain", fixture.base);
    ASSERT_TRUE(make_fake_svn(plain, false));
    char path_value[4096];
    snprintf(path_value, sizeof(path_value), "%s" PATH_LIST_DELIM "%s", plain, fixture.trusted);
    cbm_svn_client_t client;
    ASSERT_EQ(init_with(path_value, fixture.elsewhere, fixture.watched, &client), CBM_SVN_PROBE_OK);
    char expected[4096];
    ASSERT_TRUE(canonical_fake(fixture.trusted, expected, sizeof(expected)));
    ASSERT_STR_EQ(client.executable, expected);
    th_cleanup(fixture.base);
    PASS();
}
#endif

static bool make_client_layout(const char *base, const char *bin, const char *runtime_dll,
                               char *executable, size_t size) {
    char directory[2048];
    snprintf(directory, sizeof(directory), "%s/%s", base, bin);
    if (!cbm_mkdir_p(directory, 0755)) {
        return false;
    }
    snprintf(executable, size, "%s/svn.exe", directory);
    if (th_write_file(executable, "MZ") != 0) {
        return false;
    }
    if (runtime_dll) {
        char dll[2100];
        snprintf(dll, sizeof(dll), "%s/%s", directory, runtime_dll);
        return th_write_file(dll, "MZ") == 0;
    }
    return true;
}

/* The runtime DLL beside the client decides its path spelling; the install
 * prefix never does. */
TEST(client_path_style_follows_the_runtime_dll_not_the_prefix) {
    char *base = th_mktempdir("cbm_svn_style");
    ASSERT_NOT_NULL(base);
    char root[1024];
    snprintf(root, sizeof(root), "%s", base);
    char msys[2048];
    char cygwin[2048];
    char native_usr[2048];
    char native[2048];
    ASSERT_TRUE(make_client_layout(root, "msys64/usr/bin", "msys-2.0.dll", msys, sizeof(msys)));
    ASSERT_TRUE(make_client_layout(root, "cygwin64/bin", "cygwin1.dll", cygwin, sizeof(cygwin)));
    ASSERT_TRUE(make_client_layout(root, "portable/usr/bin", NULL, native_usr, sizeof(native_usr)));
    ASSERT_TRUE(make_client_layout(root, "SlikSvn/bin", NULL, native, sizeof(native)));

    ASSERT_EQ(cbm_svn_test_path_style_for_executable(msys), CBM_SVN_PATHS_MSYS);
    ASSERT_EQ(cbm_svn_test_path_style_for_executable(cygwin), CBM_SVN_PATHS_CYGWIN);
    ASSERT_EQ(cbm_svn_test_path_style_for_executable(native_usr), CBM_SVN_PATHS_NATIVE);
    ASSERT_EQ(cbm_svn_test_path_style_for_executable(native), CBM_SVN_PATHS_NATIVE);
    th_cleanup(root);
    PASS();
}

TEST(format_path_arg_matches_the_client_runtime) {
    cbm_svn_client_t client = {0};
    char out[256];
    client.path_style = CBM_SVN_PATHS_MSYS;
    ASSERT_TRUE(cbm_svn_format_path_arg(&client, "C:/R&D/%project%", out, sizeof(out)));
    ASSERT_STR_EQ(out, "/c/R&D/%project%");
    client.path_style = CBM_SVN_PATHS_CYGWIN;
    ASSERT_TRUE(cbm_svn_format_path_arg(&client, "D:/R&D/%project%", out, sizeof(out)));
    ASSERT_STR_EQ(out, "/cygdrive/d/R&D/%project%");
    client.path_style = CBM_SVN_PATHS_NATIVE;
    ASSERT_TRUE(cbm_svn_format_path_arg(&client, "C:/R&D/%project%", out, sizeof(out)));
    ASSERT_STR_EQ(out, "C:/R&D/%project%");
    client.path_style = CBM_SVN_PATHS_MSYS;
    ASSERT_TRUE(cbm_svn_format_path_arg(&client, "/workspace/project", out, sizeof(out)));
    ASSERT_STR_EQ(out, "/workspace/project");
#ifdef _WIN32
    ASSERT_TRUE(cbm_svn_format_path_arg(&client, "D:\\R&D\\%project%", out, sizeof(out)));
    ASSERT_STR_EQ(out, "/d/R&D/%project%");
#endif
    ASSERT_FALSE(cbm_svn_format_path_arg(&client, "C:/x", out, 4));
    PASS();
}

SUITE(svn_state) {
    RUN_TEST(parse_content_edit_changes_content_not_semantic_signature);
    RUN_TEST(parse_rejects_declarations_and_truncated_documents);
    RUN_TEST(parse_clean_props_and_externals_are_not_local_changes);
    RUN_TEST(parse_reads_svn_1_14_layout_with_nested_commit_and_lock);
    RUN_TEST(parse_empty_target_is_not_a_working_copy);
    RUN_TEST(parse_rejects_entries_outside_the_root);
    RUN_TEST(walk_skips_discovery_excluded_candidate_directory);
#ifndef _WIN32
    RUN_TEST(walk_skips_and_counts_symlinks_without_following);
    RUN_TEST(walk_skips_and_counts_fifos_without_blocking);
    RUN_TEST(walk_skips_and_counts_unreadable_files);
    RUN_TEST(walk_accepts_colon_in_posix_names);
    RUN_TEST(walk_accepts_backslash_in_posix_names);
#endif
    RUN_TEST(walk_skips_file_measured_over_the_size_cap);
    RUN_TEST(walk_caps_the_bytes_actually_read);
    RUN_TEST(client_init_refuses_svn_in_the_current_directory);
    RUN_TEST(client_init_refuses_svn_under_the_watched_root);
    RUN_TEST(client_init_ignores_relative_path_entries);
    RUN_TEST(client_init_is_uncertain_when_every_candidate_is_refused);
#ifndef _WIN32
    RUN_TEST(client_init_skips_svn_without_execute_permission);
#endif
    RUN_TEST(client_path_style_follows_the_runtime_dll_not_the_prefix);
    RUN_TEST(format_path_arg_matches_the_client_runtime);
    RUN_TEST(walk_deep_unversioned_tree_on_a_small_stack);
}
