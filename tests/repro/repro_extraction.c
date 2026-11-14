/*
 * repro_extraction.c — Reproduce-first cases for OPEN extraction-quality bugs.
 *
 * Each TEST() asserts the CORRECT behaviour and is RED until the bug is fixed.
 * Keep one TEST() per issue; name it repro_issue<N>_<slug> and lead with a
 * comment naming the issue, the root cause, and expected-vs-actual.
 *
 * Cluster (TIER A, in-process via cbm_extract_file):
 *   #554 — C++ out-of-line method CALLS source = Module, not enclosing Method
 *   (more added per wave: #495 #521 #382 #408 #523 #56 #333)
 *   #2998 — COBOL external scanner livelock at EOF (hang, not a wrong edge)
 */
#include "test_framework.h"
#include "cbm.h"
#include <string.h>
#if !defined(_WIN32)
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

/* Convenience: extract, return result (caller frees). Mirrors test_extraction.c. */
static CBMFileResult *rx(const char *src, CBMLanguage lang, const char *proj, const char *path) {
    return cbm_extract_file(src, (int)strlen(src), lang, proj, path, 0, NULL, NULL);
}

/* Find the first definition matching label+name (either may be NULL = wildcard). */
static CBMDefinition *find_def(CBMFileResult *r, const char *label, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (label && (!d->label || strcmp(d->label, label) != 0))
            continue;
        if (name && (!d->name || strcmp(d->name, name) != 0))
            continue;
        return d;
    }
    return NULL;
}

/* ───────────────────────────────────────────────────────────────────
 * #554 — C++ out-of-line method definitions: the CALLS edge source falls
 * back to the Module (file-level) instead of the enclosing Method.
 *
 * Root cause (#621 follow-up to #463/adc8304): for `void Foo::bar() { helper(); }`
 * the inner call's `enclosing_func_qn` drops the CLASS qualifier — it resolves to
 * the bare method name (e.g. "t.m.bar") instead of the method node's full
 * class-qualified QN (e.g. "t.m.Foo.bar"). The pre-existing guard in
 * test_extraction.c only checks `enclosing_func_qn != "t.m"` (module), which a
 * buggy "t.m.bar" PASSES — so it never caught the class-qualifier drop.
 *
 * Strong reproduction: tie the call's enclosing_func_qn to the METHOD DEFINITION's
 * own qualified_name (format-agnostic) AND require the class qualifier be present.
 * Expected: enclosing_func_qn == def(bar).qualified_name, and that QN names "Foo".
 * Actual (buggy): enclosing_func_qn loses "Foo" → mismatch → RED.
 * ─────────────────────────────────────────────────────────────────── */
TEST(repro_issue554_cpp_out_of_line_method_class_qualified) {
    CBMFileResult *r = rx("struct Foo { void bar(); };\n"
                          "int helper(int x) { return x; }\n"
                          "void Foo::bar() { helper(1); }\n",
                          CBM_LANG_CPP, "t", "m.cpp");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    /* The out-of-line method definition: its qualified_name is the ground truth
     * the inner CALLS edge must point at. */
    CBMDefinition *method = find_def(r, "Method", "bar");
    if (!method)
        method = find_def(r, NULL, "bar"); /* tolerate label variance */
    ASSERT_NOT_NULL(method);
    ASSERT_NOT_NULL(method->qualified_name);

    /* The method node must carry the class qualifier — either embedded in the QN
     * or via parent_class. This is the heart of #554/#621. */
    int qn_has_class = strstr(method->qualified_name, "Foo") != NULL;
    int parent_has_class = method->parent_class && strstr(method->parent_class, "Foo") != NULL;
    ASSERT_TRUE(qn_has_class || parent_has_class);

    /* The helper() call inside Foo::bar must attribute to the method node, i.e.
     * its enclosing_func_qn must EQUAL the method's qualified_name (class included),
     * not the bare method name and not the module. */
    int saw_helper = 0;
    for (int i = 0; i < r->calls.count; i++) {
        if (strcmp(r->calls.items[i].callee_name, "helper") == 0) {
            saw_helper = 1;
            const char *enc = r->calls.items[i].enclosing_func_qn;
            ASSERT_NOT_NULL(enc);
            ASSERT_STR_EQ(enc, method->qualified_name);
            ASSERT_TRUE(strstr(enc, "Foo") != NULL); /* class qualifier preserved */
        }
    }
    ASSERT_TRUE(saw_helper);

    cbm_free_result(r);
    PASS();
}

/* ───────────────────────────────────────────────────────────────────
 * #2998 — COBOL external scanner livelocks the parser at EOF.
 *
 * Root cause: the vendored COBOL scanner's LINE_PREFIX_COMMENT loop advanced
 * while get_column() <= 5 without checking lookahead == 0. tree-sitter's
 * advance() is a no-op at EOF (the column never increments), so any input that
 * reaches EOF at column <= 5 — e.g. a .cob file shorter than 6 bytes, or the
 * virtual empty line after a trailing "\n" in the right parser state — spun
 * the scanner forever at 100% CPU. A second hazard emitted a zero-length
 * COMMENT_ENTRY token when start_with_word() bailed at "\n"/EOF without
 * consuming, another livelock class.
 *
 * Expected: extraction RETURNS (the hang itself is the failure mode; the
 * fork+deadline harness below turns a regression back into a RED test instead
 * of a hung CI job), and the canonical valid-program fixture still extracts
 * PROGRAM-ID. Actual (buggy): never returns; last worker-log line
 * "pass.start pass=definitions".
 * ─────────────────────────────────────────────────────────────────── */
#if !defined(_WIN32)
/* Parse src in a forked child under a wall-clock deadline.
 * Returns the child's exit code (0 = extraction returned), -1 on timeout
 * (child killed; i.e. the livelock reproduced), -2 on fork failure. */
static int repro2998_parse_with_deadline(const char *src, CBMLanguage lang, const char *path) {
    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0)
        return -2;
    if (pid == 0) {
        CBMFileResult *r = cbm_extract_file(src, (int)strlen(src), lang, "t", path, 0, NULL, NULL);
        if (r)
            cbm_free_result(r);
        _exit(0); /* reaching _exit at all is what the bug took away */
    }
    int status = 0;
    for (int i = 0; i < 1000; i++) { /* 10 s in 10 ms steps */
        pid_t got = waitpid(pid, &status, WNOHANG);
        if (got == pid)
            return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
        usleep(10 * 1000);
    }
    kill(pid, SIGKILL);
    waitpid(pid, &status, 0);
    return -1;
}

TEST(repro_issue2998_cobol_scanner_eof_livelock) {
    /* EOF lands inside the LINE_PREFIX_COMMENT column scan: shorter than
     * column 6, or a trailing newline leaving the lexer at column 0 of the
     * virtual empty last line. */
    ASSERT_EQ(repro2998_parse_with_deadline("X", CBM_LANG_COBOL, "m.cob"), 0);
    ASSERT_EQ(repro2998_parse_with_deadline("*<<", CBM_LANG_COBOL, "m.cob"), 0);
    ASSERT_EQ(repro2998_parse_with_deadline("X\n", CBM_LANG_COBOL, "m.cob"), 0);
    ASSERT_EQ(repro2998_parse_with_deadline("display 1.\n", CBM_LANG_COBOL, "m.cob"), 0);
    PASS();
}

TEST(repro_issue2998_cobol_valid_program_still_extracts) {
    /* The EOF/zero-length guards must not break the happy path: the canonical
     * fixed-format fixture (same as the cobol row in grammar_cases.h) still
     * yields its PROGRAM-ID definition. */
    CBMFileResult *r = rx("       IDENTIFICATION DIVISION.\n"
                          "       PROGRAM-ID. FOO.\n"
                          "       PROCEDURE DIVISION.\n"
                          "           MAIN-PARA.\n"
                          "               STOP RUN.\n",
                          CBM_LANG_COBOL, "t", "a.cob");
    ASSERT_NOT_NULL(r);
    int saw_foo = 0;
    for (int i = 0; r && i < r->defs.count; i++)
        if (r->defs.items[i].name && strcmp(r->defs.items[i].name, "FOO") == 0)
            saw_foo = 1;
    cbm_free_result(r);
    ASSERT_TRUE(saw_foo);
    PASS();
}
#endif /* !_WIN32 */

/* ── Suite ──────────────────────────────────────────────────────── */
SUITE(repro_extraction) {
    RUN_TEST(repro_issue554_cpp_out_of_line_method_class_qualified);
#if !defined(_WIN32)
    RUN_TEST(repro_issue2998_cobol_scanner_eof_livelock);
    RUN_TEST(repro_issue2998_cobol_valid_program_still_extracts);
#endif
}
