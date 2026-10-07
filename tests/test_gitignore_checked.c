/* Independent tests for the frozen exact-byte checked-ignore API.
 * No loader, global hooks, allocation pressure, timing races or admission claims. */
#include "test_framework.h"
#include "discover/discover.h"
#include "discover/gitignore_checked.h"
#include "foundation/arena.h"
#include "foundation/platform.h"
#include <limits.h>
#include <stdint.h>

/* Existing legacy symbol; discover.h currently declares the bool wrapper only. */
extern int cbm_gitignore_match_result(const cbm_gitignore_t *, const char *, bool);

#define GIC_CHECK(expression)                                                           \
    do {                                                                                \
        if (!(expression)) {                                                            \
            fprintf(stderr, "checked-ignore assertion %s:%d: %s\n", __FILE__, __LINE__, \
                    #expression);                                                       \
            goto done;                                                                  \
        }                                                                               \
    } while (0)

typedef struct {
    CBMArena input;
    cbm_ignore_checked_t *owner[3];
} gic_fixture;

static void gic_init(gic_fixture *f) {
    memset(f, 0, sizeof(*f));
    cbm_arena_init(&f->input);
}

static int gic_finish(gic_fixture *f, int result) {
    for (size_t i = 0; i < 3; i++)
        cbm_ignore_checked_free(f->owner[i]);
    cbm_arena_destroy(&f->input);
    return result;
}

static cbm_ignore_checked_limits_t gic_limits(void) {
    return (cbm_ignore_checked_limits_t){.max_bytes = 4 * 1024 * 1024,
                                         .max_work = 100000000,
                                         .max_patterns = 1024,
                                         .max_arena_bytes = 4 * 1024 * 1024,
                                         .max_depth = CBM_IGNORE_CHECKED_DEPTH_MAX};
}

static cbm_ignore_checked_control_t gic_control(void) {
    return (cbm_ignore_checked_control_t){.deadline_ms = UINT64_MAX};
}

static bool gic_status(cbm_ignore_checked_status_t actual, cbm_ignore_checked_status_t expected,
                       const cbm_ignore_checked_error_t *error) {
    if (actual == expected && error->status == expected &&
        (expected == CBM_IGNORE_CHECKED_LIMIT || error->cap == CBM_IGNORE_CAP_NONE))
        return true;
    fprintf(stderr, "checked-ignore status actual=%d expected=%d error=%d cap=%d offset=%zu\n",
            (int)actual, (int)expected, (int)error->status, (int)error->cap, error->byte_offset);
    return false;
}

static bool gic_open(cbm_ignore_checked_t **owner, const cbm_ignore_checked_limits_t *limits) {
    cbm_ignore_checked_error_t error;
    memset(&error, 0xa5, sizeof(error));
    bool ok =
        gic_status(cbm_ignore_checked_open(limits, owner, &error), CBM_IGNORE_CHECKED_OK, &error) &&
        *owner;
    if (!ok) {
        cbm_ignore_checked_free(*owner);
        *owner = NULL;
    }
    return ok;
}

static bool gic_parse(cbm_ignore_checked_t *owner, const void *bytes, size_t length, size_t count,
                      const cbm_ignore_program_t **program) {
    cbm_ignore_checked_control_t control = gic_control();
    cbm_ignore_checked_parse_result_t out = {0};
    cbm_ignore_checked_error_t error;
    cbm_ignore_checked_status_t status =
        cbm_ignore_checked_parse(owner, bytes, length, &control, &out, &error);
    if (!gic_status(status, CBM_IGNORE_CHECKED_OK, &error) || !out.program ||
        out.pattern_count != count)
        return false;
    *program = out.program;
    return true;
}

static bool gic_match(cbm_ignore_checked_t *owner, const cbm_ignore_program_t *program,
                      const char *path, bool directory, cbm_ignore_decision_t expected) {
    cbm_ignore_checked_control_t control = gic_control();
    cbm_ignore_checked_error_t error;
    cbm_ignore_decision_t decision = CBM_IGNORE_UNAVAILABLE;
    cbm_ignore_checked_status_t status = cbm_ignore_checked_match(
        owner, program, path, strlen(path), directory, &control, &decision, &error);
    return gic_status(status, CBM_IGNORE_CHECKED_OK, &error) && decision == expected;
}

static bool gic_usage_equal(const cbm_ignore_checked_usage_t *a,
                            const cbm_ignore_checked_usage_t *b) {
    return a->bytes_reserved == b->bytes_reserved && a->work_used == b->work_used &&
           a->patterns_reserved == b->patterns_reserved &&
           a->arena_requested_bytes == b->arena_requested_bytes &&
           a->terminal_status == b->terminal_status;
}

typedef struct {
    const char *patterns, *path;
    bool directory;
    size_t count;
    cbm_ignore_decision_t expected;
} gic_semantic;

static bool gic_semantic_one(const gic_semantic *row) {
    /* Legacy controls run first, so a broken fixture is not a feature failure. */
    cbm_gitignore_t *legacy = cbm_gitignore_parse(row->patterns);
    if (!legacy)
        return false;
    int old = cbm_gitignore_match_result(legacy, row->path, row->directory);
    bool old_bool = cbm_gitignore_matches(legacy, row->path, row->directory);
    cbm_gitignore_free(legacy);
    if (old != (int)row->expected || old_bool != (row->expected == CBM_IGNORE_IGNORED))
        return false;
    cbm_ignore_checked_t *owner = NULL;
    cbm_ignore_checked_limits_t limits = gic_limits();
    const cbm_ignore_program_t *program = NULL;
    bool ok = gic_open(&owner, &limits) &&
              gic_parse(owner, row->patterns, strlen(row->patterns), row->count, &program) &&
              gic_match(owner, program, row->path, row->directory, row->expected);
    cbm_ignore_checked_free(owner);
    return ok;
}

TEST(gic_a_semantics_and_legacy_parity) {
    const gic_semantic rows[] = {
        {"", "foo", false, 0, CBM_IGNORE_NO_OPINION},
        {"#comment\n\n \t\r\n!\n/\n!/\n", "foo", false, 0, CBM_IGNORE_NO_OPINION},
        {"*.tmp \t\r\n!keep.tmp\r\n", "keep.tmp", false, 2, CBM_IGNORE_REINCLUDED},
        {"*.tmp \t\r\n!keep.tmp\r\n", "deep/x.tmp", false, 2, CBM_IGNORE_IGNORED},
        {" #literal\n", " #literal", false, 1, CBM_IGNORE_IGNORED},
        {"a\rb", "a\rb", false, 1, CBM_IGNORE_IGNORED},
        {"x\nx\n!x\nx", "x", false, 4, CBM_IGNORE_IGNORED},
        {"!x", "x", false, 1, CBM_IGNORE_REINCLUDED},
        {"/build", "src/build", true, 1, CBM_IGNORE_NO_OPINION},
        {"/build", "build", true, 1, CBM_IGNORE_IGNORED},
        {"doc/frotz", "src/doc/frotz", false, 1, CBM_IGNORE_NO_OPINION},
        {"doc/frotz", "doc/frotz", false, 1, CBM_IGNORE_IGNORED},
        {"build/", "deep/build", true, 1, CBM_IGNORE_IGNORED},
        {"build/", "build", false, 1, CBM_IGNORE_NO_OPINION},
        {"build/", "build/x", false, 1, CBM_IGNORE_NO_OPINION},
        {"**/build", "a/b/build", true, 1, CBM_IGNORE_IGNORED},
        {"a/**/b", "a/b", false, 1, CBM_IGNORE_IGNORED},
        {"logs/**", "logs/a/b", false, 1, CBM_IGNORE_IGNORED},
        {"a**b", "a/x/b", false, 1, CBM_IGNORE_IGNORED},
        {"/a*b", "a/x/b", false, 1, CBM_IGNORE_NO_OPINION},
        {"file?.txt", "file12.txt", false, 1, CBM_IGNORE_NO_OPINION},
        {"file[0-9].txt", "file3.txt", false, 1, CBM_IGNORE_IGNORED},
        {"[a", "a", false, 1, CBM_IGNORE_IGNORED},
        {"[]", "a", false, 1, CBM_IGNORE_NO_OPINION},
        {"[!]", "z", false, 1, CBM_IGNORE_IGNORED},
        {"[^a]", "a", false, 1, CBM_IGNORE_NO_OPINION},
        {"src[.-0]test.c", "src/test.c", false, 1, CBM_IGNORE_NO_OPINION},
        {"src[!x]test.c", "src/test.c", false, 1, CBM_IGNORE_NO_OPINION},
        {"src[/]test.c", "src/test.c", false, 1, CBM_IGNORE_NO_OPINION},
        {"\\#literal", "\\#literal", false, 1, CBM_IGNORE_IGNORED},
        {"\\!literal", "!literal", false, 1, CBM_IGNORE_NO_OPINION},
        {"tail\\ \t\r\n", "tail\\", false, 1, CBM_IGNORE_IGNORED},
        {"/x", "./x", false, 1, CBM_IGNORE_NO_OPINION},
        {"a/b", "a//b", false, 1, CBM_IGNORE_NO_OPINION},
        {"\x80", "\x80", false, 1, CBM_IGNORE_IGNORED},
        {"[\x80-z]", "A", false, 1, CHAR_MIN < 0 ? CBM_IGNORE_IGNORED : CBM_IGNORE_NO_OPINION},
        {"*", "", false, 1, CBM_IGNORE_IGNORED},
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        if (!gic_semantic_one(&rows[i])) {
            fprintf(stderr, "checked-ignore semantic row=%zu\n", i);
            return 1;
        }
    }
    PASS();
}

/* A latched failure leaves every accounting field and earlier program intact;
 * neither an ordinary parse nor a match may turn it into successful no-opinion. */
static bool gic_terminal(cbm_ignore_checked_t *owner, const cbm_ignore_program_t *old_program,
                         cbm_ignore_checked_status_t expected) {
    cbm_ignore_checked_usage_t before, after;
    if (!cbm_ignore_checked_usage(owner, &before) || before.terminal_status != expected)
        return false;
    cbm_ignore_checked_control_t control = gic_control();
    cbm_ignore_checked_parse_result_t out = {.program = old_program, .pattern_count = 77};
    cbm_ignore_checked_error_t error;
    if (!gic_status(cbm_ignore_checked_parse(owner, "x", 1, &control, &out, &error), expected,
                    &error) ||
        out.program || out.pattern_count)
        return false;
    if (old_program) {
        cbm_ignore_decision_t decision = CBM_IGNORE_IGNORED;
        if (!gic_status(cbm_ignore_checked_match(owner, old_program, "x", 1, false, &control,
                                                 &decision, &error),
                        expected, &error) ||
            decision != CBM_IGNORE_UNAVAILABLE)
            return false;
    }
    return cbm_ignore_checked_usage(owner, &after) && gic_usage_equal(&before, &after);
}

static bool gic_bad_span(cbm_ignore_checked_t *owner, const cbm_ignore_program_t *old_program,
                         unsigned kind) {
    cbm_ignore_checked_usage_t before, after;
    if (!cbm_ignore_checked_usage(owner, &before))
        return false;
    cbm_ignore_checked_control_t control = gic_control();
    cbm_ignore_checked_error_t error;
    cbm_ignore_checked_status_t status;
    if (kind == 0) {
        const unsigned char input[] = {'x', '\n', 0, 'y'};
        cbm_ignore_checked_parse_result_t out = {.program = old_program, .pattern_count = 77};
        status = cbm_ignore_checked_parse(owner, input, sizeof(input), &control, &out, &error);
        if (out.program || out.pattern_count)
            return false;
    } else {
        const char missing[] = {'x', 'y'};
        const char embedded[] = {'x', 0, 'y', 0};
        cbm_ignore_decision_t decision = CBM_IGNORE_IGNORED;
        status = cbm_ignore_checked_match(owner, old_program, kind == 1 ? missing : embedded,
                                          kind == 1 ? 1 : 3, false, &control, &decision, &error);
        if (decision != CBM_IGNORE_UNAVAILABLE)
            return false;
    }
    uint64_t reserved = kind == 1 ? 2 : 4;
    return cbm_ignore_checked_usage(owner, &after) &&
           after.bytes_reserved == before.bytes_reserved + reserved &&
           gic_status(status, CBM_IGNORE_CHECKED_UNSUPPORTED, &error) &&
           gic_terminal(owner, old_program, CBM_IGNORE_CHECKED_UNSUPPORTED);
}

TEST(gic_b_exact_spans_and_input_lifetime) {
    gic_fixture f;
    gic_init(&f);
    int result = 1;
    cbm_ignore_checked_limits_t limits = gic_limits();
    const cbm_ignore_program_t *program = NULL, *empty = NULL;
    GIC_CHECK(gic_open(&f.owner[0], &limits));
    GIC_CHECK(gic_parse(f.owner[0], NULL, 0, 0, &empty));
    GIC_CHECK(gic_match(f.owner[0], empty, "", false, CBM_IGNORE_NO_OPINION));
    unsigned char *input = cbm_arena_alloc(&f.input, 4);
    GIC_CHECK(input);
    memcpy(input, "a\nbX", 4);
    GIC_CHECK(gic_parse(f.owner[0], input, 3, 2, &program));
    memset(input, 0xa5, 4);
    cbm_arena_destroy(&f.input);
    cbm_arena_init(&f.input);
    GIC_CHECK(gic_match(f.owner[0], program, "a", false, CBM_IGNORE_IGNORED));
    GIC_CHECK(gic_match(f.owner[0], program, "b", false, CBM_IGNORE_IGNORED));
    GIC_CHECK(gic_match(f.owner[0], program, "bX", false, CBM_IGNORE_NO_OPINION));
    cbm_ignore_checked_free(f.owner[0]);
    f.owner[0] = NULL;
    for (unsigned kind = 0; kind < 3; kind++) {
        GIC_CHECK(gic_open(&f.owner[0], &limits));
        GIC_CHECK(gic_parse(f.owner[0], "x", 1, 1, &program));
        GIC_CHECK(gic_match(f.owner[0], program, "x", false, CBM_IGNORE_IGNORED));
        GIC_CHECK(gic_bad_span(f.owner[0], program, kind));
        cbm_ignore_checked_free(f.owner[0]);
        f.owner[0] = NULL;
    }
    result = 0;
done:
    return gic_finish(&f, result);
}

typedef struct {
    size_t calls, stop;
} gic_counter;
static bool gic_cancel(void *context) {
    gic_counter *counter = context;
    counter->calls++;
    return counter->stop && counter->calls >= counter->stop;
}

static bool gic_invalid_parse_call(cbm_ignore_checked_t *owner, const cbm_ignore_program_t *program,
                                   unsigned kind, cbm_ignore_checked_control_t *control,
                                   cbm_ignore_checked_error_t *error,
                                   cbm_ignore_checked_status_t *status) {
    if (kind == 3)
        control->deadline_ms = 0;
    cbm_ignore_checked_parse_result_t out = {.program = program, .pattern_count = 77};
    *status = cbm_ignore_checked_parse(kind == 0 ? NULL : owner, kind == 1 ? NULL : "x", 1,
                                       kind == 2 ? NULL : control, kind == 4 ? NULL : &out, error);
    return kind == 4 || (!out.program && out.pattern_count == 0);
}

static bool gic_invalid_match_call(cbm_ignore_checked_t *owner, const cbm_ignore_program_t *program,
                                   const cbm_ignore_program_t *foreign, unsigned kind,
                                   cbm_ignore_checked_control_t *control,
                                   cbm_ignore_checked_error_t *error,
                                   cbm_ignore_checked_status_t *status) {
    if (kind == 11)
        control->deadline_ms = 0;
    const cbm_ignore_program_t *chosen = program;
    if (kind == 5)
        chosen = foreign;
    if (kind == 6)
        chosen = NULL;
    cbm_ignore_decision_t decision = CBM_IGNORE_IGNORED;
    *status = cbm_ignore_checked_match(kind == 12 ? NULL : owner, chosen, kind == 7 ? NULL : "x",
                                       kind == 8 ? SIZE_MAX : 1, false, kind == 9 ? NULL : control,
                                       kind == 10 ? NULL : &decision, error);
    return kind == 10 || decision == CBM_IGNORE_UNAVAILABLE;
}

static bool gic_preflight(cbm_ignore_checked_t *owner, const cbm_ignore_program_t *program,
                          const cbm_ignore_program_t *foreign, unsigned kind) {
    cbm_ignore_checked_usage_t before, after;
    if (!cbm_ignore_checked_usage(owner, &before))
        return false;
    cbm_ignore_checked_control_t control = gic_control();
    gic_counter counter = {0};
    control.cancelled = gic_cancel;
    control.context = &counter;
    cbm_ignore_checked_error_t error;
    cbm_ignore_checked_status_t status;
    bool cleared =
        kind < 5 ? gic_invalid_parse_call(owner, program, kind, &control, &error, &status)
                 : gic_invalid_match_call(owner, program, foreign, kind, &control, &error, &status);
    return cleared && gic_status(status, CBM_IGNORE_CHECKED_INVALID, &error) &&
           counter.calls == 0 && cbm_ignore_checked_usage(owner, &after) &&
           gic_usage_equal(&before, &after);
}

static bool gic_invalid_open(unsigned kind) {
    cbm_ignore_checked_limits_t limits = gic_limits();
    if (kind == 0)
        limits.max_bytes = 0;
    if (kind == 1)
        limits.max_work = 0;
    if (kind == 2)
        limits.max_patterns = 0;
    if (kind == 3)
        limits.max_arena_bytes = 0;
    if (kind == 4)
        limits.max_depth = 0;
    if (kind == 5)
        limits.max_depth = CBM_IGNORE_CHECKED_DEPTH_MAX + 1;
    if (kind == 6)
        limits.max_patterns = (size_t)INT_MAX + 1;
    cbm_ignore_checked_t *owner = NULL;
    cbm_ignore_checked_error_t error;
    cbm_ignore_checked_status_t status =
        cbm_ignore_checked_open(kind == 7 ? NULL : &limits, kind == 8 ? NULL : &owner, &error);
    bool ok = gic_status(status, CBM_IGNORE_CHECKED_INVALID, &error) && !owner;
    cbm_ignore_checked_free(owner);
    return ok;
}

static bool gic_optional_errors(cbm_ignore_checked_t *retained) {
    cbm_ignore_checked_t *owner = NULL;
    cbm_ignore_checked_limits_t limits = gic_limits();
    if (cbm_ignore_checked_open(&limits, &owner, NULL) != CBM_IGNORE_CHECKED_OK || !owner) {
        cbm_ignore_checked_free(owner);
        return false;
    }
    cbm_ignore_checked_control_t control = gic_control();
    cbm_ignore_checked_parse_result_t out = {0};
    cbm_ignore_decision_t decision = CBM_IGNORE_UNAVAILABLE;
    bool ok =
        cbm_ignore_checked_parse(owner, "x", 1, &control, &out, NULL) == CBM_IGNORE_CHECKED_OK &&
        out.program && out.pattern_count == 1;
    if (ok)
        ok = cbm_ignore_checked_match(owner, out.program, "x", 1, false, &control, &decision,
                                      NULL) == CBM_IGNORE_CHECKED_OK &&
             decision == CBM_IGNORE_IGNORED;
    cbm_ignore_checked_free(owner);
    owner = retained; /* Retained ownership remains in the caller's fixture. */
    cbm_ignore_checked_error_t error;
    return gic_status(cbm_ignore_checked_open(NULL, &owner, &error), CBM_IGNORE_CHECKED_INVALID,
                      &error) &&
           !owner && ok;
}

TEST(gic_c_ownership_preflight_and_terminal) {
    gic_fixture f;
    gic_init(&f);
    int result = 1;
    cbm_ignore_checked_limits_t limits = gic_limits();
    const cbm_ignore_program_t *program = NULL, *foreign = NULL;
    GIC_CHECK(gic_open(&f.owner[0], &limits) && gic_open(&f.owner[1], &limits));
    memset(&limits, 0, sizeof(limits)); /* Owners must retain their copied caps. */
    GIC_CHECK(gic_parse(f.owner[0], "x", 1, 1, &program));
    GIC_CHECK(gic_parse(f.owner[1], "x", 1, 1, &foreign));
    GIC_CHECK(gic_match(f.owner[0], program, "x", false, CBM_IGNORE_IGNORED));
    for (unsigned kind = 0; kind < 9; kind++)
        GIC_CHECK(gic_invalid_open(kind));
    for (unsigned kind = 0; kind < 13; kind++)
        GIC_CHECK(gic_preflight(f.owner[0], program, foreign, kind));
    GIC_CHECK(gic_match(f.owner[0], program, "x", false, CBM_IGNORE_IGNORED));
    GIC_CHECK(gic_match(f.owner[1], foreign, "x", false, CBM_IGNORE_IGNORED));
    GIC_CHECK(gic_optional_errors(f.owner[1]));
    GIC_CHECK(gic_bad_span(f.owner[0], program, 0));
    GIC_CHECK(gic_preflight(f.owner[0], program, foreign, 5));
    GIC_CHECK(gic_terminal(f.owner[0], program, CBM_IGNORE_CHECKED_UNSUPPORTED));
    GIC_CHECK(gic_match(f.owner[1], foreign, "x", false, CBM_IGNORE_IGNORED));
    cbm_ignore_checked_usage_t usage;
    memset(&usage, 0xa5, sizeof(usage));
    GIC_CHECK(!cbm_ignore_checked_usage(NULL, &usage));
    GIC_CHECK(usage.bytes_reserved == 0 && usage.work_used == 0 && usage.patterns_reserved == 0 &&
              usage.arena_requested_bytes == 0 && usage.terminal_status == CBM_IGNORE_CHECKED_OK);
    GIC_CHECK(!cbm_ignore_checked_usage(f.owner[1], NULL));
    cbm_ignore_checked_free(NULL);
    result = 0;
done:
    return gic_finish(&f, result);
}

static const char gic_rules0[] = "*.tmp\n!keep.tmp\n";
static const char gic_rules1[] = "file?\n";
static const char *const gic_paths[] = {"drop.tmp", "keep.tmp", "file1", "other", "drop.tmp"};
static const cbm_ignore_decision_t gic_decisions[] = {CBM_IGNORE_IGNORED, CBM_IGNORE_REINCLUDED,
                                                      CBM_IGNORE_IGNORED, CBM_IGNORE_NO_OPINION,
                                                      CBM_IGNORE_IGNORED};

typedef struct {
    cbm_ignore_checked_status_t status;
    cbm_ignore_cap_t cap;
    cbm_ignore_checked_usage_t usage;
} gic_replay_result;

static bool gic_replay_parse(cbm_ignore_checked_t *owner, size_t step,
                             const cbm_ignore_program_t **program,
                             cbm_ignore_checked_status_t *status,
                             cbm_ignore_checked_error_t *error) {
    const char *rules = step == 0 ? gic_rules0 : gic_rules1;
    cbm_ignore_checked_control_t control = gic_control();
    cbm_ignore_checked_parse_result_t out = {.pattern_count = 77};
    *status = cbm_ignore_checked_parse(owner, rules, strlen(rules), &control, &out, error);
    if (*status != CBM_IGNORE_CHECKED_OK)
        return !out.program && out.pattern_count == 0;
    *program = out.program;
    return out.program && out.pattern_count == (step == 0 ? 2 : 1);
}

static bool gic_replay_match(cbm_ignore_checked_t *owner, const cbm_ignore_program_t **program,
                             size_t step, cbm_ignore_checked_status_t *status,
                             cbm_ignore_checked_error_t *error) {
    cbm_ignore_checked_control_t control = gic_control();
    cbm_ignore_decision_t decision = CBM_IGNORE_IGNORED;
    size_t row = step - 2;
    *status = cbm_ignore_checked_match(owner, program[row == 2 ? 1 : 0], gic_paths[row],
                                       strlen(gic_paths[row]), false, &control, &decision, error);
    return decision ==
           (*status == CBM_IGNORE_CHECKED_OK ? gic_decisions[row] : CBM_IGNORE_UNAVAILABLE);
}

static bool gic_replay(const cbm_ignore_checked_limits_t *limits, gic_replay_result *out) {
    cbm_ignore_checked_t *owner = NULL;
    if (!gic_open(&owner, limits))
        return false;
    const cbm_ignore_program_t *program[2] = {NULL, NULL};
    cbm_ignore_checked_error_t error = {0};
    cbm_ignore_checked_status_t status = CBM_IGNORE_CHECKED_OK;
    bool ok = true;
    size_t arena_after_parse = 0;
    for (size_t step = 0; step < 7 && ok && status == CBM_IGNORE_CHECKED_OK; step++) {
        cbm_ignore_checked_usage_t before, after;
        ok = cbm_ignore_checked_usage(owner, &before);
        if (!ok)
            break;
        ok = step < 2 ? gic_replay_parse(owner, step, &program[step], &status, &error)
                      : gic_replay_match(owner, program, step, &status, &error);
        ok = ok && error.status == status && cbm_ignore_checked_usage(owner, &after);
        if (!ok)
            break;
        if (step == 1)
            arena_after_parse = after.arena_requested_bytes;
        if (step >= 2)
            ok = after.arena_requested_bytes == arena_after_parse;
        if (status == CBM_IGNORE_CHECKED_LIMIT && error.cap == CBM_IGNORE_CAP_BYTES)
            ok = ok && before.bytes_reserved == after.bytes_reserved;
    }
    out->status = status;
    out->cap = error.cap;
    ok = ok && cbm_ignore_checked_usage(owner, &out->usage);
    if (status != CBM_IGNORE_CHECKED_OK)
        ok = ok && gic_terminal(owner, program[0], status);
    cbm_ignore_checked_free(owner);
    return ok;
}

static bool gic_quota_pair(const gic_replay_result *full, cbm_ignore_cap_t cap) {
    cbm_ignore_checked_limits_t limits = gic_limits();
    if (cap == CBM_IGNORE_CAP_BYTES)
        limits.max_bytes = full->usage.bytes_reserved;
    if (cap == CBM_IGNORE_CAP_PATTERNS)
        limits.max_patterns = full->usage.patterns_reserved;
    if (cap == CBM_IGNORE_CAP_ARENA)
        limits.max_arena_bytes = full->usage.arena_requested_bytes;
    if (cap == CBM_IGNORE_CAP_WORK)
        limits.max_work = full->usage.work_used;
    gic_replay_result exact = {0}, short_cap = {0};
    if (!gic_replay(&limits, &exact) || exact.status != CBM_IGNORE_CHECKED_OK ||
        exact.cap != CBM_IGNORE_CAP_NONE || !gic_usage_equal(&full->usage, &exact.usage))
        return false;
    if (cap == CBM_IGNORE_CAP_BYTES)
        limits.max_bytes--;
    if (cap == CBM_IGNORE_CAP_PATTERNS)
        limits.max_patterns--;
    if (cap == CBM_IGNORE_CAP_ARENA)
        limits.max_arena_bytes--;
    if (cap == CBM_IGNORE_CAP_WORK)
        limits.max_work--;
    return gic_replay(&limits, &short_cap) && short_cap.status == CBM_IGNORE_CHECKED_LIMIT &&
           short_cap.cap == cap && short_cap.usage.terminal_status == CBM_IGNORE_CHECKED_LIMIT &&
           short_cap.usage.bytes_reserved <= limits.max_bytes &&
           short_cap.usage.patterns_reserved <= limits.max_patterns &&
           short_cap.usage.arena_requested_bytes <= limits.max_arena_bytes &&
           short_cap.usage.work_used <= limits.max_work;
}

TEST(gic_d_aggregate_quota_boundaries) {
    cbm_ignore_checked_limits_t limits = gic_limits();
    gic_replay_result full = {0};
    ASSERT_TRUE(gic_replay(&limits, &full));
    ASSERT_EQ(full.status, CBM_IGNORE_CHECKED_OK);
    uint64_t expected_bytes = sizeof(gic_rules0) - 1 + sizeof(gic_rules1) - 1;
    for (size_t i = 0; i < 5; i++)
        expected_bytes += strlen(gic_paths[i]) + 1;
    ASSERT_EQ(full.usage.bytes_reserved, expected_bytes);
    ASSERT_EQ(full.usage.patterns_reserved, 3);
    ASSERT_GT(full.usage.work_used, 1);
    ASSERT_GT(full.usage.arena_requested_bytes, 1);
    ASSERT_TRUE(gic_quota_pair(&full, CBM_IGNORE_CAP_BYTES));
    ASSERT_TRUE(gic_quota_pair(&full, CBM_IGNORE_CAP_PATTERNS));
    ASSERT_TRUE(gic_quota_pair(&full, CBM_IGNORE_CAP_ARENA));
    ASSERT_TRUE(gic_quota_pair(&full, CBM_IGNORE_CAP_WORK));
    PASS();
}

static bool gic_match_limit(cbm_ignore_checked_t *owner, const cbm_ignore_program_t *program,
                            const char *path, cbm_ignore_cap_t cap) {
    cbm_ignore_checked_control_t control = gic_control();
    cbm_ignore_checked_error_t error;
    cbm_ignore_decision_t decision = CBM_IGNORE_IGNORED;
    cbm_ignore_checked_status_t status = cbm_ignore_checked_match(
        owner, program, path, strlen(path), false, &control, &decision, &error);
    return gic_status(status, CBM_IGNORE_CHECKED_LIMIT, &error) && error.cap == cap &&
           decision == CBM_IGNORE_UNAVAILABLE &&
           gic_terminal(owner, program, CBM_IGNORE_CHECKED_LIMIT);
}

static bool gic_depth_case(unsigned depth, bool earlier) {
    cbm_ignore_checked_limits_t limits = gic_limits();
    limits.max_depth = depth;
    cbm_ignore_checked_t *owner = NULL;
    const cbm_ignore_program_t *program = NULL;
    const char *rules = earlier ? "a\n!*a" : "*a";
    bool ok = gic_open(&owner, &limits) &&
              gic_parse(owner, rules, strlen(rules), earlier ? 2 : 1, &program);
    if (ok && depth == 1)
        ok = gic_match_limit(owner, program, "a", CBM_IGNORE_CAP_DEPTH);
    else if (ok)
        ok = gic_match(owner, program, "a", false,
                       earlier ? CBM_IGNORE_REINCLUDED : CBM_IGNORE_IGNORED);
    cbm_ignore_checked_free(owner);
    return ok;
}

static bool gic_attempt_case(bool earlier) {
    char pattern[96], path[64];
    size_t n = 0;
    if (earlier) {
        memcpy(pattern, "*\n!", 3);
        n = 3;
    }
    for (size_t i = 0; i < 20; i++) {
        memcpy(pattern + n, "a**", 3);
        n += 3;
    }
    pattern[n++] = 'X';
    pattern[n] = 0;
    memset(path, 'a', sizeof(path) - 1);
    path[sizeof(path) - 1] = 0;
    cbm_gitignore_t *legacy = cbm_gitignore_parse(pattern);
    if (!legacy)
        return false;
    int old = cbm_gitignore_match_result(legacy, path, false);
    cbm_gitignore_free(legacy);
    if (old != (earlier ? 1 : 0))
        return false;
    cbm_ignore_checked_t *owner = NULL;
    const cbm_ignore_program_t *program = NULL;
    cbm_ignore_checked_limits_t limits = gic_limits();
    bool ok = gic_open(&owner, &limits) &&
              gic_parse(owner, pattern, n, earlier ? 2 : 1, &program) &&
              gic_match_limit(owner, program, path, CBM_IGNORE_CAP_ATTEMPT);
    cbm_ignore_checked_free(owner);
    return ok;
}

static bool gic_max_depth_boundary(void) {
    char pattern[2 * CBM_IGNORE_CHECKED_DEPTH_MAX + 1];
    char path[CBM_IGNORE_CHECKED_DEPTH_MAX];
    size_t groups = CBM_IGNORE_CHECKED_DEPTH_MAX - 1;
    for (size_t i = 0; i < groups; i++) {
        pattern[2 * i] = '*';
        pattern[2 * i + 1] = 'a';
        path[i] = 'a';
    }
    pattern[2 * groups] = 0;
    path[groups] = 0;
    cbm_ignore_checked_limits_t limits = gic_limits();
    cbm_ignore_checked_t *owner = NULL;
    const cbm_ignore_program_t *program = NULL;
    bool ok = gic_open(&owner, &limits) && gic_parse(owner, pattern, 2 * groups, 1, &program) &&
              gic_match(owner, program, path, false, CBM_IGNORE_IGNORED);
    cbm_ignore_checked_free(owner);
    owner = NULL;
    if (!ok)
        return false;
    limits.max_depth--;
    ok = gic_open(&owner, &limits) && gic_parse(owner, pattern, 2 * groups, 1, &program) &&
         gic_match_limit(owner, program, path, CBM_IGNORE_CAP_DEPTH);
    cbm_ignore_checked_free(owner);
    return ok;
}

TEST(gic_e_depth_attempt_and_no_partial_decision) {
    const gic_semantic ordinary = {"a**Z", "a/x/Z", false, 1, CBM_IGNORE_IGNORED};
    ASSERT_TRUE(gic_semantic_one(&ordinary));
    ASSERT_TRUE(gic_max_depth_boundary());
    ASSERT_TRUE(gic_depth_case(2, false));
    ASSERT_TRUE(gic_depth_case(2, true));
    ASSERT_TRUE(gic_depth_case(1, false));
    ASSERT_TRUE(gic_depth_case(1, true));
    ASSERT_TRUE(gic_attempt_case(false));
    ASSERT_TRUE(gic_attempt_case(true));
    PASS();
}

enum { GIC_LONG = 8193 };
typedef struct {
    char *patterns, *path;
    size_t pattern_length;
    cbm_ignore_decision_t expected;
} gic_long_input;

static bool gic_long_make(gic_fixture *f, unsigned kind, gic_long_input *out) {
    out->patterns = cbm_arena_alloc(&f->input, GIC_LONG + 3);
    out->path = cbm_arena_alloc(&f->input, GIC_LONG + 1);
    if (!out->patterns || !out->path)
        return false;
    memset(out->patterns, 'a', GIC_LONG + 3);
    memset(out->path, 'a', GIC_LONG);
    out->path[GIC_LONG] = 0;
    out->pattern_length = GIC_LONG;
    out->expected = CBM_IGNORE_IGNORED;
    if (kind == 1) {
        out->patterns[0] = '[';
        out->patterns[GIC_LONG + 1] = ']';
        out->pattern_length = GIC_LONG + 2;
        out->path[1] = 0;
    }
    if (kind == 2) {
        out->patterns[0] = 'x';
        out->pattern_length = 1;
        for (size_t i = 1; i < GIC_LONG; i += 2)
            out->path[i] = '/';
        out->expected = CBM_IGNORE_NO_OPINION;
    }
    out->patterns[out->pattern_length] = 0;
    return true;
}

typedef struct {
    size_t polls;
    uint64_t work;
    bool cancelled;
} gic_observation;

static bool gic_long_operation(cbm_ignore_checked_t *owner, const cbm_ignore_program_t *program,
                               const gic_long_input *input, bool parsing,
                               const cbm_ignore_checked_control_t *control,
                               const cbm_ignore_program_t **parsed,
                               cbm_ignore_checked_status_t *status) {
    cbm_ignore_checked_error_t error;
    if (parsing) {
        cbm_ignore_checked_parse_result_t out = {.program = program, .pattern_count = 77};
        *status = cbm_ignore_checked_parse(owner, input->patterns, input->pattern_length, control,
                                           &out, &error);
        if (*status == CBM_IGNORE_CHECKED_OK) {
            *parsed = out.program;
            return error.status == *status && out.program && out.pattern_count == 1;
        }
        return error.status == *status && !out.program && out.pattern_count == 0;
    }
    cbm_ignore_decision_t decision = CBM_IGNORE_IGNORED;
    *status = cbm_ignore_checked_match(owner, program, input->path, strlen(input->path), false,
                                       control, &decision, &error);
    return error.status == *status &&
           decision ==
               (*status == CBM_IGNORE_CHECKED_OK ? input->expected : CBM_IGNORE_UNAVAILABLE);
}

static bool gic_long_run(const gic_long_input *input, bool parsing, size_t stop,
                         gic_observation *observation) {
    cbm_ignore_checked_limits_t limits = gic_limits();
    cbm_ignore_checked_t *owner = NULL;
    const cbm_ignore_program_t *program = NULL;
    bool ok = gic_open(&owner, &limits);
    if (ok)
        ok = parsing ? gic_parse(owner, "keep", 4, 1, &program)
                     : gic_parse(owner, input->patterns, input->pattern_length, 1, &program);
    cbm_ignore_checked_usage_t before = {0}, after = {0};
    ok = ok && cbm_ignore_checked_usage(owner, &before);
    gic_counter counter = {.stop = stop};
    cbm_ignore_checked_control_t control = gic_control();
    control.cancelled = gic_cancel;
    control.context = &counter;
    cbm_ignore_checked_status_t status = CBM_IGNORE_CHECKED_INVALID;
    const cbm_ignore_program_t *parsed = NULL;
    if (ok)
        ok = gic_long_operation(owner, program, input, parsing, &control, &parsed, &status);
    ok = ok && cbm_ignore_checked_usage(owner, &after);
    if (ok && status == CBM_IGNORE_CHECKED_OK)
        ok = (!stop || counter.calls < stop) && after.terminal_status == CBM_IGNORE_CHECKED_OK;
    else if (ok)
        ok = status == CBM_IGNORE_CHECKED_CANCELLED && stop && counter.calls >= stop &&
             gic_terminal(owner, program, CBM_IGNORE_CHECKED_CANCELLED);
    observation->polls = counter.calls;
    observation->work = after.work_used - before.work_used;
    observation->cancelled = status == CBM_IGNORE_CHECKED_CANCELLED;
    /* Necessary aggregate bound only. This does not observe individual gaps;
     * the callback never reenters usage or any other owner operation. */
    if (ok)
        ok = counter.calls > 0 && counter.calls <= UINT64_MAX / 4096 &&
             observation->work <= (uint64_t)counter.calls * 4096;
    if (ok && parsing && status == CBM_IGNORE_CHECKED_OK)
        ok = gic_match(owner, parsed, input->path, false, input->expected);
    size_t polls_before_free = counter.calls;
    cbm_ignore_checked_free(owner);
    return ok && counter.calls == polls_before_free;
}

static bool gic_cancel_sweep(const gic_long_input *input, bool parsing) {
    gic_observation normal = {0};
    if (!gic_long_run(input, parsing, 0, &normal) || normal.cancelled || normal.work <= 4096 ||
        !normal.polls || normal.polls >= SIZE_MAX - 1)
        return false;
    const size_t stops[] = {1, normal.polls / 2 + 1, normal.polls, normal.polls + 1};
    bool cancelled = false;
    for (size_t i = 0; i < 4; i++) {
        gic_observation sample = {0};
        if (!gic_long_run(input, parsing, stops[i], &sample))
            return false;
        cancelled = cancelled || sample.cancelled;
    }
    return cancelled;
}

static bool gic_expired(cbm_ignore_checked_t *owner, const cbm_ignore_program_t *program,
                        bool parsing) {
    cbm_ignore_checked_control_t control = gic_control();
    control.deadline_ms = 1;
    cbm_ignore_checked_error_t error;
    cbm_ignore_checked_status_t status;
    if (cbm_now_ms() <= 1)
        return false;
    if (parsing) {
        cbm_ignore_checked_parse_result_t out = {.program = program, .pattern_count = 77};
        status = cbm_ignore_checked_parse(owner, "x", 1, &control, &out, &error);
        if (out.program || out.pattern_count)
            return false;
    } else {
        cbm_ignore_decision_t decision = CBM_IGNORE_IGNORED;
        status =
            cbm_ignore_checked_match(owner, program, "x", 1, false, &control, &decision, &error);
        if (decision != CBM_IGNORE_UNAVAILABLE)
            return false;
    }
    return gic_status(status, CBM_IGNORE_CHECKED_DEADLINE, &error) &&
           gic_terminal(owner, program, CBM_IGNORE_CHECKED_DEADLINE);
}

TEST(gic_f_cancellation_deadline_and_poll_evidence) {
    gic_fixture f;
    gic_init(&f);
    int result = 1;
    gic_long_input input;
    GIC_CHECK(gic_long_make(&f, 0, &input));
    GIC_CHECK(gic_cancel_sweep(&input, true));
    GIC_CHECK(gic_cancel_sweep(&input, false));
    for (unsigned kind = 1; kind < 3; kind++) {
        gic_observation sample = {0};
        GIC_CHECK(gic_long_make(&f, kind, &input));
        GIC_CHECK(gic_long_run(&input, false, 0, &sample));
        GIC_CHECK(!sample.cancelled && sample.work > 4096);
    }
    cbm_ignore_checked_limits_t limits = gic_limits();
    const cbm_ignore_program_t *program[3] = {NULL, NULL, NULL};
    for (size_t i = 0; i < 3; i++) {
        GIC_CHECK(gic_open(&f.owner[i], &limits));
        GIC_CHECK(gic_parse(f.owner[i], "x", 1, 1, &program[i]));
        GIC_CHECK(gic_match(f.owner[i], program[i], "x", false, CBM_IGNORE_IGNORED));
    }
    GIC_CHECK(gic_expired(f.owner[0], program[0], true));
    GIC_CHECK(gic_expired(f.owner[1], program[1], false));
    GIC_CHECK(gic_match(f.owner[2], program[2], "x", false, CBM_IGNORE_IGNORED));
    result = 0;
done:
    return gic_finish(&f, result);
}

static bool gic_owner_oom(size_t owner_request) {
    cbm_ignore_checked_limits_t limits = gic_limits();
    limits.max_arena_bytes = owner_request;
    cbm_ignore_checked_t *owner = NULL;
    cbm_ignore_checked_error_t error;
    cbm_ignore_checked_status_t status =
        cbm_ignore_checked_open_oom_for_tests(&limits, &owner, &error);
    bool ok = gic_status(status, CBM_IGNORE_CHECKED_OOM, &error) && !owner;
    cbm_ignore_checked_free(owner);
    if (!ok || !owner_request)
        return false;
    limits.max_arena_bytes--;
    status = cbm_ignore_checked_open_oom_for_tests(&limits, &owner, &error);
    cbm_ignore_checked_status_t wanted =
        owner_request == 1 ? CBM_IGNORE_CHECKED_INVALID : CBM_IGNORE_CHECKED_LIMIT;
    ok = gic_status(status, wanted, &error) &&
         (wanted != CBM_IGNORE_CHECKED_LIMIT || error.cap == CBM_IGNORE_CAP_ARENA) && !owner;
    cbm_ignore_checked_free(owner);
    return ok;
}

static bool gic_parse_oom(cbm_ignore_parse_fault_t fault, cbm_ignore_checked_usage_t *before,
                          cbm_ignore_checked_usage_t *usage) {
    cbm_ignore_checked_t *owner = NULL;
    cbm_ignore_checked_limits_t limits = gic_limits();
    if (!gic_open(&owner, &limits))
        return false;
    const cbm_ignore_program_t *old_program = NULL;
    if (!gic_parse(owner, "keep", 4, 1, &old_program) || !cbm_ignore_checked_usage(owner, before)) {
        cbm_ignore_checked_free(owner);
        return false;
    }
    cbm_ignore_checked_control_t control = gic_control();
    cbm_ignore_checked_error_t error;
    cbm_ignore_checked_parse_result_t out = {.program = old_program, .pattern_count = 77};
    const char input[] = "one\ntwo\n";
    cbm_ignore_checked_status_t status = cbm_ignore_checked_parse_fault_for_tests(
        owner, input, sizeof(input) - 1, &control, fault, &out, &error);
    bool ok = gic_status(status, CBM_IGNORE_CHECKED_OOM, &error) && !out.program &&
              out.pattern_count == 0 && cbm_ignore_checked_usage(owner, usage) &&
              usage->bytes_reserved == before->bytes_reserved + sizeof(input) - 1 &&
              usage->patterns_reserved == before->patterns_reserved + 2 &&
              usage->arena_requested_bytes > before->arena_requested_bytes &&
              usage->terminal_status == CBM_IGNORE_CHECKED_OOM &&
              gic_terminal(owner, old_program, CBM_IGNORE_CHECKED_OOM);
    cbm_ignore_checked_free(owner);
    return ok;
}

static bool gic_unreached_and_invalid_faults(cbm_ignore_checked_t *owner,
                                             const cbm_ignore_program_t *program) {
    cbm_ignore_checked_control_t control = gic_control();
    gic_counter counter = {0};
    control.cancelled = gic_cancel;
    control.context = &counter;
    cbm_ignore_checked_usage_t before, after;
    if (!cbm_ignore_checked_usage(owner, &before))
        return false;
    cbm_ignore_checked_parse_result_t out = {.program = program, .pattern_count = 77};
    cbm_ignore_checked_error_t error;
    cbm_ignore_checked_status_t status = cbm_ignore_checked_parse_fault_for_tests(
        owner, "x", 1, &control, (cbm_ignore_parse_fault_t)999, &out, &error);
    if (!gic_status(status, CBM_IGNORE_CHECKED_INVALID, &error) || out.program ||
        out.pattern_count || counter.calls || !cbm_ignore_checked_usage(owner, &after) ||
        !gic_usage_equal(&before, &after))
        return false;
    status = cbm_ignore_checked_parse_fault_for_tests(owner, NULL, 0, &control,
                                                      CBM_IGNORE_PARSE_FAULT_PATTERN, &out, &error);
    if (!gic_status(status, CBM_IGNORE_CHECKED_OK, &error) || !out.program || out.pattern_count ||
        !gic_match(owner, out.program, "x", false, CBM_IGNORE_NO_OPINION))
        return false;
    status = cbm_ignore_checked_parse_fault_for_tests(owner, "z", 1, &control,
                                                      CBM_IGNORE_PARSE_FAULT_NONE, &out, &error);
    return gic_status(status, CBM_IGNORE_CHECKED_OK, &error) && out.program &&
           out.pattern_count == 1 &&
           gic_match(owner, out.program, "z", false, CBM_IGNORE_IGNORED) &&
           gic_match(owner, program, "one", false, CBM_IGNORE_IGNORED);
}

static bool gic_oom_quota_first(cbm_ignore_parse_fault_t fault, size_t owner_request) {
    cbm_ignore_checked_limits_t limits = gic_limits();
    limits.max_arena_bytes = owner_request;
    cbm_ignore_checked_t *owner = NULL;
    if (!gic_open(&owner, &limits))
        return false;
    cbm_ignore_checked_control_t control = gic_control();
    cbm_ignore_checked_error_t error;
    cbm_ignore_checked_parse_result_t out = {.pattern_count = 77};
    cbm_ignore_checked_status_t status =
        cbm_ignore_checked_parse_fault_for_tests(owner, "x", 1, &control, fault, &out, &error);
    cbm_ignore_checked_usage_t usage;
    bool ok = gic_status(status, CBM_IGNORE_CHECKED_LIMIT, &error) &&
              error.cap == CBM_IGNORE_CAP_ARENA && !out.program && out.pattern_count == 0 &&
              cbm_ignore_checked_usage(owner, &usage) &&
              usage.arena_requested_bytes == owner_request &&
              gic_terminal(owner, NULL, CBM_IGNORE_CHECKED_LIMIT);
    cbm_ignore_checked_free(owner);
    return ok;
}

TEST(gic_g_call_local_simulated_oom) {
    gic_fixture f;
    gic_init(&f);
    int result = 1;
    cbm_ignore_checked_limits_t limits = gic_limits();
    cbm_ignore_checked_usage_t opened, normal, program_failure, pattern_failure;
    cbm_ignore_checked_usage_t program_before, pattern_before;
    const cbm_ignore_program_t *program = NULL;
    GIC_CHECK(gic_open(&f.owner[0], &limits));
    GIC_CHECK(cbm_ignore_checked_usage(f.owner[0], &opened));
    GIC_CHECK(gic_parse(f.owner[0], "keep", 4, 1, &program));
    GIC_CHECK(gic_parse(f.owner[0], "one\ntwo\n", 8, 2, &program));
    GIC_CHECK(gic_match(f.owner[0], program, "one", false, CBM_IGNORE_IGNORED));
    GIC_CHECK(cbm_ignore_checked_usage(f.owner[0], &normal));
    GIC_CHECK(gic_owner_oom(opened.arena_requested_bytes));
    GIC_CHECK(gic_parse_oom(CBM_IGNORE_PARSE_FAULT_PROGRAM, &program_before, &program_failure));
    GIC_CHECK(gic_parse_oom(CBM_IGNORE_PARSE_FAULT_PATTERN, &pattern_before, &pattern_failure));
    GIC_CHECK(gic_usage_equal(&program_before, &pattern_before));
    GIC_CHECK(program_failure.arena_requested_bytes > program_before.arena_requested_bytes);
    GIC_CHECK(pattern_failure.arena_requested_bytes > program_failure.arena_requested_bytes);
    GIC_CHECK(pattern_failure.arena_requested_bytes <= normal.arena_requested_bytes);
    GIC_CHECK(program_failure.work_used > opened.work_used && pattern_failure.work_used > 0);
    GIC_CHECK(gic_oom_quota_first(CBM_IGNORE_PARSE_FAULT_PROGRAM, opened.arena_requested_bytes));
    GIC_CHECK(gic_oom_quota_first(CBM_IGNORE_PARSE_FAULT_PATTERN, opened.arena_requested_bytes));
    GIC_CHECK(gic_unreached_and_invalid_faults(f.owner[0], program));
    /* Failed calls in other owners cannot poison this retained real program. */
    GIC_CHECK(gic_match(f.owner[0], program, "two", false, CBM_IGNORE_IGNORED));
    result = 0;
done:
    return gic_finish(&f, result);
}

SUITE(gitignore_checked) {
    RUN_TEST(gic_a_semantics_and_legacy_parity);
    RUN_TEST(gic_b_exact_spans_and_input_lifetime);
    RUN_TEST(gic_c_ownership_preflight_and_terminal);
    RUN_TEST(gic_d_aggregate_quota_boundaries);
    RUN_TEST(gic_e_depth_attempt_and_no_partial_decision);
    RUN_TEST(gic_f_cancellation_deadline_and_poll_evidence);
    RUN_TEST(gic_g_call_local_simulated_oom);
}
