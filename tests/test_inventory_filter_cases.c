#include "test_inventory_filter_internal.h"
#define NEED CBM_INVENTORY_FILTER_NEEDS_LANGUAGE
#define IGN CBM_INVENTORY_FILTER_IGNORED_FILE
#define SUB CBM_INVENTORY_FILTER_EXCLUDED_SUBTREE
#define NONE CBM_INVENTORY_FILTER_REASON_NONE
#define BUILTIN CBM_INVENTORY_FILTER_REASON_DIRECTORY_BUILTIN
#define SUFFIX CBM_INVENTORY_FILTER_REASON_DIRECTORY_SUFFIX
#define GIT CBM_INVENTORY_FILTER_REASON_GITIGNORE
#define CBM CBM_INVENTORY_FILTER_REASON_CBMIGNORE
#define EXT CBM_INVENTORY_FILTER_REASON_IGNORED_SUFFIX
#define GR CBM_INVENTORY_FILTER_ROLE_GITIGNORE
#define CR CBM_INVENTORY_FILTER_ROLE_CBMIGNORE
#define GK CBM_INVENTORY_CONTROL_GITIGNORE
#define CK CBM_INVENTORY_CONTROL_CBMIGNORE
#define ABS CBM_INVENTORY_CONTROL_ABSENT
#define EMPTY CBM_INVENTORY_CONTROL_APPLIED_EMPTY
#define APPLIED CBM_INVENTORY_CONTROL_APPLIED

int if_case_policy(void) {
    if_fixture f;
    if_init(&f);
    int result = 1;
    static const if_row expected[] = {{".cbmignore", NEED, NONE, CR, NULL},
                                      {".gitignore", NEED, NONE, GR, NULL},
                                      {"LICENSE", NEED, NONE, 0, NULL},
                                      {"a.png", IGN, EXT, 0, NULL},
                                      {"backend/storage/framework/views/deep/x.c", SUB, SUFFIX, 0,
                                       "backend/storage/framework/views"},
                                      {"cb-only.c", IGN, CBM, 0, NULL},
                                      {"node_modules/.gitignore", SUB, BUILTIN, 0, "node_modules"},
                                      {"node_modules/keep.c", SUB, BUILTIN, 0, "node_modules"},
                                      {"package.json", NEED, NONE, 0, NULL},
                                      {"source.c", NEED, NONE, 0, NULL},
                                      {"sub/.gitignore", NEED, NONE, GR, NULL},
                                      {"sub/blocked/.gitignore", SUB, GIT, 0, "sub/blocked"},
                                      {"sub/blocked/child.c", SUB, GIT, 0, "sub/blocked"},
                                      {"sub/cb-only.c", IGN, GIT, 0, NULL},
                                      {"sub/keep.drop", NEED, NONE, 0, NULL},
                                      {"sub/no.drop", IGN, GIT, 0, NULL},
                                      {"thing.generated.c", NEED, NONE, 0, NULL},
                                      {"unknown.weird", NEED, NONE, 0, NULL},
                                      {"vendor/.gitignore", NEED, NONE, GR, NULL},
                                      {"vendor/file.c", NEED, NONE, 0, NULL}};
    static const if_control_row controls[] = {{GK, "", 1, APPLIED, 3},
                                              {GK, "backend", SIZE_MAX, ABS, 0},
                                              {GK, "backend/storage", SIZE_MAX, ABS, 0},
                                              {GK, "backend/storage/framework", SIZE_MAX, ABS, 0},
                                              {GK, "sub", 10, APPLIED, 4},
                                              {GK, "vendor", 18, EMPTY, 0},
                                              {CK, "", 0, APPLIED, 3}};
    IF_CHECK(if_dependency_control());
    for (size_t i = 0; i < sizeof(expected) / sizeof(expected[0]); i++)
        IF_CHECK(if_text(&f, expected[i].path, "body"));
    const char *cb = "!vendor/\n!node_modules/\ncb-only.c\n";
    const char *gi = "*.png\n*.drop\nsub/blocked/\n";
    const char *nested = "!keep.drop\nkeep.drop\n!keep.drop\ncb-only.c\n";
    /* Rebuild the same literal inventory with exact control bytes. */
    if_init(&f);
    for (size_t i = 0; i < sizeof(expected) / sizeof(expected[0]); i++) {
        const char *bytes = i == 0 ? cb : i == 1 ? gi : i == 10 ? nested : i == 18 ? "" : "body";
        IF_CHECK(if_text(&f, expected[i].path, bytes));
    }
    f.bytes[6][1] = 0;
    f.bytes[11][1] = 0; /* Excluded controls are deliberately invalid. */
    IF_CHECK(if_prepare(&f));
    IF_CHECK(if_rows(&f, expected, sizeof(expected) / sizeof(expected[0])) &&
             if_controls(&f, controls, 7));
    const size_t reads[] = {0, 1, 10, 18};
    IF_CHECK(if_reads(&f, reads, 4));
    const cbm_inventory_filter_view_t *v = cbm_inventory_filter_view(f.owner);
    IF_CHECK(v->directory_count == 10 && v->excluded_count == 3);
    IF_CHECK(
        !strcmp((const char *)v->excluded_directories[0].data, "backend/storage/framework/views"));
    IF_CHECK(!strcmp((const char *)v->excluded_directories[1].data, "node_modules"));
    IF_CHECK(!strcmp((const char *)v->excluded_directories[2].data, "sub/blocked"));
    result = 0;
done:
    return if_finish(&f, result);
}

int if_case_controls(void) {
    if_fixture f;
    if_init(&f);
    int result = 1;
    static const if_control_row absent[] = {{GK, "", SIZE_MAX, ABS, 0}, {CK, "", SIZE_MAX, ABS, 0}};
    IF_CHECK(if_dependency_control() && if_prepare(&f));
    const cbm_inventory_filter_view_t *v = cbm_inventory_filter_view(f.owner);
    IF_CHECK(v->file_count == 0 && v->directory_count == 1 && v->excluded_count == 0 &&
             f.calls == 0 && if_controls(&f, absent, 2));
    if_finish(&f, 0);
    if_init(&f);
    IF_CHECK(if_text(&f, ".cbmignore", "") && if_text(&f, ".codebase-memory.json", "not JSON") &&
             if_text(&f, ".gitignore", "# comment\n \t\n") &&
             if_add(&f, "sub/.cbmignore", "x\0y", 3) && if_text(&f, "sub/.gitignore", "drop.c\n") &&
             if_text(&f, "sub/drop.c", "body") && if_text(&f, "sub/keep.c", "body"));
    static const if_row rows[] = {{".cbmignore", NEED, NONE, CR, NULL},
                                  {".codebase-memory.json", NEED, NONE,
                                   CBM_INVENTORY_FILTER_ROLE_PHYSICAL_PROJECT_CONFIG, NULL},
                                  {".gitignore", NEED, NONE, GR, NULL},
                                  {"sub/.cbmignore", NEED, NONE, 0, NULL},
                                  {"sub/.gitignore", NEED, NONE, GR, NULL},
                                  {"sub/drop.c", IGN, GIT, 0, NULL},
                                  {"sub/keep.c", NEED, NONE, 0, NULL}};
    static const if_control_row controls[] = {
        {GK, "", 2, EMPTY, 0}, {GK, "sub", 4, APPLIED, 1}, {CK, "", 0, EMPTY, 0}};
    IF_CHECK(if_prepare(&f) && if_rows(&f, rows, 7) && if_controls(&f, controls, 3));
    const size_t reads[] = {0, 2, 4};
    IF_CHECK(if_reads(&f, reads, 3));
    v = cbm_inventory_filter_view(f.owner);
    IF_CHECK(v->directory_count == 2 && v->excluded_count == 0);
    result = 0;
done:
    return if_finish(&f, result);
}

static bool if_invalid_case(unsigned which) {
    if_fixture f;
    if_init(&f);
    bool ok = false;
    if (!if_text(&f, ".gitignore", "x\n") || !if_text(&f, "a", "body"))
        return false;
    cbm_inventory_status_t status = CBM_INVENTORY_INVALID;
    switch (which) {
    case 0:
        f.files[1].path.length = 0;
        break;
    case 1:
        strcpy(f.paths[1], "../a");
        f.files[1].path.length = 4;
        break;
    case 2:
        strcpy(f.paths[1], "a//b");
        f.files[1].path.length = 4;
        break;
    case 3:
        f.paths[1][0] = 0;
        break;
    case 4:
        f.paths[1][1] = 'x';
        break;
    case 5:
        f.files[1].oid[0] = 'A';
        break;
    case 6:
        f.files[1].oid[39] = 0;
        break;
    case 7:
        memset(f.files[1].oid, 'a', 64);
        f.files[1].oid[64] = 0;
        break;
    case 8:
        f.files[1].git_mode = 0120000;
        status = CBM_INVENTORY_UNSUPPORTED;
        break;
    case 9:
        f.files[1].content_length = UINT64_MAX;
        status = CBM_INVENTORY_LIMIT;
        break;
    case 10:
        f.files[1].path.length = SIZE_MAX;
        status = CBM_INVENTORY_LIMIT;
        break;
    case 11:
        f.source.read = NULL;
        break;
    case 12:
        f.control.deadline_ms = 0;
        break;
    case 13:
        f.limits.max_probe_prefix_bytes = 0;
        break;
    case 14:
        f.source.files = NULL;
        break;
    case 15:
        f.source.file_count = SIZE_MAX;
        status = CBM_INVENTORY_LIMIT;
        break;
    default:
        return false;
    }
    ok = if_error(&f, status, which <= 10 ? 1 : SIZE_MAX, false) && f.calls == 0;
    if_finish(&f, 0);
    return ok;
}
static bool if_structural_tail(unsigned which) {
    if_fixture f;
    if_init(&f);
    bool ok = false;
    cbm_inventory_status_t expected = CBM_INVENTORY_INVALID;
    switch (which) {
    case 0:
        if (!if_text(&f, "a", "x") || !if_text(&f, "a-b", "x") || !if_text(&f, "a/b", "x"))
            return false;
        break;
    case 1:
        if (!if_text(&f, "a", "x") || !if_text(&f, "a", "y"))
            return false;
        break;
    case 2:
        if (!if_text(&f, "z", "x") || !if_text(&f, "a", "y"))
            return false;
        break;
    case 3:
        if (!if_text(&f, ".gitignore/a", "x"))
            return false;
        expected = CBM_INVENTORY_UNSUPPORTED;
        break;
    case 4:
        if (!if_add(&f, ".gitignore", "x\0y", 3))
            return false;
        expected = CBM_INVENTORY_UNSUPPORTED;
        break;
    case 5:
        if (!if_text(&f, ".gitignore", "x\n"))
            return false;
        f.files[0].content_sha256[0] ^= 1;
        expected = CBM_INVENTORY_CHANGED;
        break;
    default:
        return false;
    }
    /* A collision/order pair can implicate either existing input row; never an
     * unknown or impossible index. An implied control directory has no file row. */
    ok = which < 3 ? if_error_known_row(&f, expected, false)
                   : if_error(&f, expected, which >= 4 ? 0 : SIZE_MAX, false);
    ok = ok && f.calls == (which >= 4 ? 1U : 0U);
    if_finish(&f, 0);
    return ok;
}
int if_case_structure(void) {
    if_fixture f;
    if_init(&f);
    int result = 1;
    IF_CHECK(if_dependency_control() && if_text(&f, ".gitignore", "x\n") &&
             if_text(&f, "a", "body") && if_prepare(&f));
    if_finish(&f, 0);
    for (unsigned i = 0; i < 16; i++)
        IF_CHECK(if_invalid_case(i));
    for (unsigned i = 0; i < 6; i++)
        IF_CHECK(if_structural_tail(i));
    result = 0;
done:
    return if_finish(&f, result);
}
