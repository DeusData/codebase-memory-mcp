#include "test_inventory_filter_internal.h"

int if_case_provider(void) {
    if_fixture f;
    if_init(&f);
    int result = 1;
    IF_CHECK(if_dependency_control() && if_text(&f, ".cbmignore", "x\n") &&
             if_text(&f, ".gitignore", "y\n") && if_prepare(&f));
    if_finish(&f, 0);
    for (int status = CBM_INVENTORY_INVALID; status <= CBM_INVENTORY_DEADLINE; status++) {
        for (unsigned cleanup = 0; cleanup < 2; cleanup++) {
            if_init(&f);
            IF_CHECK(if_text(&f, ".cbmignore", "x\n") && if_text(&f, ".gitignore", "y\n"));
            f.fail_index = 0;
            f.returned = f.reported = (cbm_inventory_status_t)status;
            f.cleanup = cleanup != 0;
            IF_CHECK(if_error(&f, (cbm_inventory_status_t)status, 0, f.cleanup) && f.calls == 1);
        }
    }
    /* Each malformed success changes one documented provider obligation. Valid
     * full bytes are written within capacity even when copied deliberately lies. */
    static const struct {
        cbm_inventory_status_t returned, reported;
        bool cleanup;
        size_t copied;
    } malformed[] = {
        {CBM_INVENTORY_OK, CBM_INVENTORY_OK, false, 1}, /* isolated short copied */
        {CBM_INVENTORY_OK, CBM_INVENTORY_IO, false, 2}, /* isolated status mismatch */
        {CBM_INVENTORY_OK, CBM_INVENTORY_OK, true, 2},  /* isolated cleanup */
        {CBM_INVENTORY_OK, CBM_INVENTORY_OK, false, 3}, /* overreport, never overwrite */
        {CBM_INVENTORY_IO, CBM_INVENTORY_STATE, true, 0},
        {(cbm_inventory_status_t)99, (cbm_inventory_status_t)99, true, 0},
        {CBM_INVENTORY_IO, CBM_INVENTORY_IO, false, 1},
    };
    const size_t first[] = {0};
    for (size_t i = 0; i < sizeof(malformed) / sizeof(malformed[0]); i++) {
        if_init(&f);
        IF_CHECK(if_text(&f, ".cbmignore", "x\n") && if_text(&f, ".gitignore", "y\n"));
        f.fail_index = 0;
        f.returned = malformed[i].returned;
        f.reported = malformed[i].reported;
        f.cleanup = malformed[i].cleanup;
        f.reply_bytes = true;
        f.reply_copied = malformed[i].copied;
        IF_CHECK(if_error(&f, CBM_INVENTORY_INVALID, 0, f.cleanup) && if_reads(&f, first, 1));
    }
    result = 0;
done:
    return if_finish(&f, result);
}

typedef struct {
    size_t observed, stop;
} if_cancel;
static bool if_cancelled(void *context) {
    if_cancel *c = context;
    c->observed++;
    return c->observed >= c->stop;
}
static bool if_cancel_fixture(if_fixture *f) {
    if_init(f);
    return if_text(f, ".cbmignore", "one\ntwo\n") && if_text(f, ".gitignore", "*.c\n!keep.c\n") &&
           if_text(f, "keep.c", "body");
}
int if_case_cancel(void) {
    if_fixture f;
    if_init(&f);
    int result = 1;
    if_cancel c = {0, SIZE_MAX};
    IF_CHECK(if_dependency_control() && if_cancel_fixture(&f));
    f.control.cancelled = if_cancelled;
    f.control.context = &c;
    IF_CHECK(if_prepare(&f) && c.observed > 2 && c.observed < 65536);
    size_t total = c.observed;
    if_finish(&f, 0);
    const size_t stops[] = {1, total / 2, total};
    for (size_t i = 0; i < 3; i++) {
        IF_CHECK(if_cancel_fixture(&f));
        c = (if_cancel){0, stops[i]};
        f.control.cancelled = if_cancelled;
        f.control.context = &c;
        if (i == 0) {
            /* Live deadline: initial cancellation alone must prevent every read. */
            IF_CHECK(f.control.deadline_ms == UINT64_MAX);
            IF_CHECK(if_error(&f, CBM_INVENTORY_CANCELLED, SIZE_MAX, false) && f.calls == 0);
        } else {
            /* Mid/last observed polls may occur with a current input row or globally. */
            IF_CHECK(if_error_during_work(&f, CBM_INVENTORY_CANCELLED, false) && f.calls <= 2);
        }
        IF_CHECK(c.observed >= stops[i]);
    }
    IF_CHECK(if_cancel_fixture(&f) && cbm_now_ms() > 1);
    f.control.deadline_ms = 1;
    IF_CHECK(if_error(&f, CBM_INVENTORY_DEADLINE, SIZE_MAX, false) && f.calls == 0);
    IF_CHECK(if_cancel_fixture(&f));
    c = (if_cancel){0, 1};
    f.control.deadline_ms = 1;
    f.control.cancelled = if_cancelled;
    f.control.context = &c;
    IF_CHECK(if_error(&f, CBM_INVENTORY_CANCELLED, SIZE_MAX, false) && f.calls == 0);
    result = 0;
done:
    return if_finish(&f, result);
}

static bool if_same_limits(const cbm_inventory_limits_t *a, const cbm_inventory_limits_t *b) {
    return a->max_files == b->max_files && a->max_directories == b->max_directories &&
           a->max_arena_bytes == b->max_arena_bytes &&
           a->max_ignore_arena_bytes == b->max_ignore_arena_bytes &&
           a->max_control_file_bytes == b->max_control_file_bytes &&
           a->max_control_total_bytes == b->max_control_total_bytes &&
           a->max_ignore_patterns == b->max_ignore_patterns &&
           a->max_probe_prefix_bytes == b->max_probe_prefix_bytes &&
           a->max_ignore_work == b->max_ignore_work &&
           a->max_verified_file_reads == b->max_verified_file_reads &&
           a->max_verified_content_bytes == b->max_verified_content_bytes;
}
static bool if_owned_metadata(const cbm_inventory_filter_view_t *v, const unsigned char sha[32]) {
    if (v->file_count != 2 || strcmp((const char *)v->files[0].path.data, ".gitignore") ||
        strcmp((const char *)v->files[1].path.data, "keep.c") || v->files[0].git_mode != 0100644 ||
        v->files[0].content_length != 7 || memcmp(v->files[0].content_sha256, sha, 32))
        return false;
    for (size_t i = 0; i < 40; i++)
        if (v->files[0].oid[i] != 'a')
            return false;
    for (size_t i = 40; i < 65; i++)
        if (v->files[0].oid[i] != 0)
            return false;
    for (size_t i = 0; i < 32; i++)
        if (v->manifest_sha256[i] != 0x4a)
            return false;
#ifdef _WIN32
    return !strcmp(v->native_root, "C:/inventory-fixture");
#else
    return !strcmp(v->native_root, "/inventory-fixture");
#endif
}
static bool if_owned_getters(if_fixture *f, const cbm_inventory_limits_t *original) {
    cbm_inventory_limits_t copied;
    cbm_inventory_filter_usage_t usage;
    const cbm_inventory_filter_view_t *v = cbm_inventory_filter_view(f->owner);
    if (!cbm_inventory_filter_limits(f->owner, &copied) || !if_same_limits(original, &copied) ||
        !cbm_inventory_filter_usage(f->owner, &usage))
        return false;
    copied.max_files = 1;
    memset(&usage, 0, sizeof(usage));
    if (!cbm_inventory_filter_limits(f->owner, &copied) || !if_same_limits(original, &copied) ||
        !cbm_inventory_filter_usage(f->owner, &usage) || usage.verified_file_reads_reserved != 1)
        return false;
    return v->rows[0].roles == CBM_INVENTORY_FILTER_ROLE_GITIGNORE &&
           v->rows[1].disposition == CBM_INVENTORY_FILTER_NEEDS_LANGUAGE &&
           v->controls[0].directory.length == 0 && v->controls[0].file_index == 0 &&
           v->controls[0].effective_patterns == 1 && !cbm_inventory_filter_usage(f->owner, NULL) &&
           !cbm_inventory_filter_limits(f->owner, NULL);
}
static bool if_null_getters(void) {
    cbm_inventory_limits_t limits;
    cbm_inventory_filter_usage_t usage;
    memset(&limits, 0xa5, sizeof(limits));
    memset(&usage, 0xa5, sizeof(usage));
    if (cbm_inventory_filter_view(NULL) || cbm_inventory_filter_limits(NULL, &limits) ||
        cbm_inventory_filter_usage(NULL, &usage))
        return false;
    cbm_inventory_limits_t zero = {0};
    if (!if_same_limits(&limits, &zero))
        return false;
    uint64_t values = usage.verified_file_reads_reserved | usage.verified_content_bytes_reserved |
                      usage.control_bytes_reserved | usage.ignore_bytes_reserved |
                      usage.ignore_work_used | usage.ignore_patterns_reserved |
                      usage.ignore_arena_requested_bytes | usage.non_ignore_arena_requested_bytes |
                      usage.arena_requested_bytes | usage.arena_budget_used_bytes;
    return !values && !cbm_inventory_filter_usage(NULL, NULL) &&
           !cbm_inventory_filter_limits(NULL, NULL);
}
int if_case_ownership(void) {
    if_fixture f;
    if_init(&f);
    int result = 1;
    if_cancel c = {0, SIZE_MAX};
    IF_CHECK(if_dependency_control() && if_text(&f, ".gitignore", "drop.c\n") &&
             if_text(&f, "keep.c", "body"));
    memset(f.files[0].oid + 41, 0xd5, 24);
    cbm_inventory_limits_t original = f.limits;
    unsigned char original_sha[32];
    memcpy(original_sha, f.files[0].content_sha256, 32);
    f.control.cancelled = if_cancelled;
    f.control.context = &c;
    IF_CHECK(if_prepare(&f));
    size_t polls = c.observed, calls = f.calls;
    const cbm_inventory_filter_view_t *v = cbm_inventory_filter_view(f.owner);
    IF_CHECK(v && v->files != f.files && v->native_root != f.root &&
             v->files[0].path.data != (const unsigned char *)f.paths[0]);
    memset(f.paths, 0xa5, sizeof(f.paths));
    memset(f.bytes, 0xa5, sizeof(f.bytes));
    memset(f.files, 0xa5, sizeof(f.files));
    memset(f.root, 0xa5, sizeof(f.root));
    memset(&f.source, 0xa5, sizeof(f.source));
    memset(&f.limits, 0xa5, sizeof(f.limits));
    memset(&f.control, 0xa5, sizeof(f.control));
    c.stop = 0;
    IF_CHECK(if_owned_metadata(v, original_sha) && if_owned_getters(&f, &original));
    IF_CHECK(c.observed == polls && f.calls == calls);
    cbm_inventory_filter_free(f.owner);
    f.owner = NULL;
    IF_CHECK(c.observed == polls && f.calls == calls && if_null_getters());
    cbm_inventory_filter_free(NULL);
    result = 0;
done:
    return if_finish(&f, result);
}
