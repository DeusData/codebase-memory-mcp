#include "test_test_impact_inventory_internal.h"

bool ni_write(if_native *n, const char *path, const void *bytes, size_t size) {
    FILE *file = cbm_fopen(path, "wb");
    if (!file)
        return false;
    bool ok = fwrite(bytes, 1, size, file) == size;
    if (fclose(file) != 0)
        if_native_stop(n, "adapter fixture file close uncertain; resources retained");
    return ok;
}

static bool ni_oid(if_native *n, const char *const *args, char oid[65]) {
    char output[80];
    if (!if_native_git(n, args, output, sizeof(output)))
        return false;
    size_t length = strlen(output);
    while (length && (output[length - 1] == '\r' || output[length - 1] == '\n'))
        output[--length] = 0;
    if (length != 40)
        return false;
    for (size_t i = 0; i < length; i++)
        if (!((output[i] >= '0' && output[i] <= '9') || (output[i] >= 'a' && output[i] <= 'f')))
            return false;
    memset(oid, 0, 65);
    memcpy(oid, output, length);
    return true;
}

static bool ni_object(if_native *n, const char *type, const void *bytes, size_t size,
                      char oid[65]) {
    const char *args[] = {"hash-object", "-w", "--no-filters", "-t", type, "--", n->payload, NULL};
    return ni_write(n, n->payload, bytes, size) && ni_oid(n, args, oid);
}

static bool ni_commit_index(if_native *n, char oid[65]) {
    char tree[65], bytes[512];
    const char *write[] = {"write-tree", NULL};
    if (!ni_oid(n, write, tree))
        return false;
    int count =
        snprintf(bytes, sizeof(bytes),
                 "tree %s\nauthor Native Adapter <fixture@example.invalid> 946684800 +0000\n"
                 "committer Native Adapter <fixture@example.invalid> 946684800 +0000\n\n"
                 "Native adapter fixture\n",
                 tree);
    return count > 0 && (size_t)count < sizeof(bytes) &&
           ni_object(n, "commit", bytes, (size_t)count, oid);
}

bool ni_ambient_change(if_native *n) {
    char path[IF_NATIVE_PATH], next[65], actual[65];
    const char changed[] = "source.c\n";
    const char *clear[] = {"read-tree", "--empty", NULL};
    const char *ref[] = {"update-ref", "refs/heads/topic", next, NULL};
    const char *head[] = {"rev-parse", "HEAD", NULL};
    return if_native_join(path, n->repo, ".gitignore") &&
           ni_write(n, path, changed, sizeof(changed) - 1) && if_native_git(n, clear, NULL, 0) &&
           ni_commit_index(n, next) && strcmp(next, n->head) != 0 &&
           if_native_git(n, ref, NULL, 0) && ni_oid(n, head, actual) && !strcmp(actual, next);
}

bool ni_tree(if_native *n, cbm_git_revision_t revision) {
    cbm_pinned_tree_options_t options = {
        .facts = n->facts,
        .revision = revision,
        .private_parent = n->parent,
        .limits = {.max_files = IF_ROWS,
                   .max_directories = 16,
                   .max_total_content_bytes = 16384,
                   .max_relative_path_bytes = IF_PATH,
                   .max_arena_bytes = 4 * 1024 * 1024,
                   .blob_batch = {.max_entries = IF_ROWS,
                                  .max_input_bytes = 16384,
                                  .max_arena_bytes = 4 * 1024 * 1024}},
        .control = {.deadline_ms = UINT64_MAX}};
    cbm_pinned_tree_error_t error;
    cbm_pinned_tree_status_t status = cbm_pinned_tree_create(&options, &n->input.tree, &error);
    if (error.git.status == CBM_GIT_FACTS_SUPERVISION)
        n->unquiesced = true;
    if (status != CBM_PINNED_TREE_OK)
        return false;
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(n->input.tree);
    if (!view || view->revision != revision || strcmp(view->commit, n->head) ||
        strlen(view->root) >= sizeof(n->input.root))
        return false;
    strcpy(n->input.root, view->root);
    n->input.source.native_root = n->input.root;
    memcpy(n->input.source.manifest_sha256, view->manifest_sha256, 32);
    return true;
}

bool ni_dependency(if_native *n, cbm_git_revision_t revision) {
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(n->input.tree);
    if (!view || !view->identity || view->revision != revision ||
        view->identity->oid_hex_length != 40 || strcmp(view->identity->head, n->head) ||
        strcmp(view->identity->merge_base, n->head) || strcmp(view->commit, n->head) ||
        view->file_count != n->input.source.file_count)
        return false;
    /* Each returned native view expires at the next valid read. Reacquire it. */
    for (size_t i = 0; i < n->input.source.file_count; i++) {
        view = cbm_pinned_tree_view(n->input.tree);
        const cbm_inventory_file_t *expected = &n->input.files[i];
        if (!view || strcmp((const char *)view->files[i].path, n->input.paths[i]) ||
            view->files[i].path_length != strlen(n->input.paths[i]) ||
            view->files[i].git_mode != expected->git_mode ||
            strcmp(view->files[i].oid, expected->oid) ||
            view->files[i].content_length != expected->content_length ||
            memcmp(view->files[i].content_sha256, expected->content_sha256, 32))
            return false;
        unsigned char bytes[IF_BYTES];
        size_t copied = SIZE_MAX;
        cbm_pinned_tree_error_t error;
        cbm_pinned_tree_control_t control = {.deadline_ms = UINT64_MAX};
        if (cbm_pinned_tree_read_prefix(n->input.tree, i, 16384, bytes, sizeof(bytes), &copied,
                                        &control, &error) != CBM_PINNED_TREE_OK ||
            error.status != CBM_PINNED_TREE_OK || copied != expected->content_length ||
            memcmp(bytes, n->input.bytes[i], copied))
            return false;
    }
    cbm_pinned_tree_error_t error;
    cbm_pinned_tree_control_t control = {.deadline_ms = UINT64_MAX};
    return cbm_pinned_tree_verify(n->input.tree, &control, &error) == CBM_PINNED_TREE_OK &&
           cbm_pinned_tree_view(n->input.tree) != NULL;
}

bool ni_start(if_native *n) {
    return if_native_start(n) && ni_dependency(n, CBM_GIT_REV_HEAD);
}

bool ni_reset_input(if_native *n) {
    if (!if_native_close(n))
        return false;
    cbm_git_facts_free(n->facts);
    n->facts = NULL;
    if_init(&n->input);
    return true;
}

bool ni_replace_snapshot(if_native *n, bool empty_controls) {
    if (!ni_reset_input(n))
        return false;
    if (empty_controls &&
        (!if_text(&n->input, ".cbmignore", "") || !if_text(&n->input, ".gitignore", "") ||
         !if_text(&n->input, "source.c", "int source;\n")))
        return false;
    return ni_commit_input(n);
}

/* Commit exactly n->input's files as HEAD, then open its facts, pinned tree
 * and dependency. Mode 100755 for the third file, as the default fixture. */
bool ni_commit_input(if_native *n) {
    const char *clear[] = {"read-tree", "--empty", NULL};
    if (!if_native_git(n, clear, NULL, 0))
        return false;
    for (size_t i = 0; i < n->input.source.file_count; i++) {
        cbm_inventory_file_t *file = &n->input.files[i];
        if (!ni_object(n, "blob", n->input.bytes[i], (size_t)file->content_length, file->oid))
            return false;
        char entry[IF_PATH + 80];
        file->git_mode = i == 2 ? 0100755 : 0100644;
        int length = snprintf(entry, sizeof(entry), "%o,%s,%s", (unsigned)file->git_mode, file->oid,
                              n->input.paths[i]);
        const char *add[] = {"update-index", "--add", "--cacheinfo", entry, NULL};
        if (length <= 0 || (size_t)length >= sizeof(entry) || !if_native_git(n, add, NULL, 0))
            return false;
    }
    const char *ref[] = {"update-ref", "refs/heads/topic", n->head, NULL};
    if (!ni_commit_index(n, n->head) || !if_native_git(n, ref, NULL, 0))
        return false;
    cbm_git_facts_options_t options = {.root = n->repo,
                                       .base_ref = "HEAD",
                                       .git_executable = n->git,
                                       .expected_head = n->head,
                                       .deadline_ms = UINT64_MAX,
                                       .command_limit = 512,
                                       .stdout_limit = 1024 * 1024,
                                       .stderr_limit = 65536,
                                       .total_output_limit = 8 * 1024 * 1024};
    cbm_git_facts_error_t error;
    n->facts = cbm_git_facts_open(&options, &error);
    if (error.status == CBM_GIT_FACTS_SUPERVISION)
        n->unquiesced = true;
    return n->facts && error.status == CBM_GIT_FACTS_OK && ni_tree(n, CBM_GIT_REV_HEAD) &&
           ni_dependency(n, CBM_GIT_REV_HEAD);
}

bool ni_prepare(if_native *n, cbm_test_impact_inventory_t **owner) {
    cbm_inventory_error_t error;
    memset(&error, 0xa5, sizeof(error));
    cbm_inventory_status_t status = cbm_test_impact_inventory_prepare(
        n->input.tree, &n->input.limits, &n->input.control, owner, &error);
    if (status != CBM_INVENTORY_OK || !*owner)
        fprintf(stderr, "native adapter status=%d error=%d index=%zu cleanup=%d\n", status,
                error.status, error.file_index, error.cleanup_required);
    return status == CBM_INVENTORY_OK && *owner && error.status == CBM_INVENTORY_OK &&
           error.file_index == SIZE_MAX && !error.cleanup_required && !error.diagnostic[0];
}

bool ni_error(if_native *n, cbm_inventory_status_t expected, size_t index, bool cleanup,
              bool allow_unknown_index) {
    cbm_test_impact_inventory_t *sentinel = (cbm_test_impact_inventory_t *)(uintptr_t)1;
    cbm_test_impact_inventory_t *owner = sentinel;
    cbm_inventory_error_t error;
    memset(&error, 0xa5, sizeof(error));
    cbm_inventory_status_t status = cbm_test_impact_inventory_prepare(
        n->input.tree, &n->input.limits, &n->input.control, &owner, &error);
    bool no_owner = owner == NULL;
    if (owner != sentinel)
        cbm_test_impact_inventory_free(owner);
    bool index_ok = allow_unknown_index ? error.file_index == SIZE_MAX ||
                                              error.file_index < n->input.source.file_count
                                        : error.file_index == index;
    bool diagnostic_ok = memchr(error.diagnostic, 0, sizeof(error.diagnostic)) != NULL;
    bool ok = status == expected && error.status == expected && no_owner && index_ok &&
              error.cleanup_required == cleanup && diagnostic_ok;
    if (!ok)
        fprintf(stderr, "native adapter error actual=%d expected=%d index=%zu cleanup=%d null=%d\n",
                status, expected, error.file_index, error.cleanup_required, no_owner);
    return ok;
}

bool ni_limits_equal(const cbm_inventory_limits_t *a, const cbm_inventory_limits_t *b) {
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

int ni_finish(if_native *n, cbm_test_impact_inventory_t *owner, int result) {
    cbm_test_impact_inventory_free(owner);
    return if_native_finish(n, result);
}
