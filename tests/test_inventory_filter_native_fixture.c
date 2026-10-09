#include "test_inventory_filter_native_internal.h"

_Noreturn void if_native_stop(const if_native *n, const char *reason) {
    fprintf(stderr,
            "inventory native fail-stop: %s; exit=2; fixture=%s parent=%s; "
            "no quiescence/disposal success is claimed\n",
            reason, n->home, n->parent);
    fflush(stderr);
    _Exit(2);
}

bool if_native_join(char *out, const char *base, const char *rel) {
    int n = snprintf(out, IF_NATIVE_PATH, "%s/%s", base, rel);
    return n > 0 && n < IF_NATIVE_PATH;
}
static bool if_native_write(const char *path, const void *bytes, size_t length) {
    FILE *file = cbm_fopen(path, "wb");
    if (!file)
        return false;
    bool ok = fwrite(bytes, 1, length, file) == length;
    return fclose(file) == 0 && ok;
}
static bool if_native_run(if_native *n, const cbm_proc_opts_t *options) {
    cbm_subprocess_t *process = NULL;
    if (cbm_subprocess_spawn(options, &process) != 0) {
        fprintf(stderr, "inventory native Git spawn failed errno=%d (%s)\n", errno,
                options->argv[5] ? options->argv[5] : "?");
        return false;
    }
    uint64_t deadline = cbm_now_ms() + 60000;
    bool cancelled = false;
    cbm_proc_result_t result = {0};
    for (;;) {
        cbm_proc_poll_t status = cbm_subprocess_poll(process, &result);
        if (status == CBM_PROC_POLL_TERMINAL)
            break;
        if (status == CBM_PROC_POLL_ERROR || cbm_now_ms() >= deadline) {
            if (!cancelled && status == CBM_PROC_POLL_ERROR)
                deadline = cbm_now_ms();
            cbm_subprocess_request_cancel(process);
            cancelled = true;
        }
        if (cancelled && cbm_now_ms() >= deadline + 5000) {
            n->unquiesced = true;
            if_native_stop(n, "process containment deadline expired; resources retained");
        }
        cbm_usleep(1000);
    }
    n->unquiesced = !result.tree_quiesced || result.supervision_failed;
    if (n->unquiesced)
        if_native_stop(n, "terminal subprocess containment unresolved; resources retained");
    cbm_subprocess_destroy(process);
    bool ok =
        !cancelled && !n->unquiesced && result.outcome == CBM_PROC_CLEAN && result.exit_code == 0;
    if (!ok)
        fprintf(stderr, "inventory native Git outcome=%d exit=%d quiescent=%d\n", result.outcome,
                result.exit_code, result.tree_quiesced);
    return ok;
}
bool if_native_git(if_native *n, const char *const *tail, char *out, size_t capacity) {
    const char *argv[40] = {n->git, "-C", n->repo, "-c", "core.autocrlf=false"};
    size_t used = 5;
    for (size_t i = 0; tail[i]; i++) {
        if (used + 1 >= 40)
            return false;
        argv[used++] = tail[i];
    }
    argv[used] = NULL;
    cbm_proc_opts_t options = {.bin = n->git,
                               .argv = argv,
                               .stdout_file = n->capture,
                               .log_file = n->log,
                               .strip_git_repo_env = true,
                               .quiet_timeout_ms = 10000,
                               .cancel_grace_ms = 1000};
    if (!if_native_run(n, &options))
        return false;
    if (!out)
        return true;
    FILE *file = cbm_fopen(n->capture, "rb");
    if (!file || capacity < 2) {
        if (file)
            fclose(file);
        return false;
    }
    size_t count = fread(out, 1, capacity - 1, file);
    out[count] = 0;
    bool ok = fgetc(file) == EOF && !ferror(file);
    return fclose(file) == 0 && ok;
}
static bool if_native_oid(if_native *n, const char *const *tail, char oid[65]) {
    char out[80];
    if (!if_native_git(n, tail, out, sizeof(out)))
        return false;
    size_t length = strlen(out);
    while (length && (out[length - 1] == '\r' || out[length - 1] == '\n'))
        out[--length] = 0;
    if (length != 40)
        return false;
    for (size_t i = 0; i < 40; i++)
        if (!((out[i] >= '0' && out[i] <= '9') || (out[i] >= 'a' && out[i] <= 'f')))
            return false;
    memcpy(oid, out, 41);
    return true;
}
static bool if_native_object(if_native *n, const char *type, const void *bytes, size_t length,
                             char oid[65]) {
    const char *args[] = {"hash-object", "-w", "--no-filters", "-t", type, "--", n->payload, NULL};
    return if_native_write(n->payload, bytes, length) && if_native_oid(n, args, oid);
}
static bool if_native_commit(if_native *n) {
    const char *clear[] = {"read-tree", "--empty", NULL};
    if (!if_native_git(n, clear, NULL, 0))
        return false;
    for (size_t i = 0; i < n->input.source.file_count; i++) {
        cbm_inventory_file_t *f = &n->input.files[i];
        if (!if_native_object(n, "blob", n->input.bytes[i], (size_t)f->content_length, f->oid))
            return false;
        char entry[IF_PATH + 80];
        int length = snprintf(entry, sizeof(entry), "100644,%s,%s", f->oid, n->input.paths[i]);
        const char *add[] = {"update-index", "--add", "--cacheinfo", entry, NULL};
        if (length <= 0 || (size_t)length >= sizeof(entry) || !if_native_git(n, add, NULL, 0))
            return false;
    }
    char tree[65], bytes[512];
    const char *write[] = {"write-tree", NULL};
    if (!if_native_oid(n, write, tree))
        return false;
    int length = snprintf(
        bytes, sizeof(bytes),
        "tree %s\nauthor Inventory Fixture <fixture@example.invalid> 946684800 +0000\ncommitter "
        "Inventory Fixture <fixture@example.invalid> 946684800 +0000\n\nInventory fixture\n",
        tree);
    if (length <= 0 || (size_t)length >= sizeof(bytes) ||
        !if_native_object(n, "commit", bytes, (size_t)length, n->head))
        return false;
    const char *ref[] = {"update-ref", "refs/heads/topic", n->head, NULL};
    return if_native_git(n, ref, NULL, 0);
}
static bool if_native_rows(if_fixture *f) {
    return if_text(f, ".cbmignore", "# native\n") &&
           if_text(f, ".codebase-memory.json", "not JSON") &&
           if_text(f, ".gitignore", "drop.c\n") && if_add(f, "a.data", "A\0Z", 3) &&
           if_text(f, "drop.c", "int drop;\n") && if_text(f, "plain", "#!/not/executed\n") &&
           if_text(f, "source.c", "int source;\n") && if_text(f, "sub/.gitignore", "nested.c\n") &&
           if_text(f, "sub/keep.c", "int keep;\n") && if_text(f, "sub/nested.c", "int nested;\n");
}
bool if_native_start(if_native *n) {
    memset(n, 0, sizeof(*n));
    if_init(&n->input);
    const char *home = th_mktempdir("cbm-inventory-filter");
    if (!home || strlen(home) >= sizeof(n->home))
        return false;
    strcpy(n->home, home);
    if (!if_native_join(n->repo, n->home, "repo") ||
        !if_native_join(n->capture, n->home, "stdout") ||
        !if_native_join(n->log, n->home, "stderr") ||
        !if_native_join(n->payload, n->home, "payload") || th_mkdir_p(n->repo) != 0 ||
        !if_native_private_parent(n) || !if_native_git_path(n->git, sizeof(n->git)))
        return false;
    const char *init[] = {"-c",      "init.templateDir=",    "init",
                          "--quiet", "--object-format=sha1", "--initial-branch=topic",
                          NULL};
    if (!if_native_git(n, init, NULL, 0) || !if_native_rows(&n->input) || !if_native_commit(n))
        return false;
    cbm_git_facts_options_t options = {.root = n->repo,
                                       .base_ref = "HEAD",
                                       .git_executable = n->git,
                                       .expected_head = n->head,
                                       .deadline_ms = cbm_now_ms() + 120000,
                                       .command_limit = 512,
                                       .stdout_limit = 1024 * 1024,
                                       .stderr_limit = 65536,
                                       .total_output_limit = 8 * 1024 * 1024};
    cbm_git_facts_error_t error;
    n->facts = cbm_git_facts_open(&options, &error);
    if (error.status == CBM_GIT_FACTS_SUPERVISION)
        n->unquiesced = true;
    const cbm_git_facts_identity_t *identity = cbm_git_facts_identity(n->facts);
    if (!identity || error.status != CBM_GIT_FACTS_OK || strcmp(identity->head, n->head) ||
        strcmp(identity->merge_base, n->head)) {
        fprintf(stderr, "inventory native facts status=%d identity=%d diagnostic=%s\n",
                error.status, identity != NULL, error.diagnostic);
        return false;
    }
    cbm_git_tree_inventory_t inventory;
    if (!cbm_git_facts_inventory(n->facts, CBM_GIT_REV_HEAD, &inventory, &error) ||
        inventory.count != n->input.source.file_count) {
        if (error.status == CBM_GIT_FACTS_SUPERVISION)
            n->unquiesced = true;
        fprintf(stderr, "inventory native inventory status=%d diagnostic=%s\n", error.status,
                error.diagnostic);
        return false;
    }
    for (size_t i = 0; i < inventory.count; i++)
        if (strcmp(inventory.entries[i].path, n->input.paths[i]) ||
            strcmp(inventory.entries[i].oid, n->input.files[i].oid)) {
            fprintf(stderr, "inventory native inventory row %zu differs: %s\n", i,
                    inventory.entries[i].path);
            return false;
        }
    return if_native_tree(n);
}
bool if_native_tree(if_native *n) {
    cbm_pinned_tree_options_t options = {
        .facts = n->facts,
        .revision = CBM_GIT_REV_HEAD,
        .private_parent = n->parent,
        .limits = {.max_files = IF_ROWS,
                   .max_directories = 16,
                   .max_total_content_bytes = 16384,
                   .max_relative_path_bytes = IF_PATH,
                   .max_arena_bytes = 4 * 1024 * 1024,
                   .blob_batch = {.max_entries = IF_ROWS,
                                  .max_input_bytes = 16384,
                                  .max_arena_bytes = 4 * 1024 * 1024}},
        .control = {.deadline_ms = cbm_now_ms() + 120000}};
    cbm_pinned_tree_error_t error;
    if (cbm_pinned_tree_create(&options, &n->input.tree, &error) != CBM_PINNED_TREE_OK) {
        if (error.git.status == CBM_GIT_FACTS_SUPERVISION)
            n->unquiesced = true;
        fprintf(stderr, "inventory native create status=%d cause=%d\n", error.status, error.cause);
        return false;
    }
    const cbm_pinned_tree_view_t *v = cbm_pinned_tree_view(n->input.tree);
    if (!v || v->revision != CBM_GIT_REV_HEAD || strcmp(v->commit, n->head) ||
        v->file_count != n->input.source.file_count) {
        fprintf(stderr, "inventory native view differs: files=%zu\n", v ? v->file_count : 0);
        return false;
    }
    for (size_t i = 0; i < v->file_count; i++) {
        if (strcmp((const char *)v->files[i].path, n->input.paths[i]) ||
            memcmp(v->files[i].content_sha256, n->input.files[i].content_sha256, 32)) {
            fprintf(stderr, "inventory native view row %zu differs: %s\n", i,
                    (const char *)v->files[i].path);
            return false;
        }
        cbm_inventory_file_t *f = &n->input.files[i];
        f->git_mode = v->files[i].git_mode;
        memcpy(f->oid, v->files[i].oid, 65);
        f->content_length = v->files[i].content_length;
    }
    if (strlen(v->root) >= sizeof(n->input.root))
        return false;
    strcpy(n->input.root, v->root);
    n->input.source.native_root = n->input.root;
    memcpy(n->input.source.manifest_sha256, v->manifest_sha256, 32);
    if_reset_reads(&n->input);
    return true;
}
bool if_native_dependency(if_native *n) {
    for (size_t i = 0; i < n->input.source.file_count; i++) {
        unsigned char bytes[IF_BYTES];
        size_t copied = SIZE_MAX;
        cbm_pinned_tree_error_t error;
        cbm_pinned_tree_control_t control = {.deadline_ms = UINT64_MAX};
        cbm_pinned_tree_status_t read = cbm_pinned_tree_read_prefix(
            n->input.tree, i, 16384, bytes, sizeof(bytes), &copied, &control, &error);
        if (read != CBM_PINNED_TREE_OK || copied != n->input.files[i].content_length ||
            memcmp(bytes, n->input.bytes[i], copied)) {
            fprintf(stderr, "inventory native read %zu status=%d copied=%zu want=%llu\n", i, read,
                    copied, (unsigned long long)n->input.files[i].content_length);
            return false;
        }
    }
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(n->input.tree);
    return view && view->file_count == 10;
}
bool if_native_close(if_native *n) {
    if (!n->input.tree)
        return true;
    if (n->close_attempted)
        if_native_stop(n, "native tree already had an unresolved disposal attempt");
    n->close_attempted = true;
    cbm_pinned_tree_error_t error;
    if (cbm_pinned_tree_close(&n->input.tree, &error) != CBM_PINNED_TREE_OK) {
        fprintf(stderr, "inventory native disposal retained status=%d cause=%d root=%s\n",
                error.status, error.cause, n->parent);
        if_native_stop(n, "native tree disposal unresolved; no retry");
    }
    if (n->input.tree)
        if_native_stop(n, "successful native disposal did not clear owner");
    n->close_attempted = false;
    return true;
}
int if_native_finish(if_native *n, int result) {
    if_finish(&n->input, 0);
    if (n->unquiesced)
        if_native_stop(n, "native provider containment unresolved; resources retained");
    bool closed = if_native_close(n);
    cbm_git_facts_free(n->facts);
    n->facts = NULL;
    if (!closed)
        if_native_stop(n, "native tree disposal unresolved; fixture retained");
    if (n->parent[0] && th_rmtree(n->parent) != 0)
        result = 1;
    if (n->home[0] && th_rmtree(n->home) != 0)
        result = 1;
    return result;
}
