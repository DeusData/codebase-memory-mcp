/*
 * pass_ansible.c — the Ansible layer.
 *
 * PHASE 7 P7.1: composition. A playbook or a task file -- a YAML file whose
 * top level is a sequence (manifests, values, CI and compose files are
 * mappings) -- IMPORTS {local_name: <directive>, via: ansible, line} what its
 * directives name:
 *   import_playbook: P                    the playbook P, from the file's
 *                                         directory
 *   include_tasks / import_tasks /
 *   include: T  (or {file: T})            the task file T, from the file's
 *                                         directory, else from the tasks/
 *                                         directory of the role holding it
 *   include_role / import_role: {name: R},
 *   roles: [R | {role: R} | {name: R}]    the role R: roles/R in the file's
 *                                         directory or above (a name may hold
 *                                         "/", roles/kubernetes/node), or a
 *                                         path ("./", "../") from the
 *                                         file's directory
 * (the ansible.builtin. prefix allowed). The edge goes from the file to the
 * target file's Module, or to the role's Folder; a target the graph does not
 * hold, or one holding a Jinja expression ({{ }}), makes none.
 *
 * PHASE 7 P7.2: tasks. A top-level sequence is not Ansible by itself (JSON
 * patches, image maps and service lists are sequences too): a file is
 * Ansible when it is a playbook (an item with hosts: / import_playbook:), a
 * role's tasks/ or handlers/ file, or -- to a fixpoint -- a task file an
 * Ansible file includes. Each of its tasks (a playbook's pre_tasks / tasks /
 * post_tasks, its handlers; a block's block / rescue / always, never the
 * block) is a Function named by its `name:`, else its module, QN
 * <file>::task/<n> or ::handler/<n> (document order), {source: ansible,
 * kind, module}, which the file DEFINES. A notify CALLS {via: notify, line}
 * every handler of that name or listen topic in the task's own role (the
 * directory before its tasks/), else in its file (a play's handlers). The
 * incremental repair declines on any change to Ansible content
 * (closure_ansible_decline): a notify joins two files.
 *
 * PHASE 7 P7.3: applies. A kube (kubespray's library module) or k8s
 * (kubernetes.core / community.kubernetes) task applies a manifest the play
 * rendered: it DEPLOYS {via: ansible, module, line} the repository file the
 * render read -- a template task's src (the role's templates/) or a copy
 * task's (its files/; never remote_src), else the task file's directory:
 *   a k8s template: argument            that template;
 *   filename == a renderer's dest (D)   that renderer's src;
 *   {{ item.item.K }} over "{{ R.results }}" (L)
 *                                       the src of the renderer registering
 *                                       R, for each item of its loop;
 *   {{ item.K }} (S)                    for each of the task's own items, the
 *                                       src of the renderer of that dest;
 *   a literal filename (R)              the src of each looped renderer item
 *                                       whose dest renders to it.
 * A loop is a literal list, or "{{ V }}": a set_fact of the file, else the
 * role's defaults/ or vars/ main.yml. Templates are searched before copies,
 * each in file order. The incremental repair declines on a change to a
 * role's defaults / vars main file (closure_ansible_decline), and when a
 * task file would join it as a dependent -- a deleted template it applies --
 * since the repair would walk it without its role's other files
 * (closure_ansible_dependent_decline).
 */
#include "foundation/constants.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/hash_table.h"
#include "foundation/limits.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "foundation/str_util.h" /* cbm_json_escape */
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "cbm.h"
#include "helpers.h"
#include "lang_specs.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { AN_GROW_MIN = 16 };

/* A notify waiting for every handler: the task, its role and file, the name. */
typedef struct {
    int64_t task;
    char *scope_role; /* "" outside a role */
    char *scope_file;
    char *name;
    int line;
} an_notify_t;

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    CBMHashTable *modules; /* file path -> its Module node */
    CBMHashTable *folders; /* directory -> its Folder node */
    const char *src;       /* the file's text */
    const char *file;      /* its repository path */
    char dir[CBM_SZ_1K];   /* its directory */
    char role[CBM_SZ_1K];  /* the role holding it (roles/<name>), or "" */
    int64_t from;          /* its File node */
    int edges;
    char **incs; /* pass 1: the task files includes name, in file order */
    int inc_n;
    int inc_cap;
    char base[CBM_SZ_1K];     /* pass 2: its File QN without `.__file__` */
    char role_dir[CBM_SZ_1K]; /* the role of its tasks / handlers, or "" */
    int ordinal[PAIR_LEN];    /* tasks, handlers so far in the file */
    CBMHashTable *handlers;   /* "<role or F:file>\x1f<name>" -> an_ids_t */
    an_notify_t *notes;
    int note_n;
    int note_cap;
    int tasks;
    int calls;
    CBMHashTable *file_nodes; /* P7.3: File nodes by path (what a task applies) */
    CBMHashTable *by_rel;     /* every input file's path -> its index + 1 */
    const cbm_file_info_t *files;
    TSParser *parser;          /* reads a role's defaults / vars */
    struct an_render *renders; /* per file: template / copy tasks */
    int render_n;
    int render_cap;
    struct an_apply *applies; /* per file: kube / k8s tasks */
    int apply_n;
    int apply_cap;
    struct an_fact *facts; /* per file: set_fact lists */
    int fact_n;
    int fact_cap;
    int deploys;
} an_t;

static bool an_type(TSNode n, const char *t) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), t) == 0;
}

/* A quoted scalar's escapes decoded in place: `''` in single quotes;
 * `\"`, `\\`, `\n`, `\t` (any other `\x`: x) in double quotes. */
static void an_unescape(char *s, bool dq) {
    char *w = s;
    for (const char *r = s; *r; r++) {
        if (dq && r[0] == '\\' && r[SKIP_ONE]) {
            r++;
            *w++ = *r == 'n' ? '\n' : *r == 't' ? '\t' : *r;
        } else if (!dq && r[0] == '\'' && r[SKIP_ONE] == '\'') {
            r++;
            *w++ = '\'';
        } else {
            *w++ = *r;
        }
    }
    *w = '\0';
}

/* A scalar's text (quotes stripped, escapes decoded) into out; false for
 * anything else. */
static bool an_scalar(const an_t *a, TSNode n, char *out, size_t sz) {
    while ((an_type(n, "flow_node") || an_type(n, "block_node") || an_type(n, "plain_scalar")) &&
           ts_node_named_child_count(n) == SKIP_ONE) {
        n = ts_node_named_child(n, 0);
    }
    if (ts_node_is_null(n)) { /* a pair without its key or value */
        return false;
    }
    uint32_t b = ts_node_start_byte(n);
    uint32_t e = ts_node_end_byte(n);
    bool dq = an_type(n, "double_quote_scalar");
    bool sq = an_type(n, "single_quote_scalar");
    if (dq || sq) {
        b++;
        e = e > b ? e - SKIP_ONE : b;
    } else if (!an_type(n, "string_scalar") && !an_type(n, "plain_scalar")) {
        return false;
    }
    if (e - b + SKIP_ONE > sz) {
        return false;
    }
    memcpy(out, a->src + b, e - b);
    out[e - b] = '\0';
    if (dq || sq) {
        an_unescape(out, dq);
    }
    return out[0] != '\0';
}

/* Is `n` an Ansible boolean that holds true -- y / yes / on / 1 / true / t,
 * any case (Ansible's boolean()), a YAML boolean or a string? an_scalar
 * reads strings only: `remote_src: true` is a boolean_scalar. */
static bool an_true(const an_t *a, TSNode n) {
    static const char *const TRUE_WORDS[] = {"y", "yes", "on", "1", "true", "t"};
    while ((an_type(n, "flow_node") || an_type(n, "block_node") || an_type(n, "plain_scalar")) &&
           ts_node_named_child_count(n) == SKIP_ONE) {
        n = ts_node_named_child(n, 0);
    }
    char v[CBM_SZ_16];
    if (an_type(n, "boolean_scalar") || an_type(n, "integer_scalar")) {
        uint32_t b = ts_node_start_byte(n);
        uint32_t e = ts_node_end_byte(n);
        if (e - b + SKIP_ONE > sizeof(v)) {
            return false;
        }
        memcpy(v, a->src + b, e - b);
        v[e - b] = '\0';
    } else if (!an_scalar(a, n, v, sizeof(v))) {
        return false;
    }
    for (char *c = v; *c; c++) {
        *c = (char)(*c >= 'A' && *c <= 'Z' ? *c - 'A' + 'a' : *c);
    }
    for (size_t i = 0; i < sizeof(TRUE_WORDS) / sizeof(TRUE_WORDS[0]); i++) {
        if (strcmp(v, TRUE_WORDS[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* The key of a mapping pair, its text, the ansible.builtin. prefix dropped. */
static bool an_key(const an_t *a, TSNode pair, char *out, size_t sz) {
    static const char BUILTIN[] = "ansible.builtin.";
    if (!an_scalar(a, ts_node_child_by_field_name(pair, "key", sizeof("key") - SKIP_ONE), out,
                   sz)) {
        return false;
    }
    size_t bl = sizeof(BUILTIN) - SKIP_ONE;
    size_t len = strlen(out);
    if (len >= bl && memcmp(out, BUILTIN, bl) == 0) {
        memmove(out, out + bl, len - bl + SKIP_ONE);
    }
    return true;
}

/* The value of `key` in a block mapping (under its block_node), or a null node. */
static TSNode an_get(const an_t *a, TSNode value, const char *key) {
    while (an_type(value, "block_node") && ts_node_named_child_count(value) == SKIP_ONE) {
        value = ts_node_named_child(value, 0);
    }
    for (uint32_t i = 0; an_type(value, "block_mapping") && i < ts_node_named_child_count(value);
         i++) {
        TSNode pair = ts_node_named_child(value, i);
        char k[CBM_SZ_64];
        if (an_type(pair, "block_mapping_pair") && an_key(a, pair, k, sizeof(k)) &&
            strcmp(k, key) == 0) {
            return ts_node_child_by_field_name(pair, "value", sizeof("value") - SKIP_ONE);
        }
    }
    return (TSNode){0};
}

static void an_edge(an_t *a, const cbm_gbuf_node_t *tgt, const char *kind, TSNode pair) {
    if (!tgt || a->from <= 0 || tgt->id == a->from) {
        return;
    }
    char props[CBM_SZ_128];
    snprintf(props, sizeof(props), "{\"local_name\":\"%s\",\"via\":\"ansible\",\"line\":%d}", kind,
             (int)ts_node_start_point(pair).row + SKIP_ONE);
    cbm_gbuf_insert_edge(a->ctx->gbuf, a->from, tgt->id, "IMPORTS", props);
    a->edges++;
}

/* Pass 1 keeps the task files an include names: they are Ansible content
 * even outside a role (an_mark_ansible). */
static void an_include(an_t *a, const cbm_gbuf_node_t *target) {
    if (!target || !target->file_path) {
        return;
    }
    if (a->inc_n == a->inc_cap) {
        int cap = a->inc_cap ? a->inc_cap * PAIR_LEN : AN_GROW_MIN;
        char **grown = cbm_realloc(CBM_MEM_CLASS_OTHER, a->incs, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return;
        }
        a->incs = grown;
        a->inc_cap = cap;
    }
    a->incs[a->inc_n++] = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, target->file_path);
}

/* A file `v` from `base` (repository-relative), its Module; NULL when none. */
static const cbm_gbuf_node_t *an_file(const an_t *a, const char *base, const char *v) {
    char path[CBM_SZ_1K];
    if (!cbm_repo_rel_join(base, v, path, sizeof(path))) {
        return NULL;
    }
    return cbm_ht_get(a->modules, path);
}

/* Role `name`'s Folder: a path ("./", "../") from the file's directory, else
 * roles/<name> in that directory or above. */
static const cbm_gbuf_node_t *an_role(const an_t *a, const char *name) {
    char path[CBM_SZ_1K];
    if (name[0] == '.' || name[0] == '/') {
        return cbm_repo_rel_join(a->dir, name, path, sizeof(path)) ? cbm_ht_get(a->folders, path)
                                                                   : NULL;
    }
    char d[CBM_SZ_1K];
    snprintf(d, sizeof(d), "%s", a->dir);
    for (;;) {
        snprintf(path, sizeof(path), "%s%sroles/%s", d, d[0] ? "/" : "", name);
        const cbm_gbuf_node_t *f = cbm_ht_get(a->folders, path);
        if (f || !d[0]) {
            return f;
        }
        char *s = strrchr(d, '/');
        if (s) {
            *s = '\0';
        } else {
            d[0] = '\0';
        }
    }
}

/* One mapping pair of a task or a play. */
static void an_pair(an_t *a, TSNode pair) {
    char key[CBM_SZ_64];
    char v[CBM_SZ_1K];
    if (!an_key(a, pair, key, sizeof(key))) {
        return;
    }
    TSNode value = ts_node_child_by_field_name(pair, "value", sizeof("value") - SKIP_ONE);
    if (strcmp(key, "import_playbook") == 0) {
        if (an_scalar(a, value, v, sizeof(v)) && !strstr(v, "{{")) {
            an_edge(a, an_file(a, a->dir, v), key, pair);
        }
    } else if (strcmp(key, "include_tasks") == 0 || strcmp(key, "import_tasks") == 0 ||
               strcmp(key, "include") == 0) {
        if ((an_scalar(a, value, v, sizeof(v)) ||
             an_scalar(a, an_get(a, value, "file"), v, sizeof(v))) &&
            !strstr(v, "{{")) {
            char tasks[CBM_SZ_1K];
            snprintf(tasks, sizeof(tasks), "%s/tasks", a->role);
            const cbm_gbuf_node_t *t = an_file(a, a->dir, v);
            const cbm_gbuf_node_t *target = t ? t : a->role[0] ? an_file(a, tasks, v) : NULL;
            an_edge(a, target, key, pair);
            an_include(a, target);
        }
    } else if (strcmp(key, "include_role") == 0 || strcmp(key, "import_role") == 0) {
        if (an_scalar(a, an_get(a, value, "name"), v, sizeof(v)) && !strstr(v, "{{")) {
            an_edge(a, an_role(a, v), key, pair);
        }
    } else if (strcmp(key, "roles") == 0) {
        while (an_type(value, "block_node") && ts_node_named_child_count(value) == SKIP_ONE) {
            value = ts_node_named_child(value, 0);
        }
        for (uint32_t i = 0;
             an_type(value, "block_sequence") && i < ts_node_named_child_count(value); i++) {
            TSNode item = ts_node_named_child(ts_node_named_child(value, i), 0);
            if ((an_scalar(a, item, v, sizeof(v)) ||
                 an_scalar(a, an_get(a, item, "role"), v, sizeof(v)) ||
                 an_scalar(a, an_get(a, item, "name"), v, sizeof(v))) &&
                !strstr(v, "{{")) {
                an_edge(a, an_role(a, v), key, ts_node_named_child(value, i));
            }
        }
    }
}

/* Every mapping that is a sequence item (a play or a task, also one nested in
 * block: / tasks: / handlers: ...): its pairs. */
static void an_walk(an_t *a, TSNode root) {
    TSTreeCursor cur = ts_tree_cursor_new(root);
    bool more = true;
    while (more) {
        TSNode c = ts_tree_cursor_current_node(&cur);
        if (an_type(c, "block_mapping")) {
            TSNode up = ts_node_parent(ts_node_parent(c));
            for (uint32_t i = 0;
                 an_type(up, "block_sequence_item") && i < ts_node_named_child_count(c); i++) {
                if (an_type(ts_node_named_child(c, i), "block_mapping_pair")) {
                    an_pair(a, ts_node_named_child(c, i));
                }
            }
        }
        if (ts_tree_cursor_goto_first_child(&cur)) {
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cur)) {
            if (!ts_tree_cursor_goto_parent(&cur)) {
                more = false;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cur);
}

/* The first content line of `src` starts a sequence item: Ansible content
 * (a playbook, a task list), not a mapping (manifest, values, CI, compose). */
static bool an_is_sequence(const char *src) {
    for (const char *p = src; *p;) {
        const char *nl = strchr(p, '\n');
        size_t n = nl ? (size_t)(nl - p) : strlen(p);
        size_t s = 0;
        while (s < n && (p[s] == ' ' || p[s] == '\t' || p[s] == '\r')) {
            s++;
        }
        bool skip = s == n || p[s] == '#' ||
                    (n - s >= sizeof("---") - SKIP_ONE &&
                     strncmp(p + s, "---", sizeof("---") - SKIP_ONE) == 0);
        if (!skip) {
            return s == 0 && n >= PAIR_LEN && p[0] == '-' && p[SKIP_ONE] == ' ';
        }
        p = nl ? nl + SKIP_ONE : p + n;
    }
    return false;
}

/* The role holding `rel`: roles/<name> of the deepest "roles" component with
 * a name after it, or "". */
static void an_role_of(const char *rel, char *out, size_t sz) {
    out[0] = '\0';
    for (const char *p = rel; (p = strstr(p, "roles/")) != NULL; p++) {
        if (p != rel && p[-1] != '/') {
            continue;
        }
        const char *name = p + sizeof("roles/") - SKIP_ONE;
        const char *end = strchr(name, '/');
        if (end && strchr(end + SKIP_ONE, '/')) {
            snprintf(out, sz, "%.*s", (int)(end - rel), rel);
        }
    }
}

static char *an_read(const char *path, size_t *len) {
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (size <= 0 || size > cbm_max_file_bytes()) {
        (void)fclose(f);
        return NULL;
    }
    enum { AN_TS_PAD = 16 }; /* tree-sitter's lexer reads past the end */
    char *buf = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)size + AN_TS_PAD);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t got = fread(buf, SKIP_ONE, (size_t)size, f);
    (void)fclose(f);
    memset(buf + got, 0, AN_TS_PAD);
    *len = got;
    return buf;
}

/* ── Tasks (P7.2) ─────────────────────────────────────────────────── */

/* Task keywords: every other key of a task is its module. */
static const char *const AN_KEYWORDS[] = {"name",
                                          "when",
                                          "tags",
                                          "become",
                                          "become_user",
                                          "become_method",
                                          "register",
                                          "loop",
                                          "with_items",
                                          "with_dict",
                                          "with_list",
                                          "with_fileglob",
                                          "with_nested",
                                          "with_together",
                                          "with_sequence",
                                          "with_subelements",
                                          "with_first_found",
                                          "loop_control",
                                          "notify",
                                          "listen",
                                          "ignore_errors",
                                          "changed_when",
                                          "failed_when",
                                          "delegate_to",
                                          "run_once",
                                          "vars",
                                          "environment",
                                          "until",
                                          "retries",
                                          "delay",
                                          "no_log",
                                          "check_mode",
                                          "diff",
                                          "any_errors_fatal",
                                          "throttle",
                                          "timeout",
                                          "args",
                                          "action",
                                          "local_action",
                                          "async",
                                          "poll",
                                          "debugger",
                                          "collections",
                                          "module_defaults",
                                          "remote_user",
                                          "connection",
                                          "port",
                                          "ignore_unreachable",
                                          "block",
                                          "rescue",
                                          "always",
                                          "delegate_facts"};

static bool an_keyword(const char *k) {
    for (size_t i = 0; i < sizeof(AN_KEYWORDS) / sizeof(AN_KEYWORDS[0]); i++) {
        if (strcmp(k, AN_KEYWORDS[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* The first named child of `n` of type `t`, or a null node. */
static TSNode an_child(TSNode n, const char *t) {
    for (uint32_t i = 0; !ts_node_is_null(n) && i < ts_node_named_child_count(n); i++) {
        if (an_type(ts_node_named_child(n, i), t)) {
            return ts_node_named_child(n, i);
        }
    }
    return (TSNode){0};
}

/* A value as a block sequence (under its block_node), or a null node. */
static TSNode an_seq(TSNode v) {
    while (an_type(v, "block_node") && ts_node_named_child_count(v) == SKIP_ONE) {
        v = ts_node_named_child(v, 0);
    }
    return an_type(v, "block_sequence") ? v : (TSNode){0};
}

/* The top-level block sequence of a parsed file, or a null node. */
static TSNode an_top(TSNode root) {
    return an_seq(an_child(an_child(root, "document"), "block_node"));
}

/* Item `i` of block sequence `seq` as a block mapping, or a null node. */
static TSNode an_item(TSNode seq, uint32_t i) {
    TSNode node = ts_node_named_child(ts_node_named_child(seq, i), 0);
    return an_type(node, "block_node") ? an_child(node, "block_mapping") : (TSNode){0};
}

/* Does block mapping `m` hold key `key` (the ansible.builtin. prefix allowed)? */
static bool an_has(const an_t *a, TSNode m, const char *key) {
    for (uint32_t i = 0; an_type(m, "block_mapping") && i < ts_node_named_child_count(m); i++) {
        char k[CBM_SZ_64];
        if (an_key(a, ts_node_named_child(m, i), k, sizeof(k)) && strcmp(k, key) == 0) {
            return true;
        }
    }
    return false;
}

/* A playbook: a top-level item is a play (hosts:) or imports one. */
static bool an_is_playbook(const an_t *a, TSNode top) {
    for (uint32_t i = 0; !ts_node_is_null(top) && i < ts_node_named_child_count(top); i++) {
        TSNode m = an_item(top, i);
        if (an_has(a, m, "hosts") || an_has(a, m, "import_playbook")) {
            return true;
        }
    }
    return false;
}

/* The role a task or handler file belongs to: the directory before the first
 * tasks/ or handlers/ under the last roles/ component that has one
 * (roles/kubernetes-apps/csi_driver/cinder for .../cinder/tasks/main.yml);
 * "" outside a role's tasks / handlers. */
static void an_role_dir(const char *rel, char *out, size_t sz) {
    out[0] = '\0';
    for (const char *p = rel; (p = strstr(p, "roles/")) != NULL; p++) {
        if (p != rel && p[-1] != '/') {
            continue;
        }
        /* the role's name: one character at least, then its tasks/ or handlers/ */
        const char *from = p + sizeof("roles/");
        if (from > p + strlen(p)) {
            break;
        }
        const char *t = strstr(from, "/tasks/");
        const char *h = strstr(from, "/handlers/");
        const char *end = t && (!h || t < h) ? t : h;
        if (end) {
            snprintf(out, sz, "%.*s", (int)(end - rel), rel);
        }
    }
}

/* Ids by key ("<scope>\x1f<name>"). */
typedef struct {
    int64_t *items;
    int count;
    int cap;
} an_ids_t;

static void an_ids_add(CBMHashTable *ix, const char *key, int64_t id) {
    an_ids_t *v = cbm_ht_get(ix, key);
    if (!v) {
        v = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*v));
        char *owned = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, key);
        if (!v || !owned) {
            cbm_free(CBM_MEM_CLASS_OTHER, v);
            cbm_free(CBM_MEM_CLASS_OTHER, owned);
            return;
        }
        cbm_ht_set(ix, owned, v);
    }
    if (v->count > 0 && v->items[v->count - SKIP_ONE] == id) {
        return; /* a handler named and listening alike */
    }
    if (v->count == v->cap) {
        int cap = v->cap ? v->cap * PAIR_LEN : AN_GROW_MIN;
        int64_t *grown = cbm_realloc(CBM_MEM_CLASS_OTHER, v->items, (size_t)cap * sizeof(int64_t));
        if (!grown) {
            return;
        }
        v->items = grown;
        v->cap = cap;
    }
    v->items[v->count++] = id;
}

/* A task list being walked: its sequence, the next item, handlers or not. */
typedef struct {
    TSNode seq;
    uint32_t next;
    bool handler;
} an_frame_t;

/* How many names a notify / listen value gives (a scalar: one; a block or
 * flow list: its items), its list node into `*list` (null for a scalar). */
static uint32_t an_name_count(const an_t *a, TSNode v, TSNode *list) {
    char probe[CBM_SZ_256];
    *list = (TSNode){0};
    if (an_scalar(a, v, probe, sizeof(probe))) {
        return SKIP_ONE;
    }
    while ((an_type(v, "block_node") || an_type(v, "flow_node")) &&
           ts_node_named_child_count(v) == SKIP_ONE) {
        v = ts_node_named_child(v, 0);
    }
    if (!an_type(v, "block_sequence") && !an_type(v, "flow_sequence")) {
        return 0;
    }
    *list = v;
    return ts_node_named_child_count(v);
}

/* Name `i` of a notify / listen value (see an_name_count). */
static bool an_name_at(const an_t *a, TSNode v, TSNode list, uint32_t i, char *out, size_t sz) {
    if (ts_node_is_null(list)) {
        return i == 0 && an_scalar(a, v, out, sz);
    }
    TSNode item = ts_node_named_child(list, i);
    if (an_type(list, "block_sequence")) {
        item = ts_node_named_child(item, 0);
    }
    return an_scalar(a, item, out, sz);
}

/* ── Applies (P7.3) ───────────────────────────────────────────────── */

/* One item of a loop list: a scalar, or a mapping's scalar pairs. */
typedef struct {
    char *scalar;
    char **keys;
    char **vals;
    int n;
} an_item_t;

/* A loop (with_items / loop): its literal items, or its expression
 * ("{{ X }}", "{{ R.results }}"). */
typedef struct {
    an_item_t *items;
    int n;
    char *expr;
} an_loop_t;

/* A task rendering a file: template (src in the role's templates/) or copy
 * of a repository file (its files/). */
typedef struct an_render {
    char *src;
    char *dest;
    char *reg;
    an_loop_t loop;
    bool copy;
} an_render_t;

/* A task applying a manifest: kube / k8s, its filename (src) or template
 * argument, its loop. */
typedef struct an_apply {
    int64_t task;
    int line;
    char *module;
    char *fname;
    char *tmpl;
    an_loop_t loop;
} an_apply_t;

/* A list a set_fact of the file names. */
typedef struct an_fact {
    char *name;
    an_loop_t list;
} an_fact_t;

static const char *const AN_APPLIERS[] = {"kube", "k8s", "kubernetes.core.k8s",
                                          "community.kubernetes.k8s"};

static void *an_grow(void *items, int *cap, int count, size_t elem) {
    if (count < *cap) {
        return items;
    }
    int ncap = *cap ? *cap * PAIR_LEN : AN_GROW_MIN;
    void *grown = cbm_realloc(CBM_MEM_CLASS_OTHER, items, (size_t)ncap * elem);
    if (grown) {
        *cap = ncap;
    }
    return grown;
}

static char *an_dup(const char *s) {
    return s ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, s) : NULL;
}

/* A value node without its block_node / flow_node wrappers. */
static TSNode an_bare(TSNode v) {
    while ((an_type(v, "block_node") || an_type(v, "flow_node")) &&
           ts_node_named_child_count(v) == SKIP_ONE) {
        v = ts_node_named_child(v, 0);
    }
    return v;
}

/* A loop item: a scalar, or the scalar pairs of a block / flow mapping. */
static void an_item_parse(const an_t *a, TSNode node, an_item_t *it) {
    char buf[CBM_SZ_1K];
    *it = (an_item_t){0};
    if (an_scalar(a, node, buf, sizeof(buf))) {
        it->scalar = an_dup(buf);
        return;
    }
    TSNode m = an_bare(node);
    if (!an_type(m, "block_mapping") && !an_type(m, "flow_mapping")) {
        return;
    }
    int cap = 0;
    int vcap = 0;
    for (uint32_t i = 0; i < ts_node_named_child_count(m); i++) {
        TSNode pair = ts_node_named_child(m, i);
        char k[CBM_SZ_128];
        if (!an_type(pair, "block_mapping_pair") && !an_type(pair, "flow_pair")) {
            continue;
        }
        if (!an_scalar(a, ts_node_child_by_field_name(pair, "key", sizeof("key") - SKIP_ONE), k,
                       sizeof(k)) ||
            !an_scalar(a, ts_node_child_by_field_name(pair, "value", sizeof("value") - SKIP_ONE),
                       buf, sizeof(buf))) {
            continue;
        }
        char **keys = an_grow(it->keys, &cap, it->n, sizeof(char *));
        if (keys) {
            it->keys = keys;
        }
        char **vals = keys ? an_grow(it->vals, &vcap, it->n, sizeof(char *)) : NULL;
        if (!keys || !vals) {
            return;
        }
        it->vals = vals;
        it->keys[it->n] = an_dup(k);
        it->vals[it->n++] = an_dup(buf);
    }
}

/* A loop value: an expression (a scalar holding {{ }}, or a list of exactly
 * one -- with_items flattens one level), else a literal list of items. */
static void an_loop_parse(const an_t *a, TSNode v, an_loop_t *out) {
    char buf[CBM_SZ_1K];
    *out = (an_loop_t){0};
    if (an_scalar(a, v, buf, sizeof(buf))) {
        out->expr = strstr(buf, "{{") ? an_dup(buf) : NULL;
        return;
    }
    TSNode s = an_bare(v);
    bool block = an_type(s, "block_sequence");
    if (!block && !an_type(s, "flow_sequence")) {
        return;
    }
    int cap = 0;
    for (uint32_t i = 0; i < ts_node_named_child_count(s); i++) {
        TSNode item = ts_node_named_child(s, i);
        if (block ? !an_type(item, "block_sequence_item") : !an_type(item, "flow_node")) {
            continue;
        }
        if (block) {
            item = ts_node_named_child(item, 0);
        }
        an_item_t *grown = an_grow(out->items, &cap, out->n, sizeof(an_item_t));
        if (!grown) {
            return;
        }
        out->items = grown;
        an_item_parse(a, item, &out->items[out->n++]);
    }
    if (out->n == SKIP_ONE && out->items[0].scalar && strstr(out->items[0].scalar, "{{")) {
        out->expr = out->items[0].scalar;
        out->items[0].scalar = NULL;
        cbm_free(CBM_MEM_CLASS_OTHER, out->items);
        out->items = NULL;
        out->n = 0;
    }
}

static void an_loop_free(an_loop_t *l) {
    for (int i = 0; i < l->n; i++) {
        an_item_t *it = &l->items[i];
        for (int k = 0; k < it->n; k++) {
            cbm_free(CBM_MEM_CLASS_OTHER, it->keys[k]);
            cbm_free(CBM_MEM_CLASS_OTHER, it->vals[k]);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, it->keys);
        cbm_free(CBM_MEM_CLASS_OTHER, it->vals);
        cbm_free(CBM_MEM_CLASS_OTHER, it->scalar);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, l->items);
    cbm_free(CBM_MEM_CLASS_OTHER, l->expr);
    *l = (an_loop_t){0};
}

/* The task's loop: with_items, else loop. */
static void an_task_loop(const an_t *a, TSNode m, an_loop_t *out) {
    TSNode v = an_get(a, m, "with_items");
    an_loop_parse(a, ts_node_is_null(v) ? an_get(a, m, "loop") : v, out);
}

/* What task `m` (Function `id`) renders, applies or sets, for an_link_applies. */
static void an_collect(an_t *a, TSNode m, int64_t id) {
    char k[CBM_SZ_128];
    char v[CBM_SZ_1K];
    for (uint32_t i = 0; i < ts_node_named_child_count(m); i++) {
        TSNode pair = ts_node_named_child(m, i);
        if (!an_key(a, pair, k, sizeof(k))) {
            continue;
        }
        TSNode value = ts_node_child_by_field_name(pair, "value", sizeof("value") - SKIP_ONE);
        bool copy = strcmp(k, "copy") == 0;
        if (strcmp(k, "template") == 0 || copy) {
            TSNode args = an_bare(value);
            if (!an_type(args, "block_mapping") && !an_type(args, "flow_mapping")) {
                continue;
            }
            if (copy && an_true(a, an_get(a, value, "remote_src"))) {
                continue;
            }
            an_render_t *r = an_grow(a->renders, &a->render_cap, a->render_n, sizeof(*r));
            if (!r) {
                continue;
            }
            a->renders = r;
            r = &a->renders[a->render_n++];
            *r = (an_render_t){.copy = copy};
            r->src = an_scalar(a, an_get(a, value, "src"), v, sizeof(v)) ? an_dup(v) : NULL;
            r->dest = an_scalar(a, an_get(a, value, "dest"), v, sizeof(v)) ? an_dup(v) : NULL;
            r->reg = an_scalar(a, an_get(a, m, "register"), v, sizeof(v)) ? an_dup(v) : NULL;
            an_task_loop(a, m, &r->loop);
        } else if (strcmp(k, "set_fact") == 0) {
            TSNode f = an_bare(value);
            for (uint32_t j = 0; an_type(f, "block_mapping") && j < ts_node_named_child_count(f);
                 j++) {
                TSNode fp = ts_node_named_child(f, j);
                char name[CBM_SZ_128];
                TSNode fv = ts_node_child_by_field_name(fp, "value", sizeof("value") - SKIP_ONE);
                TSNode seq = an_bare(fv);
                an_fact_t *g =
                    (an_type(fp, "block_mapping_pair") && an_key(a, fp, name, sizeof(name)) &&
                     (an_type(seq, "block_sequence") || an_type(seq, "flow_sequence")))
                        ? an_grow(a->facts, &a->fact_cap, a->fact_n, sizeof(*g))
                        : NULL;
                if (g) {
                    a->facts = g;
                    a->facts[a->fact_n].name = an_dup(name);
                    an_loop_parse(a, fv, &a->facts[a->fact_n++].list);
                }
            }
        } else {
            for (size_t j = 0; j < sizeof(AN_APPLIERS) / sizeof(AN_APPLIERS[0]); j++) {
                if (strcmp(k, AN_APPLIERS[j]) != 0) {
                    continue;
                }
                an_apply_t *p = an_grow(a->applies, &a->apply_cap, a->apply_n, sizeof(*p));
                if (!p) {
                    break;
                }
                a->applies = p;
                p = &a->applies[a->apply_n++];
                *p = (an_apply_t){.task = id,
                                  .line = (int)ts_node_start_point(m).row + SKIP_ONE,
                                  .module = an_dup(k)};
                if (an_scalar(a, an_get(a, value, "template"), v, sizeof(v))) {
                    p->tmpl = an_dup(v);
                } else if (an_scalar(a, an_get(a, value, "filename"), v, sizeof(v)) ||
                           an_scalar(a, an_get(a, value, "src"), v, sizeof(v))) {
                    p->fname = an_dup(v);
                }
                an_task_loop(a, m, &p->loop);
                break;
            }
        }
    }
}

/* The trimmed inside of a {{ }}, `n` bytes at `b`: "item" (key length 0) or
 * "item.K", K an identifier (into key / kl); false for anything else. */
static bool an_item_ref(const char *b, size_t n, const char **key, size_t *kl) {
    static const char ITEM[] = "item";
    size_t il = sizeof(ITEM) - SKIP_ONE;
    if (n < il || strncmp(b, ITEM, il) != 0) {
        return false;
    }
    *key = b + il;
    *kl = 0;
    if (n == il) {
        return true;
    }
    if (b[il] != '.' || n == il + SKIP_ONE) {
        return false;
    }
    *key = b + il + SKIP_ONE;
    *kl = n - il - SKIP_ONE;
    for (size_t i = 0; i < *kl; i++) {
        char c = (*key)[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' ||
              (i > 0 && c >= '0' && c <= '9'))) {
            return false;
        }
    }
    return true;
}

/* `pattern` with each {{ item }} / {{ item.K }} replaced from `it`; false
 * when one cannot be, or -- `strict` -- when any other {{ }} is left. */
static bool an_subst(const char *pattern, const an_item_t *it, bool strict, char *out, size_t sz) {
    size_t w = 0;
    for (const char *p = pattern; p && *p;) {
        const char *end = p[0] == '{' && p[SKIP_ONE] == '{' ? strstr(p + PAIR_LEN, "}}") : NULL;
        const char *piece = p;
        size_t len = SKIP_ONE;
        if (end) {
            const char *b = p + PAIR_LEN;
            while (*b == ' ' || *b == '\t') {
                b++;
            }
            size_t n = (size_t)(end - b); /* b stops at end's '}' at the latest */
            while (n > 0 && (b[n - SKIP_ONE] == ' ' || b[n - SKIP_ONE] == '\t')) {
                n--;
            }
            const char *key = NULL;
            size_t kl = 0;
            if (an_item_ref(b, n, &key, &kl)) {
                piece = kl == 0 ? it->scalar : NULL;
                for (int k = 0; kl > 0 && k < it->n && !piece; k++) {
                    if (strlen(it->keys[k]) == kl && strncmp(it->keys[k], key, kl) == 0) {
                        piece = it->vals[k];
                    }
                }
                if (!piece) {
                    return false;
                }
                len = strlen(piece);
            } else if (strict) {
                return false;
            } else {
                len = (size_t)(end + PAIR_LEN - p);
            }
            p = end + PAIR_LEN;
        } else {
            p++;
        }
        if (w + len >= sz) {
            return false;
        }
        memcpy(out + w, piece, len);
        w += len;
    }
    out[w] = '\0';
    return true;
}

/* The repository file a template / copy `src` names: in the role's
 * templates/ (files/ for a copy), else the task file's directory. */
static const cbm_gbuf_node_t *an_src_file(const an_t *a, const char *src, bool copy) {
    char base[CBM_SZ_1K];
    char path[CBM_SZ_1K];
    if (!src || strstr(src, "{{")) {
        return NULL;
    }
    if (a->role_dir[0]) {
        snprintf(base, sizeof(base), "%s/%s", a->role_dir, copy ? "files" : "templates");
        const cbm_gbuf_node_t *n = cbm_repo_rel_join(base, src, path, sizeof(path))
                                       ? cbm_ht_get(a->file_nodes, path)
                                       : NULL;
        if (n) {
            return n;
        }
    }
    return cbm_repo_rel_join(a->dir, src, path, sizeof(path)) ? cbm_ht_get(a->file_nodes, path)
                                                              : NULL;
}

/* A top-level list `name` of the role's defaults/ or vars/ main.yml, into
 * `out`; false when none holds one. */
static bool an_role_list(an_t *a, const char *name, an_loop_t *out) {
    static const char *const FILES[] = {"defaults/main.yml", "defaults/main.yaml", "vars/main.yml",
                                        "vars/main.yaml"};
    bool found = false;
    for (size_t f = 0; a->role_dir[0] && !found && f < sizeof(FILES) / sizeof(FILES[0]); f++) {
        char rel[CBM_SZ_1K];
        snprintf(rel, sizeof(rel), "%s/%s", a->role_dir, FILES[f]);
        intptr_t ix = (intptr_t)cbm_ht_get(a->by_rel, rel);
        size_t len = 0;
        char *src = ix > 0 ? an_read(a->files[ix - SKIP_ONE].path, &len) : NULL;
        TSTree *tree = src ? ts_parser_parse_string(a->parser, NULL, src, (uint32_t)len) : NULL;
        TSNode top =
            tree ? an_bare(an_child(an_child(ts_tree_root_node(tree), "document"), "block_node"))
                 : (TSNode){0};
        const char *saved = a->src;
        a->src = src;
        for (uint32_t i = 0;
             an_type(top, "block_mapping") && !found && i < ts_node_named_child_count(top); i++) {
            TSNode pair = ts_node_named_child(top, i);
            char k[CBM_SZ_128];
            TSNode v = ts_node_child_by_field_name(pair, "value", sizeof("value") - SKIP_ONE);
            TSNode seq = an_bare(v);
            if (an_key(a, pair, k, sizeof(k)) && strcmp(k, name) == 0 &&
                (an_type(seq, "block_sequence") || an_type(seq, "flow_sequence"))) {
                an_loop_parse(a, v, out);
                found = true;
            }
        }
        a->src = saved;
        if (tree) {
            ts_tree_delete(tree);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, src);
    }
    return found;
}

/* An expression "{{ X }}" with X an identifier, then `tail` ("" or
 * ".results"): X into `out`. */
static bool an_expr_name(const char *expr, const char *tail, char *out, size_t sz) {
    const char *b = expr && expr[0] == '{' && expr[SKIP_ONE] == '{' ? expr + PAIR_LEN : NULL;
    size_t n = 0;
    while (b && (*b == ' ' || *b == '\t')) {
        b++;
    }
    while (b && n + SKIP_ONE < sz &&
           ((*b >= 'a' && *b <= 'z') || (*b >= 'A' && *b <= 'Z') || *b == '_' ||
            (n > 0 && *b >= '0' && *b <= '9'))) {
        out[n++] = *b++;
    }
    out[n] = '\0';
    size_t tl = strlen(tail);
    if (!n || !b || strncmp(b, tail, tl) != 0) {
        return false;
    }
    for (b += tl; *b == ' ' || *b == '\t'; b++) {}
    return strcmp(b, "}}") == 0;
}

/* "{{ R.results }}": R into `out`. */
static bool an_results_of(const char *expr, char *out, size_t sz) {
    return an_expr_name(expr, ".results", out, sz);
}

/* Does `s` hold a {{ item.K }} reference (one key level)? */
static bool an_has_item_key(const char *s) {
    for (const char *p = s ? strstr(s, "{{") : NULL; p; p = strstr(p + PAIR_LEN, "{{")) {
        const char *b = p + PAIR_LEN;
        while (*b == ' ' || *b == '\t') {
            b++;
        }
        if (strncmp(b, "item.", sizeof("item.") - SKIP_ONE) != 0) {
            continue;
        }
        b += sizeof("item.") - SKIP_ONE;
        const char *k = b;
        while ((*b >= 'a' && *b <= 'z') || (*b >= 'A' && *b <= 'Z') || *b == '_') {
            b++;
        }
        while (b > k && (*b == ' ' || *b == '\t')) {
            b++;
        }
        if (b > k && b[0] == '}' && b[SKIP_ONE] == '}') {
            return true;
        }
    }
    return false;
}

/* The items a loop goes over: its literal list, or the list "{{ V }}" names
 * -- a set_fact of the file, else the role's defaults / vars. `scratch`
 * holds a list read from the role (the caller frees it). */
static const an_loop_t *an_items(an_t *a, const an_loop_t *l, an_loop_t *scratch) {
    if (!l->expr) {
        return l->items ? l : NULL;
    }
    char name[CBM_SZ_128];
    if (!an_expr_name(l->expr, "", name, sizeof(name))) {
        return NULL;
    }
    for (int i = 0; i < a->fact_n; i++) {
        if (strcmp(a->facts[i].name, name) == 0) {
            return &a->facts[i].list;
        }
    }
    return an_role_list(a, name, scratch) ? scratch : NULL;
}

static void an_deploy(an_t *a, const an_apply_t *p, const cbm_gbuf_node_t *target) {
    if (!target || p->task <= 0) {
        return;
    }
    char em[CBM_SZ_256];
    char props[CBM_SZ_512];
    cbm_json_escape(em, (int)sizeof(em), p->module);
    snprintf(props, sizeof(props), "{\"via\":\"ansible\",\"module\":\"%s\",\"line\":%d}", em,
             p->line);
    cbm_gbuf_insert_edge(a->ctx->gbuf, p->task, target->id, "DEPLOYS", props);
    a->deploys++;
}

/* The renderer `src` of each item of `items`, substituted (strict). */
static int an_deploy_items(an_t *a, const an_apply_t *p, const an_render_t *r,
                           const an_loop_t *items) {
    int made = 0;
    char path[CBM_SZ_1K];
    for (int i = 0; r && items && i < items->n; i++) {
        const cbm_gbuf_node_t *t =
            r->src && an_subst(r->src, &items->items[i], true, path, sizeof(path))
                ? an_src_file(a, path, r->copy)
                : NULL;
        if (t) {
            an_deploy(a, p, t);
            made++;
        }
    }
    return made;
}

/* The first renderer (templates before copies, each in file order) whose
 * `dest` -- or, with `reg`, whose register -- is the very string. */
static const an_render_t *an_render_find(const an_t *a, const char *dest, const char *reg) {
    for (int pass = 0; pass < PAIR_LEN; pass++) {
        for (int i = 0; i < a->render_n; i++) {
            const an_render_t *r = &a->renders[i];
            const char *v = reg ? r->reg : r->dest;
            if (r->copy == (pass == SKIP_ONE) && v && strcmp(v, reg ? reg : dest) == 0) {
                return r;
            }
        }
    }
    return NULL;
}

/* kube / k8s -> DEPLOYS {via: ansible, module, line} the repository file it
 * applies, traced to the template (or copied file) the play rendered it from:
 * a k8s template: argument; a renderer whose dest is the filename (D); for
 * {{ item.item.K }} over "{{ R.results }}", each item of the renderer
 * registering R (L); for {{ item.K }}, each of the task's own items through
 * the renderer of that dest (S); a literal filename a looped renderer's dest
 * renders to (R). */
static void an_link_applies(an_t *a) {
    for (int i = 0; i < a->apply_n; i++) {
        const an_apply_t *p = &a->applies[i];
        an_loop_t scratch = {0};
        int made = 0;
        if (p->tmpl) {
            an_deploy(a, p, an_src_file(a, p->tmpl, false));
            continue;
        }
        if (!p->fname) {
            continue;
        }
        const an_render_t *r = an_render_find(a, p->fname, NULL);
        const cbm_gbuf_node_t *d = r ? an_src_file(a, r->src, r->copy) : NULL;
        if (d) {
            an_deploy(a, p, d);
            continue;
        }
        char reg[CBM_SZ_128];
        if (strstr(p->fname, "item.item") && an_results_of(p->loop.expr, reg, sizeof(reg))) {
            const an_render_t *g = an_render_find(a, NULL, reg);
            made = g ? an_deploy_items(a, p, g, an_items(a, &g->loop, &scratch)) : 0;
        }
        if (!made && an_has_item_key(p->fname) && (p->loop.expr || p->loop.items)) {
            an_loop_free(&scratch);
            made = an_deploy_items(a, p, r, an_items(a, &p->loop, &scratch));
        }
        if (!made && !strstr(p->fname, "{{ item")) {
            char path[CBM_SZ_1K];
            for (int j = 0; j < a->render_n; j++) {
                const an_render_t *x = &a->renders[j];
                an_loop_free(&scratch);
                const an_loop_t *items = an_items(a, &x->loop, &scratch);
                for (int k = 0; items && x->dest && k < items->n; k++) {
                    if (an_subst(x->dest, &items->items[k], false, path, sizeof(path)) &&
                        strcmp(path, p->fname) == 0 && x->src &&
                        an_subst(x->src, &items->items[k], true, path, sizeof(path))) {
                        an_deploy(a, p, an_src_file(a, path, x->copy));
                    }
                }
            }
        }
        an_loop_free(&scratch);
    }
}

/* The per-file render / apply / set_fact records, freed. */
static void an_applies_reset(an_t *a) {
    for (int i = 0; i < a->render_n; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, a->renders[i].src);
        cbm_free(CBM_MEM_CLASS_OTHER, a->renders[i].dest);
        cbm_free(CBM_MEM_CLASS_OTHER, a->renders[i].reg);
        an_loop_free(&a->renders[i].loop);
    }
    for (int i = 0; i < a->apply_n; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, a->applies[i].module);
        cbm_free(CBM_MEM_CLASS_OTHER, a->applies[i].fname);
        cbm_free(CBM_MEM_CLASS_OTHER, a->applies[i].tmpl);
        an_loop_free(&a->applies[i].loop);
    }
    for (int i = 0; i < a->fact_n; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, a->facts[i].name);
        an_loop_free(&a->facts[i].list);
    }
    a->render_n = 0;
    a->apply_n = 0;
    a->fact_n = 0;
}

static void an_index_handler(an_t *a, const char *name, int64_t id) {
    char key[CBM_SZ_2K];
    if (a->role_dir[0]) {
        snprintf(key, sizeof(key), "%s\x1f%s", a->role_dir, name);
        an_ids_add(a->handlers, key, id);
    }
    snprintf(key, sizeof(key), "F:%s\x1f%s", a->file, name);
    an_ids_add(a->handlers, key, id);
}

static void an_note(an_t *a, int64_t task, const char *name, int line) {
    if (a->note_n == a->note_cap) {
        int cap = a->note_cap ? a->note_cap * PAIR_LEN : AN_GROW_MIN;
        an_notify_t *grown =
            cbm_realloc(CBM_MEM_CLASS_OTHER, a->notes, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return;
        }
        a->notes = grown;
        a->note_cap = cap;
    }
    a->notes[a->note_n++] =
        (an_notify_t){.task = task,
                      .scope_role = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, a->role_dir),
                      .scope_file = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, a->file),
                      .name = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, name),
                      .line = line};
}

/* Task (or handler) `m`: a Function named by its `name:` (else its module),
 * QN <file>::task/<n> (handler/<n>), {source: ansible, kind, module}; the
 * file DEFINES it. A handler is indexed by its name and listen topics; a
 * notify waits for every handler (an_link_notifies). */
static void an_task(an_t *a, TSNode m, bool handler) {
    char module[CBM_SZ_128] = "";
    for (uint32_t i = 0; !module[0] && i < ts_node_named_child_count(m); i++) {
        char k[CBM_SZ_128];
        if (an_key(a, ts_node_named_child(m, i), k, sizeof(k)) && !an_keyword(k)) {
            snprintf(module, sizeof(module), "%s", k);
        }
    }
    char action[CBM_SZ_256];
    if (!module[0] && (an_scalar(a, an_get(a, m, "action"), action, sizeof(action)) ||
                       an_scalar(a, an_get(a, m, "local_action"), action, sizeof(action)))) {
        snprintf(module, sizeof(module), "%.*s", (int)strcspn(action, " \t"), action);
    }
    char name[CBM_SZ_512];
    bool named = an_scalar(a, an_get(a, m, "name"), name, sizeof(name));
    if (!named) {
        snprintf(name, sizeof(name), "%s", module[0] ? module : "task");
    }
    const char *kind = handler ? "handler" : "task";
    int ord = ++a->ordinal[handler ? SKIP_ONE : 0];
    char qn[CBM_SZ_2K + CBM_SZ_64];
    char em[CBM_SZ_512];
    char props[CBM_SZ_1K];
    snprintf(qn, sizeof(qn), "%s::%s/%d", a->base, kind, ord);
    cbm_json_escape(em, (int)sizeof(em), module);
    snprintf(props, sizeof(props), "{\"source\":\"ansible\",\"kind\":\"%s\",\"module\":\"%s\"}",
             kind, em);
    int64_t id = cbm_gbuf_upsert_node(a->ctx->gbuf, "Function", name, qn, a->file,
                                      (int)ts_node_start_point(m).row + SKIP_ONE,
                                      (int)ts_node_end_point(m).row + SKIP_ONE, props);
    if (id <= 0) {
        return;
    }
    cbm_gbuf_insert_edge(a->ctx->gbuf, a->from, id, "DEFINES", "{}");
    a->tasks++;
    an_collect(a, m, id);
    TSNode list;
    char n[CBM_SZ_256];
    if (handler) {
        if (named) {
            an_index_handler(a, name, id);
        }
        TSNode listen = an_get(a, m, "listen");
        uint32_t ln = an_name_count(a, listen, &list);
        for (uint32_t i = 0; i < ln; i++) {
            if (an_name_at(a, listen, list, i, n, sizeof(n))) {
                an_index_handler(a, n, id);
            }
        }
    }
    for (uint32_t i = 0; i < ts_node_named_child_count(m); i++) {
        TSNode pair = ts_node_named_child(m, i);
        char k[CBM_SZ_64];
        if (!an_key(a, pair, k, sizeof(k)) || strcmp(k, "notify") != 0) {
            continue;
        }
        TSNode v = ts_node_child_by_field_name(pair, "value", sizeof("value") - SKIP_ONE);
        uint32_t nn = an_name_count(a, v, &list);
        for (uint32_t j = 0; j < nn; j++) {
            if (an_name_at(a, v, list, j, n, sizeof(n)) && !strstr(n, "{{")) {
                an_note(a, id, n, (int)ts_node_start_point(pair).row + SKIP_ONE);
            }
        }
    }
}

/* The tasks of a task list, depth first: a `block:` item walks its block,
 * rescue and always lists before the next item (Ansible's order). */
static void an_task_list(an_t *a, TSNode seq, bool handler) {
    static const char *const PARTS[] = {"always", "rescue", "block"}; /* pushed last-first */
    an_frame_t *stack = NULL;
    int depth = 0;
    int cap = 0;
    TSNode push[sizeof(PARTS) / sizeof(PARTS[0]) + SKIP_ONE];
    int pn = 0;
    push[pn++] = seq;
    for (;;) {
        for (int p = 0; p < pn; p++) {
            if (ts_node_is_null(push[p])) {
                continue;
            }
            if (depth == cap) {
                int ncap = cap ? cap * PAIR_LEN : AN_GROW_MIN;
                an_frame_t *grown =
                    cbm_realloc(CBM_MEM_CLASS_OTHER, stack, (size_t)ncap * sizeof(*grown));
                if (!grown) {
                    cbm_free(CBM_MEM_CLASS_OTHER, stack);
                    return;
                }
                stack = grown;
                cap = ncap;
            }
            stack[depth++] = (an_frame_t){.seq = push[p], .next = 0, .handler = handler};
        }
        pn = 0;
        if (depth == 0) {
            break;
        }
        an_frame_t *f = &stack[depth - SKIP_ONE];
        if (f->next >= ts_node_named_child_count(f->seq)) {
            depth--;
            continue;
        }
        TSNode m = an_item(f->seq, f->next++);
        TSNode block = an_seq(an_get(a, m, "block"));
        if (ts_node_is_null(m)) {
            continue;
        }
        if (ts_node_is_null(block)) {
            an_task(a, m, f->handler);
            continue;
        }
        for (size_t p = 0; p < sizeof(PARTS) / sizeof(PARTS[0]); p++) {
            push[pn++] = an_seq(an_get(a, m, PARTS[p]));
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, stack);
}

/* The tasks of an Ansible file: a playbook's plays (pre_tasks, tasks,
 * post_tasks, handlers), else its own items -- handlers under a handlers/
 * directory. */
static void an_file_tasks(an_t *a, TSNode top, bool playbook) {
    static const char *const SECTIONS[] = {"pre_tasks", "tasks", "post_tasks", "handlers"};
    if (!playbook) {
        bool handlers = strncmp(a->file, "handlers/", sizeof("handlers/") - SKIP_ONE) == 0 ||
                        strstr(a->file, "/handlers/") != NULL;
        an_task_list(a, top, handlers);
        return;
    }
    for (uint32_t i = 0; i < ts_node_named_child_count(top); i++) {
        TSNode play = an_item(top, i);
        for (size_t s = 0; !ts_node_is_null(play) && s < sizeof(SECTIONS) / sizeof(SECTIONS[0]);
             s++) {
            an_task_list(a, an_seq(an_get(a, play, SECTIONS[s])),
                         strcmp(SECTIONS[s], "handlers") == 0);
        }
    }
}

/* notify -> CALLS {via: notify, line} each handler of that name or listen
 * topic in the task's role, else in its file (a play's handlers). */
static void an_link_notifies(an_t *a) {
    for (int i = 0; i < a->note_n; i++) {
        const an_notify_t *n = &a->notes[i];
        char key[CBM_SZ_2K];
        const an_ids_t *ids = NULL;
        if (n->scope_role && n->scope_role[0]) {
            snprintf(key, sizeof(key), "%s\x1f%s", n->scope_role, n->name);
            ids = cbm_ht_get(a->handlers, key);
        }
        if (!ids) {
            snprintf(key, sizeof(key), "F:%s\x1f%s", n->scope_file, n->name);
            ids = cbm_ht_get(a->handlers, key);
        }
        char props[CBM_SZ_64];
        snprintf(props, sizeof(props), "{\"via\":\"notify\",\"line\":%d}", n->line);
        for (int j = 0; ids && j < ids->count; j++) {
            if (ids->items[j] != n->task) {
                cbm_gbuf_insert_edge(a->ctx->gbuf, n->task, ids->items[j], "CALLS", props);
                a->calls++;
            }
        }
    }
}

/* path -> node index of one label's nodes. */
static CBMHashTable *an_index(cbm_pipeline_ctx_t *ctx, const char *label) {
    CBMHashTable *ix = cbm_ht_create(0);
    const cbm_gbuf_node_t **nodes = NULL;
    int n = 0;
    if (ix && cbm_gbuf_find_by_label(ctx->gbuf, label, &nodes, &n) == 0) {
        for (int i = 0; i < n; i++) {
            if (nodes[i]->file_path) {
                cbm_ht_set(ix, nodes[i]->file_path, (void *)nodes[i]);
            }
        }
    }
    return ix;
}

static bool an_is_yaml(const char *rel) {
    const char *dot = strrchr(rel, '.');
    return dot && (strcmp(dot, ".yml") == 0 || strcmp(dot, ".yaml") == 0);
}

bool cbm_ansible_is_sequence(const char *src) {
    return an_is_sequence(src);
}

/* A YAML file whose top level is a sequence: Ansible when it is a playbook,
 * a role's tasks / handlers file, or included by an Ansible file. */
typedef struct {
    int file; /* index into files[] */
    bool playbook;
    bool ansible;
    int inc_lo; /* the task files it includes: incs[inc_lo, inc_hi) */
    int inc_hi;
} an_cand_t;

typedef struct {
    an_cand_t *items;
    int count;
    int cap;
} an_cands_t;

static an_cand_t *an_cand_push(an_cands_t *c, int file, int inc_lo) {
    if (c->count == c->cap) {
        int cap = c->cap ? c->cap * PAIR_LEN : AN_GROW_MIN;
        an_cand_t *grown = cbm_realloc(CBM_MEM_CLASS_OTHER, c->items, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return NULL;
        }
        c->items = grown;
        c->cap = cap;
    }
    c->items[c->count] = (an_cand_t){.file = file, .inc_lo = inc_lo, .inc_hi = inc_lo};
    return &c->items[c->count++];
}

/* The per-file state both passes start from. */
static void an_begin_file(an_t *a, const char *rel, const cbm_gbuf_node_t *fnode, const char *src) {
    static const char FILE_SUFFIX[] = ".__file__";
    const char *slash = strrchr(rel, '/');
    a->src = src;
    a->file = rel;
    a->from = fnode->id;
    snprintf(a->dir, sizeof(a->dir), "%.*s", slash ? (int)(slash - rel) : 0, rel);
    an_role_of(rel, a->role, sizeof(a->role));
    an_role_dir(rel, a->role_dir, sizeof(a->role_dir));
    const char *qn = fnode->qualified_name ? fnode->qualified_name : "";
    size_t ql = strlen(qn);
    size_t sl = sizeof(FILE_SUFFIX) - SKIP_ONE;
    bool suffixed = ql > sl && strcmp(qn + ql - sl, FILE_SUFFIX) == 0;
    snprintf(a->base, sizeof(a->base), "%.*s", (int)(suffixed ? ql - sl : ql), qn);
    a->ordinal[0] = 0;
    a->ordinal[SKIP_ONE] = 0;
}

/* Which candidates are Ansible: playbooks and role tasks / handlers files,
 * then -- to a fixpoint, by a worklist -- what an Ansible file includes. */
static void an_mark_ansible(const an_t *a, an_cands_t *c, const cbm_file_info_t *files) {
    CBMHashTable *by_path = cbm_ht_create(0);
    int *work = cbm_calloc(CBM_MEM_CLASS_OTHER, ((size_t)c->count + SKIP_ONE) * sizeof(int));
    if (!by_path || !work) {
        cbm_free(CBM_MEM_CLASS_OTHER, work);
        if (by_path) {
            cbm_ht_free(by_path);
        }
        return;
    }
    int top = 0;
    for (int i = 0; i < c->count; i++) {
        const char *rel = files[c->items[i].file].rel_path;
        char role[CBM_SZ_1K];
        an_role_dir(rel, role, sizeof(role));
        cbm_ht_set(by_path, rel, (void *)(intptr_t)(i + SKIP_ONE));
        if (c->items[i].playbook || role[0]) {
            c->items[i].ansible = true;
            work[top++] = i;
        }
    }
    while (top > 0) {
        const an_cand_t *from = &c->items[work[--top]];
        for (int k = from->inc_lo; k < from->inc_hi; k++) {
            int j =
                a->incs[k] ? (int)(intptr_t)cbm_ht_get(by_path, a->incs[k]) - SKIP_ONE : -SKIP_ONE;
            if (j >= 0 && !c->items[j].ansible) {
                c->items[j].ansible = true;
                work[top++] = j;
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, work);
    cbm_ht_free(by_path);
}

static void an_free_ids_entry(const char *key, void *value, void *ud) {
    (void)ud;
    an_ids_t *v = value;
    if (v) {
        cbm_free(CBM_MEM_CLASS_OTHER, v->items);
        cbm_free(CBM_MEM_CLASS_OTHER, v);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
}

static void an_free(an_t *a) {
    if (a->modules) {
        cbm_ht_free(a->modules);
    }
    if (a->folders) {
        cbm_ht_free(a->folders);
    }
    for (int i = 0; i < a->inc_n; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, a->incs[i]);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, a->incs);
    for (int i = 0; i < a->note_n; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, a->notes[i].scope_role);
        cbm_free(CBM_MEM_CLASS_OTHER, a->notes[i].scope_file);
        cbm_free(CBM_MEM_CLASS_OTHER, a->notes[i].name);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, a->notes);
    if (a->handlers) {
        cbm_ht_foreach(a->handlers, an_free_ids_entry, NULL);
        cbm_ht_free(a->handlers);
    }
    an_applies_reset(a);
    cbm_free(CBM_MEM_CLASS_OTHER, a->renders);
    cbm_free(CBM_MEM_CLASS_OTHER, a->applies);
    cbm_free(CBM_MEM_CLASS_OTHER, a->facts);
    if (a->by_rel) {
        cbm_ht_free(a->by_rel);
    }
}

/* Parse `files[i]` when it is a YAML file whose top level is a sequence;
 * NULL otherwise. The text stays in `*src` (the caller frees it). */
static TSTree *an_parse(TSParser *parser, const cbm_file_info_t *f, char **src) {
    size_t len = 0;
    *src = an_is_yaml(f->rel_path) ? an_read(f->path, &len) : NULL;
    if (!*src || !an_is_sequence(*src)) {
        return NULL;
    }
    return ts_parser_parse_string(parser, NULL, *src, (uint32_t)len);
}

void cbm_pipeline_pass_ansible(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                               int file_count) {
    an_t a = {.ctx = ctx};
    an_cands_t cands = {0};
    TSParser *parser = ts_parser_new();
    CBMHashTable *file_nodes = NULL;
    if (!parser || !ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_YAML))) {
        if (parser) {
            ts_parser_delete(parser);
        }
        return;
    }
    /* pass 1: composition (IMPORTS); which files are playbooks, what each includes */
    for (int i = 0; i < file_count; i++) {
        char *src = NULL;
        TSTree *tree = an_parse(parser, &files[i], &src);
        if (tree && !file_nodes) {
            a.modules = an_index(ctx, "Module");
            a.folders = an_index(ctx, "Folder");
            file_nodes = an_index(ctx, "File");
        }
        const cbm_gbuf_node_t *fnode =
            tree && file_nodes ? cbm_ht_get(file_nodes, files[i].rel_path) : NULL;
        an_cand_t *c = fnode ? an_cand_push(&cands, i, a.inc_n) : NULL;
        if (c) {
            an_begin_file(&a, files[i].rel_path, fnode, src);
            an_walk(&a, ts_tree_root_node(tree));
            c->playbook = an_is_playbook(&a, an_top(ts_tree_root_node(tree)));
            c->inc_hi = a.inc_n;
        }
        if (tree) {
            ts_tree_delete(tree);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, src);
    }
    /* pass 2: the tasks of the Ansible files (what each applies), then their notifies */
    an_mark_ansible(&a, &cands, files);
    a.handlers = cands.count > 0 ? cbm_ht_create(0) : NULL;
    a.by_rel = a.handlers ? cbm_ht_create(0) : NULL;
    for (int i = 0; a.by_rel && i < file_count; i++) {
        cbm_ht_set(a.by_rel, files[i].rel_path, (void *)(intptr_t)(i + SKIP_ONE));
    }
    a.files = files;
    a.file_nodes = file_nodes;
    a.parser = parser;
    for (int c = 0; a.handlers && c < cands.count; c++) {
        const cbm_file_info_t *f = &files[cands.items[c].file];
        char *src = NULL;
        TSTree *tree = cands.items[c].ansible ? an_parse(parser, f, &src) : NULL;
        const cbm_gbuf_node_t *fnode = tree ? cbm_ht_get(file_nodes, f->rel_path) : NULL;
        if (fnode) {
            an_begin_file(&a, f->rel_path, fnode, src);
            an_file_tasks(&a, an_top(ts_tree_root_node(tree)), cands.items[c].playbook);
            an_link_applies(&a);
            an_applies_reset(&a);
        }
        if (tree) {
            ts_tree_delete(tree);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, src);
    }
    if (a.handlers) {
        an_link_notifies(&a);
    }
    ts_parser_delete(parser);
    if (file_nodes) {
        cbm_ht_free(file_nodes);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, cands.items);
    an_free(&a);
    if (a.edges > 0 || a.tasks > 0) {
        char e[CBM_SZ_32];
        char t[CBM_SZ_32];
        char n[CBM_SZ_32];
        char d[CBM_SZ_32];
        snprintf(e, sizeof(e), "%d", a.edges);
        snprintf(t, sizeof(t), "%d", a.tasks);
        snprintf(n, sizeof(n), "%d", a.calls);
        snprintf(d, sizeof(d), "%d", a.deploys);
        cbm_log_info("pass.ansible", "imports", e, "tasks", t, "notifies", n, "deploys", d);
    }
}
