/* Local, on-demand selection impact. Graph facts and historical co-changes are
 * separate evidence domains. A retained worker keeps the single HTTP listener
 * responsive; its SQLite connection pins one read snapshot. No new schema,
 * shell evaluation, telemetry, or additional library dependencies. */
#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "foundation/constants.h"
#include "ui/atlas.h"
#include "foundation/compat_fs.h"
#include "foundation/subprocess.h"
#include "foundation/platform.h"
#include <pthread.h>
#include <errno.h>
#include <stdint.h>
#include <sqlite3.h>
#include <yyjson/yyjson.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum {
    IA_SEEDS = 128,
    IA_VISITS = 600,
    IA_EDGES = 3000,
    IA_DEPTH = 4,
    IA_SHOWN = 60,
    IA_COMMITS = 200,
    IA_MASS_FILES = 30,
    IA_COFILES = 300,
    IA_COMMIT_REFS = 5,
    IA_SECONDS = 8,
    IA_CACHE_SECONDS = 30
};

static const char *ia_text(sqlite3_stmt *st, int col) {
    const char *s = (const char *)sqlite3_column_text(st, col);
    return s ? s : "";
}

static bool ia_path_valid(const char *file) {
    if (!file || !*file || strlen(file) >= 1024 || file[0] == '/' || file[0] == '\\')
        return false;
    for (const char *p = file; *p; p++) {
        if (*p == '\\' || *p == ':' || (unsigned char)*p < 32)
            return false;
        if ((p == file || p[-1] == '/') && p[0] == '.' && p[1] == '.' && (p[2] == '/' || !p[2]))
            return false;
    }
    return true;
}

typedef struct {
    int64_t id, edge_id;
    int parent, distance, line;
    char name[192], qn[768], file[1024], type[24];
} ia_node_t;

static void ia_read_node(ia_node_t *node, sqlite3_stmt *st) {
    node->id = sqlite3_column_int64(st, 0);
    snprintf(node->name, sizeof(node->name), "%s", ia_text(st, 1));
    snprintf(node->qn, sizeof(node->qn), "%s", ia_text(st, 2));
    snprintf(node->file, sizeof(node->file), "%s", ia_text(st, 3));
    node->line = sqlite3_column_int(st, 4);
    node->parent = -1;
}

static yyjson_mut_val *ia_node_json(yyjson_mut_doc *doc, const ia_node_t *node) {
    yyjson_mut_val *obj = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_int(doc, obj, "id", node->id);
    yyjson_mut_obj_add_strcpy(doc, obj, "name", node->name);
    yyjson_mut_obj_add_strcpy(doc, obj, "qualified_name", node->qn);
    yyjson_mut_obj_add_strcpy(doc, obj, "file_path", node->file);
    yyjson_mut_obj_add_int(doc, obj, "line", node->line);
    return obj;
}

/* Every displayed finding carries an actual edge-id path directed from the
 * dependent to the selection. IMPORTS is a dependency, never a data flow. */
static yyjson_mut_val *ia_finding(yyjson_mut_doc *doc, ia_node_t *nodes, int index) {
    ia_node_t *node = &nodes[index];
    yyjson_mut_val *obj = ia_node_json(doc, node);
    yyjson_mut_obj_add_int(doc, obj, "distance", node->distance);
    yyjson_mut_obj_add_bool(doc, obj, "test_candidate", cbm_is_test_file_path(node->file));
    yyjson_mut_val *path = yyjson_mut_arr(doc);
    for (int cursor = index; nodes[cursor].parent >= 0; cursor = nodes[cursor].parent) {
        ia_node_t *from = &nodes[cursor], *to = &nodes[from->parent];
        yyjson_mut_val *edge = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, edge, "edge_id", from->edge_id);
        yyjson_mut_obj_add_strcpy(doc, edge, "type", from->type);
        yyjson_mut_obj_add_val(doc, edge, "from", ia_node_json(doc, from));
        yyjson_mut_obj_add_val(doc, edge, "to", ia_node_json(doc, to));
        yyjson_mut_arr_append(path, edge);
    }
    yyjson_mut_obj_add_val(doc, obj, "path", path);
    return obj;
}

static int ia_deadline(void *arg) {
    return time(NULL) >= *(time_t *)arg;
}

static void ia_structural(cbm_store_t *store, const char *project, const char *file,
                          int64_t node_id, const char *qn, yyjson_mut_doc *doc,
                          yyjson_mut_val *root, time_t deadline) {
    sqlite3 *db = cbm_store_get_db(store);
    yyjson_mut_val *out = yyjson_mut_obj(doc), *findings = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, root, "structural", out);
    yyjson_mut_obj_add_val(doc, out, "findings", findings);
    yyjson_mut_obj_add_str(doc, out, "basis", "CALLS and IMPORTS; reverse dependency paths");
    yyjson_mut_obj_add_int(doc, out, "max_depth", IA_DEPTH);
    yyjson_mut_obj_add_int(doc, out, "visit_cap", IA_VISITS);
    yyjson_mut_obj_add_int(doc, out, "result_cap", IA_SHOWN);
    ia_node_t *nodes = calloc(IA_VISITS, sizeof(*nodes));
    sqlite3_stmt *st = NULL, *edges = NULL;
    int count = 0, seed_count = 0, visited_edges = 0, direct = 0, test_count = 0;
    bool capped = false, failed = false;
    sqlite3_progress_handler(db, 10000, ia_deadline, &deadline);
    const char *sql = "SELECT id,name,qualified_name,file_path,start_line FROM nodes "
                      "WHERE project=?1 AND file_path=?2 AND "
                      "((?3<0 AND ?4='') OR id=?3 OR (?4<>'' AND qualified_name=?4)) "
                      "ORDER BY CASE WHEN label IN ('File','Module') THEN 0 "
                      "WHEN label IN ('Function','Method') THEN 1 ELSE 2 END,id LIMIT 129";
    if (!nodes || sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        failed = true;
        goto finish;
    }
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, file, -1, SQLITE_STATIC);
    sqlite3_bind_int64(st, 3, node_id);
    sqlite3_bind_text(st, 4, qn ? qn : "", -1, SQLITE_STATIC);
    int rc;
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        if (count == IA_SEEDS) {
            capped = true;
            break;
        }
        ia_read_node(&nodes[count++], st);
    }
    if (rc != SQLITE_DONE && rc != SQLITE_ROW)
        failed = true;
    seed_count = count;
    const char *edge_sql =
        "SELECT n.id,n.name,n.qualified_name,n.file_path,n.start_line,e.id,e.type "
        "FROM edges e JOIN nodes n ON n.id=e.source_id "
        "WHERE e.project=?1 AND n.project=?1 AND e.target_id=?2 "
        "AND e.type IN ('CALLS','IMPORTS') ORDER BY e.id LIMIT 3001";
    if (sqlite3_prepare_v2(db, edge_sql, -1, &edges, NULL) != SQLITE_OK) {
        failed = true;
        goto finish;
    }
    for (int head = 0; head < count && !failed; head++) {
        if (time(NULL) >= deadline) {
            capped = true;
            break;
        }
        sqlite3_reset(edges);
        sqlite3_bind_text(edges, 1, project, -1, SQLITE_STATIC);
        sqlite3_bind_int64(edges, 2, nodes[head].id);
        while ((rc = sqlite3_step(edges)) == SQLITE_ROW) {
            if (++visited_edges > IA_EDGES) {
                capped = true;
                goto finish;
            }
            int64_t id = sqlite3_column_int64(edges, 0);
            bool seen = false;
            for (int i = 0; i < count; i++)
                if (nodes[i].id == id) {
                    seen = true;
                    break;
                }
            if (seen)
                continue;
            if (count == IA_VISITS || nodes[head].distance >= IA_DEPTH) {
                capped = true;
                continue;
            }
            ia_node_t *next = &nodes[count];
            ia_read_node(next, edges);
            next->parent = head;
            next->distance = nodes[head].distance + 1;
            next->edge_id = sqlite3_column_int64(edges, 5);
            snprintf(next->type, sizeof(next->type), "%s", ia_text(edges, 6));
            if (next->distance == 1)
                direct++;
            if (cbm_is_test_file_path(next->file))
                test_count++;
            count++;
        }
        if (rc != SQLITE_DONE)
            failed = true;
    }
finish:
    sqlite3_finalize(st);
    sqlite3_finalize(edges);
    sqlite3_progress_handler(db, 0, NULL, NULL);
    yyjson_mut_obj_add_bool(doc, out, "available", !failed && seed_count > 0);
    yyjson_mut_obj_add_int(doc, out, "seed_count", seed_count);
    yyjson_mut_obj_add_int(doc, out, "reachable", count - seed_count);
    yyjson_mut_obj_add_int(doc, out, "direct", direct);
    yyjson_mut_obj_add_int(doc, out, "test_candidates", test_count);
    yyjson_mut_obj_add_bool(doc, out, "truncated", capped || count - seed_count > IA_SHOWN);
    if (failed)
        yyjson_mut_obj_add_str(doc, out, "error", "Graph query failed or exceeded its time bound.");
    if (seed_count == 0 && !failed)
        yyjson_mut_obj_add_str(doc, out, "error", "Selection is absent from this index snapshot.");
    /* BFS order prioritizes direct dependents; retain all counts even when the
     * evidence list is shortened. Tests remain visible among the first rows. */
    for (int i = seed_count; i < count && i < seed_count + IA_SHOWN; i++)
        yyjson_mut_arr_append(findings, ia_finding(doc, nodes, i));
    free(nodes);
}

enum { IA_HASH = 64, IA_OUTPUT_CAP = 8 * 1024 * 1024 };

typedef struct {
    char *file;
    int count, ref_count;
    char commits[IA_COMMIT_REFS][IA_HASH + 1];
} ia_cofile_t;

/* Use the existing supervisor with argv, never a shell. Raw NUL-delimited Git
 * output is read after the child exits: line callbacks cannot represent a path
 * containing a newline. Output and total wall time both have explicit bounds. */
static char *ia_git(const char *root, const char *const *args, size_t *size, time_t deadline,
                    bool *bounded) {
    *size = 0;
    if (time(NULL) >= deadline) {
        *bounded = true;
        return NULL;
    }
    char dir[256] = "/tmp/cbm-impact-XXXXXX";
    if (!cbm_mkdtemp(dir))
        return NULL;
    char log[512];
    snprintf(log, sizeof(log), "%s/output", dir);
    const char *argv[40] = {"git",
                            "--no-lazy-fetch",
                            "--no-optional-locks",
                            "-c",
                            "core.fsmonitor=false",
                            "-c",
                            "core.untrackedCache=false",
                            "-C",
                            root};
    int n = 9;
    while (*args && n < 39)
        argv[n++] = *args++;
    argv[n] = NULL;
    cbm_proc_opts_t opts = {.bin = "git",
                            .argv = argv,
                            .log_file = log,
                            .quiet_timeout_ms = 2000,
                            .cancel_grace_ms = 100};
    cbm_subprocess_t *process = NULL;
    cbm_proc_result_t result = {0};
    char *output = NULL;
    if (cbm_subprocess_spawn(&opts, &process) != 0)
        goto cleanup;
    cbm_proc_poll_t poll;
    while ((poll = cbm_subprocess_poll(process, &result)) == CBM_PROC_POLL_RUNNING) {
        if (time(NULL) >= deadline || cbm_file_size(log) > IA_OUTPUT_CAP) {
            *bounded = true;
            cbm_subprocess_request_cancel(process);
        }
        cbm_usleep(10000);
    }
    if (poll != CBM_PROC_POLL_TERMINAL || result.outcome != CBM_PROC_CLEAN ||
        result.cancellation_requested || result.supervision_failed)
        goto cleanup;
    int64_t length = cbm_file_size(log);
    if (length < 0 || length > IA_OUTPUT_CAP) {
        *bounded = true;
        goto cleanup;
    }
    FILE *fp = cbm_fopen(log, "rb");
    if (!fp)
        goto cleanup;
    output = malloc((size_t)length + 1);
    if (output && fread(output, 1, (size_t)length, fp) == (size_t)length) {
        output[length] = '\0';
        *size = (size_t)length;
    } else {
        free(output);
        output = NULL;
    }
    if (fclose(fp) != 0) {
        free(output);
        output = NULL;
        *size = 0;
    }
cleanup:
    cbm_subprocess_destroy(process);
    if (remove(log) != 0 && errno != ENOENT) {
        free(output);
        output = NULL;
        *size = 0;
    }
    (void)cbm_rmdir(dir);
    return output;
}

static bool ia_hash(const char *s) {
    size_t len = strlen(s);
    if (len != 40 && len != 64)
        return false;
    for (size_t i = 0; i < len; i++)
        if (!((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f')))
            return false;
    return true;
}

static char *ia_token(char **cursor, const char *end) {
    if (*cursor >= end)
        return NULL;
    char *token = *cursor;
    size_t len = strnlen(token, (size_t)(end - token));
    if (token + len >= end) {
        *cursor = (char *)end;
        return NULL;
    }
    *cursor += len + 1;
    return token;
}

static void ia_history(const char *root_path, const char *file, yyjson_mut_doc *doc,
                       yyjson_mut_val *root, time_t deadline) {
    yyjson_mut_val *out = yyjson_mut_obj(doc), *commits = yyjson_mut_arr(doc),
                   *cochanges = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, root, "history", out);
    yyjson_mut_obj_add_val(doc, out, "commits", commits);
    yyjson_mut_obj_add_val(doc, out, "cochanges", cochanges);
    yyjson_mut_obj_add_int(doc, out, "commit_cap", IA_COMMITS);
    yyjson_mut_obj_add_int(doc, out, "mass_change_threshold", IA_MASS_FILES);
    yyjson_mut_obj_add_int(doc, out, "window_days", 548);
    yyjson_mut_obj_add_str(doc, out, "meaning",
                           "Co-changes are historical association, not functional dependencies or "
                           "defect probabilities.");
    ia_cofile_t *cofiles = calloc(IA_COFILES, sizeof(*cofiles));
    int scanned = 0, considered = 0, merges = 0, mass = 0, touching = 0, cocount = 0, errors = 0;
    bool available = false, truncated = false;
    char *raw = NULL;
    size_t size = 0;
    const char *head_args[] = {"rev-parse", "--verify", "HEAD", NULL};
    char *head = ia_git(root_path, head_args, &size, deadline, &truncated);
    if (head)
        head[strcspn(head, "\r\n")] = '\0';
    if (!cofiles || !head || !ia_hash(head)) {
        yyjson_mut_obj_add_str(doc, out, "error",
                               "No readable committed Git HEAD is available locally.");
        free(head);
        goto finish;
    }
    yyjson_mut_obj_add_strcpy(doc, out, "head", head);
    char revision[IA_HASH + 1];
    snprintf(revision, sizeof(revision), "%s", head);
    free(head);
    const char *shallow_args[] = {"rev-parse", "--is-shallow-repository", NULL};
    char *shallow = ia_git(root_path, shallow_args, &size, deadline, &truncated);
    if (shallow)
        yyjson_mut_obj_add_bool(doc, out, "shallow", strncmp(shallow, "true", 4) == 0);
    else
        errors++;
    free(shallow);
    const char *status_args[] = {"--literal-pathspecs",
                                 "status",
                                 "--porcelain=v1",
                                 "-z",
                                 "--untracked-files=normal",
                                 "--",
                                 file,
                                 NULL};
    char *status = ia_git(root_path, status_args, &size, deadline, &truncated);
    yyjson_mut_obj_add_bool(doc, out, "worktree_status_known", status != NULL);
    if (status)
        yyjson_mut_obj_add_bool(doc, out, "selection_uncommitted", size > 0);
    free(status);
    const char *log_args[] = {"log",
                              "--max-count=201",
                              "--since=548.days",
                              "--format=%x00CBM-COMMIT%x00%H%x00%P%x00%ct%x00%s%x00",
                              "--name-only",
                              "-z",
                              "--no-renames",
                              "--no-ext-diff",
                              "--no-textconv",
                              "--no-show-signature",
                              "--relative",
                              revision,
                              "--",
                              NULL};
    raw = ia_git(root_path, log_args, &size, deadline, &truncated);
    if (!raw) {
        errors++;
        goto finish;
    }
    char *cursor = raw;
    const char *end = raw + size;
    while (cursor < end) {
        char *token = ia_token(&cursor, end);
        if (!token)
            break;
        if (!*token)
            continue;
        if (strcmp(token, "CBM-COMMIT") != 0) {
            errors++;
            break;
        }
        if (scanned == IA_COMMITS) {
            truncated = true;
            break;
        }
        char *hash = ia_token(&cursor, end), *parents = ia_token(&cursor, end),
             *epoch = ia_token(&cursor, end), *subject = ia_token(&cursor, end);
        if (!hash || !parents || !epoch || !subject || !ia_hash(hash)) {
            errors++;
            break;
        }
        char *epoch_end = NULL;
        errno = 0;
        long long commit_time = strtoll(epoch, &epoch_end, CBM_DECIMAL_BASE);
        if (errno != 0 || epoch_end == epoch || *epoch_end != '\0' || commit_time < 0) {
            errors++;
            break;
        }
        scanned++;
        bool merge = strchr(parents, ' ') != NULL;
        char *paths[IA_MASS_FILES];
        int files = 0;
        bool first_file = true;
        /* After the header Git emits an empty token, then a newline before
         * the first file. A later empty token separates commits. File names
         * are nonempty, so the boundary is unambiguous even for magic names. */
        if (cursor < end && *cursor == '\0')
            cursor++;
        while (cursor < end && *cursor != '\0') {
            char *path = ia_token(&cursor, end);
            if (!path) {
                errors++;
                break;
            }
            if (first_file && *path == '\n')
                path++;
            first_file = false;
            if (*path) {
                if (files < IA_MASS_FILES)
                    paths[files] = path;
                files++;
            }
        }
        if (merge) {
            merges++;
            continue;
        }
        if (files > IA_MASS_FILES) {
            mass++;
            continue;
        }
        considered++;
        bool touches = false;
        for (int i = 0; i < files; i++)
            if (strcmp(paths[i], file) == 0)
                touches = true;
        if (!touches)
            continue;
        touching++;
        if (yyjson_mut_arr_size(commits) < 12) {
            yyjson_mut_val *row = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(doc, row, "hash", hash);
            char summary[241];
            snprintf(summary, sizeof(summary), "%s", subject);
            yyjson_mut_obj_add_strcpy(doc, row, "subject", summary);
            yyjson_mut_obj_add_int(doc, row, "time", commit_time);
            yyjson_mut_obj_add_int(doc, row, "files", files);
            yyjson_mut_arr_append(commits, row);
        }
        for (int i = 0; i < files; i++) {
            const char *other = paths[i];
            if (strcmp(other, file) == 0)
                continue;
            int slot = 0;
            while (slot < cocount && strcmp(cofiles[slot].file, other) != 0)
                slot++;
            if (slot == cocount) {
                if (cocount == IA_COFILES) {
                    truncated = true;
                    continue;
                }
                cofiles[slot].file = strdup(other);
                if (!cofiles[slot].file) {
                    errors++;
                    continue;
                }
                cocount++;
            }
            ia_cofile_t *co = &cofiles[slot];
            co->count++;
            if (co->ref_count < IA_COMMIT_REFS)
                snprintf(co->commits[co->ref_count++], sizeof(co->commits[0]), "%s", hash);
        }
    }
    available = scanned > 0 && errors == 0;
    for (int a = 0; a < cocount; a++)
        for (int b = a + 1; b < cocount; b++)
            if (cofiles[b].count > cofiles[a].count ||
                (cofiles[b].count == cofiles[a].count &&
                 strcmp(cofiles[b].file, cofiles[a].file) < 0)) {
                ia_cofile_t swap = cofiles[a];
                cofiles[a] = cofiles[b];
                cofiles[b] = swap;
            }
    for (int i = 0; i < cocount && i < 15; i++) {
        yyjson_mut_val *row = yyjson_mut_obj(doc), *refs = yyjson_mut_arr(doc);
        yyjson_mut_obj_add_strcpy(doc, row, "file_path", cofiles[i].file);
        yyjson_mut_obj_add_int(doc, row, "shared_commits", cofiles[i].count);
        for (int j = 0; j < cofiles[i].ref_count; j++)
            yyjson_mut_arr_add_strcpy(doc, refs, cofiles[i].commits[j]);
        yyjson_mut_obj_add_val(doc, row, "commit_refs", refs);
        yyjson_mut_arr_append(cochanges, row);
    }
finish:
    yyjson_mut_obj_add_bool(doc, out, "available", available);
    yyjson_mut_obj_add_bool(doc, out, "truncated", truncated);
    yyjson_mut_obj_add_int(doc, out, "commits_scanned", scanned);
    yyjson_mut_obj_add_int(doc, out, "commits_considered", considered);
    yyjson_mut_obj_add_int(doc, out, "selection_commits", touching);
    yyjson_mut_obj_add_int(doc, out, "merges_excluded", merges);
    yyjson_mut_obj_add_int(doc, out, "mass_changes_excluded", mass);
    yyjson_mut_obj_add_int(doc, out, "read_errors", errors);
    yyjson_mut_obj_add_int(doc, out, "cochanges_omitted", cocount > 15 ? cocount - 15 : 0);
    for (int i = 0; i < cocount; i++)
        free(cofiles[i].file);
    free(cofiles);
    free(raw);
}

char *cbm_atlas_impact_analysis_json(cbm_store_t *store, const char *project, const char *file,
                                     int64_t node_id, const char *qn) {
    if (!store || !project || !ia_path_valid(file))
        return NULL;
    sqlite3 *db = cbm_store_get_db(store);
    if (sqlite3_exec(db, "BEGIN", NULL, NULL, NULL) != SQLITE_OK)
        return NULL;
    cbm_project_t info = {0};
    if (cbm_store_get_project(store, project, &info) != CBM_STORE_OK) {
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        return NULL;
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc), *snapshot = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "ready");
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    yyjson_mut_obj_add_strcpy(doc, root, "file_path", file);
    yyjson_mut_obj_add_str(doc, root, "scope", node_id >= 0 || (qn && *qn) ? "symbol" : "file");
    yyjson_mut_obj_add_int(doc, root, "computed_at", time(NULL));
    yyjson_mut_obj_add_int(doc, root, "cache_seconds", IA_CACHE_SECONDS);
    yyjson_mut_obj_add_val(doc, root, "snapshot", snapshot);
    yyjson_mut_obj_add_strcpy(doc, snapshot, "indexed_at", info.indexed_at ? info.indexed_at : "");
    char generation[80] = {0};
    cbm_store_generation(store, generation, sizeof(generation));
    yyjson_mut_obj_add_strcpy(doc, snapshot, "generation", generation);
    yyjson_mut_obj_add_str(doc, snapshot, "index_revision", "unknown");
    yyjson_mut_obj_add_str(doc, snapshot, "freshness",
                           "The index does not record a Git revision; equivalence to HEAD or the "
                           "worktree is unverified.");
    cbm_coverage_meta_t meta = {0};
    int meta_rc = cbm_store_coverage_meta_get(store, project, &meta);
    /* Reuse the indexer's persisted categories, without inventing a coverage denominator. */
    sqlite3_stmt *st = NULL;
    yyjson_mut_val *coverage = yyjson_mut_arr(doc);
    bool coverage_read = false;
    time_t deadline = time(NULL) + IA_SECONDS;
    sqlite3_progress_handler(db, 10000, ia_deadline, &deadline);
    if (sqlite3_prepare_v2(
            db,
            "SELECT kind,count(*) FROM index_coverage WHERE project=?1 GROUP BY kind ORDER BY kind",
            -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        int coverage_rc;
        while ((coverage_rc = sqlite3_step(st)) == SQLITE_ROW) {
            yyjson_mut_val *row = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(doc, row, "kind", ia_text(st, 0));
            yyjson_mut_obj_add_int(doc, row, "count", sqlite3_column_int64(st, 1));
            yyjson_mut_arr_append(coverage, row);
        }
        coverage_read = coverage_rc == SQLITE_DONE;
    }
    sqlite3_finalize(st);
    sqlite3_progress_handler(db, 0, NULL, NULL);
    yyjson_mut_obj_add_strcpy(doc, snapshot, "coverage_recording",
                              coverage_read && meta_rc == CBM_STORE_OK && meta.recording_status
                                  ? meta.recording_status
                                  : "unavailable");
    cbm_store_coverage_meta_clear(&meta);
    yyjson_mut_obj_add_val(doc, snapshot, "coverage", coverage);
    ia_structural(store, project, file, node_id, qn, doc, root, deadline);
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    ia_history(info.root_path ? info.root_path : "", file, doc, root, deadline);
    cbm_project_free_fields(&info);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

/* Single bounded analysis slot. A second selection gets busy and polls; it
 * cannot start unbounded workers. Joining a completed thread before reuse
 * retains lifecycle ownership, including orderly daemon shutdown. */
static pthread_mutex_t ia_mu = PTHREAD_MUTEX_INITIALIZER;
static struct {
    cbm_thread_t thread;
    bool started, running;
    char *key, *json, *db_path, *project, *file, *qn;
    int64_t node_id;
    time_t completed;
} ia_job;

static void *ia_worker(void *unused) {
    (void)unused;
    cbm_store_t *store = cbm_store_open_path_query(ia_job.db_path);
    char *json = store ? cbm_atlas_impact_analysis_json(store, ia_job.project, ia_job.file,
                                                        ia_job.node_id, ia_job.qn)
                       : NULL;
    if (store)
        cbm_store_close(store);
    pthread_mutex_lock(&ia_mu);
    ia_job.json =
        json ? json
             : strdup("{\"status\":\"failed\",\"error\":\"Impact snapshot could not be read.\"}");
    ia_job.running = false;
    ia_job.completed = time(NULL);
    pthread_mutex_unlock(&ia_mu);
    return NULL;
}

static void ia_clear_completed_locked(void) {
    if (ia_job.started)
        cbm_thread_join(&ia_job.thread);
    free(ia_job.key);
    free(ia_job.json);
    free(ia_job.db_path);
    free(ia_job.project);
    free(ia_job.file);
    free(ia_job.qn);
    memset(&ia_job, 0, sizeof(ia_job));
}

char *cbm_atlas_impact_analysis_request(cbm_store_t *store, const char *project, const char *file,
                                        int64_t node_id, const char *qn, bool refresh) {
    if (!store || !project || !ia_path_valid(file))
        return strdup(
            "{\"status\":\"failed\",\"error\":\"A relative indexed file path is required.\"}");
    char generation[80] = {0};
    cbm_store_generation(store, generation, sizeof(generation));
    cbm_project_t info = {0};
    cbm_store_get_project(store, project, &info);
    const char *indexed_at = info.indexed_at ? info.indexed_at : "";
    const char *db_path = cbm_store_db_path(store);
    if (!db_path || !*db_path) {
        cbm_project_free_fields(&info);
        return strdup("{\"status\":\"failed\",\"error\":\"Persistent index required.\"}");
    }
    size_t key_size = strlen(db_path) + strlen(project) + strlen(file) + strlen(indexed_at) +
                      (qn ? strlen(qn) : 0) + sizeof(generation) + 64;
    char *key = malloc(key_size);
    if (!key) {
        cbm_project_free_fields(&info);
        return NULL;
    }
    snprintf(key, key_size, "%s|%s|%s|%s|%s|%lld|%s", db_path, project, generation, indexed_at,
             file, (long long)node_id, qn ? qn : "");
    cbm_project_free_fields(&info);
    pthread_mutex_lock(&ia_mu);
    bool same = ia_job.key && strcmp(ia_job.key, key) == 0;
    if (ia_job.running) {
        pthread_mutex_unlock(&ia_mu);
        free(key);
        return strdup(same ? "{\"status\":\"pending\"}" : "{\"status\":\"busy\"}");
    }
    if (same && !refresh && time(NULL) - ia_job.completed < IA_CACHE_SECONDS) {
        char *json = ia_job.json ? strdup(ia_job.json) : NULL;
        pthread_mutex_unlock(&ia_mu);
        free(key);
        return json;
    }
    ia_clear_completed_locked();
    ia_job.key = key;
    ia_job.db_path = strdup(db_path);
    ia_job.project = strdup(project);
    ia_job.file = strdup(file);
    ia_job.qn = strdup(qn ? qn : "");
    ia_job.node_id = node_id;
    ia_job.running = true;
    if (!ia_job.db_path || !ia_job.project || !ia_job.file || !ia_job.qn ||
        cbm_thread_create(&ia_job.thread, 0, ia_worker, NULL) != 0) {
        ia_clear_completed_locked();
        pthread_mutex_unlock(&ia_mu);
        return strdup("{\"status\":\"failed\",\"error\":\"Analysis worker unavailable.\"}");
    }
    ia_job.started = true;
    pthread_mutex_unlock(&ia_mu);
    return strdup("{\"status\":\"pending\"}");
}

void cbm_atlas_impact_analysis_shutdown(void) {
    pthread_mutex_lock(&ia_mu);
    bool running = ia_job.running, started = ia_job.started;
    pthread_mutex_unlock(&ia_mu);
    if (running && started) {
        cbm_thread_join(&ia_job.thread);
        pthread_mutex_lock(&ia_mu);
        ia_job.started = false;
        pthread_mutex_unlock(&ia_mu);
    }
    pthread_mutex_lock(&ia_mu);
    ia_clear_completed_locked();
    pthread_mutex_unlock(&ia_mu);
}
