/* Bounded architecture projections over a completed graph generation. */
#include "store/architecture_projection.h"
#include "foundation/hash_table.h"
#include "foundation/platform.h"
#include <sqlite3.h>
#include <yyjson/yyjson.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    AP_TEXT_BUDGET = 64 * 1024 * 1024,
    AP_REPRESENTATIVES = 5,
    AP_ENTRYPOINTS = 128,
    AP_PASSES = 6,
    AP_GROUP_ATOMS = 32,
    AP_JSON_BUDGET = 32 * 1024 * 1024,
    AP_ARGUMENT_LIMIT = 8,
    AP_SIGNATURE_BYTES = 4096,
    AP_VALUE_BYTES = 1024,
    AP_PARAMETER_LIMIT = 64
};
enum { AP_EVIDENCE_CONTEXT, AP_EVIDENCE_CORRIDOR, AP_EVIDENCE_PATH, AP_EVIDENCE_SCOPES };

typedef struct {
    size_t allocated;
    bool failed;
} ap_json_budget;

static void *ap_json_malloc(void *context, size_t size) {
    ap_json_budget *budget = context;
    if (size > AP_JSON_BUDGET - budget->allocated) {
        budget->failed = true;
        return NULL;
    }
    void *p = malloc(size);
    if (p)
        budget->allocated += size;
    else
        budget->failed = true;
    return p;
}

static void *ap_json_realloc(void *context, void *ptr, size_t old_size, size_t size) {
    ap_json_budget *budget = context;
    size_t growth = size > old_size ? size - old_size : 0;
    if (growth > AP_JSON_BUDGET - budget->allocated) {
        budget->failed = true;
        return NULL;
    }
    void *p = realloc(ptr, size);
    if (p)
        budget->allocated += growth;
    else
        budget->failed = true;
    return p;
}

static void ap_json_free(void *context, void *ptr) {
    (void)context;
    free(ptr);
}

/* Presence and references are deliberately separate from execution. */
static const char *const ap_types[] = {
    "",           "CALLS",      "HTTP_CALLS", "ASYNC_CALLS",    "CALL_REFERENCE", "IMPORTS",
    "IMPLEMENTS", "INHERITS",   "CONFIGURES", "WRITES",         "READS",          "USAGE",
    "PUBLISHES",  "SUBSCRIBES", "DEFINES",    "DEFINES_METHOD", "CONTAINS",       "EMITS",
    "LISTENS_ON", "HANDLES",    "DATA_FLOWS", "INFRA_MAPS"};
enum { AP_DEFINES = 14, AP_METHOD = 15, AP_CONTAINS = 16 };

typedef struct {
    int64_t id;
    char *name, *qn, *file, *label;
    int line, end, atom, component, degree;
    bool entry, structural, declared, test, test_property;
} ap_node;
typedef struct {
    int64_t id;
    int source, target, type;
} ap_edge;
typedef struct {
    int source, target, weight;
} ap_pair;
typedef struct {
    int source, target, type, edge;
} ap_dep;
typedef struct {
    int root, members, files, atoms, representative, degree, displayed;
    int overview_group;
    char *directory;
    int samples[AP_REPRESENTATIVES], sample_count;
} ap_component;
typedef struct {
    char *id, *label;
    bool test;
    int components, members, files;
} ap_overview_group;
typedef struct {
    int index, members, degree;
} ap_rank;
typedef struct {
    sqlite3 *db;
    cbm_architecture_projection_options_t options;
    uint64_t started;
    bool cancelled, expired;
    size_t text_bytes;
    const char *limited;
    int64_t total_nodes, total_edges, total_files;
    ap_node *nodes;
    ap_edge *edges;
    int n, m;
    ap_component *components;
    int component_count, accounted, structural, unmodeled_edges, relationship_edges;
    int cycle_count, omitted_cycles;
    ap_overview_group groups[66];
    int group_count, grouping_depth;
    sqlite3_stmt *provenance;
    sqlite3_stmt *symbol_metadata;
    yyjson_doc **symbol_properties;
    unsigned char *symbol_properties_loaded;
    ap_json_budget metadata_budget;
    size_t evidence_bytes[AP_EVIDENCE_SCOPES];
    int evidence_scope;
    bool evidence_omitted, evidence_suppressed;
} ap_context;

static int ap_bound(int requested, int fallback, int ceiling) {
    return requested <= 0 ? fallback : requested > ceiling ? ceiling : requested;
}

static bool ap_stopped(ap_context *c) {
    if (c->options.cancel && c->options.cancel(c->options.cancel_context))
        c->cancelled = true;
    if (cbm_now_ms() >= c->options.deadline_ms)
        c->expired = true;
    return c->cancelled || c->expired;
}

static int ap_progress(void *context) {
    return ap_stopped(context) ? 1 : 0;
}

static char *ap_copy(ap_context *c, const unsigned char *text) {
    const char *s = text ? (const char *)text : "";
    size_t bytes = strlen(s) + 1;
    if (bytes > AP_TEXT_BUDGET - c->text_bytes) {
        c->limited = "Indexed symbol text exceeds the analysis memory budget.";
        return NULL;
    }
    char *result = malloc(bytes);
    if (!result)
        return NULL;
    memcpy(result, s, bytes);
    c->text_bytes += bytes;
    return result;
}

static int ap_find(const ap_context *c, int64_t id) {
    int lo = 0, hi = c->n;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (c->nodes[mid].id < id)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < c->n && c->nodes[lo].id == id ? lo : -1;
}

static int ap_type(const char *type) {
    for (int i = 1; i < (int)(sizeof(ap_types) / sizeof(ap_types[0])); i++)
        if (strcmp(type, ap_types[i]) == 0)
            return i;
    return 0;
}

static bool ap_execution(int type) {
    return type >= 1 && type <= 3;
}
static bool ap_dependency(int type) {
    return (type > 0 && type < AP_DEFINES) || type > AP_CONTAINS;
}
static bool ap_cluster_edge(int type) {
    return type == 1 || type == 5 || type == 11;
}

/* Conservative source conventions supplement the positive indexed is_test
 * flag. Non-test classification means no test evidence, not production use. */
static bool ap_test_path(const char *path) {
    const char *base = path;
    for (const char *p = path;; p++) {
        if (*p == '/' || *p == '\\' || !*p) {
            size_t len = (size_t)(p - base);
            if ((len == 4 && !strncmp(base, "test", len)) ||
                (len == 5 && !strncmp(base, "tests", len)) ||
                (len == 9 && !strncmp(base, "__tests__", len)))
                return true;
            if (!*p)
                break;
            base = p + 1;
        }
    }
    return !strncmp(base, "test_", 5) || strstr(base, "_test.") || strstr(base, ".test.") ||
           strstr(base, ".spec.");
}

static int ap_counts(ap_context *c, const char *project) {
    sqlite3_stmt *stmt = NULL;
    const char *sql = "SELECT (SELECT count(*) FROM nodes WHERE project=?1),"
                      "(SELECT count(*) FROM edges WHERE project=?1),"
                      "(SELECT count(*) FROM nodes WHERE project=?1 AND label='File') "
                      "FROM projects WHERE name=?1";
    if (sqlite3_prepare_v2(c->db, sql, -1, &stmt, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT);
    int step = sqlite3_step(stmt);
    if (step == SQLITE_ROW) {
        c->total_nodes = sqlite3_column_int64(stmt, 0);
        c->total_edges = sqlite3_column_int64(stmt, 1);
        c->total_files = sqlite3_column_int64(stmt, 2);
    }
    sqlite3_finalize(stmt);
    if (step == SQLITE_DONE)
        return CBM_STORE_NOT_FOUND;
    if (step != SQLITE_ROW)
        return CBM_STORE_ERR;
    if (c->total_nodes > c->options.max_nodes)
        c->limited = "Graph exceeds the node budget; no sampled architecture was inferred.";
    else if (c->total_edges > c->options.max_edges)
        c->limited = "Graph exceeds the edge budget; no sampled architecture was inferred.";
    return CBM_STORE_OK;
}

static int ap_load(ap_context *c, const char *project) {
    c->nodes = calloc((size_t)c->total_nodes + 1, sizeof(*c->nodes));
    c->edges = calloc((size_t)c->total_edges + 1, sizeof(*c->edges));
    if (!c->nodes || !c->edges)
        return CBM_STORE_ERR;
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "SELECT id,name,qualified_name,file_path,label,start_line,end_line,"
        "CASE WHEN json_valid(properties) THEN json_extract(properties,'$.is_entry_point') "
        "ELSE 0 END, CASE WHEN json_valid(properties) THEN json_extract(properties,'$.is_test') "
        "ELSE 0 END FROM nodes WHERE project=?1 ORDER BY id";
    if (sqlite3_prepare_v2(c->db, sql, -1, &stmt, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT);
    int step;
    while ((step = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (c->n >= c->total_nodes || ap_stopped(c))
            break;
        ap_node *n = &c->nodes[c->n++];
        n->id = sqlite3_column_int64(stmt, 0);
        n->name = ap_copy(c, sqlite3_column_text(stmt, 1));
        n->qn = ap_copy(c, sqlite3_column_text(stmt, 2));
        n->file = ap_copy(c, sqlite3_column_text(stmt, 3));
        n->label = ap_copy(c, sqlite3_column_text(stmt, 4));
        if (!n->name || !n->qn || !n->file || !n->label)
            break;
        n->line = sqlite3_column_int(stmt, 5);
        n->end = sqlite3_column_int(stmt, 6);
        n->entry = sqlite3_column_int(stmt, 7) != 0;
        n->test_property = sqlite3_column_int(stmt, 8) != 0;
        n->test = n->test_property || ap_test_path(n->file);
        n->structural = !strcmp(n->label, "Project") || !strcmp(n->label, "Folder");
        n->declared = !strcmp(n->label, "Module") || !strcmp(n->label, "Package") ||
                      !strcmp(n->label, "Namespace") || !strcmp(n->label, "Class") ||
                      !strcmp(n->label, "Struct") || !strcmp(n->label, "Interface");
        n->atom = c->n - 1;
        n->component = -1;
    }
    sqlite3_finalize(stmt);
    if (step != SQLITE_DONE)
        return CBM_STORE_ERR;
    sql = "SELECT id,source_id,target_id,type FROM edges WHERE project=?1 ORDER BY id";
    if (sqlite3_prepare_v2(c->db, sql, -1, &stmt, NULL) != SQLITE_OK)
        return CBM_STORE_ERR;
    sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT);
    int scanned = 0;
    while ((step = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (scanned++ >= c->total_edges || ap_stopped(c))
            break;
        int type = ap_type((const char *)sqlite3_column_text(stmt, 3));
        if (!type) {
            c->unmodeled_edges++;
            continue;
        }
        int source = ap_find(c, sqlite3_column_int64(stmt, 1));
        int target = ap_find(c, sqlite3_column_int64(stmt, 2));
        if (source < 0 || target < 0)
            continue;
        c->edges[c->m++] = (ap_edge){sqlite3_column_int64(stmt, 0), source, target, type};
        if (ap_dependency(type)) {
            c->nodes[source].degree++;
            c->nodes[target].degree++;
            c->relationship_edges++;
        }
    }
    sqlite3_finalize(stmt);
    return step == SQLITE_DONE ? CBM_STORE_OK : CBM_STORE_ERR;
}

/* Ownership is grounded in declared module/type names and explicit definition
 * edges. File ownership is only the fallback atom, never a folder component. */
static int ap_ownership(ap_context *c) {
    CBMHashTable *files = cbm_ht_create((uint32_t)c->n);
    CBMHashTable *declared = cbm_ht_create((uint32_t)c->n);
    if (!files || !declared) {
        cbm_ht_free(files);
        cbm_ht_free(declared);
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < c->n; i++) {
        ap_node *n = &c->nodes[i];
        if (n->declared)
            cbm_ht_set(declared, n->qn, (void *)(intptr_t)(i + 1));
        if (n->file[0] && (!strcmp(n->label, "File") || !strcmp(n->label, "Module"))) {
            intptr_t old = (intptr_t)cbm_ht_get(files, n->file);
            if (!old || (!strcmp(n->label, "Module") && strcmp(c->nodes[old - 1].label, "Module")))
                cbm_ht_set(files, n->file, (void *)(intptr_t)(i + 1));
        }
    }
    int rc = CBM_STORE_OK;
    for (int i = 0; i < c->n; i++) {
        if ((i & 255) == 0 && ap_stopped(c)) {
            rc = CBM_STORE_ERR;
            break;
        }
        ap_node *n = &c->nodes[i];
        if (n->structural) {
            n->atom = -1;
            c->structural++;
            continue;
        }
        c->accounted++;
        intptr_t file = (intptr_t)cbm_ht_get(files, n->file);
        n->atom = file ? (int)file - 1 : i;
        if (n->declared) {
            n->atom = i;
            continue;
        }
        /* QN ownership is an indexed declaration match, not a path heuristic. */
        size_t len = strlen(n->qn);
        char *qn = malloc(len + 1);
        if (!qn) {
            rc = CBM_STORE_ERR;
            break;
        }
        memcpy(qn, n->qn, len + 1);
        for (int depth = 0; depth < 64; depth++) {
            char *dot = strrchr(qn, '.');
            if (!dot)
                break;
            *dot = '\0';
            intptr_t owner = (intptr_t)cbm_ht_get(declared, qn);
            if (owner) {
                n->atom = (int)owner - 1;
                break;
            }
        }
        free(qn);
        /* Embedded tests can share a production file/class, but not its
         * architecture atom. Prefer a same-role file declaration if present. */
        if (n->test != c->nodes[n->atom].test)
            n->atom = file && n->test == c->nodes[file - 1].test ? (int)file - 1 : i;
    }
    if (rc == CBM_STORE_OK)
        for (int i = 0; i < c->m; i++) {
            ap_edge *e = &c->edges[i];
            ap_node *source = &c->nodes[e->source], *target = &c->nodes[e->target];
            if ((e->type == AP_METHOD || e->type == AP_DEFINES || e->type == AP_CONTAINS) &&
                source->declared && !target->structural && !target->declared &&
                source->test == target->test)
                target->atom = e->source;
        }
    cbm_ht_free(files);
    cbm_ht_free(declared);
    return rc;
}

static int ap_pair_compare(const void *a, const void *b) {
    const ap_pair *x = a, *y = b;
    if (x->source != y->source)
        return x->source < y->source ? -1 : 1;
    return x->target < y->target ? -1 : x->target != y->target;
}

/* Six local adjacency sweeps, O(passes * edges), with explicit declarations
 * as atoms. Weak one-off calls cannot absorb an unrelated module; communities
 * have a fixed atom-size ceiling. These remain component candidates. */
static int ap_components(ap_context *c) {
    int n = c->n;
    int *group = malloc(((size_t)n + 1) * sizeof(int));
    int *sizes = calloc((size_t)n + 1, sizeof(int));
    int *offset = calloc((size_t)n + 2, sizeof(int));
    int *score = calloc((size_t)n + 1, sizeof(int));
    int *touched = malloc(((size_t)n + 1) * sizeof(int));
    ap_pair *pairs = malloc(((size_t)c->m * 2 + 1) * sizeof(*pairs));
    if (!group || !sizes || !offset || !score || !touched || !pairs) {
        free(group);
        free(sizes);
        free(offset);
        free(score);
        free(touched);
        free(pairs);
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < n; i++)
        group[i] = i;
    for (int i = 0; i < n; i++)
        if (c->nodes[i].atom >= 0)
            sizes[c->nodes[i].atom] = 1;
    int pcount = 0;
    for (int i = 0; i < c->m; i++) {
        ap_edge *e = &c->edges[i];
        int a = c->nodes[e->source].atom, b = c->nodes[e->target].atom;
        if (a < 0 || b < 0 || a == b || !ap_cluster_edge(e->type) ||
            c->nodes[a].test != c->nodes[b].test)
            continue;
        pairs[pcount++] = (ap_pair){a, b, 1};
        pairs[pcount++] = (ap_pair){b, a, 1};
    }
    qsort(pairs, (size_t)pcount, sizeof(*pairs), ap_pair_compare);
    for (int i = 0; i < pcount; i++)
        offset[pairs[i].source + 1]++;
    for (int i = 0; i < n; i++)
        offset[i + 1] += offset[i];
    int rc = CBM_STORE_OK;
    for (int pass = 0; pass < AP_PASSES; pass++) {
        bool changed = false;
        for (int i = 0; i < n; i++) {
            if ((i & 255) == 0 && ap_stopped(c)) {
                rc = CBM_STORE_ERR;
                goto finish;
            }
            if (!sizes[group[i]] || offset[i] == offset[i + 1])
                continue;
            /* Explicit package/namespace boundaries remain separate. */
            if (!strcmp(c->nodes[i].label, "Package") || !strcmp(c->nodes[i].label, "Namespace"))
                continue;
            int used = 0, total = 0, best = group[i], best_score = 0;
            for (int j = offset[i]; j < offset[i + 1]; j++) {
                int g = group[pairs[j].target];
                if (!score[g])
                    touched[used++] = g;
                score[g]++;
                total++;
            }
            for (int j = 0; j < used; j++) {
                int g = touched[j];
                const int candidate_score = score[g];
                if (candidate_score > best_score || (candidate_score == best_score && g < best)) {
                    best = g;
                    best_score = candidate_score;
                }
            }
            int current_score = score[group[i]];
            if (best != group[i] && best_score >= 3 && best_score > current_score &&
                (int64_t)best_score * 100 >= (int64_t)total * 55 && sizes[best] < AP_GROUP_ATOMS) {
                sizes[group[i]]--;
                sizes[best]++;
                group[i] = best;
                changed = true;
            }
            for (int j = 0; j < used; j++)
                score[touched[j]] = 0;
        }
        if (!changed)
            break;
    }
    c->components = calloc((size_t)n + 1, sizeof(*c->components));
    if (!c->components) {
        rc = CBM_STORE_ERR;
        goto finish;
    }
    for (int i = 0; i < n; i++)
        score[i] = -1;
    for (int i = 0; i < n; i++) {
        ap_node *node = &c->nodes[i];
        if (node->atom < 0)
            continue;
        int g = group[node->atom];
        if (score[g] < 0) {
            int k = c->component_count++;
            score[g] = k;
            c->components[k] = (ap_component){.root = g, .representative = i, .displayed = -1};
        }
        int k = score[g];
        node->component = k;
        ap_component *part = &c->components[k];
        part->members++;
        part->degree += node->degree;
        if (node->atom == i)
            part->atoms++;
        ap_node *rep = &c->nodes[part->representative];
        if ((node->declared && !rep->declared) ||
            (node->declared == rep->declared && node->degree > rep->degree))
            part->representative = i;
    }
    for (int i = 0; i < c->component_count; i++) {
        c->components[i].samples[0] = c->components[i].representative;
        c->components[i].sample_count = 1;
    }
    for (int i = 0; i < n; i++) {
        if (c->nodes[i].component < 0)
            continue;
        ap_component *p = &c->components[c->nodes[i].component];
        if (p->sample_count < AP_REPRESENTATIVES && i != p->representative)
            p->samples[p->sample_count++] = i;
    }
    /* Count unique file memberships, including isolated files. */
    CBMHashTable *seen = cbm_ht_create((uint32_t)n);
    char **keys = calloc((size_t)n + 1, sizeof(char *));
    if (!seen || !keys) {
        cbm_ht_free(seen);
        free(keys);
        rc = CBM_STORE_ERR;
        goto finish;
    }
    int key_count = 0;
    for (int i = 0; i < n; i++) {
        ap_node *node = &c->nodes[i];
        if (node->component < 0 || !node->file[0])
            continue;
        size_t len = strlen(node->file) + 32;
        char *key = malloc(len);
        if (!key) {
            rc = CBM_STORE_ERR;
            break;
        }
        snprintf(key, len, "%d:%s", node->component, node->file);
        if (!cbm_ht_has(seen, key)) {
            cbm_ht_set(seen, key, (void *)1);
            keys[key_count++] = key;
            c->components[node->component].files++;
        } else
            free(key);
    }
    cbm_ht_free(seen);
    for (int i = 0; i < key_count; i++)
        free(keys[i]);
    free(keys);
finish:
    free(group);
    free(sizes);
    free(offset);
    free(score);
    free(touched);
    free(pairs);
    return rc;
}

static void ap_component_id(const ap_context *c, int component, char id[48]) {
    if (component < 0) {
        id[0] = '\0';
        return;
    }
    snprintf(id, 48, "component-%lld", (long long)c->nodes[c->components[component].root].id);
}

/* Coarse display aggregates, not newly inferred architectural components.
 * A whole inferred component has one owner: the common directory of all of
 * its source members. Uniform coarsening never drops small/isolated parts. */
static int ap_overview_grouping(ap_context *c) {
    for (int i = 0; i < c->n; i++) {
        ap_node *node = &c->nodes[i];
        if (node->component < 0 || !node->file[0])
            continue;
        ap_component *part = &c->components[node->component];
        const char *last = NULL;
        for (const char *p = node->file; *p; p++)
            if (*p == '/' || *p == '\\')
                last = p;
        size_t length = last ? (size_t)(last - node->file) : 0;
        if (!part->directory) {
            part->directory = ap_copy(c, (const unsigned char *)node->file);
            if (!part->directory)
                return CBM_STORE_ERR;
            part->directory[length] = '\0';
            for (char *p = part->directory; *p; p++)
                if (*p == '\\')
                    *p = '/';
        } else {
            size_t common = 0, old = strlen(part->directory);
            while (common < old && common < length &&
                   part->directory[common] ==
                       (node->file[common] == '\\' ? '/' : node->file[common]))
                common++;
            bool boundary = (common == old && (common == length || node->file[common] == '/' ||
                                               node->file[common] == '\\')) ||
                            (common == length && (common == old || part->directory[common] == '/'));
            if (!boundary)
                while (common && part->directory[common - 1] != '/')
                    common--;
            if (common && part->directory[common - 1] == '/')
                common--;
            part->directory[common] = '\0';
        }
        if ((i & 255) == 0 && ap_stopped(c))
            return CBM_STORE_ERR;
    }
    for (int depth = 2; depth >= 0; depth--) {
        for (int g = 0; g < c->group_count; g++) {
            free(c->groups[g].id);
            free(c->groups[g].label);
        }
        c->group_count = 0;
        bool retry = false;
        for (int i = 0; i < c->component_count; i++) {
            ap_component *part = &c->components[i];
            const char *directory = part->directory ? part->directory : "@no-source";
            size_t length = strlen(directory);
            if (part->directory) {
                if (!depth)
                    length = 0;
                else {
                    int separators = 0;
                    for (size_t j = 0; j < length; j++)
                        if (directory[j] == '/' && ++separators == depth) {
                            length = j;
                            break;
                        }
                }
            }
            bool test = c->nodes[part->root].test;
            size_t bytes = length + 32;
            char *key = malloc(bytes);
            if (!key)
                return CBM_STORE_ERR;
            snprintf(key, bytes, "%s:%s:%.*s", part->directory ? "directory" : "unlocated",
                     test ? "test" : "non_test", (int)length, directory);
            int g;
            for (g = 0; g < c->group_count; g++)
                if (!strcmp(c->groups[g].id, key))
                    break;
            if (g == c->group_count) {
                if (g >= 64) {
                    free(key);
                    retry = true;
                    break;
                }
                ap_overview_group *group = &c->groups[c->group_count++];
                memset(group, 0, sizeof(*group));
                group->test = test;
                group->id = ap_copy(c, (const unsigned char *)key);
                key[0] = '\0';
                if (length)
                    snprintf(key, bytes, "%.*s", (int)length, directory);
                else
                    snprintf(key, bytes, "(root)");
                group->label = ap_copy(c, (const unsigned char *)key);
                if (!group->id || !group->label) {
                    free(key);
                    return CBM_STORE_ERR;
                }
            }
            free(key);
            part->overview_group = g;
            c->groups[g].components++;
            c->groups[g].members += part->members;
            if ((i & 255) == 0 && ap_stopped(c))
                return CBM_STORE_ERR;
        }
        if (!retry) {
            c->grouping_depth = depth;
            break;
        }
    }
    /* File counts are unique within a group; component file counts can overlap. */
    CBMHashTable *seen = cbm_ht_create((uint32_t)c->n);
    char **keys = calloc((size_t)c->n + 1, sizeof(*keys));
    if (!seen || !keys) {
        cbm_ht_free(seen);
        free(keys);
        return CBM_STORE_ERR;
    }
    int count = 0, rc = CBM_STORE_OK;
    for (int i = 0; i < c->n; i++) {
        ap_node *node = &c->nodes[i];
        if (node->component < 0 || !node->file[0])
            continue;
        int g = c->components[node->component].overview_group;
        size_t length = strlen(node->file) + 32;
        char *key = malloc(length);
        if (!key) {
            rc = CBM_STORE_ERR;
            break;
        }
        snprintf(key, length, "%d:%s", g, node->file);
        if (cbm_ht_has(seen, key))
            free(key);
        else {
            cbm_ht_set(seen, key, (void *)1);
            keys[count++] = key;
            c->groups[g].files++;
        }
        if ((i & 255) == 0 && ap_stopped(c)) {
            rc = CBM_STORE_ERR;
            break;
        }
    }
    cbm_ht_free(seen);
    for (int i = 0; i < count; i++)
        free(keys[i]);
    free(keys);
    return rc;
}

/* Only opt-in, displayed symbols load indexed properties. Cache each decoded
 * object once across representatives, paths and witnesses in this response.
 * Source code is never opened or parsed. Oversized values are omitted whole. */
static yyjson_val *ap_symbol_properties(ap_context *c, int index) {
    if (!c->options.include_behavior_evidence)
        return NULL;
    if (c->symbol_properties_loaded && c->symbol_properties_loaded[index])
        return c->symbol_properties[index] ? yyjson_doc_get_root(c->symbol_properties[index])
                                           : NULL;
    if (c->metadata_budget.failed)
        return NULL;
    if (!c->symbol_properties) {
        c->symbol_properties = calloc((size_t)c->n, sizeof(*c->symbol_properties));
        c->symbol_properties_loaded = calloc((size_t)c->n, 1);
        if (!c->symbol_properties || !c->symbol_properties_loaded) {
            free(c->symbol_properties);
            free(c->symbol_properties_loaded);
            c->symbol_properties = NULL;
            c->symbol_properties_loaded = NULL;
            c->metadata_budget.failed = true;
            return NULL;
        }
    }
    c->symbol_properties_loaded[index] = 1;
    if (!c->symbol_metadata &&
        sqlite3_prepare_v2(c->db,
                           "SELECT CASE WHEN length(CAST(properties AS BLOB))<=16384 THEN "
                           "properties END FROM nodes WHERE id=?1",
                           -1, &c->symbol_metadata, NULL) != SQLITE_OK)
        return NULL;
    sqlite3_bind_int64(c->symbol_metadata, 1, c->nodes[index].id);
    if (sqlite3_step(c->symbol_metadata) == SQLITE_ROW) {
        char *text = (char *)sqlite3_column_text(c->symbol_metadata, 0);
        yyjson_alc allocator = {ap_json_malloc, ap_json_realloc, ap_json_free, &c->metadata_budget};
        if (text)
            c->symbol_properties[index] = yyjson_read_opts(
                text, (size_t)sqlite3_column_bytes(c->symbol_metadata, 0), 0, &allocator, NULL);
    }
    sqlite3_reset(c->symbol_metadata);
    return c->symbol_properties[index] ? yyjson_doc_get_root(c->symbol_properties[index]) : NULL;
}

static bool ap_bounded_string(yyjson_val *value, size_t maximum) {
    return yyjson_is_str(value) && yyjson_get_len(value) <= maximum;
}

static bool ap_parameter_array(yyjson_val *value) {
    if (!yyjson_is_arr(value) || yyjson_arr_size(value) > AP_PARAMETER_LIMIT)
        return false;
    size_t index, maximum;
    yyjson_val *item;
    yyjson_arr_foreach(value, index, maximum,
                       item) if (!ap_bounded_string(item, AP_VALUE_BYTES)) return false;
    return true;
}

/* Optional copies must leave room for the base graph and its serialized copy.
 * Charge mutable values plus worst-case JSON escaping before allocation. The
 * separate pools keep overview/catalog duplicates from consuming path evidence.
 * The shared allocator remains the final hard bound on the entire response. */
static size_t ap_evidence_limit(const ap_context *c) {
    static const size_t limits[AP_EVIDENCE_SCOPES] = {256 * 1024, 768 * 1024, 1024 * 1024};
    return limits[c->evidence_scope];
}

static size_t ap_evidence_string_cost(yyjson_val *value) {
    return 128 + 8 * yyjson_get_len(value);
}

static bool ap_evidence_admit(ap_context *c, size_t bytes) {
    if (c->evidence_suppressed)
        return false;
    if (bytes > ap_evidence_limit(c) - c->evidence_bytes[c->evidence_scope]) {
        c->evidence_omitted = true;
        return false;
    }
    c->evidence_bytes[c->evidence_scope] += bytes;
    return true;
}

static void ap_declaration_evidence(yyjson_mut_doc *doc, ap_context *c, int index,
                                    yyjson_mut_val *value) {
    if (c->evidence_suppressed)
        return;
    if (ap_evidence_limit(c) - c->evidence_bytes[c->evidence_scope] < 128) {
        c->evidence_omitted = true;
        return;
    }
    yyjson_val *properties = ap_symbol_properties(c, index);
    if (!yyjson_is_obj(properties))
        return;
    yyjson_val *signature = yyjson_obj_get(properties, "signature"),
               *returns = yyjson_obj_get(properties, "return_type");
    if (ap_bounded_string(signature, AP_SIGNATURE_BYTES) &&
        ap_evidence_admit(c, ap_evidence_string_cost(signature)))
        yyjson_mut_obj_add_val(doc, value, "signature", yyjson_val_mut_copy(doc, signature));
    if (ap_bounded_string(returns, AP_VALUE_BYTES) &&
        ap_evidence_admit(c, ap_evidence_string_cost(returns)))
        yyjson_mut_obj_add_val(doc, value, "return_type", yyjson_val_mut_copy(doc, returns));
    yyjson_val *names = yyjson_obj_get(properties, "param_names"),
               *types = yyjson_obj_get(properties, "param_types"),
               *count = yyjson_obj_get(properties, "param_count");
    if (ap_parameter_array(names) && ap_parameter_array(types) && yyjson_is_int(count) &&
        yyjson_get_sint(count) >= 0 && yyjson_get_sint(count) <= INT_MAX &&
        yyjson_arr_size(names) <= (size_t)yyjson_get_sint(count) &&
        yyjson_arr_size(types) <= (size_t)yyjson_get_sint(count)) {
        size_t bytes = 384, at, maximum;
        yyjson_val *item;
        yyjson_arr_foreach(names, at, maximum, item) bytes += ap_evidence_string_cost(item);
        yyjson_arr_foreach(types, at, maximum, item) bytes += ap_evidence_string_cost(item);
        if (!ap_evidence_admit(c, bytes))
            return;
        yyjson_mut_val *parameters = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_val(doc, parameters, "names", yyjson_val_mut_copy(doc, names));
        yyjson_mut_obj_add_val(doc, parameters, "types", yyjson_val_mut_copy(doc, types));
        yyjson_mut_obj_add_val(doc, parameters, "count", yyjson_val_mut_copy(doc, count));
        yyjson_mut_obj_add_val(doc, value, "parameters", parameters);
    }
}

static yyjson_mut_val *ap_symbol(yyjson_mut_doc *doc, ap_context *c, int index) {
    const ap_node *n = &c->nodes[index];
    yyjson_mut_val *value = yyjson_mut_obj(doc);
    char id[48];
    ap_component_id(c, n->component, id);
    yyjson_mut_obj_add_sint(doc, value, "id", n->id);
    yyjson_mut_obj_add_strcpy(doc, value, "name", n->name);
    yyjson_mut_obj_add_strcpy(doc, value, "qualified_name", n->qn);
    yyjson_mut_obj_add_strcpy(doc, value, "label", n->label);
    yyjson_mut_obj_add_strcpy(doc, value, "file_path", n->file);
    yyjson_mut_obj_add_int(doc, value, "start_line", n->line);
    yyjson_mut_obj_add_int(doc, value, "end_line", n->end);
    yyjson_mut_obj_add_strcpy(doc, value, "component_id", id);
    if (n->component >= 0 && c->group_count)
        yyjson_mut_obj_add_strcpy(doc, value, "group_id",
                                  c->groups[c->components[n->component].overview_group].id);
    if (c->options.include_behavior_evidence)
        ap_declaration_evidence(doc, c, index, value);
    return value;
}

static int ap_dep_compare(const void *a, const void *b) {
    const ap_dep *x = a, *y = b;
    if (x->source != y->source)
        return x->source < y->source ? -1 : 1;
    if (x->target != y->target)
        return x->target < y->target ? -1 : 1;
    if (x->type != y->type)
        return x->type < y->type ? -1 : 1;
    return x->edge < y->edge ? -1 : x->edge != y->edge;
}

static int ap_rank_compare(const void *a, const void *b) {
    const ap_rank *x = a, *y = b;
    if (x->members != y->members)
        return x->members > y->members ? -1 : 1;
    if (x->degree != y->degree)
        return x->degree > y->degree ? -1 : 1;
    return x->index < y->index ? -1 : x->index != y->index;
}

/* CALLS stores at most eight indexed expressions, without keyword names.
 * Preserve stored indices/strings; do not infer parameter binding or values
 * for absent arguments. Even a short array cannot establish completeness. */
static void ap_argument_evidence(yyjson_mut_doc *doc, ap_context *c, yyjson_val *properties,
                                 yyjson_mut_val *value) {
    if (c->evidence_suppressed)
        return;
    yyjson_val *args = yyjson_obj_get(properties, "args");
    if (!yyjson_is_arr(args))
        return;
    size_t count = yyjson_arr_size(args);
    if (count > AP_ARGUMENT_LIMIT)
        count = AP_ARGUMENT_LIMIT;
    size_t bytes = 384;
    for (size_t at = 0; at < count; at++) {
        yyjson_val *arg = yyjson_arr_get(args, at), *index = yyjson_obj_get(arg, "i"),
                   *expression = yyjson_obj_get(arg, "e"), *resolved = yyjson_obj_get(arg, "v");
        if (!yyjson_is_int(index) || yyjson_get_sint(index) < 0 ||
            yyjson_get_sint(index) > INT_MAX || !ap_bounded_string(expression, AP_VALUE_BYTES))
            continue;
        bytes += 256 + ap_evidence_string_cost(expression);
        if (ap_bounded_string(resolved, AP_VALUE_BYTES))
            bytes += ap_evidence_string_cost(resolved);
    }
    if (!ap_evidence_admit(c, bytes))
        return;
    yyjson_mut_val *arguments = yyjson_mut_arr(doc);
    for (size_t at = 0; at < count; at++) {
        yyjson_val *arg = yyjson_arr_get(args, at), *index = yyjson_obj_get(arg, "i"),
                   *expression = yyjson_obj_get(arg, "e"), *resolved = yyjson_obj_get(arg, "v");
        if (!yyjson_is_int(index) || yyjson_get_sint(index) < 0 ||
            yyjson_get_sint(index) > INT_MAX || !ap_bounded_string(expression, AP_VALUE_BYTES))
            continue;
        yyjson_mut_val *copy = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_val(doc, copy, "i", yyjson_val_mut_copy(doc, index));
        yyjson_mut_obj_add_val(doc, copy, "e", yyjson_val_mut_copy(doc, expression));
        if (ap_bounded_string(resolved, AP_VALUE_BYTES))
            yyjson_mut_obj_add_val(doc, copy, "v", yyjson_val_mut_copy(doc, resolved));
        yyjson_mut_arr_add_val(arguments, copy);
    }
    yyjson_mut_obj_add_val(doc, value, "arguments", arguments);
    yyjson_mut_obj_add_int(doc, value, "argument_limit", AP_ARGUMENT_LIMIT);
    yyjson_mut_obj_add_bool(doc, value, "arguments_complete", false);
}

/* Look up only displayed witnesses. Large/unavailable properties are omitted;
 * resolver confidence is an uncalibrated score, never execution probability. */
static void ap_provenance(yyjson_mut_doc *doc, ap_context *c, int edge, yyjson_mut_val *value) {
    if (!c->provenance &&
        sqlite3_prepare_v2(c->db,
                           "SELECT CASE WHEN length(CAST(properties AS BLOB))<=16384 THEN "
                           "properties END FROM edges WHERE id=?1",
                           -1, &c->provenance, NULL) != SQLITE_OK)
        return;
    sqlite3_bind_int64(c->provenance, 1, c->edges[edge].id);
    yyjson_doc *properties = NULL;
    if (sqlite3_step(c->provenance) == SQLITE_ROW) {
        const char *text = (const char *)sqlite3_column_text(c->provenance, 0);
        if (text)
            properties = yyjson_read(text, (size_t)sqlite3_column_bytes(c->provenance, 0), 0);
    }
    sqlite3_reset(c->provenance);
    if (!properties)
        return;
    yyjson_val *root = yyjson_doc_get_root(properties), *line = yyjson_obj_get(root, "line");
    const char *file = c->nodes[c->edges[edge].source].file;
    if (yyjson_is_int(line) && yyjson_get_sint(line) > 0 && file[0]) {
        yyjson_mut_val *site = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, site, "file_path", file);
        yyjson_mut_obj_add_sint(doc, site, "line", yyjson_get_sint(line));
        yyjson_mut_obj_add_val(doc, value, "callsite", site);
    }
    yyjson_val *strategy = yyjson_obj_get(root, "strategy"),
               *confidence = yyjson_obj_get(root, "confidence"),
               *candidates = yyjson_obj_get(root, "candidates");
    yyjson_mut_val *resolution = yyjson_mut_obj(doc);
    bool any = false;
    if (yyjson_is_str(strategy) && yyjson_get_len(strategy) <= 128) {
        yyjson_mut_obj_add_strcpy(doc, resolution, "strategy", yyjson_get_str(strategy));
        any = true;
    }
    if (yyjson_is_num(confidence)) {
        yyjson_mut_obj_add_real(doc, resolution, "confidence", yyjson_get_num(confidence));
        any = true;
    }
    if (yyjson_is_int(candidates) && yyjson_get_sint(candidates) >= 0) {
        yyjson_mut_obj_add_sint(doc, resolution, "candidates", yyjson_get_sint(candidates));
        any = true;
    }
    if (any)
        yyjson_mut_obj_add_val(doc, value, "resolution", resolution);
    if (c->options.include_behavior_evidence && c->edges[edge].type == 1)
        ap_argument_evidence(doc, c, root, value);
    yyjson_doc_free(properties);
}

static yyjson_mut_val *ap_path_edge(yyjson_mut_doc *doc, ap_context *c, int edge) {
    const ap_edge *e = &c->edges[edge];
    yyjson_mut_val *value = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_sint(doc, value, "id", e->id);
    yyjson_mut_obj_add_sint(doc, value, "source_id", c->nodes[e->source].id);
    yyjson_mut_obj_add_sint(doc, value, "target_id", c->nodes[e->target].id);
    yyjson_mut_obj_add_str(doc, value, "type", ap_types[e->type]);
    ap_provenance(doc, c, edge, value);
    return value;
}

static yyjson_mut_val *ap_witness(yyjson_mut_doc *doc, ap_context *c, int edge) {
    const ap_edge *e = &c->edges[edge];
    yyjson_mut_val *value = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_sint(doc, value, "edge_id", e->id);
    yyjson_mut_obj_add_val(doc, value, "source", ap_symbol(doc, c, e->source));
    yyjson_mut_obj_add_val(doc, value, "target", ap_symbol(doc, c, e->target));
    yyjson_mut_obj_add_str(doc, value, "type", ap_types[e->type]);
    ap_provenance(doc, c, edge, value);
    return value;
}

/* Iterative Kosaraju. A component SCC is a dependency cycle, not evidence of
 * a runtime loop or a contiguous path through the member symbols. */
static int ap_cycles(ap_context *c, yyjson_mut_doc *doc, yyjson_mut_val *cycles, const ap_dep *deps,
                     int count) {
    int n = c->component_count;
    int *out = calloc((size_t)n + 2, sizeof(int));
    int *in = calloc((size_t)n + 2, sizeof(int));
    int *forward = malloc(((size_t)count + 1) * sizeof(int));
    int *reverse = malloc(((size_t)count + 1) * sizeof(int));
    int *cursor = calloc((size_t)n + 1, sizeof(int));
    int *stack = malloc(((size_t)n + 1) * sizeof(int));
    int *order = malloc(((size_t)n + 1) * sizeof(int));
    unsigned char *seen = calloc((size_t)n + 1, 1);
    if (!out || !in || !forward || !reverse || !cursor || !stack || !order || !seen) {
        free(out);
        free(in);
        free(forward);
        free(reverse);
        free(cursor);
        free(stack);
        free(order);
        free(seen);
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < count; i++) {
        out[deps[i].source + 1]++;
        in[deps[i].target + 1]++;
    }
    for (int i = 0; i < n; i++) {
        out[i + 1] += out[i];
        in[i + 1] += in[i];
    }
    memcpy(cursor, out, ((size_t)n + 1) * sizeof(int));
    for (int i = 0; i < count; i++)
        forward[cursor[deps[i].source]++] = deps[i].target;
    memcpy(cursor, in, ((size_t)n + 1) * sizeof(int));
    for (int i = 0; i < count; i++)
        reverse[cursor[deps[i].target]++] = deps[i].source;
    memcpy(cursor, out, ((size_t)n + 1) * sizeof(int));
    int ordered = 0, rc = CBM_STORE_OK;
    for (int root = 0; root < n; root++) {
        if (ap_stopped(c)) {
            rc = CBM_STORE_ERR;
            goto done;
        }
        if (seen[root])
            continue;
        int depth = 0;
        stack[depth++] = root;
        seen[root] = 1;
        while (depth) {
            int v = stack[depth - 1];
            if (cursor[v] < out[v + 1]) {
                int next = forward[cursor[v]++];
                if (!seen[next]) {
                    seen[next] = 1;
                    stack[depth++] = next;
                }
            } else {
                order[ordered++] = v;
                depth--;
            }
            if ((ordered & 1023) == 0 && ap_stopped(c)) {
                rc = CBM_STORE_ERR;
                goto done;
            }
        }
    }
    memset(seen, 0, (size_t)n + 1);
    for (int i = ordered - 1; i >= 0; i--) {
        int root = order[i];
        if (seen[root])
            continue;
        int depth = 0, members = 0;
        stack[depth++] = root;
        seen[root] = 1;
        yyjson_mut_val *ids = yyjson_mut_arr(doc);
        bool visible = true;
        while (depth) {
            int v = stack[--depth];
            members++;
            if (c->components[v].displayed < 0)
                visible = false;
            char id[48];
            ap_component_id(c, v, id);
            yyjson_mut_arr_add_strcpy(doc, ids, id);
            for (int j = in[v]; j < in[v + 1]; j++) {
                int next = reverse[j];
                if (!seen[next]) {
                    seen[next] = 1;
                    stack[depth++] = next;
                }
            }
            if ((members & 255) == 0 && ap_stopped(c)) {
                rc = CBM_STORE_ERR;
                goto done;
            }
        }
        if (members > 1) {
            c->cycle_count++;
            if (visible) {
                yyjson_mut_val *cycle = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_val(doc, cycle, "component_ids", ids);
                yyjson_mut_arr_add_val(cycles, cycle);
            } else
                c->omitted_cycles++;
        }
    }
done:
    free(out);
    free(in);
    free(forward);
    free(reverse);
    free(cursor);
    free(stack);
    free(order);
    free(seen);
    return rc;
}

typedef struct {
    int node, first_hop, depth, crossings;
    bool selected;
} ap_path_candidate;

static int ap_path_candidate_compare(const void *left, const void *right) {
    const ap_path_candidate *a = left, *b = right;
    if (a->depth != b->depth)
        return a->depth > b->depth ? -1 : 1;
    if (a->crossings != b->crossings)
        return a->crossings > b->crossings ? -1 : 1;
    return a->node < b->node ? -1 : a->node != b->node;
}

/* One bounded BFS per selected entry. Select representative terminal witnesses
 * after traversal: emitting during BFS lets shallow utility leaves exhaust the
 * display budget before deeper queued branches are visited. The predecessor
 * tree stays linear in graph size; we never enumerate all possible paths. */
static int ap_paths(ap_context *c, yyjson_mut_doc *doc, yyjson_mut_val *paths, const int *entries,
                    int entry_count, bool *truncated) {
    int *offset = calloc((size_t)c->n + 2, sizeof(int));
    int *adj = malloc(((size_t)c->m + 1) * sizeof(int));
    int *cursor = calloc((size_t)c->n + 1, sizeof(int));
    int *queue = malloc(((size_t)c->n + 1) * sizeof(int));
    int *previous = malloc(((size_t)c->n + 1) * sizeof(int));
    int *depth = calloc((size_t)c->n + 1, sizeof(int));
    ap_path_candidate *candidates = malloc(((size_t)c->n + 1) * sizeof(*candidates));
    if (!offset || !adj || !cursor || !queue || !previous || !depth || !candidates) {
        free(offset);
        free(adj);
        free(cursor);
        free(queue);
        free(previous);
        free(depth);
        free(candidates);
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < c->m; i++)
        if (ap_execution(c->edges[i].type))
            offset[c->edges[i].source + 1]++;
    for (int i = 0; i < c->n; i++)
        offset[i + 1] += offset[i];
    memcpy(cursor, offset, ((size_t)c->n + 1) * sizeof(int));
    for (int i = 0; i < c->m; i++)
        if (ap_execution(c->edges[i].type))
            adj[cursor[c->edges[i].source]++] = i;
    int produced = 0, rc = CBM_STORE_OK;
    for (int ent = 0; ent < entry_count; ent++) {
        if (produced >= c->options.max_paths) {
            *truncated = true;
            break;
        }
        for (int i = 0; i < c->n; i++)
            previous[i] = -1;
        int entry = entries[ent], head = 0, tail = 0, candidate_count = 0;
        queue[tail++] = entry;
        previous[entry] = -2;
        depth[entry] = 0;
        /* Adjacency cursors are no longer needed; reuse them for first hops. */
        cursor[entry] = entry;
        while (head < tail) {
            if (ap_stopped(c)) {
                rc = CBM_STORE_ERR;
                goto done;
            }
            int node = queue[head++];
            bool extended = false;
            if (depth[node] < c->options.max_depth) {
                for (int j = offset[node]; j < offset[node + 1]; j++) {
                    int edge = adj[j], next = c->edges[edge].target;
                    if (previous[next] != -1)
                        continue;
                    previous[next] = edge;
                    depth[next] = depth[node] + 1;
                    cursor[next] = node == entry ? next : cursor[node];
                    queue[tail++] = next;
                    extended = true;
                }
            } else if (offset[node] != offset[node + 1])
                *truncated = true;
            if (extended || node == entry)
                continue;
            int crossings = 0, current = node;
            while (previous[current] >= 0) {
                int parent = c->edges[previous[current]].source;
                if (c->nodes[parent].component != c->nodes[current].component)
                    crossings++;
                current = parent;
            }
            candidates[candidate_count++] = (ap_path_candidate){.node = node,
                                                                .first_hop = cursor[node],
                                                                .depth = depth[node],
                                                                .crossings = crossings};
        }
        qsort(candidates, (size_t)candidate_count, sizeof(*candidates), ap_path_candidate_compare);
        if (ap_stopped(c)) {
            rc = CBM_STORE_ERR;
            goto done;
        }
        /* Prefer different first-hop branches, then fill spare slots with
         * other deep witnesses. Depth storage can now track selected branches. */
        memset(depth, 0, ((size_t)c->n + 1) * sizeof(int));
        int selection[32], selected = 0, available = c->options.max_paths - produced;
        for (int pass = 0; pass < 2 && selected < available; pass++) {
            for (int i = 0; i < candidate_count && selected < available; i++) {
                ap_path_candidate *candidate = &candidates[i];
                if (candidate->selected || (pass == 0 && depth[candidate->first_hop]))
                    continue;
                depth[candidate->first_hop] = 1;
                candidate->selected = true;
                selection[selected++] = candidate->node;
            }
        }
        if (candidate_count > selected)
            *truncated = true;
        for (int selected_index = 0; selected_index < selected; selected_index++) {
            if (ap_stopped(c)) {
                rc = CBM_STORE_ERR;
                goto done;
            }
            int node = selection[selected_index];
            int chain[17], length = 0, current = node;
            while (previous[current] >= 0 && length < 16) {
                chain[length++] = previous[current];
                current = c->edges[previous[current]].source;
            }
            yyjson_mut_val *path = yyjson_mut_obj(doc), *nodes = yyjson_mut_arr(doc),
                           *edges = yyjson_mut_arr(doc);
            yyjson_mut_obj_add_sint(doc, path, "entrypoint_id", c->nodes[entry].id);
            yyjson_mut_arr_add_val(nodes, ap_symbol(doc, c, entry));
            for (int j = length - 1; j >= 0; j--) {
                ap_edge *edge = &c->edges[chain[j]];
                yyjson_mut_arr_add_val(nodes, ap_symbol(doc, c, edge->target));
                yyjson_mut_arr_add_val(edges, ap_path_edge(doc, c, chain[j]));
            }
            yyjson_mut_obj_add_val(doc, path, "nodes", nodes);
            yyjson_mut_obj_add_val(doc, path, "edges", edges);
            yyjson_mut_arr_add_val(paths, path);
            produced++;
        }
    }
done:
    free(offset);
    free(adj);
    free(cursor);
    free(queue);
    free(previous);
    free(depth);
    free(candidates);
    return rc;
}

static const char *ap_component_basis(const ap_context *c, int i) {
    const ap_component *p = &c->components[i];
    return p->atoms > 1                           ? "interaction_community"
           : c->nodes[p->representative].declared ? "declared_module"
                                                  : "unassigned";
}

static int ap_overview(ap_context *c, yyjson_mut_doc *doc, yyjson_mut_val *root) {
    yyjson_mut_val *overview = yyjson_mut_obj(doc), *groups = yyjson_mut_arr(doc),
                   *parts = yyjson_mut_arr(doc), *connections = yyjson_mut_arr(doc),
                   *totals = yyjson_mut_obj(doc), *limits = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, root, "overview", overview);
    yyjson_mut_obj_add_str(doc, overview, "grouping_basis", "common_source_directory_aggregate");
    yyjson_mut_obj_add_int(doc, overview, "grouping_depth", c->grouping_depth);
    yyjson_mut_obj_add_val(doc, overview, "groups", groups);
    yyjson_mut_obj_add_val(doc, overview, "components", parts);
    yyjson_mut_obj_add_val(doc, overview, "connections", connections);
    yyjson_mut_obj_add_val(doc, overview, "totals", totals);
    yyjson_mut_obj_add_val(doc, overview, "limits", limits);
    int connection_count = 0, shown = 0;
    if (!c->limited) {
        for (int g = 0; g < c->group_count; g++) {
            ap_overview_group *group = &c->groups[g];
            yyjson_mut_val *value = yyjson_mut_obj(doc), *ids = yyjson_mut_arr(doc),
                           *reps = yyjson_mut_arr(doc);
            yyjson_mut_obj_add_strcpy(doc, value, "id", group->id);
            yyjson_mut_obj_add_strcpy(doc, value, "label", group->label);
            yyjson_mut_obj_add_str(doc, value, "role", group->test ? "test" : "non_test");
            yyjson_mut_obj_add_str(doc, value, "basis", "common_source_directory_aggregate");
            yyjson_mut_obj_add_int(doc, value, "component_count", group->components);
            yyjson_mut_obj_add_int(doc, value, "member_count", group->members);
            yyjson_mut_obj_add_int(doc, value, "file_count", group->files);
            int samples = 0;
            for (int i = 0; i < c->component_count; i++)
                if (c->components[i].overview_group == g) {
                    char id[48];
                    ap_component_id(c, i, id);
                    yyjson_mut_arr_add_strcpy(doc, ids, id);
                    if (samples++ < 1)
                        yyjson_mut_arr_add_val(reps,
                                               ap_symbol(doc, c, c->components[i].representative));
                }
            yyjson_mut_obj_add_val(doc, value, "component_ids", ids);
            yyjson_mut_obj_add_val(doc, value, "representatives", reps);
            yyjson_mut_obj_add_int(doc, value, "omitted_representatives",
                                   group->components - (group->components > 0 ? 1 : 0));
            yyjson_mut_arr_add_val(groups, value);
            if (ap_stopped(c))
                return CBM_STORE_ERR;
        }
        for (int i = 0; i < c->component_count; i++) {
            ap_component *part = &c->components[i];
            yyjson_mut_val *value = yyjson_mut_obj(doc), *reps = yyjson_mut_arr(doc);
            char id[48];
            ap_component_id(c, i, id);
            yyjson_mut_obj_add_strcpy(doc, value, "id", id);
            yyjson_mut_obj_add_strcpy(doc, value, "label", c->nodes[part->representative].name);
            yyjson_mut_obj_add_strcpy(doc, value, "group_id", c->groups[part->overview_group].id);
            yyjson_mut_obj_add_str(doc, value, "basis", ap_component_basis(c, i));
            yyjson_mut_obj_add_str(doc, value, "role",
                                   c->nodes[part->root].test ? "test" : "non_test");
            yyjson_mut_obj_add_int(doc, value, "member_count", part->members);
            yyjson_mut_obj_add_int(doc, value, "file_count", part->files);
            for (int j = 0; j < part->sample_count && j < 1; j++)
                yyjson_mut_arr_add_val(reps, ap_symbol(doc, c, part->samples[j]));
            yyjson_mut_obj_add_val(doc, value, "representatives", reps);
            yyjson_mut_obj_add_int(doc, value, "omitted_members",
                                   part->members - (part->sample_count > 0 ? 1 : 0));
            yyjson_mut_arr_add_val(parts, value);
            if ((i & 255) == 0 && ap_stopped(c))
                return CBM_STORE_ERR;
        }
        ap_dep *edges = malloc(((size_t)c->m + 1) * sizeof(*edges));
        if (!edges)
            return CBM_STORE_ERR;
        int count = 0;
        for (int i = 0; i < c->m; i++) {
            ap_edge *edge = &c->edges[i];
            int a = c->nodes[edge->source].component, b = c->nodes[edge->target].component;
            if (a >= 0 && b >= 0 && ap_dependency(edge->type))
                edges[count++] = (ap_dep){c->components[a].overview_group,
                                          c->components[b].overview_group, edge->type, i};
        }
        qsort(edges, (size_t)count, sizeof(*edges), ap_dep_compare);
        for (int i = 0; i < count;) {
            int end = i + 1;
            while (end < count && edges[end].source == edges[i].source &&
                   edges[end].target == edges[i].target && edges[end].type == edges[i].type)
                end++;
            connection_count++;
            if (shown < 4096) {
                yyjson_mut_val *value = yyjson_mut_obj(doc), *witnesses = yyjson_mut_arr(doc);
                yyjson_mut_obj_add_strcpy(doc, value, "source", c->groups[edges[i].source].id);
                yyjson_mut_obj_add_strcpy(doc, value, "target", c->groups[edges[i].target].id);
                yyjson_mut_obj_add_str(doc, value, "type", ap_types[edges[i].type]);
                yyjson_mut_obj_add_int(doc, value, "count", end - i);
                yyjson_mut_arr_add_val(witnesses, ap_witness(doc, c, edges[i].edge));
                yyjson_mut_obj_add_val(doc, value, "witnesses", witnesses);
                yyjson_mut_obj_add_int(doc, value, "omitted_witnesses", end - i - 1);
                yyjson_mut_arr_add_val(connections, value);
                shown++;
            }
            i = end;
            if (ap_stopped(c)) {
                free(edges);
                return CBM_STORE_ERR;
            }
        }
        free(edges);
    }
    yyjson_mut_obj_add_bool(doc, overview, "complete", !c->limited && shown == connection_count);
    yyjson_mut_obj_add_int(doc, totals, "groups", c->limited ? 0 : c->group_count);
    yyjson_mut_obj_add_int(doc, totals, "components", c->limited ? 0 : c->component_count);
    yyjson_mut_obj_add_int(doc, totals, "accounted_nodes", c->limited ? 0 : c->accounted);
    yyjson_mut_obj_add_int(doc, totals, "connections", connection_count);
    yyjson_mut_obj_add_int(doc, limits, "omitted_connections", connection_count - shown);
    return CBM_STORE_OK;
}

/* SCCs here are on the displayed symbol corridor, never on aggregates. */
static int ap_symbol_cycles(ap_context *c, yyjson_mut_doc *doc, yyjson_mut_val *cycles,
                            const int *out, const int *in, const int *forward, const int *reverse,
                            const bool *nodes, const bool *edges) {
    int *stack = malloc(((size_t)c->n + 1) * sizeof(int));
    int *cursor = malloc(((size_t)c->n + 1) * sizeof(int));
    int *order = malloc(((size_t)c->n + 1) * sizeof(int));
    bool *seen = calloc((size_t)c->n + 1, sizeof(bool));
    if (!stack || !cursor || !order || !seen) {
        free(stack);
        free(cursor);
        free(order);
        free(seen);
        return CBM_STORE_ERR;
    }
    memcpy(cursor, out, ((size_t)c->n + 1) * sizeof(int));
    int count = 0, rc = CBM_STORE_OK;
    for (int start = 0; start < c->n; start++)
        if (nodes[start] && !seen[start]) {
            int top = 0;
            stack[top++] = start;
            seen[start] = true;
            while (top) {
                int node = stack[top - 1];
                bool pushed = false;
                while (cursor[node] < out[node + 1]) {
                    int e = forward[cursor[node]++], next = c->edges[e].target;
                    if (edges[e] && !seen[next]) {
                        seen[next] = true;
                        stack[top++] = next;
                        pushed = true;
                        break;
                    }
                }
                if (!pushed) {
                    order[count++] = node;
                    top--;
                }
                if (ap_stopped(c)) {
                    rc = CBM_STORE_ERR;
                    goto done;
                }
            }
        }
    memset(seen, 0, ((size_t)c->n + 1) * sizeof(bool));
    for (int i = count - 1; i >= 0; i--)
        if (!seen[order[i]]) {
            int top = 0, members = 0;
            bool self = false;
            stack[top++] = order[i];
            seen[order[i]] = true;
            yyjson_mut_val *ids = yyjson_mut_arr(doc);
            while (top) {
                int node = stack[--top];
                members++;
                yyjson_mut_arr_add_sint(doc, ids, c->nodes[node].id);
                for (int j = in[node]; j < in[node + 1]; j++) {
                    int e = reverse[j], next = c->edges[e].source;
                    if (!edges[e])
                        continue;
                    if (next == node)
                        self = true;
                    if (!seen[next]) {
                        seen[next] = true;
                        stack[top++] = next;
                    }
                }
                if (ap_stopped(c)) {
                    rc = CBM_STORE_ERR;
                    goto done;
                }
            }
            if (members > 1 || self) {
                yyjson_mut_val *cycle = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_val(doc, cycle, "node_ids", ids);
                yyjson_mut_arr_add_val(cycles, cycle);
            }
        }
done:
    free(stack);
    free(cursor);
    free(order);
    free(seen);
    return rc;
}

static int ap_behavior(ap_context *c, yyjson_mut_doc *doc, yyjson_mut_val *root,
                       yyjson_mut_val *paths) {
    yyjson_mut_val *view = yyjson_mut_obj(doc), *targets = yyjson_mut_arr(doc),
                   *nodes = yyjson_mut_arr(doc), *edges = yyjson_mut_arr(doc),
                   *cycles = yyjson_mut_arr(doc), *limits = yyjson_mut_arr(doc),
                   *totals = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, root, "behavior", view);
    yyjson_mut_obj_add_str(doc, view, "mode", c->options.target_node_id ? "corridor" : "targets");
    yyjson_mut_obj_add_sint(doc, view, "source_id", c->options.entry_node_id);
    if (c->options.target_node_id)
        yyjson_mut_obj_add_sint(doc, view, "target_id", c->options.target_node_id);
    yyjson_mut_obj_add_int(doc, view, "max_hops", c->options.max_depth);
    yyjson_mut_obj_add_val(doc, view, "reachable_targets", targets);
    yyjson_mut_obj_add_val(doc, view, "nodes", nodes);
    yyjson_mut_obj_add_val(doc, view, "edges", edges);
    yyjson_mut_obj_add_val(doc, view, "cycles", cycles);
    yyjson_mut_obj_add_val(doc, view, "limits_hit", limits);
    yyjson_mut_obj_add_val(doc, view, "totals", totals);
    int entry = ap_find(c, c->options.entry_node_id),
        target = ap_find(c, c->options.target_node_id);
    bool valid = !c->limited && entry >= 0 && c->nodes[entry].component >= 0;
    if (!valid || (c->options.target_node_id && (target < 0 || c->nodes[target].component < 0))) {
        yyjson_mut_obj_add_bool(doc, view, "complete", false);
        yyjson_mut_obj_add_bool(doc, view, "corridor_complete", false);
        yyjson_mut_arr_add_str(doc, limits,
                               c->limited ? "analysis_budget"
                               : !valid   ? "select_source"
                                          : "target_not_found");
        yyjson_mut_obj_add_int(doc, totals, "reachable_nodes", 0);
        yyjson_mut_obj_add_int(doc, totals, "corridor_nodes", 0);
        yyjson_mut_obj_add_int(doc, totals, "corridor_edges", 0);
        return CBM_STORE_OK;
    }
    int *out = calloc((size_t)c->n + 2, sizeof(int)), *in = calloc((size_t)c->n + 2, sizeof(int));
    int *forward = malloc(((size_t)c->m + 1) * sizeof(int)),
        *reverse = malloc(((size_t)c->m + 1) * sizeof(int));
    int *queue = malloc(((size_t)c->n + 1) * sizeof(int)),
        *cursor = malloc(((size_t)c->n + 1) * sizeof(int));
    int *distance = malloc(((size_t)c->n + 1) * sizeof(int)),
        *back = malloc(((size_t)c->n + 1) * sizeof(int));
    int *previous = malloc(((size_t)c->n + 1) * sizeof(int)),
        *next = malloc(((size_t)c->n + 1) * sizeof(int));
    bool *keep_nodes = calloc((size_t)c->n + 1, sizeof(bool)),
         *keep_edges = calloc((size_t)c->m + 1, sizeof(bool));
    int rc = CBM_STORE_OK, reachable = 0, kept_nodes = 0, kept_edges = 0, target_count = 0;
    bool complete = true, corridor_complete = true;
    if (!out || !in || !forward || !reverse || !queue || !cursor || !distance || !back ||
        !previous || !next || !keep_nodes || !keep_edges) {
        rc = CBM_STORE_ERR;
        goto done;
    }
    for (int i = 0; i < c->n; i++)
        distance[i] = back[i] = previous[i] = next[i] = -1;
    for (int i = 0; i < c->m; i++)
        if (ap_execution(c->edges[i].type)) {
            out[c->edges[i].source + 1]++;
            in[c->edges[i].target + 1]++;
        }
    for (int i = 0; i < c->n; i++) {
        out[i + 1] += out[i];
        in[i + 1] += in[i];
    }
    memcpy(cursor, out, ((size_t)c->n + 1) * sizeof(int));
    for (int i = 0; i < c->m; i++)
        if (ap_execution(c->edges[i].type))
            forward[cursor[c->edges[i].source]++] = i;
    memcpy(cursor, in, ((size_t)c->n + 1) * sizeof(int));
    for (int i = 0; i < c->m; i++)
        if (ap_execution(c->edges[i].type))
            reverse[cursor[c->edges[i].target]++] = i;
    int head = 0, tail = 0;
    queue[tail++] = entry;
    distance[entry] = 0;
    while (head < tail) {
        int node = queue[head++];
        if (node != entry) {
            reachable++;
            if (target_count < c->options.max_targets || node == target) {
                yyjson_mut_val *symbol = ap_symbol(doc, c, node);
                yyjson_mut_obj_add_int(doc, symbol, "distance", distance[node]);
                yyjson_mut_arr_add_val(targets, symbol);
                target_count++;
            }
        }
        if (distance[node] < c->options.max_depth)
            for (int j = out[node]; j < out[node + 1]; j++) {
                int edge = forward[j], other = c->edges[edge].target;
                if (distance[other] < 0) {
                    distance[other] = distance[node] + 1;
                    previous[other] = edge;
                    queue[tail++] = other;
                }
            }
        if (ap_stopped(c)) {
            rc = CBM_STORE_ERR;
            goto done;
        }
    }
    if (reachable > target_count) {
        complete = false;
        yyjson_mut_arr_add_str(doc, limits, "target_catalog_budget");
    }
    if (c->options.target_node_id) {
        head = tail = 0;
        queue[tail++] = target;
        back[target] = 0;
        while (head < tail) {
            int node = queue[head++];
            if (back[node] < c->options.max_depth)
                for (int j = in[node]; j < in[node + 1]; j++) {
                    int edge = reverse[j], other = c->edges[edge].source;
                    if (back[other] < 0) {
                        back[other] = back[node] + 1;
                        next[other] = edge;
                        queue[tail++] = other;
                    }
                }
            if (ap_stopped(c)) {
                rc = CBM_STORE_ERR;
                goto done;
            }
        }
        for (int i = 0; i < c->n; i++)
            if (distance[i] >= 0 && back[i] >= 0 && distance[i] + back[i] <= c->options.max_depth) {
                keep_nodes[i] = true;
                kept_nodes++;
            }
        for (int i = 0; i < c->m; i++) {
            ap_edge *edge = &c->edges[i];
            if (ap_execution(edge->type) && distance[edge->source] >= 0 &&
                back[edge->target] >= 0 &&
                distance[edge->source] + 1 + back[edge->target] <= c->options.max_depth) {
                keep_edges[i] = true;
                kept_edges++;
            }
        }
        if (kept_nodes > c->options.max_corridor_nodes ||
            kept_edges > c->options.max_corridor_edges) {
            complete = corridor_complete = false;
            yyjson_mut_arr_add_str(doc, limits, "corridor_budget");
            /* Abstain instead of returning a broken graph with hidden joins. */
        } else {
            c->evidence_scope = AP_EVIDENCE_CORRIDOR;
            for (int i = 0; i < c->n; i++)
                if (keep_nodes[i])
                    yyjson_mut_arr_add_val(nodes, ap_symbol(doc, c, i));
            for (int i = 0; i < c->m; i++)
                if (keep_edges[i]) {
                    yyjson_mut_arr_add_val(edges, ap_path_edge(doc, c, i));
                    if (ap_stopped(c)) {
                        rc = CBM_STORE_ERR;
                        goto done;
                    }
                }
            if (ap_symbol_cycles(c, doc, cycles, out, in, forward, reverse, keep_nodes,
                                 keep_edges) != CBM_STORE_OK) {
                rc = CBM_STORE_ERR;
                goto done;
            }
            /* Each witness joins a shortest prefix, a real corridor edge, and
             * a shortest suffix. These bounded graph walks may revisit a node;
             * they never imply feasible branch conditions or runtime order. */
            c->evidence_scope = AP_EVIDENCE_PATH;
            int saved[32][16], lengths[32], produced = 0;
            for (int e = -1; e < c->m && produced < c->options.max_paths; e++) {
                if (e < 0 && distance[target] < 0)
                    continue;
                if (e >= 0 && !keep_edges[e])
                    continue;
                int chain[16], prefix[16], length = 0, before = 0;
                int node = e < 0 ? target : c->edges[e].source;
                while (previous[node] >= 0 && before < 16) {
                    prefix[before++] = previous[node];
                    node = c->edges[previous[node]].source;
                }
                for (int i = before - 1; i >= 0; i--)
                    chain[length++] = prefix[i];
                if (e >= 0) {
                    chain[length++] = e;
                    node = c->edges[e].target;
                    while (next[node] >= 0 && length < 16) {
                        chain[length++] = next[node];
                        node = c->edges[next[node]].target;
                    }
                }
                bool duplicate = false;
                for (int i = 0; i < produced; i++)
                    if (lengths[i] == length &&
                        !memcmp(saved[i], chain, (size_t)length * sizeof(int)))
                        duplicate = true;
                if (duplicate)
                    continue;
                memcpy(saved[produced], chain, (size_t)length * sizeof(int));
                lengths[produced++] = length;
                yyjson_mut_val *path = yyjson_mut_obj(doc), *pn = yyjson_mut_arr(doc),
                               *pe = yyjson_mut_arr(doc);
                yyjson_mut_obj_add_sint(doc, path, "entrypoint_id", c->nodes[entry].id);
                yyjson_mut_arr_add_val(pn, ap_symbol(doc, c, entry));
                for (int i = 0; i < length; i++) {
                    yyjson_mut_arr_add_val(pn, ap_symbol(doc, c, c->edges[chain[i]].target));
                    yyjson_mut_arr_add_val(pe, ap_path_edge(doc, c, chain[i]));
                }
                yyjson_mut_obj_add_val(doc, path, "nodes", pn);
                yyjson_mut_obj_add_val(doc, path, "edges", pe);
                yyjson_mut_arr_add_val(paths, path);
                if (ap_stopped(c)) {
                    rc = CBM_STORE_ERR;
                    goto done;
                }
            }
        }
        yyjson_mut_obj_add_bool(doc, view, "reachable", distance[target] >= 0);
    }
    yyjson_mut_obj_add_bool(doc, view, "complete", complete);
    yyjson_mut_obj_add_bool(doc, view, "corridor_complete", corridor_complete);
    yyjson_mut_obj_add_int(doc, totals, "reachable_nodes", reachable);
    yyjson_mut_obj_add_int(doc, totals, "omitted_targets", reachable - target_count);
    yyjson_mut_obj_add_int(doc, totals, "corridor_nodes", kept_nodes);
    yyjson_mut_obj_add_int(doc, totals, "corridor_edges", kept_edges);
done:
    c->evidence_scope = AP_EVIDENCE_CONTEXT;
    free(out);
    free(in);
    free(forward);
    free(reverse);
    free(queue);
    free(cursor);
    free(distance);
    free(back);
    free(previous);
    free(next);
    free(keep_nodes);
    free(keep_edges);
    return rc;
}

static int ap_render(ap_context *c, const char *project, char **out_json) {
    memset(c->evidence_bytes, 0, sizeof(c->evidence_bytes));
    c->evidence_scope = AP_EVIDENCE_CONTEXT;
    c->evidence_omitted = false;
    c->cycle_count = c->omitted_cycles = 0;
    ap_json_budget budget = {0};
    yyjson_alc allocator = {ap_json_malloc, ap_json_realloc, ap_json_free, &budget};
    yyjson_mut_doc *doc = yyjson_mut_doc_new(&allocator);
    if (!doc)
        return CBM_STORE_ERR;
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_val *parts = yyjson_mut_arr(doc), *dependencies = yyjson_mut_arr(doc),
                   *cycles = yyjson_mut_arr(doc), *entries = yyjson_mut_arr(doc),
                   *paths = yyjson_mut_arr(doc), *warnings = yyjson_mut_arr(doc),
                   *totals = yyjson_mut_obj(doc), *limits = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_int(doc, root, "schema_version", 1);
    yyjson_mut_obj_add_str(doc, root, "kind", "static_projection");
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    yyjson_mut_obj_add_val(doc, root, "components", parts);
    yyjson_mut_obj_add_val(doc, root, "dependencies", dependencies);
    yyjson_mut_obj_add_val(doc, root, "cycles", cycles);
    yyjson_mut_obj_add_val(doc, root, "entrypoints", entries);
    yyjson_mut_obj_add_val(doc, root, "paths", paths);
    yyjson_mut_obj_add_val(doc, root, "warnings", warnings);
    yyjson_mut_obj_add_val(doc, root, "totals", totals);
    yyjson_mut_obj_add_val(doc, root, "limits", limits);
    int shown = 0, dependency_count = 0, dependency_shown = 0, entry_count = 0, entry_total = 0;
    int entry_indexes[AP_ENTRYPOINTS];
    bool paths_truncated = false;
    int rc = CBM_STORE_OK;
    ap_dep *deps = NULL;
    if (ap_overview(c, doc, root) != CBM_STORE_OK) {
        rc = CBM_STORE_ERR;
        goto done;
    }
    if (!c->limited) {
        /* Prefer the largest connected candidates, retaining deterministic IDs.
         * Never relabel discarded components as a single invented component. */
        ap_rank *ranks = malloc(((size_t)c->component_count + 1) * sizeof(*ranks));
        if (!ranks) {
            rc = CBM_STORE_ERR;
            goto done;
        }
        for (int i = 0; i < c->component_count; i++) {
            c->components[i].displayed = -1;
            ranks[i] = (ap_rank){i, c->components[i].members, c->components[i].degree};
        }
        qsort(ranks, (size_t)c->component_count, sizeof(*ranks), ap_rank_compare);
        for (int pick = 0; pick < c->options.max_components && pick < c->component_count; pick++)
            c->components[ranks[pick].index].displayed = shown++;
        free(ranks);
        if (ap_stopped(c)) {
            rc = CBM_STORE_ERR;
            goto done;
        }
        for (int i = 0; i < c->component_count; i++) {
            ap_component *p = &c->components[i];
            if (p->displayed < 0)
                continue;
            yyjson_mut_val *part = yyjson_mut_obj(doc), *representatives = yyjson_mut_arr(doc);
            char id[48];
            ap_component_id(c, i, id);
            yyjson_mut_obj_add_strcpy(doc, part, "id", id);
            yyjson_mut_obj_add_strcpy(doc, part, "label", c->nodes[p->representative].name);
            yyjson_mut_obj_add_str(doc, part, "basis",
                                   p->atoms > 1                           ? "interaction_community"
                                   : c->nodes[p->representative].declared ? "declared_module"
                                                                          : "unassigned");
            yyjson_mut_obj_add_int(doc, part, "member_count", p->members);
            yyjson_mut_obj_add_int(doc, part, "file_count", p->files);
            yyjson_mut_obj_add_int(doc, part, "atom_count", p->atoms);
            bool test = c->nodes[p->root].test;
            yyjson_mut_obj_add_str(doc, part, "role", test ? "test" : "non_test");
            yyjson_mut_obj_add_str(doc, part, "role_basis",
                                   !test                             ? "no_test_evidence"
                                   : c->nodes[p->root].test_property ? "indexed_is_test"
                                                                     : "test_path_convention");
            for (int j = 0; j < p->sample_count && j < 1; j++)
                yyjson_mut_arr_add_val(representatives, ap_symbol(doc, c, p->samples[j]));
            yyjson_mut_obj_add_val(doc, part, "representatives", representatives);
            yyjson_mut_obj_add_int(doc, part, "omitted_members",
                                   p->members - (p->sample_count > 0 ? 1 : 0));
            yyjson_mut_arr_add_val(parts, part);
        }
        deps = malloc(((size_t)c->m + 1) * sizeof(*deps));
        if (!deps) {
            rc = CBM_STORE_ERR;
            goto done;
        }
        int count = 0;
        for (int i = 0; i < c->m; i++) {
            ap_edge *e = &c->edges[i];
            int a = c->nodes[e->source].component, b = c->nodes[e->target].component;
            if (a >= 0 && b >= 0 && a != b && ap_dependency(e->type))
                deps[count++] = (ap_dep){a, b, e->type, i};
        }
        qsort(deps, (size_t)count, sizeof(*deps), ap_dep_compare);
        for (int i = 0; i < count;) {
            int end = i + 1;
            while (end < count && deps[end].source == deps[i].source &&
                   deps[end].target == deps[i].target && deps[end].type == deps[i].type)
                end++;
            dependency_count++;
            if (dependency_shown < c->options.max_dependencies &&
                c->components[deps[i].source].displayed >= 0 &&
                c->components[deps[i].target].displayed >= 0) {
                yyjson_mut_val *dep = yyjson_mut_obj(doc), *witnesses = yyjson_mut_arr(doc);
                char a[48], b[48];
                ap_component_id(c, deps[i].source, a);
                ap_component_id(c, deps[i].target, b);
                yyjson_mut_obj_add_strcpy(doc, dep, "source", a);
                yyjson_mut_obj_add_strcpy(doc, dep, "target", b);
                yyjson_mut_obj_add_str(doc, dep, "type", ap_types[deps[i].type]);
                yyjson_mut_obj_add_int(doc, dep, "count", end - i);
                yyjson_mut_arr_add_val(witnesses, ap_witness(doc, c, deps[i].edge));
                yyjson_mut_obj_add_val(doc, dep, "witnesses", witnesses);
                yyjson_mut_obj_add_int(doc, dep, "omitted_witnesses", end - i - 1);
                yyjson_mut_arr_add_val(dependencies, dep);
                dependency_shown++;
            }
            i = end;
            if (ap_stopped(c)) {
                rc = CBM_STORE_ERR;
                goto done;
            }
        }
        if (ap_cycles(c, doc, cycles, deps, count) != CBM_STORE_OK) {
            rc = CBM_STORE_ERR;
            goto done;
        }
        for (int i = 0; i < c->n; i++) {
            ap_node *node = &c->nodes[i];
            if (node->component < 0)
                continue;
            if (node->entry) {
                entry_total++;
                if (entry_count < AP_ENTRYPOINTS) {
                    entry_indexes[entry_count++] = i;
                    c->evidence_scope = node->id == c->options.entry_node_id ? AP_EVIDENCE_PATH
                                                                             : AP_EVIDENCE_CONTEXT;
                    yyjson_mut_arr_add_val(entries, ap_symbol(doc, c, i));
                    c->evidence_scope = AP_EVIDENCE_CONTEXT;
                }
            }
        }
        c->evidence_scope = AP_EVIDENCE_PATH;
        if (c->options.target_node_id) {
            /* The target query produces exact endpoint witnesses below. */
        } else if (c->options.entry_node_id) {
            int selected = ap_find(c, c->options.entry_node_id);
            if (selected >= 0 && c->nodes[selected].component >= 0) {
                if (ap_paths(c, doc, paths, &selected, 1, &paths_truncated) != CBM_STORE_OK) {
                    rc = CBM_STORE_ERR;
                    goto done;
                }
            } else
                yyjson_mut_arr_add_str(
                    doc, warnings,
                    "The selected entry point is absent from this indexed generation.");
        } else if (ap_paths(c, doc, paths, entry_indexes, entry_count, &paths_truncated) !=
                   CBM_STORE_OK) {
            rc = CBM_STORE_ERR;
            goto done;
        }
        c->evidence_scope = AP_EVIDENCE_CONTEXT;
    }
    if (ap_behavior(c, doc, root, paths) != CBM_STORE_OK) {
        rc = CBM_STORE_ERR;
        goto done;
    }
    yyjson_mut_obj_add_str(doc, root, "status", c->limited ? "limited" : "ready");
    bool complete =
        !c->limited && shown == c->component_count && dependency_shown == dependency_count;
    yyjson_mut_obj_add_bool(doc, root, "complete", complete);
    yyjson_mut_obj_add_sint(doc, totals, "nodes", c->total_nodes);
    yyjson_mut_obj_add_sint(doc, totals, "files", c->total_files);
    yyjson_mut_obj_add_sint(doc, totals, "edges", c->total_edges);
    yyjson_mut_obj_add_int(doc, totals, "accounted_nodes", c->limited ? 0 : c->accounted);
    yyjson_mut_obj_add_int(doc, totals, "structural_nodes", c->limited ? 0 : c->structural);
    yyjson_mut_obj_add_int(doc, totals, "components", c->limited ? 0 : c->component_count);
    yyjson_mut_obj_add_int(doc, totals, "dependencies", dependency_count);
    yyjson_mut_obj_add_int(doc, totals, "entrypoints", entry_total);
    yyjson_mut_obj_add_int(doc, totals, "cycles", c->limited ? 0 : c->cycle_count);
    yyjson_mut_obj_add_int(doc, totals, "relationship_edges",
                           c->limited ? 0 : c->relationship_edges);
    yyjson_mut_obj_add_int(doc, totals, "unmodeled_edges", c->limited ? 0 : c->unmodeled_edges);
    yyjson_mut_obj_add_int(doc, limits, "node_budget", c->options.max_nodes);
    yyjson_mut_obj_add_int(doc, limits, "edge_budget", c->options.max_edges);
    yyjson_mut_obj_add_int(doc, limits, "omitted_components",
                           c->limited ? 0 : c->component_count - shown);
    yyjson_mut_obj_add_int(doc, limits, "omitted_dependencies",
                           dependency_count - dependency_shown);
    yyjson_mut_obj_add_int(doc, limits, "omitted_entrypoints", entry_total - entry_count);
    yyjson_mut_obj_add_int(doc, limits, "omitted_cycles", c->limited ? 0 : c->omitted_cycles);
    yyjson_mut_obj_add_bool(doc, limits, "paths_truncated", paths_truncated);
    yyjson_mut_obj_add_int(doc, limits, "path_depth", c->options.max_depth);
    yyjson_mut_obj_add_uint(doc, root, "elapsed_ms", cbm_now_ms() - c->started);
    yyjson_mut_arr_add_str(
        doc, warnings,
        "Component candidates are inferred from indexed declarations and interactions; they do not "
        "establish intended responsibilities or deployment boundaries.");
    yyjson_mut_arr_add_str(
        doc, warnings,
        "Paths are bounded static witnesses, not observed executions. Callback references are not "
        "invocation evidence. Index coverage gaps remain possible.");
    if (c->unmodeled_edges && !c->limited)
        yyjson_mut_arr_add_str(doc, warnings,
                               "Some indexed edge types are outside this projection's relationship "
                               "vocabulary; see unmodeled_edges. Similarity and change-history "
                               "edges are not structural interactions.");
    if (c->limited)
        yyjson_mut_arr_add_str(doc, warnings, c->limited);
    if (!complete && !c->limited)
        yyjson_mut_arr_add_str(doc, warnings,
                               "The displayed projection is capped; totals include omitted "
                               "components and dependencies.");
    if (c->omitted_cycles && !c->limited)
        yyjson_mut_arr_add_str(
            doc, warnings,
            "Dependency cycles involving omitted components are not drawn; see omitted_cycles.");
    if (c->options.include_behavior_evidence) {
        bool evidence_limited =
            c->evidence_omitted || c->metadata_budget.failed || c->evidence_suppressed;
        yyjson_mut_obj_add_bool(doc, limits, "behavior_evidence_budget_hit", evidence_limited);
        if (c->evidence_suppressed)
            yyjson_mut_arr_add_str(doc, warnings,
                                   "Optional behavior evidence was omitted entirely to keep the "
                                   "base graph within the response memory budget.");
        else if (evidence_limited)
            yyjson_mut_arr_add_str(
                doc, warnings,
                "Optional behavior evidence was omitted from some symbols or calls after reaching "
                "its memory budget; absent fields remain unknown.");
    }
    if (budget.failed)
        rc = CBM_STORE_SCAN_LIMIT;
    else {
        *out_json =
            yyjson_mut_write_opts(doc, YYJSON_WRITE_ALLOW_INVALID_UNICODE, &allocator, NULL, NULL);
        if (!*out_json)
            rc = budget.failed ? CBM_STORE_SCAN_LIMIT : CBM_STORE_ERR;
    }
done:
    free(deps);
    yyjson_mut_doc_free(doc);
    return rc;
}

int cbm_store_architecture_projection(cbm_store_t *store, const char *project,
                                      const cbm_architecture_projection_options_t *options,
                                      char **out_json) {
    if (!store || !project || !project[0] || !out_json)
        return CBM_STORE_ERR;
    *out_json = NULL;
    ap_context c = {.db = cbm_store_get_db(store), .started = cbm_now_ms()};
    if (options)
        c.options = *options;
    if (c.options.entry_node_id < 0 || c.options.target_node_id < 0 ||
        (c.options.target_node_id && !c.options.entry_node_id))
        return CBM_STORE_ERR;
    c.options.max_nodes = ap_bound(c.options.max_nodes, 100000, 250000);
    c.options.max_edges = ap_bound(c.options.max_edges, 500000, 1000000);
    c.options.max_components = ap_bound(c.options.max_components, 256, 512);
    c.options.max_dependencies = ap_bound(c.options.max_dependencies, 2048, 4096);
    c.options.max_paths = ap_bound(c.options.max_paths, 12, 32);
    c.options.max_depth = ap_bound(c.options.max_depth, 10, 16);
    c.options.max_targets = ap_bound(c.options.max_targets, 256, 1024);
    c.options.max_corridor_nodes = ap_bound(c.options.max_corridor_nodes, 1024, 4096);
    c.options.max_corridor_edges = ap_bound(c.options.max_corridor_edges, 4096, 16384);
    if (!c.options.deadline_ms)
        c.options.deadline_ms = c.started + 5000;
    if (ap_stopped(&c))
        return c.cancelled ? CBM_STORE_CANCELLED : CBM_STORE_SCAN_LIMIT;
    /* Own a read transaction only when the caller has not established one. */
    bool transaction = sqlite3_get_autocommit(c.db) != 0;
    sqlite3_progress_handler(c.db, 1000, ap_progress, &c);
    int rc = transaction && sqlite3_exec(c.db, "BEGIN", NULL, NULL, NULL) != SQLITE_OK
                 ? CBM_STORE_ERR
                 : CBM_STORE_OK;
    if (rc == CBM_STORE_OK)
        rc = ap_counts(&c, project);
    if (rc == CBM_STORE_OK && !c.limited)
        rc = ap_load(&c, project);
    if (rc == CBM_STORE_OK && !c.limited)
        rc = ap_ownership(&c);
    if (rc == CBM_STORE_OK && !c.limited)
        rc = ap_components(&c);
    if (rc == CBM_STORE_OK && !c.limited)
        rc = ap_overview_grouping(&c);
    if (c.expired) {
        c.limited = "Analysis reached its deadline; no partial architecture was inferred.";
        rc = CBM_STORE_OK;
    }
    if (c.limited && !c.cancelled)
        rc = CBM_STORE_OK;
    if (rc == CBM_STORE_OK && !c.cancelled)
        rc = ap_render(&c, project, out_json);
    if (rc == CBM_STORE_SCAN_LIMIT && c.options.include_behavior_evidence && !c.limited &&
        !c.expired && !c.cancelled && !*out_json) {
        /* Reuse the loaded graph. Optional evidence must not turn a renderable
         * base projection into an empty architecture or trigger another scan. */
        c.evidence_suppressed = true;
        rc = ap_render(&c, project, out_json);
    }
    if ((rc == CBM_STORE_SCAN_LIMIT || c.expired) && !c.cancelled && !*out_json) {
        c.limited = c.expired
                        ? "Analysis reached its deadline; no partial architecture was inferred."
                        : "The architecture response exceeded its memory budget; narrow the "
                          "requested projection.";
        rc = ap_render(&c, project, out_json);
    }
    sqlite3_progress_handler(c.db, 0, NULL, NULL);
    sqlite3_finalize(c.provenance);
    sqlite3_finalize(c.symbol_metadata);
    if (transaction)
        sqlite3_exec(c.db, "ROLLBACK", NULL, NULL, NULL);
    for (int i = 0; i < c.n; i++) {
        free(c.nodes[i].name);
        free(c.nodes[i].qn);
        free(c.nodes[i].file);
        free(c.nodes[i].label);
        if (c.symbol_properties)
            yyjson_doc_free(c.symbol_properties[i]);
    }
    free(c.symbol_properties);
    free(c.symbol_properties_loaded);
    for (int i = 0; i < c.component_count; i++)
        free(c.components[i].directory);
    for (int i = 0; i < c.group_count; i++) {
        free(c.groups[i].id);
        free(c.groups[i].label);
    }
    free(c.nodes);
    free(c.edges);
    free(c.components);
    if (c.cancelled) {
        free(*out_json);
        *out_json = NULL;
        return CBM_STORE_CANCELLED;
    }
    return rc;
}
