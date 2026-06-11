/*
 * pass_cross_repo_maven.c - Cross-repo Maven artifact dependency matching.
 */
#include "pipeline/pass_cross_repo_maven.h"
#include "foundation/constants.h"

#include <sqlite3/sqlite3.h>
#include <yyjson/yyjson.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    MCR_PATH_BUF = 1024,
    MCR_QN_BUF = 512,
    MCR_PROPS_BUF = 2048,
    MCR_MAX_EDGES = 4096,
    MCR_MAX_POM_SIZE = 2 * 1024 * 1024,
    MCR_INIT_CAP = 32,
};

typedef struct {
    char group_id[CBM_SZ_128];
    char artifact_id[CBM_SZ_128];
    char pom_path[CBM_SZ_512];
} mcr_artifact_t;

typedef struct {
    char group_id[CBM_SZ_128];
    char artifact_id[CBM_SZ_128];
    char version[CBM_SZ_128];
    char scope[CBM_SZ_64];
    char relation[CBM_SZ_32];
    char context_group_id[CBM_SZ_128];
    char context_artifact_id[CBM_SZ_128];
    char pom_path[CBM_SZ_512];
} mcr_dependency_t;

static bool text_has_suffix(const char *s, const char *suffix) {
    if (!s || !suffix) {
        return false;
    }
    size_t sl = strlen(s);
    size_t tl = strlen(suffix);
    return sl >= tl && strcmp(s + sl - tl, suffix) == 0;
}

static void trim_copy(const char *start, size_t len, char *out, size_t out_sz) {
    if (!out || out_sz == 0) {
        return;
    }
    out[0] = '\0';
    if (!start) {
        return;
    }
    while (len > 0 && (*start == ' ' || *start == '\n' || *start == '\r' || *start == '\t')) {
        start++;
        len--;
    }
    while (len > 0) {
        char c = start[len - SKIP_ONE];
        if (c != ' ' && c != '\n' && c != '\r' && c != '\t') {
            break;
        }
        len--;
    }
    if (len >= out_sz) {
        len = out_sz - SKIP_ONE;
    }
    memcpy(out, start, len);
    out[len] = '\0';
}

static bool ptr_in_span(const char *p, const char *start, const char *end) {
    return p && start && end && p >= start && p < end;
}

static bool ptr_in_xml_comment(const char *xml, const char *p) {
    const char *scan = xml;
    while ((scan = strstr(scan, "<!--")) != NULL) {
        const char *close = strstr(scan + strlen("<!--"), "-->");
        if (!close) {
            return p >= scan;
        }
        const char *comment_end = close + strlen("-->");
        if (ptr_in_span(p, scan, comment_end)) {
            return true;
        }
        scan = comment_end;
    }
    return false;
}

static bool ptr_in_xml_block_named(const char *xml, const char *tag, const char *p) {
    char open_pat[CBM_SZ_64];
    char close_pat[CBM_SZ_64];
    snprintf(open_pat, sizeof(open_pat), "<%s", tag);
    snprintf(close_pat, sizeof(close_pat), "</%s>", tag);

    const char *scan = xml;
    while ((scan = strstr(scan, open_pat)) != NULL) {
        const char *open_end = strchr(scan, '>');
        if (!open_end) {
            return false;
        }
        const char *close = strstr(open_end + SKIP_ONE, close_pat);
        if (!close) {
            return false;
        }
        const char *block_end = close + strlen(close_pat);
        if (ptr_in_span(p, scan, block_end)) {
            return true;
        }
        scan = block_end;
    }
    return false;
}

static bool find_xml_block(const char *xml, const char *tag, const char **block_start,
                           const char **block_end) {
    char open_pat[CBM_SZ_64];
    char close_pat[CBM_SZ_64];
    snprintf(open_pat, sizeof(open_pat), "<%s", tag);
    snprintf(close_pat, sizeof(close_pat), "</%s>", tag);
    const char *open = strstr(xml, open_pat);
    if (!open) {
        return false;
    }
    const char *open_end = strchr(open, '>');
    if (!open_end) {
        return false;
    }
    const char *close = strstr(open_end + SKIP_ONE, close_pat);
    if (!close) {
        return false;
    }
    *block_start = open;
    *block_end = close + strlen(close_pat);
    return true;
}

static bool xml_tag_text(const char *start, const char *end, const char *tag, char *out,
                         size_t out_sz) {
    char open_pat[CBM_SZ_64];
    char close_pat[CBM_SZ_64];
    snprintf(open_pat, sizeof(open_pat), "<%s", tag);
    snprintf(close_pat, sizeof(close_pat), "</%s>", tag);

    const char *p = start;
    while ((p = strstr(p, open_pat)) != NULL && (!end || p < end)) {
        const char *open_end = strchr(p, '>');
        if (!open_end || (end && open_end >= end)) {
            return false;
        }
        const char *close = strstr(open_end + SKIP_ONE, close_pat);
        if (!close || (end && close > end)) {
            return false;
        }
        trim_copy(open_end + SKIP_ONE, (size_t)(close - open_end - SKIP_ONE), out, out_sz);
        return out[0] != '\0';
    }
    return false;
}

static bool xml_tag_text_outside_project_blocks(const char *xml, const char *tag, char *out,
                                                size_t out_sz) {
    const char *parent_start = NULL;
    const char *parent_end = NULL;
    const char *deps_start = NULL;
    const char *deps_end = NULL;
    find_xml_block(xml, "parent", &parent_start, &parent_end);
    find_xml_block(xml, "dependencies", &deps_start, &deps_end);

    char open_pat[CBM_SZ_64];
    char close_pat[CBM_SZ_64];
    snprintf(open_pat, sizeof(open_pat), "<%s", tag);
    snprintf(close_pat, sizeof(close_pat), "</%s>", tag);

    const char *p = xml;
    while ((p = strstr(p, open_pat)) != NULL) {
        if (ptr_in_span(p, parent_start, parent_end) || ptr_in_span(p, deps_start, deps_end)) {
            p += strlen(open_pat);
            continue;
        }
        const char *open_end = strchr(p, '>');
        if (!open_end) {
            return false;
        }
        const char *close = strstr(open_end + SKIP_ONE, close_pat);
        if (!close) {
            return false;
        }
        trim_copy(open_end + SKIP_ONE, (size_t)(close - open_end - SKIP_ONE), out, out_sz);
        return out[0] != '\0';
    }
    return false;
}

static char *read_text_file_cap(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long sz = ftell(f);
    if (sz < 0 || sz > MCR_MAX_POM_SIZE) {
        fclose(f);
        return NULL;
    }
    rewind(f);
    char *buf = malloc((size_t)sz + SKIP_ONE);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    size_t n = fread(buf, SKIP_ONE, (size_t)sz, f);
    fclose(f);
    buf[n] = '\0';
    return buf;
}

static char *project_root_dup(cbm_store_t *store, const char *project) {
    cbm_project_t p = {0};
    if (cbm_store_get_project(store, project, &p) != CBM_STORE_OK || !p.root_path) {
        return NULL;
    }
    char *root = malloc(strlen(p.root_path) + SKIP_ONE);
    if (root) {
        strcpy(root, p.root_path);
    }
    free((void *)p.name);
    free((void *)p.indexed_at);
    free((void *)p.root_path);
    return root;
}

static int list_pom_paths(cbm_store_t *store, const char *project, char ***out) {
    *out = NULL;
    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db) {
        return 0;
    }
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
                           "SELECT DISTINCT file_path FROM nodes WHERE project=?1 AND "
                           "label='File' AND (name='pom.xml' OR file_path LIKE '%/pom.xml')",
                           CBM_NOT_FOUND, &st, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(st, SKIP_ONE, project, CBM_NOT_FOUND, SQLITE_STATIC);

    int cap = MCR_INIT_CAP;
    int count = 0;
    char **paths = malloc((size_t)cap * sizeof(char *));
    if (!paths) {
        sqlite3_finalize(st);
        return 0;
    }
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *path = (const char *)sqlite3_column_text(st, 0);
        if (!path || !text_has_suffix(path, "pom.xml")) {
            continue;
        }
        if (count >= cap) {
            cap *= PAIR_LEN;
            char **tmp = realloc(paths, (size_t)cap * sizeof(char *));
            if (!tmp) {
                break;
            }
            paths = tmp;
        }
        paths[count] = malloc(strlen(path) + SKIP_ONE);
        if (paths[count]) {
            strcpy(paths[count], path);
            count++;
        }
    }
    sqlite3_finalize(st);
    *out = paths;
    return count;
}

static void free_string_list(char **items, int count) {
    for (int i = 0; i < count; i++) {
        free(items[i]);
    }
    free(items);
}

static int collect_artifacts(cbm_store_t *store, const char *project, mcr_artifact_t **out) {
    *out = NULL;
    char *root = project_root_dup(store, project);
    if (!root) {
        return 0;
    }

    char **paths = NULL;
    int path_count = list_pom_paths(store, project, &paths);
    int cap = MCR_INIT_CAP;
    int count = 0;
    mcr_artifact_t *items = calloc((size_t)cap, sizeof(mcr_artifact_t));
    if (!items) {
        free_string_list(paths, path_count);
        free(root);
        return 0;
    }

    for (int i = 0; i < path_count; i++) {
        char full[MCR_PATH_BUF];
        snprintf(full, sizeof(full), "%s/%s", root, paths[i]);
        char *xml = read_text_file_cap(full);
        if (!xml) {
            continue;
        }

        char parent_group[CBM_SZ_128] = {0};
        const char *parent_start = NULL;
        const char *parent_end = NULL;
        if (find_xml_block(xml, "parent", &parent_start, &parent_end)) {
            xml_tag_text(parent_start, parent_end, "groupId", parent_group, sizeof(parent_group));
        }

        char group[CBM_SZ_128] = {0};
        char artifact[CBM_SZ_128] = {0};
        xml_tag_text_outside_project_blocks(xml, "groupId", group, sizeof(group));
        xml_tag_text_outside_project_blocks(xml, "artifactId", artifact, sizeof(artifact));
        if (!group[0] && parent_group[0]) {
            snprintf(group, sizeof(group), "%s", parent_group);
        }
        if (group[0] && artifact[0]) {
            if (count >= cap) {
                cap *= PAIR_LEN;
                mcr_artifact_t *tmp = realloc(items, (size_t)cap * sizeof(*items));
                if (!tmp) {
                    free(xml);
                    break;
                }
                items = tmp;
            }
            memset(&items[count], 0, sizeof(items[count]));
            snprintf(items[count].group_id, sizeof(items[count].group_id), "%s", group);
            snprintf(items[count].artifact_id, sizeof(items[count].artifact_id), "%s", artifact);
            snprintf(items[count].pom_path, sizeof(items[count].pom_path), "%s", paths[i]);
            count++;
        }
        free(xml);
    }

    free_string_list(paths, path_count);
    free(root);
    *out = items;
    return count;
}

static bool add_dependency(mcr_dependency_t **items, int *count, int *cap, const char *group,
                           const char *artifact, const char *version, const char *scope,
                           const char *relation, const char *context_group,
                           const char *context_artifact, const char *pom_path) {
    if (!items || !*items || !count || !cap || !group || !artifact || !group[0] ||
        !artifact[0]) {
        return false;
    }
    if (*count >= *cap) {
        *cap *= PAIR_LEN;
        mcr_dependency_t *tmp = realloc(*items, (size_t)*cap * sizeof(**items));
        if (!tmp) {
            return false;
        }
        *items = tmp;
    }
    mcr_dependency_t *dep = &(*items)[*count];
    memset(dep, 0, sizeof(*dep));
    snprintf(dep->group_id, sizeof(dep->group_id), "%s", group);
    snprintf(dep->artifact_id, sizeof(dep->artifact_id), "%s", artifact);
    snprintf(dep->version, sizeof(dep->version), "%s", version ? version : "");
    snprintf(dep->scope, sizeof(dep->scope), "%s", scope ? scope : "");
    snprintf(dep->relation, sizeof(dep->relation), "%s", relation ? relation : "dependency");
    snprintf(dep->context_group_id, sizeof(dep->context_group_id), "%s",
             context_group ? context_group : "");
    snprintf(dep->context_artifact_id, sizeof(dep->context_artifact_id), "%s",
             context_artifact ? context_artifact : "");
    snprintf(dep->pom_path, sizeof(dep->pom_path), "%s", pom_path ? pom_path : "");
    (*count)++;
    return true;
}

static int collect_dependencies(cbm_store_t *store, const char *project, mcr_dependency_t **out) {
    *out = NULL;
    char *root = project_root_dup(store, project);
    if (!root) {
        return 0;
    }

    char **paths = NULL;
    int path_count = list_pom_paths(store, project, &paths);
    int cap = MCR_INIT_CAP;
    int count = 0;
    mcr_dependency_t *items = calloc((size_t)cap, sizeof(mcr_dependency_t));
    if (!items) {
        free_string_list(paths, path_count);
        free(root);
        return 0;
    }

    for (int i = 0; i < path_count; i++) {
        char full[MCR_PATH_BUF];
        snprintf(full, sizeof(full), "%s/%s", root, paths[i]);
        char *xml = read_text_file_cap(full);
        if (!xml) {
            continue;
        }

        const char *p = xml;
        while ((p = strstr(p, "<dependency")) != NULL && count < MCR_MAX_EDGES) {
            if (ptr_in_xml_comment(xml, p)) {
                p += strlen("<dependency");
                continue;
            }
            if (ptr_in_xml_block_named(xml, "dependencyManagement", p)) {
                p += strlen("<dependency");
                continue;
            }
            const char *open_end = strchr(p, '>');
            if (!open_end) {
                break;
            }
            const char *end = strstr(open_end + SKIP_ONE, "</dependency>");
            if (!end) {
                break;
            }

            char group[CBM_SZ_128] = {0};
            char artifact[CBM_SZ_128] = {0};
            char version[CBM_SZ_128] = {0};
            char scope[CBM_SZ_64] = {0};
            xml_tag_text(open_end + SKIP_ONE, end, "groupId", group, sizeof(group));
            xml_tag_text(open_end + SKIP_ONE, end, "artifactId", artifact, sizeof(artifact));
            xml_tag_text(open_end + SKIP_ONE, end, "version", version, sizeof(version));
            xml_tag_text(open_end + SKIP_ONE, end, "scope", scope, sizeof(scope));

            add_dependency(&items, &count, &cap, group, artifact, version, scope, "dependency",
                           group, artifact, paths[i]);

            const char *ex = open_end + SKIP_ONE;
            while ((ex = strstr(ex, "<exclusion")) != NULL && ex < end && count < MCR_MAX_EDGES) {
                const char *ex_open_end = strchr(ex, '>');
                if (!ex_open_end || ex_open_end >= end) {
                    break;
                }
                const char *ex_end = strstr(ex_open_end + SKIP_ONE, "</exclusion>");
                if (!ex_end || ex_end > end) {
                    break;
                }
                char ex_group[CBM_SZ_128] = {0};
                char ex_artifact[CBM_SZ_128] = {0};
                xml_tag_text(ex_open_end + SKIP_ONE, ex_end, "groupId", ex_group,
                             sizeof(ex_group));
                xml_tag_text(ex_open_end + SKIP_ONE, ex_end, "artifactId", ex_artifact,
                             sizeof(ex_artifact));
                add_dependency(&items, &count, &cap, ex_group, ex_artifact, version, scope,
                               "exclusion", group, artifact, paths[i]);
                ex = ex_end + strlen("</exclusion>");
            }
            p = end + strlen("</dependency>");
        }
        free(xml);
    }

    free_string_list(paths, path_count);
    free(root);
    *out = items;
    return count;
}

static int64_t project_node_id(cbm_store_t *store, const char *project) {
    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db) {
        return 0;
    }
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
                           "SELECT id FROM nodes WHERE project=?1 AND label='Project' LIMIT 1",
                           CBM_NOT_FOUND, &st, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(st, SKIP_ONE, project, CBM_NOT_FOUND, SQLITE_STATIC);
    int64_t id = 0;
    if (sqlite3_step(st) == SQLITE_ROW) {
        id = sqlite3_column_int64(st, 0);
    }
    sqlite3_finalize(st);
    if (id != 0) {
        return id;
    }

    char qn[MCR_QN_BUF];
    snprintf(qn, sizeof(qn), "__project__%s", project);
    cbm_node_t node = {
        .project = project,
        .label = "Project",
        .name = project,
        .qualified_name = qn,
        .file_path = "",
        .start_line = 0,
        .end_line = 0,
        .properties_json = "{}",
    };
    return cbm_store_upsert_node(store, &node);
}

static void insert_cross_edge(cbm_store_t *store, const char *project, int64_t from_id,
                              int64_t to_id, const char *edge_type, const char *props) {
    cbm_edge_t edge = {
        .project = project,
        .source_id = from_id,
        .target_id = to_id,
        .type = edge_type,
        .properties_json = props,
    };
    cbm_store_insert_edge(store, &edge);
}

static void cleanup_reverse_library_edges(cbm_store_t *store, const char *project,
                                          const char *source_project) {
    if (!store || !project || !source_project) {
        return;
    }
    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db) {
        return;
    }
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
                           "DELETE FROM edges WHERE project=?1 AND type='CROSS_LIBRARY_USED_BY' "
                           "AND target_id IN (SELECT id FROM nodes WHERE project=?1 AND "
                           "label='LibraryConsumer' AND name=?2)",
                           CBM_NOT_FOUND, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, SKIP_ONE, project, CBM_NOT_FOUND, SQLITE_STATIC);
        sqlite3_bind_text(st, PAIR_LEN, source_project, CBM_NOT_FOUND, SQLITE_STATIC);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }
    st = NULL;
    if (sqlite3_prepare_v2(db,
                           "DELETE FROM nodes WHERE project=?1 AND label='LibraryConsumer' "
                           "AND name=?2 AND qualified_name GLOB '__library_consumer__*'",
                           CBM_NOT_FOUND, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, SKIP_ONE, project, CBM_NOT_FOUND, SQLITE_STATIC);
        sqlite3_bind_text(st, PAIR_LEN, source_project, CBM_NOT_FOUND, SQLITE_STATIC);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }
}

static void build_library_props(char *buf, size_t bufsz, const char *other_project,
                                const mcr_dependency_t *dep) {
    if (!buf || bufsz == 0) {
        return;
    }
    snprintf(buf, bufsz, "{}");
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "target_project", other_project ? other_project : "");
    yyjson_mut_obj_add_strcpy(doc, root, "group_id", dep->group_id);
    yyjson_mut_obj_add_strcpy(doc, root, "artifact_id", dep->artifact_id);
    yyjson_mut_obj_add_strcpy(doc, root, "version", dep->version);
    yyjson_mut_obj_add_strcpy(doc, root, "scope", dep->scope);
    yyjson_mut_obj_add_strcpy(doc, root, "relation", dep->relation);
    yyjson_mut_obj_add_strcpy(doc, root, "context_group_id", dep->context_group_id);
    yyjson_mut_obj_add_strcpy(doc, root, "context_artifact_id", dep->context_artifact_id);
    yyjson_mut_obj_add_strcpy(doc, root, "source_pom", dep->pom_path);

    size_t len = 0;
    char *json = yyjson_mut_write(doc, 0, &len);
    if (json && len < bufsz) {
        memcpy(buf, json, len + SKIP_ONE);
    }
    free(json);
    yyjson_mut_doc_free(doc);
}

static char *format_library_qn(const char *prefix, const char *project, const mcr_dependency_t *dep) {
    if (!prefix || !project || !dep) {
        return NULL;
    }
    int needed =
        snprintf(NULL, 0, "%s%s__%s__%s:%s__via__%s:%s__%s", prefix, project, dep->relation,
                 dep->group_id, dep->artifact_id, dep->context_group_id, dep->context_artifact_id,
                 dep->pom_path);
    if (needed < 0) {
        return NULL;
    }
    char *qn = malloc((size_t)needed + SKIP_ONE);
    if (!qn) {
        return NULL;
    }
    snprintf(qn, (size_t)needed + SKIP_ONE, "%s%s__%s__%s:%s__via__%s:%s__%s", prefix, project,
             dep->relation, dep->group_id, dep->artifact_id, dep->context_group_id,
             dep->context_artifact_id, dep->pom_path);
    return qn;
}

static int emit_library_match(cbm_store_t *src_store, const char *src_project,
                              cbm_store_t *tgt_store, const char *tgt_project,
                              const mcr_dependency_t *dep) {
    int64_t src_project_id = project_node_id(src_store, src_project);
    int64_t tgt_project_id = project_node_id(tgt_store, tgt_project);
    if (src_project_id <= 0 || tgt_project_id <= 0) {
        return 0;
    }

    char *lib_qn = format_library_qn("__library__", tgt_project, dep);
    if (!lib_qn) {
        return 0;
    }
    char *consumer_qn = format_library_qn("__library_consumer__", src_project, dep);
    if (!consumer_qn) {
        free(lib_qn);
        return 0;
    }

    char props[MCR_PROPS_BUF];
    build_library_props(props, sizeof(props), tgt_project, dep);
    cbm_node_t lib = {
        .project = src_project,
        .label = "Library",
        .name = dep->artifact_id,
        .qualified_name = lib_qn,
        .file_path = dep->pom_path,
        .start_line = 0,
        .end_line = 0,
        .properties_json = props,
    };
    int64_t lib_id = cbm_store_upsert_node(src_store, &lib);
    if (lib_id <= 0) {
        free(consumer_qn);
        free(lib_qn);
        return 0;
    }

    char reverse_props[MCR_PROPS_BUF];
    build_library_props(reverse_props, sizeof(reverse_props), src_project, dep);
    cbm_node_t consumer = {
        .project = tgt_project,
        .label = "LibraryConsumer",
        .name = src_project,
        .qualified_name = consumer_qn,
        .file_path = "",
        .start_line = 0,
        .end_line = 0,
        .properties_json = reverse_props,
    };
    int64_t consumer_id = cbm_store_upsert_node(tgt_store, &consumer);
    if (consumer_id <= 0) {
        free(consumer_qn);
        free(lib_qn);
        return 0;
    }
    insert_cross_edge(src_store, src_project, src_project_id, lib_id, "CROSS_LIBRARY_DEPENDS_ON",
                      props);
    insert_cross_edge(tgt_store, tgt_project, tgt_project_id, consumer_id, "CROSS_LIBRARY_USED_BY",
                      reverse_props);
    free(consumer_qn);
    free(lib_qn);
    return 1;
}

static bool artifact_matches_dep(const mcr_artifact_t *artifact, const mcr_dependency_t *dep) {
    return strcmp(artifact->group_id, dep->group_id) == 0 &&
           strcmp(artifact->artifact_id, dep->artifact_id) == 0;
}

int cbm_cross_repo_match_maven_libraries(cbm_store_t *src_store, const char *src_project,
                                         cbm_store_t *tgt_store, const char *tgt_project) {
    mcr_dependency_t *deps = NULL;
    mcr_artifact_t *artifacts = NULL;
    cleanup_reverse_library_edges(tgt_store, tgt_project, src_project);
    int dep_count = collect_dependencies(src_store, src_project, &deps);
    int artifact_count = collect_artifacts(tgt_store, tgt_project, &artifacts);
    if (dep_count == 0 || artifact_count == 0) {
        free(deps);
        free(artifacts);
        return 0;
    }

    int count = 0;
    for (int d = 0; d < dep_count && count < MCR_MAX_EDGES; d++) {
        for (int a = 0; a < artifact_count; a++) {
            if (!artifact_matches_dep(&artifacts[a], &deps[d])) {
                continue;
            }
            count += emit_library_match(src_store, src_project, tgt_store, tgt_project, &deps[d]);
            break;
        }
    }

    free(deps);
    free(artifacts);
    return count;
}
