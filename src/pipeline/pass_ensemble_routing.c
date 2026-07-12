#include "pipeline/pass_ensemble_routing.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/str_util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>

#define CONF_LITERAL 0.95
#define CONF_PROP 0.85

#define MAX_ITEMS 256
#define MAX_SETTINGS 8

/* ── Language gate ───────────────────────────────────────────────── */

/* Visitor state for ObjectScript detection. */
typedef struct {
    bool found;
} ens_lang_check_t;

static void check_objectscript_node(const cbm_gbuf_node_t *node, void *userdata) {
    ens_lang_check_t *s = (ens_lang_check_t *)userdata;
    if (s->found || !node->file_path)
        return;
    const char *fp = node->file_path;
    size_t len = strlen(fp);
    if (len >= 4 && (strcmp(fp + len - 4, ".cls") == 0 || strcmp(fp + len - 4, ".mac") == 0 ||
                     strcmp(fp + len - 4, ".int") == 0)) {
        s->found = true;
    }
}

/* Return true when the graph buffer contains at least one node from an
 * ObjectScript source file (.cls / .mac / .int). */
static bool has_objectscript_nodes(cbm_gbuf_t *gbuf) {
    ens_lang_check_t state = {false};
    cbm_gbuf_foreach_node(gbuf, check_objectscript_node, &state);
    return state.found;
}

static const char *TOPOLOGY_SETTINGS[] = {"TargetConfigName", "PatientHost", "ConformanceOperation",
                                          NULL};

static const char *ENTRY_POINTS[] = {"OnProcessInput", "OnMessage", "OnRequest", "OnTask", NULL};

typedef struct {
    char setting_name[CBM_SZ_256];
    char value[CBM_SZ_256];
} ens_setting_t;

typedef struct {
    char item_name[CBM_SZ_256];
    char class_name[CBM_SZ_256];
    bool enabled;
    ens_setting_t settings[MAX_SETTINGS];
    int n_settings;
} ens_item_t;

typedef struct {
    char production_class[CBM_SZ_256];
    char file_path[CBM_SZ_512];
    ens_item_t items[MAX_ITEMS];
    int n_items;
} ens_prod_def_t;

static void extract_xml_attr(const char *xml, int offset, const char *attr, char *out, int outsz) {
    char needle[CBM_SZ_64];
    snprintf(needle, sizeof(needle), "%s=\"", attr);
    const char *p = strstr(xml + offset, needle);
    out[0] = '\0';
    if (!p)
        return;
    p += strlen(needle);
    const char *e = strchr(p, '"');
    if (!e)
        return;
    int len = (int)(e - p);
    if (len >= outsz)
        len = outsz - 1;
    memcpy(out, p, (size_t)len);
    out[len] = '\0';
}

static bool is_topology_setting(const char *name) {
    for (int i = 0; TOPOLOGY_SETTINGS[i]; i++)
        if (strcmp(name, TOPOLOGY_SETTINGS[i]) == 0)
            return true;
    return false;
}

static ens_prod_def_t *parse_production_xml(const char *xml, const char *class_qn,
                                            const char *file_path) {
    ens_prod_def_t *def = calloc(1, sizeof(ens_prod_def_t));
    if (!def)
        return NULL;
    snprintf(def->production_class, CBM_SZ_256, "%s", class_qn);
    snprintf(def->file_path, sizeof(def->file_path), "%s", file_path ? file_path : "");

    const char *p = xml;
    while (*p && def->n_items < MAX_ITEMS) {
        const char *item_start = strstr(p, "<Item ");
        if (!item_start)
            break;

        ens_item_t *item = &def->items[def->n_items];
        memset(item, 0, sizeof(*item));
        item->enabled = true;

        int off = (int)(item_start - xml);
        extract_xml_attr(xml, off, "Name", item->item_name, CBM_SZ_256);
        extract_xml_attr(xml, off, "ClassName", item->class_name, CBM_SZ_256);
        char en[16];
        extract_xml_attr(xml, off, "Enabled", en, sizeof(en));
        if (en[0] && strcasecmp(en, "false") == 0)
            item->enabled = false;

        if (!item->item_name[0] || !item->class_name[0]) {
            p = item_start + 6;
            continue;
        }

        const char *item_end = strstr(item_start, "</Item>");
        if (!item_end)
            item_end = item_start + strlen(item_start);

        const char *sp = item_start;
        while (sp < item_end && item->n_settings < MAX_SETTINGS) {
            const char *set = strstr(sp, "<Setting ");
            if (!set || set >= item_end)
                break;
            int soff = (int)(set - xml);
            char tgt[64], sname[CBM_SZ_256];
            extract_xml_attr(xml, soff, "Target", tgt, sizeof(tgt));
            extract_xml_attr(xml, soff, "Name", sname, CBM_SZ_256);
            if (strcmp(tgt, "Host") == 0 && is_topology_setting(sname)) {
                const char *vs = strchr(set + 9, '>');
                if (vs) {
                    vs++;
                    const char *ve = strstr(vs, "</Setting>");
                    if (ve && ve < item_end) {
                        int vlen = (int)(ve - vs);
                        if (vlen > 0 && vlen < CBM_SZ_256) {
                            ens_setting_t *s = &item->settings[item->n_settings++];
                            snprintf(s->setting_name, CBM_SZ_256, "%s", sname);
                            memcpy(s->value, vs, (size_t)vlen);
                            s->value[vlen] = '\0';
                        }
                    }
                }
            }
            sp = set + 9;
        }
        /* Warn when topology settings were truncated at the cap. */
        if (item->n_settings == MAX_SETTINGS && strstr(sp, "<Setting ") != NULL &&
            strstr(sp, "<Setting ") < item_end) {
            cbm_log_warn("ensemble_routing.parse", "item", item->item_name, "warn",
                         "settings truncated at MAX_SETTINGS cap");
        }
        def->n_items++;
        p = item_end + 7;
    }
    /* Warn when items were truncated at the cap. */
    if (def->n_items == MAX_ITEMS && strstr(p, "<Item ") != NULL) {
        cbm_log_warn("ensemble_routing.parse", "production", def->production_class, "warn",
                     "items truncated at MAX_ITEMS cap");
    }
    return def;
}

static char *read_file(const char *full_path) {
    FILE *f = fopen(full_path, "rb");
    if (!f)
        return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz <= 0 || sz > 8 * 1024 * 1024) {
        fclose(f);
        return NULL;
    }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    fread(buf, 1, (size_t)sz, f);
    buf[sz] = '\0';
    fclose(f);
    return buf;
}

static const char *jstr(const char *json, const char *key, char *buf, int sz) {
    if (!json || !key)
        return NULL;
    char needle[CBM_SZ_64];
    snprintf(needle, sizeof(needle), "\"%s\":\"", key);
    const char *s = strstr(json, needle);
    if (!s)
        return NULL;
    s += strlen(needle);
    const char *e = strchr(s, '"');
    if (!e)
        return NULL;
    int len = (int)(e - s);
    if (len >= sz)
        len = sz - 1;
    memcpy(buf, s, (size_t)len);
    buf[len] = '\0';
    return buf;
}

static const ens_item_t *find_item(const ens_prod_def_t *def, const char *name) {
    for (int i = 0; i < def->n_items; i++)
        if (strcmp(def->items[i].item_name, name) == 0)
            return &def->items[i];
    return NULL;
}

static int64_t find_entry_point(cbm_pipeline_ctx_t *ctx, const char *class_name) {
    for (int ei = 0; ENTRY_POINTS[ei]; ei++) {
        char suffix[CBM_SZ_512];
        snprintf(suffix, sizeof(suffix), "%s.%s", class_name, ENTRY_POINTS[ei]);

        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        cbm_gbuf_find_by_name(ctx->gbuf, ENTRY_POINTS[ei], (const cbm_gbuf_node_t ***)&nodes,
                              &count);
        for (int ni = 0; ni < count; ni++) {
            if (nodes[ni]->qualified_name && strstr(nodes[ni]->qualified_name, suffix))
                return nodes[ni]->id;
        }
    }
    return 0;
}

static void emit_route(cbm_pipeline_ctx_t *ctx, int64_t src_id, const ens_item_t *item,
                       const char *via, double confidence, const char *production_class) {
    int64_t tgt_id = find_entry_point(ctx, item->class_name);
    if (!tgt_id) {
        char cls_qn[CBM_SZ_512];
        snprintf(cls_qn, sizeof(cls_qn), "%s.%s", production_class, item->item_name);
        const cbm_gbuf_node_t *cls = cbm_gbuf_find_by_qn(ctx->gbuf, cls_qn);
        if (!cls)
            return;
        tgt_id = cls->id;
        confidence -= 0.10;
    }
    char conf_str[32];
    snprintf(conf_str, sizeof(conf_str), "%.2f", confidence);
    char props[CBM_SZ_512];
    snprintf(props, sizeof(props),
             "{\"via\":\"%s\",\"production\":\"%s\",\"item_name\":\"%s\","
             "\"confidence\":%s,\"enabled\":%s}",
             via, production_class, item->item_name, conf_str, item->enabled ? "true" : "false");
    cbm_gbuf_insert_edge(ctx->gbuf, src_id, tgt_id, "ROUTES_TO", props);
}

/* Scan a .cls source file for SendRequestSync call targets and
 * InitialExpression values for a given method/property name. */
static void scan_source_for_send_targets(const char *source, const char *method_name,
                                         char *literal_out, int lit_sz, char *prop_name_out,
                                         int prop_sz) {
    literal_out[0] = '\0';
    prop_name_out[0] = '\0';
    if (!source || !method_name)
        return;

    const char *p = source;
    while ((p = strstr(p, "SendRequestSync")) != NULL) {
        p += 15;
        while (*p == ' ' || *p == '\t')
            p++;
        if (*p != '(')
            continue;
        p++;
        while (*p == ' ' || *p == '\t')
            p++;

        if (*p == '"') {
            const char *ns = p + 1, *ne = strchr(ns, '"');
            if (ne) {
                int len = (int)(ne - ns);
                if (len > 0 && len < lit_sz) {
                    memcpy(literal_out, ns, (size_t)len);
                    literal_out[len] = '\0';
                    return;
                }
            }
        } else if (p[0] == '.' && p[1] == '.') {
            const char *ps = p + 2;
            int plen = 0;
            while (ps[plen] && (isalnum((unsigned char)ps[plen]) || ps[plen] == '_'))
                plen++;
            if (plen > 0 && plen < prop_sz) {
                memcpy(prop_name_out, ps, (size_t)plen);
                prop_name_out[plen] = '\0';
                return;
            }
        }
    }
    (void)method_name;
}

/* Find InitialExpression value for a Property in the source. */
static void scan_initial_expression(const char *source, const char *prop_name, char *out,
                                    int outsz) {
    out[0] = '\0';
    if (!source || !prop_name)
        return;
    char needle[CBM_SZ_256];
    snprintf(needle, sizeof(needle), "Property %s ", prop_name);
    const char *p = strstr(source, needle);
    if (!p) {
        snprintf(needle, sizeof(needle), "Property %s[", prop_name);
        p = strstr(source, needle);
    }
    if (!p)
        return;
    const char *ie = strstr(p, "InitialExpression =");
    if (!ie)
        return;
    ie = strchr(ie, '"');
    if (!ie)
        return;
    ie++;
    const char *ie_end = strchr(ie, '"');
    if (!ie_end)
        return;
    int len = (int)(ie_end - ie);
    if (len >= outsz)
        len = outsz - 1;
    memcpy(out, ie, (size_t)len);
    out[len] = '\0';
}

static void collect_prod_defs(cbm_pipeline_ctx_t *ctx, ens_prod_def_t ***defs_out, int *count_out) {
    const cbm_gbuf_node_t **xdata_nodes = NULL;
    int xdata_count = 0;
    cbm_gbuf_find_by_label(ctx->gbuf, "XData", (const cbm_gbuf_node_t ***)&xdata_nodes,
                           &xdata_count);

    ens_prod_def_t **defs = NULL;
    int n = 0;

    for (int xi = 0; xi < xdata_count; xi++) {
        const cbm_gbuf_node_t *xd = xdata_nodes[xi];
        if (!xd->name || strcmp(xd->name, "ProductionDefinition") != 0)
            continue;
        if (!xd->file_path || !ctx->repo_path)
            continue;

        char full_path[CBM_SZ_1K];
        snprintf(full_path, sizeof(full_path), "%s/%s", ctx->repo_path, xd->file_path);

        char *source = read_file(full_path);
        if (!source)
            continue;

        char class_qn[CBM_SZ_256];
        class_qn[0] = '\0';
        if (xd->qualified_name) {
            const char *dot = strrchr(xd->qualified_name, '.');
            if (dot) {
                int len = (int)(dot - xd->qualified_name);
                if (len > 0 && len < CBM_SZ_256) {
                    memcpy(class_qn, xd->qualified_name, (size_t)len);
                    class_qn[len] = '\0';
                }
            }
        }
        if (!class_qn[0]) {
            free(source);
            continue;
        }

        const char *xml_start = strstr(source, "<Production ");
        if (!xml_start)
            xml_start = strstr(source, "<Production\n");
        if (!xml_start) {
            free(source);
            continue;
        }

        ens_prod_def_t *def = parse_production_xml(xml_start, class_qn, xd->file_path);
        free(source);
        if (!def)
            continue;

        char n_items_buf[32];
        snprintf(n_items_buf, sizeof(n_items_buf), "%d", def->n_items);
        cbm_log_info("ensemble_routing.parse", "class", class_qn, "items", n_items_buf);

        for (int i = 0; i < def->n_items; i++) {
            ens_item_t *item = &def->items[i];
            char item_qn[CBM_SZ_512];
            snprintf(item_qn, sizeof(item_qn), "%s.%s", class_qn, item->item_name);
            char iprops[CBM_SZ_512];
            snprintf(iprops, sizeof(iprops),
                     "{\"class_name\":\"%s\",\"enabled\":%s,\"production\":\"%s\"}",
                     item->class_name, item->enabled ? "true" : "false", class_qn);
            cbm_gbuf_upsert_node(ctx->gbuf, "EnsembleItem", item->item_name, item_qn, xd->file_path,
                                 xd->start_line, 0, iprops);
        }

        ens_prod_def_t **tmp = realloc(defs, (size_t)(n + 1) * sizeof(ens_prod_def_t *));
        if (!tmp) {
            free(def);
            continue;
        }
        defs = tmp;
        defs[n++] = def;
    }
    *defs_out = defs;
    *count_out = n;
}

static bool method_belongs_to_production(const cbm_gbuf_node_t *method, const ens_prod_def_t *def) {
    if (!method->properties_json)
        return false;
    char parent_class[CBM_SZ_512];
    if (!jstr(method->properties_json, "parent_class", parent_class, sizeof(parent_class)))
        return false;
    for (int i = 0; i < def->n_items; i++) {
        if (strstr(parent_class, def->items[i].class_name))
            return true;
    }
    return false;
}

static void resolve_method_routes(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *method,
                                  const char *source, const ens_prod_def_t *def) {
    if (!method->properties_json)
        return;
    if (!method_belongs_to_production(method, def))
        return;
    if (!strstr(source, "SendRequestSync"))
        return;

    char literal[CBM_SZ_256], prop_name[CBM_SZ_256];
    scan_source_for_send_targets(source, method->name, literal, sizeof(literal), prop_name,
                                 sizeof(prop_name));

    if (literal[0]) {
        const ens_item_t *item = find_item(def, literal);
        if (item)
            emit_route(ctx, method->id, item, "literal", CONF_LITERAL, def->production_class);
    } else if (prop_name[0]) {
        char init_expr[CBM_SZ_256];
        scan_initial_expression(source, prop_name, init_expr, sizeof(init_expr));
        if (init_expr[0]) {
            const ens_item_t *item = find_item(def, init_expr);
            if (item)
                emit_route(ctx, method->id, item, prop_name, CONF_PROP, def->production_class);
        }
    }
}

void cbm_pipeline_pass_ensemble_routing(cbm_pipeline_ctx_t *ctx) {
    if (!ctx || !ctx->gbuf || !ctx->repo_path)
        return;

    /* Early-exit: skip entirely when no ObjectScript source files are present
     * in the graph buffer. Avoids a full XData scan on non-IRIS projects. */
    if (!has_objectscript_nodes(ctx->gbuf))
        return;

    ens_prod_def_t **defs = NULL;
    int n_defs = 0;
    collect_prod_defs(ctx, &defs, &n_defs);
    if (n_defs == 0)
        return;

    const cbm_gbuf_node_t **method_nodes = NULL;
    int method_count = 0;
    cbm_gbuf_find_by_label(ctx->gbuf, "Method", (const cbm_gbuf_node_t ***)&method_nodes,
                           &method_count);

    int before = cbm_gbuf_edge_count_by_type(ctx->gbuf, "ROUTES_TO");

    for (int di = 0; di < n_defs; di++) {
        ens_prod_def_t *def = defs[di];

        for (int mi = 0; mi < method_count; mi++) {
            const cbm_gbuf_node_t *m = method_nodes[mi];
            if (!m->properties_json || !m->file_path)
                continue;
            if (!method_belongs_to_production(m, def))
                continue;

            char meth_full_path[CBM_SZ_1K];
            snprintf(meth_full_path, sizeof(meth_full_path), "%s/%s", ctx->repo_path, m->file_path);
            char *meth_source = read_file(meth_full_path);
            if (!meth_source)
                continue;
            resolve_method_routes(ctx, m, meth_source, def);
            free(meth_source);
        }

        for (int ii = 0; ii < def->n_items; ii++) {
            const ens_item_t *item = &def->items[ii];
            for (int si = 0; si < item->n_settings; si++) {
                const ens_setting_t *setting = &item->settings[si];
                if (!setting->value[0])
                    continue;
                const ens_item_t *target = find_item(def, setting->value);
                if (!target)
                    continue;
                char item_qn[CBM_SZ_512];
                snprintf(item_qn, sizeof(item_qn), "%s.%s", def->production_class, item->item_name);
                const cbm_gbuf_node_t *item_node = cbm_gbuf_find_by_qn(ctx->gbuf, item_qn);
                if (!item_node)
                    continue;
                emit_route(ctx, item_node->id, target, setting->setting_name, CONF_PROP,
                           def->production_class);
            }
        }

        free(defs[di]);
    }
    free(defs);

    int routes = cbm_gbuf_edge_count_by_type(ctx->gbuf, "ROUTES_TO") - before;
    char n_defs_buf[32], n_routes_buf[32];
    snprintf(n_defs_buf, sizeof(n_defs_buf), "%d", n_defs);
    snprintf(n_routes_buf, sizeof(n_routes_buf), "%d", routes);
    cbm_log_info("ensemble_routing.done", "productions", n_defs_buf, "routes", n_routes_buf);
}
