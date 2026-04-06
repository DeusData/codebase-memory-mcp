/*
 * pass_configlink.c — Config ↔ Code linking strategies (pre-dump pass).
 *
 * Three strategies link config files to code symbols:
 *   1. Key→Symbol: normalized config key matches code function/variable name
 *   2. Dep→Import: package manifest dependency matches IMPORTS edge target
 *   3. File→Ref: source code string literal references config file path
 *
 * Operates on the graph buffer before dump to .db file.
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/compat.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "foundation/compat_regex.h"

/* ── Config link confidence scores ───────────────────────────────── */
/* Strategy 1: Key→Symbol matching */
#define CONF_KEY_EXACT 0.85
#define CONF_KEY_SUBSTRING 0.75
/* Strategy 2: Dep→Import matching */
#define CONF_DEP_EXACT 0.95
#define CONF_DEP_QN_SUBSTR 0.80
/* Strategy 3: File→Ref matching */
#define CONF_FILE_FULLPATH 0.90
#define CONF_FILE_BASENAME 0.70

/* ── Manifest / dep section tables ──────────────────────────────── */

static bool is_manifest_file(const char *basename) {
    static const char *names[] = {"Cargo.toml",       "package.json",  "go.mod",
                                  "requirements.txt", "Gemfile",       "build.gradle",
                                  "pom.xml",          "composer.json", NULL};
    for (int i = 0; names[i]; i++) {
        if (strcmp(basename, names[i]) == 0) {
            return true;
        }
    }
    return false;
}

static bool is_dep_section(const char *s) {
    static const char *secs[] = {"dependencies",     "devdependencies",    "peerdependencies",
                                 "dev-dependencies", "build-dependencies", NULL};
    for (int i = 0; secs[i]; i++) {
        if (cbm_strcasestr(s, secs[i]) != NULL) {
            return true;
        }
    }
    return false;
}

/* ── Strategy 1: Config Key → Code Symbol ───────────────────────── */

typedef struct {
    int64_t node_id;
    char normalized[CBM_SZ_256];
    char name[CBM_SZ_256];
} config_entry_t;

/* Collect config Variable nodes with ≥2 tokens, each ≥3 chars. */
static int collect_config_entries(const cbm_gbuf_node_t *const *vars, int var_count,
                                  config_entry_t *out, int max_out) {
    int n = 0;
    for (int i = 0; i < var_count && n < max_out; i++) {
        if (!cbm_has_config_extension(vars[i]->file_path)) {
            continue;
        }

        char norm[CBM_SZ_256];
        int tokens = cbm_normalize_config_key(vars[i]->name, norm, sizeof(norm));
        if (tokens < PAIR_LEN) {
            continue;
        }

        /* Check all tokens ≥3 chars */
        bool all_long = true;
        const char *p = norm;
        while (*p) {
            const char *end = strchr(p, '_');
            size_t tlen = end ? (size_t)(end - p) : strlen(p);
            if (tlen < CBM_SZ_3) {
                all_long = false;
                break;
            }
            p = end ? end + SKIP_ONE : p + tlen;
        }
        if (!all_long) {
            continue;
        }

        out[n].node_id = vars[i]->id;
        snprintf(out[n].normalized, sizeof(out[n].normalized), "%s", norm);
        snprintf(out[n].name, sizeof(out[n].name), "%s", vars[i]->name);
        n++;
    }
    return n;
}

/* Collect code nodes (Function/Variable/Class) not from config files. */
typedef struct {
    int64_t node_id;
    char normalized[CBM_SZ_256];
} code_entry_t;

static int collect_code_entries(cbm_gbuf_t *gb, code_entry_t *out, int max_out) {
    int n = 0;
    static const char *labels[] = {"Function", "Variable", "Class", NULL};

    for (int li = 0; labels[li] && n < max_out; li++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(gb, labels[li], &nodes, &count) != 0) {
            continue;
        }

        for (int i = 0; i < count && n < max_out; i++) {
            if (cbm_has_config_extension(nodes[i]->file_path)) {
                continue;
            }

            char norm[CBM_SZ_256];
            int tokens = cbm_normalize_config_key(nodes[i]->name, norm, sizeof(norm));
            if (tokens == 0 || norm[0] == '\0') {
                continue;
            }

            out[n].node_id = nodes[i]->id;
            snprintf(out[n].normalized, sizeof(out[n].normalized), "%s", norm);
            n++;
        }
        /* gbuf data is borrowed — no free */
    }
    return n;
}

static int strategy_key_symbols(cbm_gbuf_t *gb) {
    /* Get all Variable nodes */
    const cbm_gbuf_node_t **vars = NULL;
    int var_count = 0;
    if (cbm_gbuf_find_by_label(gb, "Variable", &vars, &var_count) != 0) {
        return 0;
    }

    config_entry_t config_entries[CBM_SZ_4K];
    int config_count = collect_config_entries(vars, var_count, config_entries, CBM_SZ_4K);

    if (config_count == 0) {
        return 0;
    }

    code_entry_t code_entries[CBM_SZ_8K];
    int code_count = collect_code_entries(gb, code_entries, CBM_SZ_8K);

    int edge_count = 0;

    for (int ci = 0; ci < config_count; ci++) {
        for (int co = 0; co < code_count; co++) {
            double confidence = 0.0;

            if (strcmp(config_entries[ci].normalized, code_entries[co].normalized) == 0) {
                /* Exact match */
                confidence = CONF_KEY_EXACT;
            } else if (strstr(code_entries[co].normalized, config_entries[ci].normalized) != NULL) {
                /* Substring match */
                confidence = CONF_KEY_SUBSTRING;
            }

            if (confidence > 0.0) {
                char props[CBM_SZ_512];
                snprintf(props, sizeof(props),
                         "{\"strategy\":\"key_symbol\",\"confidence\":%.2f,\"config_key\":\"%s\"}",
                         confidence, config_entries[ci].name);

                cbm_gbuf_insert_edge(gb, code_entries[co].node_id, config_entries[ci].node_id,
                                     "CONFIGURES", props);
                edge_count++;
            }
        }
    }

    return edge_count;
}

/* ── Strategy 2: Dependency → Import ────────────────────────────── */

typedef struct {
    int64_t node_id;
    char name[CBM_SZ_256];
} dep_entry_t;

/* Extract basename from a file path. */
static const char *path_basename(const char *path) {
    if (!path) {
        return "";
    }
    const char *slash = strrchr(path, '/');
    return slash ? slash + SKIP_ONE : path;
}

/* Check if a Cargo.toml QN contains a dependency section in any dotted part. */
static bool is_cargo_dep_section(const char *qn) {
    char qn_copy[CBM_SZ_512];
    snprintf(qn_copy, sizeof(qn_copy), "%s", qn);
    char *saveptr = NULL;
    char *part = strtok_r(qn_copy, ".", &saveptr);
    while (part) {
        char lower[CBM_SZ_128];
        size_t plen = strlen(part);
        if (plen >= sizeof(lower)) {
            plen = sizeof(lower) - SKIP_ONE;
        }
        for (size_t j = 0; j < plen; j++) {
            lower[j] = (char)tolower((unsigned char)part[j]);
        }
        lower[plen] = '\0';

        static const char *dep_secs[] = {"dependencies",       "devdependencies",
                                         "peerdependencies",   "dev-dependencies",
                                         "build-dependencies", NULL};
        for (int k = 0; dep_secs[k]; k++) {
            if (strcmp(lower, dep_secs[k]) == 0) {
                return true;
            }
        }
        part = strtok_r(NULL, ".", &saveptr);
    }
    return false;
}

static int collect_manifest_deps(const cbm_gbuf_node_t *const *vars, int var_count,
                                 dep_entry_t *out, int max_out) {
    int n = 0;
    for (int i = 0; i < var_count && n < max_out; i++) {
        const char *base = path_basename(vars[i]->file_path);
        if (!is_manifest_file(base)) {
            continue;
        }

        bool is_dep = vars[i]->qualified_name && is_dep_section(vars[i]->qualified_name);

        if (!is_dep && strcmp(base, "Cargo.toml") == 0 && vars[i]->qualified_name) {
            is_dep = is_cargo_dep_section(vars[i]->qualified_name);
        }

        if (is_dep) {
            out[n].node_id = vars[i]->id;
            snprintf(out[n].name, sizeof(out[n].name), "%s", vars[i]->name);
            n++;
        }
    }
    return n;
}

/* Lowercase a string into buf. */
static void lowercase_into(char *buf, size_t bufsize, const char *src) {
    size_t len = src ? strlen(src) : 0;
    for (size_t j = 0; j < len && j < bufsize - SKIP_ONE; j++) {
        buf[j] = (char)tolower((unsigned char)src[j]);
    }
    buf[len < bufsize ? len : bufsize - SKIP_ONE] = '\0';
}

/* Match a dep name (lowercased) against an import target node.
 * Returns confidence > 0 on match, 0 on no match. */
static double match_dep_to_import(const cbm_gbuf_node_t *target, const char *dep_lower) {
    char target_lower[CBM_SZ_256];
    lowercase_into(target_lower, sizeof(target_lower), target->name);

    if (strcmp(target_lower, dep_lower) == 0) {
        return CONF_DEP_EXACT;
    }
    if (target->qualified_name) {
        char qn_lower[CBM_SZ_512];
        lowercase_into(qn_lower, sizeof(qn_lower), target->qualified_name);
        if (strstr(qn_lower, dep_lower) != NULL) {
            return CONF_DEP_QN_SUBSTR;
        }
    }
    return 0.0;
}

static int strategy_dep_imports(cbm_gbuf_t *gb) {
    const cbm_gbuf_node_t **vars = NULL;
    int var_count = 0;
    if (cbm_gbuf_find_by_label(gb, "Variable", &vars, &var_count) != 0) {
        return 0;
    }

    dep_entry_t deps[CBM_SZ_2K];
    int dep_count = collect_manifest_deps(vars, var_count, deps, CBM_SZ_2K);

    if (dep_count == 0) {
        return 0;
    }

    /* Get all IMPORTS edges */
    const cbm_gbuf_edge_t **imports = NULL;
    int import_count = 0;
    if (cbm_gbuf_find_edges_by_type(gb, "IMPORTS", &imports, &import_count) != 0) {
        return 0;
    }

    int edge_count = 0;

    for (int di = 0; di < dep_count; di++) {
        char dep_lower[CBM_SZ_256];
        lowercase_into(dep_lower, sizeof(dep_lower), deps[di].name);

        for (int ii = 0; ii < import_count; ii++) {
            const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(gb, imports[ii]->target_id);
            if (!target) {
                continue;
            }

            const cbm_gbuf_node_t *source = cbm_gbuf_find_by_id(gb, imports[ii]->source_id);
            if (!source) {
                continue;
            }

            double confidence = match_dep_to_import(target, dep_lower);
            if (confidence > 0.0) {
                char props[CBM_SZ_512];
                snprintf(
                    props, sizeof(props),
                    "{\"strategy\":\"dependency_import\",\"confidence\":%.2f,\"dep_name\":\"%s\"}",
                    confidence, deps[di].name);

                cbm_gbuf_insert_edge(gb, source->id, deps[di].node_id, "CONFIGURES", props);
                edge_count++;
            }
        }
    }

    /* gbuf data is borrowed — no free */
    return edge_count;
}

/* ── Strategy 3: Config File Path → Code String Reference ───────── */

typedef struct {
    const char *key;
    int64_t node_id;
} path_map_t;

/* Match a ref_path against config module maps. Returns target node_id (0 = no match). */

int cbm_pipeline_pass_configlink(cbm_pipeline_ctx_t *ctx) {
    cbm_gbuf_t *gb = ctx->gbuf;
    /* Early exit: check if any config files exist in the project. */
    bool has_config = false;

    const cbm_gbuf_node_t **vars_check = NULL;
    int var_check_count = 0;
    if (!has_config && cbm_gbuf_find_by_label(gb, "Variable", &vars_check, &var_check_count) == 0) {
        for (int i = 0; i < var_check_count; i++) {
            if (cbm_has_config_extension(vars_check[i]->file_path)) {
                has_config = true;
                break;
            }
        }
    }

    if (!has_config) {
        const cbm_gbuf_node_t **mods_check = NULL;
        int mod_check_count = 0;
        if (cbm_gbuf_find_by_label(gb, "Module", &mods_check, &mod_check_count) == 0) {
            for (int i = 0; i < mod_check_count; i++) {
                if (cbm_has_config_extension(mods_check[i]->file_path)) {
                    has_config = true;
                    break;
                }
            }
        }
    }

    if (!has_config) {
        cbm_log_info("configlinker.skip", "reason", "no_config_files");
        return 0;
    }

    char buf1[CBM_SZ_16];
    char buf2[CBM_SZ_16];
    char buf3[CBM_SZ_16];
    char buf4[CBM_SZ_16];

    int key_edges = strategy_key_symbols(gb);
    snprintf(buf1, sizeof(buf1), "%d", key_edges);
    cbm_log_info("configlinker.strategy", "name", "key_symbol", "edges", buf1);

    int dep_edges = strategy_dep_imports(gb);
    snprintf(buf2, sizeof(buf2), "%d", dep_edges);
    cbm_log_info("configlinker.strategy", "name", "dep_import", "edges", buf2);

    int ref_edges = 0;
    if (ctx->repo_path) {
        /* File refs: no longer reads from disk — config file path matching
         * is handled by CONFIGURES edges created during resolution. */
        ref_edges = 0;
    }
    snprintf(buf3, sizeof(buf3), "%d", ref_edges);
    cbm_log_info("configlinker.strategy", "name", "file_ref", "edges", buf3);

    snprintf(buf4, sizeof(buf4), "%d", key_edges + dep_edges + ref_edges);
    cbm_log_info("configlinker.done", "total", buf4);

    return key_edges + dep_edges + ref_edges;
}
