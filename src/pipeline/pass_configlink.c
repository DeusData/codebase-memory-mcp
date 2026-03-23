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

/* Use the shared manifest file list from depindex.h for DRY.
 * Adding new manifest files to CBM_MANIFEST_FILES covers both
 * dep discovery and config linking automatically. */
#include "depindex/depindex.h"

static bool is_manifest_file(const char *basename) {
    for (int i = 0; CBM_MANIFEST_FILES[i]; i++) {
        if (strcmp(basename, CBM_MANIFEST_FILES[i]) == 0) {
            return true;
        }
    }
    return false;
}

static bool is_dep_section(const char *s) {
    static const char *secs[] = {"dependencies",     "devdependencies",    "peerdependencies",
                                 "dev-dependencies", "build-dependencies", NULL};
    for (int i = 0; secs[i]; i++) {
        // NOLINTNEXTLINE(misc-include-cleaner) — strcasestr provided by standard header
        if (cbm_strcasestr(s, secs[i]) != NULL) {
            return true;
        }
    }
    return false;
}

/* ── Strategy 1: Config Key → Code Symbol ───────────────────────── */

typedef struct {
    // NOLINTNEXTLINE(misc-include-cleaner) — int64_t provided by standard header
    int64_t node_id;
    char normalized[256];
    char name[256];
} config_entry_t;

/* Collect config Variable nodes with ≥2 tokens, each ≥3 chars. */
static int collect_config_entries(const cbm_gbuf_node_t *const *vars, int var_count,
                                  config_entry_t *out, int max_out) {
    int n = 0;
    for (int i = 0; i < var_count && n < max_out; i++) {
        if (!cbm_has_config_extension(vars[i]->file_path)) {
            continue;
        }

        char norm[256];
        int tokens = cbm_normalize_config_key(vars[i]->name, norm, sizeof(norm));
        if (tokens < 2) {
            continue;
        }

        /* Check all tokens ≥3 chars */
        bool all_long = true;
        const char *p = norm;
        while (*p) {
            const char *end = strchr(p, '_');
            size_t tlen = end ? (size_t)(end - p) : strlen(p);
            if (tlen < 3) {
                all_long = false;
                break;
            }
            p = end ? end + 1 : p + tlen;
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
    char normalized[256];
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

            char norm[256];
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

    /* Heap-allocate: these structs are too large for stack (4MB+ total),
     * which causes SIGBUS in background threads with default 512KB stack. */
    config_entry_t *config_entries = calloc(4096, sizeof(config_entry_t));
    if (!config_entries) return 0;
    int config_count = collect_config_entries(vars, var_count, config_entries, 4096);

    if (config_count == 0) {
        free(config_entries);
        return 0;
    }

    code_entry_t *code_entries = calloc(8192, sizeof(code_entry_t));
    if (!code_entries) { free(config_entries); return 0; }
    int code_count = collect_code_entries(gb, code_entries, 8192);

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
                char props[512];
                snprintf(props, sizeof(props),
                         "{\"strategy\":\"key_symbol\",\"confidence\":%.2f,\"config_key\":\"%s\"}",
                         confidence, config_entries[ci].name);

                cbm_gbuf_insert_edge(gb, code_entries[co].node_id, config_entries[ci].node_id,
                                     "CONFIGURES", props);
                edge_count++;
            }
        }
    }

    free(config_entries);
    free(code_entries);
    return edge_count;
}

/* ── Strategy 2: Dependency → Import ────────────────────────────── */

typedef struct {
    int64_t node_id;
    char name[256];
} dep_entry_t;

/* Extract basename from a file path. */
static const char *path_basename(const char *path) {
    if (!path) {
        return "";
    }
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

static int collect_manifest_deps(const cbm_gbuf_node_t *const *vars, int var_count,
                                 dep_entry_t *out, int max_out) {
    int n = 0;
    for (int i = 0; i < var_count && n < max_out; i++) {
        const char *base = path_basename(vars[i]->file_path);
        if (!is_manifest_file(base)) {
            continue;
        }

        /* Check if QN contains a dependency section name */
        bool is_dep = false;
        if (vars[i]->qualified_name) {
            is_dep = is_dep_section(vars[i]->qualified_name);
        }

        /* Cargo.toml special case: check QN parts */
        if (!is_dep && strcmp(base, "Cargo.toml") == 0 && vars[i]->qualified_name) {
            /* Check if any dotted part is a dep section */
            char qn_copy[512];
            snprintf(qn_copy, sizeof(qn_copy), "%s", vars[i]->qualified_name);
            char *saveptr = NULL;
            // NOLINTNEXTLINE(misc-include-cleaner) — strtok_r provided by standard header
            char *part = strtok_r(qn_copy, ".", &saveptr);
            while (part) {
                char lower[128];
                size_t plen = strlen(part);
                if (plen >= sizeof(lower)) {
                    plen = sizeof(lower) - 1;
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
                        is_dep = true;
                        break;
                    }
                }
                if (is_dep) {
                    break;
                }
                part = strtok_r(NULL, ".", &saveptr);
            }
        }

        if (is_dep) {
            out[n].node_id = vars[i]->id;
            snprintf(out[n].name, sizeof(out[n].name), "%s", vars[i]->name);
            n++;
        }
    }
    return n;
}

static int strategy_dep_imports(cbm_gbuf_t *gb) {
    const cbm_gbuf_node_t **vars = NULL;
    int var_count = 0;
    if (cbm_gbuf_find_by_label(gb, "Variable", &vars, &var_count) != 0) {
        return 0;
    }

    dep_entry_t *deps = calloc(2048, sizeof(dep_entry_t));
    if (!deps) return 0;
    int dep_count = collect_manifest_deps(vars, var_count, deps, 2048);

    if (dep_count == 0) {
        free(deps);
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
        char dep_lower[256];
        size_t dlen = strlen(deps[di].name);
        for (size_t j = 0; j < dlen && j < sizeof(dep_lower) - 1; j++) {
            dep_lower[j] = (char)tolower((unsigned char)deps[di].name[j]);
        }
        dep_lower[dlen < sizeof(dep_lower) ? dlen : sizeof(dep_lower) - 1] = '\0';

        for (int ii = 0; ii < import_count; ii++) {
            /* Resolve target and source nodes from gbuf */
            const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(gb, imports[ii]->target_id);
            if (!target) {
                continue;
            }

            const cbm_gbuf_node_t *source = cbm_gbuf_find_by_id(gb, imports[ii]->source_id);
            if (!source) {
                continue;
            }

            /* Compare dep name to import target */
            double confidence = 0.0;
            char target_lower[256];
            size_t tlen = target->name ? strlen(target->name) : 0;
            for (size_t j = 0; j < tlen && j < sizeof(target_lower) - 1; j++) {
                target_lower[j] = (char)tolower((unsigned char)target->name[j]);
            }
            target_lower[tlen < sizeof(target_lower) ? tlen : sizeof(target_lower) - 1] = '\0';

            if (strcmp(target_lower, dep_lower) == 0) {
                confidence = CONF_DEP_EXACT;
            } else if (target->qualified_name) {
                char qn_lower[512];
                size_t qlen = strlen(target->qualified_name);
                for (size_t j = 0; j < qlen && j < sizeof(qn_lower) - 1; j++) {
                    qn_lower[j] = (char)tolower((unsigned char)target->qualified_name[j]);
                }
                qn_lower[qlen < sizeof(qn_lower) ? qlen : sizeof(qn_lower) - 1] = '\0';

                if (strstr(qn_lower, dep_lower) != NULL) {
                    confidence = CONF_DEP_QN_SUBSTR;
                }
            }

            if (confidence > 0.0) {
                char props[512];
                snprintf(
                    props, sizeof(props),
                    "{\"strategy\":\"dependency_import\",\"confidence\":%.2f,\"dep_name\":\"%s\"}",
                    confidence, deps[di].name);

                cbm_gbuf_insert_edge(gb, source->id, deps[di].node_id, "CONFIGURES", props);
                edge_count++;
            }
        }
    }

    free(deps);
    return edge_count;
}

/* ── Strategy 3: Config File Path → Code String Reference ───────── */

/* Prescan-based: config refs were extracted during extraction phase. */
static int strategy_file_refs_prescan(cbm_gbuf_t *gb, const char *project,
                                      const cbm_prescan_t *prescan_cache, int prescan_count,
                                      CBMHashTable *prescan_path_map) {
    /* Collect config Module nodes for matching */
    const cbm_gbuf_node_t **modules = NULL;
    int mod_count = 0;
    if (cbm_gbuf_find_by_label(gb, "Module", &modules, &mod_count) != 0) {
        return 0;
    }

    /* Build basename → Module and fullpath → Module maps */
    typedef struct {
        const char *key;
        int64_t node_id;
    } path_map_t;

    path_map_t *base_map = calloc((size_t)mod_count, sizeof(path_map_t));
    path_map_t *full_map = calloc((size_t)mod_count, sizeof(path_map_t));
    int base_count = 0;
    int full_count = 0;

    for (int i = 0; i < mod_count; i++) {
        if (cbm_has_config_extension(modules[i]->file_path)) {
            const char *slash = strrchr(modules[i]->file_path, '/');
            base_map[base_count].key = slash ? slash + 1 : modules[i]->file_path;
            base_map[base_count].node_id = modules[i]->id;
            base_count++;

            full_map[full_count].key = modules[i]->file_path;
            full_map[full_count].node_id = modules[i]->id;
            full_count++;
        }
    }

    if (base_count == 0) {
        free(base_map);
        free(full_map);
        return 0;
    }

    int edge_count = 0;

    /* Iterate prescan results — each file's config refs were extracted during extraction */
    for (int fi = 0; fi < prescan_count; fi++) {
        const cbm_prescan_t *ps = &prescan_cache[fi];
        if (ps->config_ref_count == 0) {
            continue;
        }

        /* Find the Module node for this file via prescan_path_map */
        /* We need to reverse-lookup: file_idx → rel_path → Module QN.
         * But we don't have the files array here. Instead, iterate modules
         * and check which ones have prescan data. */
    }

    /* Alternative approach: iterate non-config modules and check their prescan */
    for (int i = 0; i < mod_count; i++) {
        if (cbm_has_config_extension(modules[i]->file_path)) {
            continue;
        }

        /* Look up prescan data for this file */
        void *val = cbm_ht_get(prescan_path_map, modules[i]->file_path);
        if (!val) {
            continue;
        }
        int file_idx = (int)((intptr_t)val - 1);
        if (file_idx < 0 || file_idx >= prescan_count) {
            continue;
        }
        const cbm_prescan_t *ps = &prescan_cache[file_idx];
        if (ps->config_ref_count == 0) {
            continue;
        }

        for (int ri = 0; ri < ps->config_ref_count; ri++) {
            const char *ref_path = ps->config_refs[ri].ref_path;

            int64_t target_id = 0;
            double confidence = 0.0;

            /* Try full path match */
            for (int fi2 = 0; fi2 < full_count; fi2++) {
                if (strcmp(full_map[fi2].key, ref_path) == 0) {
                    target_id = full_map[fi2].node_id;
                    confidence = CONF_FILE_FULLPATH;
                    break;
                }
            }

            /* Try basename match */
            if (target_id == 0) {
                const char *ref_slash = strrchr(ref_path, '/');
                const char *ref_base = ref_slash ? ref_slash + 1 : ref_path;
                for (int bi = 0; bi < base_count; bi++) {
                    if (strcmp(base_map[bi].key, ref_base) == 0) {
                        target_id = base_map[bi].node_id;
                        confidence = CONF_FILE_BASENAME;
                        break;
                    }
                }
            }

            if (target_id != 0) {
                char *module_qn = cbm_pipeline_fqn_module(project, modules[i]->file_path);
                if (module_qn) {
                    const cbm_gbuf_node_t *src_node = cbm_gbuf_find_by_qn(gb, module_qn);
                    if (src_node) {
                        char props[512];
                        snprintf(props, sizeof(props),
                                 "{\"strategy\":\"file_reference\",\"confidence\":%.2f,\"ref_"
                                 "path\":\"%s\"}",
                                 confidence, ref_path);
                        cbm_gbuf_insert_edge(gb, src_node->id, target_id, "CONFIGURES", props);
                        edge_count++;
                    }
                    free(module_qn);
                }
            }
        }
    }

    free(base_map);
    free(full_map);
    return edge_count;
}

/* Disk-based fallback: reads source from disk (sequential pipeline path). */
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static int strategy_file_refs_disk(cbm_gbuf_t *gb, const char *project, const char *repo_path) {
    /* Collect config Module nodes */
    const cbm_gbuf_node_t **modules = NULL;
    int mod_count = 0;
    if (cbm_gbuf_find_by_label(gb, "Module", &modules, &mod_count) != 0) {
        return 0;
    }

    /* Build basename → Module and fullpath → Module maps */
    typedef struct {
        const char *key;
        int64_t node_id;
    } path_map_t;

    path_map_t *base_map = calloc((size_t)mod_count, sizeof(path_map_t));
    path_map_t *full_map = calloc((size_t)mod_count, sizeof(path_map_t));
    int base_count = 0;
    int full_count = 0;

    int scan_count = 0;
    int *scan_indices = calloc((size_t)mod_count, sizeof(int));

    for (int i = 0; i < mod_count; i++) {
        if (cbm_has_config_extension(modules[i]->file_path)) {
            base_map[base_count].key = path_basename(modules[i]->file_path);
            base_map[base_count].node_id = modules[i]->id;
            base_count++;

            full_map[full_count].key = modules[i]->file_path;
            full_map[full_count].node_id = modules[i]->id;
            full_count++;
        } else {
            scan_indices[scan_count++] = i;
        }
    }

    if (base_count == 0) {
        free(base_map);
        free(full_map);
        free(scan_indices);
        return 0;
    }

    /* Compile regex for config file references in string literals */
    cbm_regex_t re;
    int rc = cbm_regcomp(&re, "[\"']([^\"']*\\.(toml|yaml|yml|ini|json|xml|conf|cfg|env))[\"']",
                         CBM_REG_EXTENDED);
    if (rc != 0) {
        free(base_map);
        free(full_map);
        free(scan_indices);
        return 0;
    }

    int edge_count = 0;

    for (int si = 0; si < scan_count; si++) {
        int idx = scan_indices[si];
        const char *file_path = modules[idx]->file_path;

        /* Read source file from disk */
        char abs_path[1024];
        snprintf(abs_path, sizeof(abs_path), "%s/%s", repo_path, file_path);

        FILE *f = fopen(abs_path, "r");
        if (!f) {
            continue;
        }

        (void)fseek(f, 0, SEEK_END);
        long fsize = ftell(f);
        (void)fseek(f, 0, SEEK_SET);
        if (fsize <= 0 || fsize > (long)10 * 1024 * 1024) {
            (void)fclose(f);
            continue;
        }

        char *source = malloc((size_t)fsize + 1);
        if (!source) {
            (void)fclose(f);
            continue;
        }
        size_t nread = fread(source, 1, (size_t)fsize, f);
        // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
        source[nread] = '\0';
        (void)fclose(f);

        /* Find all config file references via regex */
        cbm_regmatch_t match[3];
        const char *cursor = source;
        while (cbm_regexec(&re, cursor, 3, match, 0) == 0) {
            /* Extract the captured path (group 1) */
            int start = match[1].rm_so;
            int end = match[1].rm_eo;
            int ref_len = end - start;
            char ref_path[512];
            if (ref_len >= (int)sizeof(ref_path)) {
                ref_len = (int)sizeof(ref_path) - 1;
            }
            memcpy(ref_path, cursor + start, (size_t)ref_len);
            ref_path[ref_len] = '\0';

            /* Match against config modules */
            int64_t target_id = 0;
            double confidence = 0.0;

            /* Try full path match */
            for (int fi = 0; fi < full_count; fi++) {
                if (strcmp(full_map[fi].key, ref_path) == 0) {
                    target_id = full_map[fi].node_id;
                    confidence = CONF_FILE_FULLPATH;
                    break;
                }
            }

            /* Try basename match */
            if (target_id == 0) {
                const char *ref_base = path_basename(ref_path);
                for (int bi = 0; bi < base_count; bi++) {
                    if (strcmp(base_map[bi].key, ref_base) == 0) {
                        target_id = base_map[bi].node_id;
                        confidence = CONF_FILE_BASENAME;
                        break;
                    }
                }
            }

            if (target_id != 0) {
                /* Resolve source Module QN */
                char *module_qn = cbm_pipeline_fqn_module(project, file_path);
                if (module_qn) {
                    const cbm_gbuf_node_t *src_node = cbm_gbuf_find_by_qn(gb, module_qn);
                    if (src_node) {
                        char props[512];
                        snprintf(props, sizeof(props),
                                 "{\"strategy\":\"file_reference\",\"confidence\":%.2f,\"ref_"
                                 "path\":\"%s\"}",
                                 confidence, ref_path);

                        cbm_gbuf_insert_edge(gb, src_node->id, target_id, "CONFIGURES", props);
                        edge_count++;
                    }
                    free(module_qn);
                }
            }

            cursor += match[0].rm_eo;
        }

        free(source);
    }

    cbm_regfree(&re);
    free(base_map);
    free(full_map);
    free(scan_indices);
    return edge_count;
}

/* ── Public API ──────────────────────────────────────────────────── */

int cbm_pipeline_pass_configlink(cbm_pipeline_ctx_t *ctx) {
    cbm_gbuf_t *gb = ctx->gbuf;
    const char *project = ctx->project_name;
    const char *repo_path = ctx->repo_path;
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

    char buf1[16];
    char buf2[16];
    char buf3[16];
    char buf4[16];

    int key_edges = strategy_key_symbols(gb);
    snprintf(buf1, sizeof(buf1), "%d", key_edges);
    cbm_log_info("configlinker.strategy", "name", "key_symbol", "edges", buf1);

    int dep_edges = strategy_dep_imports(gb);
    snprintf(buf2, sizeof(buf2), "%d", dep_edges);
    cbm_log_info("configlinker.strategy", "name", "dep_import", "edges", buf2);

    int ref_edges = 0;
    if (ctx->prescan_cache && ctx->prescan_path_map) {
        ref_edges = strategy_file_refs_prescan(gb, project, ctx->prescan_cache, ctx->prescan_count,
                                               ctx->prescan_path_map);
    } else if (repo_path) {
        ref_edges = strategy_file_refs_disk(gb, project, repo_path);
    }
    snprintf(buf3, sizeof(buf3), "%d", ref_edges);
    cbm_log_info("configlinker.strategy", "name", "file_ref", "edges", buf3);

    snprintf(buf4, sizeof(buf4), "%d", key_edges + dep_edges + ref_edges);
    cbm_log_info("configlinker.done", "total", buf4);

    return key_edges + dep_edges + ref_edges;
}
