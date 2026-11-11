/*
 * pass_spawns.c — SPAWNS edges: a call that starts another program, to the
 * Process node of that program (internal/cbm/spawn_patterns.h).
 *
 * Shared by both call resolvers (pass_calls.c, pass_parallel.c), which ask
 * at the same point — after registry resolution and the cross-language veto,
 * before the empty-resolution fallbacks — so they emit the same edges.
 */
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/constants.h"
#include "foundation/str_util.h"
#include "spawn_patterns.h"

#include <stdio.h>
#include <string.h>

/* Edge confidence: a site read from the source, or only from the
 * macro-expanded buffer of a C-family file. */
static const double SPAWN_CONF_SOURCE = 1.0;
static const double SPAWN_CONF_EXPANDED = 0.5;

/* Strategies that only guess by name: they never make a spawn spelling an
 * ordinary call (`subprocess.run` must not become the project's `run`, nor
 * a call of the C library's `system` the static `system` of another file). */
static bool spawn_name_guess(const char *strategy) {
    static const char *const guesses[] = {"unique_name",     "suffix_match", "qualified_suffix",
                                          "field_type_hint", "fuzzy",        NULL};
    for (int i = 0; strategy && guesses[i]; i++) {
        if (strcmp(strategy, guesses[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Labels of code a call can run: a function, or a type it constructs. */
static bool spawn_code_label(const char *label) {
    static const char *const labels[] = {"Function", "Method", "Class", "Struct", "Interface",
                                         "Trait",    "Enum",   "Type",  "Macro",  NULL};
    for (int i = 0; label && labels[i]; i++) {
        if (strcmp(label, labels[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* The call reached code in the project's own sources. A synthetic builtin
 * (a `<python-builtins>` node), a QN without a node, and a Variable are not:
 * `const { spawn } = require("child_process")` mints a Variable `spawn` that
 * the call resolves to by same module, and it is the import binding. For a
 * qualified spelling, same_module is a guess too: it matched the suffix
 * alone, so `subprocess.run` inside a module's own `def run` found that
 * `run`. */
static bool spawn_resolved_to_project(const cbm_gbuf_t *gbuf, const cbm_resolution_t *res,
                                      bool qualified) {
    if (!res || !res->qualified_name || !res->qualified_name[0]) {
        return false;
    }
    const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(gbuf, res->qualified_name);
    if (!node || !node->file_path || !node->file_path[0] || node->file_path[0] == '<' ||
        !spawn_code_label(node->label)) {
        return false;
    }
    bool guess = spawn_name_guess(res->strategy) ||
                 (qualified && res->strategy && strcmp(res->strategy, "same_module") == 0);
    return !guess;
}

bool cbm_pipeline_spawn_site(const cbm_gbuf_t *gbuf, CBMLanguage lang, const CBMCall *call,
                             const CBMImportArray *imports, const cbm_resolution_t *res,
                             cbm_pipeline_spawn_t *out) {
    const cbm_spawn_api_t *api = cbm_spawn_match(
        lang, call->callee_name, imports ? imports->items : NULL, imports ? imports->count : 0);
    bool qualified = strpbrk(call->callee_name, ".:/\\") != NULL;
    if (!api || spawn_resolved_to_project(gbuf, res, qualified)) {
        return false;
    }
    out->api = api->api;
    cbm_spawn_program(api, lang, call, out->program, sizeof(out->program));
    return true;
}

void cbm_pipeline_emit_spawn(cbm_gbuf_t *gbuf, const cbm_gbuf_node_t *source, const CBMCall *call,
                             const cbm_pipeline_spawn_t *spawn) {
    char program[CBM_SZ_128];
    char api[CBM_SZ_256];
    cbm_json_escape(program, (int)sizeof(program), spawn->program);
    cbm_json_escape(api, (int)sizeof(api), spawn->api);
    char qn[CBM_SZ_128];
    snprintf(qn, sizeof(qn), "__process__%s", spawn->program);
    char node_props[CBM_SZ_256];
    snprintf(node_props, sizeof(node_props), "{\"program\":\"%s\"}", program);
    int64_t process_id =
        cbm_gbuf_upsert_node(gbuf, "Process", spawn->program, qn, "", 0, 0, node_props);
    if (process_id <= 0) {
        return;
    }
    /* C-family files are extracted twice; a call from the macro-expanded
     * buffer carries an expanded-buffer line, not a source line. It records
     * no line and less confidence, so the raw site's edge (which wins on
     * confidence when both exist) keeps the source line. */
    char edge_props[CBM_SZ_512];
    if (call->source_origin == CBM_SOURCE_ORIGIN_RAW) {
        snprintf(edge_props, sizeof(edge_props),
                 "{\"api\":\"%s\",\"program\":\"%s\",\"line\":%d,\"confidence\":%.2f}", api,
                 program, call->start_line, SPAWN_CONF_SOURCE);
    } else {
        snprintf(edge_props, sizeof(edge_props),
                 "{\"api\":\"%s\",\"program\":\"%s\",\"confidence\":%.2f}", api, program,
                 SPAWN_CONF_EXPANDED);
    }
    cbm_gbuf_insert_edge(gbuf, source->id, process_id, "SPAWNS", edge_props);
}
