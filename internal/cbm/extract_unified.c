#include "extract_unified.h"
#include "arena.h" // cbm_arena_sprintf
#include "cbm.h"   // CBMExtractCtx
#include "helpers.h"
#include "lang_specs.h"      // CBMLangSpec, cbm_lang_spec, CBM_LANG_*
#include "tree_sitter/api.h" // TSNode, TSTreeCursor, ts_tree_cursor_*, ts_node_*
#include <stdint.h>          // uint32_t, uint8_t
#include <string.h>

// --- Scope stack management ---

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static void push_scope(WalkState *state, uint8_t kind, uint32_t depth, const char *qn) {
    if (state->scope_top >= MAX_SCOPES) {
        return;
    }
    state->scopes[state->scope_top].kind = kind;
    state->scopes[state->scope_top].depth = depth;
    state->scopes[state->scope_top].qn = qn;
    state->scope_top++;
}

// Pop scopes that we've ascended out of (depth >= current cursor depth).
static void pop_expired_scopes(WalkState *state, uint32_t cur_depth) {
    while (state->scope_top > 0 && state->scopes[state->scope_top - 1].depth >= cur_depth) {
        state->scope_top--;
    }
}

// Recompute state flags from the current scope stack.
static void recompute_state(WalkState *state, const char *top_level_qn) {
    state->enclosing_func_qn = top_level_qn;
    state->enclosing_class_qn = NULL;
    state->inside_call = false;
    state->inside_import = false;

    for (int i = 0; i < state->scope_top; i++) {
        switch (state->scopes[i].kind) {
        case SCOPE_FUNC:
            state->enclosing_func_qn = state->scopes[i].qn;
            break;
        case SCOPE_CLASS:
            state->enclosing_class_qn = state->scopes[i].qn;
            break;
        case SCOPE_CALL:
            state->inside_call = true;
            break;
        case SCOPE_IMPORT:
            state->inside_import = true;
            break;
        default:
            break;
        }
    }
}

// Compute function QN for scope tracking (mirrors cbm_enclosing_func_qn logic).
static const char *compute_func_qn(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec,
                                   WalkState *state) {
    (void)spec;
    // Wolfram: set_delayed_top/set_top/set_delayed/set — LHS is apply(user_symbol("f"), ...)
    if (ctx->language == CBM_LANG_WOLFRAM) {
        const char *nk = ts_node_type(node);
        if (strcmp(nk, "set_delayed_top") == 0 || strcmp(nk, "set_top") == 0 ||
            strcmp(nk, "set_delayed") == 0 || strcmp(nk, "set") == 0) {
            if (ts_node_named_child_count(node) > 0) {
                TSNode lhs = ts_node_named_child(node, 0);
                if (strcmp(ts_node_type(lhs), "apply") == 0 && ts_node_named_child_count(lhs) > 0) {
                    TSNode head = ts_node_named_child(lhs, 0);
                    if (strcmp(ts_node_type(head), "user_symbol") == 0) {
                        char *name = cbm_node_text(ctx->arena, head, ctx->source);
                        if (name && name[0]) {
                            return cbm_fqn_compute(ctx->arena, ctx->project, ctx->rel_path, name);
                        }
                    }
                }
            }
            return NULL;
        }
    }

    const char *node_kind = ts_node_type(node);
    const char *name = NULL;

    if (ctx->language == CBM_LANG_GDSCRIPT && strcmp(node_kind, "constructor_definition") == 0) {
        name = "_init";
    }

    TSNode name_node = ts_node_child_by_field_name(node, "name", 4);

    // Arrow function: name from parent variable_declarator
    if (ts_node_is_null(name_node) && strcmp(ts_node_type(node), "arrow_function") == 0) {
        TSNode parent = ts_node_parent(node);
        if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "variable_declarator") == 0) {
            name_node = ts_node_child_by_field_name(parent, "name", 4);
        }
    }

    if (!name && ts_node_is_null(name_node)) {
        return NULL;
    }

    if (!name) {
        name = cbm_node_text(ctx->arena, name_node, ctx->source);
        if (!name || !name[0]) {
            return NULL;
        }
    }

    if (state->enclosing_class_qn) {
        return cbm_arena_sprintf(ctx->arena, "%s.%s", state->enclosing_class_qn, name);
    }

    if (ctx->language == CBM_LANG_GDSCRIPT) {
        const char *anchor_qn =
            cbm_gdscript_anchor_qn(ctx->arena, ctx->root, ctx->source, ctx->project, ctx->rel_path);
        if (anchor_qn && anchor_qn[0]) {
            return cbm_arena_sprintf(ctx->arena, "%s.%s", anchor_qn, name);
        }
    }

    return cbm_fqn_compute(ctx->arena, ctx->project, ctx->rel_path, name);
}

// Compute class QN for scope tracking.
static const char *compute_class_qn(CBMExtractCtx *ctx, TSNode node, WalkState *state) {
    TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
    if (ts_node_is_null(name_node)) {
        return NULL;
    }

    char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
    if (!name || !name[0]) {
        return NULL;
    }

    if (state && state->enclosing_class_qn && state->enclosing_class_qn[0]) {
        return cbm_arena_sprintf(ctx->arena, "%s.%s", state->enclosing_class_qn, name);
    }

    if (ctx->language == CBM_LANG_GDSCRIPT) {
        const char *anchor_qn =
            cbm_gdscript_anchor_qn(ctx->arena, ctx->root, ctx->source, ctx->project, ctx->rel_path);
        if (anchor_qn && anchor_qn[0]) {
            return cbm_arena_sprintf(ctx->arena, "%s.%s", anchor_qn, name);
        }
    }

    return cbm_fqn_compute(ctx->arena, ctx->project, ctx->rel_path, name);
}

/* Forward declaration */
static bool is_string_node(const char *kind);

// --- Module-level constant collection ---

static void handle_string_constants(CBMExtractCtx *ctx, TSNode node, const WalkState *state,
                                    const char *top_level_qn) {
    /* Only collect at module level (not inside functions/classes) */
    if (state->enclosing_func_qn != NULL && state->enclosing_func_qn != top_level_qn) {
        return;
    }
    if (state->enclosing_class_qn != NULL) {
        return;
    }

    const char *kind = ts_node_type(node);

    /* Python: expression_statement → assignment → identifier = string */
    /* Go: short_var_declaration, const_spec */
    /* JS/TS: variable_declarator, lexical_declaration */
    if (strcmp(kind, "assignment") != 0 && strcmp(kind, "expression_statement") != 0 &&
        strcmp(kind, "short_var_declaration") != 0 && strcmp(kind, "const_spec") != 0 &&
        strcmp(kind, "variable_declarator") != 0 && strcmp(kind, "const_statement") != 0) {
        return;
    }

    /* Find name (left side) and value (right side) */
    TSNode name_node = ts_node_child_by_field_name(node, "left", 4);
    TSNode value_node = ts_node_child_by_field_name(node, "right", 5);

    /* Some grammars use "name" + "value" fields */
    if (ts_node_is_null(name_node)) {
        name_node = ts_node_child_by_field_name(node, "name", 4);
    }
    if (ts_node_is_null(value_node)) {
        value_node = ts_node_child_by_field_name(node, "value", 5);
    }

    if (ts_node_is_null(name_node) || ts_node_is_null(value_node)) {
        return;
    }

    /* Name must be an identifier */
    const char *name_kind = ts_node_type(name_node);
    if (strcmp(name_kind, "identifier") != 0 && strcmp(name_kind, "constant") != 0 &&
        strcmp(name_kind, "name") != 0 && strcmp(name_kind, "type_identifier") != 0) {
        return;
    }

    /* Value must be a string literal */
    if (!is_string_node(ts_node_type(value_node))) {
        return;
    }

    char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
    char *value = cbm_node_text(ctx->arena, value_node, ctx->source);
    if (!name || !name[0] || !value || !value[0]) {
        return;
    }

    /* Strip quotes from value */
    int vlen = (int)strlen(value);
    if (vlen >= 2 && (value[0] == '"' || value[0] == '\'')) {
        value = cbm_arena_strndup(ctx->arena, value + 1, (size_t)(vlen - 2));
        if (!value) {
            return;
        }
    }

    /* Add to constant map */
    CBMStringConstantMap *map = &ctx->string_constants;
    if (map->count < CBM_MAX_STRING_CONSTANTS) {
        map->names[map->count] = name;
        map->values[map->count] = value;
        map->count++;
    }
}

// --- String literal collection ---

static bool is_string_node(const char *kind) {
    /* Common string literal node types across tree-sitter grammars */
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return (strcmp(kind, "string_literal") == 0 || strcmp(kind, "string") == 0 ||
            strcmp(kind, "string_content") == 0 ||
            strcmp(kind, "interpreted_string_literal") == 0 ||
            strcmp(kind, "raw_string_literal") == 0 || strcmp(kind, "string_value") == 0 ||
            /* YAML string types */
            strcmp(kind, "double_quote_scalar") == 0 || strcmp(kind, "single_quote_scalar") == 0);
}

static bool has_suffix_n(const char *s, int len, const char *suffix) {
    int slen = (int)strlen(suffix);
    if (!s || !suffix || len < slen) {
        return false;
    }
    return strncmp(s + (len - slen), suffix, (size_t)slen) == 0;
}

static bool gdscript_is_global_load_call_node(CBMExtractCtx *ctx, TSNode call_node) {
    if (strcmp(ts_node_type(call_node), "attribute_call") == 0) {
        return false;
    }

    TSNode object_node = ts_node_child_by_field_name(call_node, "object", 6);
    if (!ts_node_is_null(object_node)) {
        return false;
    }

    const char *callee = NULL;
    TSNode function_node = ts_node_child_by_field_name(call_node, "function", 8);
    if (!ts_node_is_null(function_node)) {
        callee = cbm_node_text(ctx->arena, function_node, ctx->source);
    }
    if (!callee || !callee[0]) {
        TSNode name_node = ts_node_child_by_field_name(call_node, "name", 4);
        if (!ts_node_is_null(name_node)) {
            callee = cbm_node_text(ctx->arena, name_node, ctx->source);
        }
    }

    if (!callee || !callee[0]) {
        const char *call_text = cbm_node_text(ctx->arena, call_node, ctx->source);
        if (!call_text || !call_text[0]) {
            return false;
        }
        const char *open = strchr(call_text, '(');
        if (!open || open == call_text) {
            return false;
        }
        while (call_text < open && (*call_text == ' ' || *call_text == '\t')) {
            call_text++;
        }
        const char *end = open;
        while (end > call_text && (*(end - 1) == ' ' || *(end - 1) == '\t')) {
            end--;
        }
        size_t n = (size_t)(end - call_text);
        if (!((n == 4 && strncmp(call_text, "load", 4) == 0) ||
              (n == 7 && strncmp(call_text, "preload", 7) == 0))) {
            return false;
        }
        for (const char *p = call_text; p < end; p++) {
            if (*p == '.') {
                return false;
            }
        }
        return true;
    }

    if (!(strcmp(callee, "preload") == 0 || strcmp(callee, "load") == 0)) {
        return false;
    }

    return strchr(callee, '.') == NULL;
}

static bool gdscript_is_scene_load_target(CBMExtractCtx *ctx, TSNode string_node, const char *content,
                                          int len) {
    if (!content || len <= 0 || !has_suffix_n(content, len, ".tscn")) {
        return false;
    }

    TSNode arg_node = string_node;
    TSNode args_node = ts_node_parent(arg_node);
    if (!ts_node_is_null(args_node)) {
        const char *parent_kind = ts_node_type(args_node);
        if (strcmp(parent_kind, "string") == 0 || strcmp(parent_kind, "string_literal") == 0) {
            arg_node = args_node;
            args_node = ts_node_parent(arg_node);
        }
    }
    if (ts_node_is_null(args_node)) {
        return false;
    }

    const char *args_kind = ts_node_type(args_node);
    if (!(strcmp(args_kind, "arguments") == 0 || strcmp(args_kind, "argument_list") == 0)) {
        return false;
    }

    if (ts_node_named_child_count(args_node) == 0) {
        return false;
    }
    TSNode first_arg = ts_node_named_child(args_node, 0);
    if (!ts_node_eq(first_arg, arg_node)) {
        return false;
    }

    TSNode call_node = ts_node_parent(args_node);
    if (ts_node_is_null(call_node)) {
        return false;
    }
    const char *call_kind = ts_node_type(call_node);
    if (!(strcmp(call_kind, "call") == 0 || strcmp(call_kind, "attribute_call") == 0)) {
        return false;
    }

    return gdscript_is_global_load_call_node(ctx, call_node);
}

static void handle_string_refs(CBMExtractCtx *ctx, TSNode node, const WalkState *state) {
    const char *kind = ts_node_type(node);
    if (!is_string_node(kind)) {
        return;
    }

    /* Extract string content */
    char *text = cbm_node_text(ctx->arena, node, ctx->source);
    if (!text || !text[0]) {
        return;
    }

    /* Strip quotes if present */
    int len = (int)strlen(text);
    const char *content = text;
    if (len >= 2 && (text[0] == '"' || text[0] == '\'')) {
        content = text + 1;
        len -= 2;
        if (len <= 0) {
            return;
        }
    }

    /* Classify */
    int kind_val = cbm_classify_string(content, len);
    if (kind_val < 0 && ctx->language == CBM_LANG_GDSCRIPT &&
        gdscript_is_scene_load_target(ctx, node, content, len)) {
        kind_val = CBM_STRREF_CONFIG;
    }
    if (kind_val < 0) {
        return;
    }

    /* Build null-terminated content string in arena */
    char *val = cbm_arena_strndup(ctx->arena, content, (size_t)len);
    if (!val) {
        return;
    }

    CBMStringRef ref = {
        .value = val,
        .enclosing_func_qn = state->enclosing_func_qn ? state->enclosing_func_qn : ctx->module_qn,
        .kind = (CBMStringRefKind)kind_val,
    };
    cbm_stringref_push(&ctx->result->string_refs, ctx->arena, ref);
}

// --- YAML nested field extraction (D2) ---

/* Recursively walk YAML block_mapping_pair nodes, building dotted key paths.
 * Emits string_refs with key_path for leaf values that are URLs or config values.
 * Example: body.operational_info.post_url → "https://..." */
static void walk_yaml_mapping(CBMExtractCtx *ctx, TSNode node, const char *prefix) {
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_named_child(node, i);
        const char *ck = ts_node_type(child);

        if (strcmp(ck, "block_mapping_pair") != 0) {
            continue;
        }

        /* Get key */
        TSNode key = ts_node_child_by_field_name(child, "key", 3);
        if (ts_node_is_null(key)) {
            continue;
        }
        char *key_text = cbm_node_text(ctx->arena, key, ctx->source);
        if (!key_text || !key_text[0]) {
            continue;
        }

        /* Build dotted path */
        const char *path =
            prefix ? cbm_arena_sprintf(ctx->arena, "%s.%s", prefix, key_text) : key_text;

        /* Get value node */
        TSNode val = ts_node_child_by_field_name(child, "value", 5);
        if (ts_node_is_null(val)) {
            continue;
        }
        const char *vk = ts_node_type(val);

        /* If value is a nested mapping (block_node → block_mapping), recurse */
        if (strcmp(vk, "block_node") == 0 || strcmp(vk, "block_mapping") == 0) {
            /* Walk children for nested block_mapping */
            uint32_t vnc = ts_node_named_child_count(val);
            for (uint32_t vi = 0; vi < vnc; vi++) {
                TSNode vc = ts_node_named_child(val, vi);
                const char *vctype = ts_node_type(vc);
                if (strcmp(vctype, "block_mapping") == 0 ||
                    strcmp(vctype, "block_mapping_pair") == 0) {
                    walk_yaml_mapping(ctx, vc, path);
                }
            }
            continue;
        }

        /* Leaf value: extract and classify */
        char *val_text = cbm_node_text(ctx->arena, val, ctx->source);
        if (!val_text || !val_text[0]) {
            continue;
        }

        /* Strip quotes */
        int vlen = (int)strlen(val_text);
        const char *content = val_text;
        if (vlen >= 2 && (val_text[0] == '"' || val_text[0] == '\'')) {
            content = val_text + 1;
            vlen -= 2;
            if (vlen <= 0) {
                continue;
            }
        }

        int kind_val = cbm_classify_string(content, vlen);
        if (kind_val < 0) {
            continue;
        }

        char *stored = cbm_arena_strndup(ctx->arena, content, (size_t)vlen);
        if (!stored) {
            continue;
        }

        CBMStringRef ref = {
            .value = stored,
            .enclosing_func_qn = ctx->module_qn,
            .key_path = path,
            .kind = (CBMStringRefKind)kind_val,
        };
        cbm_stringref_push(&ctx->result->string_refs, ctx->arena, ref);
    }
}

/* ── Infrastructure binding extraction ─────────────────────────────
 * Scan YAML/JSON/HCL list items for topic→URL pairs.
 * Patterns detected:
 *   YAML: {topic: X, config: {push_endpoint: URL}} (Pub/Sub subscription)
 *   YAML: {uri: URL, body: ...} (Cloud Scheduler)
 *   YAML: {queue: X, uri: URL} (Cloud Tasks)
 *   HCL: resource "google_pubsub_subscription" { topic=X, push_config{push_endpoint=URL} }
 *
 * Works by collecting key-value pairs in each mapping, then checking for
 * known source+target patterns. Language-agnostic: the key names are the signal. */

/* Source key names (topic/queue/schedule identifier) */
static int is_source_key(const char *key) {
    return (strcmp(key, "topic") == 0 || strcmp(key, "queue") == 0 ||
            strcmp(key, "queue_name") == 0 || strcmp(key, "subscription") == 0 ||
            strcmp(key, "subject") == 0 || strcmp(key, "channel") == 0 ||
            strcmp(key, "stream") == 0);
}

/* Target key names (endpoint URL) */
static int is_target_key(const char *key) {
    return (strcmp(key, "push_endpoint") == 0 || strcmp(key, "uri") == 0 ||
            strcmp(key, "url") == 0 || strcmp(key, "endpoint") == 0 ||
            strcmp(key, "http_target") == 0 || strcmp(key, "target_url") == 0 ||
            strcmp(key, "webhook_url") == 0 || strcmp(key, "callback_url") == 0);
}

/* Infer broker type from surrounding context */
static const char *infer_broker(const char *file_path, const char *source_key) {
    if (strstr(file_path, "pubsub") || strstr(file_path, "pub-sub") ||
        strstr(file_path, "pub_sub")) {
        return "pubsub";
    }
    if (strstr(file_path, "scheduler") || strstr(file_path, "schedule") ||
        strstr(file_path, "cron")) {
        return "cloud_scheduler";
    }
    if (strstr(file_path, "task") || strcmp(source_key, "queue") == 0 ||
        strcmp(source_key, "queue_name") == 0) {
        return "cloud_tasks";
    }
    if (strstr(file_path, "kafka") || strcmp(source_key, "stream") == 0) {
        return "kafka";
    }
    if (strstr(file_path, "sqs") || strstr(file_path, "sns")) {
        return "sqs";
    }
    return "async";
}

/* Scan a YAML mapping for source+target key pairs.
 * Collects all key-value pairs at this level and one level deep (for nested config:). */
static void scan_mapping_for_bindings(CBMExtractCtx *ctx, TSNode mapping) {
    const char *sources[8] = {NULL};
    const char *source_keys[8] = {NULL};
    int n_sources = 0;
    const char *targets[8] = {NULL};
    int n_targets = 0;

    uint32_t nc = ts_node_named_child_count(mapping);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode pair = ts_node_named_child(mapping, i);
        if (strcmp(ts_node_type(pair), "block_mapping_pair") != 0) {
            continue;
        }
        TSNode key = ts_node_child_by_field_name(pair, "key", 3);
        TSNode val = ts_node_child_by_field_name(pair, "value", 5);
        if (ts_node_is_null(key) || ts_node_is_null(val)) {
            continue;
        }
        char *k = cbm_node_text(ctx->arena, key, ctx->source);
        if (!k) {
            continue;
        }

        /* Check if this is a source or target key with a scalar value */
        const char *vtype = ts_node_type(val);
        if (strcmp(vtype, "block_node") != 0 && strcmp(vtype, "block_mapping") != 0) {
            char *v = cbm_node_text(ctx->arena, val, ctx->source);
            if (v && v[0]) {
                /* Strip quotes */
                int vlen = (int)strlen(v);
                if (vlen >= 2 && (v[0] == '"' || v[0] == '\'')) {
                    v = cbm_arena_strndup(ctx->arena, v + 1, (size_t)(vlen - 2));
                }
                if (is_source_key(k) && n_sources < 8) {
                    sources[n_sources] = v;
                    source_keys[n_sources] = k;
                    n_sources++;
                }
                if (is_target_key(k) && n_targets < 8 && v && strstr(v, "://")) {
                    targets[n_targets++] = v;
                }
            }
        } else {
            /* Nested mapping (e.g., config: {push_endpoint: URL}) — scan one level */
            uint32_t vnc = ts_node_named_child_count(val);
            for (uint32_t vi = 0; vi < vnc; vi++) {
                TSNode vc = ts_node_named_child(val, vi);
                const char *vck = ts_node_type(vc);
                if (strcmp(vck, "block_mapping") == 0) {
                    /* Scan nested mapping for target keys */
                    uint32_t mnc = ts_node_named_child_count(vc);
                    for (uint32_t mi = 0; mi < mnc; mi++) {
                        TSNode mp = ts_node_named_child(vc, mi);
                        if (strcmp(ts_node_type(mp), "block_mapping_pair") != 0) {
                            continue;
                        }
                        TSNode mk = ts_node_child_by_field_name(mp, "key", 3);
                        TSNode mv = ts_node_child_by_field_name(mp, "value", 5);
                        if (ts_node_is_null(mk) || ts_node_is_null(mv)) {
                            continue;
                        }
                        char *mktext = cbm_node_text(ctx->arena, mk, ctx->source);
                        if (mktext && is_target_key(mktext) && n_targets < 8) {
                            char *mvtext = cbm_node_text(ctx->arena, mv, ctx->source);
                            if (mvtext && mvtext[0]) {
                                int mvlen = (int)strlen(mvtext);
                                if (mvlen >= 2 && (mvtext[0] == '"' || mvtext[0] == '\'')) {
                                    mvtext = cbm_arena_strndup(ctx->arena, mvtext + 1,
                                                               (size_t)(mvlen - 2));
                                }
                                if (mvtext && strstr(mvtext, "://")) {
                                    targets[n_targets++] = mvtext;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /* Emit bindings for each source × target pair */
    for (int si = 0; si < n_sources; si++) {
        for (int ti = 0; ti < n_targets; ti++) {
            if (!sources[si] || !targets[ti]) {
                continue;
            }
            CBMInfraBinding ib = {
                .source_name = sources[si],
                .target_url = targets[ti],
                .broker = infer_broker(ctx->rel_path, source_keys[si]),
            };
            cbm_infrabinding_push(&ctx->result->infra_bindings, ctx->arena, ib);
        }
    }
}

/* Walk a YAML block_sequence looking for list items with infra bindings */
static void scan_yaml_for_infra_bindings(CBMExtractCtx *ctx, TSNode node) {
    const char *kind = ts_node_type(node);

    /* List items are block_sequence → block_sequence_item → block_mapping */
    if (strcmp(kind, "block_mapping") == 0) {
        scan_mapping_for_bindings(ctx, node);
    }

    /* Recurse into children */
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        scan_yaml_for_infra_bindings(ctx, ts_node_named_child(node, i));
    }
}

/* Handle YAML files: walk top-level block_mapping recursively */
static void handle_yaml_nested(CBMExtractCtx *ctx, TSNode node) {
    if (ctx->language != CBM_LANG_YAML) {
        return;
    }
    const char *kind = ts_node_type(node);
    if (strcmp(kind, "block_mapping") != 0) {
        return;
    }
    /* Only process root-level block_mapping (depth 0 or 1) */
    TSNode parent = ts_node_parent(node);
    if (ts_node_is_null(parent)) {
        walk_yaml_mapping(ctx, node, NULL);
    } else {
        const char *pk = ts_node_type(parent);
        if (strcmp(pk, "stream") == 0 || strcmp(pk, "document") == 0 ||
            strcmp(pk, "block_node") == 0) {
            walk_yaml_mapping(ctx, node, NULL);
        }
    }
}

// --- Main unified cursor walk ---

void cbm_extract_unified(CBMExtractCtx *ctx) {
    const CBMLangSpec *spec = cbm_lang_spec(ctx->language);
    if (!spec) {
        return;
    }

    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    WalkState state;
    memset(&state, 0, sizeof(state));

    const char *top_level_qn = ctx->module_qn;
    if (ctx->language == CBM_LANG_GDSCRIPT) {
        const char *anchor_qn =
            cbm_gdscript_anchor_qn(ctx->arena, ctx->root, ctx->source, ctx->project, ctx->rel_path);
        if (anchor_qn && anchor_qn[0]) {
            top_level_qn = anchor_qn;
        }
    }

    uint32_t depth = 0;

    for (;;) {
        TSNode node = ts_tree_cursor_current_node(&cursor);

        // 1. Pop scopes we've ascended out of
        pop_expired_scopes(&state, depth);

        // 2. Recompute state from remaining scopes
        recompute_state(&state, top_level_qn);

        // 3. Dispatch to all handlers
        handle_string_constants(ctx, node, &state, top_level_qn);
        handle_calls(ctx, node, spec, &state);
        handle_usages(ctx, node, spec, &state);
        handle_throws(ctx, node, spec, &state);
        handle_readwrites(ctx, node, spec, &state);
        handle_type_refs(ctx, node, spec, &state);
        handle_env_accesses(ctx, node, spec, &state);
        handle_type_assigns(ctx, node, spec, &state);
        handle_string_refs(ctx, node, &state);
        handle_yaml_nested(ctx, node);

        /* Scan YAML/JSON for infra bindings (topic→URL pairs) */
        if (ctx->language == CBM_LANG_YAML || ctx->language == CBM_LANG_JSON) {
            const char *nk = ts_node_type(node);
            if (strcmp(nk, "block_sequence") == 0 || strcmp(nk, "block_mapping") == 0 ||
                strcmp(nk, "array") == 0 || strcmp(nk, "document") == 0) {
                scan_yaml_for_infra_bindings(ctx, node);
            }
        }

        // 4. Push scope markers for boundary nodes
        if (spec->function_node_types && cbm_kind_in_set(node, spec->function_node_types)) {
            const char *fqn = compute_func_qn(ctx, node, spec, &state);
            if (fqn) {
                push_scope(&state, SCOPE_FUNC, depth, fqn);
            }
        } else if (spec->class_node_types && cbm_kind_in_set(node, spec->class_node_types)) {
            const char *cqn = compute_class_qn(ctx, node, &state);
            if (cqn) {
                push_scope(&state, SCOPE_CLASS, depth, cqn);
            }
        } else if (ctx->language == CBM_LANG_RUST && strcmp(ts_node_type(node), "impl_item") == 0) {
            // Rust impl block acts as class scope for methods
            TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
            if (!ts_node_is_null(type_node)) {
                char *type_name = cbm_node_text(ctx->arena, type_node, ctx->source);
                if (type_name && type_name[0]) {
                    const char *tqn =
                        cbm_fqn_compute(ctx->arena, ctx->project, ctx->rel_path, type_name);
                    push_scope(&state, SCOPE_CLASS, depth, tqn);
                }
            }
        }

        if (spec->call_node_types && cbm_kind_in_set(node, spec->call_node_types)) {
            push_scope(&state, SCOPE_CALL, depth, NULL);
        }
        if (spec->import_node_types && cbm_kind_in_set(node, spec->import_node_types)) {
            push_scope(&state, SCOPE_IMPORT, depth, NULL);
        }

        // 5. Advance cursor: DFS order
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            depth++;
            continue;
        }
        if (ts_tree_cursor_goto_next_sibling(&cursor)) {
            continue;
        }
        // Ascend until we find a sibling
        bool found = false;
        while (ts_tree_cursor_goto_parent(&cursor)) {
            depth--;
            if (ts_tree_cursor_goto_next_sibling(&cursor)) {
                found = true;
                break;
            }
        }
        if (!found) {
            break;
        }
    }

    ts_tree_cursor_delete(&cursor);
}
