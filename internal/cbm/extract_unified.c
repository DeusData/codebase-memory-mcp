#include "extract_unified.h"
#include "arena.h" // cbm_arena_sprintf
#include "cbm.h"   // CBMExtractCtx
#include "helpers.h"
#include "lang_specs.h"      // CBMLangSpec, cbm_lang_spec, CBM_LANG_*
#include "tree_sitter/api.h" // TSNode, TSTreeCursor, ts_tree_cursor_*, ts_node_*
#include "foundation/constants.h"

enum { MAX_INFRA_BINDINGS = 8 };

#include <stdint.h> // uint32_t, uint8_t
#include <string.h>

// --- Scope stack management ---

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
    while (state->scope_top > 0 && state->scopes[state->scope_top - SKIP_ONE].depth >= cur_depth) {
        state->scope_top--;
    }
}

// Recompute state flags from the current scope stack.
static void recompute_state(WalkState *state, const char *module_qn) {
    state->enclosing_func_qn = module_qn;
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

// Try to resolve Wolfram function QN from set_delayed_top/set_top/set_delayed/set LHS.
static const char *compute_wolfram_func_qn(CBMExtractCtx *ctx, TSNode node) {
    const char *nk = ts_node_type(node);
    if (strcmp(nk, "set_delayed_top") != 0 && strcmp(nk, "set_top") != 0 &&
        strcmp(nk, "set_delayed") != 0 && strcmp(nk, "set") != 0) {
        return NULL; // not a Wolfram set node — signal caller to continue
    }
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

// Resolve the name node for a function, handling arrow functions.
static TSNode resolve_func_name_node(TSNode node) {
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (ts_node_is_null(name_node) && strcmp(ts_node_type(node), "arrow_function") == 0) {
        TSNode parent = ts_node_parent(node);
        if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "variable_declarator") == 0) {
            name_node = ts_node_child_by_field_name(parent, TS_FIELD("name"));
        }
    }
    return name_node;
}

// Compute function QN for scope tracking (mirrors cbm_enclosing_func_qn logic).
static const char *compute_func_qn(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec,
                                   WalkState *state) {
    (void)spec;
    if (ctx->language == CBM_LANG_WOLFRAM) {
        return compute_wolfram_func_qn(ctx, node);
    }

    TSNode name_node = resolve_func_name_node(node);
    if (ts_node_is_null(name_node)) {
        return NULL;
    }

    char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
    if (!name || !name[0]) {
        return NULL;
    }

    if (state->enclosing_class_qn) {
        return cbm_arena_sprintf(ctx->arena, "%s.%s", state->enclosing_class_qn, name);
    }
    return cbm_fqn_compute(ctx->arena, ctx->project, ctx->rel_path, name);
}

// Compute class QN for scope tracking.
static const char *compute_class_qn(CBMExtractCtx *ctx, TSNode node) {
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (ts_node_is_null(name_node)) {
        return NULL;
    }

    char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
    if (!name || !name[0]) {
        return NULL;
    }

    return cbm_fqn_compute(ctx->arena, ctx->project, ctx->rel_path, name);
}

/* Forward declaration */
static bool is_string_node(const char *kind);

// --- Module-level constant collection ---

static void handle_string_constants(CBMExtractCtx *ctx, TSNode node, const WalkState *state) {
    /* Only collect at module level (not inside functions/classes) */
    if (state->enclosing_func_qn != NULL && state->enclosing_func_qn != ctx->module_qn) {
        return;
    }

    const char *kind = ts_node_type(node);

    /* Python: expression_statement → assignment → identifier = string */
    /* Go: short_var_declaration, const_spec */
    /* JS/TS: variable_declarator, lexical_declaration */
    if (strcmp(kind, "assignment") != 0 && strcmp(kind, "expression_statement") != 0 &&
        strcmp(kind, "short_var_declaration") != 0 && strcmp(kind, "const_spec") != 0 &&
        strcmp(kind, "variable_declarator") != 0) {
        return;
    }

    /* Find name (left side) and value (right side) */
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("left"));
    TSNode value_node = ts_node_child_by_field_name(node, TS_FIELD("right"));

    /* Some grammars use "name" + "value" fields */
    if (ts_node_is_null(name_node)) {
        name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    }
    if (ts_node_is_null(value_node)) {
        value_node = ts_node_child_by_field_name(node, TS_FIELD("value"));
    }

    if (ts_node_is_null(name_node) || ts_node_is_null(value_node)) {
        return;
    }

    /* Name must be an identifier */
    const char *name_kind = ts_node_type(name_node);
    if (strcmp(name_kind, "identifier") != 0 && strcmp(name_kind, "constant") != 0) {
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
    if (vlen >= CBM_QUOTE_PAIR && (value[0] == '"' || value[0] == '\'')) {
        value = cbm_arena_strndup(ctx->arena, value + SKIP_ONE, (size_t)(vlen - PAIR_LEN));
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
    return (strcmp(kind, "string_literal") == 0 || strcmp(kind, "string") == 0 ||
            strcmp(kind, "string_content") == 0 ||
            strcmp(kind, "interpreted_string_literal") == 0 ||
            strcmp(kind, "raw_string_literal") == 0 || strcmp(kind, "string_value") == 0 ||
            /* YAML string types */
            strcmp(kind, "double_quote_scalar") == 0 || strcmp(kind, "single_quote_scalar") == 0);
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
    if (len >= CBM_QUOTE_PAIR && (text[0] == '"' || text[0] == '\'')) {
        content = text + SKIP_ONE;
        len -= PAIR_LEN;
        if (len <= 0) {
            return;
        }
    }

    /* Classify */
    int kind_val = cbm_classify_string(content, len);
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
// Classify and emit a YAML leaf value as a string_ref with key_path.
static void emit_yaml_leaf_value(CBMExtractCtx *ctx, TSNode val, const char *path) {
    char *val_text = cbm_node_text(ctx->arena, val, ctx->source);
    if (!val_text || !val_text[0]) {
        return;
    }

    int vlen = (int)strlen(val_text);
    const char *content = val_text;
    if (vlen >= CBM_QUOTE_PAIR && (val_text[0] == '"' || val_text[0] == '\'')) {
        content = val_text + SKIP_ONE;
        vlen -= PAIR_LEN;
        if (vlen <= 0) {
            return;
        }
    }

    int kind_val = cbm_classify_string(content, vlen);
    if (kind_val < 0) {
        return;
    }

    char *stored = cbm_arena_strndup(ctx->arena, content, (size_t)vlen);
    if (!stored) {
        return;
    }

    CBMStringRef ref = {
        .value = stored,
        .enclosing_func_qn = ctx->module_qn,
        .key_path = path,
        .kind = (CBMStringRefKind)kind_val,
    };
    cbm_stringref_push(&ctx->result->string_refs, ctx->arena, ref);
}

typedef struct {
    TSNode node;
    const char *prefix;
} yaml_walk_frame_t;
#define YAML_WALK_STACK_CAP CBM_SZ_256

/* Push block_mapping children of a block_node/block_mapping value onto the walk stack. */
static void push_yaml_block_children(TSNode val, const char *path, yaml_walk_frame_t *stack,
                                     int *top) {
    uint32_t vnc = ts_node_named_child_count(val);
    for (int vi = (int)vnc - SKIP_ONE; vi >= 0 && *top < YAML_WALK_STACK_CAP; vi--) {
        TSNode vc = ts_node_named_child(val, (uint32_t)vi);
        const char *vctype = ts_node_type(vc);
        if (strcmp(vctype, "block_mapping") == 0 || strcmp(vctype, "block_mapping_pair") == 0) {
            stack[(*top)++] = (yaml_walk_frame_t){vc, path};
        }
    }
}

static void walk_yaml_mapping(CBMExtractCtx *ctx, TSNode root, const char *root_prefix) {
    yaml_walk_frame_t stack[YAML_WALK_STACK_CAP];
    int top = 0;
    stack[top++] = (yaml_walk_frame_t){root, root_prefix};

    while (top > 0) {
        yaml_walk_frame_t frame = stack[--top];
        TSNode node = frame.node;
        const char *prefix = frame.prefix;
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (strcmp(ts_node_type(child), "block_mapping_pair") != 0) {
                continue;
            }
            TSNode key = ts_node_child_by_field_name(child, TS_FIELD("key"));
            if (ts_node_is_null(key)) {
                continue;
            }
            char *key_text = cbm_node_text(ctx->arena, key, ctx->source);
            if (!key_text || !key_text[0]) {
                continue;
            }
            const char *path =
                prefix ? cbm_arena_sprintf(ctx->arena, "%s.%s", prefix, key_text) : key_text;
            TSNode val = ts_node_child_by_field_name(child, TS_FIELD("value"));
            if (ts_node_is_null(val)) {
                continue;
            }
            const char *vk = ts_node_type(val);
            if (strcmp(vk, "block_node") == 0 || strcmp(vk, "block_mapping") == 0) {
                push_yaml_block_children(val, path, stack, &top);
                continue;
            }
            emit_yaml_leaf_value(ctx, val, path);
        }
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
// Strip quotes from a YAML scalar value.
static char *strip_yaml_quotes(CBMArena *a, char *v) {
    if (!v || !v[0]) {
        return v;
    }
    int vlen = (int)strlen(v);
    if (vlen >= CBM_QUOTE_PAIR && (v[0] == '"' || v[0] == '\'')) {
        return cbm_arena_strndup(a, v + SKIP_ONE, (size_t)(vlen - PAIR_LEN));
    }
    return v;
}

// Scan a nested YAML block_mapping for target keys (push_endpoint, uri, etc.).
static void scan_nested_mapping_targets(CBMExtractCtx *ctx, TSNode val, const char **targets,
                                        int *n_targets) {
    uint32_t vnc = ts_node_named_child_count(val);
    for (uint32_t vi = 0; vi < vnc; vi++) {
        TSNode vc = ts_node_named_child(val, vi);
        if (strcmp(ts_node_type(vc), "block_mapping") != 0) {
            continue;
        }
        uint32_t mnc = ts_node_named_child_count(vc);
        for (uint32_t mi = 0; mi < mnc; mi++) {
            TSNode mp = ts_node_named_child(vc, mi);
            if (strcmp(ts_node_type(mp), "block_mapping_pair") != 0) {
                continue;
            }
            TSNode mk = ts_node_child_by_field_name(mp, TS_FIELD("key"));
            TSNode mv = ts_node_child_by_field_name(mp, TS_FIELD("value"));
            if (ts_node_is_null(mk) || ts_node_is_null(mv)) {
                continue;
            }
            char *mktext = cbm_node_text(ctx->arena, mk, ctx->source);
            if (mktext && is_target_key(mktext) && *n_targets < MAX_INFRA_BINDINGS) {
                char *mvtext =
                    strip_yaml_quotes(ctx->arena, cbm_node_text(ctx->arena, mv, ctx->source));
                if (mvtext && strstr(mvtext, "://")) {
                    targets[(*n_targets)++] = mvtext;
                }
            }
        }
    }
}

// Emit infra bindings for each source × target pair combination.
static void emit_infra_bindings(CBMExtractCtx *ctx, const char **sources, const char **source_keys,
                                int n_sources, const char **targets, int n_targets) {
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

static void scan_mapping_for_bindings(CBMExtractCtx *ctx, TSNode mapping) {
    const char *sources[MAX_INFRA_BINDINGS] = {NULL};
    const char *source_keys[MAX_INFRA_BINDINGS] = {NULL};
    int n_sources = 0;
    const char *targets[MAX_INFRA_BINDINGS] = {NULL};
    int n_targets = 0;

    uint32_t nc = ts_node_named_child_count(mapping);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode pair = ts_node_named_child(mapping, i);
        if (strcmp(ts_node_type(pair), "block_mapping_pair") != 0) {
            continue;
        }
        TSNode key = ts_node_child_by_field_name(pair, TS_FIELD("key"));
        TSNode val = ts_node_child_by_field_name(pair, TS_FIELD("value"));
        if (ts_node_is_null(key) || ts_node_is_null(val)) {
            continue;
        }
        char *k = cbm_node_text(ctx->arena, key, ctx->source);
        if (!k) {
            continue;
        }

        const char *vtype = ts_node_type(val);
        if (strcmp(vtype, "block_node") != 0 && strcmp(vtype, "block_mapping") != 0) {
            char *v = strip_yaml_quotes(ctx->arena, cbm_node_text(ctx->arena, val, ctx->source));
            if (is_source_key(k) && n_sources < MAX_INFRA_BINDINGS) {
                sources[n_sources] = v;
                source_keys[n_sources] = k;
                n_sources++;
            }
            if (is_target_key(k) && n_targets < MAX_INFRA_BINDINGS && v && strstr(v, "://")) {
                targets[n_targets++] = v;
            }
        } else {
            scan_nested_mapping_targets(ctx, val, targets, &n_targets);
        }
    }

    emit_infra_bindings(ctx, sources, source_keys, n_sources, targets, n_targets);
}

#define INFRA_SCAN_STACK_CAP CBM_SZ_512
static void scan_yaml_for_infra_bindings(CBMExtractCtx *ctx, TSNode root) {
    TSNode stack[INFRA_SCAN_STACK_CAP];
    int top = 0;
    stack[top++] = root;
    while (top > 0) {
        TSNode node = stack[--top];
        if (strcmp(ts_node_type(node), "block_mapping") == 0) {
            scan_mapping_for_bindings(ctx, node);
        }
        uint32_t nc = ts_node_named_child_count(node);
        for (int i = (int)nc - SKIP_ONE; i >= 0 && top < INFRA_SCAN_STACK_CAP; i--) {
            stack[top++] = ts_node_named_child(node, (uint32_t)i);
        }
    }
}

/* ── HCL infrastructure binding extraction ───────────────────────────
 * Scan HCL block nodes (resource, dynamic) for attribute pairs
 * where one is a source key (topic, queue_name) and another is a
 * target key (uri, push_endpoint). Handles nested blocks like
 * push_config { push_endpoint = "..." }. */
// Extract a string value from an HCL attribute value node
// (quoted_template/template_literal/string_lit).
static char *extract_hcl_string_val(CBMArena *a, TSNode val_node, const char *source) {
    const char *vk = ts_node_type(val_node);
    if (strcmp(vk, "quoted_template") != 0 && strcmp(vk, "template_literal") != 0 &&
        strcmp(vk, "string_lit") != 0) {
        return NULL;
    }
    char *val = cbm_node_text(a, val_node, source);
    return strip_yaml_quotes(a, val);
}

// Scan a nested HCL block for target keys (push_endpoint, uri, etc.).
static void scan_hcl_nested_block_targets(CBMExtractCtx *ctx, TSNode block, const char **targets,
                                          int *n_targets) {
    uint32_t bnc = ts_node_named_child_count(block);
    for (uint32_t bi = 0; bi < bnc; bi++) {
        TSNode bchild = ts_node_named_child(block, bi);
        if (strcmp(ts_node_type(bchild), "attribute") != 0) {
            continue;
        }
        TSNode bkey = ts_node_named_child(bchild, 0);
        TSNode bval = ts_node_named_child(bchild, SKIP_ONE);
        if (ts_node_is_null(bkey) || ts_node_is_null(bval)) {
            continue;
        }
        char *bk = cbm_node_text(ctx->arena, bkey, ctx->source);
        if (!bk || !is_target_key(bk)) {
            continue;
        }
        char *bv = extract_hcl_string_val(ctx->arena, bval, ctx->source);
        if (bv && strstr(bv, "://") && *n_targets < MAX_INFRA_BINDINGS) {
            targets[(*n_targets)++] = bv;
        }
    }
}

static void scan_hcl_block_for_bindings(CBMExtractCtx *ctx, TSNode block) {
    const char *sources[MAX_INFRA_BINDINGS] = {NULL};
    const char *source_keys[MAX_INFRA_BINDINGS] = {NULL};
    int n_sources = 0;
    const char *targets[MAX_INFRA_BINDINGS] = {NULL};
    int n_targets = 0;

    uint32_t nc = ts_node_named_child_count(block);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_named_child(block, i);
        const char *ck = ts_node_type(child);

        if (strcmp(ck, "attribute") == 0) {
            TSNode key_node = ts_node_named_child(child, 0);
            TSNode val_node = ts_node_named_child(child, SKIP_ONE);
            if (ts_node_is_null(key_node) || ts_node_is_null(val_node)) {
                continue;
            }
            char *key = cbm_node_text(ctx->arena, key_node, ctx->source);
            if (!key) {
                continue;
            }

            char *val = extract_hcl_string_val(ctx->arena, val_node, ctx->source);
            if (!val || !val[0]) {
                continue;
            }

            if (is_source_key(key) && n_sources < MAX_INFRA_BINDINGS) {
                sources[n_sources] = val;
                source_keys[n_sources] = key;
                n_sources++;
            }
            if (is_target_key(key) && n_targets < MAX_INFRA_BINDINGS && strstr(val, "://")) {
                targets[n_targets++] = val;
            }
        } else if (strcmp(ck, "block") == 0) {
            scan_hcl_nested_block_targets(ctx, child, targets, &n_targets);
        }
    }

    emit_infra_bindings(ctx, sources, source_keys, n_sources, targets, n_targets);
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

// Scan infra bindings for YAML/JSON/HCL languages.
static void scan_infra_bindings(CBMExtractCtx *ctx, TSNode node) {
    if (ctx->language == CBM_LANG_YAML || ctx->language == CBM_LANG_JSON) {
        const char *nk = ts_node_type(node);
        if (strcmp(nk, "block_sequence") == 0 || strcmp(nk, "block_mapping") == 0 ||
            strcmp(nk, "array") == 0 || strcmp(nk, "document") == 0) {
            scan_yaml_for_infra_bindings(ctx, node);
        }
    } else if (ctx->language == CBM_LANG_HCL) {
        if (strcmp(ts_node_type(node), "block") == 0) {
            scan_hcl_block_for_bindings(ctx, node);
        }
    }
}

// Push scope markers for function, class, call, and import boundary nodes.
static void push_boundary_scopes(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec,
                                 WalkState *state, uint32_t depth) {
    if (spec->function_node_types && cbm_kind_in_set(node, spec->function_node_types)) {
        const char *fqn = compute_func_qn(ctx, node, spec, state);
        if (fqn) {
            push_scope(state, SCOPE_FUNC, depth, fqn);
        }
    } else if (spec->class_node_types && cbm_kind_in_set(node, spec->class_node_types)) {
        const char *cqn = compute_class_qn(ctx, node);
        if (cqn) {
            push_scope(state, SCOPE_CLASS, depth, cqn);
        }
    } else if (ctx->language == CBM_LANG_RUST && strcmp(ts_node_type(node), "impl_item") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, TS_FIELD("type"));
        if (!ts_node_is_null(type_node)) {
            char *type_name = cbm_node_text(ctx->arena, type_node, ctx->source);
            if (type_name && type_name[0]) {
                const char *tqn =
                    cbm_fqn_compute(ctx->arena, ctx->project, ctx->rel_path, type_name);
                push_scope(state, SCOPE_CLASS, depth, tqn);
            }
        }
    }

    if (spec->call_node_types && cbm_kind_in_set(node, spec->call_node_types)) {
        push_scope(state, SCOPE_CALL, depth, NULL);
    }
    if (spec->import_node_types && cbm_kind_in_set(node, spec->import_node_types)) {
        push_scope(state, SCOPE_IMPORT, depth, NULL);
    }
}

void cbm_extract_unified(CBMExtractCtx *ctx) {
    const CBMLangSpec *spec = cbm_lang_spec(ctx->language);
    if (!spec) {
        return;
    }

    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    WalkState state;
    memset(&state, 0, sizeof(state));

    uint32_t depth = 0;

    for (;;) {
        TSNode node = ts_tree_cursor_current_node(&cursor);

        pop_expired_scopes(&state, depth);
        recompute_state(&state, ctx->module_qn);

        handle_string_constants(ctx, node, &state);
        handle_calls(ctx, node, spec, &state);
        handle_usages(ctx, node, spec, &state);
        handle_throws(ctx, node, spec, &state);
        handle_readwrites(ctx, node, spec, &state);
        handle_type_refs(ctx, node, spec, &state);
        handle_env_accesses(ctx, node, spec, &state);
        handle_type_assigns(ctx, node, spec, &state);
        handle_string_refs(ctx, node, &state);
        handle_yaml_nested(ctx, node);
        scan_infra_bindings(ctx, node);

        push_boundary_scopes(ctx, node, spec, &state, depth);

        if (ts_tree_cursor_goto_first_child(&cursor)) {
            depth++;
            continue;
        }
        if (ts_tree_cursor_goto_next_sibling(&cursor)) {
            continue;
        }
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
