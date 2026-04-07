/*
 * extract_channels.c — Pub/sub channel participation extractor.
 *
 * Detects Socket.IO and EventEmitter emit / on / addListener call patterns in
 * JS/TS/TSX source and records each participation as a CBMChannel record.
 * Transport is stored on the record ("socketio", "event_emitter") so later
 * detectors for Kafka, Cloud Pub/Sub, etc. can share the same schema without
 * changing the edge types.
 *
 * String-constant resolution: when the channel name argument is a plain
 * identifier, we perform a single-pass local scan of the module body to
 * resolve `const EVENT = "foo"` style bindings.  Template literals and
 * config-driven names stay unresolved (acceptable — those require real
 * data-flow analysis).
 */
#include "cbm.h"
#include "arena.h"
#include "helpers.h"
#include "foundation/constants.h"
#include "tree_sitter/api.h"
#include <stdint.h>
#include <string.h>

enum {
    CHAN_CONST_CAP = 256,  /* max tracked identifiers per file */
    CHAN_STACK_CAP = 4096, /* tree-walk stack */
    CHAN_IDENT_MAX = 128,  /* max identifier length tracked */
};

typedef struct {
    const char *name;  /* borrowed — points into arena */
    const char *value; /* borrowed — points into arena */
} chan_const_t;

typedef struct {
    chan_const_t items[CHAN_CONST_CAP];
    int count;
} chan_const_table_t;

/* ── String literal helpers ──────────────────────────────────────── */

static const char *unquote_string(CBMArena *a, const char *s) {
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    if (len < CBM_QUOTE_PAIR) {
        return NULL;
    }
    char first = s[0];
    char last = s[len - CBM_QUOTE_OFFSET];
    if ((first == '"' && last == '"') || (first == '\'' && last == '\'') ||
        (first == '`' && last == '`')) {
        return cbm_arena_strndup(a, s + CBM_QUOTE_OFFSET, len - CBM_QUOTE_PAIR);
    }
    return NULL;
}

/* Extract a literal channel name from an argument node.  Returns NULL if the
 * argument is not a plain string literal (caller can then try identifier
 * resolution via the constant table). */
static const char *literal_from_arg(CBMExtractCtx *ctx, TSNode arg) {
    const char *kind = ts_node_type(arg);
    if (strcmp(kind, "string") != 0 && strcmp(kind, "string_literal") != 0) {
        return NULL;
    }
    char *text = cbm_node_text(ctx->arena, arg, ctx->source);
    return unquote_string(ctx->arena, text);
}

/* ── Constant resolution table ──────────────────────────────────── */

/* Walk the whole tree once and collect `const IDENT = "value"` bindings so
 * later passes can resolve bare-identifier channel arguments.  Only scalar
 * string literals are tracked — template literals and expressions are left
 * unresolved.  This is a flat lookup; scope boundaries are ignored (a single
 * const table per file is sufficient for the common Socket.IO pattern). */
static void scan_string_consts(CBMExtractCtx *ctx, chan_const_table_t *tbl) {
    TSNode stack[CHAN_STACK_CAP];
    int top = 0;
    stack[top++] = ctx->root;

    while (top > 0 && tbl->count < CHAN_CONST_CAP) {
        TSNode node = stack[--top];
        const char *kind = ts_node_type(node);

        /* `variable_declarator` is the TS/JS form of `IDENT = value`.
         * The `name` field holds the identifier, `value` holds the RHS. */
        if (strcmp(kind, "variable_declarator") == 0) {
            TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
            TSNode value_node = ts_node_child_by_field_name(node, TS_FIELD("value"));
            if (!ts_node_is_null(name_node) && !ts_node_is_null(value_node)) {
                const char *nk = ts_node_type(name_node);
                const char *vk = ts_node_type(value_node);
                if (strcmp(nk, "identifier") == 0 &&
                    (strcmp(vk, "string") == 0 || strcmp(vk, "string_literal") == 0)) {
                    char *name_text = cbm_node_text(ctx->arena, name_node, ctx->source);
                    char *value_text = cbm_node_text(ctx->arena, value_node, ctx->source);
                    const char *unq = unquote_string(ctx->arena, value_text);
                    if (name_text && unq) {
                        tbl->items[tbl->count].name = name_text;
                        tbl->items[tbl->count].value = unq;
                        tbl->count++;
                    }
                }
            }
        }

        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_ONE; i >= 0 && top < CHAN_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}

/* Resolve an identifier against the constant table.  Returns NULL on miss. */
static const char *resolve_identifier(const chan_const_table_t *tbl, const char *name) {
    if (!name) {
        return NULL;
    }
    for (int i = 0; i < tbl->count; i++) {
        if (tbl->items[i].name && strcmp(tbl->items[i].name, name) == 0) {
            return tbl->items[i].value;
        }
    }
    return NULL;
}

/* ── Enclosing function detection ───────────────────────────────── */

/* Walk up the parent chain to find the nearest function-like ancestor and
 * build a best-effort qualified name for it. */
static const char *enclosing_function_qn(CBMExtractCtx *ctx, TSNode node) {
    TSNode parent = ts_node_parent(node);
    while (!ts_node_is_null(parent)) {
        const char *pk = ts_node_type(parent);
        if (strcmp(pk, "function_declaration") == 0 || strcmp(pk, "method_definition") == 0 ||
            strcmp(pk, "arrow_function") == 0 || strcmp(pk, "function_expression") == 0 ||
            strcmp(pk, "function") == 0 || strcmp(pk, "method_signature") == 0) {
            TSNode name_node = ts_node_child_by_field_name(parent, TS_FIELD("name"));
            if (!ts_node_is_null(name_node)) {
                char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
                if (name && name[0]) {
                    return name;
                }
            }
            return NULL;
        }
        parent = ts_node_parent(parent);
    }
    return NULL;
}

/* ── Emit / listen detection ────────────────────────────────────── */

static bool is_emit_method(const char *name) {
    return name && strcmp(name, "emit") == 0;
}

static bool is_listen_method(const char *name) {
    return name && (strcmp(name, "on") == 0 || strcmp(name, "addListener") == 0 ||
                    strcmp(name, "once") == 0);
}

/* Match a transport receiver: plain identifier `socket`/`io`/`emitter` or a
 * member expression like `this.io` / `client.socket`.  Returns "socketio" or
 * "event_emitter" based on a name heuristic, NULL if the receiver is unknown
 * (which means we skip — we don't want to mistake any .emit()/.on() call
 * for a channel). */
static const char *classify_receiver(CBMExtractCtx *ctx, TSNode object_node) {
    char *text = cbm_node_text(ctx->arena, object_node, ctx->source);
    if (!text) {
        return NULL;
    }
    /* Strip leading `this.`, `self.`, or class-instance prefixes for the match. */
    const char *tail = text;
    const char *dot = strrchr(tail, '.');
    if (dot) {
        tail = dot + SKIP_ONE;
    }
    /* Common Socket.IO variable names. */
    if (strcmp(tail, "socket") == 0 || strcmp(tail, "io") == 0 || strcmp(tail, "ws") == 0 ||
        strcmp(tail, "client") == 0 || strcmp(tail, "server") == 0) {
        return "socketio";
    }
    /* Node.js EventEmitter convention. */
    if (strcmp(tail, "emitter") == 0 || strcmp(tail, "eventEmitter") == 0 ||
        strcmp(tail, "events") == 0 || strcmp(tail, "bus") == 0 || strcmp(tail, "eventBus") == 0 ||
        strcmp(tail, "pubsub") == 0) {
        return "event_emitter";
    }
    return NULL;
}

/* Process a single call_expression node if it looks like a channel call. */
static void process_channel_call(CBMExtractCtx *ctx, TSNode call,
                                 const chan_const_table_t *consts) {
    /* call_expression { function: member_expression { object, property }, arguments } */
    TSNode func = ts_node_child_by_field_name(call, TS_FIELD("function"));
    if (ts_node_is_null(func) || strcmp(ts_node_type(func), "member_expression") != 0) {
        return;
    }
    TSNode object = ts_node_child_by_field_name(func, TS_FIELD("object"));
    TSNode property = ts_node_child_by_field_name(func, TS_FIELD("property"));
    if (ts_node_is_null(object) || ts_node_is_null(property)) {
        return;
    }

    char *method = cbm_node_text(ctx->arena, property, ctx->source);
    CBMChannelDirection direction;
    if (is_emit_method(method)) {
        direction = CBM_CHANNEL_EMIT;
    } else if (is_listen_method(method)) {
        direction = CBM_CHANNEL_LISTEN;
    } else {
        return;
    }

    const char *transport = classify_receiver(ctx, object);
    if (!transport) {
        return;
    }

    /* First positional argument is the channel name. */
    TSNode args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
    if (ts_node_is_null(args)) {
        return;
    }
    uint32_t arg_count = ts_node_named_child_count(args);
    if (arg_count == 0) {
        return;
    }
    TSNode first = ts_node_named_child(args, 0);

    const char *channel_name = literal_from_arg(ctx, first);
    if (!channel_name) {
        /* Try identifier resolution via the constant table. */
        const char *kind = ts_node_type(first);
        if (strcmp(kind, "identifier") == 0) {
            char *ident = cbm_node_text(ctx->arena, first, ctx->source);
            channel_name = resolve_identifier(consts, ident);
        }
    }
    if (!channel_name) {
        return; /* template literal, member access, expression — skip */
    }

    CBMChannel ch = {
        .channel_name = channel_name,
        .transport = transport,
        .enclosing_func_qn = enclosing_function_qn(ctx, call),
        .direction = direction,
    };
    cbm_channels_push(&ctx->result->channels, ctx->arena, ch);
}

/* ── Entry point ────────────────────────────────────────────────── */

void cbm_extract_channels(CBMExtractCtx *ctx) {
    /* Only JS/TS variants — Socket.IO and EventEmitter are Node.js ecosystem. */
    if (ctx->language != CBM_LANG_JAVASCRIPT && ctx->language != CBM_LANG_TYPESCRIPT &&
        ctx->language != CBM_LANG_TSX) {
        return;
    }

    chan_const_table_t consts = {0};
    scan_string_consts(ctx, &consts);

    /* Second pass: walk the tree looking for call_expression nodes. */
    TSNode stack[CHAN_STACK_CAP];
    int top = 0;
    stack[top++] = ctx->root;

    while (top > 0) {
        TSNode node = stack[--top];
        if (strcmp(ts_node_type(node), "call_expression") == 0) {
            process_channel_call(ctx, node, &consts);
        }
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_ONE; i >= 0 && top < CHAN_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}
